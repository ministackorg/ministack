# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
Amazon Translate emulator.

JSON 1.1 protocol with X-Amz-Target prefix ``AWSShineFrontendService_20170701``
(``translate-2017-07-01``).

Implemented:
  StartTextTranslationJob, DescribeTextTranslationJob, ListTextTranslationJobs,
  StopTextTranslationJob.

Batch jobs run as a background task that walks SUBMITTED -> IN_PROGRESS ->
COMPLETED over ``TRANSLATE_JOB_RUN_SECONDS`` (0 completes immediately). The
worker reads every document under ``InputDataConfig.S3Uri`` out of MiniStack's
S3 store, writes one translated document per input per target language, and
publishes a ``Translate TextTranslationJob State Change`` event so EventBridge
rules downstream of a batch job fire the way they do on AWS.

Output lands where the real service puts it rather than where the caller asked:
``OutputDataConfig.S3Uri`` is rewritten to
``s3://<bucket>/<prefix><account-id>-TranslateText-<JobId>/`` and reported that
way from ``DescribeTextTranslationJob``, which is the location a consumer has
to read. Translated documents keep the input name with the target language code
prefixed onto it (``fr.transcript.xlf``), and each target language gets a
``details/<code>.auxiliary-translation-details.json`` summary alongside them, as
on AWS.

There is no machine translation here. The translated text is deterministic
canned output derived from the source text, following the same approach as
``bedrock_runtime._mock_reply`` and ``transcribe._mock_transcript``: identical
input gives identical output, and different input gives distinguishable output.
The document is rewritten in place rather than replaced, so the structure a
consumer parses is real:

  * ``text/plain`` is translated line by line, so line counts survive.
  * ``text/html`` keeps its markup and translates only the text nodes.
  * ``application/x-xliff+xml`` gets a populated ``<target>`` in every
    ``<trans-unit>`` and ``target-language`` on every ``<file>``, which is what
    an XLIFF round trip merges back onto its source segments.
  * The Office content types are ZIP containers, so they are copied through
    byte for byte and counted as translated rather than corrupted.

A document that cannot be decoded or parsed is counted in
``JobDetails.DocumentsWithErrorsCount`` and the job ends
``COMPLETED_WITH_ERROR``, as on AWS. An input location with no readable
documents fails the job with a ``Message`` rather than erroring at ``Start``.
``Message`` is filled on success as well as failure, which is what the service
reference shows a finished job reporting.

``ClientToken`` is honoured: replaying a start request with a token already
seen returns the original job instead of starting a second one.

Deferred:
  TranslateText and TranslateDocument (the synchronous operations),
  the custom terminology and parallel data resource families, ListLanguages
  and TagResource / UntagResource / ListTagsForResource. ``TerminologyNames``,
  ``ParallelDataNames``, ``Settings`` and ``OutputDataConfig.EncryptionKey``
  round-trip on the job record without changing the output.
"""

import asyncio
import base64
import copy
import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET

from defusedxml.common import DefusedXmlException
from defusedxml.ElementTree import fromstring as _xml_fromstring

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    now_iso,
    request_scope,
)

logger = logging.getLogger("translate")

# How long a job spends between SUBMITTED and COMPLETED. Same knob shape as
# TRANSLATE_JOB_RUN_SECONDS: tests that assert on IN_PROGRESS need a non-zero
# value, tests that just want a result set it to 0. Also settable at runtime
# through /_ministack/config, so a test can pin the pace without a restart.
_JOB_RUN_SECONDS = float(os.environ.get("TRANSLATE_JOB_RUN_SECONDS", "2"))

# Granularity at which a running job notices StopTextTranslationJob. Without
# slicing the wait, a stop request would sit unobserved until the next phase
# boundary, so a job with a long run time would stay STOP_REQUESTED for as
# long as the caller was prepared to wait.
_STOP_POLL_SECONDS = 0.1

_TERMINAL_STATUSES = ("COMPLETED", "COMPLETED_WITH_ERROR", "FAILED", "STOPPED")

_JOB_STATUSES = (
    "SUBMITTED",
    "IN_PROGRESS",
    "COMPLETED",
    "COMPLETED_WITH_ERROR",
    "FAILED",
    "STOP_REQUESTED",
    "STOPPED",
)

# AWS models JobName as ^([\p{L}\p{Z}\p{N}_.:/=+\-%@]*)$. Python's re has no
# Unicode property escapes, so \w and \s stand in for \p{L}\p{N}_ and \p{Z};
# the effect on a real job name is the same.
_JOB_NAME_RE = re.compile(r"^[\w\s.:/=+\-%@]{1,256}$", re.UNICODE)
_CLIENT_TOKEN_RE = re.compile(r"^[a-zA-Z0-9-]{1,64}$")
_ROLE_ARN_RE = re.compile(r"^arn:aws(-[^:]+)?:iam::[0-9]{12}:role/.+$")
_S3_URI_RE = re.compile(r"^s3://[a-z0-9][.\-a-z0-9]{1,61}[a-z0-9](/.*)?$")

_MAX_TARGET_LANGUAGES = 10

# The content types batch translation accepts. The three Office types are ZIP
# containers rather than text, which is why they are handled separately below.
_PLAIN_TEXT = "text/plain"
_HTML = "text/html"
_XLIFF = "application/x-xliff+xml"
_DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
_PPTX = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

_OPAQUE_CONTENT_TYPES = (_DOCX, _PPTX, _XLSX)
_SUPPORTED_CONTENT_TYPES = (_PLAIN_TEXT, _HTML, _XLIFF) + _OPAQUE_CONTENT_TYPES

# What "auto" resolves to. Real Translate detects the source language per
# document with Comprehend; there is no detector here, so the resolved code is
# fixed and only ever surfaces in the auxiliary details file. The job record
# keeps reporting "auto", as on AWS.
_DEFAULT_SOURCE_LANGUAGE = "en"

_XLIFF_12_NS = "urn:oasis:names:tc:xliff:document:1.2"

_INPUT_UNREADABLE = (
    "The input location that you provided can't be accessed. Make sure that the "
    "S3 folder exists and that Amazon Translate has read permission, then try "
    "your request again."
)

_NO_INPUT_DOCUMENTS = (
    "No documents were found at the input location that you provided. Make sure "
    "that the S3 folder contains at least one document and try your request again."
)

_OUTPUT_UNWRITABLE = (
    "The output location that you provided can't be written to. Make sure that "
    "the S3 bucket exists and that Amazon Translate has write permission, then "
    "try your request again."
)

# Public members of the TextTranslationJobProperties shape, in the order
# botocore models them. Internal bookkeeping lives under keys prefixed with "_"
# and never reaches the wire.
_JOB_MEMBERS = (
    "JobId",
    "JobName",
    "JobStatus",
    "JobDetails",
    "SourceLanguageCode",
    "TargetLanguageCodes",
    "TerminologyNames",
    "ParallelDataNames",
    "Message",
    "SubmittedTime",
    "EndTime",
    "InputDataConfig",
    "OutputDataConfig",
    "DataAccessRoleArn",
    "Settings",
)

# Members echoed straight back from the request without changing behaviour.
_ECHOED_MEMBERS = ("TerminologyNames", "ParallelDataNames", "Settings")

_jobs = AccountRegionScopedDict()  # job_id -> job record
_client_tokens = AccountRegionScopedDict()  # ClientToken -> job_id


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


def reset():
    _jobs.clear()
    _client_tokens.clear()


def get_state():
    return copy.deepcopy({"jobs": _jobs, "client_tokens": _client_tokens})


def restore_state(data):
    if not data:
        return
    _jobs.clear()
    _client_tokens.clear()
    # Not `or {}`: AccountRegionScopedDict.__bool__ is scope-relative, and
    # restore runs in the default account/region. A snapshot holding jobs only
    # in other scopes would read as falsy and every one of them would be lost.
    jobs = data.get("jobs")
    if jobs is not None:
        _jobs.update(jobs)
    tokens = data.get("client_tokens")
    if tokens is not None:
        _client_tokens.update(tokens)
    _fail_orphaned_jobs()


def load_persisted_state(data):
    restore_state(data)


def _fail_orphaned_jobs():
    """A job that was mid-flight when the process stopped has no worker any
    more. Leaving it SUBMITTED, IN_PROGRESS or STOP_REQUESTED strands every
    caller polling DescribeTextTranslationJob forever, so it is failed the way
    AWS fails a job it cannot finish."""
    for job in _jobs.all_values():
        if job.get("JobStatus") not in _TERMINAL_STATUSES:
            job["JobStatus"] = "FAILED"
            job["Message"] = "Internal Failure. The job did not survive a MiniStack restart."
            job["EndTime"] = time.time()


try:
    _restored = load_state("translate")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted Translate state; continuing with fresh store")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _invalid_parameter(message):
    return error_response_json("InvalidParameterValueException", message, 400)


def _invalid_request(message):
    return error_response_json("InvalidRequestException", message, 400)


def _not_found(message):
    return error_response_json("ResourceNotFoundException", message, 400)


def _public_job(job):
    """Project a stored job onto the TextTranslationJobProperties wire shape,
    dropping members AWS omits when they have no value."""
    return {k: job[k] for k in _JOB_MEMBERS if job.get(k) is not None}


def _parse_s3_uri(uri):
    """Return (bucket, prefix) for an ``s3://`` folder URI, or (None, None).

    Translate models both S3Uri members as ``s3://...`` only — unlike
    Transcribe's Media, an HTTPS form is not accepted — and treats the path as
    a folder, so a URI without a trailing slash still addresses a prefix."""
    if not isinstance(uri, str) or not _S3_URI_RE.match(uri):
        return None, None
    rest = uri[len("s3://"):]
    bucket, _, prefix = rest.partition("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix


def _output_folder_prefix(job_id, output_prefix):
    """The folder real Translate creates inside the caller's output location.

    Every batch job writes to ``<prefix><account-id>-TranslateText-<JobId>/``
    rather than to the prefix itself, so two jobs pointed at one location do
    not overwrite each other and a consumer can tell whose output it is
    reading."""
    return f"{output_prefix}{get_account_id()}-TranslateText-{job_id}/"


def _sort_key(job):
    """Submission time with the job id breaking ties, so the ordering is total
    and stable across calls. Callers reverse it to get newest first."""
    return (job.get("SubmittedTime") or 0.0, job.get("JobId") or "")


def _encode_token(job):
    """Tokens carry the sort position of the last item returned, not an index.
    An offset would repeat an item whenever a job was created between pages."""
    submitted, job_id = _sort_key(job)
    raw = json.dumps({"st": submitted, "id": job_id}).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_token(token):
    try:
        raw = json.loads(base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8"))
        return (float(raw["st"]), str(raw["id"]))
    except Exception:
        return None


def _emit_state_change(job_id, status):
    """Publish `Translate TextTranslationJob State Change`, the event AWS emits
    when a batch job reaches a terminal state. Pipelines that chain off a
    finished translation depend on it, so a failure to deliver is logged rather
    than swallowed silently, and never propagates into the job itself."""
    try:
        from ministack.services import eventbridge as _eb

        _eb._dispatch_event(
            {
                "EventId": new_uuid(),
                "Source": "aws.translate",
                "DetailType": "Translate TextTranslationJob State Change",
                # Lowercase members: the Translate event detail is camelCase
                # with a leading lowercase letter, unlike the API shape.
                "Detail": json.dumps({"jobId": job_id, "jobStatus": status}),
                "EventBusName": "default",
                "Time": now_iso(),
                "Resources": [],
                "Account": get_account_id(),
                "Region": get_region(),
            }
        )
    except Exception:
        logger.exception("Translate: failed to publish state change for job %s", job_id)


# ---------------------------------------------------------------------------
# Mock translation
# ---------------------------------------------------------------------------


def _mock_translation(text, target_language_code):
    """Deterministic canned translation.

    Tagging the source with its target language keeps the output reproducible
    (an integration test can assert on it), distinguishable per target language
    (a two-language job writes two different documents), and obviously not a
    real translation. Whitespace-only text is left alone so a document's
    blank lines and indentation survive the round trip."""
    if not text or not text.strip():
        return text
    leading = text[: len(text) - len(text.lstrip())]
    trailing = text[len(text.rstrip()):]
    return f"{leading}[{target_language_code}] {text.strip()}{trailing}"


def _translate_plain_text(text, target_language_code):
    """Line by line, so the line count and blank-line structure of the source
    survive. A consumer that zips source and translated lines together still
    lines up."""
    return "\n".join(_mock_translation(line, target_language_code) for line in text.split("\n"))


# Splits an HTML document into markup and the text between it. The capture
# group keeps the tags in the result, so reassembly is a plain join. A `>` is
# legal inside a quoted attribute value and inside a comment, so neither can
# be allowed to terminate the match — ending a tag early would translate the
# rest of an attribute and leave the element's real text alone.
_HTML_SPLIT_RE = re.compile(r"(<!--.*?-->|<(?:\"[^\"]*\"|'[^']*'|[^>\"'])*>)", re.DOTALL)

# Text inside these elements is code, not prose. Real Translate leaves it be,
# and translating it would break the document.
_HTML_OPAQUE_ELEMENTS = ("script", "style")

_HTML_TAG_NAME_RE = re.compile(r"^<\s*(/?)\s*([a-zA-Z][a-zA-Z0-9]*)")


def _translate_html(text, target_language_code):
    """Translate the text nodes and leave the markup untouched, so the result
    is still the document the caller uploaded."""
    parts = _HTML_SPLIT_RE.split(text)
    opaque_depth = 0
    out = []
    for part in parts:
        if part.startswith("<"):
            match = _HTML_TAG_NAME_RE.match(part)
            if match and match.group(2).lower() in _HTML_OPAQUE_ELEMENTS:
                if match.group(1):
                    opaque_depth = max(0, opaque_depth - 1)
                elif not part.rstrip().endswith("/>"):
                    opaque_depth += 1
            out.append(part)
        elif opaque_depth:
            out.append(part)
        else:
            out.append(_mock_translation(part, target_language_code))
    return "".join(out)


def _xliff_qname(namespace_uri, tag):
    return f"{{{namespace_uri}}}{tag}" if namespace_uri else tag


def _translate_xliff(text, target_language_code):
    """Fill in every ``<trans-unit>``'s ``<target>`` from its ``<source>`` and
    stamp ``target-language`` on every ``<file>``.

    This is the shape an XLIFF round trip needs: the caller wrote source
    segments, and merges the translated ``<target>`` text back onto them by
    ``trans-unit`` id. Inline markup inside a ``<source>`` (``<g>``, ``<x/>``)
    is flattened to its text, because there is no real translation to
    re-distribute across it.

    The document comes from the caller's S3 bucket, so it is parsed with
    ``defusedxml`` like every other untrusted XML parse in the emulator.
    ``ET`` is still used to build and serialise, which is safe."""
    root = _xml_fromstring(text)
    namespace_uri = root.tag[1:].split("}")[0] if root.tag.startswith("{") else ""
    if namespace_uri:
        # Otherwise ElementTree serialises the default namespace as `ns0:`,
        # and an XLIFF tool reading the output would not recognise the
        # elements it just round-tripped.
        ET.register_namespace("", namespace_uri)

    file_tag = _xliff_qname(namespace_uri, "file")
    unit_tag = _xliff_qname(namespace_uri, "trans-unit")
    source_tag = _xliff_qname(namespace_uri, "source")
    target_tag = _xliff_qname(namespace_uri, "target")

    for file_el in root.iter(file_tag):
        file_el.set("target-language", target_language_code)

    for unit in root.iter(unit_tag):
        source = unit.find(source_tag)
        if source is None:
            continue
        source_text = "".join(source.itertext())
        target = unit.find(target_tag)
        if target is None:
            target = ET.Element(target_tag)
            unit.insert(list(unit).index(source) + 1, target)
        else:
            for child in list(target):
                target.remove(child)
        target.text = _mock_translation(source_text, target_language_code)
        target.tail = source.tail

    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _translate_document(body, content_type, target_language_code):
    """Return the translated bytes for one document, or None if the document
    cannot be read as its declared content type.

    None is a per-document error, not a job failure: AWS counts it in
    DocumentsWithErrorsCount and finishes the job COMPLETED_WITH_ERROR."""
    if content_type in _OPAQUE_CONTENT_TYPES:
        # A ZIP container. Rewriting one would mean rewriting the OOXML parts
        # inside it; copying it through keeps the document openable, which is
        # what a pipeline that just moves files around needs.
        return body

    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return None

    try:
        if content_type == _XLIFF:
            return _translate_xliff(text, target_language_code).encode("utf-8")
        if content_type == _HTML:
            return _translate_html(text, target_language_code).encode("utf-8")
        return _translate_plain_text(text, target_language_code).encode("utf-8")
    except (ET.ParseError, DefusedXmlException):
        # DefusedXmlException is not a ParseError subclass, so a document with a
        # forbidden DTD or entity has to be named explicitly here or it would
        # escape to _run_job and fail the whole job instead of just this file.
        return None


# ---------------------------------------------------------------------------
# Job execution
# ---------------------------------------------------------------------------


def _live_job(job_id, run_id):
    """The job this worker was started for, or None if it has been replaced.
    Job ids are generated per start, so this only differs from a plain lookup
    after a restore, but it keeps a restored record from being driven by a
    worker that no longer owns it."""
    job = _jobs.get(job_id)
    if job is None or job.get("_run_id") != run_id:
        return None
    return job


async def _wait(job_id, run_id, seconds):
    """Sleep in slices, returning False as soon as the job disappears or a stop
    is requested, so StopTextTranslationJob takes effect promptly instead of at
    the next phase boundary."""
    remaining = seconds
    while remaining > 0:
        await asyncio.sleep(min(_STOP_POLL_SECONDS, remaining))
        remaining -= _STOP_POLL_SECONDS
        job = _live_job(job_id, run_id)
        if job is None or job.get("JobStatus") == "STOP_REQUESTED":
            return False
    job = _live_job(job_id, run_id)
    return job is not None and job.get("JobStatus") != "STOP_REQUESTED"


def _finalize_stop(job_id, run_id):
    """Complete the transition StopTextTranslationJob started. The stop handler
    only records the request; the worker owns the job's terminal state, so it
    is the one that sets STOPPED and emits the event."""
    job = _live_job(job_id, run_id)
    if job is None or job.get("JobStatus") != "STOP_REQUESTED":
        return
    job["JobStatus"] = "STOPPED"
    job["EndTime"] = time.time()
    logger.info("Translate: job %s stopped", job_id)
    _emit_state_change(job_id, "STOPPED")


async def _run_job(job_id, run_id, account_id, region):
    """Walk a job to a terminal state. Runs as a background task, so it pins
    the account and region it was started for rather than inheriting whatever
    request happens to be in flight."""
    with request_scope(account_id, region):
        try:
            await _run_job_inner(job_id, run_id)
        except Exception:
            logger.exception("Translate: job %s crashed", job_id)
            job = _live_job(job_id, run_id)
            if job is not None and job.get("JobStatus") not in _TERMINAL_STATUSES:
                job["JobStatus"] = "FAILED"
                job["Message"] = "Internal Failure. Please try your request again."
                job["EndTime"] = time.time()
                _emit_state_change(job_id, "FAILED")


async def _run_job_inner(job_id, run_id):
    half = _JOB_RUN_SECONDS / 2 if _JOB_RUN_SECONDS > 0 else 0.0

    if not await _wait(job_id, run_id, half):
        _finalize_stop(job_id, run_id)
        return

    job = _live_job(job_id, run_id)
    if job is None:
        return
    job["JobStatus"] = "IN_PROGRESS"

    if not await _wait(job_id, run_id, half):
        _finalize_stop(job_id, run_id)
        return

    job = _live_job(job_id, run_id)
    if job is None:
        return

    _translate_job(job_id, run_id, job)


def _fail(job_id, run_id, job, message):
    if _live_job(job_id, run_id) is None:
        return
    job["JobStatus"] = "FAILED"
    job["Message"] = message
    job["EndTime"] = time.time()
    logger.info("Translate: job %s failed: %s", job_id, message)
    _emit_state_change(job_id, "FAILED")


def _input_documents(bucket, prefix):
    """Every readable object under the input prefix, oldest key first.

    Folder markers (a zero-length object whose key ends in "/") are S3 console
    artefacts, not documents, so they are skipped rather than translated into
    an empty output file."""
    from ministack.services import s3 as s3_svc

    store = s3_svc._buckets.get(bucket)
    if store is None:
        return None

    documents = []
    for key in sorted(store.get("objects", {})):
        if not key.startswith(prefix) or key.endswith("/"):
            continue
        relative = key[len(prefix):]
        if not relative:
            continue
        body = s3_svc._get_object_data(bucket, key)
        if body is None:
            continue
        documents.append((relative, body))
    return documents


def _translated_key(relative_key, target_language_code):
    """``folder/mySourceText.txt`` -> ``folder/fr.mySourceText.txt``. The
    language code goes on the file name, not the path, so a nested input layout
    is preserved in the output."""
    folder, separator, name = relative_key.rpartition("/")
    return f"{folder}{separator}{target_language_code}.{name}"


def _auxiliary_details(job, target_language_code, input_uri, output_uri, entries, characters):
    """The ``details/<code>.auxiliary-translation-details.json`` summary real
    Translate writes beside the output. Counts are strings there, so they are
    strings here."""
    source_code = job["SourceLanguageCode"]
    if source_code == "auto":
        source_code = _DEFAULT_SOURCE_LANGUAGE
    return {
        "sourceLanguageCode": source_code,
        "targetLanguageCode": target_language_code,
        "charactersTranslated": str(characters),
        "documentCountWithCustomerError": str(
            sum(1 for entry in entries if entry["targetFile"] is None)
        ),
        "documentCountWithServerError": "0",
        "inputDataPrefix": input_uri,
        "outputDataPrefix": output_uri,
        "details": [
            {
                "sourceFile": entry["sourceFile"],
                "targetFile": entry["targetFile"],
                "auxiliaryData": {"appliedTerminologies": []},
            }
            for entry in entries
            if entry["targetFile"] is not None
        ],
    }


def _translate_job(job_id, run_id, job):
    from ministack.services import s3 as s3_svc

    input_uri = job["InputDataConfig"]["S3Uri"]
    content_type = job["InputDataConfig"]["ContentType"]
    input_bucket, input_prefix = _parse_s3_uri(input_uri)

    documents = _input_documents(input_bucket, input_prefix)
    if documents is None:
        _fail(job_id, run_id, job, _INPUT_UNREADABLE)
        return
    if not documents:
        _fail(job_id, run_id, job, _NO_INPUT_DOCUMENTS)
        return

    output_bucket = job["_output_bucket"]
    output_prefix = job["_output_prefix"]
    if s3_svc._ensure_bucket(output_bucket) is None:
        _fail(job_id, run_id, job, _OUTPUT_UNWRITABLE)
        return

    def _write(key, body, write_content_type):
        response = s3_svc._put_object(
            output_bucket,
            key,
            body,
            {"content-type": write_content_type, "content-length": str(len(body))},
        )
        return not (isinstance(response, tuple) and response[0] >= 300)

    # A document that fails for one target language fails for all of them, so
    # the error count is per input document rather than per output file, which
    # is how JobDetails reports it.
    failed_documents = set()

    for target_language_code in job["TargetLanguageCodes"]:
        entries = []
        characters = 0
        for relative_key, body in documents:
            translated = _translate_document(body, content_type, target_language_code)
            if translated is None:
                failed_documents.add(relative_key)
                entries.append({"sourceFile": relative_key, "targetFile": None})
                logger.info(
                    "Translate: job %s could not read %s as %s", job_id, relative_key, content_type
                )
                continue
            target_key = _translated_key(relative_key, target_language_code)
            if not _write(f"{output_prefix}{target_key}", translated, content_type):
                _fail(job_id, run_id, job, _OUTPUT_UNWRITABLE)
                return
            characters += len(body)
            entries.append({"sourceFile": relative_key, "targetFile": target_key})

        output_uri = f"s3://{output_bucket}/{output_prefix}"
        details = _auxiliary_details(
            job, target_language_code, input_uri, output_uri, entries, characters
        )
        details_key = (
            f"{output_prefix}details/{target_language_code}.auxiliary-translation-details.json"
        )
        if not _write(details_key, json.dumps(details).encode("utf-8"), "application/json"):
            _fail(job_id, run_id, job, _OUTPUT_UNWRITABLE)
            return

    translated_count = len(documents) - len(failed_documents)
    job["JobDetails"] = {
        "TranslatedDocumentsCount": translated_count,
        "DocumentsWithErrorsCount": len(failed_documents),
        "InputDocumentsCount": len(documents),
    }
    status = "COMPLETED_WITH_ERROR" if failed_documents else "COMPLETED"
    job["JobStatus"] = status
    # AWS fills Message on success too, not only on failure — its documented
    # completion text is what a caller sees on a finished job.
    if failed_documents:
        job["Message"] = (
            f"Your job has completed with errors. {len(failed_documents)} of "
            f"{len(documents)} documents could not be processed."
        )
    else:
        job["Message"] = "Your job has completed successfully."
    job["EndTime"] = time.time()
    logger.info(
        "Translate: job %s %s, %d of %d document(s) translated into %s under s3://%s/%s",
        job_id,
        status.lower(),
        translated_count,
        len(documents),
        ", ".join(job["TargetLanguageCodes"]),
        output_bucket,
        output_prefix,
    )
    _emit_state_change(job_id, status)


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


def _validate_start(data):
    """Everything StartTextTranslationJob rejects before a job exists. Returns
    an error response, or None when the request is good."""
    name = data.get("JobName")
    if name is not None and not _JOB_NAME_RE.match(str(name)):
        return _invalid_parameter(
            f"1 validation error detected: Value '{name}' at 'jobName' failed to satisfy "
            "constraint: Member must satisfy regular expression pattern: "
            r"^([\p{L}\p{Z}\p{N}_.:/=+\-%@]*)$"
        )

    token = data.get("ClientToken")
    if not isinstance(token, str) or not _CLIENT_TOKEN_RE.match(token):
        return _invalid_parameter(
            f"1 validation error detected: Value '{token}' at 'clientToken' failed to satisfy "
            "constraint: Member must satisfy regular expression pattern: ^[a-zA-Z0-9-]+$"
        )

    role_arn = data.get("DataAccessRoleArn")
    if not isinstance(role_arn, str) or not _ROLE_ARN_RE.match(role_arn):
        return _invalid_parameter(
            f"1 validation error detected: Value '{role_arn}' at 'dataAccessRoleArn' failed to "
            "satisfy constraint: Member must satisfy regular expression pattern: "
            "arn:aws(-[^:]+)?:iam::[0-9]{12}:role/.+"
        )

    input_config = data.get("InputDataConfig")
    if not isinstance(input_config, dict):
        input_config = {}
    if _parse_s3_uri(input_config.get("S3Uri")) == (None, None):
        return _invalid_parameter(
            f"1 validation error detected: Value '{input_config.get('S3Uri')}' at "
            "'inputDataConfig.s3Uri' failed to satisfy constraint: Member must satisfy regular "
            r"expression pattern: s3://[a-z0-9][\.\-a-z0-9]{1,61}[a-z0-9](/.*)?"
        )

    content_type = input_config.get("ContentType")
    if content_type not in _SUPPORTED_CONTENT_TYPES:
        return _invalid_request(
            f"Unsupported content type: {content_type}. Amazon Translate supports the following "
            f"content types: {', '.join(_SUPPORTED_CONTENT_TYPES)}."
        )

    output_config = data.get("OutputDataConfig")
    if not isinstance(output_config, dict):
        output_config = {}
    if _parse_s3_uri(output_config.get("S3Uri")) == (None, None):
        return _invalid_parameter(
            f"1 validation error detected: Value '{output_config.get('S3Uri')}' at "
            "'outputDataConfig.s3Uri' failed to satisfy constraint: Member must satisfy regular "
            r"expression pattern: s3://[a-z0-9][\.\-a-z0-9]{1,61}[a-z0-9](/.*)?"
        )

    source = data.get("SourceLanguageCode")
    if not isinstance(source, str) or not 2 <= len(source) <= 5:
        return _invalid_parameter(
            f"1 validation error detected: Value '{source}' at 'sourceLanguageCode' failed to "
            "satisfy constraint: Member must have length between 2 and 5"
        )

    targets = data.get("TargetLanguageCodes")
    if not isinstance(targets, list) or not targets:
        return _invalid_parameter(
            "1 validation error detected: Value at 'targetLanguageCodes' failed to satisfy "
            "constraint: Member must have length greater than or equal to 1"
        )
    if len(targets) > _MAX_TARGET_LANGUAGES:
        return _invalid_parameter(
            f"1 validation error detected: Value at 'targetLanguageCodes' failed to satisfy "
            f"constraint: Member must have length less than or equal to {_MAX_TARGET_LANGUAGES}"
        )
    for target in targets:
        if not isinstance(target, str) or not 2 <= len(target) <= 5:
            return _invalid_parameter(
                f"1 validation error detected: Value '{target}' at "
                "'targetLanguageCodes' failed to satisfy constraint: Member must have length "
                "between 2 and 5"
            )
        if source != "auto" and target == source:
            return error_response_json(
                "UnsupportedLanguagePairException",
                f"Unsupported language pair: {source} to {target}.",
                400,
                {"SourceLanguageCode": source, "TargetLanguageCode": target},
            )
    return None


def _start_text_translation_job(data):
    error = _validate_start(data)
    if error is not None:
        return error

    # ClientToken is an idempotency token, so a retried start — an SDK retry, a
    # replayed EventBridge delivery — must resolve to the job the first call
    # created rather than a second copy of it writing to a second output folder.
    token = data["ClientToken"]
    existing_id = _client_tokens.get(token)
    if existing_id is not None:
        existing = _jobs.get(existing_id)
        if existing is not None:
            return json_response(
                {"JobId": existing["JobId"], "JobStatus": existing["JobStatus"]}
            )

    job_id = new_uuid().replace("-", "")
    output_bucket, output_prefix = _parse_s3_uri(data["OutputDataConfig"]["S3Uri"])
    job_output_prefix = _output_folder_prefix(job_id, output_prefix)

    run_id = new_uuid()
    job = {
        "JobId": job_id,
        "JobName": data.get("JobName"),
        "JobStatus": "SUBMITTED",
        "JobDetails": None,
        "SourceLanguageCode": data["SourceLanguageCode"],
        "TargetLanguageCodes": list(data["TargetLanguageCodes"]),
        "Message": None,
        "SubmittedTime": time.time(),
        "EndTime": None,
        "InputDataConfig": copy.deepcopy(data["InputDataConfig"]),
        # Reported as the rewritten location from the moment the job exists,
        # the way AWS does: the caller's S3Uri is where the folder goes, not
        # where the documents land, and a consumer reading the original prefix
        # would find nothing.
        "OutputDataConfig": dict(
            data["OutputDataConfig"], S3Uri=f"s3://{output_bucket}/{job_output_prefix}"
        ),
        "DataAccessRoleArn": data["DataAccessRoleArn"],
        "_run_id": run_id,
        "_client_token": token,
        "_output_bucket": output_bucket,
        "_output_prefix": job_output_prefix,
    }
    for member in _ECHOED_MEMBERS:
        value = data.get(member)
        job[member] = copy.deepcopy(value) if value is not None else None

    _jobs[job_id] = job
    _client_tokens[token] = job_id

    asyncio.create_task(_run_job(job_id, run_id, get_account_id(), get_region()))

    return json_response({"JobId": job_id, "JobStatus": job["JobStatus"]})


def _describe_text_translation_job(data):
    job_id = data.get("JobId")
    if not isinstance(job_id, str) or not job_id.strip():
        return _invalid_parameter(
            "1 validation error detected: Value null at 'jobId' failed to satisfy "
            "constraint: Member must not be null"
        )

    job = _jobs.get(job_id)
    if job is None:
        return _not_found(
            "The batch translation job that you requested could not be found. Check the job ID "
            "and try your request again."
        )

    return json_response({"TextTranslationJobProperties": _public_job(job)})


def _list_text_translation_jobs(data):
    job_filter = data.get("Filter")
    if not isinstance(job_filter, dict):
        job_filter = {}
    name = job_filter.get("JobName")
    status = job_filter.get("JobStatus")
    before = job_filter.get("SubmittedBeforeTime")
    after = job_filter.get("SubmittedAfterTime")

    if status is not None and status not in _JOB_STATUSES:
        return _invalid_parameter(
            f"1 validation error detected: Value '{status}' at 'filter.jobStatus' failed to "
            "satisfy constraint: Member must satisfy enum value set: "
            f"[{', '.join(_JOB_STATUSES)}]"
        )

    # Coerce the submission-time bounds up front: the wire form is a number, but
    # a caller can send anything, and filtering on an unparseable bound inside
    # the comprehension below would surface as a 500 only once a job exists.
    for field, value in (("submittedBeforeTime", before), ("submittedAfterTime", after)):
        if value is None:
            continue
        try:
            float(value)
        except (TypeError, ValueError):
            return _invalid_parameter(
                f"1 validation error detected: Value '{value}' at 'filter.{field}' failed to "
                "satisfy constraint: Member must be a timestamp"
            )

    # "You can only set one filter at a time." The two submission-time bounds
    # describe a single window, so they count as one filter between them.
    selected = sum(
        1 for criterion in (name, status, before if before is not None else after)
        if criterion is not None
    )
    if selected > 1:
        return error_response_json(
            "InvalidFilterException",
            "You can specify only one filter at a time. Remove the extra filters and try your "
            "request again.",
            400,
        )

    max_results = data.get("MaxResults")
    if max_results is None:
        max_results = 100
    else:
        try:
            max_results = int(max_results)
        except (TypeError, ValueError):
            return _invalid_parameter("MaxResults must be an integer")
        if max_results < 1 or max_results > 500:
            return _invalid_parameter(
                f"1 validation error detected: Value '{max_results}' at 'maxResults' failed to "
                "satisfy constraint: Member must have value less than or equal to 500"
            )

    cursor = None
    token = data.get("NextToken")
    if token:
        cursor = _decode_token(token)
        if cursor is None:
            return _invalid_parameter("The NextToken that you provided is invalid.")

    jobs = list(_jobs.values())
    if name:
        # AWS documents this only as "filters the list of jobs by name". A
        # prefix match is what the console's search box produces and what the
        # equivalent Comprehend filter does.
        needle = str(name).lower()
        jobs = [j for j in jobs if (j.get("JobName") or "").lower().startswith(needle)]
    if status:
        jobs = [j for j in jobs if j.get("JobStatus") == status]
    if before is not None:
        jobs = [j for j in jobs if (j.get("SubmittedTime") or 0.0) < float(before)]
    if after is not None:
        jobs = [j for j in jobs if (j.get("SubmittedTime") or 0.0) > float(after)]

    # Most recently submitted first, matching the console and the API.
    jobs.sort(key=_sort_key, reverse=True)
    if cursor is not None:
        jobs = [j for j in jobs if _sort_key(j) < cursor]

    page = jobs[:max_results]
    result = {"TextTranslationJobPropertiesList": [_public_job(j) for j in page]}
    if len(jobs) > max_results:
        result["NextToken"] = _encode_token(page[-1])
    return json_response(result)


def _stop_text_translation_job(data):
    job_id = data.get("JobId")
    if not isinstance(job_id, str) or not job_id.strip():
        return _invalid_parameter(
            "1 validation error detected: Value null at 'jobId' failed to satisfy "
            "constraint: Member must not be null"
        )

    job = _jobs.get(job_id)
    if job is None:
        return _not_found(
            "The batch translation job that you requested could not be found. Check the job ID "
            "and try your request again."
        )

    # A job that already finished is reported as it stands: AWS documents that
    # a job which completes before it can be stopped stays COMPLETED.
    if job["JobStatus"] in _TERMINAL_STATUSES or job["JobStatus"] == "STOP_REQUESTED":
        return json_response({"JobId": job_id, "JobStatus": job["JobStatus"]})

    job["JobStatus"] = "STOP_REQUESTED"
    return json_response({"JobId": job_id, "JobStatus": job["JobStatus"]})


_DISPATCH = {
    "StartTextTranslationJob": _start_text_translation_job,
    "DescribeTextTranslationJob": _describe_text_translation_job,
    "ListTextTranslationJobs": _list_text_translation_jobs,
    "StopTextTranslationJob": _stop_text_translation_job,
}


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("X-Amz-Target") or headers.get("x-amz-target") or ""
    action = target.split(".", 1)[1] if "." in target else target
    if not action:
        return error_response_json("InvalidAction", "missing X-Amz-Target", 400)

    body_text = body.decode("utf-8") if isinstance(body, bytes) else (body or "")
    try:
        payload = json.loads(body_text) if body_text else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "invalid JSON body", 400)
    if not isinstance(payload, dict):
        return error_response_json("SerializationException", "invalid JSON body", 400)

    fn = _DISPATCH.get(action)
    if fn is None:
        return error_response_json(
            "InvalidAction",
            f"Operation '{action}' not implemented",
            400,
        )

    logger.debug("Translate: %s %s", action, body_text)
    return fn(payload)
