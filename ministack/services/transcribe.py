"""
Amazon Transcribe emulator.

JSON 1.1 protocol with X-Amz-Target prefix ``Transcribe``
(``transcribe-2017-10-26``).

Implemented:
  StartTranscriptionJob, GetTranscriptionJob, ListTranscriptionJobs,
  DeleteTranscriptionJob.

Jobs run as a background task that walks QUEUED -> IN_PROGRESS -> COMPLETED
over ``TRANSCRIBE_JOB_RUN_SECONDS`` (0 completes immediately), reads the media
object out of MiniStack's S3 store, and writes a transcript document back to
S3 in the real Transcribe result format. Entering a terminal state publishes a
``Transcribe Job State Change`` event so EventBridge rules downstream of a
transcription job fire the way they do on AWS.

The transcript document carries the structures the real service produces:
``results.transcripts``, word-level ``results.items``, ``results.segments``
when ``Settings.ShowAlternatives`` is set, ``results.audio_segments``, and
``results.speaker_labels`` when ``Settings.ShowSpeakerLabels`` is set.

Language identification is resolved rather than ignored. ``IdentifyLanguage``
sets ``LanguageCode`` and ``IdentifiedLanguageScore``; ``IdentifyMultipleLanguages``
sets ``LanguageCodes``, which is the only place the resolved code appears, as
on AWS. Both pick deterministically from ``LanguageOptions``.

``ContentRedaction`` drives the output layout the way AWS does: ``redacted``
writes only ``redacted-<key>``, ``redacted_and_unredacted`` writes both.
``Subtitles`` writes real WebVTT and SubRip files alongside the transcript and
reports them as ``SubtitleFileUris``. Subtitles carry the transcript text, so a
redacted job's subtitle files are redacted and named to match.

There is no speech recognition here. The transcript text is deterministic
canned output derived from the media URI, following the same approach as
``bedrock_runtime._mock_reply``: identical input gives identical output, and
different input gives distinguishable output. Speaker labels and alternatives
are synthesised from that text, so they are well-formed and stable rather
than meaningful.

Deferred:
  Streaming transcription (a separate endpoint), call analytics and medical
  job families, custom vocabularies / vocabulary filters / language models,
  and TagResource / UntagResource / ListTagsForResource. ``ModelSettings``,
  ``JobExecutionSettings``, ``LanguageIdSettings``, ``ToxicityDetection`` and
  ``Tags`` round-trip on the job record without changing the output.
"""

import asyncio
import base64
import contextvars
import copy
import hashlib
import json
import logging
import os
import re
import time
from urllib.parse import urlparse

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

logger = logging.getLogger("transcribe")

# How long a job spends between QUEUED and COMPLETED. Same knob shape as
# GLUE_CRAWLER_RUN_SECONDS: tests that assert on IN_PROGRESS need a non-zero
# value, tests that just want a result set it to 0.
_JOB_RUN_SECONDS = float(os.environ.get("TRANSCRIBE_JOB_RUN_SECONDS", "2"))

# Where transcripts land when the caller supplies no OutputBucketName. Real
# Transcribe uses a service-managed bucket and hands back a presigned URL; the
# local equivalent is a bucket MiniStack owns, so the URI is always fetchable.
_SERVICE_BUCKET = "ministack-transcribe-output"

_TERMINAL_STATUSES = ("COMPLETED", "FAILED")

_MEDIA_UNREADABLE = (
    "The S3 URI that you provided can't be accessed. Make sure that you have "
    "read permission and try your request again."
)

# AWS publishes the pattern as ^[0-9a-zA-Z._-]+$ with no bound; the 1-200
# length limit comes from the shape's min/max rather than the pattern itself.
_JOB_NAME_RE = re.compile(r"^[0-9a-zA-Z._-]{1,200}$")

_HOST_SANITY_RE = re.compile(r"^[A-Za-z0-9.\-\[\]:]{1,255}$")

# Virtual-hosted-style S3 host, e.g. "my-bucket.s3.ap-southeast-2.amazonaws.com".
# Path-style hosts ("s3-ap-southeast-2.amazonaws.com") deliberately do not
# match: they have no bucket component before the ".s3" label.
_S3_VIRTUAL_HOST_RE = re.compile(
    r"^(?P<bucket>[a-z0-9][a-z0-9.\-]*?)\.s3(?:[.-][a-z0-9-]+)*\.(?:amazonaws\.com|localhost)$"
)

# The pattern AWS reports when Media.MediaFileUri is rejected.
_MEDIA_URI_PATTERN = "(s3://|http(s*)://).+"

_DEFAULT_HOST = os.environ.get("MINISTACK_HOST", "localhost")
_DEFAULT_PORT = os.environ.get("GATEWAY_PORT", os.environ.get("EDGE_PORT", "4566"))

# The caller's Host header, so TranscriptFileUri points at an address the
# caller can actually reach. A Lambda container calling in over the compose
# network reaches MiniStack by service name, not by localhost. Same approach
# as sqs._request_host.
_request_host: contextvars.ContextVar[str] = contextvars.ContextVar(
    "_transcribe_request_host", default=""
)

_jobs = AccountRegionScopedDict()  # job_name -> job record

# Public members of the TranscriptionJob shape, in the order botocore models
# them. Internal bookkeeping lives under keys prefixed with "_" and never
# reaches the wire.
_JOB_MEMBERS = (
    "TranscriptionJobName",
    "TranscriptionJobStatus",
    "LanguageCode",
    "MediaSampleRateHertz",
    "MediaFormat",
    "Media",
    "Transcript",
    "StartTime",
    "CreationTime",
    "CompletionTime",
    "FailureReason",
    "Settings",
    "ModelSettings",
    "JobExecutionSettings",
    "ContentRedaction",
    "IdentifyLanguage",
    "IdentifyMultipleLanguages",
    "LanguageOptions",
    "IdentifiedLanguageScore",
    "LanguageCodes",
    "Tags",
    "Subtitles",
    "LanguageIdSettings",
    "ToxicityDetection",
)

# Members echoed straight back from the request without changing behaviour.
_ECHOED_MEMBERS = (
    "ModelSettings",
    "JobExecutionSettings",
    "LanguageIdSettings",
    "ToxicityDetection",
    "Tags",
    "LanguageOptions",
    "ContentRedaction",
)

# Fallback when a language-identification job supplies no LanguageOptions to
# choose from. AWS would infer it from the audio; there is no audio here.
_DEFAULT_LANGUAGE = "en-US"

# Fixed score for an identified language. Real Transcribe reports its
# confidence; a mock has none, so it reports certainty rather than inventing a
# number that varies.
_IDENTIFIED_LANGUAGE_SCORE = 1.0

_SUMMARY_MEMBERS = (
    "TranscriptionJobName",
    "CreationTime",
    "StartTime",
    "CompletionTime",
    "LanguageCode",
    "LanguageCodes",
    "TranscriptionJobStatus",
    "FailureReason",
    "OutputLocationType",
    "ContentRedaction",
    "ModelSettings",
    "IdentifyLanguage",
    "IdentifyMultipleLanguages",
    "IdentifiedLanguageScore",
    "ToxicityDetection",
)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


def reset():
    _jobs.clear()


def get_state():
    return copy.deepcopy({"jobs": _jobs})


def restore_state(data):
    if not data:
        return
    _jobs.clear()
    jobs = data.get("jobs")
    # Not `or {}`: AccountRegionScopedDict.__bool__ is scope-relative, and
    # restore runs in the default account/region. A snapshot holding jobs only
    # in other scopes would read as falsy and every one of them would be lost.
    if jobs is not None:
        _jobs.update(jobs)
    _fail_orphaned_jobs()


def load_persisted_state(data):
    restore_state(data)


def _fail_orphaned_jobs():
    """A job that was mid-flight when the process stopped has no worker any
    more. Leaving it QUEUED or IN_PROGRESS strands every caller polling
    GetTranscriptionJob forever, so it is failed the way AWS fails a job it
    cannot finish."""
    for job in _jobs.all_values():
        if job.get("TranscriptionJobStatus") not in _TERMINAL_STATUSES:
            job["TranscriptionJobStatus"] = "FAILED"
            job["FailureReason"] = "Internal Failure. The job did not survive a MiniStack restart."
            job["CompletionTime"] = time.time()

try:
    _restored = load_state("transcribe")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted Transcribe state; continuing with fresh store")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bad_request(message):
    return error_response_json("BadRequestException", message, 400)


def _public_job(job):
    """Project a stored job onto the TranscriptionJob wire shape, dropping
    members AWS omits when they have no value."""
    return {k: job[k] for k in _JOB_MEMBERS if job.get(k) is not None}


def _public_summary(job):
    return {k: job[k] for k in _SUMMARY_MEMBERS if job.get(k) is not None}


def _external_netloc():
    host = _request_host.get()
    if host and _HOST_SANITY_RE.match(host):
        return host
    return f"{_DEFAULT_HOST}:{_DEFAULT_PORT}"


def _parse_s3_uri(uri):
    """Return (bucket, key) for an S3 location, or (None, None).

    AWS models Media.MediaFileUri as ``(s3://|http(s*)://).+``, so an HTTPS S3
    URL is as valid as an ``s3://`` one and SDK callers do emit both. Both
    path-style (``s3.<region>.amazonaws.com/<bucket>/<key>``) and
    virtual-hosted style (``<bucket>.s3.<region>.amazonaws.com/<key>``) are
    accepted; an unrecognised host falls back to path-style, which is how a
    MiniStack gateway URL resolves."""
    if not isinstance(uri, str):
        return None, None

    parsed = urlparse(uri)
    scheme = (parsed.scheme or "").lower()

    if scheme == "s3":
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
    elif scheme in ("http", "https"):
        path = parsed.path.lstrip("/")
        match = _S3_VIRTUAL_HOST_RE.match(parsed.hostname or "")
        if match:
            bucket, key = match.group("bucket"), path
        else:
            bucket, _, key = path.partition("/")
    else:
        return None, None

    if not bucket or not key:
        return None, None
    return bucket, key


_SEGMENT_WORDS = 6
_WORD_SECONDS = 0.5
_MAX_SPEAKERS = 10


def _mock_transcript(media_uri, redacted=False):
    """Deterministic canned transcript. The digest of the media URI keeps
    output for different media distinguishable while staying stable across
    runs, which is the whole point of using fixtures instead of real ASR.

    Long enough to split into several segments, so speaker labels, alternatives
    and multi-language spans have something to distribute across rather than
    collapsing onto one segment.

    The redacted variant masks the digest, so it is the identifying part of
    the text that disappears, the way real redaction removes PII."""
    if redacted:
        subject = "[PII]"
    else:
        digest = hashlib.sha256(media_uri.encode("utf-8")).hexdigest()[:8]
        subject = f"media#{digest}"
    return (
        f"[ministack mock transcript] deterministic placeholder speech for {subject} "
        "generated locally by ministack with no speech recognition performed on the audio"
    )


def _fmt_time(seconds):
    return f"{seconds:.1f}"


def _word_items(text):
    """Word-level items in Transcribe's result format, with synthetic timings
    so consumers that walk `results.items` have something well-formed to walk.
    Any `language_code` tag is stamped on by the caller, which knows which
    segment each item belongs to."""
    items = []
    for index, word in enumerate(text.split()):
        start = index * _WORD_SECONDS
        item = {
            "type": "pronunciation",
            "start_time": _fmt_time(start),
            "end_time": _fmt_time(start + _WORD_SECONDS),
            "alternatives": [{"confidence": "1.0", "content": word}],
        }
        items.append(item)
    return items


def _alternatives(transcript, items, max_alternatives):
    """The primary reading plus synthetic runners-up. Real Transcribe returns
    competing hypotheses when ShowAlternatives is set; there is no recogniser
    here to disagree with itself, so the extras are labelled as what they are
    rather than dressed up as genuine alternatives."""
    alternatives = [{"transcript": transcript, "items": items}]
    for rank in range(1, max(1, min(max_alternatives, 10))):
        alternatives.append({"transcript": f"{transcript} [alternative {rank}]", "items": items})
    return alternatives


def _transcript_document(job_name, text, settings, language_codes):
    """Build the transcript JSON in the shape real Transcribe writes to S3.

    `segments` appears only under ShowAlternatives and `speaker_labels` only
    under ShowSpeakerLabels, matching the service: a consumer that never asked
    for diarisation should not find it there."""
    settings = settings or {}
    show_speakers = bool(settings.get("ShowSpeakerLabels"))
    show_alternatives = bool(settings.get("ShowAlternatives"))
    max_alternatives = settings.get("MaxAlternatives") or 2
    max_speakers = max(1, min(settings.get("MaxSpeakerLabels") or 2, _MAX_SPEAKERS))

    # Only tag items with a language when more than one was resolved; a
    # single-language job carries the code on the job record instead. The tag
    # is applied per segment below, so an item always agrees with the audio
    # segment that owns it.
    multi_language = len(language_codes) > 1
    items = _word_items(text)

    words = text.split()
    groups = [
        list(range(start, min(start + _SEGMENT_WORDS, len(items))))
        for start in range(0, len(items), _SEGMENT_WORDS)
    ]

    segments = []
    audio_segments = []
    speaker_segments = []

    for position, indices in enumerate(groups):
        segment_text = " ".join(words[indices[0] : indices[-1] + 1])
        segment_items = [items[i] for i in indices]
        start_time = segment_items[0]["start_time"]
        end_time = segment_items[-1]["end_time"]
        speaker_label = f"spk_{position % max_speakers}"
        segment_language = language_codes[position % len(language_codes)] if language_codes else None
        if multi_language:
            for item in segment_items:
                item["language_code"] = segment_language

        segments.append(
            {
                "start_time": start_time,
                "end_time": end_time,
                "alternatives": _alternatives(segment_text, segment_items, max_alternatives),
            }
        )

        audio_segment = {
            "id": position,
            "transcript": segment_text,
            "start_time": start_time,
            "end_time": end_time,
            "items": indices,
        }
        if language_codes:
            audio_segment["language_code"] = segment_language
        if show_speakers:
            audio_segment["speaker_label"] = speaker_label
        audio_segments.append(audio_segment)

        if show_speakers:
            speaker_segments.append(
                {
                    "start_time": start_time,
                    "end_time": end_time,
                    "speaker_label": speaker_label,
                    "items": [
                        {
                            "start_time": item["start_time"],
                            "end_time": item["end_time"],
                            "speaker_label": speaker_label,
                        }
                        for item in segment_items
                    ],
                }
            )

    results = {
        "transcripts": [{"transcript": text}],
        "items": items,
        "audio_segments": audio_segments,
    }
    if show_alternatives:
        results["segments"] = segments
    if show_speakers:
        results["speaker_labels"] = {
            "speakers": min(len(groups), max_speakers),
            "segments": speaker_segments,
        }

    return {
        "jobName": job_name,
        "accountId": get_account_id(),
        "status": "COMPLETED",
        "results": results,
    }


# ---------------------------------------------------------------------------
# Subtitles
# ---------------------------------------------------------------------------


def _subtitle_cue_time(seconds, separator):
    hours, remainder = divmod(float(seconds), 3600)
    minutes, secs = divmod(remainder, 60)
    whole, fraction = divmod(secs, 1)
    return f"{int(hours):02d}:{int(minutes):02d}:{int(whole):02d}{separator}{int(fraction * 1000):03d}"


def _render_subtitles(document, fmt, start_index):
    """Render the transcript's audio segments as SubRip or WebVTT."""
    srt = fmt == "srt"
    separator = "," if srt else "."
    blocks = []
    for offset, segment in enumerate(document["results"]["audio_segments"]):
        start = _subtitle_cue_time(segment["start_time"], separator)
        end = _subtitle_cue_time(segment["end_time"], separator)
        blocks.append(
            f"{offset + start_index}\n{start} --> {end}\n{segment['transcript']}\n"
        )
    body = "\n".join(blocks)
    return body if srt else f"WEBVTT\n\n{body}"


def _resolve_languages(job):
    """The language codes a language-identification job would have detected.

    There is no audio to analyse, so the choice comes from LanguageOptions in
    request order, which keeps it deterministic and lets a test pin the
    outcome by listing the language it wants first."""
    if job.get("LanguageCode"):
        return [job["LanguageCode"]]
    # Duplicates would otherwise report the same language twice, each with the
    # full duration, so the spans sum to more audio than the job contains.
    options = []
    for code in job.get("LanguageOptions") or []:
        if isinstance(code, str) and code not in options:
            options.append(code)
    if job.get("IdentifyMultipleLanguages"):
        return options or [_DEFAULT_LANGUAGE]
    return [options[0]] if options else [_DEFAULT_LANGUAGE]


def _apply_languages(job, codes, document):
    """Record the resolved language on the job the way AWS does.

    IdentifyLanguage fills LanguageCode and IdentifiedLanguageScore.
    IdentifyMultipleLanguages fills LanguageCodes and leaves LanguageCode
    unset, so a caller that wants the source language has to check both. This
    only happens once the job completes; AWS cannot know the language at
    submission time, so neither does this."""
    if job.get("LanguageCode"):
        return
    if job.get("IdentifyMultipleLanguages"):
        durations = {}
        for segment in document["results"]["audio_segments"]:
            code = segment.get("language_code")
            span = float(segment["end_time"]) - float(segment["start_time"])
            durations[code] = round(durations.get(code, 0.0) + span, 1)
        job["LanguageCodes"] = [
            {"LanguageCode": code, "DurationInSeconds": durations.get(code, 0.0)}
            for code in codes
        ]
    else:
        job["LanguageCode"] = codes[0]
        job["IdentifiedLanguageScore"] = _IDENTIFIED_LANGUAGE_SCORE


def _redacted_key(key):
    """Redacted output sits beside the transcript with `redacted-` prefixed to
    the file name, not to the whole key, so it stays in the same prefix."""
    prefix, separator, name = key.rpartition("/")
    return f"{prefix}{separator}redacted-{name}"


def _subtitle_key(key, fmt):
    base = key[: -len(".json")] if key.endswith(".json") else key
    return f"{base}.{fmt}"


def _emit_state_change(job_name, status):
    """Publish `Transcribe Job State Change`, the event AWS emits when a job
    reaches a terminal state. Pipelines that chain off a completed
    transcription depend on it, so a failure to deliver is logged rather than
    swallowed silently, and never propagates into the job itself."""
    try:
        from ministack.services import eventbridge as _eb

        _eb._dispatch_event(
            {
                "EventId": new_uuid(),
                "Source": "aws.transcribe",
                "DetailType": "Transcribe Job State Change",
                "Detail": json.dumps(
                    {
                        "TranscriptionJobName": job_name,
                        "TranscriptionJobStatus": status,
                    }
                ),
                "EventBusName": "default",
                "Time": now_iso(),
                "Resources": [],
                "Account": get_account_id(),
                "Region": get_region(),
            }
        )
    except Exception:
        logger.exception("Transcribe: failed to publish state change for job %s", job_name)


def _sort_key(job):
    """Creation time with the job name breaking ties, so the ordering is total
    and stable across calls. Callers reverse it to get newest first."""
    return (job.get("CreationTime") or 0.0, job.get("TranscriptionJobName") or "")


def _encode_token(job):
    """Tokens carry the sort position of the last item returned, not an index.
    An offset would skip an item whenever a job was deleted between pages and
    repeat one whenever a job was created."""
    created, name = _sort_key(job)
    raw = json.dumps({"ct": created, "n": name}).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_token(token):
    try:
        raw = json.loads(base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8"))
        return (float(raw["ct"]), str(raw["n"]))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Job execution
# ---------------------------------------------------------------------------


def _live_job(job_name, run_id):
    """The job this worker was started for, or None if it has been deleted or
    replaced. DeleteTranscriptionJob does not cancel a running worker, so a
    job re-created under the same name would otherwise be driven by both its
    own worker and the orphaned one: completing early, writing the transcript
    twice and emitting the state change event twice."""
    job = _jobs.get(job_name)
    if job is None or job.get("_run_id") != run_id:
        return None
    return job


async def _run_job(job_name, run_id, account_id, region, netloc):
    """Walk a job to a terminal state. Runs as a background task, so it pins
    the account and region it was started for rather than inheriting whatever
    request happens to be in flight."""
    with request_scope(account_id, region):
        try:
            await _run_job_inner(job_name, run_id, netloc)
        except Exception:
            logger.exception("Transcribe: job %s crashed", job_name)
            job = _live_job(job_name, run_id)
            if job is not None and job.get("TranscriptionJobStatus") not in _TERMINAL_STATUSES:
                job["TranscriptionJobStatus"] = "FAILED"
                job["FailureReason"] = "Internal Failure. Please try your request again."
                job["CompletionTime"] = time.time()
                _emit_state_change(job_name, "FAILED")


async def _run_job_inner(job_name, run_id, netloc):
    half = _JOB_RUN_SECONDS / 2 if _JOB_RUN_SECONDS > 0 else 0

    if half:
        await asyncio.sleep(half)

    job = _live_job(job_name, run_id)
    if job is None:
        return

    job["TranscriptionJobStatus"] = "IN_PROGRESS"
    job["StartTime"] = time.time()

    if half:
        await asyncio.sleep(half)

    job = _live_job(job_name, run_id)
    if job is None:
        return

    def _fail(reason):
        if _live_job(job_name, run_id) is None:
            return
        job["TranscriptionJobStatus"] = "FAILED"
        job["FailureReason"] = reason
        job["CompletionTime"] = time.time()
        logger.info("Transcribe: job %s failed: %s", job_name, reason)
        _emit_state_change(job_name, "FAILED")

    from ministack.services import s3 as s3_svc

    media_uri = job["Media"]["MediaFileUri"]
    media_bucket, media_key = _parse_s3_uri(media_uri)
    if s3_svc._get_object_data(media_bucket, media_key) is None:
        _fail(_MEDIA_UNREADABLE)
        return

    text = _mock_transcript(media_uri)
    codes = _resolve_languages(job)
    document = _transcript_document(job_name, text, job.get("Settings"), codes)

    bucket = job["_output_bucket"]
    key = job["_output_key"]

    if job["OutputLocationType"] == "SERVICE_BUCKET" and s3_svc._ensure_bucket(bucket) is None:
        s3_svc._create_bucket(bucket, b"")

    def _write(target_key, payload, content_type):
        body = payload.encode("utf-8")
        resp = s3_svc._put_object(
            bucket,
            target_key,
            body,
            {"content-type": content_type, "content-length": str(len(body))},
        )
        if isinstance(resp, tuple) and resp[0] >= 300:
            return False
        return True

    # RedactionOutput decides the layout: "redacted" replaces the transcript
    # with the masked one, "redacted_and_unredacted" writes both. Transcript
    # then carries whichever URIs actually exist.
    redaction = job.get("ContentRedaction") or {}
    redaction_output = str(redaction.get("RedactionOutput") or "").lower()
    transcript_uris = {}
    writes = []
    subtitle_document = document
    subtitle_base_key = key

    if redaction:
        redacted_document = _transcript_document(
            job_name, _mock_transcript(media_uri, redacted=True), job.get("Settings"), codes
        )
        redacted_key = _redacted_key(key)
        writes.append((redacted_key, json.dumps(redacted_document), "application/json"))
        transcript_uris["RedactedTranscriptFileUri"] = f"http://{netloc}/{bucket}/{redacted_key}"
        # Subtitles carry the transcript text, so a redacted job's subtitle
        # files have to be redacted too, and named for what they contain.
        # Otherwise the one layout whose point is that the raw text never
        # reaches the bucket writes the raw text to the bucket.
        subtitle_document = redacted_document
        subtitle_base_key = redacted_key
        if redaction_output == "redacted_and_unredacted":
            writes.append((key, json.dumps(document), "application/json"))
            transcript_uris["TranscriptFileUri"] = f"http://{netloc}/{bucket}/{key}"
    else:
        writes.append((key, json.dumps(document), "application/json"))
        transcript_uris["TranscriptFileUri"] = f"http://{netloc}/{bucket}/{key}"

    subtitles = job.get("_subtitles_request") or {}
    subtitle_formats = [f for f in (subtitles.get("Formats") or []) if f in ("vtt", "srt")]
    subtitle_start = subtitles.get("OutputStartIndex")
    subtitle_start = 0 if subtitle_start is None else int(subtitle_start)
    subtitle_uris = []
    for fmt in subtitle_formats:
        subtitle_key = _subtitle_key(subtitle_base_key, fmt)
        writes.append(
            (subtitle_key, _render_subtitles(subtitle_document, fmt, subtitle_start), "text/plain")
        )
        subtitle_uris.append(f"http://{netloc}/{bucket}/{subtitle_key}")

    for target_key, payload, content_type in writes:
        if not _write(target_key, payload, content_type):
            _fail(
                f"The output bucket that you provided can't be written to: "
                f"s3://{bucket}/{target_key}. "
                "Make sure that the bucket exists and try your request again."
            )
            return

    _apply_languages(job, codes, document)
    if subtitle_formats:
        job["Subtitles"] = {
            "Formats": subtitle_formats,
            "SubtitleFileUris": subtitle_uris,
            "OutputStartIndex": subtitle_start,
        }
    job["Transcript"] = transcript_uris
    job["TranscriptionJobStatus"] = "COMPLETED"
    job["CompletionTime"] = time.time()
    logger.info(
        "Transcribe: job %s completed, %d object(s) written to s3://%s",
        job_name,
        len(writes),
        bucket,
    )
    _emit_state_change(job_name, "COMPLETED")


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


def _start_transcription_job(data):
    name = data.get("TranscriptionJobName")
    if not isinstance(name, str) or not name.strip():
        return _bad_request(
            "1 validation error detected: Value null at 'transcriptionJobName' "
            "failed to satisfy constraint: Member must not be null"
        )
    if not _JOB_NAME_RE.match(name):
        return _bad_request(
            f"1 validation error detected: Value '{name}' at 'transcriptionJobName' failed to "
            "satisfy constraint: Member must satisfy regular expression pattern: ^[0-9a-zA-Z._-]+$"
        )

    media = data.get("Media")
    if not isinstance(media, dict) or not media.get("MediaFileUri"):
        return _bad_request(
            "1 validation error detected: Value null at 'media' failed to satisfy "
            "constraint: Member must not be null"
        )

    media_uri = media["MediaFileUri"]
    if _parse_s3_uri(media_uri) == (None, None):
        return _bad_request(
            f"1 validation error detected: Value '{media_uri}' at 'media.mediaFileUri' failed "
            "to satisfy constraint: Member must satisfy regular expression pattern: "
            f"{_MEDIA_URI_PATTERN}"
        )

    language_code = data.get("LanguageCode")
    identify = data.get("IdentifyLanguage") or data.get("IdentifyMultipleLanguages")
    if not language_code and not identify:
        return _bad_request(
            "You must include either LanguageCode or IdentifyLanguage or "
            "IdentifyMultipleLanguages in your request. Please try your request again."
        )

    if name in _jobs:
        return error_response_json(
            "ConflictException",
            "The requested job name already exists. Use a different job name.",
            400,
        )

    output_bucket = data.get("OutputBucketName")
    if output_bucket:
        output_key = data.get("OutputKey") or f"{name}.json"
        # AWS treats a key ending in "/" as a prefix and appends the job name.
        if output_key.endswith("/"):
            output_key = f"{output_key}{name}.json"
        location_type = "CUSTOMER_BUCKET"
    else:
        output_bucket = _SERVICE_BUCKET
        # Region-qualified: S3 buckets are account-scoped but jobs are
        # region-scoped, so same-named jobs in two regions are two jobs and
        # must not resolve to one transcript object.
        output_key = f"{get_account_id()}/{get_region()}/{name}.json"
        location_type = "SERVICE_BUCKET"

    run_id = new_uuid()
    job = {
        "TranscriptionJobName": name,
        "TranscriptionJobStatus": "QUEUED",
        "LanguageCode": language_code,
        "MediaSampleRateHertz": data.get("MediaSampleRateHertz"),
        "MediaFormat": data.get("MediaFormat"),
        "Media": copy.deepcopy(media),
        "Transcript": None,
        "StartTime": None,
        "CreationTime": time.time(),
        "CompletionTime": None,
        "FailureReason": None,
        "Settings": copy.deepcopy(data.get("Settings")) if data.get("Settings") else None,
        # The identification flags are echoed as sent; the codes they resolve
        # to are filled in by the worker, because AWS cannot know the language
        # until the job has run.
        "IdentifyLanguage": data.get("IdentifyLanguage"),
        "IdentifyMultipleLanguages": data.get("IdentifyMultipleLanguages"),
        "IdentifiedLanguageScore": None,
        "LanguageCodes": None,
        "Subtitles": None,
        "OutputLocationType": location_type,
        "_run_id": run_id,
        "_output_bucket": output_bucket,
        "_output_key": output_key,
    }
    for member in _ECHOED_MEMBERS:
        value = data.get(member)
        job[member] = copy.deepcopy(value) if value is not None else None
    # Subtitles is requested with Formats/OutputStartIndex but reported as
    # Formats/SubtitleFileUris/OutputStartIndex, so it is held aside until the
    # worker has written the files and can report where they landed.
    job["_subtitles_request"] = copy.deepcopy(data.get("Subtitles")) or None
    _jobs[name] = job

    asyncio.create_task(
        _run_job(name, run_id, get_account_id(), get_region(), _external_netloc())
    )

    return json_response({"TranscriptionJob": _public_job(job)})


def _get_transcription_job(data):
    name = data.get("TranscriptionJobName")
    if not isinstance(name, str) or not name.strip():
        return _bad_request(
            "1 validation error detected: Value null at 'transcriptionJobName' "
            "failed to satisfy constraint: Member must not be null"
        )

    job = _jobs.get(name)
    if job is None:
        return error_response_json(
            "NotFoundException",
            "The requested job couldn't be found. Check the job name and try your request again.",
            400,
        )

    return json_response({"TranscriptionJob": _public_job(job)})


def _list_transcription_jobs(data):
    status = data.get("Status")
    if status is not None and status not in ("QUEUED", "IN_PROGRESS", "FAILED", "COMPLETED"):
        return _bad_request(
            f"1 validation error detected: Value '{status}' at 'status' failed to satisfy "
            "constraint: Member must satisfy enum value set: "
            "[COMPLETED, IN_PROGRESS, QUEUED, FAILED]"
        )

    contains = data.get("JobNameContains")

    max_results = data.get("MaxResults")
    if max_results is None:
        max_results = 100
    else:
        try:
            max_results = int(max_results)
        except (TypeError, ValueError):
            return _bad_request("MaxResults must be an integer")
        if max_results < 1 or max_results > 100:
            return _bad_request(
                f"1 validation error detected: Value '{max_results}' at 'maxResults' failed to "
                "satisfy constraint: Member must have value less than or equal to 100"
            )

    cursor = None
    token = data.get("NextToken")
    if token:
        cursor = _decode_token(token)
        if cursor is None:
            return _bad_request("The NextToken that you provided is invalid.")

    jobs = list(_jobs.values())
    if status:
        jobs = [j for j in jobs if j.get("TranscriptionJobStatus") == status]
    if contains:
        # AWS documents this search as case insensitive.
        needle = str(contains).lower()
        jobs = [j for j in jobs if needle in j["TranscriptionJobName"].lower()]

    # Most recently created first, matching the console and the API.
    jobs.sort(key=_sort_key, reverse=True)
    if cursor is not None:
        jobs = [j for j in jobs if _sort_key(j) < cursor]

    page = jobs[:max_results]
    result = {"TranscriptionJobSummaries": [_public_summary(j) for j in page]}
    if status:
        result["Status"] = status
    if len(jobs) > max_results:
        result["NextToken"] = _encode_token(page[-1])
    return json_response(result)


def _delete_transcription_job(data):
    name = data.get("TranscriptionJobName")
    if not isinstance(name, str) or not name.strip():
        return _bad_request(
            "1 validation error detected: Value null at 'transcriptionJobName' "
            "failed to satisfy constraint: Member must not be null"
        )

    # botocore models no NotFoundException on this operation: deleting a job
    # that isn't there succeeds.
    _jobs.pop(name, None)
    return json_response({})


_DISPATCH = {
    "StartTranscriptionJob": _start_transcription_job,
    "GetTranscriptionJob": _get_transcription_job,
    "ListTranscriptionJobs": _list_transcription_jobs,
    "DeleteTranscriptionJob": _delete_transcription_job,
}


async def handle_request(method, path, headers, body, query_params):
    _request_host.set(headers.get("host", "") or headers.get("Host", ""))

    target = headers.get("X-Amz-Target") or headers.get("x-amz-target") or ""
    action = target.split(".", 1)[1] if "." in target else target
    if not action:
        return error_response_json("InvalidAction", "missing X-Amz-Target", 400)

    body_text = body.decode("utf-8") if isinstance(body, bytes) else (body or "")
    try:
        payload = json.loads(body_text) if body_text else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "invalid JSON body", 400)

    fn = _DISPATCH.get(action)
    if fn is None:
        return error_response_json(
            "InvalidAction",
            f"Operation '{action}' not implemented",
            400,
        )

    logger.debug("Transcribe: %s %s", action, body_text)
    return fn(payload)
