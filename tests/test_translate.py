import json
import os
import time
import uuid

import pytest
from botocore.exceptions import ClientError

_ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")

_ROLE_ARN = "arn:aws:iam::000000000000:role/TranslateBatchRole"

# Mirrors the server's own default for TRANSLATE_JOB_RUN_SECONDS, so a test that
# pins the pace can hand the setting back afterwards.
_RUN_SECONDS = float(os.environ.get("TRANSLATE_JOB_RUN_SECONDS", "2"))

_XLIFF = """<?xml version="1.0" encoding="UTF-8"?>
<xliff version="1.2" xmlns="urn:oasis:names:tc:xliff:document:1.2">
  <file original="transcript" source-language="en" datatype="plaintext">
    <body>
      <trans-unit id="seg-1"><source>Good morning everyone</source></trans-unit>
      <trans-unit id="seg-2"><source>Thanks for joining the call</source></trans-unit>
    </body>
  </file>
</xliff>
"""


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _wait_for_status(
    translate,
    job_id,
    statuses=("COMPLETED", "COMPLETED_WITH_ERROR", "FAILED", "STOPPED"),
    timeout=30,
):
    deadline = time.time() + timeout
    job = None
    while time.time() < deadline:
        job = translate.describe_text_translation_job(JobId=job_id)[
            "TextTranslationJobProperties"
        ]
        if job["JobStatus"] in statuses:
            return job
        time.sleep(0.2)
    raise AssertionError(
        f"job {job_id} never reached {statuses}; last status "
        f"{job['JobStatus'] if job else 'unknown'}"
    )


def _split_s3_uri(uri):
    """`s3://bucket/prefix/` -> (bucket, prefix)."""
    rest = uri[len("s3://"):]
    bucket, _, prefix = rest.partition("/")
    return bucket, prefix


def _start(translate, **overrides):
    params = {
        "JobName": _unique("job"),
        "InputDataConfig": {"S3Uri": "s3://placeholder/input/", "ContentType": "text/plain"},
        "OutputDataConfig": {"S3Uri": "s3://placeholder/output/"},
        "DataAccessRoleArn": _ROLE_ARN,
        "SourceLanguageCode": "en",
        "TargetLanguageCodes": ["fr"],
    }
    params.update(overrides)
    params.setdefault("ClientToken", uuid.uuid4().hex)
    return translate.start_text_translation_job(**params)


@pytest.fixture
def corpus(s3):
    """An input folder holding two plain-text documents, and an output folder
    in the same bucket."""
    bucket = _unique("translate-batch")
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="input/greeting.txt", Body=b"Good morning everyone")
    s3.put_object(Bucket=bucket, Key="input/closing.txt", Body=b"Thanks for joining")
    return bucket, f"s3://{bucket}/input/", f"s3://{bucket}/output/"


def test_translate_job_completes_and_writes_translated_documents(translate, s3, corpus):
    """The end-to-end contract the issue asks for: a job reaches COMPLETED on
    its own and the translated documents are real objects under the location
    Describe reports, so a consumer can fetch them without special-casing."""
    bucket, input_uri, output_uri = corpus

    started = _start(
        translate,
        InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
        OutputDataConfig={"S3Uri": output_uri},
    )
    assert started["JobStatus"] == "SUBMITTED"
    job_id = started["JobId"]
    assert 1 <= len(job_id) <= 32

    job = _wait_for_status(translate, job_id)
    assert job["JobStatus"] == "COMPLETED"
    assert job["EndTime"] >= job["SubmittedTime"]
    assert job["JobDetails"] == {
        "TranslatedDocumentsCount": 2,
        "DocumentsWithErrorsCount": 0,
        "InputDocumentsCount": 2,
    }
    # AWS reports Message on a successful job too, not only on a failed one.
    assert job["Message"] == "Your job has completed successfully."

    out_bucket, out_prefix = _split_s3_uri(job["OutputDataConfig"]["S3Uri"])
    assert out_bucket == bucket
    body = s3.get_object(Bucket=out_bucket, Key=f"{out_prefix}fr.greeting.txt")["Body"].read()
    assert body.decode("utf-8") == "[fr] Good morning everyone"


def test_translate_rewrites_the_output_location_the_way_aws_does(translate, corpus):
    """Output does not land at the prefix the caller supplied: AWS creates
    `<accountId>-TranslateText-<JobId>/` inside it and reports that back. A
    consumer reading the supplied prefix would find nothing, so Describe has to
    report the rewritten location from the start."""
    _, input_uri, output_uri = corpus

    job_id = _start(
        translate,
        InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
        OutputDataConfig={"S3Uri": output_uri},
    )["JobId"]

    described = translate.describe_text_translation_job(JobId=job_id)[
        "TextTranslationJobProperties"
    ]
    reported = described["OutputDataConfig"]["S3Uri"]
    assert reported == f"{output_uri}000000000000-TranslateText-{job_id}/"
    # Reported before the job runs, not only once it finishes.
    assert described["JobStatus"] in ("SUBMITTED", "IN_PROGRESS", "COMPLETED")

    finished = _wait_for_status(translate, job_id)
    assert finished["OutputDataConfig"]["S3Uri"] == reported


def test_translate_xliff_target_is_populated_from_source(translate, s3):
    """The pipeline in the issue writes XLIFF and merges the translated
    `<target>` back onto its timed transcript segments. Without a populated
    `<target>` per `<trans-unit>`, and `target-language` on the `<file>`, there
    is nothing to merge."""
    import xml.etree.ElementTree as ET

    bucket = _unique("translate-xliff")
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="in/transcript.xlf", Body=_XLIFF.encode("utf-8"))

    job_id = _start(
        translate,
        InputDataConfig={
            "S3Uri": f"s3://{bucket}/in/",
            "ContentType": "application/x-xliff+xml",
        },
        OutputDataConfig={"S3Uri": f"s3://{bucket}/out/"},
        TargetLanguageCodes=["es"],
    )["JobId"]

    job = _wait_for_status(translate, job_id)
    assert job["JobStatus"] == "COMPLETED"

    out_bucket, out_prefix = _split_s3_uri(job["OutputDataConfig"]["S3Uri"])
    body = s3.get_object(Bucket=out_bucket, Key=f"{out_prefix}es.transcript.xlf")["Body"].read()

    ns = {"x": "urn:oasis:names:tc:xliff:document:1.2"}
    root = ET.fromstring(body)
    assert root.find("x:file", ns).get("target-language") == "es"
    targets = {
        unit.get("id"): unit.find("x:target", ns).text
        for unit in root.iter("{urn:oasis:names:tc:xliff:document:1.2}trans-unit")
    }
    assert targets == {
        "seg-1": "[es] Good morning everyone",
        "seg-2": "[es] Thanks for joining the call",
    }
    # The source segments must survive untouched, or the merge has nothing to
    # key on.
    sources = {
        unit.get("id"): unit.find("x:source", ns).text
        for unit in root.iter("{urn:oasis:names:tc:xliff:document:1.2}trans-unit")
    }
    assert sources["seg-1"] == "Good morning everyone"


def test_translate_html_keeps_its_markup(translate, s3):
    """An HTML job must return HTML. Translating the tags along with the text
    would hand the caller back a broken document."""
    bucket = _unique("translate-html")
    s3.create_bucket(Bucket=bucket)
    s3.put_object(
        Bucket=bucket,
        Key="in/page.html",
        Body=b"<html><body><p>Hello</p><script>var x = 1;</script></body></html>",
    )

    job_id = _start(
        translate,
        InputDataConfig={"S3Uri": f"s3://{bucket}/in/", "ContentType": "text/html"},
        OutputDataConfig={"S3Uri": f"s3://{bucket}/out/"},
    )["JobId"]

    job = _wait_for_status(translate, job_id)
    out_bucket, out_prefix = _split_s3_uri(job["OutputDataConfig"]["S3Uri"])
    body = s3.get_object(Bucket=out_bucket, Key=f"{out_prefix}fr.page.html")["Body"].read()

    assert body.decode("utf-8") == (
        "<html><body><p>[fr] Hello</p><script>var x = 1;</script></body></html>"
    )


def test_translate_writes_one_document_per_target_language(translate, s3, corpus):
    """`TargetLanguageCodes` takes up to ten languages and every input document
    is translated into each of them."""
    _, input_uri, output_uri = corpus

    job_id = _start(
        translate,
        InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
        OutputDataConfig={"S3Uri": output_uri},
        TargetLanguageCodes=["fr", "de"],
    )["JobId"]

    job = _wait_for_status(translate, job_id)
    out_bucket, out_prefix = _split_s3_uri(job["OutputDataConfig"]["S3Uri"])
    keys = {
        obj["Key"]
        for obj in s3.list_objects_v2(Bucket=out_bucket, Prefix=out_prefix).get("Contents", [])
    }
    for language in ("fr", "de"):
        for name in ("greeting.txt", "closing.txt"):
            assert f"{out_prefix}{language}.{name}" in keys

    french = s3.get_object(Bucket=out_bucket, Key=f"{out_prefix}fr.greeting.txt")["Body"].read()
    german = s3.get_object(Bucket=out_bucket, Key=f"{out_prefix}de.greeting.txt")["Body"].read()
    assert french != german


def test_translate_writes_the_auxiliary_details_file(translate, s3, corpus):
    """Every batch job writes `details/<code>.auxiliary-translation-details.json`
    beside its output. Tools that reconcile a job's input and output read it."""
    _, input_uri, output_uri = corpus

    job_id = _start(
        translate,
        InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
        OutputDataConfig={"S3Uri": output_uri},
    )["JobId"]

    job = _wait_for_status(translate, job_id)
    out_bucket, out_prefix = _split_s3_uri(job["OutputDataConfig"]["S3Uri"])
    key = f"{out_prefix}details/fr.auxiliary-translation-details.json"
    details = json.loads(s3.get_object(Bucket=out_bucket, Key=key)["Body"].read())

    assert details["sourceLanguageCode"] == "en"
    assert details["targetLanguageCode"] == "fr"
    assert details["inputDataPrefix"] == input_uri
    assert details["outputDataPrefix"] == job["OutputDataConfig"]["S3Uri"]
    assert {entry["sourceFile"] for entry in details["details"]} == {
        "greeting.txt",
        "closing.txt",
    }
    assert {entry["targetFile"] for entry in details["details"]} == {
        "fr.greeting.txt",
        "fr.closing.txt",
    }


def test_translate_is_deterministic_for_the_same_input(translate, s3, corpus):
    """There is no machine translation here, so the value of the mock rests
    entirely on being reproducible. The same document must translate identically
    on every run or integration tests asserting on output become flaky."""
    _, input_uri, output_uri = corpus
    outputs = []

    for _ in range(2):
        job_id = _start(
            translate,
            InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
            OutputDataConfig={"S3Uri": output_uri},
        )["JobId"]
        job = _wait_for_status(translate, job_id)
        out_bucket, out_prefix = _split_s3_uri(job["OutputDataConfig"]["S3Uri"])
        outputs.append(
            s3.get_object(Bucket=out_bucket, Key=f"{out_prefix}fr.greeting.txt")["Body"].read()
        )

    assert outputs[0] == outputs[1]


def test_translate_nested_input_keys_keep_their_layout(translate, s3):
    """The language code is prefixed onto the file name, not onto the key, so a
    job whose input folder has subfolders writes its output into the matching
    subfolders instead of flattening them into one namespace."""
    bucket = _unique("translate-nested")
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="in/2026/01/notes.txt", Body=b"Nested document")
    s3.put_object(Bucket=bucket, Key="in/", Body=b"")

    job_id = _start(
        translate,
        InputDataConfig={"S3Uri": f"s3://{bucket}/in/", "ContentType": "text/plain"},
        OutputDataConfig={"S3Uri": f"s3://{bucket}/out/"},
    )["JobId"]

    job = _wait_for_status(translate, job_id)
    assert job["JobStatus"] == "COMPLETED"
    # The folder marker is not a document.
    assert job["JobDetails"]["InputDocumentsCount"] == 1

    out_bucket, out_prefix = _split_s3_uri(job["OutputDataConfig"]["S3Uri"])
    body = s3.get_object(Bucket=out_bucket, Key=f"{out_prefix}2026/01/fr.notes.txt")["Body"].read()
    assert body.decode("utf-8") == "[fr] Nested document"


def test_translate_unreadable_document_completes_with_error(translate, s3):
    """A document that can't be read as its declared content type is a
    per-document failure, not a job failure: AWS counts it and finishes the job
    COMPLETED_WITH_ERROR, so the documents that did translate are still there."""
    bucket = _unique("translate-mixed")
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="in/good.xlf", Body=_XLIFF.encode("utf-8"))
    s3.put_object(Bucket=bucket, Key="in/broken.xlf", Body=b"<xliff><unclosed>")

    job_id = _start(
        translate,
        InputDataConfig={
            "S3Uri": f"s3://{bucket}/in/",
            "ContentType": "application/x-xliff+xml",
        },
        OutputDataConfig={"S3Uri": f"s3://{bucket}/out/"},
    )["JobId"]

    job = _wait_for_status(translate, job_id)
    assert job["JobStatus"] == "COMPLETED_WITH_ERROR"
    assert job["JobDetails"] == {
        "TranslatedDocumentsCount": 1,
        "DocumentsWithErrorsCount": 1,
        "InputDocumentsCount": 2,
    }
    assert "errors" in job["Message"]

    out_bucket, out_prefix = _split_s3_uri(job["OutputDataConfig"]["S3Uri"])
    keys = {
        obj["Key"]
        for obj in s3.list_objects_v2(Bucket=out_bucket, Prefix=out_prefix).get("Contents", [])
    }
    assert f"{out_prefix}fr.good.xlf" in keys
    assert f"{out_prefix}fr.broken.xlf" not in keys


def test_translate_empty_input_folder_fails_the_job_not_the_start_call(translate, s3):
    """AWS accepts the job and fails it asynchronously with a `Message`. A
    caller that expects `Start` to succeed and then polls would otherwise see an
    exception where the service gives it a failed job to inspect."""
    bucket = _unique("translate-empty")
    s3.create_bucket(Bucket=bucket)

    job_id = _start(
        translate,
        InputDataConfig={"S3Uri": f"s3://{bucket}/in/", "ContentType": "text/plain"},
        OutputDataConfig={"S3Uri": f"s3://{bucket}/out/"},
    )["JobId"]

    job = _wait_for_status(translate, job_id)
    assert job["JobStatus"] == "FAILED"
    assert "no documents" in job["Message"].lower()


def test_translate_missing_output_bucket_fails_the_job(translate, s3, corpus):
    """The output bucket is only touched once the job runs, so a bucket that
    isn't there surfaces as a failed job with a reason rather than silently
    dropping every translated document."""
    _, input_uri, _ = corpus

    job_id = _start(
        translate,
        InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
        OutputDataConfig={"S3Uri": f"s3://{_unique('translate-absent')}/out/"},
    )["JobId"]

    job = _wait_for_status(translate, job_id)
    assert job["JobStatus"] == "FAILED"
    assert "output location" in job["Message"].lower()


def test_translate_client_token_is_idempotent(translate, corpus):
    """`ClientToken` is an idempotency token. A retried start — an SDK retry, a
    replayed event — must resolve to the original job rather than starting a
    second one writing to a second output folder."""
    _, input_uri, output_uri = corpus
    token = uuid.uuid4().hex

    first = _start(
        translate,
        InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
        OutputDataConfig={"S3Uri": output_uri},
        ClientToken=token,
    )
    second = _start(
        translate,
        InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
        OutputDataConfig={"S3Uri": output_uri},
        ClientToken=token,
    )

    assert first["JobId"] == second["JobId"]


def test_translate_stops_a_running_job(translate, s3, corpus):
    """StopTextTranslationJob has to actually reach the worker. A job that keeps
    running after a stop writes output the caller has already abandoned.

    The run time is pinned high for this test so the stop is guaranteed to land
    mid-flight; otherwise a fast server completes the job first and the whole
    STOP_REQUESTED -> STOPPED path goes unexercised while the test still passes.
    The finished-before-stop case is covered separately below."""
    from conftest import _ministack_config

    _, input_uri, output_uri = corpus

    _ministack_config({"translate._JOB_RUN_SECONDS": 10})
    try:
        job_id = _start(
            translate,
            InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
            OutputDataConfig={"S3Uri": output_uri},
        )["JobId"]

        stopped = translate.stop_text_translation_job(JobId=job_id)
        assert stopped["JobId"] == job_id
        assert stopped["JobStatus"] == "STOP_REQUESTED"

        job = _wait_for_status(translate, job_id)
        assert job["JobStatus"] == "STOPPED"
        assert job["EndTime"] >= job["SubmittedTime"]
        out_bucket, out_prefix = _split_s3_uri(job["OutputDataConfig"]["S3Uri"])
        listing = s3.list_objects_v2(Bucket=out_bucket, Prefix=out_prefix)
        assert listing.get("KeyCount", 0) == 0
    finally:
        _ministack_config({"translate._JOB_RUN_SECONDS": _RUN_SECONDS})


def test_translate_stopping_a_finished_job_leaves_it_alone(translate, corpus):
    """AWS documents that a job which completes before it can be stopped stays
    COMPLETED. Reporting STOPPED would tell the caller its output is gone."""
    _, input_uri, output_uri = corpus

    job_id = _start(
        translate,
        InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
        OutputDataConfig={"S3Uri": output_uri},
    )["JobId"]
    _wait_for_status(translate, job_id)

    stopped = translate.stop_text_translation_job(JobId=job_id)
    assert stopped["JobStatus"] == "COMPLETED"


def test_translate_describe_unknown_job_is_not_found(translate):
    with pytest.raises(ClientError) as excinfo:
        translate.describe_text_translation_job(JobId="0" * 32)
    assert excinfo.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_translate_stop_unknown_job_is_not_found(translate):
    with pytest.raises(ClientError) as excinfo:
        translate.stop_text_translation_job(JobId="0" * 32)
    assert excinfo.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_translate_lists_jobs_newest_first_and_filters_by_name(translate, corpus):
    """The console and SDK pair `List` with `Describe`. Ordering has to be
    newest first and the name filter has to narrow, or a caller paging through
    a busy account never finds the job it just started."""
    _, input_uri, output_uri = corpus
    prefix = _unique("listcase")
    names = [f"{prefix}-a", f"{prefix}-b"]
    ids = []
    for name in names:
        ids.append(
            _start(
                translate,
                JobName=name,
                InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
                OutputDataConfig={"S3Uri": output_uri},
            )["JobId"]
        )

    listed = translate.list_text_translation_jobs(Filter={"JobName": prefix})[
        "TextTranslationJobPropertiesList"
    ]
    assert [job["JobId"] for job in listed] == list(reversed(ids))
    assert [job["JobName"] for job in listed] == list(reversed(names))


def test_translate_list_pages(translate, corpus):
    """A NextToken has to resume where the page ended. An offset-based token
    repeats a job whenever another is submitted between pages."""
    _, input_uri, output_uri = corpus
    prefix = _unique("paging")
    for index in range(3):
        _start(
            translate,
            JobName=f"{prefix}-{index}",
            InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
            OutputDataConfig={"S3Uri": output_uri},
        )

    seen = []
    token = None
    while True:
        kwargs = {"Filter": {"JobName": prefix}, "MaxResults": 2}
        if token:
            kwargs["NextToken"] = token
        page = translate.list_text_translation_jobs(**kwargs)
        seen.extend(job["JobName"] for job in page["TextTranslationJobPropertiesList"])
        token = page.get("NextToken")
        if not token:
            break

    assert seen == [f"{prefix}-{index}" for index in reversed(range(3))]


def test_translate_list_rejects_more_than_one_filter(translate):
    """AWS allows one filter at a time. Accepting two and silently applying one
    would hand back a result set the caller did not ask for."""
    with pytest.raises(ClientError) as excinfo:
        translate.list_text_translation_jobs(
            Filter={"JobName": "anything", "JobStatus": "COMPLETED"}
        )
    assert excinfo.value.response["Error"]["Code"] == "InvalidFilterException"


def test_translate_rejects_an_unsupported_content_type(translate, corpus):
    _, input_uri, output_uri = corpus
    with pytest.raises(ClientError) as excinfo:
        _start(
            translate,
            InputDataConfig={"S3Uri": input_uri, "ContentType": "application/pdf"},
            OutputDataConfig={"S3Uri": output_uri},
        )
    assert excinfo.value.response["Error"]["Code"] == "InvalidRequestException"


def test_translate_rejects_a_same_language_pair(translate, corpus):
    """Translating a language into itself is not a translation, and AWS models a
    dedicated exception for it that callers branch on."""
    _, input_uri, output_uri = corpus
    with pytest.raises(ClientError) as excinfo:
        _start(
            translate,
            InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
            OutputDataConfig={"S3Uri": output_uri},
            SourceLanguageCode="en",
            TargetLanguageCodes=["en"],
        )
    assert excinfo.value.response["Error"]["Code"] == "UnsupportedLanguagePairException"


def test_translate_rejects_a_malformed_role_arn(translate, corpus):
    _, input_uri, output_uri = corpus
    with pytest.raises(ClientError) as excinfo:
        _start(
            translate,
            InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
            OutputDataConfig={"S3Uri": output_uri},
            DataAccessRoleArn="arn:aws:iam::12345:role/TooShortAccount",
        )
    assert excinfo.value.response["Error"]["Code"] == "InvalidParameterValueException"


def test_translate_rejects_a_non_s3_input_uri(translate, corpus):
    _, _, output_uri = corpus
    with pytest.raises(ClientError) as excinfo:
        _start(
            translate,
            InputDataConfig={
                "S3Uri": "https://example.com/input/",
                "ContentType": "text/plain",
            },
            OutputDataConfig={"S3Uri": output_uri},
        )
    assert excinfo.value.response["Error"]["Code"] == "InvalidParameterValueException"


def test_translate_rejects_more_than_ten_target_languages(translate, corpus):
    _, input_uri, output_uri = corpus
    with pytest.raises(ClientError) as excinfo:
        _start(
            translate,
            InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
            OutputDataConfig={"S3Uri": output_uri},
            TargetLanguageCodes=[f"x{index}" for index in range(11)],
        )
    assert excinfo.value.response["Error"]["Code"] == "InvalidParameterValueException"


def test_translate_echoes_terminology_and_settings(translate, corpus):
    """`TerminologyNames`, `ParallelDataNames` and `Settings` don't change the
    output here, but dropping them would make Describe disagree with what the
    caller submitted."""
    _, input_uri, output_uri = corpus

    job_id = _start(
        translate,
        InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
        OutputDataConfig={"S3Uri": output_uri},
        TerminologyNames=["product-names"],
        ParallelDataNames=["prior-translations"],
        Settings={"Formality": "FORMAL", "Profanity": "MASK"},
    )["JobId"]

    job = translate.describe_text_translation_job(JobId=job_id)[
        "TextTranslationJobProperties"
    ]
    assert job["TerminologyNames"] == ["product-names"]
    assert job["ParallelDataNames"] == ["prior-translations"]
    assert job["Settings"] == {"Formality": "FORMAL", "Profanity": "MASK"}
    assert job["DataAccessRoleArn"] == _ROLE_ARN
    assert job["SourceLanguageCode"] == "en"
    assert job["TargetLanguageCodes"] == ["fr"]


def test_translate_publishes_a_job_state_change_event(translate, s3, eb, sqs, corpus):
    """AWS emits `Translate TextTranslationJob State Change` when a batch job
    reaches a terminal state. The second Lambda in the issue's pipeline is
    triggered by that event; without it the chain stops at translation."""
    _, input_uri, output_uri = corpus

    queue_url = sqs.create_queue(QueueName=_unique("translate-events"))["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    rule_name = _unique("translate-rule")
    eb.put_rule(
        Name=rule_name,
        EventPattern=json.dumps(
            {
                "source": ["aws.translate"],
                "detail-type": ["Translate TextTranslationJob State Change"],
            }
        ),
    )
    eb.put_targets(Rule=rule_name, Targets=[{"Id": "q", "Arn": queue_arn}])

    try:
        job_id = _start(
            translate,
            InputDataConfig={"S3Uri": input_uri, "ContentType": "text/plain"},
            OutputDataConfig={"S3Uri": output_uri},
        )["JobId"]
        _wait_for_status(translate, job_id)

        deadline = time.time() + 15
        detail = None
        while time.time() < deadline and detail is None:
            received = sqs.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1
            )
            for message in received.get("Messages", []):
                envelope = json.loads(message["Body"])
                body = json.loads(envelope["detail"]) if isinstance(
                    envelope.get("detail"), str
                ) else envelope.get("detail")
                if body and body.get("jobId") == job_id:
                    detail = body
                    break

        assert detail is not None, "no Translate state change event reached the rule target"
        assert detail["jobStatus"] == "COMPLETED"
    finally:
        eb.remove_targets(Rule=rule_name, Ids=["q"])
        eb.delete_rule(Name=rule_name)
        sqs.delete_queue(QueueUrl=queue_url)
