import json
import os
import time
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

_ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _wait_for_status(transcribe, job_name, statuses=("COMPLETED", "FAILED"), timeout=30):
    deadline = time.time() + timeout
    job = None
    while time.time() < deadline:
        job = transcribe.get_transcription_job(TranscriptionJobName=job_name)["TranscriptionJob"]
        if job["TranscriptionJobStatus"] in statuses:
            return job
        time.sleep(0.2)
    raise AssertionError(
        f"job {job_name} never reached {statuses}; last status "
        f"{job['TranscriptionJobStatus'] if job else 'unknown'}"
    )


def _split_transcript_uri(uri):
    """`http://host:port/bucket/key` -> (bucket, key)."""
    _, _, rest = uri.partition("://")
    _, _, path = rest.partition("/")
    bucket, _, key = path.partition("/")
    return bucket, key


@pytest.fixture
def media(s3):
    bucket = _unique("transcribe-media")
    key = "audio/call.mp3"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key=key, Body=b"not really audio")
    return bucket, key, f"s3://{bucket}/{key}"


def test_transcribe_job_completes_and_writes_a_transcript(transcribe, s3, media):
    """The end-to-end contract the issue asks for: a job reaches COMPLETED on its
    own, and TranscriptFileUri resolves to a real object in the real Transcribe
    result shape, so a consumer can fetch and parse it without special-casing."""
    _, _, media_uri = media
    job_name = _unique("job")

    started = transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        MediaFormat="mp3",
        Media={"MediaFileUri": media_uri},
    )["TranscriptionJob"]

    assert started["TranscriptionJobName"] == job_name
    assert started["TranscriptionJobStatus"] == "QUEUED"
    assert started["LanguageCode"] == "en-US"
    assert started["Media"] == {"MediaFileUri": media_uri}
    # AWS omits Transcript until the job completes.
    assert "Transcript" not in started

    job = _wait_for_status(transcribe, job_name)
    assert job["TranscriptionJobStatus"] == "COMPLETED"
    assert job["CompletionTime"] >= job["StartTime"] >= job["CreationTime"]

    bucket, key = _split_transcript_uri(job["Transcript"]["TranscriptFileUri"])
    document = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())

    assert document["jobName"] == job_name
    assert document["status"] == "COMPLETED"
    text = document["results"]["transcripts"][0]["transcript"]
    assert text
    assert len(document["results"]["items"]) == len(text.split())
    assert document["results"]["items"][0]["alternatives"][0]["content"] == text.split()[0]


def test_transcribe_transcript_is_deterministic_per_media(transcribe, s3, media):
    """There is no ASR here, so the value of the mock rests entirely on being
    reproducible. The same media must transcribe identically on every run or
    integration tests asserting on transcript content become flaky."""
    _, _, media_uri = media
    texts = []

    for _ in range(2):
        job_name = _unique("job")
        transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            LanguageCode="en-US",
            Media={"MediaFileUri": media_uri},
        )
        job = _wait_for_status(transcribe, job_name)
        bucket, key = _split_transcript_uri(job["Transcript"]["TranscriptFileUri"])
        document = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        texts.append(document["results"]["transcripts"][0]["transcript"])

    assert texts[0] == texts[1]


def test_transcribe_different_media_gives_different_transcripts(transcribe, s3, media):
    """The flip side of determinism: a constant string would satisfy the previous
    test while making it impossible to tell two jobs apart."""
    bucket, _, first_uri = media
    s3.put_object(Bucket=bucket, Key="audio/other.mp3", Body=b"also not audio")
    second_uri = f"s3://{bucket}/audio/other.mp3"

    texts = []
    for media_uri in (first_uri, second_uri):
        job_name = _unique("job")
        transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            LanguageCode="en-US",
            Media={"MediaFileUri": media_uri},
        )
        job = _wait_for_status(transcribe, job_name)
        out_bucket, key = _split_transcript_uri(job["Transcript"]["TranscriptFileUri"])
        document = json.loads(s3.get_object(Bucket=out_bucket, Key=key)["Body"].read())
        texts.append(document["results"]["transcripts"][0]["transcript"])

    assert texts[0] != texts[1]


def test_transcribe_writes_to_the_caller_supplied_output_bucket(transcribe, s3, media):
    """OutputBucketName/OutputKey must be honoured verbatim. A CDK stack that
    points Transcribe at its own bucket and then reads a fixed key downstream
    breaks if the transcript lands anywhere else."""
    _, _, media_uri = media
    out_bucket = _unique("transcribe-out")
    s3.create_bucket(Bucket=out_bucket)
    job_name = _unique("job")

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": media_uri},
        OutputBucketName=out_bucket,
        OutputKey="results/transcript.json",
    )

    job = _wait_for_status(transcribe, job_name)
    assert job["TranscriptionJobStatus"] == "COMPLETED"
    assert job["Transcript"]["TranscriptFileUri"].endswith(
        f"/{out_bucket}/results/transcript.json"
    )
    # The object is really there.
    s3.head_object(Bucket=out_bucket, Key="results/transcript.json")

    summary = next(
        s
        for s in transcribe.list_transcription_jobs(JobNameContains=job_name)[
            "TranscriptionJobSummaries"
        ]
        if s["TranscriptionJobName"] == job_name
    )
    assert summary["OutputLocationType"] == "CUSTOMER_BUCKET"


def test_transcribe_output_key_ending_in_slash_is_a_prefix(transcribe, s3, media):
    """AWS treats a trailing slash as a prefix and appends <jobName>.json rather
    than writing an object whose key ends in a slash."""
    _, _, media_uri = media
    out_bucket = _unique("transcribe-out")
    s3.create_bucket(Bucket=out_bucket)
    job_name = _unique("job")

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": media_uri},
        OutputBucketName=out_bucket,
        OutputKey="nested/prefix/",
    )

    job = _wait_for_status(transcribe, job_name)
    assert job["Transcript"]["TranscriptFileUri"].endswith(
        f"/{out_bucket}/nested/prefix/{job_name}.json"
    )


def test_transcribe_defaults_to_a_service_managed_bucket(transcribe, media):
    """With no OutputBucketName, real Transcribe stores the transcript in a bucket
    it owns and hands back a presigned URL. MiniStack provisions its own bucket
    instead, so the URI is fetchable locally rather than being a dead link."""
    _, _, media_uri = media
    job_name = _unique("job")

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": media_uri},
    )

    job = _wait_for_status(transcribe, job_name)
    bucket, _ = _split_transcript_uri(job["Transcript"]["TranscriptFileUri"])
    assert bucket == "ministack-transcribe-output"

    summary = next(
        s
        for s in transcribe.list_transcription_jobs(JobNameContains=job_name)[
            "TranscriptionJobSummaries"
        ]
        if s["TranscriptionJobName"] == job_name
    )
    assert summary["OutputLocationType"] == "SERVICE_BUCKET"


def test_transcribe_unreadable_media_fails_the_job(transcribe, s3):
    """AWS accepts the request and fails the job asynchronously with a
    FailureReason rather than rejecting Start, because the media is not read
    until the job runs. A poller must be able to observe FAILED."""
    bucket = _unique("transcribe-media")
    s3.create_bucket(Bucket=bucket)
    job_name = _unique("job")

    # Start succeeds; AWS only discovers the object is unreadable once the job runs.
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": f"s3://{bucket}/missing.mp3"},
    )

    job = _wait_for_status(transcribe, job_name)
    assert job["TranscriptionJobStatus"] == "FAILED"
    assert "can't be accessed" in job["FailureReason"]
    assert "Transcript" not in job


def test_transcribe_duplicate_job_name_conflicts(transcribe, media):
    _, _, media_uri = media
    job_name = _unique("job")
    kwargs = dict(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": media_uri},
    )
    transcribe.start_transcription_job(**kwargs)

    with pytest.raises(ClientError) as exc:
        transcribe.start_transcription_job(**kwargs)
    assert exc.value.response["Error"]["Code"] == "ConflictException"


def test_transcribe_get_unknown_job_is_not_found(transcribe):
    """NotFoundException is modelled on GetTranscriptionJob but not on Delete, so
    the two operations deliberately differ on a missing job."""
    with pytest.raises(ClientError) as exc:
        transcribe.get_transcription_job(TranscriptionJobName=_unique("nope"))
    assert exc.value.response["Error"]["Code"] == "NotFoundException"


def test_transcribe_start_requires_a_language_directive(transcribe, media):
    """LanguageCode is not a required member, but AWS still rejects a request that
    supplies neither it nor an IdentifyLanguage flag."""
    _, _, media_uri = media
    with pytest.raises(ClientError) as exc:
        transcribe.start_transcription_job(
            TranscriptionJobName=_unique("job"),
            Media={"MediaFileUri": media_uri},
        )
    assert exc.value.response["Error"]["Code"] == "BadRequestException"


def test_transcribe_identify_language_without_options_falls_back_to_a_default(transcribe, media):
    """`LanguageOptions` is optional, so a caller can ask for identification and
    name no candidates at all. The job still has to resolve to something, or a
    consumer reading LanguageCode off the completed job gets nothing."""
    _, _, media_uri = media
    job_name = _unique("job")
    started = transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        IdentifyLanguage=True,
        Media={"MediaFileUri": media_uri},
    )["TranscriptionJob"]
    assert started["TranscriptionJobStatus"] == "QUEUED"
    assert "LanguageCode" not in started

    job = _wait_for_status(transcribe, job_name)
    assert job["LanguageCode"] == "en-US"


def test_transcribe_start_rejects_an_unusable_media_uri(transcribe):
    """AWS models MediaFileUri as `(s3://|http(s*)://).+`, so the scheme check
    is not what makes a URI bad. An unsupported scheme and an HTTP URL with no
    key to address are both unusable and both rejected."""
    for uri in ("ftp://example.com/bucket/audio.mp3", "https://example.com/audio.mp3"):
        with pytest.raises(ClientError) as exc:
            transcribe.start_transcription_job(
                TranscriptionJobName=_unique("job"),
                LanguageCode="en-US",
                Media={"MediaFileUri": uri},
            )
        assert exc.value.response["Error"]["Code"] == "BadRequestException", uri


@pytest.mark.parametrize("style", ["path", "path-dash", "virtual"])
def test_transcribe_accepts_https_s3_media_uris(transcribe, s3, media, style):
    """The AWS SDKs hand Transcribe an HTTPS S3 URL as readily as an `s3://`
    one, and AWS accepts both. Rejecting the HTTPS form fails the job at
    submission for any caller that builds its URI that way."""
    bucket, key, _ = media
    uri = {
        "path": f"https://s3.us-east-1.amazonaws.com/{bucket}/{key}",
        "path-dash": f"https://s3-us-east-1.amazonaws.com/{bucket}/{key}",
        "virtual": f"https://{bucket}.s3.us-east-1.amazonaws.com/{key}",
    }[style]

    job_name = _unique("job")
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": uri},
    )
    job = _wait_for_status(transcribe, job_name)
    # The media must actually have been located and read, not merely accepted.
    assert job["TranscriptionJobStatus"] == "COMPLETED", job.get("FailureReason")


def test_transcribe_identify_language_resolves_a_language_code(transcribe, media):
    """`IdentifyLanguage` leaves the caller with no language at submission
    time; AWS fills LanguageCode in once the job runs, alongside the
    confidence score. A consumer reading the completed job must find it."""
    _, _, media_uri = media
    job_name = _unique("job")

    started = transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        IdentifyLanguage=True,
        LanguageOptions=["fr-FR", "en-US"],
        Media={"MediaFileUri": media_uri},
    )["TranscriptionJob"]
    assert "LanguageCode" not in started

    job = _wait_for_status(transcribe, job_name)
    assert job["IdentifyLanguage"] is True
    assert job["LanguageCode"] == "fr-FR"
    assert job["IdentifiedLanguageScore"] == pytest.approx(1.0)


def test_transcribe_identify_multiple_languages_reports_language_codes(transcribe, media):
    """A multi-language job reports `LanguageCodes` and deliberately leaves
    `LanguageCode` unset, exactly as AWS does, so a consumer resolving the
    source language has to fall back to the list."""
    _, _, media_uri = media
    job_name = _unique("job")

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        IdentifyMultipleLanguages=True,
        LanguageOptions=["en-AU", "fr-FR"],
        Media={"MediaFileUri": media_uri},
    )
    job = _wait_for_status(transcribe, job_name)

    assert "LanguageCode" not in job
    codes = job["LanguageCodes"]
    assert [entry["LanguageCode"] for entry in codes] == ["en-AU", "fr-FR"]
    # Every reported language must own some of the audio, otherwise the span
    # breakdown is decorative.
    assert all(entry["DurationInSeconds"] > 0 for entry in codes)


def test_transcribe_transcript_carries_segments_and_speaker_labels(transcribe, s3, media):
    """`ShowSpeakerLabels` and `ShowAlternatives` change what the transcript
    document contains, not just what the job echoes. Consumers read
    `results.segments`, `results.audio_segments` and `results.speaker_labels`
    to render a transcript, so those have to be present and index-aligned."""
    _, _, media_uri = media
    job_name = _unique("job")

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": media_uri},
        Settings={
            "ShowSpeakerLabels": True,
            "MaxSpeakerLabels": 10,
            "ShowAlternatives": True,
            "MaxAlternatives": 4,
        },
    )
    job = _wait_for_status(transcribe, job_name)
    bucket, key = _split_transcript_uri(job["Transcript"]["TranscriptFileUri"])
    results = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())["results"]

    segments = results["segments"]
    audio_segments = results["audio_segments"]
    speaker_segments = results["speaker_labels"]["segments"]
    assert len(segments) == len(audio_segments) == len(speaker_segments) > 1

    for segment, audio, speaker in zip(segments, audio_segments, speaker_segments):
        assert segment["start_time"] == audio["start_time"] == speaker["start_time"]
        assert len(segment["alternatives"]) == 4
        assert audio["speaker_label"] == speaker["speaker_label"]
        assert audio["speaker_label"].startswith("spk_")

    # The joined text must still be the transcript, not a reshuffled version.
    assert " ".join(a["transcript"] for a in audio_segments) == (
        results["transcripts"][0]["transcript"]
    )


def test_transcribe_omits_diarisation_when_not_requested(transcribe, s3, media):
    """A caller that never asked for speaker labels or alternatives should not
    find them in the output, or it cannot tell the settings had any effect."""
    _, _, media_uri = media
    job_name = _unique("job")

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": media_uri},
    )
    job = _wait_for_status(transcribe, job_name)
    bucket, key = _split_transcript_uri(job["Transcript"]["TranscriptFileUri"])
    results = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())["results"]

    assert "speaker_labels" not in results
    assert "segments" not in results
    assert results["audio_segments"]
    assert results["items"]


def test_transcribe_redaction_writes_only_the_redacted_transcript(transcribe, s3, media):
    """`RedactionOutput=redacted` replaces the transcript rather than adding to
    it: AWS writes `redacted-<name>` and no unredacted copy, and reports the
    location as RedactedTranscriptFileUri."""
    bucket, _, media_uri = media
    job_name = _unique("job")
    output_key = "out/job.json"

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": media_uri},
        OutputBucketName=bucket,
        OutputKey=output_key,
        ContentRedaction={
            "RedactionType": "PII",
            "RedactionOutput": "redacted",
            "PiiEntityTypes": ["ALL"],
        },
    )
    job = _wait_for_status(transcribe, job_name)

    assert job["ContentRedaction"]["RedactionOutput"] == "redacted"
    assert "TranscriptFileUri" not in job["Transcript"]
    _, redacted_key = _split_transcript_uri(job["Transcript"]["RedactedTranscriptFileUri"])
    # The prefix moves onto the file name, not the whole key.
    assert redacted_key == "out/redacted-job.json"

    document = json.loads(s3.get_object(Bucket=bucket, Key=redacted_key)["Body"].read())
    assert "[PII]" in document["results"]["transcripts"][0]["transcript"]

    with pytest.raises(ClientError):
        s3.get_object(Bucket=bucket, Key=output_key)


def test_transcribe_redaction_can_keep_both_transcripts(transcribe, s3, media):
    """`redacted_and_unredacted` is the variant that keeps the original, and
    both URIs have to be reported for a caller to reach either."""
    bucket, _, media_uri = media
    job_name = _unique("job")
    output_key = "both/job.json"

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": media_uri},
        OutputBucketName=bucket,
        OutputKey=output_key,
        ContentRedaction={
            "RedactionType": "PII",
            "RedactionOutput": "redacted_and_unredacted",
            "PiiEntityTypes": ["ALL"],
        },
    )
    job = _wait_for_status(transcribe, job_name)

    _, plain_key = _split_transcript_uri(job["Transcript"]["TranscriptFileUri"])
    _, redacted_key = _split_transcript_uri(job["Transcript"]["RedactedTranscriptFileUri"])
    assert plain_key == output_key
    assert redacted_key == "both/redacted-job.json"

    plain = json.loads(s3.get_object(Bucket=bucket, Key=plain_key)["Body"].read())
    redacted = json.loads(s3.get_object(Bucket=bucket, Key=redacted_key)["Body"].read())
    plain_text = plain["results"]["transcripts"][0]["transcript"]
    redacted_text = redacted["results"]["transcripts"][0]["transcript"]
    assert "[PII]" not in plain_text
    assert "[PII]" in redacted_text


def test_transcribe_writes_requested_subtitle_files(transcribe, s3, media):
    """Subtitles are reported as SubtitleFileUris, so the files have to exist
    and be well-formed for each requested format."""
    bucket, _, media_uri = media
    job_name = _unique("job")

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": media_uri},
        OutputBucketName=bucket,
        OutputKey="subs/job.json",
        Subtitles={"Formats": ["vtt", "srt"], "OutputStartIndex": 1},
    )
    job = _wait_for_status(transcribe, job_name)

    subtitles = job["Subtitles"]
    assert subtitles["Formats"] == ["vtt", "srt"]
    assert subtitles["OutputStartIndex"] == 1
    assert len(subtitles["SubtitleFileUris"]) == 2

    bodies = {}
    for uri in subtitles["SubtitleFileUris"]:
        _, sub_key = _split_transcript_uri(uri)
        bodies[sub_key] = s3.get_object(Bucket=bucket, Key=sub_key)["Body"].read().decode()

    assert bodies["subs/job.vtt"].startswith("WEBVTT")
    # WebVTT separates the cue timestamp with a dot, SubRip with a comma.
    assert " --> " in bodies["subs/job.vtt"]
    assert "00:00:00.000 --> " in bodies["subs/job.vtt"]
    assert bodies["subs/job.srt"].startswith("1\n00:00:00,000 --> ")


def test_transcribe_redaction_redacts_the_subtitle_files_too(transcribe, s3, media):
    """Subtitles carry the transcript text. If they are rendered from the
    unredacted document, `RedactionOutput=redacted` writes the raw text to the
    bucket in a `.vtt` while claiming it never left, which defeats the whole
    point of that layout."""
    bucket, _, media_uri = media
    job_name = _unique("job")

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": media_uri},
        OutputBucketName=bucket,
        OutputKey="redsubs/job.json",
        ContentRedaction={
            "RedactionType": "PII",
            "RedactionOutput": "redacted",
            "PiiEntityTypes": ["ALL"],
        },
        Subtitles={"Formats": ["vtt"]},
    )
    job = _wait_for_status(transcribe, job_name)

    _, subtitle_key = _split_transcript_uri(job["Subtitles"]["SubtitleFileUris"][0])
    # The name has to match the content, otherwise `redsubs/job.vtt` sits next
    # to `redsubs/redacted-job.json` looking like the unredacted counterpart.
    assert subtitle_key == "redsubs/redacted-job.vtt"

    body = s3.get_object(Bucket=bucket, Key=subtitle_key)["Body"].read().decode()
    assert "[PII]" in body

    # The digest token is what redaction masks; finding it means the raw
    # transcript reached the bucket.
    _, redacted_key = _split_transcript_uri(job["Transcript"]["RedactedTranscriptFileUri"])
    document = json.loads(s3.get_object(Bucket=bucket, Key=redacted_key)["Body"].read())
    assert "media#" not in body, document["results"]["transcripts"][0]["transcript"]

    with pytest.raises(ClientError):
        s3.get_object(Bucket=bucket, Key="redsubs/job.vtt")


def test_transcribe_multi_language_items_agree_with_their_segment(transcribe, s3, media):
    """Every word item belongs to exactly one audio segment, so its
    `language_code` has to be that segment's. Tagging all items with the first
    resolved language gives a document that contradicts itself, and a consumer
    grouping items by language gets the wrong grouping."""
    _, _, media_uri = media
    job_name = _unique("job")

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        IdentifyMultipleLanguages=True,
        LanguageOptions=["en-AU", "fr-FR"],
        Media={"MediaFileUri": media_uri},
    )
    job = _wait_for_status(transcribe, job_name)
    bucket, key = _split_transcript_uri(job["Transcript"]["TranscriptFileUri"])
    results = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())["results"]

    items = results["items"]
    audio_segments = results["audio_segments"]
    # Both languages must actually appear, or the check is vacuous.
    assert len({s["language_code"] for s in audio_segments}) == 2

    for segment in audio_segments:
        for index in segment["items"]:
            assert items[index]["language_code"] == segment["language_code"], (
                f"item {index} disagrees with audio segment {segment['id']}"
            )


def test_transcribe_single_language_items_carry_no_language_tag(transcribe, s3, media):
    """A single-language job carries the code on the job record, so repeating
    it on every item is noise the real service does not emit."""
    _, _, media_uri = media
    job_name = _unique("job")

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": media_uri},
    )
    job = _wait_for_status(transcribe, job_name)
    bucket, key = _split_transcript_uri(job["Transcript"]["TranscriptFileUri"])
    results = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())["results"]

    assert all("language_code" not in item for item in results["items"])


def test_transcribe_identify_multiple_languages_reports_each_language_once(transcribe, media):
    """Repeated LanguageOptions must not become repeated LanguageCodes entries,
    each claiming the full audio. The reported spans would then sum to more
    audio than the job contains."""
    _, _, media_uri = media
    job_name = _unique("job")

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        IdentifyMultipleLanguages=True,
        LanguageOptions=["en-AU", "en-AU"],
        Media={"MediaFileUri": media_uri},
    )
    job = _wait_for_status(transcribe, job_name)

    codes = job["LanguageCodes"]
    assert [entry["LanguageCode"] for entry in codes] == ["en-AU"]


@pytest.mark.parametrize(
    "language_params",
    [
        {"IdentifyMultipleLanguages": True, "LanguageOptions": ["en-AU", "fr-FR"]},
        {"IdentifyMultipleLanguages": True, "LanguageOptions": ["en-AU"]},
        {"IdentifyMultipleLanguages": True},
    ],
    ids=["two-options", "one-option", "no-options"],
)
def test_transcribe_identify_multiple_languages_always_reports_the_array(
    transcribe, media, language_params
):
    """A multi-language job reports `LanguageCodes` however many options it was
    given, including one or none. Collapsing to a scalar `LanguageCode` when
    only one language resolves is a fidelity gap: AWS populates the array
    whenever the flag is set."""
    _, _, media_uri = media
    job_name = _unique("job")

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={"MediaFileUri": media_uri},
        **language_params,
    )
    job = _wait_for_status(transcribe, job_name)

    assert job["IdentifyMultipleLanguages"] is True
    assert "LanguageCode" not in job
    assert job["LanguageCodes"]


def test_transcribe_start_rejects_an_invalid_job_name(transcribe, media):
    _, _, media_uri = media
    with pytest.raises(ClientError) as exc:
        transcribe.start_transcription_job(
            TranscriptionJobName="not a valid name",
            LanguageCode="en-US",
            Media={"MediaFileUri": media_uri},
        )
    assert exc.value.response["Error"]["Code"] == "BadRequestException"


def test_transcribe_list_filters_by_status_and_name(transcribe, media):
    _, _, media_uri = media
    # A hex marker can come out all digits, for which `.upper()` is the
    # identity and the case-insensitivity assertion below proves nothing.
    marker = f"zz{uuid.uuid4().hex[:8]}"
    job_name = f"filter-{marker}"

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": media_uri},
    )
    _wait_for_status(transcribe, job_name)

    listed = transcribe.list_transcription_jobs(JobNameContains=marker)
    assert [s["TranscriptionJobName"] for s in listed["TranscriptionJobSummaries"]] == [job_name]

    # The search is not case sensitive on AWS.
    assert transcribe.list_transcription_jobs(JobNameContains=marker.upper())[
        "TranscriptionJobSummaries"
    ]

    completed = transcribe.list_transcription_jobs(
        Status="COMPLETED", JobNameContains=marker
    )
    assert completed["Status"] == "COMPLETED"
    assert len(completed["TranscriptionJobSummaries"]) == 1

    assert (
        transcribe.list_transcription_jobs(Status="FAILED", JobNameContains=marker)[
            "TranscriptionJobSummaries"
        ]
        == []
    )


def test_transcribe_list_paginates(transcribe, media):
    """MaxResults must cap the page and hand back a NextToken that resumes exactly
    where the previous page stopped, with no gaps or repeats."""
    _, _, media_uri = media
    marker = uuid.uuid4().hex[:8]
    names = {f"page-{marker}-{i}" for i in range(3)}
    for name in names:
        transcribe.start_transcription_job(
            TranscriptionJobName=name,
            LanguageCode="en-US",
            Media={"MediaFileUri": media_uri},
        )

    first = transcribe.list_transcription_jobs(JobNameContains=marker, MaxResults=2)
    assert len(first["TranscriptionJobSummaries"]) == 2
    assert first["NextToken"]

    second = transcribe.list_transcription_jobs(
        JobNameContains=marker, MaxResults=2, NextToken=first["NextToken"]
    )
    assert len(second["TranscriptionJobSummaries"]) == 1
    assert "NextToken" not in second

    seen = {
        s["TranscriptionJobName"]
        for s in first["TranscriptionJobSummaries"] + second["TranscriptionJobSummaries"]
    }
    assert seen == names


def test_transcribe_delete_removes_the_job_and_is_idempotent(transcribe, media):
    """botocore models no NotFoundException on DeleteTranscriptionJob, so deleting
    a job that is not there succeeds rather than erroring."""
    _, _, media_uri = media
    job_name = _unique("job")
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": media_uri},
    )
    _wait_for_status(transcribe, job_name)

    transcribe.delete_transcription_job(TranscriptionJobName=job_name)

    with pytest.raises(ClientError) as exc:
        transcribe.get_transcription_job(TranscriptionJobName=job_name)
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

    # AWS models no NotFoundException on delete, so a second call succeeds.
    transcribe.delete_transcription_job(TranscriptionJobName=job_name)


def test_transcribe_list_pagination_survives_a_deletion_between_pages(transcribe, media):
    _, _, media_uri = media
    marker = uuid.uuid4().hex[:8]
    names = [f"del-{marker}-{i}" for i in range(4)]
    for name in names:
        transcribe.start_transcription_job(
            TranscriptionJobName=name,
            LanguageCode="en-US",
            Media={"MediaFileUri": media_uri},
        )

    first = transcribe.list_transcription_jobs(JobNameContains=marker, MaxResults=2)
    returned = [s["TranscriptionJobName"] for s in first["TranscriptionJobSummaries"]]

    # Drop a job that page one already returned. An offset-based token would
    # shift the remaining jobs down a slot and skip one.
    transcribe.delete_transcription_job(TranscriptionJobName=returned[0])

    second = transcribe.list_transcription_jobs(
        JobNameContains=marker, MaxResults=2, NextToken=first["NextToken"]
    )
    returned += [s["TranscriptionJobName"] for s in second["TranscriptionJobSummaries"]]

    assert set(returned) == set(names)
    assert len(returned) == len(set(returned))


def test_transcribe_list_rejects_a_malformed_next_token(transcribe):
    """A token the service did not issue must be refused rather than silently
    treated as the first page, which would loop a paginating client forever."""
    with pytest.raises(ClientError) as exc:
        transcribe.list_transcription_jobs(NextToken="not-a-real-token")
    assert exc.value.response["Error"]["Code"] == "BadRequestException"


def test_transcribe_recreating_a_deleted_job_name_emits_one_event(
    transcribe, eb, sqs, media
):
    """A deleted job's worker is not cancelled. Without a per-run guard it
    would also drive the replacement job, completing it early and emitting a
    second state change event, so anything chained off the event runs twice."""
    _, _, media_uri = media
    suffix = uuid.uuid4().hex[:8]
    queue_url = sqs.create_queue(QueueName=f"transcribe-rerun-{suffix}")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    job_name = f"rerun-{suffix}"
    rule_name = f"transcribe-rerun-rule-{suffix}"
    eb.put_rule(
        Name=rule_name,
        EventPattern=json.dumps(
            {
                "source": ["aws.transcribe"],
                "detail": {"TranscriptionJobName": [job_name]},
            }
        ),
        State="ENABLED",
    )
    eb.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": queue_arn}])

    try:
        start = dict(
            TranscriptionJobName=job_name,
            LanguageCode="en-US",
            Media={"MediaFileUri": media_uri},
        )
        transcribe.start_transcription_job(**start)
        # Delete while still QUEUED, then immediately reuse the name.
        transcribe.delete_transcription_job(TranscriptionJobName=job_name)
        transcribe.start_transcription_job(**start)

        job = _wait_for_status(transcribe, job_name)
        assert job["TranscriptionJobStatus"] == "COMPLETED"

        # Drain well past the point the orphaned worker would have fired.
        deadline = time.time() + 12
        events = []
        while time.time() < deadline:
            batch = sqs.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1
            ).get("Messages", [])
            events += batch
            for message in batch:
                sqs.delete_message(
                    QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                )

        assert len(events) == 1, (
            f"expected exactly one state change event, got {len(events)}: "
            f"{[json.loads(e['Body'])['detail'] for e in events]}"
        )
    finally:
        eb.remove_targets(Rule=rule_name, Ids=["1"])
        eb.delete_rule(Name=rule_name)
        sqs.delete_queue(QueueUrl=queue_url)


def test_transcribe_jobs_are_region_scoped(transcribe, s3, media):
    bucket, _, east_uri = media
    s3.put_object(Bucket=bucket, Key="audio/west.mp3", Body=b"a different sound")
    west_uri = f"s3://{bucket}/audio/west.mp3"

    west = boto3.client(
        "transcribe",
        endpoint_url=_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-west-2",
    )
    job_name = _unique("regional")

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": east_uri},
    )

    assert west.list_transcription_jobs(JobNameContains=job_name)[
        "TranscriptionJobSummaries"
    ] == []
    with pytest.raises(ClientError) as exc:
        west.get_transcription_job(TranscriptionJobName=job_name)
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

    # The background worker pins the region it was started for, so the job
    # still finishes in us-east-1 rather than leaking into whichever request
    # happens to be in flight.
    east_job = _wait_for_status(transcribe, job_name)
    assert east_job["TranscriptionJobStatus"] == "COMPLETED"

    # A same-named job in another region is a different job, transcribing
    # different media. S3 buckets are account-scoped rather than region-scoped,
    # so the two transcripts must not collide on one object.
    west.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode="en-US",
        Media={"MediaFileUri": west_uri},
    )
    west_job = _wait_for_status(west, job_name)
    assert west_job["TranscriptionJobStatus"] == "COMPLETED"

    east_uri_out = east_job["Transcript"]["TranscriptFileUri"]
    west_uri_out = west_job["Transcript"]["TranscriptFileUri"]
    assert east_uri_out != west_uri_out

    def _text(transcript_uri):
        out_bucket, key = _split_transcript_uri(transcript_uri)
        document = json.loads(s3.get_object(Bucket=out_bucket, Key=key)["Body"].read())
        return document["results"]["transcripts"][0]["transcript"]

    # Each region's transcript is the one for its own media.
    assert _text(east_uri_out) != _text(west_uri_out)


def test_transcribe_job_state_change_reaches_an_eventbridge_target(
    transcribe, eb, sqs, media
):
    """The hop the reporter's pipeline depends on: S3 -> Lambda -> Transcribe ->
    EventBridge -> Translate/Bedrock. Without the terminal-state event the four
    operations work but nothing downstream of the job ever runs."""
    _, _, media_uri = media
    suffix = uuid.uuid4().hex[:8]
    queue_url = sqs.create_queue(QueueName=f"transcribe-events-{suffix}")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    job_name = f"evented-{suffix}"
    rule_name = f"transcribe-rule-{suffix}"
    eb.put_rule(
        Name=rule_name,
        EventPattern=json.dumps(
            {
                "source": ["aws.transcribe"],
                "detail-type": ["Transcribe Job State Change"],
                "detail": {"TranscriptionJobName": [job_name]},
            }
        ),
        State="ENABLED",
    )
    eb.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": queue_arn}])

    try:
        transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            LanguageCode="en-US",
            Media={"MediaFileUri": media_uri},
        )
        _wait_for_status(transcribe, job_name)

        deadline = time.time() + 15
        messages = []
        while time.time() < deadline and not messages:
            messages = sqs.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1
            ).get("Messages", [])

        assert messages, "no Transcribe Job State Change event was delivered"
        event = json.loads(messages[0]["Body"])
        assert event["source"] == "aws.transcribe"
        assert event["detail-type"] == "Transcribe Job State Change"
        assert event["detail"] == {
            "TranscriptionJobName": job_name,
            "TranscriptionJobStatus": "COMPLETED",
        }
    finally:
        eb.remove_targets(Rule=rule_name, Ids=["1"])
        eb.delete_rule(Name=rule_name)
        sqs.delete_queue(QueueUrl=queue_url)
