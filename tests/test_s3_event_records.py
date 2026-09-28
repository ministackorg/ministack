"""The S3 notification record's object block, per the S3 User Guide's
"Event message structure" and "Event notification types and destinations"
pages: a URL-encoded key, the version the write made or removed, a sequencer
that grows with each create and delete, eventVersion 2.6, a delete marker
reported as ObjectRemoved:DeleteMarkerCreated, and one event per object a
DeleteObjects removes. Evidence: official AWS documentation, not a run against
a real AWS account."""

import json
import time
import uuid

import pytest


def _queue(sqs, name):
    url = sqs.create_queue(QueueName=name)["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    return url, arn


def _records(sqs, url, expected, timeout=10):
    """Every S3 record delivered to the queue, waiting until `expected` have arrived."""
    records = []
    deadline = time.time() + timeout
    while len(records) < expected and time.time() < deadline:
        resp = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        for m in resp.get("Messages", []):
            body = json.loads(m["Body"])
            records.extend(body.get("Records", []))
            sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
    return records


@pytest.fixture
def notified(s3, sqs):
    """A bucket whose object events go to a fresh queue: (bucket, queue url, configure)."""
    suffix = uuid.uuid4().hex[:8]
    bucket = f"evt-records-{suffix}"
    s3.create_bucket(Bucket=bucket)
    url, arn = _queue(sqs, f"evt-records-{suffix}")

    def configure(*events, versioned=False):
        if versioned:
            s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        s3.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration={"QueueConfigurations": [{"QueueArn": arn, "Events": list(events)}]},
        )
        # The configuration's own s3:TestEvent carries no Records; drain it.
        _records(sqs, url, 0, timeout=1)

    return bucket, url, configure


def test_record_key_is_url_encoded(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectCreated:*")
    s3.put_object(Bucket=bucket, Key="photos/red flower+(1).jpg", Body=b"x")
    [record] = _records(sqs, url, 1)
    # "`red flower.jpg` becomes `red+flower.jpg`"; slashes stay, a literal + is escaped.
    assert record["s3"]["object"]["key"] == "photos/red+flower%2B%281%29.jpg"
    assert record["eventVersion"] == "2.6"


def test_record_carries_the_version_the_put_made(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectCreated:*", versioned=True)
    first = s3.put_object(Bucket=bucket, Key="doc.txt", Body=b"one")["VersionId"]
    second = s3.put_object(Bucket=bucket, Key="doc.txt", Body=b"two")["VersionId"]
    records = _records(sqs, url, 2)
    assert sorted(r["s3"]["object"]["versionId"] for r in records) == sorted([first, second])


def test_record_has_no_version_in_an_unversioned_bucket(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectCreated:*")
    s3.put_object(Bucket=bucket, Key="plain.txt", Body=b"x")
    [record] = _records(sqs, url, 1)
    assert "versionId" not in record["s3"]["object"]


def test_sequencer_grows_with_each_write(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectCreated:*", "s3:ObjectRemoved:*", versioned=True)
    v1 = s3.put_object(Bucket=bucket, Key="seq.txt", Body=b"1")["VersionId"]
    v2 = s3.put_object(Bucket=bucket, Key="seq.txt", Body=b"2")["VersionId"]
    marker = s3.delete_object(Bucket=bucket, Key="seq.txt")["VersionId"]
    by_version = {r["s3"]["object"]["versionId"]: r["s3"]["object"]["sequencer"] for r in _records(sqs, url, 3)}
    # Compared as S3 says to: left-pad the shorter with zeros, then compare.
    width = max(len(v) for v in by_version.values())
    ordered = [by_version[v].rjust(width, "0") for v in (v1, v2, marker)]
    assert ordered == sorted(ordered) and len(set(ordered)) == 3
    assert all(int(s, 16) >= 0 for s in ordered)


def test_delete_without_version_reports_the_marker(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectRemoved:*", versioned=True)
    s3.put_object(Bucket=bucket, Key="gone.txt", Body=b"x")
    marker = s3.delete_object(Bucket=bucket, Key="gone.txt")["VersionId"]
    [record] = _records(sqs, url, 1)
    assert record["eventName"] == "ObjectRemoved:DeleteMarkerCreated"
    assert record["s3"]["object"]["versionId"] == marker


def test_marker_is_not_an_object_removed_delete(s3, sqs, notified):
    bucket, url, configure = notified
    # Subscribed to permanent deletes only: a marker is its own event type.
    configure("s3:ObjectRemoved:Delete", versioned=True)
    version = s3.put_object(Bucket=bucket, Key="kept.txt", Body=b"x")["VersionId"]
    s3.delete_object(Bucket=bucket, Key="kept.txt")
    s3.delete_object(Bucket=bucket, Key="kept.txt", VersionId=version)
    [record] = _records(sqs, url, 1)
    assert record["eventName"] == "ObjectRemoved:Delete"
    assert record["s3"]["object"]["versionId"] == version
    assert _records(sqs, url, 1, timeout=2) == []


def test_copy_record_names_the_new_version(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectCreated:Copy", versioned=True)
    s3.put_object(Bucket=bucket, Key="src.txt", Body=b"x")
    copied = s3.copy_object(Bucket=bucket, Key="dst.txt", CopySource={"Bucket": bucket, "Key": "src.txt"})
    [record] = _records(sqs, url, 1)
    assert record["eventName"] == "ObjectCreated:Copy"
    assert record["s3"]["object"]["versionId"] == copied["VersionId"]


def test_multipart_record_names_the_new_version(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectCreated:CompleteMultipartUpload", versioned=True)
    upload = s3.create_multipart_upload(Bucket=bucket, Key="big.bin")["UploadId"]
    part = s3.upload_part(Bucket=bucket, Key="big.bin", UploadId=upload, PartNumber=1, Body=b"x" * 16)
    done = s3.complete_multipart_upload(
        Bucket=bucket,
        Key="big.bin",
        UploadId=upload,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part["ETag"]}]},
    )
    [record] = _records(sqs, url, 1)
    assert record["s3"]["object"]["versionId"] == done["VersionId"]


def test_delete_objects_sends_an_event_per_object(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectRemoved:*", versioned=True)
    for k in ("a.txt", "b.txt"):
        s3.put_object(Bucket=bucket, Key=k, Body=b"x")
    version = s3.put_object(Bucket=bucket, Key="c.txt", Body=b"x")["VersionId"]
    s3.delete_objects(
        Bucket=bucket,
        Delete={"Objects": [{"Key": "a.txt"}, {"Key": "b.txt"}, {"Key": "c.txt", "VersionId": version}]},
    )
    records = _records(sqs, url, 3)
    got = sorted((r["s3"]["object"]["key"], r["eventName"]) for r in records)
    assert got == [
        ("a.txt", "ObjectRemoved:DeleteMarkerCreated"),
        ("b.txt", "ObjectRemoved:DeleteMarkerCreated"),
        ("c.txt", "ObjectRemoved:Delete"),
    ]


def test_delete_objects_in_an_unversioned_bucket(s3, sqs, notified):
    bucket, url, configure = notified
    configure("s3:ObjectRemoved:*")
    s3.put_object(Bucket=bucket, Key="u.txt", Body=b"x")
    s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": "u.txt"}, {"Key": "never-there.txt"}]})
    [record] = _records(sqs, url, 1)
    assert (record["s3"]["object"]["key"], record["eventName"]) == ("u.txt", "ObjectRemoved:Delete")
