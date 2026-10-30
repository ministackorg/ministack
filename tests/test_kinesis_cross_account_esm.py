"""Black-box tests for Lambda event source mappings on another account's Kinesis stream.

Everything goes through the public APIs of a running MiniStack: the producer
account creates and writes the stream and its resource policy, the consumer
account owns the function and the mapping, and what the function received is
read back from its CloudWatch Logs. Nothing is patched or reached into.
"""

import io
import json
import os
import re
import time
import uuid
import zipfile

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

PRODUCER = "111111111111"
CONSUMER = "222222222222"
REGION = "us-east-1"
ROLE_NAME = "xacct-consumer-role"
ROLE_ARN = f"arn:aws:iam::{CONSUMER}:role/{ROLE_NAME}"
OTHER_ROLE_ARN = f"arn:aws:iam::{CONSUMER}:role/xacct-someone-else"
OTHER_ACCOUNT = "333333333333"
READ_ACTIONS = [
    "kinesis:DescribeStream", "kinesis:DescribeStreamSummary",
    "kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:ListShards",
]

# Prints one line per record it is given, so a test sees exactly what arrived.
HANDLER = """
import base64, json

def handler(event, context):
    for r in event["Records"]:
        print("XREC " + json.dumps({
            "data": base64.b64decode(r["kinesis"]["data"]).decode(),
            "partitionKey": r["kinesis"]["partitionKey"],
            "sequenceNumber": r["kinesis"]["sequenceNumber"],
            "eventSourceARN": r["eventSourceARN"],
            "eventSource": r["eventSource"],
            "awsRegion": r["awsRegion"],
        }))
    return len(event["Records"])
"""


def _client(service, account_id, region=REGION):
    return boto3.client(
        service, endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        region_name=region, aws_access_key_id=account_id, aws_secret_access_key="test",
        config=Config(retries={"total_max_attempts": 1}),
    )


def _policy(stream_arn, principal=ROLE_ARN, actions=READ_ACTIONS, extra=()):
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": principal},
            "Action": actions,
            "Resource": stream_arn,
        }, *extra],
    })


def _code(exc_info):
    return exc_info.value.response["Error"]["Code"]


class Accounts:
    """A producer stream and consumer functions, cleaned up afterwards."""

    def __init__(self):
        self.kin = _client("kinesis", PRODUCER)
        self.consumer_kin = _client("kinesis", CONSUMER)
        self.lam = _client("lambda", CONSUMER)
        self.logs = _client("logs", CONSUMER)
        self._cleanup = []
        # A policy may only name principals that exist: the function's role, a
        # second role, and a third account. The producer and third accounts hold an IAM
        # user, so MiniStack knows they exist, as they do on AWS.
        iam = _client("iam", CONSUMER)
        for role in (ROLE_NAME, OTHER_ROLE_ARN.rsplit("/", 1)[1]):
            try:
                iam.create_role(RoleName=role, AssumeRolePolicyDocument=json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"},
                                   "Action": "sts:AssumeRole"}],
                }))
            except iam.exceptions.EntityAlreadyExistsException:
                pass
        for account in (PRODUCER, OTHER_ACCOUNT):
            try:  # MiniStack knows an account once it holds an IAM identity
                _client("iam", account).create_user(UserName="xacct-account-user")
            except ClientError:
                pass

    def stream(self, client=None, shards=1, name=None):
        client = client or self.kin
        name = name or f"xacct-{uuid.uuid4().hex[:10]}"
        client.create_stream(StreamName=name, ShardCount=shards)
        self._cleanup.append(lambda: client.delete_stream(StreamName=name))
        arn = client.describe_stream_summary(StreamName=name)["StreamDescriptionSummary"]["StreamARN"]
        return name, arn

    def function(self):
        name = f"xacct-fn-{uuid.uuid4().hex[:10]}"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("index.py", HANDLER)
        self.lam.create_function(
            FunctionName=name, Runtime="python3.12", Handler="index.handler",
            Role=ROLE_ARN, Code={"ZipFile": buf.getvalue()},
        )
        self._cleanup.append(lambda: self.lam.delete_function(FunctionName=name))
        return name

    def mapping(self, function, stream_arn, **kwargs):
        kwargs.setdefault("StartingPosition", "TRIM_HORIZON")
        esm = self.lam.create_event_source_mapping(FunctionName=function, EventSourceArn=stream_arn, **kwargs)
        self._cleanup.append(lambda: self.lam.delete_event_source_mapping(UUID=esm["UUID"]))
        return esm["UUID"]

    def received(self, function):
        """Every record the function has logged, in log order."""
        try:
            events = self.logs.filter_log_events(logGroupName=f"/aws/lambda/{function}", limit=10000)["events"]
        except ClientError:
            return []  # no log group until the first invocation
        return [json.loads(line[len("XREC "):])
                for event in events for line in event["message"].splitlines()
                if line.startswith("XREC ")]

    def wait_received(self, function, count, timeout=30):
        deadline = time.time() + timeout
        records = []
        while time.time() < deadline:
            records = self.received(function)
            if len(records) >= count:
                return records
            time.sleep(0.5)
        return records

    def wait_result(self, esm_uuid, prefix, timeout=30):
        deadline = time.time() + timeout
        result = ""
        while time.time() < deadline:
            result = self.lam.get_event_source_mapping(UUID=esm_uuid).get("LastProcessingResult", "")
            if result.startswith(prefix):
                return result
            time.sleep(0.5)
        return result

    def close(self):
        for undo in reversed(self._cleanup):
            try:
                undo()
            except ClientError:
                pass


@pytest.fixture
def accounts():
    a = Accounts()
    yield a
    a.close()


# ---------------------------------------------------------------------------
# CreateEventSourceMapping
# ---------------------------------------------------------------------------

_CANNOT_ACCESS = ("Cannot access stream {arn}. Please ensure the role can perform the GetRecords, GetShardIterator, "
                  "DescribeStream, DescribeStreamSummary, ListShards, and ListStreams Actions on your stream.")


def _refused(exc_info):
    return exc_info.value.response["Error"]


def test_mapping_accepts_another_accounts_stream_its_policy_grants(accounts):
    _name, arn = accounts.stream()
    accounts.kin.put_resource_policy(ResourceARN=arn, Policy=_policy(arn))
    fn = accounts.function()
    esm = accounts.mapping(fn, arn)

    got = accounts.lam.get_event_source_mapping(UUID=esm)
    assert got["EventSourceArn"] == arn
    assert got["FunctionArn"] == f"arn:aws:lambda:{REGION}:{CONSUMER}:function:{fn}"
    listed = accounts.lam.list_event_source_mappings(EventSourceArn=arn)["EventSourceMappings"]
    assert [m["UUID"] for m in listed] == [esm]


def test_mapping_refuses_a_stream_missing_from_the_other_account(accounts):
    fn = accounts.function()
    missing = f"arn:aws:kinesis:{REGION}:{PRODUCER}:stream/xacct-missing-{uuid.uuid4().hex[:8]}"
    with pytest.raises(ClientError) as exc:
        accounts.mapping(fn, missing)
    assert _refused(exc) == {"Code": "InvalidParameterValueException", "Message": _CANNOT_ACCESS.format(arn=missing)}


def test_mapping_refuses_an_account_that_does_not_exist(accounts):
    fn = accounts.function()
    nobody = str(uuid.uuid4().int)[:12].rjust(12, "9")
    arn = f"arn:aws:kinesis:{REGION}:{nobody}:stream/anything"
    with pytest.raises(ClientError) as exc:
        accounts.mapping(fn, arn)
    assert _refused(exc) == {
        "Code": "InvalidParameterValueException",
        "Message": "Received Exception while reading from provided stream. No account found for the given parameters",
    }


def test_mapping_refuses_another_accounts_stream_in_another_region(accounts):
    _name, west_arn = accounts.stream(client=_client("kinesis", PRODUCER, "us-west-2"))
    fn = accounts.function()
    with pytest.raises(ClientError) as exc:
        accounts.mapping(fn, west_arn)
    assert _refused(exc) == {"Code": "InvalidParameterValueException",
                             "Message": f"Event source region must match Lambda region {REGION}"}


# ---------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------

def test_function_receives_every_record_across_shards_and_batches(accounts):
    name, arn = accounts.stream(shards=2)
    accounts.kin.put_resource_policy(ResourceARN=arn, Policy=_policy(arn))
    sent = []
    for i in range(25):
        resp = accounts.kin.put_record(StreamName=name, Data=f"record-{i}".encode(), PartitionKey=f"pk-{i % 5}")
        sent.append((f"record-{i}", f"pk-{i % 5}", resp["SequenceNumber"]))
    fn = accounts.function()
    accounts.mapping(fn, arn, BatchSize=10)

    got = accounts.wait_received(fn, len(sent))
    assert sorted((r["data"], r["partitionKey"], r["sequenceNumber"]) for r in got) == sorted(sent)
    assert {(r["eventSourceARN"], r["eventSource"], r["awsRegion"]) for r in got} == {
        (arn, "aws:kinesis", REGION)}
    for pk in {pk for _d, pk, _s in sent}:
        # Order holds within a partition key, as it does within a shard.
        assert [r["data"] for r in got if r["partitionKey"] == pk] == [d for d, p, _s in sent if p == pk]


def test_latest_mapping_only_delivers_records_written_after_it(accounts):
    name, arn = accounts.stream()
    accounts.kin.put_resource_policy(ResourceARN=arn, Policy=_policy(arn))
    accounts.kin.put_record(StreamName=name, Data=b"before", PartitionKey="pk")
    fn = accounts.function()
    esm = accounts.mapping(fn, arn, StartingPosition="LATEST")
    time.sleep(2)  # let the poller pin the starting position
    accounts.kin.put_record(StreamName=name, Data=b"after", PartitionKey="pk")

    accounts.wait_result(esm, "OK")
    assert [r["data"] for r in accounts.wait_received(fn, 1)] == ["after"]


def test_mapping_reads_the_owners_stream_not_a_same_named_local_one(accounts):
    name, producer_arn = accounts.stream()
    _same, consumer_arn = accounts.stream(client=accounts.consumer_kin, name=name)
    assert producer_arn != consumer_arn
    accounts.kin.put_resource_policy(ResourceARN=producer_arn, Policy=_policy(producer_arn))
    accounts.kin.put_record(StreamName=name, Data=b"from-producer", PartitionKey="pk")
    accounts.consumer_kin.put_record(StreamName=name, Data=b"from-consumer", PartitionKey="pk")

    remote_fn, local_fn = accounts.function(), accounts.function()
    accounts.mapping(remote_fn, producer_arn)
    accounts.mapping(local_fn, consumer_arn)

    assert [(r["data"], r["eventSourceARN"]) for r in accounts.wait_received(remote_fn, 1)] == [
        ("from-producer", producer_arn)]
    assert [(r["data"], r["eventSourceARN"]) for r in accounts.wait_received(local_fn, 1)] == [
        ("from-consumer", consumer_arn)]
    time.sleep(2)  # nothing else turns up later
    assert len(accounts.received(remote_fn)) == 1 and len(accounts.received(local_fn)) == 1


def test_disabled_mapping_waits_until_enabled(accounts):
    name, arn = accounts.stream()
    accounts.kin.put_resource_policy(ResourceARN=arn, Policy=_policy(arn))
    accounts.kin.put_record(StreamName=name, Data=b"held", PartitionKey="pk")
    fn = accounts.function()
    esm = accounts.mapping(fn, arn, Enabled=False)

    time.sleep(3)
    assert accounts.received(fn) == []
    accounts.lam.update_event_source_mapping(UUID=esm, Enabled=True)
    assert [r["data"] for r in accounts.wait_received(fn, 1)] == ["held"]


# ---------------------------------------------------------------------------
# The stream policy decides, when the mapping is created and on every pass
# ---------------------------------------------------------------------------

def _deny_get_records(stream_arn):
    return {"Effect": "Deny", "Principal": {"AWS": ROLE_ARN}, "Action": "kinesis:GetRecords",
            "Resource": stream_arn}


# (principal, actions, extra statements, mapping allowed)
_POLICIES = {
    "execution-role": (ROLE_ARN, READ_ACTIONS, (), True),
    "account-id": (CONSUMER, READ_ACTIONS, (), True),
    "account-root": (f"arn:aws:iam::{CONSUMER}:root", READ_ACTIONS, (), True),
    "other-role": (OTHER_ROLE_ARN, READ_ACTIONS, (), False),
    "other-account": (OTHER_ACCOUNT, READ_ACTIONS, (), False),
    "missing-list-shards": (ROLE_ARN, READ_ACTIONS[:-1], (), False),
    "explicit-deny": (ROLE_ARN, READ_ACTIONS, "deny", False),
    "no-policy": (None, None, (), False),
}


@pytest.mark.auth_contract
@pytest.mark.parametrize("case", list(_POLICIES))
def test_stream_policy_decides_the_mapping_contract(accounts, case):
    """AWS checks the grant whether or not MiniStack enforces IAM, so both modes agree."""
    principal, actions, extra, allowed = _POLICIES[case]
    name, arn = accounts.stream()
    if principal is not None:
        statements = [_deny_get_records(arn)] if extra == "deny" else []
        accounts.kin.put_resource_policy(ResourceARN=arn, Policy=_policy(arn, principal, actions, statements))
    accounts.kin.put_record(StreamName=name, Data=b"payload", PartitionKey="pk")
    fn = accounts.function()

    if allowed:
        esm = accounts.mapping(fn, arn)
        assert [r["data"] for r in accounts.wait_received(fn, 1)] == ["payload"]
        assert accounts.wait_result(esm, "OK").startswith("OK")  # "OK" on AWS
    else:
        with pytest.raises(ClientError) as exc:
            accounts.mapping(fn, arn)
        assert _refused(exc) == {"Code": "InvalidParameterValueException", "Message": _CANNOT_ACCESS.format(arn=arn)}


def _problem(action, arn):
    """LastProcessingResult as AWS words it; the session name is Lambda's own."""
    return re.compile(
        rf"PROBLEM: User: arn:aws:sts::{CONSUMER}:assumed-role/{ROLE_NAME}/awslambda_\d+_\d+ is not authorized "
        rf"to perform: {re.escape(action)} on resource: {re.escape(arn)} because no resource-based policy "
        rf"allows the {re.escape(action)} action$")


@pytest.mark.auth_contract
def test_revoking_and_granting_the_policy_stops_and_resumes_delivery_contract(accounts):
    """Recorded on AWS: revoking the grant stops a running mapping, which names
    DescribeStream; granting again delivers what was held."""
    name, arn = accounts.stream()
    accounts.kin.put_resource_policy(ResourceARN=arn, Policy=_policy(arn))
    fn = accounts.function()
    esm = accounts.mapping(fn, arn)
    accounts.kin.put_record(StreamName=name, Data=b"first", PartitionKey="pk")
    assert [r["data"] for r in accounts.wait_received(fn, 1)] == ["first"]

    accounts.kin.delete_resource_policy(ResourceARN=arn)
    accounts.kin.put_record(StreamName=name, Data=b"second", PartitionKey="pk")
    assert _problem("kinesis:DescribeStream", arn).match(accounts.wait_result(esm, "PROBLEM"))
    time.sleep(2)
    assert [r["data"] for r in accounts.received(fn)] == ["first"]
    assert accounts.lam.get_event_source_mapping(UUID=esm)["State"] == "Enabled"

    # Granting again picks up where delivery stopped.
    accounts.kin.put_resource_policy(ResourceARN=arn, Policy=_policy(arn))
    assert [r["data"] for r in accounts.wait_received(fn, 2)] == ["first", "second"]


@pytest.mark.auth_contract
@pytest.mark.parametrize("missing", READ_ACTIONS)
def test_a_running_mapping_needs_only_get_records_contract(accounts, missing):
    """Recorded on AWS: once created, a mapping keeps delivering without any of
    the other read actions; without GetRecords it stops and names it."""
    name, arn = accounts.stream()
    accounts.kin.put_resource_policy(ResourceARN=arn, Policy=_policy(arn))
    fn = accounts.function()
    esm = accounts.mapping(fn, arn)
    accounts.kin.put_resource_policy(
        ResourceARN=arn, Policy=_policy(arn, actions=[a for a in READ_ACTIONS if a != missing]))
    accounts.kin.put_record(StreamName=name, Data=b"payload", PartitionKey="pk")

    if missing == "kinesis:GetRecords":
        assert _problem(missing, arn).match(accounts.wait_result(esm, "PROBLEM"))
        time.sleep(2)
        assert accounts.received(fn) == []
    else:
        assert [r["data"] for r in accounts.wait_received(fn, 1)] == ["payload"]


def test_only_the_owner_manages_the_stream_policy(accounts):
    _name, arn = accounts.stream()
    policy = _policy(arn)
    accounts.kin.put_resource_policy(ResourceARN=arn, Policy=policy)
    for call in (
        lambda: accounts.consumer_kin.put_resource_policy(ResourceARN=arn, Policy=policy),
        lambda: accounts.consumer_kin.get_resource_policy(ResourceARN=arn),
        lambda: accounts.consumer_kin.delete_resource_policy(ResourceARN=arn),
    ):
        with pytest.raises(ClientError) as exc:
            call()
        assert _code(exc) == "AccessDeniedException"
    assert json.loads(accounts.kin.get_resource_policy(ResourceARN=arn)["Policy"]) == json.loads(policy)
    with pytest.raises(ClientError) as exc:
        accounts.consumer_kin.get_resource_policy(ResourceARN=arn)
    assert _refused(exc)["Message"] == (
        f"User: arn:aws:iam::{CONSUMER}:root is not authorized to perform: kinesis:GetResourcePolicy on resource: "
        f"{arn} because no resource-based policy allows the kinesis:GetResourcePolicy action")
