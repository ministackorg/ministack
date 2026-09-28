"""IMPORT change sets: what CreateChangeSet refuses or fails, the shape of an
Import change, and that the unsupported execution leaves stack and service
state intact.

Every expected message is the one AWS answers for the same request.
"""

import copy
import json
import re
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from conftest import ENDPOINT, make_client

_MODIFIED = (
    "You have modified resources [X] in your template that are not being imported. "
    "Update, create or delete operations cannot be executed during import operations."
)


@pytest.fixture
def adopted(cfn, sqs, ssm):
    tag = uuid.uuid4().hex[:10]
    qname, stack, pname = f"imp-adopt-{tag}", f"imp-{tag}", f"/imp/{tag}"
    url = sqs.create_queue(QueueName=qname, Attributes={"VisibilityTimeout": "17"})["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="keep this message")
    original = {"Resources": {"X": {"Type": "AWS::SSM::Parameter", "Properties": {
        "Type": "String", "Name": pname, "Value": "keep this value"}}}}
    created = False
    try:
        cfn.create_stack(StackName=stack, TemplateBody=json.dumps(original))
        created = True
        cfn.get_waiter("stack_create_complete").wait(
            StackName=stack, WaiterConfig={"Delay": 1, "MaxAttempts": 30})
        template = copy.deepcopy(original)
        template["Resources"]["Q"] = {
            "Type": "AWS::SQS::Queue", "DeletionPolicy": "Retain", "Properties": {"QueueName": qname},
        }
        to_import = [{"ResourceType": "AWS::SQS::Queue", "LogicalResourceId": "Q",
                      "ResourceIdentifier": {"QueueUrl": url}}]
        yield stack, url, pname, template, to_import
    finally:
        if created:
            cfn.delete_stack(StackName=stack)
            cfn.get_waiter("stack_delete_complete").wait(
                StackName=stack, WaiterConfig={"Delay": 1, "MaxAttempts": 30})
        sqs.delete_queue(QueueUrl=url)


def _create(cfn, adopted, template=None, to_import=None, client=None):
    stack, _url, _pname, default_template, default_import = adopted
    result = (client or cfn).create_change_set(
        StackName=stack, ChangeSetName="imp", ChangeSetType="IMPORT",
        ResourcesToImport=default_import if to_import is None else to_import,
        TemplateBody=json.dumps(default_template if template is None else template))
    return cfn.describe_change_set(ChangeSetName=result["Id"])


def _refusal(cfn, adopted, template=None, to_import=None):
    """The ValidationError message CreateChangeSet answers; no change set is left."""
    with pytest.raises(ClientError) as exc:
        _create(cfn, adopted, template, to_import)
    assert exc.value.response["Error"]["Code"] == "ValidationError"
    assert cfn.list_change_sets(StackName=adopted[0])["Summaries"] == []
    return exc.value.response["Error"]["Message"]


def _failed(cfn, adopted, cs):
    """A change set AWS accepts and then fails: no changes, not executable."""
    assert (cs["Status"], cs["ExecutionStatus"], cs["Changes"]) == ("FAILED", "UNAVAILABLE", [])
    summaries = cfn.list_change_sets(StackName=adopted[0])["Summaries"]
    assert [(s["Status"], s["ExecutionStatus"], s["StatusReason"]) for s in summaries] == [
        ("FAILED", "UNAVAILABLE", cs["StatusReason"])]
    return cs["StatusReason"]


# --- one resource of each looked-up type, outside any stack ---

_TYPES = {
    "sqs": ("AWS::SQS::Queue", "QueueUrl", "QueueName"),
    "sns": ("AWS::SNS::Topic", "TopicArn", "TopicName"),
    "s3": ("AWS::S3::Bucket", "BucketName", "BucketName"),
    "ddb": ("AWS::DynamoDB::Table", "TableName", "TableName"),
    "ssm": ("AWS::SSM::Parameter", "Name", "Name"),
}

_TABLE_PROPS = {
    "BillingMode": "PAY_PER_REQUEST",
    "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
    "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
}

# AWS's StatusReason for an identifier that names nothing, per type.
_NOT_FOUND = {
    "sqs": "Resource of type 'AWS::SQS::Queue' with identifier '{value}' was not found.",
    "sns": ("Topic does not exist (Service: Sns, Status Code: 404, Request ID: {request_id}) "
            "(SDK Attempt Count: 1)"),
    "s3": "Bucket not found",
    "ddb": "Table: {value} does not exist.",
    "ssm": "Resource of type 'AWS::SSM::Parameter' with identifier '{value}' was not found.",
}


def _declaration(kind, name):
    rtype, _key, name_prop = _TYPES[kind]
    props = {name_prop: name}
    if kind == "ddb":
        props.update(_TABLE_PROPS)
    elif kind == "ssm":
        props.update({"Type": "String", "Value": "m"})
    return {"Type": rtype, "DeletionPolicy": "Retain", "Properties": props}


@pytest.fixture
def identifier_value(sts):
    """The ResourceIdentifier value naming a resource of ``kind`` called ``name``,
    in the caller's account and region."""
    account, region = sts.get_caller_identity()["Account"], sts.meta.region_name

    def value(kind, name):
        if kind == "sqs":
            return f"{ENDPOINT}/{account}/{name}"
        if kind == "sns":
            return f"arn:aws:sns:{region}:{account}:{name}"
        return name
    return value


def _name(kind, tag):
    return f"/imp/{kind}-{tag}" if kind == "ssm" else f"imp-{kind}-{tag}"


@pytest.fixture
def outside(request, sqs, sns, s3, ddb, ssm):
    """Create one resource of the parametrized kind; yield its name."""
    kind = request.param
    name = _name(kind, uuid.uuid4().hex[:10])
    if kind == "sqs":
        url = sqs.create_queue(QueueName=name)["QueueUrl"]
        delete, key = sqs.delete_queue, {"QueueUrl": url}
    elif kind == "sns":
        arn = sns.create_topic(Name=name)["TopicArn"]
        delete, key = sns.delete_topic, {"TopicArn": arn}
    elif kind == "s3":
        s3.create_bucket(Bucket=name)
        delete, key = s3.delete_bucket, {"Bucket": name}
    elif kind == "ddb":
        ddb.create_table(TableName=name, **_TABLE_PROPS)
        delete, key = ddb.delete_table, {"TableName": name}
    else:
        ssm.put_parameter(Name=name, Type="String", Value="m")
        delete, key = ssm.delete_parameter, {"Name": name}
    try:
        yield kind, name
    finally:
        delete(**key)


def _import_of(kind, value):
    rtype, key, _prop = _TYPES[kind]
    return [{"ResourceType": rtype, "LogicalResourceId": "Q", "ResourceIdentifier": {key: value}}]


# --- accepted imports ---

def test_import_change_set_describes_only_imports_and_is_unavailable(cfn, adopted):
    cs = _create(cfn, adopted)
    assert cs["Status"] == "CREATE_COMPLETE"
    # AWS answers AVAILABLE and executes the import; the emulator refuses it by design.
    assert cs["ExecutionStatus"] == "UNAVAILABLE"
    assert "import" in cs["StatusReason"].lower()
    assert [(r["Type"], r["ResourceChange"]["LogicalResourceId"], r["ResourceChange"]["Action"],
             r["ResourceChange"]["ResourceType"]) for r in cs["Changes"]] == [
        ("Resource", "Q", "Import", "AWS::SQS::Queue")]


@pytest.mark.parametrize("outside", list(_TYPES), indirect=True)
def test_import_change_names_the_resource_and_has_no_replacement(cfn, adopted, outside, identifier_value):
    # AWS: PhysicalResourceId is the identifier value, and an Import change has
    # no Replacement member at all (botocore would turn an empty element into "").
    kind, name = outside
    template = copy.deepcopy(adopted[3])
    template["Resources"]["Q"] = _declaration(kind, name)
    to_import = _import_of(kind, identifier_value(kind, name))
    cs = _create(cfn, adopted, template, to_import)
    assert cs["Status"] == "CREATE_COMPLETE"
    assert cs["Changes"] == [{"Type": "Resource", "ResourceChange": {
        "Action": "Import",
        "LogicalResourceId": "Q",
        "PhysicalResourceId": identifier_value(kind, name),
        "ResourceType": _TYPES[kind][0],
        "Scope": [],
        "Details": [],
    }}]


def test_update_change_set_changes_keep_their_shape(cfn, ssm):
    # Guard, not a fix: the Import change's shape must not leak into the
    # Add / Modify / Remove changes of an ordinary UPDATE change set.
    tag = uuid.uuid4().hex[:10]
    stack = f"imp-upd-{tag}"

    def param(suffix, value="v"):
        return {"Type": "AWS::SSM::Parameter",
                "Properties": {"Type": "String", "Name": f"/imp/upd-{tag}-{suffix}", "Value": value}}

    cfn.create_stack(StackName=stack, TemplateBody=json.dumps(
        {"Resources": {"X": param("x"), "Y": param("y")}}))
    try:
        cfn.get_waiter("stack_create_complete").wait(
            StackName=stack, WaiterConfig={"Delay": 1, "MaxAttempts": 30})
        cs_id = cfn.create_change_set(
            StackName=stack, ChangeSetName="upd", ChangeSetType="UPDATE",
            TemplateBody=json.dumps({"Resources": {"X": param("x", "w"), "Z": param("z")}}))["Id"]
        changes = {c["ResourceChange"]["LogicalResourceId"]: c["ResourceChange"]
                   for c in cfn.describe_change_set(ChangeSetName=cs_id)["Changes"]}
        assert {k: v["Action"] for k, v in changes.items()} == {"X": "Modify", "Y": "Remove", "Z": "Add"}
        assert "Replacement" in changes["X"]
        assert "PhysicalResourceId" not in changes["Z"]
    finally:
        cfn.delete_stack(StackName=stack)
        cfn.get_waiter("stack_delete_complete").wait(
            StackName=stack, WaiterConfig={"Delay": 1, "MaxAttempts": 30})


# --- accepted, then FAILED: the identifier names nothing ---

@pytest.mark.parametrize("kind", list(_TYPES))
def test_import_of_a_resource_that_does_not_exist_fails_the_change_set(cfn, adopted, kind, identifier_value):
    name = _name(kind, uuid.uuid4().hex[:10]) + "-ghost"
    template = copy.deepcopy(adopted[3])
    template["Resources"]["Q"] = _declaration(kind, name)
    cs = _create(cfn, adopted, template, _import_of(kind, identifier_value(kind, name)))
    reason = _failed(cfn, adopted, cs)
    expected = re.escape(_NOT_FOUND[kind]).replace(
        re.escape("{value}"), re.escape(identifier_value(kind, name))).replace(
        re.escape("{request_id}"), "[0-9a-f-]{36}")
    assert re.fullmatch(expected, reason), reason


# AWS's StatusReason for a blank identifier, per type and value: the resource
# handler's own lookup error where the service refuses the value outright.
_SDK = r" \(Service: {service}, Status Code: 400, Request ID: {request_id}\) \(SDK Attempt Count: 1\)"
_UUID = "[0-9a-f-]{36}"
_TABLE_NAME = ("2 validation errors detected: Value '{v}' at 'tableName' failed to satisfy constraint: "
               "Member must have length greater than or equal to 3; Value '{v}' at 'tableName' failed "
               "to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+")
_BLANK = {
    ("sqs", " "): re.escape("Resource of type 'AWS::SQS::Queue' with identifier ' ' was not found."),
    ("sqs", ""): re.escape("QueueUrl is not found"),
    ("sns", " "): re.escape("Invalid parameter: TopicArn Reason: An ARN must have at least 6 elements, "
                            "not 1") + _SDK.format(service="Sns", request_id=_UUID),
    ("s3", " "): re.escape("Bucket not found"),
    ("s3", ""): re.escape("Unable to marshall request to JSON: Bucket cannot be empty."),
    ("ddb", " "): re.escape(_TABLE_NAME.format(v=" ")) + _SDK.format(
        service="DynamoDb", request_id="[0-9A-Z]{52}"),
    ("ddb", ""): re.escape(_TABLE_NAME.format(v="")) + _SDK.format(
        service="DynamoDb", request_id="[0-9A-Z]{52}"),
    ("ssm", " "): re.escape("Resource of type 'AWS::SSM::Parameter' with identifier ' ' was not found."),
    ("ssm", ""): re.escape(
        "1 validation error detected: Value '[]' at 'names' failed to satisfy constraint: Member must "
        "satisfy constraint: [Member must have length less than or equal to 2048, Member must have "
        "length greater than or equal to 1]") + _SDK.format(service="Ssm", request_id=_UUID),
}
_BLANK[("sns", "")] = _BLANK[("sns", " ")]


@pytest.mark.parametrize("kind, value", sorted(_BLANK), ids=lambda v: v if v.strip() else repr(v))
def test_blank_identifier_fails_the_change_set(cfn, adopted, kind, value):
    # botocore refuses an empty value client-side; AWS itself accepts the
    # request and fails the change set.
    unvalidated = make_client("cloudformation", {"parameter_validation": False})
    template = copy.deepcopy(adopted[3])
    template["Resources"]["Q"] = _declaration(kind, _name(kind, uuid.uuid4().hex[:10]))
    to_import = [{"ResourceType": _TYPES[kind][0], "LogicalResourceId": "Q",
                  "ResourceIdentifier": {_TYPES[kind][1]: value}}]
    cs = _create(cfn, adopted, template, to_import, client=unvalidated)
    reason = _failed(cfn, adopted, cs)
    assert re.fullmatch(_BLANK[(kind, value)], reason), reason


def test_queue_in_another_region_is_not_found(cfn, adopted):
    # The lookup runs in the stack's region: a queue of the same account in
    # another region is not the one being imported.
    west = boto3.client("sqs", endpoint_url=ENDPOINT, region_name="us-west-2",
                        aws_access_key_id="test", aws_secret_access_key="test",
                        config=Config(region_name="us-west-2", retries={"mode": "standard"}))
    name = f"imp-west-{uuid.uuid4().hex[:10]}"
    url = west.create_queue(QueueName=name)["QueueUrl"]
    try:
        template = copy.deepcopy(adopted[3])
        template["Resources"]["Q"] = _declaration("sqs", name)
        to_import = [{"ResourceType": "AWS::SQS::Queue", "LogicalResourceId": "Q",
                      "ResourceIdentifier": {"QueueUrl": url}}]
        cs = _create(cfn, adopted, template, to_import)
        assert _failed(cfn, adopted, cs) == (
            f"Resource of type 'AWS::SQS::Queue' with identifier '{url}' was not found.")
    finally:
        west.delete_queue(QueueUrl=url)


def _owner_stack(cfn, qname, retain=False):
    """A stack holding one queue named ``qname``; returns its StackId."""
    queue = {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": qname}}
    if retain:
        queue["DeletionPolicy"] = "Retain"
    stack = f"imp-own-{uuid.uuid4().hex[:10]}"
    stack_id = cfn.create_stack(StackName=stack, TemplateBody=json.dumps({"Resources": {"K": queue}}))["StackId"]
    cfn.get_waiter("stack_create_complete").wait(StackName=stack, WaiterConfig={"Delay": 1, "MaxAttempts": 30})
    return stack_id


def _drop_stack(cfn, stack):
    cfn.delete_stack(StackName=stack)
    cfn.get_waiter("stack_delete_complete").wait(StackName=stack, WaiterConfig={"Delay": 1, "MaxAttempts": 30})


def _import_queue(qname, url):
    template = {"Type": "AWS::SQS::Queue", "DeletionPolicy": "Retain", "Properties": {"QueueName": qname}}
    to_import = [{"ResourceType": "AWS::SQS::Queue", "LogicalResourceId": "Q",
                  "ResourceIdentifier": {"QueueUrl": url}}]
    return template, to_import


def test_import_of_a_queue_another_stack_holds_fails_the_change_set(cfn, sqs, adopted):
    # The URL read through the SQS API and the stack's physical id name the
    # same queue even where their hosts differ.
    qname = f"imp-owned-{uuid.uuid4().hex[:10]}"
    owner = _owner_stack(cfn, qname)
    try:
        url = sqs.get_queue_url(QueueName=qname)["QueueUrl"]
        template = copy.deepcopy(adopted[3])
        template["Resources"]["Q"], to_import = _import_queue(qname, url)
        cs = _create(cfn, adopted, template, to_import)
        assert _failed(cfn, adopted, cs) == f"{url} already exists in stack {owner}"
    finally:
        _drop_stack(cfn, owner)


def test_import_of_a_resource_this_stack_holds_under_another_id_fails_the_change_set(cfn, adopted):
    stack, _url, pname, template, _to_import = adopted
    del template["Resources"]["Q"]
    template["Resources"]["Y"] = {"Type": "AWS::SSM::Parameter", "DeletionPolicy": "Retain",
                                  "Properties": {"Type": "String", "Name": pname, "Value": "keep this value"}}
    to_import = [{"ResourceType": "AWS::SSM::Parameter", "LogicalResourceId": "Y",
                  "ResourceIdentifier": {"Name": pname}}]
    cs = _create(cfn, adopted, template, to_import)
    stack_id = cfn.describe_stacks(StackName=stack)["Stacks"][0]["StackId"]
    assert _failed(cfn, adopted, cs) == f"{pname} already exists in stack {stack_id}"


def test_a_queue_deleted_behind_its_stack_is_not_found(cfn, sqs, adopted):
    # AWS looks the resource up before it asks which stack holds it.
    qname = f"imp-gone-{uuid.uuid4().hex[:10]}"
    owner = _owner_stack(cfn, qname)
    try:
        url = sqs.get_queue_url(QueueName=qname)["QueueUrl"]
        sqs.delete_queue(QueueUrl=url)
        template = copy.deepcopy(adopted[3])
        template["Resources"]["Q"], to_import = _import_queue(qname, url)
        cs = _create(cfn, adopted, template, to_import)
        assert _failed(cfn, adopted, cs) == (
            f"Resource of type 'AWS::SQS::Queue' with identifier '{url}' was not found.")
    finally:
        _drop_stack(cfn, owner)


def test_a_queue_retained_by_a_deleted_stack_can_be_imported(cfn, sqs, adopted):
    qname = f"imp-kept-{uuid.uuid4().hex[:10]}"
    owner = _owner_stack(cfn, qname, retain=True)
    try:
        _drop_stack(cfn, owner)
        url = sqs.get_queue_url(QueueName=qname)["QueueUrl"]
        template = copy.deepcopy(adopted[3])
        template["Resources"]["Q"], to_import = _import_queue(qname, url)
        cs = _create(cfn, adopted, template, to_import)
        assert cs["Status"] == "CREATE_COMPLETE"
        assert [c["ResourceChange"]["Action"] for c in cs["Changes"]] == ["Import"]
    finally:
        _drop_stack(cfn, owner)
        sqs.delete_queue(QueueUrl=sqs.get_queue_url(QueueName=qname)["QueueUrl"])


# --- refused at creation ---

def test_import_without_resources_to_import_is_refused(cfn, adopted):
    stack, _url, _pname, template, _to_import = adopted
    with pytest.raises(ClientError) as exc:
        cfn.create_change_set(StackName=stack, ChangeSetName="imp", ChangeSetType="IMPORT",
                              TemplateBody=json.dumps(template))
    assert exc.value.response["Error"]["Code"] == "ValidationError"
    assert exc.value.response["Error"]["Message"] == "Must Provide at least one resource to import"
    assert cfn.list_change_sets(StackName=stack)["Summaries"] == []


def _mismatch(listed, declared):
    return ("Resource type of [Q] passed in ResourceToImport does not match with resource type defined "
            f"in the template. Resource type in ResourceToImport: {listed}; Resource type in Template: "
            f"{declared}.")


_ENTRY_FAULTS = {
    "missing-resource":
        "The logical resource ids [Missing] provided in ResourceToImport do not exist in the template.",
    "wrong-key": "Invalid resource identifier for resource type AWS::SNS::Topic. Expected [TopicArn]",
    "type-mismatch": _mismatch("AWS::SNS::Topic", "AWS::SQS::Queue"),
    "unlooked-type-mismatch": _mismatch("AWS::IAM::Role", "AWS::SQS::Queue"),
    "missing-identifier": (
        "1 validation error detected: Value null at 'resourcesToImport.1.member.resourceIdentifier' "
        "failed to satisfy constraint: Member must not be null"),
    "duplicate": "Every resource to import must have unique LogicalResourceId",
    "queue-name-key": "Invalid resource identifier for resource type AWS::SQS::Queue. Expected [QueueUrl]",
    "extra-key": "Invalid resource identifier for resource type AWS::SQS::Queue. Expected [QueueUrl]",
}


@pytest.mark.parametrize("fault", list(_ENTRY_FAULTS))
def test_invalid_import_entries_are_rejected(cfn, adopted, fault):
    _stack, _url, _pname, template, to_import = adopted
    if fault == "missing-resource":
        to_import[0]["LogicalResourceId"] = "Missing"
    elif fault == "wrong-key":
        # Two faults: AWS names the identifier key before the type.
        to_import[0]["ResourceType"] = "AWS::SNS::Topic"
    elif fault == "type-mismatch":
        to_import[0].update(ResourceType="AWS::SNS::Topic",
                            ResourceIdentifier={"TopicArn": "arn:aws:sns:us-east-1:000000000000:t"})
    elif fault == "unlooked-type-mismatch":
        to_import[0].update(ResourceType="AWS::IAM::Role", ResourceIdentifier={"RoleName": "r"})
    elif fault == "missing-identifier":
        to_import[0]["ResourceIdentifier"] = {}
    elif fault == "queue-name-key":
        to_import[0]["ResourceIdentifier"] = {"QueueName": template["Resources"]["Q"]["Properties"]["QueueName"]}
    elif fault == "extra-key":
        to_import[0]["ResourceIdentifier"]["Bogus"] = "x"
    else:
        to_import.append(copy.deepcopy(to_import[0]))
    assert _refusal(cfn, adopted, template, to_import) == _ENTRY_FAULTS[fault]


def _with_x(template, value):
    changed = copy.deepcopy(template)
    changed["Resources"]["X"]["Properties"]["Value"] = value
    return changed


def _with_n(template, pname):
    changed = copy.deepcopy(template)
    changed["Resources"]["N"] = {"Type": "AWS::SSM::Parameter", "DeletionPolicy": "Retain",
                                 "Properties": {"Type": "String", "Name": pname + "-new", "Value": "m"}}
    return changed


def test_import_that_also_adds_a_resource_is_refused(cfn, adopted):
    _stack, _url, pname, template, _to_import = adopted
    assert _refusal(cfn, adopted, _with_n(template, pname)) == (
        "Resources [N] is missing from ResourceToImport list")


def test_import_that_modifies_a_stack_resource_is_refused(cfn, adopted):
    template = adopted[3]
    assert _refusal(cfn, adopted, _with_x(template, "changed")) == _MODIFIED


def test_import_that_removes_a_stack_resource_is_refused(cfn, adopted):
    template = copy.deepcopy(adopted[3])
    del template["Resources"]["X"]
    assert _refusal(cfn, adopted, template) == _MODIFIED


def test_import_that_renames_a_stack_queue_is_refused_and_keeps_the_queue(cfn, sqs, adopted):
    # Executing such a set once created the renamed queue and left the
    # original outside the stack; AWS refuses the set at creation.
    _stack, url, _pname, _template, to_import = adopted
    kept = f"imp-held-{uuid.uuid4().hex[:10]}"
    owner = _owner_stack(cfn, kept)
    try:
        held = cfn.describe_stack_resources(StackName=owner)["StackResources"]
        queue = sqs.get_queue_url(QueueName=kept)["QueueUrl"]
        template = {"Resources": {
            "K": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": kept + "-renamed"}},
            "Q": copy.deepcopy(adopted[3]["Resources"]["Q"]),
        }}
        with pytest.raises(ClientError) as exc:
            cfn.create_change_set(StackName=owner, ChangeSetName="imp", ChangeSetType="IMPORT",
                                  ResourcesToImport=to_import, TemplateBody=json.dumps(template))
        assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (
            "ValidationError", _MODIFIED.replace("[X]", "[K]"))
        assert cfn.list_change_sets(StackName=owner)["Summaries"] == []
        assert cfn.describe_stack_resources(StackName=owner)["StackResources"] == held
        assert sqs.get_queue_url(QueueName=kept)["QueueUrl"] == queue
        with pytest.raises(ClientError) as exc:
            sqs.get_queue_url(QueueName=kept + "-renamed")
        assert exc.value.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue"
    finally:
        _drop_stack(cfn, owner)


def test_an_unimported_add_is_reported_before_a_modification(cfn, adopted):
    _stack, _url, pname, template, _to_import = adopted
    assert _refusal(cfn, adopted, _with_n(_with_x(template, "changed"), pname)) == (
        "Resources [N] is missing from ResourceToImport list")


def test_import_without_a_new_resource_is_refused(cfn, adopted):
    _stack, _url, pname, template, _to_import = adopted
    del template["Resources"]["Q"]
    to_import = [{"ResourceType": "AWS::SSM::Parameter", "LogicalResourceId": "X",
                  "ResourceIdentifier": {"Name": pname}}]
    assert _refusal(cfn, adopted, template, to_import) == (
        "The template should contain at least one new resource to import.")


def test_import_of_a_resource_already_in_the_stack_is_refused(cfn, adopted):
    _stack, _url, pname, template, _to_import = adopted
    del template["Resources"]["Q"]
    to_import = [{"ResourceType": "AWS::SSM::Parameter", "LogicalResourceId": "X",
                  "ResourceIdentifier": {"Name": pname}}]
    assert _refusal(cfn, adopted, _with_n(template, pname), to_import) == (
        "Resources [X] passed in ResourceToImport are already in a stack and cannot be imported.")


@pytest.mark.parametrize("ghost", [False, True])
def test_import_without_deletion_policy_is_refused(cfn, adopted, ghost):
    # Refused at creation even when the queue does not exist: the missing
    # DeletionPolicy is found before the lookup that would fail the set.
    _stack, url, _pname, template, to_import = adopted
    del template["Resources"]["Q"]["DeletionPolicy"]
    if ghost:
        to_import[0]["ResourceIdentifier"]["QueueUrl"] = url + "-ghost"
    assert _refusal(cfn, adopted, template, to_import) == (
        "The following resources to import [Q] must have DeletionPolicy attribute specified in the template.")


# --- a stack that does not exist yet ---

@pytest.fixture
def fresh(cfn, sqs):
    """A new stack name and a queue outside every stack, to import into it."""
    tag = uuid.uuid4().hex[:10]
    qname, stack = f"imp-fresh-{tag}", f"imp-fresh-{tag}"
    url = sqs.create_queue(QueueName=qname)["QueueUrl"]
    try:
        yield stack, qname, url
    finally:
        _drop_stack(cfn, stack)
        sqs.delete_queue(QueueUrl=url)


def _stack_row(cfn, stack):
    s = cfn.describe_stacks(StackName=stack)["Stacks"][0]
    return s["StackStatus"], s.get("StackStatusReason"), s.get("ChangeSetId")


def test_import_into_a_new_stack_creates_it_for_review(cfn, fresh):
    stack, qname, url = fresh
    template, to_import = _import_queue(qname, url)
    body = json.dumps({"Resources": {"Q": template}})
    cs_id = cfn.create_change_set(StackName=stack, ChangeSetName="imp", ChangeSetType="IMPORT",
                                  ResourcesToImport=to_import, TemplateBody=body)["Id"]
    cs = cfn.describe_change_set(ChangeSetName=cs_id)
    # AWS answers AVAILABLE and executes the import; the emulator refuses it by design.
    assert (cs["Status"], cs["ExecutionStatus"]) == ("CREATE_COMPLETE", "UNAVAILABLE")
    assert cs["Changes"] == [{"Type": "Resource", "ResourceChange": {
        "Action": "Import", "LogicalResourceId": "Q", "PhysicalResourceId": url,
        "ResourceType": "AWS::SQS::Queue", "Scope": [], "Details": []}}]
    assert _stack_row(cfn, stack) == ("REVIEW_IN_PROGRESS", "User Initiated", None)
    assert cfn.describe_stack_resources(StackName=stack)["StackResources"] == []
    events = cfn.describe_stack_events(StackName=stack)["StackEvents"]
    assert [(e["ResourceType"], e["ResourceStatus"], e.get("ResourceStatusReason")) for e in events] == [
        ("AWS::CloudFormation::Stack", "REVIEW_IN_PROGRESS", "User Initiated")]
    assert [s["ChangeSetName"] for s in cfn.list_change_sets(StackName=stack)["Summaries"]] == ["imp"]
    with pytest.raises(ClientError) as exc:
        cfn.execute_change_set(ChangeSetName=cs_id)
    # AWS executes the import; the emulator refuses it by design.
    assert exc.value.response["Error"]["Code"] == "InvalidChangeSetStatus"
    # A second import set against the stack under review is accepted too, and
    # deleting the change sets leaves the stack for DeleteStack to remove.
    again = cfn.create_change_set(StackName=stack, ChangeSetName="again", ChangeSetType="IMPORT",
                                  ResourcesToImport=to_import, TemplateBody=body)["Id"]
    assert cfn.describe_change_set(ChangeSetName=again)["Status"] == "CREATE_COMPLETE"
    cfn.delete_change_set(ChangeSetName=cs_id)
    cfn.delete_change_set(ChangeSetName=again)
    assert _stack_row(cfn, stack) == ("REVIEW_IN_PROGRESS", "User Initiated", None)
    _drop_stack(cfn, stack)
    with pytest.raises(ClientError) as exc:
        cfn.describe_stacks(StackName=stack)
    assert exc.value.response["Error"]["Message"] == f"Stack with id {stack} does not exist"


@pytest.mark.parametrize("fault", ["unlisted", "no-deletion-policy"])
def test_refused_import_into_a_new_stack_leaves_no_stack(cfn, fresh, fault):
    stack, qname, url = fresh
    template, to_import = _import_queue(qname, url)
    resources = {"Q": template}
    if fault == "unlisted":
        resources["X"] = {"Type": "AWS::SSM::Parameter",
                          "Properties": {"Type": "String", "Name": f"/{stack}", "Value": "v"}}
        message = "Resources [X] is missing from ResourceToImport list"
    else:
        del template["DeletionPolicy"]
        message = ("The following resources to import [Q] must have DeletionPolicy attribute specified "
                   "in the template.")
    with pytest.raises(ClientError) as exc:
        cfn.create_change_set(StackName=stack, ChangeSetName="imp", ChangeSetType="IMPORT",
                              ResourcesToImport=to_import, TemplateBody=json.dumps({"Resources": resources}))
    assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (
        "ValidationError", message)
    with pytest.raises(ClientError) as exc:
        cfn.describe_stacks(StackName=stack)
    assert exc.value.response["Error"]["Message"] == f"Stack with id {stack} does not exist"


def test_failed_import_into_a_new_stack_leaves_the_stack_for_review(cfn, fresh):
    stack, qname, url = fresh
    template, to_import = _import_queue(qname + "-ghost", url + "-ghost")
    cs_id = cfn.create_change_set(StackName=stack, ChangeSetName="imp", ChangeSetType="IMPORT",
                                  ResourcesToImport=to_import,
                                  TemplateBody=json.dumps({"Resources": {"Q": template}}))["Id"]
    cs = cfn.describe_change_set(ChangeSetName=cs_id)
    assert (cs["Status"], cs["ExecutionStatus"], cs["Changes"]) == ("FAILED", "UNAVAILABLE", [])
    assert cs["StatusReason"] == (
        f"Resource of type 'AWS::SQS::Queue' with identifier '{url}-ghost' was not found.")
    assert _stack_row(cfn, stack) == ("REVIEW_IN_PROGRESS", "User Initiated", None)


# --- execution stays refused and harmless ---

@pytest.mark.parametrize("by_arn", [False, True])
def test_repeated_execution_preserves_stack_and_service_resources(cfn, sqs, ssm, adopted, by_arn):
    stack, url, pname, _template, _to_import = adopted
    cs = _create(cfn, adopted)
    before = cfn.describe_stacks(StackName=stack)["Stacks"]
    resources = cfn.describe_stack_resources(StackName=stack)["StackResources"]
    events = cfn.describe_stack_events(StackName=stack)["StackEvents"]
    parameter = ssm.get_parameter(Name=pname)["Parameter"]
    template = cfn.get_template(StackName=stack)["TemplateBody"]
    target = {"ChangeSetName": cs["ChangeSetId"]} if by_arn else {"StackName": stack, "ChangeSetName": "imp"}
    for _ in range(2):
        with pytest.raises(ClientError) as exc:
            cfn.execute_change_set(**target)
        # AWS executes the import; the emulator refuses it by design.
        assert exc.value.response["Error"]["Code"] == "InvalidChangeSetStatus"
        assert "import" in exc.value.response["Error"]["Message"].lower()
    assert cfn.describe_stacks(StackName=stack)["Stacks"] == before
    assert cfn.describe_stack_resources(StackName=stack)["StackResources"] == resources
    assert cfn.describe_stack_events(StackName=stack)["StackEvents"] == events
    assert cfn.get_template(StackName=stack)["TemplateBody"] == template
    assert ssm.get_parameter(Name=pname)["Parameter"] == parameter
    attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["VisibilityTimeout"])["Attributes"]
    assert attrs["VisibilityTimeout"] == "17"
    assert sqs.receive_message(QueueUrl=url)["Messages"][0]["Body"] == "keep this message"
