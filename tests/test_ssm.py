import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError


def _regional_ssm(region_name):
    return boto3.client(
        "ssm",
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region_name,
        config=Config(region_name=region_name, retries={"mode": "standard"}),
    )


def test_ssm_put_get(ssm):
    ssm.put_parameter(Name="/app/db/host", Value="localhost", Type="String")
    resp = ssm.get_parameter(Name="/app/db/host")
    assert resp["Parameter"]["Value"] == "localhost"

def test_ssm_get_by_path(ssm):
    ssm.put_parameter(Name="/app/config/key1", Value="val1", Type="String")
    ssm.put_parameter(Name="/app/config/key2", Value="val2", Type="String")
    resp = ssm.get_parameters_by_path(Path="/app/config", Recursive=True)
    assert len(resp["Parameters"]) >= 2

def test_ssm_overwrite(ssm):
    ssm.put_parameter(Name="/app/overwrite", Value="v1", Type="String")
    ssm.put_parameter(Name="/app/overwrite", Value="v2", Type="String", Overwrite=True)
    resp = ssm.get_parameter(Name="/app/overwrite")
    assert resp["Parameter"]["Value"] == "v2"

def test_ssm_put_get_v2(ssm):
    ssm.put_parameter(Name="/ssm2/pg/host", Value="db.local", Type="String")
    resp = ssm.get_parameter(Name="/ssm2/pg/host")
    assert resp["Parameter"]["Value"] == "db.local"
    assert resp["Parameter"]["Type"] == "String"
    assert resp["Parameter"]["Version"] == 1

    ssm.put_parameter(Name="/ssm2/pg/pass", Value="secret123", Type="SecureString")
    resp_enc = ssm.get_parameter(Name="/ssm2/pg/pass", WithDecryption=True)
    assert resp_enc["Parameter"]["Value"] == "secret123"

def test_ssm_overwrite_version_v2(ssm):
    ssm.put_parameter(Name="/ssm2/ov/p", Value="v1", Type="String")
    r1 = ssm.get_parameter(Name="/ssm2/ov/p")
    assert r1["Parameter"]["Version"] == 1

    ssm.put_parameter(Name="/ssm2/ov/p", Value="v2", Type="String", Overwrite=True)
    r2 = ssm.get_parameter(Name="/ssm2/ov/p")
    assert r2["Parameter"]["Value"] == "v2"
    assert r2["Parameter"]["Version"] == 2

    ssm.put_parameter(Name="/ssm2/ov/p", Value="v3", Type="String", Overwrite=True)
    r3 = ssm.get_parameter(Name="/ssm2/ov/p")
    assert r3["Parameter"]["Version"] == 3

def test_ssm_get_by_path_v2(ssm):
    ssm.put_parameter(Name="/ssm2/path/x", Value="vx", Type="String")
    ssm.put_parameter(Name="/ssm2/path/y", Value="vy", Type="String")
    ssm.put_parameter(Name="/ssm2/path/sub/z", Value="vz", Type="String")

    resp = ssm.get_parameters_by_path(Path="/ssm2/path", Recursive=True)
    names = [p["Name"] for p in resp["Parameters"]]
    assert "/ssm2/path/x" in names
    assert "/ssm2/path/y" in names
    assert "/ssm2/path/sub/z" in names

    resp_shallow = ssm.get_parameters_by_path(Path="/ssm2/path", Recursive=False)
    names_shallow = [p["Name"] for p in resp_shallow["Parameters"]]
    assert "/ssm2/path/x" in names_shallow
    assert "/ssm2/path/sub/z" not in names_shallow

def test_ssm_get_parameters_multiple_v2(ssm):
    ssm.put_parameter(Name="/ssm2/multi/a", Value="va", Type="String")
    ssm.put_parameter(Name="/ssm2/multi/b", Value="vb", Type="String")
    resp = ssm.get_parameters(Names=["/ssm2/multi/a", "/ssm2/multi/b", "/ssm2/multi/nope"])
    assert len(resp["Parameters"]) == 2
    assert any(p["Name"] == "/ssm2/multi/a" for p in resp["Parameters"])
    assert any(p["Name"] == "/ssm2/multi/b" for p in resp["Parameters"])
    assert "/ssm2/multi/nope" in resp["InvalidParameters"]

def test_ssm_delete_v2(ssm):
    ssm.put_parameter(Name="/ssm2/del/tmp", Value="bye", Type="String")
    ssm.delete_parameter(Name="/ssm2/del/tmp")
    with pytest.raises(ClientError) as exc:
        ssm.get_parameter(Name="/ssm2/del/tmp")
    assert exc.value.response["Error"]["Code"] == "ParameterNotFound"
    # Real AWS sends `x-amzn-errortype` on JSON-protocol errors; Java/Go SDK v2 read it.
    assert exc.value.response["ResponseMetadata"]["HTTPHeaders"].get("x-amzn-errortype") == "ParameterNotFound"

    ssm.put_parameter(Name="/ssm2/del/b1", Value="v1", Type="String")
    ssm.put_parameter(Name="/ssm2/del/b2", Value="v2", Type="String")
    resp = ssm.delete_parameters(Names=["/ssm2/del/b1", "/ssm2/del/b2", "/ssm2/del/ghost"])
    assert len(resp["DeletedParameters"]) == 2
    assert "/ssm2/del/ghost" in resp["InvalidParameters"]

def test_ssm_describe_v2(ssm):
    ssm.put_parameter(Name="/ssm2/desc/alpha", Value="va", Type="String", Description="alpha param")
    ssm.put_parameter(Name="/ssm2/desc/beta", Value="vb", Type="SecureString")
    resp = ssm.describe_parameters(
        ParameterFilters=[{"Key": "Name", "Option": "BeginsWith", "Values": ["/ssm2/desc/"]}]
    )
    names = [p["Name"] for p in resp["Parameters"]]
    assert "/ssm2/desc/alpha" in names
    assert "/ssm2/desc/beta" in names

def test_ssm_parameter_history_v2(ssm):
    ssm.put_parameter(Name="/ssm2/hist/h", Value="h1", Type="String", Description="d1")
    ssm.put_parameter(Name="/ssm2/hist/h", Value="h2", Type="String", Overwrite=True, Description="d2")
    ssm.put_parameter(Name="/ssm2/hist/h", Value="h3", Type="String", Overwrite=True, Description="d3")
    resp = ssm.get_parameter_history(Name="/ssm2/hist/h")
    assert len(resp["Parameters"]) == 3
    assert resp["Parameters"][0]["Value"] == "h1"
    assert resp["Parameters"][0]["Version"] == 1
    assert resp["Parameters"][2]["Value"] == "h3"
    assert resp["Parameters"][2]["Version"] == 3

def test_ssm_tags_v2(ssm):
    ssm.put_parameter(Name="/ssm2/tag/t1", Value="v", Type="String")
    ssm.add_tags_to_resource(
        ResourceType="Parameter",
        ResourceId="/ssm2/tag/t1",
        Tags=[{"Key": "team", "Value": "platform"}, {"Key": "env", "Value": "staging"}],
    )
    resp = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId="/ssm2/tag/t1")
    tag_map = {t["Key"]: t["Value"] for t in resp["TagList"]}
    assert tag_map["team"] == "platform"
    assert tag_map["env"] == "staging"

    ssm.remove_tags_from_resource(
        ResourceType="Parameter",
        ResourceId="/ssm2/tag/t1",
        TagKeys=["team"],
    )
    resp2 = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId="/ssm2/tag/t1")
    tag_map2 = {t["Key"]: t["Value"] for t in resp2["TagList"]}
    assert "team" not in tag_map2
    assert tag_map2["env"] == "staging"

def test_ssm_label_parameter_version(ssm):
    import uuid as _uuid

    pname = f"/intg/label/{_uuid.uuid4().hex[:8]}"
    ssm.put_parameter(Name=pname, Value="v1", Type="String")
    ssm.put_parameter(Name=pname, Value="v2", Type="String", Overwrite=True)
    resp = ssm.label_parameter_version(Name=pname, ParameterVersion=1, Labels=["stable"])
    assert resp["ParameterVersion"] == 1
    assert resp["InvalidLabels"] == []

def test_ssm_add_remove_tags(ssm):
    import uuid as _uuid

    pname = f"/intg/tagged/{_uuid.uuid4().hex[:8]}"
    ssm.put_parameter(Name=pname, Value="hello", Type="String")
    ssm.add_tags_to_resource(
        ResourceType="Parameter",
        ResourceId=pname,
        Tags=[{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "backend"}],
    )
    tags = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=pname)
    tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
    assert tag_map.get("env") == "prod"
    assert tag_map.get("team") == "backend"
    ssm.remove_tags_from_resource(ResourceType="Parameter", ResourceId=pname, TagKeys=["team"])
    tags2 = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=pname)
    tag_map2 = {t["Key"]: t["Value"] for t in tags2["TagList"]}
    assert "team" not in tag_map2
    assert tag_map2.get("env") == "prod"

def test_ssm_put_parameter_with_tags_then_list(ssm):
    """PutParameter with Tags must be readable via ListTagsForResource (GH-249)."""
    import uuid as _uuid

    pname = f"/intg/put-tags/{_uuid.uuid4().hex[:8]}"
    ssm.put_parameter(
        Name=pname,
        Value="tagged-value",
        Type="String",
        Tags=[{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "backend"}],
    )
    tags = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=pname)
    tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
    assert tag_map.get("env") == "prod"
    assert tag_map.get("team") == "backend"


def test_ssm_put_parameter_tags_work_with_add_and_remove(ssm):
    """Tags set via PutParameter must be compatible with AddTags/RemoveTags (GH-249)."""
    import uuid as _uuid

    pname = f"/intg/put-tags-compat/{_uuid.uuid4().hex[:8]}"
    ssm.put_parameter(
        Name=pname,
        Value="v1",
        Type="String",
        Tags=[{"Key": "env", "Value": "dev"}],
    )
    # AddTagsToResource on top of PutParameter tags
    ssm.add_tags_to_resource(
        ResourceType="Parameter",
        ResourceId=pname,
        Tags=[{"Key": "team", "Value": "platform"}],
    )
    tags = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=pname)
    tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
    assert tag_map.get("env") == "dev"
    assert tag_map.get("team") == "platform"

    # RemoveTagsFromResource on PutParameter-created tag
    ssm.remove_tags_from_resource(
        ResourceType="Parameter", ResourceId=pname, TagKeys=["env"]
    )
    tags2 = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=pname)
    tag_map2 = {t["Key"]: t["Value"] for t in tags2["TagList"]}
    assert "env" not in tag_map2
    assert tag_map2.get("team") == "platform"


def test_ssm_get_parameter_history(ssm):
    """GetParameterHistory returns all versions of a parameter."""
    ssm.put_parameter(Name="/qa/ssm/hist", Value="v1", Type="String")
    ssm.put_parameter(Name="/qa/ssm/hist", Value="v2", Type="String", Overwrite=True)
    ssm.put_parameter(Name="/qa/ssm/hist", Value="v3", Type="String", Overwrite=True)
    history = ssm.get_parameter_history(Name="/qa/ssm/hist")["Parameters"]
    assert len(history) == 3
    values = [h["Value"] for h in history]
    assert "v1" in values and "v2" in values and "v3" in values

def test_ssm_describe_parameters_filter(ssm):
    """DescribeParameters with ParameterFilters filters by path prefix."""
    ssm.put_parameter(Name="/qa/ssm/filter/a", Value="1", Type="String")
    ssm.put_parameter(Name="/qa/ssm/filter/b", Value="2", Type="String")
    ssm.put_parameter(Name="/qa/ssm/other/c", Value="3", Type="String")
    resp = ssm.describe_parameters(ParameterFilters=[{"Key": "Path", "Values": ["/qa/ssm/filter"]}])
    names = [p["Name"] for p in resp["Parameters"]]
    assert "/qa/ssm/filter/a" in names
    assert "/qa/ssm/filter/b" in names
    assert "/qa/ssm/other/c" not in names

def test_ssm_secure_string_not_decrypted_by_default(ssm):
    """SecureString value is not returned in plaintext without WithDecryption=True."""
    ssm.put_parameter(Name="/qa/ssm/secure", Value="mysecret", Type="SecureString")
    resp = ssm.get_parameter(Name="/qa/ssm/secure", WithDecryption=False)
    assert resp["Parameter"]["Value"] != "mysecret"
    resp2 = ssm.get_parameter(Name="/qa/ssm/secure", WithDecryption=True)
    assert resp2["Parameter"]["Value"] == "mysecret"


def test_ssm_get_parameters_by_path_root_non_recursive(ssm):
    """GetParametersByPath with Path=/ and Recursive=False should only return top-level params."""
    ssm.put_parameter(Name="/toplevel", Value="top", Type="String", Overwrite=True)
    ssm.put_parameter(Name="/nested/deep", Value="deep", Type="String", Overwrite=True)

    resp = ssm.get_parameters_by_path(Path="/", Recursive=False)
    names = [p["Name"] for p in resp["Parameters"]]
    assert "/toplevel" in names
    assert "/nested/deep" not in names


def test_ssm_restore_legacy_parameter_history_uses_parameter_arn_region():
    from ministack.core.responses import AccountScopedDict, set_request_account_id, set_request_region
    from ministack.services import ssm as ssm_service

    account_id = "000000000000"
    name = f"/legacy/history/{_uuid_mod.uuid4().hex[:8]}"

    set_request_account_id(account_id)
    set_request_region("us-east-1")
    ssm_service.reset()
    parameters = AccountScopedDict()
    parameters[name] = {
        "Name": name,
        "Type": "String",
        "Value": "v2",
        "Version": 2,
        "ARN": f"arn:aws:ssm:us-west-2:{account_id}:parameter/{name.lstrip('/')}",
    }
    parameter_history = AccountScopedDict()
    parameter_history[name] = [
        {"Name": name, "Type": "String", "Value": "v1", "Version": 1},
        {"Name": name, "Type": "String", "Value": "v2", "Version": 2},
    ]

    try:
        ssm_service.restore_state({
            "parameters": parameters,
            "parameter_history": parameter_history,
        })

        assert len(ssm_service._parameter_history.get_scoped(account_id, "us-west-2", name)) == 2
        assert ssm_service._parameter_history.get_scoped(account_id, "us-east-1", name) is None
    finally:
        ssm_service.reset()


def test_ssm_restore_legacy_history_prefers_exact_parameter_name_region():
    from ministack.core.responses import AccountScopedDict, set_request_account_id, set_request_region
    from ministack.services import ssm as ssm_service

    account_id = "000000000000"
    suffix = _uuid_mod.uuid4().hex[:8]
    bare_name = f"legacy-history-exact-{suffix}"
    path_name = f"/{bare_name}"

    set_request_account_id(account_id)
    set_request_region("us-east-1")
    ssm_service.reset()
    parameters = AccountScopedDict()
    parameters[bare_name] = {
        "Name": bare_name,
        "Type": "String",
        "Value": "bare",
        "Version": 1,
        "ARN": f"arn:aws:ssm:us-west-2:{account_id}:parameter{bare_name}",
    }
    parameters[path_name] = {
        "Name": path_name,
        "Type": "String",
        "Value": "path",
        "Version": 1,
        "ARN": f"arn:aws:ssm:us-east-1:{account_id}:parameter/{bare_name}",
    }
    parameter_history = AccountScopedDict()
    parameter_history[bare_name] = [
        {"Name": bare_name, "Type": "String", "Value": "bare", "Version": 1},
    ]

    try:
        ssm_service.restore_state({
            "parameters": parameters,
            "parameter_history": parameter_history,
        })

        assert len(ssm_service._parameter_history.get_scoped(account_id, "us-west-2", bare_name)) == 1
        assert ssm_service._parameter_history.get_scoped(account_id, "us-east-1", bare_name) is None
    finally:
        ssm_service.reset()


def test_ssm_restore_legacy_bare_name_tags_uses_stored_parameter_arn():
    from ministack.core.responses import AccountScopedDict, set_request_account_id, set_request_region
    from ministack.services import ssm as ssm_service

    account_id = "000000000000"
    name = f"legacy-tag-{_uuid_mod.uuid4().hex[:8]}"
    legacy_arn = f"arn:aws:ssm:us-west-2:{account_id}:parameter{name}"

    set_request_account_id(account_id)
    set_request_region("us-east-1")
    ssm_service.reset()
    parameters = AccountScopedDict()
    parameters[name] = {
        "Name": name,
        "Type": "String",
        "Value": "legacy",
        "Version": 1,
        "ARN": legacy_arn,
    }
    tags = AccountScopedDict()
    tags[legacy_arn] = {"env": "legacy"}

    try:
        ssm_service.restore_state({
            "parameters": parameters,
            "tags": tags,
        })
        set_request_region("us-west-2")
        assert ssm_service.resolve_parameter_value(legacy_arn) == "legacy"
        status, _headers, body = ssm_service._list_tags_for_resource({
            "ResourceType": "Parameter",
            "ResourceId": name,
        })
        assert status == 200
        assert json.loads(body)["TagList"] == [{"Key": "env", "Value": "legacy"}]

        status, _headers, _body = ssm_service._put_parameter({
            "Name": name,
            "Type": "String",
            "Value": "updated",
            "Overwrite": True,
        })
        assert status == 200
        assert ssm_service._parameters[name]["ARN"] == legacy_arn
        status, _headers, body = ssm_service._list_tags_for_resource({
            "ResourceType": "Parameter",
            "ResourceId": name,
        })
        assert status == 200
        assert json.loads(body)["TagList"] == [{"Key": "env", "Value": "legacy"}]
    finally:
        ssm_service.reset()


def test_ssm_arn_lookup_prefers_exact_stored_arn_match():
    from ministack.core.responses import AccountScopedDict, set_request_account_id, set_request_region
    from ministack.services import ssm as ssm_service

    account_id = "000000000000"
    suffix = _uuid_mod.uuid4().hex[:8]
    bare_name = f"legacy-exact-{suffix}"
    path_name = f"/{bare_name}"
    bare_arn = f"arn:aws:ssm:us-west-2:{account_id}:parameter{bare_name}"
    path_arn = f"arn:aws:ssm:us-west-2:{account_id}:parameter/{bare_name}"

    set_request_account_id(account_id)
    set_request_region("us-east-1")
    ssm_service.reset()
    parameters = AccountScopedDict()
    parameters[bare_name] = {
        "Name": bare_name,
        "Type": "String",
        "Value": "bare",
        "Version": 1,
        "ARN": bare_arn,
    }
    parameters[path_name] = {
        "Name": path_name,
        "Type": "String",
        "Value": "path",
        "Version": 1,
        "ARN": path_arn,
    }

    try:
        ssm_service.restore_state({"parameters": parameters})
        set_request_region("us-west-2")
        assert ssm_service.resolve_parameter_value(bare_arn) == "bare"
        assert ssm_service.resolve_parameter_value(path_arn) == "path"
    finally:
        ssm_service.reset()


def test_ssm_exact_legacy_slash_twin_can_be_overwritten():
    from ministack.core.responses import AccountScopedDict, set_request_account_id, set_request_region
    from ministack.services import ssm as ssm_service

    account_id = "000000000000"
    suffix = _uuid_mod.uuid4().hex[:8]
    bare_name = f"legacy-overwrite-{suffix}"
    path_name = f"/{bare_name}"

    set_request_account_id(account_id)
    set_request_region("us-east-1")
    ssm_service.reset()
    parameters = AccountScopedDict()
    parameters[bare_name] = {
        "Name": bare_name,
        "Type": "String",
        "Value": "bare",
        "Version": 1,
        "ARN": f"arn:aws:ssm:us-west-2:{account_id}:parameter{bare_name}",
    }
    parameters[path_name] = {
        "Name": path_name,
        "Type": "String",
        "Value": "path",
        "Version": 1,
        "ARN": f"arn:aws:ssm:us-west-2:{account_id}:parameter/{bare_name}",
    }

    try:
        ssm_service.restore_state({"parameters": parameters})
        set_request_region("us-west-2")
        status, _headers, _body = ssm_service._put_parameter({
            "Name": bare_name,
            "Type": "String",
            "Value": "updated-bare",
            "Overwrite": True,
        })
        assert status == 200
        assert ssm_service._parameters[bare_name]["Value"] == "updated-bare"
        assert ssm_service._parameters[path_name]["Value"] == "path"
    finally:
        ssm_service.reset()


def test_ssm_legacy_no_slash_arn_does_not_fallback_to_path_parameter():
    from ministack.core.responses import AccountScopedDict, set_request_account_id, set_request_region
    from ministack.services import ssm as ssm_service

    account_id = "000000000000"
    suffix = _uuid_mod.uuid4().hex[:8]
    path_name = f"/legacy-stale-{suffix}"
    path_arn = f"arn:aws:ssm:us-west-2:{account_id}:parameter/{path_name.lstrip('/')}"
    stale_legacy_arn = f"arn:aws:ssm:us-west-2:{account_id}:parameter{path_name.lstrip('/')}"

    set_request_account_id(account_id)
    set_request_region("us-east-1")
    ssm_service.reset()
    parameters = AccountScopedDict()
    parameters[path_name] = {
        "Name": path_name,
        "Type": "String",
        "Value": "path",
        "Version": 1,
        "ARN": path_arn,
    }

    try:
        ssm_service.restore_state({"parameters": parameters})
        set_request_region("us-west-2")
        assert ssm_service.resolve_parameter_value(path_arn) == "path"
        assert ssm_service.resolve_parameter_value(stale_legacy_arn) is None
    finally:
        ssm_service.reset()


def test_ssm_malformed_or_foreign_partition_arn_does_not_fallback_to_name():
    from ministack.core.responses import AccountScopedDict, set_request_account_id, set_request_region
    from ministack.services import ssm as ssm_service

    account_id = "000000000000"
    name = f"/invalid-arn/{_uuid_mod.uuid4().hex[:8]}"
    canonical_arn = f"arn:aws:ssm:us-east-1:{account_id}:parameter/{name.lstrip('/')}"
    missing_region_arn = f"arn:aws:ssm::{account_id}:parameter/{name.lstrip('/')}"
    foreign_partition_arn = f"arn:aws-cn:ssm:us-east-1:{account_id}:parameter/{name.lstrip('/')}"

    set_request_account_id(account_id)
    set_request_region("us-east-1")
    ssm_service.reset()
    parameters = AccountScopedDict()
    parameters[name] = {
        "Name": name,
        "Type": "String",
        "Value": "value",
        "Version": 1,
        "ARN": canonical_arn,
    }

    try:
        ssm_service.restore_state({"parameters": parameters})
        assert ssm_service.resolve_parameter_value(canonical_arn) == "value"
        assert ssm_service.resolve_parameter_value(missing_region_arn) is None
        assert ssm_service.resolve_parameter_value(foreign_partition_arn) is None
    finally:
        ssm_service.reset()


def test_ssm_restore_legacy_add_tags_key_for_bare_name_parameter():
    from ministack.core.responses import AccountScopedDict, set_request_account_id, set_request_region
    from ministack.services import ssm as ssm_service

    account_id = "000000000000"
    name = f"legacy-add-tags-{_uuid_mod.uuid4().hex[:8]}"
    legacy_arn = f"arn:aws:ssm:us-west-2:{account_id}:parameter{name}"
    legacy_add_tags_arn = f"arn:aws:ssm:us-west-2:{account_id}:parameter/{name}"

    set_request_account_id(account_id)
    set_request_region("us-east-1")
    ssm_service.reset()
    parameters = AccountScopedDict()
    parameters[name] = {
        "Name": name,
        "Type": "String",
        "Value": "legacy",
        "Version": 1,
        "ARN": legacy_arn,
    }
    tags = AccountScopedDict()
    tags[legacy_add_tags_arn] = {"env": "legacy-add-tags"}

    try:
        ssm_service.restore_state({
            "parameters": parameters,
            "tags": tags,
        })
        set_request_region("us-west-2")
        status, _headers, body = ssm_service._list_tags_for_resource({
            "ResourceType": "Parameter",
            "ResourceId": name,
        })
        assert status == 200
        assert json.loads(body)["TagList"] == [{"Key": "env", "Value": "legacy-add-tags"}]

        status, _headers, body = ssm_service._list_tags_for_resource({
            "ResourceType": "Parameter",
            "ResourceId": legacy_add_tags_arn,
        })
        assert status == 200
        assert json.loads(body)["TagList"] == [{"Key": "env", "Value": "legacy-add-tags"}]

        status, _headers, _body = ssm_service._delete_parameter({"Name": name})
        assert status == 200
        assert ssm_service._tags.get_scoped(account_id, "us-west-2", legacy_arn) is None
        assert ssm_service._tags.get_scoped(account_id, "us-west-2", legacy_add_tags_arn) is None
    finally:
        ssm_service.reset()


def test_cloudformation_ssm_parameter_uses_canonical_arn_builder():
    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import ssm as ssm_service
    from ministack.services.cloudformation import provisioners

    account_id = "000000000000"
    name = f"cfn-param-{_uuid_mod.uuid4().hex[:8]}"

    set_request_account_id(account_id)
    set_request_region("us-west-2")
    ssm_service.reset()
    try:
        provisioners._ssm_create("Param", {"Name": name, "Value": "value"}, "stack")
        assert ssm_service._parameters[name]["ARN"] == (
            f"arn:aws:ssm:us-west-2:{account_id}:parameter/{name}"
        )
    finally:
        ssm_service.reset()


def test_ssm_parameters_are_region_scoped_by_name(ssm):
    west = _regional_ssm("us-west-2")
    name = f"/mr/ssm/{_uuid_mod.uuid4().hex[:8]}"

    ssm.put_parameter(Name=name, Value="east", Type="String")
    west.put_parameter(Name=name, Value="west", Type="String")

    east_param = ssm.get_parameter(Name=name)["Parameter"]
    west_param = west.get_parameter(Name=name)["Parameter"]

    assert east_param["Value"] == "east"
    assert west_param["Value"] == "west"
    assert ":us-east-1:" in east_param["ARN"]
    assert ":us-west-2:" in west_param["ARN"]

    ssm.delete_parameter(Name=name)
    with pytest.raises(ClientError) as exc:
        ssm.get_parameter(Name=name)
    assert exc.value.response["Error"]["Code"] == "ParameterNotFound"
    assert west.get_parameter(Name=name)["Parameter"]["Value"] == "west"


def test_ssm_delete_rejects_parameter_arn(ssm):
    name = f"/mr/ssm/delete-arn/{_uuid_mod.uuid4().hex[:8]}"
    ssm.put_parameter(Name=name, Value="value", Type="String")
    arn = ssm.get_parameter(Name=name)["Parameter"]["ARN"]

    with pytest.raises(ClientError) as exc:
        ssm.delete_parameter(Name=arn)
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert ssm.get_parameter(Name=name)["Parameter"]["Value"] == "value"

    resp = ssm.delete_parameters(Names=[arn])
    assert arn in resp["InvalidParameters"]
    assert ssm.get_parameter(Name=name)["Parameter"]["Value"] == "value"

    ssm.delete_parameter(Name=name)


def test_ssm_put_rejects_slash_variant_duplicate(ssm):
    name = f"mr-ssm-duplicate-{_uuid_mod.uuid4().hex[:8]}"
    ssm.put_parameter(Name=name, Value="bare", Type="String")

    with pytest.raises(ClientError) as exc:
        ssm.put_parameter(Name=f"/{name}", Value="path", Type="String")
    assert exc.value.response["Error"]["Code"] == "ParameterAlreadyExists"

    with pytest.raises(ClientError) as exc:
        ssm.put_parameter(Name=f"/{name}", Value="path", Type="String", Overwrite=True)
    assert exc.value.response["Error"]["Code"] == "ParameterAlreadyExists"

    assert ssm.get_parameter(Name=name)["Parameter"]["Value"] == "bare"
    ssm.delete_parameter(Name=name)


def test_ssm_parameter_arn_lookup_is_request_region_scoped(ssm):
    west = _regional_ssm("us-west-2")
    name = f"/mr/ssm/arn/{_uuid_mod.uuid4().hex[:8]}"

    ssm.put_parameter(Name=name, Value="east", Type="String")
    west.put_parameter(Name=name, Value="west", Type="String")

    east_arn = ssm.get_parameter(Name=name)["Parameter"]["ARN"]
    west_arn = west.get_parameter(Name=name)["Parameter"]["ARN"]

    assert ssm.get_parameter(Name=east_arn)["Parameter"]["Value"] == "east"
    with pytest.raises(ClientError) as exc:
        ssm.get_parameter(Name=west_arn)
    assert exc.value.response["Error"]["Code"] == "ParameterNotFound"


def test_ssm_tags_accept_current_region_parameter_arn(ssm):
    west = _regional_ssm("us-west-2")
    name = f"/mr/ssm/tag/{_uuid_mod.uuid4().hex[:8]}"

    ssm.put_parameter(Name=name, Value="east", Type="String")
    west.put_parameter(Name=name, Value="west", Type="String")

    east_arn = ssm.get_parameter(Name=name)["Parameter"]["ARN"]
    ssm.add_tags_to_resource(
        ResourceType="Parameter",
        ResourceId=east_arn,
        Tags=[{"Key": "scope", "Value": "east"}],
    )

    east_tags = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=east_arn)["TagList"]
    west_tags = west.list_tags_for_resource(ResourceType="Parameter", ResourceId=name)["TagList"]

    assert {tag["Key"]: tag["Value"] for tag in east_tags} == {"scope": "east"}
    assert west_tags == []


def test_ssm_resolve_parameter_value_uses_arn_region_without_tail_fallback():
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import ssm as ssm_service

    original_account = get_account_id()
    original_region = get_region()
    name = f"/mr/ssm/resolve/{_uuid_mod.uuid4().hex[:8]}"

    try:
        ssm_service.reset()
        set_request_account_id("000000000000")
        set_request_region("us-east-1")
        ssm_service._put_parameter({"Name": name, "Value": "east", "Type": "String"})

        west_arn = f"arn:aws:ssm:us-west-2:000000000000:parameter/{name.lstrip('/')}"
        assert ssm_service.resolve_parameter_value(west_arn) is None

        set_request_region("us-west-2")
        ssm_service._put_parameter({"Name": name, "Value": "west", "Type": "String"})

        set_request_region("us-east-1")
        assert ssm_service.resolve_parameter_value(name) == "east"
        assert ssm_service.resolve_parameter_value(west_arn) == "west"
        assert ssm_service.resolve_parameter_value(
            west_arn.replace(":ssm:", ":secretsmanager:", 1)
        ) is None
        assert ssm_service.resolve_parameter_value(
            west_arn.replace(":000000000000:", ":111111111111:", 1)
        ) is None
    finally:
        ssm_service.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)
