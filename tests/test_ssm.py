import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

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
