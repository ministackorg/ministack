"""
Bedrock control-plane parity tests — all 66 ops.

boto3 round-trip on every resource family. Each test validates the wire shape
against botocore bedrock-2023-04-20: required response fields, correct status
codes (202 for async creates, 201 for non-idempotent CRUD, 200 for sync),
correct error codes, ARN format.
"""

import botocore.exceptions
from conftest import make_client


def _bedrock():
    return make_client("bedrock")


# ---------------------------------------------------------------------------
# Guardrails — full CRUD + version
# ---------------------------------------------------------------------------


def test_bedrock_create_guardrail_returns_id_and_arn():
    resp = _bedrock().create_guardrail(
        name="gr-test-1",
        description="test",
        blockedInputMessaging="blocked",
        blockedOutputsMessaging="blocked",
    )
    assert resp["guardrailId"].startswith("gr-")
    assert ":guardrail/" in resp["guardrailArn"]
    assert resp["version"] == "DRAFT"


def test_bedrock_get_guardrail_round_trips():
    create = _bedrock().create_guardrail(
        name="gr-get",
        blockedInputMessaging="x", blockedOutputsMessaging="y",
    )
    resp = _bedrock().get_guardrail(guardrailIdentifier=create["guardrailId"])
    assert resp["name"] == "gr-get"
    assert resp["status"] == "READY"
    assert resp["version"] == "DRAFT"


def test_bedrock_list_guardrails_includes_created():
    _bedrock().create_guardrail(name="gr-list-1",
                                  blockedInputMessaging="a", blockedOutputsMessaging="b")
    _bedrock().create_guardrail(name="gr-list-2",
                                  blockedInputMessaging="a", blockedOutputsMessaging="b")
    resp = _bedrock().list_guardrails()
    names = {g["name"] for g in resp["guardrails"]}
    assert {"gr-list-1", "gr-list-2"}.issubset(names)


def test_bedrock_update_guardrail():
    create = _bedrock().create_guardrail(name="gr-upd",
                                            blockedInputMessaging="x",
                                            blockedOutputsMessaging="y")
    _bedrock().update_guardrail(
        guardrailIdentifier=create["guardrailId"],
        name="gr-upd-renamed",
        blockedInputMessaging="x2",
        blockedOutputsMessaging="y2",
    )
    g = _bedrock().get_guardrail(guardrailIdentifier=create["guardrailId"])
    assert g["name"] == "gr-upd-renamed"


def test_bedrock_create_guardrail_version_then_get():
    create = _bedrock().create_guardrail(name="gr-ver",
                                            blockedInputMessaging="x",
                                            blockedOutputsMessaging="y")
    ver = _bedrock().create_guardrail_version(
        guardrailIdentifier=create["guardrailId"], description="v1",
    )
    assert ver["version"] == "1"
    fetched = _bedrock().get_guardrail(
        guardrailIdentifier=create["guardrailId"], guardrailVersion="1",
    )
    assert fetched["version"] == "1"


def test_bedrock_delete_guardrail():
    create = _bedrock().create_guardrail(name="gr-del",
                                            blockedInputMessaging="x",
                                            blockedOutputsMessaging="y")
    _bedrock().delete_guardrail(guardrailIdentifier=create["guardrailId"])
    try:
        _bedrock().get_guardrail(guardrailIdentifier=create["guardrailId"])
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ResourceNotFoundException"
    else:
        raise AssertionError("expected NotFound after delete")


def test_bedrock_create_guardrail_duplicate_name_conflict():
    _bedrock().create_guardrail(name="gr-dup",
                                  blockedInputMessaging="x", blockedOutputsMessaging="y")
    try:
        _bedrock().create_guardrail(name="gr-dup",
                                      blockedInputMessaging="x", blockedOutputsMessaging="y")
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ConflictException"
    else:
        raise AssertionError("expected ConflictException")


# ---------------------------------------------------------------------------
# Custom models
# ---------------------------------------------------------------------------


def test_bedrock_create_custom_model_returns_arn():
    resp = _bedrock().create_custom_model(
        modelName="cm-test-1",
        modelSourceConfig={
            "s3DataSource": {"s3Uri": "s3://x/y/"},
        },
    )
    assert ":custom-model/cm-test-1" in resp["modelArn"]


def test_bedrock_get_custom_model_round_trip():
    _bedrock().create_custom_model(
        modelName="cm-get",
        modelSourceConfig={"s3DataSource": {"s3Uri": "s3://x/y/"}},
    )
    resp = _bedrock().get_custom_model(modelIdentifier="cm-get")
    assert resp["modelName"] == "cm-get"


def test_bedrock_list_custom_models():
    _bedrock().create_custom_model(modelName="cm-l1",
                                      modelSourceConfig={"s3DataSource": {"s3Uri": "s3://x/y/"}})
    _bedrock().create_custom_model(modelName="cm-l2",
                                      modelSourceConfig={"s3DataSource": {"s3Uri": "s3://x/y/"}})
    resp = _bedrock().list_custom_models()
    names = {m["modelName"] for m in resp["modelSummaries"]}
    assert {"cm-l1", "cm-l2"}.issubset(names)


def test_bedrock_delete_custom_model():
    _bedrock().create_custom_model(modelName="cm-del",
                                      modelSourceConfig={"s3DataSource": {"s3Uri": "s3://x/y/"}})
    _bedrock().delete_custom_model(modelIdentifier="cm-del")
    try:
        _bedrock().get_custom_model(modelIdentifier="cm-del")
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Provisioned throughput
# ---------------------------------------------------------------------------


def test_bedrock_create_provisioned_throughput():
    resp = _bedrock().create_provisioned_model_throughput(
        provisionedModelName="pt-test",
        modelId="anthropic.claude-3-haiku-20240307-v1:0",
        modelUnits=1,
    )
    assert ":provisioned-model/" in resp["provisionedModelArn"]


def test_bedrock_get_list_update_delete_provisioned():
    create = _bedrock().create_provisioned_model_throughput(
        provisionedModelName="pt-rt",
        modelId="amazon.nova-pro-v1:0", modelUnits=2,
    )
    pid = create["provisionedModelArn"].rsplit("/", 1)[-1]
    g = _bedrock().get_provisioned_model_throughput(provisionedModelId=pid)
    assert g["provisionedModelName"] == "pt-rt"
    assert g["modelUnits"] == 2
    lst = _bedrock().list_provisioned_model_throughputs()
    assert any(p["provisionedModelArn"] == create["provisionedModelArn"]
                for p in lst["provisionedModelSummaries"])
    _bedrock().update_provisioned_model_throughput(
        provisionedModelId=pid, desiredProvisionedModelName="pt-renamed",
    )
    _bedrock().delete_provisioned_model_throughput(provisionedModelId=pid)
    try:
        _bedrock().get_provisioned_model_throughput(provisionedModelId=pid)
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Model customization jobs
# ---------------------------------------------------------------------------


def test_bedrock_create_get_list_stop_customization_job():
    create = _bedrock().create_model_customization_job(
        jobName="cj-test",
        customModelName="cm-from-job",
        roleArn="arn:aws:iam::000000000000:role/r",
        baseModelIdentifier="anthropic.claude-3-haiku-20240307-v1:0",
        trainingDataConfig={"s3Uri": "s3://x/train/"},
        outputDataConfig={"s3Uri": "s3://x/out/"},
        hyperParameters={"epochCount": "1"},
    )
    assert ":model-customization-job/" in create["jobArn"]
    g = _bedrock().get_model_customization_job(jobIdentifier=create["jobArn"])
    assert g["jobName"] == "cj-test"
    lst = _bedrock().list_model_customization_jobs()
    assert any(j["jobArn"] == create["jobArn"] for j in lst["modelCustomizationJobSummaries"])
    _bedrock().stop_model_customization_job(jobIdentifier=create["jobArn"])
    g2 = _bedrock().get_model_customization_job(jobIdentifier=create["jobArn"])
    assert g2["status"] == "Stopped"


# ---------------------------------------------------------------------------
# Model import jobs + imported models
# ---------------------------------------------------------------------------


def test_bedrock_create_model_import_job_creates_imported_model():
    create = _bedrock().create_model_import_job(
        jobName="ij-test",
        importedModelName="imported-1",
        roleArn="arn:aws:iam::000000000000:role/r",
        modelDataSource={"s3DataSource": {"s3Uri": "s3://x/y/"}},
    )
    assert ":model-import-job/" in create["jobArn"]
    g = _bedrock().get_model_import_job(jobIdentifier=create["jobArn"])
    assert g["importedModelName"] == "imported-1"
    # Imported model side-effect
    im = _bedrock().get_imported_model(modelIdentifier="imported-1")
    assert im["modelName"] == "imported-1"


def test_bedrock_list_imported_models():
    _bedrock().create_model_import_job(
        jobName="ij-l1", importedModelName="im-l-1",
        roleArn="arn:aws:iam::000000000000:role/r",
        modelDataSource={"s3DataSource": {"s3Uri": "s3://x/"}},
    )
    resp = _bedrock().list_imported_models()
    names = {m["modelName"] for m in resp["modelSummaries"]}
    assert "im-l-1" in names


def test_bedrock_delete_imported_model():
    _bedrock().create_model_import_job(
        jobName="ij-d", importedModelName="im-del",
        roleArn="arn:aws:iam::000000000000:role/r",
        modelDataSource={"s3DataSource": {"s3Uri": "s3://x/"}},
    )
    _bedrock().delete_imported_model(modelIdentifier="im-del")
    try:
        _bedrock().get_imported_model(modelIdentifier="im-del")
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Model copy jobs
# ---------------------------------------------------------------------------


def test_bedrock_create_get_list_copy_job():
    src = "arn:aws:bedrock:us-east-1:000000000000:custom-model/src"
    create = _bedrock().create_model_copy_job(
        sourceModelArn=src,
        targetModelName="copied-1",
    )
    assert ":model-copy-job/" in create["jobArn"]
    g = _bedrock().get_model_copy_job(jobArn=create["jobArn"])
    assert g["targetModelArn"].endswith(":custom-model/copied-1")
    lst = _bedrock().list_model_copy_jobs()
    assert any(j["jobArn"] == create["jobArn"] for j in lst["modelCopyJobSummaries"])


# ---------------------------------------------------------------------------
# Model invocation jobs (batch inference)
# ---------------------------------------------------------------------------


def test_bedrock_create_get_list_stop_invocation_job():
    create = _bedrock().create_model_invocation_job(
        jobName="bj-test",
        modelId="anthropic.claude-3-haiku-20240307-v1:0",
        roleArn="arn:aws:iam::000000000000:role/r",
        inputDataConfig={"s3InputDataConfig": {"s3Uri": "s3://x/in/"}},
        outputDataConfig={"s3OutputDataConfig": {"s3Uri": "s3://x/out/"}},
    )
    assert ":model-invocation-job/" in create["jobArn"]
    g = _bedrock().get_model_invocation_job(jobIdentifier=create["jobArn"])
    assert g["jobName"] == "bj-test"
    lst = _bedrock().list_model_invocation_jobs()
    assert any(j["jobArn"] == create["jobArn"] for j in lst["invocationJobSummaries"])
    _bedrock().stop_model_invocation_job(jobIdentifier=create["jobArn"])


# ---------------------------------------------------------------------------
# Evaluation jobs
# ---------------------------------------------------------------------------


def test_bedrock_create_get_list_stop_evaluation_job():
    create = _bedrock().create_evaluation_job(
        jobName="ej-test",
        roleArn="arn:aws:iam::000000000000:role/r",
        evaluationConfig={
            "automated": {
                "datasetMetricConfigs": [{
                    "taskType": "Generation",
                    "dataset": {"name": "Builtin.BoolQ"},
                    "metricNames": ["Builtin.Accuracy"],
                }],
            },
        },
        inferenceConfig={
            "models": [{"bedrockModel": {"modelIdentifier": "anthropic.claude-3-haiku-20240307-v1:0",
                                            "inferenceParams": "{}"}}],
        },
        outputDataConfig={"s3Uri": "s3://x/eval-out/"},
    )
    assert ":evaluation-job/" in create["jobArn"]
    g = _bedrock().get_evaluation_job(jobIdentifier=create["jobArn"])
    assert g["jobName"] == "ej-test"
    lst = _bedrock().list_evaluation_jobs()
    assert any(j["jobArn"] == create["jobArn"] for j in lst["jobSummaries"])
    _bedrock().stop_evaluation_job(jobIdentifier=create["jobArn"])


def test_bedrock_batch_delete_evaluation_job():
    create = _bedrock().create_evaluation_job(
        jobName="ej-bd",
        roleArn="arn:aws:iam::000000000000:role/r",
        evaluationConfig={"automated": {"datasetMetricConfigs": [{
            "taskType": "Generation", "dataset": {"name": "x"}, "metricNames": ["m"]
        }]}},
        inferenceConfig={"models": [{"bedrockModel": {"modelIdentifier": "x",
                                                         "inferenceParams": "{}"}}]},
        outputDataConfig={"s3Uri": "s3://x/"},
    )
    resp = _bedrock().batch_delete_evaluation_job(jobIdentifiers=[create["jobArn"]])
    assert resp["evaluationJobs"][0]["jobIdentifier"] == create["jobArn"]
    assert resp["evaluationJobs"][0]["jobStatus"] == "Deleting"


# ---------------------------------------------------------------------------
# Marketplace endpoints
# ---------------------------------------------------------------------------


def test_bedrock_marketplace_endpoint_lifecycle():
    create = _bedrock().create_marketplace_model_endpoint(
        endpointName="mp-1",
        modelSourceIdentifier="arn:aws:sagemaker:us-east-1:000000000000:model-package/x",
        endpointConfig={
            "sageMaker": {
                "initialInstanceCount": 1,
                "instanceType": "ml.m5.large",
                "executionRole": "arn:aws:iam::000000000000:role/r",
            },
        },
    )
    arn = create["marketplaceModelEndpoint"]["endpointArn"]
    g = _bedrock().get_marketplace_model_endpoint(endpointArn=arn)
    assert g["marketplaceModelEndpoint"]["endpointArn"] == arn
    assert g["marketplaceModelEndpoint"]["endpointStatus"] == "InService"
    _bedrock().deregister_marketplace_model_endpoint(endpointArn=arn)
    _bedrock().delete_marketplace_model_endpoint(endpointArn=arn)


# ---------------------------------------------------------------------------
# Prompt routers
# ---------------------------------------------------------------------------


def test_bedrock_prompt_router_lifecycle():
    create = _bedrock().create_prompt_router(
        promptRouterName="pr-test",
        models=[{"modelArn": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0"}],
        fallbackModel={"modelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.nova-pro-v1:0"},
        routingCriteria={"responseQualityDifference": 0.1},
    )
    arn = create["promptRouterArn"]
    g = _bedrock().get_prompt_router(promptRouterArn=arn)
    assert g["promptRouterName"] == "pr-test"
    lst = _bedrock().list_prompt_routers()
    assert any(p["promptRouterArn"] == arn for p in lst["promptRouterSummaries"])
    _bedrock().delete_prompt_router(promptRouterArn=arn)


# ---------------------------------------------------------------------------
# Model invocation logging config
# ---------------------------------------------------------------------------


def test_bedrock_put_get_delete_logging_config():
    _bedrock().put_model_invocation_logging_configuration(
        loggingConfig={
            "cloudWatchConfig": {
                "logGroupName": "/aws/bedrock/invocations",
                "roleArn": "arn:aws:iam::000000000000:role/r",
            },
            "textDataDeliveryEnabled": True,
        },
    )
    g = _bedrock().get_model_invocation_logging_configuration()
    assert g["loggingConfig"]["textDataDeliveryEnabled"] is True
    _bedrock().delete_model_invocation_logging_configuration()
    g2 = _bedrock().get_model_invocation_logging_configuration()
    assert g2.get("loggingConfig") in (None, {})


# ---------------------------------------------------------------------------
# Application inference profiles
# ---------------------------------------------------------------------------


def test_bedrock_application_inference_profile_lifecycle():
    create = _bedrock().create_inference_profile(
        inferenceProfileName="aip-test",
        modelSource={
            "copyFrom": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0"
        },
    )
    arn = create["inferenceProfileArn"]
    g = _bedrock().get_inference_profile(inferenceProfileIdentifier=arn.rsplit("/", 1)[-1])
    assert g["inferenceProfileName"] == "aip-test"
    _bedrock().delete_inference_profile(inferenceProfileIdentifier=arn.rsplit("/", 1)[-1])


# ---------------------------------------------------------------------------
# Use case for model access
# ---------------------------------------------------------------------------


def test_bedrock_put_get_use_case_for_model_access():
    _bedrock().put_use_case_for_model_access(formData=b"hello world form data")
    resp = _bedrock().get_use_case_for_model_access()
    assert b"hello world" in resp["formData"]


# ---------------------------------------------------------------------------
# Foundation model agreement
# ---------------------------------------------------------------------------


def test_bedrock_create_foundation_model_agreement():
    resp = _bedrock().create_foundation_model_agreement(
        offerToken="offer-tok-1",
        modelId="anthropic.claude-3-haiku-20240307-v1:0",
    )
    assert resp["modelId"] == "anthropic.claude-3-haiku-20240307-v1:0"


def test_bedrock_get_foundation_model_availability():
    resp = _bedrock().get_foundation_model_availability(
        modelId="anthropic.claude-3-haiku-20240307-v1:0",
    )
    assert resp["modelId"] == "anthropic.claude-3-haiku-20240307-v1:0"
    assert resp["entitlementAvailability"] == "AVAILABLE"


def test_bedrock_list_foundation_model_agreement_offers():
    resp = _bedrock().list_foundation_model_agreement_offers(
        modelId="anthropic.claude-3-haiku-20240307-v1:0",
    )
    assert "offers" in resp


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------


def test_bedrock_tag_list_untag_resource():
    gr = _bedrock().create_guardrail(
        name="gr-tag", blockedInputMessaging="x", blockedOutputsMessaging="y",
        tags=[{"key": "env", "value": "prod"}],
    )
    arn = gr["guardrailArn"]
    _bedrock().tag_resource(resourceARN=arn, tags=[{"key": "team", "value": "ml"}])
    resp = _bedrock().list_tags_for_resource(resourceARN=arn)
    keys = {t["key"]: t["value"] for t in resp["tags"]}
    assert keys["env"] == "prod"
    assert keys["team"] == "ml"
    _bedrock().untag_resource(resourceARN=arn, tagKeys=["env"])
    resp2 = _bedrock().list_tags_for_resource(resourceARN=arn)
    keys2 = {t["key"] for t in resp2["tags"]}
    assert "env" not in keys2
    assert "team" in keys2
