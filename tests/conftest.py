"""
Pytest fixtures for MiniStack integration tests.
"""

import contextlib
import os
import socket
import urllib.request
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
ENDPOINT_HOST = urlparse(ENDPOINT).hostname
REGION = "us-east-1"

_default_kwargs = dict(
    endpoint_url=ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name=REGION,
)
# Hardcoded retry and pool settings to reduce transient connection flakes
_default_config_kwargs = dict(
    region_name=REGION,
    retries={"mode": "standard"},
    max_pool_connections=50,
)


@contextlib.contextmanager
def patch_endpoint_dns():
    """Make *.MINISTACK_ENDPOINT subdomains resolve to 127.0.0.1 for virtual-hosted S3 testing."""
    _real_getaddrinfo = socket.getaddrinfo

    def _patched(host, port, *args, **kwargs):
        if isinstance(host, str) and host.endswith(f".{ENDPOINT_HOST}"):
            host = ENDPOINT_HOST
        return _real_getaddrinfo(host, port, *args, **kwargs)

    socket.getaddrinfo = _patched
    yield
    socket.getaddrinfo = _real_getaddrinfo


def make_client(service, additional_config_kwargs=None):
    if additional_config_kwargs is None:
        additional_config_kwargs = {}
    return boto3.client(service, **_default_kwargs, config=Config(**_default_config_kwargs, **additional_config_kwargs))


_SERIAL_TESTS = {
    "tests/test_athena.py::test_athena_engine_mock_via_config",
    "tests/test_athena.py::test_athena_mixed_glue_and_s3_uri",
    "tests/test_ec2.py::test_ec2_create_default_vpc",
    "tests/test_eks.py::test_eks_cfn_cluster",
    "tests/test_eks.py::test_eks_create_describe_delete_cluster",
    "tests/test_eks.py::test_eks_restore_state_normalizes_endpoint_to_localhost",
    "tests/test_lambda.py::test_lambda_reset_terminates_workers",
    "tests/test_lambda.py::test_lambda_dynamodb_stream_esm_latest_processes_first_record",
    "tests/test_ministack.py::test_ministack_config_invalid_key_ignored",
    "tests/test_ses.py::test_ses_messages_endpoint_reset",
    "tests/test_ses.py::test_ses_messages_endpoint_account_filter",
    "tests/test_stepfunctions.py::test_sfn_mock_config_return",
    "tests/test_stepfunctions.py::test_sfn_mock_config_throw",
    "tests/test_stepfunctions.py::test_sfn_mock_config_throw_routes_to_catch",
    "tests/test_stepfunctions.py::test_sfn_mock_config_jsonata_assign_applied",
    "tests/test_stepfunctions.py::test_sfn_wait_scale_zero_does_not_timeout_lambda_tasks",
    "tests/test_stepfunctions.py::test_sfn_wait_scale_zero_skips_wait",
    "tests/test_rds.py::test_rds_lambda_network_connectivity",
    # Docker-executor Lambda timeout (skipped unless LAMBDA_EXECUTOR=docker):
    # cold-starts a container and asserts a wall-clock bound (< 9s for a
    # Timeout=3 function), which parallel container churn breaks.
    "tests/test_lambda.py::test_lambda_docker_timeout_returns_task_timed_out_promptly",
    "tests/test_elasticache.py::test_elasticache_lambda_network_connectivity",
    # API Gateway execute-api → Lambda invoke under tight urlopen / WS recv
    # timeouts. These pass cleanly when run serially but are sensitive to
    # xdist parallel load on shared CI runners (cold-start time bursts past
    # the client timeout). Pre-warming + the WS warm-pool key fix covered
    # the deterministic cases; these remaining ones tip over only under
    # sustained parallel pressure. Run them in the dedicated serial phase.
    "tests/test_apigatewayv2.py::test_apigwv2_path_based_execute_api_http",
    "tests/test_apigatewayv2.py::test_apigwv2_path_based_websocket",
    "tests/test_apigatewayv2.py::test_apigwv2_default_stage_serves_from_root",
    "tests/test_apigatewayv2.py::test_apigwv1_path_based_restapi_legacy_user_request",
    "tests/test_apigatewayv2.py::test_apigwv2_named_stage_still_requires_prefix",
    "tests/test_apigatewayv2.py::test_apigwv2_integration_wrapped_function_arn",
    # Non-proxy (custom) AWS integration tests: same execute-api → Lambda
    # cold-start shape as the ones above, one function created per test.
    "tests/test_apigatewayv1.py::test_apigwv1_execute_lambda_custom_returns_raw_output",
    "tests/test_apigatewayv1.py::test_apigwv1_execute_lambda_custom_function_error_is_502",
    "tests/test_apigatewayv1.py::test_apigwv1_execute_lambda_custom_without_integration_responses_is_200",
    "tests/test_apigatewayv1.py::test_apigwv1_execute_lambda_proxy_envelope_still_interpreted",
    # v1 Lambda-authorizer cache tests. Same cold-start-burst sensitivity as the
    # apigw tests above, doubled: every guarded request cold-starts TWO Lambdas
    # (the authorizer and the backend), and the invocation-count assertions that
    # prove a cache hit or miss leave no slack for a retry.
    "tests/test_apigatewayv1.py::test_apigwv1_authorizer_cache_is_scoped_per_method_arn",
    "tests/test_apigatewayv1.py::test_apigwv1_authorizer_cache_is_scoped_per_stage",
    "tests/test_apigatewayv1.py::test_apigwv1_authorizer_invalid_validation_expression_is_500",
    "tests/test_apigatewayv1.py::test_apigwv1_authorizer_without_principal_id_is_500_and_not_cached",
    "tests/test_apigatewayv1.py::test_apigwv1_authorizer_without_policy_document_is_500_and_not_cached",
    "tests/test_apigatewayv1.py::test_apigwv1_authorizer_unparsable_ttl_falls_back_to_the_default",
    # v2 REQUEST-authorizer cache tests. Same cold-start-burst sensitivity as
    # the apigw tests above, doubled: every guarded request cold-starts TWO
    # Lambdas (the authorizer and the integration), and the invocation-count
    # assertions that prove a cache hit or miss leave no slack for a retry.
    "tests/test_apigatewayv2.py::test_apigwv2_authorizer_cache_is_scoped_per_route_arn",
    "tests/test_apigatewayv2.py::test_apigwv2_authorizer_cache_is_scoped_per_stage",
    "tests/test_apigatewayv2.py::test_apigwv2_authorizer_simple_response_cache_covers_routes",
    "tests/test_apigatewayv2.py::test_apigwv2_authorizer_without_policy_document_is_500_and_not_cached",
    "tests/test_apigatewayv2.py::test_apigwv2_authorizer_simple_response_non_boolean_is_500",
    "tests/test_apigatewayv2.py::test_apigwv2_authorizer_unparsable_ttl_falls_back_to_the_default",
    "tests/test_apigatewayv2.py::test_apigwv2_authorizer_without_identity_source_does_not_cache",
    # {proxy+} fallthrough tests: each request cold-starts a Lambda (the bypass
    # test wires three), with the same cold-start-under-xdist flakiness as the
    # apigw tests above.
    "tests/test_apigatewayv1.py::test_apigwv1_methodless_resource_falls_through_to_proxy",
    "tests/test_apigatewayv1.py::test_apigwv1_proxy_fallthrough_cannot_bypass_a_guarded_sibling",
    # AppSync Lambda-resolver event-shape tests cold-start Lambdas under a 10s
    # urlopen timeout (Test 6 spawns two functions). Same cold-start-under-xdist
    # flakiness as the apigw Lambda tests above — run them in the serial phase.
    "tests/test_appsync.py::test_appsync_lambda_event_field_name",
    "tests/test_appsync.py::test_appsync_lambda_event_arguments",
    "tests/test_appsync.py::test_appsync_lambda_event_api_key_header",
    "tests/test_appsync.py::test_appsync_lambda_event_custom_headers_forwarded",
    "tests/test_appsync.py::test_appsync_lambda_event_no_identity_in_api_key_mode",
    "tests/test_appsync.py::test_appsync_lambda_event_identity_from_authorizer",
    "tests/test_appsync.py::test_appsync_lambda_not_found_no_crash",
    "tests/test_appsync.py::test_appsync_lambda_returns_errors",
    "tests/test_appsync.py::test_appsync_lambda_event_source_empty_for_root",
    "tests/test_appsync.py::test_appsync_lambda_event_variables_substituted",
    "tests/test_appsync.py::test_appsync_lambda_unhandled_exception_becomes_error",
    "tests/test_appsync.py::test_appsync_lambda_authorizer_rejection_returns_unauthorized",
    "tests/test_appsync.py::test_appsync_lambda_authorizer_wrong_region_arn_does_not_fallback",
    "tests/test_appsync.py::test_appsync_lambda_missing_authorizer_returns_unauthorized",
    "tests/test_appsync.py::test_appsync_lambda_failing_authorizer_returns_unauthorized",
    # AppSync Events service mutations; shared state racing under xdist.
    "tests/test_appsync_events.py::test_publish_with_appsync_sigv4_scope_on_events_vhost",
    # Credential report reflects all users in the account; run serially to avoid
    # parallel-test interference on the account-global CSV snapshot.
    "tests/test_iam.py::test_iam_credential_report_mfa_and_password",
    "tests/test_iam.py::test_iam_credential_report_header",
    # Account-global mutations (password policy, alias); must run serially.
    "tests/test_iam.py::test_iam_password_policy_absent_then_set",
    "tests/test_iam.py::test_iam_account_alias_crud",
    # Recursive-loop detection. The two chain tests cold-start 16+ Lambdas
    # each and poll CloudWatch Logs until the chain stops changing; the other
    # two assert on wall clock (a dropped invocation must come back long
    # before the handler's timeout could have elapsed). Both shapes tip over
    # under xdist load, so run them in the serial phase.
    "tests/test_lambda.py::test_lambda_recursive_loop_terminates_self_invoking_chain",
    "tests/test_lambda.py::test_lambda_recursive_loop_allow_lets_the_chain_through",
    "tests/test_lambda.py::test_lambda_recursive_loop_drop_is_recursive_invocation_exception",
    "tests/test_lambda.py::test_lambda_nested_invoke_below_limit_is_unaffected",
    # DeleteRegistrationCode discards the one code the account/region holds, and
    # the test asserts the next GetRegistrationCode mints a different one — a
    # parallel worker calling GetRegistrationCode either side of the delete sees
    # the code change under it. Same account-global mutation class as above.
    "tests/test_iot.py::test_iot_registration_code_stable_until_deleted",
    # IoT topic-rule Lambda-action tests: each creates one Lambda per test,
    # cold-starts its container, and polls an SQS sink under a bounded timeout.
    # Same cold-start-burst-under-xdist shape as the apigw Lambda tests above —
    # they pass serially (~3.5s each) but the tail ones lose the cold start
    # against _poll_sink's 12s window on the shared CI runner. Surfaced when the
    # device-shadow work lengthened the test_iot_data.py shard; the tests
    # themselves are correct, so run them in the serial phase.
    "tests/test_iot_data.py::test_iot_topic_rule_routes_publish_to_lambda",
    "tests/test_iot_data.py::test_iot_basic_ingest_routes_to_lambda",
    "tests/test_iot_data.py::test_iot_disabled_rule_does_not_fire",
    "tests/test_iot_data.py::test_iot_rule_encode_base64_projection_basic_ingest",
    "tests/test_iot_data.py::test_iot_rule_encode_base64_projection_topic_filter",
    "tests/test_iot_data.py::test_iot_rule_attribute_projection",
    "tests/test_iot_data.py::test_iot_rule_where_clause_gates_dispatch",
    "tests/test_iot_data.py::test_iot_rule_where_topic_function_under_basic_ingest",
    "tests/test_iot_data.py::test_iot_rule_where_or_clause_dispatches_either_branch",
    "tests/test_iot_data.py::test_iot_jitr_registration_event_drives_a_topic_rule",
    # IoT Jobs data-plane routing test: a raw urllib GET with the advertised
    # `{prefix}.jobs.iot.{region}` Host and a tight 5s timeout, so cross-file
    # xdist pressure on the shared event loop makes it time out at random.
    "tests/test_iot_jobs.py::test_iot_jobs_advertised_endpoint_host_reaches_the_data_plane",
    # ECS service task-spawn: with a Docker daemon present (CI has one) a task
    # whose container fails to start/exits under parallel container churn is set
    # STOPPED, so list_tasks (RUNNING-only) sees fewer than desiredCount. Passes
    # serially; run it in the serial phase.
    "tests/test_ecs.py::test_ecs_service_spawns_tasks",
    # WS/MQTT-broker tests (MQTT-over-WebSocket connect/publish/subscribe,
    # device shadows over MQTT, fleet-index connectivity). They drive the
    # single-event-loop broker over real WebSocket connections with tight
    # ready-event / delivery-window timing, so cross-file xdist pressure on the
    # shared server slips a handshake or a delivery and one of them fails at
    # random. Correct serially; run them in the dedicated serial phase.
    "tests/test_iot_data.py::test_iot_lambda_publishes_browser_subscribes_e2e",
    "tests/test_iot_data.py::test_iot_ws_publish_isolated_between_regions",
    "tests/test_iot_data.py::test_iot_ws_credential_region_wildcards_cannot_bypass_isolation",
    "tests/test_iot_data.py::test_iot_ws_topic_isolation_between_accounts",
    "tests/test_iot_data.py::test_iot_ws_same_account_publish_delivers",
    "tests/test_iot_data.py::test_iot_rule_republish_where_gated_ws_subscriber",
    "tests/test_iot_data.py::test_search_index_finds_connected_thing_and_loses_it_on_disconnect",
    "tests/test_iot_data.py::test_search_index_connectivity_timestamp_moves_on_disconnect",
    "tests/test_iot_data.py::test_search_index_connectivity_is_tied_to_the_client_id",
    "tests/test_iot_data.py::test_search_index_connectivity_reports_a_dropped_transport",
    "tests/test_iot_data.py::test_search_index_reports_duplicate_client_id_after_a_takeover",
    "tests/test_iot_data.py::test_search_index_connectivity_is_isolated_across_accounts_and_regions",
    "tests/test_iot_data.py::test_shadow_update_over_mqtt_emits_accepted_delta_documents",
    "tests/test_iot_data.py::test_named_shadow_update_over_mqtt",
    "tests/test_iot_data.py::test_shadow_get_missing_over_mqtt_rejected_404",
    "tests/test_iot_data.py::test_shadow_accepted_drives_topic_rule_republish",
    "tests/test_iot_data.py::test_mqtt5_connect_is_accepted_with_properties",
    "tests/test_iot_data.py::test_mqtt311_connack_is_byte_identical",
    "tests/test_iot_data.py::test_unsupported_protocol_level_is_refused",
    "tests/test_iot_data.py::test_mqtt5_empty_client_id_gets_assigned_identifier",
    "tests/test_iot_data.py::test_mqtt5_unknown_property_is_ignored",
    "tests/test_iot_data.py::test_mqtt5_subscribe_options_byte_gets_v5_suback",
    "tests/test_iot_data.py::test_mqtt5_publish_round_trip_forwards_properties",
    "tests/test_iot_data.py::test_mqtt311_publisher_reaches_mqtt5_subscriber",
    "tests/test_iot_data.py::test_mqtt5_publisher_reaches_mqtt311_subscriber",
    "tests/test_iot_data.py::test_mqtt5_qos1_puback_carries_reason_code",
    "tests/test_iot_data.py::test_mqtt311_qos1_puback_stays_two_bytes",
    "tests/test_iot_data.py::test_mqtt5_unsuback_reason_codes_follow_the_actual_removal",
    "tests/test_iot_data.py::test_mqtt311_unsuback_stays_two_bytes",
    "tests/test_iot_data.py::test_mqtt5_disconnect_with_reason_code_closes_cleanly",
    "tests/test_iot_data.py::test_mqtt5_property_value_running_past_the_block_is_refused",
    "tests/test_iot_data.py::test_mqtt5_property_block_truncated_at_the_packet_end_is_refused",
    "tests/test_iot_data.py::test_mqtt5_malformed_connect_is_refused_with_a_connack_reason_code",
    "tests/test_iot_data.py::test_mqtt5_property_block_longer_than_127_bytes_round_trips",
    "tests/test_iot_data.py::test_mqtt5_no_local_withholds_a_publisher_its_own_message",
    "tests/test_iot_data.py::test_mqtt5_retain_as_published_keeps_the_publishers_retain_flag",
    "tests/test_iot_data.py::test_mqtt5_retain_handling_2_suppresses_the_retained_replay",
    "tests/test_iot_data.py::test_mqtt5_retain_handling_1_replays_only_for_a_new_subscription",
    "tests/test_iot_data.py::test_mqtt5_will_with_properties_fires_on_transport_drop",
    "tests/test_iot_data.py::test_mqtt5_malformed_will_properties_answer_connack_0x81",
    # The mTLS MQTT listener tests (in test_iot_data.py, with the rest of the
    # broker) drive private MiniStack subprocesses on their own ports rather
    # than the shared server — the same reason test_tls.py runs its subprocess
    # tests serially. Several of them time a process's startup, rebind or
    # shutdown, which is exactly what xdist load makes unreliable.
    "tests/test_iot_data.py::test_mtls_on_by_default",
    "tests/test_iot_data.py::test_mtls_disabled_by_env",
    "tests/test_iot_data.py::test_mtls_connect_and_connack",
    "tests/test_iot_data.py::test_mtls_subscribe_receives_http_publish",
    "tests/test_iot_data.py::test_mtls_publish_is_brokered",
    "tests/test_iot_data.py::test_mtls_no_client_cert_uses_default_account",
    "tests/test_iot_data.py::test_mtls_unregistered_cert_gets_connack_5",
    "tests/test_iot_data.py::test_mtls_mqtt5_refusal_is_v5_connack",
    "tests/test_iot_data.py::test_mtls_inactive_cert_refused",
    "tests/test_iot_data.py::test_mtls_ambiguous_cert_is_refused",
    "tests/test_iot_data.py::test_mtls_registered_ca_chain_connects",
    "tests/test_iot_data.py::test_mtls_account_scoped_delivery",
    "tests/test_iot_data.py::test_mtls_garbage_bytes_dropped",
    "tests/test_iot_data.py::test_mtls_duplicate_client_id_evicts_first_connection",
    "tests/test_iot_data.py::test_mtls_listener_survives_reset",
    "tests/test_iot_data.py::test_mtls_reset_rebinds_with_a_device_connected",
    "tests/test_iot_data.py::test_mtls_shutdown_completes_with_a_device_connected",
}


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "serial: test must run in a dedicated sequential phase",
    )


def pytest_collection_modifyitems(config, items):
    for item in items:
        nodeid = item.nodeid.split("[", 1)[0]
        if nodeid in _SERIAL_TESTS:
            item.add_marker("serial")


@pytest.fixture(autouse=True)
def _reset_request_context():
    """Reset the request-scoped account/region contextvars to their defaults
    before every test.

    Multi-tenancy tests set these in-process via ``set_request_account_id`` /
    ``set_request_region``. Without a per-test reset, a test that sets a
    non-default account (e.g. ``111111111111``) and doesn't restore it leaks
    that account to later tests on the same xdist worker — so an
    account-sensitive assertion (e.g. an ARN's "wrong account" that happens to
    equal the leaked real account) fails intermittently. ``reset_server`` only
    clears server state over HTTP; it never touches these contextvars.
    """
    from ministack.core.responses import set_request_account_id, set_request_region
    set_request_account_id("")   # non-12-digit -> MINISTACK_ACCOUNT_ID / 000000000000
    set_request_region(None)     # -> MINISTACK_REGION / us-east-1
    yield


@pytest.fixture(scope="session", autouse=True)
def reset_server(tmp_path_factory, worker_id):
    """Reset all server state once before the test session starts.

    Under pytest-xdist, every worker spawns its own session. If each worker
    calls /_ministack/reset on startup, a slow worker's reset can fire AFTER
    a faster worker has already begun creating fixtures, wiping that state
    mid-test. Use a filesystem barrier so only the first worker resets;
    the others wait for the marker and skip.
    """
    if worker_id == "master":
        # Single-process pytest run — no xdist.
        try:
            urllib.request.urlopen(
                urllib.request.Request(f"{ENDPOINT}/_ministack/reset",
                                       data=b"", method="POST"),
                timeout=5,
            )
        except Exception:
            pass
        return

    # xdist mode — coordinate via the shared root tmp dir (one level above
    # the per-worker tmp). Only the worker that creates the marker resets.
    root_tmp = tmp_path_factory.getbasetemp().parent
    marker = root_tmp / ".ministack_reset_done"
    lock = root_tmp / ".ministack_reset.lock"
    try:
        # O_CREAT|O_EXCL ensures only one worker wins the race.
        fd = os.open(str(lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        won = True
    except FileExistsError:
        won = False
    if won:
        try:
            urllib.request.urlopen(
                urllib.request.Request(f"{ENDPOINT}/_ministack/reset",
                                       data=b"", method="POST"),
                timeout=5,
            )
        except Exception:
            pass
        marker.write_text("ok")
    else:
        # Wait briefly for the chosen worker to finish its reset before
        # any tests on this worker touch the server.
        import time as _t
        deadline = _t.time() + 10
        while not marker.exists() and _t.time() < deadline:
            _t.sleep(0.1)


@pytest.fixture(scope="session")
def s3():
    return make_client("s3")


@pytest.fixture(scope="session")
def sqs():
    return make_client("sqs")


@pytest.fixture(scope="session")
def sns():
    return make_client("sns")


@pytest.fixture(scope="session")
def ddb():
    return make_client("dynamodb")


@pytest.fixture(scope="session")
def ddb_streams():
    return make_client("dynamodbstreams")


@pytest.fixture(scope="session")
def sts():
    return make_client("sts")


@pytest.fixture
def sts_as_role(sts):
    def _make(role_arn, session_name="test-session"):
        creds = sts.assume_role(RoleArn=role_arn, RoleSessionName=session_name)["Credentials"]
        return boto3.client(
            "sts",
            endpoint_url=ENDPOINT,
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
            region_name=REGION,
            config=Config(retries={"mode": "standard"}),
        )

    return _make


@pytest.fixture(scope="session")
def sm():
    return make_client("secretsmanager")


@pytest.fixture(scope="session")
def logs():
    # StartLiveTail uses hostPrefix ``stream-``; with a custom endpoint that
    # becomes ``stream-127.0.0.1`` which does not resolve. Disable injection so
    # Live Tail hits the same MiniStack listener as every other Logs API.
    return make_client("logs", additional_config_kwargs={"inject_host_prefix": False})


@pytest.fixture(scope="session")
def lam():
    return make_client("lambda")


@pytest.fixture(scope="session")
def iam():
    return make_client("iam")


@pytest.fixture(scope="session")
def ssm():
    return make_client("ssm")


@pytest.fixture(scope="session")
def eb():
    return make_client("events")


@pytest.fixture(scope="session")
def kin():
    return make_client("kinesis")


@pytest.fixture(scope="session")
def cw():
    return make_client("cloudwatch")


@pytest.fixture(scope="session")
def ses():
    return make_client("ses")


@pytest.fixture(scope="session")
def sfn():
    return make_client("stepfunctions")


@pytest.fixture(scope="session")
def ecs():
    return make_client("ecs")


@pytest.fixture(scope="session")
def rds():
    return make_client("rds")


@pytest.fixture(scope="session")
def ecr():
    return make_client("ecr")


@pytest.fixture(scope="session")
def ec():
    return make_client("elasticache")


@pytest.fixture(scope="session")
def glue():
    return make_client("glue")


@pytest.fixture(scope="session")
def athena():
    return make_client("athena")


def _ministack_config(settings):
    """Set runtime config on the running server via POST /_ministack/config."""
    import json

    req = urllib.request.Request(
        f"{ENDPOINT}/_ministack/config",
        data=json.dumps(settings).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=5)


@pytest.fixture(scope="session")
def fh():
    return make_client("firehose")


@pytest.fixture(scope="session")
def apigw():
    return make_client("apigatewayv2")


@pytest.fixture(scope="session")
def apigw_v1():
    return make_client("apigateway")


@pytest.fixture(scope="session")
def r53():
    return make_client("route53")


@pytest.fixture(scope="session")
def cognito_idp():
    return make_client("cognito-idp")


@pytest.fixture(scope="session")
def cognito_identity():
    return make_client("cognito-identity")


@pytest.fixture(scope="session")
def ec2():
    return make_client("ec2")


@pytest.fixture(scope="session")
def emr():
    return make_client("emr")


@pytest.fixture(scope="session")
def elbv2():
    return make_client("elbv2")


@pytest.fixture(scope="session")
def efs():
    return make_client("efs")


@pytest.fixture(scope="session")
def acm_client():
    return make_client("acm")


@pytest.fixture(scope="session")
def iot_client():
    return make_client("iot")


@pytest.fixture(scope="session")
def iot_data_client():
    return make_client("iot-data")


@pytest.fixture(scope="session")
def iot_jobs_data():
    return make_client("iot-jobs-data")


@pytest.fixture(scope="session")
def wafv2():
    return make_client("wafv2")


@pytest.fixture(scope="session")
def sesv2():
    return make_client("sesv2")


@pytest.fixture(scope="session")
def cfn():
    return make_client("cloudformation")


@pytest.fixture(scope="session")
def opensearch():
    return make_client("opensearch")


@pytest.fixture(scope="session")
def kms_client():
    return make_client("kms")


@pytest.fixture(scope="session")
def sfn_sync():
    """SFN client for StartSyncExecution — forces same endpoint (boto3 normally prefixes sync-)."""
    from botocore.config import Config as BotoConfig

    return boto3.client(
        "stepfunctions",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
        config=BotoConfig(
            region_name=REGION,
            retries={"mode": "standard"},
            max_pool_connections=50,
            inject_host_prefix=False,
        ),
    )


@pytest.fixture(scope="session")
def cloudfront():
    return make_client("cloudfront")


@pytest.fixture(scope="session")
def cloudfront_kvs():
    from botocore import UNSIGNED
    from botocore.config import Config as BotoConfig

    return boto3.client(
        "cloudfront-keyvaluestore",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
        config=BotoConfig(
            region_name=REGION,
            signature_version=UNSIGNED,
            retries={"mode": "standard"},
            max_pool_connections=50,
            inject_host_prefix=False,
        ),
    )


@pytest.fixture(scope="session")
def rds_data():
    return make_client("rds-data")


@pytest.fixture(scope="session")
def appconfig_client():
    return make_client("appconfig")


@pytest.fixture(scope="session")
def appconfigdata_client():
    return make_client("appconfigdata")


@pytest.fixture(scope="session")
def sd():
    """SD client for DiscoverInstances — forces same endpoint (boto3 normally prefixes data-)."""
    from botocore.config import Config as BotoConfig

    return boto3.client(
        "servicediscovery",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
        config=BotoConfig(
            region_name=REGION,
            retries={"mode": "standard"},
            max_pool_connections=50,
            inject_host_prefix=False,
        ),
    )


@pytest.fixture(scope="session")
def codebuild():
    return make_client("codebuild")


@pytest.fixture(scope="session")
def autoscaling():
    return make_client("autoscaling")


@pytest.fixture(scope="session")
def transfer():
    return make_client("transfer")


@pytest.fixture(scope="session")
def eks():
    return make_client("eks")


@pytest.fixture(scope="session")
def appsync():
    return make_client("appsync")


@pytest.fixture(scope="session")
def scheduler():
    return make_client("scheduler")


@pytest.fixture(scope="session")
def tagging():
    return make_client("resourcegroupstaggingapi")

@pytest.fixture(scope="session")
def cur():
    return make_client("cur")


@pytest.fixture(scope="session")
def inspector2():
    return make_client("inspector2")


@pytest.fixture(scope="session")
def mq():
    return make_client("mq")


@pytest.fixture(scope="session")
def dsql():
    return make_client("dsql")


@pytest.fixture(scope="session")
def s3tables():
    return make_client("s3tables")
