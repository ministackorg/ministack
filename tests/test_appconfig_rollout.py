"""
AppConfig gradual rollout: deployments walk DEPLOYING -> BAKING -> COMPLETE
over their strategy, environment monitors roll them back, and the data plane
serves each client the version the rollout has reached.

Most tests drive the handlers in-process against a fake clock with one
strategy minute lasting 60 real seconds (AWS's pace), so a 30-minute strategy
can be stepped through without sleeping. The boto3 tests at the end run
against the shared server at its default pace, where deployments finish as
they start.
"""

import asyncio
import json
import time

import pytest
from botocore.exceptions import ClientError

from ministack.core.responses import get_account_id
from ministack.services import appconfig as appconfig_service
from ministack.services import cloudwatch as cloudwatch_service

MINUTE = 60.0
START = 1_800_000_000.0


@pytest.fixture
def clock(monkeypatch):
    now = [START]
    monkeypatch.setattr(time, "time", lambda: now[0])
    monkeypatch.setattr(appconfig_service, "_DEPLOYMENT_MINUTE_SECONDS", MINUTE)
    appconfig_service.reset()
    cloudwatch_service.reset()
    yield now
    appconfig_service.reset()
    cloudwatch_service.reset()


def _appconfig(method, path, body=None, headers=None, query=None):
    raw = json.dumps(body).encode() if isinstance(body, dict) else (body or b"")
    status, resp_headers, resp_body = asyncio.run(appconfig_service.handle_request(
        method, path, {k.lower(): v for k, v in (headers or {}).items()}, raw, query or {}))
    return status, resp_headers, resp_body


def _appconfig_json(method, path, body=None, headers=None):
    status, _, resp_body = _appconfig(method, path, body, headers)
    return status, json.loads(resp_body) if resp_body else {}


def _cloudwatch(action, body):
    headers = {"x-amz-target": f"GraniteServiceVersion20100801.{action}",
               "content-type": "application/x-amz-json-1.0"}
    status, _, _ = asyncio.run(cloudwatch_service.handle_request(
        "POST", "/", headers, json.dumps(body).encode(), {}))
    assert status == 200


def _alarm(name):
    _cloudwatch("PutMetricAlarm", {
        "AlarmName": name, "Namespace": "Orders", "MetricName": "Errors",
        "Statistic": "Sum", "Period": 60, "EvaluationPeriods": 1,
        "Threshold": 1.0, "ComparisonOperator": "GreaterThanOrEqualToThreshold",
    })
    return f"arn:aws:cloudwatch:us-east-1:{get_account_id()}:alarm:{name}"


def _set_alarm(name, state):
    _cloudwatch("SetAlarmState", {"AlarmName": name, "StateValue": state, "StateReason": "test"})


class Target:
    """An application, environment and hosted profile holding versions 1 and 2."""

    def __init__(self, monitors=None):
        _, app = _appconfig_json("POST", "/applications", {"Name": "orders"})
        _, env = _appconfig_json("POST", f"/applications/{app['Id']}/environments",
                                 {"Name": "prod", "Monitors": monitors or []})
        _, profile = _appconfig_json("POST", f"/applications/{app['Id']}/configurationprofiles",
                                     {"Name": "settings", "LocationUri": "hosted"})
        self.app, self.env, self.profile = app["Id"], env["Id"], profile["Id"]
        for version in (1, 2):
            status, _, _ = _appconfig(
                "POST",
                f"/applications/{self.app}/configurationprofiles/{self.profile}/hostedconfigurationversions",
                json.dumps({"version": version}).encode(),
                {"Content-Type": "application/json", "VersionLabel": f"v{version}"},
            )
            assert status == 201

    @property
    def deployments_path(self):
        return f"/applications/{self.app}/environments/{self.env}/deployments"

    def deploy(self, version, strategy="AppConfig.Linear20PercentEvery6Minutes", **extra):
        return _appconfig_json("POST", self.deployments_path, {
            "DeploymentStrategyId": strategy, "ConfigurationProfileId": self.profile,
            "ConfigurationVersion": str(version), **extra,
        })

    def deployment(self, number):
        status, body = _appconfig_json("GET", f"{self.deployments_path}/{number}")
        assert status == 200
        return body

    def stop(self, number, allow_revert=False):
        headers = {"Allow-Revert": "true"} if allow_revert else {}
        return _appconfig_json("DELETE", f"{self.deployments_path}/{number}", headers=headers)

    def environment_state(self):
        return _appconfig_json("GET", f"/applications/{self.app}/environments/{self.env}")[1]["State"]

    def session(self, **extra):
        status, body = _appconfig_json("POST", "/configurationsessions", {
            "ApplicationIdentifier": self.app, "EnvironmentIdentifier": self.env,
            "ConfigurationProfileIdentifier": self.profile, **extra,
        })
        assert status == 201
        return Client(body["InitialConfigurationToken"])


class Client:
    """One configuration session, polling with the token it was last handed."""

    def __init__(self, token):
        self.session_id = token
        self.token = token

    def poll(self):
        status, headers, body = _appconfig("GET", "/configuration", query={"configuration_token": self.token})
        assert status == 200
        self.token = headers["Next-Poll-Configuration-Token"]
        return headers, json.loads(body) if body else None


def _event_types(deployment):
    return [event["EventType"] for event in deployment["EventLog"]]


def test_rollout_steps_follow_the_growth_type():
    assert appconfig_service._rollout_steps("LINEAR", 20.0) == [20.0, 40.0, 60.0, 80.0, 100.0]
    assert appconfig_service._rollout_steps("LINEAR", 100.0) == [100.0]
    # G*(2^N) from the CreateDeploymentStrategy docs.
    assert appconfig_service._rollout_steps("EXPONENTIAL", 10.0) == [10.0, 20.0, 40.0, 80.0, 100.0]
    assert appconfig_service._rollout_steps("EXPONENTIAL", 2.0) == [2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 100.0]
    # Below the model's 1.0 minimum (stored unvalidated) still terminates.
    assert appconfig_service._rollout_steps("LINEAR", -5.0)[-1] == 100.0


def test_linear_deployment_steps_bakes_and_completes(clock):
    target = Target()
    status, deployment = target.deploy(1)
    assert status == 201
    assert deployment["State"] == "DEPLOYING"
    assert deployment["PercentageComplete"] == 0.0
    assert _event_types(deployment) == ["DEPLOYMENT_STARTED"]
    assert target.environment_state() == "DEPLOYING"

    # Linear20PercentEvery6Minutes: five 20% steps over 30 minutes, then a 30-minute bake.
    clock[0] = START + 6 * MINUTE
    assert target.deployment(1)["PercentageComplete"] == 20.0
    clock[0] = START + 12 * MINUTE - 1
    assert target.deployment(1)["PercentageComplete"] == 20.0
    clock[0] = START + 12 * MINUTE
    assert target.deployment(1)["PercentageComplete"] == 40.0

    clock[0] = START + 30 * MINUTE
    deployment = target.deployment(1)
    assert (deployment["State"], deployment["PercentageComplete"]) == ("BAKING", 100.0)
    assert target.environment_state() == "DEPLOYING"

    clock[0] = START + 60 * MINUTE
    deployment = target.deployment(1)
    assert deployment["State"] == "COMPLETE"
    assert deployment["CompletedAt"] == appconfig_service._iso(START + 60 * MINUTE)
    assert target.environment_state() == "READY_FOR_DEPLOYMENT"
    # Most recent first.
    assert _event_types(deployment) == [
        "DEPLOYMENT_COMPLETED", "BAKE_TIME_STARTED",
        *["PERCENTAGE_UPDATED"] * 5,
        "DEPLOYMENT_STARTED",
    ]
    first_step = deployment["EventLog"][-2]
    assert first_step["OccurredAt"] == appconfig_service._iso(START + 6 * MINUTE)
    assert first_step["TriggeredBy"] == "APPCONFIG"


def test_list_deployments_reports_rollout_progress(clock):
    target = Target()
    target.deploy(1, strategy="AppConfig.Canary10Percent20Minutes")
    clock[0] = START + 4 * MINUTE
    status, body = _appconfig_json("GET", target.deployments_path)
    assert status == 200
    [summary] = body["Items"]
    assert (summary["State"], summary["PercentageComplete"]) == ("DEPLOYING", 10.0)
    assert summary["VersionLabel"] == "v1"
    assert "CompletedAt" not in summary


def test_start_deployment_conflicts_with_one_in_progress(clock):
    target = Target()
    target.deploy(1)
    status, body = target.deploy(2)
    assert status == 409
    assert body["__type"] == "ConflictException"

    # Still in progress while baking.
    clock[0] = START + 30 * MINUTE
    assert target.deploy(2)[0] == 409

    clock[0] = START + 60 * MINUTE
    status, body = target.deploy(2)
    assert status == 201
    assert body["DeploymentNumber"] == 2


def test_start_deployment_checks_latest_deployment_number(clock):
    target = Target()
    assert target.deploy(1, strategy="AppConfig.AllAtOnce", LatestDeploymentNumber=0)[0] == 201
    clock[0] = START + 10 * MINUTE
    status, body = target.deploy(2, strategy="AppConfig.AllAtOnce", LatestDeploymentNumber=0)
    assert status == 409
    assert body["__type"] == "ConflictException"
    assert target.deploy(2, strategy="AppConfig.AllAtOnce", LatestDeploymentNumber=1)[0] == 201


def test_stop_rolls_back_an_in_progress_deployment(clock):
    target = Target()
    target.deploy(1, strategy="AppConfig.AllAtOnce")
    clock[0] = START + 10 * MINUTE
    client = target.session()
    assert client.poll()[1] == {"version": 1}

    target.deploy(2)
    clock[0] = START + 40 * MINUTE  # 100%, baking
    assert client.poll()[1] == {"version": 2}

    status, deployment = target.stop(2)
    assert status == 202
    assert deployment["State"] == "ROLLED_BACK"
    assert _event_types(deployment)[:2] == ["ROLLBACK_COMPLETED", "ROLLBACK_STARTED"]
    assert deployment["EventLog"][0]["TriggeredBy"] == "USER"
    assert target.environment_state() == "ROLLED_BACK"
    assert client.poll()[1] == {"version": 1}

    # A stopped deployment cannot be stopped again, and the environment takes a new one.
    assert target.stop(2)[0] == 400
    assert target.deploy(2)[0] == 201


def test_revert_requires_allow_revert_and_the_72_hour_window(clock):
    target = Target()
    target.deploy(1, strategy="AppConfig.AllAtOnce")
    clock[0] = START + 10 * MINUTE
    target.deploy(2, strategy="AppConfig.AllAtOnce")
    clock[0] = START + 20 * MINUTE
    assert target.deployment(2)["State"] == "COMPLETE"

    status, body = target.stop(2)
    assert status == 400
    assert body["__type"] == "BadRequestException"

    clock[0] = START + 20 * MINUTE + 72 * 3600 + 1
    assert target.stop(2, allow_revert=True)[0] == 400

    clock[0] = START + 20 * MINUTE + 72 * 3600
    status, deployment = target.stop(2, allow_revert=True)
    assert status == 202
    assert deployment["State"] == "REVERTED"
    assert target.environment_state() == "REVERTED"
    assert target.session().poll()[1] == {"version": 1}


def test_monitor_alarm_rolls_back_at_the_moment_it_fires(clock):
    alarm_arn = _alarm("orders-errors")
    target = Target(monitors=[{"AlarmArn": alarm_arn}])
    target.deploy(1, strategy="AppConfig.AllAtOnce")
    clock[0] = START + 10 * MINUTE
    target.deploy(2)
    deploy_start = clock[0]

    clock[0] = deploy_start + 7 * MINUTE
    _cloudwatch("PutMetricData", {"Namespace": "Orders", "MetricData": [{"MetricName": "Errors", "Value": 5}]})
    clock[0] = deploy_start + 20 * MINUTE

    deployment = target.deployment(2)
    assert deployment["State"] == "ROLLED_BACK"
    assert deployment["PercentageComplete"] == 20.0  # the 12-minute step never came
    rollback_started = deployment["EventLog"][1]
    assert rollback_started["EventType"] == "ROLLBACK_STARTED"
    assert rollback_started["TriggeredBy"] == "CLOUDWATCH_ALARM"
    assert alarm_arn in rollback_started["Description"]
    assert rollback_started["OccurredAt"] == appconfig_service._iso(deploy_start + 7 * MINUTE)
    assert target.environment_state() == "ROLLED_BACK"
    assert target.session().poll()[1] == {"version": 1}


def test_alarm_that_recovers_during_the_bake_still_rolls_back(clock):
    target = Target(monitors=[{"AlarmArn": _alarm("orders-errors")}])
    target.deploy(1)
    clock[0] = START + 35 * MINUTE
    _set_alarm("orders-errors", "ALARM")
    clock[0] = START + 36 * MINUTE
    _set_alarm("orders-errors", "OK")

    clock[0] = START + 90 * MINUTE
    deployment = target.deployment(1)
    assert deployment["State"] == "ROLLED_BACK"
    assert deployment["PercentageComplete"] == 100.0
    assert "BAKE_TIME_STARTED" in _event_types(deployment)


def test_alarm_after_completion_does_not_roll_back(clock):
    target = Target(monitors=[{"AlarmArn": _alarm("orders-errors")}])
    target.deploy(1)
    clock[0] = START + 61 * MINUTE
    _set_alarm("orders-errors", "ALARM")
    clock[0] = START + 62 * MINUTE
    assert target.deployment(1)["State"] == "COMPLETE"


def test_alarm_already_firing_rolls_back_before_any_step(clock, monkeypatch):
    monkeypatch.setattr(appconfig_service, "_DEPLOYMENT_MINUTE_SECONDS", 0.0)
    target = Target(monitors=[{"AlarmArn": _alarm("orders-errors")}])
    _set_alarm("orders-errors", "ALARM")
    status, deployment = target.deploy(1, strategy="AppConfig.AllAtOnce")
    assert status == 201
    assert deployment["State"] == "ROLLED_BACK"
    assert deployment["PercentageComplete"] == 0.0


def test_monitor_on_an_unknown_alarm_is_ignored(clock):
    target = Target(monitors=[{"AlarmArn": "arn:aws:cloudwatch:us-east-1:000000000000:alarm:missing"}])
    target.deploy(1, strategy="AppConfig.AllAtOnce")
    clock[0] = START + 10 * MINUTE
    assert target.deployment(1)["State"] == "COMPLETE"


def test_gradual_rollout_reaches_each_client_once_its_slot_is_covered(clock):
    target = Target()
    target.deploy(1, strategy="AppConfig.AllAtOnce")
    clock[0] = START + 10 * MINUTE
    clients = [target.session() for _ in range(60)]
    for client in clients:
        assert client.poll()[1] == {"version": 1}

    target.deploy(2)
    deploy_start = clock[0]
    clock[0] = deploy_start + 12 * MINUTE  # 40%
    reached = set()
    for client in clients:
        served = client.poll()[1]
        if appconfig_service._rollout_bucket(client.session_id) < 40:
            assert served == {"version": 2}
            reached.add(client.session_id)
        else:
            assert served is None  # still on version 1, which it already has
    assert 0 < len(reached) < len(clients)

    clock[0] = deploy_start + 30 * MINUTE  # 100%
    for client in clients:
        served = client.poll()[1]
        assert served == (None if client.session_id in reached else {"version": 2})


def test_poll_returns_empty_configuration_until_it_changes(clock):
    target = Target()
    client = target.session()
    headers, served = client.poll()
    assert served is None  # nothing deployed yet

    target.deploy(1, strategy="AppConfig.AllAtOnce")
    headers, served = client.poll()
    assert served == {"version": 1}
    assert headers["Version-Label"] == "v1"
    headers, served = client.poll()
    assert served is None
    assert headers["Version-Label"] == "v1"


def test_session_poll_interval(clock):
    target = Target()
    assert target.session().poll()[0]["Next-Poll-Interval-In-Seconds"] == "30"
    assert target.session(RequiredMinimumPollIntervalInSeconds=90).poll()[0]["Next-Poll-Interval-In-Seconds"] == "90"
    status, body = _appconfig_json("POST", "/configurationsessions", {
        "ApplicationIdentifier": target.app, "EnvironmentIdentifier": target.env,
        "ConfigurationProfileIdentifier": target.profile, "RequiredMinimumPollIntervalInSeconds": 5,
    })
    assert status == 400
    assert body["__type"] == "BadRequestException"


# ---------------------------------------------------------------------------
# boto3 against the shared server (deployments finish as they start)
# ---------------------------------------------------------------------------


def _live_target(appconfig_client, name, monitors=None):
    app = appconfig_client.create_application(Name=name)
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="prod", Monitors=monitors or [])
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="settings", LocationUri="hosted")
    appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"], ConfigurationProfileId=profile["Id"],
        Content=b'{"version": 1}', ContentType="application/json", VersionLabel="v1")
    return app["Id"], env["Id"], profile["Id"]


def test_live_alarm_in_alarm_rolls_back_deployment(appconfig_client, cw):
    alarm_name = "appconfig-rollout-live-errors"
    cw.put_metric_alarm(
        AlarmName=alarm_name, Namespace="Orders", MetricName="Errors", Statistic="Sum",
        Period=60, EvaluationPeriods=1, Threshold=1.0, ComparisonOperator="GreaterThanOrEqualToThreshold")
    alarm_arn = cw.describe_alarms(AlarmNames=[alarm_name])["MetricAlarms"][0]["AlarmArn"]
    cw.set_alarm_state(AlarmName=alarm_name, StateValue="ALARM", StateReason="test")
    try:
        app_id, env_id, profile_id = _live_target(
            appconfig_client, "rollout-live-alarm", monitors=[{"AlarmArn": alarm_arn}])
        deployment = appconfig_client.start_deployment(
            ApplicationId=app_id, EnvironmentId=env_id, DeploymentStrategyId="AppConfig.AllAtOnce",
            ConfigurationProfileId=profile_id, ConfigurationVersion="1")
        assert deployment["State"] == "ROLLED_BACK"
        assert deployment["EventLog"][0]["TriggeredBy"] == "CLOUDWATCH_ALARM"
        environment = appconfig_client.get_environment(ApplicationId=app_id, EnvironmentId=env_id)
        assert environment["State"] == "ROLLED_BACK"
    finally:
        cw.delete_alarms(AlarmNames=[alarm_name])


def test_live_data_plane_version_label_and_unchanged_poll(appconfig_client, appconfigdata_client):
    app_id, env_id, profile_id = _live_target(appconfig_client, "rollout-live-poll")
    appconfig_client.start_deployment(
        ApplicationId=app_id, EnvironmentId=env_id, DeploymentStrategyId="AppConfig.AllAtOnce",
        ConfigurationProfileId=profile_id, ConfigurationVersion="1")
    token = appconfigdata_client.start_configuration_session(
        ApplicationIdentifier=app_id, EnvironmentIdentifier=env_id,
        ConfigurationProfileIdentifier=profile_id, RequiredMinimumPollIntervalInSeconds=45,
    )["InitialConfigurationToken"]

    first = appconfigdata_client.get_latest_configuration(ConfigurationToken=token)
    assert json.loads(first["Configuration"].read()) == {"version": 1}
    assert first["VersionLabel"] == "v1"
    assert first["NextPollIntervalInSeconds"] == 45

    second = appconfigdata_client.get_latest_configuration(
        ConfigurationToken=first["NextPollConfigurationToken"])
    assert second["Configuration"].read() == b""


def test_live_concurrent_deployment_conflicts(appconfig_client):
    app_id, env_id, profile_id = _live_target(appconfig_client, "rollout-live-conflict")
    appconfig_client.start_deployment(
        ApplicationId=app_id, EnvironmentId=env_id, DeploymentStrategyId="AppConfig.AllAtOnce",
        ConfigurationProfileId=profile_id, ConfigurationVersion="1")
    with pytest.raises(ClientError) as exc:
        appconfig_client.start_deployment(
            ApplicationId=app_id, EnvironmentId=env_id, DeploymentStrategyId="AppConfig.AllAtOnce",
            ConfigurationProfileId=profile_id, ConfigurationVersion="1", LatestDeploymentNumber=0)
    assert exc.value.response["Error"]["Code"] == "ConflictException"
