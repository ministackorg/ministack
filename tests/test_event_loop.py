import asyncio
import gc
import json
import subprocess
import sys
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor

import pytest

import ministack.app as ministack_app
from ministack.core.concurrency import LoopLocal
from ministack.core.responses import (
    get_account_id,
    get_region,
    set_request_account_id,
    set_request_region,
)
from ministack.services import (
    dynamodb,
    ecs,
    eks,
    elasticache,
    eventbridge,
    opensearch,
    rds,
    rds_data,
    s3,
    stepfunctions,
    tagging,
)
from ministack.services import (
    scheduler as scheduler_service,
)
from ministack.services.cloudformation import lifecycle as cfn_lifecycle
from ministack.services.cloudformation import provisioners as cfn_provisioners
from ministack.services.cloudformation import stacks as cfn_stacks


class _SlowContainer:
    def stop(self, **_kwargs):
        time.sleep(0.35)

    def remove(self, **_kwargs):
        pass


class _SlowContainers:
    def __init__(self, container_id):
        self._container_id = container_id
        self._container = _SlowContainer()

    def get(self, container_id):
        if container_id != self._container_id:
            raise RuntimeError(f"unexpected container lookup: {container_id}")
        return self._container


class _SlowDocker:
    def __init__(self, container_id):
        self.containers = _SlowContainers(container_id)


async def _call_stepfunctions_action(action, data=None):
    return await stepfunctions.handle_request(
        "POST",
        "/",
        {"x-amz-target": f"AWSStepFunctions.{action}"},
        json.dumps(data or {}).encode(),
        {},
    )


async def _create_rds_integration_state_machine(name):
    response = await _call_stepfunctions_action(
        "CreateStateMachine",
        {
            "name": name,
            "roleArn": "arn:aws:iam::000000000000:role/sfn-role",
            "definition": json.dumps(
                {
                    "StartAt": "Describe",
                    "States": {
                        "Describe": {
                            "Type": "Task",
                            "Resource": (
                                "arn:aws:states:::aws-sdk:rds:DescribeDBClusters"
                            ),
                            "End": True,
                        }
                    },
                }
            ),
        },
    )
    assert response[0] == 200
    return json.loads(response[2])["stateMachineArn"]


async def _wait_for_state_machine_terminal_success(state_machine_arn):
    deadline = asyncio.get_running_loop().time() + 3
    while asyncio.get_running_loop().time() < deadline:
        matches = [
            execution
            for execution in stepfunctions._executions.values()
            if execution["stateMachineArn"] == state_machine_arn
        ]
        if matches and matches[-1]["status"] != "RUNNING":
            assert matches[-1]["status"] == "SUCCEEDED", matches[-1]
            return matches[-1]
        await asyncio.sleep(0.01)
    pytest.fail(f"execution for {state_machine_arn} did not reach terminal success")


def _configure_eventbridge_sfn_target(state_machine_arn, *, rule_name, source=None):
    eventbridge._ensure_default_bus()
    rule_data = {
        "Name": rule_name,
        "EventBusName": "default",
        "State": "ENABLED",
    }
    if source is None:
        rule_data["ScheduleExpression"] = "rate(1 minute)"
    else:
        rule_data["EventPattern"] = json.dumps({"source": [source]})
    assert eventbridge._put_rule(rule_data)[0] == 200
    assert eventbridge._put_targets(
        {
            "Rule": rule_name,
            "EventBusName": "default",
            "Targets": [{"Id": "sfn", "Arn": state_machine_arn}],
        }
    )[0] == 200


async def _run_with_parallel_dynamodb_probe(slow_request):
    async def probe():
        started = time.perf_counter()
        await asyncio.sleep(0.05)
        response = await dynamodb.handle_request(
            "POST",
            "/",
            {"x-amz-target": "DynamoDB_20120810.ListTables"},
            b"{}",
            {},
        )
        return response, time.perf_counter() - started

    # Start the probe first so its timer is armed before the slow request.
    # Without worker offload, the slow Docker call blocks that timer for 350ms.
    probe_task = asyncio.create_task(probe())
    slow_task = asyncio.create_task(slow_request())
    probe_response, elapsed = await probe_task
    slow_response = await slow_task

    assert probe_response[0] == 200
    assert elapsed < 0.2
    assert slow_response[0] == 200


@pytest.mark.parametrize(
    "service",
    [rds, ecs, eks, elasticache, opensearch],
    ids=["rds", "ecs", "eks", "elasticache", "opensearch"],
)
def test_sync_service_dispatch_runs_off_loop_and_stays_serial(monkeypatch, service):
    first_started = threading.Event()
    release = threading.Event()
    call_count = 0
    count_lock = threading.Lock()

    def slow_dispatch(*_args):
        nonlocal call_count
        with count_lock:
            call_count += 1
            if call_count == 1:
                first_started.set()
        release.wait(timeout=2)
        return 200, {}, b"ok"

    monkeypatch.setattr(service, "_handle_request_unlocked", slow_dispatch)

    async def exercise():
        first = asyncio.create_task(
            service.handle_request("GET", "/", {}, b"", {})
        )
        while not first_started.is_set():
            await asyncio.sleep(0.001)

        # A fallback timer prevents a broken implementation from hanging the
        # suite. If dispatch still runs on the event loop, it fires before this
        # coroutine can resume and the assertion identifies the regression.
        assert not release.is_set()

        second = asyncio.create_task(
            service.handle_request("GET", "/", {}, b"", {})
        )
        await asyncio.sleep(0.05)
        with count_lock:
            assert call_count == 1

        release.set()
        assert await asyncio.gather(first, second) == [
            (200, {}, b"ok"),
            (200, {}, b"ok"),
        ]

    fallback = threading.Timer(1, release.set)
    fallback.start()
    try:
        asyncio.run(exercise())
    finally:
        release.set()
        fallback.cancel()


@pytest.mark.parametrize(
    ("service_name", "service"),
    [("ecs", ecs), ("elasticache", elasticache)],
)
def test_tagging_waits_for_service_dispatch(monkeypatch, service_name, service):
    owner_started = threading.Event()
    release_owner = threading.Event()
    tagging_entered = threading.Event()

    def slow_owner_dispatch(*_args):
        owner_started.set()
        release_owner.wait(timeout=2)
        return 200, {}, b"ok"

    def tagging_writer(_spec, _arn, _tags):
        tagging_entered.set()

    monkeypatch.setattr(service, "_handle_request_unlocked", slow_owner_dispatch)
    monkeypatch.setitem(tagging._WRITERS, service_name, tagging_writer)

    async def exercise():
        owner = asyncio.create_task(service.handle_request("GET", "/", {}, b"", {}))
        while not owner_started.is_set():
            await asyncio.sleep(0.001)

        arn = f"arn:aws:{service_name}:us-east-1:000000000000:resource/test"
        cross_service = asyncio.create_task(
            tagging.handle_request(
                "POST",
                "/",
                {"x-amz-target": "ResourceGroupsTaggingAPI_20170126.TagResources"},
                json.dumps({"ResourceARNList": [arn], "Tags": {"source": "rgta"}}).encode(),
                {},
            )
        )
        await asyncio.sleep(0.05)
        assert not tagging_entered.is_set()

        release_owner.set()
        assert (await owner)[0] == 200
        assert (await cross_service)[0] == 200
        assert tagging_entered.is_set()

    fallback = threading.Timer(1, release_owner.set)
    fallback.start()
    try:
        asyncio.run(exercise())
    finally:
        release_owner.set()
        fallback.cancel()


def test_tagging_does_not_wait_for_unrelated_service_dispatch(monkeypatch):
    owner_started = threading.Event()
    release_owner = threading.Event()
    tagging_entered = threading.Event()

    def slow_owner_dispatch(*_args):
        owner_started.set()
        release_owner.wait(timeout=2)
        return 200, {}, b"ok"

    def tagging_writer(_spec, _arn, _tags):
        tagging_entered.set()

    monkeypatch.setattr(ecs, "_handle_request_unlocked", slow_owner_dispatch)
    monkeypatch.setitem(tagging._WRITERS, "s3", tagging_writer)

    async def exercise():
        owner = asyncio.create_task(ecs.handle_request("GET", "/", {}, b"", {}))
        while not owner_started.is_set():
            await asyncio.sleep(0.001)

        cross_service = asyncio.create_task(
            tagging.handle_request(
                "POST",
                "/",
                {"x-amz-target": "ResourceGroupsTaggingAPI_20170126.TagResources"},
                json.dumps(
                    {
                        "ResourceARNList": ["arn:aws:s3:::event-loop-unrelated"],
                        "Tags": {"source": "rgta"},
                    }
                ).encode(),
                {},
            )
        )
        await asyncio.wait_for(cross_service, timeout=0.2)
        assert tagging_entered.is_set()

        release_owner.set()
        assert (await owner)[0] == 200

    fallback = threading.Timer(1, release_owner.set)
    fallback.start()
    try:
        asyncio.run(exercise())
    finally:
        release_owner.set()
        fallback.cancel()


@pytest.mark.parametrize("body", [b"[]", b"null", b'"str"', b"5"])
def test_tagging_admission_classifier_rejects_non_object_json(body):
    with pytest.raises(tagging.InvalidRequestBody):
        tagging.classify_request_admission(
            {"x-amz-target": "ResourceGroupsTaggingAPI_20170126.TagResources"},
            body,
        )


@pytest.mark.parametrize("body", [b"[]", b"null", b'"str"', b"5"])
def test_tagging_non_object_json_returns_serialization_error(body):
    response = []
    request_sent = False

    async def receive():
        nonlocal request_sent
        if request_sent:
            return {"type": "http.disconnect"}
        request_sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        response.append(message)

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [
            (
                b"x-amz-target",
                b"ResourceGroupsTaggingAPI_20170126.TagResources",
            ),
            (
                b"authorization",
                b"AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/tagging/aws4_request",
            ),
        ],
    }

    asyncio.run(ministack_app.app(scope, receive, send))

    assert response[0]["status"] == 400
    assert json.loads(response[-1]["body"])["__type"] == "SerializationException"


def test_rds_data_waits_for_rds_dispatch(monkeypatch):
    owner_started = threading.Event()
    release_owner = threading.Event()
    data_handler_entered = threading.Event()

    def slow_owner_dispatch(*_args):
        owner_started.set()
        release_owner.wait(timeout=2)
        return 200, {}, b"ok"

    def data_handler(_data):
        data_handler_entered.set()
        return 200, {}, b"ok"

    monkeypatch.setattr(rds, "_handle_request_unlocked", slow_owner_dispatch)
    monkeypatch.setattr(rds_data, "_execute_statement", data_handler)

    async def exercise():
        owner = asyncio.create_task(rds.handle_request("GET", "/", {}, b"", {}))
        while not owner_started.is_set():
            await asyncio.sleep(0.001)

        cross_service = asyncio.create_task(
            rds_data.handle_request("POST", "/Execute", {}, b"{}", {})
        )
        await asyncio.sleep(0.05)
        assert not data_handler_entered.is_set()

        release_owner.set()
        assert (await owner)[0] == 200
        assert (await cross_service)[0] == 200
        assert data_handler_entered.is_set()

    fallback = threading.Timer(1, release_owner.set)
    fallback.start()
    try:
        asyncio.run(exercise())
    finally:
        release_owner.set()
        fallback.cancel()


def test_rds_data_does_not_block_parallel_dynamodb(monkeypatch):
    def slow_statement(_data):
        time.sleep(0.35)
        return 200, {}, b"ok"

    monkeypatch.setattr(rds_data, "_execute_statement", slow_statement)

    asyncio.run(
        _run_with_parallel_dynamodb_probe(
            lambda: rds_data.handle_request("POST", "/Execute", {}, b"{}", {})
        )
    )


def test_nested_cfn_standard_children_share_service_admission(monkeypatch):
    current = {}

    def slow_owner_dispatch(*_args):
        current["owner_started"].set()
        current["release_owner"].wait(timeout=2)
        return 200, {}, b"ok"

    def child_create(*_args):
        current["child_entered"].set()
        current["observed_scope"] = (get_account_id(), get_region())
        return "child-id", {}

    def child_update(*_args):
        current["child_entered"].set()
        current["observed_scope"] = (get_account_id(), get_region())
        return "child-id", {}

    def child_delete(*_args):
        current["child_entered"].set()
        current["observed_scope"] = (get_account_id(), get_region())

    monkeypatch.setattr(eks, "_handle_request_unlocked", slow_owner_dispatch)
    monkeypatch.setattr(cfn_provisioners, "_provision_resource", child_create)
    monkeypatch.setattr(cfn_provisioners, "_update_resource", child_update)
    monkeypatch.setattr(cfn_provisioners, "_delete_resource", child_delete)

    def nested_operation(operation, depth):
        if depth:
            return nested_operation(operation, depth - 1)
        if operation == "create":
            return cfn_provisioners._nested_stack_provision_child(
                "AWS::EKS::Cluster", "Child", {}, "nested"
            )
        if operation == "update":
            return cfn_provisioners._nested_stack_update_child(
                "AWS::EKS::Cluster",
                "child-id",
                {},
                {},
                "nested",
                "Child",
            )
        return cfn_provisioners._nested_stack_delete_child(
            "AWS::EKS::Cluster", "child-id", {}, "nested", "Child"
        )

    async def exercise():
        previous_scope = (get_account_id(), get_region())
        set_request_account_id("123456789012")
        set_request_region("us-west-2")
        try:
            for operation in ("create", "update", "delete"):
                current.update(
                    owner_started=threading.Event(),
                    release_owner=threading.Event(),
                    child_entered=threading.Event(),
                    observed_scope=None,
                )
                owner = asyncio.create_task(
                    eks.handle_request("GET", "/", {}, b"", {})
                )
                while not current["owner_started"].is_set():
                    await asyncio.sleep(0.001)

                nested = asyncio.create_task(
                    cfn_stacks._run_nested_stack_provisioner(
                        nested_operation, operation, 2
                    )
                )
                await asyncio.sleep(0.05)
                assert not current["child_entered"].is_set()

                current["release_owner"].set()
                assert (await owner)[0] == 200
                await nested
                assert current["child_entered"].is_set()
                assert current["observed_scope"] == (
                    "123456789012",
                    "us-west-2",
                )
        finally:
            set_request_account_id(previous_scope[0])
            set_request_region(previous_scope[1])

    fallback = threading.Timer(
        1,
        lambda: current.get("release_owner", threading.Event()).set(),
    )
    fallback.start()
    try:
        asyncio.run(exercise())
    finally:
        if "release_owner" in current:
            current["release_owner"].set()
        fallback.cancel()


def test_reset_skips_failed_serialized_lazy_module(monkeypatch):
    reset_called = threading.Event()
    real_resolve = ministack_app._resolve_loaded_service_module
    failed_rds = ministack_app._ErrorModule("rds", "optional dependency missing")

    def resolve_loaded(name):
        if name == "rds":
            return failed_rds
        return real_resolve(name)

    monkeypatch.setattr(
        ministack_app,
        "_resolve_loaded_service_module",
        resolve_loaded,
    )
    monkeypatch.setattr(ministack_app, "_reset_all_state", reset_called.set)
    monkeypatch.setattr(ministack_app, "_reset_locks", LoopLocal(asyncio.Lock))
    monkeypatch.setattr(
        ministack_app,
        "_ordinary_request_reset_barriers",
        LoopLocal(ministack_app._RequestResetBarrier),
    )
    monkeypatch.setattr(
        cfn_lifecycle,
        "_stack_task_lifecycles",
        LoopLocal(cfn_lifecycle._StackTaskLifecycle),
    )

    asyncio.run(ministack_app._reset_all_state_after_service_dispatch_drains())
    assert reset_called.is_set()


def test_reset_keeps_missing_healthy_admission_seam_loud(monkeypatch):
    real_resolve = ministack_app._resolve_loaded_service_module

    def resolve_loaded(name):
        if name == "rds":
            return object()
        return real_resolve(name)

    monkeypatch.setattr(
        ministack_app,
        "_resolve_loaded_service_module",
        resolve_loaded,
    )
    monkeypatch.setattr(ministack_app, "_reset_locks", LoopLocal(asyncio.Lock))
    monkeypatch.setattr(
        ministack_app,
        "_ordinary_request_reset_barriers",
        LoopLocal(ministack_app._RequestResetBarrier),
    )
    monkeypatch.setattr(
        cfn_lifecycle,
        "_stack_task_lifecycles",
        LoopLocal(cfn_lifecycle._StackTaskLifecycle),
    )

    with pytest.raises(AttributeError, match="_get_request_dispatch_lock"):
        asyncio.run(ministack_app._reset_all_state_after_service_dispatch_drains())


def test_cfn_untouched_reset_preserves_lazy_service_imports():
    script = """
import asyncio
import json
import sys

import ministack.app as app

asyncio.run(app._reset_all_state_after_service_dispatch_drains())
names = [
    "ministack.services.cloudformation.provisioners",
    "ministack.services.rds",
    "ministack.services.ecs",
    "ministack.services.eks",
    "ministack.services.opensearch",
]
unexpected = [name for name in names if name in sys.modules]
print(json.dumps(unexpected))
raise SystemExit(bool(unexpected))
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout.strip().splitlines()[-1]) == []


def test_first_touch_serialized_import_waits_for_reset_writer(monkeypatch):
    wipe_started = threading.Event()
    release_wipe = threading.Event()
    loaded = {}
    order = []

    def resolve_loaded(name):
        return loaded.get(name)

    def import_module(name):
        order.append(f"import-{name}")
        loaded[name] = object()
        return loaded[name]

    def slow_reset():
        order.append("wipe-start")
        wipe_started.set()
        release_wipe.wait(timeout=2)
        order.append("wipe-finish")

    monkeypatch.setattr(
        ministack_app,
        "_resolve_loaded_service_module",
        resolve_loaded,
    )
    monkeypatch.setattr(ministack_app, "_get_module", import_module)
    monkeypatch.setattr(ministack_app, "_reset_all_state", slow_reset)
    monkeypatch.setattr(
        ministack_app,
        "_ordinary_request_reset_barriers",
        LoopLocal(ministack_app._RequestResetBarrier),
    )
    monkeypatch.setattr(
        cfn_lifecycle,
        "_stack_task_lifecycles",
        LoopLocal(cfn_lifecycle._StackTaskLifecycle),
    )

    async def exercise():
        reset = asyncio.create_task(
            ministack_app._reset_all_state_after_service_dispatch_drains()
        )
        while not wipe_started.is_set():
            await asyncio.sleep(0.001)

        first_touch = asyncio.create_task(
            ministack_app._gate_first_touch_reset_admission_module("rds")
        )
        await asyncio.sleep(0.05)
        assert order == ["wipe-start"]

        release_wipe.set()
        await asyncio.gather(reset, first_touch)
        assert order == ["wipe-start", "wipe-finish", "import-rds"]

    fallback = threading.Timer(1, release_wipe.set)
    fallback.start()
    try:
        asyncio.run(exercise())
    finally:
        release_wipe.set()
        fallback.cancel()


@pytest.mark.parametrize(
    "service",
    sorted(ministack_app._RESET_ADMISSION_SERVICE_KEYS),
)
def test_first_touch_gate_maps_every_serialized_service_key(monkeypatch, service):
    imported = []

    monkeypatch.setattr(
        ministack_app,
        "_resolve_loaded_service_module",
        lambda _name: None,
    )
    monkeypatch.setattr(
        ministack_app,
        "_get_module",
        lambda name: imported.append(name),
    )
    monkeypatch.setattr(
        ministack_app,
        "_ordinary_request_reset_barriers",
        LoopLocal(ministack_app._RequestResetBarrier),
    )

    asyncio.run(ministack_app._gate_first_touch_reset_admission_module(service))

    assert imported == [ministack_app.SERVICE_REGISTRY[service]["module"]]


@pytest.mark.parametrize(
    "service",
    sorted(ministack_app._RESET_CALLBACK_SERVICE_KEYS),
)
def test_first_touch_gate_does_not_import_callback_services(monkeypatch, service):
    imported = []
    monkeypatch.setattr(
        ministack_app,
        "_resolve_loaded_service_module",
        lambda _name: None,
    )
    monkeypatch.setattr(
        ministack_app,
        "_get_module",
        lambda name: imported.append(name),
    )

    asyncio.run(ministack_app._gate_first_touch_reset_admission_module(service))

    assert imported == []


@pytest.mark.parametrize(
    "service",
    [rds, ecs, eks, elasticache, opensearch],
    ids=["rds", "ecs", "eks", "elasticache", "opensearch"],
)
def test_dispatch_lock_cache_does_not_retain_closed_event_loops(service):
    def contend_once():
        loop = asyncio.new_event_loop()

        async def exercise():
            lock = service._get_request_dispatch_lock()
            await lock.acquire()
            waiter = asyncio.create_task(lock.acquire())
            await asyncio.sleep(0)
            lock.release()
            await waiter
            lock.release()
            return weakref.ref(lock)

        try:
            lock_ref = loop.run_until_complete(exercise())
            return weakref.ref(loop), lock_ref
        finally:
            loop.close()

    refs = [contend_once() for _ in range(3)]
    gc.collect()
    assert all(loop_ref() is None for loop_ref, _lock_ref in refs)
    assert all(lock_ref() is None for _loop_ref, lock_ref in refs)


def test_reset_lock_is_recreated_for_each_event_loop(monkeypatch):
    monkeypatch.setattr(ministack_app, "_reset_locks", LoopLocal(asyncio.Lock))
    monkeypatch.setattr(
        ministack_app,
        "_ordinary_request_reset_barriers",
        LoopLocal(ministack_app._RequestResetBarrier),
    )

    async def exercise():
        reset_lock = ministack_app._get_reset_lock()
        await reset_lock.acquire()
        lock_waiter = asyncio.create_task(reset_lock.acquire())
        await asyncio.sleep(0)
        reset_lock.release()
        await lock_waiter
        reset_lock.release()

        barrier = ministack_app._get_ordinary_request_reset_barrier()
        await barrier.enter_request()
        reset_waiter = asyncio.create_task(barrier.enter_reset())
        await asyncio.sleep(0)
        barrier.leave_request()
        await reset_waiter
        barrier.leave_reset()

    asyncio.run(exercise())
    asyncio.run(exercise())


def test_cloudformation_task_lifecycle_is_recreated_for_each_event_loop(monkeypatch):
    monkeypatch.setattr(
        cfn_lifecycle,
        "_stack_task_lifecycles",
        LoopLocal(cfn_lifecycle._StackTaskLifecycle),
    )

    def exercise_once():
        async def exercise():
            lifecycle = cfn_lifecycle._get_stack_task_lifecycle()

            async def no_op():
                return None

            task = cfn_stacks._create_stack_task_in_region(no_op(), None)
            await task
            await asyncio.sleep(0)
            return weakref.ref(lifecycle)

        return asyncio.run(exercise())

    refs = [exercise_once() for _ in range(3)]
    gc.collect()
    assert all(ref() is None for ref in refs)


def test_stepfunctions_sync_sdk_dispatch_uses_rds_admission_router():
    async def exercise():
        loop_token = stepfunctions._execution_server_loop.set(
            asyncio.get_running_loop()
        )
        try:
            return await asyncio.to_thread(
                stepfunctions._dispatch_aws_sdk_query,
                stepfunctions._AWS_SDK_SERVICE_MAP["rds"],
                "rds",
                "DescribeDBClusters",
                {},
            )
        finally:
            stepfunctions._execution_server_loop.reset(loop_token)

    result = asyncio.run(exercise())
    assert isinstance(result["DbClusters"], list)


def test_eventbridge_direct_sfn_rds_execution_reaches_terminal_success():
    async def exercise():
        eventbridge.reset()
        stepfunctions.reset()
        try:
            state_machine_arn = await _create_rds_integration_state_machine(
                "event-loop-eb-direct"
            )
            _configure_eventbridge_sfn_target(
                state_machine_arn,
                rule_name="event-loop-eb-direct-rule",
                source="event-loop.direct",
            )
            response = eventbridge._put_events(
                {
                    "Entries": [
                        {
                            "Source": "event-loop.direct",
                            "DetailType": "Regression",
                            "Detail": "{}",
                            "EventBusName": "default",
                        }
                    ]
                }
            )
            assert response[0] == 200
            await _wait_for_state_machine_terminal_success(state_machine_arn)
        finally:
            eventbridge.reset()
            stepfunctions.reset()

    asyncio.run(exercise())


def test_eventbridge_scheduled_sfn_rds_execution_reaches_terminal_success():
    async def exercise():
        eventbridge.reset()
        stepfunctions.reset()
        try:
            state_machine_arn = await _create_rds_integration_state_machine(
                "event-loop-eb-scheduled"
            )
            rule_name = "event-loop-eb-scheduled-rule"
            _configure_eventbridge_sfn_target(
                state_machine_arn,
                rule_name=rule_name,
            )
            eventbridge._rule_last_fired[
                (get_account_id(), get_region(), eventbridge._rule_key(rule_name, "default"))
            ] = 0
            await asyncio.to_thread(
                eventbridge._tick_scheduled_rules,
                asyncio.get_running_loop(),
            )
            await _wait_for_state_machine_terminal_success(state_machine_arn)
        finally:
            eventbridge.reset()
            stepfunctions.reset()

    asyncio.run(exercise())


def test_eventbridge_replay_sfn_rds_execution_reaches_terminal_success():
    async def exercise():
        eventbridge.reset()
        stepfunctions.reset()
        try:
            eventbridge._ensure_default_bus()
            bus_arn = (
                f"arn:aws:events:{get_region()}:{get_account_id()}:event-bus/default"
            )
            archive_response = eventbridge._create_archive(
                {
                    "ArchiveName": "event-loop-eb-replay-archive",
                    "EventSourceArn": bus_arn,
                }
            )
            archive_arn = json.loads(archive_response[2])["ArchiveArn"]
            eventbridge._put_events(
                {
                    "Entries": [
                        {
                            "Source": "event-loop.replay",
                            "DetailType": "Regression",
                            "Detail": "{}",
                            "EventBusName": "default",
                        }
                    ]
                }
            )

            state_machine_arn = await _create_rds_integration_state_machine(
                "event-loop-eb-replay"
            )
            _configure_eventbridge_sfn_target(
                state_machine_arn,
                rule_name="event-loop-eb-replay-rule",
                source="event-loop.replay",
            )
            replay_response = eventbridge._start_replay(
                {
                    "ReplayName": "event-loop-eb-replay",
                    "EventSourceArn": archive_arn,
                    "EventStartTime": 0,
                    "EventEndTime": time.time() + 60,
                    "Destination": {"Arn": bus_arn},
                }
            )
            assert replay_response[0] == 200
            await _wait_for_state_machine_terminal_success(state_machine_arn)
            deadline = asyncio.get_running_loop().time() + 1
            while asyncio.get_running_loop().time() < deadline:
                if eventbridge._replays["event-loop-eb-replay"]["State"] == "COMPLETED":
                    break
                await asyncio.sleep(0.01)
            assert eventbridge._replays["event-loop-eb-replay"]["State"] == "COMPLETED"
        finally:
            eventbridge.reset()
            stepfunctions.reset()

    asyncio.run(exercise())


def test_s3_eventbridge_sfn_rds_execution_reaches_terminal_success():
    async def exercise():
        eventbridge.reset()
        s3.reset()
        stepfunctions.reset()
        bucket_name = "event-loop-s3-eventbridge"
        try:
            state_machine_arn = await _create_rds_integration_state_machine(
                "event-loop-s3-eventbridge"
            )
            _configure_eventbridge_sfn_target(
                state_machine_arn,
                rule_name="event-loop-s3-eventbridge-rule",
                source="aws.s3",
            )

            create_response = await s3.handle_request(
                "PUT", f"/{bucket_name}", {}, b"", {}
            )
            assert create_response[0] == 200
            notification_xml = (
                b'<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                b"<EventBridgeConfiguration/>"
                b"</NotificationConfiguration>"
            )
            notification_response = await s3.handle_request(
                "PUT",
                f"/{bucket_name}",
                {},
                notification_xml,
                {"notification": ""},
            )
            assert notification_response[0] == 200

            put_response = await s3.handle_request(
                "PUT",
                f"/{bucket_name}/object.txt",
                {},
                b"payload",
                {},
            )
            assert put_response[0] == 200
            await _wait_for_state_machine_terminal_success(state_machine_arn)
        finally:
            eventbridge.reset()
            s3.reset()
            stepfunctions.reset()

    asyncio.run(exercise())


def test_s3_eventbridge_inherits_loop_from_sfn_execution_worker():
    async def exercise():
        eventbridge.reset()
        s3.reset()
        stepfunctions.reset()
        bucket_name = "event-loop-s3-sfn-worker"
        try:
            assert (
                await s3.handle_request("PUT", f"/{bucket_name}", {}, b"", {})
            )[0] == 200
            assert (
                await s3.handle_request(
                    "PUT",
                    f"/{bucket_name}/source.txt",
                    {},
                    b"payload",
                    {},
                )
            )[0] == 200

            target_state_machine_arn = await _create_rds_integration_state_machine(
                "event-loop-s3-sfn-worker-target"
            )
            _configure_eventbridge_sfn_target(
                target_state_machine_arn,
                rule_name="event-loop-s3-sfn-worker-rule",
                source="aws.s3",
            )
            notification_xml = (
                b'<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                b"<EventBridgeConfiguration/>"
                b"</NotificationConfiguration>"
            )
            assert (
                await s3.handle_request(
                    "PUT",
                    f"/{bucket_name}",
                    {},
                    notification_xml,
                    {"notification": ""},
                )
            )[0] == 200

            source_state_machine = await _call_stepfunctions_action(
                "CreateStateMachine",
                {
                    "name": "event-loop-s3-sfn-worker-source",
                    "roleArn": "arn:aws:iam::000000000000:role/sfn-role",
                    "definition": json.dumps(
                        {
                            "StartAt": "Copy",
                            "States": {
                                "Copy": {
                                    "Type": "Task",
                                    "Resource": (
                                        "arn:aws:states:::aws-sdk:s3:copyObject"
                                    ),
                                    "Parameters": {
                                        "Bucket": bucket_name,
                                        "Key": "copied.txt",
                                        "CopySource": f"{bucket_name}/source.txt",
                                    },
                                    "End": True,
                                }
                            },
                        }
                    ),
                },
            )
            assert source_state_machine[0] == 200
            source_state_machine_arn = json.loads(source_state_machine[2])[
                "stateMachineArn"
            ]
            start_response = await _call_stepfunctions_action(
                "StartExecution",
                {"stateMachineArn": source_state_machine_arn, "input": "{}"},
            )
            assert start_response[0] == 200
            await _wait_for_state_machine_terminal_success(source_state_machine_arn)
            await _wait_for_state_machine_terminal_success(target_state_machine_arn)
        finally:
            eventbridge.reset()
            s3.reset()
            stepfunctions.reset()

    asyncio.run(exercise())


def test_eventbridge_dispatch_requires_explicit_server_loop():
    with pytest.raises(TypeError, match="server_loop"):
        eventbridge._dispatch_event({})


def test_eventbridge_scheduler_uses_fresh_loop_after_lifecycle_restart(monkeypatch):
    captured_loops = []

    def capture_loop(server_loop, stop_event):
        captured_loops.append(server_loop)
        stop_event.wait(timeout=1)

    eventbridge.stop_scheduler()
    monkeypatch.setattr(eventbridge, "_scheduler_loop", capture_loop)

    def run_lifecycle():
        async def exercise():
            loop = asyncio.get_running_loop()
            eventbridge.start_scheduler()
            deadline = loop.time() + 1
            while len(captured_loops) < expected_count:
                if loop.time() >= deadline:
                    pytest.fail("scheduler thread did not capture its lifecycle loop")
                await asyncio.sleep(0.001)
            await asyncio.to_thread(eventbridge.stop_scheduler)
            return loop

        return asyncio.run(exercise())

    expected_count = 1
    first_loop = run_lifecycle()
    expected_count = 2
    second_loop = run_lifecycle()

    assert first_loop is not second_loop
    assert first_loop.is_closed()
    assert second_loop.is_closed()
    assert captured_loops == [first_loop, second_loop]


def test_scheduler_targeting_rds_state_machine_reaches_terminal_success(monkeypatch):
    async def exercise():
        scheduler_service.stop_scheduler()
        scheduler_service.reset()
        scheduler_service._schedule_last_fired.clear()
        eventbridge.reset()
        stepfunctions.reset()
        previous_account = get_account_id()
        previous_region = get_region()
        set_request_account_id("000000000000")
        set_request_region("us-east-1")
        try:
            state_machine_arn = await _create_rds_integration_state_machine(
                "event-loop-scheduler-rds"
            )
            name = "event-loop-scheduler-rds"
            key = f"default/{name}"
            scheduler_service._schedules[key] = {
                "Arn": (
                    "arn:aws:scheduler:us-east-1:000000000000:"
                    f"schedule/{key}"
                ),
                "Name": name,
                "GroupName": "default",
                "ScheduleExpression": "at(1970-01-01T00:00:01)",
                "Target": {"Arn": state_machine_arn},
                "State": "ENABLED",
                "ActionAfterCompletion": "NONE",
                "CreationDate": 1,
            }
            monkeypatch.setattr(
                scheduler_service,
                "_SCHEDULE_TICK_INTERVAL",
                0.001,
            )
            scheduler_service.start_scheduler()
            await _wait_for_state_machine_terminal_success(state_machine_arn)
        finally:
            await asyncio.to_thread(scheduler_service.stop_scheduler)
            scheduler_service.reset()
            scheduler_service._schedule_last_fired.clear()
            eventbridge.reset()
            stepfunctions.reset()
            set_request_account_id(previous_account)
            set_request_region(previous_region)

    asyncio.run(exercise())


def test_scheduler_uses_fresh_loop_after_lifecycle_restart(monkeypatch):
    captured_loops = []

    def capture_loop(server_loop, stop_event):
        captured_loops.append(server_loop)
        stop_event.wait(timeout=1)

    scheduler_service.stop_scheduler()
    monkeypatch.setattr(scheduler_service, "_ticker_loop", capture_loop)

    def run_lifecycle():
        async def exercise():
            loop = asyncio.get_running_loop()
            scheduler_service.start_scheduler()
            deadline = loop.time() + 1
            while len(captured_loops) < expected_count:
                if loop.time() >= deadline:
                    pytest.fail("scheduler thread did not capture its lifecycle loop")
                await asyncio.sleep(0.001)
            await asyncio.to_thread(scheduler_service.stop_scheduler)
            return loop

        return asyncio.run(exercise())

    expected_count = 1
    first_loop = run_lifecycle()
    expected_count = 2
    second_loop = run_lifecycle()

    assert first_loop is not second_loop
    assert first_loop.is_closed()
    assert second_loop.is_closed()
    assert captured_loops == [first_loop, second_loop]


@pytest.mark.parametrize("loop_state", ["missing", "closed"])
def test_stepfunctions_non_http_loop_backstop_is_states_runtime(loop_state):
    server_loop = None
    if loop_state == "closed":
        server_loop = asyncio.new_event_loop()
        server_loop.close()

    loop_token = stepfunctions._execution_server_loop.set(server_loop)
    try:
        with pytest.raises(stepfunctions._ExecutionError, match="server loop") as exc:
            stepfunctions._drive_service_handler_sync(
                "rds",
                rds.handle_request,
                "POST",
                "/",
                {},
                b"",
                {},
            )
        assert exc.value.error == "States.Runtime"
    finally:
        stepfunctions._execution_server_loop.reset(loop_token)


def test_stepfunctions_sync_rds_waits_for_http_rds_admission(monkeypatch):
    http_started = threading.Event()
    sfn_started = threading.Event()
    release_http = threading.Event()
    state_lock = threading.Lock()
    call_count = 0

    def controlled_dispatch(*_args):
        nonlocal call_count
        with state_lock:
            call_count += 1
            this_call = call_count
        if this_call == 1:
            http_started.set()
            release_http.wait(timeout=2)
        else:
            sfn_started.set()
        return 200, {}, json.dumps({"call": this_call}).encode()

    def sync_execution(_data):
        return stepfunctions._drive_service_handler_sync(
            "rds",
            rds.handle_request,
            "POST",
            "/",
            {},
            b"",
            {},
        )

    monkeypatch.setattr(rds, "_handle_request_unlocked", controlled_dispatch)
    monkeypatch.setattr(stepfunctions, "_start_sync_execution", sync_execution)

    async def exercise():
        http_request = asyncio.create_task(
            rds.handle_request("POST", "/", {}, b"", {})
        )
        while not http_started.is_set():
            await asyncio.sleep(0.001)

        sync_request = asyncio.create_task(
            _call_stepfunctions_action("StartSyncExecution")
        )
        await asyncio.sleep(0.05)
        assert not sfn_started.is_set()

        release_http.set()
        http_result, sfn_result = await asyncio.gather(http_request, sync_request)
        assert http_result[0] == 200
        assert sfn_result[0] == 200
        assert sfn_started.is_set()

    fallback = threading.Timer(1, release_http.set)
    fallback.start()
    try:
        asyncio.run(exercise())
    finally:
        release_http.set()
        fallback.cancel()


def test_stepfunctions_sync_executions_do_not_consume_service_worker_pool(
    monkeypatch,
):
    def fast_dispatch(*_args):
        time.sleep(0.01)
        return 200, {}, b"{}"

    def sync_execution(_data):
        return stepfunctions._drive_service_handler_sync(
            "rds",
            rds.handle_request,
            "POST",
            "/",
            {},
            b"",
            {},
        )

    monkeypatch.setattr(rds, "_handle_request_unlocked", fast_dispatch)
    monkeypatch.setattr(stepfunctions, "_start_sync_execution", sync_execution)

    async def exercise():
        # If the four outer sync executions used this same four-slot pool,
        # every worker would block on an inner RDS offload and none could run.
        asyncio.get_running_loop().set_default_executor(
            ThreadPoolExecutor(max_workers=4)
        )
        requests = [
            asyncio.create_task(_call_stepfunctions_action("StartSyncExecution"))
            for _ in range(4)
        ]
        results = await asyncio.wait_for(asyncio.gather(*requests), timeout=2)
        assert all(result[0] == 200 for result in results)

    asyncio.run(exercise())


def test_cancelled_stepfunctions_sync_action_waits_for_dedicated_thread(
    monkeypatch,
):
    action_started = threading.Event()
    release_action = threading.Event()
    action_finished = threading.Event()

    def sync_execution(_data):
        action_started.set()
        release_action.wait(timeout=2)
        action_finished.set()
        return 200, {}, b"{}"

    monkeypatch.setattr(stepfunctions, "_start_sync_execution", sync_execution)

    async def exercise():
        request = asyncio.create_task(
            _call_stepfunctions_action("StartSyncExecution")
        )
        while not action_started.is_set():
            await asyncio.sleep(0.001)

        request.cancel()
        await asyncio.sleep(0.01)
        request.cancel()
        await asyncio.sleep(0.05)
        assert not request.done()
        assert not action_finished.is_set()

        release_action.set()
        result = await asyncio.gather(request, return_exceptions=True)
        assert isinstance(result[0], asyncio.CancelledError)
        assert action_finished.is_set()

    fallback = threading.Timer(1, release_action.set)
    fallback.start()
    try:
        asyncio.run(exercise())
    finally:
        release_action.set()
        fallback.cancel()


def test_stepfunctions_marshal_fails_fast_on_target_loop(monkeypatch):
    monkeypatch.setattr(rds, "_handle_request_unlocked", lambda *_args: (200, {}, b"{}"))

    async def exercise():
        loop_token = stepfunctions._execution_server_loop.set(
            asyncio.get_running_loop()
        )
        try:
            with pytest.raises(
                stepfunctions._ExecutionError,
                match="cannot synchronously marshal from its target event loop",
            ):
                stepfunctions._drive_service_handler_sync(
                    "rds",
                    rds.handle_request,
                    "POST",
                    "/",
                    {},
                    b"",
                    {},
                )
        finally:
            stepfunctions._execution_server_loop.reset(loop_token)

    asyncio.run(exercise())


def test_stepfunctions_marshaled_rds_waits_for_reset_and_sees_fresh_state(
    monkeypatch,
):
    reset_started = threading.Event()
    release_reset = threading.Event()
    integration_started = threading.Event()
    state = {"generation": "old"}

    def reset_all_state():
        state["generation"] = "fresh"
        reset_started.set()
        release_reset.wait(timeout=2)

    def read_state(*_args):
        integration_started.set()
        return 200, {}, json.dumps(state).encode()

    def sync_execution(_data):
        return stepfunctions._drive_service_handler_sync(
            "rds",
            rds.handle_request,
            "POST",
            "/",
            {},
            b"",
            {},
        )

    monkeypatch.setattr(ministack_app, "_reset_all_state", reset_all_state)
    monkeypatch.setattr(ministack_app, "_reset_locks", LoopLocal(asyncio.Lock))
    monkeypatch.setattr(
        ministack_app,
        "_ordinary_request_reset_barriers",
        LoopLocal(ministack_app._RequestResetBarrier),
    )
    monkeypatch.setattr(
        cfn_lifecycle,
        "_stack_task_lifecycles",
        LoopLocal(cfn_lifecycle._StackTaskLifecycle),
    )
    monkeypatch.setattr(rds, "_handle_request_unlocked", read_state)
    monkeypatch.setattr(stepfunctions, "_start_sync_execution", sync_execution)

    async def exercise():
        reset = asyncio.create_task(
            ministack_app._reset_all_state_after_service_dispatch_drains()
        )
        while not reset_started.is_set():
            await asyncio.sleep(0.001)

        integration = asyncio.create_task(
            _call_stepfunctions_action("StartSyncExecution")
        )
        await asyncio.sleep(0.05)
        assert not integration_started.is_set()

        release_reset.set()
        _, result = await asyncio.gather(reset, integration)
        assert integration_started.is_set()
        assert json.loads(result[2]) == {"generation": "fresh"}

    fallback = threading.Timer(1, release_reset.set)
    fallback.start()
    try:
        asyncio.run(exercise())
    finally:
        release_reset.set()
        fallback.cancel()


def test_eks_api_and_cloudformation_same_name_create_are_serialized(monkeypatch):
    cluster_name = "event-loop-cfn-eks"
    first_in_create = threading.Event()
    release_first = threading.Event()
    backend_started = threading.Event()
    state_lock = threading.Lock()
    next_port_calls = 0
    backend_starts = 0

    def controlled_next_port():
        nonlocal next_port_calls
        with state_lock:
            next_port_calls += 1
            this_call = next_port_calls
        if this_call == 1:
            first_in_create.set()
            release_first.wait(timeout=2)
        return 18000 + this_call

    def no_docker():
        nonlocal backend_starts
        with state_lock:
            backend_starts += 1
        backend_started.set()
        return None

    monkeypatch.setattr(eks, "_next_port", controlled_next_port)
    monkeypatch.setattr(eks, "_get_docker", no_docker)

    async def exercise():
        api_create = asyncio.create_task(
            eks.handle_request(
                "POST",
                "/clusters",
                {},
                json.dumps(
                    {
                        "name": cluster_name,
                        "roleArn": "arn:aws:iam::000000000000:role/api-role",
                    }
                ).encode(),
                {},
            )
        )
        while not first_in_create.is_set():
            await asyncio.sleep(0.001)

        cfn_create = asyncio.create_task(
            cfn_stacks._run_locked_standard_provisioner(
                "AWS::EKS::Cluster",
                cfn_stacks._provision_resource,
                "AWS::EKS::Cluster",
                "Cluster",
                {
                    "Name": cluster_name,
                    "RoleArn": "arn:aws:iam::000000000000:role/cfn-role",
                },
                "event-loop-stack",
            )
        )
        await asyncio.sleep(0.05)
        with state_lock:
            assert next_port_calls == 1

        release_first.set()
        api_result, cfn_result = await asyncio.gather(api_create, cfn_create)
        assert api_result[0] == 200
        assert cfn_result[0] == cluster_name

    try:
        asyncio.run(exercise())
        assert backend_started.wait(timeout=1)
        with state_lock:
            assert next_port_calls == 1
            assert backend_starts == 1
        assert eks._clusters[cluster_name]["roleArn"].endswith(":role/api-role")
    finally:
        release_first.set()
        cluster = eks._clusters.pop(cluster_name, None)
        if cluster:
            eks._tags.pop(cluster.get("arn", ""), None)


def test_serial_waiters_do_not_exhaust_shared_worker_pool(monkeypatch):
    first_started = threading.Event()
    release = threading.Event()

    def slow_dispatch(*_args):
        first_started.set()
        release.wait(timeout=2)
        return 200, {}, b"rds"

    def fast_dispatch(*_args):
        return 200, {}, b"ecs"

    monkeypatch.setattr(rds, "_handle_request_unlocked", slow_dispatch)
    monkeypatch.setattr(ecs, "_handle_request_unlocked", fast_dispatch)

    async def exercise():
        slow_tasks = [
            asyncio.create_task(rds.handle_request("GET", "/", {}, b"", {}))
            for _ in range(40)
        ]
        while not first_started.is_set():
            await asyncio.sleep(0.001)
        # Give queued RDS requests time to reach admission. Acquiring the lock
        # in workers would fill Python's default executor (capped at 32).
        await asyncio.sleep(0.05)

        fast_task = asyncio.create_task(
            ecs.handle_request("GET", "/", {}, b"", {})
        )
        try:
            result = await asyncio.wait_for(asyncio.shield(fast_task), timeout=0.2)
            assert result == (200, {}, b"ecs")
        finally:
            release.set()
            await asyncio.gather(*slow_tasks, fast_task, return_exceptions=True)

    fallback = threading.Timer(1, release.set)
    fallback.start()
    try:
        asyncio.run(exercise())
    finally:
        release.set()
        fallback.cancel()


def test_admin_reset_waits_for_in_flight_request(monkeypatch):
    request_started = threading.Event()
    reset_called = threading.Event()
    release_request = threading.Event()

    def slow_dispatch(*_args):
        request_started.set()
        release_request.wait(timeout=2)
        return 200, {}, b"request"

    async def route_to_rds(*_args):
        return await rds.handle_request("GET", "/", {}, b"", {})

    def reset_all_state():
        reset_called.set()

    monkeypatch.setattr(rds, "_handle_request_unlocked", slow_dispatch)
    monkeypatch.setattr(ministack_app, "_dispatch_service_request", route_to_rds)
    monkeypatch.setattr(ministack_app, "_reset_all_state", reset_all_state)
    monkeypatch.setattr(ministack_app, "_reset_locks", LoopLocal(asyncio.Lock))
    monkeypatch.setattr(
        ministack_app,
        "_ordinary_request_reset_barriers",
        LoopLocal(ministack_app._RequestResetBarrier),
    )
    monkeypatch.setattr(
        cfn_lifecycle,
        "_stack_task_lifecycles",
        LoopLocal(cfn_lifecycle._StackTaskLifecycle),
    )

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def exercise():
        request_messages = []
        reset_messages = []

        async def send_request(message):
            request_messages.append(message)

        async def send_reset(message):
            reset_messages.append(message)

        request_scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "query_string": b"",
            "headers": [
                (b"authorization", b"AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/rds/aws4_request"),
            ],
        }
        reset_scope = {
            "type": "http",
            "method": "POST",
            "path": "/_ministack/reset",
            "query_string": b"",
            "headers": [],
        }

        request_task = asyncio.create_task(
            ministack_app.app(request_scope, receive, send_request)
        )
        while not request_started.is_set():
            await asyncio.sleep(0.001)

        reset_task = asyncio.create_task(
            ministack_app.app(reset_scope, receive, send_reset)
        )
        await asyncio.sleep(0.05)
        assert not reset_called.is_set()

        release_request.set()
        await asyncio.gather(request_task, reset_task)
        assert reset_called.is_set()
        assert request_messages[-1]["body"] == b"request"
        assert json.loads(reset_messages[-1]["body"]) == {"reset": "ok"}

    fallback = threading.Timer(1, release_request.set)
    fallback.start()
    try:
        asyncio.run(exercise())
    finally:
        release_request.set()
        fallback.cancel()


def test_admin_reset_waits_for_ordinary_request(monkeypatch):
    request_started = asyncio.Event()
    release_request = asyncio.Event()
    reset_called = asyncio.Event()

    async def slow_dispatch(*_args):
        request_started.set()
        await release_request.wait()
        return 200, {}, b"request"

    def reset_all_state():
        reset_called.set()

    monkeypatch.setattr(ministack_app, "_dispatch_service_request", slow_dispatch)
    monkeypatch.setattr(ministack_app, "_reset_all_state", reset_all_state)
    monkeypatch.setattr(ministack_app, "_reset_locks", LoopLocal(asyncio.Lock))

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(_message):
        pass

    request_scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [
            (b"x-amz-target", b"AmazonSQS.CreateQueue"),
            (b"authorization", b"AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/sqs/aws4_request"),
        ],
    }
    reset_scope = {
        "type": "http",
        "method": "POST",
        "path": "/_ministack/reset",
        "query_string": b"",
        "headers": [],
    }

    async def exercise():
        request = asyncio.create_task(
            ministack_app.app(request_scope, receive, send)
        )
        await request_started.wait()
        reset = asyncio.create_task(
            ministack_app.app(reset_scope, receive, send)
        )
        await asyncio.sleep(0.05)
        assert not reset_called.is_set()

        release_request.set()
        await asyncio.gather(request, reset)
        assert reset_called.is_set()

    asyncio.run(exercise())


def test_admin_reset_waits_for_lockless_tagging_request(monkeypatch):
    request_started = asyncio.Event()
    release_request = asyncio.Event()
    reset_called = asyncio.Event()

    async def slow_dispatch(*_args):
        request_started.set()
        await release_request.wait()
        return 200, {}, b"request"

    def reset_all_state():
        reset_called.set()

    body = json.dumps(
        {
            "ResourceARNList": ["arn:aws:s3:::event-loop-reset-tags"],
            "Tags": {"source": "rgta"},
        }
    ).encode()
    monkeypatch.setattr(ministack_app, "_dispatch_service_request", slow_dispatch)
    monkeypatch.setattr(ministack_app, "_reset_all_state", reset_all_state)
    monkeypatch.setattr(ministack_app, "_reset_locks", LoopLocal(asyncio.Lock))

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(_message):
        pass

    request_scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [
            (
                b"x-amz-target",
                b"ResourceGroupsTaggingAPI_20170126.TagResources",
            ),
            (
                b"authorization",
                b"AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/tagging/aws4_request",
            ),
        ],
    }
    reset_scope = {
        "type": "http",
        "method": "POST",
        "path": "/_ministack/reset",
        "query_string": b"",
        "headers": [],
    }

    async def exercise():
        request = asyncio.create_task(
            ministack_app.app(request_scope, receive, send)
        )
        await request_started.wait()
        reset = asyncio.create_task(
            ministack_app.app(reset_scope, receive, send)
        )
        await asyncio.sleep(0.05)
        assert not reset_called.is_set()

        release_request.set()
        await asyncio.gather(request, reset)
        assert reset_called.is_set()

    asyncio.run(exercise())


def test_admin_reset_does_not_deadlock_nested_lambda_ministack_request(monkeypatch):
    reset_admitted = asyncio.Event()
    nested_finished = asyncio.Event()
    reset_lock = asyncio.Lock()
    dispatch_count = 0

    class _SignalingResetLock:
        async def __aenter__(self):
            await reset_lock.acquire()
            reset_admitted.set()

        async def __aexit__(self, *_exc_info):
            reset_lock.release()

    monkeypatch.setattr(
        ministack_app,
        "_get_reset_lock",
        lambda: _SignalingResetLock(),
    )
    monkeypatch.setattr(ministack_app, "_reset_all_state", lambda: None)
    monkeypatch.setattr(
        ministack_app,
        "_ordinary_request_reset_barriers",
        LoopLocal(ministack_app._RequestResetBarrier),
    )
    monkeypatch.setattr(
        cfn_lifecycle,
        "_stack_task_lifecycles",
        LoopLocal(cfn_lifecycle._StackTaskLifecycle),
    )

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(_message):
        pass

    def scope(path, service):
        return {
            "type": "http",
            "method": "GET" if path != "/_ministack/reset" else "POST",
            "path": path,
            "query_string": b"",
            "headers": [] if service is None else [
                (
                    b"authorization",
                    (
                        "AWS4-HMAC-SHA256 Credential=test/20260101/"
                        f"us-east-1/{service}/aws4_request"
                    ).encode(),
                )
            ],
        }

    async def nested_dispatch(*_args):
        nonlocal dispatch_count
        dispatch_count += 1
        if dispatch_count == 1:
            await reset_admitted.wait()
            await ministack_app.app(scope("/nested", "dynamodb"), receive, send)
            nested_finished.set()
            return 200, {}, b"outer"
        return 200, {}, b"nested"

    monkeypatch.setattr(
        ministack_app,
        "_dispatch_service_request",
        nested_dispatch,
    )

    async def exercise():
        outer = asyncio.create_task(
            ministack_app.app(
                scope("/2015-03-31/functions/outer/invocations", "lambda"),
                receive,
                send,
            )
        )
        await asyncio.sleep(0)
        reset = asyncio.create_task(
            ministack_app.app(scope("/_ministack/reset", None), receive, send)
        )
        await asyncio.wait_for(asyncio.gather(outer, reset), timeout=1)
        assert nested_finished.is_set()
        assert dispatch_count == 2

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("host", "path"),
    [
        (
            "12345678-1234-1234-1234-123456789012.lambda-url.us-east-1.localhost",
            "/outer",
        ),
        ("localhost", "/_aws/lambda-url/12345678-1234-1234-1234-123456789012/outer"),
    ],
    ids=["host", "path"],
)
def test_admin_reset_does_not_deadlock_nested_function_url_request(
    monkeypatch,
    host,
    path,
):
    reset_admitted = asyncio.Event()
    nested_finished = asyncio.Event()
    reset_lock = asyncio.Lock()
    nested_dispatches = 0

    class _SignalingResetLock:
        async def __aenter__(self):
            await reset_lock.acquire()
            reset_admitted.set()

        async def __aexit__(self, *_exc_info):
            reset_lock.release()

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(_message):
        pass

    def scope(request_path, service=None, request_host="localhost"):
        headers = [(b"host", request_host.encode())]
        if service is not None:
            headers.append(
                (
                    b"authorization",
                    (
                        "AWS4-HMAC-SHA256 Credential=test/20260101/"
                        f"us-east-1/{service}/aws4_request"
                    ).encode(),
                )
            )
        return {
            "type": "http",
            "method": "POST" if request_path == "/_ministack/reset" else "GET",
            "path": request_path,
            "query_string": b"",
            "headers": headers,
        }

    class _FunctionUrlModule:
        async def handle_function_url_request(self, *_args):
            await reset_admitted.wait()
            await ministack_app.app(
                scope("/nested", "dynamodb"),
                receive,
                send,
            )
            nested_finished.set()
            return 200, {}, b"outer"

    async def nested_dispatch(*_args):
        nonlocal nested_dispatches
        nested_dispatches += 1
        return 200, {}, b"nested"

    monkeypatch.setattr(
        ministack_app,
        "_get_reset_lock",
        lambda: _SignalingResetLock(),
    )
    monkeypatch.setattr(ministack_app, "_reset_all_state", lambda: None)
    monkeypatch.setattr(
        ministack_app,
        "_ordinary_request_reset_barriers",
        LoopLocal(ministack_app._RequestResetBarrier),
    )
    monkeypatch.setattr(
        cfn_lifecycle,
        "_stack_task_lifecycles",
        LoopLocal(cfn_lifecycle._StackTaskLifecycle),
    )
    monkeypatch.setitem(
        ministack_app._loaded_modules,
        "lambda_svc",
        _FunctionUrlModule(),
    )
    monkeypatch.setattr(
        ministack_app,
        "_dispatch_service_request",
        nested_dispatch,
    )

    async def exercise():
        outer = asyncio.create_task(
            ministack_app.app(scope(path, request_host=host), receive, send)
        )
        await asyncio.sleep(0)
        reset = asyncio.create_task(
            ministack_app.app(scope("/_ministack/reset"), receive, send)
        )
        await asyncio.wait_for(asyncio.gather(outer, reset), timeout=1)
        assert nested_finished.is_set()
        assert nested_dispatches == 1

    asyncio.run(exercise())


def test_admin_reset_quiesces_multi_resource_cloudformation_task(monkeypatch):
    first_started = threading.Event()
    release_first = threading.Event()
    reset_called = threading.Event()
    orphan_created = threading.Event()
    resources = []

    def first_provisioner():
        first_started.set()
        release_first.wait(timeout=2)
        return "first", {}

    def second_provisioner():
        orphan_created.set()
        return "second", {}

    async def deploy_stack():
        await cfn_stacks._run_locked_standard_provisioner(
            "AWS::EKS::Cluster", first_provisioner
        )
        resources.append("first")
        await cfn_stacks._run_locked_standard_provisioner(
            "AWS::EKS::Cluster", second_provisioner
        )
        resources.append("second")

    def reset_all_state():
        resources.clear()
        reset_called.set()

    monkeypatch.setattr(ministack_app, "_reset_all_state", reset_all_state)

    async def exercise():
        existing_tasks = asyncio.all_tasks()
        task = cfn_stacks._create_stack_task_in_region(deploy_stack(), None)
        if task is None:
            # Pre-fix heads did not return the untracked task from the spawn
            # seam. Recover it so the same regression exercises behavior.
            await asyncio.sleep(0)
            spawned = asyncio.all_tasks() - existing_tasks
            task = next(
                candidate
                for candidate in spawned
                if candidate.get_coro().__qualname__.endswith("deploy_stack")
            )
        while not first_started.is_set():
            await asyncio.sleep(0.001)

        reset = asyncio.create_task(
            ministack_app._reset_all_state_after_service_dispatch_drains()
        )
        await asyncio.sleep(0.05)
        assert not reset_called.is_set()

        release_first.set()
        await reset
        result = await asyncio.gather(task, return_exceptions=True)
        assert reset_called.is_set()
        assert not orphan_created.is_set()
        assert resources == []
        assert isinstance(result[0], asyncio.CancelledError)

    fallback = threading.Timer(1, release_first.set)
    fallback.start()
    try:
        asyncio.run(exercise())
    finally:
        release_first.set()
        fallback.cancel()


def test_cancelled_dispatch_holds_admission_and_reset_until_worker_finishes(
    monkeypatch,
):
    first_started = threading.Event()
    second_started = threading.Event()
    reset_called = threading.Event()
    release_first = threading.Event()
    state_lock = threading.Lock()
    overlaps = []
    active_workers = 0
    call_count = 0

    def dispatch(*_args):
        nonlocal active_workers, call_count
        with state_lock:
            call_count += 1
            this_call = call_count
            if active_workers:
                overlaps.append(f"request-{this_call}")
            active_workers += 1
        try:
            if this_call == 1:
                first_started.set()
                release_first.wait(timeout=2)
            else:
                second_started.set()
            return 200, {}, f"request-{this_call}".encode()
        finally:
            with state_lock:
                active_workers -= 1

    async def route_to_rds(*_args):
        return await rds.handle_request("GET", "/", {}, b"", {})

    def reset_all_state():
        with state_lock:
            if active_workers:
                overlaps.append("reset")
        reset_called.set()

    monkeypatch.setattr(rds, "_handle_request_unlocked", dispatch)
    monkeypatch.setattr(ministack_app, "_dispatch_service_request", route_to_rds)
    monkeypatch.setattr(ministack_app, "_reset_all_state", reset_all_state)
    monkeypatch.setattr(ministack_app, "_reset_locks", LoopLocal(asyncio.Lock))

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(_message):
        pass

    request_scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "query_string": b"",
        "headers": [],
    }
    reset_scope = {
        "type": "http",
        "method": "POST",
        "path": "/_ministack/reset",
        "query_string": b"",
        "headers": [],
    }

    async def exercise():
        first = asyncio.create_task(
            ministack_app.app(request_scope, receive, send)
        )
        while not first_started.is_set():
            await asyncio.sleep(0.001)

        first.cancel()
        await asyncio.sleep(0.01)
        # A repeated disconnect/cancellation must not cut short worker cleanup.
        first.cancel()

        second = asyncio.create_task(
            ministack_app.app(request_scope, receive, send)
        )
        await asyncio.sleep(0.01)
        reset = asyncio.create_task(
            ministack_app.app(reset_scope, receive, send)
        )
        await asyncio.sleep(0.05)

        assert not second_started.is_set()
        assert not reset_called.is_set()
        assert overlaps == []

        release_first.set()
        results = await asyncio.gather(first, second, reset, return_exceptions=True)
        assert isinstance(results[0], asyncio.CancelledError)
        assert second_started.is_set()
        assert reset_called.is_set()
        assert overlaps == []

    fallback = threading.Timer(1, release_first.set)
    fallback.start()
    try:
        asyncio.run(exercise())
    finally:
        release_first.set()
        fallback.cancel()


def test_rds_slow_docker_does_not_block_parallel_dynamodb(monkeypatch):
    instance_id = "event-loop-rds"
    container_id = "slow-rds-container"
    monkeypatch.setattr(rds, "_get_docker", lambda: None)
    response = rds._create_db_instance(
        {
            "DBInstanceIdentifier": [instance_id],
            "Engine": ["postgres"],
            "MasterUsername": ["admin"],
            "MasterUserPassword": ["password"],
        }
    )
    assert response[0] == 200
    rds._instances[instance_id]["_docker_container_id"] = container_id
    monkeypatch.setattr(rds, "_get_docker", lambda: _SlowDocker(container_id))

    async def delete_instance():
        return await rds.handle_request(
            "POST",
            "/",
            {},
            b"",
            {
                "Action": ["DeleteDBInstance"],
                "DBInstanceIdentifier": [instance_id],
                "SkipFinalSnapshot": ["true"],
            },
        )

    try:
        asyncio.run(_run_with_parallel_dynamodb_probe(delete_instance))
    finally:
        rds._instances.pop(instance_id, None)


def test_ecs_slow_docker_does_not_block_parallel_dynamodb(monkeypatch):
    cluster_name = "event-loop-ecs"
    task_id = "event-loop-task"
    container_id = "slow-ecs-container"
    cluster_arn = (
        f"arn:aws:ecs:{get_region()}:{get_account_id()}:cluster/{cluster_name}"
    )
    task_arn = (
        f"arn:aws:ecs:{get_region()}:{get_account_id()}:"
        f"task/{cluster_name}/{task_id}"
    )
    assert ecs._create_cluster({"clusterName": cluster_name})[0] == 200
    ecs._tasks[task_arn] = {
        "taskArn": task_arn,
        "clusterArn": cluster_arn,
        "lastStatus": "RUNNING",
        "desiredStatus": "RUNNING",
        "containers": [],
        "_docker_ids": [container_id],
        "_metadata_tokens": [],
    }
    monkeypatch.setattr(ecs, "_get_docker", lambda: _SlowDocker(container_id))

    async def stop_task():
        return await ecs.handle_request(
            "POST",
            "/",
            {"x-amz-target": "AmazonEC2ContainerServiceV20141113.StopTask"},
            json.dumps({"cluster": cluster_name, "task": task_arn}).encode(),
            {},
        )

    try:
        asyncio.run(_run_with_parallel_dynamodb_probe(stop_task))
    finally:
        ecs._tasks.pop(task_arn, None)
        ecs._clusters.pop(cluster_name, None)


def test_eks_slow_docker_does_not_block_parallel_dynamodb(monkeypatch):
    cluster_name = "event-loop-eks"
    container_id = "slow-eks-container"
    arn = f"arn:aws:eks:{get_region()}:{get_account_id()}:cluster/{cluster_name}"
    eks._clusters[cluster_name] = {
        "name": cluster_name,
        "arn": arn,
        "status": "ACTIVE",
        "_docker_id": container_id,
    }
    monkeypatch.setattr(eks, "_get_docker", lambda: _SlowDocker(container_id))

    async def delete_cluster():
        return await eks.handle_request(
            "DELETE",
            f"/clusters/{cluster_name}",
            {},
            b"",
            {},
        )

    try:
        asyncio.run(_run_with_parallel_dynamodb_probe(delete_cluster))
    finally:
        eks._clusters.pop(cluster_name, None)


def test_elasticache_slow_docker_does_not_block_parallel_dynamodb(monkeypatch):
    cluster_id = "event-loop-cache"
    container_id = "slow-elasticache-container"
    monkeypatch.setattr(elasticache, "_get_docker", lambda: None)
    response = elasticache._create_cache_cluster(
        {"CacheClusterId": [cluster_id], "Engine": ["redis"]}
    )
    assert response[0] == 200
    elasticache._clusters[cluster_id]["_docker_container_id"] = container_id
    monkeypatch.setattr(
        elasticache,
        "_get_docker",
        lambda: _SlowDocker(container_id),
    )

    async def delete_cluster():
        return await elasticache.handle_request(
            "POST",
            "/",
            {},
            f"Action=DeleteCacheCluster&CacheClusterId={cluster_id}".encode(),
            {},
        )

    try:
        asyncio.run(_run_with_parallel_dynamodb_probe(delete_cluster))
    finally:
        elasticache._clusters.pop(cluster_id, None)


def test_opensearch_slow_docker_does_not_block_parallel_dynamodb(monkeypatch):
    domain_name = "event-loop-search"
    container_id = "slow-opensearch-container"
    monkeypatch.setattr(opensearch, "_get_docker", lambda: None)
    record = opensearch.create_domain_record({"DomainName": domain_name})
    record["_ContainerId"] = container_id
    monkeypatch.setattr(
        opensearch,
        "_get_docker",
        lambda: _SlowDocker(container_id),
    )

    async def delete_domain():
        return await opensearch.handle_request(
            "DELETE",
            f"/2021-01-01/domain/{domain_name}",
            {},
            b"",
            {},
        )

    try:
        asyncio.run(_run_with_parallel_dynamodb_probe(delete_domain))
    finally:
        opensearch._domains.pop(domain_name, None)
