# Contributing to MiniStack

Thanks for wanting to contribute. The codebase is intentionally simple — each AWS service is a single self-contained Python file inside `ministack/services/`. Adding a new service or fixing a bug should take minutes, not hours.

## Project Structure

```
ministack/
├── ministack/
│   ├── app.py              # ASGI entry point, service routing, reset endpoint
│   ├── core/
│   │   ├── responses.py    # json_response, error_response_json, new_uuid
│   │   ├── router.py       # detect_service(), SERVICE_PATTERNS
│   │   ├── lambda_runtime.py
│   │   └── persistence.py
│   └── services/
│       ├── s3.py, sqs.py, sns.py, dynamodb.py, ...
│       └── cognito.py      # example of a two-client service file
├── tests/
│   ├── conftest.py         # pytest fixtures (boto3 clients)
│   └── test_services.py    # all integration tests
├── Dockerfile
├── pyproject.toml
└── CHANGELOG.md
```

## Adding a New Service

Every service follows the same 4-step pattern:

### 1. Create `ministack/services/myservice.py`

```python
"""
MyService Emulator.
JSON-based API via X-Amz-Target.
Supports: OperationOne, OperationTwo, ...
"""

import json
import logging
from ministack.core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("myservice")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_state: dict = {}  # in-memory storage


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "OperationOne": _operation_one,
        "OperationTwo": _operation_two,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    return handler(data)


def _operation_one(data):
    return json_response({"result": "ok"})


def _operation_two(data):
    return json_response({})


def reset():
    _state.clear()
```

**Protocol guide:**

- JSON services (DynamoDB, SecretsManager, Glue, Athena, Cognito, etc.) — use `json_response` / `error_response_json`, route via `X-Amz-Target`
- XML/Query services (S3, SQS, SNS, IAM, STS, RDS, ElastiCache, EC2) — build XML responses, route via `Action` query param; use `_xml(status, root_tag, inner)` pattern; verify field names against botocore shapes via `Loader().load_service_model()`
- REST services (Lambda, ECS, Route53) — route via URL path

### 2. Register in `ministack/app.py`

```python
from ministack.services import myservice

SERVICE_REGISTRY = {
    # ... existing ...
    "myservice": {"module": "myservice"},
}
```

If the service needs aliases, add them in the registry entry.

### 3. Add detection to `ministack/core/router.py`

```python
SERVICE_PATTERNS = {
    # ... existing ...
    "myservice": {
        "target_prefixes": ["AWSMyService"],   # for X-Amz-Target routing
        "host_patterns": [r"myservice\."],      # for host-based routing
    },
}
```

Add any credential scope or `Action`-based routing as needed.

### 4. Add a fixture to `tests/conftest.py`

```python
@pytest.fixture(scope="session")
def mysvc():
    return make_client("myservice")
```

### 5. Add tests to `tests/test_services.py`

```python
def test_myservice_operation_one(mysvc):
    resp = mysvc.operation_one(Param="value")
    assert resp["result"] == "ok"
```

---

## Running Tests Locally

```bash
# Start the stack
docker compose up -d

# Install test dependencies
pip install boto3 pytest pytest-xdist duckdb docker cbor2 

# Parallel-safe phase: run tests that are safe to run concurrently
pytest tests/ -v -n 4 --dist=loadfile -m "not serial"

# Serial/global-state phase: run tests that mutate runtime state or require isolation
pytest tests/ -v -m serial

# Run a specific service
pytest tests/ -v -k "cognito"
```

---

## Code Conventions

- **One file per service** — keep everything for a service in `ministack/services/myservice.py`
- **Imports** — always `from ministack.core.responses import ...`, never `from core.responses import ...`
- **In-memory state** — use module-level dicts (`_things: dict = {}`)
- **reset()** — every service must expose a `reset()` that clears all module-level state; it's called by `/_ministack/reset`
- **No external AWS deps** — no `boto3`, `botocore`, or `aws-sdk` in service code
- **Minimal dependencies** — `duckdb` and `docker` are optional; guard with `try/except ImportError`
- **Error responses** — match real AWS error codes and HTTP status codes as closely as possible
- **Logging** — `logger = logging.getLogger("servicename")`; DEBUG for request details, INFO for significant events

---

## Pull Request Checklist

- [ ] New service file in `ministack/services/`
- [ ] Registered in `ministack/app.py` SERVICE_REGISTRY
- [ ] Detection patterns added to `ministack/core/router.py`
- [ ] Fixture added to `tests/conftest.py`
- [ ] Tests added and passing (`pytest tests/ -v`)
- [ ] Linting passes (`ruff check ministack/`)
- [ ] Service added to the table in `README.md`
- [ ] Entry added to `CHANGELOG.md`

---

## What We're Looking For

High-value contributions right now:

- **CloudFront** — distribution CRUD, invalidations, origin configuration
- **CodeBuild / CodePipeline** — CI/CD pipeline stubs
- **AppSync** — GraphQL API CRUD
- **SQS FIFO** — message group / deduplication support
- **More Cognito flows** — hosted UI, federated identity providers, custom auth triggers

---

## Questions?

Open a GitHub Discussion or file an issue with the `question` label.
