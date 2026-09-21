# AGENTS.md

Instructions for AI coding agents working in MiniStack. Read `CONTRIBUTING.md`
before changing a service; it has the contribution process and examples.

## Where to work

- `ministack/app.py` owns service registration, dispatch, and reset.
- `ministack/core/router.py` detects services; `ministack/core/responses.py`
  contains shared response and account/region helpers.
- `ministack/services/` contains service implementations. Start with the
  existing service module or package and its neighboring code.
- `tests/test_<service>.py` is the usual test location; established services
  may have several test files. `tests/conftest.py` holds client fixtures and
  the `_SERIAL_TESTS` list.

## Before changing behavior

- New AWS services and infrastructure changes (Dockerfiles, CI workflows,
  `pyproject.toml`, dependencies) need a scoped GitHub issue before code is
  written. Check for overlapping work. Follow the scope agreed in the issue.
- Keep additions focused on operations needed by a real use case. Do not add
  speculative API coverage or operations that return placeholder results.
- Check the behavior and wire format against available evidence. Never claim
  behavior was validated against AWS unless it was actually run against a real
  AWS account. Label the evidence precisely as a real AWS observation (record
  region and account context when relevant), official AWS documentation,
  botocore service model inspection, existing tests or code, or inference;
  documentation, model inspection, and tests are not AWS validation. Match
  errors, status codes, and retry behavior.
- If AWS has one fixed behavior, implement that behavior directly rather than
  adding a MiniStack setting. Add configuration only for choices MiniStack
  itself must make.

## Service contracts

- For routing and response formats, inspect the existing handler,
  `core/router.py`, and the relevant botocore model. Some services support
  multiple protocols. Preserve the wire format used by the operation being
  changed; see `CONTRIBUTING.md` for examples.
- A new service needs an entry in `SERVICE_REGISTRY` in `app.py` and the
  routing needed in `core/router.py`. Follow an established service's module
  layout when extending it; not every helper or submodule needs its own
  routing pattern.
- Every registered service module, including registered submodules, must
  define `get_state()`, `load_persisted_state(data)`, and `reset()`. The
  registry and `tests/test_persistence.py` enforce this contract.
- Keep resources isolated by account and region where AWS does. Follow the
  existing scoped-state helpers and check that persistence saves all tenants,
  not only the current request's tenant.
- Keep AWS SDK clients out of service implementations. Use them in tests or
  for shape inspection during development. Follow nearby code for blocking
  Docker calls and avoid adding service locks without checking reentrant calls.

## Working and validation

- Python 3.10+ is supported; CI uses 3.13. Install test dependencies with
  `uv pip install --system -e ".[test]" -r requirements.txt`.
- Start MiniStack with `python -m hypercorn ministack.app:app --bind 0.0.0.0:4566`;
  check `http://localhost:4566/_ministack/health`. Most tests
  require the server at `localhost:4566`.
- Run the relevant test file, then `pytest tests/` when routing, registry, or
  service behavior changed. Run `ruff check ministack/`, the CI lint gate.
- Tests that alter global server state or depend on ordering belong in
  `_SERIAL_TESTS` in `tests/conftest.py`; keep them out of parallel runs.

## Before opening a PR

- Update the supported-services table in `README.md` for a new service and
  follow the existing `CHANGELOG.md` format for user-facing changes.
- Preserve explanatory dependency comments in `pyproject.toml` when changing
  dependencies. Avoid new lint suppressions without a reason.
- Keep each PR focused. Describe every behavior change in the diff, how it
  was validated, and any issue that the change fulfills.
- Confirm the relevant tests, broader suite when needed, and lint pass.
