# MiniStack Python Testcontainers Example

Integration tests for MiniStack using [Testcontainers](https://testcontainers-python.readthedocs.io/) and boto3.

## Prerequisites

- Python 3.10+
- Docker
- pip

## Setup

```bash
pip install -r requirements.txt
```

## Run tests

```bash
pytest test_ministack.py -v
```

The tests automatically start a MiniStack container, wait for it to become healthy, and run S3, SQS, and DynamoDB integration tests against it.
