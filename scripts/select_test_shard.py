import argparse
import glob
import json
from pathlib import Path

DEFAULT_FILE_WEIGHTS = {
    "tests/test_cognito_custom_auth.py": 120.0,
    "tests/test_alb.py": 90.0,
    "tests/test_ecs.py": 80.0,
    "tests/test_stepfunctions.py": 40.0,
    "tests/test_opensearch.py": 35.0,
    "tests/test_cfn_custom_resource.py": 70.0,
    "tests/test_apigatewayv2.py": 65.0,
    "tests/test_cfn.py": 60.0,
    "tests/test_eventbridge.py": 35.0,
    "tests/test_rds.py": 35.0,
    "tests/test_lambda_proxy.py": 25.0,
    "tests/test_package.py": 15.0,
    "tests/test_sqs.py": 20.0,
    "tests/test_ssm.py": 10.0,
}


def load_weights(weights_path: str | None) -> dict[str, float]:
    weights = dict(DEFAULT_FILE_WEIGHTS)
    if weights_path and Path(weights_path).exists():
        with open(weights_path) as f:
            data = json.load(f)
        for key, value in data.items():
            weights[key] = float(value)
    return weights


def list_test_files() -> list[str]:
    files = sorted(glob.glob("tests/test_*.py"))
    return [f for f in files if Path(f).is_file()]


def build_shards(files: list[str], weights: dict[str, float], shard_count: int) -> list[list[str]]:
    shards = [[] for _ in range(shard_count)]
    shard_totals = [0.0 for _ in range(shard_count)]

    weighted_files = sorted(files, key=lambda f: weights.get(f, 1.0), reverse=True)

    for test_file in weighted_files:
        idx = min(range(shard_count), key=lambda i: shard_totals[i])
        shards[idx].append(test_file)
        shard_totals[idx] += weights.get(test_file, 1.0)

    return [sorted(shard) for shard in shards]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--weights-file", default="")
    parser.add_argument("--exclude", action="append", default=[])
    parser.add_argument("--format", choices=["shell", "json"], default="shell")
    args = parser.parse_args()

    files = [f for f in list_test_files() if f not in set(args.exclude)]
    weights = load_weights(args.weights_file or None)
    shards = build_shards(files, weights, args.shard_count)

    selected = shards[args.shard_index]
    if args.format == "json":
        print(json.dumps(selected))
    else:
        print(" ".join(selected))


if __name__ == "__main__":
    main()
