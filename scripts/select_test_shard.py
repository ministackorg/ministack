import argparse
import ast
import glob
import json
from functools import lru_cache
from pathlib import Path

def _decorator_name(decorator: ast.AST) -> str:
    if isinstance(decorator, ast.Name):
        return decorator.id
    if isinstance(decorator, ast.Attribute):
        return decorator.attr
    if isinstance(decorator, ast.Call):
        return _decorator_name(decorator.func)
    return ""


def _is_test_function(node: ast.AST) -> bool:
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return False
    return node.name.startswith("test_")


def _is_serial_test(node: ast.AST) -> bool:
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return False
    return any(_decorator_name(deco) == "serial" for deco in node.decorator_list)


def _expr_contains_serial(expr: ast.AST) -> bool:
    if isinstance(expr, (ast.Name, ast.Attribute, ast.Call)):
        return _decorator_name(expr) == "serial"
    if isinstance(expr, (ast.List, ast.Tuple, ast.Set)):
        return any(_expr_contains_serial(elt) for elt in expr.elts)
    return False


@lru_cache(maxsize=1)
def _serial_tests_from_conftest() -> dict[str, set[str]]:
    conftest = Path("tests/conftest.py")
    if not conftest.exists():
        return {}

    try:
        source = conftest.read_text(encoding="utf-8")
        module = ast.parse(source)
    except (OSError, SyntaxError, UnicodeDecodeError):
        return {}

    serial_nodeids: set[str] = set()
    for node in ast.walk(module):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "_SERIAL_TESTS" and isinstance(node.value, ast.Set):
                    for elt in node.value.elts:
                        if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                            serial_nodeids.add(elt.value)

    per_file: dict[str, set[str]] = {}
    for nodeid in serial_nodeids:
        parts = nodeid.split("::")
        if len(parts) < 2:
            continue
        file_path = parts[0]
        test_name = parts[-1]
        per_file.setdefault(file_path, set()).add(test_name)

    return per_file


def count_tests(path: Path, mode: str) -> int:
    try:
        source = path.read_text(encoding="utf-8")
        module = ast.parse(source)
    except (OSError, SyntaxError, UnicodeDecodeError):
        return 0

    module_is_serial = False
    for stmt in module.body:
        if isinstance(stmt, ast.Assign):
            if any(isinstance(t, ast.Name) and t.id == "pytestmark" for t in stmt.targets):
                if _expr_contains_serial(stmt.value):
                    module_is_serial = True

    conftest_serial_names = _serial_tests_from_conftest().get(path.as_posix(), set())

    all_tests = 0
    serial_tests = 0

    def _walk(nodes: list[ast.stmt], inherited_serial: bool) -> None:
        nonlocal all_tests, serial_tests
        for node in nodes:
            if isinstance(node, ast.ClassDef):
                class_is_serial = inherited_serial or any(_decorator_name(deco) == "serial" for deco in node.decorator_list)
                _walk(node.body, class_is_serial)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if _is_test_function(node):
                    all_tests += 1
                    is_serial = (
                        inherited_serial
                        or module_is_serial
                        or _is_serial_test(node)
                        or node.name in conftest_serial_names
                    )
                    if is_serial:
                        serial_tests += 1

    _walk(module.body, False)

    if mode == "serial":
        return serial_tests
    if mode == "parallel":
        return max(0, all_tests - serial_tests)
    return all_tests


def file_weight(path: Path, mode: str) -> float:
    tests = count_tests(path, mode)
    size_kib = max(1.0, path.stat().st_size / 1024.0)
    # Blend test count and file size to keep shards stable without historical timings.
    return tests * 10.0 + size_kib


def list_test_files() -> list[str]:
    files = sorted(glob.glob("tests/test_*.py"))
    return [f for f in files if Path(f).is_file()]


def build_shards(files: list[str], shard_count: int, mode: str) -> list[list[str]]:
    shards = [[] for _ in range(shard_count)]
    shard_totals = [0.0 for _ in range(shard_count)]

    eligible_files = []
    weights: dict[str, float] = {}
    for f in files:
        path = Path(f)
        test_count = count_tests(path, mode)
        if mode != "all" and test_count == 0:
            continue
        weight = file_weight(path, mode)
        eligible_files.append(f)
        weights[f] = weight

    weighted_files = sorted(eligible_files, key=lambda f: weights[f], reverse=True)

    for test_file in weighted_files:
        idx = min(range(shard_count), key=lambda i: shard_totals[i])
        shards[idx].append(test_file)
        shard_totals[idx] += weights[test_file]

    return [sorted(shard) for shard in shards]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--mode", choices=["parallel", "serial", "all"], default="parallel")
    parser.add_argument("--exclude", action="append", default=[])
    parser.add_argument("--format", choices=["shell", "json"], default="shell")
    args = parser.parse_args()

    if args.shard_count <= 0:
        raise SystemExit("--shard-count must be greater than 0")
    if not 0 <= args.shard_index < args.shard_count:
        raise SystemExit("--shard-index must be in range [0, --shard-count)")

    files = [f for f in list_test_files() if f not in set(args.exclude)]
    shards = build_shards(files, args.shard_count, args.mode)

    selected = shards[args.shard_index]
    if args.format == "json":
        print(json.dumps(selected))
    else:
        print(" ".join(selected))


if __name__ == "__main__":
    main()
