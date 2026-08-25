"""The official JSONata conformance suite, run against ministack.core.jsonata.

The cases in `tests/jsonata_suite.json` come from the jsonata-js reference
implementation (https://github.com/jsonata-js/jsonata, MIT), folded from its 1319
loose files into one document. Step Functions documents itself as implementing the
JSONata 2.0.6 specification, so this suite is what says whether it does.

Each case carries `expr` plus either `result`, `undefinedResult` or the error
`code` it must raise. Its input is a named `dataset`, an inline `data`, or neither,
and neither is not the same as an input of `null`.

A case exercising a function Step Functions does not offer carries
`absent_from_step_functions` and is skipped rather than dropped, so the run says how
much of the reference this interpreter is not asked to cover. Today that is `$eval`.

Ten cases also carry a `depth`: the evaluation budget the reference's own harness
runs them under, five different values between 10 and 500. It is the harness that
sets it, not the language, so it is passed per case rather than compiled in.
"""

import json
import pathlib
import sys

import pytest

from ministack.core.jsonata import UNDEFINED, JsonataError, evaluate

SUITE = json.loads((pathlib.Path(__file__).parent / "jsonata_suite.json").read_text())
DATASETS = SUITE["datasets"]
CASES = SUITE["cases"]

# One JSONata evaluation level costs three to four Python frames (measured at 3.5 on
# tail-recursion/case005), so this process needs room for the deepest `depth` a case budgets
# before that `depth` can be what stops the expression rather than Python's own stack. It is the
# harness that needs the room, not the interpreter: `evaluate` leaves the limit where it found it.
PYTHON_FRAMES_PER_EVALUATION = 10
sys.setrecursionlimit(max(case.get("depth", 0) for case in CASES) * PYTHON_FRAMES_PER_EVALUATION)


def _matches(actual, expected, unordered=False):
    """JSON equality, with the float tolerance the suite's own runner applies."""
    if isinstance(actual, bool) or isinstance(expected, bool):
        return actual is expected
    if isinstance(actual, (int, float)) and isinstance(expected, (int, float)):
        if actual == expected:
            return True
        return abs(actual - expected) <= 1e-10 * max(abs(actual), abs(expected))
    if isinstance(actual, list) and isinstance(expected, list):
        if len(actual) != len(expected):
            return False
        if not unordered:
            return all(map(_matches, actual, expected))
        # An `unordered` case asserts the members, not their order. Match each
        # expected element against an actual one that is still unclaimed.
        remaining = list(actual)
        for item in expected:
            found = next((other for other in remaining if _matches(other, item)), None)
            if found is None:
                return False
            remaining.remove(found)
        return True
    if isinstance(actual, dict) and isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(
            _matches(actual[key], expected[key]) for key in actual)
    return actual == expected


def _document(case):
    """The input a case runs against. No `dataset` and no `data` means no input,
    which JSONata keeps distinct from an input of `null`."""
    if "dataset" in case:
        return DATASETS[case["dataset"]]
    return case["data"] if "data" in case else UNDEFINED


@pytest.mark.parametrize("case", CASES, ids=[f"{c['group']}/{c['case']}" for c in CASES])
def test_jsonata_conformance(case):
    if "absent_from_step_functions" in case:
        pytest.skip(f"Step Functions does not ship {case['absent_from_step_functions']}")
    try:
        actual = evaluate(case["expr"], _document(case), case.get("bindings") or {},
                          max_depth=case.get("depth"))
        raised = None
    except JsonataError as failure:
        actual, raised = UNDEFINED, failure.code

    if "code" in case:
        assert raised == case["code"], f"{case['expr']!r} raised {raised}, expected {case['code']}"
    elif case.get("undefinedResult"):
        assert raised is None, f"{case['expr']!r} raised {raised}, expected undefined"
        assert actual is UNDEFINED, f"{case['expr']!r} returned {actual!r}, expected undefined"
    else:
        assert raised is None, f"{case['expr']!r} raised {raised}, expected {case.get('result')!r}"
        assert actual is not UNDEFINED, f"{case['expr']!r} returned undefined"
        assert _matches(actual, case["result"], case.get("unordered", False)), \
            f"{case['expr']!r} returned {actual!r}, expected {case['result']!r}"
