"""`stringify`, the JSON text a caller gets in place of a raw `json.dumps(evaluate(...))`.

`evaluate()` hands back plain Python values plus two things `json.dumps` cannot take: the
`UNDEFINED` sentinel a builtin such as `$match` leaves inside a result, and a non-finite `float`
an expression such as `1/0` produces. Every expectation here was measured against jsonata-js
2.2.2's own `JSON.stringify(await expr.evaluate({}))`, which is what a caller wiring `evaluate()`
onto a returned document is really asking for.
"""

import json
import math

import pytest

from ministack.core.jsonata import evaluate
from ministack.core.jsonata.rendering import stringify


def rendered(expression, data=None):
    return stringify(evaluate(expression, data))


# ---- large numbers: positional inside the band, exponent notation outside it --------------------

LARGE_NUMBERS = [
    ("100000000000000000", "100000000000000000"),
    ("1e20", "100000000000000000000"),
    ("123456789012345678901", "123456789012345680000"),
    ("1e21", "1e+21"),
    ("1e-7", "1e-7"),
    ("1e-6", "0.000001"),
    ("-1e21", "-1e+21"),
    ("-1e-7", "-1e-7"),
    ("0", "0"),
    ("-0", "0"),
    ("1.5", "1.5"),
    ("78.8/2", "39.4"),
]


@pytest.mark.parametrize("expression, expected", LARGE_NUMBERS)
def test_a_number_is_spelled_the_way_javascript_spells_it(expression, expected):
    assert rendered(expression) == expected


def test_a_large_integer_keeps_the_shortest_round_tripping_decimal():
    """`int(value)` is the double's exact binary expansion above 2**53, not this decimal."""
    assert rendered("123456789012345678901") == "123456789012345680000"
    assert rendered("123456789012345678901") != str(int(float(123456789012345678901)))


# ---- UNDEFINED reaching a returned document -------------------------------------------------------

def test_an_undefined_array_element_renders_as_null():
    """`$match`'s unparticipating capture group is the reference case: a real `undefined` hole."""
    result = evaluate('$match("a1", /(a)(b)?(1)/)', {})
    assert stringify(result) == '{"match":"a1","index":0,"groups":["a",null,"1"]}'


def test_an_undefined_object_value_drops_its_key():
    assert stringify({"a": evaluate("foo", {}), "b": 1}) == '{"b":1}'


# ---- Infinity and NaN reaching a returned document, without $string()'s D3001/D1001 --------------

def test_a_bare_infinite_result_renders_as_null():
    assert rendered("1/0") == "null"
    assert rendered("-1/0") == "null"


def test_infinity_nested_in_a_document_renders_as_null_without_raising():
    assert rendered('{"a": 1/0}') == '{"a":null}'
    assert rendered("[1/0, 2]") == "[null,2]"


def test_stringify_never_raises_on_what_string_raises_on():
    """`$string()` raises `D3001`/`D1001` on a non-finite number; a plain document render does not."""
    assert stringify(math.inf) == "null"
    assert stringify(math.nan) == "null"
    assert stringify(-math.inf) == "null"


# ---- the output is always valid JSON text ---------------------------------------------------------

@pytest.mark.parametrize("expression", [
    "100000000000000000", "1e21", "1e-7", "1/0",
    '$match("a1", /(a)(b)?(1)/)', '{"a": 1/0, "b": [1/0, 2, foo]}',
])
def test_the_rendered_text_is_valid_json(expression):
    json.loads(rendered(expression))
