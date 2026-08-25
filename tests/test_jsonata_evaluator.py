"""What the JSONata evaluator does with the cases where a plausible reading gives the wrong answer.

Every expectation here was taken from jsonata-js 2.0.6 running the same expression, so a change
that "looks more sensible" than one of these assertions is a regression against the language.
"""

import sys
import time

import pytest

from ministack.core import jsonata
from ministack.core.jsonata import UNDEFINED, JsonataError, evaluate

NEST = {"nest0": [{"nest1": [{"nest2": [{"nest3": [1, 2]}, {"nest3": [3, 4]}]}]}]}

PHONES = {"Phone": [{"type": "home", "number": "0203 544 1234"},
                    {"type": "office", "number": "01962 001234"},
                    {"type": "office", "number": "01962 001235"},
                    {"type": "mobile", "number": "077 7700 1234"}]}

LIBRARY = {"library": {"loans": [{"customer": "10001", "isbn": "9780262510871"},
                                 {"customer": "10003", "isbn": "9780201038019"}],
                       "books": [{"isbn": "9780262510871", "title": "Structure and Interpretation"},
                                 {"isbn": "9780201038019", "title": "The Art of Computer Programming"}]}}

# The b of every leaf is an array, so the last step of `a[0].b` returns one unflattened.
NESTED_ARRAYS = [{"a": [{"b": [1]}, {"b": [2]}]}, {"a": [{"b": [3]}, {"b": [4]}]}]


def test_a_one_element_sequence_unwraps_but_an_array_it_selected_does_not():
    assert evaluate("a", {"a": 1}) == 1
    assert evaluate("a", {"a": [1]}) == [1]
    assert evaluate("[1]", {}) == [1]
    assert evaluate("a.b", {"a": [{"b": 1}, {"b": 2}]}) == [1, 2]


def test_selecting_nothing_is_undefined_and_never_an_empty_sequence():
    assert evaluate("bar", {}) is UNDEFINED
    assert evaluate("a.b.c", {"a": {}}) is UNDEFINED
    assert evaluate("$foo", {}) is UNDEFINED
    assert evaluate("[]", {}) == []
    assert evaluate("[a]", {}) == []


def test_a_lone_array_from_the_last_step_is_returned_unflattened():
    assert evaluate("a", {"a": [[1, 2]]}) == [[1, 2]]
    assert evaluate("a.b", {"a": {"b": [[1]]}}) == [[1]]
    assert evaluate("a", [{"a": [[1]]}, {"a": [[2]]}]) == [[1], [2]]


def test_a_constructed_array_is_pushed_whole_where_a_sequence_flattens():
    assert evaluate("nest0.nest1.nest2.nest3", NEST) == [1, 2, 3, 4]
    assert evaluate("nest0.nest1.nest2.[nest3]", NEST) == [[1, 2], [3, 4]]
    assert evaluate("nest0.[nest1.nest2.nest3]", NEST) == [1, 2, 3, 4]


def test_an_empty_predicate_keeps_the_singleton_as_an_array():
    assert evaluate('Phone[type="mobile"].number', PHONES) == "077 7700 1234"
    assert evaluate('Phone[type="mobile"][].number', PHONES) == ["077 7700 1234"]
    assert evaluate('Phone[][type="mobile"].number', PHONES) == ["077 7700 1234"]


def test_a_numeric_predicate_selecting_an_array_replaces_the_whole_result():
    assert evaluate("a[0]", {"a": [[1, 2], [3]]}) == [1, 2]
    assert evaluate("a[1]", {"a": [[1, 2], [3]]}) == [3]
    assert evaluate("a[0]", {"a": [1, 2]}) == 1


def test_a_numeric_index_is_floored_before_it_counts_from_the_end():
    assert evaluate("[1,2,3][-1.2]", {}) == 2
    assert evaluate("[1,2,3][-1]", {}) == 3
    assert evaluate("[1,2,3][1.9]", {}) == 2
    assert evaluate("[1,2,3][3]", {}) is UNDEFINED
    assert evaluate("[1,2,3][-4]", {}) is UNDEFINED


def test_a_predicate_of_numbers_selects_positions_and_anything_else_is_a_truth_test():
    assert evaluate("[1..10][[1..3,8,-1]]", {}) == [2, 3, 4, 9, 10]
    assert evaluate("[1..10][[1..3,8,false]]", {}) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    assert evaluate("clues[x=6][y=3].number", {"clues": [{"number": 7, "x": 6, "y": 3}]}) == 7
    assert evaluate("$[x=6][y=3].number", [{"number": 7, "x": 6, "y": 2}]) is UNDEFINED


def test_a_predicate_runs_once_per_input_item_unless_the_path_is_absolute():
    assert evaluate("a[0].b", NESTED_ARRAYS) == [1]
    assert evaluate("$.a[0].b", NESTED_ARRAYS) == [1, 3]


def test_the_context_variable_is_the_whole_document_only_for_an_absolute_path():
    document = [{"phone": [{"number": 0}]}, {"phone": [{"number": 1}]}]
    assert evaluate("phone[0].number", document) == 0
    assert evaluate("$.phone[0].number", document) == [0, 1]
    assert evaluate("$$.phone[0].number", document) == [0, 1]


def test_the_boolean_of_every_json_type_decides_a_condition():
    truthiness = {'""': False, '"x"': True, "0": False, "1": True, "null": False, "true": True,
                  "false": False, "[]": False, "[0]": False, "[0,0]": False, "[0,1]": True,
                  "[[]]": False, "{}": False, '{"a":1}': True, "$foo": False}
    for expression, truthy in truthiness.items():
        assert evaluate(f"{expression} ? 'yes' : 'no'", {}) == ("yes" if truthy else "no"), expression


def test_a_condition_without_an_else_branch_selects_nothing():
    assert evaluate("false ? 1", {}) is UNDEFINED
    assert evaluate("null ? 1 : 2", {}) == 2
    assert evaluate("$foo ? 1 : 2", {}) == 2


def test_a_block_scopes_its_bindings_and_returns_its_last_expression():
    assert evaluate("(1; 2; 3)", {}) == 3
    assert evaluate("($x := 1; $x + 1)", {}) == 2
    assert evaluate("($x := 1; ($x := 2); $x)", {}) == 1
    assert evaluate("$x := 5", {}) == 5


def test_a_lambda_closes_over_the_frame_and_the_context_it_was_defined_in():
    assert evaluate("($f := function($x){$x * 2}; $f(21))", {}) == 42
    assert evaluate("($n := 10; $f := function(){$n}; $n := 20; $f())", {}) == 20
    assert evaluate("a.($f := function(){b}; $f())", {"a": {"b": "inner"}}) == "inner"
    assert evaluate("($f := function($x){$x}; $f())", {}) is UNDEFINED


def test_recursion_runs_unbounded_unless_the_caller_names_a_max_depth():
    """The reference's default: with no `options.stack`, jsonata.js:2203-2222 installs no guard."""
    assert evaluate("($f := function($n){$n = 0 ? 0 : 1 + $f($n - 1)}; $f(100))", {}) == 100
    assert evaluate("1" + "+1" * 400, {}) == 401


def test_a_max_depth_stops_a_runaway_recursion_with_u1001():
    factorial = "($factorial := function($n){$n = 0 ? 1 : $n * $factorial($n - 1)}; $factorial(5))"
    assert evaluate(factorial, {}, max_depth=302) == 120
    with pytest.raises(JsonataError) as runaway:
        evaluate("($inf := function($n){$n + $inf($n - 1)}; $inf(5))", {}, max_depth=50)
    assert runaway.value.code == "U1001"
    assert runaway.value.message.startswith("Stack overflow error: Check for non-terminating")


def test_evaluating_an_expression_leaves_the_process_recursion_limit_alone():
    """MiniStack serves many requests from one process: a query cannot move the ceiling for all."""
    before = sys.getrecursionlimit()
    evaluate("($f := function($n){$n = 0 ? 0 : 1 + $f($n - 1)}; $f(20))", {})
    assert sys.getrecursionlimit() == before


def test_a_tail_call_does_not_consume_the_python_stack():
    mutual = ("($even := function($n) { $n = 0 ? true : $odd($n-1) };"
              " $odd := function($n) { $n = 0 ? false : $even($n-1) };"
              " $odd(6555))")
    assert evaluate(mutual, {}) is True


def test_a_signature_validates_the_arguments_of_a_lambda():
    assert evaluate("($f := function($s, $x)<an:s> { $x > 0 ? $f([$s, $s], $x-1) : $s}; $f('a', 2))",
                    {}) == ["a", "a", "a", "a"]
    with pytest.raises(JsonataError) as mismatch:
        evaluate("($f := function($s, $x)<sn:s> { $x > 0 ? $f([$s, $s], $x-1) : $s}; $f('a', 2))", {})
    assert mismatch.value.code == "T0410"
    # A lambda has no name of its own, so the error blames the name the call reached it by. This
    # one fails on the recursive call, which is a tail call and reaches the signature by bouncing.
    assert mismatch.value.message == 'Argument 1 of function "f" does not match function signature'
    with pytest.raises(JsonataError) as direct:
        evaluate('($fun := function($arr)<a<n>>{$arr}; $fun("f"))', {})
    assert direct.value.code == "T0412"
    assert direct.value.message == 'Argument 1 of function "fun" must be an array of "numbers"'


def test_partial_application_leaves_the_placeholders_for_the_next_call():
    assert evaluate("($add := function($x, $y){$x + $y}; $add2 := $add(?, 2); $add2(3))", {}) == 5
    assert evaluate("($add := function($x, $y){$x + $y}; $add2 := $add(2, ?); $add2(4))", {}) == 6
    assert evaluate("($firstn := $substring(?, 0, ?); $first5 := $firstn(?, 5); $first5('Hello World'))",
                    {}) == "Hello"
    with pytest.raises(JsonataError) as forgotten:
        evaluate("substring(?, 0, ?)", {})
    assert forgotten.value.code == "T1007"
    with pytest.raises(JsonataError) as unknown:
        evaluate("unknown(?)", {})
    assert unknown.value.code == "T1008"


def test_the_apply_operator_prepends_the_left_side_as_the_first_argument():
    assert evaluate('"hello" ~> $uppercase()', {}) == "HELLO"
    assert evaluate('"  hello  " ~> $trim() ~> $uppercase()', {}) == "HELLO"
    assert evaluate("($shout := $trim ~> $uppercase; $shout('  hi  '))", {}) == "HI"
    with pytest.raises(JsonataError) as not_a_function:
        evaluate('42 ~> "hello"', {})
    assert not_a_function.value.code == "T2006"


def test_a_context_binding_carries_the_left_side_into_the_predicate():
    joined = evaluate("library.loans@$L.books@$B[$L.isbn = $B.isbn].{'customer': $L.customer,"
                      " 'title': $B.title}", LIBRARY)
    assert joined == [{"customer": "10001", "title": "Structure and Interpretation"},
                      {"customer": "10003", "title": "The Art of Computer Programming"}]


def test_an_index_binding_numbers_the_stream_where_it_is_written():
    assert evaluate("$^($)#$pos[$pos < 3]", [3, 1, 4, 1]) == [1, 1, 3]
    assert evaluate("$#$pos[$pos < 3]^($)", [3, 1, 4, 1]) == [1, 3, 4]
    assert evaluate("$#$pos[$pos < 3] = $[[0..2]]", [3, 1, 4, 1]) is True


def test_a_parent_reference_reads_the_step_it_was_resolved_against():
    document = {"Account": {"Name": "Firefly", "Order": [{"id": "o1"}, {"id": "o2"}]}}
    assert evaluate("Account.Order.%.Name", document) == ["Firefly", "Firefly"]
    assert evaluate("Account.Order.{'name': %.Name, 'id': id}", document) == [
        {"name": "Firefly", "id": "o1"}, {"name": "Firefly", "id": "o2"}]


def test_grouping_collects_every_item_that_produced_the_same_key():
    assert evaluate("Phone{type: number}", PHONES) == {"home": "0203 544 1234",
                                                       "office": ["01962 001234", "01962 001235"],
                                                       "mobile": "077 7700 1234"}
    assert evaluate("Phone{type: number[]}", PHONES) == {"home": ["0203 544 1234"],
                                                         "office": ["01962 001234", "01962 001235"],
                                                         "mobile": ["077 7700 1234"]}
    assert evaluate("{'Hello': 'World'}", []) == {"Hello": "World"}
    assert evaluate("$.{'Hello': 'World'}", []) is UNDEFINED


def test_an_object_constructor_omits_undefined_values_and_keeps_nulls():
    assert evaluate("{'a': $foo, 'b': 1}", {}) == {"b": 1}
    assert evaluate("{'a': null}", {}) == {"a": None}
    with pytest.raises(JsonataError) as bad_key:
        evaluate("{1: 'one'}", {})
    assert bad_key.value.code == "T1003"
    with pytest.raises(JsonataError) as duplicate:
        evaluate("{'a': 1, 'a': 2}", {})
    assert duplicate.value.code == "D1009"


def test_a_sort_is_stable_and_puts_what_it_cannot_read_last():
    people = [{"name": "b", "age": 2}, {"name": "a", "age": 1}, {"name": "c", "age": 2}]
    assert evaluate("$^(age).name", people) == ["a", "b", "c"]
    assert evaluate("$^(>age).name", people) == ["b", "c", "a"]
    assert evaluate("$^(age).name", [{"name": "b"}, {"name": "a", "age": 1}]) == ["a", "b"]
    with pytest.raises(JsonataError) as mixed:
        evaluate("$^(age)", [{"age": 1}, {"age": "one"}])
    assert mixed.value.code == "T2007"
    with pytest.raises(JsonataError) as unsortable:
        evaluate("$^(age)", [{"age": True}, {"age": False}])
    assert unsortable.value.code == "T2008"


def test_comparison_separates_the_undefined_side_from_the_incomparable_one():
    assert evaluate("$foo < 1", {}) is UNDEFINED
    assert evaluate("1 < 2", {}) is True
    with pytest.raises(JsonataError) as crossed:
        evaluate('"32" < 42', {})
    assert crossed.value.code == "T2009"
    with pytest.raises(JsonataError) as incomparable:
        evaluate("null <= 'world'", {})
    assert incomparable.value.code == "T2010"


def test_equality_is_false_whenever_a_side_selected_nothing():
    assert evaluate("$foo = 1", {}) is False
    assert evaluate("$foo != 1", {}) is False
    assert evaluate('"3" = 3', {}) is False
    assert evaluate("[1,2] = [1,2]", {}) is True
    assert evaluate("{'a': 1} = {'a': 1}", {}) is True
    assert evaluate("null = null", {}) is True


def test_arithmetic_reports_which_side_was_not_a_number():
    assert evaluate("24 * $notexist", {}) is UNDEFINED
    assert evaluate("-5 % 3", {}) == -2
    assert evaluate("5 % -3", {}) == 2
    assert evaluate("4 / 2", {}) == 2
    with pytest.raises(JsonataError) as left:
        evaluate('"5" + 5', {})
    assert left.value.code == "T2001"
    with pytest.raises(JsonataError) as right:
        evaluate("5 + false", {})
    assert right.value.code == "T2002"
    with pytest.raises(JsonataError) as overflow:
        evaluate("1/(10e300 * 10e100)", {})
    assert overflow.value.code == "D1001"


def test_and_or_answer_with_a_boolean_even_when_a_side_is_missing():
    assert evaluate("$foo or $bar", {}) is False
    assert evaluate("true and $foo", {}) is False
    assert evaluate("true or $foo", {}) is True
    assert evaluate("[1,2] and 'x'", {}) is True


def test_a_range_yields_nothing_rather_than_an_error_when_it_cannot_count():
    assert evaluate("[5..2]", {}) == []
    assert evaluate("[-2..$blah]", {}) == []
    assert evaluate("[$blah..5, 3, -2..$blah]", {}) == [3]
    assert evaluate("[1.0 .. 3]", {}) == [1, 2, 3]
    with pytest.raises(JsonataError) as fractional:
        evaluate("[1.1 .. 5]", {})
    assert fractional.value.code == "T2003"
    with pytest.raises(JsonataError) as fractional_end:
        evaluate("[1 .. 5.5]", {})
    assert fractional_end.value.code == "T2004"


def test_string_concatenation_serialises_whatever_it_is_given():
    assert evaluate("1 & 2", {}) == "12"
    assert evaluate("[1,2] & [3,4]", {}) == "[1,2][3,4]"
    assert evaluate("$foo & 'x'", {}) == "x"
    assert evaluate("'Prices: ' & a.b", {"a": {"b": [1, 2]}}) == "Prices: [1,2]"


def test_a_wildcard_walks_the_values_and_the_descendant_operator_walks_the_tree():
    assert evaluate("*", {"a": 1, "b": 2}) == [1, 2]
    assert evaluate("*", {"a": [1, 2, 3]}) == [1, 2, 3]
    assert evaluate("*", {}) is UNDEFINED
    assert evaluate("**.b", {"a": {"b": 1}, "c": {"d": {"b": 2}}}) == [1, 2]
    assert evaluate("**", {"a": {"b": 1}}) == [{"a": {"b": 1}}, {"b": 1}, 1]


def test_a_transform_updates_a_clone_and_leaves_the_input_alone():
    document = {"Account": {"Order": [{"Price": 10}, {"Price": 20}]}}
    doubled = evaluate("$ ~> |Account.Order|{'Price': Price * 2}|", document)
    assert doubled == {"Account": {"Order": [{"Price": 20}, {"Price": 40}]}}
    assert document == {"Account": {"Order": [{"Price": 10}, {"Price": 20}]}}
    assert evaluate("$ ~> |Account|{}, ['Order']|", document) == {"Account": {}}
    with pytest.raises(JsonataError) as not_an_object:
        evaluate("$ ~> |Account|'nope'|", document)
    assert not_an_object.value.code == "T2011"
    with pytest.raises(JsonataError) as not_a_key:
        evaluate("$ ~> |Account|{}, 42|", document)
    assert not_a_key.value.code == "T2012"


def test_the_variable_bindings_of_the_caller_are_visible_to_the_expression():
    assert evaluate("$greeting & ' ' & name", {"name": "world"}, {"greeting": "hello"}) == "hello world"
    assert evaluate("$missing", {}, {"greeting": "hello"}) is UNDEFINED


def test_a_character_class_matches_the_ascii_range_a_javascript_regex_does():
    """A JSONata literal carries only `i` and `m`, so `\\d` never widens to Unicode as it would with `u`."""
    assert evaluate(r"$match('٤٢', /\d+/)", {}) is UNDEFINED
    assert evaluate(r"$match('42', /\d+/)", {})["match"] == "42"
    assert [found["match"] for found in evaluate(r"$match('héllo', /\w+/)", {})] == ["h", "llo"]


def test_one_evaluation_reads_one_clock():
    """jsonata-js binds `$now` and `$millis` per run over a single timestamp, so they cannot drift."""
    first, second, apart = evaluate("($a := $millis(); $b := $millis(); [$a, $b, $b - $a])", {})
    assert first == second and apart == 0
    assert evaluate("$now() = $now()", {}) is True
    # The frozen clock also fills the components `[MNn]` leaves unspecified.
    assert evaluate("$fromMillis($toMillis('March', '[MNn]'), '[Y0001]') = $now('[Y0001]')", {}) is True


def test_separate_evaluations_read_their_own_clock():
    earlier = evaluate("$millis()", {})
    time.sleep(0.01)
    assert evaluate("$millis()", {}) > earlier


def test_the_package_exports_only_what_is_used_from_outside_it():
    """`stepfunctions.py` reads these four; the parser and the value types come by module path."""
    assert jsonata.__all__ == ["UNDEFINED", "JsonataError", "evaluate", "stringify"]
