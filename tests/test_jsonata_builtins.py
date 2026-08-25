"""The builtin function library, at the edges the JSONata conformance suite leaves uncovered.

The suite in `tests/jsonata_suite.json` exercises what the functions compute. What it barely
touches is what they do with nothing: `UNDEFINED` in almost always means `UNDEFINED` out, and the
five functions that break that rule are the ones a port gets wrong. The other two things tested
here are the error code each function raises for the input it refuses, and `$replace`'s `$N`
substitution parser, which agrees with neither JavaScript's nor `re.sub`'s syntax.

The functions are called directly, so the signature layer is out of the way and the answer is the
function body's own; the cases that need a regex or a lambda get one out of a real expression.
"""

import pytest

from ministack.core.jsonata import UNDEFINED, JsonataError, evaluate
from ministack.core.jsonata.builtins import BUILTINS
from ministack.core.jsonata.evaluator import arity
from ministack.core.jsonata.values import ERROR_MESSAGES


def call(name, *args):
    return BUILTINS[name](*args)


def regex(source):
    """The matcher closure a `/…/` literal evaluates to, which is what a pattern argument is."""
    return evaluate(source, None)


def raises(code, name, *args):
    with pytest.raises(JsonataError) as failure:
        call(name, *args)
    assert failure.value.code == code, f"{name} raised {failure.value.code}, expected {code}"
    return failure.value


def raises_from_evaluate(code, expression, data=None):
    """Runs `expression` through the evaluator, which is what threads a builtin's name onto
    `Signature.validate`'s error: `call()` invokes the builtin bare, past the signature check."""
    with pytest.raises(JsonataError) as failure:
        evaluate(expression, data)
    assert failure.value.code == code, f"{expression!r} raised {failure.value.code}, expected {code}"
    return failure.value


# ---- UNDEFINED in, UNDEFINED out ----------------------------------------------------------------

PROPAGATES_UNDEFINED = [
    ("$string", ()),
    ("$length", ()),
    ("$substring", (1,)),
    ("$substringBefore", ("x",)),
    ("$substringAfter", ("x",)),
    ("$lowercase", ()),
    ("$uppercase", ()),
    ("$trim", ()),
    ("$pad", (5,)),
    ("$base64encode", ()),
    ("$base64decode", ()),
    ("$encodeUrl", ()),
    ("$encodeUrlComponent", ()),
    ("$decodeUrl", ()),
    ("$decodeUrlComponent", ()),
    ("$contains", ("x",)),
    ("$match", (UNDEFINED,)),
    ("$replace", ("a", "b")),
    ("$split", (",",)),
    ("$join", ()),
    ("$number", ()),
    ("$abs", ()),
    ("$floor", ()),
    ("$ceil", ()),
    ("$round", ()),
    ("$sqrt", ()),
    ("$power", (2,)),
    ("$formatBase", ()),
    ("$formatNumber", ("#",)),
    ("$formatInteger", ("0",)),
    ("$parseInteger", ("0",)),
    ("$sum", ()),
    ("$max", ()),
    ("$min", ()),
    ("$average", ()),
    ("$boolean", ()),
    ("$not", ()),
    ("$reverse", ()),
    ("$shuffle", ()),
    ("$distinct", ()),
    ("$sort", ()),
    ("$merge", ()),
    ("$spread", ()),
    ("$lookup", ("key",)),
    ("$sift", (UNDEFINED,)),
    ("$type", ()),
    ("$fromMillis", ()),
    ("$toMillis", ()),
    ("$map", (UNDEFINED,)),
    ("$filter", (UNDEFINED,)),
    ("$single", ()),
    ("$reduce", (UNDEFINED,)),
    ("$partition", (2,)),
    ("$range", (5,)),
    ("$hash", ("SHA-256",)),
    ("$parse", ()),
]

# The conformance suite only covers JSONata 2.0.6, so the functions Step Functions adds have no
# case in it at all and reach these arguments unguarded unless something here checks them.
AWS_ADDITION_UNDEFINED_IN_A_LATER_POSITION = [
    ("$partition", ([1, 2], UNDEFINED)),
    ("$range", (1, UNDEFINED)),
    ("$hash", ("payload", UNDEFINED)),
]


@pytest.mark.parametrize("name,rest", PROPAGATES_UNDEFINED, ids=[name for name, _ in PROPAGATES_UNDEFINED])
def test_undefined_first_argument_propagates(name, rest):
    assert call(name, UNDEFINED, *rest) is UNDEFINED


@pytest.mark.parametrize("name,args", AWS_ADDITION_UNDEFINED_IN_A_LATER_POSITION,
                         ids=[name for name, _ in AWS_ADDITION_UNDEFINED_IN_A_LATER_POSITION])
def test_an_aws_addition_propagates_undefined_from_any_position(name, args):
    assert call(name, *args) is UNDEFINED


def test_an_aws_addition_still_refuses_an_argument_that_is_present_and_wrong():
    """Propagating absence must not swallow the wrong type: only UNDEFINED takes the quiet exit."""
    raises("T0410", "$hash", "payload", "NOT-AN-ALGORITHM")
    raises("T0410", "$partition", [1, 2], 0)
    raises("T0410", "$range", 1, 5, 0)
    raises("D3120", "$parse", "{not json}")


def test_uuid_takes_no_argument_and_random_reads_an_undefined_seed_as_no_seed():
    """`$random`'s seed is declared `n?`, so selecting nothing is the same as omitting it."""
    assert len(call("$uuid")) == 36
    assert isinstance(call("$random", UNDEFINED), float)
    assert call("$random", 42) == call("$random", 42)


def test_count_of_nothing_is_zero():
    """The first exception: absence counts as an empty array, not as absence."""
    assert call("$count", UNDEFINED) == 0


def test_exists_of_nothing_is_false():
    assert call("$exists", UNDEFINED) is False
    assert call("$exists", None) is True
    assert call("$exists", 0) is True


def test_append_returns_the_other_argument_untouched():
    assert call("$append", UNDEFINED, [2, 3]) == [2, 3]
    assert call("$append", 1, UNDEFINED) == 1
    assert call("$append", UNDEFINED, UNDEFINED) is UNDEFINED


def test_error_and_assert_never_return():
    raises("D3137", "$error", UNDEFINED)
    raises("D3141", "$assert", False, UNDEFINED)


def test_keys_and_each_of_nothing_are_the_empty_sequence():
    """Not UNDEFINED: an empty sequence is what the evaluator later flattens into one."""
    assert call("$keys", UNDEFINED) == []
    assert call("$each", UNDEFINED, UNDEFINED) == []


def test_second_argument_absent_also_stops_contains():
    assert call("$contains", "Hello", UNDEFINED) is UNDEFINED


def test_type_of_nothing_is_nothing_not_a_name():
    assert call("$type", UNDEFINED) is UNDEFINED
    assert call("$type", None) == "null"
    assert call("$type", BUILTINS["$sum"]) == "function"


# ---- the code each function raises --------------------------------------------------------------

def test_string_refuses_a_number_it_cannot_write():
    raises("D3001", "$string", float("inf"))
    raises("D1001", "$string", {"inf": float("inf")})


def test_number_refuses_what_parse_float_would_have_accepted():
    raises("D3030", "$number", "1234 hello")
    raises("D3030", "$number", "10e500")
    raises("D3030", "$number", "")
    assert call("$number", "0x1f") == 31


def test_numeric_domain_errors():
    raises("D3060", "$sqrt", -1)
    raises("D3061", "$power", -1, 0.5)
    raises("D3061", "$power", 10, 1000)
    raises("D3100", "$formatBase", 100, 1)
    raises("D3100", "$formatBase", 100, 37)


def test_format_base_reports_the_rounded_radix_it_rejected():
    assert raises("D3100", "$formatBase", 100, 1.4).value == 1


def test_replace_and_split_and_match_reject_a_negative_limit():
    raises("D3011", "$replace", "hello", "l", "1", -2)
    raises("D3020", "$split", "a,b", ",", -3)
    raises("D3040", "$match", "a", regex("/a/"), -1)


def test_replace_refuses_an_empty_pattern():
    raises("D3010", "$replace", "hello", "", "bye")


def test_replace_refuses_a_replacement_that_is_not_a_string():
    raises("D3012", "$replace", "hat", regex("/hat/"), lambda match: True)
    raises("D3012", "$replace", "hat", regex("/hat/"), lambda match: 42)


def test_reduce_refuses_a_callback_that_cannot_accumulate():
    raises("D3050", "$reduce", [1, 2], lambda only: only)


def test_single_counts_its_matches():
    raises("D3138", "$single", [0, 1, 2])
    raises("D3139", "$single", [])
    raises("D3139", "$single", [0, 1, 2], lambda value: value == 3)
    assert call("$single", [0]) == 0
    assert call("$single", [0, 1, 2], BUILTINS["$not"]) == 0


def test_sort_without_a_comparator_needs_one_type():
    raises("D3070", "$sort", [1, "two"])
    raises("D3070", "$sort", [{"a": 1}, {"a": 2}])
    assert call("$sort", [1, 3, 2]) == [1, 2, 3]


def test_malformed_urls_name_the_function_that_rejected_them():
    for name in ("$decodeUrl", "$decodeUrlComponent"):
        assert raises("D3140", name, "%E0%A4%A").functionName == name[1:]
    for name in ("$encodeUrl", "$encodeUrlComponent"):
        assert raises("D3140", name, "\ud800").functionName == name[1:]


def test_a_matcher_that_answers_the_wrong_shape_is_rejected():
    failure = raises("T1010", "$split", "some text", BUILTINS["$uppercase"])
    assert failure.token == "split"
    raises("T1010", "$contains", "some text", BUILTINS["$uppercase"])


def test_a_zero_length_match_that_cannot_advance_is_an_error_not_a_loop():
    raises("D1004", "$replace", "abracadabra", regex("/.*?/"), "$1")


def test_error_throws_and_assert_returns_nothing_when_it_holds():
    # The wording each one carries is not asserted here: `ERROR_MESSAGES` renders the `{{{message}}}`
    # template of D3137 and D3141 as "undefined}", so no wording reaches the caller today.
    raises("D3137", "$error")
    raises("D3137", "$error", "Too Expensive")
    raises("D3141", "$assert", False)
    raises("D3141", "$assert", False, "Too Expensive")
    assert call("$assert", True) is UNDEFINED


# ---- $replace's own $N substitution parser ------------------------------------------------------

def substitute(text, pattern, replacement):
    return call("$replace", text, regex(pattern), replacement)


def test_dollar_zero_is_the_whole_match_and_dollar_dollar_is_a_literal():
    assert substitute("265USD", r"/([0-9]+)USD/", "$$$1") == "$265"
    assert substitute("265USD", r"/([0-9]+)USD/", "$0 -> $$$1") == "265USD -> $265"


def test_a_dollar_not_followed_by_a_digit_stays_a_dollar():
    assert substitute("265USD", r"/([0-9]+)USD/", "$w") == "$w"
    assert substitute("abcdefghijklmno", r"/(ijk)/", "$x$") == "abcdefgh$x$lmno"


def test_a_group_that_did_not_participate_substitutes_nothing():
    assert substitute("abcd", r"/(ab)|(a)/", "[1=$1][2=$2]") == "[1=ab][2=]cd"


def test_a_group_number_out_of_range_leaves_no_trace():
    assert substitute("265USD", r"/([0-9]+)USD/", "$0$1$2") == "265USD265"


def test_the_digit_count_read_after_the_dollar_follows_the_group_count():
    """One group reads one digit, thirteen read two, and an overshoot retries one digit shorter."""
    thirteen = r"/(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)(m)/"
    assert substitute("abcdefghijklmno", thirteen, "$8$5$12$12$18$123") == "hella8l3no"
    assert substitute("abcdefghijklmno", r"/(ijk)/", "$8$5$12$12$18$123") == "abcdefghijk2ijk2ijk8ijk23lmno"
    assert substitute("abcdefghijklmno", r"/ijk/", "$8$5$12$12$18$123") == "abcdefgh22823lmno"


def test_substitution_is_regex_only_a_literal_pattern_takes_the_text_verbatim():
    assert call("$replace", "a$0c", "$0", "X") == "aXc"
    assert call("$replace", "abc", "b", "$0") == "a$0c"


def test_the_limit_counts_replacements_not_characters():
    assert substitute("ababbxabbcc", r"/b+/", "yy") == "ayyayyxayycc"
    assert call("$replace", "ababbxabbcc", regex(r"/b+/"), "yy", 2) == "ayyayyxabbcc"
    assert call("$replace", "ababbxabbcc", regex(r"/b+/"), "yy", 0) == "ababbxabbcc"


def test_a_function_replacement_sees_the_match_object():
    matched = call("$replace", "temperature 68F", regex(r"/(\d+)F/"), lambda found: found["groups"][0] + "C")
    assert matched == "temperature 68C"


# ---- what the suite does not reach --------------------------------------------------------------

def test_string_writes_numbers_the_way_javascript_does():
    assert call("$string", 1e-7) == "1e-7"
    assert call("$string", 1e-6) == "0.000001"
    assert call("$string", 1e21) == "1e+21"
    assert call("$string", 1e20) == "100000000000000000000"
    assert call("$string", 22 / 7) == "3.14285714285714"
    assert call("$string", 39.400000000000006) == "39.4"
    # `int(value)` is the double's exact binary expansion above 2**53, not the shortest decimal
    # `repr` already carries: 123456789012345683968 there, 123456789012345680000 in the reference.
    assert call("$string", float(123456789012345678901)) == "123456789012345680000"


def test_a_signature_mismatch_names_the_builtin_that_rejected_it():
    """`Signature.validate` only runs through the evaluator, so `call()` cannot reach it here."""
    failure = raises_from_evaluate("T0412", "$sum([{}])")
    assert failure.token == "sum"
    assert failure.message == 'Argument 1 of function "sum" must be an array of "numbers"'

    failure = raises_from_evaluate("T0410", '$substring("x","y")')
    assert failure.token == "substring"
    assert failure.message == 'Argument 2 of function "substring" does not match function signature'

    failure = raises_from_evaluate("T0411", "$length()", data=23)
    assert failure.token == "length"
    assert failure.message == (
        'Context value is not a compatible type with argument 1 of function "length"')


def test_sift_that_keeps_nothing_is_undefined_not_an_empty_object():
    assert call("$sift", {"a": 0}, BUILTINS["$boolean"]) is UNDEFINED
    assert call("$sift", {"a": 1, "b": 0}, BUILTINS["$boolean"]) == {"a": 1}


def test_sort_is_stable_so_equal_elements_keep_their_order():
    entries = [{"key": 1, "at": position} for position in range(6)]
    same = call("$sort", entries, lambda left, right: left["key"] > right["key"])
    assert [entry["at"] for entry in same] == [0, 1, 2, 3, 4, 5]


def test_sort_leaves_its_input_alone():
    original = [3, 1, 2]
    assert call("$sort", original) == [1, 2, 3]
    assert original == [3, 1, 2]


def test_map_drops_what_the_callback_did_not_answer():
    assert call("$map", [1, 2, 3], lambda value: UNDEFINED if value == 2 else value) == [1, 3]


def test_round_goes_to_even_on_a_tie():
    assert call("$round", 2.5) == 2
    assert call("$round", 3.5) == 4
    assert call("$round", -0.5) == 0
    assert call("$round", 4.525, 2) == 4.52
    assert call("$round", 12450, -2) == 12400


def test_trim_collapses_only_the_four_characters_javascript_does():
    assert call("$trim", "  Hello \t\n World  ") == "Hello World"
    assert call("$trim", "Hello") == "Hello"


def test_substring_clamps_a_start_left_of_the_string_instead_of_wrapping():
    assert call("$substring", "hello world", -100, 3) == "hel"
    assert call("$substring", "hello world", -5, 5) == "world"
    assert call("$substring", "hello world", 0, -6) == ""


def test_zip_shortens_to_nothing_when_an_argument_is_not_an_array():
    assert call("$zip", [1, 2], [3, 4]) == [[1, 3], [2, 4]]
    assert call("$zip", [1, 2], UNDEFINED) == []


def test_boolean_of_an_array_is_true_when_any_element_is():
    assert call("$boolean", [0, 0]) is False
    assert call("$boolean", [0, 1]) is True
    assert call("$boolean", [[[True]]]) is True
    assert call("$boolean", BUILTINS["$sum"]) is False


# ---- what Step Functions adds -------------------------------------------------------------------

def test_partition_keeps_a_short_last_chunk():
    assert call("$partition", [1, 2, 3, 4], 3) == [[1, 2, 3], [4]]
    assert call("$partition", [1, 2, 3, 4], 2) == [[1, 2], [3, 4]]


def test_partition_rounds_a_fractional_chunk_size_down():
    assert call("$partition", [1, 2, 3, 4], 3.9) == [[1, 2, 3], [4]]
    raises("T0410", "$partition", [1, 2], 0.5)


def test_range_walks_up_and_down_and_includes_an_end_it_lands_on():
    assert call("$range", 0, 10, 2) == [0, 2, 4, 6, 8, 10]
    assert call("$range", 1, 4) == [1, 2, 3, 4]
    assert call("$range", 4, 1, -1) == [4, 3, 2, 1]
    assert call("$range", 0, 9, 2) == [0, 2, 4, 6, 8]
    raises("T0410", "$range", 0, 10, 0)


def test_hash_answers_the_hex_digest_of_a_named_algorithm():
    assert call("$hash", "abc", "SHA-256").startswith("ba7816bf8f01cfea")
    assert call("$hash", "abc", "MD5") == "900150983cd24fb0d6963f7d28e17f72"
    raises("T0410", "$hash", "abc", "sha-256")


def test_random_is_deterministic_only_when_seeded():
    assert call("$random", 42) == call("$random", 42)
    assert call("$random", 42) != call("$random", 43)
    assert 0 <= call("$random") < 1


def test_uuid_is_a_fresh_version_four_identifier():
    first, second = call("$uuid"), call("$uuid")
    assert first != second
    assert len(first) == 36 and first[14] == "4"


def test_parse_deserializes_json_and_refuses_anything_else():
    assert call("$parse", '{"a": [1, 2]}') == {"a": [1, 2]}
    assert call("$parse", "true") is True
    raises("D3120", "$parse", "{not json}")


def test_clone_copies_through_the_string_form():
    original = {"a": [1, {"b": 2}]}
    copied = call("$clone", original)
    assert copied == original
    copied["a"][1]["b"] = 3
    assert original["a"][1]["b"] == 2


def test_clone_is_a_library_function_like_every_other_builtin():
    """A transform looks `$clone` up by name, and it answers from the library, not from a patch."""
    assert BUILTINS["$clone"].__module__.endswith("jsonata.builtins")
    assert BUILTINS["$clone"].signature.definition == "<(oa)-:o>"


# ---- the message a raised error carries ---------------------------------------------------------

def test_error_and_assert_carry_the_text_they_were_given():
    """Step Functions surfaces this text as the `Cause` of a failed execution, so it has to survive."""
    assert raises("D3137", "$error", "Too Expensive").message == "Too Expensive"
    assert raises("D3141", "$assert", False, "must be positive").message == "must be positive"


def test_error_and_assert_fall_back_to_their_own_wording():
    assert raises("D3137", "$error").message == "$error() function evaluated"
    assert raises("D3141", "$assert", False).message == "$assert() statement failed"


def test_every_catalogued_message_renders_without_leaving_a_placeholder():
    """The triple-brace entries are the ones a double-brace-only pattern silently mangles."""
    supplied = {"token": "TOK", "value": "VAL", "value2": "V2", "index": 1, "type": "ty",
                "exp": 2, "message": "MSG", "functionName": "FN"}
    for code in ERROR_MESSAGES:
        rendered = JsonataError(code, **supplied).message
        assert "{" not in rendered and "}" not in rendered, f"{code} left a brace: {rendered}"
        assert "undefined" not in rendered, f"{code} lost a supplied field: {rendered}"


def test_a_triple_brace_hole_takes_the_value_raw_and_a_double_brace_hole_json_encodes_it():
    """D3140 carries both: `${{{functionName}}}` raw, `{{value}}` as `JSON.stringify` writes it."""
    assert JsonataError("D3140", functionName="decodeUrl", value="%").message == (
        'Malformed URL passed to $decodeUrl(): "%"')
    assert JsonataError("T1005", token="foo").message == (
        "Attempted to invoke a non-function. Did you mean $foo?")


# ---- the contract the evaluator reads -----------------------------------------------------------

def test_every_builtin_carries_its_signature_and_its_declared_arity():
    for name, function in BUILTINS.items():
        assert getattr(function, "signature", None) is not None, f"{name} has no signature"
        assert isinstance(function.arity, int), f"{name} has no arity"


def test_arity_is_the_required_parameter_count_not_the_signature_length():
    """`$string` declares `<x-b?:s>` but takes one argument, so a callback is handed one."""
    assert BUILTINS["$string"].arity == 1
    assert BUILTINS["$append"].arity == 2
    assert call("$map", [1, 2, 3], BUILTINS["$string"]) == ["1", "2", "3"]


def test_the_evaluator_answers_the_same_arity_the_builtin_declares():
    """One question, one answer: a higher-order function and the evaluator cannot disagree here."""
    assert arity(BUILTINS["$string"]) == BUILTINS["$string"].arity == 1
    assert len(BUILTINS["$string"].signature.params) == 2, "the signature counts the optional one"
    for name, function in BUILTINS.items():
        assert arity(function) == function.arity, f"{name} reports two arities"
