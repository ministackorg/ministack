"""Tests for the XPath 3.1 picture-string engines.

Every expectation is the value jsonata-js 2.0.6 produces for the same call. The weighting is deliberate: the
JSONata suite covers the common pictures well but barely touches the word spellout at its boundaries, ordinal
suffixes, `$parseInteger` round-tripping, timezone offsets other than a couple, or most of the error codes.
"""

import math

import pytest

from ministack.core.jsonata.datetime_picture import format_datetime, parse_datetime, parse_iso_8601
from ministack.core.jsonata.number_picture import (
    format_integer,
    format_number,
    parse_integer,
    round_half_even,
    words_to_number,
)
from ministack.core.jsonata.values import JsonataError

# 2018-03-23T10:33:36.617Z — the frozen "now" that unspecified high-order components default from.
NOW = 1521801216617


def raises(code, call, *args):
    with pytest.raises(JsonataError) as caught:
        call(*args)
    assert caught.value.code == code
    assert caught.value.message


# --- word spellout, cardinal ----------------------------------------------------------------------------------


@pytest.mark.parametrize("value,expected", [
    (0, "zero"),
    (1, "one"),
    (19, "nineteen"),
    (20, "twenty"),
    (21, "twenty-one"),
    (99, "ninety-nine"),
    (100, "one hundred"),
    (101, "one hundred and one"),
    (110, "one hundred and ten"),
    (111, "one hundred and eleven"),
    (199, "one hundred and ninety-nine"),
    (200, "two hundred"),
    (327, "three hundred and twenty-seven"),
    (999, "nine hundred and ninety-nine"),
    (1000, "one thousand"),
    (1001, "one thousand and one"),
    (1100, "one thousand, one hundred"),
    (1234, "one thousand, two hundred and thirty-four"),
    (100000, "one hundred thousand"),
    (327730, "three hundred and twenty-seven thousand, seven hundred and thirty"),
    (327730000, "three hundred and twenty-seven million, seven hundred and thirty thousand"),
    (10 ** 6, "one million"),
    (10 ** 6 + 1, "one million and one"),
    (10 ** 9, "one billion"),
    (10 ** 12, "one trillion"),
])
def test_cardinal_words(value, expected):
    assert format_integer(value, "w") == expected


@pytest.mark.parametrize("value,expected", [
    (1e15, "one thousand trillion"),
    (1e15 + 1, "one thousand trillion and one"),
    (1e18, "one million trillion"),
    (1e21, "one billion trillion"),
    (1e24, "one trillion trillion"),
    (1e46, "ten billion trillion trillion trillion"),
])
def test_words_above_the_trillion_ceiling(value, expected):
    """There is no word past "trillion": the largest magnitude repeats instead of a quadrillion being invented."""
    assert format_integer(value, "w") == expected


def test_negative_words_take_a_bare_minus():
    assert format_integer(-5, "w") == "-five"
    assert format_integer(-101, "w") == "-one hundred and one"


# --- word spellout, ordinal -----------------------------------------------------------------------------------


@pytest.mark.parametrize("value,expected", [
    (0, "zeroth"),
    (1, "first"),
    (2, "second"),
    (3, "third"),
    (4, "fourth"),
    (11, "eleventh"),
    (12, "twelfth"),
    (13, "thirteenth"),
    (20, "twentieth"),
    (21, "twenty-first"),
    (30, "thirtieth"),
    (40, "fortieth"),
    (100, "one hundredth"),
    (101, "one hundred and first"),
    (111, "one hundred and eleventh"),
    (1000, "one thousandth"),
    (10 ** 6, "one millionth"),
    (1000000000001, "one trillion and first"),
])
def test_ordinal_words(value, expected):
    assert format_integer(value, "w;o") == expected


def test_word_case_modifiers():
    assert format_integer(327730, "W") == ("THREE HUNDRED AND TWENTY-SEVEN THOUSAND, "
                                           "SEVEN HUNDRED AND THIRTY")
    assert format_integer(327730, "Ww") == "Three Hundred and Twenty-Seven Thousand, Seven Hundred and Thirty"
    assert format_integer(21, "W;o") == "TWENTY-FIRST"
    assert format_integer(21, "Ww;o") == "Twenty-First"


# --- parse_integer round trips --------------------------------------------------------------------------------


@pytest.mark.parametrize("value", [0, 19, 21, 99, 100, 101, 327, 999, 1000, 1001, 327730, 10 ** 6, 1000000000001])
@pytest.mark.parametrize("picture", ["w", "w;o", "W", "Ww"])
def test_word_round_trip(value, picture):
    assert parse_integer(format_integer(value, picture), picture) == value


@pytest.mark.parametrize("value", [1, 4, 9, 14, 40, 90, 400, 1987, 3999])
def test_roman_round_trip(value):
    assert parse_integer(format_integer(value, "I"), "I") == value
    assert parse_integer(format_integer(value, "i"), "i") == value


@pytest.mark.parametrize("value", [1, 26, 27, 52, 53, 702, 703])
def test_letters_round_trip(value):
    assert parse_integer(format_integer(value, "A"), "A") == value
    assert parse_integer(format_integer(value, "a"), "a") == value


@pytest.mark.parametrize("picture", ["0000", "#,##0", "#:###,##0", "#,##,##0", "#0;o"])
def test_decimal_round_trip(picture):
    assert parse_integer(format_integer(1234567890, picture), picture) == 1234567890


def test_parse_words_without_a_prior_format():
    assert parse_integer("one hundred and twenty-three", "w") == 123
    assert parse_integer("twelfth", "w;o") == 12
    assert parse_integer("three hundred and sixty-five", "w") == 365


# --- roman, letters and decimal rendering ---------------------------------------------------------------------


def test_roman_and_letter_edges():
    assert format_integer(1987, "I") == "MCMLXXXVII"
    assert format_integer(3999, "i") == "mmmcmxcix"
    assert format_integer(0, "I") == "", "roman numerals have no zero"
    assert format_integer(-5, "i") == "-v", "the sign is prepended after rendering, whatever the numbering"
    assert format_integer(26, "A") == "Z"
    assert format_integer(27, "A") == "AA", "bijective base 26, so Z is followed by AA and never by A0"
    assert format_integer(702, "a") == "zz"
    assert format_integer(703, "a") == "aaa"


def test_decimal_grouping_regular_and_irregular():
    assert format_integer(1234567890, "#,##0") == "1,234,567,890"
    assert format_integer(1234567890, "#:###,##0") == "1234:567,890"
    assert format_integer(1234567890, "#,##,##0") == "12345,67,890"
    assert format_integer(123, "0000") == "0123"
    assert format_integer(-3, "0000") == "-0003"


def test_format_integer_floors_the_signed_value():
    assert format_integer(3.9, "0") == "3"
    assert format_integer(-3.2, "0") == "-4", "floor runs on the signed value, so -3.2 becomes -4 before rendering"


@pytest.mark.parametrize("value,expected", [
    (11, "11th"), (12, "12th"), (13, "13th"), (21, "21st"), (22, "22nd"), (23, "23rd"),
    (28, "28th"), (112, "112th"),
])
def test_ordinal_decimal_suffix(value, expected):
    assert format_integer(value, "#0;o") == expected
    assert parse_integer(expected, "#0;o") == value


# --- timezone offsets -----------------------------------------------------------------------------------------


MIDDAY = 1531310400000


@pytest.mark.parametrize("picture,timezone,expected", [
    ("[Z]", "+0530", "+05:30"),
    ("[Z0]", "+0530", "+5:30"),
    ("[Z01:01]", "+0530", "+05:30"),
    ("[Z0000]", "+0530", "+0530"),
    ("[Z00]", "+0530", "+05:30"),
    ("[Z000]", "+0530", "+530"),
    ("[Z]", "-0500", "-05:00"),
    ("[Z0]", "-0500", "-5"),
    ("[Z0000]", "-0500", "-0500"),
    ("[Z00]", "-0500", "-05"),
    ("[Z]", "+1245", "+12:45"),
    ("[Z0]", "+1245", "+12:45"),
    ("[Z]", "+1400", "+14:00"),
    ("[Z0]", "+1400", "+14"),
    ("[Z]", "0000", "+00:00"),
    ("[Z0]", "0000", "+0"),
    ("[Z0000]", "0000", "+0000"),
])
def test_timezone_offsets(picture, timezone, expected):
    assert format_datetime(MIDDAY, picture, timezone) == expected


def test_negative_offset_with_minutes_carries_the_sign_into_both_fields():
    """`-0930` splits into -10 hours and -30 minutes, and the hour-only branch prints both signed."""
    assert format_datetime(MIDDAY, "[Z0]", "-0930") == "-10:-30"
    assert format_datetime(MIDDAY, "[Z01:01]", "-0930") == "-10:30"
    assert format_datetime(MIDDAY, "[Z0000]", "-0930") == "-1030"


def test_gmt_prefixed_offsets():
    assert format_datetime(MIDDAY, "[z]", "+0530") == "GMT+05:30"
    assert format_datetime(MIDDAY, "[z01:01]", "-0500") == "GMT-05:00"
    assert format_datetime(MIDDAY, "[z]", "0000") == "GMT+00:00"


def test_the_t_modifier_collapses_a_zero_offset_to_z():
    assert format_datetime(MIDDAY, "[Z01:01t]", "0000") == "Z"
    assert format_datetime(MIDDAY, "[Z01:01t]") == "Z"
    assert format_datetime(MIDDAY, "[z01:01t]") == "Z", "the GMT prefix is dropped along with the digits"
    assert format_datetime(MIDDAY, "[Z01:01t]", "+0530") == "+05:30"


def test_timezone_shifts_the_rendered_instant():
    assert format_datetime(1531310400000, None, "-0500") == "2018-07-11T07:00:00.000-05:00"
    assert format_datetime(1531310400000, None, "+0530") == "2018-07-11T17:30:00.000+05:30"


# --- date/time formatting -------------------------------------------------------------------------------------


def test_default_iso_8601_picture():
    assert format_datetime(1) == "1970-01-01T00:00:00.001Z"
    assert format_datetime(0) == "1970-01-01T00:00:00.000Z"
    assert format_datetime(-1) == "1969-12-31T23:59:59.999Z"
    assert format_datetime(-86400000) == "1969-12-31T00:00:00.000Z"


@pytest.mark.parametrize("picture,expected", [
    ("[M01]/[D01]/[Y0001]", "03/23/2018"),
    ("[D1o] [MNn] [Y]", "23rd March 2018"),
    ("[FNn] [D1o] [MN,*-3] [Y0001]", "Friday 23rd MAR 2018"),
    ("[dwo] day of [Y]", "eighty-second day of 2018"),
    ("[h]:[m01][Pn] on [FNn]", "10:33am on Friday"),
    ("[h]:[m01][PN]", "10:33AM"),
    ("Year: <[Y9,999,*]>", "Year: <2,018>"),
    ("[Y,2-2]", "18"),
    ("[[literal]] [Y0001]", "[literal] 2018"),
    ("[C] [E]", "ISO ISO"),
    ("[MI]/[DI]", "III/XXIII"),
    ("[Mi]/[Da]", "iii/w"),
    ("[MW] [Dw]", "THREE twenty-three"),
    ("[Y]-[w]", "2018-4"),
    ("[x]-[X]", "3-2018"),
])
def test_date_components(picture, expected):
    assert format_datetime(NOW, picture) == expected


def test_whitespace_inside_a_marker_is_ignored():
    assert format_datetime(NOW, "[ M01]/[D 01]/[Y00  01]") == "03/23/2018"


def test_iso_week_date_formatting():
    """1 Jan 2005 falls in week 53 of the 2004 ISO week-numbering year, which is what `X` exists to say."""
    assert format_datetime(1104537600000, "[X0001]-W[W01]-[F1]") == "2004-W53-6"
    assert format_datetime(1104537600000, "[Y0001]") == "2005"


# --- date/time parsing ----------------------------------------------------------------------------------------


@pytest.mark.parametrize("text,picture,expected", [
    ("27th 3 1976", "[D1o] [M#1] [Y0001]", 196732800000),
    ("20180205", "[Y0001][M01][D01]", 1517788800000),
    ("MMXVIII-III-XXIII", "[YI]-[MI]-[DI]", 1521763200000),
    ("three hundred and sixty-fifth day of 2018", "[dwo] day of [Y]", 1546214400000),
    ("2020-09-09 08:00:00 +02:00", "[Y0001]-[M01]-[D01] [H01]:[m01]:[s01] [Z]", 1599631200000),
    ("2020-09-09 12:00:00 GMT-5", "[Y0001]-[M01]-[D01] [H01]:[m01]:[s01] [z01]", 1599670800000),
])
def test_parse_full_pictures(text, picture, expected):
    assert parse_datetime(text, picture, NOW) == expected


@pytest.mark.parametrize("text,picture,expected", [
    ("13:45", "[H]:[m]", "2018-03-23T13:45:00.000Z"),
    ("10:33:36", "[H01]:[m01]:[s01]", "2018-03-23T10:33:36.000Z"),
    ("1:33 pm", "[h]:[m] [Pn]", "2018-03-23T13:33:00.000Z"),
    ("12:00 am", "[h]:[m] [Pn]", "2018-03-23T00:00:00.000Z"),
    ("12:00 pm", "[h]:[m] [Pn]", "2018-03-23T12:00:00.000Z"),
    ("March", "[MNn]", "2018-03-01T00:00:00.000Z"),
    ("23 March", "[D] [MNn]", "2018-03-23T00:00:00.000Z"),
    ("2018", "[Y0001]", "2018-01-01T00:00:00.000Z"),
    ("2018-082", "[Y0001]-[d001]", "2018-03-23T00:00:00.000Z"),
])
def test_unspecified_components_default_from_now_then_from_zero(text, picture, expected):
    assert format_datetime(parse_datetime(text, picture, NOW)) == expected


def test_a_picture_that_does_not_match_yields_nothing():
    assert parse_datetime("Hello", "Hello") is None
    assert parse_datetime("irrelevent string", "[Y]-[M]-[D]") is None


def test_iso_8601_without_a_picture():
    assert parse_datetime("2018-02-01T09:42:13.123+0000") == 1517478133123
    assert parse_datetime("2017-10-30") == 1509321600000
    assert parse_datetime("2018") == 1514764800000


# --- format_number --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("value,picture,expected", [
    (12345.6, "#,###.00", "12,345.60"),
    (0, "0.00", "0.00"),
    (-0.5, "0.0", "-0.5"),
    (1234.5678, "#,###.00", "1,234.57"),
    (-1234.5678, "#,###.00", "-1,234.57"),
    (0.14, "#0%", "14%"),
    (0.5, "#0.0%", "50.0%"),
    (0.014, "#0.0‰", "14.0‰"),
    (123.9, "#", "124"),
    (-1, "#0", "-1"),
])
def test_format_number_basics(value, picture, expected):
    assert format_number(value, picture) == expected


@pytest.mark.parametrize("value,expected", [(0.5, "0"), (1.5, "2"), (2.5, "2"), (-2.5, "-2"), (3.5, "4")])
def test_format_number_rounds_half_to_even(value, expected):
    assert format_number(value, "0") == expected


def test_two_sub_pictures_select_by_sign():
    assert format_number(1234.5678, "#,###.00;(#,###.00)") == "1,234.57"
    assert format_number(-1234.5678, "#,###.00;(#,###.00)") == "(1,234.57)"


def test_a_single_sub_picture_glues_a_minus_onto_the_prefix():
    assert format_number(-42, "<#0>") == "-<42>"


@pytest.mark.parametrize("value,picture,expected", [
    (12345678, "#0.0e0", "1.2e7"),
    (0.000012345678, "#0.0e0", "1.2e-5"),
    (0, "#0.0e0", "0.0e0"),
    (1234.5678, "00.000e0", "12.346e2"),
])
def test_exponent_pictures(value, picture, expected):
    assert format_number(value, picture) == expected


def test_decimal_format_overrides():
    assert format_number(2, "AAA.AAA", {"zero-digit": "A"}) == "AAC.AAA"
    assert format_number(1234.5678, "# ##0,00",
                         {"grouping-separator": " ", "decimal-separator": ","}) == "1 234,57"


def test_non_finite_input_is_carried_through_as_nan():
    assert format_number(float("inf"), "#0.0") == "NaN.0"


# --- error codes ----------------------------------------------------------------------------------------------


@pytest.mark.parametrize("code,picture", [
    ("D3080", "#;#;#"),
    ("D3081", "#.0.0"),
    ("D3082", "#0%%"),
    ("D3083", "#0‰‰"),
    ("D3084", "#0%‰"),
    ("D3085", ".e0"),
    ("D3086", "0+.e0"),
    ("D3087", "0,.e0"),
    ("D3088", "0,"),
    ("D3089", "0,,0"),
    ("D3090", "0#.e0"),
    ("D3091", "#0.#0e0"),
    ("D3092", "#0.0e0%"),
    ("D3093", "#0.0e0,0"),
])
def test_format_number_picture_errors(code, picture):
    raises(code, format_number, 20, picture)


def test_a_grouping_separator_override_can_turn_a_valid_picture_invalid():
    """Redefining a separator re-classifies the picture's characters: `,` stops being active and `.` may start."""
    raises("D3086", format_number, 1234.5678, "#,###.00", {"grouping-separator": " "})
    raises("D3091", format_number, 1234.5678, "#,###.00",
           {"grouping-separator": " ", "decimal-separator": ","})


def test_unsupported_numbering_sequence():
    raises("D3130", format_integer, 123456, "α")
    raises("D3130", parse_integer, "50", "#")


def test_mixed_digit_families():
    raises("D3131", format_integer, 12340, "##0０")


def test_unknown_component_specifier():
    raises("D3132", parse_datetime, "2018-05-22", "[Y]-[M]-[q]")


def test_names_on_a_component_that_has_none():
    raises("D3133", format_datetime, 1419940800000, "[YN]-[M]-[D]")
    raises("D3133", parse_datetime, "2018-05-22", "[YN]-[M]-[D]")


def test_timezone_picture_with_an_impossible_digit_count():
    raises("D3134", format_datetime, 1230757500000, "[Z010101t]", "+0530")


def test_unclosed_marker():
    raises("D3135", format_datetime, 1419940800000, "[YN]-[M")


def test_a_gap_between_the_captured_components():
    """Year and day-of-month with no month in between leaves a hole no default can fill."""
    raises("D3136", parse_datetime, "2018-22", "[Y]-[D]")
    raises("D3136", parse_datetime, "5-22 23:59", "[M]-[D] [m]:[s]")


def test_the_iso_week_date_shapes_are_formatting_only():
    """`[X][x][w][F]` and `[X][W][F]` render fine but have no inverse, so parsing them must fail loudly."""
    raises("D3136", parse_datetime, "2018-3-2-5", "[X]-[x]-[w]-[F1]")
    raises("D3136", parse_datetime, "2018-32-5", "[X]-[W]-[F1]")


def test_non_iso_8601_input_without_a_picture():
    raises("D3110", parse_datetime, "foo")
    raises("D3110", parse_datetime, "01-02-2018")
    raises("D3110", parse_datetime, "2018-02-03 11:15:33")


# --- roman numerals and non-finite pictures: an error instead of a hang or a RangeError -----------------------


def test_roman_numerals_repeat_m_up_to_a_million_like_the_reference():
    """Above 3999 XPath leaves the answer to the implementation and jsonata-js repeats M without
    bound. Past a million it throws a RangeError, which is not an error the caller can catch, so
    that is where the value reads as out of range instead."""
    assert format_integer(4999, "I") == "MMMMCMXCIX"
    assert format_integer(5000, "I") == "MMMMM"
    assert format_integer(1000000, "I") == "M" * 1000
    raises("D1001", format_integer, 1000001, "I")
    raises("D1001", format_integer, 1e15, "I")


def test_exponent_picture_of_a_non_finite_value_terminates():
    assert format_number(float("inf"), "0.0e0") == "NaN.0e0"
    assert format_number(float("-inf"), "0.0e0") == "-NaN.0e0"
    assert format_number(float("nan"), "0.0e0") == "-NaN.0e0"


def test_round_half_even_of_an_overflowing_shift_is_nan_not_infinity():
    """Shifting `1e308` two places overflows past `Number.MAX_VALUE`, and jsonata-js's own shift-back
    can't recover a magnitude from `Infinity`'s string form, so the result is NaN either side of zero."""
    assert math.isnan(round_half_even(1e308, 2))
    assert math.isnan(round_half_even(-1e308, 2))
    assert round_half_even(float("inf"), 0) == float("inf"), "precision 0 never shifts, so it stays infinite"


# --- two-digit years inside a four-digit ISO field --------------------------------------------------------------


def test_toMillis_does_not_shift_a_literal_four_digit_year():
    """`Date.parse` never applies `Date.UTC`'s two-digit-year rule: a literal "0050" stays year 50."""
    assert parse_iso_8601("0050-01-01") == -60589296000000
    assert parse_iso_8601("1950-01-01") == -631152000000


# --- unrecognised picture components -----------------------------------------------------------------------


def test_unknown_component_specifier_with_an_explicit_presentation():
    """A component nobody defines is D3132 whether or not it carries a presentation modifier."""
    raises("D3132", format_datetime, 0, "[Q01]")
    raises("D3132", format_datetime, 0, "[Qn]")


# --- garbage input to numbering sequences --------------------------------------------------------------------


def test_unparseable_roman_numeral_is_nan():
    assert math.isnan(parse_integer("lol", "i"))
    assert math.isnan(parse_integer("xyz", "i"))


def test_words_to_number_uses_double_precision():
    result = words_to_number("one trillion trillion")
    assert isinstance(result, float)
    assert result == 1e24


# --- non-finite input never escapes as a bare Python exception ------------------------------------------------
#
# jsonata.py's contract with its callers is that a failed expression raises JsonataError and nothing else;
# stepfunctions.py catches only that type, so any other exception becomes an uncatchable States.Runtime.


def test_format_integer_of_a_non_finite_value_raises_d1001():
    raises("D1001", format_integer, float("nan"), "0")
    raises("D1001", format_integer, float("inf"), "I")
    raises("D1001", format_integer, float("-inf"), "w")


def test_format_datetime_of_a_non_finite_millis_raises_d1001():
    raises("D1001", format_datetime, float("nan"))
    raises("D1001", format_datetime, float("inf"), "[Y]")
    raises("D1001", format_datetime, float("-inf"), "[Y]")


def test_parse_integer_of_a_mismatched_digit_family_is_nan():
    """ASCII digits against a full-width-zero picture have no ASCII code point to shift to."""
    assert math.isnan(parse_integer("12", "００"))


def test_parse_datetime_of_a_mismatched_digit_family_component_is_nan_not_a_crash():
    """A component that parses to NaN stops here instead of reaching date_utc's int() cast."""
    assert math.isnan(parse_datetime("12", "[D００]", NOW))
    assert math.isnan(parse_datetime("12", "[d００]", NOW))


def test_name_lookup_ignores_a_malformed_width_bound():
    """A non-numeric width upper bound parses to NaN; JS's falsy check ignores it, so must this one."""
    assert format_datetime(0, "[MNn,3-abc]") == "January"
