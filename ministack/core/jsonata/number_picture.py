"""XPath 3.1 picture strings for numbers: `format-number`, `format-integer` and `parse-integer`.

Ports jsonata-js `functions.js:773-1163` (`formatNumber`) and `datetime.js:19-476,1044-1128` (the integer picture
engine shared with `$fromMillis`/`$toMillis`). The float arithmetic is reproduced rather than idealised: JSONata's
observable output depends on JS double division, `Math.round`'s half-up bias corrected to banker's rounding, and
`toFixed`'s half-away-from-zero, so each of those is spelled out here instead of using Python's own rounding.
"""

import decimal
import math
import re
from decimal import ROUND_HALF_UP, Decimal

from .values import JsonataError

FEW = ["Zero", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine", "Ten",
       "Eleven", "Twelve", "Thirteen", "Fourteen", "Fifteen", "Sixteen", "Seventeen", "Eighteen", "Nineteen"]
ORDINALS = ["Zeroth", "First", "Second", "Third", "Fourth", "Fifth", "Sixth", "Seventh", "Eighth", "Ninth", "Tenth",
            "Eleventh", "Twelfth", "Thirteenth", "Fourteenth", "Fifteenth", "Sixteenth", "Seventeenth", "Eighteenth",
            "Nineteenth"]
DECADES = ["Twenty", "Thirty", "Forty", "Fifty", "Sixty", "Seventy", "Eighty", "Ninety", "Hundred"]
MAGNITUDES = ["Thousand", "Million", "Billion", "Trillion"]

DECIMAL = "decimal"
LETTERS = "letters"
ROMAN = "roman"
WORDS = "words"
SEQUENCE = "sequence"

UPPER = "upper"
LOWER = "lower"
TITLE = "title"

# First codepoint of each Unicode decimal-digit family JSONata recognises in a picture (datetime.js:319).
DIGIT_FAMILIES = [0x30, 0x0660, 0x06F0, 0x07C0, 0x0966, 0x09E6, 0x0A66, 0x0AE6, 0x0B66, 0x0BE6, 0x0C66, 0x0CE6,
                  0x0D66, 0x0DE6, 0x0E50, 0x0ED0, 0x0F20, 0x1040, 0x1090, 0x17E0, 0x1810, 0x1946, 0x19D0, 0x1A80,
                  0x1A90, 0x1B50, 0x1BB0, 0x1C40, 0x1C50, 0xA620, 0xA8D0, 0xA900, 0xA9D0, 0xA9F0, 0xAA50, 0xABF0,
                  0xFF10]

OPTIONAL_DIGIT = "#"
ASCII_ZERO = 0x30

ROMAN_NUMERALS = [(1000, "m"), (900, "cm"), (500, "d"), (400, "cd"), (100, "c"), (90, "xc"), (50, "l"), (40, "xl"),
                  (10, "x"), (9, "ix"), (5, "v"), (4, "iv"), (1, "i")]
ROMAN_VALUES = {"M": 1000, "D": 500, "C": 100, "L": 50, "X": 10, "V": 5, "I": 1}

# XPath's `format-integer`, which JSONata cites here, calls anything above 3999 implementation
# defined, and jsonata-js reads that as an unbounded run of M: 1000000 renders 1000 of them. It
# recurses once per numeral, so somewhere past this it throws a RangeError instead, and a RangeError
# is not a JSONata error the caller can catch. A million is where its answer is still the reference's
# and above it the value is out of range, which is a failure a state can Catch.
ROMAN_LIMIT = 1_000_000

_LEADING_INTEGER = re.compile(r"^[ \t\n\r\f\v]*([+-]?[0-9]+)")


# --- JS numeric primitives ------------------------------------------------------------------------------------


def js_parse_int(text):
    """`parseInt` — leading integer prefix, NaN when there isn't one."""
    match = _LEADING_INTEGER.match(text)
    return int(match.group(1)) if match else float("nan")


def js_round(value):
    """`Math.round` — nearest integer, ties toward +Infinity, without the naive `floor(x + 0.5)` misfires.

    A non-finite value passes through unchanged, the way `Math.round` leaves `Infinity`/`NaN` alone
    instead of `math.floor` raising `OverflowError` on them.
    """
    if not math.isfinite(value):
        return value
    floor = math.floor(value)
    return floor if value - floor < 0.5 else floor + 1


def js_number_to_string(value):
    """`'' + value` for an integral Number: JS switches to exponential notation at 1e21."""
    if not math.isfinite(value):
        return "NaN" if math.isnan(value) else ("Infinity" if value > 0 else "-Infinity")
    if abs(value) >= 10 ** 21:
        digits, _, exponent = repr(float(value)).partition("e")
        digits = digits.rstrip("0").rstrip(".") if "." in digits else digits
        return f"{digits}e{'+' if not exponent.startswith('-') else ''}{int(exponent)}"
    return str(int(value))


def _shift_decimal_point(value, places):
    """`$round`'s string-based decimal shift, which avoids the error that `value * 10**places` introduces."""
    if not math.isfinite(value):
        # `+('Infinity' + 'e2')` is NaN in JS, and jsonata-js carries that NaN into the formatted output.
        return float("nan")
    digits, _, exponent = repr(float(value)).partition("e")
    return float(f"{digits}e{int(exponent) + places if exponent else places}")


def round_half_even(value, precision=0):
    """`$round` (functions.js:1293-1330): `Math.round` then a pull-down to even on exact ties.

    The shift back to `precision` runs even when rounding left an infinite value: `_shift_decimal_point`
    can't recover a magnitude from an infinity's string form, so — like jsonata-js — the result becomes
    NaN rather than staying infinite.
    """
    shifted = _shift_decimal_point(value, precision) if precision else float(value)
    result = float(js_round(shifted))
    if abs(result - shifted) == 0.5 and abs(math.fmod(result, 2)) == 1:
        result -= 1
    return _shift_decimal_point(result, -precision) if precision else result


def to_fixed(value, places):
    """`Number.prototype.toFixed` — exact decimal expansion of the double, ties away from zero.

    `Decimal`'s default 28-digit context is too small once `places` grows past a handful of
    fractional digits, and its default `str()` switches to scientific notation for a small exponent;
    `toFixed` never does either, so the context is widened to fit and the result is rendered plain.
    """
    if not math.isfinite(value) or abs(value) >= 10 ** 21:
        return js_number_to_string(value)
    quantum = Decimal(1).scaleb(-places)
    integer_digits = len(str(abs(int(value)))) if value else 1
    with decimal.localcontext() as context:
        context.prec = integer_digits + places + 2
        rendered = Decimal(value).quantize(quantum, rounding=ROUND_HALF_UP)
    return format(rendered, "f")


# --- English number words -------------------------------------------------------------------------------------


def _spell(number, joined_to_previous, ordinal):
    """`numberToWords`'s `lookup` (datetime.js:33-71) — British "and" conventions, magnitudes capped at trillion."""
    if number <= 19:
        lead = " and " if joined_to_previous else ""
        return lead + (ORDINALS[int(number)] if ordinal else FEW[int(number)])
    if number < 100:
        return _spell_tens(number, joined_to_previous, ordinal)
    if number < 1000:
        return _spell_hundreds(number, joined_to_previous, ordinal)
    return _spell_magnitude(number, joined_to_previous, ordinal)


def _spell_tens(number, joined_to_previous, ordinal):
    tens = math.floor(number / 10)
    remainder = math.fmod(number, 10)
    words = (" and " if joined_to_previous else "") + DECADES[int(tens) - 2]
    if remainder > 0:
        return words + "-" + _spell(remainder, False, ordinal)
    return words[:-1] + "ieth" if ordinal else words


def _spell_hundreds(number, joined_to_previous, ordinal):
    hundreds = math.floor(number / 100)
    remainder = math.fmod(number, 100)
    words = (", " if joined_to_previous else "") + FEW[int(hundreds)] + " Hundred"
    if remainder > 0:
        return words + _spell(remainder, True, ordinal)
    return words + "th" if ordinal else words


def _spell_magnitude(number, joined_to_previous, ordinal):
    # Above 1e15 JSONata clamps to the largest known magnitude and stacks it rather than inventing a word.
    magnitude = math.floor(math.log10(float(number)) / 3)
    magnitude = min(magnitude, len(MAGNITUDES))
    factor = 10.0 ** (magnitude * 3)
    mantissa = math.floor(number / factor)
    remainder = number - mantissa * factor
    words = (", " if joined_to_previous else "") + _spell(mantissa, False, False) + " " + MAGNITUDES[magnitude - 1]
    if remainder > 0:
        return words + _spell(remainder, True, ordinal)
    return words + "th" if ordinal else words


def number_to_words(value, ordinal):
    return _spell(value, False, ordinal)


def _build_word_values():
    """The word -> value table, in jsonata-js insertion order: the parse regex alternates over these keys.

    Values are `float`, not `int`: JSONata has one number type, the IEEE 754 double, and
    `words_to_number` must accumulate the way `wordsToNumber` does in JS, rounding error included,
    rather than with Python's arbitrary-precision integers.
    """
    table = {}
    for index, word in enumerate(FEW):
        table[word.lower()] = float(index)
    for index, word in enumerate(ORDINALS):
        table[word.lower()] = float(index)
    for index, word in enumerate(DECADES):
        lower = word.lower()
        table[lower] = float((index + 2) * 10)
        table[lower[:-1] + "ieth"] = table[lower]
    table["hundredth"] = 100.0
    for index, word in enumerate(MAGNITUDES):
        lower = word.lower()
        table[lower] = float(10 ** ((index + 1) * 3))
        table[lower + "th"] = table[lower]
    return table


WORD_VALUES = _build_word_values()


def words_to_number(text):
    """`wordsToNumber` (datetime.js:102-120): any value >= 100 multiplies the segment being accumulated."""
    parts = re.split(r",\s|\sand\s|[\s\\\-]", text)
    segments = [0.0]
    for part in parts:
        value = WORD_VALUES.get(part)
        if value is None:
            return float("nan")
        if value < 100:
            top = segments.pop()
            if top >= 1000:
                segments.append(top)
                top = 0.0
            segments.append(top + value)
        else:
            segments.append(segments.pop() * value)
    return sum(segments)


# --- Roman numerals and spreadsheet letters -------------------------------------------------------------------


def decimal_to_roman(value):
    if value > ROMAN_LIMIT:
        raise JsonataError("D1001", value=value)
    numeral = ""
    for amount, letters in ROMAN_NUMERALS:
        count, value = divmod(value, amount)
        numeral += letters * count
    return numeral


def roman_to_decimal(roman):
    """A character outside `ROMAN_VALUES` reads as JS `undefined`: it poisons every arithmetic op after
    it into NaN, the same way `romanValues[digit]` being `undefined` does in jsonata-js."""
    total = 0
    largest = 1
    for letter in reversed(roman):
        value = ROMAN_VALUES.get(letter, float("nan"))
        if value < largest:
            total -= value
        else:
            largest = value
            total += value
    return total


def decimal_to_letters(value, first_letter):
    """Bijective base-26: there is no zero letter, so Z is followed by AA."""
    base = ord(first_letter)
    letters = []
    value = int(value)
    while value > 0:
        letters.insert(0, chr((value - 1) % 26 + base))
        value = (value - 1) // 26
    return "".join(letters)


def letters_to_decimal(letters, first_letter):
    base = ord(first_letter)
    total = 0
    for index, letter in enumerate(reversed(letters)):
        total += (ord(letter) - base + 1) * 26 ** index
    return total


# --- Integer picture ------------------------------------------------------------------------------------------


class IntegerPicture:
    """An analysed `format-integer` picture: which numbering it selects and, for decimals, how digits are laid out."""

    def __init__(self):
        self.primary = DECIMAL
        self.case = LOWER
        self.ordinal = False
        self.token = None
        self.zero_code = ASCII_ZERO
        self.mandatory_digits = None
        self.optional_digits = 0
        self.regular = False
        self.grouping_separators = []
        self.parse_width = None


def _digit_family_of(codepoint):
    for family in DIGIT_FAMILIES:
        if family <= codepoint <= family + 9:
            return family
    return None


def _regular_positions(positions):
    """The common interval between grouping positions, or 0 when the spacing is irregular."""
    if not positions:
        return 0
    factor = positions[0]
    for position in positions[1:]:
        factor = math.gcd(factor, position)
    if factor == 0:
        return 0
    for index in range(1, len(positions) + 1):
        if index * factor not in positions:
            return 0
    return factor


def _regular_repeat(separators):
    """The grouping interval if every separator is the same character at a regular spacing, else 0."""
    if not separators:
        return 0
    character = separators[0][1]
    if any(separator[1] != character for separator in separators):
        return 0
    return _regular_positions([separator[0] for separator in separators])


def _analyse_decimal_picture(picture, primary_format):
    zero_code = None
    mandatory_digits = 0
    optional_digits = 0
    separators = []
    position = 0
    # Right-to-left, so a separator's position is its distance in digits from the least significant end.
    for character in reversed(primary_format):
        codepoint = ord(character)
        family = _digit_family_of(codepoint)
        if family is not None:
            mandatory_digits += 1
            position += 1
            if zero_code is None:
                zero_code = family
            elif family != zero_code:
                raise JsonataError("D3131")
            continue
        if character == OPTIONAL_DIGIT:
            position += 1
            optional_digits += 1
        else:
            separators.append((position, character))

    if mandatory_digits == 0:
        # No mandatory digit at all: the spec calls this a numbering sequence, and this implementation has none.
        picture.primary = SEQUENCE
        picture.token = primary_format
        return

    picture.primary = DECIMAL
    picture.zero_code = zero_code
    picture.mandatory_digits = mandatory_digits
    picture.optional_digits = optional_digits
    regular = _regular_repeat(separators)
    if regular > 0:
        picture.regular = True
        picture.grouping_separators = (regular, separators[0][1])
    else:
        picture.regular = False
        picture.grouping_separators = separators


_NAMED_FORMATS = {"A": (LETTERS, UPPER), "a": (LETTERS, LOWER), "I": (ROMAN, UPPER), "i": (ROMAN, LOWER),
                  "W": (WORDS, UPPER), "Ww": (WORDS, TITLE), "w": (WORDS, LOWER)}


def analyse_integer_picture(text):
    """`analyseIntegerPicture` (datetime.js:326-476). The last `;` splits off the format modifier (`o` = ordinal)."""
    picture = IntegerPicture()
    semicolon = text.rfind(";")
    if semicolon == -1:
        primary_format = text
    else:
        primary_format = text[:semicolon]
        picture.ordinal = text[semicolon + 1:semicolon + 2] == "o"

    named = _NAMED_FORMATS.get(primary_format)
    if named is not None:
        picture.primary, picture.case = named
        return picture

    _analyse_decimal_picture(picture, primary_format)
    return picture


def _render_decimal(value, picture):
    rendered = js_number_to_string(value)
    if picture.mandatory_digits > len(rendered):
        rendered = "0" * (picture.mandatory_digits - len(rendered)) + rendered
    if picture.zero_code != ASCII_ZERO:
        rendered = "".join(chr(ord(digit) + picture.zero_code - ASCII_ZERO) for digit in rendered)
    rendered = _insert_grouping(rendered, picture)
    if picture.ordinal:
        rendered += _ordinal_suffix(rendered)
    return rendered


def _insert_grouping(rendered, picture):
    if picture.regular:
        interval, character = picture.grouping_separators
        for group in range((len(rendered) - 1) // interval, 0, -1):
            cut = len(rendered) - group * interval
            rendered = rendered[:cut] + character + rendered[cut:]
        return rendered
    # Least significant separator first, because each insertion shifts every position to its left.
    for position, character in reversed(picture.grouping_separators):
        cut = len(rendered) - position
        rendered = rendered[:cut] + character + rendered[cut:]
    return rendered


def _ordinal_suffix(rendered):
    suffix = {"1": "st", "2": "nd", "3": "rd"}.get(rendered[-1])
    if not suffix or (len(rendered) > 1 and rendered[-2] == "1"):
        return "th"
    return suffix


def render_integer(value, picture):
    """`_formatInteger` (datetime.js:246-316). The sign is prepended last, so `-5` in roman numerals is `-v`."""
    negative = value < 0
    value = abs(value)
    if picture.primary == LETTERS:
        rendered = decimal_to_letters(value, "A" if picture.case == UPPER else "a")
    elif picture.primary == ROMAN:
        rendered = decimal_to_roman(value)
        if picture.case == UPPER:
            rendered = rendered.upper()
    elif picture.primary == WORDS:
        rendered = number_to_words(value, picture.ordinal)
        if picture.case == UPPER:
            rendered = rendered.upper()
        elif picture.case == LOWER:
            rendered = rendered.lower()
    elif picture.primary == DECIMAL:
        rendered = _render_decimal(value, picture)
    else:
        raise JsonataError("D3130", value=picture.token)
    return "-" + rendered if negative else rendered


def format_integer(value, picture):
    """`$formatInteger`. Floors the signed value first, so -3.2 formats as -4."""
    if not math.isfinite(value):
        raise JsonataError("D1001", value=value)
    return render_integer(math.floor(value), analyse_integer_picture(picture))


class IntegerMatcher:
    """The regex an integer picture matches, plus the closure that turns a match back into a number."""

    def __init__(self, regex, parse):
        self.regex = regex
        self.parse = parse


def _decimal_matcher(picture):
    regex = "[0-9]" + (f"{{{picture.parse_width}}}" if picture.parse_width else "+")
    if picture.ordinal:
        regex += "(?:th|st|nd|rd)"

    def parse(text):
        digits = text[:-2] if picture.ordinal else text
        if picture.regular:
            # jsonata-js strips a literal comma here regardless of the picture's own separator character.
            digits = digits.replace(",", "")
        else:
            for _, character in picture.grouping_separators:
                digits = digits.replace(character, "")
        if picture.zero_code != ASCII_ZERO:
            # A parsed digit outside the picture's own digit family (e.g. ASCII "1" against a
            # full-width-zero picture) has no ASCII equivalent to shift to; jsonata-js's own
            # `String.fromCodePoint` throws here too, uncaught, so an unparseable value is the
            # sane outcome to model, same as elsewhere in this parser.
            shifted = []
            for digit in digits:
                code_point = ord(digit) - picture.zero_code + ASCII_ZERO
                if not 0 <= code_point <= 0x10FFFF:
                    return float("nan")
                shifted.append(chr(code_point))
            digits = "".join(shifted)
        return js_parse_int(digits)

    return IntegerMatcher(regex, parse)


def integer_matcher(picture):
    """`generateRegex`'s integer branch (datetime.js:1044-1104)."""
    upper = picture.case == UPPER
    if picture.primary == LETTERS:
        return IntegerMatcher("[A-Z]+" if upper else "[a-z]+",
                              lambda text: letters_to_decimal(text, "A" if upper else "a"))
    if picture.primary == ROMAN:
        return IntegerMatcher("[MDCLXVI]+" if upper else "[mdclxvi]+",
                              lambda text: roman_to_decimal(text if upper else text.upper()))
    if picture.primary == WORDS:
        alternatives = list(WORD_VALUES.keys()) + ["and", "[\\-, ]"]
        return IntegerMatcher("(?:" + "|".join(alternatives) + ")+", lambda text: words_to_number(text.lower()))
    if picture.primary == DECIMAL:
        return _decimal_matcher(picture)
    raise JsonataError("D3130", value=picture.token)


def parse_integer(text, picture):
    """`$parseInteger`. Like jsonata-js it does not check the input against the generated regex first."""
    return integer_matcher(analyse_integer_picture(picture)).parse(text)


# --- Decimal (format-number) picture --------------------------------------------------------------------------


DECIMAL_FORMAT_DEFAULTS = {
    "decimal-separator": ".",
    "grouping-separator": ",",
    "exponent-separator": "e",
    "infinity": "Infinity",
    "minus-sign": "-",
    "NaN": "NaN",
    "percent": "%",
    "per-mille": "‰",
    "zero-digit": "0",
    "digit": "#",
    "pattern-separator": ";",
}


class SubPicture:
    """One side of a `;`-separated `format-number` picture, split into its prefix, numeric parts and suffix."""

    def __init__(self, text, properties, active_chars):
        exponent_separator = properties["exponent-separator"]
        self.subpicture = text
        self.prefix = _leading_passive_run(text, active_chars, exponent_separator)
        self.suffix = _trailing_passive_run(text, active_chars, exponent_separator)
        self.active_part = text[len(self.prefix):len(text) - len(self.suffix)]

        exponent_at = text.find(exponent_separator, len(self.prefix))
        if exponent_at == -1 or exponent_at > len(text) - len(self.suffix):
            self.mantissa_part = self.active_part
            self.exponent_part = None
        else:
            # jsonata-js slices the active part with a subpicture-relative offset; kept so output matches.
            self.mantissa_part = self.active_part[:exponent_at]
            self.exponent_part = self.active_part[exponent_at + 1:]

        decimal_at = self.mantissa_part.find(properties["decimal-separator"])
        if decimal_at == -1:
            self.integer_part = self.mantissa_part
            self.fractional_part = self.suffix
        else:
            self.integer_part = self.mantissa_part[:decimal_at]
            self.fractional_part = self.mantissa_part[decimal_at + 1:]


def _leading_passive_run(text, active_chars, exponent_separator):
    for index, character in enumerate(text):
        if character in active_chars and character != exponent_separator:
            return text[:index]
    return ""


def _trailing_passive_run(text, active_chars, exponent_separator):
    for index in range(len(text) - 1, -1, -1):
        character = text[index]
        if character in active_chars and character != exponent_separator:
            return text[index + 1:]
    return ""


def _char_at(text, index):
    """`String.prototype.charAt` — out of range is the empty string, never a wrap-around."""
    return text[index] if 0 <= index < len(text) else ""


def _subpicture_faults(parts, properties, digit_family, active_chars):
    """F&O 4.7.3 (functions.js:870-941), in the order jsonata-js checks the rules."""
    subpicture = parts.subpicture
    digit = properties["digit"]
    grouping = properties["grouping-separator"]
    percent = properties["percent"]
    per_mille = properties["per-mille"]
    decimal_at = subpicture.find(properties["decimal-separator"])
    integer_optional_at = parts.integer_part.find(digit)
    fraction_optional_at = parts.fractional_part.rfind(digit)
    exponent = parts.exponent_part

    return [
        ("D3081", decimal_at != subpicture.rfind(properties["decimal-separator"])),
        ("D3082", subpicture.find(percent) != subpicture.rfind(percent)),
        ("D3083", subpicture.find(per_mille) != subpicture.rfind(per_mille)),
        ("D3084", percent in subpicture and per_mille in subpicture),
        ("D3085", not any(char in digit_family or char == digit for char in parts.mantissa_part)),
        ("D3086", any(char not in active_chars for char in parts.active_part)),
        ("D3087", decimal_at != -1 and grouping in (_char_at(subpicture, decimal_at - 1),
                                                    _char_at(subpicture, decimal_at + 1))),
        ("D3088", decimal_at == -1 and parts.integer_part.endswith(grouping)),
        ("D3089", grouping + grouping in subpicture),
        ("D3090", integer_optional_at != -1
                  and any(char in digit_family for char in parts.integer_part[:integer_optional_at])),
        ("D3091", fraction_optional_at != -1
                  and any(char in digit_family for char in parts.fractional_part[fraction_optional_at:])),
        ("D3092", bool(exponent) and (percent in subpicture or per_mille in subpicture)),
        ("D3093", exponent is not None
                  and (not exponent or any(char not in digit_family for char in exponent))),
    ]


def _validate_subpicture(parts, properties, digit_family, active_chars):
    """Each rule overwrites a single error slot in jsonata-js, so the last one that fails is the one reported."""
    reported = None
    for code, failed in _subpicture_faults(parts, properties, digit_family, active_chars):
        if failed:
            reported = code
    if reported:
        raise JsonataError(reported)


class DecimalFormat:
    """F&O 4.7.4 analysis of one sub-picture: how many digits are mandatory, where the grouping falls."""

    def __init__(self, parts, properties, digit_family):
        digit = properties["digit"]
        grouping = properties["grouping-separator"]

        def counted_positions(part, to_left=False):
            positions = []
            at = part.find(grouping)
            while at != -1:
                span = part[:at] if to_left else part[at:]
                positions.append(sum(1 for char in span if char in digit_family or char == digit))
                # jsonata-js advances through the integer part whichever part it was handed; kept verbatim.
                at = parts.integer_part.find(grouping, at + 1)
            return positions

        self.integer_grouping_positions = counted_positions(parts.integer_part)
        self.regular_grouping = _regular_positions(self.integer_grouping_positions)
        self.fractional_grouping_positions = counted_positions(parts.fractional_part, to_left=True)

        self.minimum_integer_digits = sum(1 for char in parts.integer_part if char in digit_family)
        self.scaling_factor = self.minimum_integer_digits
        self.minimum_fraction_digits = sum(1 for char in parts.fractional_part if char in digit_family)
        self.maximum_fraction_digits = sum(1 for char in parts.fractional_part
                                           if char in digit_family or char == digit)

        has_exponent = parts.exponent_part is not None
        if self.minimum_integer_digits == 0 and self.maximum_fraction_digits == 0:
            if has_exponent:
                self.minimum_fraction_digits = 1
                self.maximum_fraction_digits = 1
            else:
                self.minimum_integer_digits = 1
        if has_exponent and self.minimum_integer_digits == 0 and digit in parts.integer_part:
            self.minimum_integer_digits = 1
        if self.minimum_integer_digits == 0 and self.minimum_fraction_digits == 0:
            self.minimum_fraction_digits = 1

        self.minimum_exponent_digits = 0
        if has_exponent:
            self.minimum_exponent_digits = sum(1 for char in parts.exponent_part if char in digit_family)

        self.prefix = parts.prefix
        self.suffix = parts.suffix
        self.picture = parts.subpicture

    def copy(self):
        clone = DecimalFormat.__new__(DecimalFormat)
        clone.__dict__.update(self.__dict__)
        return clone


def _scale_for_percent(value, chosen, properties):
    if properties["percent"] in chosen.picture:
        return value * 100
    if properties["per-mille"] in chosen.picture:
        return value * 1000
    return value


def _split_mantissa_exponent(adjusted, chosen):
    """F&O bullet 5. Zero keeps an exponent of zero, which is also what stops the normalisation loop spinning.

    A non-finite mantissa is left at exponent zero too: `abs(mantissa) > maximum` never stops being
    true for `Infinity`, so dividing it by ten forever would spin the same way `Math.abs(Infinity) >
    maxMantissa` does in jsonata-js before its own shift-back collapses it to NaN (see `round_half_even`).
    """
    if chosen.minimum_exponent_digits == 0:
        return adjusted, None
    maximum = 10 ** chosen.scaling_factor
    minimum = 10 ** (chosen.scaling_factor - 1)
    mantissa = adjusted
    exponent = 0
    if math.isfinite(mantissa) and mantissa != 0:
        while abs(mantissa) < minimum:
            mantissa *= 10
            exponent -= 1
        while abs(mantissa) > maximum:
            mantissa /= 10
            exponent += 1
    return mantissa, exponent


def _digits_string(value, places, zero_digit, digit_family):
    text = to_fixed(abs(value), places)
    if zero_digit != "0":
        text = "".join(digit_family[ord(char) - ASCII_ZERO] if "0" <= char <= "9" else char for char in text)
    return text


def format_number(value, picture, options=None):
    """`$formatNumber` — F&O 4.7.3/4.7.4 plus the 14-bullet format algorithm (functions.js:773-1163)."""
    properties = dict(DECIMAL_FORMAT_DEFAULTS)
    if options:
        properties.update(options)

    zero_digit = properties["zero-digit"]
    digit_family = [chr(ord(zero_digit[0]) + offset) for offset in range(10)]
    active_chars = set(digit_family) | {properties["decimal-separator"], properties["exponent-separator"],
                                        properties["grouping-separator"], properties["digit"],
                                        properties["pattern-separator"]}

    subpictures = picture.split(properties["pattern-separator"])
    if len(subpictures) > 2:
        raise JsonataError("D3080")

    parts = [SubPicture(text, properties, active_chars) for text in subpictures]
    for entry in parts:
        _validate_subpicture(entry, properties, digit_family, active_chars)

    formats = [DecimalFormat(entry, properties, digit_family) for entry in parts]
    if len(formats) == 1:
        negative = formats[0].copy()
        negative.prefix = properties["minus-sign"] + negative.prefix
        formats.append(negative)

    chosen = formats[0] if value >= 0 else formats[1]
    adjusted = _scale_for_percent(value, chosen, properties)
    mantissa, exponent = _split_mantissa_exponent(adjusted, chosen)
    rounded = round_half_even(mantissa, chosen.maximum_fraction_digits)
    return _assemble(rounded, exponent, chosen, properties, digit_family)


def _normalise_decimal_separator(text, decimal_separator):
    """`to_fixed` always uses `.`; the picture's own separator replaces it, added if there wasn't one."""
    if "." in text:
        return text.replace(".", decimal_separator)
    return text + decimal_separator


def _strip_zero_padding(text, zero_digit):
    """Undo `to_fixed`'s zero-padding so the picture's own minimum-width rules can reapply it."""
    while text[:1] == zero_digit:
        text = text[1:]
    while text[-1:] == zero_digit:
        text = text[:-1]
    return text


def _pad_to_minimum_digits(text, decimal_separator, zero_digit, chosen):
    decimal_at = text.find(decimal_separator)
    pad_left = chosen.minimum_integer_digits - decimal_at
    pad_right = chosen.minimum_fraction_digits - (len(text) - decimal_at - 1)
    text = (zero_digit * pad_left if pad_left > 0 else "") + text
    return text + (zero_digit * pad_right if pad_right > 0 else "")


def _insert_integer_grouping(text, decimal_separator, grouping_separator, chosen):
    decimal_at = text.find(decimal_separator)
    if chosen.regular_grouping > 0:
        for group in range(1, (decimal_at - 1) // chosen.regular_grouping + 1):
            cut = decimal_at - group * chosen.regular_grouping
            text = text[:cut] + grouping_separator + text[cut:]
        return text
    for position in chosen.integer_grouping_positions:
        cut = decimal_at - position
        text = text[:cut] + grouping_separator + text[cut:]
        decimal_at += 1
    return text


def _insert_fractional_grouping(text, decimal_separator, grouping_separator, chosen):
    decimal_at = text.find(decimal_separator)
    for position in chosen.fractional_grouping_positions:
        cut = position + decimal_at + 1
        text = text[:cut] + grouping_separator + text[cut:]
    return text


def _trim_unwanted_decimal_separator(text, decimal_separator, chosen):
    """Drop the separator `_normalise_decimal_separator` added if the picture never asked for one."""
    decimal_at = text.find(decimal_separator)
    if decimal_separator not in chosen.picture or decimal_at == len(text) - 1:
        return text[:-1]
    return text


def _append_exponent(text, exponent, chosen, properties, zero_digit, digit_family):
    if exponent is None:
        return text
    digits = _digits_string(exponent, 0, zero_digit, digit_family)
    pad_left = chosen.minimum_exponent_digits - len(digits)
    if pad_left > 0:
        digits = zero_digit * pad_left + digits
    sign = properties["minus-sign"] if exponent < 0 else ""
    return text + properties["exponent-separator"] + sign + digits


def _assemble(rounded, exponent, chosen, properties, digit_family):
    zero_digit = properties["zero-digit"]
    decimal_separator = properties["decimal-separator"]
    grouping_separator = properties["grouping-separator"]

    text = _digits_string(rounded, chosen.maximum_fraction_digits, zero_digit, digit_family)
    text = _normalise_decimal_separator(text, decimal_separator)
    text = _strip_zero_padding(text, zero_digit)
    text = _pad_to_minimum_digits(text, decimal_separator, zero_digit, chosen)
    text = _insert_integer_grouping(text, decimal_separator, grouping_separator, chosen)
    text = _insert_fractional_grouping(text, decimal_separator, grouping_separator, chosen)
    text = _trim_unwanted_decimal_separator(text, decimal_separator, chosen)
    text = _append_exponent(text, exponent, chosen, properties, zero_digit, digit_family)

    return chosen.prefix + text + chosen.suffix
