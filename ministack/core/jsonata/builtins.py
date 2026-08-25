"""The builtin function library `$name(...)` resolves against.

`BUILTINS` maps each `$name` to the callable that implements it. Every callable carries the
`signature` the reference implementation binds it under (jsonata.js:1909-1971) and the `arity`
the higher-order functions read to decide how many arguments a callback wants, so the evaluator
validates a call and adapts a callback without knowing anything about the individual function.

The 64 in-scope JSONata 2.0.6 builtins are here, plus the five names Step Functions adds
(`$partition`, `$range`, `$hash`, `$uuid`, `$parse`) and its seeded overload of `$random`, for 69
in `BUILTINS`. `$eval` is absent: Step Functions does not ship it. The picture-string grammars of `$formatNumber`,
`$formatInteger`, `$parseInteger`, `$fromMillis` and `$toMillis` live in `number_picture` and
`datetime_picture`; what is here is the argument handling around them.
"""

import base64
import contextlib
import contextvars
import hashlib
import inspect
import json
import math
import random
import re
import time
import urllib.parse
import uuid

from .datetime_picture import format_datetime, parse_datetime
from .number_picture import format_integer, format_number, js_parse_int, parse_integer, round_half_even
from .parser import Signature
from .rendering import js_number_text
from .values import UNDEFINED, JsonataError, Sequence, is_sequence

BUILTINS = {}

RADIX_DIGITS = "0123456789abcdefghijklmnopqrstuvwxyz"

# encodeURI keeps the reserved set intact, encodeURIComponent does not; urllib.parse.quote agrees
# with neither by default, so both sets are spelled out (functions.js:596-641).
URL_SAFE = "-_.!~*'();,/?:@&=+$#"
URL_COMPONENT_SAFE = "-_.!~*'()"
URL_RESERVED = ";/?:@&=+$,#"

HASH_ALGORITHMS = {
    "MD5": hashlib.md5,
    "SHA-1": hashlib.sha1,
    "SHA-256": hashlib.sha256,
    "SHA-384": hashlib.sha384,
    "SHA-512": hashlib.sha512,
}

_TRIMMED = re.compile(r"[ \t\n\r]+")
_DECIMAL_LITERAL = re.compile(r"-?[0-9]+(\.[0-9]+)?([Ee][-+]?[0-9]+)?")
_RADIX_LITERAL = re.compile(r"(0[xX][0-9A-Fa-f]+)|(0[oO][0-7]+)|(0[bB][0-1]+)")
_TRUNCATED_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")
_ESCAPE_RUN = re.compile(r"(?:%[0-9A-Fa-f]{2})+")


def _builtin(name, signature):
    """Registers `$name`, carrying the signature the evaluator validates a call against."""
    def register(function):
        function.signature = Signature(signature, 0)
        # The bare name a T0410/T0411/T0412 validation error blames, e.g. "min" for `$min`.
        function.signature.token = name[1:] if name.startswith("$") else name
        function.arity = required_parameters(function)
        BUILTINS[name] = function
        return function
    return register


def required_parameters(function):
    """What JavaScript reports as a function's `.length`: the parameters without a default.

    `_builtin` stamps it onto every builtin as its `arity`, and `evaluator.arity` falls back to it
    for a callable that carries no `arity` of its own.
    """
    parameters = inspect.signature(function).parameters.values()
    return sum(1 for parameter in parameters
               if parameter.default is inspect.Parameter.empty
               and parameter.kind in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD))


# ---- what every function needs -----------------------------------------------------------------

def _apply(function, args):
    """Calls a function value. Imported here rather than at module level to break the cycle."""
    from .evaluator import apply
    return apply(function, args)


def _callback_args(function, value, position, whole):
    """`hofFuncArgs` (functions.js:1467-1478): a callback is only handed what it declared."""
    from .evaluator import arity
    declared = arity(function)
    args = [value]
    if declared >= 2:
        args.append(position)
    if declared >= 3:
        args.append(whole)
    return args


def _is_number(value):
    """A JSON number. `True` is an `int` in Python and must never pass for one."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _numeric(value):
    """`isNumeric` (utils.js:15-27): NaN is not numeric and Infinity is D1001, never a value."""
    if not _is_number(value):
        return False
    if math.isnan(value):
        return False
    if math.isinf(value):
        raise JsonataError("D1001", value=value)
    return True


def _plain(value):
    """A number as JSONata holds it: JavaScript has one numeric type, so 2.0 and 2 are the same 2."""
    if isinstance(value, float) and value.is_integer() and abs(value) < 1e21:
        return int(value)
    return value


def _given(value):
    """An optional argument as the picture-string modules take it: absent is `None`, not UNDEFINED."""
    return None if value is UNDEFINED else value


def _js_integer(value):
    """`ToIntegerOrInfinity`, which truncates toward zero where Python's `//` would floor."""
    return math.trunc(value)


def _deep_equals(left, right):
    """Structural equality, with `true` kept apart from `1`."""
    if isinstance(left, bool) != isinstance(right, bool):
        return False
    return left == right


# ---- $string, and the JSON text JavaScript would have produced ----------------------------------

def _fifteen_significant(value):
    """`Number(val.toPrecision(15))`, the step that turns 78.8/2 back into 39.4."""
    if isinstance(value, int) or float(value).is_integer():
        return value
    return float(f"{value:.15g}")


def _json_text(value, indent, depth):
    """`JSON.stringify` with JSONata's replacer: functions become `""`, numbers lose float noise."""
    if value is UNDEFINED or value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if callable(value):
        return '""'
    if _is_number(value):
        # `isNumeric` is what rejects Infinity here, which is why a nested one is D1001 and not D3001.
        return js_number_text(_fifteen_significant(value)) if _numeric(value) else "null"
    if isinstance(value, list):
        return _json_block("[]", [_json_text(item, indent, depth + 1) for item in value], indent, depth)
    # An `UNDEFINED` value drops its key, the way `JSON.stringify` drops any object property whose
    # value is `undefined`.
    entries = [f"{json.dumps(key, ensure_ascii=False)}:{' ' if indent else ''}{_json_text(item, indent, depth + 1)}"
               for key, item in value.items() if item is not UNDEFINED]
    return _json_block("{}", entries, indent, depth)


def _json_block(brackets, entries, indent, depth):
    if not entries:
        return brackets
    if not indent:
        return brackets[0] + ",".join(entries) + brackets[1]
    inner = " " * (indent * (depth + 1))
    body = ",\n".join(inner + entry for entry in entries)
    return f"{brackets[0]}\n{body}\n{' ' * (indent * depth)}{brackets[1]}"


@_builtin("$string", "<x-b?:s>")
def _string(value, prettify=UNDEFINED):
    if value is UNDEFINED:
        return UNDEFINED
    if isinstance(value, str):
        return value
    if callable(value):
        return ""
    if _is_number(value) and not math.isfinite(value):
        raise JsonataError("D3001", value=value)
    if isinstance(value, Sequence) and value.outer_wrapper:
        value = value[0]
    return _json_text(value, 2 if prettify is not UNDEFINED and prettify else 0, 0)


# ---- string functions ---------------------------------------------------------------------------

@_builtin("$length", "<s-:n>")
def _length(text):
    return UNDEFINED if text is UNDEFINED else len(text)


@_builtin("$substring", "<s-nn?:s>")
def _substring(text, start, length=UNDEFINED):
    if text is UNDEFINED:
        return UNDEFINED
    if start is UNDEFINED:
        # An absent start selects the whole string, and selects nothing once a length is given.
        return text if length is UNDEFINED else ""
    characters = list(text)
    # A start further left than the string is long clamps to 0 rather than wrapping around.
    if len(characters) + start < 0:
        start = 0
    if length is UNDEFINED:
        return "".join(characters[_js_integer(start):])
    if length <= 0:
        return ""
    # Truncation happens once, at the slice, so 1.7 + 2.9 reaches index 4 where 1 + 2 does not.
    end = start + length if start >= 0 else len(characters) + start + length
    return "".join(characters[_js_integer(start):_js_integer(end)])


@_builtin("$substringBefore", "<s-s:s>")
def _substring_before(text, chars):
    if text is UNDEFINED or chars is UNDEFINED:
        return text
    found = text.find(chars)
    return text if found < 0 else text[:found]


@_builtin("$substringAfter", "<s-s:s>")
def _substring_after(text, chars):
    if text is UNDEFINED or chars is UNDEFINED:
        return text
    found = text.find(chars)
    return text if found < 0 else text[found + len(chars):]


@_builtin("$lowercase", "<s-:s>")
def _lowercase(text):
    return UNDEFINED if text is UNDEFINED else text.lower()


@_builtin("$uppercase", "<s-:s>")
def _uppercase(text):
    return UNDEFINED if text is UNDEFINED else text.upper()


@_builtin("$trim", "<s-:s>")
def _trim(text):
    """Collapses runs of space/tab/LF/CR only, then drops one leading and one trailing space."""
    if text is UNDEFINED:
        return UNDEFINED
    collapsed = _TRIMMED.sub(" ", text)
    if collapsed.startswith(" "):
        collapsed = collapsed[1:]
    if collapsed.endswith(" "):
        collapsed = collapsed[:-1]
    return collapsed


@_builtin("$pad", "<s-ns?:s>")
def _pad(text, width, char=UNDEFINED):
    if text is UNDEFINED:
        return UNDEFINED
    if width is UNDEFINED:
        return text
    if char is UNDEFINED or char == "":
        char = " "
    width = _js_integer(width)
    padding_length = abs(width) - len(text)
    if padding_length <= 0:
        return text
    padding = (char * padding_length)[:padding_length]
    return text + padding if width > 0 else padding + text


@_builtin("$base64encode", "<s-:s>")
def _base64_encode(text):
    """Node's `Buffer.from(str, 'binary')`: one byte per code unit, not UTF-8."""
    if text is UNDEFINED:
        return UNDEFINED
    return base64.b64encode(bytes(ord(character) & 0xFF for character in text)).decode("ascii")


@_builtin("$base64decode", "<s-:s>")
def _base64_decode(text):
    if text is UNDEFINED:
        return UNDEFINED
    return base64.b64decode(text.encode("ascii")).decode("latin-1")


def _quote_url(text, safe, function_name):
    if text is UNDEFINED:
        return UNDEFINED
    try:
        return urllib.parse.quote(text, safe=safe, errors="strict")
    except (UnicodeEncodeError, UnicodeDecodeError) as malformed:
        raise JsonataError("D3140", value=text, functionName=function_name) from malformed


def _unquote_url(text, reserved, function_name):
    """Percent-escapes decoded as UTF-8; `decodeURI` puts back the reserved characters it must keep."""
    if text is UNDEFINED:
        return UNDEFINED
    if _TRUNCATED_ESCAPE.search(text):
        raise JsonataError("D3140", value=text, functionName=function_name)

    def decode(run):
        octets = bytes(int(run.group(0)[at + 1:at + 3], 16) for at in range(0, len(run.group(0)), 3))
        decoded = octets.decode("utf-8")
        return "".join(f"%{ord(character):02X}" if character in reserved else character for character in decoded)

    try:
        return _ESCAPE_RUN.sub(decode, text)
    except UnicodeDecodeError as malformed:
        raise JsonataError("D3140", value=text, functionName=function_name) from malformed


@_builtin("$encodeUrlComponent", "<s-:s>")
def _encode_url_component(text):
    return _quote_url(text, URL_COMPONENT_SAFE, "encodeUrlComponent")


@_builtin("$encodeUrl", "<s-:s>")
def _encode_url(text):
    return _quote_url(text, URL_SAFE, "encodeUrl")


@_builtin("$decodeUrlComponent", "<s-:s>")
def _decode_url_component(text):
    return _unquote_url(text, "", "decodeUrlComponent")


@_builtin("$decodeUrl", "<s-:s>")
def _decode_url(text):
    return _unquote_url(text, URL_RESERVED, "decodeUrl")


# ---- the matcher protocol -----------------------------------------------------------------------
#
# A pattern argument is never a compiled regex: `/re/` evaluates to a closure answering
# `{match, start, end, groups, next}` (evaluator._evaluate_regex), and a hand-written JSONata
# function answering the same shape is just as valid an argument. Everything below talks to that
# protocol, so `$contains`, `$match`, `$replace` and `$split` never touch a regex engine.

def _evaluate_matcher(matcher, args, token):
    """`evaluateMatcher` (functions.js:322-335): a matcher that answers the wrong shape is T1010."""
    result = _apply(matcher, args)
    if result is UNDEFINED or result is None or result is False or result == "" or result == 0:
        return UNDEFINED
    shape = result if isinstance(result, dict) else {}
    if not (_is_number(shape.get("start")) or isinstance(shape.get("groups"), list) or callable(shape.get("next"))):
        raise JsonataError("T1010", token=token)
    return result


def _matches(pattern, text, token):
    """Every match in order, stopping at `limit` or at the end of the string."""
    found = _evaluate_matcher(pattern, [text], token)
    while found is not UNDEFINED:
        yield found
        following = found.get("next", UNDEFINED)
        found = UNDEFINED if following is UNDEFINED else _evaluate_matcher(following, [], token)


@_builtin("$contains", "<s-(sf):b>")
def _contains(text, pattern):
    if text is UNDEFINED or pattern is UNDEFINED:
        return UNDEFINED
    if isinstance(pattern, str):
        return pattern in text
    return _evaluate_matcher(pattern, [text], "contains") is not UNDEFINED


@_builtin("$match", "<s-f<s:o>n?:a<o>>")
def _match(text, pattern, limit=UNDEFINED):
    if text is UNDEFINED:
        return UNDEFINED
    if limit is not UNDEFINED and limit < 0:
        raise JsonataError("D3040", value=limit, index=3)
    result = Sequence()
    if limit is not UNDEFINED and limit <= 0:
        return result
    for found in _matches(pattern, text, "match"):
        result.append({"match": found["match"], "index": found["start"], "groups": found["groups"]})
        if limit is not UNDEFINED and len(result) >= limit:
            break
    return result


@_builtin("$replace", "<s-(sf)(sf)n?:s>")
def _replace(text, pattern, replacement, limit=UNDEFINED):
    if text is UNDEFINED:
        return UNDEFINED
    if pattern == "":
        raise JsonataError("D3010", value=pattern, index=2)
    if limit is not UNDEFINED and limit < 0:
        raise JsonataError("D3011", value=limit, index=4)
    if limit is not UNDEFINED and limit <= 0:
        return text
    if isinstance(pattern, str):
        return _replace_literal(text, pattern, replacement, limit)
    return _replace_matches(text, pattern, replacement, limit)


def _replace_literal(text, pattern, replacement, limit):
    """A plain-string pattern takes the replacement verbatim: `$N` substitution is regex-only."""
    if not isinstance(replacement, str):
        raise JsonataError("D3012", value=replacement)
    return text.replace(pattern, replacement, -1 if limit is UNDEFINED else _js_integer(limit))


def _replace_matches(text, pattern, replacement, limit):
    written = []
    position = 0
    count = 0
    for found in _matches(pattern, text, "replace"):
        written.append(text[position:found["start"]])
        written.append(_replacement_for(found, replacement))
        position = found["start"] + len(found["match"])
        count += 1
        if limit is not UNDEFINED and count >= limit:
            break
    if not count:
        return text
    written.append(text[position:])
    return "".join(written)


def _replacement_for(found, replacement):
    if isinstance(replacement, str):
        return _substitute_groups(replacement, found)
    produced = _apply(replacement, [found])
    if not isinstance(produced, str):
        raise JsonataError("D3012", value=produced)
    return produced


def _substitute_groups(replacement, found):
    """JSONata's own `$N` parser (functions.js:441-490).

    It is neither JavaScript's `$N` nor `re.sub`'s `\\g<n>`: it reads as many digits as the group
    count can address, retries one digit shorter when that overshoots, drops a token naming a group
    that did not participate, and leaves a `$` that is not followed by a digit standing.
    """
    groups = found["groups"]
    written = []
    position = 0
    at = replacement.find("$")
    while at != -1 and position < len(replacement):
        written.append(replacement[position:at])
        position = at + 1
        marker = replacement[position:position + 1]
        if marker == "$":
            written.append("$")
            position += 1
        elif marker == "0":
            written.append(found["match"])
            position += 1
        else:
            position = _substitute_one_group(replacement, position, groups, written)
        at = replacement.find("$", position)
    written.append(replacement[position:])
    return "".join(written)


def _substitute_one_group(replacement, position, groups, written):
    max_digits = 1 if not groups else math.floor(math.log(len(groups)) * math.log10(math.e)) + 1
    number = js_parse_int(replacement[position:position + max_digits])
    if max_digits > 1 and not math.isnan(number) and number > len(groups):
        number = js_parse_int(replacement[position:position + max_digits - 1])
    if math.isnan(number):
        written.append("$")
        return position
    if groups and 0 < number <= len(groups) and groups[number - 1] is not UNDEFINED:
        written.append(groups[number - 1])
    return position + len(str(number))


@_builtin("$split", "<s-(sf)n?:a<s>>")
def _split(text, separator, limit=UNDEFINED):
    if text is UNDEFINED:
        return UNDEFINED
    if limit is not UNDEFINED and limit < 0:
        raise JsonataError("D3020", value=limit, index=3)
    if limit is not UNDEFINED and limit <= 0:
        return []
    if isinstance(separator, str):
        parts = list(text) if separator == "" else text.split(separator)
        return parts if limit is UNDEFINED else parts[:_js_integer(limit)]
    return _split_matches(text, separator, limit)


def _split_matches(text, separator, limit):
    parts = []
    start = 0
    for found in _matches(separator, text, "split"):
        parts.append(text[start:found["start"]])
        start = found["end"]
        if limit is not UNDEFINED and len(parts) >= limit:
            return parts
    parts.append(text[start:])
    return parts


@_builtin("$join", "<a<s>s?:s>")
def _join(strings, separator=UNDEFINED):
    if strings is UNDEFINED:
        return UNDEFINED
    return ("" if separator is UNDEFINED else separator).join(strings)


# ---- numeric functions --------------------------------------------------------------------------

@_builtin("$number", "<(nsb)-:n>")
def _number(value):
    if value is UNDEFINED:
        return UNDEFINED
    if isinstance(value, bool):
        return 1 if value else 0
    if _is_number(value):
        return value
    if isinstance(value, str):
        if _DECIMAL_LITERAL.fullmatch(value):
            parsed = float(value)
            if math.isfinite(parsed):
                return _plain(parsed)
        if _RADIX_LITERAL.fullmatch(value):
            return int(value, 0)
    raise JsonataError("D3030", value=value, index=1)


@_builtin("$abs", "<n-:n>")
def _abs(value):
    return UNDEFINED if value is UNDEFINED else _plain(abs(value))


@_builtin("$floor", "<n-:n>")
def _floor(value):
    return UNDEFINED if value is UNDEFINED else math.floor(value)


@_builtin("$ceil", "<n-:n>")
def _ceil(value):
    return UNDEFINED if value is UNDEFINED else math.ceil(value)


@_builtin("$round", "<n-n?:n>")
def _round(value, precision=UNDEFINED):
    """Half to even, reached by shifting the decimal point in the number's text, not by ×10ⁿ."""
    if value is UNDEFINED:
        return UNDEFINED
    places = 0 if precision is UNDEFINED else _js_integer(precision)
    return _plain(round_half_even(value, places))


@_builtin("$sqrt", "<n-:n>")
def _sqrt(value):
    if value is UNDEFINED:
        return UNDEFINED
    if value < 0:
        raise JsonataError("D3060", value=value, index=1)
    return _plain(math.sqrt(value))


@_builtin("$power", "<n-n:n>")
def _power(value, exponent):
    if value is UNDEFINED:
        return UNDEFINED
    try:
        result = math.pow(value, exponent)
    except (OverflowError, ValueError, TypeError) as unrepresentable:
        raise JsonataError("D3061", value=value, exp=exponent, index=1) from unrepresentable
    if not math.isfinite(result):
        raise JsonataError("D3061", value=value, exp=exponent, index=1)
    return _plain(result)


@_builtin("$random", "<n?:n>")
def _random(seed=UNDEFINED):
    """Step Functions adds the seeded overload; the same seed always answers the same number.

    A seed that selects nothing is an absent seed, which is what the `n?` in the signature declares
    and how every other optional argument in this file reads UNDEFINED.
    """
    if seed is UNDEFINED:
        return random.random()
    return random.Random(math.floor(seed)).random()


@_builtin("$formatBase", "<n-n?:s>")
def _format_base(value, radix=UNDEFINED):
    if value is UNDEFINED:
        return UNDEFINED
    value = int(round_half_even(value))
    radix = 10 if radix is UNDEFINED else int(round_half_even(radix))
    if radix < 2 or radix > 36:
        raise JsonataError("D3100", value=radix)
    if value == 0:
        return "0"
    digits = []
    remaining = abs(value)
    while remaining:
        remaining, digit = divmod(remaining, radix)
        digits.append(RADIX_DIGITS[digit])
    return ("-" if value < 0 else "") + "".join(reversed(digits))


@_builtin("$formatNumber", "<n-so?:s>")
def _format_number(value, picture, options=UNDEFINED):
    if value is UNDEFINED:
        return UNDEFINED
    return format_number(value, picture, _given(options))


@_builtin("$formatInteger", "<n-s:s>")
def _format_integer(value, picture):
    if value is UNDEFINED:
        return UNDEFINED
    return format_integer(value, picture)


@_builtin("$parseInteger", "<s-s:n>")
def _parse_integer(text, picture):
    if text is UNDEFINED:
        return UNDEFINED
    return _plain(parse_integer(text, picture))


# ---- aggregation functions ----------------------------------------------------------------------

@_builtin("$sum", "<a<n>:n>")
def _sum(values):
    return UNDEFINED if values is UNDEFINED else _plain(sum(values))


@_builtin("$max", "<a<n>:n>")
def _max(values):
    return UNDEFINED if values is UNDEFINED or not values else _plain(max(values))


@_builtin("$min", "<a<n>:n>")
def _min(values):
    return UNDEFINED if values is UNDEFINED or not values else _plain(min(values))


@_builtin("$average", "<a<n>:n>")
def _average(values):
    return UNDEFINED if values is UNDEFINED or not values else _plain(sum(values) / len(values))


# ---- boolean functions --------------------------------------------------------------------------

@_builtin("$boolean", "<x-:b>")
def _boolean(value):
    """The effective boolean value (functions.js:1401-1442): an array is true if any item is."""
    if value is UNDEFINED:
        return UNDEFINED
    if isinstance(value, list):
        if len(value) == 1:
            return _boolean(value[0])
        return any(_boolean(item) is True for item in value)
    if isinstance(value, str):
        return len(value) > 0
    if isinstance(value, bool):
        return value
    if _is_number(value):
        return _numeric(value) and value != 0
    if isinstance(value, dict):
        return len(value) > 0
    return False


@_builtin("$not", "<x-:b>")
def _not(value):
    return UNDEFINED if value is UNDEFINED else not _boolean(value)


@_builtin("$exists", "<x:b>")
def _exists(value):
    """One of the five that does not propagate UNDEFINED: absence is the answer, not the outcome."""
    return value is not UNDEFINED


# ---- array functions ----------------------------------------------------------------------------

@_builtin("$count", "<a:n>")
def _count(values):
    """Counts nothing as 0 rather than propagating UNDEFINED, unlike every sibling aggregation."""
    return 0 if values is UNDEFINED else len(values)


@_builtin("$append", "<xx:a>")
def _append(first, second):
    """Either argument being absent yields the other one untouched, neither wrapped nor propagated."""
    if first is UNDEFINED:
        return second
    if second is UNDEFINED:
        return first
    if not isinstance(first, list):
        first = [first]
    if not isinstance(second, list):
        second = [second]
    return list(first) + list(second)


@_builtin("$reverse", "<a:a>")
def _reverse(values):
    if values is UNDEFINED or len(values) <= 1:
        return values
    return list(reversed(values))


@_builtin("$shuffle", "<a:a>")
def _shuffle(values):
    if values is UNDEFINED or len(values) <= 1:
        return values
    shuffled = list(values)
    random.shuffle(shuffled)
    return shuffled


@_builtin("$distinct", "<x:x>")
def _distinct(values):
    """Deduplicates by structural equality, keeping the first occurrence of each value."""
    if values is UNDEFINED or not isinstance(values, list) or len(values) <= 1:
        return values
    seen = Sequence() if is_sequence(values) else []
    for value in values:
        if not any(_deep_equals(value, kept) for kept in seen):
            seen.append(value)
    return seen


@_builtin("$zip", "<a+>")
def _zip(*arrays):
    """One argument that is not an array shortens the whole result to nothing."""
    size = min((len(array) if isinstance(array, list) else 0) for array in arrays)
    return [[array[position] for array in arrays] for position in range(size)]


@_builtin("$sort", "<af?:a>")
def _sort(values, comparator=UNDEFINED):
    """A stable merge sort. The comparator answers "does `left` belong after `right`?"."""
    if values is UNDEFINED or len(values) <= 1:
        return values
    if comparator is UNDEFINED:
        if not _all_numbers(values) and not _all_strings(values):
            raise JsonataError("D3070", index=1)
        return _merge_sort(list(values), lambda left, right: left > right)
    return _merge_sort(list(values), lambda left, right: _boolean(_apply(comparator, [left, right])) is True)


def _all_numbers(values):
    return all(_numeric(value) for value in values)


def _all_strings(values):
    return all(isinstance(value, str) for value in values)


def _merge_sort(values, after):
    """Split at the middle, sort both halves, merge. A tie keeps `left` first, which is stability."""
    if len(values) <= 1:
        return values
    middle = len(values) // 2
    left = _merge_sort(values[:middle], after)
    right = _merge_sort(values[middle:], after)
    merged = []
    taken_left = 0
    taken_right = 0
    while taken_left < len(left) and taken_right < len(right):
        if after(left[taken_left], right[taken_right]):
            merged.append(right[taken_right])
            taken_right += 1
        else:
            merged.append(left[taken_left])
            taken_left += 1
    merged.extend(left[taken_left:])
    merged.extend(right[taken_right:])
    return merged


# ---- object functions ---------------------------------------------------------------------------

@_builtin("$keys", "<x-:a<s>>")
def _keys(value):
    """An array answers the union of its items' keys, in the order they were first seen."""
    result = Sequence()
    if isinstance(value, list):
        merged = {}
        for item in value:
            for key in _keys(item):
                merged[key] = True
        return _keys(merged)
    if isinstance(value, dict):
        result.extend(value.keys())
    return result


@_builtin("$lookup", "<x-s:x>")
def _lookup(value, key):
    if isinstance(value, list):
        result = Sequence()
        for item in value:
            found = _lookup(item, key)
            if found is UNDEFINED:
                continue
            if isinstance(found, list):
                result.extend(found)
            else:
                result.append(found)
        return result
    if isinstance(value, dict) and key in value:
        return value[key]
    return UNDEFINED


@_builtin("$spread", "<x-:a<o>>")
def _spread(value):
    """One single-key object per key. Anything that is not an object or array passes straight through."""
    if isinstance(value, list):
        result = Sequence()
        for item in value:
            result = _append(result, _spread(item))
        return result
    if not isinstance(value, dict):
        return value
    return Sequence({key: item} for key, item in value.items())


@_builtin("$merge", "<a<o>:o>")
def _merge(objects):
    if objects is UNDEFINED:
        return UNDEFINED
    merged = {}
    for entry in objects:
        merged.update(entry)
    return merged


@_builtin("$sift", "<o-f?:o>")
def _sift(value, predicate=UNDEFINED):
    """An object that keeps nothing is UNDEFINED, not `{}`."""
    kept = {}
    for key, entry in (value.items() if isinstance(value, dict) else ()):
        if _boolean(_apply(predicate, _callback_args(predicate, entry, key, value))) is True:
            kept[key] = entry
    return kept or UNDEFINED


@_builtin("$each", "<o-f:a>")
def _each(value, function):
    result = Sequence()
    for key, entry in (value.items() if isinstance(value, dict) else ()):
        produced = _apply(function, _callback_args(function, entry, key, value))
        if produced is not UNDEFINED:
            result.append(produced)
    return result


def _stated(message, default):
    """No message, and an empty one too, fall back to the function's own wording."""
    return default if message is UNDEFINED or not message else message


@_builtin("$error", "<s?:x>")
def _error(message=UNDEFINED):
    """Never returns: the point of the function is the throw."""
    raise JsonataError("D3137", message=_stated(message, "$error() function evaluated"))


@_builtin("$assert", "<bs?:x>")
def _assert(condition, message=UNDEFINED):
    # An absent condition is a failed assertion: `UNDEFINED` is falsy to JSONata but not to Python.
    if condition is UNDEFINED or not condition:
        raise JsonataError("D3141", message=_stated(message, "$assert() statement failed"))
    return UNDEFINED


@_builtin("$clone", "<(oa)-:o>")
def _clone(value):
    """A deep copy taken through the string form, which is what the `|...|` transform operator uses."""
    if value is UNDEFINED:
        return UNDEFINED
    return json.loads(_string(value))


@_builtin("$type", "<x:s>")
def _type(value):
    """UNDEFINED in, UNDEFINED out — this one follows the convention that `$exists` breaks."""
    if value is UNDEFINED:
        return UNDEFINED
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if _is_number(value) and _numeric(value):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if callable(value):
        return "function"
    return "object"


# ---- date and time ------------------------------------------------------------------------------

_EVALUATION_MILLIS = contextvars.ContextVar("jsonata_evaluation_millis", default=None)


@contextlib.contextmanager
def evaluation_clock():
    """Freezes `$now()` and `$millis()` for the whole of one evaluation, as jsonata.js:2137-2145 does."""
    token = _EVALUATION_MILLIS.set(round(time.time() * 1000))
    try:
        yield
    finally:
        _EVALUATION_MILLIS.reset(token)


def _evaluation_millis():
    frozen = _EVALUATION_MILLIS.get()
    return round(time.time() * 1000) if frozen is None else frozen


@_builtin("$now", "<s?s?:s>")
def _now(picture=UNDEFINED, timezone=UNDEFINED):
    return format_datetime(_evaluation_millis(), _given(picture), _given(timezone))


@_builtin("$millis", "<:n>")
def _millis():
    return _evaluation_millis()


@_builtin("$fromMillis", "<n-s?s?:s>")
def _from_millis(millis, picture=UNDEFINED, timezone=UNDEFINED):
    if millis is UNDEFINED:
        return UNDEFINED
    return format_datetime(millis, _given(picture), _given(timezone))


@_builtin("$toMillis", "<s-s?:n>")
def _to_millis(timestamp, picture=UNDEFINED):
    """A picture that captures no component at all leaves no timestamp, which is UNDEFINED."""
    if timestamp is UNDEFINED:
        return UNDEFINED
    # The frozen clock supplies the components the picture leaves unspecified, so `$toMillis('March',
    # '[MNn]')` lands in the same year `$now()` reports in the same expression.
    parsed = parse_datetime(timestamp, _given(picture), _evaluation_millis())
    return UNDEFINED if parsed is None else _plain(parsed)


# ---- higher-order functions ---------------------------------------------------------------------

@_builtin("$map", "<af>")
def _map(values, function):
    """A callback answering UNDEFINED contributes nothing, so the result can be shorter than the input."""
    if values is UNDEFINED:
        return UNDEFINED
    result = Sequence()
    for position, value in enumerate(values):
        produced = _apply(function, _callback_args(function, value, position, values))
        if produced is not UNDEFINED:
            result.append(produced)
    return result


@_builtin("$filter", "<af>")
def _filter(values, function):
    if values is UNDEFINED:
        return UNDEFINED
    result = Sequence()
    for position, value in enumerate(values):
        if _boolean(_apply(function, _callback_args(function, value, position, values))) is True:
            result.append(value)
    return result


@_builtin("$single", "<af?>")
def _single(values, function=UNDEFINED):
    if values is UNDEFINED:
        return UNDEFINED
    found = UNDEFINED
    matched = False
    for position, value in enumerate(values):
        if function is not UNDEFINED:
            kept = _boolean(_apply(function, _callback_args(function, value, position, values))) is True
        else:
            kept = True
        if not kept:
            continue
        if matched:
            raise JsonataError("D3138", index=position)
        found, matched = value, True
    if not matched:
        raise JsonataError("D3139")
    return found


@_builtin("$reduce", "<afj?:j>")
def _reduce(values, function, initial=UNDEFINED):
    if values is UNDEFINED:
        return UNDEFINED
    from .evaluator import arity
    declared = arity(function)
    if declared < 2:
        raise JsonataError("D3050", index=1)
    if initial is UNDEFINED and values:
        total, position = values[0], 1
    else:
        total, position = initial, 0
    while position < len(values):
        args = [total, values[position]]
        if declared >= 3:
            args.append(position)
        if declared >= 4:
            args.append(values)
        total = _apply(function, args)
        position += 1
    return total


# ---- what Step Functions adds -------------------------------------------------------------------

@_builtin("$partition", "<an:a>")
def _partition(values, chunk_size):
    """`States.ArrayPartition`. A trailing chunk shorter than `chunk_size` is kept."""
    if values is UNDEFINED or chunk_size is UNDEFINED:
        return UNDEFINED
    chunk_size = math.floor(chunk_size)
    if chunk_size < 1:
        raise JsonataError("T0410", token="partition", value=chunk_size, index=2)
    return [list(values[at:at + chunk_size]) for at in range(0, len(values), chunk_size)]


@_builtin("$range", "<nnn?:a>")
def _range(start, end, step=UNDEFINED):
    """`States.ArrayRange`, inclusive of `end` when the walk lands on it exactly."""
    if start is UNDEFINED or end is UNDEFINED:
        return UNDEFINED
    start = math.floor(start)
    end = math.floor(end)
    step = 1 if step is UNDEFINED else math.floor(step)
    if step == 0:
        raise JsonataError("T0410", token="range", value=step, index=3)
    walked = []
    current = start
    while current <= end if step > 0 else current >= end:
        walked.append(current)
        current += step
    return walked


@_builtin("$hash", "<ss:s>")
def _hash(data, algorithm):
    """`States.Hash`. The algorithm names are the ones AWS documents, spelled exactly as they are."""
    if data is UNDEFINED or algorithm is UNDEFINED:
        return UNDEFINED
    if algorithm not in HASH_ALGORITHMS:
        raise JsonataError("T0410", token="hash", value=algorithm, index=2)
    return HASH_ALGORITHMS[algorithm](data.encode("utf-8")).hexdigest()


@_builtin("$uuid", "<:s>")
def _uuid():
    return str(uuid.uuid4())


@_builtin("$parse", "<s:x>")
def _parse(text):
    """`$eval`'s stand-in: JSON deserialization only, never expression evaluation."""
    if text is UNDEFINED:
        return UNDEFINED
    try:
        # `NaN`, `Infinity` and `-Infinity` are JavaScript literals, not JSON, and Python accepts
        # them by default. Letting one through hands back a value the interpreter cannot represent.
        return json.loads(text, parse_constant=_not_json)
    except ValueError as malformed:
        raise JsonataError("D3120", value=text) from malformed


def _not_json(token):
    raise ValueError(token)
