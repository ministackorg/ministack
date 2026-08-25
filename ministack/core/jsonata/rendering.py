"""The JSON text the reference's own `JSON.stringify` would write for an `evaluate()` result.

`evaluate()` hands back plain Python values plus two things a plain `json.dumps` cannot take: the
`UNDEFINED` sentinel a builtin such as `$match` leaves inside a result, and a non-finite `float`
an expression such as `1/0` produces. `stringify` renders what `JSON.stringify` renders for the
same JavaScript value, with no extra step in between:

- a non-finite number and an `UNDEFINED` array element both become the token `null`, because
  that is what `JSON.stringify` writes for `Infinity`, `NaN` and `undefined` at those positions;
- an `UNDEFINED` object value drops its key, the way `JSON.stringify` drops any object property
  whose value is `undefined`;
- a finite number is spelled positionally inside the band JavaScript's `Number.prototype.toString`
  stays positional for, and in exponent notation outside it.

`js_number_text`, the number-spelling half, is also what `$string()` in `builtins.py` calls: the
reference spells a finite number the same way in both places, so the same function does it. It
disagrees on a non-finite one — `$string()` raises `D3001`/`D1001` where a plain `JSON.stringify`
writes `null` — which is why that policy stays a caller's decision and is not folded in here.
"""

import decimal
import json
import math

from .values import UNDEFINED

__all__ = ["js_number_text", "stringify"]


def js_number_text(value):
    """`String(number)`: no trailing `.0`, and exponent notation only below 1e-6 or from 1e21 up.

    `repr` already carries the shortest decimal that round-trips back to `value`; the work below
    only re-spells its digits the way JavaScript would, in or out of exponent notation. `int(value)`
    is not that decimal above 2**53: it is the double's exact binary value, which has more digits
    than `repr` needed and disagrees with the reference from `1e16` up.
    """
    if isinstance(value, int):
        return str(value)
    text = repr(value)
    if "e" not in text:
        return str(int(value)) if value.is_integer() else text
    mantissa, _, exponent = text.partition("e")
    power = int(exponent)
    if -7 < power < 21:
        return format(decimal.Decimal(mantissa).scaleb(power), "f")
    digits = mantissa.lstrip("-").replace(".", "").rstrip("0") or "0"
    head = digits[0] + ("." + digits[1:] if len(digits) > 1 else "")
    return f"{'-' if mantissa.startswith('-') else ''}{head}e{'+' if power >= 0 else '-'}{abs(power)}"


def stringify(value):
    """The JSON text a plain `JSON.stringify(value)` would write in the reference.

    Call this on whatever `evaluate()` returned, not on a value already passed through
    `$string()`: the two apply different rules to a non-finite number.
    """
    if value is UNDEFINED or value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (int, float)):
        return _number_text(value)
    if isinstance(value, list):
        return "[" + ",".join(stringify(item) for item in value) + "]"
    if isinstance(value, dict):
        entries = (f"{json.dumps(key, ensure_ascii=False)}:{stringify(item)}"
                   for key, item in value.items() if item is not UNDEFINED)
        return "{" + ",".join(entries) + "}"
    if callable(value):
        # A function value reaching a returned document is not something Step Functions JSONata
        # ever produces on purpose; `null` keeps the document valid rather than raising here.
        return "null"
    raise TypeError(f"{value!r} is not a value evaluate() can produce")


def _number_text(value):
    if isinstance(value, float) and not math.isfinite(value):
        return "null"
    return js_number_text(value)
