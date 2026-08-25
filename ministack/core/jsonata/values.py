"""The values every layer of the JSONata interpreter shares.

`UNDEFINED` is the "no value" marker JSONata keeps distinct from JSON `null`, `Sequence` is the
list a path step produces, and `JsonataError` is the single exception the tokenizer, the parser
and the evaluator raise, carrying the `Sxxxx`/`Txxxx`/`Dxxxx` code the JSONata specification
names.
"""

import json
import math
import re

_TRIPLE_PLACEHOLDER = re.compile(r"\{\{\{([^}]+)}}}")
_PLACEHOLDER = re.compile(r"\{\{([^}]+)}}")

# `JSON.stringify` escapes an unpaired surrogate, which `json.dumps(ensure_ascii=False)` leaves raw
# and unencodable. In Python a surrogate that survived decoding is always unpaired.
_UNPAIRED_SURROGATE = re.compile(r"[\ud800-\udfff]")

# The error catalog of the reference implementation (jsonata-js src/jsonata.js:1988-2079), copied
# once so no layer invents a second wording for a code.
ERROR_MESSAGES = {
    "S0101": "String literal must be terminated by a matching quote",
    "S0102": "Number out of range: {{token}}",
    "S0103": "Unsupported escape sequence: \\{{token}}",
    "S0104": "The escape sequence \\u must be followed by 4 hex digits",
    "S0105": "Quoted property name must be terminated with a backquote (`)",
    "S0106": "Comment has no closing tag",
    "S0201": "Syntax error: {{token}}",
    "S0202": "Expected {{value}}, got {{token}}",
    "S0203": "Expected {{value}} before end of expression",
    "S0204": "Unknown operator: {{token}}",
    "S0205": "Unexpected token: {{token}}",
    "S0206": "Unknown expression type: {{token}}",
    "S0207": "Unexpected end of expression",
    "S0208": "Parameter {{value}} of function definition must be a variable name (start with $)",
    "S0209": "A predicate cannot follow a grouping expression in a step",
    "S0210": "Each step can only have one grouping expression",
    "S0211": "The symbol {{token}} cannot be used as a unary operator",
    "S0212": "The left side of := must be a variable name (start with $)",
    "S0213": "The literal value {{value}} cannot be used as a step within a path expression",
    "S0214": "The right side of {{token}} must be a variable name (start with $)",
    "S0215": "A context variable binding must precede any predicates on a step",
    "S0216": "A context variable binding must precede the 'order-by' clause on a step",
    "S0217": "The object representing the 'parent' cannot be derived from this expression",
    "S0301": "Empty regular expressions are not allowed",
    "S0302": "No terminating / in regular expression",
    # Not a specification code: the reference lets JavaScript's own SyntaxError escape here, which
    # in this port would leave the caller an unhandled Python exception instead of a failed query.
    "S0303": "The regular expression {{value}} is not valid",
    "S0402": "Choice groups containing parameterized types are not supported",
    "S0401": "Type parameters can only be applied to functions and arrays",
    "S0500": "Attempted to evaluate an expression containing syntax error(s)",
    "T0410": "Argument {{index}} of function {{token}} does not match function signature",
    "T0411": "Context value is not a compatible type with argument {{index}} of function {{token}}",
    "T0412": "Argument {{index}} of function {{token}} must be an array of {{type}}",
    "D1001": "Number out of range: {{value}}",
    "D1002": "Cannot negate a non-numeric value: {{value}}",
    "T1003": "Key in object structure must evaluate to a string; got: {{value}}",
    "D1004": "Regular expression matches zero length string",
    "T1005": "Attempted to invoke a non-function. Did you mean ${{{token}}}?",
    "T1006": "Attempted to invoke a non-function",
    "T1007": "Attempted to partially apply a non-function. Did you mean ${{{token}}}?",
    "T1008": "Attempted to partially apply a non-function",
    "D1009": "Multiple key definitions evaluate to same key: {{value}}",
    "T1010": "The matcher function argument passed to function {{token}} does not return the correct object structure",
    "D1011": "Stack overflow. Check for non-terminating recursive function.  Consider rewriting as tail-recursive",
    "D1012": "Evaluation timeout after {{value}} milliseconds. Check for infinite loop",
    "D1013": "Object property names starting with _jsonata_ are reserved for internal use: {{value}}",
    "T2001": "The left side of the {{token}} operator must evaluate to a number",
    "T2002": "The right side of the {{token}} operator must evaluate to a number",
    "T2003": "The left side of the range operator (..) must evaluate to an integer",
    "T2004": "The right side of the range operator (..) must evaluate to an integer",
    "D2005": "The left side of := must be a variable name (start with $)",
    "T2006": "The right side of the function application operator ~> must be a function",
    "T2007": "Type mismatch when comparing values {{value}} and {{value2}} in order-by clause",
    "T2008": "The expressions within an order-by clause must evaluate to numeric or string values",
    "T2009": "The values {{value}} and {{value2}} either side of operator {{token}} must be of the same data type",
    "T2010": "The expressions either side of operator {{token}} must evaluate to numeric or string values",
    "T2011": "The insert/update clause of the transform expression must evaluate to an object: {{value}}",
    "T2012": "The delete clause of the transform expression must evaluate to a string or array of strings: {{value}}",
    "T2013": "The transform expression clones the input object using the $clone() function.  This has been overridden in the current scope by a non-function.",
    "D2014": "The size of the sequence allocated by the range operator (..) must not exceed 1e7.  Attempted to allocate {{value}}.",
    "D2015": "The maximum sequence length of {{value}} was exceeded.",
    "D3001": "Attempting to invoke string function on Infinity or NaN",
    "D3010": "Second argument of replace function cannot be an empty string",
    "D3011": "Fourth argument of replace function must evaluate to a positive number",
    "D3012": "Attempted to replace a matched string with a non-string value",
    "D3020": "Third argument of split function must evaluate to a positive number",
    "D3030": "Unable to cast value to a number: {{value}}",
    "D3040": "Third argument of match function must evaluate to a positive number",
    "D3050": "The second argument of reduce function must be a function with at least two arguments",
    "D3060": "The sqrt function cannot be applied to a negative number: {{value}}",
    "D3061": "The power function has resulted in a value that cannot be represented as a JSON number: base={{value}}, exponent={{exp}}",
    "D3070": "The single argument form of the sort function can only be applied to an array of strings or an array of numbers.  Use the second argument to specify a comparison function",
    "D3080": "The picture string must only contain a maximum of two sub-pictures",
    "D3081": "The sub-picture must not contain more than one instance of the 'decimal-separator' character",
    "D3082": "The sub-picture must not contain more than one instance of the 'percent' character",
    "D3083": "The sub-picture must not contain more than one instance of the 'per-mille' character",
    "D3084": "The sub-picture must not contain both a 'percent' and a 'per-mille' character",
    "D3085": "The mantissa part of a sub-picture must contain at least one character that is either an 'optional digit character' or a member of the 'decimal digit family'",
    "D3086": "The sub-picture must not contain a passive character that is preceded by an active character and that is followed by another active character",
    "D3087": "The sub-picture must not contain a 'grouping-separator' character that appears adjacent to a 'decimal-separator' character",
    "D3088": "The sub-picture must not contain a 'grouping-separator' at the end of the integer part",
    "D3089": "The sub-picture must not contain two adjacent instances of the 'grouping-separator' character",
    "D3090": "The integer part of the sub-picture must not contain a member of the 'decimal digit family' that is followed by an instance of the 'optional digit character'",
    "D3091": "The fractional part of the sub-picture must not contain an instance of the 'optional digit character' that is followed by a member of the 'decimal digit family'",
    "D3092": "A sub-picture that contains a 'percent' or 'per-mille' character must not contain a character treated as an 'exponent-separator'",
    "D3093": "The exponent part of the sub-picture must comprise only of one or more characters that are members of the 'decimal digit family'",
    "D3100": "The radix of the formatBase function must be between 2 and 36.  It was given {{value}}",
    "D3110": "The argument of the toMillis function must be an ISO 8601 formatted timestamp. Given {{value}}",
    "D3120": "Syntax error in expression passed to function eval: {{value}}",
    "D3121": "Dynamic error evaluating the expression passed to function eval: {{value}}",
    "D3130": "Formatting or parsing an integer as a sequence starting with {{value}} is not supported by this implementation",
    "D3131": "In a decimal digit pattern, all digits must be from the same decimal group",
    "D3132": "Unknown component specifier {{value}} in date/time picture string",
    "D3133": "The 'name' modifier can only be applied to months and days in the date/time picture string, not {{value}}",
    "D3134": "The timezone integer format specifier cannot have more than four digits",
    "D3135": "No matching closing bracket ']' in date/time picture string",
    "D3136": "The date/time picture string is missing specifiers required to parse the timestamp",
    "D3137": "{{{message}}}",
    "D3138": "The $single() function expected exactly 1 matching result.  Instead it matched more.",
    "D3139": "The $single() function expected exactly 1 matching result.  Instead it matched 0.",
    "D3140": "Malformed URL passed to ${{{functionName}}}(): {{value}}",
    "D3141": "{{{message}}}",
    # Not a specification code either: `U1001` is what the reference's own test harness raises for
    # a runaway recursion (jsonata-js test/run-test-suite.js:156-178), and 10 conformance cases
    # assert it. The language's opt-in guard raises D1011/D1012 instead.
    "U1001": "Stack overflow error: Check for non-terminating recursive function."
             "  Consider rewriting as tail-recursive.",
}


class _Undefined:
    """The absence of a value. Compared by identity only, never equal to `None` or to `False`."""

    __slots__ = ()

    def __repr__(self):
        return "UNDEFINED"


UNDEFINED = _Undefined()


class Sequence(list):
    """The list a path step produces, carrying the markers the evaluator reads while flattening.

    `sequence` is False for a list the expression built itself (`[1,2]`), which the flattening
    rules must leave alone but which still needs `cons`.
    """

    __slots__ = ("sequence", "keep_singleton", "tuple_stream", "outer_wrapper", "cons")

    def __init__(self, items=(), sequence=True):
        super().__init__(items)
        self.sequence = sequence
        self.keep_singleton = False
        self.tuple_stream = False
        self.outer_wrapper = False
        self.cons = False


def is_sequence(value):
    """Whether `value` is a step result rather than a JSON array."""
    return isinstance(value, list) and getattr(value, "sequence", False) is True


class JsonataError(Exception):
    """A JSONata error: a `code`, the rendered `message`, and the fields the message interpolates."""

    position = None
    token = None
    value = None

    def __init__(self, code, **fields):
        self.code = code
        for name, value in fields.items():
            setattr(self, name, value)
        self.message = _render_message(code, fields)
        super().__init__(f"{code}: {self.message}")


def _render_message(code, fields):
    template = ERROR_MESSAGES.get(code)
    if template is None:
        return code
    # The triple-brace holes go first, as jsonata.js:2085-2092 does. Left to the double-brace
    # pattern, `{{{message}}}` reads as a hole named `{message` with a stray `}` after it, and the
    # text `$error()` was given is replaced by `undefined}`.
    rendered = _TRIPLE_PLACEHOLDER.sub(lambda hole: _field_as_text(fields, hole.group(1)), template)
    return _PLACEHOLDER.sub(
        lambda hole: _field_as_json(fields.get(hole.group(1), UNDEFINED)), rendered)


def _field_as_text(fields, name):
    """A triple-brace hole: the field written out, as JavaScript coerces it into the template."""
    if name not in fields:
        return "undefined"
    value = fields[name]
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value is None:
        return "null"
    return str(value)


def _field_as_json(value):
    """A double-brace hole: the field as `JSON.stringify` writes it, quotes and all.

    That is what jsonata.js:2106-2108 interpolates, so `'a' < 1` names the values as `"a"` and `1`
    rather than letting a Python `repr` reach the user.
    """
    if value is UNDEFINED:
        return "undefined"
    if isinstance(value, float):
        if not math.isfinite(value):
            return "null"
        if value.is_integer() and abs(value) < 1e21:
            value = int(value)
    try:
        rendered = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except TypeError:
        # A function value, which `JSON.stringify` answers with `undefined` just like an absent one.
        return "undefined"
    return _UNPAIRED_SURROGATE.sub(lambda char: f"\\u{ord(char.group()):04x}", rendered)
