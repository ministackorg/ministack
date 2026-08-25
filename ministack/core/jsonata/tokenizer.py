"""Turns a JSONata expression into tokens, one at a time, on demand.

The parser pulls tokens and tells the tokenizer whether an infix operator is expected at that
point, which is what decides whether `/` opens a regular expression or divides. A token's
`position` is the index of the character *after* it, so it reads as the 1-based column of the
token's last character, and every error message and AST node position is derived from it.
"""

import re

from .values import JsonataError

WHITESPACE = " \t\n\r\v"

# Every character that terminates a name. The parser's binding powers live in its own symbol
# table; here these characters matter only as delimiters and as single-character operators.
# `!` and `~` are delimiters without being operators, so `a!b` fails as an unknown operator
# rather than scanning `!b` into the name.
OPERATOR_CHARS = frozenset(".[]{}(),@#;:?+-*/%|=<>^&!~")

TWO_CHAR_OPERATORS = ("..", ":=", "!=", ">=", "<=", "**", "~>", "?:", "??")

STRING_ESCAPES = {'"': '"', "\\": "\\", "/": "/", "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t"}

KEYWORD_OPERATORS = ("or", "in", "and")

KEYWORD_VALUES = {"true": True, "false": False, "null": None}

# A leading `-` never reaches here: `_scan_operator` takes it as an operator first.
NUMBER = re.compile(r"(0|([1-9][0-9]*))(\.[0-9]+)?([Ee][-+]?[0-9]+)?")

# `\Z` and not `$`: Python's `$` also matches just before a final newline, so `"\u041<LF>"`
# would read as four hex digits and silently yield a character plus a swallowed newline.
HEX_DIGITS = re.compile(r"^[0-9a-fA-F]+\Z")

# A JavaScript named group is `(?<name>…)` and its backreference `\k<name>`; Python spells them
# `(?P<name>…)` and `(?P=name)`.
JS_NAMED_GROUP = re.compile(r"\(\?<(?=[A-Za-z_])")
JS_BACKREFERENCE = re.compile(r"\\k<([A-Za-z_]\w*)>")

# `\d` and `\w` spelled out as the ASCII ranges a JavaScript regex without the `u` flag matches.
# Inside a character class the escape becomes a bare range so `[\d]` stays one class.
ASCII_CLASS = {"d": "[0-9]", "D": "[^0-9]", "w": "[0-9A-Za-z_]", "W": "[^0-9A-Za-z_]"}
ASCII_RANGE = {"d": "0-9", "w": "0-9A-Za-z_"}

MAX_EXACT_INTEGER = 2**53


def _narrowed_to_ascii(pattern):
    """`\\d` and `\\w` rewritten as the ranges a JavaScript regex without the `u` flag matches.

    `re.ASCII` does this in one flag, but it also takes away the Unicode case folding `i` performs
    and the Unicode whitespace `\\s` matches, both of which the reference keeps: `/ä/i` matches `Ä`
    there and a non-breaking space matches `\\s`. Spelling the two classes out keeps all three.
    What stays Python's: `\\b`, whose word characters have no range spelling, and `\\D`/`\\W`
    inside a class, where a negated subset has none either.
    """
    translated = []
    inside_class = False
    index = 0
    while index < len(pattern):
        character = pattern[index]
        if character == "\\" and index + 1 < len(pattern):
            spelling = (ASCII_RANGE if inside_class else ASCII_CLASS).get(pattern[index + 1])
            translated.append(spelling or pattern[index:index + 2])
            index += 2
            continue
        if character == "[":
            inside_class = True
        elif character == "]":
            inside_class = False
        translated.append(character)
        index += 1
    return "".join(translated)


class Tokenizer:
    """Reads `source` left to right, yielding one token per `next_token` call and None at the end."""

    def __init__(self, source):
        self.source = source
        self.position = 0
        self.length = len(source)

    def next_token(self, expecting_infix=False):
        """The next token, or None at end of input.

        `expecting_infix` says an operator is due here, so `/` is division; otherwise `/` opens a
        regular expression.
        """
        if self.position >= self.length:
            return None
        self._skip_whitespace()
        if self._char(self.position) == "/" and self._char(self.position + 1) == "*":
            self._skip_comment()
            return self.next_token(expecting_infix)
        if not expecting_infix and self._char(self.position) == "/":
            self.position += 1
            return self._token("regex", self._scan_regex())
        operator = self._scan_operator()
        if operator is not None:
            return operator
        character = self._char(self.position)
        if character in ('"', "'"):
            return self._scan_string(character)
        number = NUMBER.match(self.source, self.position)
        if number:
            return self._scan_number(number)
        if character == "`":
            return self._scan_backquoted_name()
        return self._scan_name()

    def _char(self, index):
        """The character at `index`, or the empty string past either end, like JS `charAt`."""
        if 0 <= index < self.length:
            return self.source[index]
        return ""

    def _token(self, kind, value):
        return {"type": kind, "value": value, "position": self.position}

    def _skip_whitespace(self):
        while self.position < self.length and self._char(self.position) in WHITESPACE:
            self.position += 1

    def _skip_comment(self):
        comment_start = self.position
        self.position += 2
        while not (self._char(self.position) == "*" and self._char(self.position + 1) == "/"):
            self.position += 1
            if self.position >= self.length:
                raise JsonataError("S0106", position=comment_start)
        self.position += 2

    def _scan_operator(self):
        two = self.source[self.position:self.position + 2]
        if two in TWO_CHAR_OPERATORS:
            self.position += 2
            return self._token("operator", two)
        character = self._char(self.position)
        if character in OPERATOR_CHARS:
            self.position += 1
            return self._token("operator", character)
        return None

    def _scan_regex(self):
        """The regular expression body, entered with the opening `/` already consumed."""
        start = self.position
        depth = 0
        while self.position < self.length:
            character = self._char(self.position)
            if character == "/" and depth == 0 and self._preceding_backslashes() % 2 == 0:
                pattern = self.source[start:self.position]
                if pattern == "":
                    raise JsonataError("S0301", position=self.position)
                self.position += 1
                return self._compile(pattern, self._scan_regex_flags())
            if self._char(self.position - 1) != "\\":
                if character in "([{":
                    depth += 1
                elif character in ")]}":
                    depth -= 1
            self.position += 1
        raise JsonataError("S0302", position=self.position)

    def _preceding_backslashes(self):
        count = 0
        while self._char(self.position - (count + 1)) == "\\":
            count += 1
        return count

    def _scan_regex_flags(self):
        start = self.position
        while self._char(self.position) in ("i", "m"):
            self.position += 1
        return self.source[start:self.position]

    @staticmethod
    def _compile(pattern, flags):
        options = 0
        if "i" in flags:
            options |= re.IGNORECASE
        if "m" in flags:
            options |= re.MULTILINE
        translated = JS_NAMED_GROUP.sub("(?P<", JS_BACKREFERENCE.sub(r"(?P=\1)", pattern))
        try:
            return re.compile(_narrowed_to_ascii(translated), options)
        except re.error as invalid:
            raise JsonataError("S0303", position=0, value=pattern) from invalid

    def _scan_string(self, quote):
        self.position += 1
        text = []
        while self.position < self.length:
            character = self._char(self.position)
            if character == "\\":
                self.position += 1
                text.append(self._scan_escape())
            elif character == quote:
                self.position += 1
                return self._token("string", _combine_surrogates("".join(text)))
            else:
                text.append(character)
            self.position += 1
        raise JsonataError("S0101", position=self.position)

    def _scan_escape(self):
        character = self._char(self.position)
        if character in STRING_ESCAPES:
            return STRING_ESCAPES[character]
        if character != "u":
            raise JsonataError("S0103", position=self.position, token=character)
        octets = self.source[self.position + 1:self.position + 5]
        if not HEX_DIGITS.match(octets):
            raise JsonataError("S0104", position=self.position)
        self.position += 4
        return chr(int(octets, 16))

    def _scan_number(self, match):
        text = match.group(0)
        value = float(text)
        if value in (float("inf"), float("-inf")):
            raise JsonataError("S0102", position=self.position, token=text)
        self.position += len(text)
        return self._token("number", _as_json_number(value))

    def _scan_backquoted_name(self):
        self.position += 1
        end = self.source.find("`", self.position)
        if end == -1:
            self.position = self.length
            raise JsonataError("S0105", position=self.position)
        name = self.source[self.position:end]
        self.position = end + 1
        return self._token("name", name)

    def _scan_name(self):
        end = self.position
        while end < self.length and self.source[end] not in WHITESPACE and self.source[end] not in OPERATOR_CHARS:
            end += 1
        start, self.position = self.position, end
        if self._char(start) == "$":
            return self._token("variable", self.source[start + 1:end])
        name = self.source[start:end]
        if name in KEYWORD_OPERATORS:
            return self._token("operator", name)
        if name in KEYWORD_VALUES:
            return self._token("value", KEYWORD_VALUES[name])
        if name == "" and self.position == self.length:
            return None
        return self._token("name", name)


def _combine_surrogates(text):
    """A `\\u` escape names a UTF-16 code unit, so an astral character arrives as two of them."""
    if not any("\ud800" <= character <= "\udfff" for character in text):
        return text
    return text.encode("utf-16", "surrogatepass").decode("utf-16", "surrogatepass")


def _as_json_number(value):
    """A double whose value is integral prints without a decimal point, so it becomes a Python int."""
    if value.is_integer() and abs(value) <= MAX_EXACT_INTEGER:
        return int(value)
    return value
