"""Query-protocol request parsing shared by the Query/XML services (Auto Scaling, ELBv2).

A form-encoded body is flattened by ``parse_qs`` into ``{key: [values]}``; ``query_value`` reads one member
back, typed by its botocore shape. The ``member.N`` index rules are AWS's Query front end, not any one
service's: they belong here, not in a service module.
"""

import re


def param(params, key, default=""):
    """The first value of *key* in a ``parse_qs`` dict (a repeated key keeps its first value, as AWS does)."""
    val = params.get(key, [default])
    return (val[0] if val else default) if isinstance(val, list) else val


def query_value(params, prefix, shape):
    """A Query-protocol member under *prefix*, typed by its botocore shape, or None when absent."""
    if shape.type_name == "structure":
        out = {}
        for member, member_shape in shape.members.items():
            value = query_value(params, f"{prefix}.{member}", member_shape)
            if value is not None:
                out[member] = value
        return out or None
    if shape.type_name == "list":
        items, i = [], 1
        while (value := query_value(params, f"{prefix}.member.{i}", shape.member)) is not None:
            items.append(value)
            i += 1
        return items or None
    if shape.type_name == "map":
        out, i = {}, 1
        while f"{prefix}.entry.{i}.key" in params:
            out[param(params, f"{prefix}.entry.{i}.key")] = query_value(params, f"{prefix}.entry.{i}.value",
                                                                         shape.value)
            i += 1
        return out or None
    if prefix not in params:
        return None
    raw = param(params, prefix)
    if shape.type_name in ("integer", "long"):
        return int(raw)
    if shape.type_name in ("double", "float"):
        return float(raw)
    if shape.type_name == "boolean":
        return raw.lower() == "true"
    return raw


# AWS reads a member index below 999,999,999 as a position (sparse-list error above a gap of 10) and refuses
# 999,999,999 and above as "Index N is illegal" (captured on Auto Scaling, us-east-2, 2026-11-06). The range
# between 99,999,999 and 999,999,999 is not captured, so the sparse error is kept there.
MAX_LISTED_DIGITS = 9

INTEGER = re.compile(r"-?[0-9]+")
DECIMAL = re.compile(r"-?([0-9]+\.?[0-9]*|\.[0-9]+)")


def member_index(text):
    """The number of a ``member.N`` index, or None when AWS calls it illegal. Compared as text, never through
    int(): the digit string is untrusted and can be thousands long."""
    significant = text.lstrip("0")
    if len(significant) > MAX_LISTED_DIGITS or significant == "9" * MAX_LISTED_DIGITS:
        return None
    return int(text)


def member_numbering_error(indexes, illegal=None):
    """``(code, message)`` for a list's ``member.N`` numbering AWS refuses, or None.

    Index 0 is refused, and so is a gap that skips 11 or more list elements: with the present indexes sorted
    ascending and the previous one starting at 1, an index i after p skips i - p. The count carries thousands
    separators, and the first offending gap in ascending order is reported. *illegal* is the first index
    ``member_index`` refused, as sent.
    """
    if 0 in indexes:
        return "MalformedInput", "0 is not a valid index"
    previous = 1
    for index in sorted(indexes):
        if index - previous >= 11:
            return "MalformedInput", f"Excessively sparse input would skip {index - previous:,} list elements"
        previous = index
    if illegal:
        return "MalformedInput", f"Index {illegal} is illegal"
    return None
