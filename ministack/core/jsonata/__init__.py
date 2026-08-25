"""A JSONata 2.0.6 interpreter, the query language Step Functions states are written in.

`evaluate` parses an expression and runs it against a document, raising `JsonataError` with the
code the JSONata specification names for whatever it rejects. `UNDEFINED` is what an expression
returns when it selects nothing, which JSONata keeps distinct from a JSON `null` it selected, and
`stringify` writes the JSON text the reference writes for a result, including the two things a
plain `json.dumps` cannot take: a nested `UNDEFINED` and a non-finite number.

The parser, the evaluator and the value types are reached by their own module paths, which is how
the tests that exercise one layer at a time already import them.
"""

from .evaluator import evaluate_ast
from .parser import parse
from .rendering import stringify
from .values import UNDEFINED, JsonataError

__all__ = ["UNDEFINED", "JsonataError", "evaluate", "stringify"]


def evaluate(expression, document, bindings=None, max_depth=None):
    """The value `expression` selects from `document`, with `bindings` as extra `$` variables.

    `max_depth` caps how many nested evaluations the expression may take, raising `U1001` past it.
    `None`, the default, is the reference's: an expression runs until it finishes.

    Source nested past what Python's own stack holds runs out in `parse`, before `max_depth`
    exists to cap it, so both ways of going too deep leave here as the same `U1001`.
    """
    try:
        return evaluate_ast(parse(expression), document, bindings, max_depth)
    except RecursionError:
        raise JsonataError("U1001", position=0)
