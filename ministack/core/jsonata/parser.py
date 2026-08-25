"""Parses a JSONata expression into the abstract syntax tree the evaluator walks.

Two stages, as in the reference implementation. A Pratt (top-down operator precedence) parser
turns tokens into a tree that still contains `.` and `[` operator nodes; then `_process` rewrites
that tree into the shape the evaluator expects: `.` chains become a single `path` node with a list
of `steps`, filters move onto the step they qualify as `predicate` or `stages`, groups and sorts
attach to their step, and each `%` gets a slot that names the ancestor step it reads.

A node is a dict. A field the reference implementation leaves `undefined` is a key that is not
there, so membership tests read the same as the `typeof x === 'undefined'` they port.
"""

import copy
import re

from .tokenizer import Tokenizer
from .values import UNDEFINED, JsonataError

LAMBDA_NAMES = ("function", "λ")

KEYWORDS_USABLE_AS_NAMES = ("and", "or", "in")

# Types `_process` returns untouched, by identity: the parse node is already the AST node. This
# is why `?:` and `??` deep-copy their left operand before reusing it.
LITERAL_TYPES = ("string", "number", "value", "wildcard", "descendant", "variable", "regex")

SIGNATURE_TYPES = {
    "s": "[sm]",
    "n": "[nm]",
    "b": "[bm]",
    "l": "[lm]",
    "o": "[om]",
    "a": "[asnblfom]",
    "f": "f",
    "j": "[asnblom]",
    "x": "[asnblfom]",
}

ARRAY_SUBTYPE_NAMES = {"a": "arrays", "b": "booleans", "f": "functions", "n": "numbers", "o": "objects", "s": "strings"}


def parse(source):
    """The AST of `source`, or a JsonataError carrying the code of the syntax it rejected."""
    parser = _Parser(source)
    parser.advance()
    expression = parser.expression(0)
    if parser.node["id"] != "(end)":
        raise JsonataError("S0201", position=parser.node["position"], token=parser.node["value"])
    expression = parser.process(expression)
    if expression.get("type") == "parent" or "seekingParent" in expression:
        raise JsonataError("S0217", token=expression.get("type"), position=expression.get("position"))
    return expression


class _Symbol:
    """What the parser knows about one token id: how tightly it binds and how it builds a node."""

    def __init__(self, lbp=0, nud=None, led=None):
        self.lbp = lbp
        self.nud = nud if nud is not None else _nud_error
        self.led = led


class _Parser:
    """Holds the token stream, the node under the cursor and the parent slots created so far."""

    def __init__(self, source):
        self.source = source
        self.lexer = Tokenizer(source)
        self.node = None
        self.ancestry = []

    # ---- Pratt parser -------------------------------------------------------------------

    def advance(self, expected_id=None, expecting_infix=False):
        """Moves the cursor onto the next token, first checking the current one is `expected_id`."""
        if expected_id and self.node["id"] != expected_id:
            code = "S0203" if self.node["id"] == "(end)" else "S0202"
            raise JsonataError(code, position=self.node["position"], token=self.node["value"], value=expected_id)
        token = self.lexer.next_token(expecting_infix)
        if token is None:
            # The end node has no `type`, so `_process` reaches its default branch and reports
            # the expression as truncated rather than as an unknown node.
            self.node = {"id": "(end)", "value": "(end)", "position": len(self.source)}
            return self.node
        self.node = {"id": _symbol_id(token), "value": token["value"], "type": token["type"],
                     "position": token["position"]}
        return self.node

    def expression(self, rbp):
        """Everything that binds tighter than `rbp`, starting at the current token."""
        token = self.node
        self.advance(None, True)
        left = _symbol(token).nud(self, token)
        while rbp < _symbol(self.node).lbp:
            token = self.node
            self.advance()
            left = _symbol(token).led(self, token, left)
        return left

    # ---- post-parse transform -----------------------------------------------------------

    def process(self, expr):
        """The AST node for a parse node: paths flattened, filters and binds moved onto steps."""
        kind = expr.get("type")
        if kind == "binary":
            result = _BINARY_PROCESSORS.get(expr["value"], _Parser._process_operation)(self, expr)
        elif kind in LITERAL_TYPES:
            result = expr
        elif kind in _PROCESSORS:
            result = _PROCESSORS[kind](self, expr)
        else:
            code = "S0207" if expr.get("id") == "(end)" else "S0206"
            raise JsonataError(code, position=expr.get("position"), token=expr.get("value"))
        if expr.get("keepArray"):
            result["keepArray"] = True
        return result

    def _process_path(self, expr):
        """`lhs . rhs`: one path node whose steps are the flattened chain."""
        lstep = self.process(expr["lhs"])
        result = lstep if lstep["type"] == "path" else {"type": "path", "steps": [lstep]}
        if lstep["type"] == "parent":
            result["seekingParent"] = [lstep["slot"]]
        rest = self.process(expr["rhs"])
        if rest["type"] == "path":
            result["steps"].extend(rest["steps"])
        else:
            # A filter written on the right operand qualifies the step's output, not its value.
            if "predicate" in rest:
                rest["stages"] = rest.pop("predicate")
            result["steps"].append(rest)
        _name_the_literal_steps(result["steps"])
        if any(step.get("keepArray") is True for step in result["steps"]):
            result["keepSingletonArray"] = True
        for step in (result["steps"][0], result["steps"][-1]):
            if step["type"] == "unary" and step["value"] == "[":
                step["consarray"] = True
        self._resolve_ancestry(result)
        return result

    def _process_filter(self, expr):
        """`lhs [ rhs ]`: the filter lands in `stages` on a path's last step, else in `predicate`."""
        result = self.process(expr["lhs"])
        step = result
        slot_name = "predicate"
        if result["type"] == "path":
            step = result["steps"][-1]
            slot_name = "stages"
        if "group" in step:
            raise JsonataError("S0209", position=expr["position"])
        step.setdefault(slot_name, [])
        predicate = self.process(expr["rhs"])
        if "seekingParent" in predicate:
            for slot in predicate["seekingParent"]:
                if slot["level"] == 1:
                    self._seek_parent(step, slot)
                else:
                    slot["level"] -= 1
            _push_ancestry(step, predicate)
        step[slot_name].append({"type": "filter", "expr": predicate, "position": expr["position"]})
        return result

    def _process_group(self, expr):
        """`lhs { … }`: the grouping attaches to the whole result, not to its last step."""
        result = self.process(expr["lhs"])
        if "group" in result:
            raise JsonataError("S0210", position=expr["position"])
        result["group"] = {"lhs": [[self.process(key), self.process(value)] for key, value in expr["rhs"]],
                           "position": expr["position"]}
        return result

    def _process_sort(self, expr):
        """`lhs ^ ( terms )`: the sort becomes the last step of the path it orders."""
        result = self.process(expr["lhs"])
        if result["type"] != "path":
            result = {"type": "path", "steps": [result]}
        sort_step = {"type": "sort", "position": expr["position"], "terms": []}
        for term in expr["rhs"]:
            term_expression = self.process(term["expression"])
            _push_ancestry(sort_step, term_expression)
            sort_step["terms"].append({"descending": term["descending"], "expression": term_expression})
        result["steps"].append(sort_step)
        self._resolve_ancestry(result)
        return result

    def _process_bind(self, expr):
        result = _positioned("bind", expr, value=expr["value"])
        result["lhs"] = self.process(expr["lhs"])
        result["rhs"] = self.process(expr["rhs"])
        _push_ancestry(result, result["rhs"])
        return result

    def _process_focus(self, expr):
        """`lhs @ $var`: binds the step's value to a variable, before any filter may run."""
        result = self.process(expr["lhs"])
        step = result["steps"][-1] if result["type"] == "path" else result
        if "stages" in step or "predicate" in step:
            raise JsonataError("S0215", position=expr["position"])
        if step["type"] == "sort":
            raise JsonataError("S0216", position=expr["position"])
        if expr.get("keepArray"):
            step["keepArray"] = True
        step["focus"] = expr["rhs"]["value"]
        step["tuple"] = True
        return result

    def _process_index(self, expr):
        """`lhs # $var`: binds the step's position, after the filters already on the step."""
        result = self.process(expr["lhs"])
        if result["type"] == "path":
            step = result["steps"][-1]
        else:
            step = result
            result = {"type": "path", "steps": [step]}
            if "predicate" in step:
                step["stages"] = step.pop("predicate")
        if "stages" in step:
            step["stages"].append({"type": "index", "value": expr["rhs"]["value"], "position": expr["position"]})
        else:
            step["index"] = expr["rhs"]["value"]
        step["tuple"] = True
        return result

    def _process_apply(self, expr):
        result = _positioned("apply", expr, value=expr["value"])
        result["lhs"] = self.process(expr["lhs"])
        result["rhs"] = self.process(expr["rhs"])
        if result["lhs"].get("keepArray") or result["rhs"].get("keepArray"):
            result["keepArray"] = True
        return result

    def _process_operation(self, expr):
        """Any other binary operator: both operands processed, the node kept as it stands."""
        result = _positioned(expr["type"], expr, value=expr["value"])
        result["lhs"] = self.process(expr["lhs"])
        result["rhs"] = self.process(expr["rhs"])
        _push_ancestry(result, result["lhs"])
        _push_ancestry(result, result["rhs"])
        return result

    def _process_unary(self, expr):
        result = _positioned("unary", expr, value=expr["value"])
        if expr["value"] == "[":
            result["expressions"] = [self._processed_part(result, item) for item in expr["expressions"]]
            return result
        if expr["value"] == "{":
            result["lhs"] = [[self._processed_part(result, key), self._processed_part(result, value)]
                             for key, value in expr["lhs"]]
            return result
        result["expression"] = self.process(expr["expression"])
        if expr["value"] == "-" and result["expression"]["type"] == "number":
            # Negation of a literal is folded into the literal itself, in place.
            result = result["expression"]
            result["value"] = -result["value"]
        else:
            _push_ancestry(result, result["expression"])
        return result

    def _process_call(self, expr):
        result = _positioned(expr["type"], expr, value=expr["value"])
        result["arguments"] = [self._processed_part(result, argument) for argument in expr["arguments"]]
        result["procedure"] = self.process(expr["procedure"])
        return result

    def _process_lambda(self, expr):
        result = _positioned("lambda", expr, arguments=expr["arguments"])
        if "signature" in expr:
            result["signature"] = expr["signature"]
        result["body"] = _tail_call_optimize(self.process(expr["body"]))
        return result

    def _process_condition(self, expr):
        result = _positioned("condition", expr)
        for branch in ("condition", "then", "else"):
            if branch in expr:
                result[branch] = self._processed_part(result, expr[branch])
        return result

    def _process_transform(self, expr):
        result = _positioned("transform", expr)
        for clause in ("pattern", "update", "delete"):
            if clause in expr:
                result[clause] = self.process(expr[clause])
        return result

    def _process_block(self, expr):
        result = _positioned("block", expr, expressions=[])
        for item in expr["expressions"]:
            part = self._processed_part(result, item)
            if part.get("consarray") or (part["type"] == "path" and part["steps"][0].get("consarray")):
                result["consarray"] = True
            result["expressions"].append(part)
        return result

    def _process_name(self, expr):
        """A bare name is a one-step path, and the name node itself is that step."""
        result = {"type": "path", "steps": [expr]}
        if expr.get("keepArray"):
            result["keepSingletonArray"] = True
        return result

    def _process_parent(self, expr):
        """`%` claims a slot; the ancestor step it resolves to fills the slot's label in."""
        index = len(self.ancestry)
        result = {"type": "parent", "slot": {"label": f"!{index}", "level": 1, "index": index}}
        self.ancestry.append(result)
        return result

    def _process_keyword_or_placeholder(self, expr):
        if expr["value"] in KEYWORDS_USABLE_AS_NAMES:
            expr["type"] = "name"
            return self.process(expr)
        if expr["value"] == "?":
            return expr
        raise JsonataError("S0201", position=expr.get("position"), token=expr["value"])

    def _processed_part(self, result, expr):
        part = self.process(expr)
        _push_ancestry(result, part)
        return part

    # ---- parent resolution --------------------------------------------------------------

    def _seek_parent(self, node, slot):
        """Walks `node` looking for the step `slot` names, one level of `%` at a time."""
        kind = node["type"]
        if kind in ("name", "wildcard"):
            self._claim_ancestor(node, slot)
        elif kind == "parent":
            slot["level"] += 1
        elif kind == "block":
            if node["expressions"]:
                node["tuple"] = True
                slot = self._seek_parent(node["expressions"][-1], slot)
        elif kind == "path":
            node["tuple"] = True
            index = len(node["steps"]) - 1
            slot = self._seek_parent(node["steps"][index], slot)
            index -= 1
            while slot["level"] > 0 and index >= 0:
                slot = self._seek_parent(node["steps"][index], slot)
                index -= 1
        else:
            raise JsonataError("S0217", token=kind, position=node.get("position"))
        return slot

    def _claim_ancestor(self, step, slot):
        """One step back for the slot; at level zero this step is the ancestor it was looking for."""
        slot["level"] -= 1
        if slot["level"] != 0:
            return
        if "ancestor" in step:
            # Two parents resolving to the same step share one tuple key.
            self.ancestry[slot["index"]]["slot"]["label"] = step["ancestor"]["label"]
        step["ancestor"] = slot
        step["tuple"] = True

    def _resolve_ancestry(self, path):
        """Points every slot the last step is seeking at an earlier step of the same path."""
        laststep = path["steps"][-1]
        slots = laststep["seekingParent"] if "seekingParent" in laststep else []
        if laststep["type"] == "parent":
            slots.append(laststep["slot"])
        for slot in slots:
            index = len(path["steps"]) - 2
            while slot["level"] > 0:
                if index < 0:
                    path.setdefault("seekingParent", []).append(slot)
                    break
                step = path["steps"][index]
                index -= 1
                # Contiguous steps that bind the focus are one step as far as `%` is concerned.
                while index >= 0 and step.get("focus") and path["steps"][index].get("focus"):
                    step = path["steps"][index]
                    index -= 1
                slot = self._seek_parent(step, slot)


def _name_the_literal_steps(steps):
    """A quoted step is a field name, which is what makes `a."b c"` work; a number never is."""
    for step in steps:
        if step["type"] in ("number", "value"):
            raise JsonataError("S0213", position=step.get("position"), value=step.get("value"))
        if step["type"] == "string":
            step["type"] = "name"


def _positioned(kind, expr, **fields):
    """A fresh node of `kind`, carrying the source node's position when it has one.

    The `??` operator synthesises its condition without a position, so the key can be missing.
    """
    node = {"type": kind, **fields}
    if "position" in expr:
        node["position"] = expr["position"]
    return node


def _push_ancestry(result, value):
    """Propagates the parent slots `value` is still seeking up to `result`."""
    if "seekingParent" not in value and value.get("type") != "parent":
        return
    slots = value["seekingParent"] if "seekingParent" in value else []
    if value.get("type") == "parent":
        slots.append(value["slot"])
    if "seekingParent" in result:
        result["seekingParent"].extend(slots)
    else:
        result["seekingParent"] = slots


def _tail_call_optimize(expr):
    """Wraps a lambda body that ends in a call, so the trampoline can run it without recursing."""
    kind = expr["type"]
    if kind == "function" and not expr.get("predicate"):
        return _positioned("lambda", expr, thunk=True, arguments=[], body=expr)
    if kind == "condition":
        expr["then"] = _tail_call_optimize(expr["then"])
        if "else" in expr:
            expr["else"] = _tail_call_optimize(expr["else"])
    elif kind == "block" and expr["expressions"]:
        expr["expressions"][-1] = _tail_call_optimize(expr["expressions"][-1])
    return expr


# ---- nud: how a token starts an expression -----------------------------------------------

def _nud_error(parser, node):
    raise JsonataError("S0211", token=node["value"], position=node["position"])


def _nud_itself(parser, node):
    return node


def _nud_wildcard(parser, node):
    node["type"] = "wildcard"
    return node


def _nud_descendant(parser, node):
    node["type"] = "descendant"
    return node


def _nud_parent(parser, node):
    node["type"] = "parent"
    return node


def _nud_negate(parser, node):
    node["expression"] = parser.expression(70)
    node["type"] = "unary"
    return node


def _nud_block(parser, node):
    expressions = []
    while parser.node["id"] != ")":
        expressions.append(parser.expression(0))
        if parser.node["id"] != ";":
            break
        parser.advance(";")
    parser.advance(")", True)
    node["type"] = "block"
    node["expressions"] = expressions
    return node


def _nud_array(parser, node):
    items = []
    if parser.node["id"] != "]":
        while True:
            item = parser.expression(0)
            if parser.node["id"] == "..":
                # The range operator exists only here, as a node the symbol table never sees.
                item = {"type": "binary", "value": "..", "position": parser.node["position"], "lhs": item}
                parser.advance("..")
                item["rhs"] = parser.expression(0)
            items.append(item)
            if parser.node["id"] != ",":
                break
            parser.advance(",")
    parser.advance("]", True)
    node["expressions"] = items
    node["type"] = "unary"
    return node


def _nud_object(parser, node):
    node["lhs"] = _parse_object_pairs(parser)
    node["type"] = "unary"
    return node


def _nud_transform(parser, node):
    node["type"] = "transform"
    node["pattern"] = parser.expression(0)
    parser.advance("|")
    node["update"] = parser.expression(0)
    if parser.node["id"] == ",":
        parser.advance(",")
        node["delete"] = parser.expression(0)
    parser.advance("|")
    return node


# ---- led: how a token continues an expression --------------------------------------------

def _led_binary(parser, node, left):
    node["lhs"] = left
    node["rhs"] = parser.expression(_symbol(node).lbp)
    node["type"] = "binary"
    return node


def _led_call(parser, node, left):
    """`left ( … )`: a function call, a partial application, or a lambda when left is `function`."""
    node["procedure"] = left
    node["type"] = "function"
    node["arguments"] = []
    if parser.node["id"] != ")":
        while True:
            # `.get`: the `(end)` node a truncated call stops on carries no type.
            if parser.node.get("type") == "operator" and parser.node["id"] == "?":
                node["type"] = "partial"
                node["arguments"].append(parser.node)
                parser.advance("?")
            else:
                node["arguments"].append(parser.expression(0))
            if parser.node["id"] != ",":
                break
            parser.advance(",")
    parser.advance(")", True)
    if left["type"] == "name" and left["value"] in LAMBDA_NAMES:
        _parse_lambda(parser, node)
    return node


def _parse_lambda(parser, node):
    for index, argument in enumerate(node["arguments"]):
        if argument["type"] != "variable":
            raise JsonataError("S0208", position=argument["position"], token=argument["value"], value=index + 1)
    node["type"] = "lambda"
    if parser.node["id"] == "<":
        node["signature"] = _parse_signature(parser)
    parser.advance("{")
    node["body"] = parser.expression(0)
    parser.advance("}")


def _parse_signature(parser):
    """Reads the `<…>` between the parameter list and the body, and compiles it."""
    start = parser.node["position"]
    depth = 1
    text = "<"
    while depth > 0 and parser.node["id"] not in ("{", "(end)"):
        token = parser.advance()
        if token["id"] == ">":
            depth -= 1
        elif token["id"] == "<":
            depth += 1
        text += str(token["value"])
    parser.advance(">")
    return Signature(text, start)


def _led_filter(parser, node, left):
    if parser.node["id"] == "]":
        # An empty predicate keeps singleton arrays; it is a flag on the step, not a node.
        step = left
        while step is not None and step.get("type") == "binary" and step.get("value") == "[":
            step = step.get("lhs")
        step["keepArray"] = True
        parser.advance("]")
        return left
    node["lhs"] = left
    node["rhs"] = parser.expression(0)
    node["type"] = "binary"
    parser.advance("]", True)
    return node


def _led_sort(parser, node, left):
    parser.advance("(")
    terms = []
    while True:
        term = {"descending": False}
        if parser.node["id"] == "<":
            parser.advance("<")
        elif parser.node["id"] == ">":
            term["descending"] = True
            parser.advance(">")
        term["expression"] = parser.expression(0)
        terms.append(term)
        if parser.node["id"] != ",":
            break
        parser.advance(",")
    parser.advance(")")
    node["lhs"] = left
    node["rhs"] = terms
    node["type"] = "binary"
    return node


def _led_object(parser, node, left):
    node["lhs"] = left
    node["rhs"] = _parse_object_pairs(parser)
    node["type"] = "binary"
    return node


def _parse_object_pairs(parser):
    pairs = []
    if parser.node["id"] != "}":
        while True:
            key = parser.expression(0)
            parser.advance(":")
            pairs.append([key, parser.expression(0)])
            if parser.node["id"] != ",":
                break
            parser.advance(",")
    parser.advance("}", True)
    return pairs


def _led_bind(parser, node, left):
    if left["type"] != "variable":
        raise JsonataError("S0212", position=left["position"], token=left["value"])
    node["lhs"] = left
    node["rhs"] = parser.expression(_symbol(node).lbp - 1)
    node["type"] = "binary"
    return node


def _led_variable_bind(parser, node, left):
    """`@` binds the step's value and `#` its position; both name a variable on the right."""
    node["lhs"] = left
    node["rhs"] = parser.expression(_symbol(node).lbp)
    # `.get`: an expression ending at `@` stops on the `(end)` node, which carries no type.
    if node["rhs"].get("type") != "variable":
        raise JsonataError("S0214", position=node["rhs"]["position"], token=node["value"])
    node["type"] = "binary"
    return node


def _led_ternary(parser, node, left):
    node["type"] = "condition"
    node["condition"] = left
    node["then"] = parser.expression(0)
    if parser.node["id"] == ":":
        parser.advance(":")
        node["else"] = parser.expression(0)
    return node


def _led_default(parser, node, left):
    node["type"] = "condition"
    # The condition and the then-branch must be independent nodes: `_process` mutates in place,
    # and a shared node would take those mutations twice.
    node["condition"] = copy.deepcopy(left)
    node["then"] = left
    node["else"] = parser.expression(0)
    return node


def _led_coalesce(parser, node, left):
    node["type"] = "condition"
    node["condition"] = {"type": "function", "value": "(",
                         "procedure": {"type": "variable", "value": "exists"},
                         "arguments": [copy.deepcopy(left)]}
    node["then"] = left
    node["else"] = parser.expression(0)
    return node


# ---- the symbol table --------------------------------------------------------------------

SYMBOLS = {
    "(end)": _Symbol(nud=_nud_itself),
    "(name)": _Symbol(nud=_nud_itself),
    "(literal)": _Symbol(nud=_nud_itself),
    "(regex)": _Symbol(nud=_nud_itself),
    ":": _Symbol(),
    ";": _Symbol(),
    ",": _Symbol(),
    ")": _Symbol(),
    "]": _Symbol(),
    "}": _Symbol(),
    "..": _Symbol(),
    ".": _Symbol(75, led=_led_binary),
    "+": _Symbol(50, led=_led_binary),
    "-": _Symbol(50, nud=_nud_negate, led=_led_binary),
    "*": _Symbol(60, nud=_nud_wildcard, led=_led_binary),
    "/": _Symbol(60, led=_led_binary),
    "%": _Symbol(60, nud=_nud_parent, led=_led_binary),
    "=": _Symbol(40, led=_led_binary),
    "<": _Symbol(40, led=_led_binary),
    ">": _Symbol(40, led=_led_binary),
    "!=": _Symbol(40, led=_led_binary),
    "<=": _Symbol(40, led=_led_binary),
    ">=": _Symbol(40, led=_led_binary),
    "&": _Symbol(50, led=_led_binary),
    "and": _Symbol(30, nud=_nud_itself, led=_led_binary),
    "or": _Symbol(25, nud=_nud_itself, led=_led_binary),
    "in": _Symbol(40, nud=_nud_itself, led=_led_binary),
    "~>": _Symbol(40, led=_led_binary),
    "??": _Symbol(40, led=_led_coalesce),
    "**": _Symbol(nud=_nud_descendant),
    "(": _Symbol(80, nud=_nud_block, led=_led_call),
    "[": _Symbol(80, nud=_nud_array, led=_led_filter),
    "^": _Symbol(40, led=_led_sort),
    "{": _Symbol(70, nud=_nud_object, led=_led_object),
    ":=": _Symbol(10, led=_led_bind),
    "@": _Symbol(80, led=_led_variable_bind),
    "#": _Symbol(80, led=_led_variable_bind),
    "?": _Symbol(20, led=_led_ternary),
    "?:": _Symbol(40, led=_led_default),
    "|": _Symbol(nud=_nud_transform),
}

_BINARY_PROCESSORS = {
    ".": _Parser._process_path,
    "[": _Parser._process_filter,
    "{": _Parser._process_group,
    "^": _Parser._process_sort,
    ":=": _Parser._process_bind,
    "@": _Parser._process_focus,
    "#": _Parser._process_index,
    "~>": _Parser._process_apply,
}

_PROCESSORS = {
    "unary": _Parser._process_unary,
    "function": _Parser._process_call,
    "partial": _Parser._process_call,
    "lambda": _Parser._process_lambda,
    "condition": _Parser._process_condition,
    "transform": _Parser._process_transform,
    "block": _Parser._process_block,
    "name": _Parser._process_name,
    "parent": _Parser._process_parent,
    "operator": _Parser._process_keyword_or_placeholder,
}


def _symbol(node):
    return SYMBOLS[node["id"]]


def _symbol_id(token):
    """The symbol a token is parsed with: names and variables share one, operators are their own."""
    kind = token["type"]
    if kind in ("name", "variable"):
        return "(name)"
    if kind == "regex":
        return "(regex)"
    if kind != "operator":
        return "(literal)"
    if token["value"] not in SYMBOLS:
        raise JsonataError("S0204", position=token["position"], token=token["value"])
    return token["value"]


# ---- function signatures -------------------------------------------------------------------

class Signature:
    """A parsed `<…>` signature: it validates a call's arguments and fixes them up.

    Validation returns the argument list the function should actually receive: a missing argument
    marked `-` is replaced by the context value, and a scalar passed where an array is declared is
    boxed into one.

    `token` names the function a validation error blames (`T0410`/`T0411`/`T0412` interpolate
    `{{token}}`): a builtin sets it to its bare `$name` right after construction. A lambda has no
    name of its own, so the caller passes `called_as`, the name the call site reached it by.
    """

    def __init__(self, definition, position):
        self.definition = definition
        self.params = _parse_signature_params(definition, position)
        self.regex = re.compile("^" + "".join(f"({param['regex']})" for param in self.params) + "$")
        self.token = UNDEFINED

    def validate(self, args, context, called_as=None):
        blamed = self.token if called_as is None else called_as
        supplied = "".join(_type_symbol(argument) for argument in args)
        matched = self.regex.match(supplied)
        if matched is None:
            self._report_mismatch(args, supplied, blamed)
        validated = []
        arg_index = 0
        for index, param in enumerate(self.params):
            captured = matched.group(index + 1)
            if captured == "":
                arg_index = self._take_missing(param, args, arg_index, context, validated, blamed)
                continue
            for single in captured:
                argument = args[arg_index] if arg_index < len(args) else UNDEFINED
                if param["type"] == "a":
                    argument = _as_declared_array(param, argument, single, captured, arg_index, blamed)
                validated.append(argument)
                arg_index += 1
        return validated

    def _take_missing(self, param, args, arg_index, context, validated, blamed):
        if param.get("contextRegex") is None:
            validated.append(args[arg_index] if arg_index < len(args) else UNDEFINED)
            return arg_index + 1
        if not param["contextRegex"].search(_type_symbol(context)):
            raise JsonataError("T0411", value=context, index=arg_index + 1, token=blamed)
        validated.append(context)
        return arg_index

    def _report_mismatch(self, args, supplied, blamed):
        """Grows the pattern one parameter at a time to name the first argument that fails."""
        pattern = "^"
        good_to = 0
        for param in self.params:
            pattern += param["regex"]
            matched = re.match(pattern, supplied)
            if matched is None:
                break
            good_to = matched.end()
        raise JsonataError("T0410", value=args[good_to] if good_to < len(args) else UNDEFINED,
                            index=good_to + 1, token=blamed)


def _parse_signature_params(definition, position):
    """The parameter list of `<…>`; the return type after `:` is parsed and discarded."""
    params = []
    # A modifier before any type character mutates a parameter that is never pushed, so `<?s>`
    # declares one string and the stray `?` is dropped.
    previous = {}
    index = 1
    while index < len(definition):
        symbol = definition[index]
        if symbol == ":":
            break
        if symbol in SIGNATURE_TYPES:
            previous = {"regex": SIGNATURE_TYPES[symbol], "type": symbol}
            if symbol == "a":
                previous["array"] = True
            params.append(previous)
        elif symbol in ("-", "?", "+"):
            _apply_modifier(previous, symbol)
        elif symbol == "(":
            previous, index = _choice_of_types(definition, index, position)
            params.append(previous)
        elif symbol == "<":
            index = _subtype_of(previous, definition, index, position)
        index += 1
    return params


def _apply_modifier(param, symbol):
    """`-` takes the value from the context when the argument is missing; `?` and `+` set arity."""
    if symbol == "-":
        param["contextRegex"] = re.compile(param.get("regex", ""))
    param["regex"] = param.get("regex", "") + ("?" if symbol == "-" else symbol)


def _choice_of_types(definition, index, position):
    end = _closing_bracket(definition, index, "(", ")")
    choice = definition[index + 1:end]
    if "<" in choice:
        raise JsonataError("S0402", value=choice, position=position + index)
    return {"regex": f"[{choice}m]", "type": f"({choice})"}, end


def _subtype_of(param, definition, index, position):
    if param.get("type") not in ("a", "f"):
        raise JsonataError("S0401", value=param.get("type"), position=position + index)
    end = _closing_bracket(definition, index, "<", ">")
    param["subtype"] = definition[index + 1:end]
    return end


def _closing_bracket(text, start, opening, closing):
    depth = 1
    position = start
    while position < len(text):
        position += 1
        if text[position:position + 1] == closing:
            depth -= 1
            if depth == 0:
                break
        elif text[position:position + 1] == opening:
            depth += 1
    return position


def _type_symbol(value):
    """The one-letter symbol a signature matches a runtime value with; `m` is a missing value."""
    if value is UNDEFINED:
        return "m"
    if callable(value):
        return "f"
    if isinstance(value, bool):
        return "b"
    if isinstance(value, (int, float)):
        return "n"
    if isinstance(value, str):
        return "s"
    if value is None:
        return "l"
    if isinstance(value, list):
        return "a"
    return "o"


def _as_declared_array(param, argument, single, captured, arg_index, token):
    """An `a` parameter: check the declared item type, then box a lone value into an array."""
    if single == "m":
        return UNDEFINED
    subtype = param.get("subtype")
    if subtype is not None and not _matches_subtype(argument, subtype, single, captured):
        raise JsonataError("T0412", value=argument, index=arg_index + 1,
                            type=ARRAY_SUBTYPE_NAMES.get(subtype), token=token)
    return argument if single == "a" else [argument]


def _matches_subtype(argument, subtype, single, captured):
    if single != "a":
        return captured == subtype
    if not argument:
        return True
    item_type = _type_symbol(argument[0])
    if item_type != subtype[0]:
        return False
    return all(_type_symbol(item) == item_type for item in argument)
