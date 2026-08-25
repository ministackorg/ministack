"""Evaluates a parsed JSONata expression against a document.

Two rules shape every result and are worth reading before the code. A path step collects what each
input item produced into a *sequence* and flattens it one level, except for an array the expression
built itself (`cons`) and for a lone array produced by the final step; and at the end of every node,
a sequence of one element collapses to that element while a sequence of none becomes `UNDEFINED`.
Everything else here is the reference implementation's `evaluate` switch, node by node.

`apply` invokes a function value — a lambda, a builtin or a partial — and is what the builtin
library calls back into for its higher-order functions.
"""

import math
from functools import cmp_to_key

from .builtins import BUILTINS, evaluation_clock, required_parameters
from .parser import Signature
from .values import UNDEFINED, JsonataError, Sequence, is_sequence

# A tail call recurses without nesting, so `max_depth` never stops `function(){$inf()}`: it is the
# trampoline that spins. The deepest recursion the conformance suite means to run bounces 6555
# times (groups/tail-recursion/case007), so this leaves it an order of magnitude of headroom.
MAX_TAIL_CALLS = 100_000

# The `?` of a partial application: an argument the caller has not supplied yet.
PLACEHOLDER = object()


class Frame:
    """A scope: what is bound here, where lookups fall back to, and the evaluation depth so far."""

    __slots__ = ("bindings", "parent", "base", "depth", "max_depth")

    def __init__(self, parent=None):
        self.bindings = {}
        self.parent = parent
        self.base = parent.base if parent is not None else self
        self.depth = 0
        # Only the base frame's copy is ever read. `inf` is the reference's default: with neither
        # `options.stack` nor `options.timeout` given, jsonata.js:2203-2222 installs no guard.
        self.max_depth = math.inf

    def bind(self, name, value):
        self.bindings[name] = value

    def lookup(self, name):
        """The value bound to `name` in the innermost scope that has it, or UNDEFINED."""
        frame = self
        while frame is not None:
            if name in frame.bindings:
                return frame.bindings[name]
            frame = frame.parent
        return UNDEFINED


class Lambda:
    """A user-defined function: its body, plus the frame and the context value it closed over.

    `__call__` is never invoked — `_apply_inner` dispatches on the type first — but it is what
    makes `callable()` answer "this is a function value". That is how `parser.py:_type_symbol`
    reads a signature's `f` and how `$string`, `$type` and `$` render one, none of which can
    import this module back.
    """

    __slots__ = ("parameters", "signature", "body", "environment", "context", "thunk")

    def __init__(self, parameters, body, environment, context, signature=None, thunk=False):
        self.parameters = parameters
        self.body = body
        self.environment = environment
        self.context = context
        self.signature = signature
        self.thunk = thunk

    def __call__(self, *args):
        return apply(self, list(args))


class PartialFunction:
    """A function with some arguments already supplied; `PLACEHOLDER` marks each one still missing.

    `__call__` carries the same marker `Lambda.__call__` does, for the same readers.
    """

    __slots__ = ("function", "arguments")

    def __init__(self, function, arguments):
        self.function = function
        self.arguments = arguments

    def __call__(self, *args):
        return apply(self, list(args))


def evaluate_ast(ast, document, bindings=None, max_depth=None):
    """The value `ast` selects from `document`, with `bindings` bound as `$name` variables.

    `max_depth` is how many nested evaluations the expression may take before it is called a
    runaway recursion and stopped with `U1001`. `None`, the default, is the reference's: no limit.
    """
    environment = Frame(_static_frame())
    if max_depth is not None:
        environment.base.max_depth = max_depth
    for name, value in (bindings or {}).items():
        environment.bind(name, value)
    # `$$` reads this binding, so the root is the document as given, before the wrapping below.
    environment.bind("$", document)
    if isinstance(document, list) and not is_sequence(document):
        document = Sequence([document])
        document.outer_wrapper = True
    # One evaluation reads one clock, as jsonata.js:2137-2145 does by binding `$now` and `$millis`
    # fresh per run over a single captured timestamp.
    with evaluation_clock():
        return _evaluate(ast, document, environment)


def apply(function_value, args):
    """Invoke a JSONata function value (a lambda, a builtin or a partial) with `args`."""
    return _apply(function_value, list(args), UNDEFINED)


def arity(function_value):
    """How many arguments `function_value` declares, which is what `$reduce` and `$map` branch on.

    The declared `arity` a builtin carries wins. The signature is no substitute: `$string` declares
    `<x-b?:s>` and would count two, while a callback handed that optional second argument fails its
    own type check, so `$map([1,2,3], $string)` needs the one that JavaScript's `.length` reports.
    """
    if isinstance(function_value, Lambda):
        return len(function_value.parameters)
    if isinstance(function_value, PartialFunction):
        return sum(1 for argument in function_value.arguments if argument is PLACEHOLDER)
    declared = getattr(function_value, "arity", None)
    if isinstance(declared, int):
        return declared
    return required_parameters(function_value)


# ---- the dispatch ---------------------------------------------------------------------------

def _evaluate(expr, context, environment):
    """The value of one AST node, with its predicates, its grouping and the singleton unwrap."""
    base = environment.base
    base.depth += 1
    if base.depth > base.max_depth:
        raise JsonataError("U1001")
    try:
        handler = _HANDLERS.get(expr["type"])
        result = handler(expr, context, environment) if handler is not None else UNDEFINED
        for predicate in expr.get("predicate", ()):
            result = _evaluate_filter(predicate["expr"], result, environment)
        if expr["type"] != "path" and "group" in expr:
            result = _evaluate_group(expr["group"], result, environment)
        return _unwrap_singleton(expr, result)
    finally:
        base.depth -= 1


def _unwrap_singleton(expr, result):
    """A sequence of one is its element and a sequence of none is nothing; a JSON array is neither."""
    if result is UNDEFINED or not is_sequence(result) or result.tuple_stream:
        return result
    if expr.get("keepArray"):
        result.keep_singleton = True
    if len(result) == 0:
        return UNDEFINED
    if len(result) == 1 and not result.keep_singleton:
        return result[0]
    return result


def _evaluate_literal(expr, context, environment):
    return expr["value"]


def _evaluate_name(expr, context, environment):
    return _lookup(context, expr["value"])


def _evaluate_variable(expr, context, environment):
    """`$` is the context value, `$$` the whole document, anything else a binding."""
    if expr["value"] == "":
        if isinstance(context, list) and getattr(context, "outer_wrapper", False):
            return context[0]
        return context
    return environment.lookup(expr["value"])


def _evaluate_parent(expr, context, environment):
    return environment.lookup(expr["slot"]["label"])


def _evaluate_block(expr, context, environment):
    """A block scopes the variables it binds: they live in a frame that dies with the block."""
    frame = Frame(environment)
    result = UNDEFINED
    for expression in expr["expressions"]:
        result = _evaluate(expression, context, frame)
    return result


def _evaluate_bind(expr, context, environment):
    value = _evaluate(expr["rhs"], context, environment)
    environment.bind(expr["lhs"]["value"], value)
    return value


def _evaluate_condition(expr, context, environment):
    if _boolean(_evaluate(expr["condition"], context, environment)) is True:
        return _evaluate(expr["then"], context, environment)
    if "else" in expr:
        return _evaluate(expr["else"], context, environment)
    return UNDEFINED


def _evaluate_unary(expr, context, environment):
    if expr["value"] == "-":
        return _negate(_evaluate(expr["expression"], context, environment))
    if expr["value"] == "[":
        return _evaluate_array_constructor(expr, context, environment)
    return _evaluate_group(expr, context, environment)


def _negate(value):
    if value is UNDEFINED:
        return UNDEFINED
    if not _is_numeric(value):
        raise JsonataError("D1002", value=value)
    return -value


def _evaluate_array_constructor(expr, context, environment):
    """`[…]`: an item that is itself an array constructor nests, everything else appends."""
    result = []
    for item in expr["expressions"]:
        value = _evaluate(item, context, environment)
        if value is UNDEFINED:
            continue
        if item.get("value") == "[":
            result.append(value)
        else:
            result = _append(result, value)
    if expr.get("consarray"):
        result = Sequence(result, sequence=False)
        result.cons = True
    return result


def _evaluate_wildcard(expr, context, environment):
    """`*`: every value of the object, with an array value flattened all the way down."""
    results = Sequence()
    if isinstance(context, list) and getattr(context, "outer_wrapper", False) and len(context) > 0:
        context = context[0]
    if not isinstance(context, (dict, list)):
        return results
    # An array is an object whose keys are its indices, so `*` walks it like any other.
    for value in (context.values() if isinstance(context, dict) else context):
        if isinstance(value, list):
            results = _append(results, _flatten(value))
        else:
            results.append(value)
    return results


def _flatten(value, flattened=None):
    """Every scalar of a nested array, in order; arrays themselves are dropped."""
    if flattened is None:
        flattened = []
    if isinstance(value, list):
        for item in value:
            _flatten(item, flattened)
    else:
        flattened.append(value)
    return flattened


def _evaluate_descendants(expr, context, environment):
    """`**`: this node and every node below it, pre-order, arrays contributing only their members."""
    if context is UNDEFINED:
        return UNDEFINED
    results = Sequence()
    _recurse_descendants(context, results)
    if len(results) == 1:
        return results[0]
    return results


def _recurse_descendants(value, results):
    if not isinstance(value, list):
        results.append(value)
    if isinstance(value, list):
        for member in value:
            _recurse_descendants(member, results)
    elif isinstance(value, dict):
        for member in value.values():
            _recurse_descendants(member, results)


def _evaluate_regex(expr, context, environment):
    """A regex is a function: called with a string it reports the first match from `start`."""
    pattern = expr["value"]

    def match_from(subject, start=0):
        found = pattern.search(subject, start or 0)
        if found is None:
            return UNDEFINED
        groups = [UNDEFINED if group is None else group for group in found.groups()]
        result = {"match": found.group(0), "start": found.start(), "end": found.end(), "groups": groups}

        def next_match():
            if found.end() >= len(subject):
                return UNDEFINED
            following = match_from(subject, found.end())
            if following is not UNDEFINED and following["match"] == "":
                raise JsonataError("D1004", value=pattern.pattern)
            return following

        result["next"] = next_match
        return result

    return match_from


# ---- paths ----------------------------------------------------------------------------------

def _evaluate_path(expr, context, environment):
    """Each step is evaluated against what the previous one produced, item by item."""
    steps = expr["steps"]
    result_sequence, tuple_bindings, is_tuple_stream = _walk_steps(
        steps, _first_step_input(context, steps[0]), environment)

    if is_tuple_stream:
        result_sequence = tuple_bindings if expr.get("tuple") else Sequence(bound["@"] for bound in tuple_bindings)
    if expr.get("keepSingletonArray"):
        result_sequence = _keep_singleton_array(result_sequence)
    if "group" in expr:
        grouped = tuple_bindings if is_tuple_stream else result_sequence
        result_sequence = _evaluate_group(expr["group"], grouped, environment)
    return result_sequence


def _first_step_input(context, first_step):
    """What the first step runs over: the members of an array context, item by item, unless the
    step is the variable that names the array itself."""
    if isinstance(context, list) and first_step["type"] != "variable":
        return context
    return Sequence([context])


def _walk_steps(steps, input_sequence, environment):
    """Every step over what the one before it produced, stopping as soon as one selects nothing.

    A step that binds a context or an index variable turns the walk into a tuple stream, and from
    there each value travels with those bindings instead of on its own, which is why the walk
    answers with both the plain sequence and the tuple bindings.
    """
    result_sequence = UNDEFINED
    tuple_bindings = UNDEFINED
    is_tuple_stream = False
    for index, step in enumerate(steps):
        is_tuple_stream = is_tuple_stream or bool(step.get("tuple"))
        if index == 0 and step.get("consarray"):
            result_sequence = _evaluate(step, input_sequence, environment)
        elif is_tuple_stream:
            tuple_bindings = _evaluate_tuple_step(step, input_sequence, tuple_bindings, environment)
        else:
            result_sequence = _evaluate_step(step, input_sequence, environment, index == len(steps) - 1)
        if not is_tuple_stream and (result_sequence is UNDEFINED or len(result_sequence) == 0):
            break
        if "focus" not in step:
            input_sequence = result_sequence
    return result_sequence, tuple_bindings, is_tuple_stream


def _keep_singleton_array(result_sequence):
    """`[]` on a step: the result stays an array even when it holds a single value."""
    constructed = getattr(result_sequence, "cons", False) and not is_sequence(result_sequence)
    if constructed:
        result_sequence = Sequence([result_sequence])
    if is_sequence(result_sequence):
        result_sequence.keep_singleton = True
    return result_sequence


def _evaluate_step(expr, input_sequence, environment, last_step):
    """One step over every item of the input, then the flattening that defines a path's shape."""
    if expr["type"] == "sort":
        result = _evaluate_sort(expr, input_sequence, environment)
        if "stages" in expr:
            result = _evaluate_stages(expr["stages"], result, environment)
        return result

    result = Sequence()
    for item in input_sequence:
        value = _evaluate(expr, item, environment)
        for stage in expr.get("stages", ()):
            value = _evaluate_filter(stage["expr"], value, environment)
        if value is not UNDEFINED:
            result.append(value)

    if last_step and len(result) == 1 and isinstance(result[0], list) and not is_sequence(result[0]):
        return result[0]
    flattened = Sequence()
    for value in result:
        if not isinstance(value, list) or getattr(value, "cons", False):
            flattened.append(value)
        else:
            flattened.extend(value)
    return flattened


def _evaluate_stages(stages, result, environment):
    """The predicates and the index binding the parser peeled off a step, in the order written."""
    for stage in stages:
        if stage["type"] == "filter":
            result = _evaluate_filter(stage["expr"], result, environment)
        else:
            for position, item in enumerate(result):
                item[stage["value"]] = position
    return result


def _evaluate_tuple_step(expr, input_sequence, tuple_bindings, environment):
    """A step in tuple mode: each value travels with the variables the earlier steps bound to it."""
    if expr["type"] == "sort":
        return _evaluate_tuple_sort(expr, input_sequence, tuple_bindings, environment)

    result = Sequence()
    result.tuple_stream = True
    if tuple_bindings is UNDEFINED:
        tuple_bindings = [{"@": item} for item in input_sequence]
    for incoming in tuple_bindings:
        step_environment = _frame_from_tuple(environment, incoming)
        values = _evaluate(expr, incoming["@"], step_environment)
        if values is UNDEFINED:
            continue
        if not isinstance(values, list):
            values = [values]
        for position, value in enumerate(values):
            result.append(_outgoing_tuple(expr, incoming, values, value, position))
    if "stages" in expr:
        result = _evaluate_stages(expr["stages"], result, environment)
    return result


def _outgoing_tuple(expr, incoming, values, value, position):
    """The tuple one step produces: the bindings it arrived with, plus what this step binds."""
    outgoing = dict(incoming)
    if getattr(values, "tuple_stream", False):
        outgoing.update(value)
        return outgoing
    if "focus" in expr:
        outgoing[expr["focus"]] = value
        outgoing["@"] = incoming["@"]
    else:
        outgoing["@"] = value
    if "index" in expr:
        outgoing[expr["index"]] = position
    if "ancestor" in expr:
        outgoing[expr["ancestor"]["label"]] = incoming["@"]
    return outgoing


def _evaluate_tuple_sort(expr, input_sequence, tuple_bindings, environment):
    """Sorting before any binding exists numbers the index by sorted position, not by input order."""
    if tuple_bindings is not UNDEFINED:
        result = _evaluate_sort(expr, tuple_bindings, environment)
    else:
        result = Sequence()
        result.tuple_stream = True
        for position, value in enumerate(_evaluate_sort(expr, input_sequence, environment)):
            item = {"@": value}
            if "index" in expr:
                item[expr["index"]] = position
            result.append(item)
    if "stages" in expr:
        result = _evaluate_stages(expr["stages"], result, environment)
    return result


def _frame_from_tuple(environment, tuple_):
    frame = Frame(environment)
    frame.bindings.update(tuple_)
    return frame


def _evaluate_filter(predicate, input_sequence, environment):
    """A predicate selects by position when it yields numbers, and by truth otherwise."""
    results = Sequence()
    if isinstance(input_sequence, list) and getattr(input_sequence, "tuple_stream", False):
        results.tuple_stream = True
    if not isinstance(input_sequence, list):
        input_sequence = Sequence([input_sequence])
    if predicate["type"] == "number":
        return _filter_by_literal_index(predicate["value"], input_sequence, results)
    for index, item in enumerate(input_sequence):
        context, frame = item, environment
        if getattr(input_sequence, "tuple_stream", False):
            context, frame = item["@"], _frame_from_tuple(environment, item)
        selected = _evaluate(predicate, context, frame)
        if _predicate_keeps(selected, index, len(input_sequence)):
            results.append(item)
    return results


def _predicate_keeps(selected, index, length):
    """Whether what the predicate produced keeps the item at `index`: by position when it is a
    number or an array of numbers, by truth otherwise."""
    if _is_numeric(selected):
        selected = [selected]
    if _is_array_of_numbers(selected):
        return any(_position_in(number, length) == index for number in selected)
    return _truthy(selected)


def _filter_by_literal_index(value, input_sequence, results):
    """`[n]`: the floor first, then counting from the end; an array element replaces the result."""
    index = math.floor(value)
    if index < 0:
        index = len(input_sequence) + index
    if not 0 <= index < len(input_sequence):
        return results
    item = input_sequence[index]
    if item is UNDEFINED:
        return results
    if isinstance(item, list):
        return item
    results.append(item)
    return results


def _position_in(number, length):
    index = math.floor(number)
    return index + length if index < 0 else index


# ---- grouping and sorting -------------------------------------------------------------------

def _evaluate_group(expr, input_sequence, environment):
    """`{key: value}`: every item lands in the group its key expression names."""
    reduce = isinstance(input_sequence, list) and getattr(input_sequence, "tuple_stream", False)
    if not isinstance(input_sequence, list):
        input_sequence = Sequence([input_sequence])
    if len(input_sequence) == 0 and not reduce:
        # One undefined item, so a literal object constructor still produces its object.
        input_sequence = Sequence([UNDEFINED])
    groups = {}
    for item in input_sequence:
        frame = _frame_from_tuple(environment, item) if reduce else environment
        for pair_index, pair in enumerate(expr["lhs"]):
            key = _evaluate(pair[0], item["@"] if reduce else item, frame)
            if key is not UNDEFINED:
                _assign_to_group(groups, _checked_key(key), item, pair_index)
    return _evaluate_group_values(expr, groups, reduce, environment)


def _checked_key(key):
    if not isinstance(key, str):
        raise JsonataError("T1003", value=key)
    if key in ("_jsonata_lambda", "_jsonata_function"):
        raise JsonataError("D1013", value=key)
    return key


def _assign_to_group(groups, key, item, pair_index):
    if key not in groups:
        groups[key] = [item, pair_index]
        return
    if groups[key][1] != pair_index:
        raise JsonataError("D1009", value=key)
    groups[key][0] = _append(groups[key][0], item)


def _evaluate_group_values(expr, groups, reduce, environment):
    result = {}
    for key, (data, pair_index) in groups.items():
        context, frame = data, environment
        if reduce:
            tuple_ = dict(_reduce_tuple_stream(data))
            context = tuple_.pop("@", UNDEFINED)
            frame = _frame_from_tuple(environment, tuple_)
        value = _evaluate(expr["lhs"][pair_index][1], context, frame)
        if value is not UNDEFINED:
            result[key] = value
    return result


def _reduce_tuple_stream(data):
    """The tuples of one group merged into one, every binding appended in the order they arrived."""
    if not isinstance(data, list):
        return data
    merged = dict(data[0])
    for tuple_ in data[1:]:
        for name, value in tuple_.items():
            merged[name] = _append(merged.get(name, UNDEFINED), value)
    return merged


def _evaluate_sort(expr, input_sequence, environment):
    """`^(term, …)`: a stable sort, undefined last, that stops at the first term that decides."""
    tuple_sort = isinstance(input_sequence, list) and getattr(input_sequence, "tuple_stream", False)

    def compare(earlier, later):
        for term in expr["terms"]:
            order = _compare_by_term(term, earlier, later, environment, tuple_sort)
            if order != 0:
                return order
        return 0

    if not isinstance(input_sequence, list) or len(input_sequence) <= 1:
        return input_sequence
    # `cmp_to_key` hands the pair back to front, while the reference's merge sort compares the
    # earlier item against the later one. A term that raises names its two values in the order it
    # was handed them, so the pair is put back before `compare` sees it.
    return sorted(input_sequence, key=cmp_to_key(lambda later, earlier: -compare(earlier, later)))


def _compare_by_term(term, left, right, environment, tuple_sort):
    """-1 when `left` sorts first, 1 when it sorts last, 0 when this term cannot tell them apart."""
    first = _evaluate_sort_term(term, left, environment, tuple_sort)
    second = _evaluate_sort_term(term, right, environment, tuple_sort)
    if first is UNDEFINED:
        return 0 if second is UNDEFINED else 1
    if second is UNDEFINED:
        return -1
    if not _is_sortable(first) or not _is_sortable(second):
        raise JsonataError("T2008", value=first if not _is_sortable(first) else second)
    if isinstance(first, str) != isinstance(second, str):
        raise JsonataError("T2007", value=first, value2=second)
    if first == second:
        return 0
    order = -1 if first < second else 1
    return -order if term["descending"] else order


def _evaluate_sort_term(term, item, environment, tuple_sort):
    if tuple_sort:
        return _evaluate(term["expression"], item["@"], _frame_from_tuple(environment, item))
    return _evaluate(term["expression"], item, environment)


def _is_sortable(value):
    return isinstance(value, str) or (isinstance(value, (int, float)) and not isinstance(value, bool))


# ---- operators ------------------------------------------------------------------------------

def _evaluate_binary(expr, context, environment):
    """Every infix operator; only `and` and `or` get to leave the right side unevaluated."""
    operator = expr["value"]
    left = _evaluate(expr["lhs"], context, environment)
    if operator in ("and", "or"):
        return _evaluate_boolean(operator, left, expr["rhs"], context, environment)
    right = _evaluate(expr["rhs"], context, environment)
    if operator in _ARITHMETIC:
        return _evaluate_numeric(operator, left, right)
    if operator in _COMPARISONS:
        return _evaluate_comparison(operator, left, right)
    if operator in ("=", "!="):
        return _evaluate_equality(operator, left, right)
    if operator == "&":
        return _evaluate_concat(left, right)
    if operator == "in":
        return _evaluate_includes(left, right)
    return _evaluate_range(left, right)


def _evaluate_boolean(operator, left, right_expr, context, environment):
    """`and` and `or` answer with a real boolean, and only evaluate the right side if they must."""
    left_truth = _truthy(left)
    if operator == "and" and not left_truth:
        return False
    if operator == "or" and left_truth:
        return True
    return _truthy(_evaluate(right_expr, context, environment))


def _evaluate_numeric(operator, left, right):
    if left is not UNDEFINED and not _is_numeric(left):
        raise JsonataError("T2001", value=left, token=operator)
    if right is not UNDEFINED and not _is_numeric(right):
        raise JsonataError("T2002", value=right, token=operator)
    if left is UNDEFINED or right is UNDEFINED:
        return UNDEFINED
    # JSONata has one number type, the IEEE 754 double. Python's unbounded int would answer
    # `9007199254740992 + 1` with 9007199254740993, a number no JSONata implementation can hold.
    return _as_json_number(_ARITHMETIC[operator](float(left), float(right)))


def _divide(left, right):
    if right != 0:
        return left / right
    if left == 0:
        return math.nan
    return math.copysign(math.inf, left) * math.copysign(1, right)


def _remainder(left, right):
    """The remainder JavaScript computes: it takes the sign of the dividend, `-5 % 3` being -2."""
    if right == 0:
        return math.nan
    return math.fmod(left, right)


def _evaluate_equality(operator, left, right):
    if left is UNDEFINED or right is UNDEFINED:
        return False
    equal = _is_deep_equal(left, right)
    return equal if operator == "=" else not equal


def _is_deep_equal(left, right):
    if left is right:
        return True
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(map(_is_deep_equal, left, right))
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(_is_deep_equal(left[k], right[k]) for k in left)
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    if isinstance(left, str) and isinstance(right, str):
        return left == right
    return False


def _evaluate_comparison(operator, left, right):
    if not _is_comparable(left) or not _is_comparable(right):
        raise JsonataError("T2010", value=left if not _is_comparable(left) else right, token=operator)
    if left is UNDEFINED or right is UNDEFINED:
        return UNDEFINED
    if isinstance(left, str) != isinstance(right, str):
        raise JsonataError("T2009", value=left, value2=right, token=operator)
    return _COMPARISONS[operator](left, right)


def _is_comparable(value):
    return value is UNDEFINED or _is_sortable(value)


def _evaluate_concat(left, right):
    parts = ["" if side is UNDEFINED else _string(side) for side in (left, right)]
    return parts[0] + parts[1]


def _evaluate_includes(left, right):
    if left is UNDEFINED or right is UNDEFINED:
        return False
    candidates = right if isinstance(right, list) else [right]
    # Membership is identity or primitive equality, so a matching object is still not a member.
    return any(candidate is left or _is_same_primitive(candidate, left) for candidate in candidates)


def _is_same_primitive(candidate, value):
    if isinstance(candidate, (list, dict)) or isinstance(value, (list, dict)):
        return False
    if isinstance(candidate, bool) != isinstance(value, bool):
        return False
    return candidate == value


def _evaluate_range(left, right):
    if left is not UNDEFINED and not _is_integral(left):
        raise JsonataError("T2003", value=left)
    if right is not UNDEFINED and not _is_integral(right):
        raise JsonataError("T2004", value=right)
    if left is UNDEFINED or right is UNDEFINED or left > right:
        return UNDEFINED
    size = int(right) - int(left) + 1
    if size > 1e7:
        raise JsonataError("D2014", value=size)
    return Sequence(range(int(left), int(right) + 1))


# ---- functions ------------------------------------------------------------------------------

def _evaluate_lambda(expr, context, environment):
    return Lambda(parameters=[parameter["value"] for parameter in expr["arguments"]],
                  body=expr["body"], environment=environment, context=context,
                  signature=expr.get("signature"), thunk=expr.get("thunk", False))


def _evaluate_function(expr, context, environment, chained=()):
    """A call: the procedure, then the arguments left to right, then the application."""
    procedure = _evaluate(expr["procedure"], context, environment)
    if procedure is UNDEFINED and _names_a_binding(expr["procedure"], environment):
        raise JsonataError("T1005", token=expr["procedure"]["steps"][0]["value"])
    args = list(chained)
    for argument in expr["arguments"]:
        args.append(_evaluate(argument, context, environment))
    return _apply(procedure, args, context, _procedure_name(expr["procedure"]))


def _evaluate_partial(expr, context, environment):
    """`$f(?, 2)`: the written arguments are evaluated, the placeholders are kept for the next call."""
    args = [PLACEHOLDER if _is_placeholder(argument) else _evaluate(argument, context, environment)
            for argument in expr["arguments"]]
    procedure = _evaluate(expr["procedure"], context, environment)
    if procedure is UNDEFINED and _names_a_binding(expr["procedure"], environment):
        raise JsonataError("T1007", token=expr["procedure"]["steps"][0]["value"])
    if isinstance(procedure, Lambda):
        return _partially_applied_lambda(procedure, args)
    if isinstance(procedure, PartialFunction):
        return PartialFunction(procedure.function, _substitute(procedure.arguments, args))
    if callable(procedure):
        return PartialFunction(procedure, args)
    raise JsonataError("T1008", token=_procedure_name(expr["procedure"]))


def _is_placeholder(node):
    return node.get("type") == "operator" and node.get("value") == "?"


def _names_a_binding(procedure, environment):
    """Whether a path-shaped procedure is the name of a binding, i.e. the `$` was forgotten."""
    if procedure["type"] != "path" or procedure["steps"][0]["type"] != "name":
        return False
    bound = environment.lookup(procedure["steps"][0]["value"])
    return bound is not UNDEFINED and bound is not None and bound is not False and bound != "" and bound != 0


def _procedure_name(procedure):
    if procedure["type"] == "path":
        return procedure["steps"][0].get("value")
    return procedure.get("value")


def _partially_applied_lambda(procedure, args):
    frame = Frame(procedure.environment)
    unbound = []
    for index, name in enumerate(procedure.parameters):
        argument = args[index] if index < len(args) else UNDEFINED
        if argument is PLACEHOLDER:
            unbound.append(name)
        else:
            frame.bind(name, argument)
    return Lambda(parameters=unbound, body=procedure.body, environment=frame, context=procedure.context)


def _substitute(template, args):
    """The placeholders of `template` filled from `args`, left to right; the rest stay open."""
    filled = list(template)
    supplied = iter(args)
    for index, slot in enumerate(filled):
        if slot is PLACEHOLDER:
            filled[index] = next(supplied, PLACEHOLDER)
    return filled + list(supplied)


def _apply(procedure, args, context, called_as=None):
    """Apply `procedure`, then keep applying while it hands back a tail call to run.

    `called_as` is the name the call site reached `procedure` by, which a signature error on a
    lambda blames; a builtin carries its own name and needs none.
    """
    result = _apply_inner(procedure, args, context, called_as)
    bounces = 0
    while isinstance(result, Lambda) and result.thunk:
        bounces += 1
        if bounces > MAX_TAIL_CALLS:
            raise JsonataError("U1001")
        call = result.body
        next_procedure = _evaluate(call["procedure"], result.context, result.environment)
        next_args = [_evaluate(argument, result.context, result.environment) for argument in call["arguments"]]
        result = _apply_inner(next_procedure, next_args, context, _procedure_name(call["procedure"]))
    return result


def _apply_inner(procedure, args, context, called_as=None):
    signature = getattr(procedure, "signature", None)
    if signature is not None:
        args = signature.validate(args, context, called_as)
    if isinstance(procedure, Lambda):
        return _apply_procedure(procedure, args)
    if isinstance(procedure, PartialFunction):
        filled = [UNDEFINED if slot is PLACEHOLDER else slot for slot in _substitute(procedure.arguments, args)]
        return _apply(procedure.function, filled, context)
    if callable(procedure):
        return procedure(*args)
    raise JsonataError("T1006")


def _apply_procedure(procedure, args):
    frame = Frame(procedure.environment)
    for index, name in enumerate(procedure.parameters):
        frame.bind(name, args[index] if index < len(args) else UNDEFINED)
    return _evaluate(procedure.body, procedure.context, frame)


def _evaluate_apply_operator(expr, context, environment):
    """`lhs ~> rhs`: an extra first argument for a call, composition when both sides are functions."""
    left = _evaluate(expr["lhs"], context, environment)
    if expr["rhs"]["type"] == "function":
        return _evaluate_function(expr["rhs"], context, environment, chained=(left,))
    function_value = _evaluate(expr["rhs"], context, environment)
    if not callable(function_value):
        raise JsonataError("T2006", value=function_value)
    if callable(left):
        return _composition(left, function_value)
    return _apply(function_value, [left], UNDEFINED)


def _composition(first, second):
    def composed(*args):
        return _apply(second, [_apply(first, list(args), UNDEFINED)], UNDEFINED)

    return composed


def _evaluate_transform(expr, context, environment):
    """`|pattern|update, delete|`: a function that returns a changed copy of what it is given."""

    def transform(subject):
        if subject is UNDEFINED:
            return UNDEFINED
        clone = environment.lookup("clone")
        if not callable(clone):
            raise JsonataError("T2013")
        result = _apply(clone, [subject], UNDEFINED)
        matches = _evaluate(expr["pattern"], result, environment)
        if matches is UNDEFINED:
            return result
        for match in (matches if isinstance(matches, list) else [matches]):
            _update_match(expr, match, environment)
            _delete_from_match(expr, match, environment)
        return result

    transform.signature = Signature("<(oa):o>", 0)
    return transform


def _update_match(expr, match, environment):
    update = _evaluate(expr["update"], match, environment)
    if update is UNDEFINED:
        return
    if not isinstance(update, dict):
        raise JsonataError("T2011", value=update)
    if isinstance(match, dict):
        match.update(update)


def _delete_from_match(expr, match, environment):
    if "delete" not in expr:
        return
    deletions = _evaluate(expr["delete"], match, environment)
    if deletions is UNDEFINED:
        return
    keys = deletions if isinstance(deletions, list) else [deletions]
    if not all(isinstance(key, str) for key in keys):
        raise JsonataError("T2012", value=deletions)
    if isinstance(match, dict):
        for key in keys:
            match.pop(key, None)


# ---- values ---------------------------------------------------------------------------------

def _lookup(value, key):
    """The `key` of an object, or of every object of an array collected into one sequence."""
    if isinstance(value, list):
        found = Sequence()
        for item in value:
            selected = _lookup(item, key)
            if selected is UNDEFINED:
                continue
            if isinstance(selected, list):
                found.extend(selected)
            else:
                found.append(selected)
        return found
    if isinstance(value, dict) and key in value:
        return value[key]
    return UNDEFINED


def _boolean(value):
    return BUILTINS["$boolean"](value)


def _truthy(value):
    """The truth of a value where undefined is not an answer, as a condition or a filter needs it."""
    truth = _boolean(value)
    return False if truth is UNDEFINED else truth


def _string(value):
    return BUILTINS["$string"](value)


def _append(left, right):
    """`$append`, with the sequence markers dropped: what it returns is plain JSON data."""
    joined = BUILTINS["$append"](left, right)
    return list(joined) if isinstance(joined, Sequence) else joined


def _is_numeric(value):
    """Whether arithmetic can read `value`; a number that overflowed the doubles is a D1001."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    if isinstance(value, float):
        if math.isnan(value):
            return False
        if math.isinf(value):
            raise JsonataError("D1001", value=value)
    return True


def _is_array_of_numbers(value):
    return isinstance(value, list) and all(_is_numeric(item) for item in value)


def _is_integral(value):
    return _is_numeric(value) and float(value).is_integer()


def _as_json_number(value):
    """A double whose value is integral prints without a decimal point, so it becomes an int."""
    if isinstance(value, float) and value.is_integer() and abs(value) <= 2**53:
        return int(value)
    return value


def _static_frame():
    """The frame every expression starts from: one binding per builtin, under its bare name."""
    frame = Frame()
    for name, function in BUILTINS.items():
        frame.bind(name[1:], function)
    return frame


_HANDLERS = {
    "path": _evaluate_path,
    "binary": _evaluate_binary,
    "unary": _evaluate_unary,
    "name": _evaluate_name,
    "string": _evaluate_literal,
    "number": _evaluate_literal,
    "value": _evaluate_literal,
    "wildcard": _evaluate_wildcard,
    "descendant": _evaluate_descendants,
    "parent": _evaluate_parent,
    "condition": _evaluate_condition,
    "block": _evaluate_block,
    "bind": _evaluate_bind,
    "regex": _evaluate_regex,
    "function": _evaluate_function,
    "variable": _evaluate_variable,
    "lambda": _evaluate_lambda,
    "partial": _evaluate_partial,
    "apply": _evaluate_apply_operator,
    "transform": _evaluate_transform,
}

_ARITHMETIC = {
    "+": lambda left, right: left + right,
    "-": lambda left, right: left - right,
    "*": lambda left, right: left * right,
    "/": _divide,
    "%": _remainder,
}

_COMPARISONS = {
    "<": lambda left, right: left < right,
    "<=": lambda left, right: left <= right,
    ">": lambda left, right: left > right,
    ">=": lambda left, right: left >= right,
}

