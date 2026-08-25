"""Asserts the tree the JSONata parser builds, node by node.

Every expected shape here was taken from the reference implementation (jsonata-js `src/parser.js`,
the parser Step Functions documents itself against), so this file is where a port that drifts from
it gets caught before the evaluator inherits the drift. The cases cover what the evaluator reads:
where a filter lands (`predicate` on a plain step, `stages` once the step belongs to a path), which
step carries a group, a sort or a focus bind, and which slot a `%` resolves to.
"""

import pytest

from ministack.core.jsonata.parser import Signature, parse
from ministack.core.jsonata.values import UNDEFINED, JsonataError

PARSE_TREES = {
    "a.b.c":
        {'type': 'path',
         'steps': [{'value': 'a', 'type': 'name', 'position': 1},
                   {'value': 'b', 'type': 'name', 'position': 3},
                   {'value': 'c', 'type': 'name', 'position': 5}]},
    "a.\"b c\"":
        {'type': 'path',
         'steps': [{'value': 'a', 'type': 'name', 'position': 1},
                   {'value': 'b c', 'type': 'name', 'position': 7}]},
    "[1,2].x":
        {'type': 'path',
         'steps': [{'type': 'unary',
                    'value': '[',
                    'position': 1,
                    'expressions': [{'value': 1, 'type': 'number', 'position': 2},
                                    {'value': 2, 'type': 'number', 'position': 4}],
                    'consarray': True},
                   {'value': 'x', 'type': 'name', 'position': 7}]},
    "a.[b]":
        {'type': 'path',
         'steps': [{'value': 'a', 'type': 'name', 'position': 1},
                   {'type': 'unary',
                    'value': '[',
                    'position': 3,
                    'expressions': [{'type': 'path', 'steps': [{'value': 'b', 'type': 'name', 'position': 4}]}],
                    'consarray': True}]},
    "Account.(Order.Product)":
        {'type': 'path',
         'steps': [{'value': 'Account', 'type': 'name', 'position': 7},
                   {'type': 'block',
                    'position': 9,
                    'expressions': [{'type': 'path',
                                     'steps': [{'value': 'Order', 'type': 'name', 'position': 14},
                                               {'value': 'Product', 'type': 'name', 'position': 22}]}]}]},
    "a[].b":
        {'type': 'path',
         'steps': [{'value': 'a', 'type': 'name', 'position': 1, 'keepArray': True},
                   {'value': 'b', 'type': 'name', 'position': 5}],
         'keepSingletonArray': True,
         'keepArray': True},
    "a[0]":
        {'type': 'path',
         'steps': [{'value': 'a',
                    'type': 'name',
                    'position': 1,
                    'stages': [{'type': 'filter',
                                'expr': {'value': 0, 'type': 'number', 'position': 3},
                                'position': 2}]}]},
    "$x[0]":
        {'value': 'x',
         'type': 'variable',
         'position': 2,
         'predicate': [{'type': 'filter',
                        'expr': {'value': 0, 'type': 'number', 'position': 4},
                        'position': 3}]},
    "$x[0][1]":
        {'value': 'x',
         'type': 'variable',
         'position': 2,
         'predicate': [{'type': 'filter', 'expr': {'value': 0, 'type': 'number', 'position': 4}, 'position': 3},
                       {'type': 'filter',
                        'expr': {'value': 1, 'type': 'number', 'position': 7},
                        'position': 6}]},
    "$x[0].y":
        {'type': 'path',
         'steps': [{'value': 'x',
                    'type': 'variable',
                    'position': 2,
                    'predicate': [{'type': 'filter',
                                   'expr': {'value': 0, 'type': 'number', 'position': 4},
                                   'position': 3}]},
                   {'value': 'y', 'type': 'name', 'position': 7}]},
    "a.b[0]":
        {'type': 'path',
         'steps': [{'value': 'a', 'type': 'name', 'position': 1},
                   {'value': 'b',
                    'type': 'name',
                    'position': 3,
                    'stages': [{'type': 'filter',
                                'expr': {'value': 0, 'type': 'number', 'position': 5},
                                'position': 4}]}]},
    "a[0].b":
        {'type': 'path',
         'steps': [{'value': 'a',
                    'type': 'name',
                    'position': 1,
                    'stages': [{'type': 'filter',
                                'expr': {'value': 0, 'type': 'number', 'position': 3},
                                'position': 2}]},
                   {'value': 'b', 'type': 'name', 'position': 6}]},
    "$x.(y)[0]":
        {'type': 'path',
         'steps': [{'value': 'x', 'type': 'variable', 'position': 2},
                   {'type': 'block',
                    'position': 4,
                    'expressions': [{'type': 'path', 'steps': [{'value': 'y', 'type': 'name', 'position': 5}]}],
                    'stages': [{'type': 'filter',
                                'expr': {'value': 0, 'type': 'number', 'position': 8},
                                'position': 7}]}]},
    "a[0][]":
        {'type': 'path',
         'steps': [{'value': 'a',
                    'type': 'name',
                    'position': 1,
                    'keepArray': True,
                    'stages': [{'type': 'filter',
                                'expr': {'value': 0, 'type': 'number', 'position': 3},
                                'position': 2}]}],
         'keepSingletonArray': True,
         'keepArray': True},
    "a.b{\"k\":v}":
        {'type': 'path',
         'steps': [{'value': 'a', 'type': 'name', 'position': 1},
                   {'value': 'b', 'type': 'name', 'position': 3}],
         'group': {'lhs': [[{'value': 'k', 'type': 'string', 'position': 7},
                            {'type': 'path', 'steps': [{'value': 'v', 'type': 'name', 'position': 9}]}]],
                   'position': 4}},
    "{\"a\":1}":
        {'type': 'unary',
         'value': '{',
         'position': 1,
         'lhs': [[{'value': 'a', 'type': 'string', 'position': 4},
                  {'value': 1, 'type': 'number', 'position': 6}]]},
    "a[b]{c:d}[e]":
        {'type': 'path',
         'steps': [{'value': 'a',
                    'type': 'name',
                    'position': 1,
                    'stages': [{'type': 'filter',
                                'expr': {'type': 'path',
                                         'steps': [{'value': 'b', 'type': 'name', 'position': 3}]},
                                'position': 2},
                               {'type': 'filter',
                                'expr': {'type': 'path',
                                         'steps': [{'value': 'e', 'type': 'name', 'position': 11}]},
                                'position': 10}]}],
         'group': {'lhs': [[{'type': 'path', 'steps': [{'value': 'c', 'type': 'name', 'position': 6}]},
                            {'type': 'path', 'steps': [{'value': 'd', 'type': 'name', 'position': 8}]}]],
                   'position': 5}},
    "a^(>b,c)":
        {'type': 'path',
         'steps': [{'value': 'a', 'type': 'name', 'position': 1},
                   {'type': 'sort',
                    'position': 2,
                    'terms': [{'descending': True,
                               'expression': {'type': 'path',
                                              'steps': [{'value': 'b', 'type': 'name', 'position': 5}]}},
                              {'descending': False,
                               'expression': {'type': 'path',
                                              'steps': [{'value': 'c', 'type': 'name', 'position': 7}]}}]}]},
    "a^(<b)":
        {'type': 'path',
         'steps': [{'value': 'a', 'type': 'name', 'position': 1},
                   {'type': 'sort',
                    'position': 2,
                    'terms': [{'descending': False,
                               'expression': {'type': 'path',
                                              'steps': [{'value': 'b', 'type': 'name', 'position': 5}]}}]}]},
    "|a|b,c|":
        {'type': 'transform',
         'position': 1,
         'pattern': {'type': 'path', 'steps': [{'value': 'a', 'type': 'name', 'position': 2}]},
         'update': {'type': 'path', 'steps': [{'value': 'b', 'type': 'name', 'position': 4}]},
         'delete': {'type': 'path', 'steps': [{'value': 'c', 'type': 'name', 'position': 6}]}},
    "|$|{\"x\":1}|":
        {'type': 'transform',
         'position': 1,
         'pattern': {'value': '', 'type': 'variable', 'position': 2},
         'update': {'type': 'unary',
                    'value': '{',
                    'position': 4,
                    'lhs': [[{'value': 'x', 'type': 'string', 'position': 7},
                             {'value': 1, 'type': 'number', 'position': 9}]]}},
    "a[]~>$f":
        {'type': 'apply',
         'value': '~>',
         'position': 5,
         'lhs': {'type': 'path',
                 'steps': [{'value': 'a', 'type': 'name', 'position': 1, 'keepArray': True}],
                 'keepSingletonArray': True,
                 'keepArray': True},
         'rhs': {'value': 'f', 'type': 'variable', 'position': 7},
         'keepArray': True},
    "$x~>$f~>$g":
        {'type': 'apply',
         'value': '~>',
         'position': 8,
         'lhs': {'type': 'apply',
                 'value': '~>',
                 'position': 4,
                 'lhs': {'value': 'x', 'type': 'variable', 'position': 2},
                 'rhs': {'value': 'f', 'type': 'variable', 'position': 6}},
         'rhs': {'value': 'g', 'type': 'variable', 'position': 10}},
    "$x:=1":
        {'type': 'bind',
         'value': ':=',
         'position': 4,
         'lhs': {'value': 'x', 'type': 'variable', 'position': 2},
         'rhs': {'value': 1, 'type': 'number', 'position': 5}},
    "$a:=$b:=1":
        {'type': 'bind',
         'value': ':=',
         'position': 4,
         'lhs': {'value': 'a', 'type': 'variable', 'position': 2},
         'rhs': {'type': 'bind',
                 'value': ':=',
                 'position': 8,
                 'lhs': {'value': 'b', 'type': 'variable', 'position': 6},
                 'rhs': {'value': 1, 'type': 'number', 'position': 9}}},
    "(1;2)":
        {'type': 'block',
         'position': 1,
         'expressions': [{'value': 1, 'type': 'number', 'position': 2},
                         {'value': 2, 'type': 'number', 'position': 4}]},
    "()":
        {'type': 'block', 'position': 1, 'expressions': []},
    "λ($x){$x}":
        {'type': 'lambda',
         'arguments': [{'value': 'x', 'type': 'variable', 'position': 4}],
         'position': 2,
         'body': {'value': 'x', 'type': 'variable', 'position': 8}},
    "function($x){$f($x)}":
        {'type': 'lambda',
         'arguments': [{'value': 'x', 'type': 'variable', 'position': 11}],
         'position': 9,
         'body': {'type': 'lambda',
                  'thunk': True,
                  'arguments': [],
                  'position': 16,
                  'body': {'type': 'function',
                           'value': '(',
                           'position': 16,
                           'arguments': [{'value': 'x', 'type': 'variable', 'position': 18}],
                           'procedure': {'value': 'f', 'type': 'variable', 'position': 15}}}},
    "a@$x[0]":
        {'type': 'path',
         'steps': [{'value': 'a',
                    'type': 'name',
                    'position': 1,
                    'focus': 'x',
                    'tuple': True,
                    'stages': [{'type': 'filter',
                                'expr': {'value': 0, 'type': 'number', 'position': 6},
                                'position': 5}]}]},
    "$x#$i":
        {'type': 'path',
         'steps': [{'value': 'x', 'type': 'variable', 'position': 2, 'index': 'i', 'tuple': True}]},
    "a[0]#$i":
        {'type': 'path',
         'steps': [{'value': 'a',
                    'type': 'name',
                    'position': 1,
                    'stages': [{'type': 'filter',
                                'expr': {'value': 0, 'type': 'number', 'position': 3},
                                'position': 2},
                               {'type': 'index', 'value': 'i', 'position': 5}],
                    'tuple': True}]},
    "$x[0]#$i":
        {'type': 'path',
         'steps': [{'value': 'x',
                    'type': 'variable',
                    'position': 2,
                    'stages': [{'type': 'filter',
                                'expr': {'value': 0, 'type': 'number', 'position': 4},
                                'position': 3},
                               {'type': 'index', 'value': 'i', 'position': 6}],
                    'tuple': True}]},
    "$x(1)@$y":
        {'type': 'function',
         'value': '(',
         'position': 3,
         'arguments': [{'value': 1, 'type': 'number', 'position': 4}],
         'procedure': {'value': 'x', 'type': 'variable', 'position': 2},
         'focus': 'y',
         'tuple': True},
    "a.b.%.c":
        {'type': 'path',
         'steps': [{'value': 'a', 'type': 'name', 'position': 1},
                   {'value': 'b',
                    'type': 'name',
                    'position': 3,
                    'ancestor': {'label': '!0', 'level': 0, 'index': 0},
                    'tuple': True},
                   {'type': 'parent', 'slot': {'label': '!0', 'level': 0, 'index': 0}},
                   {'value': 'c', 'type': 'name', 'position': 7}]},
    "a.b.(%.%.c)":
        {'type': 'path',
         'steps': [{'value': 'a',
                    'type': 'name',
                    'position': 1,
                    'ancestor': {'label': '!1', 'level': 0, 'index': 1},
                    'tuple': True},
                   {'value': 'b',
                    'type': 'name',
                    'position': 3,
                    'ancestor': {'label': '!0', 'level': 0, 'index': 0},
                    'tuple': True},
                   {'type': 'block',
                    'position': 5,
                    'seekingParent': [{'label': '!0', 'level': 0, 'index': 0},
                                      {'label': '!1', 'level': 0, 'index': 1}],
                    'expressions': [{'type': 'path',
                                     'steps': [{'type': 'parent',
                                                'slot': {'label': '!0', 'level': 0, 'index': 0}},
                                               {'type': 'parent',
                                                'slot': {'label': '!1', 'level': 0, 'index': 1}},
                                               {'value': 'c', 'type': 'name', 'position': 10}],
                                     'seekingParent': [{'label': '!0', 'level': 0, 'index': 0},
                                                       {'label': '!1', 'level': 0, 'index': 1}]}]}]},
    "[1..5]":
        {'type': 'unary',
         'value': '[',
         'position': 1,
         'expressions': [{'type': 'binary',
                          'value': '..',
                          'position': 4,
                          'lhs': {'value': 1, 'type': 'number', 'position': 2},
                          'rhs': {'value': 5, 'type': 'number', 'position': 5}}]},
    "[1,2,3]":
        {'type': 'unary',
         'value': '[',
         'position': 1,
         'expressions': [{'value': 1, 'type': 'number', 'position': 2},
                         {'value': 2, 'type': 'number', 'position': 4},
                         {'value': 3, 'type': 'number', 'position': 6}]},
    "[]":
        {'type': 'unary', 'value': '[', 'position': 1, 'expressions': []},
    "a?b":
        {'type': 'condition',
         'position': 2,
         'condition': {'type': 'path', 'steps': [{'value': 'a', 'type': 'name', 'position': 1}]},
         'then': {'type': 'path', 'steps': [{'value': 'b', 'type': 'name', 'position': 3}]}},
    "a?b:c":
        {'type': 'condition',
         'position': 2,
         'condition': {'type': 'path', 'steps': [{'value': 'a', 'type': 'name', 'position': 1}]},
         'then': {'type': 'path', 'steps': [{'value': 'b', 'type': 'name', 'position': 3}]},
         'else': {'type': 'path', 'steps': [{'value': 'c', 'type': 'name', 'position': 5}]}},
    "a?:b":
        {'type': 'condition',
         'position': 3,
         'condition': {'type': 'path', 'steps': [{'value': 'a', 'type': 'name', 'position': 1}]},
         'then': {'type': 'path', 'steps': [{'value': 'a', 'type': 'name', 'position': 1}]},
         'else': {'type': 'path', 'steps': [{'value': 'b', 'type': 'name', 'position': 4}]}},
    "a??b":
        {'type': 'condition',
         'position': 3,
         'condition': {'type': 'function',
                       'value': '(',
                       'arguments': [{'type': 'path',
                                      'steps': [{'value': 'a', 'type': 'name', 'position': 1}]}],
                       'procedure': {'type': 'variable', 'value': 'exists'}},
         'then': {'type': 'path', 'steps': [{'value': 'a', 'type': 'name', 'position': 1}]},
         'else': {'type': 'path', 'steps': [{'value': 'b', 'type': 'name', 'position': 4}]}},
    "-3?:4":
        {'type': 'condition',
         'position': 4,
         'condition': {'value': -3, 'type': 'number', 'position': 2},
         'then': {'value': -3, 'type': 'number', 'position': 2},
         'else': {'value': 4, 'type': 'number', 'position': 5}},
    "a[0]??b":
        {'type': 'condition',
         'position': 6,
         'condition': {'type': 'function',
                       'value': '(',
                       'arguments': [{'type': 'path',
                                      'steps': [{'value': 'a',
                                                 'type': 'name',
                                                 'position': 1,
                                                 'stages': [{'type': 'filter',
                                                             'expr': {'value': 0,
                                                                      'type': 'number',
                                                                      'position': 3},
                                                             'position': 2}]}]}],
                       'procedure': {'type': 'variable', 'value': 'exists'}},
         'then': {'type': 'path',
                  'steps': [{'value': 'a',
                             'type': 'name',
                             'position': 1,
                             'stages': [{'type': 'filter',
                                         'expr': {'value': 0, 'type': 'number', 'position': 3},
                                         'position': 2}]}]},
         'else': {'type': 'path', 'steps': [{'value': 'b', 'type': 'name', 'position': 7}]}},
    "a.*":
        {'type': 'path',
         'steps': [{'value': 'a', 'type': 'name', 'position': 1},
                   {'value': '*', 'type': 'wildcard', 'position': 3}]},
    "a.**.b":
        {'type': 'path',
         'steps': [{'value': 'a', 'type': 'name', 'position': 1},
                   {'value': '**', 'type': 'descendant', 'position': 4},
                   {'value': 'b', 'type': 'name', 'position': 6}]},
    "*":
        {'value': '*', 'type': 'wildcard', 'position': 1},
    "$f(?,2)":
        {'type': 'partial',
         'value': '(',
         'position': 3,
         'arguments': [{'value': '?', 'type': 'operator', 'position': 4},
                       {'value': 2, 'type': 'number', 'position': 6}],
         'procedure': {'value': 'f', 'type': 'variable', 'position': 2}},
    "in.or":
        {'type': 'path',
         'steps': [{'value': 'in', 'type': 'name', 'position': 2},
                   {'value': 'or', 'type': 'name', 'position': 5}]},
    "1+2*3":
        {'type': 'binary',
         'value': '+',
         'position': 2,
         'lhs': {'value': 1, 'type': 'number', 'position': 1},
         'rhs': {'type': 'binary',
                 'value': '*',
                 'position': 4,
                 'lhs': {'value': 2, 'type': 'number', 'position': 3},
                 'rhs': {'value': 3, 'type': 'number', 'position': 5}}},
    "-a":
        {'type': 'unary',
         'value': '-',
         'position': 1,
         'expression': {'type': 'path', 'steps': [{'value': 'a', 'type': 'name', 'position': 2}]}},
    "$":
        {'value': '', 'type': 'variable', 'position': 1},
    "$$":
        {'value': '$', 'type': 'variable', 'position': 2},
}

SYNTAX_ERRORS = [
    ('"a', "S0101", 2, None, None),
    ("1e1000", "S0102", 0, "1e1000", None),
    ('"a\\qb"', "S0103", 3, "q", None),
    ('"\\uZZZZ"', "S0104", 2, None, None),
    ("`ab", "S0105", 3, None, None),
    ("/* c", "S0106", 0, None, None),
    ("1 2", "S0201", 3, 2, None),
    ("a**b", "S0201", 3, "**", None),
    ("[1,2)", "S0202", 5, ")", "]"),
    ("{", "S0203", 1, "(end)", ":"),
    ("a!b", "S0204", 2, "!", None),
    ("$x := ", "S0207", 6, "(end)", None),
    ("function(1){2}", "S0208", 10, 1, 1),
    ('[1,2,3]{"num": $}[true]', "S0209", 18, None, None),
    ('a{"a":1}{"b":2}', "S0210", 9, None, None),
    ("@ bar", "S0211", 1, "@", None),
    ("1:=2", "S0212", 1, 1, None),
    ("a.1", "S0213", 3, None, 1),
    ("a.true", "S0213", 6, None, True),
    ("a@1", "S0214", 3, "@", None),
    ("a[0]@$x", "S0215", 5, None, None),
    ("a^(b)@$x", "S0216", 6, None, None),
    ("%.a", "S0217", None, "path", None),
    ("library.loans.%.%.%", "S0217", None, "path", None),
    ("//", "S0301", 1, None, None),
    ("/a", "S0302", 2, None, None),
    ("λ($arg)<n<n>>{$arg}(5)", "S0401", 10, None, "n"),
    ("λ($arr)<(sa<n>)>>{$arr}([[1]])", "S0402", 9, None, "sa<n>"),
]


def shape(node):
    """The tree without `id`, the symbol name the parser carries for its own dispatch."""
    if isinstance(node, dict):
        return {key: shape(value) for key, value in node.items() if key != "id"}
    if isinstance(node, list):
        return [shape(item) for item in node]
    return node


@pytest.mark.parametrize("expression", list(PARSE_TREES), ids=list(PARSE_TREES))
def test_parse_tree(expression):
    assert shape(parse(expression)) == PARSE_TREES[expression]


@pytest.mark.parametrize("expression, code, position, token, value", SYNTAX_ERRORS,
                         ids=[case[0] for case in SYNTAX_ERRORS])
def test_syntax_error(expression, code, position, token, value):
    with pytest.raises(JsonataError) as raised:
        parse(expression)
    assert (raised.value.code, raised.value.position) == (code, position)
    assert (raised.value.token, raised.value.value) == (token, value)


def test_lambda_with_signature():
    tree = parse("function($x,$y)<nn:n>{$x+$y}")
    assert tree["signature"].definition == "<nn:n>"
    assert shape(tree["arguments"]) == [{'value': 'x', 'type': 'variable', 'position': 11},
                                        {'value': 'y', 'type': 'variable', 'position': 14}]
    assert shape(tree["body"]) == {'type': 'binary',
                                   'value': '+',
                                   'position': 25,
                                   'lhs': {'value': 'x', 'type': 'variable', 'position': 24},
                                   'rhs': {'value': 'y', 'type': 'variable', 'position': 27}}


def test_lambda_signature_ignores_an_unknown_symbol():
    """`q` names no type, and the signature mini-language drops what it does not recognise."""
    tree = parse("function($x)<q>{1}")
    assert tree["signature"].definition == "<q>"
    assert tree["signature"].params == []


def test_lambda_without_signature_has_no_signature_key():
    assert "signature" not in parse("λ($x){$x}")


# ---- Signature.validate names the function a T0410/T0411/T0412 blames ---------------------------

def test_a_signature_with_no_token_attached_blames_undefined():
    """A lambda's own signature is never given a name here, matching an anonymous call site."""
    signature = Signature("<n:n>", 0)
    with pytest.raises(JsonataError) as failure:
        signature.validate(["not a number"], UNDEFINED)
    assert failure.value.code == "T0410"
    assert failure.value.token is UNDEFINED
    assert failure.value.message == "Argument 1 of function undefined does not match function signature"


def test_a_bad_argument_names_the_signatures_token():
    signature = Signature("<n:n>", 0)
    signature.token = "min"
    with pytest.raises(JsonataError) as failure:
        signature.validate(["not a number"], UNDEFINED)
    assert failure.value.code == "T0410"
    assert failure.value.token == "min"
    assert failure.value.message == 'Argument 1 of function "min" does not match function signature'


def test_a_context_of_the_wrong_type_names_the_signatures_token():
    signature = Signature("<n-:n>", 0)
    signature.token = "length"
    with pytest.raises(JsonataError) as failure:
        signature.validate([], "not a number")
    assert failure.value.code == "T0411"
    assert failure.value.token == "length"
    assert failure.value.message == (
        'Context value is not a compatible type with argument 1 of function "length"')


def test_an_array_of_the_wrong_item_type_names_the_signatures_token():
    signature = Signature("<a<n>:a>", 0)
    signature.token = "sum"
    with pytest.raises(JsonataError) as failure:
        signature.validate(["not an array of numbers"], UNDEFINED)
    assert failure.value.code == "T0412"
    assert failure.value.token == "sum"
    assert failure.value.message == 'Argument 1 of function "sum" must be an array of "numbers"'


def test_parent_reuses_one_slot_object_across_the_tree():
    """The slot is mutated as it travels, so the ancestor step and the `%` step must share it."""
    steps = parse("a.b.%.c")["steps"]
    assert steps[1]["ancestor"] is steps[2]["slot"]


def test_two_parents_resolving_to_one_step_share_a_label():
    steps = parse("a.b.(%.%.c)")["steps"]
    seeking = steps[2]["seekingParent"]
    assert [slot["label"] for slot in seeking] == ["!0", "!1"]
    assert steps[1]["ancestor"] is seeking[0]
    assert steps[0]["ancestor"] is seeking[1]


def test_elvis_gives_its_two_branches_independent_nodes():
    """A shared node would take the predicate-to-stages move twice and end up with two filters."""
    tree = parse("a[0]?:b")
    assert tree["condition"] is not tree["then"]
    assert len(tree["condition"]["steps"][0]["stages"]) == 1
    assert len(tree["then"]["steps"][0]["stages"]) == 1


def test_coalesce_gives_its_two_branches_independent_nodes():
    tree = parse("a[0]??b")
    assert tree["condition"]["arguments"][0] is not tree["then"]
