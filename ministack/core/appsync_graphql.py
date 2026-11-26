# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""GraphQL execution for AppSync.

The data plane used to match the query with a regex and split the body with more
regex — no schema, no validation, no execution algorithm. That capped how
correct it could be: fragments, aliases, `@skip`/`@include`, introspection,
variable coercion and validation are not things a regex can do, and each new
client shape found a new way to be mis-parsed.

This runs the real algorithm with graphql-core — the reference implementation
port — against the SDL the API already stores, and hands each field to a
resolver callback that dispatches into the existing data source handlers.

The schema is built once per API and cached against the SDL it came from, so a
schema change invalidates it and nothing else pays for the parse.
"""

from __future__ import annotations

import copy
import logging
import re

from graphql import (
    GraphQLError,
    GraphQLSchema,
    build_schema,
    execute_sync,
    parse,
    validate,
)
from graphql.execution import ExecutionResult
from graphql.language import FieldNode, InlineFragmentNode, VariableNode
from graphql.validation import MaxIntrospectionDepthRule, specified_rules

logger = logging.getLogger("appsync")

# AppSync's own scalars and auth directives are not in the GraphQL spec, so a
# schema using them will not build without declarations. Declaring them is the
# only accommodation the real engine needs.
APPSYNC_PRELUDE = """
scalar AWSDate
scalar AWSTime
scalar AWSDateTime
scalar AWSTimestamp
scalar AWSEmail
scalar AWSJSON
scalar AWSURL
scalar AWSPhone
scalar AWSIPAddress

directive @aws_subscribe(mutations: [String!]!) on FIELD_DEFINITION
directive @aws_auth(cognito_groups: [String!]) on FIELD_DEFINITION | OBJECT
directive @aws_cognito_user_pools(cognito_groups: [String!]) on FIELD_DEFINITION | OBJECT
directive @aws_api_key on FIELD_DEFINITION | OBJECT
directive @aws_iam on FIELD_DEFINITION | OBJECT
directive @aws_oidc on FIELD_DEFINITION | OBJECT
directive @aws_lambda on FIELD_DEFINITION | OBJECT
"""

# api_id -> (sdl_fingerprint, schema)
_schema_cache: dict = {}


class SchemaUnavailable(Exception):
    """The API has no usable schema, so nothing can be validated or executed."""


class QuerySyntaxError(Exception):
    """The request's query is not a parseable GraphQL document."""


def build_api_schema(api_id: str, sdl: str) -> GraphQLSchema:
    """Build (and cache) the executable schema for an API."""
    key = (len(sdl), hash(sdl))
    cached = _schema_cache.get(api_id)
    if cached and cached[0] == key:
        return cached[1]
    try:
        schema = build_schema(APPSYNC_PRELUDE + sdl, assume_valid_sdl=True)
    except GraphQLError as exc:
        raise SchemaUnavailable(f"Schema could not be built: {exc.message}") from exc
    _schema_cache[api_id] = (key, schema)
    return schema


def forget_schema(api_id: str | None = None):
    """Drop a cached schema — on schema replacement, API deletion, or reset."""
    if api_id is None:
        _schema_cache.clear()
    else:
        _schema_cache.pop(api_id, None)


# graphql-core's wording for these failures, rewritten to AppSync's (graphql-java); the bool keeps locations.
_APPSYNC_WORDING = [
    (re.compile(r"^Unknown argument '(\w+)' on field '\w+\.\w+'\.$"),
     "Validation error of type UnknownArgument: Unknown field argument {0} @ '{path}'", True),
    (re.compile(r"^Cannot query field '(\w+)' on type '(\w+)'\..*$", re.S),
     "Validation error of type FieldUndefined: Field '{0}' in type '{1}' is undefined @ '{path}'", True),
    (re.compile(r"^Variable '\$\w+' got invalid value .*; Field '\w+' is not defined by type '(\w+)'\.$", re.S),
     "The variables input contains a field that is not defined for input object type '{0}' ", False),
]


def _field_path(document, target):
    """The names of the fields from the operation root down to the field `target` is or is an argument of."""
    def walk(selection_set, prefix):
        for selection in selection_set.selections if selection_set else ():
            if isinstance(selection, FieldNode):
                path = prefix + [selection.name.value]
                if selection is target or any(arg is target for arg in selection.arguments or ()):
                    return path
                found = walk(selection.selection_set, path)
            elif isinstance(selection, InlineFragmentNode):
                found = walk(selection.selection_set, prefix)
            else:
                continue
            if found:
                return found
        return None

    for definition in document.definitions if document else ():
        found = walk(getattr(definition, "selection_set", None), [])
        if found:
            return "/".join(found)
    return None


def _appsync_wording(err, document):
    """AppSync's message for `err` and whether it keeps its locations, or None when AppSync's wording is not known."""
    for pattern, template, locations in _APPSYNC_WORDING:
        match = pattern.match(err.message)
        if match:
            path = _field_path(document, err.nodes[0]) if err.nodes else None
            return template.format(*match.groups(), path=path or match.group(1)), locations
    return None


def _as_appsync_errors(errors, document=None):
    """Render graphql-core errors the way AppSync renders them."""
    out = []
    for err in errors or []:
        original = getattr(err, "original_error", None)
        message, locations = _appsync_wording(err, document) or (err.message, True)
        entry = {"message": message}
        if err.path:
            entry["path"] = list(err.path)
        if err.locations and locations:
            entry["locations"] = [
                {"line": loc.line, "column": loc.column} for loc in err.locations
            ]
        # A resolver that raised through util.error carries its own type, which
        # clients switch on; keep it rather than flattening to a message.
        error_type = getattr(original, "error_type", None)
        if error_type:
            entry["errorType"] = error_type
        data = getattr(original, "data", None)
        if data is not None:
            entry["data"] = data
        out.append(entry)
    return out


def _unset_arguments_as_null(resolver, variables):
    """AppSync gives an argument bound to an unsent variable as null; inside an input object it stays absent."""
    def resolve(source, info, **args):
        for arg in info.field_nodes[0].arguments:
            if (isinstance(arg.value, VariableNode) and arg.value.name.value not in variables
                    and arg.name.value not in args):
                args[arg.name.value] = None
        return resolver(source, info, **args)
    return resolve


# AppSync has no introspection-depth rule, only queryDepthLimit.
_VALIDATION_RULES = [rule for rule in specified_rules if rule is not MaxIntrospectionDepthRule]

# What AppSync appends once when it cuts a selection at queryDepthLimit.
_DEPTH_LIMIT_ERROR = {
    "path": [],
    "data": None,
    "errorType": "QueryDepthLimitReached",
    "errorInfo": None,
    "locations": [],
    "message": "Query depth limit reached.",
}


class _DepthLimit:
    """Middleware: a field more than `limit` field names deep resolves to null and flags the error."""

    def __init__(self, limit):
        self.limit = limit
        self.reached = False

    def resolve(self, next_, root, info, **args):
        if sum(isinstance(key, str) for key in info.path.as_list()) > self.limit:
            self.reached = True
            return None
        return next_(root, info, **args)


def execute(api_id, sdl, query, variables, operation_name, field_resolver, context, query_depth_limit=0):
    """Parse, validate and execute one operation.

    `field_resolver(source, info, **args)` is called for every field, and is
    where AppSync's resolvers are dispatched. Returns (data, errors) with errors
    already in AppSync's shape.
    """
    schema = build_api_schema(api_id, sdl)

    try:
        document = parse(query)
    except GraphQLError as exc:
        raise QuerySyntaxError(exc.message) from exc

    # Validation is the step the regex could never do: an unknown field or a
    # mistyped variable is refused here instead of resolving to null.
    errors = validate(schema, document, _VALIDATION_RULES)
    if errors:
        return None, _as_appsync_errors(errors, document)

    depth_limit = _DepthLimit(query_depth_limit) if query_depth_limit else None
    result: ExecutionResult = execute_sync(
        schema,
        document,
        variable_values=variables or {},
        operation_name=operation_name,
        context_value=context,
        field_resolver=_unset_arguments_as_null(field_resolver, variables or {}),
        middleware=[depth_limit] if depth_limit else None,
    )
    errors = _as_appsync_errors(result.errors)
    if depth_limit and depth_limit.reached:
        errors.insert(0, copy.deepcopy(_DEPTH_LIMIT_ERROR))
    return result.data, errors
