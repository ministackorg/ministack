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

import logging

from graphql import (
    GraphQLError,
    GraphQLSchema,
    build_schema,
    execute_sync,
    parse,
    validate,
)
from graphql.execution import ExecutionResult

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


def _as_appsync_errors(errors):
    """Render graphql-core errors the way AppSync renders them."""
    out = []
    for err in errors or []:
        original = getattr(err, "original_error", None)
        entry = {"message": err.message}
        if err.path:
            entry["path"] = list(err.path)
        if err.locations:
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


def execute(api_id, sdl, query, variables, operation_name, field_resolver, context):
    """Parse, validate and execute one operation.

    `field_resolver(source, info, **args)` is called for every field, and is
    where AppSync's resolvers are dispatched. Returns (data, errors) with errors
    already in AppSync's shape.
    """
    schema = build_api_schema(api_id, sdl)

    try:
        document = parse(query)
    except GraphQLError as exc:
        return None, _as_appsync_errors([exc])

    # Validation is the step the regex could never do: an unknown field or a
    # mistyped variable is refused here instead of resolving to null.
    errors = validate(schema, document)
    if errors:
        return None, _as_appsync_errors(errors)

    result: ExecutionResult = execute_sync(
        schema,
        document,
        variable_values=variables or {},
        operation_name=operation_name,
        context_value=context,
        field_resolver=field_resolver,
    )
    return result.data, _as_appsync_errors(result.errors)
