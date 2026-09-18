# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Stage 5 of #1744: strict resource-bound RDS IAM authorization.

No runtime callers or AUTH lookup. Future integration must define and test its
AUTH gate separately from IAMDatabaseAuthenticationEnabled before using this
helper. This is not a TLS check or permission to enable MySQL login.
"""

import re
from dataclasses import dataclass, replace

from ministack.core.iam_evaluator import AuthError, EvalContext, evaluate, resolve_principal
from ministack.core.rds_iam import RdsIamTokenError, VerifiedRdsToken, verify_rds_iam_token
from ministack.core.responses import request_scope


@dataclass(frozen=True)
class RdsIamAuthorizationError:
    """A bounded failure code, with no client token or credential material."""

    code: str


@dataclass(frozen=True)
class AuthorizedRdsConnection:
    identity: VerifiedRdsToken
    resource_arn: str


def _resource_policies(policies, resource_arn):
    # The shared evaluator currently case-folds Resource as well as Action.
    # Database usernames are case-sensitive. Match Resource/NotResource here,
    # then leave action, conditions and deny precedence to the shared engine.
    # Do not change policy behavior for unrelated services in this stage.
    def matches(pattern):
        regex = re.escape(pattern).replace(r"\*", ".*").replace(r"\?", ".")
        return re.fullmatch(regex, resource_arn, re.DOTALL) is not None

    return [[replace(stmt, resources=["*"], not_resources=[]) for stmt in policy
             if (any(matches(p) for p in stmt.resources) if stmt.resources
                 else bool(stmt.not_resources) and not any(matches(p) for p in stmt.not_resources))]
            for policy in policies]


def authorize_rds_iam_token(
    token: str,
    *,
    account_id: str,
    region: str,
    resource_kind: str,
    resource_identifier: str,
    db_user: str,
    reader_endpoint: bool = False,
) -> AuthorizedRdsConnection | RdsIamAuthorizationError | RdsIamTokenError:
    """Verify and authorize against a trusted instance or cluster identifier.

    The future broker must bind these arguments to its resource capability,
    not accept a target/account claimed by the client. Endpoint/port, enablement,
    and the stable dbuser resource ID are read from that resource's scoped state.
    Aurora members use the parent cluster's enablement and resource ID, but the
    member's endpoint. Cluster callers may explicitly select the reader endpoint.
    Cross-account credentials must first assume a role in the resource account.
    A success is a point-in-time decision, not a reusable authorization cache.
    """
    from ministack.services import rds

    if (
        not isinstance(account_id, str) or not re.fullmatch(r"[0-9]{12}", account_id)
        or not isinstance(region, str) or not re.fullmatch(r"[a-z0-9-]+", region)
        or resource_kind not in ("instance", "cluster")
        or not isinstance(resource_identifier, str) or not resource_identifier
        or type(reader_endpoint) is not bool
        or (reader_endpoint and resource_kind != "cluster")
    ):
        return RdsIamAuthorizationError("InvalidTarget")

    store = rds._instances if resource_kind == "instance" else rds._clusters
    target = store.get_scoped(account_id, region, resource_identifier)
    if target is None:
        return RdsIamAuthorizationError("ResourceNotFound")
    engines = ("mysql", "aurora", "aurora-mysql") if resource_kind == "instance" else ("aurora", "aurora-mysql")
    if target.get("Engine") not in engines:
        return RdsIamAuthorizationError("UnsupportedEngine")
    if resource_kind == "instance" and target.get("Engine") != "mysql" and not target.get("DBClusterIdentifier"):
        return RdsIamAuthorizationError("InvalidTarget")

    owner = target
    resource_id_key = "DbiResourceId" if resource_kind == "instance" else "DbClusterResourceId"
    if resource_kind == "instance" and target.get("DBClusterIdentifier"):
        owner = rds._clusters.get_scoped(account_id, region, target["DBClusterIdentifier"])
        if owner is None:
            return RdsIamAuthorizationError("ResourceNotFound")
        if owner.get("Engine") not in ("aurora", "aurora-mysql"):
            return RdsIamAuthorizationError("UnsupportedEngine")
        resource_id_key = "DbClusterResourceId"
    if owner.get("IAMDatabaseAuthenticationEnabled") is not True:
        return RdsIamAuthorizationError("IAMDatabaseAuthenticationDisabled")
    resource_id = owner.get(resource_id_key)
    prefix = "db-" if resource_id_key == "DbiResourceId" else "cluster-"
    if not isinstance(resource_id, str) or not re.fullmatch(prefix + r"[A-Za-z0-9]+", resource_id):
        return RdsIamAuthorizationError("InvalidTarget")

    endpoint = target.get("ReaderEndpoint" if reader_endpoint else "Endpoint")
    if isinstance(endpoint, dict):
        hostname, port = endpoint.get("Address"), endpoint.get("Port")
    else:
        hostname, port = endpoint, target.get("Port")
    verified = verify_rds_iam_token(
        token, hostname=hostname, port=port, db_user=db_user, region=region, account_id=account_id,
    )
    if isinstance(verified, RdsIamTokenError):
        return verified

    # MiniStack's credential and RDS stores currently issue arn:aws identities.
    resource_arn = f"arn:aws:rds-db:{region}:{account_id}:dbuser:{resource_id}/{db_user}"
    # Policy gathering includes ambient-scoped user/group/managed-policy stores.
    # Pin the explicit resource account, restoring both contextvars on all exits.
    with request_scope(account_id, region):
        principal = resolve_principal(verified.access_key_id, account_id)
        if (
            isinstance(principal, AuthError) or principal.account != account_id
            or principal.arn != verified.principal_arn or principal.type != verified.principal_type
        ):
            return RdsIamAuthorizationError("InvalidCredentials")
        if principal.policies is not None:
            result = evaluate(EvalContext(
                principal_arn=principal.arn, principal_type=principal.type,
                principal_account=account_id, action="rds-db:connect",
                resource_arn=resource_arn, region=region,
            ), _resource_policies(principal.policies, resource_arn))
            if result.decision != "Allow":
                return RdsIamAuthorizationError(result.decision)
    return AuthorizedRdsConnection(verified, resource_arn)
