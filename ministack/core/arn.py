"""Small Amazon Resource Name parsing helpers.

The top-level ARN shape is service-agnostic:

    arn:partition:service:region:account-id:resource

The resource part is service-specific and may itself contain colons or slashes,
so callers should parse that tail separately.
"""

from dataclasses import dataclass

_ARN_PREFIX = "arn:"
_ARN_SECTIONS = 6


class ArnParseError(ValueError):
    """Raised when a string is not shaped like an ARN."""


@dataclass(frozen=True)
class Arn:
    partition: str
    service: str
    region: str
    account_id: str
    resource: str

    def __str__(self) -> str:
        return f"arn:{self.partition}:{self.service}:{self.region}:{self.account_id}:{self.resource}"


def parse_arn(value: str) -> Arn:
    """Parse an ARN into its six top-level sections.

    This mirrors the AWS SDK Go v2 parser behavior: split only the fixed ARN
    header fields and preserve the entire service-specific resource tail.

    Request handlers should validate ``service``, account, and region before
    interpreting ``resource``. Malformed or out-of-scope ARNs should return the
    owning service's normal error shape, not fall back to a same-named local
    resource.
    """
    if not isinstance(value, str) or not value.startswith(_ARN_PREFIX):
        raise ArnParseError("arn: invalid prefix")

    sections = value.split(":", _ARN_SECTIONS - 1)
    if len(sections) != _ARN_SECTIONS:
        raise ArnParseError("arn: not enough sections")

    return Arn(
        partition=sections[1],
        service=sections[2],
        region=sections[3],
        account_id=sections[4],
        resource=sections[5],
    )


def is_arn(value: str) -> bool:
    """Return whether a value is shaped like an ARN."""
    return isinstance(value, str) and value.startswith(_ARN_PREFIX) and value.count(":") >= _ARN_SECTIONS - 1
