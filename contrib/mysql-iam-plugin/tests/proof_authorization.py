"""Fixture-only bootstrap of real MiniStack authorizer state, not provisioning."""
import os

from ministack.core import rds_iam
from ministack.services import iam, rds

ACCOUNT = "123456789012"
REGION = "us-east-1"
HOST = "database.spike.us-east-1.rds.amazonaws.com"
KEY = "AKIASPIKELOCALTEST"


def bootstrap():
    iam._users.set_scoped(ACCOUNT, None, "spike", {
        "UserName": "spike", "Arn": f"arn:aws:iam::{ACCOUNT}:user/spike",
    })
    iam._access_keys.set_scoped(ACCOUNT, None, KEY, {
        "UserName": "spike", "Status": "Active", "SecretAccessKey": os.environ["SPIKE_SECRET"],
    })
    allowed = ["iam_user", "iam_locked", "dynamic_user"]
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "spike", {"connect": {
        "Statement": [{"Effect": "Allow", "Action": "rds-db:connect", "Resource": [
            f"arn:aws:rds-db:{REGION}:{ACCOUNT}:dbuser:db-SPIKE/{u}" for u in allowed
        ]}],
    }})
    rds._instances.set_scoped(ACCOUNT, REGION, "spike", {
        "Engine": "mysql", "DbiResourceId": "db-SPIKE",
        "IAMDatabaseAuthenticationEnabled": True,
        "Endpoint": {"Address": HOST, "Port": 3306},
    })
    cap = rds_iam.issue_capability(account_id=ACCOUNT, region=REGION,
                                   resource_kind="instance", resource_identifier="spike")

    def authorize(user, token, strict):
        # Reuse the existing capability/current-resource/AUTH decision. A
        # production refactor should expose this private seam as a public API.
        return rds_iam._decision(cap.encode(), {"username": user, "token": token}, strict)

    return authorize
