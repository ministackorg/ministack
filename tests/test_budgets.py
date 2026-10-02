import datetime
import uuid

import pytest
from botocore.exceptions import ClientError

# The AccountId request field only; MiniStack scopes state by the real
# credential-derived account (real_account_id below), so this placeholder is harmless.
ACCOUNT_ID = "000000000000"


@pytest.fixture(scope="session")
def real_account_id(sts):
    """The account MiniStack actually scopes state under; budget ARNs must use it, not ACCOUNT_ID."""
    return sts.get_caller_identity()["Account"]


def _uid() -> str:
    return uuid.uuid4().hex[:8]


def _budget(name: str, **overrides) -> dict:
    budget = {
        "BudgetName": name,
        "BudgetType": "COST",
        "TimeUnit": "MONTHLY",
        "BudgetLimit": {"Amount": "500", "Unit": "USD"},
    }
    budget.update(overrides)
    return budget


def _notification(**overrides) -> dict:
    notification = {
        "NotificationType": "ACTUAL",
        "ComparisonOperator": "GREATER_THAN",
        "Threshold": 90.0,
        "ThresholdType": "PERCENTAGE",
    }
    notification.update(overrides)
    return notification


# -- CreateBudget / DescribeBudget / DescribeBudgets -------------------------


def test_budgets_create_and_describe(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))

    resp = budgets.describe_budget(AccountId=ACCOUNT_ID, BudgetName=name)
    budget = resp["Budget"]
    assert budget["BudgetName"] == name
    assert budget["BudgetType"] == "COST"
    assert budget["TimeUnit"] == "MONTHLY"
    assert budget["BudgetLimit"] == {"Amount": "500", "Unit": "USD"}
    # A brand-new budget reports zero spend, never an invented nonzero value.
    assert budget["CalculatedSpend"]["ActualSpend"] == {"Amount": "0", "Unit": "USD"}
    assert "ForecastedSpend" not in budget["CalculatedSpend"]
    assert "LastUpdatedTime" in budget


def test_budgets_time_period_defaults_to_current_month_and_2087(budgets):
    """AWS Budgets API Reference, Budget.TimePeriod: an omitted Start defaults
    to the start of the budget's TimeUnit period and an omitted End to
    06/15/87 00:00 UTC."""
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))

    budget = budgets.describe_budget(AccountId=ACCOUNT_ID, BudgetName=name)["Budget"]
    period = budget["TimePeriod"]

    created = budget["LastUpdatedTime"].astimezone(datetime.timezone.utc)
    expected_start = created.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    assert period["Start"] == expected_start
    assert period["End"] == datetime.datetime(2087, 6, 15, tzinfo=datetime.timezone.utc)


def test_budgets_create_duplicate_rejected(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))

    with pytest.raises(ClientError) as exc_info:
        budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))

    assert exc_info.value.response["Error"]["Code"] == "DuplicateRecordException"
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_budgets_describe_budget_not_found(budgets):
    with pytest.raises(ClientError) as exc_info:
        budgets.describe_budget(AccountId=ACCOUNT_ID, BudgetName=f"missing-{_uid()}")

    assert exc_info.value.response["Error"]["Code"] == "NotFoundException"


def test_budgets_create_invalid_time_unit_rejected(budgets):
    with pytest.raises(ClientError) as exc_info:
        budgets.create_budget(
            AccountId=ACCOUNT_ID,
            Budget=_budget(f"budget-{_uid()}", TimeUnit="WEEKLY"),
        )

    assert exc_info.value.response["Error"]["Code"] == "InvalidParameterException"


def test_budgets_describe_budgets_lists_created(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))

    resp = budgets.describe_budgets(AccountId=ACCOUNT_ID)
    names = {b["BudgetName"] for b in resp["Budgets"]}
    assert name in names


def test_budgets_describe_budgets_pagination(budgets):
    prefix = f"page-{_uid()}"
    created = {f"{prefix}-{i}" for i in range(3)}
    for name in created:
        budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))

    seen = []
    next_token = None
    while True:
        kwargs = {"AccountId": ACCOUNT_ID, "MaxResults": 1}
        if next_token:
            kwargs["NextToken"] = next_token
        page = budgets.describe_budgets(**kwargs)
        assert len(page["Budgets"]) == 1
        seen.append(page["Budgets"][0]["BudgetName"])
        next_token = page.get("NextToken")
        if not next_token:
            break

    assert len(seen) == len(set(seen))
    assert created <= set(seen)


def test_budgets_update_budget(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))

    budgets.update_budget(
        AccountId=ACCOUNT_ID,
        NewBudget=_budget(name, BudgetLimit={"Amount": "999", "Unit": "USD"}),
    )

    resp = budgets.describe_budget(AccountId=ACCOUNT_ID, BudgetName=name)
    assert resp["Budget"]["BudgetLimit"] == {"Amount": "999", "Unit": "USD"}


def test_budgets_update_budget_keeps_time_period_when_omitted(budgets):
    name = f"budget-{_uid()}"
    start = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name, TimePeriod={"Start": start}))

    budgets.update_budget(AccountId=ACCOUNT_ID, NewBudget=_budget(name, BudgetLimit={"Amount": "999", "Unit": "USD"}))

    resp = budgets.describe_budget(AccountId=ACCOUNT_ID, BudgetName=name)
    assert resp["Budget"]["TimePeriod"]["Start"] == start


def test_budgets_update_budget_not_found(budgets):
    with pytest.raises(ClientError) as exc_info:
        budgets.update_budget(AccountId=ACCOUNT_ID, NewBudget=_budget(f"missing-{_uid()}"))

    assert exc_info.value.response["Error"]["Code"] == "NotFoundException"


def test_budgets_delete_budget(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))

    budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=name)

    with pytest.raises(ClientError) as exc_info:
        budgets.describe_budget(AccountId=ACCOUNT_ID, BudgetName=name)
    assert exc_info.value.response["Error"]["Code"] == "NotFoundException"


def test_budgets_delete_budget_not_found(budgets):
    with pytest.raises(ClientError) as exc_info:
        budgets.delete_budget(AccountId=ACCOUNT_ID, BudgetName=f"missing-{_uid()}")

    assert exc_info.value.response["Error"]["Code"] == "NotFoundException"


# -- CreateBudget with inline NotificationsWithSubscribers -------------------


def test_budgets_create_budget_with_inline_notifications(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(
        AccountId=ACCOUNT_ID,
        Budget=_budget(name),
        NotificationsWithSubscribers=[
            {
                "Notification": _notification(NotificationType="FORECASTED", Threshold=80.0),
                "Subscribers": [{"SubscriptionType": "SNS", "Address": "arn:aws:sns:us-east-1:000000000000:alerts"}],
            },
            {
                "Notification": _notification(NotificationType="ACTUAL", Threshold=90.0),
                "Subscribers": [{"SubscriptionType": "SNS", "Address": "arn:aws:sns:us-east-1:000000000000:alerts"}],
            },
            {
                "Notification": _notification(NotificationType="ACTUAL", Threshold=100.0),
                "Subscribers": [{"SubscriptionType": "SNS", "Address": "arn:aws:sns:us-east-1:000000000000:alerts"}],
            },
        ],
    )

    resp = budgets.describe_notifications_for_budget(AccountId=ACCOUNT_ID, BudgetName=name)
    thresholds = sorted(n["Threshold"] for n in resp["Notifications"])
    assert thresholds == [80.0, 90.0, 100.0]
    for notification in resp["Notifications"]:
        assert notification["NotificationState"] == "OK"


def test_budgets_create_budget_notification_limit_exceeded(budgets):
    name = f"budget-{_uid()}"
    entries = [
        {
            "Notification": _notification(Threshold=float(t)),
            "Subscribers": [{"SubscriptionType": "EMAIL", "Address": "a@example.com"}],
        }
        for t in range(10)
    ]
    # boto3 client-side validates the modeled max of 10 for
    # NotificationsWithSubscribers before the request is ever sent, so the
    # 11th-entry-in-one-call case can't reach the server via boto3 — it is
    # exercised through repeated CreateNotification calls instead (below).
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name), NotificationsWithSubscribers=entries)
    resp = budgets.describe_notifications_for_budget(AccountId=ACCOUNT_ID, BudgetName=name, MaxResults=100)
    assert len(resp["Notifications"]) == 10

    with pytest.raises(ClientError) as exc_info:
        budgets.create_notification(
            AccountId=ACCOUNT_ID,
            BudgetName=name,
            Notification=_notification(Threshold=999.0),
            Subscribers=[{"SubscriptionType": "EMAIL", "Address": "b@example.com"}],
        )
    assert exc_info.value.response["Error"]["Code"] == "CreationLimitExceededException"


# -- CreateNotification / UpdateNotification / DeleteNotification -----------


def test_budgets_create_notification_and_delete(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))

    budgets.create_notification(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(),
        Subscribers=[{"SubscriptionType": "EMAIL", "Address": "a@example.com"}],
    )

    resp = budgets.describe_notifications_for_budget(AccountId=ACCOUNT_ID, BudgetName=name)
    assert len(resp["Notifications"]) == 1

    budgets.delete_notification(AccountId=ACCOUNT_ID, BudgetName=name, Notification=_notification())

    resp = budgets.describe_notifications_for_budget(AccountId=ACCOUNT_ID, BudgetName=name)
    assert resp["Notifications"] == []


def test_budgets_notification_without_threshold_type_is_stored_as_percentage(budgets):
    """AWS Budgets API Reference, Notification.ThresholdType: an omitted
    ThresholdType defaults to PERCENTAGE, including for matching an existing
    notification on Delete/Update/DescribeSubscribers."""
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))
    notification = {
        "NotificationType": "ACTUAL",
        "ComparisonOperator": "GREATER_THAN",
        "Threshold": 90.0,
    }
    budgets.create_notification(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=notification,
        Subscribers=[{"SubscriptionType": "EMAIL", "Address": "a@example.com"}],
    )

    stored = budgets.describe_notifications_for_budget(AccountId=ACCOUNT_ID, BudgetName=name)["Notifications"]
    assert stored[0]["ThresholdType"] == "PERCENTAGE"

    budgets.delete_notification(AccountId=ACCOUNT_ID, BudgetName=name, Notification=notification)

    resp = budgets.describe_notifications_for_budget(AccountId=ACCOUNT_ID, BudgetName=name)
    assert resp["Notifications"] == []


def test_budgets_create_notification_duplicate_rejected(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))
    budgets.create_notification(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(),
        Subscribers=[{"SubscriptionType": "EMAIL", "Address": "a@example.com"}],
    )

    with pytest.raises(ClientError) as exc_info:
        budgets.create_notification(
            AccountId=ACCOUNT_ID,
            BudgetName=name,
            Notification=_notification(),
            Subscribers=[{"SubscriptionType": "EMAIL", "Address": "b@example.com"}],
        )

    assert exc_info.value.response["Error"]["Code"] == "DuplicateRecordException"


def test_budgets_create_notification_budget_not_found(budgets):
    with pytest.raises(ClientError) as exc_info:
        budgets.create_notification(
            AccountId=ACCOUNT_ID,
            BudgetName=f"missing-{_uid()}",
            Notification=_notification(),
            Subscribers=[{"SubscriptionType": "EMAIL", "Address": "a@example.com"}],
        )

    assert exc_info.value.response["Error"]["Code"] == "NotFoundException"


def test_budgets_delete_notification_not_found(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))

    with pytest.raises(ClientError) as exc_info:
        budgets.delete_notification(AccountId=ACCOUNT_ID, BudgetName=name, Notification=_notification())

    assert exc_info.value.response["Error"]["Code"] == "NotFoundException"


def test_budgets_update_notification(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))
    budgets.create_notification(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(Threshold=90.0),
        Subscribers=[{"SubscriptionType": "EMAIL", "Address": "a@example.com"}],
    )

    budgets.update_notification(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        OldNotification=_notification(Threshold=90.0),
        NewNotification=_notification(Threshold=95.0),
    )

    resp = budgets.describe_notifications_for_budget(AccountId=ACCOUNT_ID, BudgetName=name)
    assert resp["Notifications"][0]["Threshold"] == 95.0

    # Subscribers stay attached across a notification update.
    subs = budgets.describe_subscribers_for_notification(
        AccountId=ACCOUNT_ID, BudgetName=name, Notification=_notification(Threshold=95.0)
    )
    assert subs["Subscribers"] == [{"SubscriptionType": "EMAIL", "Address": "a@example.com"}]


def test_budgets_update_notification_not_found(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))

    with pytest.raises(ClientError) as exc_info:
        budgets.update_notification(
            AccountId=ACCOUNT_ID,
            BudgetName=name,
            OldNotification=_notification(),
            NewNotification=_notification(Threshold=50.0),
        )

    assert exc_info.value.response["Error"]["Code"] == "NotFoundException"


# -- CreateSubscriber / UpdateSubscriber / DeleteSubscriber ------------------


def test_budgets_create_subscriber_and_describe(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))
    budgets.create_notification(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(),
        Subscribers=[{"SubscriptionType": "EMAIL", "Address": "a@example.com"}],
    )

    budgets.create_subscriber(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(),
        Subscriber={"SubscriptionType": "EMAIL", "Address": "b@example.com"},
    )

    resp = budgets.describe_subscribers_for_notification(
        AccountId=ACCOUNT_ID, BudgetName=name, Notification=_notification()
    )
    addresses = {s["Address"] for s in resp["Subscribers"]}
    assert addresses == {"a@example.com", "b@example.com"}


def test_budgets_create_subscriber_duplicate_rejected(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))
    budgets.create_notification(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(),
        Subscribers=[{"SubscriptionType": "EMAIL", "Address": "a@example.com"}],
    )

    with pytest.raises(ClientError) as exc_info:
        budgets.create_subscriber(
            AccountId=ACCOUNT_ID,
            BudgetName=name,
            Notification=_notification(),
            Subscriber={"SubscriptionType": "EMAIL", "Address": "a@example.com"},
        )

    assert exc_info.value.response["Error"]["Code"] == "DuplicateRecordException"


def test_budgets_create_subscriber_limit_exceeded(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))
    budgets.create_notification(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(),
        Subscribers=[{"SubscriptionType": "EMAIL", "Address": f"s{i}@example.com"} for i in range(11)],
    )

    with pytest.raises(ClientError) as exc_info:
        budgets.create_subscriber(
            AccountId=ACCOUNT_ID,
            BudgetName=name,
            Notification=_notification(),
            Subscriber={"SubscriptionType": "EMAIL", "Address": "overflow@example.com"},
        )

    assert exc_info.value.response["Error"]["Code"] == "CreationLimitExceededException"


def test_budgets_create_subscriber_notification_not_found(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))

    with pytest.raises(ClientError) as exc_info:
        budgets.create_subscriber(
            AccountId=ACCOUNT_ID,
            BudgetName=name,
            Notification=_notification(),
            Subscriber={"SubscriptionType": "EMAIL", "Address": "a@example.com"},
        )

    assert exc_info.value.response["Error"]["Code"] == "NotFoundException"


def test_budgets_delete_last_subscriber_deletes_notification(budgets):
    """AWS API Reference, DeleteSubscriber: deleting the last subscriber to a
    notification also deletes the notification."""
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))
    budgets.create_notification(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(),
        Subscribers=[{"SubscriptionType": "EMAIL", "Address": "a@example.com"}],
    )

    budgets.delete_subscriber(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(),
        Subscriber={"SubscriptionType": "EMAIL", "Address": "a@example.com"},
    )

    resp = budgets.describe_notifications_for_budget(AccountId=ACCOUNT_ID, BudgetName=name)
    assert resp["Notifications"] == []


def test_budgets_delete_one_of_two_subscribers_keeps_notification(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))
    budgets.create_notification(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(),
        Subscribers=[{"SubscriptionType": "EMAIL", "Address": "a@example.com"}],
    )
    budgets.create_subscriber(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(),
        Subscriber={"SubscriptionType": "EMAIL", "Address": "b@example.com"},
    )

    budgets.delete_subscriber(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(),
        Subscriber={"SubscriptionType": "EMAIL", "Address": "a@example.com"},
    )

    resp = budgets.describe_notifications_for_budget(AccountId=ACCOUNT_ID, BudgetName=name)
    assert len(resp["Notifications"]) == 1
    subs = budgets.describe_subscribers_for_notification(
        AccountId=ACCOUNT_ID, BudgetName=name, Notification=_notification()
    )
    assert [s["Address"] for s in subs["Subscribers"]] == ["b@example.com"]


def test_budgets_delete_subscriber_not_found(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))
    budgets.create_notification(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(),
        Subscribers=[{"SubscriptionType": "EMAIL", "Address": "a@example.com"}],
    )

    with pytest.raises(ClientError) as exc_info:
        budgets.delete_subscriber(
            AccountId=ACCOUNT_ID,
            BudgetName=name,
            Notification=_notification(),
            Subscriber={"SubscriptionType": "EMAIL", "Address": "missing@example.com"},
        )

    assert exc_info.value.response["Error"]["Code"] == "NotFoundException"


def test_budgets_update_subscriber(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))
    budgets.create_notification(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(),
        Subscribers=[{"SubscriptionType": "EMAIL", "Address": "a@example.com"}],
    )

    budgets.update_subscriber(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(),
        OldSubscriber={"SubscriptionType": "EMAIL", "Address": "a@example.com"},
        NewSubscriber={"SubscriptionType": "EMAIL", "Address": "changed@example.com"},
    )

    resp = budgets.describe_subscribers_for_notification(
        AccountId=ACCOUNT_ID, BudgetName=name, Notification=_notification()
    )
    assert [s["Address"] for s in resp["Subscribers"]] == ["changed@example.com"]


def test_budgets_update_subscriber_not_found(budgets):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))
    budgets.create_notification(
        AccountId=ACCOUNT_ID,
        BudgetName=name,
        Notification=_notification(),
        Subscribers=[{"SubscriptionType": "EMAIL", "Address": "a@example.com"}],
    )

    with pytest.raises(ClientError) as exc_info:
        budgets.update_subscriber(
            AccountId=ACCOUNT_ID,
            BudgetName=name,
            Notification=_notification(),
            OldSubscriber={"SubscriptionType": "EMAIL", "Address": "missing@example.com"},
            NewSubscriber={"SubscriptionType": "EMAIL", "Address": "changed@example.com"},
        )

    assert exc_info.value.response["Error"]["Code"] == "NotFoundException"


# -- Tagging ------------------------------------------------------------------


def test_budgets_tag_resource_and_list_tags(budgets, real_account_id):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))
    arn = f"arn:aws:budgets::{real_account_id}:budget/{name}"

    budgets.tag_resource(ResourceARN=arn, ResourceTags=[{"Key": "env", "Value": "dev"}])

    resp = budgets.list_tags_for_resource(ResourceARN=arn)
    assert resp["ResourceTags"] == [{"Key": "env", "Value": "dev"}]


def test_budgets_untag_resource(budgets, real_account_id):
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))
    arn = f"arn:aws:budgets::{real_account_id}:budget/{name}"
    budgets.tag_resource(ResourceARN=arn, ResourceTags=[{"Key": "env", "Value": "dev"}])

    budgets.untag_resource(ResourceARN=arn, ResourceTagKeys=["env"])

    resp = budgets.list_tags_for_resource(ResourceARN=arn)
    assert resp["ResourceTags"] == []


def test_budgets_create_budget_with_resource_tags(budgets, real_account_id):
    name = f"budget-{_uid()}"
    arn = f"arn:aws:budgets::{real_account_id}:budget/{name}"
    budgets.create_budget(
        AccountId=ACCOUNT_ID,
        Budget=_budget(name),
        ResourceTags=[{"Key": "team", "Value": "platform"}],
    )

    resp = budgets.list_tags_for_resource(ResourceARN=arn)
    assert resp["ResourceTags"] == [{"Key": "team", "Value": "platform"}]


def test_budgets_tag_resource_not_found(budgets, real_account_id):
    arn = f"arn:aws:budgets::{real_account_id}:budget/missing-{_uid()}"

    with pytest.raises(ClientError) as exc_info:
        budgets.tag_resource(ResourceARN=arn, ResourceTags=[{"Key": "env", "Value": "dev"}])

    assert exc_info.value.response["Error"]["Code"] == "NotFoundException"


def test_budgets_tag_resource_wrong_account_arn_not_found(budgets, real_account_id):
    """A resource ARN naming a real budget under a different account segment
    must not resolve, even though the budget name exists in the caller's own
    account."""
    name = f"budget-{_uid()}"
    budgets.create_budget(AccountId=ACCOUNT_ID, Budget=_budget(name))
    other_account = "".join("9" if c != "9" else "8" for c in real_account_id)
    arn = f"arn:aws:budgets::{other_account}:budget/{name}"

    with pytest.raises(ClientError) as exc_info:
        budgets.tag_resource(ResourceARN=arn, ResourceTags=[{"Key": "env", "Value": "dev"}])

    assert exc_info.value.response["Error"]["Code"] == "NotFoundException"
