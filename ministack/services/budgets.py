# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
AWS Budgets service emulator.

JSON 1.1 protocol with X-Amz-Target prefix ``AWSBudgetServiceGateway``.
Global service (no region in the endpoint); state is account-scoped only.
Evidence: the botocore ``budgets`` service model (2016-10-20) and the AWS
Budgets API Reference (docs.aws.amazon.com/aws-cost-management/latest/APIReference).

Implemented:
  CreateBudget (incl. inline NotificationsWithSubscribers and ResourceTags),
  DescribeBudget, DescribeBudgets, UpdateBudget, DeleteBudget,
  CreateNotification, UpdateNotification, DeleteNotification,
  DescribeNotificationsForBudget, CreateSubscriber, UpdateSubscriber,
  DeleteSubscriber, DescribeSubscribersForNotification, TagResource,
  UntagResource, ListTagsForResource.

Deferred (no real use case driving them yet; see CONTRIBUTING.md):
  Budget Actions (CreateBudgetAction/DescribeBudgetAction/... — RI/SP
  auto-remediation, unrelated to cost-alert budgets), DescribeBudgetPerformanceHistory,
  DescribeBudgetNotificationsForAccount, and enforcing the single-SNS-subscriber
  sub-rule within the 11-subscriber cap (only the total count is enforced).
"""

import copy
import datetime
import json
import logging
import time

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
)

logger = logging.getLogger("budgets")

MAX_NOTIFICATIONS_PER_BUDGET = 10  # AWS Budgets API Reference: Notification
MAX_SUBSCRIBERS_PER_NOTIFICATION = 11  # 1 SNS + up to 10 EMAIL, per the same page

_TIME_UNITS = {"DAILY", "MONTHLY", "QUARTERLY", "ANNUALLY", "CUSTOM"}
_BUDGET_TYPES = {
    "USAGE",
    "COST",
    "RI_UTILIZATION",
    "RI_COVERAGE",
    "SAVINGS_PLANS_UTILIZATION",
    "SAVINGS_PLANS_COVERAGE",
}
_NOTIFICATION_TYPES = {"ACTUAL", "FORECASTED"}
_COMPARISON_OPERATORS = {"GREATER_THAN", "LESS_THAN", "EQUAL_TO"}
_THRESHOLD_TYPES = {"PERCENTAGE", "ABSOLUTE_VALUE"}
_NOTIFICATION_STATES = {"OK", "ALARM"}
_SUBSCRIPTION_TYPES = {"SNS", "EMAIL"}

# budget_name -> Budget dict (includes the server-computed CalculatedSpend
# and LastUpdatedTime; FilterExpression/Metrics are stored but only echoed
# back when ShowFilterExpression is requested, matching AWS's default).
_budgets = AccountScopedDict()
# budget_name -> [{"notification": {...}, "subscribers": [{...}, ...]}, ...]
# A list rather than a dict keyed by notification identity, because the
# identity tuple (NotificationType, ComparisonOperator, Threshold,
# ThresholdType) isn't JSON-serializable as a dict key for state persistence.
_notification_store = AccountScopedDict()
# resource ARN -> {tag_key: tag_value}
_tags = AccountScopedDict()


def reset():
    _budgets.clear()
    _notification_store.clear()
    _tags.clear()


def get_state():
    return {
        "budgets": copy.deepcopy(_budgets),
        "notification_store": copy.deepcopy(_notification_store),
        "tags": copy.deepcopy(_tags),
    }


def load_persisted_state(data):
    if not data:
        return
    # No `or {}`: a scoped dict holding only other accounts' entries is falsy
    # here, and the loader runs at boot with no request scope (see cur.py).
    _budgets.clear()
    _budgets.update(data.get("budgets", {}))
    _notification_store.clear()
    _notification_store.update(data.get("notification_store", {}))
    _tags.clear()
    _tags.update(data.get("tags", {}))


def _json(status: int, body: dict):
    return status, {"Content-Type": "application/x-amz-json-1.1"}, json.dumps(body).encode()


def _err_invalid(message):
    return error_response_json("InvalidParameterException", message, 400)


def _err_notfound(message):
    return error_response_json("NotFoundException", message, 400)


def _err_dup(message):
    return error_response_json("DuplicateRecordException", message, 400)


def _err_limit(message):
    return error_response_json("CreationLimitExceededException", message, 400)


def _budget_arn(budget_name: str) -> str:
    # AWS Budgets API Reference resource-type table: arn:${Partition}:budgets::${AccountId}:budget/${BudgetName}
    return f"arn:aws:budgets::{get_account_id()}:budget/{budget_name}"


def _parse_budget_arn(arn: str):
    """Parse ``arn:aws:budgets::<account>:budget/<name>``, or return None.

    Requires an exact account-id match against the caller's own scope: a
    resource ARN naming another account's budget must not resolve to a
    same-named budget that happens to exist in the caller's own account.
    """
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None
    if spec.service != "budgets" or spec.account_id != get_account_id():
        return None
    if not spec.resource.startswith("budget/"):
        return None
    return spec.resource[len("budget/"):] or None


def _valid_budget_name(name) -> bool:
    if not isinstance(name, str) or not (1 <= len(name) <= 100):
        return False
    if ":" in name or "\\" in name or "/action/" in name:
        return False
    lowered = name.lower()
    return not ("<script>" in lowered and "</script>" in lowered)


def _validate_budget_shape(budget: dict):
    if not isinstance(budget, dict):
        return _err_invalid("Budget is required")
    name = budget.get("BudgetName")
    if not _valid_budget_name(name):
        return _err_invalid("Budget.BudgetName is required and must be a valid budget name")
    if budget.get("TimeUnit") not in _TIME_UNITS:
        return _err_invalid(f"Budget.TimeUnit must be one of {sorted(_TIME_UNITS)}")
    if budget.get("BudgetType") not in _BUDGET_TYPES:
        return _err_invalid(f"Budget.BudgetType must be one of {sorted(_BUDGET_TYPES)}")
    return None


def _validate_notification(notification: dict):
    if not isinstance(notification, dict):
        return _err_invalid("Notification is required")
    if notification.get("NotificationType") not in _NOTIFICATION_TYPES:
        return _err_invalid(f"Notification.NotificationType must be one of {sorted(_NOTIFICATION_TYPES)}")
    if notification.get("ComparisonOperator") not in _COMPARISON_OPERATORS:
        return _err_invalid(f"Notification.ComparisonOperator must be one of {sorted(_COMPARISON_OPERATORS)}")
    threshold = notification.get("Threshold")
    if not isinstance(threshold, (int, float)) or isinstance(threshold, bool) or not (0 <= threshold <= 15000000000000):
        return _err_invalid("Notification.Threshold must be a number between 0 and 15000000000000")
    threshold_type = notification.get("ThresholdType")
    if threshold_type is not None and threshold_type not in _THRESHOLD_TYPES:
        return _err_invalid(f"Notification.ThresholdType must be one of {sorted(_THRESHOLD_TYPES)}")
    state = notification.get("NotificationState")
    if state is not None and state not in _NOTIFICATION_STATES:
        return _err_invalid(f"Notification.NotificationState must be one of {sorted(_NOTIFICATION_STATES)}")
    return None


def _validate_subscribers(subscribers, *, max_count=MAX_SUBSCRIBERS_PER_NOTIFICATION):
    if not isinstance(subscribers, list) or not subscribers:
        return _err_invalid("Subscribers must be a non-empty list")
    if len(subscribers) > max_count:
        return _err_limit(f"A notification can have at most {max_count} subscribers")
    for sub in subscribers:
        if not isinstance(sub, dict):
            return _err_invalid("Each subscriber must be an object")
        if sub.get("SubscriptionType") not in _SUBSCRIPTION_TYPES:
            return _err_invalid(f"Subscriber.SubscriptionType must be one of {sorted(_SUBSCRIPTION_TYPES)}")
        address = sub.get("Address")
        if not isinstance(address, str) or not address:
            return _err_invalid("Subscriber.Address is required")
    return None


def _notification_key(notification: dict):
    # AWS Budgets API Reference, Notification.ThresholdType: an omitted
    # ThresholdType defaults to PERCENTAGE, so matching treats it the same.
    return (
        notification.get("NotificationType"),
        notification.get("ComparisonOperator"),
        notification.get("Threshold"),
        notification.get("ThresholdType") or "PERCENTAGE",
    )


def _subscriber_key(subscriber: dict):
    return (subscriber.get("SubscriptionType"), subscriber.get("Address"))


def _find_notification_entry(budget_name: str, notification: dict):
    key = _notification_key(notification)
    for entry in _notification_store.get(budget_name, []):
        if _notification_key(entry["notification"]) == key:
            return entry
    return None


def _calculated_spend(budget: dict) -> dict:
    # No billing engine: always AWS's documented "no spend recorded yet" shape.
    unit = None
    limit = budget.get("BudgetLimit")
    if isinstance(limit, dict):
        unit = limit.get("Unit")
    if not unit:
        planned = budget.get("PlannedBudgetLimits")
        if isinstance(planned, dict) and planned:
            first = next(iter(planned.values()))
            if isinstance(first, dict):
                unit = first.get("Unit")
    return {"ActualSpend": {"Amount": "0", "Unit": unit or "USD"}}


# AWS Budgets API Reference, Budget.TimePeriod: an omitted End always defaults
# to 06/15/87 00:00 UTC.
_DEFAULT_TIME_PERIOD_END = datetime.datetime(2087, 6, 15, tzinfo=datetime.timezone.utc).timestamp()


def _period_start_for_time_unit(time_unit):
    # AWS Budgets API Reference, Budget.TimePeriod: an omitted Start defaults
    # to the beginning of the budget's own TimeUnit period, in UTC.
    now = datetime.datetime.now(datetime.timezone.utc)
    if time_unit == "DAILY":
        start = now
    elif time_unit == "MONTHLY":
        start = now.replace(day=1)
    elif time_unit == "QUARTERLY":
        start = now.replace(month=(now.month - 1) // 3 * 3 + 1, day=1)
    elif time_unit == "ANNUALLY":
        start = now.replace(month=1, day=1)
    else:
        return None
    return start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()


def _apply_time_period_defaults(budget: dict):
    period = budget.get("TimePeriod")
    if not isinstance(period, dict):
        period = {}
        budget["TimePeriod"] = period
    if period.get("Start") is None:
        start = _period_start_for_time_unit(budget.get("TimeUnit"))
        if start is not None:
            period["Start"] = start
    if period.get("End") is None:
        period["End"] = _DEFAULT_TIME_PERIOD_END


def _paginate(items, max_results, next_token, *, default=100, hard_cap=100):
    if not max_results or max_results <= 0:
        max_results = default
    max_results = min(max_results, hard_cap)
    start = 0
    if next_token:
        try:
            start = int(next_token)
        except (ValueError, TypeError):
            start = 0
    page = items[start : start + max_results]
    new_token = str(start + max_results) if start + max_results < len(items) else None
    return page, new_token


def _apply_show_filter_expression(budget: dict, show: bool) -> dict:
    result = copy.deepcopy(budget)
    if not show:
        result.pop("FilterExpression", None)
        result.pop("Metrics", None)
    return result


# -- Budgets ----------------------------------------------------------------


def _create_budget(data):
    budget = data.get("Budget")
    err = _validate_budget_shape(budget)
    if err:
        return err
    budget_name = budget["BudgetName"]
    if budget_name in _budgets:
        return _err_dup(f"Error creating budget: {budget_name} - the budget already exists.")

    entries_in = data.get("NotificationsWithSubscribers") or []
    if len(entries_in) > MAX_NOTIFICATIONS_PER_BUDGET:
        return _err_limit(f"A budget can have at most {MAX_NOTIFICATIONS_PER_BUDGET} notifications")

    parsed_entries = []
    seen_keys = set()
    for entry in entries_in:
        if not isinstance(entry, dict):
            return _err_invalid("Each entry in NotificationsWithSubscribers must be an object")
        notification = entry.get("Notification")
        subscribers = entry.get("Subscribers")
        err = _validate_notification(notification)
        if err:
            return err
        err = _validate_subscribers(subscribers)
        if err:
            return err
        key = _notification_key(notification)
        if key in seen_keys:
            return _err_dup("Duplicate notification in NotificationsWithSubscribers")
        seen_keys.add(key)
        stored_notification = copy.deepcopy(notification)
        stored_notification.setdefault("NotificationState", "OK")
        stored_notification.setdefault("ThresholdType", "PERCENTAGE")
        parsed_entries.append({"notification": stored_notification, "subscribers": copy.deepcopy(subscribers)})

    resource_tags = data.get("ResourceTags") or []
    for tag in resource_tags:
        if not isinstance(tag, dict) or "Key" not in tag or "Value" not in tag:
            return _err_invalid("Each ResourceTag must have Key and Value")

    stored_budget = copy.deepcopy(budget)
    _apply_time_period_defaults(stored_budget)
    stored_budget["CalculatedSpend"] = _calculated_spend(stored_budget)
    stored_budget["LastUpdatedTime"] = time.time()

    _budgets[budget_name] = stored_budget
    _notification_store[budget_name] = parsed_entries
    if resource_tags:
        _tags[_budget_arn(budget_name)] = {tag["Key"]: tag["Value"] for tag in resource_tags}

    return _json(200, {})


def _describe_budget(data):
    budget_name = data.get("BudgetName")
    if not isinstance(budget_name, str) or not budget_name:
        return _err_invalid("BudgetName is required")
    budget = _budgets.get(budget_name)
    if budget is None:
        return _err_notfound(f"Unable to get budget: {budget_name} - the budget doesn't exist.")
    show_filter = bool(data.get("ShowFilterExpression"))
    return _json(200, {"Budget": _apply_show_filter_expression(budget, show_filter)})


def _describe_budgets(data):
    max_results = data.get("MaxResults")
    next_token = data.get("NextToken")
    show_filter = bool(data.get("ShowFilterExpression"))
    budgets = _budgets.values()
    page, new_token = _paginate(budgets, max_results, next_token, default=100, hard_cap=1000)
    result = {"Budgets": [_apply_show_filter_expression(b, show_filter) for b in page]}
    if new_token:
        result["NextToken"] = new_token
    return _json(200, result)


def _update_budget(data):
    new_budget = data.get("NewBudget")
    err = _validate_budget_shape(new_budget)
    if err:
        return err
    budget_name = new_budget["BudgetName"]
    if budget_name not in _budgets:
        return _err_notfound(f"Unable to update budget: {budget_name} - the budget doesn't exist.")

    stored_budget = copy.deepcopy(new_budget)
    stored_budget.setdefault("TimePeriod", copy.deepcopy(_budgets[budget_name].get("TimePeriod")))
    _apply_time_period_defaults(stored_budget)
    stored_budget["CalculatedSpend"] = _calculated_spend(stored_budget)
    stored_budget["LastUpdatedTime"] = time.time()
    _budgets[budget_name] = stored_budget
    return _json(200, {})


def _delete_budget(data):
    budget_name = data.get("BudgetName")
    if not isinstance(budget_name, str) or not budget_name:
        return _err_invalid("BudgetName is required")
    if budget_name not in _budgets:
        return _err_notfound(f"Unable to delete budget: {budget_name} - the budget doesn't exist.")
    del _budgets[budget_name]
    _notification_store.pop(budget_name, None)
    _tags.pop(_budget_arn(budget_name), None)
    return _json(200, {})


# -- Notifications ------------------------------------------------------------


def _create_notification(data):
    budget_name = data.get("BudgetName")
    if budget_name not in _budgets:
        return _err_notfound(f"Unable to create notification - budget: {budget_name} doesn't exist")
    notification = data.get("Notification")
    err = _validate_notification(notification)
    if err:
        return err
    subscribers = data.get("Subscribers")
    err = _validate_subscribers(subscribers)
    if err:
        return err

    entries = _notification_store.setdefault(budget_name, [])
    if len(entries) >= MAX_NOTIFICATIONS_PER_BUDGET:
        return _err_limit(f"A budget can have at most {MAX_NOTIFICATIONS_PER_BUDGET} notifications")
    if _find_notification_entry(budget_name, notification) is not None:
        return _err_dup("A notification with this Type/ComparisonOperator/Threshold/ThresholdType already exists")

    stored_notification = copy.deepcopy(notification)
    stored_notification.setdefault("NotificationState", "OK")
    stored_notification.setdefault("ThresholdType", "PERCENTAGE")
    entries.append({"notification": stored_notification, "subscribers": copy.deepcopy(subscribers)})
    return _json(200, {})


def _delete_notification(data):
    budget_name = data.get("BudgetName")
    notification = data.get("Notification")
    if budget_name not in _budgets:
        return _err_notfound(f"Unable to delete notification - budget: {budget_name} doesn't exist")
    entries = _notification_store.get(budget_name, [])
    entry = _find_notification_entry(budget_name, notification or {})
    if entry is None:
        return _err_notfound("The specified notification doesn't exist")
    entries.remove(entry)
    return _json(200, {})


def _update_notification(data):
    budget_name = data.get("BudgetName")
    old_notification = data.get("OldNotification")
    new_notification = data.get("NewNotification")
    if budget_name not in _budgets:
        return _err_notfound(f"Unable to update notification - budget: {budget_name} doesn't exist")
    err = _validate_notification(new_notification)
    if err:
        return err
    entry = _find_notification_entry(budget_name, old_notification or {})
    if entry is None:
        return _err_notfound("The specified notification doesn't exist")

    new_key = _notification_key(new_notification)
    if new_key != _notification_key(old_notification or {}):
        for other in _notification_store.get(budget_name, []):
            if other is not entry and _notification_key(other["notification"]) == new_key:
                return _err_dup("A notification with this Type/ComparisonOperator/Threshold/ThresholdType already exists")

    stored_notification = copy.deepcopy(new_notification)
    stored_notification.setdefault("NotificationState", "OK")
    stored_notification.setdefault("ThresholdType", "PERCENTAGE")
    entry["notification"] = stored_notification
    return _json(200, {})


def _describe_notifications_for_budget(data):
    budget_name = data.get("BudgetName")
    if budget_name not in _budgets:
        return _err_notfound(f"Unable to get notifications for budget: {budget_name} - the budget doesn't exist.")
    max_results = data.get("MaxResults")
    next_token = data.get("NextToken")
    notifications = [e["notification"] for e in _notification_store.get(budget_name, [])]
    page, new_token = _paginate(notifications, max_results, next_token)
    result = {"Notifications": page}
    if new_token:
        result["NextToken"] = new_token
    return _json(200, result)


# -- Subscribers --------------------------------------------------------------


def _create_subscriber(data):
    budget_name = data.get("BudgetName")
    notification = data.get("Notification")
    subscriber = data.get("Subscriber")
    if budget_name not in _budgets:
        return _err_notfound(f"Unable to create subscriber - budget: {budget_name} doesn't exist")
    if not isinstance(subscriber, dict) or subscriber.get("SubscriptionType") not in _SUBSCRIPTION_TYPES or not subscriber.get("Address"):
        return _err_invalid("Subscriber is required and must have SubscriptionType and Address")
    entry = _find_notification_entry(budget_name, notification or {})
    if entry is None:
        return _err_notfound("The specified notification doesn't exist")
    if len(entry["subscribers"]) >= MAX_SUBSCRIBERS_PER_NOTIFICATION:
        return _err_limit(f"A notification can have at most {MAX_SUBSCRIBERS_PER_NOTIFICATION} subscribers")
    if any(_subscriber_key(s) == _subscriber_key(subscriber) for s in entry["subscribers"]):
        return _err_dup("This subscriber already exists on this notification")
    entry["subscribers"].append(copy.deepcopy(subscriber))
    return _json(200, {})


def _delete_subscriber(data):
    budget_name = data.get("BudgetName")
    notification = data.get("Notification")
    subscriber = data.get("Subscriber")
    if budget_name not in _budgets:
        return _err_notfound(f"Unable to delete subscriber - budget: {budget_name} doesn't exist")
    entries = _notification_store.get(budget_name, [])
    entry = _find_notification_entry(budget_name, notification or {})
    if entry is None:
        return _err_notfound("The specified notification doesn't exist")
    target_key = _subscriber_key(subscriber or {})
    remaining = [s for s in entry["subscribers"] if _subscriber_key(s) != target_key]
    if len(remaining) == len(entry["subscribers"]):
        return _err_notfound("The specified subscriber doesn't exist")
    entry["subscribers"] = remaining
    if not remaining:
        # AWS Budgets API Reference, DeleteSubscriber: "Deleting the last
        # subscriber to a notification also deletes the notification."
        entries.remove(entry)
    return _json(200, {})


def _update_subscriber(data):
    budget_name = data.get("BudgetName")
    notification = data.get("Notification")
    old_subscriber = data.get("OldSubscriber")
    new_subscriber = data.get("NewSubscriber")
    if budget_name not in _budgets:
        return _err_notfound(f"Unable to update subscriber - budget: {budget_name} doesn't exist")
    if not isinstance(new_subscriber, dict) or new_subscriber.get("SubscriptionType") not in _SUBSCRIPTION_TYPES or not new_subscriber.get("Address"):
        return _err_invalid("NewSubscriber is required and must have SubscriptionType and Address")
    entry = _find_notification_entry(budget_name, notification or {})
    if entry is None:
        return _err_notfound("The specified notification doesn't exist")
    old_key = _subscriber_key(old_subscriber or {})
    for i, sub in enumerate(entry["subscribers"]):
        if _subscriber_key(sub) == old_key:
            new_key = _subscriber_key(new_subscriber)
            if new_key != old_key and any(_subscriber_key(s) == new_key for s in entry["subscribers"]):
                return _err_dup("This subscriber already exists on this notification")
            entry["subscribers"][i] = copy.deepcopy(new_subscriber)
            return _json(200, {})
    return _err_notfound("The specified subscriber doesn't exist")


def _describe_subscribers_for_notification(data):
    budget_name = data.get("BudgetName")
    notification = data.get("Notification")
    if budget_name not in _budgets:
        return _err_notfound(f"Unable to get subscribers - budget: {budget_name} doesn't exist")
    entry = _find_notification_entry(budget_name, notification or {})
    if entry is None:
        return _err_notfound("The specified notification doesn't exist")
    max_results = data.get("MaxResults")
    next_token = data.get("NextToken")
    page, new_token = _paginate(entry["subscribers"], max_results, next_token)
    result = {"Subscribers": page}
    if new_token:
        result["NextToken"] = new_token
    return _json(200, result)


# -- Tags -----------------------------------------------------------------


def _resolve_tagged_budget(resource_arn):
    """Return the canonical budget ARN for an existing budget, or None.

    Re-derives the ARN from the resolved budget name via ``_budget_arn``
    rather than trusting the caller's ``ResourceARN`` string verbatim.
    """
    budget_name = _parse_budget_arn(resource_arn)
    if budget_name is None or budget_name not in _budgets:
        return None
    return _budget_arn(budget_name)


def _tag_resource(data):
    resource_arn = data.get("ResourceARN")
    canonical_arn = _resolve_tagged_budget(resource_arn)
    if canonical_arn is None:
        return _err_notfound(f"Unable to tag resource - {resource_arn} doesn't exist")
    tags = data.get("ResourceTags")
    if not isinstance(tags, list) or not tags:
        return _err_invalid("ResourceTags is required")
    for tag in tags:
        if not isinstance(tag, dict) or "Key" not in tag or "Value" not in tag:
            return _err_invalid("Each ResourceTag must have Key and Value")
    tag_dict = _tags.setdefault(canonical_arn, {})
    for tag in tags:
        tag_dict[tag["Key"]] = tag["Value"]
    return _json(200, {})


def _untag_resource(data):
    resource_arn = data.get("ResourceARN")
    canonical_arn = _resolve_tagged_budget(resource_arn)
    if canonical_arn is None:
        return _err_notfound(f"Unable to untag resource - {resource_arn} doesn't exist")
    tag_keys = data.get("ResourceTagKeys")
    if not isinstance(tag_keys, list):
        return _err_invalid("ResourceTagKeys is required")
    tag_dict = _tags.get(canonical_arn, {})
    for key in tag_keys:
        tag_dict.pop(key, None)
    return _json(200, {})


def _list_tags_for_resource(data):
    resource_arn = data.get("ResourceARN")
    canonical_arn = _resolve_tagged_budget(resource_arn)
    if canonical_arn is None:
        return _err_notfound(f"Unable to list tags - {resource_arn} doesn't exist")
    tag_dict = _tags.get(canonical_arn, {})
    return _json(200, {"ResourceTags": [{"Key": k, "Value": v} for k, v in tag_dict.items()]})


_DISPATCH = {
    "CreateBudget": _create_budget,
    "DescribeBudget": _describe_budget,
    "DescribeBudgets": _describe_budgets,
    "UpdateBudget": _update_budget,
    "DeleteBudget": _delete_budget,
    "CreateNotification": _create_notification,
    "DeleteNotification": _delete_notification,
    "UpdateNotification": _update_notification,
    "DescribeNotificationsForBudget": _describe_notifications_for_budget,
    "CreateSubscriber": _create_subscriber,
    "DeleteSubscriber": _delete_subscriber,
    "UpdateSubscriber": _update_subscriber,
    "DescribeSubscribersForNotification": _describe_subscribers_for_notification,
    "TagResource": _tag_resource,
    "UntagResource": _untag_resource,
    "ListTagsForResource": _list_tags_for_resource,
}


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target") or headers.get("X-Amz-Target") or ""
    action = target.split(".", 1)[1] if "." in target else target
    if not action:
        return error_response_json("InvalidAction", "missing X-Amz-Target", 400)

    body_text = body.decode("utf-8") if isinstance(body, bytes) else (body or "")
    try:
        payload = json.loads(body_text) if body_text else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "invalid JSON body", 400)

    fn = _DISPATCH.get(action)
    if fn is None:
        return error_response_json(
            "InvalidAction",
            f"Operation '{action}' not implemented",
            400,
        )
    return fn(payload)
