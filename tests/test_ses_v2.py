import asyncio
import copy
import json
import re
import uuid

import pytest
from botocore.exceptions import ClientError

from ministack.core.responses import set_request_account_id, set_request_region

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"


@pytest.fixture()
def ses_v2():
    from ministack.services import ses_v2 as service

    set_request_account_id(ACCOUNT_ID)
    set_request_region(REGION)
    _reset(service)
    yield service
    _reset(service)


def _reset(service):
    service.reset()
    service._templates.clear()
    service._sent_emails_list().clear()


def _verify(service, identity="example.com"):
    """Senders must be verified identities, as on AWS."""
    status, _ = _call(service, "POST", "/v2/email/identities", body={"EmailIdentity": identity})
    assert status == 200


def _arn(kind, name, *, partition="aws", service="ses", region=REGION, account=ACCOUNT_ID):
    return f"arn:{partition}:{service}:{region}:{account}:{kind}/{name}"


def _call(service, method, path="/v2/email/tags", *, body=None, query=None):
    raw_body = json.dumps(body).encode("utf-8") if body is not None else b""
    status, _headers, raw = asyncio.run(
        service.handle_request(method, path, {}, raw_body, query or {})
    )
    return status, json.loads(raw.decode("utf-8")) if raw else {}


def test_ses_v2_target_delegates_to_ses_v2_handler(monkeypatch):
    """The legacy SES dispatcher must not retain a separate v2 implementation."""
    from ministack.services import ses, ses_v2

    received = {}

    async def delegated_handler(method, path, headers, body, query_params):
        received.update(
            method=method,
            path=path,
            headers=headers,
            body=body,
            query_params=query_params,
        )
        return 204, {}, b""

    monkeypatch.setattr(ses_v2, "handle_request", delegated_handler)
    result = asyncio.run(
        ses.handle_request(
            "POST", "/", {"x-amz-target": "SESv2.SendEmail"}, b"{}", {"key": ["value"]}
        )
    )

    assert result == (204, {}, b"")
    assert received == {
        "method": "POST",
        "path": "/",
        "headers": {"x-amz-target": "SESv2.SendEmail"},
        "body": b"{}",
        "query_params": {"key": ["value"]},
    }


@pytest.mark.parametrize("prefix", ["", "/v2/email"])
@pytest.mark.parametrize("suffix", ["", "/"])
@pytest.mark.parametrize("bulk", [False, True])
def test_ses_target_send_uses_real_v2_handler(ses_v2, prefix, suffix, bulk):
    _verify(ses_v2)
    from ministack.services import ses

    template = {"TemplateContent": {"Subject": "Hello {{name}}", "Text": "Welcome"},
                "TemplateData": json.dumps({"name": "Alice"})}
    destination = {"ToAddresses": ["recipient@example.com"]}
    body = {"FromEmailAddress": "sender@example.com"}
    if bulk:
        operation, route = "SendBulkEmail", "/outbound-bulk-emails"
        body.update(DefaultContent={"Template": template},
                    BulkEmailEntries=[{"Destination": destination}])
    else:
        operation, route = "SendEmail", "/outbound-emails"
        body.update(Content={"Template": template}, Destination=destination)

    status, _, raw = asyncio.run(ses.handle_request(
        "POST", prefix + route + suffix, {"x-amz-target": f"SESv2.{operation}"},
        json.dumps(body).encode(), {},
    ))

    assert status == 200
    response = json.loads(raw)
    message_id = response["BulkEmailEntryResults"][0]["MessageId"] if bulk else response["MessageId"]
    records = ses_v2._sent_emails_list()
    assert len(records) == 1
    assert records[0]["MessageId"] == message_id
    assert records[0]["Subject"] == "Hello Alice"
    assert records[0]["To"] == destination["ToAddresses"]


def _send_template(ses_v2, template, template_data, *, to="recipient@example.com"):
    return _call(
        ses_v2,
        "POST",
        "/v2/email/outbound-emails",
        body={
            "FromEmailAddress": "noreply@example.com",
            "Destination": {"ToAddresses": [to]},
            "Content": {"Template": dict(template, TemplateData=template_data)},
        },
    )


def test_ses_v2_identity_tag_resource_uses_parser_backed_resource_arn(ses_v2):
    identity = "parser.example.com"
    resource_arn = _arn("identity", identity)

    status, _body = _call(
        ses_v2,
        "POST",
        "/v2/email/identities",
        body={
            "EmailIdentity": identity,
            "Tags": [{"Key": "created", "Value": "yes"}],
        },
    )
    assert status == 200

    status, body = _call(
        ses_v2,
        "GET",
        query={"ResourceArn": [resource_arn]},
    )
    assert status == 200
    assert body["Tags"] == [{"Key": "created", "Value": "yes"}]

    status, _body = _call(
        ses_v2,
        "POST",
        body={
            "ResourceArn": resource_arn,
            "Tags": [
                {"Key": "created", "Value": "updated"},
                {"Key": "team", "Value": "platform"},
            ],
        },
    )
    assert status == 200

    status, body = _call(ses_v2, "GET", query={"ResourceArn": [resource_arn]})
    assert status == 200
    assert body["Tags"] == [
        {"Key": "created", "Value": "updated"},
        {"Key": "team", "Value": "platform"},
    ]

    status, _body = _call(
        ses_v2,
        "DELETE",
        query={"ResourceArn": [resource_arn], "TagKeys": ["created"]},
    )
    assert status == 200

    status, body = _call(ses_v2, "GET", query={"ResourceArn": [resource_arn]})
    assert status == 200
    assert body["Tags"] == [{"Key": "team", "Value": "platform"}]


def test_ses_v2_create_domain_identity_returns_easy_dkim_tokens(ses_v2):
    """A domain created without DkimSigningAttributes gets Easy DKIM tokens (CreateEmailIdentity API reference)."""
    status, body = _call(
        ses_v2,
        "POST",
        "/v2/email/identities",
        body={"EmailIdentity": "dkim.example.com"},
    )
    assert status == 200
    dkim = body["DkimAttributes"]
    assert len(set(dkim["Tokens"])) == 3
    assert (dkim["SigningAttributesOrigin"], dkim["Status"]) == ("AWS_SES", "PENDING")

    status, body = _call(ses_v2, "GET", "/v2/email/identities/dkim.example.com")
    assert status == 200
    assert body["DkimAttributes"]["Tokens"] == dkim["Tokens"]


def test_ses_v2_create_byodkim_domain_identity_gets_no_easy_dkim_tokens(ses_v2):
    """DkimSigningAttributes with a key and selector is BYODKIM, not Easy DKIM."""
    status, body = _call(
        ses_v2,
        "POST",
        "/v2/email/identities",
        body={"EmailIdentity": "byodkim.example.com", "DkimSigningAttributes": {
            "DomainSigningSelector": "sel1", "DomainSigningPrivateKey": "cHJpdmF0ZQ=="}},
    )
    assert status == 200
    assert body["DkimAttributes"].get("SigningAttributesOrigin") != "AWS_SES"


def test_ses_v2_create_email_address_identity_has_no_dkim_tokens(ses_v2):
    status, body = _call(
        ses_v2,
        "POST",
        "/v2/email/identities",
        body={"EmailIdentity": "person@example.com"},
    )
    assert status == 200
    assert body["DkimAttributes"]["SigningEnabled"] is False
    assert body["DkimAttributes"]["Tokens"] == []


def test_ses_v2_configuration_set_tag_resource_uses_parser_backed_resource_arn(ses_v2):
    config_set_name = "parser-config"
    resource_arn = _arn("configuration-set", config_set_name)

    status, _body = _call(
        ses_v2,
        "POST",
        "/v2/email/configuration-sets",
        body={
            "ConfigurationSetName": config_set_name,
            "Tags": [{"Key": "created", "Value": "yes"}],
        },
    )
    assert status == 200

    status, _body = _call(
        ses_v2,
        "POST",
        body={
            "ResourceArn": resource_arn,
            "Tags": [{"Key": "team", "Value": "email"}],
        },
    )
    assert status == 200

    status, body = _call(ses_v2, "GET", query={"ResourceArn": [resource_arn]})
    assert status == 200
    assert body["Tags"] == [
        {"Key": "created", "Value": "yes"},
        {"Key": "team", "Value": "email"},
    ]


@pytest.mark.parametrize(
    "bad_arn",
    [
        "not-an-arn",
        f"arn:aws:ses:{REGION}:{ACCOUNT_ID}",
        _arn("identity", "parser.example.com", partition="aws-cn"),
        _arn("identity", "parser.example.com", service="sesv2"),
        _arn("identity", "parser.example.com", region="us-west-2"),
        _arn("identity", "parser.example.com", account="111111111111"),
        _arn("template", "parser-template"),
        f"arn:aws:ses:{REGION}:{ACCOUNT_ID}:identity/parser.example.com/extra",
    ],
)
@pytest.mark.parametrize("method", ["GET", "POST", "DELETE"])
def test_ses_v2_tag_apis_reject_invalid_resource_arns_before_touching_tags(ses_v2, bad_arn, method):
    identity = "parser.example.com"
    valid_arn = _arn("identity", identity)
    _call(ses_v2, "POST", "/v2/email/identities", body={"EmailIdentity": identity})
    _call(
        ses_v2,
        "POST",
        body={"ResourceArn": valid_arn, "Tags": [{"Key": "keep", "Value": "yes"}]},
    )

    if method == "POST":
        status, body = _call(
            ses_v2,
            method,
            body={"ResourceArn": bad_arn, "Tags": [{"Key": "bad", "Value": "no"}]},
        )
    elif method == "DELETE":
        status, body = _call(
            ses_v2,
            method,
            query={"ResourceArn": [bad_arn], "TagKeys": ["keep"]},
        )
    else:
        status, body = _call(ses_v2, method, query={"ResourceArn": [bad_arn]})

    assert status == 400
    assert body["name"] == "BadRequestException"
    assert ses_v2._ses_tags.get(bad_arn) is None

    status, body = _call(ses_v2, "GET", query={"ResourceArn": [valid_arn]})
    assert status == 200
    assert body["Tags"] == [{"Key": "keep", "Value": "yes"}]


@pytest.mark.parametrize("method", ["GET", "POST", "DELETE"])
def test_ses_v2_tag_apis_reject_missing_local_resources_before_touching_tags(ses_v2, method):
    missing_arn = _arn("identity", "missing.example.com")

    if method == "POST":
        status, body = _call(
            ses_v2,
            method,
            body={"ResourceArn": missing_arn, "Tags": [{"Key": "bad", "Value": "no"}]},
        )
    elif method == "DELETE":
        status, body = _call(
            ses_v2,
            method,
            query={"ResourceArn": [missing_arn], "TagKeys": ["bad"]},
        )
    else:
        status, body = _call(ses_v2, method, query={"ResourceArn": [missing_arn]})

    assert status == 404
    assert body["name"] == "NotFoundException"
    assert ses_v2._ses_tags.get(missing_arn) is None


def test_ses_v2_email_template_crud(ses_v2):
    status, _body = _call(
        ses_v2,
        "POST",
        "/v2/email/templates",
        body={
            "TemplateName": "welcome",
            "TemplateContent": {
                "Subject": "Hello {{name}}",
                "Text": "Hi {{name}}",
                "Html": "<p>Hi {{name}}</p>",
            },
            "Tags": [{"Key": "team", "Value": "growth"}],
        },
    )
    assert status == 200

    status, body = _call(ses_v2, "GET", "/v2/email/templates/welcome")
    assert status == 200
    assert body["TemplateName"] == "welcome"
    assert body["TemplateContent"] == {
        "Subject": "Hello {{name}}",
        "Text": "Hi {{name}}",
        "Html": "<p>Hi {{name}}</p>",
    }
    assert body["Tags"] == [{"Key": "team", "Value": "growth"}]

    status, body = _call(ses_v2, "GET", "/v2/email/templates")
    assert status == 200
    assert [t["TemplateName"] for t in body["TemplatesMetadata"]] == ["welcome"]
    assert body["TemplatesMetadata"][0]["CreatedTimestamp"]

    status, _body = _call(
        ses_v2,
        "PUT",
        "/v2/email/templates/welcome",
        body={"TemplateContent": {"Subject": "Welcome {{name}}", "Text": "Hey {{name}}"}},
    )
    assert status == 200

    status, body = _call(ses_v2, "GET", "/v2/email/templates/welcome")
    assert body["TemplateContent"] == {
        "Subject": "Welcome {{name}}",
        "Text": "Hey {{name}}",
        "Html": "",
    }

    status, _body = _call(ses_v2, "DELETE", "/v2/email/templates/welcome")
    assert status == 200

    status, body = _call(ses_v2, "GET", "/v2/email/templates/welcome")
    assert status == 404
    assert body["name"] == "NotFoundException"


def _create_templates(ses_v2, count, prefix="tpl"):
    names = [f"{prefix}-{i:02d}" for i in range(count)]
    for name in names:
        _call(
            ses_v2,
            "POST",
            "/v2/email/templates",
            body={"TemplateName": name, "TemplateContent": {"Subject": "S", "Text": "T"}},
        )
    return names


def test_ses_v2_list_email_templates_pagination_walks_all_items(ses_v2):
    names = _create_templates(ses_v2, 4)

    seen = []
    token = None
    for _ in range(5):
        query = {"PageSize": ["2"]}
        if token:
            query["NextToken"] = [token]
        status, body = _call(ses_v2, "GET", "/v2/email/templates", query=query)
        assert status == 200
        assert len(body["TemplatesMetadata"]) <= 2
        seen += [t["TemplateName"] for t in body["TemplatesMetadata"]]
        token = body.get("NextToken")
        if not token:
            break
    else:
        raise AssertionError("pagination did not terminate")

    assert seen == names


def test_ses_v2_list_email_templates_defaults_to_ten_per_page(ses_v2):
    names = _create_templates(ses_v2, 12)

    status, body = _call(ses_v2, "GET", "/v2/email/templates")
    assert status == 200
    assert [t["TemplateName"] for t in body["TemplatesMetadata"]] == names[:10]

    status, body = _call(
        ses_v2, "GET", "/v2/email/templates", query={"NextToken": [body["NextToken"]]}
    )
    assert status == 200
    assert [t["TemplateName"] for t in body["TemplatesMetadata"]] == names[10:]
    assert "NextToken" not in body


@pytest.mark.parametrize(
    "query",
    [
        {"PageSize": ["101"]},
        {"PageSize": ["abc"]},
        {"NextToken": ["not base64 at all"]},
    ],
)
def test_ses_v2_list_email_templates_rejects_invalid_paging_parameters(ses_v2, query):
    _create_templates(ses_v2, 2)

    status, body = _call(ses_v2, "GET", "/v2/email/templates", query=query)

    assert status == 400
    assert body["name"] == "BadRequestException"


def test_ses_v2_create_email_template_rejects_duplicates_and_incomplete_requests(ses_v2):
    valid = {"TemplateName": "dup", "TemplateContent": {"Subject": "s", "Text": "t"}}

    status, _body = _call(ses_v2, "POST", "/v2/email/templates", body=valid)
    assert status == 200

    status, body = _call(ses_v2, "POST", "/v2/email/templates", body=valid)
    assert status == 400
    assert body["name"] == "AlreadyExistsException"

    status, body = _call(
        ses_v2, "POST", "/v2/email/templates", body={"TemplateContent": {"Subject": "s"}}
    )
    assert status == 400
    assert body["name"] == "BadRequestException"

    status, body = _call(ses_v2, "POST", "/v2/email/templates", body={"TemplateName": "no-content"})
    assert status == 400
    assert body["name"] == "BadRequestException"
    assert "no-content" not in ses_v2._templates


@pytest.mark.parametrize(
    ("method", "body"),
    [
        ("GET", None),
        ("PUT", {"TemplateContent": {"Subject": "s"}}),
        ("DELETE", None),
    ],
)
def test_ses_v2_email_template_apis_reject_missing_templates(ses_v2, method, body):
    status, response = _call(ses_v2, method, "/v2/email/templates/missing", body=body)

    assert status == 404
    assert response["name"] == "NotFoundException"


def test_ses_v2_send_email_renders_stored_templates(ses_v2):
    _verify(ses_v2)
    _call(
        ses_v2,
        "POST",
        "/v2/email/templates",
        body={
            "TemplateName": "ses-tpl-send",
            "TemplateContent": {
                "Subject": "Hello {{name}}",
                "Text": "Hi {{name}}, order #{{oid}}",
                "Html": "<p>Hi {{name}}</p>",
            },
        },
    )

    status, body = _send_template(
        ses_v2,
        {"TemplateName": "ses-tpl-send"},
        json.dumps({"name": "Alice", "oid": "42"}),
        to='"Alice Example" <alice@example.com>',
    )
    assert status == 200
    assert body["MessageId"]

    record = ses_v2._sent_emails_list()[-1]
    assert record["Type"] == "v2.SendEmail"
    assert record["To"] == ['"Alice Example" <alice@example.com>']
    assert record["Subject"] == "Hello Alice"
    assert record["BodyText"] == "Hi Alice, order #42"
    assert record["BodyHtml"] == "<p>Hi Alice</p>"
    assert record["Template"] == "ses-tpl-send"
    assert record["TemplateData"] == '{"name": "Alice", "oid": "42"}'


def test_ses_v2_send_email_resolves_template_arns(ses_v2):
    _verify(ses_v2)
    _call(
        ses_v2,
        "POST",
        "/v2/email/templates",
        body={"TemplateName": "by-arn", "TemplateContent": {"Subject": "S {{v}}", "Text": "T {{v}}"}},
    )

    status, _body = _send_template(
        ses_v2, {"TemplateArn": _arn("template", "by-arn")}, json.dumps({"v": "x"})
    )
    assert status == 200

    record = ses_v2._sent_emails_list()[-1]
    assert record["Subject"] == "S x"
    assert record["Template"] == "by-arn"


def test_ses_v2_send_email_renders_inline_template_content_without_storing_it(ses_v2):
    _verify(ses_v2)
    status, _body = _send_template(
        ses_v2,
        {"TemplateContent": {"Subject": "Inline {{v}}", "Text": "Body {{v}}"}},
        json.dumps({"v": "42"}),
    )
    assert status == 200

    record = ses_v2._sent_emails_list()[-1]
    assert record["Subject"] == "Inline 42"
    assert record["BodyText"] == "Body 42"
    assert "Template" not in record
    assert list(ses_v2._templates.values()) == []


@pytest.mark.parametrize(
    ("template", "expected_status", "expected_error"),
    [
        ({"TemplateName": "missing"}, 404, "NotFoundException"),
        ({"TemplateArn": _arn("template", "missing")}, 404, "NotFoundException"),
        ({"TemplateArn": _arn("identity", "not-a-template")}, 400, "BadRequestException"),
        ({"TemplateArn": "not-an-arn"}, 400, "BadRequestException"),
        ({}, 400, "BadRequestException"),
    ],
)
def test_ses_v2_send_email_rejects_unusable_templates_without_recording_sends(
    ses_v2, template, expected_status, expected_error
):
    status, body = _send_template(ses_v2, template, json.dumps({"v": "1"}))

    assert status == expected_status
    assert body["name"] == expected_error
    assert ses_v2._sent_emails_list() == []


async def _post_via_app(path, body):
    """Drive MiniStack's ASGI ``app`` in-process so the /v2/email router is exercised."""
    from ministack.app import app as asgi_app

    raw = json.dumps(body).encode("utf-8")
    messages = []

    async def send(message):
        messages.append(message)

    sent = False

    async def receive():
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": raw, "more_body": False}
        return {"type": "http.request", "body": b"", "more_body": False}

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "root_path": "",
        "query_string": b"",
        "headers": [
            (b"host", b"localhost:4566"),
            (b"content-type", b"application/json"),
            (b"content-length", str(len(raw)).encode("ascii")),
        ],
        "client": ("127.0.0.1", 55555),
        "server": ("127.0.0.1", 4566),
    }
    await asgi_app(scope, receive, send)
    start = next(m for m in messages if m["type"] == "http.response.start")
    payload = b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body")
    return start["status"], json.loads(payload.decode("utf-8")) if payload else {}


def _bulk_body(template, entries):
    return {
        "FromEmailAddress": "noreply@example.com",
        "DefaultContent": {"Template": dict(template, TemplateData=json.dumps({"name": "Default"}))},
        "BulkEmailEntries": entries,
    }


def _bulk_entry(to, **replacement):
    entry = {"Destination": {"ToAddresses": [to]}}
    if replacement:
        entry["ReplacementEmailContent"] = {
            "ReplacementTemplate": {"ReplacementTemplateData": json.dumps(replacement)}
        }
    return entry


def test_ses_v2_send_bulk_email_via_app_router_with_inline_template(ses_v2):
    _verify(ses_v2)
    status, body = asyncio.run(
        _post_via_app(
            "/v2/email/outbound-bulk-emails",
            _bulk_body(
                {"TemplateContent": {"Subject": "Hi {{name}}", "Text": "Body {{name}}"}},
                [_bulk_entry("a@example.com", name="Alice"), _bulk_entry("b@example.com")],
            ),
        )
    )

    assert status == 200
    results = body["BulkEmailEntryResults"]
    assert len(results) == 2
    assert all(r["Status"] == "SUCCESS" and r["MessageId"] for r in results)
    assert len({r["MessageId"] for r in results}) == 2

    records = ses_v2._sent_emails_list()
    assert [r["Type"] for r in records] == ["v2.SendBulkEmail"] * 2
    assert [r["MessageId"] for r in records] == [r["MessageId"] for r in results]
    assert records[0]["To"] == ["a@example.com"]
    assert records[0]["Subject"] == "Hi Alice"
    assert records[1]["Subject"] == "Hi Default"
    assert records[1]["BodyText"] == "Body Default"
    assert "Template" not in records[0]
    assert list(ses_v2._templates.values()) == []


def test_ses_v2_send_bulk_email_via_app_router_with_stored_template(ses_v2):
    _verify(ses_v2)
    _call(
        ses_v2,
        "POST",
        "/v2/email/templates",
        body={"TemplateName": "bulk-tpl", "TemplateContent": {"Subject": "S {{name}}", "Text": "T {{name}}"}},
    )

    status, body = asyncio.run(
        _post_via_app(
            "/v2/email/outbound-bulk-emails",
            _bulk_body(
                {"TemplateName": "bulk-tpl"},
                [
                    _bulk_entry("a@example.com", name="A"),
                    _bulk_entry("b@example.com", name="B"),
                    _bulk_entry("c@example.com"),
                ],
            ),
        )
    )

    assert status == 200
    results = body["BulkEmailEntryResults"]
    assert len(results) == 3
    assert all(r["Status"] == "SUCCESS" for r in results)

    records = ses_v2._sent_emails_list()
    assert [r["Subject"] for r in records] == ["S A", "S B", "S Default"]
    assert all(r["Template"] == "bulk-tpl" for r in records)


def test_ses_v2_send_bulk_email_rejects_missing_template_without_recording_sends(ses_v2):
    status, body = asyncio.run(
        _post_via_app(
            "/v2/email/outbound-bulk-emails",
            _bulk_body({"TemplateName": "missing"}, [_bulk_entry("a@example.com")]),
        )
    )

    assert status == 404
    assert body["name"] == "NotFoundException"
    assert ses_v2._sent_emails_list() == []


# ── Tenants and resource associations ──


def _tenant_error(client, operation, code, message, **params):
    with pytest.raises(ClientError) as caught:
        getattr(client, operation)(**params)
    response = caught.value.response
    assert response["Error"] == {"Code": code, "Message": message}
    assert response["ResponseMetadata"]["HTTPStatusCode"] == (404 if code == "NotFoundException" else 400)


def test_tenant_configuration_set_association_lifecycle(sesv2):
    name = "tenant-" + uuid.uuid4().hex[:10]
    tags = [{"Key": "purpose", "Value": "regression"}]
    sesv2.create_configuration_set(ConfigurationSetName=name, Tags=tags)
    tenant = sesv2.create_tenant(TenantName=name, Tags=tags)
    assert re.fullmatch(r"tn-[a-f0-9]{29}", tenant["TenantId"])
    assert tenant["TenantArn"].endswith(f"tenant/{name}/{tenant['TenantId']}")
    assert tenant["SendingStatus"] == "ENABLED"
    assert tenant["Tags"] == tags
    assert sesv2.get_tenant(TenantName=name)["Tenant"] == {k: v for k, v in tenant.items() if k != "ResponseMetadata"}
    _tenant_error(
        sesv2,
        "create_tenant",
        "AlreadyExistsException",
        f"Tenant with name {name} already exists in account 000000000000",
        TenantName=name,
    )
    listed = sesv2.list_tenants(Filter={"TENANT_NAME_CONTAINS": name, "SENDING_STATUS": "ENABLED"})["Tenants"]
    assert listed == [{k: v for k, v in tenant.items() if k not in ("ResponseMetadata", "Tags")}]
    assert sesv2.list_tags_for_resource(ResourceArn=tenant["TenantArn"])["Tags"] == tags
    sesv2.tag_resource(ResourceArn=tenant["TenantArn"], Tags=[{"Key": "updated", "Value": "yes"}])
    sesv2.untag_resource(ResourceArn=tenant["TenantArn"], TagKeys=["purpose"])
    assert sesv2.get_tenant(TenantName=name)["Tenant"]["Tags"] == [{"Key": "updated", "Value": "yes"}]
    arn = f"arn:aws:ses:us-east-1:000000000000:configuration-set/{name}"
    sesv2.create_tenant_resource_association(TenantName=name, ResourceArn=arn)
    _tenant_error(
        sesv2,
        "create_tenant_resource_association",
        "AlreadyExistsException",
        f"Resources {arn} has already been associated with tenant {name}",
        TenantName=name,
        ResourceArn=arn,
    )
    assert sesv2.list_tenant_resources(TenantName=name)["TenantResources"] == [
        {"ResourceType": "CONFIGURATION_SET", "ResourceArn": arn}
    ]
    inverse = sesv2.list_resource_tenants(ResourceArn=arn)["ResourceTenants"]
    assert inverse[0]["TenantId"] == tenant["TenantId"]
    assert inverse[0]["ResourceArn"] == arn
    _tenant_error(
        sesv2,
        "delete_configuration_set",
        "BadRequestException",
        f"Cannot delete <{arn}> because it has tenant associations. Remove all tenant associations and try again.",
        ConfigurationSetName=name,
    )
    sesv2.delete_tenant_resource_association(TenantName=name, ResourceArn=arn)
    sesv2.delete_tenant_resource_association(TenantName=name, ResourceArn=arn)
    assert sesv2.list_tenant_resources(TenantName=name)["TenantResources"] == []
    sesv2.delete_tenant(TenantName=name)
    _tenant_error(sesv2, "get_tenant", "NotFoundException", f"The requested tenant <{name}> does not exist.", TenantName=name)
    _tenant_error(
        sesv2,
        "list_tags_for_resource",
        "NotFoundException",
        f"No Tenant present with name: {name}with tenantId: {tenant['TenantId']}",
        ResourceArn=tenant["TenantArn"],
    )
    for operation, params in [
        ("tag_resource", {"Tags": [{"Key": "purpose", "Value": "regression"}]}),
        ("untag_resource", {"TagKeys": ["purpose"]}),
    ]:
        _tenant_error(sesv2, operation, "NotFoundException", f"No Tenant present with name: {name}with tenantId: {tenant['TenantId']}",
               ResourceArn=tenant["TenantArn"], **params)
    sesv2.delete_configuration_set(ConfigurationSetName=name)


def test_tenant_invalid_and_missing_resources(sesv2):
    name = "tenant-errors-" + uuid.uuid4().hex[:10]
    _tenant_error(
        sesv2,
        "create_tenant",
        "BadRequestException",
        f"Invalid tenant name <{name} bad>: only alphanumeric ASCII characters, '_', and '-' are allowed.",
        TenantName=name + " bad",
    )
    _tenant_error(
        sesv2,
        "create_tenant_resource_association",
        "NotFoundException",
        f"The requested tenant <{name}> does not exist.",
        TenantName=name,
        ResourceArn="invalid-arn",
    )
    sesv2.create_tenant(TenantName=name)
    _tenant_error(
        sesv2,
        "create_tenant_resource_association",
        "BadRequestException",
        "Provided resource identifier is not an SES resource",
        TenantName=name,
        ResourceArn="invalid-arn",
    )
    arn = f"arn:aws:ses:us-east-1:000000000000:configuration-set/{name}"
    _tenant_error(
        sesv2,
        "create_tenant_resource_association",
        "NotFoundException",
        f"Configuration set <{name}> does not exist:",
        TenantName=name,
        ResourceArn=arn,
    )
    other = arn.replace("us-east-1", "us-west-2")
    _tenant_error(
        sesv2,
        "create_tenant_resource_association",
        "BadRequestException",
        f"Resource <{other}> must be in the same region",
        TenantName=name,
        ResourceArn=other,
    )
    sesv2.delete_tenant(TenantName=name)


@pytest.mark.parametrize("filters,message", [
    ({"INVALID": "template"},
     "1 validation error detected: Value at 'filter' failed to satisfy constraint: Map keys must satisfy constraint: [Member must satisfy enum value set: [RESOURCE_TYPE]]"),
    ({"RESOURCE_TYPE": "bogus"}, "Invalid resource type bogus specified."),
    ({"RESOURCE_TYPE": "template,configuration-set"},
     "Invalid resource type template,configuration-set specified."),
])
def test_tenant_resource_filter_errors(sesv2, filters, message):
    name = "tenant-filter-" + uuid.uuid4().hex[:10]
    sesv2.create_tenant(TenantName=name)
    _tenant_error(sesv2, "list_tenant_resources", "BadRequestException", message,
           TenantName=name, Filter=filters)
    sesv2.delete_tenant(TenantName=name)


def test_tenant_sending_status_filter(sesv2):
    name = "tenant-status-" + uuid.uuid4().hex[:10]
    sesv2.create_tenant(TenantName=name)
    _tenant_error(sesv2, "list_tenants", "BadRequestException", "Invalid sending status <bogus>.",
           Filter={"TENANT_NAME_CONTAINS": name, "SENDING_STATUS": "bogus"})
    for status in ("REINSTATED", "DISABLED"):
        assert sesv2.list_tenants(Filter={"TENANT_NAME_CONTAINS": name, "SENDING_STATUS": status})["Tenants"] == []
    sesv2.delete_tenant(TenantName=name)


def test_tenant_template_association_and_delete_with_associations(sesv2):
    name = "tenant-template-" + uuid.uuid4().hex[:10]
    sesv2.create_email_template(TemplateName=name, TemplateContent={"Subject": "probe", "Text": "probe"})
    sesv2.create_configuration_set(ConfigurationSetName=name)
    template_arn = f"arn:aws:ses:us-east-1:000000000000:template/{name}"
    config_arn = f"arn:aws:ses:us-east-1:000000000000:configuration-set/{name}"
    tenants = [name + suffix for suffix in ("-a", "-b")]
    for tenant_name in tenants:
        sesv2.create_tenant(TenantName=tenant_name)
        for arn in (config_arn, template_arn):
            sesv2.create_tenant_resource_association(TenantName=tenant_name, ResourceArn=arn)
        assert sesv2.list_tenant_resources(TenantName=tenant_name, Filter={"RESOURCE_TYPE": "EMAIL_TEMPLATE"})["TenantResources"] == [
            {"ResourceType": "EMAIL_TEMPLATE", "ResourceArn": template_arn}
        ]
        first = sesv2.list_tenant_resources(TenantName=tenant_name, PageSize=1)
        second = sesv2.list_tenant_resources(TenantName=tenant_name, PageSize=1, NextToken=first["NextToken"])
        assert first["TenantResources"] + second["TenantResources"] == [
            {"ResourceType": "CONFIGURATION_SET", "ResourceArn": config_arn},
            {"ResourceType": "EMAIL_TEMPLATE", "ResourceArn": template_arn},
        ]
        assert "NextToken" not in second
    first = sesv2.list_resource_tenants(ResourceArn=config_arn, PageSize=1)
    second = sesv2.list_resource_tenants(ResourceArn=config_arn, PageSize=1, NextToken=first["NextToken"])
    assert [item["TenantName"] for item in first["ResourceTenants"] + second["ResourceTenants"]] == tenants
    assert "NextToken" not in second
    sesv2.delete_tenant(TenantName=tenants[0])
    assert [item["TenantName"] for item in sesv2.list_resource_tenants(ResourceArn=config_arn)["ResourceTenants"]] == tenants[1:]
    _tenant_error(sesv2, "delete_configuration_set", "BadRequestException",
           f"Cannot delete <{config_arn}> because it has tenant associations. Remove all tenant associations and try again.",
           ConfigurationSetName=name)
    sesv2.delete_tenant(TenantName=tenants[1])
    assert sesv2.list_resource_tenants(ResourceArn=config_arn)["ResourceTenants"] == []
    sesv2.delete_configuration_set(ConfigurationSetName=name)
    sesv2.delete_email_template(TemplateName=name)


def test_tenant_state_roundtrip_and_account_region_isolation():
    from ministack.core.persistence import _json_default, _json_object_hook
    from ministack.services import ses_v2

    snapshot = copy.deepcopy(ses_v2.get_state())
    scopes = [
        ("000000000000", "us-east-1"),
        ("111111111111", "us-east-1"),
        ("000000000000", "us-west-2"),
    ]
    saved_tenants = {}
    try:
        for account, region in scopes:
            set_request_account_id(account)
            set_request_region(region)
            assert "same" not in ses_v2._tenants
            assert "same" not in ses_v2._tenant_resources
            status, _, body = ses_v2._tenant_request(
                "POST", "/tenants", {"TenantName": "same", "Tags": [{"Key": "region", "Value": region}]}
            )
            assert status == 200
            saved_tenants[(account, region)] = json.loads(body)
            arn = f"arn:aws:ses:{region}:{account}:configuration-set/same"
            ses_v2._config_sets["same"] = {"ConfigurationSetName": "same"}
            assert ses_v2._tenant_request(
                "POST", "/tenants/resources", {"TenantName": "same", "ResourceArn": arn}
            )[0] == 200

        encoded = json.dumps(ses_v2.get_state(), default=_json_default, sort_keys=True)
        saved = json.loads(encoded, object_hook=_json_object_hook)
        ses_v2.reset()
        for account, region in scopes:
            set_request_account_id(account)
            set_request_region(region)
            assert not ses_v2._tenants
            assert not ses_v2._tenant_resources
        ses_v2.load_persisted_state(saved)
        assert json.dumps(ses_v2.get_state(), default=_json_default, sort_keys=True) == encoded
        for account, region in scopes:
            set_request_account_id(account)
            set_request_region(region)
            tenant = saved_tenants[(account, region)]
            assert ses_v2._tenants["same"] == tenant
            assert ses_v2._ses_tags[tenant["TenantArn"]] == [{"Key": "region", "Value": region}]
            arn = f"arn:aws:ses:{region}:{account}:configuration-set/same"
            assert list(ses_v2._tenant_resources["same"]) == [arn]
            inverse = json.loads(ses_v2._tenant_request(
                "POST", "/resources/tenants/list", {"ResourceArn": arn}
            )[2])["ResourceTenants"]
            assert inverse == [{
                "TenantName": "same",
                "TenantId": tenant["TenantId"],
                "ResourceArn": arn,
                "AssociatedTimestamp": ses_v2._tenant_resources["same"][arn],
            }]
    finally:
        ses_v2.reset()
        ses_v2.load_persisted_state(snapshot)


def test_tenant_suppression_lifecycle(sesv2):
    name = "tenant-suppr-" + uuid.uuid4().hex[:10]
    attrs = {"SuppressedReasons": ["BOUNCE"], "SuppressionScope": "TENANT"}
    created = sesv2.create_tenant(TenantName=name, SuppressionAttributes=attrs)
    assert created["SuppressionAttributes"] == attrs
    assert sesv2.get_tenant(TenantName=name)["Tenant"]["SuppressionAttributes"] == attrs
    listed = sesv2.list_tenants(Filter={"TENANT_NAME_CONTAINS": name})["Tenants"]
    assert listed and "SuppressionAttributes" not in listed[0]
    updated = {"SuppressedReasons": ["BOUNCE", "COMPLAINT"], "SuppressionScope": "ACCOUNT"}
    sesv2.put_tenant_suppression_attributes(TenantName=name, **updated)
    assert sesv2.get_tenant(TenantName=name)["Tenant"]["SuppressionAttributes"] == updated
    sesv2.put_tenant_suppression_attributes(TenantName=name)
    assert "SuppressionAttributes" not in sesv2.get_tenant(TenantName=name)["Tenant"]
    sesv2.put_tenant_suppression_attributes(
        TenantName=name, SuppressedReasons=[], SuppressionScope="TENANT"
    )
    assert sesv2.get_tenant(TenantName=name)["Tenant"]["SuppressionAttributes"] == {
        "SuppressedReasons": [],
        "SuppressionScope": "TENANT",
    }
    sesv2.delete_tenant(TenantName=name)


def test_tenant_suppression_validation(sesv2):
    name = "tenant-supval-" + uuid.uuid4().hex[:10]
    create_reasons = (
        "1 validation error detected: Value at 'suppressionAttributes.suppressedReasons'"
        " failed to satisfy constraint: Member must satisfy constraint:"
        " [Member must satisfy enum value set: [BOUNCE, COMPLAINT]]"
    )
    create_scope = (
        "1 validation error detected: Value at 'suppressionAttributes.suppressionScope'"
        " failed to satisfy constraint: Member must satisfy enum value set: [TENANT, ACCOUNT]"
    )
    put_reasons = create_reasons.replace("suppressionAttributes.suppressedReasons", "suppressedReasons")
    put_scope = create_scope.replace("suppressionAttributes.suppressionScope", "suppressionScope")
    null_reasons = (
        "1 validation error detected: Value null at 'suppressionAttributes.suppressedReasons'"
        " failed to satisfy constraint: Member must not be null"
    )
    null_scope = (
        "1 validation error detected: Value null at 'suppressionAttributes.suppressionScope'"
        " failed to satisfy constraint: Member must not be null"
    )
    both_null = "2 validation errors detected: " + "; ".join(
        [null_reasons.split(": ", 1)[1], null_scope.split(": ", 1)[1]]
    )
    both_create = "2 validation errors detected: " + "; ".join(
        [create_reasons.split(": ", 1)[1], create_scope.split(": ", 1)[1]]
    )
    both_put = "2 validation errors detected: " + "; ".join(
        [put_reasons.split(": ", 1)[1], put_scope.split(": ", 1)[1]]
    )
    bad_attrs = {"SuppressedReasons": ["NOPE"], "SuppressionScope": "TENANT"}
    _tenant_error(sesv2, "create_tenant", "BadRequestException", create_reasons,
           TenantName=name, SuppressionAttributes=bad_attrs)
    _tenant_error(sesv2, "create_tenant", "BadRequestException", create_scope,
           TenantName=name,
           SuppressionAttributes={"SuppressedReasons": ["BOUNCE"], "SuppressionScope": "NOPE"})
    _tenant_error(sesv2, "create_tenant", "BadRequestException", both_create,
           TenantName=name,
           SuppressionAttributes={"SuppressedReasons": ["NOPE"], "SuppressionScope": "NOPE"})
    _tenant_error(sesv2, "create_tenant", "BadRequestException",
           "SuppressedReasons cannot be specified without SuppressionScope.",
           TenantName=name, SuppressionAttributes={"SuppressedReasons": ["BOUNCE"]})
    _tenant_error(sesv2, "create_tenant", "BadRequestException",
           "SuppressionScope cannot be specified without SuppressedReasons.",
           TenantName=name, SuppressionAttributes={"SuppressionScope": "TENANT"})
    _tenant_error(sesv2, "create_tenant", "BadRequestException", both_null,
           TenantName=name, SuppressionAttributes={})
    _tenant_error(sesv2, "create_tenant", "BadRequestException", null_scope,
           TenantName=name, SuppressionAttributes={"SuppressedReasons": []})
    # Precedence: model enum errors beat the name check, which beats pairing rules,
    # which beat the duplicate check.
    _tenant_error(sesv2, "create_tenant", "BadRequestException", create_reasons,
           TenantName="bad name", SuppressionAttributes=bad_attrs)
    _tenant_error(sesv2, "create_tenant", "BadRequestException",
           "Invalid tenant name <bad name>: only alphanumeric ASCII characters, '_', and '-' are allowed.",
           TenantName="bad name", SuppressionAttributes={"SuppressedReasons": ["BOUNCE"]})
    sesv2.create_tenant(TenantName=name)
    _tenant_error(sesv2, "create_tenant", "BadRequestException", create_reasons,
           TenantName=name, SuppressionAttributes=bad_attrs)
    _tenant_error(sesv2, "create_tenant", "BadRequestException",
           "SuppressedReasons cannot be specified without SuppressionScope.",
           TenantName=name, SuppressionAttributes={"SuppressedReasons": ["BOUNCE"]})
    _tenant_error(sesv2, "put_tenant_suppression_attributes", "BadRequestException", put_reasons,
           TenantName=name, SuppressedReasons=["NOPE"], SuppressionScope="TENANT")
    _tenant_error(sesv2, "put_tenant_suppression_attributes", "BadRequestException", put_scope,
           TenantName=name, SuppressedReasons=["BOUNCE"], SuppressionScope="NOPE")
    _tenant_error(sesv2, "put_tenant_suppression_attributes", "BadRequestException", both_put,
           TenantName=name, SuppressedReasons=["NOPE"], SuppressionScope="NOPE")
    _tenant_error(sesv2, "put_tenant_suppression_attributes", "BadRequestException",
           "SuppressedReasons cannot be specified without SuppressionScope.",
           TenantName=name, SuppressedReasons=["BOUNCE"])
    _tenant_error(sesv2, "put_tenant_suppression_attributes", "BadRequestException",
           "SuppressionScope is required when SuppressedReasons are provided. Valid values are: TENANT, ACCOUNT",
           TenantName=name, SuppressedReasons=[])
    _tenant_error(sesv2, "put_tenant_suppression_attributes", "BadRequestException",
           "SuppressionScope cannot be specified without SuppressedReasons.",
           TenantName=name, SuppressionScope="TENANT")
    missing = name + "-missing"
    _tenant_error(sesv2, "put_tenant_suppression_attributes", "BadRequestException", put_reasons,
           TenantName=missing, SuppressedReasons=["NOPE"], SuppressionScope="TENANT")
    _tenant_error(sesv2, "put_tenant_suppression_attributes", "BadRequestException",
           "SuppressionScope cannot be specified without SuppressedReasons.",
           TenantName=missing, SuppressionScope="TENANT")
    _tenant_error(sesv2, "put_tenant_suppression_attributes", "NotFoundException",
           f"The requested tenant <{missing}> does not exist.", TenantName=missing)
    sesv2.delete_tenant(TenantName=name)


def test_tenant_list_ordering(sesv2):
    prefix = "tenant-ord-" + uuid.uuid4().hex[:10]
    first, second = prefix + "-zz", prefix + "-aa"
    sesv2.create_tenant(TenantName=first)
    sesv2.create_tenant(TenantName=second)
    names = [
        t["TenantName"]
        for t in sesv2.list_tenants(Filter={"TENANT_NAME_CONTAINS": prefix})["Tenants"]
    ]
    assert names == [second, first]
    cs_b, cs_a = prefix + "-cs-b", prefix + "-cs-a"
    sesv2.create_configuration_set(ConfigurationSetName=cs_b)
    sesv2.create_configuration_set(ConfigurationSetName=cs_a)
    dom = prefix + ".example.com"
    sesv2.create_email_identity(EmailIdentity=dom)
    arns = [
        f"arn:aws:ses:us-east-1:000000000000:{kind}/{leaf}"
        for kind, leaf in [
            ("configuration-set", cs_b),
            ("identity", dom),
            ("configuration-set", cs_a),
        ]
    ]
    for arn in arns:
        sesv2.create_tenant_resource_association(TenantName=first, ResourceArn=arn)
    listed = sesv2.list_tenant_resources(TenantName=first)["TenantResources"]
    assert [item["ResourceArn"] for item in listed] == sorted(arns)
    for arn in arns:
        sesv2.delete_tenant_resource_association(TenantName=first, ResourceArn=arn)
    sesv2.delete_tenant(TenantName=first)
    sesv2.delete_tenant(TenantName=second)
    sesv2.delete_configuration_set(ConfigurationSetName=cs_b)
    sesv2.delete_configuration_set(ConfigurationSetName=cs_a)
    sesv2.delete_email_identity(EmailIdentity=dom)


def test_tenant_resource_tenants_keep_creation_order(sesv2):
    prefix = "tenant-rto-" + uuid.uuid4().hex[:10]
    cs = prefix + "-cs"
    sesv2.create_configuration_set(ConfigurationSetName=cs)
    arn = f"arn:aws:ses:us-east-1:000000000000:configuration-set/{cs}"
    early, late = prefix + "-early", prefix + "-late"
    sesv2.create_tenant(TenantName=early)
    sesv2.create_tenant(TenantName=late)
    sesv2.create_tenant_resource_association(TenantName=late, ResourceArn=arn)
    sesv2.create_tenant_resource_association(TenantName=early, ResourceArn=arn)
    names = [
        item["TenantName"]
        for item in sesv2.list_resource_tenants(ResourceArn=arn)["ResourceTenants"]
    ]
    assert names == [early, late]
    sesv2.delete_tenant(TenantName=early)
    sesv2.delete_tenant(TenantName=late)
    sesv2.delete_configuration_set(ConfigurationSetName=cs)


def test_tenant_resource_delete_guards(sesv2, ses):
    prefix = "tenant-guard-" + uuid.uuid4().hex[:10]
    cs, tpl, dom = prefix + "-cs", prefix + "-tpl", prefix + ".example.com"
    sesv2.create_configuration_set(ConfigurationSetName=cs)
    sesv2.create_email_template(TemplateName=tpl, TemplateContent={"Subject": "s", "Text": "t"})
    sesv2.create_email_identity(EmailIdentity=dom)
    name = prefix + "-t"
    sesv2.create_tenant(TenantName=name)
    arns = {
        kind: f"arn:aws:ses:us-east-1:000000000000:{kind}/{leaf}"
        for kind, leaf in [("configuration-set", cs), ("template", tpl), ("identity", dom)]
    }
    for arn in arns.values():
        sesv2.create_tenant_resource_association(TenantName=name, ResourceArn=arn)
    for operation, params in [
        ("delete_configuration_set", {"ConfigurationSetName": cs}),
        ("delete_email_template", {"TemplateName": tpl}),
        ("delete_email_identity", {"EmailIdentity": dom}),
    ]:
        kind = {"delete_configuration_set": "configuration-set",
                "delete_email_template": "template",
                "delete_email_identity": "identity"}[operation]
        _tenant_error(sesv2, operation, "BadRequestException",
               f"Cannot delete <{arns[kind]}> because it has tenant associations."
               " Remove all tenant associations and try again.",
               **params)
    for operation, params in [
        ("delete_configuration_set", {"ConfigurationSetName": cs}),
        ("delete_template", {"TemplateName": tpl}),
        ("delete_identity", {"Identity": dom}),
    ]:
        kind = {"delete_configuration_set": "configuration-set",
                "delete_template": "template",
                "delete_identity": "identity"}[operation]
        _tenant_error(ses, operation, "InvalidParameterValue",
               f"Cannot delete <{arns[kind]}> because it has tenant associations."
               " Remove all tenant associations and try again.",
               **params)
    for arn in arns.values():
        sesv2.delete_tenant_resource_association(TenantName=name, ResourceArn=arn)
    sesv2.delete_tenant(TenantName=name)
    sesv2.delete_configuration_set(ConfigurationSetName=cs)
    sesv2.delete_email_template(TemplateName=tpl)
    sesv2.delete_email_identity(EmailIdentity=dom)


def test_tenant_association_rejects_non_ses_arn(sesv2):
    name = "tenant-nonses-" + uuid.uuid4().hex[:10]
    sesv2.create_tenant(TenantName=name)
    _tenant_error(sesv2, "create_tenant_resource_association", "BadRequestException",
           "Provided ARN is not in SES resource ARN format",
           TenantName=name, ResourceArn=f"arn:aws:s3:::{name}-bucket")
    _tenant_error(sesv2, "list_tags_for_resource", "NotFoundException",
           f"No Tenant present with name: nullwith tenantId: {name}",
           ResourceArn=f"arn:aws:ses:us-east-1:000000000000:tenant/{name}")
    sesv2.delete_tenant(TenantName=name)


def test_tenant_list_pagesize_bounds(sesv2):
    name = "tenant-pages-" + uuid.uuid4().hex[:10]
    sesv2.create_tenant(TenantName=name)
    _tenant_error(sesv2, "list_tenants", "BadRequestException",
           "1 validation error detected: Value '0' at 'pageSize' failed to satisfy"
           " constraint: Member must have value greater than or equal to 1",
           PageSize=0)
    over = ("1 validation error detected: Value '101' at 'pageSize' failed to satisfy"
            " constraint: Member must have value less than or equal to 100")
    _tenant_error(sesv2, "list_tenants", "BadRequestException", over, PageSize=101)
    _tenant_error(sesv2, "list_tenant_resources", "BadRequestException", over,
           TenantName=name, PageSize=101)
    cs = name + "-cs"
    sesv2.create_configuration_set(ConfigurationSetName=cs)
    arn = f"arn:aws:ses:us-east-1:000000000000:configuration-set/{cs}"
    _tenant_error(sesv2, "list_resource_tenants", "BadRequestException", over,
           ResourceArn=arn, PageSize=101)
    sesv2.delete_tenant(TenantName=name)
    sesv2.delete_configuration_set(ConfigurationSetName=cs)


def test_tenant_v1_resources_can_be_associated(sesv2, ses):
    prefix = "tenant-v1res-" + uuid.uuid4().hex[:10]
    cs, dom = prefix + "-cs", prefix + ".example.com"
    ses.create_configuration_set(ConfigurationSet={"Name": cs})
    ses.verify_domain_identity(Domain=dom)
    name = prefix + "-t"
    sesv2.create_tenant(TenantName=name)
    cs_arn = f"arn:aws:ses:us-east-1:000000000000:configuration-set/{cs}"
    idn_arn = f"arn:aws:ses:us-east-1:000000000000:identity/{dom}"
    sesv2.create_tenant_resource_association(TenantName=name, ResourceArn=cs_arn)
    sesv2.create_tenant_resource_association(TenantName=name, ResourceArn=idn_arn)
    assert sesv2.list_tenant_resources(TenantName=name)["TenantResources"] == [
        {"ResourceType": "CONFIGURATION_SET", "ResourceArn": cs_arn},
        {"ResourceType": "EMAIL_IDENTITY", "ResourceArn": idn_arn},
    ]
    blocked = "because it has tenant associations. Remove all tenant associations and try again."
    _tenant_error(sesv2, "delete_configuration_set", "BadRequestException",
           f"Cannot delete <{cs_arn}> {blocked}", ConfigurationSetName=cs)
    _tenant_error(sesv2, "delete_email_identity", "BadRequestException",
           f"Cannot delete <{idn_arn}> {blocked}", EmailIdentity=dom)
    _tenant_error(ses, "delete_configuration_set", "InvalidParameterValue",
           f"Cannot delete <{cs_arn}> {blocked}", ConfigurationSetName=cs)
    _tenant_error(ses, "delete_identity", "InvalidParameterValue",
           f"Cannot delete <{idn_arn}> {blocked}", Identity=dom)
    sesv2.delete_tenant_resource_association(TenantName=name, ResourceArn=cs_arn)
    sesv2.delete_tenant_resource_association(TenantName=name, ResourceArn=idn_arn)
    sesv2.delete_tenant(TenantName=name)
    ses.delete_configuration_set(ConfigurationSetName=cs)
    ses.delete_identity(Identity=dom)


def test_tenant_association_normalizes_foreign_partitions(sesv2):
    prefix = "tenant-part-" + uuid.uuid4().hex[:10]
    cs = prefix + "-cs"
    sesv2.create_configuration_set(ConfigurationSetName=cs)
    name = prefix + "-t"
    tenant = sesv2.create_tenant(TenantName=name)
    aws_arn = f"arn:aws:ses:us-east-1:000000000000:configuration-set/{cs}"
    cn_arn = f"arn:aws-cn:ses:us-east-1:000000000000:configuration-set/{cs}"
    sesv2.create_tenant_resource_association(TenantName=name, ResourceArn=cn_arn)
    assert sesv2.list_tenant_resources(TenantName=name)["TenantResources"] == [
        {"ResourceType": "CONFIGURATION_SET", "ResourceArn": aws_arn}
    ]
    _tenant_error(sesv2, "create_tenant_resource_association", "AlreadyExistsException",
           f"Resources {aws_arn} has already been associated with tenant {name}",
           TenantName=name, ResourceArn=aws_arn)
    inverse = sesv2.list_resource_tenants(ResourceArn=cn_arn)["ResourceTenants"]
    assert [(item["TenantName"], item["ResourceArn"]) for item in inverse] == [(name, aws_arn)]
    _tenant_error(sesv2, "delete_configuration_set", "BadRequestException",
           f"Cannot delete <{aws_arn}> because it has tenant associations."
           " Remove all tenant associations and try again.",
           ConfigurationSetName=cs)
    sesv2.delete_tenant_resource_association(TenantName=name, ResourceArn=aws_arn)
    assert sesv2.list_tenant_resources(TenantName=name)["TenantResources"] == []
    sesv2.create_tenant_resource_association(TenantName=name, ResourceArn=aws_arn)
    sesv2.delete_tenant_resource_association(TenantName=name, ResourceArn=cn_arn)
    assert sesv2.list_tenant_resources(TenantName=name)["TenantResources"] == []
    xacct = f"arn:aws-cn:ses:us-east-1:111122223333:configuration-set/{cs}"
    _tenant_error(sesv2, "create_tenant_resource_association", "BadRequestException",
           f"Resource <{xacct}> must be in the same account",
           TenantName=name, ResourceArn=xacct)
    # Tag APIs keep rejecting a non-aws partition.
    with pytest.raises(ClientError):
        sesv2.tag_resource(ResourceArn=cn_arn, Tags=[{"Key": "ck", "Value": "v"}])
    sesv2.delete_tenant(TenantName=name)
    sesv2.delete_configuration_set(ConfigurationSetName=cs)


def test_dedicated_ip_pool_lifecycle(sesv2):
    name = f"pool-{uuid.uuid4().hex[:8]}"
    sesv2.create_dedicated_ip_pool(PoolName=name, ScalingMode="MANAGED", Tags=[{"Key": "k", "Value": "v"}])
    try:
        assert sesv2.get_dedicated_ip_pool(PoolName=name)["DedicatedIpPool"] == {
            "PoolName": name, "ScalingMode": "MANAGED"}
        assert name in sesv2.list_dedicated_ip_pools()["DedicatedIpPools"]
        arn = f"arn:aws:ses:us-east-1:000000000000:dedicated-ip-pool/{name}"
        assert sesv2.list_tags_for_resource(ResourceArn=arn)["Tags"] == [{"Key": "k", "Value": "v"}]
        with pytest.raises(ClientError) as exc:
            sesv2.create_dedicated_ip_pool(PoolName=name)
        assert exc.value.response["Error"]["Code"] == "AlreadyExistsException"
    finally:
        sesv2.delete_dedicated_ip_pool(PoolName=name)
    with pytest.raises(ClientError) as exc:
        sesv2.get_dedicated_ip_pool(PoolName=name)
    assert exc.value.response["Error"]["Code"] == "NotFoundException"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
