import asyncio
import json

import pytest

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


def _arn(kind, name, *, partition="aws", service="ses", region=REGION, account=ACCOUNT_ID):
    return f"arn:{partition}:{service}:{region}:{account}:{kind}/{name}"


def _call(service, method, path="/v2/email/tags", *, body=None, query=None):
    raw_body = json.dumps(body).encode("utf-8") if body is not None else b""
    status, _headers, raw = asyncio.run(
        service.handle_request(method, path, {}, raw_body, query or {})
    )
    return status, json.loads(raw.decode("utf-8")) if raw else {}


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
