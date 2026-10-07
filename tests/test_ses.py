"""SES tests — API operations + SMTP relay integration."""

import asyncio
import json
import os
import time
import uuid as _uuid_mod
from datetime import datetime
from unittest.mock import MagicMock, patch
from urllib.parse import urlencode
from uuid import uuid4

import pytest
from botocore import UNSIGNED
from botocore.exceptions import ClientError
from botocore.parsers import create_parser
from botocore.serialize import create_serializer
from botocore.session import Session

from ministack.core import persistence
from ministack.core.responses import (
    AccountRegionScopedDict,
    set_request_account_id,
    set_request_region,
)
from ministack.services import ses as _ses_svc


def test_ses_parse_smtp_host_not_set():
    from ministack.services.ses import _parse_smtp_host
    assert _parse_smtp_host() is None


def test_ses_parse_smtp_host_with_port():
    os.environ['SMTP_HOST'] = '127.0.0.1:1025'
    from ministack.services.ses import _parse_smtp_host
    assert _parse_smtp_host() == ('127.0.0.1', 1025)


def test_ses_parse_smtp_host_without_port():
    os.environ['SMTP_HOST'] = 'mail.example.com'
    from ministack.services.ses import _parse_smtp_host
    assert _parse_smtp_host() == ('mail.example.com', 25)


def test_ses_parse_smtp_host_hostname_with_port():
    os.environ['SMTP_HOST'] = 'smtp.gmail.com:587'
    from ministack.services.ses import _parse_smtp_host
    assert _parse_smtp_host() == ('smtp.gmail.com', 587)


def test_ses_send(ses):
    ses.verify_email_identity(EmailAddress="sender@example.com")
    resp = ses.send_email(
        Source="sender@example.com",
        Destination={"ToAddresses": ["recipient@example.com"]},
        Message={
            "Subject": {"Data": "Test Subject"},
            "Body": {"Text": {"Data": "Hello from MiniStack SES"}},
        },
    )
    assert "MessageId" in resp


@pytest.mark.parametrize("subject", ["", "   "])
def test_ses_send_email_accepts_present_empty_content(monkeypatch, subject):
    from ministack.services import ses as ses_service

    monkeypatch.setattr(ses_service, "_message_rejection", lambda *args: None)
    monkeypatch.setattr(ses_service, "_record_send", lambda **kwargs: "empty-content")
    params = {
        "Source": ["sender@example.com"],
        "Destination.ToAddresses.member.1": ["recipient@example.com"],
        "Message.Subject.Data": [subject],
        "Message.Body.Text.Data": [""],
    }
    assert ses_service._send_email(params)[0] == 200


def test_ses_send_email_rejects_invalid_address(ses):
    ses.verify_email_identity(EmailAddress="sender@example.com")
    with pytest.raises(ClientError) as exc:
        ses.send_email(
            Source="sender@example.com",
            Destination={"ToAddresses": ["not-an-email"]},
            Message={
                "Subject": {"Data": "Invalid address"},
                "Body": {"Text": {"Data": "body"}},
            },
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"
    assert exc.value.response["Error"]["Message"] == "Missing final '@domain'"


def test_ses_send_email_rejects_missing_configuration_set(ses):
    ses.verify_email_identity(EmailAddress="sender@example.com")
    with pytest.raises(ClientError) as exc:
        ses.send_email(
            Source="sender@example.com",
            Destination={"ToAddresses": ["recipient@example.com"]},
            Message={
                "Subject": {"Data": "Configuration"},
                "Body": {"Text": {"Data": "body"}},
            },
            ConfigurationSetName="missing-set",
        )
    assert exc.value.response["Error"]["Code"] == "ConfigurationSetDoesNotExist"
    assert exc.value.response["Error"]["Message"] == "Configuration set missing-set does not exist"
    assert exc.value.response["ConfigurationSetName"] == "missing-set"


def test_ses_list_identities(ses):
    ses.verify_email_identity(EmailAddress="another@example.com")
    resp = ses.list_identities()
    assert "sender@example.com" in resp["Identities"]

def test_ses_quota(ses):
    resp = ses.get_send_quota()
    assert resp["Max24HourSend"] == 50000.0

def test_ses_verify_identity_v2(ses):
    ses.verify_email_identity(EmailAddress="ses-v2@example.com")
    identities = ses.list_identities()["Identities"]
    assert "ses-v2@example.com" in identities

    attrs = ses.get_identity_verification_attributes(Identities=["ses-v2@example.com"])
    assert "ses-v2@example.com" in attrs["VerificationAttributes"]
    assert attrs["VerificationAttributes"]["ses-v2@example.com"]["VerificationStatus"] == "Success"

def test_ses_send_email_v2(ses):
    ses.verify_email_identity(EmailAddress="ses-send-v2@example.com")
    resp = ses.send_email(
        Source="ses-send-v2@example.com",
        Destination={
            "ToAddresses": ["to@example.com"],
            "CcAddresses": ["cc@example.com"],
        },
        Message={"Subject": {"Data": "Test V2"}, "Body": {"Text": {"Data": "Body v2"}}},
    )
    assert "MessageId" in resp

def test_ses_list_identities_v2(ses):
    ses.verify_email_identity(EmailAddress="ses-li-v2@example.com")
    ses.verify_domain_identity(Domain="example-v2.com")
    email_ids = ses.list_identities(IdentityType="EmailAddress")["Identities"]
    assert "ses-li-v2@example.com" in email_ids
    domain_ids = ses.list_identities(IdentityType="Domain")["Identities"]
    assert "example-v2.com" in domain_ids

def test_ses_quota_v2(ses):
    resp = ses.get_send_quota()
    assert resp["Max24HourSend"] == 50000.0
    assert resp["MaxSendRate"] == 14.0
    assert "SentLast24Hours" in resp

def test_ses_send_raw_email_v2(ses):
    ses.verify_email_identity(EmailAddress="raw-v2@example.com")
    raw = (
        "From: raw-v2@example.com\r\n"
        "To: dest-v2@example.com\r\n"
        "Subject: Raw V2\r\n"
        "Content-Type: text/plain\r\n\r\n"
        "Raw body v2"
    )
    resp = ses.send_raw_email(RawMessage={"Data": raw})
    assert "MessageId" in resp

def test_ses_configuration_set_v2(ses):
    ses.create_configuration_set(ConfigurationSet={"Name": "ses-cs-v2"})
    listed = ses.list_configuration_sets()["ConfigurationSets"]
    assert any(cs["Name"] == "ses-cs-v2" for cs in listed)

    described = ses.describe_configuration_set(ConfigurationSetName="ses-cs-v2")
    assert described["ConfigurationSet"]["Name"] == "ses-cs-v2"

    ses.delete_configuration_set(ConfigurationSetName="ses-cs-v2")
    listed2 = ses.list_configuration_sets()["ConfigurationSets"]
    assert not any(cs["Name"] == "ses-cs-v2" for cs in listed2)

def test_ses_template_v2(ses):
    ses.create_template(
        Template={
            "TemplateName": "ses-tpl-v2",
            "SubjectPart": "Hello {{name}}",
            "TextPart": "Hi {{name}}, order #{{oid}}",
            "HtmlPart": "<h1>Hi {{name}}</h1>",
        }
    )
    resp = ses.get_template(TemplateName="ses-tpl-v2")
    assert resp["Template"]["TemplateName"] == "ses-tpl-v2"
    assert "{{name}}" in resp["Template"]["SubjectPart"]

    listed = ses.list_templates()["TemplatesMetadata"]
    assert any(t["Name"] == "ses-tpl-v2" for t in listed)

    ses.update_template(
        Template={
            "TemplateName": "ses-tpl-v2",
            "SubjectPart": "Updated {{name}}",
            "TextPart": "Updated",
            "HtmlPart": "<p>Updated</p>",
        }
    )
    resp2 = ses.get_template(TemplateName="ses-tpl-v2")
    assert "Updated" in resp2["Template"]["SubjectPart"]

    ses.delete_template(TemplateName="ses-tpl-v2")
    with pytest.raises(ClientError):
        ses.get_template(TemplateName="ses-tpl-v2")

def test_ses_send_templated_v2(ses):
    ses.verify_email_identity(EmailAddress="tpl-v2@example.com")
    ses.create_template(
        Template={
            "TemplateName": "ses-tpl-send-v2",
            "SubjectPart": "Hey {{name}}",
            "TextPart": "Hi {{name}}",
            "HtmlPart": "<h1>Hi {{name}}</h1>",
        }
    )
    resp = ses.send_templated_email(
        Source="tpl-v2@example.com",
        Destination={"ToAddresses": ["r@example.com"]},
        Template="ses-tpl-send-v2",
        TemplateData=json.dumps({"name": "Alice"}),
    )
    assert "MessageId" in resp

def test_ses_send_templated_email(ses):
    """SendTemplatedEmail renders template and stores email."""
    ses.verify_email_identity(EmailAddress="sender@example.com")
    ses.create_template(
        Template={
            "TemplateName": "qa-ses-tmpl",
            "SubjectPart": "Hello {{name}}",
            "TextPart": "Hi {{name}}, welcome!",
            "HtmlPart": "<p>Hi {{name}}</p>",
        }
    )
    resp = ses.send_templated_email(
        Source="sender@example.com",
        Destination={"ToAddresses": ["user@example.com"]},
        Template="qa-ses-tmpl",
        TemplateData=json.dumps({"name": "Alice"}),
    )
    assert "MessageId" in resp

def test_ses_verify_domain(ses):
    """VerifyDomainIdentity returns a verification token."""
    resp = ses.verify_domain_identity(Domain="example.com")
    assert "VerificationToken" in resp
    assert len(resp["VerificationToken"]) > 0
    identities = ses.list_identities(IdentityType="Domain")["Identities"]
    assert "example.com" in identities

def test_ses_configuration_set_crud(ses):
    """CreateConfigurationSet / DescribeConfigurationSet / DeleteConfigurationSet."""
    ses.create_configuration_set(ConfigurationSet={"Name": "qa-ses-config"})
    desc = ses.describe_configuration_set(ConfigurationSetName="qa-ses-config")
    assert desc["ConfigurationSet"]["Name"] == "qa-ses-config"
    sets = ses.list_configuration_sets()["ConfigurationSets"]
    assert any(s["Name"] == "qa-ses-config" for s in sets)
    ses.delete_configuration_set(ConfigurationSetName="qa-ses-config")
    sets2 = ses.list_configuration_sets()["ConfigurationSets"]
    assert not any(s["Name"] == "qa-ses-config" for s in sets2)

def test_ses_v2_send_email(sesv2):
    resp = sesv2.send_email(
        FromEmailAddress="sender@example.com",
        Destination={"ToAddresses": ["recipient@example.com"]},
        Content={
            "Simple": {
                "Subject": {"Data": "Test Subject"},
                "Body": {"Text": {"Data": "Hello world"}},
            }
        },
    )
    # Same shape the six v1 send paths return; nothing an AWS client sees
    # should be prefixed with the emulator's name.
    assert resp["MessageId"].endswith("@email.amazonses.com")

def test_ses_v2_email_identity_crud(sesv2):
    sesv2.create_email_identity(EmailIdentity="test-domain.com")
    resp = sesv2.get_email_identity(EmailIdentity="test-domain.com")
    assert resp["VerifiedForSendingStatus"] is True
    lst = sesv2.list_email_identities()
    names = [e["IdentityName"] for e in lst["EmailIdentities"]]
    assert "test-domain.com" in names
    sesv2.delete_email_identity(EmailIdentity="test-domain.com")
    lst2 = sesv2.list_email_identities()
    names2 = [e["IdentityName"] for e in lst2["EmailIdentities"]]
    assert "test-domain.com" not in names2

def test_ses_v2_configuration_set_crud(sesv2):
    sesv2.create_configuration_set(ConfigurationSetName="my-cfg-set")
    resp = sesv2.get_configuration_set(ConfigurationSetName="my-cfg-set")
    assert resp["ConfigurationSetName"] == "my-cfg-set"
    lst = sesv2.list_configuration_sets()
    assert "my-cfg-set" in lst["ConfigurationSets"]
    sesv2.delete_configuration_set(ConfigurationSetName="my-cfg-set")
    lst2 = sesv2.list_configuration_sets()
    assert "my-cfg-set" not in lst2["ConfigurationSets"]

def test_ses_v2_list_routes_old_and_new(sesv2):
    """SDKs since botocore 1.43.106 list with POST /v2/email/list-identities and
    /list-configuration-sets (paging and Filter in the body); older ones GET."""
    import urllib.request

    from conftest import ENDPOINT
    uid = _uuid_mod.uuid4().hex[:8]
    names = [f"list-{uid}-{i}.example.com" for i in range(3)] + [f"user-{uid}@example.com"]
    for name in names:
        sesv2.create_email_identity(EmailIdentity=name)
    sesv2.create_configuration_set(ConfigurationSetName=f"cfg-{uid}")
    try:
        domains = sesv2.list_email_identities(
            Filter={"IDENTITY_NAME_CONTAINS": uid, "IDENTITY_TYPE": "DOMAIN"}, PageSize=2)
        first = [e["IdentityName"] for e in domains["EmailIdentities"]]
        rest = sesv2.list_email_identities(
            Filter={"IDENTITY_NAME_CONTAINS": uid, "IDENTITY_TYPE": "DOMAIN"},
            PageSize=2, NextToken=domains["NextToken"])
        assert sorted(first + [e["IdentityName"] for e in rest["EmailIdentities"]]) == names[:3]
        assert "NextToken" not in rest
        assert sesv2.list_configuration_sets(
            Filter={"CONFIGURATION_SET_NAME_CONTAINS": uid})["ConfigurationSets"] == [f"cfg-{uid}"]

        auth = {"Authorization": "AWS4-HMAC-SHA256 Credential=test/20261001/us-east-1/ses/aws4_request"}
        for path, key, expected in (("/v2/email/identities", "EmailIdentities", names[0]),
                                    ("/v2/email/configuration-sets", "ConfigurationSets", f"cfg-{uid}")):
            with urllib.request.urlopen(urllib.request.Request(ENDPOINT + path, headers=auth)) as r:
                listed = json.loads(r.read())[key]
            assert expected in [i["IdentityName"] if isinstance(i, dict) else i for i in listed]
    finally:
        for name in names:
            sesv2.delete_email_identity(EmailIdentity=name)
        sesv2.delete_configuration_set(ConfigurationSetName=f"cfg-{uid}")


def test_ses_v2_get_account(sesv2):
    resp = sesv2.get_account()
    assert resp["SendingEnabled"] is True
    assert resp["ProductionAccessEnabled"] is True

def test_ses_v2_send_email_with_v1_template(ses, sesv2):
    """A template created with v1 CreateTemplate renders through v2 SendEmail."""
    import urllib.request

    ses.create_template(Template={
        "TemplateName": "cross-version-tmpl",
        "SubjectPart": "Hello {{name}}",
        "TextPart": "Hi {{name}}, order #{{oid}}",
    })

    tpl = sesv2.get_email_template(TemplateName="cross-version-tmpl")
    assert tpl["TemplateContent"]["Subject"] == "Hello {{name}}"

    resp = sesv2.send_email(
        FromEmailAddress="cross-version@example.com",
        Destination={"ToAddresses": ['"Alice Example" <alice@example.com>']},
        Content={"Template": {
            "TemplateName": "cross-version-tmpl",
            "TemplateData": json.dumps({"name": "Alice", "oid": "42"}),
        }},
    )

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    req = urllib.request.Request(f"{endpoint}/_ministack/ses/messages", method="GET")
    with urllib.request.urlopen(req, timeout=5) as r:
        data = json.loads(r.read().decode())

    sent = [m for m in data["messages"]["000000000000"] if m["MessageId"] == resp["MessageId"]]
    assert len(sent) == 1
    assert sent[0]["Type"] == "v2.SendEmail"
    assert sent[0]["To"] == ['"Alice Example" <alice@example.com>']
    assert sent[0]["Subject"] == "Hello Alice"
    assert sent[0]["BodyText"] == "Hi Alice, order #42"

@pytest.fixture(autouse=True)
def _inline_smtp_relay(monkeypatch):
    """Run the background SMTP relay inline so tests can assert on it."""
    monkeypatch.setattr('ministack.services.ses.spawn_background',
                        lambda fn, *args, **_kw: fn(*args))


@pytest.fixture(autouse=True)
def _clear_smtp_host():
    """Ensure SMTP_HOST is clean before/after each test."""
    old = os.environ.pop('SMTP_HOST', None)
    yield
    if old is not None:
        os.environ['SMTP_HOST'] = old
    else:
        os.environ.pop('SMTP_HOST', None)


@pytest.fixture(autouse=True)
def _reset_ses():
    """Reset SES module state between tests."""
    from ministack.services import ses
    ses.reset()

# ---------------------------------------------------------------------------
# _build_mime_message
# ---------------------------------------------------------------------------

def _parse_mime(msg_str):
    """Parse a MIME message string back for assertion."""
    from email import message_from_string
    return message_from_string(msg_str)


def test_build_mime_text_only():
    from ministack.services.ses import _build_mime_message
    result = _build_mime_message(
        'from@test.com', ['to@test.com'], [], [],
        'Subject', 'body text', '', 'msg-001',
    )
    msg = _parse_mime(result)
    assert msg['Subject'] == 'Subject'
    assert msg['From'] == 'from@test.com'
    assert msg['To'] == 'to@test.com'
    assert msg.get_content_type() == 'text/plain'


def test_build_mime_html_only():
    from ministack.services.ses import _build_mime_message
    result = _build_mime_message(
        'from@test.com', ['to@test.com'], [], [],
        'Subject', '', '<b>html</b>', 'msg-002',
    )
    msg = _parse_mime(result)
    assert msg.get_content_type() == 'text/html'


def test_build_mime_multipart():
    from ministack.services.ses import _build_mime_message
    result = _build_mime_message(
        'from@test.com', ['to@test.com'], ['cc@test.com'], [],
        'Subject', 'text', '<b>html</b>', 'msg-003',
    )
    msg = _parse_mime(result)
    assert msg.get_content_type() == 'multipart/alternative'
    assert msg['Cc'] == 'cc@test.com'

# ---------------------------------------------------------------------------
# _smtp_relay
# ---------------------------------------------------------------------------

def test_ses_smtp_relay_skipped_when_no_host():
    from ministack.services.ses import _smtp_relay
    with patch('ministack.services.ses.smtplib.SMTP') as mock_cls:
        _smtp_relay('from@test.com', ['to@test.com'], 'message')
        mock_cls.assert_not_called()


def test_ses_smtp_relay_sends_when_host_set():
    os.environ['SMTP_HOST'] = '127.0.0.1:1025'
    from ministack.services.ses import _smtp_relay
    mock_smtp = MagicMock()
    with patch('ministack.services.ses.smtplib.SMTP', return_value=mock_smtp) as mock_cls:
        mock_smtp.__enter__ = MagicMock(return_value=mock_smtp)
        mock_smtp.__exit__ = MagicMock(return_value=False)
        _smtp_relay('from@test.com', ['to@test.com'], 'message body')
        mock_cls.assert_called_once_with('127.0.0.1', 1025, timeout=10)
        mock_smtp.sendmail.assert_called_once_with(
            'from@test.com', ['to@test.com'], 'message body',
        )


def test_ses_smtp_relay_does_not_block_the_caller(monkeypatch):
    """An unreachable SMTP_HOST must not stall the request that sent the email."""
    import threading

    from ministack.core.concurrency import spawn_background
    from ministack.services.ses import _smtp_relay
    monkeypatch.setattr('ministack.services.ses.spawn_background', spawn_background)
    os.environ['SMTP_HOST'] = '127.0.0.1:1025'
    release = threading.Event()
    with patch('ministack.services.ses.smtplib.SMTP', side_effect=lambda *a, **k: release.wait(5)):
        start = time.monotonic()
        _smtp_relay('from@test.com', ['to@test.com'], 'message')
        assert time.monotonic() - start < 1
        release.set()


def test_ses_smtp_relay_error_is_logged_not_raised():
    os.environ['SMTP_HOST'] = '127.0.0.1:1025'
    from ministack.services.ses import _smtp_relay
    with patch('ministack.services.ses.smtplib.SMTP', side_effect=ConnectionRefusedError):
        # Should not raise
        _smtp_relay('from@test.com', ['to@test.com'], 'message')


# ---------------------------------------------------------------------------
# SendEmail with SMTP relay
# ---------------------------------------------------------------------------

def _verify_example_com(monkeypatch):
    """Senders must be verified identities, as on AWS."""
    from ministack.services import ses
    monkeypatch.setitem(ses._identities, "example.com", ses._make_identity("example.com", "Domain"))


def test_ses_smtp_relay_send_email(monkeypatch):
    _verify_example_com(monkeypatch)
    monkeypatch.setenv('SMTP_HOST', '127.0.0.1:1025')
    from ministack.services.ses import _send_email
    mock_smtp = MagicMock()
    with patch('ministack.services.ses.smtplib.SMTP', return_value=mock_smtp):
        mock_smtp.__enter__ = MagicMock(return_value=mock_smtp)
        mock_smtp.__exit__ = MagicMock(return_value=False)
        params = {
            'Source': ['sender@example.com'],
            'Destination.ToAddresses.member.1': ['to@example.com'],
            'Destination.CcAddresses.member.1': ['cc@example.com'],
            'Message.Subject.Data': ['Test Subject'],
            'Message.Body.Text.Data': ['Hello'],
            'Message.Body.Html.Data': ['<b>Hello</b>'],
        }
        status, headers, body = _send_email(params)
        assert status == 200
        mock_smtp.sendmail.assert_called_once()
        call_args = mock_smtp.sendmail.call_args
        assert call_args[0][0] == 'sender@example.com'
        assert set(call_args[0][1]) == {'to@example.com', 'cc@example.com'}
        msg = _parse_mime(call_args[0][2])
        assert msg['Subject'] == 'Test Subject'
        assert msg.get_content_type() == 'multipart/alternative'


def test_ses_smtp_relay_send_email_no_relay_without_host(monkeypatch):
    _verify_example_com(monkeypatch)
    from ministack.services.ses import _send_email
    with patch('ministack.services.ses.smtplib.SMTP') as mock_cls:
        params = {
            'Source': ['sender@example.com'],
            'Destination.ToAddresses.member.1': ['to@example.com'],
            'Message.Subject.Data': ['Test'],
            'Message.Body.Text.Data': ['body'],
        }
        status, _, _ = _send_email(params)
        assert status == 200
        mock_cls.assert_not_called()


# ---------------------------------------------------------------------------
# SendRawEmail with SMTP relay
# ---------------------------------------------------------------------------

def test_ses_smtp_relay_send_raw_email(monkeypatch):
    _verify_example_com(monkeypatch)
    monkeypatch.setenv('SMTP_HOST', 'localhost:2525')
    from ministack.services.ses import _send_raw_email
    mock_smtp = MagicMock()
    with patch('ministack.services.ses.smtplib.SMTP', return_value=mock_smtp):
        mock_smtp.__enter__ = MagicMock(return_value=mock_smtp)
        mock_smtp.__exit__ = MagicMock(return_value=False)
        raw_msg = (
            'From: raw@example.com\r\n'
            'To: dest@example.com\r\n'
            'Subject: Raw Test\r\n'
            '\r\n'
            'Raw body'
        )
        params = {
            'Source': ['raw@example.com'],
            'Destinations.member.1': ['dest@example.com'],
            'RawMessage.Data': [raw_msg],
        }
        status, _, _ = _send_raw_email(params)
        assert status == 200
        mock_smtp.sendmail.assert_called_once()
        call_args = mock_smtp.sendmail.call_args
        assert call_args[0][0] == 'raw@example.com'
        assert 'dest@example.com' in call_args[0][1]


def test_ses_smtp_relay_send_raw_email_replaces_existing_message_id(monkeypatch):
    _verify_example_com(monkeypatch)
    monkeypatch.setenv('SMTP_HOST', 'localhost:2525')
    from ministack.services.ses import _send_raw_email
    mock_smtp = MagicMock()
    with patch('ministack.services.ses.smtplib.SMTP', return_value=mock_smtp):
        mock_smtp.__enter__ = MagicMock(return_value=mock_smtp)
        mock_smtp.__exit__ = MagicMock(return_value=False)
        raw_msg = (
            'From: raw@example.com\r\n'
            'Message-ID:\r\n'
            ' <folded-id@example.com>\r\n'
            'To: dest@example.com\r\n'
            'Subject: Raw Test\r\n'
            '\r\n'
            'Message-ID: <in-body@example.com>\r\n'
        )
        params = {
            'Source': ['raw@example.com'],
            'Destinations.member.1': ['dest@example.com'],
            'RawMessage.Data': [raw_msg],
        }
        status, _, _ = _send_raw_email(params)
        assert status == 200
        sent = mock_smtp.sendmail.call_args[0][2]
        header_part, body = sent.split('\r\n\r\n', 1)
        ids = [l for l in header_part.split('\r\n') if l.lower().startswith('message-id:')]
        assert len(ids) == 1
        assert ids[0].endswith('@email.amazonses.com>')
        assert 'folded-id' not in sent
        assert 'To: dest@example.com' in header_part
        assert 'in-body@example.com' in body


# ---------------------------------------------------------------------------
# SendTemplatedEmail with SMTP relay
# ---------------------------------------------------------------------------

def test_ses_smtp_relay_send_templated_email(monkeypatch):
    _verify_example_com(monkeypatch)
    monkeypatch.setenv('SMTP_HOST', 'localhost:1025')
    from ministack.services.ses import _send_templated_email, _templates
    _templates['MyTemplate'] = {
        'TemplateName': 'MyTemplate',
        'SubjectPart': 'Hello {{name}}',
        'TextPart': 'Hi {{name}}',
        'HtmlPart': '<b>Hi {{name}}</b>',
    }
    mock_smtp = MagicMock()
    with patch('ministack.services.ses.smtplib.SMTP', return_value=mock_smtp):
        mock_smtp.__enter__ = MagicMock(return_value=mock_smtp)
        mock_smtp.__exit__ = MagicMock(return_value=False)
        params = {
            'Source': ['tmpl@example.com'],
            'Destination.ToAddresses.member.1': ['to@example.com'],
            'Template': ['MyTemplate'],
            'TemplateData': ['{"name": "World"}'],
        }
        status, _, _ = _send_templated_email(params)
        assert status == 200
        mock_smtp.sendmail.assert_called_once()
        msg = _parse_mime(mock_smtp.sendmail.call_args[0][2])
        assert 'Hello World' in msg['Subject']

# ---------------------------------------------------------------------------
# Endpoint tests for the new /_ministack/ses/messages endpoint
# Verifies acceptance criteria from issue #415:
# - v1 SES: SendEmail, SendRawEmail, SendTemplatedEmail, SendBulkTemplatedEmail
# - v2 SES: SendEmail via sesv2 client
# ---------------------------------------------------------------------------

def test_ses_messages_endpoint_all_v1_send_types(ses):
    """GET /_ministack/ses/messages shows all v1 send operations."""
    import urllib.request
    
    # Prepare template for SendTemplatedEmail and SendBulkTemplatedEmail
    ses.create_template(Template={
        "TemplateName": "test-template",
        "SubjectPart": "Hello {{name}}",
        "TextPart": "Hi {{name}}!",
    })
    
    # Test 1: Verify SendEmail appears
    ses.verify_email_identity(EmailAddress="v1-sender@example.com")
    ses.send_email(
        Source="v1-sender@example.com",
        Destination={"ToAddresses": ["recipient@example.com"]},
        Message={
            "Subject": {"Data": "Test v1 subject"},
            "Body": {"Text": {"Data": "Hello from MiniStack SES v1"}},
        },
    )
    
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    url = f"{endpoint}/_ministack/ses/messages"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=5) as r:
        data = json.loads(r.read().decode())
    
    assert "messages" in data
    send_emails = [m for m in data["messages"]["000000000000"] if m["Type"] == "SendEmail" and m["Source"] == "v1-sender@example.com"]
    assert len(send_emails) >= 1, f"Expected SendEmail, got {[m['Type'] for m in data['messages']['000000000000']]}"
    
    # Test 2: Verify SendRawEmail appears
    ses.verify_email_identity(EmailAddress="raw-sender@example.com")
    raw = (
        "From: raw-sender@example.com\r\nTo: dest@example.com\r\nSubject: Raw Test\r\n\r\nRaw body"
    )
    ses.send_raw_email(RawMessage={"Data": raw})
    
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=5) as r:
        data = json.loads(r.read().decode())
    
    raw_emails = [m for m in data["messages"]["000000000000"] if m["Type"] == "SendRawEmail" and m["Source"] == "raw-sender@example.com"]
    assert len(raw_emails) >= 1, f"Expected SendRawEmail, got {[m['Type'] for m in data['messages']['000000000000']]}"
    
    # Test 3: Verify SendTemplatedEmail appears
    resp = ses.send_templated_email(
        Source="template-sender@example.com",
        Destination={"ToAddresses": ["user@example.com"]},
        Template="test-template",
        TemplateData=json.dumps({"name": "Alice"}),
    )
    
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=5) as r:
        data = json.loads(r.read().decode())
    
    templated_emails = [m for m in data["messages"]["000000000000"] if m["Type"] == "SendTemplatedEmail" and m["Source"] == "template-sender@example.com"]
    assert len(templated_emails) >= 1, f"Expected SendTemplatedEmail, got {[m['Type'] for m in data['messages']['000000000000']]}"
    
    # Test 4: Verify SendBulkTemplatedEmail appears
    resp = ses.send_bulk_templated_email(
        Source="bulk-sender@example.com",
        Template="test-template",
        DefaultTemplateData=json.dumps({"name": "Bob"}),
        Destinations=[
            {
                "Destination": {"ToAddresses": ["user1@example.com"]},
                "ReplacementTemplateData": json.dumps({"name": "Bob"}),
            },
            {
                "Destination": {"ToAddresses": ["user2@example.com"]},
                "ReplacementTemplateData": json.dumps({"name": "Carol"}),
            },
        ],
    )
    
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=5) as r:
        data = json.loads(r.read().decode())
    
    bulk_emails = [m for m in data["messages"]["000000000000"] if m["Type"] == "SendBulkTemplatedEmail" and m["Source"] == "bulk-sender@example.com"]
    assert len(bulk_emails) >= 2, f"Expected SendBulkTemplatedEmail (>=2), got {[m['Type'] for m in data['messages']['000000000000']]}"

def test_ses_messages_endpoint_v2(sesv2):
    """GET /_ministack/ses/messages shows v2 SendEmail via sesv2 client."""
    import urllib.request
    
    sesv2.send_email(
        FromEmailAddress="v2-sender@example.com",
        Destination={"ToAddresses": ["recipient@example.com"]},
        Content={
            "Simple": {
                "Subject": {"Data": "Test v2 subject"},
                "Body": {"Text": {"Data": "Hello from MiniStack SES v2"}},
            }
        },
    )
    
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    url = f"{endpoint}/_ministack/ses/messages"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=5) as r:
        data = json.loads(r.read().decode())
    
    assert "messages" in data
    v2_emails = [m for m in data["messages"]["000000000000"] if m["Type"] == "v2.SendEmail" and m["Source"] == "v2-sender@example.com"]
    assert len(v2_emails) >= 1, f"Expected v2.SendEmail, got {[m['Type'] for m in data['messages']]}"
    assert v2_emails[0]["Subject"] == "Test v2 subject"

def test_ses_messages_endpoint_reset(ses):
    """ Calling POST /_ministack/reset clears stored SES messages. """
    ses.verify_email_identity(EmailAddress="from@example.com")
    ses.send_email(                                                                                                                                                 
        Source="from@example.com",
        Destination={"ToAddresses": ["to@example.com"]},                                                                                                            
        Message={"Subject": {"Data": "Hi"}, "Body": {"Text": {"Data": "body"}}},                                                           
    )                                                                                                                                                               
    import urllib.request
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

    urllib.request.urlopen(                                                                                                                                         
        urllib.request.Request(f"{endpoint}/_ministack/reset", method="POST")                                                                                       
    )                                  
    with urllib.request.urlopen(f"{endpoint}/_ministack/ses/messages") as r:                                                                                        
        data = json.loads(r.read())                                                                                                                                 
    assert data == {"messages": {}}

# ---------------------------------------------------------------------------
# Account filtering test for /_ministack/ses/messages endpoint
#
# Verifies that the ?account query parameter properly validates and filters emails.
# Invalid non-12-digit accounts now return a 400 InvalidAccountID error.
# ---------------------------------------------------------------------------
def _client(service, access_key="test", region="us-east-1"):
    """Create a boto3 client with a specific access key."""
    import boto3
    from botocore.config import Config
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    return boto3.client(
        service,
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key="test",
        region_name=region,
        config=Config(region_name=region, retries={"max_attempts": 0}),
    )

def test_ses_messages_endpoint_account_filter():
    """GET /_ministack/ses/messages?account=X filters by account ID."""
    import urllib.request

    # Clear any existing messages on the running server
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    urllib.request.urlopen(urllib.request.Request(f"{endpoint}/_ministack/reset", method="POST"))

    # Account 1 and Account 2
    ACCOUNT_1 = "011111111111"
    ACCOUNT_2 = "987654321098"
    ses_account_1 = _client("ses", access_key=ACCOUNT_1)
    ses_account_2 = _client("ses", access_key=ACCOUNT_2)

    # Send 2 emails to ACCOUNT_1
    ses_account_1.verify_email_identity(EmailAddress=f"sender-{ACCOUNT_1}@example.com")
    ses_account_1.send_email(
        Source=f"sender-{ACCOUNT_1}@example.com",
        Destination={"ToAddresses": [f"recipient-{ACCOUNT_1}@example.com"]},
        Message={"Subject": {"Data": "Test email 1"}, "Body": {"Text": {"Data": "Body of test email 1"}}},
    )
    ses_account_1.send_email(
        Source=f"sender-{ACCOUNT_1}@example.com",
        Destination={"ToAddresses": [f"recipient-{ACCOUNT_1}@example.com"]},
        Message={"Subject": {"Data": "Test email 2"}, "Body": {"Text": {"Data": "Body of test email 2"}}},
    )

    # Send 1 email to ACCOUNT_2
    ses_account_2.verify_email_identity(EmailAddress=f"sender-{ACCOUNT_2}@example.com")
    ses_account_2.send_email(
        Source=f"sender-{ACCOUNT_2}@example.com",
        Destination={"ToAddresses": [f"recipient-{ACCOUNT_2}@example.com"]},
        Message={"Subject": {"Data": "Test email 3"}, "Body": {"Text": {"Data": "Body of test email 3"}}},
    )

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

    # Test 1: Without ?account: returns all emails
    url = f"{endpoint}/_ministack/ses/messages"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=5) as r:
        data = json.loads(r.read().decode())

    assert "messages" in data
    total = sum(len(messages) for messages in data["messages"].values())
    assert total == 3, f"Expected 3 messages across all accounts, got {total}"

    # Test 2: With invalid non-12-digit account should return error
    url = f"{endpoint}/_ministack/ses/messages?account=notvalid"
    req = urllib.request.Request(url, method="GET")
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req, timeout=5)
    assert exc_info.value.code == 400
    import io
    error_data = json.loads(io.BytesIO(exc_info.value.read()).read())
    assert error_data["__type"] == "InvalidAccountID"
    assert "got: notvalid" in error_data["message"]

    # Test 3: With correct valid custom account (ACCOUNT_1)
    url = f"{endpoint}/_ministack/ses/messages?account={ACCOUNT_1}"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=5) as r:
        data = json.loads(r.read().decode())

    assert "messages" in data
    messages_for_a1 = data["messages"].get(ACCOUNT_1, [])
    assert len(messages_for_a1) == 2, f"Expected 2 messages from ACCOUNT_1, got {len(messages_for_a1)}"

    # Test 4: With correct valid custom account (ACCOUNT_2)
    url = f"{endpoint}/_ministack/ses/messages?account={ACCOUNT_2}"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=5) as r:
        data = json.loads(r.read().decode())

    assert "messages" in data
    messages_for_a2 = data["messages"].get(ACCOUNT_2, [])
    assert len(messages_for_a2) == 1, f"Expected 1 message from ACCOUNT_2, got {len(messages_for_a2)}"

    # Test 5: Empty messages for correct account with no emails sent
    url = f"{endpoint}/_ministack/ses/messages?account=123456789012"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=5) as r:
        data = json.loads(r.read().decode())
    # Should return empty list (no emails for this account)
    assert "messages" in data
    assert data["messages"].get("123456789012", []) == [], "Expected 0 messages for account with no emails"


def test_ses_resources_and_send_statistics_are_region_scoped():
    import urllib.request

    east = _client("ses", region="us-east-1")
    west = _client("ses", region="us-west-2")
    suffix = _uuid_mod.uuid4().hex[:8]
    identity = f"same-region-{suffix}@example.com"
    template = f"same-region-template-{suffix}"
    configuration_set = f"same-region-config-{suffix}"

    for client, label in ((east, "east"), (west, "west")):
        client.verify_email_identity(EmailAddress=identity)
        client.create_template(
            Template={
                "TemplateName": template,
                "SubjectPart": label,
                "TextPart": label,
            }
        )
        client.create_configuration_set(
            ConfigurationSet={"Name": configuration_set}
        )

    assert east.get_template(TemplateName=template)["Template"]["SubjectPart"] == "east"
    assert west.get_template(TemplateName=template)["Template"]["SubjectPart"] == "west"

    east.delete_identity(Identity=identity)
    east.delete_configuration_set(ConfigurationSetName=configuration_set)
    assert identity not in east.list_identities()["Identities"]
    assert identity in west.list_identities()["Identities"]
    assert not any(
        item["Name"] == configuration_set
        for item in east.list_configuration_sets()["ConfigurationSets"]
    )
    assert any(
        item["Name"] == configuration_set
        for item in west.list_configuration_sets()["ConfigurationSets"]
    )

    east_before = east.get_send_quota()["SentLast24Hours"]
    west_before = west.get_send_quota()["SentLast24Hours"]
    for client, label in ((east, "east"), (west, "west")):
        client.verify_email_identity(EmailAddress=f"{label}-{identity}")
        client.send_email(
            Source=f"{label}-{identity}",
            Destination={"ToAddresses": ["recipient@example.com"]},
            Message={
                "Subject": {"Data": label},
                "Body": {"Text": {"Data": label}},
            },
        )

    assert east.get_send_quota()["SentLast24Hours"] == east_before + 1
    assert west.get_send_quota()["SentLast24Hours"] == west_before + 1

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    with urllib.request.urlopen(f"{endpoint}/_ministack/ses/messages") as response:
        messages = json.loads(response.read())["messages"]["000000000000"]
    sources = {message["Source"] for message in messages}
    assert f"east-{identity}" in sources
    assert f"west-{identity}" in sources


def test_ses_v2_resources_and_tags_are_region_scoped():
    east = _client("sesv2", region="us-east-1")
    west = _client("sesv2", region="us-west-2")
    suffix = _uuid_mod.uuid4().hex[:8]
    identity = f"same-v2-region-{suffix}.example.com"
    configuration_set = f"same-v2-region-config-{suffix}"

    for client, label in ((east, "east"), (west, "west")):
        tags = [{"Key": "region", "Value": label}]
        client.create_email_identity(EmailIdentity=identity, Tags=tags)
        client.create_configuration_set(
            ConfigurationSetName=configuration_set,
            Tags=tags,
        )

    assert east.get_email_identity(EmailIdentity=identity)["Tags"] == [
        {"Key": "region", "Value": "east"}
    ]
    assert west.get_email_identity(EmailIdentity=identity)["Tags"] == [
        {"Key": "region", "Value": "west"}
    ]

    for client, region, label in (
        (east, "us-east-1", "east"),
        (west, "us-west-2", "west"),
    ):
        arn = (
            f"arn:aws:ses:{region}:000000000000:"
            f"configuration-set/{configuration_set}"
        )
        assert client.list_tags_for_resource(ResourceArn=arn)["Tags"] == [
            {"Key": "region", "Value": label}
        ]

    east.delete_email_identity(EmailIdentity=identity)
    east.delete_configuration_set(ConfigurationSetName=configuration_set)
    assert not any(
        item["IdentityName"] == identity
        for item in east.list_email_identities()["EmailIdentities"]
    )
    assert any(
        item["IdentityName"] == identity
        for item in west.list_email_identities()["EmailIdentities"]
    )
    assert configuration_set not in east.list_configuration_sets()[
        "ConfigurationSets"
    ]
    assert configuration_set in west.list_configuration_sets()["ConfigurationSets"]


def test_ses_restore_legacy_state_maps_unregionalized_values_to_boot_region():
    from ministack.core.responses import (
        AccountScopedDict,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import ses as service

    account_id = "111111111111"
    boot_region = "us-east-1"
    foreign_region = "us-west-2"
    values = {
        "_identities": (
            "legacy@example.com",
            {
                "VerificationStatus": "Success",
                "NotificationTopics": {
                    "Bounce": "arn:aws:sns:us-west-2:111111111111:legacy"
                },
            },
        ),
        "_templates": (
            "legacy-template",
            {
                "TemplateName": "legacy-template",
                "TextPart": "arn:aws:sns:us-west-2:111111111111:content",
            },
        ),
        "_configuration_sets": (
            "legacy-config",
            {"Name": "legacy-config"},
        ),
    }

    set_request_account_id(account_id)
    set_request_region(boot_region)
    legacy_state = {}
    for state_key, (resource_key, value) in values.items():
        store = AccountScopedDict()
        store[resource_key] = value
        legacy_state[state_key] = store

    service.reset()
    try:
        service.load_persisted_state(legacy_state)
        for state_key, (resource_key, value) in values.items():
            store = getattr(service, state_key)
            assert store.get_scoped(account_id, boot_region, resource_key) == value
            assert store.get_scoped(account_id, foreign_region, resource_key) is None
    finally:
        service.reset()


def test_ses_v2_restore_legacy_state_maps_unregionalized_values_to_boot_region():
    from ministack.core.responses import (
        AccountScopedDict,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import ses_v2 as service

    account_id = "111111111111"
    boot_region = "us-east-1"
    foreign_region = "us-west-2"
    values = {
        "_identities": (
            "legacy.example.com",
            {"EmailIdentity": "legacy.example.com"},
        ),
        "_config_sets": (
            "legacy-config",
            {"ConfigurationSetName": "legacy-config"},
        ),
        "_ses_tags": (
            "arn:aws:ses:us-west-2:111111111111:identity/legacy.example.com",
            [{"Key": "legacy", "Value": "true"}],
        ),
    }

    set_request_account_id(account_id)
    set_request_region(boot_region)
    legacy_state = {}
    for state_key, (resource_key, value) in values.items():
        store = AccountScopedDict()
        store[resource_key] = value
        legacy_state[state_key] = store

    service.reset()
    try:
        service.load_persisted_state(legacy_state)
        for state_key, (resource_key, value) in values.items():
            store = getattr(service, state_key)
            expected_key = resource_key
            if state_key == "_ses_tags":
                expected_key = resource_key.replace(
                    f":{foreign_region}:", f":{boot_region}:"
                )
                assert store.get_scoped(account_id, boot_region, resource_key) is None
            assert store.get_scoped(account_id, boot_region, expected_key) == value
            assert store.get_scoped(account_id, foreign_region, resource_key) is None
    finally:
        service.reset()


def _ses_clients(region):
    import boto3
    from botocore.config import Config

    kw = dict(endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
              aws_access_key_id="test", aws_secret_access_key="test", region_name=region,
              config=Config(retries={"mode": "standard"}))
    return boto3.client("ses", **kw), boto3.client("sesv2", **kw)


def test_ses_sandbox_account_only_sends_to_verified_or_simulator_recipients():
    # Account state is per region; a region of its own keeps other tests in production.
    v1, v2 = _ses_clients("ap-southeast-4")
    s = _uuid_mod.uuid4().hex[:8]
    sender, verified = f"sender-{s}.example.com", f"ok-{s}@example.com"
    v2.create_email_identity(EmailIdentity=sender)
    v2.create_email_identity(EmailIdentity=verified)
    assert v2.get_account()["ProductionAccessEnabled"] is True
    content = {"Simple": {"Subject": {"Data": "s"}, "Body": {"Text": {"Data": "b"}}}}
    try:
        v2.put_account_details(MailType="TRANSACTIONAL", WebsiteURL="https://example.com",
                               ProductionAccessEnabled=False)
        account = v2.get_account()
        assert account["ProductionAccessEnabled"] is False
        assert account["Details"]["MailType"] == "TRANSACTIONAL"

        def v2_send(to):
            return v2.send_email(FromEmailAddress=f"app@{sender}",
                                 Destination={"ToAddresses": [to]}, Content=content)

        v2_send(verified)
        v2_send("success@simulator.amazonses.com")
        unverified = f"nobody-{s}@unverified.example.invalid"
        message = ("Email address is not verified. The following identities failed the check "
                   f"in region AP-SOUTHEAST-4: {unverified}")
        with pytest.raises(ClientError) as exc:
            v2_send(unverified)
        assert exc.value.response["Error"]["Code"] == "MessageRejected"
        assert exc.value.response["Error"]["Message"] == message
        with pytest.raises(ClientError) as exc:
            v1.send_email(Source=f"app@{sender}", Destination={"ToAddresses": [unverified]},
                          Message={"Subject": {"Data": "s"}, "Body": {"Text": {"Data": "b"}}})
        assert exc.value.response["Error"]["Code"] == "MessageRejected"

        tpl = f"tpl-{s}"
        v2.create_email_template(TemplateName=tpl, TemplateContent={"Subject": "s", "Text": "b"})
        results = v2.send_bulk_email(
            FromEmailAddress=f"app@{sender}",
            DefaultContent={"Template": {"TemplateName": tpl, "TemplateData": "{}"}},
            BulkEmailEntries=[{"Destination": {"ToAddresses": [verified]}},
                              {"Destination": {"ToAddresses": [unverified]}}])["BulkEmailEntryResults"]
        assert [r["Status"] for r in results] == ["SUCCESS", "MESSAGE_REJECTED"]
        assert results[1]["Error"] == message

        v2.put_account_details(MailType="TRANSACTIONAL", WebsiteURL="https://example.com",
                               ProductionAccessEnabled=True)
        v2_send(unverified)
    finally:
        v2.put_account_details(MailType="TRANSACTIONAL", WebsiteURL="https://example.com",
                               ProductionAccessEnabled=True)


def test_ses_send_from_an_unverified_sender_is_rejected():
    v1, v2 = _ses_clients("ap-southeast-5")
    s = _uuid_mod.uuid4().hex[:8]
    domain, address = f"verified-{s}.example.com", f"Person-{s}@other.example.org"
    v2.create_email_identity(EmailIdentity=domain)
    v1.verify_email_identity(EmailAddress=address)
    content = {"Simple": {"Subject": {"Data": "s"}, "Body": {"Text": {"Data": "b"}}}}

    def send(sender):
        return v2.send_email(FromEmailAddress=sender, Destination={"ToAddresses": ["to@example.com"]},
                             Content=content)

    send(f"app@{domain}")
    send(f"app@mail.{domain}")  # a verified domain covers its subdomains
    send(address)
    unverified = f"person-{s}@other.example.org"  # address identities are case-sensitive
    for sender in (unverified, f"app@unverified-{s}.example.invalid"):
        with pytest.raises(ClientError) as exc:
            send(sender)
        assert exc.value.response["Error"]["Code"] == "MessageRejected"
        assert exc.value.response["Error"]["Message"] == (
            "Email address is not verified. The following identities failed the check "
            f"in region AP-SOUTHEAST-5: {sender}")
    with pytest.raises(ClientError) as exc:
        v1.send_email(Source=unverified, Destination={"ToAddresses": ["to@example.com"]},
                      Message={"Subject": {"Data": "s"}, "Body": {"Text": {"Data": "b"}}})
    assert exc.value.response["Error"]["Code"] == "MessageRejected"


# ── Receipt rules ──
@pytest.fixture
def receipt_api(monkeypatch):
    # Private stores keep in-process reset/persistence tests independent of the
    # running integration server and other services' fixtures on this worker.
    for name in (
        "_identities", "_sent_emails", "_templates", "_configuration_sets",
        "_account_details", "_receipt_rule_sets", "_active_receipt_rule_set",
    ):
        monkeypatch.setattr(_ses_svc, name, AccountRegionScopedDict())
    model = Session().get_service_model("ses")
    # Exercise service-side required-field errors instead of SDK validation.
    serializer = create_serializer("query", include_validation=False)
    parser = create_parser("query")

    def call(action, **params):
        operation = model.operation_model(action)
        request = serializer.serialize_to_request(params, operation)
        status, headers, body = asyncio.run(_ses_svc.handle_request(
            "POST", "/", {}, urlencode(request["body"]).encode(), {}
        ))
        result = parser.parse(
            {"status_code": status, "headers": headers, "body": body},
            operation.output_shape,
        )
        if status >= 400:
            raise ClientError(result, action)
        assert status == 200
        return {key: value for key, value in result.items() if key != "ResponseMetadata"}

    return call


def assert_error(api, action, code, message, **params):
    with pytest.raises(ClientError) as exc:
        api(action, **params)
    response = exc.value.response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert response["Error"] == {"Code": code, "Message": message, "Type": "Sender"}


def test_receipt_rule_set_and_rule_lifecycle(receipt_api):
    api = receipt_api
    assert api("DescribeActiveReceiptRuleSet") == {}
    api("CreateReceiptRuleSet", RuleSetName="set.with-periods")
    empty = api("DescribeReceiptRuleSet", RuleSetName="set.with-periods")
    assert empty["Metadata"]["Name"] == "set.with-periods"
    assert isinstance(empty["Metadata"]["CreatedTimestamp"], datetime)
    assert empty["Rules"] == []
    assert api("ListReceiptRuleSets")["RuleSets"] == [empty["Metadata"]]

    api("CreateReceiptRule", RuleSetName="set.with-periods", Rule={"Name": "first"})
    assert api("DescribeReceiptRule", RuleSetName="set.with-periods", RuleName="first")["Rule"] == {
        "Name": "first", "Enabled": False, "ScanEnabled": False,
        "TlsPolicy": "Optional", "Actions": [],
    }
    rule = {
        "Name": "second", "Enabled": True, "ScanEnabled": True,
        "TlsPolicy": "Require", "Recipients": ["EXAMPLE.COM", "User+tag@EXAMPLE.ORG", ".EXAMPLE.COM", "foo.example-test.com"],
        "Actions": [
            {"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": "value<&"}},
            {"StopAction": {"Scope": "RuleSet"}},
        ],
    }
    api("CreateReceiptRule", RuleSetName="set.with-periods", Rule=rule)
    rule["Recipients"] = [".example.com", "example.com", "foo.example-test.com", "user+tag@example.org"]
    assert api("DescribeReceiptRule", RuleSetName="set.with-periods", RuleName="second")["Rule"] == rule
    api("CreateReceiptRule", RuleSetName="set.with-periods", After="second", Rule={"Name": "third"})
    described = api("DescribeReceiptRuleSet", RuleSetName="set.with-periods")
    assert [r["Name"] for r in described["Rules"]] == ["second", "third", "first"]
    api("SetActiveReceiptRuleSet", RuleSetName="set.with-periods")
    assert api("DescribeActiveReceiptRuleSet") == described
    assert_error(api, "DeleteReceiptRuleSet", "CannotDelete",
                 "Cannot delete active rule set: set.with-periods", RuleSetName="set.with-periods")
    api("DeleteReceiptRule", RuleSetName="set.with-periods", RuleName="third")
    assert [r["Name"] for r in api("DescribeActiveReceiptRuleSet")["Rules"]] == ["second", "first"]
    api("SetActiveReceiptRuleSet")
    assert api("DescribeActiveReceiptRuleSet") == {}
    api("DeleteReceiptRuleSet", RuleSetName="set.with-periods")
    api("DeleteReceiptRuleSet", RuleSetName="set.with-periods")
    assert api("ListReceiptRuleSets") == {"RuleSets": []}


def test_receipt_duplicate_and_missing_resource_errors_leave_order_unchanged(receipt_api):
    api = receipt_api
    for action, params in (
        ("DescribeReceiptRuleSet", {}), ("SetActiveReceiptRuleSet", {}),
        ("CreateReceiptRule", {"Rule": {"Name": "first"}}),
        ("DescribeReceiptRule", {"RuleName": "first"}),
        ("DeleteReceiptRule", {"RuleName": "first"}),
    ):
        assert_error(api, action, "RuleSetDoesNotExist", "Rule set does not exist: missing",
                     RuleSetName="missing", **params)
    api("CreateReceiptRuleSet", RuleSetName="rules")
    assert_error(api, "CreateReceiptRuleSet", "AlreadyExists", "Rule set already exists: rules",
                 RuleSetName="rules")
    api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "first"})
    assert_error(api, "CreateReceiptRule", "AlreadyExists", "Rule already exists: first",
                 RuleSetName="rules", Rule={"Name": "first"}, After="missing")
    assert_error(api, "CreateReceiptRule", "RuleDoesNotExist", "Rule does not exist: missing",
                 RuleSetName="rules", Rule={"Name": "second"}, After="missing")
    assert_error(api, "DescribeReceiptRule", "RuleDoesNotExist", "Rule does not exist: missing",
                 RuleSetName="rules", RuleName="missing")
    api("DeleteReceiptRule", RuleSetName="rules", RuleName="missing")
    assert [r["Name"] for r in api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"]] == ["first"]
    assert_error(api, "ListReceiptRuleSets", "InvalidParameterValue", "Invalid token: invalid", NextToken="invalid")


@pytest.mark.parametrize("name", ["-bad", "bad-", "a" * 65])
def test_receipt_invalid_names(receipt_api, name):
    api = receipt_api
    assert_error(api, "CreateReceiptRuleSet", "InvalidParameterValue",
                 f"Not a valid ruleSetName: {name}", RuleSetName=name)
    # AWS validates rule metadata before looking up the parent rule set.
    assert_error(api, "CreateReceiptRule", "InvalidParameterValue",
                 f"Not a valid ruleName: {name}", RuleSetName="missing", Rule={"Name": name})
    assert api("ListReceiptRuleSets") == {"RuleSets": []}


@pytest.mark.parametrize("name", ["-bad", "bad-", "a" * 65])
@pytest.mark.parametrize("action,params,field", [
    ("DescribeReceiptRuleSet", {}, "ruleSetName"),
    ("DeleteReceiptRuleSet", {}, "ruleSetName"),
    ("SetActiveReceiptRuleSet", {}, "ruleSetName"),
    ("CreateReceiptRule", {"Rule": {"Name": "valid"}}, "ruleSetName"),
    ("DescribeReceiptRule", {"RuleName": "valid"}, "ruleSetName"),
    ("DeleteReceiptRule", {"RuleName": "valid"}, "ruleSetName"),
    ("DescribeReceiptRule", {"RuleSetName": "rules"}, "ruleName"),
    ("DeleteReceiptRule", {"RuleSetName": "rules"}, "ruleName"),
])
def test_receipt_lookup_delete_and_activation_validate_names(receipt_api, name, action, params, field):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "valid"})
    api("SetActiveReceiptRuleSet", RuleSetName="rules")
    before = api("DescribeActiveReceiptRuleSet")
    key = "RuleSetName" if field == "ruleSetName" else "RuleName"
    assert_error(api, action, "InvalidParameterValue",
                 f"Not a valid {field}: {name}", **{**params, key: name})
    assert api("DescribeActiveReceiptRuleSet") == before
    assert api("ListReceiptRuleSets")["RuleSets"] == [before["Metadata"]]


@pytest.mark.parametrize("action,params", [
    ("CreateReceiptRuleSet", {}),
    ("DescribeReceiptRuleSet", {}),
    ("DeleteReceiptRuleSet", {}),
    ("CreateReceiptRule", {"Rule": {"Name": "valid"}}),
    ("DescribeReceiptRule", {"RuleName": "valid"}),
    ("DeleteReceiptRule", {"RuleName": "valid"}),
])
def test_receipt_missing_rule_set_name_error(receipt_api, action, params):
    assert_error(receipt_api, action, "ValidationError",
                 "1 validation error detected: Value at 'ruleSetName' failed to satisfy constraint: Member must not be null",
                 **params)
    assert receipt_api("ListReceiptRuleSets") == {"RuleSets": []}


@pytest.mark.parametrize("action,params,field", [
    ("CreateReceiptRule", {}, "rule"),
    ("CreateReceiptRule", {"Rule": {}}, "rule"),
    ("CreateReceiptRule", {"Rule": {"Enabled": True}}, "rule.name"),
    ("DescribeReceiptRule", {}, "ruleName"),
    ("DeleteReceiptRule", {}, "ruleName"),
])
def test_receipt_missing_rule_fields_error(receipt_api, action, params, field):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    assert_error(api, action, "ValidationError",
                 f"1 validation error detected: Value at '{field}' failed to satisfy constraint: Member must not be null",
                 RuleSetName="rules", **params)
    assert api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"] == []


@pytest.mark.parametrize("recipients", [
    ["", "first@example.com", "second@example.com"],
    ["first@example.com", "", "second@example.com"],
    ["first@example.com", "second@example.com", ""],
])
def test_receipt_empty_recipient_is_rejected_without_creating_a_rule(receipt_api, recipients):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    assert_error(api, "CreateReceiptRule", "InvalidParameterValue", "Invalid recipient: ",
                 RuleSetName="rules", Rule={"Name": "invalid", "Recipients": recipients})
    assert api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"] == []


@pytest.mark.parametrize("value", ["a\tb", "café 雪 😀 <&>"])
def test_receipt_header_tab_and_unicode_round_trip(receipt_api, value):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    actions = [{"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": value}}]
    api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "header", "Actions": actions})
    api("SetActiveReceiptRuleSet", RuleSetName="rules")
    assert api("DescribeReceiptRule", RuleSetName="rules", RuleName="header")["Rule"]["Actions"] == actions
    assert api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"][0]["Actions"] == actions
    assert api("DescribeActiveReceiptRuleSet")["Rules"][0]["Actions"] == actions


def test_receipt_nul_header_is_created_but_readback_fails_until_deleted(receipt_api):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "valid"})
    api("SetActiveReceiptRuleSet", RuleSetName="rules")
    before = api("DescribeActiveReceiptRuleSet")
    assert api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "broken", "Actions": [
        {"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": "a\x00b"}},
    ]}) == {}
    for action, params in (
        ("DescribeReceiptRule", {"RuleSetName": "rules", "RuleName": "broken"}),
        ("DescribeReceiptRuleSet", {"RuleSetName": "rules"}),
        ("DescribeActiveReceiptRuleSet", {}),
    ):
        with pytest.raises(ClientError) as exc:
            api(action, **params)
        response = exc.value.response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 500
        assert response["Error"] == {"Type": "Receiver", "Code": "InternalFailure"}
    assert api("DescribeReceiptRule", RuleSetName="rules", RuleName="valid")["Rule"] == before["Rules"][0]
    assert api("ListReceiptRuleSets")["RuleSets"] == [before["Metadata"]]
    api("DeleteReceiptRule", RuleSetName="rules", RuleName="broken")
    assert api("DescribeReceiptRuleSet", RuleSetName="rules") == before
    assert api("DescribeActiveReceiptRuleSet") == before


@pytest.mark.parametrize("rule,code,message", [
    ({"Name": "tls", "TlsPolicy": "Invalid"}, "ValidationError",
     "1 validation error detected: Value at 'rule.tlsPolicy' failed to satisfy constraint: Member must satisfy enum value set: [Optional, Require]"),
    ({"Name": "recipient", "Recipients": ["not valid"]}, "InvalidParameterValue", "Invalid recipient: not valid"),
    ({"Name": "scope", "Actions": [{"StopAction": {"Scope": "Invalid"}}]}, "ValidationError",
     "1 validation error detected: Value at 'rule.actions.1.member.stopAction.scope' failed to satisfy constraint: Member must satisfy enum value set: [RuleSet]"),
    ({"Name": "ordering", "Actions": [{"StopAction": {"Scope": "RuleSet"}},
      {"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": "ok"}}]}, "InvalidParameterValue",
     "Stop action, if any, must be placed at the end of the actions list"),
    ({"Name": "multiple", "Actions": [{"StopAction": {"Scope": "RuleSet"},
      "AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": "ok"}}]}, "InvalidParameterValue",
     "Exactly one action type must be specified for each ReceiptAction"),
    ({"Name": "header", "Actions": [{"AddHeaderAction": {"HeaderName": "X bad", "HeaderValue": "ok"}}]},
     "InvalidParameterValue", "Invalid header name: X bad"),
    ({"Name": "newline", "Actions": [{"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": "a\nb"}}]},
     "InvalidParameterValue", "Invalid header value: a0x000ab"),
])
def test_receipt_invalid_rule_metadata_is_atomic(receipt_api, rule, code, message):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    assert_error(api, "CreateReceiptRule", code, message, RuleSetName="rules", Rule=rule)
    assert api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"] == []


@pytest.mark.parametrize("header,missing", [
    ({"HeaderName": "X-Test"}, "headerValue"),
    ({"HeaderValue": "value"}, "headerName"),
])
def test_receipt_missing_header_fields_are_rejected_without_creating_a_rule(receipt_api, header, missing):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    assert_error(api, "CreateReceiptRule", "ValidationError",
                 f"1 validation error detected: Value at 'rule.actions.1.member.addHeaderAction.{missing}' failed to satisfy constraint: Member must not be null",
                 RuleSetName="rules", Rule={"Name": "missing", "Actions": [{"AddHeaderAction": header}]})
    assert api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"] == []


@pytest.mark.parametrize("recipient", [
    "example..com", "example.-com", "example-.com", "foo..example.com",
    "foo.-example.com", "foo.example-.com", "example.com-",
])
def test_receipt_invalid_domain_labels_are_rejected_without_creating_a_rule(receipt_api, recipient):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    assert_error(api, "CreateReceiptRule", "InvalidParameterValue", f"Invalid recipient: {recipient}",
                 RuleSetName="rules", Rule={"Name": "invalid", "Recipients": [recipient]})
    assert api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"] == []


def test_receipt_recipients_are_lowercased_deduplicated_and_sorted(receipt_api):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "recipients", "Recipients": [
        "z@EXAMPLE.COM", "A@Example.com", "a@example.com", "EXAMPLE.COM",
        "example.com", ".EXAMPLE.COM", ".example.com",
    ]})
    recipients = [".example.com", "a@example.com", "example.com", "z@example.com"]
    assert api("DescribeReceiptRule", RuleSetName="rules", RuleName="recipients")["Rule"]["Recipients"] == recipients
    assert api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"][0]["Recipients"] == recipients


@pytest.mark.parametrize("action", [
    {"S3Action": {"BucketName": "bucket"}},
    {"SNSAction": {"TopicArn": "arn:aws:sns:us-east-1:111111111111:topic"}},
    {"LambdaAction": {"FunctionArn": "arn:aws:lambda:us-east-1:111111111111:function:test"}},
    {"BounceAction": {"SmtpReplyCode": "550", "Message": "no", "Sender": "sender@example.com"}},
    {"WorkmailAction": {"OrganizationArn": "arn:aws:workmail:us-east-1:111111111111:organization/m-test"}},
    {"ConnectAction": {"InstanceARN": "arn:aws:connect:us-east-1:111111111111:instance/test",
                       "IAMRoleARN": "arn:aws:iam::111111111111:role/test"}},
    {"StopAction": {"Scope": "RuleSet", "TopicArn": "arn:aws:sns:us-east-1:111111111111:topic"}},
])
def test_receipt_every_action_type_is_stored(receipt_api, action):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "rule", "Actions": [action]})
    assert api("DescribeReceiptRule", RuleSetName="rules", RuleName="rule")["Rule"]["Actions"] == [action]


def test_receipt_explicit_empty_fields_are_preserved_and_after_is_validated(receipt_api):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "empty", "Recipients": [], "Actions": [{}]})
    assert api("DescribeReceiptRule", RuleSetName="rules", RuleName="empty")["Rule"]["Recipients"] == []
    for name, action in (("empty-stop", {"StopAction": {}}), ("empty-header", {"AddHeaderAction": {}})):
        api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": name, "Actions": [action]})
        assert api("DescribeReceiptRule", RuleSetName="rules", RuleName=name)["Rule"]["Actions"] == []
    api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "header", "Actions": [
        {"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": ""}},
    ]})
    assert api("DescribeReceiptRule", RuleSetName="rules", RuleName="header")["Rule"]["Actions"] == [
        {"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": ""}},
    ]
    assert_error(api, "CreateReceiptRule", "ValidationError",
                 "2 validation errors detected: Value at 'after' failed to satisfy constraint: Member must have length greater than or equal to 1; Value at 'after' failed to satisfy constraint: Member must satisfy regular expression pattern: ^[a-zA-Z0-9_.-]+$",
                 RuleSetName="rules", After="", Rule={"Name": "after"})


def test_receipt_metadata_and_active_selection_survive_all_tenant_persistence_and_reset(receipt_api, monkeypatch, tmp_path):
    api = receipt_api
    scopes = [("111111111111", "us-east-1"), ("111111111111", "us-west-2"), ("222222222222", "us-east-1")]
    expected = {}
    for i, (account, region) in enumerate(scopes):
        set_request_account_id(account)
        set_request_region(region)
        assert api("ListReceiptRuleSets") == {"RuleSets": []}
        api("CreateReceiptRuleSet", RuleSetName="same-name")
        api("CreateReceiptRule", RuleSetName="same-name", Rule={"Name": f"rule-{i}"})
        api("SetActiveReceiptRuleSet", RuleSetName="same-name")
        expected[account, region] = api("DescribeActiveReceiptRuleSet")
    snapshot = _ses_svc.get_state()
    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))
    persistence.save_state("ses", snapshot)
    _ses_svc.reset()
    for account, region in scopes:
        set_request_account_id(account)
        set_request_region(region)
        assert api("ListReceiptRuleSets") == {"RuleSets": []}
        assert api("DescribeActiveReceiptRuleSet") == {}
    _ses_svc.load_persisted_state(persistence.load_state("ses"))
    for account, region in scopes:
        set_request_account_id(account)
        set_request_region(region)
        assert api("DescribeActiveReceiptRuleSet") == expected[account, region]
    api("SetActiveReceiptRuleSet")
    api("DeleteReceiptRuleSet", RuleSetName="same-name")
    set_request_account_id(scopes[0][0])
    set_request_region(scopes[0][1])
    assert api("DescribeActiveReceiptRuleSet") == expected[scopes[0]]
    assert snapshot["_receipt_rule_sets"].get_scoped(*scopes[-1], "same-name")["Rules"]


@pytest.mark.serial
def test_receipt_unsigned_sdk_lifecycle():
    # The endpoint comes from conftest so this uses the same isolated local
    # server as the integration suite. Activation changes shared server state.
    from conftest import make_client

    client = make_client("ses", additional_config_kwargs={"signature_version": UNSIGNED})
    name = f"unsigned-receipts-{uuid4().hex[:12]}"
    previous = client.describe_active_receipt_rule_set().get("Metadata", {}).get("Name")
    client.create_receipt_rule_set(RuleSetName=name)
    try:
        assert name in {item["Name"] for item in client.list_receipt_rule_sets()["RuleSets"]}
        client.create_receipt_rule(RuleSetName=name, Rule={"Name": "first"})
        client.create_receipt_rule(RuleSetName=name, After="first", Rule={"Name": "second", "Actions": [
            {"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": "value<&"}},
            {"StopAction": {"Scope": "RuleSet"}},
        ]})
        described = client.describe_receipt_rule_set(RuleSetName=name)
        assert [rule["Name"] for rule in described["Rules"]] == ["first", "second"]
        assert client.describe_receipt_rule(RuleSetName=name, RuleName="second")["Rule"] == described["Rules"][1]
        client.set_active_receipt_rule_set(RuleSetName=name)
        active = client.describe_active_receipt_rule_set()
        assert active["Metadata"] == described["Metadata"]
        assert active["Rules"] == described["Rules"]
        client.delete_receipt_rule(RuleSetName=name, RuleName="second")
        assert [rule["Name"] for rule in client.describe_active_receipt_rule_set()["Rules"]] == ["first"]
    finally:
        client.set_active_receipt_rule_set(**({"RuleSetName": previous} if previous else {}))
        client.delete_receipt_rule_set(RuleSetName=name)
    assert name not in {item["Name"] for item in client.list_receipt_rule_sets()["RuleSets"]}
@pytest.fixture
def ses_notify_queue(ses, sns, sqs):
    """An SNS topic subscribed by an SQS queue, plus a `drain(n)` collector of the
    notifications received so far."""
    from conftest import sqs_policy_allow_sns

    suffix = _uuid_mod.uuid4().hex[:8]
    topic_arn = sns.create_topic(Name=f"ses-notify-{suffix}")["TopicArn"]
    queue_url = sqs.create_queue(QueueName=f"ses-notify-{suffix}")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={
        "Policy": json.dumps(sqs_policy_allow_sns(queue_arn, topic_arn))})
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
    identities = []

    def bind(identity):
        identities.append(identity)
        for kind in ("Delivery", "Bounce", "Complaint"):
            ses.set_identity_notification_topic(
                Identity=identity, NotificationType=kind, SnsTopic=topic_arn)

    def drain(expected):
        got = []
        for _ in range(10):
            for m in sqs.receive_message(
                    QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages", []):
                env = json.loads(m["Body"])
                assert env["Type"] == "Notification"
                got.append(json.loads(env["Message"]))
            if len(got) >= expected:
                break
        return got

    yield suffix, bind, drain
    for identity in identities:
        ses.delete_identity(Identity=identity)
    sqs.delete_queue(QueueUrl=queue_url)
    sns.delete_topic(TopicArn=topic_arn)


_NOTIFY_MSG = {"Subject": {"Data": "s"}, "Body": {"Text": {"Data": "b"}}}


def test_ses_identity_notifications_published_to_sns_sqs(ses, ses_notify_queue):
    suffix, bind, drain = ses_notify_queue
    sender = f"notify-{suffix}@example.com"
    ses.verify_email_identity(EmailAddress=sender)
    bind(sender)

    dest = ["ok@example.com", "bounce@simulator.amazonses.com",
            "complaint@simulator.amazonses.com", "suppressionlist@simulator.amazonses.com"]
    msg_id = ses.send_email(
        Source=f'"Notify" <{sender}>', Destination={"ToAddresses": dest}, Message=_NOTIFY_MSG,
    )["MessageId"]

    got = drain(5)
    by_kind = {}
    for n in got:
        by_kind.setdefault(n["notificationType"], []).append(n)
    # complaint@ is delivered before it is marked as spam; suppressionlist@ is a suppressed hard bounce.
    assert sorted(r for n in by_kind["Delivery"] for r in n["delivery"]["recipients"]) == [dest[2], dest[0]]
    bounces = {n["bounce"]["bouncedRecipients"][0]["emailAddress"]: n["bounce"] for n in by_kind["Bounce"]}
    assert (bounces[dest[1]]["bounceType"], bounces[dest[1]]["bounceSubType"]) == ("Permanent", "General")
    assert (bounces[dest[3]]["bounceType"], bounces[dest[3]]["bounceSubType"]) == ("Permanent", "Suppressed")
    assert [n["complaint"]["complainedRecipients"][0]["emailAddress"] for n in by_kind["Complaint"]] == [dest[2]]
    mail = got[0]["mail"]
    assert mail["messageId"] == msg_id
    assert mail["source"] == sender
    assert mail["sourceArn"].endswith(f":identity/{sender}")
    assert mail["destination"] == dest


def test_ses_identity_notifications_fall_back_to_domain_identity(ses, ses_notify_queue):
    suffix, bind, drain = ses_notify_queue
    domain = f"notify-{suffix}.example.com"
    ses.verify_domain_identity(Domain=domain)
    bind(domain)

    msg_id = ses.send_email(
        Source=f"anyone@sub.{domain}", Destination={"ToAddresses": ["ok@example.com"]},
        Message=_NOTIFY_MSG,
    )["MessageId"]

    got = drain(1)
    assert [n["mail"]["messageId"] for n in got if n["notificationType"] == "Delivery"] == [msg_id]
    assert got[0]["mail"]["sourceArn"].endswith(f":identity/{domain}")


def test_ses_identity_notifications_simulator_subaddress(ses, ses_notify_queue):
    suffix, bind, drain = ses_notify_queue
    sender = f"notify-{suffix}@example.com"
    ses.verify_email_identity(EmailAddress=sender)
    bind(sender)

    ses.send_email(
        Source=sender, Destination={"ToAddresses": ["bounce+label@simulator.amazonses.com"]},
        Message=_NOTIFY_MSG,
    )

    got = drain(1)
    assert [n["notificationType"] for n in got] == ["Bounce"]


def test_ses_identity_notifications_other_send_paths(ses, ses_notify_queue):
    suffix, bind, drain = ses_notify_queue
    sender = f"notify-{suffix}@example.com"
    ses.verify_email_identity(EmailAddress=sender)
    bind(sender)
    tpl = f"notify-tpl-{suffix}"
    ses.create_template(Template={"TemplateName": tpl, "SubjectPart": "s", "TextPart": "t"})
    raw = f"From: {sender}\r\nTo: ok@example.com\r\nSubject: s\r\n\r\nb"
    try:
        ids = {
            ses.send_raw_email(RawMessage={"Data": raw})["MessageId"],
            ses.send_templated_email(
                Source=sender, Destination={"ToAddresses": ["ok@example.com"]},
                Template=tpl, TemplateData="{}")["MessageId"],
        }
        bulk = ses.send_bulk_templated_email(
            Source=sender, Template=tpl, DefaultTemplateData="{}",
            Destinations=[{"Destination": {"ToAddresses": ["ok@example.com"]}}],
        )
        ids.update(s["MessageId"] for s in bulk["Status"])

        got = drain(len(ids))
        assert {n["mail"]["messageId"] for n in got} == ids
        assert {n["notificationType"] for n in got} == {"Delivery"}
    finally:
        ses.delete_template(TemplateName=tpl)


def test_ses_identity_notifications_not_published_without_topic(ses, sns, ses_notify_queue):
    suffix, bind, drain = ses_notify_queue
    sender = f"notify-{suffix}@example.com"
    ses.verify_email_identity(EmailAddress=sender)
    bind(sender)
    for kind in ("Delivery", "Bounce", "Complaint"):
        ses.set_identity_notification_topic(Identity=sender, NotificationType=kind)

    ses.send_email(
        Source=sender, Destination={"ToAddresses": ["ok@example.com"]}, Message=_NOTIFY_MSG)

    assert drain(1) == []
