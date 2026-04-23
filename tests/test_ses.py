"""SES tests — API operations + SMTP relay integration."""

import io
import base64
import json
import os
import pytest
import time
import uuid as _uuid_mod
import zipfile
from botocore.exceptions import ClientError
from urllib.parse import urlparse
from unittest.mock import MagicMock, patch



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
    assert resp["MessageId"].startswith("ministack-")

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

def test_ses_v2_get_account(sesv2):
    resp = sesv2.get_account()
    assert resp["SendingEnabled"] is True
    assert resp["ProductionAccessEnabled"] is True

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
        mock_cls.assert_called_once_with('127.0.0.1', 1025)
        mock_smtp.sendmail.assert_called_once_with(
            'from@test.com', ['to@test.com'], 'message body',
        )


def test_ses_smtp_relay_error_is_logged_not_raised():
    os.environ['SMTP_HOST'] = '127.0.0.1:1025'
    from ministack.services.ses import _smtp_relay
    with patch('ministack.services.ses.smtplib.SMTP', side_effect=ConnectionRefusedError):
        # Should not raise
        _smtp_relay('from@test.com', ['to@test.com'], 'message')


# ---------------------------------------------------------------------------
# SendEmail with SMTP relay
# ---------------------------------------------------------------------------

def test_ses_smtp_relay_send_email(monkeypatch):
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


def test_ses_smtp_relay_send_email_no_relay_without_host():
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


# ---------------------------------------------------------------------------
# SendTemplatedEmail with SMTP relay
# ---------------------------------------------------------------------------

def test_ses_smtp_relay_send_templated_email(monkeypatch):
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
def _client(service, access_key="test"):
    """Create a boto3 client with a specific access key."""
    import boto3
    from botocore.config import Config
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    region = "us-east-1"
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
  