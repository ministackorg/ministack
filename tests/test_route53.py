import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


def test_route53_create_and_get_hosted_zone(r53):
    resp = r53.create_hosted_zone(
        Name="example.com",
        CallerReference="ref-create-1",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
    hz = resp["HostedZone"]
    zone_id = hz["Id"].split("/")[-1]
    assert hz["Name"] == "example.com."
    assert "DelegationSet" in resp
    assert len(resp["DelegationSet"]["NameServers"]) == 4

    get_resp = r53.get_hosted_zone(Id=zone_id)
    assert get_resp["HostedZone"]["Name"] == "example.com."
    assert get_resp["HostedZone"]["ResourceRecordSetCount"] == 2  # SOA + NS

def test_route53_create_zone_idempotency(r53):
    r53.create_hosted_zone(Name="idempotent.com", CallerReference="ref-idem-1")
    resp2 = r53.create_hosted_zone(Name="idempotent.com", CallerReference="ref-idem-1")
    # Same CallerReference → same zone returned, not a new one
    assert resp2["HostedZone"]["Name"] == "idempotent.com."

def test_route53_list_hosted_zones(r53):
    r53.create_hosted_zone(Name="list-test.com", CallerReference="ref-list-1")
    resp = r53.list_hosted_zones()
    names = [hz["Name"] for hz in resp["HostedZones"]]
    assert "list-test.com." in names

def test_route53_list_hosted_zones_by_name(r53):
    r53.create_hosted_zone(Name="byname-alpha.com", CallerReference="ref-bn-1")
    r53.create_hosted_zone(Name="byname-beta.com", CallerReference="ref-bn-2")
    resp = r53.list_hosted_zones_by_name(DNSName="byname-alpha.com")
    assert resp["HostedZones"][0]["Name"] == "byname-alpha.com."

def test_route53_delete_hosted_zone(r53):
    resp = r53.create_hosted_zone(Name="delete-me.com", CallerReference="ref-del-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    # Must remove non-default records first (none here, just SOA+NS which are auto-removed)
    r53.delete_hosted_zone(Id=zone_id)

    import botocore.exceptions

    with pytest.raises(botocore.exceptions.ClientError) as exc:
        r53.get_hosted_zone(Id=zone_id)
    assert exc.value.response["Error"]["Code"] == "NoSuchHostedZone"

def test_route53_change_resource_record_sets_create(r53):
    resp = r53.create_hosted_zone(Name="records.com", CallerReference="ref-rrs-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    change_resp = r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "www.records.com",
                        "Type": "A",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "1.2.3.4"}],
                    },
                }
            ]
        },
    )
    assert change_resp["ChangeInfo"]["Status"] == "INSYNC"

def test_route53_list_resource_record_sets(r53):
    resp = r53.create_hosted_zone(Name="listrrs.com", CallerReference="ref-lrrs-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "mail.listrrs.com",
                        "Type": "MX",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "10 mail.example.com."}],
                    },
                }
            ]
        },
    )
    list_resp = r53.list_resource_record_sets(HostedZoneId=zone_id)
    types = [rrs["Type"] for rrs in list_resp["ResourceRecordSets"]]
    assert "MX" in types
    assert "SOA" in types
    assert "NS" in types

def test_route53_list_resource_record_sets_start_name_uses_reversed_label_order(r53):
    parent = r53.create_hosted_zone(
        Name="parent-zone.com", CallerReference="ref-parent-zone"
    )
    parent_zone_id = parent["HostedZone"]["Id"].split("/")[-1]

    child = r53.create_hosted_zone(
        Name="child.parent-zone.com",
        CallerReference="ref-child-zone",
    )
    child_zone_id = child["HostedZone"]["Id"].split("/")[-1]

    child_ns = [
        rrs
        for rrs in r53.list_resource_record_sets(HostedZoneId=child_zone_id)["ResourceRecordSets"]
        if rrs["Name"] == "child.parent-zone.com."
        and rrs["Type"] == "NS"
    ][0]

    r53.change_resource_record_sets(
        HostedZoneId=parent_zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "child.parent-zone.com",
                        "Type": "NS",
                        "TTL": child_ns["TTL"],
                        "ResourceRecords": child_ns["ResourceRecords"],
                    },
                }
            ]
        },
    )

    list_resp = r53.list_resource_record_sets(
        HostedZoneId=parent_zone_id,
        StartRecordName="child.parent-zone.com.",
        StartRecordType="NS",
    )
    returned = list_resp["ResourceRecordSets"]

    assert returned[0]["Name"] == "child.parent-zone.com."
    assert returned[0]["Type"] == "NS"
    assert all(
        not (rrs["Name"] == "parent-zone.com." and rrs["Type"] == "NS")
        for rrs in returned
    )

def test_route53_list_resource_record_sets_truncated_next_record_uses_next_page_start(r53):
    resp = r53.create_hosted_zone(
        Name="pagination-zone.com", CallerReference="ref-next-record"
    )
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "token.pagination-zone.com",
                        "Type": "TXT",
                        "TTL": 60,
                        "ResourceRecords": [{"Value": '"target.pagination-zone.com"'}],
                    },
                },
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "zz-next.pagination-zone.com",
                        "Type": "NS",
                        "TTL": 120,
                        "ResourceRecords": [
                            {"Value": "ns-1.example.com."},
                            {"Value": "ns-2.example.com."},
                            {"Value": "ns-3.example.com."},
                            {"Value": "ns-4.example.com."},
                        ],
                    },
                }
            ]
        },
    )

    list_resp = r53.list_resource_record_sets(
        HostedZoneId=zone_id,
        StartRecordName="token.pagination-zone.com.",
        StartRecordType="TXT",
        MaxItems="1",
    )

    assert list_resp["ResourceRecordSets"][0]["Name"] == "token.pagination-zone.com."
    assert list_resp["ResourceRecordSets"][0]["Type"] == "TXT"
    assert list_resp["IsTruncated"] is True
    assert list_resp["NextRecordName"] == "zz-next.pagination-zone.com."
    assert list_resp["NextRecordType"] == "NS"

def test_route53_list_resource_record_sets_pagination_advances_with_next_record_cursor(r53):
    resp = r53.create_hosted_zone(
        Name="cursor-zone.com", CallerReference="ref-cursor-pagination"
    )
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "token.cursor-zone.com",
                        "Type": "TXT",
                        "TTL": 60,
                        "ResourceRecords": [{"Value": '"target.cursor-zone.com"'}],
                    },
                },
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "zz-next.cursor-zone.com",
                        "Type": "NS",
                        "TTL": 120,
                        "ResourceRecords": [
                            {"Value": "ns-1.example.com."},
                            {"Value": "ns-2.example.com."},
                            {"Value": "ns-3.example.com."},
                            {"Value": "ns-4.example.com."},
                        ],
                    },
                },
            ]
        },
    )

    first_page = r53.list_resource_record_sets(
        HostedZoneId=zone_id,
        StartRecordName="token.cursor-zone.com.",
        StartRecordType="TXT",
        MaxItems="1",
    )

    assert first_page["ResourceRecordSets"][0]["Name"] == "token.cursor-zone.com."
    assert first_page["ResourceRecordSets"][0]["Type"] == "TXT"
    assert first_page["IsTruncated"] is True

    second_page = r53.list_resource_record_sets(
        HostedZoneId=zone_id,
        StartRecordName=first_page["NextRecordName"],
        StartRecordType=first_page["NextRecordType"],
        MaxItems="1",
    )

    assert second_page["ResourceRecordSets"][0]["Name"] == "zz-next.cursor-zone.com."
    assert second_page["ResourceRecordSets"][0]["Type"] == "NS"
    assert second_page["ResourceRecordSets"][0]["Name"] != first_page["ResourceRecordSets"][0]["Name"]
    assert second_page["IsTruncated"] is False

def test_route53_upsert_record(r53):
    resp = r53.create_hosted_zone(Name="upsert.com", CallerReference="ref-ups-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    for ip in ("1.1.1.1", "2.2.2.2"):
        r53.change_resource_record_sets(
            HostedZoneId=zone_id,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": "www.upsert.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": ip}],
                        },
                    }
                ]
            },
        )

    list_resp = r53.list_resource_record_sets(HostedZoneId=zone_id)
    a_records = [rrs for rrs in list_resp["ResourceRecordSets"] if rrs["Type"] == "A"]
    assert len(a_records) == 1
    assert a_records[0]["ResourceRecords"][0]["Value"] == "2.2.2.2"

def test_route53_delete_record(r53):
    resp = r53.create_hosted_zone(Name="delrec.com", CallerReference="ref-dr-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "www.delrec.com",
                        "Type": "A",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "5.5.5.5"}],
                    },
                }
            ]
        },
    )

    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "DELETE",
                    "ResourceRecordSet": {
                        "Name": "www.delrec.com",
                        "Type": "A",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "5.5.5.5"}],
                    },
                }
            ]
        },
    )

    list_resp = r53.list_resource_record_sets(HostedZoneId=zone_id)
    a_records = [rrs for rrs in list_resp["ResourceRecordSets"] if rrs["Type"] == "A"]
    assert len(a_records) == 0

def test_route53_get_change(r53):
    resp = r53.create_hosted_zone(Name="change-status.com", CallerReference="ref-cs-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    change_resp = r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "a.change-status.com",
                        "Type": "A",
                        "TTL": 60,
                        "ResourceRecords": [{"Value": "9.9.9.9"}],
                    },
                }
            ]
        },
    )
    change_id = change_resp["ChangeInfo"]["Id"].split("/")[-1]
    get_change = r53.get_change(Id=change_id)
    assert get_change["ChangeInfo"]["Status"] == "INSYNC"

def test_route53_create_health_check(r53):
    resp = r53.create_health_check(
        CallerReference="ref-hc-1",
        HealthCheckConfig={
            "IPAddress": "1.2.3.4",
            "Port": 80,
            "Type": "HTTP",
            "ResourcePath": "/health",
            "RequestInterval": 30,
            "FailureThreshold": 3,
        },
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
    hc = resp["HealthCheck"]
    hc_id = hc["Id"]
    assert hc["HealthCheckConfig"]["Type"] == "HTTP"

    get_resp = r53.get_health_check(HealthCheckId=hc_id)
    assert get_resp["HealthCheck"]["Id"] == hc_id

def test_route53_list_health_checks(r53):
    r53.create_health_check(
        CallerReference="ref-hcl-1",
        HealthCheckConfig={"IPAddress": "2.2.2.2", "Port": 443, "Type": "HTTPS"},
    )
    resp = r53.list_health_checks()
    assert len(resp["HealthChecks"]) >= 1

def test_route53_delete_health_check(r53):
    resp = r53.create_health_check(
        CallerReference="ref-hcd-1",
        HealthCheckConfig={"IPAddress": "3.3.3.3", "Port": 80, "Type": "HTTP"},
    )
    hc_id = resp["HealthCheck"]["Id"]
    r53.delete_health_check(HealthCheckId=hc_id)

    import botocore.exceptions

    with pytest.raises(botocore.exceptions.ClientError) as exc:
        r53.get_health_check(HealthCheckId=hc_id)
    assert exc.value.response["Error"]["Code"] == "NoSuchHealthCheck"

def test_route53_tags_for_hosted_zone(r53):
    resp = r53.create_hosted_zone(Name="tagged.com", CallerReference="ref-tag-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    r53.change_tags_for_resource(
        ResourceType="hostedzone",
        ResourceId=zone_id,
        AddTags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "infra"}],
    )

    tags_resp = r53.list_tags_for_resource(ResourceType="hostedzone", ResourceId=zone_id)
    tags = {t["Key"]: t["Value"] for t in tags_resp["ResourceTagSet"]["Tags"]}
    assert tags["env"] == "test"
    assert tags["team"] == "infra"

    r53.change_tags_for_resource(
        ResourceType="hostedzone",
        ResourceId=zone_id,
        RemoveTagKeys=["team"],
    )
    tags_resp2 = r53.list_tags_for_resource(ResourceType="hostedzone", ResourceId=zone_id)
    keys2 = [t["Key"] for t in tags_resp2["ResourceTagSet"]["Tags"]]
    assert "env" in keys2
    assert "team" not in keys2

def test_route53_no_such_hosted_zone(r53):
    import botocore.exceptions

    with pytest.raises(botocore.exceptions.ClientError) as exc:
        r53.get_hosted_zone(Id="ZNOTEXIST1234")
    assert exc.value.response["Error"]["Code"] == "NoSuchHostedZone"

def test_route53_alias_record(r53):
    resp = r53.create_hosted_zone(Name="alias.com", CallerReference="ref-alias-1")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]

    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "www.alias.com",
                        "Type": "A",
                        "AliasTarget": {
                            "HostedZoneId": "Z2FDTNDATAQYW2",
                            "DNSName": "d1234.cloudfront.net",
                            "EvaluateTargetHealth": False,
                        },
                    },
                }
            ]
        },
    )

    list_resp = r53.list_resource_record_sets(HostedZoneId=zone_id)
    alias_recs = [rrs for rrs in list_resp["ResourceRecordSets"] if rrs["Type"] == "A" and "AliasTarget" in rrs]
    assert len(alias_recs) == 1
    assert alias_recs[0]["AliasTarget"]["DNSName"] == "d1234.cloudfront.net."

# Migrated from test_r53.py
def test_route53_delete_zone_with_records_fails(r53):
    """DeleteHostedZone fails if non-default records exist."""
    zone_id = r53.create_hosted_zone(
        Name="qa-r53-nonempty.com.",
        CallerReference=f"qa-nonempty-{int(time.time())}",
    )["HostedZone"]["Id"].split("/")[-1]
    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "www.qa-r53-nonempty.com.",
                        "Type": "A",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "1.2.3.4"}],
                    },
                }
            ]
        },
    )
    with pytest.raises(ClientError) as exc:
        r53.delete_hosted_zone(Id=zone_id)
    assert exc.value.response["Error"]["Code"] == "HostedZoneNotEmpty"

def test_route53_upsert_is_idempotent(r53):
    """UPSERT on existing record updates it without error."""
    zone_id = r53.create_hosted_zone(
        Name="qa-r53-upsert.com.",
        CallerReference=f"qa-upsert-{int(time.time())}",
    )["HostedZone"]["Id"].split("/")[-1]
    for ip in ["1.1.1.1", "2.2.2.2"]:
        r53.change_resource_record_sets(
            HostedZoneId=zone_id,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": "api.qa-r53-upsert.com.",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": ip}],
                        },
                    }
                ]
            },
        )
    records = r53.list_resource_record_sets(HostedZoneId=zone_id)["ResourceRecordSets"]
    a_records = [r for r in records if r["Name"] == "api.qa-r53-upsert.com." and r["Type"] == "A"]
    assert len(a_records) == 1
    assert a_records[0]["ResourceRecords"][0]["Value"] == "2.2.2.2"

def test_route53_create_record_duplicate_fails(r53):
    """CREATE on existing record raises InvalidChangeBatch."""
    zone_id = r53.create_hosted_zone(
        Name="qa-r53-dup.com.",
        CallerReference=f"qa-dup-{int(time.time())}",
    )["HostedZone"]["Id"].split("/")[-1]
    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": "dup.qa-r53-dup.com.",
                        "Type": "A",
                        "TTL": 60,
                        "ResourceRecords": [{"Value": "1.1.1.1"}],
                    },
                }
            ]
        },
    )
    with pytest.raises(ClientError) as exc:
        r53.change_resource_record_sets(
            HostedZoneId=zone_id,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "dup.qa-r53-dup.com.",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "2.2.2.2"}],
                        },
                    }
                ]
            },
        )
    assert exc.value.response["Error"]["Code"] == "InvalidChangeBatch"

