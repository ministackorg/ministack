import os
import time
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError
from conftest import ENDPOINT, make_client


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
    # ChangeResourceRecordSets returns PENDING on submission (real Route 53).
    assert change_resp["ChangeInfo"]["Status"] == "PENDING"

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

def _create_zone_with_a_record(r53, zone_name, values, ttl=300):
    resp = r53.create_hosted_zone(Name=zone_name, CallerReference=f"ref-{zone_name}")
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]
    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "CREATE",
                    "ResourceRecordSet": {
                        "Name": f"www.{zone_name}",
                        "Type": "A",
                        "TTL": ttl,
                        "ResourceRecords": [{"Value": v} for v in values],
                    },
                }
            ]
        },
    )
    return zone_id

def _a_records(r53, zone_id):
    list_resp = r53.list_resource_record_sets(HostedZoneId=zone_id)
    return [rrs for rrs in list_resp["ResourceRecordSets"] if rrs["Type"] == "A"]

def test_route53_delete_record_rejects_value_mismatch(r53):
    # Real Route 53 requires DELETE to specify the record's current values
    # exactly and rejects the whole batch otherwise:
    # https://docs.aws.amazon.com/Route53/latest/APIReference/API_ChangeResourceRecordSets.html
    zone_id = _create_zone_with_a_record(r53, "delmismatch.com", ["5.5.5.5"])

    with pytest.raises(ClientError) as exc:
        r53.change_resource_record_sets(
            HostedZoneId=zone_id,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "other.delmismatch.com",
                            "Type": "A",
                            "TTL": 300,
                            "ResourceRecords": [{"Value": "7.7.7.7"}],
                        },
                    },
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": "www.delmismatch.com",
                            "Type": "A",
                            "TTL": 300,
                            "ResourceRecords": [{"Value": "6.6.6.6"}],
                        },
                    },
                ]
            },
        )
    assert exc.value.response["Error"]["Code"] == "InvalidChangeBatch"
    assert (
        "Tried to delete resource record set [name='www.delmismatch.com.', type='A'] "
        "but the values provided do not match the current values"
    ) in exc.value.response["Error"]["Message"]

    # The batch is atomic: the record survives and the CREATE was not applied.
    remaining = _a_records(r53, zone_id)
    assert len(remaining) == 1
    assert remaining[0]["Name"] == "www.delmismatch.com."
    assert remaining[0]["ResourceRecords"] == [{"Value": "5.5.5.5"}]

def test_route53_delete_record_rejects_ttl_mismatch(r53):
    zone_id = _create_zone_with_a_record(r53, "delttl.com", ["5.5.5.5"], ttl=300)

    with pytest.raises(ClientError) as exc:
        r53.change_resource_record_sets(
            HostedZoneId=zone_id,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "DELETE",
                        "ResourceRecordSet": {
                            "Name": "www.delttl.com",
                            "Type": "A",
                            "TTL": 60,
                            "ResourceRecords": [{"Value": "5.5.5.5"}],
                        },
                    }
                ]
            },
        )
    assert exc.value.response["Error"]["Code"] == "InvalidChangeBatch"
    assert (
        "Tried to delete resource record set [name='www.delttl.com.', type='A'] "
        "but the values provided do not match the current values"
    ) in exc.value.response["Error"]["Message"]
    assert len(_a_records(r53, zone_id)) == 1

def test_route53_delete_record_accepts_reordered_values(r53):
    # Route 53 compares resource record values as a set, so a DELETE listing
    # the same values in a different order must match.
    zone_id = _create_zone_with_a_record(r53, "delorder.com", ["5.5.5.5", "6.6.6.6"])

    r53.change_resource_record_sets(
        HostedZoneId=zone_id,
        ChangeBatch={
            "Changes": [
                {
                    "Action": "DELETE",
                    "ResourceRecordSet": {
                        "Name": "www.delorder.com",
                        "Type": "A",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": "6.6.6.6"}, {"Value": "5.5.5.5"}],
                    },
                }
            ]
        },
    )
    assert len(_a_records(r53, zone_id)) == 0

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
    # Route 53 is born PENDING and flips to INSYNC once propagated. We emulate
    # that lazily: the first GetChange sees PENDING, subsequent reads INSYNC —
    # so a poll loop / SDK waiter actually exercises its retry path.
    assert change_resp["ChangeInfo"]["Status"] == "PENDING"
    change_id = change_resp["ChangeInfo"]["Id"].split("/")[-1]
    assert r53.get_change(Id=change_id)["ChangeInfo"]["Status"] == "PENDING"
    assert r53.get_change(Id=change_id)["ChangeInfo"]["Status"] == "INSYNC"

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

def test_route53_health_check_interval_defaults_only_for_endpoint_checks(r53, cw):
    # Endpoint checks get RequestInterval 30 / FailureThreshold 3 when omitted;
    # CLOUDWATCH_METRIC and CALCULATED checks return neither field (Terraform
    # would otherwise see request_interval drift and replace them).
    cw.put_metric_alarm(
        AlarmName="hc-default-alarm", MetricName="Invocations", Namespace="AWS/Lambda", Statistic="Sum",
        Period=60, EvaluationPeriods=1, Threshold=1, ComparisonOperator="GreaterThanThreshold",
    )
    configs = {
        "HTTP": {"Type": "HTTP", "FullyQualifiedDomainName": "example.com", "Port": 80, "ResourcePath": "/"},
        "HTTPS": {"Type": "HTTPS", "FullyQualifiedDomainName": "example.com", "Port": 443, "ResourcePath": "/"},
        "HTTP_STR_MATCH": {
            "Type": "HTTP_STR_MATCH", "FullyQualifiedDomainName": "example.com", "Port": 80,
            "ResourcePath": "/", "SearchString": "ok",
        },
        "HTTPS_STR_MATCH": {
            "Type": "HTTPS_STR_MATCH", "FullyQualifiedDomainName": "example.com", "Port": 443,
            "ResourcePath": "/", "SearchString": "ok",
        },
        "TCP": {"Type": "TCP", "FullyQualifiedDomainName": "example.com", "Port": 443},
        "CLOUDWATCH_METRIC": {
            "Type": "CLOUDWATCH_METRIC",
            "AlarmIdentifier": {"Region": "us-east-1", "Name": "hc-default-alarm"},
            "InsufficientDataHealthStatus": "LastKnownStatus",
        },
        "CALCULATED": {"Type": "CALCULATED", "ChildHealthChecks": [], "HealthThreshold": 0},
    }
    ids = {}
    for t, cfg in configs.items():
        created = r53.create_health_check(CallerReference=str(uuid.uuid4()), HealthCheckConfig=cfg)
        ids[t] = created["HealthCheck"]["Id"]
        for got in (created["HealthCheck"], r53.get_health_check(HealthCheckId=ids[t])["HealthCheck"]):
            c = got["HealthCheckConfig"]
            if t not in ("CLOUDWATCH_METRIC", "CALCULATED"):
                assert c["RequestInterval"] == 30
                assert c["FailureThreshold"] == 3
            else:
                assert "RequestInterval" not in c
                assert "FailureThreshold" not in c
    listed = {
        h["Id"]: h["HealthCheckConfig"]
        for page in r53.get_paginator("list_health_checks").paginate()
        for h in page["HealthChecks"]
    }
    assert "RequestInterval" not in listed[ids["CLOUDWATCH_METRIC"]]
    assert "RequestInterval" not in listed[ids["CALCULATED"]]
    updated = r53.update_health_check(HealthCheckId=ids["CLOUDWATCH_METRIC"], InsufficientDataHealthStatus="Unhealthy")
    assert "RequestInterval" not in updated["HealthCheck"]["HealthCheckConfig"]
    for hc_id in ids.values():
        r53.delete_health_check(HealthCheckId=hc_id)
    cw.delete_alarms(AlarmNames=["hc-default-alarm"])

def test_route53_metric_and_calculated_health_check_validation(r53, cw):
    cw.put_metric_alarm(
        AlarmName="hc-validation-alarm", MetricName="Invocations", Namespace="AWS/Lambda", Statistic="Sum",
        Period=60, EvaluationPeriods=1, Threshold=1, ComparisonOperator="GreaterThanThreshold",
    )
    cw.put_composite_alarm(AlarmName="hc-validation-composite", AlarmRule='ALARM("hc-validation-alarm")')
    alarm = {"Region": "us-east-1", "Name": "hc-validation-alarm"}
    missing = {"Region": "us-east-1", "Name": "hc-validation-missing"}
    cw_cfg = {"Type": "CLOUDWATCH_METRIC", "AlarmIdentifier": alarm, "InsufficientDataHealthStatus": "LastKnownStatus"}
    calc_cfg = {"Type": "CALCULATED", "ChildHealthChecks": [], "HealthThreshold": 0}
    period = "Invalid parameter : {} health checks must not have a period specified."
    threshold = "Invalid parameter : {} health checks must not have a failure threshold specified."
    not_found = "The specified CloudWatch alarm doesn't exist. Name: {}, region: {}"

    def create_error(cfg):
        with pytest.raises(ClientError) as exc:
            r53.create_health_check(CallerReference=str(uuid.uuid4()), HealthCheckConfig=cfg)
        err = exc.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert err["Error"]["Code"] == "InvalidInput"
        return err["Error"]["Message"]

    # The alarm is checked first, then the period, then the failure threshold.
    assert create_error({**cw_cfg, "AlarmIdentifier": missing, "RequestInterval": 30}) == not_found.format(
        "hc-validation-missing", "us-east-1"
    )
    assert create_error({**cw_cfg, "AlarmIdentifier": {"Region": "us-west-2", "Name": "hc-validation-alarm"}}) == (
        not_found.format("hc-validation-alarm", "us-west-2")
    )
    assert create_error({**cw_cfg, "AlarmIdentifier": {"Region": "us-east-1", "Name": "hc-validation-composite"}}) == (
        not_found.format("hc-validation-composite", "us-east-1")
    )
    assert create_error({**cw_cfg, "RequestInterval": 10, "FailureThreshold": 3}) == period.format("Metric-based")
    assert create_error({**cw_cfg, "FailureThreshold": 3}) == threshold.format("Metric-based")
    assert create_error({**calc_cfg, "RequestInterval": 30, "FailureThreshold": 3}) == period.format("Calculated")
    assert create_error({**calc_cfg, "FailureThreshold": 1}) == threshold.format("Calculated")
    assert create_error({"Type": "CLOUDWATCH_METRIC", "RequestInterval": 30}) == "Alarm identifier must not be null."
    rc_cfg = {
        "Type": "RECOVERY_CONTROL",
        "RoutingControlArn": "arn:aws:route53-recovery-control::000000000000:controlpanel/"
        "c14b2e806664476aae6db2edd214cc6e/routingcontrol/a7011654a2034f07",
    }
    assert create_error({"Type": "RECOVERY_CONTROL", "RequestInterval": 30}) == "Routing control ARN must not be null"
    assert create_error({"Type": "RECOVERY_CONTROL", "FailureThreshold": 3}) == "Routing control ARN must not be null"
    # AWS words the recovery-control errors as calculated-check errors. AWS checks
    # that the routing control exists before these; MiniStack has no Route 53 ARC
    # service to check against, so any ARN passes here.
    assert create_error({**rc_cfg, "RequestInterval": 10, "FailureThreshold": 1}) == period.format("Calculated")
    assert create_error({**rc_cfg, "FailureThreshold": 3}) == threshold.format("Calculated")
    rc = r53.create_health_check(CallerReference=str(uuid.uuid4()), HealthCheckConfig=rc_cfg)["HealthCheck"]
    assert "RequestInterval" not in rc["HealthCheckConfig"]
    assert "FailureThreshold" not in rc["HealthCheckConfig"]
    for kwargs in ({}, {"FailureThreshold": 5}, {"Inverted": True}, {"Disabled": True}):
        with pytest.raises(ClientError) as exc:
            r53.update_health_check(HealthCheckId=rc["Id"], **kwargs)
        assert exc.value.response["Error"]["Code"] == "InvalidInput"
        assert exc.value.response["Error"]["Message"] == (
            "Invalid parameter : Update is not supported for this health check type."
        )
    rc_after = r53.get_health_check(HealthCheckId=rc["Id"])["HealthCheck"]["HealthCheckConfig"]
    assert "FailureThreshold" not in rc_after
    assert not rc_after.get("Inverted")
    r53.delete_health_check(HealthCheckId=rc["Id"])

    cw_ref = str(uuid.uuid4())
    cw_id = r53.create_health_check(CallerReference=cw_ref, HealthCheckConfig=cw_cfg)["HealthCheck"]["Id"]
    with pytest.raises(ClientError) as exc:
        r53.create_health_check(CallerReference=cw_ref, HealthCheckConfig={**cw_cfg, "RequestInterval": 30})
    assert exc.value.response["Error"]["Message"] == period.format("Metric-based")
    calc_id = r53.create_health_check(CallerReference=str(uuid.uuid4()), HealthCheckConfig=calc_cfg)["HealthCheck"]["Id"]
    for hc_id, kwargs, message in (
        (cw_id, {"AlarmIdentifier": missing, "FailureThreshold": 3}, not_found.format("hc-validation-missing", "us-east-1")),
        (cw_id, {"FailureThreshold": 3}, threshold.format("Metric-based")),
        (calc_id, {"FailureThreshold": 5}, threshold.format("Calculated")),
    ):
        with pytest.raises(ClientError) as exc:
            r53.update_health_check(HealthCheckId=hc_id, **kwargs)
        assert exc.value.response["Error"]["Code"] == "InvalidInput"
        assert exc.value.response["Error"]["Message"] == message
    # An update with no members (empty body) succeeds and bumps the version.
    assert r53.update_health_check(HealthCheckId=cw_id)["HealthCheck"]["HealthCheckVersion"] == 2
    assert r53.update_health_check(HealthCheckId=calc_id)["HealthCheck"]["HealthCheckVersion"] == 2
    cw.put_metric_alarm(
        AlarmName="hc-validation-alarm-2", MetricName="Invocations", Namespace="AWS/Lambda", Statistic="Sum",
        Period=60, EvaluationPeriods=1, Threshold=1, ComparisonOperator="GreaterThanThreshold",
    )
    moved = r53.update_health_check(HealthCheckId=cw_id, AlarmIdentifier={"Region": "us-east-1", "Name": "hc-validation-alarm-2"})
    assert moved["HealthCheck"]["HealthCheckConfig"]["AlarmIdentifier"]["Name"] == "hc-validation-alarm-2"
    r53.update_health_check(HealthCheckId=cw_id, AlarmIdentifier=alarm)
    cw.delete_alarms(AlarmNames=["hc-validation-alarm-2"])
    cfg = r53.get_health_check(HealthCheckId=cw_id)["HealthCheck"]["HealthCheckConfig"]
    assert cfg["AlarmIdentifier"]["Name"] == "hc-validation-alarm"
    assert "FailureThreshold" not in cfg
    assert "FailureThreshold" not in r53.get_health_check(HealthCheckId=calc_id)["HealthCheck"]["HealthCheckConfig"]
    for hc_id in (cw_id, calc_id):
        r53.delete_health_check(HealthCheckId=hc_id)
    cw.delete_alarms(AlarmNames=["hc-validation-composite"])
    cw.delete_alarms(AlarmNames=["hc-validation-alarm"])

def test_route53_update_health_check_malformed_body(r53):
    import urllib.error
    import urllib.request

    hc_id = r53.create_health_check(
        CallerReference=str(uuid.uuid4()),
        HealthCheckConfig={"Type": "HTTP", "FullyQualifiedDomainName": "example.com", "Port": 80, "ResourcePath": "/"},
    )["HealthCheck"]["Id"]
    for target in (hc_id, str(uuid.uuid4())):
        for body in (b" ", b"abc"):
            req = urllib.request.Request(f"{ENDPOINT}/2013-04-01/healthcheck/{target}", data=body, method="POST")
            with pytest.raises(urllib.error.HTTPError) as exc:
                urllib.request.urlopen(req)
            assert exc.value.code == 400
            text = exc.value.read().decode()
            assert "<Code>MalformedInput</Code>" in text
            assert "<Message>Could not parse XML</Message>" in text
    r53.delete_health_check(HealthCheckId=hc_id)

def _hc_error(call, **kwargs):
    with pytest.raises(ClientError) as exc:
        call(**kwargs)
    err = exc.value.response
    assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert err["Error"]["Code"] == "InvalidInput"
    return err["Error"]["Message"]


def _peel(call, fields, expected):
    """Send all fields, then drop the one AWS reported, in AWS's reported order."""
    fields = dict(fields)
    for field, message in expected:
        assert _hc_error(call, **fields) == message
        del fields[field]
    return call(**fields)


def test_route53_health_check_field_rules(r53, cw):
    for name, extra in (("hc-rules-std", {"Statistic": "Sum", "Period": 60}), ("hc-rules-std2", {"Statistic": "Sum", "Period": 120}),
                        ("hc-rules-ext", {"ExtendedStatistic": "p90", "Period": 60}), ("hc-rules-p30", {"Statistic": "Sum", "Period": 30}),
                        ("hc-rules-p10", {"Statistic": "Sum", "Period": 10})):
        cw.put_metric_alarm(AlarmName=name, MetricName="Invocations", Namespace="AWS/Lambda", EvaluationPeriods=1,
                            Threshold=1, ComparisonOperator="GreaterThanThreshold", **extra)
    cw.put_metric_alarm(AlarmName="hc-rules-math", EvaluationPeriods=1, Threshold=1, ComparisonOperator="GreaterThanThreshold", Metrics=[
        {"Id": "m1", "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": "Invocations"}, "Period": 60, "Stat": "Sum"},
         "ReturnData": False},
        {"Id": "e1", "Expression": "m1 * 2", "Label": "double", "ReturnData": True}])
    alarm = {"Region": "us-east-1", "Name": "hc-rules-std"}
    ghosts = [str(uuid.uuid4()), str(uuid.uuid4())]
    endpoint = {"IPAddress": "1.2.3.4", "Port": 80, "FullyQualifiedDomainName": "example.com", "ResourcePath": "/",
                "SearchString": "ok", "Regions": ["us-east-1", "us-west-1", "eu-west-1"], "EnableSNI": True}
    created = []

    def create(**cfg):
        hc_id = r53.create_health_check(CallerReference=str(uuid.uuid4()), HealthCheckConfig=cfg)["HealthCheck"]["Id"]
        created.append(hc_id)
        return hc_id

    def create_error(**cfg):
        return _hc_error(r53.create_health_check, CallerReference=str(uuid.uuid4()), HealthCheckConfig=cfg)

    def peel_create(cfg, expected):
        return _peel(lambda **c: created.append(
            r53.create_health_check(CallerReference=str(uuid.uuid4()), HealthCheckConfig=c)["HealthCheck"]["Id"]
        ), cfg, expected)

    def msg(kind, label):
        return f"Invalid parameter : {kind} health checks must not have {label} specified."

    metric_order = [("IPAddress", "an IP address"), ("Port", "a port"), ("ResourcePath", "a resource path"),
                    ("SearchString", "a search string"), ("RequestInterval", "a period"), ("FailureThreshold", "a failure threshold"),
                    ("Regions", "regions"), ("MeasureLatency", "a latency measurement"), ("FullyQualifiedDomainName", "a FQDN"),
                    ("HealthThreshold", "a health threshold"), ("ChildHealthChecks", "child health checks"), ("EnableSNI", "enable SNI")]
    calc_order = [("IPAddress", "an IP address"), ("Port", "a port"), ("ResourcePath", "a resource path"),
                  ("SearchString", "a search string"), ("RequestInterval", "a period"), ("FailureThreshold", "a failure threshold"),
                  ("Regions", "regions"), ("MeasureLatency", "a latency measurement"), ("FullyQualifiedDomainName", "a FQDN"),
                  ("EnableSNI", "enable SNI"), ("AlarmIdentifier", "an metric region"),
                  ("InsufficientDataHealthStatus", "an insufficient data health status")]
    everything = dict(endpoint, MeasureLatency=True, RequestInterval=30, FailureThreshold=3)

    # Metric checks: the alarm comes before every field rule.
    cw_cfg = {"Type": "CLOUDWATCH_METRIC", "AlarmIdentifier": alarm, "InsufficientDataHealthStatus": "LastKnownStatus"}
    assert create_error(Type="CLOUDWATCH_METRIC", IPAddress="1.2.3.4") == "Alarm identifier must not be null."
    assert create_error(**{**cw_cfg, "AlarmIdentifier": {"Region": "us-east-1", "Name": "hc-rules-missing"},
                           "IPAddress": "1.2.3.4"}) == (
        "The specified CloudWatch alarm doesn't exist. Name: hc-rules-missing, region: us-east-1")
    extended = "Route 53 does not support extended CloudWatch metric statistics"
    high_res = "Route 53 does not support high-resolution CloudWatch alarms"
    for name, message in (("hc-rules-ext", extended), ("hc-rules-math", extended), ("hc-rules-p30", high_res), ("hc-rules-p10", high_res)):
        assert create_error(**{**cw_cfg, "AlarmIdentifier": {"Region": "us-east-1", "Name": name}, "IPAddress": "1.2.3.4"}) == message
    create(**{**cw_cfg, "AlarmIdentifier": {"Region": "us-east-1", "Name": "hc-rules-std2"}})
    # A metric check without InsufficientDataHealthStatus gets LastKnownStatus.
    no_status = create(Type="CLOUDWATCH_METRIC", AlarmIdentifier=alarm)
    assert r53.get_health_check(HealthCheckId=no_status)["HealthCheck"]["HealthCheckConfig"][
        "InsufficientDataHealthStatus"] == "LastKnownStatus"
    # An empty child list is accepted on a metric check and not returned.
    no_children = create(**cw_cfg, ChildHealthChecks=[])
    assert "ChildHealthChecks" not in r53.get_health_check(HealthCheckId=no_children)["HealthCheck"]["HealthCheckConfig"]
    for extra, label in (({"EnableSNI": False}, "enable SNI"), ({"MeasureLatency": False}, "a latency measurement"),
                         ({"HealthThreshold": 0}, "a health threshold")):
        assert create_error(**cw_cfg, **extra) == msg("Metric-based", label)
    peel_create({**cw_cfg, **everything, "HealthThreshold": 1, "ChildHealthChecks": [ghosts[0]]},
                [(f, msg("Metric-based", label)) for f, label in metric_order])

    # Calculated checks: field rules, then a missing threshold, then unknown children.
    calc_cfg = {"Type": "CALCULATED", "ChildHealthChecks": ghosts, "HealthThreshold": 1}
    peel_create({**calc_cfg, **everything, "AlarmIdentifier": alarm, "InsufficientDataHealthStatus": "Healthy"},
                [(f, msg("Calculated", label)) for f, label in calc_order]
                + [("ChildHealthChecks", "Invalid parameter : Child health check IDs do not exist or are deleted: "
                    + ", ".join(ghosts))])
    assert create_error(Type="CALCULATED", ChildHealthChecks=[ghosts[0]]) == (
        "Invalid parameter : Calculated health checks must have a health threshold specified.")
    assert create_error(Type="CALCULATED", ChildHealthChecks=[], IPAddress="1.2.3.4") == msg("Calculated", "an IP address")
    child = create(Type="HTTP", FullyQualifiedDomainName="example.com", Port=80, ResourcePath="/")
    assert create_error(Type="CALCULATED", ChildHealthChecks=[child, ghosts[0]], HealthThreshold=1) == (
        f"Invalid parameter : Child health check IDs do not exist or are deleted: {ghosts[0]}")
    calc_id = create(Type="CALCULATED", ChildHealthChecks=[child], HealthThreshold=1)
    assert r53.get_health_check(HealthCheckId=calc_id)["HealthCheck"]["HealthCheckConfig"]["ChildHealthChecks"] == [child]
    # A calculated check always lists its children, even when none were sent.
    bare_calc = create(Type="CALCULATED", HealthThreshold=0)
    assert r53.get_health_check(HealthCheckId=bare_calc)["HealthCheck"]["HealthCheckConfig"]["ChildHealthChecks"] == []

    # Recovery-control checks use the calculated rules but allow children and a threshold.
    # (AWS also checks that the routing control exists; MiniStack has no ARC service.)
    rc_cfg = {"Type": "RECOVERY_CONTROL", "RoutingControlArn": "arn:aws:route53-recovery-control::000000000000:controlpanel/"
              "c14b2e806664476aae6db2edd214cc6e/routingcontrol/a7011654a2034f07"}
    assert create_error(Type="RECOVERY_CONTROL", IPAddress="1.2.3.4") == "Routing control ARN must not be null"
    peel_create({**rc_cfg, **everything, "AlarmIdentifier": alarm, "InsufficientDataHealthStatus": "Healthy",
                 "HealthThreshold": 1, "ChildHealthChecks": [ghosts[0]]},
                [(f, msg("Calculated", label)) for f, label in calc_order])
    # ...and accepts children (even unknown ones) and a threshold without returning either.
    rc_cfg_back = r53.get_health_check(HealthCheckId=created[-1])["HealthCheck"]["HealthCheckConfig"]
    assert "ChildHealthChecks" not in rc_cfg_back
    assert "HealthThreshold" not in rc_cfg_back

    # Updates: the same rules without RequestInterval and MeasureLatency, which UpdateHealthCheck lacks.
    update_fields = dict(endpoint, FailureThreshold=3, HealthThreshold=1, ChildHealthChecks=[ghosts[0]],
                         AlarmIdentifier={"Region": "us-east-1", "Name": "hc-rules-std2"}, InsufficientDataHealthStatus="Unhealthy")
    cw_id = create(**cw_cfg)
    assert _hc_error(r53.update_health_check, HealthCheckId=cw_id, IPAddress="1.2.3.4",
                     AlarmIdentifier={"Region": "us-east-1", "Name": "hc-rules-ext"}) == extended
    _peel(lambda **f: r53.update_health_check(HealthCheckId=cw_id, **f), update_fields,
          [(f, msg("Metric-based", label)) for f, label in metric_order if f not in ("RequestInterval", "MeasureLatency")])
    emptied = r53.update_health_check(HealthCheckId=cw_id, ChildHealthChecks=[])["HealthCheck"]
    assert emptied["HealthCheckVersion"] == 3
    assert "ChildHealthChecks" not in emptied["HealthCheckConfig"]
    calc_update_id = create(Type="CALCULATED", ChildHealthChecks=[], HealthThreshold=0)
    _peel(lambda **f: r53.update_health_check(HealthCheckId=calc_update_id, **f), update_fields,
          [(f, msg("Calculated", label)) for f, label in calc_order if f not in ("RequestInterval", "MeasureLatency")]
          + [("ChildHealthChecks", f"Invalid parameter : Child health check IDs do not exist or are deleted: {ghosts[0]}")])

    for hc_id in reversed(created):
        r53.delete_health_check(HealthCheckId=hc_id)
    cw.delete_alarms(AlarmNames=["hc-rules-std", "hc-rules-std2", "hc-rules-ext", "hc-rules-p30", "hc-rules-p10", "hc-rules-math"])

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



def test_unicode_route53_zone_comment(r53):
    resp = r53.create_hosted_zone(
        Name="unicode-zone.com",
        CallerReference="ref-uc-1",
        HostedZoneConfig={"Comment": "zona en español — Ünïcödé"},
    )
    zone_id = resp["HostedZone"]["Id"].split("/")[-1]
    get = r53.get_hosted_zone(Id=zone_id)
    assert get["HostedZone"]["Config"]["Comment"] == "zona en español — Ünïcödé"



# ─── private hosted zones ─────────────────────────────────────────────────────

_VPC_A = {"VPCRegion": "us-east-1", "VPCId": "vpc-0123456789abcdef0"}
_VPC_B = {"VPCRegion": "us-west-2", "VPCId": "vpc-0fedcba9876543210"}


def _assert_error(exc, code, status):
    assert exc.value.response["Error"]["Code"] == code
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == status


@pytest.fixture
def private_zone(r53):
    """Factory for private zones, deleted after the test even when it fails."""
    created = []

    def create(name=None, vpc=_VPC_A, caller_ref=None):
        resp = r53.create_hosted_zone(
            Name=name or f"pz-{uuid.uuid4().hex[:8]}.example.internal",
            CallerReference=caller_ref or str(uuid.uuid4()), VPC=vpc)
        created.append(resp["HostedZone"]["Id"].split("/")[-1])
        return resp

    yield create
    for zone_id in created:
        try:
            r53.delete_hosted_zone(Id=zone_id)
        except ClientError:
            pass


def test_route53_private_zone_create_returns_vpc(r53, private_zone):
    created = private_zone()
    assert created["ResponseMetadata"]["HTTPStatusCode"] == 201
    assert created["HostedZone"]["Config"]["PrivateZone"] is True
    assert created["VPC"] == _VPC_A
    assert "DelegationSet" not in created

    got = r53.get_hosted_zone(Id=created["HostedZone"]["Id"].split("/")[-1])
    assert got["HostedZone"]["Config"]["PrivateZone"] is True
    assert got["VPCs"] == [_VPC_A]
    assert "DelegationSet" not in got


def test_route53_private_zone_caller_reference_replay(r53, private_zone):
    ref = str(uuid.uuid4())
    first = private_zone(caller_ref=ref)
    replay = r53.create_hosted_zone(
        Name=first["HostedZone"]["Name"], CallerReference=ref, VPC=_VPC_A)
    assert replay["ResponseMetadata"]["HTTPStatusCode"] == 201
    assert replay["HostedZone"]["Id"] == first["HostedZone"]["Id"]
    assert replay["VPC"] == _VPC_A
    assert "DelegationSet" not in replay
    change_id = replay["ChangeInfo"]["Id"].split("/")[-1]
    assert r53.get_change(Id=change_id)["ChangeInfo"]["Id"] == replay["ChangeInfo"]["Id"]


def test_route53_public_zone_has_no_vpcs(r53):
    created = r53.create_hosted_zone(Name="public-assoc.example.com", CallerReference=str(uuid.uuid4()))
    zone_id = created["HostedZone"]["Id"].split("/")[-1]
    try:
        assert "DelegationSet" in created and "VPC" not in created
        got = r53.get_hosted_zone(Id=zone_id)
        assert got["HostedZone"]["Config"]["PrivateZone"] is False
        assert "VPCs" not in got and "DelegationSet" in got
        with pytest.raises(ClientError) as exc:
            r53.associate_vpc_with_hosted_zone(HostedZoneId=zone_id, VPC=_VPC_A)
        _assert_error(exc, "PublicZoneVPCAssociation", 400)
        with pytest.raises(ClientError) as exc:
            r53.disassociate_vpc_from_hosted_zone(HostedZoneId=zone_id, VPC=_VPC_A)
        _assert_error(exc, "VPCAssociationNotFound", 404)
    finally:
        r53.delete_hosted_zone(Id=zone_id)


def test_route53_private_zone_associate_and_disassociate(r53, private_zone):
    zone_id = private_zone()["HostedZone"]["Id"].split("/")[-1]
    resp = r53.associate_vpc_with_hosted_zone(HostedZoneId=zone_id, VPC=_VPC_B, Comment="second")
    assert resp["ChangeInfo"]["Comment"] == "second"
    assert r53.get_change(Id=resp["ChangeInfo"]["Id"].split("/")[-1])["ChangeInfo"]["Status"] in (
        "PENDING", "INSYNC")
    assert r53.get_hosted_zone(Id=zone_id)["VPCs"] == [_VPC_A, _VPC_B]

    with pytest.raises(ClientError) as exc:
        r53.associate_vpc_with_hosted_zone(HostedZoneId=zone_id, VPC=_VPC_B)
    _assert_error(exc, "ConflictingDomainExists", 400)

    r53.disassociate_vpc_from_hosted_zone(HostedZoneId=zone_id, VPC=_VPC_B)
    assert r53.get_hosted_zone(Id=zone_id)["VPCs"] == [_VPC_A]
    with pytest.raises(ClientError) as exc:
        r53.disassociate_vpc_from_hosted_zone(HostedZoneId=zone_id, VPC=_VPC_B)
    _assert_error(exc, "VPCAssociationNotFound", 404)
    with pytest.raises(ClientError) as exc:
        r53.disassociate_vpc_from_hosted_zone(HostedZoneId=zone_id, VPC=_VPC_A)
    _assert_error(exc, "LastVPCAssociation", 400)
    # The same VPC id in another region is a different VPC.
    with pytest.raises(ClientError) as exc:
        r53.disassociate_vpc_from_hosted_zone(
            HostedZoneId=zone_id, VPC={"VPCRegion": "eu-west-1", "VPCId": _VPC_A["VPCId"]})
    _assert_error(exc, "VPCAssociationNotFound", 404)


def test_route53_private_zone_same_name_conflicts_on_shared_vpc(r53, private_zone):
    name = f"dup-{uuid.uuid4().hex[:8]}.example.internal"
    private_zone(name=name)
    with pytest.raises(ClientError) as exc:
        r53.create_hosted_zone(Name=name, CallerReference=str(uuid.uuid4()), VPC=_VPC_A)
    _assert_error(exc, "ConflictingDomainExists", 400)

    other_id = private_zone(name=name, vpc=_VPC_B)["HostedZone"]["Id"].split("/")[-1]
    with pytest.raises(ClientError) as exc:
        r53.associate_vpc_with_hosted_zone(HostedZoneId=other_id, VPC=_VPC_A)
    _assert_error(exc, "ConflictingDomainExists", 400)


@pytest.mark.parametrize("operation", ["associate_vpc_with_hosted_zone", "disassociate_vpc_from_hosted_zone"])
def test_route53_vpc_association_unknown_zone(r53, operation):
    with pytest.raises(ClientError) as exc:
        getattr(r53, operation)(HostedZoneId="ZNOSUCHZONE0001", VPC=_VPC_A)
    _assert_error(exc, "NoSuchHostedZone", 404)


@pytest.mark.parametrize("vpc,code", [
    ({"VPCRegion": "us-east-1", "VPCId": "not-a-vpc"}, "InvalidVPCId"),
    ({"VPCRegion": "us-east-1"}, "InvalidVPCId"),
    ({"VPCId": "vpc-0123456789abcdef0"}, "InvalidInput"),
])
def test_route53_private_zone_rejects_incomplete_vpc(r53, private_zone, vpc, code):
    with pytest.raises(ClientError) as exc:
        r53.create_hosted_zone(Name="bad-vpc.example.internal", CallerReference=str(uuid.uuid4()), VPC=vpc)
    _assert_error(exc, code, 400)
    zone_id = private_zone()["HostedZone"]["Id"].split("/")[-1]
    with pytest.raises(ClientError) as exc:
        r53.associate_vpc_with_hosted_zone(HostedZoneId=zone_id, VPC=vpc)
    _assert_error(exc, code, 400)


def test_route53_private_zone_flag_without_vpc_is_accepted(r53):
    """MiniStack keeps accepting PrivateZone=true with no VPC (Cloud Map relies on it)."""
    created = r53.create_hosted_zone(
        Name="flag-only.example.internal", CallerReference=str(uuid.uuid4()),
        HostedZoneConfig={"PrivateZone": True})
    zone_id = created["HostedZone"]["Id"].split("/")[-1]
    try:
        assert created["HostedZone"]["Config"]["PrivateZone"] is True
        assert r53.get_hosted_zone(Id=zone_id)["VPCs"] == []
    finally:
        r53.delete_hosted_zone(Id=zone_id)


def test_route53_private_zone_delete(r53):
    zone_id = r53.create_hosted_zone(
        Name="delete-private.example.internal", CallerReference=str(uuid.uuid4()),
        VPC=_VPC_A)["HostedZone"]["Id"].split("/")[-1]
    r53.delete_hosted_zone(Id=zone_id)
    with pytest.raises(ClientError) as exc:
        r53.get_hosted_zone(Id=zone_id)
    _assert_error(exc, "NoSuchHostedZone", 404)
    assert zone_id not in [
        s["HostedZoneId"] for s in r53.list_hosted_zones_by_vpc(**_VPC_A)["HostedZoneSummaries"]]


def test_route53_list_hosted_zones_by_vpc_paginates(r53, private_zone):
    vpc = {"VPCRegion": "eu-central-1", "VPCId": f"vpc-{uuid.uuid4().hex[:17]}"}
    ids = sorted(private_zone(vpc=vpc)["HostedZone"]["Id"].split("/")[-1] for _ in range(3))
    first = r53.list_hosted_zones_by_vpc(VPCId=vpc["VPCId"], VPCRegion=vpc["VPCRegion"], MaxItems="2")
    assert [s["HostedZoneId"] for s in first["HostedZoneSummaries"]] == ids[:2]
    assert first["MaxItems"] == "2"
    assert first["HostedZoneSummaries"][0]["Owner"]["OwningAccount"]
    second = r53.list_hosted_zones_by_vpc(
        VPCId=vpc["VPCId"], VPCRegion=vpc["VPCRegion"], MaxItems="2", NextToken=first["NextToken"])
    assert [s["HostedZoneId"] for s in second["HostedZoneSummaries"]] == ids[2:]
    assert "NextToken" not in second


def test_route53_list_hosted_zones_by_vpc_filters_and_validates(r53, private_zone):
    vpc = {"VPCRegion": "ap-south-1", "VPCId": f"vpc-{uuid.uuid4().hex[:17]}"}
    zone_id = private_zone(vpc=vpc)["HostedZone"]["Id"].split("/")[-1]
    match = r53.list_hosted_zones_by_vpc(VPCId=vpc["VPCId"], VPCRegion=vpc["VPCRegion"])
    assert [s["HostedZoneId"] for s in match["HostedZoneSummaries"]] == [zone_id]
    # Same VPC id, other region: no match.
    other = r53.list_hosted_zones_by_vpc(VPCId=vpc["VPCId"], VPCRegion="us-east-2")
    assert other["HostedZoneSummaries"] == []
    assert "NextToken" not in other
    with pytest.raises(ClientError) as exc:
        r53.list_hosted_zones_by_vpc(VPCId=vpc["VPCId"], VPCRegion=vpc["VPCRegion"], NextToken="bogus")
    _assert_error(exc, "InvalidPaginationToken", 400)


def test_route53_list_hosted_zones_by_vpc_requires_region():
    # botocore enforces VPCRegion client-side; turn that off to reach the server.
    client = make_client("route53", {"parameter_validation": False})
    with pytest.raises(ClientError) as exc:
        client.list_hosted_zones_by_vpc(VPCId=_VPC_A["VPCId"])
    _assert_error(exc, "InvalidInput", 400)


def test_route53_private_zones_are_account_scoped(r53, private_zone):
    vpc = {"VPCRegion": "us-east-1", "VPCId": f"vpc-{uuid.uuid4().hex[:17]}"}
    zone_id = private_zone(vpc=vpc)["HostedZone"]["Id"].split("/")[-1]
    other = boto3.client(
        "route53", endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        region_name="us-east-1", aws_access_key_id="222233334444", aws_secret_access_key="test")
    assert other.list_hosted_zones_by_vpc(**vpc)["HostedZoneSummaries"] == []
    with pytest.raises(ClientError) as exc:
        other.associate_vpc_with_hosted_zone(HostedZoneId=zone_id, VPC=_VPC_B)
    _assert_error(exc, "NoSuchHostedZone", 404)
    assert r53.get_hosted_zone(Id=zone_id)["VPCs"] == [vpc]


@pytest.mark.auth_contract
@pytest.mark.parametrize("permission", ["none", "allow", "other-zone"])
def test_route53_associate_vpc_permission_contract(auth_enabled, iam_user, private_zone, permission):
    zone_id = private_zone()["HostedZone"]["Id"].split("/")[-1]
    statements = []
    if permission != "none":
        resource_zone = "ZOTHERZONE0001" if permission == "other-zone" else zone_id
        statements.append({"Effect": "Allow", "Action": "route53:AssociateVPCWithHostedZone",
                           "Resource": f"arn:aws:route53:::hostedzone/{resource_zone}"})
    policy = {"Version": "2012-10-17", "Statement": statements} if statements else None
    client = iam_user(policy=policy).client("route53")
    if auth_enabled and permission != "allow":
        with pytest.raises(ClientError) as exc:
            client.associate_vpc_with_hosted_zone(HostedZoneId=zone_id, VPC=_VPC_B)
        assert exc.value.response["Error"]["Code"] == "AccessDenied"
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    else:
        resp = client.associate_vpc_with_hosted_zone(HostedZoneId=zone_id, VPC=_VPC_B)
        assert resp["ChangeInfo"]["Status"] in ("PENDING", "INSYNC")


@pytest.mark.auth_contract
def test_route53_list_hosted_zones_by_vpc_permission_contract(auth_enabled, iam_user):
    client = iam_user().client("route53")
    if auth_enabled:
        with pytest.raises(ClientError) as exc:
            client.list_hosted_zones_by_vpc(**_VPC_A)
        assert exc.value.response["Error"]["Code"] == "AccessDenied"
    else:
        assert "HostedZoneSummaries" in client.list_hosted_zones_by_vpc(**_VPC_A)


def test_route53_invalid_change_batch_messages_follow_aws(r53):
    """#2465, captured on AWS: duplicate CREATE and missing DELETE share the bracketed layout, the name always
    carries its trailing dot, and a batch with a failing change applies nothing."""
    from botocore.exceptions import ClientError

    zone_name = f"r53-msg-{uuid.uuid4().hex[:8]}.example.test"
    zone = r53.create_hosted_zone(Name=zone_name, CallerReference=uuid.uuid4().hex)["HostedZone"]["Id"]
    a = {"Name": f"www.{zone_name}", "Type": "A", "TTL": 60, "ResourceRecords": [{"Value": "10.0.0.1"}]}
    alias = {"Name": f"alias.{zone_name}", "Type": "A", "AliasTarget": {
        "HostedZoneId": zone.split("/")[-1], "DNSName": f"www.{zone_name}", "EvaluateTargetHealth": False}}

    def change(*changes):
        return r53.change_resource_record_sets(HostedZoneId=zone, ChangeBatch={"Changes": list(changes)})

    def refused(*changes):
        with pytest.raises(ClientError) as exc:
            change(*changes)
        assert exc.value.response["Error"]["Code"] == "InvalidChangeBatch"
        return exc.value.response["Error"]["Message"]

    change({"Action": "CREATE", "ResourceRecordSet": a}, {"Action": "CREATE", "ResourceRecordSet": alias})
    exists = f"[Tried to create resource record set [name='www.{zone_name}.', type='A'] but it already exists]"
    assert refused({"Action": "CREATE", "ResourceRecordSet": a}) == exists
    assert refused({"Action": "CREATE", "ResourceRecordSet": {**a, "Name": a["Name"] + "."}}) == exists
    assert refused({"Action": "CREATE", "ResourceRecordSet": alias}) == (
        f"[Tried to create resource record set [name='alias.{zone_name}.', type='A'] but it already exists]")
    assert refused({"Action": "DELETE", "ResourceRecordSet": {**a, "Name": f"nope.{zone_name}"}}) == (
        f"[Tried to delete resource record set [name='nope.{zone_name}.', type='A'] but it was not found]")
    assert refused({"Action": "CREATE", "ResourceRecordSet": {**a, "Name": f"new.{zone_name}"}},
                   {"Action": "CREATE", "ResourceRecordSet": a}) == exists
    names = [r["Name"] for r in r53.list_resource_record_sets(HostedZoneId=zone)["ResourceRecordSets"]]
    assert f"new.{zone_name}." not in names
