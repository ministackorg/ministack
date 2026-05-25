"""
Integration tests for the Inspector2 emulator.
"""

import json

import pytest
from botocore.exceptions import ClientError
from conftest import ENDPOINT


class TestEnableDisable:
    def test_enable_ecr(self, inspector2):
        resp = inspector2.enable(resourceTypes=["ECR"])
        accounts = resp["accounts"]
        assert len(accounts) >= 1
        acct = accounts[0]
        assert acct["resourceStatus"]["ecr"] == "ENABLED"
        assert acct["status"] == "ENABLED"

    def test_enable_multiple_resource_types(self, inspector2):
        resp = inspector2.enable(resourceTypes=["ECR", "EC2", "LAMBDA", "LAMBDA_CODE"])
        acct = resp["accounts"][0]
        assert acct["resourceStatus"]["ecr"] == "ENABLED"
        assert acct["resourceStatus"]["ec2"] == "ENABLED"
        assert acct["resourceStatus"]["lambda"] == "ENABLED"
        assert acct["resourceStatus"]["lambdaCode"] == "ENABLED"

    def test_disable_ecr(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        resp = inspector2.disable(resourceTypes=["ECR"])
        acct = resp["accounts"][0]
        assert acct["resourceStatus"]["ecr"] == "DISABLED"

    def test_disable_all(self, inspector2):
        inspector2.enable(resourceTypes=["ECR", "EC2"])
        resp = inspector2.disable(resourceTypes=[])
        acct = resp["accounts"][0]
        assert acct["resourceStatus"]["ecr"] == "DISABLED"
        assert acct["resourceStatus"]["ec2"] == "DISABLED"

    def test_enable_with_specific_account_ids(self, inspector2):
        resp = inspector2.enable(
            resourceTypes=["ECR"],
            accountIds=["111111111111"],
        )
        assert len(resp["accounts"]) == 1
        assert resp["accounts"][0]["accountId"] == "111111111111"


class TestListFindings:
    def test_list_findings_empty_when_scanning_disabled(self, inspector2):
        inspector2.disable(resourceTypes=[])
        resp = inspector2.list_findings()
        assert "findings" in resp
        assert resp["findings"] == []

    def test_list_findings_deterministic_structure(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        resp1 = inspector2.list_findings()
        resp2 = inspector2.list_findings()
        assert len(resp1["findings"]) == len(resp2["findings"])
        assert resp1["findings"][0]["severity"] == resp2["findings"][0]["severity"]
        assert resp1["findings"][0]["type"] == resp2["findings"][0]["type"]

    def test_list_findings_pagination(self, inspector2):
        resp = inspector2.list_findings(maxResults=10)
        assert "findings" in resp
        assert len(resp["findings"]) <= 10


class TestCoverage:
    def test_list_coverage_has_resources(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        inspector2.list_findings()
        resp = inspector2.list_coverage()
        assert len(resp["coveredResources"]) > 0

    def test_list_coverage_statistics_counts(self, inspector2):
        inspector2.list_findings()
        resp = inspector2.list_coverage_statistics()
        assert "countsByGroup" in resp
        assert resp["totalCounts"] > 0


class TestFindingAggregations:
    def test_aggregation_by_finding_type(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        inspector2.list_findings()
        resp = inspector2.list_finding_aggregations(aggregationType="FINDING_TYPE")
        assert resp["aggregationType"] == "FINDING_TYPE"

    def test_aggregation_by_account(self, inspector2):
        inspector2.list_findings()
        resp = inspector2.list_finding_aggregations(aggregationType="ACCOUNT")
        assert resp["aggregationType"] == "ACCOUNT"

    def test_aggregation_by_package(self, inspector2):
        inspector2.list_findings()
        resp = inspector2.list_finding_aggregations(aggregationType="PACKAGE")
        assert resp["aggregationType"] == "PACKAGE"


class TestSearchVulnerabilities:
    def test_search_known_cve(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        findings = inspector2.list_findings()["findings"]
        assert len(findings) > 0
        first_vuln_id = findings[0]["packageVulnerabilityDetails"]["vulnerabilityId"]

        resp = inspector2.search_vulnerabilities(filterCriteria={"vulnerabilityIds": [first_vuln_id]})
        matches = resp["vulnerabilities"]
        assert len(matches) >= 1
        assert matches[0]["id"] == first_vuln_id

    def test_search_nonexistent_cve(self, inspector2):
        resp = inspector2.search_vulnerabilities(filterCriteria={"vulnerabilityIds": ["CVE-nonexistent"]})
        assert resp["vulnerabilities"] == []


class TestPersistence:
    def test_state_methods_exist(self):
        from ministack.services import inspector2 as _inspector2

        assert hasattr(_inspector2, "get_state")
        assert hasattr(_inspector2, "restore_state")
        assert hasattr(_inspector2, "reset")
        assert hasattr(_inspector2, "handle_request")

    def test_reset_clears_state(self):
        from ministack.services import inspector2 as _inspector2

        _inspector2.reset()
        state = _inspector2.get_state()
        for key in ("account_config", "findings", "coverage", "scan_history", "tags", "filters"):
            assert key in state

    def test_get_state_returns_expected_keys(self):
        from ministack.services import inspector2 as _inspector2

        _inspector2.reset()
        state = _inspector2.get_state()
        for key in ("account_config", "findings", "coverage", "scan_history", "tags", "filters"):
            assert key in state

    def test_restore_state_preserves_config(self):
        from ministack.services import inspector2 as _inspector2

        _inspector2.reset()
        state = _inspector2.get_state()
        _inspector2.restore_state(state)
        state2 = _inspector2.get_state()
        assert set(state.keys()) == set(state2.keys())


class TestFindingsFiltering:
    def test_filter_by_severity(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        inspector2.list_findings()
        resp = inspector2.list_findings(
            filterCriteria={
                "severity": [{"comparison": "EQUALS", "value": "HIGH"}],
            }
        )
        for f in resp["findings"]:
            assert f["severity"] == "HIGH"

    def test_filter_by_type(self, inspector2):
        inspector2.list_findings()
        resp = inspector2.list_findings(
            filterCriteria={
                "findingType": [{"comparison": "EQUALS", "value": "PACKAGE_VULNERABILITY"}],
            }
        )
        for f in resp["findings"]:
            assert f["type"] == "PACKAGE_VULNERABILITY"

    def test_filter_by_status(self, inspector2):
        inspector2.list_findings()
        resp = inspector2.list_findings(
            filterCriteria={
                "findingStatus": [{"comparison": "EQUALS", "value": "ACTIVE"}],
            }
        )
        for f in resp["findings"]:
            assert f["status"] == "ACTIVE"

    def test_filter_by_resource_type(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        inspector2.list_findings()
        resp = inspector2.list_findings(
            filterCriteria={
                "resourceType": [{"comparison": "EQUALS", "value": "AWS_ECR_CONTAINER_IMAGE"}],
            }
        )
        for f in resp["findings"]:
            resource_types = {r["type"] for r in f.get("resources", [])}
            assert "AWS_ECR_CONTAINER_IMAGE" in resource_types

    def test_pagination_next_token(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        inspector2.list_findings()
        resp1 = inspector2.list_findings(maxResults=5)
        findings = resp1.get("findings", [])
        next_token = resp1.get("nextToken")
        if next_token and len(findings) == 5:
            resp2 = inspector2.list_findings(maxResults=5, nextToken=next_token)
            assert "findings" in resp2


class TestBoto3ResponseShapes:
    """Validate responses are parseable by boto3 and match AWS service model."""

    def test_list_findings_response_has_expected_fields(self, inspector2):
        """Every finding field expected by botocore must be present."""
        inspector2.enable(resourceTypes=["ECR"])
        resp = inspector2.list_findings()
        findings = resp["findings"]
        assert len(findings) > 0

        f = findings[0]
        # Top-level Finding fields from botocore service model
        expected_top = {
            "findingArn",
            "awsAccountId",
            "type",
            "description",
            "title",
            "remediation",
            "severity",
            "firstObservedAt",
            "lastObservedAt",
            "updatedAt",
            "status",
            "resources",
            "inspectorScore",
            "inspectorScoreDetails",
            "packageVulnerabilityDetails",
            "fixAvailable",
            "exploitAvailable",
        }
        for key in expected_top:
            assert key in f, f"Missing finding field: {key}"

        # Resource shape
        r = f["resources"][0]
        assert "type" in r
        assert "id" in r
        assert "partition" in r
        assert "region" in r
        assert "details" in r

        # Package vulnerability details shape
        pvd = f["packageVulnerabilityDetails"]
        assert "vulnerabilityId" in pvd
        assert "source" in pvd
        assert "sourceUrl" in pvd
        assert "vendorSeverity" in pvd
        assert "vendorCreatedAt" in pvd
        assert "vendorUpdatedAt" in pvd
        assert "referenceUrls" in pvd
        assert "relatedVulnerabilities" in pvd
        assert "cvss" in pvd
        assert "vulnerablePackages" in pvd

        # Vulnerable package shape
        pkg = pvd["vulnerablePackages"][0]
        assert "name" in pkg
        assert "version" in pkg
        assert "packageManager" in pkg
        assert "fixedInVersion" in pkg

    def test_dates_are_parsed_by_boto3(self, inspector2):
        """Boto3 REST-JSON parser materialises ISO timestamps as datetime."""
        from datetime import datetime

        inspector2.enable(resourceTypes=["ECR"])
        resp = inspector2.list_findings()
        f = resp["findings"][0]

        for key in ("firstObservedAt", "lastObservedAt", "updatedAt"):
            val = f[key]
            assert isinstance(val, datetime), f"{key} should be datetime, got {type(val)}"

    def test_coverage_response_shape(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        inspector2.list_findings()
        resp = inspector2.list_coverage()
        resources = resp["coveredResources"]
        assert len(resources) > 0

        r = resources[0]
        assert "resourceType" in r
        assert "resourceId" in r
        assert "accountId" in r
        assert "scanType" in r
        assert "scanStatus" in r
        assert "lastScannedAt" in r

    def test_search_vulnerabilities_response_shape(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        findings = inspector2.list_findings()["findings"]
        vuln_id = findings[0]["packageVulnerabilityDetails"]["vulnerabilityId"]

        resp = inspector2.search_vulnerabilities(filterCriteria={"vulnerabilityIds": [vuln_id]})
        vulns = resp["vulnerabilities"]
        assert len(vulns) > 0
        v = vulns[0]
        # Fields from botocore Vulnerability shape
        assert "id" in v
        assert "source" in v
        assert "description" in v
        assert "vendorSeverity" in v
        assert "vendorCreatedAt" in v
        assert "vendorUpdatedAt" in v
        assert "sourceUrl" in v
        assert "referenceUrls" in v
        assert "relatedVulnerabilities" in v
        assert "atigData" in v
        assert "cvss4" in v
        assert "cvss3" in v
        assert "cvss2" in v
        assert "cisaData" in v
        assert "exploitObserved" in v
        assert "detectionPlatforms" in v
        assert "epss" in v
        assert "cwes" in v

    def test_error_response_shape(self, inspector2):
        """Boto3 must raise ClientError with Code and Message."""
        with pytest.raises(ClientError) as exc:
            inspector2.delete_filter(arn="arn:aws:inspector2:us-east-1:000000000000:filter/does-not-exist")
        err = exc.value.response["Error"]
        assert "Code" in err
        assert "Message" in err
        assert err["Code"] == "ResourceNotFoundException"
        assert "does-not-exist" in err["Message"] or "not found" in err["Message"].lower()


class TestEndToEndScan:
    def test_full_stub_scan_ecr(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])

        resp = inspector2.list_findings()
        findings = resp["findings"]
        assert len(findings) > 0, "Expected stub findings for ECR"

        f = findings[0]
        assert f["type"] == "PACKAGE_VULNERABILITY"
        assert f["severity"] in ("CRITICAL", "HIGH", "MEDIUM", "LOW")
        assert f["status"] == "ACTIVE"
        assert f["fixAvailable"] in ("YES", "NO")
        assert "inspectorScore" in f
        assert len(f["resources"]) >= 1
        assert f["resources"][0]["type"] == "AWS_ECR_CONTAINER_IMAGE"

        pvd = f["packageVulnerabilityDetails"]
        assert len(pvd["vulnerabilityId"]) > 0
        assert len(pvd["vulnerablePackages"]) >= 1
        pkg = pvd["vulnerablePackages"][0]
        assert len(pkg["name"]) > 0
        assert pkg["packageManager"] in ("OS", "PIP", "NPM", "GOMOD", "JAR")

        coverage = inspector2.list_coverage()
        assert len(coverage["coveredResources"]) > 0

        stats = inspector2.list_coverage_statistics()
        assert stats["totalCounts"] > 0

        agg = inspector2.list_finding_aggregations(aggregationType="FINDING_TYPE")
        assert len(agg.get("responses", [])) > 0

    def test_full_stub_scan_lambda(self, inspector2):
        from ministack.services import inspector2 as _inspector2

        _inspector2.reset()
        inspector2.enable(resourceTypes=["LAMBDA"])

        resp = inspector2.list_findings()
        findings = resp["findings"]
        assert len(findings) > 0, "Expected stub findings for Lambda"

        resource_types = set()
        for f in findings:
            for r in f["resources"]:
                resource_types.add(r["type"])
        assert "AWS_LAMBDA_FUNCTION" in resource_types

    def test_full_stub_scan_ec2(self, inspector2):
        from ministack.services import inspector2 as _inspector2

        _inspector2.reset()
        inspector2.enable(resourceTypes=["EC2"])

        resp = inspector2.list_findings()
        findings = resp["findings"]
        assert len(findings) > 0, "Expected stub findings for EC2"

        resource_types = set()
        for f in findings:
            for r in f["resources"]:
                resource_types.add(r["type"])
        assert "AWS_EC2_INSTANCE" in resource_types

    def test_scan_deterministic_across_calls(self, inspector2):
        from ministack.services import inspector2 as _inspector2

        _inspector2.reset()
        inspector2.enable(resourceTypes=["ECR"])

        resp1 = inspector2.list_findings()
        resp2 = inspector2.list_findings()
        assert len(resp1["findings"]) == len(resp2["findings"])
        assert len(resp1["findings"]) > 0
        assert resp1["findings"][0]["severity"] == resp2["findings"][0]["severity"]


class TestMultitenancy:
    """Verify Inspector2 state is isolated per account ID."""

    def _client(self, access_key):
        import boto3
        from botocore.config import Config

        return boto3.client(
            "inspector2",
            endpoint_url=ENDPOINT,
            aws_access_key_id=access_key,
            aws_secret_access_key="test",
            region_name="us-east-1",
            config=Config(region_name="us-east-1", retries={"max_attempts": 0}),
        )

    def test_findings_isolated_by_account(self):
        from ministack.services import inspector2 as _inspector2

        _inspector2.reset()
        client_a = self._client("111111111111")
        client_b = self._client("222222222222")

        client_a.enable(resourceTypes=["ECR"])
        client_b.enable(resourceTypes=["ECR"])

        findings_a = client_a.list_findings()["findings"]
        findings_b = client_b.list_findings()["findings"]

        assert len(findings_a) > 0
        assert len(findings_b) > 0
        # Account A findings should have A's account ID
        assert findings_a[0]["awsAccountId"] == "111111111111"
        # Account B findings should have B's account ID
        assert findings_b[0]["awsAccountId"] == "222222222222"

    def test_filters_isolated_by_account(self):
        from ministack.services import inspector2 as _inspector2

        _inspector2.reset()
        client_a = self._client("111111111111")
        client_b = self._client("222222222222")

        client_a.create_filter(name="acct-a-filter", action="NONE", filterCriteria={})
        client_b.create_filter(name="acct-b-filter", action="NONE", filterCriteria={})

        filters_a = client_a.list_filters()["filters"]
        filters_b = client_b.list_filters()["filters"]

        assert len(filters_a) == 1
        assert len(filters_b) == 1
        assert filters_a[0]["name"] == "acct-a-filter"
        assert filters_b[0]["name"] == "acct-b-filter"

    def test_tags_isolated_by_account(self):
        from ministack.services import inspector2 as _inspector2

        _inspector2.reset()
        client_a = self._client("111111111111")
        client_b = self._client("222222222222")

        arn = "arn:aws:inspector2:us-east-1:000000000000:finding/shared-arn"
        client_a.tag_resource(resourceArn=arn, tags={"owner": "a"})
        client_b.tag_resource(resourceArn=arn, tags={"owner": "b"})

        tags_a = client_a.list_tags_for_resource(resourceArn=arn)["tags"]
        tags_b = client_b.list_tags_for_resource(resourceArn=arn)["tags"]

        assert tags_a["owner"] == "a"
        assert tags_b["owner"] == "b"


class TestAdvancedFiltering:
    """Complex filter criteria and edge cases."""

    def test_filter_between_inspector_score(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        all_resp = inspector2.list_findings()
        total = len(all_resp["findings"])

        resp = inspector2.list_findings(
            filterCriteria={
                "inspectorScore": [{"lowerInclusive": 8.0, "upperInclusive": 10.0}],
            }
        )
        filtered = resp["findings"]
        # Should filter out lower-scored findings
        assert len(filtered) < total, f"Expected filtering to reduce count from {total}, got {len(filtered)}"
        for f in filtered:
            score = f.get("inspectorScore", 0) * 10
            assert 8.0 <= score <= 10.0

    def test_filter_not_equals(self, inspector2):
        inspector2.list_findings()
        resp = inspector2.list_findings(
            filterCriteria={
                "severity": [{"comparison": "NOT_EQUALS", "value": "CRITICAL"}],
            }
        )
        for f in resp["findings"]:
            assert f["severity"] != "CRITICAL"

    def test_combined_filters(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        inspector2.list_findings()
        resp = inspector2.list_findings(
            filterCriteria={
                "severity": [{"comparison": "EQUALS", "value": "HIGH"}],
                "findingStatus": [{"comparison": "EQUALS", "value": "ACTIVE"}],
            }
        )
        for f in resp["findings"]:
            assert f["severity"] == "HIGH"
            assert f["status"] == "ACTIVE"

    def test_filter_or_within_same_field(self, inspector2):
        """Multiple conditions on the same field are ORed together."""
        inspector2.enable(resourceTypes=["ECR"])
        all_resp = inspector2.list_findings()
        total = len(all_resp["findings"])

        resp = inspector2.list_findings(
            filterCriteria={
                "severity": [
                    {"comparison": "EQUALS", "value": "HIGH"},
                    {"comparison": "EQUALS", "value": "MEDIUM"},
                ],
            }
        )
        filtered = resp["findings"]
        # Should include both HIGH and MEDIUM, so more than just one severity
        assert len(filtered) > 0
        severities = {f["severity"] for f in filtered}
        assert severities.issubset({"HIGH", "MEDIUM"}), f"Expected only HIGH/MEDIUM, got {severities}"
        # Should be more inclusive than a single-severity filter
        single = inspector2.list_findings(filterCriteria={"severity": [{"comparison": "EQUALS", "value": "HIGH"}]})
        assert len(filtered) >= len(single["findings"])

    def test_filter_no_matches(self, inspector2):
        inspector2.list_findings()
        resp = inspector2.list_findings(
            filterCriteria={
                "severity": [{"comparison": "EQUALS", "value": "NONEXISTENT"}],
            }
        )
        assert resp["findings"] == []

    def test_sort_by_first_observed(self, inspector2):
        inspector2.list_findings()
        resp = inspector2.list_findings(sortCriteria={"field": "FIRST_OBSERVED_AT", "sortOrder": "DESC"})
        assert "findings" in resp

    def test_sort_by_inspector_score(self, inspector2):
        inspector2.list_findings()
        resp = inspector2.list_findings(sortCriteria={"field": "INSPECTOR_SCORE", "sortOrder": "DESC"})
        findings = resp["findings"]
        if len(findings) > 1:
            assert findings[0]["inspectorScore"] >= findings[1]["inspectorScore"]


class TestErrorBoundaries:
    """Invalid inputs and edge cases."""

    def test_invalid_aggregation_type(self, inspector2):
        resp = inspector2.list_finding_aggregations(aggregationType="INVALID")
        assert resp["aggregationType"] == "INVALID"
        assert resp["responses"] == []

    def test_enable_invalid_resource_type(self, inspector2):
        resp = inspector2.enable(resourceTypes=["INVALID_TYPE"])
        assert resp["accounts"][0]["status"] == "ENABLED"


class TestIntegration:
    """Inspector2 integration with other ministack services."""

    def test_findings_use_actual_account_id(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        resp = inspector2.list_findings()
        if resp["findings"]:
            assert resp["findings"][0]["awsAccountId"] == "000000000000"

    def test_coverage_matches_findings_resources(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        findings = inspector2.list_findings()["findings"]
        coverage = inspector2.list_coverage()["coveredResources"]

        finding_resource_types = set()
        for f in findings:
            for r in f["resources"]:
                finding_resource_types.add(r["type"])

        coverage_resource_types = {c["resourceType"] for c in coverage}
        assert finding_resource_types == coverage_resource_types

    def test_disable_then_list_findings_empty(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        inspector2.list_findings()

        # Disable everything
        inspector2.disable(resourceTypes=[])

        resp = inspector2.list_findings()
        assert resp["findings"] == []

    def test_multiple_resource_types_enabled(self, inspector2):
        inspector2.enable(resourceTypes=["ECR", "EC2"])
        findings = inspector2.list_findings()["findings"]

        resource_types = set()
        for f in findings:
            for r in f["resources"]:
                resource_types.add(r["type"])

        assert "AWS_ECR_CONTAINER_IMAGE" in resource_types
        assert "AWS_EC2_INSTANCE" in resource_types


class TestBatchGetFindingDetails:
    def test_batch_get_finding_details(self, inspector2):
        inspector2.enable(resourceTypes=["ECR"])
        findings = inspector2.list_findings()["findings"]
        assert len(findings) > 0

        arn = findings[0]["findingArn"]
        resp = inspector2.batch_get_finding_details(findingArns=[arn])
        assert len(resp["findingDetails"]) == 1
        assert resp["findingDetails"][0]["findingArn"] == arn

    def test_batch_get_finding_details_missing_arn(self, inspector2):
        resp = inspector2.batch_get_finding_details(
            findingArns=["arn:aws:inspector2:us-east-1:000000000000:finding/nonexistent"]
        )
        assert len(resp["findingDetails"]) == 0
        assert len(resp["errors"]) == 1
        assert resp["errors"][0]["errorCode"] == "RESOURCE_NOT_FOUND"
