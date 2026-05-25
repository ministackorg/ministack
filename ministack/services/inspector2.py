"""
Amazon Inspector2 Emulator.
REST-JSON API with deterministic stub vulnerability findings.
"""

import copy
import json
import logging
import time
import uuid

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
)

logger = logging.getLogger("inspector2")

_account_config = AccountScopedDict()
_findings = AccountScopedDict()
_coverage = AccountScopedDict()
_scan_history = AccountScopedDict()
_tags = AccountScopedDict()
_filters = AccountScopedDict()


def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())


def _finding_arn(acct_id, finding_id):
    return f"arn:aws:inspector2:{get_region()}:{acct_id}:finding/{finding_id}"


_STUB_PACKAGES = [
    {
        "name": "libcrypto3",
        "version": "3.3.2-r0",
        "fixedIn": "3.3.2-r1",
        "manager": "OS",
        "arch": "aarch64",
        "vulnId": "CVE-2024-9143",
        "severity": "MEDIUM",
        "cwe": "CWE-787",
        "cvssScore": 7.5,
        "cvssVector": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:N/I:N/A:H",
        "fixState": "fixed",
    },
    {
        "name": "openssl",
        "version": "3.0.7-r3",
        "fixedIn": "3.0.8-r0",
        "manager": "OS",
        "arch": "x86_64",
        "vulnId": "CVE-2023-0464",
        "severity": "HIGH",
        "cwe": "CWE-400",
        "cvssScore": 8.2,
        "cvssVector": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:N/I:L/A:H",
        "fixState": "fixed",
    },
    {
        "name": "curl",
        "version": "7.87.0",
        "fixedIn": "7.88.0",
        "manager": "OS",
        "arch": "x86_64",
        "vulnId": "CVE-2023-38545",
        "severity": "HIGH",
        "cwe": "CWE-122",
        "cvssScore": 9.8,
        "cvssVector": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
        "fixState": "fixed",
    },
    {
        "name": "busybox",
        "version": "1.35.0",
        "fixedIn": "1.35.1",
        "manager": "OS",
        "arch": "aarch64",
        "vulnId": "CVE-2023-42363",
        "severity": "MEDIUM",
        "cwe": "CWE-416",
        "cvssScore": 5.5,
        "cvssVector": "CVSS:3.1/AV:L/AC:L/PR:N/UI:R/S:U/C:N/I:N/A:H",
        "fixState": "fixed",
    },
    {
        "name": "requests",
        "version": "2.28.0",
        "fixedIn": "2.31.0",
        "manager": "PIP",
        "arch": "",
        "vulnId": "CVE-2023-32681",
        "severity": "MEDIUM",
        "cwe": "CWE-200",
        "cvssScore": 6.1,
        "cvssVector": "CVSS:3.1/AV:N/AC:L/PR:N/UI:R/S:C/C:L/I:L/A:N",
        "fixState": "fixed",
    },
    {
        "name": "urllib3",
        "version": "1.26.12",
        "fixedIn": "1.26.18",
        "manager": "PIP",
        "arch": "",
        "vulnId": "CVE-2023-45803",
        "severity": "MEDIUM",
        "cwe": "CWE-200",
        "cvssScore": 4.2,
        "cvssVector": "CVSS:3.1/AV:A/AC:H/PR:H/UI:N/S:U/C:H/I:N/A:N",
        "fixState": "wont-fix",
    },
    {
        "name": "lodash",
        "version": "4.17.20",
        "fixedIn": "4.17.21",
        "manager": "NPM",
        "arch": "",
        "vulnId": "CVE-2021-23337",
        "severity": "HIGH",
        "cwe": "CWE-77",
        "cvssScore": 7.2,
        "cvssVector": "CVSS:3.1/AV:N/AC:L/PR:H/UI:N/S:U/C:H/I:H/A:H",
        "fixState": "fixed",
    },
    {
        "name": "express",
        "version": "4.17.2",
        "fixedIn": "4.17.3",
        "manager": "NPM",
        "arch": "",
        "vulnId": "CVE-2022-24999",
        "severity": "HIGH",
        "cwe": "CWE-1321",
        "cvssScore": 7.5,
        "cvssVector": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:N/I:N/A:H",
        "fixState": "fixed",
    },
    {
        "name": "golang.org/x/net",
        "version": "0.5.0",
        "fixedIn": "0.7.0",
        "manager": "GOMOD",
        "arch": "",
        "vulnId": "CVE-2023-3978",
        "severity": "MEDIUM",
        "cwe": "CWE-79",
        "cvssScore": 6.1,
        "cvssVector": "CVSS:3.1/AV:N/AC:L/PR:N/UI:R/S:C/C:L/I:L/A:N",
        "fixState": "fixed",
    },
    {
        "name": "apk-tools",
        "version": "2.10.6",
        "fixedIn": "2.10.7",
        "manager": "OS",
        "arch": "x86_64",
        "vulnId": "CVE-2021-36159",
        "severity": "CRITICAL",
        "cwe": "CWE-787",
        "cvssScore": 9.8,
        "cvssVector": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
        "fixState": "fixed",
    },
    {
        "name": "zlib",
        "version": "1.2.12",
        "fixedIn": "1.2.13",
        "manager": "OS",
        "arch": "aarch64",
        "vulnId": "CVE-2022-37434",
        "severity": "CRITICAL",
        "cwe": "CWE-787",
        "cvssScore": 9.8,
        "cvssVector": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
        "fixState": "fixed",
    },
    {
        "name": "logback-core",
        "version": "1.2.11",
        "fixedIn": "1.2.13",
        "manager": "JAR",
        "arch": "",
        "vulnId": "CVE-2023-6481",
        "severity": "HIGH",
        "cwe": "CWE-400",
        "cvssScore": 7.5,
        "cvssVector": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:N/I:N/A:H",
        "fixState": "fixed",
    },
]

_ECR_IMAGE_NAMES = [
    "python:3.12-slim",
    "node:20-alpine",
    "golang:1.21-bullseye",
    "amazonlinux:2023",
    "ubuntu:22.04",
]

FIX_STATE_MAP = {"fixed": "YES", "not-fixed": "NO", "wont-fix": "NO", "unknown": "NO"}


def _default_config():
    return {
        "ec2": {"status": "DISABLED"},
        "ecr": {"status": "DISABLED"},
        "lambda": {"status": "DISABLED"},
        "lambdaCode": {"status": "DISABLED"},
        "code": {"status": "DISABLED"},
    }


def _get_config(account_id=None):
    if account_id is None:
        account_id = get_account_id()
    if account_id not in _account_config:
        _account_config[account_id] = _default_config()
    return _account_config[account_id]


def _build_finding(pkg, resource_type, resource_id, account_id):
    fix = pkg.get("fixState", "unknown")
    fixed_in = pkg["fixedIn"] if fix == "fixed" else ""
    finding_id = uuid.uuid4().hex[:16]

    resource_details = {}
    if resource_type == "AWS_ECR_CONTAINER_IMAGE":
        repo, _, tag = resource_id.partition(":")
        resource_details = {
            "awsEcrContainerImage": {
                "repositoryName": repo,
                "imageTags": [tag] if tag else [],
                "registry": account_id,
                "imageHash": "sha256:" + uuid.uuid4().hex,
                "architecture": pkg.get("arch", ""),
                "platform": "linux",
                "pushedAt": _now_iso(),
            }
        }
    elif resource_type == "AWS_LAMBDA_FUNCTION":
        resource_details = {
            "awsLambdaFunction": {
                "functionName": resource_id,
                "runtime": "PYTHON_3_10",
                "codeSha256": uuid.uuid4().hex,
                "version": "$LATEST",
                "executionRoleArn": f"arn:aws:iam::{account_id}:role/service-role/test-role",
                "packageType": "ZIP",
                "lastModifiedAt": _now_iso(),
            }
        }
    elif resource_type == "AWS_EC2_INSTANCE":
        resource_details = {
            "awsEc2Instance": {
                "type": "t3.medium",
                "imageId": "ami-0abcdef1234567890",
                "platform": "linux",
                "launchedAt": _now_iso(),
            }
        }

    risk = pkg["cvssScore"] / 10.0
    return {
        "findingArn": _finding_arn(account_id, finding_id),
        "awsAccountId": account_id,
        "type": "PACKAGE_VULNERABILITY",
        "title": pkg["vulnId"],
        "description": f"{pkg['vulnId']} — {pkg['cwe']} — in {pkg['name']}@{pkg['version']}",
        "severity": pkg["severity"],
        "status": "ACTIVE",
        "firstObservedAt": _now_iso(),
        "lastObservedAt": _now_iso(),
        "updatedAt": _now_iso(),
        "inspectorScore": round(risk, 2),
        "fixAvailable": FIX_STATE_MAP.get(fix, "NO"),
        "exploitAvailable": "NO" if pkg["severity"] != "CRITICAL" else "YES",
        "resources": [
            {
                "type": resource_type,
                "id": resource_id,
                "partition": "aws",
                "region": get_region(),
                "tags": {},
                "details": resource_details,
            }
        ],
        "packageVulnerabilityDetails": {
            "vulnerabilityId": pkg["vulnId"],
            "source": "NVD",
            "sourceUrl": f"https://nvd.nist.gov/vuln/detail/{pkg['vulnId']}",
            "vendorSeverity": pkg["severity"],
            "vendorCreatedAt": _now_iso(),
            "vendorUpdatedAt": _now_iso(),
            "referenceUrls": [f"https://nvd.nist.gov/vuln/detail/{pkg['vulnId']}"],
            "relatedVulnerabilities": [],
            "cvss": [
                {
                    "baseScore": pkg["cvssScore"],
                    "scoringVector": pkg["cvssVector"],
                    "version": "3.1",
                    "source": "NVD",
                }
            ],
            "vulnerablePackages": [
                {
                    "name": pkg["name"],
                    "version": pkg["version"],
                    "epoch": 0,
                    "release": "",
                    "arch": pkg.get("arch", ""),
                    "packageManager": pkg["manager"],
                    "filePath": "",
                    "sourceLayerHash": "",
                    "sourceLambdaLayerArn": "",
                    "fixedInVersion": fixed_in,
                    "remediation": f"Upgrade {pkg['name']} to {fixed_in}" if fixed_in else "",
                }
            ],
        },
        "inspectorScoreDetails": {
            "adjustedCvss": {
                "scoreSource": "Inspector",
                "cvssSource": "NVD",
                "version": "3.1",
                "score": pkg["cvssScore"],
                "scoringVector": pkg["cvssVector"],
                "adjustments": [],
            }
        },
        "remediation": {
            "recommendation": {
                "text": f"Upgrade {pkg['name']} to {pkg['fixedIn']}"
                if fix == "fixed"
                else f"Consider mitigating {pkg['vulnId']} in {pkg['name']}",
                "Url": f"https://nvd.nist.gov/vuln/detail/{pkg['vulnId']}",
            }
        },
    }


def _generate_findings(account_id):
    config = _get_config(account_id)
    findings = []

    if config.get("ecr", {}).get("status") == "ENABLED":
        for img in _ECR_IMAGE_NAMES:
            for pkg in _STUB_PACKAGES[:8]:
                findings.append(_build_finding(pkg, "AWS_ECR_CONTAINER_IMAGE", img, account_id))
    if config.get("lambda", {}).get("status") == "ENABLED":
        for func in ("my-lambda-py", "api-handler", "worker-processor"):
            for pkg in _STUB_PACKAGES[4:10]:
                findings.append(_build_finding(pkg, "AWS_LAMBDA_FUNCTION", func, account_id))
    if config.get("ec2", {}).get("status") == "ENABLED":
        for instance in ("i-0abc123def4567890", "i-0fed321cba0987654"):
            for pkg in _STUB_PACKAGES[:6]:
                findings.append(_build_finding(pkg, "AWS_EC2_INSTANCE", instance, account_id))

    return findings


def _run_scan(account_id):
    config = _get_config(account_id)
    any_enabled = any(config[k]["status"] == "ENABLED" for k in ("ecr", "lambda", "ec2"))
    if not any_enabled:
        return

    findings = _generate_findings(account_id)
    resources = set()

    for f in findings:
        for r in f["resources"]:
            resources.add((r["type"], r["id"]))

    timestamp = _now_iso()
    _findings[account_id] = findings
    _scan_history[account_id] = {
        "lastScanAt": timestamp,
        "resourceTypes": list({k for k in ("ecr", "lambda", "ec2") if config[k]["status"] == "ENABLED"}),
        "resourcesScanned": len(resources),
        "findingsCount": len(findings),
    }

    coverage = []
    for rtype, rid in sorted(resources):
        coverage.append(
            {
                "accountId": account_id,
                "resourceId": rid,
                "resourceType": rtype,
                "scanType": "PACKAGE",
                "scanStatus": {"code": "ACTIVE", "reason": "INITIAL_SCAN_COMPLETE"},
                "lastScannedAt": timestamp,
                "scanMode": "EC2_AGENTLESS",
            }
        )
    _coverage[account_id] = coverage

    logger.info(
        "Inspector2 stub scan: account=%s resources=%d findings=%d",
        account_id,
        len(resources),
        len(findings),
    )


def _scan_if_needed(account_id):
    config = _get_config(account_id)
    enabled_types = {k for k in ("ecr", "lambda", "ec2") if config[k]["status"] == "ENABLED"}
    if not enabled_types:
        return

    last_scan = _scan_history.get(account_id, {})
    last_types = set(last_scan.get("resourceTypes", []))
    if not _findings.get(account_id) or enabled_types != last_types:
        _run_scan(account_id)


def _matches_filter(finding, filter_criteria):
    if not filter_criteria:
        return True

    for field, conditions in filter_criteria.items():
        if not conditions:
            continue

        if field == "severity":
            matched = False
            for cond in conditions:
                comparison = cond.get("comparison", "EQUALS")
                value = cond.get("value", "")
                if comparison == "EQUALS" and finding.get("severity") == value:
                    matched = True
                    break
                if comparison == "NOT_EQUALS" and finding.get("severity") != value:
                    matched = True
                    break
            if not matched:
                return False

        elif field == "findingStatus":
            matched = False
            for cond in conditions:
                comparison = cond.get("comparison", "EQUALS")
                value = cond.get("value", "")
                if comparison == "EQUALS" and finding.get("status") == value:
                    matched = True
                    break
                if comparison == "NOT_EQUALS" and finding.get("status") != value:
                    matched = True
                    break
            if not matched:
                return False

        elif field == "findingType":
            matched = False
            for cond in conditions:
                comparison = cond.get("comparison", "EQUALS")
                value = cond.get("value", "")
                if comparison == "EQUALS" and finding.get("type") == value:
                    matched = True
                    break
                if comparison == "NOT_EQUALS" and finding.get("type") != value:
                    matched = True
                    break
            if not matched:
                return False

        elif field == "resourceType":
            matched = False
            for cond in conditions:
                comparison = cond.get("comparison", "EQUALS")
                value = cond.get("value", "")
                if comparison == "EQUALS":
                    for r in finding.get("resources", []):
                        if r.get("type") == value:
                            matched = True
                            break
                if comparison == "NOT_EQUALS":
                    has_match = False
                    for r in finding.get("resources", []):
                        if r.get("type") == value:
                            has_match = True
                            break
                    if not has_match:
                        matched = True
                if matched:
                    break
            if not matched:
                return False

        elif field == "vulnerabilityId":
            matched = False
            for cond in conditions:
                comparison = cond.get("comparison", "EQUALS")
                value = cond.get("value", "")
                pvd = finding.get("packageVulnerabilityDetails", {})
                if comparison == "EQUALS" and pvd.get("vulnerabilityId") == value:
                    matched = True
                    break
                if comparison == "NOT_EQUALS" and pvd.get("vulnerabilityId") != value:
                    matched = True
                    break
            if not matched:
                return False

        elif field == "inspectorScore":
            matched = False
            for cond in conditions:
                lower = cond.get("lowerInclusive", 0)
                upper = cond.get("upperInclusive", 10)
                score = finding.get("inspectorScore", 0) * 10
                if lower <= score <= upper:
                    matched = True
                    break
            if not matched:
                return False

    return True


def _paginate(items, max_results, next_token):
    if not max_results or max_results <= 0:
        max_results = 100
    max_results = min(max_results, 100)

    start = 0
    if next_token:
        try:
            start = int(next_token)
        except (ValueError, TypeError):
            start = 0

    page = items[start : start + max_results]
    new_token = str(start + max_results) if start + max_results < len(items) else None
    return page, new_token


def _enable(data):
    account_ids = data.get("accountIds", []) or []
    resource_types = data.get("resourceTypes", [])

    if not account_ids:
        account_ids = [get_account_id()]

    result_accounts = []
    failed_accounts = []

    for aid in account_ids:
        config = _get_config(aid)
        for rt in resource_types:
            rt_key = rt.lower()
            if rt_key == "ec2":
                config["ec2"]["status"] = "ENABLED"
            elif rt_key == "ecr":
                config["ecr"]["status"] = "ENABLED"
            elif rt_key == "lambda":
                config["lambda"]["status"] = "ENABLED"
            elif rt_key == "lambda_code":
                config["lambdaCode"]["status"] = "ENABLED"
            elif rt_key == "code":
                config["code"]["status"] = "ENABLED"

        result_accounts.append(
            {
                "accountId": aid,
                "resourceStatus": {
                    "ec2": config["ec2"]["status"],
                    "ecr": config["ecr"]["status"],
                    "lambda": config["lambda"]["status"],
                    "lambdaCode": config["lambdaCode"]["status"],
                },
                "status": "ENABLED",
            }
        )

    return json_response(
        {
            "accounts": result_accounts,
            "failedAccounts": failed_accounts,
        }
    )


def _disable(data):
    account_ids = data.get("accountIds", []) or []
    resource_types = data.get("resourceTypes", [])

    if not account_ids:
        account_ids = [get_account_id()]

    result_accounts = []
    failed_accounts = []

    for aid in account_ids:
        config = _get_config(aid)

        if not resource_types:
            for k in config:
                config[k]["status"] = "DISABLED"
        else:
            for rt in resource_types:
                rt_key = rt.lower()
                if rt_key in config:
                    config[rt_key]["status"] = "DISABLED"

        result_accounts.append(
            {
                "accountId": aid,
                "resourceStatus": {
                    "ec2": config["ec2"]["status"],
                    "ecr": config["ecr"]["status"],
                    "lambda": config["lambda"]["status"],
                    "lambdaCode": config["lambdaCode"]["status"],
                },
                "status": "DISABLED",
            }
        )

    return json_response(
        {
            "accounts": result_accounts,
            "failedAccounts": failed_accounts,
        }
    )


def _list_findings(data, account_id):
    config = _get_config(account_id)
    any_enabled = any(config[k]["status"] == "ENABLED" for k in ("ecr", "lambda", "ec2"))
    if not any_enabled:
        return json_response({"findings": []})

    _scan_if_needed(account_id)

    filter_criteria = data.get("filterCriteria", {})
    max_results = data.get("maxResults", 100)
    next_token = data.get("nextToken")
    sort_criteria = data.get("sortCriteria", {})

    all_findings = _findings.get(account_id, [])

    filtered = [f for f in all_findings if _matches_filter(f, filter_criteria)]

    if sort_criteria:
        field = sort_criteria.get("field", "")
        order = sort_criteria.get("sortOrder", "ASC")
        if field == "SEVERITY":
            sev_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3, "INFORMATIONAL": 4, "UNTRIAGED": 5}
            filtered.sort(key=lambda f: sev_order.get(f.get("severity", ""), 99), reverse=(order == "DESC"))
        elif field == "FIRST_OBSERVED_AT":
            filtered.sort(key=lambda f: f.get("firstObservedAt", ""), reverse=(order == "DESC"))
        elif field == "INSPECTOR_SCORE":
            filtered.sort(key=lambda f: f.get("inspectorScore", 0), reverse=(order == "DESC"))

    page, new_token = _paginate(filtered, max_results, next_token)

    response = {"findings": page}
    if new_token:
        response["nextToken"] = new_token

    return json_response(response)


def _list_coverage(data, account_id):
    filter_criteria = data.get("filterCriteria", {})
    max_results = data.get("maxResults", 100)
    next_token = data.get("nextToken")

    all_coverage = _coverage.get(account_id, [])

    if filter_criteria:
        scan_status_filter = filter_criteria.get("scanStatusCode", [])
        if scan_status_filter:
            filtered = []
            for cov in all_coverage:
                status_code = cov.get("scanStatus", {}).get("code", "")
                for cond in scan_status_filter:
                    comparison = cond.get("comparison", "EQUALS")
                    value = cond.get("value", "")
                    if comparison == "EQUALS" and status_code == value:
                        filtered.append(cov)
                        break
                    elif comparison == "NOT_EQUALS" and status_code != value:
                        filtered.append(cov)
                        break
            all_coverage = filtered

    page, new_token = _paginate(all_coverage, max_results, next_token)

    response = {"coveredResources": page}
    if new_token:
        response["nextToken"] = new_token

    return json_response(response)


def _list_coverage_statistics(data, account_id):
    all_coverage = _coverage.get(account_id, [])

    counts_by_resource = {}
    counts_by_scan = {}

    for cov in all_coverage:
        rt = cov.get("resourceType", "UNKNOWN")
        sc = cov.get("scanStatus", {}).get("code", "UNKNOWN")
        counts_by_resource[rt] = counts_by_resource.get(rt, 0) + 1
        counts_by_scan[sc] = counts_by_scan.get(sc, 0) + 1

    counts_by_group = [
        {
            "countBy": "RESOURCE_TYPE",
            "counts": [{"key": k, "count": v} for k, v in counts_by_resource.items()],
        },
        {
            "countBy": "SCAN_STATUS_CODE",
            "counts": [{"key": k, "count": v} for k, v in counts_by_scan.items()],
        },
    ]

    return json_response(
        {
            "countsByGroup": counts_by_group,
            "totalCounts": sum(1 for _ in all_coverage),
        }
    )


def _list_finding_aggregations(data, account_id):
    aggregation_type = data.get("aggregationType", "FINDING_TYPE")
    all_findings = _findings.get(account_id, [])

    if aggregation_type == "FINDING_TYPE":
        by_type = {}
        for f in all_findings:
            ft = f.get("type", "UNKNOWN")
            by_type[ft] = by_type.get(ft, 0) + 1
        return json_response(
            {
                "aggregationType": aggregation_type,
                "responses": [{"findingTypeAggregation": {"severityCounts": {"all": v}}} for v in by_type.values()],
            }
        )

    elif aggregation_type == "ACCOUNT":
        by_acct = {}
        for f in all_findings:
            aid = f.get("awsAccountId", "UNKNOWN")
            by_acct[aid] = by_acct.get(aid, 0) + 1
        return json_response(
            {
                "aggregationType": aggregation_type,
                "responses": [{"accountAggregation": {"severityCounts": {"all": v}}} for v in by_acct.values()],
            }
        )

    return json_response({"aggregationType": aggregation_type, "responses": []})


def _search_vulnerabilities(data, account_id):
    filter_criteria = data.get("filterCriteria", {})
    vulnerability_ids = filter_criteria.get("vulnerabilityIds", []) if isinstance(filter_criteria, dict) else []
    if not vulnerability_ids:
        return json_response({"vulnerabilities": []})

    all_findings = _findings.get(account_id, [])

    seen_ids = set()
    vulnerabilities = []
    for f in all_findings:
        pvd = f.get("packageVulnerabilityDetails", {})
        vid = pvd.get("vulnerabilityId", "")
        if vid in vulnerability_ids and vid not in seen_ids:
            seen_ids.add(vid)
            cvss_entry = pvd.get("cvss", [{}])[0] if pvd.get("cvss") else {}
            base_score = cvss_entry.get("baseScore", 0.0)
            scoring_vector = cvss_entry.get("scoringVector", "")
            vulnerabilities.append(
                {
                    "id": vid,
                    "source": pvd.get("source", ""),
                    "description": f.get("description", ""),
                    "vendorSeverity": pvd.get("vendorSeverity", ""),
                    "vendorCreatedAt": pvd.get("vendorCreatedAt", _now_iso()),
                    "vendorUpdatedAt": pvd.get("vendorUpdatedAt", _now_iso()),
                    "sourceUrl": pvd.get("sourceUrl", ""),
                    "referenceUrls": pvd.get("referenceUrls", []),
                    "relatedVulnerabilities": pvd.get("relatedVulnerabilities", []),
                    "atigData": {
                        "firstSeen": pvd.get("vendorCreatedAt", _now_iso()),
                        "lastSeen": pvd.get("vendorUpdatedAt", _now_iso()),
                    },
                    "cvss4": {"baseScore": base_score, "scoringVector": scoring_vector},
                    "cvss3": {"baseScore": base_score, "scoringVector": scoring_vector},
                    "cvss2": {"baseScore": 0.0, "scoringVector": ""},
                    "cisaData": {"dateAdded": _now_iso(), "dateDue": _now_iso(), "action": ""},
                    "exploitObserved": {"lastSeen": _now_iso(), "firstSeen": _now_iso()},
                    "detectionPlatforms": [],
                    "epss": {"score": 0.0},
                    "cwes": [],
                }
            )

    return json_response({"vulnerabilities": vulnerabilities})


def _batch_get_finding_details(data, account_id):
    finding_arns = data.get("findingArns", [])
    all_findings = _findings.get(account_id, [])

    found = {}
    errors = []
    for f in all_findings:
        if f["findingArn"] in finding_arns:
            found[f["findingArn"]] = f

    for arn in finding_arns:
        if arn not in found:
            errors.append({"arn": arn, "errorCode": "RESOURCE_NOT_FOUND", "errorMessage": "Finding not found"})

    return json_response({"findingDetails": list(found.values()), "errors": errors})


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def _tag_resource(data, account_id):
    arn = data.get("resourceArn", "")
    tags = data.get("tags", {})
    if not arn:
        return error_response_json("ValidationException", "resourceArn is required", 400)

    existing = _tags.get(account_id, {}).get(arn, {})
    existing.update(tags)
    if account_id not in _tags:
        _tags[account_id] = {}
    _tags[account_id][arn] = existing
    return json_response({})


def _untag_resource(data, account_id):
    arn = data.get("resourceArn", "")
    tag_keys = data.get("tagKeys", [])
    if not arn:
        return error_response_json("ValidationException", "resourceArn is required", 400)

    existing = _tags.get(account_id, {}).get(arn, {})
    for key in tag_keys:
        existing.pop(key, None)
    if account_id not in _tags:
        _tags[account_id] = {}
    _tags[account_id][arn] = existing
    return json_response({})


def _list_tags_for_resource(data, account_id):
    arn = data.get("resourceArn", "")
    if not arn:
        return error_response_json("ValidationException", "resourceArn is required", 400)

    tags = _tags.get(account_id, {}).get(arn, {})
    return json_response({"tags": tags})


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


def _filter_arn(account_id, filter_name):
    return f"arn:aws:inspector2:{get_region()}:{account_id}:filter/{filter_name}"


def _create_filter(data, account_id):
    name = data.get("name", "")
    action = data.get("action", "NONE")
    filter_criteria = data.get("filterCriteria", {})
    description = data.get("description", "")

    if not name:
        return error_response_json("ValidationException", "name is required", 400)

    if account_id not in _filters:
        _filters[account_id] = {}

    if name in _filters[account_id]:
        return error_response_json("ConflictException", f"Filter {name} already exists", 409)

    filt = {
        "arn": _filter_arn(account_id, name),
        "name": name,
        "action": action,
        "findingCriteria": filter_criteria,
        "description": description,
    }
    _filters[account_id][name] = filt
    return json_response(filt)


def _list_filters(data, account_id):
    all_filters = list(_filters.get(account_id, {}).values())
    return json_response({"filters": all_filters})


def _delete_filter(data, account_id):
    arn = data.get("arn", "")
    if not arn:
        return error_response_json("ValidationException", "arn is required", 400)

    # Extract filter name from ARN: arn:aws:inspector2:...:filter/<name>
    name = arn.split("/")[-1] if "/" in arn else ""
    account_filters = _filters.get(account_id, {})

    if name not in account_filters:
        return error_response_json(
            "ResourceNotFoundException",
            f"The filter with arn '{arn}' does not exist",
            400,
        )

    del account_filters[name]
    return json_response({})


_path_handlers = {
    "/enable": _enable,
    "/disable": _disable,
    "/findings/list": _list_findings,
    "/findings/details/batch/get": _batch_get_finding_details,
    "/coverage/list": _list_coverage,
    "/coverage/statistics/list": _list_coverage_statistics,
    "/findings/aggregation/list": _list_finding_aggregations,
    "/vulnerabilities/search": _search_vulnerabilities,
    "/tags/tag": _tag_resource,
    "/tags/untag": _untag_resource,
    "/tags/list": _list_tags_for_resource,
    "/filters/create": _create_filter,
    "/filters/list": _list_filters,
    "/filters/delete": _delete_filter,
}

_async_handlers = {}


async def handle_request(method, path, headers, body, query_params):
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    # Merge query parameters into data (boto3 REST-JSON sends some params as query strings)
    if query_params:
        for key, value in query_params.items():
            if key not in data:
                data[key] = value

    account_id = get_account_id()

    # Inspector2 tag routes embed the ARN in the path: /tags/{resourceArn}
    if path.startswith("/tags/"):
        arn = path[len("/tags/") :]
        if not data.get("resourceArn"):
            data["resourceArn"] = arn
        if method == "POST":
            return _tag_resource(data, account_id)
        if method == "DELETE":
            # tagKeys may come via query params (already merged above)
            return _untag_resource(data, account_id)
        if method == "GET":
            return _list_tags_for_resource(data, account_id)
        return error_response_json("InvalidAction", f"Unknown method for /tags: {method}", 400)

    handler = _path_handlers.get(path)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown path: {path}", 400)

    try:
        if path in _async_handlers:
            return await handler(data, account_id)
        else:
            return handler(data, account_id) if "account_id" in handler.__code__.co_varnames else handler(data)
    except Exception as e:
        logger.exception("Error handling %s: %s", path, e)
        return error_response_json("InternalServerError", str(e), 500)


def get_state():
    return {
        "account_config": copy.deepcopy(dict(_account_config._data)),
        "findings": copy.deepcopy(dict(_findings._data)),
        "coverage": copy.deepcopy(dict(_coverage._data)),
        "scan_history": copy.deepcopy(dict(_scan_history._data)),
        "tags": copy.deepcopy(dict(_tags._data)),
        "filters": copy.deepcopy(dict(_filters._data)),
    }


def restore_state(data):
    if not data:
        return
    acc_config = data.get("account_config", {})
    if acc_config:
        _account_config._data.update(acc_config)
    findings = data.get("findings", {})
    if findings:
        _findings._data.update(findings)
    coverage = data.get("coverage", {})
    if coverage:
        _coverage._data.update(coverage)
    scan_history = data.get("scan_history", {})
    if scan_history:
        _scan_history._data.update(scan_history)
    tags = data.get("tags", {})
    if tags:
        _tags._data.update(tags)
    filters_data = data.get("filters", {})
    if filters_data:
        _filters._data.update(filters_data)


def reset():
    _account_config.clear()
    _findings.clear()
    _coverage.clear()
    _scan_history.clear()
    _tags.clear()
    _filters.clear()


try:
    _restored = load_state("inspector2")
    if _restored:
        restore_state(_restored)
except Exception:
    logging.getLogger(__name__).exception("Failed to restore persisted state; continuing with fresh store")
