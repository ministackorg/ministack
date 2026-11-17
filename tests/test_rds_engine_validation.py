"""Engine and typed-integer validation on the RDS create and modify paths (issue 2637).

Every request in CAPTURED is the body sent to real AWS (us-east-2), minus Version and with the
account ARN replaced; the expected status, Code, Type and Message are what AWS answered.  The
other tests send hand-built bodies for the same behaviours.

Regression guards: these tests also pass on the merge base and only protect accepted behaviour
from the new checks: test_rds_instance_accepts_policy_engines_as_metadata_only (the six params
oracle-se2-cdb, db2-ce, sqlserver-dev-ee and the three custom-sqlserver-* engines),
test_rds_multi_az_mysql_cluster_stays_in_the_store,
test_rds_describe_option_group_options_retired_oracle_without_major_is_accepted (all four params),
test_rds_engine_filter_matches_stored_instance_case_insensitively[postgres], the describe keys
r08-orderable-upper and r08-versions-blank, the r42-g-replica-source-missing key, and these
test_rds_pass4_coverage_matches_capture keys: r29-echo-upper-known-* (six), r26-option-groups-docdb,
r26-orderable-custom-sqlserver-{ee,web-class-bad} and r30-standalone-version-plain.
"""

import uuid
import xml.etree.ElementTree as ET

import pytest
import requests
from conftest import ENDPOINT

_AUTH = {"Authorization": "AWS4-HMAC-SHA256 Credential=test/20260930/us-east-1/rds/aws4_request"}

# DB subnet groups that already existed in the capture account. The captured bodies name them,
# and CreateDBCluster/CreateDBInstance refuse a subnet group that does not exist.
_CAPTURE_SUBNET_GROUPS = (
    "msc-2633w-sng",
    "terraform-598b16a546e6a6786032a7c370",
    "terraform-58a8f1c94a62cad74429e9314c",
)


@pytest.fixture(autouse=True)
def _capture_subnet_groups(ec2, rds):
    vpc_id = ec2.describe_vpcs()["Vpcs"][0]["VpcId"]
    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])["Subnets"]
    subnet_ids = [subnet["SubnetId"] for subnet in subnets][:2]
    existing = {group["DBSubnetGroupName"] for group in rds.describe_db_subnet_groups()["DBSubnetGroups"]}
    for name in _CAPTURE_SUBNET_GROUPS:
        if name not in existing:
            rds.create_db_subnet_group(
                DBSubnetGroupName=name, DBSubnetGroupDescription="capture account", SubnetIds=subnet_ids,
            )
_NO_MESSAGE = None
_I3 = "The input isn't valid. Input can't contain control characters."
_INTERNAL = "An internal error has occurred. Please try your query again at a later time."

CAPTURED = [
    ('r01-engine-typo', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r01-e-typo'), ('Engine', 'postgress'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r01-instance-engines'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r01-instance-engines'), ('DBSubnetGroupName', 'terraform-598b16a546e6a6786032a7c370')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r01-engine-aurora-retired', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r01-e-aurora-retired'), ('Engine', 'aurora'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r01-instance-engines'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r01-instance-engines'), ('DBSubnetGroupName', 'terraform-598b16a546e6a6786032a7c370')], (500, 'InternalFailure', 'Receiver', 'An internal error has occurred. Please try your query again at a later time.')),
    ('r02-tab-lead', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r02-tab-lead'), ('Engine', '\tpostgres'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r02-instance-engine-accepts'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r02-instance-engine-accepts'), ('DBSubnetGroupName', 'terraform-598b16a546e6a6786032a7c370')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r09-instance-engine-nul', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r09-w-nul'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r09-wire'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r09-wire'), ('DBSubnetGroupName', 'terraform-598b16a546e6a6786032a7c370'), ('Engine', 'postgress\x00')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r16-engine-nel-mid', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r16-22-e-nel'), ('Engine', 'post\x85gres'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r16-control-chars'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r16-control-chars'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r16-engine-zwsp-mid', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r16-28-e-zwsp'), ('Engine', 'post\u200bgres'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r16-control-chars'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r16-control-chars'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r16-engine-tab-id-missing', [('Action', 'CreateDBInstance'), ('Engine', 'postgres\t'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r16-control-chars'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r16-control-chars'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r09-instance-port-abc', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r09-n-port-abc'), ('Engine', 'postgress'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r09-wire'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r09-wire'), ('DBSubnetGroupName', 'terraform-598b16a546e6a6786032a7c370'), ('Port', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r09-instance-port-blank', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r09-n-port-blank'), ('Engine', 'postgress'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r09-wire'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r09-wire'), ('DBSubnetGroupName', 'terraform-598b16a546e6a6786032a7c370'), ('Port', '')], (400, 'MalformedInput', 'Sender', 'missing value for decimal type')),
    ('r15-port-up-2147483648', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r15-i68'), ('Engine', 'postgress'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r15-integer-bounds'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r15-integer-bounds'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('Port', '2147483648')], (400, 'MalformedInput', 'Sender', None)),
    ('r15-instance-id-missing-port-abc', [('Action', 'CreateDBInstance'), ('Engine', 'postgress'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r15-integer-bounds'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r15-integer-bounds'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('Port', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r04-engine-typo', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r04-e-typo'), ('Engine', 'postgress'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r04-cluster-order'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r04-cluster-order'), ('DBSubnetGroupName', 'terraform-598b16a546e6a6786032a7c370')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r04-engine-blank', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r04-e-blank'), ('Engine', ''), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r04-cluster-order'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r04-cluster-order'), ('DBSubnetGroupName', 'terraform-598b16a546e6a6786032a7c370')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r04-engine-missing', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r04-e-missing'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r04-cluster-order'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r04-cluster-order'), ('DBSubnetGroupName', 'terraform-598b16a546e6a6786032a7c370')], (400, 'InvalidParameterValue', 'Sender', "1 validation error detected: Value null at 'engine' failed to satisfy constraint: Member must not be null")),
    ('r04-engine-mariadb', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r04-e-mariadb'), ('Engine', 'mariadb'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r04-cluster-order'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r04-cluster-order'), ('DBSubnetGroupName', 'terraform-598b16a546e6a6786032a7c370')], (400, 'InvalidParameterCombination', 'Sender', "The DB engine version wasn't specified and Amazon RDS couldn't identify a valid DB engine version for the mariadb DB cluster. Submit a new request that specifies a valid DB engine version.")),
    ('r04-engine-aurora-retired', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r04-e-aurora-retired'), ('Engine', 'aurora'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r04-cluster-order'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r04-cluster-order'), ('DBSubnetGroupName', 'terraform-598b16a546e6a6786032a7c370')], (400, 'InvalidParameterValue', 'Sender', 'Engine version not found: oscar/null')),
    ('r14-aurora-version-blank', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r14-55-aurora-v-blank'), ('Engine', 'aurora'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r14-cluster-c2-engines'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r14-cluster-c2-engines'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version  for aurora')),
    ('r14-mariadb-version-default', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r14-24-mariadb-v-default'), ('Engine', 'mariadb'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r14-cluster-c2-engines'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r14-cluster-c2-engines'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '11.8.9')], (400, 'InvalidParameterCombination', 'Sender', "The specified DB engine version isn't valid for the mariadb DB cluster. Specify a valid DB engine version.")),
    ('r14-postgres-class-bad-no-storage', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r14-61-postgres-cls'), ('Engine', 'postgres'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r14-cluster-c2-engines'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r14-cluster-c2-engines'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('DBClusterInstanceClass', 'db.notaclass')], (400, 'InvalidParameterValue', 'Sender', 'AllocatedStorage is required.')),
    ('r14-postgres-storage-zero-no-class', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r14-60-postgres-st0'), ('Engine', 'postgres'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r14-cluster-c2-engines'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r14-cluster-c2-engines'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('AllocatedStorage', '0')], (400, 'InvalidParameterValue', 'Sender', 'DBClusterInstanceClass is required.')),
    ('r14-engine-mysql-title', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r14-21-mysql-title'), ('Engine', 'MySQL'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r14-cluster-c2-engines'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r14-cluster-c2-engines'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r16-cluster-engine-tab-mid', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r16-3-ce-tab'), ('Engine', 'aurora-post\tgresql'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r16-control-chars'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r16-control-chars'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r16-cluster-engine-tab-id-missing', [('Action', 'CreateDBCluster'), ('Engine', 'aurora-postgresql\t'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r16-control-chars'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r16-control-chars'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r04-global-engine-typo', [('Action', 'CreateGlobalCluster'), ('GlobalClusterIdentifier', 'msc2637-r04-g-typo'), ('Engine', 'postgress')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r05-global-engine-omitted', [('Action', 'CreateGlobalCluster'), ('GlobalClusterIdentifier', 'msc2637-r05-global-omitted')], (400, 'InvalidParameterValue', 'Sender', 'When creating standalone global cluster, value for engineName should be specified')),
    ('r05-global-engine-blank', [('Action', 'CreateGlobalCluster'), ('GlobalClusterIdentifier', 'msc2637-r05-global-engine-blank'), ('Engine', '')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r05-global-case-upper', [('Action', 'CreateGlobalCluster'), ('GlobalClusterIdentifier', 'msc2637-r05-global-case-upper'), ('Engine', 'AURORA-POSTGRESQL')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r04-global-engine-postgres', [('Action', 'CreateGlobalCluster'), ('GlobalClusterIdentifier', 'msc2637-r04-g-postgres'), ('Engine', 'postgres')], (400, 'InvalidParameterValue', 'Sender', 'The requested engine version was not found or does not support global functionality')),
    ('r16-global-engine-tab', [('Action', 'CreateGlobalCluster'), ('GlobalClusterIdentifier', 'msc2637-r16-g-engine'), ('Engine', 'aurora-postgresql\t'), ('EngineVersion', '99.99')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r09-versions-filter-index-gap', [('Action', 'DescribeDBEngineVersions'), ('Filters.Filter.2.Name', 'engine'), ('Filters.Filter.2.Values.Value.4', 'postgress')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized filter name: null')),
    ('r23-blank-filter-instances-engine-value-blank', [('Action', 'DescribeDBInstances'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', '')], (400, 'InvalidParameterCombination', 'Sender', "Invalid value '' for filter engine")),
    ('r29-versions-engine-and-filter-both-unknown', [('Action', 'DescribeDBEngineVersions'), ('Engine', 'postgresx'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'mysqlx')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgresx')),
    ('r35-f1-filter-index-minus1-unknown-instances', [('Action', 'DescribeDBInstances'), ('Filters.Filter.-1.Name', 'engine'), ('Filters.Filter.-1.Values.Value.1', 'postgress')], (400, 'MalformedInput', 'Sender', "Key -1 may not have a leading '-'")),
    ('r35-f1-filter-index-plus1-unknown-clusters', [('Action', 'DescribeDBClusters'), ('Filters.Filter.+1.Name', 'engine'), ('Filters.Filter.+1.Values.Value.1', 'postgress')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized filter name: null')),
    ('r35-f1-value-index-minus1-unknown-versions', [('Action', 'DescribeDBEngineVersions'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.-1', 'postgress')], (400, 'MalformedInput', 'Sender', "Key -1 may not have a leading '-'")),
    ('r35-f1-value-index-plus1-unknown-versions', [('Action', 'DescribeDBEngineVersions'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.+1', 'postgress')], (400, 'MalformedInput', 'Sender', 'Start of list found where not expected')),
    ('r37-b-filter-index-clusters-bis-12', [('Action', 'DescribeDBClusters'), ('Filters.Filter.12.Name', 'engine'), ('Filters.Filter.12.Values.Value.1', 'postgres')], (400, 'MalformedInput', 'Sender', 'Excessively sparse input would skip 11 list elements')),
    ('r37-b-filter-index-clusters-n1001', [('Action', 'DescribeDBClusters'), ('Filters.Filter.1001.Name', 'engine'), ('Filters.Filter.1001.Values.Value.1', 'postgres')], (400, 'MalformedInput', 'Sender', 'Excessively sparse input would skip 1,000 list elements')),
    ('r37-b-filter-index-instances-n4', [('Action', 'DescribeDBInstances'), ('Filters.Filter.4.Name', 'engine'), ('Filters.Filter.4.Values.Value.1', 'postgres')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized filter name: null')),
    ('r37-b-filter-index-versions-n999999999', [('Action', 'DescribeDBEngineVersions'), ('Filters.Filter.999999999.Name', 'engine'), ('Filters.Filter.999999999.Values.Value.1', 'postgres')], (400, 'MalformedInput', 'Sender', 'Index 999999999 is illegal')),
    ('r42-d-snapshots-filter-11', [('Action', 'DescribeDBSnapshots'), ('Filters.Filter.11.Name', 'engine'), ('Filters.Filter.11.Values.Value.1', 'postgres')], (400, 'InvalidParameterValue', 'Sender', None)),
    ('r42-d-snapshots-filter-plus-1', [('Action', 'DescribeDBSnapshots'), ('Filters.Filter.+1.Name', 'engine'), ('Filters.Filter.+1.Values.Value.1', 'postgres')], (400, 'InvalidParameterValue', 'Sender', None)),
    ('r42-d-cluster-snapshots-filter-11', [('Action', 'DescribeDBClusterSnapshots'), ('Filters.Filter.11.Name', 'engine'), ('Filters.Filter.11.Values.Value.1', 'postgres')], (400, 'InvalidParameterValue', 'Sender', None)),
    ('r42-d-cluster-snapshots-filter-plus-1', [('Action', 'DescribeDBClusterSnapshots'), ('Filters.Filter.+1.Name', 'engine'), ('Filters.Filter.+1.Values.Value.1', 'postgres')], (400, 'InvalidParameterValue', 'Sender', None)),
    ('r43-d-cluster-snapshots-filter-11', [('Action', 'DescribeDBClusterSnapshots'), ('Filters.Filter.11.Name', 'engine'), ('Filters.Filter.11.Values.Value.1', 'aurora-postgresql')], (400, 'InvalidParameterValue', 'Sender', None)),
    ('r37-g-monitoring-abc-create-cluster-id-missing', [('Action', 'CreateDBCluster'), ('Engine', 'postgress'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r37-open-gaps-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r37-open-gaps-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('MonitoringInterval', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r38-d-backtrack-abc-create-cluster-apg', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r38-4-d-backtrack-abc'), ('Engine', 'aurora-postgresql'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r38-open-gaps-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r38-open-gaps-existing'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '17.9'), ('BacktrackWindow', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r40-a-backtrack-i64-max-create-cluster-unknown', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r40-12-a-backtrack-i64-max'), ('Engine', 'postgress'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r40-i8v-w1x2-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r40-i8v-w1x2-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('BacktrackWindow', '9223372036854775807')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r42-f-modify-cluster-missing-backtrackwindow-abc', [('Action', 'ModifyDBCluster'), ('DBClusterIdentifier', 'msc2637-r42-no-such-cluster'), ('ApplyImmediately', 'true'), ('BacktrackWindow', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r35-sv2-create-unknown-min-blank', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-28-sv2-min-blank'), ('Engine', 'postgress'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('ServerlessV2ScalingConfiguration.MinCapacity', ''), ('ServerlessV2ScalingConfiguration.MaxCapacity', '1')], (400, 'MalformedInput', 'Sender', 'missing value for double type')),
    ('r35-sv2-create-unknown-max-blank', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-29-sv2-max-blank'), ('Engine', 'postgress'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('ServerlessV2ScalingConfiguration.MaxCapacity', ''), ('ServerlessV2ScalingConfiguration.MinCapacity', '0.5')], (400, 'MalformedInput', 'Sender', 'missing value for double type')),
    ('r37-h-sv2-create-unknown-min-nan-alt', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r37-48-sv2-nan-alt'), ('Engine', 'postgress'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r37-open-gaps-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r37-open-gaps-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('ServerlessV2ScalingConfiguration.MinCapacity', 'nAn'), ('ServerlessV2ScalingConfiguration.MaxCapacity', '2')], (400, 'MalformedInput', 'Sender', None)),
    ('r37-h-sv2-create-unknown-min-suffix-d-upper', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r37-50-sv2-suffix-d-upper'), ('Engine', 'postgress'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r37-open-gaps-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r37-open-gaps-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('ServerlessV2ScalingConfiguration.MinCapacity', '1D'), ('ServerlessV2ScalingConfiguration.MaxCapacity', '2')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r37-h-sv2-modify-missing-min-nan-alt', [('Action', 'ModifyDBCluster'), ('DBClusterIdentifier', 'msc2637-r37-no-such-cluster'), ('ApplyImmediately', 'true'), ('ServerlessV2ScalingConfiguration.MinCapacity', 'nAn'), ('ServerlessV2ScalingConfiguration.MaxCapacity', '2')], (400, 'MalformedInput', 'Sender', None)),
    ('r35-i5p-ose-id-trail-hyphen', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r35-ose-trail-'), ('Engine', 'oracle-se'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'The parameter DBInstanceIdentifier is not a valid identifier. Identifiers must begin with a letter; must contain only ASCII letters, digits, and hyphens; and must not end with a hyphen or contain two consecutive hyphens.')),
    ('r35-i5p-ose-id-underscore', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637_r35_ose'), ('Engine', 'oracle-se'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'The parameter DBInstanceIdentifier is not a valid identifier. Identifiers must begin with a letter; must contain only ASCII letters, digits, and hyphens; and must not end with a hyphen or contain two consecutive hyphens.')),
    ('r37-f-ose-id-double-hyphen', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'abad--id'), ('Engine', 'oracle-se'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r37-open-gaps-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r37-open-gaps-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'The parameter DBInstanceIdentifier is not a valid identifier. Identifiers must begin with a letter; must contain only ASCII letters, digits, and hyphens; and must not end with a hyphen or contain two consecutive hyphens.')),
    ('r35-i5p-ose-id-digit-first', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', '1msc2637-r35-ose'), ('Engine', 'oracle-se'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'Invalid database identifier:  1msc2637-r35-ose')),
    ('r35-i5p-ose-id-len-64', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r35-oselen64-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'), ('Engine', 'oracle-se'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'Invalid database identifier:  msc2637-r35-oselen64-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')),
    ('r35-i5p-ose-class-x9-huge', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r35-72-ose-x9-huge'), ('Engine', 'oracle-se'), ('DBInstanceClass', 'db.x9.huge'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB Instance class: db.x9.huge')),
    ('r35-i5p-ose-class-upper', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r35-75-ose-upper'), ('Engine', 'oracle-se'), ('DBInstanceClass', 'DB.T3.MICRO'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (500, 'InternalFailure', 'Receiver', 'An internal error has occurred. Please try your query again at a later time.')),
    ('r35-i5p-ose-class-serverless', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r35-76-ose-serverless'), ('Engine', 'oracle-se'), ('DBInstanceClass', 'db.serverless'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (500, 'InternalFailure', 'Receiver', 'An internal error has occurred. Please try your query again at a later time.')),
    ('r37-f-ose-class-blank', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r37-16-ose-cb'), ('Engine', 'oracle-se'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r37-open-gaps-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r37-open-gaps-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('DBInstanceClass', '')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB Instance class: ')),
    ('r35-i5p-aurora-class-x9-huge', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r35-92-aurora-x9-huge'), ('Engine', 'aurora'), ('DBInstanceClass', 'db.x9.huge'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB Instance class: db.x9.huge')),
    ('r35-i5p-aurora-class-serverless', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r35-94-aurora-serverless'), ('Engine', 'aurora'), ('DBInstanceClass', 'db.serverless'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (500, 'InternalFailure', 'Receiver', 'An internal error has occurred. Please try your query again at a later time.')),
    ('r35-i5p-aurora-id-len-64', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r35-auroralen64-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'), ('Engine', 'aurora'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'Invalid database identifier:  msc2637-r35-auroralen64-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')),
    ('r35-i5p-aurora-id-trail-hyphen', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r35-aurora-trail-'), ('Engine', 'aurora'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'The parameter DBInstanceIdentifier is not a valid identifier. Identifiers must begin with a letter; must contain only ASCII letters, digits, and hyphens; and must not end with a hyphen or contain two consecutive hyphens.')),
    ('r37-f-aurora-id-double-hyphen', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'abad--id'), ('Engine', 'aurora'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r37-open-gaps-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r37-open-gaps-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'The parameter DBInstanceIdentifier is not a valid identifier. Identifiers must begin with a letter; must contain only ASCII letters, digits, and hyphens; and must not end with a hyphen or contain two consecutive hyphens.')),
    ('r37-f-aurora-id-underscore-double-hyphen', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'a_b--c'), ('Engine', 'aurora'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r37-open-gaps-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r37-open-gaps-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'The parameter DBInstanceIdentifier is not a valid identifier. Identifiers must begin with a letter; must contain only ASCII letters, digits, and hyphens; and must not end with a hyphen or contain two consecutive hyphens.')),
    ('r37-f-aurora-class-blank', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r37-17-aurora-cb'), ('Engine', 'aurora'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r37-open-gaps-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r37-open-gaps-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('DBInstanceClass', '')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB Instance class: ')),
    ('r42-f-docdb-create-storagethroughput-abc-no-cluster', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r42-19-f-storag'), ('Engine', 'docdb'), ('DBInstanceClass', 'db.t3.medium'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r42-open-gaps2-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r42-open-gaps2-refused'), ('StorageThroughput', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r42-f-docdb-create-storagethroughput-abc-cluster-missing', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r42-20-f-storag'), ('Engine', 'docdb'), ('DBInstanceClass', 'db.t3.medium'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r42-open-gaps2-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r42-open-gaps2-refused'), ('DBClusterIdentifier', 'msc2637-r42-no-such-cluster'), ('StorageThroughput', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r42-f-docdb-create-maxallocatedstorage-abc-no-cluster', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r42-21-f-maxall'), ('Engine', 'docdb'), ('DBInstanceClass', 'db.t3.medium'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r42-open-gaps2-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r42-open-gaps2-refused'), ('MaxAllocatedStorage', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r42-f-docdb-create-maxallocatedstorage-abc-cluster-missing', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r42-22-f-maxall'), ('Engine', 'docdb'), ('DBInstanceClass', 'db.t3.medium'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r42-open-gaps2-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r42-open-gaps2-refused'), ('DBClusterIdentifier', 'msc2637-r42-no-such-cluster'), ('MaxAllocatedStorage', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r35-sv2-create-unknown-min-lead-space', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-30-sv2-min-lead-space'), ('Engine', 'postgress'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('ServerlessV2ScalingConfiguration.MinCapacity', ' 1'), ('ServerlessV2ScalingConfiguration.MaxCapacity', '1')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r26-create-option-group-oracle-se-major-blank', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r26-og-oracle-se-mb'), ('EngineName', 'oracle-se'), ('MajorEngineVersion', ''), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r26-orderable-option-group-engines'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r26-orderable-option-group-engines')], (400, 'InvalidParameterValue', 'Sender', 'The parameter MajorEngineVersion must be provided and must not be blank.')),
    ('r26-create-option-group-oracle-se1-major-blank', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r26-og-oracle-se1-mb'), ('EngineName', 'oracle-se1'), ('MajorEngineVersion', ''), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r26-orderable-option-group-engines'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r26-orderable-option-group-engines')], (400, 'InvalidParameterValue', 'Sender', 'The parameter MajorEngineVersion must be provided and must not be blank.')),
    ('r38-c-og-create-engine-soh', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r38-og-soh'), ('EngineName', 'post\x01gres'), ('MajorEngineVersion', '16'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r38-open-gaps-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r38-open-gaps-existing')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r38-c-og-dup-engine-soh', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r38-og-pg15'), ('EngineName', 'post\x01gres'), ('MajorEngineVersion', '16'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r38-open-gaps-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r38-open-gaps-existing')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r42-j-create-global-del-no-id', [('Action', 'CreateGlobalCluster'), ('Engine', 'aurora\x7f')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r29-versions-engine-and-filter-param-known', [('Action', 'DescribeDBEngineVersions'), ('Engine', 'postgres'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'postgress')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgress')),
    ('r29-versions-engine-and-filter-param-unknown', [('Action', 'DescribeDBEngineVersions'), ('Engine', 'postgress'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'postgres')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgress')),
    ('r42-c-instances-engine-no-values', [('Action', 'DescribeDBInstances'), ('Filters.Filter.1.Name', 'engine')], (400, 'InvalidParameterCombination', 'Sender', 'The values list cannot be null for the filter engine.')),
    ('r42-c-instances-engine-values-marker-blank', [('Action', 'DescribeDBInstances'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values', '')], (400, 'InvalidParameterCombination', 'Sender', 'The list of Engine Names must not be empty.')),
    ('r35-f1-filter-index-i32-minus1-unknown-instances', [('Action', 'DescribeDBInstances'), ('Filters.Filter.2147483646.Name', 'engine'), ('Filters.Filter.2147483646.Values.Value.1', 'postgress')], (400, 'MalformedInput', 'Sender', 'Index 2147483646 is illegal')),
    ('r35-f1-value-index-i32-max-unknown-instances', [('Action', 'DescribeDBInstances'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.2147483647', 'postgress')], (400, 'MalformedInput', 'Sender', 'Index 2147483647 is illegal')),
    ('r37-b-filter-index-versions-n10', [('Action', 'DescribeDBEngineVersions'), ('Filters.Filter.10.Name', 'engine'), ('Filters.Filter.10.Values.Value.1', 'postgres')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized filter name: null')),
    ('r37-b-filter-index-versions-n99', [('Action', 'DescribeDBEngineVersions'), ('Filters.Filter.99.Name', 'engine'), ('Filters.Filter.99.Values.Value.1', 'postgres')], (400, 'MalformedInput', 'Sender', 'Excessively sparse input would skip 98 list elements')),
]


def _post(pairs):
    body = pairs if isinstance(pairs, list) else list(pairs.items())
    response = requests.post(
        ENDPOINT, data=body, headers={**_AUTH, "Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    root = ET.fromstring(response.content)
    error = root.find(".//{*}Error")
    if error is None:
        return response.status_code, None, None, None, root
    return (
        response.status_code,
        error.findtext("{*}Code"),
        error.findtext("{*}Type"),
        error.findtext("{*}Message"),
        root,
    )


def _name(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.mark.parametrize("key,pairs,expected", CAPTURED, ids=[c[0] for c in CAPTURED])
def test_rds_engine_validation_matches_capture(key, pairs, expected):
    # The captured identifiers repeat across runs; nothing is stored on these errors.
    assert _post(pairs)[:4] == expected


_BASE_INSTANCE = {
    "Action": "CreateDBInstance",
    "DBInstanceClass": "db.t4g.micro",
    "AllocatedStorage": "20",
    "MasterUsername": "mscadmin",
    "MasterUserPassword": "MsCapture2637pw",
}


def _instance(db_id, **extra):
    return [*{**_BASE_INSTANCE, "DBInstanceIdentifier": db_id}.items(), *extra.items()]


def _delete_instance(db_id):
    _post({"Action": "DeleteDBInstance", "DBInstanceIdentifier": db_id, "SkipFinalSnapshot": "true"})


def test_rds_instance_unknown_engine_is_not_stored(rds):
    db_id = _name("eng-none")
    for extra in ({"Engine": "postgress"}, {"Engine": ""}, {}):
        status, code, kind, message, _ = _post(_instance(db_id, **extra))
        assert (status, code, kind, message) == (400, "InvalidParameterValue", "Sender", "Invalid DB engine")
    assert rds.describe_db_instances()["DBInstances"] == [
        i for i in rds.describe_db_instances()["DBInstances"] if i["DBInstanceIdentifier"] != db_id
    ]


def test_rds_instance_known_engine_any_case_is_stored_lower_case(rds):
    db_id = _name("eng-case")
    try:
        status, *_ = _post(_instance(db_id, Engine="ORACLE-EE"))
        assert status == 200
        instance = rds.describe_db_instances(DBInstanceIdentifier=db_id)["DBInstances"][0]
        assert instance["Engine"] == "oracle-ee"
        assert instance["DBParameterGroups"][0]["DBParameterGroupName"].startswith("default.oracle-ee")
    finally:
        _delete_instance(db_id)


@pytest.mark.parametrize("engine", [
    "oracle-se2-cdb", "db2-ce", "sqlserver-dev-ee", "custom-sqlserver-se",
    "custom-sqlserver-web", "custom-sqlserver-dev",
])
def test_rds_instance_accepts_policy_engines_as_metadata_only(engine, rds):
    db_id = _name("eng-p5")
    try:
        assert _post(_instance(db_id, Engine=engine))[0] == 200
        assert rds.describe_db_instances(DBInstanceIdentifier=db_id)["DBInstances"][0]["Engine"] == engine
    finally:
        _delete_instance(db_id)


def test_rds_instance_custom_oracle_ee_is_unknown():
    assert _post(_instance(_name("eng-coe"), Engine="custom-oracle-ee"))[:4] == (
        400, "InvalidParameterValue", "Sender", "Invalid DB engine")


def test_rds_instance_integer_members_accept_int32_edges(rds):
    db_id = _name("eng-int")
    try:
        status, *_ = _post(_instance(db_id, Engine="postgres", Port="+0005432"))
        # r24-w1-postgres-port-plus-5432: AWS 200. (2147483647 is out of the 1150-65535 range on AWS.)
        assert status == 200
    finally:
        _delete_instance(db_id)


@pytest.mark.parametrize("member", [
    "Port", "AllocatedStorage", "BackupRetentionPeriod", "Iops", "MonitoringInterval",
    "PromotionTier", "StorageThroughput", "MaxAllocatedStorage",
])
def test_rds_instance_integer_members_reject_out_of_range(member):
    pairs = [p for p in _instance(_name("eng-w1"), Engine="oracle-ee") if p[0] != member]
    assert _post([*pairs, (member, "2147483648")])[:4] == (400, "MalformedInput", "Sender", None)
    assert _post([*pairs, (member, "")])[:4] == (400, "MalformedInput", "Sender", "missing value for decimal type")
    assert _post([*pairs, (member, " 5")])[:4] == (400, "MalformedInput", "Sender", None)


def test_rds_cluster_integer_members_reject_non_integers():
    base = {"Action": "CreateDBCluster", "DBClusterIdentifier": _name("eng-cw1"), "Engine": "aurora-postgresql"}
    for member in ("Port", "BackupRetentionPeriod", "Iops"):
        assert _post({**base, member: "1.5"})[:4] == (400, "MalformedInput", "Sender", None)
        assert _post({**base, member: "-2147483649"})[:4] == (400, "MalformedInput", "Sender", None)


def test_rds_engine_control_characters_run_before_identifier_checks():
    for action in ("CreateDBInstance", "CreateDBCluster", "ModifyDBInstance"):
        assert _post({"Action": action, "Engine": "postgres"})[:4] == (
            400, "InvalidParameterValue", "Sender", _I3)
    assert _post({"Action": "CreateGlobalCluster", "GlobalClusterIdentifier": _name("g"),
                  "Engine": "aurora"})[:4] == (400, "InvalidParameterValue", "Sender", _I3)


def test_rds_modify_instance_engine_change_is_refused(rds):
    db_id = _name("eng-mod")
    try:
        assert _post(_instance(db_id, Engine="oracle-ee"))[0] == 200

        def modify(**extra):
            return _post({"Action": "ModifyDBInstance", "DBInstanceIdentifier": db_id, **extra})[:4]

        for same in ("", "oracle-ee", "ORACLE-EE"):
            assert modify(Engine=same)[0] == 200
        for requested, shown in (
            ("mysql", "mysql"), ("POSTGRESS", "postgress"), ("aurora-postgresql", "manfred"),
            ("aurora", "oscar"), ("neptune", "graphdb"), ("docdb", "chimera"),
            ("custom-oracle-ee", "flex-oracle-ee"), (" postgress", " postgress"),
        ):
            assert modify(Engine=requested) == (
                400, "InvalidParameterCombination", "Sender",
                f"Modification of database from engine oracle-ee to engine {shown} is not supported.")
        # The engine is refused before the rename: nothing moves.
        assert modify(Engine="mysql", NewDBInstanceIdentifier=db_id + "-x") == (
            400, "InvalidParameterCombination", "Sender",
            "Modification of database from engine oracle-ee to engine mysql is not supported.")
        assert rds.describe_db_instances(DBInstanceIdentifier=db_id)["DBInstances"][0]["Engine"] == "oracle-ee"
        # The first repeated Engine wins.
        assert modify(Engine="oracle-ee")[0] == 200
        status, code, kind, message, _ = _post(
            [("Action", "ModifyDBInstance"), ("DBInstanceIdentifier", db_id), ("Engine", "mysql"), ("Engine", "oracle-ee")])
        assert (status, code, kind, message) == (
            400, "InvalidParameterCombination", "Sender",
            "Modification of database from engine oracle-ee to engine mysql is not supported.")
    finally:
        _delete_instance(db_id)


# Member bodies come from the captures: the cluster has the master credentials, the member has none.
_CAPTURE_TAGS = [
    ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'),
    ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r43-open-gaps2-existing'),
    ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r43-open-gaps2-existing'),
]


def _capture_cluster(cluster_id, engine, version=None):
    # r06-create-cluster, r43-create-apg and r43-create-amy: the cluster carries MasterUsername/MasterUserPassword.
    pairs = [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', cluster_id), ('Engine', engine),
             ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'),
             ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), *_CAPTURE_TAGS,
             ('DBSubnetGroupName', 'msc-2633w-sng')]
    return [*pairs, ('EngineVersion', version)] if version else pairs


def _capture_member(cluster_id, db_id, engine, version, instance_class="db.r6g.large"):
    # r43-e-i8v-member-*: no master credentials; db.r6g.large (or db.serverless / db.t4g.medium) is accepted.
    return [('Action', 'CreateDBInstance'), ('DBClusterIdentifier', cluster_id), ('Engine', engine),
            ('DBInstanceClass', instance_class), *_CAPTURE_TAGS, ('DBInstanceIdentifier', db_id),
            ('EngineVersion', version)]


def _retarget(pairs, cluster_id, db_id):
    """Point a captured member body at this test's cluster and instance identifiers."""
    swap = {'DBClusterIdentifier': cluster_id, 'DBInstanceIdentifier': db_id}
    return [(k, swap.get(k, v)) for k, v in pairs]


# Captured member bodies against an aurora-postgresql cluster (r06, r17, r31); the member has no master
# credentials, and AWS answers the engine check for each.
_MEMBER_CAPTURED = [
    ('r06-member-unknown', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r06-m-unknown'), ('DBClusterIdentifier', 'msc2637-r06-cl'), ('Engine', 'postgress'), ('DBInstanceClass', 'db.t4g.medium'), ('PubliclyAccessible', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r06-existing-resources'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r06-existing-resources')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r06-member-blank', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r06-m-blank'), ('DBClusterIdentifier', 'msc2637-r06-cl'), ('Engine', ''), ('DBInstanceClass', 'db.t4g.medium'), ('PubliclyAccessible', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r06-existing-resources'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r06-existing-resources')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r17-member-apg-absent', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r17-23-apg-x'), ('DBClusterIdentifier', 'msc2637-r17-cl-apg'), ('DBInstanceClass', 'db.t4g.medium'), ('PubliclyAccessible', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r17-members-globals'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r17-members-globals')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r06-member-mysql', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r06-m-mysql'), ('DBClusterIdentifier', 'msc2637-r06-cl'), ('Engine', 'mysql'), ('DBInstanceClass', 'db.t4g.medium'), ('PubliclyAccessible', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r06-existing-resources'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r06-existing-resources')], (400, 'InvalidParameterCombination', 'Sender', "The engine name requested for your DB instance (mysql) doesn't match the engine name of your DB cluster (aurora-postgresql).")),
    ('r06-member-postgres', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r06-m-postgres'), ('DBClusterIdentifier', 'msc2637-r06-cl'), ('Engine', 'postgres'), ('DBInstanceClass', 'db.t4g.medium'), ('PubliclyAccessible', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r06-existing-resources'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r06-existing-resources')], (400, 'InvalidParameterCombination', 'Sender', "The engine name requested for your DB instance (postgres) doesn't match the engine name of your DB cluster (aurora-postgresql).")),
    ('r06-member-aurora-mysql', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r06-m-aurora-mysql'), ('DBClusterIdentifier', 'msc2637-r06-cl'), ('Engine', 'aurora-mysql'), ('DBInstanceClass', 'db.t4g.medium'), ('PubliclyAccessible', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r06-existing-resources'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r06-existing-resources')], (400, 'InvalidParameterCombination', 'Sender', "The engine name requested for your DB instance (aurora-mysql) doesn't match the engine name of your DB cluster (aurora-postgresql).")),
    ('r17-creating-member-aurora', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r17-2-cr-a'), ('DBClusterIdentifier', 'msc2637-r17-cl-apg'), ('DBInstanceClass', 'db.t4g.medium'), ('PubliclyAccessible', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r17-members-globals'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r17-members-globals'), ('Engine', 'aurora')], (400, 'InvalidParameterCombination', 'Sender', "The engine name requested for your DB instance (aurora) doesn't match the engine name of your DB cluster (aurora-postgresql).")),
    ('r17-member-apg-aurora-upper', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r17-6-apg-a-upper'), ('DBClusterIdentifier', 'msc2637-r17-cl-apg'), ('DBInstanceClass', 'db.t4g.medium'), ('PubliclyAccessible', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r17-members-globals'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r17-members-globals'), ('Engine', 'AURORA')], (400, 'InvalidParameterCombination', 'Sender', "The engine name requested for your DB instance (aurora) doesn't match the engine name of your DB cluster (aurora-postgresql).")),
    ('r31-member-mixed-case', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r31-43-m-mixed'), ('DBClusterIdentifier', 'msc2637-r31-apg'), ('Engine', 'Aurora-PostgreSQL'), ('DBInstanceClass', 'db.serverless'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r31-existing-pg-apg'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r31-existing-pg-apg')], (200, None, None, None)),
    ('r06-member-valid', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r06-member'), ('DBClusterIdentifier', 'msc2637-r06-cl'), ('Engine', 'aurora-postgresql'), ('DBInstanceClass', 'db.t4g.medium'), ('PubliclyAccessible', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r06-existing-resources'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r06-existing-resources')], (200, None, None, None)),
]
_MEMBER_MODIFY_CAPTURED = ('r06-member-modify-engine-unknown', [('Action', 'ModifyDBInstance'), ('DBInstanceIdentifier', 'msc2637-r06-member'), ('ApplyImmediately', 'true'), ('Engine', 'postgress')], (400, 'InvalidParameterCombination', 'Sender', 'Modification of database from engine manfred to engine postgress is not supported.'))


def test_rds_cluster_member_engine_checks(rds):
    cluster_id = _name("eng-c")
    try:
        assert _post(_capture_cluster(cluster_id, "aurora-postgresql"))[0] == 200
        for key, pairs, expected in _MEMBER_CAPTURED:
            member_id = _name("eng-m")
            try:
                assert _post(_retarget(pairs, cluster_id, member_id))[:4] == expected, key
                if expected[0] == 200:
                    described = rds.describe_db_instances(DBInstanceIdentifier=member_id)["DBInstances"][0]
                    assert described["Engine"] == "aurora-postgresql", key
            finally:
                _delete_instance(member_id)
    finally:
        _post({"Action": "DeleteDBCluster", "DBClusterIdentifier": cluster_id, "SkipFinalSnapshot": "true"})

def test_rds_member_of_unknown_cluster_arn_is_not_found_before_the_engine():
    arn = "arn:aws:rds:us-east-1:000000000000:cluster:no-such-" + uuid.uuid4().hex[:6]
    # r03-cluster-bad-arn-unknown-engine-first: the cluster lookup answers before the engine check.
    assert _post(_instance(_name("eng-arn"), DBClusterIdentifier=arn))[:4] == (
        404, "DBClusterNotFoundFault", "Sender",
        f"The source cluster could not be found or cannot be accessed: {arn}")


def test_rds_cluster_joining_a_global_cluster_engine_checks(rds):
    global_id, cluster_id = _name("eng-g"), _name("eng-gc")
    try:
        assert _post({"Action": "CreateGlobalCluster", "GlobalClusterIdentifier": global_id,
                      "Engine": "aurora-postgresql"})[0] == 200
        join = {"Action": "CreateDBCluster", "DBClusterIdentifier": cluster_id, "GlobalClusterIdentifier": global_id}
        for engine in ("postgress", "", "POSTGRES", "AURORA-POSTGRESQL"):
            assert _post({**join, "Engine": engine})[:4] == (500, "InternalFailure", "Receiver", _INTERNAL)
        for engine in ("postgres", "mysql", "aurora-mysql"):
            assert _post({**join, "Engine": engine})[:4] == (
                400, "InvalidParameterValue", "Sender",
                f"Value for engine should match setting for global cluster {global_id}")
        assert _post({**join, "Engine": "mariadb"})[:4] == (
            400, "InvalidParameterCombination", "Sender",
            "The DB engine version wasn't specified and Amazon RDS couldn't identify a valid DB engine "
            "version for the mariadb DB cluster. Submit a new request that specifies a valid DB engine version.")
        assert _post({**join, "Engine": "aurora"})[:4] == (
            400, "InvalidParameterValue", "Sender", "Engine version not found: oscar/null")
        assert _post(join)[:4] == (
            400, "InvalidParameterValue", "Sender",
            "1 validation error detected: Value null at 'engine' failed to satisfy constraint: Member must not be null")
        assert cluster_id not in [c["DBClusterIdentifier"] for c in rds.describe_db_clusters()["DBClusters"]]
    finally:
        _post({"Action": "DeleteGlobalCluster", "GlobalClusterIdentifier": global_id})


def test_rds_cluster_engine_checks_run_before_the_parameter_group_lookup():
    base = {"Action": "CreateDBCluster", "DBClusterIdentifier": _name("eng-cpg"),
            "DBClusterParameterGroupName": "no-such-group"}
    assert _post({**base, "Engine": "postgress"})[:4] == (400, "InvalidParameterValue", "Sender", "Invalid DB engine")
    assert _post({**base, "Engine": "mariadb"})[:4] == (
        400, "InvalidParameterCombination", "Sender",
        "The DB engine version wasn't specified and Amazon RDS couldn't identify a valid DB engine "
        "version for the mariadb DB cluster. Submit a new request that specifies a valid DB engine version.")
    # r04-cpg-missing-alone
    assert _post({**base, "Engine": "aurora-postgresql"})[:4] == (
        404, "DBClusterParameterGroupNotFound", "Sender", "DBClusterParameterGroup no-such-group not found.")


def test_rds_cluster_multi_az_engines_need_storage_then_class():
    base = {"Action": "CreateDBCluster", "DBClusterIdentifier": _name("eng-maz"), "EngineVersion": ""}
    for engine in ("postgres", "mysql"):
        assert _post({**base, "Engine": engine})[:4] == (
            400, "InvalidParameterValue", "Sender", "AllocatedStorage is required.")
        assert _post({**base, "Engine": engine, "AllocatedStorage": "0"})[:4] == (
            400, "InvalidParameterValue", "Sender", "DBClusterInstanceClass is required.")


def test_rds_restore_from_snapshot_engine(rds):
    db_id, snap_id = _name("eng-rs"), _name("eng-rs-snap")
    restored = _name("eng-rs-new")
    try:
        assert _post(_instance(db_id, Engine="oracle-ee"))[0] == 200
        assert _post({"Action": "CreateDBSnapshot", "DBInstanceIdentifier": db_id,
                      "DBSnapshotIdentifier": snap_id})[0] == 200

        def restore(**extra):
            return _post({"Action": "RestoreDBInstanceFromDBSnapshot", "DBInstanceIdentifier": restored,
                          "DBSnapshotIdentifier": snap_id, **extra})[:4]

        for engine in ("postgress", ""):
            assert restore(Engine=engine) == (400, "InvalidParameterValue", "Sender", "Invalid DB engine")
        for engine in ("mysql", "aurora-postgresql"):
            assert restore(Engine=engine) == (
                400, "InvalidParameterCombination", "Sender", "Requested DBSnapshot and Engine are incompatible.")
        assert restore(Engine="ORACLE-EE")[0] == 200
        assert rds.describe_db_instances(DBInstanceIdentifier=restored)["DBInstances"][0]["Engine"] == "oracle-ee"
    finally:
        _delete_instance(restored)
        _delete_instance(db_id)
        _post({"Action": "DeleteDBSnapshot", "DBSnapshotIdentifier": snap_id})


def test_rds_global_cluster_from_existing_cluster_rejects_engine(rds):
    # r06-global-from-cluster-unknown: the source cluster existed in the capture (r06-create-cluster).
    cluster_id = _name("eng-r06")
    try:
        assert _post(_capture_cluster(cluster_id, "aurora-postgresql"))[0] == 200
        got = _post([
            ('Action', 'CreateGlobalCluster'), ('GlobalClusterIdentifier', _name("eng-r06g")),
            ('SourceDBClusterIdentifier', f'arn:aws:rds:us-east-1:000000000000:cluster:{cluster_id}'),
            ('Engine', 'postgress')])
        assert got[:4] == (
            400, 'InvalidParameterCombination', 'Sender',
            'When creating global cluster from existing db cluster, value for engineName should not be '
            'specified since it will be inherited from source cluster')
    finally:
        _post({"Action": "DeleteDBCluster", "DBClusterIdentifier": cluster_id, "SkipFinalSnapshot": "true"})


def test_rds_global_cluster_from_source_rejects_engine():
    status, code, kind, message, _ = _post({
        "Action": "CreateGlobalCluster", "GlobalClusterIdentifier": _name("eng-gs"),
        "SourceDBClusterIdentifier": "arn:aws:rds:us-east-1:000000000000:cluster:nope", "Engine": "postgress"})
    assert (status, code, kind) == (400, "InvalidParameterCombination", "Sender")
    assert message == (
        "When creating global cluster from existing db cluster, value for engineName should not be "
        "specified since it will be inherited from source cluster")


def test_rds_documentdb_engine_routing():
    # Creates with Engine=docdb still go to DocumentDB; other actions are claimed by identifier.
    from ministack.services import documentdb
    assert documentdb.claims_request({"Action": ["CreateDBCluster"], "Engine": ["docdb"]})
    assert documentdb.claims_request({"Action": ["CreateDBInstance"], "Engine": ["docdb"]})
    assert not documentdb.claims_request({"Action": ["ModifyDBInstance"], "Engine": ["docdb"]})


# Describe-side engine checks (D1, D2, D3), bodies captured against AWS.
_DESCRIBE_CAPTURED = [
    ("r08-versions-unknown", [('Action', 'DescribeDBEngineVersions'), ('Engine', 'postgress')], (400, "InvalidParameterValue", "Sender", "Unrecognized engine name: postgress")),
    ("r08-option-groups-unknown", [('Action', 'DescribeOptionGroups'), ('EngineName', 'postgress')], (400, "InvalidParameterValue", "Sender", "Unrecognized engine name: postgress")),
    ("r08-orderable-unknown", [('Action', 'DescribeOrderableDBInstanceOptions'), ('Engine', 'postgress'), ('DBInstanceClass', 'db.t3.micro'), ('MaxRecords', '20')], (400, "InvalidParameterValue", "Sender", "Invalid DB engine")),
    ("r08-orderable-blank", [('Action', 'DescribeOrderableDBInstanceOptions'), ('Engine', ''), ('DBInstanceClass', 'db.t3.micro'), ('MaxRecords', '20')], (400, "InvalidParameterValue", "Sender", "Invalid DB engine")),
    ("r08-orderable-missing", [('Action', 'DescribeOrderableDBInstanceOptions'), ('MaxRecords', '20')], (400, "InvalidParameterValue", "Sender", "Please specify a value for parameter 'engine'.")),
    ("r08-orderable-upper", [('Action', 'DescribeOrderableDBInstanceOptions'), ('Engine', 'POSTGRES'), ('DBInstanceClass', 'db.t3.micro'), ('MaxRecords', '20')], (200, None, None, None)),
    ("r08-versions-blank", [('Action', 'DescribeDBEngineVersions'), ('Engine', '')], (200, None, None, None)),
    ("r08-create-option-group-unknown", [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r08-og-unknown'), ('EngineName', 'postgress'), ('MajorEngineVersion', '16'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r08-engine-siblings'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r08-engine-siblings')], (400, "InvalidParameterValue", "Sender", "Invalid DB engine")),
    ("r08-create-option-group-blank", [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r08-og-blank'), ('EngineName', ''), ('MajorEngineVersion', '16'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r08-engine-siblings'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r08-engine-siblings')], (400, "InvalidParameterValue", "Sender", "The parameter EngineName must be provided and must not be blank.")),
    ("r08-option-group-options-unknown", [('Action', 'DescribeOptionGroupOptions'), ('EngineName', 'postgress')], (400, "InvalidParameterValue", "Sender", "Invalid DB engine")),
    ("r08-option-group-options-blank", [('Action', 'DescribeOptionGroupOptions'), ('EngineName', '')], (400, "InvalidParameterValue", "Sender", "EngineName must be specified.")),
    ("r24-g2-source-blank-engine-apg-sourcedbclusteridentifier-first", [('Action', 'CreateGlobalCluster'), ('SourceDBClusterIdentifier', ''), ('Engine', 'aurora-postgresql'), ('GlobalClusterIdentifier', 'msc2637-r24-63-g2-source-blank-engine-apg-f')], (500, "InternalFailure", "Receiver", _INTERNAL)),
    ("r23-c2-aurora-version-markup", [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r23-239-c2-aurora-markup'), ('Engine', 'aurora'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r23-absent-wire-blanks'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r23-absent-wire-blanks'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', 'a<b&c')], (400, "InvalidParameterCombination", "Sender", "Cannot find version a<b&c for aurora")),
]


@pytest.mark.parametrize("key,pairs,expected", _DESCRIBE_CAPTURED, ids=[c[0] for c in _DESCRIBE_CAPTURED])
def test_rds_describe_side_engine_validation_matches_capture(key, pairs, expected):
    assert _post(pairs)[:4] == expected


def test_rds_error_body_escapes_quote_but_not_apostrophe():
    # r23-c2-aurora-version-quote: `Cannot find version a&quot;b'c for aurora`; AWS never sends &apos; (ledger X1).
    raw = requests.post(ENDPOINT, data=[
        ("Action", "CreateDBCluster"), ("DBClusterIdentifier", "msc2637-r23-x1-raw"), ("Engine", "aurora"),
        ("MasterUsername", "mscadmin"), ("MasterUserPassword", "MsCapture2637pw"), ("BackupRetentionPeriod", "1"),
        ("DeletionProtection", "false"), ("DBSubnetGroupName", "msc-2633w-sng"), ("EngineVersion", "a\"b'c")],
        headers=_AUTH, timeout=30)
    assert raw.status_code == 400
    assert "<Code>InvalidParameterCombination</Code>" in raw.text
    assert "<Type>Sender</Type>" in raw.text
    assert "<Message>Cannot find version a&quot;b'c for aurora</Message>" in raw.text
    assert "&apos;" not in raw.text


def test_rds_error_body_keeps_apostrophe_literal():
    # r16 control-character keys: `The input isn't valid. Input can't contain control characters.`
    raw = requests.post(ENDPOINT, data=[
        ("Action", "CreateDBInstance"), ("DBInstanceIdentifier", "msc2637-x1-apos"), ("Engine", "post\x01gres"),
        ("DBInstanceClass", "db.t3.micro")], headers=_AUTH, timeout=30)
    assert raw.status_code == 400
    assert "<Code>InvalidParameterValue</Code>" in raw.text
    assert "<Type>Sender</Type>" in raw.text
    assert "<Message>The input isn't valid. Input can't contain control characters.</Message>" in raw.text
    assert "&apos;" not in raw.text


@pytest.mark.parametrize("suffix,engine,message", [
    ("markup", "a<b&c", "Modification of database from engine postgres to engine a<b&c is not supported."),
    ("amp", "postgres&x", "Modification of database from engine postgres to engine postgres&x is not supported."),
    ("quote", 'post"gres', 'Modification of database from engine postgres to engine post"gres is not supported.'),
])
def test_rds_modify_engine_error_escapes_markup(suffix, engine, message, rds):
    db_id = _name("eng-x1")
    try:
        assert _post(_instance(db_id, Engine="postgres"))[0] == 200
        status, code, kind, text, _ = _post(
            [("Action", "ModifyDBInstance"), ("DBInstanceIdentifier", db_id), ("ApplyImmediately", "true"), ("Engine", engine)]
        )
        assert (status, code, kind, text) == (400, "InvalidParameterCombination", "Sender", message)
    finally:
        _delete_instance(db_id)


_MULTI_AZ_MYSQL_CREATE = [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r25-maz-mysql'), ('Engine', 'mysql'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r25-multiaz-and-q6-creates'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r25-multiaz-and-q6-creates'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('AllocatedStorage', '100'), ('StorageType', 'io1'), ('Iops', '1000'), ('DBClusterInstanceClass', 'db.m6gd.large'), ('EngineVersion', '8.4.11')]


def test_rds_multi_az_mysql_cluster_stays_in_the_store(rds):
    # S1, r25-multiaz-mysql-create: AWS answers 200 for this body and the cluster is described
    # afterwards (r25-multiaz-mysql--available, r25-multiaz-mysql-list-filter-engine).
    cluster_id = dict(_MULTI_AZ_MYSQL_CREATE)["DBClusterIdentifier"]
    _post({"Action": "DeleteDBCluster", "DBClusterIdentifier": cluster_id, "SkipFinalSnapshot": "true"})
    try:
        assert _post(_MULTI_AZ_MYSQL_CREATE)[0] == 200
        status, _, _, _, root = _post(
            [("Action", "DescribeDBClusters"), ("DBClusterIdentifier", cluster_id)],
        )
        assert status == 200
        assert root.findtext(".//{*}DBCluster/{*}Engine") == "mysql"
        status, _, _, _, root = _post([
            ("Action", "DescribeDBClusters"), ("Filters.Filter.1.Name", "engine"),
            ("Filters.Filter.1.Values.Value.1", "mysql"),
        ])
        assert status == 200
        assert cluster_id in [e.text for e in root.findall(".//{*}DBClusterIdentifier")]
        # r25-multiaz-mysql-create-again: refused (MiniStack words the message its own way).
        assert _post(_MULTI_AZ_MYSQL_CREATE)[:3] == (400, "DBClusterAlreadyExistsFault", "Sender")
    finally:
        _post({"Action": "DeleteDBCluster", "DBClusterIdentifier": cluster_id, "SkipFinalSnapshot": "true"})


# W1x3, R3, F1v and X2 (captures r42-c, r42-g, r42-m, r43-c, r43-g, r43-m).
_NEW_CAPTURED = [
    ("r42-g-replica-source-missing-monitoringinterval-abc", [('Action', 'CreateDBInstanceReadReplica'), ('DBInstanceIdentifier', 'msc2637-r42-23-g-rr-monito-abc'), ('SourceDBInstanceIdentifier', 'msc2637-r42-no-such-db'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r42-open-gaps2-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r42-open-gaps2-refused'), ('MonitoringInterval', 'abc')], (400, "MalformedInput", "Sender", None)),
    ("r42-g-replica-source-missing-storagethroughput-blank", [('Action', 'CreateDBInstanceReadReplica'), ('DBInstanceIdentifier', 'msc2637-r42-28-g-rr-storag-blank'), ('SourceDBInstanceIdentifier', 'msc2637-r42-no-such-db'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r42-open-gaps2-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r42-open-gaps2-refused'), ('StorageThroughput', '')], (400, "MalformedInput", "Sender", "missing value for decimal type")),
    ("r42-g-restore-snapshot-missing-storagethroughput-abc", [('Action', 'RestoreDBInstanceFromDBSnapshot'), ('DBInstanceIdentifier', 'msc2637-r42-35-g-rs-abc'), ('DBSnapshotIdentifier', 'msc2637-r42-no-such-snap'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r42-open-gaps2-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r42-open-gaps2-refused'), ('StorageThroughput', 'abc')], (400, "MalformedInput", "Sender", None)),
    ("r42-g-replica-source-missing-backupretentionperiod-abc", [('Action', 'CreateDBInstanceReadReplica'), ('DBInstanceIdentifier', 'msc2637-r42-31-g-rr-backup-abc'), ('SourceDBInstanceIdentifier', 'msc2637-r42-no-such-db'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r42-open-gaps2-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r42-open-gaps2-refused'), ('BackupRetentionPeriod', 'abc')], (404, "DBInstanceNotFound", "Sender", "DBInstance msc2637-r42-no-such-db not found.")),
    ("r42-c-clusters-engine-values-marker-blank", [('Action', 'DescribeDBClusters'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values', '')], (400, "InvalidParameterCombination", "Sender", "The list of Engine Names must not be empty.")),
    ("r42-c-versions-engine-no-values", [('Action', 'DescribeDBEngineVersions'), ('Filters.Filter.1.Name', 'engine')], (400, "InvalidParameterCombination", "Sender", "The values list cannot be null for the filter engine.")),
    ("r42-c-snapshots-engine-no-values", [('Action', 'DescribeDBSnapshots'), ('Filters.Filter.1.Name', 'engine')], (400, "InvalidParameterCombination", "Sender", "Invalid value '' for filter engine")),
    ("r42-c-cluster-snapshots-engine-values-marker-blank", [('Action', 'DescribeDBClusterSnapshots'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values', '')], (400, "InvalidParameterCombination", "Sender", "Invalid value '' for filter engine")),
]


@pytest.mark.parametrize("key,pairs,expected", _NEW_CAPTURED, ids=[c[0] for c in _NEW_CAPTURED])
def test_rds_w1x3_r3_f1v_match_capture(key, pairs, expected):
    assert _post(pairs)[:4] == expected


@pytest.mark.parametrize("pairs", [
    [("Action", "DescribeDBEngineVersions"), ("Engine", "post\ufffegres")],
    [("Action", "DescribeDBInstances"), ("Filters.Filter.1.Name", "engine"),
     ("Filters.Filter.1.Values.Value.1", "post\uffffgres")],
], ids=["r42-m-fffe-versions-engine", "r42-m-ffff-instances-engine-filter"])
def test_rds_x2_noncharacter_echo_is_empty_400(pairs):
    response = requests.post(ENDPOINT, data=pairs, headers=_AUTH, timeout=30)
    assert response.status_code == 400
    assert response.content == b""


# F1 index tokens and F1s, W1x2, W1sv2, I5p-id and I5p-class (captures r09, r23, r29, r35, r37, r38, r40, r42, r43).

CAPTURED_F1_W1_I5P = [
    ('r29-versions-engine-and-filter-both-unknown', [('Action', 'DescribeDBEngineVersions'), ('Engine', 'postgresx'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'mysqlx')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgresx')),
]


@pytest.mark.parametrize(
    "key,pairs,expected", CAPTURED_F1_W1_I5P, ids=[c[0] for c in CAPTURED_F1_W1_I5P],
)
def test_rds_f1_w1_i5p_match_capture(key, pairs, expected):
    assert _post(pairs)[:4] == expected


def test_rds_w1sv2_accepts_java_double_spellings():
    # r35/r37/r38 accepted list: the parse passes and the next check (engine) answers.
    for value in (" 1", "1 ", "1e0", "0x1p0", "NaN", "-Infinity", ".5", "1.", "+1", "1d", "1F"):
        pairs = [
            ("Action", "CreateDBCluster"), ("DBClusterIdentifier", "msc-sv2"), ("Engine", "nope"),
            ("ServerlessV2ScalingConfiguration.MinCapacity", value),
        ]
        assert _post(pairs)[:4] == (400, "InvalidParameterValue", "Sender", "Invalid DB engine"), value
    for value in ("abc", "0x1", "0x", "nan", "inf", "INFINITY", "1e", "e1", "1..0", "++1", "1_0", "１"):
        pairs = [
            ("Action", "CreateDBCluster"), ("DBClusterIdentifier", "msc-sv2"), ("Engine", "nope"),
            ("ServerlessV2ScalingConfiguration.MaxCapacity", value),
        ]
        assert _post(pairs)[:4] == (400, "MalformedInput", "Sender", None), value


# Part 2 findings: expected answers are the AWS ones from the named capture keys.
_I3_ANSWER = (400, "InvalidParameterValue", "Sender", _I3)


@pytest.mark.parametrize("key,pairs,expected", [
    ("r42-j-create-global-del-no-id", [('Action', 'CreateGlobalCluster'), ('Engine', 'aurora\x7f')], _I3_ANSWER),
    ("r38-c-og-create-engine-tab", [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r38-og-tab'), ('EngineName', 'postgres\t'), ('MajorEngineVersion', '16'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r38-open-gaps-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r38-open-gaps-existing')], _I3_ANSWER),
    ("r37-a-og-name-missing-engine-soh", [('Action', 'CreateOptionGroup'), ('EngineName', 'post\x01gres'), ('MajorEngineVersion', '16'), ('OptionGroupDescription', 'ministack capture 2637')], _I3_ANSWER),
    ("r26-create-option-group-oracle-se-major-blank", [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r26-og-oracle-se-mb'), ('EngineName', 'oracle-se'), ('MajorEngineVersion', ''), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r26-orderable-option-group-engines'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r26-orderable-option-group-engines')], (400, "InvalidParameterValue", "Sender", "The parameter MajorEngineVersion must be provided and must not be blank.")),
    ("r42-f-docdb-create-storagethroughput-abc-no-cluster", [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r42-19-f-storag'), ('Engine', 'docdb'), ('DBInstanceClass', 'db.t3.medium'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r42-open-gaps2-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r42-open-gaps2-refused'), ('StorageThroughput', 'abc')], (400, "MalformedInput", "Sender", None)),
    ("r42-f-docdb-create-maxallocatedstorage-abc-cluster-missing", [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r42-22-f-maxall'), ('Engine', 'docdb'), ('DBInstanceClass', 'db.t3.medium'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r42-open-gaps2-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r42-open-gaps2-refused'), ('DBClusterIdentifier', 'msc2637-r42-no-such-cluster'), ('MaxAllocatedStorage', 'abc')], (400, "MalformedInput", "Sender", None)),
], ids=lambda v: v if isinstance(v, str) and v.startswith("r") and "-" in v else None)
def test_rds_part2_findings_match_capture(key, pairs, expected):
    assert _post(pairs)[:4] == expected


def test_rds_option_group_oracle_se_blank_major_is_not_stored():
    name = _name("og-ose")
    for extra in ([("MajorEngineVersion", "")], []):
        pairs = [("Action", "CreateOptionGroup"), ("OptionGroupName", name), ("EngineName", "oracle-se1"),
                 ("OptionGroupDescription", "d"), *extra]
        assert _post(pairs)[:4] == (400, "InvalidParameterValue", "Sender",
                                    "The parameter MajorEngineVersion must be provided and must not be blank.")
    assert _post([("Action", "DescribeOptionGroups"), ("OptionGroupName", name)])[:2] == (
        404, "OptionGroupNotFoundFault")


@pytest.mark.parametrize("engine,version", [("neptune", "1.4.8.0"), ("docdb", "8.0.3")])
def test_rds_standalone_global_cluster_default_version(engine, version, rds):
    gc_id = _name("gc-dv")
    try:
        assert _post([("Action", "CreateGlobalCluster"), ("GlobalClusterIdentifier", gc_id), ("Engine", engine)])[0] == 200
        got = rds.describe_global_clusters(GlobalClusterIdentifier=gc_id)["GlobalClusters"][0]
        assert (got["Engine"], got["EngineVersion"]) == (engine, version)
    finally:
        _post([("Action", "DeleteGlobalCluster"), ("GlobalClusterIdentifier", gc_id)])


# r43-e-i8v-member-* and r38-j-i8v-member-*: the engine-version token a member may send for a cluster
# at 17.9 (aurora-postgresql) or 8.0.mysql_aurora.3.10.3 (aurora-mysql).
@pytest.mark.parametrize("engine,cluster_version,accepted,refused", [
    ("aurora-postgresql", "17.9",
     ["", "17", " 17", " 17.9", "17.09", "17.009", "17.9.", ".17.9", "17..9", "+17.9", "17 .9", "17.9 ", "１７.９",
      "17.9￾", "17.9￿", "17.9�"],
     ["17.9.0", "1 7.9", "11.9", "99.99", "8.0.mysql_aurora.3.10.3"]),
    ("aurora-mysql", "8.0.mysql_aurora.3.10.3",
     ["8.00", "08.0", "8.0.", "8.0 ", "8.0.mysql_aurora.03.10.3", "8.0.Mysql_Aurora.3.10.3", " 8.0.mysql_aurora.3.10.3"],
     ["8.0.mysql_aurora.3.10"]),
], ids=["apg", "amy"])
def test_rds_member_engine_version_token_match(engine, cluster_version, accepted, refused, rds):
    cluster_id = _name("i8v-c")
    members = []
    try:
        assert _post(_capture_cluster(cluster_id, engine, cluster_version))[0] == 200
        for version in refused:
            got = _post(_capture_member(cluster_id, _name("i8v-m"), engine, version))
            assert got[:4] == (
                400, "InvalidParameterCombination", "Sender",
                f"The engine version that you requested for your DB instance ({version}) does not match "
                f"the engine version of your DB cluster ({cluster_version})."), version
        for version in accepted:
            member = _name("i8v-m")
            members.append(member)
            assert _post(_capture_member(cluster_id, member, engine, version))[0] == 200, version
            assert rds.describe_db_instances(DBInstanceIdentifier=member)["DBInstances"][0]["EngineVersion"] == cluster_version
    finally:
        for member in members:
            _delete_instance(member)
        _post({"Action": "DeleteDBCluster", "DBClusterIdentifier": cluster_id, "SkipFinalSnapshot": "true"})

_URL_ACTION_CAPTURED = [
    ('r42-i-url-action-create-instance-unknown-storage-blank', 'CreateDBInstance',
     [('DBInstanceIdentifier', 'msc2637-r42-63-i-url-instance-unknown-storage-blank'), ('Engine', 'postgress'), ('DBInstanceClass', 'db.t3.micro'), ('AllocatedStorage', '')],
     (400, 'MalformedInput', 'Sender', 'missing value for decimal type')),
    ('r43-i-url-action-create-instance-postgres-valid-storage-blank', 'CreateDBInstance',
     [('DBInstanceIdentifier', 'msc2637-r43-38-i-url-storage-blank'), ('Engine', 'postgres'), ('DBInstanceClass', 'db.t4g.micro'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('AllocatedStorage', '')],
     (400, 'MalformedInput', 'Sender', 'missing value for decimal type')),
    ('r43-i-url-action-create-cluster-apg-valid-port-blank', 'CreateDBCluster',
     [('DBClusterIdentifier', 'msc2637-r43-40-i-url-cluster-port-blank'), ('Engine', 'aurora-postgresql'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('Port', ''), ('DBSubnetGroupName', 'msc-2633w-sng')],
     (400, 'MalformedInput', 'Sender', 'missing value for decimal type')),
    ('r43-i-url-action-modify-instance-existing-port-blank', 'ModifyDBInstance',
     [('DBInstanceIdentifier', 'msc2637-r43-pg'), ('DBPortNumber', ''), ('ApplyImmediately', 'true')],
     (400, 'MalformedInput', 'Sender', 'missing value for decimal type')),
    ('r43-i-url-action-modify-instance-existing-storage-blank', 'ModifyDBInstance',
     [('DBInstanceIdentifier', 'msc2637-r43-pg'), ('AllocatedStorage', ''), ('ApplyImmediately', 'true')],
     (400, 'MalformedInput', 'Sender', 'missing value for decimal type')),
]


@pytest.mark.parametrize("key,action,pairs,expected", _URL_ACTION_CAPTURED,
                         ids=[c[0] for c in _URL_ACTION_CAPTURED])
def test_rds_url_action_blank_members_are_kept(key, action, pairs, expected):
    """B1q: Action and Version in the URL still keep the captured blank members."""
    response = requests.post(
        f"{ENDPOINT}/?Action={action}&Version=2014-10-31", data=pairs,
        headers={**_AUTH, "Content-Type": "application/x-www-form-urlencoded"}, timeout=30,
    )
    error = ET.fromstring(response.content).find(".//{*}Error")
    assert (response.status_code, error.findtext("{*}Code"), error.findtext("{*}Type"),
            error.findtext("{*}Message")) == expected


def test_rds_engine_filter_noncharacter_answers_empty_400():
    # r43-m-fffe-instances-engine-filter-existing: AWS answers 400 with no error body.
    response = requests.post(
        ENDPOINT,
        data=[("Action", "DescribeDBInstances"), ("Filters.Filter.1.Name", "engine"),
              ("Filters.Filter.1.Values.Value.1", "postgres￾")],
        headers={**_AUTH, "Content-Type": "application/x-www-form-urlencoded"}, timeout=30,
    )
    assert response.status_code == 400
    assert response.content == b""


def test_rds_url_only_action_keeps_blank_port():
    # r42-i-url-action-create-cluster-port-blank: Action in the query string, rest in the body.
    response = requests.post(
        f"{ENDPOINT}/?Action=CreateDBCluster&Version=2014-10-31",
        data=[("DBClusterIdentifier", _name("url-blank")), ("Engine", "aurora-postgresql"), ("Port", "")],
        headers={**_AUTH, "Content-Type": "application/x-www-form-urlencoded"}, timeout=30,
    )
    error = ET.fromstring(response.content).find(".//{*}Error")
    assert (response.status_code, error.findtext("{*}Code"), error.findtext("{*}Type"),
            error.findtext("{*}Message")) == (400, "MalformedInput", "Sender", "missing value for decimal type")


@pytest.mark.parametrize("action", ["DescribeDBInstances", "DescribeDBEngineVersions"])
def test_rds_engine_filter_4301_digit_index_is_malformed_input(action):
    # r29-filter-shape-filter-index-4301-digits-{instances,versions}: AWS 400, never a 500.
    token = "9" * 4301
    status, code, kind, message, _ = _post([
        ("Action", action), ("Version", "2014-10-31"),
        (f"Filters.Filter.{token}.Name", "engine"),
        (f"Filters.Filter.{token}.Values.Value.1", "postgress"),
    ])
    assert (status, code, kind, message) == (400, "MalformedInput", "Sender", f"Index {token} is illegal")


@pytest.mark.parametrize("engine", ["oracle-se", "ORACLE-SE", "oracle-se1", "ORACLE-SE1"])
def test_rds_describe_option_group_options_retired_oracle_without_major_is_accepted(engine):
    # r26-option-group-options-oracle-se{,-upper,1,1-upper}: AWS 200 with no MajorEngineVersion.
    status, code, _, _, root = _post([
        ("Action", "DescribeOptionGroupOptions"), ("EngineName", engine), ("MaxRecords", "20"),
    ])
    assert (status, code) == (200, None)


@pytest.mark.parametrize("value", ["POSTGRES", "Postgres", "postgres"])
def test_rds_engine_filter_matches_stored_instance_case_insensitively(rds, value):
    # r31-filter-stored-pg-upper / -pg-title: AWS returns the stored resource.
    db_id = _name("eng-flt")
    try:
        assert _post(_instance(db_id, Engine="postgres"))[0] == 200
        status, code, _, _, root = _post([
            ("Action", "DescribeDBInstances"), ("Version", "2014-10-31"),
            ("Filters.Filter.1.Name", "engine"), ("Filters.Filter.1.Values.Value.1", value),
            ("Filters.Filter.2.Name", "db-instance-id"), ("Filters.Filter.2.Values.Value.1", db_id),
        ])
        assert (status, code) == (200, None)
        assert [e.text for e in root.findall(".//{*}DBInstanceIdentifier")] == [db_id]
    finally:
        _delete_instance(db_id)


def test_rds_engine_filter_matches_stored_cluster_case_insensitively(rds):
    # r31-filter-stored-apg-upper: AWS returns the stored cluster.
    cid = _name("eng-flc")
    try:
        assert _post({"Action": "CreateDBCluster", "DBClusterIdentifier": cid, "Engine": "aurora-postgresql",
                      "MasterUsername": "mscadmin", "MasterUserPassword": "MsCapture2637pw"})[0] == 200
        status, code, _, _, root = _post([
            ("Action", "DescribeDBClusters"), ("Version", "2014-10-31"),
            ("Filters.Filter.1.Name", "engine"), ("Filters.Filter.1.Values.Value.1", "AURORA-POSTGRESQL"),
            ("Filters.Filter.2.Name", "db-cluster-id"), ("Filters.Filter.2.Values.Value.1", cid),
        ])
        assert (status, code) == (200, None)
        assert [e.text for e in root.findall(".//{*}DBClusterIdentifier")] == [cid]
    finally:
        _post({"Action": "DeleteDBCluster", "DBClusterIdentifier": cid, "SkipFinalSnapshot": "true"})


# Pass 4 coverage: C2osev, D3cs, G1x, I5c/I5cv, the describe-side engine echo and sets, W1len
# and the typed-integer keys.  Every body is the one sent to AWS (Version dropped, account ARN
# replaced); the answer is AWS's.  Rows are stateless refusals; the duplicate and quota keys and the
# successful creates are left out.
_COVERAGE_CAPTURED = [
    # C2osev: CreateDBCluster Engine=oracle-se / oracle-se1 (r35-c2osev-*).
    ('r35-c2osev-ose-no-version', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-1-ose-nov'), ('Engine', 'oracle-se'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'Engine version not found: oracle-se/null')),
    ('r35-c2osev-ose-version', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-2-ose-v'), ('Engine', 'oracle-se'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '11.2.0.4.v1')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version 11.2.0.4.v1 for oracle-se')),
    ('r35-c2osev-ose-version-subnets-missing', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-10-ose-vsm'), ('Engine', 'oracle-se'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc2637-r35-no-such-subnets'), ('EngineVersion', '11.2.0.4.v1')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version 11.2.0.4.v1 for oracle-se')),
    ('r35-c2osev-ose-version-junk', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-3-ose-vj'), ('Engine', 'oracle-se'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '99.99')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version 99.99 for oracle-se')),
    ('r35-c2osev-ose-version-blank', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-4-ose-vb'), ('Engine', 'oracle-se'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version  for oracle-se')),
    ('r35-c2osev-ose-version-blank-engineversion-last', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-8-ose-vbl'), ('Engine', 'oracle-se'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version  for oracle-se')),
    ('r35-c2osev-ose-version-port-abc', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-9-ose-vp'), ('Engine', 'oracle-se'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '11.2.0.4.v1'), ('Port', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r35-c2osev-ose1-version-port-abc', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-20-ose1-vp'), ('Engine', 'oracle-se1'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '11.2.0.4.v1'), ('Port', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r35-c2osev-ose1-no-version', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-12-ose1-nov'), ('Engine', 'oracle-se1'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'Engine version not found: oracle-se1/null')),
    ('r35-c2osev-ose1-version', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-13-ose1-v'), ('Engine', 'oracle-se1'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '11.2.0.4.v1')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version 11.2.0.4.v1 for oracle-se1')),
    ('r35-c2osev-ose1-version-subnets-missing', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-21-ose1-vsm'), ('Engine', 'oracle-se1'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc2637-r35-no-such-subnets'), ('EngineVersion', '11.2.0.4.v1')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version 11.2.0.4.v1 for oracle-se1')),
    ('r35-c2osev-ose1-version-junk', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-14-ose1-vj'), ('Engine', 'oracle-se1'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '99.99')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version 99.99 for oracle-se1')),
    ('r35-c2osev-ose1-version-blank', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-15-ose1-vb'), ('Engine', 'oracle-se1'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version  for oracle-se1')),
    ('r35-c2osev-ose1-version-blank-engineversion-last', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-19-ose1-vbl'), ('Engine', 'oracle-se1'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version  for oracle-se1')),
    ('r35-c2osev-ose-upper-version', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-23-ose-upper-v'), ('Engine', 'ORACLE-SE'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '11.2.0.4.v1')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r35-c2osev-ose-upper-version-blank', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r35-27-ose-up-vb'), ('Engine', 'ORACLE-SE'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r35-open-q21-q26-refused'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r35-open-q21-q26-refused'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    # D3cs: custom-sqlserver option groups (r36-d3cs-*, r27-create-option-group-custom-sqlserver-*, r34-*).
    ('r36-d3cs-create-custom-sqlserver-ee-lower-major', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r36-og-custom-sqlserver-ee-lower'), ('EngineName', 'custom-sqlserver-ee'), ('MajorEngineVersion', '16.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r36-open-q21-q26-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r36-open-q21-q26-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-ee.")),
    ('r36-d3cs-create-custom-sqlserver-ee-upper-major', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r36-og-custom-sqlserver-ee-upper'), ('EngineName', 'CUSTOM-SQLSERVER-EE'), ('MajorEngineVersion', '16.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r36-open-q21-q26-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r36-open-q21-q26-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-ee.")),
    ('r36-d3cs-create-custom-sqlserver-ee-mixed-major', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r36-og-custom-sqlserver-ee-mixed'), ('EngineName', 'Custom-Sqlserver-Ee'), ('MajorEngineVersion', '16.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r36-open-q21-q26-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r36-open-q21-q26-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-ee.")),
    ('r36-d3cs-create-custom-sqlserver-se-lower-major', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r36-og-custom-sqlserver-se-lower'), ('EngineName', 'custom-sqlserver-se'), ('MajorEngineVersion', '16.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r36-open-q21-q26-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r36-open-q21-q26-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-se.")),
    ('r36-d3cs-create-custom-sqlserver-se-upper-major', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r36-og-custom-sqlserver-se-upper'), ('EngineName', 'CUSTOM-SQLSERVER-SE'), ('MajorEngineVersion', '16.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r36-open-q21-q26-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r36-open-q21-q26-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-se.")),
    ('r36-d3cs-create-custom-sqlserver-se-mixed-major', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r36-og-custom-sqlserver-se-mixed'), ('EngineName', 'Custom-Sqlserver-Se'), ('MajorEngineVersion', '16.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r36-open-q21-q26-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r36-open-q21-q26-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-se.")),
    ('r36-d3cs-create-custom-sqlserver-web-lower-major', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r36-og-custom-sqlserver-web-lower'), ('EngineName', 'custom-sqlserver-web'), ('MajorEngineVersion', '16.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r36-open-q21-q26-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r36-open-q21-q26-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-web.")),
    ('r36-d3cs-create-custom-sqlserver-web-upper-major', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r36-og-custom-sqlserver-web-upper'), ('EngineName', 'CUSTOM-SQLSERVER-WEB'), ('MajorEngineVersion', '16.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r36-open-q21-q26-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r36-open-q21-q26-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-web.")),
    ('r36-d3cs-create-custom-sqlserver-web-mixed-major', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r36-og-custom-sqlserver-web-mixed'), ('EngineName', 'Custom-Sqlserver-Web'), ('MajorEngineVersion', '16.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r36-open-q21-q26-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r36-open-q21-q26-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-web.")),
    ('r36-d3cs-create-custom-sqlserver-dev-lower-major', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r36-og-custom-sqlserver-dev-lower'), ('EngineName', 'custom-sqlserver-dev'), ('MajorEngineVersion', '15.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r36-open-q21-q26-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r36-open-q21-q26-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-dev.")),
    ('r36-d3cs-create-custom-sqlserver-dev-upper-major', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r36-og-custom-sqlserver-dev-upper'), ('EngineName', 'CUSTOM-SQLSERVER-DEV'), ('MajorEngineVersion', '15.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r36-open-q21-q26-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r36-open-q21-q26-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-dev.")),
    ('r36-d3cs-create-custom-sqlserver-dev-mixed-major', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r36-og-custom-sqlserver-dev-mixed'), ('EngineName', 'Custom-Sqlserver-Dev'), ('MajorEngineVersion', '15.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r36-open-q21-q26-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r36-open-q21-q26-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-dev.")),
    ('r27-create-option-group-custom-sqlserver-ee', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r27-og-custom-sqlserver-ee'), ('EngineName', 'custom-sqlserver-ee'), ('MajorEngineVersion', '16.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r27-option-group-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r27-option-group-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-ee.")),
    ('r27-create-option-group-custom-sqlserver-se', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r27-og-custom-sqlserver-se'), ('EngineName', 'custom-sqlserver-se'), ('MajorEngineVersion', '16.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r27-option-group-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r27-option-group-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-se.")),
    ('r27-create-option-group-custom-sqlserver-web', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r27-og-custom-sqlserver-web'), ('EngineName', 'custom-sqlserver-web'), ('MajorEngineVersion', '16.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r27-option-group-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r27-option-group-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-web.")),
    ('r27-create-option-group-custom-sqlserver-dev', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r27-og-custom-sqlserver-dev'), ('EngineName', 'custom-sqlserver-dev'), ('MajorEngineVersion', '15.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r27-option-group-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r27-option-group-existing')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-dev.")),
    ('r34-create-option-group-custom-sqlserver-ee', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r34-og-custom-sqlserver-ee'), ('EngineName', 'custom-sqlserver-ee'), ('MajorEngineVersion', '16.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r34-option-group-case'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r34-option-group-case')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-ee.")),
    ('r34-create-option-group-custom-sqlserver-se', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r34-og-custom-sqlserver-se'), ('EngineName', 'custom-sqlserver-se'), ('MajorEngineVersion', '16.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r34-option-group-case'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r34-option-group-case')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-se.")),
    ('r34-create-option-group-custom-sqlserver-web', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r34-og-custom-sqlserver-web'), ('EngineName', 'custom-sqlserver-web'), ('MajorEngineVersion', '16.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r34-option-group-case'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r34-option-group-case')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-web.")),
    ('r34-create-option-group-custom-sqlserver-dev', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r34-og-custom-sqlserver-dev'), ('EngineName', 'custom-sqlserver-dev'), ('MajorEngineVersion', '15.00'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r34-option-group-case'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r34-option-group-case')], (400, 'InvalidParameterCombination', 'Sender', "You can't create or assign a custom option group for engine custom-sqlserver-dev.")),
    ('r36-d3cs-describe-og-engine-upper', [('Action', 'DescribeOptionGroups'), ('OptionGroupName', 'msc2637-r36-og-pg'), ('EngineName', 'CUSTOM-SQLSERVER-EE')], (404, 'OptionGroupNotFoundFault', 'Sender', 'Specified OptionGroupName: msc2637-r36-og-pg not found.')),
    # G1x: standalone and source-based global clusters (r35-g1x-*).
    ('r35-g1x-standalone-oracle-se2', [('Action', 'CreateGlobalCluster'), ('GlobalClusterIdentifier', 'msc2637-r35-g-oracle-se2'), ('Engine', 'oracle-se2')], (400, 'InvalidParameterValue', 'Sender', 'The requested engine version was not found or does not support global functionality')),
    ('r35-g1x-custom-sqlserver-se-source-blank', [('Action', 'CreateGlobalCluster'), ('GlobalClusterIdentifier', 'msc2637-r35-g-custom-sqlserver-se-sb'), ('Engine', 'custom-sqlserver-se'), ('SourceDBClusterIdentifier', '')], (400, 'InvalidParameterValue', 'Sender', 'The requested engine version was not found or does not support global functionality')),
    ('r35-g1x-standalone-custom-oracle-ee-cdb', [('Action', 'CreateGlobalCluster'), ('GlobalClusterIdentifier', 'msc2637-r35-g-custom-oracle-ee-cdb'), ('Engine', 'custom-oracle-ee-cdb')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r35-g1x-custom-oracle-ee-cdb-source-blank', [('Action', 'CreateGlobalCluster'), ('GlobalClusterIdentifier', 'msc2637-r35-g-custom-oracle-ee-cdb-sb'), ('Engine', 'custom-oracle-ee-cdb'), ('SourceDBClusterIdentifier', '')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r35-g1x-oracle-se2-source-missing', [('Action', 'CreateGlobalCluster'), ('GlobalClusterIdentifier', 'msc2637-r35-g-oracle-se2-sm'), ('Engine', 'oracle-se2'), ('SourceDBClusterIdentifier', 'arn:aws:rds:us-east-1:000000000000:cluster:msc2637-r35-no-such-cluster')], (400, 'InvalidParameterCombination', 'Sender', 'When creating global cluster from existing db cluster, value for engineName should not be specified since it will be inherited from source cluster')),
    ('r35-g1x-custom-oracle-ee-cdb-source-missing', [('Action', 'CreateGlobalCluster'), ('GlobalClusterIdentifier', 'msc2637-r35-g-custom-oracle-ee-cdb-sm'), ('Engine', 'custom-oracle-ee-cdb'), ('SourceDBClusterIdentifier', 'arn:aws:rds:us-east-1:000000000000:cluster:msc2637-r35-no-such-cluster')], (400, 'InvalidParameterCombination', 'Sender', 'When creating global cluster from existing db cluster, value for engineName should not be specified since it will be inherited from source cluster')),
    # I5c / I5cv: standalone aurora (retired) on CreateDBInstance (r13-aurora-*).
    ('r13-aurora-lower', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r13-1-a-lower'), ('Engine', 'aurora'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r13-aurora-standalone'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r13-aurora-standalone'), ('DBSubnetGroupName', 'msc-2633w-sng')], (500, 'InternalFailure', 'Receiver', 'An internal error has occurred. Please try your query again at a later time.')),
    ('r13-aurora-retry-upper', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r13-a-retry'), ('Engine', 'AURORA'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r13-aurora-standalone'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r13-aurora-standalone'), ('DBSubnetGroupName', 'msc-2633w-sng')], (500, 'InternalFailure', 'Receiver', 'An internal error has occurred. Please try your query again at a later time.')),
    ('r13-aurora-lead-space', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r13-5-a-lead-space'), ('Engine', ' aurora'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r13-aurora-standalone'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r13-aurora-standalone'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r13-aurora-trail-space', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r13-6-a-trail-space'), ('Engine', 'aurora '), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r13-aurora-standalone'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r13-aurora-standalone'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r13-aurora-version-56', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r13-7-a-version-56'), ('Engine', 'aurora'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r13-aurora-standalone'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r13-aurora-standalone'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '5.6.10a')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version 5.6.10a for aurora')),
    ('r13-aurora-version-aurora-mysql', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r13-8-a-version-aurora-mysql'), ('Engine', 'aurora'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r13-aurora-standalone'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r13-aurora-standalone'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '5.6.mysql_aurora.1.23.4')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version 5.6.mysql_aurora.1.23.4 for aurora')),
    ('r13-aurora-version-junk', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r13-9-a-version-junk'), ('Engine', 'aurora'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r13-aurora-standalone'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r13-aurora-standalone'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '99.99')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version 99.99 for aurora')),
    ('r13-aurora-version-blank', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r13-10-a-version-blank'), ('Engine', 'aurora'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r13-aurora-standalone'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r13-aurora-standalone'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version  for aurora')),
    ('r13-aurora-class-bad', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r13-11-a-class'), ('Engine', 'aurora'), ('DBInstanceClass', 'db.notaclass'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r13-aurora-standalone'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r13-aurora-standalone'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB Instance class: db.notaclass')),
    ('r13-aurora-upper-class-bad', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r13-12-a-up-class'), ('Engine', 'AURORA'), ('DBInstanceClass', 'db.notaclass'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r13-aurora-standalone'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r13-aurora-standalone'), ('DBSubnetGroupName', 'msc-2633w-sng')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB Instance class: db.notaclass')),
    ('r13-aurora-lower-cluster-missing-bare', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r13-14-a-cm-bare-lower'), ('Engine', 'aurora'), ('DBInstanceClass', 'db.t4g.micro'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r13-aurora-standalone'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r13-aurora-standalone'), ('DBClusterIdentifier', 'msc2637-r13-no-such-cluster')], (404, 'DBClusterNotFoundFault', 'Sender', 'The source cluster could not be found or cannot be accessed: msc2637-r13-no-such-cluster')),
    ('r13-aurora-cluster-missing-arn-engine-last', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r13-21-a-cm-arn-last'), ('DBInstanceClass', 'db.t4g.micro'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r13-aurora-standalone'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r13-aurora-standalone'), ('DBClusterIdentifier', 'arn:aws:rds:us-east-1:000000000000:cluster:msc2637-r13-no-such-cluster'), ('Engine', 'aurora')], (404, 'DBClusterNotFoundFault', 'Sender', 'The source cluster could not be found or cannot be accessed: arn:aws:rds:us-east-1:000000000000:cluster:msc2637-r13-no-such-cluster')),
    # Describe side: engine echo, engine sets, option-group case (r29-echo-upper, r30, r26, r27, r34).
    ('r29-echo-upper-unknown-engine-versions', [('Action', 'DescribeDBEngineVersions'), ('Engine', 'POSTGRESS')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgress')),
    ('r30-versions-engine-plain', [('Action', 'DescribeDBEngineVersions'), ('Engine', 'postgress')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgress')),
    ('r29-echo-upper-unknown-option-groups', [('Action', 'DescribeOptionGroups'), ('EngineName', 'POSTGRESS')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgress')),
    ('r34-describe-oracle-se1-engine-unknown', [('Action', 'DescribeOptionGroups'), ('OptionGroupName', 'msc2637-r34-og-oracle-se1'), ('EngineName', 'postgress')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgress')),
    ('r29-echo-upper-unknown-filter-instances', [('Action', 'DescribeDBInstances'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'POSTGRESS')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgress')),
    ('r30-filter-instances-engine-plain', [('Action', 'DescribeDBInstances'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'postgress')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgress')),
    ('r29-echo-upper-unknown-filter-clusters', [('Action', 'DescribeDBClusters'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'POSTGRESS')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgress')),
    ('r30-filter-clusters-engine-plain', [('Action', 'DescribeDBClusters'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'postgress')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgress')),
    ('r29-echo-upper-unknown-filter-snapshots', [('Action', 'DescribeDBSnapshots'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'POSTGRESS')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgress')),
    ('r30-filter-snapshots-engine-plain', [('Action', 'DescribeDBSnapshots'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'postgress')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgress')),
    ('r29-echo-upper-unknown-filter-cluster-snapshots', [('Action', 'DescribeDBClusterSnapshots'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'POSTGRESS')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgress')),
    ('r30-filter-cluster-snapshots-engine-plain', [('Action', 'DescribeDBClusterSnapshots'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'postgress')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: postgress')),
    ('r29-echo-upper-aurora-engine-versions', [('Action', 'DescribeDBEngineVersions'), ('Engine', 'AURORA')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: aurora')),
    ('r29-echo-upper-aurora-option-groups', [('Action', 'DescribeOptionGroups'), ('EngineName', 'AURORA')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: aurora')),
    ('r26-option-groups-aurora', [('Action', 'DescribeOptionGroups'), ('EngineName', 'aurora'), ('MaxRecords', '20')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: aurora')),
    ('r29-echo-upper-aurora-filter-instances', [('Action', 'DescribeDBInstances'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'AURORA')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: aurora')),
    ('r29-echo-upper-aurora-filter-clusters', [('Action', 'DescribeDBClusters'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'AURORA')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: aurora')),
    ('r29-echo-upper-aurora-filter-snapshots', [('Action', 'DescribeDBSnapshots'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'AURORA')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: aurora')),
    ('r29-echo-upper-aurora-filter-cluster-snapshots', [('Action', 'DescribeDBClusterSnapshots'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'AURORA')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: aurora')),
    ('r29-echo-upper-aurora-unknown-engine-versions', [('Action', 'DescribeDBEngineVersions'), ('Engine', 'AURORA-POSTGRESS')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: aurora-postgress')),
    ('r29-echo-upper-aurora-unknown-option-groups', [('Action', 'DescribeOptionGroups'), ('EngineName', 'AURORA-POSTGRESS')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: aurora-postgress')),
    ('r29-echo-upper-aurora-unknown-filter-instances', [('Action', 'DescribeDBInstances'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'AURORA-POSTGRESS')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: aurora-postgress')),
    ('r29-echo-upper-aurora-unknown-filter-clusters', [('Action', 'DescribeDBClusters'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'AURORA-POSTGRESS')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: aurora-postgress')),
    ('r29-echo-upper-aurora-unknown-filter-snapshots', [('Action', 'DescribeDBSnapshots'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'AURORA-POSTGRESS')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: aurora-postgress')),
    ('r29-echo-upper-aurora-unknown-filter-cluster-snapshots', [('Action', 'DescribeDBClusterSnapshots'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'AURORA-POSTGRESS')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: aurora-postgress')),
    ('r29-echo-upper-known-engine-versions', [('Action', 'DescribeDBEngineVersions'), ('Engine', 'POSTGRES')], (200, None, None, None)),
    ('r29-echo-upper-known-option-groups', [('Action', 'DescribeOptionGroups'), ('EngineName', 'POSTGRES')], (200, None, None, None)),
    ('r26-option-groups-docdb', [('Action', 'DescribeOptionGroups'), ('EngineName', 'docdb'), ('MaxRecords', '20')], (200, None, None, None)),
    ('r29-echo-upper-known-filter-instances', [('Action', 'DescribeDBInstances'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'POSTGRES')], (200, None, None, None)),
    ('r29-echo-upper-known-filter-clusters', [('Action', 'DescribeDBClusters'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'POSTGRES')], (200, None, None, None)),
    ('r29-echo-upper-known-filter-snapshots', [('Action', 'DescribeDBSnapshots'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'POSTGRES')], (200, None, None, None)),
    ('r29-echo-upper-known-filter-cluster-snapshots', [('Action', 'DescribeDBClusterSnapshots'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'POSTGRES')], (200, None, None, None)),
    ('r30-versions-engine-soh', [('Action', 'DescribeDBEngineVersions'), ('Engine', 'post\x01gres')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-versions-engine-tab', [('Action', 'DescribeDBEngineVersions'), ('Engine', 'postgress\t')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-option-groups-engine-soh', [('Action', 'DescribeOptionGroups'), ('EngineName', 'post\x01gres')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-option-groups-engine-tab', [('Action', 'DescribeOptionGroups'), ('EngineName', 'postgress\t')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-option-group-options-engine-soh', [('Action', 'DescribeOptionGroupOptions'), ('EngineName', 'post\x01gres')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-option-group-options-engine-tab', [('Action', 'DescribeOptionGroupOptions'), ('EngineName', 'postgress\t')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-orderable-engine-soh', [('Action', 'DescribeOrderableDBInstanceOptions'), ('Engine', 'post\x01gres')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-orderable-engine-tab', [('Action', 'DescribeOrderableDBInstanceOptions'), ('Engine', 'postgress\t')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-filter-instances-engine-soh', [('Action', 'DescribeDBInstances'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'post\x01gres')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-filter-instances-engine-tab', [('Action', 'DescribeDBInstances'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'postgress\t')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-filter-clusters-engine-soh', [('Action', 'DescribeDBClusters'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'post\x01gres')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-filter-clusters-engine-tab', [('Action', 'DescribeDBClusters'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'postgress\t')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-filter-snapshots-engine-soh', [('Action', 'DescribeDBSnapshots'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'post\x01gres')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-filter-snapshots-engine-tab', [('Action', 'DescribeDBSnapshots'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'postgress\t')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-filter-cluster-snapshots-engine-soh', [('Action', 'DescribeDBClusterSnapshots'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'post\x01gres')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-filter-cluster-snapshots-engine-tab', [('Action', 'DescribeDBClusterSnapshots'), ('Filters.Filter.1.Name', 'engine'), ('Filters.Filter.1.Values.Value.1', 'postgress\t')], (400, 'InvalidParameterValue', 'Sender', "The input isn't valid. Input can't contain control characters.")),
    ('r30-option-group-options-engine-plain', [('Action', 'DescribeOptionGroupOptions'), ('EngineName', 'postgress')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r30-orderable-engine-plain', [('Action', 'DescribeOrderableDBInstanceOptions'), ('Engine', 'postgress')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r26-orderable-custom-sqlserver-class-bad', [('Action', 'DescribeOrderableDBInstanceOptions'), ('Engine', 'custom-sqlserver'), ('DBInstanceClass', 'db.notaclass'), ('MaxRecords', '20')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r30-delete-option-group-plain', [('Action', 'DeleteOptionGroup'), ('OptionGroupName', 'x-y')], (404, 'OptionGroupNotFoundFault', 'Sender', 'Specified OptionGroupName: x-y not found.')),
    ('r30-describe-option-group-plain', [('Action', 'DescribeOptionGroups'), ('OptionGroupName', 'x-y')], (404, 'OptionGroupNotFoundFault', 'Sender', 'Specified OptionGroupName: x-y not found.')),
    ('r30-member-cluster-plain', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r30-17-m-plain'), ('DBClusterIdentifier', 'x-y'), ('Engine', 'aurora-postgresql'), ('DBInstanceClass', 'db.r6g.large'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r30-control-char-echo'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r30-control-char-echo')], (404, 'DBClusterNotFoundFault', 'Sender', 'The source cluster could not be found or cannot be accessed: x-y')),
    ('r30-standalone-version-plain', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r30-19-ver-plain'), ('Engine', 'aurora-postgresql'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r30-control-char-echo'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r30-control-char-echo'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('EngineVersion', '1-')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find version 1- for aurora-postgresql')),
    ('r26-option-groups-custom-oracle-ee', [('Action', 'DescribeOptionGroups'), ('EngineName', 'custom-oracle-ee'), ('MaxRecords', '20')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: custom-oracle-ee')),
    ('r34-describe-custom-oracle-ee-engine-upper', [('Action', 'DescribeOptionGroups'), ('OptionGroupName', 'msc2637-r34-og-custom-oracle-ee'), ('EngineName', 'CUSTOM-ORACLE-EE')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: custom-oracle-ee')),
    ('r26-option-groups-custom-oracle-ee-cdb', [('Action', 'DescribeOptionGroups'), ('EngineName', 'custom-oracle-ee-cdb'), ('MaxRecords', '20')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: custom-oracle-ee-cdb')),
    ('r34-describe-custom-oracle-ee-cdb-engine-upper', [('Action', 'DescribeOptionGroups'), ('OptionGroupName', 'msc2637-r34-og-custom-oracle-ee-cdb'), ('EngineName', 'CUSTOM-ORACLE-EE-CDB')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: custom-oracle-ee-cdb')),
    ('r26-option-groups-custom-oracle-se2', [('Action', 'DescribeOptionGroups'), ('EngineName', 'custom-oracle-se2'), ('MaxRecords', '20')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: custom-oracle-se2')),
    ('r34-describe-custom-oracle-se2-engine-upper', [('Action', 'DescribeOptionGroups'), ('OptionGroupName', 'msc2637-r34-og-custom-oracle-se2'), ('EngineName', 'CUSTOM-ORACLE-SE2')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: custom-oracle-se2')),
    ('r26-option-groups-custom-oracle-se2-cdb', [('Action', 'DescribeOptionGroups'), ('EngineName', 'custom-oracle-se2-cdb'), ('MaxRecords', '20')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: custom-oracle-se2-cdb')),
    ('r34-describe-custom-oracle-se2-cdb-engine-upper', [('Action', 'DescribeOptionGroups'), ('OptionGroupName', 'msc2637-r34-og-custom-oracle-se2-cdb'), ('EngineName', 'CUSTOM-ORACLE-SE2-CDB')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: custom-oracle-se2-cdb')),
    ('r26-option-groups-oracle-se', [('Action', 'DescribeOptionGroups'), ('EngineName', 'oracle-se'), ('MaxRecords', '20')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: oracle-se')),
    ('r34-describe-oracle-se-engine-upper', [('Action', 'DescribeOptionGroups'), ('OptionGroupName', 'msc2637-r34-og-oracle-se'), ('EngineName', 'ORACLE-SE')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: oracle-se')),
    ('r26-option-groups-oracle-se1', [('Action', 'DescribeOptionGroups'), ('EngineName', 'oracle-se1'), ('MaxRecords', '20')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: oracle-se1')),
    ('r34-describe-oracle-se1-engine-upper', [('Action', 'DescribeOptionGroups'), ('OptionGroupName', 'msc2637-r34-og-oracle-se1'), ('EngineName', 'ORACLE-SE1')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: oracle-se1')),
    ('r26-option-groups-custom-oracle', [('Action', 'DescribeOptionGroups'), ('EngineName', 'custom-oracle'), ('MaxRecords', '20')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: custom-oracle')),
    ('r26-option-groups-custom-sqlserver', [('Action', 'DescribeOptionGroups'), ('EngineName', 'custom-sqlserver'), ('MaxRecords', '20')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: custom-sqlserver')),
    ('r26-option-groups-db2', [('Action', 'DescribeOptionGroups'), ('EngineName', 'db2'), ('MaxRecords', '20')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: db2')),
    ('r26-option-groups-sqlserver', [('Action', 'DescribeOptionGroups'), ('EngineName', 'sqlserver'), ('MaxRecords', '20')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: sqlserver')),
    ('r26-option-groups-oracle', [('Action', 'DescribeOptionGroups'), ('EngineName', 'oracle'), ('MaxRecords', '20')], (400, 'InvalidParameterValue', 'Sender', 'Unrecognized engine name: oracle')),
    ('r26-orderable-custom-sqlserver-ee', [('Action', 'DescribeOrderableDBInstanceOptions'), ('Engine', 'custom-sqlserver-ee'), ('MaxRecords', '20')], (200, None, None, None)),
    ('r26-orderable-custom-sqlserver-web-class-bad', [('Action', 'DescribeOrderableDBInstanceOptions'), ('Engine', 'custom-sqlserver-web'), ('DBInstanceClass', 'db.notaclass'), ('MaxRecords', '20')], (200, None, None, None)),
    ('r27-create-option-group-custom-oracle-ee', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r27-og-custom-oracle-ee'), ('EngineName', 'custom-oracle-ee'), ('MajorEngineVersion', '19'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r27-option-group-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r27-option-group-existing')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r34-create-option-group-custom-oracle-se2-cdb', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r34-og-custom-oracle-se2-cdb'), ('EngineName', 'custom-oracle-se2-cdb'), ('MajorEngineVersion', '19'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r34-option-group-case'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r34-option-group-case')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r27-create-option-group-oracle-se', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r27-og-oracle-se'), ('EngineName', 'oracle-se'), ('MajorEngineVersion', '19'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r27-option-group-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r27-option-group-existing')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find major version 19 for oracle-se')),
    ('r34-create-option-group-oracle-se', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r34-og-oracle-se'), ('EngineName', 'oracle-se'), ('MajorEngineVersion', '19'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r34-option-group-case'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r34-option-group-case')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find major version 19 for oracle-se')),
    ('r27-create-option-group-oracle-se1', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r27-og-oracle-se1'), ('EngineName', 'oracle-se1'), ('MajorEngineVersion', '19'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r27-option-group-existing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r27-option-group-existing')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find major version 19 for oracle-se1')),
    ('r34-create-option-group-oracle-se1', [('Action', 'CreateOptionGroup'), ('OptionGroupName', 'msc2637-r34-og-oracle-se1'), ('EngineName', 'oracle-se1'), ('MajorEngineVersion', '19'), ('OptionGroupDescription', 'ministack capture 2637'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r34-option-group-case'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r34-option-group-case')], (400, 'InvalidParameterCombination', 'Sender', 'Cannot find major version 19 for oracle-se1')),
    ('r34-describe-upper-aurora-mysql-engine-other', [('Action', 'DescribeOptionGroups'), ('OptionGroupName', 'msc2637-r34-og-upper-aurora-mysql'), ('EngineName', 'postgres')], (404, 'OptionGroupNotFoundFault', 'Sender', 'Specified OptionGroupName: msc2637-r34-og-upper-aurora-mysql not found.')),
    ('r34-describe-oracle-se1-engine-other', [('Action', 'DescribeOptionGroups'), ('OptionGroupName', 'msc2637-r34-og-oracle-se1'), ('EngineName', 'postgres')], (404, 'OptionGroupNotFoundFault', 'Sender', 'Specified OptionGroupName: msc2637-r34-og-oracle-se1 not found.')),
    ('r34-delete-upper-custom-oracle-ee', [('Action', 'DeleteOptionGroup'), ('OptionGroupName', 'msc2637-r34-og-upper-custom-oracle-ee')], (404, 'OptionGroupNotFoundFault', 'Sender', 'Specified OptionGroupName: msc2637-r34-og-upper-custom-oracle-ee not found.')),
    ('r34-delete-oracle-se1', [('Action', 'DeleteOptionGroup'), ('OptionGroupName', 'msc2637-r34-og-oracle-se1')], (404, 'OptionGroupNotFoundFault', 'Sender', 'Specified OptionGroupName: msc2637-r34-og-oracle-se1 not found.')),
    # W1len and the typed-integer keys of modify, replica, restore and cluster (r28-*, r15-modify-*).
    ('r28-zeros-inst-port-len-104', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r28-1-z-ip'), ('Engine', 'postgress'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r28-w1-bounds-missing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r28-w1-bounds-missing'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('Port', '00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005432')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r28-zeros-inst-port-edge-above', [('Action', 'CreateDBInstance'), ('DBInstanceIdentifier', 'msc2637-r28-12-z-ip'), ('Engine', 'postgress'), ('DBInstanceClass', 'db.t4g.micro'), ('AllocatedStorage', '20'), ('StorageType', 'gp2'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '0'), ('MultiAZ', 'false'), ('PubliclyAccessible', 'false'), ('DeletionProtection', 'false'), ('AutoMinorVersionUpgrade', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r28-w1-bounds-missing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r28-w1-bounds-missing'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('Port', '000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005432')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r28-zeros-cluster-port-len-104', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r28-13-z-cp'), ('Engine', 'postgress'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r28-w1-bounds-missing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r28-w1-bounds-missing'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('Port', '00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005432')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r28-sv2-cluster-unknown-valid', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r28-48-sv2-valid'), ('Engine', 'postgress'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r28-w1-bounds-missing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r28-w1-bounds-missing'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('ServerlessV2ScalingConfiguration.MinCapacity', '0.5'), ('ServerlessV2ScalingConfiguration.MaxCapacity', '1')], (400, 'InvalidParameterValue', 'Sender', 'Invalid DB engine')),
    ('r28-blank-modify-missing-dbportnumber-last', [('Action', 'ModifyDBInstance'), ('DBInstanceIdentifier', 'msc2637-r28-no-such-db'), ('ApplyImmediately', 'true'), ('DBPortNumber', '')], (400, 'MalformedInput', 'Sender', 'missing value for decimal type')),
    ('r28-int-modify-missing-storagethroughput-blank-first', [('Action', 'ModifyDBInstance'), ('StorageThroughput', ''), ('DBInstanceIdentifier', 'msc2637-r28-no-such-db'), ('ApplyImmediately', 'true')], (400, 'MalformedInput', 'Sender', 'missing value for decimal type')),
    ('r28-blank-modify-cluster-missing-port-last', [('Action', 'ModifyDBCluster'), ('DBClusterIdentifier', 'msc2637-r28-no-such-cluster'), ('ApplyImmediately', 'true'), ('Port', '')], (400, 'MalformedInput', 'Sender', 'missing value for decimal type')),
    ('r28-int-modify-cluster-missing-iops-blank-first', [('Action', 'ModifyDBCluster'), ('Iops', ''), ('DBClusterIdentifier', 'msc2637-r28-no-such-cluster'), ('ApplyImmediately', 'true')], (400, 'MalformedInput', 'Sender', 'missing value for decimal type')),
    ('r28-blank-restore-missing-port-last', [('Action', 'RestoreDBInstanceFromDBSnapshot'), ('DBSnapshotIdentifier', 'msc2637-r28-no-such-snap'), ('DBInstanceIdentifier', 'msc2637-r28-37-rs-blank'), ('Port', '')], (400, 'MalformedInput', 'Sender', 'missing value for decimal type')),
    ('r28-blank-restore-missing-port-first', [('Action', 'RestoreDBInstanceFromDBSnapshot'), ('Port', ''), ('DBSnapshotIdentifier', 'msc2637-r28-no-such-snap'), ('DBInstanceIdentifier', 'msc2637-r28-37-rs-blank')], (400, 'MalformedInput', 'Sender', 'missing value for decimal type')),
    ('r28-blank-replica-missing-port-last', [('Action', 'CreateDBInstanceReadReplica'), ('SourceDBInstanceIdentifier', 'msc2637-r28-no-such-db'), ('DBInstanceIdentifier', 'msc2637-r28-38-rr-blank'), ('Port', '')], (400, 'MalformedInput', 'Sender', 'missing value for decimal type')),
    ('r28-blank-replica-missing-port-first', [('Action', 'CreateDBInstanceReadReplica'), ('Port', ''), ('SourceDBInstanceIdentifier', 'msc2637-r28-no-such-db'), ('DBInstanceIdentifier', 'msc2637-r28-38-rr-blank')], (400, 'MalformedInput', 'Sender', 'missing value for decimal type')),
    ('r28-int-modify-missing-iops-abc', [('Action', 'ModifyDBInstance'), ('DBInstanceIdentifier', 'msc2637-r28-no-such-db'), ('ApplyImmediately', 'true'), ('Iops', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r15-modify-no-id-port-abc', [('Action', 'ModifyDBInstance'), ('DBPortNumber', 'abc'), ('ApplyImmediately', 'true')], (400, 'MalformedInput', 'Sender', None)),
    ('r28-int-modify-cluster-missing-backupretentionperiod-abc', [('Action', 'ModifyDBCluster'), ('DBClusterIdentifier', 'msc2637-r28-no-such-cluster'), ('ApplyImmediately', 'true'), ('BackupRetentionPeriod', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r28-int-modify-cluster-missing-port-int32-over', [('Action', 'ModifyDBCluster'), ('DBClusterIdentifier', 'msc2637-r28-no-such-cluster'), ('ApplyImmediately', 'true'), ('Port', '2147483648')], (400, 'MalformedInput', 'Sender', None)),
    ('r28-int-replica-missing-maxallocatedstorage-abc', [('Action', 'CreateDBInstanceReadReplica'), ('SourceDBInstanceIdentifier', 'msc2637-r28-no-such-db'), ('DBInstanceIdentifier', 'msc2637-r28-39-rr-maxa'), ('MaxAllocatedStorage', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r28-int-replica-missing-allocatedstorage-abc', [('Action', 'CreateDBInstanceReadReplica'), ('SourceDBInstanceIdentifier', 'msc2637-r28-no-such-db'), ('DBInstanceIdentifier', 'msc2637-r28-41-rr-allo'), ('AllocatedStorage', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r28-int-restore-missing-iops-abc', [('Action', 'RestoreDBInstanceFromDBSnapshot'), ('DBSnapshotIdentifier', 'msc2637-r28-no-such-snap'), ('DBInstanceIdentifier', 'msc2637-r28-42-rs-iops'), ('Iops', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r28-int-restore-missing-backupretentionperiod-abc', [('Action', 'RestoreDBInstanceFromDBSnapshot'), ('DBSnapshotIdentifier', 'msc2637-r28-no-such-snap'), ('DBInstanceIdentifier', 'msc2637-r28-45-rs-back'), ('BackupRetentionPeriod', 'abc')], (400, 'MalformedInput', 'Sender', None)),
    ('r28-sv2-cluster-unknown-min-abc', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r28-46-sv2-min-abc'), ('Engine', 'postgress'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r28-w1-bounds-missing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r28-w1-bounds-missing'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('ServerlessV2ScalingConfiguration.MinCapacity', 'abc'), ('ServerlessV2ScalingConfiguration.MaxCapacity', '1')], (400, 'MalformedInput', 'Sender', None)),
    ('r28-sv2-cluster-unknown-max-abc', [('Action', 'CreateDBCluster'), ('DBClusterIdentifier', 'msc2637-r28-47-sv2-max-abc'), ('Engine', 'postgress'), ('MasterUsername', 'mscadmin'), ('MasterUserPassword', 'MsCapture2637pw'), ('BackupRetentionPeriod', '1'), ('DeletionProtection', 'false'), ('Tags.Tag.1.Key', 'Owner'), ('Tags.Tag.1.Value', 'ministack-capture'), ('Tags.Tag.2.Key', 'Purpose'), ('Tags.Tag.2.Value', 'ministack evidence msc-2637-r28-w1-bounds-missing'), ('Tags.Tag.3.Key', 'Prefix'), ('Tags.Tag.3.Value', 'msc-2637-r28-w1-bounds-missing'), ('DBSubnetGroupName', 'msc-2633w-sng'), ('ServerlessV2ScalingConfiguration.MinCapacity', '0.5'), ('ServerlessV2ScalingConfiguration.MaxCapacity', 'abc')], (400, 'MalformedInput', 'Sender', None)),
]


@pytest.mark.parametrize("key,pairs,expected", _COVERAGE_CAPTURED, ids=[c[0] for c in _COVERAGE_CAPTURED])
def test_rds_pass4_coverage_matches_capture(key, pairs, expected):
    assert _post(pairs)[:4] == expected


def test_rds_java_double_is_linear_on_long_hex_input():
    # Pass 4 code finding 1: the hex branch backtracked quadratically; the spellings stay the same.
    import time

    from ministack.services import rds as rds_module

    started = time.monotonic()
    assert rds_module._java_double("0x" + "a" * 200000 + "q") is None
    assert rds_module._java_double("0x" + "a" * 200000 + "p1") is not None
    assert time.monotonic() - started < 0.5
    for text in ("0x1.8p1", "0x.8p1", "0x1.p1", "0x1p1", "0X1P-1d"):
        assert rds_module._java_double(text) is not None
    for text in ("0x", "0x.p1", "0x1.8", "0x1..8p1", "0x1.8.8p1"):
        assert rds_module._java_double(text) is None


def test_rds_cluster_global_arn_is_invalid_before_the_absent_engine():
    # r23-n13-global-arn-global-engine-absent (AWS record): no Engine, GlobalClusterIdentifier an ARN.
    body = (
        "Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=msc2637-r23-21-g-arn-global-a"
        "&MasterUsername=mscadmin&MasterUserPassword=MsCapture2637pw&BackupRetentionPeriod=1"
        "&DeletionProtection=false&DBSubnetGroupName=msc-2633w-sng&GlobalClusterIdentifier="
        "arn:aws:rds::000000000000:global-cluster:msc2637-r23-no-such-global"
    )
    status, code, kind, message, _ = _post([tuple(kv.split("=", 1)) for kv in body.split("&")])
    assert (status, code, kind, message) == (
        400, "InvalidParameterValue", "Sender",
        "Invalid global cluster identifier:  "
        "arn:aws:rds::000000000000:global-cluster:msc2637-r23-no-such-global",
    )


def test_rds_read_replica_non_integer_backupretentionperiod_on_existing_source(rds):
    # r43-g-replica-source-existing-backupretentionperiod-abc: AWS answers 400 InvalidDBInstanceState
    # 'Automated backups are not enabled...' for the retention-0 source.  MiniStack does not add that
    # check (known difference q5-replica-backups); it reads the non-integer as absent and creates the
    # replica where the merge base answered 500 InternalError.
    source, replica = _name("r3-es-src"), _name("r3-es-rep")
    try:
        assert _post(_instance(source, Engine="postgres", BackupRetentionPeriod="0"))[0] == 200
        status, code, *_ = _post([
            ('Action', 'CreateDBInstanceReadReplica'), ('DBInstanceIdentifier', replica),
            ('SourceDBInstanceIdentifier', source), ('DBInstanceClass', 'db.t4g.micro'),
            *_CAPTURE_TAGS, ('BackupRetentionPeriod', 'abc')])
        assert (status, code) == (200, None)
    finally:
        _delete_instance(replica)
        _delete_instance(source)


def test_rds_read_replica_oversized_backupretentionperiod_is_not_a_crash(rds):
    # R3 / A4: an ASCII value over 4300 digits reads as absent, not a 500.
    source = _name("r3-src")
    replica = _name("r3-rep")
    try:
        assert _post(_instance(source, Engine="postgres"))[0] == 200
        status, code, *_ = _post({
            "Action": "CreateDBInstanceReadReplica", "DBInstanceIdentifier": replica,
            "SourceDBInstanceIdentifier": source, "BackupRetentionPeriod": "9" * 4400,
        })
        assert (status, code) == (200, None)
    finally:
        _delete_instance(replica)
        _delete_instance(source)
