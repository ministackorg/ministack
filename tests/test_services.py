"""
MiniStack Integration Tests — pytest edition.
Run: pytest tests/ -v
Requires: pip install boto3 pytest
"""

import io
import json
import time
import zipfile


# ========== S3 ==========

def test_s3_create_bucket(s3):
    s3.create_bucket(Bucket="test-bucket-1")
    buckets = s3.list_buckets()["Buckets"]
    assert any(b["Name"] == "test-bucket-1" for b in buckets)


def test_s3_put_get(s3):
    s3.create_bucket(Bucket="test-bucket-2")
    s3.put_object(Bucket="test-bucket-2", Key="hello.txt", Body=b"Hello, MiniStack!")
    resp = s3.get_object(Bucket="test-bucket-2", Key="hello.txt")
    assert resp["Body"].read() == b"Hello, MiniStack!"


def test_s3_list(s3):
    s3.create_bucket(Bucket="test-bucket-3")
    for i in range(5):
        s3.put_object(Bucket="test-bucket-3", Key=f"file-{i}.txt", Body=f"content-{i}".encode())
    resp = s3.list_objects_v2(Bucket="test-bucket-3")
    assert resp["KeyCount"] == 5


def test_s3_delete(s3):
    s3.create_bucket(Bucket="test-bucket-4")
    s3.put_object(Bucket="test-bucket-4", Key="to-delete.txt", Body=b"bye")
    s3.delete_object(Bucket="test-bucket-4", Key="to-delete.txt")
    resp = s3.list_objects_v2(Bucket="test-bucket-4")
    assert resp["KeyCount"] == 0


def test_s3_copy(s3):
    s3.create_bucket(Bucket="test-bucket-5a")
    s3.create_bucket(Bucket="test-bucket-5b")
    s3.put_object(Bucket="test-bucket-5a", Key="original.txt", Body=b"original content")
    s3.copy_object(
        CopySource={"Bucket": "test-bucket-5a", "Key": "original.txt"},
        Bucket="test-bucket-5b", Key="copied.txt",
    )
    resp = s3.get_object(Bucket="test-bucket-5b", Key="copied.txt")
    assert resp["Body"].read() == b"original content"


def test_s3_head(s3):
    s3.create_bucket(Bucket="test-bucket-6")
    s3.put_object(Bucket="test-bucket-6", Key="meta.txt", Body=b"12345")
    resp = s3.head_object(Bucket="test-bucket-6", Key="meta.txt")
    assert resp["ContentLength"] == 5


# ========== SQS ==========

def test_sqs_basic(sqs):
    resp = sqs.create_queue(QueueName="test-queue-1")
    url = resp["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="hello sqs")
    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1)
    assert len(msgs.get("Messages", [])) == 1
    assert msgs["Messages"][0]["Body"] == "hello sqs"


def test_sqs_delete_msg(sqs):
    resp = sqs.create_queue(QueueName="test-queue-2")
    url = resp["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="to delete")
    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1)
    receipt = msgs["Messages"][0]["ReceiptHandle"]
    sqs.delete_message(QueueUrl=url, ReceiptHandle=receipt)


def test_sqs_list(sqs):
    sqs.create_queue(QueueName="test-queue-list-1")
    sqs.create_queue(QueueName="test-queue-list-2")
    resp = sqs.list_queues()
    assert len(resp.get("QueueUrls", [])) >= 2


def test_sqs_batch(sqs):
    resp = sqs.create_queue(QueueName="test-queue-batch")
    url = resp["QueueUrl"]
    sqs.send_message_batch(
        QueueUrl=url,
        Entries=[{"Id": str(i), "MessageBody": f"msg{i}"} for i in range(3)],
    )
    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
    assert len(msgs.get("Messages", [])) >= 1


# ========== SNS ==========

def test_sns_basic(sns):
    resp = sns.create_topic(Name="test-topic-1")
    arn = resp["TopicArn"]
    assert "test-topic-1" in arn
    pub = sns.publish(TopicArn=arn, Message="hello sns", Subject="Test")
    assert "MessageId" in pub


def test_sns_subscribe(sns):
    resp = sns.create_topic(Name="test-topic-2")
    arn = resp["TopicArn"]
    sub = sns.subscribe(TopicArn=arn, Protocol="email", Endpoint="test@example.com")
    assert "SubscriptionArn" in sub
    subs = sns.list_subscriptions_by_topic(TopicArn=arn)
    assert len(subs.get("Subscriptions", [])) >= 1


def test_sns_sqs_fanout(sns, sqs):
    # Create topic and queue
    topic_arn = sns.create_topic(Name="fanout-topic")["TopicArn"]
    q_url = sqs.create_queue(QueueName="fanout-queue")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

    # Subscribe SQS queue to SNS topic
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)

    # Publish to topic
    sns.publish(TopicArn=topic_arn, Message="hello fanout", Subject="test")

    # Message should appear in SQS queue
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1)
    assert len(msgs.get("Messages", [])) == 1
    import json
    body = json.loads(msgs["Messages"][0]["Body"])
    assert body["Message"] == "hello fanout"
    assert body["TopicArn"] == topic_arn


# ========== DynamoDB ==========

def test_dynamodb_basic(ddb):
    try:
        ddb.delete_table(TableName="TestTable1")
    except Exception:
        pass
    ddb.create_table(
        TableName="TestTable1",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName="TestTable1", Item={"pk": {"S": "key1"}, "data": {"S": "value1"}})
    resp = ddb.get_item(TableName="TestTable1", Key={"pk": {"S": "key1"}})
    assert resp["Item"]["data"]["S"] == "value1"
    ddb.delete_item(TableName="TestTable1", Key={"pk": {"S": "key1"}})
    resp = ddb.get_item(TableName="TestTable1", Key={"pk": {"S": "key1"}})
    assert "Item" not in resp


def test_dynamodb_scan(ddb):
    try:
        ddb.delete_table(TableName="ScanTable")
    except Exception:
        pass
    ddb.create_table(
        TableName="ScanTable",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(10):
        ddb.put_item(TableName="ScanTable", Item={"pk": {"S": f"key{i}"}, "val": {"N": str(i)}})
    resp = ddb.scan(TableName="ScanTable")
    assert resp["Count"] == 10


def test_dynamodb_batch(ddb):
    try:
        ddb.delete_table(TableName="BatchTable")
    except Exception:
        pass
    ddb.create_table(
        TableName="BatchTable",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.batch_write_item(RequestItems={
        "BatchTable": [
            {"PutRequest": {"Item": {"pk": {"S": f"bk{i}"}, "v": {"S": f"bv{i}"}}}}
            for i in range(5)
        ]
    })
    resp = ddb.scan(TableName="BatchTable")
    assert resp["Count"] == 5


# ========== STS ==========

def test_sts_get_caller_identity(sts):
    resp = sts.get_caller_identity()
    assert resp["Account"] == "000000000000"


# ========== SecretsManager ==========

def test_secrets_create_get(sm):
    sm.create_secret(Name="test-secret-1", SecretString='{"user":"admin"}')
    resp = sm.get_secret_value(SecretId="test-secret-1")
    assert json.loads(resp["SecretString"])["user"] == "admin"


def test_secrets_update_list(sm):
    sm.create_secret(Name="test-secret-2", SecretString="original")
    sm.update_secret(SecretId="test-secret-2", SecretString="updated")
    resp = sm.get_secret_value(SecretId="test-secret-2")
    assert resp["SecretString"] == "updated"
    listed = sm.list_secrets()
    assert any(s["Name"] == "test-secret-2" for s in listed["SecretList"])


# ========== CloudWatch Logs ==========

def test_logs_put_get(logs):
    logs.create_log_group(logGroupName="/test/ministack")
    logs.create_log_stream(logGroupName="/test/ministack", logStreamName="stream1")
    logs.put_log_events(
        logGroupName="/test/ministack",
        logStreamName="stream1",
        logEvents=[
            {"timestamp": int(time.time() * 1000), "message": "Hello from MiniStack"},
            {"timestamp": int(time.time() * 1000), "message": "Second log line"},
        ],
    )
    resp = logs.get_log_events(logGroupName="/test/ministack", logStreamName="stream1")
    assert len(resp["events"]) == 2


def test_logs_filter(logs):
    resp = logs.filter_log_events(logGroupName="/test/ministack", filterPattern="MiniStack")
    assert len(resp["events"]) >= 1


# ========== Lambda ==========

def test_lambda_create_invoke(lam):
    code = b'def handler(event, context):\n    return {"statusCode": 200, "body": "Hello!", "event": event}\n'
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName="test-func-1",
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    funcs = lam.list_functions()
    assert any(f["FunctionName"] == "test-func-1" for f in funcs["Functions"])
    resp = lam.invoke(FunctionName="test-func-1", Payload=json.dumps({"key": "value"}))
    payload = json.loads(resp["Payload"].read())
    assert payload["statusCode"] == 200


# ========== IAM ==========

def test_lambda_esm_sqs(lam, sqs):
    """SQS → Lambda event source mapping: messages sent to SQS trigger Lambda."""
    import io, zipfile as zf

    # Clean up from previous runs
    try:
        lam.delete_function(FunctionName="esm-test-func")
    except Exception:
        pass

    # Lambda that records what it received
    code = (
        b"import json\n"
        b"received = []\n"
        b"def handler(event, context):\n"
        b"    received.extend(event.get('Records', []))\n"
        b"    return {'processed': len(event.get('Records', []))}\n"
    )
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w") as z:
        z.writestr("index.py", code)

    lam.create_function(
        FunctionName="esm-test-func",        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    q_url = sqs.create_queue(QueueName="esm-test-queue")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

    # Create event source mapping
    resp = lam.create_event_source_mapping(
        EventSourceArn=q_arn,
        FunctionName="esm-test-func",
        BatchSize=5,
        Enabled=True,
    )
    esm_uuid = resp["UUID"]
    assert resp["State"] == "Enabled"

    # Send a message to SQS
    sqs.send_message(QueueUrl=q_url, MessageBody="trigger-lambda")

    # Wait for poller to pick it up (max 5s)
    import time
    for _ in range(10):
        time.sleep(0.5)
        msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1)
        if not msgs.get("Messages"):
            break  # message was consumed by Lambda

    # Queue should be empty — Lambda consumed the message
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1)
    assert not msgs.get("Messages"), "Message should have been consumed by Lambda via ESM"

    # Cleanup
    lam.delete_event_source_mapping(UUID=esm_uuid)


def test_iam_role_user(iam):
    iam.create_role(
        RoleName="test-role",
        AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": []}),
    )
    roles = iam.list_roles()
    assert any(r["RoleName"] == "test-role" for r in roles.get("Roles", []))
    iam.create_user(UserName="test-user")
    users = iam.list_users()
    assert any(u["UserName"] == "test-user" for u in users.get("Users", []))


# ========== SSM ==========

def test_ssm_put_get(ssm):
    ssm.put_parameter(Name="/app/db/host", Value="localhost", Type="String")
    resp = ssm.get_parameter(Name="/app/db/host")
    assert resp["Parameter"]["Value"] == "localhost"


def test_ssm_get_by_path(ssm):
    ssm.put_parameter(Name="/app/config/key1", Value="val1", Type="String")
    ssm.put_parameter(Name="/app/config/key2", Value="val2", Type="String")
    resp = ssm.get_parameters_by_path(Path="/app/config", Recursive=True)
    assert len(resp["Parameters"]) >= 2


def test_ssm_overwrite(ssm):
    ssm.put_parameter(Name="/app/overwrite", Value="v1", Type="String")
    ssm.put_parameter(Name="/app/overwrite", Value="v2", Type="String", Overwrite=True)
    resp = ssm.get_parameter(Name="/app/overwrite")
    assert resp["Parameter"]["Value"] == "v2"


# ========== EventBridge ==========

def test_eventbridge_bus_rule(eb):
    eb.create_event_bus(Name="test-bus")
    eb.put_rule(Name="test-rule", EventBusName="test-bus", ScheduleExpression="rate(5 minutes)", State="ENABLED")
    rules = eb.list_rules(EventBusName="test-bus")
    assert any(r["Name"] == "test-rule" for r in rules["Rules"])


def test_eventbridge_put_events(eb):
    resp = eb.put_events(Entries=[
        {"Source": "myapp", "DetailType": "UserSignup", "Detail": json.dumps({"userId": "123"}), "EventBusName": "default"},
        {"Source": "myapp", "DetailType": "OrderPlaced", "Detail": json.dumps({"orderId": "456"}), "EventBusName": "default"},
    ])
    assert resp["FailedEntryCount"] == 0
    assert len(resp["Entries"]) == 2


def test_eventbridge_targets(eb):
    eb.put_rule(Name="target-rule", ScheduleExpression="rate(1 minute)", State="ENABLED")
    eb.put_targets(Rule="target-rule", Targets=[
        {"Id": "1", "Arn": "arn:aws:lambda:us-east-1:000000000000:function:my-func"},
    ])
    resp = eb.list_targets_by_rule(Rule="target-rule")
    assert len(resp["Targets"]) == 1


# ========== Kinesis ==========

def test_kinesis_put_get(kin):
    kin.create_stream(StreamName="test-stream", ShardCount=1)
    kin.put_record(StreamName="test-stream", Data=b"hello kinesis", PartitionKey="pk1")
    kin.put_record(StreamName="test-stream", Data=b"second record", PartitionKey="pk2")
    desc = kin.describe_stream(StreamName="test-stream")
    shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
    it = kin.get_shard_iterator(StreamName="test-stream", ShardId=shard_id, ShardIteratorType="TRIM_HORIZON")
    records = kin.get_records(ShardIterator=it["ShardIterator"])
    assert len(records["Records"]) == 2


def test_kinesis_batch(kin):
    kin.create_stream(StreamName="test-stream-batch", ShardCount=1)
    resp = kin.put_records(
        StreamName="test-stream-batch",
        Records=[{"Data": f"record-{i}".encode(), "PartitionKey": f"pk{i}"} for i in range(5)],
    )
    assert resp["FailedRecordCount"] == 0
    assert len(resp["Records"]) == 5


def test_kinesis_list(kin):
    resp = kin.list_streams()
    assert "test-stream" in resp["StreamNames"]


# ========== CloudWatch Metrics ==========

def test_cloudwatch_metrics(cw):
    cw.put_metric_data(
        Namespace="MyApp",
        MetricData=[
            {"MetricName": "RequestCount", "Value": 42.0, "Unit": "Count"},
            {"MetricName": "Latency", "Value": 123.5, "Unit": "Milliseconds"},
        ],
    )
    resp = cw.list_metrics(Namespace="MyApp")
    names = [m["MetricName"] for m in resp["Metrics"]]
    assert "RequestCount" in names
    assert "Latency" in names


def test_cloudwatch_alarm(cw):
    cw.put_metric_alarm(
        AlarmName="high-latency",
        MetricName="Latency",
        Namespace="MyApp",
        Statistic="Average",
        Period=60,
        EvaluationPeriods=1,
        Threshold=500.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    resp = cw.describe_alarms(AlarmNames=["high-latency"])
    assert len(resp["MetricAlarms"]) == 1


# ========== SES ==========

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


# ========== Step Functions ==========

def test_sfn_create_execute(sfn):
    definition = json.dumps({
        "Comment": "Simple state machine",
        "StartAt": "HelloWorld",
        "States": {"HelloWorld": {"Type": "Pass", "End": True}},
    })
    resp = sfn.create_state_machine(
        name="test-machine",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/StepFunctionsRole",
    )
    sm_arn = resp["stateMachineArn"]
    exec_resp = sfn.start_execution(stateMachineArn=sm_arn, input=json.dumps({"key": "value"}))
    exec_arn = exec_resp["executionArn"]
    desc = sfn.describe_execution(executionArn=exec_arn)
    assert desc["status"] == "RUNNING"


def test_sfn_list(sfn):
    machines = sfn.list_state_machines()
    assert any(m["name"] == "test-machine" for m in machines["stateMachines"])
    sm_arn = next(m["stateMachineArn"] for m in machines["stateMachines"] if m["name"] == "test-machine")
    execs = sfn.list_executions(stateMachineArn=sm_arn)
    assert len(execs["executions"]) >= 1


# ========== ECS ==========

def test_ecs_cluster(ecs):
    ecs.create_cluster(clusterName="test-cluster")
    clusters = ecs.list_clusters()
    assert any("test-cluster" in arn for arn in clusters["clusterArns"])


def test_ecs_task_def(ecs):
    resp = ecs.register_task_definition(
        family="test-task",
        containerDefinitions=[{
            "name": "web",
            "image": "nginx:alpine",
            "cpu": 128,
            "memory": 256,
            "portMappings": [{"containerPort": 80, "hostPort": 8080}],
        }],
        requiresCompatibilities=["EC2"],
        cpu="256",
        memory="512",
    )
    assert resp["taskDefinition"]["family"] == "test-task"
    assert resp["taskDefinition"]["revision"] == 1


def test_ecs_list_task_defs(ecs):
    resp = ecs.list_task_definitions(familyPrefix="test-task")
    assert len(resp["taskDefinitionArns"]) >= 1


def test_ecs_run_task_stops_after_exit(ecs):
    """DescribeTasks transitions to STOPPED after Docker container exits."""
    ecs.create_cluster(clusterName="task-lifecycle")
    ecs.register_task_definition(
        family="short-lived",
        containerDefinitions=[{
            "name": "worker",
            "image": "alpine:latest",
            "command": ["sh", "-c", "echo done"],
            "essential": True,
        }],
    )
    resp = ecs.run_task(cluster="task-lifecycle", taskDefinition="short-lived")
    task_arn = resp["tasks"][0]["taskArn"]
    assert resp["tasks"][0]["lastStatus"] == "RUNNING"

    # Poll until STOPPED (container exits almost immediately)
    stopped = False
    for _ in range(10):
        time.sleep(1)
        desc = ecs.describe_tasks(cluster="task-lifecycle", tasks=[task_arn])
        task = desc["tasks"][0]
        if task["lastStatus"] == "STOPPED":
            stopped = True
            assert task["desiredStatus"] == "STOPPED"
            assert task["stopCode"] == "EssentialContainerExited"
            assert task["containers"][0]["lastStatus"] == "STOPPED"
            assert task["containers"][0]["exitCode"] == 0
            break
    assert stopped, "Task should transition to STOPPED after container exits"


def test_ecs_service(ecs):
    ecs.create_service(
        cluster="test-cluster",
        serviceName="test-service",
        taskDefinition="test-task",
        desiredCount=1,
    )
    resp = ecs.describe_services(cluster="test-cluster", services=["test-service"])
    assert len(resp["services"]) == 1
    assert resp["services"][0]["serviceName"] == "test-service"


# ========== RDS ==========

def test_rds_create(rds):
    rds.create_db_instance(
        DBInstanceIdentifier="test-db",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="password123",
        DBName="testdb",
        AllocatedStorage=20,
    )
    resp = rds.describe_db_instances(DBInstanceIdentifier="test-db")
    instances = resp["DBInstances"]
    assert len(instances) == 1
    assert instances[0]["DBInstanceIdentifier"] == "test-db"
    assert instances[0]["Engine"] == "postgres"
    assert "Address" in instances[0]["Endpoint"]


def test_rds_engines(rds):
    resp = rds.describe_db_engine_versions(Engine="postgres")
    assert len(resp["DBEngineVersions"]) > 0


def test_rds_cluster(rds):
    rds.create_db_cluster(
        DBClusterIdentifier="test-cluster",
        Engine="aurora-postgresql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    resp = rds.describe_db_clusters(DBClusterIdentifier="test-cluster")
    assert resp["DBClusters"][0]["DBClusterIdentifier"] == "test-cluster"


# ========== ElastiCache ==========

def test_elasticache_create(ec):
    ec.create_cache_cluster(
        CacheClusterId="test-redis",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    resp = ec.describe_cache_clusters(CacheClusterId="test-redis")
    clusters = resp["CacheClusters"]
    assert len(clusters) == 1
    assert clusters[0]["CacheClusterId"] == "test-redis"
    assert clusters[0]["Engine"] == "redis"


def test_elasticache_replication_group(ec):
    ec.create_replication_group(
        ReplicationGroupId="test-rg",
        ReplicationGroupDescription="Test replication group",
        CacheNodeType="cache.t3.micro",
    )
    resp = ec.describe_replication_groups(ReplicationGroupId="test-rg")
    assert resp["ReplicationGroups"][0]["ReplicationGroupId"] == "test-rg"


def test_elasticache_engines(ec):
    resp = ec.describe_cache_engine_versions(Engine="redis")
    assert len(resp["CacheEngineVersions"]) > 0


# ========== Glue ==========

def test_glue_catalog(glue):
    glue.create_database(DatabaseInput={"Name": "test_db", "Description": "Test database"})
    glue.create_table(
        DatabaseName="test_db",
        TableInput={
            "Name": "test_table",
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "id", "Type": "int"},
                    {"Name": "name", "Type": "string"},
                ],
                "Location": "s3://my-bucket/data/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
            },
            "TableType": "EXTERNAL_TABLE",
        },
    )
    resp = glue.get_table(DatabaseName="test_db", Name="test_table")
    assert resp["Table"]["Name"] == "test_table"


def test_glue_list(glue):
    dbs = glue.get_databases()
    assert any(d["Name"] == "test_db" for d in dbs["DatabaseList"])
    tables = glue.get_tables(DatabaseName="test_db")
    assert any(t["Name"] == "test_table" for t in tables["TableList"])


def test_glue_job(glue):
    glue.create_job(
        Name="test-job",
        Role="arn:aws:iam::000000000000:role/GlueRole",
        Command={"Name": "glueetl", "ScriptLocation": "s3://my-bucket/scripts/etl.py"},
        GlueVersion="3.0",
    )
    resp = glue.start_job_run(JobName="test-job")
    assert "JobRunId" in resp
    runs = glue.get_job_runs(JobName="test-job")
    assert len(runs["JobRuns"]) == 1


def test_glue_crawler(glue):
    glue.create_crawler(
        Name="test-crawler",
        Role="arn:aws:iam::000000000000:role/GlueRole",
        DatabaseName="test_db",
        Targets={"S3Targets": [{"Path": "s3://my-bucket/data/"}]},
    )
    resp = glue.get_crawler(Name="test-crawler")
    assert resp["Crawler"]["Name"] == "test-crawler"
    glue.start_crawler(Name="test-crawler")


# ========== Athena ==========

def test_athena_query(athena):
    resp = athena.start_query_execution(
        QueryString="SELECT 1 AS num, 'hello' AS greeting",
        QueryExecutionContext={"Database": "default"},
        ResultConfiguration={"OutputLocation": "s3://athena-results/"},
    )
    query_id = resp["QueryExecutionId"]
    state = None
    for _ in range(10):
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.2)
    assert state == "SUCCEEDED", f"Query ended in state: {state}"
    results = athena.get_query_results(QueryExecutionId=query_id)
    assert len(results["ResultSet"]["Rows"]) >= 1


def test_athena_workgroup(athena):
    athena.create_work_group(
        Name="test-wg",
        Description="Test workgroup",
        Configuration={"ResultConfiguration": {"OutputLocation": "s3://athena-results/test/"}},
    )
    wgs = athena.list_work_groups()
    assert any(wg["Name"] == "test-wg" for wg in wgs["WorkGroups"])
    resp = athena.create_named_query(
        Name="my-query",
        Database="default",
        QueryString="SELECT * FROM my_table LIMIT 10",
        WorkGroup="test-wg",
    )
    assert "NamedQueryId" in resp
