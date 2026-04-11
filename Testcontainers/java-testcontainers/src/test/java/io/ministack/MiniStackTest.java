package io.ministack;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MiniStackTest {

    @Container
    static final GenericContainer<?> ministack = new GenericContainer<>(
            DockerImageName.parse("ministackorg/ministack:latest"))
            .withExposedPorts(4566)
            .withEnv("GATEWAY_PORT", "4566")
            .withEnv("LOG_LEVEL", "INFO")
            .waitingFor(new HttpWaitStrategy()
                    .forPath("/_ministack/health")
                    .forPort(4566)
                    .withStartupTimeout(Duration.ofSeconds(60)));

    private static URI endpoint;
    private static StaticCredentialsProvider credentials;

    @BeforeAll
    static void setup() {
        endpoint = URI.create("http://" + ministack.getHost() + ":" + ministack.getMappedPort(4566));
        credentials = StaticCredentialsProvider.create(
                AwsBasicCredentials.create("test", "test"));
    }

    private S3Client s3() {
        return S3Client.builder()
                .endpointOverride(endpoint)
                .credentialsProvider(credentials)
                .region(Region.US_EAST_1)
                .httpClient(UrlConnectionHttpClient.create())
                .forcePathStyle(true)
                .build();
    }

    private SqsClient sqs() {
        return SqsClient.builder()
                .endpointOverride(endpoint)
                .credentialsProvider(credentials)
                .region(Region.US_EAST_1)
                .httpClient(UrlConnectionHttpClient.create())
                .build();
    }

    private DynamoDbClient ddb() {
        return DynamoDbClient.builder()
                .endpointOverride(endpoint)
                .credentialsProvider(credentials)
                .region(Region.US_EAST_1)
                .httpClient(UrlConnectionHttpClient.create())
                .build();
    }

    // ── S3 ──────────────────────────────────────────────────────────────────

    @Test
    @Order(1)
    void s3_createBucketPutAndGetObject() {
        try (S3Client client = s3()) {
            client.createBucket(b -> b.bucket("test-bucket"));

            client.putObject(
                    PutObjectRequest.builder().bucket("test-bucket").key("hello.txt").build(),
                    RequestBody.fromString("Hello MiniStack!"));

            String body = client.getObjectAsBytes(
                    GetObjectRequest.builder().bucket("test-bucket").key("hello.txt").build()
            ).asUtf8String();

            assertEquals("Hello MiniStack!", body);
        }
    }

    @Test
    @Order(2)
    void s3_listBuckets() {
        try (S3Client client = s3()) {
            List<Bucket> buckets = client.listBuckets().buckets();
            assertTrue(buckets.stream().anyMatch(b -> b.name().equals("test-bucket")));
        }
    }

    @Test
    @Order(3)
    void s3_deleteObject() {
        try (S3Client client = s3()) {
            client.deleteObject(b -> b.bucket("test-bucket").key("hello.txt"));
            assertThrows(NoSuchKeyException.class, () ->
                    client.getObjectAsBytes(b -> b.bucket("test-bucket").key("hello.txt")));
        }
    }

    // ── SQS ─────────────────────────────────────────────────────────────────

    @Test
    @Order(10)
    void sqs_sendAndReceiveMessage() {
        try (SqsClient client = sqs()) {
            String queueUrl = client.createQueue(b -> b.queueName("test-queue")).queueUrl();

            client.sendMessage(b -> b.queueUrl(queueUrl).messageBody("hello from java"));

            ReceiveMessageResponse resp = client.receiveMessage(b -> b
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(2));

            assertEquals(1, resp.messages().size());
            assertEquals("hello from java", resp.messages().get(0).body());

            client.deleteMessage(b -> b
                    .queueUrl(queueUrl)
                    .receiptHandle(resp.messages().get(0).receiptHandle()));
        }
    }

    @Test
    @Order(11)
    void sqs_queueAttributes() {
        try (SqsClient client = sqs()) {
            String queueUrl = client.createQueue(b -> b.queueName("test-queue")).queueUrl();
            GetQueueAttributesResponse attrs = client.getQueueAttributes(b -> b
                    .queueUrl(queueUrl)
                    .attributeNames(QueueAttributeName.ALL));
            assertNotNull(attrs.attributes().get(QueueAttributeName.QUEUE_ARN));
        }
    }

    // ── DynamoDB ─────────────────────────────────────────────────────────────

    @Test
    @Order(20)
    void dynamodb_createTablePutAndGetItem() {
        try (DynamoDbClient client = ddb()) {
            client.createTable(b -> b
                    .tableName("test-table")
                    .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName("pk").attributeType(ScalarAttributeType.S).build())
                    .billingMode(BillingMode.PAY_PER_REQUEST));

            client.putItem(b -> b
                    .tableName("test-table")
                    .item(Map.of(
                            "pk", AttributeValue.fromS("key1"),
                            "value", AttributeValue.fromS("hello dynamodb"))));

            GetItemResponse resp = client.getItem(b -> b
                    .tableName("test-table")
                    .key(Map.of("pk", AttributeValue.fromS("key1"))));

            assertTrue(resp.hasItem());
            assertEquals("hello dynamodb", resp.item().get("value").s());
        }
    }

    @Test
    @Order(21)
    void dynamodb_updateAndDeleteItem() {
        try (DynamoDbClient client = ddb()) {
            client.updateItem(b -> b
                    .tableName("test-table")
                    .key(Map.of("pk", AttributeValue.fromS("key1")))
                    .updateExpression("SET #v = :v")
                    .expressionAttributeNames(Map.of("#v", "value"))
                    .expressionAttributeValues(Map.of(":v", AttributeValue.fromS("updated"))));

            GetItemResponse resp = client.getItem(b -> b
                    .tableName("test-table")
                    .key(Map.of("pk", AttributeValue.fromS("key1"))));
            assertEquals("updated", resp.item().get("value").s());

            client.deleteItem(b -> b
                    .tableName("test-table")
                    .key(Map.of("pk", AttributeValue.fromS("key1"))));

            GetItemResponse after = client.getItem(b -> b
                    .tableName("test-table")
                    .key(Map.of("pk", AttributeValue.fromS("key1"))));
            assertFalse(after.hasItem());
        }
    }
}
