package ministacktest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// newMiniStackContainer starts a MiniStack container and returns the endpoint URL.
func newMiniStackContainer(ctx context.Context, t *testing.T) (string, func()) {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        "ministackorg/ministack:latest",
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"GATEWAY_PORT": "4566",
			"LOG_LEVEL":    "INFO",
		},
		WaitingFor: wait.ForHTTP("/_ministack/health").
			WithPort("4566/tcp").
			WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start MiniStack container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "4566")
	if err != nil {
		t.Fatalf("failed to get mapped port: %v", err)
	}

	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	cleanup := func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}

	return endpoint, cleanup
}

func awsCfg(endpoint string) aws.Config {
	return aws.Config{
		Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
		EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint}, nil
			},
		),
	}
}

// ── S3 ──────────────────────────────────────────────────────────────────────

func TestS3_PutAndGetObject(t *testing.T) {
	ctx := context.Background()
	endpoint, cleanup := newMiniStackContainer(ctx, t)
	defer cleanup()

	cfg := awsCfg(endpoint)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	bucket := "go-test-bucket"
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("hello.txt"),
		Body:   strings.NewReader("Hello from Go!"),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("hello.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer out.Body.Close()

	buf := new(strings.Builder)
	if _, err := fmt.Fscan(out.Body, buf); err != nil && buf.Len() == 0 {
		t.Fatalf("read body: %v", err)
	}
	if !strings.Contains(buf.String(), "Hello") {
		t.Errorf("unexpected body: %q", buf.String())
	}
}

func TestS3_ListBuckets(t *testing.T) {
	ctx := context.Background()
	endpoint, cleanup := newMiniStackContainer(ctx, t)
	defer cleanup()

	cfg := awsCfg(endpoint)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("list-bucket")})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}

	found := false
	for _, b := range out.Buckets {
		if aws.ToString(b.Name) == "list-bucket" {
			found = true
		}
	}
	if !found {
		t.Error("expected list-bucket in ListBuckets response")
	}
}

// ── SQS ──────────────────────────────────────────────────────────────────────

func TestSQS_SendAndReceive(t *testing.T) {
	ctx := context.Background()
	endpoint, cleanup := newMiniStackContainer(ctx, t)
	defer cleanup()

	cfg := awsCfg(endpoint)
	client := sqs.NewFromConfig(cfg)

	q, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("go-test-queue")})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    q.QueueUrl,
		MessageBody: aws.String("hello from go"),
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	recv, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            q.QueueUrl,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     2,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	if len(recv.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(recv.Messages))
	}
	if aws.ToString(recv.Messages[0].Body) != "hello from go" {
		t.Errorf("unexpected body: %q", aws.ToString(recv.Messages[0].Body))
	}
}

// ── DynamoDB ──────────────────────────────────────────────────────────────────

func TestDynamoDB_PutAndGet(t *testing.T) {
	ctx := context.Background()
	endpoint, cleanup := newMiniStackContainer(ctx, t)
	defer cleanup()

	cfg := awsCfg(endpoint)
	client := dynamodb.NewFromConfig(cfg)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("go-test-table"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("go-test-table"),
		Item: map[string]types.AttributeValue{
			"pk":    &types.AttributeValueMemberS{Value: "key1"},
			"value": &types.AttributeValueMemberS{Value: "hello dynamodb from go"},
		},
	})
	if err != nil {
		t.Fatalf("PutItem: %v", err)
	}

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("go-test-table"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "key1"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}

	val, ok := out.Item["value"].(*types.AttributeValueMemberS)
	if !ok {
		t.Fatal("expected string attribute 'value'")
	}
	if val.Value != "hello dynamodb from go" {
		t.Errorf("unexpected value: %q", val.Value)
	}
}

func TestDynamoDB_DeleteItem(t *testing.T) {
	ctx := context.Background()
	endpoint, cleanup := newMiniStackContainer(ctx, t)
	defer cleanup()

	cfg := awsCfg(endpoint)
	client := dynamodb.NewFromConfig(cfg)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("go-delete-table"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("go-delete-table"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "del1"},
		},
	})
	if err != nil {
		t.Fatalf("PutItem: %v", err)
	}

	_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String("go-delete-table"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "del1"},
		},
	})
	if err != nil {
		t.Fatalf("DeleteItem: %v", err)
	}

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("go-delete-table"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "del1"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if len(out.Item) != 0 {
		t.Error("expected item to be deleted")
	}
}
