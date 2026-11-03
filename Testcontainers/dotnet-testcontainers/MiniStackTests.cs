using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Xunit;

namespace MiniStack.Tests;

public sealed class MiniStackFixture : IAsyncLifetime
{
    private readonly IContainer _container = new ContainerBuilder("ministackorg/ministack:latest")
        .WithPortBinding(4566, true)
        .WithEnvironment("GATEWAY_PORT", "4566")
        .WithEnvironment("LOG_LEVEL", "INFO")
        .WithWaitStrategy(Wait.ForUnixContainer()
            .UntilHttpRequestIsSucceeded(r => r.ForPort(4566).ForPath("/_ministack/health")))
        .Build();

    public string Endpoint => $"http://{_container.Hostname}:{_container.GetMappedPublicPort(4566)}";

    public AWSCredentials Credentials { get; } = new BasicAWSCredentials("test", "test");

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

public sealed class MiniStackTests : IClassFixture<MiniStackFixture>
{
    private readonly MiniStackFixture _ministack;

    public MiniStackTests(MiniStackFixture ministack) => _ministack = ministack;

    private AmazonS3Client S3() => new(_ministack.Credentials, new AmazonS3Config
    {
        ServiceURL = _ministack.Endpoint,
        AuthenticationRegion = "us-east-1",
        ForcePathStyle = true,
    });

    private AmazonSQSClient Sqs() => new(_ministack.Credentials, new AmazonSQSConfig
    {
        ServiceURL = _ministack.Endpoint,
        AuthenticationRegion = "us-east-1",
    });

    private AmazonDynamoDBClient DynamoDb() => new(_ministack.Credentials, new AmazonDynamoDBConfig
    {
        ServiceURL = _ministack.Endpoint,
        AuthenticationRegion = "us-east-1",
    });

    // ── S3 ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task S3_PutAndGetObject()
    {
        using var s3 = S3();
        await s3.PutBucketAsync("dotnet-test-bucket");
        await s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "dotnet-test-bucket",
            Key = "hello.txt",
            ContentBody = "Hello from .NET!",
        });

        using var response = await s3.GetObjectAsync("dotnet-test-bucket", "hello.txt");
        using var reader = new StreamReader(response.ResponseStream);
        Assert.Equal("Hello from .NET!", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task S3_ListBuckets()
    {
        using var s3 = S3();
        await s3.PutBucketAsync("dotnet-list-bucket");

        var response = await s3.ListBucketsAsync();
        Assert.Contains(response.Buckets, b => b.BucketName == "dotnet-list-bucket");
    }

    // ── SQS ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Sqs_SendAndReceive()
    {
        using var sqs = Sqs();
        var queueUrl = (await sqs.CreateQueueAsync("dotnet-test-queue")).QueueUrl;
        await sqs.SendMessageAsync(queueUrl, "hello from dotnet");

        var response = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = queueUrl,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = 2,
        });
        var message = Assert.Single(response.Messages);
        Assert.Equal("hello from dotnet", message.Body);
    }

    // ── DynamoDB ────────────────────────────────────────────────────────────

    [Fact]
    public async Task DynamoDb_PutGetAndDelete()
    {
        using var ddb = DynamoDb();
        await ddb.CreateTableAsync(new CreateTableRequest
        {
            TableName = "dotnet-test-table",
            KeySchema = [new KeySchemaElement("pk", KeyType.HASH)],
            AttributeDefinitions = [new AttributeDefinition("pk", ScalarAttributeType.S)],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        });

        var key = new Dictionary<string, AttributeValue> { ["pk"] = new AttributeValue("key1") };
        await ddb.PutItemAsync("dotnet-test-table", new Dictionary<string, AttributeValue>
        {
            ["pk"] = new AttributeValue("key1"),
            ["value"] = new AttributeValue("hello dynamodb from dotnet"),
        });

        var item = (await ddb.GetItemAsync("dotnet-test-table", key)).Item;
        Assert.Equal("hello dynamodb from dotnet", item["value"].S);

        await ddb.DeleteItemAsync("dotnet-test-table", key);
        var deleted = (await ddb.GetItemAsync("dotnet-test-table", key)).Item;
        Assert.True(deleted is null || deleted.Count == 0);
    }
}
