# MiniStack — .NET Testcontainers Example

Integration tests for S3, SQS, and DynamoDB using [Testcontainers for .NET](https://dotnet.testcontainers.org/), xUnit and the AWS SDK for .NET v4.

## Prerequisites

- .NET 10 SDK
- Docker (running)

## Run

```bash
dotnet test
```

Testcontainers will pull `ministackorg/ministack:latest`, start it, run the tests, and tear it down automatically.

## What's tested

| Service    | Operations |
|------------|------------|
| S3         | CreateBucket, PutObject, GetObject, ListBuckets |
| SQS        | CreateQueue, SendMessage, ReceiveMessage |
| DynamoDB   | CreateTable, PutItem, GetItem, DeleteItem |
