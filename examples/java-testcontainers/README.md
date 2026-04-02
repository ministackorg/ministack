# MiniStack — Java Testcontainers Example

Integration tests for S3, SQS, and DynamoDB using [Testcontainers](https://testcontainers.com/) and the AWS SDK v2.

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker (running)

## Run

```bash
mvn test
```

Testcontainers will pull `nahuelnucera/ministack:latest`, start it, run the tests, and tear it down automatically.

## What's tested

| Service    | Operations |
|------------|------------|
| S3         | CreateBucket, PutObject, GetObject, ListBuckets, DeleteObject |
| SQS        | CreateQueue, SendMessage, ReceiveMessage, DeleteMessage, GetQueueAttributes |
| DynamoDB   | CreateTable, PutItem, GetItem, UpdateItem, DeleteItem |
