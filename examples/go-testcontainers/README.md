# MiniStack — Go Testcontainers Example

Integration tests for S3, SQS, and DynamoDB using [testcontainers-go](https://golang.testcontainers.org/) and the AWS SDK v2.

## Prerequisites

- Go 1.21+
- Docker (running)

## Run

```bash
go mod tidy
go test ./... -v
```

Testcontainers will pull `nahuelnucera/ministack:latest`, start it, run the tests, and tear it down automatically.

## What's tested

| Service    | Operations |
|------------|------------|
| S3         | CreateBucket, PutObject, GetObject, ListBuckets |
| SQS        | CreateQueue, SendMessage, ReceiveMessage |
| DynamoDB   | CreateTable, PutItem, GetItem, DeleteItem |
