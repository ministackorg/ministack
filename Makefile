.PHONY: build run stop test logs health clean

IMAGE_NAME := ministack
CONTAINER_NAME := ministack
PORT := 4566

build:
	docker build -t $(IMAGE_NAME) .

run: build
	docker run -d --name $(CONTAINER_NAME) -p $(PORT):4566 \
		-e LOG_LEVEL=INFO \
		-v /var/run/docker.sock:/var/run/docker.sock \
		$(IMAGE_NAME)
	@echo "MiniStack running on http://localhost:$(PORT)"
	@echo "Health: http://localhost:$(PORT)/_localstack/health"

run-compose:
	docker compose up -d --build
	@echo "MiniStack running on http://localhost:$(PORT)"

stop:
	docker stop $(CONTAINER_NAME) 2>/dev/null || true
	docker rm $(CONTAINER_NAME) 2>/dev/null || true

stop-compose:
	docker compose down

logs:
	docker logs -f $(CONTAINER_NAME)

health:
	@curl -s http://localhost:$(PORT)/_localstack/health | python3 -m json.tool

test: run
	@sleep 2
	@echo "=== S3 ==="
	aws --endpoint-url=http://localhost:$(PORT) s3 mb s3://test-bucket
	echo "hello" | aws --endpoint-url=http://localhost:$(PORT) s3 cp - s3://test-bucket/hello.txt
	aws --endpoint-url=http://localhost:$(PORT) s3 ls s3://test-bucket
	aws --endpoint-url=http://localhost:$(PORT) s3 cp s3://test-bucket/hello.txt -
	@echo ""
	@echo "=== SQS ==="
	aws --endpoint-url=http://localhost:$(PORT) sqs create-queue --queue-name test-queue
	aws --endpoint-url=http://localhost:$(PORT) sqs send-message --queue-url http://localhost:$(PORT)/000000000000/test-queue --message-body "hello sqs"
	aws --endpoint-url=http://localhost:$(PORT) sqs receive-message --queue-url http://localhost:$(PORT)/000000000000/test-queue
	@echo ""
	@echo "=== DynamoDB ==="
	aws --endpoint-url=http://localhost:$(PORT) dynamodb create-table \
		--table-name TestTable \
		--attribute-definitions AttributeName=pk,AttributeType=S \
		--key-schema AttributeName=pk,KeyType=HASH \
		--billing-mode PAY_PER_REQUEST
	aws --endpoint-url=http://localhost:$(PORT) dynamodb put-item \
		--table-name TestTable \
		--item '{"pk":{"S":"key1"},"data":{"S":"value1"}}'
	aws --endpoint-url=http://localhost:$(PORT) dynamodb get-item \
		--table-name TestTable \
		--key '{"pk":{"S":"key1"}}'
	@echo ""
	@echo "=== SNS ==="
	aws --endpoint-url=http://localhost:$(PORT) sns create-topic --name test-topic
	aws --endpoint-url=http://localhost:$(PORT) sns list-topics
	@echo ""
	@echo "=== STS ==="
	aws --endpoint-url=http://localhost:$(PORT) sts get-caller-identity
	@echo ""
	@echo "=== SecretsManager ==="
	aws --endpoint-url=http://localhost:$(PORT) secretsmanager create-secret --name test-secret --secret-string '{"user":"admin","pass":"s3cr3t"}'
	aws --endpoint-url=http://localhost:$(PORT) secretsmanager get-secret-value --secret-id test-secret
	@echo ""
	@echo "=== Lambda ==="
	aws --endpoint-url=http://localhost:$(PORT) lambda list-functions
	@echo ""
	@echo "=== All tests passed ==="

clean: stop
	docker rmi $(IMAGE_NAME) 2>/dev/null || true
