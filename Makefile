.PHONY: build run stop test logs health clean

IMAGE_NAME := ministack
CONTAINER_NAME := ministack
PORT := 4566

# Override any shell AWS credentials so make test is self-contained
export AWS_ACCESS_KEY_ID := test
export AWS_SECRET_ACCESS_KEY := test
export AWS_DEFAULT_REGION := us-east-1
unexport AWS_PROFILE

build:
	docker build -t $(IMAGE_NAME) .

run: build
	docker run -d --name $(CONTAINER_NAME) -p $(PORT):4566 \
		-e LOG_LEVEL=INFO \
		-v /var/run/docker.sock:/var/run/docker.sock \
		$(IMAGE_NAME)
	@echo "MiniStack running on http://localhost:$(PORT)"
	@echo "Health: http://localhost:$(PORT)/_ministack/health"

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
	@curl -s http://localhost:$(PORT)/_ministack/health | python3 -m json.tool

test: stop run
	@echo "Waiting for ministack to be ready..."
	@READY=0; \
	for i in $$(seq 1 30); do \
		if curl -sf http://localhost:$(PORT)/_ministack/health > /dev/null 2>&1; then \
			echo "Ready after $$i second(s)."; READY=1; break; \
		fi; \
		sleep 1; \
	done; \
	if [ "$$READY" = "0" ]; then echo "ERROR: ministack did not start within 30s" >&2; exit 1; fi
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
	@echo "=== ALB/ELBv2 ==="
	@python3 -c "\
import zipfile; \
z = zipfile.ZipFile('/tmp/ms-alb-test.zip', 'w'); \
z.writestr('index.py', 'import json\ndef handler(event, context):\n    return {\"statusCode\": 200, \"headers\": {\"Content-Type\": \"application/json\"}, \"body\": json.dumps({\"ok\": True, \"path\": event[\"path\"]})}\n'); \
z.close(); \
print('Lambda zip created')"
	aws --endpoint-url=http://localhost:$(PORT) lambda create-function \
		--function-name alb-test-fn --runtime python3.9 \
		--handler index.handler \
		--role arn:aws:iam::000000000000:role/role \
		--zip-file fileb:///tmp/ms-alb-test.zip
	@LB_ARN=$$(aws --endpoint-url=http://localhost:$(PORT) elbv2 create-load-balancer \
		--name test-alb --query 'LoadBalancers[0].LoadBalancerArn' --output text) && \
	TG_ARN=$$(aws --endpoint-url=http://localhost:$(PORT) elbv2 create-target-group \
		--name test-tg --target-type lambda --protocol HTTP --port 80 \
		--vpc-id vpc-00000001 --query 'TargetGroups[0].TargetGroupArn' --output text) && \
	FN_ARN=$$(aws --endpoint-url=http://localhost:$(PORT) lambda get-function \
		--function-name alb-test-fn --query 'Configuration.FunctionArn' --output text) && \
	aws --endpoint-url=http://localhost:$(PORT) elbv2 register-targets \
		--target-group-arn $$TG_ARN --targets Id=$$FN_ARN && \
	aws --endpoint-url=http://localhost:$(PORT) elbv2 create-listener \
		--load-balancer-arn $$LB_ARN --protocol HTTP --port 80 \
		--default-actions Type=forward,TargetGroupArn=$$TG_ARN && \
	RESULT=$$(curl -sf http://localhost:$(PORT)/_alb/test-alb/ping) && \
	echo "ALB response: $$RESULT" && \
	echo "$$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['ok'] and d['path']=='/ping', f'Unexpected response: {d}'" && \
	echo "ALB -> Lambda routing: OK"
	@echo ""
	@echo "=== All tests passed ==="

clean: stop
	docker rmi $(IMAGE_NAME) 2>/dev/null || true

purge: stop-compose
	docker rm -f $$(docker ps -aq --filter "label=ministack") 2>/dev/null || true
	docker volume prune -f
	rm -rf ./data/s3/*
	@echo "Orphaned ministack containers, dangling volumes, and S3 data cleared"
