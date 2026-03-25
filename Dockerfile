FROM python:3.12-alpine

LABEL maintainer="MiniStack" \
      description="Local AWS Service Emulator — drop-in LocalStack replacement"

# Upgrade base packages to pick up latest security patches.
RUN apk upgrade --no-cache && rm -f /usr/bin/wget /bin/wget

WORKDIR /opt/ministack

# Install all Python dependencies.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
        uvicorn==0.30.6 \
        "cbor2>=5.4.0" \
        "docker>=7.0.0"

COPY core/ core/
COPY services/ services/
COPY app.py .

RUN addgroup -S ministack && adduser -S ministack -G ministack
RUN mkdir -p /tmp/localstack-data/s3 && chown -R ministack:ministack /tmp/localstack-data
RUN mkdir -p /docker-entrypoint-initaws.d && chown ministack:ministack /docker-entrypoint-initaws.d
VOLUME /docker-entrypoint-initaws.d
USER ministack

ENV GATEWAY_PORT=4566 \
    LOG_LEVEL=INFO \
    S3_PERSIST=0 \
    S3_DATA_DIR=/tmp/localstack-data/s3 \
    REDIS_HOST=redis \
    REDIS_PORT=6379 \
    RDS_BASE_PORT=15432 \
    ELASTICACHE_BASE_PORT=16379 \
    LAMBDA_EXECUTOR=local \
    PYTHONUNBUFFERED=1

EXPOSE 4566

# Pure Python healthcheck — no curl dependency
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:4566/_localstack/health')" || exit 1

ENTRYPOINT ["python", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "4566"]
