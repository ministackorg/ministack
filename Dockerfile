FROM python:3.12-alpine

LABEL maintainer="MiniStack" \
      description="Local AWS Service Emulator — drop-in LocalStack replacement"

# Upgrade base packages to pick up latest security patches.
RUN apk upgrade --no-cache && apk add --no-cache nodejs && rm -f /usr/bin/wget /bin/wget

WORKDIR /opt/ministack

# Install all Python dependencies.
# cryptography needs build deps on Alpine — install, build, then remove to keep image small.
RUN apk add --no-cache --virtual .build-deps gcc musl-dev libffi-dev openssl-dev && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
        uvicorn==0.30.6 \
        "cbor2>=5.4.0" \
        "docker>=7.0.0" \
        "pyyaml==6.0.3" \
        "aiohttp==3.13.5" \
        "asyncpg==0.31.0" \
        "cryptography>=41.0" && \
    apk del .build-deps

COPY ministack/ ministack/
COPY config/ config/

RUN addgroup -S ministack && adduser -S ministack -G ministack
RUN mkdir -p /tmp/ministack-data/s3 && chown -R ministack:ministack /tmp/ministack-data
RUN mkdir -p /docker-entrypoint-initaws.d && chown ministack:ministack /docker-entrypoint-initaws.d
VOLUME /docker-entrypoint-initaws.d

ENV GATEWAY_PORT=4566 \
    LOG_LEVEL=INFO \
    S3_PERSIST=0 \
    S3_DATA_DIR=/tmp/ministack-data/s3 \
    REDIS_HOST=redis \
    REDIS_PORT=6379 \
    RDS_BASE_PORT=15432 \
    ELASTICACHE_BASE_PORT=16379 \
    LAMBDA_EXECUTOR=local \
    PYTHONUNBUFFERED=1 \
    LITELLM_BASE_URL=http://litellm:4000 \
    PGVECTOR_HOST=pgvector \
    PGVECTOR_PORT=5432 \
    PGVECTOR_DB=bedrock_kb \
    PGVECTOR_USER=bedrock \
    PGVECTOR_PASSWORD=bedrock \
    BEDROCK_MODELS_CONFIG=config/bedrock_models.yaml

EXPOSE 4566

# Pure Python healthcheck — no curl dependency
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:4566/_ministack/health')" || exit 1

ENTRYPOINT ["python", "-m", "uvicorn", "ministack.app:app", "--host", "0.0.0.0", "--port", "4566"]
