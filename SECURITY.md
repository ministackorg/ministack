# Security Policy

## ⚠️ Important: Local Development Only

MiniStack is designed **exclusively for local development and CI/CD testing**.

**Do not expose MiniStack to the internet or any untrusted network.**

- By default it has no authentication: any request is accepted and credentials (`aws_access_key_id`, `aws_secret_access_key`) are ignored. `AUTH=true` checks SigV4 signatures for IAM user and STS session keys and evaluates IAM policies
- All data is stored in-memory and is not encrypted
- The Docker socket mount (for ECS/RDS/ElastiCache) gives container-level access to your host

## Reporting a Vulnerability

If you find a security issue that could affect users running MiniStack in a way that exposes their host system or data, please open a GitHub issue tagged `security`.

Since this is a local dev tool with no auth by design, most "vulnerabilities" are intentional trade-offs for simplicity. But if you find something that could cause unintended host compromise (e.g. path traversal in S3 persistence, command injection in Lambda execution), please report it.

## Recommended Usage

```yaml
# docker-compose.yml — bind to localhost only, never 0.0.0.0 on a shared machine
ports:
  - "127.0.0.1:4566:4566"
```

The containers MiniStack starts through the Docker socket (RDS, ElastiCache, OpenSearch, MWAA, EKS, DocumentDB, ECS task ports) publish their ports the same way MiniStack's own port is published: on `127.0.0.1` only when MiniStack's `4566` is bound to loopback (as above, or `BIND_HOST=127.0.0.1` when MiniStack runs on the host), otherwise on every interface. Several of them accept connections without credentials by default (an ElastiCache cache without a user group or `AuthToken`, an OpenSearch domain), so keep MiniStack on loopback on any shared or untrusted network.

Never run with `S3_PERSIST=1` pointing to sensitive directories.
