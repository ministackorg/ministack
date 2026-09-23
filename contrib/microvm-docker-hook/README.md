# Docker-backed MicroVM example

Build a ZIP containing this directory and upload it to MiniStack S3. The ZIP
must contain `Dockerfile` at its root.

The Docker-backed draft requires this image configuration:

```json
{
  "port": 9000,
  "microvmImageHooks": {
    "ready": "ENABLED",
    "validate": "ENABLED"
  },
  "microvmHooks": {
    "run": "ENABLED",
    "suspend": "ENABLED",
    "resume": "ENABLED",
    "terminate": "ENABLED"
  }
}
```

Start MiniStack with Docker access and opt in:

```bash
MINISTACK_MICROVM_BACKEND=docker \
  docker run -p 4566:4566 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  ministackorg/ministack
```

`CreateMicrovmImage` reads `codeArtifact.uri`, extracts the ZIP safely,
builds the Dockerfile, starts a temporary validation container, and calls
`/ready` and `/validate`. `RunMicrovm` then starts a new container from the
image tag produced by that build and calls the runtime `/run` hook.

Create the image using the required AWS CLI parameters:

```bash
aws --endpoint-url=http://localhost:4566 lambda-microvms create-microvm-image \
  --base-image-arn arn:aws:lambda:us-east-1:aws:microvm-image:base \
  --build-role-arn arn:aws:iam::000000000000:role/build \
  --code-artifact uri=s3://microvm-artifacts/microvm-hook.zip \
  --name docker-hook-smoke \
  --hooks file:///tmp/microvm-hooks.json
```

The `--code-artifact` value uses the AWS CLI tagged-union shorthand. The
`--hooks` file should contain the hook configuration above.
