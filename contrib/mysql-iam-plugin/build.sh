#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
output_dir="${1:-${repo_root}/build/mysql-plugins}"
image_tag="ministack-mysql-iam-plugin-build:local"

docker build \
  --file "${repo_root}/Dockerfile.full" \
  --target plugin-build \
  --tag "${image_tag}" \
  "${repo_root}"

container_id="$(docker create "${image_tag}")"
trap 'docker rm -f "${container_id}" >/dev/null 2>&1 || true' EXIT
mkdir -p "${output_dir}"
docker cp "${container_id}:/opt/ministack/mysql-plugins/." "${output_dir}/"

printf 'MySQL IAM plugin artifacts copied to %s\n' "${output_dir}"
