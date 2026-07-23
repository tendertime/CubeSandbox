#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Tencent. All rights reserved.

set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mock_host="${MOCK_AUTH_HOST:-127.0.0.1}"
mock_port="${MOCK_AUTH_PORT:-8081}"
callback_url="http://${mock_host}:${mock_port}/verify"
health_url="http://${mock_host}:${mock_port}/health"

mock_pid=""

cleanup() {
  if [[ -n "${mock_pid}" ]]; then
    kill "${mock_pid}" 2>/dev/null || true
    wait "${mock_pid}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

if curl --fail --silent "${health_url}" >/dev/null 2>&1; then
  echo "reusing mock auth callback at ${callback_url}"
else
  python3 -u "${repo_dir}/scripts/mock-auth-service.py" \
    --host "${mock_host}" \
    --port "${mock_port}" &
  mock_pid=$!

  for ((attempt = 0; attempt < 50; attempt++)); do
    if curl --fail --silent "${health_url}" >/dev/null 2>&1; then
      break
    fi
    if ! kill -0 "${mock_pid}" 2>/dev/null; then
      echo "mock auth service exited before becoming ready" >&2
      exit 1
    fi
    sleep 0.1
  done
fi

if ! curl --fail --silent --show-error "${health_url}" >/dev/null; then
  echo "mock auth service did not become ready at ${health_url}" >&2
  exit 1
fi

echo "mock auth callback ready at ${callback_url}"
if [[ -z "${MOCK_AUTH_KEYS:-}" && -z "${MOCK_READONLY_KEYS:-}" ]]; then
  echo "MOCK_AUTH_KEYS is empty: any non-empty WebUI login token or API key will be accepted"
else
  echo "mock auth callback is using the configured key allowlists"
fi

AUTH_CALLBACK_URL="${callback_url}" \
  cargo run --manifest-path "${repo_dir}/CubeAPI/Cargo.toml"
