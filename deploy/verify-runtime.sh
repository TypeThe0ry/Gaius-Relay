#!/usr/bin/env bash
set -euo pipefail

service_name=${GAIUS_VERIFY_SERVICE:-gaius-relaynode.service}
expected_port=${GAIUS_VERIFY_PORT:-8080}
expected_cwd=${GAIUS_VERIFY_WORKDIR:-/opt/gaius/apps/bridge}
runtime_mode=${GAIUS_VERIFY_RUNTIME:-auto}
base_url=${GAIUS_VERIFY_BASE_URL:-}
curl_timeout=${GAIUS_VERIFY_TIMEOUT_SECONDS:-10}

fail() {
    printf 'verify-runtime: %s\n' "$1" >&2
    exit 1
}

command -v ss >/dev/null 2>&1 || fail "ss is required"
command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v readlink >/dev/null 2>&1 || fail "readlink is required"

[[ "$expected_port" =~ ^[0-9]+$ ]] || fail "GAIUS_VERIFY_PORT must be numeric"
(( expected_port >= 1 && expected_port <= 65535 )) || fail "invalid port"
[[ "$curl_timeout" =~ ^[0-9]+$ ]] || fail "GAIUS_VERIFY_TIMEOUT_SECONDS must be numeric"
(( curl_timeout >= 1 )) || fail "timeout must be positive"
case "$runtime_mode" in
    auto|systemd|docker) ;;
    *) fail "GAIUS_VERIFY_RUNTIME must be auto, systemd, or docker" ;;
esac

listener_output=$(ss -H -ltnp "sport = :$expected_port" 2>/dev/null || true)
[[ -n "$listener_output" ]] || fail "no TCP listener found on port $expected_port"

listener_pids=$(printf '%s\n' "$listener_output" \
    | grep -oE 'pid=[0-9]+' \
    | cut -d= -f2 \
    | sort -u || true)
[[ -n "$listener_pids" ]] || fail "cannot identify the listener PID; run with permission to inspect ss"

pid_count=$(printf '%s\n' "$listener_pids" | sed '/^$/d' | wc -l | tr -d ' ')
[[ "$pid_count" == "1" ]] || fail "expected one listener process on port $expected_port, found $pid_count"
pid=$(printf '%s\n' "$listener_pids")
[[ "$pid" =~ ^[0-9]+$ ]] || fail "invalid listener PID"

actual_cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null || true)
listener_command=$(ps -p "$pid" -o comm= 2>/dev/null | tr -d '[:space:]' || true)
[[ -n "$base_url" ]] || base_url="http://127.0.0.1:$expected_port"

if [[ "$runtime_mode" == "auto" ]]; then
    case "$listener_command" in
        docker-proxy|docker-proxy*) runtime_mode=docker ;;
        *) runtime_mode=systemd ;;
    esac
fi

runtime_label=systemd
runtime_detail="pid=$pid cwd=$actual_cwd"
if [[ "$runtime_mode" == "docker" ]]; then
    command -v docker >/dev/null 2>&1 || fail "docker is required for Docker runtime verification"
    container_ids=$(docker ps --quiet --filter "publish=$expected_port" 2>/dev/null || true)
    container_count=$(printf '%s\n' "$container_ids" | sed '/^$/d' | wc -l | tr -d ' ')
    [[ "$container_count" == "1" ]] || fail "expected one running Docker container published on port $expected_port, found $container_count"
    container_id=$(printf '%s\n' "$container_ids")
    container_workdir=$(docker inspect --format '{{.Config.WorkingDir}}' "$container_id" 2>/dev/null || true)
    [[ "$container_workdir" == "/app" ]] || fail "Docker RelayNode working directory is '$container_workdir', expected '/app'"
    docker_running=$(docker inspect --format '{{.State.Running}}' "$container_id" 2>/dev/null || true)
    [[ "$docker_running" == "true" ]] || fail "Docker RelayNode container is not running"
    docker exec "$container_id" test -r /app/dist/main.js \
        || fail "Docker RelayNode entrypoint is not readable inside /app"
    docker exec "$container_id" grep -Fq 'target-attestation' /app/dist/main.js \
        || fail "running Docker RelayNode does not contain target-attestation"
    runtime_label=docker
    runtime_detail="container=$container_id host_listener=$pid host_cwd=$actual_cwd"
else
    [[ "$actual_cwd" == "$expected_cwd" ]] || fail "PID $pid cwd is '$actual_cwd', expected '$expected_cwd'"
    main_js="$actual_cwd/dist/main.js"
    [[ -r "$main_js" ]] || fail "RelayNode entrypoint is not readable: $main_js"
    grep -Fq 'target-attestation' "$main_js" \
        || fail "running RelayNode does not contain target-attestation"
fi

tmp_dir=$(mktemp -d)
cleanup() {
    rm -rf "$tmp_dir"
}
trap cleanup EXIT

curl --fail --silent --show-error --max-time "$curl_timeout" \
    --proto '=http,https' --output "$tmp_dir/health.json" \
    "$base_url/health" \
    || fail "health endpoint failed"
curl --fail --silent --show-error --max-time "$curl_timeout" \
    --proto '=http,https' --output "$tmp_dir/manifest.json" \
    "$base_url/relay-node/v1" \
    || fail "manifest endpoint failed"

grep -Fq '"kind":"gaius-relay-node"' "$tmp_dir/health.json" \
    || fail "health response is not a Gaius RelayNode response"
grep -Fq '"protocolVersion":1' "$tmp_dir/manifest.json" \
    || fail "manifest protocol version is not 1"
grep -Fq '"target-attestation"' "$tmp_dir/manifest.json" \
    || fail "manifest does not advertise target-attestation"

printf 'verify-runtime: RelayNode service=%s runtime=%s %s port=%s health=ok manifest=ok target-attestation=ok\n' \
    "$service_name" "$runtime_label" "$runtime_detail" "$expected_port"
