#!/usr/bin/env bash
set -euo pipefail

container_name=${GAIUS_DOCKER_CONTAINER:-gaius-relaynode}
backup_name=${GAIUS_DOCKER_BACKUP_CONTAINER:-${container_name}-previous}
host_port=${GAIUS_DOCKER_HOST_PORT:-18080}
base_url=${GAIUS_DOCKER_VERIFY_BASE_URL:-http://127.0.0.1:${host_port}}
timeout=${GAIUS_DOCKER_VERIFY_TIMEOUT_SECONDS:-45}
lock_file=${GAIUS_DOCKER_LOCK_FILE:-/run/lock/${container_name}.deploy.lock}

fail() {
    printf 'docker-single-rollback: %s\n' "$1" >&2
    exit 1
}

command -v docker >/dev/null 2>&1 || fail "docker is required"
command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v flock >/dev/null 2>&1 || fail "flock is required"
command -v mktemp >/dev/null 2>&1 || fail "mktemp is required"
[[ "$host_port" =~ ^[0-9]+$ ]] || fail "GAIUS_DOCKER_HOST_PORT must be numeric"
[[ "$timeout" =~ ^[0-9]+$ ]] || fail "GAIUS_DOCKER_VERIFY_TIMEOUT_SECONDS must be numeric"
(( host_port >= 1 && host_port <= 65535 )) || fail "invalid host port"
(( timeout >= 1 )) || fail "verification timeout must be positive"

exec 9>"$lock_file" || fail "deployment lock could not be opened: $lock_file"
flock -n 9 || fail "another deploy or rollback is already running: $lock_file"

tmp_dir=$(mktemp -d) || fail "temporary verification directory could not be created"
cleanup() {
    rm -rf "$tmp_dir"
}
trap cleanup EXIT

docker container inspect "$backup_name" >/dev/null 2>&1 \
    || fail "rollback backup does not exist: $backup_name"

replaced_name="${container_name}-replaced-$(date +%s)"
had_active=0
restore_original_before_swap() {
    trap - INT TERM HUP
    if (( had_active == 1 )); then
        if docker container inspect "$replaced_name" >/dev/null 2>&1; then
            docker rename "$replaced_name" "$container_name" >/dev/null 2>&1 || return 1
        fi
        docker start "$container_name" >/dev/null 2>&1 || return 1
        [[ "$(docker inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null)" == true ]] \
            || return 1
    fi
    return 0
}
trap 'restore_original_before_swap || { printf "docker-single-rollback: signal recovery failed before swap\n" >&2; exit 131; }; exit 130' INT TERM HUP
if docker container inspect "$container_name" >/dev/null 2>&1; then
    had_active=1
    if ! docker stop "$container_name" >/dev/null; then
        docker start "$container_name" >/dev/null 2>&1 \
            || fail "active container stop failed and it could not be restarted"
        [[ "$(docker inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null)" == true ]] \
            || fail "active container stop failed and its running state is unknown"
        fail "active container could not be stopped; it was restarted"
    fi
    if ! docker rename "$container_name" "$replaced_name"; then
        docker start "$container_name" >/dev/null 2>&1 \
            || fail "active container rename failed and it could not be restarted"
        [[ "$(docker inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null)" == true ]] \
            || fail "active container rename failed and its running state is unknown"
        fail "active container could not be preserved before rollback; it was restarted"
    fi
fi

restore_replaced() {
    trap - INT TERM HUP
    docker stop "$container_name" >/dev/null 2>&1 || true
    docker rename "$container_name" "$backup_name" >/dev/null 2>&1 || return 1
    if (( had_active == 1 )); then
        docker rename "$replaced_name" "$container_name" >/dev/null 2>&1 || return 1
        docker start "$container_name" >/dev/null 2>&1 || return 1
        [[ "$(docker inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null)" == true ]] \
            || return 1
    fi
    return 0
}

if ! docker rename "$backup_name" "$container_name"; then
    if (( had_active == 1 )); then
        docker rename "$replaced_name" "$container_name" >/dev/null 2>&1 \
            && docker start "$container_name" >/dev/null 2>&1 \
            || fail "backup rename failed and the original container could not be restored"
        [[ "$(docker inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null)" == true ]] \
            || fail "backup rename failed and the original container running state is unknown"
    fi
    if (( had_active == 1 )); then
        fail "backup container could not be renamed; original container was restored and started"
    fi
    fail "backup container could not be renamed; no original active container existed"
fi
trap 'restore_replaced || { printf "docker-single-rollback: signal recovery failed after swap\n" >&2; exit 131; }; exit 130' INT TERM HUP
docker start "$container_name" >/dev/null || {
    restore_replaced \
        || fail "backup start failed and the original container could not be restored"
    if (( had_active == 1 )); then
        fail "backup container could not be started; original container was restored and started"
    fi
    fail "backup container could not be started; no original active container existed"
}

deadline=$((SECONDS + timeout))
verified=0
while (( SECONDS < deadline )); do
    if curl --fail --silent --show-error --max-time 5 \
        --proto '=http,https' --output "$tmp_dir/health.json" \
        "$base_url/health" 2>/dev/null \
        && grep -Fq '"kind":"gaius-relay-node"' "$tmp_dir/health.json" \
        && curl --fail --silent --show-error --max-time 5 \
        --proto '=http,https' --output "$tmp_dir/manifest.json" \
        "$base_url/relay-node/v1" 2>/dev/null \
        && grep -Fq '"protocolVersion":1' "$tmp_dir/manifest.json" \
        && grep -Fq '"target-attestation"' "$tmp_dir/manifest.json"; then
        verified=1
        break
    fi
    sleep 1
done

if (( verified != 1 )); then
    restore_replaced \
        || fail "rolled-back container failed verification and the prior state could not be restored"
    if (( had_active == 1 )); then
        fail "rolled-back container failed verification; original container was restored and started"
    fi
    fail "rolled-back container failed verification; no original active container existed"
fi

trap - INT TERM HUP

if (( had_active == 1 )); then
    replaced_display=$replaced_name
else
    replaced_display=none
fi
printf 'docker-single-rollback: restored container=%s host_port=%s replaced=%s health=ok manifest=ok\n' \
    "$container_name" "$host_port" "$replaced_display"
