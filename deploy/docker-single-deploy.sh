#!/usr/bin/env bash
set -euo pipefail

container_name=${GAIUS_DOCKER_CONTAINER:-gaius-relaynode}
backup_name=${GAIUS_DOCKER_BACKUP_CONTAINER:-${container_name}-previous}
image=${GAIUS_DOCKER_IMAGE:-gaius-relaynode:current}
host_port=${GAIUS_DOCKER_HOST_PORT:-18080}
env_file=${GAIUS_DOCKER_ENV_FILE:-docker-single.env}
base_url=${GAIUS_DOCKER_VERIFY_BASE_URL:-http://127.0.0.1:${host_port}}
timeout=${GAIUS_DOCKER_VERIFY_TIMEOUT_SECONDS:-45}
lock_file=${GAIUS_DOCKER_LOCK_FILE:-/run/lock/${container_name}.deploy.lock}

fail() {
    printf 'docker-single-deploy: %s\n' "$1" >&2
    exit 1
}

command -v docker >/dev/null 2>&1 || fail "docker is required"
command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v mktemp >/dev/null 2>&1 || fail "mktemp is required"
command -v flock >/dev/null 2>&1 || fail "flock is required"
[[ -f "$env_file" ]] || fail "environment file is missing: $env_file"
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

if docker container inspect "$backup_name" >/dev/null 2>&1; then
    fail "rollback backup already exists: $backup_name; validate or remove it before deploying again"
fi

# Build from package-lock.json. Dockerfile performs npm ci --omit=dev.
docker build --tag "$image" "$(cd "$(dirname "$0")/.." && pwd)"

had_previous=0
recover_stopping_previous() {
    trap - INT TERM HUP
    docker start "$container_name" >/dev/null 2>&1 || return 1
    [[ "$(docker inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null)" == true ]]
}
recover_renamed_previous() {
    trap - INT TERM HUP
    docker rename "$backup_name" "$container_name" >/dev/null 2>&1 || return 1
    docker start "$container_name" >/dev/null 2>&1 || return 1
    [[ "$(docker inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null)" == true ]]
}
if docker container inspect "$container_name" >/dev/null 2>&1; then
    trap 'recover_stopping_previous || { printf "docker-single-deploy: signal recovery failed before rename\n" >&2; exit 131; }; exit 130' INT TERM HUP
    if ! docker stop "$container_name" >/dev/null; then
        docker start "$container_name" >/dev/null 2>&1 \
            || fail "existing container stop failed and it could not be restarted: $container_name"
        running=$(docker inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null || true)
        [[ "$running" == true ]] \
            || fail "existing container stop failed and its running state is unknown: $container_name"
        fail "existing container could not be stopped; it was confirmed running again: $container_name"
    fi
    if ! docker rename "$container_name" "$backup_name"; then
        docker start "$container_name" >/dev/null 2>&1 \
            || fail "existing container rename failed and it could not be restarted"
        running=$(docker inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null || true)
        [[ "$running" == true ]] \
            || fail "existing container rename failed and its running state is unknown"
        fail "existing container could not be renamed; it was confirmed running again"
    fi
    had_previous=1
    trap 'recover_renamed_previous || { printf "docker-single-deploy: signal recovery failed after rename\n" >&2; exit 131; }; exit 130' INT TERM HUP
fi

verify_active() {
    local deadline=$((SECONDS + timeout))
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
            return 0
        fi
        sleep 1
    done
    return 1
}

rollback() {
    trap - INT TERM HUP
    local failed_name="${container_name}-failed-$(date +%s)"
    if docker container inspect "$container_name" >/dev/null 2>&1; then
        docker stop "$container_name" >/dev/null 2>&1 || true
        if ! docker rename "$container_name" "$failed_name"; then
            if (( had_previous == 1 )); then
                docker rm --force "$container_name" >/dev/null 2>&1 \
                    || fail "failed release could not be removed to restore the previous container"
            else
                fail "failed first release could not be preserved for inspection"
            fi
        fi
    fi
    if (( had_previous == 1 )); then
        if ! docker rename "$backup_name" "$container_name"; then
            if docker container inspect "$failed_name" >/dev/null 2>&1 \
                && docker rename "$failed_name" "$container_name" >/dev/null 2>&1 \
                && docker start "$container_name" >/dev/null 2>&1 \
                && [[ "$(docker inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null)" == true ]]; then
                fail "previous container could not be restored; failed release was confirmed running again"
            fi
            fail "previous container could not be restored and no replacement was confirmed running"
        fi
        docker start "$container_name" >/dev/null \
            || fail "previous container was restored by name but could not be started"
        verify_active \
            || fail "previous container restarted but failed health or target-attestation verification"
        printf 'docker-single-deploy: previous container restored and verified; failed=%s\n' \
            "$failed_name" >&2
    else
        printf 'docker-single-deploy: first deployment failed; no previous container exists; failed=%s\n' \
            "$failed_name" >&2
    fi
}

trap 'rollback; exit 130' INT TERM HUP

if ! docker run --detach --name "$container_name" --restart unless-stopped \
    --env-file "$env_file" \
    --publish "127.0.0.1:${host_port}:8080" \
    "$image" >/dev/null; then
    rollback
    fail "new container could not be started"
fi

if ! verify_active; then
    rollback
    fail "health or manifest verification failed"
fi

trap - INT TERM HUP

printf 'docker-single-deploy: active container=%s host_port=%s health=ok manifest=ok backup=%s\n' \
    "$container_name" "$host_port" "$backup_name"
