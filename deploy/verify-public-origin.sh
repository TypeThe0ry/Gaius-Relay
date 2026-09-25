#!/usr/bin/env bash
set -euo pipefail

# Read-only public deployment check.  It deliberately uses the hosted Pages
# origin and the file:// origin separately so a permissive wildcard cannot hide
# a missing production allow-list entry.
base_url=${GAIUS_VERIFY_BASE_URL:-https://example.com}
pages_origin=${GAIUS_VERIFY_PAGES_ORIGIN:-https://example.com}
file_origin=${GAIUS_VERIFY_FILE_ORIGIN:-null}
target_host=${GAIUS_VERIFY_TARGET_HOST:-183.247.170.218}
target_port=${GAIUS_VERIFY_TARGET_PORT:-14803}
resource_pack_url=${GAIUS_VERIFY_RESOURCE_PACK_URL:-https://jihulab.com/-/project/356228/uploads/076ac7018675285fa0f103e4a5ade52a/resource_pack.zip}
encoded_resource_pack_url=$(node -e 'process.stdout.write(encodeURIComponent(process.argv[1]))' "$resource_pack_url")

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

curl_args=(--silent --show-error --location --max-time "${GAIUS_VERIFY_TIMEOUT_SECONDS:-20}")

curl "${curl_args[@]}" -D "$tmp_dir/pages.headers" -o "$tmp_dir/pages.body" \
  -w '%{http_code}' >"$tmp_dir/pages.status" \
  -H "Origin: $pages_origin" \
  "$base_url/relay-node/v1?host=$target_host&port=$target_port"
curl "${curl_args[@]}" -D "$tmp_dir/file.headers" -o "$tmp_dir/file.body" \
  -w '%{http_code}' >"$tmp_dir/file.status" \
  -H "Origin: $file_origin" \
  "$base_url/relay-node/v1?host=$target_host&port=$target_port"
curl "${curl_args[@]}" -D "$tmp_dir/pack.headers" -o "$tmp_dir/pack.body" \
  -w '%{http_code}' >"$tmp_dir/pack.status" \
  -H "Origin: $pages_origin" \
  -H "Range: bytes=0-0" \
  -H "X-Gaius-Resource-Pack: 1" \
  "$base_url/proxy/resource-pack?url=$encoded_resource_pack_url&stream=1"

header_value() {
  awk -F': *' -v wanted="$1" 'tolower($1) == tolower(wanted) {sub(/[\r\n]+$/, "", $2); print $2; exit}' "$2"
}

pages_allow=$(header_value access-control-allow-origin "$tmp_dir/pages.headers")
file_allow=$(header_value access-control-allow-origin "$tmp_dir/file.headers")
pages_status=$(<"$tmp_dir/pages.status")
file_status=$(<"$tmp_dir/file.status")
pack_status=$(<"$tmp_dir/pack.status")
[[ "$pages_status" == 200 ]] || {
  printf 'verify-public-origin: Pages request returned HTTP %s\n' "$pages_status" >&2
  exit 1
}
[[ "$file_status" == 200 ]] || {
  printf 'verify-public-origin: file request returned HTTP %s\n' "$file_status" >&2
  exit 1
}
[[ "$pages_allow" == "$pages_origin" ]] || {
  printf 'verify-public-origin: Pages origin mismatch: expected %s, got %s\n' "$pages_origin" "${pages_allow:-<missing>}" >&2
  exit 1
}
[[ "$file_allow" == "$file_origin" ]] || {
  printf 'verify-public-origin: file origin mismatch: expected %s, got %s\n' "$file_origin" "${file_allow:-<missing>}" >&2
  exit 1
}
[[ "$pack_status" == 200 || "$pack_status" == 206 ]] || {
  printf 'verify-public-origin: resource-pack proxy returned HTTP %s\n' "$pack_status" >&2
  exit 1
}
pack_allow=$(header_value access-control-allow-origin "$tmp_dir/pack.headers")
[[ "$pack_allow" == "$pages_origin" ]] || {
  printf 'verify-public-origin: resource-pack CORS mismatch: expected %s, got %s\n' "$pages_origin" "${pack_allow:-<missing>}" >&2
  exit 1
}

grep -Fq '"kind":"gaius-relay-node"' "$tmp_dir/pages.body" || {
  echo 'verify-public-origin: relay manifest kind missing' >&2
  exit 1
}

printf 'verify-public-origin: PASS base=%s pages=%s file=%s resource-pack=%s\n' "$base_url" "$pages_allow" "$file_allow" "$pack_status"
