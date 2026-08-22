# Gaius 转译节点（RelayNode）

RelayNode is the self-hostable multiplayer component of Gaius. It creates one
temporary WebSocket-to-TCP tunnel per browser connection for Minecraft status
pings and play traffic. It does not store worlds or run the single-player
server, so volunteers can contribute nodes without hosting the game itself.

Run a local node:

```sh
GAIUS_BRIDGE_HOST=0.0.0.0 \
GAIUS_BRIDGE_PORT=8080 \
GAIUS_ALLOWED_ORIGINS=https://play.example,null \
GAIUS_ALLOWED_HOSTS='*.example.net,mc.example.org' \
GAIUS_ALLOWED_RESOURCE_PACK_HOSTS='cdn.example.net,gitlab.com' \
GAIUS_BRIDGE_TOKEN=<runtime-secret> \
GAIUS_CONNECT_TIMEOUT_MS=10000 \
GAIUS_MAXIMUM_CONNECTIONS=256 \
GAIUS_TARGET_AFFINITY_MS=300000 \
GAIUS_MAXIMUM_TARGET_ROUTES=4096 \
GAIUS_RESOURCE_PACK_CACHE_MS=300000 \
GAIUS_RESOURCE_PACK_CACHE_BYTES=536870912 \
GAIUS_RESOURCE_PACK_CACHE_ENTRIES=64 \
GAIUS_RELAY_NODE_NAME='Example RelayNode' \
npm start
```

Use a TLS reverse proxy for public nodes so the browser endpoint is
`wss://relay.example/tunnel`. A node also exposes
`https://relay.example/relay-node/v1` and `/health`, which report the RelayNode
protocol version, supported tunnel features, capacity, and whether a token is
required. `null` in `GAIUS_ALLOWED_ORIGINS` permits a
downloaded `file://` Gaius HTML file; omit it when portable clients should not
use the node. `/health` returns the active and maximum connection counts.

`GAIUS_ALLOWED_HOSTS` controls Minecraft TCP destinations.
When the RelayNode listens on a non-loopback address, private, loopback,
link-local, carrier-grade NAT, multicast, and DNS-resolved private targets are
blocked by default even if this list contains `*`. Set
`GAIUS_ALLOW_PRIVATE_TARGETS=1` only for a trusted LAN deployment.
`GAIUS_ALLOWED_RESOURCE_PACK_HOSTS` independently controls HTTP(S) server-pack
downloads and defaults to the TCP host list for compatibility. Set the latter
to the CDN domains used by the allowed servers; this avoids opening arbitrary
TCP destinations just because a server hosts its pack elsewhere.

Resource-pack bodies are downloaded to a size-limited temporary file before
the RelayNode sends response headers to Chrome. If an upstream CDN cuts a body
short, the node retries the complete GET up to three times; the browser receives
only a complete response with an exact `Content-Length`. A completed `200`
response is retained in a bounded five-minute disk cache keyed by the complete
URL and forwarded Minecraft headers, so status-to-join and nearby players do
not redownload the same pack from a slow CDN. `GAIUS_RESOURCE_PACK_CACHE_MS`,
`GAIUS_RESOURCE_PACK_CACHE_BYTES`, and `GAIUS_RESOURCE_PACK_CACHE_ENTRIES`
control or disable that cache. Client disconnects cancel unfinished upstream
downloads without retrying, and all temporary files are removed on eviction,
failure, or RelayNode exit. Mojang authentication, player textures, Realms, and
the blocked-server list use the same origin/token-gated HTTP proxy. Their
idempotent GET requests also retry transient network failures and 429/502/503/504
responses; authentication and Realms write requests are never replayed.

The public registry rejects token-protected nodes because it never publishes a
shared tunnel secret to browsers. A public wildcard node can reach arbitrary
public Minecraft servers, but still needs upstream rate limiting, per-IP
connection quotas, traffic accounting and an abuse contact. Private nodes can
use `GAIUS_BRIDGE_TOKEN` and a restricted `GAIUS_ALLOWED_HOSTS` list. A node
operator can give players its URL. The browser accepts either a legacy string
or a node object in `gaius.bridgeNodes`; objects can carry their own token and
priority:

```json
[
  {"url":"https://relay.example", "priority":100},
  {"url":"wss://backup.example/tunnel", "token":"node-token", "priority":10}
]
```

For every multiplayer status ping or join, the browser first attempts the
same-host Gaius server plugin while it queries every discovered RelayNode at
`/relay-node/v1?host=...&port=...`. It prefers a node with an active connection
to that normalized Minecraft target, then a node that reached it recently,
then available capacity and configured priority. A string URL and repeated
`bridge` query parameters remain supported. Target-specific manifest queries
use the node token when configured and never expose the node's other targets.
The browser retains a failed direct-plugin probe for 30 seconds and successful
RelayNode manifest results for 15 seconds. This removes the repeated discovery
delay between a server-list ping and the subsequent join without reusing their
WebSocket or TCP streams.
The initial same-host WebSocket probe remains bounded to 800 milliseconds. A
current Gaius server plugin immediately returns `connecting` with its configured
TCP timeout, at which point the browser extends only that live direct attempt.
Older plugins remain compatible and use the original short probe behavior.

Affinity only reuses the selected RelayNode. Every browser channel still opens
its own WebSocket and its own Minecraft TCP connection; protocol streams are
never shared between players, status pings, or reconnects. A tunnel is usable
only after its `connected` control response, which means that node reached the
requested Java server. `GAIUS_TARGET_AFFINITY_MS` controls how long a successful
target remains preferred after its last connection, and
`GAIUS_MAXIMUM_TARGET_ROUTES` bounds retained inactive route records.
The temporary unit is the tunnel lease, not the RelayNode process. Closing the
browser channel cancels an in-progress TCP dial or destroys its connected TCP
socket immediately. When the last player leaves, the target's active count is
zero; only bounded, expiring reachability metadata remains for later node
selection and it carries no open server connection.
The manifest publishes `targetConnectTimeoutMs`, and the browser derives a
larger end-to-end tunnel budget from it so an SRV timeout can still fall back to
the entered host and port. `GAIUS_CONNECT_TIMEOUT_MS` controls each RelayNode
TCP attempt. Closing the browser tunnel cancels any attempt still in progress.

The downloaded HTML cannot start a process on another machine. Volunteer nodes
must already be deployed and listed in the curated root `relay-nodes.json`, a
leased live registry, a custom `relayRegistry`, `gaius.bridgeNodes`, or a
`bridge` query parameter. The portable HTML embeds the registry snapshot and
refreshes it from GitHub when it has network access. A root registry can point
to a live registry, so contributors can renew short leases without rebuilding
the client. A RelayNode removes its lease during normal SIGINT/SIGTERM shutdown;
the registry TTL remains the fallback for crashes or network partitions. The
client discovers which node already has affinity for the
entered server and opens a new isolated tunnel there; if none does, it selects
an available node and establishes the first tunnel. See
[`docs/relay-nodes.md`](../../docs/relay-nodes.md) for the contribution and
security requirements.

By default, the RelayNode replies to exact unencrypted keepalive frames while
the browser is busy reloading a server resource pack. It selects the packet
table from the initial Minecraft handshake for 1.21.11 (774) or 26.2 (776),
then emits the profile-specific payloadless client tick after configuration
enters PLAY and replays the client's exact tick frame while the browser is
stalled. The relay tracks the reversible `PLAY -> CONFIGURATION -> PLAY`
transition: it stops synthetic PLAY ticks as soon as the server sends Start
Configuration, proxies configuration keepalives, and resumes only after the
client finishes the new configuration cycle. Unknown or malformed profiles
remain opaque and never receive a guessed rewrite. Set
`GAIUS_PROXY_KEEPALIVES=0` to disable this behavior.
Encrypted online-mode traffic is opaque and remains fully transparent.

Run the focused protocol test with:

```sh
GAIUS_SMOKE_MINECRAFT_VERSION=1.21.11 npm run smoke
GAIUS_SMOKE_MINECRAFT_VERSION=26.2 npm run smoke
npm run smoke:profiles
```

Run the standalone full browser-transport path against an unmodified local
vanilla server and the local RelayNode with:

```sh
npm run smoke:browser-full-path
GAIUS_VERSION_PROFILE_PATH=1.21.11 npm run smoke:browser-full-path
```

This check is intentionally not part of `npm run smoke` or ordinary CI: it
starts Java and a real local RelayNode, and requires the profile-scoped vanilla
archive at `port/target/<profile>/multiplayer-smoke-server/server-artifact.bin`. The archive
must be a regular file whose SHA-1 matches the active profile; set
`GAIUS_BROWSER_FULL_PATH_SERVER_archive` to an existing verified archive when using a
different location. `GAIUS_BROWSER_FULL_PATH_CLIENTS` (1--4) and
`GAIUS_BROWSER_FULL_PATH_SOAK_MS` tune the run. MSYS `/c/...` paths are accepted
for the profile, archive, and Java environment variables. Evidence and logs remain
under `port/target/<profile>/browser-relay-full-path-evidence/`.

The harness evaluates the source `BrowserWebSocketChannel` JSBody with Node's
`ws` implementation, so it proves real WebSocket framing, RelayNode TCP
acceptance, online-mode RSA/AES, configuration, and PLAY/chunk traffic without
requiring a TeaVM rebuild; the session API is a deterministic local fixture,
not a call to Mojang. It is not a headed Chrome test: browser DOM,
Chrome's WebSocket implementation, rendering/event-loop scheduling, and
TeaVM-generated call boundaries remain outside this check.

Verify the public node separately against a real Java server without making
external network availability a CI requirement:

```sh
GAIUS_PUBLIC_RELAY_MINECRAFT_VERSION=1.21.11 npm run smoke:public
GAIUS_PUBLIC_RELAY_MINECRAFT_VERSION=26.2 npm run smoke:public
GAIUS_PUBLIC_RELAY_MINECRAFT_VERSION=26.2 \
  GAIUS_PUBLIC_RELAY_TARGET=example.org:25565 npm run smoke:public
```

The public smoke uses the first node in the root `relay-nodes.json` unless
`GAIUS_PUBLIC_RELAY_URL` is set. It sends the same `Origin: null` as the
downloaded HTML, opens an isolated tunnel, completes a Minecraft status query,
closes the WebSocket, and verifies that the target lease is released. On hosts
whose local proxy exposes DNS through the reserved `198.18.0.0/15` fake-IP
range, the smoke resolves only the RelayNode hostname through DNS-over-HTTPS
while preserving TLS hostname verification. `GAIUS_PUBLIC_RELAY_EDGE_IP` can
be used as an explicit diagnostic override.

The smoke test injects two cleanly truncated 20 MiB resource-pack responses
whose `Content-Length` still declares the full body, requires the third
response to reach the client with its original SHA-1, verifies a second cache
hit without another upstream request, and ensures a browser abort does not
start a retry. The
local protocol fixture also forces a complete PLAY reconfiguration and rejects
any PLAY tick injected before configuration finishes. Set
`GAIUS_SMOKE_PLAY_SOAK_MS=60000` with `GAIUS_SMOKE_MINECRAFT_HOST` to keep a
real server connection alive and validate any server-initiated reconfiguration.
The protocol fixture defaults to Minecraft `1.21.11` (protocol `774`); set
`GAIUS_SMOKE_MINECRAFT_VERSION=26.2` (or `776`) to exercise the 26.2 packet
table. RelayNode selects the same table from each client's initial handshake,
so one node can carry either supported version without a global setting.
The optional public-server path also understands supported vanilla server
dialogs and Code of Conduct confirmation before entering PLAY. Prompt handling
is disabled unless the smoke process is explicitly started with
`GAIUS_SMOKE_ACCEPT_SERVER_PROMPTS=1`. Text fields must also be supplied as a
JSON object in `GAIUS_SMOKE_DIALOG_INPUTS_JSON`; for example, an offline server
registration test can provide `{"password":"...","confirm":"..."}`. Use a
disposable test account because registration changes server state. The
production client keeps the normal visible screens and never accepts a server
policy or submits credentials silently.

## Container deployment

The repository includes one container definition for both the RelayNode and
its optional lease registry. The compose example starts both processes. Copy
`compose.example.yaml` to `compose.yaml`, replace every example hostname,
origin and registry token, then start it behind a TLS reverse proxy:

```sh
docker compose up -d --build
```

Expose RelayNode `/health` and `/relay-node/v1` through HTTPS and `/tunnel`
through WSS. Expose registry `/relay-nodes.json` through HTTPS, but keep the
lease write endpoint and `GAIUS_REGISTRY_TOKEN` restricted to trusted node
operators. Do not publish ports 8080 or 8083 directly to the public Internet
unless an upstream proxy enforces TLS, rate limits and request accounting.

For a public node, use the TLS-ready example instead of exposing either Node.js
process directly:

```sh
cp public.env.example .env
# Edit both DNS names, the allowed client origins, node identity, and secret.
docker compose --env-file .env -f compose.public.example.yaml up -d --build
```

`compose.public.example.yaml` runs the registry and RelayNode on a private
Compose network. Caddy is the only service with host ports: it obtains TLS,
serves the RelayNode manifest and WSS tunnel on `GAIUS_RELAY_DOMAIN`, and exposes
only `/relay-nodes.json` plus `/health` on `GAIUS_REGISTRY_DOMAIN`. The bearer
token lease endpoint is deliberately unavailable through Caddy. Add
`https://<GAIUS_REGISTRY_DOMAIN>/relay-nodes.json` to the root
`relay-nodes.json` only after the public health and manifest URLs work.
