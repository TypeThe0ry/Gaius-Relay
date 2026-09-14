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
GAIUS_RESOURCE_PACK_HEADERS_TIMEOUT_MS=15000 \
GAIUS_RESOURCE_PACK_BODY_IDLE_TIMEOUT_MS=15000 \
GAIUS_RESOURCE_PACK_OVERALL_TIMEOUT_MS=110000 \
GAIUS_RESOURCE_PACK_STREAM_OVERALL_TIMEOUT_MS=300000 \
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

The client requests `/proxy/resource-pack?stream=1` to receive real resource-pack
bytes while the upstream download is still in progress. The RelayNode forwards a
valid upstream `Content-Length`, independently spools the upstream body to a
size-limited temporary file, and lets the downstream response tail completed file
regions through a fixed 256 KiB read window. A slow browser or reverse proxy
therefore cannot serialize every upstream read behind its HTTP backpressure, and
memory usage stays bounded regardless of pack size. The shared cache is populated
only after the complete body and declared length have been validated. A truncated
or failed spool is closed and discarded; partial bytes are never cached or joined
to bytes from another attempt.
Requests without `stream=1` retain the buffered behavior: download before sending
headers, retry an interrupted body up to three times, and return the complete
response with an exact `Content-Length`. Both paths share the cache. A completed `200`
response is retained in a bounded five-minute disk cache keyed by the complete
URL and forwarded Minecraft headers, so status-to-join and nearby players do
not redownload the same pack from a slow CDN. `GAIUS_RESOURCE_PACK_CACHE_MS`,
`GAIUS_RESOURCE_PACK_CACHE_BYTES`, and `GAIUS_RESOURCE_PACK_CACHE_ENTRIES`
control or disable that cache. Client disconnects cancel unfinished upstream
downloads without retrying, and all temporary files are removed on eviction,
failure, or RelayNode exit. Mojang authentication, player textures, Realms, and
the blocked-server list use the same origin/token-gated HTTP proxy. Resource-pack
upstream headers, body-idle, and complete-download deadlines are controlled by
`GAIUS_RESOURCE_PACK_HEADERS_TIMEOUT_MS`,
`GAIUS_RESOURCE_PACK_BODY_IDLE_TIMEOUT_MS`, and
`GAIUS_RESOURCE_PACK_OVERALL_TIMEOUT_MS` (buffered requests, default 110 seconds),
and `GAIUS_RESOURCE_PACK_STREAM_OVERALL_TIMEOUT_MS` (streaming requests, default
300 seconds, maximum 600 seconds). Headers and idle timeouts still apply to
streaming requests, including clients stalled by backpressure. A timeout before
headers returns a CORS `504`; after response data starts, it closes the incomplete
stream. Streaming requires the updated RelayNode deployment; older nodes ignore
the query parameter and continue buffering. Reverse proxies must also permit
streaming instead of buffering the entire response. Their
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
GAIUS_VERSION_PROFILE_PATH=versions/1.21.11.json npm run smoke:browser-full-path
```

The strict dual-profile acceptance gate runs `26.2` first and `1.21.11` second
with exactly 4 clients, 9 chunks, one real reconnect wave, and a 15,000 ms
post-reconnect soak. It fails closed on suffixed/invalid gate parameters and
returns one aggregate JSON record (nonzero if either profile fails):

```sh
npm run smoke:browser-full-path-acceptance
```

The acceptance runner owns the profile order (`26.2`, then `1.21.11`), the
exact 4/9/15,000/1 gate, and a 600,000 ms per-profile wall-clock deadline.
It removes inherited `GAIUS_*` tuning, registry/DNS/trace/private-network,
target-affinity, and server-archive override variables before starting each child;
only the profile Java candidates (`GAIUS_JAVA`, `GAIUS_JAVA_HOME`,
`GAIUS_JAVA_21`, and `GAIUS_JAVA_25`) are preserved. It then injects fixed
loopback test values. The child must report the executable and major actually
probed: strict `1.21.11` requires Java major **21 exactly**, while strict
`26.2` accepts Java major **25 or newer**. A candidate variable may name either
the `java` executable or a JDK home (the resolver adds `bin/java`).

This is separate from the standalone compatibility smoke. `npm run
smoke:browser-full-path` (or a direct `GAIUS_VERSION_PROFILE_PATH=...` run)
keeps its compatibility tuning and accepts a verified
`GAIUS_BROWSER_FULL_PATH_SERVER_archive` override; those tuning and override
variables do not change the strict runner's fixed contract. The aggregate
acceptance JSON has `actual` and `observed` copied only from independently
validated child acceptance sections, while `runs` contains process/timeout
diagnostics.

High-load multiplayer is a third, explicitly opt-in contract. It does **not**
change the ordinary 2-client smoke or the strict 4/9/15,000/1 acceptance gate.
The stress runner supports only the fixed 8- and 16-client tiers. Both use an
8-chunk client/server view distance, a 4-chunk simulation distance, all 257
unique positions in Minecraft's radius-8 `ChunkTrackingView`, and repeated
full lifecycle waves. Tier 8 uses two reconnects (24 client lifecycles) and a
60-second post-reconnect soak; tier 16 uses four reconnects (80 client
lifecycles) and a 120-second soak. Tier 8 acknowledges chunk batches at 32
chunks/tick; tier 16 uses 64 chunks/tick. The child hard deadlines are 20 and
40 minutes respectively, and timeout/exit/signal/elapsed/stdout/stderr/JSON
parse evidence is retained in the aggregate record:

```sh
npm run stress:browser-full-path -- --tier=8
npm run stress:browser-full-path -- --tier=16
# Sequentially run both tiers (expensive):
npm run stress:browser-full-path -- --all
# Validate the resolved contract without starting Java, RelayNode, or clients:
npm run stress:browser-full-path -- --tier=16 --print-config
```

The stress entry point always selects the repository's canonical 26.2 profile
(protocol 776/world 4903), removes inherited build/dist/profile overrides, and
removes every external relay/target/dialog-input override. This prevents an
8/16-client run from accidentally targeting `relay-host` or an operator's existing
relay. A hard-deadline timeout terminates the job-owned child process tree
(`taskkill /T /F` on Windows, a detached process group on POSIX) and records the
cleanup attempt/error with the process evidence.

Each PLAY client handles Chunk Batch Finished/Start packet IDs 11/12 and sends
the profile-specific big-endian float32 batch acknowledgement (serverbound ID
11 for 26.2, ID 10 for 1.21.11). Evidence keeps total and unique chunks,
duplicates, batch/ACK counts, configured/effective radius, the exact radius
capacity, observed bounds, and a bounded batch list. Stress evidence parses
the real PLAY Set Chunk Cache Center/Radius and Set Simulation Distance packets
(26.2 IDs 94/95/111; 1.21.11 IDs 92/93/109). A stress client must observe
radius 8 and simulation distance 4 before any unique chunk counts toward the
257-position target; the observed radius capacity must also be exactly 257.
The runner rejects a requested unique-chunk target above the configured
client/server radius before starting the vanilla server. Deterministic UUIDs
remain exactly 32 hexadecimal
digits beyond client 9, and the session fixture uses an explicit
profile-ID-to-username map rather than decoding a one-digit suffix.

Extreme latency evidence uses only fixed bucket counters (1/2/4/8/16.7/25/50/
60/75/100/250/500/1000/+Inf ms), never an unbounded raw sample list. Each client
reports count, p95, p99, p99.9 and max for poll gaps, PLAY tick gaps, and
pre-target chunk packet gaps. Stress fails closed above: poll p99 16.7 ms,
p99.9 50 ms, max 100 ms; tick p99 60 ms, p99.9 75 ms, max 100 ms; pre-target
chunk p99 100 ms, max 250 ms.

The browser transport flushes the first idle-to-active outbound write in a
microtask, then keeps every 32-frame/256-KiB/2-ms budget continuation on a
`MessageChannel` macrotask. Exactly one continuation callback runs per message
task, so rendering and inbound network work retain task boundaries without
accumulating the browser's nested zero-delay timer clamp. Timers remain only
for real WebSocket `bufferedAmount` backpressure or as a fail-closed fallback
when `MessageChannel` is unavailable. Strict multiplayer evidence exposes the
schedule/callback/fallback counters and rejects any budget continuation that
silently falls back to a clamped timer.

Each profile result includes the required-versus-observed/actual soak and wave
health, per-client online RSA/AES fail-closed evidence, canonical profile tuple,
actual server-archive SHA-1, browser/RelayNode/target active counts, and explicit
RelayNode runtime-gauge/synthetic-marker evidence.

This strict check is intentionally not part of `npm run smoke` or ordinary CI:
it starts Java and a real local RelayNode, and requires the profile-scoped
vanilla archive at `port/target/<profile>/multiplayer-smoke-server/server-artifact.bin`.
The archive must be a regular file whose SHA-1 matches the active profile; the
strict runner does not accept a server-archive override. Use the standalone
compatibility command above when a different verified archive or tunable values
are needed. In that standalone mode, `GAIUS_BROWSER_FULL_PATH_CLIENTS` (1--4)
and `GAIUS_BROWSER_FULL_PATH_SOAK_MS` tune the run.
`GAIUS_BROWSER_FULL_PATH_MIN_CHUNKS` defaults to 9 (range 1--128) and gates
every client on that many PLAY chunk packets. The JSON evidence records
relay/login/configuration/PLAY/first-chunk timing, packet and byte rates, queue
cleanup, WebSocket cleanup, target-lease cleanup, and RelayNode CPU/RSS deltas
for both protocol 776 and 774. Online-mode evidence also fails if the encrypted
PLAY tunnels arm the offline stall-tick interval; those timers are now created
only after a framed connection actually enters PLAY. MSYS `/c/...` paths are
accepted for the profile, archive, and Java environment variables. Evidence and logs remain under
`port/target/<profile>/browser-relay-full-path-evidence/`.
`GAIUS_BROWSER_FULL_PATH_CLIENT_VIEW_DISTANCE` (2--32, default 6) controls the
encoded client preference; `GAIUS_BROWSER_FULL_PATH_SERVER_VIEW_DISTANCE`
(2--32, default 2) controls the local vanilla fixture. These compatibility
knobs do not alter either fixed release acceptance or fixed stress tiers.

Set `GAIUS_BROWSER_FULL_PATH_RECONNECT_WAVES=1` (default `0`, range `0--8`)
to add a simultaneous multiplayer reconnect gate. After every client reaches
PLAY and the chunk threshold, the harness abnormally terminates every WebSocket
in one dispatch turn without asking the harness client to send a Minecraft
disconnect packet. It verifies that each closed bridge entry retains its
non-1000 close error and a deterministic synthetic marker delivered directly
through the JSBody `onmessage` path immediately before termination. The marker
checks inbound queue ordering and retention; it is not evidence of a real
network tail frame. The harness then invokes the Java-like channel final-close
hook and waits for active Browser and RelayNode state to drain to zero before reconnecting the same
account identities on entirely new channel IDs. Each wave repeats online-mode
session join/hasJoined, RSA/AES, configuration, PLAY, and chunk acceptance
without reusing cipher objects or protocol buffers. Wave evidence includes
drop-to-stage timings, RelayNode
active/target-total connections and CPU/RSS deltas, browser queues/leases,
WebSockets/channels, session counters, and final all-zero cleanup. A dual-profile
cluster acceptance run normally uses 4 clients, 9 chunks, one reconnect wave,
and a 15-second post-reconnect soak.

For a real public RelayNode and Minecraft target, run the external multi-client
transport check with the target's actual port:

```sh
GAIUS_EXTERNAL_RELAY_URL=wss://example.com/tunnel \
GAIUS_EXTERNAL_TARGET=relay-host.top:00000 \
GAIUS_EXTERNAL_CLIENTS=4 \
GAIUS_EXTERNAL_SOAK_MS=15000 \
npm run smoke:external-multiplayer
```

This check drives the browser bridge's real WebSocket JSBody and Minecraft
status protocol through every external tunnel. It records status RTT, target
attestation, RelayNode active connections, browser queues, event-loop gaps, and
zero-state cleanup. It also requires the deployed RelayNode runtime manifest to
publish DNS cache counters and proves that the extra same-target clients reused
an in-flight or short-lived lookup. A node without those counters is stale and
fails closed. It is intentionally not LOGIN/PLAY evidence; use the
strict full-path acceptance gate above for encrypted multiplayer, chunks,
reconnect, and soak. `GAIUS_EXTERNAL_ENABLE_PING=1` enables an optional status
ping probe and fails closed if the remote endpoint does not return every pong.

For release-grade external multiplayer evidence, run the full-path harness with
`GAIUS_BROWSER_FULL_PATH_ACCEPTANCE=1` (or `--acceptance`). In that mode the
external RelayNode must expose the complete runtime-gauge contract, including
both logical/physical connection gauges (`activeTunnelLeases` and
`activeTransportWebSockets`), and prove zero active leases, drain handles,
synthetic ticks, and stall timers at every required lifecycle point. The logical
lease retires when the tunnel is closed; the physical WebSocket gauge may remain
nonzero briefly while the close handshake drains, so both are recorded rather
than collapsed into one connection count. Compatible external runs may record
a node that predates those gauges, but such a run is not a no-stall release
result.
The currently optional `retireClosedEntry` hook is reported as undefined and
not invoked by this harness; the gate proves retained close evidence and final
cleanup but does not claim to repair that product hook.
Reconnect creation is driven manually by the harness after cleanup; this gate
does not exercise or claim an automatic product retry policy.

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
be used as an explicit diagnostic override. When a diagnostic URL points at an
edge IP behind a virtual-host proxy, set `GAIUS_PUBLIC_RELAY_HOST_HEADER` to
the configured RelayNode host; the smoke sends that value as the
HTTP/WebSocket `Host` header while leaving normal DNS/TLS behavior unchanged.

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
registration test can provide `{"password":"...","confirm":"..."}`. For
real external tests, prefer `GAIUS_SMOKE_DIALOG_INPUTS_FILE` pointing to an
owner-readable-only JSON file with the same object, so credentials do not enter
the process command line, environment value, or evidence logs. Use a disposable
test account because registration changes server state. If a server emits more
than one dynamic dialog for one login (for example, an outer AuthMe dialog
followed by a separate password submit dialog), set
`GAIUS_SMOKE_DIALOG_ACTION_IDS` to a comma-separated action sequence in the
same order; this is mutually exclusive with the single-dialog
`GAIUS_SMOKE_DIALOG_ACTION_ID` override. The production client keeps the normal
visible screens and never accepts a server policy or submits credentials
silently.

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
