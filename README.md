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
GAIUS_BRIDGE_TOKEN=<runtime-secret> \
GAIUS_MAXIMUM_CONNECTIONS=256 \
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

Do not expose a public node with the default wildcard destination and no access
token. Restrict `GAIUS_ALLOWED_HOSTS`, or put authentication, rate limiting,
traffic accounting, and abuse controls in front of the relay. A node operator
can give players its URL. The browser accepts either a legacy string or a node
object in `gaius.bridgeNodes`; objects can carry their own token and priority:

```json
[
  {"url":"https://relay.example", "priority":100},
  {"url":"wss://backup.example/tunnel", "token":"node-token", "priority":10}
]
```

Higher priorities are tried first. A string URL and repeated `bridge` query
parameters remain supported. The client confirms each selected node by opening
the tunnel and waiting for its `connected` control response; that response
means the RelayNode reached the requested Java server.

By default, the RelayNode replies to the exact unencrypted 1.21 keepalive frame
while the browser is busy reloading a server resource pack. This prevents a
backend read timeout during a long main-thread reload without inspecting or
modifying ordinary game packets. Set `GAIUS_PROXY_KEEPALIVES=0` to disable it.
Encrypted online-mode traffic is opaque and remains fully transparent.

Run the focused protocol test with:

```sh
npm run smoke
```

## Container deployment

The repository includes a production container definition and a compose
example. Copy `compose.example.yaml` to `compose.yaml`, replace every example
origin, destination and token, then start it behind a TLS reverse proxy:

```sh
docker compose up -d --build
```

Expose `/health` and `/relay-node/v1` through HTTPS and `/tunnel` through WSS.
Do not publish port 8080 directly to the public Internet unless an upstream
proxy enforces TLS and rate limits.
