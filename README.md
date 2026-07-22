# Gaius community relay

This service is independent from the downloadable Gaius browser client. It
creates temporary WebSocket-to-TCP tunnels for Minecraft status pings and play
connections, and does not store worlds or run the single-player server.

Run a local node:

```sh
GAIUS_BRIDGE_HOST=0.0.0.0 \
GAIUS_BRIDGE_PORT=8080 \
GAIUS_ALLOWED_ORIGINS=https://play.example,null \
GAIUS_ALLOWED_HOSTS='*.example.net,mc.example.org' \
GAIUS_BRIDGE_TOKEN=<runtime-secret> \
GAIUS_MAXIMUM_CONNECTIONS=256 \
node dist/main.js
```

Use a TLS reverse proxy for public nodes so the browser endpoint is
`wss://relay.example/tunnel`. `null` in `GAIUS_ALLOWED_ORIGINS` permits a
downloaded `file://` Gaius HTML file; omit it when portable clients should not
use the node. `/health` returns the active and maximum connection counts.

Do not expose a public node with the default wildcard destination and no access
token. Restrict `GAIUS_ALLOWED_HOSTS`, or put authentication, rate limiting,
traffic accounting, and abuse controls in front of the relay. A node operator
can give players its URL; players add it with a repeated `bridge` query
parameter or the `gaius.bridgeNodes` local-storage JSON array.
