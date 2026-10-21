# Gaius RelayNode

RelayNode is a self-hostable WebSocket-to-TCP transport for browser clients
that need to reach an ordinary Java server. Each browser connection receives
an isolated TCP socket; the RelayNode does not store worlds or run a game
server.

## Local run

```sh
GAIUS_BRIDGE_HOST=127.0.0.1 \
GAIUS_BRIDGE_PORT=8080 \
GAIUS_ALLOWED_ORIGINS=null \
GAIUS_ALLOWED_HOSTS='*.example.test' \
GAIUS_ALLOWED_RESOURCE_PACK_HOSTS='cdn.example.test' \
GAIUS_MAXIMUM_CONNECTIONS=256 \
GAIUS_RELAY_NODE_NAME='Example RelayNode' \
npm start
```

Put real origins, destinations, and credentials in the process environment or
an operator-owned secret store. Do not commit them. Public deployments should
use TLS, a destination allow-list, origin checks, connection and traffic
limits, and an abuse contact. The manifest endpoints are `/health`,
`/relay-node/v1`, and `/relay-node/v1.runtime`; the tunnel endpoint is
`/tunnel`.

Resource-pack proxying is bounded and can stream through a temporary file. The
registry process in `dist/registry.js` accepts authenticated leases on a
private management endpoint; never expose its management credential to a
browser.

## Tests

The public test suite uses only local TCP/HTTP fixtures and the checked-in
runtime:

```sh
npm ci
npm test
```

The suite covers framing, bounded drains, resource-pack timeouts and streams,
registry leases, bandwidth limits, telemetry, abuse metadata, and the frame
timeline contract. Browser/client acceptance and server integration tests live
in the private client project and consume a pinned RelayNode release.

## Release and integration contract

The wire-contract tables in `dist/protocol.js` are versioned data. A client
must pin a RelayNode release and verify the manifest protocol version before
opening `/tunnel`. The client-facing contract is limited to the HTTP/WebSocket
endpoints and manifest fields; this repository does not contain a browser
client, build inputs, game assets, protocol-metadata, or generated client output.

Publish a versioned npm tarball or OCI image and pin its version and digest in
the consuming client's integration tests. Deployments should use the examples
under `deploy/` and `compose.public.example.yaml` with operator-owned values.

## License and security

The RelayNode source is MIT-licensed. See `LICENSE`. Report vulnerabilities
through the private GitHub security channel described in `SECURITY.md`; do not
include live endpoints, credentials, or target-server details in public issues.
