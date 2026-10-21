# RelayNode deployment templates

These templates are deployment examples. Replace every `example.com` value
with an operator-owned DNS name and provide credentials through a root-owned
environment file or secret manager. No live endpoint or credential belongs in
this repository.

The service template expects the checkout at `/opt/gaius/relay`, listens on a
loopback address, and places Nginx or Caddy in front of the Node process. Keep
the registry lease write endpoint on a private network. Public listeners need
TLS, origin and destination policy, connection limits, traffic limits, and an
abuse contact.

## Files

- `env.example`: non-secret RelayNode defaults.
- `gaius-relaynode.service`: hardened systemd unit.
- `nginx-gaius-relay.conf` and `nginx-gaius-relay-docker.conf`: narrow proxy
  locations for the tunnel, manifests, health, and resource-pack proxy.
- `docker-single-compose.example.yaml` and the deploy/rollback scripts: a
  loopback-bound single-container deployment.
- `verify-runtime.sh`, `verify-dnat.sh`, and `verify-public-origin.sh`: read-only
  operator checks. Supply target and origin values in the environment when
  running them.

## Local validation

```sh
npm ci
npm test
```

Run the verification scripts only against infrastructure that you operate:

```sh
GAIUS_VERIFY_BASE_URL=https://relay-proxy.example.com \
GAIUS_VERIFY_PAGES_ORIGIN=https://play.example.com \
GAIUS_VERIFY_TARGET_HOST=server.example.test \
GAIUS_VERIFY_TARGET_PORT=25565 \
GAIUS_VERIFY_RESOURCE_PACK_URL=https://cdn.example.test/pack.zip \
bash deploy/verify-public-origin.sh
```

Do not commit the values used for a real deployment.
