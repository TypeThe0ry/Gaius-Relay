# Linux RelayNode deployment

This directory is a reproducible, key-free deployment template for one public
Gaius RelayNode. It runs the Node process on 127.0.0.1:8080 and puts Nginx in
front of it for TLS and the browser-facing API. It does not run a Minecraft
server, a registry management service, or a downloaded game client.

The public node accepts arbitrary public Java server targets by default.
Private, loopback, link-local, carrier-grade NAT, multicast, and reserved
targets remain blocked by GAIUS_ALLOW_PRIVATE_TARGETS=0. Keep that setting on
an Internet-facing node.

## Files

- gaius-relaynode.service: systemd unit with explicit working directory,
  executable, and environment file.
- env.example: public-safe defaults and bounded connection, frame, cache, and
  timeout settings. It contains no password or token.
- docker-single.env.example and docker-single-compose.example.yaml: single
  Docker container with container port 8080 bound only to host loopback port
  18080.
- docker-single-deploy.sh and docker-single-rollback.sh: mutually exclusive
  image deployment, preserved old/failed containers, health/manifest gate, and
  verified rollback.
- verify-dnat.sh: read-only IPv4/IPv6 NAT audit that rejects an unscoped
  PREROUTING DNAT rule.
- nginx-gaius-relay.conf: systemd TLS reverse proxy for only /tunnel,
  /relay-node/v1, /health, and /proxy/.
- nginx-gaius-relay-docker.conf: the same proxy locations pointing to the
  Docker loopback port 18080.
- verify-runtime.sh: read-only Linux audit for the listener, PID, cwd,
  running target-attestation code, health, and manifest. It supports both a
  systemd Node listener and a Docker-published listener whose docker-proxy cwd
  is `/`.

The registry lease write endpoint /relay-registry/v1/nodes/ is intentionally not
proxied by the Nginx example. If a registry is used, expose its read-only
manifest from a separate controlled service and keep management writes on a
private network.

## Prerequisites

Use a Linux host with systemd, Nginx, TLS certificates, `ss`, `curl`, and
Node.js 22 or newer. Docker deployment additionally requires Docker Engine,
`flock`, and `mktemp`. The service account and paths used by the templates are:

    user/group: gaius:gaius
    application: /opt/gaius/apps/bridge
    environment: /etc/gaius/relaynode.env
    state: /var/lib/gaius-relaynode
    listener: 127.0.0.1:8080

Create the account and directories once:

    sudo useradd --system --home-dir /opt/gaius --shell /usr/sbin/nologin gaius
    sudo install -d -o gaius -g gaius /opt/gaius/apps/bridge
    sudo install -d -o root -g gaius -m 0750 /etc/gaius

## Upload and install

Run the upload from a checked-out release on the operator machine. Replace the
SSH account and hostname with the deployment host; the commands below are
examples and do not contain credentials.

    rsync -a --delete --exclude node_modules/ --exclude '*.log' --exclude target/ apps/bridge/ deploy@relay.example.com:/opt/gaius/apps/bridge/
    scp apps/bridge/deploy/gaius-relaynode.service deploy@relay.example.com:/tmp/gaius-relaynode.service
    scp apps/bridge/deploy/env.example deploy@relay.example.com:/tmp/gaius-relaynode.env.example
    scp apps/bridge/deploy/nginx-gaius-relay.conf deploy@relay.example.com:/tmp/nginx-gaius-relay.conf
    scp apps/bridge/deploy/verify-runtime.sh deploy@relay.example.com:/opt/gaius/apps/bridge/verify-runtime.sh

On the Linux host, install the environment file as root, edit only the public
origin and node identity, and make it unreadable by other users:

    sudo install -o root -g gaius -m 0640 /tmp/gaius-relaynode.env.example /etc/gaius/relaynode.env
    sudoedit /etc/gaius/relaynode.env
    sudo chown -R gaius:gaius /opt/gaius/apps/bridge
    sudo chmod 0750 /opt/gaius/apps/bridge/verify-runtime.sh

Do not add GAIUS_BRIDGE_TOKEN to a public node. Browser clients cannot safely
receive a shared public secret. If registering with a private registry, add the
registry URL, node ID, public WSS URL, and a generated token only to the
root-owned environment file. Never commit those values.

## Install dependencies and validate

The RelayNode package is deployed with `package-lock.json` for reproducible
installation. Do not replace `npm ci` with an unlocked install on a production
public node.

    cd /opt/gaius/apps/bridge
    test -f package-lock.json
    npm ci --omit=dev
    node --version
    node --check dist/config.js
    node --check dist/policy.js
    node --check dist/main.js
    node --check dist/registry.js

Run the local protocol and routing checks before a release is uploaded:

    cd /opt/gaius/apps/bridge
    npm run smoke

## Single-container Docker deployment

This is the deployment shape for a host that runs one RelayNode container and
keeps the public TLS proxy outside Docker. The process listens on `0.0.0.0:8080`
inside the container; Docker publishes it only as `127.0.0.1:18080` on the host.
The host must not publish container port 8080 directly to the Internet.

On the deployment host, from `apps/bridge`:

    cp deploy/docker-single.env.example deploy/docker-single.env
    # Edit only the public origin, node identity, and non-secret limits.
    chmod 0640 deploy/docker-single.env
    chmod 0750 deploy/docker-single-deploy.sh deploy/docker-single-rollback.sh
    GAIUS_DOCKER_HOST_PORT=18080 deploy/docker-single-deploy.sh

The script builds the image from the lockfile-backed Dockerfile and digest-pinned
multi-architecture Node base image. It holds a host deployment lock, renames the
existing container to `gaius-relaynode-previous`, starts the new container, and
requires both `/health` and `/relay-node/v1` to report the expected RelayNode
kind, protocol version, and `target-attestation` capability. If either check
fails, it preserves the failed release under a timestamped container name and
restores and verifies the previous one. It never prints the environment file or
its values. Keep the previous container until the two-target connection
acceptance test has completed.

For a manual Compose start using the same loopback mapping:

    cp deploy/docker-single-compose.example.yaml deploy/docker-single-compose.yaml
    docker compose --env-file deploy/docker-single.env \
      -f deploy/docker-single-compose.yaml up -d --build

The deployment script is preferred for releases because it performs the
old-container backup and automatic rollback. The Compose file is a minimal
repeatable reference, not a replacement for the release gate.

Install `nginx-gaius-relay-docker.conf` for this mode and replace only the
example hostname and certificate paths. It proxies TLS/WSS and HTTP checks to
`127.0.0.1:18080`; do not use the systemd config unchanged, because that
config targets the systemd listener on port 8080.

To audit the Docker mode without exposing environment values:

    sudo GAIUS_VERIFY_RUNTIME=docker GAIUS_VERIFY_PORT=18080 \
      GAIUS_VERIFY_BASE_URL=http://127.0.0.1:18080 \
      bash deploy/verify-runtime.sh

The audit accepts a host `docker-proxy` listener with cwd `/`, then resolves
the single published container, checks its `/app` working directory and loaded
entrypoint, and checks health and manifest through the loopback port.

## NAT and Docker egress safety

Before accepting a public Docker RelayNode, inspect the host NAT table. Any
Minecraft inbound DNAT must match the public entry interface, for example
`-i eth0`; a DNAT rule without `-i` can also match the RelayNode's arbitrary
outbound connections during `PREROUTING` and redirect them to the local
Minecraft service. The check is read-only and does not assume a destination
address or a particular public interface:

    sudo bash deploy/verify-dnat.sh

The script is equivalent to reviewing the output of:

    sudo iptables-save -t nat
    sudo ip6tables-save -t nat

It scans every IPv4 and available IPv6 `PREROUTING` rule with `-j DNAT` and
fails if any lacks an explicit ingress-interface match. If IPv6 forwarding is
enabled but `ip6tables-save` is unavailable, it fails closed. A host with no
such rules passes this check. Do not add a broad PREROUTING DNAT rule to make
the RelayNode reachable; the public TLS proxy should remain the only
Internet-facing entry point.

The public smoke is a separate acceptance test because it requires a reachable
Java server and a deployed public URL. It must be run only against targets the
operator is authorized to test.

## systemd and Nginx

Install the unit, reload systemd, and restart the process after every release:

    sudo install -m 0644 /tmp/gaius-relaynode.service /etc/systemd/system/gaius-relaynode.service
    sudo systemctl daemon-reload
    sudo systemctl enable gaius-relaynode.service
    sudo systemctl restart gaius-relaynode.service
    sudo systemctl --no-pager --full status gaius-relaynode.service

Replace relay.example.com and the certificate paths in the Nginx example, then
install and validate it:

    sudo install -m 0644 /tmp/nginx-gaius-relay.conf /etc/nginx/conf.d/gaius-relay.conf
    sudo nginx -t
    sudo systemctl reload nginx

The map and limit_*_zone declarations in the example are intentionally at Nginx
http scope. A standard /etc/nginx/conf.d/*.conf include has that scope. The
final catch-all returns 404; no registry management endpoint is published.

## Non-sensitive runtime audit

The audit does not print the service environment. It identifies the one PID
owning the 8080 listener, checks /proc/$PID/cwd, checks that the loaded
dist/main.js contains target attestation, and requests health and manifest JSON
without echoing either response.

    sudo GAIUS_VERIFY_RUNTIME=systemd GAIUS_VERIFY_BASE_URL=https://relay.example.com GAIUS_VERIFY_WORKDIR=/opt/gaius/apps/bridge bash /opt/gaius/apps/bridge/verify-runtime.sh

For a local-only check before Nginx is configured, omit GAIUS_VERIFY_BASE_URL;
it defaults to http://127.0.0.1:8080.

For a non-sensitive systemd audit, inspect only these fields:

    sudo systemctl show gaius-relaynode.service -p MainPID -p ActiveState -p SubState
    pid=$(sudo systemctl show -p MainPID --value gaius-relaynode.service)
    sudo readlink -f "/proc/$pid/cwd"
    sudo ss -ltnp 'sport = :8080'

Do not print /proc/$pid/environ wholesale. If an environment audit is needed,
allowlist non-secret keys such as GAIUS_BRIDGE_HOST, GAIUS_BRIDGE_PORT,
GAIUS_ALLOWED_HOSTS, GAIUS_ALLOWED_ORIGINS, GAIUS_ALLOW_PRIVATE_TARGETS,
GAIUS_MAXIMUM_CONNECTIONS, GAIUS_MAXIMUM_FRAME_BYTES,
GAIUS_CONNECT_TIMEOUT_MS, and GAIUS_IDLE_TIMEOUT_MS; omit every *_TOKEN,
*_PASSWORD, and credential value.

## Target-attestation acceptance

The RelayNode must prove that its TCP connection is to the target requested by
the browser. Test two different, authorized public Java servers, not only the
RelayNode host. The returned JSON must show target, attestedTarget, status
version, and a server description belonging to the same target on both runs.

    cd /opt/gaius/apps/bridge
    GAIUS_PUBLIC_RELAY_URL=wss://relay.example.com/tunnel GAIUS_PUBLIC_RELAY_TARGET=server-a.example:25565 npm run smoke:public
    GAIUS_PUBLIC_RELAY_URL=wss://relay.example.com/tunnel GAIUS_PUBLIC_RELAY_TARGET=server-b.example:25565 npm run smoke:public

For each run, record the actual `connected` fields and the status fingerprint
before adding the node to the curated registry. The minimum two-target gate is:

- Target A and target B are different authorized Java servers.
- For a direct target, `connected.candidateHost` and
  `connected.candidatePort` match the requested host and port. When Minecraft
  SRV discovery applies, they instead match the selected SRV host and port.
  `connected.remoteAddress` and `connected.remotePort` show the final socket
  peer and must be consistent with that candidate.
- The candidate fields and the status fingerprint (`version`, description, and
  SHA-256 of the status payload) differ between the two runs as expected for
  the two servers.
- `targetConnections.released` is true after each run.

Run the public smoke twice, capturing its machine-readable result without
printing credentials:

    GAIUS_PUBLIC_RELAY_URL=wss://relay.example.com/tunnel GAIUS_PUBLIC_RELAY_TARGET=server-a.example:25565 npm run smoke:public
    GAIUS_PUBLIC_RELAY_URL=wss://relay.example.com/tunnel GAIUS_PUBLIC_RELAY_TARGET=server-b.example:25565 npm run smoke:public

A node that reports the same candidate or status for both targets is misrouted
and must be removed from service until its running process and upstream proxy
are corrected.

Do not treat a `connected` message that merely repeats the requested host and
port as proof of a real TCP connection. The current release requirement is
that the `connected` control message includes the selected `candidateHost` and
`candidatePort`, plus the socket's actual `remoteAddress` and `remotePort`.
The candidate must match either the direct request or a valid SRV result for
that request, and the remote endpoint must be consistent with the candidate's
resolved address. If a deployed client or node does not publish these fields,
it is an older incompatible release and must be updated before it is accepted
into the public registry.

## Rollback

Keep the previous dist/, package-lock.json, and environment file outside the
active release directory. To roll back, upload the previous application bundle,
restore the previous environment file if it changed, reinstall its locked
dependencies, validate syntax, then restart the same unit:

    cd /opt/gaius/apps/bridge
    npm ci --omit=dev
    node --check dist/main.js
    sudo systemctl restart gaius-relaynode.service
    sudo nginx -t
    sudo systemctl reload nginx

Run verify-runtime.sh and the two-target attestation smoke after rollback. If
the old release lacks target-attestation, do not return it to a public registry;
use it only behind a restricted private deployment while the current release is
repaired.
