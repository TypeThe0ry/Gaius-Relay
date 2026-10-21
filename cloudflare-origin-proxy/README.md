# Relay origin proxy

This Cloudflare Worker is an optional example origin proxy. Replace the
example upstream, route, and allowed origin values through an operator-owned
deployment overlay before publishing it.

The proxy is intentionally narrow: only `/tunnel`, the RelayNode manifests,
and the browser's exact `/proxy/resource-pack` endpoint are forwarded. The
legacy `/resource-pack/*` paths remain allowed. Unknown paths and origins are
rejected at the edge.

Run the local contract test with:

```sh
node cloudflare-origin-proxy/worker.test.mjs
```

After deployment and DNS setup, verify the public Pages origin and a resource
pack request with `deploy/verify-public-origin.sh` using
`GAIUS_VERIFY_BASE_URL=https://relay-proxy.example.com`. The local test and
Wrangler dry-run do not prove that the public route has been deployed.
