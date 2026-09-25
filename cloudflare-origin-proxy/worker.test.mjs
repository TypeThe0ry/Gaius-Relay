import assert from "node:assert/strict";

import { proxyRequest } from "./worker.js";

let upstreamRequest = null;
const upstreamFetch = async (request) => {
  upstreamRequest = request;
  return new Response(JSON.stringify({ ok: true }), {
    headers: { "Content-Type": "application/json" },
  });
};

const pagesManifest = await proxyRequest(new Request(
  "https://pages-relay.example.com/relay-node/v1",
  { headers: { Origin: "https://example.com" } },
), upstreamFetch);
assert.equal(pagesManifest.status, 200);
assert.equal(pagesManifest.headers.get("Access-Control-Allow-Origin"),
  "https://example.com");
assert.equal(pagesManifest.headers.get("X-Gaius-Relay-Proxy"), "pages-origin-v1");
assert.equal(upstreamRequest.url, "https://example.com/relay-node/v1");
assert.equal(upstreamRequest.headers.get("Origin"), "null");

const pagesTunnel = await proxyRequest(new Request(
  "https://pages-relay.example.com/tunnel",
  {
    headers: {
      Origin: "https://example.com",
      Upgrade: "websocket",
    },
  },
), upstreamFetch);
assert.equal(pagesTunnel.status, 200);
assert.equal(upstreamRequest.url, "https://example.com/tunnel");
assert.equal(upstreamRequest.headers.get("Origin"), "null");
assert.equal(upstreamRequest.headers.get("Upgrade"), "websocket");

const pagesResourcePack = await proxyRequest(new Request(
  "https://pages-relay.example.com/proxy/resource-pack?url=https%3A%2F%2Fexample.test%2Fpack.zip&stream=1",
  {
    headers: {
      Origin: "https://example.com",
      "X-Gaius-Resource-Pack": "1",
      Range: "bytes=0-15",
    },
  },
), async (request) => {
  upstreamRequest = request;
  return new Response("pack-bytes", {
    status: 206,
    headers: { "Content-Length": "10", "Content-Range": "bytes 0-9/10" },
  });
});
assert.equal(pagesResourcePack.status, 206);
assert.equal(pagesResourcePack.headers.get("Access-Control-Allow-Origin"),
  "https://example.com");
assert.equal(pagesResourcePack.headers.get("X-Gaius-Relay-Proxy"), "pages-origin-v1");
assert.equal(upstreamRequest.url,
  "https://example.com/proxy/resource-pack?url=https%3A%2F%2Fexample.test%2Fpack.zip&stream=1");
assert.equal(upstreamRequest.headers.get("Origin"), "null");
assert.equal(upstreamRequest.headers.get("Range"), "bytes=0-15");
assert.equal(upstreamRequest.headers.get("X-Gaius-Resource-Pack"), "1");

const fileTunnel = await proxyRequest(new Request(
  "https://pages-relay.example.com/tunnel",
  { headers: { Origin: "null", Upgrade: "websocket" } },
), upstreamFetch);
assert.equal(fileTunnel.status, 200);

const rejected = await proxyRequest(new Request(
  "https://pages-relay.example.com/tunnel",
  { headers: { Origin: "https://attacker.example", Upgrade: "websocket" } },
), upstreamFetch);
assert.equal(rejected.status, 403);

const missing = await proxyRequest(new Request(
  "https://pages-relay.example.com/private",
), upstreamFetch);
assert.equal(missing.status, 404);

const preflight = await proxyRequest(new Request(
  "https://pages-relay.example.com/relay-node/v1",
  { method: "OPTIONS", headers: { Origin: "https://example.com" } },
), upstreamFetch);
assert.equal(preflight.status, 204);
assert.equal(preflight.headers.get("Access-Control-Allow-Origin"),
  "https://example.com");

console.log("Cloudflare Pages Relay origin proxy tests passed");
