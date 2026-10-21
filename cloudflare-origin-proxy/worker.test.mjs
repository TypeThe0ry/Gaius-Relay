import assert from "node:assert/strict";

import {proxyRequest} from "./worker.js";

let upstreamRequest = null;
const upstreamFetch = async (request) => {
  upstreamRequest = request;
  return new Response(JSON.stringify({ok: true}), {
    headers: {"Content-Type": "application/json"},
  });
};

const origin = "https://play.example.com";
const proxy = "https://relay-proxy.example.com";
const upstream = "https://relay.example.com";

const manifest = await proxyRequest(new Request(
  `${proxy}/relay-node/v1`, {headers: {Origin: origin}},
), upstreamFetch);
assert.equal(manifest.status, 200);
assert.equal(manifest.headers.get("Access-Control-Allow-Origin"), origin);
assert.equal(manifest.headers.get("X-Gaius-Relay-Proxy"), "pages-origin-v1");
assert.equal(upstreamRequest.url, `${upstream}/relay-node/v1`);
assert.equal(upstreamRequest.headers.get("Origin"), "null");

const tunnel = await proxyRequest(new Request(
  `${proxy}/tunnel`,
  {headers: {Origin: origin, Upgrade: "websocket"}},
), upstreamFetch);
assert.equal(tunnel.status, 200);
assert.equal(upstreamRequest.url, `${upstream}/tunnel`);
assert.equal(upstreamRequest.headers.get("Upgrade"), "websocket");

const packUrl = `${proxy}/proxy/resource-pack?url=https%3A%2F%2Fcdn.example.test%2Fpack.zip&stream=1`;
const pack = await proxyRequest(new Request(packUrl, {
  headers: {Origin: origin, "X-Gaius-Resource-Pack": "1", Range: "bytes=0-15"},
}), async (request) => {
  upstreamRequest = request;
  return new Response("pack-bytes", {
    status: 206,
    headers: {"Content-Length": "10", "Content-Range": "bytes 0-9/10"},
  });
});
assert.equal(pack.status, 206);
assert.equal(upstreamRequest.url, `${upstream}/proxy/resource-pack?url=https%3A%2F%2Fcdn.example.test%2Fpack.zip&stream=1`);
assert.equal(upstreamRequest.headers.get("Range"), "bytes=0-15");

assert.equal((await proxyRequest(new Request(
  `${proxy}/tunnel`, {headers: {Origin: "null", Upgrade: "websocket"}},
), upstreamFetch)).status, 200);
assert.equal((await proxyRequest(new Request(
  `${proxy}/tunnel`, {headers: {Origin: "https://attacker.example", Upgrade: "websocket"}},
), upstreamFetch)).status, 403);
assert.equal((await proxyRequest(new Request(`${proxy}/private`), upstreamFetch)).status, 404);

const preflight = await proxyRequest(new Request(
  `${proxy}/relay-node/v1`, {method: "OPTIONS", headers: {Origin: origin}},
), upstreamFetch);
assert.equal(preflight.status, 204);
assert.equal(preflight.headers.get("Access-Control-Allow-Origin"), origin);

console.log("Relay origin proxy tests passed");
