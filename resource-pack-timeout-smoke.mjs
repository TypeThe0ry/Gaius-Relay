import assert from "node:assert/strict";
import {createServer} from "node:http";
import {spawn} from "node:child_process";
import {fileURLToPath} from "node:url";
import {tmpdir} from "node:os";
import {readdir} from "node:fs/promises";

const host = "127.0.0.1";
const origin = "http://127.0.0.1:8781";
const token = "timeout-runtime-generated-token-1234";
const reserve = () => new Promise((resolve, reject) => {
  const server = createServer();
  server.once("error", reject);
  server.listen(0, host, () => resolve({server, port: server.address().port}));
});
const fixture = await reserve();
const bridgePort = await reserve();
await new Promise((resolve) => bridgePort.server.close(resolve));
let retrySlowAttempts = 0;
const fixtureTimers = new Set();
const later = (fn, delay) => {
  const timer = setTimeout(() => { fixtureTimers.delete(timer); fn(); }, delay);
  fixtureTimers.add(timer);
};
fixture.server.on("request", (_request, response) => {
  const requestUrl = new URL(_request.url, `http://${host}`);
  if (requestUrl.pathname === "/retry-slow.zip") {
    retrySlowAttempts++;
    response.writeHead(200, {"content-length": "1000"});
    response.write("a");
    if (retrySlowAttempts === 1) {
      later(() => response.destroy(), 1500);
      later(() => response.write("b"), 500);
      later(() => response.write("c"), 1000);
      return;
    }
    const trickle = () => {
      if (response.destroyed) return;
      response.write("b");
      later(trickle, 400);
    };
    later(trickle, 400);
    return;
  }
  if (requestUrl.pathname === "/idle.zip") {
    response.writeHead(200, {"content-length": "4"});
    response.write("a");
    return;
  }
  later(() => { if (!response.destroyed) response.writeHead(200, {"content-length": "4"}); }, 5000);
});
const bridge = spawn(process.execPath, ["dist/main.js"], {
  cwd: fileURLToPath(new URL(".", import.meta.url)),
  env: {...process.env, NODE_ENV: "test", GAIUS_BRIDGE_HOST: host,
    GAIUS_BRIDGE_PORT: String(bridgePort.port), GAIUS_ALLOWED_ORIGINS: origin,
    GAIUS_ALLOWED_RESOURCE_PACK_HOSTS: host, GAIUS_BRIDGE_TOKEN: token,
    GAIUS_RESOURCE_PACK_HEADERS_TIMEOUT_MS: "1000",
    GAIUS_RESOURCE_PACK_BODY_IDLE_TIMEOUT_MS: "1000",
    GAIUS_RESOURCE_PACK_OVERALL_TIMEOUT_MS: "5000"},
  stdio: ["ignore", "pipe", "pipe"],
});
let output = "";
bridge.stdout.on("data", (chunk) => { output += chunk; });
bridge.stderr.on("data", (chunk) => { output += chunk; });
try {
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      clearInterval(poll);
      reject(new Error(`bridge startup timeout: ${output}`));
    }, 5000);
    const poll = setInterval(() => {
      if (!output.includes("Gaius translator node listening")) return;
      clearTimeout(timer);
      clearInterval(poll);
      resolve();
    }, 20);
  });
  const url = new URL(`http://${host}:${bridgePort.port}/proxy/resource-pack`);
  url.searchParams.set("url", `http://${host}:${fixture.port}/pack.zip`);
  url.searchParams.set("token", token);
  const response = await fetch(url, {headers: {origin}});
  assert.equal(response.status, 504);
  assert.equal(response.headers.get("access-control-allow-origin"), origin);
  const retryUrl = new URL(url);
  retryUrl.searchParams.set("url", `http://${host}:${fixture.port}/retry-slow.zip`);
  const started = Date.now();
  const retryResponse = await fetch(retryUrl, {headers: {origin}});
  assert.equal(retryResponse.status, 504);
  assert.equal(retryResponse.headers.get("access-control-allow-origin"), origin);
  assert.ok(retrySlowAttempts >= 2, `truncated upstream was not retried: ${retrySlowAttempts}`);
  const elapsed = Date.now() - started;
  assert.ok(elapsed >= 4500 && elapsed < 6200,
    `overall deadline must cover both attempts, elapsed=${elapsed}`);
  const idleUrl = new URL(url);
  idleUrl.searchParams.set("url", `http://${host}:${fixture.port}/idle.zip`);
  const idleResponse = await fetch(idleUrl, {headers: {origin}});
  assert.equal(idleResponse.status, 504);
  assert.equal(idleResponse.headers.get("access-control-allow-origin"), origin);
  // Other Relay smoke tests may run concurrently in the shared OS temp directory.
  // Assert cleanup for this child only; all files from this fresh PID belong to this test.
  const tempAfter = (await readdir(tmpdir())).filter((name) =>
    name.startsWith(`gaius-relay-resource-pack-${bridge.pid}-`));
  assert.deepEqual(tempAfter, [],
    "timeout paths leaked a resource-pack temporary file");
  console.log("resource-pack-timeout-smoke: PASS");
} finally {
  bridge.kill();
  for (const timer of fixtureTimers) clearTimeout(timer);
  fixture.server.closeAllConnections();
  await new Promise((resolve) => fixture.server.close(resolve));
}
