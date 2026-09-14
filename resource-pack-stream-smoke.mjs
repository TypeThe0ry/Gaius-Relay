import assert from "node:assert/strict";
import {createServer, get} from "node:http";
import {spawn} from "node:child_process";
import {fileURLToPath} from "node:url";
import {tmpdir} from "node:os";
import {readdir} from "node:fs/promises";
import {setTimeout as delay} from "node:timers/promises";

const host = "127.0.0.1";
const origin = "http://127.0.0.1:8781";
const token = "stream-runtime-generated-token-1234";
const listen = async (server) => {
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, host, resolve);
  });
  return server.address().port;
};
const hits = new Map();
const timers = new Set();
let finishGated;
let upstreamClosed = 0;
let decoupledUpstreamFinished = false;
const bytes = Buffer.from([80, 75, 3, 4, 0, 255, 27, 10, 99]);
const fixture = createServer((request, response) => {
  const path = new URL(request.url, `http://${host}`).pathname;
  hits.set(path, (hits.get(path) ?? 0) + 1);
  if (path === "/large.zip") {
    response.writeHead(200, {"content-length": String(251 * 1024 * 1024)});
    response.flushHeaders();
    return;
  }
  if (path === "/gated.zip") {
    response.writeHead(200, {"content-length": String(bytes.length)});
    response.write(bytes.subarray(0, 4));
    finishGated = () => response.end(bytes.subarray(4));
    return;
  }
  if (path === "/cache.zip") {
    response.end(bytes);
    return;
  }
  if (path === "/backpressure.zip") {
    response.on("close", () => upstreamClosed++);
    response.writeHead(200, {"content-length": String(64 * 1024 * 1024)});
    const chunk = Buffer.alloc(64 * 1024, 0x50);
    let remaining = 1024;
    const pump = () => {
      while (remaining > 0 && !response.destroyed) {
        remaining--;
        if (!response.write(chunk)) {
          response.once("drain", pump);
          return;
        }
      }
      if (!response.destroyed) response.end();
    };
    pump();
    return;
  }
  if (path === "/decoupled.zip") {
    const total = 4 * 1024 * 1024;
    response.writeHead(200, {"content-length": String(total)});
    const chunk = Buffer.alloc(64 * 1024, 0x44);
    let remaining = total / chunk.byteLength;
    const pump = () => {
      while (remaining > 0 && !response.destroyed) {
        remaining--;
        if (!response.write(chunk)) {
          response.once("drain", pump);
          return;
        }
      }
      if (!response.destroyed) {
        decoupledUpstreamFinished = true;
        response.end();
      }
    };
    pump();
    return;
  }
  response.on("close", () => upstreamClosed++);
  response.writeHead(200, {"content-length": "1000000"});
  response.write(bytes);
  if (path === "/truncated.zip") {
    const timer = setTimeout(() => response.destroy(), 100);
    timers.add(timer);
  } else if (path === "/overall.zip") {
    const timer = setInterval(() => {
      if (!response.destroyed) response.write(bytes);
    }, 200);
    timers.add(timer);
    response.on("close", () => clearInterval(timer));
  }
});
const fixturePort = await listen(fixture);
const reservation = createServer();
const port = await listen(reservation);
await new Promise((resolve) => reservation.close(resolve));
const bridge = spawn(process.execPath, ["dist/main.js"], {
  cwd: fileURLToPath(new URL(".", import.meta.url)),
  env: {...process.env, NODE_ENV: "test", GAIUS_BRIDGE_HOST: host,
    GAIUS_BRIDGE_PORT: String(port), GAIUS_ALLOWED_ORIGINS: origin,
    GAIUS_ALLOWED_RESOURCE_PACK_HOSTS: host, GAIUS_BRIDGE_TOKEN: token,
    GAIUS_RESOURCE_PACK_HEADERS_TIMEOUT_MS: "1000",
    GAIUS_RESOURCE_PACK_BODY_IDLE_TIMEOUT_MS: "1000",
    GAIUS_RESOURCE_PACK_STREAM_OVERALL_TIMEOUT_MS: "5000",
    GAIUS_RESOURCE_PACK_CACHE_MS: "60000"},
  stdio: ["pipe", "pipe", "pipe"],
});
let output = "";
bridge.stdout.on("data", (chunk) => { output += chunk; });
bridge.stderr.on("data", (chunk) => { output += chunk; });
const url = (path, stream = true) => {
  const value = new URL(`http://${host}:${port}/proxy/resource-pack`);
  value.searchParams.set("url", `http://${host}:${fixturePort}${path}`);
  value.searchParams.set("token", token);
  if (stream) value.searchParams.set("stream", "1");
  return value;
};
const request = (path, options = {}) => fetch(url(path), {
  signal: AbortSignal.timeout(8000), headers: {origin}, ...options,
});
const tempFiles = async () => (await readdir(tmpdir()))
  .filter((name) => name.startsWith(`gaius-relay-resource-pack-${bridge.pid}-`));
try {
  const deadline = Date.now() + 5000;
  while (!output.includes("Gaius translator node listening")) {
    assert.ok(Date.now() < deadline, `bridge startup failed: ${output}`);
    await delay(20);
  }

  // The upstream cannot finish until this client receives the first bytes.
  // A spool-before-response implementation deterministically deadlocks here.
  const response = await request("/gated.zip", {signal: AbortSignal.timeout(2000)});
  assert.equal(response.status, 200);
  assert.equal(response.headers.get("access-control-allow-origin"), origin);
  const reader = response.body.getReader();
  const first = await reader.read();
  assert.deepEqual(Buffer.from(first.value), bytes.subarray(0, 4));
  finishGated();
  const chunks = [Buffer.from(first.value)];
  for (;;) {
    const part = await reader.read();
    if (part.done) break;
    chunks.push(Buffer.from(part.value));
  }
  assert.deepEqual(Buffer.concat(chunks), bytes);
  const cached = await fetch(url("/gated.zip", false), {headers: {origin}});
  assert.deepEqual(Buffer.from(await cached.arrayBuffer()), bytes);
  assert.equal(hits.get("/gated.zip"), 1, "completed stream must populate the shared cache");

  for (const username of ["Alice", "Bob", "Alice"]) {
    const result = await request("/cache.zip", {headers: {origin, "x-minecraft-username": username}});
    assert.deepEqual(Buffer.from(await result.arrayBuffer()), bytes);
  }
  assert.equal(hits.get("/cache.zip"), 2, "cache identity must retain forwarded user headers");
  let completedFiles = (await tempFiles()).sort();
  assert.equal(completedFiles.length, 3);

  // The live sender must not serialize every upstream read behind a paused
  // downstream drain. This body fits inside the bounded fan-out window, so it
  // should finish and enter the shared cache while the first client is paused.
  const paused = get(url("/decoupled.zip"), {headers: {origin}}, (body) => body.pause());
  paused.on("error", () => {});
  try {
    const decoupledDeadline = Date.now() + 2000;
    while (!decoupledUpstreamFinished) {
      assert.ok(Date.now() < decoupledDeadline,
        "slow downstream serialized and stalled the upstream resource-pack spool");
      await delay(20);
    }
    const cachedWhilePaused = await fetch(url("/decoupled.zip", false), {headers: {origin}});
    assert.equal((await cachedWhilePaused.arrayBuffer()).byteLength, 4 * 1024 * 1024);
    assert.equal(hits.get("/decoupled.zip"), 1,
      "completed upstream spool must be cache-visible while the original sender is paused");
  } finally {
    paused.destroy();
  }
  completedFiles = (await tempFiles()).sort();
  assert.equal(completedFiles.length, 4);

  for (let attempt = 0; attempt < 2; attempt++) {
    const broken = await request("/truncated.zip");
    await assert.rejects(() => broken.arrayBuffer());
  }
  assert.equal(hits.get("/truncated.zip"), 2, "partial ZIP must neither retry in-place nor enter cache");

  const idle = await request("/idle.zip");
  await assert.rejects(() => idle.arrayBuffer());
  const started = Date.now();
  const overall = await request("/overall.zip");
  await assert.rejects(() => overall.arrayBuffer());
  assert.ok(Date.now() - started >= 4500 && Date.now() - started < 7000,
    "overall deadline must stop a continuously progressing stream");

  const abort = new AbortController();
  const canceled = await request("/cancel.zip", {signal: abort.signal});
  const canceledReader = canceled.body.getReader();
  await canceledReader.read();
  const closedBefore = upstreamClosed;
  abort.abort();
  await assert.rejects(() => canceledReader.read());
  const cleanupDeadline = Date.now() + 1500;
  while (upstreamClosed <= closedBefore ||
      JSON.stringify((await tempFiles()).sort()) !== JSON.stringify(completedFiles)) {
    assert.ok(Date.now() < cleanupDeadline, "disconnect/failed stream leaked upstream or temporary file");
    await delay(20);
  }
  const oversized = await request("/large.zip");
  assert.equal(oversized.status, 413);
  assert.equal(oversized.headers.get("access-control-allow-origin"), origin);
  await oversized.text();
  assert.deepEqual((await tempFiles()).sort(), completedFiles);
  const pressureClosedBefore = upstreamClosed;
  const stalledRequest = get(url("/backpressure.zip"), {headers: {origin}}, (body) => body.pause());
  stalledRequest.on("error", () => {});
  try {
    const deadline = Date.now() + 7000;
    while (upstreamClosed <= pressureClosedBefore) {
      assert.ok(Date.now() < deadline, "timeout must release a downstream stalled on drain");
      await delay(20);
    }
    const pressureFiles = (await tempFiles()).sort();
    assert.ok(pressureFiles.length === completedFiles.length ||
      pressureFiles.length === completedFiles.length + 1,
    "stalled downstream left an unbounded or duplicate resource-pack spool");
    if (pressureFiles.length === completedFiles.length + 1) {
      const cachedPressure = await fetch(url("/backpressure.zip", false), {headers: {origin}});
      assert.equal((await cachedPressure.arrayBuffer()).byteLength, 64 * 1024 * 1024);
      assert.equal(hits.get("/backpressure.zip"), 1,
        "a fully spooled body should remain cache-visible after the slow sender times out");
    }
  } finally {
    stalledRequest.destroy();
  }
  console.log("resource-pack-stream-smoke: PASS (early bytes, exact body, shared/scoped cache, truncation, idle/overall deadlines, cancel cleanup, size guard, downstream backpressure)");
} finally {
  if (bridge.exitCode === null) {
    bridge.stdin?.write("graceful-shutdown\n");
    const exited = new Promise((resolve) => bridge.once("exit", resolve));
    await Promise.race([exited, delay(3500)]);
    if (bridge.exitCode === null) bridge.kill();
  }
  for (const timer of timers) clearTimeout(timer);
  fixture.closeAllConnections();
  await new Promise((resolve) => fixture.close(resolve));
}
