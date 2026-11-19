// Verifies target prewarm: dial before first client byte, immediate reuse, per-client limit,
// idle close of an unused prewarm, and that normal connects are unaffected.
// Usage: node target-prewarm-smoke.mjs [relayDir] [relayPort]
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import path from "node:path";
import { WebSocket } from "./node_modules/ws/wrapper.mjs";

const directory = path.resolve(process.argv[2] ?? ".");
const port = Number(process.argv[3] ?? 8794);
const host = "127.0.0.1";
const origin = "http://127.0.0.1:8781";
const fail = (label) => { throw new Error(`target-prewarm smoke failed: ${label}`); };
const assert = (c, l) => { if (!c) fail(l); };

let dials = 0;
const fixture = createServer((socket) => {
    dials++;
    socket.setNoDelay(true);
    socket.on("data", (d) => socket.write(d));
    socket.on("error", () => socket.destroy());
});
fixture.listen(0, host);
await once(fixture, "listening");
const fixturePort = fixture.address().port;

const child = spawn(process.execPath, ["dist/main.js"], {
    cwd: directory,
    env: { ...process.env, NODE_ENV: "test", GAIUS_BRIDGE_HOST: host, GAIUS_BRIDGE_PORT: String(port),
        GAIUS_ALLOWED_ORIGINS: origin, GAIUS_ALLOWED_HOSTS: host, GAIUS_IDLE_TIMEOUT_MS: "60000",
        RELAY_PREWARM_IDLE_MS: "1500" },
    stdio: ["ignore", "pipe", "pipe"],
});
let output = "";
child.stdout.setEncoding("utf8"); child.stderr.setEncoding("utf8");
child.stdout.on("data", (d) => { output += d; }); child.stderr.on("data", (d) => { output += d; });
for (let i = 0; i < 400 && !output.includes("Gaius translator node listening"); i++) await delay(25);
assert(output.includes("Gaius translator node listening"), "relay startup");

function open(prewarm) {
    const ws = new WebSocket(`ws://${host}:${port}/tunnel`, { headers: { Origin: origin } });
    const state = { ws, msgs: [], closed: undefined, bytes: [] };
    ws.on("message", (d, bin) => { if (bin) state.bytes.push(Buffer.from(d)); else state.msgs.push(JSON.parse(d.toString())); });
    ws.on("close", (code) => { state.closed = code; });
    ws.on("error", () => {});
    state.ready = once(ws, "open").then(() => ws.send(JSON.stringify({ type: "connect", host, port: fixturePort, ...(prewarm ? { prewarm: true } : {}) })));
    return state;
}
const waitFor = async (fn, label, ms = 3000) => { const t = Date.now(); while (!fn()) { if (Date.now() - t > ms) fail(label); await delay(10); } };

try {
    const health = await (await fetch(`http://${host}:${port}/health`)).json();
    assert(health.capabilities?.includes("target-prewarm"), "health advertises target-prewarm");

    // 1: prewarm dials before any client byte, then echoes immediately.
    const a = open(true); await a.ready;
    await waitFor(() => a.msgs.some((m) => m.type === "connected"), "prewarm connected");
    assert(dials === 1, "target dialed during prewarm");
    await delay(300);
    assert(a.closed === undefined, "prewarm survives short idle");
    a.ws.send(Buffer.from("hello"));
    await waitFor(() => Buffer.concat(a.bytes).toString() === "hello", "echo after prewarm");
    await delay(1800);
    assert(a.closed === undefined, "used prewarm not closed by prewarm idle");

    // 2: second prewarm from same client is refused while first is open.
    const b = open(true); await b.ready;
    await waitFor(() => b.closed !== undefined, "second prewarm refused");
    assert(b.closed === 1013, `second prewarm closed with 1013 (got ${b.closed})`);

    // 3: normal connect still works alongside.
    const c = open(false); await c.ready;
    await waitFor(() => c.msgs.some((m) => m.type === "connected"), "normal connect");
    c.ws.send(Buffer.from("x"));
    await waitFor(() => Buffer.concat(c.bytes).toString() === "x", "normal echo");
    c.ws.close(); a.ws.close();
    await delay(200);

    // 4: unused prewarm closes after idle, and slot is released.
    const d = open(true); await d.ready;
    await waitFor(() => d.msgs.some((m) => m.type === "connected"), "prewarm d connected");
    await waitFor(() => d.closed !== undefined, "unused prewarm closed", 4000);
    assert(d.closed === 1001, `idle prewarm closed with 1001 (got ${d.closed})`);
    const e = open(true); await e.ready;
    await waitFor(() => e.msgs.some((m) => m.type === "connected"), "slot released after close");
    e.ws.close();

    // 5: invalid flag type rejected.
    const f = new WebSocket(`ws://${host}:${port}/tunnel`, { headers: { Origin: origin } });
    f.on("error", () => {});
    let fc; f.on("close", (code) => { fc = code; });
    await once(f, "open");
    f.send(JSON.stringify({ type: "connect", host, port: fixturePort, prewarm: "yes" }));
    await waitFor(() => fc !== undefined, "bad prewarm closed");
    assert(fc === 1007, `bad prewarm 1007 (got ${fc})`);
    console.log("target-prewarm smoke passed");
} finally {
    child.kill();
    fixture.close();
}
