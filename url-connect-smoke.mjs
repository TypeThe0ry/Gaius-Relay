// Verifies the optional single-round-trip start (`/tunnel?c=<base64url connect JSON>`)
// and measures join latency against the classic control-message path, with and
// without simulated network delay (a delaying TCP proxy in front of the relay).
// Usage: node url-connect-smoke.mjs [relayDir] [relayPort] [oneWayDelayMs]
import { spawn } from "node:child_process";
import { createServer, connect } from "node:net";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { WebSocket } from "./node_modules/ws/wrapper.mjs";
import { ephemeralCredential } from "./test-support.mjs";

const directory = path.resolve(process.argv[2] ?? ".");
const port = Number(process.argv[3] ?? 8793);
const oneWay = Number(process.argv[4] ?? 40);
const host = "127.0.0.1";
const origin = "http://127.0.0.1:8781";
const samples = 15;

function assert(condition, label) {
    if (!condition) throw new Error(`url-connect smoke failed: ${label}`);
}

const fixture = createServer((socket) => {
    socket.setNoDelay(true);
    socket.on("data", (data) => socket.write(data));
    socket.on("error", () => socket.destroy());
});
fixture.listen(0, host);
await once(fixture, "listening");
const fixturePort = fixture.address().port;

async function startRelay(extraEnv, relayPort) {
    const child = spawn(process.execPath, ["dist/main.js"], {
        cwd: directory,
        env: {
            ...process.env, NODE_ENV: "test",
            GAIUS_BRIDGE_HOST: host, GAIUS_BRIDGE_PORT: String(relayPort),
            GAIUS_ALLOWED_ORIGINS: origin, GAIUS_ALLOWED_HOSTS: host,
            GAIUS_IDLE_TIMEOUT_MS: "60000", ...extraEnv,
        },
        stdio: ["ignore", "pipe", "pipe"],
    });
    let output = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (d) => { output += d; });
    child.stderr.on("data", (d) => { output += d; });
    for (let i = 0; i < 400 && !output.includes("Gaius translator node listening"); i++) {
        await delay(25);
    }
    assert(output.includes("Gaius translator node listening"), "relay startup");
    return child;
}

// Delaying proxy: adds `oneWay` ms to every chunk in both directions.
function delayProxy(listenPort, targetPort, ms) {
    const server = createServer((client) => {
        const upstream = connect(targetPort, host);
        const pipe = (from, to) => {
            from.on("data", (chunk) => setTimeout(() => { if (!to.destroyed) to.write(chunk); }, ms));
            from.on("close", () => setTimeout(() => to.destroy(), ms));
            from.on("error", () => to.destroy());
        };
        client.setNoDelay(true);
        upstream.setNoDelay(true);
        pipe(client, upstream);
        pipe(upstream, client);
    });
    server.listen(listenPort, host);
    return server;
}

function connectText(extra = {}) {
    return JSON.stringify({ type: "connect", host, port: fixturePort, ...extra });
}
function embeddedUrl(base, text) {
    return `${base}/tunnel?c=${Buffer.from(text, "utf8").toString("base64url")}`;
}

async function join(url, { sendControl }) {
    const startedAt = performance.now();
    const webSocket = new WebSocket(url, { headers: { origin } });
    const connected = new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("connected timeout")), 10000);
        webSocket.on("message", (data, isBinary) => {
            if (!isBinary && data.toString().includes('"type":"connected"')) {
                clearTimeout(timer);
                resolve();
            }
        });
        webSocket.on("close", (code, reason) => {
            clearTimeout(timer);
            reject(new Error(`closed ${code} ${reason}`));
        });
    });
    await once(webSocket, "open");
    if (sendControl) webSocket.send(connectText());
    await connected;
    const elapsed = performance.now() - startedAt;
    // Echo proves the TCP leg is live and ordered.
    const echo = new Promise((resolve) => webSocket.once("message", (d) => resolve(d)));
    webSocket.send(Buffer.from("ping"));
    assert(Buffer.from(await echo).toString() === "ping", "echo");
    webSocket.close();
    return elapsed;
}

const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];

// 1. Tokenless relay: URL path works, control path still works, both fast.
const relay = await startRelay({}, port);
const proxyPort = port + 1;
const proxy = delayProxy(proxyPort, port, oneWay);
try {
    const health = await (await fetch(`http://${host}:${port}/health`)).json()
        .catch(() => null);
    if (health) {
        assert(Array.isArray(health.capabilities) && health.capabilities.includes("url-connect"),
            "health advertises url-connect");
    }
    const base = `ws://${host}:${proxyPort}`;
    await join(embeddedUrl(base, connectText()), { sendControl: false });
    await join(`${base}/tunnel`, { sendControl: true });
    const control = [];
    const embedded = [];
    for (let i = 0; i < samples; i++) {
        control.push(await join(`${base}/tunnel`, { sendControl: true }));
        embedded.push(await join(embeddedUrl(base, connectText()), { sendControl: false }));
    }
    // Malformed / oversized / disallowed `c` values must fall back to the control path.
    await join(`${base}/tunnel?c=!!!`, { sendControl: true });
    await join(`${base}/tunnel?c=${"A".repeat(4000)}`, { sendControl: true });
    // A disallowed target in `c` is rejected by the same host policy as the control path.
    const rejected = await join(embeddedUrl(base,
        JSON.stringify({ type: "connect", host: "10.0.0.1", port: fixturePort })),
        { sendControl: false }).then(() => false, () => true);
    assert(rejected, "disallowed embedded target is rejected");
    console.log(JSON.stringify({
        oneWayDelayMs: oneWay,
        controlPathMedianMs: Number(median(control).toFixed(1)),
        urlPathMedianMs: Number(median(embedded).toFixed(1)),
        savedMs: Number((median(control) - median(embedded)).toFixed(1)),
    }, null, 2));
} finally {
    proxy.close();
    relay.kill();
}

// 2. Token-protected relay: `c` must be ignored (credentials stay out of URLs).
const tokenRelayPort = port + 2;
const token = ephemeralCredential("url-connect");
const tokenRelay = await startRelay({ GAIUS_BRIDGE_TOKEN: token }, tokenRelayPort);
try {
    const webSocket = new WebSocket(
        embeddedUrl(`ws://${host}:${tokenRelayPort}`, connectText()), { headers: { origin } });
    await once(webSocket, "open");
    const closed = new Promise((resolve) => webSocket.on("close", (code) => resolve(code)));
    // Relay must still wait for a tokened control message; sending none keeps it idle.
    webSocket.send(connectText({ token }));
    const connected = await new Promise((resolve) => {
        webSocket.on("message", (d) => { if (d.toString().includes('"connected"')) resolve(true); });
        setTimeout(() => resolve(false), 3000);
    });
    assert(connected, "token relay still accepts control-message connect");
    webSocket.close();
    await closed;
} finally {
    tokenRelay.kill();
    fixture.close();
}
console.log("url-connect smoke ok");
