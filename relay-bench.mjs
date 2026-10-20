// RelayNode benchmark: pumps a fixed payload through the WS<->TCP tunnel and
// measures throughput, round-trip latency, and CPU time of the relay process.
// Usage: node relay-bench.mjs <bridgeDir> <port> [--per-conn-rate <bytes/s>]
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { readFileSync } from "node:fs";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { WebSocket } from "./node_modules/ws/wrapper.mjs";

const directory = path.resolve(process.argv[2]);
const port = Number(process.argv[3]);
const rateArg = process.argv.indexOf("--per-conn-rate");
const perConnRate = rateArg === -1 ? "0" : process.argv[rateArg + 1];

const host = "127.0.0.1";
const origin = "http://127.0.0.1:8781";
const bridgeToken = "runtime-generated-token";
const concurrent = 8;
const payloadBytesPerTunnel = 32 * 1024 * 1024; // 32 MiB each way
const frame = Buffer.alloc(64 * 1024, 0x62); // 64 KiB frames
const latencySamples = 500;

function assert(condition, label) {
    if (!condition) {
        throw new Error(`bench failed: ${label}`);
    }
}

async function waitFor(predicate, label, timeoutMs = 20000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        if (predicate()) {
            return;
        }
        await delay(25);
    }
    throw new Error(`bench timed out: ${label}`);
}

// Echo TCP fixture: bytes sent by the relay come straight back.
const fixture = createServer((socket) => {
    socket.setNoDelay(true);
    socket.on("data", (data) => {
        socket.write(data);
    });
    socket.on("error", () => socket.destroy());
});
fixture.listen(0, host);
await once(fixture, "listening");
const fixturePort = fixture.address().port;

const bridge = spawn(process.execPath, ["dist/main.js"], {
    cwd: directory,
    env: {
        ...process.env,
        NODE_ENV: "test",
        GAIUS_BRIDGE_HOST: host,
        GAIUS_BRIDGE_PORT: String(port),
        GAIUS_ALLOWED_ORIGINS: origin,
        GAIUS_ALLOWED_HOSTS: host,
        GAIUS_BRIDGE_TOKEN: bridgeToken,
        GAIUS_IDLE_TIMEOUT_MS: "60000",
        GAIUS_MAXIMUM_BANDWIDTH_PER_CONNECTION: perConnRate,
        GAIUS_MAXIMUM_BANDWIDTH_TOTAL: "0",
        GAIUS_MAXIMUM_TRANSFER_PER_CONNECTION: "0",
    },
    stdio: ["ignore", "pipe", "pipe"],
});
let output = "";
bridge.stdout.setEncoding("utf8");
bridge.stderr.setEncoding("utf8");
bridge.stdout.on("data", (data) => { output += data; });
bridge.stderr.on("data", (data) => { output += data; });
await waitFor(() => output.includes("Gaius translator node listening"),
    "translator node startup");

// Read the relay process CPU usage from /proc for the process tree.
function relayCpuSeconds() {
    let total = 0;
    for (const pid of [bridge.pid]) {
        try {
            const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
            const fields = stat.split(" ");
            const utime = Number(fields[13]);
            const stime = Number(fields[14]);
            total += (utime + stime) / 100;
        } catch { /* gone */ }
    }
    return total;
}

async function runTunnel() {
    const webSocket = new WebSocket(`ws://${host}:${port}/tunnel`, {
        headers: { origin },
    });
    await once(webSocket, "open");
    webSocket.send(JSON.stringify({
        type: "connect", host, port: fixturePort, token: bridgeToken,
    }));
    await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("connect timeout")), 10000);
        webSocket.on("message", (data) => {
            if (data.toString().includes('"type":"connected"')) {
                clearTimeout(timer);
                resolve();
            }
        });
    });

    let received = 0;
    let latencies = [];
    let outstanding = 0;
    let sendDone = false;
    const latencyDone = new Promise((resolve) => {
        webSocket.on("message", (data) => {
            received += data.byteLength;
            if (outstanding > 0) {
                outstanding--;
                if (latencies.length < latencySamples) {
                    // handled via echoLatency below instead
                }
                if (outstanding === 0 && sendDone) {
                    resolve();
                }
            }
        });
    });

    // Phase 1: latency — 500 sequential 1 KiB ping-pongs.
    const ping = Buffer.alloc(1024, 0x63);
    const latStart = performance.now();
    for (let i = 0; i < latencySamples; i++) {
        const sentAt = performance.now();
        const reply = await new Promise((resolve) => {
            const onMessage = (data) => {
                webSocket.off("message", onMessage);
                resolve(data);
            };
            webSocket.on("message", onMessage);
            webSocket.send(ping);
        });
        latencies.push(performance.now() - sentAt);
        received += reply.byteLength;
    }
    const latTotalMs = performance.now() - latStart;
    latencies.sort((a, b) => a - b);

    // Phase 2: throughput — saturate with 64 KiB frames, count echoes.
    const startedAt = performance.now();
    sendDone = true;
    const chunks = payloadBytesPerTunnel / frame.byteLength;
    for (let i = 0; i < chunks; i++) {
        webSocket.send(frame);
    }
    outstanding = chunks;
    const drainStart = received;
    await Promise.race([
        latencyDone,
        delay(120000).then(() => { throw new Error("throughput phase timed out"); }),
    ]);
    const elapsedMs = performance.now() - startedAt;
    const echoedBytes = received - drainStart;
    webSocket.close();
    return {
        latencyAvgMs: +(latTotalMs / latencySamples).toFixed(3),
        latencyP99Ms: +latencies[Math.floor(latencySamples * 0.99) - 1].toFixed(3),
        throughputMbps: +((echoedBytes * 2) / (elapsedMs / 1000) / 1e6).toFixed(1),
        echoedBytes,
        elapsedMs: +elapsedMs.toFixed(0),
    };
}

const cpuBefore = relayCpuSeconds();
const results = await Promise.all(
    Array.from({ length: concurrent }, () => runTunnel()));
const cpuAfter = relayCpuSeconds();

const totalBytes = results.reduce((sum, r) => sum + r.echoedBytes, 0) * 2;
const makespanMs = Math.max(...results.map((r) => r.elapsedMs));
const avgLatency = results.reduce((sum, r) => sum + r.latencyAvgMs, 0) / concurrent;
const p99s = results.map((r) => r.latencyP99Ms).sort((a, b) => a - b);

console.log(JSON.stringify({
    bridgeDir: directory,
    perConnRate: perConnRate === "0" ? "unlimited" : perConnRate,
    tunnels: concurrent,
    mbPerTunnelRoundTrip: +(payloadBytesPerTunnel / 1e6).toFixed(1),
    aggregateGoodputMbps: +((totalBytes / (makespanMs / 1000)) / 1e6).toFixed(1),
    makespanMs: +makespanMs.toFixed(0),
    avgLatencyMs: +avgLatency.toFixed(3),
    worstP99LatencyMs: p99s[p99s.length - 1],
    relayCpuSeconds: +(cpuAfter - cpuBefore).toFixed(2),
    cpuSecondsPerGB: +(((cpuAfter - cpuBefore)) / (totalBytes / 1e9)).toFixed(2),
}, null, 2));

bridge.kill("SIGTERM");
fixture.close();
