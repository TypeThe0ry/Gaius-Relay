import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { WebSocket } from "./node_modules/ws/wrapper.mjs";
import { ephemeralCredential } from "./test-support.mjs";

const host = "127.0.0.1";
const origin = "http://127.0.0.1:8781";
const bridgeToken = ephemeralCredential("bridge");
const directory = fileURLToPath(new URL(".", import.meta.url));
const chunk = Buffer.alloc(16 * 1024, 0x61);

function assert(condition, label) {
    if (!condition) {
        throw new Error(`Bandwidth smoke failed: ${label}`);
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
    throw new Error(`Bandwidth smoke timed out: ${label}`);
}

function startBridge(extraEnv, port) {
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
            ...extraEnv,
        },
        stdio: ["ignore", "pipe", "pipe"],
    });
    let output = "";
    bridge.stdout.setEncoding("utf8");
    bridge.stderr.setEncoding("utf8");
    bridge.stdout.on("data", (data) => {
        output += data;
    });
    bridge.stderr.on("data", (data) => {
        output += data;
    });
    bridge.output = () => output;
    return bridge;
}

async function startEchoFixture() {
    const fixture = createServer((socket) => {
        socket.setNoDelay(true);
        socket.on("data", (data) => {
            socket.write(data);
        });
        socket.on("error", () => socket.destroy());
    });
    fixture.listen(0, host);
    await once(fixture, "listening");
    return fixture;
}

async function fetchManifest(port) {
    const response = await fetch(
        `http://${host}:${port}/relay-node/v1?token=${bridgeToken}`);
    return response.json();
}

async function getFreePort() {
    const probe = createServer();
    probe.listen(0, host);
    await once(probe, "listening");
    const port = probe.address().port;
    await new Promise((resolve) => probe.close(resolve));
    return port;
}

// Sends `totalBytes` in 16 KiB WebSocket frames and resolves once the same
// number of bytes has been echoed back by the TCP fixture.
async function pumpThroughTunnel(webSocket, totalBytes) {
    let received = 0;
    const echoed = new Promise((resolve) => {
        webSocket.on("message", (data) => {
            received += data.byteLength;
            if (received >= totalBytes) {
                resolve();
            }
        });
    });
    const closed = new Promise((resolve) => {
        webSocket.once("close", (code) => resolve(code));
    });
    for (let sent = 0; sent < totalBytes; sent += chunk.byteLength) {
        if (webSocket.readyState !== WebSocket.OPEN) {
            break;
        }
        await delay(1);
        webSocket.send(chunk);
    }
    const winner = await Promise.race([
        echoed.then(() => "echoed"),
        closed.then((code) => `closed:${code}`),
        delay(30000).then(() => "timeout"),
    ]);
    return winner;
}

// Scenario A: a 64 KiB/s per-connection bucket must throttle 512 KiB so the
// round trip takes at least two refill windows, then still complete (pausing
// must resume, never drop or wedge).
async function scenarioRateLimit() {
    const bandwidthPerConnection = 64 * 1024;
    const totalBytes = 512 * 1024;
    const fixture = await startEchoFixture();
    const fixturePort = fixture.address().port;
    const bridgePort = await getFreePort();
    const bridge = startBridge({
        GAIUS_MAXIMUM_BANDWIDTH_PER_CONNECTION: String(bandwidthPerConnection),
    }, bridgePort);
    try {
        await waitFor(
            () => bridge.output().includes("Gaius translator node listening"),
            "translator node startup");
        const manifest = await fetchManifest(bridgePort);
        assert(manifest.traffic?.enabled === true, "traffic accounting enabled");
        assert(manifest.traffic?.bandwidthPerConnectionBytesPerSecond ===
                bandwidthPerConnection,
            "manifest reports the per-connection rate");
        assert(manifest.traffic?.totalIngressBytes !== undefined,
            "manifest reports live traffic totals");
        assert(manifest.capabilities?.includes("traffic-accounting") &&
            manifest.capabilities?.includes("bandwidth-limits"),
            "manifest advertises traffic capabilities");

        const webSocket = new WebSocket(`ws://${host}:${bridgePort}/tunnel`, {
            headers: { origin },
        });
        await once(webSocket, "open");
        webSocket.send(JSON.stringify({
            type: "connect",
            host,
            port: fixturePort,
            token: bridgeToken,
        }));
        await new Promise((resolve, reject) => {
            const timer = setTimeout(
                () => reject(new Error("tunnel connect timed out")), 10000);
            webSocket.on("message", (data) => {
                if (data.toString().includes('"type":"connected"')) {
                    clearTimeout(timer);
                    resolve();
                }
            });
        });

        const startedAt = Date.now();
        const outcome = await pumpThroughTunnel(webSocket, totalBytes);
        const elapsedMs = Date.now() - startedAt;
        assert(outcome === "echoed",
            `throttled tunnel echoed all bytes (outcome=${outcome})`);
        // 512 KiB at 64 KiB/s is 8 seconds of budget; a single-burst bucket
        // plus scheduling slack must still take well over 2 seconds, while an
        // unthrottled loopback echo would finish in well under 1 second.
        assert(elapsedMs > 2000,
            `rate limit throttles throughput (elapsed=${elapsedMs}ms)`);

        const after = await fetchManifest(bridgePort);
        assert(after.traffic?.totalIngressBytes >= totalBytes,
            "manifest accounts ingress bytes");
        assert(after.traffic?.totalEgressBytes >= totalBytes,
            "manifest accounts egress bytes");
        webSocket.close();
        fixture.close();
        return true;
    } finally {
        bridge.kill("SIGTERM");
    }
}

// Scenario B: a per-tunnel transfer cap must close the tunnel with 1009 once
// the cap is exceeded.
async function scenarioTransferLimit() {
    const fixture = await startEchoFixture();
    const fixturePort = fixture.address().port;
    const bridgePort = await getFreePort();
    const bridge = startBridge({
        GAIUS_MAXIMUM_TRANSFER_PER_CONNECTION: String(128 * 1024),
    }, bridgePort);
    try {
        await waitFor(
            () => bridge.output().includes("Gaius translator node listening"),
            "translator node startup (transfer cap)");
        const webSocket = new WebSocket(`ws://${host}:${bridgePort}/tunnel`, {
            headers: { origin },
        });
        await once(webSocket, "open");
        webSocket.send(JSON.stringify({
            type: "connect",
            host,
            port: fixturePort,
            token: bridgeToken,
        }));
        await new Promise((resolve, reject) => {
            const timer = setTimeout(
                () => reject(new Error("tunnel connect timed out")), 10000);
            webSocket.on("message", (data) => {
                if (data.toString().includes('"type":"connected"')) {
                    clearTimeout(timer);
                    resolve();
                }
            });
        });

        const closeCode = await new Promise((resolve, reject) => {
            const timer = setTimeout(
                () => reject(new Error("transfer cap did not close the tunnel")),
                20000);
            webSocket.on("close", (code) => {
                clearTimeout(timer);
                resolve(code);
            });
            const flood = setInterval(() => {
                if (webSocket.readyState !== WebSocket.OPEN) {
                    return;
                }
                webSocket.send(chunk);
            }, 5);
            webSocket.once("close", () => clearInterval(flood));
        });
        assert(closeCode === 1009,
            `transfer cap closes with 1009 (code=${closeCode})`);
        fixture.close();
        return true;
    } finally {
        bridge.kill("SIGTERM");
    }
}

try {
    await scenarioRateLimit();
    await scenarioTransferLimit();
    console.log("bandwidth smoke passed");
} catch (error) {
    console.error(error);
    process.exitCode = 1;
}
