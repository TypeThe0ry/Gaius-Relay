import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { createServer as createHttpServer } from "node:http";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { WebSocket } from "./node_modules/ws/wrapper.mjs";

const host = "127.0.0.1";
const origin = "http://127.0.0.1:8781";
const bridgeToken = "relay-telemetry-runtime-generated-token";
const directory = fileURLToPath(new URL(".", import.meta.url));

async function getFreePort() {
    const probe = createServer();
    probe.listen(0, host);
    await once(probe, "listening");
    const port = probe.address().port;
    await new Promise((resolve) => probe.close(resolve));
    return port;
}

const bridgePort = await getFreePort();
const otelPort = await getFreePort();

function assert(condition, label) {
    if (!condition) {
        throw new Error(`Telemetry smoke failed: ${label}`);
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
    throw new Error(`Telemetry smoke timed out: ${label}`);
}

// Mock OTLP collector: records every POSTed /v1/metrics payload.
const collectorPayloads = [];
const collector = createHttpServer((request, response) => {
    if (request.method !== "POST" || !request.url.endsWith("/v1/metrics")) {
        response.writeHead(404);
        response.end();
        return;
    }
    let body = "";
    request.setEncoding("utf8");
    request.on("data", (chunk) => {
        body += chunk;
    });
    request.on("end", () => {
        collectorPayloads.push(JSON.parse(body));
        response.writeHead(200);
        response.end("{}");
    });
});
collector.listen(0, host);
await once(collector, "listening");
const collectorPort = collector.address().port;

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
        GAIUS_BRIDGE_PORT: String(bridgePort),
        GAIUS_ALLOWED_ORIGINS: origin,
        GAIUS_ALLOWED_HOSTS: host,
        GAIUS_BRIDGE_TOKEN: bridgeToken,
        GAIUS_IDLE_TIMEOUT_MS: "60000",
        GAIUS_OTEL_PORT: String(otelPort),
        GAIUS_OTEL_EXPORTER_URL:
            `http://${host}:${collectorPort}/v1/metrics`,
        GAIUS_OTEL_EXPORT_INTERVAL_MS: "2000",
    },
    stdio: ["ignore", "pipe", "pipe"],
});
let output = "";
bridge.stdout.setEncoding("utf8");
bridge.stderr.setEncoding("utf8");
bridge.stdout.on("data", (data) => { output += data; });
bridge.stderr.on("data", (data) => { output += data; });

try {
    await waitFor(() => output.includes("Gaius translator node listening"),
        "translator node startup");
    await waitFor(() => output.includes("Gaius OTel metrics listening"),
        "OTel listener startup");

    // Push some real tunnel traffic so counters are non-zero.
    const webSocket = new WebSocket(`ws://${host}:${bridgePort}/tunnel`, {
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
    const frame = Buffer.alloc(16 * 1024, 0x64);
    for (let i = 0; i < 16; i++) {
        webSocket.send(frame);
    }
    let received = 0;
    await new Promise((resolve) => {
        webSocket.on("message", (data) => {
            received += data.byteLength;
            if (received >= 16 * frame.byteLength) {
                resolve();
            }
        });
    });
    webSocket.close();

    // The private pull endpoint must serve OTLP-shaped metrics.
    const metrics = await (await fetch(`http://${host}:${otelPort}/v1/metrics`)).json();
    assert(Array.isArray(metrics.resourceMetrics) &&
        metrics.resourceMetrics.length === 1, "OTLP resourceMetrics shape");
    const scope = metrics.resourceMetrics[0].scopeMetrics[0];
    assert(scope.scope.name === "dev.gaius.relay-node", "OTLP scope name");
    const names = scope.metrics.map((metric) => metric.name);
    assert(names.includes("gaius.tunnel.active"), "tunnel gauge present");
    const trafficIn = scope.metrics.find((metric) =>
        metric.name === "gaius.tunnel.traffic" &&
        metric.sum.dataPoints[0].attributes
            .some((attribute) => attribute.key === "direction" &&
                attribute.value.stringValue === "ingress"));
    assert(trafficIn !== undefined && trafficIn.sum.dataPoints[0].asDouble > 0,
        "ingress traffic counter non-zero");
    assert(names.includes("gaius.tunnel.throttled"), "throttle gauge present");
    assert(names.includes("gaius.uptime"), "uptime gauge present");
    assert(names.includes("gaius.proxy.traffic"), "proxy counter present");

    // The metrics endpoint is private: the public bridge port must not serve it.
    const leaked = await fetch(`http://${host}:${bridgePort}/v1/metrics`);
    assert(leaked.status === 404, "no metrics on the public listener");

    // The push exporter must deliver a well-formed payload to the collector.
    await waitFor(() => collectorPayloads.length > 0, "OTLP export delivered");
    const exported = collectorPayloads[0];
    assert(exported.resourceMetrics[0].scopeMetrics[0].scope.name ===
        "dev.gaius.relay-node", "exported payload is OTLP for the relay scope");

    console.log("telemetry smoke passed");
} finally {
    bridge.kill("SIGTERM");
    collector.close();
    fixture.close();
}
