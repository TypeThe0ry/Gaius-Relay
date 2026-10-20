// OpenTelemetry metrics for the RelayNode, with no new dependencies.
//
// The relay already keeps rich in-process telemetry (runtime snapshot,
// traffic meter, registration counters), but it was only reachable as pull
// JSON on the public manifest endpoints. This module exposes it as OTLP/HTTP
// JSON in two ways:
//
// - Pull: a private listener serving OTLP-compatible metric JSON on
//   GET /v1/metrics (plus a plain /health). Bind it to loopback or a VPN
//   interface; it is never part of the public nginx/Caddy surface.
// - Push: an optional OTLP/HTTP JSON exporter that POSTs to a collector
//   endpoint (Grafana Alloy, Uptrace, otel-collector, ...) on an interval.
//
// Everything here is additive: with no port configured and no exporter URL,
// the module is inert.

import { createServer } from "node:http";

const otelProtocolVersion = 1;

function encodeOtlpMetrics(resourceAttributes, metrics) {
    return {
        resourceMetrics: [
            {
                resource: {
                    attributes: resourceAttributes.map(([key, value]) => ({
                        key,
                        value: { stringValue: String(value) },
                    })),
                },
                scopeMetrics: [
                    {
                        scope: {
                            name: "dev.gaius.relay-node",
                            version: `1+otel-${otelProtocolVersion}`,
                        },
                        metrics,
                        schemaUrl: "https://opentelemetry.io/schemas/v1.26.0",
                    },
                ],
            },
        ],
    };
}

// Helpers that build OTLP metric bodies. Counters are monotonic Sums with a
// cumulative aggregation tempo; gauges are plain Gauges. Exemplars and
// histogram buckets are intentionally omitted: the relay's own smoke and
// bench tooling already covers duration distributions.
export function counterMetric(name, description, unit, value, attributes = []) {
    return {
        name,
        description,
        unit,
        sum: {
            dataPoints: [
                {
                    asDouble: value,
                    attributes: attributes.map(([key, v]) => ({
                        key,
                        value: { stringValue: String(v) },
                    })),
                    timeUnixNano: Date.now() * 1e6,
                },
            ],
            aggregationTemporality: 2, // CUMULATIVE
            isMonotonic: true,
        },
    };
}

export function gaugeMetric(name, description, unit, value, attributes = []) {
    return {
        name,
        description,
        unit,
        gauge: {
            dataPoints: [
                {
                    asDouble: value,
                    attributes: attributes.map(([key, v]) => ({
                        key,
                        value: { stringValue: String(v) },
                    })),
                    timeUnixNano: Date.now() * 1e6,
                },
            ],
        },
    };
}

// `providers` is a map of collector functions; each returns an array of OTLP
// metric objects when called. Keeping collection out of main.js lets tests
// register their own providers.
export function createTelemetryCollector(resourceAttributes, providers) {
    return function collect() {
        const metrics = [];
        for (const provider of Object.values(providers)) {
            metrics.push(...provider());
        }
        return encodeOtlpMetrics(resourceAttributes, metrics);
    };
}

// Builds the standard RelayNode metric set from the runtime snapshot and
// traffic meter that main.js already maintains.
export function relayMetricProviders(runtimeSnapshot) {
    return {
        uptime: () => [
            gaugeMetric("gaius.uptime", "RelayNode process uptime", "s",
                runtimeSnapshot().uptimeMillis / 1000),
        ],
        tunnels: () => {
            const snapshot = runtimeSnapshot();
            return [
                gaugeMetric("gaius.tunnel.active",
                    "Active tunnel leases", "{tunnel}",
                    snapshot.activeTunnelLeases),
                gaugeMetric("gaius.transport.active",
                    "Active transport WebSockets", "{connection}",
                    snapshot.activeTransportWebSockets),
                gaugeMetric("gaius.tunnel.local.active",
                    "Active local tunnel sessions", "{session}",
                    snapshot.activeLocalTunnelSessions),
            ];
        },
        traffic: () => {
            const traffic = runtimeSnapshot().traffic ?? {};
            if (traffic.enabled !== true) {
                return [];
            }
            return [
                counterMetric("gaius.tunnel.traffic",
                    "Tunnel bytes transferred", "By", traffic.totalIngressBytes ?? 0,
                    [["direction", "ingress"]]),
                counterMetric("gaius.tunnel.traffic",
                    "Tunnel bytes transferred", "By", traffic.totalEgressBytes ?? 0,
                    [["direction", "egress"]]),
                gaugeMetric("gaius.tunnel.traffic.rate",
                    "Tunnel byte rate", "By", traffic.ingressBytesPerSecond ?? 0,
                    [["direction", "ingress"]]),
                gaugeMetric("gaius.tunnel.traffic.rate",
                    "Tunnel byte rate", "By", traffic.egressBytesPerSecond ?? 0,
                    [["direction", "egress"]]),
                gaugeMetric("gaius.tunnel.throttled",
                    "Tunnels currently paused by a rate limit", "{tunnel}",
                    traffic.throttledTunnels ?? 0),
                counterMetric("gaius.proxy.traffic",
                    "HTTP proxy bytes transferred", "By",
                    traffic.proxyBytes ?? 0),
            ];
        },
        registration: () => {
            const snapshot = runtimeSnapshot();
            const registration = snapshot.registration ?? {};
            if (registration.configured !== true) {
                return [];
            }
            return [
                counterMetric("gaius.registry.registrations",
                    "Registry registration attempts by result", "{attempt}",
                    registration.successes ?? 0, [["result", "success"]]),
                counterMetric("gaius.registry.registrations",
                    "Registry registration attempts by result", "{attempt}",
                    registration.failures ?? 0, [["result", "failure"]]),
            ];
        },
        keepalive: () => {
            const snapshot = runtimeSnapshot();
            const proxy = snapshot.keepAliveProxy ?? {};
            return [
                counterMetric("gaius.keepalive.proxied",
                    "Keepalive packets proxied", "{packet}",
                    proxy.proxiedKeepAlives ?? 0),
                counterMetric("gaius.keepalive.opaque_transitions",
                    "Tunnels that turned opaque", "{tunnel}",
                    (proxy.opaqueTransitions ?? 0) +
                    (proxy.encryptionOpaqueTransitions ?? 0)),
            ];
        },
    };
}

// Private pull listener. Never binds a public interface by default; the
// caller passes config-derived host/port.
export function createTelemetryListener(collect, listenHost, listenPort) {
    const server = createServer((request, response) => {
        const requestUrl = new URL(request.url ?? "/",
            `http://${request.headers.host ?? "localhost"}`);
        if (requestUrl.pathname === "/health") {
            response.writeHead(200, { "content-type": "application/json" });
            response.end(JSON.stringify({ ok: true }));
            return;
        }
        if (request.method === "GET" && requestUrl.pathname === "/v1/metrics") {
            response.writeHead(200, { "content-type": "application/json" });
            response.end(JSON.stringify(collect()));
            return;
        }
        response.writeHead(404);
        response.end();
    });
    server.listen(listenPort, listenHost);
    return {
        server,
        close: () => new Promise((resolve) => server.close(resolve)),
    };
}

// Push exporter on a fixed interval. Errors are logged and retried on the
// next interval; a flaky collector must never take the relay down.
export function createTelemetryExporter(collect, exporterUrl, intervalMs) {
    let stopped = false;
    let inFlight = false;
    const timer = setInterval(() => {
        if (stopped || inFlight) {
            return;
        }
        inFlight = true;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(),
            Math.min(10_000, Math.max(1_000, intervalMs)));
        fetch(exporterUrl, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(collect()),
            signal: controller.signal,
        }).then(async (response) => {
            await response.body?.cancel();
            if (!response.ok) {
                throw new Error(`collector returned ${response.status}`);
            }
        }).catch((error) => {
            // Windows may surface a collector closing an idle keep-alive as a
            // connection-reset error. It is a failed export, not a relay
            // failure; the next interval retries it without noisy logging.
            const connectionReset = error?.code === "ECONNRESET" ||
                error?.cause?.code === "ECONNRESET";
            if (!stopped && !connectionReset) {
                console.error("OTLP export failed:", error.message);
            }
        }).finally(() => {
            clearTimeout(timeout);
            inFlight = false;
        });
    }, intervalMs);
    timer.unref();
    return {
        stop: () => {
            stopped = true;
            clearInterval(timer);
        },
    };
}
