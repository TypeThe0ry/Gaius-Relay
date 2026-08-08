import { timingSafeEqual } from "node:crypto";
import { lookup } from "node:dns/promises";
import { createServer } from "node:http";
import { isPrivateNetworkAddress } from "./policy.js";

const protocolVersion = 1;
const registryKind = "gaius-relay-registry";
const registrationKind = "gaius-relay-registration";
const nodesPath = "/relay-registry/v1/nodes";
const maximumRequestBytes = 64 * 1024;

const config = {
    listenHost: process.env.GAIUS_REGISTRY_HOST ?? "127.0.0.1",
    listenPort: parseInteger("GAIUS_REGISTRY_PORT", 8083, 1, 65535),
    token: parseSecret("GAIUS_REGISTRY_TOKEN"),
    leaseMs: parseInteger("GAIUS_REGISTRY_LEASE_MS", 90_000, 2_000, 3_600_000),
    maximumNodes: parseInteger("GAIUS_REGISTRY_MAXIMUM_NODES", 256, 1, 4096),
    verificationTimeoutMs: parseInteger(
        "GAIUS_REGISTRY_VERIFY_TIMEOUT_MS", 5_000, 250, 30_000),
    allowPrivateNodes: process.env.GAIUS_REGISTRY_ALLOW_PRIVATE_NODES === "1",
    allowTokenProtectedNodes:
        process.env.GAIUS_REGISTRY_ALLOW_TOKEN_PROTECTED_NODES === "1",
};

const leases = new Map();
let changedAt = Date.now();
let acceptedRegistrations = 0;
let rejectedRegistrations = 0;

const server = createServer((request, response) => {
    handleRequest(request, response).catch((error) => {
        rejectedRegistrations++;
        const message = error instanceof RequestError
            ? error.message
            : "Registry request failed";
        if (!(error instanceof RequestError)) {
            console.error("Relay registry request failed:", error);
        }
        if (!response.headersSent) {
            sendJson(response, error instanceof RequestError ? error.status : 500, {
                ok: false,
                error: message,
            });
        }
        else {
            response.destroy();
        }
    });
});

const pruneTimer = setInterval(pruneExpiredLeases, Math.max(1_000, config.leaseMs / 3));
pruneTimer.unref();

server.listen(config.listenPort, config.listenHost, () => {
    console.log(
        `Gaius relay registry listening on http://${config.listenHost}:${config.listenPort}`);
});

for (const [signal, code] of [["SIGINT", 130], ["SIGTERM", 143]]) {
    process.once(signal, () => {
        clearInterval(pruneTimer);
        server.close(() => {
            process.exitCode = code;
        });
    });
}

async function handleRequest(request, response) {
    const requestUrl = new URL(
        request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
    const method = request.method ?? "GET";
    if (method === "OPTIONS" &&
            (requestUrl.pathname === "/relay-nodes.json" ||
                requestUrl.pathname === nodesPath)) {
        response.writeHead(204, publicHeaders());
        response.end();
        return;
    }
    if ((requestUrl.pathname === "/relay-nodes.json" ||
            requestUrl.pathname === nodesPath) && (method === "GET" || method === "HEAD")) {
        pruneExpiredLeases();
        sendRegistry(response, method === "HEAD");
        return;
    }
    if (requestUrl.pathname === "/health" && (method === "GET" || method === "HEAD")) {
        pruneExpiredLeases();
        sendJson(response, 200, {
            ok: true,
            kind: "gaius-relay-registry-health",
            protocolVersion,
            activeNodes: leases.size,
            maximumNodes: config.maximumNodes,
            leaseMs: config.leaseMs,
            acceptedRegistrations,
            rejectedRegistrations,
        }, method === "HEAD");
        return;
    }
    if (!requestUrl.pathname.startsWith(nodesPath + "/")) {
        throw new RequestError(404, "Not found");
    }
    const encodedId = requestUrl.pathname.slice(nodesPath.length + 1);
    let nodeId;
    try {
        nodeId = decodeURIComponent(encodedId);
    }
    catch {
        throw new RequestError(400, "Invalid RelayNode id");
    }
    validateNodeId(nodeId);
    requireToken(request);
    if (method === "DELETE") {
        if (leases.delete(nodeId)) changedAt = Date.now();
        response.writeHead(204);
        response.end();
        return;
    }
    if (method !== "PUT") {
        throw new RequestError(405, "RelayNode leases use PUT or DELETE");
    }
    const payload = await readJsonBody(request);
    const registration = await verifyRegistration(nodeId, payload);
    const existing = leases.get(nodeId);
    if (existing === undefined && leases.size >= config.maximumNodes) {
        throw new RequestError(503, "Relay registry is at capacity");
    }
    const expiresAt = Date.now() + config.leaseMs;
    const changed = existing === undefined || existing.url !== registration.url ||
        existing.name !== registration.name || existing.priority !== registration.priority;
    leases.set(nodeId, {...registration, expiresAt});
    if (changed) changedAt = Date.now();
    acceptedRegistrations++;
    sendJson(response, existing === undefined ? 201 : 200, {
        ok: true,
        kind: "gaius-relay-lease",
        protocolVersion,
        id: nodeId,
        expiresAt: new Date(expiresAt).toISOString(),
        renewAfterMs: Math.max(1_000, Math.floor(config.leaseMs / 3)),
    });
}

function sendRegistry(response, headOnly) {
    const nodes = [...leases.entries()]
        .map(([id, lease]) => ({
            id,
            name: lease.name,
            url: lease.url,
            priority: lease.priority,
        }))
        .sort((left, right) => right.priority - left.priority ||
            left.id.localeCompare(right.id));
    sendJson(response, 200, {
        kind: registryKind,
        protocolVersion,
        updatedAt: new Date(changedAt).toISOString(),
        nodes,
    }, headOnly, publicHeaders());
}

async function verifyRegistration(nodeId, payload) {
    if (payload?.kind !== registrationKind || Number(payload.protocolVersion) !== protocolVersion) {
        throw new RequestError(400, "RelayNode registration is incompatible");
    }
    if (payload.id !== nodeId) {
        throw new RequestError(400, "RelayNode id does not match the lease URL");
    }
    const publicUrl = parsePublicRelayUrl(payload.url);
    if (!config.allowPrivateNodes) {
        await requirePublicHost(publicUrl.hostname);
    }
    const priority = Number(payload.priority ?? 0);
    if (!Number.isInteger(priority) || priority < -10_000 || priority > 10_000) {
        throw new RequestError(400, "RelayNode priority must be an integer from -10000 to 10000");
    }
    const manifestUrl = new URL(publicUrl.href);
    manifestUrl.protocol = publicUrl.protocol === "wss:" ? "https:" : "http:";
    manifestUrl.pathname = "/relay-node/v1";
    const manifest = await fetchNodeManifest(manifestUrl);
    if (manifest.requiresToken && !config.allowTokenProtectedNodes) {
        throw new RequestError(400, "Public RelayNodes cannot require an unpublished tunnel token");
    }
    if (manifest.allowsPrivateTargets && !config.allowPrivateNodes) {
        throw new RequestError(400, "Public RelayNodes must block private TCP targets");
    }
    const capabilities = Array.isArray(manifest.capabilities) ? manifest.capabilities : [];
    if (!capabilities.includes("tcp-tunnel")) {
        throw new RequestError(400, "RelayNode does not advertise tcp-tunnel capability");
    }
    const name = String(manifest.name ?? payload.name ?? "").trim();
    if (name.length === 0 || name.length > 80 || /[\r\n]/u.test(name)) {
        throw new RequestError(400, "RelayNode name is invalid");
    }
    return {name, url: publicUrl.href, priority};
}

async function fetchNodeManifest(url) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), config.verificationTimeoutMs);
    try {
        const response = await fetch(url, {
            method: "GET",
            cache: "no-store",
            headers: {accept: "application/json"},
            signal: controller.signal,
        });
        if (!response.ok) {
            throw new RequestError(400, `RelayNode manifest returned ${response.status}`);
        }
        const manifest = await response.json();
        if (manifest?.kind !== "gaius-relay-node" ||
                Number(manifest.protocolVersion) !== protocolVersion ||
                manifest.tunnelPath !== "/tunnel") {
            throw new RequestError(400, "RelayNode manifest is incompatible");
        }
        return manifest;
    }
    catch (error) {
        if (error instanceof RequestError) throw error;
        throw new RequestError(400, "RelayNode manifest could not be verified");
    }
    finally {
        clearTimeout(timeout);
    }
}

function parsePublicRelayUrl(value) {
    let url;
    try {
        url = new URL(String(value ?? ""));
    }
    catch {
        throw new RequestError(400, "RelayNode URL must be absolute");
    }
    const loopback = url.hostname === "127.0.0.1" || url.hostname === "::1" ||
        url.hostname === "localhost";
    if (url.protocol !== "wss:" &&
            !(config.allowPrivateNodes && loopback && url.protocol === "ws:")) {
        throw new RequestError(400, "RelayNode URL must use WSS");
    }
    if (url.pathname !== "/tunnel" || url.search || url.hash ||
            url.username || url.password) {
        throw new RequestError(
            400, "RelayNode URL must be an origin plus /tunnel without credentials or query data");
    }
    return url;
}

async function requirePublicHost(hostname) {
    const normalized = hostname.toLowerCase().replace(/\.$/u, "");
    if (normalized === "localhost" || normalized.endsWith(".localhost") ||
            isPrivateNetworkAddress(normalized)) {
        throw new RequestError(400, "RelayNode URL cannot target a private host");
    }
    let addresses;
    try {
        addresses = await lookup(normalized, {all: true, verbatim: true});
    }
    catch {
        throw new RequestError(400, "RelayNode hostname does not resolve");
    }
    if (addresses.length === 0 ||
            addresses.some((entry) => isPrivateNetworkAddress(entry.address))) {
        throw new RequestError(400, "RelayNode hostname resolves to a private address");
    }
}

function requireToken(request) {
    const authorization = request.headers.authorization;
    const supplied = typeof authorization === "string" && authorization.startsWith("Bearer ")
        ? authorization.slice("Bearer ".length)
        : "";
    const expectedBytes = Buffer.from(config.token);
    const suppliedBytes = Buffer.from(supplied);
    if (expectedBytes.byteLength !== suppliedBytes.byteLength ||
            !timingSafeEqual(expectedBytes, suppliedBytes)) {
        throw new RequestError(403, "Invalid relay registry token");
    }
}

function pruneExpiredLeases(now = Date.now()) {
    let changed = false;
    for (const [id, lease] of leases) {
        if (lease.expiresAt <= now) {
            leases.delete(id);
            changed = true;
        }
    }
    if (changed) changedAt = now;
}

async function readJsonBody(request) {
    const chunks = [];
    let received = 0;
    for await (const chunk of request) {
        received += chunk.byteLength;
        if (received > maximumRequestBytes) {
            throw new RequestError(413, "RelayNode registration is too large");
        }
        chunks.push(chunk);
    }
    try {
        return JSON.parse(Buffer.concat(chunks, received).toString("utf8"));
    }
    catch {
        throw new RequestError(400, "RelayNode registration must be valid JSON");
    }
}

function sendJson(response, status, value, headOnly = false, headers = {}) {
    const body = Buffer.from(JSON.stringify(value));
    response.writeHead(status, {
        "cache-control": "no-store",
        "content-type": "application/json; charset=utf-8",
        "content-length": String(body.byteLength),
        ...headers,
    });
    response.end(headOnly ? undefined : body);
}

function publicHeaders() {
    return {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET, HEAD, OPTIONS",
        "access-control-allow-headers": "content-type",
    };
}

function validateNodeId(value) {
    if (!/^[a-z0-9][a-z0-9._-]{0,63}$/u.test(value)) {
        throw new RequestError(400, "Invalid RelayNode id");
    }
}

function parseInteger(name, fallback, minimum, maximum) {
    const raw = process.env[name];
    if (raw === undefined) return fallback;
    const value = Number(raw);
    if (!Number.isInteger(value) || value < minimum || value > maximum) {
        throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
    }
    return value;
}

function parseSecret(name) {
    const value = process.env[name];
    if (value === undefined || value.length < 16 || value.length > 1024 || /[\r\n]/u.test(value)) {
        throw new Error(`${name} must contain between 16 and 1024 single-line characters`);
    }
    return value;
}

class RequestError extends Error {
    constructor(status, message) {
        super(message);
        this.name = "RequestError";
        this.status = status;
    }
}
