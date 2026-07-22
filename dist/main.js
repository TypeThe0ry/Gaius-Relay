import { timingSafeEqual } from "node:crypto";
import { resolveSrv } from "node:dns/promises";
import { createServer } from "node:http";
import { connect } from "node:net";
import { WebSocket, WebSocketServer, } from "ws";
import { loadConfig } from "./config.js";
import { isHostAllowed, isOriginAllowed, parseConnectRequest, } from "./policy.js";
const config = loadConfig();
const maximumResourcePackBytes = 250 * 1024 * 1024;
const maximumTextureBytes = 16 * 1024 * 1024;
const maximumAuthResponseBytes = 4 * 1024 * 1024;
const maximumAuthRequestBytes = 1024 * 1024;
const maximumRealmsResponseBytes = 16 * 1024 * 1024;
const maximumRealmsRequestBytes = 4 * 1024 * 1024;
const maximumWebSocketBufferedBytes = 4 * 1024 * 1024;
const localTunnelWaitMs = 10 * 60 * 1000;
const localTunnelSessions = new Map();
const allowedAuthHosts = new Set([
    "api.minecraftservices.com",
    "api.mojang.com",
    "sessionserver.mojang.com",
]);
const allowedTextureHosts = new Set([
    "skins.minecraft.net",
    "textures.minecraft.net",
]);
const allowedRealmsHosts = new Set([
    "pc.realms.minecraft.net",
    "java.frontendlegacy.realms.minecraft-services.net",
    "pc-stage.realms.minecraft.net",
    "java.frontendlegacy.stage-c2a40e62.realms.minecraft-services.net",
]);
const httpServer = createServer((request, response) => {
    handleHttpRequest(request, response).catch((error) => {
        console.error("Bridge HTTP request failed:", error instanceof Error ? error.message : error);
        if (!response.headersSent) {
            response.writeHead(502, { "content-type": "text/plain; charset=utf-8" });
            response.end("Upstream request failed");
        }
        else {
            response.destroy();
        }
    });
});
const webSocketServer = new WebSocketServer({
    noServer: true,
    maxPayload: config.maximumFrameBytes,
    perMessageDeflate: false,
});
httpServer.on("upgrade", (request, socket, head) => {
    if (request.url !== "/tunnel") {
        socket.write("HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n");
        socket.destroy();
        return;
    }
    if (!isOriginAllowed(request.headers.origin, config.allowedOrigins)) {
        socket.write("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
        socket.destroy();
        return;
    }
    if (webSocketServer.clients.size >= config.maximumConnections) {
        socket.write("HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n");
        socket.destroy();
        return;
    }
    webSocketServer.handleUpgrade(request, socket, head, (webSocket) => {
        webSocketServer.emit("connection", webSocket, request);
    });
});
webSocketServer.on("connection", (webSocket) => {
    let tcpSocket;
    let connected = false;
    let tcpPausedForWebSocket = false;
    let tcpPausedForClient = false;
    let tunnelCancelled = false;
    let lastActivity = Date.now();
    const idleTimer = setInterval(() => {
        if (Date.now() - lastActivity > config.idleTimeoutMs) {
            closeBoth(1001, "Idle timeout");
        }
    }, Math.min(config.idleTimeoutMs, 5_000));
    idleTimer.unref();
    const closeBoth = (code, reason) => {
        clearInterval(idleTimer);
        tunnelCancelled = true;
        tcpSocket?.destroy();
        if (webSocket.readyState === WebSocket.OPEN ||
            webSocket.readyState === WebSocket.CONNECTING) {
            webSocket.close(code, reason.slice(0, 123));
        }
    };
    const updateTcpReadState = () => {
        if (!connected || tcpSocket === undefined) {
            return;
        }
        if (tcpPausedForWebSocket || tcpPausedForClient) {
            tcpSocket.pause();
        }
        else {
            tcpSocket.resume();
        }
    };
    webSocket.on("message", () => {
        lastActivity = Date.now();
    });
    webSocket.once("message", (rawData, isBinary) => {
        lastActivity = Date.now();
        if (isBinary) {
            closeBoth(1003, "Expected connect control message");
            return;
        }
        try {
            const request = parseConnectRequest(rawData.toString());
            if (!tokenMatches(request.token, config.accessToken)) {
                closeBoth(1008, "Invalid bridge token");
                return;
            }
            const localTunnel = parseLocalTunnelHost(request.host);
            if (localTunnel !== undefined) {
                clearInterval(idleTimer);
                registerLocalTunnel(webSocket, localTunnel, closeBoth);
                return;
            }
            if (!isHostAllowed(request.host, config.allowedHosts)) {
                closeBoth(1008, "Destination is not allowed");
                return;
            }
            openTcpTunnel(request)
                .then((socket) => {
                if (tunnelCancelled || webSocket.readyState !== WebSocket.OPEN) {
                    socket.destroy();
                    return;
                }
                tcpSocket = socket;
                tcpSocket.setNoDelay(true);
                tcpSocket.setKeepAlive(true, 30_000);
                tcpSocket.setTimeout(0);
                connected = true;
                webSocket.send(JSON.stringify({ type: "connected" }));
                tcpSocket.on("data", (chunk) => {
                    lastActivity = Date.now();
                    if (webSocket.readyState !== WebSocket.OPEN) {
                        return;
                    }
                    webSocket.send(chunk, { binary: true }, (error) => {
                        if (error) {
                            console.error(`WebSocket send error for ${request.host}:${request.port}:`, error.message);
                        }
                        if (error && webSocket.readyState === WebSocket.OPEN) {
                            closeBoth(1011, "WebSocket send failed");
                            return;
                        }
                        if (tcpPausedForWebSocket &&
                            webSocket.bufferedAmount < maximumWebSocketBufferedBytes) {
                            tcpPausedForWebSocket = false;
                            updateTcpReadState();
                        }
                    });
                    if (!tcpPausedForWebSocket &&
                        webSocket.bufferedAmount >= maximumWebSocketBufferedBytes) {
                        tcpPausedForWebSocket = true;
                        updateTcpReadState();
                    }
                });
                tcpSocket.once("error", (error) => {
                    console.error("TCP tunnel error:", error.message);
                    closeBoth(1011, "TCP connection failed");
                });
                tcpSocket.once("close", () => closeBoth(1000, "TCP connection closed"));
            }, (error) => {
                console.error("TCP tunnel setup error:", error);
                closeBoth(1011, "TCP tunnel setup failed");
            });
            webSocket.on("message", (data, binary) => {
                lastActivity = Date.now();
                if (!connected || tcpSocket === undefined) {
                    closeBoth(1003, "Tunnel is not connected");
                    return;
                }
                if (!binary) {
                    try {
                        const message = JSON.parse(toBuffer(data).toString("utf8"));
                        if (message?.type !== "flow" || typeof message.paused !== "boolean") {
                            throw new TypeError("Expected flow control message");
                        }
                        tcpPausedForClient = message.paused;
                        updateTcpReadState();
                        webSocket.send(JSON.stringify({ type: "flow", paused: tcpPausedForClient }));
                    }
                    catch {
                        closeBoth(1003, "Invalid tunnel control message");
                    }
                    return;
                }
                if (!tcpSocket.write(toBuffer(data))) {
                    webSocket.pause();
                    tcpSocket.once("drain", () => webSocket.resume());
                }
            });
        }
        catch (error) {
            console.error("Invalid bridge control message:", error);
            closeBoth(1007, "Invalid connect control message");
        }
    });
    webSocket.once("error", (error) => {
        console.error("WebSocket tunnel error:", error.message);
        closeBoth(1011, "WebSocket error");
    });
    webSocket.once("close", () => {
        clearInterval(idleTimer);
        tcpSocket?.destroy();
    });
});
httpServer.listen(config.listenPort, config.listenHost, () => {
    console.log(`Gaius bridge listening on http://${config.listenHost}:${config.listenPort}`);
    console.log(`Allowed Minecraft hosts: ${config.allowedHosts.join(", ")}`);
    if (config.accessToken === undefined) {
        console.warn("GAIUS_BRIDGE_TOKEN is unset; this is acceptable only for local development.");
    }
});
async function openTcpTunnel(request) {
    const targets = await resolveMinecraftTargets(request);
    let lastError;
    for (const target of targets) {
        try {
            return await connectTcpTarget(target);
        }
        catch (error) {
            lastError = error;
            console.warn(`TCP target failed for ${target.host}:${target.port}:`, error instanceof Error ? error.message : error);
        }
    }
    throw lastError ?? new Error("No Minecraft TCP targets were available");
}
async function resolveMinecraftTargets(request) {
    if (request.port !== 25565 || isLiteralAddress(request.host)) {
        return [request];
    }
    try {
        const records = await resolveSrv(`_minecraft._tcp.${request.host}`);
        if (records.length === 0) {
            return [request];
        }
        const targets = orderSrvRecords(records)
            .filter((record) => record.name !== ".")
            .slice(0, 8)
            .map((record) => ({ ...request, host: record.name, port: record.port }));
        if (!targets.some((target) => target.host === request.host && target.port === request.port)) {
            targets.push(request);
        }
        return targets.length > 0 ? targets : [request];
    }
    catch {
        return [request];
    }
}
function orderSrvRecords(records) {
    const priorities = [...new Set(records.map((record) => record.priority))]
        .sort((left, right) => left - right);
    const ordered = [];
    for (const priority of priorities) {
        const candidates = records.filter((record) => record.priority === priority);
        while (candidates.length > 0) {
            const totalWeight = candidates.reduce((sum, record) => sum + Math.max(0, record.weight), 0);
            let index;
            if (totalWeight === 0) {
                index = Math.floor(Math.random() * candidates.length);
            }
            else {
                let selectedWeight = Math.random() * totalWeight;
                index = candidates.length - 1;
                for (let candidate = 0; candidate < candidates.length; candidate++) {
                    selectedWeight -= Math.max(0, candidates[candidate].weight);
                    if (selectedWeight < 0) {
                        index = candidate;
                        break;
                    }
                }
            }
            ordered.push(candidates.splice(index, 1)[0]);
        }
    }
    return ordered;
}
function connectTcpTarget(target) {
    return new Promise((resolve, reject) => {
        const socket = connect({ host: target.host, port: target.port });
        const fail = (error) => {
            socket.off("connect", succeed);
            socket.off("timeout", timeout);
            socket.off("error", fail);
            socket.destroy();
            reject(error);
        };
        const timeout = () => fail(new Error("TCP connect timeout"));
        const succeed = () => {
            socket.off("timeout", timeout);
            socket.off("error", fail);
            socket.setTimeout(0);
            resolve(socket);
        };
        socket.once("connect", succeed);
        socket.once("timeout", timeout);
        socket.once("error", fail);
        socket.setTimeout(config.connectTimeoutMs);
    });
}
function isLiteralAddress(host) {
    return /^\d{1,3}(?:\.\d{1,3}){3}$/u.test(host) || host.includes(":");
}
function tokenMatches(supplied, expected) {
    if (expected === undefined) {
        return true;
    }
    if (supplied === undefined) {
        return false;
    }
    const suppliedBytes = Buffer.from(supplied);
    const expectedBytes = Buffer.from(expected);
    return (suppliedBytes.byteLength === expectedBytes.byteLength &&
        timingSafeEqual(suppliedBytes, expectedBytes));
}
function parseLocalTunnelHost(host) {
    const match = /^(client|server)-([a-f0-9]{32})\.gaius-local$/u.exec(host);
    if (match === null) {
        return undefined;
    }
    return { role: match[1], sessionId: match[2] };
}
function registerLocalTunnel(webSocket, request, closeSelf) {
    let session = localTunnelSessions.get(request.sessionId);
    if (session === undefined) {
        session = {
            client: undefined,
            server: undefined,
            timeout: setTimeout(() => {
                closeLocalTunnelSession(request.sessionId, 1013, "Local server tunnel timed out");
            }, localTunnelWaitMs),
        };
        session.timeout.unref();
        localTunnelSessions.set(request.sessionId, session);
    }
    if (session[request.role] !== undefined) {
        closeSelf(1008, `Duplicate local ${request.role} tunnel`);
        return;
    }
    const endpoint = {
        webSocket,
        role: request.role,
        peer: undefined,
        closed: false,
        flowPaused: false,
        backpressurePaused: false,
    };
    session[request.role] = endpoint;
    const removeEndpoint = () => {
        if (endpoint.closed) {
            return;
        }
        endpoint.closed = true;
        const current = localTunnelSessions.get(request.sessionId);
        if (current?.[request.role] === endpoint) {
            current[request.role] = undefined;
        }
        const peer = endpoint.peer;
        endpoint.peer = undefined;
        if (peer !== undefined && !peer.closed) {
            peer.peer = undefined;
            peer.closed = true;
            if (peer.webSocket.readyState === WebSocket.OPEN ||
                peer.webSocket.readyState === WebSocket.CONNECTING) {
                peer.webSocket.close(1000, "Local tunnel peer closed");
            }
        }
        if (current !== undefined && current.client === undefined && current.server === undefined) {
            clearTimeout(current.timeout);
            localTunnelSessions.delete(request.sessionId);
        }
    };
    webSocket.on("message", (data, binary) => {
        const peer = endpoint.peer;
        if (peer === undefined || peer.closed || peer.webSocket.readyState !== WebSocket.OPEN) {
            closeSelf(1003, "Local tunnel is not connected");
            return;
        }
        if (!binary) {
            try {
                const message = JSON.parse(toBuffer(data).toString("utf8"));
                if (message?.type !== "flow" || typeof message.paused !== "boolean") {
                    throw new TypeError("Expected flow control message");
                }
                if (message.paused) {
                    peer.flowPaused = true;
                }
                else {
                    peer.flowPaused = false;
                }
                updateLocalReadState(peer);
                webSocket.send(JSON.stringify({ type: "flow", paused: message.paused }));
            }
            catch {
                closeSelf(1003, "Invalid local tunnel control message");
            }
            return;
        }
        const bytes = toBuffer(data);
        peer.webSocket.send(bytes, { binary: true }, (error) => {
            if (error) {
                closeLocalTunnelSession(request.sessionId, 1011, "Local tunnel send failed");
                return;
            }
            if (endpoint.backpressurePaused &&
                peer.webSocket.bufferedAmount < maximumWebSocketBufferedBytes) {
                endpoint.backpressurePaused = false;
                updateLocalReadState(endpoint);
            }
        });
        if (peer.webSocket.bufferedAmount >= maximumWebSocketBufferedBytes) {
            endpoint.backpressurePaused = true;
            updateLocalReadState(endpoint);
        }
    });
    webSocket.once("close", removeEndpoint);
    webSocket.once("error", removeEndpoint);
    if (session.client !== undefined && session.server !== undefined) {
        clearTimeout(session.timeout);
        session.client.peer = session.server;
        session.server.peer = session.client;
        session.client.webSocket.send(JSON.stringify({ type: "connected" }));
        session.server.webSocket.send(JSON.stringify({ type: "connected" }));
    }
}
function updateLocalReadState(endpoint) {
    if (endpoint.flowPaused || endpoint.backpressurePaused) {
        endpoint.webSocket.pause();
    }
    else {
        endpoint.webSocket.resume();
    }
}
function closeLocalTunnelSession(sessionId, code, reason) {
    const session = localTunnelSessions.get(sessionId);
    if (session === undefined) {
        return;
    }
    clearTimeout(session.timeout);
    localTunnelSessions.delete(sessionId);
    for (const endpoint of [session.client, session.server]) {
        if (endpoint === undefined) {
            continue;
        }
        endpoint.closed = true;
        endpoint.peer = undefined;
        if (endpoint.webSocket.readyState === WebSocket.OPEN ||
            endpoint.webSocket.readyState === WebSocket.CONNECTING) {
            endpoint.webSocket.close(code, reason.slice(0, 123));
        }
    }
}
function toBuffer(data) {
    if (Buffer.isBuffer(data)) {
        return data;
    }
    if (Array.isArray(data)) {
        return Buffer.concat(data);
    }
    return Buffer.from(data);
}
async function handleHttpRequest(request, response) {
    const requestUrl = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
    if (requestUrl.pathname === "/health") {
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify({
            ok: true,
            activeConnections: webSocketServer.clients.size,
            maximumConnections: config.maximumConnections,
        }));
        return;
    }
    let proxyKind;
    if (requestUrl.pathname === "/proxy/resource-pack") {
        proxyKind = "resource-pack";
    }
    else if (requestUrl.pathname === "/proxy/texture") {
        proxyKind = "texture";
    }
    else if (requestUrl.pathname === "/proxy/auth") {
        proxyKind = "auth";
    }
    else if (requestUrl.pathname === "/proxy/realms") {
        proxyKind = "realms";
    }
    if (proxyKind === undefined) {
        response.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
        response.end("Not found");
        return;
    }
    const origin = request.headers.origin;
    if (!isOriginAllowed(origin, config.allowedOrigins)) {
        response.writeHead(403, { "content-type": "text/plain; charset=utf-8" });
        response.end("Origin is not allowed");
        return;
    }
    const corsHeaders = createCorsHeaders(origin);
    if (request.method === "OPTIONS") {
        response.writeHead(204, corsHeaders);
        response.end();
        return;
    }
    if (!tokenMatches(requestUrl.searchParams.get("token") ?? undefined, config.accessToken)) {
        response.writeHead(403, { ...corsHeaders, "content-type": "text/plain; charset=utf-8" });
        response.end("Invalid bridge token");
        return;
    }
    const targetText = requestUrl.searchParams.get("url");
    if (targetText === null) {
        response.writeHead(400, { ...corsHeaders, "content-type": "text/plain; charset=utf-8" });
        response.end("Missing target URL");
        return;
    }
    let target;
    try {
        target = validateProxyTarget(new URL(targetText), proxyKind);
    }
    catch (error) {
        response.writeHead(400, { ...corsHeaders, "content-type": "text/plain; charset=utf-8" });
        response.end(error instanceof Error ? error.message : "Invalid target URL");
        return;
    }
    const method = request.method ?? "GET";
    if ((proxyKind === "resource-pack" || proxyKind === "texture") && method !== "GET") {
        response.writeHead(405, { ...corsHeaders, allow: "GET, OPTIONS" });
        response.end();
        return;
    }
    if (proxyKind === "auth" && method !== "GET" && method !== "POST") {
        response.writeHead(405, { ...corsHeaders, allow: "GET, POST, OPTIONS" });
        response.end();
        return;
    }
    if (proxyKind === "realms" &&
        method !== "GET" && method !== "POST" && method !== "PUT" && method !== "DELETE") {
        response.writeHead(405, { ...corsHeaders, allow: "GET, POST, PUT, DELETE, OPTIONS" });
        response.end();
        return;
    }
    let body;
    if (method === "POST" || method === "PUT") {
        const maximumRequestBytes = proxyKind === "realms"
            ? maximumRealmsRequestBytes
            : maximumAuthRequestBytes;
        const declaredRequestLength = Number(request.headers["content-length"]);
        if (Number.isFinite(declaredRequestLength) &&
            declaredRequestLength > maximumRequestBytes) {
            response.writeHead(413, { ...corsHeaders, "content-type": "text/plain; charset=utf-8" });
            response.end("Proxy request exceeded size limit");
            return;
        }
        try {
            body = await readRequestBody(request, maximumRequestBytes);
        }
        catch (error) {
            if (error instanceof ProxyRequestSizeError) {
                response.writeHead(413, { ...corsHeaders, "content-type": "text/plain; charset=utf-8" });
                response.end(error.message);
                return;
            }
            throw error;
        }
    }
    const upstream = await fetchWithValidatedRedirects(target, {
        method,
        headers: createUpstreamHeaders(request, proxyKind),
        ...(body === undefined ? {} : { body }),
        redirect: "manual",
    }, proxyKind);
    const maximumBytes = proxyKind === "resource-pack"
        ? maximumResourcePackBytes
        : proxyKind === "texture"
            ? maximumTextureBytes
            : proxyKind === "realms"
                ? maximumRealmsResponseBytes
                : maximumAuthResponseBytes;
    const declaredLength = Number(upstream.headers.get("content-length"));
    if (Number.isFinite(declaredLength) && declaredLength > maximumBytes) {
        await upstream.body?.cancel("Proxy response exceeded size limit");
        response.writeHead(413, { ...corsHeaders, "content-type": "text/plain; charset=utf-8" });
        response.end("Upstream response exceeded size limit");
        return;
    }
    const responseHeaders = {
        ...corsHeaders,
        "cache-control": "no-store",
        "content-type": upstream.headers.get("content-type") ?? "application/octet-stream",
    };
    const contentDisposition = upstream.headers.get("content-disposition");
    if (contentDisposition !== null) {
        responseHeaders["content-disposition"] = contentDisposition;
    }
    const retryAfter = upstream.headers.get("retry-after");
    if (retryAfter !== null) {
        responseHeaders["retry-after"] = retryAfter;
    }
    response.writeHead(upstream.status, responseHeaders);
    if (upstream.body === null) {
        response.end();
        return;
    }
    let received = 0;
    for await (const chunk of upstream.body) {
        received += chunk.byteLength;
        if (received > maximumBytes) {
            response.destroy(new Error("Proxy response exceeded size limit"));
            return;
        }
        if (!response.write(chunk)) {
            await new Promise((resolve) => response.once("drain", resolve));
        }
    }
    response.end();
}
function createCorsHeaders(origin) {
    return {
        "access-control-allow-origin": origin ?? "null",
        "access-control-allow-methods": "GET, POST, PUT, DELETE, OPTIONS",
        "access-control-allow-headers": [
            "authorization",
            "content-type",
            "x-minecraft-pack-format",
            "x-minecraft-username",
            "x-minecraft-uuid",
            "x-minecraft-version",
            "x-minecraft-version-id",
            "x-gaius-realms-cookie",
            "is-prerelease",
        ].join(", "),
        "access-control-expose-headers": "content-type, content-disposition, retry-after",
        "vary": "Origin",
    };
}
function validateProxyTarget(target, proxyKind) {
    if (target.protocol !== "http:" && target.protocol !== "https:") {
        throw new Error("Only HTTP and HTTPS targets are supported");
    }
    if (target.username !== "" || target.password !== "") {
        throw new Error("Target URL credentials are not allowed");
    }
    const host = target.hostname.toLowerCase();
    if (proxyKind === "auth") {
        if (!allowedAuthHosts.has(host)) {
            throw new Error("Authentication target is not allowed");
        }
    }
    else if (proxyKind === "texture") {
        if (!allowedTextureHosts.has(host)) {
            throw new Error("Texture target is not allowed");
        }
    }
    else if (proxyKind === "realms") {
        if (!allowedRealmsHosts.has(host)) {
            throw new Error("Realms target is not allowed");
        }
    }
    else if (!isHostAllowed(host, config.allowedHosts)) {
        throw new Error("Resource-pack target is not allowed");
    }
    return target;
}
async function fetchWithValidatedRedirects(initialTarget, init, proxyKind) {
    let target = initialTarget;
    for (let redirect = 0; redirect <= 5; redirect++) {
        const response = await fetch(target, init);
        if (![301, 302, 303, 307, 308].includes(response.status)) {
            return response;
        }
        const location = response.headers.get("location");
        if (location === null) {
            return response;
        }
        if (redirect === 5) {
            throw new Error("Upstream redirect limit exceeded");
        }
        target = validateProxyTarget(new URL(location, target), proxyKind);
        if (response.status === 303 ||
            ((response.status === 301 || response.status === 302) && init.method === "POST")) {
            const headers = new Headers(init.headers);
            headers.delete("content-type");
            init = { ...init, method: "GET", headers, body: undefined };
        }
    }
    throw new Error("Upstream redirect limit exceeded");
}
function createUpstreamHeaders(request, proxyKind) {
    const headers = new Headers();
    let allowed = [];
    if (proxyKind === "auth") {
        allowed = ["authorization", "content-type", "accept"];
    }
    else if (proxyKind === "realms") {
        allowed = ["content-type", "accept", "is-prerelease"];
    }
    else if (proxyKind === "resource-pack") {
        allowed = [
            "x-minecraft-pack-format",
            "x-minecraft-username",
            "x-minecraft-uuid",
            "x-minecraft-version",
            "x-minecraft-version-id",
        ];
    }
    for (const name of allowed) {
        const value = request.headers[name];
        if (typeof value === "string") {
            headers.set(name, value);
        }
    }
    if (proxyKind === "realms") {
        const cookie = request.headers["x-gaius-realms-cookie"];
        if (typeof cookie === "string") {
            headers.set("cookie", cookie);
        }
    }
    headers.set("user-agent", "Gaius Minecraft browser bridge");
    return headers;
}
async function readRequestBody(request, maximumBytes) {
    const chunks = [];
    let received = 0;
    for await (const chunk of request) {
        received += chunk.byteLength;
        if (received > maximumBytes) {
            throw new ProxyRequestSizeError();
        }
        chunks.push(chunk);
    }
    return Buffer.concat(chunks, received);
}
class ProxyRequestSizeError extends Error {
    constructor() {
        super("Proxy request exceeded size limit");
        this.name = "ProxyRequestSizeError";
    }
}
