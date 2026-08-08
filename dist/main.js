import { randomUUID, timingSafeEqual } from "node:crypto";
import { lookup as dnsLookup } from "node:dns";
import { resolveSrv } from "node:dns/promises";
import { createReadStream, unlinkSync } from "node:fs";
import { open, unlink } from "node:fs/promises";
import { createServer } from "node:http";
import { connect } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WebSocket, WebSocketServer, } from "ws";
import { loadConfig } from "./config.js";
import { isHostAllowed, isOriginAllowed, isPrivateNetworkAddress, normalizeHost, parseConnectRequest, } from "./policy.js";
const config = loadConfig();
const traceTunnel = process.env.GAIUS_TRACE_TUNNEL === "1";
const relayNodeProtocolVersion = 1;
const relayNodeManifestPath = "/relay-node/v1";
const maximumResourcePackBytes = 250 * 1024 * 1024;
const maximumTextureBytes = 16 * 1024 * 1024;
const maximumAuthResponseBytes = 4 * 1024 * 1024;
const maximumAuthRequestBytes = 1024 * 1024;
const maximumRealmsResponseBytes = 16 * 1024 * 1024;
const maximumRealmsRequestBytes = 4 * 1024 * 1024;
const maximumWebSocketBufferedBytes = 4 * 1024 * 1024;
const resourcePackBodyAttempts = 3;
const localTunnelWaitMs = 10 * 60 * 1000;
const relayCapabilities = [
    "tcp-tunnel",
    "ephemeral-tunnel-lease",
    "target-affinity",
    "srv-resolution",
    "flow-control",
    "keepalive-proxy",
    "configuration-reentry",
    "resource-pack-proxy",
    "resource-pack-cache",
    "public-target-guard",
];
// `ServerboundClientTickEndPacket` is emitted once per client tick in 1.21.11.
// A resource/model reload can temporarily stop browser ticks, while a spawn
// proxy can require the next tick before its short read timeout. The relay can
// synthesize this payloadless 1.21.11 packet as soon as configuration enters
// PLAY, then switches to the exact frame observed from the browser.
const stalledClientTickIntervalMs = 50;
const stalledClientTickGraceMs = 100;
const localTunnelSessions = new Map();
const targetRoutes = new Map();
const resourcePackCache = new Map();
const resourcePackTemporaryPaths = new Set();
let resourcePackCacheBytes = 0;
const relayRegistrationState = {
    configured: config.registration !== undefined,
    registered: false,
    attempts: 0,
    successes: 0,
    failures: 0,
    lastAttemptAt: 0,
    lastSuccessAt: 0,
    lastError: null,
};
process.once("exit", cleanupResourcePackTemporaryFiles);
process.once("SIGINT", () => process.exit(130));
process.once("SIGTERM", () => process.exit(143));

function targetRouteKey(host, port) {
    const normalized = normalizeHost(host);
    const authority = normalized.includes(":") ? `[${normalized}]` : normalized;
    return `${authority}:${port}`;
}

function pruneTargetRoutes(now = Date.now()) {
    for (const [key, route] of targetRoutes) {
        if (route.activeConnections === 0 &&
            now - route.lastConnectedAt > config.targetAffinityMs) {
            targetRoutes.delete(key);
        }
    }
    while (targetRoutes.size >= config.maximumTargetRoutes) {
        let oldestKey;
        let oldestConnectedAt = Number.POSITIVE_INFINITY;
        for (const [key, route] of targetRoutes) {
            if (route.activeConnections === 0 && route.lastConnectedAt < oldestConnectedAt) {
                oldestKey = key;
                oldestConnectedAt = route.lastConnectedAt;
            }
        }
        if (oldestKey === undefined) {
            break;
        }
        targetRoutes.delete(oldestKey);
    }
}

function acquireTargetRoute(request) {
    const now = Date.now();
    pruneTargetRoutes(now);
    const key = targetRouteKey(request.host, request.port);
    let route = targetRoutes.get(key);
    if (route === undefined) {
        route = {
            activeConnections: 0,
            totalConnections: 0,
            lastConnectedAt: now,
            lastDisconnectedAt: 0,
        };
        targetRoutes.set(key, route);
    }
    route.activeConnections++;
    route.totalConnections++;
    route.lastConnectedAt = now;
    let released = false;
    return () => {
        if (released) {
            return;
        }
        released = true;
        route.activeConnections = Math.max(0, route.activeConnections - 1);
        route.lastDisconnectedAt = Date.now();
        pruneTargetRoutes(route.lastDisconnectedAt);
    };
}

function targetRouteSnapshot(request) {
    const now = Date.now();
    pruneTargetRoutes(now);
    const route = targetRoutes.get(targetRouteKey(request.host, request.port));
    if (route === undefined) {
        return {
            activeConnections: 0,
            recentlyReachable: false,
        };
    }
    return {
        activeConnections: route.activeConnections,
        recentlyReachable: route.activeConnections > 0 ||
            now - route.lastConnectedAt <= config.targetAffinityMs,
        totalConnections: route.totalConnections,
        lastSuccessAgeMs: Math.max(0, now - route.lastConnectedAt),
    };
}

// A zero-compression keepalive is a complete 11-byte Minecraft frame. During a
// large browser resource reload, acknowledging it in the translator node prevents a
// backend read timeout without delaying arbitrary game packets. These ids are
// from the 1.21.11 configuration and play protocol tables respectively.
function proxyVanillaKeepAlive(socket, frame, protocolPhase) {
    if (!config.proxyKeepAlives || frame.byteLength !== 11 ||
        frame[0] !== 0x0a || frame[1] !== 0x00) {
        return false;
    }
    const packetId = frame[2];
    let responsePacketId;
    if ((protocolPhase === "login" || protocolPhase === "configuration") &&
        packetId === 0x04) {
        // Configuration uses the common keepalive packet id in both directions.
        responsePacketId = 0x04;
    }
    else if (protocolPhase === "play" && packetId === 0x2b) {
        // PLAY has different clientbound/serverbound packet registries.
        responsePacketId = 0x1b;
    }
    else {
        return false;
    }
    const response = Buffer.from(frame);
    response[2] = responsePacketId;
    socket.write(response);
    traceTunnelEvent(
        `proxied ${packetId === 0x04 ? "configuration" : "play"} keepalive `
            + `head=${response.toString("hex")}`
    );
    return true;
}

function readMinecraftFrame(buffer) {
    let length = 0;
    let shift = 0;
    for (let index = 0; index < 5; index++) {
        if (index >= buffer.byteLength) {
            return undefined;
        }
        const value = buffer[index];
        length |= (value & 0x7f) << shift;
        if ((value & 0x80) === 0) {
            const headerBytes = index + 1;
            if (length < 0 || length > config.maximumFrameBytes) {
                return null;
            }
            const frameBytes = headerBytes + length;
            if (buffer.byteLength < frameBytes) {
                return undefined;
            }
            return {
                frame: buffer.subarray(0, frameBytes),
                remainder: buffer.subarray(frameBytes),
                headerBytes,
            };
        }
        shift += 7;
    }
    return null;
}

function isLoginEncryptionRequest(frame, headerBytes) {
    // Encryption begins after this login packet. Once the client answers it,
    // bytes are opaque and must remain a direct WebSocket-to-TCP tunnel.
    return frame.byteLength > headerBytes && frame[headerBytes] === 0x01;
}

function isMinecraftHandshake(frame) {
    const parsed = readMinecraftFrame(frame);
    if (parsed === undefined || parsed === null || parsed.remainder.byteLength !== 0) {
        return false;
    }
    const packetOffset = parsed.headerBytes;
    const nextState = parsed.frame[parsed.frame.byteLength - 1];
    return parsed.frame.byteLength > packetOffset + 2 &&
        parsed.frame[packetOffset] === 0x00 && (nextState === 0x01 || nextState === 0x02);
}

function minecraftPacketId(frame, headerBytes) {
    if (frame.byteLength <= headerBytes) {
        return undefined;
    }
    // Compression framing prefixes uncompressed packets with a zero data length.
    const packetOffset = frame[headerBytes] === 0x00 ? headerBytes + 1 : headerBytes;
    if (packetOffset >= frame.byteLength) {
        return undefined;
    }
    return { id: frame[packetOffset], packetOffset };
}

function isPayloadlessPacket(frame, headerBytes, packetId) {
    const packet = minecraftPacketId(frame, headerBytes);
    return packet !== undefined && packet.id === packetId &&
        packet.packetOffset + 1 === frame.byteLength;
}

function traceCustomPayload(frame, headerBytes, direction, playPhase) {
    if (!traceTunnel || !playPhase || frame.byteLength <= headerBytes + 2) {
        return;
    }
    // Compressed packet framing adds one zero byte before the packet id when
    // the packet remains below the compression threshold. Only inspect that
    // small, plaintext form; encrypted/compressed traffic stays opaque.
    const packetOffset = frame[headerBytes] === 0x00 ? headerBytes + 1 : headerBytes;
    const packetId = frame[packetOffset];
    const expectedPacketId = direction === "server" ? 0x18 : 0x15;
    if (packetId !== expectedPacketId || frame.byteLength <= packetOffset + 1) {
        return;
    }
    const payload = frame.subarray(packetOffset + 1);
    const channelLength = payload[0];
    if (!Number.isInteger(channelLength) || channelLength < 1 ||
        channelLength > payload.byteLength - 1) {
        return;
    }
    const channel = payload.subarray(1, 1 + channelLength).toString("utf8");
    const preview = payload.subarray(1 + channelLength, 1 + channelLength + 192)
        .toString("utf8").replace(/[\u0000-\u001f\u007f-\uffff]/g, "?");
    traceTunnelEvent(`PLAY custom payload ${direction} channel=${channel} preview=${preview}`);
}

function traceTunnelEvent(message) {
    if (traceTunnel) {
        console.info(`[Gaius tunnel trace] ${message}`);
    }
}
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
    const requestUrl = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
    if (requestUrl.pathname !== "/tunnel") {
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
    let tunnelRequest;
    let releaseTargetRoute = () => {};
    let connected = false;
    let tcpPausedForWebSocket = false;
    let tcpPausedForClient = false;
    let protocolPhase = "login";
    let configurationCycles = 0;
    let serverFrameBuffer = Buffer.alloc(0);
    let clientFrameBuffer = Buffer.alloc(0);
    const tunnelStartedAt = Date.now();
    let playStartedAt;
    let lastServerPlayPacket;
    let lastClientPlayPacket;
    // Only frame streams after a valid Minecraft handshake. This preserves the
    // relay's raw-TCP behavior for generic tunnel tests and opaque protocols.
    let packetFramingEnabled = false;
    let encryptionResponsePending = false;
    let playTickFrame;
    let lastClientTrafficAt = Date.now();
    let clientStallTimer;
    let tunnelCancelled = false;
    const tunnelConnectAbortController = new AbortController();
    let lastActivity = Date.now();
    const idleTimer = setInterval(() => {
        if (Date.now() - lastActivity > config.idleTimeoutMs) {
            closeBoth(1001, "Idle timeout");
        }
    }, Math.min(config.idleTimeoutMs, 5_000));
    idleTimer.unref();
    const closeBoth = (code, reason) => {
        traceTunnelEvent(`closing tunnel code=${code} reason=${reason}`);
        clearInterval(idleTimer);
        clearInterval(clientStallTimer);
        tunnelCancelled = true;
        tunnelConnectAbortController.abort();
        releaseTargetRoute();
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
    clientStallTimer = setInterval(() => {
        if (!connected || protocolPhase !== "play" || playTickFrame === undefined ||
            tcpSocket === undefined || Date.now() - lastClientTrafficAt < stalledClientTickGraceMs) {
            return;
        }
        tcpSocket.write(playTickFrame);
        lastClientTrafficAt = Date.now();
        traceTunnelEvent("proxied observed play tick while browser was stalled");
    }, stalledClientTickIntervalMs);
    clientStallTimer.unref();
    const forwardServerFrame = (frame) => {
        if (webSocket.readyState !== WebSocket.OPEN) {
            return;
        }
        webSocket.send(frame, { binary: true }, (error) => {
            if (error) {
                const target = tunnelRequest === undefined
                    ? "unknown target"
                    : `${tunnelRequest.host}:${tunnelRequest.port}`;
                console.error(`WebSocket send error for ${target}:`, error.message);
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
            tunnelRequest = request;
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
            webSocket.send(JSON.stringify({
                type: "connecting",
                targetConnectTimeoutMs: config.connectTimeoutMs,
            }));
            openTcpTunnel(request, tunnelConnectAbortController.signal)
                .then((socket) => {
                if (tunnelCancelled || webSocket.readyState !== WebSocket.OPEN) {
                    socket.destroy();
                    return;
                }
                tcpSocket = socket;
                tcpSocket.setNoDelay(true);
                tcpSocket.setKeepAlive(true, 30_000);
                tcpSocket.setTimeout(0);
                releaseTargetRoute = acquireTargetRoute(request);
                connected = true;
                webSocket.send(JSON.stringify({ type: "connected" }));
                tcpSocket.on("data", (chunk) => {
                    traceTunnelEvent(
                        `server data ${request.host}:${request.port} bytes=${chunk.byteLength} `
                            + `head=${chunk.subarray(0, 24).toString("hex")}`
                    );
                    lastActivity = Date.now();
                    if (!packetFramingEnabled) {
                        if (proxyVanillaKeepAlive(tcpSocket, chunk, protocolPhase)) {
                            return;
                        }
                        forwardServerFrame(chunk);
                        return;
                    }
                    serverFrameBuffer = serverFrameBuffer.byteLength === 0
                        ? chunk
                        : Buffer.concat([serverFrameBuffer, chunk]);
                    while (serverFrameBuffer.byteLength > 0) {
                        const parsed = readMinecraftFrame(serverFrameBuffer);
                        if (parsed === undefined) {
                            return;
                        }
                        if (parsed === null) {
                            // This is normally encrypted online-mode traffic.
                            packetFramingEnabled = false;
                            traceTunnelEvent("disabled keepalive proxy for opaque server traffic");
                            forwardServerFrame(serverFrameBuffer);
                            serverFrameBuffer = Buffer.alloc(0);
                            return;
                        }
                        serverFrameBuffer = parsed.remainder;
                        if (protocolPhase === "play") {
                            const packet = minecraftPacketId(parsed.frame, parsed.headerBytes);
                            if (packet !== undefined) {
                                lastServerPlayPacket = `0x${packet.id.toString(16)}/${parsed.frame.byteLength}`;
                            }
                        }
                        if (protocolPhase === "play" &&
                            isPayloadlessPacket(parsed.frame, parsed.headerBytes, 0x74)) {
                            protocolPhase = "reconfiguring";
                            traceTunnelEvent("server started PLAY to CONFIGURATION transition");
                        }
                        if (isLoginEncryptionRequest(parsed.frame, parsed.headerBytes)) {
                            encryptionResponsePending = true;
                        }
                        traceCustomPayload(
                            parsed.frame,
                            parsed.headerBytes,
                            "server",
                            protocolPhase === "play"
                        );
                        if (!proxyVanillaKeepAlive(tcpSocket, parsed.frame, protocolPhase)) {
                            forwardServerFrame(parsed.frame);
                        }
                    }
                });
                tcpSocket.once("error", (error) => {
                    console.error("TCP tunnel error:", error.message);
                    closeBoth(1011, "TCP connection failed");
                });
                tcpSocket.once("close", (hadError) => {
                    traceTunnelEvent(
                        `TCP closed ${request.host}:${request.port} hadError=${Boolean(hadError)} `
                            + `tunnelMs=${Date.now() - tunnelStartedAt} `
                            + `playMs=${playStartedAt === undefined ? "n/a" : Date.now() - playStartedAt} `
                            + `phase=${protocolPhase} configurationCycles=${configurationCycles} `
                            + `lastServerPlay=${lastServerPlayPacket ?? "n/a"} `
                            + `lastClientPlay=${lastClientPlayPacket ?? "n/a"}`
                    );
                    closeBoth(1000, "TCP connection closed");
                });
            }, (error) => {
                if (tunnelCancelled || tunnelConnectAbortController.signal.aborted) {
                    return;
                }
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
                const clientData = toBuffer(data);
                lastClientTrafficAt = Date.now();
                traceTunnelEvent(
                    `client data ${request.host}:${request.port} bytes=${clientData.byteLength} `
                        + `head=${clientData.subarray(0, 24).toString("hex")}`
                );
                if (!packetFramingEnabled && config.proxyKeepAlives &&
                    isMinecraftHandshake(clientData)) {
                    packetFramingEnabled = true;
                    traceTunnelEvent("enabled framed keepalive proxy after Minecraft handshake");
                }
                if (packetFramingEnabled) {
                    clientFrameBuffer = clientFrameBuffer.byteLength === 0
                        ? clientData
                        : Buffer.concat([clientFrameBuffer, clientData]);
                    while (clientFrameBuffer.byteLength > 0) {
                        const parsed = readMinecraftFrame(clientFrameBuffer);
                        if (parsed === undefined) {
                            break;
                        }
                        if (parsed === null) {
                            packetFramingEnabled = false;
                            clientFrameBuffer = Buffer.alloc(0);
                            traceTunnelEvent("disabled keepalive proxy for opaque client traffic");
                            break;
                        }
                        clientFrameBuffer = parsed.remainder;
                        if (protocolPhase === "login" &&
                            isPayloadlessPacket(parsed.frame, parsed.headerBytes, 0x03)) {
                            protocolPhase = "configuration";
                            traceTunnelEvent("login acknowledged; entered CONFIGURATION");
                        }
                        else if (protocolPhase === "configuration" &&
                            isPayloadlessPacket(parsed.frame, parsed.headerBytes, 0x03)) {
                            configurationCycles++;
                            protocolPhase = "play";
                            // 1.21.11 ServerboundClientTickEndPacket is 0x0c. Match the
                            // compression framing already used by the configuration ACK.
                            playTickFrame = parsed.frame[parsed.headerBytes] === 0x00
                                ? Buffer.from([0x02, 0x00, 0x0c])
                                : Buffer.from([0x01, 0x0c]);
                            lastClientTrafficAt = Date.now();
                            if (playStartedAt === undefined) {
                                playStartedAt = Date.now();
                                traceTunnelEvent("armed synthetic play tick for initial spawn");
                            }
                            else {
                                traceTunnelEvent(
                                    `re-entered PLAY after configuration cycle ${configurationCycles}`
                                );
                            }
                        }
                        else if ((protocolPhase === "play" ||
                            protocolPhase === "reconfiguring") &&
                            isPayloadlessPacket(parsed.frame, parsed.headerBytes, 0x0f)) {
                            protocolPhase = "configuration";
                            lastClientTrafficAt = Date.now();
                            traceTunnelEvent("client acknowledged PLAY to CONFIGURATION transition");
                        }
                        if (protocolPhase === "play") {
                            const packet = minecraftPacketId(parsed.frame, parsed.headerBytes);
                            if (packet !== undefined) {
                                lastClientPlayPacket = `0x${packet.id.toString(16)}/${parsed.frame.byteLength}`;
                            }
                        }
                        if (protocolPhase === "play" &&
                            isPayloadlessPacket(parsed.frame, parsed.headerBytes, 0x0c)) {
                            playTickFrame = Buffer.from(parsed.frame);
                            traceTunnelEvent("observed play tick for stall proxy");
                        }
                        traceCustomPayload(
                            parsed.frame,
                            parsed.headerBytes,
                            "client",
                            protocolPhase === "play"
                        );
                    }
                }
                if (encryptionResponsePending) {
                    packetFramingEnabled = false;
                    encryptionResponsePending = false;
                    serverFrameBuffer = Buffer.alloc(0);
                    clientFrameBuffer = Buffer.alloc(0);
                    traceTunnelEvent("disabled keepalive proxy after login encryption response");
                }
                if (!tcpSocket.write(clientData)) {
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
    webSocket.once("close", (code, reason) => {
        traceTunnelEvent(
            `WebSocket closed code=${code} reason=${reason.toString()} connected=${connected}`
        );
        clearInterval(idleTimer);
        clearInterval(clientStallTimer);
        tunnelCancelled = true;
        tunnelConnectAbortController.abort();
        releaseTargetRoute();
        tcpSocket?.destroy();
    });
});
httpServer.listen(config.listenPort, config.listenHost, () => {
    console.log(`Gaius translator node listening on http://${config.listenHost}:${config.listenPort}`);
    console.log(`Allowed Minecraft hosts: ${config.allowedHosts.join(", ")}`);
    if (config.accessToken === undefined) {
        console.warn("GAIUS_BRIDGE_TOKEN is unset; this is acceptable only for local development.");
    }
    startRelayRegistration();
});
function startRelayRegistration() {
    if (config.registration === undefined) {
        return;
    }
    const registration = config.registration;
    const leaseUrl = new URL(encodeURIComponent(registration.nodeId), registration.registryUrl);
    let requestRunning = false;
    let announced = false;
    const register = async () => {
        if (requestRunning) {
            return;
        }
        requestRunning = true;
        relayRegistrationState.attempts++;
        relayRegistrationState.lastAttemptAt = Date.now();
        const controller = new AbortController();
        const timeout = setTimeout(
            () => controller.abort(), Math.min(10_000, registration.intervalMs));
        try {
            const response = await fetch(leaseUrl, {
                method: "PUT",
                headers: {
                    authorization: `Bearer ${registration.token}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({
                    kind: "gaius-relay-registration",
                    protocolVersion: relayNodeProtocolVersion,
                    id: registration.nodeId,
                    name: config.relayName,
                    url: registration.publicUrl,
                    priority: registration.priority,
                }),
                signal: controller.signal,
            });
            if (!response.ok) {
                const detail = (await response.text()).slice(0, 240);
                throw new Error(`registry returned ${response.status}: ${detail}`);
            }
            relayRegistrationState.registered = true;
            relayRegistrationState.successes++;
            relayRegistrationState.lastSuccessAt = Date.now();
            relayRegistrationState.lastError = null;
            if (!announced) {
                announced = true;
                console.log(
                    `RelayNode registered as ${registration.nodeId} at ${registration.registryUrl}`);
            }
        }
        catch (error) {
            relayRegistrationState.registered = false;
            relayRegistrationState.failures++;
            relayRegistrationState.lastError = String(
                error instanceof Error ? error.message : error).slice(0, 240);
            console.warn("RelayNode registration failed:", relayRegistrationState.lastError);
        }
        finally {
            clearTimeout(timeout);
            requestRunning = false;
        }
    };
    void register();
    const timer = setInterval(register, registration.intervalMs);
    timer.unref();
}
async function openTcpTunnel(request, signal) {
    const targets = await resolveMinecraftTargets(request);
    let lastError;
    for (const target of targets) {
        if (signal.aborted) {
            throw new Error("TCP tunnel cancelled");
        }
        try {
            return await connectTcpTarget(target, signal);
        }
        catch (error) {
            if (signal.aborted) {
                throw new Error("TCP tunnel cancelled");
            }
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
function connectTcpTarget(target, signal) {
    if (!config.allowPrivateTargets && isPrivateNetworkAddress(target.host)) {
        return Promise.reject(new Error("Private TCP targets are not allowed"));
    }
    return new Promise((resolve, reject) => {
        const socket = connect({
            host: target.host,
            port: target.port,
            ...(config.allowPrivateTargets ? {} : {lookup: publicTargetLookup}),
        });
        let settled = false;
        const cleanup = () => {
            socket.off("connect", succeed);
            socket.off("timeout", timeout);
            socket.off("error", fail);
            signal.removeEventListener("abort", abort);
        };
        const fail = (error) => {
            if (settled) {
                return;
            }
            settled = true;
            cleanup();
            socket.destroy();
            reject(error);
        };
        const timeout = () => fail(new Error("TCP connect timeout"));
        const abort = () => fail(new Error("TCP tunnel cancelled"));
        const succeed = () => {
            if (settled) {
                return;
            }
            settled = true;
            cleanup();
            socket.setTimeout(0);
            resolve(socket);
        };
        if (signal.aborted) {
            abort();
            return;
        }
        signal.addEventListener("abort", abort, { once: true });
        socket.once("connect", succeed);
        socket.once("timeout", timeout);
        socket.once("error", fail);
        socket.setTimeout(config.connectTimeoutMs);
    });
}
function publicTargetLookup(host, options, callback) {
    const returnAll = typeof options === "object" && options !== null && options.all === true;
    const lookupOptions = typeof options === "object" && options !== null
        ? {...options, all: true}
        : {family: options, all: true};
    dnsLookup(host, lookupOptions, (error, addresses) => {
        if (error !== null) {
            callback(error);
            return;
        }
        const publicAddresses = addresses.filter(
            (entry) => !isPrivateNetworkAddress(entry.address));
        if (publicAddresses.length === 0) {
            const denied = new Error("Target hostname resolves only to private addresses");
            denied.code = "EACCES";
            callback(denied);
            return;
        }
        if (returnAll) {
            callback(null, publicAddresses);
        }
        else {
            callback(null, publicAddresses[0].address, publicAddresses[0].family);
        }
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
    if (requestUrl.pathname === "/health" || requestUrl.pathname === relayNodeManifestPath) {
        handleRelayNodeManifest(request, response, requestUrl);
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
    traceTunnelEvent(`proxy ${proxyKind} request target=${target.origin}${target.pathname}`);
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
    const proxyClient = proxyKind === "resource-pack"
        ? watchProxyClient(request, response)
        : undefined;
    const upstreamRequest = {
        method,
        headers: createUpstreamHeaders(request, proxyKind),
        ...(body === undefined ? {} : { body }),
        ...(proxyClient === undefined ? {} : { signal: proxyClient.signal }),
        redirect: "manual",
    };
    const maximumBytes = proxyKind === "resource-pack"
        ? maximumResourcePackBytes
        : proxyKind === "texture"
            ? maximumTextureBytes
            : proxyKind === "realms"
                ? maximumRealmsResponseBytes
                : maximumAuthResponseBytes;
    let resourcePackDownload;
    let upstream;
    try {
        if (proxyKind === "resource-pack") {
            resourcePackDownload = await acquireResourcePackDownload(
                target, upstreamRequest, maximumBytes);
            upstream = resourcePackDownload.upstream;
        }
        else {
            upstream = await fetchWithValidatedRedirects(target, upstreamRequest, proxyKind);
        }
    }
    catch (error) {
        proxyClient?.dispose();
        if (proxyClient?.signal.aborted || error instanceof ProxyClientDisconnectedError) {
            return;
        }
        if (error instanceof ProxyResponseSizeError) {
            response.writeHead(413, { ...corsHeaders, "content-type": "text/plain; charset=utf-8" });
            response.end(error.message);
            return;
        }
        throw error;
    }
    try {
        const declaredLength = Number(upstream.headers.get("content-length"));
        traceTunnelEvent(
            `proxy ${proxyKind} response status=${upstream.status} length=`
                + `${Number.isFinite(declaredLength) ? declaredLength : "unknown"}`
        );
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
            ...(resourcePackDownload === undefined
                ? {}
                : { "content-length": String(resourcePackDownload.byteLength) }),
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
        let received = 0;
        const responseBody = resourcePackDownload?.path === undefined
            ? upstream.body
            : createReadStream(resourcePackDownload.path);
        if (responseBody !== null) {
            for await (const chunk of responseBody) {
                received += chunk.byteLength;
                if (received > maximumBytes) {
                    response.destroy(new Error("Proxy response exceeded size limit"));
                    return;
                }
                await writeHttpChunk(response, chunk);
            }
        }
        response.end();
        traceTunnelEvent(`proxy ${proxyKind} complete bytes=${received}`);
    }
    finally {
        await releaseResourcePackDownload(resourcePackDownload);
        proxyClient?.dispose();
    }
}
function handleRelayNodeManifest(request, response, requestUrl) {
    pruneResourcePackCache();
    const origin = request.headers.origin;
    if (origin !== undefined && !isOriginAllowed(origin, config.allowedOrigins)) {
        response.writeHead(403, { "content-type": "text/plain; charset=utf-8" });
        response.end("Origin is not allowed");
        return;
    }
    const method = request.method ?? "GET";
    if (method === "OPTIONS") {
        response.writeHead(204, {
            ...(origin === undefined ? {} : createCorsHeaders(origin)),
            "access-control-max-age": "600",
        });
        response.end();
        return;
    }
    if (method !== "GET" && method !== "HEAD") {
        response.writeHead(405, { allow: "GET, HEAD, OPTIONS" });
        response.end();
        return;
    }
    const targetHost = requestUrl.searchParams.get("host");
    const targetPortText = requestUrl.searchParams.get("port");
    let target;
    if (targetHost !== null || targetPortText !== null) {
        if (targetHost === null || targetPortText === null) {
            response.writeHead(400, { "content-type": "text/plain; charset=utf-8" });
            response.end("Target host and port must be supplied together");
            return;
        }
        const authorization = request.headers.authorization;
        const suppliedToken = typeof authorization === "string" &&
            authorization.startsWith("Bearer ")
            ? authorization.slice("Bearer ".length)
            : undefined;
        if (!tokenMatches(suppliedToken, config.accessToken)) {
            response.writeHead(403, { "content-type": "text/plain; charset=utf-8" });
            response.end("Invalid relay token");
            return;
        }
        const port = Number(targetPortText);
        try {
            target = parseConnectRequest(JSON.stringify({
                type: "connect",
                host: targetHost,
                port,
            }));
        }
        catch (error) {
            response.writeHead(400, { "content-type": "text/plain; charset=utf-8" });
            response.end(error instanceof Error ? error.message : "Invalid target");
            return;
        }
        if (!isHostAllowed(target.host, config.allowedHosts)) {
            response.writeHead(403, { "content-type": "text/plain; charset=utf-8" });
            response.end("Destination is not allowed");
            return;
        }
    }
    const activeConnections = webSocketServer.clients.size;
    const body = JSON.stringify({
        ok: true,
        kind: "gaius-relay-node",
        protocolVersion: relayNodeProtocolVersion,
        name: config.relayName,
        tunnelPath: "/tunnel",
        tunnelLease: {
            scope: "websocket",
            protocolStreamsShared: false,
            releasedOn: "websocket-close",
        },
        activeConnections,
        maximumConnections: config.maximumConnections,
        availableConnections: Math.max(0, config.maximumConnections - activeConnections),
        maximumFrameBytes: config.maximumFrameBytes,
        targetConnectTimeoutMs: config.connectTimeoutMs,
        requiresToken: config.accessToken !== undefined,
        allowsPrivateTargets: config.allowPrivateTargets,
        targetAffinityMs: config.targetAffinityMs,
        resourcePackCache: {
            entries: resourcePackCache.size,
            bytes: resourcePackCacheBytes,
            maximumBytes: config.maximumResourcePackCacheBytes,
            ttlMs: config.resourcePackCacheMs,
        },
        ...(target === undefined ? {} : { target: targetRouteSnapshot(target) }),
        registration: {
            configured: relayRegistrationState.configured,
            registered: relayRegistrationState.registered,
            attempts: relayRegistrationState.attempts,
            successes: relayRegistrationState.successes,
            failures: relayRegistrationState.failures,
            lastSuccessAt: relayRegistrationState.lastSuccessAt,
            lastError: relayRegistrationState.lastError,
        },
        capabilities: relayCapabilities,
    });
    const headers = {
        "cache-control": "no-store",
        "content-type": "application/json; charset=utf-8",
        ...(origin === undefined ? {} : createCorsHeaders(origin)),
    };
    response.writeHead(200, headers);
    response.end(method === "HEAD" ? undefined : body);
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
        "access-control-expose-headers": "content-type, content-length, content-disposition, retry-after",
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
    else if (proxyKind === "resource-pack" &&
        !isHostAllowed(host, config.allowedResourcePackHosts)) {
        throw new Error("Resource-pack target is not allowed");
    }
    return target;
}
function watchProxyClient(request, response) {
    const controller = new AbortController();
    let disposed = false;
    const abort = () => {
        if (disposed || response.writableEnded || controller.signal.aborted) {
            return;
        }
        controller.abort(new ProxyClientDisconnectedError());
    };
    request.once("aborted", abort);
    response.once("close", abort);
    return {
        signal: controller.signal,
        dispose() {
            if (disposed) {
                return;
            }
            disposed = true;
            request.off("aborted", abort);
            response.off("close", abort);
        },
    };
}
function throwIfProxyClientDisconnected(signal) {
    if (!signal?.aborted) {
        return;
    }
    if (signal.reason instanceof ProxyClientDisconnectedError) {
        throw signal.reason;
    }
    throw new ProxyClientDisconnectedError();
}
function resourcePackCacheKey(target, init) {
    const headers = Array.from(new Headers(init.headers).entries());
    headers.sort((left, right) => left[0].localeCompare(right[0]) ||
        left[1].localeCompare(right[1]));
    return JSON.stringify([target.href, headers]);
}
function resourcePackCacheEnabled() {
    return config.resourcePackCacheMs > 0 &&
        config.maximumResourcePackCacheBytes > 0 &&
        config.maximumResourcePackCacheEntries > 0;
}
function deleteResourcePackCacheFile(entry) {
    if (entry.deleted || entry.path === undefined) {
        return;
    }
    entry.deleted = true;
    void removeResourcePackTemporaryFile(entry.path);
}
function cleanupResourcePackTemporaryFiles() {
    for (const path of resourcePackTemporaryPaths) {
        try {
            unlinkSync(path);
        }
        catch {
            // The file may already have been removed by an asynchronous release.
        }
    }
    resourcePackTemporaryPaths.clear();
}
async function removeResourcePackTemporaryFile(path) {
    if (path === undefined) {
        return;
    }
    await unlink(path).catch(() => undefined);
    resourcePackTemporaryPaths.delete(path);
}
function evictResourcePackCacheEntry(key, entry) {
    if (resourcePackCache.get(key) === entry) {
        resourcePackCache.delete(key);
        resourcePackCacheBytes = Math.max(0, resourcePackCacheBytes - entry.byteLength);
    }
    entry.evicted = true;
    if (entry.readers === 0) {
        deleteResourcePackCacheFile(entry);
    }
}
function pruneResourcePackCache(now = Date.now()) {
    for (const [key, entry] of resourcePackCache) {
        if (!resourcePackCacheEnabled() || now >= entry.expiresAt) {
            evictResourcePackCacheEntry(key, entry);
        }
    }
    while (resourcePackCache.size > config.maximumResourcePackCacheEntries ||
        resourcePackCacheBytes > config.maximumResourcePackCacheBytes) {
        let oldestKey;
        let oldestEntry;
        for (const [key, entry] of resourcePackCache) {
            if (oldestEntry === undefined || entry.lastAccessAt < oldestEntry.lastAccessAt) {
                oldestKey = key;
                oldestEntry = entry;
            }
        }
        if (oldestKey === undefined || oldestEntry === undefined) {
            break;
        }
        evictResourcePackCacheEntry(oldestKey, oldestEntry);
    }
}
function acquireCachedResourcePack(entry) {
    entry.readers++;
    entry.lastAccessAt = Date.now();
    return {
        upstream: {
            status: entry.status,
            headers: new Headers(entry.headers),
            body: null,
        },
        path: entry.path,
        byteLength: entry.byteLength,
        cacheEntry: entry,
        cacheHit: true,
    };
}
async function acquireResourcePackDownload(target, init, maximumBytes) {
    throwIfProxyClientDisconnected(init.signal);
    const key = resourcePackCacheKey(target, init);
    const now = Date.now();
    pruneResourcePackCache(now);
    const cached = resourcePackCache.get(key);
    if (cached !== undefined && now < cached.expiresAt && !cached.evicted) {
        traceTunnelEvent(`resource-pack cache hit bytes=${cached.byteLength}`);
        return acquireCachedResourcePack(cached);
    }
    const download = await downloadResourcePackWithRetries(target, init, maximumBytes);
    throwIfProxyClientDisconnected(init.signal);
    const cacheable = resourcePackCacheEnabled() &&
        download.path !== undefined &&
        download.byteLength > 0 &&
        download.byteLength <= config.maximumResourcePackCacheBytes &&
        download.upstream.status === 200;
    if (!cacheable) {
        return { ...download, deleteAfterUse: download.path !== undefined };
    }
    const incumbent = resourcePackCache.get(key);
    if (incumbent !== undefined && Date.now() < incumbent.expiresAt && !incumbent.evicted) {
        await removeResourcePackTemporaryFile(download.path);
        return acquireCachedResourcePack(incumbent);
    }
    const storedAt = Date.now();
    const entry = {
        key,
        path: download.path,
        byteLength: download.byteLength,
        status: download.upstream.status,
        headers: Array.from(download.upstream.headers.entries()),
        storedAt,
        lastAccessAt: storedAt,
        expiresAt: storedAt + config.resourcePackCacheMs,
        readers: 1,
        evicted: false,
        deleted: false,
    };
    resourcePackCache.set(key, entry);
    resourcePackCacheBytes += entry.byteLength;
    pruneResourcePackCache(storedAt);
    traceTunnelEvent(
        `resource-pack cache store bytes=${entry.byteLength} entries=${resourcePackCache.size}`);
    return { ...download, cacheEntry: entry, cacheHit: false };
}
async function releaseResourcePackDownload(download) {
    if (download === undefined) {
        return;
    }
    if (download.cacheEntry !== undefined) {
        const entry = download.cacheEntry;
        entry.readers = Math.max(0, entry.readers - 1);
        pruneResourcePackCache();
        if (entry.evicted && entry.readers === 0) {
            deleteResourcePackCacheFile(entry);
        }
        return;
    }
    if (download.deleteAfterUse && download.path !== undefined) {
        await removeResourcePackTemporaryFile(download.path);
    }
}
async function downloadResourcePackWithRetries(target, init, maximumBytes) {
    let lastError;
    for (let attempt = 0; attempt < resourcePackBodyAttempts; attempt++) {
        let upstream;
        let temporary;
        try {
            throwIfProxyClientDisconnected(init.signal);
            upstream = await fetchWithValidatedRedirects(target, init, "resource-pack");
            temporary = await spoolResponseBody(upstream.body, maximumBytes);
            throwIfProxyClientDisconnected(init.signal);
            traceTunnelEvent(
                `resource-pack body ready bytes=${temporary.byteLength} attempt=${attempt + 1}`);
            return { upstream, ...temporary };
        }
        catch (error) {
            lastError = error;
            await upstream?.body?.cancel("Retrying interrupted resource-pack body").catch(() => undefined);
            if (temporary?.path !== undefined) {
                await removeResourcePackTemporaryFile(temporary.path);
            }
            throwIfProxyClientDisconnected(init.signal);
            if (error instanceof ProxyResponseSizeError || attempt + 1 >= resourcePackBodyAttempts) {
                throw error;
            }
            traceTunnelEvent(
                `retrying interrupted resource-pack body attempt=${attempt + 1} error=`
                    + `${error instanceof Error ? error.message : String(error)}`);
            await new Promise((resolve) => setTimeout(resolve, 250 * (1 << attempt)));
            throwIfProxyClientDisconnected(init.signal);
        }
    }
    throw lastError ?? new Error("Resource-pack body download exhausted all retries");
}
async function spoolResponseBody(body, maximumBytes) {
    if (body === null) {
        return { path: undefined, byteLength: 0 };
    }
    const path = join(
        tmpdir(), `gaius-relay-resource-pack-${process.pid}-${randomUUID()}.tmp`);
    const file = await open(path, "wx", 0o600);
    resourcePackTemporaryPaths.add(path);
    let byteLength = 0;
    try {
        for await (const chunk of body) {
            const buffer = Buffer.isBuffer(chunk)
                ? chunk
                : Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
            byteLength += buffer.byteLength;
            if (byteLength > maximumBytes) {
                throw new ProxyResponseSizeError();
            }
            let offset = 0;
            while (offset < buffer.byteLength) {
                const result = await file.write(
                    buffer, offset, buffer.byteLength - offset, null);
                if (result.bytesWritten < 1) {
                    throw new Error("Resource-pack temporary file stopped accepting data");
                }
                offset += result.bytesWritten;
            }
        }
        await file.close();
        return { path, byteLength };
    }
    catch (error) {
        await file.close().catch(() => undefined);
        await removeResourcePackTemporaryFile(path);
        throw error;
    }
}
async function writeHttpChunk(response, chunk) {
    if (response.destroyed) {
        throw new Error("Proxy client disconnected");
    }
    if (response.write(chunk)) {
        return;
    }
    await new Promise((resolve, reject) => {
        const cleanup = () => {
            response.off("drain", drained);
            response.off("close", closed);
            response.off("error", failed);
        };
        const drained = () => {
            cleanup();
            resolve();
        };
        const closed = () => {
            cleanup();
            reject(new Error("Proxy client disconnected"));
        };
        const failed = (error) => {
            cleanup();
            reject(error);
        };
        response.once("drain", drained);
        response.once("close", closed);
        response.once("error", failed);
    });
}
async function fetchWithValidatedRedirects(initialTarget, init, proxyKind) {
    let target = initialTarget;
    for (let redirect = 0; redirect <= 5; redirect++) {
        throwIfProxyClientDisconnected(init.signal);
        let response;
        const retryable = init.method === "GET" &&
            (proxyKind === "resource-pack" || proxyKind === "texture" ||
                proxyKind === "auth" || proxyKind === "realms");
        for (let attempt = 0; attempt < 3; attempt++) {
            try {
                response = await fetch(target, init);
                if (!retryable || ![429, 502, 503, 504].includes(response.status) || attempt === 2) {
                    break;
                }
                await response.body?.cancel("Retrying transient proxy response");
                traceTunnelEvent(`retrying ${proxyKind} status=${response.status} attempt=${attempt + 1}`);
            }
            catch (error) {
                throwIfProxyClientDisconnected(init.signal);
                if (!retryable || attempt === 2) {
                    throw error;
                }
                traceTunnelEvent(`retrying ${proxyKind} fetch attempt=${attempt + 1}`);
            }
            await new Promise((resolve) => setTimeout(resolve, 250 * (1 << attempt)));
            throwIfProxyClientDisconnected(init.signal);
        }
        if (response === undefined) {
            throw new Error(`${proxyKind} request exhausted all retries`);
        }
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
class ProxyClientDisconnectedError extends Error {
    constructor() {
        super("Proxy client disconnected");
        this.name = "ProxyClientDisconnectedError";
    }
}
class ProxyResponseSizeError extends Error {
    constructor() {
        super("Proxy response exceeded size limit");
        this.name = "ProxyResponseSizeError";
    }
}
