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
import { resolveMinecraftProfile } from "./protocol.js";
import { MinecraftFrameAccumulator } from "./framed-stream.js";
const config = loadConfig();
const traceTunnel = process.env.GAIUS_TRACE_TUNNEL === "1";
const dnsTestScenario = process.env.NODE_ENV === "test"
    ? {
        lookupHost: process.env.GAIUS_DNS_TEST_LOOKUP_HOST,
        lookupPermanentHost: process.env.GAIUS_DNS_TEST_LOOKUP_PERMANENT_HOST,
        lookupAddress: process.env.GAIUS_DNS_TEST_LOOKUP_ADDRESS ?? "127.0.0.1",
        lookupFailures: parseDnsTestInteger(process.env.GAIUS_DNS_TEST_LOOKUP_FAILURES),
        srvHost: process.env.GAIUS_DNS_TEST_SRV_HOST,
        srvPermanentHost: process.env.GAIUS_DNS_TEST_SRV_PERMANENT_HOST,
        srvTarget: process.env.GAIUS_DNS_TEST_SRV_TARGET ?? "127.0.0.1",
        srvPort: parseDnsTestInteger(process.env.GAIUS_DNS_TEST_SRV_PORT),
        srvFailures: parseDnsTestInteger(process.env.GAIUS_DNS_TEST_SRV_FAILURES),
    }
    : undefined;
let dnsTestLookupAttempts = 0;
let dnsTestSrvAttempts = 0;
const relayNodeProtocolVersion = 1;
const relayNodeManifestPath = "/relay-node/v1";
const maximumResourcePackBytes = 250 * 1024 * 1024;
const maximumTextureBytes = 16 * 1024 * 1024;
const maximumAuthResponseBytes = 4 * 1024 * 1024;
const maximumAuthRequestBytes = 1024 * 1024;
const maximumRealmsResponseBytes = 16 * 1024 * 1024;
const maximumRealmsRequestBytes = 4 * 1024 * 1024;
const maximumWebSocketBufferedBytes = 4 * 1024 * 1024;
// A PLAY/chunk burst must not monopolize the RelayNode event loop while a
// single TCP data callback drains every complete frame.  Frames remain
// ordered; the existing setImmediate continuation carries the remainder.
const maximumServerFrameDrainFrames = 32;
const maximumServerFrameDrainBytes = 512 * 1024;
const maximumServerFrameDrainMillis = 2;
// Handshakes are tiny (the host field is capped at 255 bytes), but a generic
// TCP tunnel can arrive one WebSocket message at a time. Keep only a bounded
// sniffing buffer and fall back to opaque forwarding once it is exceeded.
const maximumMinecraftHandshakeBytes = 4 * 1024;
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
    "target-attestation",
    "runtime-telemetry",
];
// `ServerboundClientTickEndPacket` is emitted once per client tick. A resource
// or model reload can temporarily stop browser ticks, while a spawn proxy can
// require the next tick before its short read timeout. The relay can synthesize
// the profile-specific payloadless packet as soon as configuration enters PLAY,
// then switches to the exact frame observed from the browser.
const stalledClientTickIntervalMs = 50;
const stalledClientTickGraceMs = 100;

function parseDnsTestInteger(value) {
    const parsed = Number.parseInt(value ?? "", 10);
    return Number.isInteger(parsed) && parsed >= 0 ? parsed : 0;
}

const localTunnelSessions = new Map();
const targetRoutes = new Map();
// Public target connects can arrive in one browser turn when several players
// join the same server. Coalesce their DNS lookups and retain only filtered
// public addresses for a short window. Failed lookups are never cached, and
// the short TTL limits the impact of DNS changes or rebinding.
const publicDnsCache = new Map();
const publicDnsCacheTtlMs = 5_000;
const maximumPublicDnsCacheEntries = 1024;
let publicDnsCacheHits = 0;
let publicDnsCacheMisses = 0;
let publicDnsCacheInflightJoins = 0;
const resourcePackCache = new Map();
const resourcePackTemporaryPaths = new Set();
let resourcePackCacheBytes = 0;
const relayStartedAt = Date.now();
let activeClientStallTimers = 0;
let syntheticPlayTickWrites = 0;
let syntheticPlayTickBackpressureEvents = 0;
let syntheticPlayTickMaxWritableLength = 0;
let pendingSyntheticPlayTicks = 0;
let maxPendingSyntheticPlayTicks = 0;
let activeServerFrameDrainHandles = 0;
// Server -> WebSocket framing telemetry is aggregate process state. The
// enqueued/error/cleanup counters survive tunnel cleanup so the manifest
// remains useful for a bounded smoke/operations window. Gauge fields are
// updated exactly; an underflow is counted rather than silently clamped.
const serverFrameTelemetry = {
    enqueuedFrames: 0,
    enqueuedBytes: 0,
    sendErrors: 0,
    cleanupBytes: 0,
    bufferedUnderflows: 0,
    bufferedUnderflowBytes: 0,
    drainHandleUnderflows: 0,
    dataCallbacks: 0,
    scheduledDrains: 0,
    drainCompletions: 0,
    dataCallbacksAtPause: 0,
    dataCallbacksAtDrainStart: 0,
    dataCallbacksAtDrainCompletion: 0,
    appendedChunks: 0,
    coalescedFrames: 0,
    coalescedBytes: 0,
    retainedCompleteFrames: 0,
    maxRetainedCompleteFrames: 0,
    pauses: 0,
    resumes: 0,
    framesAfterPause: 0,
    maxBufferedAmount: 0,
    bufferedFrameBytes: 0,
    maxBufferedFrameBytes: 0,
    drainBudgetYields: 0,
    maxDrainFrames: 0,
    maxDrainBytes: 0,
    maxDrainDurationMillis: 0,
};
const clientFrameTelemetry = {
    appendedChunks: 0,
    coalescedFrames: 0,
    coalescedBytes: 0,
};
const serverFrameForwardResult = Object.freeze({
    ENQUEUED: "enqueued",
    ENQUEUED_PAUSED: "enqueued-paused",
    PAUSED: "paused",
    CLOSED: "closed",
    ERROR: "error",
});
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
let stopRelayRegistration = async () => {};
let shutdownStarted = false;
process.once("exit", cleanupResourcePackTemporaryFiles);
process.once("SIGINT", () => void shutdownRelayNode(130));
process.once("SIGTERM", () => void shutdownRelayNode(143));
// Windows cannot deliver SIGTERM across process boundaries (child.kill("SIGTERM")
// hard-terminates without running this process' handlers). Accept a
// "graceful-shutdown" line on stdin as a cross-platform equivalent so
// orchestration and smoke tests can still exercise the graceful unregister path.
process.stdin.setEncoding("utf8");
let stdinBuffer = "";
process.stdin.on("data", (chunk) => {
    stdinBuffer += chunk;
    let newline;
    while ((newline = stdinBuffer.indexOf("\n")) !== -1) {
        const line = stdinBuffer.slice(0, newline).trim();
        stdinBuffer = stdinBuffer.slice(newline + 1);
        if (line === "graceful-shutdown") {
            void shutdownRelayNode(0);
            return;
        }
    }
});
process.stdin.resume();

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
// backend read timeout without delaying arbitrary game packets. The packet ids
// come from the profile selected by the client's handshake.
function proxyVanillaKeepAlive(socket, frame, protocolPhase, profile) {
    if (!config.proxyKeepAlives || profile === undefined || frame.byteLength !== 11 ||
        frame[0] !== 0x0a || frame[1] !== 0x00) {
        return false;
    }
    const packetId = frame[2];
    let responsePacketId;
    if ((protocolPhase === "login" || protocolPhase === "configuration" ||
        protocolPhase === "reconfiguring") &&
        packetId === profile.configuration.clientboundKeepAlive) {
        // Configuration uses the common keepalive packet id in both directions.
        responsePacketId = profile.configuration.serverboundKeepAlive;
    }
    else if (protocolPhase === "play" &&
        packetId === profile.play.clientboundKeepAlive) {
        // PLAY has different clientbound/serverbound packet registries.
        responsePacketId = profile.play.serverboundKeepAlive;
    }
    else {
        return false;
    }
    const response = Buffer.from(frame);
    response[2] = responsePacketId;
    socket.write(response);
    if (traceTunnel) {
        traceTunnelEvent(
            `proxied ${packetId === profile.configuration.clientboundKeepAlive
                ? "configuration"
                : "play"} keepalive `
                + `head=${response.toString("hex")}`
        );
    }
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

function decodeMinecraftVarInt(bytes, offset = 0) {
    let value = 0;
    for (let index = 0; index < 5; index++) {
        if (offset + index >= bytes.byteLength) {
            return undefined;
        }
        const current = bytes[offset + index];
        value |= (current & 0x7f) << (index * 7);
        if ((current & 0x80) === 0) {
            return { value: value >>> 0, bytesRead: index + 1 };
        }
    }
    return null;
}

function encodeMinecraftVarInt(value) {
    if (!Number.isSafeInteger(value) || value < 0 || value > 0x7fffffff) {
        throw new RangeError(`Minecraft packet id must be a non-negative 31-bit integer, got ${value}`);
    }
    const bytes = [];
    do {
        let current = value & 0x7f;
        value >>>= 7;
        if (value !== 0) {
            current |= 0x80;
        }
        bytes.push(current);
    } while (value !== 0);
    return Buffer.from(bytes);
}

function createPayloadlessMinecraftFrame(packetId, compressed) {
    const packet = encodeMinecraftVarInt(packetId);
    const body = compressed
        ? Buffer.concat([encodeMinecraftVarInt(0), packet])
        : packet;
    return Buffer.concat([encodeMinecraftVarInt(body.byteLength), body]);
}

function inspectMinecraftHandshake(frame) {
    const parsed = readMinecraftFrame(frame);
    if (parsed === undefined || parsed === null) {
        return {
            state: parsed === undefined ? "incomplete" : "opaque",
        };
    }
    const packetOffset = parsed.headerBytes;
    const packetId = decodeMinecraftVarInt(parsed.frame, packetOffset);
    if (packetId === undefined || packetId === null || packetId.value !== 0) {
        return { state: "opaque" };
    }
    let offset = packetOffset + packetId.bytesRead;
    const protocolVersion = decodeMinecraftVarInt(parsed.frame, offset);
    if (protocolVersion === undefined || protocolVersion === null) {
        return { state: "opaque" };
    }
    offset += protocolVersion.bytesRead;
    const hostLength = decodeMinecraftVarInt(parsed.frame, offset);
    if (hostLength === undefined || hostLength === null ||
        hostLength.value > 255) {
        return { state: "opaque" };
    }
    offset += hostLength.bytesRead;
    const hostEnd = offset + hostLength.value;
    if (hostEnd + 2 > parsed.frame.byteLength) {
        return { state: "opaque" };
    }
    offset = hostEnd + 2;
    const nextState = decodeMinecraftVarInt(parsed.frame, offset);
    if (nextState === undefined || nextState === null ||
        (nextState.value !== 1 && nextState.value !== 2) ||
        offset + nextState.bytesRead !== parsed.frame.byteLength) {
        return { state: "opaque" };
    }
    return {
        state: "complete",
        handshake: {
            protocolVersion: protocolVersion.value,
            profile: resolveMinecraftProfile(protocolVersion.value),
            remainder: parsed.remainder,
        },
    };
}

function parseMinecraftHandshake(frame) {
    const result = inspectMinecraftHandshake(frame);
    return result.state === "complete" ? result.handshake : undefined;
}

function isDefinitelyNotMinecraftHandshake(buffer) {
    const length = decodeMinecraftVarInt(buffer, 0);
    if (length === undefined || length === null) {
        return length === null;
    }
    const packetId = decodeMinecraftVarInt(buffer, length.bytesRead);
    return packetId !== undefined && packetId !== null && packetId.value !== 0;
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

function traceCustomPayload(frame, headerBytes, direction, playPhase, profile) {
    if (!traceTunnel || !playPhase || frame.byteLength <= headerBytes + 2) {
        return;
    }
    // Compressed packet framing adds one zero byte before the packet id when
    // the packet remains below the compression threshold. Only inspect that
    // small, plaintext form; encrypted/compressed traffic stays opaque.
    const packetOffset = frame[headerBytes] === 0x00 ? headerBytes + 1 : headerBytes;
    const packetId = frame[packetOffset];
    const expectedPacketId = direction === "server"
        ? profile.play.clientboundCustomPayload
        : profile.play.serverboundCustomPayload;
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
function relayRuntimeSnapshot() {
    const memory = process.memoryUsage();
    const cpu = process.cpuUsage();
    return {
        uptimeMillis: Math.max(0, Date.now() - relayStartedAt),
        activeClientStallTimers,
        activeLocalTunnelSessions: localTunnelSessions.size,
        syntheticPlayTickWrites,
        syntheticPlayTickBackpressureEvents,
        syntheticPlayTickMaxWritableLength,
        pendingSyntheticPlayTicks,
        maxPendingSyntheticPlayTicks,
        activeServerFrameDrainHandles,
        activeServerFrameDrainTimers: activeServerFrameDrainHandles,
        serverFrameBackpressure: {
            ...serverFrameTelemetry,
            // Compatibility aliases for the first P0 telemetry shape.
            sendFrames: serverFrameTelemetry.enqueuedFrames,
            sendBytes: serverFrameTelemetry.enqueuedBytes,
        },
        // Keep the scalar names easy to consume from existing health probes;
        // their established "sent" spelling means successfully enqueued.
        serverFramesSent: serverFrameTelemetry.enqueuedFrames,
        serverFrameBytesSent: serverFrameTelemetry.enqueuedBytes,
        serverFrameEnqueuedFrames: serverFrameTelemetry.enqueuedFrames,
        serverFrameEnqueuedBytes: serverFrameTelemetry.enqueuedBytes,
        serverFrameSendErrors: serverFrameTelemetry.sendErrors,
        serverFrameCleanupBytes: serverFrameTelemetry.cleanupBytes,
        serverFrameBufferedUnderflows: serverFrameTelemetry.bufferedUnderflows,
        serverFrameBufferedUnderflowBytes: serverFrameTelemetry.bufferedUnderflowBytes,
        activeServerFrameDrainHandleUnderflows: serverFrameTelemetry.drainHandleUnderflows,
        serverFrameDataCallbacks: serverFrameTelemetry.dataCallbacks,
        serverFrameScheduledDrains: serverFrameTelemetry.scheduledDrains,
        serverFrameDrainCompletions: serverFrameTelemetry.drainCompletions,
        serverFrameDataCallbacksAtPause: serverFrameTelemetry.dataCallbacksAtPause,
        serverFrameDataCallbacksAtDrainStart: serverFrameTelemetry.dataCallbacksAtDrainStart,
        serverFrameDataCallbacksAtDrainCompletion:
            serverFrameTelemetry.dataCallbacksAtDrainCompletion,
        serverFrameAppendedChunks: serverFrameTelemetry.appendedChunks,
        serverFrameCoalescedFrames: serverFrameTelemetry.coalescedFrames,
        serverFrameCoalescedBytes: serverFrameTelemetry.coalescedBytes,
        clientFrameAppendedChunks: clientFrameTelemetry.appendedChunks,
        clientFrameCoalescedFrames: clientFrameTelemetry.coalescedFrames,
        clientFrameCoalescedBytes: clientFrameTelemetry.coalescedBytes,
        publicDnsCacheEntries: publicDnsCache.size,
        publicDnsCacheHits,
        publicDnsCacheMisses,
        publicDnsCacheInflightJoins,
        serverFrameBufferedCompleteFrames: serverFrameTelemetry.retainedCompleteFrames,
        serverFrameMaxBufferedCompleteFrames: serverFrameTelemetry.maxRetainedCompleteFrames,
        serverFramePauses: serverFrameTelemetry.pauses,
        serverFrameResumes: serverFrameTelemetry.resumes,
        serverFramesAfterPause: serverFrameTelemetry.framesAfterPause,
        serverFrameMaxBufferedAmount: serverFrameTelemetry.maxBufferedAmount,
        serverFrameBufferedBytes: serverFrameTelemetry.bufferedFrameBytes,
        serverFrameMaxBufferedBytes: serverFrameTelemetry.maxBufferedFrameBytes,
        serverFrameDrainBudgetYields: serverFrameTelemetry.drainBudgetYields,
        serverFrameMaxDrainFrames: serverFrameTelemetry.maxDrainFrames,
        serverFrameMaxDrainBytes: serverFrameTelemetry.maxDrainBytes,
        serverFrameMaxDrainDurationMillis: serverFrameTelemetry.maxDrainDurationMillis,
        rssBytes: memory.rss,
        heapUsedBytes: memory.heapUsed,
        externalBytes: memory.external,
        cpuUserMicros: cpu.user,
        cpuSystemMicros: cpu.system,
    };
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
    // Minecraft status, login, keepalive, and play packets are predominantly
    // small frames. Disable Nagle on the browser-facing TCP leg as well as the
    // target leg below, otherwise the RelayNode can add an avoidable delayed
    // ACK/Nagle interval even when its queues are empty.
    socket.setNoDelay(true);
    socket.setKeepAlive(true, 30_000);
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
    // A low-water callback must not resume the TCP source until the retained
    // parser remainder has had its own guarded drain turn. This separate hold
    // keeps TCP paused while tcpPausedForWebSocket is cleared for the drain.
    let serverFrameDrainHoldingRead = false;
    let protocolPhase = "login";
    let configurationCycles = 0;
    const serverFrameBuffer = new MinecraftFrameAccumulator(config.maximumFrameBytes);
    let serverFrameRetainedCompleteFrames = 0;
    let serverFrameInFlightFrameBytes = 0;
    let serverFrameDrainHandle;
    let serverFrameDrainScheduled = false;
    let serverFrameDrainRunning = false;
    let serverFrameDrainRescheduleRequested = false;
    const clientFrameBuffer = new MinecraftFrameAccumulator(config.maximumFrameBytes);
    let minecraftHandshakeBuffer = Buffer.alloc(0);
    // The first Minecraft handshake selects the packet-id table. Do not assume
    // the legacy table before that handshake: an unknown or malformed profile
    // must stay an opaque raw TCP tunnel rather than receive a guessed rewrite.
    let minecraftProfile;
    let minecraftHandshakeSeen = false;
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
    let syntheticTickPending = false;
    let syntheticTickDrainListener;
    let tunnelCancelled = false;
    const tunnelConnectAbortController = new AbortController();
    let lastActivity = Date.now();
    const idleTimer = setInterval(() => {
        if (Date.now() - lastActivity > config.idleTimeoutMs) {
            closeBoth(1001, "Idle timeout");
        }
    }, Math.min(config.idleTimeoutMs, 5_000));
    idleTimer.unref();
    const clearSyntheticTickState = () => {
        if (syntheticTickDrainListener !== undefined && tcpSocket !== undefined) {
            tcpSocket.off("drain", syntheticTickDrainListener);
            syntheticTickDrainListener = undefined;
        }
        if (syntheticTickPending) {
            syntheticTickPending = false;
            pendingSyntheticPlayTicks = Math.max(0, pendingSyntheticPlayTicks - 1);
        }
    };
    const armSyntheticTickDrain = () => {
        if (syntheticTickDrainListener !== undefined || tcpSocket === undefined ||
            tcpSocket.destroyed) {
            return;
        }
        const socket = tcpSocket;
        syntheticTickDrainListener = () => {
            syntheticTickDrainListener = undefined;
            if (!syntheticTickPending) {
                return;
            }
            syntheticTickPending = false;
            pendingSyntheticPlayTicks = Math.max(0, pendingSyntheticPlayTicks - 1);
            if (connected && protocolPhase === "play" && playTickFrame !== undefined &&
                tcpSocket === socket && Date.now() - lastClientTrafficAt >= stalledClientTickGraceMs) {
                writeSyntheticPlayTick();
            }
        };
        socket.once("drain", syntheticTickDrainListener);
    };
    const writeSyntheticPlayTick = () => {
        if (!connected || protocolPhase !== "play" || playTickFrame === undefined ||
            tcpSocket === undefined || tcpSocket.destroyed || tcpSocket.writable === false) {
            return;
        }
        const writableLength = Number(tcpSocket.writableLength);
        if (Number.isFinite(writableLength)) {
            syntheticPlayTickMaxWritableLength = Math.max(
                syntheticPlayTickMaxWritableLength,
                writableLength,
            );
        }
        // A previous false write or an already asserted writableNeedDrain means
        // exactly one tick is pending. Do not enqueue a second copy on the next
        // interval; the drain callback sends it after the reader catches up.
        if (syntheticTickPending || tcpSocket.writableNeedDrain === true) {
            if (!syntheticTickPending) {
                syntheticTickPending = true;
                pendingSyntheticPlayTicks++;
                maxPendingSyntheticPlayTicks = Math.max(
                    maxPendingSyntheticPlayTicks,
                    pendingSyntheticPlayTicks,
                );
                syntheticPlayTickBackpressureEvents++;
            }
            armSyntheticTickDrain();
            return;
        }
        const accepted = tcpSocket.write(playTickFrame);
        syntheticPlayTickWrites++;
        const postWriteLength = Number(tcpSocket.writableLength);
        if (Number.isFinite(postWriteLength)) {
            syntheticPlayTickMaxWritableLength = Math.max(
                syntheticPlayTickMaxWritableLength,
                postWriteLength,
            );
        }
        lastClientTrafficAt = Date.now();
        traceTunnelEvent("proxied observed play tick while browser was stalled");
        if (!accepted || tcpSocket.writableNeedDrain === true) {
            syntheticTickPending = true;
            pendingSyntheticPlayTicks++;
            maxPendingSyntheticPlayTicks = Math.max(
                maxPendingSyntheticPlayTicks,
                pendingSyntheticPlayTicks,
            );
            syntheticPlayTickBackpressureEvents++;
            armSyntheticTickDrain();
        }
    };
    const clearClientStallTimer = () => {
        if (clientStallTimer === undefined) {
            clearSyntheticTickState();
            return;
        }
        clearInterval(clientStallTimer);
        clientStallTimer = undefined;
        activeClientStallTimers = Math.max(0, activeClientStallTimers - 1);
        clearSyntheticTickState();
    };
    const armClientStallTimer = () => {
        if (clientStallTimer !== undefined || playTickFrame === undefined) {
            return;
        }
        clientStallTimer = setInterval(() => {
            if (!connected || protocolPhase !== "play" || playTickFrame === undefined ||
                tcpSocket === undefined ||
                Date.now() - lastClientTrafficAt < stalledClientTickGraceMs) {
                return;
            }
            writeSyntheticPlayTick();
        }, stalledClientTickIntervalMs);
        clientStallTimer.unref();
        activeClientStallTimers++;
    };
    const closeBoth = (code, reason) => {
        traceTunnelEvent(`closing tunnel code=${code} reason=${reason}`);
        clearInterval(idleTimer);
        clearClientStallTimer();
        tunnelCancelled = true;
        clearServerFrameState();
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
        if (tcpPausedForWebSocket || tcpPausedForClient || serverFrameDrainHoldingRead) {
            tcpSocket.pause();
        }
        else {
            tcpSocket.resume();
        }
    };
    const countCompleteServerFrames = () => {
        if (!packetFramingEnabled) {
            return 0;
        }
        return serverFrameBuffer.countCompleteFrames(serverFrameInFlightFrameBytes);
    };
    const updateRetainedCompleteFrameTelemetry = (next = countCompleteServerFrames()) => {
        const nextCount = Number.isInteger(next) && next >= 0 ? next : 0;
        const aggregate = serverFrameTelemetry.retainedCompleteFrames +
            nextCount - serverFrameRetainedCompleteFrames;
        if (aggregate < 0) {
            serverFrameTelemetry.bufferedUnderflows++;
            serverFrameTelemetry.bufferedUnderflowBytes += -aggregate;
            serverFrameTelemetry.retainedCompleteFrames = 0;
        }
        else {
            serverFrameTelemetry.retainedCompleteFrames = aggregate;
        }
        serverFrameRetainedCompleteFrames = nextCount;
        serverFrameTelemetry.maxRetainedCompleteFrames = Math.max(
            serverFrameTelemetry.maxRetainedCompleteFrames,
            serverFrameTelemetry.retainedCompleteFrames,
        );
    };
    const syncServerFrameBufferTelemetry = () => {
        // The accumulator is the source of truth; consume/append operations
        // are committed only after ownership of a frame changes hands.
        serverFrameTelemetry.bufferedFrameBytes = serverFrameBuffer.byteLength;
        serverFrameTelemetry.maxBufferedFrameBytes = Math.max(
            serverFrameTelemetry.maxBufferedFrameBytes,
            serverFrameTelemetry.bufferedFrameBytes,
        );
    };
    const appendServerFrameBuffer = (chunk) => {
        const beforeChunks = serverFrameBuffer.appendedChunks;
        serverFrameBuffer.append(chunk);
        if (serverFrameBuffer.appendedChunks !== beforeChunks) {
            serverFrameTelemetry.appendedChunks +=
                serverFrameBuffer.appendedChunks - beforeChunks;
        }
        syncServerFrameBufferTelemetry();
    };
    const consumeServerFrameBuffer = (bytes) => {
        serverFrameBuffer.consume(bytes);
        syncServerFrameBufferTelemetry();
    };
    const clearServerFrameBuffer = () => {
        serverFrameBuffer.clear();
        syncServerFrameBufferTelemetry();
    };
    const observeServerFrameCoalescing = (parsed) => {
        if (parsed?.coalesced === true) {
            serverFrameTelemetry.coalescedFrames++;
            serverFrameTelemetry.coalescedBytes += parsed.frameBytes;
        }
    };
    const appendClientFrameBuffer = (chunk) => {
        const beforeChunks = clientFrameBuffer.appendedChunks;
        clientFrameBuffer.append(chunk);
        clientFrameTelemetry.appendedChunks +=
            clientFrameBuffer.appendedChunks - beforeChunks;
    };
    const observeClientFrameCoalescing = (parsed) => {
        if (parsed?.coalesced === true) {
            clientFrameTelemetry.coalescedFrames++;
            clientFrameTelemetry.coalescedBytes += parsed.frameBytes;
        }
    };
    const observeWebSocketBufferedAmount = () => {
        const bufferedAmount = Number(webSocket.bufferedAmount);
        if (Number.isFinite(bufferedAmount) && bufferedAmount >= 0) {
            serverFrameTelemetry.maxBufferedAmount = Math.max(
                serverFrameTelemetry.maxBufferedAmount,
                bufferedAmount,
            );
            return bufferedAmount;
        }
        return 0;
    };
    const pauseTcpForWebSocket = () => {
        if (tcpPausedForWebSocket) {
            return;
        }
        tcpPausedForWebSocket = true;
        serverFrameTelemetry.pauses++;
        serverFrameTelemetry.dataCallbacksAtPause = serverFrameTelemetry.dataCallbacks;
        updateRetainedCompleteFrameTelemetry();
        updateTcpReadState();
    };
    let drainServerFrameBuffer;
    const releaseServerFrameDrainHandle = () => {
        if (activeServerFrameDrainHandles <= 0) {
            serverFrameTelemetry.drainHandleUnderflows++;
            activeServerFrameDrainHandles = 0;
            return;
        }
        activeServerFrameDrainHandles--;
    };
    const scheduleServerFrameDrain = () => {
        if (serverFrameDrainRunning) {
            // A synchronous/fake WebSocket callback can clear the pause while
            // the parser is still on the stack. Remember exactly one retry for
            // the finally block instead of re-entering the parser.
            serverFrameDrainRescheduleRequested = true;
            return;
        }
        if (serverFrameDrainScheduled || tunnelCancelled || !connected || tcpSocket === undefined ||
            tcpSocket.destroyed || webSocket.readyState !== WebSocket.OPEN ||
            tcpPausedForWebSocket || tcpPausedForClient || serverFrameBuffer.byteLength === 0) {
            return;
        }
        serverFrameDrainScheduled = true;
        activeServerFrameDrainHandles++;
        serverFrameDrainHandle = setImmediate(() => {
            serverFrameDrainHandle = undefined;
            serverFrameDrainScheduled = false;
            releaseServerFrameDrainHandle();
            serverFrameTelemetry.scheduledDrains++;
            drainServerFrameBuffer?.();
        });
    };
    const resumeServerFrameIfLowWater = () => {
        if (!tcpPausedForWebSocket || tunnelCancelled ||
            webSocket.readyState !== WebSocket.OPEN) {
            return false;
        }
        const bufferedAmount = observeWebSocketBufferedAmount();
        if (bufferedAmount >= maximumWebSocketBufferedBytes) {
            return false;
        }
        // Clear only the WebSocket high-water flag. If a parser remainder is
        // present, serverFrameDrainHoldingRead keeps the TCP source paused
        // until the next-turn drain has actually consumed it.
        tcpPausedForWebSocket = false;
        serverFrameTelemetry.resumes++;
        if (serverFrameBuffer.byteLength > 0) {
            serverFrameDrainHoldingRead = true;
            updateTcpReadState();
            scheduleServerFrameDrain();
        }
        else {
            serverFrameDrainHoldingRead = false;
            updateTcpReadState();
        }
        return true;
    };
    const clearServerFrameState = () => {
        if (serverFrameDrainHandle !== undefined) {
            clearImmediate(serverFrameDrainHandle);
            serverFrameDrainHandle = undefined;
            releaseServerFrameDrainHandle();
        }
        serverFrameDrainScheduled = false;
        serverFrameDrainRescheduleRequested = false;
        if (serverFrameBuffer.byteLength > 0) {
            serverFrameTelemetry.cleanupBytes += serverFrameBuffer.byteLength;
        }
        updateRetainedCompleteFrameTelemetry(0);
        clearServerFrameBuffer();
        tcpPausedForWebSocket = false;
        serverFrameDrainHoldingRead = false;
    };
    const forwardServerFrame = (frame) => {
        if (webSocket.readyState !== WebSocket.OPEN || tunnelCancelled) {
            return serverFrameForwardResult.CLOSED;
        }
        if (tcpPausedForWebSocket) {
            // This is an attempted parser send while paused. The parser should
            // normally break before reaching here; retaining the counter makes
            // regressions visible without allowing a duplicate frame to escape.
            serverFrameTelemetry.framesAfterPause++;
            return serverFrameForwardResult.PAUSED;
        }
        const frameBytes = frame.byteLength;
        observeWebSocketBufferedAmount();
        let sendCallbackError;
        try {
            webSocket.send(frame, { binary: true }, (error) => {
                if (error) {
                    sendCallbackError = error;
                    serverFrameTelemetry.sendErrors++;
                    const target = tunnelRequest === undefined
                        ? "unknown target"
                        : `${tunnelRequest.host}:${tunnelRequest.port}`;
                    console.error(`WebSocket send error for ${target}:`, error.message);
                }
                if (error && webSocket.readyState === WebSocket.OPEN) {
                    closeBoth(1011, "WebSocket send failed");
                    return;
                }
                // The callback and the post-send path both use the same
                // low-water transition so a synchronous callback cannot race
                // a later high-water assertion into a lost drain.
                resumeServerFrameIfLowWater();
            });
        }
        catch (error) {
            serverFrameTelemetry.sendErrors++;
            console.error("WebSocket send failed:", error instanceof Error ? error.message : error);
            closeBoth(1011, "WebSocket send failed");
            return serverFrameForwardResult.ERROR;
        }
        if (sendCallbackError !== undefined) {
            return serverFrameForwardResult.ERROR;
        }
        serverFrameTelemetry.enqueuedFrames++;
        serverFrameTelemetry.enqueuedBytes += frameBytes;
        const bufferedAmount = observeWebSocketBufferedAmount();
        let pausedByThisSend = false;
        if (bufferedAmount >= maximumWebSocketBufferedBytes) {
            pauseTcpForWebSocket();
            pausedByThisSend = true;
        }
        // This second call is intentional: ws test doubles can invoke the
        // callback synchronously before send() returns.
        resumeServerFrameIfLowWater();
        return pausedByThisSend || tcpPausedForWebSocket
            ? serverFrameForwardResult.ENQUEUED_PAUSED
            : serverFrameForwardResult.ENQUEUED;
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
                host: request.host,
                port: request.port,
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
                const candidate = tcpSocket.gaiusTarget ?? request;
                webSocket.send(JSON.stringify({
                    type: "connected",
                    host: request.host,
                    port: request.port,
                    candidateHost: candidate.host,
                    candidatePort: candidate.port,
                    remoteAddress: tcpSocket.remoteAddress ?? null,
                    remotePort: tcpSocket.remotePort ?? null,
                }));
                const isEnqueuedServerFrameResult = (result) =>
                    result === serverFrameForwardResult.ENQUEUED ||
                    result === serverFrameForwardResult.ENQUEUED_PAUSED;
                drainServerFrameBuffer = () => {
                    if (serverFrameDrainRunning || tunnelCancelled || !connected ||
                        tcpSocket === undefined || tcpSocket.destroyed ||
                        webSocket.readyState !== WebSocket.OPEN || tcpPausedForWebSocket ||
                        tcpPausedForClient) {
                        return;
                    }
                    const drainStartedWithReadHold = serverFrameDrainHoldingRead;
                    const drainStartedAt = performance.now();
                    let drainFrames = 0;
                    let drainBytes = 0;
                    let drainBudgetYielded = false;
                    if (drainStartedWithReadHold) {
                        serverFrameTelemetry.dataCallbacksAtDrainStart =
                            serverFrameTelemetry.dataCallbacks;
                    }
                    serverFrameDrainRunning = true;
                    try {
                        while (serverFrameBuffer.byteLength > 0 &&
                            !tcpPausedForWebSocket && !tcpPausedForClient && !tunnelCancelled) {
                            // Always process at least one frame, even when it is
                            // larger than the byte budget.  Subsequent frames
                            // yield to the event loop through setImmediate.
                            if (drainFrames > 0 &&
                                (drainFrames >= maximumServerFrameDrainFrames ||
                                    drainBytes >= maximumServerFrameDrainBytes ||
                                    performance.now() - drainStartedAt >=
                                        maximumServerFrameDrainMillis)) {
                                drainBudgetYielded = true;
                                serverFrameTelemetry.drainBudgetYields++;
                                break;
                            }
                            if (!packetFramingEnabled) {
                                const opaqueChunk = serverFrameBuffer.peekChunk();
                                if (opaqueChunk === undefined) {
                                    break;
                                }
                                if (proxyVanillaKeepAlive(
                                    tcpSocket,
                                    opaqueChunk,
                                    protocolPhase,
                                    minecraftProfile,
                                )) {
                                    consumeServerFrameBuffer(opaqueChunk.byteLength);
                                    continue;
                                }
                                const result = forwardServerFrame(opaqueChunk);
                                if (isEnqueuedServerFrameResult(result)) {
                                    // Ownership transfers to ws only after
                                    // send() accepted the bytes.
                                    consumeServerFrameBuffer(opaqueChunk.byteLength);
                                    drainFrames++;
                                    drainBytes += opaqueChunk.byteLength;
                                }
                                else if (result === serverFrameForwardResult.CLOSED ||
                                    result === serverFrameForwardResult.ERROR) {
                                    clearServerFrameState();
                                }
                                if (result !== serverFrameForwardResult.ENQUEUED) {
                                    break;
                                }
                                continue;
                            }
                            const parsed = serverFrameBuffer.peekFrame();
                            if (parsed === undefined) {
                                break;
                            }
                            if (parsed === null) {
                                // This is normally encrypted online-mode traffic.
                                // Keep the opaque bytes in order and send them as
                                // one frame once the current high-water pause clears.
                                packetFramingEnabled = false;
                                minecraftProfile = undefined;
                                clearClientStallTimer();
                                traceTunnelEvent("disabled keepalive proxy for opaque server traffic");
                                // Keep the accumulator intact and let the next
                                // loop iteration forward its original chunks.
                                continue;
                            }
                            if (proxyVanillaKeepAlive(
                                tcpSocket,
                                parsed.frame,
                                protocolPhase,
                                minecraftProfile,
                            )) {
                                observeServerFrameCoalescing(parsed);
                                consumeServerFrameBuffer(parsed.frameBytes);
                                drainFrames++;
                                drainBytes += parsed.frameBytes;
                                continue;
                            }
                            let result;
                            serverFrameInFlightFrameBytes = parsed.frameBytes;
                            try {
                                result = forwardServerFrame(parsed.frame);
                            }
                            finally {
                                serverFrameInFlightFrameBytes = 0;
                            }
                            if (isEnqueuedServerFrameResult(result)) {
                                // Do not consume the deque entry until the
                                // frame itself is confirmed enqueued.
                                observeServerFrameCoalescing(parsed);
                                consumeServerFrameBuffer(parsed.frameBytes);
                                drainFrames++;
                                drainBytes += parsed.frameBytes;
                                if (traceTunnel && protocolPhase === "play") {
                                    const packet = minecraftPacketId(parsed.frame, parsed.headerBytes);
                                    if (packet !== undefined) {
                                        lastServerPlayPacket =
                                            `0x${packet.id.toString(16)}/${parsed.frame.byteLength}`;
                                    }
                                }
                                if (protocolPhase === "play" &&
                                    isPayloadlessPacket(
                                        parsed.frame,
                                        parsed.headerBytes,
                                        minecraftProfile.play.clientboundStartConfiguration,
                                    )) {
                                    protocolPhase = "reconfiguring";
                                    clearClientStallTimer();
                                    traceTunnelEvent("server started PLAY to CONFIGURATION transition");
                                }
                                if (isLoginEncryptionRequest(parsed.frame, parsed.headerBytes)) {
                                    encryptionResponsePending = true;
                                }
                                traceCustomPayload(
                                    parsed.frame,
                                    parsed.headerBytes,
                                    "server",
                                    protocolPhase === "play",
                                    minecraftProfile,
                                );
                            }
                            else if (result === serverFrameForwardResult.PAUSED) {
                                // The complete frame and its remainder are
                                // still owned by the relay; retry in order.
                                break;
                            }
                            else {
                                clearServerFrameState();
                                break;
                            }
                            if (result !== serverFrameForwardResult.ENQUEUED) {
                                break;
                            }
                        }
                    }
                    finally {
                        serverFrameTelemetry.maxDrainFrames = Math.max(
                            serverFrameTelemetry.maxDrainFrames, drainFrames);
                        serverFrameTelemetry.maxDrainBytes = Math.max(
                            serverFrameTelemetry.maxDrainBytes, drainBytes);
                        serverFrameTelemetry.maxDrainDurationMillis = Math.max(
                            serverFrameTelemetry.maxDrainDurationMillis,
                            performance.now() - drainStartedAt,
                        );
                        serverFrameDrainRunning = false;
                        serverFrameTelemetry.drainCompletions++;
                        if (drainStartedWithReadHold) {
                            serverFrameTelemetry.dataCallbacksAtDrainCompletion =
                                serverFrameTelemetry.dataCallbacks;
                        }
                        if (drainStartedWithReadHold && serverFrameDrainHoldingRead &&
                            !tcpPausedForWebSocket &&
                            !tcpPausedForClient && !tunnelCancelled) {
                            updateRetainedCompleteFrameTelemetry();
                            // An active drain either exhausted the retained
                            // bytes or reached an incomplete frame. In both
                            // cases the next TCP data callback is required to
                            // make progress, so release the read hold now.
                            serverFrameDrainHoldingRead = false;
                            updateTcpReadState();
                        }
                        if (drainBudgetYielded) {
                            serverFrameDrainRescheduleRequested = true;
                        }
                        const rescheduleRequested = serverFrameDrainRescheduleRequested;
                        serverFrameDrainRescheduleRequested = false;
                        if (rescheduleRequested && !tcpPausedForWebSocket &&
                            !tunnelCancelled && serverFrameBuffer.byteLength > 0) {
                            scheduleServerFrameDrain();
                        }
                        if (serverFrameDrainHoldingRead && !tcpPausedForWebSocket &&
                            !tcpPausedForClient && !tunnelCancelled &&
                            serverFrameBuffer.byteLength === 0) {
                            updateRetainedCompleteFrameTelemetry(0);
                            serverFrameDrainHoldingRead = false;
                            updateTcpReadState();
                        }
                        // The accumulator is authoritative even when the drain
                        // started without a read hold (774 can reach this path
                        // through a fragmented TCP callback). Reconcile the
                        // bounded diagnostic counter after every drain so a
                        // fully consumed buffer cannot retain stale frames.
                        updateRetainedCompleteFrameTelemetry();
                    }
                };
                tcpSocket.on("data", (chunk) => {
                    serverFrameTelemetry.dataCallbacks++;
                    // Hex previews are diagnostic-only. Building them on every PLAY chunk
                    // needlessly taxes the RelayNode even when tunnel tracing is disabled.
                    if (traceTunnel) {
                        traceTunnelEvent(
                            `server data ${request.host}:${request.port} bytes=${chunk.byteLength} `
                                + `head=${chunk.subarray(0, 24).toString("hex")}`
                        );
                    }
                    lastActivity = Date.now();
                    if (!packetFramingEnabled && serverFrameBuffer.byteLength === 0 &&
                        !tcpPausedForWebSocket && !tcpPausedForClient) {
                        if (proxyVanillaKeepAlive(tcpSocket, chunk, protocolPhase, minecraftProfile)) {
                            return;
                        }
                        // There is no parser remainder in the opaque path, so a
                        // successful send needs no queue bookkeeping.
                        forwardServerFrame(chunk);
                        return;
                    }
                    appendServerFrameBuffer(chunk);
                    drainServerFrameBuffer();
                });
                tcpSocket.once("error", (error) => {
                    console.error("TCP tunnel error:", error.message);
                    closeBoth(1011, "TCP connection failed");
                });
                tcpSocket.once("close", (hadError) => {
                    if (traceTunnel) {
                        traceTunnelEvent(
                            `TCP closed ${request.host}:${request.port} hadError=${Boolean(hadError)} `
                                + `tunnelMs=${Date.now() - tunnelStartedAt} `
                                + `playMs=${playStartedAt === undefined ? "n/a" : Date.now() - playStartedAt} `
                                + `phase=${protocolPhase} configurationCycles=${configurationCycles} `
                                + `lastServerPlay=${lastServerPlayPacket ?? "n/a"} `
                                + `lastClientPlay=${lastClientPlayPacket ?? "n/a"}`
                        );
                    }
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
                        if (!tcpPausedForClient) {
                            scheduleServerFrameDrain();
                        }
                    }
                    catch {
                        closeBoth(1003, "Invalid tunnel control message");
                    }
                    return;
                }
                const clientData = toBuffer(data);
                lastClientTrafficAt = Date.now();
                if (traceTunnel) {
                    traceTunnelEvent(
                        `client data ${request.host}:${request.port} bytes=${clientData.byteLength} `
                            + `head=${clientData.subarray(0, 24).toString("hex")}`
                    );
                }
                let frameClientData = clientData;
                if (!packetFramingEnabled && config.proxyKeepAlives && !minecraftHandshakeSeen) {
                    let handshakeResult;
                    if (minecraftHandshakeBuffer.byteLength + clientData.byteLength >
                        maximumMinecraftHandshakeBytes) {
                        // The probe is only for profile selection. The bytes have
                        // already been queued exactly once below, so dropping the
                        // bounded copy here preserves opaque tunnel semantics.
                        handshakeResult = minecraftHandshakeBuffer.byteLength === 0 &&
                            isDefinitelyNotMinecraftHandshake(clientData)
                            ? { state: "raw" }
                            : { state: "opaque" };
                    }
                    else {
                        minecraftHandshakeBuffer = minecraftHandshakeBuffer.byteLength === 0
                            ? clientData
                            : Buffer.concat([minecraftHandshakeBuffer, clientData]);
                        handshakeResult = isDefinitelyNotMinecraftHandshake(minecraftHandshakeBuffer)
                            ? { state: "raw" }
                            : inspectMinecraftHandshake(minecraftHandshakeBuffer);
                    }
                    if (handshakeResult.state === "incomplete" || handshakeResult.state === "raw") {
                        // Keep forwarding the raw chunk below while retaining only
                        // the bounded sniffing copy for an incomplete next message.
                        frameClientData = undefined;
                        if (handshakeResult.state === "raw") {
                            // A non-Minecraft preamble is an opaque stream decision.
                            // End the one-shot probe so a later byte sequence cannot
                            // be promoted into a 774/776 profile handshake.
                            minecraftHandshakeSeen = true;
                            minecraftHandshakeBuffer = Buffer.alloc(0);
                        }
                    }
                    else if (handshakeResult.state === "complete") {
                        const handshake = handshakeResult.handshake;
                        minecraftHandshakeSeen = true;
                        minecraftHandshakeBuffer = Buffer.alloc(0);
                        frameClientData = handshake.remainder;
                        if (handshake.profile === undefined) {
                            // Unsupported versions remain a raw tunnel. A later
                            // supported-looking packet must never reopen framing.
                            minecraftProfile = undefined;
                            frameClientData = undefined;
                            traceTunnelEvent(
                                `disabled profile-aware rewrites for unsupported Minecraft protocol `
                                    + `${handshake.protocolVersion}`
                            );
                        }
                        else {
                            minecraftProfile = handshake.profile;
                            packetFramingEnabled = true;
                            traceTunnelEvent(
                                `enabled framed keepalive proxy for Minecraft ${minecraftProfile.name}`
                            );
                        }
                    }
                    else {
                        minecraftHandshakeSeen = true;
                        minecraftHandshakeBuffer = Buffer.alloc(0);
                        minecraftProfile = undefined;
                        frameClientData = undefined;
                        traceTunnelEvent(
                            "disabled profile-aware rewrites for opaque or malformed Minecraft traffic"
                        );
                    }
                }
                if (packetFramingEnabled && frameClientData !== undefined) {
                    // Handshake probing supplies only the post-handshake
                    // remainder so the raw handshake is never inspected twice.
                    // Appending keeps each WebSocket chunk owned by the deque.
                    appendClientFrameBuffer(frameClientData);
                    while (clientFrameBuffer.byteLength > 0) {
                        const parsed = clientFrameBuffer.peekFrame();
                        if (parsed === undefined) {
                            break;
                        }
                        if (parsed === null) {
                            packetFramingEnabled = false;
                            minecraftProfile = undefined;
                            clientFrameBuffer.clear();
                            clearClientStallTimer();
                            traceTunnelEvent("disabled keepalive proxy for opaque client traffic");
                            break;
                        }
                        // Commit parser ownership before publishing any state
                        // transition or packet observation.
                        clientFrameBuffer.consume(parsed.frameBytes);
                        observeClientFrameCoalescing(parsed);
                        if (protocolPhase === "login" &&
                            isPayloadlessPacket(
                                parsed.frame,
                                parsed.headerBytes,
                                minecraftProfile.login.serverboundLoginAcknowledged
                            )) {
                            protocolPhase = "configuration";
                            traceTunnelEvent("login acknowledged; entered CONFIGURATION");
                        }
                        else if (protocolPhase === "configuration" &&
                            isPayloadlessPacket(
                                parsed.frame,
                                parsed.headerBytes,
                                minecraftProfile.configuration.serverboundFinish
                            )) {
                            configurationCycles++;
                            protocolPhase = "play";
                            // Match the compression framing already used by the
                            // configuration ACK while selecting the profile's tick id.
                            playTickFrame = createPayloadlessMinecraftFrame(
                                minecraftProfile.play.serverboundClientTickEnd,
                                parsed.frame[parsed.headerBytes] === 0x00
                            );
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
                            armClientStallTimer();
                        }
                        else if ((protocolPhase === "play" ||
                            protocolPhase === "reconfiguring") &&
                            isPayloadlessPacket(
                                parsed.frame,
                                parsed.headerBytes,
                                minecraftProfile.play.serverboundConfigurationAcknowledged
                            )) {
                            protocolPhase = "configuration";
                            lastClientTrafficAt = Date.now();
                            clearClientStallTimer();
                            traceTunnelEvent("client acknowledged PLAY to CONFIGURATION transition");
                        }
                        if (traceTunnel && protocolPhase === "play") {
                            const packet = minecraftPacketId(parsed.frame, parsed.headerBytes);
                            if (packet !== undefined) {
                                lastClientPlayPacket = `0x${packet.id.toString(16)}/${parsed.frame.byteLength}`;
                            }
                        }
                        if (protocolPhase === "play" &&
                            isPayloadlessPacket(
                                parsed.frame,
                                parsed.headerBytes,
                                minecraftProfile.play.serverboundClientTickEnd
                            )) {
                            playTickFrame = Buffer.from(parsed.frame);
                            traceTunnelEvent("observed play tick for stall proxy");
                        }
                        traceCustomPayload(
                            parsed.frame,
                            parsed.headerBytes,
                            "client",
                            protocolPhase === "play",
                            minecraftProfile
                        );
                    }
                }
                if (encryptionResponsePending) {
                    packetFramingEnabled = false;
                    minecraftProfile = undefined;
                    encryptionResponsePending = false;
                    clientFrameBuffer.clear();
                    clearClientStallTimer();
                    traceTunnelEvent("disabled keepalive proxy after login encryption response");
                    // Keep any complete/partial server bytes retained by a
                    // high-water pause. They are now opaque encrypted bytes,
                    // not disposable parser state, and must drain in order.
                    scheduleServerFrameDrain();
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
        clearClientStallTimer();
        tunnelCancelled = true;
        clearServerFrameState();
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
    let stopped = false;
    let activeController;
    let activeRequest = Promise.resolve();
    const register = () => {
        if (requestRunning || stopped) {
            return activeRequest;
        }
        requestRunning = true;
        relayRegistrationState.attempts++;
        relayRegistrationState.lastAttemptAt = Date.now();
        const controller = new AbortController();
        activeController = controller;
        const timeout = setTimeout(
            () => controller.abort(), Math.min(10_000, registration.intervalMs));
        activeRequest = (async () => {
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
                if (!stopped) {
                    relayRegistrationState.failures++;
                    relayRegistrationState.lastError = String(
                        error instanceof Error ? error.message : error).slice(0, 240);
                    console.warn("RelayNode registration failed:", relayRegistrationState.lastError);
                }
            }
            finally {
                clearTimeout(timeout);
                if (activeController === controller) {
                    activeController = undefined;
                }
                requestRunning = false;
            }
        })();
        return activeRequest;
    };
    void register();
    const timer = setInterval(() => void register(), registration.intervalMs);
    timer.unref();
    stopRelayRegistration = async () => {
        if (stopped) {
            return;
        }
        stopped = true;
        clearInterval(timer);
        activeController?.abort();
        await activeRequest;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 2_000);
        try {
            const response = await fetch(leaseUrl, {
                method: "DELETE",
                headers: { authorization: `Bearer ${registration.token}` },
                signal: controller.signal,
            });
            if (!response.ok && response.status !== 404) {
                throw new Error(`registry returned ${response.status}`);
            }
            relayRegistrationState.registered = false;
            console.log(`RelayNode unregistered as ${registration.nodeId}`);
        }
        finally {
            clearTimeout(timeout);
        }
    };
}
async function shutdownRelayNode(code) {
    if (shutdownStarted) {
        return;
    }
    shutdownStarted = true;
    const forcedExit = setTimeout(() => process.exit(code), 3_000);
    try {
        await stopRelayRegistration();
    }
    catch (error) {
        console.warn("RelayNode unregister failed:", error instanceof Error ? error.message : error);
    }
    for (const client of webSocketServer.clients) {
        client.terminate();
    }
    await new Promise((resolve) => httpServer.close(resolve));
    clearTimeout(forcedExit);
    process.exit(code);
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
        const records = await resolveSrvWithRetries(`_minecraft._tcp.${request.host}`);
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
async function resolveSrvWithRetries(hostname) {
    return withDnsRetries(`SRV ${hostname}`, async () => {
        if (dnsTestScenario?.srvHost !== undefined &&
            hostname === `_minecraft._tcp.${dnsTestScenario.srvHost}`) {
            dnsTestSrvAttempts++;
            if (dnsTestSrvAttempts <= dnsTestScenario.srvFailures) {
                throw createDnsTestError("EAI_AGAIN", hostname);
            }
            return [{
                    priority: 0,
                    weight: 0,
                    port: dnsTestScenario.srvPort,
                    name: dnsTestScenario.srvTarget,
                }];
        }
        if (dnsTestScenario?.srvPermanentHost !== undefined &&
            hostname === `_minecraft._tcp.${dnsTestScenario.srvPermanentHost}`) {
            throw createDnsTestError("ENOTFOUND", hostname);
        }
        return resolveSrv(hostname);
    });
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
            socket.gaiusTarget = target;
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
    void resolvePublicAddresses(host, lookupOptions).then((publicAddresses) => {
        if (returnAll) {
            callback(null, publicAddresses);
        }
        else {
            callback(null, publicAddresses[0].address, publicAddresses[0].family);
        }
    }, (error) => callback(error));
}
function resolvePublicAddresses(host, lookupOptions) {
    const family = lookupOptions?.family ?? 0;
    const key = `${String(host).trim().toLowerCase()}|${family}`;
    const now = Date.now();
    const cached = publicDnsCache.get(key);
    if (cached?.addresses !== undefined && cached.expiresAt > now) {
        publicDnsCacheHits++;
        return Promise.resolve(cached.addresses);
    }
    if (cached?.promise !== undefined) {
        publicDnsCacheInflightJoins++;
        return cached.promise;
    }
    publicDnsCacheMisses++;
    let promise;
    promise = lookupDnsWithRetries(host, lookupOptions).then(({addresses}) => {
        const publicAddresses = addresses.filter(
            (entry) => !isPrivateNetworkAddress(entry.address));
        if (publicAddresses.length === 0) {
            const denied = new Error("Target hostname resolves only to private addresses");
            denied.code = "EACCES";
            throw denied;
        }
        publicDnsCache.set(key, {
            addresses: publicAddresses,
            expiresAt: Date.now() + publicDnsCacheTtlMs,
        });
        prunePublicDnsCache();
        return publicAddresses;
    }).catch((error) => {
        const current = publicDnsCache.get(key);
        if (current?.promise === promise) {
            publicDnsCache.delete(key);
        }
        throw error;
    });
    publicDnsCache.set(key, {promise, expiresAt: 0});
    prunePublicDnsCache();
    return promise;
}
function prunePublicDnsCache() {
    const now = Date.now();
    for (const [key, entry] of publicDnsCache) {
        if (entry.promise === undefined && entry.expiresAt <= now) {
            publicDnsCache.delete(key);
        }
    }
    while (publicDnsCache.size > maximumPublicDnsCacheEntries) {
        const oldestKey = publicDnsCache.keys().next().value;
        if (oldestKey === undefined) {
            break;
        }
        const oldest = publicDnsCache.get(oldestKey);
        if (oldest?.promise !== undefined) {
            break;
        }
        publicDnsCache.delete(oldestKey);
    }
}
async function lookupDnsWithRetries(host, options) {
    return withDnsRetries(`lookup ${host}`, () => lookupDns(host, options));
}
function lookupDns(host, options) {
    if (dnsTestScenario?.lookupHost === host) {
        dnsTestLookupAttempts++;
        if (dnsTestLookupAttempts <= dnsTestScenario.lookupFailures) {
            return Promise.reject(createDnsTestError("EAI_AGAIN", host));
        }
        return Promise.resolve({
            addresses: [{ address: dnsTestScenario.lookupAddress, family: 4 }],
            family: 4,
        });
    }
    if (dnsTestScenario?.lookupPermanentHost === host) {
        return Promise.reject(createDnsTestError("ENOTFOUND", host));
    }
    return new Promise((resolve, reject) => {
        dnsLookup(host, options, (error, addresses, family) => {
            if (error !== null) {
                reject(error);
                return;
            }
            resolve({addresses, family});
        });
    });
}
async function withDnsRetries(label, operation) {
    for (let attempt = 0; ; attempt++) {
        try {
            return await operation();
        }
        catch (error) {
            if (!isTransientDnsError(error) || attempt >= config.dnsRetryAttempts) {
                throw error;
            }
            const retryNumber = attempt + 1;
            const code = error instanceof Error && "code" in error
                ? String(error.code)
                : "unknown";
            console.warn(
                `DNS ${label} transient error ${code}; retry ${retryNumber}/${config.dnsRetryAttempts}`
            );
            const delayMs = config.dnsRetryDelayMs * retryNumber;
            if (delayMs > 0) {
                await new Promise((resolve) => setTimeout(resolve, delayMs));
            }
        }
    }
}
function isTransientDnsError(error) {
    const code = error !== null && typeof error === "object" && "code" in error
        ? String(error.code)
        : "";
    return ["EAI_AGAIN", "ETIMEOUT", "ESERVFAIL", "EAI_FAIL", "EAI_SYSTEM"].includes(code);
}
function createDnsTestError(code, host) {
    const error = new Error(`${code} resolving ${host}`);
    error.code = code;
    return error;
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
        if (peer !== undefined) {
            // Clearing only the endpoint that observed `close` leaves the
            // peer's role slot occupied by a closed object. A same-session
            // reconnect then looks like a duplicate and the session map leaks.
            if (current?.[peer.role] === peer) {
                current[peer.role] = undefined;
            }
            peer.peer = undefined;
            if (!peer.closed) {
                peer.closed = true;
                if (peer.webSocket.readyState === WebSocket.OPEN ||
                    peer.webSocket.readyState === WebSocket.CONNECTING) {
                    peer.webSocket.close(1000, "Local tunnel peer closed");
                }
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
        runtime: relayRuntimeSnapshot(),
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
            const declaredLength = parseResponseContentLength(upstream.headers);
            temporary = await spoolResponseBody(upstream.body, maximumBytes, declaredLength);
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
function parseResponseContentLength(headers) {
    const raw = headers.get("content-length");
    if (raw === null || raw.trim() === "") {
        return undefined;
    }
    const parsed = Number(raw);
    return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : undefined;
}
async function spoolResponseBody(body, maximumBytes, declaredLength) {
    if (declaredLength !== undefined && declaredLength > maximumBytes) {
        throw new ProxyResponseSizeError();
    }
    if (body === null) {
        if (declaredLength !== undefined && declaredLength !== 0) {
            throw new ProxyResponseTruncatedError(declaredLength, 0);
        }
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
        if (declaredLength !== undefined && byteLength !== declaredLength) {
            throw new ProxyResponseTruncatedError(declaredLength, byteLength);
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
class ProxyResponseTruncatedError extends Error {
    constructor(expectedLength, receivedLength) {
        super(`Resource-pack body length mismatch: expected ${expectedLength} bytes, received ${receivedLength}`);
        this.name = "ProxyResponseTruncatedError";
        this.expectedLength = expectedLength;
        this.receivedLength = receivedLength;
    }
}
