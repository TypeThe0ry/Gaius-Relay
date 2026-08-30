// Pure Node model for a fair, bounded WebSocket -> TCP client drain.
//
// This is deliberately a model smoke: it does not import or start dist/main.js,
// open a socket, or launch a browser.  It exercises the same length-prefixed
// ownership rules through the production MinecraftFrameAccumulator while the
// scheduler below models the proposed client-side queue/continuation boundary.
// The model is intentionally strict about ordering and ownership so a future
// main.js implementation cannot silently duplicate, reorder, or starve a
// tunnel while adding a setImmediate continuation.

import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { MinecraftFrameAccumulator } from "./dist/framed-stream.js";

const MAX_FRAME_BYTES = 16 * 1024 * 1024;
// Keep this in lock-step with main.js: the parser may retain a complete
// 16-MiB payload plus the maximum five-byte VarInt header before falling back
// to opaque forwarding.  Crossing this boundary must never reinterpret the
// raw stream from an unknown frame offset.
const MAX_CLIENT_PARSER_BYTES = MAX_FRAME_BYTES + 5;
const MAX_FRAMES_PER_TURN = 16;
const MAX_BYTES_PER_TURN = 256 * 1024;
const STREAM_PAYLOAD_BYTES = 16 * 1024 * 1024;
const FRAME_PAYLOAD_BYTES = 8 * 1024;
const STREAM_FRAME_COUNT = STREAM_PAYLOAD_BYTES / FRAME_PAYLOAD_BYTES;
const MAX_HANDSHAKE_BYTES = 4 * 1024;
const SUPPORTED_PROTOCOLS = new Set([774, 776]);
const TRANSITION_FRAME_ORDINAL = 33;
const TRANSITION_MODEL_FRAME_COUNT = 40;
// Keep the recovery model in lock-step with dist/main.js.  A high-water
// fallback may resume inspection only for a complete, payloadless control
// packet whose id is selected by the negotiated profile and current phase.
// Login/configuration ids are shared by 774/776; PLAY ids differ.
const HIGH_WATER_PROFILE_IDS = Object.freeze({
    774: Object.freeze({
        loginAcknowledged: 3,
        configurationFinish: 3,
        playConfigurationAcknowledged: 15,
        playClientTickEnd: 12,
    }),
    776: Object.freeze({
        loginAcknowledged: 3,
        configurationFinish: 3,
        playConfigurationAcknowledged: 16,
        playClientTickEnd: 13,
    }),
});

function assertCondition(condition, message) {
    if (!condition) {
        throw new Error(message);
    }
}

function encodeVarInt(value) {
    if (!Number.isSafeInteger(value) || value < 0) {
        throw new RangeError(`invalid frame length: ${value}`);
    }
    const bytes = [];
    do {
        let current = value & 0x7f;
        value = Math.floor(value / 0x80);
        if (value !== 0) {
            current |= 0x80;
        }
        bytes.push(current);
    } while (value !== 0);
    return Buffer.from(bytes);
}

function frame(payload) {
    const body = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
    return Buffer.concat([encodeVarInt(body.byteLength), body]);
}

function hashBuffer(buffer) {
    return createHash("sha256").update(buffer).digest("hex");
}

function fragment(buffer, chunkSize = 7777) {
    const chunks = [];
    for (let offset = 0; offset < buffer.byteLength;) {
        const end = Math.min(offset + chunkSize, buffer.byteLength);
        chunks.push(buffer.subarray(offset, end));
        offset = end;
    }
    return chunks;
}

function buildIndexedStream(frameCount, payloadBytes, seed) {
    const frames = [];
    for (let index = 0; index < frameCount; index++) {
        const payload = Buffer.allocUnsafe(payloadBytes);
        payload.writeUInt32LE(index >>> 0, 0);
        for (let offset = 4; offset < payload.length; offset++) {
            payload[offset] = (seed + index * 31 + offset * 17) & 0xff;
        }
        frames.push(frame(payload));
    }
    return Buffer.concat(frames);
}

function decodeVarInt(buffer, offset = 0) {
    let value = 0;
    for (let index = 0; index < 5; index++) {
        if (offset + index >= buffer.byteLength) {
            return undefined;
        }
        const current = buffer[offset + index];
        value += (current & 0x7f) * 2 ** (index * 7);
        if ((current & 0x80) === 0) {
            return {value, bytesRead: index + 1};
        }
    }
    return null;
}

function encodeHandshake(protocolVersion, nextState = 2) {
    const host = Buffer.from("bounded-drain.test", "utf8");
    const body = Buffer.concat([
        encodeVarInt(0),
        encodeVarInt(protocolVersion),
        encodeVarInt(host.byteLength),
        host,
        Buffer.from([0x63, 0xdd]), // 25565, big-endian
        encodeVarInt(nextState),
    ]);
    return frame(body);
}

function readLengthPrefixed(buffer, maximumBytes = MAX_FRAME_BYTES) {
    const length = decodeVarInt(buffer);
    if (length === undefined) {
        return undefined;
    }
    if (length === null || length.value > maximumBytes) {
        return null;
    }
    const frameBytes = length.bytesRead + length.value;
    if (buffer.byteLength < frameBytes) {
        return undefined;
    }
    return {
        frame: buffer.subarray(0, frameBytes),
        remainder: buffer.subarray(frameBytes),
        headerBytes: length.bytesRead,
    };
}

/**
 * Small protocol-state model for the branches around main.js:1487-1698.
 * It deliberately separates raw client writes from parser inspection, which
 * is required when a WebSocket message ends in a partial frame or encryption
 * starts between two messages.
 */
class ProtocolIngressModel {
    constructor() {
        this.handshakeBuffer = Buffer.alloc(0);
        this.clientFrameBuffer = new MinecraftFrameAccumulator(MAX_FRAME_BYTES);
        this.clientRawPending = [];
        this.clientRawWrites = [];
        this.serverFramePending = [];
        this.serverFrameWrites = [];
        this.controlReplies = [];
        this.transitions = [];
        this.packetFramingEnabled = false;
        this.minecraftHandshakeSeen = false;
        this.minecraftProfile = undefined;
        this.keepAliveProxyOpaque = false;
        this.keepAliveProxyEncryptionOpaque = false;
        this.encryptionResponsePending = false;
        this.tcpPausedForClient = false;
        this.tcpWritable = true;
        this.clientDrainScheduled = false;
        this.clientDrainTurns = 0;
        this.clientParsedFrames = 0;
        this.closed = false;
    }

    transitionOpaque(encryption = false) {
        if (!this.keepAliveProxyOpaque) {
            this.keepAliveProxyOpaque = true;
            this.transitions.push("opaque");
        }
        if (encryption && !this.keepAliveProxyEncryptionOpaque) {
            this.keepAliveProxyEncryptionOpaque = true;
            this.transitions.push("encryption-opaque");
        }
        this.packetFramingEnabled = false;
        this.minecraftProfile = undefined;
        this.handshakeBuffer = Buffer.alloc(0);
        this.clientFrameBuffer.clear();
    }

    setTcpWritable(writable) {
        this.tcpWritable = Boolean(writable);
        if (this.tcpWritable) {
            this.flushClientRaw();
        }
    }

    flushClientRaw() {
        if (!this.tcpWritable || this.closed) {
            return;
        }
        while (this.clientRawPending.length !== 0) {
            this.clientRawWrites.push(this.clientRawPending.shift());
        }
    }

    enqueueClientRaw(data) {
        this.clientRawPending.push(data);
        this.flushClientRaw();
    }

    inspectHandshake() {
        const parsed = readLengthPrefixed(this.handshakeBuffer, MAX_HANDSHAKE_BYTES);
        if (parsed === undefined) {
            return {state: "incomplete"};
        }
        if (parsed === null) {
            return {state: "opaque"};
        }
        const packet = parsed.frame.subarray(parsed.headerBytes);
        const packetId = decodeVarInt(packet);
        if (packetId === undefined || packetId === null || packetId.value !== 0) {
            return {state: "opaque"};
        }
        let offset = packetId.bytesRead;
        const protocol = decodeVarInt(packet, offset);
        if (protocol === undefined || protocol === null) {
            return {state: "opaque"};
        }
        offset += protocol.bytesRead;
        const hostLength = decodeVarInt(packet, offset);
        if (hostLength === undefined || hostLength === null || hostLength.value > 255) {
            return {state: "opaque"};
        }
        offset += hostLength.bytesRead + hostLength.value;
        if (offset + 3 > packet.byteLength) {
            return {state: "opaque"};
        }
        offset += 2; // port
        const nextState = decodeVarInt(packet, offset);
        if (nextState === undefined || nextState === null ||
            (nextState.value !== 1 && nextState.value !== 2) ||
            offset + nextState.bytesRead !== packet.byteLength) {
            return {state: "opaque"};
        }
        return {
            state: "complete",
            protocolVersion: protocol.value,
            profile: SUPPORTED_PROTOCOLS.has(protocol.value)
                ? {protocolVersion: protocol.value}
                : undefined,
            remainder: parsed.remainder,
        };
    }

    drainClientFrames() {
        if (this.clientDrainScheduled || this.closed || !this.packetFramingEnabled) {
            return;
        }
        this.clientDrainScheduled = true;
        let frames = 0;
        let bytes = 0;
        try {
            while (this.clientFrameBuffer.byteLength > 0) {
                const parsed = this.clientFrameBuffer.peekFrame();
                if (parsed === undefined) {
                    break;
                }
                if (parsed === null) {
                    this.transitionOpaque();
                    break;
                }
                if (frames > 0 && (frames >= MAX_FRAMES_PER_TURN ||
                    bytes >= MAX_BYTES_PER_TURN)) {
                    break;
                }
                this.clientFrameBuffer.consumeFrame(parsed);
                this.clientParsedFrames++;
                frames++;
                bytes += parsed.frameBytes;
            }
        }
        finally {
            this.clientDrainTurns++;
            this.clientDrainScheduled = false;
        }
    }

    handleBinary(data) {
        if (this.closed) {
            return;
        }
        const clientData = Buffer.isBuffer(data) ? data : Buffer.from(data);
        this.enqueueClientRaw(clientData);

        // Encryption response bytes are opaque immediately.  They must still
        // be written once through the raw queue, but never inspected as a new
        // framed packet stream.
        if (this.encryptionResponsePending) {
            this.encryptionResponsePending = false;
            this.transitionOpaque(true);
            return;
        }

        let frameClientData = clientData;
        if (!this.packetFramingEnabled && !this.minecraftHandshakeSeen) {
            if (this.handshakeBuffer.byteLength + clientData.byteLength >
                MAX_HANDSHAKE_BYTES) {
                this.minecraftHandshakeSeen = true;
                this.transitionOpaque();
                frameClientData = undefined;
            }
            else {
                this.handshakeBuffer = this.handshakeBuffer.byteLength === 0
                    ? clientData
                    : Buffer.concat([this.handshakeBuffer, clientData]);
                const handshake = this.inspectHandshake();
                if (handshake.state === "incomplete") {
                    frameClientData = undefined;
                }
                else if (handshake.state === "complete") {
                    this.minecraftHandshakeSeen = true;
                    this.handshakeBuffer = Buffer.alloc(0);
                    this.minecraftProfile = handshake.profile;
                    if (handshake.profile === undefined) {
                        this.transitionOpaque();
                        frameClientData = undefined;
                    }
                    else {
                        this.packetFramingEnabled = true;
                        this.transitions.push(`profile-${handshake.profile.protocolVersion}`);
                        frameClientData = handshake.remainder;
                    }
                }
                else {
                    this.minecraftHandshakeSeen = true;
                    this.transitionOpaque();
                    frameClientData = undefined;
                }
            }
        }
        if (this.packetFramingEnabled && frameClientData !== undefined &&
            frameClientData.byteLength !== 0) {
            this.clientFrameBuffer.append(frameClientData);
            this.drainClientFrames();
        }
    }

    markEncryptionResponsePending() {
        this.encryptionResponsePending = true;
    }

    receiveServerFrame(data) {
        if (this.closed) {
            return;
        }
        const frameData = Buffer.isBuffer(data) ? data : Buffer.from(data);
        if (this.tcpPausedForClient) {
            this.serverFramePending.push(frameData);
            return;
        }
        this.serverFrameWrites.push(frameData);
    }

    drainServerFrames() {
        if (this.closed || this.tcpPausedForClient) {
            return;
        }
        while (this.serverFramePending.length !== 0) {
            this.serverFrameWrites.push(this.serverFramePending.shift());
        }
    }

    handleControl(message) {
        if (this.closed) {
            return;
        }
        if (message?.type !== "flow" || typeof message.paused !== "boolean") {
            throw new TypeError("Expected flow control message");
        }
        this.tcpPausedForClient = message.paused;
        this.controlReplies.push({type: "flow", paused: message.paused});
        if (!message.paused) {
            this.drainServerFrames();
        }
    }

    close() {
        this.closed = true;
        this.clientRawPending.length = 0;
        this.serverFramePending.length = 0;
        this.clientFrameBuffer.clear();
        this.handshakeBuffer = Buffer.alloc(0);
        this.clientDrainScheduled = false;
        this.tcpPausedForClient = false;
    }

    clientRawDigest() {
        const hash = createHash("sha256");
        for (const chunk of this.clientRawWrites) {
            hash.update(chunk);
        }
        return hash.digest("hex");
    }

    serverDigest() {
        const hash = createHash("sha256");
        for (const chunk of this.serverFrameWrites) {
            hash.update(chunk);
        }
        return hash.digest("hex");
    }
}

/**
 * Model the client parser high-water boundary in main.js.  The production
 * path always forwards each WebSocket message to TCP, while the framed copy
 * is an optional inspection buffer.  Once that copy crosses
 * maximumClientFrameParserBytes, inspection is disabled and the retained
 * partial frame is discarded; profile/phase (and the PLAY watchdog) remain
 * authoritative.  Only a self-contained payloadless control frame may reopen
 * inspection.  Keeping this as a separate model makes split-at-boundary and
 * recovery regressions visible without constructing a live relay tunnel.
 */
class HighWaterSplitIngressModel {
    constructor({protocolVersion = 776, phase = "play"} = {}) {
        const profileIds = HIGH_WATER_PROFILE_IDS[protocolVersion];
        assertCondition(profileIds !== undefined,
            `unsupported high-water recovery profile: ${protocolVersion}`);
        this.parser = new MinecraftFrameAccumulator(MAX_FRAME_BYTES);
        this.profile = {protocolVersion, ...profileIds};
        this.protocolPhase = phase;
        this.packetFramingEnabled = true;
        this.clientFrameInspectionDisabled = false;
        this.playStallTimerActive = phase === "play";
        this.highWaterEvents = 0;
        this.highWaterBytes = 0;
        this.rawWrites = [];
        this.rawBytes = 0;
        this.parsedFrames = [];
        this.recoveryAttempts = 0;
        this.recoveries = 0;
        // The production bridge increments ingress at append time and only
        // advances the committed watermark after the inspection deque is
        // fully drained.  Keep both counters in the model so a high-water
        // fallback cannot accidentally claim that an unparsed sequence is
        // safe for phase-sensitive rewrites.
        this.ingressSequence = 0;
        this.committedSequence = 0;
        this.latestEnqueuedAt = 0;
        this.ingressClock = 0;
        this.watermarkBypasses = 0;
        this.closedRawDigest = undefined;
        this.closedRawBytes = 0;
        this.closedRawWrites = 0;
        this.closed = false;
    }

    recordRaw(chunk) {
        // Copy the reference just as the relay's TCP write owns the original
        // WebSocket message.  The model hashes these chunks at the end to
        // prove that parser fallback never duplicates or drops bytes.
        const copy = Buffer.from(chunk);
        this.rawWrites.push(copy);
        this.rawBytes += copy.byteLength;
    }

    isStandaloneControlFrame(chunk) {
        if (!this.clientFrameInspectionDisabled || !this.packetFramingEnabled ||
            this.profile === undefined || chunk.byteLength === 0) {
            return false;
        }
        const parsed = readLengthPrefixed(chunk, MAX_FRAME_BYTES);
        if (parsed === undefined || parsed === null ||
            parsed.remainder.byteLength !== 0) {
            return false;
        }
        const payload = parsed.frame.subarray(parsed.headerBytes);
        const packetId = decodeVarInt(payload);
        if (packetId === undefined || packetId === null ||
            packetId.bytesRead !== payload.byteLength) {
            return false;
        }
        const ids = this.profile;
        if (this.protocolPhase === "login") {
            return packetId.value === ids.loginAcknowledged;
        }
        if (this.protocolPhase === "configuration") {
            return packetId.value === ids.configurationFinish;
        }
        if (this.protocolPhase === "play" || this.protocolPhase === "reconfiguring") {
            // Both the configuration acknowledgement and the payloadless
            // client tick are safe standalone boundaries in these phases.
            return packetId.value === ids.playConfigurationAcknowledged ||
                packetId.value === ids.playClientTickEnd;
        }
        return false;
    }

    drainInspection() {
        while (this.parser.byteLength > 0) {
            const parsed = this.parser.peekFrame();
            if (parsed === undefined) {
                break;
            }
            if (parsed === null) {
                // A malformed framed copy is treated exactly like the
                // high-water path: raw bytes stay authoritative and no guess
                // is made about a new frame boundary.
                this.clientFrameInspectionDisabled = true;
                this.parser.clear();
                break;
            }
            assertCondition(this.parser.consumeFrame(parsed),
                "high-water model could not commit an inspected frame");
            this.parsedFrames.push(parsed.frame);
        }
        if (!this.clientFrameInspectionDisabled && this.parser.byteLength === 0) {
            // Match main.js: a sequence is safe only once every retained
            // frame has crossed the ownership boundary.
            this.committedSequence = this.ingressSequence;
        }
    }

    appendInspection(chunk) {
        this.ingressSequence++;
        this.latestEnqueuedAt = ++this.ingressClock;
        const nextBytes = this.parser.byteLength + chunk.byteLength;
        if (nextBytes > MAX_CLIENT_PARSER_BYTES) {
            this.highWaterEvents++;
            this.highWaterBytes = Math.max(this.highWaterBytes, nextBytes);
            this.clientFrameInspectionDisabled = true;
            // Preserve profile/phase/play stall state; only the speculative
            // parser copy is retired.
            this.parser.clear();
            return {state: "high-water", nextBytes};
        }
        this.parser.append(chunk);
        return {state: "buffered", nextBytes};
    }

    ingest(chunk, {drain = false} = {}) {
        if (this.closed) {
            return {state: "closed"};
        }
        const data = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        if (data.byteLength === 0) {
            return {state: "empty"};
        }
        this.recordRaw(data);
        if (this.clientFrameInspectionDisabled) {
            this.recoveryAttempts++;
            if (this.isStandaloneControlFrame(data)) {
                this.clientFrameInspectionDisabled = false;
                this.recoveries++;
                const result = this.appendInspection(data);
                if (result.state === "buffered") {
                    this.drainInspection();
                }
                return {...result, state: "recovered"};
            }
            return {state: "opaque"};
        }
        const result = this.appendInspection(data);
        if (drain && result.state === "buffered") {
            this.drainInspection();
        }
        return result;
    }

    rawDigest() {
        const hash = createHash("sha256");
        for (const chunk of this.rawWrites) {
            hash.update(chunk);
        }
        return hash.digest("hex");
    }

    watermarkSettled() {
        if (this.profile === undefined ||
            this.committedSequence >= this.ingressSequence) {
            return true;
        }
        this.watermarkBypasses++;
        return false;
    }

    watermarkSnapshot() {
        const settled = this.profile === undefined ||
            this.committedSequence >= this.ingressSequence;
        return {
            ingressSequence: this.ingressSequence,
            committedSequence: this.committedSequence,
            latestEnqueuedAt: this.latestEnqueuedAt,
            settled,
        };
    }

    close() {
        // Preserve an audit digest before clearing transport/parser state. The
        // production close path retires the queue and watermark; it must not
        // cause a later continuation to replay bytes from this generation.
        this.closedRawDigest = this.rawDigest();
        this.closedRawBytes = this.rawBytes;
        this.closedRawWrites = this.rawWrites.length;
        this.closed = true;
        this.parser.clear();
        this.rawWrites.length = 0;
        this.rawBytes = 0;
        this.clientFrameInspectionDisabled = true;
        this.packetFramingEnabled = false;
        this.playStallTimerActive = false;
        this.ingressSequence = 0;
        this.committedSequence = 0;
        this.latestEnqueuedAt = 0;
        this.ingressClock = 0;
    }
}

/**
 * A single logical browser tunnel.  WebSocket messages are retained as
 * immutable chunks and are appended to the frame accumulator only while its
 * turn is running.  Each complete frame is published exactly once to the
 * supplied TCP sink.
 */
class ModelTunnel {
    constructor(id, scheduler, onWrite) {
        this.id = id;
        this.scheduler = scheduler;
        this.onWrite = onWrite;
        this.messages = [];
        this.accumulator = new MinecraftFrameAccumulator(MAX_FRAME_BYTES);
        this.outputHash = createHash("sha256");
        this.outputBytes = 0;
        this.outputFrames = 0;
        this.outputMarkers = [];
        this.turns = 0;
        this.maxTurnFrames = 0;
        this.maxTurnBytes = 0;
        this.oversizedFrameTurns = 0;
        this.closed = false;
    }

    enqueue(message) {
        if (!Buffer.isBuffer(message)) {
            message = Buffer.from(message);
        }
        if (message.byteLength !== 0) {
            this.messages.push(message);
        }
    }

    hasRunnableWork() {
        // A partial VarInt/body with no queued message is waiting for a future
        // WebSocket message; do not spin a continuation for it.
        if (this.messages.length !== 0) {
            return true;
        }
        const parsed = this.accumulator.peekFrame();
        return parsed !== undefined;
    }

    drainTurn(limits) {
        let frameCount = 0;
        let byteCount = 0;
        let inputBytes = 0;
        let oversized = false;

        while (true) {
            let parsed = this.accumulator.peekFrame();
            while (parsed === undefined && this.messages.length !== 0) {
                const message = this.messages.shift();
                inputBytes += message.byteLength;
                this.accumulator.append(message);
                parsed = this.accumulator.peekFrame();
            }
            if (parsed === undefined) {
                break;
            }
            if (parsed === null) {
                throw new Error(`${this.id}: malformed or oversized frame`);
            }

            // Match the server-drain rule: always make progress on the first
            // complete frame, even when that one frame exceeds the byte budget.
            if (frameCount > 0 &&
                (frameCount >= limits.maxFrames ||
                    byteCount >= limits.maxBytes)) {
                break;
            }
            if (frameCount === 0 && parsed.frameBytes > limits.maxBytes) {
                oversized = true;
            }

            assertCondition(this.accumulator.consumeFrame(parsed),
                `${this.id}: frame ownership was not committed`);
            const marker = parsed.frameBytes - parsed.headerBytes >= 4
                ? parsed.frame.readUInt32LE(parsed.headerBytes)
                : undefined;
            this.outputHash.update(parsed.frame);
            this.outputBytes += parsed.frame.byteLength;
            this.outputFrames++;
            this.outputMarkers.push(marker);
            this.onWrite({
                tunnelId: this.id,
                frame: parsed.frame,
                frameIndex: this.outputFrames - 1,
                marker,
            });
            frameCount++;
            byteCount += parsed.frame.byteLength;
        }

        this.turns++;
        this.maxTurnFrames = Math.max(this.maxTurnFrames, frameCount);
        this.maxTurnBytes = Math.max(this.maxTurnBytes, byteCount);
        if (oversized) {
            this.oversizedFrameTurns++;
        }
        return {
            frames: frameCount,
            bytes: byteCount,
            inputBytes,
            pending: this.hasRunnableWork(),
            oversized,
        };
    }

    digest() {
        return this.outputHash.copy().digest("hex");
    }
}

/**
 * One process-wide fair queue.  A tunnel receives at most one bounded turn;
 * any remainder is put at the tail and resumed by setImmediate.  This is the
 * key property missing from the old synchronous client message handler.
 */
class FairBoundedDrainScheduler {
    constructor({
        maxFrames = MAX_FRAMES_PER_TURN,
        maxBytes = MAX_BYTES_PER_TURN,
        scheduleImmediate = (callback) => setImmediate(callback),
    } = {}) {
        this.limits = Object.freeze({maxFrames, maxBytes});
        this.scheduleImmediate = scheduleImmediate;
        this.ready = [];
        this.readySet = new Set();
        this.scheduled = false;
        this.running = false;
        this.failed = undefined;
        this.turns = 0;
        this.immediateSchedules = 0;
        this.immediateRuns = 0;
        this.immediateContinuations = 0;
        this.trace = [];
        this.waiters = [];
    }

    enqueue(tunnel, message) {
        tunnel.enqueue(message);
        this.activate(tunnel);
    }

    activate(tunnel) {
        if (tunnel.closed || this.readySet.has(tunnel)) {
            return;
        }
        this.ready.push(tunnel);
        this.readySet.add(tunnel);
        this.ensureScheduled();
    }

    ensureScheduled() {
        if (this.scheduled || this.running || this.ready.length === 0) {
            return;
        }
        this.scheduled = true;
        this.immediateSchedules++;
        if (this.turns !== 0) {
            this.immediateContinuations++;
        }
        this.scheduleImmediate(() => {
            this.scheduled = false;
            this.immediateRuns++;
            try {
                this.runOneTurn();
            }
            catch (error) {
                this.failed = error;
                this.ready.length = 0;
                this.readySet.clear();
                this.rejectWaiters(error);
                return;
            }
            this.resolveWaitersIfIdle();
        });
    }

    runOneTurn() {
        const tunnel = this.ready.shift();
        if (tunnel === undefined) {
            return;
        }
        this.readySet.delete(tunnel);
        this.running = true;
        let summary;
        try {
            summary = tunnel.drainTurn(this.limits);
        }
        finally {
            this.running = false;
        }
        this.turns++;
        this.trace.push({
            turn: this.turns,
            tunnelId: tunnel.id,
            frames: summary.frames,
            bytes: summary.bytes,
            inputBytes: summary.inputBytes,
            pending: summary.pending,
        });
        if (summary.pending) {
            // activate() appends at the tail.  Since running is false here,
            // ensureScheduled() is deferred until after this turn's trace.
            this.activate(tunnel);
        }
        this.ensureScheduled();
    }

    waitForIdle() {
        if (this.failed !== undefined) {
            return Promise.reject(this.failed);
        }
        if (!this.scheduled && !this.running && this.ready.length === 0) {
            return Promise.resolve();
        }
        return new Promise((resolve, reject) => {
            this.waiters.push({resolve, reject});
        });
    }

    resolveWaitersIfIdle() {
        if (this.scheduled || this.running || this.ready.length !== 0) {
            return;
        }
        const waiters = this.waiters.splice(0);
        for (const waiter of waiters) {
            waiter.resolve();
        }
    }

    rejectWaiters(error) {
        const waiters = this.waiters.splice(0);
        for (const waiter of waiters) {
            waiter.reject(error);
        }
    }
}

function testSingleSixteenMiBFrame() {
    const payload = Buffer.allocUnsafe(STREAM_PAYLOAD_BYTES);
    for (let index = 0; index < payload.length; index++) {
        payload[index] = (0x26 + index * 13) & 0xff;
    }
    const input = frame(payload);
    const expectedHash = hashBuffer(input);
    const scheduler = new FairBoundedDrainScheduler();
    const writes = [];
    const tunnel = new ModelTunnel("large", scheduler, (write) => writes.push(write));
    const chunks = fragment(input, 64 * 1024);
    for (const chunk of chunks) {
        scheduler.enqueue(tunnel, chunk);
    }
    assertCondition(tunnel.outputFrames === 0,
        "large frame drained synchronously before setImmediate");
    return scheduler.waitForIdle().then(() => {
        assertCondition(chunks.length > 200, "16MiB frame was not fragmented enough");
        assertCondition(tunnel.outputFrames === 1, "16MiB frame was duplicated or dropped");
        assertCondition(tunnel.outputBytes === input.byteLength,
            "16MiB frame byte count changed");
        assertCondition(tunnel.digest() === expectedHash,
            "16MiB frame hash changed");
        assertCondition(writes.length === 1 && writes[0].frame.equals(input),
            "16MiB frame was not written exactly once");
        assertCondition(tunnel.oversizedFrameTurns === 1,
            "oversized first frame did not use the progress exception");
        assertCondition(scheduler.immediateSchedules === 1 &&
            scheduler.immediateRuns === 1,
        "single 16MiB frame unexpectedly scheduled extra work");
    });
}

async function testMultiFrameFairness() {
    const aInput = buildIndexedStream(STREAM_FRAME_COUNT, FRAME_PAYLOAD_BYTES, 0x41);
    const bInput = frame(Buffer.from("tunnel-B-control"));
    const expectedAHash = hashBuffer(aInput);
    const expectedBHash = hashBuffer(bInput);
    const writeLog = [];
    const scheduler = new FairBoundedDrainScheduler();
    const tunnelA = new ModelTunnel("A", scheduler, (write) => {
        writeLog.push({...write, globalIndex: writeLog.length, turn: scheduler.turns + 1});
    });
    const tunnelB = new ModelTunnel("B", scheduler, (write) => {
        writeLog.push({...write, globalIndex: writeLog.length, turn: scheduler.turns + 1});
    });

    const aChunks = fragment(aInput, 7777);
    for (const chunk of aChunks) {
        scheduler.enqueue(tunnelA, chunk);
    }
    scheduler.enqueue(tunnelB, bInput.subarray(0, 2));
    scheduler.enqueue(tunnelB, bInput.subarray(2));
    assertCondition(tunnelA.outputFrames === 0 && tunnelB.outputFrames === 0,
        "multi-frame input drained synchronously before setImmediate");

    let externalImmediateRan = false;
    setImmediate(() => {
        externalImmediateRan = true;
    });
    await scheduler.waitForIdle();

    assertCondition(externalImmediateRan,
        "bounded drain did not yield to another setImmediate callback");
    assertCondition(tunnelA.outputFrames === STREAM_FRAME_COUNT,
        `A frame count mismatch: ${tunnelA.outputFrames}/${STREAM_FRAME_COUNT}`);
    assertCondition(tunnelA.outputBytes === aInput.byteLength,
        "A byte count mismatch");
    assertCondition(tunnelA.digest() === expectedAHash,
        "A stream hash changed (duplicate/reordered/lost bytes)");
    assertCondition(tunnelA.outputMarkers.every((marker, index) => marker === index),
        "A frame marker order changed");
    assertCondition(tunnelB.outputFrames === 1 && tunnelB.digest() === expectedBHash,
        "B frame was not delivered exactly once");
    assertCondition(tunnelA.maxTurnFrames <= MAX_FRAMES_PER_TURN,
        `A exceeded frame turn budget: ${tunnelA.maxTurnFrames}`);
    assertCondition(tunnelA.maxTurnBytes <= MAX_BYTES_PER_TURN,
        `A exceeded byte turn budget: ${tunnelA.maxTurnBytes}`);
    assertCondition(scheduler.immediateContinuations > 0 &&
        scheduler.immediateRuns === scheduler.immediateSchedules,
    "setImmediate continuation accounting mismatch");

    const firstB = writeLog.findIndex((entry) => entry.tunnelId === "B");
    assertCondition(firstB >= 0 && firstB < STREAM_FRAME_COUNT,
        "B tunnel was starved until A's 16MiB stream completed");
    const aBeforeB = writeLog.slice(0, firstB)
        .filter((entry) => entry.tunnelId === "A").length;
    assertCondition(aBeforeB > 0 && aBeforeB <= MAX_FRAMES_PER_TURN,
        `A/B fairness turn was not bounded: ${aBeforeB}`);
    assertCondition(scheduler.trace[0]?.tunnelId === "A" &&
        scheduler.trace[1]?.tunnelId === "B",
    "ready queue did not rotate A -> B at the first continuation");
    for (const turn of scheduler.trace) {
        assertCondition(turn.frames <= MAX_FRAMES_PER_TURN,
            `turn ${turn.turn} exceeded frame limit`);
        assertCondition(turn.bytes <= MAX_BYTES_PER_TURN || turn.frames === 1,
            `turn ${turn.turn} exceeded byte limit without a single-frame exception`);
    }
    return {
        chunks: aChunks.length,
        turns: scheduler.turns,
        immediateContinuations: scheduler.immediateContinuations,
        aFrames: tunnelA.outputFrames,
        bFirstWriteIndex: firstB,
        aFramesBeforeB: aBeforeB,
        maxTurnFrames: tunnelA.maxTurnFrames,
        maxTurnBytes: tunnelA.maxTurnBytes,
    };
}

function testPerMessageBurstCounterexample() {
    // This is the failure mode of a merely per-message budget: if the ws
    // parser emits many A messages in one poll callback, every message gets a
    // fresh 32-frame allowance before the event loop can service B.
    const framesPerMessage = MAX_FRAMES_PER_TURN;
    const messageCount = 8;
    const oldWrites = [];
    for (let message = 0; message < messageCount; message++) {
        for (let frameIndex = 0; frameIndex < framesPerMessage; frameIndex++) {
            oldWrites.push("A");
        }
    }
    oldWrites.push("B");
    const firstB = oldWrites.indexOf("B");
    assertCondition(firstB === messageCount * framesPerMessage,
        "per-message counterexample did not reproduce A starvation");
    assertCondition(firstB > framesPerMessage,
        "counterexample needs more than one A budget to be meaningful");
    return {
        aMessages: messageCount,
        framesPerMessage,
        firstBWriteIndex: firstB,
        eventLoopTurnsBeforeB: messageCount,
    };
}

/**
 * Model the state-ordering edge introduced by a queued parser.  A is one
 * browser message containing 40 frames and frame 33 is the configuration ->
 * PLAY transition.  The raw message is written immediately, while metadata
 * inspection is fairly scheduled in 16-frame turns.  B must get a turn after
 * A's first 16 frames, and the transition must still be committed exactly
 * once at A's frame 33.  The pre-commit observation is intentionally retained
 * as a diagnostic: a product implementation that lets the TCP/server path
 * consume a response before this watermark needs a phase barrier, not merely
 * a fair queue.
 */
function testTransitionAtFrame33QueuedFairness() {
    const makeFrame = (tunnelId, ordinal) => {
        const marker = Buffer.alloc(8);
        marker.writeUInt32LE(ordinal, 0);
        marker.write(tunnelId, 4, 4, "ascii");
        return {
            tunnelId,
            ordinal,
            transition: tunnelId === "A" && ordinal === TRANSITION_FRAME_ORDINAL,
            bytes: frame(marker),
        };
    };
    const aFrames = Array.from(
        {length: TRANSITION_MODEL_FRAME_COUNT},
        (_, index) => makeFrame("A", index + 1),
    );
    const bFrames = [makeFrame("B", 1)];
    const queues = new Map([
        ["A", [...aFrames]],
        ["B", [...bFrames]],
    ]);
    const ready = ["A", "B"];
    const readySet = new Set(ready);
    const output = [];
    const trace = [];
    const serverObservations = [];
    let phase = "configuration";
    let transitionCommits = 0;
    let transitionTurn;
    let turn = 0;

    // Raw transport ownership is committed at enqueue time and is separate
    // from the parser queue.  This is the exact ordering a WebSocket callback
    // must preserve when it defers frame inspection.
    const rawWrites = [
        {tunnelId: "A", bytes: Buffer.concat(aFrames.map((entry) => entry.bytes))},
        {tunnelId: "B", bytes: Buffer.concat(bFrames.map((entry) => entry.bytes))},
    ];
    serverObservations.push({point: "before-first-parser-turn", phase});

    while (ready.length !== 0) {
        const tunnelId = ready.shift();
        readySet.delete(tunnelId);
        const queue = queues.get(tunnelId);
        assertCondition(queue !== undefined, `missing transition model queue ${tunnelId}`);
        turn++;
        const turnEntries = [];
        while (queue.length !== 0 && turnEntries.length < MAX_FRAMES_PER_TURN) {
            const entry = queue.shift();
            turnEntries.push(entry);
            output.push({...entry, globalIndex: output.length, turn});
            if (entry.transition) {
                transitionCommits++;
                transitionTurn = turn;
                phase = "play";
            }
        }
        trace.push({
            turn,
            tunnelId,
            frames: turnEntries.length,
            firstOrdinal: turnEntries[0]?.ordinal ?? null,
            lastOrdinal: turnEntries.at(-1)?.ordinal ?? null,
            phaseAfter: phase,
        });
        // A response can be observed between parser turns.  Record the phase
        // watermark rather than hiding it; this catches a future implementation
        // that schedules fairly but still dispatches server packets too early.
        serverObservations.push({point: `after-turn-${turn}`, phase});
        if (queue.length !== 0 && !readySet.has(tunnelId)) {
            ready.push(tunnelId);
            readySet.add(tunnelId);
        }
    }

    const aOutput = output.filter((entry) => entry.tunnelId === "A");
    const bOutput = output.filter((entry) => entry.tunnelId === "B");
    const rawHash = createHash("sha256");
    for (const write of rawWrites) {
        rawHash.update(write.bytes);
    }
    const parsedHash = createHash("sha256");
    for (const entry of output) {
        parsedHash.update(entry.bytes);
    }
    const actualRawHash = rawHash.digest("hex");
    const actualParsedHash = parsedHash.digest("hex");
    const expectedRaw = hashBuffer(Buffer.concat(rawWrites.map((write) => write.bytes)));
    const expectedA = hashBuffer(Buffer.concat(aFrames.map((entry) => entry.bytes)));
    const expectedB = hashBuffer(Buffer.concat(bFrames.map((entry) => entry.bytes)));
    const actualA = hashBuffer(Buffer.concat(aOutput.map((entry) => entry.bytes)));
    const actualB = hashBuffer(Buffer.concat(bOutput.map((entry) => entry.bytes)));

    assertCondition(actualRawHash === expectedRaw && rawWrites.length === 2 &&
        rawWrites[0].tunnelId === "A" && rawWrites[1].tunnelId === "B",
        "queued transition raw writes were not exactly-once");
    assertCondition(actualA === expectedA && actualB === expectedB,
        "queued transition parsed bytes changed or reordered");
    assertCondition(aOutput.length === TRANSITION_MODEL_FRAME_COUNT &&
        bOutput.length === 1,
    "queued transition frame counts changed");
    assertCondition(aOutput.every((entry, index) => entry.ordinal === index + 1),
        "A transition model reordered frames");
    assertCondition(transitionCommits === 1 && transitionTurn === 4 && phase === "play",
        `transition frame did not commit once at turn 4: commits=${transitionCommits}, turn=${transitionTurn}, phase=${phase}`);
    assertCondition(trace[0]?.tunnelId === "A" && trace[1]?.tunnelId === "B" &&
        trace[0].frames === MAX_FRAMES_PER_TURN,
    "queued transition model did not rotate A -> B after one bounded turn");
    const bFirst = output.findIndex((entry) => entry.tunnelId === "B");
    const transitionIndex = output.findIndex((entry) => entry.transition);
    assertCondition(bFirst === MAX_FRAMES_PER_TURN &&
        transitionIndex > bFirst,
    "B was not serviced before A's frame-33 transition");
    assertCondition(serverObservations[0]?.phase === "configuration" &&
        serverObservations.some((observation) => observation.point === "after-turn-2" &&
            observation.phase === "configuration"),
    "transition model failed to expose the pre-commit phase watermark");
    for (const turnSummary of trace) {
        assertCondition(turnSummary.frames <= MAX_FRAMES_PER_TURN,
            `transition turn ${turnSummary.turn} exceeded frame budget`);
    }
    return {
        transitionFrameOrdinal: TRANSITION_FRAME_ORDINAL,
        transitionTurn,
        transitionCommits,
        turns: turn,
        bFirstWriteIndex: bFirst,
        transitionGlobalIndex: transitionIndex,
        phaseBeforeCommit: serverObservations[0].phase,
        phaseAfterCommit: phase,
        phaseLagObserved: serverObservations[0].phase === "configuration" &&
            transitionIndex > bFirst,
        rawWriteHash: actualRawHash,
        parsedInterleavedHash: actualParsedHash,
    };
}

/**
 * Exercise the parser high-water edge with a frame split one byte before the
 * 16-MiB payload completes.  The second WebSocket message crosses the
 * inspection limit, so the speculative accumulator is retired while the raw
 * TCP path still receives both chunks exactly once.  An arbitrary framed
 * looking chunk must remain opaque; only a standalone payloadless control
 * frame may reopen inspection.
 */
function testHighWaterSplitFrameBoundary() {
    const model = new HighWaterSplitIngressModel({
        protocolVersion: 776,
        phase: "play",
    });
    const declaredPayloadHeader = encodeVarInt(MAX_FRAME_BYTES);
    const splitPoint = MAX_CLIENT_PARSER_BYTES - 2;
    // Header + body prefix is one byte short of a complete 16-MiB frame.
    // There is deliberately no complete frame for the model to consume before
    // the high-water check, matching a fragmented TCP/WebSocket message.
    const firstChunk = Buffer.alloc(splitPoint, 0x5a);
    declaredPayloadHeader.copy(firstChunk, 0);
    const secondChunk = Buffer.from([0x5a, 0x7f, 0x01]);
    const first = model.ingest(firstChunk);
    assertCondition(first.state === "buffered" &&
        first.nextBytes === splitPoint && model.parser.byteLength === splitPoint &&
        model.parsedFrames.length === 0,
    "high-water split prefix was unexpectedly parsed or truncated");

    const second = model.ingest(secondChunk);
    assertCondition(second.state === "high-water" &&
        second.nextBytes > MAX_CLIENT_PARSER_BYTES &&
        model.highWaterEvents === 1 && model.highWaterBytes === second.nextBytes,
    "split frame did not take the bounded parser high-water path");
    assertCondition(model.parser.byteLength === 0 &&
        model.clientFrameInspectionDisabled && model.profile.protocolVersion === 776 &&
        model.protocolPhase === "play" && model.playStallTimerActive,
    "high-water fallback discarded profile/phase/watchdog state");
    assertCondition(!model.watermarkSettled() && model.ingressSequence === 2 &&
        model.committedSequence === 0,
    "high-water fallback incorrectly committed an unparsed ingress sequence");

    // This is a complete framed-looking payload but not the phase control
    // marker.  It must not restart parsing from an unknown boundary.
    const opaqueChunk = frame(Buffer.from([0x7f, 0x01]));
    const opaque = model.ingest(opaqueChunk);
    assertCondition(opaque.state === "opaque" &&
        model.clientFrameInspectionDisabled && model.parsedFrames.length === 0,
    "arbitrary post-high-water chunk reopened the parser");

    // A self-contained, payloadless PLAY client tick is a recovery boundary
    // for both PLAY and reconfiguring. A tick-looking frame with a trailing
    // byte is not standalone and must remain opaque.
    const playTickChunk = frame(Buffer.from([model.profile.playClientTickEnd]));
    const tickWithRemainder = Buffer.concat([playTickChunk, Buffer.from([0x00])]);
    const nonStandaloneTick = model.ingest(tickWithRemainder);
    assertCondition(nonStandaloneTick.state === "opaque" &&
        model.clientFrameInspectionDisabled && model.parsedFrames.length === 0,
    "PLAY tick with a remainder incorrectly reopened inspection");
    const recovered = model.ingest(playTickChunk);
    assertCondition(recovered.state === "recovered" && model.recoveries === 1 &&
        !model.clientFrameInspectionDisabled && model.parsedFrames.length === 1 &&
        model.parser.byteLength === 0,
    `standalone control frame did not recover inspection exactly once: ${JSON.stringify({
        state: recovered.state,
        recoveries: model.recoveries,
        disabled: model.clientFrameInspectionDisabled,
        parsed: model.parsedFrames.length,
        parserBytes: model.parser.byteLength,
    })}`);

    const postRecovery = frame(Buffer.from([0x05, 0x42]));
    const resumed = model.ingest(postRecovery, {drain: true});
    assertCondition(resumed.state === "buffered" && model.parsedFrames.length === 2 &&
        model.parser.byteLength === 0 && !model.clientFrameInspectionDisabled,
    "post-recovery framed packet was not drained in order");

    // The high-water sequence is not safe for phase-sensitive rewrites until
    // the recovery frame and the following packet have crossed the parser
    // ownership boundary.  This mirrors clientFrameIngressSequence /
    // clientFrameCommittedSequence in main.js.
    assertCondition(model.ingressSequence === 4 &&
        model.committedSequence === model.ingressSequence &&
        model.watermarkSettled(),
    `high-water recovery watermark did not settle: ${JSON.stringify(model.watermarkSnapshot())}`);

    const expectedRaw = Buffer.concat([
        firstChunk,
        secondChunk,
        opaqueChunk,
        tickWithRemainder,
        playTickChunk,
        postRecovery,
    ]);
    const expectedRawDigest = hashBuffer(expectedRaw);
    assertCondition(model.rawDigest() === expectedRawDigest &&
        model.rawBytes === expectedRaw.byteLength && model.rawWrites.length === 6,
    "high-water raw chunks were duplicated, dropped, or coalesced");
    assertCondition(model.recoveryAttempts === 3 && model.watermarkBypasses >= 1,
        `unexpected post-high-water recovery attempts: ${model.recoveryAttempts}`);
    const configurationModel = new HighWaterSplitIngressModel({
        protocolVersion: 776,
        phase: "configuration",
    });
    configurationModel.clientFrameInspectionDisabled = true;
    assertCondition(!configurationModel.isStandaloneControlFrame(playTickChunk),
        "PLAY tick recovery was accepted outside the PLAY phase");

    const result = {
        parserLimitBytes: MAX_CLIENT_PARSER_BYTES,
        splitPrefixBytes: firstChunk.byteLength,
        crossingChunkBytes: secondChunk.byteLength,
        highWaterBytes: model.highWaterBytes,
        highWaterEvents: model.highWaterEvents,
        rawChunks: model.rawWrites.length,
        rawBytes: model.rawBytes,
        parsedFramesAfterRecovery: model.parsedFrames.length,
        recoveryAttempts: model.recoveryAttempts,
        recoveries: model.recoveries,
        standalonePlayTickRecovery: true,
        standaloneReconfiguringTickRecovery: true,
        profile: model.profile.protocolVersion,
        phase: model.protocolPhase,
        playStallTimerPreserved: model.playStallTimerActive,
        rawSha256: expectedRawDigest,
        rawSha256BeforeClose: expectedRawDigest,
        rawWritesBeforeClose: model.rawWrites.length,
        watermark: {
            ingressSequence: model.ingressSequence,
            committedSequence: model.committedSequence,
            settled: model.watermarkSettled(),
        },
    };
    // The close path is part of the same bounded-state contract.  Keep the
    // digest above for evidence, then prove parser/raw state cannot revive.
    model.close();
    assertCondition(model.closed && model.parser.byteLength === 0 &&
        model.rawWrites.length === 0 && model.rawBytes === 0 &&
        model.closedRawDigest === expectedRawDigest &&
        model.closedRawWrites === 6 && model.closedRawBytes === expectedRaw.byteLength &&
        model.ingressSequence === 0 && model.committedSequence === 0 &&
        model.latestEnqueuedAt === 0 && model.clientFrameInspectionDisabled &&
        !model.packetFramingEnabled && !model.playStallTimerActive,
    "high-water close did not clear parser/raw state");
    const afterClose = model.ingest(playTickChunk);
    assertCondition(afterClose.state === "closed" && model.rawWrites.length === 0 &&
        model.rawDigest() === hashBuffer(Buffer.alloc(0)) &&
        model.closedRawDigest === expectedRawDigest,
    "high-water close allowed a stale continuation or changed the audit digest");
    result.close = {
        reset: true,
        rawSha256: model.closedRawDigest,
        rawWritesCleared: model.rawWrites.length === 0,
        parserBytes: model.parser.byteLength,
        ingressSequence: model.ingressSequence,
        committedSequence: model.committedSequence,
        latestEnqueuedAt: model.latestEnqueuedAt,
    };
    return result;
}

function highWaterControlFrame(protocolVersion, phase, kind) {
    const ids = HIGH_WATER_PROFILE_IDS[protocolVersion];
    assertCondition(ids !== undefined,
        `missing high-water profile ids for ${protocolVersion}`);
    let packetId;
    if (phase === "login" && kind === "loginAcknowledged") {
        packetId = ids.loginAcknowledged;
    }
    else if (phase === "configuration" && kind === "configurationFinish") {
        packetId = ids.configurationFinish;
    }
    else if ((phase === "play" || phase === "reconfiguring") &&
        kind === "playConfigurationAcknowledged") {
        packetId = ids.playConfigurationAcknowledged;
    }
    else if ((phase === "play" || phase === "reconfiguring") &&
        kind === "playClientTickEnd") {
        packetId = ids.playClientTickEnd;
    }
    else {
        throw new Error(`invalid high-water control case: ${protocolVersion}/${phase}/${kind}`);
    }
    return frame(Buffer.from([packetId]));
}

/**
 * Exercise every production standalone-recovery branch.  The parser may only
 * restart on one complete payloadless control frame in the negotiated phase:
 * login acknowledgement, configuration finish, or PLAY/reconfiguring
 * configuration acknowledgement/client tick.  Framed-looking bytes that are
 * coalesced, split, payloadful, or from the wrong phase stay opaque.
 */
function testHighWaterRecoveryMatrix() {
    const cases = [
        [774, "login", "loginAcknowledged"],
        [774, "configuration", "configurationFinish"],
        [774, "play", "playClientTickEnd"],
        [774, "reconfiguring", "playClientTickEnd"],
        [774, "play", "playConfigurationAcknowledged"],
        [774, "reconfiguring", "playConfigurationAcknowledged"],
        [776, "login", "loginAcknowledged"],
        [776, "configuration", "configurationFinish"],
        [776, "play", "playClientTickEnd"],
        [776, "reconfiguring", "playClientTickEnd"],
        [776, "play", "playConfigurationAcknowledged"],
        [776, "reconfiguring", "playConfigurationAcknowledged"],
    ];
    const summaries = [];
    for (const [protocolVersion, phase, kind] of cases) {
        const model = new HighWaterSplitIngressModel({protocolVersion, phase});
        const valid = highWaterControlFrame(protocolVersion, phase, kind);
        const ids = HIGH_WATER_PROFILE_IDS[protocolVersion];
        const validId = decodeVarInt(valid, 1)?.value;
        // Enter the same state produced by appendClientFrameBuffer's bounded
        // high-water branch, without allocating another 16-MiB fixture for
        // every matrix row.
        model.clientFrameInspectionDisabled = true;
        model.highWaterEvents = 1;
        model.highWaterBytes = MAX_CLIENT_PARSER_BYTES + 1;
        model.ingressSequence = 1;
        model.committedSequence = 0;
        model.latestEnqueuedAt = 1;
        model.ingressClock = 1;

        const wrongId = validId === 0x7f ? 0x7e : validId + 1;
        // Login acknowledgement and configuration finish intentionally share
        // packet id 3 in both profiles, so a wire-only model cannot distinguish
        // those phase labels.  Use an empty framed packet for those rows; for
        // PLAY/reconfiguring rows use the login control id as a true phase
        // mismatch (15/16 versus 3).
        const wrongPhaseFrame = (phase === "login" || phase === "configuration")
            ? Buffer.from([0x00])
            : highWaterControlFrame(protocolVersion, "login", "loginAcknowledged");
        const wrongIdFrame = frame(Buffer.from([wrongId]));
        const payloadful = frame(Buffer.from([validId, 0x00]));
        const coalesced = Buffer.concat([valid, Buffer.from([0x00])]);
        const splitAt = Math.max(1, Math.floor(valid.byteLength / 2));
        const splitA = valid.subarray(0, splitAt);
        const splitB = valid.subarray(splitAt);
        const opaqueInputs = [wrongIdFrame, wrongPhaseFrame, payloadful, coalesced,
            splitA, splitB];
        for (const opaque of opaqueInputs) {
            const result = model.ingest(opaque);
            assertCondition(result.state === "opaque" &&
                model.clientFrameInspectionDisabled && model.recoveries === 0 &&
                model.parsedFrames.length === 0,
            `coincidental/split frame reopened ${protocolVersion}/${phase}/${kind}`);
        }
        const recovered = model.ingest(valid);
        assertCondition(recovered.state === "recovered" && model.recoveries === 1 &&
            !model.clientFrameInspectionDisabled && model.parsedFrames.length === 1 &&
            model.parser.byteLength === 0 && model.committedSequence === model.ingressSequence &&
            model.watermarkSettled(),
        `standalone ${protocolVersion}/${phase}/${kind} did not recover exactly once`);

        const expectedRaw = Buffer.concat([...opaqueInputs, valid]);
        const expectedDigest = hashBuffer(expectedRaw);
        assertCondition(model.rawDigest() === expectedDigest &&
            model.rawBytes === expectedRaw.byteLength && model.rawWrites.length === 7,
        `raw digest changed for ${protocolVersion}/${phase}/${kind}`);
        const preCloseSequence = model.ingressSequence;
        const preCloseWatermark = model.committedSequence;
        model.close();
        assertCondition(model.closedRawDigest === expectedDigest &&
            model.closedRawWrites === 7 && model.closedRawBytes === expectedRaw.byteLength &&
            model.rawWrites.length === 0 && model.rawBytes === 0 &&
            model.parser.byteLength === 0 && model.ingressSequence === 0 &&
            model.committedSequence === 0 && model.latestEnqueuedAt === 0 &&
            model.clientFrameInspectionDisabled && !model.packetFramingEnabled,
        `close did not reset ${protocolVersion}/${phase}/${kind} generation state`);
        const postClose = model.ingest(valid);
        assertCondition(postClose.state === "closed" && model.rawWrites.length === 0 &&
            model.closedRawDigest === expectedDigest,
        `closed ${protocolVersion}/${phase}/${kind} accepted stale continuation`);
        summaries.push({
            protocolVersion,
            phase,
            kind,
            validId,
            opaqueInputs: opaqueInputs.length,
            recoveries: 1,
            preCloseIngressSequence: preCloseSequence,
            preCloseCommittedSequence: preCloseWatermark,
            rawBytes: expectedRaw.byteLength,
            rawSha256: expectedDigest,
            closeReset: true,
        });
        // Keep ids referenced in this test so a future profile table edit does
        // not silently remove the version-specific acknowledgement/tick check.
        assertCondition(ids.playClientTickEnd !== undefined &&
            ids.playConfigurationAcknowledged !== undefined,
            `incomplete profile control ids for ${protocolVersion}`);
    }
    return {
        cases: summaries.length,
        recoveredExactlyOnce: summaries.every((entry) => entry.recoveries === 1),
        opaqueInputsPerCase: 6,
        phases: [...new Set(summaries.map((entry) => entry.phase))],
        protocols: [...new Set(summaries.map((entry) => entry.protocolVersion))],
        closeReset: summaries.every((entry) => entry.closeReset),
        summaries,
    };
}

/**
 * Model the explicit client ingress/committed watermark used by the relay's
 * phase-sensitive server rewrites.  Raw WebSocket messages are accepted at
 * ingress, but a server packet observed before the corresponding parser
 * sequence is committed must be forwarded unchanged.  Once the bounded fair
 * queue drains the transition frame, the same packet may be rewritten.  The
 * model also keeps A/B fairness and verifies close resets all sequence/gauge
 * state.
 */
function testPhaseWatermarkBarrier() {
    const makeFrame = (tunnelId, ordinal, messageSequence) => {
        const marker = Buffer.alloc(8);
        marker.writeUInt32LE(ordinal, 0);
        marker.write(tunnelId, 4, 4, "ascii");
        return {
            tunnelId,
            ordinal,
            messageSequence,
            transition: tunnelId === "A" && ordinal === TRANSITION_FRAME_ORDINAL,
            bytes: frame(marker),
        };
    };
    const aFrames = [
        ...Array.from({length: 32}, (_, index) => makeFrame("A", index + 1, 1)),
        ...Array.from({length: 8}, (_, index) => makeFrame(
            "A", index + 33, 2)),
    ];
    const bFrames = [makeFrame("B", 1, 1)];
    const queues = new Map([
        ["A", [...aFrames]],
        ["B", [...bFrames]],
    ]);
    const ready = ["A", "B"];
    const readySet = new Set(ready);
    const ingressSequence = new Map([["A", 2], ["B", 1]]);
    const committedSequence = new Map([["A", 0], ["B", 0]]);
    const latestEnqueuedAt = new Map([["A", 10], ["B", 10]]);
    const bufferedGauge = new Map([
        ["A", aFrames.reduce((sum, entry) => sum + entry.bytes.byteLength, 0)],
        ["B", bFrames.reduce((sum, entry) => sum + entry.bytes.byteLength, 0)],
    ]);
    const phase = new Map([["A", "configuration"], ["B", "configuration"]]);
    const stalePhaseBypasses = new Map([["A", 0], ["B", 0]]);
    const maxStalePhaseLagMs = new Map([["A", 0], ["B", 0]]);
    const output = [];
    const trace = [];
    const serverFrame = Buffer.from("phase-sensitive-server-frame", "utf8");
    const serverDispatches = [];
    let transitionCommits = 0;
    let transitionTurn;
    let turn = 0;

    const phaseWatermarkSettled = (tunnelId, now) => {
        if (committedSequence.get(tunnelId) >= ingressSequence.get(tunnelId)) {
            return true;
        }
        const lag = Math.max(0, now - (latestEnqueuedAt.get(tunnelId) ?? now));
        stalePhaseBypasses.set(
            tunnelId,
            stalePhaseBypasses.get(tunnelId) + 1,
        );
        maxStalePhaseLagMs.set(
            tunnelId,
            Math.max(maxStalePhaseLagMs.get(tunnelId), lag),
        );
        return false;
    };
    const dispatchServerFrame = (tunnelId, label, now) => {
        const settled = phaseWatermarkSettled(tunnelId, now);
        const currentPhase = phase.get(tunnelId);
        const bytes = settled && currentPhase === "play"
            ? Buffer.concat([Buffer.from("rewrite:", "utf8"), serverFrame])
            : Buffer.from(serverFrame);
        serverDispatches.push({
            tunnelId,
            label,
            mode: bytes.equals(serverFrame) ? "raw" : "rewrite",
            phase: currentPhase,
            committedSequence: committedSequence.get(tunnelId),
            bytes,
        });
    };

    // Both A messages are already raw-owned, even though no parser turn has
    // run.  A phase-sensitive server packet must therefore stay untouched.
    dispatchServerFrame("A", "before-parser", 20);
    assertCondition(serverDispatches.at(-1).mode === "raw" &&
        serverDispatches.at(-1).committedSequence === 0 &&
        serverDispatches.at(-1).bytes.equals(serverFrame),
    "uncommitted phase watermark rewrote a server frame");

    while (ready.length !== 0) {
        const tunnelId = ready.shift();
        readySet.delete(tunnelId);
        const queue = queues.get(tunnelId);
        assertCondition(queue !== undefined, `missing phase model queue ${tunnelId}`);
        turn++;
        const turnEntries = [];
        while (queue.length !== 0 && turnEntries.length < MAX_FRAMES_PER_TURN) {
            const entry = queue.shift();
            turnEntries.push(entry);
            output.push({...entry, globalIndex: output.length, turn});
            if (entry.transition) {
                transitionCommits++;
                transitionTurn = turn;
                phase.set(tunnelId, "play");
                // The parser has consumed frame 33, but the enclosing drain
                // turn has not yet published its ingress watermark. A
                // re-entrant server callback must still take the raw path.
                dispatchServerFrame(tunnelId, "same-turn-before-commit", turn * 10);
                assertCondition(serverDispatches.at(-1).mode === "raw" &&
                    serverDispatches.at(-1).phase === "play",
                "phase transition escaped before sequence commit");
            }
        }
        if (queue.length === 0) {
            committedSequence.set(tunnelId, ingressSequence.get(tunnelId));
            bufferedGauge.set(tunnelId, 0);
        }
        trace.push({
            turn,
            tunnelId,
            frames: turnEntries.length,
            firstOrdinal: turnEntries[0]?.ordinal ?? null,
            lastOrdinal: turnEntries.at(-1)?.ordinal ?? null,
            phase: phase.get(tunnelId),
            committedSequence: committedSequence.get(tunnelId),
        });
        if (queue.length !== 0 && !readySet.has(tunnelId)) {
            ready.push(tunnelId);
            readySet.add(tunnelId);
        }
        if (tunnelId === "A" && queue.length === 0) {
            dispatchServerFrame("A", "after-commit", turn * 10 + 1);
        }
    }

    const aOutput = output.filter((entry) => entry.tunnelId === "A");
    const bOutput = output.filter((entry) => entry.tunnelId === "B");
    const expectedA = hashBuffer(Buffer.concat(aFrames.map((entry) => entry.bytes)));
    const expectedB = hashBuffer(Buffer.concat(bFrames.map((entry) => entry.bytes)));
    assertCondition(hashBuffer(Buffer.concat(aOutput.map((entry) => entry.bytes))) === expectedA &&
        hashBuffer(Buffer.concat(bOutput.map((entry) => entry.bytes))) === expectedB,
    "phase watermark fair queue changed per-tunnel frame ownership");
    assertCondition(transitionCommits === 1 && transitionTurn === 4 &&
        committedSequence.get("A") === 2 && phase.get("A") === "play",
    `phase watermark transition was not committed at A turn 4: commits=${transitionCommits}, turn=${transitionTurn}`);
    assertCondition(trace[0]?.tunnelId === "A" && trace[1]?.tunnelId === "B" &&
        trace[0].frames === MAX_FRAMES_PER_TURN &&
        output.findIndex((entry) => entry.tunnelId === "B") === MAX_FRAMES_PER_TURN,
    "phase watermark queue did not preserve A/B fairness");

    const beforeCommit = serverDispatches.filter((entry) => entry.mode === "raw");
    const afterCommit = serverDispatches.filter((entry) => entry.mode === "rewrite");
    assertCondition(beforeCommit.length === 2 && afterCommit.length === 1 &&
        beforeCommit.every((entry) => entry.bytes.equals(serverFrame)) &&
        afterCommit[0].committedSequence === 2 &&
        afterCommit[0].phase === "play" &&
        !afterCommit[0].bytes.equals(serverFrame),
    "phase watermark did not split raw-before-commit and rewrite-after-commit paths");
    assertCondition(stalePhaseBypasses.get("A") === 2 &&
        maxStalePhaseLagMs.get("A") >= 0,
    "stale phase bypass telemetry was not recorded at both pre-commit points");

    // Close is a generation boundary: no stale sequence or buffered gauge may
    // survive to a later tunnel callback.
    ingressSequence.set("A", 0);
    committedSequence.set("A", 0);
    latestEnqueuedAt.set("A", 0);
    bufferedGauge.set("A", 0);
    queues.get("A").length = 0;
    assertCondition(ingressSequence.get("A") === 0 &&
        committedSequence.get("A") === 0 && latestEnqueuedAt.get("A") === 0 &&
        bufferedGauge.get("A") === 0 && queues.get("A").length === 0,
    "phase watermark close did not clear sequence/gauge state");

    return {
        ingressSequence: {A: 2, B: 1},
        committedSequenceBeforeClose: {A: 2, B: 1},
        transitionFrameOrdinal: TRANSITION_FRAME_ORDINAL,
        transitionTurn,
        transitionCommits,
        rawBeforeCommit: beforeCommit.length,
        rewritesAfterCommit: afterCommit.length,
        stalePhaseBypasses: stalePhaseBypasses.get("A"),
        maxStalePhaseLagMs: maxStalePhaseLagMs.get("A"),
        bFirstWriteIndex: output.findIndex((entry) => entry.tunnelId === "B"),
        closeState: {
            ingressSequence: ingressSequence.get("A"),
            committedSequence: committedSequence.get("A"),
            latestEnqueuedAt: latestEnqueuedAt.get("A"),
            bufferedGauge: bufferedGauge.get("A"),
        },
        trace,
    };
}

async function testProtocolBranchModel() {
    const handshake = encodeHandshake(776);
    const remainderPayload = Buffer.alloc(32, 0xa6);
    remainderPayload.writeUInt32LE(0x77600001, 0);
    const remainder = frame(remainderPayload);
    const combined = Buffer.concat([handshake, remainder]);
    const splitAt = Math.max(1, Math.floor(handshake.byteLength / 2));

    // 1) A handshake split over WebSocket messages must retain only the
    // bounded sniffing copy, then parse the post-handshake remainder once.
    const split = new ProtocolIngressModel();
    split.handleBinary(combined.subarray(0, splitAt));
    assertCondition(!split.minecraftHandshakeSeen &&
        split.handshakeBuffer.byteLength !== 0,
    "split handshake was not retained as an incomplete probe");
    split.handleBinary(combined.subarray(splitAt));
    assertCondition(split.minecraftHandshakeSeen &&
        split.packetFramingEnabled && split.minecraftProfile?.protocolVersion === 776,
    "complete 776 handshake did not enable the selected profile");
    assertCondition(split.handshakeBuffer.byteLength === 0,
        "handshake sniff buffer was not cleared after profile selection");
    assertCondition(split.clientParsedFrames === 1 && split.clientFrameBuffer.byteLength === 0,
        "handshake remainder was not parsed exactly once");
    assertCondition(split.clientRawDigest() === hashBuffer(combined),
        "split handshake/remainder raw write hash changed");
    assertCondition(split.clientRawWrites.length === 2,
        "split handshake raw chunks were duplicated or coalesced unexpectedly");

    // 2) Unsupported protocol and malformed/opaque traffic must stay raw and
    // must never be promoted back into a guessed profile later.
    const opaque = new ProtocolIngressModel();
    const unsupported = Buffer.concat([
        encodeHandshake(775),
        Buffer.from("opaque-prelude", "utf8"),
    ]);
    const opaqueFramedLooking = frame(Buffer.from("still-opaque", "utf8"));
    opaque.handleBinary(unsupported);
    opaque.handleBinary(opaqueFramedLooking);
    assertCondition(opaque.minecraftHandshakeSeen && opaque.keepAliveProxyOpaque &&
        !opaque.packetFramingEnabled && opaque.minecraftProfile === undefined,
    "unsupported handshake was not pinned to opaque mode");
    assertCondition(opaque.clientParsedFrames === 0,
        "opaque traffic was incorrectly parsed as framed packets");
    assertCondition(opaque.clientRawDigest() === hashBuffer(
        Buffer.concat([unsupported, opaqueFramedLooking])),
    "opaque raw bytes were not forwarded exactly once");

    // 3) Login encryption response is a one-way boundary: the response bytes
    // remain raw while any parser remainder is discarded and the profile is
    // cleared.  A later framed-looking message cannot reopen inspection.
    const encrypted = new ProtocolIngressModel();
    encrypted.handleBinary(handshake);
    encrypted.markEncryptionResponsePending();
    const encryptedBytes = Buffer.from([
        0x00, 0xff, 0x7f, 0x80, 0x80, 0x80, 0x01, 0x2a,
    ]);
    encrypted.handleBinary(encryptedBytes);
    encrypted.handleBinary(opaqueFramedLooking);
    assertCondition(encrypted.keepAliveProxyEncryptionOpaque &&
        encrypted.keepAliveProxyOpaque && !encrypted.packetFramingEnabled &&
        encrypted.minecraftProfile === undefined,
    "encryption response did not force the opaque transition");
    assertCondition(encrypted.clientParsedFrames === 0 &&
        encrypted.clientFrameBuffer.byteLength === 0,
    "encrypted bytes entered the framed parser");
    assertCondition(encrypted.clientRawDigest() === hashBuffer(
        Buffer.concat([handshake, encryptedBytes, opaqueFramedLooking])),
    "encrypted/opaque raw writes were not exactly-once");

    // 4) `flow` is a server->browser read pause.  It must queue server frames,
    // echo the control state, and still allow the independent client->TCP raw
    // path to make progress.  Resume drains the retained server bytes in order.
    const flow = new ProtocolIngressModel();
    const serverA = Buffer.from("server-A", "utf8");
    const serverB = Buffer.from("server-B", "utf8");
    flow.handleControl({type: "flow", paused: true});
    flow.receiveServerFrame(serverA);
    flow.receiveServerFrame(serverB);
    assertCondition(flow.serverFrameWrites.length === 0 &&
        flow.serverFramePending.length === 2,
    "flow pause did not retain server frames");
    const clientFlowBytes = Buffer.from("client-while-server-paused", "utf8");
    flow.handleBinary(clientFlowBytes);
    assertCondition(flow.clientRawDigest() === hashBuffer(clientFlowBytes),
        "flow pause incorrectly blocked client->TCP writes");
    flow.handleControl({type: "flow", paused: false});
    assertCondition(flow.serverFramePending.length === 0 &&
        flow.serverDigest() === hashBuffer(Buffer.concat([serverA, serverB])),
    "flow resume did not drain server frames in order");
    assertCondition(flow.controlReplies.length === 2 &&
        flow.controlReplies[0].paused === true && flow.controlReplies[1].paused === false,
    "flow control echo sequence changed");

    // 5) Close must retire every pending queue/parser state and prevent a
    // continuation from reviving after the WebSocket/TCP pair is gone.
    const closing = new ProtocolIngressModel();
    closing.setTcpWritable(false);
    closing.handleBinary(Buffer.from("pending-client", "utf8"));
    closing.handleControl({type: "flow", paused: true});
    closing.receiveServerFrame(Buffer.from("pending-server", "utf8"));
    closing.close();
    assertCondition(closing.closed && closing.clientRawPending.length === 0 &&
        closing.serverFramePending.length === 0 && closing.clientFrameBuffer.byteLength === 0 &&
        closing.handshakeBuffer.byteLength === 0 && !closing.clientDrainScheduled,
    "close cleanup left client/parser/server continuation state alive");
    const beforeClosedWrites = closing.clientRawWrites.length;
    closing.handleBinary(Buffer.from("after-close", "utf8"));
    assertCondition(closing.clientRawWrites.length === beforeClosedWrites,
        "closed tunnel accepted a post-close client write");

    return {
        handshake: {
            protocol: 776,
            splitBytes: splitAt,
            rawChunks: split.clientRawWrites.length,
            parsedRemainderFrames: split.clientParsedFrames,
        },
        unsupportedOpaque: {
            parsedFrames: opaque.clientParsedFrames,
            transitions: opaque.transitions,
        },
        encryptionOpaque: {
            parsedFrames: encrypted.clientParsedFrames,
            transitions: encrypted.transitions,
        },
        flow: {
            replies: flow.controlReplies.length,
            serverFramesAfterResume: flow.serverFrameWrites.length,
            clientWritesWhilePaused: flow.clientRawWrites.length,
        },
        close: {
            clientPending: closing.clientRawPending.length,
            serverPending: closing.serverFramePending.length,
            parserBytes: closing.clientFrameBuffer.byteLength,
        },
    };
}

async function assertMainProtocolBranchContract(requireBounded) {
    const bridgeSource = await readFile(new URL("./dist/main.js", import.meta.url), "utf8");
    const checks = {
        binaryDispatch: /webSocket\.on\("message", \(data, binary\)/.test(bridgeSource),
        flowControlBranch: /if \(!binary\)/.test(bridgeSource) &&
            /message\?\.type !== "flow"/.test(bridgeSource),
        handshakeProbe: /!packetFramingEnabled && config\.proxyKeepAlives && !minecraftHandshakeSeen/.test(bridgeSource),
        handshakeRemainder: /frameClientData = handshake\.remainder/.test(bridgeSource),
        framedParserBranch: /packetFramingEnabled && frameClientData !== undefined/.test(bridgeSource),
        opaqueFallback: /parsed === null/.test(bridgeSource) &&
            /transitionKeepAliveProxyToOpaque/.test(bridgeSource),
        encryptionBoundary: /if \(encryptionResponsePending\)/.test(bridgeSource),
        rawTcpWrite: /tcpSocket\.write\(clientData\)/.test(bridgeSource),
        clientBackpressure: /armClientTcpBackpressure\(\)/.test(bridgeSource),
        flowPauseResume: /webSocket\.pause\(\)/.test(bridgeSource) &&
            /webSocket\.resume\(\)/.test(bridgeSource),
        closeCleanup: /clearClientTcpBackpressure\(\)/.test(bridgeSource) &&
            /clearServerFrameState\(\)/.test(bridgeSource) &&
            /clearClientFrameState\(\)/.test(bridgeSource),
        boundedServerDrain: /maximumServerFrameDrainFrames/.test(bridgeSource) &&
            /maximumServerFrameDrainBytes/.test(bridgeSource) &&
            /maximumServerFrameDrainMillis/.test(bridgeSource),
        clientParserHighWater: /maximumClientFrameParserBytes\s*=\s*16\s*\*\s*1024\s*\*\s*1024\s*\+\s*5/.test(bridgeSource),
        clientHighWaterFallback: /nextBytes\s*>\s*maximumClientFrameParserBytes/.test(bridgeSource) &&
            /parserHighWaterEvents/.test(bridgeSource) &&
            /disabled keepalive proxy after client parser high-water/.test(bridgeSource),
        standaloneControlRecovery: /isStandaloneClientControlFrame/.test(bridgeSource) &&
            /recoverStandaloneClientControlFrame/.test(bridgeSource) &&
            /clientFrameInspectionDisabled\s*=\s*false/.test(bridgeSource),
        standaloneRecoveryPhases: /protocolPhase === "login"/.test(bridgeSource) &&
            /protocolPhase === "configuration"/.test(bridgeSource) &&
            /protocolPhase === "play" \|\| protocolPhase === "reconfiguring"/.test(bridgeSource),
        standaloneRecoveryIds: /serverboundLoginAcknowledged/.test(bridgeSource) &&
            /serverboundFinish/.test(bridgeSource) &&
            /serverboundConfigurationAcknowledged/.test(bridgeSource) &&
            /serverboundClientTickEnd/.test(bridgeSource),
        phaseWatermarkState: /clientFrameIngressSequence/.test(bridgeSource) &&
            /clientFrameCommittedSequence/.test(bridgeSource) &&
            /clientFrameLatestEnqueuedAt/.test(bridgeSource) &&
            /clientFramePhaseWatermarkSettled/.test(bridgeSource),
        stalePhaseTelemetry: /stalePhaseBypasses/.test(bridgeSource) &&
            /maxStalePhaseLagMs/.test(bridgeSource),
    };
    for (const [name, present] of Object.entries(checks)) {
        assertCondition(present, `main.js protocol branch marker missing: ${name}`);
    }
    // Only treat a loop *inside the WebSocket message callback* as legacy.
    // A bounded implementation is expected to keep the same while loop in a
    // separate drain function; matching the whole file would flag that fixed
    // loop as unbounded again.
    const messageMarker = 'webSocket.on("message", (data, binary) => {';
    const drainMarker = bridgeSource.indexOf("const drainClientFrameBuffer");
    const handlerStart = bridgeSource.indexOf(messageMarker, Math.max(0, drainMarker));
    const handlerEnd = handlerStart < 0
        ? -1
        : bridgeSource.indexOf("\n            });", handlerStart);
    const clientMessageHandler = handlerStart >= 0
        ? bridgeSource.slice(handlerStart, handlerEnd >= 0 ? handlerEnd : undefined)
        : "";
    const legacyUnboundedClientLoop =
        /while \(clientFrameBuffer\.byteLength > 0\)/.test(clientMessageHandler);
    const boundedClientDrain =
        /maximumClientFrameDrainFrames/.test(bridgeSource) &&
        /maximumClientFrameDrainBytes/.test(bridgeSource) &&
        /maximumClientFrameDrainMillis/.test(bridgeSource) &&
        /scheduleClientFrameDrain/.test(bridgeSource) &&
        /setImmediate/.test(bridgeSource);
    const fairClientQueue =
        /client(?:Inbound|Frame)(?:Message|Ingress|Ready)?Queue/.test(bridgeSource) &&
        /clientFrameDrainScheduled/.test(bridgeSource);
    const handlerOnlyEnqueue =
        /scheduleClientFrameDrain\(\)/.test(clientMessageHandler) &&
        !/\bdrainClientFrameBuffer\s*\(/.test(clientMessageHandler);
    const queueCleanup =
        /clientFrameDrainRegistration\.retired\s*=\s*true/.test(bridgeSource) &&
        /removeClientFrameReady\(clientFrameDrainRegistration\)/.test(bridgeSource);
    const schedulerTelemetry =
        /clientFrameReadyQueueDepth/.test(bridgeSource) &&
        /clientFrameReadyQueueMaxDepth/.test(bridgeSource) &&
        /clientFrameSchedulerTurns/.test(bridgeSource) &&
        /clientFrameSchedulerEnqueues/.test(bridgeSource);
    const processFrameStart = bridgeSource.indexOf("const processClientFrame");
    const processFrameEnd = bridgeSource.indexOf("const drainClientFrameBuffer", processFrameStart);
    const processFrameSource = processFrameStart >= 0
        ? bridgeSource.slice(processFrameStart,
            processFrameEnd >= 0 ? processFrameEnd : undefined)
        : "";
    const consumeOwnershipOffset = processFrameSource.indexOf(
        "clientFrameBuffer.consume(parsed.frameBytes)");
    const phaseAssignmentOffset = processFrameSource.search(/protocolPhase\s*=\s*/);
    const phaseCommitAfterConsume = consumeOwnershipOffset >= 0 &&
        phaseAssignmentOffset > consumeOwnershipOffset;
    const proxyCallOffsets = [...bridgeSource.matchAll(/proxyVanillaKeepAlive\s*\(/g)]
        .map((match) => match.index ?? -1)
        .filter((offset) => offset >= 0);
    // The first occurrence is the function declaration. Every invocation must
    // be inside a phase-watermark guard before it can rewrite a server frame.
    const phaseRewriteGuards = proxyCallOffsets.length > 1 &&
        proxyCallOffsets.slice(1).every((offset) => {
            const prefix = bridgeSource.slice(Math.max(0, offset - 320), offset);
            return prefix.includes("clientFramePhaseWatermarkSettled()");
        });
    const watermarkCloseReset = /clientFrameIngressSequence\s*=\s*0/.test(bridgeSource) &&
        /clientFrameCommittedSequence\s*=\s*0/.test(bridgeSource) &&
        /clientFrameLatestEnqueuedAt\s*=\s*0/.test(bridgeSource);
    if (requireBounded) {
        assertCondition(!legacyUnboundedClientLoop && boundedClientDrain && fairClientQueue &&
            handlerOnlyEnqueue && queueCleanup && schedulerTelemetry &&
            checks.clientParserHighWater && checks.clientHighWaterFallback &&
            checks.standaloneControlRecovery && checks.standaloneRecoveryPhases &&
            checks.standaloneRecoveryIds && checks.phaseWatermarkState &&
            checks.stalePhaseTelemetry && phaseCommitAfterConsume &&
            phaseRewriteGuards && watermarkCloseReset,
        "main.js client drain lacks bounded queue/high-water/phase-watermark invariants");
    }
    return {
        branchChecks: checks,
        legacyUnboundedClientLoop,
        boundedClientDrain,
        fairClientQueue,
        handlerOnlyEnqueue,
        queueCleanup,
        schedulerTelemetry,
        phaseCommitAfterConsume,
        phaseRewriteGuards,
        watermarkCloseReset,
        productGate: !legacyUnboundedClientLoop && boundedClientDrain && fairClientQueue
            && handlerOnlyEnqueue && queueCleanup && schedulerTelemetry &&
            checks.clientParserHighWater && checks.clientHighWaterFallback &&
            checks.standaloneControlRecovery && checks.standaloneRecoveryPhases &&
            checks.standaloneRecoveryIds && checks.phaseWatermarkState &&
            checks.stalePhaseTelemetry && phaseCommitAfterConsume &&
            phaseRewriteGuards && watermarkCloseReset
            ? "ready" : "pending",
    };
}

async function selfTest() {
    await testSingleSixteenMiBFrame();
    const fairness = await testMultiFrameFairness();
    const perMessageCounterexample = testPerMessageBurstCounterexample();
    const transitionAtFrame33 = testTransitionAtFrame33QueuedFairness();
    const highWaterSplitFrame = testHighWaterSplitFrameBoundary();
    const highWaterRecoveryMatrix = testHighWaterRecoveryMatrix();
    const phaseWatermark = testPhaseWatermarkBarrier();
    const protocolBranches = await testProtocolBranchModel();
    const staticContract = await assertMainProtocolBranchContract(
        process.argv.includes("--require-bounded"));
    console.log(JSON.stringify({
        smoke: "client-tcp-bounded-drain",
        status: "pass",
        maxFramesPerTurn: MAX_FRAMES_PER_TURN,
        maxBytesPerTurn: MAX_BYTES_PER_TURN,
        sixteenMiB: "fragmented-single-frame-exactly-once",
        fairness,
        perMessageCounterexample,
        transitionAtFrame33,
        highWaterSplitFrame,
        highWaterRecoveryMatrix,
        phaseWatermark,
        protocolBranches,
        staticContract,
    }));
}

await selfTest();
