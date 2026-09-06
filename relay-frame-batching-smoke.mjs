#!/usr/bin/env node

/**
 * Relay batching boundary smoke.
 *
 * This drives the real MinecraftFrameAccumulator used by dist/main.js.  It
 * verifies that arbitrary TCP fragmentation and batching preserve the exact
 * Minecraft byte stream, while a not-yet-accepted send leaves ownership in
 * the accumulator.  The source checks protect the RelayNode admission,
 * phase, keepalive, and pause boundaries around that accumulator.
 */

import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import {MinecraftFrameAccumulator} from "./dist/framed-stream.js";

const mainSource = await readFile(new URL("./dist/main.js", import.meta.url), "utf8");

function varInt(value) {
    const out = [];
    do {
        let byte = value & 0x7f;
        value >>>= 7;
        if (value !== 0) byte |= 0x80;
        out.push(byte);
    } while (value !== 0);
    return Buffer.from(out);
}

function frame(payload) {
    const body = Buffer.from(payload);
    return Buffer.concat([varInt(body.length), body]);
}

function makePayload(index, length = 1) {
    const body = Buffer.alloc(length, index & 0xff);
    body[0] = index & 0xff;
    return body;
}

function testBurstBatchAndExactOrder() {
    const accumulator = new MinecraftFrameAccumulator(1024 * 1024);
    const frames = Array.from({length: 4096}, (_, index) => frame(makePayload(index, 1 + (index % 17))));
    const expected = Buffer.concat(frames);

    // A burst can be one TCP callback or many arbitrary chunks.  Feed a
    // deterministic mixed pattern to the real accumulator implementation.
    let offset = 0;
    let step = 1;
    while (offset < expected.length) {
        const length = Math.min(step, expected.length - offset);
        accumulator.append(expected.subarray(offset, offset + length));
        offset += length;
        step = step === 97 ? 1 : step + 1;
    }
    assert.equal(accumulator.countCompleteFrames(), frames.length,
        "batched TCP burst lost or merged a Minecraft frame");

    const rebuilt = [];
    for (let index = 0; index < frames.length; index++) {
        const parsed = accumulator.peekFrame();
        assert.ok(parsed, `frame ${index} was not available`);
        rebuilt.push(Buffer.from(parsed.frame));
        accumulator.consumeFrame(parsed);
    }
    assert.deepEqual(Buffer.concat(rebuilt), expected,
        "frame order/bytes changed after batched fragmented delivery");
    assert.equal(accumulator.byteLength, 0, "batched accumulator retained bytes");
    assert.ok(accumulator.coalescedFrames > 0,
        "test did not exercise a frame crossing a TCP chunk boundary");
}

function testRealBatchParserBounds() {
    const accumulator = new MinecraftFrameAccumulator(1024 * 1024);
    const frames = Array.from({length: 64}, (_, index) => frame(makePayload(index, 63)));
    accumulator.append(Buffer.concat(frames));
    const batch = accumulator.peekBatch(16 * 1024, 32);
    assert.equal(batch.length, 32, "real batch parser did not enforce frame bound");
    assert.equal(accumulator.peekBatch(100, 32).length, 1,
        "byte budget must stop before a second complete 64-byte frame");
    assert.ok(batch.reduce((total, item) => total + item.frameBytes, 0) <= 16 * 1024,
        "real batch parser exceeded byte bound");
    assert.deepEqual(Buffer.concat(batch.map((item) => item.frame)),
        Buffer.concat(frames.slice(0, 32)), "real batch parser changed packet order");
    assert.equal(accumulator.byteLength, Buffer.concat(frames).byteLength,
        "read-only batch parser consumed bytes before send acceptance");
    assert.ok(batch.every((item) => item.coalesced === false));
    const split = new MinecraftFrameAccumulator(1024);
    for (const byte of Buffer.concat(frames.slice(0, 2))) {
        split.append(Buffer.from([byte]));
    }
    assert.ok(split.peekBatch(1024, 32).every((item) => item.coalesced === true));
    const limited = new MinecraftFrameAccumulator(2);
    limited.append(Buffer.concat([frame([1]), frame([2, 3, 4]), frame([5])]));
    assert.equal(limited.peekBatch(100, 32).length, 1,
        "oversize packet must stop lookahead before subsequent valid packets");
    limited.consume(2);
    assert.deepEqual(limited.peekBatch(100, 32), []);
    assert.equal(limited.peekFrame(), null);
}

function testSplitMinecraftFrameAndSendOwnership() {
    const accumulator = new MinecraftFrameAccumulator(4096);
    const packet = frame(Buffer.alloc(130, 0x5a));
    accumulator.append(packet.subarray(0, 1));
    assert.equal(accumulator.peekFrame(), undefined, "split VarInt was accepted early");
    accumulator.append(packet.subarray(1, 3));
    assert.equal(accumulator.peekFrame(), undefined, "split body was accepted early");
    accumulator.append(packet.subarray(3));
    const before = accumulator.byteLength;
    const first = accumulator.peekFrame();
    const second = accumulator.peekFrame();
    assert.ok(first && second, "complete split frame missing");
    assert.deepEqual(first.frame, packet, "split frame bytes changed");
    assert.deepEqual(second.frame, packet, "peek changed frame ownership before send acceptance");
    assert.equal(accumulator.byteLength, before,
        "failed/unaccepted send path consumed source bytes");
    accumulator.consumeFrame(first);
    assert.equal(accumulator.byteLength, 0, "accepted frame was not consumed exactly once");
}

function testSourceBoundaries() {
    assert.match(mainSource, /tcpPausedForClient/, "client pause state missing");
    assert.match(mainSource, /tcpSocket\.pause\(\)/, "TCP pause ownership boundary missing");
    assert.match(mainSource, /webSocket\.send\(frame, \{ binary: true \}/,
        "binary frame send path missing");
    assert.match(mainSource, /proxyVanillaKeepAlive\(/,
        "keepalive proxy boundary missing");
    assert.match(mainSource, /protocolPhase === "play"/,
        "protocol phase transitions missing from relay path");
    assert.match(mainSource, /drainServerFrameBuffer = \(\) =>/,
        "server frame drain path missing");
    assert.match(mainSource, /serverFrameDrainHoldingRead/,
        "paused parser remainder ownership guard missing");
    assert.match(mainSource, /result === serverFrameForwardResult\.ENQUEUED/,
        "send acceptance result boundary missing");
    assert.match(mainSource, /serverFrameBuffer\.peekBatch\(\s*16 \* 1024/u,
        "PLAY batch parser is not connected to the real server drain");
    assert.match(mainSource, /sourceKind: "parsed-batch"/u,
        "batched timeline source metadata is missing");
    assert.match(mainSource, /forwardServerFrame\(batchFrame,[\s\S]*batch\.length/u,
        "batched send does not retain packet count telemetry");
    assert.match(mainSource, /if \(keepAlive \|\| isLoginEncryptionRequest/u,
        "batch path does not guard keepalive/encryption boundaries");
}

testBurstBatchAndExactOrder();
testRealBatchParserBounds();
testSplitMinecraftFrameAndSendOwnership();
testSourceBoundaries();
console.log("relay-frame-batching-smoke: PASS (real accumulator burst order, split frame ownership, relay pause/phase/send boundaries)");
