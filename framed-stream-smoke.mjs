import { createHash } from "node:crypto";
import { ByteChunkDeque, MinecraftFrameAccumulator } from "./dist/framed-stream.js";

const MAX_FRAME = 32 * 1024 * 1024;

function encodeVarInt(value) {
    const bytes = [];
    do {
        let current = value & 0x7f;
        value >>>= 7;
        if (value !== 0) current |= 0x80;
        bytes.push(current);
    } while (value !== 0);
    return Buffer.from(bytes);
}

function frame(payload, header = encodeVarInt(payload.byteLength)) {
    return Buffer.concat([header, payload]);
}

function deterministicBytes(length, seed = 0x41) {
    const result = Buffer.allocUnsafe(length);
    let state = seed >>> 0;
    for (let index = 0; index < result.length; index++) {
        state = (state * 1664525 + 1013904223) >>> 0;
        result[index] = state >>> 24;
    }
    return result;
}

function assert(condition, message) {
    if (!condition) throw new Error(message);
}

function testZeroCopyAndHeadIndex() {
    const accumulator = new MinecraftFrameAccumulator(1024);
    const payload = Buffer.from("zero-copy-frame");
    const input = frame(payload);
    accumulator.append(input);
    const parsed = accumulator.peekFrame();
    assert(parsed?.frame.buffer === input.buffer, "contiguous frame was copied");
    assert(parsed?.coalesced === false, "contiguous frame was marked coalesced");
    assert(accumulator.consumeFrame(parsed), "contiguous frame was not consumed");
    assert(accumulator.byteLength === 0, "deque did not become empty");
    assert(accumulator.deque.head === 0, "empty deque head was not reset");
}

function testFragmentedSixteenMiBFrame() {
    const payload = deterministicBytes(16 * 1024 * 1024, 0x26);
    const expected = frame(payload);
    const accumulator = new MinecraftFrameAccumulator(MAX_FRAME);
    let offset = 0;
    let chunks = 0;
    while (offset < expected.length) {
        const size = Math.min(64 * 1024, expected.length - offset);
        accumulator.append(expected.subarray(offset, offset + size));
        offset += size;
        chunks++;
    }
    const parsed = accumulator.peekFrame();
    assert(parsed !== undefined && parsed !== null, "16MiB fragmented frame incomplete");
    assert(parsed.coalesced === true, "fragmented frame was not coalesced");
    assert(parsed.frame.equals(expected), "16MiB fragmented frame changed bytes");
    assert(accumulator.peekFrame() === parsed, "complete frame peek cache was not reused");
    assert(accumulator.coalescedFrames === 1, "16MiB frame was coalesced more than once");
    assert(accumulator.coalescedBytes === expected.byteLength, "coalesced byte count mismatch");
    assert(accumulator.appendedChunks === chunks, "chunk count mismatch");
    accumulator.consumeFrame(parsed);
    assert(accumulator.byteLength === 0, "16MiB accumulator did not drain");
}

function testRandomFragmentation() {
    const random = (seed) => {
        let state = seed >>> 0;
        return () => (state = (state * 1103515245 + 12345) >>> 0);
    };
    const next = random(0x776774);
    const frames = Array.from({length: 256}, (_, index) => {
        const payloadLength = next() % 32768;
        return frame(deterministicBytes(payloadLength, index + 1));
    });
    const expectedHash = createHash("sha256");
    const accumulator = new MinecraftFrameAccumulator(MAX_FRAME);
    let consumed = 0;
    let frameIndex = 0;
    for (const input of frames) {
        expectedHash.update(input);
        let offset = 0;
        while (offset < input.byteLength) {
            const chunkLength = Math.min(1 + (next() % 4096), input.byteLength - offset);
            accumulator.append(input.subarray(offset, offset + chunkLength));
            offset += chunkLength;
            while (true) {
                const parsed = accumulator.peekFrame();
                if (parsed === undefined) break;
                assert(parsed !== null, `random frame ${frameIndex} became opaque`);
                assert(parsed.frame.equals(frames[frameIndex]),
                    `random frame ${frameIndex} changed bytes`);
                accumulator.consumeFrame(parsed);
                consumed++;
                frameIndex++;
            }
        }
    }
    assert(frameIndex === frames.length && consumed === frames.length,
        "random fragmentation frame count mismatch");
    assert(accumulator.byteLength === 0, "random fragmentation left bytes");
    assert(expectedHash.digest("hex").length === 64, "hash test did not execute");
}

function testSequentialCompleteFrameCount() {
    const accumulator = new MinecraftFrameAccumulator(4096);
    const frames = [];
    for (let index = 0; index < 4096; index++) {
        const packet = frame(Buffer.from([index & 0xff, (index >>> 8) & 0xff]));
        frames.push(packet);
        // One chunk per frame intentionally exercises the forward cursor used
        // by the retained-frame telemetry path; it must not rescan from head.
        accumulator.append(packet);
    }
    assert(accumulator.countCompleteFrames() === frames.length,
        "sequential complete-frame count mismatch");
    for (const packet of frames) {
        const parsed = accumulator.peekFrame();
        assert(parsed?.frame.equals(packet), "sequential frame peek mismatch");
        accumulator.consumeFrame(parsed);
    }
    assert(accumulator.byteLength === 0, "sequential frame count test left bytes");
}

function testTruncatedOversizeAndFiveByteVarInts() {
    const accumulator = new MinecraftFrameAccumulator(64);
    accumulator.append(Buffer.from([0x05]));
    assert(accumulator.peekFrame() === undefined, "truncated VarInt was accepted");
    accumulator.append(Buffer.from([0x5a]));
    assert(accumulator.peekFrame() === undefined, "truncated body was accepted");
    accumulator.append(Buffer.alloc(4, 0x5a));
    assert(accumulator.peekFrame() !== undefined, "complete five-byte body was not accepted");
    accumulator.clear();
    accumulator.append(Buffer.from([0x80, 0x01]));
    assert(accumulator.peekFrame() === null, "oversize frame was not rejected");
    accumulator.clear();

    const fiveByteZero = frame(Buffer.alloc(0), Buffer.from([0x80, 0x80, 0x80, 0x80, 0x00]));
    accumulator.append(fiveByteZero.subarray(0, 2));
    assert(accumulator.peekFrame() === undefined, "five-byte VarInt became complete too soon");
    accumulator.append(fiveByteZero.subarray(2));
    const parsed = accumulator.peekFrame();
    assert(parsed?.headerBytes === 5 && parsed.frame.equals(fiveByteZero),
        "valid five-byte VarInt was not parsed");
    accumulator.consumeFrame(parsed);
    accumulator.append(Buffer.from([0x80, 0x80, 0x80, 0x80, 0x80]));
    assert(accumulator.peekFrame() === null, "unterminated five-byte VarInt was accepted");
}

function testEncryptionTailOwnership() {
    const accumulator = new MinecraftFrameAccumulator(1024);
    const first = frame(Buffer.from([0x01, 0x02, 0x03]));
    const encryptedTail = deterministicBytes(4096, 0xe1);
    accumulator.append(first.subarray(0, 2));
    accumulator.append(Buffer.concat([first.subarray(2), encryptedTail.subarray(0, 1700)]));
    const parsed = accumulator.peekFrame();
    assert(parsed !== undefined && parsed !== null, "encryption-prefix frame missing");
    accumulator.consumeFrame(parsed);
    assert(accumulator.byteLength === 1700, "encrypted tail was consumed with frame");
    assert(accumulator.peekChunk().equals(encryptedTail.subarray(0, 1700)),
        "encrypted tail bytes changed");
    accumulator.append(encryptedTail.subarray(1700));
    assert(accumulator.byteLength === encryptedTail.byteLength, "encrypted tail append lost bytes");
    accumulator.clear();
    assert(accumulator.byteLength === 0, "encrypted tail cleanup left bytes");
}

function testByteChunkDeque() {
    const deque = new ByteChunkDeque();
    const first = Buffer.from("abc");
    const second = Buffer.from("defgh");
    deque.append(first);
    deque.append(second);
    assert(deque.copyRange(5).toString() === "abcde", "deque range copy mismatch");
    deque.consume(3);
    assert(deque.peekChunk().toString() === "defgh", "deque head offset mismatch");
    deque.consume(5);
    assert(deque.byteLength === 0 && deque.head === 0, "deque did not reset after full consume");
}

testByteChunkDeque();
testZeroCopyAndHeadIndex();
testFragmentedSixteenMiBFrame();
testRandomFragmentation();
testSequentialCompleteFrameCount();
testTruncatedOversizeAndFiveByteVarInts();
testEncryptionTailOwnership();
console.log("framed-stream-smoke: PASS (zero-copy, random fragmentation, 16MiB, truncation, VarInt, encryption tail)");
