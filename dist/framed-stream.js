// Incremental byte storage for Minecraft's length-prefixed TCP stream.
//
// The relay receives arbitrary TCP chunks.  Buffer.concat() on every append
// turns a fragmented stream into an O(n^2) copy pattern and retains a second
// full copy of the parser remainder.  These two small types keep ownership of
// the original chunks, consume them with a head index, and materialize a
// complete frame only when a WebSocket send/packet inspection actually needs
// a contiguous Buffer.

function asBuffer(chunk) {
    if (Buffer.isBuffer(chunk)) {
        return chunk;
    }
    if (chunk instanceof Uint8Array) {
        return Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
    }
    throw new TypeError("framed stream chunks must be Buffer or Uint8Array");
}

export class ByteChunkDeque {
    constructor() {
        this.chunks = [];
        this.head = 0;
        this.headOffset = 0;
        this.byteLength = 0;
    }

    append(chunk) {
        const buffer = asBuffer(chunk);
        if (buffer.byteLength === 0) {
            return;
        }
        this.chunks.push(buffer);
        this.byteLength += buffer.byteLength;
    }

    peekChunk() {
        if (this.byteLength === 0) {
            return undefined;
        }
        const chunk = this.chunks[this.head];
        return this.headOffset === 0 ? chunk : chunk.subarray(this.headOffset);
    }

    byteAt(offset) {
        if (!Number.isInteger(offset) || offset < 0 || offset >= this.byteLength) {
            return undefined;
        }
        let index = this.head;
        let relative = this.headOffset + offset;
        while (index < this.chunks.length) {
            const chunk = this.chunks[index];
            if (relative < chunk.byteLength) {
                return chunk[relative];
            }
            relative -= chunk.byteLength;
            index++;
        }
        return undefined;
    }

    cursor(offset = 0) {
        if (!Number.isInteger(offset) || offset < 0 || offset > this.byteLength) {
            throw new RangeError(`invalid deque cursor offset: ${offset}`);
        }
        let index = this.head;
        let relative = this.headOffset + offset;
        while (index < this.chunks.length && relative >= this.chunks[index].byteLength) {
            relative -= this.chunks[index].byteLength;
            index++;
        }
        let remaining = this.byteLength - offset;
        return {
            get remaining() {
                return remaining;
            },
            next: () => {
                if (remaining === 0) {
                    return undefined;
                }
                const chunk = this.chunks[index];
                const value = chunk[relative];
                relative++;
                remaining--;
                if (relative === chunk.byteLength) {
                    index++;
                    relative = 0;
                }
                return value;
            },
            skip: (length) => {
                if (!Number.isInteger(length) || length < 0 || length > remaining) {
                    return false;
                }
                let amount = length;
                while (amount > 0) {
                    const chunk = this.chunks[index];
                    const available = chunk.byteLength - relative;
                    const step = Math.min(amount, available);
                    relative += step;
                    amount -= step;
                    remaining -= step;
                    if (relative === chunk.byteLength) {
                        index++;
                        relative = 0;
                    }
                }
                return true;
            },
        };
    }

    copyRange(length) {
        if (!Number.isInteger(length) || length < 0 || length > this.byteLength) {
            throw new RangeError(`cannot copy ${length} bytes from ${this.byteLength}`);
        }
        if (length === 0) {
            return Buffer.alloc(0);
        }
        const first = this.peekChunk();
        if (first !== undefined && first.byteLength >= length) {
            return first.subarray(0, length);
        }
        const result = Buffer.allocUnsafe(length);
        let written = 0;
        let index = this.head;
        let offset = this.headOffset;
        while (written < length && index < this.chunks.length) {
            const chunk = this.chunks[index].subarray(offset);
            const amount = Math.min(chunk.byteLength, length - written);
            chunk.copy(result, written, 0, amount);
            written += amount;
            index++;
            offset = 0;
        }
        if (written !== length) {
            throw new Error(`byte deque copy underflow: ${written}/${length}`);
        }
        return result;
    }

    consume(length) {
        if (!Number.isInteger(length) || length < 0 || length > this.byteLength) {
            throw new RangeError(`cannot consume ${length} bytes from ${this.byteLength}`);
        }
        let remaining = length;
        while (remaining > 0) {
            const chunk = this.chunks[this.head];
            const available = chunk.byteLength - this.headOffset;
            if (remaining < available) {
                this.headOffset += remaining;
                this.byteLength -= remaining;
                remaining = 0;
                break;
            }
            remaining -= available;
            this.byteLength -= available;
            this.head++;
            this.headOffset = 0;
            if (this.head === this.chunks.length) {
                // Reset only after the deque is empty.  No Array#shift is used
                // on the hot path, so fragmented input remains amortized O(1).
                this.chunks = [];
                this.head = 0;
                break;
            }
        }
        // A long-lived stream can have a partial tail while many complete
        // chunks have already been consumed.  Compact occasionally, never on
        // every consume, to keep retained metadata bounded.
        if (this.head >= 1024 && this.head * 2 >= this.chunks.length) {
            this.chunks = this.chunks.slice(this.head);
            this.head = 0;
        }
    }

    clear() {
        this.chunks = [];
        this.head = 0;
        this.headOffset = 0;
        this.byteLength = 0;
    }
}

function parseLength(deque) {
    let value = 0;
    for (let index = 0; index < 5; index++) {
        const current = deque.byteAt(index);
        if (current === undefined) {
            return undefined;
        }
        value += (current & 0x7f) * 2 ** (index * 7);
        if ((current & 0x80) === 0) {
            // Match the bridge's historical bitwise VarInt parser: negative
            // five-byte lengths are opaque, never an allocation request.
            if (value > 0x7fffffff) {
                return null;
            }
            return {value, headerBytes: index + 1};
        }
    }
    return null;
}

export class MinecraftFrameAccumulator {
    constructor(maximumFrameBytes) {
        if (!Number.isSafeInteger(maximumFrameBytes) || maximumFrameBytes < 0) {
            throw new RangeError(`invalid maximum frame size: ${maximumFrameBytes}`);
        }
        this.maximumFrameBytes = maximumFrameBytes;
        this.deque = new ByteChunkDeque();
        this._peekCache = undefined;
        this.appendedChunks = 0;
        this.coalescedFrames = 0;
        this.coalescedBytes = 0;
    }

    get byteLength() {
        return this.deque.byteLength;
    }

    append(chunk) {
        const buffer = asBuffer(chunk);
        if (buffer.byteLength === 0) {
            return;
        }
        this.deque.append(buffer);
        this.appendedChunks++;
        if (this._peekCache?.kind === "incomplete") {
            this._peekCache = undefined;
        }
    }

    peekChunk() {
        return this.deque.peekChunk();
    }

    peekFrame() {
        if (this._peekCache !== undefined) {
            return this._peekCache.kind === "incomplete"
                ? undefined
                : this._peekCache.value;
        }
        const length = parseLength(this.deque);
        if (length === undefined) {
            this._peekCache = {kind: "incomplete"};
            return undefined;
        }
        if (length === null) {
            this._peekCache = {kind: "opaque", value: null};
            return null;
        }
        if (length.value > this.maximumFrameBytes) {
            this._peekCache = {kind: "opaque", value: null};
            return null;
        }
        const frameBytes = length.headerBytes + length.value;
        if (this.byteLength < frameBytes) {
            this._peekCache = {kind: "incomplete"};
            return undefined;
        }
        const first = this.deque.peekChunk();
        const coalesced = first === undefined || first.byteLength < frameBytes;
        const frame = this.deque.copyRange(frameBytes);
        if (coalesced) {
            this.coalescedFrames++;
            this.coalescedBytes += frameBytes;
        }
        const value = {
            frame,
            frameBytes,
            headerBytes: length.headerBytes,
            coalesced,
        };
        this._peekCache = {kind: "complete", value};
        return value;
    }

    consumeFrame(frame = this.peekFrame()) {
        if (frame === undefined || frame === null) {
            return false;
        }
        this.consume(frame.frameBytes ?? frame.frame.byteLength);
        return true;
    }

    consume(length) {
        this.deque.consume(length);
        this._peekCache = undefined;
    }

    countCompleteFrames(skipBytes = 0) {
        const cursor = this.deque.cursor();
        let remaining = this.byteLength;
        let skip = Math.max(0, Math.min(skipBytes, this.byteLength));
        let count = 0;
        while (remaining > 0) {
            const header = parseLengthFromCursor(cursor);
            if (header === undefined || header === null ||
                header.value > this.maximumFrameBytes) {
                break;
            }
            const frameBytes = header.headerBytes + header.value;
            if (remaining < frameBytes || !cursor.skip(header.value)) {
                break;
            }
            remaining -= frameBytes;
            if (skip >= frameBytes) {
                skip -= frameBytes;
            }
            else {
                count++;
            }
        }
        return count;
    }

    clear() {
        this.deque.clear();
        this._peekCache = undefined;
    }
}

function parseLengthFromCursor(cursor) {
    let value = 0;
    for (let index = 0; index < 5; index++) {
        const current = cursor.next();
        if (current === undefined) {
            return undefined;
        }
        value += (current & 0x7f) * 2 ** (index * 7);
        if ((current & 0x80) === 0) {
            return value > 0x7fffffff
                ? null
                : {value, headerBytes: index + 1};
        }
    }
    return null;
}
