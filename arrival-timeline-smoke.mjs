#!/usr/bin/env node

import assert from "node:assert/strict";

// This is a server-free diagnostic model.  It intentionally does not alter the
// multiplayer runtime or any strict acceptance gate.  The model is useful for
// the next real run because it makes the missing wire/onmessage/decode timeline
// explicit and keeps only bounded evidence.
const SCHEMA_VERSION = "gaius.browser-client-arrival-timeline.v1";
const SLOW_GAP_THRESHOLD_MILLIS = 250;
const PER_CLIENT_EVENT_LIMIT = 64;
const PER_CLIENT_SAMPLE_LIMIT = 64;
const GLOBAL_SAMPLE_LIMIT = 256;
const RECONNECT_TIMELINE_LIMIT = 32;

const RECONNECT_PHASES = Object.freeze([
    "disconnect",
    "reconnect-scheduled",
    "connect-begin",
    "transport-open",
    "login-begin",
    "login-done",
    "configuration-begin",
    "configuration-done",
    "play-enter",
    "first-onmessage",
    "first-decoded-packet",
]);

class ArrivalTimelineRecorder {
    constructor({
        slowThresholdMillis = SLOW_GAP_THRESHOLD_MILLIS,
        perClientEventLimit = PER_CLIENT_EVENT_LIMIT,
        perClientSampleLimit = PER_CLIENT_SAMPLE_LIMIT,
        globalSampleLimit = GLOBAL_SAMPLE_LIMIT,
        reconnectTimelineLimit = RECONNECT_TIMELINE_LIMIT,
    } = {}) {
        assert(Number.isFinite(slowThresholdMillis) && slowThresholdMillis > 0);
        assert(Number.isInteger(perClientEventLimit) && perClientEventLimit > 0);
        assert(Number.isInteger(perClientSampleLimit) && perClientSampleLimit > 0);
        assert(Number.isInteger(globalSampleLimit) && globalSampleLimit > 0);
        assert(Number.isInteger(reconnectTimelineLimit) && reconnectTimelineLimit > 0);
        this.slowThresholdMillis = slowThresholdMillis;
        this.perClientEventLimit = perClientEventLimit;
        this.perClientSampleLimit = perClientSampleLimit;
        this.globalSampleLimit = globalSampleLimit;
        this.reconnectTimelineLimit = reconnectTimelineLimit;
        this.eventsByClient = new Map();
        this.samplesByClient = new Map();
        this.samples = [];
        this.reconnectByClient = new Map();
        this.clientIds = new Set();
        this.previousDecodeEndByClient = new Map();
        this.packetSequence = 0;
        this.sampleSequence = 0;
        this.slowSampleCountTotal = 0;
        this.slowSamplesDropped = 0;
        this.intentionalDropCount = 0;
    }

    recordReconnectPhase({ clientId, wave, phase, at }) {
        this.#validateIdentity(clientId, wave);
        this.clientIds.add(String(clientId));
        assert(RECONNECT_PHASES.includes(phase), `unknown reconnect phase: ${phase}`);
        assert(Number.isFinite(at) && at >= 0, "reconnect phase timestamp must be finite");
        const key = this.#key(clientId, wave);
        const timeline = this.reconnectByClient.get(key) ?? [];
        if (timeline.length >= this.reconnectTimelineLimit) timeline.shift();
        timeline.push({ phase, at });
        this.reconnectByClient.set(key, timeline);
    }

    recordPacket({
        clientId,
        wave = 0,
        frameSeq,
        phase = "steady-soak",
        wireAt = null,
        onmessageAt = null,
        bridgeEnqueueAt = null,
        pollAt = null,
        decodeStartAt = null,
        decodeEndAt = null,
        dispatchAt = null,
        queueDepth = null,
        intentionalDrop = false,
    }) {
        this.#validateIdentity(clientId, wave);
        assert(Number.isSafeInteger(frameSeq) && frameSeq >= 0,
            "frameSeq must be a non-negative safe integer");
        const timestamps = {
            wireAt,
            onmessageAt,
            bridgeEnqueueAt,
            pollAt,
            decodeStartAt,
            decodeEndAt,
            dispatchAt,
        };
        for (const [name, value] of Object.entries(timestamps)) {
            if (value !== null) {
                assert(Number.isFinite(value) && value >= 0,
                    `${name} must be null or a non-negative finite timestamp`);
            }
        }
        if (queueDepth !== null) {
            assert(Number.isSafeInteger(queueDepth) && queueDepth >= 0,
                "queueDepth must be null or a non-negative safe integer");
        }
        const packet = {
            schemaVersion: SCHEMA_VERSION,
            packetSequence: ++this.packetSequence,
            clientId,
            wave,
            frameSeq,
            phase,
            timestamps,
            queueDepth,
            intentionalDrop: intentionalDrop === true,
        };
        this.clientIds.add(String(clientId));
        const clientKey = String(clientId);
        const events = this.eventsByClient.get(clientKey) ?? [];
        if (events.length >= this.perClientEventLimit) events.shift();
        events.push(packet);
        this.eventsByClient.set(clientKey, events);

        if (packet.intentionalDrop) {
            this.intentionalDropCount++;
            return packet;
        }

        const previousDecodeEnd = this.previousDecodeEndByClient.get(clientId);
        if (Number.isFinite(decodeEndAt)) {
            this.previousDecodeEndByClient.set(clientId, decodeEndAt);
        }
        const decodedGapMillis = Number.isFinite(previousDecodeEnd) &&
            Number.isFinite(decodeEndAt)
            ? Math.max(0, decodeEndAt - previousDecodeEnd)
            : null;
        const segments = this.#segments(timestamps);
        const reconnect = this.#reconnectSummary(clientId, wave);
        const slow = decodedGapMillis !== null &&
            decodedGapMillis >= this.slowThresholdMillis;
        let classification;
        if (slow) {
            classification = classifyArrivalGap({
                decodedGapMillis,
                segments,
                phase,
                reconnect,
                queueDepth,
            });
            const sample = {
                schemaVersion: SCHEMA_VERSION,
                sampleSequence: ++this.sampleSequence,
                clientId,
                wave,
                frameSeq,
                phase,
                decodedGapMillis,
                thresholdMillis: this.slowThresholdMillis,
                segments,
                queueDepth,
                reconnect,
                classification,
            };
            this.#retainSample(sample);
        }
        return { ...packet, decodedGapMillis, segments, reconnect, classification };
    }

    snapshot() {
        const reconnectCounts = new Map();
        for (const [key, entries] of this.reconnectByClient) {
            const clientId = key.replace(/@wave-\d+$/u, "");
            reconnectCounts.set(clientId,
                (reconnectCounts.get(clientId) ?? 0) + entries.length);
        }
        return {
            schemaVersion: SCHEMA_VERSION,
            independentExecution: true,
            strictGatesChanged: false,
            slowThresholdMillis: this.slowThresholdMillis,
            limits: {
                perClientEvents: this.perClientEventLimit,
                perClientSamples: this.perClientSampleLimit,
                globalSamples: this.globalSampleLimit,
                reconnectTimelineEntries: this.reconnectTimelineLimit,
            },
            slowSampleCountTotal: this.slowSampleCountTotal,
            slowSamplesDropped: this.slowSamplesDropped,
            intentionalDropCount: this.intentionalDropCount,
            slowSamples: [...this.samples],
            clientRings: [...this.clientIds].sort().map((clientId) => ({
                clientId,
                events: (this.eventsByClient.get(clientId) ?? []).length,
                samples: (this.samplesByClient.get(clientId) ?? []).length,
                reconnectEntries: reconnectCounts.get(clientId) ?? 0,
            })),
            reconnectTimelines: [...this.reconnectByClient.entries()].map(
                ([key, entries]) => ({ key, entries: [...entries] })),
        };
    }

    #validateIdentity(clientId, wave) {
        assert((typeof clientId === "string" && clientId.length > 0) ||
            (Number.isSafeInteger(clientId) && clientId >= 0),
        "clientId must be a non-empty string or non-negative integer");
        assert(Number.isSafeInteger(wave) && wave >= 0, "wave must be non-negative");
    }

    #key(clientId, wave) {
        return `${String(clientId)}@wave-${wave}`;
    }

    #segments(timestamps) {
        const delta = (start, end) => Number.isFinite(start) && Number.isFinite(end)
            ? Math.max(0, end - start) : null;
        return {
            wireToOnmessageMillis: delta(timestamps.wireAt, timestamps.onmessageAt),
            onmessageToEnqueueMillis: delta(
                timestamps.onmessageAt, timestamps.bridgeEnqueueAt),
            enqueueToPollMillis: delta(timestamps.bridgeEnqueueAt, timestamps.pollAt),
            pollToDecodeMillis: delta(timestamps.pollAt, timestamps.decodeStartAt),
            decodeMillis: delta(timestamps.decodeStartAt, timestamps.decodeEndAt),
            decodeToDispatchMillis: delta(timestamps.decodeEndAt, timestamps.dispatchAt),
            wireToDispatchMillis: delta(timestamps.wireAt, timestamps.dispatchAt),
        };
    }

    #reconnectSummary(clientId, wave) {
        const entries = this.reconnectByClient.get(this.#key(clientId, wave)) ?? [];
        const at = new Map(entries.map(({ phase, at }) => [phase, at]));
        const delta = (from, to) => Number.isFinite(at.get(from)) && Number.isFinite(at.get(to))
            ? Math.max(0, at.get(to) - at.get(from)) : null;
        return {
            inReconnect: entries.length > 0,
            phasesObserved: entries.map(({ phase }) => phase),
            disconnectToConnectBeginMillis: delta("disconnect", "connect-begin"),
            connectBeginToTransportOpenMillis: delta("connect-begin", "transport-open"),
            transportOpenToPlayEnterMillis: delta("transport-open", "play-enter"),
            playEnterToFirstOnmessageMillis: delta("play-enter", "first-onmessage"),
            firstOnmessageToFirstDecodedMillis: delta(
                "first-onmessage", "first-decoded-packet"),
        };
    }

    #retainSample(sample) {
        this.slowSampleCountTotal++;
        const clientKey = String(sample.clientId);
        const clientSamples = this.samplesByClient.get(clientKey) ?? [];
        clientSamples.push(sample);
        clientSamples.sort(compareSamples);
        if (clientSamples.length > this.perClientSampleLimit) {
            clientSamples.length = this.perClientSampleLimit;
            this.slowSamplesDropped++;
        }
        this.samplesByClient.set(clientKey, clientSamples);
        this.samples.push(sample);
        this.samples.sort(compareSamples);
        if (this.samples.length > this.globalSampleLimit) {
            this.samples.length = this.globalSampleLimit;
            this.slowSamplesDropped++;
        }
    }
}

function compareSamples(left, right) {
    return right.decodedGapMillis - left.decodedGapMillis ||
        left.sampleSequence - right.sampleSequence;
}

function classifyArrivalGap({ decodedGapMillis, segments, phase, reconnect, queueDepth }) {
    assert(Number.isFinite(decodedGapMillis));
    if (reconnect.inReconnect && phase !== "steady-soak") {
        return "reconnect-gap";
    }
    if (segments.wireToOnmessageMillis !== null &&
        segments.wireToOnmessageMillis >= SLOW_GAP_THRESHOLD_MILLIS) {
        return "browser-delivery-delay";
    }
    if (segments.decodeMillis !== null &&
        segments.decodeMillis >= SLOW_GAP_THRESHOLD_MILLIS) {
        return "decode-delay";
    }
    if (segments.onmessageToEnqueueMillis !== null &&
        segments.onmessageToEnqueueMillis >= SLOW_GAP_THRESHOLD_MILLIS) {
        return "browser-queue-delay";
    }
    if (segments.enqueueToPollMillis !== null &&
        segments.enqueueToPollMillis >= SLOW_GAP_THRESHOLD_MILLIS &&
        (queueDepth === null || queueDepth > 0)) {
        return "dispatch-queue-delay";
    }
    if (segments.decodeToDispatchMillis !== null &&
        segments.decodeToDispatchMillis >= SLOW_GAP_THRESHOLD_MILLIS) {
        return "dispatch-delay";
    }
    if (segments.wireToOnmessageMillis === null &&
        segments.onmessageToEnqueueMillis === null &&
        segments.decodeMillis === null &&
        segments.enqueueToPollMillis === null) {
        return "upstream-silence-observed";
    }
    return "unknown-arrival-gap";
}

function printConfig() {
    console.log(JSON.stringify({
        ok: true,
        schemaVersion: SCHEMA_VERSION,
        independentExecution: true,
        strictGatesChanged: false,
        slowThresholdMillis: SLOW_GAP_THRESHOLD_MILLIS,
        limits: {
            perClientEvents: PER_CLIENT_EVENT_LIMIT,
            perClientSamples: PER_CLIENT_SAMPLE_LIMIT,
            globalSamples: GLOBAL_SAMPLE_LIMIT,
            reconnectTimelineEntries: RECONNECT_TIMELINE_LIMIT,
        },
        timestampFields: [
            "wireAt",
            "onmessageAt",
            "bridgeEnqueueAt",
            "pollAt",
            "decodeStartAt",
            "decodeEndAt",
            "dispatchAt",
        ],
        classifications: [
            "upstream-silence-observed",
            "browser-delivery-delay",
            "browser-queue-delay",
            "decode-delay",
            "dispatch-queue-delay",
            "dispatch-delay",
            "reconnect-gap",
            "unknown-arrival-gap",
            "intentional-transport-drop-tail",
        ],
        reconnectPhases: RECONNECT_PHASES,
    }));
}

function runSelfTest() {
    const recorder = new ArrivalTimelineRecorder();
    const full = (base, frameSeq, overrides = {}) => recorder.recordPacket({
        clientId: "c1",
        frameSeq,
        wireAt: base,
        onmessageAt: base + 1,
        bridgeEnqueueAt: base + 2,
        pollAt: base + 3,
        decodeStartAt: base + 4,
        decodeEndAt: base + 5,
        dispatchAt: base + 6,
        ...overrides,
    });
    full(0, 1);
    const silence = recorder.recordPacket({
        clientId: "c1",
        frameSeq: 2,
        decodeEndAt: 1005,
        phase: "steady-soak",
    });
    assert.equal(silence.classification, "upstream-silence-observed");
    const browserDelay = full(2000, 3, {
        wireAt: 2000,
        onmessageAt: 2300,
        bridgeEnqueueAt: 2301,
        pollAt: 2302,
        decodeStartAt: 2303,
        decodeEndAt: 2304,
        dispatchAt: 2305,
    });
    assert.equal(browserDelay.classification, "browser-delivery-delay");
    const decodeDelay = full(3000, 4, {
        wireAt: 3000,
        onmessageAt: 3001,
        bridgeEnqueueAt: 3002,
        pollAt: 3003,
        decodeStartAt: 3004,
        decodeEndAt: 3304,
        dispatchAt: 3305,
    });
    assert.equal(decodeDelay.classification, "decode-delay");
    const dispatchDelay = full(4000, 5, {
        wireAt: 4000,
        onmessageAt: 4001,
        bridgeEnqueueAt: 4002,
        pollAt: 4302,
        decodeStartAt: 4303,
        decodeEndAt: 4304,
        dispatchAt: 4305,
        queueDepth: 3,
    });
    assert.equal(dispatchDelay.classification, "dispatch-queue-delay");
    for (const [phase, at] of [
        ["disconnect", 5000],
        ["reconnect-scheduled", 5001],
        ["connect-begin", 5100],
        ["transport-open", 5200],
        ["play-enter", 5300],
        ["first-onmessage", 5400],
        ["first-decoded-packet", 5401],
    ]) recorder.recordReconnectPhase({ clientId: "c1", wave: 1, phase, at });
    recorder.recordPacket({
        clientId: "c1",
        wave: 1,
        frameSeq: 6,
        phase: "reconnect-recovery",
        decodeEndAt: 5600,
    });
    const intentional = recorder.recordPacket({
        clientId: "c1",
        wave: 1,
        frameSeq: 7,
        phase: "intentional-drop-tail",
        decodeEndAt: 9000,
        intentionalDrop: true,
    });
    assert.equal(intentional.intentionalDrop, true);
    const before = recorder.snapshot();
    for (let i = 0; i < 70; i++) {
        const at = 10_000 + i * 300;
        recorder.recordPacket({
            clientId: "c2",
            frameSeq: i,
            decodeEndAt: at,
        });
    }
    const result = recorder.snapshot();
    assert.equal(result.schemaVersion, SCHEMA_VERSION);
    assert.equal(result.strictGatesChanged, false);
    assert.equal(result.limits.perClientEvents, PER_CLIENT_EVENT_LIMIT);
    assert.ok(result.slowSampleCountTotal >= before.slowSampleCountTotal + 69);
    assert.ok(result.slowSamples.length <= GLOBAL_SAMPLE_LIMIT);
    assert.ok(result.clientRings.every((ring) => ring.events <= PER_CLIENT_EVENT_LIMIT));
    assert.ok(result.clientRings.every((ring) => ring.samples <= PER_CLIENT_SAMPLE_LIMIT));
    assert.equal(result.intentionalDropCount, 1);
    const overflowRecorder = new ArrivalTimelineRecorder({ globalSampleLimit: 8 });
    overflowRecorder.recordPacket({ clientId: "overflow", frameSeq: 0, decodeEndAt: 0 });
    for (let i = 1; i <= 12; i++) {
        overflowRecorder.recordPacket({
            clientId: "overflow",
            frameSeq: i,
            decodeEndAt: i * 300,
        });
    }
    const overflow = overflowRecorder.snapshot();
    assert.equal(overflow.limits.globalSamples, 8);
    assert.equal(overflow.slowSamples.length, 8);
    assert.ok(overflow.slowSamplesDropped > 0);
    return { ...result, overflow };
}

if (process.argv.includes("--print-config")) {
    printConfig();
}
else {
    const result = runSelfTest();
    console.log(JSON.stringify({ ok: true, selfTest: result }));
}
