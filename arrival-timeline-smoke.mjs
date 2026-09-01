#!/usr/bin/env node

import assert from "node:assert/strict";

// This is a server-free diagnostic model.  It intentionally does not alter the
// multiplayer runtime or any strict acceptance gate.  The model is useful for
// the next real run because it makes the missing wire/onmessage/decode timeline
// explicit and keeps only bounded evidence.
// This executable is a server-free model, not the production runtime
// payload.  Keep a distinct schema id so a consumer cannot accidentally
// treat the model's abbreviated segment names/limits as live evidence.
const SCHEMA_VERSION = "gaius.browser-client-arrival-timeline-model.v1";
const SLOW_GAP_THRESHOLD_MILLIS = 250;
const PER_CLIENT_EVENT_LIMIT = 64;
const PER_CLIENT_SAMPLE_LIMIT = 64;
const GLOBAL_SAMPLE_LIMIT = 256;
const RECONNECT_TIMELINE_LIMIT = 32;
const TRACE_SCHEMA_VERSION = "gaius.browser-client-arrival-trace.v1";
const TRACE_EVENT_LIMIT = 64;
const TRACE_POLL_EVENT_STRIDE = 256;
const TRACE_FRAME_EVENT_STRIDE = 8;
const TRACE_PACKET_BEGIN_EVENT_STRIDE = 16;
const TRACE_PACKET_END_EVENT_STRIDE = 4;
const TRACE_RING_KEY_LIMIT = 256;
const TRACE_EVENT_KINDS = Object.freeze([
    "onmessage-enter",
    "bridge-enqueue",
    "bridge-dequeue",
    "poll-ready",
    "poll-begin",
    "poll-end",
    "decode-begin",
    "decode-end",
    "dispatch-begin",
    "dispatch-end",
    "phase",
    "client-created",
    "handshake-sent",
    "disconnect",
    "reconnect-scheduled",
    "synthetic-transport-drop",
    "connect-begin",
    "transport-open",
    "login-begin",
    "login-done",
    "configuration-begin",
    "configuration-done",
    "play-enter",
    "play-login",
    "first-onmessage",
    "first-decoded-packet",
    "first-chunk",
    "minimum-chunks",
    "close",
]);
const TRACE_EVENT_KIND_SET = new Set(TRACE_EVENT_KINDS);

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

function createTraceRing() {
    return {
        limit: TRACE_EVENT_LIMIT,
        nextIndex: 0,
        sequence: 0,
        dropped: 0,
        events: new Array(TRACE_EVENT_LIMIT),
    };
}

function traceInteger(value) {
    return Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function traceNumber(value) {
    return Number.isFinite(value) && value >= 0 ? Number(value) : null;
}

function appendTraceEvent(ring, {
    kind,
    at,
    source = "client",
    frameSequence = null,
    frameBytes = null,
    packetSequence = null,
    packetId = null,
    phase = null,
    schedulerCallbackSequence = null,
    pollSequence = null,
    queueDepth = null,
    bufferedBytes = null,
    durationMillis = null,
    intentional = false,
}) {
    assert(ring !== undefined && ring !== null, "trace ring is required");
    assert(TRACE_EVENT_KIND_SET.has(kind), `unknown trace event kind: ${kind}`);
    const sequence = ++ring.sequence;
    const index = ring.nextIndex;
    if (ring.events[index] !== undefined) ring.dropped++;
    const event = {
        schemaVersion: TRACE_SCHEMA_VERSION,
        sequence,
        kind,
        at: traceNumber(at),
        source: source === "socket" ? "socket" : "client",
        frameSequence: traceInteger(frameSequence),
        frameBytes: traceInteger(frameBytes),
        packetSequence: traceInteger(packetSequence),
        packetId: traceInteger(packetId),
        phase: phase === null || phase === undefined ? null : String(phase),
        schedulerCallbackSequence: traceInteger(schedulerCallbackSequence),
        pollSequence: traceInteger(pollSequence),
        queueDepth: traceInteger(queueDepth),
        bufferedBytes: traceInteger(bufferedBytes),
        durationMillis: traceNumber(durationMillis),
        intentional: intentional === true,
    };
    ring.events[index] = event;
    ring.nextIndex = (index + 1) % ring.limit;
    return event;
}

function orderedTraceEvents(ring) {
    if (ring === undefined || ring === null) return [];
    return ring.events.filter((event) => event !== undefined)
        .sort((left, right) => left.sequence - right.sequence);
}

function traceWindowFromRing(ring, startAt, endAt) {
    const start = Number.isFinite(startAt) ? Number(startAt) : null;
    const end = Number.isFinite(endAt) ? Number(endAt) : null;
    const bounded = start !== null || end !== null;
    const ringAvailable = ring !== undefined && ring !== null;
    let ringOverflowAffectsWindow = false;
    const retainedEvents = orderedTraceEvents(ring);
    const timestampUnavailable = retainedEvents.filter((event) =>
        event.at === null).length;
    const retainedTimes = retainedEvents.map((event) => event.at)
        .filter((at) => Number.isFinite(at));
    const ringDropped = ring?.dropped ?? 0;
    if (ringDropped > 0 && (start === null || retainedTimes.length === 0 ||
        Math.min(...retainedTimes) > start)) {
        ringOverflowAffectsWindow = true;
    }
    const events = retainedEvents.filter((event) =>
        (() => {
            if (bounded && event.at === null) {
                return false;
            }
            return (start === null || event.at >= start) &&
                (end === null || event.at <= end);
        })())
        .sort((left, right) =>
            (left.at ?? Number.POSITIVE_INFINITY) -
                (right.at ?? Number.POSITIVE_INFINITY) ||
            left.sequence - right.sequence || left.source.localeCompare(right.source));
    const retained = events.slice(0, TRACE_EVENT_LIMIT);
    const overflow = Math.max(0, events.length - retained.length);
    const times = retained.map((event) => event.at)
        .filter((at) => Number.isFinite(at));
    let maxInterEventGapMillis = 0;
    for (let index = 1; index < times.length; index++) {
        maxInterEventGapMillis = Math.max(
            maxInterEventGapMillis, Math.max(0, times[index] - times[index - 1]));
    }
    const eventCounts = Object.fromEntries(
        TRACE_EVENT_KINDS.map((eventKind) => [eventKind, 0]));
    for (const event of retained) eventCounts[event.kind]++;
    const dropped = ringDropped + overflow;
    return {
        schemaVersion: TRACE_SCHEMA_VERSION,
        diagnosticOnly: true,
        strictGatesChanged: false,
        limit: TRACE_EVENT_LIMIT,
        gapStartAt: start,
        gapEndAt: end,
        gapMillis: start !== null && end !== null ? Math.max(0, end - start) : null,
        firstEventAt: times.length > 0 ? times[0] : null,
        lastEventAt: times.length > 0 ? times[times.length - 1] : null,
        maxInterEventGapMillis,
        eventCounts,
        events: retained,
        dropped,
        ringDropped,
        ringOverflowAffectsWindow,
        timestampUnavailableEvents: timestampUnavailable,
        coverage: !ringAvailable ? "unavailable" : timestampUnavailable > 0
            ? "timestamp-incomplete" : (ringOverflowAffectsWindow || overflow > 0)
                ? "ring-overflow" : "complete",
        wireAtSource: "unavailable",
        bridgeEnqueueTimestampAvailable: false,
    };
}

function traceContract() {
    return {
        schemaVersion: TRACE_SCHEMA_VERSION,
        enabled: true,
        strictEvidenceEligible: false,
        independentExecution: true,
        eventLimit: TRACE_EVENT_LIMIT,
        pollEventStride: TRACE_POLL_EVENT_STRIDE,
        frameEventStride: TRACE_FRAME_EVENT_STRIDE,
        packetBeginEventStride: TRACE_PACKET_BEGIN_EVENT_STRIDE,
        packetEndEventStride: TRACE_PACKET_END_EVENT_STRIDE,
        diagnosticOnly: true,
        strictGatesChanged: false,
        wireAtSource: "unavailable",
        bridgeEnqueueTimestampAvailable: false,
        retention: "bounded-gap-window",
        sampling: "producer-sampled-before-record",
        ringKeyLimit: TRACE_RING_KEY_LIMIT,
    };
}

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
        this.traceByClient = new Map();
        this.traceRingsDropped = 0;
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
        this.recordTraceEvent({ clientId, wave, kind: phase, at, phase });
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
        traceEvents = [],
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
        assert(Array.isArray(traceEvents), "traceEvents must be an array");
        for (const event of traceEvents) {
            this.recordTraceEvent({
                ...event,
                clientId,
                wave,
            });
        }

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
        let triggerMask = 0;
        let slowTriggerMillis = decodedGapMillis ?? 0;
        const threshold = this.slowThresholdMillis;
        if (Number.isFinite(segments.wireToOnmessageMillis) &&
            segments.wireToOnmessageMillis >= threshold) {
            triggerMask |= 1;
            slowTriggerMillis = Math.max(slowTriggerMillis, segments.wireToOnmessageMillis);
        }
        if (Number.isFinite(segments.onmessageToEnqueueMillis) &&
            segments.onmessageToEnqueueMillis >= threshold) {
            triggerMask |= 2;
            slowTriggerMillis = Math.max(slowTriggerMillis, segments.onmessageToEnqueueMillis);
        }
        if (Number.isFinite(segments.enqueueToPollMillis) &&
            segments.enqueueToPollMillis >= threshold) {
            triggerMask |= 4;
            slowTriggerMillis = Math.max(slowTriggerMillis, segments.enqueueToPollMillis);
        }
        if (Number.isFinite(segments.pollToDecodeMillis) &&
            segments.pollToDecodeMillis >= threshold) {
            triggerMask |= 8;
            slowTriggerMillis = Math.max(slowTriggerMillis, segments.pollToDecodeMillis);
        }
        if (Number.isFinite(segments.decodeMillis) &&
            segments.decodeMillis >= threshold) {
            triggerMask |= 16;
            slowTriggerMillis = Math.max(slowTriggerMillis, segments.decodeMillis);
        }
        if (Number.isFinite(segments.decodeToDispatchMillis) &&
            segments.decodeToDispatchMillis >= threshold) {
            triggerMask |= 32;
            slowTriggerMillis = Math.max(slowTriggerMillis, segments.decodeToDispatchMillis);
        }
        const slow = slowTriggerMillis >= threshold;
        const triggerSegments = slow ? [] : null;
        if (triggerSegments !== null) {
            if (triggerMask & 1) triggerSegments.push("wireToOnmessageMillis");
            if (triggerMask & 2) triggerSegments.push("onmessageToEnqueueMillis");
            if (triggerMask & 4) triggerSegments.push("enqueueToPollMillis");
            if (triggerMask & 8) triggerSegments.push("pollToDecodeMillis");
            if (triggerMask & 16) triggerSegments.push("decodeMillis");
            if (triggerMask & 32) triggerSegments.push("decodeToDispatchMillis");
        }
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
                slowTriggerMillis,
                thresholdMillis: this.slowThresholdMillis,
                segments,
                triggerSegments,
                traceWindow: this.traceWindow(
                    clientId, wave, previousDecodeEnd, decodeEndAt),
                queueDepth,
                reconnect,
                classification,
            };
            this.#retainSample(sample);
        }
        return {
            ...packet,
            decodedGapMillis,
            slowTriggerMillis: slow ? slowTriggerMillis : null,
            triggerSegments,
            segments,
            reconnect,
            classification,
        };
    }

    recordTraceEvent({ clientId, wave = 0, ...event }) {
        this.#validateIdentity(clientId, wave);
        assert(event !== null && typeof event === "object",
            "trace event must be an object");
        const key = this.#key(clientId, wave);
        if (!this.traceByClient.has(key) && this.traceByClient.size >= TRACE_RING_KEY_LIMIT) {
            const oldestKey = this.traceByClient.keys().next().value;
            this.traceByClient.delete(oldestKey);
            this.traceRingsDropped++;
        }
        const ring = this.traceByClient.get(key) ?? createTraceRing();
        const recorded = appendTraceEvent(ring, event);
        this.traceByClient.set(key, ring);
        return recorded;
    }

    traceWindow(clientId, wave = 0, startAt = null, endAt = null) {
        this.#validateIdentity(clientId, wave);
        return traceWindowFromRing(
            this.traceByClient.get(this.#key(clientId, wave)), startAt, endAt);
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
            implementation: "server-free-model",
            strictGatesChanged: false,
            slowThresholdMillis: this.slowThresholdMillis,
            source: {
                wireTimestampAvailable: false,
                wireAtSource: "unavailable",
                wireAtPolicy: "null-when-unavailable",
                attributionPolicy:
                    "trusted-wire-required-for-upstream; missing-local-segments=>unattributed",
            },
            trace: traceContract(),
            limits: {
                perClientEvents: this.perClientEventLimit,
                perClientSamples: this.perClientSampleLimit,
                globalSamples: this.globalSampleLimit,
                reconnectTimelineEntries: this.reconnectTimelineLimit,
                traceEvents: TRACE_EVENT_LIMIT,
                traceRingKeys: TRACE_RING_KEY_LIMIT,
            },
            slowSampleCountTotal: this.slowSampleCountTotal,
            slowSamplesDropped: this.slowSamplesDropped,
            intentionalDropCount: this.intentionalDropCount,
            slowSamples: this.samples.map((sample) => ({
                ...sample,
                segments: { ...sample.segments },
                triggerSegments: sample.triggerSegments === null
                    ? null : [...sample.triggerSegments],
                traceWindow: sample.traceWindow === undefined ? null : {
                    ...sample.traceWindow,
                    eventCounts: { ...sample.traceWindow.eventCounts },
                    events: sample.traceWindow.events.map((event) => ({ ...event })),
                },
                reconnect: { ...sample.reconnect },
            })),
            traceRings: [...this.traceByClient.entries()].map(([key, ring]) => ({
                key,
                schemaVersion: TRACE_SCHEMA_VERSION,
                limit: TRACE_EVENT_LIMIT,
                retainedEvents: orderedTraceEvents(ring).length,
                dropped: ring.dropped,
            })),
            traceRingsDropped: this.traceRingsDropped,
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
    return (right.slowTriggerMillis ?? right.decodedGapMillis) -
        (left.slowTriggerMillis ?? left.decodedGapMillis) ||
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
        // The model has no independent wire/transport clock in this case.
        // Missing timestamps are therefore an attribution gap, not proof of
        // upstream silence.
        return "unattributed-arrival-gap";
    }
    return "unknown-arrival-gap";
}

function printConfig() {
    console.log(JSON.stringify({
        ok: true,
        schemaVersion: SCHEMA_VERSION,
        independentExecution: true,
        implementation: "server-free-model",
        strictGatesChanged: false,
        slowThresholdMillis: SLOW_GAP_THRESHOLD_MILLIS,
        source: {
            wireTimestampAvailable: false,
            wireAtSource: "unavailable",
            wireAtPolicy: "null-when-unavailable",
            attributionPolicy:
                "trusted-wire-required-for-upstream; missing-local-segments=>unattributed",
        },
        trace: traceContract(),
        limits: {
            perClientEvents: PER_CLIENT_EVENT_LIMIT,
            perClientSamples: PER_CLIENT_SAMPLE_LIMIT,
            globalSamples: GLOBAL_SAMPLE_LIMIT,
            reconnectTimelineEntries: RECONNECT_TIMELINE_LIMIT,
            traceEvents: TRACE_EVENT_LIMIT,
            traceRingKeys: TRACE_RING_KEY_LIMIT,
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
            "unattributed-arrival-gap",
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
    assert.equal(silence.classification, "unattributed-arrival-gap");
    const noEventTraceRecorder = new ArrivalTimelineRecorder();
    noEventTraceRecorder.recordPacket({
        clientId: "trace-gap",
        frameSeq: 0,
        decodeEndAt: 0,
    });
    noEventTraceRecorder.recordPacket({
        clientId: "trace-gap",
        frameSeq: 1,
        phase: "steady-soak",
        decodeEndAt: 1065,
    });
    const noEventTraceSample = noEventTraceRecorder.snapshot().slowSamples[0];
    assert.equal(noEventTraceSample.classification, "unattributed-arrival-gap");
    assert.equal(noEventTraceSample.traceWindow.gapMillis, 1065);
    assert.equal(noEventTraceSample.traceWindow.events.length, 0);
    assert.equal(noEventTraceSample.traceWindow.coverage, "unavailable");
    assert.equal(noEventTraceSample.traceWindow.wireAtSource, "unavailable");
    assert.equal(noEventTraceSample.traceWindow.bridgeEnqueueTimestampAvailable, false);

    const traceRecorder = new ArrivalTimelineRecorder();
    traceRecorder.recordPacket({
        clientId: "trace",
        frameSeq: 0,
        decodeEndAt: 0,
    });
    traceRecorder.recordPacket({
        clientId: "trace",
        frameSeq: 1,
        phase: "steady-soak",
        decodeEndAt: 1065,
        traceEvents: [
            { kind: "onmessage-enter", at: 100, source: "socket", frameSequence: 1,
                frameBytes: 12 },
            { kind: "bridge-dequeue", at: 400, source: "socket", frameSequence: 1,
                frameBytes: 12, bufferedBytes: 12 },
            { kind: "decode-begin", at: 401, frameSequence: 1 },
            { kind: "decode-end", at: 402, frameSequence: 1, packetSequence: 7,
                packetId: 39 },
            { kind: "dispatch-begin", at: 402, packetSequence: 7, packetId: 39 },
            { kind: "dispatch-end", at: 403, packetSequence: 7, packetId: 39,
                durationMillis: 1 },
        ],
    });
    const traceSample = traceRecorder.snapshot().slowSamples[0];
    assert.equal(traceSample.traceWindow.schemaVersion, TRACE_SCHEMA_VERSION);
    assert.equal(traceSample.traceWindow.gapMillis, 1065);
    assert.equal(traceSample.traceWindow.events.length, 6);
    assert.equal(traceSample.traceWindow.eventCounts["onmessage-enter"], 1);
    assert.equal(traceSample.traceWindow.eventCounts["dispatch-end"], 1);
    assert.equal(traceSample.traceWindow.events[0].source, "socket");
    assert.equal(traceSample.traceWindow.events.at(-1).durationMillis, 1);
    assert.equal(traceSample.traceWindow.wireAtSource, "unavailable");
    assert.equal(traceSample.traceWindow.bridgeEnqueueTimestampAvailable, false);
    traceRecorder.recordTraceEvent({
        clientId: "trace",
        wave: 0,
        kind: "phase",
        at: null,
    });
    const nullTimestampTrace = traceRecorder.traceWindow("trace", 0, null, null);
    assert.equal(nullTimestampTrace.timestampUnavailableEvents, 1);
    assert.equal(nullTimestampTrace.coverage, "timestamp-incomplete");
    assert.equal(nullTimestampTrace.events.length, 7);
    assert.deepEqual(traceRecorder.snapshot().trace, traceContract());
    assert.equal(traceRecorder.snapshot().limits.traceRingKeys, TRACE_RING_KEY_LIMIT);
    const identityRecorder = new ArrivalTimelineRecorder();
    identityRecorder.recordPacket({
        clientId: "owner",
        wave: 2,
        frameSeq: 0,
        decodeEndAt: 0,
        traceEvents: [{
            clientId: "spoofed",
            wave: 99,
            kind: "decode-end",
            at: 1,
        }],
    });
    const identityRings = identityRecorder.snapshot().traceRings;
    assert.deepEqual(identityRings.map(({ key }) => key), ["owner@wave-2"]);
    const keyOverflowRecorder = new ArrivalTimelineRecorder();
    for (let index = 0; index < TRACE_RING_KEY_LIMIT + 1; index++) {
        keyOverflowRecorder.recordTraceEvent({
            clientId: `key-${index}`,
            kind: "phase",
            at: index,
        });
    }
    const keyOverflowSnapshot = keyOverflowRecorder.snapshot();
    assert.equal(keyOverflowSnapshot.traceRings.length, TRACE_RING_KEY_LIMIT);
    assert.equal(keyOverflowSnapshot.traceRingsDropped, 1);
    assert.equal(keyOverflowSnapshot.traceRings[0].key, "key-1@wave-0");
    const evictedWindow = keyOverflowRecorder.traceWindow("key-0");
    assert.equal(evictedWindow.coverage, "unavailable");
    assert.throws(() => traceRecorder.recordTraceEvent({
        clientId: "trace",
        kind: "raw-wire-payload",
        at: 500,
    }), /unknown trace event kind/);
    assert.deepEqual(
        Object.keys(traceSample.traceWindow.events[0]).sort(),
        [
            "at",
            "bufferedBytes",
            "durationMillis",
            "frameBytes",
            "frameSequence",
            "intentional",
            "kind",
            "packetId",
            "packetSequence",
            "phase",
            "pollSequence",
            "queueDepth",
            "schedulerCallbackSequence",
            "schemaVersion",
            "source",
            "sequence",
        ].sort(),
    );

    const outOfOrderTraceRecorder = new ArrivalTimelineRecorder();
    outOfOrderTraceRecorder.recordTraceEvent({
        clientId: "ordered",
        kind: "decode-end",
        at: 20,
    });
    outOfOrderTraceRecorder.recordTraceEvent({
        clientId: "ordered",
        kind: "decode-begin",
        at: 10,
    });
    const orderedTrace = outOfOrderTraceRecorder.traceWindow("ordered", 0, 0, 20);
    assert.deepEqual(orderedTrace.events.map(({ at }) => at), [10, 20]);
    assert.equal(orderedTrace.maxInterEventGapMillis, 10);

    const traceOverflowRecorder = new ArrivalTimelineRecorder();
    for (let index = 0; index < TRACE_EVENT_LIMIT + 1; index++) {
        traceOverflowRecorder.recordTraceEvent({
            clientId: "trace-overflow",
            kind: "decode-end",
            at: index,
            packetSequence: index,
        });
    }
    const traceOverflow = traceOverflowRecorder.traceWindow(
        "trace-overflow", 0, 0, TRACE_EVENT_LIMIT);
    assert.equal(traceOverflow.events.length, TRACE_EVENT_LIMIT);
    assert.equal(traceOverflow.dropped, 1);
    assert.equal(traceOverflow.ringDropped, 1);
    assert.equal(traceOverflow.coverage, "ring-overflow");
    assert.equal(traceOverflow.events[0].at, 1);
    assert.equal(traceOverflow.events.at(-1).at, TRACE_EVENT_LIMIT);

    const traceContractSnapshot = traceRecorder.snapshot();
    assert.deepEqual(traceContractSnapshot.trace, traceContract());
    assert.equal(traceContractSnapshot.limits.traceEvents, TRACE_EVENT_LIMIT);
    assert.equal(traceContractSnapshot.traceRings[0].retainedEvents, 7);
    assert.equal(traceContractSnapshot.traceRings[0].dropped, 0);
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
    const segmentTriggerRecorder = new ArrivalTimelineRecorder();
    segmentTriggerRecorder.recordPacket({
        clientId: "segment",
        frameSeq: 0,
        decodeEndAt: 800,
    });
    const segmentTriggered = segmentTriggerRecorder.recordPacket({
        clientId: "segment",
        frameSeq: 1,
        onmessageAt: 200,
        bridgeEnqueueAt: 201,
        pollAt: 501,
        decodeStartAt: 502,
        decodeEndAt: 503,
        dispatchAt: 504,
    });
    assert.equal(segmentTriggered.decodedGapMillis, 0,
        "decoded gap should remain observable for segment trigger coverage");
    assert.ok(segmentTriggered.decodedGapMillis < SLOW_GAP_THRESHOLD_MILLIS,
        "segment trigger case must keep aggregate decoded gap below threshold");
    // A local segment is independently sufficient to materialize a sample;
    // use a small decoded gap so this does not regress to the aggregate-only
    // trigger that previously hid queue/decode stalls.
    assert.ok(segmentTriggered.classification,
        "segment trigger must materialize a diagnostic sample");
    assert.ok(segmentTriggered.triggerSegments.includes("enqueueToPollMillis"));
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
    const exactOverflowRecorder = new ArrivalTimelineRecorder({
        perClientSampleLimit: 64,
        globalSampleLimit: 256,
    });
    exactOverflowRecorder.recordPacket({
        clientId: "exact-overflow", frameSeq: 0, decodeEndAt: 0,
    });
    for (let i = 1; i <= 65; i++) {
        exactOverflowRecorder.recordPacket({
            clientId: "exact-overflow",
            frameSeq: i,
            decodeEndAt: i * 300,
        });
    }
    const exactOverflow = exactOverflowRecorder.snapshot();
    assert.equal(exactOverflow.slowSampleCountTotal, 65);
    assert.equal(exactOverflow.slowSamplesDropped, 1);
    assert.equal(exactOverflow.slowSamples.length, 65);
    assert.equal(exactOverflow.clientRings[0].samples, 64);
    assert.equal(exactOverflow.clientRings[0].events, 64);
    assert.equal(exactOverflow.slowSamples[0].slowTriggerMillis, 300);
    assert.equal(exactOverflow.slowSamples.at(-1).slowTriggerMillis, 300);
    const fastRecorder = new ArrivalTimelineRecorder();
    fastRecorder.recordPacket({
        clientId: "fast", frameSeq: 0, decodeEndAt: 0,
    });
    const fast = fastRecorder.recordPacket({
        clientId: "fast", frameSeq: 1,
        onmessageAt: 10, bridgeEnqueueAt: 11, pollAt: 12,
        decodeStartAt: 13, decodeEndAt: 14, dispatchAt: 15,
    });
    assert.equal(fast.classification, undefined);
    assert.equal(fast.triggerSegments, null);
    assert.equal(fast.slowTriggerMillis, null);
    assert.equal(fastRecorder.snapshot().slowSampleCountTotal, 0);
    assert.equal(fastRecorder.snapshot().slowSamples.length, 0);
    return { ...result, overflow };
}

if (process.argv.includes("--print-config")) {
    printConfig();
}
else {
    const result = runSelfTest();
    console.log(JSON.stringify({ ok: true, selfTest: result }));
}
