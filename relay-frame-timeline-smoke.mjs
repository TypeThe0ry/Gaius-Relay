#!/usr/bin/env node

/**
 * Server-frame timeline contract/model smoke.
 *
 * This test is intentionally server-free.  It checks that the optional relay
 * timeline is disabled by default, keeps only scalar bounded records when
 * explicitly enabled, and preserves the existing aggregate/strict contracts.
 * It does not start RelayNode, open a WebSocket, or claim public-runtime
 * latency evidence.
 */

import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import {fileURLToPath} from "node:url";

const repository = fileURLToPath(new URL("../..", import.meta.url));
const relayMainPath = `${repository}/apps/bridge/dist/main.js`;
const relayMainSource = await readFile(relayMainPath, "utf8");

const SCHEMA = "gaius.relay.server-frame-timeline.v1";
const PER_TUNNEL_LIMIT = 64;
const GLOBAL_LIMIT = 256;

function countMatches(value, expression) {
    return [...value.matchAll(expression)].length;
}

// ----------------------------- static contracts -----------------------------

assert.match(relayMainSource,
    /const relayFrameTimelineEnabled = process\.env\.GAIUS_RELAY_FRAME_TIMELINE === "1"/u,
    "timeline must be an explicit opt-in flag");
assert.match(relayMainSource,
    /const relayFrameTimelinePerTunnelLimit = 64/u,
    "per-tunnel timeline cap drifted");
assert.match(relayMainSource,
    /const relayFrameTimelineGlobalLimit = 256/u,
    "global timeline cap drifted");
assert.match(relayMainSource,
    /const relayFrameTimelineSchemaVersion = "gaius\.relay\.server-frame-timeline\.v1"/u,
    "timeline schema drifted");
assert.match(relayMainSource,
    /serverFrameTimeline:\s*relayFrameTimelineSnapshot\(\)/u,
    "runtime snapshot does not expose the timeline metadata");
assert.match(relayMainSource,
    /const createRelayFrameTimelineState = \(\) =>/u,
    "per-tunnel timeline state is missing");
assert.match(relayMainSource,
    /const beginRelayFrameTimelineRecord = \(state, frame, context = \{\}\) =>/u,
    "frame record constructor is missing");
assert.match(relayMainSource,
    /tcpDataAt:\s*state\.pendingTcpDataAt/u,
    "TCP ingress timestamp is not retained in frame metadata");
for (const marker of [
    "forwardAttemptAt",
    "sendAcceptedAt",
    "sendCallbackAt",
    "drainSequence",
    "bufferedAmountBefore",
    "bufferedAmountAfter",
    "sourceKind",
    "result",
]) {
    assert.match(relayMainSource, new RegExp(`\\b${marker}\\b`, "u"),
        `timeline record omitted ${marker}`);
}
assert.match(relayMainSource,
    /recordRelayFrameTcpData\(\s*relayFrameTimelineState,\s*chunk\.byteLength/u,
    "TCP data callback does not stamp timeline ingress");
assert.match(relayMainSource,
    /beginRelayFrameTimelineDrain\(\s*relayFrameTimelineState\s*\)/u,
    "server drain sequence is not stamped");
assert.match(relayMainSource,
    /timelineRecord\.sendCallbackAt = relayFrameTimelineClock\(\)/u,
    "send callback timestamp is missing");
assert.match(relayMainSource,
    /timelineRecord\.sendAcceptedAt = relayFrameTimelineClock\(\)/u,
    "send acceptance timestamp is missing");
assert.match(relayMainSource,
    /sendErrorsAfterClose:\s*0/u,
    "after-close send-error counter is missing or unbounded");
assert.match(relayMainSource,
    /const incrementServerFrameSendErrorsAfterClose = \(\) => \{[\s\S]*?Math\.min\([\s\S]*?Number\.MAX_SAFE_INTEGER[\s\S]*?sendErrorsAfterClose \+ 1/u,
    "after-close send-error counter must remain a bounded scalar");
assert.match(relayMainSource,
    /serverFrameSendErrorsAfterClose:\s*serverFrameTelemetry\.sendErrorsAfterClose/u,
    "after-close send-error counter is not exposed as a scalar");
assert.match(relayMainSource,
    /const sendErrorAfterClose = tunnelCancelled \|\|\s+webSocket\.readyState !== WebSocket\.OPEN/u,
    "send callback does not classify tunnel teardown state");
assert.match(relayMainSource,
    /if \(sendErrorAfterClose\) \{\s+incrementServerFrameSendErrorsAfterClose\(\);/u,
    "closed send callback errors must enter the after-close counter");
assert.match(relayMainSource,
    /else \{\s+serverFrameTelemetry\.sendErrors\+\+;/u,
    "active/open send callback errors must remain strict errors");
assert.match(relayMainSource,
    /if \(error && !tunnelCancelled &&\s+webSocket\.readyState === WebSocket\.OPEN\) \{\s+closeBoth/u,
    "active/open send errors must still close the failed tunnel");
assert.match(relayMainSource,
    /sendErrorAfterClose:\s*false/u,
    "timeline records must carry the send-error classification field");

const snapshotStart = relayMainSource.indexOf(
    "const relayFrameTimelineSnapshot = () =>");
const snapshotEnd = relayMainSource.indexOf(
    "const clientFrameTelemetry =", snapshotStart);
assert.ok(snapshotStart >= 0 && snapshotEnd > snapshotStart,
    "timeline snapshot boundaries changed; inspect before updating this smoke");
const snapshotSource = relayMainSource.slice(snapshotStart, snapshotEnd);
assert.match(snapshotSource,
    /if \(!relayFrameTimelineEnabled\)\s*\{[\s\S]*?return metadata;/u,
    "disabled timeline path must return metadata only");
const disabledSnapshotBody = snapshotSource.slice(
    snapshotSource.indexOf("if (!relayFrameTimelineEnabled)"),
    snapshotSource.indexOf("return {", snapshotSource.indexOf("if (!relayFrameTimelineEnabled)")),
);
assert.doesNotMatch(disabledSnapshotBody, /samples:/u,
    "disabled runtime snapshot must not expose raw samples");
assert.match(snapshotSource, /samples:\s*relayFrameTimelineSamples\.map/u,
    "enabled runtime snapshot lost bounded samples");
assert.match(snapshotSource, /dropped:\s*relayFrameTimelineGlobalDropped/u,
    "enabled runtime snapshot lost drop count");

// No payload/credential retention is allowed in the record constructor.  The
// only frame access there is byteLength, and snapshots clone scalar timestamp
// objects rather than retaining Buffer/Uint8Array references.
const recordStart = relayMainSource.indexOf(
    "const beginRelayFrameTimelineRecord =");
const recordEnd = relayMainSource.indexOf(
    "const finishRelayFrameTimelineRecord =", recordStart);
assert.ok(recordStart >= 0 && recordEnd > recordStart,
    "timeline record boundaries changed; inspect before updating this smoke");
const recordSource = relayMainSource.slice(recordStart, recordEnd);
assert.doesNotMatch(recordSource, /rawData|token|password|toString\(|toString|hex|Buffer\.from/u,
    "timeline record must remain scalar and payload-free");
assert.match(recordSource, /frame\?\.byteLength/u,
    "record must use frame size only, not payload contents");

// Existing strict relay limits are immutable diagnostics contracts.  This
// timeline must not be used as an excuse to relax them.
assert.match(relayMainSource,
    /const maximumServerFrameDrainFrames = 32/u,
    "server frame drain frame budget changed");
assert.match(relayMainSource,
    /const maximumServerFrameDrainBytes = 512 \* 1024/u,
    "server frame drain byte budget changed");
assert.match(relayMainSource,
    /const maximumServerFrameDrainMillis = 2/u,
    "server frame drain time budget changed");

// ----------------------------- bounded model -----------------------------

function makeTimeline(enabled = true) {
    const stateByTunnel = new Map();
    const globalSamples = [];
    let globalDropped = 0;
    let perTunnelDropped = 0;

    function state(tunnelSequence) {
        let current = stateByTunnel.get(tunnelSequence);
        if (current === undefined) {
            current = {samples: [], dropped: 0};
            stateByTunnel.set(tunnelSequence, current);
        }
        return current;
    }

    return {
        snapshot() {
            const metadata = {
                schemaVersion: SCHEMA,
                enabled,
                perTunnelLimit: PER_TUNNEL_LIMIT,
                globalLimit: GLOBAL_LIMIT,
            };
            if (!enabled) {
                return metadata;
            }
            return {
                ...metadata,
                sampleCount: globalSamples.length,
                dropped: globalDropped + perTunnelDropped,
                globalDropped,
                perTunnelDropped,
                samples: globalSamples.map((sample) => ({...sample})),
            };
        },
        push(tunnelSequence, frameSequence) {
            if (!enabled) {
                return;
            }
            const current = state(tunnelSequence);
            const sample = {
                schemaVersion: 1,
                tunnelSequence,
                frameSequence,
                tcpDataSequence: frameSequence,
                drainSequence: 1,
                bytes: 128,
                phase: "play",
                tcpDataAt: {monoMillis: frameSequence, epochMillis: 1_700_000_000_000 + frameSequence},
                frameReadyAt: {monoMillis: frameSequence + 0.1, epochMillis: 1_700_000_000_000 + frameSequence},
                forwardAttemptAt: {monoMillis: frameSequence + 0.2, epochMillis: 1_700_000_000_000 + frameSequence},
                sendAcceptedAt: {monoMillis: frameSequence + 0.3, epochMillis: 1_700_000_000_000 + frameSequence},
                sendCallbackAt: {monoMillis: frameSequence + 0.4, epochMillis: 1_700_000_000_000 + frameSequence},
                bufferedAmountBefore: 0,
                bufferedAmountAfter: 128,
                sendErrorAfterClose: false,
                result: "enqueued",
            };
            if (current.samples.length >= PER_TUNNEL_LIMIT) {
                current.samples.shift();
                current.dropped++;
                perTunnelDropped++;
            }
            current.samples.push(sample);
            if (globalSamples.length >= GLOBAL_LIMIT) {
                globalSamples.shift();
                globalDropped++;
            }
            globalSamples.push(sample);
        },
        stateByTunnel,
    };
}

// The strict counter is deliberately a classification boundary, not an error
// suppression mechanism: an error while the socket is OPEN still increments
// sendErrors, while a callback that arrives after cancellation/close is kept in
// the separate bounded diagnostic scalar.  Both records remain observable.
function makeSendErrorLedger() {
    const ledger = {
        sendErrors: 0,
        sendErrorsAfterClose: 0,
        records: [],
    };
    return {
        record({readyState, tunnelCancelled, message}) {
            const sendErrorAfterClose = tunnelCancelled || readyState !== "OPEN";
            const record = {
                readyState,
                tunnelCancelled,
                message,
                sendErrorAfterClose,
            };
            ledger.records.push(record);
            if (sendErrorAfterClose) {
                ledger.sendErrorsAfterClose++;
            }
            else {
                ledger.sendErrors++;
            }
            return sendErrorAfterClose;
        },
        snapshot() {
            return {
                sendErrors: ledger.sendErrors,
                sendErrorsAfterClose: ledger.sendErrorsAfterClose,
                records: ledger.records.map((record) => ({...record})),
            };
        },
    };
}

const sendErrorLedger = makeSendErrorLedger();
assert.equal(sendErrorLedger.record({
    readyState: "OPEN", tunnelCancelled: false, message: "active-send-error",
}), false, "open send error must remain a strict error");
assert.equal(sendErrorLedger.record({
    readyState: "CLOSED", tunnelCancelled: false, message: "closed-send-callback",
}), true, "closed callback error must be classified after-close");
assert.equal(sendErrorLedger.record({
    readyState: "OPEN", tunnelCancelled: true, message: "cancelled-send-callback",
}), true, "cancelled callback error must be classified after-close");
const sendErrorSnapshot = sendErrorLedger.snapshot();
assert.equal(sendErrorSnapshot.sendErrors, 1,
    "open send error was incorrectly removed from strict accounting");
assert.equal(sendErrorSnapshot.sendErrorsAfterClose, 2,
    "closed callback errors were not retained in after-close accounting");
assert.deepEqual(sendErrorSnapshot.records.map((record) => record.sendErrorAfterClose),
    [false, true, true], "send-error classification record drifted");
assert.equal(sendErrorSnapshot.records.length, 3,
    "send-error evidence was dropped instead of classified");

const disabled = makeTimeline(false);
const disabledSnapshot = disabled.snapshot();
assert.deepEqual(Object.keys(disabledSnapshot).sort(), [
    "enabled", "globalLimit", "perTunnelLimit", "schemaVersion",
].sort(), "disabled snapshot leaked raw timeline fields");
assert.equal(disabledSnapshot.enabled, false);
assert.equal("samples" in disabledSnapshot, false);
disabled.push(1, 1);
assert.equal("samples" in disabled.snapshot(), false,
    "disabled timeline allocated samples");

const bounded = makeTimeline(true);
for (let i = 1; i <= 70; i++) {
    bounded.push(7, i);
}
const oneTunnel = bounded.snapshot();
assert.equal(bounded.stateByTunnel.get(7).samples.length, PER_TUNNEL_LIMIT,
    "per-tunnel ring exceeded 64 records");
assert.equal(bounded.stateByTunnel.get(7).dropped, 6,
    "per-tunnel drop accounting drifted");
assert.equal(oneTunnel.samples.length, 70,
    "global ring unexpectedly discarded records before its cap");
for (const sample of oneTunnel.samples) {
    assert.equal(sample.tunnelSequence, 7);
    assert.equal(sample.result, "enqueued");
    assert.equal(typeof sample.bytes, "number");
    assert.equal(typeof sample.tcpDataAt.monoMillis, "number");
    assert.equal(typeof sample.sendAcceptedAt.epochMillis, "number");
}

const globalBounded = makeTimeline(true);
for (let i = 0; i < 300; i++) {
    globalBounded.push((i % 5) + 1, i + 1);
}
const globalSnapshot = globalBounded.snapshot();
assert.ok(globalSnapshot.samples.length <= GLOBAL_LIMIT,
    "global timeline ring exceeded 256 records");
assert.equal(globalSnapshot.globalDropped, 44,
    "global drop count does not reflect 300-256 overflow");
for (const current of globalBounded.stateByTunnel.values()) {
    assert.ok(current.samples.length <= PER_TUNNEL_LIMIT,
        "one tunnel exceeded its bounded ring");
}
assert.equal(countMatches(JSON.stringify(globalSnapshot), /password|token|payload|rawData/giu), 0,
    "timeline model contains forbidden payload/credential fields");

const result = {
    status: "pass",
    schemaVersion: SCHEMA,
    limits: {perTunnel: PER_TUNNEL_LIMIT, global: GLOBAL_LIMIT},
    model: {
        disabledMetadataOnly: true,
        oneTunnelRetained: oneTunnel.samples.length,
        oneTunnelPerTunnelRetained: bounded.stateByTunnel.get(7).samples.length,
        oneTunnelDropped: bounded.stateByTunnel.get(7).dropped,
        globalRetained: globalSnapshot.samples.length,
        globalDropped: globalSnapshot.globalDropped,
        sendErrorClassification: {
            openErrors: sendErrorSnapshot.sendErrors,
            afterCloseErrors: sendErrorSnapshot.sendErrorsAfterClose,
            recordsRetained: sendErrorSnapshot.records.length,
        },
    },
    gates: {
        strictDrainBudgetsUnchanged: true,
        strictOpenSendErrorsUnchanged: true,
        closedSendErrorsRetainedSeparately: true,
        payloadFree: true,
        publicRelayRuntimeProof: false,
    },
    interpretation: {
        confirmed: [
            "timeline is explicit opt-in and disabled snapshots are metadata-only",
            "enabled records are bounded scalar metadata with monotonic and epoch clocks",
            "per-tunnel and global drop behavior is bounded",
            "open send errors remain strict while closed callback errors are retained separately",
        ],
        notProven: [
            "this is a server-free model/static contract, not a public relay-host.top run",
            "frame-to-browser correlation remains best-effort when coalescing or reconnecting",
        ],
    },
};

console.log(JSON.stringify(result));
