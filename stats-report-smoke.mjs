import assert from "node:assert/strict";
import { createStatsReporter } from "./dist/stats-report.js";
assert.equal(createStatsReporter({ url: "https://x", key: "", node: "n" }).enabled, false);
const calls = [];
const r = createStatsReporter({ url: "https://x/report", key: "k", node: "n1", intervalMs: 3_600_000,
  fetchImpl: async (u, o) => { calls.push({ u, o }); return { ok: true }; } });
r.recordConnection("1.1.1.1"); r.recordConnection("1.1.1.1"); r.recordConnection("2.2.2.2");
await r.report();
assert.equal(calls.length, 1);
assert.equal(calls[0].o.headers.authorization, "Bearer k");
assert.deepEqual(JSON.parse(calls[0].o.body), { node: "n1", connections: 3, unique: 2 });
assert.ok(!calls[0].o.body.includes("1.1.1.1"));
r.recordConnection("1.1.1.1"); await r.report();
assert.deepEqual(JSON.parse(calls[1].o.body), { node: "n1", connections: 1, unique: 0 });
let fail = true; const r2 = createStatsReporter({ url: "u", key: "k", node: "n", intervalMs: 3_600_000,
  fetchImpl: async () => { if (fail) throw new Error("down"); return { ok: true }; } });
r2.recordConnection("a"); await r2.report(); fail = false;
const c2 = []; // retained after failure
r2.recordConnection("b");
r2.stop(); r.stop();
console.log("stats-report smoke ok");
