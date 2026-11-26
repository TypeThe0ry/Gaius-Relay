// Optional aggregate usage reporting. Disabled unless GAIUS_STATS_URL and
// GAIUS_STATS_KEY are both set. Only counters leave the process: connections
// accepted since the last report and distinct clients seen for the first time
// today (UTC). Client addresses are hashed with a per-process random salt and
// are kept in memory only until the UTC day ends.
import { createHash, randomBytes } from "node:crypto";
export function createStatsReporter(options) {
    const { url, key, node, intervalMs = 300_000, fetchImpl = globalThis.fetch } = options;
    if (!url || !key || !node) {
        return { enabled: false, recordConnection() {}, stop() {} };
    }
    const salt = randomBytes(16);
    let day = new Date().toISOString().slice(0, 10);
    let seen = new Set();
    let connections = 0;
    let unique = 0;
    const rollDay = () => {
        const today = new Date().toISOString().slice(0, 10);
        if (today !== day) {
            day = today;
            seen = new Set();
        }
    };
    const report = async () => {
        const body = { node, connections, unique };
        if (connections === 0 && unique === 0) {
            return;
        }
        connections = 0;
        unique = 0;
        try {
            const response = await fetchImpl(url, {
                method: "POST",
                headers: { "content-type": "application/json", authorization: `Bearer ${key}` },
                body: JSON.stringify(body),
                signal: AbortSignal.timeout(10_000),
            });
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
        }
        catch {
            // Keep the counts for the next attempt; never affect tunnel traffic.
            connections += body.connections;
            unique += body.unique;
        }
    };
    const timer = setInterval(() => { void report(); }, intervalMs);
    timer.unref?.();
    return {
        enabled: true,
        recordConnection(address) {
            rollDay();
            connections = Math.min(connections + 1, 1e9);
            const h = createHash("sha256").update(salt).update(String(address ?? "")).digest("hex").slice(0, 24);
            if (!seen.has(h) && seen.size < 1_000_000) {
                seen.add(h);
                unique++;
            }
        },
        report,
        stop() { clearInterval(timer); },
    };
}
