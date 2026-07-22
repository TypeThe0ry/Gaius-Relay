function parseInteger(name, fallback, minimum, maximum) {
    const raw = process.env[name];
    if (raw === undefined) {
        return fallback;
    }
    const parsed = Number(raw);
    if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
        throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
    }
    return parsed;
}
function parseList(name, fallback) {
    const raw = process.env[name];
    if (raw === undefined) {
        return fallback;
    }
    return raw
        .split(",")
        .map((item) => item.trim())
        .filter((item) => item.length > 0);
}
export function loadConfig() {
    const accessToken = process.env.GAIUS_BRIDGE_TOKEN;
    return {
        listenHost: process.env.GAIUS_BRIDGE_HOST ?? "127.0.0.1",
        listenPort: parseInteger("GAIUS_BRIDGE_PORT", 8080, 1, 65535),
        allowedOrigins: parseList("GAIUS_ALLOWED_ORIGINS", [
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:8780",
            "http://127.0.0.1:8780",
            "http://localhost:8781",
            "http://127.0.0.1:8781",
        ]),
        allowedHosts: parseList("GAIUS_ALLOWED_HOSTS", ["*"]),
        ...(accessToken === undefined ? {} : { accessToken }),
        connectTimeoutMs: parseInteger("GAIUS_CONNECT_TIMEOUT_MS", 10_000, 100, 60_000),
        idleTimeoutMs: parseInteger("GAIUS_IDLE_TIMEOUT_MS", 10 * 60_000, 1_000, 3_600_000),
        maximumConnections: parseInteger("GAIUS_MAXIMUM_CONNECTIONS", 1024, 1, 100_000),
        maximumFrameBytes: parseInteger("GAIUS_MAXIMUM_FRAME_BYTES", 16 * 1024 * 1024, 1024, 16 * 1024 * 1024),
    };
}
