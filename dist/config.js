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
function parseName(name, fallback) {
    const raw = process.env[name];
    if (raw === undefined) {
        return fallback;
    }
    const value = raw.trim();
    if (value.length === 0 || value.length > 80 || /[\r\n]/u.test(value)) {
        throw new Error(`${name} must be a non-empty single-line value of at most 80 chars`);
    }
    return value;
}
function parseIdentifier(name) {
    const raw = process.env[name];
    if (raw === undefined) {
        throw new Error(`${name} is required when RelayNode registration is enabled`);
    }
    const value = raw.trim().toLowerCase();
    if (!/^[a-z0-9][a-z0-9._-]{0,63}$/u.test(value)) {
        throw new Error(`${name} must match [a-z0-9][a-z0-9._-]{0,63}`);
    }
    return value;
}
function parseSecret(name) {
    const raw = process.env[name];
    if (raw === undefined || raw.length < 16 || raw.length > 1024 || /[\r\n]/u.test(raw)) {
        throw new Error(`${name} must contain between 16 and 1024 single-line characters`);
    }
    return raw;
}
function parseRegistration() {
    const registryText = process.env.GAIUS_RELAY_REGISTRY_URL;
    if (registryText === undefined || registryText.trim().length === 0) {
        return undefined;
    }
    let registryUrl;
    let publicUrl;
    try {
        registryUrl = new URL(registryText);
        publicUrl = new URL(process.env.GAIUS_RELAY_PUBLIC_URL ?? "");
    }
    catch {
        throw new Error(
            "GAIUS_RELAY_REGISTRY_URL and GAIUS_RELAY_PUBLIC_URL must be absolute URLs");
    }
    const allowInsecureRegistration =
        process.env.GAIUS_RELAY_ALLOW_INSECURE_REGISTRATION === "1";
    const publicLoopback = publicUrl.hostname === "127.0.0.1" ||
        publicUrl.hostname === "::1" || publicUrl.hostname === "localhost";
    if (registryUrl.protocol !== "https:" &&
            !(allowInsecureRegistration && registryUrl.protocol === "http:")) {
        throw new Error("GAIUS_RELAY_REGISTRY_URL must use HTTPS");
    }
    if (publicUrl.protocol !== "wss:" &&
            !(allowInsecureRegistration && publicLoopback && publicUrl.protocol === "ws:")) {
        throw new Error("GAIUS_RELAY_PUBLIC_URL must use WSS");
    }
    if (publicUrl.pathname !== "/tunnel" || publicUrl.search || publicUrl.hash ||
            publicUrl.username || publicUrl.password) {
        throw new Error(
            "GAIUS_RELAY_PUBLIC_URL must be an origin plus /tunnel without credentials or query data");
    }
    registryUrl.hash = "";
    registryUrl.search = "";
    if (!registryUrl.pathname.endsWith("/")) {
        registryUrl.pathname += "/";
    }
    return {
        registryUrl: registryUrl.href,
        publicUrl: publicUrl.href,
        nodeId: parseIdentifier("GAIUS_RELAY_NODE_ID"),
        token: parseSecret("GAIUS_RELAY_REGISTRY_TOKEN"),
        priority: parseInteger("GAIUS_RELAY_PRIORITY", 0, -10_000, 10_000),
        intervalMs: parseInteger(
            "GAIUS_RELAY_REGISTRATION_INTERVAL_MS", 30_000, 1_000, 300_000),
    };
}
export function loadConfig() {
    const accessToken = process.env.GAIUS_BRIDGE_TOKEN;
    const proxyKeepAlives = process.env.GAIUS_PROXY_KEEPALIVES !== "0";
    const allowedHosts = parseList("GAIUS_ALLOWED_HOSTS", ["*"]);
    const listenHost = process.env.GAIUS_BRIDGE_HOST ?? "127.0.0.1";
    const privateTargetsSetting = process.env.GAIUS_ALLOW_PRIVATE_TARGETS;
    const loopbackListener = listenHost === "127.0.0.1" || listenHost === "::1" ||
        listenHost === "localhost";
    return {
        listenHost,
        listenPort: parseInteger("GAIUS_BRIDGE_PORT", 8080, 1, 65535),
        allowedOrigins: parseList("GAIUS_ALLOWED_ORIGINS", [
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:8780",
            "http://127.0.0.1:8780",
            "http://localhost:8781",
            "http://127.0.0.1:8781",
        ]),
        allowedHosts,
        allowPrivateTargets: privateTargetsSetting === "1" ||
            (privateTargetsSetting === undefined && loopbackListener),
        allowedResourcePackHosts: parseList(
            "GAIUS_ALLOWED_RESOURCE_PACK_HOSTS",
            allowedHosts),
        ...(accessToken === undefined ? {} : { accessToken }),
        connectTimeoutMs: parseInteger("GAIUS_CONNECT_TIMEOUT_MS", 10_000, 100, 60_000),
        idleTimeoutMs: parseInteger("GAIUS_IDLE_TIMEOUT_MS", 10 * 60_000, 1_000, 3_600_000),
        maximumConnections: parseInteger("GAIUS_MAXIMUM_CONNECTIONS", 1024, 1, 100_000),
        maximumFrameBytes: parseInteger("GAIUS_MAXIMUM_FRAME_BYTES", 16 * 1024 * 1024, 1024, 16 * 1024 * 1024),
        targetAffinityMs: parseInteger(
            "GAIUS_TARGET_AFFINITY_MS", 5 * 60_000, 1_000, 3_600_000),
        maximumTargetRoutes: parseInteger(
            "GAIUS_MAXIMUM_TARGET_ROUTES", 4096, 16, 100_000),
        resourcePackCacheMs: parseInteger(
            "GAIUS_RESOURCE_PACK_CACHE_MS", 5 * 60_000, 0, 24 * 60 * 60_000),
        maximumResourcePackCacheBytes: parseInteger(
            "GAIUS_RESOURCE_PACK_CACHE_BYTES", 512 * 1024 * 1024, 0, 4 * 1024 * 1024 * 1024),
        maximumResourcePackCacheEntries: parseInteger(
            "GAIUS_RESOURCE_PACK_CACHE_ENTRIES", 64, 0, 4096),
        proxyKeepAlives,
        relayName: parseName("GAIUS_RELAY_NODE_NAME", "Gaius Translator Node"),
        registration: parseRegistration(),
    };
}
