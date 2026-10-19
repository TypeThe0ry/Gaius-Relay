import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

const host = "127.0.0.1";
const directory = fileURLToPath(new URL(".", import.meta.url));
const bridgeToken = "relay-abuse-runtime-generated-token";
const registryToken = "relay-abuse-smoke-registry-token";

function assert(condition, label) {
    if (!condition) {
        throw new Error(`Abuse-contact smoke failed: ${label}`);
    }
}

async function waitFor(predicate, label, timeoutMs = 20000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const value = await predicate();
        if (value) {
            return value;
        }
        await delay(25);
    }
    throw new Error(`Abuse-contact smoke timed out: ${label}`);
}

function startNode(script, port, env, deleteKeys = []) {
    const childEnv = {
        ...process.env,
        NODE_ENV: "test",
        ...env,
    };
    for (const key of deleteKeys) {
        delete childEnv[key];
    }
    const child = spawn(process.execPath, [script], {
        cwd: directory,
        env: childEnv,
        stdio: ["ignore", "pipe", "pipe"],
    });
    let output = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (data) => { output += data; });
    child.stderr.on("data", (data) => { output += data; });
    child.output = () => output;
    child.port = port;
    return child;
}

async function fetchManifest(port) {
    const response = await fetch(
        `http://${host}:${port}/relay-node/v1?token=${bridgeToken}`);
    assert(response.status === 200, "manifest is served");
    return response.json();
}

// Scenario A: a mailto contact is validated and published in the manifest.
{
    const bridge = startNode("dist/main.js", 18840, {
        GAIUS_BRIDGE_PORT: "18840",
        GAIUS_ALLOWED_ORIGINS: "null",
        GAIUS_ALLOWED_HOSTS: host,
        GAIUS_BRIDGE_TOKEN: bridgeToken,
        GAIUS_IDLE_TIMEOUT_MS: "60000",
        GAIUS_ABUSE_CONTACT: "mailto:abuse@example.com",
    });
    try {
        await waitFor(() => bridge.output().includes(
            "Gaius translator node listening"), "translator node startup");
        const manifest = await fetchManifest(18840);
        assert(manifest.abuseContact === "mailto:abuse@example.com",
            "manifest publishes the mailto abuse contact");
    } finally {
        bridge.kill("SIGTERM");
    }
}

// Scenario B: an https URL is accepted.
{
    const bridge = startNode("dist/main.js", 18841, {
        GAIUS_BRIDGE_PORT: "18841",
        GAIUS_ALLOWED_ORIGINS: "null",
        GAIUS_ALLOWED_HOSTS: host,
        GAIUS_BRIDGE_TOKEN: bridgeToken,
        GAIUS_IDLE_TIMEOUT_MS: "60000",
        GAIUS_ABUSE_CONTACT: "https://example.com/abuse",
    });
    try {
        await waitFor(() => bridge.output().includes(
            "Gaius translator node listening"), "translator node startup (https)");
        const manifest = await fetchManifest(18841);
        assert(manifest.abuseContact === "https://example.com/abuse",
            "manifest publishes the https abuse contact");
    } finally {
        bridge.kill("SIGTERM");
    }
}

// Scenario C: an invalid contact aborts startup rather than publishing junk.
{
    const bridge = startNode("dist/main.js", 18842, {
        GAIUS_BRIDGE_PORT: "18842",
        GAIUS_ALLOWED_ORIGINS: "null",
        GAIUS_ALLOWED_HOSTS: host,
        GAIUS_BRIDGE_TOKEN: bridgeToken,
        GAIUS_IDLE_TIMEOUT_MS: "60000",
        GAIUS_ABUSE_CONTACT: "not-a-contact",
    });
    try {
        await waitFor(() =>
            bridge.output().includes("GAIUS_ABUSE_CONTACT"),
            "invalid contact fails config load");
        assert(!bridge.output().includes("Gaius translator node listening"),
            "relay does not start with an invalid contact");
    } finally {
        bridge.kill("SIGTERM");
    }
}

// Scenario D: a registry with GAIUS_REGISTRY_REQUIRE_ABUSE_CONTACT=1 rejects
// a node whose manifest has no abuse contact.
{
    const registryPort = 18851;
    const relayPort = 18852;
    const registry = startNode("dist/registry.js", registryPort, {
        GAIUS_REGISTRY_HOST: host,
        GAIUS_REGISTRY_PORT: String(registryPort),
        GAIUS_REGISTRY_TOKEN: registryToken,
        GAIUS_REGISTRY_LEASE_MS: "3000",
        GAIUS_REGISTRY_VERIFY_TIMEOUT_MS: "1000",
        GAIUS_REGISTRY_ALLOW_PRIVATE_NODES: "1",
        GAIUS_REGISTRY_REQUIRE_ABUSE_CONTACT: "1",
    });
    const relay = startNode("dist/main.js", relayPort, {
        GAIUS_BRIDGE_PORT: String(relayPort),
        GAIUS_ALLOWED_ORIGINS: "null",
        GAIUS_ALLOWED_HOSTS: host,
        GAIUS_BRIDGE_TOKEN: bridgeToken,
        GAIUS_IDLE_TIMEOUT_MS: "60000",
        GAIUS_RELAY_NODE_NAME: "Abuse smoke RelayNode",
        GAIUS_RELAY_REGISTRY_URL: `http://${host}:${registryPort}/relay-registry/v1/nodes/`,
        GAIUS_RELAY_REGISTRY_TOKEN: registryToken,
        GAIUS_RELAY_PUBLIC_URL: `ws://${host}:${relayPort}/tunnel`,
        GAIUS_RELAY_NODE_ID: "abuse-smoke-node",
        GAIUS_RELAY_REGISTRATION_INTERVAL_MS: "1000",
        GAIUS_RELAY_ALLOW_INSECURE_REGISTRATION: "1",
        // Deliberately no GAIUS_ABUSE_CONTACT.
    }, ["GAIUS_BRIDGE_TOKEN"]);
    try {
        await waitFor(() => registry.output().includes(
            "relay registry listening"), "registry startup");
        await waitFor(() => relay.output().includes(
            "Gaius translator node listening"), "relay startup");
        // The registry must reject the registration; the relay reports its
        // failures through its own manifest.
        const rejected = await waitFor(async () => {
            const manifest = await fetchManifest(relayPort);
            return (manifest.registration?.failures ?? 0) > 0
                ? manifest.registration?.lastError
                : undefined;
        }, "registry rejects a node without an abuse contact");
        assert(String(rejected).includes("abuse contact"),
            `rejection mentions the abuse contact (got: ${rejected})`);
        const health = await (await fetch(
            `http://${host}:${registryPort}/health`)).json();
        assert(health.rejectedRegistrations > 0,
            "registry health counts the rejection");
    } finally {
        relay.kill("SIGTERM");
        registry.kill("SIGTERM");
    }
}

console.log("abuse-contact smoke passed");
process.exit(0);
