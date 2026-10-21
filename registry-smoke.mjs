import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import { WebSocket } from "ws";
import { isPrivateNetworkAddress } from "./dist/policy.js";
import { ephemeralCredential } from "./test-support.mjs";

const registryToken = ephemeralCredential("registry");
const registryPort = await reservePort();
const relayPort = await reservePort();
const registryBase = `http://127.0.0.1:${registryPort}`;
const registryNodesUrl = `${registryBase}/relay-registry/v1/nodes/`;
const relayUrl = `ws://127.0.0.1:${relayPort}/tunnel`;
const relayEnvironment = {
    GAIUS_BRIDGE_HOST: "127.0.0.1",
    GAIUS_BRIDGE_PORT: String(relayPort),
    GAIUS_ALLOWED_ORIGINS: "null",
    GAIUS_ALLOWED_HOSTS: "127.0.0.1",
    GAIUS_RELAY_NODE_NAME: "Registry smoke RelayNode",
    GAIUS_RELAY_REGISTRY_URL: registryNodesUrl,
    GAIUS_RELAY_REGISTRY_TOKEN: registryToken,
    GAIUS_RELAY_PUBLIC_URL: relayUrl,
    GAIUS_RELAY_NODE_ID: "registry-smoke",
    GAIUS_RELAY_PRIORITY: "42",
    GAIUS_RELAY_REGISTRATION_INTERVAL_MS: "1000",
    GAIUS_RELAY_ALLOW_INSECURE_REGISTRATION: "1",
};
let registry;
let relay;
let guardedRelay;

try {
    registry = startNode("dist/registry.js", {
        GAIUS_REGISTRY_HOST: "127.0.0.1",
        GAIUS_REGISTRY_PORT: String(registryPort),
        GAIUS_REGISTRY_TOKEN: registryToken,
        GAIUS_REGISTRY_LEASE_MS: "3000",
        GAIUS_REGISTRY_VERIFY_TIMEOUT_MS: "1000",
        GAIUS_REGISTRY_ALLOW_PRIVATE_NODES: "1",
    });
    await waitFor(
        () => registry.output.includes("relay registry listening"),
        "relay registry startup");

    relay = startNode("dist/main.js", relayEnvironment, ["GAIUS_BRIDGE_TOKEN"]);
    await waitFor(
        () => relay.output.includes("RelayNode registered as registry-smoke"),
        "RelayNode registration");

    const publicRegistryResponse = await fetch(`${registryBase}/relay-nodes.json`, {
        cache: "no-store",
        headers: {origin: "null"},
    });
    assert(publicRegistryResponse.ok, "portable client could not read the relay registry");
    assert(publicRegistryResponse.headers.get("access-control-allow-origin") === "*",
        "relay registry omitted portable-client CORS");
    let discovered = await publicRegistryResponse.json();
    assert(discovered.kind === "gaius-relay-registry", "registry kind is incorrect");
    assert(discovered.protocolVersion === 1, "registry protocol version is incorrect");
    assert(discovered.nodes.length === 1, "registered RelayNode was not discovered");
    assert(discovered.nodes[0].id === "registry-smoke", "RelayNode id was not retained");
    assert(discovered.nodes[0].url === relayUrl, "RelayNode URL was not retained");
    assert(discovered.nodes[0].priority === 42, "RelayNode priority was not retained");

    const manifestResponse = await fetch(
        `http://127.0.0.1:${relayPort}/relay-node/v1`, {
            cache: "no-store",
            headers: {origin: "null"},
        });
    assert(manifestResponse.ok, "portable client could not read the RelayNode manifest");
    assert(manifestResponse.headers.get("access-control-allow-origin") === "null",
        "RelayNode manifest omitted portable-client CORS");
    const manifest = await manifestResponse.json();
    assert(manifest.registration?.registered === true,
        "RelayNode manifest did not report a successful registration");
    assert(manifest.registration.successes >= 1,
        "RelayNode registration success counter was not incremented");

    await delay(3500);
    discovered = await fetchJson(`${registryBase}/relay-nodes.json`);
    assert(discovered.nodes.length === 1,
        "RelayNode lease expired while registration heartbeats were active");

    const gracefulStopStartedAt = Date.now();
    const gracefulViaStdin = process.platform === "win32";
    if (gracefulViaStdin) {
        relay.child.stdin.write("graceful-shutdown\n");
    } else {
        relay.child.kill("SIGTERM");
    }
    const gracefulExitCode = await relay.exited;
    assert(gracefulExitCode === (gracefulViaStdin ? 0 : 143),
        "RelayNode did not complete graceful shutdown");
    relay = undefined;
    await waitFor(async () => {
        const value = await fetchJson(`${registryBase}/relay-nodes.json`);
        return value.nodes.length === 0;
    }, "gracefully removed RelayNode lease", 2000);
    const gracefulUnregisterMs = Date.now() - gracefulStopStartedAt;
    assert(gracefulUnregisterMs < 2500,
        "RelayNode waited for lease expiry instead of unregistering during shutdown");

    relay = startNode("dist/main.js", relayEnvironment, ["GAIUS_BRIDGE_TOKEN"]);
    await waitFor(
        () => relay.output.includes("RelayNode registered as registry-smoke"),
        "RelayNode re-registration after graceful shutdown");

    const rejected = await fetch(
        `${registryBase}/relay-registry/v1/nodes/unauthorized`, {
            method: "PUT",
            headers: {
                authorization: "Bearer definitely-wrong-token",
                "content-type": "application/json",
            },
            body: "{}",
        });
    assert(rejected.status === 403, "registry accepted an invalid management token");

    relay.child.kill("SIGKILL");
    await relay.exited;
    relay = undefined;
    await waitFor(async () => {
        const value = await fetchJson(`${registryBase}/relay-nodes.json`);
        return value.nodes.length === 0;
    }, "expired RelayNode lease", 7000);

    const health = await fetchJson(`${registryBase}/health`);
    assert(health.activeNodes === 0, "expired RelayNode remained active");
    assert(health.acceptedRegistrations >= 3,
        "RelayNode did not renew its registration lease");
    assert(health.rejectedRegistrations >= 1,
        "registry rejection telemetry was not incremented");

    const guardedPort = await reservePort();
    guardedRelay = startNode("dist/main.js", {
        GAIUS_BRIDGE_HOST: "0.0.0.0",
        GAIUS_BRIDGE_PORT: String(guardedPort),
        GAIUS_ALLOWED_ORIGINS: "https://play.example",
        GAIUS_ALLOWED_HOSTS: "*",
    }, [
        "GAIUS_ALLOW_PRIVATE_TARGETS",
        "GAIUS_BRIDGE_TOKEN",
        "GAIUS_RELAY_REGISTRY_URL",
    ]);
    await waitFor(
        () => guardedRelay.output.includes("translator node listening"),
        "public RelayNode policy startup");
    const guardedManifest = await fetchJson(
        `http://127.0.0.1:${guardedPort}/relay-node/v1`);
    assert(guardedManifest.allowsPrivateTargets === false,
        "a public listener enabled private TCP targets by default");
    assert(guardedManifest.capabilities.includes("public-target-guard"),
        "RelayNode manifest omitted its public target guard");
    const privateTargets = ["127.0.0.1", "localhost", "::ffff:127.0.0.1"];
    assert(!isPrivateNetworkAddress([1, 1, 1, 1].join(".")),
        "public IPv4 address was classified as private");
    assert(isPrivateNetworkAddress("::ffff:127.0.0.1"),
        "IPv4-mapped loopback address was not classified as private");
    for (const target of privateTargets) {
        const deniedCode = await attemptPrivateTunnel(guardedPort, target);
        assert(deniedCode === 1011,
            `public RelayNode did not reject ${target} (close=${deniedCode})`);
    }

    console.log(JSON.stringify({
        ok: true,
        nodeId: "registry-smoke",
        relayUrl,
        acceptedRegistrations: health.acceptedRegistrations,
        rejectedRegistrations: health.rejectedRegistrations,
        gracefulUnregisterMs,
        expiredAfterCrash: true,
        privateTargetDenied: true,
        privateTargetVariantsDenied: privateTargets.length,
        publicIpv4Allowed: true,
        portableRegistryCors: true,
        portableManifestCors: true,
    }));
}
finally {
    if (guardedRelay !== undefined) await stopNode(guardedRelay);
    if (relay !== undefined) await stopNode(relay);
    if (registry !== undefined) await stopNode(registry);
}

async function attemptPrivateTunnel(port, host) {
    return new Promise((resolve, reject) => {
        const socket = new WebSocket(`ws://127.0.0.1:${port}/tunnel`, {
            origin: "https://play.example",
        });
        const timeout = setTimeout(() => {
            socket.terminate();
            reject(new Error("private target tunnel did not close"));
        }, 3000);
        socket.once("open", () => {
            socket.send(JSON.stringify({type: "connect", host, port: 9}));
        });
        socket.once("close", (code) => {
            clearTimeout(timeout);
            resolve(code);
        });
        socket.once("error", (error) => {
            clearTimeout(timeout);
            reject(error);
        });
    });
}

function startNode(script, additions, removals = []) {
    const environment = {...process.env};
    for (const name of removals) delete environment[name];
    Object.assign(environment, additions);
    const child = spawn(process.execPath, [script], {
        cwd: new URL(".", import.meta.url),
        env: environment,
        stdio: ["pipe", "pipe", "pipe"],
    });
    const state = {
        child,
        output: "",
        exited: new Promise((resolve) => child.once("exit", resolve)),
    };
    for (const stream of [child.stdout, child.stderr]) {
        stream.setEncoding("utf8");
        stream.on("data", (chunk) => {
            state.output += chunk;
        });
    }
    return state;
}

async function stopNode(state) {
    if (state.child.exitCode === null && state.child.signalCode === null) {
        state.child.kill("SIGTERM");
    }
    await Promise.race([
        state.exited,
        delay(3000).then(() => {
            if (state.child.exitCode === null && state.child.signalCode === null) {
                state.child.kill("SIGKILL");
            }
        }),
    ]);
}

async function fetchJson(url) {
    const response = await fetch(url, {cache: "no-store"});
    if (!response.ok) throw new Error(`${url} returned ${response.status}`);
    return response.json();
}

async function waitFor(predicate, label, timeoutMs = 5000) {
    const deadline = Date.now() + timeoutMs;
    while (true) {
        if (await predicate()) return;
        if (Date.now() >= deadline) {
            const details = [registry?.output, relay?.output, guardedRelay?.output]
                .filter(Boolean).join("\n");
            throw new Error(`Timed out waiting for ${label}\n${details}`);
        }
        await delay(25);
    }
}

function assert(condition, message) {
    if (!condition) throw new Error(message);
}

async function reservePort() {
    const server = createServer();
    await new Promise((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
    });
    const port = server.address().port;
    await new Promise((resolve) => server.close(resolve));
    return port;
}
