import { BlockList, isIP } from "node:net";

const privateNetworkBlockList = createPrivateNetworkBlockList();
export function isOriginAllowed(origin, allowedOrigins) {
    if (origin === undefined) {
        return false;
    }
    return allowedOrigins.includes(origin);
}
export function isHostAllowed(host, allowedHosts) {
    const normalized = normalizeHost(host);
    return allowedHosts.some((entry) => {
        const allowed = normalizeHost(entry);
        if (allowed === "*") {
            return true;
        }
        if (allowed.startsWith("*.")) {
            const suffix = allowed.slice(1);
            return normalized.endsWith(suffix) && normalized.length > suffix.length;
        }
        return normalized === allowed;
    });
}
export function normalizeHost(host) {
    const trimmed = host.trim().toLowerCase();
    if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
        return trimmed.slice(1, -1);
    }
    return trimmed.endsWith(".") ? trimmed.slice(0, -1) : trimmed;
}
export function isPrivateNetworkAddress(value) {
    const normalized = normalizeHost(value);
    const version = isIP(normalized);
    return version !== 0 && privateNetworkBlockList.check(
        normalized, version === 4 ? "ipv4" : "ipv6");
}
export function parseConnectRequest(text) {
    const value = JSON.parse(text);
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
        throw new TypeError("Control message must be an object");
    }
    const candidate = value;
    if (candidate.type !== "connect") {
        throw new TypeError("First control message must have type 'connect'");
    }
    if (typeof candidate.host !== "string" || candidate.host.length > 255) {
        throw new TypeError("Connect host must be a string of at most 255 chars");
    }
    if (typeof candidate.port !== "number" ||
        !Number.isInteger(candidate.port) ||
        candidate.port < 1 ||
        candidate.port > 65535) {
        throw new TypeError("Connect port must be an integer from 1 to 65535");
    }
    if (candidate.token !== undefined &&
        typeof candidate.token !== "string") {
        throw new TypeError("Connect token must be a string");
    }
    const host = normalizeConnectHost(candidate.host, candidate.port);
    if (host.length === 0 || (isIP(host) === 0 && !isValidDnsName(host))) {
        throw new TypeError("Connect host is not a valid IP address or DNS name");
    }
    return {
        type: "connect",
        host,
        port: candidate.port,
        ...(candidate.token === undefined ? {} : { token: candidate.token }),
    };
}
function normalizeConnectHost(host, port) {
    const value = host.trim().toLowerCase();
    if (value.startsWith("[")) {
        const closingBracket = value.indexOf("]");
        if (closingBracket <= 1) {
            return value;
        }
        const remainder = value.slice(closingBracket + 1);
        if (remainder.length === 0) {
            return value.slice(1, closingBracket);
        }
        if (remainder === `:${port}`) {
            return value.slice(1, closingBracket);
        }
        return value;
    }
    const firstColon = value.indexOf(":");
    if (firstColon > 0 && firstColon === value.lastIndexOf(":")) {
        const suffix = value.slice(firstColon + 1);
        if (suffix === String(port)) {
            return value.slice(0, firstColon);
        }
    }
    return normalizeHost(value);
}
function isValidDnsName(host) {
    if (host.length > 253) {
        return false;
    }
    return host.split(".").every((label) => label.length >= 1 &&
        label.length <= 63 &&
        /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/u.test(label));
}
function createPrivateNetworkBlockList() {
    const blockList = new BlockList();
    for (const [network, prefix] of [
        ["0.0.0.0", 8],
        ["10.0.0.0", 8],
        ["100.64.0.0", 10],
        ["127.0.0.0", 8],
        ["169.254.0.0", 16],
        ["172.16.0.0", 12],
        ["192.0.0.0", 24],
        ["192.0.2.0", 24],
        ["192.168.0.0", 16],
        ["198.18.0.0", 15],
        ["198.51.100.0", 24],
        ["203.0.113.0", 24],
        ["224.0.0.0", 4],
        ["240.0.0.0", 4],
    ]) {
        blockList.addSubnet(network, prefix, "ipv4");
    }
    for (const [network, prefix] of [
        ["::", 128],
        ["::1", 128],
        ["64:ff9b:1::", 48],
        ["100::", 64],
        ["2001:db8::", 32],
        ["2001:10::", 28],
        ["2001:20::", 28],
        ["fc00::", 7],
        ["fe80::", 10],
        ["ff00::", 8],
    ]) {
        blockList.addSubnet(network, prefix, "ipv6");
    }
    return blockList;
}
