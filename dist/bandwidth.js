// Bandwidth accounting and rate limiting for RelayNode tunnels.
//
// Public nodes forward unauthenticated client traffic, so the operator needs
// two protections that connection limits alone cannot provide: a per-tunnel
// transfer cap that stops a single client from consuming an unbounded amount
// of upload quota, and token-bucket rate limits (per tunnel and node-wide)
// that stop a burst from saturating the host uplink.
//
// Enforcement never drops or buffers tunnel bytes: the caller pauses the
// underlying source (WebSocket or TCP socket) through callbacks and the
// scheduler resumes it once the bucket refills, mirroring the existing
// backpressure flags in main.js.

const schedulerTickMs = 250;

export function createTokenBucket(rateBytesPerSecond, burstBytes) {
    let tokens = burstBytes;
    let lastRefillAt = Date.now();
    return {
        get tokens() {
            return tokens;
        },
        refill(now = Date.now()) {
            if (rateBytesPerSecond <= 0) {
                return;
            }
            const elapsed = now - lastRefillAt;
            if (elapsed <= 0) {
                return;
            }
            tokens = Math.min(burstBytes, tokens + (elapsed / 1000) * rateBytesPerSecond);
            lastRefillAt = now;
        },
        // Returns the milliseconds until `bytes` can be consumed, or 0 when
        // the bytes are available right now (and are consumed).
        tryConsume(bytes, now = Date.now()) {
            this.refill(now);
            if (tokens >= bytes) {
                tokens -= bytes;
                return 0;
            }
            if (rateBytesPerSecond <= 0) {
                return 0;
            }
            const deficit = bytes - tokens;
            return Math.max(1, Math.ceil((deficit / rateBytesPerSecond) * 1000));
        },
        waitFor(bytes, now = Date.now()) {
            this.refill(now);
            if (tokens >= bytes || rateBytesPerSecond <= 0) {
                return 0;
            }
            return Math.max(1, Math.ceil(((bytes - tokens) /
                rateBytesPerSecond) * 1000));
        },
    };
}

export function createTrafficMeter() {
    let totalIngressBytes = 0;
    let totalEgressBytes = 0;
    let proxyBytes = 0;
    let sampledIngressBytes = 0;
    let sampledEgressBytes = 0;
    let ingressBytesPerSecond = 0;
    let egressBytesPerSecond = 0;
    let throttledTunnels = 0;
    return {
        get throttledTunnels() {
            return throttledTunnels;
        },
        setThrottledTunnels(count) {
            throttledTunnels = count;
        },
        accountIngress(bytes) {
            totalIngressBytes += bytes;
            sampledIngressBytes += bytes;
        },
        accountEgress(bytes) {
            totalEgressBytes += bytes;
            sampledEgressBytes += bytes;
        },
        accountProxy(bytes) {
            proxyBytes += bytes;
        },
        sampleRates() {
            const windowMs = Math.max(1, schedulerTickMs);
            const nextIngress = (sampledIngressBytes / windowMs) * 1000;
            const nextEgress = (sampledEgressBytes / windowMs) * 1000;
            // Exponential smoothing keeps /relay-node/v1 readable even though
            // the sample window is one scheduler tick.
            ingressBytesPerSecond = Math.round(
                ingressBytesPerSecond === 0
                    ? nextIngress
                    : ingressBytesPerSecond * 0.5 + nextIngress * 0.5);
            egressBytesPerSecond = Math.round(
                egressBytesPerSecond === 0
                    ? nextEgress
                    : egressBytesPerSecond * 0.5 + nextEgress * 0.5);
            sampledIngressBytes = 0;
            sampledEgressBytes = 0;
        },
        snapshot() {
            return {
                totalIngressBytes,
                totalEgressBytes,
                proxyBytes,
                ingressBytesPerSecond,
                egressBytesPerSecond,
                throttledTunnels,
            };
        },
    };
}

// One scheduler per RelayNode process. The single timer refills every bucket
// (per tunnel and node-wide) and wakes tunnels whose reads were paused by a
// rate limit, so 1024 connections cost one interval handle instead of 2048.
export function createBandwidthScheduler(meter) {
    const tunnels = new Set();
    const timer = setInterval(() => {
        for (const tunnel of tunnels) {
            tunnel.refill();
        }
        meter?.sampleRates();
        meter?.setThrottledTunnels(
            [...tunnels].filter((tunnel) => tunnel.throttled).length);
    }, schedulerTickMs);
    timer.unref();
    return {
        register(tunnel) {
            tunnels.add(tunnel);
        },
        unregister(tunnel) {
            tunnels.delete(tunnel);
        },
        dispose() {
            clearInterval(timer);
        },
    };
}

// A per-tunnel limiter. `callbacks.pause`/`callbacks.resume` are invoked at
// most once per direction until the corresponding resume fires; the caller
// keeps its own pause flag coherent with the rest of its backpressure state.
export function createTunnelLimiter(scheduler, meter, options, callbacks) {
    const perConnectionRate = options.perConnectionRate;
    const globalRate = options.globalRate;
    const transferCap = options.transferCap;
    // Accounting is unconditional; only enforcement depends on the limits.
    // With every limit at zero the limiter counts bytes and never pauses.
    const enforcing = perConnectionRate > 0 || globalRate > 0 || transferCap > 0;
    const bucket = perConnectionRate > 0
        ? createTokenBucket(perConnectionRate, perConnectionRate)
        : undefined;
    const globalBucket = globalRate > 0
        ? options.globalBucket
        : undefined;
    let transferredBytes = 0;
    let transferLimitReported = false;
    let disposed = false;
    const paused = { ingress: false, egress: false };
    const pausedBytes = { ingress: 0, egress: 0 };
    const tunnel = {
        get throttled() {
            return paused.ingress || paused.egress;
        },
        refill(now = Date.now()) {
            if (disposed) {
                return;
            }
            for (const direction of ["ingress", "egress"]) {
                if (!paused[direction]) {
                    continue;
                }
                // Resume only when a complete source chunk can be admitted.
                // A one-byte probe would let a large WebSocket/TCP chunk
                // bypass the configured rate on every scheduler tick.
                const required = Math.max(1, pausedBytes[direction]);
                const perConnectionWait = bucket === undefined
                    ? 0 : bucket.waitFor(required, now);
                const globalWait = globalBucket === undefined
                    ? 0 : globalBucket.waitFor(required, now);
                if (perConnectionWait === 0 && globalWait === 0) {
                    paused[direction] = false;
                    pausedBytes[direction] = 0;
                    callbacks.resume[direction]();
                }
            }
        },
        account(direction, bytes) {
            if (disposed) {
                return;
            }
            if (direction === "ingress") {
                meter?.accountIngress(bytes);
            }
            else {
                meter?.accountEgress(bytes);
            }
            transferredBytes += bytes;
            if (transferCap > 0 && transferredBytes > transferCap &&
                    !transferLimitReported) {
                transferLimitReported = true;
                disposed = true;
                scheduler.unregister(tunnel);
                callbacks.onTransferLimit?.();
                return false;
            }
            const perConnectionWait = bucket === undefined
                ? 0 : bucket.tryConsume(bytes);
            const globalWait = globalBucket === undefined
                ? 0 : globalBucket.tryConsume(bytes);
            const waitMs = Math.max(perConnectionWait, globalWait);
            if (waitMs > 0 && !paused[direction]) {
                paused[direction] = true;
                pausedBytes[direction] = bytes;
                callbacks.pause[direction]();
            }
            else if (waitMs > 0) {
                pausedBytes[direction] = Math.max(pausedBytes[direction], bytes);
            }
            return true;
        },
        dispose() {
            if (!disposed) {
                disposed = true;
                scheduler.unregister(tunnel);
            }
        },
    };
    if (enforcing) {
        scheduler.register(tunnel);
    }
    return tunnel;
}
