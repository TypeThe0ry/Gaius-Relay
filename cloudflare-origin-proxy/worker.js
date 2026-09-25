const UPSTREAM_ORIGIN = "https://example.com";
const ALLOWED_ORIGINS = new Set([
  "null",
  "https://example.com",
]);

const ALLOWED_PATHS = new Set([
  "/relay-node/v1",
  "/relay-node/v1.runtime",
  "/tunnel",
]);

function corsOrigin(request) {
  const origin = request.headers.get("Origin");
  return origin && ALLOWED_ORIGINS.has(origin) ? origin : null;
}

function isAllowedPath(pathname) {
  // The browser uses the RelayNode's existing resource-pack endpoint. Keep
  // the exact endpoint allowlisted so Pages-origin requests are not rejected
  // before they reach the upstream proxy.
  return ALLOWED_PATHS.has(pathname)
    || pathname === "/proxy/resource-pack"
    || pathname.startsWith("/resource-pack/");
}

function corsHeaders(headers, origin) {
  const output = new Headers(headers);
  if (origin) {
    output.set("Access-Control-Allow-Origin", origin);
    output.set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
    output.set(
      "Access-Control-Allow-Headers",
      "Content-Type, Range, X-Gaius-Resource-Pack",
    );
    output.set("Access-Control-Expose-Headers", "Content-Length, Content-Range, ETag");
    output.set("Vary", "Origin");
  }
  output.set("X-Gaius-Relay-Proxy", "pages-origin-v1");
  return output;
}

export async function proxyRequest(request, upstreamFetch = fetch) {
  const incomingUrl = new URL(request.url);
  if (!isAllowedPath(incomingUrl.pathname)) {
    return new Response("Not found", { status: 404 });
  }

  const origin = request.headers.get("Origin");
  const allowedOrigin = corsOrigin(request);
  const websocket = request.headers.get("Upgrade")?.toLowerCase() === "websocket";
  if (origin && !allowedOrigin) {
    return new Response("Origin is not allowed", { status: 403 });
  }
  if (websocket && incomingUrl.pathname !== "/tunnel") {
    return new Response("WebSocket endpoint not found", { status: 404 });
  }

  if (request.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: corsHeaders(new Headers(), allowedOrigin),
    });
  }

  const upstreamUrl = new URL(incomingUrl.pathname + incomingUrl.search, UPSTREAM_ORIGIN);
  const headers = new Headers(request.headers);
  // The existing RelayNode already accepts the portable file:// origin. The
  // edge proxy enforces the public Pages allowlist, then presents that trusted
  // origin to the upstream without changing the RelayNode process.
  if (origin) headers.set("Origin", "null");
  headers.delete("Host");

  const init = {
    method: request.method,
    headers,
    redirect: "manual",
  };
  if (request.method !== "GET" && request.method !== "HEAD") {
    init.body = request.body;
  }

  const response = await upstreamFetch(new Request(upstreamUrl, init));
  if (websocket) return response;

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers: corsHeaders(response.headers, allowedOrigin),
  });
}

export default {
  async fetch(request) {
    try {
      return await proxyRequest(request);
    } catch (error) {
      console.error(JSON.stringify({
        event: "gaius-pages-relay-proxy-error",
        path: new URL(request.url).pathname,
        message: String(error?.message || error),
      }));
      return new Response("Relay proxy unavailable", { status: 502 });
    }
  },
};
