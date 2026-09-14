/** The internal HTTP ABI shared by WASM, worker RPC, sync and admission. */
export type HttpResponsePolicy =
  | { mode: "buffered"; maxBytes: number }
  | { mode: "streaming" };
export type HttpRequest = { url: string; init: RequestInit; response: HttpResponsePolicy };
export type HttpTransport = (request: HttpRequest) => Promise<Response>;

export class HttpTransportError extends Error {
  constructor(readonly code: string, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "HttpTransportError";
  }
}

export function validateHttpRequest(request: HttpRequest): void {
  if (!request || typeof request.url !== "string" || !request.init ||
      (request.response?.mode !== "streaming" &&
       !(request.response?.mode === "buffered" && Number.isSafeInteger(request.response.maxBytes) && request.response.maxBytes > 0))) {
    throw new HttpTransportError("LIX_TRANSPORT_CONTRACT", "HTTP request requires an explicit response policy");
  }
}

/** Invoke browser fetch with failure classification at the actual I/O boundary.
 * Custom telemetry adapters should delegate here and preserve the typed error.
 */
export async function networkFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const request = validatedRequest(input, init);
  let response: Response;
  try { response = await globalThis.fetch(request); }
  catch (error) { throw nativeNetworkFailure(error, request.signal); }
  return classifiedResponse(response, error => nativeNetworkFailure(error, request.signal));
}

// Keep error classification attached to the body producer, including failures
// after response headers. This wrapper preserves pull-driven backpressure.
function classifiedResponse(response: Response, classify: (error: unknown) => Error): Response {
  // Fetch instrumentation may expose an empty stream for a status whose
  // body is forbidden by the Response constructor.
  if (response.status === 204 || response.status === 205 || response.status === 304) {
    void response.body?.cancel().catch(() => undefined);
    return new Response(null, {status: response.status, statusText: response.statusText, headers: response.headers});
  }
  if (!response.body) return response;
  const reader = response.body.getReader();
  let released = false;
  const release = () => { if (!released) {released = true; reader.releaseLock();} };
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const chunk = await reader.read();
        if (chunk.done) {release(); controller.close();}
        else controller.enqueue(chunk.value);
      } catch (error) {release(); controller.error(classify(error));}
    },
    async cancel(reason) {try {await reader.cancel(reason);} finally {release();}},
  });
  return new Response(body, {status:response.status,statusText:response.statusText,headers:response.headers});
}

function nativeNetworkFailure(error: unknown, signal: AbortSignal): HttpTransportError {
  if (signal.aborted || (error instanceof Error && error.name === "AbortError")) {
    return new HttpTransportError("LIX_TRANSPORT_ABORTED", "HTTP request was cancelled");
  }
  if (error instanceof TypeError) return new HttpTransportError("LIX_TRANSPORT_NETWORK", "HTTP network request failed");
  return new HttpTransportError("LIX_TRANSPORT_CALLBACK", "HTTP adapter failed");
}

/** Public fetch callbacks are network adapters; they must preserve explicit credentials. */
export function fetchTransport(fetcher?: typeof fetch): HttpTransport {
  return async (request) => {
    validateHttpRequest(request);
    // Validate browser request arguments outside the network-failure boundary.
    const validated = validatedRequest(request.url, request.init);
    let response: Response;
    try {
      response = await (fetcher ?? networkFetch)(validated.url, request.init);
    } catch (error) {
      if (request.init.signal?.aborted || (error instanceof Error && error.name === "AbortError")) {
        throw new HttpTransportError("LIX_TRANSPORT_ABORTED", "HTTP request was cancelled");
      }
      if (isTransportFailure(error)) throw error;
      // Arbitrary user callbacks may throw TypeError for programming errors.
      throw new HttpTransportError("LIX_TRANSPORT_CALLBACK", "HTTP adapter failed");
    }
    if (fetcher) {
      response = classifiedResponse(response, error => {
        if (isTransportFailure(error)) return error;
        if (request.init.signal?.aborted) return new HttpTransportError("LIX_TRANSPORT_ABORTED", "HTTP request was cancelled");
        return new HttpTransportError("LIX_TRANSPORT_CALLBACK", "HTTP response adapter failed");
      });
    }
    if (request.response.mode === "streaming") return response;
    const bytes = await boundedResponseBody(response, request.response.maxBytes);
    return new Response(response.status === 204 || response.status === 205 || response.status === 304 ? null : bytes,
      { status: response.status, statusText: response.statusText, headers: response.headers });
  };
}

export async function boundedResponseBody(response: Response, maxBytes: number): Promise<Uint8Array<ArrayBuffer>> {
  const oversized = () => new HttpTransportError("LIX_TRANSPORT_RESPONSE_LIMIT", "HTTP response exceeds its declared resource budget");
  if (Number(response.headers.get("content-length")) > maxBytes) {
    await response.body?.cancel().catch(() => undefined);
    throw oversized();
  }
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) { await reader.cancel().catch(() => undefined); throw oversized(); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return bytes;
}

function validatedRequest(input: RequestInfo | URL, init?: RequestInit): Request {
  try { return new Request(input, init); }
  catch { throw new HttpTransportError("LIX_TRANSPORT_CONTRACT", "Invalid HTTP request arguments"); }
}

// Error codes, unlike constructor identity, survive worker RPC and duplicate
// package realms. Only the explicit transport namespace crosses this boundary.
function isTransportFailure(error: unknown): error is Error & {code: string} {
  return error instanceof Error && typeof (error as {code?: unknown}).code === "string" &&
    (error as Error & {code: string}).code.startsWith("LIX_TRANSPORT_");
}
