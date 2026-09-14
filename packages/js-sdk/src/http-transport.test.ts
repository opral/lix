import { expect, test, vi } from "vitest";
import { fetchTransport, HttpTransportError, type HttpRequest } from "./http-transport.js";
import { serializeWorkerError, deserializeWorkerError } from "./worker/protocol.js";
const request: HttpRequest = {url: "https://example.test", init: {}, response: {mode: "buffered", maxBytes: 8}};
test("missing response contract fails before network", async () => {
  const fetcher = vi.fn();
  await expect(fetchTransport(fetcher)({...request, response: undefined} as any)).rejects.toMatchObject({code: "LIX_TRANSPORT_CONTRACT"});
  expect(fetcher).not.toHaveBeenCalled();
});
test("ordinary fetch receives no hidden response extensions", async () => {
  const fetcher = vi.fn(async () => new Response("ok"));
  expect(await (await fetchTransport(fetcher)(request)).text()).toBe("ok");
  expect(fetcher.mock.calls[0]).toEqual([request.url + "/", {}]);
});
test("oversized streaming bodies are cancelled at the response limit", async () => {
  const cancel = vi.fn();
  const body = new ReadableStream({start(c) {c.enqueue(new Uint8Array(9));}, cancel});
  await expect(fetchTransport(async () => new Response(body))(request)).rejects.toMatchObject({code: "LIX_TRANSPORT_RESPONSE_LIMIT"});
  expect(cancel).toHaveBeenCalledTimes(1);
});
test("custom callback TypeError cannot masquerade as network unavailability", async () => {
  await expect(fetchTransport(async () => {throw new TypeError("bug");})(request)).rejects.toMatchObject({code: "LIX_TRANSPORT_CALLBACK"});
});
test("native fetch rejection is classified at the boundary and preserves no credential text", async () => {
  vi.stubGlobal("fetch", async () => {throw new TypeError("Bearer secret-token");});
  try {await expect(fetchTransport()(request)).rejects.toMatchObject({code: "LIX_TRANSPORT_NETWORK", message: "HTTP network request failed"});}
  finally {vi.unstubAllGlobals();}
});
test("worker RPC preserves bounded structured causes and redacts credentials", () => {
  const error = new HttpTransportError("OUTER", "outer", {cause: new HttpTransportError("INNER", "Authorization: secret")});
  Object.assign(error, {details: {headers: {authorization: "secret"}, safe: "ok"}});
  const serialized = serializeWorkerError(error);
  expect(JSON.stringify(serialized)).not.toContain("secret");
  expect(deserializeWorkerError(serialized).cause).toMatchObject({code: "INNER"});
});

test("invalid request arguments are contract errors before the fetch callback", async () => {
  const fetcher = vi.fn();
  await expect(fetchTransport(fetcher)({...request, url:"not a URL"})).rejects.toMatchObject({code:"LIX_TRANSPORT_CONTRACT"});
  expect(fetcher).not.toHaveBeenCalled();
});
test("cancellation is distinct from network unavailability", async () => {
  const controller = new AbortController(); controller.abort();
  await expect(fetchTransport(async()=>{throw new DOMException("aborted","AbortError");})({...request,init:{signal:controller.signal}}))
    .rejects.toMatchObject({code:"LIX_TRANSPORT_ABORTED"});
});

test("native response-body failures retain network classification after headers", async () => {
  vi.stubGlobal("fetch",async()=>new Response(new ReadableStream({start(c){c.error(new TypeError("connection closed"));}})));
  try {await expect(fetchTransport()(request)).rejects.toMatchObject({code:"LIX_TRANSPORT_NETWORK"});}
  finally {vi.unstubAllGlobals();}
});
test("synthetic custom stream failures cannot claim a network outage", async () => {
  await expect(fetchTransport(async()=>new Response(new ReadableStream({start(c){c.error(new TypeError("implementation bug"));}})))(request))
    .rejects.toMatchObject({code:"LIX_TRANSPORT_CALLBACK"});
});

test("typed network attestation survives worker reconstruction without constructor identity", async () => {
  const reconstructed=deserializeWorkerError(serializeWorkerError(new HttpTransportError('LIX_TRANSPORT_NETWORK','network unavailable')));
  expect(reconstructed).not.toBeInstanceOf(HttpTransportError);
  await expect(fetchTransport(async()=>{throw reconstructed;})(request)).rejects.toBe(reconstructed);
});
