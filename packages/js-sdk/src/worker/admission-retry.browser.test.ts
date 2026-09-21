import { expect, test } from "vitest";
import { HttpTransportError } from "../http-transport.js";
import { requestAdmission, SharedAdmissionCache } from "./shared-admission.js";
import { compatibility } from "@lix-js/sdk/compatibility";

const repositoryId = "00000000-0000-7000-8000-000000000004";
const url = `https://lix.test/lix/${repositoryId}`;
const identity = {
  repositoryId, principalId: "account-test",
  protocolEpoch: compatibility.syncProtocolVersion,
  storageEpoch: compatibility.storageFormatVersion,
};

test("real WASM admission recovers from a gateway failure without reopening", async () => {
  let requests = 0;
  const result = await requestAdmission(url, [], async request => {
    expect(request.init.method).toBe("GET");
    expect(request.url).toBe(`${url.replace('/lix/', '/lix/v1/')}/admission`);
    return ++requests === 1 ? new Response("Bad Gateway", { status: 502 }) : Response.json(identity);
  });
  expect(result).toEqual(identity);
  expect(requests).toBe(2);
}, 30_000);

test("exhausted network retries still allow verified cached offline admission", async () => {
  const cache = new SharedAdmissionCache();
  const headers: [string, string][] = [["authorization", "Bearer test"]];
  cache.record(url, headers, identity);
  let requests = 0;
  const result = await cache.verify(url, headers, identity, () => requestAdmission(url, headers, async () => {
    requests++;
    throw new HttpTransportError("LIX_TRANSPORT_NETWORK", "offline");
  }));
  expect(result).toEqual({identity, online: false});
  expect(requests).toBe(5);
}, 30_000);

test("admission timeout aborts stalled fetch before retrying", async () => {
  let requests = 0;
  let aborted = false;
  const result = await requestAdmission(url, [], async request => {
    if (++requests > 1) { expect(aborted).toBe(true); return Response.json(identity); }
    return new Promise<Response>((_resolve, reject) => {
      request.init.signal!.addEventListener("abort", () => {
        aborted = true;
        reject(new HttpTransportError("LIX_TRANSPORT_ABORTED", "cancelled"));
      }, {once: true});
    });
  });
  expect(result).toEqual(identity);
  expect(requests).toBe(2);
}, 30_000);
