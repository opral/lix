import { expect, test, vi } from "vitest";
import { HttpTransportError } from "../http-transport.js";
import { SharedAdmissionCache, requestAdmission, type AdmissionIdentity } from "./shared-admission.js";
const repositoryId = "00000000-0000-7000-8000-000000000004";
const url = `https://example.test/lix/${repositoryId}`;
const headers: [string, string][] = [["Authorization", "Bearer exact-token"]];
const identity: AdmissionIdentity = {repositoryId, principalId: "00000000-0000-7000-8000-000000000003", protocolEpoch: 15, storageEpoch: 81};
const offline = async (): Promise<AdmissionIdentity> => {throw new HttpTransportError("LIX_TRANSPORT_NETWORK", "offline");};

test("new ports verify principal even with previously admitted credentials", async () => {
  const cache = new SharedAdmissionCache();
  cache.record(url, headers, identity);
  await expect(cache.verify(url, headers, identity, async () => ({...identity, principalId: "other"}))).rejects.toMatchObject({code: "LIX_SHARED_ENGINE_IDENTITY_MISMATCH"});
});
test("only exact same-worker proof grants offline local attachment, never a remote lease", async () => {
  const cache = new SharedAdmissionCache();
  cache.record(url, headers, identity);
  expect(await cache.verify(url, headers, identity, offline)).toEqual({identity, online: false});
  await expect(cache.verify(url, headers, identity, offline, false)).rejects.toMatchObject({code: "LIX_IDENTITY_UNVERIFIED_OFFLINE"});
  await expect(cache.verify(url, [["Authorization", "rotated"]], identity, offline)).rejects.toMatchObject({code: "LIX_IDENTITY_UNVERIFIED_OFFLINE"});
  await expect(new SharedAdmissionCache().verify(url, headers, identity, offline)).rejects.toMatchObject({code: "LIX_IDENTITY_UNVERIFIED_OFFLINE"});
});
test.each(["LIX_ADMISSION_AUTH_REJECTED", "LIX_TRANSPORT_CONTRACT", "LIX_TRANSPORT_CALLBACK", "LIX_TRANSPORT_ABORTED", "LIX_ADMISSION_EPOCH"])("%s never uses offline fallback", async code => {
  const cache = new SharedAdmissionCache(); cache.record(url, headers, identity);
  await expect(cache.verify(url, headers, identity, async () => {throw new HttpTransportError(code, "failed");})).rejects.toMatchObject({code});
});
test("invisible credentials or different repository/epoch cannot reuse a proof", async () => {
  const cache = new SharedAdmissionCache(); cache.record(url, [], identity);
  await expect(cache.verify(url, [], identity, offline)).rejects.toMatchObject({code: "LIX_IDENTITY_UNVERIFIED_OFFLINE"});
  cache.record(url, headers, identity);
  await expect(cache.verify(url, headers, {...identity, storageEpoch: 82}, offline)).rejects.toMatchObject({code: "LIX_IDENTITY_UNVERIFIED_OFFLINE"});
});
test("metadata admission uses bounded GET and explicit credentials without SQL opening", async () => {
  const transport = vi.fn(async () => Response.json(identity));
  expect(await requestAdmission(url, headers, transport)).toEqual(identity);
  const request = transport.mock.calls[0]![0] as any;
  expect(request.url).toBe(`https://example.test/lix/v1/${repositoryId}/admission`);
  expect(request.response).toEqual({mode: "buffered", maxBytes: 16384});
  expect(new Headers(request.init.headers).get("authorization")).toBe("Bearer exact-token");
  expect(new Headers(request.init.headers).get("lix-sync-protocol-version")).toBe("15");
  expect(request.init.credentials).toBe("omit");
});
test.each([401,403])("HTTP %s is authorization rejection", async status => {
  await expect(requestAdmission(url, headers, async () => new Response(null, {status}))).rejects.toMatchObject({code: "LIX_ADMISSION_AUTH_REJECTED"});
});
test("admission rejects mismatched epoch and repository", async () => {
  await expect(requestAdmission(url, headers, async () => Response.json({...identity, storageEpoch: 80}))).rejects.toMatchObject({code: "LIX_ADMISSION_EPOCH"});
  await expect(requestAdmission(url, headers, async () => Response.json({...identity, repositoryId: identity.principalId}))).rejects.toMatchObject({code: "LIX_ADMISSION_PROTOCOL"});
});

test("admission accepts the server's bounded opaque account identity contract", async () => {
  expect((await requestAdmission(url,headers,async()=>Response.json({...identity,principalId:"user-123"}))).principalId).toBe("user-123");
});
test.each([409,426])("HTTP %s reports migration/version requirement without offline fallback",async status=>{
  await expect(requestAdmission(url,headers,async()=>new Response(null,{status}))).rejects.toMatchObject({code:"LIX_ADMISSION_EPOCH"});
});
