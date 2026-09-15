import { expect, test, vi } from "vitest";
import { observeOpenProgress } from "./open-progress.js";
import { requestAdmission, ADMISSION_PROTOCOL_EPOCH, ADMISSION_STORAGE_EPOCH } from "./worker/shared-admission.js";
import type { HttpRequest, HttpTransport } from "./http-transport.js";
const id = "01936f4e-7b6c-7c3d-8f9a-123456789abc";
const request: HttpRequest = {url: `https://example.test/lix/v1/${id}`, init: {method: "GET"}, response: {mode:"buffered",maxBytes:1024}};
const migrating = (code = "LIX_REPOSITORY_MIGRATING", details?: object) => Response.json({error:{code,details}}, {status:503});
test("bounded migration responses remain intact and progress completes with actual open", async () => {
 const callback = vi.fn(); const progress = observeOpenProgress(callback);
 const responses = [migrating("LIX_ERROR_MIGRATING", {fromVersion:80}), migrating(), Response.json({ok:true})];
 const transport = progress.transport(async () => responses.shift()!);
 expect((await (await transport(request)).json()).error.code).toBe("LIX_ERROR_MIGRATING");
 await transport(request);
 expect(callback.mock.calls.map(([p])=>p.phase)).toEqual(["migrating"]);
 expect(callback.mock.calls[0][0]).toMatchObject({scope:"authority",fromFormat:80,toFormat:ADMISSION_STORAGE_EPOCH});
 await transport(request);
 expect(callback.mock.calls.map(([p])=>p.phase)).toEqual(["migrating","opening"]);
 progress.complete();
 expect(callback.mock.calls.map(([p])=>p.phase)).toEqual(["migrating","opening","complete"]);
});
test("healthy opens, unrelated errors, writes and runtime requests do not emit upgrades", async () => {
 const callback = vi.fn(); const progress = observeOpenProgress(callback);
 await progress.transport(async()=>Response.json({ok:true}))(request);
 await progress.transport(async()=>migrating("OTHER"))(request);
 await progress.transport(async()=>migrating())({...request,init:{method:"POST"}});
 await progress.transport(async()=>migrating())({...request,url:request.url+"/execute"});
 progress.complete(); await progress.transport(async()=>migrating())(request);
 expect(callback).not.toHaveBeenCalled();
});
test("missing fromFormat and throwing observers cannot break opening", async () => {
 const callback=vi.fn((_: unknown)=>{throw new Error("observer");}); const progress=observeOpenProgress(callback);
 const response=await progress.transport(async()=>migrating())(request);
 expect(response.status).toBe(503);
 expect(callback.mock.calls[0]).toEqual([{phase:"migrating",scope:"authority",toFormat:ADMISSION_STORAGE_EPOCH}]);
 expect(()=>progress.complete()).not.toThrow();
});
test("observer never buffers past transport budget", async () => {
 const callback=vi.fn(); const progress=observeOpenProgress(callback);
 await expect(progress.transport(async()=>new Response("x".repeat(1025),{status:503}))(request))
  .rejects.toMatchObject({code:"LIX_TRANSPORT_RESPONSE_LIMIT"});
 expect(callback).not.toHaveBeenCalled();
});
test("shared admission exposes upgrades without app retries or duplicate events", async () => {
 vi.useFakeTimers();
 try {
  const callback=vi.fn(); const progress=observeOpenProgress(callback); let count=0;
  const transport: HttpTransport=progress.transport(async()=>++count<3 ? migrating() : Response.json({repositoryId:id,principalId:"account",protocolEpoch:ADMISSION_PROTOCOL_EPOCH,storageEpoch:ADMISSION_STORAGE_EPOCH}));
  const admitted=requestAdmission(`https://example.test/lix/${id}`,[],transport);
  await vi.runAllTimersAsync(); await admitted;
  expect(callback.mock.calls.map(([p])=>p.phase)).toEqual(["migrating","opening"]);
  progress.complete(); expect(callback.mock.calls.at(-1)?.[0].phase).toBe("complete");
 } finally {vi.useRealTimers();}
});
test("failed opening never signals completion and stopped observers remain silent", async () => {
 const callback=vi.fn(); const progress=observeOpenProgress(callback);
 const transport=progress.transport(async()=>migrating());
 await transport(request); progress.stop(); await transport(request); progress.complete();
 expect(callback.mock.calls.map(([p])=>p.phase)).toEqual(["migrating"]);
});
test("no observer preserves transport identity; stopped observers never read response bodies", async () => {
 const transport = vi.fn(async()=>migrating());
 expect(observeOpenProgress().transport(transport)).toBe(transport);
 const response=migrating(); const body=vi.spyOn(response,"body","get");
 const progress=observeOpenProgress(vi.fn()); progress.stop();
 expect(await progress.transport(async()=>response)(request)).toBe(response);
 expect(body).not.toHaveBeenCalled();
});
