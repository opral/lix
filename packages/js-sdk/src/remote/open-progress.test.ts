import { beforeEach, expect, test, vi } from "vitest";
import type { HttpTransport } from "../http-transport.js";
import { openLix } from "../open-lix.js";
const mocks=vi.hoisted(()=>({open:vi.fn()}));
vi.mock("../wasm-init.js",()=>({initializeWasm:async()=>{}}));
vi.mock("../wasm/lix_js_sdk.js",()=>({openRemote:mocks.open}));
const url="https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc";
const request={url:url.replace("/lix/","/lix/v1/")+"/",init:{method:"GET"},response:{mode:"buffered" as const,maxBytes:1024}};
beforeEach(()=>{mocks.open.mockReset();});
test("public remote opening forwards authority progress and completes once",async()=>{
 const callback=vi.fn(); let retained:HttpTransport;
 mocks.open.mockImplementation(async(_url,transport:HttpTransport)=>{
  retained=transport;
  expect((await transport(request)).status).toBe(503);
  expect((await transport(request)).status).toBe(200);
  return {close:async()=>{}};
 });
 const fetcher=vi.fn().mockResolvedValueOnce(Response.json({error:{code:"LIX_ERROR_MIGRATING",details:{fromVersion:80}}},{status:503}))
  .mockResolvedValueOnce(Response.json({ok:true})).mockResolvedValueOnce(Response.json({error:{code:"LIX_ERROR_MIGRATING"}},{status:503}));
 const lix=await openLix({server:{url,fetch:fetcher},onProgress:callback});
 expect(callback.mock.calls.map(([p])=>p.phase)).toEqual(["migrating","opening","complete"]);
 await retained!(request);
 expect(callback).toHaveBeenCalledTimes(3);
 await lix.close();
});
test("public remote progress errors stay observational and terminal failure has no complete",async()=>{
 const callback=vi.fn(()=>{throw new Error("UI callback failed");});
 mocks.open.mockImplementation(async(_url,transport:HttpTransport)=>{
  await transport(request); throw new Error("migration failed");
 });
 await expect(openLix({server:{url,fetch:async()=>Response.json({error:{code:"LIX_REPOSITORY_MIGRATING"}},{status:503})},onProgress:callback})).rejects.toThrow("migration failed");
 expect(callback).toHaveBeenCalledTimes(1);
});
