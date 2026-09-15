import { beforeEach, expect, test, vi } from "vitest";
import { openLixWorkerBinding } from "./client.js";
import type { SyncServerBindingOptions } from "../binding-types.js";
const mocks=vi.hoisted(()=>({direct:vi.fn()}));
vi.mock("#worker-factory",()=>({openDirectLixBinding:mocks.direct,createWorkerConnection:vi.fn(),createSharedWorkerConnection:vi.fn()}));
beforeEach(()=>{mocks.direct.mockReset();});
const url="https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc";
test("native direct opening observes migration through its bounded transport",async()=>{
 const progress=vi.fn();
 mocks.direct.mockImplementation(async(_storage,_telemetry,_parent,server:SyncServerBindingOptions)=>{
  const request={url:url.replace("/lix/","/lix/v1/"),init:{method:"GET"},response:{mode:"buffered" as const,maxBytes:1024}};
  expect((await server.transport!(request)).status).toBe(503);
  expect((await server.transport!(request)).status).toBe(200);
  return {close:async()=>{}};
 });
 const fetcher=vi.fn().mockResolvedValueOnce(Response.json({error:{code:"LIX_ERROR_MIGRATING"}},{status:503})).mockResolvedValueOnce(Response.json({ok:true}));
 const binding=await openLixWorkerBinding({kind:"memory"},undefined,undefined,{url,fetch:fetcher},progress);
 expect(progress.mock.calls.map(([p])=>p.phase)).toEqual(["migrating","opening","complete"]);
 await binding.close();
});
test("native healthy opening preserves default transport without a progress observer",async()=>{
 mocks.direct.mockResolvedValue({close:async()=>{}});
 const binding=await openLixWorkerBinding({kind:"memory"},undefined,undefined,{url});
 expect(mocks.direct.mock.calls[0][3].transport).toBeUndefined();
 await binding.close();
});
