// The packed direct entry has the provider implementation but no direct.d.ts.
// @ts-expect-error bundled worker entry; the instance uses its source type below.
import { OpfsBackend } from "../dist/direct.js";
import type { OpfsBackend as Backend } from "../js/provider.js";
import type { LixStorageSpace } from "@lix-js/sdk";
const space: LixStorageSpace = { id: 987, name: "read-progress", valueSemantics: "mutable", valueIntegrity: "backendVerified" };
const key = (i: number) => new TextEncoder().encode(String(i).padStart(5, "0"));
self.onmessage = async (event) => {
 const db: Backend = await OpfsBackend.open(`read-progress:${crypto.randomUUID()}`);
 try {
  async function put(i: number, value: Uint8Array) {
   const write = await db.beginWrite({ awaitDurable: false, preconditions: [], batchCapacityHintBytes: 256 });
   await write.putMany(space, [{ key: key(i), value }]);
   await write.commit();
  }
  if (event.data.scenario === "semantics") {
   const range = {lower: {kind: "unbounded" as const}, upper: {kind: "unbounded" as const}};
   const readOptions = {consistency: "snapshot" as const, durability: "visible" as const};
   async function values(read: Awaited<ReturnType<typeof db.beginRead>>) {
    const rows = await read.getMany([{space, keys: [key(1),key(2),key(3)], options: {projection: "fullValue"}}]);
    return rows.map(row => row?.kind === "fullValue" ? row.value[0] : null);
   }
   const assert = (ok: boolean, message: string) => {if(!ok) throw new Error(message);};
   await put(1,new Uint8Array([1])); await put(2,new Uint8Array([2]));
   const old = await db.beginRead(readOptions);
   await put(1,new Uint8Array([3])); await put(3,new Uint8Array([4]));
   const middle = await db.beginRead(readOptions);
   const erase = await db.beginWrite({awaitDurable:false,preconditions:[],batchCapacityHintBytes:0});
   await erase.deleteRange(space, range); await erase.putMany(space,[{key:key(2),value:new Uint8Array([9])}]); await erase.commit();
   assert(JSON.stringify(await values(old)) === "[1,2,null]", "old point values changed");
   assert(JSON.stringify(await values(middle)) === "[3,2,4]", "middle point values changed");
   assert(JSON.stringify(await values(await db.beginRead(readOptions))) === "[null,9,null]", "current point values incorrect");
   for(const order of ["ascending", "descending"] as const) {
    const scan = await old.beginScan(space,range,{projection:"fullValue",order});
    const found: number[]=[];
    for(let i=0;i<3;i++) {
     const page = await scan.nextPage(1);
     found.push(...page.entries.map(row => row.value.kind === "fullValue" ? row.value.value[0]! : -1));
    }
    assert(JSON.stringify(found) === (order === "ascending" ? "[1,2]" : "[2,1]"), "historical scan changed");
   }
   const expires = await db.beginRead(readOptions);
   for(let i=0;i<513;i++) await put(2,new Uint8Array([i%256]));
   let expired = false; try {await values(expires);} catch(error) {expired=(error as {code:string}).code === "LIX_STORAGE_READ_EXPIRED";}
   assert(expired, "generation retention did not expire");
   await put(1, new Uint8Array(33*1024*1024));
   const large = await db.beginRead(readOptions);
   await put(1,new Uint8Array([1]));
   expired = false; try {await values(large);} catch(error) {expired=(error as {code:string}).code === "LIX_STORAGE_READ_EXPIRED";}
   assert(expired, "oversized retained value did not expire");
   self.postMessage({semantics:true, usage: db.readHistoryUsage()});
   return;
  }
  const initial = await db.beginWrite({ awaitDurable: false, preconditions: [], batchCapacityHintBytes: 140000 });
  await initial.putMany(space, Array.from({length: 1000}, (_, i) => ({ key: key(i), value: new Uint8Array(128).fill(1) })));
  await initial.commit();
  let maxReadMs = 0, maxWriteMs = 0, completed = 0, expiry: string | undefined;
  for(let n = 0; n < 20; n++) {
   const started = performance.now();
   const read = await db.beginRead({ consistency: "snapshot", durability: "visible" });
   const scan = await read.beginScan(space, {lower: {kind: "unbounded"}, upper: {kind: "unbounded"}}, { projection: "fullValue", order: "ascending" });
   let count = 0;
   const expectedLast = n === 0 ? 1 : n + 1;
   try {
    for(let page = 0; page < 20; page++) {
     const entries = await scan.nextPage(50);
     count += entries.entries.length;
     for(const row of entries.entries) {
      const expected = new TextDecoder().decode(row.key) === "00999" ? expectedLast : 1;
      if(row.value.kind !== "fullValue" || row.value.value[0] !== expected) throw new Error("mixed-generation result");
     }
     const writeStart = performance.now();
     await put(999, new Uint8Array(128).fill(n + 2));
     maxWriteMs = Math.max(maxWriteMs, performance.now() - writeStart);
    }
    if(count !== 1000) throw new Error(`wrong count ${count}`);
    completed++;
   } catch(error) { expiry = (error as {code?: string}).code; if(!expiry) throw error; break; }
   maxReadMs = Math.max(maxReadMs, performance.now() - started);
  }
  self.postMessage({completed, expiry, maxReadMs, maxWriteMs, usage: db.readHistoryUsage()});
 } catch(error) { self.postMessage({error: String(error)}); }
 finally { await db.close(); }
};
