import { expect, test } from "vitest";
import { acquirePartialOwner } from "../js/partial-owner.js";

async function reacquire(name: string) {
 for (let i=0;i<100;i++) {
  const owner=acquirePartialOwner(name);
  try { await owner.ready; return owner; }
  catch (error) {
   owner.close();
   if ((error as {code?:string}).code!=="LIX_STORAGE_IN_USE") throw error;
   await new Promise(resolve=>setTimeout(resolve,0));
  }
 }
 throw new Error("owner did not release");
}
test("physical OPFS owner refuses a second engine and releases without disposal",async()=>{
 const name=`partial-owner-${crypto.randomUUID()}`;
 const first=acquirePartialOwner(name);
 await first.ready;
 const other=acquirePartialOwner(name);
 await expect(other.ready).rejects.toMatchObject({code:"LIX_STORAGE_IN_USE"});
 other.close();
 first.close();
 const reopened=await reacquire(name);
 reopened.close();
});
test("cancel before grant does not leak the physical Web Lock",async()=>{
 const name=`partial-owner-cancel-${crypto.randomUUID()}`;
 const pending=acquirePartialOwner(name);
 pending.close();
 await expect(pending.ready).rejects.toMatchObject({code:"LIX_STORAGE_CLOSED"});
 const next=await reacquire(name);
 next.close();
});
test("cancel after grant before consumer continuation releases the lock",async()=>{
 const name=`partial-owner-granted-${crypto.randomUUID()}`;
 const pending=acquirePartialOwner(name);
 // Register cancellation before the caller continuation, mirroring a dropped
 // Rust future whose JS promise has already resolved.
 const cancel=pending.ready.then(()=>pending.close());
 await cancel;
 const next=await reacquire(name);
 next.close();
});

test("UTF-8 aliases of one physical filename share ownership",async()=>{
 const prefix=`partial-owner-alias-${crypto.randomUUID()}`;
 const first=acquirePartialOwner(prefix+"\ud800");
 await first.ready;
 const alias=acquirePartialOwner(prefix+"\ud801");
 try { await expect(alias.ready).rejects.toMatchObject({code:"LIX_STORAGE_IN_USE"}); }
 finally { alias.close(); first.close(); }
});

test("provider shutdown waits for owned work to release its handle",async()=>{
 const { PartialOwnerLifetimes } = await import("../js/partial-owner.js");
 const pool=new PartialOwnerLifetimes();
 const owner=pool.acquire(`partial-owner-provider-${crypto.randomUUID()}`);
 await owner.ready;
 let closed=false;
 const closing=pool.close().then(()=>{closed=true;});
 await Promise.resolve();
 expect(closed).toBe(false);
 expect(()=>pool.acquire("another")).toThrow();
 owner.close();
 await closing;
 expect(closed).toBe(true);
});

test("actual OPFS client close preserves storage until owner release",async()=>{
 const { OpfsStorage } = await import("@lix-js/storage-opfs");
 const { OpfsStorageClient } = await import("../js/client.js");
 const name=`partial-owner-client-${crypto.randomUUID()}`;
 // Start the package-owned SQLite worker; the client remains in this realm.
 void new OpfsStorage({name}).lixStorage;
 const client=await OpfsStorageClient.open(name);
 const token=await client.acquireSession();
 const owner=client.acquirePartialReplicaOwner(token);
 await owner.ready;
 let closed=false;
 const closing=client.close().then(()=>{closed=true;});
 try {
  await Promise.resolve();
  expect(closed).toBe(false);
  const read=await client.beginRead({durability:"visible",consistency:"snapshot",sessionToken:token});
  expect(await read.getMany([])).toEqual([]);
 } finally { owner.close(); await closing; }
 expect(closed).toBe(true);
},30_000);
