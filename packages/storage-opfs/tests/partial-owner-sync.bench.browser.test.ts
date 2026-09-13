// Opt-in real WASM/OPFS lifecycle test; requires the seeded authority manifest
// and public partial-opening dispatch. No mocked transport or full bootstrap.
import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";
import { acquirePartialOwner } from "../js/partial-owner.js";

async function expectPhysicalOwnerHeld(name: string) {
 const contender=acquirePartialOwner(name);
 try { await expect(contender.ready).rejects.toMatchObject({code:"LIX_STORAGE_IN_USE"}); }
 finally { contender.close(); }
}

async function expectPhysicalOwnerReleased(name: string) {
 for(let attempt=0;attempt<100;attempt++) {
  const contender=acquirePartialOwner(name);
  try { await contender.ready; contender.close(); await new Promise(resolve=>setTimeout(resolve,0)); return; }
  catch(error) {
   contender.close();
   if((error as {code?:string}).code!=="LIX_STORAGE_IN_USE") throw error;
   await new Promise(resolve=>setTimeout(resolve,10));
  }
 }
 throw new Error("last session did not release the physical partial owner");
}

test("last child close releases partial owner and preserves pending edits on offline reopen",async()=>{
 const response=await fetch("/__partial_sync_profile.json");
 if (!response.ok) throw new Error("Seeded authority manifest is required");
 const [fixture]=await response.json() as Array<{url:string;headers?:Record<string,string>;key:string;expected:unknown}>;
 if (!fixture) throw new Error("At least one seeded repository is required");
 const name=`partial-owner-lix-${crypto.randomUUID()}`;
 let offline=false;
 const transport:typeof fetch=async(input,init)=>{
  if(offline) throw new TypeError("offline lifecycle check");
  return fetch(input,init);
 };
 const server={mode:"partial_replica" as const,url:fixture.url,headers:fixture.headers,fetch:transport};
 const root=await openLix({storage:new OpfsStorage({name}),server});
 const child=await root.openAnotherSession();
 try {
  await expectPhysicalOwnerHeld(name);
  const sql="SELECT value FROM lix_key_value WHERE key=$1";
  const joined=await openLix({storage:new OpfsStorage({name}),server});
  try { expect((await joined.execute(sql,[fixture.key])).rows[0]?.value).toEqual(fixture.expected); }
  finally { await joined.close(); }
  expect((await child.execute(sql,[fixture.key])).rows[0]?.value).toEqual(fixture.expected);
  offline=true;
  const pending=`pending-owner-edit-${crypto.randomUUID()}`;
  await child.execute("UPDATE lix_key_value SET value=$1 WHERE key=$2",[pending,fixture.key]);
  expect((await child.execute(sql,[fixture.key])).rows[0]?.value).toBe(pending);
  await root.close();
  await expectPhysicalOwnerHeld(name);
  expect((await child.execute(sql,[fixture.key])).rows[0]?.value).toBe(pending);
  await child.close();
  await expectPhysicalOwnerReleased(name);
  // root and child deliberately remain allocated. Reopen uses a fresh worker,
  // and network denial proves persisted admission/local native data suffices.
  const reopened=await openLix({storage:new OpfsStorage({name}),server:{...server,fetch:async()=>{throw new TypeError("offline lifecycle check");}}});
  try { expect((await reopened.execute(sql,[fixture.key])).rows[0]?.value).toBe(pending); }
  finally { await reopened.close(); }
 } finally { await child.close(); await root.close(); }
},120_000);
