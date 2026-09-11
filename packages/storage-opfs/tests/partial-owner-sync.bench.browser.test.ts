// Opt-in real WASM/OPFS lifecycle test; requires the seeded authority manifest
// and public partial-opening dispatch. No mocked transport or full bootstrap.
import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";

test("last child close releases partial owner while closed Lix handles remain allocated",async()=>{
 const response=await fetch("/__partial_sync_profile.json");
 if (!response.ok) throw new Error("Seeded authority manifest is required");
 const [fixture]=await response.json() as Array<{url:string;headers?:Record<string,string>;key:string;expected:unknown}>;
 if (!fixture) throw new Error("At least one seeded repository is required");
 const name=`partial-owner-lix-${crypto.randomUUID()}`;
 const server={mode:"partial_replica" as const,url:fixture.url,headers:fixture.headers};
 const root=await openLix({storage:new OpfsStorage({name}),server});
 const child=await root.openAnotherSession();
 try {
  await expect(openLix({storage:new OpfsStorage({name}),server})).rejects.toMatchObject({code:"LIX_STORAGE_IN_USE"});
  const sql="SELECT value FROM lix_key_value WHERE key=$1";
  expect((await child.execute(sql,[fixture.key])).rows[0]?.value).toEqual(fixture.expected);
  await root.close();
  await expect(openLix({storage:new OpfsStorage({name}),server})).rejects.toMatchObject({code:"LIX_STORAGE_IN_USE"});
  await child.close();
  // root and child deliberately remain allocated. Reopen uses a fresh worker,
  // and network denial proves persisted admission/local native data suffices.
  const reopened=await openLix({storage:new OpfsStorage({name}),server:{...server,fetch:async()=>{throw new TypeError("offline lifecycle check");}}});
  try { expect((await reopened.execute(sql,[fixture.key])).rows[0]?.value).toEqual(fixture.expected); }
  finally { await reopened.close(); }
 } finally { await child.close(); await root.close(); }
},120_000);
