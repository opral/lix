import { expect, test } from 'vitest';
import { openLix, networkFetch, type Lix } from '../dist/index.js';
import { OpfsStorage } from '../../storage-opfs/dist/index.js';

test.runIf(import.meta.env.LIX_AUTHORITY_COMPOSITION === true)('actual authority, WASM, SharedWorker and OPFS compose through credential rotation', async () => {
  const name = `admission-composition-${crypto.randomUUID()}`;
  const url = `${location.origin}/lix/00000000-0000-7000-8000-000000000004`;
  const opened: Lix[] = [];
  let admissions = 0;
  async function attach(token: string) {
    const lix = await openLix({storage:new OpfsStorage({name}),server:{url,mode:'partial_replica',
      headers:()=>({Authorization:token}), fetch:async(input,init)=>{
        if(String(input).endsWith('/admission')) admissions++;
        return networkFetch(input,init);
      }}});
    opened.push(lix);
    return lix;
  }
  try {
    const first = await attach('Bearer initial');
    const same = await attach('Bearer initial');
    const rotated = await attach('Bearer refreshed');
    expect(admissions).toBe(3);
    expect(await first.activeAccountId()).toBe('00000000-0000-7000-8000-000000000003');
    expect(await rotated.activeAccountId()).toBe(await first.activeAccountId());
    await first.execute("INSERT INTO lix_key_value (key, value) VALUES ('admission-composition', 'preserved')");
    expect((await same.execute("SELECT value FROM lix_key_value WHERE key = 'admission-composition'")).rows).toEqual([{value:'preserved'}]);
    expect((await rotated.execute("SELECT value FROM lix_key_value WHERE key = 'admission-composition'")).rows).toEqual([{value:'preserved'}]);
    await expect(attach('Bearer other-account')).rejects.toMatchObject({code:'LIX_SHARED_ENGINE_IDENTITY_MISMATCH'});
    await first.close(); opened.splice(opened.indexOf(first),1);
    expect((await rotated.execute("SELECT value FROM lix_key_value WHERE key = 'admission-composition'")).rows).toEqual([{value:'preserved'}]);
  } finally {await Promise.all(opened.map(lix=>lix.close()));}
},60000);
