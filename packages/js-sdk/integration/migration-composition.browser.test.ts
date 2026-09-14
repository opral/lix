import {expect,test} from 'vitest';
import {openLix,networkFetch,HttpTransportError} from '../dist/index.js';
import {OpfsStorage} from '../../storage-opfs/dist/index.js';
import {migrateOpfsReplica} from '../../storage-opfs/dist/migration.js';
import {OPFS_RPC_PROTOCOL_VERSION} from '../../storage-opfs/js/rpc.js';
async function stored(name:string,downgrade=false,phase="inspect"):Promise<string>{
 const worker=new Worker(new URL('./migration-stage.browser.worker.ts',import.meta.url),{type:'module'});
 try{return await new Promise((resolve,reject)=>{worker.onerror=e=>reject(new Error(e.message));worker.onmessage=({data})=>data.error?reject(new Error(`${phase}: ${data.error}`)):resolve(data.digest);worker.postMessage({name,downgrade});});}
 finally{worker.terminate();}
}
test('detached WASM and built OPFS migrator preserve offline work through owner loss and v80 cutover',async()=>{
 const sourceName=`migration-composition-${crypto.randomUUID()}`;
 const destinationName=`${sourceName}-v81`;
 const url=`${location.origin}/lix/00000000-0000-7000-8000-000000000004`;
 const headers:[string,string][]=[['Authorization','Bearer migration-fixture']];
 let offline=false;
 const lix=await openLix({storage:new OpfsStorage({name:sourceName}),server:{url,mode:'partial_replica',headers,fetch:(input,init)=>{
  if(offline)throw new HttpTransportError('LIX_TRANSPORT_NETWORK','test network disconnected');
  return networkFetch(input,init);
 }}});
 const key=`migration-${crypto.randomUUID()}`;
 try{
  await lix.execute('INSERT INTO lix_key_value (key,value) VALUES ($1,$2)',[key,'initial']);
  await lix.execute('SELECT value FROM lix_key_value WHERE key=$1',[key]);
  offline=true;
  await lix.execute('UPDATE lix_key_value SET value=$1 WHERE key=$2',['pending-retained',key]).catch(error=>{throw new Error('offline local update failed',{cause:error});});
  expect((await lix.execute('SELECT value FROM lix_key_value WHERE key=$1',[key]).catch(error=>{throw new Error('offline local read failed',{cause:error});})).rows).toEqual([{value:'pending-retained'}]);
 }finally{await lix.close();}
 const sourceDigest=await stored(sourceName,true,"after SDK close");
 await expect(migrateOpfsReplica({sourceName,destinationName:`${destinationName}-rejected`,server:{url,headers},expectedSourceDigest:'changed'})).rejects.toMatchObject({code:'LIX_MIGRATION_SOURCE_CHANGED'});
 expect(await stored(sourceName,false,"after digest rejection")).toBe(sourceDigest);
 // Kill the actual migrator after physical source ownership is acquired and
 // before publication. A retry uses a distinct destination; source remains intact.
 const crashed=new Worker(new URL('../../storage-opfs/dist/migration.worker.js',import.meta.url),{type:'module'});
 await new Promise<void>((resolve,reject)=>{crashed.onerror=e=>reject(new Error(e.message));crashed.onmessage=({data})=>{if(data.phase==='copying'){crashed.terminate();resolve();}else if(data.ok===false)reject(new Error(data.error.message));};crashed.postMessage({sourceName,destinationName:`${destinationName}-crashed`,server:{url,headers}});});
 // terminate() requests worker shutdown; wait for the browser to release both
 // real owner locks before testing recovery. This is a fence, not a timed delay.
 await navigator.locks.request(`lix:opfs-owner:rpc-v${OPFS_RPC_PROTOCOL_VERSION}:${sourceName}`,{signal:AbortSignal.timeout(10000)},async()=>{
  await navigator.locks.request(`lix:opfs-sqlite:${sourceName}`,{signal:AbortSignal.timeout(10000)},()=>{});
 });
 expect(await stored(sourceName,false,"after worker termination")).toBe(sourceDigest);
 const result=await migrateOpfsReplica({sourceName,destinationName,server:{url,headers},expectedSourceDigest:sourceDigest});
 expect(result.sourceDigest).toBe(sourceDigest);
 expect(await stored(sourceName,false,"after migration success")).toBe(sourceDigest);
 const reopened=await openLix({storage:new OpfsStorage({name:destinationName}),server:{url,mode:'partial_replica',headers}});
 try{expect((await reopened.execute('SELECT value FROM lix_key_value WHERE key=$1',[key])).rows).toEqual([{value:'pending-retained'}]);}
 finally{await reopened.close();}
},120000);

test('actual OPFS migrator accepts a fresh empty profile without migration work',async()=>{
 const suffix=crypto.randomUUID();
 const result=await migrateOpfsReplica({sourceName:`migration-empty-${suffix}`,destinationName:`migration-empty-${suffix}-v81`,server:{url:`${location.origin}/lix/00000000-0000-7000-8000-000000000004`,headers:[]}});
 expect(result.empty).toBe(true);
},30000);
