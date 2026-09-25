// The real bundled OPFS backend. Fixtures stage supported historical marker
// envelopes; v77 also accepts already-rewritten commit records on migration retry.
// @ts-expect-error bundled provider has no declaration
import {OpfsBackend} from '../../storage-opfs/dist/direct.js';
import type {OpfsBackend as Backend} from '../../storage-opfs/js/provider.js';
import {migrateStorageProvider} from "../dist/migration.js";
import type {LixStorageSpace} from '../src/storage-adapter.js';
const space=(id:number,name:string):LixStorageSpace=>({id,name,valueSemantics:'mutable',valueIntegrity:'backendVerified'});
const encode=(text:string)=>new TextEncoder().encode(text);
self.onmessage=async({data})=>{
 let db:Backend|undefined;
 let outcome:unknown;
 try {
  db=await OpfsBackend.open(data.name);
  if(data.downgrade) {
   const sessionToken=await db.acquireSession();
   const epoch=space(0x00090001,'repository.epoch.v1');
   const read=await db.beginRead({sessionToken,consistency:'snapshot',durability:'durable'});
   const [entry]=await read.getMany([{space:epoch,keys:[encode('active')],options:{projection:'fullValue'}}]);
   if(entry?.kind!=='fullValue')throw new Error('Missing active epoch');
   const fields=new TextDecoder().decode(entry.value).split('|');
   if(fields[1]!=='active'||fields[4]!=='83')throw new Error('Expected current repository');
   // Fresh partial stores use legacy physical bank; support A/B if the runtime changes allocation.
   const prefix=fields[2]==='legacy'?0:fields[2]==='a'?0x40000000:fields[2]==='b'?0x80000000:NaN;
   if(!Number.isFinite(prefix))throw new Error(`Unsupported fixture bank ${fields[2]}`);
   const format=data.format??80;
   if(format!==77&&format!==80)throw new Error('Unsupported test source format');
   fields[4]=String(format);
   const write=await db.beginWrite({sessionToken,awaitDurable:true,preconditions:[],batchCapacityHintBytes:1024});
   await write.putMany(epoch,[{key:encode('active'),value:encode(fields.join('|'))}]);
   await write.putMany(space((prefix+0x00040011)>>>0,'repository.protocol.v1'),[{key:encode('current'),value:encode(`tracked-default-branch.v${format}${data.partial===false?'':'-partial-replica.v1'}`)}]);
   await write.commit();
  }
  const report=data.migrate?await migrateStorageProvider(db):undefined;
  outcome={digest:await db.migrationDigest(),report};
 }catch(error){outcome={error:String(error)};}
 finally{await db?.close();self.postMessage(outcome);self.close();}
};
