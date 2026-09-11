import type {LixStorageProvider,LixStorageGetManyRequest,LixStorageProjectedValue,LixStorageWriteOptions,LixStorageSpace,LixStoragePutEntry} from "@lix-js/sdk";

type Options={moduleUrl:string;options:unknown;channel:string};
/** Benchmark-only wrapper of the real packaged provider; keeps native methods,
 * snapshot caching, physical owner capability and storage semantics intact. */
export async function createLixStorageProvider(raw:unknown):Promise<LixStorageProvider> {
 const options=raw as Options;
 const original=await import(/* @vite-ignore */options.moduleUrl) as {createLixStorageProvider(options:unknown):Promise<LixStorageProvider>};
 const provider=await original.createLixStorageProvider(options.options);
 const channel=new BroadcastChannel(options.channel);let traceId="";
 channel.onmessage=event=>{if(event.data?.kind==="trace"){traceId=event.data.traceId;channel.postMessage({kind:"traceAck",seq:event.data.seq});}};
 const measured=new Set(["beginRead","beginWrite","getMany","beginScan","nextPage","putMany","replaceMany","deleteMany","deleteRange","commit"]);
 const wrap=(target:object,owner:string):object=>new Proxy(target,{get(object,key){
  const value=Reflect.get(object,key,object);
  if(typeof value!=="function")return value;
  if(key==="close"&&owner==="provider")return async()=>{try{return await Reflect.apply(value,object,[]);}finally{channel.close();}};
  if(typeof key!=="string"||!measured.has(key))return value.bind(object);
  return async(...args:unknown[])=>{
   const trace=traceId;const started=performance.timeOrigin+performance.now();let result:unknown;
   try {result=await Reflect.apply(value,object,args);return key==="beginRead"?wrap(result as object,"read"):key==="beginWrite"?wrap(result as object,"write"):key==="beginScan"?wrap(result as object,"scan"):result;}
   finally {
    const elapsed=performance.timeOrigin+performance.now()-started;
    if(trace){
     // No row keys, values, credentials or payloads are recorded.
     const reads=key==="getMany"?(args[0] as LixStorageGetManyRequest[]):[];
     const values=key==="getMany"?(result as Array<LixStorageProjectedValue|null>|undefined):undefined;
     let offset=0;const bySpace=reads.map(request=>{const entries=values?.slice(offset,offset+request.keys.length)??[];offset+=request.keys.length;return {space:request.space.name,keys:request.keys.length,hits:entries.filter(Boolean).length,bytes:entries.reduce((n,v)=>n+(v?.kind==="fullValue"?v.value.byteLength:0),0)};});
     const writes=key==="putMany"||key==="replaceMany"?[{space:(args[0] as LixStorageSpace).name,keys:(args[1] as LixStoragePutEntry[]).length,bytes:(args[1] as LixStoragePutEntry[]).reduce((n,entry)=>n+entry.value.byteLength,0)}]:key==="deleteMany"?[{space:(args[0] as LixStorageSpace).name,keys:(args[1] as Uint8Array[]).length,bytes:0}]:key==="deleteRange"?[{space:(args[0] as LixStorageSpace).name,keys:null,bytes:0}]:[];
     channel.postMessage({kind:"io",traceId:trace,owner,operation:key,startedAtUnixMs:started,durationMs:elapsed,bySpace,writeSpaces:writes,writeOptions:key==="beginWrite"?{awaitDurable:(args[0] as LixStorageWriteOptions).awaitDurable,preconditions:(args[0] as LixStorageWriteOptions).preconditions.length}:undefined,stats:key==="commit"?(result as {stats?:unknown}|undefined)?.stats:undefined});
    }
   }
  };
 }});
 channel.postMessage({kind:"ready"});
 return wrap(provider,"provider") as LixStorageProvider;
}
