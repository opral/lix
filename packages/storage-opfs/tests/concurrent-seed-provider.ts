import type {LixStorageProvider} from '@lix-js/sdk';
export async function createLixStorageProvider(options:{moduleUrl:string;providerOptions:unknown;gate:string}):Promise<LixStorageProvider>{
 const module=await import(/* @vite-ignore */ options.moduleUrl);
 const provider:LixStorageProvider=await module.createLixStorageProvider(options.providerOptions);
 const channel=new BroadcastChannel(options.gate);
 let release!:()=>void;
 const resumed=new Promise<void>(resolve=>{release=resolve;});
 channel.onmessage=event=>{if(event.data==='resume')release();};
 let paused=false;
 return new Proxy(provider,{get(target,property){
  if(property==='beginRead')return async(...args:Parameters<LixStorageProvider['beginRead']>)=>{
   const read=await target.beginRead(...args);
   return new Proxy(read,{get(reader,key){
    if(key==='getMany')return async(...requests:Parameters<typeof read.getMany>)=>{
     if(!paused&&requests[0].some(request=>(request.space.id&0x3fffffff)>=0x50001&&(request.space.id&0x3fffffff)<=0x50004)){
      paused=true;channel.postMessage('seed-paused');await resumed;
     }
     return reader.getMany(...requests);
    };
    const value=Reflect.get(reader,key);return typeof value==='function'?value.bind(reader):value;
   }});
  };
  if(property==='close')return async()=>{channel.close();await target.close();};
  const value=Reflect.get(target,property);return typeof value==='function'?value.bind(target):value;
 }});
}
