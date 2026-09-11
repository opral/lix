import {openLix} from "@lix-js/sdk";
import {OpfsStorage} from "@lix-js/storage-opfs";
import {expect,test} from "vitest";

type Fixture={dimension:string;size:number;url:string;key:string;expected:unknown;headers?:Record<string,string>};
const readSql="SELECT value FROM lix_key_value WHERE key = $1";
const writeSql="UPDATE lix_key_value SET value = $1 WHERE key = $2";

test("compares complete and partial OPFS warm SQL in the same WASM artifact",async()=>{
 const fixtures=(await(await fetch("/__partial_sync_profile.json")).json() as Fixture[]).filter(f=>/^rows_(16|16000)$/.test(f.dimension));
 expect(fixtures).toHaveLength(2);
 const results:unknown[]=[];
 for(const fixture of fixtures) {
  // Reverse order at the second scale to expose order/system warm-up effects.
  for(const mode of (fixture.size===16?["complete","partial"]:["partial","complete"])) {
   const storage=new OpfsStorage({name:`paired-${mode}-${crypto.randomUUID()}`});
   let offline=false; const controllers=new Set<AbortController>();
   const attempts:{method:string;path:string}[]=[];
   const transport:typeof fetch=async(input,init)=>{
    const url=new URL(input instanceof Request?input.url:String(input));
    const method=(init?.method??(input instanceof Request?input.method:"GET")).toUpperCase();
    if(offline){attempts.push({method,path:url.pathname});throw new TypeError("paired profile offline");}
    const controller=new AbortController();controllers.add(controller);
    const signal=init?.signal?AbortSignal.any([init.signal,controller.signal]):controller.signal;
    const response=await fetch(input,{...init,signal});
    const body=response.body?.pipeThrough(new TransformStream<Uint8Array,Uint8Array>({transform(bytes,sink){if(offline)throw new TypeError("paired profile disconnected");sink.enqueue(bytes);},flush(){controllers.delete(controller);}}));
    return new Response(body,{status:response.status,statusText:response.statusText,headers:response.headers});
   };
   const lix=mode==="partial"?await openLix({storage,server:{mode:"partial_replica",url:fixture.url,headers:fixture.headers,fetch:transport}}):await openLix({storage});
   try {
    if(mode==="complete") {
     // Same row count, keys, payload length and selected key as the authority;
     // fixture seeding and cold preparation are outside measured warm loops.
     for(let start=0;start<fixture.size;start+=256) {
      const values=Array.from({length:Math.min(256,fixture.size-start)},(_,i)=>{
       const key=String(start+i).padStart(6,"0");return `('partial-open-${key}','payload-${key}-${"x".repeat(128)}')`;
      }).join(",");
      await lix.execute(`INSERT INTO lix_key_value(key,value) VALUES ${values}`);
     }
    }
    const hoverStart=performance.now();
    const prefetched=await lix.execute(readSql,[fixture.key]);
    const hoverPrefetchMs=performance.now()-hoverStart;
    offline=true;for(const controller of controllers)controller.abort();
    expect(prefetched.rows[0]?.value).toEqual(fixture.expected);
    const selectMs:number[]=[],updateMs:number[]=[];
    for(let i=0;i<35;i++) {
     let begin=performance.now();
     const row=await lix.execute(readSql,[fixture.key]);
     const elapsed=performance.now()-begin;
     expect(row.rows[0]?.value).toBe(i===0?fixture.expected:`paired edit ${i-1}`);
     if(i>=5)selectMs.push(elapsed);
     begin=performance.now();await lix.execute(writeSql,[`paired edit ${i}`,fixture.key]);
     if(i>=5)updateMs.push(performance.now()-begin);
    }
    expect(attempts.filter(a=>/\/sync\/native-(objects|object-range|metadata)$/.test(a.path)||(a.method==="GET"&&/\/sync\/(blob|chunk)$/.test(a.path)))).toHaveLength(0);
    results.push({mode,rows:fixture.size,hoverPrefetchMs,selectMs,updateMs,offlineNetworkAttempts:attempts,excludedWarmups:5});
   }finally{await lix.close();}
  }
 }
 const artifact={benchmark:"same-artifact-complete-vs-partial-opfs",userAgent:navigator.userAgent,generatedAt:new Date().toISOString(),results,
  limits:"Same SQL/keys/row count/payload and OPFS provider/WASM artifact; complete native layout is intentionally materialized, partial working set hydrated on demand. First SELECT is the only online prefetch; all updates run disconnected. Five offline warmup iterations excluded from timing arrays but included in zero-hydration checks. No telemetry enabled in timing pass. Background upload attempts retained in partial mode."};
 const saved=await fetch("/__partial_sync_profile_result",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(artifact)});
 expect(saved.ok).toBe(true);console.info(JSON.stringify(artifact));
},600_000);
