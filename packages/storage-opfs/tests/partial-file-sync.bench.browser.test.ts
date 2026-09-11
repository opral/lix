// Run only with vitest.partial-file-sync.config.ts against a canonical seeded authority.
// The harness must serve /__partial_sync_profile.json from a locally generated
// manifest. Repositories are seeded on the authority, never in this browser.
import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";

type Fixture = { dimension: string; size: number; url: string; headers?: Record<string,string>; directory: string; directoryFiles: number; target: string; negative: string };
type Transfer = { phase: string; method: string; path: string; kind: string; bytes: number; status?: number; blocked?: boolean; errorBody?: string; startedMs?: number; headersMs?: number; completedMs?: number; nativeRequest?: unknown };

// Native object/metadata reads use POST; blob/chunk GETs read inputs while
// POST blob registration and PUT chunks belong to the independent upload lane.
const isInputRead = (item:Transfer):boolean => {
 const path=item.path.split("?")[0]!;
 return /\/sync\/native-(objects|metadata|object-range)$/.test(path)
  || (item.method==="GET" && /\/sync\/(blob|chunk)$/.test(path));
};
const isBackgroundUpload = (item:Transfer):boolean => {
 const path=item.path.split("?")[0]!;
 return (item.method==="POST" && /\/sync\/(blob|push|merge|body-wave)$/.test(path))
  || (item.method==="PUT" && /\/sync\/chunk$/.test(path));
};

test("profiles public file tree, negative scope and editable content in OPFS", async () => {
 const manifest = await fetch("/__partial_sync_profile.json");
 if (!manifest.ok) throw new Error("Start the seeded real authority and install the profile manifest; no mocked or full-populate fallback");
 const fixtures = await manifest.json() as Fixture[];
 const results: unknown[] = [];
 for (const fixture of fixtures) {
  const transfers: Transfer[] = [];
  const unexpectedOpeningRequests: string[] = [];
  const inFlight = new Set<AbortController>();
  const repositoryPath = new URL(fixture.url).pathname.replace(/\/$/, "");
  const protocolRepositoryPath = repositoryPath.replace(/\/lix\//, "/lix/v1/");
  const isDescriptor = (url: URL) => url.pathname === `${protocolRepositoryPath}/sync/descriptor`;
  const isOpeningRoute = (url: URL, method: string) => {
   if (method !== "GET") return false;
   if (url.pathname.replace(/\/$/, "") === protocolRepositoryPath) return url.search === "";
   if (!isDescriptor(url)) return false;
   // Opening selects the default branch; only the worker's cursor watch may
   // add query parameters. Unknown routes or parameters fail the opening gate.
   if (url.search === "") return true;
   const keys=[...url.searchParams.keys()];
   return keys.length===2 && keys.includes("branchId") && keys.includes("after")
    && /^[0-9]+$/.test(url.searchParams.get("after") ?? "")
    && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(url.searchParams.get("branchId") ?? "");
  };
  let phase = "open", offline = false, firstRequestAt: number | undefined;
  const countedFetch: typeof fetch = async (input, init) => {
   const url = new URL(input instanceof Request ? input.url : String(input));
   const path = url.pathname + url.search;
   const method = init?.method ?? (input instanceof Request ? input.method : "GET");
   const kind = isDescriptor(url)
    ? (url.searchParams.has("after") ? "backgroundWatch" : "foregroundDescriptor")
    : method === "GET" && url.pathname.replace(/\/$/, "") === protocolRepositoryPath ? "foregroundHandshake" : "backgroundOrDemand";
   const record: Transfer = { phase, method: method.toUpperCase(), path, kind, bytes: 0, startedMs: performance.now() };
   transfers.push(record);
   firstRequestAt ??= performance.now();
   if (offline) { record.blocked = true; throw new TypeError("profile transport disconnected"); }
   if (phase === "open" && !isOpeningRoute(url, method)) {
    unexpectedOpeningRequests.push(`${method} ${path}`);
    throw new Error(`Unexpected repository I/O during opening: ${method} ${path}`);
   }
   const controller = new AbortController();
   inFlight.add(controller);
   const signal = init?.signal ? AbortSignal.any([init.signal, controller.signal]) : controller.signal;
   // Native request DTOs contain only bounded immutable addresses/offsets;
   // capture their families to distinguish singleton traversal from batching.
   // Do not capture SQL, headers, user rows, or blob upload payloads.
   if (/\/sync\/native-(objects|metadata|object-range)$/.test(url.pathname)) {
    const body=init?.body;
    let encoded:string|undefined;
    if(typeof body==="string") encoded=body;
    else if(body instanceof ArrayBuffer) encoded=body.byteLength<=16384?new TextDecoder().decode(body):undefined;
    else if(ArrayBuffer.isView(body)) encoded=body.byteLength<=16384?new TextDecoder().decode(body):undefined;
    else if(input instanceof Request) encoded=await input.clone().text();
    if(encoded!==undefined && encoded.length<=16384) {
     try {record.nativeRequest=JSON.parse(encoded);} catch {record.nativeRequest="unparsed native request";}
    }
   }
   let response: Response;
   try { response = await fetch(input, { ...init, signal }); record.headersMs=performance.now(); }
   catch (error) { inFlight.delete(controller); throw error; }
   if (offline) {
    controller.abort(); inFlight.delete(controller);
    await response.body?.cancel().catch(() => undefined);
    throw new TypeError("profile disconnected before response completed");
   }
   record.status = response.status;
   if (!response.ok && response.body) {
    // Failure diagnostics only; bound the cloned error stream independently.
    const reader=response.clone().body!.getReader(); const chunks:Uint8Array[]=[]; let total=0;
    try { while(total<4096) { const part=await reader.read(); if(part.done)break; const bytes=part.value.subarray(0,4096-total); chunks.push(bytes); total+=bytes.length; } }
    finally { void reader.cancel().catch(()=>undefined); }
    const bytes=new Uint8Array(total); let offset=0; for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.length;}
    record.errorBody=new TextDecoder().decode(bytes);
   }
   // Count bytes as consumed, without clone/arrayBuffer prebuffering that would
   // distort streaming or memory behavior. Bytes are decoded HTTP payload size.
   const body = response.body?.pipeThrough(new TransformStream<Uint8Array,Uint8Array>({
    transform(chunk, sink) {
     if (offline || signal.aborted) { inFlight.delete(controller); throw new TypeError("profile disconnected during response"); }
     record.bytes += chunk.byteLength; sink.enqueue(chunk);
    },
    flush() { record.completedMs=performance.now(); inFlight.delete(controller); },
   }));
   if (!body) {record.completedMs=performance.now(); inFlight.delete(controller);}
   signal.addEventListener("abort", () => inFlight.delete(controller), {once:true});
   return new Response(body, { status: response.status, statusText: response.statusText, headers: response.headers });
  };
  const storage = new OpfsStorage({ name: `partial-profile-${crypto.randomUUID()}` });
  const started = performance.now();
  let lix = await openLix({ storage, server: { mode: "partial_replica", url: fixture.url, headers: fixture.headers, fetch: countedFetch } });
  const opened = performance.now();
  try {
   const openingTransfers = transfers.map(item => ({ ...item }));
   expect(unexpectedOpeningRequests).toEqual([]);
   const foreground = openingTransfers.filter(item => item.kind.startsWith("foreground"));
   expect(foreground).toHaveLength(2);
   expect(foreground.reduce((bytes, item) => bytes + item.bytes, 0)).toBeLessThanOrEqual(8192);
   // Excludes unrelated WASM/JS resource downloads; includes handshake/descriptor.
   expect(openingTransfers.some(item => item.path.includes("descriptor"))).toBe(true);
   expect(openingTransfers.every(item => item.kind === "foregroundHandshake" || item.kind === "foregroundDescriptor" || item.kind === "backgroundWatch")).toBe(true);
   expect(openingTransfers.some(item => /snapshot|native-objects|native-object-range|native-metadata|\/blob|\/chunk/.test(item.path))).toBe(false);
   const timings: Record<string, number> = {};
   const measured = async <T>(name: string, action: () => Promise<T>): Promise<T> => {
    phase = name; const begin = performance.now();
    try { return await action(); } finally { timings[name] = performance.now() - begin; }
   };
   // Prefix means direct children here: this fixture has no deeper descendants.
   const pattern = fixture.directory + "/%";
   const listSql = "SELECT id,path FROM lix_file WHERE path LIKE $1 ORDER BY path";
   const countSql = "SELECT COUNT(*) AS n FROM lix_file WHERE path LIKE $1";
   const contentSql = "SELECT content FROM lix_file WHERE path=$1";
   const listed = await measured("coldDirectoryList", () => lix.execute(listSql, [pattern]));
   expect(listed.rows).toHaveLength(fixture.directoryFiles);
   const count = await measured("coldDirectoryCount", () => lix.execute(countSql, [pattern]));
   expect(Number(count.rows[0]?.n)).toBe(fixture.directoryFiles);
   expect((await measured("coldNegativeContent", () => lix.execute(contentSql, [fixture.negative]))).rows).toHaveLength(0);
   const initial = await measured("coldTargetContent", () => lix.execute(contentSql, [fixture.target]));
   let bytes = initial.rows[0]?.content as Uint8Array;
   expect(bytes).toBeInstanceOf(Uint8Array);
   expect(bytes.byteLength).toBe(96 * 1024);

   // A separate remote session mutates the real authority. The browser working
   // set must advance through the production descriptor watcher/publication.
   const remote = await openLix({server:{url:fixture.url,headers:fixture.headers}});
   const appeared = new Uint8Array([91,92,93]);
   let publicationAcknowledged = 0;
   try { await remote.execute("INSERT INTO lix_file(path,content) VALUES($1,$2)", [fixture.negative,appeared]); publicationAcknowledged = performance.now(); }
   finally { await remote.close(); }
   await measured("remoteNegativePublication", async () => {
    const deadline = performance.now()+30_000;
    while (performance.now()<deadline) {
     const result = await lix.execute(contentSql,[fixture.negative]);
     if(result.rows.length) { expect(result.rows[0]?.content).toEqual(appeared); return; }
     await new Promise(resolve=>setTimeout(resolve,50));
    }
    throw new Error("Retained empty file scope did not receive remote insertion");
   });
   // Includes remote close and up to one polling interval after INSERT acknowledgment.
   timings.remoteNegativePublication = performance.now() - publicationAcknowledged;
   expect(Number((await lix.execute(countSql,[pattern])).rows[0]?.n)).toBe(fixture.directoryFiles+1);
   await measured("retainedFileReadAfterPublication", () => lix.execute(contentSql,[fixture.target]));
   expect((await lix.execute(contentSql,[fixture.target])).rows[0]?.content).toEqual(bytes);
   offline=true; for(const controller of inFlight) controller.abort();
   const offlineStart=transfers.length;
   const warmSelectMs:number[]=[], warmUpdateMs:number[]=[], warmCountMs:number[]=[];
   for(let i=0;i<30;i++) {
    phase="offlineContentRead"; let begin=performance.now();
    const warmRead=await lix.execute(contentSql,[fixture.target]);
    warmSelectMs.push(performance.now()-begin);
    expect(warmRead.rows[0]?.content).toEqual(bytes);
    bytes=new Uint8Array(bytes); bytes[1+i]^=1;
    phase="offlineContentWrite"; begin=performance.now();
    await lix.execute("UPDATE lix_file SET content=$1 WHERE path=$2",[bytes,fixture.target]);
    warmUpdateMs.push(performance.now()-begin);
    phase="offlineCount"; begin=performance.now();
    const warmCount=await lix.execute(countSql,[pattern]);
    warmCountMs.push(performance.now()-begin);
    expect(Number(warmCount.rows[0]?.n)).toBe(fixture.directoryFiles+1);
   }
   expect(transfers.slice(offlineStart).filter(isInputRead)).toHaveLength(0);
   await lix.close();
   await measured("offlineReopen", async()=> { lix=await openLix({storage,server:{mode:"partial_replica",url:fixture.url,headers:fixture.headers,fetch:countedFetch}}); });
   expect((await lix.execute(contentSql,[fixture.target])).rows[0]?.content).toEqual(bytes);
   expect((await lix.execute(contentSql,[fixture.negative])).rows[0]?.content).toEqual(appeared);
   expect(Number((await lix.execute(countSql,[pattern])).rows[0]?.n)).toBe(fixture.directoryFiles+1);
   expect(transfers.slice(offlineStart).filter(isInputRead)).toHaveLength(0);
   results.push({dimension:fixture.dimension,size:fixture.size,coldOpenMs:opened-started,timings,warmSelectMs,warmUpdateMs,warmCountMs,
    offlineInputReadAttempts:transfers.slice(offlineStart).filter(isInputRead).length,offlineBackgroundUploadAttempts:transfers.slice(offlineStart).filter(isBackgroundUpload).length,openingTransfers,foregroundOpeningRequests:foreground.length,foregroundOpeningBytes:foreground.reduce((n,x)=>n+x.bytes,0),transfers});
  } catch(error) {
   const failure={benchmark:"partial-replica-opfs-file-tree-failure",fixture,phase,error:String(error),transfers,completed:results};
   console.error(JSON.stringify(failure));
   await fetch("/__partial_sync_profile_result",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(failure)}).catch(()=>undefined);
   throw error;
  } finally { phase="close"; await lix.close(); }
 }
 const report={benchmark:"partial-replica-opfs-file-tree",generatedAt:new Date().toISOString(),userAgent:navigator.userAgent,
  responseByteSemantics:"decoded response payload consumed by browser replica; excludes separate remote mutation session, JS/WASM and HTTP framing",results};
 const saved=await fetch("/__partial_sync_profile_result",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(report)});
 expect(saved.ok).toBe(true); console.info(JSON.stringify(report));
},600_000);
