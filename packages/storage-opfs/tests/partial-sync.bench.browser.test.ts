// Run only with vitest.partial-sync.config.ts against a canonical seeded authority.
// The harness must serve /__partial_sync_profile.json from a locally generated
// manifest. Repositories are seeded on the authority, never in this browser.
import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";

type Fixture = { dimension: string; size: number; url: string; headers?: Record<string,string>; key: string; expected: unknown };
type Transfer = { phase: string; method: string; path: string; kind: string; bytes: number; status?: number; blocked?: boolean };

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

test("profiles real partial replica with on-demand sync in OPFS", async () => {
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
   const record: Transfer = { phase, method: method.toUpperCase(), path, kind, bytes: 0 };
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
   let response: Response;
   try { response = await fetch(input, { ...init, signal }); }
   catch (error) { inFlight.delete(controller); throw error; }
   if (offline) {
    controller.abort(); inFlight.delete(controller);
    await response.body?.cancel().catch(() => undefined);
    throw new TypeError("profile disconnected before response completed");
   }
   record.status = response.status;
   // Count bytes as consumed, without clone/arrayBuffer prebuffering that would
   // distort streaming or memory behavior. Bytes are decoded HTTP payload size.
   const body = response.body?.pipeThrough(new TransformStream<Uint8Array,Uint8Array>({
    transform(chunk, sink) {
     if (offline || signal.aborted) { inFlight.delete(controller); throw new TypeError("profile disconnected during response"); }
     record.bytes += chunk.byteLength; sink.enqueue(chunk);
    },
    flush() { inFlight.delete(controller); },
   }));
   if (!body) inFlight.delete(controller);
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
   phase = "coldSql";
   const sqlStart = performance.now();
   const first = await lix.execute("SELECT value FROM lix_key_value WHERE key = $1", [fixture.key]);
   expect(first.rows[0]?.value).toEqual(fixture.expected);
   const coldSqlMs = performance.now() - sqlStart;
   phase = "writePreparation";
   const preparationStart = performance.now();
   await lix.execute("UPDATE lix_key_value SET value = $1 WHERE key = $2", ["prepared edit", fixture.key]);
   const writePreparationMs = performance.now() - preparationStart;
   offline = true;
   for (const controller of inFlight) controller.abort();
   inFlight.clear();
   const offlineStart = transfers.length;
   const warmSelectMs: number[] = [], warmUpdateMs: number[] = [];
   for (let i = 0; i < 30; i++) {
    phase = "offlineWarmSelect";
    const readStart = performance.now();
    const read = await lix.execute("SELECT value FROM lix_key_value WHERE key = $1", [fixture.key]);
    warmSelectMs.push(performance.now() - readStart);
    expect(read.rows).toHaveLength(1);
    expect(read.rows[0]?.value).toBe(i === 0 ? "prepared edit" : `offline profile edit ${i - 1}`);
    phase = "offlineWarmUpdate";
    const updateStart = performance.now();
    await lix.execute("UPDATE lix_key_value SET value = $1 WHERE key = $2", [`offline profile edit ${i}`, fixture.key]);
    warmUpdateMs.push(performance.now() - updateStart);
   }
   // This deliberately stronger gate rejects ANY native input request during
   // warm execution, including background demand; it does not infer attribution.
   // No prior online response can deliver data after the disconnect boundary.
   expect(transfers.slice(offlineStart).filter(isInputRead)).toHaveLength(0);
   const edited = await lix.execute("SELECT value FROM lix_key_value WHERE key = $1", [fixture.key]);
   expect(edited.rows[0]?.value).toBe("offline profile edit 29");
   phase = "close";
   await lix.close();
   phase = "offlineReopen";
   const reopenStart = performance.now();
   lix = await openLix({ storage, server: { mode: "partial_replica", url: fixture.url, headers: fixture.headers, fetch: countedFetch } });
   const offlineReopenMs = performance.now() - reopenStart;
   expect((await lix.execute("SELECT value FROM lix_key_value WHERE key = $1", [fixture.key])).rows[0]?.value).toBe("offline profile edit 29");
   expect(transfers.slice(offlineStart).filter(isInputRead)).toHaveLength(0);
   results.push({ dimension: fixture.dimension, size: fixture.size, coldOpenMs: opened-started,
    // This includes worker+WASM+OPFS initialization; do not label it WASM-only.
    beforeFirstAuthorityRequestMs: firstRequestAt === undefined ? null : firstRequestAt-started,
    wasmInitMs: null, wasmInitNote: "requires worker-side initializeWasm timing event",
    coldSqlMs, writePreparationMs, warmSelectMs, warmUpdateMs, warmIterations: 30, offlineReopenMs,
    offlineInputReadAttempts:transfers.slice(offlineStart).filter(isInputRead).length,offlineBackgroundUploadAttempts:transfers.slice(offlineStart).filter(isBackgroundUpload).length,
    foregroundOpeningRequests: foreground.length, foregroundOpeningResponseBytes: foreground.reduce((n,x)=>n+x.bytes,0), openingRequests: openingTransfers.length, openingResponseBytes: openingTransfers.reduce((n,x)=>n+x.bytes,0), transfers });
  } finally { phase = "close"; await lix.close(); }
 }
 const report = { benchmark: "partial-replica-opfs-sync", userAgent: navigator.userAgent,
  responseByteSemantics: "decoded HTTP payload consumed by fetch; excludes JS/WASM/HTTP framing",
  generatedAt: new Date().toISOString(), results };
 const saved = await fetch("/__partial_sync_profile_result", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(report) });
 expect(saved.ok).toBe(true);
 console.info(JSON.stringify(report));
}, 600_000);
