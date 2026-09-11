# Partial replica with on-demand sync: browser profile

Run against a newly seeded canonical native Memory authority, never a mock or
preloaded browser database. The native fixture writes a JSON array:

```json
[{"dimension":"rows","size":16,"url":"http://127.0.0.1:PORT/lix/ID","key":"partial-open-000000","expected":"payload-000000"}]
```

The authority must allow CORS preflight, preserve all native response headers,
and handle concurrent long-poll and foreground requests. Use separate seeded
repositories for baseline, large rows, long history, and unrelated large blobs.
Restart the authority before each run because preparation makes real commits.

After building the current SDK/WASM and OPFS packages:

```sh
LIX_PARTIAL_PROFILE_MANIFEST=/absolute/path/manifest.json \
LIX_PARTIAL_PROFILE_RESULT=/absolute/path/browser-profile.json \
pnpm exec vitest run --config vitest.partial-sync.config.ts --reporter=verbose
```

Opening includes worker/WASM/OPFS setup. Authority request timing is separate;
the harness does not claim to isolate WASM initialization. Response counters
measure consumed decoded HTTP payload, not TCP bytes or compressed wire bytes.
They count attempted requests before offline rejection and stream without
prebuffering. Opening snapshots include in-flight background requests.

Cold SQL and first online UPDATE preparation have separate measurements. Thirty
warm SELECTs and UPDATEs run with transport disconnected, then OPFS closes and
reopens offline to verify pending edits. Background watch/lease/upload attempts
are reported; native input requests during prepared operations fail the gate.
The offline switch is a fetch-level disconnect, not termination of the authority.
OPFS uses a unique database name per fixture; results include every request and
individual timings. Report medians/p95 across repeated fresh-authority runs,
not one best sample. A separate CDP network capture is needed for wire-size data.

For complete versus partial local SQL attribution, use
`vitest.partial-local-comparison.config.ts` for the uninstrumented baseline and
`vitest.partial-provider-telemetry.config.ts` for the separate provider trace.
The latter reports `writeSpaces` as well as `bySpace` reads, allowing background
upload capture commits to be distinguished from foreground SQL and interest
journal writes. Its result includes five warmups plus ten measured pairs; use
`operations` to select measured statement trace IDs before aggregating counts.
Do not interpret instrumented elapsed durations as CPU samples or compare them
directly with the uninstrumented baseline.
