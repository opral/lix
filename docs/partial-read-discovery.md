# Partial-replica read dependency discovery

A cold read previously discovered one missing native pointer, fetched it, and
restarted SQL to discover the next pointer. Batching already-known addresses did
not remove the dependency chain: the client did not yet know the next addresses.

The authority has the complete graph. Protocol 20 therefore adds
`POST /sync/read-fulfillment`: the client sends typed logical read interests,
its admitted descriptor and epoch, the first missing immutable inputs, and its
baseline lease. The authority evaluates those interests against the leased
roots in one storage snapshot and returns the selected immutable input closure.
It does not execute client SQL or return query results. After installing the
closure, the client evaluates locally, including its own pending changes and
untracked state.

## Why discovery belongs at the authority

The network cost of pointer chasing is approximately dependency depth times
RTT. Increasing a transfer batch size cannot reduce that depth when the next
addresses are encoded inside the current response. The database principle is
to send a bounded operation to the data, resolve its dependencies in one local
snapshot, and transfer the selected working set. Network rounds then follow
bounded response pages rather than individual graph edges.

This extends the history-discovery work in `3e677a214` (#1817) and the authority
history inventory work in `83b47246d` (#1848). Independent metadata discovery in
`4ebc7e78d` reduced that history profile from 381 to 320 requests. The later
endpoint-only experiment in `651253efc` left the request count at 320 and was
rejected. The important unit of batching is the unresolved logical read.

Client evaluation remains necessary: the authority does not have unpublished
client edits. Sending typed native interests instead of arbitrary SQL also
keeps discovery bounded and avoids executing client plugins on the server.

## Isolation and integrity

Discovery creates fresh serving generations and fresh native caches. It stages
only branch controls and root bases; it does not reuse the authority's current
mutable overlay. Decoded process-global payload caches must perform physical
reads during discovery so cache hits cannot hide dependency addresses.

The wire input algebra admits only native metadata, content-addressed native
objects, canonical blob manifests, and verified raw blob chunks. Arbitrary
storage keys, HOT rows, and local physical CAS layouts cannot be installed.
Logical blob reads are captured before delta/base traversal, so ranged reads
select canonical chunks for the requested range instead of exporting an
unrelated physical base. Discovery prepares plugin dependencies without
executing plugin code. The closure also includes logically derived change
locators and owning commit inventories, even when a selected row has no
physical locator record. Admitted head/checkpoint mutation inventories are retained even for empty
checkpoints, so warmed replicas can still publish local writes offline.
Internal plugin-registry and file-owner reads belong
to that same closure; otherwise a later local preparation step would restart
the network waterfall. Foreground registry and owner loading uses the same
correlated exact lookups as discovery. A client collection scan for an already
known identity can otherwise require additional catalogs even when authority
side exact lookup has proved that identity absent.

Every response binds to the repository, epoch, request, and closure digest.
Installation verifies each input and the complete closure before an atomic,
durable, admission-fenced write. Existing immutable inputs must agree; existing
valid blob layouts remain intact. UUID-addressed graph/locator metadata has a
mutable local lifecycle: a differing, validated resident value wins over an
optional authority observation, preserving pending local work. Required inputs
cannot use that exception. If a locally retained commit uses a different valid
physical representation, its header, catalog, and directly addressed parts stay
coherent as one owner bundle. Optional inputs from another representation do
not replace individual members of that bundle; required inputs and
content-addressed objects remain strict. A changed descriptor or epoch rejects
installation.
Hydration supplies data, never persisted row coverage or absence certificates.

Pinned transactions receive an ephemeral list of installed immutable coordinates.
Their original read remains primary; the hydrated read can fill only absent
immutable keys. Canonical manifest-prefix scans become visible only if neither
the original read nor staged local bytes already supplied that manifest. This
avoids replacing an existing physical layout or exposing newer mutable rows.

## Bounds and pagination

Recipes have a 512 KiB / 4,096-interest limit. Discovery is bounded by 65,536
storage calls, 1 GiB of observed storage bytes, and a 256 MiB / 16,384-input
closure. Every wire page has at most 4 MiB decoded payload; larger canonical
members use ranges carrying address, total length, offset, and whole-member digest.
A codec member remains capped separately at 64 MiB. Continuations carry opaque
spool ownership plus exact input/offset progress and the complete closure digest.
The authority discovers once in the pinned snapshot, seals its bounded spool,
and revalidates account/repository/lease/epoch/request admission before serving
later pages. Native scratch has a 512 MiB operation and 1 GiB process disk cap;
sealed operations expire within two minutes and their baseline lease. In-flight
capacity is reserved before discovery, and replay egress is charged to a hard
three-times operation page/byte budget. Bound release requests finish or cancel
idempotently; abrupt cancellation uses bounded expiry.

The client reserves bounded private staging before receiving data. Small closures
can retain payloads under a shared nonblocking 4 MiB permit; unavailable capacity,
oversized payloads and framed records use durable owner-fenced scratch. It stages
each page with admission fencing, then verifies the complete commitment, required
frontier and selected-row membership using compact proof facts. Neither staging
path grants coverage. Only fully validated dependency groups enter normal storage through
the existing CAS installer; owner bundles and payload/locator pairs remain atomic.
Scratch is registered in format87, excluded from semantic snapshots and copied
repository epochs, and has durable ownership/expiry/reaping. Its migration from86
starts empty without changing authored rows or durable prepared attempts.

Oversized operations return an explicit bound error or a supported typed native
fallback. Lower-level bounded transfer primitives remain for writes and specialized
history/diff paths. Bounded fixed historical metadata diffs use read fulfillment
when both endpoints prove on the leased selected-branch first-parent lane. Captured
historical diffs retain specialized discovery because private pending client
history can be absent from the authority. Current sync32 peers share this
wire contract; old live peers are rejected.

## Profiling

`sync::partial_sql_tests::runtime_http::file_open_probe` contains an ignored
external-snapshot A/B profile and a generated public-API regression. The profile
compares both strategies in the same binary, with a fresh partial replica for
every cold sample and a subsequent warm read. Only the comparator removes the
captured operation recipe to exercise the previous pointer-discovery path.
Production has no such switch.

Run with a private snapshot and file ID supplied through environment variables:

```sh
LIX_PROBE_SNAPSHOT=/absolute/path/authority.lixsnap \
LIX_PROBE_FILE_ID=<file-uuid> \
cargo test -p lix --features server-protocol --lib \
  attached_snapshot_file_open_probe -- --ignored --nocapture
```

The harness uses the real protocol dispatcher and separately records HTTP
request count, SQL attempts, response bytes, server discovery calls/bytes,
server duration, and elapsed time. The optional 100 ms per-request delay is a
controlled RTT sensitivity experiment, not a measurement of production latency.
Snapshot contents, private paths, and credentials do not belong in published
profiling artifacts.

## Measured results

The [sanitized sample data](performance/partial-read-discovery-2026-09-21.json)
contains 24 reads: three cold/warm pairs for each strategy and each added delay.
Every cold read starts from a descriptor-only Memory replica and asserts exact
content equality with the authority. Restore and bootstrap are outside timing.

| Metric | Pointer discovery | Operation discovery |
| --- | ---: | ---: |
| Cold HTTP requests | 51 | 1 |
| SQL attempts | 47 | 2 |
| Cold median, no added delay | 75.151 ms | 27.239 ms |
| Cold median, 100 ms added per request | 5,345.896 ms | 130.996 ms |
| Response bytes | 261,386 | 291,658 |
| Warm HTTP requests | 0 | 0 |

Request counts and response bytes were identical across all cold samples for
each strategy. The original implementation reproduced 53 requests and 49 SQL
attempts. The shared exact-lookup corrections reduce the final pointer
comparator to 51 requests; operation discovery then reduces 51 to one, a 98.0%
reduction in the same binary. The original-to-final reduction is 53 to one.

This exchanges more authority-local work and 11.6% more response bytes for fewer
network rounds and SQL restarts. The operation response contains 71 inputs and
207,391 decoded payload bytes. Discovery performs 610 authority storage calls
and observes 597,128 storage bytes. With no added delay, median server handling
increases from 7.014 ms to 15.693 ms while total read time falls. Warm reads stay
local (medians approximately 1.7–1.9 ms).

These are native, unoptimized Linux measurements using the real HTTP protocol
dispatcher in-process, without TCP, browser/OPFS, or background watchers. The
100 ms experiment isolates sensitivity to serial request latency; it is not a
production UI timing claim. Large closures may require multiple bounded pages.

## Validation

The implementation passed 4,653 tests (86 skipped), including the base and
tracked-state rebuild simulations, RocksDB conformance, and both normal and
cached SlateDB conformance:

```sh
cargo nextest run -p lix -p lix-storage-rocksdb -p lix-storage-slatedb \
  --features lix/all-simulations,lix/server-protocol --test-threads 8 --no-fail-fast
```

All 10 engine doctests passed, and the browser SDK compiled for
`wasm32-unknown-unknown`:

```sh
cargo test -p lix -p lix-storage-rocksdb -p lix-storage-slatedb \
  --features lix/all-simulations,lix/server-protocol --doc
cargo check -p lix_js_sdk --target wasm32-unknown-unknown
```

Regressions cover one-request cold point reads with an explicit plugin registry,
warm reads without network, unrelated content exclusion, pending local edits,
pinned transactions across authority updates, ranged reads, multi-page blobs,
corruption and omission rejection, continuation integrity, lease validation,
bounded fixed historical metadata diffs, and rejection of out-of-scope or
unproved historical-diff recipes at this endpoint. Two GPT-5.6 Luna
reviews at extra-high reasoning examined discovery and installation independently;
their scope, owner-representation, and receipt findings were addressed.

CI follow-up also passes all 28 sync E2E tests (3 ignored), including offline
folder moves and checkpoint publication after ordinary read warmup.

Provider admission uses ordered point prefixes and bounded scan prefixes.
Missing point slots and duplicate slots retain their positions, and each
returned duplicate full value charges its bytes. Exact codec collectors may
assemble provider pages only within a separate explicit aggregate codec limit;
selected-row production consumes each page before advancing.

SlateDB stores atomic value-length metadata in a tagged physical generation.
Bounded mutable reads consult the corresponding snapshot/overlay length before
payload I/O, and bounded scans traverse metadata before loading admitted values.
The physical upgrade journals copied keys and the source sequence, retains the
original generation, and protects its immutable segments from current-generation
GC. Old binaries must be stopped for this physical upgrade; this is not a rolling
protocol compatibility path. Legacy SlateDB has no size-only read API, so the
upgrade can retain one unknown-size legacy value plus its bounded copy batch.
Its ordinary new-generation reads do not have that exception. Tagged keys allow
65,530 logical key bytes; an unrepresentable maximal legacy key causes a typed,
recoverable refusal before completion publication and leaves the original
physical repository intact. Native repository coordinates fit this limit.

Client scratch admission is a durable two-operation ledger. Each frame append
renews its operation lease in the same CAS publication as the frame bytes. An
owned heartbeat renews the lease during network waits and promotion. Cleanup
first marks the operation Reaping with a durable CAS, then removes frames in
bounded pages, then releases the ledger slot. A cleanup crash leaves a resumable
fence. Opening a partial replica holds its exclusive physical owner lease before
reclaiming the previous owner's unfinished operations, including same-epoch work
whose TTL has not elapsed. The periodic janitor reaps expired or old-epoch work
without taking ownership away from a successfully renewed operation.
