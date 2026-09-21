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
physical locator record. Internal plugin-registry and file-owner reads belong
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
closure. Pages carry up to 4 MiB of payload, except one native input may be as
large as 64 MiB. A continuation identifies the next input and closure digest.
The authority re-evaluates the pinned recipe for each page; deterministic input
ordering and the closure digest reject changed or incomplete pagination. This
keeps server state bounded without a cursor registry, at the cost of repeating
local discovery work for large closures.

The client assembles and verifies the bounded complete closure before publishing
it. Oversized operations return an explicit bound error rather than silently
falling back to per-pointer RPCs. Existing lower-level transfer primitives remain
for writes and specialized history/diff paths. Captured historical diffs stay
on that path because their endpoints can belong to private pending client
history that the authority cannot evaluate. Protocol 19 peers are rejected; there is no compatibility fallback.

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
| Cold median, no added delay | 73.225 ms | 26.190 ms |
| Cold median, 100 ms added per request | 5,328.101 ms | 130.794 ms |
| Response bytes | 261,386 | 290,685 |
| Warm HTTP requests | 0 | 0 |

Request counts and response bytes were identical across all cold samples for
each strategy. The original implementation reproduced 53 requests and 49 SQL
attempts. The shared exact-lookup corrections reduce the final pointer
comparator to 51 requests; operation discovery then reduces 51 to one, a 98.0%
reduction in the same binary. The original-to-final reduction is 53 to one.

This exchanges more authority-local work and 11.2% more response bytes for fewer
network rounds and SQL restarts. The operation response contains 69 inputs and
207,095 decoded payload bytes. Discovery performs 603 authority storage calls
and observes 593,909 storage bytes. With no added delay, median server handling
increases from 6.877 ms to 15.065 ms while total read time falls. Warm reads stay
local (medians approximately 1.7–1.8 ms).

These are native, unoptimized Linux measurements using the real HTTP protocol
dispatcher in-process, without TCP, browser/OPFS, or background watchers. The
100 ms experiment isolates sensitivity to serial request latency; it is not a
production UI timing claim. Large closures may require multiple bounded pages.

## Validation

The implementation passed 4,586 tests (86 skipped), including the base and
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
and rejection of historical-diff recipes at this endpoint. Two GPT-5.6 Luna
reviews at extra-high reasoning examined discovery and installation independently;
their scope, owner-representation, and receipt findings were addressed.
