# Lix plugin API v1

The canonical, intentionally incompatible Component API for Lix plugins. It
uses fused guest transitions with host-owned sources and push sinks.

## Current contract

`wit/lix-plugin.wit` is the single canonical definition. The host owns
immutable `snapshot` inputs and the atomic `transition` output. A file
successor enters the guest once through `apply(request, borrow<transition>)`;
the guest emits bounded generic or typed pages through host imports while that
export remains entered. Semantic edits use `entities-changed` with a lazy
entity source and stream exact replacement bytes to the same transition.
Conflict resolution is stateless: `resolve-conflicts` reads canonical inputs
lazily and pushes ordered resolutions into a host-owned sink.

There are no guest-owned document, edit-cursor, change-cursor, or returned
transaction resources. A failed export, invalid page, or renderer failure
publishes none of the staged state, semantic changes, or file bytes.

The implementation was developed in phases:

- A: one exported call per transition on one persistent actor executor;
- B: bounded typed CSV row batches without guest JSON snapshots or UUIDs;
- C: storage-native batch consumption;
- D: `apply-cold-successor(durable-entities, new-source, sink)` plus bounded
  execution across independent files;
- E: checkpoint-plus-tail hydration for operations that truly need a retained
  document.

Warm `file-changed` no longer returns a guest-owned change cursor. The
document export accepts a borrowed host sink and emits every bounded packet
while that one export remains entered. A two-page producer/consumer channel
lets reconciliation validate pages concurrently instead of retaining the
complete pushed transition. Transaction atomicity is unchanged: nothing is
published unless the export, complete drain, and subsequent commit all succeed.

Cold-successor is not ordinary hydration. A cache miss during replay must not
rebuild and render the predecessor merely to parse the successor. Warm apply,
cold successor, and explicit hydration are separate v1 operations so the host
cannot accidentally reintroduce that work through an adapter.

Measured on the 10.68 MiB / 220,001-entity CSV fixture:

| lane | p50 | peak live host allocation |
| --- | ---: | ---: |
| returned-cursor prototype | 5.169 s | 929.7 MB |
| A, fused packet-v1 | 5.042 s | 929.7 MB |
| B, typed CSV batch | 4.576 s | 927.8 MB |
| B + certified import/storage-retention cuts | 2.325 s | 448.4 MB |
| C, deferred storage-native lowering | 2.225 s | 282.4 MB |
| C + streamed sink + one-shot cold CSV view | 2.066 s | 282.4 MB |

The final measured lane is 2.50x faster and uses 3.29x less peak live host
allocation than the returned-cursor prototype. It retains the compact prepared row owner through
validation and expands hot row, file, and working-diff keys in 4,096-row pages
only while lowering the atomic backend transaction. Exact file bytes, all
220,000 CSV rows, the active checkpoint baseline and coverage proof, and
INSERT absence validation remain unchanged.

The history-replay lane has a different multiplier. With only 16 cached actors,
cache misses can materialize and render the predecessor before parsing the
successor. v1 treats `apply-cold-successor(durable-entities, new-source, sink)`
as a first-class operation rather than implementing it as `hydrate` followed
by warm `apply`. The cache is an acceleration layer; correctness and replay
complexity do not depend on keeping every file actor resident. Durable entities
must be exposed as a bounded cursor and consumed inside the single successor
guest call; passing a fully materialized `Vec<Entity>` would merely move the
same amplification to the API boundary.

## Warm transition push-sink benchmark

A matched 25-pair release benchmark alternated lane order while toggling one
byte in the same 980,000-byte / 20,000-row CSV document. Both lanes preserved
exact bytes and semantic row count and committed exactly one durable semantic
change per sample.

| lane | p50 | p95 | guest transition exports | peak live host allocation |
| --- | ---: | ---: | ---: | ---: |
| returned-cursor prototype | 1.939 ms | 3.524 ms | 3 | 1.090 MB |
| v1 imported push sink | 1.343 ms | 1.558 ms | 1 | 1.090 MB |

The push path is 1.44x faster at p50. The completed storage-native path
validates host-owned pages into transaction batches and avoids a complete
generic entity vector between the Component boundary and storage.

## Large JSON and remaining boundaries

The JSON lane deliberately kept the same parser and packet-v1 snapshots during
the A/B measurement. A seven-sample 10 MiB / 39,871-entity RocksDB import
therefore isolated the API/runtime change:

| lane | p50 | p95 | guest exports | host imports | peak live host allocation |
| --- | ---: | ---: | ---: | ---: | ---: |
| returned-cursor prototype | 675.7 ms | 767.0 ms | 17 | 10 | 85.7 MB |
| v1 push sink | 600.4 ms | 652.1 ms | 1 | 9 | 85.6 MB |

Bulk JSON is 1.13x faster at p50. The fused call plus 2 MiB bounded pages
replace sixteen `cursor.next` exports with eight sink imports.

The source side now returns any ABI-addressable complete file in one bounded
host import instead of copying eleven 1 MiB results into a second guest vector.
Together with the bounded producer/consumer sink, this changes v1 import-call
counts from 26 to 9 for JSON and from 29 to 11 for CSV. Seven post-cut CSV
samples have a 2.066 s p50 and 282.4 MB peak live host allocation.

Large CSV opens now use a one-shot scan view instead of building a persistent
guest document that the engine has already decided not to cache. Guest
linear-memory high water fell from 53.0 MB to 41.2 MB (22%), but end-to-end
host peak stayed flat. The remaining peak is the complete host transaction
representation, not retained guest state or sink pages.

| phase | 10 MiB JSON | 10.68 MiB CSV |
| --- | ---: | ---: |
| plugin drain complete | 63.0 MB | 198.4 MB |
| tracked-head publication | 94.1 MB | 235.0 MB |
| end-to-end peak delta | 85.6 MB | 282.4 MB |

Deferring packet decoding did not change end-to-end peak. The next cut cannot
be another Component cursor optimization: the sink must validate into a
transaction-native, page-backed owner and storage preparation must consume
that owner without first constructing the complete
`ValidatedFileTransition.changes` vector.

Releasing shared canonical snapshot owners page-by-page after RocksDB encoding
was also tested and removed: peak remained 282.4 MB while CSV p50 regressed
from 2.127 s to 2.253 s. The remaining cut must remove the row representation
itself rather than clearing payload owners that are already shared.
