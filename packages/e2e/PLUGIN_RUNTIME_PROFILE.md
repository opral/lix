# Cross-format Wasm plugin runtime profile

Profiled on Linux x86-64 with Rust nightly 1.97.0. The release benchmarks were
verified against `origin/main` at `c082f5f14`. The exact VS Code Docs replay
uses commit `d5badf95f8ab16c4deb91199dc696f2293d93554`.

## Conflict-resolution scaling

The Component-v2 resolver has two materially different format paths. JSON and
Git text use the API's canonical `b`-or-delete default without reading snapshot
bytes. CSV reads each conflicting row and may compose disjoint cell edits;
Markdown does the same for disjoint inline spans. CSV and Markdown therefore
have an information-theoretic lower bound of one decision per semantic
conflict. Returning an exact aligned result for every arbitrary conflict is
also Ω(conflicts), so sublinear total resolution is not a valid correctness
target.

The avoidable scaling was in host routing around that required resolver pass.
For a successful all-plugin merge, the old path scanned the complete conflict
batch four times: derived-file discovery, resolver eligibility, unresolved-row
rejection, and resolver input construction. It also loaded the same base,
target, and source plugin registries twice and reconstructed the same common
file descriptors twice. The merge now builds one file-to-conflict index while
discovering derived files, retains the pinned plugin generation and descriptor,
and routes later stages by eligible indices. The successful path is one O(N)
classification pass plus O(K) resolver preparation for K plugin conflicts;
the unresolved-error path alone rescans N rows to construct public details.
Point membership remains O(log K), and the plugin is still invoked once per
file rather than once per conflict.

## Results

| Workload | Time | Host allocation | Peak live host allocation | Process peak RSS |
| --- | ---: | ---: | ---: | ---: |
| JSON, 10 MiB / 39,871 rows, plugin import p50 | 1,099.9 ms | 696.6 MB | 186.6 MB | 486.6 MB |
| JSON, same file-scoped rows without Wasm p50 | 1,003.2 ms | 872.8 MB | 223.3 MB | — |
| JSON, same rows without file ownership p50 | 530.2 ms | 591.7 MB | 115.3 MB | — |
| JSON, warm sparse byte edit p50 | 6.47 ms | 11.0 MB | 10.6 MB | 348.2 MB |
| CSV v2, 10.68 MiB / 220,001 rows, plugin import p50 | 5,168.8 ms | 3.47 GB | 929.7 MB | 1.71 GB |
| CSV v3 C, same fixture and output | 2,225.2 ms | 1.70 GB | 282.4 MB | — |
| CSV, same file-scoped rows without Wasm p50 | 4,201.2 ms | 3.85 GB | 995.9 MB | — |
| v3 A, fused packet-v1 CSV import p50 | 5,041.6 ms | 3.47 GB | 929.7 MB | 1.62 GB |
| v3 B, typed CSV batch import p50 | 4,576.1 ms | 3.52 GB | 927.8 MB | 1.55 GB |
| v3 B + certified import/storage-retention cuts, p50 | 2,325.2 ms | 1.73 GB | 448.4 MB | — |
| CSV warm edit, v2 returned cursor, p50 | 1.909 ms | 1.433 MB | 1.090 MB | — |
| CSV warm edit, v3 push sink, p50 | 1.217 ms | 1.433 MB | 1.090 MB | — |
| Markdown, VS Code Docs failing transition | 8,700.5 ms execute | — | — | 97.63 MiB guest |

The JSON plugin is only 1.096x slower than inserting the same file-scoped
semantic rows directly. CSV is 1.24x slower. Bulk imports therefore cannot
become 2-5x faster through parser or Wasm tuning alone: the current
file-scoped semantic storage path is already the dominant floor.

The JSON no-file control is 1.89x faster than the file-scoped control. This
isolates a large cost in ownership validation, tracked-state materialization,
and per-row transaction staging.

The CSV import allocates 3.47 GB on the host for 10.68 MB of input and reaches
929.7 MB of live host allocation. Direct insertion is even larger. This is a
storage/materialization problem, not a Wasm 128 MiB guest-memory problem.

## v3 prototype results

The prototypes use the same RocksDB import, schemas, ownership rules,
transaction validation, atomic commit, exact source-byte assertion, and exact
220,001 durable-change assertion as v2.

| Candidate | p50 | Speedup vs matched v2 | Boundary bytes | Peak live allocation |
| --- | ---: | ---: | ---: | ---: |
| matched v2 packet-v1 | 5,168.8 ms | 1.00x | 39.4 MB | 929.7 MB |
| A: one fused call, fixed actor thread | 5,041.6 ms | 1.025x | 39.4 MB | 929.7 MB |
| B: fused typed CSV batches | 4,576.1 ms | 1.130x | 28.2 MB | 927.8 MB |
| B + certified import/storage-retention cuts | 2,325.2 ms | 2.223x | 31.4 MB | 448.4 MB |
| C: deferred storage-native lowering | 2,225.2 ms | 2.323x | 31.4 MB | 282.4 MB |

Prototype A enters the guest once and creates one persistent executor thread.
It therefore passes the structural scheduling gates but fails the performance
gate: removing re-entry is not material for a 220k-row transaction once
reconciliation and storage dominate.

Prototype B emits bounded batches of row ordinals, order ranks, decoded cells,
and lexical layout. Wasm creates neither row UUID strings nor row JSON
snapshots. Boundary traffic falls 28.5%, but the host immediately rebuilds
220k canonical row snapshots for the unchanged v2 transaction path.
Cumulative host allocation therefore does not fall and peak live allocation
moves by only 0.2%. This is direct evidence that a storage-native sink is
required; typed transport alone merely moves materialization across the Wasm
boundary.

For a warm one-byte CSV transition, replacing the returned guest change cursor
with an imported host push sink reduced three guest exports
(`file-changed`, `next(Some)`, `next(None)`) to one. Across 25 paired,
alternating-order RocksDB samples, p50 improved from 1.909 ms to 1.217 ms
(1.57x) and p95 from 2.539 ms to 1.466 ms (1.73x). Exact bytes, 20,000
semantic rows, and one durable change per sample matched. Peak and cumulative
host allocation were flat, confirming that cursor removal is a control-flow
win; memory falls only when the host sink lowers pages directly instead of
retaining an adapter vector for generic reconciliation.

The follow-on prototype certifies the exact fresh-plugin import shape, avoids
the generic O(rows) transaction validation index, removes retained physical
RocksDB point keys on the final write lane, bounds prepared dictionaries, and
evicts large completed v3 import actors before materialization. Five samples
were 2,276.4, 2,317.2, 2,325.2, 2,336.5, and 2,770.1 ms. This clears the 2x
time gate without weakening exact-byte, exact-row, ownership, validation, or
atomicity checks.

It does not clear the 3x memory gate. Allocation readings at phase close were:

| Materialization boundary | Live Rust allocation |
| --- | ---: |
| changelog staged | 161.6 MB |
| tracked roots staged | 233.0 MB |
| hot deltas and absence guards built | 270.8 MB |
| expanded hot identities built | 335.9 MB |
| hot values encoded | 457.2 MB |

The measured scope starts with roughly 8.8 MB live, yielding the reported
448.4 MB peak delta. Prototype C now retains one compact semantic owner and
lowers 4,096-row chunks into the atomic backend transaction, so expanded
storage identities and encoded current-state values exist for only one page.
Five samples were 2,181.2, 2,199.7, 2,225.2, 2,237.2, and 2,556.2 ms. Peak
live host allocation was 282.4 MB. Prototype C therefore clears both hard
gates: 2.32x faster and 3.29x lower peak allocation than v2. The deferred
encoder also preserves the active checkpoint's `BeforeAbsent` baseline,
sparse dirty-key index and coverage proof, plus the validated unscoped
descriptor INSERT.

## Component boundary profile

The VS Code Docs Markdown transition has:

| Counter | Value |
| --- | ---: |
| Source bytes read | 3,273,092 |
| Component-boundary bytes | 27,832,278 |
| Packet records | 25,656 |
| Semantic rows materialized | 25,362 |
| Durable semantic changes | 294 |
| Full document reparses/renders | 151 / 151 |
| Guest high-water memory | 102,367,232 bytes |

That is 8.5x byte amplification and 86x row amplification relative to the
durable changes.

A 999 Hz sampled profile recorded about 51,000 short-lived `lix` threads.
`call_sync_guest` currently spawns and joins an OS thread for every synchronous
component method. The dominant sampled leaf was Linux `clone3`, followed by
mmap/mprotect/munmap and Wasmtime per-thread signal initialization. This cost
is shared by Markdown, CSV, JSON, and Excalidraw.

A deliberately unsafe direct-call prototype retained Wasm but removed the
per-method thread:

| 17-document control | Current | Direct-call prototype | Speedup |
| --- | ---: | ---: | ---: |
| CSV | 30.26 ms | 17.29 ms | 1.75x |
| Text | 21.71 ms | 12.27 ms | 1.77x |
| Excalidraw | 23.49 ms | 14.42 ms | 1.63x |
| Voluntary context switches | 295 | 43 | 6.9x fewer |

The direct call is not shippable: it blocks a current-thread Tokio runtime, and
serializing a 1,184-document Markdown batch regressed 922 ms to 1,723 ms.
The result proves the overhead but also shows that the replacement must retain
bounded parallelism.

## Recommended hard cut: plugin API v3

No backward compatibility should be carried on the hot path.

1. Replace the resource-method conversation with one bulk transition call per
   file operation. Pass source edits, the required semantic projection, and
   transition context in one typed binary request; return one typed change
   batch plus a materialization proof.
2. Execute component calls on a fixed actor executor pool, or use a genuinely
   async Wasmtime component boundary. Never create an OS thread per WIT method.
3. Replace packet-v1 row records and JSON-in-JSON snapshots with a typed binary
   batch: dictionary-coded schema/file IDs, fixed-width local refs, column
   vectors, and offset/data buffers for variable fields.
4. Add a compact, content-addressed per-file actor checkpoint. Cold hydration
   should read one checkpoint plus changes after its root, rather than
   materializing every semantic row and replaying them through packet cursors.
5. Add a storage-native file row batch. Validate ownership once per batch,
   sort once, lower once, and persist packed column buffers/ranges rather than
   independently staging and validating every semantic row.
6. Preserve the existing sparse edit path. The 10 MiB JSON edit is already
   6.47 ms; optimize its full-buffer allocation separately with borrowed input
   ranges or host attachments.

### Cold-successor is distinct from checkpoint hydration

The full-history profile exposes a more important cache-miss operation than
ordinary hydration. A 16-actor cache can repeatedly evict files in a wide
history. Reconstructing and rendering the previous document before parsing its
successor turns durable semantic rows into transient plugin state even though
the caller only needs the successor.

API v3 should expose three different operations:

1. `apply-warm(document, edit, source, sink)` for an actor already in memory;
2. `apply-cold-successor(durable-rows, new-source, sink)` for an evicted
   actor, producing the successor directly without rendering the predecessor;
3. `hydrate(durable-rows)` only when a retained document is actually
   needed for a later read or semantic edit.

The scheduler should route cache misses during replay to cold-successor and run
independent files through a bounded, memory-budgeted worker pool. Checkpoints
then optimize the remaining true hydration operations as `checkpoint + tail`;
they are not a substitute for removing predecessor reconstruction from the
successor path.

On the measured wide Markdown replay, only 16 actors are resident while
5.29 million semantic rows are reconstructed for 414,928 durable changes:
12.7x row amplification. This changes the expected v3 replay model from
“optimize hydration” to “avoid hydration on the successor path.” Parallelism
alone remains bounded by the serial work and increases concurrent actor RSS;
cold-successor removes the work before bounded parallel scheduling is applied.

## Expected ceiling

| Cut | Target workload | Expected result |
| --- | --- | --- |
| Fused transition + fixed executor | Small files and many-file commits | 1.5-2x |
| Packed row batch + batch ownership validation | Bulk CSV/JSON imports | 2-3x, much lower host allocation |
| Actor checkpoints | Cold edits and histories wider than the 16-actor cache | 2-5x and lower boundary traffic |
| Parser arenas/local IDs | Format-specific memory peaks | Useful where copies remain, but not the shared multiplier |

The 2-5x target is credible only as an API/runtime/storage redesign. For bulk
imports, the Wasm plugin layer itself is currently just 9.6% of JSON time and
19.7% of CSV time relative to their direct file-scoped controls.

## Large CSV/JSON v3 push profile

JSON v3 reuses the JSON v2 parser and packet-v1 snapshots. Seven release
samples on the 10 MiB / 39,871-row flat-object import produced:

| lane | p50 | p95 | exports | imports | peak host allocation |
| --- | ---: | ---: | ---: | ---: | ---: |
| JSON v2 cursor | 675.7 ms | 767.0 ms | 17 | 10 | 85.7 MB |
| JSON v3 push | 600.4 ms | 652.1 ms | 1 | 9 | 85.6 MB |

The final bulk JSON lane is 1.13x faster at p50 and 1.18x faster at p95.
One fused export and eight 2 MiB sink pages replace seventeen v2 guest exports.

Three follow-up cuts were profiled. v3 sources return a complete ABI-addressable
input from one import, avoiding ten extra imports and a guest-side
chunk-assembly copy. A two-page producer/consumer channel drains sink pages
while the sole guest export is still entered. Finally, a large CSV open uses a
one-shot scan view and never builds a persistent document/index that the
engine's large-import policy would immediately evict.

Import calls fell from 26 to 9 for JSON and from 29 to 11 for CSV. Seven
10.68 MiB CSV samples had a 2.066 s p50, 2.50x faster than the matched
5.169 s v2 lane. Peak remained 282.4 MB, 3.29x below v2, and JSON remained
85.6 MB.

| phase | JSON | CSV |
| --- | ---: | ---: |
| plugin drain complete | 63.0 MB | 198.4 MB |
| tracked-head publication | 94.1 MB | 235.0 MB |
| measured peak delta | 85.6 MB | 282.4 MB |

CSV guest linear-memory high water fell from 53.0 MB to 41.2 MB (22%) after
removing the unused persistent cold-import document. The unchanged host peak
shows that the remaining peak is downstream of the Component boundary. A real next step
must replace the complete `ValidatedFileTransition.changes` owner with a
transaction-native page-backed batch, preserve transition-wide uniqueness and
rollback metadata separately, and let storage lowering consume those pages
without a `Vec<RowChange>` materialization.

A follow-up experiment made the deferred RocksDB encoder the unique owner of
the prepared batch and released canonical snapshot owners after each 4,096-row
page was encoded. It did not change peak (282.4 MB) and regressed p50 from
2.127 s to 2.253 s, so it was removed. This rules out retained snapshot arenas
as the remaining peak and keeps the next cut focused on eliminating the
complete generic/prepared row representation, not incrementally clearing its
already-shared payloads.

## Immutable row-batch segment result

The next prototype removed that generic row representation. A v3 transition
may now hand the transaction a bounded, host-certified encoded batch. The
engine validates its schema membership, packet framing, primary keys, and
canonical snapshots before commit. Current-state queries decode the immutable
segment lazily; RocksDB receives one batch owner rather than one hot row,
history segment, and locator per semantic row.

Five release samples after this cut:

| workload | old lane p50 | segment p50 | segment p95 | peak live allocation | cumulative allocation |
| --- | ---: | ---: | ---: | ---: | ---: |
| CSV, 10.68 MiB / 220,000 rows | 5.227 s | 129.4 ms | 171.8 ms | 36.1 MB | 65.5 MB |
| JSON, 10 MiB / 39,871 rows | 625.6 ms | 258.0 ms | 273.5 ms | 45.5 MB | 103.4 MB |

CSV is 40.4x faster than the original storage-materializing lane and uses
about 26x less peak live host allocation than its approximately 930 MB
baseline. JSON is 2.43x faster than the current v2 cursor and uses 47% less
peak live host allocation. CSV performs about 33,400 host allocations instead
of approximately 4.04 million.

Both gates use durable RocksDB writes and verify exact file bytes, exact
semantic cardinality, an exact projected semantic row, and close/reopen query
persistence. JSON additionally retains the engine's streaming schema and
primary-key validation rather than trusting the Wasm guest's certificate.

This establishes that fused control flow alone is not the bulk multiplier.
The large win comes from preserving one encoded semantic owner through plugin
transport, validation, transaction staging, storage, and query consumption.
Remaining production gates are historical/time-travel visibility,
cold-successor operation after actor eviction, and equivalent codecs for
Markdown and Excalidraw.

## Cross-format successor profile

The Markdown and Excalidraw v3 adapters use the same fused export and bounded
packet sink as JSON. Complete opening state is one certified immutable batch;
incremental changes are ordinary sparse row overlays. Correctness tests cover
exact bytes, semantic projection, history, and RocksDB close/reopen for all
three formats.

The exact 1,237,840-byte VS Code API Markdown transition at
`d5badf95f8ab16c4deb91199dc696f2293d93554` changes two of 3,808 semantic
rows:

| lane | p50 | p95 | host allocation | peak host allocation | exports / imports | guest high water |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Markdown v2 cursor | 883.3 ms | 947.1 ms | 32.2 MB | 11.2 MB | 3 / 0 | 102.4 MB |
| Markdown v3 push, full parse | 912.0 ms | 951.1 ms | 136.2 MB | 21.0 MB | 1 / 1 | 102.4 MB |
| Markdown v3 push, local subtree | 42.8 ms | 60.8 ms | 136.2 MB | 21.0 MB | 1 / 1 | 56.8 MB |

The fused transition alone therefore failed Prototype A's acceptance gate.
The exact repository transition changes only `DateApproved` inside the first
frontmatter block: four source bytes become three while 1,237,730 trailing
bytes remain identical. The paragraph-only fast path rejected that edit and
the document's exact-byte lexical fallback, forcing a full parse and stable
render of the entire file.

The persistent Markdown tree now stores sparse complete top-level subtree
overrides rather than node-only overrides. A single edit fully contained in
one block parses and reconciles only that block, retains the untouched base
tree, shifts later source ranges, and updates the exact-byte fallback directly
from the piece table. Edits crossing block or line boundaries still take the
full parser. This makes the measured transition 21.3x faster and reduces guest
high water by 45.6 MB without changing its two durable semantic changes.

A read-facade cache experiment had made the old full-parse path worse—1,225.7
ms, 710.3 MB cumulative allocation, and 44.7 MB peak—because the cache owner
was shorter lived than the validation sequence and repeatedly hydrated the
full segment. It remains removed.

A generated 1.849 MB Excalidraw document with 20,000 elements changes one
element. The current sparse arena implementation stores source-span metadata
once, binary-searches that index through bounded state reads, reads only the
selected element, and emits one semantic overlay:

| lane | p50 | host allocation | peak host allocation | exports / imports | guest high water |
| --- | ---: | ---: | ---: | ---: | ---: |
| Excalidraw v2 cursor | 233.4 ms | 4.187 MB | 2.957 MB | 3 / 0 | 42.140 MB |
| Excalidraw v3 push | 3.482 ms | 4.209 MB | 2.959 MB | 1 / 23 | 25.166 MB |

The seven-sample v3 result is **67.0x faster** while cumulative and peak host
allocation stay within one percent of v2. The 23 imports are small range reads
for the persistent span index and selected element; total boundary payload is
about 1 KiB. The benchmark now fails unless v3 remains at least ten percent
faster than v2 and both cumulative and peak host allocation remain within five
percent. The earlier 217.5 ms result predated direct cold arena hydration and
the sparse span index: it rebuilt the predecessor and is no longer
representative of the current runtime.

The final Markdown cut also stopped storing the complete accepted source as a
base64 semantic-root fallback. Exact bytes already live in the file arena, so
the sparse successor now reads only the edited top-level block and emits one
semantic overlay. Seven paired samples measured 33.302 ms for v2 and 5.702 ms
for v3 (5.84x), with peak host allocation falling from 11.247 MB to 3.756 MB
and Component boundary traffic falling from 1.651 MB to roughly 3 KiB.

Bulk JSON had the same smaller-scale duplicate-materialization pattern. Its
packet encoder allocated and copied one temporary vector per row, while
the sparse arena index generated a complete scalar snapshot, parsed that JSON
back into a value tree to remove `scalar_json`, and serialized it again.
Encoding directly into the bounded sink page and serializing arena metadata
directly reduced the five-sample v3 p50 from 459.9 ms to 380.5 ms (17.3%).
The next profile showed that every node still constructed a temporary
`serde_json::Map` and `Value` tree solely to produce its canonical semantic
snapshot. Writing those already-known fields directly in canonical key order
reduced v3 again from 380.5 ms to 320.7 ms (15.7%). The paired v2 p50 was
590.8 ms, so v3 is 1.84x faster with 115.0 MB cumulative and 63.2 MB peak host
allocation versus v2's 336.3 MB and 85.7 MB. The direct encoder preserves
serde_json string escaping and the existing snapshot bytes; all 23 JSON
correctness tests and the benchmark's exact-byte, semantic, history, and
RocksDB-reopen checks pass.

The scalar checkpoint then proved to be another redundant representation: its
metadata embedded a JSON snapshot repeating the relation, identity, order, and
layout fields already present in the same record. Replacing that snapshot with
a compact tagged binary record reduced v3 from 320.7 ms to 264.6 ms (17.5%),
peak host allocation from 63.2 MB to 55.2 MB, guest high water from 44.2 MB to
35.3 MB, and boundary traffic from 33.85 MB to 29.90 MB. The paired v2 median
was 641.3 ms, making v3 2.42x faster. Removing the checkpoint entirely measured
258.6 ms, so only about 2.3% remains in that subsystem. A borrowed
snapshot-direct-to-packet experiment regressed the median to 281.3 ms with
flat memory and was removed rather than expanding the API.

The same 10 MiB JSON fixture changes one scalar in 6.779 ms warm versus
8.222 ms for v2 (1.21x). A process-cold reopen plus successor is 216.1 ms
versus 533.6 ms (2.47x), hydrates zero semantic rows, and invokes zero
predecessor renders.

Current release verification after the sparse-overlay, direct cold-arena, and
non-duplicated-source policies:

| workload | v3 p50 | v3 p95 | peak host allocation | guest high water |
| --- | ---: | ---: | ---: | ---: |
| CSV, 10.68 MiB / 220,001 rows | 137.8 ms | 418.2 ms | 34.8 MB | 41.2 MB |
| JSON, 10 MiB / 39,871 rows | 264.6 ms | 281.4 ms | 55.2 MB | 35.3 MB |
| Markdown, 1.24 MiB / 3,808 rows | 5.702 ms | 5.789 ms | 3.756 MB | 53.477 MB |
| Excalidraw, 1.85 MiB / 20,000 rows | 3.482 ms | 3.659 ms | 2.959 MB | 25.166 MB |

All four lanes remain Wasm components. They use one guest export and bounded
push pages, preserve exact file bytes and semantic cardinality, and retain the
format-specific history and RocksDB reopen checks. CSV creates no per-row
history segments or locator records before hot publication.

The subsequent hard API cut added the borrowed atomic transition,
host-imported conflict-resolution sink, lazy conflict/row sources, and the
fused semantic renderer. Paired release verification on the `origin/main`
tracked-head/protocol changes merged in #976 shows that this control-flow cut
retains the optimized paths:

| workload | v2 p50 | hard-cut v3 p50 | speedup | v3 peak host allocation |
| --- | ---: | ---: | ---: | ---: |
| JSON, 10 MiB / 39,871 rows | 618.340 ms | 287.018 ms | 2.15x | 55.294 MB |
| Markdown, exact VS Code API transition | 34.090 ms | 6.236 ms | 5.47x | 3.756 MB |
| Excalidraw, 20,000 elements | 243.150 ms | 3.507 ms | 69.33x | 2.959 MB |

JSON v3 allocated 107.3 MB cumulatively versus 352.4 MB for v2 and performed
79,061 allocations versus 485,051. Markdown v3 allocated 21.2 MB versus
29.5 MB and reduced peak live host allocation by 66.6%. Excalidraw reduced
guest linear-memory high water from 42.140 MB to 25.166 MB. Every v3 lane used
one top-level guest export and preserved exact output bytes and semantic rows.

The hard-cut CSV lane remains in the same performance band at 146.824 ms p50
with 35.024 MB peak live host allocation; its prior matched post-#976 result
was 141.625 ms. This 3.7% movement is below the ten-percent reprofile gate.

## Reproduction

```sh
cargo test --release -p lix_e2e --test e2e --no-run

E2E_BIN="$(find target/release/deps -maxdepth 1 -type f -executable \
  -name 'e2e-*' -print -quit)"

"$E2E_BIN" --ignored --exact \
  v2_json_ten_mib_rocksdb_import_parity_benchmark \
  --nocapture --test-threads=1

"$E2E_BIN" --ignored --exact \
  v2_csv_ten_mib_rocksdb_import_parity_benchmark \
  --nocapture --test-threads=1

"$E2E_BIN" --ignored --exact \
  v2_json_ten_mib_ordinary_sql_byte_edit_benchmark \
  --nocapture --test-threads=1
```
