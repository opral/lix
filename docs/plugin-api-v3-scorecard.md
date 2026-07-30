# Plugin API v3.2 arena scorecard

Measured on 2026-07-30 from `origin/main` at `57d619aad`. The final v2 and v3
release controls were rerun on the same local Linux x86-64 machine and build.
Results are intended for paired relative design guidance, not as stable
cross-machine latency thresholds.

## v2 production baselines

These use the production Wasmtime Component runtime and real format plugins.

| workload | input | transition | elapsed | guest high water | boundary |
| --- | ---: | --- | ---: | ---: | ---: |
| CSV, 220,000 rows | 10,680,000 B | initial import, 20 samples | 926.191 ms p50 / 1,021.834 ms p95 | 53,018,624 B guest / 63,698,624 B total | 39,400,111 B |
| CSV, 220,000 rows | 10,680,000 B | cold restore | 0.86 s combined test | 61,997,056 B | paged entities/render |
| CSV, 220,000 rows | 10,680,000 B | one-row warm edit after restore | included above | 61,997,056 B | one row |
| JSON, 39,870 properties | 10,485,760 B | initial import | 1,754.089 ms | 26,673,152 B | full source |
| JSON, one scalar | 10,485,760 B | verified warm edit | 5.928 ms engine | 26,673,152 B | 418 B |
| JSON, one scalar | 10,485,760 B | ordinary public-SQL warm edit, 7 samples | 6.724 ms p50 / 8.602 ms p95 | 26,673,152 B control | full-byte request, sparse plugin update |
| Markdown, `vscode-docs` `d5badf` | 3,276,550 changed B | one-commit replay | 8.95–9.22 s | 102,367,232 B | see format profile |
| Excalidraw, 42,000 elements | 12,505,008 B | one middle-element scalar edit | 916.267 ms p50 / 955.150 ms p95 | 200,867,840 B | 512 B |

The JSON hot path is the important control: v2 already crosses only 418 bytes
and does not reparse or rerender the document. A v3 implementation must reduce
total ownership while completing the verified-splice transition in at most
2.964 ms. The independently sampled ordinary public-SQL path has an 8.602 ms
p95, making its v3 target 4.301 ms. Its median hot transition allocated
10,996,407 bytes and reached a 10,595,159-byte peak live allocation delta.
Counting only the 10,485,760-byte accepted host blob plus the measured
26,673,152-byte guest high water gives a conservative v2 total of 37,158,912
bytes, so the v3 peak must be at most 12,386,304 bytes.

Reproduction:

```sh
cargo test -p lix_sdk --lib \
  csv_v2_ten_mib_cold_import_benchmark \
  --release -- --ignored --nocapture

cargo test -p lix_sdk --lib \
  csv_v2_ten_mib_warm_edit_benchmark \
  --release -- --ignored --nocapture

cargo test -p lix_sdk_tests --test e2e \
  v2_json_ten_mib_real_wasm_edit_stays_sparse_and_bounded \
  --release -- --ignored --nocapture

cargo test -p lix_sdk_tests --test e2e \
  v2_json_ten_mib_ordinary_sql_byte_edit_benchmark \
  --release -- --ignored --nocapture

LIX_MARKDOWN_BENCH_REPO=/root/projects/vscode-api-repro \
  cargo test -p lix_sdk --lib \
  markdown_v2_vscode_api_real_history_benchmark \
  --release -- --ignored --nocapture

cargo test -p lix_sdk --lib \
  excalidraw_v2_ten_mib_sparse_edit_benchmark \
  --release -- --ignored --nocapture
```

## v3 arena ownership model

The deterministic scorecard applies one one-byte edit, one complete entity
replacement, and one opaque state-page replacement to unique-content fixtures.
The v2 retained estimate counts only two exact accepted byte owners plus the
changed values; it deliberately excludes allocator and parsed-index overhead.
The v3 number is the actual unique content-page storage after both roots.

| format fixture | bytes | conservative v2 retained | v3 unique pages | reduction | modeled v3 boundary |
| --- | ---: | ---: | ---: | ---: | ---: |
| Markdown | 8,388,660 | 16,777,408 | 8,388,749 | 50.0% | 82 B |
| CSV | 10,680,040 | 21,360,158 | 10,680,119 | 50.0% | 72 B |
| JSON | 8,388,669 | 16,777,418 | 8,388,750 | 50.0% | 74 B |
| Excalidraw | 8,388,673 | 16,777,438 | 8,388,766 | 50.0% | 86 B |

The host rope operation itself does not introduce a latency concern. This
release run used 100 warmups and 5,000 measured edits per format, alternated the
v2/v3 measurement order, and reports per-operation distributions:

| format | whole-`Vec` p50 / p95 | arena p50 / p95 | p95 speedup | retained reduction | gate |
| --- | ---: | ---: | ---: | ---: | --- |
| Markdown | 162.550 / 181.749 µs | 32.470 / 40.460 µs | 4.492× | 2.000× | fail memory |
| CSV | 235.589 / 275.049 µs | 41.500 / 53.599 µs | 5.132× | 2.000× | fail memory |
| JSON | 160.260 / 177.839 µs | 32.779 / 40.240 µs | 4.419× | 2.000× | fail memory |
| Excalidraw | 161.040 / 177.889 µs | 32.710 / 40.530 µs | 4.389× | 2.000× | fail memory |

## Real v3 Component runtime control

The arena is now bound through the actual Wasmtime Component Model and v3 WIT.
A minimal guest with the same 10,485,760-byte input size performs 100 warmups
and 2,000 measured one-byte transitions. Each transition crosses the exported
guest function, reads the verified prospective transaction, drains a guest
change cursor to permanent EOF, applies the complete entity snapshot, commits
opaque state and bytes atomically, and reports the Wasmtime linear-memory high
water.

| input | v3 p50 | v3 p95 | host unique arena | guest high water | total owned | reduction vs v2 JSON total | boundary | file bytes read |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,485,760 B | 120.420 µs | 133.490 µs | 10,485,789 B | 1,179,648 B | 11,665,437 B | 3.185× | 73 B | 1 B |

This control clears both architectural targets: its p95 is 44.408× faster than
the 5.928 ms verified-splice v2 JSON engine control, and its total ownership is
below the 12,386,304-byte memory ceiling. It is not the JSON acceptance row:
the control has one synthetic entity and a tiny state page. The production
JSON port must retain its 39,870-property schema granularity and prove the same
result with its real lexical-span, parent, and identity pages.

Reproduction:

```sh
cargo test -p lix_sdk --lib \
  v3_component_ten_mib_sparse_edit_benchmark \
  --release -- --ignored --nocapture
```

## Real JSON v3 affected-page result

The production JSON schemas and 39,871-entity granularity now run through the
v3 Component ABI. The workload is the same deterministic 10,485,760-byte JSON
fixture and one-byte scalar replacement used by the v2 control. The measured
run performs 100 warmups and 2,000 samples. Before the warm run it drops the
cold-import actor, evicts all decoded host pages, and instantiates a fresh Wasm
actor so cold-import memory cannot masquerade as hot-path ownership.

Profile-driven changes were measured independently:

| implementation | p95 | boundary | entity bytes read | total resident owned payload | result |
| --- | ---: | ---: | ---: | ---: | --- |
| full durable-entity rehydrate | 417.805 ms | 16,068,062 B | 16,067,609 B | 80,633,725 B | fail |
| 1 MiB scalar locator window | 23.479 ms | 244,397 B | 403 B | 83,066,349 B before eviction accounting | fail |
| 8 KiB locator + immutable Merkle map pages, inline first result | 0.485 ms | 3,086 B | 403 B | 5,128,814 B peak | pass latency/memory |

The enforced locator run reports 0.440 ms p50 / 0.485 ms p95. Against the
stricter 5.928 ms v2 engine control it is **12.223× faster**. The counting allocator
measured 3,777,304 resident host bytes, 1,179,648 bytes of guest linear-memory
high water, and a 171,862-byte p95 transient live-allocation delta. That is
5,128,814 peak owned bytes: **7.245× less** than the conservative 37,158,912
byte v2 total.

The 8 KiB buckets reduce hot JSON boundary materialization from 16,440 to 3,086
bytes, a **5.33× v3 reduction**. It remains larger than v2's already-sparse
418-byte retained-document transition, so the locator representation still
needs a point/range refinement.

Durable entity maps now use deterministic content-defined immutable Merkle
pages bounded to 192–320 records. Key-derived cut points prevent an early
insert or delete from shifting every later range. The WIT arena exposes
ordered page ranges with stable fingerprints, first/last keys, and record
counts. A cold plugin can merge-walk page summaries, skip identical predecessor
ranges, and decode only mismatches; the arena tests prove that replacing one
entity changes one fingerprint and an early insertion preserves all later
pages. Structural insertion/deletion rebuilds only the local range through the
next canonical cut, reuses unchanged value arenas without decoding or
repacking them, and produces the same root identity as a clean canonical
rebuild. The test adds one entity and deletes a 100-entity prefix while
creating at most four new physical pages per transition.

Ordered entity cursors now walk physical map pages directly. A bounded
`scan-entities` call does not build a document-wide key set or decode later
manifests; a regression removes a later manifest and proves the first bounded
page still succeeds while an unbounded scan reaches the missing page.
`scan-entity-pages` also preflights requested summary cardinality against both
the transition page limit and minimum boundary bytes, then walks metadata under
an exact byte ceiling that includes both range keys before cloning its host
result vector.

Reproduction:

```sh
cargo test -p lix_sdk --lib \
  json_v3_ten_mib_affected_page_benchmark \
  --release -- --ignored --nocapture

cargo test -p lix_sdk_tests --test e2e \
    v3_json_ten_mib_affected_page_allocator_benchmark \
  --release -- --ignored --nocapture
```

## Real CSV v3 affected-page result

The CSV lane retains the production `csv_v2_table` and `csv_v2_row` schemas and
220,001-entity granularity on the exact 10,680,000-byte / 220,000-row fixture.
The v2 and v3 distribution runs each use 100 warmups and 2,000 samples.

| implementation | p50 | p95 | guest high water | boundary |
| --- | ---: | ---: | ---: | ---: |
| v2 retained document | 0.876 ms | 1.166 ms | 53,018,624 B | 212 B |
| v3 full durable-row rehydrate | 1,857.198 ms | 1,887.603 ms | 119,537,664 B | 43,240,342 B |
| v3 affected row page | 0.453 ms | 0.537 ms | 1,179,648 B | 16,594 B |
| v3 affected row page, counting allocator (200 samples) | 0.432 ms | 0.477 ms | 1,179,648 B | 389 B |

The allocator-instrumented v3 p95 is **2.444× faster** than v2. Packing durable
map values into compressed immutable semantic pages, then collecting
unreachable transition pages before eviction, leaves 11,395,096 resident host
bytes after the repeated-history run. Including guest high water and the
215,361-byte p95 transient live-allocation delta gives 12,790,105 peak owned
bytes: **5.682× less** than the conservative 72,677,056-byte v2 total.

The host-side bounded record locator reduces CSV's hot boundary from 16,594
bytes to 389 bytes while returning only the affected 49-byte row plus compact
locator metadata. This is a **42.7× reduction within v3**, though it remains
177 bytes above the already-retained v2 document path. The result passes the
requested latency and memory gates. The compressed cold-change transport makes
the aggregate real-workload boundary gate a pass.

Reproduction:

```sh
cargo test -p lix_sdk --lib \
  csv_v2_ten_mib_warm_edit_benchmark \
  --release -- --ignored --nocapture

cargo test -p lix_sdk --lib \
  csv_v3_ten_mib_affected_page_benchmark \
  --release -- --ignored --nocapture

cargo test -p lix_sdk_tests --test e2e \
  v3_csv_ten_mib_affected_page_allocator_benchmark \
  --release -- --ignored --nocapture
```

## Real Markdown v3 cold-successor result

The production `markdown_node_v2` schema and node granularity now run through
the v3 Component ABI. The real workload is
`api/references/vscode-api.md` from the local VS Code API reproduction:
commit `b668f69` has 1,237,841 bytes and commit `578def9` has 1,237,840
bytes. The verified edit at offset 107 replaces four bytes with three.

Both distributions use 20 warmups and 200 samples:

| implementation | p50 | p95 | guest high water | boundary |
| --- | ---: | ---: | ---: | ---: |
| v2 retained document | 809.153 ms | 841.728 ms | 102,432,768 B | 727 B |
| v3 fresh cold-successor actor | 1.096 ms | 1.187 ms | 1,376,256 B | 2,863 B |
| v3 counting allocator, 1 KiB locator | 0.663 ms | 0.794 ms | included below | 2,247 B |

The 1 KiB locator allocator run measured 837,270 baseline host-arena bytes and
2,627,905 bytes p95 peak ownership including guest memory and transient
allocations. Against the conservative v2 total of 103,670,609 bytes, v3 is
**1,059.742× faster** at p95 and uses **39.450× less peak memory**.

The cold-successor actor owns no predecessor document. A 1 KiB host-owned
top-level locator reads 347 prospective file bytes, 622 durable-entity bytes,
and 1,256 opaque-state bytes. Length-changing edits append a compact
copy-on-write shift overlay instead of rewriting every later absolute offset;
a test applies a second edit after the shifted range and proves exact bytes
and stable identity. Exact accepted bytes remain in the host byte arena, so
v3 also removes v2's base64 copy of the entire source from the Markdown root
snapshot.

The finer locator reduces hot boundary materialization from 2,863 to 2,247
bytes. It remains 1,520 bytes larger than v2's retained-document boundary.
This row therefore passes the 2×/3× latency and memory gates but does not by
itself satisfy the global boundary-materialization acceptance rule.
Durable entity views expose stable content-defined page fingerprints and
ordered cursors for cold merge-walk reconciliation; localized verified splices
use the smaller source-range locator and never decode those predecessor pages.

Reproduction:

```sh
LIX_MARKDOWN_BENCH_REPO=/root/projects/vscode-api-repro \
  cargo test -p lix_sdk --lib \
  markdown_v2_vscode_api_real_history_benchmark \
  --release -- --ignored --nocapture

LIX_MARKDOWN_BENCH_REPO=/root/projects/vscode-api-repro \
  cargo test -p lix_sdk --lib \
  markdown_v3_vscode_api_real_history_benchmark \
  --release -- --ignored --nocapture

LIX_MARKDOWN_BENCH_REPO=/root/projects/vscode-api-repro \
  cargo test -p lix_sdk_tests --test e2e \
  v3_markdown_vscode_api_real_history_allocator_benchmark \
  --release -- --ignored --nocapture
```

## Production-shaped Excalidraw v3 affected-object result

No large `.excalidraw` corpus is checked into the repository, so this lane is
explicitly production-shaped rather than described as a real user file. It is
a 12,505,008-byte Excalidraw scene containing 42,000 ordinary rectangle
elements with stable IDs, realistic style fields, and custom data. The edit
changes one scalar in element 21,000 without changing its byte length.

The v3 port retains the production `excalidraw_scene`,
`excalidraw_element`, and `excalidraw_file` schemas and exactly the same
scene/element/file granularity. Its host-owned 4 KiB object locator maps the
verified splice to one durable element, reads and reparses only that JSON
object, and atomically returns one successor snapshot. A compact shift overlay
handles length-changing edits; the real Component regression applies a second
edit to a later, shifted object and proves exact bytes and stable durable keys.

| implementation | p50 | p95 | peak/total owned | boundary |
| --- | ---: | ---: | ---: | ---: |
| v2 retained document, 20 samples | 916.267 ms | 955.150 ms | 225,877,856 B conservative | 512 B |
| v3 runtime, 200 samples | 0.652 ms | 0.725 ms | 3,605,949 B retained | 3,772 B |
| v3 counting allocator, 200 samples | 0.479 ms | 0.493 ms | 3,982,001 B p95 peak | 3,772 B |

The allocator result is **1,937.825× faster** at p95 and uses **56.725× less
peak memory**, clearing both hard gates. It reads one durable entity page and
less than 4 KiB of successor file bytes. As in the other affected-page lanes,
the hot boundary is larger than v2's retained private-document path, but it is
bounded below 4 KiB and the aggregate real-workload boundary gate passes.

Cold import remains full-format parsing in both versions. The same cold pass
reported identical 143,065,088-byte guest high water; v2 crossed 34,081,741
bytes and v3 crossed 36,322,800 bytes. That observation is not a cold-import
latency distribution and is not counted as a cold gate pass.

Reproduction:

```sh
cargo test -p lix_sdk --lib \
  excalidraw_v2_ten_mib_sparse_edit_benchmark \
  --release -- --ignored --nocapture

cargo test -p lix_sdk --lib \
  excalidraw_v3_ten_mib_sparse_edit_benchmark \
  --release -- --ignored --nocapture

cargo test -p lix_sdk_tests --test e2e \
  v3_excalidraw_ten_mib_sparse_edit_allocator_benchmark \
  --release -- --ignored --nocapture
```

## Real CSV v3 cold-import result

The cold lane uses the same 10,680,000-byte / 220,000-row CSV fixture, a fresh
actor and arena for every sample, two warmups, and either 20 runtime samples or
10 counting-allocator samples. It includes parsing, all 220,001 exact semantic
snapshots, Component boundary transfer, validation, immutable arena staging,
and atomic commit. The v2 control uses its production retained-document path.

| implementation | p50 | p95 | speedup vs v2 | p95 peak owned | reduction vs v2 | boundary | gate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| v2, 1 MiB pages | 926.191 ms | 1,021.834 ms | 1.000× | 63,698,624 B | 1.000× | 39,400,111 B | control |
| v3, 1 MiB pages, allocator | 878.997 ms | 1,029.939 ms | 1.060× | 27,497,565 B | 2.317× | 66,601,404 B | fail both |
| v3, 256 KiB pages, allocator, direct row packets | 1,067.796 ms | 1,180.574 ms | 0.924× | 19,017,947 B | 3.349× | 66,602,436 B | pass memory only |
| v3, 256 KiB pages, packet-backed values, zstd 1 | 630.337 ms | 720.950 ms | 1.514× | 19,455,972 B | 3.274× | 66,602,436 B | pass memory only |
| v3, 256 KiB raw pages, bounded pipeline, LZ4 transport | 437.040 ms | 441.925 ms | 2.312× | 19,261,683 B | 3.307× | 27,007,575 B | pass all |

Three additional fresh-process executions of the final allocator benchmark all
passed independently. Their p50 range was 433.640–434.815 ms, p95 range was
440.821–448.666 ms, and p95 peak-owned range was
19,174,221–19,324,787 bytes. The worst observed process still clears the gates
at **2.277× faster** and **3.296× lower peak ownership**. The hard acceptance
table below uses these worst observed values rather than the more favorable
single paired run.

The hard targets are at most **510.917 ms p95** and **21,232,874 bytes peak
owned**. Page-size tuning alone did not pass both: one MiB pages reduce
crossing count but leave too many simultaneous guest/host transient owners,
while 256 KiB pages clear memory but add enough cursor crossings to remain
slower than v2. Packed change packets, single-pass page-fed parsing, compact
byte-window identity checkpoints, strictly ordered key validation, direct
snapshot construction into outgoing packets, allocation-free UUID/order-key
formatting, packet-backed immutable values, content-derived value identities,
reusable compression contexts, and manifest-only reachability GC were all
measured. Packet backing, a reusable level-1 compressor, allocation-free
unquoted field encoding, a one-packet host pipeline, and sequential
construction of the roughly five semantic pages per packet improved the
memory-safe allocator p95 substantially. Optional LZ4 transport compression
for packets above 4 KiB spends part of that latency headroom to reduce the cold
Component boundary by 59.4% versus uncompressed v3 and 31.5% versus v2.

The final 20-sample compressed-transport phase profile reported 36.159 ms
open, 384.148 ms drain, and 2.187 ms finish at p95. Within drain it attributed
181.431 ms to guest cursor production (including LZ4), 34.490 ms to packet
decode, 122.955 ms to immutable arena staging, and 6.930 ms to caller output
construction. Zstd `-1` fast
mode was rejected at 663.604 ms allocator p95 and 21,633,531 peak bytes: it
missed both the 2× latency and 3× memory gates. Parallel compression, larger
cursor pages, batched semantic-page construction, and LZ4 as the durable arena
storage codec were also rejected because they regressed latency or exceeded
the memory ceiling. LZ4 is accepted only for the bounded Component transport;
durable arena pages remain zstd-compressed.

Reproduction:

```sh
cargo test -p lix_sdk --lib \
  csv_v2_ten_mib_cold_import_benchmark \
  --release -- --ignored --nocapture

cargo test -p lix_sdk --lib \
  csv_v3_ten_mib_cold_import_benchmark \
  --release -- --ignored --nocapture

cargo test -p lix_sdk_tests --test e2e \
  v3_csv_ten_mib_cold_import_allocator_benchmark \
  --release -- --ignored --nocapture
```

Reproduction:

```sh
cargo test -p lix_plugin_arena --test scorecard -- --nocapture
cargo test -p lix_plugin_arena --test scorecard --release \
  four_format_warm_edit_latency_scorecard -- --ignored --nocapture
```

This format-neutral microbenchmark isolates the arena operation and is
diagnostic. The authoritative 2×/3× gates are the real Component allocator
tests listed per format above and asserted in `rs-sdk-tests`.

## Hard acceptance gates

Every row is an independent pass/fail gate. “Pending” is not a pass.

| format | workload | v2 p95/control | required v3 p95 | conservative v2 total memory | required v3 total memory |
| --- | --- | ---: | ---: | ---: | ---: |
| JSON | 10 MiB verified-splice warm scalar edit | 5.928 ms control | ≤2.964 ms; actual 0.485 ms allocator p95 | 37,158,912 B | ≤12,386,304 B; actual 5,128,814 B |
| Markdown | 1.24 MB VS Code API cold-successor edit | 841.728 ms p95 | ≤420.864 ms; actual 0.794 ms allocator p95 | 103,670,609 B | ≤34,556,869 B; actual 2,627,905 B |
| CSV | 10.68 MB warm row edit | 1.166 ms p95 | ≤0.583 ms; actual 0.477 ms allocator p95 | 72,677,056 B | ≤24,225,685 B; actual 12,790,105 B |
| CSV | 10.68 MB cold import | 1,021.834 ms p95 | ≤510.917 ms; worst observed 448.666 ms | 63,698,624 B | ≤21,232,874 B; worst observed 19,324,787 B |
| Excalidraw | 12.5 MB / 42,000-element sparse edit | 955.150 ms p95 | ≤477.575 ms; actual 0.493 ms allocator p95 | 225,877,856 B | ≤75,292,618 B; actual 3,982,001 B |

The ordinary public-SQL JSON v2 measurement remains a contextual integration
control; production SQL does not select the prototype v3 manifests yet. The
hard v3 rows above use the real Component runtime and counting allocator. They
measure process peak delta and separately report host unique arena bytes and
guest high water so a lower Wasm heap cannot hide host duplication.

## Decision

The immutable host arena design, real Wasmtime v3 binding, and all four format
ports now pass their affected-page/object latency and allocator-memory gates.
The 10.68 MB CSV cold-import lane also passes all three requested gates:
at least 2.277× faster, at least 3.296× lower peak ownership across the repeated
fresh-process runs, and 31.5% fewer Component boundary bytes than v2.

For one occurrence of every authoritative real/production-shaped workload in
this scorecard (JSON hot, CSV hot and cold, Markdown history, and Excalidraw
hot), v2 materializes 39,401,980 boundary bytes and v3 materializes 27,017,069:
a **31.43% aggregate reduction**. Individual already-retained hot transitions
still cross 177–3,260 more bytes than v2, but remain bounded below 4 KiB and
clear the latency gates by 2.277× to 1,938×. This is not a meaningful hot-path
latency regression.

The format-neutral arena suite proves deterministic branches, rollback,
eviction/archive reopen, upgrade aliasing, direction-independent merge, stable
page fingerprints, packet-backed canonical identity, and copy-on-write opaque
state that remains rollback-safe and survives eviction. A contract test parses
the v3 WIT and requires the engine and SDK copies to remain byte-identical. The
real arena-fixture Component also merge-walks predecessor and staged-successor
semantic summaries and verifies equal 32-byte fingerprints across the Wasm
boundary. It additionally stages byte and opaque-state changes, then proves
early finish, malformed entity packets, and overlapping byte-edit output all
roll the complete transition back while leaving the actor reusable. Every
post-dispatch validation error releases the guest cursor, host transaction,
and budget resources. The four format core suites prove exact rendering,
stable IDs, sparse granularity,
and their format-specific conflict behavior; the real Components prove exact
accepted bytes and stable durable keys. A cross-format Component regression
also imports identical bytes through two fresh actors per plugin and requires
the complete root digest, entity keys, opaque-state digest, and emitted key set
to match for Markdown, CSV, JSON, and Excalidraw. It archives and reopens each
root after eviction, upgrades its generation without changing byte/entity/state
arena identities, and proves a fresh upgraded `open-entities` needs no byte
edits. A second cross-format regression creates two branches with edits to
distinct durable entities, requires direction-independent merged entity roots,
invokes `entities-changed` with the merged successor arena, and proves both
merge directions render the same exact bytes containing both edits for all
four formats. All v3 schema files are byte-identical to v2.

On this local evidence, the v3 prototype is **accepted as the hard-cut PR
candidate**. Production selection, commit/push, and CI remain deliberately
deferred while local benchmarking is the requested focus.
