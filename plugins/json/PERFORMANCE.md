# JSON plugin performance

Run the reproducible native adapter profile with:

```sh
cargo test -p plugin_json profile_json_adapter_scaling -- --ignored --nocapture --test-threads=1
```

The fixture contains flat object members with short scalar values. Each batch updates evenly spaced members, including the final member. The profile reports the best of five serialize calls, initial fixture construction/parse time, private-state bytes, and actual scalar-index read counts. The native testing host clones the entire snapshot when committing a transition; its timings therefore include overhead proportional to file and state size even when the plugin only reads one scalar. These are native adapter measurements, not Wasm or end-to-end SQL throughput claims.

## Baseline comparison

Measured on Linux x86-64 with Rust nightly-2026-05-21, plugin opt-level 0 and dependency opt-level 1. Baseline is origin/main `852c1151f`, built from an isolated copy with the identical profile function and the same cached dependency artifacts/compiler flags. The scalar-entry counter was added to the baseline read helper. No legacy lookup implementation is shipped in the plugin.

| Object members | Changed rows | Baseline serialize µs | Indexed serialize µs | Baseline scalar-entry reads | Indexed identity + scalar-entry reads |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 128 | 1 | 125 | 22 | 128 | 8 + 1 |
| 128 | 128 | 8,013 | 1,761 | 8,256 | 1,026 + 128 |
| 1,024 | 1 | 872 | 25 | 1,024 | 11 + 1 |
| 1,024 | 128 | 54,147 | 1,827 | 66,048 | 1,409 + 128 |
| 8,192 | 1 | 6,721 | 67 | 8,192 | 14 + 1 |
| 8,192 | 128 | 420,361 | 1,967 | 528,384 | 1,792 + 128 |

The identity index adds 36 bytes per scalar, plus page keys. At 8,192 members initial fixture construction/parse was 65.5 ms before and 69.6 ms after; total state grew from 712,751 to 1,089,583 bytes. That increase includes both the identity index and duplicate-key occurrence metadata/checkpoint fields. These indexed measurements use the final occurrence-column schema.

## Optimized plugin check

A second comparison compiled both plugin sources with `rustc -C opt-level=3`, linking the same cached dependency artifacts (dependency opt-level 1). This isolates optimized plugin code without claiming a fully optimized production SDK or Wasm benchmark. The same fixture, five-sample minimum, and native host were used.

| Object members | Changed rows | Baseline serialize µs | Indexed serialize µs |
| ---: | ---: | ---: | ---: |
| 128 | 1 | 57 | 17 |
| 128 | 128 | 4,353 | 1,089 |
| 1,024 | 1 | 346 | 18 |
| 1,024 | 128 | 23,484 | 1,108 |
| 8,192 | 1 | 3,003 | 61 |
| 8,192 | 128 | 180,520 | 1,187 |

The largest batch improved about 152×; a single last-member update improved about 49×. Read counts match the unoptimized comparison. Initial fixture construction/parse was 56.1 ms baseline and 51.6 ms indexed in this run; such small parse-time differences should not be interpreted as a guaranteed speedup.

## Structural scaling

```sh
cargo test -p plugin_json profile_json_structural_scaling -- --ignored --nocapture --test-threads=1
```

This profile replaces the whole file in a nested array with one sibling per level (`[[[0,0],0],0]`), changing the deepest value. Both sources use plugin opt-level 0 with identical dependencies. Fixture creation is outside the timer; the baseline runs on a 16 MiB test thread stack to accommodate its recursive parser. Counts instrument actual subtree rebases and source bytes passed to hashing.

| Depth | File bytes | Baseline µs | Updated µs | Baseline descendant rebases | Updated descendant rebases | Baseline hashed bytes | Updated hashed bytes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 129 | 478 | 438 | 992 | 0 | 4,096 | 258 |
| 128 | 513 | 2,465 | 1,721 | 16,256 | 0 | 65,536 | 1,026 |
| 512 | 2,049 | 20,215 | 6,695 | 261,632 | 0 | 1,048,576 | 4,098 |

Reconciliation now visits matched nodes once, rebases only unmatched or kind-changed subtrees, and hashes each input byte once using bottom-up child hashes. Identical subtrees adopt accepted identities in one traversal after exact byte verification. The provenance lookup uses binary search over unchanged spans instead of rescanning all edit spans for every node. These changes remove the measured depth-squared rebase/hash work; child-order reconciliation can still require sorting siblings.

## Complexity and limits

Let `B` be input bytes, `N` node count (the scalar index contains only scalar nodes), `K` changed rows, `M` distinct scalars with a nonzero length delta since the last full checkpoint, and `V` total bytes of the touched scalar values.

- Previously, semantic updates scanned scalar metadata from the start for each changed row: up to `O(KN)` state range reads, plus an unconditional `O(B)` full-file read.
- Semantic scalar updates now use a sorted, paged identity index: `O(K log N)` index reads, `O(K)` scalar metadata reads, and `O(V)` file bytes. Full primary keys are verified after matching the hash. Hash collisions cannot update a different row.
- The canonical shift overlay uses a length manifest and bounded pages, so more than 87,000 distinct length-changing scalars do not exceed the host state-operation limit. It decodes in `O(M)`; replacing it remains `O(M)` and removes obsolete pages. Prefix sums take `O(M)` once; shifted starts use `O(log M)` lookup instead of rescanning all shifts for each binary-search probe. A file scalar edit costs `O(M + log N log(M+1) + V)` CPU work and `O(log N)` index reads. Batched semantic delta updates use a balanced map instead of repeated vector insertion, avoiding `O(KM)` element movement.
- Structural reconciliation uses an explicit event stack. Subtree hashes take `O(B + N)` work; provenance lookups take `O(log(E+1))` for `E` file edits. Direct-child iteration follows sibling links, so it does not scan unrelated nodes. Child matching and order reconciliation may add sibling sorting work.
- Complete checkpoints build the identity index in `O(N log N)` time and `O(N)` extra memory, with pages bounded below 1 MiB to leave room for state-operation keys. Structural changes rebuild/checkpoint the full document; they do not use the scalar fast path. Cold parsing and reconstruction must read the full bytes/rows.
- Empty semantic batches return before accessing file or private state.

Regression tests assert logarithmic identity reads for a last-member update in an 8,192-member document, no full-file read on that path, correct offset shifts with scattered updates, multi-page lookups and corrupt state rejection, duplicate occurrence identities, last-write-wins batches, and prefix-sum overflow handling. The counters compile only in tests. A default-budget native regression applies 90,000 length-changing SQL rows, follows with source/SQL scalar edits, shrinks a two-page overlay to one page, and verifies checkpoint cleanup. Missing/truncated shift pages and invalid manifests fail cleanly.

Timing thresholds are deliberately excluded from CI because host load and build profile affect them.
