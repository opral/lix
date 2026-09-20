# Undo/redo performance profile

This report records a preliminary run and a refreshed run from the optimized benchmark binary.

## Environment

- Date: 2026-09-20 UTC
- Host: `ryzen-9950x-I`, Linux 6.17.0-23-generic, x86_64
- CPU: AMD Ryzen 9 9950X, 32 logical CPUs
- Preliminary benchmark binary: `target/release/deps/undo_redo-7796f81e56595a86`, 1,166,305,992 bytes, optimized + debuginfo
- Refreshed benchmark binary: the same path, 1,166,135,208 bytes, optimized + debuginfo
- Backend: Criterion walltime compatibility (`codspeed_criterion_compat_walltime`); preliminary routines use `iter_custom`, while refreshed routines use `iter_with_setup`, with setup/fork excluded from the reported operation interval.
- Commands used `--bench <group> --sample-size 10 --warm-up-time 0.05 --measurement-time 0.1 --noplot`.

## Preliminary operation timings

Reported intervals are Criterion `[low, central estimate, high]` values. They are operation timings from the benchmark's explicit `Instant` interval, excluding fixture fork/session setup.

| Case | Width/depth | Time |
| --- | ---: | ---: |
| checkpoint full | 1 | 1.038–1.068 ms (central estimate 1.058 ms) |
| checkpoint scoped first undo | 1 | 1.281–1.321 ms (central estimate 1.295 ms) |
| checkpoint full | 100 | 2.117–2.128 ms (central estimate 2.121 ms) |
| checkpoint scoped first undo | 100 | 1.570–1.577 ms (central estimate 1.573 ms) |
| checkpoint full | 1,000 | 10.944–10.987 ms (central estimate 10.964 ms) |
| checkpoint scoped first undo | 1,000 | 4.040–4.057 ms (central estimate 4.049 ms) |
| checkpoint full | 10,000 | 125.86–130.12 ms (central estimate 127.92 ms) |
| checkpoint scoped first undo | 10,000 | 31.756–33.243 ms (central estimate 32.487 ms) |
| ordinary update | history 10 | 0.609–0.612 ms (central estimate 0.610 ms) |
| undo | history 10 | 0.536–0.600 ms (central estimate 0.562 ms; 2 outliers) |
| redo | history 10 | 3.211–4.077 ms (central estimate 3.671 ms; 2 outliers) |
| ordinary update | history 1,000 | 3.358–4.771 ms (central estimate 3.940 ms; 1 outlier) |
| undo | history 1,000 | 3.433–4.981 ms (central estimate 4.115 ms; 1 outlier) |
| redo | history 1,000 | 7.826–11.681 ms (central estimate 9.867 ms) |
| file delete undo | unrelated width 1 | 2.990–3.754 ms (central estimate 3.363 ms) |
| file delete undo | unrelated width 1,000 | 5.044–5.784 ms (central estimate 5.418 ms) |
| file delete undo | unrelated width 10,000 | 5.266–6.675 ms (central estimate 5.941 ms) |
| wide transition undo | width 1 | 0.597–0.603 ms (central estimate 0.599 ms) |
| wide transition undo | width 100 | 2.651–2.747 ms (central estimate 2.704 ms) |
| wide transition undo | width 1,000 | 24.777–25.868 ms (central estimate 25.322 ms) |
| wide parent undo | width 10 | 0.580–0.582 ms (central estimate 0.581 ms) |
| wide parent undo | width 1,000 | 0.719–0.722 ms (central estimate 0.720 ms) |
| sparse identity gap undo | history 10 | 0.556–0.557 ms (central estimate 0.556 ms) |
| sparse identity gap undo | history 1,000 | 0.562–0.566 ms (central estimate 0.564 ms) |

The checkpoint inventory measurements rise with effect width for both full and one-row scoped undo. The scoped path is lower than full undo at large widths but still scales with the complete inventory, consistent with checkpoint effect discovery and durable state serialization. The ordinary sparse-gap result stays nearly flat across history depth in this fixture.

## CPU profile

`perf stat -d` around `undo_checkpoint_effect_inventory/full/1000` recorded 1.576 s process wall time, 1.735 s user, 0.052 s system, 17.02 billion instructions, 9.37 billion cycles, 1.52% branch misses, and 2.72% L1 data-cache misses. Process counters include benchmark fixture construction for all groups because the harness seeds fixtures before Criterion applies the filter; they are not operation-only counters.

The preliminary 99 Hz frame-pointer profile had 1,408 samples; its textual report is in [undo-checkpoint-full-1000.perf.txt](undo-checkpoint-full-1000.perf.txt), and raw data is retained outside the repository at `/tmp/lix-undo-profile/undo-checkpoint-full-1000.perf.data`. The highest symbols were `lix::order_preserving_key::write_key_string` (4.62%), `sqlparser::ast::query::TableFactor::clone` (4.30%), allocator `malloc`/`free` paths (about 4% each), and BLAKE3 compression (3.11%). The profile is process wide and includes fixture setup; it does not establish that any one symbol dominates the measured SQL operation.

The source review found one likely duplicate cost in the no-argument undo path: `normalize_cursor` performs a payload-free target/parent diff to decide whether a partially undone ordinary target remains, and `execute` then computes the same diff again to build effects. Checkpoint redo now avoids that scan by using the durable receipt's pending effect IDs directly. The checkpoint `all` inventory remains a BTreeSet serialized into durable target state; replacing it with a count would require preserving membership validation and is not recommended without a targeted change and regression test.

## Limitations

The preliminary binary predates the current `iter_with_setup` benchmark adaptation and the latest core source changes. It is useful for scale shape and baseline magnitude only. Criterion samples are short, and the process-level `perf` counters include fixture setup. The refreshed benchmark build and measurements below supersede these values.

## Refreshed operation timings

The refreshed binary was built with `cargo bench -p lix --bench undo_redo --no-run` at 2026-09-20 19:09 UTC (1,166,135,208 bytes, optimized + debuginfo). Benchmarks now use Criterion's `iter_with_setup`, so fixture fork/open setup is outside the measured operation and remains compatible with the pinned codspeed Criterion 4.6 API. Values below are Criterion `[low, central estimate, high]` intervals from 10 samples. The central estimate is reported for scale comparison; only the helper line below is an explicit median.

| Case | Width/depth | Time |
| --- | ---: | ---: |
| checkpoint full | 1 | 1.088–1.537 ms (central estimate 1.327 ms) |
| checkpoint scoped first undo | 1 | 1.372–1.954 ms (central estimate 1.575 ms) |
| checkpoint full | 100 | 2.243–2.648 ms (central estimate 2.357 ms) |
| checkpoint scoped first undo | 100 | 1.506–1.508 ms (central estimate 1.506 ms) |
| checkpoint full | 1,000 | 9.678–9.709 ms (central estimate 9.692 ms) |
| checkpoint scoped first undo | 1,000 | 3.243–3.251 ms (central estimate 3.247 ms) |
| checkpoint full | 10,000 | 107.99–108.70 ms (central estimate 108.35 ms) |
| checkpoint scoped first undo | 10,000 | 19.124–19.180 ms (central estimate 19.150 ms) |
| ordinary update | history 10 | 0.673–0.693 ms (central estimate 0.680 ms) |
| undo | history 10 | 0.560–0.566 ms (central estimate 0.562 ms) |
| redo | history 10 | 0.603–0.607 ms (central estimate 0.605 ms) |
| ordinary update | history 1,000 | 0.636–0.639 ms (central estimate 0.638 ms) |
| undo | history 1,000 | 0.575–0.582 ms (central estimate 0.578 ms) |
| redo | history 1,000 | 0.600–0.603 ms (central estimate 0.601 ms) |
| file delete undo | unrelated width 1 | 0.750–0.751 ms (central estimate 0.750 ms) |
| file delete undo | unrelated width 1,000 | 1.036–1.072 ms (central estimate 1.051 ms) |
| file delete undo | unrelated width 10,000 | 1.307–1.317 ms (central estimate 1.313 ms) |
| wide transition undo | width 1 | 0.560–0.564 ms (central estimate 0.562 ms) |
| wide transition undo | width 100 | 2.200–2.205 ms (central estimate 2.203 ms) |
| wide transition undo | width 1,000 | 17.172–18.128 ms (central estimate 17.636 ms) |
| wide parent undo | width 10 | 0.573–0.579 ms (central estimate 0.576 ms) |
| wide parent undo | width 1,000 | 0.691–0.699 ms (central estimate 0.695 ms) |
| sparse identity gap undo | history 10 | 0.556–0.558 ms (central estimate 0.557 ms) |
| sparse identity gap undo | history 1,000 | 0.565–0.575 ms (central estimate 0.570 ms) |

The operation-only timing helper independently recorded 19.221 ms median for scoped checkpoint undo at width 10,000 over 13 samples, consistent with Criterion's 19.150 ms central estimate. The new redo path stays near 0.60 ms at both tested history depths, while checkpoint inventory remains the scaling case: full inventory reaches 108 ms at width 10,000 and one-row scoped undo reaches 19 ms.

## Refreshed CPU profile

`perf stat -d` for refreshed checkpoint full/1,000 reported 1.454 s process wall time, 1.584 s user, 0.051 s system, 16.20 billion instructions, 8.95 billion cycles, 1.61% branch misses, and 2.66% L1 data-cache misses. As with the preliminary counters, the process includes fixture construction before the filtered benchmark executes.

The refreshed 99 Hz frame-pointer capture has about 1K samples. Its textual report is [undo-final-checkpoint-full-1000.perf.txt](undo-final-checkpoint-full-1000.perf.txt); raw data is retained at `/tmp/lix-undo-profile/undo-final-checkpoint-full-1000.perf.data`. The largest sampled symbol was `__memmove_avx512_unaligned_erms` (20.31%), followed by `_int_malloc` (5.57%), `lix::hot_state::tracked_head::hot::checked_add_hot_next_value_capacity` (5.29%), `_int_free_merge_chunk` (5.07%), and tracked-state materialization/storage paths. These are process-wide samples and should be treated as attribution leads rather than operation-only hotspots.

## RocksDB backend measurements

The real-backend harness was rebuilt with all features using:

```text
cargo bench --manifest-path tooling/Cargo.toml -p lix_e2e --bench undo_redo_storage --all-features --no-run
```

The build completed at 2026-09-20 20:19 UTC in 17m59s. The executable was `target/release/deps/undo_redo_storage-3737453c40020a1d`, 2,310,901,528 bytes, SHA-256 `4db83ef3c6057cb43dcce4b1eeec0e9f8b3a71fd21c5f3f2536dbde049a053f4`. The setup diagnostic uses the active session-bound adapter, so it does not trigger the RocksDB session fence. Build output is retained at `/tmp/lix-e2e-undo-build-final.log`.

Each operation used a fresh isolated RocksDB directory under `/tmp/lix-undo-rocksdb-final-20260920/`, with 1,000 seeded rows and a 100-row transition. Setup confirmed durable roots for both seed and transition commits. The measured `wall_ms` interval starts immediately before `SELECT commit_id FROM lix_undo()` or `SELECT commit_id FROM lix_redo()` and ends after the SQL operation; opening the database, fixture construction, and post-operation verification are outside that interval.

| Operation | Samples (ms) | Median (ms) | Allocated bytes | Backend growth (bytes) | I/O counters |
| --- | ---: | ---: | ---: | ---: | --- |
| RocksDB undo | 4.097, 3.902, 3.977 | 3.977 | 7,756,829 | 47,770–76,847 | 209 get-many calls / 539 keys; 12 scans / 137 rows; 14 put batches / 129 puts; 1 delete batch / 1 delete |
| RocksDB redo | 3.650, 3.614, 3.714 | 3.650 | 7,215,137 | 77,728–79,054 | 213 get-many calls / 548 keys; 12 scans / 140 rows; 12 put batches / 125 puts; 1 delete batch / 1 delete |

The three-sample means were 3.992 ms for undo and 3.659 ms for redo. Allocation-call counts were stable at 27,887–27,888 for undo and 27,878 for redo. The harness's process CPU tick counter reported 0–1 ticks for these short operations, so it is not a useful CPU-time estimate at this duration. RSS before measurement was about 118 MiB and peak RSS about 140–142 MiB; these RSS values include the opened process and backend, not only the SQL operation. The samples are bounded backend evidence rather than a statistical benchmark: one fixture shape, three samples per operation, and no concurrent clients.
