# Shared tracked-head proof and publication

Prepared-row and ordered-journal replacements share one ancestry walker. Their callers preserve different admission requirements: prepared rows can decline certification; journals require proof. Journal lifecycle seeds remain bound to the exact state parent. Missing-parent/cycle errors, physical authority checks, partition/cardinality/identity comparisons and fallback ancestry are retained.

Journal replacement, ordered/columnar insert and complete replacement share one working-diff epoch/branch-control publication step. `PackedHeadSchemas` carries the schema-note behavior and preserves exclusive-columnar bookkeeping. The specialized writers and their arguments/instrumentation remain unchanged. Earlier checkpoint/catalog-refresh paths and later ordinary-delta/untracked publication paths are textually unchanged from a2d84abf2.

No public API, storage format, optimized writer or publication transaction boundary changed. Added a canonical Memory regression for unproven fallback boundaries and missing ancestry despite a lifecycle seed.

Validation passed:

- 3,729 engine tests with `all-simulations,server-protocol` (69 skipped).
- 10 doctests.
- Four additional tests covering RocksDB/SlateDB replacement-delete-checkpoint reopen, columnar queries above the publication threshold, and queries after sparse/moderate mutations.
- Seven alternating baseline/candidate process pairs for each of four optimized-path tests; every correctness assertion passed. Timing medians ranged 0.989–1.007 of baseline, peak RSS 0.995–0.998. No allocation instrumentation or production-throughput claim is made for this comparison.
- Source comparison verified all four packed writer call sites retain identical arguments; `git diff --check` passed.

## Paired test timings

Baseline a2d84abf2; seven alternating process pairs per workload, same nightly debug profile. All assertions passed. These measure full test processes including setup and assertions, not isolated production throughput. Peak RSS includes the process runtime and libraries.

| Workload | Baseline median ms | Candidate median ms | Time ratio | Peak RSS ratio |
|---|---:|---:|---:|---:|
| `large_ordered_parameter_insert_reuses_commit_delta_as_current_base` | 49.96 | 49.42 | 0.989 | 0.997 |
| `successive_columnar_inserts_preserve_the_existing_schema_base` | 114.48 | 115.28 | 1.007 | 0.998 |
| `large_ordered_parameter_update_replaces_complete_packed_current_base` | 265.67 | 264.94 | 0.997 | 0.996 |
| `packed_replacement_over_hot_before_resolves_compact_working_diff` | 78.74 | 78.45 | 0.996 | 0.995 |

Candidate source SHA-256: `a1ceaf2d74170b60b82fcea4bda156bffca39e6075e759ab9c9de3bc019aa571`. Raw samples: `/root/repos/lix-tracked-publication-timing.json`.
