# Shared plugin cold-file transition pipeline

Fresh unowned-file imports and ownership reselection now use one guest open/drain routine, one host create-row/accounting preparation step, and one checkpoint/publication constructor. Fresh-file metadata is shared across its prepared and pending states.

Preserved boundaries:

- Fresh imports acquire batch reservations and store permits before spawning bounded chunks, join every worker, retain the first error, and finalize in input order.
- Fresh imports append rows before checkpointing; reselection checkpoints before row staging and retains its explicit publication-discard path on append failure.
- Keyless create validation/materialization precedes row-authority derivation and counters.
- `PluginActorStore` owns the actor and permit across host preparation/checkpoint awaits, preserving actor-before-permit destruction on failure or cancellation.
- The same-owner sparse transition, fresh permit-admission loop and join-all/first-error code are textually unchanged from integration fc0c0ecea.
- No additional full-file bytes allocation, storage scan, worker, guest call or checkpoint is introduced.

This removes duplicated protocol implementations; helper signatures and explicit intermediate state offset the removed lines, so it is not a net production line-count reduction.

A real-plugin Memory regression covers a two-file fresh batch, markdown-to-JSON ownership reselection, transactional rows, rollback identity/byte restoration, retry, sibling preservation and a subsequent edit. Correctness and performance sub-agents reviewed the production diff; the performance reviewer independently reviewed the new test. No correctness findings remain.

Validation: all 3,728 engine tests passed with `all-simulations,server-protocol` (69 skipped), all 10 doctests passed, and all five plugin-file-observation tests passed. Four additional existing regressions passed: the 17-file RocksDB import/lifecycle test, tracked/untracked ownership-loss reparse, markdown sparse successor/history/reopen, and JSON actor-eviction/sparse-successor behavior. The final code passed `git diff --check`. Performance results follow below.

## Performance comparison

Baseline: `fc0c0ecea`; candidate: shared cold plugin pipeline working tree. Same nightly toolchain and debug build profile. Three alternating process pairs, seven samples per lane per process (21 samples per variant/lane), run after compilation and tests were idle. Every workload passed its built-in correctness assertions. Ratios are candidate / baseline; values below 1 improve.

| Workload | Time p50 | Allocations p50 | Allocated bytes p50 | Peak live bytes p50 |
| --- | ---: | ---: | ---: | ---: |
| csv-file-roundtrip | 0.969 | 0.996 | 1.011 | 0.992 |
| csv-sparse-file-update | 0.992 | 1.015 | 1.041 | 1.050 |
| json-file-roundtrip | 0.958 | 1.021 | 1.008 | 1.008 |
| json-sparse-file-update | 0.957 | 0.980 | 1.055 | 1.019 |
| markdown-file-roundtrip | 1.021 | 1.000 | 0.993 | 1.003 |
| markdown-sparse-file-update | 1.011 | 0.999 | 1.051 | 1.020 |
| text-file-roundtrip | 1.017 | 0.997 | 1.006 | 1.005 |
| text-sparse-file-update | 1.004 | 0.999 | 1.000 | 1.003 |
| excalidraw-file-roundtrip | 1.022 | 1.028 | 1.004 | 0.971 |
| excalidraw-sparse-file-update | 0.987 | 0.993 | 1.003 | 1.032 |
| markdown-same-row-text-merge | 0.955 | 0.986 | 0.997 | 1.000 |
| csv-same-row-column-merge | 0.990 | 0.993 | 1.000 | 1.000 |

These are bounded debug-profile regression measurements, not a production throughput guarantee. The five fresh-file and five warm sparse lanes use the public APIs; the two merge lanes provide adjacent-path controls. Parallel batching and reselection are additionally covered by source review and dedicated correctness tests. Raw sample logs are outside the repository at `/root/repos/lix-plugin-perf-{baseline,candidate}-run{2,3,4}.log`.

Candidate `transaction/context.rs` SHA-256: `521fe273b6fccf1bbd97d49c7a76e197750f1c717601bf3c1002757536a8d657`. Toolchain: `nightly-2026-05-21`; E2E features: `plugin-tests,sdk-tests`.

All observed ratios passed the benchmark's existing gates (10% for median elapsed/allocated bytes/peak live bytes and 15% for p95 elapsed). Maximum p95 elapsed ratio was 1.046. These results bound this comparison; they do not prove identical costs for every workload.
