# File-qualified row-reference profile

Measured 2026-09-21 on a shared Linux x86_64 host, release build, canonical
in-memory storage. Concurrent local CI adds substantial timing noise.

## Reproduce

```sh
cargo build -p lix --release --example profile_row_ref
target/release/examples/profile_row_ref 1000 100
target/release/examples/profile_row_ref 10000 100
target/release/examples/profile_row_ref 100000 100
```

The fixture creates two files with identical integer primary-key sets. Setup is
excluded. Construction returns a reference for every row. Each lookup selects
two different `(file_id, primary_key)` pairs from `lix_diff`, with an assertion
that exactly two rows are returned (the Cartesian product would return four).

| Changed rows | Construct all references (ms) | Two-reference lookup (ms/query) |
| ---: | ---: | ---: |
| 2,000 | 5.781 / 5.192 / 5.946 | 0.705 / 0.677 / 0.701 |
| 20,000 | 39.998 / 40.194 / 27.541 | 0.670 / 0.648 / 0.540 |
| 200,000 | 420.880 | 0.540 |

Each lookup measurement averages 100 warmed queries. These samples support
bounded point lookup for this workload; they are not a before/after comparison
or a latency guarantee. Large reference lists can still overfetch combinations
of selected files and keys: the storage request narrows both dimensions, then
an exact tuple filter removes cross-pair matches before applying a limit.

## CPU samples

A separate run used 20,000 rows and 5,000 warmed lookups (0.570 ms/query).
`perf record -e cpu-clock -F 499 -D -1 --control fifo:...` was enabled through
`LIX_ROW_REF_PERF_CONTROL` only around the lookup loop, excluding fixture setup
and reference construction. There were 1,424 samples and no lost samples.
Largest self-time symbols: memory copy 6.32%, malloc 4.78%, memory comparison
3.65%, free 2.60%, and tracked-state node decoding 2.53%.
No individual row-reference codec symbol exceeded 1% self time. Inlining and
sampling granularity mean this does not establish zero codec cost.
