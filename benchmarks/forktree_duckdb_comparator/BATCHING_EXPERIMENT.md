# Bounded authenticated block-to-Arrow experiment

## Frozen verdict

**REJECTED at the focused 10K gate; no 50K/500K widening.** The benchmark-only
candidate preserved every result digest, cold-reopen result, authenticated
range authority, and zero-write invariant for all nine queries on RocksDB and
SlateDB. It did not qualify for continuation: SlateDB `narrow_scan` regressed
**+55.3% wall time**, a critical regression over the 5% limit. No cross-adapter
improvement over 20% was therefore admitted. The current cell is evidence for
the blocker and contract, not an accepted performance result.

The candidate is benchmark/model-only. It changes no production crate, storage
format, OLTP path, version-control path, or PR. The control and candidate use
the same newly built binary; the control leaves `FORKTREE_OLAP_BATCH_ROWS`
unset and follows the unchanged `load_batch` path, while the candidate sets it
to `1024`.

## Exact identity and build

- Base/frozen comparator parent: `cd76d29406ed7e00711a5b5ba9c40da537524dd3`
  (tree `585d9906eb9ae931f3dea2fb7d7a0b724d6eccba`), parent model
  `2a0e8512bb37c9da2050c99c366e5ac05bb01553`.
- Candidate source blob, before the evidence commit:
  `packages/engine-benchmarks/benches/forktree_replacement/olap_datafusion.rs`
  SHA-256 `e473534d9f2920f21c5679c6e604a033c6e922dc192ce5349642e7d09fe84035`.
- Candidate release binary:
  `/root/projects/olap-comparator-target-batching/release/deps/forktree_replacement-4e0a4f3a32542798`,
  SHA-256 `51087bb0b1290b15e5402bedf19ca266a545e255207e815ee80679027e17178`.
- Build command (completed in 14m58s, under the 20m cap):

  ```text
  timeout 20m env CARGO_TARGET_DIR=/root/projects/olap-comparator-target-batching CARGO_BUILD_JOBS=4 cargo build --release -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb
  ```

- Targeted `cargo check` and `rustfmt --edition 2024` passed. No broad Clippy,
  production build, or second build was started after the gate. The final
  rustfmt-only import ordering change is semantically inert; the measured
  binary was built from the same candidate source before that formatting-only
  change and is retained as the raw-run binary identity above.

## Commands and correctness

The RocksDB and SlateDB cells each used three measured samples after one
warm-up, with setup excluded, cold file reopen before query execution, and
the following commands:

```text
timeout 20m env TMPDIR=/root/projects/olap-comparator-db /usr/bin/time -v <binary> olap-datafusion rocksdb forktree 10000 32 3 1 1
timeout 20m env TMPDIR=/root/projects/olap-comparator-db FORKTREE_OLAP_BATCH_ROWS=1024 /usr/bin/time -v <binary> olap-datafusion rocksdb forktree 10000 32 3 1 1
timeout 20m env TMPDIR=/root/projects/olap-comparator-db /usr/bin/time -v <binary> olap-datafusion slatedb forktree 10000 32 3 1 1
timeout 20m env TMPDIR=/root/projects/olap-comparator-db FORKTREE_OLAP_BATCH_ROWS=1024 /usr/bin/time -v <binary> olap-datafusion slatedb forktree 10000 32 3 1 1
timeout 20m env TMPDIR=/root/projects/olap-comparator-db /usr/bin/time -v /root/projects/olap-comparator-target-duckdb-1.10505/release/forktree-duckdb-comparator 10000 3 1
```

The nine query cases cover point, key range, narrow scan, wide projection,
selective filter, grouped aggregate, order/limit, join, and column
projection. Every control/candidate pair had identical result digests and
row cardinalities, cold reopen passed, `begin_writes=0`, `commits=0`, and no
write batches/puts/deletes. Backend logical work was unchanged: RocksDB
point/range/scans used the same six `get_calls` (join twelve), while SlateDB
used the same five physical objects for one-source queries and ten for the
join. Settled disk was unchanged: RocksDB `931857` bytes, SlateDB `892207`
bytes, and DuckDB `3158016` bytes.

## 10K medians

Wall is microseconds, allocations are bytes, and RSS is maximum process RSS
from the run. Deltas are candidate relative to control.

| Query | Rocks control wall / alloc | Rocks candidate wall / alloc | Rocks wall / alloc / RSS delta | Slate control wall / alloc | Slate candidate wall / alloc | Slate wall / alloc / RSS delta |
|---|---:|---:|---:|---:|---:|---:|
| point | 276 / 208,796 | 248 / 221,146 | -10.4% / +5.9% / +0.6% | 282 / 247,357 | 336 / 259,707 | +19.0% / +5.0% / -1.0% |
| key range | 397 / 302,713 | 346 / 313,679 | -12.8% / +3.6% / -0.1% | 396 / 341,274 | 416 / 352,240 | +5.2% / +3.2% / -1.2% |
| narrow scan | 9,072 / 11,944,570 | 6,949 / 11,010,106 | -23.4% / -7.8% / -5.7% | 8,890 / 12,670,009 | 13,803 / 11,735,545 | **+55.3%** / -7.4% / -2.6% |
| wide projection | 21,667 / 48,628,087 | 16,669 / 48,573,199 | -23.1% / -0.1% / -5.6% | 21,404 / 50,075,665 | 21,016 / 50,020,777 | -1.8% / -0.1% / -7.0% |
| selective filter | 8,989 / 7,884,371 | 7,410 / 7,413,595 | -17.6% / -6.0% / -5.4% | 8,899 / 9,119,874 | 9,585 / 8,649,098 | +7.7% / -5.2% / -6.8% |
| group aggregate | 10,591 / 14,335,397 | 8,707 / 15,395,669 | -17.8% / +7.4% / -5.6% | 9,480 / 15,031,220 | 9,910 / 16,091,300 | +4.5% / +7.1% / -6.5% |
| order/limit | 8,973 / 9,175,869 | 7,691 / 8,179,625 | -14.3% / -10.9% / -5.5% | 8,204 / 9,871,692 | 9,055 / 8,875,448 | +10.4% / -10.1% / -6.2% |
| join | 11,365 / 19,525,272 | 9,694 / 18,642,958 | -14.7% / -4.5% / -5.1% | 9,702 / 20,260,028 | 10,756 / 19,377,330 | +10.9% / -4.4% / -6.6% |
| column projection | 16,674 / 23,585,143 | 12,006 / 26,153,235 | -28.0% / +10.9% / -5.1% | 13,771 / 24,875,197 | 13,974 / 27,443,289 | +1.5% / +10.3% / -6.6% |

Rocks maximum RSS fell from approximately `123124 KiB` to `116768 KiB` on
the representative process run; SlateDB fell from approximately `162220 KiB`
to `142420 KiB`. The allocation counters are process measurements and vary
slightly by query/sample; the raw logs retain every sample, backend counter,
digest, and `/usr/bin/time -v` field. DuckDB 10K medians were: point `277.389`
us, key range `669.817` us, narrow `2253.498` us, wide `15960.779` us,
filter `706.357` us, group `1498.373` us, order `3562.401` us, join
`2847.176` us, and column projection `1672.231` us. DuckDB has no Lix
authentication, branch/version-control authority, or comparable Rust
allocation scope; it is a query-engine comparator only.

## Source and complexity conclusion

The model adds an opt-in `BatchBuilders` path that appends decoded values
directly to typed Arrow builders in chunks, rather than retaining decoded
`Vec<Vec<Datum>>` and calling the old `rows_to_batch` second copy. It still
calls the existing `ForkTree::read_range`, which returns a full raw row vector.
Therefore this is **not** a true end-to-end `O(B)` implementation.

Let `H` be authenticated tree height, `P` touched immutable packs, `N` visited
rows, `W` selected width, `B` batch rows, and `S` downstream operator state:

- Existing path: `O(H + P + N*W + operator(N))` time and approximately
  `O(N*raw + N*Datum*W + N*Arrow*W + S)` transient ownership.
- This experiment: same asymptotic time and `O(N*raw + B*Datum*W +
  B*Arrow*W + S)` provider ownership. It deletes the decoded full-row and
  second provider-copy terms, but the raw `read_range` vector remains `O(N)`.
- A post-landing implementation must replace only the read-side range source
  with a cursor that keeps one authenticated `StorageRead`, validates each
  complete immutable block before exposing fields, and yields backpressured
  `RecordBatch`es of at most `B` rows. Its intended working set is
  `O(B*W + S + authentication frontier)`; time remains
  `O(H + P + N*W + operator(N))` without a second index or format.

The measured RocksDB gains are not sufficient: SlateDB has a critical narrow
scan regression and no accepted cross-adapter >20% result. No OLTP or
version-control cost was introduced or measured because no write/storage
production path was changed; the post-landing contract must keep the feature
strictly in the DataFusion OLAP provider read path, with point/transaction/
publication/recovery code byte-identical.

## Post-landing implementer contract

1. Keep one coherent authenticated read and one monotonic range cursor. Do not
   reopen, reauthenticate a full closure, cache rows, add an index, or add a
   persisted format.
2. Make the storage-side cursor yield authenticated block slices into bounded
   typed Arrow builders. Predicate evaluation may happen during decode, but
   complete block authentication must precede any dependent output.
3. Preserve exact null, projection, filter, aggregate, order/limit, join,
   pagination, result-digest, cold-reopen, and corruption-fail-closed
   semantics. Keep the cursor lifetime and cancellation behavior explicit.
4. Keep this read-only provider seam out of OLTP/version-control code and
   persisted writes; verify unchanged write/recovery/version-control tests and
   backend calls.
5. Re-run the identical 10K RocksDB+SlateDB gate first. Only if both adapters
   show >20% meaningful wall/allocation/RSS improvement with no critical >5%
   regression may 50K, then 500K, be admitted. The current candidate fails
   this rule and must not be widened.

## Raw evidence hashes

```text
batching-2a0e-10k-control-rocks.log   dfc7984859a15ae35738d5870e8fbe5ade785a8faa658b6da2d3a3c70f1423ac
batching-2a0e-10k-candidate-rocks.log  cdbf545b36b1141757973a15d953fa4a7315666e69a67f123bda03f4bf8e491b
batching-2a0e-10k-control-slate.log    babd9e7d7027537673226bfa8f3a90989b49693f7bd0a02b0061bd701a735ce1
batching-2a0e-10k-candidate-slate.log  200166a6526dd4d48e7a7db7ab2f51e2978b397ef29152a32c380af9e4cd2c34
batching-2a0e-10k-duckdb.log           d83430cd06fa053cfeb53d7bb8444b7daafe52ab4ca5cf72c426ed1cf87f8a11
```
