# Large-payload read qualification

Qualified on 2026-08-07 against exact `origin/main`
`f793c70a1b3dd4437826457301974bad702f0f81` (tree
`2cce156eee24587be8c5d9dea04662924801da7d`). The accepted hard cut transfers
the sole assembled full-blob buffer into its output slot instead of cloning
the complete payload. Duplicate result slots and buffers retained as flat
delta bases keep their established independent ownership.

The storage format, public API, authentication, chunker, manifest layout,
adapter calls, and durable authority are unchanged. This lane did not change
binary-CAS reclamation, upload/receipt handling, ForkTree/content-defined
chunking, or SlateDB startup GC.

## Dominant term and complexity

Let `N` be output bytes, `C` the number of 1 MiB fixed chunks, and `K` the
chunks intersecting a requested range.

| Operation | Before | After / lower bound |
| --- | --- | --- |
| full read | `Theta(C + N)` backend/authentication work plus a second `N`-byte post-assembly clone | `Theta(C + N)` with ownership transfer; `Omega(N)` output and authentication remain |
| fixed-layout range read | `Theta(K + output)` | unchanged; only selected manifest and authenticated chunk rows are read |
| branch without edit | `O(1)` in payload size | unchanged |
| diff / merge / checkpoint | `O(D)` semantic/commit work, independent of payload bytes | unchanged |
| reopen then full read | metadata-only reopen plus `Theta(C + N)` read | unchanged asymptotically; one `N` allocation/copy removed |

The eliminated term's perfect ceiling is exactly one complete `N`-byte
allocation and copy. At 512 MiB the measured allocation ceiling was 33.3% on
RocksDB (about `3N` before) and 25.0% on SlateDB (about `4N` before). The cut
reaches that ceiling: the respective shapes become about `2N` and `3N`.
The remaining buffers include backend-owned bytes, authenticated assembly,
and the public output; removing them requires a streaming public result and is
outside this existing-layout lane.

The pre-cut route was already structurally sound: one manifest range scan and
one logical multi-get for all unique payload chunks. RocksDB documents that
batching keys in `MultiGet` reduces dispatch/cache overhead and can pipeline
I/O: <https://github.com/facebook/rocksdb/wiki/MultiGet-Performance>.
Object-store guidance likewise favors byte ranges and bounded concurrent
range requests for large objects:
<https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-guidelines.html>.
The existing public range reader follows that practice; the accepted change
therefore removes local duplicate ownership rather than adding a second
fetch/index/cache path.

## Workload and correctness

The ignored release oracle is
`tests/large_payload_read_qualification.rs`. Opaque binary bytes have identical
engine treatment regardless of image, audio, archive, or video extension, so
the focused 64 MiB and 512 MiB cells cover those representative payload
classes without multiplying a content-type matrix. Every cell uses
deterministic authenticated resumable bytes, 16 MiB upload parts, 1 MiB CAS
chunks, and a 4 KiB localized edit. Setup is excluded from read timings.

Each RocksDB and SlateDB cell validates:

1. first and warm whole-file reads with full BLAKE3 verification;
2. 4 KiB public ranges before and after reopen;
3. checkpoint and branch without payload duplication;
4. branch range/full reads and stable content identity;
5. one authenticated localized edit, historical diff, merge, and checkpoint;
6. flush/drop/reopen, exact bytes, content identity, CAS layout, and ownership.

All 24 exact-main/candidate repeated lifecycle runs passed (three repetitions
per adapter/size/revision), as did all four initial profiling cells. Branch,
merge, and checkpoints added no CAS rows or bytes. Final 512 MiB cells on both
adapters retained 513 unique payload chunks, two manifests, 1 GiB of logical
references, 537,923,079 encoded payload bytes, and 538,033,821 total encoded
CAS bytes. Every read hash and range boundary matched.

## Three-run medians

Wall times are setup-excluded. Allocation bytes use the qualification
binary's counting allocator. `HWM` is the median process high-water mark for
the complete lifecycle cell.

| Adapter | MiB | operation | wall before -> after | allocation before -> after |
| --- | ---: | --- | ---: | ---: |
| RocksDB | 64 | first full | 103.870 -> 98.414 ms (-5.3%) | 202.28 -> 135.16 MB (-33.2%) |
| RocksDB | 64 | warm full | 102.346 -> 97.195 ms (-5.0%) | 201.80 -> 134.68 MB (-33.3%) |
| SlateDB | 64 | first full | 32.331 -> 26.182 ms (-19.0%) | 269.99 -> 202.87 MB (-24.9%) |
| SlateDB | 64 | warm full | 29.204 -> 25.552 ms (-12.5%) | 269.02 -> 201.91 MB (-25.0%) |
| RocksDB | 512 | first full | 708.043 -> 670.475 ms (-5.3%) | 1,613.22 -> 1,076.28 MB (-33.3%) |
| RocksDB | 512 | warm full | 674.773 -> 649.286 ms (-3.8%) | 1,612.67 -> 1,075.75 MB (-33.3%) |
| SlateDB | 512 | first full | 248.632 -> 203.756 ms (-18.1%) | 2,152.61 -> 1,615.65 MB (-24.9%) |
| SlateDB | 512 | warm full | 228.324 -> 195.578 ms (-14.3%) | 2,150.02 -> 1,613.08 MB (-25.0%) |

| Adapter | MiB | lifecycle HWM before -> after | CPU ticks, first full before -> after |
| --- | ---: | ---: | ---: |
| RocksDB | 64 | 653,824 -> 520,320 KiB (-20.4%) | 10 -> 10 |
| SlateDB | 64 | 554,344 -> 455,904 KiB (-17.8%) | 3 -> 3 |
| RocksDB | 512 | 3,679,460 -> 2,789,100 KiB (-24.2%) | 72 -> 67 |
| SlateDB | 512 | 3,211,436 -> 2,388,696 KiB (-25.6%) | 25 -> 21 |

RocksDB's large-cell latency remains dominated by backend checksum and chunk
authentication, so this is accepted there as a major allocation/RSS win, not
as a greater-than-10% latency claim. SlateDB exceeds 10% on latency and has the
same resource win.

## Backend work, disk, and controls

The 512 MiB first read is identical before and after: 539 logical keys and
536,875,468 returned value bytes. Warm reads use 533 keys and 536,875,219
value bytes. SlateDB reads the same approximately 512 MiB from the same object
set; RocksDB issues the same single payload multi-get. The change performs no
writes, so backend bytes, object counts, final disk, and CAS encoding are
neutral.

Three-run medians for branch, diff, merge, and checkpoint stayed within 5% in
the 512 MiB gate on both adapters. Ranges execute `load_ranges_many`, not the
changed full-read branch; their allocation and backend work are byte-for-byte
unchanged. One post-reopen SlateDB 4 KiB range moved from 0.894 to 1.186 ms
while other range placements were neutral or faster. With identical code,
one object, 1,048,599 backend bytes, and about 4.02 MB allocation, this is
classified as unconfirmed sub-millisecond timing noise rather than a critical
regression.

A 64 MiB whole-lifecycle `perf` profile attributed 12.35% self and 20.61%
inclusive cycles to `memmove`; RocksDB CRC32C, chunk/blob authentication, and
backend I/O were the other dominant terms. Exact-main and candidate repeated
logs have SHA-256
`17a3306697d9f56a46eccc88e42ac0744c528ddf4dac43692354153373e46cd3`
and `a3662a115722435363e83fd9b1dc657d266349eee65c5a1d73b894a0e1e6a9c5`.

## Commands

```text
LIX_MEDIA_QUAL_BACKEND=<rocksdb|slatedb> LIX_MEDIA_QUAL_MIB=<64|512> \
  cargo test --release -p lix_benchmarks \
  --test large_payload_read_qualification \
  --features storage-benches,slatedb -- \
  --ignored --nocapture --test-threads=1

cargo test -p lix binary_cas::kv::tests:: --lib --features storage-benches
cargo test -p lix --features all-simulations binary_cas::kv::tests:: --lib
cargo test -p lix --features all-simulations session::media_upload::tests:: --lib
cargo test -p lix --features all-simulations \
  session::execute::tests::exact_batch_file_read_returns_each_matching_file_once --lib
cargo test -p lix --features all-simulations \
  session::execute::tests::late_file_content_read --lib
cargo fmt --all -- --check
cargo clippy --profile test --workspace --all-targets --all-features -- -D warnings
```
