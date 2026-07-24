# Small Binary CAS Optimization Log

## Inline manifests through 32 KiB

Date: 2026-07-23

This entry compares the former three-row small-blob layout with an inline
manifest. It measures the production binary CAS API directly through
`StorageAdapter`; it is not a `lix-server` end-to-end benchmark.

The cutoff is deliberately 32 KiB. That is the largest measured payload, and
the RocksDB benefit decreases as payload size grows. Blobs from 32 KiB + 1
through 64 KiB retain the existing single-chunk fast path.

### Method

- Backends: RocksDB and SlateDB.
- Payloads: deterministic high-entropy 4 KiB and 32 KiB byte strings.
- Operations: a new-content write, a repeat-same-content write, and a hot read.
- Seven counterbalanced baseline/candidate process pairs per exact case.
- Each process used a fresh temporary database, 300 warmups, and 3,000 timed
  samples.
- Values below are the median per-run p50. Improvement is the median of the
  seven paired percentage changes, so it need not equal the percentage
  calculated from the two independently reported medians.
- Host: 16-vCPU AMD EPYC-Genoa KVM guest, Linux x86-64.
- Write baseline tree: the nanosecond benchmark commit immediately before the
  inline-layout change.
- Write baseline executable SHA-256:
  `3e24f816521361da3a07c0d3c51cbf658b8e195d7de700ba3d7b61f5e73acb69`.
- Write candidate executable SHA-256:
  `85c0583c6a7a2fdca7a25fa7d9a1ddd32d898f98bcc5f87ed47c6755a33b06b2`.
- Read baseline tree: that same pre-inline benchmark with this log commit's
  fixture-only byte validation applied.
- Read baseline executable SHA-256:
  `ca6ecfe0008ed0446273ca65e0a74d88cae9797215d3fc5dfb55ee89dec412f3`.
- Read candidate executable SHA-256:
  `156b82974cb2293dde870f027155d1c70c8620ce9ae8a46d425b45acf44b3d53`.
- The read fixture validates the seeded bytes once before warmup; the timed
  operation includes storage read, decode, allocation, and destruction, but
  excludes the full-byte correctness comparison.
- Combined raw-result SHA-256:
  `d76a7a73b0690e81443d064ea2ef25fac033cc3f04f710b812fd56f6c11e4f3b`.

Run one exact case with:

```sh
LIX_SMALL_BLOB_BACKENDS=slatedb \
LIX_SMALL_BLOB_SIZES_KIB=4 \
LIX_SMALL_BLOB_OPERATIONS=unique_write \
LIX_SMALL_BLOB_WARMUPS=300 \
LIX_SMALL_BLOB_SAMPLES=3000 \
cargo bench -p lix_engine --features storage-benches,slatedb \
  --bench small_blob_cas
```

### Results

| Backend | Size | Operation | Baseline p50 | Inline p50 | Paired improvement |
| ------- | ---: | --------- | -----------: | ---------: | -----------------: |
| RocksDB | 4 KiB | New-content write | 20,570 ns | 17,030 ns | +17.2% |
| RocksDB | 4 KiB | Repeat write | 6,690 ns | 16,740 ns | -150.4% |
| RocksDB | 4 KiB | Hot read | 5,820 ns | 3,880 ns | +33.0% |
| RocksDB | 32 KiB | New-content write | 79,341 ns | 75,670 ns | +4.9% |
| RocksDB | 32 KiB | Repeat write | 9,260 ns | 75,000 ns | -713.8% |
| RocksDB | 32 KiB | Hot read | 9,930 ns | 4,070 ns | +58.5% |
| SlateDB | 4 KiB | New-content write | 155,429 ns | 78,749 ns | +36.3% |
| SlateDB | 4 KiB | Repeat write | 154,759 ns | 114,730 ns | +29.7% |
| SlateDB | 4 KiB | Hot read | 138,420 ns | 81,760 ns | +33.9% |
| SlateDB | 32 KiB | New-content write | 170,299 ns | 120,779 ns | +32.6% |
| SlateDB | 32 KiB | Repeat write | 177,149 ns | 168,110 ns | +7.4% |
| SlateDB | 32 KiB | Hot read | 141,179 ns | 89,940 ns | +27.8% |

The layout changes a unique small blob from three logical rows to one, its key
bytes from 108 to 36, its write-time presence lookup from one to zero, and its
read point phases from two to one. Logical value bytes are nearly flat: 4,140
to 4,106 bytes at 4 KiB and 32,815 to 32,780 bytes at 32 KiB. Physical
SST/WAL bytes were not measured.

### Tradeoff

RocksDB repeat-same-content writes regress because the old layout probes the
presence row and rewrites only the small manifest, while the inline layout
rewrites the whole value. This change is justified only when new or changed
file content plus reads represent the dominant path. If repeat writes of
identical content are common, do not treat this optimization as a net win
without a representative workload measurement.
