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
cargo bench -p lix_benchmarks --features storage-benches,slatedb \
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

## Manifest-probed inline values through 64 KiB

Date: 2026-07-29

OpenClaw's complete `main` object set contains 455,883 unique blobs. Of those,
43,444 blobs (9.5%, 1,991,214,838 logical bytes) fall above 32 KiB and at or
below 64 KiB. The previous layout stored each of these as a manifest, an empty
presence row, and a payload row.

The extended layout stores that band as one inline manifest. Reads finish from
the manifest point read, while repeat writes use a key-only manifest probe and
stage no value. Payloads through 32 KiB retain the original unguarded one-row
layout.

Seven counterbalanced baseline/candidate process pairs used fresh temporary
databases per exact case, 300 warmups, and 3,000 timed samples. The values are
the median per-run p50:

| Backend | Operation | Baseline p50 | Inline p50 | Change |
| ------- | --------- | -----------: | ---------: | -----: |
| RocksDB | New-content write | 139,402 ns | 135,556 ns | 2.8% faster |
| RocksDB | Repeat write | 13,459 ns | 13,169 ns | 2.2% faster |
| RocksDB | Hot read | 4,276 ns | 3,685 ns | **13.8% faster** |
| SlateDB | New-content write | 72,400 ns | 66,161 ns | 8.6% faster |
| SlateDB | Repeat write | 28,418 ns | 12,636 ns | **55.5% faster** |
| SlateDB | Hot read | 5,208 ns | 4,636 ns | **11.0% faster** |

The logical shape drops from three rows and 96 key bytes to one row and 32 key
bytes per unique blob in the extended band, both 66.7% reductions. Encoded
value bytes remain nearly flat (65,583 to 65,548 bytes for the measured
high-entropy 64 KiB payload).

The exact-case binaries had SHA-256
`335b84b9ef6ae399b999613ef49f095684f313411f761b877a9a696f4422e815`
(baseline) and
`eb0cbbd5d2f1a6acf11589c73175fb8a326f0a0a6eb31353dc4e0bd2097da8e4`
(candidate). The combined raw-result SHA-256 is
`86f828358d38bff5f4a82d3a0bd956d079b2162efa9e7e2823db4d24e88594e3`.

## Inline manifests through 128 KiB

Date: 2026-07-29

OpenClaw has another 23,049 unique blobs (2,088,120,165 logical bytes) above
64 KiB and at or below 128 KiB. Extending the manifest-probed inline layout to
this band again replaces one manifest, payload, and presence row with one
manifest row.

Seven counterbalanced baseline/candidate process pairs used fresh temporary
databases per exact case, 300 warmups, and 3,000 timed samples. The values are
the median per-run p50:

| Backend | Operation | 64 KiB limit p50 | 128 KiB limit p50 | Change |
| ------- | --------- | ---------------: | ----------------: | -----: |
| RocksDB | New-content write | 226,571 ns | 231,737 ns | 2.3% slower |
| RocksDB | Repeat write | 22,230 ns | 24,293 ns | 9.3% slower |
| RocksDB | Hot read | 5,938 ns | 4,917 ns | **17.2% faster** |
| SlateDB | New-content write | 111,402 ns | 106,846 ns | 4.1% faster |
| SlateDB | Repeat write | 35,839 ns | 21,460 ns | **40.1% faster** |
| SlateDB | Hot read | 6,970 ns | 6,249 ns | **10.3% faster** |

The logical shape drops from three rows and 96 key bytes to one row and 32 key
bytes per unique blob in this band. Encoded value bytes remain nearly flat
(131,119 to 131,084 bytes for the measured high-entropy 128 KiB payload).

The exact-case binaries had SHA-256
`29725d7f58f0ce28830e958bd0186756b8d5ff28e08a3cafbd057e7f79786ccf`
(baseline) and
`3e2d86c24a18491b352bbcd77d8a0c6417ab0d9fd881efe8a209d52799500866`
(candidate). The combined raw-result SHA-256 is
`e78b2dcd263addb7cff669f43779c9c24a86828d4b407ce3e0ab82e2862bc174`.

### Rejected 256 KiB extension

The same seven-pair protocol tested extending inline manifests through
256 KiB. This is rejected because it moves the larger value into the manifest
space and materially regresses RocksDB:

| Backend | Operation | 128 KiB limit p50 | 256 KiB limit p50 | Change |
| ------- | --------- | ---------------: | ----------------: | -----: |
| RocksDB | New-content write | 412,075 ns | 413,457 ns | 0.3% slower |
| RocksDB | Repeat write | 39,174 ns | 48,827 ns | **24.6% slower** |
| RocksDB | Hot read | 10,473 ns | 12,798 ns | **22.2% slower** |
| SlateDB | New-content write | 204,020 ns | 203,700 ns | 0.2% faster |
| SlateDB | Repeat write | 52,703 ns | 43,640 ns | **17.2% faster** |
| SlateDB | Hot read | 8,581 ns | 8,151 ns | 5.0% faster |

The exact-case binaries had SHA-256
`1ead339d48e6f3e351e88376eebdfc58031c4fd6bbf673c4d0719d6c7e65b949`
(baseline) and
`7224531462d84fd4d87d0928719ca3bf5caab195c1ab36e3eaa8950117a9a3f1`
(candidate). The combined raw-result SHA-256 is
`add3c863a4e22fb4494a9eb4f327230939d388e2870ad1c9452c3ee542f0e39e`.

### OpenClaw end-to-end check

The storage-neutral Git replay profiler from PR #925 replayed OpenClaw commit
`c5c50a2141f2cdd805ae9b70a14a2e66dabac9b6` with all text, CSV, and Markdown
plugins enabled. This commit changes 3,423 paths and eagerly persists 559,701
semantic changes. Three counterbalanced pairs compared #925 alone with #925
plus the 64 KiB and 128 KiB layout changes:

| Backend | Metric | Baseline median | Layout stack median | Change |
| ------- | ------ | --------------: | ------------------: | -----: |
| RocksDB | Timed replay | 17,830.306 ms | 17,919.724 ms | 0.5% slower |
| RocksDB | Parent bootstrap | 1,488.259 ms | 1,416.860 ms | 4.8% faster |
| RocksDB | Final flush | 1,720.548 ms | 1,729.472 ms | 0.5% slower |
| SlateDB | Timed replay | 17,618.113 ms | 17,823.678 ms | 1.2% slower |
| SlateDB | Parent bootstrap | 1,388.375 ms | 1,387.330 ms | 0.1% faster |
| SlateDB | Final flush | 4,410.714 ms | 4,576.873 ms | 3.8% slower |

Final physical size was effectively flat: RocksDB changed from 136,500,209 to
136,503,305 bytes and SlateDB from 341,107,956 to 341,168,511 bytes. This
confirms that plugin reconciliation and validation, rather than binary CAS
layout, dominate this pathological commit.

The exact replay binaries had SHA-256
`9b99f9ac44bd4e3c06913fadc375d5bba1d6c65039d7d9f01ceb0635e210f19b`
(baseline) and
`eb4df3645df61d65213e706034efebe39db92b04f68df00f688f24910da2ffca`
(layout stack). The ordered profile-hash manifest SHA-256 is
`23f3b0551cc4b729895871e7b6c5976ee27402182614ab4222807ecb5145473c`.
