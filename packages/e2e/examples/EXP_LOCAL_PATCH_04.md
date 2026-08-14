# EXP-LOCAL-PATCH-04 — qualified NO-WIN

## Verdict

- Ledger: `EXP-LOCAL-PATCH-04`
- Pinned base: `dc4f42917937150fa20fcb7517c46c21d1840045`
- Comparator: exact C2 immutable 64-row slotted pages
- Candidate: C2 base pages plus at most one authenticated sparse patch per leaf
- Backends: shipping RocksDB and SlateDB adapters
- History: 100 commits
- Scale: 1K, 10K, 50K, 100K rows; D=1, 10, 1%
- Distributions: uniform leaves, deterministic random leaves, pathological repeated same key
- Classification: **qualified NO-WIN**; global consecutive no-win streak **4/20**.

The local-patch layout removes global delta/history traversal and delivers large update wins, hot-point wins, structural root diff, and lower write/settled bytes. It nevertheless fails the lexicographic OLTP gate: leaves with a patch require authenticating both patch and base on a miss, causing important cold-point regressions on both adapters. At higher mutation density, p95 update latency also regresses because deterministic patch compaction is visible in the tail.

## Physical and authority contract

The content-addressed root carries aligned vectors of base-page ObjectIds and optional patch ObjectIds. There is no global delta chain, Bloom, cache, fallback, or second authority. A point maps directly to one leaf; if a patch exists, one deduplicated `get_many` authenticates patch and base. The patch is checked first and the base is used only on absence. Full/range reads batch the exact base and patch objects for the selected leaves. Root diff compares `(base_id, patch_id)` pairs and authenticates only differing leaves.

A patch is a canonical sorted typed-row page. The selected deterministic class compacts when the patch reaches 2 entries or 256 encoded bytes. Compaction merges it into that leaf and clears the patch in the same authenticated root. Root hashes therefore include patch identity. Missing/substituted base or patch objects fail content-address authentication before output; cold reopen reconstructs solely from the persisted root.

The tuple generator is unchanged from the prior experiments: canonical Schema-v1 `uuid`, `int8`, `boolean`, `timestamptz`, nullable `text`, never JSON.

## Threshold sensitivity at 1K/H100

Candidate/slotted aggregate ratios; lower is better. Every larger class fails cold-point neutrality more severely.

| entries/bytes | Rocks cold D1/D10 | Slate cold D1/D10 | Rocks update D1/D10 | Slate update D1/D10 |
|---|---:|---:|---:|---:|
| 2 / 256 | 1.039 / 0.991 | **1.094 / 1.146** | 0.681 / 0.623 | 0.695 / 0.804 |
| 4 / 384 | **1.109 / 1.133** | **1.170 / 1.191** | 0.524 / 0.413 | 0.963 / 0.622 |
| 8 / 512 | **1.126 / 1.133** | **1.287 / 1.197** | 0.477 / 0.344 | 0.858 / 0.565 |
| 16 / 1024 | **1.152 / 1.212** | **1.306 / 1.300** | 0.495 / 0.354 | 0.678 / 0.509 |
| 32 / 2048 | **1.246 / 1.288** | **1.297 / 1.360** | 0.576 / 0.492 | 0.837 / 0.637 |

A one-entry threshold degenerates into ordinary slotted path-copying and cannot provide the required >5% architectural win, so 2/256 is the final candidate and the best honest boundary.

## Uniform scale results

Aggregate candidate/slotted ratios:

| Backend | N | D | update | hot point | cold point | range 1K | full scan | root diff |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Rocks | 1K | 1 | 0.699 | 0.673 | **1.060** | **1.187** | 0.972 | **1.143** |
| Rocks | 10K | 10 | 0.618 | 0.746 | **1.061** | 1.013 | 1.005 | 0.996 |
| Rocks | 50K | 1% | 0.599 | 0.928 | **1.133** | 0.991 | 0.979 | 1.033 |
| Rocks | 100K | 1K | 0.579 | **1.091** | **1.193** | **1.055** | 0.988 | 1.033 |
| Slate | 1K | 1 | 0.802 | 0.794 | **1.151** | 0.967 | 0.982 | 1.035 |
| Slate | 10K | 10 | 0.869 | 0.899 | **1.188** | 1.009 | **1.098** | 1.022 |
| Slate | 50K | 1% | 0.787 | 0.995 | **1.156** | 1.008 | 1.005 | 1.032 |
| Slate | 100K | 1K | 0.780 | **1.078** | **1.214** | 1.018 | **1.065** | 1.012 |

The per-operation latency distributions agree with the aggregate rejection. Examples (slotted→candidate):

- Slate 10K/D10 cold point p50 22→26 µs (1.182x), p95 22→27 µs (1.227x).
- Slate 50K/D1 cold point p50/p95 27→31 µs (1.148x).
- Slate 100K/D10 cold point p50 32→36 µs (1.125x), p95 35→39 µs (1.114x).
- Slate 100K/D1K update p50 22.119→17.552 ms (0.794x), but p95 23.136→27.345 ms (**1.182x**).
- Rocks 100K/D10 update p50 230→142 µs (0.617x), but p95 238→258 µs (**1.084x**).

## Distribution controls

Repeated updates to one key are favorable: at 10K, update is 0.506x Rocks/0.779x Slate, hot point 0.415x/0.736x, cold point 0.780x/1.000x, and scan 0.887x/0.980x. This proves the format handles the pathological same-key patch efficiently.

Deterministic random leaves reproduce the rejection rather than an artifact of uniform spacing. At 10K/D100, cold point is 1.110x Rocks and 1.121x Slate; history is 1.127x/1.213x and Slate full scan is 1.091x. Updates remain favorable at 0.670x/0.788x.

## I/O, bytes, CPU and RSS

At 100K/H100/D1%, candidate versus slotted:

| Backend | root bytes | page bytes | settled bytes |
|---|---:|---:|---:|
| Rocks slotted | 5,319,023 | 459,027,034 | 883,519,888 |
| Rocks local patch | 8,074,319 | 238,567,481 | 452,066,619 |
| Slate slotted | 5,319,023 | 459,027,034 | 467,090,790 |
| Slate local patch | 8,074,319 | 238,567,481 | 249,420,770 |

The candidate roughly halves page and settled bytes, but root bytes rise 51.8% from the optional patch slots. Storage is the last decision dimension and cannot mask failed OLTP latency.

The complete uniform matrix consumed 17.84 s user CPU, 2.14 s system CPU, 18.64 s wall (107% CPU), 269,088 KiB peak RSS, and 8,771,416 filesystem output blocks. Backend calls/keys/bytes, puts/bytes, decoded pages/rows, patch reads/compactions/touched leaves, and settled bytes are recorded per operation in the raw log.

## Correctness

All measured cells passed exact point values, typed full/range rows, final map digests, structural changed-key counts, history digest, branch isolation, missing base-page corruption, missing patch corruption, and cold reopen on RocksDB and SlateDB. A Bloom cannot serve values because this format contains no Bloom. No production code or durable format was changed.

## Reproduction and immutable evidence

```sh
CARGO_TARGET_DIR=/root/repos/.target-exp-delta-page-01 \
  cargo build -p lix_e2e --release --example exp_delta_page_01 \
  --features rocksdb,slatedb

EXP_LOCAL_PATCH_MAX_ENTRIES=2 \
EXP_LOCAL_PATCH_MAX_BYTES=256 \
EXP_LOCAL_PATCH_PATTERN=uniform \
EXP_DELTA_PAGE_BACKENDS=rocksdb,slatedb \
EXP_DELTA_PAGE_SIZES=1000,10000,50000,100000 \
EXP_DELTA_PAGE_HISTORIES=100 \
EXP_DELTA_PAGE_DELTAS=1,10,1pct \
EXP_DELTA_PAGE_ROOT=/root/repos/evidence/exp-local-patch-04/final-uniform-h100 \
timeout 1200 \
  /root/repos/.target-exp-delta-page-01/release/examples/exp_delta_page_01
```

- Uniform log SHA-256: `3680b8995254501ef6231c912883ccae00d1ae64a400d4a75bc23705d59fa2ad`
- Random log SHA-256: `b281aacaf3aba565568db02003e9e8489b5fb68065f9d9f2a5f4582164131034`
- Repeated-key log SHA-256: `8a01d6b100dd9181a6cc2c653efcf51ad63879cd50d222ebd099eb22db3682a3`
- Uniform resource log SHA-256: `c9e1ba460aa9ce0ad5e494a225f88d7adec3d1c8585d2abe9f4afe7fb540869c`
- Release binary SHA-256: `6c8af956e0cbb8870ed17ccd96306ac0e29c53775b347dd5613ba2f8499e03e7`

## Conclusion

Page-local patches are substantially better than global sparse-delta chains and remove their history-depth dependence. They still do not beat C2 lexicographically: an occupied patch introduces unavoidable authenticated work for a miss, and dense occupancy creates cold-point and tail-latency regressions above 5%. `EXP-LOCAL-PATCH-04` is frozen as **NO-WIN**, advancing the global streak to **4/20**. No reviewer is spawned and no production cut is recommended.
