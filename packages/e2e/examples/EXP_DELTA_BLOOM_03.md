# EXP-DELTA-BLOOM-03 — qualified NO-WIN

## Identity and decision

- Ledger: `EXP-DELTA-BLOOM-03`
- Comparator: approved immutable 64-row slotted base-page layout
- Candidate: immutable base pages plus deterministic leveled sparse delta pages whose root carries an authenticated per-page Bloom summary
- Source base: `dc4f42917937150fa20fcb7517c46c21d1840045`
- Source tree: `6b4b9e14eb95dfa5fb5fc7046cf169c12f4813e1`
- Backends: shipping RocksDB and SlateDB adapters
- Tuple: canonical Schema-v1 typed tuple (`uuid`, `int8`, `boolean`, `timestamptz`, nullable `text`), never JSON
- Page rows: 64
- History depth: 100
- Sizes: 1K, 10K, 50K
- Mutations per commit: 1, 10, 1%
- Deterministic compaction: accumulated sparse rows at 10% of base cardinality
- Verdict: **qualified NO-WIN** under the lexicographic OLTP gate; global consecutive no-win streak **3/20**.

The candidate wins mutation cost and storage, but it is not OLTP-neutral. At realistic sparse-history density, authenticated Bloom checks and false-positive page loads make hot and cold point reads materially slower. VCS or byte savings are not allowed to override this failure.

## Authority contract

The root content-addresses every Bloom summary together with its page object ID. A Bloom result can only skip a page read. A positive result loads the page, verifies the object ID, decodes the complete page, recomputes the canonical Bloom, and rejects a mismatch before using any value. Multiple positive pages are all authenticated; the Bloom never serves a value and never substitutes for page authentication. There is no cache, fallback, second reader, or alternate authority.

Canonical Bloom classes in the measured candidate are selected from immutable decoded page cardinality: 16 bytes for 1–2 rows, 32 bytes for 3–16 rows, and 64 bytes above 16 rows. Root page-reference cost is `32-byte ObjectId + 4-byte length + Bloom bytes`, or 52/68/100 bytes. The decoder rejects every other size class.

## OLTP gate

Ratios are candidate/slotted wall time. Values above 1.05 fail.

| Backend | N | D | update | hot point | cold point | full readback |
|---|---:|---:|---:|---:|---:|---:|
| RocksDB | 1K | 1 | 0.543 | 0.327 | **1.089** | 0.987 |
| RocksDB | 10K | 10 | 0.278 | 0.678 | **1.159** | 0.978 |
| RocksDB | 10K | 1% | 0.142 | 0.979 | **1.270** | 0.966 |
| RocksDB | 50K | 10 | 0.273 | 0.761 | **1.081** | 1.008 |
| RocksDB | 50K | 1% | 0.135 | **1.375** | **1.757** | 0.968 |
| SlateDB | 1K | 1 | 0.835 | 0.521 | **1.067** | 0.869 |
| SlateDB | 10K | 10 | 0.453 | 0.824 | **1.081** | 0.898 |
| SlateDB | 10K | 1% | 0.179 | 0.969 | **1.191** | 0.973 |
| SlateDB | 50K | 10 | 0.367 | 0.869 | 1.021 | 1.021 |
| SlateDB | 50K | 1% | 0.151 | **1.475** | **1.776** | 0.974 |

At 50K/1%, the Rocks hot-point series decodes 143 pages versus 101 and the cold series decodes 181 versus 101. SlateDB has the same 143/181 page pattern. The candidate performs 1,670 Bloom checks for hot probes and 3,580 for cold probes; 42 and 80 false-positive pages respectively. This is the measured physical owner of the decisive regression.

History lookup is likewise not neutral at 50K/1%: 1.924x RocksDB and 1.773x SlateDB. The current generic endpoint diff is 3–6x slower for sparse deltas; no VCS optimization was attempted after the OLTP gate failed.

## False-positive and root-byte sweep

Each cell uses 100,000 absent probes.

| Entries/page | 16 B | 32 B | 64 B | 128 B |
|---:|---:|---:|---:|---:|
| 1 | 0% | 0% | 0% | 0% |
| 10 | 0.480% | 0.036% | 0.003% | 0.002% |
| 64 | 65.515% | 17.960% | 2.492% | 0.295% |

Increasing the dense-page Bloom from 64 to 128 bytes lowers its synthetic false-positive rate by 8.45x, but expands every dense root reference from 100 to 164 bytes. It cannot address the more fundamental need to inspect every level summary on a miss, and earlier fixed-128/deeper-compaction trials did not satisfy the OLTP gate.

At 50K/H100 the authenticated candidate root bytes versus slotted are:

| D | slotted root | Bloom root | delta |
|---:|---:|---:|---:|
| 1 | 2,585,094 | 2,613,782 | +1.1% |
| 10 | 2,585,094 | 2,685,250 | +3.9% |
| 1% | 2,585,094 | 2,946,134 | +14.0% |

The root overhead buys substantial write reduction, but bytes are the final acceptance dimension and cannot mask failed OLTP latency. At 50K/1%, update-series staged bytes fall from 228,171,035 to 45,612,255 (-80.0%), while settled inventory falls from 436,381,502 to 40,167,026 on RocksDB and from 233,173,252 to 49,397,280 on SlateDB.

## Correctness and reopen

Every measured cell asserts identical point values, full-map digest, expected changed identity count, non-empty history digest, and branch isolation. Cold reopen reopens the same adapter, fully materializes through authenticated roots/pages, and checks the final map digest. A forged root referencing a missing page is rejected before output. Bloom summaries are recomputed from authenticated decoded pages and mismatches return corruption.

This early-stop NO-WIN does not claim exhaustive production-format corruption qualification. The benchmark is an architectural experiment, not a production cutover.

## Reproduction

```sh
CARGO_TARGET_DIR=/root/repos/.target-exp-delta-page-01 \
  cargo build -p lix_e2e --release --example exp_delta_page_01 \
  --features rocksdb,slatedb

EXP_DELTA_PAGE_BACKENDS=rocksdb,slatedb \
EXP_DELTA_PAGE_SIZES=1000,10000,50000 \
EXP_DELTA_PAGE_HISTORIES=100 \
EXP_DELTA_PAGE_DELTAS=1,10,1pct \
EXP_DELTA_PAGE_ROOT=/root/repos/evidence/exp-delta-bloom-03/compact-root-h100 \
timeout 1200 \
  /root/repos/.target-exp-delta-page-01/release/examples/exp_delta_page_01 \
  > /root/repos/exp-delta-bloom-03-compact-root-h100.log 2>&1
```

- Raw log SHA-256: `0e6eed3ed79a349b36d09f959829982fc1481029c6f6c1e7091893a0fe8f2187`
- Release binary SHA-256: `594f2f5526fbd50940013b6490e6134321548fba21b798f459241398d2ea9c17`

## Conclusion

Authenticated Bloom summaries improve the leveled sparse-delta design but do not make it a better OLTP layout than slotted pages. Sparse updates are much cheaper; point and history reads pay history-density-dependent summary/probe costs and false-positive page authentication. `EXP-DELTA-BLOOM-03` is therefore frozen as **NO-WIN**, not promoted, and no production change is recommended.
