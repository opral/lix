# EXP-HOTCOLD-04 — authenticated hot/cold scalar layout

Status: **qualified no-win**. This is an additive model only; it is not wired to
production and must not be promoted.

## Immutable inputs

- Schema-v1 baseline: `dc4f42917937150fa20fcb7517c46c21d1840045`
  (tree `6b4b9e14eb95dfa5fb5fc7046cf169c12f4813e1`).
- Approved C2 control: `b384d051b4ae888ba84cadcd5f9da719deb0f5f8`
  (tree `f1f525a39ff17287f476b0337cfa326be4f09bd9`).
- The C2 encoder, content-defined page partitioner, envelope, and authenticated
  internal tree are copied from that exact control. Both geometries consume the
  same generated canonical `SVT1` tuple bytes.

## Candidate

Each leaf has one authenticated prefix-compressed PK directory. The directory
binds ordered keys, canonical row fingerprints, fixed/variable lengths, and the
ObjectIds of two separately authenticated scalar segments. Verification checks
object domain, layout fingerprint, key order/distinctness/bounds, segment
identity, row counts and lengths, then reconstructs each canonical tuple and
checks its row fingerprint before returning any result.

The fixed segment contains canonical int8, float8, boolean, timestamptz and UUID
bytes. Ordinary text and declared JSONB bytes are retained in the variable
segment. There is no JSON row snapshot, compatibility decoder, fallback, cache,
second writer, or second authority.

## Matrix

The optimized full sweep ran one isolated process for every combination of:

- PK: integer, UUID, text, composite;
- row: narrow, wide;
- JSONB: absent, sparse, dense;
- N: 1K, 10K, 50K, 100K;
- geometry: approved C2, HotCold.

That is 192 full cells. A 32-cell crossover subset ran three repetitions for
p50/p95 stability. Operations in every model cell include point hit, bounded 1%
range, one-column projection, all-column scan, and spread updates D=1/100/1000.
Object-set mutation accounting is content-addressed and branch sharing checks
that unchanged roots/pages/segments remain byte-identical. Representative 10K
narrow and wide closures were persisted, flushed, dropped, reopened and fully
authenticated on RocksDB and SlateDB.

## Result

Ratios below are HotCold / C2; lower is better.

| Metric | Narrow median | Wide median | Decision impact |
|---|---:|---:|---|
| full point bytes | 1.204 | 1.042 | regression |
| 1-column projection bytes | 1.098 | **0.818** | wide-only win |
| all-row scan bytes | 1.705 | 1.147 | critical regression |
| settled/model bytes | 1.705 | 1.147 | critical regression |
| sparse update bytes | 1.043 | 1.015 | regression |
| point CPU | 1.244 | 1.158 | regression/noisy |
| scan CPU | 1.833 | 1.357 | critical regression |
| sparse update CPU | 2.393 | 2.038 | critical regression |

Representative repeated 10K wide/text/dense p50/p95:

- projected bytes: 7,736 vs C2 10,472 (`0.739x`);
- full point bytes: 9,386 vs 10,472 (`0.896x`);
- point CPU: 30/35 us vs 33/36 us (`0.909x`/`0.972x`);
- scan CPU: 26,887/29,046 us vs 19,700/22,018 us
  (`1.365x`/`1.319x`);
- sparse-update CPU: 99,793/102,791 us vs 49,158/61,435 us
  (`2.030x`/`1.673x`).

Representative settled closure bytes after cold reopen:

| Fixture | Backend | C2 | HotCold | Ratio |
|---|---|---:|---:|---:|
| 10K narrow/integer/no JSONB | RocksDB | 547,123 | 945,614 | 1.728 |
| 10K narrow/integer/no JSONB | SlateDB | 500,908 | 943,935 | 1.884 |
| 10K wide/text/dense JSONB | RocksDB | 3,819,429 | 4,512,787 | 1.182 |
| 10K wide/text/dense JSONB | SlateDB | 3,815,959 | 4,669,634 | 1.224 |

Median process RSS was 79,486 KiB vs C2 75,110 KiB. At 100K the observed
maxima were 746,644 KiB vs 715,276 KiB.

The separate segments make projected wide reads cheaper, but repeat directory
fingerprints and per-page segment envelopes, increase leaf/object count, and
force directory plus one segment plus the authenticated internal path to change
for a scalar update. Under OLTP-first scoring the scan/update regressions reject
the candidate before VCS/OLAP/bytes tie-breakers.

## Correctness gates

Fresh-build and cold-reopen authentication passed. Negative controls reject:

- missing fixed segment;
- swapped fixed/variable ObjectIds;
- duplicate segment ObjectIds;
- row-fingerprint substitution;
- truncated segment bytes;
- malformed domain/layout/bounds/directory and branch-root substitution.

C2 compression allocation bounds, content-defined split bounds, local
insert/delete page stability, and branch root sharing remain green.

## Evidence

Host evidence root: `/root/repos/lix-evidence/exp-hotcold-04-b384`.
The final handoff records the immutable source/tree identity, executable SHA256,
build/check logs, per-cell CSV and `/usr/bin/time -v` logs, and recursive
checksums. No runtime claim is made for production because this is a model-only
experiment.
