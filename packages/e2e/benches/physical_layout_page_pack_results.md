# EXP-PAGE-PACK-16 result

Verdict: **qualified crossover NO-WIN**. Do not compose. This is global
rejection #14/20.

## Immutable inputs and candidate

- Approved Schema-v1 baseline: `dc4f42917937150fa20fcb7517c46c21d1840045`
  (tree `6b4b9e14eb95dfa5fb5fc7046cf169c12f4813e1`).
- Approved C2 parent/model: `b384d051b4ae888ba84cadcd5f9da719deb0f5f8`
  (tree `f1f525a39ff17287f476b0337cfa326be4f09bd9`).
- Logical C2 leaves retain the fixed 4 KiB narrow / 16 KiB wide policy and
  canonical content-defined boundaries.

The candidate groups 2/4/8/16 adjacent logical leaves, or a canonical
content-defined group capped at 16 leaves and 240 KiB decoded bytes, into one
authenticated `LXPK` object. The pack is the sole physical owner: unpacked C2
leaves are absent from storage. Its ordered directory binds every logical leaf
ObjectId/domain, first/last fence, offset, length, and row count to concatenated
leaf bytes. Grouping uses only ordered leaf fences and fixed constants.

There is no cache, raw shortcut, fallback, compatibility format, second
authority, or dual writer.

## Bounded crossover

Three isolated-process repeats covered integer/text PKs, narrow/wide schemas,
absent/dense JSONB, N=1K/10K, D=1/100/1000, C2 and every pack factor: 288
process cells. Median deltas relative to C2 were:

| Geometry | Total bytes | Point bytes | Update bytes | Scan CPU | Update CPU |
|---|---:|---:|---:|---:|---:|
| pack2 | +2.6% | **+19.1%** | **+19.5%** | -78.9% | **+29.8%** |
| pack4 | +2.0% | **+76.7%** | **+41.2%** | -87.8% | **+20.6%** |
| pack8 | +1.6% | **+185.5%** | **+81.0%** | -92.9% | **+15.9%** |
| pack16 | +1.3% | **+436.8%** | **+111.7%** | -94.2% | **+15.4%** |
| content-defined | +1.7% | **+402.1%** | **+31.9%** | -89.9% | **+18.3%** |

Packing strongly reduces scan object hashing/decompression overhead, but every
factor broadens important point reads and sparse writes beyond the 5% critical
regression ceiling. Pack2 is the Pareto candidate and still increases median
point bytes by 19.1%, update bytes by 19.5%, and update CPU by 29.8%. The full
matrix was therefore intentionally not run.

## Correctness and adapters

- Pack/leaf ObjectId mismatch, swapped leaf identity, wrong fence, directory
  gap/overlap, truncation, payload mutation, root substitution, and declared
  decompression/length bomb fail closed.
- Inner C2 domain/fingerprint/directory validation remains mandatory.
- Failed validation leaves the immutable store unchanged; there is no cache to
  populate.
- Canonical insertion order, one physical leaf-to-root update path, branch
  sharing, and sparse-diff object identity controls pass.
- C2, factors 2/4/8/16, and content-defined packs all pass RocksDB and SlateDB
  write/flush/drop/cold-reopen authentication.

## Evidence

Evidence is frozen under
`/root/repos/lix-evidence/exp-page-pack-16-b384/`:

- `crossover/manifest.tsv`, per-cell CSV/time files, and `SHA256SUMS`;
- `controls.csv`;
- paired backend CSVs for C2 and all five pack geometries.

No independent reviewer was spawned because the candidate did not win.
