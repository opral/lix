# EXP-PAX-13 result

Verdict: **qualified crossover NO-WIN**. Do not compose this experiment. The
global consecutive no-win streak advances from 11/20 to 12/20.

## Immutable inputs

- Schema-v1 baseline: `dc4f42917937150fa20fcb7517c46c21d1840045`
  (tree `6b4b9e14eb95dfa5fb5fc7046cf169c12f4813e1`).
- Approved C2 parent/model: `b384d051b4ae888ba84cadcd5f9da719deb0f5f8`
  (tree `f1f525a39ff17287f476b0337cfa326be4f09bd9`).
- Fixed page policy: 4 KiB narrow and 16 KiB wide.

## Candidate

PAX keeps C2's one schema-partitioned authenticated slotted page and sorted PK
directory. Inside that page it stores native non-PK columns contiguously:
`int8`, `float8`, boolean bitmap, `timestamptz`, UUID, text varlen minipages,
and a distinct declared JSONB varlen minipage. Each minipage has an authenticated
NULL bitmap and fixed-width bytes or restart offsets. Per-row bindings bind the
ordered columns back to the PK and canonical Schema-v1 tuple.

There are no sidecar/column objects, JSON row snapshots, fallback readers,
compatibility formats, caches, second authorities, or dual writers. Full-row
points still read one leaf page.

## Bounded paired crossover

Three isolated-process repeats covered integer/text PKs, narrow/wide schemas,
absent/dense JSONB, N=1K/10K, D=1/100/1000, and both C2/PAX: 96 process cells
and 48 paired scenario/D comparisons.

| Metric | Median delta | Best | Worst |
|---|---:|---:|---:|
| total/scan bytes | **+32.95%** | +30.76% | +39.68% |
| point bytes | **+10.27%** | -7.18% | +18.48% |
| update bytes | **+30.37%** | -5.73% | +39.66% |
| point CPU | +17.39% | -30.00% | +114.29% |
| scan CPU | **+33.93%** | -9.57% | +123.11% |
| update CPU | **+110.60%** | +59.10% | +259.64% |

The 1K narrow integer control alone grew from 46,780 to 63,212 authenticated
bytes and from 2,587 to 3,065 point-read bytes. Thus PAX violates the critical
5% OLTP regression ceiling before projection ranking. Although contiguous
minipages can avoid tuple reconstruction for selected columns, that potential
OLAP/projection benefit cannot qualify a layout with these full-point, update,
and write-amplification regressions. The full matrix was intentionally not run.

## Correctness and adapters

- Canonical reconstruction and insertion-order controls pass.
- Domain/fingerprint/bounds, page/key directory, column count/type tag,
  minipage offset, NULL bitmap, cross-column row substitution, duplicate key,
  payload mutation, truncation, root substitution, and decompression-bomb
  controls fail closed.
- One changed row rewrites exactly one leaf-to-root authenticated path.
- RocksDB and SlateDB flush/drop/cold-reopen authentication pass for C2 and
  PAX. No partial result is returned after validation failure.

## Evidence

Evidence is under `/root/repos/lix-evidence/exp-pax-13-b384/`:

- `crossover/manifest.tsv`, per-process CSV/time files, and `SHA256SUMS`;
- `controls.csv`;
- `backend/c2_slotted.csv` and `backend/pax.csv`.

No independent reviewer was spawned because this was not a win.
