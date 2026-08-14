# EXP-INPAGE-CODEC-08 result

Verdict: **qualified crossover NO-WIN**. This experiment must not be composed
into production. The global no-win streak advances to 11/20.

## Immutable inputs

- Schema-v1 main baseline: `dc4f42917937150fa20fcb7517c46c21d1840045`
  (tree `6b4b9e14eb95dfa5fb5fc7046cf169c12f4813e1`).
- Approved C2 model and parent:
  `b384d051b4ae888ba84cadcd5f9da719deb0f5f8`
  (tree `f1f525a39ff17287f476b0337cfa326be4f09bd9`).
- Identical tuple generator, content-defined page boundaries, branch model,
  storage adapters, and process isolation were used for C2 and InPageCodec.

## Candidate

InPageCodec keeps C2's single schema-partitioned authenticated slotted page and
key directory. It canonically constructs both:

1. compressed raw Schema-v1 tuple bytes; and
2. an in-page scalar codec with frame-of-reference/bit-packed `int8` and
   `timestamptz`, a boolean bitmap, raw floats, UUID common-prefix suffixes, and
   a strict byte-cost-selected raw/dictionary variable area.

The smaller complete authenticated page encoding wins; ties select raw. There
are no sidecar objects, segments, fallback readers, compatibility formats,
caches, second authorities, or dual writers. Decoder allocation remains capped
at 256 KiB.

## Crossover

The bounded crossover ran three isolated-process samples for integer/text PKs,
narrow/wide schemas, absent/dense JSONB, N=1K/10K, D=1/100/1000, and fixed 4
KiB narrow / 16 KiB wide page targets: 96 process cells and 48 paired
scenario/D comparisons.

Median candidate deltas across the paired comparisons:

| Metric | Median | Best | Worst |
|---|---:|---:|---:|
| total bytes | +0.05% | -0.96% | +0.73% |
| point bytes | +0.05% | -18.06% | +2.74% |
| scan bytes | +0.05% | -0.96% | +0.73% |
| update bytes | +0.04% | -15.73% | +2.50% |
| point CPU | 0.00% | -20.00% | +125.00% |
| scan CPU | +1.23% | -17.52% | +117.10% |
| update CPU | **+15.85%** | -5.33% | **+155.07%** |

The isolated narrow integer 10K point leaf improved bytes by 18.1%, but total
tree bytes improved only 0.6%, scan CPU regressed 5.3-10.0%, and update CPU
regressed 28.6-40.5%. Wide layouts normally stayed within about 0.2% bytes and
still showed repeated update regressions above 5%. These are critical OLTP
regressions under the experiment's lexicographic score, so the candidate was
disqualified before the full 192-cell matrix.

## Correctness and adapters

- Canonical scalar round trip passes even where byte-cost correctly selects the
  smaller raw page.
- Malformed scalar width, variable codec/dictionary tag, page slot offset,
  schema fingerprint/domain/bounds, truncation, payload mutation, root-link
  substitution, and decompression bomb are rejected.
- Insertion-order canonicality and hash-pruned one-leaf-to-root branch rewrite
  controls pass.
- Paired RocksDB and SlateDB write/flush/drop/cold-reopen authentication pass.
  For the 1K narrow integer control, settled bytes were 113,370 vs 113,082
  (RocksDB C2/candidate) and 52,986 vs 52,817 (SlateDB C2/candidate), less than
  0.4% improvement.

## Evidence

Raw evidence is under
`/root/repos/lix-evidence/exp-inpage-codec-08-b384/`:

- `crossover/manifest.tsv` and per-process CSV/time files;
- `controls.csv`;
- `backend/c2_slotted.csv` and `backend/inpage_codec.csv`;
- generated `SHA256SUMS` files.

No independent reviewer was spawned because the candidate did not win.
