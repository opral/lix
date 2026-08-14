# EXP-MPHF-18 result

Verdict: **qualified crossover NO-WIN**. Do not compose. This is global
rejection #18/20, following reconciled PAGE-PACK rejection #17/20.

## Immutable inputs and candidate

- Approved Schema-v1 baseline: `dc4f42917937150fa20fcb7517c46c21d1840045`
  (tree `6b4b9e14eb95dfa5fb5fc7046cf169c12f4813e1`).
- Approved C2 parent/model: `b384d051b4ae888ba84cadcd5f9da719deb0f5f8`
  (tree `f1f525a39ff17287f476b0337cfa326be4f09bd9`).
- Logical C2 pages retain the fixed 4 KiB narrow / 16 KiB wide policy and
  canonical content-defined boundaries.

The candidates add an authenticated deterministic CHD displacement table to
the same C2 leaf. The canonical sorted key/slot directory remains the sole
scan/order representation. The table only proposes a row ordinal; each lookup
reconstructs and compares the complete canonical key before returning a hit.
The 8-bit and 16-bit fingerprint variants use the lowest successful canonical
seed and deterministic largest-bucket-first displacement order. A page omits
the table when construction fails or its bytes are not smaller than the page's
key suffix area.

There is no separate object, cache, fallback reader, second authority, or dual
writer.

## Bounded crossover

Three isolated-process repeats covered integer/text PKs, narrow/wide schemas,
absent/dense JSONB, N=1K/10K, D=1/100/1000, C2 and both CHD variants: 144
process cells. Median D=1 deltas relative to C2 were:

| Geometry | Total bytes | Point bytes | Point CPU | Build CPU | Update CPU | Update bytes | Scan CPU |
|---|---:|---:|---:|---:|---:|---:|---:|
| CHD + 8-bit fingerprint | +0.07% | +0.02% | -1.77% | **+2128.8%** | **+2645.2%** | +0.11% | -2.06% |
| CHD + 16-bit fingerprint | +0.08% | +0.04% | -2.00% | **+2160.6%** | **+2600.3%** | +0.05% | -2.11% |

The authenticated page hash/decode dominates point lookup, so avoiding binary
search saves less than the required 5%. Deterministic CHD reconstruction for
each changed page makes D=1 updates roughly 27 times slower. This violates the
critical update guardrail by orders of magnitude. The full matrix was therefore
intentionally not run.

## Correctness and adapters

- Altered seed, bucket count, displacement, proposed slot, fingerprint, width,
  directory offset, duplicate key, wrong-key proposal, payload mutation,
  truncation, root substitution, and decompression/length bomb fail closed.
- Every table is reconstructed and compared during structural authentication;
  lookup additionally checks the fingerprint, proposed ordinal, and complete
  canonical key.
- Failed validation leaves the immutable store unchanged; there is no cache to
  populate.
- Canonical insertion order, branch sharing, sparse-diff object identity, and
  the inherited C2 boundary controls pass.
- C2, CHD-8, and CHD-16 pass RocksDB and SlateDB write/flush/drop/cold-reopen
  authentication.

## Evidence

Evidence is frozen under `/root/repos/lix-evidence/exp-mphf-18-b384/`:

- `crossover/manifest.tsv`, 144 per-cell CSV/time files, and `SHA256SUMS`;
- `controls.csv`;
- paired RocksDB/SlateDB backend CSVs for C2, CHD-8, and CHD-16.

No independent reviewer was spawned because the candidate did not win.
