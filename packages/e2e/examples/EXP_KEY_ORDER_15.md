# EXP-PREDICATE-DIRECTORY-16

Status: **QUALIFIED NO-CUT (ForkTree model); frozen for reference**.

The filename is retained because this experiment deliberately reuses the
accepted EXP-KEY-ORDER-15 real-adapter scaffold. It is not a continuation of
the key-order experiment.

## Anchor and authority

- Parent: `604b1f59d0331ee6278e8c67640a4520709a0fd7`
  (`origin/codex/native-state-cardinality-summary-cc3`).
- Parent tree: `f3b45758d733f0c00692a3d98a8c59bb47ca4202`.
- Baseline and candidate state pages are byte-identical and remain the sole
  tuple-value authority.
- The candidate selected state root additionally binds one immutable typed
  predicate catalog. Catalog pages map canonical int8 ranges to state-row
  handles; every selected row is reauthenticated from the canonical state
  page before output.
- No cache, probabilistic authority, fallback reader, alternate value writer,
  JSON scalar encoding, or second state geometry is present.

## Model geometry

The first honest model used 256-value directory pages. A 64-process
alternating matrix (baseline/candidate then candidate/baseline) found the
expected range win but also a real SlateDB D=1 update regression from the
extra object population. The final bounded cell used one deterministic 16 KiB
policy: 1,024 fixed-width `(typed_value, state_key)` entries per page. Empty
ranges are represented canonically by the catalog; catalogs reject gaps and
overlaps.

At 100K rows, 20 samples, final 1,024-entry geometry:

| Backend / operation | Baseline p50/p95 us | Candidate p50/p95 us | Ratio p50/p95 |
| --- | ---: | ---: | ---: |
| Rocks range100, D=1 | 59160 / 62184 | 1779 / 1804 | 0.030 / 0.029 |
| Slate range100, D=1 | 60462 / 60771 | 1950 / 2035 | 0.032 / 0.033 |
| Rocks update, D=1 | 84 / 89 | 83 / 94 | 0.988 / 1.056 |
| Slate update, D=1 | 151 / 155 | 160 / 168 | 1.060 / 1.084 |
| Rocks update, D=1% | 44634 / 47626 | 44902 / 47359 | 1.006 / 0.994 |
| Slate update, D=1% | 46978 / 47528 | 46967 / 48351 | 1.000 / 1.017 |
| Rocks full, D=1 | 45972 / 46075 | 45698 / 46025 | 0.994 / 0.999 |
| Slate full, D=1 | 47584 / 48345 | 47934 / 48124 | 1.007 / 0.995 |
| Rocks point, D=1 | 33 / 35 | 34 / 35 | 1.030 / 1.000 |
| Slate point, D=1 | 40 / 41 | 40 / 43 | 1.000 / 1.049 |

The range objective clears by roughly 97% on both adapters. Point and full
scan are neutral. The candidate nevertheless fails the strict `<5%`
point/update/full guardrail because Slate D=1 update regresses 6.0% p50 and
8.4% p95 (and Rocks D=1 p95 is 5.6% slower). Therefore this ForkTree-side
experiment is frozen as NO-CUT rather than promoted.

## Correctness

All completed RocksDB and SlateDB cells preserved logical digests. The model
passed cold reopen and fail-closed controls for missing canonical state pages,
wrong state child/root substitution, malformed directory catalog gaps and
overlaps, substituted/truncated directory pages, duplicate/unordered entries,
typed range escape, and state-value mismatch. The selected root atomically
binds the derived catalog; the catalog cannot serve tuple values.

## Evidence

- 256-value alternating raw matrix: `evidence/exp-predicate-directory-16/raw/`
- Combined reduction SHA-256:
  `92bfa0b03309da711ada84034519100292be115c842f58f3672bdbf55478decd`
- Ratio table SHA-256:
  `07ab4b2a695196786be295295c0895b175b95c55048cd641c85e7759af978772`
- Final 1,024-entry 100K log SHA-256:
  `283051e2febb57afbb329397725d5277de33357d449da601c25e1cc45773244c`

## Decision and handoff

Do not implement this model on ForkTree. The manager redirected optimization
to exact main `d2c634b2aeb780aff46013ec04902fcbb5c6f846`, where the existing native
current-state index and adapter boundary must be profiled first. This frozen
model remains evidence that a derived typed predicate directory can remove
about 97% of selective-range latency, and that object population/transaction
maintenance—not predicate lookup—is the update guardrail to solve on main.
