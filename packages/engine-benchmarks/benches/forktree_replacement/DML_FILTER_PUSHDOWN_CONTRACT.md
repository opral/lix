# ForkTree Stage-2 SQL filter-pushdown contract

Verdict: **semantic boundary GO; strict performance qualification BLOCKER**.
The model proves the correct one-authority boundary, but the 1K RocksDB gate
retains a +23.689% allocation-call regression and SlateDB retains the known
+85.860% physical-write-byte tradeoff. The required no-regression gate is not
met, so no 10K matrix was run.

## One-authority boundary

1. Lix remains the sole parser, binder, catalog, `RETURNING`, `ON CONFLICT`,
   constraint, batch, savepoint, and physical write-target authority.
2. The existing entity `TableProvider::supports_filters_pushdown` classifies
   canonical primary-key equality/`IN` expressions. Lix lowers only exact
   identities into its structured live-state request.
3. The ForkTree target never receives SQL or a DataFusion `Expr`. It consumes
   only Lix-lowered correlated identities or a residual range request.
4. Exact identities use one transaction-scoped coherent `StorageRead`, batch
   each path level, authenticate every immutable object by `ObjectId`, and
   reuse decoded authenticated nodes/value packs only for that read view.
5. Mixed `OR`, `NULL`, `LIKE`, and other noncanonical predicates remain
   DataFusion residuals. They use the authenticated ordered range iterator;
   the target does not reinterpret them.
6. The read view owns no selector or mutation capability. It is dropped before
   Lix's coalesced postimages are published through one ForkTree root change.

The Memory oracle proves:

- PK equality: exact point pushdown;
- PK `IN`: exact batched point pushdown;
- mixed PK/payload `OR`: authenticated range plus DataFusion residual;
- `IS NOT NULL`: authenticated range plus DataFusion residual;
- noncanonical `LIKE`: authenticated range plus DataFusion residual.

It produced affected-row counts `[1, 2, 1, 0, 0]`, seven point identities,
three broad range requests, and digest
`248233f2695149496b62d2465671dd547bf6c4671ddfb9f6a625656f471f180d`.

The measured 18-statement DML batch retained Lix's exact result digest
`93ca58c1bbfe93ab2d99e323e317b9e0b2441291be25fe64397cb7fdfa88c41e`.
All 15 source/postimage scans became point requests; broad scans fell from 15
to zero. `RETURNING`, PK equality updates, `ON CONFLICT UPDATE/DO NOTHING`,
multirow/default/NULL postimages, ordering, and one atomic publication remained
unchanged. This read-only capability cannot acquire or duplicate #1260's write
target, FK, savepoint, or stale-writer authority.

## Complexity

The rejected bridge was `O(N + S*N + R log_B N + E)`: one full snapshot plus
`S` in-memory scans. Direct broad reads without pushdown were also `O(S*N)`.

Canonical pushdown is
`O(D + P + R log_B N + E)`, where `P` is the number of exact identities and
`D <= P*H` is the number of distinct authenticated path/value objects in the
transaction view. With bounded tree height this is point-proportional.
Residual predicates remain honestly `O(N + E)` through one ordered range.
Lix SQL semantics remain `O(R + E)`.

The corrected perfect-elimination ceiling includes both the original 946 us
full snapshot and the 15 `O(N)` in-memory filters hidden inside binder time.
Observed Slate model wall fell from 5,239.224 us to 1,939.605 us (-62.979%),
which bounds the removable bridge/read term at approximately 63% for this
shape.

## Exact 1K focused gate

Setup and the identical Memory semantic oracle were excluded from both sides.

| Adapter / axis | Current Lix | ForkTree pushdown | Delta |
|---|---:|---:|---:|
| Slate wall | 4,108.029 us | 1,939.605 us | -52.785% |
| Slate CPU | 4,149.315 us | 1,943.686 us | -53.156% |
| Slate allocated bytes | 10,044,449 | 3,050,711 | -69.628% |
| Slate allocation calls | 29,753 | 28,635 | -3.758% |
| Slate RSS delta | 2,297,856 B | 1,122,304 B | -51.159% |
| Slate physical reads | 71 / 132,187 B | 11 / 5,705 B | -84.507% / -95.684% |
| Slate physical writes | 2 / 1,662 B | 1 / 3,089 B | -50.000% / +85.860% |
| Slate logical write bytes | 6,575 B | 3,245 B | -50.646% |
| Slate settled disk | 127,600 B | 29,722 B | -76.707% |
| Rocks wall | 3,213.828 us | 1,924.096 us | -40.131% |
| Rocks CPU | 4,001.687 us | 2,062.029 us | -48.471% |
| Rocks allocated bytes | 6,518,420 | 2,862,645 | -56.084% |
| Rocks allocation calls | 22,166 | 27,417 | **+23.689%** |
| Rocks RSS delta | 2,408,448 B | 1,544,192 B | -35.884% |
| Rocks logical write bytes | 6,567 B | 3,245 B | -50.586% |
| Rocks settled disk | 163,101 B | 111,947 B | -31.363% |

The Rocks allocation-call excess is the test-only DTO/materialization seam
between private Lix live-state batches and the external model. Removing it
honestly requires Stage 2 to implement this contract directly beneath the
existing provider, not another binder or benchmark-local row authority.

Ryzen-V acceptance should require the same exact/residual oracle, zero broad
reads for the 18-statement canonical batch, one coherent authenticated read
view, one root publication, both adapter performance with every critical
regression <=5%, and all existing #1260 SQL semantics unchanged.
