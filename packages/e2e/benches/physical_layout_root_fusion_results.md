# EXP-ROOT-FUSION-21 — qualified no-win

## Identity and format rule

- Exact parent: `b384d051b4ae888ba84cadcd5f9da719deb0f5f8`
- Parent tree: `f1f525a39ff17287f476b0337cfa326be4f09bd9`
- Question: can direct repository-root embedding remove SCHEMA-ROOT's extra authenticated directory hop without introducing a threshold regression?
- Acceptance bar: more than 5% important-path improvement over global C2 and no critical regression above 5%, especially at the inline/external transition.

Both variants retain one repository-root authority. Root fusion has one C2 subtree per schema and no mirrored global tree, fallback, cache, compatibility geometry, or second writer. The immutable format rule is:

- inline sorted schema-root entries when canonical root bytes are at most 1,024 bytes;
- otherwise one canonical authenticated directory-tree root;
- mode depends only on canonical schema-root count/bytes.

Each entry binds the exact 16-byte schema identity, subtree ObjectId, and row count. With 52-byte fixed entries, 1/4/16 schemas are inline and 64/256 schemas are external.

## Small decisive crossover

The terminal crossover covered N=1,000; 1/4/16/64/256 schemas; uniform and 80% hot-schema distributions; integer, UUID, text, and composite keys; present/missing points; range-100/full typed scan; and D=1/10/1% mutations within one schema and across schemas. Point cells use seven samples and 10,000 lookups per sample.

Candidate deltas relative to global C2:

| Operation | Cells | Mean p50 | Best | Worst | Auth calls | Read bytes | Puts | Write bytes | Settled bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| point present | 40 | +6.48% | -35.27% | +79.50% | +13.33% | -7.56% | — | — | -9.91% |
| point missing | 40 | +6.95% | -35.27% | +79.67% | +13.33% | -6.59% | — | — | -9.91% |
| range 100 | 40 | -5.61% | -31.42% | +47.61% | +3.75% | -10.92% | — | — | -9.91% |
| full typed scan | 40 | -6.75% | -31.31% | +46.49% | +1.34% | -12.17% | — | — | -9.91% |
| update, one schema | 120 | +46.67% | -5.59% | +106.66% | — | — | +19.48% | -8.01% | -9.91% |
| update, across schemas | 120 | +47.20% | +24.16% | +104.58% | — | — | +22.28% | -8.84% | -9.91% |

Point p50 by schema count:

| Schemas | Root mode | Present | Missing |
|---:|---|---:|---:|
| 1 | inline | -5.21% | -5.20% |
| 4 | inline | -15.40% | -14.82% |
| 16 | inline | -12.98% | -13.10% |
| 64 | external | +33.83% | +34.97% |
| 256 | external | +32.18% | +32.90% |

At the precise 16→64 mode transition, 16-schema hot point cells already range from +4.33% to +9.54%, because authenticating and structurally validating the 840-byte inline inventory is not free. At 64 schemas, external hot point cells regress +57.96% to +69.31%. Uniform 64-schema cells range from -0.22% to +6.58%, also crossing the critical ceiling.

The mutation timing model canonically reconstructs trees to derive before/after immutable object sets; it is not a production path-copy latency claim. Its object differential captures physical puts and bytes. The rejection is independent of mutation timing because point and range cells violate the threshold guard.

## Causal result

Fusion successfully removes the directory read below the cap and improves 1/4/16-schema average points by 5–15%. It cannot preserve that property above the cap: the external representation restores the directory traversal, while each small selected subtree still needs root-to-leaf authentication. The fixed canonical transition therefore creates a large and predictable latency cliff. Increasing the cap merely moves the cliff and makes every root hash/decode scale linearly with schema count; workload-dependent tuning is forbidden.

## Correctness and corruption controls

The model validates root/page ObjectIds, canonical mode selection, sorted unique schema entries, exact schema/subtree binding, child IDs/ordinals, page order, and one atomic repository root. Controls reject:

- duplicate or unsorted inline schemas;
- wrong repository-root binding and root substitution;
- inline/external mode mismatch;
- swapped or missing schema subtrees;
- partial two-schema publication;
- malformed external schema identity and directory bytes;
- root truncation and duplicate keys;
- insertion-order noncanonicality.

Reversed insertion order yields the same repository root. N=10K/100K, RocksDB/SlateDB, sparse diff, and cold-reopen expansion were deliberately not run because the mandatory N=1K inline/external threshold crossover failed first.

## Evidence

- Model executable SHA-256: `e37e485ba64a260d006be6cefdb6202fb5ac0bccb58b53f95392643978f0df05`
- Final CSV SHA-256: `5042ab45f21b04a7743d3a3bd8b1fa93fbafd28dc4993a907b7a07626e16d364`
- Resource log SHA-256: `81d47e05ea8aecbcb48639db1102d50672783711461e0f5330367a7b11870750`
- Summary CSV SHA-256: `01592a112ddc921b825c264085dcafeaa04016387aa0c22694c767ba8e81dd72`
- Final run: 83.81 seconds wall, 83.79 seconds user CPU, 7,132 KiB maximum RSS, exit 0.
- `cargo check -p lix_e2e --bench physical_layout_root_fusion`: pass.
- `cargo fmt --all -- --check`: pass.
- `git diff --check`: pass.

## Verdict

**NO-WIN / reject EXP-ROOT-FUSION-21.** Root fusion solves the extra-hop problem only below its cap and introduces a critical canonical threshold cliff. Do not adopt it as C2's schema layout.
