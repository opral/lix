# EXP-SCHEMA-ROOT-17 — qualified no-win

## Identity and question

- Exact parent: `b384d051b4ae888ba84cadcd5f9da719deb0f5f8`
- Parent tree: `f1f525a39ff17287f476b0337cfa326be4f09bd9`
- Question: can one authenticated schema-root directory plus one canonical C2 subtree per schema replace the global mixed-schema C2 tree without an important OLTP regression?
- Acceptance bar: more than 5% important-path improvement and no critical regression above 5%.

The experiment has one repository root in both variants. The candidate has no mirrored global tree, fallback, cache, or second writer. Its root authenticates one canonical schema directory; each directory member binds the exact schema identity and sole C2 subtree root. Every positive point verifies and compares the complete key.

## Final smallest crossover

The final crossover covered N=1,000, 1/4/16/64/256 schemas, uniform and 80% hot-schema distributions, integer/UUID/text/composite primary keys, present and missing points, range-100 and full typed scans, and D=1/10/1% updates within one schema and across schemas. Seven samples and 10,000 lookups per point cell were used.

Candidate deltas relative to the global C2 layout:

| Operation | Cells | Mean p50 | Best | Worst | Auth calls | Read bytes | Puts | Write bytes | Settled bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| point present | 40 | +8.60% | -27.38% | +78.62% | +33.33% | -8.26% | — | — | -9.85% |
| point missing | 40 | +8.83% | -26.90% | +78.94% | +33.33% | -8.26% | — | — | -9.85% |
| range 100 | 40 | -1.11% | -28.72% | +48.39% | +19.50% | -6.64% | — | — | -9.85% |
| full typed scan | 40 | -3.22% | -28.56% | +46.15% | +9.74% | -7.66% | — | — | -9.85% |
| update, one schema | 120 | +49.16% | +8.09% | +110.20% | — | — | +27.18% | -9.01% | -9.85% |
| update, across schemas | 120 | +49.80% | +26.17% | +108.40% | — | — | +30.28% | -9.36% | -9.85% |

Point p50 by schema count (average across key types and distributions):

| Schemas | Present | Missing |
|---:|---:|---:|
| 1 | -3.28% | -3.06% |
| 4 | -12.07% | -12.39% |
| 16 | -5.12% | -5.58% |
| 64 | +33.44% | +34.48% |
| 256 | +30.05% | +30.72% |

The update timing model reconstructs immutable trees to obtain canonical before/after object sets and therefore is not a production path-copy latency claim. Its object differential is still useful for physical amplification. The terminal rejection does not depend on update timing: prescribed N=1K point/range cells independently exceed the critical 5% regression ceiling.

## Causal result

Partitioning shortens the selected state subtree and saves 6–8% read bytes, but it adds a separately authenticated directory traversal. At 64 and 256 schemas the directory gains depth while each tiny schema subtree still requires its own root-to-leaf verification. The result is one-third more authenticated object calls and 30–34% slower points in those required cells. The layout is therefore not a stable format rule across schema-count scaling.

## Correctness and corruption controls

The model verifies canonical page bytes and ObjectIds, full child IDs and ordinals, page ordering, schema identity binding, directory-to-subtree roots, and the repository root. Controls reject:

- wrong repository root and root substitution;
- swapped schema subtrees and missing directory members;
- partial two-schema publication;
- malformed schema identity and malformed directory bytes;
- duplicate keys/members;
- noncanonical insertion order.

Reversed input produces the same repository root. The crossover did not proceed to RocksDB/SlateDB, branch/reopen, or VCS cells because the mandatory in-memory OLTP guard failed first; no adapter or production claim is made.

## Evidence

- Model executable SHA-256: `229c21cadaac5d02610aa826ff88b3dd51623eb5b0ccf549f2a23b66fafd4ef2`
- Final CSV SHA-256: `7b7c7a96898cfcf35f2d370abcd7bbe751c636f4bf1317f3af3c2592198a3446`
- Resource log SHA-256: `01bacf5d07ef93de76fe59c1c6d41d54ca377b67bbdb9da5457cb481c2b5eb63`
- Summary CSV SHA-256: `dda9597b24bdc9b58dcda8e2f13aaea36c4099d467e68888363640698be52e37`
- Final run: 84.23 seconds wall, 84.22 seconds user CPU, 6,876 KiB maximum RSS, exit 0.
- `cargo check -p lix_e2e --bench physical_layout_schema_root`: pass.
- `cargo fmt --all -- --check`: pass.
- `git diff --check`: pass.

## Verdict

**NO-WIN / reject EXP-SCHEMA-ROOT-17.** Do not add schema-root directory partitioning to C2. The byte reduction does not compensate for the extra authenticated owner boundary, and required schema-count cells materially regress OLTP.
