# EXP-SUBRANGE-MERKLE-12 — qualified no-win

## Identity and scope

- Parent: `aecf821658644f95724f22e3d29deda04573fdf1`
- Parent tree: `5e504b7ecf2e0d080dd0c79f407ea72387c8279b`
- Scope: test/benchmark/report only. No production, format, compatibility, cache, or fallback change.
- Comparator: the approved schema-partitioned C2 slotted-page model, using identical canonical typed PK and Schema-v1 non-PK tuple bytes.
- Candidate: deterministic BLAKE3 hashes for fixed row subranges embedded in the same authenticated leaf object. Hashes are derived from canonical full-key + tuple bytes and cannot serve values.

## Verdict

**Correctness-qualified NO-WIN.** Fixed 16-row subranges improve the representative VCS aggregate by 29.21%, but the OLTP aggregate is 1.29% slower and the important 1%-mutation path is 9.83% slower (p50 geometric means across 16 distribution/scale cells). The latter exceeds the hard 5% regression guardrail, so the VCS win cannot qualify the layout under the lexicographic OLTP-first policy.

The candidate also increases authenticated object bytes by 2.91%, RocksDB cold-reopen bytes by 2.85% and SlateDB cold-reopen bytes by 2.85%. Settled bytes are +0.52% geometric mean on RocksDB and +2.73% on SlateDB. Object count and search height are unchanged.

## Canonical-policy sweep

At N=1K/integer, 20 samples per operation, 16 rows/block was selected over 32 and 64. All choices materially accelerated sparse diff, but 16 had the smallest mutation penalty:

| Block rows | point | missing | update-one | mutate 1% | range 100 | full scan | diff D=1 | diff D=10 | diff D=1% |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 16 | 0.979x | 1.028x | 1.075x | 1.110x | 1.022x | 1.026x | 0.770x | 0.555x | 0.556x |
| 32 | 0.974x | 1.008x | 1.067x | 1.168x | 1.011x | 1.008x | 0.766x | 0.561x | 0.562x |
| 64 | 0.956x | 1.002x | 1.111x | 1.243x | 1.001x | 1.005x | 0.774x | 0.570x | 0.566x |

Values are candidate/C2 p50 ratios; below 1 is faster.

## Full fixed-policy matrix

Four PK distributions (integer, UUID, text, composite), N=1K/10K/50K/100K, 20 samples per operation, 64 KiB C2 pages, fixed 16-row blocks:

| Operation | p50 ratio | p95 ratio | CPU ratio | Worst RSS ratio |
|---|---:|---:|---:|---:|
| point | 1.0031x | 0.9952x | 1.0021x | 1.0100x |
| missing point | 1.0014x | 0.9833x | 0.9994x | 1.0100x |
| update one | 0.9992x | 0.9750x | 0.9991x | 1.0100x |
| mutate 1% | **1.0983x** | **1.0861x** | **1.0985x** | 1.0100x |
| range 100 | 0.9924x | 0.9955x | 0.9925x | 1.0100x |
| full scan | 0.9874x | 0.9881x | 0.9869x | 1.0100x |
| diff D=1 | 0.9205x | 0.9395x | 0.9206x | 1.0100x |
| diff D=10 | 0.7597x | 0.7610x | 0.7598x | 1.0100x |
| diff D=1% | 0.5071x | 0.5296x | 0.5071x | 1.0100x |
| three-way merge D=1 | 0.9187x | 0.9256x | 0.9187x | 1.0100x |
| three-way merge D=10 | 0.7635x | 0.7858x | 0.7635x | 1.0100x |
| three-way merge D=1% | 0.5057x | 0.5092x | 0.5057x | 1.0100x |

- OLTP p50 geometric mean: `1.012937170x`.
- VCS p50 geometric mean: `0.707859630x`.
- N=100K mutate-1% ratios are 1.083x integer, 1.121x UUID, 1.113x text, and 1.109x composite, confirming the regression scales rather than disappearing.

## Correctness and authority

- All C2/candidate result digests match for build, present/missing point, update, mutation, range, scan, branch sharing, D=1/10/1% diff, and current-LWW three-way merge across all four PK distributions.
- Fresh authentication and branch-root verification pass.
- RocksDB and SlateDB persistence, flush/drop/cold reopen, physical object authentication, and logical digest checks pass for N=1K/all PK distributions/both geometries.
- Candidate corruption controls fail closed for substituted block hash, an actual row-directory move across block boundaries, wrong block count, wrong fence, duplicate/gapped directory slot, truncation, missing root/child, ObjectId substitution, schema/compression/length corruption, parent-edge corruption, wrong domain, and stale/mismatched branch root.
- Verification completes before diff/merge result construction, so corruption cannot produce partial logical output.
- Updates reuse unchanged authenticated block hashes and recompute only touched blocks. There is no side object, second read, cache, fallback, or alternate value-serving geometry.

## Commands

Build:

```sh
timeout 1200s env CARGO_TARGET_DIR=/root/repos/exp-art-01-target CARGO_BUILD_JOBS=2 \
  cargo bench -p lix_e2e --bench physical_layout_subrange_merkle \
  --features 'storage-benches slatedb' --no-run
```

Each N cell used this shape with `N` replaced by 1000, 10000, 50000, and 100000:

```sh
timeout 1200s "$BIN" --n=N --pk=all --geometry=c2,merkle \
  --page-target=65536 --block-sizes=16 --backends=model --repeats=20
```

Backend closure:

```sh
timeout 1200s "$BIN" --n=1000 --pk=all --geometry=c2,merkle \
  --page-target=65536 --block-sizes=16 --backends=rocksdb,slatedb --repeats=0
```

## Evidence hashes

- executable: `b6cf2753faa22f36c58b63071fcefc70d0ab7e477d6971212ec4da9cff84a652`
- sweep CSV: `12c4e4b802329ab10319c6664d57b1037cd2f477aa52e0ab756e6186119767de`
- final correctness smoke: `bcb0ee9311581eb761fbd872a3bb51f34261732a6527405e6339d99af16443c8`
- backend closure: `464d4a2b919b1a1f5397905b4f63a628b8dc161e52b7da54b20222508d8a4239`
- N=1K matrix: `61da36b68be5e2b3bb020bb60aa79ad478f0f199bd70adf60b80afe2842ca6f9`
- N=10K matrix: `cd53a2e33027ae49b6a05ac44780f45c435ec66e939678bbf69c0bd8059e575f`
- N=50K matrix: `94cfd49195e9b0d3e9169bd6016512542844bec2434a372bdfca96389a16214e`
- N=100K matrix: `b8f5585b81702fe6e072a4d64caeba7124a78dbd34373edb971b50e89c15b5ea`
- tracked summary CSV: `8ea2e4bbe27604ad1612882115e8ed4bf257b1b430b6369f580f029045853425`
- benchmark source before formatting: `4771b6144a7441389c5abed681489da3b7ce608a8d96032ff4cd4ad44b72e794`
- analyzer source: `775e2acd90ce4061a0391fcb197bb5603d85b704bb044bf8a0599d219d434230`

This is global consecutive rejection **#17/20**, following frozen ART rejection #16/20.
