# EXP-BPTREE-07: canonical authenticated high-fanout B+ tree

## Verdict

**QUALIFIED NO-WIN. Corrected global consecutive no-win streak: 8/20.**

The immutable B+ tree ref originally recorded 7/20. EXP-HOTCOLD-04 completed
independently and precedes it in the coordinator's global ledger, making the
B+ tree the eighth consecutive no-win. Its frozen ref remains unchanged.

The prior HAMT result is globally experiment 6/20 because EXP-JSONB-01
completed independently. Its immutable ref remains unchanged.

The B+ tree preserves ordered access and substantially reduces settled bytes,
but fails the lexicographic OLTP gate. Updates are 3.2-17.8x slower than C2 in
the primary matrix. Most point, bounded-range and scan cells also regress by
more than 5%. Storage savings cannot override slower OLTP.

No production integration or independent reviewer was started.

## Physical contract

The candidate and C2 store identical Schema-v1 native tuple bytes and complete
canonical StateKeys. The candidate is the only current-state authority:

- immutable content-addressed B+ tree ordered by encoded StateKey;
- prefix-compressed full keys in leaves and prefix-compressed minimum-key
  fences in internal pages;
- deterministic byte-based page partitioning, split, merge and root collapse;
- batched path-copy updates rewrite each touched leaf and ancestor once;
- each loaded child proves the parent fence equals its authenticated minimum;
- root-recursive diff skips equal ObjectIds and enumerates only a structurally
  changed authenticated subtree;
- no sibling authority, ordered side index, cache, fallback, JSON or dual path.

Forward/reverse insertion produces the same root. Delete/reinsert controls
prove deterministic merge and root restoration. Codec/runtime controls reject
duplicate/unordered/malformed fences, malformed key prefixes, duplicate leaf
keys, truncation, missing/wrong children, same-domain child substitution,
wrong fence/child minimum, wrong object hash and forged root. Cold reopen
authenticates and enumerates the complete tree.

## Canonical page-width policy

The 4/8/16/32 KiB sweep used the same integer-PK 10K workload. Larger pages
reduce scan calls modestly but multiply path-copy update cost. At D=10,
RocksDB update ratios were 6.90x, 16.45x, 56.17x and 211.50x respectively;
SlateDB ratios were 6.19x, 13.27x, 44.34x and 147.16x.

The derived deterministic policy targets approximately 32 encoded tuples per
leaf, rounds to a power of two, and clamps to 4-32 KiB. It selects 4 KiB for
the integer/UUID/text widths and 8 KiB for the wider composite tuple. This is
schema-width-derived, not runtime tuning.

## Primary matrix

Ratios are B+ tree / C2; lower is better. Twenty update and point samples were
recorded per cell.

| Backend/cell | Point hit | Update p50 | Update p95 | 1K range | Full scan | Diff |
|---|---:|---:|---:|---:|---:|---:|
| Rocks 10K D=1 | 1.41x | 14.37x | 12.86x | 1.11x | 1.08x | 0.98x |
| Rocks 10K D=10 | 1.27x | 7.10x | 6.93x | 1.13x | 1.17x | 5.38x |
| Rocks 100K D=1 | 1.14x | 9.22x | 8.67x | 1.01x | 1.12x | 1.01x |
| Rocks 100K D=10 | 1.07x | 17.60x | 17.49x | 1.01x | 1.13x | 7.61x |
| Rocks 100K D=1% | 0.77x | 4.37x | 4.21x | 0.89x | 1.11x | 1.60x |
| Slate 10K D=1 | 1.50x | 8.36x | 8.26x | 1.31x | 1.37x | 1.03x |
| Slate 10K D=10 | 1.34x | 6.57x | 6.47x | 1.32x | 1.38x | 5.85x |
| Slate 100K D=1 | 1.26x | 6.15x | 4.84x | 1.12x | 1.39x | 1.00x |
| Slate 100K D=10 | 1.20x | 15.05x | 13.86x | 1.21x | 1.37x | 8.95x |
| Slate 100K D=1% | 1.14x | 4.93x | 4.88x | 1.18x | 1.52x | 1.68x |

Point misses likewise regress: at 10K/D=1, RocksDB is 23us versus 9us C2
and SlateDB is 34us versus 15us. At 100K/D=1 the gap narrows to 23/22us and
34/27us, but it is not a win.

UUID/text/composite 10K D=10 reproduce the result. RocksDB updates are
5.23x/5.04x/3.82x; SlateDB updates are 6.13x/5.23x/4.04x. At 50K D=10,
contiguous-prefix, random and repeated-key distributions remain 7.05-12.23x
slower. Thus the result is not an integer-key or distribution artifact.

Branch/history/cold-reopen digests pass on every cell. The primary sweep covers
N=1K/10K/50K/100K and D=1/10/1%; UUID/text/composite runs cover 10K; prefix,
random and repeated-key runs cover 10K/50K.

## Storage and amplification

The candidate's compact root and prefix compression provide real storage wins.
At 100K/H=20:

| Backend / D | C2 settled | B+ tree settled | Ratio |
|---|---:|---:|---:|
| Rocks D=1 | 11.38 MB | 7.92 MB | 0.70x |
| Rocks D=10 | 12.54 MB | 9.41 MB | 0.75x |
| Rocks D=1% | 280.30 MB | 158.65 MB | 0.57x |
| Slate D=1 | 11.35 MB | 7.90 MB | 0.70x |
| Slate D=10 | 12.51 MB | 9.40 MB | 0.75x |
| Slate D=1% | 140.15 MB | 92.35 MB | 0.66x |

Those savings arise from eliminating C2's large root page-ID vector and from
prefix compression. They do not compensate for decoding/hashing and rewriting
4-8 KiB leaves plus multiple content-addressed ancestors per update.

## Decision

The B+ tree is a coherent authenticated ordered layout, but not a C2
replacement for OLTP. It loses every important update class by far more than
5% and has no offsetting critical latency win. EXP-BPTREE-07 advances the
global no-win streak to 7/20.

## Reproduction

Primary command:

```text
EXP_BPTREE_PK_KIND=integer EXP_BPTREE_PATTERN=uniform \
EXP_DELTA_PAGE_BACKENDS=rocksdb,slatedb \
EXP_DELTA_PAGE_SIZES=1000,10000,50000,100000 \
EXP_DELTA_PAGE_HISTORIES=20 EXP_DELTA_PAGE_DELTAS=1,10,1pct \
EXP_DELTA_PAGE_ROOT=/root/repos/evidence/exp-bptree-07/final-integer-uniform-h20 \
timeout 1200 /root/repos/.target-exp-delta-page-01/release/examples/exp_delta_page_01
```

Release binary SHA-256:
`680a597db1c8967fcd9a726f3079cffb0198f6de24560a10da247769f52f0793`

Raw log SHA-256:

- primary integer: `39c5fc9c8f59aed0bb4e7c27863e874add2c722d2ad7e327da789a067d61d61a`
- UUID: `c382c2a42459b5fe68b429c37341e4b1505884935d5330aca89ee5aa7d8981ed`
- text: `45f73dc475a85aa35d24fb0d20c67ae6a2cc802e949d93cd94e41899d09c86b2`
- composite: `cf682449ba1f0d6ff2271483fa73e8606303b46268eb67bad089b79445417f13`
- prefix: `be6edba5b880b4d1ef1470bb4952e710badc62ba2c2a1e8ca866a18ce6b549a9`
- random: `80329003c926042c3c8af2471d537a98733e82b1e4b2f7bc1427c1a74e47a9d9`
- repeated: `4b0827d25665ef24534f9a9f9da7374a198035c9df4bf75cc981f51cc0b8edc1`
- page sweep 4/8/16/32 KiB:
  `4ef9c3a2897f61a8a07f7c278e0e9b438596fe6038106205402a9ccea6f40192`,
  `b14e34d83a724f08ce3d261c4616f4c7f7fcfd01f9a6d22d0af7787e73e155f4`,
  `c5b5773e1747a04b016e6cc5cfa1be0b822d8b0128d794852109baf6fc853455`,
  `1cd65abb17f7e3d29bb66e1c293b7f4a0fb88eac815109b3c107e80620e52be1`.

The primary matrix consumed 20.72s user CPU, 1.60s system CPU, 21.59s wall,
and 181,448 KiB maximum RSS.
