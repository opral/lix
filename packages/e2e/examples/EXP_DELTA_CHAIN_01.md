# EXP-DELTA-CHAIN-01 — qualified NO-WIN (1/10)

Base: `dc4f42917937150fa20fcb7517c46c21d1840045`  
Tree: `6b4b9e14eb95dfa5fb5fc7046cf169c12f4813e1`

This experiment compared canonical Schema-v1 typed tuples in immutable 64-row
slotted pages against an immutable base plus one authenticated sparse delta per
commit. A delta root carried authenticated page min/max bounds and a parent root;
point lookup walked ancestry until it found the key. Full deterministic compaction
was tested at depths 4, 8, 16, and 32. RocksDB and SlateDB used the shipping storage
adapters. Every read operation used one retained storage view.

## Lexicographic OLTP verdict

The chain is rejected. No compaction depth kept both update throughput and cold
point latency within the 5% OLTP gate.

Representative H=100 series ratios (delta/slotted; lower is better):

| Depth | D | Backend | Update series | Hot points | Cold points | Full readback |
|---:|---:|---|---:|---:|---:|---:|
| 4 | 1 | RocksDB | 1.259 | 0.373 | 1.116 | 0.979 |
| 4 | 1 | SlateDB | 1.122 | 0.521 | 1.249 | 0.871 |
| 4 | 10 | RocksDB | 0.204 | 0.492 | 1.436 | 0.991 |
| 4 | 10 | SlateDB | 0.389 | 0.673 | 1.613 | 1.036 |
| 8 | 1 | RocksDB | 0.741 | 0.257 | 1.247 | 1.021 |
| 8 | 1 | SlateDB | 0.992 | 0.504 | 1.607 | 1.043 |
| 16 | 1 | RocksDB | 0.473 | 0.202 | 1.408 | 1.004 |
| 16 | 1 | SlateDB | 0.792 | 0.456 | 2.274 | 1.116 |
| 32 | 1 | RocksDB | 0.314 | 0.178 | 1.857 | 0.988 |
| 32 | 1 | SlateDB | 0.746 | 0.428 | 3.626 | 1.004 |

VCS history and storage often improved substantially, but the acceptance rule does
not allow those gains to mask a material OLTP regression. The causal failure is
architectural: cold points perform one authenticated root lookup per delta depth;
compacting often enough to hide that cost rewrites the full base frequently enough
to lose sparse D=1 update throughput.

## Frozen evidence

- Exact release binary SHA-256:
  `52e9c160072cfc806ebcc36825c58067b8899ae27beb518cf2d7530b23b64e83`
- Retained-view quick log SHA-256:
  `15e9c8eab95a479708749b851fe7953461f8ed25ac62dd6dade936901e69cc7d`
- Compaction/series log SHA-256:
  `32018b6c3ef06f9ba0878c01c276c6a34b3fe0806302fd1ee7bc1ed0f35546be`
- N=1K/10K/50K H=10 scaling log SHA-256:
  `d8e7daf8eae51b9b85748d86f418fbe091abaff6c231c4ae1696e9282b30ef31`
- H=10/100/1000 depth log SHA-256:
  `89f83bc6947d0942f4487bb660bc73d2389b13ee76368a1e4f4b4bbb24a812ea`

The next design is separately identified as `EXP-DELTA-LEVELS-02`; it replaces
ancestry lookup with authenticated deterministic binary levels and has an
independent verdict/evidence set.
