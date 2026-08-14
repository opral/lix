# EXP-DELTA-LEVELS-02 — qualified NO-WIN (2/10)

Base: `dc4f42917937150fa20fcb7517c46c21d1840045`  
Parent evidence commit: `b2eb19dfd82bca61909df77ef42bb98a2ecfbbe2`

This design replaced ancestry lookup with immutable deterministic binary delta
levels. Each current root authenticated one base page set and a bounded set of
sparse levels; lower levels were newer. Binary carries merged each tuple once per
occupied level, and deterministic full compaction remained at depth 32. Reads did
not walk commit ancestry. Delta page references carried authenticated min/max key
bounds, and every selected page was content-address verified and decoded before
serving values.

## Lexicographic OLTP verdict

The design is rejected. D=1 was close to viable, but sparse random D=10 pages have
broad min/max intervals. Cold keys therefore caused false page selections and full
page authentication at multiple levels.

H=100 ratios (levels/slotted; lower is better):

| D | Backend | Update series | Hot points | Cold points | Full readback | Diff | History |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | RocksDB | 0.549 | 0.277 | 1.094 | 0.983 | 3.485 | 1.061 |
| 1 | SlateDB | 0.782 | 0.493 | 1.026 | 0.960 | 3.190 | 1.012 |
| 10 | RocksDB | 0.137 | 0.533 | 2.461 | 1.033 | 1.066 | 2.557 |
| 10 | SlateDB | 0.343 | 0.739 | 2.310 | 1.004 | 0.927 | 2.405 |

The D=10 cold-point regression is materially above 5%, so VCS and settled-byte
benefits cannot promote the design. The next experiment, `EXP-DELTA-BLOOM-03`,
adds an authenticated per-page membership summary to avoid false page loads. The
summary may only reject absent keys; it never serves values or bypasses page hash,
bounds, tuple, or content authentication.

## Frozen evidence

- Exact release binary SHA-256:
  `6896c0de68705c92884df7ff5d4084790a7c0b2f953131c4a6482b7498098df2`
- H=10 quick log SHA-256:
  `acef7c35762465d89b1f0eaa782672768b1bdfed2a1d1ad8a11fc04162169539`
- H=100 decisive log SHA-256:
  `53b215158b73ab411b32c30edd875e90b44f1a5f1705692c2c55750c9046dd7c`
