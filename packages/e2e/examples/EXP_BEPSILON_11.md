# EXP-BEPSILON-11 — qualified no-win

## Decision

`NO-WIN`. The global consecutive qualified no-win streak advances from 10/20
to 11/20. The experiment is not a production candidate and must not be
composed.

The candidate stores a single canonical sorted mutation buffer in the same
authenticated root/routing object that owns the C2 leaf directory. Leaves are
unchanged canonical C2 pages. A flush groups the complete sorted buffer by
child, reads all touched children in one `get_many`, and stages each changed
leaf once. There are no sidecars, chains, caches, fallback readers, secondary
indexes, or dual authorities.

The deterministic candidate policy is 128 entries or 16 KiB, whichever is
reached first. The 32/128/512-entry crossover was measured separately. 128 is
the only plausible crossover: 32 flushes too eagerly; 512 makes the routing
root large enough to materially regress points and ranges.

## Qualification

All 576 paired layout cells completed (288 C2/Bε pairs): RocksDB and SlateDB;
N=1K/10K/50K/100K; D=1/10/1%; repeated/uniform/random updates; and
integer/UUID/text/composite PKs. Each cell covers present/missing hot/cold
points, range-100, full scan, sparse root diff, branch sharing, history,
authenticated corruption, and cold reopen. Operation and final-state digests
matched in every completed cell. No cell was omitted after the accelerated
stopping instruction because the matrix had already completed.

Across all pairs, candidate/C2 median ratios were: update p50 0.349x, update
p95 0.372x, point-hit p50 0.389x, point-miss p50 1.091x, full readback 1.005x,
range-100 1.082x, sparse diff 0.954x, settled bytes 0.946x, and process RSS
1.014x. Medians conceal critical tails: worst update p95 was 1.568x, miss p50
4.818x, range-100 2.115x, readback 1.273x, diff 1.648x, settled bytes 1.451x,
and RSS 1.324x.

The decisive random/composite 1%-mutation cells demonstrate the rejection:

| Backend | N | update p50 | update p95 | miss p50 | range-100 | readback | settled |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Rocks | 1K | 0.12x | 0.16x | 2.67x | 1.23x | 0.93x | 0.36x |
| Rocks | 10K | 0.03x | 1.41x | 1.00x | 1.09x | 1.04x | 0.81x |
| Rocks | 100K | 1.05x | 1.06x | 1.00x | 1.21x | 1.06x | 1.00x |
| Slate | 1K | 0.26x | 0.31x | 2.69x | 1.35x | 1.20x | 0.32x |
| Slate | 10K | 0.05x | 1.48x | 1.00x | 1.30x | 1.07x | 0.81x |
| Slate | 100K | 0.95x | 0.94x | 1.00x | 0.88x | 0.96x | 1.00x |

The buffer produces excellent non-flush medians and can substantially reduce
bytes at intermediate scales. It nevertheless fails the lexicographic OLTP
gate: periodic random flushes regress update p95 by 41–48% at 10K, and reading
the authenticated routing buffer regresses misses/ranges by 9–169%. At 100K,
where 1% updates flush every transaction, the design converges to or loses to
C2 and provides no storage advantage. This is a geometry tradeoff, not an
adapter artifact.

## Integrity controls

The canonical root codec rejects malformed ordering, duplicate buffered keys,
keys outside child fences, malformed tags, tuple corruption, and truncation.
The same codec proves NULL tuple decoding and tombstone precedence. Runtime
controls reject missing objects, wrong-child substitution, page/root position
mismatch, and content-hash substitution before partial output. Every RocksDB
and SlateDB cell performs cold reopen and verifies the full-state digest.

## Evidence

- Warm target: `/root/repos/.target-exp-delta-page-01`
- Raw matrix: `/root/repos/evidence/exp-bepsilon-11/matrix/`
- Matrix checksum manifest:
  `/root/repos/evidence/exp-bepsilon-11/matrix-sha256.txt`
- Matrix checksum-manifest SHA-256:
  `09a12b4825a473204c0fd6d9e1ca4ff1e1c400ccb0b2c552e02b244566b2a9d3`
- Crossover logs:
  `4cb7f81ee89bca5504d809e52cc0e1c8810d8bb8f72a1fab83a0a0bb378d4ae1`
  (32),
  `677e7ed112fa9156ca207d9dc56a022a05c485b5803c7c9ac994d2c40289e41c`
  (128),
  `e8e36b6fdc1e2d03eebbb1e3bc49fbfcd7f9e6cb3ea08ff9be0496458c946c07`
  (512).
- Final compile check: `4bd56b284fea62ab01305d04293f0d4af3371fa003236272737ec936a4e99777`.
- Final release build: `584c410bbed6843e08d5629b22ed49f1b191f7c78059ae973d28199e6f8e8760`.
- Final RocksDB/SlateDB corruption and reopen controls:
  `192511b5f5cb2f9f2ccd14e0b8422bfc1bcb417891e85e8b22172191cd9dc119`.
- Exact release binary:
  `98e543d4b3bb2e572e11c39a2e28a9abf9592fc7defd19100545b2c6f6b1d907`.

No reviewer was spawned because the experiment is a qualified no-win.
