# ForkTree SQL DML A/B result

Provenance:

- SQL owner base/head: `7061aad7f4b14e611b32bbe5493f39253b826378`
  (`#1260`, exact Lix parser/binder/write executor and transaction-scoped
  physical-target registry).
- Frozen ForkTree model source: `2a0e8512bb37c9da2050c99c366e5ac05bb01553`.
- Gate: 1,000 live rows, one 18-statement labeled transaction, repeated three
  times after excluded setup, RocksDB and SlateDB.

## Verdict

`BLOCKER` for Stage-2 qualification. The model physical result is promising,
but the only cfg-gated bridge available at this source point implements
`SqlWriteExecutionContext`, which sits above transaction commit validation.
It therefore reuses Lix parsing, binding, RETURNING, ON CONFLICT, defaults,
NULL handling, provider scans, and staged postimages, but cannot prove FK
validation, failed-statement savepoints, stale-writer rejection, or atomic
commit/reopen semantics without recreating those owners. No 10K/50K result is
admitted from a bridge that has not passed that semantic gate.

The measured transaction used INSERT/UPDATE/DELETE RETURNING, ON CONFLICT DO
UPDATE/DO NOTHING, multirow INSERT, defaults, NULL, string payloads, exact
statement labels/indexes, and one final ForkTree selector publication. Current
and model result digests matched exactly at every admitted cell.

## Stable 1K result (three transactions)

| Backend | Axis | Current | ForkTree model | Delta |
|---|---:|---:|---:|---:|
| RocksDB | wall / tx | 3,375.285 us | 3,476.372 us | +2.995% |
| RocksDB | CPU / tx | 3,804.059 us | 3,741.494 us | -1.645% |
| RocksDB | allocated bytes / tx | 6,512,572.7 | 7,538,750.7 | +15.757% |
| RocksDB | allocation calls / tx | 22,137.3 | 60,237.3 | +172.108% |
| RocksDB | logical write bytes / 3 tx | 19,717 | 9,924 | -49.667% |
| RocksDB | puts / 3 tx | 90 | 24 | -73.333% |
| RocksDB | settled disk | 167,722 B | 118,192 B | -29.532% |
| SlateDB | wall / tx | 3,568.498 us | 3,662.662 us | +2.639% |
| SlateDB | CPU / tx | 3,619.618 us | 3,705.393 us | +2.370% |
| SlateDB | allocated bytes / tx | 9,998,616.3 | 8,087,757.7 | -19.111% |
| SlateDB | allocation calls / tx | 29,175.7 | 64,030.7 | +119.468% |
| SlateDB | physical read objects / 3 tx | 213 | 116 | -45.540% |
| SlateDB | physical read bytes / 3 tx | 396,459 | 63,349 | -84.021% |
| SlateDB | physical write objects / 3 tx | 6 | 3 | -50.000% |
| SlateDB | physical write bytes / 3 tx | 4,978 | 9,456 | +89.956% |
| SlateDB | settled disk | 136,535 B | 37,387 B | -72.617% |

Result digest for both layouts/adapters:
`9d86713ea989d2d6086264f0a06dd0f6c68c490ed5241a71d465bf809bf7fb56`.
Both layouts ended with 1,015 live rows.

The one-transaction diagnostic split attributed ForkTree wall time as follows:

- RocksDB: full model row load/decode 1,014 us; exact Lix binder/executor
  3,229 us; authenticated path-copy publication 269 us.
- SlateDB: full model row load/decode 946 us; exact Lix binder/executor
  2,798 us; authenticated path-copy publication 281 us.

The bridge's full model snapshot materialization is the dominant removable
model-only term. It introduces `O(N)` work before the unchanged SQL
`O(R + E)` semantics. Publication itself is
`O(R log_B N + E)` for `R` final changed identities, tree fanout `B`, and
constraint/effect work `E`. The perfect-elimination ceiling for the measured
bridge is the entire ~0.95-1.01 ms model load plus its decoded-row allocation;
publication is only ~0.27-0.28 ms.

## Stage-2 implementer contract

Ryzen-V must place the ForkTree target below the existing transaction semantic
owner, after row normalization/default/generated/FK validation and statement
savepoint handling, but before current-layout write-set materialization.

1. The existing #1260 parser, binder, logical write plan, provider target,
   RETURNING capture, and ON CONFLICT driver remain the only SQL authority.
2. The existing transaction remains the only read-your-writes overlay,
   constraint validator, savepoint owner, and stale-head/precondition owner.
3. The physical target consumes validated final postimages/tombstones and the
   transaction's expected branch selector. It must not rescan or decode all
   `N` rows; narrow scans/points must call the accepted one-StorageRead
   authenticated ForkTree iterator.
4. Sort and coalesce final mutations once, path-copy authenticated leaves and
   internal paths, then publish immutable objects plus exactly one branch
   selector/epoch transition under the existing transaction precondition.
5. RETURNING is evaluated from the transaction-owned selected pre/postimage,
   never reread from a second model mirror. A failed statement restores only
   its transaction checkpoint; a failed commit publishes no selector.
6. Reopen, branch visibility, FK/default/generated/NULL/blob, stale-writer,
   rollback/savepoint, history/diff/undo-redo, and corruption tests must run
   through public Lix APIs before this A/B is repeated.
7. The production target must retain one layout/authority. This diagnostic
   bridge and its full-snapshot materialization are not production code and
   must not be copied into Stage 2.

Only after that boundary is runnable should the exact 1K corpus be rerun, with
10K/50K admitted when wall/CPU regressions remain at most 5% and a meaningful
resource axis improves by more than 10% on both adapters.
