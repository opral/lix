# SQLite OLTP optimization-target package — ForkTree `b59e1f11`

Status: **TEST/REPORT-ONLY; UNRUN**.

This package freezes a deterministic standalone SQLite 3.46.0 OLTP control and
an equivalent target against the accepted ForkTree historical semantics at
`b59e1f11a51153e0a787a81f0f25bf104d150aaf`. It is a hypothesis package, not a
benchmark result, qualification, or production change. The SQLite control is
not a Lix adapter and owns no ForkTree state, history, authentication,
publication, selector, or epoch. No current-main comparison is implied.

The standalone control is pinned to bundled `libsqlite3-sys 0.30.1`, SQLite
`3.46.0`, source ID
`2024-05-23 13:25:27 96c92aba00c8375bc32fafcdf12429c58bd8aabfcadab6683e35bbb9cdebf19e`,
and the exact source/archive/static-artifact hashes in `MANIFEST.json`. The
future executable hash is not fabricated: it must be recorded by the compile
gate before any runtime cell.

## Immutable binding

```text
ForkTree commit: b59e1f11a51153e0a787a81f0f25bf104d150aaf
ForkTree tree:   700fd04d21bc40c05425c9fc9e10d65c9e1eda24
ForkTree parent: 713455a3557907ce705d06f720fcdc4486bddd4a
Parent..head full-index binary SHA-256:
  4b2885709ba09034068b321be2fe5f27348d6681b1060133af1df0b7d76bb8d4
```

The semantic source seams are the b59 `ForkTreeReadFacade` and serving
functions:

- `load_state_value_at_commit` authenticates the selected CommitCatalog entry,
  Commit object, retained catalog/member closure, and both state roots before
  resolving a point. A missing state key is the only legitimate `None` result.
- `load_commit_member_records` requires the selected CommitCatalog entry and
  fails closed on a missing commit, missing member object, missing
  ChangeCatalog owner, malformed object, or invalid owner/back-edge.
- `load_state_rows_at_commit` lowers every requested key through the same
  retained `StorageRead`; it does not open another read or consult a legacy
  tracked-state source.
- Global/local precedence, NULL, tombstone, ordering, and typed identity are
  inherited from b59 state-tree semantics.

The package must never add a SQLite-side authority, historical fallback,
whole-table rebuild, compatibility reader, or second publication path.

## Standalone SQLite control boundary

The SQLite control uses a fresh file per cell, the pinned bundled core, and
these exact setup statements before seeding:

```sql
PRAGMA journal_mode = DELETE;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;
PRAGMA recursive_triggers = OFF;
PRAGMA temp_store = MEMORY;
PRAGMA locking_mode = NORMAL;
PRAGMA busy_timeout = 0;
```

Write cells use `BEGIN IMMEDIATE`, named savepoints where specified, and a
plain `COMMIT`. No SQLite `RETURNING` result is attached to `COMMIT`; each DML
statement carries its own `RETURNING` clause. The control reports SQLite
planning, execution, row materialization, file I/O, and transaction costs
separately from ForkTree authentication, coherent-view, path-copy, selector,
epoch, and publication counters. It never supplies a history/VC result to
ForkTree or treats SQLite bytes as a comparable authority.

## Deterministic fixture

Each cell uses a fresh logical database with this schema identity:

```text
schema_key = app.sqlite_oltp
file_id    = sqlite-oltp-target-v1
primary key = typed text EntityPk("r" + six decimal digits)
```

The seed commit contains exactly 1,000 global rows, `r000000` through
`r000999`. The canonical row payload has these typed fields:

```text
id        = EntityPk text, also present in the materialized row
value     = UTF-8 "value:" + six-digit id
version   = integer seed index
nullable  = NULL when index % 11 == 0, otherwise UTF-8 "nullable:" + id
```

The overlay control fixture uses a separate branch snapshot over that seed:

```text
r000007 -> local Value("branch:value:r000007")
r000011 -> local Tombstone
r000013 -> local NULL
```

The global values remain present for all three identities. This makes local
value override, local tombstone suppression, local NULL, global fall-through,
and `include_tombstone` observable without confusing NULL with absence.

Every cell records a canonical semantic result stream. The stream is encoded
with length-prefixed UTF-8 operation labels, raw typed key bytes, explicit
value tags (`value`, `null`, `tombstone`, `absent`), canonical integer bytes,
and returned metadata. Its digest is:

```text
SHA-256("lix.sqlite-oltp.target.b59.v1\0" || canonical_result_stream)
```

The golden digest is intentionally **UNMEASURED** until the authorized future
ForkTree model/harness run. A fabricated hex digest is not evidence. Every
future warm and cold result must report the exact 64-hex digest and require
warm digest == cold digest == the model digest for the same vector.

## Frozen SQL-shaped workload cells

The SQL text below describes the public operation shape only. A future harness
may lower it through a test SPI, but must preserve the exact result stream and
typed semantics.

| Cell | Fresh state and operation | Exact logical result contract |
|---|---|---|
| `point-1000` | Seed 1,000 rows; one transaction-scoped view; point `SELECT id,value,version,nullable WHERE id = ?` for permutation `((i*37+11) % 1000)` | 1,000 point operations, 1,000 row results, no writes or commits |
| `range-128x32` | Seed 1,000 rows; `SELECT ... WHERE id >= ? AND id < ? ORDER BY id LIMIT 32` for 128 starts `((i*7) % 968)` | 128 ordered ranges, 4,096 returned rows, no writes or commits |
| `insert-256-returning` | Empty state; one `INSERT INTO app.sqlite_oltp(id,value,version,nullable) VALUES ... RETURNING id,value,version,nullable` batch | 256 inserts and 256 exact RETURNING rows |
| `update-256-returning` | Seed state; update `r000000..r000255`, increment `version`, replace `value`, `RETURNING` all updated rows | 256 updates and 256 exact RETURNING rows |
| `delete-128-returning` | Seed state; delete `r000256..r000383` with `RETURNING id,value,version,nullable` | 128 deletes and 128 exact pre-delete RETURNING rows |
| `upsert-256-returning` | Seed state; 128 conflicting IDs `r000320..r000447` and 128 new IDs `r001000..r001127` through `ON CONFLICT(id) DO UPDATE`, `RETURNING` | 256 upsert inputs, 128 updates, 128 inserts, 256 exact RETURNING rows |
| `mixed-savepoint` | Seed state; `BEGIN IMMEDIATE`, savepoint; stage 8 mutations then `ROLLBACK TO`; stage 64 inserts, 64 updates, 32 deletes, 32 upserts, each DML statement with `RETURNING`; `RELEASE` and plain `COMMIT` | Rolled-back mutations have zero durable/result effect; committed cohort has 192 successful mutations and 192 canonicalized RETURNING rows |
| `overlay-precedence` | Open the separate overlay branch and point/range read the three control identities plus ordinary global rows | Local value wins, tombstone suppresses, NULL remains a value, global rows fall through, ordered output is deterministic |
| `historical-fail-closed` | Against the seed commit, query an absent row, then independently remove/malformed-substitute the CommitCatalog entry, selected state root, and selected object kind | Absent row is `absent` only after authenticated roots; each damaged authority returns typed failure with zero fallback/retry/write |

For all write cells, the intended publication shape is exactly one sorted
mutation source, one `PreparedPublication`, one storage plan, one prepared
write set, one backend commit, and one selector+epoch CAS. The target may
report additional physical object puts from path copying, but never an
independent commit or legacy tracked-head write.

Every DML RETURNING stream is canonicalized before digest comparison by
`(statement_index, primary_key_bytes)`. This is required because SQLite does
not guarantee the physical order in which RETURNING rows are emitted. The
ForkTree target must compare the same canonical stream, not incidental engine
arrival order.

## Exact counters and reopen contract

Each sample emits this complete counter record, even when a value is zero:

```text
cell, seed_rows, logical_ops, returned_rows, result_sha256,
begin_reads, get_calls, get_keys, get_value_bytes,
scan_calls, scan_rows, put_batches, puts, delete_batches, deletes,
logical_write_bytes, prepared_publications, storage_plans,
prepared_write_sets, backend_commits, selector_cas, epoch_cas,
legacy_reads, legacy_writes, fallback_reads, retries, cache_refreshes,
wall_ns, cpu_ns, alloc_calls, alloc_bytes, rss_peak_bytes,
disk_before_bytes, disk_after_bytes, cold_reopen, cold_result_sha256,
verified
```

Non-negotiable exact invariants are:

- every cell has one coherent read per transaction scope; no helper may call
  `begin_read` again;
- read-only cells have zero puts, deletes, plans, CAS operations, commits, and
  logical write bytes;
- write cells have exactly one plan, one prepared write set, one backend commit,
  one selector CAS, and one epoch CAS;
- all `legacy_*`, `fallback_reads`, `retries`, and `cache_refreshes` are zero;
- `point-1000` has exactly 1,000 point operations and `range-128x32` exactly
  128 ranges/4,096 returned rows, independent of backend batching;
- returned rows, NULL/tombstone/absent tags, ordering, metadata, and every
  RETURNING row are digest-bound; and
- after flush/drop/reopen, the selected root, row count, semantic digest, and
  public values equal the warm result. A cold read performs no publication.

Resource numbers are measured, not invented. The package freezes the exact
counter schema and zero/one constraints; it does not claim a performance win
before the authorized run.

## Quantitative ceilings, targets, and guardrails

The first report must compare each target cell with its named baseline and
publish both measured work and the perfect call-elimination ceiling. The
ceilings are not wall-time promises:

* if the point baseline acquires one coherent view per operation,
  `point-1000` has a snapshot-call ceiling of `(1000-1)/1000 = 99.9%` with a
  one-view target;
* if the mixed baseline commits one mutation at a time, the corrected
  `mixed-savepoint` cohort has a commit-call ceiling of `(192-1)/192 =
  99.479%` with one backend commit;
* fallback, retry, legacy-read/write, and cache-refresh elimination is exactly
  100% because the target count is zero; any nonzero count is a correctness
  blocker;
* range work must publish baseline and target scan rows/bytes, with measured
  elimination `1 - target_authenticated_scan_work / baseline_scan_work`.

A candidate experiment is meaningful only with **at least 10%** improvement in
the targeted wall time, CPU, allocation, backend work, or settled disk measure.
No primary guardrail may regress by more than **5%** in point/range latency,
CPU, allocations, RSS, backend calls/keys/bytes, writes, settled disk, digest,
or cold reopen on either RocksDB or SlateDB. Exact digest or authority failure
overrides any performance win.

## Focused optimization hypotheses

These are falsifiable hypotheses for the future target, not results:

1. **Snapshot count.** One transaction-scoped `CoherentView` should replace
   per-statement or per-row snapshot acquisition. `point-1000` is the decisive
   control: `begin_reads` must remain one while result digest remains exact.
2. **Authenticated gets.** Point reads should retain `O(log_F N)` authenticated
   reads per distinct tree path; ranges should be `O(log_F N + output)`. A
   repeated-key batch may reuse only reader-local authenticated state and must
   remain bound to the same view.
3. **Allocation.** Typed state decoding and one result materialization should
   remove duplicate SQLite row/value buffers. The counter to test is
   `alloc_bytes` per returned row, with RSS and digest as guards.
4. **Publication.** One sorted mutation batch should collapse transaction
   publication overhead to one commit while path-copy work remains
   `O(U log_F N + Z)`. `logical_write_bytes`, object puts, and settled disk must
   expose any path-copy amplification.
5. **UPSERT/RETURNING.** Conflict classification and returned rows should be
   produced from the same staged mutation source, avoiding a second read or
   post-commit requery. `retries`, helper reads, and RETURNING digest expose
   violations.
6. **Range shape.** Ordered authenticated range output should avoid a full
   materialization scan. `scan_rows` must equal returned rows plus explicitly
   documented authenticated boundary work, never an unbounded fallback scan.

The first future gate is a semantic/counter gate, not a broad benchmark:
Memory model/vector → SQLite-shaped target control → ForkTree target, then
RocksDB/SlateDB only if the focused target is runnable. Any digest, cold
reopen, authority, or one-publication failure stops the lane before scaling.

## Explicit exclusions

This package does not modify or qualify `packages/sqlite-storage`, current
main, SQL providers, adapters, ForkTree production readers/writers, or storage
formats. It adds no compatibility reader, migration, dual write, cache
authority, or fallback. No benchmark, model, compile, adapter, or runtime cell
has been run for this package.
