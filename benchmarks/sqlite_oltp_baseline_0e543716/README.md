# Standalone SQLite OLTP baseline — model-independent

Status: **TEST/REPORT-ONLY; SQLite baseline qualified for the focused smoke
cell. No ForkTree comparison was run or claimed.**

This crate is a standalone SQLite control for later pairing with an integrated
ForkTree target. It does not depend on Lix, ForkTree, DataFusion, adapters, or
production storage. The source model anchor is recorded only to explain the
integration gap: `0e543716b0a89d377015ce8cdb1579cad70408ff` is an OLAP-only
benchmark and supplies no OLTP target.

## Exact SQLite binding

The crate pins `rusqlite 0.32.1` with bundled `libsqlite3-sys 0.30.1`, which
was verified at runtime as SQLite `3.46.0`.

```text
libsqlite3-sys crate SHA-256: 2e99fb7a497b1e3339bc746195567ed8d3e24945ecd636e3619d20b9de9e9149
sqlite3.c SHA-256:             c01235302fe80da901fb70c7622c39147e29d9f29b7f6eb746b23517f320c90d
sqlite3.h SHA-256:             d088aa96aa70db50f02acc5c86eca61a5d17556e4c363b9c06079239bf7f87b1
bundled static archive SHA-256: e2532979ce9bde50b950ffb7c63c4f2fc2da72f7499c75afc4275948faa674ca
```

Each cell uses a fresh file and these setup statements. Setup and seed work
are reported separately from `operation_ns`:

```sql
PRAGMA journal_mode = DELETE;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;
PRAGMA recursive_triggers = OFF;
PRAGMA temp_store = MEMORY;
PRAGMA locking_mode = NORMAL;
PRAGMA busy_timeout = 0;
```

## Schema and deterministic workload

The row table is a typed-text primary-key control:

```sql
CREATE TABLE rows(
  id TEXT PRIMARY KEY NOT NULL,
  value TEXT NOT NULL,
  version INTEGER NOT NULL,
  nullable TEXT
) WITHOUT ROWID;
```

The file-row control makes descriptor, BlobRef, snapshot-content, directory,
and tombstone states observable:

```sql
CREATE TABLE file_rows(
  file_id TEXT NOT NULL,
  snapshot_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  descriptor_id TEXT,
  blob_ref BLOB,
  snapshot_content BLOB,
  deleted INTEGER NOT NULL,
  PRIMARY KEY(file_id, snapshot_id)
) WITHOUT ROWID;
```

The focused seed is 1,000 rows (`r000000`–`r000999`), with `nullable` NULL at
indices divisible by 11. The smoke workload contains:

| Cell | Coverage | Result contract |
| --- | --- | --- |
| `point-1000` | 1,000 deterministic point reads | 1,000 rows, zero writes |
| `crud` | 64 INSERT RETURNING, 64 UPDATE RETURNING, 32 DELETE RETURNING | 160 returned rows, one commit boundary |
| `transaction-savepoint` | 8 staged inserts rolled back, 8 committed INSERT RETURNING | rollback has no durable effect; one commit |
| `conflict` | UPSERT update, UPSERT insert, `ON CONFLICT DO NOTHING` | exact RETURNING/no-row behavior |
| `reopen` | seed, close, reopen, digest | warm digest equals cold digest |
| `file-row` | live file update, tombstone, directory row | valid owner/tombstone/directory transitions |
| `corruption` | explicit empty, missing BlobRef, payload tombstone, descriptor mismatch | only explicit empty is accepted; three corruptions reject |

Every result digest is SHA-256 over length-delimited events under domain
`lix.sqlite-oltp.baseline.0e543716.v1\0`. This is a SQLite baseline digest,
not a ForkTree-compatible digest.

## Counters and focused result

The executable emits, per cell:

```text
sqlite_version, seed_rows, logical_ops, returned_rows, result_sha256,
cold_result_sha256, cold_reopen, sql_statements, read_queries,
write_statements, transactions, savepoints, commits, rollbacks,
sqlite_changes, page_count, freelist_pages, file_bytes, setup_ns,
operation_ns, verified
```

Process wall/CPU/RSS and filesystem I/O are captured with `/usr/bin/time -v`.
The direct release smoke run passed all seven cells in 0.05 seconds after
build, with maximum RSS 4,296 KiB. The release compile was separately bounded
and completed in 21.28 seconds; no compile time is included in cell
`operation_ns`.

The exact per-cell digests and counters are frozen in `EVIDENCE_SMOKE.txt`.
All seven cells reported `verified=true`; `reopen` reported identical warm
and cold digests.

## Integration gap map

| Required later comparison | SQLite baseline | 0e model / ForkTree target |
| --- | --- | --- |
| CRUD and DML RETURNING | measured in `crud` | absent; adapter required |
| point reads and ordered ranges | point baseline present; range schema reserved | absent for OLTP |
| transactions/savepoints/rollback | `transaction-savepoint` measured | absent; OLAP fixture writes are not equivalent |
| UPSERT/conflicts | `conflict` measured | absent; stale-owner semantics remain untested |
| close/reopen | `reopen` measured | no OLTP target to reopen |
| file-row mutations/corruption | `file-row` and `corruption` measured | absent; no silent substitution permitted |
| backend counters | SQLite SQL/page/file counters measured | ForkTree counters not collected |

No performance comparison, ForkTree result, adapter result, or production
semantic claim follows from this baseline. The later integrated target must
match the workload and digest contracts before any 1K/10K expansion.
