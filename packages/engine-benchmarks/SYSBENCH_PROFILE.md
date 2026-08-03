# Sysbench-derived OLTP comparison profile

Status: benchmark contract. The existing `tracked_state_crud` benchmark is a
component-level qualification harness; results from it must not be presented as
Sysbench results.

## Claim boundary

The suite compares current-state SQL performance in Lix with always-on tracked
history against conventional SQLite and PostgreSQL current-state tables. It is
described as **Sysbench 1.0.20 OLTP-derived, common-feature profile**. It is not
an official or unmodified Sysbench result because Sysbench has no Lix driver
and the common profile deliberately removes features unavailable in Lix.

The first publication does not claim feature-equivalent version control for
SQLite or PostgreSQL. A later suite measures branching, diff, merge, rollback,
and historical queries separately.

## Systems under test

- `lix-slatedb`: the public Lix SQL session backed by local durable SlateDB;
  tracked history and normal commit semantics remain enabled.
- `sqlite`: standalone SQLite in WAL mode through its in-process driver.
- `postgres`: PostgreSQL through a loopback client connection.

All three are driven from the same benchmark process. The embedded versus
client/server boundary is part of the measured system and must be stated beside
published charts. Remote object-store latency and the Lix HTTP protocol are
separate product benchmarks and are not mixed into this engine comparison.

## Common schema

The logical schema follows Sysbench's `sbtest` table:

```sql
CREATE TABLE sbtest1 (
  id BIGINT NOT NULL PRIMARY KEY,
  k BIGINT NOT NULL,
  c VARCHAR(120) NOT NULL,
  pad VARCHAR(60) NOT NULL
);
```

The profile corresponds to `--auto-inc=off --create-secondary=off`. Secondary
index maintenance cannot be compared until Lix exposes a corresponding public
index feature. Every engine receives identical deterministic values and row
order. Load, schema registration, connection setup, and warmup are outside the
measurement interval. PostgreSQL uses reusable prepared statements, SQLite uses
its prepared-statement cache, and Lix includes the per-event parse/plan costs
imposed by its public SQL API. That difference is recorded as part of the tested
system.

## Workloads

The initial CRUD publication includes the operation definitions from Sysbench
1.0.20:

- `oltp_point_select`: one primary-key point select per event.
- `oltp_insert`: one insert per event into a reserved, non-colliding key range.
- `oltp_update_index`: `UPDATE ... SET k=k+1 WHERE id=?`.
- `oltp_update_non_index`: replace `c` by primary key.
- `oltp_delete`: one primary-key delete per event from an independently seeded
  disposable key range.

The follow-up mixed suite adds the exact read-only, write-only, and read/write
transaction composition: ten point reads; one each of simple, sum, ordered,
and distinct range reads; one indexed-column update; one non-indexed-column
update; and one delete/insert pair where applicable.

## Fixed publication matrix

- Sysbench reference: tag `1.0.20`.
- Table size: 1,000 for automated qualification; 100,000 and 1,000,000 for
  publication.
- Access distribution: uniform with a recorded 64-bit seed.
- Clients: 1, 4, and 16.
- Warmup: 15 seconds.
- Measurement: 60 seconds.
- Repetitions: at least five fresh, independently loaded databases per point,
  run in counterbalanced engine order.
- Durability: defaults are retained and recorded; no `fsync` or synchronous
  commit relaxation is allowed.

Each event is atomic. The runner performs no automatic retries; failed or
conflicted events are counted and reported rather than silently removed.

## Required report fields

Every machine-readable result records engine and library versions, Git
revision and dirty state, database configuration, workload parameters, seed,
start/end Unix timestamps, successful and failed events, retries, throughput,
p50/p95/p99/max latency, logical row count, retained history/commit count where
available, and physical storage bytes before and after the run. The matrix
manifest records OS/kernel, CPU, memory, block devices, filesystem, invocation,
run order, and the exact result files.

Publication charts use absolute values with uncertainty across independent
repetitions. Relative multipliers are secondary annotations. Raw JSON results,
runner source, and the exact reproduction command ship with the post.
