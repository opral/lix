# SQL changelog runtime qualification — e1af successor contract

This is a TEST/REPORT-only, dormant qualification package. It is anchored to
accepted production head `e1af471b9ab0f598dafa7c2ddec7867667c81740` and the
approved SQL changelog v4 semantics. It does not contain production code and
must not be run against e1af: the source gate is expected RED until a
compiler-green production successor exists.

## Frozen provenance

```text
accepted production head/tree:
  e1af471b9ab0f598dafa7c2ddec7867667c81740 /
  bfa0d271a723da8250ab76ada16fda90926f1099
accepted production parent:
  b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
e1af parent..head full-index SHA-256:
  9795ee3da81a06657a45a47a50417522a6a6bd7057e21eeb75597096417c9f3c
e1af stable patch-id:
  31cc575644bf17e65c59d558a03acffc848c2e20

approved v4 oracle head/tree:
  d36e1fe34c4bf93c01478b876a5f73a8dccd33de /
  7fcaeb5f965d5c8b641e5e82a741c75cc18365b6
v4 parent..head full-index SHA-256:
  4f73b5a944ea15b9d0ed6c040ef80fa60f7631437c2fd22ce1bf5ae6a97798d0
v4 stable patch-id:
  212a1ff4a6f050905e5d62f94f78ce2e76d5fc24
v4 exact fd2 RED SHA-256:
  74d2a1d2512ece658aa213e235142935c161a81bd3d859b2c1ffa8ae2006c0a5

accepted source-binding package:
  c34699d08cd090b6b6dc5f92322baf94475b39b2 /
  baa1e3be8b9ea9b7652e763161a6f80760ca64b8
source-binding package full-index SHA-256:
  17c0d040d7aee79c74e48223003790d57bcd06ecc4784bd30ec2f99564c4f8cf
```

## Dormant execution guard

Do not execute any command below until all conditions hold:

1. The candidate is an immutable child of e1af and the v2 source-binding
   verifier returns `SOURCE_BINDING=PASS`.
2. The candidate's bounded library and test-aware compiler checks are green;
   no compiler-red or inherited diagnostic is being treated as a runtime
   qualification.
3. The candidate supplies the named test-only target
   `packages/engine-benchmarks/tests/sql_changelog_runtime_qualification.rs`
   and its instrumentation fields. That target is intentionally absent from
   this accepted e1af tree.
4. The test worktree is detached and clean; each adapter has a fresh isolated
   database path; seed and warmup are excluded from all measurements.

Any missing target, source RED, compile error, semantic mismatch, or malformed
fixture stops the lane before widening. No current e1af runtime result is
claimed by this package.

## Exact focused commands, in order

Each cell has a hard 20-minute cap (`timeout 1200`). The commands are exact
future commands, not executions or results. Use separate target/database
paths so compiler artifacts and adapter state cannot cross-contaminate cells.

### Memory

```bash
LIX_CHANGE_BACKEND=memory \
LIX_CHANGE_ROWS=1000 \
LIX_CHANGE_HISTORY_DEPTH=4 \
LIX_CHANGE_FIXTURE=runtime-v1 \
CARGO_TARGET_DIR=/tmp/lix-stage2-sql-change-runtime-memory-target \
timeout 1200 cargo test -p lix_benchmarks \
  --test sql_changelog_runtime_qualification \
  --features 'storage-benches slatedb' -- \
  --ignored --exact memory_sql_changelog_runtime \
  --nocapture --test-threads=1
```

### RocksDB

```bash
LIX_CHANGE_BACKEND=rocksdb \
LIX_CHANGE_ROWS=1000 \
LIX_CHANGE_HISTORY_DEPTH=4 \
LIX_CHANGE_FIXTURE=runtime-v1 \
CARGO_TARGET_DIR=/tmp/lix-stage2-sql-change-runtime-rocks-target \
timeout 1200 cargo test -p lix_benchmarks \
  --test sql_changelog_runtime_qualification \
  --features 'storage-benches slatedb' -- \
  --ignored --exact rocksdb_sql_changelog_runtime \
  --nocapture --test-threads=1
```

### SlateDB

```bash
LIX_CHANGE_BACKEND=slatedb \
LIX_CHANGE_ROWS=1000 \
LIX_CHANGE_HISTORY_DEPTH=4 \
LIX_CHANGE_FIXTURE=runtime-v1 \
CARGO_TARGET_DIR=/tmp/lix-stage2-sql-change-runtime-slate-target \
timeout 1200 cargo test -p lix_benchmarks \
  --test sql_changelog_runtime_qualification \
  --features 'storage-benches slatedb' -- \
  --ignored --exact slatedb_sql_changelog_runtime \
  --nocapture --test-threads=1
```

The harness must print one machine-readable `QUALIFICATION_JSON` record per
cell and retain the raw log. It must report setup, warmup, measured query,
cold-reopen, and final-close phases separately. No 10K/50K or adapter matrix
is authorized by this package before all three focused cells pass.

## Public SQL contract

The harness executes the same deterministic fixture and records canonical
result digests for these public surfaces:

```text
change_direct:
  SELECT id, account_id, entity_pk, schema_key, file_id, metadata,
         created_at, origin_key, snapshot_content
  FROM lix_change
  WHERE entity_pk = lix_json('["runtime-row-0001"]')
  ORDER BY created_at, id

change_commit:
  SELECT id, entity_pk, schema_key, file_id, snapshot_content
  FROM lix_change
  WHERE schema_key = 'lix_commit'
  ORDER BY created_at, id

history:
  SELECT id, key, value, lixcol_change_id, lixcol_as_of_commit_id
  FROM lix_key_value_history()
  WHERE id = 'runtime-row-0001'
  ORDER BY lixcol_as_of_commit_id, lixcol_change_id

diff:
  SELECT diff_id, entity_pk, schema_key, file_id, diff_type,
         before_change_id, after_change_id
  FROM lix_diff(:from_commit_id, :to_commit_id)
  ORDER BY schema_key, entity_pk, diff_type, diff_id
```

The harness also runs each applicable query with `LIMIT 0`, `LIMIT 1`, and
`LIMIT 2`. Digest input is canonical JSON containing query ID, ordered column
names, typed row values, and nulls; object keys are sorted and arrays retain
their SQL order. The digest is SHA-256 of UTF-8 canonical JSON. A combined
digest is the SHA-256 of sorted `query_id=digest` lines. Timestamp and UUIDv7
inputs are deterministic fixture values, not wall-clock values.

The v4 ten-case semantic matrix is frozen in `SQL_RUNTIME_CASES.tsv` and
retains direct/derived rows, authenticated absence, missing/malformed,
wrong-kind/domain, wrong embedded identity, duplicate logical ID, canonical
merge order, limit-after-merge, and one caller-owned read.

## Measurement and fail-closed contract

For every measured query and the cold-reopen replay, emit:

```text
wall_ns, process_cpu_ns
alloc_calls, alloc_bytes
peak_rss_bytes, settled_rss_bytes
begin_read_calls, retained_read_scope_count, facade_construction_count
backend_get_calls, backend_get_many_calls, backend_scan_pages
backend_object_reads, backend_read_bytes
begin_write_calls, backend_write_calls, backend_write_bytes
logical_commit_calls, flush_calls, compaction_calls
settled_disk_bytes, post_close_disk_bytes
result_digest, combined_digest
```

The read phase must have exactly one coherent retained read and one
operation-owned ForkTree facade: `begin_read_calls=1`,
`retained_read_scope_count=1`, `facade_construction_count=1`. Provider/helper
acquisition counters must be zero after the operation-owned read is created.
All read-phase writes are hard failures:
`begin_write_calls=0`, `backend_write_calls=0`, `backend_write_bytes=0`,
`logical_commit_calls=0`, and `flush_calls=0`. Adapter-specific physical
metadata reads are reported, not hidden; SlateDB object/request counts and
bytes are mandatory. Settled and post-close disk are measured after the
database is closed, with LSM/object-store files disclosed.

No latency or resource result is accepted without matching result and cold-
reopen digests on Memory, RocksDB, and SlateDB. No result is accepted if a
corruption error becomes an empty result, cache miss, fallback, reset, or
successful write.

## Stop conditions

Stop the package immediately on any one of: source/compiler RED, missing
runtime target, a second read/facade, any read-phase write, digest divergence,
reordered or duplicated output, pre-limit truncation, authenticated absence
confused with missing required data, wrong-kind/domain acceptance, malformed
or substituted object acceptance, cold-reopen divergence, or a timeout.

This package is qualification-only. It authorizes no production edit, PR,
merge, compatibility reader, cache, fallback, persisted format, or second
authority.
