---
date: "2025-11-24"
---

# Preprocess writes to avoid vtable overhead

## Summary

Write operations in Lix are slow due to the vtable mechanism crossing the JS ↔ SQLite WASM boundary multiple times per row. This RFC proposes extending the existing SQL preprocessor to handle writes, bypassing [SQLite's Vtable mechanism](https://www.sqlite.org/vtab.html) entirely.

## Background & Current Architecture

### How We Got Here

Lix evolved organically from application requirements:

1. **Git era**: Initially built on git, which [proved unsuited despite the ecosystem appeal](https://opral.substack.com/p/building-on-git-was-our-failure-mode).

2. **SQLite migration**: Rewrote on top of SQLite to gain ACID guarantees, a storage format, and a query engine.

3. **DML triggers**: Early prototypes used triggers on regular tables to track changes.

4. **VTable adoption**: The requirement to control transaction and commit semantics led to [SQLite's vtable mechanism](https://www.sqlite.org/vtab.html) to intercept reads and writes.

5. **Read performance fix**: VTables can't be optimized by SQLite (no filter pushdown for `json_extract`, etc.). A preprocessor was built ([#3723](https://github.com/opral/monorepo/pull/3723)) that rewrites SELECT queries to target real tables, achieving native read performance.

6. **Current state**: Reads are fast. Writes remain slow because they still hit the vtable.

### Current Data Model

Lix has a unified read/write interface via the virtual table `lix_internal_state_vtable`.

Underneath the vtable, the state is spread across four groups of physical tables:

1. **Change History** – `lix_internal_change`

   - Stores the history of changes which are used to materialize the committed state.
   - The foundation of the system.

2. **Transaction state** – `lix_internal_transaction_state`

   - Uncommitted changes (“staging area”) visible via the vtable before commit.

3. **Untracked state** – `lix_internal_state_all_untracked`

   - Local-only changes; not synced; coexist with transaction/committed rows.

4. **Committed state** – `lix_internal_state_cache_v1_*`
   - Schema-partitioned cache tables representing immutable history, optimized for reads. Materialized from `lix_internal_change`.

Conceptually:

```
┌─────────────────────────────────────────────────────────────────┐
│                    lix_internal_state_vtable                    │
│                      (unified read/write interface)             │
└─────────────────────────────────────────────────────────────────┘
                                 │
                    ┌────────────┼────────────────────────┐
                    ▼            ▼                        ▼
        ┌───────────────┐ ┌───────────┐ ┌─────────────────────────────┐
        │  Transaction  │ │ Untracked │ │      Committed State        │
        │    State      │ │   State   │ │      (cache tables)         │
        │   (staging)   │ │  (local)  │ │                             │
        └───────────────┘ └───────────┘ └──────────────▲──────────────┘
              │                │                       │
              │                │            ┌──────────┴──────────┐
              │                │            │ lix_internal_change │
              │                │            │   (change history)  │
              │                │            └─────────────────────┘
              │                │                       │
              └────────────────┴───────────────────────┘
                               │
                        Prioritized UNION
                      (transaction > untracked > committed)
```

### Current Read Path (Fast)

```
App Query                    Preprocessor                    SQLite
    │                            │                             │
    │  SELECT * FROM vtable      │                             │
    │ ─────────────────────────► │                             │
    │                            │  Rewrite to UNION of        │
    │                            │  physical tables            │
    │                            │ ──────────────────────────► │
    │                            │                             │
    │  ◄─────────────────────────────────────────────────────  │
    │         Results (native speed)                           │
```

The preprocessor intercepts SELECT queries and rewrites them into a `UNION` query combining the three physical tables, using `ROW_NUMBER()` to prioritize uncommitted/untracked changes.

_Example rewritten query:_

```sql
-- User writes:
SELECT * FROM lix_internal_state_vtable
WHERE schema_key = 'lix_key_value'

-- Preprocessor rewrites to (pseudocode):
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY entity_pk
    ORDER BY priority
  ) AS rn
  FROM (
    -- Priority 1: Uncommitted transaction state
    SELECT *, 1 AS priority
    FROM lix_internal_transaction_state
    WHERE schema_key = 'lix_key_value'

    UNION ALL

    -- Priority 2: Untracked state
    SELECT *, 2 AS priority
    FROM lix_internal_state_all_untracked
    WHERE schema_key = 'lix_key_value'

    UNION ALL

    -- Priority 3: Committed state from schema-specific cache table
    SELECT *, 3 AS priority
    FROM lix_internal_state_cache_v1_lix_key_value
  )
) WHERE rn = 1
```

### Current Write Path (Slow)

```
App Query                    SQLite                      JavaScript
    │                          │                             │
    │  INSERT INTO vtable      │                             │
    │ ───────────────────────► │                             │
    │                          │  xUpdate() callback         │
    │                          │ ──────────────────────────► │
    │                          │                             │  ┌─────────────────┐
    │                          │  SELECT (validation)        │  │ Per-row loop:   │
    │                          │ ◄────────────────────────── │  │  • 1 timestamp  │
    │                          │                             │  │  • 3-5 schema   │
    │                          │  SELECT (FK check)          │  │  • N FK checks  │
    │                          │ ◄────────────────────────── │  │  • N unique     │
    │                          │                             │  │  • 1 insert     │
    │                          │  INSERT (transaction state) │  └─────────────────┘
    │                          │ ◄────────────────────────── │
    │                          │         ...repeat...        │
```

Each write triggers `xUpdate` in JavaScript, which executes multiple synchronous queries back into SQLite for validation.

### Query Breakdown Per Write

From [`validate-state-mutation.ts`](https://github.com/opral/monorepo/blob/bbcb3b551f4d5cbf47f52eb8bc2846c3a5c0c411/packages/lix/sdk/src/state/vtable/validate-state-mutation.ts) and [`vtable.ts`](https://github.com/opral/monorepo/blob/bbcb3b551f4d5cbf47f52eb8bc2846c3a5c0c411/packages/lix/sdk/src/state/vtable/vtable.ts):

| Phase                    | Queries                  |
| ------------------------ | ------------------------ |
| Timestamp                | 1                        |
| Version existence check  | 1                        |
| Schema retrieval         | 1-2                      |
| JSON Schema validation   | (in-memory via AJV)      |
| Primary key uniqueness   | 1                        |
| Unique constraints       | 1 per constraint         |
| Foreign key constraints  | 2-3 per FK               |
| Transaction state insert | 1                        |
| File cache update        | 0-2 (if file_descriptor) |

**Total: ~10-25 queries per row**, depending on schema complexity.

## Problem

**Write performance is poor.** A single logical write from the application results in:

1. One JS ↔ WASM boundary crossing to enter `xUpdate`
2. 10-25 internal SQL queries inside `xUpdate` for validation and bookkeeping
3. Each internal query crosses the JS ↔ WASM boundary again

For bulk operations, this scales linearly: inserting 1,000 rows triggers 1,000 `xUpdate` calls and 10,000-25,000 boundary crossings.

### Quantifying the Problem

Based on the existing benchmark suite (`vtable.insert.bench.ts`, `commit.bench.ts`):

| Operation            | Current Behavior (bench.base.json)      |
| -------------------- | --------------------------------------- |
| Single row insert    | ~15.3ms (state_by_version insert)       |
| 10-row chunk insert  | ~39ms (state_by_version 10-row chunk)   |
| 100-row chunk insert | ~344ms (state_by_version 100-row chunk) |

**Target**: A single mutation that writes ~100 rows should complete in <50ms.

Why this target:

- 50ms leaves another ~50ms in the 100ms UI budget for other work (rendering, effects).
- 100 rows matches typical document transactions and keeps bulk edits responsive.

## Proposal

Extend the preprocessor to handle `INSERT`, `UPDATE`, and `DELETE` statements, bypassing the vtable entirely.

### Write Path

```
App Query                    Preprocessor                    SQLite
    │                            │                             │
    │  INSERT INTO vtable        │                             │
    │ ─────────────────────────► │                             │
    │                            │  1. Parse SQL               │
    │                            │  2. Extract mutation rows   │
    │                            │  3. JSON Schema validate    │
    │                            │     (in-memory)             │
    │                            │  4. File change detection   │
    │                            │     (plugin callbacks)      │
    │                            │  5. Build bulk SQL with     │
    │                            │     constraint checks       │
    │                            │ ──────────────────────────► │
    │                            │    Single optimized query   │
    │  ◄─────────────────────────────────────────────────────  │
    │         Done (single boundary crossing)                  │
```

### Pseudocode Flow

```typescript
async function execute(sql: string): Promise<string> {
  // 1. Parse the incoming SQL
  const ast = parse(sql);
  if (!isMutation(ast)) return sql; // Pass through

  // 2. Extract target table and mutation type
  const { table, operation, rows } = extractMutation(ast);

  // 3. Resolve values (handle subqueries, defaults, etc.)
  const resolvedRows = await resolveRowValues(ast, rows);

  // 4. In-memory JSON Schema validation
  for (const row of resolvedRows) {
    const schema = getStoredSchema(row.schema_key);
    validateJsonSchema(row.snapshot_content, schema); // throws on error
  }

  // 5. File change detection (for file mutations)
  const detectedChanges: MutationRow[] = [];
  for (const row of resolvedRows) {
    if (row.schema_key === "lix_file") {
      const plugin = getMatchingPlugin(row);
      const changes = plugin.detectChanges({
        after: row.snapshot_content,
        // Provide a query function that reads from pending state
        querySync: createPendingStateQuery(resolvedRows),
      });
      detectedChanges.push(...changes);
    }
  }
  const allRows = [...resolvedRows, ...detectedChanges];

  // 6. Build optimized SQL with constraint validation
  const targetTable = determineTargetTable(allRows); // transaction_state or untracked

  const optimizedSql = `
    -- Constraint validation (fails entire transaction on violation)
    SELECT CASE
      WHEN EXISTS (${buildForeignKeyValidation(allRows)})
      THEN RAISE(ABORT, 'Foreign key constraint failed')
    END;

    SELECT CASE
      WHEN EXISTS (${buildUniqueConstraintValidation(allRows)})
      THEN RAISE(ABORT, 'Unique constraint failed')
    END;

    -- Bulk insert into physical table
    INSERT INTO ${targetTable} (entity_pk, schema_key, file_id, ...)
    VALUES ${allRows.map(formatRow).join(", ")}
    ON CONFLICT (entity_pk, schema_key, file_id, version_id)
    DO UPDATE SET snapshot_content = excluded.snapshot_content, ...;
  `;

  const result = sqlite.exec(optimizedSql);

  emit("onStateCommit", allRows);
  return result;
}
```

### Benefits

1.  **No VTable Overhead**: By bypassing `xUpdate` and `xCommit`, we eliminate the costly JS ↔ WASM boundary crossings for every row.
2.  **Elimination of `lix_internal_transaction_state`**: Since we write directly to the physical tables within the user's transaction, we no longer need a separate table to stage uncommitted changes. The underlying SQL database handles the transaction isolation for us.
3.  **Bulk Performance**: Batch inserts (e.g., `INSERT INTO ... VALUES (...), (...)`) are handled as a single efficient SQL operation. In the vtable approach, SQLite loops and calls `xUpdate` for _each row_ individually, preventing bulk optimizations.

### Downsides / Risks

1.  **Complexity of SQL Rewriting**: The preprocessor must correctly parse and rewrite potentially complex SQL statements, including handling edge cases.

### Bonus: Using Postgres or any other SQL database as the execution provider

Relying purely on preprocessing rather than SQLite (WASM build) specific APIs enables Lix to delegate rewritten SQL to PostgreSQL, Turso, MySQL, or another SQL database.

```ts
const lix = await openLix({
  environment: new PostgreSQL({ ... }),
});
```

For NodeJS we wouldn't need to build a SQLite WASM <-> FS bridge.

Instead, we can use `better-sqlite3` or `node-postgres` directly. Of which the performance is significantly better than the WASM build which, for example, lacks WAL mode.

```ts
const lix = await openLix({
  environment: new BetterSqlite3({ ... }),
});
```
