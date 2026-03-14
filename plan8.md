# Plan 8: Unified Transaction Program

## Goal

Every mutating transaction in Lix must execute with exactly this database shape:

1. `BEGIN`
2. one `preflight`
3. one `write batch`
4. `COMMIT`

The number of SQL statements in the original transaction script must not multiply database work.

A transaction with:

- 1 file update
- 61 file updates
- 200 mixed tracked and untracked writes

must still execute with one preflight and one write batch.

That is the only sound architecture if write cost is meant to be `O(1)` in database roundtrips with respect to statement count.

## Non-Goals

This plan does not preserve backward compatibility with the current transaction execution model.

It does not keep:

- statement-by-statement public transaction execution
- helper-owned transactional side effects
- per-statement tracked append planning
- SQL-text reinspection after semantic planning
- fallback from transaction-wide planning back to per-statement execution

There is one sound way only.

## Root Problem

The current engine still treats a mutating transaction script as a list of independent writes.

Example:

```sql
BEGIN;
UPDATE lix_file SET data = ? WHERE id = ?;
UPDATE lix_file SET data = ? WHERE id = ?;
UPDATE lix_file SET data = ? WHERE id = ?;
COMMIT;
```

Today this becomes:

- one outer transaction
- one tracked preflight for statement 1
- one tracked write batch for statement 1
- one tracked preflight for statement 2
- one tracked write batch for statement 2
- one tracked preflight for statement 3
- one tracked write batch for statement 3

That is still `O(n statements)` database work.

The correct unit of execution is the transaction, not the statement.

## Final Architecture

The engine must compile the whole mutating transaction script into one semantic transaction program:

```rust
TransactionProgram
```

That program is then lowered into one executable transaction plan:

```rust
TransactionTxnPlan {
    preflight: TransactionPreflight,
    write_batch: TransactionWriteBatch,
    result_program: TransactionResultProgram,
    post_commit_effects: PostCommitEffects,
}
```

The runtime executes only this:

1. `BEGIN`
2. `run_preflight(TransactionPreflight)`
3. `run_write_batch(TransactionWriteBatch)`
4. `COMMIT`
5. apply engine-only post-commit effects

No mutating transaction path may execute database work outside those two in-transaction calls.

## Semantic Pipeline

The pipeline is:

1. parse full transaction script
2. canonicalize each statement semantically
3. build one `TransactionProgram`
4. lower `TransactionProgram` into one `TransactionTxnPlan`
5. execute one preflight and one write batch

The planner, not the runtime, owns:

- selector semantics
- exact vs subtree vs generic mutation family
- tracked vs untracked partitioning
- write ordering
- dependency graph
- read visibility rules inside the transaction

The runtime owns only:

- running the preflight
- running the write batch
- applying post-commit engine effects

## Transaction Program

The transaction-level semantic IR should look roughly like:

```rust
TransactionProgram {
    statements: Vec<TransactionStatement>,
    mutations: Vec<TransactionMutation>,
    reads: Vec<TransactionRead>,
    result_slots: Vec<TransactionResultSlot>,
}
```

Important property:

- statement order is preserved semantically
- database execution is not statement-shaped

The transaction program expresses:

- which rows each statement reads
- which rows each statement writes
- how later statements observe earlier writes
- which statement results must be returned to the user

## Statement Semantics Inside a Transaction

The transaction program must preserve SQL-visible statement ordering without requiring per-statement database calls.

For example:

```sql
BEGIN;
UPDATE lix_file SET data = ? WHERE id = '/a';
SELECT data FROM lix_file WHERE id = '/a';
COMMIT;
```

The `SELECT` result must reflect the preceding `UPDATE`.

The sound way to do that is:

- preflight loads the rows needed for the whole transaction
- the engine builds transaction-local semantic state
- each statement is evaluated against that semantic state
- read results are computed from semantic state, not by issuing a database read barrier
- the final persisted state is emitted once in the write batch

So statement barriers remain semantically real, but they are not database barriers.

## One Shared Visibility Model

The transaction planner must use one visibility resolver for all statements in the transaction.

That resolver must answer:

- exact file by id with version ancestry
- exact file by path with version ancestry
- subtree by path prefix
- generic predicate scan
- tracked/untracked overlay visibility

There may not be:

- one visibility model for exact fast paths
- another for generic fallback
- another hidden inside helper SQL

Every statement in the transaction must resolve through the same semantic visibility rules.

## Mutation Families

Each mutation in the transaction must be classified once by the planner:

```rust
enum TransactionMutation {
    Specialized(SpecializedMutationPlan),
    GenericSemantic(GenericMutationPlan),
    Unsupported(UnsupportedMutationReason),
}
```

Examples of specialized families:

- exact file data update by id
- exact file metadata update by id
- exact file delete by id
- exact path rename
- subtree directory delete

Examples of generic semantic families:

- filesystem predicate update not yet specialized
- broad state mutation with a valid semantic selector

The planner may choose `GenericSemantic`.

The runtime may not silently widen `Specialized` into generic fallback behavior.

## Preflight

The preflight must be transaction-wide.

It must load all DB state required to evaluate the whole transaction:

- current tips / append replay state
- exact file descriptors and payload refs for all exact ids in the transaction
- subtree membership for explicit subtree operations
- active version / active account / deterministic state if required
- any schema / foreign-key / validation inputs needed for the whole transaction

The preflight must be shaped by semantic need, not statement count.

For the 61-update formatting commit, the preflight should conceptually be:

- one lookup for all 61 file ids
- one append/tip preflight for the transaction lane
- any other global transaction inputs once

Not:

- 61 exact-file preflights
- 61 append preflights

## Write Batch

The write batch must persist the final transaction result once.

It should include:

- all tracked commit rows
- all tracked state rows
- all untracked state rows
- all payload CAS rows
- all idempotency rows
- all deterministic-sequence persistence
- all observe-tick rows

No separate transactional helper writes are allowed outside the batch.

For the 61-update formatting commit, the batch should be one commit-level append containing the changes for all 61 files, not 61 append batches.

## Result Program

A transaction still returns one result per original statement.

That must come from a result program computed against semantic transaction state:

```rust
TransactionResultProgram {
    statement_results: Vec<StatementResultPlan>,
}
```

Examples:

- mutating statement returns row count
- `RETURNING` returns rows projected from semantic post-statement state
- `SELECT` inside the transaction returns rows from semantic state after earlier mutations

This preserves SQL semantics without per-statement database execution.

## What Must Be Deleted

The following model must be removed as a valid execution shape:

- per-statement `prepare_execution_with_backend(...)` for mutating public transaction scripts
- per-statement `build_write_txn_plan(...)` inside a transaction script loop
- per-statement `run_write_txn_plan_with_transaction(...)` for public scripts
- SQL-text side-effect recovery after semantic planning
- helper-owned transactional payload persistence

In practical terms, the current public script path in:

- [statement_scripts.rs](/Users/samuel/git-repos/lix-2/packages/engine/src/sql/execution/statement_scripts.rs)
- [transaction_exec.rs](/Users/samuel/git-repos/lix-2/packages/engine/src/sql/execution/transaction_exec.rs)

must be replaced by transaction-wide compilation and execution.

## Soundness Rules

The final architecture must obey:

1. Every mutating transaction is compiled as a whole.
2. There is exactly one transaction preflight.
3. There is exactly one transaction write batch.
4. No mutating transaction may issue statement-shaped DB writes.
5. No runtime helper may rediscover write semantics from SQL text.
6. Exact families may not widen into broad query fallbacks.
7. Generic semantic plans are explicit planner output, not runtime recovery.
8. Statement ordering is preserved semantically, not through per-statement DB calls.

## Complexity Target

For any mutating transaction:

- database roundtrips inside the transaction are `O(1)`
- database roundtrips do not scale with statement count
- database roundtrips do not scale with changed file count

The unavoidable scaling is in:

- preflight result size
- write batch size
- engine-side semantic computation

But not in:

- number of database calls
- number of append batches

## Validation

This architecture is correct when:

1. A transaction with 1 file update and a transaction with 61 exact file updates both execute with:
   - one preflight
   - one write batch
2. A transaction with interleaved reads and writes returns correct read results without issuing DB read barriers between statements.
3. Public replay commits no longer show one tracked append batch per file.
4. The trace for mass-formatting commits shows:
   - one preflight
   - one write batch
   - one commit
5. No mutating transaction path calls statement-by-statement public write execution.

## Progress Log

- Checkpoint 1:
  Replaced the public mutating transaction-script runner in [statement_scripts.rs](/Users/samuel/git-repos/lix-2/packages/engine/src/sql/execution/statement_scripts.rs) with a transaction-wide combined `WriteTxnPlan` path for mutating public scripts. Public mutating scripts no longer call `execute_with_options_in_transaction(...)` once per statement.

- Checkpoint 2:
  Added script-level plan concatenation and runtime rebinding in [write_txn_plan.rs](/Users/samuel/git-repos/lix-2/packages/engine/src/sql/execution/write_txn_plan.rs). Statement-local write plans are now merged into one transaction-wide plan and share one deterministic runtime function provider, which fixes duplicate `lix_internal_change.id` generation inside combined tracked scripts.

- Checkpoint 3:
  Validation passed for [transaction_execution.rs](/Users/samuel/git-repos/lix-2/packages/engine/tests/transaction_execution.rs): `40 passed; 0 failed`. A 20-commit release replay smoke on `paraglide-js` also passed.

- Checkpoint 4:
  A traced 400-commit release replay on `paraglide-js` completed successfully. The previous 61-update outlier commit `66afe5ba4b2e8c24a1c49fc712316cc0ad69ba80` dropped to:
  - `execute_ms = 31.59 ms`
  - `total_ms = 46.85 ms`
  This is down from the earlier `~600 ms execute` shape caused by one tracked append per statement.

- Checkpoint 5:
  The current trace for `66afe5...` now shows one transaction-wide tracked append preflight instead of 61 tracked append preflights. The remaining work is inside the logical write batch: payload CAS persistence still expands into many physical `transaction.execute(...)` calls for blob manifest/store/chunk rows. The statement-count-driven append explosion is fixed, but the write batch is not yet a single physical SQL call.
