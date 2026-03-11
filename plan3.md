The problem:

- Explicit BEGIN ... COMMIT scripts still execute one statement at a time in statement_scripts.rs:102.
- Each tracked sql2 write independently calls append_commit_if_preconditions_hold in shared_path.rs:563.
- EngineTransaction::execute() uses the same per-statement path, so this is not just the SQL wrapper case; it affects
  transaction handles and callback transactions too in engine.rs:148.

What the extra commit is:

- Not a global-admin commit.
- It is a second normal tracked commit on the same concrete version lane.
- The active version pointer is advanced twice.
- The global version pointer stays on bootstrap.

What I would not do:

- I would not “fix” this by loosening tests.
- I would not use statement coalescing or multi-row insert support as the main fix.
- That only patches the happy path for repeated inserts and does not fix:
  - tx.execute() across multiple calls
  - mixed insert/update/delete scripts
  - read-after-write semantics inside the same transaction
  - duplicate-key / ON CONFLICT sequencing semantics

The clean cut I recommend:

1. Add a transaction-scoped sql2 append session keyed by concrete append lane.
2. First tracked sql2 write in a DB transaction creates the commit/change_set/version-pointer as today.
3. Later tracked sql2 writes on the same lane merge into that pending commit instead of appending a new one.
4. If the lane changes, flush batching and start a new pending commit.
5. Keep legacy execution for non-sql2 surfaces, but stop making sql2 tracked writes statement-scoped.

Why this is the right seam:

- It preserves transaction visibility, because the first write already materializes rows inside the open DB transaction.
- It fixes both BEGIN ... COMMIT scripts and EngineTransaction.
- It matches the semantic model: transaction boundary owns commit scope, not statement boundary.

Concretely, the new unit should own:

- pending concrete lane
- pending commit_id
- pending change_set_id
- merge logic for additional domain changes into the existing commit snapshot/materialized rows

That merge path needs to:

- insert new business lix_internal_change rows
- insert new materialized state rows with the existing commit_id
- insert new lix_change_set_element rows for the existing change_set_id
- update the materialized lix_commit snapshot’s change_ids
- not create a second lix_version_pointer meta-change

Net:

- sql2 planner architecture is still the right direction
- the execution cut is wrong today
- the fix should be a transaction-scoped sql2 append session, not a coalescing hack

## Progress log

- 2026-03-09 18:26 PST - initial plan draft
- 2026-03-10 09:42 PST - started implementation; wiring a transaction-scoped sql2 append session through EngineTransaction and explicit BEGIN/COMMIT script execution
- 2026-03-11 09:22 PDT - verified single-commit behavior end to end; explicit BEGIN/COMMIT and EngineTransaction multi-call regressions now pass across sqlite, postgres, and materialization
- 2026-03-11 11:05 PDT - enabled sql2 ON CONFLICT DO NOTHING for public state/entity inserts, including explicit-version paths; tracked no-op inserts now skip empty commit generation
- 2026-03-11 12:02 PDT - added first-principles sql2 support for `lix_json(...)` values and restored builtin `lix_stored_schema_by_version` entity routing so stored-schema `ON CONFLICT DO NOTHING` now works through sql2 across sqlite, postgres, and materialization
- 2026-03-11 14:11 PDT - kept entity writes on sql2 and closed the current regression set: entity writes with `lixcol_global=true` now bind to the global lane, entity `DEFAULT VALUES` applies schema defaults, multi-row entity inserts resolve on sql2, and selector-driven entity update/delete now handle nested public read subqueries
