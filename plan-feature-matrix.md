# Lix Feature Matrix

Single table for the Rust plan status (JS SDK intentionally omitted).

Legend:

- **✓** = available
- **partial** = available but incomplete
- **todo** = planned / not available yet
- **not in plan** = not covered yet

| Feature                                        | Status      |
| ---------------------------------------------- | ----------- |
| `lix.execute()` (pass-through SQL)             | todo        |
| SQLite backend                                 | todo        |
| Postgres backend                               | todo        |
| SQL parse/serialize normalization (DataFusion) | todo        |
| Untracked state                                | todo        |
| Materialized state tables per schema           | todo        |
| Vtable SELECT rewriting                        | todo        |
| Vtable INSERT/UPDATE/DELETE rewriting          | todo        |
| Change history + snapshots                     | todo        |
| JSON Schema validation                         | todo        |
| Constraint validation (PK/UNIQUE/FK)           | todo        |
| `state_by_version` view                        | todo        |
| `state` view                                   | todo        |
| `state_history` view                           | todo        |
| `entity_by_version` view                       | todo        |
| `entity` view                                  | todo        |
| `entity_history` view                          | todo        |
| File/directory schema (metadata)               | todo        |
| File content materialization                   | todo        |
| Plugin registry + detect/apply changes         | todo        |
| Checkpoints                                    | not in plan |
| Deterministic mode                             | not in plan |
| Hooks / observe                                | not in plan |
| Server protocol handler / sync                 | not in plan |
