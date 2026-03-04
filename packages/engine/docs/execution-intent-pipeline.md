# Execution Intent Pipeline

## Goal

Interpret file-write intent once, as typed data, then drive execution, cache behavior, and side effects from that typed intent. Avoid string-based routing that can drift across phases.

## Pipeline

1. Parse SQL into AST statements.
2. Build one `ExecutionIntent` from statements + bound params.
3. Build execution plan from the same statements + intent-derived detected changes.
4. Execute statements with barriers (multi-statement transaction calls execute statement-by-statement).
5. Run planner-owned postprocess followup for tracked domain changes (`Vtable*` or `DomainChangesOnly`).
6. Apply remaining side effects from typed intent (`pending_file_writes`, delete targets, untracked changes).
7. Derive cache refresh/invalidation targets from intent + plan effects.
8. Emit post-commit stream updates.

## Core Types

- `sql/execution/intent.rs`
  - `ExecutionIntent`
  - `IntentCollectionPolicy`
  - `collect_execution_intent_with_backend(...)`
- `sql/execution/shared_path.rs`
  - `PreparedExecutionContext { intent, plan, ... }`
  - `derive_cache_targets(...)`

## Cache Target Semantics

`derive_cache_targets(...)` enforces two distinct invalidation domains:

- `file_data_cache_invalidation_targets`
  - refresh targets
  - descriptor eviction targets
  - delete targets
- `file_path_cache_invalidation_targets`
  - refresh targets
  - descriptor eviction targets
  - delete targets
  - (does not include data-write-only targets)

Authoritative file data writes are write-through (`lix_internal_file_data_cache` + CAS rows) and are not invalidation targets.

## No-Stringly Rules (Current)

- Placeholder advancement for side-effect analysis uses AST traversal (`advance_placeholder_state_for_statement_ast`) instead of rendering statement SQL text and rebinding it.
- Intent routing/caching logic does not use SQL string matching to infer write semantics.

## Safety Contract

For authoritative file writes, engine verifies persisted `lix_binary_blob_ref` hash/size against intended bytes and persists full payload in `lix_internal_binary_blob_store` and file cache. Verification checks/failures are counted by internal intent telemetry counters.

## Maintenance

Guardrail tests in `tests/sql_guardrails.rs` enforce:

- no reintroduction of legacy string-matched postprocess fallback
- no legacy pipeline module wiring
- side-effect placeholder advancement remains AST-based (no `statement.to_string()` rebinding path)
