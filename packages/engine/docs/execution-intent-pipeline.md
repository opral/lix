# Execution Intent Pipeline

## Goal

Interpret file-write intent once, as typed data, then drive execution, cache behavior, and side effects from that typed intent. Avoid string-based routing that can drift across phases.

## Pipeline

1. Parse SQL into AST statements.
2. Build one `ExecutionIntent` from statements + bound params.
3. Build execution plan from the same statements + intent-derived detected changes.
4. Execute backend SQL.
5. Apply side effects from typed intent (`pending_file_writes`, delete targets, detected changes).
6. Derive cache refresh/invalidation targets from intent + plan mutations.
7. Emit post-commit stream updates.

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
  - intent write targets (authoritative data writes)
- `file_path_cache_invalidation_targets`
  - refresh targets
  - descriptor eviction targets
  - delete targets
  - (does not include data-write-only targets)

This prevents stale data while avoiding accidental path-cache churn for write-only updates.

## No-Stringly Rules (Current)

- Placeholder advancement for side-effect analysis uses AST traversal (`advance_placeholder_state_for_statement_ast`) instead of rendering statement SQL text and rebinding it.
- Intent routing/caching logic does not use SQL string matching to infer write semantics.

## Safety Contract

For authoritative file writes, engine verifies persisted `lix_binary_blob_ref` hash/size against intended bytes before finishing side-effect application. Verification checks/failures are counted by internal intent telemetry counters.

## Maintenance

Guardrail tests in `tests/sql_guardrails.rs` enforce:

- no reintroduction of legacy string-matched postprocess fallback
- no legacy pipeline module wiring
- side-effect placeholder advancement remains AST-based (no `statement.to_string()` rebinding path)
