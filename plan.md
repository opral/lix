# SQL Execution Unification Plan (Divergence Prevention)

## Goal
Keep one source of truth for SQL execution orchestration so `api.rs` and `in_transaction.rs` do not drift.

## Steps
1. [x] Add a shared execution-path helper under `packages/engine/src/sql/execution/`.
2. [x] Move duplicated pre-execution orchestration into that helper:
   - requirement derivation
   - read materialization/refresh
   - side-effect collection policy
   - runtime function preparation
   - execution plan build
   - validation
3. [x] Add a transaction execution entry in `execution/run.rs` so both call paths use the same execution module.
4. [x] Rewire `api.rs` to use shared preparation and shared cache-target derivation.
5. [x] Rewire `in_transaction.rs` to use shared preparation and shared execution module.
6. [x] Keep `api.rs` and `in_transaction.rs` as thin wrappers around mode-specific policy.
7. [x] Run formatting and targeted tests for SQL runtime.
8. [x] Iterate on compile/test errors until green.

## Iteration Loop
- Iteration 1: implement steps 1-6. (done)
- Iteration 2: fix compile/test issues and tighten code paths. (done)
