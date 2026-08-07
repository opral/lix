# ForkTree Stage-2 end-to-end SQL DML A/B — structural blocker

Verdict: **BLOCKER; no valid 1K A/B cell was admitted.**

The exact-main SQL semantics are green, and a transaction-scoped
`SqlWriteExecutionContext` can drive #1260's registered `SpecWriteTarget` for
one statement. The public transaction/batch owner cannot, however, be given a
test physical target. Constructing the requested comparator would therefore
require either changing the prohibited production seam or recreating
`execute_batch`/statement-checkpoint behavior in the benchmark. The latter
would be a second SQL/transaction authority and was rejected before timing.

## Provenance

- Base/head: `803d19ec0b67fb4b759aceab7ceb74650d9d894f`
- Base tree: `2ae6ffd8faef595ca9bf2e60447ef31a8922b92f`
- Subject: `Merge pull request #1260 from opral/codex/sql-write-owner-cut`
- Worktree: `/root/repos/lix-forktree-dml-e2e-803d`
- Prior model controls remained immutable. No prior model result was relabeled
  as an end-to-end result.

## Exact ownership boundary

1. `SqlWriteContext::new` creates a fresh private `WriteTargetRegistry` for
   one write execution (`sql2/context.rs:329-343`).
2. `register_spec_table` inserts only `Arc<SpecWriteTarget>` values
   (`sql2/providers/spec.rs:505-520,557-605`). This correctly keeps
   `RETURNING` and `ON CONFLICT` in Lix and keeps mutation authority out of
   DataFusion's public `TableProvider`.
3. `build_write_session_with_options` takes a
   `&mut dyn SqlWriteExecutionContext`, registers the Lix providers, and
   returns the registry (`sql2/session.rs:138-196`). This is sufficient for a
   statement-level model target whose reads/staged rows are supplied by that
   context.
4. `SessionContext::execute_batch` owns parsing/classification, labels,
   statement indexes, one transaction, and final result annotation
   (`session/execute.rs:1535-1792`). It does not accept a write context,
   registry, target factory, or physical target.
5. `SessionTransaction` contains a concrete `Transaction<StorageImpl>` and is
   constructed by `open_transaction_with_runtime_boundary`
   (`session/transaction.rs:26-109`). It has no target parameter.
6. Failed-statement savepoints call methods on that concrete transaction
   (`session/execute.rs:2917-2943`). `SqlWriteExecutionContext` has no
   checkpoint/rollback capability. A model loop that snapshots rows or roots
   would therefore duplicate Lix's savepoint authority.

Consequently, these requested semantics cannot all be exercised against a
ForkTree target through the current test seam:

- 18-statement `execute_batch` atomicity, labels, and statement indexes;
- failed-statement savepoint rollback while retaining the transaction;
- explicit commit/rollback and stale-writer publication conflicts;
- one ForkTree selector publication through the same commit boundary.

A statement-only bridge can truthfully cover binder, defaults/generated
values, FK checks, `RETURNING`, and `ON CONFLICT`, but calling it an end-to-end
batch/transaction comparator would be false.

## Exact-main gates

All commands ran from the clean exact-main worktree. Setup/build time is not a
benchmark result.

```text
cargo test -p lix --lib \
  sql2::exec::datafusion::tests::target_only_delete_returning_executes_with_selected_provider \
  -- --exact --nocapture
PASS: 1 passed, 0 failed

cargo test -p lix --lib \
  session::execute::tests::execute_batch_metadata_preserves_returning_rows_and_duplicate_labels \
  -- --exact --nocapture
PASS: 1 passed, 0 failed

cargo test -p lix --lib \
  session::execute::tests::execute_batch_parameter_batch_preserves_failing_statement_index \
  -- --exact --nocapture
PASS: 1 passed, 0 failed

cargo test -p lix_tests --test e2e \
  lix_owned_sql_write_semantics_rocksdb_reopen -- --exact --nocapture
PASS: 1 passed, 0 failed

cargo test -p lix_tests --test e2e \
  lix_owned_sql_write_semantics_slatedb_reopen -- --exact --nocapture
PASS: 1 passed, 0 failed
```

The first gate proves the #1260 physical-target registry and Lix-owned
`RETURNING` path. The other four prove that current main's public batch,
rollback, adapter, and reopen semantics are healthy. The blocker is therefore
not an inherited correctness failure.

## Measurement disposition

No wall, CPU, allocation, RSS, backend-work, or disk comparison is reported.
The admission rule required exact result/final digests and exact transaction
semantics before performance. Those semantics cannot be wired to the model
through the current authorized seam. Measuring the statement-only bridge
against public `execute_batch` would compare different owners and boundaries.

Current SQL semantic complexity remains `O(R + E)`, where `R` is affected or
examined rows and `E` is expression/constraint work. The intended ForkTree
physical point-write cost remains `O(P log_B N + E)` for `P` distinct point
identities, plus one atomic root publication; residual scans remain
`O(N + E)`. This lane produced no evidence that changes either bound.

The perfect-elimination ceiling is intentionally **unquantified on 803d**:
there is no valid end-to-end model cell from which to separate SQL,
transaction, and physical-layout coefficients. Prior statement-level model
gains are not transferable evidence for this comparator.

## Minimal implementer contract for Ryzen-V

Stage 2 should provide one transaction-owned physical target at the existing
#1260 boundary; it must not add another binder, executor, or batch loop.

1. Keep `SessionContext::execute_batch`, `LogicalWritePlan`, Lix
   `RETURNING`/`ON CONFLICT`, FK/default/generated checks, result annotation,
   and statement checkpoints unchanged as the sole semantic owners.
2. Let the concrete production `Transaction` implement the ForkTree-backed
   `SqlWriteExecutionContext`: one coherent authenticated read view supplies
   scans/exact reads, and `stage_write` owns transaction-local postimages.
3. Register the existing entity `TableSpec`/`SpecWriteTarget` through the
   existing private registry. Do not pass SQL or DataFusion `Expr` into the
   storage target and do not expose a caller-selectable `BlobId`/root.
4. Make the transaction checkpoint include the ForkTree staged mutation/root
   candidate so Lix's existing rollback method restores it. Do not create a
   model-specific savepoint authority.
5. Commit all authenticated objects and exactly one CAS-protected branch/root
   selector in the existing transaction commit. No precommit, side writer,
   cache authority, or compatibility layout.
6. Add a cfg-only session-construction seam that swaps only the physical
   target while preserving the concrete Lix transaction orchestration. The
   acceptance harness must call the public `execute_batch` and explicit
   transaction APIs unchanged.
7. Re-run the requested 1K RocksDB/SlateDB matrix only after exact result,
   final-state, rollback, label/index, reopen, and stale-writer digests match.
   Admit 10K only with every critical regression at most 5% and at least one
   meaningful improvement over 10%.

## Source identity

```text
faea7048423043078b2667f40d83c94bba958e29eae33b699c6e961fede341aa  packages/lix/src/sql2/providers/spec.rs
61b67fcfcfd1ff0cb51714613f2efde5b6eb03169f60ecf4544bcb94dac2fa91  packages/lix/src/sql2/session.rs
14e7383d6b789b35cce68c82522c734d44f7b5cbc2f209878077172237c25ccf  packages/lix/src/sql2/context.rs
d3016b92421ad64b0faa490ce07004f328753c56bbe4e10a1736ccea5dd3027c  packages/lix/src/session/execute.rs
af75ab3d1744e47751f8876cf0b2a05c475df70bb33911be9171faf764d04b01  packages/lix/src/session/transaction.rs
e18572810c84d36df28a5b9f4f92c2733d1f6176be4cb964ee25ecf642deab60  packages/rs-sdk-tests/tests/e2e.rs
```
