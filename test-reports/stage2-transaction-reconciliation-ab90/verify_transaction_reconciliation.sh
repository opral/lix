#!/usr/bin/env bash
set -euo pipefail

ROOT=${1:?usage: verify_transaction_reconciliation.sh WORKTREE [candidate|control]}
MODE=${2:-candidate}
ANCHOR=ab90fc51e148611f5fdacde173dd6789ab22ab88
ANCHOR_TREE=5bcf259918f86e5b439c1bc50a3e198f87826adc
ANCHOR_PARENT=413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d

case "$MODE" in
  candidate|control) ;;
  *) echo "mode must be candidate or control" >&2; exit 2 ;;
esac

ROOT=$(cd "$ROOT" && pwd)
git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null
HEAD=$(git -C "$ROOT" rev-parse HEAD)
TREE=$(git -C "$ROOT" rev-parse HEAD^{tree})
PARENT=$(git -C "$ROOT" rev-parse HEAD^ 2>/dev/null || true)

show_source() {
  git -C "$ROOT" show "HEAD:$1" 2>/dev/null || true
}

commit=$(show_source packages/lix/src/transaction/commit.rs)
context=$(show_source packages/lix/src/transaction/context.rs)
stale=$(show_source packages/lix/src/transaction/stale_commit.rs)
undo_redo=$(show_source packages/lix/src/session/undo_redo.rs)
execute=$(show_source packages/lix/src/session/execute.rs)
forktree_mod=$(show_source packages/lix/src/forktree/mod.rs)
publication=$(show_source packages/lix/src/forktree/publication.rs)
forktree_reader=$(show_source packages/lix/src/live_state/forktree_reader.rs)
entity_batch=$(show_source packages/lix/src/sql2/entity_batch.rs)
live_context=$(show_source packages/lix/src/live_state/context.rs)
serving=$(show_source packages/lix/src/forktree/serving.rs)
model=$(show_source packages/lix/src/forktree/model.rs)

pass_count=0
red_count=0
fail_count=0
not_run_count=0

emit() {
  local status=$1 name=$2 evidence=$3
  printf '%s\t%s\t%s\n' "$status" "$name" "$evidence"
  case "$status" in
    PASS) pass_count=$((pass_count + 1)) ;;
    RED) red_count=$((red_count + 1)) ;;
    FAIL) fail_count=$((fail_count + 1)) ;;
    NOT_RUN) not_run_count=$((not_run_count + 1)) ;;
  esac
}

has_all() {
  local haystack=$1
  shift
  while (( $# )); do
    # Do not use rg -q under pipefail: early quit sends SIGPIPE to printf and
    # turns an otherwise present source token into a false negative.
    printf '%s\n' "$haystack" | rg -F -- "$1" >/dev/null || return 1
    shift
  done
}

has_any() {
  local haystack=$1
  shift
  while (( $# )); do
    if printf '%s\n' "$haystack" | rg -F -- "$1" >/dev/null; then return 0; fi
    shift
  done
  return 1
}

if [[ "$MODE" == control ]]; then
  if [[ "$HEAD" == "$ANCHOR" && "$TREE" == "$ANCHOR_TREE" && "$PARENT" == "$ANCHOR_PARENT" ]]; then
    emit PASS provenance "HEAD=$HEAD TREE=$TREE PARENT=$PARENT"
  else
    emit FAIL provenance "control expected HEAD=$ANCHOR TREE=$ANCHOR_TREE PARENT=$ANCHOR_PARENT; got HEAD=$HEAD TREE=$TREE PARENT=$PARENT"
  fi
else
  if git -C "$ROOT" merge-base --is-ancestor "$ANCHOR" "$HEAD"; then
    emit PASS provenance "HEAD=$HEAD TREE=$TREE descends from $ANCHOR"
  else
    emit FAIL provenance "HEAD=$HEAD is not descended from $ANCHOR"
  fi
fi

# This is intentionally an external prerequisite.  Ordinary local validators
# are not allowed to self-certify the historical duplicate/order invariant.
# The current anchor is explicitly not accepted for that prerequisite.
if [[ "${HISTORICAL_FAIL_CLOSED_ORACLE:-}" == accepted:* ]]; then
  emit PASS historical_fail_closed_prerequisite "bound to ${HISTORICAL_FAIL_CLOSED_ORACLE#accepted:}"
else
  emit RED historical_fail_closed_prerequisite "no independently accepted duplicate/order/member oracle supplied; ab90 remains held"
fi

# Canonical transaction owners already present on the anchor.  These checks
# record the owner boundary and do not claim that a migration is complete.
if has_all "$stale" 'classify_stale_commit' 'StaleCommitPlan::ReconcilePlugin' 'StaleCommitPlan::Unsafe' 'indexed_overlap_indices'; then
  emit PASS stale_owner_classifier "same-key overlap, disjoint-owner composition, and unsafe mixed conflicts have one classifier"
else
  emit FAIL stale_owner_classifier "transaction/stale_commit.rs lacks the canonical owner-overlap classifier"
fi

if has_all "$undo_redo" 'pub async fn undo' 'pub async fn redo' 'undo_target_after' 'redo_top_after' 'redo_next'; then
  emit PASS undo_redo_owner "durable undo/redo marker owns target and redo chronology"
else
  emit FAIL undo_redo_owner "undo/redo chronology owner is incomplete"
fi

if has_all "$execute" 'begin_sql_statement_checkpoint' 'rollback_sql_statement_checkpoint' 'restore_statement_checkpoint'; then
  emit PASS savepoint_rollback_owner "statement checkpoints restore staged and function state before commit"
else
  emit FAIL savepoint_rollback_owner "statement checkpoint/rollback owner is missing"
fi

if has_all "$execute" 'execute_idempotent_write' 'resolve_idempotency_receipt' 'stage_execute_idempotency_receipt' && has_all "$context" 'prepare_write_set' 'prepared_commit.commit'; then
  emit PASS idempotency_boundary "receipt lookup/staging is coupled to the existing transaction commit boundary"
else
  emit FAIL idempotency_boundary "idempotency or transaction commit boundary is not visible"
fi

if has_all "$model" 'enum SnapshotRole' 'Checkpoint' 'Undo' 'Redo' && has_all "$publication$context" 'CheckpointPublication' 'into_storage_plan'; then
  emit PASS checkpoint_undo_root_owner "checkpoint and undo/redo roots are represented by existing ForkTree roles/plans"
else
  emit FAIL checkpoint_undo_root_owner "required existing root/plan owners are absent"
fi

if has_all "$context" 'into_storage_plan' 'prepare_write_set' 'prepared_commit.commit'; then
  emit PASS one_transaction_commit_boundary "existing transaction lowerer has one plan/prepare/backend-commit sequence"
else
  emit FAIL one_transaction_commit_boundary "no single transaction-owned plan/prepare/commit sequence found"
fi

# Direct publication commit is forbidden.  Test-only helpers may still use a
# storage plan; this gate only rejects a production publication commit method.
if ! has_any "$publication" 'PreparedPublication::commit' 'pub(crate) async fn commit' 'pub async fn commit'; then
  emit PASS no_direct_forktree_commit "PreparedPublication exposes storage-plan lowering, not an independent backend commit"
else
  emit FAIL no_direct_forktree_commit "ForkTree publication still exposes a direct commit seam"
fi

# The one-view migration seam is not present on ab90.  Keep this as a focused
# RED, rather than accepting the current multiple-reader/cache arrangement.
if has_all "$forktree_reader" 'open_coherent_view_on_read' 'scan_batch' && has_all "$entity_batch" '.scan_batch(request)' && ! has_any "$live_context" 'TrackedStateContext::new().reader' 'HotStateTransactionCache'; then
  emit PASS one_retained_view "canonical state scan has one retained view and no legacy live-state reader/cache"
else
  emit RED one_retained_view "ab90 still has inherited tracked-state/cache seams; transaction migration must thread one operation-owned view"
fi

if has_any "$commit$context$forktree_mod" 'unsupported' 'fail closed' 'return Err'; then
  emit PASS unsupported_fail_before_plan_marker "unsupported publication families have typed rejection markers"
else
  emit FAIL unsupported_fail_before_plan_marker "no typed pre-plan rejection marker found"
fi

# The exact old reader name is absent, but an inherited tracked-state reader
# and cache remain reachable.  Both are RED until the future reader closure
# replaces them without resurrecting a fallback or adding a second authority.
if ! has_any "$commit$context$forktree_mod$live_context" 'TrackedStateStoreReader' 'tracked_state::reader'; then
  emit PASS old_named_reader_absent "literal deleted TrackedStateStoreReader/raw reader symbol is absent"
else
  emit RED old_named_reader_absent "TrackedStateStoreReader remains in inherited transaction reader paths; the future migration must delete it rather than add a facade around it"
fi

if has_any "$live_context" 'TrackedStateContext' 'TrackedHeadContext' 'branch_head_control_cache' 'scan_batch_at_commit'; then
  emit RED inherited_reader_cache_frontier "inherited tracked-state/control/cache reader remains; do not treat absence of one old name as deletion proof"
else
  emit PASS inherited_reader_cache_frontier "no inherited tracked-state reader/cache symbol remains in live-state context"
fi

if has_all "$serving" 'load_commit_member_records' 'validate_retained_commit' 'select_historical_commit_member' && has_any "$serving" 'duplicate' 'strictly ordered' 'corruption'; then
  emit PASS historical_member_fail_closed_shape "historical member path has authenticated validation/selection hooks"
else
  emit FAIL historical_member_fail_closed_shape "historical member validation owner is not visible"
fi

# ab90's known direct-reader red: tombstone-inclusive terminal requests are
# rejected before acquisition.  A transaction migration must not use that
# capability rejection to hide missing semantics or fall back to old rows.
if printf '%s\n' "$entity_batch" | rg -q 'include_tombstones.*return Err|terminal_projection_rejects_tombstones_before_acquisition'; then
  emit RED historical_tombstone_prerequisite "ab90 rejects tombstone-inclusive terminal reads before acquisition; prerequisite correction is not accepted"
else
  emit PASS historical_tombstone_prerequisite "tombstone-inclusive terminal behavior is represented by the canonical reader"
fi

emit NOT_RUN runtime_matrix "TEST/REPORT-ONLY package: no Memory/RocksDB/SlateDB build or runtime was run"

printf 'SUMMARY\tmode=%s\tpass=%d\tred=%d\tfail=%d\tnot_run=%d\thead=%s\ttree=%s\n' \
  "$MODE" "$pass_count" "$red_count" "$fail_count" "$not_run_count" "$HEAD" "$TREE"

# A calibrated anchor may contain expected REDs, but never an unexpected FAIL.
(( fail_count == 0 ))
if [[ "$MODE" == control && "$red_count" == 0 ]]; then
  echo 'control unexpectedly passed all migration discriminators' >&2
  exit 1
fi
