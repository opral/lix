#!/usr/bin/env bash
set -euo pipefail

ROOT=${1:?usage: verify_entity_semantics.sh WORKTREE [candidate|control]}
MODE=${2:-candidate}
ANCHOR=413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d
ANCHOR_TREE=820fe560da3bbd2b00b788b0b1759c409048cd6e

case "$MODE" in
  candidate|control) ;;
  *) echo "mode must be candidate or control" >&2; exit 2 ;;
esac

ROOT=$(cd "$ROOT" && pwd)
git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null
HEAD=$(git -C "$ROOT" rev-parse HEAD)
TREE=$(git -C "$ROOT" rev-parse HEAD^{tree})

show_source() {
  git -C "$ROOT" show "HEAD:$1" 2>/dev/null || true
}

view=$(show_source packages/lix/src/forktree/view.rs)
context=$(show_source packages/lix/src/live_state/context.rs)
fork_reader=$(show_source packages/lix/src/live_state/forktree_reader.rs)
entity_batch=$(show_source packages/lix/src/sql2/entity_batch.rs)
provider=$(show_source packages/lix/src/sql2/providers/entity.rs)
types=$(show_source packages/lix/src/live_state/types.rs)
serving=$(show_source packages/lix/src/forktree/serving.rs)

pass_count=0
red_count=0
fail_count=0

emit() {
  local status=$1 name=$2 evidence=$3
  printf '%s\t%s\t%s\n' "$status" "$name" "$evidence"
  case "$status" in
    PASS) pass_count=$((pass_count + 1)) ;;
    RED) red_count=$((red_count + 1)) ;;
    FAIL) fail_count=$((fail_count + 1)) ;;
  esac
}

contains() { printf '%s\n' "$1" | rg -q -- "$2"; }
not_contains() { ! contains "$1" "$2"; }

has_all_literal() {
  while (( $# >= 2 )); do
    local haystack=$1 needle=$2
    shift 2
    printf '%s\n' "$haystack" | rg -Fq -- "$needle" || return 1
  done
}

has_all_in() {
  local haystack=$1
  shift
  while (( $# )); do
    printf '%s\n' "$haystack" | rg -Fq -- "$1" || return 1
    shift
  done
}

has_one_scan_each() {
  local snapshot primary
  snapshot=$(printf '%s\n' "$entity_batch" | sed -n '/canonical_snapshot_projection/,/canonical_primary_key_projection/p')
  primary=$(printf '%s\n' "$entity_batch" | sed -n '/canonical_primary_key_projection/,/\#\[cfg(test)\]/p')
  printf '%s\n' "$snapshot" | rg -q 'scan_batch\(request\)' \
    && printf '%s\n' "$primary" | rg -q 'scan_batch\(request\)'
}

filter_before_limit() {
  printf '%s\n' "$fork_reader" | awk '
    /entity_pks/ && !entity { entity=NR }
    /output\.push\(/ && !push { push=NR }
    /output\.len\(\).*limit/ && !limit { limit=NR }
    END { exit !(entity && push && limit && entity < push && push < limit) }
  '
}

one_view_read() {
  local facade
  facade=$(printf '%s\n' "$view" | sed -n '/pub(crate) struct ForkTreeReadFacade/,/pub(crate) async fn load_commit_member_records/p')
  [[ "$fork_reader" == *open_coherent_view_on_read* ]] \
    && [[ "$context" == *ForkTreeReadFacade::new* ]] \
    && [[ "$context" == *scan_forktree_view* ]] \
    && [[ "$facade" == *open_coherent_view_on_read* ]] \
    && ! printf '%s\n' "$facade" | rg -q 'begin_read\s*\('
}

fail_closed_decode() {
  has_all_in "$fork_reader" \
    'state_range(&view, None, None, None, true).await?' \
    'state_point(&view, &key, request.include_tombstones).await?' \
    'decode_state_key(&row.encoded_key)?' \
    && ! printf '%s\n' "$fork_reader" | rg -q '\.(unwrap|expect)\('
}

branch_global_overlay() {
  has_all_in "$serving" \
    'global_state_root' \
    'local_state_root' \
    'local_key <= global_key' \
    'StateSource::Branch' \
    'StateSource::Global'
}

if [[ "$MODE" == control ]]; then
  if [[ "$HEAD" == "$ANCHOR" && "$TREE" == "$ANCHOR_TREE" ]]; then
    emit PASS anchor "HEAD=$HEAD TREE=$TREE"
  else
    emit FAIL anchor "expected HEAD=$ANCHOR TREE=$ANCHOR_TREE, got HEAD=$HEAD TREE=$TREE"
  fi
else
  if git -C "$ROOT" merge-base --is-ancestor "$ANCHOR" "$HEAD"; then
    emit PASS anchor "HEAD=$HEAD TREE=$TREE (anchor=$ANCHOR)"
  else
    emit FAIL anchor "HEAD=$HEAD is not descended from $ANCHOR"
  fi
fi

if not_contains "$view$context$entity_batch" 'scan_entity_rows'; then
  emit PASS scan_entity_rows_deleted 'old ForkTreeReadFacade::scan_entity_rows is absent'
else
  emit FAIL scan_entity_rows_deleted 'old ForkTreeReadFacade::scan_entity_rows remains reachable'
fi

if has_one_scan_each; then
  emit PASS one_canonical_scan 'snapshot and primary-key terminal projections each call LiveStateReader::scan_batch once'
else
  emit FAIL one_canonical_scan 'one or both terminal projections do not have the canonical scan_batch call'
fi

if one_view_read; then
  emit PASS one_view 'tracked/untracked traversal borrows one operation-owned coherent view/read'
else
  emit FAIL one_view 'reader path can acquire or refresh a second view/read'
fi

if branch_global_overlay; then
  emit PASS branch_global_overlay 'authenticated global/local roots merge with branch precedence and ordered keys'
else
  emit FAIL branch_global_overlay 'branch/global replacement authority or ordering proof is absent'
fi

if filter_before_limit; then
  emit PASS filter_before_limit 'entity/filter eligibility is evaluated before output limit'
else
  emit FAIL filter_before_limit 'limit can be applied before entity/filter eligibility'
fi

if has_all_in "$fork_reader" \
    'row.value.cell.deleted() && !request.filter.include_tombstones' \
    'state_point(&view, &key, request.include_tombstones)'; then
  emit PASS tombstone_filter 'canonical reader filters tombstones unless explicitly requested'
else
  emit FAIL tombstone_filter 'canonical reader lacks explicit tombstone filtering/point semantics'
fi

if has_all_in "$fork_reader" \
    'StateCell::Null | StateCell::Tombstone => None' \
    'deleted = row.value.cell.deleted'; then
  emit PASS null_tombstone_source 'canonical materialization distinguishes deleted from NULL before terminal projection'
else
  emit FAIL null_tombstone_source 'canonical materialization does not expose NULL/tombstone distinction'
fi

# This is the candidate-owned semantic discriminator. The old implementation
# returned None for tombstone/retention-scoped requests, which hid missing
# ForkTree semantics behind the generic row path. The accepted correction must
# serve those requests through the canonical reader and preserve the marker;
# merely restoring that rejection is not an acceptable green result.
snapshot_impl=$(printf '%s\n' "$entity_batch" \
  | sed -n '/async fn scan_entity_snapshots/,/async fn scan_entity_primary_keys/p')
snapshot_projection=$(printf '%s\n' "$entity_batch" \
  | sed -n '/async fn canonical_snapshot_projection/,/async fn canonical_primary_key_projection/p')
if printf '%s\n' "$snapshot_impl" | rg -q 'Result<Option<Vec<Option<Bytes>>>, LixError>' \
    && printf '%s\n' "$snapshot_projection" | rg -q 'into_identity_ordered_snapshots' \
    && ! printf '%s\n' "$snapshot_projection" | rg -q 'deleted|tombstone'; then
  emit RED projected_deleted_marker 'terminal snapshot type is Vec<Option<Bytes>> with no deletion marker; NULL and tombstone collapse'
else
  emit PASS projected_deleted_marker 'terminal snapshot projection carries an explicit deletion/null marker'
fi

if printf '%s\n' "$provider" | rg -q 'request\.filter\.include_tombstones' \
    || printf '%s\n' "$entity_batch" | rg -q 'include_tombstones'; then
  emit PASS tombstone_eligibility 'direct capability explicitly accounts for tombstone requests'
else
  emit RED tombstone_eligibility 'direct capability accepts tombstone requests without an explicit projection contract'
fi

if has_all_in "$serving" \
    'strictly ordered and distinct' \
    'duplicate' \
    'corruption('; then
  emit PASS duplicate_fail_closed 'authenticated catalog/tree validators reject duplicate or noncanonical authority'
else
  emit FAIL duplicate_fail_closed 'no source proof rejects duplicate or malformed authenticated authority'
fi

# The old implementation rejected explicit retention modes. A corrected
# implementation must compose the complete tracked+untracked view, not choose
# one plane or fall back to the old row reader.
if printf '%s\n' "$fork_reader" | rg -q 'untracked == Some\(true\).*scan_untracked_view|return scan_untracked_view'; then
  emit RED complete_retention_overlay 'canonical scan selects a separate untracked plane instead of one complete tracked+untracked view'
else
  emit PASS complete_retention_overlay 'canonical scan composes tracked and untracked retention on one view'
fi

if printf '%s\n' "$entity_batch" | rg -q 'scan_direct_entity|plan_direct_entity'; then
  emit FAIL no_raw_direct_path 'direct/raw entity reader symbols remain'
else
  emit PASS no_raw_direct_path 'old direct entity reader symbols are absent'
fi

if [[ "$provider" == *'if let Some(direct_entity_snapshot)'* \
    && "$provider" == *'.scan_batch(&request)'* ]]; then
  emit RED no_old_row_fallback 'provider retains an old materialized-row capability fallback; corrected semantics must not hide behind it'
else
  emit PASS no_old_row_fallback 'no old row-path fallback remains in entity execution'
fi

if fail_closed_decode; then
  emit PASS malformed_fail_closed 'range/point/key decode errors propagate before projection'
else
  emit FAIL malformed_fail_closed 'decode/authentication path can swallow malformed or duplicate state'
fi

printf 'SUMMARY\tmode=%s\tpass=%d\tred=%d\tfail=%d\thead=%s\ttree=%s\n' \
  "$MODE" "$pass_count" "$red_count" "$fail_count" "$HEAD" "$TREE"

# The 413 calibration is deliberately RED. A future candidate is green only
# when every semantic discriminator passes and no structural check fails.
if (( fail_count != 0 )); then
  exit 1
fi
if [[ "$MODE" == control && "$red_count" == 0 ]]; then
  echo 'control unexpectedly passed the semantic discriminator' >&2
  exit 1
fi
if [[ "$MODE" == candidate && "$HEAD" == "$ANCHOR" && "$red_count" == 0 ]]; then
  echo '413 unexpectedly passed the semantic discriminator' >&2
  exit 1
fi
