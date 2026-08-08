#!/usr/bin/env bash
set -euo pipefail

ROOT=${1:?usage: verify_direct_reader.sh WORKTREE [control|candidate]}
MODE=${2:-candidate}
ANCHOR=e1666edd0b4d814a88d985086ecc5a477b5d32e6
ANCHOR_TREE=c680bd7e7f7b70cd784676515839af2dcbbc7917

case "$MODE" in
  control|candidate) ;;
  *) echo "mode must be control or candidate" >&2; exit 2 ;;
esac

ROOT=$(cd "$ROOT" && pwd)
git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null
HEAD=$(git -C "$ROOT" rev-parse HEAD)
TREE=$(git -C "$ROOT" rev-parse HEAD^{tree})

show_source() {
  git -C "$ROOT" show "HEAD:$1" 2>/dev/null || true
}

context_all=$(show_source packages/lix/src/live_state/context.rs)
context_prod=$(printf '%s\n' "$context_all" | sed '/^mod tests[[:space:]]*{/,$d')
fork_reader=$(show_source packages/lix/src/live_state/forktree_reader.rs)
entity_batch=$(show_source packages/lix/src/sql2/entity_batch.rs)
reader_trait=$(show_source packages/lix/src/live_state/reader.rs)
live_mod=$(show_source packages/lix/src/live_state/mod.rs)
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

contains() {
  printf '%s\n' "$1" | rg -q -- "$2"
}

not_contains() {
  ! contains "$1" "$2"
}

positive() {
  local name=$1 evidence=$2
  shift 2
  if "$@"; then emit PASS "$name" "$evidence"; else emit FAIL "$name" "$evidence"; fi
}

forbidden() {
  local name=$1 evidence=$2
  shift 2
  if "$@"; then
    emit RED "$name" "$evidence"
  else
    emit PASS "$name" "$evidence"
  fi
}

expect_head() {
  [[ "$HEAD" == "$ANCHOR" ]]
}

source_has() {
  local haystack=$1 pattern=$2
  contains "$haystack" "$pattern"
}

source_lacks() {
  local haystack=$1 pattern=$2
  not_contains "$haystack" "$pattern"
}

source_has_literal() {
  local haystack=$1 needle=$2
  printf '%s\n' "$haystack" | rg -Fq -- "$needle"
}

source_lacks_literal() {
  local haystack=$1 needle=$2
  ! source_has_literal "$haystack" "$needle"
}

all_literal() {
  while (( $# >= 2 )); do
    local haystack=$1 needle=$2
    shift 2
    source_has_literal "$haystack" "$needle" || return 1
  done
}

filter_before_limit() {
  printf '%s\n' "$fork_reader" | awk '
    /entity_pks/ && !entity { entity=NR }
    /output\.push\(/ && !push { push=NR }
    /output\.len\(\).*limit/ && !limit { limit=NR }
    END { exit !(entity && push && limit && entity < push && push < limit) }
  '
}

decode_auth_gate() {
  all_literal "$fork_reader" \
    'state_range(&view, None, None, None, true).await?' \
    "$fork_reader" 'state_point(&view, &key, request.include_tombstones).await?' \
    "$fork_reader" 'decode_state_key(&row.encoded_key)?' \
    && source_lacks "$fork_reader" '\.(unwrap|expect)\('
}

no_write_side_effect() {
  ! printf '%s\n' "$fork_reader$context_prod$entity_batch" \
    | rg -q -- '(prepare_write_set|StorageWrite|\.commit\(|\.write\()'
}

if [[ "$MODE" == control ]]; then
  positive anchor "HEAD=$HEAD TREE=$TREE" expect_head
else
  emit PASS anchor "HEAD=$HEAD TREE=$TREE (candidate mode; anchor=$ANCHOR)"
fi

positive coherent_view \
  'forktree_reader uses caller-provided read and open_coherent_view_on_read; context routes through ForkTreeReadFacade' \
  all_literal "$fork_reader" 'open_coherent_view_on_read' \
    "$context_prod" 'ForkTreeReadFacade::new' \
    "$context_prod" 'scan_forktree_view'

forbidden no_raw_read_getter \
  'direct reader/context/entity adapter has no public raw-read, storage_read, or begin_read helper' \
  source_has "$fork_reader$context_prod$entity_batch" \
    '(pub\(crate\)|pub\(super\)|pub) fn (read|storage_read)\(&self\)|begin_read\s*\('

positive overlay_order \
  'serving merges global_state_root/local_state_root with branch precedence and ordered local_key comparison' \
  all_literal "$serving" 'global_state_root' \
    "$serving" 'local_state_root' \
    "$serving" 'local_key <= global_key' \
    "$serving" 'StateSource::Branch' \
    "$serving" 'StateSource::Global'

positive entity_filter_before_limit \
  'scan_view filters entity_pks before output.push and output limit' \
  filter_before_limit

positive null_preserved \
  'StateCell::Null and Tombstone map to None while deleted() distinguishes SQL NULL from delete' \
  all_literal "$fork_reader" 'StateCell::Null | StateCell::Tombstone => None' \
    "$fork_reader" 'deleted = row.value.cell.deleted()'

positive tombstone_policy \
  'tombstones are filtered unless include_tombstones is requested, including exact point reads' \
  all_literal "$fork_reader" 'row.value.cell.deleted() && !request.filter.include_tombstones' \
    "$fork_reader" 'state_point(&view, &key, request.include_tombstones)'

positive decode_auth_fail_closed \
  'view/range/point/key decode errors use ? and direct reader has no unwrap/expect' \
  decode_auth_gate

positive untracked_same_view \
  'explicit untracked requests use scan_untracked_view/scan_untracked_rows on the supplied coherent view' \
  all_literal "$fork_reader" 'scan_untracked_view' \
    "$fork_reader" 'view.scan_untracked_rows()'

positive unsupported_fail_before_view \
  'derived/history are rejected by validate_scan_request and ambiguous branch/row lanes reject before output' \
  all_literal "$fork_reader" 'request_may_include_derived(request)' \
    "$fork_reader" 'current ForkTree reader does not serve derived or history schemas' \
    "$fork_reader" 'requires one branch'

forbidden no_tracked_head_owner \
  'context/SQL reader still names tracked-head or TrackedState owners' \
  source_has "$context_prod$entity_batch" \
    '(TrackedHeadContext|TrackedStateContext|tracked_head|TrackedStateStoreReader)'

forbidden no_columnar_owner \
  'context/SQL live-state reader still owns or reaches durable EntityColumnar/EntityDecoded paths' \
  source_has "$context_prod$entity_batch$live_mod" \
    '(EntityColumnar|EntityDecodedColumn|entity_columnar|columnar)'

forbidden no_current_state_cache \
  'context/SQL reader still owns or invokes entity snapshot/columnar caches' \
  source_has "$context_prod$entity_batch" \
    '(entity_point_snapshot_cache|entity_columnar_.*cache|entity_decoded_column_cache|Cache<)'

forbidden no_fallback \
  'reader trait/direct SQL sources still expose a fallback or direct fallback owner' \
  source_has "$reader_trait$fork_reader$context_prod$entity_batch" \
    '(fallback|falling back|scan_direct_entity|direct_entity_)'

forbidden no_raw_state_shortcut \
  'SQL entity adapter still calls scan_direct_* or plan_direct_*' \
  source_has "$entity_batch" '(scan_direct_entity|plan_direct_entity)'

positive no_write_side_effect \
  'reader-only sources contain no write-set, StorageWrite, commit, or write call' \
  no_write_side_effect

printf 'SUMMARY\tmode=%s\tpass=%d\tred=%d\tfail=%d\thead=%s\ttree=%s\n' \
  "$MODE" "$pass_count" "$red_count" "$fail_count" "$HEAD" "$TREE"

if (( fail_count != 0 )); then
  exit 1
fi
if [[ "$MODE" == control && "$red_count" < 5 ]]; then
  echo 'control expected all five e166 RED discriminators' >&2
  exit 1
fi
if [[ "$MODE" == candidate && "$red_count" != 0 ]]; then
  echo 'candidate still contains forbidden current-state authority/fallback' >&2
  exit 1
fi
