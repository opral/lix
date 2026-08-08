#!/usr/bin/env bash
set -Eeuo pipefail

readonly BASE_SHA="e1af471b9ab0f598dafa7c2ddec7867667c81740"
readonly BASE_TREE="bfa0d271a723da8250ab76ada16fda90926f1099"
readonly BASE_PARENT="b484e20d845aee3f8137bfa3496f9b3cd0e8cd35"
readonly BASELINE_REJECTION="file payload publication requires the ForkTree receipt/manifest lowering slice"
readonly ALLOWED_PATHS=(
  packages/lix/src/transaction/commit.rs
  packages/lix/src/transaction/context.rs
  packages/lix/src/transaction/staging.rs
  packages/lix/src/transaction/types.rs
  packages/lix/src/forktree/publication.rs
  packages/lix/src/forktree/blob.rs
  packages/lix/src/sql2/providers/file.rs
)

usage() {
  cat >&2 <<'EOF'
usage:
  verify_w4a_source.sh BASE_ROOT BASE_COMMIT
  verify_w4a_source.sh BASE_ROOT BASE_COMMIT CANDIDATE_ROOT CANDIDATE_COMMIT
  verify_w4a_source.sh --self-test
EOF
  exit 2
}

die() {
  echo "BLOCKER: $*" >&2
  exit 1
}

rev_at_head() {
  git -C "$1" rev-parse HEAD
}

require_commit() {
  git -C "$1" rev-parse --verify "${2}^{commit}" >/dev/null
}

count_literal() {
  local text=$1
  local needle=$2
  (printf '%s\n' "$text" | rg -o --fixed-strings "$needle" || true) | wc -l | tr -d ' '
}

operation_window() {
  awk '
    /async[[:space:]]+fn[[:space:]]+commit_prepared[[:space:]]*\(/ { started = 1 }
    started { print }
    started && /prepared_commit[.]commit[(][)]([.]await)?/ { exit }
  ' "$1"
}

source_baseline() {
  local root=$1
  local commit=$2
  local actual tree parent
  actual=$(rev_at_head "$root")
  [[ "$actual" == "$commit" ]] ||
    die "base root HEAD=$actual does not equal supplied base commit=$commit"
  [[ "$commit" == "$BASE_SHA" ]] ||
    die "baseline must be frozen e1af=$BASE_SHA, got $commit"
  tree=$(git -C "$root" rev-parse "${commit}^{tree}")
  [[ "$tree" == "$BASE_TREE" ]] ||
    die "baseline tree mismatch expected=$BASE_TREE actual=$tree"
  parent=$(git -C "$root" rev-parse "${commit}^")
  [[ "$parent" == "$BASE_PARENT" ]] ||
    die "baseline parent mismatch expected=$BASE_PARENT actual=$parent"

  local commit_rs="$root/packages/lix/src/transaction/commit.rs"
  local context_rs="$root/packages/lix/src/transaction/context.rs"
  local window
  window=$(operation_window "$context_rs")
  [[ -f "$commit_rs" && -f "$context_rs" ]] ||
    die "baseline transaction source is incomplete"

  if rg -Fq "$BASELINE_REJECTION" "$commit_rs"; then
    echo "BASE-RED-01 file_content_writes is still rejected before ForkTree lowering"
  else
    die "BASE-BLOCKER rejection calibration disappeared"
  fi
  [[ "$(count_literal "$window" 'begin_read(')" == 1 ]] ||
    die "BASE-BLOCKER commit_prepared read count is not one"
  echo "BASE-PASS-02 one commit-prepared coherent-read acquisition"
  [[ "$(count_literal "$window" 'prepare_write_set(')" == 1 ]] ||
    die "BASE-BLOCKER commit_prepared prepare count is not one"
  [[ "$(count_literal "$window" 'prepared_commit.commit().await')" == 1 ]] ||
    die "BASE-BLOCKER commit_prepared commit count is not one"
  echo "BASE-PASS-03 one prepare and one backend commit"
  if rg -n --fixed-strings 'PreparedPublication::commit' "$root/packages/lix/src" >/tmp/w4a-prepared-commit.$$ 2>/dev/null; then
    cat /tmp/w4a-prepared-commit.$$
    rm -f /tmp/w4a-prepared-commit.$$
    die "BASE-BLOCKER direct PreparedPublication commit residue"
  fi
  rm -f /tmp/w4a-prepared-commit.$$
  echo "BASE-PASS-04 no direct PreparedPublication commit symbol"
  if rg -n --fixed-strings 'binary_cas::kv' "$root/packages/lix/src/binary_cas/context.rs"; then
    echo "BASE-RED-06 stale Binary CAS KV owner references remain"
  else
    die "BASE-BLOCKER stale Binary CAS calibration disappeared"
  fi
  echo "BASE-SOURCE-RESULT=RED"
  return 1
}

is_allowed_path() {
  local path=$1 allowed
  for allowed in "${ALLOWED_PATHS[@]}"; do
    [[ "$path" == "$allowed" ]] && return 0
  done
  return 1
}

candidate_scope() {
  local base_root=$1 base_commit=$2 candidate_root=$3 candidate_commit=$4
  mapfile -t changed < <(git -C "$candidate_root" diff --name-only "$base_commit" "$candidate_commit")
  ((${#changed[@]} > 0)) ||
    die "candidate has no source diff from base"
  local path
  for path in "${changed[@]}"; do
    if ! is_allowed_path "$path"; then
      echo "CANDIDATE-BLOCKER forbidden changed path: $path"
      return 1
    fi
  done
  printf 'CANDIDATE-SCOPE paths=%s\n' "${changed[*]}"
}

candidate_forbidden_routes() {
  local root=$1
  local scan_paths=(
    "$root/packages/lix/src/transaction/commit.rs"
    "$root/packages/lix/src/transaction/context.rs"
    "$root/packages/lix/src/transaction/staging.rs"
    "$root/packages/lix/src/transaction/types.rs"
    "$root/packages/lix/src/forktree/publication.rs"
    "$root/packages/lix/src/forktree/blob.rs"
    "$root/packages/lix/src/sql2/providers/file.rs"
  )
  local path
  for path in "${scan_paths[@]}"; do
    [[ -f "$path" ]] || continue
    if rg -n -e 'PreparedPublication::commit' \
      -e 'stage_atomic_cas_publication' \
      -e 'execute_fast_lix_file_prepared_path_write' \
      -e 'binary_cas::kv' \
      -e 'fallback_full_write' \
      -e 'legacy_file_content_writer' "$path"; then
      echo "CANDIDATE-BLOCKER independent or legacy file/CAS route in $path"
      return 1
    fi
  done
}

candidate_green() {
  local root=$1 base_commit=$2 candidate_commit=$3
  require_commit "$root" "$base_commit"
  require_commit "$root" "$candidate_commit"
  [[ "$(rev_at_head "$root")" == "$candidate_commit" ]] ||
    die "candidate root HEAD does not equal supplied candidate commit"

  candidate_scope "$root" "$base_commit" "$root" "$candidate_commit" || return 1
  candidate_forbidden_routes "$root" || return 1

  local context_rs="$root/packages/lix/src/transaction/context.rs"
  local commit_rs="$root/packages/lix/src/transaction/commit.rs"
  local publication_rs="$root/packages/lix/src/forktree/publication.rs"
  local blob_rs="$root/packages/lix/src/forktree/blob.rs"
  local window read_var publication_call
  [[ -f "$context_rs" && -f "$commit_rs" && -f "$publication_rs" && -f "$blob_rs" ]] ||
    { echo "CANDIDATE-BLOCKER required W4a source closure missing"; return 1; }
  window=$(operation_window "$context_rs")
  [[ -n "$window" ]] ||
    { echo "CANDIDATE-BLOCKER commit_prepared operation not found"; return 1; }

  [[ "$(count_literal "$window" 'begin_read(')" == 1 ]] ||
    { echo "CANDIDATE-BLOCKER operation must own exactly one coherent read"; return 1; }
  read_var=$(awk '
    /let[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*=/ {
      pending = $0
      sub(/^.*let[[:space:]]+/, "", pending)
      sub(/[[:space:]]*=.*$/, "", pending)
    }
    pending != "" && /begin_read[(]/ { print pending; exit }
  ' <<<"$window")
  [[ -n "$read_var" ]] ||
    { echo "CANDIDATE-BLOCKER read binding is not visible"; return 1; }

  [[ "$(count_literal "$window" 'prepare_forktree_publication_with_parent_heads(')" == 1 ]] ||
    { echo "CANDIDATE-BLOCKER operation must build one ForkTree publication"; return 1; }
  publication_call=$(awk '
    /prepare_forktree_publication_with_parent_heads[(]/ { started = 1 }
    started { print }
    started && /[)][.]await/ { exit }
  ' <<<"$window")
  printf '%s\n' "$publication_call" | rg --quiet --regexp "\b${read_var}([.]clone[(][)])?" ||
    { echo "CANDIDATE-BLOCKER publication does not consume the operation-owned read"; return 1; }

  [[ "$(count_literal "$window" 'into_storage_plan()')" == 1 ]] ||
    { echo "CANDIDATE-BLOCKER operation must consume one PreparedPublication into one storage plan"; return 1; }
  [[ "$(count_literal "$window" 'prepare_write_set(')" == 1 ]] ||
    { echo "CANDIDATE-BLOCKER operation must prepare exactly one transaction write set"; return 1; }
  [[ "$(count_literal "$window" 'prepared_commit.commit().await')" == 1 ]] ||
    { echo "CANDIDATE-BLOCKER operation must commit exactly once"; return 1; }

  rg -Fq 'file_content_writes' "$commit_rs" ||
    { echo "CANDIDATE-BLOCKER file-content lowerer is not wired"; return 1; }
  ! rg -Fq "$BASELINE_REJECTION" "$commit_rs" ||
    { echo "CANDIDATE-BLOCKER baseline file-content rejection remains"; return 1; }

  local closure="$publication_rs"$'\n'"$(cat "$blob_rs")"
  local token
  for token in BlobId BlobManifestV1 BlobChunkV1 CoherentView PreparedPublication; do
    printf '%s\n' "$closure" | rg -Fq "$token" ||
      { echo "CANDIDATE-BLOCKER missing authenticated ownership token: $token"; return 1; }
  done
  candidate_forbidden_routes "$root" || return 1
  echo "CANDIDATE-PASS-01 scope and no legacy/independent route"
  echo "CANDIDATE-PASS-02 one read -> one publication -> one plan -> one prepare -> one commit"
  echo "CANDIDATE-PASS-03 BlobId/manifest/chunk/CoherentView closure is visible"
  echo "CANDIDATE-GREEN-RESULT=GREEN"
}

self_test() {
  local temp base candidate
  temp=$(mktemp -d "${TMPDIR:-/tmp}/w4a-source-green.XXXXXX")
  trap 'rm -rf "$temp"' RETURN
  mkdir -p "$temp/packages/lix/src/transaction" \
    "$temp/packages/lix/src/forktree" "$temp/packages/lix/src/sql2/providers"
  cat >"$temp/packages/lix/src/transaction/context.rs" <<'EOF'
async fn commit_prepared() {
    let old_read = storage.begin_read().await?;
    reject_old_route();
}
EOF
  cat >"$temp/packages/lix/src/transaction/commit.rs" <<'EOF'
fn reject_not_yet_lowered_cohorts() {
    "file payload publication requires the ForkTree receipt/manifest lowering slice";
}
EOF
  cat >"$temp/packages/lix/src/forktree/publication.rs" <<'EOF'
struct CoherentView;
struct BlobManifestV1;
struct BlobChunkV1;
struct BlobId;
struct PreparedPublication;
fn prepare_file_content(_view: &CoherentView, _m: BlobManifestV1, _c: &[BlobChunkV1], _id: BlobId) -> PreparedPublication { PreparedPublication }
EOF
  cat >"$temp/packages/lix/src/forktree/blob.rs" <<'EOF'
use super::{BlobChunkV1, BlobId, BlobManifestV1, CoherentView, PreparedPublication};
EOF
  git -C "$temp" init -q
  git -C "$temp" config user.email w4a@example.invalid
  git -C "$temp" config user.name w4a-source-verifier
  git -C "$temp" add .
  git -C "$temp" commit -qm baseline
  base=$(git -C "$temp" rev-parse HEAD)
  cat >"$temp/packages/lix/src/transaction/context.rs" <<'EOF'
async fn commit_prepared() {
    let commit_read = storage.begin_read().await?;
    let prepared_forktree_plan =
        prepare_forktree_publication_with_parent_heads(commit_read.clone()).await?;
    let (writes, _) = prepared_forktree_plan.into_storage_plan()?;
    let prepared_commit = storage.prepare_write_set(writes).await?;
    let _ = prepared_commit.commit().await?;
}
EOF
  cat >"$temp/packages/lix/src/transaction/commit.rs" <<'EOF'
fn lower_file_content(_writes: &mut Vec<PreparedPublication>) {
    let _ = file_content_writes();
}
EOF
  git -C "$temp" add .
  git -C "$temp" commit -qm candidate
  candidate=$(git -C "$temp" rev-parse HEAD)
  W4A_SELF_TEST=1 "$0" "$temp" "$base" "$temp" "$candidate"
}

if [[ ${1:-} == "--self-test" ]]; then
  self_test
  exit 0
fi

if [[ $# -eq 2 ]]; then
  source_baseline "$1" "$2"
  exit $?
fi
[[ $# -eq 4 ]] || usage

base_root=$1
base_commit=$2
candidate_root=$3
candidate_commit=$4
if [[ ${W4A_SELF_TEST:-0} != 1 ]]; then
  source_baseline "$base_root" "$base_commit" || true
else
  require_commit "$base_root" "$base_commit"
fi
candidate_green "$candidate_root" "$base_commit" "$candidate_commit"
