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

function_block() {
  local file=$1
  local function_name=$2
  awk -v fn="$function_name" '
    $0 ~ "fn[[:space:]]*" fn "[[:space:]]*[(]" { started = 1 }
    started {
      print
      opens = gsub(/\{/, "{")
      closes = gsub(/\}/, "}")
      depth += opens - closes
      if (index($0, "{") > 0) saw_body = 1
      if (saw_body && depth <= 0) exit
    }
  ' "$file"
}

outside_operation() {
  awk '
    /async[[:space:]]+fn[[:space:]]+commit_prepared[[:space:]]*\(/ { skipping = 1 }
    skipping {
      if (/prepared_commit[.]commit[(][)]([.]await)?/) { skipping = 0 }
      next
    }
    { print }
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
  local candidate_root=$1 base_commit=$2 candidate_commit=$3
  git -C "$candidate_root" merge-base --is-ancestor "$base_commit" "$candidate_commit" || {
    echo "CANDIDATE-BLOCKER candidate is not descended from the exact anchor"
    return 1
  }
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

candidate_direct_writer_scan() {
  local root=$1
  local context_rs="$root/packages/lix/src/transaction/context.rs"
  local outside
  outside=$(outside_operation "$context_rs")
  if printf '%s\n' "$outside" | rg -n -e 'begin_write[(]' \
      -e 'prepare_write_set[(]' \
      -e 'StorageWrite' \
      -e 'storage[.]write[(]' \
      -e 'storage[.](put|delete|commit)[(]' \
      -e 'backend[.](put|delete|commit|write)[(]' \
      -e 'PreparedPublication::commit'; then
    echo "CANDIDATE-BLOCKER writer route outside commit_prepared"
    return 1
  fi

  local paths=(
    "$root/packages/lix/src/transaction/commit.rs"
    "$root/packages/lix/src/transaction/staging.rs"
    "$root/packages/lix/src/transaction/types.rs"
    "$root/packages/lix/src/forktree/publication.rs"
    "$root/packages/lix/src/forktree/blob.rs"
    "$root/packages/lix/src/sql2/providers/file.rs"
  )
  local path
  for path in "${paths[@]}"; do
    [[ -f "$path" ]] || continue
    if rg -ni -e 'begin_read[(]' \
      -e 'begin_write[(]' \
      -e 'prepare_write_set[(]' \
      -e 'StorageWrite' \
      -e 'storage[.]write[(]' \
      -e 'storage[.](put|delete|commit)[(]' \
      -e 'backend[.](put|delete|commit|write)[(]' \
      -e 'PreparedPublication::commit' \
      -e 'FileContent(Cache|Index)' \
      -e 'Blob(Content)?(Cache|Index)' \
      -e 'Secondary.*Authority' \
      -e 'Shadow.*(Writer|Index)' \
      -e 'Legacy.*(Writer|Reader)' \
      -e 'Fallback.*(Writer|Read)' "$path"; then
      echo "CANDIDATE-BLOCKER alternate writer/cache/authority in $path"
      return 1
    fi
  done
}

candidate_blob_authority_scan() {
  local root=$1
  local publication_rs="$root/packages/lix/src/forktree/publication.rs"
  local blob_rs="$root/packages/lix/src/forktree/blob.rs"
  local closure="$publication_rs"$'\n'"$(cat "$blob_rs")"
  local prep_body prep_header
  prep_body=$(function_block "$publication_rs" "prepare_file_content")
  [[ -n "$prep_body" ]] ||
    { echo "CANDIDATE-BLOCKER prepare_file_content owner function missing"; return 1; }
  prep_header=$(printf '%s\n' "$prep_body" | sed '/{/q')
  if printf '%s\n' "$prep_header" | rg -q '(^|[^a-z_])blob_id[[:space:]]*:'; then
    echo "CANDIDATE-BLOCKER caller-supplied blob_id argument"
    return 1
  fi
  if rg -n 'pub[[:space:]]+(struct|enum|type)[[:space:]]+BlobId' "$publication_rs" "$blob_rs"; then
    echo "CANDIDATE-BLOCKER BlobId owner is public"
    return 1
  fi
  rg -n '^[[:space:]]*struct[[:space:]]+BlobId' "$publication_rs" "$blob_rs" >/dev/null ||
    { echo "CANDIDATE-BLOCKER owner-private BlobId declaration missing"; return 1; }
  rg -n '^[[:space:]]*fn[[:space:]]+from_ordered_manifest[[:space:]]*[(]' "$publication_rs" "$blob_rs" >/dev/null ||
    { echo "CANDIDATE-BLOCKER canonical ordered BlobId derivation missing"; return 1; }

  local auth_line derive_line compare_line bytes_line row_line
  auth_line=$(printf '%s\n' "$prep_body" | rg -n 'authenticate_ordered_chunks' | head -n1 | cut -d: -f1 || true)
  derive_line=$(printf '%s\n' "$prep_body" | rg -n 'owner_blob_id[[:space:]]*=.*from_ordered_manifest' | head -n1 | cut -d: -f1 || true)
  compare_line=$(printf '%s\n' "$prep_body" | rg -n 'row_blob_id.*(!=|==).*owner_blob_id|owner_blob_id.*(!=|==).*row_blob_id' | head -n1 | cut -d: -f1 || true)
  bytes_line=$(printf '%s\n' "$prep_body" | rg -n 'read_authenticated_range[[:space:]]*[(]' | head -n1 | cut -d: -f1 || true)
  [[ -n "$auth_line" && -n "$derive_line" && -n "$compare_line" && -n "$bytes_line" ]] ||
    { echo "CANDIDATE-BLOCKER authenticated derive/row compare/payload sequence missing"; return 1; }
  ((auth_line < derive_line && derive_line < compare_line && compare_line < bytes_line)) ||
    { echo "CANDIDATE-BLOCKER row identity is not checked before payload bytes"; return 1; }
  row_line=$(printf '%s\n' "$prep_body" | rg -n 'let[[:space:]]+row_blob_id[[:space:]]*=' | head -n1 || true)
  printf '%s\n' "$row_line" | rg -q '=[[:space:]]*read[.]row_identity[[:space:]]*;' ||
    { echo "CANDIDATE-BLOCKER row identity is not the exact retained-read identity"; return 1; }
  if printf '%s\n' "$prep_body" | rg -n 'read_(full|all)_payload|load_(full|all)_payload|read_payload_bytes|payload_bytes_all|read_all' >/dev/null; then
    echo "CANDIDATE-BLOCKER file publication requests an unbounded/full payload"
    return 1
  fi

  for token in BlobManifestV1 BlobChunkV1 CoherentView ReadLease PreparedPublication; do
    printf '%s\n' "$closure" | rg -Fq "$token" ||
      { echo "CANDIDATE-BLOCKER missing view/manifest/chunk owner token: $token"; return 1; }
  done
  if printf '%s\n' "$closure" | rg -n 'derive[(][^)]*blob_id|caller_blob_id|supplied_blob_id'; then
    echo "CANDIDATE-BLOCKER caller-supplied BlobId route"
    return 1
  fi
  if printf '%s\n' "$closure" | rg -n '#\[derive\([^)]*(Copy|Clone)'; then
    echo "CANDIDATE-BLOCKER view/lease is copyable"
    return 1
  fi
  printf '%s\n' "$prep_body" | rg -n 'read_id|view_id' >/dev/null ||
    { echo "CANDIDATE-BLOCKER publication does not bind read/view identity"; return 1; }
}

candidate_publication_scan() {
  local root=$1
  local publication_rs="$root/packages/lix/src/forktree/publication.rs"
  local count
  count=$(rg -n 'fn[[:space:]]+prepare_file_content[[:space:]]*[(]' "$publication_rs" | wc -l | tr -d ' ')
  [[ "$count" == 1 ]] ||
    { echo "CANDIDATE-BLOCKER expected one file-content publication constructor, got $count"; return 1; }
  if rg -n 'fn[[:space:]]+(publish|write)_file_content[[:space:]]*[(]' "$publication_rs"; then
    echo "CANDIDATE-BLOCKER second file-content publication route"
    return 1
  fi
}

candidate_green() {
  local root=$1 base_commit=$2 candidate_commit=$3
  require_commit "$root" "$base_commit"
  require_commit "$root" "$candidate_commit"
  [[ "$(rev_at_head "$root")" == "$candidate_commit" ]] ||
    die "candidate root HEAD does not equal supplied candidate commit"

  candidate_scope "$root" "$base_commit" "$candidate_commit" || return 1
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
  printf '%s\n' "$publication_call" | rg --quiet --regexp "\b${read_var}\b" ||
    { echo "CANDIDATE-BLOCKER publication does not consume the operation-owned read"; return 1; }
  if printf '%s\n' "$window" | rg -Fq "${read_var}.clone("; then
    echo "CANDIDATE-BLOCKER operation copies the retained read/lease"
    return 1
  fi

  [[ "$(count_literal "$window" 'into_storage_plan()')" == 1 ]] ||
    { echo "CANDIDATE-BLOCKER operation must consume one publication into one storage plan"; return 1; }
  [[ "$(count_literal "$window" 'prepare_write_set(')" == 1 ]] ||
    { echo "CANDIDATE-BLOCKER operation must prepare exactly one transaction write set"; return 1; }
  [[ "$(count_literal "$window" 'prepared_commit.commit().await')" == 1 ]] ||
    { echo "CANDIDATE-BLOCKER operation must commit exactly once"; return 1; }

  rg -Fq 'file_content_writes' "$commit_rs" ||
    { echo "CANDIDATE-BLOCKER file-content lowerer is not wired"; return 1; }
  ! rg -Fq "$BASELINE_REJECTION" "$commit_rs" ||
    { echo "CANDIDATE-BLOCKER baseline file-content rejection remains"; return 1; }

  candidate_publication_scan "$root" || return 1
  candidate_blob_authority_scan "$root" || return 1
  candidate_direct_writer_scan "$root" || return 1
  echo "CANDIDATE-PASS-01 whole allowlisted closure has no second writer/cache/authority"
  echo "CANDIDATE-PASS-02 one read -> one publication -> one plan -> one prepare -> one commit"
  echo "CANDIDATE-PASS-03 private BlobId derives from authenticated ordered closure before payload bytes"
  echo "CANDIDATE-PASS-04 non-copy read/lease identity is argument-bound"
  echo "CANDIDATE-GREEN-RESULT=GREEN"
}

self_test() {
  local temp base candidate fixture fixture_candidate output
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
struct CoherentView { view_id: u64 }
struct ReadLease { view: CoherentView, read_id: u64, row_identity: BlobId }
struct BlobManifestV1;
struct BlobChunkV1;
struct BlobId([u8; 32]);
struct PreparedPublication;
impl BlobId {
    fn from_ordered_manifest(_manifest: &BlobManifestV1, _chunks: &[BlobChunkV1]) -> Result<Self, Error> {
        Ok(BlobId([0; 32]))
    }
}
fn authenticate_ordered_chunks(_manifest: &BlobManifestV1, _chunks: &[BlobChunkV1]) -> Result<(), Error> {
    Ok(())
}
fn read_authenticated_range(_read: &ReadLease) -> Result<Vec<u8>, Error> {
    Ok(Vec::new())
}
fn prepare_file_content(
    read: &ReadLease,
    manifest: BlobManifestV1,
    chunks: Vec<BlobChunkV1>,
) -> Result<PreparedPublication, Error> {
    authenticate_ordered_chunks(&manifest, &chunks)?;
    let owner_blob_id = BlobId::from_ordered_manifest(&manifest, &chunks)?;
    let row_blob_id = read.row_identity;
    if row_blob_id != owner_blob_id {
        return Err(Error);
    }
    let _bytes = read_authenticated_range(read)?;
    let _ = read.view.view_id;
    let _ = read.read_id;
    Ok(PreparedPublication)
}
struct Error;
EOF
  cat >"$temp/packages/lix/src/forktree/blob.rs" <<'EOF'
use super::{BlobChunkV1, BlobId, BlobManifestV1, CoherentView, PreparedPublication, ReadLease};
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
        prepare_forktree_publication_with_parent_heads(commit_read).await?;
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

  make_fixture() {
    fixture=$(mktemp -d "${TMPDIR:-/tmp}/w4a-source-negative.XXXXXX")
    git -C "$temp" archive "$candidate" | tar -x -C "$fixture"
    cp -a "$temp/.git" "$fixture/.git"
    git -C "$fixture" reset --mixed "$candidate" >/dev/null
  }

  expect_blocker() {
    local label=$1
    if output=$(W4A_SELF_TEST=1 "$0" "$fixture" "$base" "$fixture" "$fixture_candidate" 2>&1); then
      printf 'NEGATIVE-BLOCKER %s unexpectedly accepted\n%s\n' "$label" "$output"
      return 1
    fi
    printf 'NEGATIVE-PASS %s\n' "$label"
  }

  make_fixture
  perl -0pi -e 's/fn prepare_file_content\(\n    read:/fn prepare_file_content(\n    blob_id: BlobId,\n    read:/' "$fixture/packages/lix/src/forktree/publication.rs"
  git -C "$fixture" add . && git -C "$fixture" commit -qm negative-caller-blob-id
  fixture_candidate=$(git -C "$fixture" rev-parse HEAD)
  expect_blocker caller-supplied-BlobId

  make_fixture
  sed -i 's/let row_blob_id = read.row_identity;/let other_read = read; let row_blob_id = other_read.row_identity;/' \
    "$fixture/packages/lix/src/forktree/publication.rs"
  git -C "$fixture" add . && git -C "$fixture" commit -qm negative-swapped-row-read
  fixture_candidate=$(git -C "$fixture" rev-parse HEAD)
  expect_blocker swapped-row-read-identity

  make_fixture
  sed -i '/authenticate_ordered_chunks/i\    let _bytes = read_authenticated_range(read)?;' \
    "$fixture/packages/lix/src/forktree/publication.rs"
  git -C "$fixture" add . && git -C "$fixture" commit -qm negative-validation-after-bytes
  fixture_candidate=$(git -C "$fixture" rev-parse HEAD)
  expect_blocker validation-after-byte-request

  make_fixture
  sed -i 's/read_authenticated_range/read_full_payload/g' \
    "$fixture/packages/lix/src/forktree/publication.rs"
  git -C "$fixture" add . && git -C "$fixture" commit -qm negative-full-payload
  fixture_candidate=$(git -C "$fixture" rev-parse HEAD)
  expect_blocker copied-or-full-payload-range

  make_fixture
  printf '\nstruct FileContentCache;\nstruct BlobIndex;\nstruct AlternateAuthority;\nstruct LegacyWriter;\n' >> \
    "$fixture/packages/lix/src/forktree/blob.rs"
  git -C "$fixture" add . && git -C "$fixture" commit -qm negative-alternate-authority
  fixture_candidate=$(git -C "$fixture" rev-parse HEAD)
  expect_blocker alternate-writer-cache-index-authority

  make_fixture
  sed -i '/let commit_read = storage.begin_read()/a\    let second_read = storage.begin_read().await?;' \
    "$fixture/packages/lix/src/transaction/context.rs"
  git -C "$fixture" add . && git -C "$fixture" commit -qm negative-second-read
  fixture_candidate=$(git -C "$fixture" rev-parse HEAD)
  expect_blocker second-read-view

  make_fixture
  sed -i '/let (writes, _) = prepared_forktree_plan.into_storage_plan()/a\    let _second_plan = prepared_forktree_plan.into_storage_plan()?;' \
    "$fixture/packages/lix/src/transaction/context.rs"
  git -C "$fixture" add . && git -C "$fixture" commit -qm negative-second-plan
  fixture_candidate=$(git -C "$fixture" rev-parse HEAD)
  expect_blocker second-plan-commit

  make_fixture
  sed -i 's/prepare_forktree_publication_with_parent_heads(commit_read)/prepare_forktree_publication_with_parent_heads(other_read)/' \
    "$fixture/packages/lix/src/transaction/context.rs"
  git -C "$fixture" add . && git -C "$fixture" commit -qm negative-mismatched-read-argument
  fixture_candidate=$(git -C "$fixture" rev-parse HEAD)
  expect_blocker mismatched-read-argument

  make_fixture
  mkdir -p "$fixture/packages/lix/src/sql2/providers"
  printf '\nfn legacy_fallback_reader() {}\nfn compatibility_writer() {}\n' >> \
    "$fixture/packages/lix/src/sql2/providers/file.rs"
  git -C "$fixture" add . && git -C "$fixture" commit -qm negative-fallback-compat
  fixture_candidate=$(git -C "$fixture" rev-parse HEAD)
  expect_blocker fallback-compatibility-route

  make_fixture
  mkdir -p "$fixture/packages/lix/src/transaction"
  printf 'fn forbidden_escape() {}\n' > "$fixture/packages/lix/src/transaction/forbidden.rs"
  git -C "$fixture" add . && git -C "$fixture" commit -qm negative-scope-escape
  fixture_candidate=$(git -C "$fixture" rev-parse HEAD)
  expect_blocker production-path-scope-escape
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
