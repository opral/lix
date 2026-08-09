#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 5 || $# -gt 6 ]]; then
  echo "usage: $0 MAIN_BINARY CANDIDATE_BINARY BACKEND BRANCHES CANDIDATE_ROOT [OUTPUT_DIR]" >&2
  exit 2
fi

main_binary=$(realpath "$1")
candidate_binary=$(realpath "$2")
backend=$3
branches=$4
candidate_root=$(realpath "$5")
output_dir=${6:-$(mktemp -d)}
mkdir -p "$output_dir"

main_log="$output_dir/main-${backend}-b${branches}.log"
candidate_log="$output_dir/candidate-${backend}-b${branches}.log"

timeout 1200 /usr/bin/time -v "$main_binary" "$backend" "$branches" 3 >"$main_log" 2>&1
timeout 1200 /usr/bin/time -v "$candidate_binary" "$backend" "$branches" 3 >"$candidate_log" 2>&1

normalize() {
  rg '^sample=0 surface=(branch_listing|branch_ref_r1|branch_ref_rmany|derived_commit_by_branch_(empty|explicit|global)) ' "$1" \
    | sed -E 's/ elapsed_us=[0-9]+ rows=/ rows=/; s/ digest=[^ ]+//'
  rg '^lifecycle=post_delete ' "$1" \
    | sed -E 's/ listing_digest=[^ ]+ refs_digest=[^ ]+//'
  rg '^cold_reopen_us=' "$1" \
    | sed -E 's/cold_reopen_us=[0-9]+ //; s/^listing_digest=[^ ]+ refs_digest=[^ ]+ //; s/ listing_digest=[^ ]+ refs_digest=[^ ]+//; s/^ //'
}

main_normalized="$output_dir/main-${backend}-b${branches}.normalized"
candidate_normalized="$output_dir/candidate-${backend}-b${branches}.normalized"
normalize "$main_log" >"$main_normalized"
normalize "$candidate_log" >"$candidate_normalized"

[[ $(wc -l <"$main_normalized") -eq 8 ]] || {
  echo "main oracle output is incomplete: $main_log" >&2
  exit 1
}
[[ $(wc -l <"$candidate_normalized") -eq 8 ]] || {
  echo "candidate oracle output is incomplete: $candidate_log" >&2
  exit 1
}

diff -u "$main_normalized" "$candidate_normalized"

context="$candidate_root/packages/lix/src/commit_graph/context.rs"
branch_ref="$candidate_root/packages/lix/src/sql2/branch_ref.rs"
for required in \
  derived_explicit_branch_filter_uses_one_requested_metadata_batch \
  derived_explicit_missing_branch_fails_closed_after_one_requested_batch \
  global_commit_surface_reuses_scanned_heads_without_global_point_reload \
  branch_ref_entity_scope_uses_requested_metadata_batch \
  branch_ref_exact_batch_uses_requested_metadata_batch; do
  rg -F -q "$required" "$context" || {
    echo "missing batch-bound source control: $required" >&2
    exit 1
  }
done
rg -F -q 'load_head_metadata_batch' "$branch_ref" || {
  echo "candidate has no PreparedBranchRefReader metadata-batch fence" >&2
  exit 1
}
rg -F -q 'scan_head_metadata' "$context" || {
  echo "candidate has no explicit scan-vs-batch branch-head split" >&2
  exit 1
}

echo "BRANCH_LIFECYCLE_ORACLE=PASS backend=$backend branches=$branches"
echo "main_log=$main_log"
echo "candidate_log=$candidate_log"
echo "normalized=$main_normalized,$candidate_normalized"
