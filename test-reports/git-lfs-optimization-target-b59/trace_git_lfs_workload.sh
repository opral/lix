#!/usr/bin/env bash
set -euo pipefail

repo=${1:?usage: $0 <pinned-checkout> <output-dir>}
out=${2:?usage: $0 <pinned-checkout> <output-dir>}
[[ -d "$repo/.git" ]] || { echo "not a git checkout: $repo" >&2; exit 2; }
cell_timeout_seconds=${CELL_TIMEOUT_SECONDS:-1200}
mkdir -p "$out/cells" "$out/metadata"

head=$(git -C "$repo" rev-parse HEAD)
tree=$(git -C "$repo" rev-parse HEAD^{tree})
parent=$(git -C "$repo" rev-parse HEAD^)
remote=$(git -C "$repo" remote get-url origin)
export head tree parent repo out

{
  echo "remote=$remote"
  echo "head=$head"
  echo "tree=$tree"
  echo "parent=$parent"
  git -C "$repo" count-objects -v
  git -C "$repo" ls-tree -r -l "$head"
} > "$out/metadata/repository.txt"
git -C "$repo" ls-tree -r -l "$head" | sha256sum > "$out/metadata/tree-list.sha256"
git -C "$repo" lfs ls-files --long > "$out/metadata/lfs-pointers.txt"
sha256sum "$out/metadata/lfs-pointers.txt" > "$out/metadata/lfs-pointers.sha256"
git -C "$repo" lfs ls-files --long | wc -l > "$out/metadata/lfs-count.txt"
{
  du -sb "$repo/.git" 2>/dev/null | awk '{print "git_dir_bytes=" $1}'
  du -sb "$repo/.git/lfs/objects" 2>/dev/null | awk '{print "lfs_objects_bytes=" $1}'
  du -sb --exclude=.git "$repo" 2>/dev/null | awk '{print "working_tree_bytes=" $1}'
} > "$out/metadata/storage-before.txt" || true

run_cell() {
  local label=$1
  shift
  local cell="$out/cells/$label"
  mkdir -p "$cell"
  local status=0
  /usr/bin/time -v -o "$cell/time.txt" timeout "${cell_timeout_seconds}s" "$@" >"$cell/stdout.txt" 2>"$cell/stderr.txt" || status=$?
  printf 'label=%s\nstatus=%s\n' "$label" "$status" > "$cell/result.env"
  sha256sum "$cell/stdout.txt" "$cell/stderr.txt" "$cell/time.txt" > "$cell/hashes.sha256"
  if ((status != 0)); then
    echo "cell failed: $label ($status)" >&2
    exit "$status"
  fi
}

run_cell replay bash -c 'git -C "$repo" rev-list --all --objects | git -C "$repo" cat-file --batch-check="%(objectname) %(objecttype) %(objectsize)"'
run_cell status git -C "$repo" status --short --branch
run_cell history bash -c 'git -C "$repo" rev-list --count --all; git -C "$repo" log --all --date-order --format="%H %P %s" -n 100'
run_cell branch bash -c 'git -C "$repo" branch --force workload-target "$head"; git -C "$repo" branch --delete --force workload-target'
run_cell diff git -C "$repo" diff --name-status --stat "$parent" "$head"
run_cell merge bash -c 'git -C "$repo" branch --force workload-merge "$parent"; git -C "$repo" merge --ff-only "$head"; git -C "$repo" branch --delete --force workload-merge'
run_cell reopen_checkout bash -c 'fresh="$out/reopen-checkout"; git clone --local --no-hardlinks --no-checkout "$repo" "$fresh"; git -C "$fresh" checkout --detach "$head"; git -C "$fresh" lfs checkout'
run_cell lfs_inventory bash -c 'git -C "$repo" lfs env; git -C "$repo" lfs ls-files --long; git -C "$repo" lfs status'
run_cell lfs_fsck git -C "$repo" lfs fsck --pointers
run_cell lfs_fetch git -C "$repo" lfs fetch origin "$head"
run_cell gc_repack bash -c 'git -C "$repo" gc --prune=now; git -C "$repo" lfs prune --dry-run'

{
  du -sb "$repo/.git" 2>/dev/null | awk '{print "git_dir_bytes=" $1}'
  du -sb "$repo/.git/lfs/objects" 2>/dev/null | awk '{print "lfs_objects_bytes=" $1}'
  du -sb --exclude=.git "$repo" 2>/dev/null | awk '{print "working_tree_bytes=" $1}'
} > "$out/metadata/storage-after.txt" || true
git -C "$repo" count-objects -v > "$out/metadata/count-after.txt"
printf 'head=%s\ntree=%s\nparent=%s\n' "$head" "$tree" "$parent" > "$out/metadata/final.env"
