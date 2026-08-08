#!/usr/bin/env bash
set -euo pipefail

# Test/report-only seven-stage readiness overlay.
# Default mode is read-only provenance verification. Runtime is impossible
# unless RUN_RUNTIME=1, COMPILE_GREEN=1, and R1 is fully immutable/bound.

die() { printf 'BLOCKER\t%s\n' "$*" >&2; exit 1; }
note() { printf '%b\n' "$*"; }

root=${1:?usage: $0 <candidate-checkout> [candidate-head] [verify|commands|materialize|run]}
requested_head=${2:-HEAD}
mode=${3:-verify}
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
binding="$script_dir/R1_CHECKPOINT_GC_BINDING.tsv"
base_head=1f742a382c755399b8a49ab536c4f6dc55fffdd8
base_tree=860a047b98eaa38368a3d889497628e244c2e0ec
base_parent=7c9b1060bc396dfa54efcc6c888e37894a7cfb04
base_parent_tree=ee96c5b64912b8fa8bb15fb7c31916244a255523
base_diff=18a7df6d37fce9809b2214f5b1530204b1a2dd4cf19760aa876ec7856249dbc7
main_head=822c204ce0670969ca71045bc74f9ca25fde8093
main_tree=fac3f2b713683be17c34515062dd72edc8feed95

gitc() { git -C "$root" "$@"; }
sha_file() { sha256sum "$1" | awk '{print $1}'; }

verify_identity() {
  [[ -d "$root/.git" || -f "$root/.git" ]] || die "candidate is not a git checkout: $root"
  [[ -z "$(gitc status --porcelain)" ]] || die "candidate checkout is dirty"
  local actual tree parent parent_tree diff
  actual=$(gitc rev-parse "$requested_head^{commit}") || die "candidate head is unresolved"
  tree=$(gitc rev-parse "$actual^{tree}")
  [[ "$actual" == "$requested_head" || "$requested_head" == HEAD ]] || die "candidate head mismatch requested=$requested_head actual=$actual"
  gitc cat-file -e "$base_head^{commit}" || die "exact 1f742 anchor is unavailable"
  [[ "$(gitc rev-parse "$base_head^{tree}")" == "$base_tree" ]] || die "1f742 tree mismatch"
  parent=$(gitc rev-parse "$base_head^")
  [[ "$parent" == "$base_parent" ]] || die "1f742 parent mismatch"
  parent_tree=$(gitc rev-parse "$base_parent^{tree}")
  [[ "$parent_tree" == "$base_parent_tree" ]] || die "1f742 parent tree mismatch"
  diff=$(gitc diff --binary --full-index "$base_parent..$base_head" | sha256sum | awk '{print $1}')
  [[ "$diff" == "$base_diff" ]] || die "1f742 parent diff mismatch expected=$base_diff actual=$diff"
  gitc merge-base --is-ancestor "$base_head" "$actual" || die "candidate is not a descendant of exact 1f742"
  note "anchor\t1f742\tPASS\t$base_head\t$base_tree\t$base_diff"
  note "candidate\tPASS\t$actual\t$tree\tcurrent-main-anchor=$main_head/$main_tree"
}

verify_scope() {
  local forbidden='(^|/)(OBJECT_SPACE|UNTRACKED_ROW_SPACE|StorageRead::scan|ScanOptions|ScanPlan|ScanPlanCursor|FileStorage|sqlite-storage)([^A-Za-z0-9_]|$)'
  local p
  for p in \
    FORKTREE_STAGE2_SEVEN_STAGE_OVERLAY.md \
    FORKTREE_STAGE2_SEVEN_STAGE_OVERLAY.tsv \
    R1_CHECKPOINT_GC_BINDING.tsv \
    forktree_stage2_seven_stage_overlay.sh; do
    [[ "$p" =~ $forbidden ]] && die "overlay scope contains forbidden legacy token in path: $p"
    [[ -f "$script_dir/$p" ]] || die "overlay artifact missing: $p"
  done
  [[ "$(sha_file "$script_dir/FORKTREE_STAGE2_SEVEN_STAGE_OVERLAY.tsv")" != "" ]] || die "missing stage map"
  [[ "$(sha_file "$binding")" != "" ]] || die "missing R1 binding placeholder"
  note "scope\tPASS\tmanifest=TSV/MD/binding/script\tproduction-source=untouched\tno-oracle-copy"
}

r1_status() {
  awk -F '\t' '$1=="status" {print $2}' "$binding"
}

verify_r1() {
  local status
  status=$(r1_status)
  if [[ "$status" != ready ]]; then
    note "r1\tHOLD\tstatus=$status\tno checkpoint/GC runtime enabled"
    return 0
  fi
  for key in ref head tree parent source_sha256 report_sha256 rocks_checkpoint_case slate_checkpoint_case rocks_gc_case slate_gc_case; do
    local value
    value=$(awk -F '\t' -v k="$key" '$1==k {print $2}' "$binding")
    [[ -n "$value" && "$value" != UNBOUND ]] || die "R1 binding is ready but $key is unbound"
  done
  note "r1\tREADY\t$(awk -F '\t' '$1=="ref" {print $2}' "$binding")\t$(awk -F '\t' '$1=="head" {print $2}' "$binding")\t$(awk -F '\t' '$1=="tree" {print $2}' "$binding")"
}

print_commands() {
  cat <<'EOF'
seven-stage execution order (all cells timeout 20m; RocksDB before SlateDB):
1 static-owner: P0 verify -> residue audit -> semantic/CLI/cursor audit -> fmt/diff -> warnings-denied Clippy
2 delete-65:    forktree_delete_repro rocksdb -> slatedb, fresh DB each
3 sql-dml:      forktree_stage2_sql_dml_rocksdb -> _slatedb, fresh DB each
4 version-control: forktree_stage2_version_control_rocksdb -> _slatedb, fresh DB each
5 parsed-files-blobref: discovery vc-rocks-1k -> vc-slate-1k -> blob-rocks-64 -> blob-slate-64
6 checkpoint-recovery: R1 exact RocksDB case -> R1 exact SlateDB case
7 gc-publication: R1 exact RocksDB case -> R1 exact SlateDB case
post-landing only: point-read, OLAP, broad VC/history, multimedia, 512 MiB, comparator/scaling
runtime is dormant until verify reports candidate compile-green and R1 READY.
EOF
}

case "$mode" in
  verify)
    verify_identity
    verify_scope
    verify_r1
    note "runtime\tDORMANT\tset RUN_RUNTIME=1 only after explicit compile-green immutable approval"
    ;;
  commands)
    print_commands
    ;;
  materialize)
    [[ -z "$(gitc status --porcelain)" ]] || die "candidate checkout is dirty"
    overlay=${OVERLAY_DIR:?set OVERLAY_DIR to a fresh nonexistent directory}
    [[ ! -e "$overlay" ]] || die "OVERLAY_DIR already exists: $overlay"
    gitc worktree add --detach "$overlay" "${requested_head:-HEAD}"
    mkdir -p "$overlay/.stage2-acceptance-overlay"
    cp "$script_dir"/FORKTREE_STAGE2_SEVEN_STAGE_OVERLAY.{md,tsv,sh} "$overlay/.stage2-acceptance-overlay/"
    cp "$binding" "$overlay/.stage2-acceptance-overlay/"
    note "materialized\tPASS\t$overlay\tproduction paths unchanged\truntime dormant"
    ;;
  run)
    [[ "${RUN_RUNTIME:-0}" == 1 ]] || die "runtime disabled; set RUN_RUNTIME=1 explicitly"
    [[ "${COMPILE_GREEN:-0}" == 1 ]] || die "candidate compile-green attestation is required"
    verify_identity
    verify_scope
    [[ "$(r1_status)" == ready ]] || die "R1 immutable checkpoint/GC binding is not ready"
    die "execution handoff requires the exact R1 command fields; use commands after binding"
    ;;
  *) die "unknown mode: $mode (verify|commands|materialize|run)" ;;
esac
