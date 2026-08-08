#!/usr/bin/env bash
set -euo pipefail

# Test/report-only seven-stage readiness overlay.
# Default mode is read-only provenance verification. Runtime is impossible
# unless RUN_RUNTIME=1, COMPILE_GREEN=1, R1 is fully immutable/bound, and the
# reviewed R5 correction plus W5/R7 contract are bound.

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
r5_binding="$script_dir/R5_CORRECTED_FRONTIER_BINDING.tsv"
w5_binding="$script_dir/W5_R7_GC_REACHABILITY_CONTRACT.tsv"
reader_binding="$script_dir/READER_FRONTIER_BINDING.tsv"

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
  note "anchor\t1f742\tBLOCKED\t$base_head\t$base_tree\t$base_diff\tmissing CommitRecord fail-closed correction"
  if gitc merge-base --is-ancestor "$base_head" "$actual"; then
    if [[ "$(r5_status)" == ready ]] && gitc merge-base --is-ancestor "$(r5_value corrected_head)" "$actual"; then
      note "candidate\tR5-LINEAGE\t$actual\t$tree\tdescends from reviewed R5 correction"
    else
      note "candidate\tBLOCKED-LINEAGE\t$actual\t$tree\t1f742 lineage is not executable before exact R5 transport"
    fi
  else
    note "candidate\tUNBOUND\t$actual\t$tree\tawaiting R5 corrected frontier"
  fi
}

verify_scope() {
  local forbidden='(^|/)(OBJECT_SPACE|UNTRACKED_ROW_SPACE|StorageRead::scan|ScanOptions|ScanPlan|ScanPlanCursor|FileStorage|sqlite-storage)([^A-Za-z0-9_]|$)'
  local p
  for p in \
    FORKTREE_STAGE2_SEVEN_STAGE_OVERLAY.md \
    FORKTREE_STAGE2_SEVEN_STAGE_OVERLAY.tsv \
    R1_CHECKPOINT_GC_BINDING.tsv \
    R5_CORRECTED_FRONTIER_BINDING.tsv \
    W5_R7_GC_REACHABILITY_CONTRACT.tsv \
    READER_FRONTIER_BINDING.tsv \
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

r5_status() {
  awk -F '\t' '$1=="status" {print $2}' "$r5_binding"
}

binding_value() {
  local key=$1
  awk -F '\t' -v k="$key" '$1==k {print $2}' "$binding"
}

r5_value() {
  local key=$1
  awk -F '\t' -v k="$key" '$1==k {print $2}' "$r5_binding"
}

reader_status() {
  awk -F '\t' '$1=="status" {print $2}' "$reader_binding"
}

reader_value() {
  local key=$1
  awk -F '\t' -v k="$key" '$1==k {print $2}' "$reader_binding"
}

verify_r1() {
  local status
  status=$(r1_status)
  if [[ "$status" != ready ]]; then
    note "r1\tHOLD\tstatus=$status\tno checkpoint/GC runtime enabled"
    return 0
  fi
  for key in ref head tree parent full_index_diff_sha256 ordinary_diff_sha256 patch_id report_sha256 checkpoint_test_path checkpoint_test_sha256 oracle_report_path oracle_report_sha256 rocks_checkpoint_case slate_checkpoint_case gc_contract_case rocks_gc_case slate_gc_case; do
    local value
    value=$(binding_value "$key")
    [[ -n "$value" && "$value" != UNBOUND ]] || die "R1 binding is ready but $key is unbound"
  done
  local advertised_ref remote_branch local_ref actual tree parent full_index ordinary patch_id test_sha report_sha
  advertised_ref=$(binding_value ref)
  remote_branch=${advertised_ref#origin/}
  local_ref=refs/stage2-acceptance-overlay/r1
  timeout 20m git -C "$root" fetch --no-tags origin "+refs/heads/$remote_branch:$local_ref" >/dev/null
  actual=$(gitc rev-parse "$local_ref^{commit}")
  tree=$(gitc rev-parse "$local_ref^{tree}")
  parent=$(gitc rev-parse "$local_ref^")
  [[ "$actual" == "$(binding_value head)" ]] || die "R1 head mismatch expected=$(binding_value head) actual=$actual"
  [[ "$tree" == "$(binding_value tree)" ]] || die "R1 tree mismatch expected=$(binding_value tree) actual=$tree"
  [[ "$parent" == "$(binding_value parent)" ]] || die "R1 parent mismatch expected=$(binding_value parent) actual=$parent"
  full_index=$(gitc -c core.abbrev=40 -c diff.noprefix=false diff --binary --full-index --no-ext-diff "$parent..$local_ref" | sha256sum | awk '{print $1}')
  [[ "$full_index" == "$(binding_value full_index_diff_sha256)" ]] || die "R1 full-index diff mismatch expected=$(binding_value full_index_diff_sha256) actual=$full_index"
  ordinary=$(gitc diff --no-ext-diff "$parent..$local_ref" | sha256sum | awk '{print $1}')
  [[ "$ordinary" == "$(binding_value ordinary_diff_sha256)" ]] || die "R1 ordinary diff mismatch expected=$(binding_value ordinary_diff_sha256) actual=$ordinary"
  patch_id=$(gitc -c core.abbrev=40 -c diff.noprefix=false diff --binary --full-index --no-ext-diff "$parent..$local_ref" | git patch-id --stable | awk '{print $1}')
  [[ "$patch_id" == "$(binding_value patch_id)" ]] || die "R1 patch-id mismatch expected=$(binding_value patch_id) actual=$patch_id"
  test_sha=$(gitc show "$local_ref:$(binding_value checkpoint_test_path)" | sha256sum | awk '{print $1}')
  [[ "$test_sha" == "$(binding_value checkpoint_test_sha256)" ]] || die "R1 test blob mismatch expected=$(binding_value checkpoint_test_sha256) actual=$test_sha"
  report_sha=$(gitc show "$local_ref:$(binding_value oracle_report_path)" | sha256sum | awk '{print $1}')
  [[ "$report_sha" == "$(binding_value oracle_report_sha256)" ]] || die "R1 oracle blob mismatch expected=$(binding_value oracle_report_sha256) actual=$report_sha"
  if [[ -n "${R1_REPORT_PATH:-}" ]]; then
    [[ "$(sha_file "$R1_REPORT_PATH")" == "$(binding_value report_sha256)" ]] || die "R1 external report mismatch"
    note "r1-report\tPASS\t$(binding_value report_sha256)\t$R1_REPORT_PATH"
  else
    note "r1-report\tEXTERNAL\texpected=$(binding_value report_sha256)\tset R1_REPORT_PATH to verify mounted report"
  fi
  note "r1\tREADY\t$advertised_ref\t$actual\t$tree\tfull-index=$full_index\tpatch=$patch_id"
}

verify_r5() {
  local status blocked_head blocked_tree
  status=$(r5_status)
  blocked_head=$(r5_value blocked_frontier_head)
  blocked_tree=$(r5_value blocked_frontier_tree)
  [[ "$blocked_head" == "$base_head" ]] || die "R5 binding blocked-head mismatch"
  [[ "$blocked_tree" == "$base_tree" ]] || die "R5 binding blocked-tree mismatch"
  if [[ "$status" != ready ]]; then
    [[ "$(r5_value corrected_head)" == "d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768" ]] || die "R5 pending head mismatch"
    [[ "$(r5_value corrected_tree)" == "641654079f60fcd1c9ff9ccbbd06d3edcabe4096" ]] || die "R5 pending tree mismatch"
    [[ "$(r5_value corrected_parent)" == "$base_head" ]] || die "R5 pending parent mismatch"
    [[ "$(r5_value corrected_diff_sha256_prefix)" == "be940f41" ]] || die "R5 source-approved diff prefix mismatch"
    [[ "$(r5_value corrected_patch_id_prefix)" == "1902f4c9" ]] || die "R5 source-approved patch prefix mismatch"
    note "r5\tHOLD\tstatus=$status\tsource-approved=d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768\tapprovals=R2,R4\tawaiting immutable ref/report; no candidate runtime enabled"
    return 0
  fi
  for key in corrected_ref corrected_head corrected_tree corrected_parent corrected_diff_sha256 corrected_report_sha256; do
    local value
    value=$(r5_value "$key")
    [[ -n "$value" && "$value" != UNBOUND ]] || die "R5 binding is ready but $key is unbound"
  done
  local corrected_ref remote_branch local_ref corrected_head corrected_tree actual tree candidate
  corrected_ref=$(r5_value corrected_ref)
  remote_branch=${corrected_ref#origin/}
  local_ref=refs/stage2-acceptance-overlay/r5-corrected
  timeout 20m git -C "$root" fetch --no-tags origin "+refs/heads/$remote_branch:$local_ref" >/dev/null
  corrected_head=$(r5_value corrected_head)
  corrected_tree=$(r5_value corrected_tree)
  actual=$(gitc rev-parse "$local_ref^{commit}")
  tree=$(gitc rev-parse "$local_ref^{tree}")
  [[ "$actual" == "$corrected_head" && "$tree" == "$corrected_tree" ]] || die "R5 corrected identity mismatch"
  candidate=$(gitc rev-parse "$requested_head^{commit}")
  gitc merge-base --is-ancestor "$local_ref" "$candidate" || die "candidate is not descended from R5 corrected frontier"
  note "r5\tREADY\t$corrected_ref\t$actual\t$tree\tcandidate lineage verified"
}

verify_w5() {
  local status ref actual tree
  status=$(awk -F '\t' '$1=="status" {print $2}' "$w5_binding")
  ref=$(awk -F '\t' '$1=="ref" {print $2}' "$w5_binding")
  [[ "$status" == immutable-report-only-no-run-blocked ]] || die "W5/R7 status changed unexpectedly"
  [[ "$ref" == refs/heads/codex/forktree-w5-r7-gc-reachability-oracle ]] || die "W5/R7 ref mismatch"
  [[ "$(awk -F '\t' '$1=="base_head" {print $2}' "$w5_binding")" == "d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768" ]] || die "W5/R7 base mismatch"
  [[ "$(awk -F '\t' '$1=="head" {print $2}' "$w5_binding")" == "6487170dfa11b24411dbbd73e3c003439072df09" ]] || die "W5/R7 head mismatch"
  [[ "$(awk -F '\t' '$1=="tree" {print $2}' "$w5_binding")" == "94eefb7de3260a8c8a3217805a5372cb8670157c" ]] || die "W5/R7 tree mismatch"
  [[ "$(awk -F '\t' '$1=="full_index_diff_sha256" {print $2}' "$w5_binding")" == "b12d49fbb8f991459ca9a9e6513f26f392ce642c9b25e95efc1be44ecb166345" ]] || die "W5/R7 full-index mismatch"
  [[ "$(awk -F '\t' '$1=="patch_id" {print $2}' "$w5_binding")" == "3b8ef7eeec6cb3b6edbc5f5b1d5226f79615a247" ]] || die "W5/R7 patch mismatch"
  local report_path
  report_path=$(awk -F '\t' '$1=="report_path" {print $2}' "$w5_binding")
  if [[ -f "$report_path" ]]; then
    [[ "$(sha_file "$report_path")" == "fd47899844bafc72fb47c254f77c74b91d4d40f43d0bb2a54d043823892b6a35" ]] || die "W5/R7 report hash mismatch"
    note "w5-r7\tPASS\t$report_path\timmutable report verified"
  else
    note "w5-r7\tEXTERNAL\t6487170dfa11b24411dbbd73e3c003439072df09\timmutable no-run-blocked package; report not mounted"
  fi
}

verify_reader_frontier() {
  [[ "$(reader_status)" == blocked-derived-scan-and-legacy-reader ]] || die "reader frontier status changed unexpectedly"
  [[ "$(reader_value approved_base_head)" == "d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768" ]] || die "reader approved-base head mismatch"
  [[ "$(reader_value approved_base_tree)" == "641654079f60fcd1c9ff9ccbbd06d3edcabe4096" ]] || die "reader approved-base tree mismatch"
  [[ "$(reader_value pending_ref)" == UNBOUND ]] || die "reader frontier ref must remain unbound"
  [[ "$(reader_value pending_head)" == "9f3c703e953440cde1d60b1511467c4337648c8f" ]] || die "reader frontier head mismatch"
  [[ "$(reader_value pending_tree)" == "51a0026c0c3eced6fdaa5e5ed4824111377f086c" ]] || die "reader frontier tree mismatch"
  [[ "$(reader_value pending_parent)" == "d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768" ]] || die "reader frontier parent mismatch"
  [[ "$(reader_value pending_diff_sha256_prefix)" == "6000f34f" ]] || die "reader frontier diff prefix mismatch"
  [[ "$(reader_value pending_patch_id_prefix)" == "3890dad2" ]] || die "reader frontier patch prefix mismatch"
  [[ "$(reader_value expected_cargo_errors)" == 185 && "$(reader_value expected_cargo_warnings)" == 7 ]] || die "reader frontier expected compile frontier mismatch"
  [[ "$(reader_value successor_status)" == pending-correction-review ]] || die "reader successor status changed unexpectedly"
  [[ "$(reader_value successor_ref)" == UNBOUND ]] || die "reader successor ref must remain unbound"
  [[ "$(reader_value successor_head)" == "705440f55eccba9e2d55c0951d6a684737005d76" ]] || die "reader successor head mismatch"
  [[ "$(reader_value successor_tree)" == "2b8dcb45a2d06bdda86d0fa5add5ea8c12d18c2d" ]] || die "reader successor tree mismatch"
  [[ "$(reader_value successor_parent)" == "9f3c703e953440cde1d60b1511467c4337648c8f" ]] || die "reader successor parent mismatch"
  [[ "$(reader_value successor_diff_sha256_prefix)" == "c68b9338" ]] || die "reader successor diff prefix mismatch"
  [[ "$(reader_value successor_patch_id_prefix)" == "7504d3c1" ]] || die "reader successor patch prefix mismatch"
  note "reader-frontier\tBLOCKED\t9f3c703e953440cde1d60b1511467c4337648c8f\t51a0026c0c3eced6fdaa5e5ed4824111377f086c\tderived/history empty-success and legacy TrackedHead/control reader\tpending-correction=705440f55eccba9e2d55c0951d6a684737005d76\tlast-approved=d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768\tno runtime enabled"
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
    verify_r5
    verify_w5
    verify_reader_frontier
    note "runtime\tDORMANT\tset RUN_RUNTIME=1 only after explicit compile-green immutable approval"
    ;;
  commands)
    print_commands
    ;;
  materialize)
    verify_identity
    verify_r5
    [[ "$(r5_status)" == ready ]] || die "R5 corrected frontier is not ready; no materialization"
    [[ -z "$(gitc status --porcelain)" ]] || die "candidate checkout is dirty"
    overlay=${OVERLAY_DIR:?set OVERLAY_DIR to a fresh nonexistent directory}
    [[ ! -e "$overlay" ]] || die "OVERLAY_DIR already exists: $overlay"
    gitc worktree add --detach "$overlay" "${requested_head:-HEAD}"
    mkdir -p "$overlay/.stage2-acceptance-overlay"
    cp "$script_dir"/FORKTREE_STAGE2_SEVEN_STAGE_OVERLAY.{md,tsv,sh} "$overlay/.stage2-acceptance-overlay/"
    cp "$binding" "$overlay/.stage2-acceptance-overlay/"
    cp "$r5_binding" "$overlay/.stage2-acceptance-overlay/"
    cp "$w5_binding" "$overlay/.stage2-acceptance-overlay/"
    cp "$reader_binding" "$overlay/.stage2-acceptance-overlay/"
    note "materialized\tPASS\t$overlay\tproduction paths unchanged\truntime dormant"
    ;;
  run)
    [[ "${RUN_RUNTIME:-0}" == 1 ]] || die "runtime disabled; set RUN_RUNTIME=1 explicitly"
    [[ "${COMPILE_GREEN:-0}" == 1 ]] || die "candidate compile-green attestation is required"
    verify_identity
    verify_scope
    [[ "$(r1_status)" == ready ]] || die "R1 immutable checkpoint/GC binding is not ready"
    [[ "$(r5_status)" == ready ]] || die "R5 reviewed correction is not ready"
    [[ "$(reader_status)" == ready ]] || die "reader frontier review/transport is not ready"
    die "execution handoff requires the exact R1 command fields; use commands after binding"
    ;;
  *) die "unknown mode: $mode (verify|commands|materialize|run)" ;;
esac
