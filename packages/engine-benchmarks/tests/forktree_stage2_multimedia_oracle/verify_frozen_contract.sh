#!/usr/bin/env bash
set -euo pipefail

package=/root/repos/lix-evidence/forktree-stage2-multimedia-oracle
source_file="$package/stage2_multimedia_acceptance.rs"
manifest="$package/Cargo.toml"

test -f "$source_file"
test -f "$manifest"

required=(
  'lix::storage_bench::forktree'
  'ForkTreeInventory'
  'ForkTreeGcRunSummary'
  'GcBudget::default()'
  'GcTerminalStatus::Complete'
  'inventory(&engine)'
  'run_gc_to_completion(&engine, GcBudget::default())'
  'image-64-1'
  'audio-64-1'
  'archive-512-10'
  'video-512-10'
  'branch_without_edit'
  'post_merge_checkpoint'
  'retained_history_gc'
  'final_reference_gc'
  'cold_reopen_engine'
  'stage2_media_final_reopen'
)
for token in "${required[@]}"; do
  rg -F --quiet "$token" "$source_file" || {
    echo "missing required contract token: $token" >&2
    exit 1
  }
done

forbidden=(
  'use lix::forktree'
  'SpaceId'
  'OBJECT_SPACE'
  'SELECTOR_SPACE'
  'presence_rows'
  'ScanPlan'
  'ScanPlanCursor'
  'resume_after'
  'collect_repository_gc_for_bench'
  'binary_cas_owner_layout_accounting'
  'read_binary_cas_for_bench'
)
for token in "${forbidden[@]}"; do
  if rg -F --quiet "$token" "$source_file"; then
    echo "forbidden authority/compatibility token: $token" >&2
    exit 1
  fi
done

rustfmt --edition 2024 --check "$source_file"
echo "frozen Stage2 multimedia contract: PASS"
