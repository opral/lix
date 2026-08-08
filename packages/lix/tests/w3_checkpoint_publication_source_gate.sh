#!/usr/bin/env bash
set -euo pipefail

# Test/report-only W3 source gate.  It is deliberately usable on the
# non-runnable frontier: the exact frontier must pass --expect-red, while a
# future W3 candidate must pass --expect-green.

mode="${1:---expect-red}"
root="$(git rev-parse --show-toplevel)"
cd "$root"

expected_frontier="1f742a382c755399b8a49ab536c4f6dc55fffdd8"
head="$(git rev-parse HEAD)"
if [[ "$head" != "$expected_frontier" ]]; then
  echo "W3 source gate requires exact frontier $expected_frontier (got $head)" >&2
  exit 2
fi

git diff --check

production='packages/lix/src'
if rg -n 'PreparedPublication::commit' "$production"; then
  echo "forbidden independent PreparedPublication::commit production seam" >&2
  exit 1
fi

if rg -n --glob '!**/tests.rs' 'commit_publication_for_test' "$production"; then
  echo "test-only publication helper leaked into production" >&2
  exit 1
fi

if [[ "$mode" == "--expect-red" ]]; then
  rg -n 'checkpoint publication requires the ForkTree snapshot-root lowering slice' \
    packages/lix/src/transaction/commit.rs >/dev/null
  rg -n 'if !prepared\.checkpoint_publications\.is_empty\(\)' \
    packages/lix/src/transaction/commit.rs >/dev/null
  echo "RED CONTROL: checkpoint_publications is still rejected before publication planning"
elif [[ "$mode" == "--expect-green" ]]; then
  if rg -n 'checkpoint publication requires the ForkTree snapshot-root lowering slice' \
      packages/lix/src/transaction/commit.rs; then
    echo "W3 candidate still carries the old checkpoint-publications rejection" >&2
    exit 1
  fi
  echo "GREEN CONTROL: old checkpoint_publications rejection is absent"
else
  echo "usage: $0 [--expect-red|--expect-green]" >&2
  exit 2
fi

checkpoint_count="$(rg -o 'checkpoint_publications' packages/lix/src | wc -l | tr -d ' ')"
recovery_count="$(rg -o 'CheckpointRecoveryRef' packages/lix/src | wc -l | tr -d ' ')"
plan_count="$(rg -o 'into_storage_plan' packages/lix/src | wc -l | tr -d ' ')"
printf 'frontier=%s checkpoint_publications=%s recovery_refs=%s into_storage_plan=%s\n' \
  "$head" "$checkpoint_count" "$recovery_count" "$plan_count"
echo "W3 source gate PASS ($mode)"
