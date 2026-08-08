#!/usr/bin/env bash
set -euo pipefail

if test "$#" -lt 1; then
  printf 'source root required\n' >&2
  exit 2
fi
ROOT="$1"
cd "$ROOT"
git merge-base --is-ancestor 1d9c47728377c6ec7d2646704d51f3aadb11c773 HEAD

MODEL=test-reports/tracked-head-corruption-oracle-v3/selector_domain_contract_model.rs
MANIFEST=test-reports/tracked-head-corruption-oracle-v3/MANIFEST.json
test -f "$MODEL"
test -f "$MANIFEST"
rg -n -F "7ff277c297e93eba83da09bf12f83d6485a8458b" "$MANIFEST" >/dev/null
rg -n -F "55d018ea5389898414dbf7844053c5339b316bf36652574b86983c1c8cb43b4b" "$MANIFEST" >/dev/null
for token in \
  GlobalSelector BranchSelector StateRoot CommitCatalog ChangeCatalog CheckpointRoot \
  Malformed Missing WrongKind IdentitySubstitution \
  every_domain_and_corruption_is_one_read_then_zero_durable_work \
  any_domain_replacement_invalidates_the_pinned_view_without_writes \
  retained_reads retained_views selector_rotations
do
  rg -n -F "$token" "$MODEL" >/dev/null
done

changed=$(git diff --name-only 1d9c47728377c6ec7d2646704d51f3aadb11c773..HEAD)
if printf '%s\n' "$changed" | rg -q '^packages/lix/src/'; then
  printf 'RED production source changed\n'
  exit 1
fi
printf 'PASS v3 per-domain source contract\n'
