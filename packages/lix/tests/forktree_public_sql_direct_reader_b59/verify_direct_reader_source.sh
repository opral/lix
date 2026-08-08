#!/usr/bin/env bash
set -euo pipefail

# Test/report-only future gate. It performs no writes and is not run as part
# of the b59 freeze. It is intentionally path-aware: broad legacy context and
# physical writer/GC owners are reported as deferred, while the SQL reader
# closure is checked strictly.

ROOT=${1:?usage: verify_direct_reader_source.sh CANDIDATE_ROOT [ANCHOR]}
ANCHOR=${2:-b59e1f11a51153e0a787a81f0f25bf104d150aaf}
ROOT=$(cd "$ROOT" && pwd)
SRC="$ROOT/packages/lix/src"

DIRECT=(
  "$SRC/live_state/context.rs"
  "$SRC/live_state/forktree_reader.rs"
  "$SRC/live_state/visibility.rs"
  "$SRC/sql2/entity_batch.rs"
  "$SRC/sql2/providers/entity.rs"
  "$SRC/sql2/entity_projection.rs"
  "$SRC/sql2/catalog/entity_surface.rs"
  "$SRC/entity_pk.rs"
  "$SRC/session/context.rs"
)
OUTER=(
  "$SRC/live_state/visibility.rs"
  "$SRC/sql2/entity_batch.rs"
  "$SRC/sql2/providers/entity.rs"
  "$SRC/sql2/entity_projection.rs"
  "$SRC/sql2/catalog/entity_surface.rs"
  "$SRC/entity_pk.rs"
)
REQUIRED=(
  ForkTreeReadFacade
  scan_forktree_operation
  scan_view
  load_exact_batch
  EntitySnapshotReader
  EntityProjectionDecoder
  EntityPk
  state_point
  state_range
)

# `live_state/context.rs` still contains legacy generation/filesystem helpers
# that belong to separate deletion waves.  The public SQL closure is therefore
# checked by extracting the exact direct methods, rather than by silently
# omitting the file or treating its unrelated legacy implementation as part of
# the direct reader.  A successor must keep these methods free of legacy
# readers, caches, independent snapshot acquisition, and fallback routes.
CONTEXT_DIRECT_SIGNATURES=(
  "pub(crate) async fn scan_batch("
  "pub(crate) async fn load_exact_batch("
  "pub(crate) async fn scan_tracked_batch("
  "async fn scan_forktree_operation("
)
CONTEXT_DIRECT_FORBIDDEN='TrackedHeadContext|TrackedStateStoreReader|BranchHeadControl|HotStateTransactionCache|branch_head_control_cache|filesystem_path_index_cache|load_branch_head_controls|tracked_head|begin_read|fallback[[:space:]]+reader|compatibility[[:space:]]+reader|rebuild[[:space:]]+reader|RowGroup|row_group|columnar'
CALLER_SLICES=(
  "$SRC/sql2/entity_batch.rs|impl<S> EntitySnapshotReader for CurrentEntitySnapshotReader<S>"
  "$SRC/sql2/providers/entity.rs|async fn plan_scan("
  "$SRC/session/context.rs|fn entity_snapshot_reader("
)
CALLER_FORBIDDEN='TrackedHeadContext|TrackedStateStoreReader|BranchHeadControl|HotStateTransactionCache|branch_head_control_cache|filesystem_path_index_cache|load_branch_head_controls|begin_read|open_coherent_view_on_read|fallback[[:space:]]+reader|compatibility[[:space:]]+reader|rebuild[[:space:]]+reader|RowGroup|row_group|columnar'

function_slice() {
  local file=$1
  local signature=$2
  awk -v signature="$signature" '
    BEGIN { found = 0; opens = 0; closes = 0 }
    {
      line = $0
      if (!found && index(line, signature) > 0) found = 1
      if (found) {
        print line
        opens += gsub(/\{/, "", line)
        closes += gsub(/\}/, "", line)
        if (opens > 0 && opens == closes) exit
      }
    }
  ' "$file"
}

if ! git -C "$ROOT" merge-base --is-ancestor "$ANCHOR" HEAD; then
  echo "BLOCKED: candidate is not descended from required anchor $ANCHOR" >&2
  exit 2
fi

for file in "${DIRECT[@]}"; do
  [[ -f "$file" ]] || {
    echo "MISSING direct reader file: ${file#"$ROOT/"}" >&2
    exit 1
  }
done

all=""
for file in "${DIRECT[@]}"; do
  all+="$(cat "$file")"
  all+=$'\n'
done

missing=()
for token in "${REQUIRED[@]}"; do
  grep -Fq "$token" <<<"$all" || missing+=("$token")
done

residues=()
context_file="$SRC/live_state/context.rs"
for signature in "${CONTEXT_DIRECT_SIGNATURES[@]}"; do
  body=$(function_slice "$context_file" "$signature")
  if [[ -z "$body" ]]; then
    missing+=("live_state/context.rs:$signature")
    continue
  fi
  while IFS= read -r line; do
    residues+=("${context_file#"$ROOT/"}:$signature:$line")
  done < <(grep -nE "$CONTEXT_DIRECT_FORBIDDEN" <<<"$body" || true)
done

for descriptor in "${CALLER_SLICES[@]}"; do
  caller_file=${descriptor%%|*}
  caller_signature=${descriptor#*|}
  body=$(function_slice "$caller_file" "$caller_signature")
  if [[ -z "$body" ]]; then
    missing+=("${caller_file#"$ROOT/"}:$caller_signature")
    continue
  fi
  while IFS= read -r line; do
    residues+=("${caller_file#"$ROOT/"}:$caller_signature:$line")
  done < <(grep -nE "$CALLER_FORBIDDEN" <<<"$body" || true)
done

for file in "${DIRECT[@]}"; do
  # The context file is checked above at function scope.  Its legacy helpers
  # are intentionally present for separate waves and must not mask whether
  # the direct SQL methods themselves bypass the ForkTree facade.
  [[ "$file" == "$context_file" ]] && continue
  rel=${file#"$ROOT/"}
  while IFS= read -r line; do
    residues+=("$rel:$line")
  done < <(grep -nE \
    'TrackedHeadContext|TrackedStateStoreReader|BranchHeadControl|HotStateTransactionCache|plan_direct_entity_columnar_scan|EntityColumnar(LayoutCache|OverlayRow|WriteSets)|RowGroupManifest|RowGroupSetId|ColumnarRowGroup|columnar_row_group::|sql2::entity_columnar_layout|live_state::entity_columnar|fallback[[:space:]]+reader|compatibility[[:space:]]+reader|rebuild[[:space:]]*reader' \
    "$file" || true)
done

# The outer SQL layers may not acquire a second snapshot. The one coherent
# acquisition belongs below the direct boundary, not in provider/projection
# helpers.
for file in "${OUTER[@]}"; do
  rel=${file#"$ROOT/"}
  while IFS= read -r line; do
    residues+=("$rel:$line")
  done < <(grep -nE 'begin_read|open_coherent_view_on_read|StorageRead' "$file" || true)
done

deferred=()
for file in \
  "$SRC/live_state/context.rs" \
  "$SRC/live_state/entity_columnar.rs" \
  "$SRC/sql2/entity_columnar_layout.rs" \
  "$SRC/sql2/exec/bound_public_write.rs" \
  "$SRC/gc.rs" \
  "$SRC/session/execute.rs" \
  "$SRC/tracked_state/types.rs"; do
  if [[ -f "$file" ]] && grep -qE 'TrackedHead|BranchHead|EntityColumnar|RowGroup|row_group|columnar' "$file"; then
    deferred+=("${file#"$ROOT/"}")
  fi
done

echo "SQL direct-reader root=$ROOT"
echo "SQL direct-reader anchor=$ANCHOR"
echo "SQL direct-reader missing-required=${#missing[@]}"
printf '  missing=%s\n' "${missing[*]:-none}"
echo "SQL direct-reader forbidden-in-closure=${#residues[@]}"
printf '  residue=%s\n' "${residues[@]:-none}"
echo "SQL deferred-physical-owner-files=${#deferred[@]}"
printf '  deferred=%s\n' "${deferred[*]:-none}"

if ((${#missing[@]} || ${#residues[@]})); then
  echo "RED public SQL direct-reader boundary"
  exit 1
fi
echo "GREEN public SQL direct-reader boundary; deferred physical owners are not serving authority"
