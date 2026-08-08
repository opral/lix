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
  "$SRC/session/context.rs"
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
for file in "${DIRECT[@]}" "$SRC/live_state/context.rs"; do
  all+="$(cat "$file")"
  all+=$'\n'
done

missing=()
for token in "${REQUIRED[@]}"; do
  grep -Fq "$token" <<<"$all" || missing+=("$token")
done

residues=()
for file in "${DIRECT[@]}"; do
  rel=${file#"$ROOT/"}
  while IFS= read -r line; do
    residues+=("$rel:$line")
  done < <(grep -nE \
    'TrackedHeadContext|TrackedStateStoreReader|BranchHeadControl|HotStateTransactionCache|plan_direct_entity_columnar_scan|EntityColumnar(LayoutCache|OverlayRow|WriteSets)|RowGroupManifest|RowGroupSetId|ColumnarRowGroup|columnar_row_group::|sql2::entity_columnar_layout|live_state::entity_columnar|fallback|compatibility|rebuild[[:space:]]*reader' \
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
