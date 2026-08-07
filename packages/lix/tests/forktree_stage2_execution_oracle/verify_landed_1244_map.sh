#!/usr/bin/env bash
set -euo pipefail

repo=${1:?usage: verify_landed_1244_map.sh E871_REPOSITORY_ROOT [ORACLE_SOURCE]}
oracle=${2:-$repo/packages/lix/tests/forktree_stage2_execution_oracle/main.rs}
base=b5e78190f49cab5de7bb19b6f967706c214363b6
head=e8713ed191e05d29c44dbc8e7ce1d6b1a11695e7

expected=$(mktemp)
actual=$(mktemp)
trap 'rm -f "$expected" "$actual"' EXIT

cat >"$expected" <<'EOF'
Cargo.lock
packages/engine-benchmarks/Cargo.toml
packages/engine-benchmarks/examples/storage_layout.rs
packages/engine-benchmarks/tests/corruption_recovery_qualification.rs
packages/lix/src/branch/control.rs
packages/lix/src/init.rs
packages/lix/src/storage_bench.rs
packages/lix/src/transaction/context.rs
packages/lix/src/transaction/plugin_checkpoint.rs
packages/rs-sdk-tests/tests/e2e.rs
packages/server-protocol/src/lib.rs
EOF

git -C "$repo" diff --name-only "$base..$head" | sort >"$actual"
diff -u "$expected" "$actual"

branch_control="$repo/packages/lix/src/branch/control.rs"
plugin_checkpoint="$repo/packages/lix/src/transaction/plugin_checkpoint.rs"
init="$repo/packages/lix/src/init.rs"
transaction_context="$repo/packages/lix/src/transaction/context.rs"

rg -q 'branch\.head_control\.v10' "$branch_control"
rg -q 'LBC1' "$branch_control"
rg -q 'lix branch-head control v1' "$branch_control"
rg -q 'plugin\.current_checkpoint\.v2' "$plugin_checkpoint"
rg -q 'LPC3' "$plugin_checkpoint"
rg -q 'lix plugin current checkpoint v3' "$plugin_checkpoint"
rg -q 'immutable-physical-commit-state\.v61' "$init"
rg -q '\.await\?' "$transaction_context"

for token in \
  BRANCH_HEAD_CONTROL_NAMESPACE BRANCH_HEAD_CONTROL_MAGIC \
  BRANCH_HEAD_CONTROL_DIGEST_CONTEXT branch.head_control.v10 LBC1 \
  plugin.current_checkpoint.v2 LPC3 'lix plugin current checkpoint v3' \
  immutable-physical-commit-state.v61; do
  rg -Fq "$token" "$oracle"
done

echo 'landed-1244 path/symbol map PASS'
