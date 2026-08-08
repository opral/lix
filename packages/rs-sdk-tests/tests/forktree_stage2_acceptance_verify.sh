#!/usr/bin/env bash
set -euo pipefail

# Read-only acceptance-artifact provenance verifier.
# It fetches only immutable test/report refs into a private ref namespace,
# verifies exact commits/trees/diffs/files, and never checks out or applies
# them to a candidate.

repo=${1:-.}
git_cmd=(git -C "$repo")

fail() {
  printf 'FAIL\t%s\n' "$*" >&2
  exit 1
}

sha_stdin() {
  sha256sum | awk '{print $1}'
}

verify_ref() {
  local id=$1
  local remote_branch=$2
  local expected_head=$3
  local expected_tree=$4
  local base=$5
  local expected_diff=$6
  local attr_mode=${7:-worktree}
  local expected_patch_id=${8:--}
  local expected_bytes=${9:-0}
  local local_ref="refs/stage2-acceptance-verifier/$id"

  timeout 20m "${git_cmd[@]}" fetch --no-tags origin "+refs/heads/$remote_branch:$local_ref" >/dev/null

  local head tree diff
  head=$("${git_cmd[@]}" rev-parse "$local_ref^{commit}")
  [[ "$head" == "$expected_head" ]] ||
    fail "$id head expected=$expected_head actual=$head"
  tree=$("${git_cmd[@]}" rev-parse "$head^{tree}")
  [[ "$tree" == "$expected_tree" ]] ||
    fail "$id tree expected=$expected_tree actual=$tree"
  "${git_cmd[@]}" cat-file -e "$base^{commit}"
  if [[ "$attr_mode" == "head" ]]; then
    diff=$(GIT_ATTR_SOURCE="$local_ref" "${git_cmd[@]}" -c core.abbrev=40 -c diff.noprefix=false diff --binary --full-index --no-ext-diff "$base..$head" | sha_stdin)
  else
    diff=$("${git_cmd[@]}" diff --binary --full-index "$base..$head" | sha_stdin)
  fi
  [[ "$diff" == "$expected_diff" ]] ||
    fail "$id diff expected=$expected_diff actual=$diff"
  printf 'ref\t%s\tPASS\t%s\t%s\t%s\n' "$id" "$head" "$tree" "$diff"

  if [[ "$expected_patch_id" != "-" ]]; then
    local patch_id stream_bytes
    patch_id=$(GIT_ATTR_SOURCE="$local_ref" "${git_cmd[@]}" -c core.abbrev=40 -c diff.noprefix=false diff --binary --full-index --no-ext-diff "$base..$head" | git patch-id --stable | awk '{print $1}')
    [[ "$patch_id" == "$expected_patch_id" ]] ||
      fail "$id patch-id expected=$expected_patch_id actual=$patch_id"
    stream_bytes=$(GIT_ATTR_SOURCE="$local_ref" "${git_cmd[@]}" -c core.abbrev=40 -c diff.noprefix=false diff --binary --full-index --no-ext-diff "$base..$head" | wc -c | tr -d ' ')
    [[ "$stream_bytes" == "$expected_bytes" ]] ||
      fail "$id stream-bytes expected=$expected_bytes actual=$stream_bytes"
    printf 'patch\t%s\tPASS\t%s\thead-attributes\t%s bytes\n' "$id" "$patch_id" "$stream_bytes"
  fi
}

verify_file() {
  local id=$1
  local path=$2
  local expected=$3
  local ref="refs/stage2-acceptance-verifier/$id"
  local actual
  actual=$("${git_cmd[@]}" show "$ref:$path" | sha_stdin)
  [[ "$actual" == "$expected" ]] ||
    fail "$id file=$path expected=$expected actual=$actual"
  printf 'file\t%s\tPASS\t%s\t%s\t-\n' "$id" "$path" "$actual"
}

verify_readiness_ref() {
  local id=$1
  local remote_branch=$2
  local expected_head=$3
  local expected_tree=$4
  local expected_parent=$5
  local expected_focused_diff=$6
  local lineage_base=$7
  local expected_lineage_diff=$8
  local local_ref="refs/stage2-acceptance-verifier/readiness-$id"

  timeout 20m "${git_cmd[@]}" fetch --no-tags origin "+refs/heads/$remote_branch:$local_ref" >/dev/null

  local head tree parent focused_diff lineage_diff
  head=$("${git_cmd[@]}" rev-parse "$local_ref^{commit}")
  [[ "$head" == "$expected_head" ]] ||
    fail "$id readiness head expected=$expected_head actual=$head"
  tree=$("${git_cmd[@]}" rev-parse "$head^{tree}")
  [[ "$tree" == "$expected_tree" ]] ||
    fail "$id readiness tree expected=$expected_tree actual=$tree"
  parent=$("${git_cmd[@]}" rev-parse "$head^")
  [[ "$parent" == "$expected_parent" ]] ||
    fail "$id readiness parent expected=$expected_parent actual=$parent"
  focused_diff=$("${git_cmd[@]}" diff --binary --full-index "$parent..$head" | sha_stdin)
  [[ "$focused_diff" == "$expected_focused_diff" ]] ||
    fail "$id readiness diff expected=$expected_focused_diff actual=$focused_diff"
  lineage_diff=$("${git_cmd[@]}" diff --binary --full-index "$lineage_base..$head" | sha_stdin)
  [[ "$lineage_diff" == "$expected_lineage_diff" ]] ||
    fail "$id lineage diff expected=$expected_lineage_diff actual=$lineage_diff"
  printf 'readiness\t%s\tPASS\t%s\t%s\t%s\n' "$id" "$head" "$tree" "$focused_diff"
  printf 'lineage\t%s\tPASS\t%s\t%s\t%s\n' "$id" "$parent" "$lineage_base" "$lineage_diff"
}

printf 'kind\tid\tstatus\tidentity_or_path\ttree_or_sha\tdiff_sha256\n'

verify_ref sql agent/forktree-stage2-sql-dml-oracle cb834007768205d5e9fb83919ca2915c77acca2d 8826a0a404a39bf4f932ad5140e0dfd1657f48fb a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3 be976527a15ec049be6465c3cf91020b3f58d0788792d7a5f0b1e00165a8b8ff
verify_ref vc agent/forktree-stage2-version-control-oracle 3cb6aa56804642efbe703f5e36bdc1788b51a4e7 911e0d6138b760a1c63e0e2c16b00e8f4b95c7dd a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3 9348633179a5991dacf6bba85510e4f0cb1d391eeaae0042ab1956a0b08348b4
verify_ref checkpoint codex/checkpoint-stage2-acceptance-oracle 9bace2186664fc77877aa24abae6e516855313a1 e006aa4a5a3c6443e13d2c746fe81d9f97c30761 c3a58cc293c8a2df052bc590886ea040f98aa3fb 7525ac6d2dd2b11e7b69709c341fe14a8bfc1b6bbfb525abe995a398e3ef8841
verify_ref delete65 agent/forktree-bc823-oltp-delete-repro 9713361663df727af88dcf88aa05bd4b998c4149 a1b1ef1bed7f2a48b9f11a1a6288f325b3f64590 bc82385ec42b1789018fbd1213f637c19104a02c 6d633a6d61b33700f12b05b5f38486a16941eb556c40bba7a5e3c42004ebf065
verify_ref pointread agent/stage2-point-read-oracle-a12 33117e1c128b038a5bbe486db126b3cb303c0f20 098afc641a8219390dd16e17b99598c928af0760 a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3 255127d4faf169f0127c7c254dc6df33c5be20959f4bfc17655d9bfe6a432b55
verify_ref residue codex/forktree-stage2-deletion-oracle-v2 1dbbf3d206540d36f5912eab8372a42819778b47 7fe3b3c83133344dff4025b558dbdd63bb1be21f d00584e845fed69422282e164387275267a77018 0a6edac94dd03cd287e134bd873962bc841c0d2d5aebb9f92b1de45d5e359da5
verify_ref nolease codex/stage2-no-lease-read-view-boundary-ee402 89c73a24b97ce8dedee5e6c9a85e67c481b29090 6b90abcc440a3c13a6e95c641426629593536012 138b55e1de90806c380ad27b2b349f4c66a1387f e93a1d78d01f3c7d29d4038627c691047fa8953c55bd61e77c6351e714114796
verify_ref gc codex/stage2-gc-publication-acceptance-oracle 0b4e5042b6a79b8be80dbfe4e4cdbff3b28d9a9c 0ac1ab8e74b85a92a8044cb4280adf8cf66ba387 cbe48835f6f07a21e0babf1ba16652a0c6b8a214 bb6a70454484b9bba9e29929656a205a0706d1a0a2e60e495ea52fc19e567224
verify_ref olap codex/forktree-stage2-olap-acceptance-oracle-a12 b9055810dff42c9eb2a096a83ab2207024dce1c6 231939d8a8d0f2a46803264184eea8171fa05f90 a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3 545ff4e7c74b6bc19223d4977fd4dbba11914d46e3e5efadcae8407741e44b42 head f88e0ea04bef0e93b0280c19f7c89d59c678223e 42863
verify_ref multimedia codex/forktree-stage2-multimedia-oracle-a12 61fc367988190b3438672743331a81d83d450fae 1600e8ce54d9f52f6ee3546068362ae298d4d243 a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3 65cda6ee906b6986bf70b636dfaadda5f8f89a2f8f4af407852687c474472660

verify_readiness_ref topology codex/forktree-stage2-milestone3e-topology-owned-reader af7899f41c489fe763ce1a64c5468083570979e2 da097bd739b50629ea39b155d4fa9efc870654e0 2e0cea1b91558179e6ed90847bc8b04b23de246f 942d05f6c92f89e6c32c3b706c82c4e506e498263b5798c92eb2af607a219587 a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3 734d02bfe332e4f8384301de243d85248b639aec0edeffac48b3f56a4ec271e5

verify_file sql packages/rs-sdk-tests/tests/forktree_stage2_sql_dml.rs b410b717f45d68e928e93dcf1332de2895db0246202e9ba9a6e5bc10b416c6bb
verify_file sql packages/rs-sdk-tests/tests/FORKTREE_STAGE2_SQL_DML_ORACLE.md 1867643051628903232c3cbe8f4ae2c1e2655b7cbb0b044ec1046acf35947e22
verify_file vc packages/rs-sdk-tests/tests/forktree_stage2_version_control.rs c120b5ca3ab20acb67dd472791e281af03bffb39e280a5b4d28212337e42a6f9
verify_file vc packages/rs-sdk-tests/tests/FORKTREE_STAGE2_VERSION_CONTROL_ORACLE.md f71d3db5d1aeb62407b38deb63a4e8894448782b329ad807f12add1db8ab7117
verify_file checkpoint packages/rs-sdk-tests/tests/forktree_stage2_checkpoint_rotation.rs ffd47152ea7ed763e893baca8a00c2fadf50c1f38fc9aba4f32433695115f5d5
verify_file checkpoint packages/rs-sdk-tests/tests/FORKTREE_STAGE2_CHECKPOINT_ROTATION_ORACLE.md 03f5312436f84d0169e5eadbccd9fef7c7f1ce24f8358b3041c6f02ca57ee66a
verify_file delete65 packages/engine-benchmarks/benches/forktree_delete_repro.rs 652abfe9dde5a1ff09b45b63c59a5efc6f9e53a7b5ac280a14f0161328c6f533
verify_file delete65 packages/engine-benchmarks/benches/forktree_replacement/model.rs 818818f673249bf50fb623199e3c8884985146683c41344ec8a2cf74a6d070ea
verify_file pointread packages/engine-benchmarks/benches/stage2_point_read_oracle.rs 8e9f6aa4e1e3d085c7b1bf6315ff0d8aa94912d5266133594a97961ee2d3e756
verify_file pointread packages/engine-benchmarks/tests/stage2_point_read_source_gate.sh af8897916e9d3dacbd54f3e48245766d163804b6a9004afa9905b8bb01eb5033
verify_file residue packages/lix/tests/FORKTREE_STAGE2_AUTHORITY_DELETION_BUDGET.md e070e8c53d9cd58dc425ba61cfbfbdd79b373ca8519b852ffdcaf9cce1ab5dec
verify_file residue packages/lix/tests/forktree_stage2_execution_oracle/main.rs f71e91fcbccbb7d6df676a95e9d747725856b77f7e3177ec42f12ca8b28736cc
verify_file residue packages/lix/tests/forktree_stage2_execution_oracle/INVOCATIONS.txt 607d94cd0da9ef998b87a16f63624a0964541ad5d0f50a6602aedd625733ce5f
verify_file residue packages/cli/tests/forktree_stage2_cli_storage_routing.rs 8fb6b08de60ec874731f72c0766efcce33c979d79b918618a56699c0d80c0327
verify_file nolease packages/engine-benchmarks/benches/FORKTREE_STAGE2_RECOVERY_NO_LEASE_REPORT.md 04b42313ff4ff561d2456f53c30dbae3de2d97c9cbf6b2366893f1b8e511e60d
verify_file nolease packages/engine-benchmarks/benches/forktree_stage2_recovery_no_lease.rs df365937ab40bf13d44c8f304257e11b87cb18cf9650d5ebb8a31ff224121059
verify_file nolease packages/engine-benchmarks/tests/forktree_stage2_recovery_no_lease_adversarial.rs 8972c5f7dfef8bf033bee782c4c9fb8a26acad2ab9699ddc29e9c85cdac560d0
verify_file gc packages/engine-benchmarks/tests/FORKTREE_STAGE2_GC_PUBLICATION_ACCEPTANCE.md 6373db1e21d7c4e74f6fcff4329b9fcbaf93ca36681e8b04c47b5da80ffc4403
verify_file gc packages/engine-benchmarks/tests/forktree_stage2_gc_publication_acceptance.rs a43980d3de613d5800478e6c7e8a12c73a4d1833f53ec213f3fb26f317aec1c7
verify_file olap packages/engine-benchmarks/tests/forktree_stage2_olap_acceptance/REPORT.md 005646c33cda54a363ab3c81f0b7ed0a5891e24de20639f05d8ca00a0f66009f
verify_file olap packages/engine-benchmarks/tests/forktree_stage2_olap_acceptance/SPI.md c54c50ee301338ef3c04a3364eaff06e075a3f0212099cbc9a4290e8c5197193
verify_file olap packages/engine-benchmarks/tests/forktree_stage2_olap_acceptance/HASHES.sha256 8e8b616f39bd37fc7eb3dda7aa19da7ce2011c16561c1687caa2cb6942d1a7c2
verify_file multimedia packages/engine-benchmarks/tests/forktree_stage2_multimedia_oracle/stage2_multimedia_acceptance.rs cc0d3cfb14b562b7821ca124c67cbb8ead0da7287f9e0125ba39738304a4a09e
verify_file multimedia packages/engine-benchmarks/tests/forktree_stage2_multimedia_oracle/REPORT.md 0dd241e1d6bd8fa32d84751972bd96fed666f2dafe742b447ca496f06aadc5bb
verify_file multimedia packages/engine-benchmarks/tests/forktree_stage2_multimedia_oracle/COMPILE_RUN_MANIFEST.md 2463197613022e9321e24f09f12211faae3cc85c5cb156ee6c9c7bb1667f2b4d
verify_file multimedia packages/engine-benchmarks/tests/forktree_stage2_multimedia_oracle/EXPECTED_GATES.md bac266cdcb648b4df04cff18c088a5b757455bd8aa41abcd207c5e0134398fc4
verify_file multimedia packages/engine-benchmarks/tests/forktree_stage2_multimedia_oracle/SHA256SUMS ccc755a1cc70a28bc08145aeb61bec940f3db9b01b49c7e89931c9bd9218d0e8

printf 'external\tdelete65\tNOTE\tFORKTREE_DELETE_BLOCKER.md\tac5bb9a01d0e696aac3ee13ca16f92fbb6f9baaad1459b0c34f37352d76aba2c\tnot embedded in ref\n'
printf 'external\tpointread\tNOTE\tREPORT.md\tb86db402ec0bf9b25ca619564edb82a420fcdb34f68de2f972f6c774e863dbf5\tnot embedded in ref; not mounted on reviewer host\n'
printf 'external\tpointread\tNOTE\tSHA256SUMS\tcacbcee8f7b80a627f96dd8b7d6d55beef0fe2a2f7228f0290b488ae7717a888\tnot embedded in ref; author reports 3/3 verify\n'
printf 'external\tpointread\tNOTE\tstage2_point_read_oracle-025c3b394ec8760c\tead3dae2ad74b349ef116b1e3ff9265a20a09f90f24dbdb2e32542c4cd5c8c1a\tbench binary; rebuild on candidate\n'
printf 'external\tolap\tNOTE\tprovenance-report\te78821631888e8a8810df78e9bdffbe31c8a8124227c5ad0c3b549a6e60795a4\tnot embedded in ref\n'
printf 'summary\tall\tPASS\t10 acceptance refs; 1 readiness milestone\t27 embedded files\tno artifacts applied\n'
