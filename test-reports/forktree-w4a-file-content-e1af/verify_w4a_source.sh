#!/usr/bin/env bash
set -Eeuo pipefail

root=${1:?usage: verify_w4a_source.sh SOURCE_ROOT EXPECTED_SHA}
expected=${2:?usage: verify_w4a_source.sh SOURCE_ROOT EXPECTED_SHA}
cd "$root"

actual=$(git rev-parse HEAD)
if [[ "$actual" != "$expected" ]]; then
  echo "SOURCE-IDENTITY-BLOCKER expected=$expected actual=$actual"
  exit 2
fi

red=0
commit_block=$(sed -n '1476,1690p' packages/lix/src/transaction/context.rs)

if grep -Fq 'file payload publication requires the ForkTree receipt/manifest lowering slice' \
  packages/lix/src/transaction/commit.rs; then
  echo 'RED-01 file_content_writes is still rejected before ForkTree lowering'
  red=1
else
  echo 'PASS-01 file_content_writes rejection removed'
fi

if [[ $(grep -Fc 'begin_read(' <<<"$commit_block") -eq 1 ]]; then
  echo 'PASS-02 commit_prepared has one direct coherent-read acquisition'
else
  echo 'RED-02 commit_prepared read count is not exactly one'
  red=1
fi

if [[ $(grep -Fc 'prepare_write_set(' <<<"$commit_block") -eq 1 && \
      $(grep -Fc 'prepared_commit.commit()' <<<"$commit_block") -eq 1 ]]; then
  echo 'PASS-03 commit_prepared has one prepare and one backend commit'
else
  echo 'RED-03 commit_prepared does not expose one prepare/one commit'
  red=1
fi

if rg -n --fixed-strings 'PreparedPublication::commit' packages/lix/src >/tmp/w4a-prepared-commit.$$ 2>/dev/null; then
  echo 'RED-04 direct PreparedPublication commit residue:'
  cat /tmp/w4a-prepared-commit.$$
  red=1
else
  echo 'PASS-04 no direct PreparedPublication commit symbol'
fi
rm -f /tmp/w4a-prepared-commit.$$

# These are deliberately deferred, not silently accepted: multipart lifecycle
# is outside W4a and keeps its old bridge until its own typed wave lands.
if grep -Fq 'stage_atomic_cas_publication' packages/lix/src/transaction/context.rs && \
   grep -Fq 'execute_fast_lix_file_prepared_path_write' packages/lix/src/sql2/providers/file.rs; then
  echo 'DEFERRED-01 multipart direct-CAS bridge remains outside W4a scope'
else
  echo 'RED-05 deferred multipart bridge was unexpectedly absent or renamed'
  red=1
fi

if grep -Fq 'binary_cas::kv' packages/lix/src/binary_cas/context.rs; then
  echo 'RED-06 stale Binary CAS KV owner references remain on e1af'
  red=1
else
  echo 'PASS-05 no binary_cas::kv reference in context.rs'
fi

echo "W4A-SOURCE-RESULT=$([[ $red -eq 0 ]] && echo GREEN || echo RED)"
exit "$red"
