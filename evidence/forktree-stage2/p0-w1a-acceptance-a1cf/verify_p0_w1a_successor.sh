#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 CHECKOUT IMMUTABLE_HEAD" >&2
  exit 64
fi

checkout=$1
expected_head=$2
baseline=a1cf8f7fd55ac21ef7e5bfe7f385c49d99140737
baseline_tree=d8326da2b1d38bd51b8ac7229d00684a6865bce2
baseline_checkout=${BASELINE_CHECKOUT:-/root/repos/lix-stage2-production-review-a1cf}
evidence_root=/root/repos/lix-evidence/stage2-production-review-a12b
atomic_verifier="$evidence_root/atomic-writer-frontier-54e90/verify_atomic_writer_successor.sh"
oracle="$evidence_root/deletion/residue-oracle"
expected_oracle_source=f71e91fcbccbb7d6df676a95e9d747725856b77f7e3177ec42f12ca8b28736cc
expected_baseline_output=3891a48613e5d6ebd3d0ab2780aed13c6dd0236f1c2ff343320dd73fb2158a0d

die() {
  printf 'BLOCKER: %s\n' "$*" >&2
  exit 1
}

[[ $(git -C "$checkout" rev-parse HEAD) == "$expected_head" ]] || die "candidate HEAD mismatch"
[[ -z $(git -C "$checkout" status --porcelain=v1) ]] || die "candidate worktree is dirty"
git -C "$checkout" merge-base --is-ancestor "$baseline" "$expected_head" ||
  die "candidate is not an a1cf descendant"
[[ $(git -C "$baseline_checkout" rev-parse HEAD) == "$baseline" ]] ||
  die "baseline checkout HEAD mismatch"
[[ $(git -C "$baseline_checkout" rev-parse HEAD^{tree}) == "$baseline_tree" ]] ||
  die "baseline checkout tree mismatch"
[[ -z $(git -C "$baseline_checkout" status --porcelain=v1) ]] ||
  die "baseline checkout is dirty"

"$atomic_verifier" "$checkout" "$expected_head"

publication="$checkout/packages/lix/src/forktree/publication.rs"
forktree="$checkout/packages/lix/src/forktree"
commit="$checkout/packages/lix/src/transaction/commit.rs"
context="$checkout/packages/lix/src/transaction/context.rs"

if rg -n 'PreparedPublication::commit|publication\.commit\(|publish\.commit\(|stale_publish\.commit\(' "$forktree" >/dev/null; then
  rg -n 'PreparedPublication::commit|publication\.commit\(|publish\.commit\(|stale_publish\.commit\(' "$forktree" >&2 || true
  die "PreparedPublication direct commit remains nameable"
fi
if rg -n '\.begin_write\(' "$forktree/tests.rs" | rg -v 'self\.inner\.begin_write' >/dev/null; then
  rg -n '\.begin_write\(' "$forktree/tests.rs" | rg -v 'self\.inner\.begin_write' >&2 || true
  die "ForkTree fixtures bypass the transaction-owned test publication seam"
fi
if rg -n '\.commit\(&(storage|inverse)\)|\.commit\(storage\)' "$forktree/tests.rs" >/dev/null; then
  rg -n '\.commit\(&(storage|inverse)\)|\.commit\(storage\)' "$forktree/tests.rs" >&2 || true
  die "ForkTree fixtures retain direct publication commit calls"
fi
if rg -n 'pub\(crate\)[[:space:]]+async[[:space:]]+fn[[:space:]]+commit<|pub[[:space:]]+async[[:space:]]+fn[[:space:]]+commit<' "$publication" >/dev/null; then
  die "PreparedPublication retains a direct commit method"
fi
if rg -n '\.begin_write\(' "$forktree" --glob '!tests.rs' >/dev/null; then
  rg -n '\.begin_write\(' "$forktree" --glob '!tests.rs' >&2 || true
  die "ForkTree production retains direct begin_write"
fi
if rg -n '\bwrite\.commit\(\)|\bwrites\.commit\(|commit_write_set|prepare_write_set' "$forktree" --glob '!tests.rs' >/dev/null; then
  rg -n '\bwrite\.commit\(\)|\bwrites\.commit\(|commit_write_set|prepare_write_set' "$forktree" --glob '!tests.rs' >&2 || true
  die "ForkTree production retains a direct commit/prepare seam"
fi

[[ $(rg -n 'prepared_forktree_plan\.into_storage_plan\(\)' "$context" | wc -l) -eq 1 ]] ||
  die "transaction must consume exactly one ForkTree storage plan"
[[ $(rg -n '\.prepare_write_set\(' "$context" | wc -l) -eq 1 ]] ||
  die "transaction must prepare exactly one write set"
[[ $(rg -n 'prepared_commit\.commit\(\)' "$context" | wc -l) -eq 1 ]] ||
  die "transaction must own exactly one prepared backend commit"

python3 - "$commit" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
source = path.read_text()

def function_body(name: str) -> str:
    marker = f"fn {name}"
    start = source.find(marker)
    if start < 0:
        raise SystemExit(f"BLOCKER: {name} is absent")
    brace = source.find("{", start)
    if brace < 0:
        raise SystemExit(f"BLOCKER: {name} has no body")
    depth = 0
    for index in range(brace, len(source)):
        char = source[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[brace:index + 1]
    raise SystemExit(f"BLOCKER: {name} body is unterminated")

reject = function_body("reject_not_yet_lowered_cohorts")
for admitted in ("intermediate_commits", "first_commit_parent_override_by_branch"):
    if admitted in reject:
        raise SystemExit(f"BLOCKER: W1a admitted cohort {admitted} is still rejected")
for deferred in ("file_content_writes", "checkpoint_publications"):
    if deferred not in reject:
        raise SystemExit(f"BLOCKER: deferred cohort {deferred} lost its fail-closed classifier")

if "selected historical members require" in source:
    raise SystemExit("BLOCKER: selected historical members remain rejected")
if "const _: Option<StagedIntermediateCommit>" in source:
    raise SystemExit("BLOCKER: intermediate commits remain only a compiler placeholder")
for required in (
    "intermediate_commits",
    "first_commit_parent_override_by_branch",
    "into_selected_change_batches",
    "source_commit_id",
):
    if required not in source:
        raise SystemExit(f"BLOCKER: W1a lowering lacks {required}")

classify = function_body("classify_publication_intent")
if "file_content_writes" not in reject or "checkpoint_publications" not in reject:
    raise SystemExit("BLOCKER: unsupported cohorts are not classified before publication")
PY

if git -C "$checkout" diff --name-only "$baseline..$expected_head" |
  rg '(^|/)(sql2|storage|storage_adapter|binary_cas|live_state|tracked_state|changelog)(/|\.rs$)' >/dev/null; then
  git -C "$checkout" diff --name-only "$baseline..$expected_head" |
    rg '(^|/)(sql2|storage|storage_adapter|binary_cas|live_state|tracked_state|changelog)(/|\.rs$)' >&2 || true
  die "P0/W1a successor changes an excluded production owner"
fi

oracle_source=$(git -C "$checkout" show 1dbbf3d206540d36f5912eab8372a42819778b47:packages/lix/tests/forktree_stage2_execution_oracle/main.rs | sha256sum | cut -d' ' -f1)
[[ "$oracle_source" == "$expected_oracle_source" ]] || die "canonical 1dbbf source hash mismatch"
"$oracle" self-test >/dev/null

baseline_out=$(mktemp)
candidate_out=$(mktemp)
trap 'rm -f "$baseline_out" "$candidate_out"' EXIT
set +e
"$oracle" audit "$baseline_checkout" >"$baseline_out" 2>&1
baseline_status=$?
"$oracle" audit "$checkout" >"$candidate_out" 2>&1
candidate_status=$?
set -e
[[ $baseline_status -eq 1 ]] || die "canonical baseline audit status $baseline_status != 1"
[[ $candidate_status -eq 0 || $candidate_status -eq 1 ]] || die "candidate audit status is invalid"
[[ $(sha256sum "$baseline_out" | cut -d' ' -f1) == "$expected_baseline_output" ]] ||
  die "canonical a1cf scanner output hash mismatch"
[[ $(sed -n 's/^finding_count=//p' "$baseline_out") == 166 ]] ||
  die "canonical a1cf scanner count mismatch"

python3 - "$baseline_out" "$candidate_out" <<'PY'
import pathlib
import sys

def rows(path):
    values = {}
    count = None
    for line in pathlib.Path(path).read_text().splitlines():
        if line.startswith("finding_count="):
            count = int(line.split("=", 1)[1])
            continue
        parts = line.split("\t")
        if len(parts) == 3 and parts[2].isdigit():
            values[(parts[0], parts[1])] = int(parts[2])
    return values, count

baseline, baseline_count = rows(sys.argv[1])
candidate, candidate_count = rows(sys.argv[2])
if baseline_count != 166:
    raise SystemExit("BLOCKER: parsed baseline count is not 166")
if candidate_count is None:
    raise SystemExit("BLOCKER: candidate scanner omitted finding_count")
if candidate_count > baseline_count:
    raise SystemExit(f"BLOCKER: candidate residue {candidate_count} exceeds {baseline_count}")
for key, value in candidate.items():
    if key not in baseline:
        raise SystemExit(f"BLOCKER: candidate adds scanner key {key}")
    if value > baseline[key]:
        raise SystemExit(
            f"BLOCKER: candidate increases {key} from {baseline[key]} to {value}"
        )
PY

git -C "$checkout" diff --check "$baseline..$expected_head"

echo "PASS: immutable P0/W1a successor clears compiler/direct-write/history/scanner gates"
echo "HEAD=$expected_head"
echo "TREE=$(git -C "$checkout" rev-parse HEAD^{tree})"
echo "BASE_DIFF_SHA256=$(git -C "$checkout" diff --full-index --binary "$baseline..$expected_head" | sha256sum | cut -d' ' -f1)"
echo "RESIDUE_COUNT=$(sed -n 's/^finding_count=//p' "$candidate_out")"
echo "RESIDUE_SHA256=$(sha256sum "$candidate_out" | cut -d' ' -f1)"
