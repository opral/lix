#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
base="fd2be256d763f17e9f127d4c984e36fba191cb82"
tree="20110ca5e3c33d34217630fff0a2b784b545317a"
mode="${1:-audit}"

[[ "$(git -C "$root" rev-parse "$base^{tree}")" == "$tree" ]] || {
  echo "source gate requires exact fd2 tree $tree" >&2
  exit 2
}

python3 - "$root" "$base" "$tree" "$mode" <<'PY'
import subprocess
import sys

root, base, tree, mode = sys.argv[1:]
package = "test-reports/forktree-stage2-historical-correction-fd2-defects/"
names = subprocess.check_output(
    ["git", "-C", root, "diff", "--name-only", f"{base}..HEAD"], text=True
).splitlines()
if any(not name.startswith(package) for name in names):
    print("scope violation: production or unrelated path changed", file=sys.stderr)
    sys.exit(2)

def source(path):
    return subprocess.check_output(["git", "-C", root, "show", f"HEAD:{path}"], text=True)

def balanced_body(text, name, occurrence=0):
    needle = f"fn {name}"
    starts = []
    cursor = 0
    while True:
        index = text.find(needle, cursor)
        if index < 0:
            break
        starts.append(index)
        cursor = index + len(needle)
    if occurrence >= len(starts):
        raise AssertionError(f"function {name} occurrence {occurrence} not found")
    signature_start = starts[occurrence]
    brace = text.find("{", signature_start)
    if brace < 0:
        raise AssertionError(f"function {name} has no body")
    depth = 0
    i = brace
    state = "normal"
    raw_end = None
    while i < len(text):
        c = text[i]
        n = text[i + 1] if i + 1 < len(text) else ""
        if state == "line":
            if c == "\n":
                state = "normal"
        elif state == "block":
            if c == "/" and n == "*":
                depth += 0
                i += 1
            elif c == "*" and n == "/":
                state = "normal"
                i += 1
        elif state == "string":
            if c == "\\":
                i += 1
            elif c == '"':
                state = "normal"
        elif state == "char":
            if c == "\\":
                i += 1
            elif c == "'":
                state = "normal"
        elif state == "raw":
            if raw_end and text.startswith(raw_end, i):
                i += len(raw_end) - 1
                state = "normal"
        else:
            if c == "/" and n == "/":
                state = "line"
                i += 1
            elif c == "/" and n == "*":
                state = "block"
                i += 1
            elif c == '"':
                state = "string"
            elif c == "'":
                state = "char"
            elif c == "r":
                j = i + 1
                while j < len(text) and text[j] == "#":
                    j += 1
                if j < len(text) and text[j] == '"':
                    hashes = text[i + 1:j]
                    raw_end = '"' + hashes + '"'
                    state = "raw"
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return text[signature_start:i + 1]
        i += 1
    raise AssertionError(f"unbalanced body for {name}")

checkpoint = balanced_body(source("packages/lix/src/session/checkpoint.rs"), "create_checkpoint")
working = balanced_body(source("packages/lix/src/sql2/providers/working_diff.rs"), "plan_scan")
filesystem = balanced_body(source("packages/lix/src/sql2/providers/filesystem_working_diff.rs"), "plan_scan")
filesystem_rows = balanced_body(source("packages/lix/src/sql2/providers/filesystem_working_diff.rs"), "load_rows")
descriptors = balanced_body(source("packages/lix/src/sql2/providers/filesystem_working_diff.rs"), "scan_descriptors")
history = balanced_body(source("packages/lix/src/sql2/providers/file_history.rs"), "load_file_history_rows")
prepare = balanced_body(source("packages/lix/src/sql2/providers/file_history.rs"), "prepare_file_history_rows")

def require(body, text, label):
    if text not in body:
        raise AssertionError(f"missing {label}: {text}")

def forbid(body, text, label):
    if text in body:
        raise AssertionError(f"forbidden {label}: {text}")

for body, label in [(working, "working_diff::plan_scan")]:
    require(body, "forktree_reader", f"retained ForkTree reader in {label}")
    require(body, "latest_checkpoint_for_branch", f"checkpoint lookup in {label}")
    require(body, "diff_state_rows_between_commits", f"ForkTree diff in {label}")
    for forbidden_text in ["ForkTreeReadFacade::new", "TrackedStateContext::diff_commits", "BranchHeadControlContext", "TrackedHeadContext"]:
        forbid(body, forbidden_text, f"legacy fallback in {label}")

require(filesystem, "forktree_reader", "retained ForkTree reader in filesystem_working_diff::plan_scan")
require(filesystem, "latest_checkpoint_for_branch", "checkpoint lookup in filesystem_working_diff::plan_scan")
require(filesystem, "load_rows", "ForkTree filesystem row loader")
require(filesystem_rows, "scan_state_rows_at_commit", "ForkTree state scans in filesystem row loader")
for body, label in [(filesystem, "filesystem_working_diff::plan_scan"), (filesystem_rows, "filesystem_working_diff::load_rows")]:
    for forbidden_text in ["ForkTreeReadFacade::new", "TrackedStateContext::diff_commits", "BranchHeadControlContext", "TrackedHeadContext"]:
        forbid(body, forbidden_text, f"legacy fallback in {label}")

for required in ["branch_ref_reader_on_opening_read", "forktree_read_facade", "checkpoint_history_from_head", "diff_state_rows_between_commits"]:
    require(checkpoint, required, f"fd2 chronology owner {required}")
for forbidden_text in ["BranchHeadControlContext", "TrackedHeadContext"]:
    forbid(checkpoint, forbidden_text, f"checkpoint fallback {forbidden_text}")

def has_tombstone_rejection(body):
    return "if row.deleted" in body and "is tombstoned" in body

def has_projection_gate(body):
    return "if needs_data && prepared_row.descriptor().name.is_some()" in body and "validate_file_history_materialization" in body

def has_unchecked_lookup(body):
    return ".find(|blob| blob.file_id == event.file_id)" in body and "filter(|blob| blob.file_id == event.file_id)" not in body

def has_exact_one_before_projection(body):
    return ("validate_exactly_one_blob_ref" in body or "exactly_one_blob_ref" in body) and "needs_data" in body

def has_tombstone_absence(body):
    return ("row.deleted" in body and "continue" in body) or "logical absence" in body

issues = []
if has_tombstone_rejection(descriptors):
    issues.append("descriptor_tombstone_rejected")
if has_projection_gate(history):
    issues.append("blob_validation_projection_gated")
if has_unchecked_lookup(prepare):
    issues.append("blob_reference_cardinality_unchecked")

if mode == "audit":
    if not issues[:2] == ["descriptor_tombstone_rejected", "blob_validation_projection_gated"]:
        print("fd2 baseline calibration changed: expected first two defects are absent or reordered", file=sys.stderr)
        print("issues:", issues, file=sys.stderr)
        sys.exit(1)
    print("STATUS=BLOCKED_EXPECTED_RED")
    print("DEFECT=descriptor_tombstone_rejected")
    print("DEFECT=blob_validation_projection_gated")
    print("RELATED=blob_reference_cardinality_unchecked" if "blob_reference_cardinality_unchecked" in issues else "RELATED=not-observed")
    print("PRESERVED=one-retained-ForkTree-history-view-and-fail-closed-chronology")
    sys.exit(1)

if mode != "corrected":
    print(f"unknown mode: {mode}", file=sys.stderr)
    sys.exit(2)
if issues:
    print("corrected candidate still has structural defects:", ",".join(issues), file=sys.stderr)
    sys.exit(1)
if not has_tombstone_absence(descriptors):
    print("corrected candidate has no structural tombstone-to-absence handling", file=sys.stderr)
    sys.exit(1)
if not has_exact_one_before_projection(history):
    print("corrected candidate has no structural pre-projection exact-one BlobRef check", file=sys.stderr)
    sys.exit(1)
print("STATUS=CORRECTED_STRUCTURAL_GREEN")
print("CHECK=descriptor_tombstone_to_logical_absence")
print("CHECK=exact_one_blobref_and_payload_before_projection")
print("CHECK=metadata_projection_does_not_bypass_authentication")
PY
