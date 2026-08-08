#!/usr/bin/env python3
"""Function-scoped source RED gate for the immutable b484 control."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

HEAD = "b484e20d845aee3f8137bfa3496f9b3cd0e8cd35"
PARENT = "fd2be256d763f17e9f127d4c984e36fba191cb82"
TREE = "4477c83b246bddac09cd972564bd4ccd67f90f7b"
PATHS = {
    "packages/lix/src/sql2/providers/file_history.rs",
    "packages/lix/src/sql2/providers/filesystem_working_diff.rs",
}


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True)


def mask(text: str) -> str:
    out = list(text)
    i = 0
    block = 0
    while i < len(text):
        if block:
            if text.startswith("/*", i):
                out[i : i + 2] = "  "
                block += 1
                i += 2
            elif text.startswith("*/", i):
                out[i : i + 2] = "  "
                block -= 1
                i += 2
            else:
                if text[i] != "\n":
                    out[i] = " "
                i += 1
            continue
        if text.startswith("//", i):
            out[i : i + 2] = "  "
            i += 2
            while i < len(text) and text[i] != "\n":
                out[i] = " "
                i += 1
            continue
        if text.startswith("/*", i):
            out[i : i + 2] = "  "
            block = 1
            i += 2
            continue
        if text[i] == '"':
            i += 1
            while i < len(text):
                char = text[i]
                if char != "\n":
                    out[i] = " "
                if char == '"' and text[i - 1] != "\\":
                    i += 1
                    break
                i += 1
            continue
        i += 1
    return "".join(out)


def function_body(text: str, name: str) -> str:
    masked = mask(text)
    match = re.search(rf"\bfn\s+{re.escape(name)}\b", masked)
    if not match:
        raise AssertionError(f"missing function {name}")
    opening = masked.find("{", match.end())
    depth = 0
    for index in range(opening, len(masked)):
        if masked[index] == "{":
            depth += 1
        elif masked[index] == "}":
            depth -= 1
            if depth == 0:
                return text[match.start() : index + 1]
    raise AssertionError(f"unbalanced function {name}")


def main(source_arg: str) -> int:
    root = Path(source_arg).resolve()
    red: list[str] = []
    if git(root, "rev-parse", "HEAD").strip() != HEAD:
        red.append("HEAD_ID")
    if git(root, "rev-parse", "HEAD^").strip() != PARENT:
        red.append("PARENT_ID")
    if git(root, "rev-parse", "HEAD^{tree}").strip() != TREE:
        red.append("TREE_ID")
    if set(git(root, "diff", "--name-only", f"{PARENT}..HEAD").splitlines()) != PATHS:
        red.append("SCOPE")

    history = (root / next(iter(sorted(PATHS)))).read_text()
    working = (root / sorted(PATHS)[1]).read_text()
    forbidden = (
        "ForkTreeReadFacade::new",
        "TrackedStateContext::diff_commits",
        "BranchHeadControlContext",
        "TrackedHeadContext",
        "owner.schema_keys",
    )
    for token in forbidden:
        if token in history or token in working:
            red.append(f"LEGACY_{token}")

    load = function_body(history, "load_file_history_rows")
    blob = function_body(history, "validate_exactly_one_blob_ref")
    descriptor = function_body(history, "parse_file_history_descriptors")
    directory = function_body(history, "parse_file_history_directories")
    observed_descriptor = function_body(history, "parse_file_history_observed_descriptors")
    observed_directory = function_body(history, "parse_file_history_observed_directories")
    observed_owner = function_body(history, "parse_file_history_observed_plugin_owners")
    grouped = function_body(history, "sorted_grouped_file_history_events")
    selector = function_body(working, "single_entity_pk_value")
    working_scan = function_body(working, "scan_descriptors")

    if "unwrap_or_default" in load or "needs_data.then" in load:
        red.append("ABSENT_TO_EMPTY_FALLBACK")
    if "let Some(blob) = refs.into_iter().next() else" in blob and "return Ok(None)" in blob:
        red.append("ZERO_BLOBREF_NOT_DISTINGUISHED")
    if "file_id" not in descriptor or "row_id" not in descriptor:
        red.append("DESCRIPTOR_ENTITYPK_FILEID_BINDING")
    if "file_id" not in directory or "row_id" not in directory:
        red.append("DIRECTORY_FILEID_NULL_BINDING")
    for label, body in (
        ("DESCRIPTOR_TOMBSTONE_PAYLOAD", observed_descriptor),
        ("DIRECTORY_TOMBSTONE_PAYLOAD", observed_directory),
        ("PLUGIN_OWNER_TOMBSTONE_PAYLOAD", observed_owner),
    ):
        if "deleted" in body and "snapshot_content" in body and "is_some" not in body:
            red.append(label)
    if ".into_iter()" in selector and ".next()" in selector:
        red.append("COMPOSITE_PK_FIRST_COMPONENT")
    if ".dedup_by(|left, right| left.id == right.id)" in grouped:
        red.append("CONFLICTING_SOURCE_DEDUP")

    positive = (
        "validate_descriptor_row_identity" in working_scan
        and "row.deleted" in working_scan
        and "row.snapshot_content.is_some()" in working_scan
    )
    print(f"SOURCE_HEAD={HEAD}")
    print(f"SOURCE_TREE={TREE}")
    print("WORKING_DIFF_POSITIVE_CONTROL=" + ("PASS" if positive else "FAIL"))
    for item in red:
        print(f"RED={item}")
    if red:
        print("SOURCE_STATUS=BLOCKED_EXPECTED_RED")
        return 1
    print("SOURCE_STATUS=GREEN")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: source_gate.py <exact-b484-checkout>")
    raise SystemExit(main(sys.argv[1]))
