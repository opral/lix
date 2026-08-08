#!/usr/bin/env python3
"""W1b-5 whole-scope, balanced-function source contract checker."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ALLOWLIST = {
    "packages/lix/src/forktree/view.rs",
    "packages/lix/src/forktree/serving.rs",
    "packages/lix/src/forktree/tests.rs",
    "packages/lix/src/sql2/context.rs",
    "packages/lix/src/sql2/providers/working_diff.rs",
    "packages/lix/src/sql2/providers/filesystem_working_diff.rs",
    "packages/lix/src/sql2/providers/checkpoint.rs",
    "packages/lix/src/session/checkpoint.rs",
    "packages/lix/src/session/context.rs",
    "packages/lix/src/filesystem/read.rs",
    "packages/lix/src/live_state/forktree_reader.rs",
}
RESIDUE_PATHS = {
    "packages/lix/src/live_state/context.rs",
    "packages/lix/src/live_state/tracked_head.rs",
    "packages/lix/src/init.rs",
    "packages/lix/src/gc.rs",
    "packages/lix/src/transaction/context.rs",
    "packages/lix/src/branch/refs.rs",
}
PROVIDER_PATHS = (
    "packages/lix/src/sql2/providers/working_diff.rs",
    "packages/lix/src/sql2/providers/filesystem_working_diff.rs",
    "packages/lix/src/sql2/providers/checkpoint.rs",
)
FORBIDDEN = (
    "TrackedStateStoreReader",
    "TrackedHeadContext",
    "BranchHeadControlContext",
    "stage_current_state_with_working_diff",
    "Storage::begin_read",
    "StorageAdapter::new",
    "begin_read",
    "with_opening_tracked_reader",
    "JsonStoreReader::new",
    "fallback",
    "Fallback",
    "cache",
    "Cache",
    "retry",
    "selected_heads",
    "BranchRefReader",
    "ForkTreeReadFacade::new",
    "StorageRead",
)


def mask_rust(text: str) -> str:
    out = list(text)
    i = 0
    state = "code"
    block_depth = 0
    while i < len(text):
        if state == "code":
            if text.startswith("//", i):
                out[i] = out[i + 1] = " "
                i += 2
                state = "line"
                continue
            if text.startswith("/*", i):
                out[i] = out[i + 1] = " "
                i += 2
                block_depth = 1
                state = "block"
                continue
            if text[i] == '"':
                out[i] = " "
                i += 1
                state = "string"
                continue
            if text[i] == "'":
                if i + 1 < len(text) and (text[i + 1].isalpha() or text[i + 1] == "_"):
                    i += 1
                    continue
                out[i] = " "
                i += 1
                state = "char"
                continue
            i += 1
        elif state == "line":
            if text[i] == "\n":
                state = "code"
            else:
                out[i] = " "
            i += 1
        elif state == "block":
            if text.startswith("/*", i):
                out[i] = out[i + 1] = " "
                i += 2
                block_depth += 1
            elif text.startswith("*/", i):
                out[i] = out[i + 1] = " "
                i += 2
                block_depth -= 1
                if block_depth == 0:
                    state = "code"
            else:
                if text[i] != "\n":
                    out[i] = " "
                i += 1
        else:
            if state == "string" and text[i] == "\\":
                out[i] = " "
                if i + 1 < len(text):
                    out[i + 1] = " "
                    i += 2
                else:
                    i += 1
                continue
            if (state == "string" and text[i] == '"') or (state == "char" and text[i] == "'"):
                out[i] = " "
                i += 1
                state = "code"
            else:
                if text[i] != "\n":
                    out[i] = " "
                i += 1
    return "".join(out)


def functions(text: str) -> dict[str, str]:
    masked = mask_rust(text)
    result: dict[str, str] = {}
    for match in re.finditer(r"\b(?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*", masked):
        brace = masked.find("{", match.end())
        if brace < 0:
            continue
        depth = 0
        end = None
        for pos in range(brace, len(masked)):
            if masked[pos] == "{":
                depth += 1
            elif masked[pos] == "}":
                depth -= 1
                if depth == 0:
                    end = pos + 1
                    break
        if end is not None:
            result[match.group(1)] = masked[brace:end]
    return result


def source_at(root: Path, target: str, path: str) -> str | None:
    proc = subprocess.run(
        ["git", "-C", str(root), "show", f"{target}:{path}"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    return proc.stdout if proc.returncode == 0 else None


def has(text: str, pattern: str) -> bool:
    return re.search(pattern, mask_rust(text), re.MULTILINE) is not None


def forbidden_in(text: str) -> list[str]:
    code = mask_rust(text)
    return sorted({token for token in FORBIDDEN if token in code})


def provider_errors(path: str, text: str) -> list[str]:
    errors: list[str] = []
    code = mask_rust(text)
    if "ForkTreeReadFacade" not in code or "forktree_reader" not in code:
        errors.append(f"{path}: no operation-owned ForkTreeReadFacade field")
    if "projected_schema" not in code or "limit" not in code:
        errors.append(f"{path}: projection/LIMIT lowering is not structurally present")
    if "ordering: Some" not in code:
        errors.append(f"{path}: authenticated ascending output ordering is absent")
    bad = forbidden_in(text)
    if bad:
        errors.append(f"{path}: alternate reader/authority tokens: {','.join(bad)}")
    return errors


def check_sources(sources: dict[str, str]) -> list[str]:
    errors: list[str] = []
    working = sources.get(PROVIDER_PATHS[0], "")
    filesystem = sources.get(PROVIDER_PATHS[1], "")
    checkpoint = sources.get(PROVIDER_PATHS[2], "")
    view = sources.get("packages/lix/src/forktree/view.rs", "")
    session_checkpoint = sources.get("packages/lix/src/session/checkpoint.rs", "")

    for path in PROVIDER_PATHS:
        errors.extend(provider_errors(path, sources.get(path, "")))

    if "latest_checkpoint_for_branch" not in working or "diff_state_rows_between_commits" not in working:
        errors.append("SQL working diff lacks exact checkpoint/base/head ForkTree calls")
    if "latest_checkpoint_for_branch" not in filesystem or "scan_state_rows_at_commit" not in filesystem:
        errors.append("filesystem working diff lacks exact checkpoint/base/head ForkTree calls")
    if "BlobRef" not in mask_rust(filesystem) and "BlobId" not in mask_rust(filesystem):
        errors.append("filesystem working diff lacks BlobRef/BlobId identity seam")
    if "load_blob" not in mask_rust(filesystem) and "blob_bytes" not in mask_rust(filesystem):
        errors.append("filesystem working diff lacks authenticated blob payload seam")
    if "checkpoint_history_from_head" not in mask_rust(checkpoint):
        errors.append("checkpoint provider lacks ForkTree chronology capability")
    if "checkpoint_marker_matches_commit" not in mask_rust(view):
        errors.append("marker-to-walked-commit validation is absent")
    if "walked_commit_id" not in mask_rust(view) or "is_root" not in mask_rust(view):
        errors.append("root-as-implicit and marker identity proof is absent")
    view_code = mask_rust(view)
    for token, label in (
        ("StateCell::Null", "NULL value handling"),
        ("StateCell::Tombstone", "tombstone handling"),
        ("required_object", "missing-object failure"),
        ("serde_json::from_str", "malformed-object failure"),
        ("Value::as_str", "wrong-kind/shape failure"),
    ):
        if token not in view_code:
            errors.append(f"ForkTree view lacks fail-closed {label}")
    if "scan_untracked_rows" not in mask_rust(view):
        errors.append("tracked/untracked facade seam is absent")
    if "forktree_read_facade" not in mask_rust(session_checkpoint):
        errors.append("session checkpoint does not use caller-owned ForkTree facade")
    if "TrackedStateStoreReader" in mask_rust(session_checkpoint) or "TrackedHeadContext" in mask_rust(session_checkpoint):
        errors.append("session checkpoint retains a legacy reader")
    for path in sorted(RESIDUE_PATHS):
        text = sources.get(path, "")
        code = mask_rust(text)
        if path.endswith("transaction/context.rs") and "TrackedStateStoreReader" in code:
            errors.append("transaction context retains TrackedStateStoreReader factory/callback")
        elif path.endswith(("live_state/context.rs", "live_state/tracked_head.rs", "init.rs", "gc.rs")):
            if "TrackedHeadContext" in code or "BranchHeadControlContext" in code or "stage_current_state_with_working_diff" in code:
                errors.append(f"{path}: current-layout TrackedHead/BranchHeadControl owner remains reachable")
    return errors


def fixture_sources(path: Path) -> dict[str, str]:
    body = (path / "candidate.rs").read_text()
    return {name: body for name in (*PROVIDER_PATHS, "packages/lix/src/forktree/view.rs", "packages/lix/src/session/checkpoint.rs")}


def fixture_self_test(base: Path) -> list[str]:
    errors: list[str] = []
    if check_sources(fixture_sources(base / "positive")):
        errors.append("positive structural fixture was rejected")
    for name in ("second_reader", "separate_head", "unordered", "blob_gap", "marker_gap", "legacy_owner"):
        if not check_sources(fixture_sources(base / "negative" / name)):
            errors.append(f"negative structural fixture was accepted: {name}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path)
    parser.add_argument("--target")
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    fixture_errors = fixture_self_test(args.fixtures)
    if fixture_errors:
        for error in fixture_errors:
            print("FIXTURE-RED " + error)
        return 2
    print("FIXTURE GREEN positive accepted; six negative fixtures rejected")
    if args.self_test:
        return 0
    if args.root is None or args.target is None:
        parser.error("--root and --target are required")
    sources = {}
    for path in sorted(ALLOWLIST | RESIDUE_PATHS):
        text = source_at(args.root, args.target, path)
        if text is not None:
            sources[path] = text
    errors = check_sources(sources)
    if errors:
        for error in errors:
            print("STRUCTURAL-RED " + error)
        return 1
    print("STRUCTURAL GREEN W1b-5 provider/chronology contract")
    return 0


if __name__ == "__main__":
    sys.exit(main())
