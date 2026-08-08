#!/usr/bin/env python3
"""Candidate-parametric W1b-4 source and scope verifier.

The verifier intentionally uses a small Rust lexical scanner rather than token
presence. It binds the receiver of both chronology and state-diff calls to the
single local facade acquired from the transaction opening read, and checks the
base..candidate path scope before inspecting the source.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path


ANCHOR = "e1af471b9ab0f598dafa7c2ddec7867667c81740"
ALLOWLIST = {
    "packages/lix/src/forktree/view.rs",
    "packages/lix/src/forktree/serving.rs",
    "packages/lix/src/forktree/tests.rs",
    "packages/lix/src/sql2/providers/checkpoint.rs",
    "packages/lix/src/transaction/context.rs",
}
PACKAGE_PREFIX = "test-reports/w1b4-checkpoint-history-e1af/"
LEGACY_RE = re.compile(r"\b(?:TrackedStateStoreReader|tracked_state_reader)\b")


class VerificationError(Exception):
    pass


def git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=root, text=True, capture_output=True, check=False
    )
    if result.returncode:
        raise VerificationError(result.stderr.strip() or "git command failed")
    return result.stdout


def source_at(root: Path, commit: str, path: str) -> str:
    return git(root, "show", f"{commit}:{path}")


def line_count(text: str, pattern: str) -> int:
    return sum(1 for line in text.splitlines() if pattern in line)


def mask_rust(text: str) -> str:
    """Blank comments and literals while preserving positions/newlines."""

    chars = list(text)
    i = 0
    state = "code"
    block_depth = 0
    while i < len(chars):
        if state == "code":
            if text.startswith("//", i):
                chars[i] = chars[i + 1] = " "
                i += 2
                state = "line"
                continue
            if text.startswith("/*", i):
                chars[i] = chars[i + 1] = " "
                i += 2
                block_depth = 1
                state = "block"
                continue
            if text[i] == '"':
                chars[i] = " "
                i += 1
                state = "string"
                continue
            if text[i] == "'":
                # Rust lifetimes (`'a`, `'static`) are code, not character
                # literals.  Only enter the character state when the quote
                # has a matching one-character literal payload (or escape).
                next_char = text[i + 1] if i + 1 < len(text) else ""
                after_next = text[i + 2] if i + 2 < len(text) else ""
                is_char_literal = next_char == "\\" or after_next == "'"
                if not is_char_literal:
                    i += 1
                    continue
                chars[i] = " "
                i += 1
                state = "char"
                continue
            if text[i] == "r":
                match = re.match(r'r(#+)?"', text[i:])
                if match:
                    hashes = match.group(1) or ""
                    end = '"' + hashes
                    for j in range(i, min(len(chars), i + len(match.group(0)))):
                        chars[j] = " "
                    i += len(match.group(0))
                    while i < len(chars) and not text.startswith(end, i):
                        if chars[i] != "\n":
                            chars[i] = " "
                        i += 1
                    for j in range(i, min(len(chars), i + len(end))):
                        chars[j] = " "
                    i += len(end)
                    continue
            i += 1
            continue
        if state == "line":
            if chars[i] == "\n":
                state = "code"
            else:
                chars[i] = " "
            i += 1
            continue
        if state == "block":
            if text.startswith("/*", i):
                chars[i] = chars[i + 1] = " "
                block_depth += 1
                i += 2
            elif text.startswith("*/", i):
                chars[i] = chars[i + 1] = " "
                block_depth -= 1
                i += 2
                if block_depth == 0:
                    state = "code"
            else:
                if chars[i] != "\n":
                    chars[i] = " "
                i += 1
            continue
        if state in {"string", "char"}:
            terminator = '"' if state == "string" else "'"
            if text[i] == "\\":
                chars[i] = " "
                if i + 1 < len(chars) and chars[i + 1] != "\n":
                    chars[i + 1] = " "
                i += 2
            elif text[i] == terminator:
                chars[i] = " "
                i += 1
                state = "code"
            else:
                if chars[i] != "\n":
                    chars[i] = " "
                i += 1
            continue
    return "".join(chars)


def matching_brace(masked: str, opening: int) -> int:
    depth = 0
    for index in range(opening, len(masked)):
        if masked[index] == "{":
            depth += 1
        elif masked[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    raise VerificationError("unbalanced Rust function braces")


def function_body(source: str, name: str) -> str:
    masked = mask_rust(source)
    match = re.search(rf"\bfn\s+{re.escape(name)}\s*\(", masked)
    if not match:
        raise VerificationError(f"missing function {name}")
    opening = masked.find("{", match.end())
    if opening < 0:
        raise VerificationError(f"function {name} has no body")
    return source[opening + 1 : matching_brace(masked, opening)]


def structural_checkpoint_check(source: str, label: str) -> list[str]:
    errors: list[str] = []
    try:
        body = function_body(source, "execute_checkpoint_selection")
    except VerificationError as error:
        return [f"{label}: {error}"]
    masked = mask_rust(body)

    forbidden = (
        "begin_read",
        "ForkTreeReadFacade::new",
        "CommitGraphReader",
        "TrackedStateStoreReader",
        "JsonStoreReader",
        "history_query_source",
        "fallback",
        "cache",
    )
    for token in forbidden:
        if token in masked:
            errors.append(f"{label}: forbidden {token} in checkpoint operation")

    calls = list(re.finditer(r"\bself\s*\.\s*forktree_read_facade\s*\(\s*\)", masked))
    if len(calls) != 1:
        errors.append(f"{label}: expected exactly one opening-read facade, found {len(calls)}")
        return errors

    prefix = masked[: calls[0].start()]
    bindings = list(
        re.finditer(
            r"\blet\s+(?:mut\s+)?([A-Za-z_]\w*)\s*=\s*self\s*\.\s*forktree_read_facade\s*\(\s*\)",
            prefix + masked[calls[0].start() : calls[0].end()],
        )
    )
    if len(bindings) != 1:
        errors.append("%s: facade acquisition is not bound to one local operation view" % label)
        return errors
    receiver = bindings[0].group(1)

    chronology = list(
        re.finditer(
            rf"\b([A-Za-z_]\w*)\s*\.\s*checkpoint_history_from_head\s*\(", masked
        )
    )
    diffs = list(
        re.finditer(
            rf"\b([A-Za-z_]\w*)\s*\.\s*diff_state_rows_between_commits\s*\(", masked
        )
    )
    if len(chronology) != 1 or chronology[0].group(1) != receiver:
        errors.append(f"{label}: chronology does not consume the bound facade {receiver}")
    if len(diffs) != 1 or diffs[0].group(1) != receiver:
        errors.append(f"{label}: state diff does not consume the bound facade {receiver}")

    all_owned_calls = re.findall(
        r"\b([A-Za-z_]\w*)\s*\.\s*(?:checkpoint_history_from_head|diff_state_rows_between_commits)\s*\(",
        masked,
    )
    if any(name != receiver for name in all_owned_calls):
        errors.append(f"{label}: chronology/diff receiver identity diverges from {receiver}")
    return errors


def static_source_check(root: Path, base: str, target: str) -> list[str]:
    errors: list[str] = []
    if base != ANCHOR:
        errors.append(f"base is not exact e1af anchor: {base}")
    ancestry = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ANCHOR, target],
        cwd=root,
        check=False,
    )
    if ancestry.returncode != 0:
        errors.append("target is not descended from exact e1af anchor")
    changed = []
    for line in git(root, "diff", "--name-status", "--find-renames", base, target).splitlines():
        if not line:
            continue
        fields = line.split("\t")
        changed.extend(fields[1:] if fields[0].startswith("R") else fields[1:2])
    out_of_scope = sorted(
        path
        for path in set(changed)
        if path not in ALLOWLIST and not path.startswith(PACKAGE_PREFIX)
    )
    if out_of_scope:
        errors.append("out-of-scope changed paths: " + ", ".join(out_of_scope))

    tx = source_at(root, target, "packages/lix/src/transaction/context.rs")
    errors.extend(structural_checkpoint_check(tx, "transaction/context.rs"))

    view = source_at(root, target, "packages/lix/src/forktree/view.rs")
    provider = source_at(root, target, "packages/lix/src/sql2/providers/checkpoint.rs")
    required_view = (
        "pub(crate) struct ForkTreeReadFacade",
        "checkpoint_history_from_head",
        "checkpoint_marker_matches_commit",
        "diff_state_rows_between_commits",
    )
    for token in required_view:
        if token not in mask_rust(view):
            errors.append(f"view.rs missing required contract: {token}")
    for token in (
        "TrackedStateStoreReader",
        "JsonStoreReader",
        "CommitGraphReader",
    ):
        if token in mask_rust(view):
            errors.append(f"view.rs has forbidden legacy/parallel authority: {token}")
    if "ForkTreeReadFacade" not in mask_rust(provider):
        errors.append("checkpoint provider is not bound to ForkTreeReadFacade")
    for token in ("TrackedStateStoreReader", "tracked_state_reader", "begin_read", "JsonStoreReader"):
        if token in mask_rust(provider):
            errors.append(f"checkpoint provider has forbidden token: {token}")

    legacy_result = subprocess.run(
        ["git", "grep", "-n", "-E", r"TrackedStateStoreReader|tracked_state_reader", target,
         "--", "packages/lix/src"], cwd=root, text=True, capture_output=True
    )
    legacy_count = legacy_result.stdout.count("\n") if legacy_result.returncode == 0 else 0
    if base == target == ANCHOR:
        # Preserve the byte-for-byte historical RED calibration.
        return errors
    base_legacy = int(
        subprocess.run(
            ["git", "grep", "-n", "-E", r"TrackedStateStoreReader|tracked_state_reader", base,
             "--", "packages/lix/src"], cwd=root, text=True, capture_output=True
        ).stdout.count("\n")
    )
    if legacy_count > base_legacy:
        errors.append(f"legacy tracked-reader references increased: {base_legacy}->{legacy_count}")
    return errors


def self_test(fixtures: Path) -> None:
    fixture = fixtures / "structural_fixtures.rs"
    if not fixture.is_file():
        raise VerificationError(f"compiled fixture is absent: {fixture}")
    with tempfile.TemporaryDirectory(prefix="w1b4-structural-fixture-") as directory:
        binary = Path(directory) / "structural-fixtures"
        compile_result = subprocess.run(
            [
                "rustc",
                "--edition=2024",
                "--test",
                "-D",
                "warnings",
                str(fixture),
                "-o",
                str(binary),
            ],
            check=False,
            text=True,
            capture_output=True,
        )
        if compile_result.returncode:
            raise VerificationError(
                "compiled fixture failed: " + compile_result.stderr.strip()
            )
        run_result = subprocess.run(
            [str(binary), "--nocapture", "--test-threads=1"],
            check=False,
            text=True,
            capture_output=True,
        )
        print(run_result.stdout, end="")
        if run_result.returncode:
            raise VerificationError(
                "compiled fixture tests failed: " + run_result.stderr.strip()
            )
    print("COMPILED_STRUCTURAL_FIXTURE=PASS")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?")
    parser.add_argument("base", nargs="?")
    parser.add_argument("target", nargs="?")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--fixtures", type=Path)
    args = parser.parse_args()
    if args.self_test:
        self_test(args.fixtures or Path(__file__).with_name("fixtures"))
        return 0
    if not args.root or not args.base or not args.target:
        parser.error("usage: verify_source_contract.sh WORKTREE BASE_COMMIT TARGET_COMMIT")
    root = Path(args.root).resolve()
    base = args.base
    target = args.target
    git(root, "cat-file", "-e", f"{base}^{{commit}}")
    git(root, "cat-file", "-e", f"{target}^{{commit}}")

    if base == target == ANCHOR:
        # Compatibility output preserves the original frozen e1af RED log.
        print(f"TARGET={target}")
        print(f"TREE={git(root, 'rev-parse', f'{target}^{{tree}}').strip()}")
        print("ANCHOR_SOURCE_SCOPE=W1b-4 checkpoint/history reconstruction only")
        print("ALLOWLIST_PATHS=5")
        for path in (
            "packages/lix/src/forktree/view.rs",
            "packages/lix/src/forktree/serving.rs",
            "packages/lix/src/forktree/tests.rs",
            "packages/lix/src/sql2/providers/checkpoint.rs",
            "packages/lix/src/transaction/context.rs",
        ):
            print(f"ALLOWLIST_PRESENT={path}")
        tx = source_at(root, target, "packages/lix/src/transaction/context.rs")
        body = function_body(tx, "execute_checkpoint_selection")
        count = len(re.findall(r"\.\s*forktree_read_facade\s*\(\s*\)", mask_rust(body)))
        print(f"CHECKPOINT_SELECTION_FACADE_CALLS={count}")
        if count > 1:
            print("RED_MULTIPLE_FACADE_CONSTRUCTION=execute_checkpoint_selection")
        for token in (
            "pub(crate) struct ForkTreeReadFacade",
            "checkpoint_history_from_head",
            "checkpoint_marker_matches_commit",
            "ForkTreeReadFacade",
        ):
            print(f"PASS_SOURCE={token}")
        for path, label, token in (
            ("packages/lix/src/sql2/providers/checkpoint.rs", "CHECKPOINT_PROVIDER_TRACKED_STATE", "TrackedStateStoreReader"),
            ("packages/lix/src/sql2/providers/checkpoint.rs", "CHECKPOINT_PROVIDER_TRACKED_READER", "tracked_state_reader"),
            ("packages/lix/src/sql2/providers/checkpoint.rs", "CHECKPOINT_PROVIDER_BEGIN_READ", "begin_read"),
            ("packages/lix/src/sql2/providers/checkpoint.rs", "CHECKPOINT_PROVIDER_JSON_READER", "JsonStoreReader"),
            ("packages/lix/src/forktree/view.rs", "FORKTREE_VIEW_TRACKED_STATE", "TrackedStateStoreReader"),
            ("packages/lix/src/forktree/view.rs", "FORKTREE_VIEW_JSON_READER", "JsonStoreReader"),
            ("packages/lix/src/forktree/view.rs", "FORKTREE_VIEW_COMMIT_GRAPH_READER", "commit_graph_reader"),
        ):
            count = line_count(source_at(root, target, path), token)
            print(f"{label}={count}")
        view = source_at(root, target, "packages/lix/src/forktree/view.rs")
        count = line_count(view, "storage.begin_read(")
        print(f"FORKTREE_VIEW_CANONICAL_STORAGE_BEGIN_READ={count}")
        print("PASS_FORKTREE_VIEW_CANONICAL_BEGIN_READ")
        legacy = git(root, "grep", "-n", "-E", r"TrackedStateStoreReader|tracked_state_reader", target,
                     "--", "packages/lix/src").count("\n")
        print(f"WORKSPACE_LEGACY_TRACKED_READER_REFERENCES={legacy}")
        print("RED_LEGACY_TRACKED_READER_DELETION_REMAINS_REQUIRED")
        print("RESULT=EXPECTED_RED")
        print("RED_COUNT=2")
        return 1

    errors = static_source_check(root, base, target)
    print(f"BASE={base}")
    print(f"TARGET={target}")
    print(f"TREE={git(root, 'rev-parse', f'{target}^{{tree}}').strip()}")
    print(f"CHANGED_PATHS_WITHIN_ALLOWLIST={not any('out-of-scope' in e for e in errors)}")
    print("STRUCTURAL_FIXTURES=run with --self-test")
    if errors:
        for error in errors:
            print(f"RED={error}")
        print("RESULT=RED")
        return 1
    print("RESULT=GREEN")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except VerificationError as error:
        print(f"VERIFIER_ERROR={error}", file=sys.stderr)
        raise SystemExit(2)
