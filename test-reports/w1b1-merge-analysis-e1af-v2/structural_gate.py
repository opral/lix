#!/usr/bin/env python3
"""Balanced, item-scoped W1b-1 source contract checker."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

FORBIDDEN = (
    "TrackedStateStoreReader",
    "diff_commits",
    "with_opening_tracked_reader",
    "JsonStoreReader",
    "HistoryQuerySource",
    "ChangelogQuerySource",
    "SqlHistoryQuerySource",
    "CommitGraphStoreReader",
    "TrackedStateContext",
    "fallback",
    "Fallback",
    "cache",
    "Cache",
    "begin_read",
    "open_coherent_view",
    "StorageAdapterRead",
    "StorageRead",
    "get_many",
)

ALLOWLIST = {
    "packages/lix/src/session/merge/analysis.rs",
    "packages/lix/src/session/merge/branch.rs",
    "packages/lix/src/transaction/context.rs",
    "packages/lix/src/tracked_state/diff.rs",
    "packages/lix/src/forktree/view.rs",
    "packages/lix/src/forktree/serving.rs",
    "packages/lix/src/forktree/tests.rs",
}


def mask_rust(text: str) -> str:
    """Replace comments and literals with spaces, preserving offsets/newlines."""
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
                # Rust lifetimes such as 'static are code, not character
                # literals.  Only enter char-literal mode when the apostrophe
                # is followed by a non-identifier character.
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
                block_depth += 1
                i += 2
            elif text.startswith("*/", i):
                out[i] = out[i + 1] = " "
                block_depth -= 1
                i += 2
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


def functions(text: str) -> dict[str, tuple[str, str]]:
    masked = mask_rust(text)
    found: dict[str, tuple[str, str]] = {}
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
            found[match.group(1)] = (masked[match.start():brace], masked[brace:end])
    return found


def source_at(root: Path, target: str, path: str) -> str | None:
    proc = subprocess.run(
        ["git", "-C", str(root), "show", f"{target}:{path}"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    return proc.stdout if proc.returncode == 0 else None


def forbidden(text: str) -> list[str]:
    code = mask_rust(text)
    # Underscore is an identifier character in Rust.  Match forbidden
    # authority components inside snake_case names such as
    # merge_payload_fallback_cache, while comments and literals are already
    # blanked by mask_rust.
    return sorted({token for token in FORBIDDEN if re.search(re.escape(token), code)})


def check_sources(sources: dict[str, str]) -> list[str]:
    errors: list[str] = []
    analysis = sources.get("packages/lix/src/session/merge/analysis.rs", "")
    branch = sources.get("packages/lix/src/session/merge/branch.rs", "")
    context = sources.get("packages/lix/src/transaction/context.rs", "")
    view = sources.get("packages/lix/src/forktree/view.rs", "")
    analysis_fn = functions(analysis).get("analyze")
    if analysis_fn is None:
        errors.append("analysis.rs lacks function analyze")
    else:
        signature, body = analysis_fn
        if not re.search(r"\b(?:ForkTreeReadFacade|CoherentView)\b", signature):
            errors.append("analyze lacks typed ForkTreeReadFacade/CoherentView parameter")
        if not re.search(r"\b(?:view|facade)\b\s*:\s*[^,)]*\b(?:ForkTreeReadFacade|CoherentView)\b", signature):
            errors.append("analyze lacks named typed operation-view argument")
        if not re.search(r"\b(?:view|facade)\s*\.", body):
            errors.append("analyze does not use its operation-view argument")
        if not re.search(r"\b(?:view|facade)\s*\.\s*(?:branch|coherent_view)\s*\(", body):
            errors.append("analyze does not traverse facade to CoherentView")
        bad = forbidden(body)
        if bad:
            errors.append("analyze contains forbidden authority/acquisition: " + ",".join(bad))

    ctx_fn = functions(context).get("forktree_read_facade")
    if ctx_fn is None:
        errors.append("transaction context lacks forktree_read_facade")
    else:
        _, body = ctx_fn
        if not re.search(r"\bopening_read\s*\(", body):
            errors.append("forktree_read_facade does not use retained opening_read")
        if re.search(r"\b(?:begin_read|open_coherent_view)\b", body):
            errors.append("forktree_read_facade acquires or refreshes a read")
        if not re.search(r"\bForkTreeReadFacade\s*::\s*new\s*\(", body):
            errors.append("forktree_read_facade lacks typed facade construction")

    caller_functions = functions(branch)
    callers = [
        (name, sig, body)
        for name, (sig, body) in caller_functions.items()
        if name.startswith("merge_branch") and "analysis::analyze" in body
    ]
    if not callers:
        errors.append("no merge_branch caller contains analysis::analyze")
    for name, _, body in callers:
        acquisitions = re.findall(
            r"let\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*transaction\.forktree_read_facade\s*\(\s*\)",
            body,
        )
        if len(acquisitions) != 1:
            errors.append(f"{name} must acquire exactly one transaction facade")
            continue
        alias = acquisitions[0]
        calls = re.findall(
            r"analysis::analyze\s*\(\s*&?\s*([A-Za-z_][A-Za-z0-9_]*)\b",
            body,
        )
        if not calls or any(call != alias for call in calls):
            errors.append(f"{name} analysis call does not use exact facade alias {alias}")
        if re.search(
            r"\b(?:begin_read|open_coherent_view|ForkTreeReadFacade\s*::\s*new|CommitGraphStoreReader\s*::\s*new)\b",
            body,
        ):
            errors.append(f"{name} constructs/acquires a second read or graph")
        bad = forbidden(body)
        bad = [token for token in bad if token != "forktree_read_facade"]
        if bad:
            errors.append(f"{name} contains forbidden alternate authority: " + ",".join(bad))

    view_code = mask_rust(view)
    if not re.search(r"\bstruct\s+ForkTreeReadFacade\b", view_code):
        errors.append("view.rs lacks ForkTreeReadFacade owner")
    if not re.search(r"->\s*Result\s*<\s*CoherentView", view_code):
        errors.append("view.rs lacks facade-to-CoherentView return path")

    for path, text in sources.items():
        if path.endswith("transaction/context.rs") and "with_opening_tracked_reader" in mask_rust(text):
            errors.append("transaction context retains with_opening_tracked_reader")
        if path.endswith("tracked_state/diff.rs") and "TrackedStateStoreReader" in mask_rust(text):
            errors.append("tracked-state diff owner retains TrackedStateStoreReader")
    return errors


def fixture_sources(root: Path) -> dict[str, str]:
    return {
        "packages/lix/src/session/merge/analysis.rs": (root / "analysis.rs").read_text(),
        "packages/lix/src/session/merge/branch.rs": (root / "branch.rs").read_text(),
        "packages/lix/src/transaction/context.rs": (root / "context.rs").read_text(),
        "packages/lix/src/forktree/view.rs": (root / "view.rs").read_text(),
    }


def fixture_self_test(base: Path) -> list[str]:
    errors: list[str] = []
    positive = check_sources(fixture_sources(base / "positive"))
    if positive:
        errors.append("positive fixture rejected: " + "; ".join(positive))
    for name in ("second_read", "alias_mismatch", "fresh_graph", "fallback", "second_authority"):
        fixture = fixture_sources(base / "negative" / name)
        if not check_sources(fixture):
            errors.append(f"negative fixture accepted: {name}")
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
    print("FIXTURE GREEN positive accepted; five negative fixtures rejected")
    if args.self_test:
        return 0
    if args.root is None or args.target is None:
        parser.error("--root and --target are required without --self-test")
    sources = {}
    for path in sorted(ALLOWLIST):
        text = source_at(args.root, args.target, path)
        if text is not None:
            sources[path] = text
    errors = check_sources(sources)
    if errors:
        for error in errors:
            print("STRUCTURAL-RED " + error)
        return 1
    print("STRUCTURAL GREEN one operation-owned facade/view contract")
    return 0


if __name__ == "__main__":
    sys.exit(main())
