#!/usr/bin/env python3
"""Candidate-parametric source gate for the retained-view identity contract.

The gate is deliberately conservative.  It is a structural pre-filter, not a
substitute for manual call-graph review or backend tests.  It accepts a Git
repository, base ref, and candidate ref, so it cannot accidentally certify the
immutable oracle's own source as a candidate.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ALLOWED_PREFIXES = ("packages/lix/src/forktree/",)
IDENTITY_COMPONENTS = {
    "storage_read_epoch": re.compile(r"(?:storage|read)[_ ]?(?:read[_ ]?)?(?:epoch|version|id)", re.I),
    "repository": re.compile(r"\brepository(?:_root|_id|_identity)?\b", re.I),
    "global_selector": re.compile(r"global[_ ]selector", re.I),
    "global_root": re.compile(r"global[_ ](?:state_)?root", re.I),
    "branch_selector": re.compile(r"branch[_ ]selector", re.I),
    "branch_root": re.compile(r"branch[_ ](?:snapshot|state_)?root", re.I),
    "snapshot_commit": re.compile(r"(?:snapshot|selected)[_ ](?:commit|head)|snapshot_commit", re.I),
}
TOKEN_TYPE = re.compile(r"\b(?:ViewToken|ReadToken|ReadIdentity|ViewIdentity|CoherentViewId)\b")
RAW_READER = re.compile(r"(?:Raw(?:Control)?Read|raw[_ ]?(?:control[_ ])?read|control[_ ]read)", re.I)
PACKED_READER = re.compile(r"(?:PackedRead|packed[_ ]read|HotPack(?:Read|Reader)?)", re.I)
HISTORY_READER = re.compile(r"(?:HistoryRead|history[_ ]read|CommitGraphReader)", re.I)
READ_ACQUIRE = re.compile(r"(?:\.\s*begin_read\s*\(|\bbegin_read\s*\(|\bacquire_read\s*\()")


@dataclass(frozen=True)
class Function:
    name: str
    body: str
    start: int


def git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.stdout


def function_bodies(source: str) -> list[Function]:
    functions: list[Function] = []
    for match in re.finditer(r"\bfn\s+([A-Za-z_][A-Za-z0-9_]*)[^\{]*\{", source):
        depth = 0
        end = None
        for index in range(match.end() - 1, len(source)):
            if source[index] == "{":
                depth += 1
            elif source[index] == "}":
                depth -= 1
                if depth == 0:
                    end = index + 1
                    break
        if end is not None:
            functions.append(Function(match.group(1), source[match.start() : end], match.start()))
    return functions


def code_only(source: str) -> str:
    """Mask comments and string/character literals before structural checks."""
    source = re.sub(r"//[^\n]*", "", source)
    source = re.sub(r"/\*.*?\*/", "", source, flags=re.DOTALL)
    source = re.sub(r'"(?:\\.|[^"\\])*"', '""', source)
    source = re.sub(r"'(?:\\.|[^'\\])*'", "''", source)
    return source


def test_function(name: str) -> bool:
    return name.startswith("test_") or name.startswith("qualification_") or name.endswith("_test")


def source_for_candidate(repo: Path, candidate: str) -> tuple[str, list[str]]:
    paths = git(repo, "ls-tree", "-r", "--name-only", candidate, "--", "packages/lix/src/forktree").splitlines()
    rust_paths = [path for path in paths if path.endswith(".rs")]
    chunks: list[str] = []
    for path in rust_paths:
        chunks.append(f"\n// SOURCE: {path}\n")
        chunks.append(git(repo, "show", f"{candidate}:{path}"))
    return "".join(chunks), rust_paths


def source_for_root(root: Path) -> tuple[str, list[str]]:
    paths = sorted(root.rglob("*.rs"))
    chunks: list[str] = []
    for path in paths:
        chunks.append(f"\n// SOURCE: {path.relative_to(root)}\n")
        chunks.append(path.read_text())
    return "".join(chunks), [str(path.relative_to(root)) for path in paths]


def reject(reason: str, failures: list[str]) -> None:
    failures.append(reason)


def check(
    repo: Path | None,
    base: str,
    candidate: str,
    source_root: Path | None = None,
) -> tuple[list[str], list[str]]:
    failures: list[str] = []
    notes: list[str] = []
    if source_root is None:
        assert repo is not None
        git(repo, "rev-parse", "--verify", f"{base}^{{commit}}")
        git(repo, "rev-parse", "--verify", f"{candidate}^{{commit}}")
        changed = git(repo, "diff", "--name-only", "--diff-filter=ACDMRTUXB", base, candidate, "--").splitlines()
    else:
        changed = []
    forbidden_paths = [path for path in changed if not path.startswith(ALLOWED_PREFIXES)]
    if forbidden_paths:
        reject(f"changed paths outside ForkTree allowlist: {forbidden_paths}", failures)
    notes.append(f"changed_paths={len(changed)}")

    source, rust_paths = (
        source_for_root(source_root)
        if source_root is not None
        else source_for_candidate(repo, candidate)  # type: ignore[arg-type]
    )
    code = code_only(source)
    if not rust_paths:
        reject("candidate has no ForkTree Rust source", failures)

    missing_components = [name for name, pattern in IDENTITY_COMPONENTS.items() if not pattern.search(code)]
    if missing_components:
        reject(f"identity omits authenticated components: {','.join(missing_components)}", failures)
    if not TOKEN_TYPE.search(code):
        reject("no named retained-view token/identity type", failures)

    functions = function_bodies(code)
    shared = [
        function
        for function in functions
        if RAW_READER.search(function.body)
        and PACKED_READER.search(function.body)
        and re.search(r"\b(?:view|read|facade|operation)\w*\b", function.body, re.I)
        and re.search(r"\b(?:token|identity)\w*\b", function.body, re.I)
    ]
    if not shared:
        reject("no single operation body constructs raw/control and packed readers from one view/read identity", failures)
    else:
        notes.append(f"shared_owner_functions={','.join(function.name for function in shared)}")

    reader_functions = [
        function
        for function in functions
        if not test_function(function.name)
        and (RAW_READER.search(function.name) or PACKED_READER.search(function.name) or HISTORY_READER.search(function.name))
    ]
    for function in reader_functions:
        if READ_ACQUIRE.search(function.body):
            reject(f"reader helper {function.name} acquires/refreshes a read", failures)
        if re.search(r"\bfallback\b|\brefresh\b|\bdetach\b|\bextract\b", function.body, re.I):
            reject(f"reader helper {function.name} contains refresh/fallback/detach behavior", failures)

    if re.search(r"static[^\n]*(?:cache|index)", code, re.I):
        reject("candidate declares a static cache/index authority", failures)
    if re.search(r"\b(?:legacy|compatibility|fallback)\b", code, re.I):
        reject("candidate source names a compatibility/legacy/fallback path", failures)

    index_functions = [
        function
        for function in functions
        if not test_function(function.name)
        and re.search(
            r"(?:install|build|validate).*(?:index|pack)|(?:index|pack).*(?:install|build|validate)",
            function.name,
            re.I,
        )
    ]
    for function in index_functions:
        body = function.body[function.body.find("{") + 1 :]
        validation = re.search(r"(?:validate|authenticate|proof|verify)", body, re.I)
        installation = re.search(r"(?:install|insert|cache|index)", body, re.I)
        if installation and (validation is None or validation.start() > installation.start()):
            reject(f"index helper {function.name} installs before validation", failures)

    if not re.search(r"(?:unknown|unsupported|invalid).*(?:domain|space)|(?:domain|space).*(?:unknown|unsupported|invalid)", code, re.I):
        reject("no explicit unknown-domain/space fail-closed branch", failures)
    else:
        notes.append("unknown_domain_branch=present")
    return failures, notes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--base", required=True)
    parser.add_argument("--candidate", required=True)
    args = parser.parse_args()
    if (args.repo is None) == (args.source_root is None):
        parser.error("provide exactly one of --repo or --source-root")
    try:
        failures, notes = check(args.repo, args.base, args.candidate, args.source_root)
    except subprocess.CalledProcessError as error:
        print(f"SOURCE_GATE=ERROR command={error.cmd} stderr={error.stderr.strip()}")
        return 2
    print(f"SOURCE_GATE={'GREEN' if not failures else 'RED'} base={args.base} candidate={args.candidate}")
    for note in notes:
        print(f"NOTE {note}")
    for failure in failures:
        print(f"FAIL {failure}")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
