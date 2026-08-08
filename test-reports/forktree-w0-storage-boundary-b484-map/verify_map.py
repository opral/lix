#!/usr/bin/env python3
"""Source/provenance-only verifier for the W0/b484 planning package.

It intentionally does not invoke Cargo, tests, adapters, or production code.
"""

from __future__ import annotations

import argparse
import hashlib
import pathlib
import subprocess
import sys


W0 = "846981ead666eda465d358368f73cf93e2c9339f"
CANDIDATE = "b484e20d845aee3f8137bfa3496f9b3cd0e8cd35"
FD2 = "fd2be256d763f17e9f127d4c984e36fba191cb82"
W0_TREE = "8731e9a4c4239ab175d938b069870703fc5affb4"
CANDIDATE_TREE = "4477c83b246bddac09cd972564bd4ccd67f90f7b"


def git(repo: pathlib.Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def die(message: str) -> None:
    print(f"FAIL: {message}", file=sys.stderr)
    raise SystemExit(1)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=pathlib.Path, required=True)
    parser.add_argument("--w0", default=W0)
    parser.add_argument("--candidate", default=CANDIDATE)
    args = parser.parse_args()
    repo = args.repo.resolve()

    if args.w0 != W0 or args.candidate != CANDIDATE:
        die("only the frozen W0 and b484 identities are accepted")
    if git(repo, "rev-parse", args.w0) != W0:
        die("W0 object mismatch")
    if git(repo, "rev-parse", f"{args.w0}^{{tree}}") != W0_TREE:
        die("W0 tree mismatch")
    if git(repo, "rev-parse", args.candidate) != CANDIDATE:
        die("candidate object mismatch")
    if git(repo, "rev-parse", f"{args.candidate}^{{tree}}") != CANDIDATE_TREE:
        die("candidate tree mismatch")
    if git(repo, "rev-parse", f"{args.candidate}^") != FD2:
        die("candidate parent is not fd2")

    paths = git(repo, "diff", "--name-only", f"{FD2}..{args.candidate}").splitlines()
    expected = {
        "packages/lix/src/sql2/providers/file_history.rs",
        "packages/lix/src/sql2/providers/filesystem_working_diff.rs",
    }
    if set(paths) != expected:
        die(f"unexpected b484 source scope: {paths!r}")

    report_dir = pathlib.Path(__file__).resolve().parent
    report = (report_dir / "IMPLEMENTABILITY_MAP.md").read_text()
    required = [
        "OBJECT_SPACE",
        "SELECTOR_SPACE",
        "UNTRACKED_ROW_SPACE",
        "StorageSpace::mutable",
        "BinaryCasContext",
        "TrackedStateContext",
        "BranchHeadControl",
        "Wave A",
        "Wave B",
        "Wave C",
        "Wave D",
        "Wave E",
        "W0 binding: accepted report-only boundary",
    ]
    for marker in required:
        if marker not in report:
            die(f"report missing required marker: {marker}")

    # Verify the package itself remains report-only and its sums are reproducible.
    allowed = {
        "IMPLEMENTABILITY_MAP.md",
        "MANIFEST.md",
        "README.md",
        "SHA256SUMS",
        "verify_map.py",
    }
    actual = {p.name for p in report_dir.iterdir() if p.is_file()}
    if actual != allowed:
        die(f"unexpected package files: {sorted(actual)!r}")
    sums = {}
    for line in (report_dir / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ", 1)
        sums[name] = digest
    for name in allowed - {"SHA256SUMS"}:
        actual_digest = hashlib.sha256((report_dir / name).read_bytes()).hexdigest()
        if sums.get(name) != actual_digest:
            die(f"checksum mismatch for {name}")

    print("PASS: exact W0/b484 provenance, two-path source scope, report markers, and package checksums")
    print("UNRUN: Cargo/compiler/runtime gates; this verifier is static only")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
