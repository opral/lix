#!/usr/bin/env python3
"""Pass/fail package verifier; it does not execute the expected RED gate."""

from __future__ import annotations

import argparse
import hashlib
import pathlib
import subprocess
import sys

CANDIDATE = "e1af471b9ab0f598dafa7c2ddec7867667c81740"
TREE = "bfa0d271a723da8250ab76ada16fda90926f1099"
PARENT = "b484e20d845aee3f8137bfa3496f9b3cd0e8cd35"


def git(repo: pathlib.Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def fail(message: str) -> None:
    print(f"FAIL: {message}", file=sys.stderr)
    raise SystemExit(1)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=pathlib.Path, required=True)
    args = parser.parse_args()
    repo = args.repo.resolve()
    if git(repo, "rev-parse", CANDIDATE) != CANDIDATE:
        fail("candidate object mismatch")
    if git(repo, "rev-parse", f"{CANDIDATE}^{{tree}}") != TREE:
        fail("candidate tree mismatch")
    if git(repo, "rev-parse", f"{CANDIDATE}^") != PARENT:
        fail("candidate parent mismatch")

    report_dir = pathlib.Path(__file__).resolve().parent
    report = (report_dir / "W4_READINESS_E1AF.md").read_text()
    for marker in [
        "one operation-owned CoherentView",
        "one PreparedPublication",
        "one existing transaction prepare_write_set",
        "lix_binary_blob_ref",
        "OBJECT_SPACE",
        "SELECTOR_SPACE",
        "16 MiB",
        "1 MiB",
        "source gate is intentionally calibrated **RED**",
        "Memory -> RocksDB -> SlateDB",
        "W5",
    ]:
        if marker not in report:
            fail(f"missing report marker: {marker}")

    allowed = {
        "W4_READINESS_E1AF.md",
        "README.md",
        "MANIFEST.md",
        "source_gate_red.sh",
        "source_gate_expected.log",
        "SHA256SUMS",
        "verify_package.py",
    }
    actual = {item.name for item in report_dir.iterdir() if item.is_file()}
    if actual != allowed:
        fail(f"unexpected package files: {sorted(actual)!r}")
    sums = {}
    for line in (report_dir / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ", 1)
        sums[name] = digest
    for name in allowed - {"SHA256SUMS"}:
        digest = hashlib.sha256((report_dir / name).read_bytes()).hexdigest()
        if sums.get(name) != digest:
            fail(f"checksum mismatch: {name}")

    # This package is report-only; production paths may not be introduced by its
    # commit after the exact candidate.
    changed = git(repo, "diff", "--name-only", f"{CANDIDATE}..HEAD").splitlines()
    if any(path.startswith("packages/lix/src/") for path in changed):
        fail("package commit changes production source")
    print("PASS: exact e1af provenance, W4 contract markers, report scope, and checksums")
    print("EXPECTED-RED: source_gate_red.sh must exit 1 on e1af")
    print("UNRUN: compiler, Memory, RocksDB, SlateDB, and adapter runtime")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
