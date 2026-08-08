#!/usr/bin/env python3
"""Static, report-only provenance and W4 source-boundary verifier."""

from __future__ import annotations

import argparse
import hashlib
import pathlib
import subprocess
import sys


CANDIDATE = "b484e20d845aee3f8137bfa3496f9b3cd0e8cd35"
TREE = "4477c83b246bddac09cd972564bd4ccd67f90f7b"
PARENT = "fd2be256d763f17e9f127d4c984e36fba191cb82"


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
        fail("b484 object mismatch")
    if git(repo, "rev-parse", f"{CANDIDATE}^{{tree}}") != TREE:
        fail("b484 tree mismatch")
    if git(repo, "rev-parse", f"{CANDIDATE}^") != PARENT:
        fail("b484 parent mismatch")

    source_paths = set(
        git(repo, "ls-tree", "-r", "--name-only", CANDIDATE, "packages/lix/src").splitlines()
    )
    if "packages/lix/src/binary_cas/kv.rs" in source_paths:
        fail("legacy binary_cas/kv.rs unexpectedly exists")
    required_paths = {
        "packages/lix/src/forktree/blob.rs",
        "packages/lix/src/forktree/publication.rs",
        "packages/lix/src/session/media_upload.rs",
        "packages/lix/src/sql2/providers/file.rs",
        "packages/lix/src/transaction/commit.rs",
        "packages/lix/src/transaction/types.rs",
    }
    if not required_paths <= source_paths:
        fail("required W4 source path is absent")

    blob = git(repo, "show", f"{CANDIDATE}:packages/lix/src/forktree/blob.rs")
    publication = git(repo, "show", f"{CANDIDATE}:packages/lix/src/forktree/publication.rs")
    commit = git(repo, "show", f"{CANDIDATE}:packages/lix/src/transaction/commit.rs")
    media = git(repo, "show", f"{CANDIDATE}:packages/lix/src/session/media_upload.rs")
    file = git(repo, "show", f"{CANDIDATE}:packages/lix/src/sql2/providers/file.rs")
    for text, markers in [
        (blob, ["CanonicalBlobIdBuilder", "prepare_upload_completion", "load_blob_ranges_many"]),
        (publication, ["publish_new_upload", "abort_upload", "publish_completed_upload"]),
        (commit, ["reject_not_yet_lowered_cohorts", "file payload publication"]),
        (media, ["UPLOAD_STATE_SPACE", "FILE_UPLOAD_PART_BYTES", "stage_atomic_cas_publication"]),
        (file, ["execute_fast_lix_file_path_writes", "stage_lix_file_content_blob_ref_write"]),
    ]:
        for marker in markers:
            if marker not in text:
                fail(f"source marker absent: {marker}")

    report_dir = pathlib.Path(__file__).resolve().parent
    required_report_markers = [
        "OBJECT_SPACE",
        "SELECTOR_SPACE",
        "lix_binary_blob_ref",
        "16 MiB",
        "1 MiB",
        "W4-A",
        "W4-B",
        "W4-C",
        "W4-D",
        "W5",
        "UNRUN",
    ]
    report = (report_dir / "W4_FILE_BLOB_UPLOAD_MAP.md").read_text()
    for marker in required_report_markers:
        if marker not in report:
            fail(f"report marker absent: {marker}")

    allowed = {
        "W4_FILE_BLOB_UPLOAD_MAP.md",
        "MANIFEST.md",
        "README.md",
        "SHA256SUMS",
        "verify_map.py",
    }
    actual = {p.name for p in report_dir.iterdir() if p.is_file()}
    if actual != allowed:
        fail(f"unexpected package files: {sorted(actual)!r}")
    checksums = {}
    for line in (report_dir / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ", 1)
        checksums[name] = digest
    for name in allowed - {"SHA256SUMS"}:
        digest = hashlib.sha256((report_dir / name).read_bytes()).hexdigest()
        if checksums.get(name) != digest:
            fail(f"checksum mismatch: {name}")

    package_delta = git(repo, "diff", "--name-only", f"{CANDIDATE}..HEAD").splitlines()
    if any(path.startswith("packages/lix/src/") for path in package_delta):
        fail("package branch changed production source")
    print("PASS: exact b484 provenance, ForkTree W4 seams, legacy boundary, and report checksums")
    print("UNRUN: compiler, Memory, RocksDB, SlateDB, and W5 runtime gates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
