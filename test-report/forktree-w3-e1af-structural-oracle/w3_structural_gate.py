#!/usr/bin/env python3
"""Read-only W3 structural gate and e1af RED calibration."""

from __future__ import annotations

import hashlib
import re
import subprocess
import sys
from pathlib import Path


COMMIT = "e1af471b9ab0f598dafa7c2ddec7867667c81740"
TREE = "bfa0d271a723da8250ab76ada16fda90926f1099"
PARENT = "b484e20d845aee3f8137bfa3496f9b3cd0e8cd35"
PARENT_TREE = "4477c83b246bddac09cd972564bd4ccd67f90f7b"

CALIBRATION = [
    (r"BranchHeadControl|TrackedHead|current.?generation", 58, "legacy_control_generation"),
    (r"checkpoint|recovery|snapshot.?pin|undo|redo", 1139, "checkpoint_history"),
    (r"snapshot.?pin", 16, "snapshot_pin"),
    (r"GlobalSelectorV1|BranchSelectorV1|global.?epoch|selector", 770, "selector_epoch"),
    (
        r"stage_branch_head_control|branch_head_control_precondition|stage_mutation_revision|MUTATION_REVISION_SPACE|TRACKED_MUTATION_REVISION_SPACE",
        24,
        "mutation_revision",
    ),
]


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def source_text(repo: Path) -> str:
    return "\n".join(
        path.read_text()
        for path in sorted((repo / "packages/lix/src").rglob("*.rs"))
    )


def count_git_lines(repo: Path, pattern: str) -> int:
    result = subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "grep",
            "-n",
            "-i",
            "-E",
            pattern,
            COMMIT,
            "--",
            "packages/lix/src/**/*.rs",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    return len(result.stdout.splitlines())


def operation_counts(text: str) -> dict[str, int]:
    return {
        "begin_read": len(re.findall(r"\bbegin_read\s*\(", text)),
        "publication": len(re.findall(r"\bPreparedPublication\b", text)),
        "prepare": len(re.findall(r"\bprepare_write_set\s*\(", text)),
        "commit": len(re.findall(r"\bprepared_commit\.commit\s*\(", text)),
    }


def accepts_one_operation(text: str) -> bool:
    counts = operation_counts(text)
    return counts == {"begin_read": 1, "publication": 1, "prepare": 1, "commit": 1}


def main() -> int:
    repo = Path(sys.argv[1] if len(sys.argv) == 2 else ".").resolve()
    assert git(repo, "rev-parse", f"{COMMIT}^{{commit}}") == COMMIT
    assert git(repo, "show", "-s", "--format=%T", COMMIT) == TREE
    assert git(repo, "show", "-s", "--format=%P", COMMIT) == PARENT
    assert git(repo, "show", "-s", "--format=%T", PARENT) == PARENT_TREE

    package = Path(__file__).resolve().parent
    assert hashlib.sha256((package / "W3_B484_READINESS_MAP.md").read_bytes()).hexdigest() == (
        "d9a6653f5f5f62e476d7dac10a7bcb5377d0642d9365cbd330c13e778841e471"
    )
    assert sum(line.startswith("| W3-") for line in (package / "W3_B484_READINESS_MAP.md").read_text().splitlines()) == 14
    assert sum(
        1
        for line in (package / "DIAGNOSTICS.tsv").read_text().splitlines()
        if line.strip() and not line.startswith("id\t")
    ) == 14

    accepted = """
    fn operation() {
        let view = begin_read();
        let publication = PreparedPublication::new(view);
        let plan = publication.into_storage_plan();
        let prepared = prepare_write_set(plan);
        prepared_commit.commit();
    }
    """
    assert accepts_one_operation(accepted)

    negatives = {
        "second_read": accepted.replace("let view = begin_read();", "let view = begin_read(); let retry = begin_read();"),
        "second_publication": accepted.replace("let publication = PreparedPublication::new(view);", "let publication = PreparedPublication::new(view); let other = PreparedPublication::new(view);") ,
        "second_commit": accepted.replace("prepared_commit.commit();", "prepared_commit.commit(); prepared_commit.commit();"),
    }
    for name, fixture in negatives.items():
        assert not accepts_one_operation(fixture), name
        print(f"PASS negative_{name}")

    text = source_text(repo)
    for pattern, expected, label in CALIBRATION:
        actual = count_git_lines(repo, pattern)
        assert actual == expected, (label, actual, expected)
        print(f"{label}\t{actual}")

    assert "CoherentView" in text
    assert "PreparedPublication" in text
    assert "advance_gc" in text
    assert "abort_corrupt_gc" in text
    assert "commit_progress" in text
    print("PASS embedded_map_and_diagnostics")
    print("RED e1af legacy W3 source frontier")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
