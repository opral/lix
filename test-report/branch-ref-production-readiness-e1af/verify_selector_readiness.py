#!/usr/bin/env python3
"""Selector-specific, candidate-aware BranchRef readiness gate.

The base and candidate are explicit Git identities.  The e1af tree is only a
calibration input; the candidate tree is always scanned independently.  A
future candidate can turn GREEN only after the complete legacy selector /
BranchHead authority closure is gone.
"""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys
import tempfile


V4_HEAD = "32200a21f4cb7a77276ff619179b2c05687ffd2a"
V4_TREE = "5eef6528fcde96417c0303d3c1df78c48e257ffb"
V4_REPORT_SHA = "3cca3a49a4578720dfac22b59e2412916fa90a73cecdf5c9aa5daaa4d4aedec4"
FIXTURE_DIR = pathlib.Path(__file__).with_name("fixtures")
V4_REPORT_PATH = FIXTURE_DIR / "approved_v4_report.md"

LEGACY_PATTERNS: dict[str, str] = {
    "branch_head_control": r"BranchHeadControl|BranchHeadControlContext|BranchHeadTrackedReachability",
    "branch_head_cache": r"BranchHeadControlCache|CachingBranchRefReader|branch_head_control_cache",
    "branch_ref_reader": r"BranchRefReader|BranchRefContext|BranchRefStoreReader",
    "branch_ref_fallback": r"fallback_branch_ref|legacy_branch_ref|branch_ref_fallback|raw_branch_ref|BranchRefFallback|BranchHeadFallback|SecondBranchAuthority|DualBranchAuthority|DualSelectorAuthority",
    "branch_ref_stage": r"branch_ref_stage_row|branch_ref_tombstone_row|BRANCH_REF_SCHEMA_KEY",
    "mutation_revision": r"stage_branch_head_control|branch_head_control_precondition|stage_mutation_revision|MUTATION_REVISION_SPACE|TRACKED_MUTATION_REVISION_SPACE",
    "tracked_generation": r"TrackedHead|current.?generation|untracked_lifecycle_generation|stage_untracked_generation|next_current_state_revision",
    "raw_authority": r"BranchRefAuthority|BranchRefWriter|SecondSelectorAuthority|raw_selector_authority|branch_ref_reader_cache",
}

LEGACY_PATHS = (
    "packages/lix/src/branch/refs.rs",
    "packages/lix/src/branch/context.rs",
    "packages/lix/src/branch/stage_rows.rs",
    "packages/lix/src/sql2/branch_ref.rs",
)

REQUIRED_OWNERS = (
    "GlobalSelectorV1",
    "BranchSelectorV1",
    "CoherentView",
    "PreparedPublication",
    "open_coherent_view_on_read",
    "SELECTOR_SPACE",
    "global_selector_key",
    "branch_selector_key",
    "ForkTreeReadFacade",
    "StorageRead",
    "from_branch_view",
    "from_global_epoch",
)
AUTHORIZED_PRODUCTION_PREFIXES = (
    "packages/lix/src/forktree/",
    "packages/lix/src/schema/builtin/lix_branch_ref.json",
    "packages/lix/src/schema/builtin/lix_branch_descriptor.json",
    "packages/lix/src/schema/builtin/mod.rs",
    "packages/lix/src/sql2/bind/table.rs",
    "packages/lix/src/sql2/catalog/registry.rs",
    "packages/lix/src/sql2/catalog/entity_surface.rs",
    "packages/lix/src/sql2/read_only.rs",
    "packages/lix/src/engine.rs",
)
V4_MODEL_RESULT = (
    "packages/lix/tests/branch_ref_whole_closure_oracle_b59/SOURCE_GATE_RESULT.md"
)
REPORT_ALLOWED_PREFIXES = (
    "test-report/branch-ref-production-readiness-e1af/",
    "test-report/forktree-w3-e1af-selector-rebind/",
)


def git(root: pathlib.Path, *args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "git command failed")
    return result.stdout


def verify_identity(root: pathlib.Path, commit: str, label: str) -> str:
    try:
        actual = git(root, "rev-parse", "--verify", f"{commit}^{{commit}}").strip()
        tree = git(root, "show", "-s", "--format=%T", commit).strip()
    except RuntimeError as error:
        raise RuntimeError(f"{label} identity unavailable: {error}") from error
    if actual != commit:
        raise RuntimeError(f"{label} commit mismatch: {actual} != {commit}")
    print(f"{label}_commit={actual}")
    print(f"{label}_tree={tree}")
    return tree


ORACLE_PATHSPEC = ":(exclude)packages/lix/tests/branch_ref_whole_closure_oracle_b59/**"
WORKSPACE_PATHS = ("packages", "plugins", "crates", "src")


def grep_lines(root: pathlib.Path, commit: str, pattern: str) -> list[str]:
    result = subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "grep",
            "-n",
            "-E",
            pattern,
            commit,
            "--",
            *WORKSPACE_PATHS,
            ORACLE_PATHSPEC,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode not in (0, 1):
        raise RuntimeError(result.stderr.strip() or "git grep failed")
    return result.stdout.splitlines()


def source_text(root: pathlib.Path, commit: str) -> str:
    return "\n".join(file_text(root, commit, path) for path in code_paths(root, commit))


DERIVED_ONLY_PATHS = {
    "packages/lix/src/schema/builtin/lix_branch_ref.json",
    "packages/lix/src/schema/builtin/lix_branch_descriptor.json",
    "packages/lix/src/schema/builtin/mod.rs",
    "packages/lix/src/sql2/bind/table.rs",
    "packages/lix/src/sql2/catalog/registry.rs",
    "packages/lix/src/sql2/catalog/entity_surface.rs",
    "packages/lix/src/sql2/read_only.rs",
    "packages/lix/src/engine.rs",
}


def code_paths(root: pathlib.Path, commit: str) -> list[str]:
    result = subprocess.run(
        ["git", "-C", str(root), "ls-tree", "-r", "--name-only", commit],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "cannot enumerate package sources")
    suffixes = (".rs", ".toml", ".json", ".js", ".mjs", ".ts")
    return [
        path
        for path in result.stdout.splitlines()
        if path.startswith(tuple(f"{prefix}/" for prefix in WORKSPACE_PATHS))
        and path.endswith(suffixes)
        and not path.startswith("packages/lix/tests/branch_ref_whole_closure_oracle_b59/")
    ]


def projection_files(root: pathlib.Path, commit: str) -> list[str]:
    paths = code_paths(root, commit)
    result: list[str] = []
    for path in paths:
        text = file_text(root, commit, path)
        if "lix_branch_ref" in text:
            result.append(path)
    return result


def non_derived_projection_files(paths: list[str]) -> list[str]:
    result = []
    for path in paths:
        if path.startswith(("packages/lix/tests/", "packages/engine-benchmarks/", "packages/rs-sdk-tests/")):
            continue
        if path not in DERIVED_ONLY_PATHS:
            result.append(path)
    return result


def file_text(root: pathlib.Path, commit: str, path: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), "show", f"{commit}:{path}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"missing {path} at {commit}")
    return result.stdout


def run_compiled_fixtures() -> list[str]:
    errors: list[str] = []
    fixture = FIXTURE_DIR / "selector_readiness_fixtures.rs"
    if not fixture.is_file():
        return ["missing compiled selector fixture"]
    with tempfile.TemporaryDirectory(prefix="branch-ref-fixture-") as directory:
        binary = pathlib.Path(directory) / "selector-fixtures"
        compile_result = subprocess.run(
            [
                "rustc",
                "--edition=2021",
                "--test",
                "-D",
                "warnings",
                str(fixture),
                "-o",
                str(binary),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if compile_result.returncode != 0:
            return [f"compiled fixture failed to compile: {compile_result.stderr.strip()}"]
        run_result = subprocess.run(
            [str(binary), "--exact", "--test-threads=1"],
            check=False,
            capture_output=True,
            text=True,
        )
        if run_result.returncode != 0:
            errors.append(f"compiled fixture failed: {run_result.stdout}{run_result.stderr}")
    return errors


def verify_v4_report(errors: list[str]) -> None:
    try:
        report_bytes = V4_REPORT_PATH.read_bytes()
    except OSError as error:
        errors.append(f"approved v4 report unavailable: {error}")
        return
    import hashlib

    actual_sha = hashlib.sha256(report_bytes).hexdigest()
    if actual_sha != V4_REPORT_SHA:
        errors.append(f"approved v4 report hash mismatch: {actual_sha} != {V4_REPORT_SHA}")
    report = report_bytes.decode("utf-8")
    for required in (V4_HEAD, V4_TREE, "15/15 model tests", "Terminal verdict: **APPROVE**"):
        if required not in report:
            errors.append(f"approved v4 report is missing bound bytes: {required}")


def verify_changed_path_allowlist(root: pathlib.Path, base: str, candidate: str) -> list[str]:
    errors: list[str] = []
    changed = git(root, "diff", "--name-only", base, candidate)
    for path in changed.splitlines():
        if path.startswith(REPORT_ALLOWED_PREFIXES):
            continue
        if any(path == allowed or path.startswith(allowed) for allowed in AUTHORIZED_PRODUCTION_PREFIXES):
            continue
        errors.append(f"candidate path outside BranchRef/report allowlist: {path}")
    return errors


def verify_structural_owner_contract(source: str) -> list[str]:
    errors: list[str] = []
    publication_calls = list(
        re.finditer(
            r"PreparedPublication::from_branch_view\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*([A-Za-z_][A-Za-z0-9_]*)\s*,",
            source,
        )
    )
    if len(publication_calls) != 1:
        errors.append("expected exactly one structured PreparedPublication::from_branch_view call")
    else:
        read_alias, view_alias = publication_calls[0].groups()
        if not re.search(
            rf"compare_and_swap\(\s*{re.escape(read_alias)}\s*,\s*prepared\s*\)",
            source,
        ):
            errors.append("CAS does not use the exact publication read alias")
        if not re.search(rf"from_branch_view\(\s*{re.escape(read_alias)}\s*,\s*{re.escape(view_alias)}\s*,", source):
            errors.append("publication view/read arguments are not structurally paired")
    for field in ("owner", "expected_epoch", "generation", "compare_and_swap"):
        if field not in source:
            errors.append(f"owner/epoch/generation CAS field is absent: {field}")
    return errors


def main() -> int:
    if len(sys.argv) != 5:
        print(
            "usage: verify_selector_readiness.py <base-root> <base-commit> "
            "<candidate-root> <candidate-commit>",
            file=sys.stderr,
        )
        return 2

    base_root = pathlib.Path(sys.argv[1]).resolve()
    base_commit = sys.argv[2]
    candidate_root = pathlib.Path(sys.argv[3]).resolve()
    candidate_commit = sys.argv[4]
    errors: list[str] = []
    try:
        base_tree = verify_identity(base_root, base_commit, "base")
        candidate_tree = verify_identity(candidate_root, candidate_commit, "candidate")
        v4_tree = verify_identity(candidate_root, V4_HEAD, "approved_v4")
        if v4_tree != V4_TREE:
            errors.append(f"approved v4 tree mismatch: {v4_tree} != {V4_TREE}")
        v4_result = file_text(candidate_root, V4_HEAD, V4_MODEL_RESULT)
        if "15/15" not in v4_result or "PASS" not in v4_result:
            errors.append("approved v4 does not contain the required 15/15 PASS model result")
        else:
            print("approved_v4_model=15/15 PASS")
        if subprocess.run(
            ["git", "-C", str(candidate_root), "merge-base", "--is-ancestor", base_commit, candidate_commit],
            check=False,
        ).returncode != 0:
            errors.append("candidate is not descended from the explicit base commit")
        verify_v4_report(errors)
        errors.extend(verify_changed_path_allowlist(candidate_root, base_commit, candidate_commit))
    except RuntimeError as error:
        print(f"SOURCE_GATE=ERROR {error}")
        return 2

    print(f"approved_v4_report_sha256={V4_REPORT_SHA}")
    print(f"base_tree={base_tree}")
    print(f"candidate_tree={candidate_tree}")

    for name, pattern in LEGACY_PATTERNS.items():
        base_count = len(grep_lines(base_root, base_commit, pattern))
        candidate_count = len(grep_lines(candidate_root, candidate_commit, pattern))
        delta = candidate_count - base_count
        print(f"legacy.{name}.base={base_count}")
        print(f"legacy.{name}.candidate={candidate_count}")
        print(f"legacy.{name}.delta={delta}")
        if delta > 0:
            errors.append(f"legacy authority increased: {name} (+{delta})")
        if candidate_count != 0:
            errors.append(f"legacy authority remains: {name} ({candidate_count})")

    for path in LEGACY_PATHS:
        base_present = subprocess.run(
            ["git", "-C", str(base_root), "cat-file", "-e", f"{base_commit}:{path}"],
            check=False,
        ).returncode == 0
        candidate_present = subprocess.run(
            ["git", "-C", str(candidate_root), "cat-file", "-e", f"{candidate_commit}:{path}"],
            check=False,
        ).returncode == 0
        print(f"legacy_path.{path}.base={int(base_present)}")
        print(f"legacy_path.{path}.candidate={int(candidate_present)}")
        if candidate_present:
            errors.append(f"legacy closure path remains: {path}")
        if candidate_present and not base_present:
            errors.append(f"legacy closure path introduced: {path}")

    candidate_source = source_text(candidate_root, candidate_commit)
    calibration = base_commit == candidate_commit
    if calibration:
        for token in REQUIRED_OWNERS:
            count = candidate_source.count(token)
            print(f"required_owner.{token}={count}")
    else:
        for error in verify_structural_owner_contract(candidate_source):
            errors.append(error)

    candidate_projection = projection_files(candidate_root, candidate_commit)
    candidate_non_derived = non_derived_projection_files(candidate_projection)
    print(f"lix_branch_ref_occurrence_files={len(candidate_projection)}")
    print(f"non_derived_lix_branch_ref_files={len(candidate_non_derived)}")
    for path in candidate_non_derived:
        errors.append(f"lix_branch_ref is not derived-only: {path}")

    for error in run_compiled_fixtures():
        errors.append(f"fixture: {error}")

    if errors:
        print("SOURCE_GATE=RED")
        for error in errors:
            print(f"RED: {error}")
        return 1
    print("SOURCE_GATE=GREEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
