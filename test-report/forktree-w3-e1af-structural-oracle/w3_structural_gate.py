#!/usr/bin/env python3
"""Candidate-parametric W3 source, scope, and operation-ownership gate.

This is a TEST/REPORT-only verifier.  It never compiles or runs Lix.  The
frozen e1af counts are a baseline: a candidate may lower them or fail closed,
but may not introduce a new legacy authority or increase any count.

The operation checks intentionally parse function bodies and call arguments,
not token presence alone.  The accepted fixture is a small typed-shape model
of the required production seam; the negative fixtures mutate one ownership,
authority, CAS, plan, or call-count invariant at a time.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


PACKAGE = "test-report/forktree-w3-e1af-structural-oracle"
MAP_FILE = "W3_B484_READINESS_MAP.md"
DIAGNOSTICS_FILE = "DIAGNOSTICS.tsv"
BASELINE_COMMIT = "e1af471b9ab0f598dafa7c2ddec7867667c81740"
BASELINE_TREE = "bfa0d271a723da8250ab76ada16fda90926f1099"
BASELINE_PARENT = "b484e20d845aee3f8137bfa3496f9b3cd0e8cd35"
BASELINE_PARENT_TREE = "4477c83b246bddac09cd972564bd4ccd67f90f7b"
BASELINE_CALIBRATION = {
    "legacy_control_generation": (r"BranchHeadControl|TrackedHead|current.?generation", 58),
    "checkpoint_history": (r"checkpoint|recovery|snapshot.?pin|undo|redo", 1139),
    "snapshot_pin": (r"snapshot.?pin", 16),
    "selector_epoch": (r"GlobalSelectorV1|BranchSelectorV1|global.?epoch|selector", 770),
    "mutation_revision": (
        r"stage_branch_head_control|branch_head_control_precondition|stage_mutation_revision|"
        r"MUTATION_REVISION_SPACE|TRACKED_MUTATION_REVISION_SPACE",
        24,
    ),
}

# These are the legacy authorities named by the W3 map.  They are compared
# line-for-line between exact base and candidate commits and are also used to
# classify each diagnostic cluster.  A token in a test or comment is still a
# residue: W3 deletes the superseded authority rather than preserving a shim.
LEGACY_AUTHORITIES = {
    "BranchHeadControl": r"\bBranchHeadControl(?:Context|Cache)?\b",
    "stage_branch_head_control": r"\bstage_branch_head_control\b",
    "branch_head_control_precondition": r"\bbranch_head_control_precondition\b",
    "BRANCH_HEAD_CONTROL_SPACE": r"\bBRANCH_HEAD_CONTROL_SPACE\b",
    "TrackedHeadContext": r"\bTrackedHeadContext\b",
    "CurrentStateDeltaRef": r"\bCurrentStateDeltaRef\b",
    "CHECKPOINT_RECOVERY_REF_SPACE": r"\bCHECKPOINT_RECOVERY_REF_SPACE\b",
    "stage_recovery_ref_rotation": r"\bstage_recovery_ref_rotation\b",
    "MUTATION_REVISION_SPACE": r"\bMUTATION_REVISION_SPACE\b",
    "TRACKED_MUTATION_REVISION_SPACE": r"\bTRACKED_MUTATION_REVISION_SPACE\b",
    "load_mutation_revision": r"\bload_mutation_revision\w*\b",
    "stage_mutation_revision": r"\bstage_mutation_revision\w*\b",
    "TrackedStateStoreReader": r"\bTrackedStateStoreReader\b",
    "tracked_state_reader": r"\btracked_state_reader\w*\b",
    "BranchRefReader": r"\bBranchRefReader\b",
}

# These patterns cover a newly introduced second authority even when it uses a
# different identifier.  They are deliberately narrow; ordinary prose such as
# "legacy tests" is not an authority match.
SECOND_AUTHORITY_PATTERNS = {
    "legacy_reader_or_store": r"\b(?:Legacy|Fallback|Compatibility|Alternate|Secondary)\w*(?:Reader|Store)\b",
    "legacy_selector_or_authority": r"\b(?:Legacy|Fallback|Compatibility|Alternate|Secondary)\w*(?:Selector|Authority|Publication|Commit)\b",
    "snake_case_fallback_authority": r"\b(?:legacy|fallback|compatibility|alternate|secondary)_(?:reader|store|selector|authority|publication|commit)\b",
}

PLAN_TOKENS = (
    "PreparedPublication",
    "prepare_write_set",
    "into_storage_plan",
    "begin_write",
    ".commit(",
)
FAIL_CLOSED_RE = re.compile(
    r"return\s+(?:Err\b|Err\s*\()|\b(?:Unsupported|Corrupt|Missing|Invalid)(?:Error)?\b"
)
CALL_RE = re.compile(r"(?P<name>[A-Za-z_][\w:]*)(?:\s*::\s*[A-Za-z_]\w*)?\s*\((?P<args>.*?)\)", re.S)
RAW_OR_SECOND_AUTHORITY_RE = re.compile(
    r"\b(?:RawStore|StorageWrite|LegacyReader|FallbackReader|CompatibilityReader|"
    r"AlternateReader|SecondaryReader|ReadCache|ViewCache|legacy_reader|"
    r"fallback_reader|compatibility_reader|alternate_reader|secondary_reader)\b"
)


class GateFailure(RuntimeError):
    pass


@dataclass(frozen=True)
class RustFunction:
    path: str
    name: str
    header: str
    body: str


def run_git(root: Path, *args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        raise GateFailure(
            f"git {' '.join(args)} failed in {root}: {result.stderr.strip()}"
        )
    return result.stdout


def exact_commit(root: Path, commit: str, label: str) -> str:
    resolved = run_git(root, "rev-parse", f"{commit}^{{commit}}").strip()
    if resolved != commit:
        raise GateFailure(f"{label} does not resolve exactly to {commit}: {resolved}")
    return run_git(root, "show", "-s", "--format=%T", commit).strip()


def assert_baseline_identity(root: Path) -> None:
    tree = exact_commit(root, BASELINE_COMMIT, "frozen e1af baseline")
    parent = run_git(root, "show", "-s", "--format=%P", BASELINE_COMMIT).strip()
    parent_tree = run_git(root, "show", "-s", "--format=%T", parent).strip()
    if tree != BASELINE_TREE or parent != BASELINE_PARENT or parent_tree != BASELINE_PARENT_TREE:
        raise GateFailure(
            "frozen e1af identity changed: "
            f"tree={tree}, parent={parent}, parent_tree={parent_tree}"
        )


def require_ancestor(root: Path, ancestor: str, descendant: str, label: str) -> None:
    result = subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", ancestor, descendant],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise GateFailure(f"{label} {ancestor} is not an ancestor of {descendant}")


def commit_file(root: Path, commit: str, path: str) -> str:
    return run_git(root, "show", f"{commit}:{path}", check=False)


def commit_source_files(root: Path, commit: str) -> dict[str, str]:
    paths = run_git(root, "ls-tree", "-r", "--name-only", commit, "--", "packages/lix/src")
    return {
        path: commit_file(root, commit, path)
        for path in paths.splitlines()
        if path.endswith(".rs")
    }


def git_line_count(root: Path, commit: str, pattern: str) -> int:
    result = subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "grep",
            "-n",
            "-i",
            "-E",
            pattern,
            commit,
            "--",
            "packages/lix/src/**/*.rs",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode not in (0, 1):
        raise GateFailure(f"git grep failed for {pattern!r}: {result.stderr.strip()}")
    return len(result.stdout.splitlines())


def source_line_count(files: dict[str, str], pattern: str) -> int:
    r"""Count authority lines with Python regex semantics, including \b/\(?:."""
    compiled = re.compile(pattern, re.IGNORECASE)
    return sum(
        1
        for text in files.values()
        for line in text.splitlines()
        if compiled.search(line)
    )


def verify_frozen_calibration(root: Path) -> list[str]:
    errors: list[str] = []
    for label, (pattern, expected) in BASELINE_CALIBRATION.items():
        actual = git_line_count(root, BASELINE_COMMIT, pattern)
        if actual != expected:
            errors.append(f"frozen e1af {label}: expected {expected}, got {actual}")
        else:
            print(f"baseline_{label}\t{actual}")
    return errors


def parse_diagnostic_paths(package: Path) -> tuple[dict[str, list[str]], list[str]]:
    diagnostics = package / DIAGNOSTICS_FILE
    rows = diagnostics.read_text(encoding="utf-8").splitlines()
    if not rows or rows[0] != "id\tpaths\tcurrent_diagnostic_class\tsole_owner\trequired_cut\tboundary":
        raise GateFailure("DIAGNOSTICS.tsv header is not the frozen schema")
    clusters: dict[str, list[str]] = {}
    for row in rows[1:]:
        if not row.strip():
            continue
        fields = row.split("\t")
        if len(fields) != 6 or not fields[0].startswith("W3-"):
            raise GateFailure(f"malformed diagnostic row: {row}")
        paths = []
        for raw_path in fields[1].split(";"):
            path = raw_path.strip().split(":", 1)[0].split()[0]
            brace = re.fullmatch(r"(.+)\{([^{}]+)\}(.*)", path)
            fragments = (
                [f"{brace.group(1)}{part}{brace.group(3)}" for part in brace.group(2).split(",")]
                if brace
                else [path]
            )
            for fragment in fragments:
                if fragment:
                    paths.append(
                        f"packages/lix/src/{fragment}"
                        if not fragment.startswith("packages/")
                        else fragment
                    )
        clusters[fields[0]] = paths
    if sorted(clusters) != [f"W3-{index:02d}" for index in range(1, 15)]:
        raise GateFailure(f"expected exactly W3-01..W3-14, got {sorted(clusters)}")
    return clusters, sorted({path for paths in clusters.values() for path in paths})


def verify_embedded_artifacts(package: Path) -> tuple[dict[str, list[str]], list[str]]:
    map_path = package / MAP_FILE
    diagnostics_path = package / DIAGNOSTICS_FILE
    map_hash = hashlib.sha256(map_path.read_bytes()).hexdigest()
    diagnostics_hash = hashlib.sha256(diagnostics_path.read_bytes()).hexdigest()
    errors: list[str] = []
    if map_hash != "d9a6653f5f5f62e476d7dac10a7bcb5377d0642d9365cbd330c13e778841e471":
        errors.append(f"W3 map hash changed: {map_hash}")
    if diagnostics_hash != "1d6cb84157c64eed06d5e4a3cc6925b645fd2ddab2c28a901a774aaf55d49126":
        errors.append(f"W3 diagnostics hash changed: {diagnostics_hash}")
    clusters, paths = parse_diagnostic_paths(package)
    if sum(line.startswith("| W3-") for line in map_path.read_text().splitlines()) != 14:
        errors.append("W3 map does not contain exactly 14 cluster rows")
    if len(clusters) != 14:
        errors.append("diagnostics does not contain exactly 14 clusters")
    return clusters, paths


def verify_sums(package: Path) -> list[str]:
    sums_path = package / "SHA256SUMS"
    errors: list[str] = []
    listed: set[str] = set()
    for row in sums_path.read_text(encoding="utf-8").splitlines():
        if not row.strip():
            continue
        fields = row.split("  ", 1)
        if len(fields) != 2 or not re.fullmatch(r"[0-9a-f]{64}", fields[0]):
            errors.append(f"malformed SHA256SUMS row: {row}")
            continue
        relative_path = fields[1]
        listed.add(relative_path)
        file_path = package / relative_path
        if not file_path.is_file():
            errors.append(f"SHA256SUMS names missing file: {relative_path}")
            continue
        actual = hashlib.sha256(file_path.read_bytes()).hexdigest()
        if actual != fields[0]:
            errors.append(f"SHA256SUMS mismatch {relative_path}: {actual}")
    expected = {
        path.relative_to(package).as_posix()
        for path in package.rglob("*")
        if path.is_file() and path.name != "SHA256SUMS" and ".git" not in path.parts
    }
    if listed != expected:
        errors.append(
            f"SHA256SUMS coverage mismatch: missing={sorted(expected - listed)}, "
            f"extra={sorted(listed - expected)}"
        )
    return errors


def changed_paths(root: Path, base_commit: str, candidate_commit: str) -> list[str]:
    result = run_git(
        root,
        "diff",
        "--name-status",
        "--find-renames",
        f"{base_commit}..{candidate_commit}",
    )
    paths: list[str] = []
    for line in result.splitlines():
        fields = line.split("\t")
        if len(fields) >= 2:
            paths.extend(fields[1:])
    return paths


def scope_errors(paths: list[str], allowed_source_paths: list[str]) -> list[str]:
    allowed_sources = set(allowed_source_paths)
    errors: list[str] = []
    for path in paths:
        allowed = path.startswith(PACKAGE + "/") or path in allowed_sources
        if not allowed:
            errors.append(f"out-of-scope candidate path: {path}")
    return errors


def rust_functions(path: str, text: str) -> list[RustFunction]:
    results: list[RustFunction] = []
    pattern = re.compile(r"\bfn\s+([A-Za-z_]\w*)\b")
    for match in pattern.finditer(text):
        opening = text.find("{", match.end())
        semicolon = text.find(";", match.end(), opening if opening >= 0 else len(text))
        if opening < 0 or (semicolon >= 0 and semicolon < opening):
            continue
        depth = 0
        quote: str | None = None
        escaped = False
        line_comment = False
        block_comment = False
        index = opening
        while index < len(text):
            char = text[index]
            next_char = text[index + 1] if index + 1 < len(text) else ""
            if line_comment:
                if char == "\n":
                    line_comment = False
            elif block_comment:
                if char == "*" and next_char == "/":
                    block_comment = False
                    index += 1
            elif quote is not None:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == quote:
                    quote = None
            elif char == "/" and next_char == "/":
                line_comment = True
                index += 1
            elif char == "/" and next_char == "*":
                block_comment = True
                index += 1
            elif char in ('"', "'"):
                quote = char
            elif char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    header = text[match.start() : opening]
                    results.append(RustFunction(path, match.group(1), header, text[opening + 1 : index]))
                    break
            index += 1
    return results


def all_functions(files: dict[str, str]) -> list[RustFunction]:
    return [function for path, text in files.items() for function in rust_functions(path, text)]


def call_count(text: str, pattern: str) -> int:
    return len(re.findall(pattern, text))


def call_arguments(text: str, qualified_name: str) -> list[str]:
    """Return shallow call argument strings for one qualified call name.

    The fixtures deliberately keep call arguments flat.  Matching the actual
    argument text is what prevents a copied/swapped read or selector from
    passing a count-only check.
    """
    escaped = re.escape(qualified_name).replace(r"\:", r"\s*:\s*")
    pattern = re.compile(rf"{escaped}\s*\(([^()]*)\)", re.S)
    return [match.group(1).strip() for match in pattern.finditer(text)]


def exact_call_errors(text: str, label: str) -> list[str]:
    errors: list[str] = []

    reads = call_arguments(text, "open_coherent_view_on_read")
    if len(reads) != 1:
        errors.append(f"{label}: expected one open_coherent_view_on_read, got {len(reads)}")
    elif not re.match(r"^&?\s*read\s*,\s*selector\s*$", reads[0].replace("\n", " ")):
        errors.append(f"{label}: coherent view is not opened from exact caller read+selector")

    publications = call_arguments(text, "PreparedPublication::from_view")
    if len(publications) != 1:
        errors.append(f"{label}: expected one PreparedPublication::from_view, got {len(publications)}")
    elif publications[0].replace("\n", " ") != "&view, selector, owner, epoch":
        errors.append(f"{label}: publication does not bind exact view/selector/owner/epoch")

    cas = call_arguments(text, "publication.bind_selector_epoch_owner_cas")
    if len(cas) != 1:
        errors.append(f"{label}: expected one selector/epoch/owner CAS binding, got {len(cas)}")
    elif cas[0].replace("\n", " ") != "selector, owner, epoch":
        errors.append(f"{label}: CAS does not bind exact selector/owner/epoch")

    plans = call_arguments(text, "publication.into_storage_plan")
    if len(plans) != 1:
        errors.append(f"{label}: expected one complete publication lowering, got {len(plans)}")
    elif plans[0].replace("\n", " ") != "metadata, idempotency":
        errors.append(f"{label}: plan is not complete metadata+idempotency lowering")

    prepares = call_arguments(text, "prepare_write_set")
    if len(prepares) != 1:
        errors.append(f"{label}: expected one prepare_write_set, got {len(prepares)}")
    elif prepares[0].strip() != "plan":
        errors.append(f"{label}: prepare_write_set does not consume exact complete plan")

    commits = re.findall(r"\bprepared\s*\.\s*commit\s*\(\s*\)", text)
    if len(commits) != 1:
        errors.append(f"{label}: expected one prepared.commit(), got {len(commits)}")

    if re.search(r"\b(?:copied|cloned|other|fresh|swapped)_(?:read|view|selector|owner|epoch)\b", text):
        errors.append(f"{label}: copied/swapped/fresh authority alias")
    if re.search(r"\b(?:read|view|selector|owner|epoch)\s*\.\s*(?:clone|to_owned)\s*\(", text):
        errors.append(f"{label}: caller-owned authority was cloned")
    if RAW_OR_SECOND_AUTHORITY_RE.search(text):
        errors.append(f"{label}: raw/second reader-writer/cache/fallback authority")
    if re.search(r"\b(?:begin_write|raw_store\.|legacy_|fallback_|compatibility_|alternate_|secondary_)", text):
        errors.append(f"{label}: forbidden alternate writer/reader/fallback seam")
    return errors


def publication_call_errors(text: str, label: str) -> list[str]:
    errors: list[str] = []
    reads = call_arguments(text, "open_coherent_view_on_read")
    if len(reads) != 1:
        errors.append(f"{label}: expected one open_coherent_view_on_read, got {len(reads)}")
    elif not re.match(r"^&?\s*read\s*,\s*selector\s*$", reads[0].replace("\n", " ")):
        errors.append(f"{label}: coherent view is not opened from exact caller read+selector")
    publications = call_arguments(text, "PreparedPublication::from_view")
    if len(publications) != 1:
        errors.append(f"{label}: expected one PreparedPublication::from_view, got {len(publications)}")
    elif publications[0].replace("\n", " ") != "&view, selector, owner, epoch":
        errors.append(f"{label}: publication does not bind exact view/selector/owner/epoch")
    cas = call_arguments(text, "publication.bind_selector_epoch_owner_cas")
    if len(cas) != 1:
        errors.append(f"{label}: expected one selector/epoch/owner CAS binding, got {len(cas)}")
    elif cas[0].replace("\n", " ") != "selector, owner, epoch":
        errors.append(f"{label}: CAS does not bind exact selector/owner/epoch")
    if "CoherentView" not in text:
        errors.append(f"{label}: missing typed CoherentView binding")
    if "PreparedPublication" not in text:
        errors.append(f"{label}: missing typed PreparedPublication binding")
    if re.search(r"\b(?:read|view|selector|owner|epoch)\s*\.\s*(?:clone|to_owned)\s*\(", text):
        errors.append(f"{label}: caller-owned authority was cloned")
    if re.search(r"\b(?:copied|cloned|other|fresh|swapped)_(?:read|view|selector|owner|epoch)\b", text):
        errors.append(f"{label}: copied/swapped/fresh authority alias")
    if RAW_OR_SECOND_AUTHORITY_RE.search(text):
        errors.append(f"{label}: raw/second reader-writer/cache/fallback authority")
    return errors


def operation_fixture_errors(text: str, label: str) -> list[str]:
    return exact_call_errors(text, label)


def fixture_errors(package: Path) -> list[str]:
    errors: list[str] = []
    fixture_dir = package / "fixtures"
    positive = fixture_dir / "valid_one_operation.rs"
    negatives = {
        "second_read": fixture_dir / "second_read.rs",
        "second_publication": fixture_dir / "second_publication.rs",
        "second_commit": fixture_dir / "second_commit.rs",
        "copied_read": fixture_dir / "copied_read.rs",
        "swapped_view": fixture_dir / "swapped_view.rs",
        "fresh_facade": fixture_dir / "fresh_facade.rs",
        "wrong_selector": fixture_dir / "wrong_selector.rs",
        "wrong_owner": fixture_dir / "wrong_owner.rs",
        "wrong_epoch": fixture_dir / "wrong_epoch.rs",
        "partial_plan": fixture_dir / "partial_plan.rs",
        "raw_writer": fixture_dir / "raw_writer.rs",
        "fallback_cache": fixture_dir / "fallback_cache.rs",
    }
    if not positive.is_file():
        errors.append("missing positive operation fixture")
    else:
        errors.extend(operation_fixture_errors(positive.read_text(), "positive fixture"))
    for label, path in negatives.items():
        if not path.is_file():
            errors.append(f"missing negative fixture: {path.name}")
        elif not operation_fixture_errors(path.read_text(), label):
            errors.append(f"negative fixture was accepted: {label}")
        else:
            print(f"PASS negative_{label}")
    if not errors and positive.is_file():
        print("PASS positive_one_operation")
    return errors


def first_plan_position(text: str) -> int | None:
    positions = [position for token in PLAN_TOKENS if (position := text.find(token)) >= 0]
    return min(positions) if positions else None


def rust_code_only(text: str) -> str:
    """Erase comments and string/character literals before authority checks."""
    output: list[str] = []
    index = 0
    block_comment = False
    line_comment = False
    quote: str | None = None
    escaped = False
    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""
        if line_comment:
            if char == "\n":
                line_comment = False
                output.append(char)
            else:
                output.append(" ")
        elif block_comment:
            if char == "*" and next_char == "/":
                block_comment = False
                output.extend((" ", " "))
                index += 1
            else:
                output.append("\n" if char == "\n" else " ")
        elif quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            output.append("\n" if char == "\n" else " ")
        elif char == "/" and next_char == "/":
            line_comment = True
            output.extend((" ", " "))
            index += 1
        elif char == "/" and next_char == "*":
            block_comment = True
            output.extend((" ", " "))
            index += 1
        elif char in ('"', "'"):
            quote = char
            output.append(" ")
        else:
            output.append(char)
        index += 1
    return "".join(output)


def explicitly_fails_closed_before_plan(text: str) -> bool:
    code = rust_code_only(text)
    close = FAIL_CLOSED_RE.search(code)
    if close is None:
        return False
    plan = first_plan_position(code)
    return plan is None or close.start() < plan


def legacy_hits(text: str) -> list[str]:
    # Keep the historical e1af cluster calibration byte-for-byte comparable.
    # The acceptance decision is structural in operation_source_errors and
    # explicitly-fails-closed bodies; this list is only the frozen RED report.
    return [label for label, pattern in LEGACY_AUTHORITIES.items() if re.search(pattern, text)]


def operation_source_errors(files: dict[str, str]) -> list[str]:
    errors: list[str] = []
    functions = all_functions(files)
    publication_roots = [
        function
        for function in functions
        if function.name == "prepare_forktree_publication_with_parent_heads"
    ]
    if len(publication_roots) != 1:
        errors.append(
            "expected exactly one prepare_forktree_publication_with_parent_heads, "
            f"found {len(publication_roots)}"
        )
    else:
        function = publication_roots[0]
        body = function.body
        if not re.search(r"\bread\s*:\s*", function.header):
            errors.append("publication root does not receive a caller-owned read")
        errors.extend(publication_call_errors(body, "publication root"))
        for token in (".begin_read(", "StorageAdapterReadScope::new", "begin_write(", ".commit("):
            if token in body:
                errors.append(f"publication root contains an independent {token} authority")
        if "CoherentView" not in body:
            errors.append("publication root does not bind a typed CoherentView")
        if "PreparedPublication" not in body:
            errors.append("publication root does not bind a typed PreparedPublication")
        if "PreparedForkTreePlan::Publication" not in body:
            errors.append("publication root does not return the prepared ForkTree plan")

    lowering = [function for function in functions if function.name == "into_storage_plan"]
    if not any(
        len(call_arguments(function.body, "publication.into_storage_plan")) == 1
        and call_arguments(function.body, "publication.into_storage_plan")[0].replace("\n", " ")
        == "metadata, idempotency"
        for function in lowering
    ):
        errors.append("no PreparedPublication::into_storage_plan lowering seam")
    for function in lowering:
        if any(token in function.body for token in (".begin_read(", "begin_write(", ".commit(")):
            errors.append(f"{function.path}:{function.name} mixes lowering with I/O")

    commits = [function for function in functions if function.name == "commit_write_set"]
    if len(commits) != 1:
        errors.append(f"expected exactly one transaction commit_write_set, found {len(commits)}")
    else:
        body = commits[0].body
        if call_count(body, r"\.prepare_write_set\s*\(") != 1:
            errors.append("transaction commit does not prepare exactly once")
        if call_count(body, r"\bprepared\s*\.commit\s*\(") != 1:
            errors.append("transaction commit does not commit exactly once")
        if "begin_read(" in body or "open_coherent_view_on_read" in body:
            errors.append("transaction prepare/commit body acquires an independent read")
        if "PreparedPublication::" in body:
            errors.append("transaction prepare/commit body constructs a second publication")
        if "metadata" not in body or "idempotency" not in body:
            errors.append("transaction prepare/commit body omits metadata/idempotency plan inputs")
        if RAW_OR_SECOND_AUTHORITY_RE.search(body):
            errors.append("transaction prepare/commit body contains raw/second authority")
    return errors


def cluster_errors(
    files: dict[str, str], clusters: dict[str, list[str]], print_status: bool = True
) -> list[str]:
    errors: list[str] = []
    all_functions_by_path = {
        path: rust_functions(path, files.get(path, "")) for path in files
    }
    for cluster, paths in sorted(clusters.items()):
        combined = "\n".join(files.get(path, "") for path in paths)
        hits = legacy_hits(combined)
        if not hits:
            if print_status:
                print(f"cluster_{cluster}\tLOWERED")
            continue
        residual_functions = [
            function
            for path in paths
            for function in all_functions_by_path.get(path, [])
            if legacy_hits(function.body)
        ]
        if residual_functions and all(
            explicitly_fails_closed_before_plan(function.body)
            for function in residual_functions
        ):
            if print_status:
                print(f"cluster_{cluster}\tFAIL_CLOSED\t{','.join(hits)}")
            continue
        errors.append(
            f"{cluster} retains legacy authority without fail-closed-before-plan: "
            f"{','.join(hits)}"
        )
    return errors


def authority_delta_errors(
    base_files: dict[str, str],
    candidate_files: dict[str, str],
) -> list[str]:
    errors: list[str] = []
    for label, pattern in {**LEGACY_AUTHORITIES, **SECOND_AUTHORITY_PATTERNS}.items():
        base_count = source_line_count(base_files, pattern)
        candidate_count = source_line_count(candidate_files, pattern)
        if candidate_count > base_count:
            errors.append(
                f"authority increase {label}: base={base_count}, candidate={candidate_count}"
            )
        if base_count == 0 and candidate_count > 0:
            errors.append(f"new authority {label}: candidate={candidate_count}")
    return errors


def candidate_semantic_errors(
    base_root: Path,
    base_commit: str,
    candidate_root: Path,
    candidate_commit: str,
    clusters: dict[str, list[str]],
) -> tuple[list[str], dict[str, str]]:
    base_files = commit_source_files(base_root, base_commit)
    candidate_files = commit_source_files(candidate_root, candidate_commit)
    errors = authority_delta_errors(base_files, candidate_files)
    for label, (pattern, baseline) in BASELINE_CALIBRATION.items():
        base_count = git_line_count(base_root, base_commit, pattern)
        candidate_count = git_line_count(candidate_root, candidate_commit, pattern)
        if base_count > baseline:
            errors.append(f"base {label} already exceeds frozen e1af baseline: {base_count}>{baseline}")
        if candidate_count > base_count:
            errors.append(f"candidate {label} increases over base: {candidate_count}>{base_count}")
        if candidate_count > baseline:
            errors.append(f"candidate {label} exceeds frozen e1af baseline: {candidate_count}>{baseline}")
        print(f"candidate_{label}\tbase={base_count}\tcandidate={candidate_count}")
    # The exact e1af source is the frozen 14-cluster RED control.  Its old
    # operation shape is intentionally not reclassified into extra findings;
    # every non-control candidate must pass the structural operation graph.
    if candidate_commit != BASELINE_COMMIT:
        errors.extend(operation_source_errors(candidate_files))
    errors.extend(cluster_errors(candidate_files, clusters))
    return errors, candidate_files


def self_test(clusters: dict[str, list[str]]) -> list[str]:
    accepted_commit_source = """
    async fn prepare_forktree_publication_with_parent_heads<R>(
        read: &R, selector: SelectorExpect, owner: OwnerId, epoch: u64
    ) {
        let view: CoherentView<R> = open_coherent_view_on_read(read, selector).await;
        let publication: PreparedPublication =
            PreparedPublication::from_view(&view, selector, owner, epoch);
        publication.bind_selector_epoch_owner_cas(selector, owner, epoch);
        PreparedForkTreePlan::Publication(publication)
    }
    impl PreparedForkTreePlan {
        fn into_storage_plan(self, metadata: Metadata, idempotency: Idempotency) {
            publication.into_storage_plan(metadata, idempotency);
        }
    }
    """
    accepted_storage_source = """
    async fn commit_write_set(
        &self, plan: PreparedForkTreePlan, metadata: Metadata, idempotency: Idempotency
    ) {
        let plan = plan.into_storage_plan(metadata, idempotency);
        let prepared = self.prepare_write_set(plan).await;
        prepared.commit().await;
    }
    """
    files = {
        "packages/lix/src/transaction/commit.rs": accepted_commit_source,
        "packages/lix/src/storage_adapter/context.rs": accepted_storage_source,
    }
    for path in {path for paths in clusters.values() for path in paths}:
        files.setdefault(path, "")
    errors = operation_source_errors(files)
    if errors:
        return ["candidate GREEN self-test rejected accepted source: " + "; ".join(errors)]
    errors.extend(cluster_errors(files, clusters, print_status=True))
    if errors:
        return errors
    print("PASS candidate_green_self_test")
    return []


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--base-commit")
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--candidate-commit")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    package = Path(__file__).resolve().parent
    errors: list[str] = []
    try:
        clusters, allowed_source_paths = verify_embedded_artifacts(package)
        errors.extend(verify_sums(package))
        errors.extend(fixture_errors(package))
        errors.extend(self_test(clusters) if args.self_test else [])
        if not all((args.base_root, args.base_commit, args.candidate_root, args.candidate_commit)):
            if args.self_test:
                if errors:
                    print("W3_GATE=RED")
                    print("\n".join(f"RED: {error}" for error in errors))
                    return 1
                print("W3_GATE=GREEN")
                return 0
            raise GateFailure(
                "--base-root, --base-commit, --candidate-root, and --candidate-commit are required"
            )
        base_root = args.base_root.resolve()
        candidate_root = args.candidate_root.resolve()
        exact_commit(base_root, args.base_commit, "base commit")
        exact_commit(candidate_root, args.candidate_commit, "candidate commit")
        assert_baseline_identity(base_root)
        require_ancestor(base_root, BASELINE_COMMIT, args.base_commit, "e1af baseline")
        require_ancestor(candidate_root, args.base_commit, args.candidate_commit, "base commit")
        errors.extend(verify_frozen_calibration(base_root))
        paths = changed_paths(candidate_root, args.base_commit, args.candidate_commit)
        errors.extend(scope_errors(paths, allowed_source_paths))
        semantic_errors, _ = candidate_semantic_errors(
            base_root, args.base_commit, candidate_root, args.candidate_commit, clusters
        )
        errors.extend(semantic_errors)
    except (GateFailure, OSError, ValueError) as error:
        errors.append(str(error))
    if errors:
        print("W3_GATE=RED")
        for error in errors:
            print(f"RED: {error}")
        return 1
    print("W3_GATE=GREEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
