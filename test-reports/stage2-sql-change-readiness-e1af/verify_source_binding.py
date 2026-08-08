#!/usr/bin/env python3
"""Small read-only binding gate; e1af is deliberately expected to be RED.

The frozen v4 verifier remains authoritative for balanced call arguments and
transitive closure. This binding adds the concrete providers/diff.py consumer
and the exact forbidden legacy routes it currently exposes.
"""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys


ALLOWED = {
    "packages/lix/src/sql2/context.rs",
    "packages/lix/src/sql2/providers/change.rs",
    "packages/lix/src/sql2/providers/diff.rs",
    "packages/lix/src/sql2/exec/datafusion.rs",
    "packages/lix/src/session/context.rs",
    "packages/lix/src/transaction/context.rs",
    "packages/lix/src/forktree/view.rs",
    "packages/lix/src/forktree/serving.rs",
    "packages/lix/src/forktree/mod.rs",
}

BASE_COMMIT = "e1af471b9ab0f598dafa7c2ddec7867667c81740"
FIXTURE_DIR = pathlib.Path(__file__).with_name("fixtures")


def function_body(text: str, name: str) -> str | None:
    """Return one Rust function body using balanced braces.

    This is intentionally a small source gate, not a Rust parser. It skips
    quoted strings and comments while balancing braces so a fixture cannot
    satisfy the gate by placing an unrelated token in another function.
    """
    match = re.search(rf"\bfn\s+{re.escape(name)}\s*[^{{]*{{", text)
    if match is None:
        return None
    opening = text.find("{", match.start(), match.end())
    depth = 0
    index = opening
    quote: str | None = None
    escaped = False
    line_comment = False
    block_comment = False
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
                return text[opening + 1 : index]
        index += 1
    return None


def changed_paths(root: pathlib.Path) -> tuple[list[str], list[str]]:
    """Return changed paths and diagnostics for BASE_COMMIT..HEAD."""
    try:
        subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--verify", f"{BASE_COMMIT}^{{commit}}"],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        return [], [f"candidate does not contain base commit {BASE_COMMIT}"]

    result = subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "diff",
            "--name-status",
            "--find-renames",
            f"{BASE_COMMIT}..HEAD",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return [], [f"cannot inspect base..candidate diff: {result.stderr.strip()}"]

    paths: list[str] = []
    for line in result.stdout.splitlines():
        fields = line.split("\t")
        if len(fields) >= 2:
            # A rename/copy has old and new paths; both are scope-relevant.
            paths.extend(fields[1:])
    return paths, []


def scope_errors(paths: list[str]) -> list[str]:
    return [
        f"base..candidate changed out-of-scope path: {path}"
        for path in paths
        if path not in ALLOWED
    ]


def diff_reader_identity_errors(diff: str) -> list[str]:
    """Prove the diff chronology receiver is the propagated query reader.

    The accepted shape carries the exact query_source.forktree_reader into
    DiffFunction, clones that field into DiffSpec, and places that clone in
    the first position of the scan closure tuple. Every chronology call in
    that closure must use the corresponding first closure parameter. This
    rejects a body that merely mentions the right field while calling a
    different reader argument.
    """
    errors: list[str] = []
    registration = function_body(diff, "register_diff_function")
    call = function_body(diff, "call")
    plan = function_body(diff, "plan_scan")
    if registration is None:
        return ["diff.rs has no register_diff_function body"]
    if call is None:
        errors.append("diff.rs has no DiffFunction::call body")
    if plan is None:
        errors.append("diff.rs has no DiffSpec::plan_scan body")

    if not re.search(
        r"forktree_reader\s*:\s*query_source\.forktree_reader(?:\.clone\(\))?",
        registration,
    ):
        errors.append(
            "diff registration does not bind DiffFunction to query_source.forktree_reader"
        )
    if re.search(r"\bstore\s*:|query_source\.store", registration):
        errors.append("diff registration retains a raw store field")
    if call is not None and not re.search(
        r"forktree_reader\s*:\s*self\.forktree_reader\.clone\(\)", call
    ):
        errors.append("DiffFunction::call does not propagate its reader into DiffSpec")

    if plan is None:
        return errors
    if "query_source" in plan:
        errors.append("diff plan body reaches query_source instead of its owned reader")
    if "ForkTreeReadFacade::new" in plan or ".begin_read(" in plan:
        errors.append("diff plan body acquires a second reader/facade")

    direct_reader = "self.forktree_reader.clone()"
    alias_match = re.search(
        r"let\s+(?P<alias>[A-Za-z_]\w*)\s*=\s*self\.forktree_reader\.clone\(\)",
        plan,
    )
    tuple_match = re.search(
        r"\(\s*(?P<reader>self\.forktree_reader\.clone\(\)|[A-Za-z_]\w*)\s*,",
        plan,
    )
    if tuple_match is None:
        errors.append("diff scan closure has no reader-first tuple binding")
        return errors
    tuple_reader = tuple_match.group("reader")
    if tuple_reader != direct_reader and (
        alias_match is None or tuple_reader != alias_match.group("alias")
    ):
        errors.append(
            "diff scan tuple reader is not the exact self.forktree_reader identity"
        )

    closure_match = re.search(r"move\s*\|\s*\(\s*([^)]*)\)\s*\|", plan)
    if closure_match is None:
        errors.append("diff scan has no destructured reader closure")
        return errors
    first_parameter = closure_match.group(1).split(",", 1)[0].strip()
    if not first_parameter or not re.fullmatch(r"[A-Za-z_]\w*", first_parameter):
        errors.append("diff scan closure first parameter is not a named reader")
        return errors

    chronology_methods = (
        "scan_state_rows_at_commit",
        "load_commit_records",
        "load_commit_member_records",
        "load_commit_topology",
    )
    receiver_pattern = (
        r"(?P<receiver>[A-Za-z_]\w*)\s*\.\s*(?:"
        + "|".join(chronology_methods)
        + r")\s*\("
    )
    receivers = [match.group("receiver") for match in re.finditer(receiver_pattern, plan)]
    if len(receivers) < 2:
        errors.append("diff scan has fewer than two authenticated chronology calls")
    for receiver in receivers:
        if receiver != first_parameter:
            errors.append(
                "diff chronology call receiver is not the reader-first closure identity: "
                f"{receiver} != {first_parameter}"
            )
    return errors


def run_negative_fixtures() -> list[str]:
    errors: list[str] = []
    escape = FIXTURE_DIR / "allowed_path_escape.txt"
    mismatch = FIXTURE_DIR / "diff_reader_argument_mismatch.rs"
    if not escape.is_file() or not mismatch.is_file():
        return ["required negative fixture is missing"]
    escaped_paths = [line.strip() for line in escape.read_text(encoding="utf-8").splitlines()]
    if not scope_errors(escaped_paths):
        errors.append("allowed-path escape fixture was accepted")
    if not diff_reader_identity_errors(mismatch.read_text(encoding="utf-8")):
        errors.append("mismatched diff-reader argument fixture was accepted")
    return errors


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: verify_source_binding.py CANDIDATE", file=sys.stderr)
        return 2
    root = pathlib.Path(sys.argv[1]).resolve()
    errors: list[str] = []

    paths, path_errors = changed_paths(root)
    errors.extend(path_errors)
    errors.extend(scope_errors(paths))
    errors.extend(run_negative_fixtures())

    def source(relative: str) -> str:
        path = root / relative
        if not path.is_file():
            errors.append(f"missing source path: {relative}")
            return ""
        return path.read_text(encoding="utf-8")

    context = source("packages/lix/src/sql2/context.rs")
    change = source("packages/lix/src/sql2/providers/change.rs")
    diff = source("packages/lix/src/sql2/providers/diff.rs")
    session = source("packages/lix/src/session/context.rs")
    transaction = source("packages/lix/src/transaction/context.rs")
    dummy = source("packages/lix/src/sql2/exec/datafusion.rs")

    if "forktree_reader" not in context:
        errors.append("ChangelogQuerySource has no forktree_reader field")
    if "&query_source.forktree_reader" not in change:
        errors.append("change provider does not bind both routes to query_source.forktree_reader")
    if "query_source.store" in change:
        errors.append("change provider still consumes query_source.store")
    if "query_source.store" in diff:
        errors.append("diff provider still consumes query_source.store")
    if "ForkTreeReadFacade::new(store)" in diff:
        errors.append("diff provider constructs a second facade from store")
    errors.extend(f"diff identity: {error}" for error in diff_reader_identity_errors(diff))

    for label, text in (("change", change), ("diff", diff)):
        forbidden = (
            "tracked_state::scan_change_records_from_commit_deltas",
            "tracked_state::load_change_record_by_id",
            "COMMIT_CHANGE_ID_SPACE",
            "ChangelogContext::new().reader",
            "ChangelogReader",
            "ChangeScanRequest",
            "ChangeLoadRequest",
            "CommitGraphContext::new().reader",
            ".begin_read(",
        )
        for token in forbidden:
            if token in text:
                errors.append(f"{label} retains forbidden legacy token: {token}")

    for label, text in (("session", session), ("transaction", transaction), ("dummy", dummy)):
        starts = [
            index
            for index in range(len(text))
            if text.startswith("ChangelogQuerySource {", index)
        ]
        if not starts:
            errors.append(f"{label} has no ChangelogQuerySource constructor")
        for start in starts:
            block = text[start : start + 500]
            if "forktree_reader" not in block:
                errors.append(f"{label} changelog constructor lacks forktree_reader")
            if block.count("ForkTreeReadFacade::new") != 1:
                errors.append(
                    f"{label} changelog constructor must have exactly one ForkTreeReadFacade::new"
                )

    if errors:
        print("SOURCE_BINDING=RED")
        for error in errors:
            print(f"RED: {error}")
        return 1
    print("SOURCE_BINDING=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
