#!/usr/bin/env python3
"""Structural one-read/one-facade oracle for the c899 SQL changelog child.

This is a TEST/REPORT-only gate.  It deliberately does not compile or run
Lix.  The c899 calibration is expected to fail because its SQL session creates
separate changelog/history/commit-graph authorities.
"""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys


BASE_COMMIT = "c8992e070a9a988a695bdb77f9a49e214431a5bc"
PACKAGE = "test-reports/stage2-sql-change-one-facade-oracle-c899"
ALLOWED = {
    f"{PACKAGE}/README.md",
    f"{PACKAGE}/REPORT.md",
    f"{PACKAGE}/SHA256SUMS",
    f"{PACKAGE}/verify_one_facade.py",
    f"{PACKAGE}/fixtures/two_facades.rs",
    f"{PACKAGE}/fixtures/separate_history_graph_reader.rs",
    f"{PACKAGE}/fixtures/valid_shared_reader.rs",
}


def rust_functions(text: str, name: str) -> list[tuple[str, str]]:
    """Return (header, body) pairs for concrete Rust functions named name."""
    results: list[tuple[str, str]] = []
    pattern = re.compile(rf"\bfn\s+{re.escape(name)}\b")
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
                    results.append((header, text[opening + 1 : index]))
                    break
            index += 1
    return results


def struct_body(text: str, name: str) -> str | None:
    match = re.search(rf"\bstruct\s+{re.escape(name)}\b[^{{]*{{", text)
    if match is None:
        return None
    opening = text.rfind("{", match.start(), match.end())
    depth = 0
    for index in range(opening, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return text[opening + 1 : index]
    return None


def call_arguments(text: str, needle: str) -> list[str]:
    results: list[str] = []
    for match in re.finditer(re.escape(needle) + r"\s*\(", text):
        opening = text.find("(", match.start(), match.end())
        depth = 0
        quote: str | None = None
        escaped = False
        for index in range(opening, len(text)):
            char = text[index]
            if quote is not None:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == quote:
                    quote = None
                continue
            if char in ('"', "'"):
                quote = char
            elif char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    results.append(text[opening + 1 : index])
                    break
    return results


def changed_paths(root: pathlib.Path) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    check = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--verify", f"{BASE_COMMIT}^{{commit}}"],
        capture_output=True,
        text=True,
    )
    if check.returncode != 0:
        return [], [f"candidate does not contain immutable base {BASE_COMMIT}"]
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
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return [], [f"cannot inspect base..candidate scope: {result.stderr.strip()}"]
    paths: list[str] = []
    for line in result.stdout.splitlines():
        fields = line.split("\t")
        if len(fields) >= 2:
            paths.extend(fields[1:])
    errors.extend(f"out-of-scope base..candidate path: {path}" for path in paths if path not in ALLOWED)
    return paths, errors


def read_source(root: pathlib.Path, relative: str, errors: list[str]) -> str:
    path = root / relative
    if not path.is_file():
        errors.append(f"missing required source: {relative}")
        return ""
    return path.read_text(encoding="utf-8")


def has_query_reader(text: str) -> bool:
    return re.search(r"query_source\s*\.\s*forktree_reader", text) is not None


def one_constructor_errors(text: str, label: str) -> list[str]:
    count = text.count("ForkTreeReadFacade::new")
    if count != 1:
        return [f"{label} constructs {count} ForkTreeReadFacade values; expected exactly one"]
    return []


def context_constructor_errors(text: str, label: str) -> list[str]:
    errors: list[str] = []
    history = rust_functions(text, "history_query_source")
    changelog = rust_functions(text, "changelog_query_source")
    if len(history) != 1:
        errors.append(f"{label} must expose exactly one history_query_source function")
    if len(changelog) != 1:
        errors.append(f"{label} must expose exactly one changelog_query_source function")
    for _, body in history:
        if "ForkTreeReadFacade::new" in body:
            errors.append(f"{label} history_query_source constructs a second facade")
    if changelog:
        errors.extend(one_constructor_errors(changelog[0][1], f"{label} changelog_query_source"))
    return errors


def boundary_errors(session_text: str) -> list[str]:
    errors: list[str] = []
    for function_name, sink in (
        ("build_read_session_with_active_head", "providers::register_read"),
        ("build_transaction_read_session", "providers::register_transaction"),
    ):
        functions = rust_functions(session_text, function_name)
        if len(functions) != 1:
            errors.append(f"sql2/session.rs missing {function_name}")
            continue
        _, body = functions[0]
        matches = re.findall(
            r"let\s+(?P<name>[A-Za-z_]\w*)\s*=\s*\w+\.changelog_query_source\(\)",
            body,
        )
        if len(matches) != 1:
            errors.append(
                f"{function_name} must bind exactly one operation-owned changelog source"
            )
            continue
        source_name = matches[0]
        graph_matches = re.findall(
            r"let\s+(?P<name>[A-Za-z_]\w*)\s*=\s*\w+\.commit_graph\(\)",
            body,
        )
        if len(graph_matches) != 1:
            errors.append(
                f"{function_name} must bind exactly one operation-scoped CommitGraphReader"
            )
        graph_name = graph_matches[0] if len(graph_matches) == 1 else "__missing_graph__"
        diff_calls = call_arguments(body, "providers::register_diff_function")
        if len(diff_calls) != 1 or not re.search(rf"\b{re.escape(source_name)}\b", diff_calls[0]):
            errors.append(f"{function_name} does not pass the bound source to lix_diff")
        sink_calls = call_arguments(body, sink)
        if len(sink_calls) != 1 or not re.search(rf"\b{re.escape(source_name)}\b", sink_calls[0]):
            errors.append(f"{function_name} does not pass the same source into {sink}")
        if len(sink_calls) != 1 or not re.search(rf"\b{re.escape(graph_name)}\b", sink_calls[0]):
            errors.append(f"{function_name} does not pass the shared graph into {sink}")
    return errors


def provider_source_errors(providers: str) -> list[str]:
    errors: list[str] = []
    forbidden = (
        "ctx.changelog_query_source(",
        "ctx.history_query_source(",
        "ctx.commit_graph(",
        "ForkTreeReadFacade::new",
        ".begin_read(",
    )
    read_bodies = rust_functions(providers, "register_read") + rust_functions(
        providers, "register_read_from_catalog"
    )
    transaction = rust_functions(providers, "register_transaction")
    for name, body in read_bodies:
        for token in forbidden:
            if token in body:
                errors.append(f"read provider closure retains forbidden {token}")
        if "query_source" not in body:
            errors.append("read provider closure does not pass the boundary query_source")
        if "commit_graph" not in body:
            errors.append("read provider closure does not pass the shared CommitGraphReader")
    for _, body in transaction:
        for token in forbidden:
            if token in body:
                errors.append(f"transaction read registration retains forbidden {token}")
        if "query_source" not in body:
            errors.append("transaction registration does not pass the boundary query_source")
        if "commit_graph" not in body:
            errors.append("transaction registration does not pass the shared CommitGraphReader")
    for name in ("register_read", "register_read_from_catalog", "register_transaction"):
        headers = [header for header, _ in rust_functions(providers, name)]
        if not any("query_source" in header for header in headers):
            errors.append(f"{name} has no operation-owned query_source parameter")
        if not any("commit_graph" in header for header in headers):
            errors.append(f"{name} has no shared CommitGraphReader parameter")
    catalog_functions = rust_functions(providers, "register_read_from_catalog")
    if catalog_functions:
        catalog_body = catalog_functions[0][1]
        if "history_query_source_for_provider" in catalog_body:
            errors.append("history/working-diff providers construct a separate query source")
        for provider_name in (
            "register_lix_file_history_surface",
            "register_lix_directory_history_surface",
            "register_entity_providers",
            "register_working_diff_provider",
            "register_filesystem_working_diff_provider",
            "register_checkpoint_provider",
        ):
            for args in call_arguments(catalog_body, provider_name):
                if "query_source" not in args:
                    errors.append(
                        f"{provider_name} does not receive the operation-owned query_source"
                    )
    return errors


def history_errors(history_route: str) -> list[str]:
    errors: list[str] = []
    functions = rust_functions(history_route, "load_history_entries")
    if len(functions) != 1:
        return ["history_route.rs missing load_history_entries"]
    header, body = functions[0]
    if "commit_graph" not in header:
        errors.append("history chronology has no shared operation-scoped CommitGraphReader")
    if "query_source" not in header:
        errors.append("history chronology has no caller-owned query_source parameter")
    for token in (
        "CommitGraphContext::new().reader",
        "ForkTreeReadFacade::new",
        ".begin_read(",
        "query_source.store",
    ):
        if token in body:
            errors.append(f"history chronology retains forbidden local authority {token}")
    aliases = {"query_source.forktree_reader"}
    graph_aliases = {"commit_graph"}
    aliases.update(
        match.group(1)
        for match in re.finditer(
            r"let\s+(\w+)\s*=\s*(?:Arc::clone\(&)?query_source\.forktree_reader",
            body,
        )
    )
    graph_aliases.update(
        match.group(1)
        for match in re.finditer(
            r"let\s+(?:mut\s+)?(\w+)\s*=\s*commit_graph(?:\.lock\(\))?",
            body,
        )
    )
    methods = (
        "change_history_from_commit",
        "load_commit_records",
        "load_commit_member_records",
        "load_commit_topologies",
        "scan_commit_records",
    )
    call_pattern = re.compile(
        rf"(?P<receiver>[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\s*\.\s*(?:{'|'.join(methods)})\s*\("
    )
    for match in call_pattern.finditer(body):
        receiver = match.group("receiver")
        if receiver not in aliases and receiver not in graph_aliases:
            errors.append(
                "history chronology receiver is neither the shared graph nor "
                "query_source.forktree_reader: " + receiver
            )
    if not any(method in body for method in methods):
        errors.append("history chronology has no authenticated topology/record call")
    if not has_query_reader(body):
        errors.append("history chronology never consumes the caller-owned ForkTree reader")
    return errors


def context_type_errors(sql_context: str) -> list[str]:
    errors: list[str] = []
    for name in ("HistoryQuerySource", "ChangelogQuerySource"):
        body = struct_body(sql_context, name)
        if body is None:
            errors.append(f"sql2/context.rs missing {name}")
        elif re.search(r"\bstore\s*:", body):
            errors.append(f"{name} retains a raw store authority")
    # One CommitGraphReader capability is explicitly allowed in this narrow
    # correction.  Its W1a deletion/replacement is a separate task.  The
    # boundary and provider checks enforce that it is acquired once and shared.
    return errors


def changed_source_errors(root: pathlib.Path) -> list[str]:
    errors: list[str] = []
    context = read_source(root, "packages/lix/src/sql2/context.rs", errors)
    session = read_source(root, "packages/lix/src/sql2/session.rs", errors)
    providers = read_source(root, "packages/lix/src/sql2/providers/mod.rs", errors)
    history_route = read_source(root, "packages/lix/src/sql2/history_route.rs", errors)
    change = read_source(root, "packages/lix/src/sql2/providers/change.rs", errors)
    diff = read_source(root, "packages/lix/src/sql2/providers/diff.rs", errors)
    session_context = read_source(root, "packages/lix/src/session/context.rs", errors)
    transaction_context = read_source(root, "packages/lix/src/transaction/context.rs", errors)
    datafusion = read_source(root, "packages/lix/src/sql2/exec/datafusion.rs", errors)

    errors.extend(context_type_errors(context))
    errors.extend(boundary_errors(session))
    errors.extend(provider_source_errors(providers))
    errors.extend(history_errors(history_route))
    errors.extend(context_constructor_errors(session_context, "session/context.rs"))
    errors.extend(context_constructor_errors(transaction_context, "transaction/context.rs"))
    errors.extend(context_constructor_errors(datafusion, "sql2/exec/datafusion.rs dummy"))

    forbidden_change = (
        "tracked_state::scan_change_records_from_commit_deltas",
        "tracked_state::load_change_record_by_id",
        "COMMIT_CHANGE_ID_SPACE",
        "ChangelogContext::new().reader",
        "ChangelogReader",
        "ChangeScanRequest",
        "ChangeLoadRequest",
        "CommitGraphContext::new().reader",
        "query_source.store",
        "ForkTreeReadFacade::new",
        ".begin_read(",
        ".begin_write(",
        ".flush(",
    )
    for token in forbidden_change:
        if token in change:
            errors.append(f"change provider retains forbidden {token}")
    if not has_query_reader(change):
        errors.append("change provider never consumes query_source.forktree_reader")
    if "load_exact_change(&query_source.forktree_reader" not in change:
        errors.append("change exact lookup is not bound to query_source.forktree_reader")
    if "scan_changelog_changes(\n                            &query_source.forktree_reader" not in change:
        errors.append("change scan is not bound to query_source.forktree_reader")

    for token in (
        "query_source.store",
        "ForkTreeReadFacade::new",
        ".begin_read(",
        ".begin_write(",
        ".flush(",
        "CommitGraphContext::new().reader",
    ):
        if token in diff:
            errors.append(f"diff provider retains forbidden {token}")
    if not re.search(r"forktree_reader\s*:\s*query_source\s*\.\s*forktree_reader", diff):
        errors.append("diff registration does not bind the boundary reader")
    if "scan_state_rows_at_commit" not in diff:
        errors.append("diff provider has no authenticated ForkTree chronology calls")

    return errors


def negative_fixture_errors(root: pathlib.Path) -> list[str]:
    errors: list[str] = []
    fixture_dir = root / PACKAGE / "fixtures"
    two = fixture_dir / "two_facades.rs"
    graph = fixture_dir / "separate_history_graph_reader.rs"
    valid = fixture_dir / "valid_shared_reader.rs"
    for fixture in (two, graph, valid):
        if not fixture.is_file():
            errors.append(f"missing fixture: {fixture.name}")
    if errors:
        return errors
    two_text = two.read_text(encoding="utf-8")
    if not one_constructor_errors(two_text, "two-facade negative fixture"):
        errors.append("two-facade negative fixture was accepted")
    graph_errors = history_errors(graph.read_text(encoding="utf-8"))
    if not graph_errors:
        errors.append("separate-history-graph negative fixture was accepted")
    valid_text = valid.read_text(encoding="utf-8")
    if one_constructor_errors(valid_text, "positive shared-reader fixture"):
        errors.append("positive shared-reader constructor fixture was rejected")
    if history_errors(valid_text):
        errors.append("positive shared-reader chronology fixture was rejected")
    return errors


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: verify_one_facade.py CANDIDATE", file=sys.stderr)
        return 2
    root = pathlib.Path(sys.argv[1]).resolve()
    _, errors = changed_paths(root)
    errors.extend(changed_source_errors(root))
    errors.extend(negative_fixture_errors(root))
    if errors:
        print("ONE_FACADE_ORACLE=RED")
        for error in errors:
            print(f"RED: {error}")
        return 1
    print("ONE_FACADE_ORACLE=GREEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
