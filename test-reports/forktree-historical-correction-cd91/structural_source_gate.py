#!/usr/bin/env python3
"""Dependency-free, source-only gate for the cd91 historical correction.

This is deliberately not a Rust build or product/runtime test.  It lexes only
the selected Rust function bodies, balances calls/braces, and proves that the
reader used by each historical seam is the caller-owned `*.forktree_reader`
field (or a local alias proven directly from that field).  It also checks
production-shaped history row fields and runs source-negative fixtures.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


class Gate:
    def __init__(self) -> None:
        self.failed = False

    def check(self, label: str, ok: bool, detail: str) -> None:
        state = "PASS" if ok else "RED"
        print(f"{state}\t{label}\t{detail}")
        self.failed |= not ok


def mask_rust(source: str) -> str:
    """Blank comments and literals while preserving positions/newlines."""

    out = list(source)
    i = 0
    block_depth = 0
    while i < len(source):
        if block_depth:
            if source.startswith("/*", i):
                out[i : i + 2] = "  "
                block_depth += 1
                i += 2
            elif source.startswith("*/", i):
                out[i : i + 2] = "  "
                block_depth -= 1
                i += 2
            else:
                if source[i] != "\n":
                    out[i] = " "
                i += 1
            continue
        if source.startswith("//", i):
            out[i : i + 2] = "  "
            i += 2
            while i < len(source) and source[i] != "\n":
                out[i] = " "
                i += 1
            continue
        if source.startswith("/*", i):
            out[i : i + 2] = "  "
            block_depth = 1
            i += 2
            continue
        raw = re.match(r"r(#+)\"", source[i:])
        if raw:
            opening = raw.group(0)
            closing = '"' + raw.group(1) + "#"  # the final # is fixed below
            # `r#"..."#`, `r##"..."##`, ...
            closing = '"' + raw.group(1) + ""
            end = source.find(closing, i + len(opening))
            end = len(source) if end < 0 else end + len(closing)
            for j in range(i, end):
                if source[j] != "\n":
                    out[j] = " "
            i = end
            continue
        if source[i] in ('"', "'"):
            quote = source[i]
            out[i] = " "
            i += 1
            escaped = False
            while i < len(source):
                ch = source[i]
                if ch == "\n" and quote == "'":
                    break
                if ch == quote and not escaped:
                    out[i] = " "
                    i += 1
                    break
                if ch != "\n":
                    out[i] = " "
                escaped = ch == "\\" and not escaped
                if ch != "\\":
                    escaped = False
                i += 1
            continue
        i += 1
    return "".join(out)


def matching(masked: str, opening: int, left: str, right: str) -> int | None:
    depth = 0
    for i in range(opening, len(masked)):
        if masked[i] == left:
            depth += 1
        elif masked[i] == right:
            depth -= 1
            if depth == 0:
                return i
    return None


def function_body(source: str, name: str) -> str | None:
    masked = mask_rust(source)
    match = re.search(rf"\bfn\s+{re.escape(name)}\b", masked)
    if not match:
        return None
    opening = masked.find("{", match.end())
    if opening < 0:
        return None
    closing = matching(masked, opening, "{", "}")
    if closing is None:
        return None
    return source[opening + 1 : closing]


def struct_body(source: str, name: str) -> str | None:
    masked = mask_rust(source)
    match = re.search(rf"\bstruct\s+{re.escape(name)}\b", masked)
    if not match:
        return None
    opening = masked.find("{", match.end())
    if opening < 0:
        return None
    closing = matching(masked, opening, "{", "}")
    if closing is None:
        return None
    return source[opening + 1 : closing]


def compact(value: str) -> str:
    return re.sub(r"\s+", "", mask_rust(value))


def normalize_expr(value: str) -> str:
    value = re.sub(r"\s+", "", value)
    value = value.strip(";& ")
    changed = True
    while changed:
        changed = False
        for wrapper in (".clone()", ".as_ref()", ".as_deref()"):
            if value.endswith(wrapper):
                value = value[: -len(wrapper)]
                changed = True
        if value.startswith("Arc::clone(&") and value.endswith(")"):
            value = value[len("Arc::clone(&") : -1]
            changed = True
        if value.startswith("std::sync::Arc::clone(&") and value.endswith(")"):
            value = value[len("std::sync::Arc::clone(&") : -1]
            changed = True
        if value.startswith("(") and value.endswith(")"):
            value = value[1:-1]
            changed = True
        value = value.lstrip("&")
    return value


def aliases(body: str) -> dict[str, str]:
    masked = mask_rust(body)
    result: dict[str, str] = {}
    for match in re.finditer(
        r"\blet\s+(?:mut\s+)?([A-Za-z_]\w*)\s*=\s*(.*?);", masked, re.S
    ):
        result[match.group(1)] = normalize_expr(match.group(2))
    return result


def is_owned(expr: str, known: dict[str, str], creation: bool = False) -> bool:
    expr = normalize_expr(expr)
    seen: set[str] = set()
    while expr in known and expr not in seen:
        seen.add(expr)
        expr = normalize_expr(known[expr])
    if re.fullmatch(r"(?:self|provider|query_source)\.forktree_reader", expr):
        return True
    if creation and re.fullmatch(r"transaction\.forktree_reader", expr):
        return True
    if creation and re.fullmatch(r"transaction\.forktree_read_facade\(\)", expr):
        return True
    return False


def call_receivers(body: str, method: str) -> list[str]:
    masked = mask_rust(body)
    pattern = re.compile(
        rf"(?P<receiver>[A-Za-z_]\w*(?:\s*\.\s*[A-Za-z_]\w*)*)"
        rf"\s*\.\s*{re.escape(method)}\s*\("
    )
    return [normalize_expr(match.group("receiver")) for match in pattern.finditer(masked)]


def call_arguments(body: str, name: str) -> list[list[str]]:
    masked = mask_rust(body)
    result: list[list[str]] = []
    for match in re.finditer(rf"\b{re.escape(name)}\s*\(", masked):
        opening = masked.find("(", match.start(), match.end())
        closing = matching(masked, opening, "(", ")")
        if closing is None:
            continue
        args: list[str] = []
        start = opening + 1
        depth = 0
        for i in range(start, closing):
            ch = masked[i]
            if ch in "([{":
                depth += 1
            elif ch in ")]}":
                depth -= 1
            elif ch == "," and depth == 0:
                args.append(body[start:i])
                start = i + 1
        args.append(body[start:closing])
        result.append([normalize_expr(arg) for arg in args])
    return result


def check_owned_seam(
    gate: Gate,
    path: Path,
    function: str,
    method: str,
    free_call: str | None = None,
    creation: bool = False,
) -> None:
    source = path.read_text()
    body = function_body(source, function)
    if body is None:
        gate.check("function-present", False, f"{path}:{function}")
        return
    known = aliases(body)
    receivers = call_receivers(body, method)
    ok_receivers = bool(receivers) and all(is_owned(receiver, known, creation) for receiver in receivers)
    gate.check(
        "chronology-receiver-identity",
        ok_receivers,
        f"{path}:{function} method={method} receivers={receivers}",
    )
    if free_call:
        calls = call_arguments(body, free_call)
        ok_args = bool(calls) and all(args and is_owned(args[0], known, creation) for args in calls)
        gate.check(
            "chronology-call-argument-identity",
            ok_args,
            f"{path}:{function} call={free_call} first_args={[args[0] if args else '' for args in calls]}",
        )


def check_registration(gate: Gate, path: Path, register_name: str, struct_name: str) -> None:
    source = path.read_text()
    registration = function_body(source, register_name)
    spec = struct_body(source, struct_name)
    reg_compact = compact(registration or "")
    spec_compact = compact(spec or "")
    gate.check(
        "caller-owned-reader-field",
        "forktree_reader" in spec_compact and "forktree_reader:" in reg_compact,
        f"{path}:{register_name}/{struct_name}",
    )
    gate.check(
        "no-history-store-extraction",
        "query_source.store" not in reg_compact and "store:query_source.store" not in reg_compact,
        f"{path}:{register_name}",
    )


FORBIDDEN = (
    "ForkTreeReadFacade::new",
    "begin_read(",
    "BranchHeadControlContext",
    "TrackedHeadContext",
    "TrackedStateStoreReader",
    "TrackedStateContext",
    "working_diff_at_head",
)


def check_scoped_fallbacks(gate: Gate, path: Path, function: str) -> None:
    body = function_body(path.read_text(), function)
    compact_body = compact(body or "")
    bad = [token for token in FORBIDDEN if token.replace(" ", "") in compact_body]
    gate.check("no-scoped-legacy-fallback", not bad, f"{path}:{function} bad={bad}")


PRODUCTION_FIELDS: dict[str, list[str]] = {
    "parse_file_history_descriptors": [
        "entry.change.entity_pk",
        "entry.change.snapshot_content",
        "serde_json::from_str",
        "snapshot.id != row_id",
    ],
    "parse_file_history_directories": [
        "entry.change.entity_pk",
        "entry.change.snapshot_content",
        "serde_json::from_str",
        "snapshot.id != row_id",
    ],
    "parse_file_history_blobs": [
        "entry.change.entity_pk",
        "entry.change.snapshot_content",
        "entry.change.file_id",
        "snapshot.id != row_id",
    ],
    "parse_file_history_observed_descriptors": [
        "row.deleted()",
        "row.snapshot_content()",
        "snapshot.id != row_id",
    ],
    "parse_file_history_observed_directories": [
        "row.deleted()",
        "row.snapshot_content()",
        "snapshot.id != row_id",
    ],
    "parse_file_history_observed_blobs": [
        "row.deleted()",
        "row.snapshot_content()",
        "row.file_id()",
        "snapshot.id != row_id",
        "snapshot.blob_hash",
    ],
    "parse_file_history_plugin_state": [
        "entry.change.file_id",
        "entry.change.snapshot_content",
    ],
    "parse_file_history_plugin_owners": [
        "entry.change.file_id",
        "entry.change.snapshot_content",
    ],
    "parse_file_history_observed_plugin_owners": [
        "row.deleted()",
        "row.snapshot_content()",
        "row.file_id()",
    ],
    "load_file_history_blob_bytes": [
        "BlobId::from_hex",
        "blob_reader.load_bytes_many",
        "loaded.len() != request.len()",
    ],
    "validate_file_history_materialization": [
        "live_file_history_plugin_owner",
        "state.plugin_registry",
        "plugin_registry",
        "has_blob",
        "invalid_file_history_state",
    ],
}


def check_production_functions(gate: Gate, file: Path) -> None:
    source = file.read_text()
    for function, required in PRODUCTION_FIELDS.items():
        body = function_body(source, function)
        compact_body = compact(body or "")
        present = body is not None
        missing = [token for token in required if compact(token) not in compact_body]
        # A first-match lookup is not an exact-one authenticated reference.
        if function == "prepare_file_history_rows":
            missing.extend(
                token
                for token in ("state.blobs", "blob.file_id == event.file_id", "blob_hash")
                if compact(token) not in compact_body
            )
            if ".find(|blob|" in compact_body:
                missing.append("no-first-match-blob-lookup")
        gate.check(
            "production-function-fields",
            present and not missing,
            f"{file}:{function} missing={missing}",
        )


def check_exact_blob_contract(gate: Gate, file: Path) -> None:
    source = file.read_text()
    body = function_body(source, "prepare_file_history_rows")
    compact_body = compact(body or "")
    # The successor must make the cardinality decision in this production
    # function; `.find` is explicitly rejected because it hides duplicates.
    exact_one = all(
        token in compact_body
        for token in ("collect", "match", "blob.file_id==event.file_id", "blob_hash")
    ) and ".find(|blob|" not in compact_body
    gate.check("exactly-one-production-BlobRef", exact_one, str(file))
    body = function_body(source, "load_file_history_blob_bytes")
    compact_body = compact(body or "")
    payload = all(
        token in compact_body
        for token in ("BlobId::from_hex", "blob_reader.load_bytes_many", "loaded.len()!=request.len()")
    )
    gate.check("authenticated-production-payload", payload, str(file))


def classify_fixture(row: dict[str, str]) -> str:
    if row["row_deleted"] == "1":
        return "Deletion"
    if row["schema_key"] == "plugin_owner" and row["plugin_registry"] != "1":
        return "FailClosed(MissingPluginRegistry)"
    if not row["entity_pk"] or not row["file_id"]:
        return "FailClosed(MissingIdentity)"
    if row["snapshot"] == "missing" or row["snapshot"] == "malformed":
        return "FailClosed(MissingOrMalformedPayload)"
    if row["snapshot"] == "substituted":
        return "FailClosed(IdentityMismatch)"
    refs = int(row["blob_refs"])
    if row["schema_key"] in ("file", "plugin_owner"):
        if refs != 1:
            return "FailClosed(BlobRefCardinality)"
        if row["payload"] != "present":
            return "FailClosed(MissingBlobPayload)"
    return "Value"


def check_production_fixtures(gate: Gate, fixture_file: Path) -> None:
    rows = []
    for line in fixture_file.read_text().splitlines():
        if not line or line.startswith("case\t"):
            continue
        values = line.split("\t")
        headers = [
            "case",
            "schema_key",
            "row_deleted",
            "entity_pk",
            "file_id",
            "snapshot",
            "blob_refs",
            "payload",
            "plugin_registry",
            "expected",
        ]
        rows.append(dict(zip(headers, values, strict=True)))
    actual = [classify_fixture(row) for row in rows]
    expected = [row["expected"] for row in rows]
    gate.check("production-shaped-history-fixtures", actual == expected, f"cases={len(rows)}")


def check_negative_fixtures(gate: Gate, fixture_dir: Path) -> None:
    valid = fixture_dir / "valid_owner.rs"
    valid_body = function_body(valid.read_text(), "plan_scan") if valid.exists() else None
    if valid_body is None:
        gate.check("positive-source-fixture", False, str(valid))
    else:
        known = aliases(valid_body)
        receivers = call_receivers(valid_body, "latest_checkpoint_for_branch")
        calls = call_arguments(valid_body, "load_rows")
        ok = bool(receivers) and all(is_owned(r, known) for r in receivers)
        ok &= bool(calls) and all(args and is_owned(args[0], known) for args in calls)
        gate.check("positive-source-fixture", ok, str(valid))
    for path in sorted(fixture_dir.glob("*.rs")):
        if path.name == "valid_owner.rs":
            continue
        body = function_body(path.read_text(), "plan_scan")
        if body is None:
            gate.check("negative-source-fixture", False, path.name)
            continue
        known = aliases(body)
        receivers = call_receivers(body, "latest_checkpoint_for_branch")
        calls = call_arguments(body, "load_rows")
        bad_identity = not receivers or not all(is_owned(r, known) for r in receivers)
        if calls:
            bad_identity |= not all(args and is_owned(args[0], known) for args in calls)
        body_compact = compact(body)
        bad_identity |= any(token.replace(" ", "") in body_compact for token in FORBIDDEN)
        gate.check("negative-source-fixture", bad_identity, path.name)


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: structural_source_gate.py SOURCE_ROOT FIXTURE_DIR", file=sys.stderr)
        return 2
    root = Path(sys.argv[1]).resolve()
    fixture_dir = Path(sys.argv[2]).resolve()
    gate = Gate()
    checkpoint = root / "packages/lix/src/sql2/providers/checkpoint.rs"
    filesystem = root / "packages/lix/src/sql2/providers/filesystem_working_diff.rs"
    working = root / "packages/lix/src/sql2/providers/working_diff.rs"
    session = root / "packages/lix/src/session/checkpoint.rs"
    file_history = root / "packages/lix/src/sql2/providers/file_history.rs"

    check_registration(gate, checkpoint, "register_checkpoint_provider", "CheckpointSpec")
    check_registration(
        gate,
        filesystem,
        "register_filesystem_working_diff_provider",
        "FilesystemWorkingDiffSpec",
    )
    check_registration(gate, working, "register_working_diff_provider", "WorkingDiffSpec")
    check_owned_seam(gate, checkpoint, "plan_scan", "checkpoint_history_from_head")
    check_owned_seam(
        gate,
        filesystem,
        "plan_scan",
        "latest_checkpoint_for_branch",
        free_call="load_rows",
    )
    check_owned_seam(
        gate,
        working,
        "plan_scan",
        "latest_checkpoint_for_branch",
    )
    check_owned_seam(
        gate,
        session,
        "create_checkpoint",
        "checkpoint_history_from_head",
        creation=True,
    )
    for path, function in (
        (checkpoint, "plan_scan"),
        (filesystem, "plan_scan"),
        (working, "plan_scan"),
        (session, "create_checkpoint"),
    ):
        check_scoped_fallbacks(gate, path, function)
    check_production_functions(gate, file_history)
    check_exact_blob_contract(gate, file_history)
    check_production_fixtures(gate, fixture_dir.parent / "production_history_fixtures.tsv")
    check_negative_fixtures(gate, fixture_dir)
    print("GREEN" if not gate.failed else "RED")
    return 0 if not gate.failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
