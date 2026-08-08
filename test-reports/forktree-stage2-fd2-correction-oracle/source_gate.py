#!/usr/bin/env python3
"""Source-only fd2 correction oracle; no Rust build or runtime execution."""

from __future__ import annotations

import re
import sys
from pathlib import Path


BASE = "fd2be256d763f17e9f127d4c984e36fba191cb82"
PACKAGE = "test-reports/forktree-stage2-fd2-correction-oracle"
FORBIDDEN = (
    "ForkTreeReadFacade::new",
    "begin_read(",
    "query_source.store",
    "TrackedStateStoreReader",
    "TrackedStateContext",
    "TrackedHeadContext",
    "BranchHeadControlContext",
    "working_diff_at_head",
    "legacy",
    "fallback",
    "cache",
    "second authority",
)


class Gate:
    def __init__(self) -> None:
        self.failed = False

    def check(self, label: str, ok: bool, detail: str) -> None:
        status = "PASS" if ok else "RED"
        print(f"{status}\t{label}\t{detail}")
        self.failed |= not ok


def mask(source: str) -> str:
    out = list(source)
    i = 0
    block = 0
    while i < len(source):
        if block:
            if source.startswith("/*", i):
                out[i : i + 2] = "  "
                block += 1
                i += 2
            elif source.startswith("*/", i):
                out[i : i + 2] = "  "
                block -= 1
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
            block = 1
            i += 2
            continue
        if source[i] in ('"', "'"):
            quote = source[i]
            out[i] = " "
            i += 1
            escaped = False
            while i < len(source):
                ch = source[i]
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


def matching(source: str, opening: int, left: str, right: str) -> int | None:
    depth = 0
    for i in range(opening, len(source)):
        if source[i] == left:
            depth += 1
        elif source[i] == right:
            depth -= 1
            if depth == 0:
                return i
    return None


def body(source: str, name: str) -> str | None:
    masked = mask(source)
    match = re.search(rf"\bfn\s+{re.escape(name)}\b", masked)
    if not match:
        return None
    opening = masked.find("{", match.end())
    closing = matching(masked, opening, "{", "}") if opening >= 0 else None
    return source[opening + 1 : closing] if closing is not None else None


def split_top(value: str) -> list[str]:
    masked = mask(value)
    parts: list[str] = []
    start = 0
    depth = 0
    for i, ch in enumerate(masked):
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(value[start:i])
            start = i + 1
    parts.append(value[start:])
    return parts


def norm(value: str) -> str:
    value = re.sub(r"\s+", "", value).strip(";& ")
    changed = True
    while changed:
        changed = False
        for suffix in (".clone()", ".as_ref()", ".as_deref()"):
            if value.endswith(suffix):
                value = value[: -len(suffix)]
                changed = True
        for prefix in ("Arc::clone(&", "std::sync::Arc::clone(&"):
            if value.startswith(prefix) and value.endswith(")"):
                value = value[len(prefix) : -1]
                changed = True
        if value.startswith("&"):
            value = value[1:]
            changed = True
    return value


def function_calls(body_text: str, method: str) -> list[str]:
    masked = mask(body_text)
    pattern = re.compile(rf"(?P<receiver>[A-Za-z_]\w*)\s*\.\s*{re.escape(method)}\s*\(")
    return [norm(m.group("receiver")) for m in pattern.finditer(masked)]


def call_args(body_text: str, name: str) -> list[list[str]]:
    masked = mask(body_text)
    result: list[list[str]] = []
    for match in re.finditer(rf"\b{re.escape(name)}\s*\(", masked):
        opening = masked.find("(", match.start(), match.end())
        closing = matching(masked, opening, "(", ")")
        if closing is None:
            continue
        result.append([norm(arg) for arg in split_top(body_text[opening + 1 : closing])])
    return result


def scan_tuple_contract(body_text: str) -> tuple[bool, str]:
    masked = mask(body_text)
    scan = re.search(r"\bscan_row_source\s*\(", masked)
    if not scan:
        return False, "scan_row_source absent"
    tuple_open = None
    tuple_close = None
    for opening in range(scan.end(), len(masked)):
        if masked[opening] != "(":
            continue
        closing = matching(masked, opening, "(", ")")
        if closing is None:
            continue
        candidate = body_text[opening + 1 : closing]
        if "forktree_reader" in candidate:
            tuple_open, tuple_close = opening, closing
            break
    if tuple_open is None or tuple_close is None:
        return False, "caller-owned reader tuple absent"
    tuple_parts = split_top(body_text[tuple_open + 1 : tuple_close])
    reader_indices = [
        i for i, part in enumerate(tuple_parts) if "forktree_reader" in norm(part)
    ]
    if len(reader_indices) != 1 or "self.forktree_reader" not in norm(tuple_parts[reader_indices[0]]):
        return False, f"reader tuple sources={reader_indices}"
    closure = re.search(r"move\s*\|\s*\(", masked[tuple_close:], re.S)
    if not closure:
        return False, "destructured closure absent"
    relative = tuple_close + closure.start()
    param_open = masked.find("(", relative, relative + 32)
    param_close = matching(masked, param_open, "(", ")") if param_open >= 0 else None
    if param_close is None:
        return False, "closure parameter list unbalanced"
    params = split_top(body_text[param_open + 1 : param_close])
    index = reader_indices[0]
    if index >= len(params):
        return False, f"reader tuple index {index} exceeds closure parameters {len(params)}"
    reader_param = norm(params[index])
    if not re.fullmatch(r"[A-Za-z_]\w*", reader_param):
        return False, f"reader parameter is not a binding: {reader_param}"
    closure_body = body_text[param_close + 1 :]
    return True, reader_param + "|" + closure_body


def scoped_forbidden(text: str) -> list[str]:
    compact = norm(text).lower()
    return [token for token in FORBIDDEN if token.replace(" ", "").lower() in compact]


def check_reader(gate: Gate, path: Path, function: str, method: str, load: bool = False) -> None:
    target = body(path.read_text(), function)
    if target is None:
        gate.check("function-present", False, f"{path}:{function}")
        return
    ok, detail = scan_tuple_contract(target)
    if not ok:
        gate.check("destructured-reader-identity", False, f"{path}:{function} {detail}")
        return
    reader, closure_body = detail.split("|", 1)
    receivers = function_calls(closure_body, method)
    ok = bool(receivers) and all(receiver == reader for receiver in receivers)
    args = call_args(closure_body, "load_rows") if load else []
    if load:
        ok &= bool(args) and all(items and items[0] == reader for items in args)
    bad = scoped_forbidden(target)
    ok &= not bad
    gate.check(
        "destructured-reader-identity",
        ok,
        f"{path}:{function} tuple_reader={reader} receivers={receivers} load_args={args} forbidden={bad}",
    )


def check_plugin_source(gate: Gate, file: Path) -> None:
    source = file.read_text()
    owner = body(source, "file_history_owner_schema_keys")
    validation = body(source, "validate_file_history_materialization")
    owner_compact = norm(owner or "")
    validation_compact = norm(validation or "")
    forbidden_fallback = any(
        token in owner_compact
        for token in ("unwrap_or", "owner.schema_keys()", "fallback", "legacy", "cache")
    )
    owner_ok = (
        owner is not None
        and "state.plugin_registry.get(owner.plugin_key())" in owner_compact
        and not forbidden_fallback
    )
    validation_ok = (
        validation is not None
        and "state.plugin_registry" in validation_compact
        and "ok_or_else" in validation_compact
    )
    gate.check("plugin-registry-fail-closed-source", owner_ok and validation_ok, str(file))


def registry_result(state: str) -> str:
    return {
        "present-valid": "VALID",
        "present-empty": "VALID_EMPTY",
        "missing": "FAIL_CLOSED_MISSING",
        "present-wrong-kind": "FAIL_CLOSED_WRONG_KIND",
        "present-malformed": "FAIL_CLOSED_MALFORMED",
        "present-substituted": "FAIL_CLOSED_SUBSTITUTED",
    }.get(state, "FAIL_CLOSED_UNKNOWN")


def check_registry_model(gate: Gate, path: Path) -> None:
    rows = []
    for line in path.read_text().splitlines():
        if not line or line.startswith("case\t"):
            continue
        case, state, expected = line.split("\t")
        rows.append((case, state, expected))
    actual = [(case, registry_result(state), expected) for case, state, expected in rows]
    gate.check("plugin-registry-discriminator", all(result == expected for _, result, expected in actual), f"cases={len(rows)}")


def check_fixture(gate: Gate, path: Path, expected: bool) -> None:
    target = body(path.read_text(), "plan_scan")
    ok = False
    detail = "function absent"
    if target is not None:
        ok, detail = scan_tuple_contract(target)
        if ok:
            reader, closure = detail.split("|", 1)
            receivers = function_calls(closure, "latest_checkpoint_for_branch")
            args = call_args(closure, "load_rows")
            ok = bool(receivers) and all(item == reader for item in receivers)
            ok &= bool(args) and all(item and item[0] == reader for item in args)
            ok &= not scoped_forbidden(target)
            detail = f"reader={reader} receivers={receivers}"
    gate.check("reader-negative-fixture", ok == expected, f"{path.name} {detail}")


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: source_gate.py SOURCE_ROOT", file=sys.stderr)
        return 2
    root = Path(sys.argv[1]).resolve()
    gate = Gate()
    check_reader(gate, root / "packages/lix/src/sql2/providers/checkpoint.rs", "plan_scan", "checkpoint_history_from_head")
    check_reader(gate, root / "packages/lix/src/sql2/providers/filesystem_working_diff.rs", "plan_scan", "latest_checkpoint_for_branch", load=True)
    check_reader(gate, root / "packages/lix/src/sql2/providers/working_diff.rs", "plan_scan", "latest_checkpoint_for_branch")
    check_plugin_source(gate, root / "packages/lix/src/sql2/providers/file_history.rs")
    fixture_dir = root / PACKAGE / "fixtures/readers"
    for name, expected in (("valid.rs", True), ("distinct_view.rs", False), ("fresh_read.rs", False), ("legacy_reader.rs", False), ("mismatched_argument.rs", False)):
        check_fixture(gate, fixture_dir / name, expected)
    check_registry_model(gate, root / PACKAGE / "fixtures/registry_cases.tsv")
    print("GREEN" if not gate.failed else "RED")
    return 0 if not gate.failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
