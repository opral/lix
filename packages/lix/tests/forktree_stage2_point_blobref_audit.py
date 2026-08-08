#!/usr/bin/env python3
"""Function-scoped Stage 2 point/BlobRef coherent-view source audit.

The scanner reads immutable Git objects and never imports or executes Lix. The
reviewer supplies the exact public logical entry function names from an
immutable handoff. Error-erasure checks operate only on those functions and
their statically discovered helpers. A pure `binary_search(...).ok()` is
allowed; `.ok()` on every other immediate receiver remains a finding.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import importlib.util
import json
import re
import sys
from pathlib import Path


HERE = Path(__file__).resolve().parent
BASE_SCANNER = HERE / "forktree_stage2_coherent_view_audit.py"
SPEC = importlib.util.spec_from_file_location("coherent_audit", BASE_SCANNER)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load frozen scanner helpers from {BASE_SCANNER}")
coherent = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = coherent
SPEC.loader.exec_module(coherent)


SELECTED = (
    "packages/lix/src/forktree/view.rs",
    "packages/lix/src/forktree/serving.rs",
    "packages/lix/src/forktree/blob.rs",
    "packages/lix/src/forktree/state.rs",
    "packages/lix/src/forktree/tree.rs",
    "packages/lix/src/forktree/object.rs",
    "packages/lix/src/forktree/mod.rs",
    "packages/lix/src/commit_graph/context.rs",
    "packages/lix/src/binary_cas/context.rs",
    "packages/lix/src/binary_cas/mod.rs",
    "packages/lix/src/engine.rs",
)

AUTH_CALLS = (
    "get_many",
    "begin_scan",
    "next_page",
    "load_object_bytes",
    "load_object_map",
    "lookup_on_read",
    "scan_page_on_read",
    "decode",
    "authenticate",
    "validate",
    "load_commit",
    "load_commit_topolog",
    "state_point",
    "blob",
    "manifest",
    "chunk",
)

ERROR_ERASURE_PATTERNS = (
    ("unwrap-or-default", re.compile(r"\.unwrap_or_default\s*\(")),
    ("unwrap-or", re.compile(r"\.unwrap_or\s*\(")),
    ("unwrap-or-else", re.compile(r"\.unwrap_or_else\s*\(")),
    ("if-let-ok", re.compile(r"\bif\s+let\s+Ok\s*\(")),
    ("while-let-ok", re.compile(r"\bwhile\s+let\s+Ok\s*\(")),
    ("err-wildcard", re.compile(r"\bErr\s*\(\s*_\s*\)\s*=>")),
)

FALLBACK_TOKENS = (
    "fallback",
    "legacy",
    "retry",
    "refresh_snapshot",
    "BinaryCasContext",
    "BINARY_CAS_",
    "TRACKED_STATE_",
    "COMMIT_SPACE",
)

CROSS_VIEW_CACHE_TOKENS = (
    "OnceLock",
    "LazyLock",
    "DashMap",
    "static mut",
    "thread_local!",
)


@dataclasses.dataclass(frozen=True)
class Finding:
    name: str
    passed: bool
    detail: str


def immediate_method_before(body: str, position: int) -> str | None:
    """Return the method whose call result is immediately consumed at position."""
    cursor = position - 1
    while cursor >= 0 and body[cursor].isspace():
        cursor -= 1
    if cursor < 0 or body[cursor] != ")":
        return None
    depth = 1
    cursor -= 1
    while cursor >= 0 and depth:
        if body[cursor] == ")":
            depth += 1
        elif body[cursor] == "(":
            depth -= 1
        cursor -= 1
    if depth:
        return None
    prefix = body[: cursor + 1]
    match = re.search(r"\.([A-Za-z_][A-Za-z0-9_]*)\s*$", prefix)
    return match.group(1) if match else None


def error_erasure(functions: list[object]) -> list[str]:
    hits: list[str] = []
    for function in functions:
        body = function.body
        for match in re.finditer(r"\.ok\s*\(\s*\)", body):
            method = immediate_method_before(body, match.start())
            if method == "binary_search":
                continue
            hits.append(f"{function.path}:{function.name}:.ok:{method or 'unknown'}")
        for label, pattern in ERROR_ERASURE_PATTERNS:
            if pattern.search(body):
                hits.append(f"{function.path}:{function.name}:{label}")
    return sorted(set(hits))


def function_identity(function: object) -> str:
    return f"{function.path}:{function.name}"


def audit(repo: Path, baseline_ref: str, target_ref: str, entries: tuple[str, ...]) -> dict[str, object]:
    baseline, baseline_tree = coherent.resolve(repo, baseline_ref)
    target, target_tree = coherent.resolve(repo, target_ref)
    sources = coherent.source_map(repo, target, SELECTED)
    index = coherent.function_index(sources)
    entry_functions = [function for name in entries for function in index.get(name, [])]
    entry_ids = {(function.path, function.name, function.signature) for function in entry_functions}
    scoped = coherent.closure(index, entries)
    helpers = [
        function
        for function in scoped
        if (function.path, function.name, function.signature) not in entry_ids
    ]
    findings: list[Finding] = []

    findings.append(
        Finding(
            "exact-public-entry-resolution",
            bool(entries)
            and len(entry_functions) == len(entries)
            and {function.name for function in entry_functions} == set(entries),
            "requested=" + ",".join(entries)
            + " found=" + ",".join(sorted(function_identity(function) for function in entry_functions)),
        )
    )

    entry_reads = sum(coherent.count_call(function, "begin_read") for function in entry_functions)
    helper_reads = sum(coherent.count_call(function, "begin_read") for function in helpers)
    findings.append(
        Finding(
            "one-public-entry-begin-read",
            len(entry_functions) == 1 and entry_reads == 1,
            f"entries={len(entry_functions)} begin_read={entry_reads}",
        )
    )
    findings.append(
        Finding(
            "zero-helper-begin-read",
            bool(helpers) and helper_reads == 0,
            f"helpers={len(helpers)} begin_read={helper_reads}",
        )
    )

    scoped_text = "\n".join(function.signature + function.body for function in scoped)
    lower = scoped_text.lower()
    required = {
        "selector": "selector" in lower,
        "catalog": "catalog" in lower,
        "topology": "topolog" in lower or "load_commit" in lower,
        "state": "state_point" in lower or "state_range" in lower,
        "blobref": "blob_ref" in lower or "blobref" in lower,
        "blob-auth": all(token in lower for token in ("manifest", "chunk", "decode")),
    }
    same_read_signatures = [
        function_identity(function)
        for function in scoped
        if any(call in function.body for call in ("get_many", "begin_scan", "load_object_bytes", "lookup_on_read"))
        and not re.search(r"CoherentView|CommitTopologyReader|\bR\b|StorageAdapterRead", function.signature)
    ]
    findings.append(
        Finding(
            "same-read-complete-authority-route",
            bool(scoped)
            and all(required.values())
            and not same_read_signatures
            and helper_reads == 0,
            "coverage=" + ",".join(f"{key}:{value}" for key, value in required.items())
            + " unbound=" + (",".join(same_read_signatures) or "none"),
        )
    )

    erasures = error_erasure(scoped)
    findings.append(
        Finding(
            "no-function-scoped-error-erasure",
            not erasures,
            "hits=" + (",".join(erasures) if erasures else "none")
            + "; binary_search(...).ok() is explicitly allowed",
        )
    )

    fallback_hits = sorted(token for token in FALLBACK_TOKENS if token in scoped_text)
    cache_hits = sorted(token for token in CROSS_VIEW_CACHE_TOKENS if token in scoped_text)
    findings.append(
        Finding(
            "no-fallback-or-cross-view-cache",
            not fallback_hits and not cache_hits,
            "fallback=" + (",".join(fallback_hits) or "none")
            + " cache=" + (",".join(cache_hits) or "none"),
        )
    )

    auth_functions = [
        function for function in scoped if any(token in function.body for token in AUTH_CALLS)
    ]
    propagated = [
        function_identity(function)
        for function in auth_functions
        if "?" not in function.body and "return Err" not in function.body
    ]
    findings.append(
        Finding(
            "authentication-errors-propagate",
            bool(auth_functions) and not propagated,
            "auth_functions=" + str(len(auth_functions))
            + " non_propagating=" + (",".join(propagated) or "none"),
        )
    )

    baseline_paths = coherent.paths(repo, baseline)
    target_paths = coherent.paths(repo, target)
    baseline_deleted = baseline_paths.intersection(coherent.DELETED_OWNER_PATHS)
    target_deleted = target_paths.intersection(coherent.DELETED_OWNER_PATHS)
    findings.append(
        Finding(
            "deleted-owner-modules-do-not-reappear",
            not (target_deleted - baseline_deleted),
            "new=" + (",".join(sorted(target_deleted - baseline_deleted)) or "none"),
        )
    )
    baseline_spaces = coherent.space_definitions(repo, baseline)
    target_spaces = coherent.space_definitions(repo, target)
    findings.append(
        Finding(
            "legacy-space-definitions-do-not-reappear",
            not (target_spaces - baseline_spaces),
            "new=" + (",".join(sorted(target_spaces - baseline_spaces)) or "none"),
        )
    )

    payload: dict[str, object] = {
        "schema": 1,
        "baseline": baseline,
        "baseline_tree": baseline_tree,
        "target": target,
        "target_tree": target_tree,
        "entries": list(entries),
        "scope": [function_identity(function) for function in scoped],
        "findings": [dataclasses.asdict(finding) for finding in findings],
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    payload["evidence_sha256"] = hashlib.sha256(canonical).hexdigest()
    return payload


def self_test() -> dict[str, object]:
    function_type = coherent.Function
    pure = function_type(
        path="fixture.rs",
        name="pure_lookup",
        signature="fn pure_lookup(values: &[u8]) -> Option<usize>",
        body="{ values.binary_search(&7).ok() }",
    )
    storage = function_type(
        path="fixture.rs",
        name="storage_lookup",
        signature="async fn storage_lookup<R: StorageAdapterRead>(read: &R)",
        body="{ read.get_many(&requests).await.ok().flatten() }",
    )
    decode = function_type(
        path="fixture.rs",
        name="decode_lookup",
        signature="fn decode_lookup(bytes: &[u8])",
        body="{ BlobManifestV1::decode(bytes).ok() }",
    )
    pure_hits = error_erasure([pure])
    storage_hits = error_erasure([storage])
    decode_hits = error_erasure([decode])
    payload: dict[str, object] = {
        "schema": 1,
        "binary_search_ok_allowed": not pure_hits,
        "storage_ok_rejected": bool(storage_hits),
        "decode_ok_rejected": bool(decode_hits),
        "storage_hits": storage_hits,
        "decode_hits": decode_hits,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    payload["evidence_sha256"] = hashlib.sha256(canonical).hexdigest()
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--baseline", default=coherent.CBE)
    parser.add_argument("--target")
    parser.add_argument("--entry", action="append", default=[])
    parser.add_argument("--self-test", action="store_true")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--calibrate", action="store_true")
    mode.add_argument("--strict", action="store_true")
    arguments = parser.parse_args()

    if arguments.self_test:
        payload = self_test()
        print(json.dumps(payload, sort_keys=True, indent=2))
        return 0 if all(
            payload[name]
            for name in ("binary_search_ok_allowed", "storage_ok_rejected", "decode_ok_rejected")
        ) else 1
    if not arguments.target or not arguments.entry or not (arguments.calibrate or arguments.strict):
        parser.error("candidate mode requires --target, at least one --entry, and --calibrate or --strict")
    payload = audit(
        arguments.repo.resolve(),
        arguments.baseline,
        arguments.target,
        tuple(arguments.entry),
    )
    print(json.dumps(payload, sort_keys=True, indent=2))
    failed = [finding for finding in payload["findings"] if not finding["passed"]]
    if arguments.strict and failed:
        for finding in failed:
            print(f"BLOCKER\t{finding['name']}\t{finding['detail']}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
