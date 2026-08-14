#!/usr/bin/env python3
"""Candidate-parametric no-JSON/no-compat current-state acceptance gate.

The gate deliberately combines structural source checks with a compiled/test
command contract.  It does not infer authority from a passing runtime test:
durable current-row source must expose a typed tuple and must not retain the
old string/JSON carrier, fallback, dual writer, or alternate key codec.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


JSON_TOKEN = re.compile(
    r"serde_json|JsonValue|JsonSlot|snapshot_content|jsonb|JSONB|json!\s*\("
)
TYPED_CARRIER = re.compile(
    r"\b(?:Canonical|Native|Typed|Durable)(?:Row|Tuple|Cell|Value|Slot)|"
    r"\b(?:Row|Tuple)(?:Cell|Value|Slot)\b"
)


@dataclass(frozen=True)
class Finding:
    gate: str
    path: str
    line: int
    message: str


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", *args], cwd=root, text=True, stderr=subprocess.DEVNULL
    ).strip()


def production_text(path: Path) -> str:
    """Drop the conventional trailing cfg(test) module from source scans."""
    text = path.read_text(encoding="utf-8")
    marker = re.search(r"(?m)^#\[cfg\(test\)\]\s*\n(?:#\[[^\n]+\]\s*\n)*mod tests\s*\{", text)
    return text[: marker.start()] if marker else text


def line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def function_body(text: str, name: str) -> str | None:
    match = re.search(rf"\bfn\s+{re.escape(name)}\b[^{{]*\{{", text)
    if not match:
        return None
    depth = 1
    index = match.end()
    while index < len(text) and depth:
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
        index += 1
    return text[match.start() : index] if depth == 0 else None


def require_tokens_in_order(
    findings: list[Finding],
    gate: str,
    relative: str,
    text: str,
    body: str | None,
    tokens: list[str],
    message: str,
) -> None:
    if body is None:
        findings.append(Finding(gate, relative, 1, message))
        return
    offsets = [body.find(token) for token in tokens]
    if any(offset < 0 for offset in offsets) or offsets != sorted(offsets):
        findings.append(Finding(gate, relative, 1, message))


def classify(path: str, owners: list[dict[str, object]]) -> tuple[str, str] | None:
    for entry in owners:
        for pattern in entry["globs"]:
            if fnmatch.fnmatch(path, pattern):
                return str(entry["owner"]), str(entry["reason"])
    return None


def rust_sources(root: Path, production_roots: Iterable[str]) -> Iterable[Path]:
    for relative in production_roots:
        path = root / relative
        if path.is_file() and path.suffix == ".rs":
            yield path
        elif path.exists():
            yield from (
                candidate
                for candidate in sorted(path.rglob("*.rs"))
                if candidate.name != "tests.rs" and "test_support" not in candidate.parts
            )


def analyze_root(root: Path, policy_path: Path) -> dict[str, object]:
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    findings: list[Finding] = []
    inventory: dict[str, dict[str, object]] = {}

    # Enumerate every production JSON token by an explicit semantic owner.
    for path in rust_sources(root, policy["production_roots"]):
        relative = path.relative_to(root).as_posix()
        text = production_text(path)
        matches = list(JSON_TOKEN.finditer(text))
        if not matches:
            continue
        owner = classify(relative, policy["json_owners"])
        if owner is None:
            for match in matches:
                findings.append(
                    Finding(
                        "json-owner",
                        relative,
                        line_number(text, match.start()),
                        f"unowned JSON residue: {match.group(0)}",
                    )
                )
            continue
        owner_name, reason = owner
        record = inventory.setdefault(
            owner_name, {"reason": reason, "files": {}, "matches": 0}
        )
        record["files"][relative] = len(matches)
        record["matches"] += len(matches)

    state_path = root / "packages/lix/src/forktree/state.rs"
    state = production_text(state_path) if state_path.exists() else ""
    state_relative = state_path.relative_to(root).as_posix()

    if not TYPED_CARRIER.search(state):
        findings.append(
            Finding(
                "typed-current-row",
                state_relative,
                1,
                "durable current-state source exposes no typed tuple/cell carrier",
            )
        )
    for pattern, message in [
        (r"StateCell\s*\{[\s\S]*?Value\s*\(\s*(?:SharedStr|String|&\s*str)",
         "old StateCell string/JSON value remains authoritative"),
        (r"CURRENT_STATE_VALUE_MAGIC[\s\S]{0,120}LIXFCV\\0\\x01",
         "legacy LIXFCV v1 current-row codec remains"),
    ]:
        match = re.search(pattern, state)
        if match:
            findings.append(
                Finding("carrier-authority", state_relative, line_number(state, match.start()), message)
            )

    encode_body = function_body(state, "encode_current_state_value") or ""
    decode_body = function_body(state, "decode_current_state_value") or ""
    decoder_body = function_body(state, "state_cell") or ""
    if re.search(r"cell[\s\S]*?as_bytes\s*\(", encode_body):
        findings.append(
            Finding(
                "carrier-authority",
                state_relative,
                line_number(state, state.find("encode_current_state_value")),
                "current-row cell is serialized as text bytes",
            )
        )
    if "state_cell(" in decode_body and (
        "from_utf8" in decoder_body or "SharedStr" in decoder_body
    ):
        findings.append(
            Finding(
                "carrier-authority",
                state_relative,
                line_number(state, state.find("fn state_cell")),
                "current-row cell decoder reconstructs a UTF-8 string authority",
            )
        )

    # No JSON serializer/deserializer is allowed in durable carrier files.
    for relative in policy["carrier_files"]:
        path = root / relative
        if not path.exists():
            findings.append(Finding("carrier-surface", relative, 1, "required carrier file missing"))
            continue
        text = production_text(path)
        for match in re.finditer(
            r"serde_json::(?:from_str|from_slice|to_string|to_vec)|JsonSlot|json_store|"
            r"(?:legacy|compat|fallback).*(?:state|row|cell)|(?:state|row|cell).*(?:legacy|compat|fallback)",
            text,
            re.IGNORECASE,
        ):
            findings.append(
                Finding(
                    "no-compat-carrier",
                    relative,
                    line_number(text, match.start()),
                    f"forbidden carrier residue: {match.group(0)[:120]}",
                )
            )

    # Whole-row snapshot JSON is forbidden in the current-state authority
    # even where a file also legitimately handles a declared jsonb scalar.
    current_authority_globs = [
        "packages/lix/src/branch/**",
        "packages/lix/src/client_state.rs",
        "packages/lix/src/collection_generation.rs",
        "packages/lix/src/engine.rs",
        "packages/lix/src/entity_pk.rs",
        "packages/lix/src/filesystem/**",
        "packages/lix/src/forktree/bootstrap.rs",
        "packages/lix/src/forktree/current_pack.rs",
        "packages/lix/src/forktree/publication.rs",
        "packages/lix/src/forktree/state.rs",
        "packages/lix/src/functions/state.rs",
        "packages/lix/src/json_store/**",
        "packages/lix/src/session/context.rs",
        "packages/lix/src/session/create_branch.rs",
        "packages/lix/src/session/execute.rs",
        "packages/lix/src/session/switch_branch.rs",
        "packages/lix/src/state/mod.rs",
        "packages/lix/src/transaction/**",
    ]
    for path in rust_sources(root, policy["production_roots"]):
        relative = path.relative_to(root).as_posix()
        if not any(fnmatch.fnmatch(relative, pattern) for pattern in current_authority_globs):
            continue
        text = production_text(path)
        for match in re.finditer(
            r"snapshot_content|JsonSlot|json_store|"
            r"serde_json::(?:from_str|from_slice|to_string|to_vec)(?=[^\n]*(?:row|state|snapshot))|"
            r"(?:serialize|deserialize)_[a-zA-Z0-9_]*(?:row|state|snapshot)",
            text,
        ):
            findings.append(
                Finding(
                    "whole-row-json",
                    relative,
                    line_number(text, match.start()),
                    f"whole-row/current-state JSON residue: {match.group(0)}",
                )
            )

    # The canonical key must be schema -> typed PK -> owner/file in both directions.
    encode_key = function_body(state, "encode_state_key")
    decode_key = function_body(state, "decode_state_key")
    for name, body, tokens in [
        ("encode_state_key", encode_key, ["write_key_string", "write_entity_pk", "write_file_id"]),
        ("decode_state_key", decode_key, ["read_key_string", "read_entity_pk", "read_file_id"]),
    ]:
        if body is None:
            findings.append(Finding("key-order", state_relative, 1, f"{name} is missing"))
            continue
        offsets = [body.find(token) for token in tokens]
        if any(offset < 0 for offset in offsets) or offsets != sorted(offsets):
            findings.append(
                Finding(
                    "key-order",
                    state_relative,
                    line_number(state, state.find(f"fn {name}")),
                    f"{name} is not schema -> typed PK -> owner/file",
                )
            )
    if len(re.findall(r"\bfn\s+encode_state_key\b", state)) != 1:
        findings.append(
            Finding("key-authority", state_relative, 1, "state key does not have one encoder")
        )

    # Dynamically registered rows must bind their durable tuple to the exact
    # trusted catalog plan selected for the operation. A bare Schema/layout
    # hash is insufficient: it permits a caller to construct bytes without
    # proving which authenticated catalog plan authorized that layout.
    normalization_path = root / "packages/lix/src/transaction/normalization.rs"
    normalization = production_text(normalization_path) if normalization_path.exists() else ""
    normalization_relative = normalization_path.relative_to(root).as_posix()
    normalize_body = function_body(normalization, "normalize_raw_write_row_in_place")
    require_tokens_in_order(
        findings,
        "dynamic-schema-native-plan",
        normalization_relative,
        normalization,
        normalize_body,
        ["plan_for_key", "normalized_row_facts"],
        "dynamic Schema-v1 write does not resolve a trusted plan before native lowering",
    )
    native_path = root / "packages/lix/src/native_row.rs"
    native = production_text(native_path) if native_path.exists() else ""
    native_relative = native_path.relative_to(root).as_posix()
    native_cell = re.search(r"struct\s+NativeRowCell\s*\{([\s\S]*?)\}", state)
    native_cell_body = native_cell.group(1) if native_cell else ""
    has_plan_identity = bool(
        re.search(r"schema_(?:plan_id|fingerprint)|plan_(?:id|fingerprint)", native_cell_body)
    )
    encode_native = function_body(native, "encode")
    decode_native = function_body(native, "decode")
    plan_bound_api = bool(
        encode_native
        and decode_native
        and re.search(r"&\s*SchemaPlan\b|&\s*crate::catalog::SchemaPlan\b", encode_native)
        and re.search(r"&\s*SchemaPlan\b|&\s*crate::catalog::SchemaPlan\b", decode_native)
    )
    if not has_plan_identity or not plan_bound_api:
        findings.append(
            Finding(
                "dynamic-schema-native-plan",
                native_relative,
                1,
                "native row is not bound to and decoded through its trusted SchemaPlan identity",
            )
        )
    for match in re.finditer(
        r"(?:plan_for_key|SchemaPlan)[\s\S]{0,240}(?:unwrap_or|unwrap_or_else|or_else)[\s\S]{0,160}(?:json|Value|snapshot|legacy|fallback)",
        normalization,
        re.IGNORECASE,
    ):
        findings.append(
            Finding(
                "dynamic-schema-native-plan",
                normalization_relative,
                line_number(normalization, match.start()),
                "missing/untrusted dynamic plan can fall back to a non-native representation",
            )
        )

    # lix_commit and lix_commit_edge are derived system-current surfaces. They
    # must be sourced directly from authenticated commit topology and may only
    # materialize JSON at the final SQL/public boundary; current StateCell JSON
    # is never an alternate representation for these schemas.
    entity_provider_path = root / "packages/lix/src/sql2/providers/entity.rs"
    entity_provider = (
        production_text(entity_provider_path) if entity_provider_path.exists() else ""
    )
    entity_provider_relative = entity_provider_path.relative_to(root).as_posix()
    commit_scan = function_body(entity_provider, "load_commit_slots")
    if commit_scan is None:
        # The exact helper name may evolve, but the derived provider must still
        # visibly branch on both system schemas and use commit records.
        derived_topology = all(
            token in entity_provider
            for token in ["lix_commit", "lix_commit_edge", "load_commit_records"]
        )
    else:
        derived_topology = all(
            token in commit_scan
            for token in ["lix_commit", "load_commit_records"]
        )
    if not derived_topology:
        findings.append(
            Finding(
                "system-current-native-row",
                entity_provider_relative,
                1,
                "lix_commit system rows are not derived directly from authenticated commit topology",
            )
        )
    for match in re.finditer(
        r"commit_row_snapshot_json|commit_snapshot[\s\S]{0,160}commit_projection_row|"
        r"(?:lix_commit|COMMIT_SCHEMA_KEY)[\s\S]{0,240}JsonSlot::Inline",
        entity_provider,
    ):
        findings.append(
            Finding(
                "system-current-native-row",
                entity_provider_relative,
                line_number(entity_provider, match.start()),
                "lix_commit current projection still passes through removed whole-row JSON",
            )
        )
    for path in rust_sources(root, policy["production_roots"]):
        relative = path.relative_to(root).as_posix()
        text = production_text(path)
        for match in re.finditer(
            r"(?:lix_commit|LIX_COMMIT_SCHEMA_KEY|COMMIT_SCHEMA_KEY)[\s\S]{0,320}StateCell::Value|"
            r"StateCell::Value[\s\S]{0,320}(?:lix_commit|LIX_COMMIT_SCHEMA_KEY|COMMIT_SCHEMA_KEY)",
            text,
        ):
            findings.append(
                Finding(
                    "system-current-native-row",
                    relative,
                    line_number(text, match.start()),
                    "lix_commit/system current row still admits removed JSON StateCell representation",
                )
            )

    # Schema-v1 is closed over exactly seven canonical public types.
    model_path = root / "packages/lix-schema/src/model.rs"
    model = production_text(model_path) if model_path.exists() else ""
    data_type = re.search(r"pub enum DataType\s*\{([\s\S]*?)\n\}", model)
    declared_types = set(re.findall(r"#\[serde\(rename\s*=\s*\"([^\"]+)\"\)\]", data_type.group(1))) if data_type else set()
    expected_types = set(policy["canonical_types"])
    if declared_types != expected_types:
        findings.append(
            Finding(
                "schema-types",
                model_path.relative_to(root).as_posix(),
                1,
                f"canonical types differ: expected={sorted(expected_types)} actual={sorted(declared_types)}",
            )
        )

    schemas: dict[str, list[str]] = {}
    builtin_dir = root / "packages/lix/src/schema/builtin"
    for schema_path in sorted(builtin_dir.glob("*.json")):
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        column_types = [column["type"] for column in schema.get("columns", [])]
        schemas[schema_path.name] = column_types
        unknown = sorted(set(column_types) - expected_types)
        if unknown:
            findings.append(
                Finding(
                    "system-schema-coverage",
                    schema_path.relative_to(root).as_posix(),
                    1,
                    f"system schema uses noncanonical types: {unknown}",
                )
            )

    head = git(root, "rev-parse", "HEAD")
    tree = git(root, "rev-parse", "HEAD^{tree}")
    result = {
        "version": policy["version"],
        "head": head,
        "tree": tree,
        "verdict": "APPROVE" if not findings else "BLOCK",
        "finding_count": len(findings),
        "findings": [asdict(finding) for finding in findings],
        "json_owner_inventory": inventory,
        "canonical_types": sorted(declared_types),
        "system_schemas": schemas,
        "required_carrier_predicates": {
            "dynamic_schema_native_plan": "trusted plan-bound native tuple; no missing-plan fallback",
            "system_current_native_row": "lix_commit/lix_commit_edge derive from authenticated topology; no JSON StateCell",
        },
        "compiled_surface_commands": [
            "cargo check --workspace --all-targets --all-features",
            "cargo test -p lix --test schema_v1_public_smoke --all-features -- --nocapture",
            "cargo test -p lix --lib --all-features immutable_objects_and_typed_state_codecs_fail_closed -- --nocapture",
            "cargo test -p lix --lib --all-features current_state_pack_round_trips_and_rejects_identity_substitution -- --nocapture",
            "cargo test -p lix --lib --all-features coherent_state_point_and_range_preserve_overlay_semantics -- --nocapture",
            "cargo test -p lix --test integration --all-features schema:: -- --nocapture",
            "cargo test -p lix_benchmarks --test checkpoint_gc_replay_reopen --features 'storage-benches slatedb' -- --nocapture"
        ],
    }
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--policy",
        type=Path,
        default=Path(__file__).with_name("no_json_current_state_policy.json"),
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--expect-head")
    args = parser.parse_args()
    root = args.root.resolve()
    result = analyze_root(root, args.policy.resolve())
    if args.expect_head and result["head"] != args.expect_head:
        result["verdict"] = "BLOCK"
        result["finding_count"] += 1
        result["findings"].append(
            asdict(
                Finding(
                    "identity",
                    ".git",
                    1,
                    f"expected head {args.expect_head}, got {result['head']}",
                )
            )
        )
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(encoded, encoding="utf-8")
    else:
        sys.stdout.write(encoded)
    return 0 if result["verdict"] == "APPROVE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
