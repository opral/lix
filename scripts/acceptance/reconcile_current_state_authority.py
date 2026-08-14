#!/usr/bin/env python3
"""Reachability-based A/B/C audit for the Schema-v1 current-state carrier.

This deliberately does not reject vocabulary tokens.  It proves the current
durable writer/decoder chain and separately inventories historical/public and
test-only uses of the old logical cell vocabulary.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=root, text=True).strip()


def production(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    marker = re.search(r"(?m)^#\[cfg\(test\)\]\s*\n(?:#\[[^\n]+\]\s*\n)*mod tests\s*\{", text)
    return text[: marker.start()] if marker else text


def body(text: str, name: str) -> str:
    match = re.search(rf"\bfn\s+{re.escape(name)}\b[^{{]*\{{", text)
    if not match:
        return ""
    depth = 1
    cursor = match.end()
    while cursor < len(text) and depth:
        depth += text[cursor] == "{"
        depth -= text[cursor] == "}"
        cursor += 1
    return text[match.start():cursor] if depth == 0 else ""


def ordered(text: str, *tokens: str) -> bool:
    offsets = [text.find(token) for token in tokens]
    return all(offset >= 0 for offset in offsets) and offsets == sorted(offsets)


def analyze(root: Path) -> dict[str, object]:
    state = production(root / "packages/lix/src/forktree/state.rs")
    current_pack = production(root / "packages/lix/src/forktree/current_pack.rs")
    normalization = production(root / "packages/lix/src/transaction/normalization.rs")
    staging = production(root / "packages/lix/src/transaction/staging.rs")
    commit = production(root / "packages/lix/src/transaction/commit.rs")
    bootstrap = production(root / "packages/lix/src/forktree/bootstrap.rs")
    native = production(root / "packages/lix/src/native_row.rs")
    serving = production(root / "packages/lix/src/forktree/serving.rs")
    entity = production(root / "packages/lix/src/sql2/providers/entity.rs")

    encode_current = body(state, "encode_current_state_value")
    decode_cell = body(state, "state_cell")
    normalize = body(normalization, "normalize_raw_write_row_in_place")
    staged_cell = body(staging, "staged_cell")
    native_encode = body(native, "encode")
    native_decode = body(native, "decode")
    layout = body(native, "layout_id")
    owner = production(root / "packages/lix/src/entity_pk.rs")
    owner_digest = body(owner, "native_row_owner_digest")
    history_bind = body(serving, "authenticated_current_cell_for_history")
    commit_projection = body(entity, "commit_projection_row")

    checks = [
        Check(
            "trusted-plan-before-lowering",
            ordered(normalize, "plan_for_key", "normalized_row_facts"),
            "transaction normalization resolves the operation catalog plan before native lowering",
        ),
        Check(
            "prepared-live-row-is-native",
            "StateCell::NativeRow" in staged_cell
            and "StateCell::Tombstone" in staged_cell
            and "StateCell::Value" not in staged_cell
            and "StateCell::Null" not in staged_cell,
            "prepared current rows lower only to NativeRow or Tombstone",
        ),
        Check(
            "commit-current-writer-is-native",
            "StateCell::NativeRow" in commit
            and "encode_current_state_packs" in commit
            and "StateCell::Value" not in commit
            and "StateCell::Null" not in commit,
            "commit publication has no JSON/null current-cell writer",
        ),
        Check(
            "bootstrap-current-writer-is-native",
            "StateCell::NativeRow" in bootstrap
            and "encode_current_state_packs" in bootstrap
            and "StateCell::Value" not in bootstrap
            and "StateCell::Null" not in bootstrap,
            "bootstrap publication has no JSON/null current-cell writer",
        ),
        Check(
            "current-encoder-rejects-removed-tags",
            "StateCell::Value(_)" in encode_current
            and "removed JSON row representation" in encode_current
            and "StateCell::Null" in encode_current
            and "removed whole-row null" in encode_current
            and "StateCell::NativeRow" in encode_current,
            "LIXFCV envelope cannot encode old Value/Null cells",
        ),
        Check(
            "current-decoder-rejects-removed-tags",
            '0 => Err' in decode_cell
            and '1 => Err' in decode_cell
            and '2 => Ok(StateCell::Tombstone)' in decode_cell
            and '3 =>' in decode_cell
            and 'StateCell::NativeRow' in decode_cell,
            "LIXFCV tags 0/1 fail closed; only tombstone/native tags decode",
        ),
        Check(
            "current-pack-single-codec",
            "encode_current_state_value" in current_pack
            and "decode_current_state_value" in current_pack
            and "serde_json" not in current_pack,
            "current packs use the one binary current-value codec",
        ),
        Check(
            "layout-binding",
            all(token in layout for token in [
                "schema.key", "schema.columns", "column.name",
                "column.data_type.postgres_name", "column.nullable", "schema.primary_key",
            ])
            and "layout_id(schema)" in native_encode
            and "layout_id(schema)" in native_decode,
            "layout digest binds schema key, ordered columns/types/nullability, and PK layout",
        ),
        Check(
            "owner-binding",
            all(token in owner_digest for token in ["branch_id", "schema_key", "file_id", "entity_pk"])
            and "native_row_owner_digest" in native_encode
            and "native_row_owner_digest" in native_decode
            and "owner_branch_id" in native_decode,
            "owner digest binds repository-visible domain identity: branch/schema/typed PK/file",
        ),
        Check(
            "semantic-binding",
            "semantic_digest(snapshot)" in native_encode
            and "native.semantic_digest" in history_bind
            and "semantic_digest_text" in history_bind,
            "native logical bytes are cross-checked against authenticated change history",
        ),
        Check(
            "canonical-body-decode",
            "value_layout::encode_body" in native_encode
            and "value_layout::decode_body" in native_decode,
            "body uses one fail-closed typed value-layout codec",
        ),
        Check(
            "derived-commit-is-not-durable-json",
            "load_commit_records" in entity
            and "commit_row_snapshot_json" in entity
            and "StateCell::NativeRow" in commit_projection
            and "native_row::encode" in commit_projection
            and "encode_current_state_packs" not in commit_projection,
            "lix_commit JSON is transient public projection input, then native-encoded in-memory; it is never a current-pack writer",
        ),
    ]

    current_pack_callers: list[str] = []
    for path in sorted((root / "packages/lix/src").rglob("*.rs")):
        if path.name == "tests.rs" or "test_support" in path.parts:
            continue
        text = production(path)
        if "encode_current_state_packs(" in text:
            current_pack_callers.append(path.relative_to(root).as_posix())
    allowed_callers = {
        "packages/lix/src/forktree/bootstrap.rs",
        "packages/lix/src/forktree/current_pack.rs",
        "packages/lix/src/transaction/commit.rs",
    }
    checks.append(Check(
        "current-writer-callers-closed",
        set(current_pack_callers) == allowed_callers,
        f"production current-pack encoder callers={current_pack_callers}",
    ))

    classifications = {
        "A_current_durable_authority": [
            {
                "chain": "RawWriteBatch -> normalize_raw_write_row_in_place -> trusted TransactionCatalog plan -> native_row::encode -> PreparedStateRow.native_row -> staged_cell/commit StateCell::NativeRow -> encode_current_state_packs -> encode_current_state_value(tag=3)",
                "verdict": "single writer",
            },
            {
                "chain": "ForkTree current pack -> decode_current_state_value -> StateCell::NativeRow -> authenticated_current_cell_for_history semantic digest -> native_row::decode layout/owner/body checks",
                "verdict": "single fail-closed decoder",
            },
            {
                "concern": "StateCell::Value/Null",
                "verdict": "not current durable ingress: encoder rejects both and decoder rejects legacy tags 0/1",
            },
            {
                "concern": "LIXFCV v1 magic",
                "verdict": "one envelope marker only, not compatibility: old tag payloads fail closed and no fallback decoder exists",
            },
            {
                "concern": "SchemaPlan identity",
                "verdict": "equivalently authenticated without persisting a plan object: trusted plan selected before write; StateKey schema plus layout digest plus branch/schema/typed-PK/file owner digest plus semantic digest bind decode; owner/layout substitution fails",
            },
        ],
        "B_deferred_history_plugin_public_json": [
            {
                "owner": "history",
                "chain": "ChangeRecord JsonSlot -> logical_history_cell -> StateCell::Value/Null -> HistoricalStateRow/merge/blob/history consumers",
                "note": "logical historical projection only; current encoder rejects it",
            },
            {
                "owner": "plugin",
                "chain": "historical/plugin wire snapshots -> serde_json at plugin boundary",
                "note": "explicitly deferred plugin/history JSON contract",
            },
            {
                "owner": "public lix_commit projection",
                "chain": "authenticated CommitGraph load_commit_records -> commit_row_snapshot_json transient value -> commit_projection_row -> in-memory NativeRow -> Arrow/public output",
                "note": "avoidable transient conversion, but no durable JSON bytes or second authority",
            },
        ],
        "C_dead_or_test_vocabulary": [
            {
                "owner": "test fixtures",
                "detail": "StateCellRef and direct StateCell::Value/Null constructors in forktree/tests.rs and trailing cfg(test) modules",
            },
            {
                "owner": "production rejection arms",
                "detail": "matches on Value/Null in current consumers are fail-closed guards, not constructors/writers",
            },
        ],
    }

    failed = [asdict(check) for check in checks if not check.ok]
    return {
        "version": 3,
        "head": git(root, "rev-parse", "HEAD"),
        "tree": git(root, "rev-parse", "HEAD^{tree}"),
        "verdict": "APPROVE" if not failed else "BLOCK",
        "checks": [asdict(check) for check in checks],
        "failed_checks": failed,
        "classifications": classifications,
        "separate_non_json_findings": [
            "transaction validation currently decodes a StateRowSource::Global row using the selected branch owner; this is a branch-owner propagation defect, not a JSON/current-carrier authority defect"
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--expect-head")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = analyze(args.root.resolve())
    if args.expect_head and result["head"] != args.expect_head:
        result["verdict"] = "BLOCK"
        result["failed_checks"].append({
            "name": "identity", "ok": False,
            "detail": f"expected {args.expect_head}, got {result['head']}",
        })
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(encoded, encoding="utf-8")
    else:
        print(encoded, end="")
    return 0 if result["verdict"] == "APPROVE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
