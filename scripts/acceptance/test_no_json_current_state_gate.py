#!/usr/bin/env python3
"""Deterministic fixture tests for the no-JSON current-state gate."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "no_json_current_state_gate", HERE / "no_json_current_state_gate.py"
)
assert SPEC and SPEC.loader
GATE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = GATE
SPEC.loader.exec_module(GATE)


POLICY = {
    "version": 1,
    "canonical_types": ["text", "uuid", "int8", "float8", "boolean", "jsonb", "timestamptz"],
    "json_owners": [
        {"owner": "plugin", "reason": "plugin", "globs": ["packages/lix/src/plugin/**"]},
        {"owner": "declared-jsonb", "reason": "schema", "globs": ["packages/lix-schema/**"]},
    ],
    "production_roots": ["packages/lix/src", "packages/lix-schema/src"],
    "carrier_files": [
        "packages/lix/src/forktree/state.rs",
        "packages/lix/src/forktree/current_pack.rs",
        "packages/lix/src/state/mod.rs",
    ],
}


GOOD_STATE = r'''
pub enum StateCell { Tuple(CanonicalTuple), Null, Tombstone }
pub struct CanonicalTuple;
pub struct NativeRowCell { schema_plan_id: SchemaPlanId, body: Vec<u8> }
fn encode_state_key(key: Key) { write_key_string(key.schema); write_entity_pk(key.pk); write_file_id(key.owner); }
fn decode_state_key(bytes: &[u8]) { read_key_string(bytes); read_entity_pk(bytes); read_file_id(bytes); }
fn encode_current_state_value(value: &StateValue) { encode_typed_tuple(value); }
fn decode_current_state_value(bytes: &[u8]) { decode_typed_tuple(bytes); }
'''


MODEL = r'''
pub enum DataType {
#[serde(rename = "text")] Text,
#[serde(rename = "uuid")] Uuid,
#[serde(rename = "int8")] Int8,
#[serde(rename = "float8")] Float8,
#[serde(rename = "boolean")] Boolean,
#[serde(rename = "jsonb")] Jsonb,
#[serde(rename = "timestamptz")] Timestamptz,
}
'''


class GateFixture(unittest.TestCase):
    def fixture(self, state: str = GOOD_STATE, extra: dict[str, str] | None = None) -> tuple[Path, Path]:
        directory = Path(tempfile.mkdtemp(prefix="no-json-gate-"))
        subprocess.run(["git", "init", "-q"], cwd=directory, check=True)
        subprocess.run(["git", "config", "user.email", "gate@example.invalid"], cwd=directory, check=True)
        subprocess.run(["git", "config", "user.name", "gate"], cwd=directory, check=True)
        files = {
            "packages/lix/src/forktree/state.rs": state,
            "packages/lix/src/forktree/current_pack.rs": "pub struct CurrentStatePack;\n",
            "packages/lix/src/state/mod.rs": "pub struct TransactionStateView;\n",
            "packages/lix/src/native_row.rs": (
                "fn encode(plan: &SchemaPlan) { bind(plan); }\n"
                "fn decode(plan: &SchemaPlan) { verify(plan); }\n"
            ),
            "packages/lix/src/transaction/normalization.rs": (
                "fn normalize_raw_write_row_in_place() { "
                "plan_for_key(); normalized_row_facts(); }\n"
            ),
            "packages/lix/src/sql2/providers/entity.rs": (
                "fn load_commit_slots() { load_commit_records(); "
                "let _ = (\"lix_commit\", \"lix_commit_edge\"); }\n"
            ),
            "packages/lix-schema/src/model.rs": MODEL,
            "packages/lix/src/schema/builtin/row.json": json.dumps({"columns": [{"name": "id", "type": "uuid"}]}),
        }
        files.update(extra or {})
        for name, value in files.items():
            path = directory / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(value, encoding="utf-8")
        policy = directory / "policy.json"
        policy.write_text(json.dumps(POLICY), encoding="utf-8")
        subprocess.run(["git", "add", "."], cwd=directory, check=True)
        subprocess.run(["git", "commit", "-qm", "fixture"], cwd=directory, check=True)
        return directory, policy

    def test_accepts_typed_tuple_and_owned_plugin_json(self) -> None:
        root, policy = self.fixture(extra={"packages/lix/src/plugin/wire.rs": "use serde_json::Value;\n"})
        self.assertEqual(GATE.analyze_root(root, policy)["verdict"], "APPROVE")

    def test_rejects_string_state_cell(self) -> None:
        root, policy = self.fixture(state=GOOD_STATE.replace("Tuple(CanonicalTuple)", "Value(String)"))
        result = GATE.analyze_root(root, policy)
        self.assertEqual(result["verdict"], "BLOCK")
        self.assertIn("carrier-authority", {finding["gate"] for finding in result["findings"]})

    def test_rejects_wrong_key_order(self) -> None:
        root, policy = self.fixture(state=GOOD_STATE.replace(
            "write_entity_pk(key.pk); write_file_id(key.owner)",
            "write_file_id(key.owner); write_entity_pk(key.pk)",
        ))
        result = GATE.analyze_root(root, policy)
        self.assertIn("key-order", {finding["gate"] for finding in result["findings"]})

    def test_rejects_unowned_json_and_unknown_schema_type(self) -> None:
        root, policy = self.fixture(extra={
            "packages/lix/src/transaction/bad.rs": "use serde_json::Value;\n",
            "packages/lix/src/schema/builtin/bad.json": json.dumps({"columns": [{"name": "x", "type": "object"}]}),
        })
        result = GATE.analyze_root(root, policy)
        gates = {finding["gate"] for finding in result["findings"]}
        self.assertIn("json-owner", gates)
        self.assertIn("system-schema-coverage", gates)

    def test_rejects_native_row_without_trusted_dynamic_plan(self) -> None:
        root, policy = self.fixture(extra={
            "packages/lix/src/native_row.rs": (
                "fn encode(schema: &Schema) { bind(schema); }\n"
                "fn decode(schema: &Schema) { verify(schema); }\n"
            )
        })
        result = GATE.analyze_root(root, policy)
        self.assertIn(
            "dynamic-schema-native-plan",
            {finding["gate"] for finding in result["findings"]},
        )

    def test_rejects_system_current_json_state_cell(self) -> None:
        root, policy = self.fixture(extra={
            "packages/lix/src/sql2/providers/entity.rs": (
                "fn load_commit_slots() { load_commit_records(); "
                "let schema = \"lix_commit\"; StateCell::Value(snapshot); }\n"
            )
        })
        result = GATE.analyze_root(root, policy)
        self.assertIn(
            "system-current-native-row",
            {finding["gate"] for finding in result["findings"]},
        )

    def test_rejects_system_current_json_projection(self) -> None:
        root, policy = self.fixture(extra={
            "packages/lix/src/sql2/providers/entity.rs": (
                "fn load_commit_slots() { load_commit_records(); "
                "let _ = (\"lix_commit\", \"lix_commit_edge\"); "
                "let commit_snapshot = commit_row_snapshot_json(); "
                "commit_projection_row(commit_snapshot); }\n"
            )
        })
        result = GATE.analyze_root(root, policy)
        self.assertIn(
            "system-current-native-row",
            {finding["gate"] for finding in result["findings"]},
        )


if __name__ == "__main__":
    unittest.main()
