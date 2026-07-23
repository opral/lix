"""Deterministic checks for the preregistered JSON v1-v2 gate."""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).with_name("json_paired_gate.py")
SPEC = importlib.util.spec_from_file_location("json_paired_gate", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


def counter_row(round_index: int) -> dict:
    return {
        "label": "edit",
        "round": round_index,
        "source_read_calls": 0,
        "source_bytes_read": 0,
        "component_boundary_bytes": 128,
        "guest_linear_memory_high_water_bytes": 16 * 1024 * 1024,
        "host_full_diff_bytes_compared": 0,
        "host_full_content_classification_bytes": 0,
        "full_state_semantic_rows_materialized": 0,
        "change_payload_requests": 2,
        "returned_change_payloads": 1,
        "durable_semantic_changes": 1,
        "private_document_cache_hits": 1,
        "shared_renderer_cache_hits": 0,
        "full_document_reparses": 0,
        "full_renderer_invocations": 0,
        "filesystem_sync_full_renders": 0,
    }


def metric_blocks(
    *,
    measured: int = 20,
    blocks: int = 12,
    v2_ratio: float,
    counters: bool,
) -> list[dict]:
    output = []
    for index in range(blocks):
        v1 = [100.0 + sample for sample in range(measured)]
        v2 = [sample * v2_ratio for sample in v1]
        output.append(
            {
                "index": index,
                "order": "v1-v2" if index % 2 == 0 else "v2-v1",
                "arms": {
                    "v1": {"sample_ms": v1, "counters": []},
                    "v2": {
                        "sample_ms": v2,
                        "counters": (
                            [counter_row(round_index) for round_index in range(measured)]
                            if counters
                            else []
                        ),
                    },
                },
            }
        )
    return output


def fixture(edit_ratio: float = 0.5) -> dict:
    archives = {
        "v1": {"bytes": 1234, "sha256": "1" * 64},
        "v2": {"bytes": 2345, "sha256": "2" * 64},
    }
    backend = {
        "status": "complete",
        "setup": {
            api: {"plugin_archive": copy.deepcopy(fingerprint)}
            for api, fingerprint in archives.items()
        },
        "edit": metric_blocks(v2_ratio=edit_ratio, counters=True),
        "render": metric_blocks(v2_ratio=1.0, counters=False),
    }
    return {
        "status": "complete",
        "design": {
            "format": "json",
            "apis": ["v1", "v2"],
            "same_benchmark_executable": True,
            "fixture_bytes": 10_000_000,
            "properties": 220_000,
            "edit_property": "property_110000",
            "backends": ["rocksdb-fs", "slatedb-cached"],
            "blocks": 12,
            "warmups_per_arm_block": 5,
            "measured_per_arm_block": 20,
            "wasm_memory_mib": 256,
            "acceptance_eligible": True,
            "analysis": {
                "seed": "0x4c49584a",
                "draws": 100,
                "thresholds_preregistered_before_samples": copy.deepcopy(
                    gate.THRESHOLDS
                ),
            },
        },
        "plugins": {
            api: {"archive_observed_at_setup": copy.deepcopy(fingerprint)}
            for api, fingerprint in archives.items()
        },
        "backends": {
            "rocksdb-fs": copy.deepcopy(backend),
            "slatedb-cached": copy.deepcopy(backend),
        },
    }


class JsonPairedGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_min_draws = gate.MIN_DRAWS
        gate.MIN_DRAWS = 100

    def tearDown(self) -> None:
        gate.MIN_DRAWS = self.original_min_draws

    def test_clear_edit_improvement_and_render_parity_pass_deterministically(
        self,
    ) -> None:
        first = gate.evaluate(fixture())
        second = gate.evaluate(fixture())
        self.assertEqual(first, second)
        self.assertTrue(first["pass"])
        self.assertEqual(first["decision"], "pass")
        self.assertTrue(first["counter_gate"]["pass"])
        self.assertAlmostEqual(
            first["backends"]["rocksdb-fs"]["edit"]["p50"][
                "pooled_ratio_v2_over_v1"
            ],
            0.5,
        )

    def test_edit_upper_bound_above_preregistered_limit_fails(self) -> None:
        result = gate.evaluate(fixture(0.9))
        self.assertFalse(result["pass"])
        self.assertEqual(result["decision"], "fail")
        self.assertFalse(result["backends"]["rocksdb-fs"]["edit"]["pass"])

    def test_hot_path_counter_violation_fails_even_when_latency_passes(self) -> None:
        value = fixture()
        value["backends"]["rocksdb-fs"]["edit"][0]["arms"]["v2"]["counters"][0][
            "full_document_reparses"
        ] = 1
        result = gate.evaluate(value)
        self.assertFalse(result["pass"])
        self.assertFalse(result["counter_gate"]["pass"])
        self.assertEqual(
            result["counter_gate"]["violations"][0]["field"],
            "full_document_reparses",
        )

    def test_exact_fixture_and_counterbalance_are_required(self) -> None:
        wrong_fixture = fixture()
        wrong_fixture["design"]["fixture_bytes"] -= 1
        with self.assertRaisesRegex(ValueError, "exact fixture"):
            gate.evaluate(wrong_fixture)

        wrong_order = fixture()
        wrong_order["backends"]["slatedb-cached"]["render"][1]["order"] = "v1-v2"
        with self.assertRaisesRegex(ValueError, "not exactly counterbalanced"):
            gate.evaluate(wrong_order)

    def test_render_must_not_contain_edit_counter_rows(self) -> None:
        value = fixture()
        value["backends"]["rocksdb-fs"]["render"][0]["arms"]["v2"][
            "counters"
        ] = [counter_row(index) for index in range(20)]
        with self.assertRaisesRegex(ValueError, "exactly 0 counter rows"):
            gate.evaluate(value)

    def test_reduced_campaign_is_explicitly_smoke_only(self) -> None:
        value = fixture()
        design = value["design"]
        design["blocks"] = 2
        design["warmups_per_arm_block"] = 1
        design["measured_per_arm_block"] = 2
        design["acceptance_eligible"] = False
        for backend in value["backends"].values():
            for metric in ("edit", "render"):
                backend[metric] = backend[metric][:2]
                for block in backend[metric]:
                    for arm in block["arms"].values():
                        arm["sample_ms"] = arm["sample_ms"][:2]
                        arm["counters"] = arm["counters"][:2]

        with self.assertRaisesRegex(ValueError, "smoke-only"):
            gate.evaluate(value)
        result = gate.evaluate(value, allow_smoke=True)
        self.assertFalse(result["acceptance_eligible"])
        self.assertFalse(result["pass"])
        self.assertTrue(result["statistical_and_counter_pass"])
        self.assertEqual(
            result["decision"],
            "smoke-only; checks passed, no acceptance decision",
        )


if __name__ == "__main__":
    unittest.main()
