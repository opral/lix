"""Deterministic checks for the preregistered PR2 acceptance analyzer."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).with_name("paired_gate.py")
SPEC = importlib.util.spec_from_file_location("paired_gate", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
paired_gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(paired_gate)


def fixture(candidate_ratio: float = 0.5) -> dict:
    blocks = []
    for index in range(12):
        baseline = [100.0 + sample for sample in range(20)]
        candidate = [value * candidate_ratio for value in baseline]
        blocks.append(
            {
                "order": (
                    "baseline-candidate"
                    if index % 2 == 0
                    else "candidate-baseline"
                ),
                "baseline": baseline,
                "candidate": candidate,
            }
        )
    return {
        "design": {
            "format": "csv",
            "blocks": 12,
            "warmups_per_arm_block": 5,
            "measured_per_arm_block": 20,
        },
        "backends": {
            backend: {
                "edit": [dict(block) for block in blocks],
                # Exact-render is a regression guard, not an improvement gate.
                "render": [
                    {**block, "candidate": list(block["baseline"])}
                    for block in blocks
                ],
            }
            for backend in ("rocksdb-fs", "slatedb-cached")
        },
    }


class PairedGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_draws = paired_gate.DRAWS
        paired_gate.DRAWS = 100

    def tearDown(self) -> None:
        paired_gate.DRAWS = self.original_draws

    def test_clear_improvement_passes_every_cell_deterministically(self) -> None:
        first = paired_gate.evaluate(fixture(0.5))
        second = paired_gate.evaluate(fixture(0.5))
        self.assertEqual(first, second)
        self.assertTrue(first["pass"])
        self.assertTrue(
            all(backend["pass"] for backend in first["backends"].values())
        )

    def test_edit_upper_bound_at_or_above_point_eight_fails(self) -> None:
        result = paired_gate.evaluate(fixture(0.9))
        self.assertFalse(result["pass"])
        self.assertFalse(result["backends"]["rocksdb-fs"]["edit"]["pass"])

    def test_non_counterbalanced_order_is_rejected(self) -> None:
        value = fixture()
        value["backends"]["rocksdb-fs"]["edit"][1]["order"] = (
            "baseline-candidate"
        )
        with self.assertRaisesRegex(ValueError, "not exactly counterbalanced"):
            paired_gate.evaluate(value)

    def test_wrong_sample_count_is_rejected(self) -> None:
        value = fixture()
        value["backends"]["slatedb-cached"]["render"][0]["candidate"].pop()
        with self.assertRaisesRegex(ValueError, "exactly 20"):
            paired_gate.evaluate(value)

    def test_point_ratio_uses_pooled_observations_not_mean_block_percentiles(self) -> None:
        value = fixture(0.5)
        blocks = value["backends"]["rocksdb-fs"]["edit"]
        for index, block in enumerate(blocks):
            if index < 7:
                block["baseline"] = [10.0] * 20
                block["candidate"] = [5.0] * 20
            else:
                block["baseline"] = [1_000.0] * 20
                block["candidate"] = [900.0] * 20
        result = paired_gate.evaluate(value)
        self.assertEqual(
            result["backends"]["rocksdb-fs"]["edit"]["p50"]["pooled_ratio"],
            0.5,
        )

    def test_runner_environment_removes_inherited_profile_knobs(self) -> None:
        runner_path = Path(__file__).with_name("run_paired.py")
        runner_spec = importlib.util.spec_from_file_location("run_paired", runner_path)
        assert runner_spec is not None and runner_spec.loader is not None
        runner = importlib.util.module_from_spec(runner_spec)
        runner_spec.loader.exec_module(runner)
        import os

        previous = os.environ.get("LIX_PROFILE_IO_STATS")
        os.environ["LIX_PROFILE_IO_STATS"] = "1"
        try:
            env = runner.environment(220_000, 256)
        finally:
            if previous is None:
                os.environ.pop("LIX_PROFILE_IO_STATS", None)
            else:
                os.environ["LIX_PROFILE_IO_STATS"] = previous
        self.assertNotIn("LIX_PROFILE_IO_STATS", env)
        self.assertNotIn("LIX_PROFILE_SPLICE_PROVENANCE", env)
        self.assertEqual(env["LIX_PROFILE_FORMAT"], "csv")


if __name__ == "__main__":
    unittest.main()
