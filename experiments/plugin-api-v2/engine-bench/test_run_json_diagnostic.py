"""Dependency-free tests for the candidate-only JSON diagnostic runner."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest


MODULE_PATH = Path(__file__).with_name("run_json_diagnostic.py")
SPEC = importlib.util.spec_from_file_location("run_json_diagnostic", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def sample_line(label: str, count: int = 20) -> str:
    return f"{label} sample_ms={[float(index) for index in range(1, count + 1)]}"


class JsonDiagnosticTests(unittest.TestCase):
    def test_environment_fixes_design_and_removes_inherited_profile_knobs(self) -> None:
        old = os.environ.get("LIX_PROFILE_SPLICE_PROVENANCE")
        os.environ["LIX_PROFILE_SPLICE_PROVENANCE"] = "1"
        try:
            env = runner.environment()
        finally:
            if old is None:
                os.environ.pop("LIX_PROFILE_SPLICE_PROVENANCE", None)
            else:
                os.environ["LIX_PROFILE_SPLICE_PROVENANCE"] = old
        self.assertNotIn("LIX_PROFILE_SPLICE_PROVENANCE", env)
        self.assertEqual(env["LIX_PROFILE_FORMAT"], "json")
        self.assertEqual(env["LIX_PROFILE_WARMUPS"], "5")
        self.assertEqual(env["LIX_PROFILE_ROUNDS"], "20")
        self.assertEqual(env["LIX_PROFILE_WASM_MEMORY_MIB"], "256")

    def test_exact_sample_count_and_positive_values_are_required(self) -> None:
        values = runner.measured_samples(sample_line("edit"), "edit")
        self.assertEqual(len(values), 20)
        with self.assertRaisesRegex(ValueError, "exactly 20"):
            runner.measured_samples(sample_line("edit", 19), "edit")
        with self.assertRaisesRegex(ValueError, "finite positive"):
            runner.measured_samples(
                "edit sample_ms=" + repr([-1.0] + [1.0] * 19), "edit"
            )
        with self.assertRaisesRegex(ValueError, "exactly one"):
            runner.measured_samples(sample_line("edit") + "\n" + sample_line("edit"), "edit")

    def test_fixture_and_mode_contracts_are_exact(self) -> None:
        runner.validate_setup(
            "setup format=json runtime=wasm-component-v1 mechanism_only=true "
            "properties=220000 bytes=10000000 edit_property=property_110000\n"
        )
        runner.validate_mode(
            "edit format=json properties=220000 bytes=10000000 "
            "property=property_110000 warmups=5 rounds=20\n",
            "edit",
        )
        runner.validate_mode(
            "render format=json properties=220000 warmups=5 rounds=20\n",
            "render",
        )
        with self.assertRaisesRegex(ValueError, "exact"):
            runner.validate_setup(
                "setup format=json runtime=wasm-component-v1 mechanism_only=true "
                "properties=220000 bytes=9999999 edit_property=property_110000\n"
            )

    def test_run_directory_requires_an_exact_ownership_marker(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            empty = parent / "empty"
            empty.mkdir()
            root = runner.prepare_root(empty)
            marker = root / runner.MARKER
            self.assertEqual(marker.read_text(encoding="utf-8"), runner.MARKER_CONTENT)
            self.assertEqual(runner.prepare_root(empty), root)

            unowned = parent / "unowned"
            unowned.mkdir()
            (unowned / "data").write_text("mine", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "new, empty"):
                runner.prepare_root(unowned)

            wrong = parent / "wrong"
            wrong.mkdir()
            (wrong / runner.MARKER).write_text("wrong\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unexpected contents"):
                runner.prepare_root(wrong)

    def test_run_owned_symlink_cannot_be_reset(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = runner.prepare_root(parent / "run")
            outside = parent / "outside"
            outside.mkdir()
            child = root / "logs"
            child.symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "outside run directory"):
                runner.reset_directory(root, child)

    def test_atomic_writer_leaves_only_complete_json(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result.json"
            runner.write_artifact(output, {"complete": True})
            self.assertEqual(
                json.loads(output.read_text(encoding="utf-8")), {"complete": True}
            )
            self.assertEqual(list(Path(temporary).glob("*.tmp")), [])

    def test_end_to_end_flow_uses_fresh_cases_and_persists_logs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            binary = parent / "fake-profile.py"
            binary.write_text(
                """#!/usr/bin/env python3
import os
from pathlib import Path
import sys

backend, mode, raw_case = sys.argv[1:]
case = Path(raw_case)
assert backend in {"rocksdb-fs", "slatedb-cached"}
assert os.environ["LIX_PROFILE_FORMAT"] == "json"
assert os.environ["LIX_PROFILE_WARMUPS"] == "5"
assert os.environ["LIX_PROFILE_ROUNDS"] == "20"
assert os.environ["LIX_PROFILE_WASM_MEMORY_MIB"] == "256"
if mode == "setup":
    (case / "pristine").write_text("yes", encoding="utf-8")
    print("setup format=json runtime=wasm-component-v1 mechanism_only=true properties=220000 bytes=10000000 edit_property=property_110000")
elif mode == "edit":
    assert (case / "pristine").read_text(encoding="utf-8") == "yes"
    (case / "edit-dirt").write_text("changed", encoding="utf-8")
    print("edit format=json properties=220000 bytes=10000000 property=property_110000 warmups=5 rounds=20")
    print("edit sample_ms=" + repr([float(index) for index in range(1, 21)]))
elif mode == "render":
    assert (case / "pristine").read_text(encoding="utf-8") == "yes"
    assert not (case / "edit-dirt").exists()
    print("render format=json properties=220000 warmups=5 rounds=20")
    print("render sample_ms=" + repr([float(index) for index in range(1, 21)]))
else:
    raise AssertionError(mode)
""",
                encoding="utf-8",
            )
            binary.chmod(0o755)
            run_dir = parent / "diagnostic"
            output = runner.run_diagnostic(binary, run_dir)
            artifact = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(artifact["status"], "complete")
            self.assertFalse(artifact["design"]["pr2_v2_acceptance"])
            self.assertTrue(artifact["design"]["mechanism_only"])
            self.assertEqual(artifact["design"]["runtime"], "wasm-component-v1")
            for backend in runner.BACKENDS:
                self.assertEqual(artifact["backends"][backend]["status"], "complete")
                self.assertEqual(
                    artifact["backends"][backend]["edit"]["summary"]["samples"], 20
                )
                self.assertEqual(
                    artifact["backends"][backend]["render"]["summary"]["samples"], 20
                )
                self.assertTrue((run_dir / "logs" / backend / "setup.log").is_file())
                self.assertTrue((run_dir / "logs" / backend / "edit.log").is_file())
                self.assertTrue((run_dir / "logs" / backend / "render.log").is_file())
            self.assertEqual(list((run_dir / "cases").glob("*/*")), [])


if __name__ == "__main__":
    unittest.main()
