"""Dependency-free tests for the JSON v1-v2 paired campaign runner."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest


MODULE_PATH = Path(__file__).with_name("run_json_paired.py")
SPEC = importlib.util.spec_from_file_location("run_json_paired", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)

GATE_PATH = Path(__file__).with_name("json_paired_gate.py")
GATE_SPEC = importlib.util.spec_from_file_location("json_paired_gate", GATE_PATH)
assert GATE_SPEC is not None and GATE_SPEC.loader is not None
gate = importlib.util.module_from_spec(GATE_SPEC)
GATE_SPEC.loader.exec_module(gate)


def write_manifest(path: Path, *, runtime: str, api_version: str) -> None:
    path.write_text(
        json.dumps(
            {
                "key": f"test-{runtime}",
                "runtime": runtime,
                "api_version": api_version,
                "entry": "plugin.wasm",
            }
        ),
        encoding="utf-8",
    )


class JsonPairedRunnerTests(unittest.TestCase):
    def test_environment_selects_one_api_and_removes_inherited_knobs(self) -> None:
        previous = os.environ.get("LIX_PROFILE_IO_STATS")
        os.environ["LIX_PROFILE_IO_STATS"] = "1"
        try:
            env = runner.environment(
                "v2", 3, 7, 192, splice_provenance=True
            )
        finally:
            if previous is None:
                os.environ.pop("LIX_PROFILE_IO_STATS", None)
            else:
                os.environ["LIX_PROFILE_IO_STATS"] = previous
        self.assertNotIn("LIX_PROFILE_IO_STATS", env)
        self.assertEqual(env["LIX_PROFILE_FORMAT"], "json")
        self.assertEqual(env["LIX_PROFILE_JSON_API"], "v2")
        self.assertEqual(env["LIX_PROFILE_JSON_SHAPE"], "flat")
        self.assertEqual(env["LIX_PROFILE_WARMUPS"], "3")
        self.assertEqual(env["LIX_PROFILE_ROUNDS"], "7")
        self.assertEqual(env["LIX_PROFILE_WASM_MEMORY_MIB"], "192")
        self.assertEqual(env["LIX_PROFILE_SPLICE_PROVENANCE"], "1")

        v1 = runner.environment("v1", 3, 7, 192)
        self.assertNotIn("LIX_PROFILE_SPLICE_PROVENANCE", v1)
        nested = runner.environment("v2", 3, 7, 192, json_shape="nested")
        self.assertEqual(nested["LIX_PROFILE_JSON_SHAPE"], "nested")
        with self.assertRaisesRegex(ValueError, "JSON shape"):
            runner.environment("v2", 3, 7, 192, json_shape="wide")

    def test_structured_cell_line_ignores_human_timing_lines(self) -> None:
        runner.validate_setup(
            "setup format=json shape=flat runtime=wasm-component-v2 mechanism_only=false "
            "properties=220000 bytes=10000000 "
            "edit_property=property_110000 plugin_archive_bytes=1234 "
            f"plugin_archive_sha256={'2' * 64}\n"
            "setup insert took 123 ms\n",
            "v2",
        )
        runner.validate_mode(
            "edit format=json shape=flat properties=220000 bytes=10000000 "
            "property=property_110000 warmups=1 rounds=2\n"
            "edit took 4 ms\n",
            "edit",
            1,
            2,
        )
        runner.validate_setup(
            "setup format=json shape=nested runtime=wasm-component-v2 "
            "mechanism_only=false properties=220000 bytes=10000000 "
            "edit_property=/payload/property_110000 plugin_archive_bytes=1234 "
            f"plugin_archive_sha256={'2' * 64}\n",
            "v2",
            json_shape="nested",
        )
        runner.validate_mode(
            "edit format=json shape=nested properties=220000 bytes=10000000 "
            "property=/payload/property_110000 warmups=1 rounds=2\n",
            "edit",
            1,
            2,
            json_shape="nested",
        )

    def test_sample_and_counter_contracts_are_exact(self) -> None:
        self.assertEqual(
            runner.measured_samples("edit sample_ms=[1.0, 2.0]\n", "edit", 2),
            [1.0, 2.0],
        )
        with self.assertRaisesRegex(ValueError, "exactly 2"):
            runner.measured_samples("edit sample_ms=[1.0]\n", "edit", 2)
        with self.assertRaisesRegex(ValueError, "finite positive"):
            runner.measured_samples("edit sample_ms=[1.0, 0.0]\n", "edit", 2)

        counter = (
            "plugin_v2_counters label=edit round=0 "
            "source_read_calls=0 source_bytes_read=0 "
            "component_boundary_bytes=64 "
            "guest_linear_memory_high_water_bytes=1048576 "
            "host_full_diff_bytes_compared=0 "
            "host_full_content_classification_bytes=0 "
            "full_state_semantic_rows_materialized=0 "
            "change_payload_requests=1 returned_change_payloads=1 "
            "durable_semantic_changes=1 private_document_cache_hits=1 "
            "shared_renderer_cache_hits=1 full_document_reparses=0 "
            "full_renderer_invocations=0 filesystem_sync_full_renders=0\n"
        )
        rows = runner.parse_v2_counters(counter, 1)
        self.assertEqual(rows[0]["round"], 0)
        self.assertEqual(rows[0]["durable_semantic_changes"], 1)
        with self.assertRaisesRegex(ValueError, "exactly 2"):
            runner.parse_v2_counters(counter, 2)

    def test_end_to_end_smoke_is_counterbalanced_and_auditable(self) -> None:
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
api = os.environ["LIX_PROFILE_JSON_API"]
shape = os.environ["LIX_PROFILE_JSON_SHAPE"]
rounds = int(os.environ["LIX_PROFILE_ROUNDS"])
warmups = int(os.environ["LIX_PROFILE_WARMUPS"])
edit_property = "property_110000" if shape == "flat" else "/payload/property_110000"
assert backend in {"rocksdb-fs", "slatedb-cached"}
assert api in {"v1", "v2"}
assert shape in {"flat", "nested"}
assert os.environ["LIX_PROFILE_FORMAT"] == "json"
assert os.environ["LIX_PROFILE_WASM_MEMORY_MIB"] == "256"
if mode == "setup":
    assert "LIX_PROFILE_SPLICE_PROVENANCE" not in os.environ
    (case / "pristine").write_text(f"{api}:{shape}", encoding="utf-8")
    runtime = "wasm-component-v1" if api == "v1" else "wasm-component-v2"
    archive_hash = ("1" if api == "v1" else "2") * 64
    print(f"setup format=json shape={shape} runtime={runtime} mechanism_only=false properties=220000 bytes=10000000 edit_property={edit_property} plugin_archive_bytes=1234 plugin_archive_sha256={archive_hash}")
    print("setup insert took 1 ms")
elif mode == "edit":
    assert (case / "pristine").read_text(encoding="utf-8") == f"{api}:{shape}"
    assert ("LIX_PROFILE_SPLICE_PROVENANCE" in os.environ) == (api == "v2")
    print(f"edit format=json shape={shape} properties=220000 bytes=10000000 property={edit_property} warmups={warmups} rounds={rounds}")
    scale = 50.0 if api == "v2" else 100.0
    print("edit sample_ms=" + repr([scale + index for index in range(rounds)]))
    print("edit took 1 ms")
    if api == "v2":
        for index in range(rounds):
            print(
                "plugin_v2_counters label=edit "
                f"round={index} source_read_calls=0 source_bytes_read=0 "
                "component_boundary_bytes=64 "
                "guest_linear_memory_high_water_bytes=1048576 "
                "host_full_diff_bytes_compared=0 "
                "host_full_content_classification_bytes=0 "
                "full_state_semantic_rows_materialized=0 "
                "change_payload_requests=1 returned_change_payloads=1 "
                "durable_semantic_changes=1 private_document_cache_hits=1 "
                "shared_renderer_cache_hits=1 full_document_reparses=0 "
                "full_renderer_invocations=0 "
                "filesystem_sync_full_renders=0"
            )
elif mode == "render":
    assert (case / "pristine").read_text(encoding="utf-8") == f"{api}:{shape}"
    assert "LIX_PROFILE_SPLICE_PROVENANCE" not in os.environ
    print(f"render format=json shape={shape} properties=220000 warmups={warmups} rounds={rounds}")
    print("render sample_ms=" + repr([80.0 + index for index in range(rounds)]))
    print("render took 1 ms")
else:
    raise AssertionError(mode)
""",
                encoding="utf-8",
            )
            binary.chmod(0o755)
            v1_manifest = parent / "v1-manifest.json"
            v2_manifest = parent / "v2-manifest.json"
            write_manifest(
                v1_manifest, runtime="wasm-component-v1", api_version="0.1.0"
            )
            write_manifest(
                v2_manifest, runtime="wasm-component-v2", api_version="2.0.0"
            )

            run_dir = parent / "paired"
            output = runner.run_campaign(
                binary,
                run_dir,
                v1_manifest=v1_manifest,
                v2_manifest=v2_manifest,
                blocks=2,
                warmups=1,
                samples=2,
                memory_mib=256,
                bootstrap_draws=200,
                timeout_seconds=30,
            )
            artifact = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(artifact["status"], "complete")
            self.assertFalse(artifact["design"]["acceptance_eligible"])
            self.assertTrue(artifact["design"]["same_benchmark_executable"])
            self.assertEqual(artifact["design"]["json_shape"], "flat")
            self.assertEqual(
                artifact["design"]["edit_property"], "property_110000"
            )
            self.assertEqual(artifact["benchmark"]["sha256"], runner.sha256_file(binary))
            self.assertEqual(
                artifact["plugins"]["v2"]["sha256"],
                runner.sha256_file(v2_manifest),
            )

            for backend in runner.BACKENDS:
                backend_result = artifact["backends"][backend]
                self.assertEqual(backend_result["status"], "complete")
                for metric in ("edit", "render"):
                    blocks = backend_result[metric]
                    self.assertEqual(
                        [block["order"] for block in blocks],
                        ["v1-v2", "v2-v1"],
                    )
                    for block in blocks:
                        self.assertEqual(set(block["arms"]), {"v1", "v2"})
                        for api in runner.APIS:
                            arm = block["arms"][api]
                            self.assertEqual(len(arm["sample_ms"]), 2)
                            log = run_dir / arm["log"]["path"]
                            self.assertTrue(log.is_file())
                            self.assertEqual(
                                arm["log"]["sha256"], runner.sha256_file(log)
                            )
                    if metric == "edit":
                        for block in blocks:
                            self.assertEqual(
                                len(block["arms"]["v2"]["counters"]), 2
                            )
                    else:
                        for block in blocks:
                            self.assertEqual(block["arms"]["v2"]["counters"], [])
            self.assertEqual(list((run_dir / "cases").glob("*/*/*")), [])

            gate_result = gate.evaluate(artifact, allow_smoke=True)
            self.assertFalse(gate_result["acceptance_eligible"])
            self.assertTrue(gate_result["statistical_and_counter_pass"])
            self.assertEqual(
                gate_result["decision"],
                "smoke-only; checks passed, no acceptance decision",
            )

            rocks_before = json.loads(
                json.dumps(artifact["backends"]["rocksdb-fs"])
            )
            artifact["status"] = "running"
            artifact["backends"]["slatedb-cached"]["status"] = "running"
            artifact["backends"]["slatedb-cached"]["edit"] = []
            artifact["backends"]["slatedb-cached"]["render"] = []
            runner.write_artifact(output, artifact)
            for api in runner.APIS:
                template = run_dir / "templates" / "slatedb-cached" / api
                template.mkdir(parents=True)
                (template / "pristine").write_text(
                    f"{api}:flat", encoding="utf-8"
                )
            stale_case = (
                run_dir
                / "cases"
                / "slatedb-cached"
                / "edit"
                / "block-00-v1"
            )
            stale_case.mkdir(parents=True)
            (stale_case / "partial-copy").write_text("stale", encoding="utf-8")

            with self.assertRaisesRegex(
                ValueError, "resume arguments do not match"
            ):
                runner.run_campaign(
                    binary,
                    run_dir,
                    v1_manifest=v1_manifest,
                    v2_manifest=v2_manifest,
                    blocks=2,
                    warmups=1,
                    samples=2,
                    memory_mib=256,
                    bootstrap_draws=200,
                    timeout_seconds=30,
                    json_shape="nested",
                    resume=True,
                    resume_reason="attempted mismatched nested continuation",
                )

            resumed_output = runner.run_campaign(
                binary,
                run_dir,
                v1_manifest=v1_manifest,
                v2_manifest=v2_manifest,
                blocks=2,
                warmups=1,
                samples=2,
                memory_mib=256,
                bootstrap_draws=200,
                timeout_seconds=30,
                resume=True,
                resume_reason="test interruption before first SlateDB sample",
            )
            resumed = json.loads(resumed_output.read_text(encoding="utf-8"))
            self.assertEqual(resumed["status"], "complete")
            self.assertEqual(
                resumed["backends"]["rocksdb-fs"],
                rocks_before,
            )
            self.assertEqual(
                resumed["continuations"][-1]["reason"],
                "test interruption before first SlateDB sample",
            )
            self.assertEqual(
                resumed["continuations"][-1]["segment_resumed"],
                {"backend": "slatedb-cached", "first_block": 0},
            )
            self.assertEqual(
                resumed["continuations"][-1]["segments_preserved"][
                    "rocksdb-fs"
                ],
                {"status": "complete", "paired_blocks": 2},
            )
            self.assertEqual(
                resumed["runner"],
                artifact["runner"],
            )
            self.assertFalse(stale_case.exists())
            self.assertEqual(list((run_dir / "cases").glob("*/*/*")), [])

            nested_run_dir = parent / "paired-nested"
            nested_output = runner.run_campaign(
                binary,
                nested_run_dir,
                v1_manifest=v1_manifest,
                v2_manifest=v2_manifest,
                blocks=2,
                warmups=1,
                samples=2,
                memory_mib=256,
                bootstrap_draws=200,
                timeout_seconds=30,
                json_shape="nested",
            )
            nested_artifact = json.loads(
                nested_output.read_text(encoding="utf-8")
            )
            self.assertEqual(nested_artifact["status"], "complete")
            self.assertEqual(nested_artifact["design"]["json_shape"], "nested")
            self.assertEqual(
                nested_artifact["design"]["edit_property"],
                "/payload/property_110000",
            )
            for backend in runner.BACKENDS:
                for setup in nested_artifact["backends"][backend]["setup"].values():
                    setup_log = nested_run_dir / setup["log"]["path"]
                    self.assertIn(
                        "shape=nested",
                        setup_log.read_text(encoding="utf-8"),
                    )


if __name__ == "__main__":
    unittest.main()
