#!/usr/bin/env python3
"""Focused tests for the benchmark runner/report contract."""

from __future__ import annotations

import gzip
import importlib.util
import inspect
import json
import pathlib
import tempfile
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[3]
SCRIPT = pathlib.Path(__file__).with_name("plugin_api_benchmark.py")
SPEC = importlib.util.spec_from_file_location("plugin_api_benchmark", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def complete_profile(lane: str = "csv-file-roundtrip", sample: int = 0) -> dict:
    counters = {name: 0 for name in MODULE.REQUIRED_CANDIDATE_COUNTERS}
    counters["typed_row_decode_records"] = 1
    counters["row_output_records"] = 1
    counters["typed_row_schema_validation_calls"] = 1
    return {
        "schema": "lix.universal-plugin-transition-profile.v1",
        "lane": lane,
        "sample": sample,
        "phases_ms": {"parse_or_import": 1.0, "total": 1.0},
        "correctness": {"logical_rows": 1},
        "counters": counters,
    }


def complete_sample(lane: str = "csv-file-roundtrip", sample: int = 0) -> dict:
    metrics = {name: 1 for name in MODULE.REQUIRED_SAMPLE_METRICS}
    metrics["live_bytes_delta"] = 0
    metrics["process_rss_delta_bytes"] = 0
    return {
        "kind": "sample",
        "lane": lane,
        "sample": sample,
        "fixture": {"input_bytes": 1, "logical_rows": 1},
        "metrics": metrics,
    }


class PluginApiBenchmarkTests(unittest.TestCase):
    def test_hard_cut_audit_requires_non_vacuous_zero_json_controls(self) -> None:
        audit = MODULE.hard_cut_source_audit(ROOT)
        self.assertEqual(audit["status"], "pass")
        self.assertTrue(all(audit["instrumentation_positive_controls"].values()))

    def test_profile_revision_order_alternates_for_every_lane(self) -> None:
        lanes = ["csv-file-roundtrip", "json-file-roundtrip", "text-file-roundtrip"]
        orders = [MODULE.profile_revision_order(index) for index, _ in enumerate(lanes)]
        self.assertEqual(
            orders,
            [
                ("baseline", "candidate"),
                ("candidate", "baseline"),
                ("baseline", "candidate"),
            ],
        )

    def test_merge_profiles_use_extended_on_cpu_scope(self) -> None:
        self.assertEqual(MODULE.profile_sample_count("csv-file-roundtrip"), 101)
        self.assertEqual(MODULE.profile_sample_count("csv-direct-row-mutation"), 1)
        self.assertEqual(MODULE.profile_sample_count("json-ten-mib-paged-roundtrip"), 1)
        self.assertEqual(MODULE.profile_sample_count("csv-same-row-column-merge"), 255)
        self.assertFalse(MODULE.profile_requires_attach_capture("csv-file-roundtrip"))
        self.assertEqual(
            MODULE.profile_minimum_retained_samples("csv-same-row-column-merge"),
            MODULE.PROFILE_MINIMUM_RETAINED_SAMPLES,
        )
        self.assertEqual(
            MODULE.profile_minimum_retained_samples("csv-direct-row-mutation"), 5
        )
        self.assertGreaterEqual(
            MODULE.profile_minimum_retained_samples("csv-file-roundtrip"), 25
        )
        self.assertEqual(
            MODULE.profile_minimum_guest_samples("csv-file-roundtrip"), 5
        )
        self.assertEqual(
            MODULE.profile_minimum_guest_samples("csv-direct-row-mutation"), 3
        )
        self.assertEqual(
            MODULE.profile_minimum_guest_samples("csv-same-row-column-merge"), 5
        )
        self.assertEqual(MODULE.profile_sampling_rate("csv-file-roundtrip"), 1_000)
        self.assertEqual(MODULE.profile_sampling_rate("csv-direct-row-mutation"), 10_000)
        self.assertEqual(MODULE.profile_sampling_rate("csv-same-row-column-merge"), 10_000)
        self.assertEqual(
            MODULE.profile_expected_guest_symbol("csv-same-row-column-merge", "baseline"),
            "plugin_csv",
        )
        self.assertIn(
            "sample_count >= minimum_samples",
            inspect.getsource(MODULE.validate_cpu_profile),
        )

    def test_profiler_launches_benchmark_to_capture_short_lived_threads(self) -> None:
        source = inspect.getsource(MODULE.collect_cpu_profiles)
        self.assertIn('[*profiler_command, "--", *benchmark_profile_command]', source)
        self.assertIn('"--reuse-threads"', source)
        self.assertLessEqual(MODULE.PROFILE_ARM_DELAY_SECONDS, 0.025)
        self.assertIn("attached_process_ephemeral_workers_best_effort", source)
        self.assertIn('"--per-cpu-threads"', source)

    def test_runner_checkpoints_measurements_before_cpu_profiles(self) -> None:
        source = inspect.getsource(MODULE.run_orchestration)
        checkpoint = source.index('metadata["baseline"] = baseline_run')
        profiles = source.index('metadata["cpu_profiles"] = collect_cpu_profiles')
        self.assertLess(checkpoint, profiles)
        self.assertIn('run["status"] == "passed"', source)
        self.assertIn("paired measurements must pass before CPU profile collection", source)

    def test_paired_runner_is_globally_fail_fast(self) -> None:
        source = inspect.getsource(MODULE.run_paired_benchmarks)
        self.assertIn("aborted = True", source)
        self.assertIn("aborted_after_first_failure", source)
        self.assertNotIn("if returncodes[label]:\n                    continue", source)

    def test_report_and_profile_resume_share_checkpoint_verification(self) -> None:
        source = inspect.getsource(MODULE.profile_existing)
        report_source = inspect.getsource(MODULE.report_existing)
        self.assertIn("verify_measurement_checkpoint", source)
        self.assertIn("verify_measurement_checkpoint", report_source)
        self.assertIn("working_tree_sha256", source)
        self.assertIn("no longer matches the paired-measurement checkpoint", source)

    def test_qualification_enforces_host_and_toolchain_pins(self) -> None:
        qualification = json.loads(
            (ROOT / "packages/e2e/benchmarks/plugin_api_qualification.json").read_text()
        )
        requirements = qualification["environment_requirements"]
        self.assertEqual(requirements["machine"], "x86_64")
        with self.assertRaisesRegex(ValueError, "environment mismatch"):
            MODULE.enforce_environment_requirements(
                {**requirements, "cpu_governor": "powersave"}, requirements
            )

    def test_every_profiled_workload_has_balanced_barriers(self) -> None:
        source = (ROOT / "packages/e2e/tests/plugin_api_benchmarks.rs").read_text()
        self.assertEqual(source.count("cpu_profile_barrier(lane);"), 8)
        self.assertEqual(source.count("cpu_profile_end(lane);"), 8)
        self.assertGreaterEqual(MODULE.PROFILE_SAMPLES, 31)
        self.assertIn("completed_iterations < expected_iterations", source)

    def test_normalized_workload_adapts_native_jsonb_to_baseline_text(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "plugin_api_benchmarks.rs"
            path.write_text(
                MODULE.JSONB_SCALAR_UPDATE_PARAM
                + "\n"
                + MODULE.JSONB_SCALAR_QUERY_PARAM
                + "\n"
            )
            normalized = MODULE.normalized_workload_bytes(path).decode()
        self.assertNotIn("Value::Jsonb", normalized)
        self.assertEqual(normalized.count("Value::Text"), 2)

    def test_normalized_workload_adapts_excalidraw_jsonb_to_baseline_text(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "plugin_api_benchmarks.rs"
            path.write_text("\n".join(MODULE.EXCALIDRAW_JSONB_PARAMS))
            normalized = MODULE.normalized_workload_bytes(path).decode()
        self.assertNotIn("Value::Jsonb", normalized)
        self.assertEqual(normalized.count("Value::Text"), 4)

    def test_cpu_profile_trim_uses_samply_monotonic_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = pathlib.Path(temporary) / "profile.json.gz"
            profile = {
                "meta": {"startTime": 1_787_000_000_000.0},
                "threads": [
                    {
                        "samples": {
                            "time": [1_507_000_000.0, 1_507_000_250.0, 1_507_000_251.0],
                            "stack": [0, 1, 2],
                        }
                    }
                ],
            }
            with gzip.open(artifact, "wt") as stream:
                json.dump(profile, stream)
            result = MODULE.trim_cpu_profile_to_monotonic_interval(
                artifact,
                start_monotonic_ms=1_507_000_249.0,
                end_monotonic_ms=1_507_000_252.0,
            )
            with gzip.open(artifact, "rt") as stream:
                trimmed = json.load(stream)
        self.assertEqual(result["retained_samples"], 2)
        self.assertEqual(trimmed["threads"][0]["samples"]["stack"], [1, 2])

    def test_cpu_profile_requires_guest_symbol_in_a_sampled_stack(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = pathlib.Path(temporary) / "profile.json.gz"
            sidecar = pathlib.Path(temporary) / "profile.syms.json"
            profile = {
                "meta": {"product": "lix-candidate-json-file-roundtrip"},
                "threads": [
                    {
                        "samples": {"stack": [0, 0, 0]},
                        "stackTable": {"prefix": [None], "frame": [0]},
                        "frameTable": {"func": [0]},
                        "funcTable": {"name": [0]},
                        "stringArray": ["plugin_json::JsonPlugin::parse"],
                    }
                ],
            }
            with gzip.open(artifact, "wt", encoding="utf-8") as stream:
                json.dump(profile, stream)
            sidecar.write_text(
                json.dumps({"symbols": ["plugin_api_public_workflows"]}),
                encoding="utf-8",
            )
            valid = MODULE.validate_cpu_profile(
                artifact,
                sidecar,
                expected_profile_name="lix-candidate-json-file-roundtrip",
                expected_guest_symbol="plugin_json",
                minimum_samples=3,
                minimum_guest_samples=3,
            )
            self.assertTrue(valid["valid"])
            self.assertEqual(valid["samples_with_guest_frames"], 3)

            profile["threads"][0]["stringArray"] = ["host_only"]
            profile["meta"]["known_guest_symbol"] = "plugin_json"
            with gzip.open(artifact, "wt", encoding="utf-8") as stream:
                json.dump(profile, stream)
            invalid = MODULE.validate_cpu_profile(
                artifact,
                sidecar,
                expected_profile_name="lix-candidate-json-file-roundtrip",
                expected_guest_symbol="plugin_json",
                minimum_samples=3,
                minimum_guest_samples=3,
            )
            self.assertFalse(invalid["valid"])
            self.assertEqual(invalid["samples_with_guest_frames"], 0)

    def test_cpu_profile_artifacts_are_reopened_and_digest_verified(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            artifact = directory / "candidate-json-file-roundtrip.json.gz"
            sidecar = directory / "candidate-json-file-roundtrip.json.syms.json"
            log = directory / "candidate-json-file-roundtrip.log"
            profile = {
                "meta": {"product": "lix-candidate-json-file-roundtrip"},
                "threads": [
                    {
                        "samples": {"stack": [0] * 25},
                        "stackTable": {"prefix": [None], "frame": [0]},
                        "frameTable": {"func": [0]},
                        "funcTable": {"name": [0]},
                        "stringArray": ["plugin_json::JsonPlugin::parse"],
                    }
                ],
            }
            with gzip.open(artifact, "wt", encoding="utf-8") as stream:
                json.dump(profile, stream)
            sidecar.write_text(
                json.dumps({"symbols": ["plugin_api_public_workflows"]}),
                encoding="utf-8",
            )
            log.write_text(
                MODULE.MACHINE_PREFIX
                + json.dumps(complete_sample("json-file-roundtrip"))
                + "\n",
                encoding="utf-8",
            )
            structure = MODULE.validate_cpu_profile(
                artifact,
                sidecar,
                expected_profile_name="lix-candidate-json-file-roundtrip",
                expected_guest_symbol="plugin_json",
            )
            item = {
                "revision": "candidate",
                "status": "passed",
                "lane": "json-file-roundtrip",
                "profile": str(artifact),
                "profile_sha256": MODULE.sha256_file(artifact),
                "presymbolicated_sidecar": str(sidecar),
                "presymbolicated_sidecar_sha256": MODULE.sha256_file(sidecar),
                "log": str(log),
                "log_sha256": MODULE.sha256_file(log),
                "samples": 1,
                "lane_sample_records": 1,
                "profile_structure": structure,
            }
            evidence = {"status": "complete", "artifacts": [item]}
            failures = MODULE.cpu_profile_evidence_failures(
                evidence, {"json-file-roundtrip"}
            )
            self.assertTrue(any("missing" in failure for failure in failures))
            baseline_artifact = directory / "baseline-json-file-roundtrip.json.gz"
            baseline_profile = json.loads(json.dumps(profile).replace("candidate", "baseline"))
            with gzip.open(baseline_artifact, "wt", encoding="utf-8") as stream:
                json.dump(baseline_profile, stream)
            baseline_structure = MODULE.validate_cpu_profile(
                baseline_artifact,
                sidecar,
                expected_profile_name="lix-baseline-json-file-roundtrip",
                expected_guest_symbol="plugin_json",
            )
            evidence["artifacts"].append(
                {
                    **item,
                    "revision": "baseline",
                    "profile": str(baseline_artifact),
                    "profile_sha256": MODULE.sha256_file(baseline_artifact),
                    "profile_structure": baseline_structure,
                }
            )
            self.assertEqual(
                MODULE.cpu_profile_evidence_failures(
                    evidence, {"json-file-roundtrip"}
                ),
                [],
            )
            sidecar.write_text("{}", encoding="utf-8")
            self.assertTrue(
                MODULE.cpu_profile_evidence_failures(
                    evidence, {"json-file-roundtrip"}
                )
            )

    def test_normalized_workload_ignores_only_baseline_missing_counter_lines(self) -> None:
        path = ROOT / "packages/e2e/tests" / "benchmark_metrics.rs"
        original = path.read_text(encoding="utf-8")
        normalized = MODULE.normalized_workload_bytes(path).decode("utf-8")
        self.assertIn("component_boundary_bytes", normalized)
        self.assertNotIn('"typed_row_decode_records"', normalized)
        self.assertIn("typed_row_decode_records", original)
        self.assertIn(MODULE.BASELINE_ROW_PAGE_CALLBACK_METRIC, normalized)
        self.assertNotIn(MODULE.ROW_PAGE_CALLBACK_METRIC, normalized)

    def test_counter_profiles_report_per_lane_percentiles(self) -> None:
        records = [
            {
                "schema": "lix.universal-plugin-transition-profile.v1",
                "lane": "csv-file-roundtrip",
                "counters": {"component_boundary_bytes": value},
            }
            for value in (10, 20, 30)
        ]
        counters = MODULE.counter_profiles(records)["csv-file-roundtrip"]
        self.assertEqual(counters["component_boundary_bytes.p50"], 20)
        self.assertEqual(counters["component_boundary_bytes.p95"], 30)

    def test_parser_keeps_machine_and_transition_records(self) -> None:
        records = MODULE.parse_records(
            "noise\n"
            + MODULE.MACHINE_PREFIX
            + json.dumps({"kind": "sample", "lane": "csv-file-roundtrip"})
            + "\n"
            + MODULE.TRANSITION_PREFIX
            + json.dumps(
                {
                    "schema": "lix.universal-plugin-transition-profile.v1",
                    "lane": "csv-file-roundtrip",
                    "phases_ms": {"parse_or_import": 2.0},
                    "counters": {"outer_row_json_parse_calls": 0},
                }
            )
        )
        self.assertEqual(len(records), 2)
        self.assertEqual(records[1]["phases_ms"]["parse_or_import"], 2.0)

    def test_fresh_process_summary_is_not_an_indexed_sample_record(self) -> None:
        summary = {"kind": "summary", "lane": "csv-file-roundtrip"}
        sample = {"kind": "sample", "lane": "csv-file-roundtrip", "sample": 0}
        profile = {
            "schema": "lix.universal-plugin-transition-profile.v1",
            "lane": "csv-file-roundtrip",
            "sample": 0,
        }
        indexed = [
            record
            for record in (summary, sample, profile)
            if MODULE.is_indexed_measurement_record(record)
        ]
        self.assertEqual(indexed, [sample, profile])

    def test_cross_plugin_report_names_work_phase_not_total(self) -> None:
        report = MODULE.cross_plugin_report(
            {
                "lanes": {"csv-file-roundtrip": {}},
                "phase_profiles": {
                    "csv-file-roundtrip": {
                        "total": {"p95_ms": 12.0},
                        "parse_or_import": {"p95_ms": 7.0},
                        "serialize_or_export": {"p95_ms": 5.0},
                    }
                },
            },
            {"lanes": {}},
        )
        self.assertEqual(
            report["csv"]["largest_remaining_measured_phase"]["phase"],
            "parse_or_import",
        )

    def test_report_marks_missing_baseline_without_fabricating_comparison(self) -> None:
        baseline = {
            "label": "baseline",
            "status": "unavailable",
            "returncode": None,
            "elapsed_seconds": 0,
            "command": [],
            "worktree": None,
            "log": "baseline.log",
            "records": "baseline.records.jsonl",
            "record_count": 0,
            "summary_count": 0,
            "transition_profile_count": 0,
        }
        candidate = {
            "label": "candidate",
            "status": "passed",
            "returncode": 0,
            "elapsed_seconds": 1,
            "command": [],
            "worktree": ".",
            "log": "candidate.log",
            "records": "candidate.records.jsonl",
            "record_count": 1,
            "summary_count": 0,
            "transition_profile_count": 1,
        }
        profile = complete_profile()
        sample = complete_sample()
        report, exit_code = MODULE.build_report(
            ROOT,
            {"corpus": {"sha256": "test", "manifest": {"lanes": ["csv-file-roundtrip"]}}},
            baseline,
            candidate,
            [],
            [profile, sample],
            require_baseline=False,
        )
        self.assertEqual(exit_code, 0)
        self.assertEqual(report["gate"]["status"], "baseline_unavailable")
        self.assertEqual(report["comparison"]["common_lanes"], [])
        self.assertIn("the pinned baseline benchmark is unavailable", report["remaining_blockers"])
        self.assertEqual(
            report["candidate"]["outer_row_json_status"], "proven_zero"
        )
        _, strict_exit_code = MODULE.build_report(
            ROOT,
            {"corpus": {"sha256": "test", "manifest": {"lanes": ["csv-file-roundtrip"]}}},
            baseline,
            candidate,
            [],
            [profile, sample],
            require_baseline=True,
        )
        self.assertEqual(strict_exit_code, 2)

    def test_report_fails_nonzero_outer_json_counter(self) -> None:
        run = {
            "label": "candidate",
            "status": "passed",
            "returncode": 0,
            "elapsed_seconds": 1,
            "command": [],
            "worktree": ".",
            "log": "candidate.log",
            "records": "candidate.records.jsonl",
            "record_count": 1,
            "summary_count": 1,
            "transition_profile_count": 1,
        }
        profile = complete_profile()
        profile["counters"]["outer_row_json_parse_calls"] = 1
        report, exit_code = MODULE.build_report(
            ROOT,
            {"corpus": {"manifest": {"lanes": ["csv-file-roundtrip"]}}},
            {**run, "label": "baseline", "status": "unavailable"},
            run,
            [],
            [profile],
            require_baseline=False,
        )
        self.assertEqual(exit_code, 1)
        self.assertEqual(
            report["gate"]["status"], "typed_row_zero_json_invariant_unproven"
        )
        self.assertTrue(report["remaining_blockers"])

    def test_comparable_report_reaches_regression_gate_without_crashing(self) -> None:
        run = {
            "status": "passed",
            "returncode": 0,
            "elapsed_seconds": 1,
            "command": [],
            "worktree": ".",
            "log": "run.log",
            "records": "run.records.jsonl",
            "record_count": 2,
            "summary_count": 0,
            "transition_profile_count": 1,
        }
        records = [complete_profile(), complete_sample()]
        report, exit_code = MODULE.build_report(
            ROOT,
            {
                "samples": 1,
                "corpus": {
                    "manifest": {"lanes": ["csv-file-roundtrip"]},
                },
            },
            {**run, "label": "baseline"},
            {**run, "label": "candidate"},
            records,
            records,
            require_baseline=True,
        )
        self.assertEqual(exit_code, 1)
        self.assertEqual(report["gate"]["status"], "regression")
        self.assertEqual(report["comparison"]["common_lanes"], ["csv-file-roundtrip"])

    def test_missing_outer_json_byte_counter_cannot_prove_zero(self) -> None:
        profile = complete_profile()
        del profile["counters"]["outer_row_json_parse_bytes"]
        totals, failures = MODULE.transition_counter_totals([profile])
        self.assertEqual(totals["outer_row_json_parse_bytes"], 0)
        self.assertTrue(any("outer_row_json_parse_bytes" in failure for failure in failures))

    def test_verified_jsonl_rejects_modified_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "candidate.records.jsonl"
            path.write_text(json.dumps(complete_sample()) + "\n", encoding="utf-8")
            digest = MODULE.sha256_file(path)
            self.assertEqual(len(MODULE.verified_jsonl_records(path, digest)), 1)
            path.write_text("{}\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                MODULE.verified_jsonl_records(path, digest)

    def test_checkpoint_rejects_paired_fixture_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            lane = "csv-file-roundtrip"
            baseline = [complete_sample(lane), complete_profile(lane)]
            candidate = [complete_sample(lane), complete_profile(lane)]
            candidate[0]["fixture"]["input_bytes"] = 2
            metadata = {
                "corpus": {"manifest": {"lanes": [lane]}},
                "baseline": {"status": "passed"},
                "candidate_run": {"status": "passed"},
            }
            for label, records, key in (
                ("baseline", baseline, "baseline"),
                ("candidate", candidate, "candidate_run"),
            ):
                path = output / f"{label}.records.jsonl"
                path.write_text(
                    "".join(json.dumps(record) + "\n" for record in records),
                    encoding="utf-8",
                )
                metadata[key]["records_sha256"] = MODULE.sha256_file(path)
            with self.assertRaisesRegex(ValueError, "fixture mismatch"):
                MODULE.verify_measurement_checkpoint(
                    output,
                    metadata,
                    require_passed=True,
                    authenticate_sources=False,
                )

    def test_checkpoint_rejects_lane_inventory_mismatch(self) -> None:
        metadata = {
            "corpus": {"manifest": {"lanes": ["csv-file-roundtrip"]}},
            "baseline": {"status": "passed"},
        }
        failures = MODULE.measurement_checkpoint_failures(
            metadata,
            [complete_sample("json-file-roundtrip"), complete_profile("json-file-roundtrip")],
            [complete_sample(), complete_profile()],
        )
        self.assertTrue(any("baseline sample lanes" in failure for failure in failures))

    def test_checkpoint_rejects_stale_tree_qualification_and_workload(self) -> None:
        tree = {"head": "head", "status_sha256": "status", "working_tree_sha256": "tree"}
        qualification = {"sha256": "qualification", "spec": {"schema": "test"}}
        corpus = {
            "sha256": "corpus",
            "manifest": {"lanes": [], "default_samples": 61, "warmup_samples": 5},
        }
        workload = {"files": {"workload": "digest"}, "contract_sha256": "contract"}
        metadata = {
            "root": str(ROOT),
            "baseline_revision": MODULE.PINNED_BASELINE_REVISION,
            "candidate": tree,
            "qualification": qualification,
            "corpus": corpus,
            "samples": 61,
            "warmups": 5,
            "baseline": {"workload": workload},
            "candidate_run": {"workload": workload},
        }
        cases = (
            ("working tree", {**tree, "working_tree_sha256": "changed"}, qualification, workload),
            ("qualification spec", tree, {**qualification, "sha256": "changed"}, workload),
            ("workload contract", tree, qualification, {**workload, "contract_sha256": "changed"}),
        )
        for expected, current_tree, current_qualification, current_workload in cases:
            with self.subTest(expected=expected), mock.patch.object(
                MODULE, "working_tree_metadata", return_value=current_tree
            ), mock.patch.object(
                MODULE, "read_qualification_spec", return_value=current_qualification
            ), mock.patch.object(
                MODULE, "read_corpus_manifest", return_value=corpus
            ), mock.patch.object(
                MODULE, "workload_metadata", return_value=current_workload
            ):
                with self.assertRaisesRegex(ValueError, expected):
                    MODULE.verify_measurement_checkpoint(
                        pathlib.Path("/unused"), metadata, require_passed=False
                    )

    def test_paired_p95_requires_tail_sample_count(self) -> None:
        result = MODULE.paired_quantile_comparison(
            {index: 1.0 for index in range(21)},
            {index: 1.0 for index in range(21)},
            fraction=0.95,
            limit=1.15,
            seed_text="test",
        )
        self.assertEqual(result["status"], "insufficient_samples")
        self.assertEqual(result["minimum_samples"], 61)

    def test_paired_comparison_distinguishes_confirmed_from_inconclusive(self) -> None:
        baseline = {index: 1.0 for index in range(61)}
        confirmed = MODULE.paired_quantile_comparison(
            baseline,
            {index: 2.0 for index in range(61)},
            fraction=0.95,
            limit=1.15,
            seed_text="confirmed",
        )
        inconclusive = MODULE.paired_quantile_comparison(
            baseline,
            {index: 1.20 if index < 31 else 1.0 for index in range(61)},
            fraction=0.5,
            limit=1.10,
            seed_text="inconclusive",
        )
        self.assertEqual(confirmed["status"], "confirmed_regression")
        self.assertEqual(inconclusive["status"], "inconclusive_regression")

    def test_absolute_delta_pass_requires_ci_and_proportional_ceiling(self) -> None:
        baseline = {index: 10.0 for index in range(61)}
        within_both = MODULE.paired_quantile_comparison(
            baseline,
            {index: 11.0 for index in range(61)},
            fraction=0.95,
            limit=1.05,
            absolute_limit=2.0,
            proportional_ceiling=1.20,
            seed_text="absolute-pass",
        )
        beyond_ceiling = MODULE.paired_quantile_comparison(
            baseline,
            {index: 15.0 for index in range(61)},
            fraction=0.95,
            limit=1.05,
            absolute_limit=10.0,
            proportional_ceiling=1.20,
            seed_text="absolute-ceiling",
        )
        self.assertEqual(within_both["status"], "pass")
        self.assertEqual(
            within_both["pass_basis"],
            "absolute_delta_and_proportional_ceiling",
        )
        self.assertEqual(
            within_both["paired_bootstrap_absolute_delta_ci95"], [1.0, 1.0]
        )
        self.assertEqual(beyond_ceiling["status"], "confirmed_regression")

    def test_comparison_gates_workloads_not_protocol_shape_and_records_pareto_tradeoff(
        self,
    ) -> None:
        lane = "json-ten-mib-paged-roundtrip"

        def run(*, candidate: bool) -> dict:
            metrics = {
                name: (20 * 1024 * 1024 if candidate else 10 * 1024 * 1024)
                if name == "allocated_bytes"
                else 1.0
                for name in MODULE.REQUIRED_SAMPLE_METRICS
            }
            if candidate:
                metrics["elapsed_ms"] = 0.7
                metrics["physical_written_bytes"] = 0.4
            counters = {name: 1 for name, _, _ in MODULE.COMPARE_COUNTERS}
            counters.update(
                {
                    "typed_row_decode_records": 1,
                    "typed_row_encode_records": 1,
                    "typed_row_schema_validation_calls": 1,
                    "typed_transaction_validation_calls": 0,
                }
            )
            if candidate:
                counters["row_page_callback_calls"] = 2
            return {
                "lanes": {lane: {}},
                "lane_fixtures": {lane: {"input_bytes": 1, "logical_rows": 1}},
                "samples_by_lane": {
                    lane: {index: dict(metrics) for index in range(61)}
                },
                "profiles_by_lane": {
                    lane: {
                        index: {
                            "counters": dict(counters),
                            "phases_ms": {"parse_or_import": 2.0 if candidate else 1.0},
                        }
                        for index in range(61)
                    }
                },
            }

        comparison, failures = MODULE.compare_runs(
            run(candidate=False),
            run(candidate=True),
            paired_cpu_profile_lanes={lane},
        )
        self.assertEqual(failures, [])
        lane_result = comparison["lanes"][lane]
        self.assertEqual(
            lane_result["allocated_bytes.p50"]["gate_status"],
            "profiled_pareto_tradeoff",
        )
        self.assertEqual(
            lane_result["transition_counters"]["row_page_callback_calls.p50"][
                "gate_status"
            ],
            "observational",
        )
        self.assertEqual(
            lane_result["phases"]["parse_or_import"]["p50_ms"]["gate_status"],
            "diagnostic",
        )

    def test_pareto_tradeoff_requires_explicit_lane_cpu_pair_and_typed_evidence(self) -> None:
        lane = "json-ten-mib-paged-roundtrip"
        self.assertEqual(
            MODULE.PARETO_EXCEPTION_LANES,
            {lane, "text-large-typed-attachment-roundtrip"},
        )

        def summary(*, typed: bool) -> dict:
            metrics = {name: 1.0 for name in MODULE.REQUIRED_SAMPLE_METRICS}
            metrics["allocated_bytes"] = 20 * 1024 * 1024
            metrics["elapsed_ms"] = 0.7
            metrics["physical_written_bytes"] = 0.4
            counters = {name: 1 for name, _, _ in MODULE.COMPARE_COUNTERS}
            counters.update(
                {
                    name: int(typed)
                    for name in MODULE.TYPED_TRANSITION_COUNTERS
                }
            )
            return {
                "lanes": {lane: {}},
                "lane_fixtures": {lane: [{"input_bytes": 1}]},
                "samples_by_lane": {
                    lane: {index: dict(metrics) for index in range(61)}
                },
                "profiles_by_lane": {
                    lane: {
                        index: {"counters": dict(counters), "phases_ms": {"total": 1.0}}
                        for index in range(61)
                    }
                },
            }

        baseline = summary(typed=True)
        for values in baseline["samples_by_lane"][lane].values():
            values["allocated_bytes"] = 10 * 1024 * 1024
            values["elapsed_ms"] = 1.0
            values["physical_written_bytes"] = 1.0
        _, no_cpu_failures = MODULE.compare_runs(baseline, summary(typed=True))
        _, no_typed_failures = MODULE.compare_runs(
            baseline,
            summary(typed=False),
            paired_cpu_profile_lanes={lane},
        )
        self.assertTrue(any("allocated_bytes" in failure for failure in no_cpu_failures))
        self.assertTrue(any("allocated_bytes" in failure for failure in no_typed_failures))

    def test_fresh_process_environment_passes_real_sample_and_warmups(self) -> None:
        environment = MODULE.benchmark_environment(
            pathlib.Path("/tmp/target"),
            lane="json-file-roundtrip",
            sample_index=37,
            warmups=5,
        )
        self.assertEqual(environment["LIX_PLUGIN_API_BENCH_SAMPLES"], "1")
        self.assertEqual(environment["LIX_PLUGIN_API_BENCH_SAMPLE_INDEX"], "37")
        self.assertEqual(environment["LIX_PLUGIN_API_BENCH_WARMUPS"], "5")

    def test_markdown_report_contains_auditable_gate_and_plugins(self) -> None:
        markdown = MODULE.render_markdown_report(
            {
                "gate": {"status": "pass", "exit_code": 0, "failures": []},
                "metadata": {
                    "baseline_revision": MODULE.PINNED_BASELINE_REVISION,
                    "samples": 61,
                    "warmups": 5,
                    "environment": {},
                },
                "candidate": {"outer_row_json_status": "proven_zero"},
                "invariants": {
                    "required_evidence": {"candidate_complete": True},
                    "cpu_profiles": {"status": "complete"},
                },
                "cross_plugin": {
                    "csv": {
                        "lanes": ["csv-file-roundtrip"],
                        "elapsed_p50": [],
                        "largest_remaining_measured_phase": None,
                    }
                },
            }
        )
        self.assertIn("Gate: `pass`", markdown)
        self.assertIn("| csv | 1 |", markdown)


if __name__ == "__main__":
    unittest.main()
