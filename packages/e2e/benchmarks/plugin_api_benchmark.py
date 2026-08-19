#!/usr/bin/env python3
"""Run and compare the matched plugin API benchmark.

The orchestration code uses only Python's standard library; qualification also
requires the pinned ``samply`` executable for CPU profiles. It runs the
candidate in the current worktree and the requested baseline in an isolated
detached worktree, records the exact command and environment, preserves raw
logs, extracts the benchmark's JSONL records, and writes a comparison report.

The baseline is comparable only when it has the same pinned workload contract
and corpus manifest. A missing or failed baseline is reported as
``baseline_unavailable``; it never becomes a fabricated zero or a passing
qualification. ``--require-baseline`` makes that state non-zero for CI.
"""

from __future__ import annotations

import argparse
import bisect
import gzip
import hashlib
import json
import math
import os
import platform
import random
import signal
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable


MACHINE_PREFIX = "LIX_BATCH_BENCHMARK_JSON="
TRANSITION_PREFIX = "LIX_TRANSITION_PROFILE_JSON="
REPORT_SCHEMA = "lix.plugin-api-benchmark-report.v1"
CORPUS_SCHEMA = "lix.plugin-api-benchmark-corpus.v1"
QUALIFICATION_SCHEMA = "lix.plugin-api-benchmark-qualification.v1"
PINNED_BASELINE_REVISION = "89aea5d55773586ea60f77c1d9dddcfc8b394dd1"
DEFAULT_BASELINE = PINNED_BASELINE_REVISION
DEFAULT_SAMPLES = 61
DEFAULT_WARMUPS = 5
PROFILE_SAMPLES = 101
PROFILE_SAMPLE_OVERRIDES = {
    # Sparse updates finish in well under a millisecond, so the default scope
    # can miss their short guest parse/render interval entirely at 1 kHz.
    # Repetition is appropriate here because these are explicitly warmed
    # incremental workloads; cold lanes are profiled once below.
    "csv-sparse-file-update": 1001,
    "json-sparse-file-update": 1001,
    "markdown-sparse-file-update": 1001,
    "text-sparse-file-update": 1001,
    "excalidraw-sparse-file-update": 1001,
    "markdown-same-row-text-merge": 255,
    "csv-same-row-column-merge": 255,
}
PROFILE_MINIMUM_RETAINED_SAMPLES = 25
PROFILE_MINIMUM_GUEST_SAMPLES = 5
COLD_PROFILE_LANES = {
    "csv-direct-row-mutation",
    "json-direct-row-mutation",
    "markdown-direct-row-mutation",
    "text-direct-row-mutation",
    "excalidraw-direct-row-mutation",
    "json-ten-mib-paged-roundtrip",
}
HIGH_RATE_PROFILE_LANES = COLD_PROFILE_LANES | {
    "markdown-same-row-text-merge",
    "csv-same-row-column-merge",
}
PROFILE_ARM_DELAY_SECONDS = 0.025
PROFILE_ATTACH_ARM_DELAY_SECONDS = 0.25
SAMPLY_VERSION = "samply 0.13.1"
BENCHMARK_FEATURES = "sdk-tests,storage-benches"
BENCHMARK_TARGET = "plugin_api_benchmarks"
BENCHMARK_TEST = "plugin_api_public_workflows"
OUTER_JSON_COUNTERS = (
    "outer_row_json_parse_calls",
    "outer_row_json_parse_bytes",
    "outer_row_json_serialize_calls",
    "outer_row_json_serialize_bytes",
    "outer_row_json_canonicalize_calls",
    "outer_row_json_canonicalize_bytes",
    "outer_row_json_dom_fallback_calls",
    "outer_row_json_dom_fallback_bytes",
)
BASELINE_MISSING_COUNTERS = (
    "typed_row_decode_records",
    "typed_row_decode_bytes",
    "typed_row_decode_nanos",
    "typed_row_encode_records",
    "typed_row_encode_bytes",
    "typed_row_schema_validation_calls",
    "typed_row_schema_validation_bytes",
    "typed_row_schema_validation_nanos",
    "typed_transaction_validation_calls",
    "typed_transaction_validation_bytes",
    "outer_row_json_parse_calls",
    "outer_row_json_parse_bytes",
    "outer_row_json_serialize_calls",
    "outer_row_json_serialize_bytes",
    "outer_row_json_canonicalize_calls",
    "outer_row_json_canonicalize_bytes",
    "outer_row_json_dom_fallback_calls",
    "outer_row_json_dom_fallback_bytes",
    "row_input_page_eof_callbacks",
)
ROW_PAGE_CALLBACK_METRIC = (
    '                "row_page_callback_calls": counters.row_page_callback_calls,'
)
BASELINE_ROW_PAGE_CALLBACK_METRIC = (
    '                "row_page_callback_calls": counters.row_input_pages'
    '.saturating_add(counters.row_output_pages),'
)
BASELINE_WASMTIME_IMPORT = (
    "    Cache, CacheConfig, Config, Engine, ResourceLimiter, Store, StoreLimits, "
    "StoreLimitsBuilder,\n"
)
PROFILED_WASMTIME_IMPORT = (
    "    Cache, CacheConfig, Config, Engine, ProfilingStrategy, ResourceLimiter, "
    "Store, StoreLimits,\n    StoreLimitsBuilder,\n"
)
BASELINE_ENGINE_EPOCH_CONFIG = "    config.epoch_interruption(epoch_interruption);\n"
PROFILED_ENGINE_EPOCH_CONFIG = BASELINE_ENGINE_EPOCH_CONFIG + """    if env::var_os("LIX_WASMTIME_PROFILER")
        .as_deref()
        .and_then(|value| value.to_str())
        == Some("perf-map")
    {
        config.profiler(ProfilingStrategy::PerfMap);
    }
"""
TYPED_ATTACHMENT_ASSERTION = """        assert!(
            counters.row_output_attachment_writes > 0
                && counters.row_output_attachment_bytes > 64 * 1024,
            "{lane} must exercise typed output attachments: {counters:?}"
        );
"""
BASELINE_ATTACHMENT_ASSERTION_ADAPTER = (
    "        // The pinned JSON baseline has no typed attachment invariant; "
    "the byte-identical timed lane still runs.\n"
)
JSONB_SCALAR_UPDATE_PARAM = "&[Value::Jsonb(scalar.clone().into())]"
JSONB_SCALAR_QUERY_PARAM = "&[Value::Jsonb(scalar.into())]"
BASELINE_TEXT_SCALAR_UPDATE_PARAM = (
    "&[Value::Text(serde_json::to_string(&scalar).unwrap())]"
)
BASELINE_TEXT_SCALAR_QUERY_PARAM = BASELINE_TEXT_SCALAR_UPDATE_PARAM
EXCALIDRAW_JSONB_PARAMS = (
    "Value::Jsonb(created.clone().into())",
    "Value::Jsonb(updated.clone().into())",
    "Value::Jsonb(created.into())",
    "Value::Jsonb(updated.into())",
)
BASELINE_EXCALIDRAW_TEXT_PARAMS = (
    "Value::Text(serde_json::to_string(&created).unwrap())",
    "Value::Text(serde_json::to_string(&updated).unwrap())",
    "Value::Text(serde_json::to_string(&created).unwrap())",
    "Value::Text(serde_json::to_string(&updated).unwrap())",
)
COMPARE_METRICS = (
    ("elapsed_ms", "p50", 1.10),
    ("elapsed_ms", "p95", 1.15),
    ("allocated_bytes", "p50", 1.10),
    ("allocated_bytes", "p95", 1.15),
    ("allocation_count", "p50", 1.20),
    ("allocation_count", "p95", 1.25),
    ("peak_live_bytes_delta", "p50", 1.10),
    ("peak_live_bytes_delta", "p95", 1.15),
    ("live_bytes_delta", "p50", 1.25),
    ("live_bytes_delta", "p95", 1.35),
    ("large_allocation_count", "p50", 1.20),
    ("large_allocation_count", "p95", 1.25),
    ("process_rss_end_bytes", "p50", 1.15),
    ("process_rss_end_bytes", "p95", 1.20),
    ("process_rss_delta_bytes", "p50", 1.35),
    ("process_rss_delta_bytes", "p95", 1.50),
    ("physical_puts", "p50", 1.10),
    ("physical_puts", "p95", 1.15),
    ("physical_deletes", "p50", 1.10),
    ("physical_deletes", "p95", 1.15),
    ("physical_written_bytes", "p50", 1.10),
    ("physical_written_bytes", "p95", 1.15),
)
SIGNED_MAGNITUDE_METRICS = {"live_bytes_delta", "process_rss_delta_bytes"}
COMPARE_COUNTERS = (
    ("component_boundary_bytes", "p50", 1.10),
    ("component_boundary_bytes", "p95", 1.15),
    ("guest_linear_memory_high_water_bytes", "p50", 1.10),
    ("guest_linear_memory_high_water_bytes", "p95", 1.15),
    ("row_input_wire_bytes", "p50", 1.10),
    ("row_input_wire_bytes", "p95", 1.15),
    ("row_output_wire_bytes", "p50", 1.10),
    ("row_output_wire_bytes", "p95", 1.15),
    ("row_input_pages", "p50", 1.10),
    ("row_input_pages", "p95", 1.15),
    ("row_output_pages", "p50", 1.10),
    ("row_output_pages", "p95", 1.15),
    ("row_page_callback_calls", "p50", 1.10),
    ("row_page_callback_calls", "p95", 1.15),
    ("row_input_attachment_reads", "p50", 1.10),
    ("row_input_attachment_reads", "p95", 1.15),
    ("row_input_attachment_bytes", "p50", 1.10),
    ("row_input_attachment_bytes", "p95", 1.15),
    ("row_output_attachment_writes", "p50", 1.10),
    ("row_output_attachment_writes", "p95", 1.15),
    ("row_output_attachment_bytes", "p50", 1.10),
    ("row_output_attachment_bytes", "p95", 1.15),
    ("component_import_calls", "p50", 1.10),
    ("component_import_calls", "p95", 1.15),
    ("guest_export_calls", "p50", 1.10),
    ("guest_export_calls", "p95", 1.15),
    ("conflict_resolution_calls", "p50", 1.10),
    ("conflict_resolution_calls", "p95", 1.15),
    ("conflict_resolution_records", "p50", 1.10),
    ("conflict_resolution_records", "p95", 1.15),
    ("conflict_resolution_takes", "p50", 1.10),
    ("conflict_resolution_takes", "p95", 1.15),
)
GATED_METRICS = {
    "elapsed_ms",
    "allocated_bytes",
    "allocation_count",
    "peak_live_bytes_delta",
    "process_rss_end_bytes",
    "physical_puts",
    "physical_deletes",
    "physical_written_bytes",
}
ABSOLUTE_REGRESSION_TOLERANCES = {
    "allocated_bytes": 1024 * 1024,
    "peak_live_bytes_delta": 2 * 1024 * 1024,
    # Linux RSS moves in allocator/page-sized steps and includes retained
    # process arenas, so ratios over a small denominator are not material.
    "process_rss_end_bytes": 8 * 1024 * 1024,
    "guest_linear_memory_high_water_bytes": 2 * 1024 * 1024,
}
ABSOLUTE_TOLERANCE_PROPORTIONAL_CEILINGS = {
    "allocated_bytes": 2.00,
    "peak_live_bytes_delta": 2.00,
    "process_rss_end_bytes": 1.25,
    "guest_linear_memory_high_water_bytes": 1.25,
}
# These two explicitly large-payload lanes can trade cumulative host allocation
# for substantially less elapsed time and durable I/O. Every exception still
# requires paired CI bounds, complete typed counters, and paired CPU artifacts.
PARETO_EXCEPTION_LANES = {
    "json-ten-mib-paged-roundtrip",
    "text-large-typed-attachment-roundtrip",
}
TYPED_TRANSITION_COUNTERS = (
    "typed_row_decode_records",
    "typed_row_encode_records",
    "typed_row_schema_validation_calls",
    "typed_transaction_validation_calls",
)
REQUIRED_SAMPLE_METRICS = (
    "elapsed_ms",
    "allocation_count",
    "allocated_bytes",
    "live_bytes_delta",
    "peak_live_bytes_delta",
    "large_allocation_count",
    "process_rss_start_bytes",
    "process_rss_end_bytes",
    "process_rss_delta_bytes",
    "physical_puts",
    "physical_deletes",
    "physical_written_bytes",
)
REQUIRED_PROFILE_COUNTERS = tuple(sorted({name for name, _, _ in COMPARE_COUNTERS}))
REQUIRED_CANDIDATE_COUNTERS = tuple(
    sorted(
        set(REQUIRED_PROFILE_COUNTERS)
        | set(OUTER_JSON_COUNTERS)
        | {
            "typed_row_decode_records",
            "typed_row_decode_bytes",
            "typed_row_encode_records",
            "typed_row_encode_bytes",
            "typed_row_schema_validation_calls",
            "typed_row_schema_validation_bytes",
            "typed_transaction_validation_calls",
            "typed_transaction_validation_bytes",
            "row_page_callback_calls",
            "row_input_page_eof_callbacks",
            "row_input_attachment_reads",
            "row_input_attachment_bytes",
            "row_output_attachment_writes",
            "row_output_attachment_bytes",
            "row_input_records",
            "row_output_records",
        }
    )
)
BUNDLED_PLUGINS = ("csv", "json", "markdown", "text", "excalidraw")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def run_command(
    command: list[str],
    cwd: Path,
    *,
    check: bool = True,
    capture: bool = True,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )


def git_output(root: Path, *arguments: str, check: bool = True) -> str:
    result = run_command(["git", *arguments], root, check=check)
    return (result.stdout or "").strip()


def resolve_revision(root: Path, ref: str) -> str | None:
    result = run_command(
        ["git", "rev-parse", "--verify", f"{ref}^{{commit}}"],
        root,
        check=False,
    )
    if result.returncode:
        return None
    return (result.stdout or "").strip()


def read_corpus_manifest(root: Path) -> dict[str, Any]:
    path = root / "packages/e2e/benchmarks/plugin_api_corpus.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("schema") != CORPUS_SCHEMA:
        raise ValueError(f"unexpected corpus schema in {path}")
    lanes = manifest.get("lanes")
    lane_contracts = manifest.get("lane_contracts")
    workload_contract = manifest.get("workload_contract")
    if not isinstance(lanes, list) or not isinstance(lane_contracts, dict):
        raise ValueError("corpus must declare lanes and lane_contracts")
    if set(lane_contracts) != set(lanes):
        raise ValueError("corpus lane_contracts must cover exactly the pinned lanes")
    if not isinstance(workload_contract, dict):
        raise ValueError("corpus must declare workload_contract")
    for lane, contract_name in lane_contracts.items():
        phases = workload_contract.get(contract_name)
        if (
            not isinstance(phases, list)
            or not phases
            or any(not isinstance(phase, str) or not phase for phase in phases)
            or len(set(phases)) != len(phases)
        ):
            raise ValueError(f"lane {lane} references an invalid phase contract")
    return {
        "path": str(path.relative_to(root)),
        "sha256": sha256_file(path),
        "manifest": manifest,
    }


def adapter_contract() -> dict[str, Any]:
    return {
        "schema": "lix.plugin-api-baseline-adapter-contract.v1",
        "baseline_revision": PINNED_BASELINE_REVISION,
        "omitted_counter_fields": list(BASELINE_MISSING_COUNTERS),
        "row_page_callback_mapping": {
            "candidate": "instrumented row page callbacks",
            "baseline": "row_input_pages + row_output_pages",
            "reason": "the pinned runtime invokes exactly one callback for every input or output page",
        },
        "typed_attachment_assertion_sha256": sha256_bytes(
            TYPED_ATTACHMENT_ASSERTION.encode()
        ),
        "typed_attachment_replacement_sha256": sha256_bytes(
            BASELINE_ATTACHMENT_ASSERTION_ADAPTER.encode()
        ),
        "json_scalar_parameter_adaptation": {
            "candidate": "native jsonb value",
            "baseline": "equivalent JSON encoded for the historical text column",
            "candidate_update_sha256": sha256_bytes(JSONB_SCALAR_UPDATE_PARAM.encode()),
            "baseline_update_sha256": sha256_bytes(
                BASELINE_TEXT_SCALAR_UPDATE_PARAM.encode()
            ),
            "candidate_query_sha256": sha256_bytes(JSONB_SCALAR_QUERY_PARAM.encode()),
            "baseline_query_sha256": sha256_bytes(
                BASELINE_TEXT_SCALAR_QUERY_PARAM.encode()
            ),
        },
        "excalidraw_parameter_adaptation": {
            "candidate": "native jsonb values",
            "baseline": "equivalent JSON encoded for the historical text column",
            "candidate_sha256": [sha256_bytes(value.encode()) for value in EXCALIDRAW_JSONB_PARAMS],
            "baseline_sha256": [
                sha256_bytes(value.encode()) for value in BASELINE_EXCALIDRAW_TEXT_PARAMS
            ],
        },
        "public_accessor_edits": [
            "plugin_transition_counters",
            "reset_plugin_transition_counters",
        ],
        "baseline_profile_symbolization": {
            "environment": "LIX_WASMTIME_PROFILER=perf-map",
            "scope": "CPU profile collection only; timed measurement runs leave it unset",
        },
    }


def hard_cut_source_audit(root: Path) -> dict[str, Any]:
    forbidden = (
        "WasmCanonicalJson",
        "CanonicalJsonBatch",
        "WasmCertifiedRowBatch",
        "PACKET_FORMAT_V1",
        "WasmHostBytes::Source",
        "SchemaRows",
        "create_typed",
        "upsert_typed",
        "delete_typed",
    )
    paths = [
        root / "packages/lix/src/plugin",
        root / "packages/lix/src/transaction",
        root / "packages/lix/wit/lix-plugin.wit",
        root / "plugins",
    ]
    findings: list[dict[str, Any]] = []
    scanned_files = 0
    for path in paths:
        files = [path] if path.is_file() else sorted(path.rglob("*"))
        for file in files:
            if not file.is_file() or file.suffix not in {".rs", ".wit"}:
                continue
            scanned_files += 1
            for line_number, line in enumerate(
                file.read_text(encoding="utf-8", errors="replace").splitlines(), 1
            ):
                for token in forbidden:
                    if token in line:
                        findings.append(
                            {
                                "path": str(file.relative_to(root)),
                                "line": line_number,
                                "token": token,
                            }
                        )
    wit = (root / "packages/lix/wit/lix-plugin.wit").read_text(encoding="utf-8")
    if "row-pk" in wit:
        findings.append(
            {"path": "packages/lix/wit/lix-plugin.wit", "line": None, "token": "row-pk"}
        )
    contract = (
        root / "packages/lix/src/plugin/runtime/contract.rs"
    ).read_text(encoding="utf-8")
    contract_production = contract.split("#[cfg(test)]", 1)[0]
    hook_call_sites = []
    for file in sorted((root / "packages/lix/src").rglob("*.rs")):
        if file.name == "contract.rs" and file.parent.name == "runtime":
            continue
        for line_number, line in enumerate(
            file.read_text(encoding="utf-8", errors="replace").splitlines(), 1
        ):
            if ".record_outer_row_json_operation(" in line:
                hook_call_sites.append(
                    {"path": str(file.relative_to(root)), "line": line_number}
                )
    runner_tests = (
        root / "packages/e2e/benchmarks/test_plugin_api_benchmark.py"
    ).read_text(encoding="utf-8")
    transaction_context = (
        root / "packages/lix/src/transaction/context.rs"
    ).read_text(encoding="utf-8")
    typed_wire = (
        root / "packages/lix/src/plugin/wire/typed.rs"
    ).read_text(encoding="utf-8")
    typed_wire_production = typed_wire.split("#[cfg(test)]", 1)[0]
    host_payload_enum = contract_production.split("pub enum WasmHostBytes {", 1)[1].split(
        "}", 1
    )[0]
    guest_payload_enum = contract_production.split(
        "pub enum WasmGuestRowPayload {", 1
    )[1].split("}", 1)[0]
    expected_hook_sites = [
        site
        for site in hook_call_sites
        if site["path"] == "packages/lix/src/transaction/context.rs"
    ]
    positive_controls = {
        "central_runtime_hook": "pub fn record_outer_row_json_operation(" in contract,
        "single_forbidden_ingress_hook": (
            contract_production.count("record_outer_row_json_operation(") == 1
            and len(hook_call_sites) == 1
            and len(expected_hook_sites) == 1
            and "fn record_forbidden_plugin_json_ingress(" in transaction_context
            and "OuterRowJsonOperation::DomFallback" in transaction_context
        ),
        "ingress_guard_precedes_certified_shortcut": (
            0 <= transaction_context.find("reject_plugin_owned_json_row(&rows, index")
            < transaction_context.find("let certified_preparation = rows.certified_preparation()")
        ),
        "native_wire_has_no_json_snapshot_codec": not any(
            token in typed_wire_production
            for token in ("serde_json", "TransactionJson", "snapshot_content")
        ),
        "host_row_payload_is_typed_only": (
            "Typed(Arc<WasmTypedRow>)" in host_payload_enum
            and not any(token in host_payload_enum for token in ("Json", "Source", "Bytes"))
        ),
        "guest_row_payload_is_typed_only": (
            "Typed(Arc<WasmTypedRow>)" in guest_payload_enum
            and not any(token in guest_payload_enum for token in ("Json", "Source", "Bytes"))
        ),
        "public_row_page_is_opaque_typed_transport": (
            "record row-page {" in wit
            and "payload: bytes" in wit
            and "attachments: list<bytes>" in wit
            and not any(
                token in wit
                for token in ("snapshot-content", "schema-rows", "json-row", "row-pk")
            )
        ),
        "runtime_all_operations_test": (
            "outer_row_json_counter_positive_control_covers_every_forbidden_operation"
            in contract
            and all(
                f"OuterRowJsonOperation::{variant}" in contract
                for variant in ("Parse", "Serialize", "Canonicalize", "DomFallback")
            )
        ),
        "runner_rejects_nonzero_counter_test": (
            "test_report_fails_nonzero_outer_json_counter" in runner_tests
        ),
    }
    if not all(positive_controls.values()):
        findings.append(
            {
                "path": "typed-row zero-JSON instrumentation",
                "line": None,
                "token": "missing positive control",
            }
        )
    return {
        "status": "pass" if not findings else "fail",
        "scanned_files": scanned_files,
        "forbidden_tokens": list(forbidden) + ["row-pk in public WIT"],
        "findings": findings,
        "instrumentation_positive_controls": positive_controls,
        "production_outer_json_hook_call_sites": hook_call_sites,
        "plugin_json_ingress_choke_point": "packages/lix/src/transaction/context.rs",
    }


def canonical_json_sha256(value: Any) -> str:
    return sha256_bytes(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def read_qualification_spec(root: Path) -> dict[str, Any]:
    path = root / "packages/e2e/benchmarks/plugin_api_qualification.json"
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema") != QUALIFICATION_SCHEMA:
        raise ValueError(f"unexpected qualification schema in {path}")
    if spec.get("baseline_revision") != PINNED_BASELINE_REVISION:
        raise ValueError("qualification spec does not pin the required baseline")
    if spec.get("default_samples") != DEFAULT_SAMPLES or spec.get(
        "default_warmups"
    ) != DEFAULT_WARMUPS:
        raise ValueError("qualification spec sample/warmup policy differs from the runner")
    for relative, expected in spec.get("timed_harness_sha256", {}).items():
        actual = sha256_file(root / relative)
        if actual != expected:
            raise ValueError(
                f"qualification digest mismatch for {relative}: expected {expected}, found {actual}"
            )
    orchestrator_hashes = spec.get("orchestrator_sha256")
    if not isinstance(orchestrator_hashes, dict) or not orchestrator_hashes:
        raise ValueError("qualification spec must pin benchmark orchestration sources")
    for relative, expected in orchestrator_hashes.items():
        actual = sha256_file(root / relative)
        if actual != expected:
            raise ValueError(
                "qualification orchestrator digest mismatch for "
                f"{relative}: expected {expected}, found {actual}"
            )
    actual_adapter = canonical_json_sha256(adapter_contract())
    if spec.get("adapter_contract_sha256") != actual_adapter:
        raise ValueError(
            "baseline adapter contract digest mismatch: "
            f"expected {spec.get('adapter_contract_sha256')}, found {actual_adapter}"
        )
    actual_contract = workload_metadata(root)["contract_sha256"]
    if spec.get("normalized_timed_contract_sha256") != actual_contract:
        raise ValueError(
            "normalized timed contract digest mismatch: "
            f"expected {spec.get('normalized_timed_contract_sha256')}, found {actual_contract}"
        )
    return {"path": str(path.relative_to(root)), "sha256": sha256_file(path), "spec": spec}


def command_version(root: Path, command: list[str]) -> str | None:
    result = run_command(command, root, check=False)
    if result.returncode:
        return None
    return (result.stdout or "").strip()


def environment_metadata(root: Path, samples: int, warmups: int) -> dict[str, Any]:
    uname = platform.uname()
    cpu_model = None
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("model name") and ":" in line:
                cpu_model = line.split(":", 1)[1].strip()
                break
    def text_file(path: str) -> str | None:
        file = Path(path)
        return file.read_text(encoding="utf-8", errors="replace").strip() if file.is_file() else None

    microcode = None
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("microcode") and ":" in line:
                microcode = line.split(":", 1)[1].strip()
                break
    selected_cpu = pinned_cpu()
    topology_root = Path(f"/sys/devices/system/cpu/cpu{selected_cpu}/topology")
    load_average = list(os.getloadavg()) if hasattr(os, "getloadavg") else None
    rustc = command_version(root, ["rustc", "--version", "--verbose"])
    rustc_fields = {}
    for line in (rustc or "").splitlines():
        if ": " in line:
            key, value = line.split(": ", 1)
            rustc_fields[key] = value
    return {
        "os": platform.platform(),
        "system": uname.system,
        "release": uname.release,
        "machine": uname.machine,
        "cpu_model": cpu_model,
        "cpu_microcode": microcode,
        "selected_cpu": selected_cpu,
        "selected_cpu_thread_siblings": text_file(
            str(topology_root / "thread_siblings_list")
        ),
        "selected_cpu_core_siblings": text_file(str(topology_root / "core_siblings_list")),
        "smt_active": text_file("/sys/devices/system/cpu/smt/active"),
        "isolated_cpus": text_file("/sys/devices/system/cpu/isolated"),
        "cpu_governor": text_file(
            "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
        ),
        "cpu_scaling_min_khz": text_file(
            "/sys/devices/system/cpu/cpu0/cpufreq/scaling_min_freq"
        ),
        "cpu_scaling_max_khz": text_file(
            "/sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq"
        ),
        "meminfo": text_file("/proc/meminfo"),
        "load_average": load_average,
        "load_average_1m": load_average[0] if load_average else None,
        "ci_runner_image": os.environ.get("ImageOS"),
        "ci_runner_image_version": os.environ.get("ImageVersion"),
        "python": sys.version,
        "rustc": rustc,
        "rustc_release": rustc_fields.get("release"),
        "rustc_commit_hash": rustc_fields.get("commit-hash"),
        "cargo": command_version(root, ["cargo", "--version"]),
        "rustup_active_toolchain": command_version(
            root, ["rustup", "show", "active-toolchain"]
        ),
        "rust_target": command_version(root, ["rustc", "-vV"]),
        "profile": "release",
        "features": BENCHMARK_FEATURES,
        "storage_backend": "open_lix_default",
        "samples": samples,
        "warmups": warmups,
        "cargo_incremental": os.environ.get("CARGO_INCREMENTAL", "0"),
    }


def enforce_environment_requirements(
    environment: dict[str, Any], requirements: dict[str, Any]
) -> None:
    """Reject qualification runs outside the explicitly pinned host contract."""
    if not isinstance(requirements, dict) or not requirements:
        raise ValueError("qualification spec must pin environment_requirements")
    mismatches = []
    for field, expected in requirements.items():
        actual = environment.get(field)
        if isinstance(expected, dict) and set(expected) <= {"min", "max"}:
            if not isinstance(actual, (int, float)) or isinstance(actual, bool):
                mismatches.append(f"{field}: expected numeric bounds {expected!r}, found {actual!r}")
                continue
            if "min" in expected and actual < expected["min"]:
                mismatches.append(f"{field}: expected >= {expected['min']!r}, found {actual!r}")
            if "max" in expected and actual > expected["max"]:
                mismatches.append(f"{field}: expected <= {expected['max']!r}, found {actual!r}")
        elif actual != expected:
            mismatches.append(f"{field}: expected {expected!r}, found {actual!r}")
    if mismatches:
        raise ValueError("qualification environment mismatch: " + "; ".join(mismatches))


def normalized_workload_bytes(path: Path) -> bytes:
    value = path.read_text(encoding="utf-8")
    if path.name == "plugin_api_benchmarks.rs":
        value = value.replace(
            TYPED_ATTACHMENT_ASSERTION, BASELINE_ATTACHMENT_ASSERTION_ADAPTER
        )
        value = value.replace(
            JSONB_SCALAR_UPDATE_PARAM, BASELINE_TEXT_SCALAR_UPDATE_PARAM
        ).replace(JSONB_SCALAR_QUERY_PARAM, BASELINE_TEXT_SCALAR_QUERY_PARAM)
        for candidate, baseline in zip(
            EXCALIDRAW_JSONB_PARAMS, BASELINE_EXCALIDRAW_TEXT_PARAMS, strict=True
        ):
            value = value.replace(candidate, baseline)
    if path.name == "benchmark_metrics.rs":
        value = value.replace(
            ROW_PAGE_CALLBACK_METRIC, BASELINE_ROW_PAGE_CALLBACK_METRIC
        )
        value = "\n".join(
            line
            for line in value.splitlines()
            if not any(f'"{name}"' in line for name in BASELINE_MISSING_COUNTERS)
        ) + "\n"
    return value.encode("utf-8")


def workload_metadata(root: Path) -> dict[str, Any]:
    paths = [
        root / "packages/e2e/tests/plugin_api_benchmarks.rs",
        root / "packages/e2e/tests/benchmark_metrics.rs",
        root / "packages/e2e/benchmarks/plugin_api_corpus.json",
    ]
    files: dict[str, str | None] = {}
    for path in paths:
        relative = str(path.relative_to(root))
        files[relative] = (
            sha256_bytes(normalized_workload_bytes(path)) if path.is_file() else None
        )
    digest = hashlib.sha256()
    for relative, file_digest in sorted(files.items()):
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update((file_digest or "missing").encode("ascii"))
        digest.update(b"\0")
    return {"files": files, "contract_sha256": digest.hexdigest()}


def prepare_pinned_baseline(candidate_root: Path, baseline_root: Path) -> dict[str, Any]:
    """Install the identical timed harness into the frozen pre-refactor tree.

    The baseline predates the public transition-counter additions. The adapter
    removes only references to those unavailable fields from records emitted
    after each allocation/timing scope; all workload and measurement code is
    copied verbatim from the candidate.
    """
    benchmark_relative = Path("packages/e2e/tests/plugin_api_benchmarks.rs")
    metrics_relative = Path("packages/e2e/tests/benchmark_metrics.rs")
    corpus_relative = Path("packages/e2e/benchmarks/plugin_api_corpus.json")
    copied_hashes: dict[str, str] = {}
    for relative in (benchmark_relative, metrics_relative, corpus_relative):
        destination = baseline_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(candidate_root / relative, destination)
        copied_hashes[str(relative)] = sha256_file(destination)

    metrics_path = baseline_root / metrics_relative
    metrics = metrics_path.read_text(encoding="utf-8")
    if metrics.count(ROW_PAGE_CALLBACK_METRIC) != 1:
        raise ValueError(
            "pinned baseline adapter expected exactly one row page callback metric"
        )
    metrics = metrics.replace(
        ROW_PAGE_CALLBACK_METRIC, BASELINE_ROW_PAGE_CALLBACK_METRIC, 1
    )
    removed_counter_lines = sum(
        any(f'"{name}"' in line for name in BASELINE_MISSING_COUNTERS)
        for line in metrics.splitlines()
    )
    if removed_counter_lines != len(BASELINE_MISSING_COUNTERS):
        raise ValueError(
            "pinned baseline adapter expected exactly one metrics line for every "
            f"candidate-only counter, found {removed_counter_lines} for "
            f"{len(BASELINE_MISSING_COUNTERS)} counters"
        )
    adapted_lines = [
        line
        for line in metrics.splitlines()
        if not any(f'"{name}"' in line for name in BASELINE_MISSING_COUNTERS)
    ]
    metrics_path.write_text("\n".join(adapted_lines) + "\n", encoding="utf-8")

    benchmark_path = baseline_root / benchmark_relative
    benchmark = benchmark_path.read_text(encoding="utf-8")
    if TYPED_ATTACHMENT_ASSERTION not in benchmark:
        raise ValueError("pinned baseline adapter could not find typed attachment assertion")
    if benchmark.count(TYPED_ATTACHMENT_ASSERTION) != 1:
        raise ValueError("pinned baseline adapter expected exactly one attachment assertion")
    for candidate, baseline in (
        (TYPED_ATTACHMENT_ASSERTION, BASELINE_ATTACHMENT_ASSERTION_ADAPTER),
        (JSONB_SCALAR_UPDATE_PARAM, BASELINE_TEXT_SCALAR_UPDATE_PARAM),
        (JSONB_SCALAR_QUERY_PARAM, BASELINE_TEXT_SCALAR_QUERY_PARAM),
        *zip(EXCALIDRAW_JSONB_PARAMS, BASELINE_EXCALIDRAW_TEXT_PARAMS, strict=True),
    ):
        if benchmark.count(candidate) != 1:
            raise ValueError(
                "pinned baseline adapter expected exactly one occurrence of "
                f"{candidate!r}"
            )
        benchmark = benchmark.replace(candidate, baseline, 1)
    benchmark_path.write_text(benchmark, encoding="utf-8")

    handle_path = baseline_root / "packages/lix/src/handle.rs"
    handle = handle_path.read_text(encoding="utf-8")
    exposed_accessors = []
    for method in ("plugin_transition_counters", "reset_plugin_transition_counters"):
        needle = f"    pub(crate) fn {method}"
        replacement = f"    pub fn {method}"
        if handle.count(needle) != 1:
            raise ValueError(
                f"pinned baseline adapter expected exactly one {needle!r}"
            )
        handle = handle.replace(needle, replacement, 1)
        exposed_accessors.append(method)
    handle_path.write_text(handle, encoding="utf-8")

    runtime_path = baseline_root / "packages/lix/src/plugin/runtime/default/mod.rs"
    runtime = runtime_path.read_text(encoding="utf-8")
    for needle, replacement in (
        (BASELINE_WASMTIME_IMPORT, PROFILED_WASMTIME_IMPORT),
        (BASELINE_ENGINE_EPOCH_CONFIG, PROFILED_ENGINE_EPOCH_CONFIG),
    ):
        if runtime.count(needle) != 1:
            raise ValueError(
                "pinned baseline adapter expected exactly one profiler insertion point: "
                f"{needle!r}"
            )
        runtime = runtime.replace(needle, replacement, 1)
    runtime_path.write_text(runtime, encoding="utf-8")

    adapter_patch = run_command(
        [
            "git",
            "diff",
            "--binary",
            "--",
            str(benchmark_relative),
            str(metrics_relative),
            str(corpus_relative),
            "packages/lix/src/handle.rs",
            "packages/lix/src/plugin/runtime/default/mod.rs",
        ],
        baseline_root,
    ).stdout or ""
    adapter_patch_sha256 = sha256_bytes(adapter_patch.encode())
    expected_patch = read_qualification_spec(candidate_root)["spec"].get(
        "baseline_adapter_patch_sha256"
    )
    if adapter_patch_sha256 != expected_patch:
        raise ValueError(
            "baseline adapter patch digest mismatch: "
            f"expected {expected_patch}, found {adapter_patch_sha256}"
        )

    return {
        "schema": "lix.plugin-api-baseline-adapter.v1",
        "revision": PINNED_BASELINE_REVISION,
        "timed_harness": "copied_verbatim_from_candidate",
        "outside_timed_scope_changes": [
            "make existing transition counter accessors public to the e2e harness",
            "enable Wasmtime perf-map symbols only when CPU profiling requests them",
            "omit post-scope counter fields absent at the pinned revision",
            "derive baseline row page callback count from one input/output callback per emitted page",
            "omit the candidate-only typed-attachment assertion after the identical timed lane",
        ],
        "timed_semantic_adaptations": [
            "bind the same JSON scalar as encoded text on the pinned baseline and as native JSONB on the candidate",
            "bind the same Excalidraw element JSON as encoded text on the pinned baseline and as native JSONB on the candidate",
        ],
        "omitted_counter_fields": list(BASELINE_MISSING_COUNTERS),
        "copied_source_sha256": copied_hashes,
        "removed_counter_lines": removed_counter_lines,
        "exposed_accessors": exposed_accessors,
        "adapted_source_sha256": {
            str(benchmark_relative): sha256_file(benchmark_path),
            str(metrics_relative): sha256_file(metrics_path),
            "packages/lix/src/handle.rs": sha256_file(handle_path),
            "packages/lix/src/plugin/runtime/default/mod.rs": sha256_file(runtime_path),
        },
        "adapter_contract": adapter_contract(),
        "adapter_contract_sha256": canonical_json_sha256(adapter_contract()),
        "adapter_patch_sha256": adapter_patch_sha256,
        "adapter_patch": adapter_patch,
    }


def working_tree_metadata(root: Path) -> dict[str, Any]:
    status = git_output(root, "status", "--porcelain=v1", "-z", check=False)
    diff = run_command(["git", "diff", "--binary", "HEAD"], root, check=False).stdout or ""
    untracked = git_output(
        root, "ls-files", "--others", "--exclude-standard", "-z", check=False
    )
    untracked_paths = [path for path in untracked.split("\0") if path]
    digest = hashlib.sha256()
    digest.update(diff.encode("utf-8", errors="surrogateescape"))
    for relative in sorted(untracked_paths):
        path = root / relative
        if path.is_file():
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            digest.update(path.read_bytes())
    return {
        "head": git_output(root, "rev-parse", "HEAD"),
        "dirty": bool(status),
        "status_sha256": sha256_bytes(status.encode("utf-8", errors="surrogateescape")),
        "working_tree_sha256": digest.hexdigest(),
        "untracked_paths": untracked_paths,
    }


def benchmark_command() -> list[str]:
    return [
        "cargo",
        "test",
        "--manifest-path",
        "tooling/Cargo.toml",
        "-p",
        "lix_e2e",
        "--features",
        BENCHMARK_FEATURES,
        "--test",
        BENCHMARK_TARGET,
        "--release",
        BENCHMARK_TEST,
        "--",
        "--ignored",
        "--exact",
        "--nocapture",
        "--test-threads=1",
    ]


def benchmark_build_command() -> list[str]:
    return [
        "cargo",
        "test",
        "--manifest-path",
        "tooling/Cargo.toml",
        "-p",
        "lix_e2e",
        "--features",
        BENCHMARK_FEATURES,
        "--test",
        BENCHMARK_TARGET,
        "--release",
        "--no-run",
    ]


def benchmark_executable(root: Path, target_dir: Path) -> Path:
    command = benchmark_build_command() + ["--message-format=json"]
    completed = run_command(
        command,
        root,
        env=benchmark_environment(target_dir),
    )
    executables = []
    for line in (completed.stdout or "").splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (
            message.get("reason") == "compiler-artifact"
            and message.get("target", {}).get("name") == BENCHMARK_TARGET
            and message.get("executable")
        ):
            executables.append(Path(message["executable"]))
    if len(executables) != 1:
        raise ValueError(
            f"expected one {BENCHMARK_TARGET} executable, found {len(executables)}"
        )
    return executables[0]


def collect_cpu_profiles(
    baseline_root: Path,
    candidate_root: Path,
    output: Path,
    *,
    lanes: list[str],
    baseline_target: Path,
    candidate_target: Path,
) -> dict[str, Any]:
    profiler = shutil.which("samply")
    if profiler is None:
        return {"status": "unavailable", "reason": "samply is not installed"}
    profiler_version = command_version(output, [profiler, "--version"])
    if profiler_version != SAMPLY_VERSION:
        return {
            "status": "unavailable",
            "reason": f"expected {SAMPLY_VERSION}, found {profiler_version!r}",
        }
    profile_dir = output / "cpu-profiles"
    profile_dir.mkdir(parents=True, exist_ok=True)
    cpu = pinned_cpu()
    roots = {"baseline": baseline_root, "candidate": candidate_root}
    targets = {"baseline": baseline_target, "candidate": candidate_target}
    executables = {
        label: benchmark_executable(roots[label], targets[label])
        for label in roots
    }
    artifacts: list[dict[str, Any]] = []
    for lane_index, lane in enumerate(lanes):
        profile_samples = profile_sample_count(lane)
        plugin = lane.split("-", 1)[0]
        order = profile_revision_order(lane_index)
        for ordinal, label in enumerate(order):
            attach_capture = profile_requires_attach_capture(lane)
            artifact = profile_dir / f"{label}-{lane}.json.gz"
            profiler_command = [
                profiler,
                "record",
                "--save-only",
                "--unstable-presymbolicate",
                "--reuse-threads",
                "--per-cpu-threads",
                "--rate",
                str(profile_sampling_rate(lane)),
                "--output",
                str(artifact),
                "--profile-name",
                f"lix-{label}-{lane}",
            ]
            benchmark_profile_command = [
                str(executables[label]),
                BENCHMARK_TEST,
                "--ignored",
                "--exact",
                "--nocapture",
                "--test-threads=1",
            ]
            executed_profiler_command = pinned_command(
                (
                    profiler_command
                    if attach_capture
                    else [*profiler_command, "--", *benchmark_profile_command]
                ),
                cpu,
            )
            ready = profile_dir / f".{label}-{plugin}.ready"
            go = profile_dir / f".{label}-{plugin}.go"
            done = profile_dir / f".{label}-{plugin}.done"
            release = profile_dir / f".{label}-{plugin}.release"
            for marker in (ready, go, done, release):
                marker.unlink(missing_ok=True)
            environment = benchmark_environment(
                targets[label], lane=lane, sample_index=0, warmups=DEFAULT_WARMUPS
            )
            environment.update(
                {
                    "LIX_PLUGIN_API_BENCH_SAMPLES": str(profile_samples),
                    "LIX_PLUGIN_API_PROFILE_LANE": lane,
                    "LIX_PLUGIN_API_PROFILE_READY": str(ready),
                    "LIX_PLUGIN_API_PROFILE_GO": str(go),
                    "LIX_PLUGIN_API_PROFILE_DONE": str(done),
                    "LIX_PLUGIN_API_PROFILE_RELEASE": str(release),
                    # Both candidate and narrowly adapted baseline runtimes
                    # emit Wasmtime symbol maps during profile collection.
                    "LIX_WASMTIME_PROFILER": "perf-map",
                }
            )
            log = profile_dir / f"{label}-{lane}.log"
            with log.open("w+", encoding="utf-8") as log_stream:
                benchmark_process = None
                if attach_capture:
                    benchmark_process = subprocess.Popen(
                        pinned_command(benchmark_profile_command, cpu),
                        cwd=roots[label],
                        env=environment,
                        text=True,
                        stdout=log_stream,
                        stderr=subprocess.STDOUT,
                    )
                    observed_process = benchmark_process
                else:
                    observed_process = subprocess.Popen(
                        executed_profiler_command,
                        cwd=roots[label],
                        env=environment,
                        text=True,
                        stdout=log_stream,
                        stderr=subprocess.STDOUT,
                    )
                profiler_process = observed_process if not attach_capture else None
                deadline = time.monotonic() + 60.0
                while not ready.is_file() and observed_process.poll() is None:
                    if time.monotonic() >= deadline:
                        observed_process.terminate()
                        raise ValueError(f"{label} {lane} did not reach the profile barrier")
                    time.sleep(0.01)
                if observed_process.poll() is not None:
                    raise ValueError(f"{label} {lane} exited before the profile barrier")
                if attach_capture:
                    executed_profiler_command = pinned_command(
                        [*profiler_command, "--pid", str(benchmark_process.pid)], cpu
                    )
                    profiler_process = subprocess.Popen(
                        executed_profiler_command,
                        cwd=roots[label],
                        env=environment,
                        text=True,
                        stdout=log_stream,
                        stderr=subprocess.STDOUT,
                    )
                time.sleep(
                    PROFILE_ATTACH_ARM_DELAY_SECONDS
                    if attach_capture
                    else PROFILE_ARM_DELAY_SECONDS
                )
                profile_go_wall_ms = time.time() * 1000.0
                profile_go_monotonic_ms = time.monotonic() * 1000.0
                go.write_text("profile measured scope\n", encoding="utf-8")
                deadline = time.monotonic() + 300.0
                while not done.is_file() and observed_process.poll() is None:
                    if time.monotonic() >= deadline:
                        observed_process.terminate()
                        if profiler_process is not observed_process:
                            profiler_process.terminate()
                        raise ValueError(f"{label} {lane} did not finish measured profiling scope")
                    time.sleep(0.01)
                if observed_process.poll() is not None:
                    raise ValueError(f"{label} {lane} exited before the profile end barrier")
                profile_done_wall_ms = time.time() * 1000.0
                profile_done_monotonic_ms = time.monotonic() * 1000.0
                if attach_capture:
                    profiler_process.send_signal(signal.SIGINT)
                    profiler_returncode = profiler_process.wait(timeout=60)
                    release.write_text("profile artifact closed\n", encoding="utf-8")
                    benchmark_returncode = benchmark_process.wait(timeout=60)
                else:
                    release.write_text("profile artifact closed\n", encoding="utf-8")
                    profiler_returncode = profiler_process.wait(timeout=60)
                    benchmark_returncode = profiler_returncode
                log_stream.flush()
            for marker in (ready, go, done, release):
                marker.unlink(missing_ok=True)
            log_text = log.read_text(encoding="utf-8")
            profile_written = artifact.is_file() and artifact.stat().st_size > 0
            sidecar = artifact.with_suffix(".syms.json")
            sidecar_written = sidecar.is_file() and sidecar.stat().st_size > 0
            measured_interval = trim_cpu_profile_to_monotonic_interval(
                artifact,
                start_monotonic_ms=profile_go_monotonic_ms,
                end_monotonic_ms=profile_done_monotonic_ms,
            )
            measured_interval.update(
                {
                    "start_wall_ms": profile_go_wall_ms,
                    "end_wall_ms": profile_done_wall_ms,
                }
            )
            structure = validate_cpu_profile(
                artifact,
                sidecar,
                expected_profile_name=f"lix-{label}-{lane}",
                expected_guest_symbol=profile_expected_guest_symbol(lane, label),
                minimum_samples=profile_minimum_retained_samples(lane),
                minimum_guest_samples=profile_minimum_guest_samples(lane),
            )
            lane_records = [
                record
                for record in parse_records(log_text)
                if record.get("lane") == lane and record.get("kind") == "sample"
            ]
            artifacts.append(
                {
                    "revision": label,
                    "plugin": plugin,
                    "lane": lane,
                    "samples": profile_samples,
                    "status": (
                        "passed"
                        if benchmark_returncode == 0
                        and profiler_returncode in (0, -signal.SIGINT)
                        and profile_written
                        and sidecar_written
                        and measured_interval.get("status") == "trimmed"
                        and measured_interval.get("retained_samples", 0)
                        >= profile_minimum_retained_samples(lane)
                        and structure["valid"]
                        and len(lane_records) == profile_samples
                        else "failed"
                    ),
                    "benchmark_returncode": benchmark_returncode,
                    "profiler_returncode": profiler_returncode,
                    "profile": str(artifact.relative_to(output)),
                    "profile_sha256": sha256_file(artifact) if profile_written else None,
                    "presymbolicated_sidecar": str(sidecar.relative_to(output)),
                    "presymbolicated_sidecar_sha256": (
                        sha256_file(sidecar) if sidecar_written else None
                    ),
                    "log": str(log.relative_to(output)),
                    "log_sha256": sha256_file(log),
                    "benchmark_command": benchmark_profile_command,
                    "profiler_command": executed_profiler_command,
                    "profile_interval": "explicit_post-warmup_measured_scope_barriers",
                    "capture_scope": (
                        "attached_process_ephemeral_workers_best_effort"
                        if attach_capture
                        else "launched_benchmark_process_tree"
                    ),
                    "measured_interval": measured_interval,
                    "environment": {
                        name: environment[name]
                        for name in (
                            "CARGO_INCREMENTAL",
                            "CARGO_TARGET_DIR",
                            "LIX_PLUGIN_API_BENCH_LANE",
                            "LIX_PLUGIN_API_BENCH_SAMPLE_INDEX",
                            "LIX_PLUGIN_API_BENCH_SAMPLES",
                            "LIX_PLUGIN_API_BENCH_WARMUPS",
                            "LIX_PLUGIN_API_PROFILE_LANE",
                            "LIX_PLUGIN_API_PROFILE_READY",
                            "LIX_PLUGIN_API_PROFILE_GO",
                            "LIX_PLUGIN_API_PROFILE_DONE",
                            "LIX_PLUGIN_API_PROFILE_RELEASE",
                            "LIX_WASMTIME_PROFILER",
                        )
                    },
                    "cpu_affinity": cpu,
                    "paired_order": "AB" if lane_index % 2 == 0 else "BA",
                    "ordinal": ordinal,
                    "profile_structure": structure,
                    "lane_sample_records": len(lane_records),
                }
            )
    complete = all(item["status"] == "passed" for item in artifacts) and len(
        artifacts
    ) == 2 * len(lanes)
    return {
        "status": "complete" if complete else "failed",
        "profiler": profiler_version,
        "artifact_root": str(output),
        "artifacts": artifacts,
    }


def profile_revision_order(lane_index: int) -> tuple[str, str]:
    """Alternate profile order for every lane to balance temporal drift."""
    return (
        ("baseline", "candidate")
        if lane_index % 2 == 0
        else ("candidate", "baseline")
    )


def profile_sample_count(lane: str) -> int:
    """Use a longer scope for lanes that yield few on-CPU perf samples."""
    if lane in COLD_PROFILE_LANES:
        return 1
    return PROFILE_SAMPLE_OVERRIDES.get(lane, PROFILE_SAMPLES)


def profile_sampling_rate(lane: str) -> int:
    """Sample exact cold and short merge scopes densely enough for attribution."""
    return 10_000 if lane in HIGH_RATE_PROFILE_LANES else 1_000


def profile_requires_attach_capture(lane: str) -> bool:
    """Launch profiles around the full process tree so guest workers are visible."""
    del lane
    return False


def profile_minimum_retained_samples(lane: str) -> int:
    """Require a meaningful measured-scope sample set for every lane."""
    if lane in COLD_PROFILE_LANES:
        return 5
    return PROFILE_MINIMUM_RETAINED_SAMPLES


def profile_minimum_guest_samples(lane: str) -> int:
    """Match guest-stack proof to whether guest work spans a sampled phase."""
    if lane.endswith("-direct-row-mutation"):
        return 3
    return PROFILE_MINIMUM_GUEST_SAMPLES


def profile_expected_guest_symbol(lane: str, revision: str) -> str:
    """Name a guest-only frame that survives each revision's symbol scheme."""
    del revision
    return f"plugin_{lane.split('-', 1)[0]}"


def trim_cpu_profile_to_monotonic_interval(
    artifact: Path, *, start_monotonic_ms: float, end_monotonic_ms: float
) -> dict[str, Any]:
    """Keep Samply's CLOCK_MONOTONIC samples from the measured scope only."""
    if not artifact.is_file():
        return {"status": "missing_profile"}
    try:
        with gzip.open(artifact, "rt", encoding="utf-8") as stream:
            profile = json.load(stream)
        end_monotonic_ms = max(start_monotonic_ms, end_monotonic_ms)
        retained = 0
        removed = 0
        for thread in profile.get("threads", []):
            samples = thread.get("samples", {})
            times = samples.get("time")
            if not isinstance(times, list):
                continue
            keep = [
                index
                for index, value in enumerate(times)
                if isinstance(value, (int, float))
                and start_monotonic_ms <= float(value) <= end_monotonic_ms
            ]
            retained += len(keep)
            removed += len(times) - len(keep)
            for key, values in list(samples.items()):
                if isinstance(values, list) and len(values) == len(times):
                    samples[key] = [values[index] for index in keep]
        with gzip.open(artifact, "wt", encoding="utf-8") as stream:
            json.dump(profile, stream, separators=(",", ":"))
        return {
            "status": "trimmed",
            "start_monotonic_ms": start_monotonic_ms,
            "end_monotonic_ms": end_monotonic_ms,
            "retained_samples": retained,
            "removed_samples": removed,
        }
    except (KeyError, TypeError, ValueError, OSError, json.JSONDecodeError) as error:
        return {"status": "invalid", "reason": str(error)}


def validate_cpu_profile(
    artifact: Path,
    sidecar: Path,
    *,
    expected_profile_name: str,
    expected_guest_symbol: str,
    minimum_samples: int = PROFILE_MINIMUM_RETAINED_SAMPLES,
    minimum_guest_samples: int = PROFILE_MINIMUM_GUEST_SAMPLES,
) -> dict[str, Any]:
    if not artifact.is_file() or not sidecar.is_file():
        return {"valid": False, "reason": "profile or symbol sidecar is missing"}
    try:
        with gzip.open(artifact, "rt", encoding="utf-8") as stream:
            profile = json.load(stream)
        sidecar_value = json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        return {"valid": False, "reason": f"invalid JSON profile artifact: {error}"}
    threads = profile.get("threads") if isinstance(profile, dict) else None
    sample_count = 0
    if isinstance(threads, list):
        for thread in threads:
            samples = thread.get("samples", {}) if isinstance(thread, dict) else {}
            stacks = samples.get("stack", []) if isinstance(samples, dict) else []
            if isinstance(stacks, list):
                sample_count += len(stacks)
    sidecar_nonempty = isinstance(sidecar_value, (dict, list)) and bool(sidecar_value)
    profile_text = json.dumps(profile, sort_keys=True, separators=(",", ":"))
    sidecar_text = json.dumps(sidecar_value, sort_keys=True, separators=(",", ":"))
    profile_name_present = expected_profile_name in profile_text
    benchmark_symbol_present = any(
        symbol in sidecar_text
        for symbol in ("plugin_api_public_workflows", "plugin_api_benchmarks")
    )
    sampled_analysis = sampled_profile_analysis(
        profile, sidecar_value, expected_guest_symbol
    )
    return {
        "valid": (
            bool(threads)
            and sample_count >= minimum_samples
            and sidecar_nonempty
            and profile_name_present
            and benchmark_symbol_present
            and (
                minimum_guest_samples == 0
                or sampled_analysis["sampled_guest_frames"] > 0
            )
            and sampled_analysis["samples_with_guest_frames"] >= minimum_guest_samples
        ),
        "threads": len(threads) if isinstance(threads, list) else 0,
        "samples": sample_count,
        "minimum_samples": minimum_samples,
        "minimum_guest_samples": minimum_guest_samples,
        "symbol_sidecar_nonempty": sidecar_nonempty,
        "expected_profile_name_present": profile_name_present,
        "benchmark_symbol_present": benchmark_symbol_present,
        "expected_guest_symbol": expected_guest_symbol,
        **sampled_analysis,
    }


def sampled_profile_analysis(
    profile: dict[str, Any],
    sidecar: dict[str, Any] | list[Any],
    guest_symbol_needle: str,
    limit: int = 12,
) -> dict[str, Any]:
    """Resolve retained sample RVAs through the authenticated symbol sidecar."""
    string_table = sidecar.get("string_table", []) if isinstance(sidecar, dict) else []
    modules = sidecar.get("data", []) if isinstance(sidecar, dict) else []
    symbol_tables: dict[str, tuple[list[int], list[dict[str, Any]]]] = {}
    if isinstance(string_table, list) and isinstance(modules, list):
        for module in modules:
            if not isinstance(module, dict) or not isinstance(module.get("debug_name"), str):
                continue
            symbols = [
                symbol
                for symbol in module.get("symbol_table", [])
                if isinstance(symbol, dict) and isinstance(symbol.get("rva"), int)
            ]
            symbols.sort(key=lambda symbol: symbol["rva"])
            symbol_tables[module["debug_name"]] = (
                [symbol["rva"] for symbol in symbols],
                symbols,
            )

    libraries = profile.get("libs", [])
    sampled_guest_frames = 0
    samples_with_guest_frames = 0
    leaf_counts: dict[str, int] = {}

    def resolve_frame(thread: dict[str, Any], frame: int, raw_name: str) -> str:
        if not raw_name.startswith("0x"):
            return raw_name
        frame_table = thread.get("frameTable", {})
        func_table = thread.get("funcTable", {})
        resource_table = thread.get("resourceTable", {})
        addresses = frame_table.get("address", [])
        funcs = frame_table.get("func", [])
        resources = func_table.get("resource", [])
        resource_libs = resource_table.get("lib", [])
        if not all(
            isinstance(values, list)
            for values in (addresses, funcs, resources, resource_libs, libraries)
        ) or not 0 <= frame < len(addresses) or not 0 <= frame < len(funcs):
            return raw_name
        address = addresses[frame]
        function = funcs[frame]
        if (
            not isinstance(address, int)
            or address < 0
            or not isinstance(function, int)
            or not 0 <= function < len(resources)
        ):
            return raw_name
        resource = resources[function]
        if not isinstance(resource, int) or not 0 <= resource < len(resource_libs):
            return raw_name
        library = resource_libs[resource]
        if not isinstance(library, int) or not 0 <= library < len(libraries):
            return raw_name
        library_value = libraries[library]
        if not isinstance(library_value, dict):
            return raw_name
        debug_name = library_value.get("debugName") or library_value.get("name")
        table = symbol_tables.get(str(debug_name))
        if table is None:
            return raw_name
        rvas, symbols = table
        position = bisect.bisect_right(rvas, address) - 1
        if position < 0:
            return raw_name
        symbol = symbols[position]
        size = symbol.get("size")
        if isinstance(size, int) and size > 0 and address >= symbol["rva"] + size:
            return raw_name
        symbol_index = symbol.get("symbol")
        if not isinstance(symbol_index, int) or not 0 <= symbol_index < len(string_table):
            return raw_name
        resolved = string_table[symbol_index]
        return resolved if isinstance(resolved, str) and resolved else raw_name

    for thread in profile.get("threads", []):
        if not isinstance(thread, dict):
            continue
        samples = thread.get("samples", {})
        stacks = samples.get("stack", []) if isinstance(samples, dict) else []
        stack_table = thread.get("stackTable", {})
        frame_table = thread.get("frameTable", {})
        func_table = thread.get("funcTable", {})
        strings = thread.get("stringArray", [])
        prefixes = stack_table.get("prefix", []) if isinstance(stack_table, dict) else []
        frames = stack_table.get("frame", []) if isinstance(stack_table, dict) else []
        funcs = frame_table.get("func", []) if isinstance(frame_table, dict) else []
        names = func_table.get("name", []) if isinstance(func_table, dict) else []
        if not all(isinstance(values, list) for values in (stacks, prefixes, frames, funcs, names, strings)):
            continue
        for leaf in stacks:
            stack = leaf
            sample_has_guest = False
            leaf_symbol = None
            visited = 0
            while isinstance(stack, int) and 0 <= stack < len(frames):
                frame = frames[stack]
                if isinstance(frame, int) and 0 <= frame < len(funcs):
                    function = funcs[frame]
                    if isinstance(function, int) and 0 <= function < len(names):
                        name = names[function]
                        if isinstance(name, int) and 0 <= name < len(strings):
                            raw_name = strings[name]
                            if isinstance(raw_name, str):
                                symbol = resolve_frame(thread, frame, raw_name)
                                if leaf_symbol is None:
                                    leaf_symbol = symbol
                                if guest_symbol_needle in symbol:
                                    sampled_guest_frames += 1
                                    sample_has_guest = True
                stack = prefixes[stack] if stack < len(prefixes) else None
                visited += 1
                if visited > len(frames):
                    break
            samples_with_guest_frames += int(sample_has_guest)
            if leaf_symbol and leaf_symbol != "<Idle>":
                leaf_counts[leaf_symbol] = leaf_counts.get(leaf_symbol, 0) + 1
    ranked = sorted(leaf_counts.items(), key=lambda item: (-item[1], item[0]))[:limit]
    return {
        "sampled_guest_frames": sampled_guest_frames,
        "samples_with_guest_frames": samples_with_guest_frames,
        "top_sampled_leaf_functions": [
            {"function": function, "samples": samples}
            for function, samples in ranked
        ],
        "sidecar_rva_symbolication": True,
    }


def cpu_profile_evidence_failures(
    profile_evidence: dict[str, Any], expected_lanes: set[str]
) -> list[str]:
    """Re-open and authenticate every profile artifact used by the report gate."""
    failures: list[str] = []
    artifacts = profile_evidence.get("artifacts")
    if profile_evidence.get("status") != "complete" or not isinstance(artifacts, list):
        return ["paired CPU profile collection is not complete"]
    expected = {(revision, lane) for revision in ("baseline", "candidate") for lane in expected_lanes}
    actual: set[tuple[str, str]] = set()
    artifact_root = Path(str(profile_evidence.get("artifact_root", ".")))
    for item in artifacts:
        revision = str(item.get("revision"))
        lane = str(item.get("lane"))
        key = (revision, lane)
        if key in actual:
            failures.append(f"duplicate CPU profile artifact for {revision} {lane}")
        actual.add(key)
        if item.get("status") != "passed":
            failures.append(f"CPU profile artifact did not pass for {revision} {lane}")
        artifact = Path(str(item.get("profile", "")))
        sidecar = Path(str(item.get("presymbolicated_sidecar", "")))
        log = Path(str(item.get("log", "")))
        if not artifact.is_absolute():
            artifact = artifact_root / artifact
        if not sidecar.is_absolute():
            sidecar = artifact_root / sidecar
        if not log.is_absolute():
            log = artifact_root / log
        for path, digest_field in (
            (artifact, "profile_sha256"),
            (sidecar, "presymbolicated_sidecar_sha256"),
        ):
            expected_digest = item.get(digest_field)
            if not path.is_file():
                failures.append(f"missing CPU profile evidence {path}")
            elif not expected_digest or sha256_file(path) != expected_digest:
                failures.append(f"CPU profile evidence digest mismatch for {path}")
        structure = validate_cpu_profile(
            artifact,
            sidecar,
            expected_profile_name=f"lix-{revision}-{lane}",
            expected_guest_symbol=profile_expected_guest_symbol(lane, revision),
            minimum_samples=profile_minimum_retained_samples(lane),
            minimum_guest_samples=profile_minimum_guest_samples(lane),
        )
        if not structure.get("valid"):
            failures.append(f"CPU profile structure is invalid for {revision} {lane}")
        elif structure != item.get("profile_structure"):
            failures.append(f"CPU profile validation metadata changed for {revision} {lane}")
        if not log.is_file() or sha256_file(log) != item.get("log_sha256"):
            failures.append(f"CPU profile log digest mismatch for {revision} {lane}")
        else:
            lane_records = [
                record
                for record in parse_records(log.read_text(encoding="utf-8"))
                if record.get("lane") == lane and record.get("kind") == "sample"
            ]
            if len(lane_records) != item.get("samples") or len(lane_records) != item.get(
                "lane_sample_records"
            ):
                failures.append(f"CPU profile sample log is incomplete for {revision} {lane}")
    if actual != expected:
        missing = sorted(expected - actual)
        excess = sorted(actual - expected)
        if missing:
            failures.append(f"CPU profiles missing {len(missing)} revision/lane artifacts")
        if excess:
            failures.append(f"CPU profiles contain {len(excess)} unexpected artifacts")
    return failures


def passed_paired_cpu_profile_lanes(profile_evidence: dict[str, Any]) -> set[str]:
    """Return lanes with explicit passed artifacts for both measured revisions."""
    revisions_by_lane: dict[str, set[str]] = {}
    for item in profile_evidence.get("artifacts", []):
        if item.get("status") != "passed":
            continue
        revision = item.get("revision")
        lane = item.get("lane")
        if revision in {"baseline", "candidate"} and isinstance(lane, str):
            revisions_by_lane.setdefault(lane, set()).add(str(revision))
    return {
        lane
        for lane, revisions in revisions_by_lane.items()
        if revisions == {"baseline", "candidate"}
    }


def benchmark_environment(
    target_dir: Path,
    *,
    lane: str | None = None,
    sample_index: int | None = None,
    warmups: int = DEFAULT_WARMUPS,
) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "CARGO_TERM_COLOR": "never",
            "CARGO_INCREMENTAL": "0",
            "RUST_BACKTRACE": "1",
            "CARGO_TARGET_DIR": str(target_dir),
            "LIX_PLUGIN_API_BENCH_WARMUPS": str(warmups),
        }
    )
    if lane is not None:
        environment.update(
            {
                "LIX_PLUGIN_API_BENCH_SAMPLES": "1",
                "LIX_PLUGIN_API_BENCH_LANE": lane,
            }
        )
    if sample_index is not None:
        environment["LIX_PLUGIN_API_BENCH_SAMPLE_INDEX"] = str(sample_index)
    return environment


def pinned_cpu() -> int:
    affinity = getattr(os, "sched_getaffinity", None)
    if affinity is None:
        raise ValueError("qualification requires sched_getaffinity CPU pinning support")
    allowed = sorted(affinity(0))
    if not allowed:
        raise ValueError("qualification process has no available CPU for affinity pinning")
    if shutil.which("taskset") is None:
        raise ValueError("qualification requires taskset for per-process CPU affinity")
    return allowed[0]


def pinned_command(command: list[str], cpu: int) -> list[str]:
    return ["taskset", "-c", str(cpu), *command]


def run_benchmark(
    root: Path,
    output: Path,
    label: str,
    *,
    samples: int,
    warmups: int = DEFAULT_WARMUPS,
    target_dir: Path,
) -> dict[str, Any]:
    command = benchmark_command()
    environment = benchmark_environment(target_dir, warmups=warmups)
    environment["LIX_PLUGIN_API_BENCH_SAMPLES"] = str(samples)
    started = time.monotonic()
    completed = subprocess.run(
        command,
        cwd=root,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    elapsed = time.monotonic() - started
    log_path = output / f"{label}.log"
    log_path.write_text(completed.stdout or "", encoding="utf-8")
    records = parse_records(completed.stdout or "")
    records_path = output / f"{label}.records.jsonl"
    with records_path.open("w", encoding="utf-8") as stream:
        for record in records:
            stream.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
            stream.write("\n")
    return {
        "label": label,
        "status": "passed" if completed.returncode == 0 else "failed",
        "returncode": completed.returncode,
        "elapsed_seconds": elapsed,
        "command": command,
        "worktree": str(root),
        "log": str(log_path),
        "records": str(records_path),
        "records_sha256": sha256_file(records_path),
        "record_count": len(records),
        "summary_count": sum(record.get("kind") == "summary" for record in records),
        "transition_profile_count": sum(
            record.get("schema") == "lix.universal-plugin-transition-profile.v1"
            for record in records
        ),
        "workload": workload_metadata(root),
    }


def run_paired_benchmarks(
    baseline_root: Path,
    candidate_root: Path,
    output: Path,
    *,
    lanes: list[str],
    samples: int,
    warmups: int,
    baseline_target: Path,
    candidate_target: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run fresh-process paired samples in alternating AB/BA order."""
    cpu = pinned_cpu()
    roots = {"baseline": baseline_root, "candidate": candidate_root}
    targets = {"baseline": baseline_target, "candidate": candidate_target}
    logs = {label: [] for label in roots}
    records = {label: [] for label in roots}
    elapsed = {label: 0.0 for label in roots}
    returncodes = {label: 0 for label in roots}

    aborted = False
    for label in ("baseline", "candidate"):
        build = run_command(
            pinned_command(benchmark_build_command(), cpu),
            roots[label],
            check=False,
            env=benchmark_environment(targets[label], warmups=warmups),
        )
        logs[label].append(build.stdout or "")
        if build.returncode:
            returncodes[label] = build.returncode
            aborted = True
            break

    for lane_index, lane in enumerate(lanes):
        if aborted:
            break
        for sample in range(samples):
            if aborted:
                break
            baseline_first = (lane_index + sample) % 2 == 0
            order = (
                ("baseline", "candidate")
                if baseline_first
                else ("candidate", "baseline")
            )
            for ordinal, label in enumerate(order):
                started = time.monotonic()
                completed = run_command(
                    pinned_command(benchmark_command(), cpu),
                    roots[label],
                    check=False,
                    env=benchmark_environment(
                        targets[label],
                        lane=lane,
                        sample_index=sample,
                        warmups=warmups,
                    ),
                )
                elapsed[label] += time.monotonic() - started
                logs[label].append(completed.stdout or "")
                if completed.returncode:
                    returncodes[label] = completed.returncode
                    aborted = True
                    break
                parsed = parse_records(completed.stdout or "")
                for record in parsed:
                    if record.get("lane") == lane and is_indexed_measurement_record(record):
                        if record.get("sample") != sample:
                            raise ValueError(
                                f"{label} {lane} emitted sample {record.get('sample')}, "
                                f"expected {sample}"
                            )
                        record["orchestration"] = {
                            "pair_order": "AB" if baseline_first else "BA",
                            "ordinal": ordinal,
                            "lane_index": lane_index,
                            "schedule_parity": (lane_index + sample) % 2,
                            "cpu": cpu,
                            "fresh_process": True,
                        }
                        records[label].append(record)

    runs: dict[str, dict[str, Any]] = {}
    for label in ("baseline", "candidate"):
        log_path = output / f"{label}.log"
        log_path.write_text("\n".join(logs[label]), encoding="utf-8")
        records_path = output / f"{label}.records.jsonl"
        with records_path.open("w", encoding="utf-8") as stream:
            for record in records[label]:
                stream.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
                stream.write("\n")
        records_sha256 = sha256_file(records_path)
        runs[label] = {
            "label": label,
            "status": "passed" if returncodes[label] == 0 and not aborted else "failed",
            "returncode": returncodes[label],
            "elapsed_seconds": elapsed[label],
            "command": benchmark_command(),
            "worktree": str(roots[label]),
            "log": str(log_path),
            "records": str(records_path),
            "records_sha256": records_sha256,
            "record_count": len(records[label]),
            "summary_count": sum(record.get("kind") == "summary" for record in records[label]),
            "transition_profile_count": sum(
                record.get("schema") == "lix.universal-plugin-transition-profile.v1"
                for record in records[label]
            ),
            "workload": workload_metadata(roots[label]),
            "execution": {
                "fresh_process_per_lane_sample": True,
                "paired_order": "alternating_AB_BA",
                "cpu_affinity": cpu,
                "global_fail_fast": True,
                "aborted_after_first_failure": aborted,
            },
        }
    return runs["baseline"], runs["candidate"]


def parse_records(text: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line in text.splitlines():
        prefix = None
        if MACHINE_PREFIX in line:
            prefix = MACHINE_PREFIX
        elif TRANSITION_PREFIX in line:
            prefix = TRANSITION_PREFIX
        if prefix is None:
            continue
        payload = line.split(prefix, 1)[1].strip()
        try:
            record = json.loads(payload)
        except json.JSONDecodeError as error:
            raise ValueError(f"invalid benchmark JSON record: {payload[:160]}") from error
        if isinstance(record, dict):
            records.append(record)
    return records


def is_indexed_measurement_record(record: dict[str, Any]) -> bool:
    return record.get("kind") == "sample" or record.get(
        "schema"
    ) == "lix.universal-plugin-transition-profile.v1"


def read_jsonl_records(path: str | Path) -> list[dict[str, Any]]:
    file = Path(path)
    if not file.is_file():
        return []
    return [json.loads(line) for line in file.read_text(encoding="utf-8").splitlines() if line]


def verified_jsonl_records(path: Path, expected_sha256: str | None) -> list[dict[str, Any]]:
    if not path.is_file():
        raise ValueError(f"missing saved benchmark records: {path}")
    actual = sha256_file(path)
    if not expected_sha256:
        raise ValueError(f"metadata does not pin a digest for {path.name}")
    if actual != expected_sha256:
        raise ValueError(
            f"saved benchmark records digest mismatch for {path.name}: "
            f"expected {expected_sha256}, found {actual}"
        )
    return read_jsonl_records(path)


def measurement_checkpoint_failures(
    metadata: dict[str, Any],
    baseline_records: list[dict[str, Any]],
    candidate_records: list[dict[str, Any]],
) -> list[str]:
    """Validate lane identity and paired fixtures before checkpoint reuse."""
    failures: list[str] = []
    expected_lanes = set(metadata.get("corpus", {}).get("manifest", {}).get("lanes", []))

    def indexed(
        label: str, records: list[dict[str, Any]]
    ) -> dict[tuple[str, int], dict[str, Any]]:
        result: dict[tuple[str, int], dict[str, Any]] = {}
        sample_lanes: set[str] = set()
        profile_lanes: set[str] = set()
        for record in records:
            lane = str(record.get("lane"))
            if record.get("kind") == "sample":
                sample_lanes.add(lane)
                key = (lane, int(record.get("sample", -1)))
                if key in result:
                    failures.append(
                        f"{label} has duplicate fixture record for {lane} sample {key[1]}"
                    )
                fixture = record.get("fixture")
                if not isinstance(fixture, dict):
                    failures.append(f"{label} {lane} sample {key[1]} has no fixture")
                else:
                    result[key] = fixture
            elif record.get("schema") == "lix.universal-plugin-transition-profile.v1":
                profile_lanes.add(lane)
        for kind, actual in (("sample", sample_lanes), ("profile", profile_lanes)):
            if actual != expected_lanes:
                failures.append(
                    f"{label} {kind} lanes {sorted(actual)} do not match checkpoint lanes "
                    f"{sorted(expected_lanes)}"
                )
        return result

    candidate_fixtures = indexed("candidate", candidate_records)
    baseline_status = metadata.get("baseline", {}).get("status")
    if baseline_status != "passed":
        return failures
    baseline_fixtures = indexed("baseline", baseline_records)
    if set(baseline_fixtures) != set(candidate_fixtures):
        failures.append("baseline and candidate fixture lane/sample pairs differ")
    for key in sorted(set(baseline_fixtures) & set(candidate_fixtures)):
        if baseline_fixtures[key] != candidate_fixtures[key]:
            failures.append(f"fixture mismatch for {key[0]} sample {key[1]}")
    return failures


def verify_measurement_checkpoint(
    output: Path,
    metadata: dict[str, Any],
    *,
    require_passed: bool,
    authenticate_sources: bool = True,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Authenticate and validate the saved measurements used by replay/resume."""
    if authenticate_sources:
        root_value = metadata.get("root")
        if not isinstance(root_value, str):
            raise ValueError("measurement checkpoint does not bind a repository root")
        root = Path(root_value).resolve()
        if metadata.get("baseline_revision") != PINNED_BASELINE_REVISION:
            raise ValueError("measurement checkpoint does not use the pinned baseline")
        current_candidate = working_tree_metadata(root)
        checkpoint_candidate = metadata.get("candidate", {})
        bound_fields = ("head", "status_sha256", "working_tree_sha256")
        if any(
            checkpoint_candidate.get(field) != current_candidate.get(field)
            for field in bound_fields
        ):
            raise ValueError(
                "candidate working tree no longer matches the measurement checkpoint"
            )
        current_qualification = read_qualification_spec(root)
        saved_qualification = metadata.get("qualification", {})
        if (
            saved_qualification.get("sha256") != current_qualification["sha256"]
            or saved_qualification.get("spec") != current_qualification["spec"]
        ):
            raise ValueError("qualification spec no longer matches the measurement checkpoint")
        current_corpus = read_corpus_manifest(root)
        if metadata.get("corpus") != current_corpus:
            raise ValueError("corpus no longer matches the measurement checkpoint")
        if metadata.get("samples") != current_corpus["manifest"].get("default_samples"):
            raise ValueError("sample count no longer matches the pinned corpus")
        if metadata.get("warmups") != current_corpus["manifest"].get("warmup_samples"):
            raise ValueError("warmup count no longer matches the pinned corpus")
        current_workload = workload_metadata(root)
        for key in ("baseline", "candidate_run"):
            if metadata.get(key, {}).get("workload") != current_workload:
                raise ValueError(
                    f"{key} workload contract no longer matches the measurement checkpoint"
                )
    runs: list[dict[str, Any]] = []
    records_by_label: dict[str, list[dict[str, Any]]] = {}
    for label, key in (("baseline", "baseline"), ("candidate", "candidate_run")):
        saved = metadata.get(key, {})
        if require_passed and saved.get("status") != "passed":
            raise ValueError(f"measurement checkpoint {key} did not pass")
        records_path = output / f"{label}.records.jsonl"
        records = verified_jsonl_records(records_path, saved.get("records_sha256"))
        run = {
            "label": label,
            "status": saved.get("status", "unknown"),
            "returncode": saved.get("returncode"),
            "elapsed_seconds": saved.get("elapsed_seconds", 0),
            "command": metadata.get("benchmark_command", benchmark_command()),
            "worktree": None,
            "log": str(output / f"{label}.log"),
            "records": str(records_path),
            "records_sha256": saved.get("records_sha256"),
            "record_count": len(records),
            "summary_count": sum(record.get("kind") == "summary" for record in records),
            "transition_profile_count": sum(
                record.get("schema") == "lix.universal-plugin-transition-profile.v1"
                for record in records
            ),
            "workload": saved.get("workload", {}),
        }
        runs.append(run)
        records_by_label[label] = records
    checkpoint_failures = measurement_checkpoint_failures(
        metadata, records_by_label["baseline"], records_by_label["candidate"]
    )
    if checkpoint_failures:
        raise ValueError("measurement checkpoint mismatch: " + "; ".join(checkpoint_failures))
    return (
        runs[0],
        runs[1],
        records_by_label["baseline"],
        records_by_label["candidate"],
    )


def percentile(values: Iterable[float], fraction: float) -> float | None:
    sorted_values = sorted(float(value) for value in values)
    if not sorted_values:
        return None
    index = max(0, math.ceil(len(sorted_values) * fraction) - 1)
    return sorted_values[index]


def numeric_sample_metrics(records: list[dict[str, Any]]) -> dict[str, dict[str, float | None]]:
    by_lane: dict[str, dict[str, list[float]]] = {}
    for record in records:
        if record.get("kind") != "sample":
            continue
        lane = str(record.get("lane", "unknown"))
        metrics = record.get("metrics") or {}
        lane_values = by_lane.setdefault(lane, {})
        for name, value in metrics.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                lane_values.setdefault(name, []).append(float(value))
    result: dict[str, dict[str, float | None]] = {}
    for lane, metrics in by_lane.items():
        result[lane] = {}
        for name, values in metrics.items():
            result[lane][f"{name}.p50"] = percentile(values, 0.50)
            result[lane][f"{name}.p95"] = percentile(values, 0.95)
    return result


def lane_fixture_metadata(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    fixtures: dict[str, set[str]] = {}
    decoded: dict[str, dict[str, dict[str, Any]]] = {}
    for record in records:
        if record.get("kind") != "sample" or not isinstance(record.get("fixture"), dict):
            continue
        lane = str(record.get("lane", "unknown"))
        fixture = record["fixture"]
        encoded = json.dumps(fixture, sort_keys=True, separators=(",", ":"))
        fixtures.setdefault(lane, set()).add(encoded)
        decoded.setdefault(lane, {})[encoded] = fixture
    return {
        lane: [decoded[lane][encoded] for encoded in sorted(values)]
        for lane, values in fixtures.items()
    }


def phase_profiles(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    values: dict[str, dict[str, list[float]]] = {}
    for record in records:
        if record.get("schema") != "lix.universal-plugin-transition-profile.v1":
            continue
        phases = record.get("phases_ms") or (record.get("correctness") or {}).get("phases_ms")
        if not isinstance(phases, dict):
            continue
        lane = str(record.get("lane", "unknown"))
        lane_values = values.setdefault(lane, {})
        for name, value in phases.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                lane_values.setdefault(name, []).append(float(value))
    result: dict[str, dict[str, Any]] = {}
    for lane, phases in values.items():
        result[lane] = {
            name: {
                "samples": len(items),
                "p50_ms": percentile(items, 0.50),
                "p95_ms": percentile(items, 0.95),
            }
            for name, items in phases.items()
        }
    return result


def counter_profiles(records: list[dict[str, Any]]) -> dict[str, dict[str, float | None]]:
    values: dict[str, dict[str, list[float]]] = {}
    for record in records:
        if record.get("schema") != "lix.universal-plugin-transition-profile.v1":
            continue
        counters = record.get("counters") or {}
        lane = str(record.get("lane", "unknown"))
        lane_values = values.setdefault(lane, {})
        for name, value in counters.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                lane_values.setdefault(name, []).append(float(value))
    return {
        lane: {
            key: result
            for name, items in counters.items()
            for key, result in (
                (f"{name}.p50", percentile(items, 0.50)),
                (f"{name}.p95", percentile(items, 0.95)),
            )
        }
        for lane, counters in values.items()
    }


def transition_counter_totals(records: list[dict[str, Any]]) -> tuple[dict[str, int], list[str]]:
    totals = {name: 0 for name in OUTER_JSON_COUNTERS}
    failures: list[str] = []
    for record in records:
        if record.get("schema") != "lix.universal-plugin-transition-profile.v1":
            continue
        counters = record.get("counters") or {}
        for name in totals:
            value = counters.get(name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                failures.append(
                    f"{record.get('lane')} sample {record.get('sample')} has invalid or "
                    f"missing {name}"
                )
            else:
                totals[name] += value
    return totals, failures


def evidence_failures(
    records: list[dict[str, Any]],
    *,
    candidate: bool,
    expected_phases: dict[str, list[str]] | None = None,
) -> list[str]:
    failures: list[str] = []
    required_counters = REQUIRED_CANDIDATE_COUNTERS if candidate else REQUIRED_PROFILE_COUNTERS
    for record in records:
        lane = str(record.get("lane", "unknown"))
        sample = record.get("sample", -1)
        if record.get("kind") == "sample":
            metrics = record.get("metrics")
            if not isinstance(metrics, dict):
                failures.append(f"{lane} sample {sample} has no metrics object")
                continue
            for name in REQUIRED_SAMPLE_METRICS:
                value = metrics.get(name)
                signed = name in {"live_bytes_delta", "process_rss_delta_bytes"}
                if (
                    not isinstance(value, (int, float))
                    or isinstance(value, bool)
                    or not math.isfinite(float(value))
                    or (not signed and float(value) < 0)
                ):
                    failures.append(f"{lane} sample {sample} has invalid or missing metric {name}")
            if metrics.get("process_rss_start_bytes") == 0 or metrics.get(
                "process_rss_end_bytes"
            ) == 0:
                failures.append(f"{lane} sample {sample} has unavailable current RSS evidence")
            continue
        if record.get("schema") != "lix.universal-plugin-transition-profile.v1":
            continue
        counters = record.get("counters")
        if not isinstance(counters, dict):
            failures.append(f"{lane} sample {sample} has no counters object")
            continue
        for name in required_counters:
            value = counters.get(name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                failures.append(f"{lane} sample {sample} has invalid or missing counter {name}")
        if candidate and counters.get("conflict_resolution_records", 0) == 0:
            for typed_name, transport_name in (
                ("typed_row_decode_records", "row_output_records"),
                ("typed_row_decode_bytes", "row_output_wire_bytes"),
                ("typed_row_encode_records", "row_input_records"),
                ("typed_row_encode_bytes", "row_input_wire_bytes"),
                ("typed_row_schema_validation_calls", "typed_row_decode_records"),
            ):
                if counters.get(typed_name) != counters.get(transport_name):
                    failures.append(
                        f"{lane} sample {sample} counter {typed_name} does not match "
                        f"its typed transport authority {transport_name}"
                    )
        if candidate:
            expected_page_callbacks = (
                int(counters.get("row_input_pages", 0))
                + int(counters.get("row_output_pages", 0))
                + int(counters.get("row_input_page_eof_callbacks", 0))
            )
            if int(counters.get("row_page_callback_calls", 0)) != expected_page_callbacks:
                failures.append(
                    f"{lane} sample {sample} has {counters.get('row_page_callback_calls')} "
                    f"row page callbacks, expected exactly {expected_page_callbacks} "
                    "from input, output, and terminal source pages"
                )
        phases = record.get("phases_ms")
        if not isinstance(phases, dict) or not phases:
            failures.append(f"{lane} sample {sample} has no phase accounting")
        else:
            values = {
                name: float(value)
                for name, value in phases.items()
                if isinstance(value, (int, float))
                and not isinstance(value, bool)
                and math.isfinite(float(value))
                and float(value) >= 0
            }
            if set(values) != set(phases) or "total" not in values:
                failures.append(f"{lane} sample {sample} has invalid phase values")
            else:
                required = set((expected_phases or {}).get(lane, [])) | {"total"}
                if lane in (expected_phases or {}) and set(values) != required:
                    failures.append(
                        f"{lane} sample {sample} phases {sorted(values)} do not match "
                        f"the pinned contract {sorted(required)}"
                    )
                accounted = sum(value for name, value in values.items() if name != "total")
                tolerance = max(0.2, values["total"] * 0.02)
                if abs(accounted - values["total"]) > tolerance:
                    failures.append(
                        f"{lane} sample {sample} phases account for {accounted:.3f}ms "
                        f"of {values['total']:.3f}ms"
                    )
        if candidate and lane == "text-large-typed-attachment-roundtrip":
            if counters.get("row_output_attachment_writes", 0) <= 0 or counters.get(
                "row_output_attachment_bytes", 0
            ) <= 64 * 1024:
                failures.append(f"{lane} sample {sample} did not exercise a large attachment")
    return failures


def run_summary(run: dict[str, Any], records: list[dict[str, Any]]) -> dict[str, Any]:
    profiles = [
        record
        for record in records
        if record.get("schema") == "lix.universal-plugin-transition-profile.v1"
    ]
    counters, outer_counter_failures = transition_counter_totals(records)
    samples_by_lane: dict[str, dict[int, dict[str, float]]] = {}
    profiles_by_lane: dict[str, dict[int, dict[str, Any]]] = {}
    for record in records:
        lane = str(record.get("lane", "unknown"))
        sample = int(record.get("sample", -1))
        if record.get("kind") == "sample":
            samples_by_lane.setdefault(lane, {})[sample] = record.get("metrics") or {}
        elif record.get("schema") == "lix.universal-plugin-transition-profile.v1":
            profiles_by_lane.setdefault(lane, {})[sample] = {
                "counters": record.get("counters") or {},
                "phases_ms": record.get("phases_ms") or {},
            }
    return {
        **run,
        "lanes": numeric_sample_metrics(records),
        "lane_fixtures": lane_fixture_metadata(records),
        "phase_profiles": phase_profiles(records),
        "counter_profiles": counter_profiles(records),
        "transition_profiles": len(profiles),
        "outer_row_json_counters": counters,
        "outer_row_json_evidence_failures": outer_counter_failures,
        "outer_row_json_status": (
            "proven_zero"
            if profiles
            and not outer_counter_failures
            and all(value == 0 for value in counters.values())
            else "violated"
            if any(value != 0 for value in counters.values())
            else "unmeasured"
        ),
        "samples_by_lane": samples_by_lane,
        "profiles_by_lane": profiles_by_lane,
    }


def paired_quantile_comparison(
    baseline: dict[int, float],
    candidate: dict[int, float],
    *,
    fraction: float,
    limit: float,
    seed_text: str,
    absolute_limit: float | None = None,
    proportional_ceiling: float | None = None,
) -> dict[str, Any]:
    common = sorted(
        index
        for index in set(baseline) & set(candidate)
        if isinstance(baseline[index], (int, float))
        and not isinstance(baseline[index], bool)
        and isinstance(candidate[index], (int, float))
        and not isinstance(candidate[index], bool)
        and math.isfinite(float(baseline[index]))
        and math.isfinite(float(candidate[index]))
    )
    minimum = 61 if fraction == 0.95 else 21
    if len(common) < minimum:
        return {
            "status": "insufficient_samples",
            "samples": len(common),
            "minimum_samples": minimum,
            "limit": limit,
        }
    before_values = [float(baseline[index]) for index in common]
    after_values = [float(candidate[index]) for index in common]
    before = percentile(before_values, fraction)
    after = percentile(after_values, fraction)
    if before is None or after is None or before < 0 or after < 0:
        return {"status": "unavailable", "samples": len(common), "limit": limit}
    if before == 0:
        status = "pass" if after == 0 else "unavailable"
        return {
            "baseline": before,
            "candidate": after,
            "ratio": 1.0 if status == "pass" else None,
            "status": status,
            "samples": len(common),
            "limit": limit,
            "absolute_limit": absolute_limit,
            "proportional_ceiling": proportional_ceiling,
        }
    ratio = after / before
    rng = random.Random(int.from_bytes(hashlib.sha256(seed_text.encode()).digest()[:8], "big"))
    bootstrap_ratios: list[float] = []
    bootstrap_deltas: list[float] = []
    for _ in range(5000):
        indices = [rng.randrange(len(common)) for _ in common]
        sampled_before = percentile((before_values[index] for index in indices), fraction)
        sampled_after = percentile((after_values[index] for index in indices), fraction)
        if sampled_before and sampled_after is not None:
            bootstrap_ratios.append(sampled_after / sampled_before)
            bootstrap_deltas.append(sampled_after - sampled_before)
    if len(bootstrap_ratios) < 4900:
        return {"status": "unavailable", "samples": len(common), "limit": limit}
    lower = percentile(bootstrap_ratios, 0.025)
    upper = percentile(bootstrap_ratios, 0.975)
    delta_lower = percentile(bootstrap_deltas, 0.025)
    delta_upper = percentile(bootstrap_deltas, 0.975)
    absolute_pass = (
        absolute_limit is not None
        and proportional_ceiling is not None
        and delta_upper is not None
        and delta_upper <= absolute_limit
        and upper is not None
        and upper <= proportional_ceiling
    )
    if upper is not None and upper <= limit:
        status = "pass"
        pass_basis = "proportional_limit"
    elif absolute_pass:
        status = "pass"
        pass_basis = "absolute_delta_and_proportional_ceiling"
    elif lower is not None and lower > limit:
        status = "confirmed_regression"
        pass_basis = None
    else:
        status = "inconclusive_regression"
        pass_basis = None
    return {
        "baseline": before,
        "candidate": after,
        "ratio": ratio,
        "paired_bootstrap_ratio_ci95": [lower, upper],
        "absolute_delta": after - before,
        "paired_bootstrap_absolute_delta_ci95": [delta_lower, delta_upper],
        "bootstrap_resamples": len(bootstrap_ratios),
        "samples": len(common),
        "limit": limit,
        "absolute_limit": absolute_limit,
        "proportional_ceiling": proportional_ceiling,
        "pass_basis": pass_basis,
        "status": status,
        "policy": (
            "pass when the paired-bootstrap upper 95% bound is within the limit; "
            "an absolute-delta exception also requires its upper 95% bound and the "
            "ratio upper bound to satisfy the configured proportional ceiling"
        ),
    }


def typed_transition_evidence(
    baseline_profiles: dict[int, dict[str, Any]],
    candidate_profiles: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    paired_samples = sorted(set(baseline_profiles) & set(candidate_profiles))
    candidate_samples = sorted(candidate_profiles)
    missing_or_untyped: list[int] = []
    for index in candidate_samples:
        counters = candidate_profiles[index].get("counters", {})
        values = [counters.get(name) for name in TYPED_TRANSITION_COUNTERS]
        if (
            any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in values)
            or sum(int(value) for value in values) == 0
        ):
            missing_or_untyped.append(index)
    complete = (
        bool(candidate_samples)
        and paired_samples == sorted(baseline_profiles) == candidate_samples
        and not missing_or_untyped
    )
    return {
        "complete": complete,
        "paired_samples": len(paired_samples),
        "candidate_samples": len(candidate_samples),
        "missing_or_untyped_samples": missing_or_untyped,
        "required_counters": list(TYPED_TRANSITION_COUNTERS),
    }


def compare_runs(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    paired_cpu_profile_lanes: set[str] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    comparisons: dict[str, Any] = {}
    failures: list[str] = []
    paired_cpu_profile_lanes = paired_cpu_profile_lanes or set()
    baseline_lanes = set(baseline.get("lanes", {}))
    candidate_lanes = set(candidate.get("lanes", {}))
    common_lanes = sorted(baseline_lanes & candidate_lanes)
    for lane in common_lanes:
        if baseline.get("lane_fixtures", {}).get(lane) != candidate.get(
            "lane_fixtures", {}
        ).get(lane):
            failures.append(f"{lane}: baseline and candidate fixture metadata differ")
    for lane in common_lanes:
        lane_comparison: dict[str, Any] = {}
        baseline_samples = baseline.get("samples_by_lane", {}).get(lane, {})
        candidate_samples = candidate.get("samples_by_lane", {}).get(lane, {})
        for metric, percentile_name, limit in COMPARE_METRICS:
            key = f"{metric}.{percentile_name}"
            def measured_metric(values: dict[str, Any]) -> Any:
                value = values.get(metric)
                return abs(value) if metric in SIGNED_MAGNITUDE_METRICS and value is not None else value
            result = paired_quantile_comparison(
                {index: measured_metric(values) for index, values in baseline_samples.items()},
                {index: measured_metric(values) for index, values in candidate_samples.items()},
                fraction=0.50 if percentile_name == "p50" else 0.95,
                limit=limit,
                seed_text=f"{lane}:{key}",
                absolute_limit=ABSOLUTE_REGRESSION_TOLERANCES.get(metric),
                proportional_ceiling=ABSOLUTE_TOLERANCE_PROPORTIONAL_CEILINGS.get(metric),
            )
            lane_comparison[key] = result
        # Whole-workload latency and resource metrics are acceptance gates.
        # Protocol-shape counters and subphases remain fully reported below,
        # but cannot be interpreted as regressions across a deliberate wire
        # format hard cut (for example, fewer larger pages are not worse merely
        # because their byte or callback counts differ).
        for metric, percentile_name, _limit in COMPARE_METRICS:
            key = f"{metric}.{percentile_name}"
            result = lane_comparison[key]
            if metric not in GATED_METRICS:
                result["gate_status"] = "observational"
                continue
            if result["status"] == "pass":
                result["gate_status"] = "pass"
                continue
            elapsed = lane_comparison.get("elapsed_ms.p50", {})
            physical = lane_comparison.get("physical_written_bytes.p50", {})
            baseline_profiles = baseline.get("profiles_by_lane", {}).get(lane, {})
            candidate_profiles = candidate.get("profiles_by_lane", {}).get(lane, {})
            typed_evidence = typed_transition_evidence(
                baseline_profiles, candidate_profiles
            )
            elapsed_upper = (elapsed.get("paired_bootstrap_ratio_ci95") or [None, None])[1]
            physical_upper = (physical.get("paired_bootstrap_ratio_ci95") or [None, None])[1]
            if (
                metric == "allocated_bytes"
                and lane in PARETO_EXCEPTION_LANES
                and lane in paired_cpu_profile_lanes
                and typed_evidence["complete"]
                and isinstance(elapsed_upper, (int, float))
                and elapsed_upper <= 0.80
                and isinstance(physical_upper, (int, float))
                and physical_upper <= 0.50
            ):
                result["gate_status"] = "profiled_pareto_tradeoff"
                result["gate_reason"] = (
                    "cumulative allocation increased while elapsed and durable written bytes "
                    "CI upper bounds improve by at least 20% and 50%, with passed paired CPU "
                    "artifacts and complete typed-transition evidence"
                )
                result["pareto_evidence"] = {
                    "explicit_lane": True,
                    "paired_cpu_profiles_passed": True,
                    "elapsed_ratio_ci95_upper": elapsed_upper,
                    "physical_written_ratio_ci95_upper": physical_upper,
                    "typed_transition": typed_evidence,
                }
                continue
            result["gate_status"] = "fail"
            failures.append(f"{lane}:{key} evidence is {result['status']}")
        baseline_profiles = baseline.get("profiles_by_lane", {}).get(lane, {})
        candidate_profiles = candidate.get("profiles_by_lane", {}).get(lane, {})
        counter_comparison: dict[str, Any] = {}
        for counter, percentile_name, limit in COMPARE_COUNTERS:
            key = f"{counter}.{percentile_name}"
            result = paired_quantile_comparison(
                {
                    index: values.get("counters", {}).get(counter)
                    for index, values in baseline_profiles.items()
                },
                {
                    index: values.get("counters", {}).get(counter)
                    for index, values in candidate_profiles.items()
                },
                fraction=0.50 if percentile_name == "p50" else 0.95,
                limit=limit,
                seed_text=f"{lane}:counter:{key}",
                absolute_limit=(
                    ABSOLUTE_REGRESSION_TOLERANCES.get(counter)
                    if counter == "guest_linear_memory_high_water_bytes"
                    else None
                ),
                proportional_ceiling=(
                    ABSOLUTE_TOLERANCE_PROPORTIONAL_CEILINGS.get(counter)
                    if counter == "guest_linear_memory_high_water_bytes"
                    else None
                ),
            )
            counter_comparison[key] = result
            if counter == "guest_linear_memory_high_water_bytes":
                if result["status"] == "pass":
                    result["gate_status"] = "pass"
                else:
                    result["gate_status"] = "fail"
                    failures.append(f"{lane}:{key} evidence is {result['status']}")
            else:
                result["gate_status"] = "observational"
        lane_comparison["transition_counters"] = counter_comparison
        phase_comparison: dict[str, Any] = {}
        baseline_phase_names = {
            name
            for values in baseline_profiles.values()
            for name in values.get("phases_ms", {})
        }
        candidate_phase_names = {
            name
            for values in candidate_profiles.values()
            for name in values.get("phases_ms", {})
        }
        if baseline_phase_names != candidate_phase_names:
            failures.append(
                f"{lane}: phase sets differ between baseline "
                f"{sorted(baseline_phase_names)} and candidate {sorted(candidate_phase_names)}"
            )
        for phase in sorted(baseline_phase_names | candidate_phase_names):
            phase_comparison[phase] = {}
            for percentile_name, limit in (("p50_ms", 1.10), ("p95_ms", 1.15)):
                result = paired_quantile_comparison(
                    {
                        index: values.get("phases_ms", {}).get(phase)
                        for index, values in baseline_profiles.items()
                    },
                    {
                        index: values.get("phases_ms", {}).get(phase)
                        for index, values in candidate_profiles.items()
                    },
                    fraction=0.50 if percentile_name == "p50_ms" else 0.95,
                    limit=limit,
                    seed_text=f"{lane}:phase:{phase}:{percentile_name}",
                )
                phase_comparison[phase][percentile_name] = result
                result["gate_status"] = "diagnostic"
        lane_comparison["phases"] = phase_comparison
        comparisons[lane] = lane_comparison
    return {
        "common_lanes": common_lanes,
        "lanes": comparisons,
        "unmatched_baseline_lanes": sorted(set(baseline.get("lanes", {})) - set(common_lanes)),
        "unmatched_candidate_lanes": sorted(set(candidate.get("lanes", {})) - set(common_lanes)),
    }, failures


def paired_correctness_failures(
    baseline_records: list[dict[str, Any]],
    candidate_records: list[dict[str, Any]],
    expected_lanes: set[str],
    samples: int,
) -> list[str]:
    failures: list[str] = []

    def indexed(records: list[dict[str, Any]], kind: str) -> dict[tuple[str, int], dict[str, Any]]:
        result: dict[tuple[str, int], dict[str, Any]] = {}
        for record in records:
            is_profile = record.get("schema") == "lix.universal-plugin-transition-profile.v1"
            if (kind == "profile") != is_profile:
                continue
            if kind == "sample" and record.get("kind") != "sample":
                continue
            key = (str(record.get("lane")), int(record.get("sample", -1)))
            if key in result:
                failures.append(f"duplicate {kind} record for {key[0]} sample {key[1]}")
            result[key] = record
        return result

    baseline_samples = indexed(baseline_records, "sample")
    candidate_samples = indexed(candidate_records, "sample")
    baseline_profiles = indexed(baseline_records, "profile")
    candidate_profiles = indexed(candidate_records, "profile")
    expected = {(lane, sample) for lane in expected_lanes for sample in range(samples)}
    for label, actual in (
        ("baseline sample", set(baseline_samples)),
        ("candidate sample", set(candidate_samples)),
        ("baseline profile", set(baseline_profiles)),
        ("candidate profile", set(candidate_profiles)),
    ):
        missing = expected - actual
        excess = actual - expected
        if missing:
            failures.append(f"{label} records missing {len(missing)} lane/sample pairs")
        if excess:
            failures.append(f"{label} records contain {len(excess)} unexpected lane/sample pairs")

    for key in sorted(expected & set(baseline_profiles) & set(candidate_profiles)):
        before = dict(baseline_profiles[key].get("correctness") or {})
        after = dict(candidate_profiles[key].get("correctness") or {})
        before.pop("phases_ms", None)
        after.pop("phases_ms", None)
        if before != after:
            failures.append(f"correctness mismatch for {key[0]} sample {key[1]}")
    return failures


def cross_plugin_report(
    candidate: dict[str, Any],
    comparison: dict[str, Any],
    cpu_profiles: dict[str, Any] | None = None,
) -> dict[str, Any]:
    report: dict[str, Any] = {}
    cpu_profiles = cpu_profiles or {}
    for plugin in BUNDLED_PLUGINS:
        lanes = sorted(
            lane for lane in candidate.get("lanes", {}) if lane.startswith(f"{plugin}-")
        )
        phase_candidates = []
        for lane in lanes:
            for phase, values in candidate.get("phase_profiles", {}).get(lane, {}).items():
                if phase == "total":
                    continue
                p95 = values.get("p95_ms")
                if p95 is not None:
                    phase_candidates.append((float(p95), lane, phase))
        largest = max(phase_candidates, default=None)
        elapsed_ratios = []
        for lane in lanes:
            measured = (
                comparison.get("lanes", {})
                .get(lane, {})
                .get("elapsed_ms.p50", {})
            )
            if measured.get("ratio") is not None:
                elapsed_ratios.append(
                    {
                        "lane": lane,
                        "candidate_to_baseline_ratio": measured["ratio"],
                        "percent_change": (measured["ratio"] - 1.0) * 100.0,
                    }
                )
        sampled_bottlenecks = []
        for artifact in cpu_profiles.get("artifacts", []):
            if artifact.get("revision") != "candidate" or artifact.get("plugin") != plugin:
                continue
            top = artifact.get("profile_structure", {}).get("top_sampled_leaf_functions", [])
            if top:
                sampled_bottlenecks.append(
                    {
                        "lane": artifact.get("lane"),
                        "function": top[0].get("function"),
                        "self_samples": top[0].get("samples"),
                        "retained_samples": artifact.get("profile_structure", {}).get("samples"),
                    }
                )
        sampled_bottlenecks.sort(
            key=lambda value: (-int(value.get("self_samples") or 0), str(value.get("lane")))
        )
        report[plugin] = {
            "lanes": lanes,
            "elapsed_p50": elapsed_ratios,
            "largest_remaining_measured_phase": (
                {
                    "lane": largest[1],
                    "phase": largest[2],
                    "candidate_p95_ms": largest[0],
                }
                if largest
                else None
            ),
            "cpu_profile_lanes": lanes,
            "largest_remaining_cpu_bottleneck": (
                sampled_bottlenecks[0] if sampled_bottlenecks else None
            ),
        }
    return report


def build_report(
    root: Path,
    metadata: dict[str, Any],
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    baseline_records: list[dict[str, Any]],
    candidate_records: list[dict[str, Any]],
    *,
    require_baseline: bool,
) -> tuple[dict[str, Any], int]:
    baseline_summary = run_summary(baseline, baseline_records)
    candidate_summary = run_summary(candidate, candidate_records)
    corpus_manifest = metadata.get("corpus", {}).get("manifest", {})
    workload_contract = corpus_manifest.get("workload_contract", {})
    expected_phases = {
        lane: list(workload_contract[contract])
        for lane, contract in corpus_manifest.get("lane_contracts", {}).items()
        if contract in workload_contract
    }
    baseline_available = (
        baseline_summary["status"] == "passed"
        and bool(baseline_summary["lanes"])
        and baseline_summary["transition_profiles"] > 0
    )
    candidate_ok = candidate_summary["status"] == "passed"
    candidate_evidence_failures = evidence_failures(
        candidate_records, candidate=True, expected_phases=expected_phases
    )
    baseline_evidence_failures = (
        evidence_failures(
            baseline_records, candidate=False, expected_phases=expected_phases
        )
        if baseline_summary["status"] == "passed"
        else []
    )
    expected_lanes = set(
        metadata.get("corpus", {}).get("manifest", {}).get("lanes", [])
    )
    candidate_profile_lanes = {
        str(record.get("lane"))
        for record in candidate_records
        if record.get("schema") == "lix.universal-plugin-transition-profile.v1"
    }
    missing_profile_lanes = sorted(expected_lanes - candidate_profile_lanes)
    unexpected_profile_lanes = sorted(candidate_profile_lanes - expected_lanes)
    expected_profile_count = len(expected_lanes) * int(metadata.get("samples", 0))
    actual_profile_count = candidate_summary["transition_profiles"]
    profile_count_matches = (
        expected_profile_count == 0 or actual_profile_count == expected_profile_count
    )
    candidate_profiles_complete = (
        not missing_profile_lanes
        and not unexpected_profile_lanes
        and profile_count_matches
    )
    typed_engagement_missing = []
    for record in candidate_records:
        if record.get("schema") != "lix.universal-plugin-transition-profile.v1":
            continue
        lane = str(record.get("lane"))
        counters = record.get("counters", {})
        if (
            int(counters.get("typed_row_decode_records", 0))
            + int(counters.get("typed_row_encode_records", 0))
            + int(counters.get("typed_row_schema_validation_calls", 0))
            + int(counters.get("typed_transaction_validation_calls", 0))
            == 0
        ):
            typed_engagement_missing.append(
                {"lane": lane, "sample": int(record.get("sample", -1))}
            )
    typed_engagement_complete = not typed_engagement_missing
    candidate_zero = (
        candidate_summary["outer_row_json_status"] == "proven_zero"
        and candidate_profiles_complete
        and typed_engagement_complete
        and metadata.get("hard_cut_source_audit", {"status": "pass"}).get("status")
        == "pass"
        and not candidate_evidence_failures
    )
    cpu_profile_status = metadata.get("cpu_profiles", {}).get("status")
    cpu_profile_failures = (
        cpu_profile_evidence_failures(metadata["cpu_profiles"], expected_lanes)
        if "cpu_profiles" in metadata
        else []
    )
    cpu_profiles_complete = cpu_profile_status in (None, "complete") and not cpu_profile_failures
    paired_cpu_profile_lanes = (
        passed_paired_cpu_profile_lanes(metadata["cpu_profiles"])
        if "cpu_profiles" in metadata
        else set()
    )
    comparison: dict[str, Any] = {
        "status": "baseline_unavailable",
        "reason": "baseline did not produce a successful machine-readable summary",
        "common_lanes": [],
        "lanes": {},
    }
    failures: list[str] = [*candidate_evidence_failures, *cpu_profile_failures]
    if baseline_available:
        failures.extend(baseline_evidence_failures)
        workload_compatible = (
            baseline_summary.get("workload", {}).get("contract_sha256")
            == candidate_summary.get("workload", {}).get("contract_sha256")
        )
        if not workload_compatible:
            comparison["status"] = "baseline_not_comparable"
            comparison["reason"] = (
                "baseline and candidate benchmark source/corpus digests differ; "
                "the baseline is retained as an artifact but is not used for a gate"
            )
            comparison["baseline_workload"] = baseline_summary.get("workload")
            comparison["candidate_workload"] = candidate_summary.get("workload")
        else:
            comparison, comparison_failures = compare_runs(
                baseline_summary,
                candidate_summary,
                paired_cpu_profile_lanes=paired_cpu_profile_lanes,
            )
            failures.extend(comparison_failures)
            failures.extend(
                paired_correctness_failures(
                    baseline_records,
                    candidate_records,
                    expected_lanes,
                    int(metadata.get("samples", 0)),
                )
            )
        if not comparison["common_lanes"]:
            if comparison.get("status") != "baseline_not_comparable":
                comparison["status"] = "baseline_not_comparable"
                comparison["reason"] = "baseline and candidate have no common measured lanes"
        else:
            comparison["status"] = "regression" if failures else "comparable"
    gate_status = "pass"
    if not candidate_ok:
        gate_status = "candidate_failed"
    elif not candidate_zero:
        gate_status = "typed_row_zero_json_invariant_unproven"
    elif not cpu_profiles_complete:
        gate_status = "cpu_profiles_incomplete"
    elif not baseline_available:
        gate_status = "baseline_unavailable"
    elif failures:
        gate_status = "regression"
    elif comparison.get("status") == "baseline_not_comparable":
        gate_status = "baseline_not_comparable"
    exit_code = 0
    if gate_status in {
        "candidate_failed",
        "typed_row_zero_json_invariant_unproven",
        "cpu_profiles_incomplete",
        "regression",
    }:
        exit_code = 1
    if gate_status in {"baseline_unavailable", "baseline_not_comparable"} and require_baseline:
        exit_code = 2

    remaining_blockers: list[str] = []
    if not candidate_ok:
        remaining_blockers.append("candidate benchmark execution or record production failed")
    if not candidate_zero:
        remaining_blockers.append(
            "zero outer-row JSON and complete typed-boundary evidence is not proven"
        )
    if not cpu_profiles_complete:
        remaining_blockers.append(
            "paired CPU profiles with retained sampled guest frames are incomplete"
        )
    if not baseline_available:
        remaining_blockers.append("the pinned baseline benchmark is unavailable")
    elif comparison.get("status") == "baseline_not_comparable":
        remaining_blockers.append("baseline and candidate evidence is not comparable")
    if failures:
        remaining_blockers.append(
            f"{len(failures)} correctness, evidence, or material-regression gate failure(s) remain"
        )

    report = {
        "schema": REPORT_SCHEMA,
        "benchmark": BENCHMARK_TEST,
        "contract": "typed-row-fixtures-v2",
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "metadata": metadata,
        "baseline": baseline_summary,
        "candidate": candidate_summary,
        "comparison": comparison,
        "cross_plugin": cross_plugin_report(
            candidate_summary, comparison, metadata.get("cpu_profiles", {})
        ),
        "invariants": {
            "candidate_outer_row_json": {
                "status": candidate_summary["outer_row_json_status"],
                "counters": candidate_summary["outer_row_json_counters"],
                "evidence_failures": candidate_summary[
                    "outer_row_json_evidence_failures"
                ],
                "expected_lanes": sorted(expected_lanes),
                "profile_lanes": sorted(candidate_profile_lanes),
                "missing_lanes": missing_profile_lanes,
                "unexpected_lanes": unexpected_profile_lanes,
                "expected_profile_count": expected_profile_count,
                "actual_profile_count": actual_profile_count,
                "profile_count_matches": profile_count_matches,
                "complete": candidate_profiles_complete,
                "required": True,
            },
            "candidate_machine_records": {
                "status": "present" if candidate_summary["record_count"] else "missing",
                "required": True,
            },
            "candidate_typed_boundary_engagement": {
                "complete": typed_engagement_complete,
                "missing_lane_samples": typed_engagement_missing,
                "required": True,
            },
            "hard_cut_source_audit": {
                **metadata.get("hard_cut_source_audit", {}),
                "required": True,
            },
            "required_evidence": {
                "candidate_failures": candidate_evidence_failures,
                "baseline_failures": baseline_evidence_failures,
                "candidate_complete": not candidate_evidence_failures,
                "baseline_complete": not baseline_evidence_failures,
                "required_sample_metrics": list(REQUIRED_SAMPLE_METRICS),
                "required_candidate_counters": list(REQUIRED_CANDIDATE_COUNTERS),
                "required_baseline_counters": list(REQUIRED_PROFILE_COUNTERS),
                "required": True,
            },
            "cpu_profiles": {
                "status": cpu_profile_status or "not_requested",
                "complete": cpu_profiles_complete,
                "evidence_failures": cpu_profile_failures,
                "required_for_orchestrated_run": "cpu_profiles" in metadata,
            },
        },
        "gate": {
            "status": gate_status,
            "exit_code": exit_code,
            "baseline_required": require_baseline,
            "failures": failures,
            "policy": {
                "missing_baseline": "report_and_pass_unless_require_baseline",
                "regression": "fail",
                "candidate_failure": "fail",
                "outer_row_json_nonzero": "fail",
            },
        },
        "remaining_blockers": remaining_blockers,
    }
    return report, exit_code


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def render_markdown_report(report: dict[str, Any]) -> str:
    metadata = report.get("metadata", {})
    environment = metadata.get("environment", {})
    lines = [
        "# Lix typed-row plugin benchmark qualification",
        "",
        f"- Gate: `{report['gate']['status']}` (exit `{report['gate']['exit_code']}`)",
        f"- Baseline: `{metadata.get('baseline_revision')}`",
        f"- Samples: `{metadata.get('samples')}` paired observations per lane",
        f"- Warmups: `{metadata.get('warmups')}` default; pinned lane exceptions are in the corpus manifest",
        f"- CPU: `{environment.get('cpu_model')}`; affinity `{metadata.get('candidate_run', {}).get('execution', {}).get('cpu_affinity')}`",
        f"- Rust: `{str(environment.get('rustc')).splitlines()[0] if environment.get('rustc') else None}`",
        "",
        "## Cross-plugin result",
        "",
        "| Plugin | Measured lanes | p50 elapsed changes | Largest candidate p95 phase | Top candidate CPU self-sample |",
        "| --- | ---: | --- | --- | --- |",
    ]
    for plugin, values in report.get("cross_plugin", {}).items():
        changes = ", ".join(
            f"{item['lane']}: {item['percent_change']:+.1f}%"
            for item in values.get("elapsed_p50", [])
        ) or "unavailable"
        bottleneck = values.get("largest_remaining_measured_phase")
        bottleneck_text = (
            f"{bottleneck['lane']} / {bottleneck['phase']}: {bottleneck['candidate_p95_ms']:.3f} ms"
            if bottleneck
            else "unavailable"
        )
        cpu_bottleneck = values.get("largest_remaining_cpu_bottleneck")
        cpu_function = (
            str(cpu_bottleneck.get("function", "")).replace("|", "\\|")
            if cpu_bottleneck
            else ""
        )
        cpu_bottleneck_text = (
            f"{cpu_bottleneck['lane']} / `{cpu_function}`: "
            f"{cpu_bottleneck['self_samples']} of {cpu_bottleneck['retained_samples']} samples"
            if cpu_bottleneck
            else "unavailable"
        )
        lines.append(
            f"| {plugin} | {len(values.get('lanes', []))} | {changes} | "
            f"{bottleneck_text} | {cpu_bottleneck_text} |"
        )
    lines.extend(
        [
            "",
            "## Per-lane before/after measurements",
            "",
            "All values are baseline → candidate p50 / p95. Bytes and counts are per measured sample.",
            "",
            "| Lane | Elapsed ms | Allocation count | Allocated bytes | Live delta | Peak live delta | Large allocations | RSS end | RSS delta |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    baseline = report.get("baseline", {})
    candidate = report.get("candidate", {})
    comparison = report.get("comparison", {}).get("lanes", {})
    def pair(lane: str, metric: str) -> str:
        before50 = baseline.get("lanes", {}).get(lane, {}).get(f"{metric}.p50")
        before95 = baseline.get("lanes", {}).get(lane, {}).get(f"{metric}.p95")
        after50 = candidate.get("lanes", {}).get(lane, {}).get(f"{metric}.p50")
        after95 = candidate.get("lanes", {}).get(lane, {}).get(f"{metric}.p95")
        if None in (before50, before95, after50, after95):
            return "unavailable"
        return f"{before50:.3f}→{after50:.3f} / {before95:.3f}→{after95:.3f}"
    def counter_pair(lane: str, counter: str) -> str:
        values = comparison.get(lane, {}).get("transition_counters", {})
        p50 = values.get(f"{counter}.p50", {})
        p95 = values.get(f"{counter}.p95", {})
        if any(item.get("baseline") is None or item.get("candidate") is None for item in (p50, p95)):
            return "unavailable"
        return (
            f"{p50['baseline']:.0f}→{p50['candidate']:.0f} / "
            f"{p95['baseline']:.0f}→{p95['candidate']:.0f}"
        )
    def summed_profile(
        prefixes: tuple[str, ...], percentile_name: str, source: dict[str, Any]
    ) -> float | None:
        values = [source.get(f"{name}.{percentile_name}") for name in prefixes]
        return None if any(value is None for value in values) else sum(float(value) for value in values)
    for lane in sorted(candidate.get("lanes", {})):
        lines.append(
            f"| {lane} | {pair(lane, 'elapsed_ms')} | {pair(lane, 'allocation_count')} | "
            f"{pair(lane, 'allocated_bytes')} | {pair(lane, 'live_bytes_delta')} | "
            f"{pair(lane, 'peak_live_bytes_delta')} | {pair(lane, 'large_allocation_count')} | "
            f"{pair(lane, 'process_rss_end_bytes')} | {pair(lane, 'process_rss_delta_bytes')} |"
        )
    lines.extend(
        [
            "",
            "## Per-lane typed boundary measurements",
            "",
            "| Lane | Boundary bytes | Input wire bytes | Output wire bytes | Pages (in+out) | Callbacks | Input attachment bytes | Output attachment bytes | Guest memory bytes |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for lane in sorted(candidate.get("lanes", {})):
        before_counters = baseline.get("counter_profiles", {}).get(lane, {})
        after_counters = candidate.get("counter_profiles", {}).get(lane, {})
        page_names = ("row_input_pages", "row_output_pages")
        before_pages50 = summed_profile(page_names, "p50", before_counters)
        before_pages95 = summed_profile(page_names, "p95", before_counters)
        after_pages50 = summed_profile(page_names, "p50", after_counters)
        after_pages95 = summed_profile(page_names, "p95", after_counters)
        pages = (
            "unavailable"
            if None in (before_pages50, before_pages95, after_pages50, after_pages95)
            else f"{before_pages50:.0f}→{after_pages50:.0f} / {before_pages95:.0f}→{after_pages95:.0f}"
        )
        lines.append(
            f"| {lane} | {counter_pair(lane, 'component_boundary_bytes')} | "
            f"{counter_pair(lane, 'row_input_wire_bytes')} | {counter_pair(lane, 'row_output_wire_bytes')} | "
            f"{pages} | {counter_pair(lane, 'row_page_callback_calls')} | "
            f"{counter_pair(lane, 'row_input_attachment_bytes')} | "
            f"{counter_pair(lane, 'row_output_attachment_bytes')} | "
            f"{counter_pair(lane, 'guest_linear_memory_high_water_bytes')} |"
        )
    lines.extend(
        [
            "",
            "## Phase-level p50 / p95",
            "",
            "| Lane | Phase | Baseline ms | Candidate ms |",
            "| --- | --- | --- | --- |",
        ]
    )
    for lane in sorted(candidate.get("phase_profiles", {})):
        before_phases = baseline.get("phase_profiles", {}).get(lane, {})
        after_phases = candidate.get("phase_profiles", {}).get(lane, {})
        for phase in sorted(after_phases):
            before = before_phases.get(phase, {})
            after = after_phases.get(phase, {})
            before_text = (
                f"{before['p50_ms']:.3f} / {before['p95_ms']:.3f}"
                if before.get("p50_ms") is not None and before.get("p95_ms") is not None
                else "unavailable"
            )
            after_text = (
                f"{after['p50_ms']:.3f} / {after['p95_ms']:.3f}"
                if after.get("p50_ms") is not None and after.get("p95_ms") is not None
                else "unavailable"
            )
            lines.append(f"| {lane} | {phase} | {before_text} | {after_text} |")
    lines.extend(
        [
            "",
            "## Hard-cut evidence",
            "",
            f"- Outer-row JSON: `{report.get('candidate', {}).get('outer_row_json_status')}`",
            f"- Candidate required evidence complete: `{report.get('invariants', {}).get('required_evidence', {}).get('candidate_complete')}`",
            f"- CPU profiles: `{report.get('invariants', {}).get('cpu_profiles', {}).get('status')}`",
            "",
            "The machine-readable report, JSONL records, raw logs, corpus/adapter digests, and presymbolicated CPU profiles are retained in the same artifact.",
            "",
        ]
    )
    blockers = report.get("remaining_blockers", [])
    lines.extend(["## Remaining blockers", ""])
    lines.extend([f"- {blocker}" for blocker in blockers] if blockers else ["- None."])
    lines.append("")
    failures = report.get("gate", {}).get("failures", [])
    if failures:
        lines.extend(["## Qualification failures", ""])
        lines.extend(f"- {failure}" for failure in failures)
        lines.append("")
    return "\n".join(lines)


def write_reports(output: Path, report: dict[str, Any]) -> None:
    write_json(output / "report.json", report)
    (output / "PLUGIN_TYPED_ROW_BENCHMARK_REPORT.md").write_text(
        render_markdown_report(report), encoding="utf-8"
    )


def run_orchestration(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    if args.samples <= 0 or args.warmups < 0:
        raise ValueError("samples must be positive and warmups must be non-negative")
    qualification = read_qualification_spec(root)
    corpus = read_corpus_manifest(root)
    if corpus["manifest"].get("default_samples") != args.samples:
        raise ValueError(
            f"samples={args.samples} does not match pinned corpus default "
            f"{corpus['manifest'].get('default_samples')}"
        )
    if corpus["manifest"].get("warmup_samples") != args.warmups:
        raise ValueError(
            f"warmups={args.warmups} does not match pinned corpus "
            f"{corpus['manifest'].get('warmup_samples')}"
        )

    baseline_revision = resolve_revision(root, args.baseline)
    if baseline_revision is not None and baseline_revision != PINNED_BASELINE_REVISION:
        raise ValueError(
            f"baseline {args.baseline} resolved to {baseline_revision}, expected pinned "
            f"revision {PINNED_BASELINE_REVISION}"
        )
    environment = environment_metadata(root, args.samples, args.warmups)
    enforce_environment_requirements(
        environment, qualification["spec"].get("environment_requirements", {})
    )
    cpu = pinned_cpu()
    metadata = {
        "root": str(root),
        "baseline_ref": args.baseline,
        "baseline_revision": baseline_revision,
        "candidate": working_tree_metadata(root),
        "corpus": corpus,
        "qualification": qualification,
        "environment": environment,
        "pinned_cpu": cpu,
        "samples": args.samples,
        "warmups": args.warmups,
        "benchmark_command": benchmark_command(),
        "baseline_policy": (
            "allow_missing" if args.allow_missing_baseline else "require_available"
        ),
        "baseline_adapter": None,
        "hard_cut_source_audit": hard_cut_source_audit(root),
    }
    build_cache = Path(
        os.environ.get("LIX_PLUGIN_API_BUILD_CACHE", "/tmp/lix-plugin-api-build-cache")
    ).resolve()
    build_cache.mkdir(parents=True, exist_ok=True)
    baseline_target = build_cache / f"baseline-{PINNED_BASELINE_REVISION[:10]}"
    candidate_target = root / "target"
    metadata["build_cache"] = {
        "root": str(build_cache),
        "baseline_target": str(baseline_target),
        "candidate_target": str(candidate_target),
        "measurement_policy": "build time excluded; Cargo validates cached artifacts before paired samples",
    }
    write_json(output / "metadata.json", metadata)

    baseline_run: dict[str, Any]
    candidate_run: dict[str, Any]
    with tempfile.TemporaryDirectory(prefix="lix-plugin-api-benchmark-"):
        if baseline_revision is None:
            baseline_run = {
                "label": "baseline",
                "status": "unavailable",
                "returncode": None,
                "elapsed_seconds": 0,
                "command": benchmark_command(),
                "worktree": None,
                "log": str(output / "baseline.log"),
                "records": str(output / "baseline.records.jsonl"),
                "record_count": 0,
                "summary_count": 0,
                "transition_profile_count": 0,
                "reason": f"cannot resolve baseline ref {args.baseline}",
            }
            (output / "baseline.log").write_text(
                baseline_run["reason"] + "\n", encoding="utf-8"
            )
            (output / "baseline.records.jsonl").write_text("", encoding="utf-8")
            baseline_run["records_sha256"] = sha256_file(
                output / "baseline.records.jsonl"
            )
        else:
            worktree_root = build_cache / "worktrees"
            worktree_root.mkdir(parents=True, exist_ok=True)
            baseline_worktree = (
                worktree_root / f"baseline-{PINNED_BASELINE_REVISION[:10]}"
            )
            run_command(
                ["git", "worktree", "add", "--detach", str(baseline_worktree), baseline_revision],
                root,
            )
            try:
                metadata["baseline_adapter"] = prepare_pinned_baseline(
                    root, baseline_worktree
                )
                baseline_run, candidate_run = run_paired_benchmarks(
                    baseline_worktree,
                    root,
                    output,
                    lanes=list(corpus["manifest"]["lanes"]),
                    samples=args.samples,
                    warmups=args.warmups,
                    baseline_target=baseline_target,
                    candidate_target=candidate_target,
                )
                metadata["baseline"] = baseline_run
                metadata["candidate_run"] = candidate_run
                write_json(output / "metadata.json", metadata)
                if all(
                    run["status"] == "passed"
                    for run in (baseline_run, candidate_run)
                ):
                    try:
                        metadata["cpu_profiles"] = collect_cpu_profiles(
                            baseline_worktree,
                            root,
                            output,
                            lanes=list(corpus["manifest"]["lanes"]),
                            baseline_target=baseline_target,
                            candidate_target=candidate_target,
                        )
                    except Exception as error:
                        metadata["cpu_profiles"] = {
                            "status": "failed",
                            "reason": str(error),
                            "exception": type(error).__name__,
                        }
                else:
                    metadata["cpu_profiles"] = {
                        "status": "skipped",
                        "reason": "paired measurements must pass before CPU profile collection",
                    }
            finally:
                run_command(
                    ["git", "worktree", "remove", "--force", str(baseline_worktree)],
                    root,
                    check=False,
                )
        if baseline_revision is None:
            metadata["cpu_profiles"] = {
                "status": "unavailable",
                "reason": "paired CPU profiles require the pinned baseline",
            }
            candidate_run = run_benchmark(
                root,
                output,
                "candidate",
                samples=args.samples,
                warmups=args.warmups,
                target_dir=candidate_target,
            )

    baseline_records = read_jsonl_records(baseline_run["records"])
    candidate_records = read_jsonl_records(candidate_run["records"])
    metadata["baseline"] = baseline_run
    metadata["candidate_run"] = candidate_run
    write_json(output / "metadata.json", metadata)
    report, exit_code = build_report(
        root,
        metadata,
        baseline_run,
        candidate_run,
        baseline_records,
        candidate_records,
        require_baseline=args.require_baseline or not args.allow_missing_baseline,
    )
    write_reports(output, report)
    print(json.dumps({"report": str(output / "report.json"), "gate": report["gate"]}, indent=2))
    return exit_code


def report_existing(args: argparse.Namespace) -> int:
    output = Path(args.output).resolve()
    metadata = json.loads((output / "metadata.json").read_text(encoding="utf-8"))
    if isinstance(metadata.get("cpu_profiles"), dict):
        metadata["cpu_profiles"]["artifact_root"] = str(output)
    baseline_run, candidate_run, baseline_records, candidate_records = (
        verify_measurement_checkpoint(output, metadata, require_passed=False)
    )
    report, exit_code = build_report(
        Path(metadata["root"]),
        metadata,
        baseline_run,
        candidate_run,
        baseline_records,
        candidate_records,
        require_baseline=args.require_baseline,
    )
    write_reports(output, report)
    print(json.dumps(report["gate"], indent=2))
    return exit_code


def profile_existing(args: argparse.Namespace) -> int:
    """Recollect CPU profiles from a verified paired-measurement checkpoint."""
    output = Path(args.output).resolve()
    metadata_path = output / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    root = Path(metadata["root"]).resolve()
    qualification = read_qualification_spec(root)
    environment = environment_metadata(
        root, int(metadata.get("samples", 0)), int(metadata.get("warmups", 0))
    )
    enforce_environment_requirements(
        environment, qualification["spec"].get("environment_requirements", {})
    )
    pinned_cpu()
    if metadata.get("baseline_revision") != PINNED_BASELINE_REVISION:
        raise ValueError("profile checkpoint does not use the pinned baseline revision")
    verify_measurement_checkpoint(output, metadata, require_passed=True)
    checkpoint_candidate = metadata.get("candidate", {})
    resumed_candidate = working_tree_metadata(root)
    bound_fields = ("head", "status_sha256", "working_tree_sha256")
    if any(checkpoint_candidate.get(field) != resumed_candidate.get(field) for field in bound_fields):
        raise ValueError(
            "candidate working tree no longer matches the paired-measurement checkpoint"
        )

    build_cache = Path(metadata["build_cache"]["root"])
    baseline_target = Path(metadata["build_cache"]["baseline_target"])
    candidate_target = Path(metadata["build_cache"]["candidate_target"])
    worktree_root = build_cache / "worktrees"
    worktree_root.mkdir(parents=True, exist_ok=True)
    baseline_worktree = worktree_root / f"baseline-{PINNED_BASELINE_REVISION[:10]}"
    run_command(
        ["git", "worktree", "add", "--detach", str(baseline_worktree), PINNED_BASELINE_REVISION],
        root,
    )
    try:
        metadata["baseline_adapter"] = prepare_pinned_baseline(root, baseline_worktree)
        try:
            metadata["cpu_profiles"] = collect_cpu_profiles(
                baseline_worktree,
                root,
                output,
                lanes=list(metadata["corpus"]["manifest"]["lanes"]),
                baseline_target=baseline_target,
                candidate_target=candidate_target,
            )
        except Exception as error:
            metadata["cpu_profiles"] = {
                "status": "failed",
                "reason": str(error),
                "exception": type(error).__name__,
            }
    finally:
        run_command(
            ["git", "worktree", "remove", "--force", str(baseline_worktree)],
            root,
            check=False,
        )
    metadata["profile_collection_resume"] = {
        "measurement_checkpoint_reused": True,
        "candidate_checkpoint_match": True,
        "runner_working_tree": resumed_candidate,
    }
    write_json(metadata_path, metadata)
    return report_existing(
        argparse.Namespace(output=str(output), require_baseline=args.require_baseline)
    )


def parser() -> argparse.ArgumentParser:
    command_parser = argparse.ArgumentParser(description=__doc__)
    subparsers = command_parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run", help="run baseline and candidate")
    run_parser.add_argument("--root", default=".")
    run_parser.add_argument("--output", required=True)
    run_parser.add_argument("--baseline", default=DEFAULT_BASELINE)
    run_parser.add_argument("--samples", type=int, default=DEFAULT_SAMPLES)
    run_parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUPS)
    run_parser.add_argument(
        "--allow-missing-baseline",
        action="store_true",
        help="document that an unavailable baseline is reported but does not fail the gate",
    )
    run_parser.add_argument("--require-baseline", action="store_true")
    run_parser.set_defaults(handler=run_orchestration)

    report_parser = subparsers.add_parser("report", help="rebuild a report from saved logs")
    report_parser.add_argument("--output", required=True)
    report_parser.add_argument("--require-baseline", action="store_true")
    report_parser.set_defaults(handler=report_existing)
    profile_parser = subparsers.add_parser(
        "profiles", help="recollect profiles from a verified measurement checkpoint"
    )
    profile_parser.add_argument("--output", required=True)
    profile_parser.add_argument("--require-baseline", action="store_true")
    profile_parser.set_defaults(handler=profile_existing)
    return command_parser


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"plugin API benchmark runner: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
