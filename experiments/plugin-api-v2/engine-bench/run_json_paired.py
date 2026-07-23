#!/usr/bin/env python3
"""Run a counterbalanced JSON Component-v1 versus Component-v2 campaign.

Both arms use the same benchmark executable. `LIX_PROFILE_JSON_API` selects the
embedded plugin, while each fresh process starts from an API-specific pristine
setup template. Raw logs and an incrementally updated, self-describing artifact
are retained below an explicitly claimed run directory.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Any


BACKENDS = ("rocksdb-fs", "slatedb-cached")
APIS = ("v1", "v2")
RUNTIMES = {"v1": "wasm-component-v1", "v2": "wasm-component-v2"}
FIXTURE_BYTES = 10_000_000
PROPERTY_COUNT = 220_000
EDIT_PROPERTY = "property_110000"
DEFAULT_BLOCKS = 12
DEFAULT_WARMUPS = 5
DEFAULT_SAMPLES = 20
DEFAULT_MEMORY_MIB = 256
DEFAULT_BOOTSTRAP_DRAWS = 10_000
DEFAULT_PROCESS_TIMEOUT_SECONDS = 1_800
MIN_ACCEPTANCE_BLOCKS = 12
MIN_ACCEPTANCE_WARMUPS = 5
ACCEPTANCE_SAMPLES = 20
MIN_ACCEPTANCE_DRAWS = 10_000
BOOTSTRAP_SEED = 0x4C49584A
MARKER = ".lix-json-v1-v2-paired-run"
MARKER_CONTENT = "lix-json-v1-v2-paired-run-v1\n"
ARTIFACT = "json-v1-v2-paired-raw.json"
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_V1_MANIFEST = REPO_ROOT / "plugins/json/manifest.json"
DEFAULT_V2_MANIFEST = REPO_ROOT / "plugins/json-v2/manifest.json"
THRESHOLDS = {
    "edit": {
        "p50": {"upper_ratio_strictly_below": 0.80},
        "p95": {"upper_ratio_strictly_below": 0.80},
    },
    "render": {
        "p50": {"upper_ratio_at_most": 1.05},
        "p95": {"upper_ratio_at_most": 1.10},
    },
}
REQUIRED_COUNTER_FIELDS = (
    "source_read_calls",
    "source_bytes_read",
    "component_boundary_bytes",
    "guest_linear_memory_high_water_bytes",
    "host_full_diff_bytes_compared",
    "host_full_content_classification_bytes",
    "full_state_semantic_rows_materialized",
    "change_payload_requests",
    "returned_change_payloads",
    "durable_semantic_changes",
    "private_document_cache_hits",
    "shared_renderer_cache_hits",
    "full_document_reparses",
    "full_renderer_invocations",
    "filesystem_sync_full_renders",
)


def environment(
    api: str,
    warmups: int,
    samples: int,
    memory_mib: int,
    *,
    splice_provenance: bool = False,
) -> dict[str, str]:
    """Return an exact profiling environment without inherited profile knobs."""

    if api not in APIS:
        raise ValueError(f"unsupported JSON API arm: {api}")
    result = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("LIX_PROFILE_")
    }
    result.update(
        {
            "LIX_PROFILE_FORMAT": "json",
            "LIX_PROFILE_JSON_API": api,
            "LIX_PROFILE_WARMUPS": str(warmups),
            "LIX_PROFILE_ROUNDS": str(samples),
            "LIX_PROFILE_WASM_MEMORY_MIB": str(memory_mib),
        }
    )
    if splice_provenance:
        result["LIX_PROFILE_SPLICE_PROVENANCE"] = "1"
    return result


def prepare_root(requested_root: Path) -> Path:
    """Claim or validate a dedicated run directory and return its real path."""

    if requested_root.is_symlink():
        raise ValueError("run directory must not be a symbolic link")
    root = requested_root.expanduser().resolve()
    if root == Path(root.anchor) or root == Path.home().resolve():
        raise ValueError("run directory must not be a filesystem or home root")
    if root.exists() and not root.is_dir():
        raise ValueError("run directory must be a directory")
    root.mkdir(parents=True, exist_ok=True)

    marker = root / MARKER
    entries = list(root.iterdir())
    if marker.exists():
        if not marker.is_file() or marker.is_symlink():
            raise ValueError("JSON paired-run marker must be a regular file")
        if marker.read_text(encoding="utf-8") != MARKER_CONTENT:
            raise ValueError("JSON paired-run marker has unexpected contents")
    elif entries:
        raise ValueError(
            "run directory must be new, empty, or carry the JSON paired-run marker"
        )
    else:
        marker.write_text(MARKER_CONTENT, encoding="utf-8")
    return root


def checked_child(root: Path, child: Path) -> Path:
    root = root.resolve()
    resolved = child.resolve()
    if resolved == root or root not in resolved.parents:
        raise ValueError(f"refusing to modify path outside run directory: {child}")
    return resolved


def reset_directory(root: Path, child: Path) -> Path:
    resolved = checked_child(root, child)
    if child.is_symlink():
        raise ValueError(f"run-owned path must not be a symbolic link: {child}")
    if child.exists():
        if not child.is_dir():
            raise ValueError(f"run-owned path must be a directory: {child}")
        shutil.rmtree(resolved)
    resolved.mkdir(parents=True)
    return resolved


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def file_fingerprint(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise ValueError(f"artifact does not exist: {resolved}")
    return {
        "path": str(resolved),
        "bytes": resolved.stat().st_size,
        "sha256": sha256_file(resolved),
    }


def load_manifest(path: Path, api: str) -> dict[str, Any]:
    fingerprint = file_fingerprint(path)
    raw = Path(fingerprint["path"]).read_text(encoding="utf-8")
    try:
        manifest = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError(f"{api} manifest is invalid JSON: {error}") from error
    if not isinstance(manifest, dict):
        raise ValueError(f"{api} manifest must be a JSON object")
    if manifest.get("runtime") != RUNTIMES[api]:
        raise ValueError(
            f"{api} manifest runtime must be {RUNTIMES[api]}, "
            f"got {manifest.get('runtime')!r}"
        )
    expected_api = "0.1.0" if api == "v1" else "2.0.0"
    if manifest.get("api_version") != expected_api:
        raise ValueError(
            f"{api} manifest api_version must be {expected_api}, "
            f"got {manifest.get('api_version')!r}"
        )
    fingerprint["manifest"] = manifest
    return fingerprint


def write_artifact(path: Path, artifact: dict[str, Any]) -> None:
    """Atomically replace the artifact so interrupted runs leave valid JSON."""

    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(artifact, indent=2, sort_keys=True) + "\n"
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_name = temporary.name
            temporary.write(encoded)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, path)
        temporary_name = None
    finally:
        if temporary_name is not None:
            Path(temporary_name).unlink(missing_ok=True)


def run_process(
    binary: Path,
    backend: str,
    mode: str,
    case_dir: Path,
    env: dict[str, str],
    log_path: Path,
    timeout_seconds: int,
) -> str:
    """Run one fresh benchmark process and retain combined raw output."""

    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with log_path.open("w", encoding="utf-8") as log:
            completed = subprocess.run(
                [str(binary), backend, mode, str(case_dir)],
                env=env,
                text=True,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=False,
                timeout=timeout_seconds,
            )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(
            f"{binary.name} {backend} {mode} exceeded {timeout_seconds}s; "
            f"inspect {log_path}"
        ) from error
    output = log_path.read_text(encoding="utf-8")
    if completed.returncode:
        raise RuntimeError(
            f"{binary.name} {backend} {mode} failed; inspect {log_path}"
        )
    return output


def parse_fields(line: str, prefix: str) -> dict[str, str]:
    if not line.startswith(prefix + " "):
        raise ValueError(f"expected line beginning with {prefix!r}")
    fields: dict[str, str] = {}
    for token in line[len(prefix) + 1 :].split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        if not key or key in fields:
            raise ValueError(f"{prefix} line contains an invalid duplicate field")
        fields[key] = value
    return fields


def unique_json_fields_line(output: str, prefix: str) -> dict[str, str]:
    """Return the unique structured JSON cell line for a benchmark phase.

    The profile executable also emits human-readable lines such as
    ``setup insert took ...`` and ``edit took ...``. Selecting the line by its
    ``format=json`` field keeps those diagnostics in the raw log without
    confusing contract validation.
    """

    candidates = []
    for line in output.splitlines():
        if not line.startswith(prefix + " "):
            continue
        fields = parse_fields(line, prefix)
        if fields.get("format") == "json":
            candidates.append(fields)
    if len(candidates) != 1:
        raise ValueError(
            f"benchmark output must contain exactly one structured JSON {prefix} line"
        )
    return candidates[0]


def validate_setup(output: str, api: str) -> dict[str, Any]:
    fields = unique_json_fields_line(output, "setup")
    expected = {
        "format": "json",
        "runtime": RUNTIMES[api],
        "properties": str(PROPERTY_COUNT),
        "bytes": str(FIXTURE_BYTES),
        "edit_property": EDIT_PROPERTY,
    }
    mismatches = {
        key: (fields.get(key), value)
        for key, value in expected.items()
        if fields.get(key) != value
    }
    if mismatches:
        raise ValueError(f"{api} setup did not report the exact JSON fixture: {mismatches}")
    archive_hash = fields.get("plugin_archive_sha256")
    if (
        archive_hash is None
        or len(archive_hash) != 64
        or any(character not in "0123456789abcdef" for character in archive_hash)
    ):
        raise ValueError(f"{api} setup did not report a lowercase SHA-256 archive hash")
    try:
        archive_bytes = int(fields.get("plugin_archive_bytes", ""))
    except ValueError as error:
        raise ValueError(f"{api} setup plugin_archive_bytes must be an integer") from error
    if archive_bytes <= 0:
        raise ValueError(f"{api} setup plugin archive must be nonempty")
    return {"bytes": archive_bytes, "sha256": archive_hash}


def validate_mode(
    output: str, mode: str, warmups: int, samples: int
) -> None:
    if mode not in ("edit", "render"):
        raise ValueError(f"unsupported JSON paired mode: {mode}")
    fields = unique_json_fields_line(output, mode)
    expected = {
        "format": "json",
        "properties": str(PROPERTY_COUNT),
        "warmups": str(warmups),
        "rounds": str(samples),
    }
    if mode == "edit":
        expected.update(
            {
                "bytes": str(FIXTURE_BYTES),
                "property": EDIT_PROPERTY,
            }
        )
    mismatches = {
        key: (fields.get(key), value)
        for key, value in expected.items()
        if fields.get(key) != value
    }
    if mismatches:
        raise ValueError(f"{mode} did not report the configured JSON cell: {mismatches}")


def measured_samples(output: str, label: str, expected: int) -> list[float]:
    pattern = re.compile(rf"^{re.escape(label)} sample_ms=(\[.*\])$", re.MULTILINE)
    matches = pattern.findall(output)
    if len(matches) != 1:
        raise ValueError(
            f"benchmark output must contain exactly one {label} sample_ms line"
        )
    try:
        raw_samples = ast.literal_eval(matches[0])
    except (SyntaxError, ValueError) as error:
        raise ValueError(f"{label} sample_ms is not a numeric array") from error
    if not isinstance(raw_samples, list) or len(raw_samples) != expected:
        received = len(raw_samples) if isinstance(raw_samples, list) else "non-array"
        raise ValueError(
            f"expected exactly {expected} measured {label} samples, received {received}"
        )
    result = []
    for value in raw_samples:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{label} samples must be numeric milliseconds")
        sample = float(value)
        if not math.isfinite(sample) or sample <= 0:
            raise ValueError(f"{label} samples must be finite positive milliseconds")
        result.append(sample)
    return result


def parse_v2_counters(output: str, expected: int) -> list[dict[str, int | str]]:
    rows: list[dict[str, int | str]] = []
    for line in output.splitlines():
        if not line.startswith("plugin_v2_counters "):
            continue
        fields = parse_fields(line, "plugin_v2_counters")
        if fields.get("label") != "edit":
            raise ValueError("v2 counter line must have label=edit")
        parsed: dict[str, int | str] = {"label": "edit"}
        for key, value in fields.items():
            if key == "label":
                continue
            try:
                parsed[key] = int(value)
            except ValueError as error:
                raise ValueError(f"v2 counter {key} must be an integer") from error
        missing = [key for key in ("round", *REQUIRED_COUNTER_FIELDS) if key not in parsed]
        if missing:
            raise ValueError(f"v2 counter line is missing fields: {', '.join(missing)}")
        rows.append(parsed)
    if len(rows) != expected:
        raise ValueError(
            f"expected exactly {expected} v2 edit counter rows, received {len(rows)}"
        )
    rows.sort(key=lambda row: int(row["round"]))
    if [row["round"] for row in rows] != list(range(expected)):
        raise ValueError("v2 edit counter rounds must be unique and contiguous from zero")
    return rows


def relative_log_record(root: Path, log: Path) -> dict[str, Any]:
    return {
        "path": str(log.relative_to(root)),
        "bytes": log.stat().st_size,
        "sha256": sha256_file(log),
    }


def is_acceptance_design(
    blocks: int, warmups: int, samples: int, bootstrap_draws: int
) -> bool:
    return (
        blocks >= MIN_ACCEPTANCE_BLOCKS
        and blocks % 2 == 0
        and warmups >= MIN_ACCEPTANCE_WARMUPS
        and samples == ACCEPTANCE_SAMPLES
        and bootstrap_draws >= MIN_ACCEPTANCE_DRAWS
    )


def validate_configuration(
    blocks: int,
    warmups: int,
    samples: int,
    memory_mib: int,
    bootstrap_draws: int,
    timeout_seconds: int,
) -> None:
    if blocks < 2 or blocks % 2:
        raise ValueError("blocks must be an even integer of at least two")
    if warmups < 0:
        raise ValueError("warmups must be non-negative")
    if samples < 1:
        raise ValueError("samples must be positive")
    if memory_mib < 1:
        raise ValueError("memory MiB must be positive")
    if bootstrap_draws < 100:
        raise ValueError("bootstrap draws must be at least 100")
    if timeout_seconds < 1:
        raise ValueError("process timeout must be positive")


def run_campaign(
    benchmark: Path,
    requested_root: Path,
    *,
    output: Path | None = None,
    v1_manifest: Path = DEFAULT_V1_MANIFEST,
    v2_manifest: Path = DEFAULT_V2_MANIFEST,
    blocks: int = DEFAULT_BLOCKS,
    warmups: int = DEFAULT_WARMUPS,
    samples: int = DEFAULT_SAMPLES,
    memory_mib: int = DEFAULT_MEMORY_MIB,
    bootstrap_draws: int = DEFAULT_BOOTSTRAP_DRAWS,
    timeout_seconds: int = DEFAULT_PROCESS_TIMEOUT_SECONDS,
) -> Path:
    validate_configuration(
        blocks, warmups, samples, memory_mib, bootstrap_draws, timeout_seconds
    )
    benchmark = benchmark.expanduser().resolve()
    if not benchmark.is_file():
        raise ValueError(f"benchmark executable does not exist: {benchmark}")
    if not os.access(benchmark, os.X_OK):
        raise ValueError(f"benchmark executable is not executable: {benchmark}")

    root = prepare_root(requested_root)
    templates_root = reset_directory(root, root / "templates")
    cases_root = reset_directory(root, root / "cases")
    logs_root = reset_directory(root, root / "logs")
    artifact_path = (
        checked_child(root, root / ARTIFACT)
        if output is None
        else output.expanduser().resolve()
    )
    acceptance = is_acceptance_design(blocks, warmups, samples, bootstrap_draws)
    artifact: dict[str, Any] = {
        "schema_version": 1,
        "status": "running",
        "classification": (
            "JSON v1-v2 paired acceptance campaign"
            if acceptance
            else "JSON v1-v2 paired smoke/diagnostic; not acceptance evidence"
        ),
        "design": {
            "format": "json",
            "apis": list(APIS),
            "same_benchmark_executable": True,
            "fixture_bytes": FIXTURE_BYTES,
            "properties": PROPERTY_COUNT,
            "edit_property": EDIT_PROPERTY,
            "edit": "one byte, alternating original/edited property value",
            "blocks": blocks,
            "warmups_per_arm_block": warmups,
            "measured_per_arm_block": samples,
            "wasm_memory_mib": memory_mib,
            "production_default_wasm_memory_mib": 64,
            "backends": list(BACKENDS),
            "order": "exactly counterbalanced alternating blocks",
            "isolation": (
                "API-specific pristine setup templates; fresh process and fresh "
                "template copy for every arm, block, and metric"
            ),
            "edit_path": {
                "v1": "ordinary SQL blob write",
                "v2": (
                    "same SQL blob write plus validated splice provenance; "
                    "hot plugin receives one inline splice"
                ),
            },
            "acceptance_eligible": acceptance,
            "minimum_acceptance_design": {
                "blocks": MIN_ACCEPTANCE_BLOCKS,
                "warmups_per_arm_block": MIN_ACCEPTANCE_WARMUPS,
                "measured_per_arm_block": ACCEPTANCE_SAMPLES,
                "bootstrap_draws": MIN_ACCEPTANCE_DRAWS,
            },
            "analysis": {
                "method": (
                    "paired hierarchical cluster bootstrap of pooled v2/v1 "
                    "p50 and p95 ratios"
                ),
                "seed": f"0x{BOOTSTRAP_SEED:08x}",
                "draws": bootstrap_draws,
                "thresholds_preregistered_before_samples": THRESHOLDS,
            },
        },
        "environment": {
            "platform": platform.platform(),
            "python": sys.version,
            "logical_cpus": os.cpu_count(),
            "process_timeout_seconds": timeout_seconds,
        },
        "benchmark": file_fingerprint(benchmark),
        "runner": file_fingerprint(Path(__file__)),
        "plugins": {
            "v1": load_manifest(v1_manifest, "v1"),
            "v2": load_manifest(v2_manifest, "v2"),
        },
        "backends": {
            backend: {"status": "pending", "setup": {}, "edit": [], "render": []}
            for backend in BACKENDS
        },
    }
    write_artifact(artifact_path, artifact)

    observed_plugin_archives: dict[str, dict[str, Any]] = {}
    for backend in BACKENDS:
        backend_result = artifact["backends"][backend]
        backend_result["status"] = "running"
        templates: dict[str, Path] = {}
        for api in APIS:
            template = templates_root / backend / api
            template.mkdir(parents=True)
            setup_log = logs_root / backend / f"setup-{api}.log"
            setup_output = run_process(
                benchmark,
                backend,
                "setup",
                template,
                environment(api, warmups, samples, memory_mib),
                setup_log,
                timeout_seconds,
            )
            archive = validate_setup(setup_output, api)
            previous_archive = observed_plugin_archives.setdefault(api, archive)
            if archive != previous_archive:
                raise ValueError(
                    f"{api} setup observed different plugin archives across backends"
                )
            artifact["plugins"][api]["archive_observed_at_setup"] = archive
            backend_result["setup"][api] = {
                "runtime": RUNTIMES[api],
                "plugin_archive": archive,
                "log": relative_log_record(root, setup_log),
            }
            templates[api] = template
            write_artifact(artifact_path, artifact)

        for block_index in range(blocks):
            order = APIS if block_index % 2 == 0 else tuple(reversed(APIS))
            for mode in ("edit", "render"):
                block: dict[str, Any] = {
                    "index": block_index,
                    "order": "-".join(order),
                    "arms": {},
                }
                for api in order:
                    case = cases_root / backend / mode / f"block-{block_index:02d}-{api}"
                    case.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copytree(templates[api], case)
                    log = (
                        logs_root
                        / backend
                        / mode
                        / f"block-{block_index:02d}-{api}.log"
                    )
                    env = environment(
                        api,
                        warmups,
                        samples,
                        memory_mib,
                        splice_provenance=(mode == "edit" and api == "v2"),
                    )
                    process_output = run_process(
                        benchmark,
                        backend,
                        mode,
                        case,
                        env,
                        log,
                        timeout_seconds,
                    )
                    validate_mode(process_output, mode, warmups, samples)
                    counter_rows = (
                        parse_v2_counters(process_output, samples)
                        if mode == "edit" and api == "v2"
                        else []
                    )
                    if api == "v1" and "plugin_v2_counters " in process_output:
                        raise ValueError("v1 benchmark arm unexpectedly emitted v2 counters")
                    block["arms"][api] = {
                        "sample_ms": measured_samples(process_output, mode, samples),
                        "counters": counter_rows,
                        "log": relative_log_record(root, log),
                    }
                    shutil.rmtree(checked_child(root, case))
                backend_result[mode].append(block)
            write_artifact(artifact_path, artifact)
            print(
                f"completed JSON {backend} block {block_index + 1}/{blocks}",
                flush=True,
            )

        backend_result["status"] = "complete"
        write_artifact(artifact_path, artifact)

    artifact["status"] = "complete"
    write_artifact(artifact_path, artifact)
    return artifact_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run the same profile executable as counterbalanced JSON v1 and v2 arms."
        )
    )
    parser.add_argument("--benchmark", required=True, type=Path)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--v1-manifest", type=Path, default=DEFAULT_V1_MANIFEST)
    parser.add_argument("--v2-manifest", type=Path, default=DEFAULT_V2_MANIFEST)
    parser.add_argument("--blocks", type=int, default=DEFAULT_BLOCKS)
    parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUPS)
    parser.add_argument("--samples", type=int, default=DEFAULT_SAMPLES)
    parser.add_argument("--memory-mib", type=int, default=DEFAULT_MEMORY_MIB)
    parser.add_argument(
        "--bootstrap-draws", type=int, default=DEFAULT_BOOTSTRAP_DRAWS
    )
    parser.add_argument(
        "--process-timeout-seconds",
        type=int,
        default=DEFAULT_PROCESS_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--fast-smoke",
        action="store_true",
        help="use 2 blocks, 1 warmup, 2 samples, and 200 bootstrap draws",
    )
    args = parser.parse_args()
    if args.fast_smoke:
        args.blocks = 2
        args.warmups = 1
        args.samples = 2
        args.bootstrap_draws = 200
    artifact = run_campaign(
        args.benchmark,
        args.run_dir,
        output=args.output,
        v1_manifest=args.v1_manifest,
        v2_manifest=args.v2_manifest,
        blocks=args.blocks,
        warmups=args.warmups,
        samples=args.samples,
        memory_mib=args.memory_mib,
        bootstrap_draws=args.bootstrap_draws,
        timeout_seconds=args.process_timeout_seconds,
    )
    print(f"wrote {artifact}")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"JSON paired benchmark failed: {error}", file=sys.stderr)
        raise
