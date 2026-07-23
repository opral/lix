#!/usr/bin/env python3
"""Run the candidate's large-JSON mechanism diagnostic.

This is deliberately not a PR2 acceptance runner.  The repository's JSON
plugin still uses the Component Model v1 API, so these samples only exercise
the current full-engine mechanism on the candidate binary.  Each backend gets
one pristine setup template and separate fresh-process copies for localized
edit and exact-render measurements.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Any


BACKENDS = ("rocksdb-fs", "slatedb-cached")
WARMUPS = 5
MEASURED = 20
MEMORY_MIB = 256
FIXTURE_BYTES = 10_000_000
PROPERTY_COUNT = 220_000
EDIT_PROPERTY = "property_110000"
MARKER = ".lix-pr2-json-diagnostic-run"
MARKER_CONTENT = "lix-pr2-json-diagnostic-run-v1\n"
ARTIFACT = "json-diagnostic.json"


def environment() -> dict[str, str]:
    """Return an exact profiling environment without inherited profile knobs."""

    result = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("LIX_PROFILE_")
    }
    result.update(
        {
            "LIX_PROFILE_FORMAT": "json",
            "LIX_PROFILE_WARMUPS": str(WARMUPS),
            "LIX_PROFILE_ROUNDS": str(MEASURED),
            "LIX_PROFILE_WASM_MEMORY_MIB": str(MEMORY_MIB),
        }
    )
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
            raise ValueError("JSON diagnostic run marker must be a regular file")
        if marker.read_text(encoding="utf-8") != MARKER_CONTENT:
            raise ValueError("JSON diagnostic run marker has unexpected contents")
    elif entries:
        raise ValueError(
            "run directory must be new, empty, or carry the JSON diagnostic marker"
        )
    else:
        marker.write_text(MARKER_CONTENT, encoding="utf-8")
    return root


def checked_child(root: Path, child: Path) -> Path:
    """Resolve a child without allowing a symlink to escape the claimed root."""

    root = root.resolve()
    resolved = child.resolve()
    if resolved == root or root not in resolved.parents:
        raise ValueError(f"refusing to modify path outside run directory: {child}")
    return resolved


def reset_directory(root: Path, child: Path) -> Path:
    """Replace one known run-owned directory after containment validation."""

    resolved = checked_child(root, child)
    if child.is_symlink():
        raise ValueError(f"run-owned path must not be a symbolic link: {child}")
    if child.exists():
        if not child.is_dir():
            raise ValueError(f"run-owned path must be a directory: {child}")
        shutil.rmtree(resolved)
    resolved.mkdir(parents=True)
    return resolved


def binary_sha256(binary: Path) -> str:
    digest = hashlib.sha256()
    with binary.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def run_process(
    binary: Path,
    backend: str,
    mode: str,
    case_dir: Path,
    env: dict[str, str],
    log_path: Path,
) -> str:
    """Run one real profile process and retain its combined raw output."""

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log:
        completed = subprocess.run(
            [str(binary), backend, mode, str(case_dir)],
            env=env,
            text=True,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
        )
    output = log_path.read_text(encoding="utf-8")
    if completed.returncode:
        raise RuntimeError(
            f"{binary.name} {backend} {mode} failed; inspect {log_path}"
        )
    return output


def validate_setup(output: str) -> None:
    expected = (
        "setup format=json runtime=wasm-component-v1 mechanism_only=true "
        f"properties={PROPERTY_COUNT} bytes={FIXTURE_BYTES} "
        f"edit_property={EDIT_PROPERTY}"
    )
    if len(re.findall(rf"^{re.escape(expected)}$", output, re.MULTILINE)) != 1:
        raise ValueError(
            "setup did not report the exact 10,000,000-byte, 220,000-property "
            "Component v1 JSON fixture"
        )


def validate_mode(output: str, mode: str) -> None:
    if mode == "edit":
        expected = (
            f"edit format=json properties={PROPERTY_COUNT} bytes={FIXTURE_BYTES} "
            f"property={EDIT_PROPERTY} warmups={WARMUPS} rounds={MEASURED}"
        )
    elif mode == "render":
        expected = (
            f"render format=json properties={PROPERTY_COUNT} "
            f"warmups={WARMUPS} rounds={MEASURED}"
        )
    else:
        raise ValueError(f"unsupported JSON diagnostic mode: {mode}")
    if len(re.findall(rf"^{re.escape(expected)}$", output, re.MULTILINE)) != 1:
        raise ValueError(
            f"{mode} did not report the required JSON fixture and 5/20 design"
        )


def measured_samples(output: str, label: str) -> list[float]:
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
    if not isinstance(raw_samples, list) or len(raw_samples) != MEASURED:
        received = len(raw_samples) if isinstance(raw_samples, list) else "non-array"
        raise ValueError(
            f"expected exactly {MEASURED} measured {label} samples, received {received}"
        )
    result: list[float] = []
    for value in raw_samples:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{label} samples must be numeric milliseconds")
        sample = float(value)
        if not math.isfinite(sample) or sample <= 0:
            raise ValueError(f"{label} samples must be finite positive milliseconds")
        result.append(sample)
    return result


def summarize(samples: list[float]) -> dict[str, float | int]:
    ordered = sorted(samples)
    return {
        "samples": len(ordered),
        "p50_ms": ordered[math.ceil(len(ordered) * 0.50) - 1],
        "p95_ms": ordered[math.ceil(len(ordered) * 0.95) - 1],
        "min_ms": ordered[0],
        "max_ms": ordered[-1],
    }


def write_artifact(path: Path, artifact: dict[str, Any]) -> None:
    """Durably replace the result in-place so partial JSON is never exposed."""

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


def relative_log(root: Path, log: Path) -> str:
    return str(log.relative_to(root))


def run_diagnostic(candidate: Path, requested_root: Path) -> Path:
    candidate = candidate.expanduser().resolve()
    if not candidate.is_file():
        raise ValueError(f"candidate benchmark binary does not exist: {candidate}")
    if not os.access(candidate, os.X_OK):
        raise ValueError(f"candidate benchmark binary is not executable: {candidate}")

    root = prepare_root(requested_root)
    templates_root = reset_directory(root, root / "templates")
    cases_root = reset_directory(root, root / "cases")
    logs_root = reset_directory(root, root / "logs")
    artifact_path = checked_child(root, root / ARTIFACT)
    env = environment()
    artifact: dict[str, Any] = {
        "status": "running",
        "classification": "candidate-only mechanism diagnostic; not PR2 v2 acceptance",
        "design": {
            "format": "json",
            "runtime": "wasm-component-v1",
            "mechanism_only": True,
            "pr2_v2_acceptance": False,
            "fixture_bytes": FIXTURE_BYTES,
            "properties": PROPERTY_COUNT,
            "edit_property": EDIT_PROPERTY,
            "edit": "one byte, alternating original/edited property value",
            "warmups_per_process": WARMUPS,
            "measured_per_process": MEASURED,
            "diagnostic_wasm_memory_mib": MEMORY_MIB,
            "production_default_wasm_memory_mib": 64,
            "backends": list(BACKENDS),
            "isolation": "pristine setup-template copy and fresh process per measured mode",
        },
        "candidate": {
            "path": str(candidate),
            "sha256": binary_sha256(candidate),
        },
        "backends": {backend: {"status": "pending"} for backend in BACKENDS},
    }
    write_artifact(artifact_path, artifact)

    for backend in BACKENDS:
        backend_result: dict[str, Any] = {"status": "running"}
        artifact["backends"][backend] = backend_result
        write_artifact(artifact_path, artifact)

        template = templates_root / backend
        template.mkdir(parents=True)
        setup_log = logs_root / backend / "setup.log"
        setup_output = run_process(
            candidate, backend, "setup", template, env, setup_log
        )
        validate_setup(setup_output)
        backend_result["setup_log"] = relative_log(root, setup_log)
        write_artifact(artifact_path, artifact)

        for mode in ("edit", "render"):
            case = cases_root / backend / mode
            case.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(template, case)
            log = logs_root / backend / f"{mode}.log"
            output = run_process(candidate, backend, mode, case, env, log)
            validate_mode(output, mode)
            samples = measured_samples(output, mode)
            backend_result[mode] = {
                "log": relative_log(root, log),
                "sample_ms": samples,
                "summary": summarize(samples),
            }
            shutil.rmtree(checked_child(root, case))
            write_artifact(artifact_path, artifact)

        backend_result["status"] = "complete"
        write_artifact(artifact_path, artifact)
        print(f"completed JSON mechanism diagnostic for {backend}", flush=True)

    artifact["status"] = "complete"
    write_artifact(artifact_path, artifact)
    return artifact_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run candidate-only 10 MB JSON v1 mechanism diagnostics; this is "
            "not the PR2 v2 acceptance gate."
        )
    )
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--run-dir", required=True, type=Path)
    args = parser.parse_args()
    artifact = run_diagnostic(args.candidate, args.run_dir)
    print(f"wrote {artifact}")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # preserve the traceback and point at retained logs
        print(f"JSON diagnostic failed: {error}", file=sys.stderr)
        raise
