#!/usr/bin/env python3
"""Run counterbalanced fresh-process PR2 benchmark blocks.

The two binaries are built from the immutable control and candidate worktrees.
Each arm starts from its own setup template, performs five unreported warmups
and twenty measured alternating middle-row edits in one fresh process, and is
then discarded. Raw process logs and an incrementally written JSON artifact are
kept below the explicitly supplied run directory.
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
from typing import Any

BACKENDS = ("rocksdb-fs", "slatedb-cached")
WARMUPS = 5
MEASURED = 20
MARKER = ".lix-pr2-paired-run"


def environment(rows: int, memory_mib: int) -> dict[str, str]:
    # A caller's profiling knobs must not silently change the preregistered
    # cells. In particular, provenance belongs only to the candidate edit arm
    # and logical-I/O instrumentation is a separate run.
    result = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("LIX_PROFILE_")
    }
    result.update(
        {
            "LIX_PROFILE_FORMAT": "csv",
            "LIX_PROFILE_INITIAL_ROWS": str(rows),
            "LIX_PROFILE_NEW_ROWS": "1",
            "LIX_PROFILE_WARMUPS": str(WARMUPS),
            "LIX_PROFILE_ROUNDS": str(MEASURED),
            "LIX_PROFILE_WASM_MEMORY_MIB": str(memory_mib),
        }
    )
    return result


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
    completed = subprocess.run(
        [str(binary), backend, mode, str(case_dir)],
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(completed.stdout, encoding="utf-8")
    if completed.returncode:
        raise RuntimeError(
            f"{binary.name} {backend} {mode} failed; inspect {log_path}"
        )
    return completed.stdout


def measured_samples(output: str, label: str) -> list[float]:
    pattern = re.compile(rf"^{re.escape(label)} sample_ms=(\[.*\])$", re.MULTILINE)
    match = pattern.search(output)
    if not match:
        raise ValueError(f"benchmark output did not contain {label} sample_ms")
    samples = ast.literal_eval(match.group(1))
    if len(samples) != MEASURED:
        raise ValueError(
            f"expected {MEASURED} measured samples, received {len(samples)}"
        )
    result = [float(value) for value in samples]
    if any(not math.isfinite(value) or value <= 0 for value in result):
        raise ValueError(f"{label} samples must be finite positive milliseconds")
    return result


def write_artifact(path: Path, artifact: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def prepare_root(root: Path) -> None:
    root = root.resolve()
    if root == Path(root.anchor) or root == Path.home():
        raise ValueError("run directory must not be a filesystem or home root")
    if root.exists():
        if not root.is_dir():
            raise ValueError("run directory must be a directory")
        if any(root.iterdir()) and not (root / MARKER).is_file():
            raise ValueError(
                "run directory must be new, empty, or carry the paired-run marker"
            )
    else:
        root.mkdir(parents=True)
    (root / MARKER).touch(exist_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--blocks", type=int, default=12)
    parser.add_argument("--rows", type=int, default=220_000)
    parser.add_argument("--memory-mib", type=int, default=256)
    args = parser.parse_args()
    if args.blocks < 12 or args.blocks % 2:
        parser.error("--blocks must be an even integer of at least 12")
    for binary in (args.baseline, args.candidate):
        if not binary.is_file():
            parser.error(f"benchmark binary does not exist: {binary}")
    prepare_root(args.run_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    env = environment(args.rows, args.memory_mib)
    artifact: dict[str, Any] = {
        "design": {
            "format": "csv",
            "blocks": args.blocks,
            "warmups_per_arm_block": WARMUPS,
            "measured_per_arm_block": MEASURED,
            "rows": args.rows,
            "diagnostic_memory_mib": args.memory_mib,
            "order": "exactly counterbalanced alternating blocks",
            "edit_path": {
                "baseline": "ordinary SQL blob write (v1 has no splice consumer)",
                "candidate": "ordinary SQL blob write plus validated remote-protocol splice sidecar",
            },
        },
        "binaries": {
            "baseline": {
                "path": str(args.baseline.resolve()),
                "sha256": binary_sha256(args.baseline),
            },
            "candidate": {
                "path": str(args.candidate.resolve()),
                "sha256": binary_sha256(args.candidate),
            },
        },
        "backends": {
            backend: {"edit": [], "render": []} for backend in BACKENDS
        },
    }
    binaries = {"baseline": args.baseline, "candidate": args.candidate}

    for backend in BACKENDS:
        templates: dict[str, Path] = {}
        for arm, binary in binaries.items():
            template = args.run_dir / "templates" / backend / arm
            if template.exists():
                shutil.rmtree(template)
            template.mkdir(parents=True)
            run_process(
                binary,
                backend,
                "setup",
                template,
                env,
                args.run_dir / "logs" / backend / f"setup-{arm}.log",
            )
            templates[arm] = template

        for block_index in range(args.blocks):
            order = (
                ("baseline", "candidate")
                if block_index % 2 == 0
                else ("candidate", "baseline")
            )
            edit_block: dict[str, Any] = {"order": "-".join(order)}
            render_block: dict[str, Any] = {"order": "-".join(order)}
            for arm in order:
                case = args.run_dir / "active-case"
                if case.exists():
                    shutil.rmtree(case)
                shutil.copytree(templates[arm], case)
                edit_env = env.copy()
                if arm == "candidate":
                    edit_env["LIX_PROFILE_SPLICE_PROVENANCE"] = "1"
                output = run_process(
                    binaries[arm],
                    backend,
                    "edit",
                    case,
                    edit_env,
                    args.run_dir
                    / "logs"
                    / backend
                    / f"block-{block_index:02d}-{arm}.log",
                )
                edit_block[arm] = measured_samples(output, "edit")
                shutil.rmtree(case)

                # Exact-render is a separately guarded cell. Give it another
                # fresh process and pristine copy of the same setup template;
                # otherwise the preceding update would make its cache/storage
                # history arm-dependent.
                shutil.copytree(templates[arm], case)
                output = run_process(
                    binaries[arm],
                    backend,
                    "render",
                    case,
                    env,
                    args.run_dir
                    / "logs"
                    / backend
                    / f"block-{block_index:02d}-{arm}-render.log",
                )
                render_block[arm] = measured_samples(output, "render")
                shutil.rmtree(case)
            artifact["backends"][backend]["edit"].append(edit_block)
            artifact["backends"][backend]["render"].append(render_block)
            write_artifact(args.output, artifact)
            print(f"completed {backend} block {block_index + 1}/{args.blocks}", flush=True)

    write_artifact(args.output, artifact)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # keep profiling failures visible in shell logs
        print(f"paired benchmark failed: {error}", file=sys.stderr)
        raise
