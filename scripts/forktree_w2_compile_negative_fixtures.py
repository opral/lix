#!/usr/bin/env python3
"""Compile the typed-boundary probes and require the intended diagnostics."""

from __future__ import annotations

import argparse
import subprocess
import tempfile
from pathlib import Path


def compile_one(source: Path, expect_success: bool) -> tuple[int, str]:
    with tempfile.TemporaryDirectory(prefix="w2-fixture-") as directory:
        output = Path(directory) / "fixture"
        result = subprocess.run(
            [
                "rustc",
                "--edition=2021",
                "-D",
                "warnings",
                "--crate-type",
                "bin",
                str(source),
                "-o",
                str(output),
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        normalized = result.stdout.replace(str(directory), "<tmp>")
        if expect_success and result.returncode != 0:
            raise SystemExit(f"COMPILE-RED positive {source}:\n{normalized}")
        if not expect_success:
            if result.returncode == 0:
                raise SystemExit(f"COMPILE-RED negative unexpectedly compiled: {source}")
            if "mismatched types" not in normalized:
                raise SystemExit(
                    f"COMPILE-RED negative missing intended diagnostic {source}:\n{normalized}"
                )
        return result.returncode, normalized


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    root = args.root.resolve()
    positive = root / "scripts/forktree_w2_compile_fixtures/positive_typed_boundaries.rs"
    negatives = sorted((root / "scripts/forktree_w2_compile_fixtures").glob("negative_*.rs"))
    compile_one(positive, True)
    diagnostics = []
    for source in negatives:
        _, diagnostic = compile_one(source, False)
        diagnostics.append((source.name, diagnostic.count("mismatched types")))
    print(f"COMPILE FIXTURES GREEN positive=1 negatives={len(negatives)} diagnostics={diagnostics}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
