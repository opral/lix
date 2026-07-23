#!/usr/bin/env python3
"""Evaluate PR2's preregistered paired hierarchical bootstrap gate.

Input is JSON with `backends.<name>` arrays. Every array element is one
fresh-process block with equally configured `baseline` and `candidate` sample
arrays. The runner deliberately stays dependency-free so the raw artifact can
be checked on CI and on profiling hosts without installing scipy/numpy.
"""

from __future__ import annotations

import argparse
import json
import math
import random
from pathlib import Path
from typing import Any

SEED = 0x4C495832
DRAWS = 10_000
REQUIRED_BACKENDS = ("rocksdb-fs", "slatedb-cached")
REQUIRED_METRICS = ("edit", "render")
MIN_BLOCKS = 12
MIN_WARMUPS = 5
MEASURED = 20
VALID_ORDERS = ("baseline-candidate", "candidate-baseline")


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        raise ValueError("a measured arm must contain at least one sample")
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return ordered[index]


def pooled_ratio(blocks: list[dict[str, Any]], fraction: float) -> float:
    """Return the candidate/control ratio after pooling all paired blocks."""

    baseline = [sample for block in blocks for sample in block["baseline"]]
    candidate = [sample for block in blocks for sample in block["candidate"]]
    return percentile(candidate, fraction) / percentile(baseline, fraction)


def bootstrap_upper(
    blocks: list[dict[str, Any]], fraction: float, rng: random.Random
) -> float:
    log_estimates: list[float] = []
    for _ in range(DRAWS):
        baseline_pool: list[float] = []
        candidate_pool: list[float] = []
        for _ in range(len(blocks)):
            block = blocks[rng.randrange(len(blocks))]
            baseline_raw = block["baseline"]
            candidate_raw = block["candidate"]
            baseline_pool.extend(
                baseline_raw[rng.randrange(len(baseline_raw))]
                for _ in range(len(baseline_raw))
            )
            candidate_pool.extend(
                candidate_raw[rng.randrange(len(candidate_raw))]
                for _ in range(len(candidate_raw))
            )
        log_estimates.append(
            math.log(
                percentile(candidate_pool, fraction)
                / percentile(baseline_pool, fraction)
            )
        )
    return math.exp(percentile(log_estimates, 0.95))


def validate_blocks(name: str, blocks: list[dict[str, Any]]) -> None:
    if not isinstance(blocks, list):
        raise ValueError(f"{name}: paired blocks must be an array")
    if len(blocks) < MIN_BLOCKS or len(blocks) % 2:
        raise ValueError(f"{name}: expected an even count of at least 12 paired blocks")
    orders = [block.get("order") for block in blocks]
    invalid_orders = [order for order in orders if order not in VALID_ORDERS]
    if invalid_orders:
        raise ValueError(
            f"{name}: every block order must be baseline-candidate or candidate-baseline"
        )
    baseline_first = orders.count("baseline-candidate")
    candidate_first = orders.count("candidate-baseline")
    if baseline_first != candidate_first:
        raise ValueError(f"{name}: block order is not exactly counterbalanced")
    for index, block in enumerate(blocks):
        for arm in ("baseline", "candidate"):
            samples = block.get(arm)
            if not isinstance(samples, list) or len(samples) != MEASURED:
                raise ValueError(
                    f"{name} block {index} {arm}: expected exactly {MEASURED} measured samples"
                )
            for sample_index, sample in enumerate(samples):
                if (
                    isinstance(sample, bool)
                    or not isinstance(sample, (int, float))
                    or not math.isfinite(sample)
                    or sample <= 0
                ):
                    raise ValueError(
                        f"{name} block {index} {arm} sample {sample_index}: "
                        "latency must be a finite positive number"
                    )


def validate_design(input_data: dict[str, Any]) -> int:
    design = input_data.get("design")
    if not isinstance(design, dict):
        raise ValueError("input must contain paired-run design metadata")
    if design.get("format") != "csv":
        raise ValueError("the PR2 acceptance gate requires the CSV fixture")
    blocks = design.get("blocks")
    if isinstance(blocks, bool) or not isinstance(blocks, int):
        raise ValueError("design.blocks must be an integer")
    if blocks < MIN_BLOCKS or blocks % 2:
        raise ValueError("design.blocks must be an even integer of at least 12")
    warmups = design.get("warmups_per_arm_block")
    if isinstance(warmups, bool) or not isinstance(warmups, int) or warmups < MIN_WARMUPS:
        raise ValueError("the acceptance design requires at least five warmups per arm/block")
    if design.get("measured_per_arm_block") != MEASURED:
        raise ValueError("the acceptance design requires exactly 20 measured samples per arm/block")
    return blocks


def evaluate(input_data: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(input_data, dict):
        raise ValueError("input must be a JSON object")
    designed_blocks = validate_design(input_data)
    backends = input_data.get("backends")
    if not isinstance(backends, dict):
        raise ValueError("input must contain a backends object")
    if set(backends) != set(REQUIRED_BACKENDS):
        raise ValueError(
            "acceptance input must contain exactly rocksdb-fs and slatedb-cached"
        )

    output: dict[str, Any] = {
        "seed": f"0x{SEED:08x}",
        "draws": DRAWS,
        "method": "paired hierarchical cluster bootstrap of log p50/p95 ratios",
        "backends": {},
    }
    for backend in REQUIRED_BACKENDS:
        metrics = backends[backend]
        if not isinstance(metrics, dict):
            raise ValueError(f"{backend}: metrics must be an object")
        missing_metrics = [metric for metric in REQUIRED_METRICS if metric not in metrics]
        if missing_metrics:
            raise ValueError(
                f"{backend}: missing required cells: {', '.join(missing_metrics)}"
            )
        backend_output: dict[str, Any] = {}
        for metric, limits in (
            ("edit", {"p50": (0.80, True), "p95": (0.80, True)}),
            ("render", {"p50": (1.05, False), "p95": (1.10, False)}),
        ):
            blocks = metrics[metric]
            validate_blocks(f"{backend}/{metric}", blocks)
            if len(blocks) != designed_blocks:
                raise ValueError(
                    f"{backend}/{metric}: block count does not match design.blocks"
                )
            cells = {}
            for label, fraction in (("p50", 0.50), ("p95", 0.95)):
                # The preregistered analysis fixes every cell's stream at this
                # exact seed so results do not depend on iteration order.
                upper = bootstrap_upper(blocks, fraction, random.Random(SEED))
                estimate = pooled_ratio(blocks, fraction)
                limit, strict = limits[label]
                cell_pass = upper < limit if strict else upper <= limit
                cells[label] = {
                    "pooled_ratio": estimate,
                    "one_sided_95_upper": upper,
                    (
                        "required_upper_strictly_below"
                        if strict
                        else "required_upper_at_most"
                    ): limit,
                    "pass": cell_pass,
                }
            backend_output[metric] = {
                "paired_blocks": len(blocks),
                "p50": cells["p50"],
                "p95": cells["p95"],
                "pass": cells["p50"]["pass"] and cells["p95"]["pass"],
            }
        backend_output["pass"] = all(
            backend_output[metric]["pass"] for metric in ("edit", "render")
        )
        output["backends"][backend] = backend_output
    output["pass"] = all(cell["pass"] for cell in output["backends"].values())
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = evaluate(json.loads(args.input.read_text(encoding="utf-8")))
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    raise SystemExit(0 if result["pass"] else 1)


if __name__ == "__main__":
    main()
