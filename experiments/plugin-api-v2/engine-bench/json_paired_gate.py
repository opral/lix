#!/usr/bin/env python3
"""Evaluate the preregistered JSON Component-v2/Component-v1 paired gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import random
from typing import Any


REQUIRED_BACKENDS = ("rocksdb-fs", "slatedb-cached")
REQUIRED_METRICS = ("edit", "render")
VALID_ORDERS = ("v1-v2", "v2-v1")
FIXTURE_BYTES = 10_000_000
PROPERTY_COUNT = 220_000
EDIT_PROPERTY = "property_110000"
MIN_BLOCKS = 12
MIN_WARMUPS = 5
MEASURED = 20
MIN_DRAWS = 10_000
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
ZERO_COUNTERS = (
    "source_read_calls",
    "source_bytes_read",
    "host_full_diff_bytes_compared",
    "host_full_content_classification_bytes",
    "full_state_semantic_rows_materialized",
    "shared_renderer_cache_hits",
    "full_document_reparses",
    "full_renderer_invocations",
    "filesystem_sync_full_renders",
)
ONE_COUNTERS = (
    "durable_semantic_changes",
    "private_document_cache_hits",
)


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        raise ValueError("a measured arm must contain at least one sample")
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return ordered[index]


def arm_samples(block: dict[str, Any], api: str) -> list[float]:
    return block["arms"][api]["sample_ms"]


def pooled_percentile(blocks: list[dict[str, Any]], api: str, fraction: float) -> float:
    return percentile(
        [sample for block in blocks for sample in arm_samples(block, api)], fraction
    )


def pooled_ratio(blocks: list[dict[str, Any]], fraction: float) -> float:
    return pooled_percentile(blocks, "v2", fraction) / pooled_percentile(
        blocks, "v1", fraction
    )


def derived_seed(base_seed: int, label: str) -> int:
    digest = hashlib.sha256(label.encode("utf-8")).digest()
    return base_seed ^ int.from_bytes(digest[:8], "big")


def bootstrap_ratios(
    blocks: list[dict[str, Any]],
    fraction: float,
    draws: int,
    rng: random.Random,
) -> list[float]:
    estimates: list[float] = []
    for _ in range(draws):
        v1_pool: list[float] = []
        v2_pool: list[float] = []
        for _ in range(len(blocks)):
            block = blocks[rng.randrange(len(blocks))]
            v1_raw = arm_samples(block, "v1")
            v2_raw = arm_samples(block, "v2")
            v1_pool.extend(v1_raw[rng.randrange(len(v1_raw))] for _ in v1_raw)
            v2_pool.extend(v2_raw[rng.randrange(len(v2_raw))] for _ in v2_raw)
        estimates.append(
            math.exp(
                math.log(percentile(v2_pool, fraction))
                - math.log(percentile(v1_pool, fraction))
            )
        )
    return estimates


def validate_sample(value: Any, name: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{name}: latency must be a finite positive number")


def validate_blocks(
    name: str,
    blocks: Any,
    designed_blocks: int,
    measured: int,
    *,
    expect_v2_counters: bool,
) -> list[dict[str, Any]]:
    if not isinstance(blocks, list) or len(blocks) != designed_blocks:
        raise ValueError(f"{name}: block count must match design.blocks")
    orders = [block.get("order") for block in blocks if isinstance(block, dict)]
    if len(orders) != len(blocks) or any(order not in VALID_ORDERS for order in orders):
        raise ValueError(f"{name}: every block order must be v1-v2 or v2-v1")
    if orders.count("v1-v2") != orders.count("v2-v1"):
        raise ValueError(f"{name}: block order is not exactly counterbalanced")
    for index, block in enumerate(blocks):
        if block.get("index") != index:
            raise ValueError(f"{name}: block indices must be contiguous from zero")
        arms = block.get("arms")
        if not isinstance(arms, dict) or set(arms) != {"v1", "v2"}:
            raise ValueError(f"{name} block {index}: arms must be exactly v1 and v2")
        for api in ("v1", "v2"):
            arm = arms[api]
            if not isinstance(arm, dict):
                raise ValueError(f"{name} block {index} {api}: arm must be an object")
            samples = arm.get("sample_ms")
            if not isinstance(samples, list) or len(samples) != measured:
                raise ValueError(
                    f"{name} block {index} {api}: expected exactly {measured} samples"
                )
            for sample_index, sample in enumerate(samples):
                validate_sample(sample, f"{name} block {index} {api} sample {sample_index}")
            counters = arm.get("counters")
            if not isinstance(counters, list):
                raise ValueError(f"{name} block {index} {api}: counters must be an array")
            if api == "v1" and counters:
                raise ValueError(f"{name} block {index}: v1 must not emit v2 counters")
            if api == "v2":
                expected_counters = measured if expect_v2_counters else 0
                if len(counters) != expected_counters:
                    raise ValueError(
                        f"{name} block {index} v2: expected exactly "
                        f"{expected_counters} counter rows"
                    )
    return blocks


def counter_violations(
    backend: str,
    blocks: list[dict[str, Any]],
    measured: int,
    memory_mib: int,
) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    memory_limit = memory_mib * 1024 * 1024
    for block_index, block in enumerate(blocks):
        counters = block["arms"]["v2"]["counters"]
        if len(counters) != measured:
            violations.append(
                {
                    "backend": backend,
                    "block": block_index,
                    "field": "counter_rows",
                    "actual": len(counters),
                    "expected": measured,
                }
            )
            continue
        for round_index, row in enumerate(counters):
            if not isinstance(row, dict):
                violations.append(
                    {
                        "backend": backend,
                        "block": block_index,
                        "round": round_index,
                        "field": "counter_row",
                        "actual": type(row).__name__,
                        "expected": "object",
                    }
                )
                continue
            if row.get("round") != round_index:
                violations.append(
                    {
                        "backend": backend,
                        "block": block_index,
                        "round": round_index,
                        "field": "round",
                        "actual": row.get("round"),
                        "expected": round_index,
                    }
                )
            for field in ZERO_COUNTERS:
                if row.get(field) != 0:
                    violations.append(
                        {
                            "backend": backend,
                            "block": block_index,
                            "round": round_index,
                            "field": field,
                            "actual": row.get(field),
                            "expected": 0,
                        }
                    )
            for field in ONE_COUNTERS:
                if row.get(field) != 1:
                    violations.append(
                        {
                            "backend": backend,
                            "block": block_index,
                            "round": round_index,
                            "field": field,
                            "actual": row.get(field),
                            "expected": 1,
                        }
                    )
            high_water = row.get("guest_linear_memory_high_water_bytes")
            if (
                isinstance(high_water, bool)
                or not isinstance(high_water, int)
                or high_water <= 0
                or high_water > memory_limit
            ):
                violations.append(
                    {
                        "backend": backend,
                        "block": block_index,
                        "round": round_index,
                        "field": "guest_linear_memory_high_water_bytes",
                        "actual": high_water,
                        "expected": f"1..={memory_limit}",
                    }
                )
    return violations


def validate_design(input_data: dict[str, Any], allow_smoke: bool) -> dict[str, Any]:
    if input_data.get("status") != "complete":
        raise ValueError("input campaign must have status=complete")
    design = input_data.get("design")
    if not isinstance(design, dict):
        raise ValueError("input must contain design metadata")
    if design.get("format") != "json" or design.get("apis") != ["v1", "v2"]:
        raise ValueError("the JSON paired gate requires v1 and v2 JSON arms")
    exact_fixture = {
        "fixture_bytes": FIXTURE_BYTES,
        "properties": PROPERTY_COUNT,
        "edit_property": EDIT_PROPERTY,
        "backends": list(REQUIRED_BACKENDS),
    }
    mismatches = {
        key: (design.get(key), expected)
        for key, expected in exact_fixture.items()
        if design.get(key) != expected
    }
    if mismatches:
        raise ValueError(f"JSON gate design does not use the exact fixture: {mismatches}")
    if design.get("same_benchmark_executable") is not True:
        raise ValueError("both arms must use the same benchmark executable")
    analysis = design.get("analysis")
    if not isinstance(analysis, dict):
        raise ValueError("design must preregister its analysis")
    if analysis.get("thresholds_preregistered_before_samples") != THRESHOLDS:
        raise ValueError("JSON gate thresholds differ from the preregistered defaults")
    draws = analysis.get("draws")
    seed_raw = analysis.get("seed")
    if isinstance(draws, bool) or not isinstance(draws, int) or draws < 100:
        raise ValueError("analysis draws must be an integer of at least 100")
    if not isinstance(seed_raw, str):
        raise ValueError("analysis seed must be a hexadecimal string")
    try:
        seed = int(seed_raw, 16)
    except ValueError as error:
        raise ValueError("analysis seed must be a hexadecimal string") from error

    blocks = design.get("blocks")
    warmups = design.get("warmups_per_arm_block")
    measured = design.get("measured_per_arm_block")
    memory_mib = design.get("wasm_memory_mib")
    for name, value in (
        ("blocks", blocks),
        ("warmups_per_arm_block", warmups),
        ("measured_per_arm_block", measured),
        ("wasm_memory_mib", memory_mib),
    ):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"design.{name} must be an integer")
    if blocks < 2 or blocks % 2:
        raise ValueError("design.blocks must be an even integer of at least two")
    if warmups < 0 or measured < 1 or memory_mib < 1:
        raise ValueError("warmups, measured samples, and memory must be valid")
    eligible = (
        blocks >= MIN_BLOCKS
        and warmups >= MIN_WARMUPS
        and measured == MEASURED
        and draws >= MIN_DRAWS
    )
    if design.get("acceptance_eligible") is not eligible:
        raise ValueError("design.acceptance_eligible is inconsistent with its sample design")
    if not eligible and not allow_smoke:
        raise ValueError(
            "campaign is smoke-only; pass --allow-smoke to analyze without an "
            "acceptance decision"
        )
    return {
        "blocks": blocks,
        "warmups": warmups,
        "measured": measured,
        "memory_mib": memory_mib,
        "draws": draws,
        "seed": seed,
        "eligible": eligible,
    }


def validate_plugin_fingerprints(input_data: dict[str, Any]) -> None:
    plugins = input_data.get("plugins")
    backends = input_data.get("backends")
    if not isinstance(plugins, dict) or set(plugins) != {"v1", "v2"}:
        raise ValueError("input must fingerprint exactly the v1 and v2 plugins")
    if not isinstance(backends, dict):
        raise ValueError("input backends must be an object")
    for api in ("v1", "v2"):
        plugin = plugins[api]
        if not isinstance(plugin, dict):
            raise ValueError(f"{api} plugin fingerprint must be an object")
        observed = plugin.get("archive_observed_at_setup")
        if not isinstance(observed, dict):
            raise ValueError(f"{api} plugin is missing its observed archive fingerprint")
        size = observed.get("bytes")
        digest = observed.get("sha256")
        if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            raise ValueError(f"{api} observed plugin archive size is invalid")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise ValueError(f"{api} observed plugin archive SHA-256 is invalid")
        for backend in REQUIRED_BACKENDS:
            backend_input = backends.get(backend)
            setup = backend_input.get("setup") if isinstance(backend_input, dict) else None
            arm = setup.get(api) if isinstance(setup, dict) else None
            if not isinstance(arm, dict) or arm.get("plugin_archive") != observed:
                raise ValueError(
                    f"{backend} {api} setup does not match the observed plugin archive"
                )


def evaluate(input_data: dict[str, Any], *, allow_smoke: bool = False) -> dict[str, Any]:
    if not isinstance(input_data, dict):
        raise ValueError("input must be a JSON object")
    design = validate_design(input_data, allow_smoke)
    backends = input_data.get("backends")
    if not isinstance(backends, dict) or set(backends) != set(REQUIRED_BACKENDS):
        raise ValueError(
            "input must contain exactly rocksdb-fs and slatedb-cached backends"
        )
    validate_plugin_fingerprints(input_data)

    output: dict[str, Any] = {
        "method": (
            "paired hierarchical cluster bootstrap of pooled log v2/v1 "
            "p50 and p95 ratios"
        ),
        "seed": f"0x{design['seed']:08x}",
        "draws": design["draws"],
        "acceptance_eligible": design["eligible"],
        "thresholds": THRESHOLDS,
        "backends": {},
    }
    all_counter_violations: list[dict[str, Any]] = []
    for backend in REQUIRED_BACKENDS:
        backend_input = backends[backend]
        if not isinstance(backend_input, dict) or backend_input.get("status") != "complete":
            raise ValueError(f"{backend}: campaign must be complete")
        backend_output: dict[str, Any] = {}
        metric_blocks: dict[str, list[dict[str, Any]]] = {}
        for metric in REQUIRED_METRICS:
            blocks = validate_blocks(
                f"{backend}/{metric}",
                backend_input.get(metric),
                design["blocks"],
                design["measured"],
                expect_v2_counters=(metric == "edit"),
            )
            metric_blocks[metric] = blocks
            cells: dict[str, Any] = {}
            for label, fraction in (("p50", 0.50), ("p95", 0.95)):
                seed = derived_seed(
                    design["seed"], f"{backend}/{metric}/{label}"
                )
                bootstrap = bootstrap_ratios(
                    blocks,
                    fraction,
                    design["draws"],
                    random.Random(seed),
                )
                threshold = THRESHOLDS[metric][label]
                upper = percentile(bootstrap, 0.95)
                if "upper_ratio_strictly_below" in threshold:
                    limit = threshold["upper_ratio_strictly_below"]
                    passed = upper < limit
                else:
                    limit = threshold["upper_ratio_at_most"]
                    passed = upper <= limit
                cells[label] = {
                    "v1_ms": pooled_percentile(blocks, "v1", fraction),
                    "v2_ms": pooled_percentile(blocks, "v2", fraction),
                    "pooled_ratio_v2_over_v1": pooled_ratio(blocks, fraction),
                    "bootstrap_two_sided_95_ratio": {
                        "lower": percentile(bootstrap, 0.025),
                        "upper": percentile(bootstrap, 0.975),
                    },
                    "one_sided_95_upper_ratio": upper,
                    "required": threshold,
                    "pass": passed,
                }
            backend_output[metric] = {
                "paired_blocks": len(blocks),
                "p50": cells["p50"],
                "p95": cells["p95"],
                "pass": cells["p50"]["pass"] and cells["p95"]["pass"],
            }
        violations = counter_violations(
            backend,
            metric_blocks["edit"],
            design["measured"],
            design["memory_mib"],
        )
        all_counter_violations.extend(violations)
        backend_output["v2_hot_path_counter_gate"] = {
            "pass": not violations,
            "violations": violations,
        }
        backend_output["thresholds_pass"] = all(
            backend_output[metric]["pass"] for metric in REQUIRED_METRICS
        )
        backend_output["pass"] = (
            backend_output["thresholds_pass"] and not violations
        )
        output["backends"][backend] = backend_output

    output["counter_gate"] = {
        "pass": not all_counter_violations,
        "violations": all_counter_violations,
    }
    output["thresholds_pass"] = all(
        backend["thresholds_pass"] for backend in output["backends"].values()
    )
    output["statistical_and_counter_pass"] = all(
        backend["pass"] for backend in output["backends"].values()
    )
    output["pass"] = (
        output["acceptance_eligible"] and output["statistical_and_counter_pass"]
    )
    if output["acceptance_eligible"]:
        output["decision"] = "pass" if output["pass"] else "fail"
    elif output["statistical_and_counter_pass"]:
        output["decision"] = "smoke-only; checks passed, no acceptance decision"
    else:
        output["decision"] = "smoke-only; checks failed, no acceptance decision"
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--allow-smoke",
        action="store_true",
        help="analyze a reduced design but never label it acceptance evidence",
    )
    args = parser.parse_args()
    result = evaluate(
        json.loads(args.input.read_text(encoding="utf-8")),
        allow_smoke=args.allow_smoke,
    )
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    if args.allow_smoke and not result["acceptance_eligible"]:
        raise SystemExit(0 if result["statistical_and_counter_pass"] else 1)
    raise SystemExit(0 if result["pass"] else 1)


if __name__ == "__main__":
    main()
