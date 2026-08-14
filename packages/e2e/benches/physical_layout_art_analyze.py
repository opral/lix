#!/usr/bin/env python3
"""Deterministic EXP-ART-01 p50/p95 and C2-relative scorecard."""

from __future__ import annotations

import csv
import math
import sys
from collections import defaultdict


def percentile(values: list[int], percentile_value: float) -> int:
    ordered = sorted(values)
    index = max(0, math.ceil(percentile_value * len(ordered)) - 1)
    return ordered[index]


def geometric_mean(values: list[float]) -> float:
    return math.exp(sum(math.log(value) for value in values) / len(values))


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: physical_layout_art_analyze.py RESULTS.csv")
    with open(sys.argv[1], newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source))

    groups: dict[tuple[str, str, str, int, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        if row["status"] not in {"ok", "full_rebuild_model_non_oltp"}:
            continue
        key = (
            row["backend"],
            row["operation"],
            row["pk"],
            int(row["n"]),
            row["geometry"],
        )
        groups[key].append(row)

    writer = csv.writer(sys.stdout)
    writer.writerow(
        [
            "backend",
            "operation",
            "pk",
            "n",
            "geometry",
            "samples",
            "wall_p50_ns",
            "wall_p95_ns",
            "cpu_p50_ns",
            "cpu_p95_ns",
            "rss_max_kb",
            "read_objects_p50",
            "read_bytes_p50",
            "writes_p50",
            "write_bytes_p50",
            "object_bytes_p50",
            "settled_bytes_p50",
        ]
    )
    medians: dict[tuple[str, str, str, int, str], int] = {}
    for key in sorted(groups):
        samples = groups[key]
        integers = {
            field: [int(row[field]) for row in samples]
            for field in (
                "wall_ns",
                "cpu_ns",
                "rss_kb",
                "read_objects",
                "read_bytes",
                "writes",
                "write_bytes",
                "object_bytes",
                "settled_bytes",
            )
        }
        medians[key] = percentile(integers["wall_ns"], 0.50)
        writer.writerow(
            [
                *key,
                len(samples),
                medians[key],
                percentile(integers["wall_ns"], 0.95),
                percentile(integers["cpu_ns"], 0.50),
                percentile(integers["cpu_ns"], 0.95),
                max(integers["rss_kb"]),
                percentile(integers["read_objects"], 0.50),
                percentile(integers["read_bytes"], 0.50),
                percentile(integers["writes"], 0.50),
                percentile(integers["write_bytes"], 0.50),
                percentile(integers["object_bytes"], 0.50),
                percentile(integers["settled_bytes"], 0.50),
            ]
        )

    oltp_operations = {"point", "missing_point", "update_one", "mutate_1pct"}
    vcs_operations = {"hash_diff_1", "hash_diff_10", "hash_diff_1pct"}
    ratios: dict[str, list[float]] = {"oltp": [], "vcs": []}
    for key, art_median in medians.items():
        backend, operation, pk, n, geometry = key
        if geometry != "crit_bit" or backend != "model":
            continue
        c2_key = (backend, operation, pk, n, "c2_slotted")
        c2_median = medians.get(c2_key)
        if not c2_median or not art_median:
            continue
        ratio = art_median / c2_median
        if operation in oltp_operations:
            ratios["oltp"].append(ratio)
        if operation in vcs_operations:
            ratios["vcs"].append(ratio)

    for priority in ("oltp", "vcs"):
        if ratios[priority]:
            ratio = geometric_mean(ratios[priority])
            print(
                f"# {priority}_wall_geomean_ratio={ratio:.6f} "
                f"delta_percent={(ratio - 1.0) * 100:.3f} cells={len(ratios[priority])}"
            )


if __name__ == "__main__":
    main()
