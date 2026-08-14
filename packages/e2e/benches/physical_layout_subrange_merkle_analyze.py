#!/usr/bin/env python3
"""Deterministic C2-relative scorecard for EXP-SUBRANGE-MERKLE-12."""

from __future__ import annotations

import csv
import math
import sys
from collections import defaultdict


def percentile(values: list[int], quantile: float) -> int:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def geometric_mean(values: list[float]) -> float:
    return math.exp(sum(math.log(value) for value in values) / len(values))


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: physical_layout_subrange_merkle_analyze.py RESULTS.csv...")
    rows: list[dict[str, str]] = []
    for path in sys.argv[1:]:
        with open(path, newline="", encoding="utf-8") as source:
            rows.extend(csv.DictReader(source))

    groups: dict[tuple[str, str, int, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        if row["backend"] != "model" or row["status"] != "ok":
            continue
        key = (row["operation"], row["pk"], int(row["n"]), row["geometry"])
        groups[key].append(row)

    writer = csv.writer(sys.stdout)
    writer.writerow(
        [
            "operation",
            "pk",
            "n",
            "samples",
            "c2_p50_ns",
            "candidate_p50_ns",
            "p50_ratio",
            "c2_p95_ns",
            "candidate_p95_ns",
            "p95_ratio",
            "cpu_ratio",
            "rss_ratio",
        ]
    )
    for operation, pk, n, geometry in sorted(groups):
        if geometry != "c2_slotted" or operation in {"build"}:
            continue
        baseline = groups[(operation, pk, n, "c2_slotted")]
        candidate = groups[(operation, pk, n, "c2_subrange_merkle")]
        if not candidate:
            continue
        c2_p50 = percentile([int(row["wall_ns"]) for row in baseline], 0.50)
        candidate_p50 = percentile([int(row["wall_ns"]) for row in candidate], 0.50)
        if c2_p50 == 0 or candidate_p50 == 0:
            continue
        c2_p95 = percentile([int(row["wall_ns"]) for row in baseline], 0.95)
        candidate_p95 = percentile([int(row["wall_ns"]) for row in candidate], 0.95)
        c2_cpu = percentile([int(row["cpu_ns"]) for row in baseline], 0.50)
        candidate_cpu = percentile([int(row["cpu_ns"]) for row in candidate], 0.50)
        c2_rss = max(int(row["rss_kb"]) for row in baseline)
        candidate_rss = max(int(row["rss_kb"]) for row in candidate)
        writer.writerow(
            [
                operation,
                pk,
                n,
                len(candidate),
                c2_p50,
                candidate_p50,
                f"{candidate_p50 / c2_p50:.6f}",
                c2_p95,
                candidate_p95,
                f"{candidate_p95 / c2_p95:.6f}",
                f"{candidate_cpu / max(1, c2_cpu):.6f}",
                f"{candidate_rss / max(1, c2_rss):.6f}",
            ]
        )

    for label, operations in (
        ("OLTP", {"point", "missing_point", "update_one", "mutate_1pct", "range_100", "full_scan"}),
        ("VCS", {"hash_diff_1", "hash_diff_10", "hash_diff_1pct", "merge_1", "merge_10", "merge_1pct"}),
    ):
        ratios: list[float] = []
        for (operation, pk, n, geometry), baseline in groups.items():
            if geometry != "c2_slotted" or operation not in operations:
                continue
            candidate = groups[(operation, pk, n, "c2_subrange_merkle")]
            ratios.append(
                percentile([int(row["wall_ns"]) for row in candidate], 0.50)
                / percentile([int(row["wall_ns"]) for row in baseline], 0.50)
            )
        print(f"# {label}_P50_GEOMEAN_RATIO={geometric_mean(ratios):.9f}")


if __name__ == "__main__":
    main()
