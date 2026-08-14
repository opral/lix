#!/usr/bin/env python3
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def percentile(values: list[float], percentile_value: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percentile_value * len(ordered)) - 1)]


def load(path: Path) -> tuple[dict[str, list[dict]], list[dict]]:
    metrics: dict[str, list[dict]] = defaultdict(list)
    results: list[dict] = []
    for line in path.read_text().splitlines():
        record = json.loads(line)
        if record["event"] == "metric":
            metrics[record["operation"]].append(record)
        elif record["event"] == "result":
            results.append(record)
    return metrics, results


def summarize(metrics: dict[str, list[dict]]) -> dict[str, dict]:
    output: dict[str, dict] = {}
    for operation, records in sorted(metrics.items()):
        elapsed = [record["elapsed_ms"] for record in records]
        output[operation] = {
            "samples": len(records),
            "p50_ms": statistics.median(elapsed),
            "p95_ms": percentile(elapsed, 0.95),
            "throughput_per_second_p50": statistics.median(
                record["throughput_per_second"] for record in records
            ),
            "cpu_ticks_p50": statistics.median(
                record["cpu_ticks"] for record in records
            ),
            "alloc_bytes_p50": statistics.median(
                record["alloc_bytes"] for record in records
            ),
            "rss_after_kib_p50": statistics.median(
                record["rss_after_kib"] for record in records
            ),
            "rss_hwm_kib_max": max(record["rss_hwm_kib"] for record in records),
            "read_bytes_p50": statistics.median(
                record["read_bytes"] for record in records
            ),
            "write_bytes_p50": statistics.median(
                record["write_bytes"] for record in records
            ),
        }
    return output


def main() -> None:
    root = Path(sys.argv[1])
    report = {}
    for backend in ("rocksdb", "slatedb"):
        metrics, results = load(root / f"{backend}-measured.jsonl")
        if not results or not all(result["verified"] for result in results):
            raise SystemExit(f"{backend}: missing or unverified result")
        semantic_fields = (
            "fixture_digest",
            "rendered_digest",
            "exact_row_digest",
            "range_row_digest",
            "full_row_digest",
            "batch_17_digest",
            "row_count_after_reopen",
        )
        for field in semantic_fields:
            if len({result[field] for result in results}) != 1:
                raise SystemExit(f"{backend}: unstable {field}")
        report[backend] = {
            "operations": summarize(metrics),
            "semantic_result": {field: results[0][field] for field in semantic_fields},
            "settled_disk_bytes_p50": statistics.median(
                result["settled_disk_bytes"] for result in results
            ),
        }
    if report["rocksdb"]["semantic_result"] != report["slatedb"]["semantic_result"]:
        raise SystemExit("RocksDB and SlateDB semantic digests differ")
    (root / "summary.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
