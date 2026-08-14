#!/usr/bin/env python3
import csv
import json
import math
import pathlib
import statistics
import sys


def percentile(values, percentage):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percentage / 100) - 1)]


manifest = pathlib.Path(sys.argv[1])
groups = {}
with manifest.open(newline="") as handle:
    for cell in csv.DictReader(handle, delimiter="\t"):
        with pathlib.Path(cell["stdout"]).open() as output:
            for line in output:
                event = json.loads(line)
                if event.get("event") != "metric":
                    continue
                key = (cell["label"], cell["backend"], cell["size"], event["label"].split("/", 1)[1])
                groups.setdefault(key, []).append(event)

columns = [
    "label", "backend", "size", "operation", "samples",
    "wall_p50_ms", "wall_p95_ms", "cpu_p50_ticks", "cpu_p95_ticks",
    "alloc_p50_bytes", "alloc_p95_bytes", "rss_p50_kib", "rss_p95_kib",
    "read_calls_p50", "read_keys_p50", "read_bytes_p50",
    "write_calls_p50", "backend_written_bytes_p50", "disk_delta_p50",
]
writer = csv.DictWriter(sys.stdout, fieldnames=columns, delimiter="\t", lineterminator="\n")
writer.writeheader()
for key, events in sorted(groups.items()):
    def values(field):
        return [event[field] for event in events]

    def io_values(field):
        return [event["io"][field] for event in events]

    writer.writerow({
        "label": key[0],
        "backend": key[1],
        "size": key[2],
        "operation": key[3],
        "samples": len(events),
        "wall_p50_ms": f"{percentile(values('wall_ms'), 50):.6f}",
        "wall_p95_ms": f"{percentile(values('wall_ms'), 95):.6f}",
        "cpu_p50_ticks": percentile(values("cpu_ticks"), 50),
        "cpu_p95_ticks": percentile(values("cpu_ticks"), 95),
        "alloc_p50_bytes": percentile(values("alloc_bytes"), 50),
        "alloc_p95_bytes": percentile(values("alloc_bytes"), 95),
        "rss_p50_kib": percentile(values("rss_hwm_kib"), 50),
        "rss_p95_kib": percentile(values("rss_hwm_kib"), 95),
        "read_calls_p50": statistics.median(io_values("get_many_calls")),
        "read_keys_p50": statistics.median(io_values("get_many_keys")),
        "read_bytes_p50": statistics.median(io_values("get_many_value_bytes")),
        "write_calls_p50": statistics.median(io_values("begin_writes")),
        "backend_written_bytes_p50": statistics.median(io_values("backend_written_bytes")),
        "disk_delta_p50": statistics.median(values("disk_delta")),
    })
