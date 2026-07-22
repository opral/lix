#!/usr/bin/env python3
"""Summarize native Samply profiles without counting parked threads as work."""

from __future__ import annotations

import argparse
import bisect
import gzip
import json
import re
import subprocess
from collections import Counter
from pathlib import Path


IDLE_NAMES = (
    "__psynch_",
    "kevent",
    "mach_msg",
    "park",
    "poll",
    "select",
    "semaphore_wait",
    "sleep",
    "ulock_wait",
)

IDLE_LEAF_RESOURCES = (
    "libsystem_kernel",
    "libsystem_pthread",
)


def load_profile(path: Path) -> dict:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as profile_file:
        return json.load(profile_file)


class NativeSymbols:
    def __init__(self, binary: Path | None):
        self.binary_name = binary.name if binary else ""
        self.addresses: list[int] = []
        self.names: list[str] = []
        if binary is None:
            return
        output = subprocess.check_output(["nm", "-nm", str(binary)], text=True)
        for line in output.splitlines():
            match = re.match(r"^([0-9a-fA-F]+) .* (\S+)$", line)
            if match:
                self.addresses.append(int(match.group(1), 16))
                self.names.append(match.group(2))

    def resolve(self, resource: str, name: str) -> str | None:
        if resource != self.binary_name or not name.startswith("0x") or not self.addresses:
            return None
        # Samply stores image-relative addresses while `nm` reports the normal
        # macOS Mach-O image base plus that offset.
        address = 0x1_0000_0000 + int(name, 16)
        index = bisect.bisect_right(self.addresses, address) - 1
        if index < 0:
            return None
        return self.names[index]


def frame_names(thread: dict, symbols: NativeSymbols) -> list[tuple[str, str]]:
    strings = thread["stringArray"]
    func_names = [strings[index] for index in thread["funcTable"]["name"]]
    resources = thread["resourceTable"]
    resource_names = [strings[index] for index in resources["name"]]
    func_resources = thread["funcTable"]["resource"]
    result = []
    for func_index in thread["frameTable"]["func"]:
        resource_index = func_resources[func_index]
        resource = resource_names[resource_index] if resource_index is not None else ""
        name = func_names[func_index]
        resolved = symbols.resolve(resource, name)
        display = resolved or (f"{resource}!{name}" if name.startswith("0x") and resource else name)
        result.append((display, resource))
    return result


def stack_frames(thread: dict, stack_index: int | None) -> list[int]:
    prefixes = thread["stackTable"]["prefix"]
    frames = thread["stackTable"]["frame"]
    result = []
    while stack_index is not None:
        result.append(frames[stack_index])
        stack_index = prefixes[stack_index]
    result.reverse()
    return result


def is_idle(name: str, resource: str) -> bool:
    lowered = name.lower()
    return any(fragment in lowered for fragment in IDLE_NAMES) or any(
        fragment in resource.lower() for fragment in IDLE_LEAF_RESOURCES
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("profile", type=Path)
    parser.add_argument("--top", type=int, default=30)
    parser.add_argument("--binary", type=Path)
    parser.add_argument(
        "--under",
        help="only count samples whose stack contains this case-sensitive substring",
    )
    args = parser.parse_args()

    profile = load_profile(args.profile)
    symbols = NativeSymbols(args.binary)
    leaves: Counter[str] = Counter()
    inclusive: Counter[str] = Counter()
    active_leaves: Counter[str] = Counter()
    active_inclusive: Counter[str] = Counter()
    thread_samples: Counter[str] = Counter()
    total = 0
    active = 0

    for thread in profile["threads"]:
        names = frame_names(thread, symbols)
        for stack_index in thread["samples"]["stack"]:
            if stack_index is None:
                continue
            stack = [names[index] for index in stack_frames(thread, stack_index)]
            if not stack:
                continue
            if args.under and not any(args.under in name for name, _ in stack):
                continue
            total += 1
            leaf, leaf_resource = stack[-1]
            leaves[leaf] += 1
            for name, _ in set(stack):
                inclusive[name] += 1
            if not is_idle(leaf, leaf_resource):
                active += 1
                thread_samples[thread["name"]] += 1
                active_leaves[leaf] += 1
                for name, _ in set(stack):
                    active_inclusive[name] += 1

    def show(title: str, counts: Counter[str], denominator: int) -> None:
        print(f"\n{title} (denominator={denominator:,})")
        for name, count in counts.most_common(args.top):
            percent = 100.0 * count / max(denominator, 1)
            print(f"{count:9,d} {percent:6.2f}%  {name}")

    print(f"threads={len(profile['threads'])} samples={total:,} active={active:,}")
    show("active samples by thread", thread_samples, active)
    show("active leaf frames", active_leaves, active)
    show("active inclusive frames", active_inclusive, active)
    show("all leaf frames", leaves, total)


if __name__ == "__main__":
    main()
