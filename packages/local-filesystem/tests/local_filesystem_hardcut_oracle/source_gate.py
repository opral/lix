#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import pathlib
import re
import sys


def files(root: pathlib.Path, relative: str, suffixes: tuple[str, ...]):
    base = root / relative
    for path in sorted(base.rglob("*")):
        if not path.is_file() or path.suffix not in suffixes:
            continue
        name = path.as_posix()
        if "local_filesystem_hardcut_oracle" in name or "local-filesystem-hardcut" in name:
            continue
        yield path


def collect(root: pathlib.Path, relative: str, suffixes: tuple[str, ...]) -> str:
    return "\n".join(path.read_text(errors="replace") for path in files(root, relative, suffixes))


def add_matches(findings: list[str], label: str, pattern: str, source: str):
    count = len(re.findall(pattern, source, flags=re.MULTILINE))
    if count:
        findings.append(f"{label}\t{count}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=pathlib.Path)
    parser.add_argument("mode", choices=("baseline", "candidate"))
    args = parser.parse_args()
    root = args.root.resolve()

    rust = collect(root, "packages/local-filesystem", (".rs", ".md", ".toml"))
    js = collect(root, "packages/js-sdk", (".ts", ".js", ".mjs", ".md", ".json"))
    native = collect(root, "packages/js-sdk/native", (".rs",))
    filesystem = (root / "packages/local-filesystem/src/filesystem.rs").read_text()

    findings: list[str] = []
    for label, pattern, source in (
        ("rust-options-type", r"\bpub\s+struct\s+LocalFilesystemOpenOptions\b", rust),
        ("rust-options-export", r"\bpub\s+use[^;]*LocalFilesystemOpenOptions", rust),
        ("rust-open-with-options", r"\bpub\s+async\s+fn\s+open_with_options\b", rust),
        ("rust-open-with-runtime", r"\bpub\s+async\s+fn\s+open_with_options_and_wasm_runtime\b", rust),
        ("rust-public-import", r"\bpub\s+async\s+fn\s+import_paths\b", rust),
        ("rust-public-manual-sync", r"\bpub\s+async\s+fn\s+sync_disk_to_lix\b", rust),
        ("js-options-type", r"\bLocalFilesystemOptions\b", js),
        ("js-lix-dir-option", r"\blixDir\b", js),
        ("js-sync-all-option", r"\bsyncAllFiles\b", js),
        ("js-public-import", r"\bimportPaths\b", js),
        ("js-public-manual-sync", r"\bsyncDiskToLix\b", js),
        ("native-import-command", r"\bImportFilesystemPaths\b|\bimport_filesystem_paths\b", native),
        ("native-sync-command", r"\bSyncDiskToLix\b|\bpub\s+fn\s+sync_disk_to_lix\b", native),
        ("native-lix-dir-option", r"\blix_dir\s*:", native),
        ("native-sync-all-option", r"\bsync_all_files\s*:", native),
    ):
        add_matches(findings, label, pattern, source)

    watcher_constructors = len(re.findall(r"\bnew_debouncer_opt\s*::<", filesystem))
    supervisor_owners = len(re.findall(r"\bstruct\s+FilesystemSupervisorInner\b", filesystem))
    worker_owners = filesystem.count('"lix-sdk-filesystem-sync"')
    js_watchers = len(re.findall(r"\b(?:fs\.watch|watchFile|chokidar)\b", js))

    if not re.search(r"pub\s+async\s+fn\s+open\s*<[^>]+>\s*\([^)]*path|pub\s+async\s+fn\s+open\s*<[^>]+>\s*\([^)]*dir", filesystem, re.DOTALL):
        findings.append("missing-rust-positional-open\t0")
    open_lix_ts = (root / "packages/js-sdk/src/open-lix.ts").read_text()
    if not re.search(r"constructor\s*\(\s*path\s*:\s*string\s*\)", open_lix_ts):
        findings.append("missing-js-positional-constructor\t0")
    if watcher_constructors != 1:
        findings.append(f"watcher-constructor-count\t{watcher_constructors}")
    if supervisor_owners != 1:
        findings.append(f"supervisor-owner-count\t{supervisor_owners}")
    if worker_owners != 1:
        findings.append(f"worker-owner-count\t{worker_owners}")
    if js_watchers != 0:
        findings.append(f"js-watcher-owner-count\t{js_watchers}")

    terminal_ack = re.search(
        r"(?:Shutdown|Close)\s*\{[^}]{0,400}(?:reply|ack|done)",
        filesystem,
        re.DOTALL,
    )
    if not terminal_ack:
        findings.append("missing-terminal-drain-ack\t0")
    if ".join()" not in filesystem:
        findings.append("missing-worker-join\t0")
    if re.search(
        r"Ok\(FilesystemEvent::Shutdown\)[^=]{0,300}return\s+true",
        filesystem,
        re.DOTALL,
    ):
        findings.append("shutdown-can-bypass-collected-sync-replies\t1")

    findings.sort()
    payload = "".join(f"{line}\n" for line in findings).encode()
    print(f"mode={args.mode}")
    print(f"finding_count={len(findings)}")
    print(f"finding_sha256={hashlib.sha256(payload).hexdigest()}")
    print(f"watcher_constructors={watcher_constructors}")
    print(f"supervisor_owners={supervisor_owners}")
    print(f"worker_owners={worker_owners}")
    print(f"js_watcher_owners={js_watchers}")
    for finding in findings:
        print(finding)

    if args.mode == "baseline":
        return 0 if findings else 1
    return 0 if not findings else 1


if __name__ == "__main__":
    sys.exit(main())
