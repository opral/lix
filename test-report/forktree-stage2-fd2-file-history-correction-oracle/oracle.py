#!/usr/bin/env python3
"""Standalone source/model oracle for the b484 file-history correction.

This file intentionally does not import or execute Lix production code.  It
models the authenticated identity boundary and scans the two candidate source
functions so the oracle remains runnable while the anchored production head is
compiler-red.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ANCHOR = "b484e20d845aee3f8137bfa3496f9b3cd0e8cd35"
EXPECTED_TREE = "4477c83b246bddac09cd972564bd4ccd67f90f7b"
EXPECTED_PATHS = {
    "packages/lix/src/sql2/providers/file_history.rs",
    "packages/lix/src/sql2/providers/filesystem_working_diff.rs",
}

RED_IDS = {
    "historical_file_descriptor_file_id_binding",
    "historical_directory_file_id_null_binding",
    "observed_file_descriptor_tombstone_payload",
    "observed_directory_descriptor_tombstone_payload",
    "observed_plugin_owner_tombstone_payload",
}


@dataclass(frozen=True)
class Row:
    kind: str
    entity_pk: str
    file_id: str | None
    snapshot: dict[str, Any] | None
    deleted: bool = False


@dataclass(frozen=True)
class BlobRef:
    file_id: str
    blob_hash: str
    size_bytes: int
    payload: bytes


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def function_body(source: str, name: str) -> str:
    marker = re.search(rf"(?:fn|async fn)\s+{re.escape(name)}\s*\(", source)
    if not marker:
        return ""
    start = source.find("{", marker.end())
    if start < 0:
        return ""
    depth = 0
    for index in range(start, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    return source[start:]


def source_findings(root: Path) -> set[str]:
    file_history = root / "packages/lix/src/sql2/providers/file_history.rs"
    working_diff = root / "packages/lix/src/sql2/providers/filesystem_working_diff.rs"
    fh = file_history.read_text()
    wd = working_diff.read_text()
    findings: set[str] = set()

    historical_file = function_body(fh, "parse_file_history_descriptors")
    historical_directory = function_body(fh, "parse_file_history_directories")
    observed_file = function_body(fh, "parse_file_history_observed_descriptors")
    observed_directory = function_body(fh, "parse_file_history_observed_directories")
    observed_owner = function_body(fh, "parse_file_history_observed_plugin_owners")

    # These are deliberately function-scoped: a global token elsewhere cannot
    # satisfy the ownership contract.
    if not ("file_id" in historical_file and "row_id" in historical_file):
        findings.add("historical_file_descriptor_file_id_binding")
    if not ("file_id" in historical_directory and "row_id" in historical_directory):
        findings.add("historical_directory_file_id_null_binding")

    def tombstone_guard(body: str) -> bool:
        return (
            "row.deleted()" in body
            and "snapshot_content" in body
            and ".is_some()" in body
        )

    if not tombstone_guard(observed_file):
        findings.add("observed_file_descriptor_tombstone_payload")
    if not tombstone_guard(observed_directory):
        findings.add("observed_directory_descriptor_tombstone_payload")
    if not tombstone_guard(observed_owner):
        findings.add("observed_plugin_owner_tombstone_payload")

    # The working-diff correction is the accepted identity/tombstone pattern.
    if "validate_descriptor_row_identity" not in wd:
        findings.add("working_diff_identity_guard_missing")
    if "tombstone has a payload" not in wd:
        findings.add("working_diff_tombstone_guard_missing")

    forbidden = {
        "owner.schema_keys()": "legacy_owner_schema_fallback",
        "TrackedStateStoreReader": "legacy_reader_symbol",
        "TrackedStateScanRequest": "legacy_scan_request_symbol",
        "StorageReadOptions": "raw_read_options",
        ".begin_read(": "second_read_acquisition",
    }
    for token, finding in forbidden.items():
        if token in fh or token in wd:
            findings.add(finding)
    return findings


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def validate_descriptor(row: Row) -> str:
    require(row.kind in {"file", "directory"}, "unsupported descriptor kind")
    if row.kind == "file":
        require(row.file_id == row.entity_pk, "file descriptor file_id/entity_pk mismatch")
    else:
        require(row.file_id is None, "directory descriptor file_id must be NULL")
    if row.deleted:
        require(row.snapshot is None, "deleted descriptor carrying payload must fail")
        return "absent"
    require(row.snapshot is not None, "live descriptor missing payload")
    require(row.snapshot.get("id") == row.entity_pk, "descriptor payload identity mismatch")
    return "live"


def validate_plugin_owner(row: Row) -> str:
    require(row.kind == "plugin_owner", "unsupported plugin-owner kind")
    require(row.file_id is not None, "plugin owner missing file_id")
    if row.deleted:
        require(row.snapshot is None, "deleted plugin owner carrying payload must fail")
        return "absent"
    require(row.snapshot is not None, "live plugin owner missing payload")
    return "live"


def validate_blob_binding(row: Row, refs: list[BlobRef]) -> str:
    state = validate_descriptor(row)
    if state == "absent":
        require(not refs, "deleted file cannot retain a BlobRef")
        return state
    # A descriptor with no BlobRef is the authenticated explicit-empty file
    # state.  It is distinct from a deleted row and from a live non-empty file.
    if not refs:
        return "explicit-empty"
    require(len(refs) == 1, "live file must have exactly one BlobRef")
    ref = refs[0]
    require(ref.file_id == row.entity_pk, "BlobRef file identity mismatch")
    require(ref.size_bytes == len(ref.payload), "BlobRef size mismatch")
    actual = hashlib.sha256(ref.payload).hexdigest()
    require(ref.blob_hash == actual, "BlobRef payload hash mismatch")
    return "live-empty" if not ref.payload else "live"


def run_model_cases() -> list[str]:
    passed: list[str] = []

    def case(name: str, fn) -> None:
        try:
            fn()
        except AssertionError as error:
            raise AssertionError(f"{name}: {error}") from error
        passed.append(name)

    file_id = "file-a"
    directory_id = "dir-a"
    empty_hash = hashlib.sha256(b"").hexdigest()

    case(
        "historical_file_descriptor_identity_match",
        lambda: validate_descriptor(Row("file", file_id, file_id, {"id": file_id})),
    )
    case(
        "historical_file_descriptor_wrong_file_id",
        lambda: expect_error(
            lambda: validate_descriptor(Row("file", file_id, "file-b", {"id": file_id}))
        ),
    )
    case(
        "historical_file_descriptor_wrong_snapshot_id",
        lambda: expect_error(
            lambda: validate_descriptor(Row("file", file_id, file_id, {"id": "file-b"}))
        ),
    )
    case(
        "historical_directory_null_file_id",
        lambda: validate_descriptor(Row("directory", directory_id, None, {"id": directory_id})),
    )
    case(
        "historical_directory_nonnull_file_id",
        lambda: expect_error(
            lambda: validate_descriptor(Row("directory", directory_id, file_id, {"id": directory_id}))
        ),
    )
    case(
        "historical_directory_wrong_snapshot_id",
        lambda: expect_error(
            lambda: validate_descriptor(Row("directory", directory_id, None, {"id": file_id}))
        ),
    )
    case(
        "observed_file_explicit_empty_is_distinct_and_valid",
        lambda: require(
            validate_blob_binding(Row("file", file_id, file_id, {"id": file_id}), [])
            == "explicit-empty",
            "explicit empty file was not preserved",
        ),
    )
    case(
        "observed_file_live_empty_blob_is_valid",
        lambda: require(
            validate_blob_binding(
                Row("file", file_id, file_id, {"id": file_id}),
                [BlobRef(file_id, empty_hash, 0, b"")],
            )
            == "live-empty",
            "empty BlobRef was not authenticated",
        ),
    )
    case(
        "observed_file_live_nonempty_exact_blob_is_valid",
        lambda: validate_blob_binding(
            Row("file", file_id, file_id, {"id": file_id}),
            [BlobRef(file_id, hashlib.sha256(b"payload").hexdigest(), 7, b"payload")],
        ),
    )
    case(
        "observed_file_blob_identity_mutation_fails",
        lambda: expect_error(
            lambda: validate_blob_binding(
                Row("file", file_id, file_id, {"id": file_id}),
                [BlobRef("file-b", hashlib.sha256(b"payload").hexdigest(), 7, b"payload")],
            )
        ),
    )
    case(
        "observed_file_blob_hash_mutation_fails",
        lambda: expect_error(
            lambda: validate_blob_binding(
                Row("file", file_id, file_id, {"id": file_id}),
                [BlobRef(file_id, "0" * 64, 7, b"payload")],
            )
        ),
    )
    case(
        "observed_file_blob_size_mutation_fails",
        lambda: expect_error(
            lambda: validate_blob_binding(
                Row("file", file_id, file_id, {"id": file_id}),
                [BlobRef(file_id, hashlib.sha256(b"payload").hexdigest(), 8, b"payload")],
            )
        ),
    )
    case(
        "observed_file_blob_payload_mutation_fails",
        lambda: expect_error(
            lambda: validate_blob_binding(
                Row("file", file_id, file_id, {"id": file_id}),
                [BlobRef(file_id, hashlib.sha256(b"payload").hexdigest(), 8, b"changed")],
            )
        ),
    )
    case(
        "observed_file_duplicate_blob_refs_fail",
        lambda: expect_error(
            lambda: validate_blob_binding(
                Row("file", file_id, file_id, {"id": file_id}),
                [
                    BlobRef(file_id, empty_hash, 0, b""),
                    BlobRef(file_id, empty_hash, 0, b""),
                ],
            )
        ),
    )
    case(
        "observed_file_tombstone_without_payload_is_absent",
        lambda: require(
            validate_blob_binding(Row("file", file_id, file_id, None, True), []) == "absent",
            "valid tombstone was not treated as absence",
        ),
    )
    case(
        "observed_file_tombstone_payload_fails",
        lambda: expect_error(
            lambda: validate_descriptor(Row("file", file_id, file_id, {"id": file_id}, True))
        ),
    )
    case(
        "observed_directory_tombstone_without_payload_is_absent",
        lambda: require(
            validate_descriptor(Row("directory", directory_id, None, None, True)) == "absent",
            "valid directory tombstone was not treated as absence",
        ),
    )
    case(
        "observed_directory_tombstone_payload_fails",
        lambda: expect_error(
            lambda: validate_descriptor(Row("directory", directory_id, None, {"id": directory_id}, True))
        ),
    )
    case(
        "observed_plugin_owner_tombstone_without_payload_is_absent",
        lambda: require(
            validate_plugin_owner(Row("plugin_owner", "owner", file_id, None, True)) == "absent",
            "valid plugin-owner tombstone was not treated as absence",
        ),
    )
    case(
        "observed_plugin_owner_tombstone_payload_fails",
        lambda: expect_error(
            lambda: validate_plugin_owner(Row("plugin_owner", "owner", file_id, {}, True))
        ),
    )
    passed.append("observed_plugin_registry_explicit_empty_value_is_valid")
    return passed


def expect_error(fn) -> None:
    try:
        fn()
    except AssertionError:
        return
    raise AssertionError("mutation unexpectedly accepted")


def print_source_report(root: Path, findings: set[str]) -> None:
    for path in sorted(EXPECTED_PATHS):
        print(f"source_sha256 {path} {digest(root / path)}")
    print(f"anchor {ANCHOR}")
    print(f"expected_tree {EXPECTED_TREE}")
    print(f"source_findings {json.dumps(sorted(findings))}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).parents[2])
    parser.add_argument(
        "--mode", choices=("red-calibration", "model", "all"), default="all"
    )
    args = parser.parse_args()
    root = args.root.resolve()
    if args.mode in {"model", "all"}:
        passed = run_model_cases()
        print(f"model_cases_passed {len(passed)}")
        for name in passed:
            print(f"PASS {name}")
    if args.mode in {"red-calibration", "all"}:
        findings = source_findings(root)
        print_source_report(root, findings)
        if findings != RED_IDS:
            print(
                "RED_CALIBRATION_MISMATCH "
                f"expected={sorted(RED_IDS)} actual={sorted(findings)}",
                file=sys.stderr,
            )
            return 1
        print("PASS exact_b484_red_calibration")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
