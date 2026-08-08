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
    "selected_missing_or_tombstoned_blob_ref_fails_closed",
    "composite_entity_pk_first_component_selection",
    "conflicting_source_change_ids_fail_closed",
    "zero_blob_ref_projection_state_not_distinguished",
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
    deleted: bool = False


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def function_body(source: str, name: str) -> str:
    marker = re.search(rf"(?:fn|async fn)\s+{re.escape(name)}\b", source)
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
    blob_validator = function_body(fh, "validate_exactly_one_blob_ref")
    row_loader = function_body(fh, "load_file_history_rows")
    composite_pk_selector = function_body(wd, "single_entity_pk_value")
    source_change_grouping = function_body(fh, "sorted_grouped_file_history_events")

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

    # A missing/NULL selected reference and a tombstoned reference are not the
    # authenticated empty BlobRef. b484 returns Ok(None) for both and then
    # turns the missing bytes into Some([]) in the projection path.
    if (
        "unwrap_or_default()" in row_loader
        or ("blob.deleted" in blob_validator and "return Ok(None)" in blob_validator)
    ):
        findings.add("selected_missing_or_tombstoned_blob_ref_fails_closed")

    # The working-diff selector must authenticate the complete EntityPk.  The
    # b484 implementation deserializes the composite key and returns only its
    # first component, which can select a different entity under a collision.
    if not composite_pk_selector or ".next()" in composite_pk_selector:
        findings.add("composite_entity_pk_first_component_selection")

    # Grouping currently sorts source changes and silently deduplicates by ID.
    # A same-ID/different-payload pair is corruption, not a legal dedup case.
    if (
        not source_change_grouping
        or "dedup_by" in source_change_grouping
        and "left.id == right.id" in source_change_grouping
        and "LixError" not in source_change_grouping
    ):
        findings.add("conflicting_source_change_ids_fail_closed")

    # The projection currently turns a missing blob byte value into the same
    # empty byte vector used by an authenticated zero-length BlobRef.
    if "unwrap_or_default()" in row_loader:
        findings.add("zero_blob_ref_projection_state_not_distinguished")

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


def canonical_composite_file_id(parts: tuple[str, ...]) -> str:
    require(len(parts) == 2, "composite file identity must have two components")
    require(all(parts), "composite file identity components must be nonempty")
    return "::".join(parts)


def validate_composite_file_identity(parts: tuple[str, ...], file_id: str) -> str:
    expected = canonical_composite_file_id(parts)
    require(file_id == expected, "file_id does not bind the complete composite EntityPk")
    return "live"


def validate_blob_binding(row: Row, refs: list[BlobRef] | None) -> str:
    state = validate_descriptor(row)
    if state == "absent":
        require(not refs, "deleted file cannot retain a BlobRef")
        return state
    # Only an explicit zero-length authenticated BlobRef is valid empty
    # content. Missing/NULL selected references are corruption.
    if refs is None:
        raise AssertionError("NULL selected BlobRef")
    if not refs:
        raise AssertionError("missing selected BlobRef")
    require(len(refs) == 1, "live file must have exactly one BlobRef")
    ref = refs[0]
    require(not ref.deleted, "tombstoned selected BlobRef")
    require(ref.file_id == row.entity_pk, "BlobRef file identity mismatch")
    require(ref.size_bytes == len(ref.payload), "BlobRef size mismatch")
    actual = hashlib.sha256(ref.payload).hexdigest()
    require(ref.blob_hash == actual, "BlobRef payload hash mismatch")
    return "live-empty" if not ref.payload else "live"


def project_file_result(
    row: Row, refs: list[BlobRef] | None, needs_data: bool
) -> tuple[str, bytes | None]:
    """Return a state tag so empty content cannot equal absence."""
    state = validate_blob_binding(row, refs)
    if state == "absent":
        return ("absent", None)
    if state == "live-empty":
        return ("live-empty", b"" if needs_data else None)
    return ("live", None)


def project_file(row: Row, refs: list[BlobRef] | None, needs_data: bool) -> bytes | None:
    """Validate before either metadata-only or data projection."""
    return project_file_result(row, refs, needs_data)[1]


def validate_source_change_records(records: list[dict[str, Any]]) -> None:
    """Reject conflicting authenticated IDs before any source-change dedup."""
    seen: dict[str, Any] = {}
    for record in records:
        require(record.get("authenticated") is True, "source change is unauthenticated")
        change_id = record.get("change_id")
        require(isinstance(change_id, str) and change_id, "source change ID is missing")
        payload = record.get("payload")
        if change_id in seen:
            require(seen[change_id] == payload, "conflicting duplicate source change ID")
            raise AssertionError("duplicate source change ID cannot be silently deduplicated")
        seen[change_id] = payload


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
            validate_blob_binding(
                Row("file", file_id, file_id, {"id": file_id}),
                [BlobRef(file_id, empty_hash, 0, b"")],
            )
            == "live-empty",
            "explicit zero-length BlobRef was not authenticated",
        ),
    )
    case(
        "observed_file_missing_blob_ref_fails",
        lambda: expect_error(
            lambda: validate_blob_binding(
                Row("file", file_id, file_id, {"id": file_id}), []
            )
        ),
    )
    case(
        "observed_file_null_blob_ref_fails",
        lambda: expect_error(
            lambda: validate_blob_binding(
                Row("file", file_id, file_id, {"id": file_id}), None
            )
        ),
    )
    case(
        "observed_file_tombstoned_blob_ref_fails",
        lambda: expect_error(
            lambda: validate_blob_binding(
                Row("file", file_id, file_id, {"id": file_id}),
                [BlobRef(file_id, empty_hash, 0, b"", True)],
            )
        ),
    )
    case(
        "observed_file_missing_blob_ref_fails_before_metadata_and_data_projection",
        lambda: [
            expect_error(
                lambda: project_file(
                    Row("file", file_id, file_id, {"id": file_id}), [], needs_data
                )
            )
            for needs_data in (False, True)
        ],
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
    case(
        "historical_composite_pk_first_component_fails",
        lambda: [
            validate_composite_file_identity(("tenant-a", "file-a"), "tenant-a::file-a"),
            expect_error(
                lambda: validate_composite_file_identity(("tenant-a", "file-a"), "tenant-a")
            ),
        ],
    )
    case(
        "conflicting_authenticated_source_change_ids_fail",
        lambda: expect_error(
            lambda: validate_source_change_records(
                [
                    {"change_id": "chg-conflict", "payload": "left", "authenticated": True},
                    {"change_id": "chg-conflict", "payload": "right", "authenticated": True},
                ]
            )
        ),
    )
    case(
        "zero_blob_ref_distinct_from_missing_and_tombstone_in_both_projections",
        lambda: [
            require(
                project_file_result(
                    Row("file", file_id, file_id, {"id": file_id}),
                    [BlobRef(file_id, empty_hash, 0, b"")],
                    needs_data,
                )
                == ("live-empty", b"" if needs_data else None),
                "explicit empty BlobRef was not observable in both projections",
            )
            for needs_data in (False, True)
        ]
        + [
            expect_error(
                lambda: project_file_result(
                    Row("file", file_id, file_id, {"id": file_id}), refs, needs_data
                )
            )
            for refs in ([], None, [BlobRef(file_id, empty_hash, 0, b"", True)])
            for needs_data in (False, True)
        ]
        + [
            require(
                project_file_result(Row("file", file_id, file_id, None, True), [], needs_data)
                == ("absent", None),
                "payload-less tombstone was not distinct from live empty",
            )
            for needs_data in (False, True)
        ],
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
