#!/usr/bin/env python3
"""Pure deterministic semantic fixtures for the FD2 closure contract."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from hashlib import sha256


class ModelError(Exception):
    pass


class Materialization(Enum):
    ABSENT = "absent"
    PRESENT = "present"


@dataclass(frozen=True)
class BlobRef:
    row_key: str
    snapshot_id: str
    descriptor_id: str
    file_id: str
    blob_id: str
    size: int
    tombstone: bool = False


@dataclass(frozen=True)
class DescriptorRow:
    row_key: str
    entity_pk: tuple[str, ...]
    file_id: str | None
    snapshot_id: str
    deleted: bool
    payload_id: str | None
    payload: bytes | None


def blob_id(data: bytes) -> str:
    return sha256(data).hexdigest()


def resolve_blob(
    refs: list[BlobRef],
    payloads: dict[str, bytes],
    expected: tuple[str, str, str, str],
) -> tuple[Materialization, bytes | None]:
    if len(refs) == 0:
        raise ModelError("missing/zero BlobRef")
    if len(refs) != 1:
        raise ModelError("duplicate BlobRef")
    ref = refs[0]
    if (ref.row_key, ref.snapshot_id, ref.descriptor_id, ref.file_id) != expected:
        raise ModelError("BlobRef identity substitution")
    if ref.tombstone:
        if ref.blob_id or ref.size:
            raise ModelError("tombstone carries live fields")
        return Materialization.ABSENT, None
    payload = payloads.get(ref.blob_id)
    if payload is None:
        raise ModelError("missing payload")
    if ref.size != len(payload) or ref.blob_id != blob_id(payload):
        raise ModelError("BlobId/size mismatch")
    return Materialization.PRESENT, payload


def resolve_descriptor(row: DescriptorRow, directory: bool) -> Materialization:
    if len(row.entity_pk) != 1 or row.entity_pk[0] != row.row_key:
        raise ModelError("EntityPk identity")
    if directory:
        if row.file_id is not None:
            raise ModelError("directory file_id must be NULL")
    elif row.file_id != row.row_key:
        raise ModelError("descriptor file_id identity")
    if row.deleted:
        if row.payload is not None:
            raise ModelError("payload-bearing tombstone")
        return Materialization.ABSENT
    if row.payload is None or row.payload_id != row.row_key:
        raise ModelError("missing/substituted descriptor payload")
    return Materialization.PRESENT


def resolve_owner(deleted: bool, payload: bytes | None) -> Materialization:
    if deleted:
        if payload is not None:
            raise ModelError("payload-bearing owner tombstone")
        return Materialization.ABSENT
    if payload is None:
        raise ModelError("missing owner payload")
    return Materialization.PRESENT


def select_single_pk(parts: tuple[str, ...]) -> str:
    if len(parts) != 1:
        raise ModelError("composite EntityPk cannot select a file")
    return parts[0]


def merge_source_changes(changes: list[tuple[str, bytes]]) -> list[tuple[str, bytes]]:
    merged: dict[str, bytes] = {}
    for change_id, content in changes:
        if change_id in merged and merged[change_id] != content:
            raise ModelError("conflicting duplicate source change")
        merged[change_id] = content
    return sorted(merged.items())


def expect_error(label: str, callback) -> None:
    try:
        callback()
    except ModelError:
        print(f"PASS {label}")
        return
    raise AssertionError(f"expected ModelError: {label}")


def main() -> int:
    expected = ("row-1", "snapshot-1", "file-1", "file-1")
    live_empty = BlobRef(*expected, blob_id(b""), 0)
    live_data = BlobRef(*expected, blob_id(b"data"), 4)
    tombstone = BlobRef(*expected, "", 0, tombstone=True)

    # 1: absence is not empty; 2: zero refs are corruption.
    status, data = resolve_blob([tombstone], {}, expected)
    assert (status, data) == (Materialization.ABSENT, None)
    print("PASS absence_is_not_empty")
    expect_error("zero_blobref_is_corrupt", lambda: resolve_blob([], {}, expected))

    # Valid live and explicit-empty controls.
    assert resolve_blob([live_data], {blob_id(b"data"): b"data"}, expected) == (
        Materialization.PRESENT,
        b"data",
    )
    assert resolve_blob([live_empty], {blob_id(b""): b""}, expected) == (
        Materialization.PRESENT,
        b"",
    )
    print("PASS authenticated_live_and_explicit_empty")

    # 3: descriptor EntityPk/file_id; 4: directory NULL file_id.
    descriptor = DescriptorRow("row-1", ("row-1",), "row-1", "snapshot-1", False, "row-1", b"x")
    assert resolve_descriptor(descriptor, False) is Materialization.PRESENT
    expect_error(
        "descriptor_entity_pk_file_id_binding",
        lambda: resolve_descriptor(
            DescriptorRow("row-1", ("row-1",), "other", "snapshot-1", False, "row-1", b"x"),
            False,
        ),
    )
    assert resolve_descriptor(
        DescriptorRow("dir-1", ("dir-1",), None, "snapshot-1", False, "dir-1", b"x"), True
    ) is Materialization.PRESENT
    expect_error(
        "directory_file_id_must_be_null",
        lambda: resolve_descriptor(
            DescriptorRow("dir-1", ("dir-1",), "dir-1", "snapshot-1", False, "dir-1", b"x"),
            True,
        ),
    )

    # 5, 6, 7: payload-bearing tombstones.
    for label, callback in (
        (
            "descriptor_tombstone_payload_rejected",
            lambda: resolve_descriptor(
                DescriptorRow("row-1", ("row-1",), "row-1", "snapshot-1", True, "row-1", b"x"),
                False,
            ),
        ),
        (
            "directory_tombstone_payload_rejected",
            lambda: resolve_descriptor(
                DescriptorRow("dir-1", ("dir-1",), None, "snapshot-1", True, "dir-1", b"x"),
                True,
            ),
        ),
        ("plugin_owner_tombstone_payload_rejected", lambda: resolve_owner(True, b"x")),
    ):
        expect_error(label, callback)

    # 8: never select the first component of a composite key.
    expect_error("composite_pk_first_component_rejected", lambda: select_single_pk(("file-1", "scope")))
    assert select_single_pk(("file-1",)) == "file-1"
    print("PASS exact_single_component_selection")

    # 9: equal duplicate is safe to collapse; conflicting duplicate fails.
    assert merge_source_changes([("c1", b"same"), ("c1", b"same")]) == [("c1", b"same")]
    expect_error(
        "conflicting_duplicate_source_change_fails_closed",
        lambda: merge_source_changes([("c1", b"left"), ("c1", b"right")]),
    )

    # Working-diff positive controls: authenticated live, NULL directory scope,
    # and payload-free tombstone are all valid model inputs.
    assert resolve_descriptor(
        DescriptorRow("file-1", ("file-1",), "file-1", "s", False, "file-1", b"x"), False
    ) is Materialization.PRESENT
    assert resolve_descriptor(
        DescriptorRow("file-1", ("file-1",), "file-1", "s", True, None, None), False
    ) is Materialization.ABSENT
    assert resolve_descriptor(
        DescriptorRow("dir-1", ("dir-1",), None, "s", True, None, None), True
    ) is Materialization.ABSENT
    print("PASS working_diff_live_and_tombstone_controls")
    print("MODEL_STATUS=GREEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
