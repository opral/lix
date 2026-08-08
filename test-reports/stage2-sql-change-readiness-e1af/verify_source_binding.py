#!/usr/bin/env python3
"""Small read-only binding gate; e1af is deliberately expected to be RED.

The frozen v4 verifier remains authoritative for balanced call arguments and
transitive closure. This binding adds the concrete providers/diff.py consumer
and the exact forbidden legacy routes it currently exposes.
"""

from __future__ import annotations

import pathlib
import sys


ALLOWED = {
    "packages/lix/src/sql2/context.rs",
    "packages/lix/src/sql2/providers/change.rs",
    "packages/lix/src/sql2/providers/diff.rs",
    "packages/lix/src/sql2/exec/datafusion.rs",
    "packages/lix/src/session/context.rs",
    "packages/lix/src/transaction/context.rs",
    "packages/lix/src/forktree/view.rs",
    "packages/lix/src/forktree/serving.rs",
    "packages/lix/src/forktree/mod.rs",
}


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: verify_source_binding.py CANDIDATE", file=sys.stderr)
        return 2
    root = pathlib.Path(sys.argv[1]).resolve()
    errors: list[str] = []

    def source(relative: str) -> str:
        path = root / relative
        if not path.is_file():
            errors.append(f"missing source path: {relative}")
            return ""
        return path.read_text(encoding="utf-8")

    context = source("packages/lix/src/sql2/context.rs")
    change = source("packages/lix/src/sql2/providers/change.rs")
    diff = source("packages/lix/src/sql2/providers/diff.rs")
    session = source("packages/lix/src/session/context.rs")
    transaction = source("packages/lix/src/transaction/context.rs")
    dummy = source("packages/lix/src/sql2/exec/datafusion.rs")

    if "forktree_reader" not in context:
        errors.append("ChangelogQuerySource has no forktree_reader field")
    if "&query_source.forktree_reader" not in change:
        errors.append("change provider does not bind both routes to query_source.forktree_reader")
    if "query_source.store" in change:
        errors.append("change provider still consumes query_source.store")
    if "query_source.store" in diff:
        errors.append("diff provider still consumes query_source.store")
    if "ForkTreeReadFacade::new(store)" in diff:
        errors.append("diff provider constructs a second facade from store")

    for label, text in (("change", change), ("diff", diff)):
        forbidden = (
            "tracked_state::scan_change_records_from_commit_deltas",
            "tracked_state::load_change_record_by_id",
            "COMMIT_CHANGE_ID_SPACE",
            "ChangelogContext::new().reader",
            "ChangelogReader",
            "ChangeScanRequest",
            "ChangeLoadRequest",
            "CommitGraphContext::new().reader",
            ".begin_read(",
        )
        for token in forbidden:
            if token in text:
                errors.append(f"{label} retains forbidden legacy token: {token}")

    for label, text in (("session", session), ("transaction", transaction), ("dummy", dummy)):
        starts = [
            index
            for index in range(len(text))
            if text.startswith("ChangelogQuerySource {", index)
        ]
        if not starts:
            errors.append(f"{label} has no ChangelogQuerySource constructor")
        for start in starts:
            block = text[start : start + 500]
            if "forktree_reader" not in block:
                errors.append(f"{label} changelog constructor lacks forktree_reader")
            if block.count("ForkTreeReadFacade::new") != 1:
                errors.append(
                    f"{label} changelog constructor must have exactly one ForkTreeReadFacade::new"
                )

    if errors:
        print("SOURCE_BINDING=RED")
        for error in errors:
            print(f"RED: {error}")
        return 1
    print("SOURCE_BINDING=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
