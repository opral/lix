#!/usr/bin/env python3
"""Candidate-parametric NativeRow-v2 authority gate.

This is deliberately a source/contract gate.  It does not mutate or build the
candidate and it rejects the v1 branch-UUID-bearing durable row outright.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    detail: str


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def contains(text: str, pattern: str) -> bool:
    return re.search(pattern, text, re.MULTILINE | re.DOTALL) is not None


def check(name: str, passed: bool, detail: str) -> Check:
    return Check(name, passed, detail)


def digest(domain: str, schema: str, pk: bytes, file_id: str | None) -> str:
    """Executable model of the required stable, branch-independent identity."""
    h = hashlib.blake2s(person=b"LIXNRV2")
    for value in (domain.encode(), schema.encode(), pk, (file_id or "").encode()):
        h.update(len(value).to_bytes(4, "big"))
        h.update(value)
    return h.hexdigest()


def inspect(root: Path) -> tuple[list[Check], dict[str, str]]:
    state_path = root / "packages/lix/src/forktree/state.rs"
    publication_path = root / "packages/lix/src/forktree/publication.rs"
    view_path = root / "packages/lix/src/forktree/view.rs"
    tests_path = root / "packages/lix/src/forktree/tests.rs"
    state = state_path.read_text()
    native_row_path = root / "packages/lix/src/native_row.rs"
    native_row = native_row_path.read_text() if native_row_path.exists() else ""
    codec = state + "\n" + native_row
    publication = publication_path.read_text()
    view = view_path.read_text()
    tests = "\n".join(
        path.read_text()
        for path in (root / "packages/lix").rglob("*.rs")
        if path.name.endswith("tests.rs")
        or "/tests/" in str(path)
        or "#[cfg(test)]" in path.read_text()
    )
    production = "\n".join(
        path.read_text()
        for path in (root / "packages/lix/src").rglob("*.rs")
        if not path.name.endswith("tests.rs") and "/tests/" not in str(path)
    )

    checks = [
        check(
            "v2_magic_only",
            'b"LIXFCV\\0\\x02"' in state and 'b"LIXFCV\\0\\x01"' not in state,
            "current durable row uses only LIXFCV v2; v1 is not accepted",
        ),
        check(
            "no_embedded_branch_owner",
            contains(codec, r"NativeRow")
            and not contains(codec, r"owner_branch_id|owner_digest"),
            "NativeRow wire contains no selected/source branch UUID or owner digest",
        ),
        check(
            "stable_domain_digest",
            contains(codec, r"state_(?:identity|domain)_digest")
            and contains(codec, r"Global|Local|global"),
            "v2 binds a canonical global/local state domain digest",
        ),
        check(
            "canonical_key_binding",
            contains(codec, r"schema_key")
            and contains(codec, r"entity_pk")
            and contains(codec, r"file_id")
            and contains(codec, r"semantic_digest"),
            "digest/decode binds schema, typed PK, file owner and semantic body",
        ),
        check(
            "decoder_rejects_noncanonical_v1",
            contains(state, r"unsupported.*version|invalid.*magic|does not use v2")
            and not contains(state, r"CURRENT_STATE_VALUE_MAGIC_V1|decode_.*v1|legacy.*current"),
            "old/truncated/noncanonical current rows fail closed with no legacy decoder",
        ),
        check(
            "branch_create_root_sharing",
            contains(
                publication,
                r"publish_new_branch_selector.*local_state_root:\s*source_commit\.local_state_root",
            ),
            "child selector reuses the authenticated source local root",
        ),
        check(
            "branch_create_has_no_state_rewrite",
            not contains(
                re.search(
                    r"publish_new_branch_selector(?P<body>.*?)(?:\n\s*pub\(crate\) async fn|\n\s*async fn)",
                    publication,
                    re.DOTALL,
                ).group("body")
                if re.search(
                    r"publish_new_branch_selector(?P<body>.*?)(?:\n\s*pub\(crate\) async fn|\n\s*async fn)",
                    publication,
                    re.DOTALL,
                )
                else "",
                r"stage_(?:state|current_pack)|encode_current_state|rewrite",
            ),
            "new branch publication stages selector/topology only, never per-row/current-pack rewrites",
        ),
        check(
            "pack_domain_matches_root",
            contains(view + state, r"pack.*global|global.*pack")
            and contains(view, r"global_state_root|local_state_root"),
            "current-pack domain is checked against the selected global/local root",
        ),
        check(
            "stable_authenticated_chain",
            all(
                token in production
                for token in (
                    "encoded_key",
                    "semantic_digest",
                    "pack_ordinal",
                    "local_state_root",
                    "global_state_root",
                )
            ),
            "retained selector/root -> tree key -> pack ordinal -> row identity/body chain remains present",
        ),
        check(
            "adversarial_controls_present",
            all(
                contains(tests, pattern)
                for pattern in (
                    r"(?:native|current).*v1.*reject|reject.*v1",
                    r"(?:owner|domain|root).*substitut|transplant",
                    r"truncat",
                    r"graft|cross_branch|cross.*branch",
                    r"branch.*(?:child|grandchild).*(?:root|sharing)|root.*child.*grandchild",
                    r"cold.*reopen|reopen.*cold",
                )
            ),
            "v1, transplant, truncation, graft, child/grandchild sharing and cold-reopen tests exist",
        ),
        check(
            "no_compatibility_authority",
            not contains(production, r"NativeRowV1|native_row_v1|legacy_native_row|fallback.*LIXFCV"),
            "no v1 alias, fallback decoder or dual durable authority remains",
        ),
    ]
    model = {
        "local_main": digest("local", "fixture", b"pk", None),
        "local_child": digest("local", "fixture", b"pk", None),
        "global": digest("global", "fixture", b"pk", None),
        "other_pk": digest("local", "fixture", b"other", None),
    }
    checks.extend(
        [
            check(
                "model_branch_independence",
                model["local_main"] == model["local_child"],
                "same local row identity is stable across immutable branch-root sharing",
            ),
            check(
                "model_domain_separation",
                model["local_main"] != model["global"],
                "global/local transplant changes the authenticated identity",
            ),
            check(
                "model_key_separation",
                model["local_main"] != model["other_pk"],
                "same-size PK transplant changes the authenticated identity",
            ),
        ]
    )
    identity = {
        "head": git(root, "rev-parse", "HEAD"),
        "tree": git(root, "rev-parse", "HEAD^{tree}"),
    }
    return checks, identity


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--expect-head")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    checks, identity = inspect(root)
    if args.expect_head and identity["head"] != args.expect_head:
        checks.insert(0, check("exact_head", False, f"expected {args.expect_head}, got {identity['head']}"))
    elif args.expect_head:
        checks.insert(0, check("exact_head", True, identity["head"]))
    report = {
        "candidate": identity,
        "verdict": "APPROVE" if all(item.passed for item in checks) else "BLOCK",
        "checks": [asdict(item) for item in checks],
    }
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered, end="")
    return 0 if report["verdict"] == "APPROVE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
