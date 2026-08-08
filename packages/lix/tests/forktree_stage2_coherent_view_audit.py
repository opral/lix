#!/usr/bin/env python3
"""Static, read-only Stage 2 coherent-view and authority-deletion audit.

The scanner reads immutable Git objects. It never imports or executes Lix and
does not need a runnable candidate. `--calibrate` reports findings without
failing; `--strict` exits non-zero on any blocker.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path


A12 = "a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3"
CBE = "cbe48835f6f07a21e0babf1ba16652a0c6b8a214"

LEGACY_SPACES = (
    "BINARY_CAS_MUTATION_EPOCH_SPACE",
    "BINARY_CAS_MANIFEST_SPACE",
    "BINARY_CAS_MANIFEST_CHUNK_SPACE",
    "BINARY_CAS_CHUNK_SPACE",
    "BINARY_CAS_CHUNK_PRESENCE_SPACE",
    "BRANCH_HEAD_CONTROL_SPACE",
    "CERTIFIED_ENTITY_BATCH_SPACE",
    "CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE",
    "CERTIFIED_ENTITY_BATCH_PAGE_SPACE",
    "CHANGE_SPACE",
    "CHECKPOINT_GC_STATE_SPACE",
    "CHECKPOINT_RECOVERY_REF_SPACE",
    "COMMIT_CHANGE_ID_SPACE",
    "COMMIT_SPACE",
    "CURRENT_STATE_DATA_PART_SPACE",
    "CURRENT_STATE_DATA_PART_REFS_SPACE",
    "GC_REACHABILITY_DELTA_SPACE",
    "GC_REACHABILITY_QUEUE_SPACE",
    "GC_TREE_SWEEP_CURSOR_SPACE",
    "GC_TREE_SWEEP_EPOCH_SPACE",
    "GC_TREE_SWEEP_MARK_SPACE",
    "HOT_COLLECTION_CONTROL_SPACE",
    "HOT_DIFF_SPACE",
    "HOT_FILE_SPACE",
    "HOT_ROW_SPACE",
    "MUTATION_DIRECTORY_NODE_SPACE",
    "PACKED_CURRENT_BASE_CONTROL_SPACE",
    "PACKED_CURRENT_BASE_SPACE",
    "PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE",
    "PLUGIN_CHECKPOINT_SPACE",
    "ROOT_CURRENT_BASE_SPACE",
    "ROW_GROUP_COLUMN_SPACE",
    "ROW_GROUP_MANIFEST_SPACE",
    "SCOPED_RANGE_NODE_SPACE",
    "TRACKED_STATE_CHANGE_LOCATOR_SPACE",
    "TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE",
    "TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE",
    "TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE",
    "TRACKED_STATE_TREE_CHUNK_SPACE",
    "TRACKED_WORKING_DIFF_MARKER_SPACE",
    "UPLOAD_MANIFEST_LEAF_SPACE",
    "UPLOAD_STATE_SPACE",
)

DELETED_OWNER_PATHS = (
    "packages/lix/src/binary_cas/chunking.rs",
    "packages/lix/src/binary_cas/codec.rs",
    "packages/lix/src/binary_cas/kv.rs",
    "packages/lix/src/binary_cas/stats.rs",
    "packages/lix/src/branch/control.rs",
    "packages/lix/src/changelog/codec.rs",
    "packages/lix/src/changelog/store.rs",
    "packages/lix/src/columnar_row_group.rs",
    "packages/lix/src/commit_graph/walker.rs",
    "packages/lix/src/live_state/tracked_head.rs",
    "packages/lix/src/live_state/tracked_head/hot.rs",
    "packages/lix/src/tracked_state/codec.rs",
    "packages/lix/src/tracked_state/commit_root_rebuild.rs",
    "packages/lix/src/tracked_state/current_state_data_part.rs",
    "packages/lix/src/tracked_state/current_state_envelope.rs",
    "packages/lix/src/tracked_state/mutation_directory.rs",
    "packages/lix/src/tracked_state/replacement_part.rs",
    "packages/lix/src/tracked_state/scoped_current_state.rs",
    "packages/lix/src/tracked_state/scoped_range.rs",
    "packages/lix/src/tracked_state/storage.rs",
    "packages/lix/src/tracked_state/tree.rs",
    "packages/lix/src/transaction/plugin_checkpoint.rs",
    "packages/lix/src/storage_adapter/scan.rs",
)

MODEL_SUBSTITUTION_MARKERS = (
    "forktree_stage2_recovery_no_lease",
    "forktree_bounded_gc_oracle",
    "frozen_oracle",
    "model_authority",
    "FTAUTH1",
    "packages/engine-benchmarks",
    "include!(concat!(env!(\"OUT_DIR\")",
)

TOPOLOGY_ROOTS = (
    "load_node",
    "load_nodes",
    "all_nodes",
    "reachable_nodes",
    "best_common_ancestors",
    "merge_base",
    "linear_merge_base",
    "load_linear_parent",
)

TOPOLOGY_FORBIDDEN = (
    "load_member_changes",
    "load_change",
    "page_changes",
    "ChangeObjectV1::decode",
    "load_commit_delta_members",
    "load_commit_delta_change_records",
    "load_commit_delta_replay_metadata",
    "load_commit_delta_members_with_payloads_for_schemas",
    "validate_retained_commit",
    "crate::changelog",
    "crate::tracked_state",
    "COMMIT_SPACE",
    "TRACKED_STATE_",
)


@dataclasses.dataclass(frozen=True)
class Function:
    path: str
    name: str
    signature: str
    body: str


@dataclasses.dataclass(frozen=True)
class Finding:
    name: str
    passed: bool
    detail: str


def git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.stdout


def resolve(repo: Path, ref: str) -> tuple[str, str]:
    return (
        git(repo, "rev-parse", ref).strip(),
        git(repo, "rev-parse", f"{ref}^{{tree}}").strip(),
    )


def paths(repo: Path, ref: str) -> set[str]:
    return set(git(repo, "ls-tree", "-r", "--name-only", ref).splitlines())


def read(repo: Path, ref: str, path: str) -> str:
    try:
        return git(repo, "show", f"{ref}:{path}")
    except subprocess.CalledProcessError:
        return ""


def production_prefix(source: str) -> str:
    match = re.search(r"#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]\s*mod\s+tests\b", source)
    return source[: match.start()] if match else source


def mask_non_code(source: str) -> str:
    """Mask comments and string/character literals while preserving offsets."""
    output = list(source)
    index = 0
    state = "code"
    raw_hashes = 0
    while index < len(source):
        if state == "code":
            if source.startswith("//", index):
                output[index : index + 2] = "  "
                index += 2
                state = "line"
            elif source.startswith("/*", index):
                output[index : index + 2] = "  "
                index += 2
                state = "block"
            elif source[index] == '"':
                output[index] = " "
                index += 1
                state = "string"
            elif source[index] == "'" and index + 2 < len(source):
                # Lifetimes are followed by identifiers; character literals
                # close within a few bytes and may contain an escape.
                end = index + 2 if source[index + 1] != "\\" else index + 3
                if end < len(source) and source[end] == "'":
                    for offset in range(index, end + 1):
                        output[offset] = " "
                    index = end + 1
                else:
                    index += 1
            elif source[index] == "r":
                raw = re.match(r'r(#{0,16})"', source[index:])
                if raw:
                    raw_hashes = len(raw.group(1))
                    length = raw.end()
                    output[index : index + length] = " " * length
                    index += length
                    state = "raw"
                else:
                    index += 1
            else:
                index += 1
        elif state == "line":
            if source[index] == "\n":
                state = "code"
            else:
                output[index] = " "
            index += 1
        elif state == "block":
            if source.startswith("*/", index):
                output[index : index + 2] = "  "
                index += 2
                state = "code"
            else:
                if source[index] != "\n":
                    output[index] = " "
                index += 1
        elif state == "string":
            if source[index] == "\\":
                output[index] = " "
                if index + 1 < len(source):
                    output[index + 1] = " "
                index += 2
            elif source[index] == '"':
                output[index] = " "
                index += 1
                state = "code"
            else:
                if source[index] != "\n":
                    output[index] = " "
                index += 1
        else:
            terminator = '"' + "#" * raw_hashes
            if source.startswith(terminator, index):
                output[index : index + len(terminator)] = " " * len(terminator)
                index += len(terminator)
                state = "code"
            else:
                if source[index] != "\n":
                    output[index] = " "
                index += 1
    return "".join(output)


def functions(path: str, source: str) -> list[Function]:
    source = production_prefix(source)
    masked = mask_non_code(source)
    found: list[Function] = []
    pattern = re.compile(r"\b(?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\b")
    for match in pattern.finditer(masked):
        brace = masked.find("{", match.end())
        semicolon = masked.find(";", match.end())
        if brace < 0 or (semicolon >= 0 and semicolon < brace):
            continue
        depth = 0
        end = brace
        while end < len(masked):
            if masked[end] == "{":
                depth += 1
            elif masked[end] == "}":
                depth -= 1
                if depth == 0:
                    end += 1
                    break
            end += 1
        found.append(
            Function(
                path=path,
                name=match.group(1),
                signature=masked[match.start() : brace],
                body=masked[brace:end],
            )
        )
    return found


def source_map(repo: Path, ref: str, selected: tuple[str, ...]) -> dict[str, str]:
    return {path: read(repo, ref, path) for path in selected}


def function_index(sources: dict[str, str]) -> dict[str, list[Function]]:
    output: dict[str, list[Function]] = {}
    for path, source in sources.items():
        for function in functions(path, source):
            output.setdefault(function.name, []).append(function)
    return output


def closure(index: dict[str, list[Function]], roots: tuple[str, ...]) -> list[Function]:
    queue = list(roots)
    seen_names: set[str] = set()
    seen_functions: set[tuple[str, str, str]] = set()
    output: list[Function] = []
    while queue:
        name = queue.pop(0)
        if name in seen_names:
            continue
        seen_names.add(name)
        for function in index.get(name, []):
            identity = (function.path, function.name, function.signature)
            if identity in seen_functions:
                continue
            seen_functions.add(identity)
            output.append(function)
            for candidate in index:
                if re.search(rf"\b{re.escape(candidate)}\s*\(", function.body):
                    queue.append(candidate)
    return output


def count_call(function: Function, name: str) -> int:
    return len(re.findall(rf"(?:\.|\b){re.escape(name)}\s*\(", function.body))


def space_definitions(repo: Path, ref: str) -> set[str]:
    result: set[str] = set()
    for path in paths(repo, ref):
        if not path.startswith("packages/lix/src/") or not path.endswith(".rs"):
            continue
        source = mask_non_code(production_prefix(read(repo, ref, path)))
        for name in LEGACY_SPACES:
            if re.search(rf"\b(?:const|static)\s+{re.escape(name)}\b", source):
                result.add(name)
    return result


def audit(repo: Path, baseline_ref: str, target_ref: str, profile: str) -> dict[str, object]:
    baseline, baseline_tree = resolve(repo, baseline_ref)
    target, target_tree = resolve(repo, target_ref)
    baseline_paths = paths(repo, baseline)
    target_paths = paths(repo, target)
    findings: list[Finding] = []

    selected = (
        "packages/lix/src/forktree/view.rs",
        "packages/lix/src/forktree/serving.rs",
        "packages/lix/src/forktree/blob.rs",
        "packages/lix/src/commit_graph/context.rs",
        "packages/lix/src/binary_cas/context.rs",
    )
    sources = source_map(repo, target, selected)
    index = function_index(sources)

    open_view = index.get("open_coherent_view", [])
    open_on_read = index.get("open_coherent_view_on_read", [])
    findings.append(
        Finding(
            "public-boundary-one-begin-read",
            len(open_view) == 1 and count_call(open_view[0], "begin_read") == 1,
            f"functions={len(open_view)} begin_read={sum(count_call(value, 'begin_read') for value in open_view)}",
        )
    )
    findings.append(
        Finding(
            "inner-view-zero-refresh",
            len(open_on_read) == 1
            and count_call(open_on_read[0], "begin_read") == 0
            and "read" in open_on_read[0].signature
            and "CoherentView" in open_on_read[0].signature,
            f"functions={len(open_on_read)} begin_read={sum(count_call(value, 'begin_read') for value in open_on_read)}",
        )
    )
    view_source = sources["packages/lix/src/forktree/view.rs"]
    findings.append(
        Finding(
            "view-owns-read-handle",
            bool(re.search(r"struct\s+CoherentView[^\{]*\{[^}]*\bread\s*:\s*R\b", mask_non_code(view_source), re.S)),
            "CoherentView must own read: R",
        )
    )

    serving_names = ("state_point", "state_range", "load_commit", "load_change", "page_commits", "page_changes")
    serving = [function for name in serving_names for function in index.get(name, [])]
    findings.append(
        Finding(
            "serving-same-coherent-view",
            len(serving) == len(serving_names)
            and all("CoherentView" in function.signature and count_call(function, "begin_read") == 0 for function in serving),
            f"found={','.join(sorted(function.name for function in serving))}",
        )
    )

    topology = closure(index, TOPOLOGY_ROOTS)
    topology_text = "\n".join(function.signature + function.body for function in topology)
    nested_reads = sum(count_call(function, "begin_read") for function in topology)
    forbidden_hits = sorted(token for token in TOPOLOGY_FORBIDDEN if token in topology_text)
    context_source = mask_non_code(sources["packages/lix/src/commit_graph/context.rs"])
    findings.append(
        Finding(
            "topology-zero-snapshot-refresh",
            bool(topology) and nested_reads == 0,
            f"functions={len(topology)} begin_read={nested_reads}",
        )
    )
    findings.append(
        Finding(
            "topology-same-coherent-view",
            bool(topology)
            and "CoherentView" in context_source
            and "crate::forktree" in context_source,
            "commit graph must borrow the ForkTree CoherentView rather than a raw/legacy store",
        )
    )
    findings.append(
        Finding(
            "topology-zero-member-payload",
            bool(topology) and not forbidden_hits,
            "hits=" + (",".join(forbidden_hits) if forbidden_hits else "none"),
        )
    )
    findings.append(
        Finding(
            "topology-dedupes-parent-loads",
            "node_cache" in context_source
            and ("BTreeSet" in context_source or ".dedup()" in context_source)
            and "topology_reads_do_not_load_commit_member_payloads" in sources["packages/lix/src/commit_graph/context.rs"],
            "requires node cache, input de-duplication, and non-vacuous topology regression",
        )
    )

    history = closure(index, ("change_history_from_commit", "load_member_changes"))
    history_text = "\n".join(function.signature + function.body for function in history)
    serving_source = mask_non_code(sources["packages/lix/src/forktree/serving.rs"])
    history_calls_change = bool(re.search(r"\b(?:load_change|page_changes)\s*\(", history_text))
    back_edge = all(
        token in serving_source
        for token in ("ChangeCatalogOwner::CommitMember", "member_change_object_ids", "ordinal")
    )
    findings.append(
        Finding(
            "history-authenticates-members-back-edges",
            history_calls_change and back_edge and "load_commit_delta_members_with_payloads_for_schemas" not in history_text,
            f"history_functions={len(history)} fork_tree_change_load={history_calls_change} back_edge={back_edge}",
        )
    )

    if profile == "blob":
        blob_functions = functions("packages/lix/src/forktree/blob.rs", sources["packages/lix/src/forktree/blob.rs"])
        blob_reads = [
            function
            for function in blob_functions
            if re.search(r"(?:load|read|authenticate).*(?:blob|manifest|chunk)|(?:blob|manifest|chunk).*(?:load|read|authenticate)", function.name)
        ]
        blob_text = "\n".join(function.signature + function.body for function in blob_reads)
        findings.append(
            Finding(
                "blob-reader-same-view-authentication",
                bool(blob_reads)
                and all(count_call(function, "begin_read") == 0 for function in blob_reads)
                and "CoherentView" in blob_text
                and "decode" in blob_text,
                f"functions={','.join(sorted(function.name for function in blob_reads)) or 'none'}",
            )
        )

    baseline_deleted = baseline_paths.intersection(DELETED_OWNER_PATHS)
    target_deleted = target_paths.intersection(DELETED_OWNER_PATHS)
    findings.append(
        Finding(
            "deleted-owner-modules-do-not-reappear",
            not (target_deleted - baseline_deleted),
            f"baseline={len(baseline_deleted)} target={len(target_deleted)} new="
            + (",".join(sorted(target_deleted - baseline_deleted)) or "none"),
        )
    )
    baseline_spaces = space_definitions(repo, baseline)
    target_spaces = space_definitions(repo, target)
    findings.append(
        Finding(
            "legacy-space-definitions-do-not-reappear",
            not (target_spaces - baseline_spaces),
            f"baseline={len(baseline_spaces)} target={len(target_spaces)} new={','.join(sorted(target_spaces - baseline_spaces)) or 'none'}",
        )
    )

    changed = git(repo, "diff", "--name-only", f"{baseline}..{target}").splitlines()
    changed_production = [
        path for path in changed if path.startswith("packages/lix/src/") and path.endswith(".rs")
    ]
    substitutions: list[str] = []
    for path in changed_production:
        source = production_prefix(read(repo, target, path))
        for marker in MODEL_SUBSTITUTION_MARKERS:
            if marker in source:
                substitutions.append(f"{path}:{marker}")
    findings.append(
        Finding(
            "no-benchmark-model-substitution",
            not substitutions,
            "hits=" + (",".join(substitutions) if substitutions else "none"),
        )
    )

    payload = {
        "schema": 1,
        "profile": profile,
        "baseline": baseline,
        "baseline_tree": baseline_tree,
        "target": target,
        "target_tree": target_tree,
        "changed_production_files": len(changed_production),
        "findings": [dataclasses.asdict(finding) for finding in findings],
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    payload["evidence_sha256"] = hashlib.sha256(canonical).hexdigest()
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--baseline", default=CBE)
    parser.add_argument("--target", required=True)
    parser.add_argument("--profile", choices=("topology", "blob"), default="topology")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--calibrate", action="store_true")
    mode.add_argument("--strict", action="store_true")
    arguments = parser.parse_args()

    payload = audit(arguments.repo.resolve(), arguments.baseline, arguments.target, arguments.profile)
    print(json.dumps(payload, sort_keys=True, indent=2))
    failed = [finding for finding in payload["findings"] if not finding["passed"]]
    if arguments.strict and failed:
        for finding in failed:
            print(f"BLOCKER\t{finding['name']}\t{finding['detail']}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
