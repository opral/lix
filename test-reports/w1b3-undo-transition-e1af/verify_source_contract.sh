#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 <git-worktree> <target-commit-or-ref> [anchor-commit]" >&2
  exit 2
}

[[ $# -ge 2 && $# -le 3 ]] || usage
root=$1
target=$2
anchor=${3:-e1af471b9ab0f598dafa7c2ddec7867667c81740}

git -C "$root" rev-parse --is-inside-work-tree >/dev/null
target_commit=$(git -C "$root" rev-parse "$target^{commit}")
anchor_commit=$(git -C "$root" rev-parse "$anchor^{commit}")

if [[ "$anchor_commit" != e1af471b9ab0f598dafa7c2ddec7867667c81740 ]]; then
  echo "BLOCKER anchor is not exact e1af: $anchor_commit" >&2
  exit 2
fi

python3 - "$root" "$target_commit" "$anchor_commit" <<'PY'
import re
import subprocess
import sys

root, target, anchor = sys.argv[1:]

PRODUCTION_ALLOWLIST = {
    "packages/lix/src/session/undo_redo.rs",
    "packages/lix/src/transaction/context.rs",
    "packages/lix/src/forktree/view.rs",
    "packages/lix/src/forktree/serving.rs",
    "packages/lix/src/forktree/tests.rs",
}
FORKTREE_PATHS = {
    "packages/lix/src/forktree/view.rs",
    "packages/lix/src/forktree/serving.rs",
    "packages/lix/src/forktree/tests.rs",
}
PACKAGE_PREFIX = "test-reports/w1b3-undo-transition-e1af/"


def git(*args: str) -> str:
    return subprocess.check_output(["git", "-C", root, *args], text=True)


def source(path: str) -> str:
    return git("show", f"{target}:{path}")


def changed_paths() -> list[str]:
    output = git("diff", "--name-only", anchor, target)
    return [line for line in output.splitlines() if line]


def added_lines(path: str) -> list[str]:
    output = git("diff", "--unified=0", anchor, target, "--", path)
    return [
        line[1:]
        for line in output.splitlines()
        if line.startswith("+") and not line.startswith("+++")
    ]


def mask_non_code(text: str) -> str:
    """Mask comments and literals while preserving delimiters and newlines."""
    output = []
    index = 0
    block_depth = 0
    while index < len(text):
        if block_depth:
            if text.startswith("/*", index):
                block_depth += 1
                output.extend("  ")
                index += 2
            elif text.startswith("*/", index):
                block_depth -= 1
                output.extend("  ")
                index += 2
            else:
                output.append("\n" if text[index] == "\n" else " ")
                index += 1
            continue
        if text.startswith("//", index):
            output.extend("  ")
            index += 2
            while index < len(text) and text[index] != "\n":
                output.append(" ")
                index += 1
            continue
        raw = re.match(r"(?:br|r)(#+)\"", text[index:])
        if raw:
            hashes = raw.group(1)
            end = "\"" + hashes
            output.extend(" " * len(raw.group(0)))
            index += len(raw.group(0))
            while index < len(text) and not text.startswith(end, index):
                output.append("\n" if text[index] == "\n" else " ")
                index += 1
            if index < len(text):
                output.extend(" " * len(end))
                index += len(end)
            continue
        if text[index] in {'"', "'"}:
            quote = text[index]
            output.append(" ")
            index += 1
            escaped = False
            while index < len(text):
                char = text[index]
                output.append("\n" if char == "\n" else " ")
                index += 1
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == quote:
                    break
            continue
        output.append(text[index])
        index += 1
    return "".join(output)


def balanced_region(text: str, function_name: str) -> tuple[str, str, str]:
    masked = mask_non_code(text)
    match = re.search(rf"\b(?:async\s+)?fn\s+{re.escape(function_name)}\b", masked)
    if not match:
        return "", "", ""
    opening = masked.find("{", match.end())
    if opening < 0:
        return "", "", ""
    depth = 0
    index = opening
    while index < len(masked):
        if masked[index] == "{":
            depth += 1
        elif masked[index] == "}":
            depth -= 1
            if depth == 0:
                signature = text[match.start() : opening]
                body = text[opening + 1 : index]
                return signature, body, masked[opening + 1 : index]
        index += 1
    return "", "", ""


def matching_call_arguments(masked_body: str, function_name: str) -> list[str]:
    calls = []
    for match in re.finditer(rf"\b{re.escape(function_name)}\s*\(", masked_body):
        opening = masked_body.find("(", match.start())
        depth = 0
        index = opening
        while index < len(masked_body):
            if masked_body[index] == "(":
                depth += 1
            elif masked_body[index] == ")":
                depth -= 1
                if depth == 0:
                    calls.append(masked_body[opening + 1 : index])
                    break
            index += 1
    return calls


def facade_parameter(signature: str) -> bool:
    return bool(
        re.search(
            r"\bforktree_read\s*:\s*&\s*(?:crate::forktree::)?ForkTreeReadFacade\b",
            mask_non_code(signature),
        )
    )


def exact_facade_constructor(body_code: str) -> str:
    return (
        r"ForkTreeReadFacade::from_opening_read\s*\(\s*"
        r"transaction\.opening_read\s*\(\s*\)\s*\)"
    )


def forbidden_operation_tokens(body_code: str) -> list[str]:
    forbidden = [
        "begin_read(",
        "commit_graph_reader",
        "tracked_state_reader",
        "TrackedStateStoreReader",
        "StorageRead::",
        "read_store",
        ".refresh(",
        "fallback",
        "retry",
        "cache",
        "raw_store",
        "forktree_read.clone()",
        "opening_read().clone()",
        "ForkTreeReadFacade::new",
    ]
    return [token for token in forbidden if token in body_code]


def aliases_facade(body_code: str) -> list[str]:
    aliases = []
    aliases.extend(
        re.findall(r"\blet\s+(\w+)\s*=\s*(?:&\s*)?forktree_read\b", body_code)
    )
    aliases.extend(
        re.findall(
            r"\blet\s+(\w+)\s*:\s*[^=;]*ForkTreeReadFacade\b", body_code
        )
    )
    if "forktree_read.clone" in body_code:
        aliases.append("forktree_read.clone")
    return aliases


paths = changed_paths()
for path in paths:
    if path.startswith(PACKAGE_PREFIX):
        continue
    if path in PRODUCTION_ALLOWLIST:
        continue
    print(f"RED-SCOPE forbidden candidate path: {path}")
    raise SystemExit(1)

session = source("packages/lix/src/session/undo_redo.rs")
context = source("packages/lix/src/transaction/context.rs")
undo_signature, undo_body, undo_code = balanced_region(session, "undo_in_transaction")
redo_signature, redo_body, redo_code = balanced_region(session, "redo_in_transaction")
transition_signature, transition_body, transition_code = balanced_region(
    context, "execute_typed_state_transitions"
)

print(f"ANCHOR PASS target={target} anchor={anchor}")
print(f"SCOPE PASS changed_paths={len(paths)}")

reds = []

if "tracked_state_reader()" in session:
    reds.append("undo/redo still opens or uses the legacy tracked-state reader")
if "commit_graph_reader()" in session:
    reds.append("undo/redo still opens a fresh commit-graph reader")

facade_bodies = [undo_code, redo_code]
facade_count = sum(body.count("ForkTreeReadFacade::from_opening_read") for body in facade_bodies)
if facade_count != 2 or not all(
    re.search(exact_facade_constructor(body), body) for body in facade_bodies
):
    reds.append("undo/redo lacks the retained ForkTree facade anchor")

if "execute_tracked_state_transition" in context and "tracked_state_reader().await" in context:
    reds.append("typed transitions still reload through the legacy tracked-state reader")
elif not transition_code or "ForkTreeReadFacade" not in transition_code or "forktree_read" not in transition_code:
    reds.append("typed transitions still reload through the legacy tracked-state reader")

# Preserve the exact four-predicate e1af calibration. Structural checks are
# additional only once those legacy-reader predicates are absent.
if not reds:
    helper_names = [
        "semantic_state_at",
        "semantic_state_for_record",
        "operation_marker_at",
        "load_commit_delta",
        "load_node",
        "apply_state_diff",
    ]
    closure = [("undo", undo_signature, undo_code), ("redo", redo_signature, redo_code)]
    for helper in helper_names:
        signature, _body, code = balanced_region(session, helper)
        if not signature:
            print(f"RED-STRUCT missing helper function {helper}")
            raise SystemExit(1)
        closure.append((helper, signature, code))
    closure.append(("transition", transition_signature, transition_code))

    for name, signature, code in closure:
        if name not in {"undo", "redo"} and not facade_parameter(signature):
            print(f"RED-STRUCT {name} lacks explicit caller-owned facade parameter")
            raise SystemExit(1)
        tokens = forbidden_operation_tokens(code)
        if tokens:
            print(f"RED-STRUCT {name} forbidden read/authority tokens={','.join(tokens)}")
            raise SystemExit(1)
        aliases = aliases_facade(code)
        if aliases:
            print(f"RED-STRUCT {name} facade aliases or clones={','.join(aliases)}")
            raise SystemExit(1)
        if "ForkTreeReadFacade::" in code:
            remainder = re.sub(exact_facade_constructor(code), "", code)
            if "ForkTreeReadFacade::" in remainder:
                print(f"RED-STRUCT {name} constructs a second facade")
                raise SystemExit(1)

    required_calls = helper_names + ["execute_tracked_state_transition"]
    for name, _signature, code in closure:
        for helper in required_calls:
            for arguments in matching_call_arguments(code, helper):
                if not re.search(r"\bforktree_read\b", arguments):
                    print(f"RED-STRUCT {name} {helper} lacks retained facade argument")
                    raise SystemExit(1)
    if "forktree_read" not in transition_signature or not facade_parameter(transition_signature):
        print("RED-STRUCT typed transition facade argument is not explicit")
        raise SystemExit(1)

    for path in FORKTREE_PATHS:
        additions = "\n".join(added_lines(path))
        additions_code = mask_non_code(additions)
        forbidden_additions = [
            "begin_read(",
            "commit_graph_reader",
            "tracked_state_reader",
            "TrackedStateStoreReader",
            "StorageRead::",
            "read_store",
            "raw_store",
            "ForkTreeReadFacade::from_opening_read",
            "ForkTreeReadFacade::new",
        ]
        hits = [token for token in forbidden_additions if token in additions_code]
        if hits:
            print(f"RED-STRUCT {path} new reader/authority tokens={','.join(hits)}")
            raise SystemExit(1)

    for path in PRODUCTION_ALLOWLIST:
        additions_code = mask_non_code("\n".join(added_lines(path)))
        if path != "packages/lix/src/session/undo_redo.rs" and "ForkTreeReadFacade::from_opening_read" in additions_code:
            print(f"RED-STRUCT {path} adds an opening facade outside undo/redo")
            raise SystemExit(1)
        allowed_code = re.sub(exact_facade_constructor(additions_code), "", additions_code)
        forbidden_additions = [
            "begin_read(",
            "commit_graph_reader",
            "tracked_state_reader",
            "TrackedStateStoreReader",
            "StorageRead::",
            "read_store",
            "raw_store",
            "ForkTreeReadFacade::new",
        ]
        hits = [token for token in forbidden_additions if token in allowed_code]
        if hits:
            print(f"RED-STRUCT {path} new reader/authority tokens={','.join(hits)}")
            raise SystemExit(1)

    session_constructor_count = mask_non_code(session).count(
        "ForkTreeReadFacade::from_opening_read"
    )
    if session_constructor_count != 2:
        print(
            "RED-STRUCT undo/redo constructor count is not exactly two: "
            f"{session_constructor_count}"
        )
        raise SystemExit(1)

    for path in PRODUCTION_ALLOWLIST:
        target_code = mask_non_code(source(path))
        if path not in {
            "packages/lix/src/session/undo_redo.rs",
            "packages/lix/src/transaction/context.rs",
        } and "ForkTreeReadFacade::from_opening_read" in target_code:
            print(f"RED-STRUCT {path} contains an extra opening facade")
            raise SystemExit(1)

    print("STRUCTURAL GREEN one opening facade, propagated helper arguments, and no alternate authority")

positive_anchors = [
    (session, "async fn undo_in_transaction", "undo state-machine entry anchor"),
    (session, "async fn redo_in_transaction", "redo state-machine entry anchor"),
    (session, "apply_state_diff", "inverse/replay transition anchor"),
    (context, "execute_typed_state_transitions", "typed atomic staging anchor"),
    (session, "CHECKPOINT_MARKER_SCHEMA_KEY", "checkpoint-floor marker anchor"),
]
for text, token, label in positive_anchors:
    if token not in text:
        print(f"RED-STRUCT missing {label}")
        raise SystemExit(1)

if reds:
    for index, message in enumerate(reds, start=1):
        print(f"RED-{index} {message}")
    print(f"EXPECTED-RED predicates={len(reds)} target={target}")
    raise SystemExit(1)

print("GREEN undo/redo and typed-transition structural reader predicates pass")
PY
