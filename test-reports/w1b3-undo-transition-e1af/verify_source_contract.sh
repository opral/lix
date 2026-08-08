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
PACKAGE_PREFIX = "test-reports/w1b3-undo-transition-e1af/"


def git(*args: str) -> str:
    return subprocess.check_output(["git", "-C", root, *args], text=True)


def source(path: str) -> str:
    return git("show", f"{target}:{path}")


def changed_paths() -> list[str]:
    output = git("diff", "--name-only", anchor, target)
    return [line for line in output.splitlines() if line]


def balanced_body(text: str, function_name: str) -> str:
    match = re.search(rf"\b(?:async\s+)?fn\s+{re.escape(function_name)}\b", text)
    if not match:
        return ""
    opening = text.find("{", match.end())
    if opening < 0:
        return ""
    depth = 0
    in_string = False
    escaped = False
    index = opening
    while index < len(text):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[match.start() : index + 1]
        index += 1
    return ""


def call_arguments(body: str, function_name: str) -> list[str]:
    calls = []
    for match in re.finditer(rf"\b{re.escape(function_name)}\s*\(", body):
        opening = body.find("(", match.start())
        depth = 0
        in_string = False
        escaped = False
        index = opening
        while index < len(body):
            char = body[index]
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    in_string = False
            elif char == '"':
                in_string = True
            elif char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    calls.append(body[opening + 1 : index])
                    break
            index += 1
    return calls


def has_exact_facade_constructor(body: str) -> bool:
    return bool(
        re.search(
            r"\blet\s+forktree_read\s*(?::\s*ForkTreeReadFacade(?:<[^;=]+>)?\s*)?="
            r"\s*ForkTreeReadFacade::from_opening_read\(\s*"
            r"transaction\.opening_read\(\s*\)\s*\)",
            body,
        )
    )


def forbidden_operation_tokens(body: str) -> list[str]:
    forbidden = [
        "begin_read(",
        "commit_graph_reader",
        "tracked_state_reader",
        "TrackedStateStoreReader",
        "StorageRead::",
        "read_store",
        ".refresh(",
        ".clone()",
        "fallback",
        "retry",
        "cache",
        "raw_store",
    ]
    return [token for token in forbidden if token in body]


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
undo_body = balanced_body(session, "undo_in_transaction")
redo_body = balanced_body(session, "redo_in_transaction")
transition_body = balanced_body(context, "execute_typed_state_transitions")

print(f"ANCHOR PASS target={target} anchor={anchor}")
print(f"SCOPE PASS changed_paths={len(paths)}")

reds = []

if "tracked_state_reader()" in session:
    reds.append("undo/redo still opens or uses the legacy tracked-state reader")
if "commit_graph_reader()" in session:
    reds.append("undo/redo still opens a fresh commit-graph reader")

facade_bodies = [undo_body, redo_body]
facade_count = sum(body.count("ForkTreeReadFacade::from_opening_read") for body in facade_bodies)
if facade_count != 2 or not all(has_exact_facade_constructor(body) for body in facade_bodies):
    reds.append("undo/redo lacks the retained ForkTree facade anchor")

if "execute_tracked_state_transition" in context and "tracked_state_reader().await" in context:
    reds.append("typed transitions still reload through the legacy tracked-state reader")
elif not transition_body or "ForkTreeReadFacade" not in transition_body or "forktree_read" not in transition_body:
    reds.append("typed transitions still reload through the legacy tracked-state reader")

# Preserve the exact four-predicate e1af calibration. Structural checks are
# additional only once a candidate has crossed the RED legacy-reader gate.
if not reds:
    closure_bodies = [("undo", undo_body), ("redo", redo_body), ("transition", transition_body)]
    for helper in [
        "semantic_state_at",
        "semantic_state_for_record",
        "operation_marker_at",
        "load_commit_delta",
        "load_node",
        "apply_state_diff",
    ]:
        helper_body = balanced_body(session, helper) or balanced_body(context, helper)
        if helper_body:
            closure_bodies.append((helper, helper_body))

    operation_bodies = closure_bodies[:3]
    for name, body in operation_bodies:
        tokens = forbidden_operation_tokens(body)
        if tokens:
            print(f"RED-STRUCT {name} forbidden read/authority tokens={','.join(tokens)}")
            raise SystemExit(1)
    for name, body in closure_bodies[3:]:
        if "ForkTreeReadFacade" not in body or "forktree_read" not in body:
            print(f"RED-STRUCT {name} is not bound to the retained facade")
            raise SystemExit(1)
        tokens = forbidden_operation_tokens(body)
        if tokens:
            print(f"RED-STRUCT {name} forbidden read/authority tokens={','.join(tokens)}")
            raise SystemExit(1)
    required_calls = [
        "semantic_state_at",
        "semantic_state_for_record",
        "operation_marker_at",
        "load_commit_delta",
        "load_node",
        "apply_state_diff",
    ]
    for name, body in [("undo", undo_body), ("redo", redo_body), ("transition", transition_body)]:
        for helper in required_calls:
            for arguments in call_arguments(body, helper):
                if "&forktree_read" not in arguments and "forktree_read" not in arguments:
                    print(f"RED-STRUCT {name} {helper} lacks the retained facade argument")
                    raise SystemExit(1)
    if "forktree_read: &ForkTreeReadFacade" not in transition_body:
        print("RED-STRUCT typed transition facade argument is not explicit")
        raise SystemExit(1)
    print("STRUCTURAL GREEN one retained opening read/facade and exact helper arguments")

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
