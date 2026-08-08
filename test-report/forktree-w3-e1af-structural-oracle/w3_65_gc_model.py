#!/usr/bin/env python3
"""Pure W3 lifecycle, publication, root, cursor, and GC model.

This file is deliberately independent of Lix and every storage adapter.  It
models the invariants that a first runnable candidate must satisfy before the
Memory -> RocksDB -> SlateDB commands are admitted.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import hashlib
import json


class ModelError(RuntimeError):
    pass


@dataclass(frozen=True)
class Commit:
    commit_id: str
    parent: str | None
    generation: int


@dataclass(frozen=True)
class ObjectNode:
    object_id: str
    kind: str
    edges: tuple[tuple[str, str], ...] = ()


@dataclass
class Cursor:
    view_id: str
    keys: tuple[str, ...]
    next_index: int = 0
    poisoned: bool = False

    def next_page(
        self, limit: int, *, cancelled: bool = False, malformed: bool = False
    ) -> tuple[str, ...]:
        if self.poisoned:
            raise ModelError("poisoned cursor")
        if cancelled or malformed:
            self.poisoned = True
            raise ModelError("cursor terminal failure")
        if limit <= 0:
            self.poisoned = True
            raise ModelError("invalid page limit")
        page = self.keys[self.next_index : self.next_index + limit]
        self.next_index += len(page)
        return page


@dataclass
class ModelState:
    global_epoch: int = 1
    selector_owner: str = "owner-a"
    selector_generation: int = 1
    branch_snapshot: str = "commit-0"
    checkpoint_root: str | None = "checkpoint-0"
    recovery_root: str | None = "recovery-0"
    final_reference: str | None = "final-0"
    upload_roots: set[str] = field(default_factory=set)
    pins: dict[tuple[str, str], tuple[str, ...]] = field(default_factory=dict)
    queue: list[bool] = field(default_factory=list)
    queue_head: int = 0
    debt: int = 0
    progress: int = 0
    gc_epoch: int = 0
    objects: dict[str, ObjectNode] = field(default_factory=dict)
    commits: dict[str, Commit] = field(default_factory=dict)
    redo: tuple[str, ...] = ()

    def fingerprint(self) -> str:
        value = {
            "global_epoch": self.global_epoch,
            "selector_owner": self.selector_owner,
            "selector_generation": self.selector_generation,
            "branch_snapshot": self.branch_snapshot,
            "checkpoint_root": self.checkpoint_root,
            "recovery_root": self.recovery_root,
            "final_reference": self.final_reference,
            "upload_roots": sorted(self.upload_roots),
            "pins": sorted(
                (owner, view, sorted(roots))
                for (owner, view), roots in self.pins.items()
            ),
            "queue": self.queue,
            "queue_head": self.queue_head,
            "debt": self.debt,
            "progress": self.progress,
            "gc_epoch": self.gc_epoch,
            "objects": sorted(
                (object_id, node.kind, sorted(node.edges))
                for object_id, node in self.objects.items()
            ),
            "commits": sorted(
                (commit_id, commit.parent, commit.generation)
                for commit_id, commit in self.commits.items()
            ),
            "redo": self.redo,
        }
        return hashlib.sha256(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


def assert_unchanged(before: str, after: ModelState) -> None:
    assert before == after.fingerprint(), (before, after.fingerprint())


def validate_first_parent(
    commits: dict[str, Commit], tip: str, floor_generation: int = 0
) -> list[str]:
    if tip not in commits:
        raise ModelError("missing tip commit")
    path: list[str] = []
    seen: set[str] = set()
    current: str | None = tip
    child_generation: int | None = None
    while current is not None:
        if current in seen:
            raise ModelError("duplicate/cyclic first-parent commit")
        seen.add(current)
        commit = commits.get(current)
        if commit is None:
            raise ModelError("missing first-parent commit")
        if child_generation is not None and commit.generation >= child_generation:
            raise ModelError("non-decreasing generation")
        if commit.parent == current:
            raise ModelError("self-cycle in first-parent commit")
        if commit.parent is not None:
            parent = commits.get(commit.parent)
            if parent is None:
                raise ModelError("missing first-parent commit")
            if parent.generation >= commit.generation:
                raise ModelError("non-increasing parent generation")
        path.append(current)
        if commit.generation == floor_generation:
            break
        child_generation = commit.generation
        current = commit.parent
    if path and commits[path[-1]].generation < floor_generation:
        raise ModelError("history below checkpoint floor")
    return path


def validate_object_closure(
    objects: dict[str, ObjectNode], roots: tuple[tuple[str, str], ...]
) -> set[str]:
    reachable: set[str] = set()
    visiting: set[str] = set()

    def visit(object_id: str, expected_kind: str) -> None:
        if object_id in visiting:
            raise ModelError("cyclic object root")
        node = objects.get(object_id)
        if node is None:
            raise ModelError("missing object root")
        if node.kind != expected_kind:
            raise ModelError("wrong-kind or substituted object root")
        if object_id in reachable:
            return
        visiting.add(object_id)
        for child_id, child_kind in node.edges:
            visit(child_id, child_kind)
        visiting.remove(object_id)
        reachable.add(object_id)

    for object_id, kind in roots:
        visit(object_id, kind)
    return reachable


def publish(
    state: ModelState,
    *,
    expected_epoch: int,
    writer_owner: str,
    branch_snapshot: str,
    checkpoint_root: str | None,
    recovery_root: str | None,
    roots: tuple[tuple[str, str], ...],
) -> ModelState:
    if expected_epoch != state.global_epoch:
        raise ModelError("stale selector/epoch")
    if branch_snapshot not in state.commits:
        raise ModelError("missing or malformed selected commit")
    validate_first_parent(state.commits, branch_snapshot)
    if checkpoint_root is None or recovery_root is None:
        raise ModelError("missing required checkpoint/recovery root")
    validate_object_closure(state.objects, roots)
    next_state = deepcopy(state)
    next_state.selector_owner = writer_owner
    next_state.selector_generation += 1
    next_state.global_epoch += 1
    next_state.branch_snapshot = branch_snapshot
    next_state.checkpoint_root = checkpoint_root
    next_state.recovery_root = recovery_root
    next_state.progress += 1
    next_state.gc_epoch = next_state.global_epoch
    next_state.redo = ()
    return next_state


def try_publish(state: ModelState, **kwargs: object) -> tuple[ModelState, str | None]:
    before = state.fingerprint()
    try:
        return publish(state, **kwargs), None
    except ModelError as error:
        assert_unchanged(before, state)
        return state, str(error)


def reopen(state: ModelState, encoded: bytes) -> ModelState:
    expected = state.fingerprint().encode()
    if encoded != expected:
        raise ModelError("corrupt or substituted reopen fingerprint")
    return deepcopy(state)


def gc_step(state: ModelState, page: int = 64) -> tuple[bool, bool, int]:
    if state.queue_head == len(state.queue):
        return False, True, 0
    if state.queue[state.queue_head]:
        state.debt = 1
        return False, False, 0
    old = state.queue_head
    state.queue_head = min(old + page, len(state.queue))
    reclaimed = state.queue_head - old
    state.progress += 1
    state.gc_epoch += 1
    state.debt = 0
    return True, state.queue_head == len(state.queue), reclaimed


def gc_roots(state: ModelState) -> set[str]:
    roots: set[str] = set()
    for root in (state.checkpoint_root, state.recovery_root, state.final_reference):
        if root is not None:
            roots.add(root)
    roots.update(state.upload_roots)
    for pinned_roots in state.pins.values():
        roots.update(pinned_roots)
    return roots


def test_atomic_publication_races() -> None:
    base = ModelState(
        commits={"commit-0": Commit("commit-0", None, 0), "commit-1": Commit("commit-1", "commit-0", 1)},
        objects={
            "checkpoint-0": ObjectNode("checkpoint-0", "checkpoint"),
            "recovery-0": ObjectNode("recovery-0", "recovery"),
        },
    )
    kwargs = dict(
        expected_epoch=base.global_epoch,
        writer_owner="owner-a",
        branch_snapshot="commit-1",
        checkpoint_root="checkpoint-0",
        recovery_root="recovery-0",
        roots=(("checkpoint-0", "checkpoint"), ("recovery-0", "recovery")),
    )
    branch_first, error = try_publish(base, **kwargs)
    assert error is None and branch_first.global_epoch == 2
    stale_gc, error = try_publish(branch_first, **{**kwargs, "writer_owner": "gc-owner"})
    assert error == "stale selector/epoch" and stale_gc.fingerprint() == branch_first.fingerprint()
    unrelated, error = try_publish(branch_first, **{**kwargs, "expected_epoch": 2, "writer_owner": "other-branch"})
    assert error is None and unrelated.selector_owner == "other-branch"
    gc_first, error = try_publish(base, **{**kwargs, "writer_owner": "gc-owner"})
    assert error is None and gc_first.global_epoch == 2
    stale_branch, error = try_publish(gc_first, **kwargs)
    assert error == "stale selector/epoch" and stale_branch.fingerprint() == gc_first.fingerprint()
    print("PASS atomic_branch_first_gc_first_races")


def test_noop_savepoint_and_stale_owner() -> None:
    state = ModelState()
    before = state.fingerprint()
    noop = deepcopy(state)
    assert noop.fingerprint() == before
    savepoint = deepcopy(state)
    state.selector_generation += 1
    state.branch_snapshot = "temporary"
    state = savepoint
    assert_unchanged(before, state)
    stale, error = try_publish(
        state,
        expected_epoch=0,
        writer_owner="owner-a",
        branch_snapshot="missing",
        checkpoint_root="missing",
        recovery_root="missing",
        roots=(("missing", "checkpoint"),),
    )
    assert stale.fingerprint() == before and error == "stale selector/epoch"
    print("PASS noop_savepoint_rollback_stale_same_owner")


def test_first_parent_floors_and_corruption() -> None:
    commits = {
        "c0": Commit("c0", None, 0),
        "c1": Commit("c1", "c0", 1),
        "c2": Commit("c2", "c1", 2),
    }
    assert validate_first_parent(commits, "c2", floor_generation=1) == ["c2", "c1"]
    for bad, tip in (
        ({"c2": Commit("c2", "missing", 2)}, "c2"),
        ({"c0": Commit("c0", "c0", 0)}, "c0"),
        ({"c0": Commit("c0", None, 0), "c1": Commit("c1", "c0", 0)}, "c1"),
    ):
        try:
            validate_first_parent(bad, tip)
        except ModelError:
            pass
        else:
            raise AssertionError("corrupt chronology accepted")

    valid = {
        "root": ObjectNode("root", "checkpoint", (("leaf", "blob"),)),
        "leaf": ObjectNode("leaf", "blob"),
    }
    assert validate_object_closure(valid, (("root", "checkpoint"),)) == {"root", "leaf"}
    corruptions = [
        {},
        {"root": ObjectNode("root", "wrong", (("leaf", "blob"),)), "leaf": valid["leaf"]},
        {"root": ObjectNode("root", "checkpoint", (("substituted", "blob"),)), "leaf": valid["leaf"]},
        {"root": ObjectNode("root", "checkpoint", (("root", "checkpoint"),))},
    ]
    for objects in corruptions:
        try:
            validate_object_closure(objects, (("root", "checkpoint"),))
        except ModelError:
            pass
        else:
            raise AssertionError("corrupt root accepted")
    print("PASS first_parent_floor_and_authenticated_corruption")


def test_pins_and_transitive_roots() -> None:
    state = ModelState(
        checkpoint_root="checkpoint",
        recovery_root="recovery",
        final_reference="final",
        upload_roots={"upload"},
        objects={
            "checkpoint": ObjectNode("checkpoint", "checkpoint", (("shared", "blob"),)),
            "recovery": ObjectNode("recovery", "recovery", (("shared", "blob"),)),
            "final": ObjectNode("final", "root", (("shared", "blob"),)),
            "upload": ObjectNode("upload", "upload", (("shared", "blob"),)),
            "pinned": ObjectNode("pinned", "checkpoint", (("shared", "blob"),)),
            "shared": ObjectNode("shared", "blob"),
            "orphan": ObjectNode("orphan", "blob"),
        },
    )
    state.pins[("owner-a", "view-a")] = ("pinned",)
    root_types = {
        "checkpoint": "checkpoint",
        "recovery": "recovery",
        "upload": "upload",
        "pinned": "checkpoint",
    }
    reachable = validate_object_closure(
        state.objects,
        tuple((root, root_types[root]) for root in gc_roots(state) if root != "final"),
    )
    assert "shared" in reachable and "orphan" not in reachable
    before = state.fingerprint()
    try:
        state.pins.pop(("owner-b", "view-a"))
    except KeyError:
        assert_unchanged(before, state)
    else:
        raise AssertionError("cross-owner unpin accepted")
    state.pins.pop(("owner-a", "view-a"))
    assert "shared" in reachable  # checkpoint/final/upload still retain it
    state.final_reference = None
    state.checkpoint_root = None
    state.recovery_root = None
    state.upload_roots.clear()
    assert gc_roots(state) == set()
    print("PASS owner_view_pins_transitive_roots_final_reference")


def test_cursor_poison_and_exclusive_restart() -> None:
    cursor = Cursor("view-a", ("a", "b", "c"))
    assert cursor.next_page(2) == ("a", "b")
    try:
        cursor.next_page(1, cancelled=True)
    except ModelError:
        pass
    else:
        raise AssertionError("cancel did not poison cursor")
    try:
        cursor.next_page(1)
    except ModelError:
        pass
    else:
        raise AssertionError("poisoned cursor silently refreshed")
    fresh = Cursor("view-a", tuple(key for key in ("a", "b", "c") if key > "b"))
    assert fresh.next_page(10) == ("c",)
    print("PASS poisoned_cursor_explicit_exclusive_restart")


def test_65_entry_debt_and_reopen() -> None:
    state = ModelState(queue=[False] * 65)
    assert gc_step(state) == (True, False, 64)
    assert gc_step(state) == (True, True, 1)
    assert gc_step(state) == (False, True, 0)
    assert state.queue_head == 65 and state.progress == 2 and state.debt == 0

    blocked = ModelState(queue=[True, False])
    assert gc_step(blocked) == (False, False, 0)
    assert blocked.debt == 1 and blocked.progress == 0 and blocked.queue_head == 0
    blocked_after_first = blocked.fingerprint()
    assert gc_step(blocked) == (False, False, 0)
    assert blocked.fingerprint() == blocked_after_first
    blocked.queue[0] = False
    assert gc_step(blocked) == (True, True, 2)
    encoded = blocked.fingerprint().encode()
    assert reopen(blocked, encoded).fingerprint() == blocked.fingerprint()
    for bad in (b"missing", b"wrong-kind", encoded[:-1] + b"x"):
        try:
            reopen(blocked, bad)
        except ModelError:
            pass
        else:
            raise AssertionError("corrupt reopen accepted")
    print("PASS 65_entry_suffix_debt_no_spin_release_reopen")


def main() -> None:
    tests = [
        test_atomic_publication_races,
        test_noop_savepoint_and_stale_owner,
        test_first_parent_floors_and_corruption,
        test_pins_and_transitive_roots,
        test_cursor_poison_and_exclusive_restart,
        test_65_entry_debt_and_reopen,
    ]
    for test in tests:
        test()
    print(f"PASS model_tests={len(tests)}/6")


if __name__ == "__main__":
    main()
