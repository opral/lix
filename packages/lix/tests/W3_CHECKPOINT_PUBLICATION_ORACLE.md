# W3 checkpoint/snapshot-pin acceptance oracle

This is test/report-only material anchored to the exact non-runnable frontier
`1f742a382c755399b8a49ab536c4f6dc55fffdd8`. It does not lower production
checkpoint publication and does not implement W4 GC/recovery or W5 writers.

## Acceptance shape

Every accepted checkpoint/snapshot-pin transaction must use the existing
transaction path exactly once:

1. Open one caller-owned `CoherentView`.
2. Prepare one `PreparedPublication` and call `into_storage_plan` once.
3. Append branch/global selector, checkpoint, recovery, runtime, idempotency,
   catalog, and revision effects to that same transaction write set.
4. Call the existing `prepare_write_set` once.
5. Call the existing boundary commit once.

No independent `PreparedPublication::commit`, checkpoint writer, recovery
writer, retry publication, second coherent read, cache, format, or compatibility
path is accepted. W4 sweep/recovery GC remains out of scope.

## Required positive cases

- ordered selected history members, intermediate commits, and parent override;
- checkpoint and recovery references in the same atomic batch;
- branch-first and GC-first races, with stale retry from a fresh view;
- same-owner stale selector/recovery rejection and unrelated-owner handling;
- true no-op, savepoint/rollback, and unsupported zero-write behavior;
- duplicate, out-of-order, ordinal, back-edge, wrong-parent, and missing-parent
  corruption fail closed with no partial publication;
- cold reopen preserves branch, checkpoint, recovery, and user state.

The Rust integration module is intentionally future-facing: its public API
checkpoint/reopen test must be green only after W3 is present. The pure model
tests encode the one-view/one-plan/one-prepare/one-commit and race invariants.

## Discriminating red control

On the pinned frontier, `packages/lix/src/transaction/commit.rs` must still
contain the rejection:

`checkpoint publication requires the ForkTree snapshot-root lowering slice`

Run the source gate with `--expect-red` on `1f742a382…`; a W3 candidate must
run the same gate with `--expect-green`. This prevents a green result caused
by silently accepting the pre-W3 checkpoint path.

## Commands

```text
git diff --check
bash packages/lix/tests/w3_checkpoint_publication_source_gate.sh --expect-red
cargo test -p lix --test integration w3_ --no-run   # candidate only
cargo test -p lix --test integration w3_            # Memory candidate gate
```

Adapter acceptance repeats the focused public test on RocksDB and SlateDB,
then adds cold reopen and the race/corruption controls. Every cell is capped
at 20 minutes. The pinned frontier's known compiler frontier is not silently
treated as a candidate pass.
