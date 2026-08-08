# Future execution recipe — dormant

No command in this file was run when the package was frozen. Every future cell
uses a fresh nonexistent output path and has a 20-minute cap. The target must
be a later explicitly runnable test-only harness; this package does not add a
Cargo target or SQLite production implementation.

## Static binding

```bash
ROOT=/path/to/frozen-candidate
ANCHOR=b59e1f11a51153e0a787a81f0f25bf104d150aaf
ORACLE=$ROOT/packages/lix/tests/forktree_sqlite_oltp_optimization_target_b59
test "$(git -C "$ROOT" merge-base --is-ancestor "$ANCHOR" HEAD; echo $?)" = 0
sha256sum "$ORACLE/README.md" "$ORACLE/MANIFEST.json"
```

The future source gate must additionally prove that all target reads remain on
one coherent ForkTree view and that no SQLite fallback/dual writer or legacy
tracked-state reader is reachable. The shell fragment above is only a binding
check, not a production residue verifier.

## Dormant vector order

The future harness should expose one harness binary with this interface:

```text
forktree_sqlite_oltp_optimization_target <backend> <fresh-path> <cell> <samples>
```

Run the semantic model first, then the SQLite-shaped control, then the
ForkTree target. The following are command forms only:

```bash
TARGET=/path/to/isolated-target
BIN=/path/to/forktree_sqlite_oltp_optimization_target
timeout 1200 "$BIN" memory /tmp/forktree-sqlite-oltp-b59-memory point-1000 5
timeout 1200 "$BIN" sqlite /tmp/forktree-sqlite-oltp-b59-sqlite point-1000 5
timeout 1200 "$BIN" forktree-memory /tmp/forktree-sqlite-oltp-b59-ft-memory point-1000 5
timeout 1200 "$BIN" rocksdb /tmp/forktree-sqlite-oltp-b59-rocks point-1000 5
timeout 1200 "$BIN" slatedb /tmp/forktree-sqlite-oltp-b59-slate point-1000 5
```

Repeat the exact order for `range-128x32`, `insert-256-returning`,
`update-256-returning`, `delete-128-returning`, `upsert-256-returning`,
`mixed-savepoint`, `overlay-precedence`, and `historical-fail-closed` only
after `point-1000` passes semantic digest, counter, and cold-reopen gates.

## Required evidence per cell

```text
source/head/tree, harness/source/binary hashes, invocation, elapsed wall,
user/system CPU, allocation calls/bytes, peak RSS, backend calls/keys/bytes,
logical rows/bytes, object puts/deletes, commits, selector/epoch CAS,
warm digest, cold digest, disk-before/after, corruption result, verified
```

The first report must state the exact digest rather than a rounded or
path-dependent rendering. A warm/cold mismatch, any nonzero fallback/retry,
more than one publication commit, or any semantic mismatch stops the lane.
No broad scaling, current-main comparison, or production edit follows from a
failed focused cell.
