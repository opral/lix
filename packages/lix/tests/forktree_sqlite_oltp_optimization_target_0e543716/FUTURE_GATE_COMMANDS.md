# Future execution recipe — dormant and blocked at target compatibility

No command in this file was run when the package was frozen. Every future cell
uses a fresh nonexistent output path and has a 20-minute cap. The target must
be a later explicitly runnable test-only harness; this package does not add a
Cargo target or SQLite production implementation.

## Static binding

```bash
ROOT=/path/to/frozen-candidate
ANCHOR=0e543716b0a89d377015ce8cdb1579cad70408ff
ORACLE=$ROOT/packages/lix/tests/forktree_sqlite_oltp_optimization_target_0e543716
EXPECTED_TREE=b0589d9037a8b3a530d252150360ce9f5648577c
EXPECTED_PARENT=cd76d29406ed7e00711a5b5ba9c40da537524dd3
test "$(git -C "$ROOT" rev-parse "$ANCHOR^{tree}")" = "$EXPECTED_TREE"
test "$(git -C "$ROOT" rev-parse "$ANCHOR^1")" = "$EXPECTED_PARENT"
test "$(git -C "$ROOT" merge-base --is-ancestor "$ANCHOR" HEAD; echo $?)" = 0
sha256sum "$ORACLE/README.md" "$ORACLE/MANIFEST.json"
```

The 0e compatibility preflight must fail the OLTP lane until a test-only
adapter exists. The published 0e entry points are the OLAP provider and
DuckDB comparator, not this package's OLTP interface:

```bash
git -C "$ROOT" grep -n -E 'read_projected_range|read_range|TableProvider' \
  "$ANCHOR" -- packages/engine-benchmarks/benches/forktree_replacement/olap_datafusion.rs
git -C "$ROOT" grep -n -E 'RETURNING|ON CONFLICT|savepoint|stale|file.row|begin_write|PreparedPublication' \
  "$ANCHOR" -- packages/engine-benchmarks/benches/forktree_replacement/olap_datafusion.rs \
  benchmarks/forktree_duckdb_comparator/src/main.rs
```

The first command identifies the available read-only OLAP seam. The second is
an evidence command, not a pass condition: absent CRUD/transaction/publication
owners are a **BLOCKER**, and fixture setup writes do not satisfy the OLTP
contract. Do not relabel the OLAP benchmark as an OLTP target.

If a future test-only adapter is added, its source gate must additionally prove
all target reads remain on one coherent ForkTree view and that no SQLite
fallback/dual writer or legacy tracked-state reader is reachable. The shell
fragment above is only a binding check, not a production residue verifier.

The standalone SQLite control is pinned before any run:

```bash
SQLITE_CRATE_SHA=2e99fb7a497b1e3339bc746195567ed8d3e24945ecd636e3619d20b9de9e9149
SQLITE_C_SHA=c01235302fe80da901fb70c7622c39147e29d9f29b7f6eb746b23517f320c90d
SQLITE_H_SHA=d088aa96aa70db50f02acc5c86eca61a5d17556e4c363b9c06079239bf7f87b1
SQLITE_ARCHIVE_SHA=e2532979ce9bde50b950ffb7c63c4f2fc2da72f7499c75afc4275948faa674ca
test "$SQLITE_CRATE_SHA" = "$(sha256sum /path/to/libsqlite3-sys-0.30.1.crate | cut -d' ' -f1)"
test "$SQLITE_C_SHA" = "$(sha256sum /path/to/sqlite3/sqlite3.c | cut -d' ' -f1)"
test "$SQLITE_H_SHA" = "$(sha256sum /path/to/sqlite3/sqlite3.h | cut -d' ' -f1)"
test "$SQLITE_ARCHIVE_SHA" = "$(sha256sum /path/to/libsqlite3.a | cut -d' ' -f1)"
```

The compile gate must publish the standalone harness executable SHA before
runtime; `binary_sha256` may not remain a placeholder. The harness must execute
the pinned `PRAGMA` set from `README.md`, use `BEGIN IMMEDIATE` for writes,
attach `RETURNING` only to DML, canonicalize rows by
`(statement_index, primary_key_bytes)`, and use plain `COMMIT`.

## Dormant vector order

The future harness should expose one harness binary with this interface:

```text
forktree_sqlite_oltp_optimization_target_0e543716 <backend> <fresh-path> <cell> <samples>
```

Run the semantic model first, then the SQLite-shaped control, then the
ForkTree target. The following are command forms only:

```bash
TARGET=/path/to/isolated-target
BIN=/path/to/forktree_sqlite_oltp_optimization_target_0e543716
timeout 1200 "$BIN" memory /tmp/forktree-sqlite-oltp-0e543716-memory point-1000 5
timeout 1200 "$BIN" sqlite /tmp/forktree-sqlite-oltp-0e543716-sqlite point-1000 5
timeout 1200 "$BIN" forktree-memory /tmp/forktree-sqlite-oltp-0e543716-ft-memory point-1000 5
timeout 1200 "$BIN" rocksdb /tmp/forktree-sqlite-oltp-0e543716-rocks point-1000 5
timeout 1200 "$BIN" slatedb /tmp/forktree-sqlite-oltp-0e543716-slate point-1000 5
```

Those commands are intentionally dormant because no such 0e OLTP harness
binary exists in this commit. The only runnable 0e command shape is the
unrelated OLAP benchmark:

```bash
timeout 1200 "$OLAP_BIN" olap-datafusion rocksdb forktree 10000 32 3 1 1
timeout 1200 "$OLAP_BIN" olap-datafusion slatedb forktree 10000 32 3 1 1
```

Running those OLAP commands does not qualify any SQLite OLTP cell and was not
done for this package.

Repeat the exact order for `range-128x32`, `insert-256-returning`,
`update-256-returning`, `delete-128-returning`, `upsert-256-returning`,
`mixed-savepoint`, `overlay-precedence`, and `historical-fail-closed` only
after `point-1000` passes semantic digest, counter, and cold-reopen gates.

The corrected mixed cell is exactly 192 committed mutations and 192
canonicalized RETURNING rows: 64 inserts + 64 updates + 32 deletes + 32
upserts, after eight rolled-back mutations.

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

The target requires at least 10% improvement in the targeted measure and at
most 5% regression in every primary guardrail. Report the perfect call ceilings
from `MANIFEST.json`; do not convert them into wall-time claims.
