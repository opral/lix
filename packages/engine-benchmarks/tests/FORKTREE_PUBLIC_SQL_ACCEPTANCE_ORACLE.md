# ForkTree public-SQL acceptance oracle

Status: test/report/source-artifact-only. This package makes no production,
SQL, current-main, adapter, or performance change. It freezes the public
semantic gate for the direct entity snapshot/primary-key path while the
obsolete row-group planner is deleted. Runtime is deliberately unclaimed on
the compiler-red frontier.

## Immutable anchor

The exact accepted frontier is `e1666edd0b4d814a88d985086ecc5a477b5d32e6`
with tree `c680bd7e7f7b70cd784676515839af2dcbbc7917`. The frontier is the
`stage2: delete dead live-state hot scan helpers` change. The oracle branch
must remain based directly on that commit; it does not merge current main or
the moving R5 candidate.

## Authority and call graph

The semantic owner and required deletion boundary are:

* `packages/lix/src/live_state/context.rs`: SQL-facing
  `scan_direct_entity_snapshots`, `scan_direct_entity_primary_keys`,
  `direct_entity_snapshot_scope`, `scan_forktree_operation`, `load_row`, and
  `load_exact_batch`. Direct scope must retain one requested schema and one
  serving branch, preserve branch/global visibility rules, and use one coherent
  state view. `plan_direct_entity_columnar_scan` is the obsolete physical
  planner and must disappear; its removal cannot change the canonical row
  result.
* `packages/lix/src/live_state/forktree_reader.rs`: direct authenticated
  snapshot/primary-key point and range reads. It owns exact `EntityPk` identity,
  missing-root/corrupt-object handling, and tombstone filtering for the direct
  view.
* `packages/lix/src/live_state/visibility.rs`: canonical branch/local versus
  global overlay precedence, identity deduplication, tombstone hiding, and
  `include_tombstones` behavior. No row-group planner may reimplement this.
* `packages/lix/src/sql2/providers/entity.rs` and
  `packages/lix/src/sql2/entity_projection.rs`: SQL entity provider and ordered
  projection decoding. Projection is applied after identity/overlay
  canonicalization and must not change row order or `LIMIT` placement.
* `packages/lix/src/entity_pk.rs` and `packages/lix/src/sql2/catalog/entity_surface.rs`:
  typed schema/primary-key authority. A string lookalike, wrong component kind,
  wrong arity, or malformed identity fails closed.

The intended post-cut flow is:

```text
SQL entity request
  -> entity provider / projection request
  -> direct snapshot or direct primary-key reader
  -> one coherent ForkTree view
  -> canonical identity + branch/global overlay
  -> tombstone filter
  -> ordered projection
  -> LIMIT
```

The obsolete flow must not survive as a second authority:

```text
SQL request -> plan_direct_entity_columnar_scan -> row-group manifest/overlay
```

## Required public semantics

The pure oracle covers the following exact behaviors:

* one row per canonical typed `EntityPk`; identical same-owner duplicates
  collapse, conflicting same-owner duplicates fail closed;
* local branch rows override global rows by identity; a local tombstone hides a
  global value and is returned only when `include_tombstones` is requested;
* explicit `NULL` is a value and is never treated as a tombstone;
* output is sorted by canonical identity before projection and `LIMIT`, and
  projection columns retain requested order;
* exact primary-key filters are typed, schema-bound, and do not alter ordering;
* malformed identity/value pairs, missing schema/branch roots, unknown
  projection columns, and wrong physical kinds fail closed;
* direct execution is differentially compared with an independent sorted-vector
  reference representing the deleted row-group planner, across full, projected,
  limited, tombstone-inclusive, and exact-PK shapes;
* Memory, RocksDB, and SlateDB controls must produce the same public rows,
  errors, order, and bytes, with no writes during reads. Durable controls must
  add flush/drop/reopen before claiming runtime green.

## Source/residue gate

`scripts/forktree_public_sql_residue_verify.mjs` requires the direct boundary
tokens and rejects row-group files, `plan_direct_entity_columnar_scan`,
`EntityColumnar*` owners, row-group manifests/IDs, direct columnar imports, and
the already-deleted hot helpers. The exact e1666 calibration is intentionally
red while the in-flight obsolete planner/owner files remain; this frozen
result is the comparator for R5's candidate. A future candidate is accepted
only when this verifier is green and the compiler no longer names those
owners. No compatibility wrapper, fallback planner, second registry, or raw
physical authority is allowed.

## Ordered commands

Run from an isolated candidate checkout with one target directory. The first
three are source/model gates; do not run durable adapters until the candidate's
package no-run is green.

```sh
node scripts/forktree_public_sql_residue_verify.mjs --root "$PWD"
cargo fmt --all -- --check
git diff --check
rustc --edition=2021 --test -D warnings \
  packages/engine-benchmarks/tests/forktree_public_sql_acceptance_oracle.rs \
  -o /tmp/forktree-public-sql-oracle
/tmp/forktree-public-sql-oracle --nocapture --test-threads=1
cargo clippy -p lix_benchmarks --test forktree_public_sql_acceptance_oracle -- -D warnings
cargo test -p lix_benchmarks --test forktree_public_sql_acceptance_oracle --no-run
cargo test -p lix_benchmarks --test forktree_public_sql_acceptance_oracle -- --nocapture --test-threads=1
```

For a compile-green candidate, run the same semantic test with isolated
`W0_BACKEND=memory`, `W0_BACKEND=rocksdb`, and `W0_BACKEND=slatedb` controls.
Memory is the first runtime cell; RocksDB and SlateDB must flush/drop/reopen
and replay the exact fixture. Each runtime cell is capped at 20 minutes. This
oracle claims no runtime result on e1666.

## Expected compiler/residue reduction

The source-only package does not claim a numeric compiler reduction. The
candidate must record exact before/after `cargo check -p lix --lib
--all-features` and warnings-denied Clippy diagnostics. The expected reduction
is removal of the row-group planner and its owners/imports, while direct
snapshot/primary-key readers and SQL projection remain. Any public-result or
error divergence is a blocker even if compiler output improves.

## Exact e1666 calibration

The source verifier was run once against the exact anchor. It found 0 missing
direct-boundary tokens and 187 obsolete-columnar residues in 192 output lines;
the final output SHA-256 is
`bc3b8db204e80707c3b890228fa07165b9cfb6098d426b0c452d5b2e465d4f97`.
This is the expected red comparator because `plan_direct_entity_columnar_scan`,
row-group manifests/IDs, entity-columnar caches/overlays, and columnar imports
remain in the frontier. The already-deleted hot helper names were not found.

The pure model compiled with `rustc --edition=2021 --test -D warnings` and ran
8/8 tests green. The standalone test binary SHA-256 is
`40cd9f6e2603947af1807348a5031e7fedcb4348ceb65f21d7f4d8c8f73d3732`.
The model is intentionally independent of the compiler-red production crate;
the Cargo no-run, Clippy, and Memory/RocksDB/SlateDB runtime gates were not
claimed or started on e1666. R5 must replay them on its compile-green
successor, beginning with the verifier and this exact model.
