# Typed scalar range accelerator experiment — NO-CUT

## Provenance

- Exact base: `887b7a45ae732f2d3e0de8c13778dfc9b05f6e4c`
- Base tree: `d2e8a4a5c7fae82543e916a863dcfa385b311d20`
- Scope: benchmark/report only. No production source was changed.
- PR #1480 was not fetched, checked out, or modified.

## Bounded public workload

`packages/e2e/benches/typed_scalar_range.rs` creates paired Schema-v1
collections through public SQL. One declares a single-column unique index and
one has identical rows without the declaration. It covers `int8`, `text`, and
`uuid`; 1/100/1%-selective ranges; point/full/update guards; delta updates;
digest equality; adapter flush; public reopen; CPU, RSS, SlateDB I/O, hot-index
candidates, and settled bytes. There is no query-name or benchmark-only engine
route.

The intended full matrix was not run because release closure arrived during
the first bounded build cell. The build reached the benchmark crate with no
source diagnostic; native release linking exceeded the bounded continuation.
No runtime or performance claim is made.

## Concrete correctness blocker

Current main already contains a typed scalar range seek, but completeness is
selected by a mutable record in `INDEX_SPACE`:

- `sql2/providers/schema.rs::declared_column_range` lowers a public predicate.
- `hot_state/context.rs::resolve_declared_column_eq` routes it to the hot index.
- `hot_state/tracked_head/hot.rs::scan_hot_index_range_candidates` accepts a
  count-only witness from `INDEX_SPACE`, then scans candidate entries.
- `hot_state/tracked_head/hot.rs::stage_hot_index_entries` publishes put-only
  entries and the count witness.

Returned candidates are revalidated against canonical rows, which rejects a
bad extra candidate. It cannot detect a deleted/substituted missing index entry
while the count witness remains. Such corruption can silently omit a matching
canonical row. The witness is neither selected nor authenticated by the
canonical publication/state root.

This is pre-existing on the exact base and is not evidence against PR #1480.
It does block treating the current range accelerator as correctness-qualified.

## Required production design (not started)

Any future cut must delete the witness authority and select a disposable
accelerator descriptor through the authenticated commit/publication state root.
The descriptor must bind repository/branch owner, generation, schema layout,
canonical source root, accelerator root, row count, and object identity. Wrong
root/schema/owner/generation/substitution must fail closed; absence may fall
back to canonical NativeRows. GC must retain selected accelerator objects.

An older experiment (`origin/codex/accelerator-manifest-a21`) demonstrates the
necessary cross-cutting shape (commit-state root set, selector digest,
publication certificate, and GC traversal), but it is based on an obsolete
lineage and was not composed or reused.

## Gates

- `cargo fmt --all -- --check`: PASS.
- `git diff --check`: PASS.
- First release build cell: timeout after 1200 seconds while compiling the
  dependency graph; no benchmark-source diagnostic.
- Warm continuation: benchmark crate compiled after the sole generic-flush
  source error was corrected; native link exceeded the 180-second bound.
- RocksDB/SlateDB runtime cells: UNRUN.

Verdict: **NO-CUT / correctness blocker recorded**. Performance gaps are not a
release blocker and no production experiment follows this report.
