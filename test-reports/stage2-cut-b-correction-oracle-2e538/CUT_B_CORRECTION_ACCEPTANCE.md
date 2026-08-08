# Cut B reader correction oracle — 2e538 red anchor

This is a test/report-only source oracle. It does not compile, execute, or modify
production code. Its purpose is to prevent a narrow Cut B correction from being
declared green while the historical plugin/merge path, raw scan entry point, or
facade ownership seam still bypasses the retained authenticated read.

## Immutable anchor

- Ref: `origin/codex/forktree-stage2-cut-b-reader-correction-51ff`
- Head: `2e5389265d0495728325efe43d7eb6d9ad715aa0`
- Tree: `17087b2241deacfa83f5ae95052d8f0703668eb6`
- Parent: `51ff5dbc353cb0322bcedcd191d6e2082e7ed479`
- Parent..head full-index binary SHA-256: `6845fd08feee545f732f8011979b3c9a60fb2ba2ec3e85c2e7ed8f22f7699f53`
- Stable patch ID: `55544da1d0395b9924299d25ef76518501c1e346`

Run from the repository root:

```sh
test-reports/stage2-cut-b-correction-oracle-2e538/verify_source_contract.sh \
  "$PWD" \
  2e5389265d0495728325efe43d7eb6d9ad715aa0 \
  2e5389265d0495728325efe43d7eb6d9ad715aa0
```

The anchor is intentionally RED. A later immutable correction may be checked by
passing its exact base/head; the same predicates must then be evaluated before
any runtime qualification.

## Required correction contract

One operation owner must retain one authenticated `CoherentView`/`StorageRead`
for all Cut B historical work. The owner, not an arbitrary raw adapter read,
must expose typed ForkTree operations for historical registry, member, state,
and JSON loading. Branch wrappers must borrow that same operation owner. A
missing selected root, malformed semantic record, wrong kind, or invalid JSON
must return typed corruption before roots/rows are emitted; it must never become
an empty registry, empty result, `None`, or a fallback to the deleted reader.
Different branch IDs may have lightweight branch-bound view descriptors, but
each descriptor must borrow the same operation owner's retained read and must
not refresh, extract, detach, or cross-use a cursor/read.

For current plugin roots, zero selected registry rows are valid only when the
source proves an authenticated, explicit bootstrap-empty condition. A missing
selected registry in an ordinary branch/control is corruption, not an empty
registry. For filesystem roots, the JSON `id` and semantic row identity must be
validated together with `blob_hash`; a same-size remapped BlobRef JSON must
fail before a root is emitted.

The correction must remove or make unreachable from Cut B consumers:

- historical plugin/merge calls through `TrackedStateStoreReader`;
- raw `scan_branch(&S)` and raw `open_coherent_view` acquisition usable by
  filesystem/plugin/merge consumers;
- arbitrary `&R` facade construction, read extraction, and detached branch or
  history views;
- any second durable reader, selector, cache, writer, GC algorithm, scalar,
  W2/W3/W4/W5, CAS, or compatibility path.

Explicit bootstrap-empty behavior is allowed only at a declared bootstrap
boundary; retained historical roots are never allowed to use it.

## Exact source/callsite map on 2e538

- `packages/lix/src/plugin/registry.rs:459-485` —
  `load_plugin_registry_at_commit` still accepts `&mut
  TrackedStateStoreReader<S>` and calls `load_projected_batch_at_commit`.
- `packages/lix/src/plugin/registry.rs:508-567` — the WASM root collector
  accepts `owner: &O`, constructs `ForkTreeReadFacade::from_retained_read`,
  and separately handles current and retained registry reads.
- `packages/lix/src/session/merge/branch.rs:547-654` — the derived plugin
  conflict path owns a `TrackedStateStoreReader` and calls the old historical
  registry loader for base/target/source.
- `packages/lix/src/live_state/forktree_reader.rs:31-45` — raw
  `scan_branch<S>(&S, ...)` calls `open_coherent_view_on_read(store, ...)`;
  `scan_view` exists beside it but does not delete the raw entry.
- `packages/lix/src/forktree/view.rs:133-210` — `ForkTreeReadFacade<'a, R>`
  stores arbitrary `read: &'a R`; `from_retained_read` accepts raw `&R`, and
  `branch()` constructs another `CoherentView<&R>` from that field.
- `packages/lix/src/filesystem/read.rs:21-82` — filesystem roots accept raw
  `owner: &O` and construct the facade; retained member/JSON reads are typed
  but not operation-owner bound.
- `packages/lix/src/filesystem/read.rs:83-90` — `blob_id_from_snapshot` only
  parses `blob_hash`; it does not bind `snapshot.id` to the semantic row.
- `packages/lix/src/plugin/registry.rs:532-542` — an empty current scan is
  iterated without an authenticated bootstrap distinction, so zero rows become
  empty-success roots.
- `packages/lix/src/tracked_state/context.rs:977+` — the legacy reader type
  remains the reachable owner for the plugin/merge path even though its raw
  `store()` accessor was removed.

## Red calibration meaning

The previous 51ff correction oracle reported a green result on 2e538 because
it only checked the new root collector shape and did not inspect the historical
plugin loader, its merge callsites, the raw scan entry, or the raw generic
facade. This oracle makes those concrete seams mandatory. It is therefore a
source gate, not a claim that all unrelated legacy code in the staged compiler
frontier must already disappear.

## Scope guard

The oracle rejects additions to writer/publication, selector, GC algorithm,
scalar/W2-W5, binary-CAS, compatibility, or cache paths. It is not an
acceptance test for runtime, SQL, or the full non-runnable Stage2 compiler
frontier.
