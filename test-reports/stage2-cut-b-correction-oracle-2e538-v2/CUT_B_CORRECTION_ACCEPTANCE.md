# Cut B correction oracle v2 — 2e538 red anchor

This is a TEST/REPORT-ONLY source oracle. It does not compile, execute, or
modify production code. It is a direct successor of the immutable `2ace`
oracle and closes the H4 report contract identified by SHA
`a9a13f5f58410e779d8494f288f9dafbecf69f6e5a0c2c984b63f813c1a7eb7b`.

## Immutable anchor and scope

- Successor base/ref: `2ace19022e7cb6aa74936c759e96b6311beac00f`
- Production correction anchor: `2e5389265d0495728325efe43d7eb6d9ad715aa0`
- Anchor tree: `17087b2241deacfa83f5ae95052d8f0703668eb6`
- H4 report SHA-256: `a9a13f5f58410e779d8494f288f9dafbecf69f6e5a0c2c984b63f813c1a7eb7b`

Relative to `2e538`, a future production correction may change only:

1. `filesystem/read.rs`
2. `forktree/mod.rs`
3. `forktree/view.rs`
4. `live_state/forktree_reader.rs`
5. `live_state/mod.rs`
6. `plugin/registry.rs`
7. `session/merge/branch.rs`
8. `tracked_state/context.rs`, and only by deletion (including a whole-file
   deletion status).

Any other production path is rejected. Added writer/transaction/publication,
selector, GC algorithm, scalar/W2/W3/W4/W5, CAS/storage, cache, compatibility,
fallback, or migration authority is rejected even inside an allowed path.

## One-owner/lifetime contract

Cut B must have one opaque operation owner retaining exactly one authenticated
`StorageRead`. Branch-bound descriptors may differ by branch ID, but they must
borrow that exact retained read. `branch()` must not call `begin_read`, refresh,
clone, replace, or construct a detached view; the owner must expose no raw-read
extraction. Cursor/resume tokens must carry the descriptor/view identity and
reject use with another descriptor. No negative/error cache or second durable
authority is permitted.

The facade cannot be constructed at Cut B callsites from arbitrary raw `&R`.
Typed ForkTree methods must cover historical registry, member, state, and JSON
loading. The raw `scan_branch`/raw `open_coherent_view_on_read(store, ...)`
boundaries must not remain usable by those consumers.

## Fail-closed discriminators

- A missing selected plugin registry is typed corruption. An authenticated
  explicit bootstrap-empty row is the only empty success.
- A missing selected CommitCatalog/member/state/registry root, malformed JSON,
  wrong kind, or invalid row must fail before root/row output.
- Filesystem BlobRef extraction must bind JSON `snapshot.id`, `blob_hash`, and
  `size` to the authenticated semantic row before returning a root. A same-size
  remapped JSON object must fail closed; trusting only `blob_hash` is invalid.

`cut_b_discriminators.rs` is a standalone, dependency-free positive/negative
model of these exact cases. It is intentionally not compiled or run by this
oracle.

## Exact 2e callsite map and expected red causes

- `packages/lix/src/plugin/registry.rs:459-485` —
  `load_plugin_registry_at_commit` accepts `&mut TrackedStateStoreReader<S>`
  and calls `load_projected_batch_at_commit`.
- `packages/lix/src/session/merge/branch.rs:547-654` — derived plugin merge
  calls the legacy loader for base/target/source.
- `packages/lix/src/live_state/forktree_reader.rs:31-45` — raw
  `scan_branch<S>(&S, ...)`; approximately lines 241-252 in `load_exact_batch`
  also calls `open_coherent_view_on_read(store, ...)`.
- `packages/lix/src/forktree/view.rs:133-210` — `ForkTreeReadFacade<'a, R>`
  stores raw `read: &'a R`, accepts `from_retained_read(read: &'a R)`, and
  creates `CoherentView<&R>` in `branch()`.
- `packages/lix/src/filesystem/read.rs:21-90` — filesystem roots accept a
  raw owner and `blob_id_from_snapshot` trusts only `blob_hash`.
- `packages/lix/src/plugin/registry.rs:508-567` — plugin roots accept a raw
  owner and zero current registry rows become empty-success iteration.

## Commands

```sh
test-reports/stage2-cut-b-correction-oracle-2e538-v2/verify_source_contract.sh \
  "$PWD" \
  2e5389265d0495728325efe43d7eb6d9ad715aa0 \
  2e5389265d0495728325efe43d7eb6d9ad715aa0
```

The exact anchor must return exit 1. No build, runtime, or broad matrix is part
of this package.
