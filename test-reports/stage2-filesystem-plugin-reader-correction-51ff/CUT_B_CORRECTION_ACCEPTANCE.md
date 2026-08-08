# Cut B correction oracle: one retained read and fail-closed roots

Status: immutable TEST/REPORT-ONLY acceptance contract. This package contains
no production source, build, adapter runtime, PR, or merge mutation.

## Immutable anchor and expected source scope

| item | value |
|---|---|
| correction-oracle base/ref | `origin/codex/forktree-stage2-cut-b-reader-705` |
| base/head under review | `51ff5dbc353cb0322bcedcd191d6e2082e7ed479` |
| base tree | `e3b8d765cee51d61744fabb4e54c9143c04257dc` |
| parent/base frontier | `705440f55eccba9e2d55c0951d6a684737005d76` |
| parent→base full-index diff SHA-256 | `873c60457007fe2494188b3e84d104e96b2e2b98d74dd4e4708fd6cd59e4cbad` |
| parent→base patch ID | `ff3cb28c8e62d2932aee906501ab19bcd051f4da` |

Changed production paths in the immutable base are exactly:

```text
packages/lix/src/filesystem/read.rs
packages/lix/src/forktree/mod.rs
packages/lix/src/forktree/serving.rs
packages/lix/src/forktree/state.rs
packages/lix/src/live_state/forktree_reader.rs
packages/lix/src/plugin/registry.rs
packages/lix/src/session/merge/branch.rs
packages/lix/src/tracked_state/context.rs
```

The correction successor may touch only the read-facade allowlist in the
manifest. It must not widen into a writer, selector mutator, GC algorithm,
scalar/W2/W3/W4/W5 lane, compatibility path, cache, format, or second root.

## Exact red call-site map on 51ff

These are source-only discriminators, not runtime claims:

| invariant | exact 51ff seam | required correction |
|---|---|---|
| one retained view across all branches/modes | `filesystem/read.rs:31-57`; `plugin/registry.rs:499-524`; each calls `scan_forktree_branch` | acquire one operation-owned `CoherentView`/retained read and pass it through every branch and untracked-mode traversal; no nested view acquisition |
| no raw history read | `filesystem/read.rs:59-70`; `plugin/registry.rs:472,526-529`; `forktree/serving.rs:668-675` | historical file/plugin roots consume the same authenticated view/facade, never `&StorageAdapterRead` directly |
| no raw reader escape | `tracked_state/context.rs:1065-1067`; `session/merge/branch.rs:640-654` | delete `TrackedStateStoreReader::store`; merge uses the bound read/view owner, not an extracted `&S` |
| missing selected retained root fails closed | `filesystem/read.rs:60-62` uses `unwrap_or_default()` | propagate missing commit/member/root as a typed corruption error |
| missing selected plugin state fails closed | `plugin/registry.rs:473-475` maps `None`/tombstone to optional empty registry | distinguish authenticated explicit bootstrap from absent/malformed selected state; absence is an error |
| no raw branch/untracked fallback | `live_state/forktree_reader.rs:55,131`; raw `UNTRACKED_ROW_SPACE` export in `forktree/state.rs` | keep untracked access behind the bound read facade; no caller-owned space or second serving authority |
| one authority | `forktree/mod.rs:36-43` and `live_state/mod.rs:21-22` exports | expose only read-facade methods; no new selector/root/cache/space owner |

## Correction contract

1. A filesystem/plugin/GC-root operation obtains one authenticated
   `CoherentView` or inseparable read-facade owner and retains it until all
   current, untracked, branch, and retained-history roots are decoded.
2. Branch iteration and untracked mode reuse that same retained read/view.
   No helper calls `begin_read`, `open_coherent_view_on_read`, or an equivalent
   read refresh inside a branch/mode loop.
3. Filesystem and plugin root collectors accept the authenticated view/facade,
   not a generic `&S`/`StorageAdapterRead`; historical loaders cannot receive
   an extracted `reader.store()`.
4. Selected retained commit/member/catalog/root absence, wrong kind, malformed
   JSON slot, missing payload, wrong branch, and identity mismatch return typed
   corruption before a root is emitted. No `unwrap_or_default`, `None` masking,
   permissive empty fallback, retry, or partial root set is allowed.
5. Explicit empty bootstrap is valid only when the authenticated selected
   state contains the documented empty-registry/bootstrap value. Raw absence,
   missing selector, missing commit/member, or missing selected row is not
   bootstrap.
6. Tombstones are ignored only after their authenticated row kind, owner,
   branch/file scope, and deletion semantics have been validated. A malformed
   or absent selected fact cannot be reclassified as a tombstone.
7. Roots are validated `BlobId`s only. No payload read, CAS space, selector
   mutation, GC progress mutation, durable cache/index, compatibility reader,
   or second logical root is introduced.

## Corrected path policy

Allowed production paths are limited to genuinely necessary read-facade
plumbing:

```text
packages/lix/src/filesystem/read.rs
packages/lix/src/filesystem/mod.rs
packages/lix/src/plugin/registry.rs
packages/lix/src/plugin/mod.rs
packages/lix/src/session/merge/branch.rs       # read-only historical plumbing
packages/lix/src/forktree/mod.rs                # read-only reexports only
packages/lix/src/forktree/serving.rs            # read-only view/root loaders only
packages/lix/src/forktree/state.rs              # existing untracked decode facade only
packages/lix/src/forktree/view.rs               # existing CoherentView read facade only
packages/lix/src/live_state/forktree_reader.rs # reader implementation only
packages/lix/src/live_state/mod.rs              # reader reexports only
packages/lix/src/tracked_state/context.rs      # deletion-only raw accessor cleanup
```

`tracked_state/context.rs` is allowed only when the diff deletes the raw
`store()` accessor and does not add another extraction path. The other
allowlisted files are not blanket permission: added writes, selector
mutation, GC scheduling, CAS/storage ownership, new spaces, or durable
caches are rejected by the verifier.

Hard-forbidden paths include `gc.rs`, `session/gc.rs`, all transaction or
publication/writer paths, ForkTree publication/reachability/tree/object/blob
algorithm files, selector mutation paths, `tracked_state/tree.rs` and
legacy reader paths, scalar/entity/W2/W3/W4/W5 lanes, SQL binder/executor
lanes, CAS/storage implementation, and any compatibility/migration/fallback
path. Only the exact read-facade files above may change.

## Required correction tests

Before any runtime approval, the successor must provide deterministic
Memory/RocksDB/SlateDB controls for:

- two or more branches plus tracked and untracked modes, asserting one
  retained read/view identity and no additional view acquisition;
- missing selected file/member/root, missing plugin registry/owner, wrong kind,
  malformed slot, remapped identity, and cold reopen; all fail closed with
  zero root output and no writes/selector/epoch/GC progress changes;
- explicit authenticated empty plugin bootstrap succeeds and remains distinct
  from raw absence;
- valid parsed-file/history and plugin registry roots preserve exact
  BlobId/order/dedupe semantics;
- no payload bytes are read merely to collect BlobId roots, and no raw
  `StorageAdapterRead` or store accessor is reachable from the reader owners.

## Source gate

Run without compiling or opening an adapter:

```text
bash test-reports/stage2-filesystem-plugin-reader-correction-51ff/verify_source_contract.sh 51ff5dbc353cb0322bcedcd191d6e2082e7ed479 51ff5dbc353cb0322bcedcd191d6e2082e7ed479
```

The exact 51ff invocation must be RED for the call sites above. A corrected
successor may pass only with zero raw-view/store/empty-masking diagnostics,
an allowed path delta, and explicit `CoherentView`/facade ownership in both
primary filesystem/plugin readers. This package does not claim compilation
or runtime qualification.
