# Selector authority inventory binding for Cut B

Status: immutable TEST/REPORT-ONLY successor manifest. This binding adds no
production source, storage format, runtime, adapter, PR, or merge behavior.
It is attached to the exact Cut B package at `c80bafbe` and preserves that
package's source-RED calibration.

## Exact immutable identities

```text
Cut B ref: origin/codex/forktree-cut-b-reader-acceptance-705
Cut B head: c80bafbed5545b7768ac3a8dd4ed2ee9d3dacef4
Cut B tree: 18913425e9ce29b1c821837e04339458e200d397
Cut B parent: 705440f55eccba9e2d55c0951d6a684737005d76
Cut B parent..head full-index SHA-256: 74e460189b0fa79d003da8431d805e72fa551836df5e3995c512f8f51f6ae23b
Cut B stable patch ID: d69af7a88361a1791bb411729bfe9a7363164deb
Current-state reader prerequisite: 705440f55eccba9e2d55c0951d6a684737005d76
Current-state reader prerequisite tree: 2b8dcb45a2d06bdda86d0fa5add5ea8c12d18c2d
Selector contract supplied SHA-256: ff784043429f563fb01a29c42eecc90a939f7ce8ac7926d9db07a0f13313da24
```

The selector inventory source package remains a separate immutable report:

```text
ref: origin/codex/selector-authority-acceptance-705
head: 7f467eb3192c8964c9f25f62ff1a2cd78b280dc3
tree: 530195b10d787c8fc32e014032731f7470ec10c9
parent: 705440f55eccba9e2d55c0951d6a684737005d76
full-index SHA-256: d7938dc112b8f5a0e118831ddddbf2449e3f782a2a2eb3050ccb8b7b6f4b09d8
stable patch ID: b1518bcfea98a2ac18e1ac7baf25861ef86347ae
selector contract SHA-256: ff784043429f563fb01a29c42eecc90a939f7ce8ac7926d9db07a0f13313da24
```

The contract document named by the manager was not present in the local
evidence tree; its supplied digest is the binding identity, not a claim that
a local copy was reviewed.

## Required ordering and hard boundary

The selector inventory is a prerequisite/source-readiness gate for Cut B,
not permission to edit selector or ForkTree production code in this
successor. Cut B may proceed only after the selector/current-state owner is
source complete (`705` or a newer explicitly frozen replacement) and its
selector acceptance controls are available to the integration runner. A
compiler-red prerequisite is not a runtime waiver and must not be repaired by
restoring the deleted tracked-head reader.

```text
selector/current-state source gate
        -> one CoherentView reader facade
        -> Cut B filesystem/plugin/file-provider/merge-loader reader cut
        -> W4/W5 publication, root-reclamation, and adapter qualification
```

Only this Cut B production surface may change in a future source successor:

```text
packages/lix/src/filesystem/read.rs
packages/lix/src/filesystem/mod.rs
packages/lix/src/plugin/registry.rs
packages/lix/src/plugin/mod.rs
packages/lix/src/sql2/providers/file.rs
packages/lix/src/session/merge/branch.rs
```

Only artifacts under
`test-reports/stage2-filesystem-plugin-reader-705/` are otherwise permitted.
Selector/ForkTree, scalar/W2, transaction/publication/writer, GC
orchestration, CAS/storage, compatibility, fallback, and second-authority
paths remain hard-rejected. No production path is changed by this binding.

## Selector inventory Cut B must consume, not reimplement

| Fact | Sole intended owner | Cut B rule | Red condition |
| --- | --- | --- | --- |
| repository selected root/epoch | authenticated `GlobalSelectorV1` in `SELECTOR_SPACE` | consume through one retained `CoherentView` | caller root, selector cache, or second reader |
| selected branch snapshot | authenticated `BranchSelectorV1` keyed by canonical UUID | consume global and branch identity from one view | `BranchRefReader` or branch-ref fallback |
| root/object closure | authenticated `RepositoryRootV1`/`BranchSnapshotV1` | validate before rows or BlobIds are emitted | missing/wrong-kind/hash becomes empty |
| chronology | commit-object parent list and semantic catalog | use historical views supplied by the facade | selector/recovery/queue/checkpoint becomes parent |
| serving checkpoint floor | serving projection as context only | do not create a file/plugin floor or root | marker becomes chronology authority |
| atomic visibility | `PreparedPublication` and exact selector CAS | readers stay read-only and view-bound | reader writes, repair, epoch/queue mutation |
| undo/redo context | canonical selector/chronology owner | preserve public cursor semantics through facade | `lix_undo_redo_marker` is independent authority |

The selector source inventory is RED in these inherited areas and they must
be resolved by their own owner before Cut B is integration-ready:
`session/create_branch.rs` still stages legacy branch descriptor/ref rows;
`session/switch_branch.rs` still reads `BranchRefReader` and stages a
workspace selector row; `branch/stage_rows.rs` retains branch-ref
stage/tombstone writers; and `session/undo_redo.rs` persists
`lix_undo_redo_marker` without a proven selector-owner bridge. This binding
does not edit or duplicate those paths.

## Readiness gates

Run the Cut B source verifier first. Exact `c80bafbe` must retain its recorded
expected RED; a source-complete reader candidate may pass only without adding
selector/ForkTree or compatibility paths. Separately run selector controls
A1-A15 from the immutable selector inventory:

```text
A1 valid global+branch selectors reopen as one coherent view
A2 atomic branch create selector publication
A3 authenticated pinned/workspace switch
A4 exact-CAS branch delete and closure retirement
A5 same-owner stale write rejection
A6 unrelated-owner rejection
A7 chronology versus serving-floor separation
A8 undo/redo/reopen stability
A9-A12 missing/malformed/wrong-kind root, commit, parent, cycle fail-closed
A13 old-or-new crash publication
A14 retained-view publication/GC pinning
A15 key/domain/UUID/generation/root substitution rejection
```

Then require Cut B consumer controls:

```text
B1 one CoherentView for filesystem/plugin/history/merge/GC-root reads
B2 parsed-file, directory, BlobId and file-history semantics unchanged
B3 plugin registry/owner valid, bootstrap and corruption behavior
B4 BlobId-only deduplicated roots with no payload byte read
B5 historical merge-registry views, not mutable current state
B6 selected-row/root/owner/scope/key/BlobId fail-closed behavior
B7 RocksDB and SlateDB flush/drop/reopen
B8 no legacy reader, write, queue, selector, cache or second authority residue
```

Memory runs first, then identical RocksDB and SlateDB runs. Required
filesystem/plugin cases include parsed-file/history ordering, directory
closure/collisions, untracked scope, valid/empty-bootstrap registry,
owner/hash/version failures, BlobId roots, historical merge views, selected
row/root corruption, and cold reopen. Readers must produce zero writes,
selector/epoch/queue mutation, second read, legacy tracked-head/tracked-state/
CAS-reader acquisition, durable cache/index, or second root authority.

## Exact command shapes (not run)

These are qualification shapes only; no compile, adapter runtime, or PR/CI
claim is made on this source-RED report-only anchor:

```text
bash test-reports/stage2-filesystem-plugin-reader-705/verify_source_contract.sh
cargo test -p lix --lib forktree::tests::selector_codecs_have_single_edges_and_canonical_keys -- --exact --nocapture --test-threads=1
cargo test -p lix --lib forktree::tests::full_selector_scan_crosses_storage_page_and_corruption_fails_closed -- --exact --nocapture --test-threads=1
cargo test -p lix --lib session::undo_redo::tests::checkpoint_is_an_undo_floor -- --exact --nocapture --test-threads=1
cargo test -p lix --lib filesystem::read::tests::from_live_rows_rejects_file_directory_namespace_conflicts -- --exact --nocapture --test-threads=1
cargo test -p lix --lib plugin::registry -- --nocapture --test-threads=1
cargo fmt --all -- --check
cargo clippy -p lix --lib --tests --all-features -- -D warnings
```

Exact public filters must be reconciled with the immutable successor before
execution; invented or silently skipped filters are not evidence. Report
Memory, RocksDB, and SlateDB separately, including backend work and settled
disk where available.

## Acceptance and stop rule

This successor approves only the binding artifact, not Cut B production. Cut
B remains source-RED at `c80bafbe`. Block a future candidate on any forbidden
path, legacy reader, second selector/root/view/cache/format, permissive
published absence, reader-side publication/GC mutation, or changed
parsed-file/plugin/history semantics. A future candidate is integration-ready
only after the selector prerequisite is independently source-complete and
both durable adapters pass the Cut B controls.

No production edits, compile, adapter runtime, PR mutation, or merge were
performed for this binding.
