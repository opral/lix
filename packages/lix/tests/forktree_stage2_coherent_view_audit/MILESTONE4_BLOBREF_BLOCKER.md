# Stage2 Milestone 4 BlobRef source review — BLOCKER

Verdict: **SOURCE BLOCKER** for immutable
`origin/codex/forktree-stage2-milestone4-blobref-owned-view`.

## Immutable candidate

- parent: `af7899f41c489fe763ce1a64c5468083570979e2`
- head: `08f8dd5cf20842f79996fae9eb7b0924f074a084`
- tree: `19c8706d6bc3d1dbe9217b4f8386b19c66f027a8`
- parent..head full-index SHA-256: `d7217fafa02e3c50a6c10b7e3a7a0985697b4ba82beb1ba896d2cf636f34d71f`
- a12 (`a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3`)..head full-index SHA-256: `2a06b554cea5f28a24117dfb52c3e24be9ddc408bbb59405ea66b051b73ddb47`
- stable patch-id: `472848104c8fd79ddb3d1d7a4aa96b6a0690a703`
- exact scope: `packages/lix/src/forktree/{blob.rs,mod.rs,serving.rs,tests.rs,view.rs}`

All five supplied blob IDs, the remote ref, commit/tree/parent, scope, both diff
hashes, and patch-id independently reproduce. The detached candidate worktree
was clean.

## Actual logical closure

The source-derived acquisition entry is `open_coherent_view`, which performs
the sole `begin_read` and passes its owned `StorageAdapterReadScope` to
`open_coherent_view_on_read`. The retained `CoherentView::read()` feeds:

- state point/range: `state_point`, `state_range`, ordered-tree lookup/scan and
  state decoding;
- topology: `CommitTopologyReader::{new, read, load}` and its private positive
  topology batch, plus `load_commit_member_records` for semantic members;
- blob ownership/read: `CoherentView::{bind_blob, load_blob_bytes_many,
  load_blob_ranges_many}` through private `bind_state_blob_ref`,
  `load_blob_bytes_many_on_read`, `load_blob_ranges_many_on_read`,
  `load_manifests`, and `load_required_chunks`.

No helper in this actual closure starts a second read. `AuthenticatedBlobRef`
is opaque, caller-supplied digest/size/ObjectId inputs are gone, the private
instance brand rejects cross-view row/ref reuse, and storage/decode failures use
`?`. The only relevant `.ok()` calls are the benign
`binary_search_by(...).ok()` conversions in authenticated ordered-tree lookup.
No legacy space/module, detached physical reader, fallback, or persisted/global
cache was added.

The unchanged function-scoped scanner resolved `open_coherent_view` and proved
its one direct `begin_read`; evidence digest
`5c88ddd5d85b27e8031ddf7d47f5183c8fb79d9c2e5f765abeb540325aec8fdb`.
Its remaining lexical reds are conservative unqualified-name collisions:
unrelated `new`/`decode_id`/engine `BinaryCasContext` functions enter the
syntactic closure. Manual qualified-call inspection above classifies those
scanner reds; it does not clear the independent range-authentication blocker.

## Terminal blocker: same-length manifest transplant passes range reads

`bind_state_blob_ref` parses `blob_hash` into the opaque ref's `semantic_id`
and separately copies the row's sole manifest ObjectId. The only common
manifest-owner check, `validate_manifest_owner`, compares
`manifest.logical_bytes` with `expected_size`.

Full reads subsequently reconstruct every chunk and check
`BlobId::from_content(bytes) == reference.semantic_id`. Range reads do not:
they load only intersecting chunks, authenticate each ObjectId/domain/length,
and return bytes after a length check. They never bind the manifest or returned
range to `reference.semantic_id`.

Therefore an authenticated `lix_binary_blob_ref` row for blob A can be changed
to carry same-length manifest B while retaining A's `blob_hash`. Both the row
and B's manifest/chunks authenticate, but `load_blob_ranges_many` returns B's
bytes under A's semantic BlobId. The existing size-transplant control changes
the size and cannot detect this equal-size case. The current manifest stores a
raw full-content digest and ObjectIds, while canonical multi-chunk `BlobId`
uses fixed-manifest chunk hashes, so ObjectIds alone cannot prove equality to
the semantic BlobId.

## Required narrow successor oracle

Before any bytes escape a range read, authenticate the row's semantic BlobId
against the selected manifest under the same retained `StorageRead`. Preserve
one object authority, no fallback/legacy reader, and no second view. Add a
deterministic equal-length A/B transplant where the row keeps A's BlobId and
size but points to B's valid manifest; both full and range reads must fail
closed. Healthy full/range reads and same-view/cross-view controls must remain.

This is a source-only milestone; no runtime/build matrix was run while the
declared compiler frontier remains red.
