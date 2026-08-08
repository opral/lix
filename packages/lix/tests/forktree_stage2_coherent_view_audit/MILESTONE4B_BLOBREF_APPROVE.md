# Stage2 Milestone 4B BlobRef source review — APPROVE

Verdict: **SOURCE APPROVE** for immutable
`origin/codex/forktree-stage2-milestone4b-blob-manifest-identity`.

This is an intentionally non-runnable source milestone. Approval covers the
coherent-view/BlobRef authority correction only; it does not claim a runtime or
compile-green Stage2 system.

## Immutable candidate

- parent: `08f8dd5cf20842f79996fae9eb7b0924f074a084`
- head: `54e90dbf2bcf55c74de0be6ea4b217dc02cec89c`
- tree: `5a8da9f8b11d83bf8216e266beaf4042cee84068`
- parent..head full-index SHA-256: `c507282c79b8de8b9cdec3960157276efef2769e2866a895b4c0d015b77fa8f1`
- a12 (`a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3`)..head full-index SHA-256: `d5adb4a322dbf98a590d765c9ee2179a3a1e583211cb6a41c3fe2bf2cd786bae`
- stable patch-id: `242302af3d9db6ecb81f258570b1ed0ec99cde3c`
- exact scope: `packages/lix/src/forktree/{blob.rs,model.rs,tests.rs}`
- blobs: `blob.rs` `a9ab0f78cace48a036966fd8f53b0b3941ba2ef3`;
  `model.rs` `62209166b0168e094653dceacdd69ea6783d80ca`;
  `tests.rs` `483ea45dfad532ef4d744d6719a4b0bd1d5db878`

The remote ref, commit/tree/parent, three-path scope, blobs, both diff hashes,
and patch-id independently reproduce in a clean detached worktree.

## Authority and coherent-view audit

The source-derived logical acquisition entry remains `open_coherent_view`.
It alone calls `begin_read`; the retained `CoherentView` continues to feed
selector/root authentication, state point/range, topology/member traversal,
BlobRef binding, manifest reads, and full/range payload reads. The three-file
successor adds no `begin_read`, detached reader, fallback, cache, index, space,
or compatibility decoder.

The selected authenticated `lix_binary_blob_ref` row remains the sole visible
BlobId owner. `AuthenticatedBlobRef` still derives its semantic identity, size,
and sole manifest edge from that row and remains bound to the exact read
instance. `BlobManifestV1::canonical_blob_id` is an immutable, ObjectId-bound
integrity copy with parent-module visibility only. It is not facade-exported,
keyed, queryable, cached, or exposed through a getter.

There is one production manifest constructor. During the existing authenticated
upload traversal, its private streaming builder consumes each decoded chunk
after ObjectId/domain/length validation and delegates canonical identity to the
existing `ChunkHash`, `BlobId::from_single_chunk`, and `BlobId::from_chunks`
primitives. It introduces no competing BlobId formula or second writer.

Both `load_blob_bytes_many_on_read` and `load_blob_ranges_many_on_read` decode
the ObjectId-authenticated manifest and exact-compare its canonical identity
and size to the row-derived ref before calling `load_required_chunks`. Range
reads then fetch and authenticate only intersecting chunks; corruption outside
the requested range may remain latent, but no bytes can escape under a
different semantic BlobId.

The correction hard-cuts the manifest encoding/decoder. There is no old-format
branch, migration, fallback, or dual read/write path.

## Blocker discriminator

The focused test constructs two distinct, same-size `1 MiB + 1` payloads. A
state owner retaining A's BlobId/size is pointed at B's otherwise-valid
manifest. Both full and range reads must reject it, while B's valid `0..1`
range succeeds. The comparison is source-ordered before any payload-chunk
request. Existing wrong-schema, size-transplant, same-selector/different-read,
healthy full/range, and topology controls remain.

## Unchanged scanner and frontier classification

The frozen function-scoped scanner was run unchanged with
`--entry open_coherent_view`; target evidence digest:
`a5743499348cdc2f76e59f3b83b38fa81fd9613f6ad65cf8f9eca3f176141009`.
It resolves the exact entry and proves one direct `begin_read`. Its other reds
remain conservative unqualified-name/entry-lifetime classifications:

- unrelated same-named `new`, engine `BinaryCasContext`, and `decode_id`
  functions enter the lexical closure;
- state and BlobRef methods are called after the acquisition entry returns, so
  they are not syntactically reachable from `open_coherent_view` despite
  retaining its owned read;
- `CanonicalBlobIdBuilder::finish` uses `Option::unwrap_or_else` only to select
  the pending single-chunk hash when no complete 1 MiB chunk exists. It does not
  erase a storage/decode/authentication error;
- the allowed tree `.ok()` sites remain only
  `binary_search_by(...).ok()` absence conversion.

Manual qualified-call inspection therefore clears those scanner reds. No
manual override is needed for the former same-length identity defect: the new
manifest comparison directly closes it.

Author frontier evidence remains 219 errors/3 warnings for the library and
485/3 for focused test no-run, with no touched-module diagnostic; residue is
176. Per assignment, no runtime/build matrix was added before compile-green.
