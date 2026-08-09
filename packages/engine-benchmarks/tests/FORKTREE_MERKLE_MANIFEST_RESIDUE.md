# ForkTree Merkle manifest fixture/residue gate

Test/report-only successor for the canonical Merkle manifest/root hard cut.
This package is a direct child of `9e209e41775892c6ba42ad790efdf7e414111c7e`
and deliberately does not edit the R4-owned `forktree/blob.rs`,
`forktree/publication.rs`, session, or splice callers.

## Contract

The migrated fixture closure uses `BlobManifestV1::from_merkle_root` or
`build_blob_merkle_tree`; no fixture constructs a flat `ordered_chunks` /
`content_digest` manifest. The shared-reference GC fixture uses canonical
1 MiB leaves, while small upload/duplicate-owner controls use the authenticated
single-leaf test builder. The controls cover:

- upload completion and rollback/stale publication boundaries;
- duplicate-owner and same-size substituted content;
- wrong root, wrong geometry, missing root, wrong domain, malformed leaf/internal
  proof, missing proof path, and cycle/path substitution;
- range proof StateKey binding and exact requested ordinals;
- shared and final-reference reachability, checkpoint retention, and cold-open
  lifecycle fixtures already present in the ForkTree test module.

The executable source gate is:

```text
packages/engine-benchmarks/tests/forktree_merkle_manifest_residue.sh $PWD
```

It scopes the `BlobManifestV1` model section and the `BlobManifest` GC edge
arm, so `UploadPartV1::ordered_chunks` is not misclassified. After R4
composition, pass the changed production closure through the same policy and
require zero `ordered_chunks`/`content_digest` flat-manifest authority,
whole-base SHA witness, `BlobId::from_content`/`from_chunks`, or
`from_authenticated_chunks` constructor in that closure.

## Frozen checks

- `cargo fmt --all -- --check`: PASS
- `git diff --check`: PASS
- source residue gate: PASS
- `cargo check -p lix --features storage-benches`: inherited RED, 20 errors,
  all in untouched `forktree/blob.rs` and `forktree/publication.rs`; log SHA-256
  `f0a00e0e8dba3cacd731a3f9b2274ed7f2feac89963bbef06fefe3ab72ab35a1`
- `cargo test -p lix forktree::merkle --lib --no-run`: inherited RED, 20
  errors, all in the same untouched R4 callers; log SHA-256
  `9892aee5aa6d14dd5abe694cfe6046cdddaa2802fbd9f1e6fd347bfb28ca9dfa`

No adapter runtime is claimed: the current parent cannot compile until R4
lowers its production blob/publication/session/splice consumers to the Merkle
manifest. No production caller was changed by this package.
