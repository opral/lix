# ForkTree Stage 2 point/BlobRef function-scoped source gate

Status: test/report-only static gate. It reads immutable Git objects and does not compile or execute Lix.

The reviewer supplies the exact public logical point/BlobRef entry name from the immutable owner handoff. The scanner builds a function-level call closure across ForkTree view/catalog/topology/state/blob and the public binary-CAS/engine seam. It requires:

- exactly one `begin_read` in the public entry and zero in every helper;
- one retained read through selector, catalog, topology, state, BlobRef, manifest, chunk, and content authentication;
- no fallback, retry, legacy owner, cross-view cache, restored module, or restored durable space;
- propagation of storage, decode, identity, and authentication failures;
- no function-local conversion of those failures to absence/default.

## `.ok()` policy

The prior lexical policy was overly broad. This gate identifies the immediate method whose `Result` is consumed by `.ok()`. It permits only `binary_search(...).ok()`, a pure in-memory conversion from insertion-point search to optional match. It rejects `.ok()` on storage, decode, selector, catalog, topology, state, BlobRef, manifest, chunk, hash, and all other receivers. It also reports `unwrap_or*`, `if/while let Ok`, and wildcard `Err(_)` branches inside the selected call closure. `ok_or(...)?`, which strengthens an `Option` into a propagated error, is not an erasure.

The embedded self-test must remain green:

```sh
python3 packages/lix/tests/forktree_stage2_point_blobref_audit.py --self-test
```

Candidate command:

```sh
python3 packages/lix/tests/forktree_stage2_point_blobref_audit.py \
  --repo . --baseline af7899f41c489fe763ce1a64c5468083570979e2 \
  --target <immutable-head> --entry <exact-public-entry> --strict
```

Automation is intentionally supplemented by manual source review. The reviewer verifies the exact entry/helper signatures, source-level read ownership, BlobRef-to-manifest binding, manifest/chunk length/order/hash checks, missing/malformed/mismatched corruption behavior, positive-cache lifetime, and absence of an alternate BlobId directory or binary-CAS fallback.
