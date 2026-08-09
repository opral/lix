# Transaction BlobDataReader necessity audit

Status: frozen source audit; no production change.

## Immutable anchor

- Reviewed head: `31449d223c37b712e3e005e7d8c7b620707c5929`
- Tree: `5f3a5d0ffc9f07553c034768d8f7da1cb6567697`
- Parent: `13ddba800e67b6b6de331517feb5e5a741f816af`
- Production closure successor: `origin/codex/w1b5-plugin-blobref-statekey-closure-13dd`

## Terminal conclusion

No additional deletion-safe production cut exists on this frontier. The
remaining `BlobDataReader` reads are transaction-owned staged/write or plugin
preflight paths. They are not public committed-file serving: public read-only
providers are registered with the authenticated reader, while write providers
use the transaction overlay so unpublished bytes remain visible.

The read API is nevertheless an unfinished migration boundary. A committed
hash miss currently fails closed with `LIX_UNSUPPORTED_SQL`; removing the API
now would either break valid cold plugin/preflight flows or require inventing
an unauthenticated fallback. The next cut must first supply an authenticated
owner for every committed payload request.

## Call-chain classification

### Staged/write-only paths

- `packages/lix/src/sql2/providers/file.rs:231-274,382-459`:
  `LixFilePayloadReader::Write` is installed only by the write-provider
  registrations. Its `load_blob_bytes_for_files` call at
  `packages/lix/src/sql2/providers/file.rs:5435-5475` serves DML
  conflict/pre-image/RETURNING work and may observe unpublished transaction
  bytes.
- `packages/lix/src/sql2/context.rs:362-363,506-519`:
  `WriteContextBlobDataReader` is the transaction-owned write capability.
- `packages/lix/src/transaction/staging.rs:2854-2920`:
  `load_staged_file_bytes_many` resolves main and auxiliary inline payloads by
  their eventual `BlobId`, before commit has placed them in durable storage.
- `packages/lix/src/transaction/context.rs:8810-8818` and
  `8133-8168`: the transaction overlay is checked first, then the committed
  base is requested only for unresolved hashes.

### Plugin preflight / actor paths

The same staged-overlay loader is used at the following production call sites:

- `transaction/context.rs:2396-2398`: uncached plugin conflict factory WASM;
- `transaction/context.rs:2637-2655`: component materialization cold open;
- `transaction/context.rs:4275-4299`: cold plugin factory loading during
  registry reconciliation;
- `transaction/context.rs:4890-4909`: plugin actor cold replacement/splice;
- `transaction/context.rs:11114-11120`: owned-generation upgrade preflight.

These are pre-publication operations. They are not a public content reader,
but they do need committed bytes when the relevant plugin/materialization is
not already staged or cached.

### Fail-closed owner boundary

- `packages/lix/src/binary_cas/context.rs:14-18` defines the explicit
  unsupported error for hash-only reads.
- `packages/lix/src/binary_cas/context.rs:92-97` constructs the old reader,
  and `120-126` always returns that error. No successful production public
  fallback remains.
- Public file registration uses `ctx.authenticated_blob_reader()` at
  `packages/lix/src/sql2/providers/mod.rs:392-419`; the public authenticated
  loader binds StateKeys and does not consume path-index payload cache data.

## Why the current APIs cannot be deleted safely

1. Staged inline and auxiliary payloads have no durable StateKey yet. Their
   transaction-owned identity is the eventual content hash, so the overlay
   lookup is necessary before commit.
2. Component materialization rows do have an authenticated
   `lix_binary_blob_ref` owner and can be migrated first, but the current
   helper accepts only hashes and does not carry the row StateKey through every
   caller.
3. Plugin registry WASM is persisted in `PluginRegistryEntry.wasm_blob_hash`
   (`packages/lix/src/plugin/registry.rs:60-63`). Installation stages the
   extracted WASM as an auxiliary payload (`transaction/context.rs:3313-3382`)
   but does not publish a corresponding authenticated BlobRef owner. A hash
   alone cannot be safely lowered to a ForkTree read.

Therefore deleting `BinaryCasContext::reader`, `BlobDataReader`, or
`load_transaction_blob_bytes` now would either break cold committed plugin
operations or force a compatibility/raw-CAS path. Neither is acceptable.

## Required next hard-cut prerequisites

1. Add one canonical authenticated owner for extracted plugin WASM (a typed
   BlobRef/manifest StateKey, or an equivalent authenticated registry payload),
   with exact plugin/branch identity, BlobId, manifest, chunks, and size. Do
   not add a second authority.
2. Extend the transaction-owned prepared plan to carry the exact StateKey and
   expected semantic BlobId for every committed materialization/WASM request.
   Keep staged bytes as an overlay, but validate them against that same
   identity rather than treating a hash match as ownership.
3. Replace `load_transaction_blob_bytes` with a request-aware loader that uses
   staged authenticated receipts/manifests for unpublished data and the
   transaction's retained authenticated read for committed data. Missing,
   malformed, wrong-kind, identity-substituted, or conflicting duplicate
   owners must fail closed.
4. Split DML pre-image loading into staged-overlay and authenticated committed
   branches while preserving atomic rollback, conflict, and RETURNING
   semantics.
5. Add cold/uncached plugin conflict, materialization reopen, staged
   replacement, wrong-owner/same-BlobId, corruption, and final-commit tests on
   Memory, RocksDB, and SlateDB.
6. Only after all production call sites above consume the request-aware
   authenticated loader should the hash-only `BlobDataReader`/reader API be
   deleted. The write-side CAS staging writer remains a separate concern.

## Evidence boundary

The predecessor closure gates remain valid on the immutable production head:
the focused StateKey/plugin/upload controls and Memory/RocksDB/SlateDB exact
file-read controls are recorded in the handoff for `31449d`. This report adds
no compatibility path, cache, second read, or source modification.
