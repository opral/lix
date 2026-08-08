# Cut B filesystem/plugin reader acceptance package

Status: immutable TEST/REPORT-ONLY contract. No production logic, W5 logic,
storage format, build, adapter runtime, branch merge, or PR mutation is part
of this package.

## Immutable anchors and applicability

The package is based on the approved d6b lineage:

| item | value |
|---|---|
| package anchor ref | `origin/codex/forktree-stage2-commit-catalog-failclosed-1f742` |
| package anchor head | `d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768` |
| anchor parent | `1f742a382c755399b8a49ab536c4f6dc55fffdd8` |
| anchor tree | `641654079f60fcd1c9ff9ccbbd06d3edcabe4096` |
| current-state reader prerequisite | `origin/codex/forktree-stage2-reader-acceptance-d6` |
| prerequisite head | `8b0cf91387ffc86851b99029bdd8942938ba2be6` |
| prerequisite tree | `5bfc6d63011789c85b70fd0675ffb8a2216210c0` |
| prerequisite d6b..head diff SHA-256 | `b75fed7dace12928a800eb1f92cb5abc025f3e13d6fa36f19013d8c96114aee3` |
| prerequisite stable patch ID | `912561f9e9f11d3341225354c9df119d681180ac` |

This Cut B package is applicable only after the current-state reader
successor, or a newer explicitly frozen successor, has passed its own source
and review gates. It must not be used to restore the old tracked-head reader
just because the prerequisite is compiler-red or incomplete. The prerequisite
owns the current-state facade; Cut B consumes it read-only.

The d6b anchor is not runtime-qualified. No compile or adapter runtime was
run for this package.

The source-only oracle was run once on the anchor: exit `1`, log SHA-256
`e2b11cee996a8b29b724ac5f3efa45c212ad985deb481d7c7053a830c9f2227b`. It
reported the expected legacy `TrackedHeadContext`/`TrackedState*` acquisition,
retained-snapshot scan, and `BranchHeadControl` residue in the two primary
readers. This is an intentional pre-cut RED result, not runtime evidence.

## Cut B authority boundary

Cut B replaces only filesystem and plugin read-side state acquisition. One
authenticated `CoherentView`/`StorageRead` supplied by the current-state
facade must cover current serving roots, retained commit roots, file/plugin
semantic rows, and GC root extraction. It must not create a second view,
cache, index, locator, manifest, byte store, or persisted root space.

Sole intended owners:

| fact | owner |
|---|---|
| file, directory, blob-ref rows and parsed paths | authenticated semantic state tree through `CoherentView` |
| plugin registry and per-file plugin owners | plugin-owned semantic `lix_key_value` rows (`PLUGIN_REGISTRY_KEY`, `PLUGIN_OWNER_KEY`) |
| file/archive/WASM bytes | existing authoritative BlobId/CAS and plugin archive owner; readers return validated BlobId roots only |
| chronology and retained commit selection | authenticated CommitCatalog/Commit objects and serving selectors, supplied by the facade |
| compiled plugin matcher | existing ephemeral `PluginCatalogCache`; never durable authority |

`BlobId` is a validated logical root. It is not a payload copy, raw CAS
reader, alternate directory, or second root index.

## Permitted production surface

Only these reader/plumbing paths may change in the eventual successor:

* `packages/lix/src/filesystem/read.rs`: `collect_gc_binary_blob_roots`,
  `blob_id_from_snapshot`, and `FilesystemIndex::from_live_batch` plumbing;
  preserve path, scope, directory closure, collision, history and BlobId
  semantics.
* `packages/lix/src/plugin/registry.rs`:
  `load_plugin_registry_at_commit`, `collect_gc_wasm_blob_roots`, and typed
  reader plumbing. Preserve `PluginRegistry`/`PluginFileOwner` validation and
  the semantic row writers; do not add a writer here.
* Direct signature/re-export plumbing only in
  `filesystem/mod.rs`, `plugin/mod.rs`, `gc.rs`,
  `sql2/providers/file.rs`, and `session/merge/branch.rs`.

The current-state prerequisite owns the `live_state`/`tracked_state` facade.
Cut B must not modify its production owner, ForkTree selectors/trees,
publication, reachability, transaction writers, binary CAS implementation,
changelog, storage adapter, session writer, or W5 GC scheduler. `file_history`
is an acceptance consumer, not permission to broaden this cut.

## One-CoherentView contract

Every filesystem/plugin/GC-root operation must:

1. obtain one retained `CoherentView`/`StorageRead` from the current-state
   facade;
2. authenticate global and branch selectors, repository/branch roots,
   retained CommitCatalog roots, and semantic head before output;
3. reuse that read for current rows, retained history, plugin registry/owner
   rows, BlobId roots and filesystem materialization;
4. reject any caller-supplied root, branch control, selector generation,
   storage space, or detached root/read pair;
5. remain stable if publication/GC advances after the view is acquired;
6. perform no write, selector mutation, epoch rotation, repair, retry
   publication or GC queue mutation from a read/error path.

No helper may call `begin_read` per branch, retained commit, root collector,
or plugin registry. No cache may outlive or replace the bound view.

## Filesystem semantic contract

`FilesystemIndex::from_live_batch` remains the parsed-file materializer. Preserve:

* exact `filesystem_schema_keys` selection and global/branch/untracked/file
  scope rules;
* tracked fallback for untracked directory ancestry;
* strict JSON decoding of file, directory and blob-ref snapshots;
* directory-parent closure and unreachable-directory errors;
* file/directory path collision errors in either insertion order;
* file descriptor/blob/directory history and observed-commit ordering;
* `BlobId::from_hex` as the only blob identity conversion.

Missing required selected rows, wrong schema/entity/file scope, malformed
JSON, non-canonical BlobId, missing parent, wrong-kind state or forged owner
must fail closed. Only an explicit bootstrap/empty state from `CoherentView`
may be empty; missing published state must not be synthesized as an empty
filesystem.

Parsed-file, file-history, branch, small edit, diff, merge and undo/redo
observable semantics must be byte/row/order equivalent to the pre-cut owner.

## Plugin semantic contract

`PLUGIN_REGISTRY_KEY` is the sole registry entity and `PLUGIN_OWNER_KEY` the
sole per-file owner entity. Preserve registry version/count, sorted unique
plugin keys, manifest agreement, generation digest, runtime API, archive hash,
WASM hash, file-id storage identity, branch/schema/entity identity and sorted
owner schema keys.

Missing required registry/owner rows, wrong-kind or wrong-scope rows,
malformed/unknown/version-invalid snapshots, generation mismatch, invalid
plugin/archive/WASM hash, and owner mismatch must fail closed before any byte
read or root output. A legitimately empty registry is explicit bootstrap, not
raw absence after publication.

Root collectors return only a deduplicated set of validated `BlobId`s. They do
not load/copy archive or WASM bytes, create a CAS space, or use registry
metadata as a byte authority.

## GC-root and merge-registry contract

GC root extraction must use the same bound view for current and retained
filesystem/plugin roots, deduplicate equal BlobIds, include valid untracked and
retained-history roots, and fail closed on missing/malformed selected rows.
It must not alter queue/epoch/retirement state; W5 owns publication races and
physical reclamation.

Merge registry loading for base/target/source commits must use the retained
authenticated commit views supplied by the facade. Existing three-way plugin
generation conflict semantics, plugin selection, archive/WASM BlobIds,
metadata and deterministic ordering remain unchanged. No mutable current
registry may substitute for a historical view.

## Corruption/fail-closed matrix

Each case must return a typed error, produce no row/root, and leave selectors,
repository/branch roots, epochs and GC progress byte-identical:

* missing global/branch selector;
* selector key/value branch mismatch;
* missing or wrong-kind repository, branch, state, commit or catalog root;
* state node missing, truncated, malformed, duplicate/out-of-order or invalid
  range summary;
* row key/entity/file/scope mismatch, invalid value/NULL/tombstone or global
  tombstone;
* missing/malformed/wrong-kind file descriptor, directory, blob-ref,
  registry, owner or retained historical row;
* missing parent, path collision, unreachable directory or non-canonical
  BlobId;
* missing/remapped CommitCatalog or ChangeCatalog entry, forged source/member,
  wrong ordinal/generation/ChangeId/CommitId;
* plugin generation, manifest, API, archive/WASM hash or owner mismatch;
* a second read/root/view injected into a valid view;
* selected BlobId member missing/corrupt/wrong-domain during root collection.

No old reader may silently retry or canonicalize these cases. No write or
selector/epoch/receipt/progress mutation is allowed on failure.

## Legacy reader/CAS red controls

The following symbols and spaces must disappear from the two primary readers;
they may appear only in explicit negative-test/report text:

```text
TrackedHeadContext
TrackedHeadContext::new
TrackedStateStoreReader
TrackedStateScanRequest
TrackedStateFilter
TrackedStateReadColumns
scan_live_batches_for_controls
load_projected_batch_at_commit
load_retained_commit_snapshots_for_schemas
BranchHeadControl
binary_cas::BINARY_CAS_*
BinaryCasContext
tracked_head
tracked_state/storage.rs
tracked_state/tree.rs
tracked_state/codec.rs
load_commit_state_manifest
load_change_record_by_id
scan_change_records_from_commit_deltas
StorageSpace::mutable
```

The Cut B reader must not restore tracked-head/TrackedState/branch-control or
raw CAS readers, old codecs, migration, compatibility reader, fallback scan,
dual writer, payload copy, durable cache/index, or second root authority.
The semantic `BlobId`, plugin row keys/validators and ephemeral
`PluginCatalogCache` are allowed.

## Required source oracle

Run first, without compiling:

```text
bash test-reports/stage2-filesystem-plugin-reader-d6b/verify_source_contract.sh
```

It must be RED on d6b because the predecessor still contains the old reader;
the recorded red is evidence that the oracle is discriminating. It must PASS
on a later Cut B candidate only when the primary readers have zero forbidden
acquisition symbols, retain `BlobId` and semantic validators, and show
`CoherentView`/facade plumbing. It must not inspect or mutate production
storage and must not accept a permissive empty fallback.

## Public test matrix

The eventual candidate must run Memory first and then identical RocksDB and
SlateDB fixtures. Required focused controls are:

1. parsed file create/update/rename/delete; directory-parent closure;
   tracked/untracked directory fallback; duplicate path and collision in both
   insertion orders; malformed descriptor/blob-ref; wrong kind/scope; missing
   selected row; non-canonical/wrong BlobId;
2. plugin valid/empty-bootstrap/deleted/missing-required/wrong entity or file
   scope/malformed/unknown field/wrong version/bad generation/invalid archive
   or WASM hash/mismatched owner; assert no byte read precedes failure;
3. one-view GC roots over current, untracked and retained history; equal
   BlobId dedupe; missing/malformed selected row failure; zero queue/epoch
   mutation;
4. base/target/source merge registry reads, plugin-generation conflicts,
   parsed-file and file-history exact order/identity/metadata;
5. flush/drop/reopen on both durable adapters, then repeat healthy reads and
   selected-row/BlobId corruption fail-closed cases;
6. no writes from readers, no old TrackedHead/TrackedState/CAS reader, and no
   source/view cache authority.

Suggested existing semantic hooks, to be confirmed against the successor's
exact test names:

```text
cargo test -p lix --lib filesystem::read::tests::from_live_rows_rejects_file_directory_namespace_conflicts -- --exact --nocapture --test-threads=1
cargo test -p lix --lib filesystem::read::tests::insert_entry_rejects_file_directory_namespace_conflicts_in_both_orders -- --exact --nocapture --test-threads=1
cargo test -p lix --lib filesystem::read::tests::from_live_rows_attaches_blob_refs_by_storage_scope -- --exact --nocapture --test-threads=1
cargo test -p lix --lib plugin::registry -- --nocapture --test-threads=1
cargo test -p lix --test integration checkpoint_gc -- --nocapture --test-threads=1
cargo fmt --all -- --check
cargo clippy -p lix --lib --tests --all-features -- -D warnings
```

The final candidate must record the exact public adapter target/filter,
binary SHA and semantic output digest for Memory, RocksDB and SlateDB. No
runtime claim is made for this d6b package.

## Acceptance rule

Approve only an immutable successor descended from the current-state reader
successor (or an explicitly frozen replacement) whose primary-reader delta is
limited to the permitted surface, source red controls are zero, and all
filesystem/plugin/GC/merge/history semantics and fail-closed controls pass on
both durable adapters. Block on a second view, raw storage escape, old reader,
permissive absence, byte copy, durable cache/index, second BlobId/root owner,
write/epoch mutation, or changed parsed-file/plugin/history behavior.

This package is an acceptance contract, not runtime qualification or merge
approval.
