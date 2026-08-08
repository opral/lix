# Minimum parsed-file and 64 MiB Blob landing acceptance

Status: immutable TEST/REPORT-ONLY package. No production edits, W5 logic,
compiler build, adapter runtime, benchmark, comparator, PR, or merge is part
of this package.

## Exact lineage and reused frozen oracles

This package is anchored directly to approved d6b:

| item | value |
|---|---|
| anchor ref | `origin/codex/forktree-stage2-commit-catalog-failclosed-1f742` |
| anchor head | `d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768` |
| anchor parent | `1f742a382c755399b8a49ab536c4f6dc55fffdd8` |
| anchor tree | `641654079f60fcd1c9ff9ccbbd06d3edcabe4096` |
| accepted W4 oracle | `origin/codex/forktree-w4-acceptance-d6b` at `b1dd25ebc90e95304709fbbafcc662c144b0449c` |
| W4 tree/diff/patch | `7632519278f18665bd1cd32590d031e817df0a65` / `e74e45c3210a4f254923c7e81ea38654aca07229e9de079550dca4a0aa60be44` / `3e12a72f876a26e895121046014b167599e781e2` |
| accepted Cut B reader oracle | `origin/codex/forktree-cut-b-reader-acceptance-d6b` at `e92ea2e505ee3d96abbb529dbaedb23d4908ff42` |
| Cut B tree/diff/patch | `0d0797c024706beb1510cb2f0f88f8414a9a0c96` / `eeaf1b5b6adc0730f679fd04a865286f3e8bcbeef65c9a718f0fd00fa87d6f20` / `de28e24fc96f1e76fa323405000f6df482625570` |
| current-state reader prerequisite | `origin/codex/forktree-stage2-reader-acceptance-d6` at `8b0cf91387ffc86851b99029bdd8942938ba2be6` |

This stage consumes the accepted W4 and Cut B contracts; it does not redefine
their semantics. It is applicable only after the current-state reader
successor, or a newer explicitly frozen successor, is accepted.

The d6b anchor is not runtime-qualified. No compile or adapter runtime was
run for this package.

## Minimum stage boundary

This stage qualifies one representative parsed-file/blob vertical slice:

```text
public file mutation
  -> plugin-parsed semantic rows + file descriptor/history rows
  -> one authenticated BlobId/BlobManifestV1 edge
  -> ordered 1 MiB BlobChunkV1 objects
  -> one CoherentView read for parsed file, history, roots and reopen
```

It covers one ordinary file create/update/branch/diff/merge/history/reopen
flow and one 64 MiB payload represented as exactly 64 canonical 1 MiB chunks.
It introduces no file format, payload owner, manifest index, locator, cache,
compatibility reader, migration, or W5 implementation.

Explicitly excluded: 512 MiB payloads, large-history/row scaling matrices,
comparator/alternative-layout experiments, and performance claims. Resource
measurements are limited to correctness counters needed to prove unchanged
chunk reuse and final-reference ownership.

## Sole authority and one-view contract

The semantic file row owns the logical BlobId and exactly one authenticated
manifest edge. `BlobManifestV1` owns only ordered immutable chunk references;
its private canonical BlobId is an integrity check. Plugin-parsed rows remain
owned by plugin semantic registry/owner rows and the ordinary state tree.

Every read, file materialization, plugin parse, file-history lookup, GC-root
collection and merge-registry lookup uses one retained
`CoherentView`/`StorageRead` supplied by the accepted current-state facade.
That view authenticates selectors, roots, commit/catalog ancestry and selected
semantic rows before output. A helper may not reacquire a read, pair roots
from another view, accept a caller root/manifest, or use a mutable current
registry for historical merge state.

Readers return validated BlobIds and semantic rows only. They never copy
archive/WASM/blob bytes into a second authority and never mutate selectors,
epochs, GC queues or receipt state.

## Parsed-file lifecycle oracle

Use a deterministic small fixture with one plugin-parsed file and one ordinary
binary file. Include valid registry/owner rows, plugin archive/WASM BlobId,
descriptor, directory parents and file-history rows.

Required sequence, Memory first and then each durable adapter:

1. publish registry/owner, directory, descriptor and file bytes;
2. read parsed rows and exact raw bytes from one coherent view;
3. update content and one parsed semantic field; verify plugin selection, row
   identity, metadata and BlobId change exactly once;
4. branch, make disjoint source/target edits, diff and merge; verify ordinary
   three-way results and no duplicate semantic file history;
5. query `lix_file_history` before/after update, branch and merge; verify path,
   observed commit, source ChangeId/CommitId, order and parsed values;
6. flush/drop/reopen and repeat point, bounded range, parsed, history, branch
   and merged-state reads with identical healthy outputs;
7. retire one file root while retaining a second shared-chunk root, then use
   the existing W5 final-reference handoff: unique objects may be reclaimable,
   shared objects survive until the final authenticated root.

Preserve path collisions, directory-parent closure, tracked/untracked fallback,
file-id scope, path history, plugin generation and error classes. Missing
published state must not become an empty filesystem/registry; only explicit
bootstrap absence is valid.

## BlobId and bounded partial-read oracle

For each healthy state, assert that the state row's BlobId and sole manifest
edge authenticate content; full and range reads use one view; ranges touching
first, middle, last and chunk-boundary regions validate selected chunk domain,
length, bytes and digest; and no caller manifest/object ID or old CAS query is
accepted. Missing/wrong-kind/malformed manifest/chunk, wrong owner/size,
forged BlobId, digest mismatch, duplicate/out-of-order edge or view mismatch
must fail closed before returning bytes.

This is bounded by selected chunks and range output, not a scaling/comparator
gate.

## 64 x 1 MiB unchanged-chunk reuse

Use exactly 64 MiB / 64 canonical 1 MiB chunks:

1. publish `/landing/large.bin`, record BlobId, manifest ID and ordered 64
   chunk ObjectIds;
2. flush/drop/reopen and verify identity and bytes;
3. update one selected 1 MiB region through the public file path;
4. prove exactly 63 unchanged chunk ObjectIds/bytes are reused, the changed
   chunk receives a new authenticated ObjectId, and the new BlobId/manifest
   order/content are correct;
5. attempt a different same-size manifest/content identity under the old
   semantic owner and require owner/BlobId/digest validation to fail closed;
6. remove or corrupt one selected manifest/chunk after reopen and require a
   typed failure, never canonicalization or fallback.

No 64 MiB chunk format or duplicate raw payload authority is permitted. This
is an identity/resource counter, not a performance claim.

## Corruption and final-reference matrix

Every case produces no row/bytes/root and no write, selector, epoch, receipt
or GC-progress mutation:

* missing/malformed/wrong-kind descriptor, directory, blob-ref, manifest or
  chunk; non-canonical/wrong BlobId, wrong scope/size, forged owner, same-size
  substitution, truncated/duplicate/out-of-order edge or digest mismatch;
* missing/malformed/unknown/version-invalid registry/owner, generation or
  manifest/API mismatch, invalid archive/WASM BlobId;
* missing selected row, missing parent, path collision, global tombstone,
  missing/remapped historical catalog member, forged source ChangeId/CommitId,
  wrong ordinal/generation, or a second read/view injected into a valid one;
* stale publication/selector/global epoch or failure after plan preparation.

Initial empty/bootstrap state remains explicit and valid. All other missing
selected owner/control state fails closed.

W4 publishes roots/selectors; W5 owns queue processing and deletion. Current
file state roots manifest/chunks; retained history/checkpoint and plugin
archive/WASM BlobIds remain roots; open upload receipts remain roots until
completion/abort; branch/file retirement removes only its authenticated root;
shared chunks survive until the final root. No new GC index, legacy receipt
scan or cleanup-debt authority is allowed.

## Exact RocksDB then SlateDB commands

Run each focused cell independently, RocksDB first and SlateDB second, only
after a qualifying successor exists. These are existing frozen entry points:

```text
# Public parsed-file/read/history controls (Memory + durable adapters)
cargo test -p lix --test integration fs_api -- --nocapture --test-threads=1
cargo test -p lix --test integration sql::lix_file_history -- --nocapture --test-threads=1
cargo test -p lix --test semantic_merge -- --nocapture --test-threads=1
cargo test -p lix_benchmarks --release --test exact_file_read_benchmark --features storage-benches,slatedb -- --ignored --nocapture --test-threads=1

# Existing deterministic 64MiB accounting control: RocksDB first
cargo run -p lix_benchmarks --example cas_publication_epoch_qualification --features storage-benches,slatedb -- rocksdb /safe/evidence/w4-64m-rocks 0 1

# Same control: SlateDB second
cargo run -p lix_benchmarks --example cas_publication_epoch_qualification --features storage-benches,slatedb -- slatedb /safe/evidence/w4-64m-slate 0 1
```

The CAS accounting example is a pre-W4 legacy red control: its
`deduplicated_64mib` fixture/counters may be reused, but it is not final W4
approval until the landing worker routes it through the transaction owner and
deletes the legacy writer. The final W4 public adapter target/filter and
binary SHA must be named by the implementation; this report does not invent a
missing target. Do not run 512 MiB, scaling, or comparator commands.

## Acceptance rule and exclusions

Approve only a later immutable candidate with zero legacy reader/writer
residue, exact current-state-reader prerequisite, parsed-file/plugin/history/
branch/merge/reopen correctness, bounded partial-read and same-size
substitution/corruption fail-closed controls, 63/64 unchanged chunk reuse,
and identical semantic digests on RocksDB then SlateDB with final-reference
evidence.

Explicitly excluded are 512 MiB, 1K/10K/50K scaling, performance comparator or
alternate-layout claims, W5 implementation/tuning, and any second reader,
CAS writer, format, cache, migration or compatibility route. This package is
not runtime qualification or merge approval.
