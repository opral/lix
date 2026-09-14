# Detached historical repository bridge

This operator-only package builds format 68/71 → 74 migration from the pinned
historical revision `6e1efebb3953e8e4e5945fd9a9269a69803970c8` plus the reviewed
`bridge.patch`. It is not a dependency of the current engine or server runtime.
The patch checksum identifies the actual bridge implementation, while reports
retain the upstream base revision separately.

Run `./build.sh /absolute/new-checkout` from this directory, then
`./run.sh /absolute/new-checkout/target/debug/examples/historical_bridge COPIED_DIR REPORT.json --isolated-copy`.
Build verifies the patch, checks out the exact historical base, applies it, and
uses the locked dependencies. Run verifies the executable and patch digests.
`CARGO_TARGET_DIR` may select a separate target directory during build.

Only provide a separately copied, offline physical repository. Retain immutable
originals. The bridge captures source object hashes before opening, verifies
source-derived retained history, memberships, branch ancestry, current rows,
blobs and receipts, then seals all destination logical records. Its only schema
amendment is the exact historical account schema amendment. It restores original
standalone history records deleted by that historical migration; active state
is not restored. Unsupported source formats or proof differences fail closed.

`--verify-only` verifies the logical seal and refreshes the final closed physical
manifest (opening SlateDB can update physical metadata). `--manifest-only` prints
a raw manifest without opening storage. Disposable negative probes
`--negative-delete` and `--negative-alter` corrupt retained branch-ref history and
succeed only when verification detects the corruption. Never use these on a
migration destination intended for publication.

Next, copy the closed format 74 result to another isolated destination and run
the current detached migrator. Bind historical `verifiedDestinationObjects` to
current `sourceObjects` exactly. The current server's offline adoption command
requires both verified reports and unchanged original source objects; it
activates the authority under an exact-content witness before catalog Create.
All serving writers must remain stopped throughout publication.

This native operator does not provide the historical browser WASM artifact.
Returning format 68/71 browser repositories still require a separately loaded
historical bridge with the same proof before current migration can proceed.

## Offline authority adoption

Build the current server with `--features offline-migration`. With the same
operator S3 configuration and all serving hosts stopped, run
`lix-server adopt-staged-repository /absolute/adoption.json`. The JSON input is:

```json
{
  "schemaVersion": 1,
  "hostedRepositoryId": "explicit-control-plane-repository-uuid",
  "sourceStorageId": "immutable-original-physical-uuid",
  "stagedStorageId": "separately-uploaded-current-physical-uuid",
  "sourceObjects": [{"key": "relative-object-key", "bytes": 123, "blake3": "hex-digest"}],
  "currentProofPath": "current-migration-proof.json",
  "historicalProofPath": "historical-bridge-proof.json"
}
```

Proof paths resolve relative to the manifest. `historicalProofPath` is null when
the current detached migrator directly supports the source. The operator must
supply the hosted identity from the control plane; embedded repository identity
is not sufficient. Each original object is checked before staging activation
and again immediately before publication. Source and destination must differ.
Existing, deleted, or conflicting catalog identities are never overwritten.

A prepared report is durable before catalog Create. Retrying a completed
publication with the same manifest returns its report. A prepared but unpublished
stage is rechecked before retry. A crash during activation before the prepared
report is saved fails closed: retain that stage for diagnosis and create a fresh
stage from the verified migration output; never edit or delete the original.
The command requires the global stopped-writer barrier because catalog listing
and object hashing are not a cross-repository transaction.

## Retain deleted repository sources

`lix-server retain-tombstone /absolute/manifest.json` is an offline,
metadata-only operation. Its manifest contains `schemaVersion: 1`,
`hostedRepositoryId`, `controlInventoryPath`, `controlInventoryBlake3`,
`controlDeletedAt`, `catalogBlake3`, and `physicalSources`. Each physical source
has `storageId` and the complete `objects` array of `key`, `bytes`, and `blake3`.
The sources must cover the active physical ID and every retired ID exactly.

The command requires the target control row to have exactly the reviewed
nonempty `deleted_at` in the digest-bound control inventory. It verifies source
bytes, requires the exact reviewed live catalog hash, saves its original bytes
and version in a prepared report, and publishes the tombstone using catalog CAS.
It preserves unknown catalog metadata, physical IDs, and all physical objects;
only state, admission, and fingerprint change. It never opens storage or calls
cleanup. Exact retries revalidate retained bytes and recover the prepared report.

Ordinary lifecycle DELETE has different behavior and deletes physical sources.
Never use it for this retention operation. Keep serving/lifecycle writers and
automatic restarts stopped throughout verification and publication.
