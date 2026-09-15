# Current-format runtime and detached migration

The target runtime accepts repository format **81**, sync protocol **16**, and
remote SQL protocol **10**. Older protocol clients must be rejected before any
mutation. The reference server automatically migrates supported older authorities
inside its owned repository opener before serving requests. Concurrent opens share
the same migration. Compatible transport-only upgrades require no content scan.
The browser SDK awaits server migration internally; applications keep awaiting open.
The low-level current-format engine does not itself run historical migrations or
retire old source banks.

`lix::migration::inspect_repository(storage)` performs read-only inventory.
`lix::migration::migrate_repository(storage)` is available only with the
`offline-migration` Cargo feature, which the reference server enables for
automatic authority opening. It resumes the registered
copy-and-activate chain (full repositories v72–80 and partial v79–80), retains
source banks, and reports before/after format, role, and logical-content digests.
Historical preflight defaults to 250,000 changes and 512 MiB.
`migrate_repository_with_options` / `migrateStorageProvider(provider, limits)`
accept explicit positive limits; limit exhaustion preserves the source and fails
with `LIX_ERROR_MIGRATION_LIMIT_EXCEEDED`. Automatic authority opening uses these
default limits and verifies semantic preservation before publishing admission.
No migration chain exists for pre-v72 full repository record shapes; inventory
must identify these repositories and preserve them for a version-specific
export/import tool. Never silently reset them.

The v80→81 step changes only the protocol marker. Its digest excludes exactly
the protocol marker and mutation-revision key and covers all other registered
logical records, including blobs, history, pending work, and acceptance receipts.
Equal digests prove exact preservation. For v79, a bounded source-derived plan
permits exactly canonical header-incorporation encoding, certified selected
locator inventory repair, and certified omitted-history owner markers. The tool
projects that plan over the complete source record stream before migration and
compares `expected_content_digest` with the final candidate digest. Raw before
and after hashes remain in the report; `preservation_basis` identifies the
normalization. Unplanned changes to rows, history, pending work, or receipts
fail the comparison. Older transformations still require their own validator;
a false `semantic_preservation_verified` must block fleet completion.

`embedded_repository_id` records the portable identity inside the validated
repository. A hosted URL/catalog identity can intentionally differ after cloning
or restoration. Preserve that original catalog mapping through cutover; an
embedded identity or physical prefix alone does not authorize adopting an orphan.
Candidate engine admission alone is not a preservation proof.

Build browser migration separately with `npm run build:migration:wasm` in the
SDK. It emits `dist/migration-wasm`; normal WASM is emitted to `dist/wasm`
without historical full-format decoders. The public `@lix-js/sdk/migration`
entry owns migration APIs; they are absent from the ordinary package entry.
Native migration similarly builds `lix_js_sdk_migration.node` with
`npm run build:migration:native`.

Dedicated browser migration workers may call `initializeMigration`,
`inspectStorageProvider`, `migrateStorageProvider`, and
`convertStorageProviderToPartial`. These direct APIs neither close a supplied
provider nor publish a browser active-store pointer. The host owns existing
physical-lock exclusion, source quarantine, isolated destination, crash-safe
publication, and detection of late edits from old offline tabs. A new epoch flag
cannot fence an old bundle that does not read it.

After new-format writes begin, recover forward. Restoring an older backup would
lose those writes; a rollback then requires a separate data-preserving migration.
