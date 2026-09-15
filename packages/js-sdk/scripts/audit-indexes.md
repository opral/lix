# Indexed query regression audits

Build the matching native and JavaScript SDK before running these scripts. Each
script uses fresh databases and exits unsuccessfully on an assertion failure.
They supplement the Rust regression tests with public SDK calls.

```sh
cd packages/js-sdk
node scripts/audit-index-publication.mjs
node scripts/audit-indexed-queries.mjs
node scripts/audit-nullable-projections.mjs
node scripts/audit-index-lifecycle.mjs
node scripts/audit-index-budgets.mjs
node scripts/audit-index-witnesses.mjs
```

The matrices compare indexed equality, IN, range, and joined queries against
scan-based equivalents. They cover execution thresholds, nullable primitive
values, tracked and untracked rows, updates, deletion, forks, undo/redo,
checkpoints, snapshots, and compatible schema amendments. Publication checks
also assert foreign-key and unique-constraint behavior.

## Existing affected databases

The legacy audit needs two independently built SDKs with the same repository
format. First create the fixture using pre-fix revision `8be660752`, then verify
it with this PR's SDK:

```sh
SDK_INDEX=/path/to/affected/packages/js-sdk/dist/index.js \
  node scripts/audit-legacy-index.mjs create /tmp/legacy-lix /tmp/legacy-lix.bin
node scripts/audit-legacy-index.mjs verify /tmp/legacy-lix /tmp/legacy-lix.bin
```

Use a fresh storage directory for each run. The fixture seeds an index and
appends 512 rows in primary-key order, exercising packed publication. Creation
asserts that the rows exist while the indexed query incorrectly returns no rows.
Verification checks both disk reopen and snapshot import, subsequent writes,
schema amendment, and uniqueness enforcement. Older index-completeness markers
are ignored; uncertain indexes fall back to scanning, without requiring an
on-open rewrite of the database.

See [profile-indexed-import.md](profile-indexed-import.md) for the import and
query performance workload.
