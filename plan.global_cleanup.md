# `sql2` Hidden Global Version Cleanup

## Summary

Make `global` a normal internal version again, with one rule across the `sql2` pipeline:

- internal storage/commit/materialization/admin/checkpoint logic uses `version_id = "global"` as an ordinary version lane
- public surfaces still use `lixcol_global = true|false`
- for reads scoped to version `V`, global-overlay rows project as `version_id = V, global = true`
- for explicit `lix_*_by_version` queries scoped to `version_id = 'global'`, rows may expose `version_id = 'global'`

This is a clean cut in `sql2`. No backward-compat migration layer, no mixed sentinel-plus-boolean authority, no `lix_global_pointer`.

## Key Changes

### Canonical model

- Treat `global` as the reserved hidden version id.
- Seed it through normal `lix_version_descriptor` and `lix_version_pointer` rows only.
- Mark the global version descriptor `hidden = true`.
- Remove `lix_global_pointer` from the live path entirely.
- Collapse `GlobalAdmin` handling in `sql2`/append/preconditions to the normal version lane for `version_id = "global"`.

### Storage and internal contracts

- Remove `global` from live authoritative state storage:
  - `lix_internal_state_untracked`
  - `lix_internal_state_materialized_v1_*`
  - stored-schema bootstrap/materialized state tables
  - related materialization/runtime row structs
- Remove indexes/uniqueness patterns that include `global`; keys become version-based only.
- Keep `lix_internal_change` as version-based only.
- Keep `lix_internal_last_checkpoint(version_id, checkpoint_commit_id)` and store the global checkpoint as `version_id = "global"` like any other version.
- Replace all admin singleton “storage version id” helpers with normal hidden-global-version reads/writes or dedicated admin handling that still points at the hidden global version, not a second sentinel mechanism.

### `sql2` read/write semantics

- Effective-state resolution overlays two ordinary version lanes:
  - requested version `V`
  - hidden global version `global`
- Public projection rule:
  - if query scope is `V != global`, rows from the global lane project as `version_id = V, global = true`
  - if query scope is explicit `global`, rows expose `version_id = global, global = true`
- Writes with `lixcol_global = true` target internal version `global`.
- Writes with `lixcol_global = false` target the requested/local version.
- Remove all live-path logic that infers overlay from a stored `global` column instead of the version lane.
- Filesystem/state/entity/history/by-version paths all follow the same rule.

### Runtime and builtin cleanup

- Delete the `lix_global_pointer` builtin schema and all live callers.
- Remove `*_storage_version_id()` patterns that currently manufacture `"global"` as a special storage target.
- Update:
  - init/seed/bootstrap
  - commit tip lookup and append preconditions
  - working changes lowering
  - checkpoint creation/lookup
  - filesystem live projection
  - admin lowering and admin write resolution
  - materialization winner selection and visible-row projection
- Reserve `global` as a forbidden user-created version id.

## Test Plan

- Prefer sqlite suites during iteration:
  - `active_version`
  - `active_account`
  - `version_view`
  - `version_api`
  - `checkpoint`
  - `working_changes_view`
  - `state_view`
  - `state_by_version_view`
  - `state_history_view`
  - `entity_view`
  - `filesystem_view`
  - `file_history_view`
  - `init`
- Add or update coverage for:
  - hidden global version exists in `lix_version` with `hidden = true`
  - no live path depends on `lix_global_pointer`
  - `lix_state_by_version(... version_id = V ...)` shows global rows as `version_id = V, global = true`
  - `lix_state_by_version(... version_id = 'global' ...)` shows rows as `version_id = 'global', global = true`
  - same projection rules for entity/filesystem/history surfaces
  - checkpoints and working changes resolve global through the normal version DAG
  - creating a user version with id `global` is rejected
- Before merge, run `cargo test -p lix_engine`.

## Assumptions

- No backward-compat path is needed; this is a direct `sql2` cutover.
- `hidden = true` only applies to the version descriptor in `lix_version`, not to public row projection.
- Public `lixcol_global` remains the only overlay API; public inheritance/version-chain semantics do not return.

## Notes

- Test with sqlite simulation only during development for faster iteration speed. Before commit run the entire lix_engine suite

## Progress log

- 2026-03-09 18:34 PDT: Drafted initial plan.
- 2026-03-09 18:55 PDT: Moved the live init/checkpoint/admin path off `lix_global_pointer` onto the hidden global `lix_version_pointer`, seeded the hidden global version descriptor with `hidden = true`, removed the `lix_global_pointer` builtin from the live install set, and restored sqlite coverage for `active_version`, `checkpoint`, `working_changes_view`, `init`, `version_view`, and `state_by_version_view`.
- 2026-03-09 20:12 PDT: Removed the remaining live-path `lix_global_pointer` assumptions from commit tip lookup, bootstrap/init, state/filesystem effective-state lowering, and filesystem live projection so the hidden `global` version is treated as a normal version lane, while public `sql2` reads still reproject global-overlay rows as `lixcol_global = true` for the requested version. Restored sqlite coverage for `init`, `active_account`, `version_api`, `working_changes_view`, `state_by_version_view`, `filesystem_view`, and `file_history_view`.
- 2026-03-09 21:41 PDT: Finished the clean cut in the live `sql2` path by removing the `lix_global_pointer` builtin and other live-path sentinel assumptions, deriving global overlay projection from the hidden `global` version lane, updating same-request sql2 schema registration for tracked writes, and validating the result with the full `cargo test -p lix_engine` suite.
