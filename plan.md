# Remove Inheritance in `packages/engine` with an Internal Global Lane

## Summary

- Verified against `packages/engine` on `next`: the engine is pointer-and-checkpoint based, so the design should stay aligned with `lix_version_pointer`, `lix_working_changes`, materialization, and vtable rewrites.
- Remove version inheritance entirely. Replace it with explicit scope: rows are either local or global via `lixcol_global`.
- Keep a single internal global lane for storage and commit tracking:
  - local rows: `global = false`, `version_id = <real version>`
  - global rows: `global = true`, `version_id = 'global'`
- Keep `version_id` non-null everywhere. Do not use `NULL` version ids.
- Hide the internal global lane from `lix_version`; public version APIs only expose real branch versions.
- Rename the singleton global head schema to `lix_global_pointer`.

## Key Changes

### Public model

- Remove `inherits_from_version_id` from:
  - `CreateVersionOptions`
  - `CreateVersionResult`
  - `LixVersionDescriptor`
  - `lix_version_descriptor`
  - `lix_version` read/write rewrites
  - public row shapes and tests
- Add `lixcol_global` anywhere row scope is surfaced today, alongside `lixcol_untracked`.
- Remove `lixcol_inherited_from_version_id` from all public views and entity/file projections.
- `lix_version` shows only real versions. The internal `global` lane is not returned and cannot become the active version.
- Reserve the version id `"global"` so user-created versions cannot collide with the internal lane.

### Storage and tracked heads

- Keep `version_id TEXT NOT NULL`.
- Add `global BOOLEAN NOT NULL DEFAULT false` to raw scoped state storage and tracked materialized state.
- Global rows are stored once in the internal lane with `version_id = 'global'`.
- Keep local version heads in `lix_version_pointer`.
- Add singleton `lix_global_pointer { commit_id }` for the tracked head of global scope.
- Keep the checkpoint store keyed by version id and add a global baseline row at `version_id = 'global'`.

### Read and write resolution

- Delete all inheritance and ancestry logic from:
  - vtable read rewrites
  - followup execution
  - filesystem mutation rewrite/version-chain cache
  - materialization ancestry stages
- Replace it with explicit overlay resolution:
  - local transaction
  - global transaction
  - local untracked
  - global untracked
  - local tracked
  - global tracked
- For the same `(entity_id, schema_key, file_id)`, local wins over global.
- A local delete of a visible global row creates a local tombstone; it hides the global row only in that version.
- Global writes only happen when `lixcol_global = true` is explicit.
- Public resolved `_by_version` views must project global rows with the requested real `version_id`, plus `lixcol_global = true`.
  - This preserves `WHERE version_id = 'v1'` behavior on public SQL.
  - Internally, source reads must still include both the target local lane and the internal global lane.

### Working changes and checkpoint

- Keep the working-changes mental model symmetric across scopes.
- `lix_working_changes` becomes the union of:
  - local pending changes against the active version checkpoint
  - global pending changes against the global checkpoint baseline
- Global working changes are based on:
  - global untracked rows
  - `lix_global_pointer`
  - the `version_id = 'global'` checkpoint row
- `create_checkpoint()` updates both baselines:
  - active local version checkpoint
  - global checkpoint row
- Keep the current local-only return shape of `create_checkpoint()`, even though it updates both scopes.

## Test Plan

- Bootstrapping seeds `main`, `lix_version_pointer`, and `lix_global_pointer`, but no public `global` version row.
- Creating a version no longer stores or returns inheritance metadata.
- A committed global row is visible from every version with `lixcol_global = true`.
- A local row shadows a global row only in that version.
- A local tombstone hides a global row only in that version.
- `lix_state_by_version`, entity `_by_version`, and filesystem `_by_version` views return real target `version_id` values for global rows, never the internal `'global'` id.
- Rewrite tests confirm no recursive `version_chain` or inheritance CTEs remain.
- Version-filter pushdown tests confirm source reads include the internal global lane when resolving a real target version.
- `lix_working_changes` shows both local and global pending changes.
- `create_checkpoint()` clears both local and global working changes.
- Mixed local/global tracked writes advance the correct heads only:
  - local writes move the owning `lix_version_pointer`
  - global writes move `lix_global_pointer`

## Assumptions

- No backward compatibility is required.
- The internal global lane is a storage/runtime detail only.
- `lixcol_global = false` is the default unless a write or schema override sets it.
- The earlier `version_id = NULL` design is superseded; the engine-fit design is non-null `version_id` plus internal `version_id = 'global'`.

## Notes

- If you find a severe architectural simplification possibility while implementing this plan, prompt the user for guidance.

- Append to the progress log on significant milestones

## Progress logs

- 2026-03-05 11:34: Crafted plan
- 2026-03-05 13:22: Removed public inheritance fields from version descriptors/API, added builtin `lix_global_pointer`, and hid the internal `global` lane from `lix_version`
- 2026-03-05 14:07: Wired global checkpoint/global-pointer plumbing through bootstrap, `create_checkpoint()`, `lix_working_changes`, materialization ancestry, and effective-state rewrites; `cargo check -p lix_engine` passes and `cargo test -p lix_engine --test checkpoint checkpoint_labels_current_commit_sqlite` passes
- 2026-03-05 16:02: Switched public state/entity/filesystem metadata surfaces from inherited markers to `global`/`lixcol_global`, stopped filesystem projections from deriving commit ids through `lix_version`, and kept the package green with `cargo check -p lix_engine`, `cargo test -p lix_engine --test checkpoint`, `cargo test -p lix_engine --test entity_view --no-run`, `cargo test -p lix_engine --test filesystem_view --no-run`, and the `filesystem::select_rewrite::tests::rewrites_simple_file_path_data_query_to_projection` unit test
- 2026-03-05 12:30: Replaced the remaining internal `inherited_from_version_id` plumbing in `packages/engine/src` with explicit `global` booleans across raw/materialized state DDL, materialization writes/debug rows, vtable/followup effective-state readers, and vtable delete cleanup; verified with `cargo fmt --package lix_engine`, `cargo check -p lix_engine`, `cargo test -p lix_engine --test materialization --no-run`, `cargo test -p lix_engine --lib --no-run`, `cargo test -p lix_engine rewrite_delete_effective_scope_preserves_global_predicate_for_untracked_cleanup -- --nocapture`, and `cargo test -p lix_engine --test materialization apply_materialization_plan_full_scope_clears_existing_rows_in_schema_tables -- --nocapture`
- 2026-03-05 12:38: Finished the scope-flag follow-through by stamping `global` in the immediate write/commit-runtime paths, updating the remaining inheritance-era tests and schema overrides to `global`/`lixcol_global`, and clearing the repo-wide `inherited_from_version_id` references from `packages/engine`; verified with `cargo fmt --package lix_engine`, `cargo test -p lix_engine --test state_by_version_view --test state_inheritance --test entity_view --test filesystem_view --no-run`, `cargo test -p lix_engine --test filesystem_view filesystem_views_expose_expected_lixcol_columns -- --nocapture`, `cargo test -p lix_engine --test entity_view lix_entity_view_select_pushes_down_inherited_from_version_override -- --nocapture`, `cargo test -p lix_engine --test state_by_version_view lix_state_by_version_select_inherits_from_parent_version -- --nocapture`, and `cargo test -p lix_engine --test state_inheritance lix_state_delete_with_inherited_null_filter_deletes_only_local_rows -- --nocapture`
- 2026-03-05 13:00: Moved schema-level global scope onto `lixcol_global = true` for the engine metadata schemas, split entity-view read overrides from write-lane routing so global-only schemas still read through `lix_state` but write through the internal `global` lane, and refreshed the dynamic entity-view tests to assert the new global projection behavior instead of inheritance-era `lixcol_version_id = 'global'`; verified with `cargo fmt --package lix_engine`, `cargo check -p lix_engine`, `cargo test -p lix_engine --lib --no-run`, `cargo test -p lix_engine --test init --no-run`, `cargo test -p lix_engine --test entity_view --no-run`, `cargo test -p lix_engine --test entity_view lix_entity_view_base_insert_read_honors_lixcol_global_override -- --nocapture`, and `cargo test -p lix_engine --test entity_view lix_entity_view_select_pushes_down_literal_lixcol_overrides -- --nocapture`
- 2026-03-05 13:03: Collapsed `filesystem/mutation_rewrite.rs` off the recursive `inherits_from_version_id` lookup to a fixed local-plus-global chain (`[version_id, global]`), which removes one of the remaining core inheritance hooks from engine logic; verified with the `version_chain_lookup_uses_session_cache_for_repeated_version` unit target and confirmed that the old “duplicate inherited path in child version” filesystem canaries now fail because their expectations still encode cross-version inheritance instead of explicit global shadowing.
- 2026-03-05 13:18: Rewrote the remaining inheritance-era filesystem canaries to use explicit global rows instead of parent-version visibility, removed the redundant nested duplicate-path case, and turned the old vacuous child-tombstone case into a real local-delete-over-global test. Verified with `cargo test -p lix_engine --test filesystem_view directory_duplicate_global_path_is_rejected_in_child_version -- --nocapture`, `cargo test -p lix_engine --test filesystem_view file_duplicate_global_path_is_rejected_in_child_version -- --nocapture`, `cargo test -p lix_engine --test filesystem_view file_path_update_to_global_path_is_rejected_in_child_version -- --nocapture`, `cargo test -p lix_engine --test filesystem_view file_reinsert_path_after_child_tombstone_of_global_file_succeeds -- --nocapture`, and `cargo test -p lix_engine --test filesystem_view -- --nocapture`.
- 2026-03-05 14:02: Finished the remaining inheritance-era test cleanup outside the filesystem suite. `state_view` and `on_conflict_views` now seed versions without the removed parent column, `version_view` now asserts the current public contract (no `inherits_from_version_id`, no public `global` row, less brittle internal row-count expectations), and `file_materialization` now inserts extra versions through the simplified `lix_version` shape. Verified with `cargo test -p lix_engine --test state_view -- --nocapture`, `cargo test -p lix_engine --test on_conflict_views -- --nocapture`, `cargo test -p lix_engine --test version_view -- --nocapture`, `cargo test -p lix_engine --test file_materialization -- --nocapture`, and a final `cargo test -p lix_engine --test state_view --test on_conflict_views --test version_view --test file_materialization --no-run`.
- 2026-03-05 14:11: Fixed the `--lib` regressions in `lix_state_by_version_view_read` by updating the version-id pushdown unit tests to the current target-version CTE shape: concrete `version_id` filters now apply on the local-plus-real-version union instead of a direct `version_descriptor` CTE filter. Verified with `cargo test -p lix_engine --lib -- --nocapture`.
- 2026-03-05 14:16: Fixed `active_version` by removing the last public-`global` version assumptions from the FK-switch tests. The suite now creates real versions before switching `lix_active_version.version_id`, which matches the new model where `global` is an internal lane, not a public version row. Verified with `cargo test -p lix_engine --test active_version -- --nocapture`.
- 2026-03-05 14:22: Fixed `entity_view` by updating the remaining by-version visibility canary to the new global-scope model and normalizing the actual `lixcol_global` column instead of `lixcol_version_id`, which removed the cross-backend deterministic mismatch. Verified with `cargo test -p lix_engine --test entity_view -- --nocapture`.
- 2026-03-05 14:31: Fixed `init` by replacing the remaining public-`global` version assertions with the real engine contract: bootstrap-global checks now read `lix_global_pointer`, per-version checkpoint tests combine the public main version with the internal global checkpoint row, and system-directory visibility is asserted through an active-version by-version read filtered by `lixcol_global = true`. Verified with `cargo test -p lix_engine --test init -- --nocapture`.
- 2026-03-05 14:36: Fixed `observe` by rewriting the last active-version-switch scenarios away from `switch_version(\"global\")`. The tests now start from the default real version, where global rows are already visible, and assert that switching into a branch-local shadow emits the expected observe delta. Verified with `cargo test -p lix_engine --test observe -- --nocapture`.
- 2026-03-05 14:40: Fixed `plugin_install` by moving the archive persistence assertions off `lix_file_by_version ... lixcol_version_id = 'global'` and onto the real public contract: installed plugin archives are visible in `lix_file` and flagged by `lixcol_global = true`. Verified with `cargo test -p lix_engine --test plugin_install -- --nocapture`.
- 2026-03-05 14:45: Fixed `state_by_version_view` by normalizing the remaining backend-dependent boolean column in the local-over-global overlay test and renaming the inheritance-era canaries to explicit global/local semantics (`reads_visible_global_row`, `prefers_local_row_over_global_row`, `local_tombstone_hides_global_row`). Verified with `cargo test -p lix_engine --test state_by_version_view -- --nocapture`.
- 2026-03-05 14:41: Applied the canonical active-version cache fix: `Engine.active_version_id` is now an unloaded-or-real-id cache (`Option<String>`) instead of a fake `"main"` placeholder, public execution/transaction/plugin-install/script paths now require a loaded active version, internal bootstrap execution keeps the internal `global` fallback only for seeding, and missing `lix_active_version` rows now fail as an invariant violation instead of silently substituting a version name. Updated the pre-init execution coverage to assert `LIX_ERROR_NOT_INITIALIZED`, switched the reopen-init sqlite test to call `open()` before querying, and seeded the few lib/observe unit tests that intentionally bypass init with a real synthetic version id. Verified with `cargo test -p lix_engine --test execute -- --nocapture`, `cargo test -p lix_engine --test active_version -- --nocapture`, `cargo test -p lix_engine --test init -- --nocapture`, `cargo test -p lix_engine --test plugin_install -- --nocapture`, `cargo test -p lix_engine --test observe -- --nocapture`, and `cargo test -p lix_engine --lib -- --nocapture`.
- 2026-03-05 14:48: Hardened every direct `lix_global_pointer` lookup to assert `global = true` in addition to the internal `version_id = 'global'` lane. This covers the reviewed seed path plus bootstrap detection, checkpoint loading, active-version bootstrap, and `lix_working_changes` tip resolution. Verified with `cargo test -p lix_engine --test init init_seeds_main_version_and_global_checkpoint_pointers_sqlite -- --nocapture`, `cargo test -p lix_engine --test checkpoint -- --nocapture`, and `cargo test -p lix_engine --lib lix_working_changes_view_read -- --nocapture`.
- 2026-03-05 14:56: Cleaned the dead `main_version_id` bindings in `packages/engine/tests/file_materialization.rs` by renaming only the genuinely unused destructured values to `_main_version_id`, keeping the helper return shape stable while removing the noisy test-target warnings. Verified with `cargo test -p lix_engine --test file_materialization --no-run`; the target still emits unrelated unused-import/dead-helper warnings, but the `main_version_id` warnings are gone.
- 2026-03-05 14:58: Simplified the `file_materialization` test helpers further by removing the `main_version_id` return value entirely from the boot helpers. The helpers now return only the engine, and each test queries `main_version_id(&engine).await` only when it actually needs that value. Verified with `cargo test -p lix_engine --test file_materialization --no-run`.
