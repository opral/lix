# Frozen v3 review report

Status: `TEST/REPORT-ONLY RED CONTROL / CORRECTION REQUIRED`

Package successor:

- parent/v2 `1b8134f7bc02802c203853a3f71dbbee639b6932`
- anchor production commit `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`
- anchor tree `4477c83b246bddac09cd972564bd4ccd67f90f7b`
- production paths under review: `file_history.rs`, `filesystem_working_diff.rs`

This report binds the required independent findings:

- H3 terminal v2 blocker report SHA-256:
  `fabefc265ec09b225eaac2ff4daeefd6baef35971497b077547c6d60dccaf39c`
- R1 whole-closure report SHA-256:
  `83871d2d7c1e8faa0231f77aae75a3f2811debfaeaebd5fb6c18aa83d74d5e96`
- prior R1 terminal report SHA-256 retained from v2:
  `f3091f560d468cb83119b56071db74f8860ad14cd8995e7fc59f2607ea46b268`

The exact b484 source RED set has nine findings:

1. `historical_file_descriptor_file_id_binding`
2. `historical_directory_file_id_null_binding`
3. `observed_file_descriptor_tombstone_payload`
4. `observed_directory_descriptor_tombstone_payload`
5. `observed_plugin_owner_tombstone_payload`
6. `selected_missing_or_tombstoned_blob_ref_fails_closed`
7. `composite_entity_pk_first_component_selection`
8. `conflicting_source_change_ids_fail_closed`
9. `zero_blob_ref_projection_state_not_distinguished`

The first six are the exact v2 RED findings; v3 adds the three R1/H3
discriminators. On b484, `oracle.py --mode all` must print all 28 model cases
(all 25 v2 cases plus the composite-PK, conflicting-source-change, and
zero-BlobRef projection cases) and exactly this nine-element source set.
The immutable replay stdout is 2,284 bytes, SHA-256
`4b933f0da4a1fd0c62e70d811976bd5144e4772a34e2445b22743b7f0ece5f98`; the
source-only RED stdout is 850 bytes, SHA-256
`767e567390fe54f43fb503393ef412989ec40254062ab88b3c04f63775989ec2`.

The independent additive nine-seam oracle is losslessly bound in
`ADDITIVE_CLOSURE_BINDING.md`: exact ref/head/tree/parent/full-index/patch,
artifact sums, and the supplied calibration log SHA are recorded. Its exact
runner replayed against detached b484 with exit 0 and combined stdout SHA
`6f74dbaa54574e6e94dec6f758c1a6d2047225d7f7bfe31e1f50582f2426e832`.

The executable model requires historical file `file_id == entity_pk ==
snapshot.id`, historical directory `file_id == NULL`, complete composite
EntityPk binding, exact one live BlobRef with canonical hash/size/payload, and
strict distinction between explicit authenticated empty, missing/NULL, and
tombstoned references. It exercises missing references through both
metadata-only and data projections; a payload-less tombstone is absence, while
a deleted row carrying payload fails before projection. Conflicting
authenticated source-change IDs fail before any silent deduplication.

The source oracle remains function-scoped and test/report-only. It checks the
two historical key contracts, all three observed tombstone contracts, the
selected BlobRef failure, first-component composite selection, source-change
deduplication, and zero-BlobRef projection fallback. It also preserves the
working-diff identity/tombstone checks and raw-reader/legacy-fallback negative
checks. No production source is modified.

The v3 `verify.sh` requires the exact v2 parent, exact b484 ancestry,
package-only successor paths, all SHA256SUMS entries, model/source RED
calibration, diff-check, and b484's expected RED `cargo fmt --all -- --check`
result. The current format stderr/stdout is 8,334 bytes, SHA-256
`a07bae0c5595f3c6c82c08c3f78107b3d94c84618c4128e5f4e43ef99d0ce303`.
No production build, adapter runtime, Cargo manifest, PR, or main branch was
changed.
