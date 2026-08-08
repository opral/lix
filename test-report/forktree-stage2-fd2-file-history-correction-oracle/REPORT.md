# Frozen review report

Status: `RED CONTROL / CORRECTION REQUIRED`

Anchor:

- head `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`
- tree `4477c83b246bddac09cd972564bd4ccd67f90f7b`
- parent `fd2be256d763f17e9f127d4c984e36fba191cb82`
- production paths under review: `file_history.rs`, `filesystem_working_diff.rs`

The source oracle is expected to report exactly these b484 findings:

1. `historical_file_descriptor_file_id_binding`
2. `historical_directory_file_id_null_binding`
3. `observed_file_descriptor_tombstone_payload`
4. `observed_directory_descriptor_tombstone_payload`
5. `observed_plugin_owner_tombstone_payload`

The smallest production correction is function-scoped. Historical file and
directory parsers must validate the durable row key's `file_id` before decoding
or projecting. Observed file and directory parsers must reject a deleted row
that carries any snapshot before decoding; the plugin-owner parser must apply
the same rule. A deleted row with no snapshot remains an authenticated logical
absence. A live explicit empty BlobRef (size zero, hash of empty bytes) and an
explicit empty registry Value remain live values, not tombstones.

The accepted BlobRef contract is exactly one live reference for non-empty file
content, with matching file identity, canonical BlobId, declared size, and
payload bytes. Duplicate, missing, substituted, wrong-size, or wrong-hash
references fail closed before SQL projection. Explicit empty file state is
tested separately from deletion.

The b484 compiler frontier is intentionally outside this package. No production
runtime, adapter test, Cargo manifest, PR, or main branch was changed.

The frozen package gate is `verify.sh`: it requires the package commit's parent
to be exact b484, runs the 21 model cases and exact five-finding source RED,
checks the package diff, and runs `cargo fmt --all -- --check` as an expected
b484 RED gate. No production runtime is invoked.
