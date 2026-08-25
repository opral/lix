#![allow(clippy::needless_raw_string_hashes, clippy::redundant_clone)]

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::LixError;
use crate::catalog::{SchemaPlan, SchemaPlanId, TransactionCatalog};
use crate::common::{format_json_pointer, validate_row_metadata};
use crate::domain::Domain;
use crate::functions::FunctionProviderHandle;
use crate::row_pk::{RowPk, RowPkError};
use crate::schema::{
    SchemaKey, schema_from_registered_snapshot, validate_lix_schema, validate_lix_schema_definition,
};
use crate::sql2::PublicCatalog;
#[cfg(test)]
use crate::transaction_types::TransactionWriteRow;
use crate::transaction_types::{PreparedRowFacts, RawWriteBatch, RawWriteRowRef, TransactionJson};

pub(crate) const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";

/// Compact side columns produced while normalizing a row in place.
///
/// The row payload remains in the incoming batch allocation; only these
/// fixed-size facts need a new batch-wide column before preparation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalizedRowFacts {
    pub(crate) schema_plan_id: SchemaPlanId,
    pub(crate) facts: PreparedRowFacts,
}

/// Normalizes one incoming row into a row with final snapshot/row primary key.
///
/// This is the canonical schema-semantics boundary for transaction writes. It owns
/// schema default application, primary-key identity derivation, and explicit
/// identity mismatch validation. SQL providers should not pre-derive primary
/// keys for schemas that can be normalized here; they should pass decoded
/// snapshots and let this layer complete them.
///
/// This function intentionally does not assign timestamps, change ids, or
/// commit ids; those are prepared-row fields assigned after semantic
/// normalization has produced the final identity.
/// Normalizes one row without replacing the batch's row buffer.
///
/// This is the production bulk path. Schema defaults and identity derivation
/// update the row in place, while the returned fixed-width facts form a small
/// side column consumed by preparation.
pub(crate) fn normalize_raw_write_row_in_place(
    rows: &mut RawWriteBatch,
    row_index: usize,
    schema_catalog: &mut TransactionCatalog,
    functions: FunctionProviderHandle,
    default_timestamp: &mut Option<crate::common::LixTimestamp>,
) -> Result<NormalizedRowFacts, LixError> {
    populate_registered_schema_snapshot(rows, row_index)?;
    let row = rows.row(row_index);
    validate_transaction_write_row_schema_identity(row)?;
    ensure_internal_control_schema(row, schema_catalog)?;

    let Some((schema_plan_id, schema_plan)) =
        schema_catalog.snapshot().plan_for_key(&row.schema_key)
    else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "schema '{}' is not visible to this transaction",
                row.schema_key
            ),
        ));
    };

    // Typed plugin rows are authoritative native Schema v1 values. They must
    // be normalized and carried forward without manufacturing an outer JSON
    // snapshot; generic SQL/filesystem rows continue through the JSON branch
    // below. The component boundary has already applied plugin semantics, so
    // this host-side check is limited to the pinned schema fingerprint, row
    // shape, complete durable encoding, and identity agreement.
    if rows.row(row_index).decoded_snapshot().is_some() {
        let schema_key = rows.row(row_index).schema_key.clone();
        let mut typed = rows
            .take_decoded_snapshot(row_index)
            .expect("typed snapshot presence was just checked");
        if !schema_plan
            .fingerprint()
            .matches_bytes(&typed.schema_fingerprint)
        {
            if typed.boundary_validation_certified() {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "typed plugin row '{}' has a schema fingerprint that does not match the transaction catalog",
                        schema_key
                    ),
                ));
            }
            // Engine SQL batches may be compiled before a compatible schema
            // amendment is staged in the same transaction. Their revoked
            // preparation certificate deliberately routes them back here;
            // rebind the owned typed values to the authoritative transaction
            // plan, then apply its defaults and complete-row validation below.
            let typed = std::sync::Arc::make_mut(&mut typed);
            typed.invalidate_durable_payload();
            typed.schema_fingerprint = schema_plan.fingerprint().bytes();
        }
        if typed.boundary_validation_certified() {
            let Some(staged_row_pk) = rows.row(row_index).row_pk else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "boundary-certified typed row lost its durable primary key before transaction normalization",
                ));
            };
            if !staged_row_pk.matches_schema_values(&typed.row_pk) {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "typed plugin row '{}' durable primary key does not match its typed primary key",
                        schema_key
                    ),
                ));
            }
            rows.set_decoded_snapshot(row_index, Some(typed));
            let row = rows.row(row_index);
            validate_normalized_row_content(row, None, schema_plan)?;
            let requires_transaction_validation =
                !schema_plan.uniques.is_empty() || !schema_plan.foreign_keys.is_empty();
            canonicalize_descriptor_file_id(rows, row_index)?;
            return Ok(NormalizedRowFacts {
                schema_plan_id,
                facts: PreparedRowFacts {
                    row_content_validated: true,
                    requires_transaction_validation,
                },
            });
        }
        if schema_plan.compiled_schema.defaults_would_apply(&typed.row) {
            let timestamp_functions = functions.clone();
            let typed = std::sync::Arc::make_mut(&mut typed);
            typed.invalidate_durable_payload();
            schema_plan
                .compiled_schema
                .apply_defaults(
                    &mut typed.row,
                    || functions.call_uuid_v7(),
                    || {
                        let timestamp = *default_timestamp
                            .get_or_insert_with(|| timestamp_functions.call_timestamp());
                        i64::try_from(timestamp.milliseconds_since_unix_epoch())
                            .expect("Lix timestamps fit signed milliseconds")
                            * 1_000
                    },
                )
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!("typed plugin row '{schema_key}' failed to apply Schema v1 defaults: {error}"),
                    )
                })?;
        }
        schema_plan
            .compiled_schema
            .validate_complete_row(&typed.row)
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!("typed plugin row '{schema_key}' failed Schema v1 validation: {error}"),
                )
            })?;
        let primary_key = schema_plan.compiled_schema.primary_key();
        if typed.row_pk.is_empty() {
            let row_pk = primary_key
                .iter()
                .map(|column| {
                    typed.row.get(column).cloned().ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_SCHEMA_VALIDATION,
                            format!("typed plugin row '{schema_key}' is missing primary-key column '{column}'"),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let typed = std::sync::Arc::make_mut(&mut typed);
            typed.invalidate_durable_payload();
            typed.row_pk = row_pk.into();
        }
        if typed.row_pk.len() != primary_key.len()
            || primary_key
                .iter()
                .zip(typed.row_pk.iter())
                .any(|(column, value)| typed.row.get(column) != Some(value))
        {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "typed plugin row '{}' primary key does not match its typed row",
                    schema_key
                ),
            ));
        }
        let typed_row_pk = RowPk::from_schema_values(&typed.row_pk).map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!("typed plugin row '{schema_key}' has an invalid primary key: {error}"),
            )
        })?;
        if rows.row(row_index).row_pk.is_none() {
            *rows.row_pk_mut(row_index) = Some(typed_row_pk.clone());
        }
        let staged_row_pk = rows
            .row(row_index)
            .row_pk
            .expect("typed row primary key was just materialized");
        if staged_row_pk != &typed_row_pk {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "typed plugin row '{}' durable primary key does not match its typed primary key",
                    schema_key
                ),
            ));
        }
        typed.durable_payload().map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!("typed plugin row '{schema_key}' is not durably encodable: {error:?}"),
            )
        })?;
        rows.set_decoded_snapshot(row_index, Some(typed));
        let row = rows.row(row_index);
        validate_normalized_row_content(row, None, schema_plan)?;
        let requires_transaction_validation =
            !schema_plan.uniques.is_empty() || !schema_plan.foreign_keys.is_empty();
        canonicalize_descriptor_file_id(rows, row_index)?;
        return Ok(NormalizedRowFacts {
            schema_plan_id,
            facts: PreparedRowFacts {
                row_content_validated: true,
                requires_transaction_validation,
            },
        });
    }

    let row = rows.row(row_index);
    if row
        .snapshot_json()
        .is_some_and(TransactionJson::row_content_certified)
        && row.schema_key != REGISTERED_SCHEMA_KEY
    {
        if row.row_pk.is_none() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified replacement row is missing its proven row identity",
            ));
        }
        let typed =
            std::sync::Arc::new(crate::plugin::runtime::WasmTypedRow::from_normalized_json(
                schema_plan,
                row.row_pk
                    .as_ref()
                    .expect("certified live row has a proven primary key"),
                row.snapshot_json()
                    .expect("certified live row retains row content")
                    .value(),
            )?);
        rows.set_decoded_snapshot(row_index, Some(typed));
        canonicalize_descriptor_file_id(rows, row_index)?;
        return Ok(NormalizedRowFacts {
            schema_plan_id,
            facts: PreparedRowFacts {
                row_content_validated: true,
                requires_transaction_validation: false,
            },
        });
    }

    let normalized_snapshot = if let Some(snapshot) = rows.take_snapshot(row_index) {
        let row = rows.row(row_index);
        let snapshot_object = snapshot.value().as_object().ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "snapshot_content for schema '{}' must be a JSON object",
                    row.schema_key
                ),
            )
        })?;
        if schema_plan.defaults.would_apply(snapshot_object) {
            // Missing defaults are the uncommon rewrite path. Materialize
            // only this row, apply the semantic change, and canonicalize the
            // result once. Complete engine JSON rows retain their batch
            // handle through the branch below; plugin-owned rows use typed
            // payloads and do not enter this branch.
            let mut snapshot = snapshot_object_for_mutation(snapshot, row)?;
            apply_defaults(
                &mut snapshot,
                schema_plan,
                row,
                functions,
                default_timestamp,
            )?;
            let snapshot = JsonValue::Object(snapshot);
            let row_pk = resolve_row_pk(row, schema_plan, &snapshot)?;
            *rows.row_pk_mut(row_index) = Some(row_pk);
            Some(TransactionJson::from_value(
                snapshot,
                "normalized transaction snapshot_content",
            )?)
        } else {
            let row_pk = resolve_row_pk(row, schema_plan, snapshot.value())?;
            *rows.row_pk_mut(row_index) = Some(row_pk);
            Some(snapshot)
        }
    } else if rows.row(row_index).row_pk.is_none() {
        let schema_key = rows.row(row_index).schema_key.clone();
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!("tombstone for schema '{}' requires row_pk", schema_key),
        ));
    } else {
        None
    };

    let row = rows.row(row_index);
    validate_normalized_row_content(row, normalized_snapshot.as_ref(), schema_plan)?;
    let requires_transaction_validation = if normalized_snapshot.is_some() {
        !schema_plan.uniques.is_empty() || !schema_plan.foreign_keys.is_empty()
    } else {
        schema_catalog
            .snapshot()
            .delete_plan_for_key(&row.schema_key)
            .has_committed_checks()
    } && !row.constraints_unchanged;

    let converted_typed = if let Some(snapshot) = normalized_snapshot.as_ref() {
        Some(std::sync::Arc::new(
            crate::plugin::runtime::WasmTypedRow::from_normalized_json(
                schema_plan,
                row.row_pk.expect("normalized live row has a primary key"),
                snapshot.value(),
            )?,
        ))
    } else {
        None
    };

    if row.schema_key == REGISTERED_SCHEMA_KEY {
        if row.file_id.is_some() {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                "lix_registered_schema rows must not be scoped to a file",
            )
            .with_hint("Schema definitions are scoped by branch and durability only; write them with null file_id."));
        }
        let schema_domain =
            Domain::schema_catalog(row.schema_scope_branch_id().to_string(), row.untracked);
        remember_pending_registered_schema(
            normalized_snapshot.as_ref().map(TransactionJson::value),
            schema_domain,
            schema_catalog,
        )?;
    }

    if let Some(typed) = converted_typed {
        rows.set_decoded_snapshot(row_index, Some(typed));
    }
    canonicalize_descriptor_file_id(rows, row_index)?;
    Ok(NormalizedRowFacts {
        schema_plan_id,
        facts: PreparedRowFacts {
            row_content_validated: true,
            requires_transaction_validation,
        },
    })
}

fn populate_registered_schema_snapshot(
    rows: &mut RawWriteBatch,
    row_index: usize,
) -> Result<(), LixError> {
    let row = rows.row(row_index);
    if row.schema_key != REGISTERED_SCHEMA_KEY {
        return Ok(());
    }
    let Some(snapshot) = row.snapshot_json() else {
        return Ok(());
    };
    let Some(value) = snapshot.value().get("value") else {
        return Ok(());
    };
    let schema = value.clone();
    let schema_key = schema
        .get("key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                "lix_registered_schema value is missing string key",
            )
        })?;
    let mut converted = snapshot.value().clone();
    let object = converted.as_object_mut().ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "lix_registered_schema snapshot must be an object",
        )
    })?;
    object
        .entry("schema_key")
        .or_insert_with(|| JsonValue::String(schema_key.into()));
    object.insert("value".into(), schema);
    rows.set_snapshot(
        row_index,
        Some(TransactionJson::from_value(
            converted,
            "normalized registered schema snapshot",
        )?),
    );
    Ok(())
}

fn canonicalize_descriptor_file_id(
    rows: &mut RawWriteBatch,
    row_index: usize,
) -> Result<(), LixError> {
    let row = rows.row(row_index);
    let file_id = match row.schema_key.as_str() {
        FILE_DESCRIPTOR_SCHEMA_KEY => {
            let row_pk = row.row_pk.expect("normalized row has a row identity");
            Some(
                row_pk
                    .as_single_string_owned()
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_SCHEMA_VALIDATION,
                            format!(
                                "lix_file_descriptor identity must contain its file id: {error}"
                            ),
                        )
                    })?
                    .into(),
            )
        }
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY => None,
        _ => return Ok(()),
    };
    rows.set_file_id(row_index, file_id);
    Ok(())
}

fn validate_normalized_row_content(
    row: RawWriteRowRef<'_>,
    snapshot: Option<&TransactionJson>,
    schema_plan: &SchemaPlan,
) -> Result<(), LixError> {
    if let Some(metadata) = row.metadata {
        if !metadata.metadata_content_certified() {
            validate_row_metadata(
                metadata.value(),
                format!("metadata for schema '{}'", row.schema_key),
            )?;
        }
    }
    let Some(snapshot) = snapshot else {
        return Ok(());
    };
    if schema_plan.accepts_row_content_fast(snapshot.value()) {
        return Ok(());
    }
    if let Err(errors) = schema_plan.compiled_schema.validate(snapshot.value()) {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot_content validation failed for schema '{}': {}",
                row.schema_key,
                crate::schema::format_lix_schema_validation_errors(errors)
            ),
        ));
    }
    Ok(())
}

/// Engine-owned control rows have fixed schemas and are intentionally hidden
/// from public schema surfaces. Internal producers may therefore validate
/// them from the compile-time definition without first materializing that
/// definition in the transaction's visible schema catalog.
fn ensure_internal_control_schema(
    row: RawWriteRowRef<'_>,
    schema_catalog: &mut TransactionCatalog,
) -> Result<(), LixError> {
    let internal = matches!(
        row.schema_key.as_str(),
        crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY
            | crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
    );
    if !internal || schema_catalog.snapshot().schema(&row.schema_key).is_some() {
        return Ok(());
    }
    let schema = crate::schema::seed_schema_definition(&row.schema_key)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "compile-time internal control schema is missing",
            )
        })?
        .clone();
    let key = crate::schema::schema_key_from_definition(&schema)?;
    schema_catalog.insert_schema_for_domain(
        Domain::schema_catalog(row.schema_scope_branch_id().to_string(), row.untracked),
        key,
        schema,
    )?;
    Ok(())
}

fn validate_transaction_write_row_schema_identity(row: RawWriteRowRef<'_>) -> Result<(), LixError> {
    if row.schema_key.is_empty() {
        return Err(LixError::new(
            LixError::CODE_UNKNOWN,
            "engine transaction staging requires non-empty schema_key",
        ));
    }
    Ok(())
}

fn snapshot_object_for_mutation(
    snapshot: TransactionJson,
    row: RawWriteRowRef<'_>,
) -> Result<JsonMap<String, JsonValue>, LixError> {
    match snapshot.into_value_for_mutation() {
        JsonValue::Object(snapshot) => Ok(snapshot),
        _ => Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot_content for schema '{}' must be a JSON object",
                row.schema_key
            ),
        )),
    }
}

fn apply_defaults(
    snapshot: &mut JsonMap<String, JsonValue>,
    schema_plan: &SchemaPlan,
    row: RawWriteRowRef<'_>,
    functions: FunctionProviderHandle,
    default_timestamp: &mut Option<crate::common::LixTimestamp>,
) -> Result<bool, LixError> {
    let timestamp_functions = functions.clone();
    schema_plan
        .defaults
        .apply(snapshot, functions, &row.schema_key, || {
            Ok(*default_timestamp.get_or_insert_with(|| timestamp_functions.call_timestamp()))
        })
}

fn resolve_row_pk(
    row: RawWriteRowRef<'_>,
    schema_plan: &SchemaPlan,
    snapshot: &JsonValue,
) -> Result<RowPk, LixError> {
    let Some(primary_key_paths) = schema_plan.primary_key.as_ref() else {
        return row.row_pk.cloned().ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "write for schema '{}' requires row_pk because the schema has no primary_key",
                    row.schema_key
                ),
            )
        });
    };
    let component_types = schema_plan
        .primary_key_component_types
        .as_deref()
        .expect("primary-key paths and component types are compiled together");
    let derived = RowPk::from_primary_key_plan(snapshot, primary_key_paths, component_types)
        .map_err(|error| row_pk_derivation_error(row, primary_key_paths, error))?;
    if let Some(row_pk) = row.row_pk {
        if row_pk.as_json_array_value()? != derived.as_json_array_value()? {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "row_pk '{}' does not match primary_key-derived row_pk '{}' for schema '{}'",
                    row_pk.as_json_array_text()?,
                    derived.as_json_array_text()?,
                    row.schema_key
                ),
            ));
        }
    }
    Ok(derived)
}

fn row_pk_derivation_error(
    row: RawWriteRowRef<'_>,
    primary_key_paths: &[Vec<String>],
    error: RowPkError,
) -> LixError {
    let detail = match error {
        RowPkError::EmptyPrimaryKey => "empty primary_key".to_string(),
        RowPkError::EmptyPrimaryKeyPath { index } => {
            format!("empty primary_key column at index {index}")
        }
        RowPkError::MissingPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("missing value at primary-key pointer '{pointer}'")
        }
        RowPkError::UnsupportedPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("unsupported value at primary-key pointer '{pointer}'")
        }
        RowPkError::InvalidPrimaryKeyValue { index, expected } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("value at primary-key pointer '{pointer}' must be a valid {expected}")
        }
        RowPkError::InvalidEncodedRowPk => "invalid encoded row primary key".to_string(),
    };
    LixError::new(
        LixError::CODE_SCHEMA_VALIDATION,
        format!(
            "failed to derive row_pk for schema '{}': {detail}",
            row.schema_key
        ),
    )
}

pub(crate) fn remember_pending_registered_schema(
    snapshot: Option<&JsonValue>,
    domain: Domain,
    schema_catalog: &mut TransactionCatalog,
) -> Result<(), LixError> {
    let Some(snapshot) = snapshot else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "lix_registered_schema rows cannot be deleted yet; schema deletion is not supported",
        ));
    };
    if let Some(schema) = snapshot.get("value") {
        validate_lix_schema_definition(schema)?;
    }
    {
        let registered_schema_definition = schema_catalog
            .snapshot()
            .schema(REGISTERED_SCHEMA_KEY)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    "lix_registered_schema schema is not visible to this transaction",
                )
            })?;
        validate_lix_schema(registered_schema_definition, snapshot)?;
    }
    let (key, schema) = schema_from_registered_snapshot(snapshot)?;
    reject_reserved_schema_namespace_unless_exact_builtin(&key, &schema)?;
    validate_lix_schema_definition(&schema)?;
    schema_catalog.insert_schema_for_domain(domain, key, schema)?;
    Ok(())
}

pub(crate) fn reject_reserved_schema_namespace(key: &SchemaKey) -> Result<(), LixError> {
    if !PublicCatalog::runtime_schema_key_uses_reserved_namespace(&key.schema_key) {
        return Ok(());
    }
    Err(LixError::new(
        LixError::CODE_RESERVED_SCHEMA_NAMESPACE,
        format!(
            "schema '{}' uses the reserved Lix schema namespace and cannot be registered at runtime",
            key.schema_key
        ),
    )
    .with_hint(
        "Choose an application-owned schema key outside the reserved `lix` and `lix_*` namespace, for example `acme_task`.",
    ))
}

/// The offline repository migration persists an engine-owned schema through
/// the ordinary schema-amendment transaction path. Permit only the exact
/// bundled definition; arbitrary runtime schemas remain excluded from the
/// reserved namespace.
pub(crate) fn reject_reserved_schema_namespace_unless_exact_builtin(
    key: &SchemaKey,
    schema: &JsonValue,
) -> Result<(), LixError> {
    if crate::schema::seed_schema_definition(&key.schema_key) == Some(schema) {
        return Ok(());
    }
    reject_reserved_schema_namespace(key)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::functions::FunctionProvider;
    use crate::schema::seed_schema_definition;

    #[test]
    fn normalization_derives_row_pk_from_primary_key() {
        let mut catalog = catalog_with(vec![schema_with_default_id()]);
        let row = TransactionWriteRow {
            row_pk: None,
            schema_key: "normalization_schema".into(),
            snapshot: Some(snapshot_json(
                r#"{"id":"row-from-snapshot","value":"hello"}"#,
            )),
            ..base_stage_row()
        };

        let row = normalize_test_row(row, &mut catalog, functions()).expect("normalize row");

        assert_eq!(
            row.row_pk.as_ref(),
            Some(&RowPk::single("row-from-snapshot"))
        );
    }

    #[test]
    fn historical_shared_snapshot_is_revalidated_after_schema_amendment() {
        let canonical = crate::common::SharedStr::from(r#"{"id":"row-1","value":"old-value"}"#);
        let row = || TransactionWriteRow {
            row_pk: Some(RowPk::single("row-1")),
            schema_key: "certificate_schema".into(),
            snapshot: Some(TransactionJson::from_unvalidated_shared_normalized_content(
                canonical.clone(),
            )),
            ..base_stage_row()
        };

        let mut old_catalog = catalog_with(vec![certificate_test_schema(None)]);
        let accepted =
            normalize_test_row(row(), &mut old_catalog, functions()).expect("old schema row");
        assert_eq!(
            accepted
                .snapshot
                .as_ref()
                .expect("accepted snapshot")
                .normalized(),
            canonical.as_str()
        );

        let mut amended_catalog = catalog_with(vec![certificate_test_schema(Some("new-value"))]);
        let error = normalize_test_row(row(), &mut amended_catalog, functions())
            .expect_err("historical row must satisfy the current amended schema");
        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
    }

    #[test]
    fn normalization_applies_schema_v1_literal_defaults() {
        let mut catalog = catalog_with(vec![schema_with_cel_field_default()]);
        let row = TransactionWriteRow {
            row_pk: None,
            schema_key: "cel_field_default_schema".into(),
            snapshot: Some(snapshot_json(r#"{"id":"row-1","name":"Sample"}"#)),
            ..base_stage_row()
        };

        let row = normalize_test_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(snapshot["slug"], "default-slug");
    }

    #[test]
    fn normalization_applies_schema_v1_default_value() {
        let mut catalog = catalog_with(vec![schema_with_overridden_default()]);
        let row = TransactionWriteRow {
            row_pk: None,
            schema_key: "overridden_default_schema".into(),
            snapshot: Some(snapshot_json(r#"{"id":"row-1"}"#)),
            ..base_stage_row()
        };

        let row = normalize_test_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(snapshot["status"], "literal");
    }

    #[test]
    fn normalization_does_not_overwrite_explicit_null_with_default() {
        let mut catalog = catalog_with(vec![schema_with_nullable_default()]);
        let row = TransactionWriteRow {
            row_pk: None,
            schema_key: "nullable_default_schema".into(),
            snapshot: Some(snapshot_json(r#"{"id":"row-1","status":null}"#)),
            ..base_stage_row()
        };

        let row = normalize_test_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(snapshot["status"], JsonValue::Null);
    }

    #[test]
    fn normalization_applies_timestamp_function_default() {
        let mut catalog = catalog_with(vec![schema_with_timestamp_default()]);
        let row = TransactionWriteRow {
            row_pk: None,
            schema_key: "timestamp_default_schema".into(),
            snapshot: Some(snapshot_json(r#"{"id":"row-1"}"#)),
            ..base_stage_row()
        };

        let row = normalize_test_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(snapshot["created_at"], "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn normalization_rejects_row_pk_that_disagrees_with_primary_key() {
        let mut catalog = catalog_with(vec![schema_with_default_id()]);
        let row = TransactionWriteRow {
            row_pk: Some(RowPk::single("wrong-id")),
            schema_key: "normalization_schema".into(),
            snapshot: Some(snapshot_json(r#"{"id":"right-id","value":"hello"}"#)),
            ..base_stage_row()
        };

        let error =
            normalize_test_row(row, &mut catalog, functions()).expect_err("id mismatch fails");

        assert!(
            error
                .message
                .contains("does not match primary_key-derived row_pk")
        );
    }

    #[test]
    fn normalization_rejects_typed_primary_key_that_disagrees_with_durable_identity() {
        let mut catalog = catalog_with(vec![schema_with_default_id()]);
        let fingerprint = catalog
            .snapshot()
            .plan_for_key("normalization_schema")
            .expect("schema plan")
            .1
            .fingerprint()
            .bytes();
        let typed = std::sync::Arc::new(crate::plugin::runtime::WasmTypedRow {
            schema_fingerprint: fingerprint,
            row_pk: vec![lix_schema::Value::Text("right-id".to_owned())].into(),
            row: lix_schema::Row::from([
                (
                    "id".to_owned(),
                    lix_schema::Value::Text("right-id".to_owned()),
                ),
                (
                    "value".to_owned(),
                    lix_schema::Value::Text("hello".to_owned()),
                ),
            ]),
            native_payload: std::sync::OnceLock::new(),
            boundary_create_validation: std::sync::OnceLock::new(),
        });
        let mut rows = RawWriteBatch::with_capacity(1);
        rows.push_typed_parts(
            Some(RowPk::single("wrong-id")),
            "normalization_schema".into(),
            None,
            Some(typed),
            None,
            None,
            None,
            None,
            true,
            None,
            None,
            false,
            crate::GLOBAL_BRANCH_ID.into(),
        );
        let mut default_timestamp = None;

        let error = normalize_raw_write_row_in_place(
            &mut rows,
            0,
            &mut catalog,
            functions(),
            &mut default_timestamp,
        )
        .expect_err("typed envelope identity mismatch must fail");

        assert!(error.message.contains("durable primary key does not match"));
    }

    #[test]
    fn normalization_materializes_defaults_in_typed_rows() {
        let mut catalog = catalog_with(vec![schema_with_overridden_default()]);
        let fingerprint = catalog
            .snapshot()
            .plan_for_key("overridden_default_schema")
            .expect("schema plan")
            .1
            .fingerprint()
            .bytes();
        let typed = std::sync::Arc::new(crate::plugin::runtime::WasmTypedRow {
            schema_fingerprint: fingerprint,
            row_pk: vec![lix_schema::Value::Text("row-1".to_owned())].into(),
            row: lix_schema::Row::from([(
                "id".to_owned(),
                lix_schema::Value::Text("row-1".to_owned()),
            )]),
            native_payload: std::sync::OnceLock::new(),
            boundary_create_validation: std::sync::OnceLock::new(),
        });
        let mut rows = RawWriteBatch::with_capacity(1);
        rows.push_typed_parts(
            Some(RowPk::single("row-1")),
            "overridden_default_schema".into(),
            None,
            Some(typed),
            None,
            None,
            None,
            None,
            true,
            None,
            None,
            false,
            crate::GLOBAL_BRANCH_ID.into(),
        );
        let mut default_timestamp = None;

        normalize_raw_write_row_in_place(
            &mut rows,
            0,
            &mut catalog,
            functions(),
            &mut default_timestamp,
        )
        .expect("typed defaults normalize");

        assert_eq!(
            rows.row(0)
                .decoded_snapshot()
                .expect("typed row remains native")
                .row
                .get("status"),
            Some(&lix_schema::Value::Text("literal".to_owned()))
        );
    }

    #[test]
    fn normalization_derives_json_array_row_pk_for_composite_primary_key() {
        let mut catalog = catalog_with(vec![composite_key_schema()]);
        let row = TransactionWriteRow {
            row_pk: None,
            schema_key: "composite_key_schema".into(),
            snapshot: Some(snapshot_json(r#"{"namespace":"a~b","key":"1"}"#)),
            ..base_stage_row()
        };

        let row = normalize_test_row(row, &mut catalog, functions()).expect("normalize row");
        let row_pk = row.row_pk.expect("composite row pk");
        let projected_row_pk = row_pk.as_json_array_text().expect("row pk should project");

        assert_eq!(projected_row_pk, "[\"a~b\",\"1\"]");
    }

    #[test]
    fn normalization_rejects_non_string_primary_key_values() {
        let mut catalog = catalog_with(vec![composite_key_schema()]);
        let row = TransactionWriteRow {
            row_pk: None,
            schema_key: "composite_key_schema".into(),
            snapshot: Some(snapshot_json(r#"{"namespace":"a~b","key":1}"#)),
            ..base_stage_row()
        };

        let error = normalize_test_row(row, &mut catalog, functions())
            .expect_err("non-string primary key values should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error
                .message
                .contains("value at primary-key pointer '/key' must be a valid string")
        );
    }

    #[test]
    fn normalization_validates_explicit_composite_row_pk_against_projection() {
        let mut catalog = catalog_with(vec![composite_key_schema()]);
        let snapshot = json!({
            "namespace": "a~b",
            "key": "1",
        });
        let derived = RowPk::from_primary_key_paths(
            &snapshot,
            &[vec!["namespace".to_string()], vec!["key".to_string()]],
        )
        .expect("identity should derive");
        let row = TransactionWriteRow {
            row_pk: Some(derived.clone()),
            schema_key: "composite_key_schema".into(),
            snapshot: Some(transaction_json(snapshot.clone())),
            ..base_stage_row()
        };

        let row = normalize_test_row(row, &mut catalog, functions()).expect("normalize row");

        assert_eq!(row.row_pk.as_ref(), Some(&derived));
    }

    #[test]
    fn normalization_makes_pending_registered_schema_visible_to_later_rows() {
        let mut catalog = catalog_with(vec![
            seed_schema_definition(REGISTERED_SCHEMA_KEY)
                .expect("registered schema builtin")
                .clone(),
        ]);
        let registered = TransactionWriteRow {
            row_pk: None,
            schema_key: REGISTERED_SCHEMA_KEY.into(),
            snapshot: Some(transaction_json(json!({
                "value": dynamic_schema_definition(),
            }))),
            ..base_stage_row()
        };

        normalize_test_row(registered, &mut catalog, functions()).expect("register schema");

        let dynamic = TransactionWriteRow {
            row_pk: None,
            schema_key: "dynamic_schema".into(),
            snapshot: Some(snapshot_json(r#"{"id":"dynamic-1"}"#)),
            ..base_stage_row()
        };
        let dynamic = normalize_test_row(dynamic, &mut catalog, functions()).expect("dynamic row");

        assert_eq!(dynamic.row_pk.as_ref(), Some(&RowPk::single("dynamic-1")));
    }

    #[test]
    fn normalization_rejects_the_complete_reserved_lix_schema_namespace() {
        let mut catalog = catalog_with(vec![
            seed_schema_definition(REGISTERED_SCHEMA_KEY)
                .expect("registered schema builtin")
                .clone(),
        ]);

        for schema_key in [
            "lix",
            "lix_file",
            "lix_key_value_history",
            "lix_file_descriptor",
            "lix_plugin_note",
        ] {
            let mut schema = dynamic_schema_definition();
            schema["key"] = json!(schema_key);
            let registered = TransactionWriteRow {
                row_pk: None,
                schema_key: REGISTERED_SCHEMA_KEY.into(),
                snapshot: Some(transaction_json(json!({ "value": schema }))),
                ..base_stage_row()
            };

            let error = normalize_test_row(registered, &mut catalog, functions())
                .expect_err("lix_* should be reserved");

            assert_eq!(error.code, LixError::CODE_RESERVED_SCHEMA_NAMESPACE);
            assert!(
                error.message.contains("reserved Lix schema namespace"),
                "{error:?}"
            );
            assert!(error.message.contains(schema_key), "{error:?}");
            assert!(
                !catalog.snapshot().contains(schema_key),
                "rejected schema must not enter the transaction catalog"
            );
        }
    }

    #[test]
    fn normalization_allows_application_owned_schema_key() {
        let mut catalog = catalog_with(vec![
            seed_schema_definition(REGISTERED_SCHEMA_KEY)
                .expect("registered schema builtin")
                .clone(),
        ]);
        let mut schema = dynamic_schema_definition();
        schema["key"] = json!("acme_plugin_note");
        let registered = TransactionWriteRow {
            row_pk: None,
            schema_key: REGISTERED_SCHEMA_KEY.into(),
            snapshot: Some(transaction_json(json!({ "value": schema }))),
            ..base_stage_row()
        };

        normalize_test_row(registered, &mut catalog, functions())
            .expect("an application-owned key remains valid");
        assert!(catalog.snapshot().contains("acme_plugin_note"));
    }

    #[test]
    fn normalization_preserves_filesystem_descriptor_segments() {
        let mut catalog = catalog_with(vec![
            builtin_schema(FILE_DESCRIPTOR_SCHEMA_KEY),
            builtin_schema(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        ]);

        let file = TransactionWriteRow {
            row_pk: None,
            schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.into(),
            snapshot: Some(transaction_json(json!({
                "id": "01920000-0000-7000-8000-0000000000c1",
                "directory_id": null,
                "name": "Cafe\u{301}.txt",
            }))),
            global: false,
            ..base_stage_row()
        };
        let file = normalize_test_row(file, &mut catalog, functions()).expect("normalize file");
        let file_snapshot = normalized_snapshot(&file);
        assert_eq!(file_snapshot["name"], "Cafe\u{301}.txt");
        assert_eq!(
            file.file_id.as_deref(),
            Some("01920000-0000-7000-8000-0000000000c1")
        );

        let directory = TransactionWriteRow {
            row_pk: None,
            schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY.into(),
            snapshot: Some(transaction_json(json!({
                "id": "01920000-0000-7000-8000-0000000000c2",
                "parent_id": null,
                "name": "Cafe\u{301}",
            }))),
            file_id: Some("must-be-cleared".into()),
            global: false,
            ..base_stage_row()
        };
        let directory =
            normalize_test_row(directory, &mut catalog, functions()).expect("normalize directory");
        let directory_snapshot = normalized_snapshot(&directory);
        assert_eq!(directory_snapshot["name"], "Cafe\u{301}");
        assert_eq!(directory.file_id, None);

        let bidi = TransactionWriteRow {
            row_pk: None,
            schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.into(),
            snapshot: Some(transaction_json(json!({
                "id": "01920000-0000-7000-8000-0000000000c3",
                "directory_id": null,
                "name": "safe\u{202E}txt",
            }))),
            global: false,
            ..base_stage_row()
        };
        let bidi =
            normalize_test_row(bidi, &mut catalog, functions()).expect("normalize bidi file");
        let bidi_snapshot = normalized_snapshot(&bidi);
        assert_eq!(bidi_snapshot["name"], "safe\u{202E}txt");

        let zero_width = TransactionWriteRow {
            row_pk: None,
            schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY.into(),
            snapshot: Some(transaction_json(json!({
                "id": "01920000-0000-7000-8000-0000000000d6",
                "parent_id": null,
                "name": "zero\u{200D}width",
            }))),
            global: false,
            ..base_stage_row()
        };
        let zero_width = normalize_test_row(zero_width, &mut catalog, functions())
            .expect("normalize zero-width directory");
        let zero_width_snapshot = normalized_snapshot(&zero_width);
        assert_eq!(zero_width_snapshot["name"], "zero\u{200D}width");

        let dotdot = TransactionWriteRow {
            row_pk: None,
            schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.into(),
            snapshot: Some(transaction_json(json!({
                "id": "01920000-0000-7000-8000-0000000000e6",
                "directory_id": null,
                "name": "..",
            }))),
            global: false,
            ..base_stage_row()
        };
        let dotdot = normalize_test_row(dotdot, &mut catalog, functions())
            .expect("Schema v1 treats descriptor names as plain text");
        assert_eq!(normalized_snapshot(&dotdot)["name"], "..");
    }

    #[test]
    fn normalization_treats_filesystem_descriptor_names_as_text() {
        let mut catalog = catalog_with(vec![
            builtin_schema(FILE_DESCRIPTOR_SCHEMA_KEY),
            builtin_schema(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        ]);

        let row = normalize_test_row(
            TransactionWriteRow {
                row_pk: None,
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.into(),
                snapshot: Some(transaction_json(json!({
                    "id": "01920000-0000-7000-8000-0000000000c4",
                    "directory_id": null,
                    "name": "nested/name",
                }))),
                global: false,
                ..base_stage_row()
            },
            &mut catalog,
            functions(),
        )
        .expect("Schema v1 does not apply nested path-segment validation");
        assert_eq!(normalized_snapshot(&row)["name"], "nested/name");
    }

    #[test]
    fn normalization_keeps_file_descriptor_name_opaque() {
        let mut catalog = catalog_with(vec![
            builtin_schema(FILE_DESCRIPTOR_SCHEMA_KEY),
            builtin_schema(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        ]);

        let row = normalize_test_row(
            TransactionWriteRow {
                row_pk: None,
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.into(),
                snapshot: Some(transaction_json(json!({
                    "id": "01920000-0000-7000-8000-0000000000c5",
                    "directory_id": null,
                    "name": "foo.bar",
                }))),
                global: false,
                ..base_stage_row()
            },
            &mut catalog,
            functions(),
        )
        .expect("file descriptor name should be an opaque basename");

        let snapshot = normalized_snapshot(&row);
        assert_eq!(snapshot["name"], "foo.bar");
    }

    #[test]
    fn normalization_supports_global_checkpoint_row() {
        let mut catalog = catalog_with(vec![
            builtin_schema("lix_commit"),
            builtin_schema(crate::checkpoint::CHECKPOINT_SCHEMA_KEY),
        ]);
        let commit_id = "01920000-0000-7000-8000-0000000000c6";
        let row = TransactionWriteRow {
            row_pk: None,
            schema_key: crate::checkpoint::CHECKPOINT_SCHEMA_KEY.into(),
            snapshot: Some(transaction_json(json!({
                "id": commit_id,
                "commit_id": commit_id,
            }))),
            global: true,
            untracked: false,
            branch_id: crate::GLOBAL_BRANCH_ID.into(),
            ..base_stage_row()
        };

        let normalized = normalize_test_row(row, &mut catalog, functions())
            .expect("checkpoint should normalize through its registered row schema");

        assert_eq!(
            normalized.row_pk,
            Some(RowPk::uuid_from_canonical(commit_id).expect("checkpoint commit ID"))
        );
        assert!(
            catalog
                .snapshot()
                .schema(crate::checkpoint::CHECKPOINT_SCHEMA_KEY)
                .is_some()
        );
    }

    fn normalize_test_row(
        row: TransactionWriteRow,
        catalog: &mut TransactionCatalog,
        functions: FunctionProviderHandle,
    ) -> Result<TransactionWriteRow, LixError> {
        let mut rows = RawWriteBatch::with_capacity(1);
        rows.push(row);
        let mut default_timestamp = None;
        normalize_raw_write_row_in_place(&mut rows, 0, catalog, functions, &mut default_timestamp)?;
        Ok(rows.into_rows().pop().expect("single normalized test row"))
    }

    fn normalized_snapshot(row: &TransactionWriteRow) -> &JsonValue {
        row.snapshot
            .as_ref()
            .expect("normalized test row should have a snapshot")
            .value()
    }

    fn catalog_with(schemas: Vec<JsonValue>) -> TransactionCatalog {
        let mut visible_schemas = schemas;
        if visible_schemas.iter().any(|schema| {
            schema.get("key").and_then(JsonValue::as_str) == Some(FILE_DESCRIPTOR_SCHEMA_KEY)
        }) && !visible_schemas.iter().any(|schema| {
            schema.get("key").and_then(JsonValue::as_str) == Some(DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
        }) {
            visible_schemas.push(builtin_schema(DIRECTORY_DESCRIPTOR_SCHEMA_KEY));
        }
        TransactionCatalog::Owned(
            crate::catalog::CatalogSnapshot::from_visible_schemas(&visible_schemas)
                .expect("catalog"),
        )
    }

    fn builtin_schema(schema_key: &str) -> JsonValue {
        seed_schema_definition(schema_key)
            .unwrap_or_else(|| panic!("{schema_key} builtin schema should exist"))
            .clone()
    }

    fn transaction_json(value: JsonValue) -> TransactionJson {
        TransactionJson::from_value_for_test(value)
    }

    fn snapshot_json(value: &str) -> TransactionJson {
        transaction_json(serde_json::from_str(value).expect("test snapshot should parse"))
    }

    fn base_stage_row() -> TransactionWriteRow {
        TransactionWriteRow {
            row_pk: Some(RowPk::single("row-1")),
            schema_key: "normalization_schema".into(),
            file_id: None,
            snapshot: Some(snapshot_json(r#"{"id":"row-1","value":"hello"}"#)),
            metadata: None,
            origin: None,
            created_at: None,
            updated_at: None,
            global: true,
            change_id: None,
            commit_id: None,
            untracked: false,
            branch_id: crate::GLOBAL_BRANCH_ID.into(),
        }
    }

    fn schema_with_default_id() -> JsonValue {
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "normalization_schema",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false, "default_value": "literal-default" },
            ],
            "primary_key": ["id"],
        })
    }

    fn certificate_test_schema(required_value: Option<&str>) -> JsonValue {
        let value_type = match required_value {
            None => "jsonb",
            Some("old-value") => "text",
            Some(_) => "boolean",
        };
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "certificate_schema",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": value_type, "nullable": false },
            ],
            "primary_key": ["id"],
        })
    }

    fn schema_with_cel_field_default() -> JsonValue {
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "cel_field_default_schema",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "name", "type": "text", "nullable": false },
                { "name": "slug", "type": "text", "nullable": false, "default_value": "default-slug" },
            ],
            "primary_key": ["id"],
        })
    }

    fn schema_with_overridden_default() -> JsonValue {
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "overridden_default_schema",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "status", "type": "text", "nullable": false, "default_value": "literal" },
            ],
            "primary_key": ["id"],
        })
    }

    fn schema_with_nullable_default() -> JsonValue {
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "nullable_default_schema",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "status", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["id"],
        })
    }

    fn schema_with_timestamp_default() -> JsonValue {
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "timestamp_default_schema",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "created_at", "type": "timestamptz", "nullable": false, "default_expression": "CURRENT_TIMESTAMP" },
            ],
            "primary_key": ["id"],
        })
    }

    fn composite_key_schema() -> JsonValue {
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "composite_key_schema",
            "columns": [
                { "name": "namespace", "type": "text", "nullable": false },
                { "name": "key", "type": "text", "nullable": false },
            ],
            "primary_key": ["namespace", "key"],
        })
    }

    fn dynamic_schema_definition() -> JsonValue {
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "dynamic_schema",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        })
    }

    #[expect(trivial_casts)]
    fn functions() -> FunctionProviderHandle {
        FunctionProviderHandle::shared(Box::new(FixedFunctions) as Box<dyn FunctionProvider + Send>)
    }

    struct FixedFunctions;

    impl FunctionProvider for FixedFunctions {
        fn uuid_v7(&mut self) -> uuid::Uuid {
            uuid::Uuid::nil()
        }

        fn timestamp(&mut self) -> crate::common::LixTimestamp {
            crate::common::LixTimestamp::expect_parse("timestamp", "1970-01-01T00:00:00.000Z")
        }
    }
}
