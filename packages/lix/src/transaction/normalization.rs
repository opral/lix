#![allow(clippy::needless_raw_string_hashes, clippy::redundant_clone)]

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::LixError;
use crate::catalog::{SchemaPlan, SchemaPlanId, TransactionCatalog};
use crate::common::{format_json_pointer, validate_row_metadata};
use crate::domain::Domain;
use crate::entity_pk::{EntityPk, EntityPkError};
use crate::functions::FunctionProviderHandle;
use crate::schema::{
    SchemaKey, schema_from_registered_snapshot, validate_lix_schema, validate_lix_schema_definition,
};
use crate::sql2::PublicCatalog;
#[cfg(test)]
use crate::transaction_types::TransactionWriteRow;
use crate::transaction_types::{
    NativeRowPayload, PreparedRowFacts, RawWriteBatch, RawWriteRowRef, TransactionJson,
};

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

/// Normalizes one incoming row into a row with final snapshot/entity primary key.
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

    if let Some(certificate) = row
        .snapshot
        .and_then(TransactionJson::canonical_batch_certificate)
        .map(|certificate| certificate.into_owned())
    {
        let normalized = row
            .snapshot
            .and_then(TransactionJson::canonical_batch_normalized_shared)
            .expect("a canonical v2 certificate belongs to a canonical batch row");
        if certificate.schema_fingerprint() != schema_plan.fingerprint()
            || !schema_plan.accepts_canonical_certificate()
        {
            // A schema amendment can be staged after the plugin transition
            // was drained against its pinned SQL catalog. The old certificate
            // is then only a transport optimization: decode the retained
            // canonical bytes and run the ordinary current-plan path below.
            rows.set_snapshot(
                row_index,
                Some(TransactionJson::from_unvalidated_shared_normalized_content(
                    normalized,
                )),
            );
        } else {
            if row.entity_pk != Some(certificate.entity_pk()) {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "certified plugin identity does not match the staged entity_pk for schema '{}'",
                        row.schema_key
                    ),
                ));
            }
            if let Some(metadata) = row.metadata {
                if !metadata.metadata_content_certified() {
                    validate_row_metadata(
                        metadata.value(),
                        format!("metadata for schema '{}'", row.schema_key),
                    )?;
                }
            }
            *rows.entity_pk_mut(row_index) = Some(certificate.entity_pk().clone());
            rows.set_snapshot(
                row_index,
                Some(TransactionJson::from_certified_shared_normalized_row_content(normalized)),
            );
            attach_native_schema_v1_row(rows, row_index, schema_plan)?;
            canonicalize_descriptor_file_id(rows, row_index)?;
            return Ok(NormalizedRowFacts {
                schema_plan_id,
                facts: PreparedRowFacts {
                    row_content_validated: true,
                    requires_transaction_validation: false,
                },
            });
        }
    }

    let row = rows.row(row_index);
    if row
        .snapshot
        .is_some_and(TransactionJson::row_content_certified)
    {
        if row.entity_pk.is_none() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified replacement row is missing its proven entity identity",
            ));
        }
        attach_native_schema_v1_row(rows, row_index, schema_plan)?;
        canonicalize_descriptor_file_id(rows, row_index)?;
        return Ok(NormalizedRowFacts {
            schema_plan_id,
            facts: PreparedRowFacts {
                row_content_validated: true,
                requires_transaction_validation: false,
            },
        });
    }

    let mut normalized_snapshot = if let Some(snapshot) = rows.take_snapshot(row_index) {
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
            // result once. Complete plugin snapshots retain their batch
            // handle through the branch below.
            let mut snapshot = snapshot_object_for_mutation(snapshot, row)?;
            apply_defaults(
                &mut snapshot,
                schema_plan,
                row,
                functions,
                default_timestamp,
            )?;
            let snapshot = JsonValue::Object(snapshot);
            let entity_pk = resolve_entity_pk(row, schema_plan, &snapshot)?;
            *rows.entity_pk_mut(row_index) = Some(entity_pk);
            Some(TransactionJson::from_value(
                snapshot,
                "normalized transaction snapshot_content",
            )?)
        } else {
            let entity_pk = resolve_entity_pk(row, schema_plan, snapshot.value())?;
            *rows.entity_pk_mut(row_index) = Some(entity_pk);
            Some(snapshot)
        }
    } else if rows.row(row_index).entity_pk.is_none() {
        let schema_key = rows.row(row_index).schema_key.clone();
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!("tombstone for schema '{}' requires entity_pk", schema_key),
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

    if let Some(snapshot) = normalized_snapshot.as_mut() {
        let row = rows.row(row_index);
        if let Some(native_row) = native_schema_v1_row(
            schema_plan,
            row.entity_pk,
            row.branch_id,
            row.file_id.map(AsRef::as_ref),
            row.untracked,
            snapshot.value(),
        )?
        {
            snapshot.set_native_row(native_row);
        }
    }

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

    rows.set_snapshot(row_index, normalized_snapshot);
    canonicalize_descriptor_file_id(rows, row_index)?;
    Ok(NormalizedRowFacts {
        schema_plan_id,
        facts: PreparedRowFacts {
            row_content_validated: true,
            requires_transaction_validation,
        },
    })
}

fn attach_native_schema_v1_row(
    rows: &mut RawWriteBatch,
    row_index: usize,
    schema_plan: &SchemaPlan,
) -> Result<(), LixError> {
    let row = rows.row(row_index);
    let entity_pk = row
        .entity_pk
        .cloned()
        .ok_or_else(|| LixError::new(LixError::CODE_INTERNAL_ERROR, "native row lacks identity"))?;
    let branch_id = row.branch_id.to_owned();
    let file_id = row.file_id.map(ToOwned::to_owned);
    let untracked = row.untracked;
    let native = {
        let snapshot = rows
            .row(row_index)
            .snapshot
            .ok_or_else(|| LixError::new(LixError::CODE_INTERNAL_ERROR, "native row lacks payload"))?;
        native_schema_v1_row(
            schema_plan,
            Some(&entity_pk),
            &branch_id,
            file_id.as_deref(),
            untracked,
            snapshot.value(),
        )?
    };
    if let Some(native) = native {
        rows.snapshot_mut(row_index)
            .expect("snapshot was checked above")
            .set_native_row(native);
    }
    Ok(())
}

fn native_schema_v1_row(
    schema_plan: &SchemaPlan,
    entity_pk: Option<&EntityPk>,
    branch_id: &str,
    file_id: Option<&str>,
    untracked: bool,
    snapshot: &JsonValue,
) -> Result<Option<NativeRowPayload>, LixError> {
    let Ok(schema) = lix_schema::from_value(schema_plan.schema.as_ref().clone()) else {
        return Ok(None);
    };
    let entity_pk = entity_pk.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "normalized Schema v1 row is missing its typed primary key",
        )
    })?;
    native_schema_v1_payload(
        &schema,
        entity_pk,
        branch_id,
        file_id,
        untracked,
        snapshot,
    )
    .map(Some)
}

fn native_schema_v1_payload(
    schema: &lix_schema::Schema,
    entity_pk: &EntityPk,
    branch_id: &str,
    file_id: Option<&str>,
    untracked: bool,
    snapshot: &JsonValue,
) -> Result<NativeRowPayload, LixError> {
    crate::native_row::encode(
        schema,
        entity_pk,
        branch_id,
        file_id,
        untracked,
        snapshot,
    )
}

fn canonicalize_descriptor_file_id(
    rows: &mut RawWriteBatch,
    row_index: usize,
) -> Result<(), LixError> {
    let row = rows.row(row_index);
    let file_id = match row.schema_key.as_str() {
        FILE_DESCRIPTOR_SCHEMA_KEY => {
            let entity_pk = row
                .entity_pk
                .expect("normalized row has an entity identity");
            Some(
                entity_pk
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
/// from public entity surfaces. Internal producers may therefore validate
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

fn resolve_entity_pk(
    row: RawWriteRowRef<'_>,
    schema_plan: &SchemaPlan,
    snapshot: &JsonValue,
) -> Result<EntityPk, LixError> {
    let Some(primary_key_paths) = schema_plan.primary_key.as_ref() else {
        return row.entity_pk.cloned().ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "write for schema '{}' requires entity_pk because the schema has no primary_key",
                    row.schema_key
                ),
            )
        });
    };
    let component_types = schema_plan
        .primary_key_component_types
        .as_deref()
        .expect("primary-key paths and component types are compiled together");
    let derived = EntityPk::from_primary_key_plan(snapshot, primary_key_paths, component_types)
        .map_err(|error| entity_pk_derivation_error(row, primary_key_paths, error))?;
    if let Some(entity_pk) = row.entity_pk {
        if entity_pk.as_json_array_value()? != derived.as_json_array_value()? {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "entity_pk '{}' does not match primary_key-derived entity_pk '{}' for schema '{}'",
                    entity_pk.as_json_array_text()?,
                    derived.as_json_array_text()?,
                    row.schema_key
                ),
            ));
        }
    }
    Ok(derived)
}

fn entity_pk_derivation_error(
    row: RawWriteRowRef<'_>,
    primary_key_paths: &[Vec<String>],
    error: EntityPkError,
) -> LixError {
    let detail = match error {
        EntityPkError::EmptyPrimaryKey => "empty primary_key".to_string(),
        EntityPkError::EmptyPrimaryKeyPath { index } => {
            format!("empty primary_key column at index {index}")
        }
        EntityPkError::MissingPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("missing value at primary-key pointer '{pointer}'")
        }
        EntityPkError::UnsupportedPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("unsupported value at primary-key pointer '{pointer}'")
        }
        EntityPkError::InvalidPrimaryKeyValue { index, expected } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("value at primary-key pointer '{pointer}' must be a valid {expected}")
        }
        EntityPkError::InvalidEncodedEntityPk => "invalid encoded entity primary key".to_string(),
    };
    LixError::new(
        LixError::CODE_SCHEMA_VALIDATION,
        format!(
            "failed to derive entity_pk for schema '{}': {detail}",
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
    reject_reserved_schema_namespace(&key)?;
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::functions::FunctionProvider;
    use crate::schema::seed_schema_definition;

    #[test]
    fn normalization_derives_entity_pk_from_primary_key() {
        let mut catalog = catalog_with(vec![schema_with_default_id()]);
        let row = TransactionWriteRow {
            entity_pk: None,
            schema_key: "normalization_schema".into(),
            snapshot: Some(snapshot_json(
                r#"{"id":"entity-from-snapshot","value":"hello"}"#,
            )),
            ..base_stage_row()
        };

        let row = normalize_test_row(row, &mut catalog, functions()).expect("normalize row");

        assert_eq!(
            row.entity_pk.as_ref(),
            Some(&EntityPk::single("entity-from-snapshot"))
        );
    }

    #[test]
    fn normalization_retains_complete_canonical_batch_rows() {
        let normalized = br#"{"id":"entity-from-batch","value":"hello"}"#.to_vec();
        let end = u32::try_from(normalized.len()).expect("fixture length");
        let mut batch = crate::plugin::runtime::WasmCanonicalJson::from_batch_parts(
            vec![json!({"id": "entity-from-batch", "value": "hello"})],
            normalized,
            vec![(0, end)],
            1,
            1,
        )
        .expect("canonical batch");
        let batch_row = batch.pop().expect("canonical row");
        let mut catalog = catalog_with(vec![schema_with_default_id()]);
        let row = TransactionWriteRow {
            entity_pk: None,
            schema_key: "normalization_schema".into(),
            snapshot: Some(TransactionJson::from_canonical_batch(batch_row.clone())),
            ..base_stage_row()
        };

        let normalized = normalize_test_row(row, &mut catalog, functions()).expect("normalize row");
        let retained = normalized
            .snapshot
            .as_ref()
            .and_then(TransactionJson::canonical_batch_row)
            .expect("complete row must retain its canonical batch");

        assert!(batch_row.shares_batch_with(retained));
        assert_eq!(
            normalized.entity_pk.as_ref(),
            Some(&EntityPk::single("entity-from-batch"))
        );
        assert_eq!(retained.validation_counts(), (1, 1));
    }

    #[test]
    fn canonical_certificate_falls_back_when_schema_plan_was_amended() {
        let old_schema = certificate_test_schema(None);
        let old_catalog = catalog_with(vec![old_schema]);
        let (_, old_plan) = old_catalog
            .snapshot()
            .plan_for_key("certificate_schema")
            .expect("old certificate schema");
        let normalized = br#"{"id":"entity-1","value":"old-value"}"#.to_vec();
        let end = u32::try_from(normalized.len()).expect("fixture length");
        let certificate = crate::plugin::runtime::WasmCanonicalJsonCertificate::new(
            EntityPk::single("entity-1"),
            old_plan.shared_fingerprint(),
        );
        let mut batch = crate::plugin::runtime::WasmCanonicalJson::from_mixed_batch_parts(
            vec![None],
            vec![Some(certificate)],
            normalized,
            vec![(0, end)],
            1,
            0,
        )
        .expect("certified canonical batch");
        let batch_row = batch.pop().expect("certified row");

        // A harmless amendment still forces the ordinary decoded path.
        let mut amended_catalog = catalog_with(vec![certificate_test_schema(Some("old-value"))]);
        let row = TransactionWriteRow {
            entity_pk: Some(EntityPk::single("entity-1")),
            schema_key: "certificate_schema".into(),
            snapshot: Some(TransactionJson::from_canonical_batch(batch_row.clone())),
            ..base_stage_row()
        };
        let row = normalize_test_row(row, &mut amended_catalog, functions()).expect("amended row");
        assert!(
            row.snapshot
                .as_ref()
                .and_then(TransactionJson::canonical_batch_row)
                .is_none(),
            "a stale certificate must be replaced by an ordinary decoded row"
        );
        assert_eq!(normalized_snapshot(&row)["value"], "old-value");

        // A stricter amendment must be enforced rather than bypassed by the
        // old plan's otherwise valid certificate.
        let mut rejecting_catalog = catalog_with(vec![certificate_test_schema(Some("new-value"))]);
        let row = TransactionWriteRow {
            entity_pk: Some(EntityPk::single("entity-1")),
            schema_key: "certificate_schema".into(),
            snapshot: Some(TransactionJson::from_canonical_batch(batch_row)),
            ..base_stage_row()
        };
        let error = normalize_test_row(row, &mut rejecting_catalog, functions())
            .expect_err("amended row-local constraint must be enforced");
        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
    }

    #[test]
    fn historical_shared_snapshot_is_revalidated_after_schema_amendment() {
        let canonical = crate::common::SharedStr::from(r#"{"id":"entity-1","value":"old-value"}"#);
        let row = || TransactionWriteRow {
            entity_pk: Some(EntityPk::single("entity-1")),
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
    fn normalization_applies_cel_defaults_from_snapshot_context() {
        let mut catalog = catalog_with(vec![schema_with_cel_field_default()]);
        let row = TransactionWriteRow {
            entity_pk: None,
            schema_key: "cel_field_default_schema".into(),
            snapshot: Some(snapshot_json(r#"{"id":"entity-1","name":"Sample"}"#)),
            ..base_stage_row()
        };

        let row = normalize_test_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(snapshot["slug"], "Sample-slug");
    }

    #[test]
    fn normalization_x_lix_default_overrides_json_default() {
        let mut catalog = catalog_with(vec![schema_with_overridden_default()]);
        let row = TransactionWriteRow {
            entity_pk: None,
            schema_key: "overridden_default_schema".into(),
            snapshot: Some(snapshot_json(r#"{"id":"entity-1"}"#)),
            ..base_stage_row()
        };

        let row = normalize_test_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(snapshot["status"], "computed");
    }

    #[test]
    fn normalization_does_not_overwrite_explicit_null_with_default() {
        let mut catalog = catalog_with(vec![schema_with_nullable_default()]);
        let row = TransactionWriteRow {
            entity_pk: None,
            schema_key: "nullable_default_schema".into(),
            snapshot: Some(snapshot_json(r#"{"id":"entity-1","status":null}"#)),
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
            entity_pk: None,
            schema_key: "timestamp_default_schema".into(),
            snapshot: Some(snapshot_json(r#"{"id":"entity-1"}"#)),
            ..base_stage_row()
        };

        let row = normalize_test_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(snapshot["created_at"], "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn normalization_rejects_entity_pk_that_disagrees_with_primary_key() {
        let mut catalog = catalog_with(vec![schema_with_default_id()]);
        let row = TransactionWriteRow {
            entity_pk: Some(EntityPk::single("wrong-id")),
            schema_key: "normalization_schema".into(),
            snapshot: Some(snapshot_json(r#"{"id":"right-id","value":"hello"}"#)),
            ..base_stage_row()
        };

        let error =
            normalize_test_row(row, &mut catalog, functions()).expect_err("id mismatch fails");

        assert!(
            error
                .message
                .contains("does not match x-lix-primary-key derived entity_pk")
        );
    }

    #[test]
    fn normalization_derives_json_array_entity_pk_for_composite_primary_key() {
        let mut catalog = catalog_with(vec![composite_key_schema()]);
        let row = TransactionWriteRow {
            entity_pk: None,
            schema_key: "composite_key_schema".into(),
            snapshot: Some(snapshot_json(r#"{"namespace":"a~b","key":"1"}"#)),
            ..base_stage_row()
        };

        let row = normalize_test_row(row, &mut catalog, functions()).expect("normalize row");
        let entity_pk = row.entity_pk.expect("composite entity pk");
        let projected_entity_pk = entity_pk
            .as_json_array_text()
            .expect("entity pk should project");

        assert_eq!(projected_entity_pk, "[\"a~b\",\"1\"]");
    }

    #[test]
    fn normalization_rejects_non_string_primary_key_values() {
        let mut catalog = catalog_with(vec![composite_key_schema()]);
        let row = TransactionWriteRow {
            entity_pk: None,
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
    fn normalization_validates_explicit_composite_entity_pk_against_projection() {
        let mut catalog = catalog_with(vec![composite_key_schema()]);
        let snapshot = json!({
            "namespace": "a~b",
            "key": "1",
        });
        let derived = EntityPk::from_primary_key_paths(
            &snapshot,
            &[vec!["namespace".to_string()], vec!["key".to_string()]],
        )
        .expect("identity should derive");
        let row = TransactionWriteRow {
            entity_pk: Some(derived.clone()),
            schema_key: "composite_key_schema".into(),
            snapshot: Some(transaction_json(snapshot.clone())),
            ..base_stage_row()
        };

        let row = normalize_test_row(row, &mut catalog, functions()).expect("normalize row");

        assert_eq!(row.entity_pk.as_ref(), Some(&derived));
    }

    #[test]
    fn normalization_makes_pending_registered_schema_visible_to_later_rows() {
        let mut catalog = catalog_with(vec![
            seed_schema_definition(REGISTERED_SCHEMA_KEY)
                .expect("registered schema builtin")
                .clone(),
        ]);
        let registered = TransactionWriteRow {
            entity_pk: None,
            schema_key: REGISTERED_SCHEMA_KEY.into(),
            snapshot: Some(transaction_json(json!({
                "value": dynamic_schema_definition(),
            }))),
            ..base_stage_row()
        };

        normalize_test_row(registered, &mut catalog, functions()).expect("register schema");

        let dynamic = TransactionWriteRow {
            entity_pk: None,
            schema_key: "dynamic_schema".into(),
            snapshot: Some(snapshot_json(r#"{"id":"dynamic-1"}"#)),
            ..base_stage_row()
        };
        let dynamic = normalize_test_row(dynamic, &mut catalog, functions()).expect("dynamic row");

        assert_eq!(
            dynamic.entity_pk.as_ref(),
            Some(&EntityPk::single("dynamic-1"))
        );
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
            schema["x-lix-key"] = json!(schema_key);
            let registered = TransactionWriteRow {
                entity_pk: None,
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
        schema["x-lix-key"] = json!("acme_plugin_note");
        let registered = TransactionWriteRow {
            entity_pk: None,
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
            entity_pk: None,
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
            entity_pk: None,
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
            entity_pk: None,
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
            entity_pk: None,
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
            entity_pk: None,
            schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.into(),
            snapshot: Some(transaction_json(json!({
                "id": "01920000-0000-7000-8000-0000000000e6",
                "directory_id": null,
                "name": "..",
            }))),
            global: false,
            ..base_stage_row()
        };
        let error = normalize_test_row(dotdot, &mut catalog, functions())
            .expect_err("schema validation should reject a parent-directory segment");
        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
    }

    #[test]
    fn normalization_applies_structural_filesystem_descriptor_schema() {
        let mut catalog = catalog_with(vec![
            builtin_schema(FILE_DESCRIPTOR_SCHEMA_KEY),
            builtin_schema(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        ]);

        let error = normalize_test_row(
            TransactionWriteRow {
                entity_pk: None,
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.into(),
                snapshot: Some(transaction_json(json!({
                    "id": "file-slash",
                    "directory_id": null,
                    "name": "nested/name",
                }))),
                global: false,
                ..base_stage_row()
            },
            &mut catalog,
            functions(),
        )
        .expect_err("schema validation should reject a path in a descriptor name");
        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
    }

    #[test]
    fn normalization_keeps_file_descriptor_name_opaque() {
        let mut catalog = catalog_with(vec![builtin_schema(FILE_DESCRIPTOR_SCHEMA_KEY)]);

        let row = normalize_test_row(
            TransactionWriteRow {
                entity_pk: None,
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
    fn normalization_supports_global_checkpoint_entity() {
        let mut catalog = catalog_with(vec![
            builtin_schema("lix_commit"),
            builtin_schema(crate::checkpoint::CHECKPOINT_SCHEMA_KEY),
        ]);
        let commit_id = "01920000-0000-7000-8000-0000000000c6";
        let row = TransactionWriteRow {
            entity_pk: None,
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
            .expect("checkpoint should normalize through its registered entity schema");

        assert_eq!(
            normalized.entity_pk,
            Some(EntityPk::uuid_from_canonical(commit_id).expect("checkpoint commit ID"))
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
        normalize_raw_write_row_in_place(
            &mut rows,
            0,
            catalog,
            functions,
            &mut default_timestamp,
        )?;
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
            schema.get("x-lix-key").and_then(JsonValue::as_str) == Some(FILE_DESCRIPTOR_SCHEMA_KEY)
        }) && !visible_schemas.iter().any(|schema| {
            schema.get("x-lix-key").and_then(JsonValue::as_str)
                == Some(DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
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
            entity_pk: Some(EntityPk::single("entity-1")),
            schema_key: "normalization_schema".into(),
            file_id: None,
            snapshot: Some(snapshot_json(r#"{"id":"entity-1","value":"hello"}"#)),
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
            "x-lix-key": "normalization_schema",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string", "x-lix-default": "uuidv7()" },
                "value": { "type": "string", "default": "literal-default" }
            },
            "required": ["id", "value"],
            "additionalProperties": false
        })
    }

    fn certificate_test_schema(required_value: Option<&str>) -> JsonValue {
        let value_schema = required_value.map_or_else(
            || json!({"type": "string"}),
            |required| json!({"type": "string", "const": required}),
        );
        json!({
            "x-lix-key": "certificate_schema",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "value": value_schema,
            },
            "required": ["id", "value"],
            "additionalProperties": false,
        })
    }

    fn schema_with_cel_field_default() -> JsonValue {
        json!({
            "x-lix-key": "cel_field_default_schema",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "name": { "type": "string" },
                "slug": { "type": "string", "x-lix-default": "name + '-slug'" }
            },
            "required": ["id", "name"],
            "additionalProperties": false
        })
    }

    fn schema_with_overridden_default() -> JsonValue {
        json!({
            "x-lix-key": "overridden_default_schema",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "status": {
                    "type": "string",
                    "default": "literal",
                    "x-lix-default": "'computed'"
                }
            },
            "required": ["id"],
            "additionalProperties": false
        })
    }

    fn schema_with_nullable_default() -> JsonValue {
        json!({
            "x-lix-key": "nullable_default_schema",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "status": {
                    "anyOf": [{ "type": "string" }, { "type": "null" }],
                    "x-lix-default": "'computed'"
                }
            },
            "required": ["id"],
            "additionalProperties": false
        })
    }

    fn schema_with_timestamp_default() -> JsonValue {
        json!({
            "x-lix-key": "timestamp_default_schema",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "created_at": { "type": "string", "x-lix-default": "CURRENT_TIMESTAMP" }
            },
            "required": ["id"],
            "additionalProperties": false
        })
    }

    fn composite_key_schema() -> JsonValue {
        json!({
            "x-lix-key": "composite_key_schema",
            "x-lix-primary-key": ["/namespace", "/key"],
            "type": "object",
            "properties": {
                "namespace": { "type": "string" },
                "key": { "type": "string" }
            },
            "required": ["namespace", "key"],
            "additionalProperties": false
        })
    }

    fn dynamic_schema_definition() -> JsonValue {
        json!({
            "x-lix-key": "dynamic_schema",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" }
            },
            "required": ["id"],
            "additionalProperties": false
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
