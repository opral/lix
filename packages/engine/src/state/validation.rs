use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use jsonschema::JSONSchema;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::schema::{
    schema_from_registered_snapshot, validate_lix_schema_definition, OverlaySchemaProvider,
    SchemaKey, SchemaProvider, SqlRegisteredSchemaProvider,
};
use crate::sql::ast::utils::bind_sql;
use crate::sql::execution::contracts::planned_statement::{
    MutationOperation, MutationRow, UpdateValidationKind, UpdateValidationPlan,
};
use crate::sql::public::catalog::SurfaceFamily;
use crate::sql::public::planner::ir::{
    InsertOnConflictAction, PlannedStateRow, PlannedWrite, ResolvedWritePlan, WriteMode,
    WriteOperationKind,
};
use crate::{LixBackend, LixError, SqlDialect, Value};

const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const REGISTERED_SCHEMA_FILE_ID: &str = "lix";
const REGISTERED_SCHEMA_PLUGIN_KEY: &str = "lix";
const REGISTERED_SCHEMA_VERSION_ID: &str = "global";

#[derive(Debug, Default)]
pub struct SchemaCache {
    inner: RwLock<HashMap<SchemaKey, Arc<JSONSchema>>>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ConstraintStorageKind {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConstraintRowIdentity {
    entity_id: String,
    schema_key: String,
    file_id: String,
    version_id: String,
}

#[derive(Debug, Clone)]
struct ConstraintCandidateRow {
    index: usize,
    identity: ConstraintRowIdentity,
    schema_version: String,
    snapshot: JsonValue,
    storage: ConstraintStorageKind,
    shadows_committed_identity: bool,
}

#[derive(Debug, Clone)]
struct ConstraintCommittedRow {
    identity: ConstraintRowIdentity,
    schema_version: String,
    snapshot: JsonValue,
}

#[derive(Debug, Clone)]
struct ConstraintDeletedRow {
    identity: ConstraintRowIdentity,
    schema_version: String,
    snapshot: JsonValue,
    storage: ConstraintStorageKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConstraintScopeKey {
    storage: ConstraintStorageKind,
    schema_key: String,
    file_id: String,
    version_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConstraintSchemaVersionKey {
    storage: ConstraintStorageKind,
    schema_key: String,
    version_id: String,
}

#[derive(Debug, Default)]
struct ConstraintContext {
    pending_rows: Vec<ConstraintCandidateRow>,
    deleted_rows: Vec<ConstraintDeletedRow>,
    deleted_identities: HashSet<(ConstraintStorageKind, ConstraintRowIdentity)>,
    committed_rows: HashMap<ConstraintScopeKey, Vec<ConstraintCommittedRow>>,
    committed_schema_version_rows: HashMap<ConstraintSchemaVersionKey, Vec<ConstraintCommittedRow>>,
}

#[derive(Debug, Clone)]
struct ConstraintRowView<'a> {
    identity: &'a ConstraintRowIdentity,
    schema_version: &'a str,
    snapshot: &'a JsonValue,
}

pub async fn validate_inserts(
    backend: &dyn LixBackend,
    cache: &SchemaCache,
    mutations: &[MutationRow],
) -> Result<(), LixError> {
    let mut schema_provider = OverlaySchemaProvider::from_backend(backend);

    for row in mutations {
        if row.operation == MutationOperation::Insert && row.schema_key == REGISTERED_SCHEMA_KEY {
            validate_registered_schema_insert(&mut schema_provider, row).await?;
            if let Some(snapshot) = row.snapshot_content.as_ref() {
                let (key, schema) = schema_from_registered_snapshot(snapshot)?;
                schema_provider.remember_pending_schema(key, schema);
            }
        }
    }

    let pending_rows = collect_insert_constraint_candidates(mutations);

    for row in mutations {
        if row.operation != MutationOperation::Insert || row.schema_key == REGISTERED_SCHEMA_KEY {
            continue;
        }

        let Some(snapshot) = row.snapshot_content.as_ref() else {
            continue;
        };

        let key = SchemaKey::new(row.schema_key.clone(), row.schema_version.clone());
        validate_snapshot_content(&mut schema_provider, cache, &key, snapshot).await?;
        validate_entity_id_matches_primary_key(
            &mut schema_provider,
            &key,
            &row.entity_id,
            snapshot,
        )
        .await?;
        validate_filesystem_insert_integrity(backend, row, snapshot).await?;
    }

    let mut constraints = ConstraintContext {
        pending_rows,
        deleted_rows: Vec::new(),
        deleted_identities: HashSet::new(),
        committed_rows: HashMap::new(),
        committed_schema_version_rows: HashMap::new(),
    };
    for row in &constraints.pending_rows.clone() {
        validate_row_constraints(backend, &mut schema_provider, &mut constraints, row).await?;
    }

    Ok(())
}

pub async fn validate_updates(
    backend: &dyn LixBackend,
    cache: &SchemaCache,
    plans: &[UpdateValidationPlan],
    params: &[Value],
) -> Result<(), LixError> {
    let mut schema_provider = SqlRegisteredSchemaProvider::new(backend);
    let mut pending_rows = Vec::new();
    let mut deleted_rows = Vec::new();

    for plan in plans {
        let mut sql = format!(
            "SELECT entity_id, file_id, version_id, plugin_key, schema_key, schema_version, snapshot_content FROM {}",
            plan.table
        );
        if let Some(where_clause) = &plan.where_clause {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clause.to_string());
        }

        let bound = bind_sql(&sql, params, backend.dialect())?;
        let result = backend.execute(&bound.sql, &bound.params).await?;
        if result.rows.is_empty() {
            continue;
        }

        for row in result.rows {
            let entity_id = value_to_string(&row[0], "entity_id")?;
            let schema_key = value_to_string(&row[4], "schema_key")?;
            let schema_version = value_to_string(&row[5], "schema_version")?;
            let snapshot = resolve_update_snapshot(plan, row.get(6), &schema_key)?;
            let storage = storage_kind_for_table(&plan.table);

            if schema_key == REGISTERED_SCHEMA_KEY {
                if let Some(snapshot) = snapshot.as_ref() {
                    validate_registered_schema_snapshot(&mut schema_provider, snapshot).await?;
                }
                continue;
            }

            if plan.kind == UpdateValidationKind::Delete {
                let snapshot = parse_row_snapshot_content(row.get(6), &schema_key)?;
                deleted_rows.push(ConstraintDeletedRow {
                    identity: ConstraintRowIdentity {
                        entity_id,
                        schema_key,
                        file_id: value_to_string(&row[1], "file_id")?,
                        version_id: value_to_string(&row[2], "version_id")?,
                    },
                    schema_version,
                    snapshot,
                    storage,
                });
                continue;
            }

            let key = SchemaKey::new(schema_key.clone(), schema_version.clone());
            let schema = schema_provider.load_schema(&key).await?;

            if schema.get("x-lix-immutable").and_then(|v| v.as_bool()) == Some(true) {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "Schema '{}' is immutable and cannot be updated.",
                        schema_key
                    ),
                });
            }

            if let Some(snapshot) = snapshot.as_ref() {
                validate_snapshot_content(&mut schema_provider, cache, &key, snapshot).await?;
                validate_entity_id_matches_primary_key(
                    &mut schema_provider,
                    &key,
                    &entity_id,
                    snapshot,
                )
                .await?;

                pending_rows.push(ConstraintCandidateRow {
                    index: pending_rows.len(),
                    identity: ConstraintRowIdentity {
                        entity_id,
                        schema_key: schema_key.clone(),
                        file_id: value_to_string(&row[1], "file_id")?,
                        version_id: value_to_string(&row[2], "version_id")?,
                    },
                    schema_version,
                    snapshot: snapshot.clone(),
                    storage,
                    shadows_committed_identity: true,
                });
            }
        }
    }

    let deleted_identities = deleted_rows
        .iter()
        .map(|row| (row.storage, row.identity.clone()))
        .collect();
    let mut constraints = ConstraintContext {
        pending_rows,
        deleted_rows,
        deleted_identities,
        committed_rows: HashMap::new(),
        committed_schema_version_rows: HashMap::new(),
    };
    for row in &constraints.pending_rows.clone() {
        validate_row_constraints(backend, &mut schema_provider, &mut constraints, row).await?;
    }
    let deleted_rows = constraints.deleted_rows.clone();
    if !deleted_rows.is_empty() {
        validate_delete_constraints(
            backend,
            &mut schema_provider,
            &mut constraints,
            &deleted_rows,
        )
        .await?;
    }

    Ok(())
}

pub(crate) async fn validate_sql2_batch_local_write(
    backend: &dyn LixBackend,
    cache: &SchemaCache,
    planned_write: &PlannedWrite,
) -> Result<(), LixError> {
    validate_sql2_write(backend, cache, planned_write, false).await
}

pub(crate) async fn validate_sql2_append_time_write(
    backend: &dyn LixBackend,
    cache: &SchemaCache,
    planned_write: &PlannedWrite,
) -> Result<(), LixError> {
    validate_sql2_write(backend, cache, planned_write, true).await
}

async fn validate_sql2_write(
    backend: &dyn LixBackend,
    cache: &SchemaCache,
    planned_write: &PlannedWrite,
    require_binary_blob_ref_cas: bool,
) -> Result<(), LixError> {
    let resolved = planned_write
        .resolved_write_plan
        .as_ref()
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "sql2 validation requires a resolved write plan".to_string(),
        })?;
    let mut schema_provider = OverlaySchemaProvider::from_backend(backend);
    remember_pending_sql2_registered_schemas(&mut schema_provider, resolved).await?;
    let shadows_committed_identity = planned_write.command.operation_kind
        == WriteOperationKind::Update
        || planned_write
            .command
            .on_conflict
            .as_ref()
            .is_some_and(|conflict| conflict.action == InsertOnConflictAction::DoUpdate);
    let pending_rows = collect_sql2_constraint_candidates(resolved, shadows_committed_identity)?;
    let deleted_rows = collect_sql2_delete_candidates(resolved)?;

    if planned_write.command.operation_kind == WriteOperationKind::Update {
        for row in resolved.intended_post_state() {
            if row.tombstone {
                continue;
            }
            validate_sql2_update_is_mutable(&mut schema_provider, row).await?;
        }
    }

    for row in resolved.intended_post_state() {
        validate_sql2_planned_row(
            backend,
            &mut schema_provider,
            cache,
            planned_write.command.operation_kind,
            row,
            require_binary_blob_ref_cas,
        )
        .await?;
    }

    if !matches!(
        planned_write.command.target.descriptor.surface_family,
        SurfaceFamily::State | SurfaceFamily::Entity
    ) {
        return Ok(());
    }

    let mut constraints = ConstraintContext {
        pending_rows,
        deleted_rows,
        deleted_identities: HashSet::new(),
        committed_rows: HashMap::new(),
        committed_schema_version_rows: HashMap::new(),
    };
    constraints.deleted_identities = constraints
        .deleted_rows
        .iter()
        .map(|row| (row.storage, row.identity.clone()))
        .collect();
    for row in &constraints.pending_rows.clone() {
        validate_row_constraints(backend, &mut schema_provider, &mut constraints, row).await?;
    }
    let deleted_rows = constraints.deleted_rows.clone();
    if !deleted_rows.is_empty() {
        validate_delete_constraints(
            backend,
            &mut schema_provider,
            &mut constraints,
            &deleted_rows,
        )
        .await?;
    }

    Ok(())
}

async fn remember_pending_sql2_registered_schemas(
    provider: &mut OverlaySchemaProvider<'_>,
    resolved: &ResolvedWritePlan,
) -> Result<(), LixError> {
    for row in resolved.intended_post_state() {
        if row.tombstone || row.schema_key != REGISTERED_SCHEMA_KEY {
            continue;
        }
        let Some(snapshot) = planned_row_snapshot(row)? else {
            continue;
        };
        validate_registered_schema_snapshot(provider, &snapshot).await?;
        provider.remember_pending_schema_from_snapshot(&snapshot)?;
    }
    Ok(())
}

fn collect_insert_constraint_candidates(mutations: &[MutationRow]) -> Vec<ConstraintCandidateRow> {
    collapse_shadowing_constraint_candidates(
        mutations
            .iter()
            .enumerate()
            .filter(|row| {
                row.1.operation == MutationOperation::Insert
                    && row.1.schema_key != REGISTERED_SCHEMA_KEY
                    && row.1.snapshot_content.is_some()
            })
            .filter_map(|(index, row)| {
                row.snapshot_content
                    .as_ref()
                    .map(|snapshot| ConstraintCandidateRow {
                        index,
                        identity: ConstraintRowIdentity {
                            entity_id: row.entity_id.clone(),
                            schema_key: row.schema_key.clone(),
                            file_id: row.file_id.clone(),
                            version_id: row.version_id.clone(),
                        },
                        schema_version: row.schema_version.clone(),
                        snapshot: snapshot.clone(),
                        storage: if row.untracked {
                            ConstraintStorageKind::Untracked
                        } else {
                            ConstraintStorageKind::Tracked
                        },
                        shadows_committed_identity: true,
                    })
            })
            .collect(),
    )
}

fn collect_sql2_constraint_candidates(
    resolved: &ResolvedWritePlan,
    shadows_committed_identity: bool,
) -> Result<Vec<ConstraintCandidateRow>, LixError> {
    let mut rows = Vec::new();
    let mut next_index = 0usize;

    for partition in &resolved.partitions {
        let storage = storage_kind_for_write_mode(partition.execution_mode);
        for row in &partition.intended_post_state {
            if row.tombstone || row.schema_key == REGISTERED_SCHEMA_KEY {
                continue;
            }
            let Some(snapshot) = planned_row_snapshot(row)? else {
                continue;
            };
            rows.push(ConstraintCandidateRow {
                index: next_index,
                identity: ConstraintRowIdentity {
                    entity_id: row.entity_id.clone(),
                    schema_key: row.schema_key.clone(),
                    file_id: planned_row_required_text(row, "file_id")?,
                    version_id: planned_row_required_text(row, "version_id")?,
                },
                schema_version: planned_row_required_text(row, "schema_version")?,
                snapshot,
                storage,
                shadows_committed_identity,
            });
            next_index += 1;
        }
    }

    Ok(rows)
}

fn collect_sql2_delete_candidates(
    resolved: &ResolvedWritePlan,
) -> Result<Vec<ConstraintDeletedRow>, LixError> {
    let mut rows = Vec::new();

    for partition in &resolved.partitions {
        let storage = storage_kind_for_write_mode(partition.execution_mode);
        for row in &partition.intended_post_state {
            if !row.tombstone || row.schema_key == REGISTERED_SCHEMA_KEY {
                continue;
            }
            let Some(snapshot) = planned_row_snapshot(row)? else {
                continue;
            };
            rows.push(ConstraintDeletedRow {
                identity: ConstraintRowIdentity {
                    entity_id: row.entity_id.clone(),
                    schema_key: row.schema_key.clone(),
                    file_id: planned_row_required_text(row, "file_id")?,
                    version_id: planned_row_required_text(row, "version_id")?,
                },
                schema_version: planned_row_required_text(row, "schema_version")?,
                snapshot,
                storage,
            });
        }
    }

    Ok(rows)
}

fn collapse_shadowing_constraint_candidates(
    rows: Vec<ConstraintCandidateRow>,
) -> Vec<ConstraintCandidateRow> {
    let mut last_visible_index = HashMap::new();
    for (index, row) in rows.iter().enumerate() {
        last_visible_index.insert((row.storage, row.identity.clone()), index);
    }

    rows.into_iter()
        .enumerate()
        .filter_map(|(index, mut row)| {
            (last_visible_index.get(&(row.storage, row.identity.clone())) == Some(&index)).then(
                || {
                    row.index = index;
                    row
                },
            )
        })
        .collect()
}

fn storage_kind_for_write_mode(mode: WriteMode) -> ConstraintStorageKind {
    match mode {
        WriteMode::Tracked => ConstraintStorageKind::Tracked,
        WriteMode::Untracked => ConstraintStorageKind::Untracked,
    }
}

fn storage_kind_for_table(table: &str) -> ConstraintStorageKind {
    if table.eq_ignore_ascii_case("lix_internal_live_untracked_v1") {
        ConstraintStorageKind::Untracked
    } else {
        ConstraintStorageKind::Tracked
    }
}

async fn validate_snapshot_content<P: SchemaProvider + ?Sized>(
    provider: &mut P,
    cache: &SchemaCache,
    key: &SchemaKey,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    let compiled = load_compiled_schema(provider, cache, key).await?;
    let details = match compiled.validate(snapshot) {
        Ok(()) => None,
        Err(errors) => {
            let mut parts = Vec::new();
            for error in errors {
                let path = error.instance_path.to_string();
                let message = error.to_string();
                if path.is_empty() {
                    parts.push(message);
                } else {
                    parts.push(format!("{path} {message}"));
                }
            }
            Some(parts.join("; "))
        }
    };

    if let Some(details) = details {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "snapshot_content does not match schema '{}' ({}): {details}",
                key.schema_key, key.schema_version
            ),
        });
    }

    Ok(())
}

async fn validate_sql2_update_is_mutable(
    provider: &mut OverlaySchemaProvider<'_>,
    row: &PlannedStateRow,
) -> Result<(), LixError> {
    let key = SchemaKey::new(
        row.schema_key.clone(),
        planned_row_required_text(row, "schema_version")?,
    );
    let schema = provider.load_schema(&key).await?;

    if schema.get("x-lix-immutable").and_then(|v| v.as_bool()) == Some(true) {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "Schema '{}' is immutable and cannot be updated.",
                row.schema_key
            ),
        });
    }

    Ok(())
}

async fn validate_sql2_planned_row(
    backend: &dyn LixBackend,
    provider: &mut OverlaySchemaProvider<'_>,
    cache: &SchemaCache,
    operation_kind: WriteOperationKind,
    row: &PlannedStateRow,
    require_binary_blob_ref_cas: bool,
) -> Result<(), LixError> {
    if row.tombstone {
        return Ok(());
    }

    let Some(snapshot) = planned_row_snapshot(row)? else {
        return Ok(());
    };

    if row.schema_key == REGISTERED_SCHEMA_KEY {
        validate_registered_schema_snapshot(provider, &snapshot).await?;
        let (key, schema) = schema_from_registered_snapshot(&snapshot)?;
        let expected_entity_id = key.entity_id();
        let actual_version_id = planned_row_required_text(row, "version_id")?;
        let actual_file_id = planned_row_required_text(row, "file_id")?;
        let actual_plugin_key = planned_row_required_text(row, "plugin_key")?;
        let actual_schema_version = planned_row_required_text(row, "schema_version")?;

        if row.entity_id != expected_entity_id {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "registered schema entity_id '{}' must match '{}'",
                    row.entity_id, expected_entity_id
                ),
            });
        }
        if actual_version_id != REGISTERED_SCHEMA_VERSION_ID {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "registered schema version_id '{}' must be '{}'",
                    actual_version_id, REGISTERED_SCHEMA_VERSION_ID
                ),
            });
        }
        if actual_file_id != REGISTERED_SCHEMA_FILE_ID {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "registered schema file_id '{}' must be '{}'",
                    actual_file_id, REGISTERED_SCHEMA_FILE_ID
                ),
            });
        }
        if actual_plugin_key != REGISTERED_SCHEMA_PLUGIN_KEY {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "registered schema plugin_key '{}' must be '{}'",
                    actual_plugin_key, REGISTERED_SCHEMA_PLUGIN_KEY
                ),
            });
        }
        if actual_schema_version != key.schema_version {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "registered schema row schema_version '{}' must match '{}'",
                    actual_schema_version, key.schema_version
                ),
            });
        }
        provider.remember_pending_schema(key, schema);
        return Ok(());
    }

    let key = SchemaKey::new(
        row.schema_key.clone(),
        planned_row_required_text(row, "schema_version")?,
    );
    validate_snapshot_content(provider, cache, &key, &snapshot).await?;
    validate_entity_id_matches_primary_key(provider, &key, &row.entity_id, &snapshot).await?;

    let _ = operation_kind;
    validate_filesystem_snapshot_integrity(
        backend,
        &row.schema_key,
        &snapshot,
        require_binary_blob_ref_cas,
    )
    .await?;

    Ok(())
}

async fn validate_filesystem_insert_integrity(
    backend: &dyn LixBackend,
    row: &MutationRow,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    validate_filesystem_snapshot_integrity(backend, &row.schema_key, snapshot, true).await
}

async fn binary_cas_blob_exists(
    backend: &dyn LixBackend,
    blob_hash: &str,
) -> Result<bool, LixError> {
    let result = backend
        .execute(
            "SELECT 1 \
             FROM lix_internal_binary_blob_store bs \
             JOIN lix_internal_binary_blob_manifest bm ON bm.blob_hash = bs.blob_hash \
             WHERE bs.blob_hash = $1 \
             LIMIT 1",
            &[Value::Text(blob_hash.to_string())],
        )
        .await?;
    Ok(!result.rows.is_empty())
}

fn extract_registered_schema_value(snapshot: &JsonValue) -> Result<&JsonValue, LixError> {
    snapshot.get("value").ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "registered schema snapshot_content missing value".to_string(),
    })
}

async fn validate_registered_schema_snapshot<P: SchemaProvider + ?Sized>(
    provider: &mut P,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    let schema_value = extract_registered_schema_value(snapshot)?;
    validate_lix_schema_definition(schema_value)?;
    validate_foreign_key_reference_targets(provider, schema_value).await?;
    Ok(())
}

async fn validate_filesystem_snapshot_integrity(
    backend: &dyn LixBackend,
    schema_key: &str,
    snapshot: &JsonValue,
    require_binary_blob_ref_cas: bool,
) -> Result<(), LixError> {
    if schema_key != BINARY_BLOB_REF_SCHEMA_KEY {
        return Ok(());
    }

    let blob_hash = snapshot
        .get("blob_hash")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description:
                "lix_binary_blob_ref integrity violation: snapshot_content missing blob_hash"
                    .to_string(),
        })?;

    if require_binary_blob_ref_cas && !binary_cas_blob_exists(backend, blob_hash).await? {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "lix_binary_blob_ref integrity violation: blob_hash '{}' is missing from binary CAS",
                blob_hash
            ),
        });
    }

    Ok(())
}

async fn validate_registered_schema_insert<P: SchemaProvider + ?Sized>(
    provider: &mut P,
    row: &MutationRow,
) -> Result<(), LixError> {
    let snapshot = row.snapshot_content.as_ref().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "registered schema insert requires snapshot_content".to_string(),
    })?;
    validate_registered_schema_snapshot(provider, snapshot).await?;

    Ok(())
}

async fn validate_foreign_key_reference_targets<P: SchemaProvider + ?Sized>(
    provider: &mut P,
    schema: &JsonValue,
) -> Result<(), LixError> {
    let Some(foreign_keys) = schema.get("x-lix-foreign-keys").and_then(|v| v.as_array()) else {
        return Ok(());
    };

    for (index, foreign_key) in foreign_keys.iter().enumerate() {
        let references = foreign_key
            .get("references")
            .and_then(|v| v.as_object())
            .ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "foreign key at index {index} missing references object in schema definition"
                ),
            })?;
        let referenced_key = references
            .get("schemaKey")
            .and_then(|v| v.as_str())
            .ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "foreign key at index {index} references.schemaKey must be a string"
                ),
            })?;
        let referenced_properties = references
            .get("properties")
            .and_then(|v| v.as_array())
            .ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "foreign key at index {index} references.properties must be an array"
                ),
            })?;

        let referenced_properties: Vec<String> = referenced_properties
            .iter()
            .filter_map(|value| value.as_str())
            .map(|value| value.to_string())
            .collect();

        let referenced_schema = provider.load_latest_schema(referenced_key).await?;
        let allowed_keys = collect_unique_key_groups(&referenced_schema);
        if !allowed_keys
            .iter()
            .any(|group| group == &referenced_properties)
        {
            return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                    "foreign key at index {index} references properties that are not a primary key or unique key on schema '{}'",
                    referenced_key
                ),
            });
        }
    }

    Ok(())
}

fn collect_unique_key_groups(schema: &JsonValue) -> Vec<Vec<String>> {
    let mut keys = Vec::new();
    if let Some(primary) = schema
        .get("x-lix-primary-key")
        .and_then(|value| value.as_array())
    {
        let group: Vec<String> = primary
            .iter()
            .filter_map(|value| value.as_str())
            .map(|value| value.to_string())
            .collect();
        if !group.is_empty() {
            keys.push(group);
        }
    }
    if let Some(unique_groups) = schema
        .get("x-lix-unique")
        .and_then(|value| value.as_array())
    {
        for group in unique_groups {
            let Some(group_values) = group.as_array() else {
                continue;
            };
            let group_values: Vec<String> = group_values
                .iter()
                .filter_map(|value| value.as_str())
                .map(|value| value.to_string())
                .collect();
            if !group_values.is_empty() {
                keys.push(group_values);
            }
        }
    }
    keys
}

async fn validate_row_constraints<P: SchemaProvider + ?Sized>(
    backend: &dyn LixBackend,
    provider: &mut P,
    context: &mut ConstraintContext,
    row: &ConstraintCandidateRow,
) -> Result<(), LixError> {
    let key = SchemaKey::new(row.identity.schema_key.clone(), row.schema_version.clone());
    let schema = provider.load_schema(&key).await?;

    validate_primary_and_unique_constraints(backend, context, row, &schema).await?;
    validate_foreign_key_constraints(backend, context, row, &schema).await?;

    Ok(())
}

async fn validate_primary_and_unique_constraints(
    backend: &dyn LixBackend,
    context: &mut ConstraintContext,
    row: &ConstraintCandidateRow,
    schema: &JsonValue,
) -> Result<(), LixError> {
    let candidate_view = ConstraintRowView {
        identity: &row.identity,
        schema_version: &row.schema_version,
        snapshot: &row.snapshot,
    };

    if let Some(primary_key) = schema
        .get("x-lix-primary-key")
        .and_then(JsonValue::as_array)
    {
        let pointers = json_pointer_group(primary_key, "x-lix-primary-key")?;
        if !pointers.is_empty() {
            validate_constraint_group_conflict(
                backend,
                context,
                row,
                &candidate_view,
                &pointers,
                "primary key",
            )
            .await?;
        }
    }

    if let Some(unique_groups) = schema.get("x-lix-unique").and_then(JsonValue::as_array) {
        for group in unique_groups {
            let Some(group_values) = group.as_array() else {
                continue;
            };
            let pointers = json_pointer_group(group_values, "x-lix-unique")?;
            if pointers.is_empty() {
                continue;
            }
            validate_constraint_group_conflict(
                backend,
                context,
                row,
                &candidate_view,
                &pointers,
                "unique constraint",
            )
            .await?;
        }
    }

    Ok(())
}

async fn validate_constraint_group_conflict(
    backend: &dyn LixBackend,
    context: &mut ConstraintContext,
    row: &ConstraintCandidateRow,
    candidate_view: &ConstraintRowView<'_>,
    pointers: &[String],
    label: &str,
) -> Result<(), LixError> {
    let Some(candidate_tuple) = extract_pointer_tuple(candidate_view, pointers)? else {
        return Ok(());
    };

    for pending in context.pending_rows.iter().filter(|pending| {
        pending.index != row.index
            && pending.storage == row.storage
            && pending.identity.schema_key == row.identity.schema_key
            && pending.identity.version_id == row.identity.version_id
            && pending.identity.file_id == row.identity.file_id
            && pending_row_is_visible(context, pending)
    }) {
        let pending_view = ConstraintRowView {
            identity: &pending.identity,
            schema_version: &pending.schema_version,
            snapshot: &pending.snapshot,
        };
        let Some(other_tuple) = extract_pointer_tuple(&pending_view, pointers)? else {
            continue;
        };
        if other_tuple == candidate_tuple {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "{label} violation for schema '{}': entity '{}' conflicts with pending row '{}' in version '{}' and file '{}'",
                    row.identity.schema_key,
                    row.identity.entity_id,
                    pending.identity.entity_id,
                    row.identity.version_id,
                    row.identity.file_id
                ),
            ));
        }
    }

    let scope = ConstraintScopeKey {
        storage: row.storage,
        schema_key: row.identity.schema_key.clone(),
        file_id: row.identity.file_id.clone(),
        version_id: row.identity.version_id.clone(),
    };
    let shadowed_identities = shadowed_committed_identities(context, row.storage);
    let committed_rows = load_committed_scope_rows(backend, context, &scope).await?;

    for committed in committed_rows.iter().filter(|committed| {
        (!row.shadows_committed_identity || committed.identity != row.identity)
            && !shadowed_identities.contains(&committed.identity)
    }) {
        let committed_view = ConstraintRowView {
            identity: &committed.identity,
            schema_version: &committed.schema_version,
            snapshot: &committed.snapshot,
        };
        let Some(other_tuple) = extract_pointer_tuple(&committed_view, pointers)? else {
            continue;
        };
        if other_tuple == candidate_tuple {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "{label} violation for schema '{}': entity '{}' conflicts with existing row '{}' in version '{}' and file '{}'",
                    row.identity.schema_key,
                    row.identity.entity_id,
                    committed.identity.entity_id,
                    row.identity.version_id,
                    row.identity.file_id
                ),
            ));
        }
    }

    Ok(())
}

async fn validate_foreign_key_constraints(
    backend: &dyn LixBackend,
    context: &mut ConstraintContext,
    row: &ConstraintCandidateRow,
    schema: &JsonValue,
) -> Result<(), LixError> {
    let Some(foreign_keys) = schema
        .get("x-lix-foreign-keys")
        .and_then(JsonValue::as_array)
    else {
        return Ok(());
    };

    let candidate_view = ConstraintRowView {
        identity: &row.identity,
        schema_version: &row.schema_version,
        snapshot: &row.snapshot,
    };

    for (index, foreign_key) in foreign_keys.iter().enumerate() {
        let local_properties = foreign_key
            .get("properties")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "foreign key at index {index} missing properties array in schema '{}'",
                        row.identity.schema_key
                    ),
                )
            })?;
        let local_properties =
            json_pointer_group(local_properties, "x-lix-foreign-keys.properties")?;

        let references = foreign_key
            .get("references")
            .and_then(JsonValue::as_object)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "foreign key at index {index} missing references object in schema '{}'",
                        row.identity.schema_key
                    ),
                )
            })?;
        let referenced_schema_key = references
            .get("schemaKey")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "foreign key at index {index} references.schemaKey must be a string in schema '{}'",
                    row.identity.schema_key
                ),
            ))?;
        let referenced_properties = references
            .get("properties")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "foreign key at index {index} references.properties must be an array in schema '{}'",
                    row.identity.schema_key
                ),
            ))?;
        let referenced_properties = json_pointer_group(
            referenced_properties,
            "x-lix-foreign-keys.references.properties",
        )?;

        if local_properties.len() != referenced_properties.len() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "foreign key at index {index} in schema '{}' must have the same number of local and referenced properties",
                    row.identity.schema_key
                ),
            ));
        }

        let Some(local_values) = extract_pointer_tuple(&candidate_view, &local_properties)? else {
            continue;
        };

        let target_schema_key = effective_foreign_key_target_schema_key(
            referenced_schema_key,
            &referenced_properties,
            &local_values,
            index,
            &row.identity.schema_key,
        )?;
        let target_file_id = effective_foreign_key_target_file_id(
            &row.identity.file_id,
            &referenced_properties,
            &local_values,
        )?;
        let target_scope = ConstraintScopeKey {
            storage: row.storage,
            schema_key: target_schema_key.clone(),
            file_id: target_file_id.clone(),
            version_id: row.identity.version_id.clone(),
        };

        if foreign_key_target_exists(
            backend,
            context,
            &target_scope,
            &referenced_properties,
            &local_values,
        )
        .await?
        {
            continue;
        }

        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "foreign key violation for schema '{}': no row in schema '{}' matches constraint {} in version '{}' and file '{}'",
                row.identity.schema_key,
                target_schema_key,
                index,
                row.identity.version_id,
                target_file_id
            ),
        ));
    }

    Ok(())
}

async fn validate_delete_constraints<P: SchemaProvider + ?Sized>(
    backend: &dyn LixBackend,
    provider: &mut P,
    context: &mut ConstraintContext,
    deleted_rows: &[ConstraintDeletedRow],
) -> Result<(), LixError> {
    let source_schemas = provider.load_visible_schema_entries().await?;

    for deleted_row in deleted_rows {
        validate_delete_constraints_for_row(backend, context, deleted_row, &source_schemas).await?;
    }

    Ok(())
}

async fn validate_delete_constraints_for_row(
    backend: &dyn LixBackend,
    context: &mut ConstraintContext,
    deleted_row: &ConstraintDeletedRow,
    source_schemas: &[(SchemaKey, JsonValue)],
) -> Result<(), LixError> {
    let deleted_view = ConstraintRowView {
        identity: &deleted_row.identity,
        schema_version: &deleted_row.schema_version,
        snapshot: &deleted_row.snapshot,
    };

    for (source_key, source_schema) in source_schemas {
        let Some(foreign_keys) = source_schema
            .get("x-lix-foreign-keys")
            .and_then(JsonValue::as_array)
        else {
            continue;
        };

        let source_scope = ConstraintSchemaVersionKey {
            storage: deleted_row.storage,
            schema_key: source_key.schema_key.clone(),
            version_id: deleted_row.identity.version_id.clone(),
        };

        for (index, foreign_key) in foreign_keys.iter().enumerate() {
            let local_properties = foreign_key
                .get("properties")
                .and_then(JsonValue::as_array)
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "foreign key at index {index} missing properties array in schema '{}'",
                            source_key.schema_key
                        ),
                    )
                })?;
            let local_properties =
                json_pointer_group(local_properties, "x-lix-foreign-keys.properties")?;

            let references = foreign_key
                .get("references")
                .and_then(JsonValue::as_object)
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "foreign key at index {index} missing references object in schema '{}'",
                            source_key.schema_key
                        ),
                    )
                })?;
            let referenced_schema_key = references
                .get("schemaKey")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "foreign key at index {index} references.schemaKey must be a string in schema '{}'",
                            source_key.schema_key
                        ),
                    )
                })?;
            let referenced_properties = references
                .get("properties")
                .and_then(JsonValue::as_array)
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "foreign key at index {index} references.properties must be an array in schema '{}'",
                            source_key.schema_key
                        ),
                    )
                })?;
            let referenced_properties = json_pointer_group(
                referenced_properties,
                "x-lix-foreign-keys.references.properties",
            )?;

            if local_properties.len() != referenced_properties.len() {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "foreign key at index {index} in schema '{}' must have the same number of local and referenced properties",
                        source_key.schema_key
                    ),
                ));
            }

            let Some(target_values) = extract_pointer_tuple(&deleted_view, &referenced_properties)?
            else {
                continue;
            };

            if delete_has_referencing_row(
                backend,
                context,
                deleted_row,
                source_key,
                &source_scope,
                &source_key.schema_key,
                referenced_schema_key,
                &local_properties,
                &referenced_properties,
                &target_values,
                index,
            )
            .await?
            {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "foreign key restrict violation for schema '{}': entity '{}' is still referenced in version '{}' and file '{}'",
                        deleted_row.identity.schema_key,
                        deleted_row.identity.entity_id,
                        deleted_row.identity.version_id,
                        deleted_row.identity.file_id
                    ),
                ));
            }
        }
    }

    Ok(())
}

async fn delete_has_referencing_row(
    backend: &dyn LixBackend,
    context: &mut ConstraintContext,
    deleted_row: &ConstraintDeletedRow,
    source_key: &SchemaKey,
    source_scope: &ConstraintSchemaVersionKey,
    source_schema_key: &str,
    referenced_schema_key: &str,
    local_properties: &[String],
    referenced_properties: &[String],
    target_values: &[JsonValue],
    index: usize,
) -> Result<bool, LixError> {
    for pending in context.pending_rows.iter().filter(|pending| {
        pending.storage == deleted_row.storage
            && pending.identity.schema_key == source_scope.schema_key
            && pending.identity.version_id == source_scope.version_id
            && pending.schema_version == source_key.schema_version
            && pending_row_is_visible(context, pending)
    }) {
        let pending_view = ConstraintRowView {
            identity: &pending.identity,
            schema_version: &pending.schema_version,
            snapshot: &pending.snapshot,
        };
        if row_references_deleted_target(
            &pending_view,
            deleted_row,
            source_schema_key,
            referenced_schema_key,
            local_properties,
            referenced_properties,
            target_values,
            index,
        )? {
            return Ok(true);
        }
    }

    let shadowed_identities = shadowed_committed_identities(context, deleted_row.storage);
    let committed_rows = load_committed_schema_version_rows(backend, context, source_scope).await?;
    for committed in committed_rows.iter().filter(|committed| {
        committed.schema_version == source_key.schema_version
            && !shadowed_identities.contains(&committed.identity)
    }) {
        let committed_view = ConstraintRowView {
            identity: &committed.identity,
            schema_version: &committed.schema_version,
            snapshot: &committed.snapshot,
        };
        if row_references_deleted_target(
            &committed_view,
            deleted_row,
            source_schema_key,
            referenced_schema_key,
            local_properties,
            referenced_properties,
            target_values,
            index,
        )? {
            return Ok(true);
        }
    }

    Ok(false)
}

fn row_references_deleted_target(
    source_row: &ConstraintRowView<'_>,
    deleted_row: &ConstraintDeletedRow,
    source_schema_key: &str,
    referenced_schema_key: &str,
    local_properties: &[String],
    referenced_properties: &[String],
    target_values: &[JsonValue],
    index: usize,
) -> Result<bool, LixError> {
    let Some(local_values) = extract_pointer_tuple(source_row, local_properties)? else {
        return Ok(false);
    };

    let effective_target_schema_key = effective_foreign_key_target_schema_key(
        referenced_schema_key,
        referenced_properties,
        &local_values,
        index,
        source_schema_key,
    )?;
    if effective_target_schema_key != deleted_row.identity.schema_key {
        return Ok(false);
    }

    let effective_target_file_id = effective_foreign_key_target_file_id(
        &source_row.identity.file_id,
        referenced_properties,
        &local_values,
    )?;
    if effective_target_file_id != deleted_row.identity.file_id {
        return Ok(false);
    }

    Ok(local_values == target_values)
}

async fn foreign_key_target_exists(
    backend: &dyn LixBackend,
    context: &mut ConstraintContext,
    target_scope: &ConstraintScopeKey,
    referenced_properties: &[String],
    local_values: &[JsonValue],
) -> Result<bool, LixError> {
    let mut storage_kinds = vec![target_scope.storage];
    if target_scope.storage == ConstraintStorageKind::Untracked {
        storage_kinds.push(ConstraintStorageKind::Tracked);
    }

    for storage in storage_kinds {
        let scope = ConstraintScopeKey {
            storage,
            schema_key: target_scope.schema_key.clone(),
            file_id: target_scope.file_id.clone(),
            version_id: target_scope.version_id.clone(),
        };

        if foreign_key_target_exists_in_storage(
            backend,
            context,
            &scope,
            referenced_properties,
            local_values,
        )
        .await?
        {
            return Ok(true);
        }
    }

    Ok(false)
}

async fn foreign_key_target_exists_in_storage(
    backend: &dyn LixBackend,
    context: &mut ConstraintContext,
    target_scope: &ConstraintScopeKey,
    referenced_properties: &[String],
    local_values: &[JsonValue],
) -> Result<bool, LixError> {
    for pending in context.pending_rows.iter().filter(|pending| {
        pending.storage == target_scope.storage
            && pending.identity.schema_key == target_scope.schema_key
            && pending.identity.version_id == target_scope.version_id
            && pending.identity.file_id == target_scope.file_id
            && pending_row_is_visible(context, pending)
    }) {
        let pending_view = ConstraintRowView {
            identity: &pending.identity,
            schema_version: &pending.schema_version,
            snapshot: &pending.snapshot,
        };
        let Some(target_values) = extract_pointer_tuple(&pending_view, referenced_properties)?
        else {
            continue;
        };
        if target_values == local_values {
            return Ok(true);
        }
    }

    let shadowed_identities = shadowed_committed_identities(context, target_scope.storage);
    let committed_rows = load_committed_scope_rows(backend, context, target_scope).await?;
    for committed in committed_rows
        .iter()
        .filter(|committed| !shadowed_identities.contains(&committed.identity))
    {
        let committed_view = ConstraintRowView {
            identity: &committed.identity,
            schema_version: &committed.schema_version,
            snapshot: &committed.snapshot,
        };
        let Some(target_values) = extract_pointer_tuple(&committed_view, referenced_properties)?
        else {
            continue;
        };
        if target_values == local_values {
            return Ok(true);
        }
    }

    Ok(false)
}

async fn load_committed_scope_rows<'a>(
    backend: &dyn LixBackend,
    context: &'a mut ConstraintContext,
    scope: &ConstraintScopeKey,
) -> Result<&'a Vec<ConstraintCommittedRow>, LixError> {
    if !context.committed_rows.contains_key(scope) {
        let rows = query_committed_scope_rows(backend, scope).await?;
        context.committed_rows.insert(scope.clone(), rows);
    }
    Ok(context
        .committed_rows
        .get(scope)
        .expect("constraint scope cache should be populated"))
}

async fn load_committed_schema_version_rows<'a>(
    backend: &dyn LixBackend,
    context: &'a mut ConstraintContext,
    scope: &ConstraintSchemaVersionKey,
) -> Result<&'a Vec<ConstraintCommittedRow>, LixError> {
    if !context.committed_schema_version_rows.contains_key(scope) {
        let rows = query_committed_schema_version_rows(backend, scope).await?;
        context
            .committed_schema_version_rows
            .insert(scope.clone(), rows);
    }
    Ok(context
        .committed_schema_version_rows
        .get(scope)
        .expect("constraint schema-version cache should be populated"))
}

async fn query_committed_scope_rows(
    backend: &dyn LixBackend,
    scope: &ConstraintScopeKey,
) -> Result<Vec<ConstraintCommittedRow>, LixError> {
    let rows = match scope.storage {
        ConstraintStorageKind::Tracked => {
            let table_name = live_table_name(&scope.schema_key);
            if !relation_exists(backend, &table_name).await? {
                return Ok(Vec::new());
            }
            let sql = format!(
                "SELECT entity_id, schema_version, snapshot_content \
                 FROM {} \
                 WHERE version_id = $1 \
                   AND file_id = $2 \
                   AND is_tombstone = 0 \
                   AND snapshot_content IS NOT NULL",
                quote_sql_ident(&table_name)
            );
            backend
                .execute(
                    &sql,
                    &[
                        Value::Text(scope.version_id.clone()),
                        Value::Text(scope.file_id.clone()),
                    ],
                )
                .await?
                .rows
        }
        ConstraintStorageKind::Untracked => {
            backend
                .execute(
                    "SELECT entity_id, schema_version, snapshot_content \
                 FROM lix_internal_live_untracked_v1 \
                 WHERE schema_key = $1 \
                   AND version_id = $2 \
                   AND file_id = $3 \
                   AND snapshot_content IS NOT NULL",
                    &[
                        Value::Text(scope.schema_key.clone()),
                        Value::Text(scope.version_id.clone()),
                        Value::Text(scope.file_id.clone()),
                    ],
                )
                .await?
                .rows
        }
    };

    rows.into_iter()
        .map(|row| {
            let entity_id = value_to_string(&row[0], "entity_id")?;
            let schema_version = value_to_string(&row[1], "schema_version")?;
            let snapshot_raw = value_to_string(&row[2], "snapshot_content")?;
            let snapshot = serde_json::from_str::<JsonValue>(&snapshot_raw).map_err(|err| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "stored snapshot_content for schema '{}' is invalid JSON: {err}",
                        scope.schema_key
                    ),
                )
            })?;

            Ok(ConstraintCommittedRow {
                identity: ConstraintRowIdentity {
                    entity_id,
                    schema_key: scope.schema_key.clone(),
                    file_id: scope.file_id.clone(),
                    version_id: scope.version_id.clone(),
                },
                schema_version,
                snapshot,
            })
        })
        .collect()
}

async fn query_committed_schema_version_rows(
    backend: &dyn LixBackend,
    scope: &ConstraintSchemaVersionKey,
) -> Result<Vec<ConstraintCommittedRow>, LixError> {
    let rows = match scope.storage {
        ConstraintStorageKind::Tracked => {
            let table_name = live_table_name(&scope.schema_key);
            if !relation_exists(backend, &table_name).await? {
                return Ok(Vec::new());
            }
            let sql = format!(
                "SELECT entity_id, file_id, schema_version, snapshot_content \
                 FROM {} \
                 WHERE version_id = $1 \
                   AND is_tombstone = 0 \
                   AND snapshot_content IS NOT NULL",
                quote_sql_ident(&table_name)
            );
            backend
                .execute(&sql, &[Value::Text(scope.version_id.clone())])
                .await?
                .rows
        }
        ConstraintStorageKind::Untracked => {
            backend
                .execute(
                    "SELECT entity_id, file_id, schema_version, snapshot_content \
                     FROM lix_internal_live_untracked_v1 \
                     WHERE schema_key = $1 \
                       AND version_id = $2 \
                       AND snapshot_content IS NOT NULL",
                    &[
                        Value::Text(scope.schema_key.clone()),
                        Value::Text(scope.version_id.clone()),
                    ],
                )
                .await?
                .rows
        }
    };

    rows.into_iter()
        .map(|row| {
            let entity_id = value_to_string(&row[0], "entity_id")?;
            let file_id = value_to_string(&row[1], "file_id")?;
            let schema_version = value_to_string(&row[2], "schema_version")?;
            let snapshot_raw = value_to_string(&row[3], "snapshot_content")?;
            let snapshot = serde_json::from_str::<JsonValue>(&snapshot_raw).map_err(|err| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "stored snapshot_content for schema '{}' is invalid JSON: {err}",
                        scope.schema_key
                    ),
                )
            })?;

            Ok(ConstraintCommittedRow {
                identity: ConstraintRowIdentity {
                    entity_id,
                    schema_key: scope.schema_key.clone(),
                    file_id,
                    version_id: scope.version_id.clone(),
                },
                schema_version,
                snapshot,
            })
        })
        .collect()
}

async fn relation_exists(backend: &dyn LixBackend, relation_name: &str) -> Result<bool, LixError> {
    let result = match backend.dialect() {
        SqlDialect::Sqlite => {
            backend
                .execute(
                    "SELECT 1 \
                     FROM sqlite_master \
                     WHERE name = $1 \
                       AND type IN ('table', 'view') \
                     LIMIT 1",
                    &[Value::Text(relation_name.to_string())],
                )
                .await?
        }
        SqlDialect::Postgres => {
            backend
                .execute(
                    "SELECT 1 \
                     FROM pg_catalog.pg_class c \
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = current_schema() \
                       AND c.relname = $1 \
                     LIMIT 1",
                    &[Value::Text(relation_name.to_string())],
                )
                .await?
        }
    };

    Ok(!result.rows.is_empty())
}

fn extract_pointer_tuple(
    row: &ConstraintRowView<'_>,
    pointers: &[String],
) -> Result<Option<Vec<JsonValue>>, LixError> {
    let mut values = Vec::with_capacity(pointers.len());
    for pointer in pointers {
        let Some(value) = constraint_pointer_value(row, pointer)? else {
            return Ok(None);
        };
        if value.is_null() {
            return Ok(None);
        }
        values.push(value);
    }
    Ok(Some(values))
}

fn constraint_pointer_value(
    row: &ConstraintRowView<'_>,
    pointer: &str,
) -> Result<Option<JsonValue>, LixError> {
    let path = parse_json_pointer(pointer)?;
    if let Some(value) = json_pointer_get(row.snapshot, &path) {
        return Ok(Some(value.clone()));
    }

    if path.len() != 1 {
        return Ok(None);
    }

    let value = match path[0].as_str() {
        "entity_id" => Some(JsonValue::String(row.identity.entity_id.clone())),
        "schema_key" => Some(JsonValue::String(row.identity.schema_key.clone())),
        "file_id" => Some(JsonValue::String(row.identity.file_id.clone())),
        "version_id" => Some(JsonValue::String(row.identity.version_id.clone())),
        "schema_version" => Some(JsonValue::String(row.schema_version.to_string())),
        _ => None,
    };

    Ok(value)
}

fn effective_foreign_key_target_schema_key(
    referenced_schema_key: &str,
    referenced_properties: &[String],
    local_values: &[JsonValue],
    index: usize,
    source_schema_key: &str,
) -> Result<String, LixError> {
    if referenced_schema_key != "lix_state" {
        return Ok(referenced_schema_key.to_string());
    }

    let Some(schema_key_position) = referenced_properties
        .iter()
        .position(|pointer| pointer == "/schema_key")
    else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "foreign key at index {index} in schema '{}' references lix_state and must include '/schema_key' in references.properties",
                source_schema_key
            ),
        ));
    };

    json_value_to_string(
        &local_values[schema_key_position],
        "foreign key target schema_key",
    )
}

fn effective_foreign_key_target_file_id(
    source_file_id: &str,
    referenced_properties: &[String],
    local_values: &[JsonValue],
) -> Result<String, LixError> {
    match referenced_properties
        .iter()
        .position(|pointer| pointer == "/file_id")
    {
        Some(position) => {
            json_value_to_string(&local_values[position], "foreign key target file_id")
        }
        None => Ok(source_file_id.to_string()),
    }
}

fn json_pointer_group(values: &[JsonValue], label: &str) -> Result<Vec<String>, LixError> {
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(|value| value.to_string())
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("{label} entries must be strings"),
                    )
                })
        })
        .collect()
}

fn json_value_to_string(value: &JsonValue, label: &str) -> Result<String, LixError> {
    match value {
        JsonValue::String(text) => Ok(text.clone()),
        other => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{label} must be a string, got {other}"),
        )),
    }
}

fn pending_row_is_visible(context: &ConstraintContext, pending: &ConstraintCandidateRow) -> bool {
    !context
        .deleted_identities
        .contains(&(pending.storage, pending.identity.clone()))
}

fn shadowed_committed_identities(
    context: &ConstraintContext,
    storage: ConstraintStorageKind,
) -> HashSet<ConstraintRowIdentity> {
    let mut identities = context
        .pending_rows
        .iter()
        .filter(|row| row.storage == storage && row.shadows_committed_identity)
        .map(|row| row.identity.clone())
        .collect::<HashSet<_>>();
    identities.extend(
        context
            .deleted_rows
            .iter()
            .filter(|row| row.storage == storage)
            .map(|row| row.identity.clone()),
    );
    identities
}

fn live_table_name(schema_key: &str) -> String {
    format!("lix_internal_live_v1_{schema_key}")
}

fn quote_sql_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

async fn load_compiled_schema<P: SchemaProvider + ?Sized>(
    provider: &mut P,
    cache: &SchemaCache,
    key: &SchemaKey,
) -> Result<Arc<JSONSchema>, LixError> {
    if let Some(existing) = cache.inner.read().unwrap().get(key) {
        return Ok(existing.clone());
    }

    let schema = provider.load_schema(key).await?;
    let compiled = JSONSchema::compile(&schema).map_err(|err| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "failed to compile schema '{}' ({}): {err}",
            key.schema_key, key.schema_version
        ),
    })?;
    let compiled = Arc::new(compiled);

    cache
        .inner
        .write()
        .unwrap()
        .insert(key.clone(), compiled.clone());

    Ok(compiled)
}

fn value_to_string(value: &Value, name: &str) -> Result<String, LixError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("expected text value for {name}"),
        }),
    }
}

fn planned_row_required_text(row: &PlannedStateRow, name: &str) -> Result<String, LixError> {
    let value = match name {
        "entity_id" => Some(row.entity_id.clone()),
        "schema_key" => Some(row.schema_key.clone()),
        "version_id" => row
            .version_id
            .clone()
            .or_else(|| row.values.get(name).and_then(planned_row_text_value)),
        _ => row.values.get(name).and_then(planned_row_text_value),
    };

    value.ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("sql2 validation requires text-compatible '{name}'"),
    })
}

fn planned_row_snapshot(row: &PlannedStateRow) -> Result<Option<JsonValue>, LixError> {
    let Some(value) = row.values.get("snapshot_content") else {
        return Ok(None);
    };

    match value {
        Value::Null => Ok(None),
        Value::Json(json) => Ok(Some(json.clone())),
        Value::Text(text) => serde_json::from_str::<JsonValue>(text)
            .map(Some)
            .map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "snapshot_content for schema '{}' is not valid JSON during sql2 validation: {err}",
                    row.schema_key
                ),
            }),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "snapshot_content for schema '{}' must be JSON, text, or null during sql2 validation, got {other:?}",
                row.schema_key
            ),
        }),
    }
}

fn planned_row_text_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(value.to_string()),
        Value::Real(value) => Some(value.to_string()),
        _ => None,
    }
}

fn resolve_update_snapshot(
    plan: &UpdateValidationPlan,
    row_snapshot_value: Option<&Value>,
    schema_key: &str,
) -> Result<Option<JsonValue>, LixError> {
    if let Some(snapshot) = plan.snapshot_content.as_ref() {
        return Ok(Some(snapshot.clone()));
    }
    let Some(patch) = plan.snapshot_patch.as_ref() else {
        return Ok(None);
    };
    let mut base = parse_row_snapshot_content(row_snapshot_value, schema_key)?;
    apply_snapshot_patch(&mut base, patch, schema_key)?;
    Ok(Some(base))
}

fn parse_row_snapshot_content(
    value: Option<&Value>,
    schema_key: &str,
) -> Result<JsonValue, LixError> {
    match value {
        None | Some(Value::Null) => Ok(JsonValue::Object(JsonMap::new())),
        Some(Value::Text(text)) => serde_json::from_str::<JsonValue>(text).map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                "snapshot_content for schema '{}' is not valid JSON during update validation: {err}",
                schema_key
            ),
        }),
        Some(other) => Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                "snapshot_content for schema '{}' must be text or null during update validation, got {other:?}",
                schema_key
            ),
        }),
    }
}

fn apply_snapshot_patch(
    snapshot: &mut JsonValue,
    patch: &BTreeMap<String, JsonValue>,
    schema_key: &str,
) -> Result<(), LixError> {
    let object = snapshot.as_object_mut().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "snapshot_content for schema '{}' must be a JSON object for property update validation",
            schema_key
        ),
    })?;
    for (property, value) in patch {
        object.insert(property.clone(), value.clone());
    }
    Ok(())
}

async fn validate_entity_id_matches_primary_key<P: SchemaProvider + ?Sized>(
    provider: &mut P,
    key: &SchemaKey,
    entity_id: &str,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    let schema = provider.load_schema(key).await?;
    let Some(primary_key) = schema
        .get("x-lix-primary-key")
        .and_then(JsonValue::as_array)
    else {
        return Ok(());
    };
    if primary_key.is_empty() {
        return Ok(());
    }

    let mut parts = Vec::with_capacity(primary_key.len());
    for pointer_value in primary_key {
        let pointer = pointer_value.as_str().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "schema '{}' ({}) has non-string x-lix-primary-key entry",
                key.schema_key, key.schema_version
            ),
        })?;
        let pointer_path = parse_json_pointer(pointer)?;
        if pointer_path.is_empty() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "schema '{}' ({}) has invalid empty x-lix-primary-key pointer",
                    key.schema_key, key.schema_version
                ),
            });
        }

        let value = json_pointer_get(snapshot, &pointer_path).ok_or_else(|| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                "entity_id '{}' is inconsistent for schema '{}' ({}): missing primary-key field at pointer '{}'",
                entity_id, key.schema_key, key.schema_version, pointer
            ),
        })?;
        parts.push(entity_id_component_from_json_value(value, pointer)?);
    }

    let expected = if parts.len() == 1 {
        parts.pop().unwrap()
    } else {
        parts.join("~")
    };

    if expected != entity_id {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "entity_id '{}' is inconsistent for schema '{}' ({}): expected '{}'",
                entity_id, key.schema_key, key.schema_version, expected
            ),
        });
    }

    Ok(())
}

fn entity_id_component_from_json_value(
    value: &JsonValue,
    pointer: &str,
) -> Result<String, LixError> {
    match value {
        JsonValue::Null => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "cannot derive entity_id from null primary-key value at pointer '{}'",
                pointer
            ),
        }),
        JsonValue::String(text) => Ok(text.clone()),
        JsonValue::Bool(flag) => Ok(flag.to_string()),
        JsonValue::Number(number) => Ok(number.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => Ok(value.to_string()),
    }
}

fn parse_json_pointer(pointer: &str) -> Result<Vec<String>, LixError> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("invalid JSON pointer '{pointer}'"),
        });
    }
    pointer[1..]
        .split('/')
        .map(decode_json_pointer_segment)
        .collect()
}

fn decode_json_pointer_segment(segment: &str) -> Result<String, LixError> {
    let mut out = String::new();
    let mut chars = segment.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') => out.push('~'),
                Some('1') => out.push('/'),
                _ => {
                    return Err(LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: format!("invalid JSON pointer segment '{segment}'"),
                    })
                }
            }
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

fn json_pointer_get<'a>(value: &'a JsonValue, pointer: &[String]) -> Option<&'a JsonValue> {
    let mut current = value;
    for segment in pointer {
        match current {
            JsonValue::Object(object) => {
                current = object.get(segment)?;
            }
            JsonValue::Array(array) => {
                let index = segment.parse::<usize>().ok()?;
                current = array.get(index)?;
            }
            _ => return None,
        }
    }
    Some(current)
}
