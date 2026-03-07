use crate::builtin_schema::builtin_schema_definition;
use crate::schema::{SchemaProvider, SqlStoredSchemaProvider};
use crate::sql2::catalog::SurfaceFamily;
use crate::sql2::planner::ir::{
    MutationPayload, PlannedStateRow, PlannedWrite, ResolvedRowRef, ResolvedWritePlan, RowLineage,
    SchemaProof, ScopeProof, TargetSetProof, WriteLane, WriteMode, WriteOperationKind,
};
use crate::sql2::planner::semantics::effective_state_resolver::{
    resolve_exact_effective_state_row, ExactEffectiveStateRow, ExactEffectiveStateRowRequest,
    OverlayLane,
};
use crate::{LixBackend, Value};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteResolveError {
    pub(crate) message: String,
}

pub(crate) async fn resolve_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let target_write_lane = match planned_write.command.mode {
        WriteMode::Tracked => Some(write_lane_from_scope(&planned_write.scope_proof)?),
        WriteMode::Untracked => None,
    };

    match planned_write.command.target.descriptor.surface_family {
        SurfaceFamily::State => match planned_write.command.operation_kind {
            WriteOperationKind::Insert => {
                resolve_state_insert_write_plan(planned_write, target_write_lane)
            }
            WriteOperationKind::Update | WriteOperationKind::Delete => {
                resolve_existing_state_write(backend, planned_write, target_write_lane).await
            }
        },
        SurfaceFamily::Entity => {
            let mut provider = SqlStoredSchemaProvider::new(backend);
            let entity_schema = load_entity_schema(&mut provider, planned_write)
                .await
                .map_err(write_resolve_backend_error)?;
            match planned_write.command.operation_kind {
                WriteOperationKind::Insert => resolve_entity_insert_write_plan(
                    planned_write,
                    target_write_lane,
                    &entity_schema,
                ),
                WriteOperationKind::Update | WriteOperationKind::Delete => {
                    resolve_existing_entity_write(
                        backend,
                        planned_write,
                        target_write_lane,
                        &entity_schema,
                    )
                    .await
                }
            }
        }
        SurfaceFamily::Filesystem | SurfaceFamily::Admin | SurfaceFamily::Change => {
            Err(WriteResolveError {
                message: format!(
                    "sql2 write resolver does not support '{}' writes",
                    planned_write.command.target.descriptor.public_name
                ),
            })
        }
    }
}

fn resolve_state_insert_write_plan(
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let entity_id = resolved_entity_id(planned_write)?;
    let schema_key = resolved_schema_key(planned_write)?;
    let version_id = resolved_version_id(planned_write)?;

    Ok(ResolvedWritePlan {
        authoritative_pre_state: Vec::new(),
        intended_post_state: vec![PlannedStateRow {
            entity_id: entity_id.clone(),
            schema_key,
            version_id,
            values: payload_map(planned_write)?,
            tombstone: false,
        }],
        tombstones: Vec::new(),
        lineage: vec![RowLineage {
            entity_id,
            source_change_id: None,
            source_commit_id: None,
        }],
        target_write_lane,
    })
}

async fn resolve_existing_state_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    ensure_exact_selector(planned_write)?;
    let schema_key = resolved_schema_key(planned_write)?;
    let version_id = resolved_version_id(planned_write)?.ok_or_else(|| WriteResolveError {
        message: "sql2 existing-row write resolver requires a concrete version_id".to_string(),
    })?;
    let Some(current_row) = resolve_exact_effective_state_row(
        backend,
        &ExactEffectiveStateRowRequest {
            schema_key: schema_key.clone(),
            version_id,
            exact_filters: planned_write.command.selector.exact_filters.clone(),
            include_global_overlay: true,
            include_untracked_overlay: true,
        },
    )
    .await
    .map_err(write_resolve_backend_error)?
    else {
        return Ok(ResolvedWritePlan {
            authoritative_pre_state: Vec::new(),
            intended_post_state: Vec::new(),
            tombstones: Vec::new(),
            lineage: Vec::new(),
            target_write_lane,
        });
    };
    ensure_local_tracked_overlay(&current_row)?;

    let row_ref = ResolvedRowRef {
        entity_id: current_row.entity_id.clone(),
        schema_key: current_row.schema_key.clone(),
        version_id: Some(current_row.version_id.clone()),
        source_change_id: current_row.source_change_id.clone(),
        source_commit_id: None,
    };
    let lineage = vec![RowLineage {
        entity_id: current_row.entity_id.clone(),
        source_change_id: current_row.source_change_id.clone(),
        source_commit_id: None,
    }];

    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let values = merged_update_values(&current_row.values, planned_write)?;
            ensure_identity_columns_preserved(
                &current_row.entity_id,
                &current_row.schema_key,
                &current_row.file_id,
                &current_row.version_id,
                &values,
            )?;

            Ok(ResolvedWritePlan {
                authoritative_pre_state: vec![row_ref],
                intended_post_state: vec![PlannedStateRow {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
                    values,
                    tombstone: false,
                }],
                tombstones: Vec::new(),
                lineage,
                target_write_lane,
            })
        }
        WriteOperationKind::Delete => Ok(ResolvedWritePlan {
            authoritative_pre_state: vec![row_ref.clone()],
            intended_post_state: vec![PlannedStateRow {
                entity_id: current_row.entity_id,
                schema_key: current_row.schema_key,
                version_id: Some(current_row.version_id),
                values: current_row.values,
                tombstone: true,
            }],
            tombstones: vec![row_ref],
            lineage,
            target_write_lane,
        }),
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "sql2 existing-row resolver does not handle inserts".to_string(),
        }),
    }
}

async fn resolve_existing_entity_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
    entity_schema: &EntityWriteSchema,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    ensure_exact_selector(planned_write)?;
    reject_unsupported_entity_overrides(entity_schema)?;
    let version_id = resolved_version_id(planned_write)?.ok_or_else(|| WriteResolveError {
        message: "sql2 entity write resolver requires a concrete version_id".to_string(),
    })?;
    let entity_id = resolved_entity_id_for_entity(planned_write, entity_schema)?;
    let exact_filters = entity_state_exact_filters(planned_write, entity_schema, &entity_id)?;
    let Some(current_row) = resolve_exact_effective_state_row(
        backend,
        &ExactEffectiveStateRowRequest {
            schema_key: entity_schema.schema_key.clone(),
            version_id,
            exact_filters,
            include_global_overlay: true,
            include_untracked_overlay: true,
        },
    )
    .await
    .map_err(write_resolve_backend_error)?
    else {
        return Ok(ResolvedWritePlan {
            authoritative_pre_state: Vec::new(),
            intended_post_state: Vec::new(),
            tombstones: Vec::new(),
            lineage: Vec::new(),
            target_write_lane,
        });
    };
    ensure_local_tracked_overlay(&current_row)?;
    ensure_entity_selector_matches_current_row(entity_schema, planned_write, &current_row)?;

    let row_ref = ResolvedRowRef {
        entity_id: current_row.entity_id.clone(),
        schema_key: current_row.schema_key.clone(),
        version_id: Some(current_row.version_id.clone()),
        source_change_id: current_row.source_change_id.clone(),
        source_commit_id: None,
    };
    let lineage = vec![RowLineage {
        entity_id: current_row.entity_id.clone(),
        source_change_id: current_row.source_change_id.clone(),
        source_commit_id: None,
    }];

    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let values = merged_entity_update_values(planned_write, entity_schema, &current_row)?;
            Ok(ResolvedWritePlan {
                authoritative_pre_state: vec![row_ref],
                intended_post_state: vec![PlannedStateRow {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
                    values,
                    tombstone: false,
                }],
                tombstones: Vec::new(),
                lineage,
                target_write_lane,
            })
        }
        WriteOperationKind::Delete => Ok(ResolvedWritePlan {
            authoritative_pre_state: vec![row_ref.clone()],
            intended_post_state: vec![PlannedStateRow {
                entity_id: current_row.entity_id,
                schema_key: current_row.schema_key,
                version_id: Some(current_row.version_id),
                values: current_row.values,
                tombstone: true,
            }],
            tombstones: vec![row_ref],
            lineage,
            target_write_lane,
        }),
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "sql2 entity existing-row resolver does not handle inserts".to_string(),
        }),
    }
}

fn resolve_entity_insert_write_plan(
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
    entity_schema: &EntityWriteSchema,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    reject_unsupported_entity_overrides(entity_schema)?;
    let row = build_entity_insert_row(planned_write, entity_schema)?;
    Ok(ResolvedWritePlan {
        authoritative_pre_state: Vec::new(),
        intended_post_state: vec![row.clone()],
        tombstones: Vec::new(),
        lineage: vec![RowLineage {
            entity_id: row.entity_id,
            source_change_id: None,
            source_commit_id: None,
        }],
        target_write_lane,
    })
}

fn merged_update_values(
    current_values: &BTreeMap<String, Value>,
    planned_write: &PlannedWrite,
) -> Result<BTreeMap<String, Value>, WriteResolveError> {
    let MutationPayload::Patch(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "sql2 update resolver requires a patch payload".to_string(),
        });
    };

    let mut values = current_values.clone();
    for (key, value) in payload {
        values.insert(key.clone(), value.clone());
    }
    Ok(values)
}

fn ensure_identity_columns_preserved(
    entity_id: &str,
    schema_key: &str,
    file_id: &str,
    version_id: &str,
    values: &BTreeMap<String, Value>,
) -> Result<(), WriteResolveError> {
    for (column, expected) in [
        ("entity_id", entity_id),
        ("schema_key", schema_key),
        ("file_id", file_id),
        ("version_id", version_id),
    ] {
        let Some(actual) = values.get(column).and_then(text_from_value) else {
            return Err(WriteResolveError {
                message: format!("sql2 update resolver requires '{column}' in authoritative row"),
            });
        };
        if actual != expected {
            return Err(WriteResolveError {
                message: format!("sql2 day-1 update resolver does not support changing '{column}'"),
            });
        }
    }

    Ok(())
}

fn ensure_exact_selector(planned_write: &PlannedWrite) -> Result<(), WriteResolveError> {
    if !planned_write.command.selector.exact_only {
        return Err(WriteResolveError {
            message: "sql2 day-1 update/delete resolver only supports exact conjunctive selectors"
                .to_string(),
        });
    }
    Ok(())
}

fn ensure_local_tracked_overlay(
    current_row: &ExactEffectiveStateRow,
) -> Result<(), WriteResolveError> {
    if current_row.overlay_lane != OverlayLane::LocalTracked {
        return Err(WriteResolveError {
            message: format!(
                "sql2 live tracked writes do not yet support {:?} effective-state winners",
                current_row.overlay_lane
            ),
        });
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct EntityWriteSchema {
    schema_key: String,
    schema_version: String,
    property_columns: Vec<String>,
    primary_key_paths: Vec<Vec<String>>,
    state_defaults: BTreeMap<String, Value>,
}

async fn load_entity_schema(
    provider: &mut dyn SchemaProvider,
    planned_write: &PlannedWrite,
) -> Result<EntityWriteSchema, crate::LixError> {
    let schema_key = resolved_schema_key(planned_write).map_err(write_resolve_to_lix_error)?;
    let schema = if let Some(schema) = builtin_schema_definition(&schema_key) {
        schema.clone()
    } else {
        provider.load_latest_schema(&schema_key).await?
    };
    let schema_version = schema
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| crate::LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("schema '{}' is missing string x-lix-version", schema_key),
        })?
        .to_string();
    let mut property_columns = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| {
            properties
                .keys()
                .filter(|key| !key.starts_with("lixcol_"))
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    property_columns.sort();
    property_columns.dedup();

    let primary_key_paths = schema
        .get("x-lix-primary-key")
        .and_then(JsonValue::as_array)
        .map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    let pointer = entry.as_str().ok_or_else(|| crate::LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: format!(
                            "schema '{}' has non-string x-lix-primary-key entry",
                            schema_key
                        ),
                    })?;
                    parse_json_pointer(pointer)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    let mut state_defaults = BTreeMap::new();
    state_defaults.insert(
        "schema_version".to_string(),
        Value::Text(schema_version.clone()),
    );
    if let Some(overrides) = schema
        .get("x-lix-override-lixcols")
        .and_then(JsonValue::as_object)
    {
        for (raw_key, expr) in overrides {
            let Some(expr) = expr.as_str() else {
                continue;
            };
            let Some(key) = entity_state_column_name(raw_key) else {
                continue;
            };
            state_defaults.insert(key.to_string(), parse_literal_override(expr)?);
        }
    }

    Ok(EntityWriteSchema {
        schema_key,
        schema_version,
        property_columns,
        primary_key_paths,
        state_defaults,
    })
}

fn reject_unsupported_entity_overrides(
    entity_schema: &EntityWriteSchema,
) -> Result<(), WriteResolveError> {
    if entity_schema
        .state_defaults
        .get("global")
        .and_then(value_as_bool)
        == Some(true)
    {
        return Err(WriteResolveError {
            message: "sql2 entity live slice does not yet support lixcol_global write overrides"
                .to_string(),
        });
    }
    if entity_schema
        .state_defaults
        .get("untracked")
        .and_then(value_as_bool)
        == Some(true)
    {
        return Err(WriteResolveError {
            message: "sql2 entity live slice does not yet support lixcol_untracked write overrides"
                .to_string(),
        });
    }
    Ok(())
}

fn build_entity_insert_row(
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
) -> Result<PlannedStateRow, WriteResolveError> {
    let MutationPayload::FullSnapshot(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "sql2 entity insert resolver requires a full snapshot payload".to_string(),
        });
    };
    let version_id = resolved_version_id(planned_write)?;
    let snapshot = snapshot_from_entity_payload(payload, entity_schema)?;
    let entity_id = payload
        .get("entity_id")
        .and_then(text_from_value)
        .map(|value| value.to_string())
        .or_else(|| {
            derive_entity_id_from_snapshot(&snapshot, &entity_schema.primary_key_paths).ok()
        })
        .ok_or_else(|| WriteResolveError {
            message: "sql2 entity insert resolver requires an exact primary-key-derived entity_id"
                .to_string(),
        })?;
    let file_id = resolved_entity_state_text(payload, entity_schema, "file_id")?;
    let plugin_key = resolved_entity_state_text(payload, entity_schema, "plugin_key")?;
    let schema_version = resolved_entity_state_text(payload, entity_schema, "schema_version")?;
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(entity_id.clone()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(entity_schema.schema_key.clone()),
    );
    values.insert("file_id".to_string(), Value::Text(file_id));
    values.insert("plugin_key".to_string(), Value::Text(plugin_key));
    values.insert("schema_version".to_string(), Value::Text(schema_version));
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(
            serde_json::to_string(&JsonValue::Object(snapshot)).map_err(|error| {
                WriteResolveError {
                    message: format!(
                        "sql2 entity insert resolver could not serialize snapshot: {error}"
                    ),
                }
            })?,
        ),
    );
    if let Some(version_id) = version_id.clone() {
        values.insert("version_id".to_string(), Value::Text(version_id));
    }
    if let Some(metadata) = resolved_entity_state_value(payload, entity_schema, "metadata") {
        if metadata != Value::Null {
            values.insert("metadata".to_string(), metadata);
        }
    }

    Ok(PlannedStateRow {
        entity_id,
        schema_key: entity_schema.schema_key.clone(),
        version_id,
        values,
        tombstone: false,
    })
}

fn resolved_entity_id_for_entity(
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
) -> Result<String, WriteResolveError> {
    if let Some(entity_id) = planned_write
        .command
        .selector
        .exact_filters
        .get("entity_id")
        .and_then(text_from_value)
    {
        return Ok(entity_id.to_string());
    }
    if let Some(entity_id) = payload_text_value(planned_write, "entity_id") {
        return Ok(entity_id);
    }

    let snapshot = snapshot_from_exact_filters(
        &planned_write.command.selector.exact_filters,
        &entity_schema.property_columns,
    );
    derive_entity_id_from_snapshot(&snapshot, &entity_schema.primary_key_paths).map_err(|_| {
        WriteResolveError {
            message:
                "sql2 entity write resolver requires an exact selector over the entity primary key"
                    .to_string(),
        }
    })
}

fn entity_state_exact_filters(
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
    entity_id: &str,
) -> Result<BTreeMap<String, Value>, WriteResolveError> {
    let mut filters = BTreeMap::new();
    filters.insert("entity_id".to_string(), Value::Text(entity_id.to_string()));
    for key in [
        "file_id",
        "plugin_key",
        "schema_version",
        "global",
        "untracked",
    ] {
        if let Some(value) = planned_write.command.selector.exact_filters.get(key) {
            filters.insert(key.to_string(), value.clone());
            continue;
        }
        if let Some(default) = entity_schema.state_defaults.get(key) {
            filters.insert(key.to_string(), default.clone());
        }
    }
    Ok(filters)
}

fn ensure_entity_selector_matches_current_row(
    entity_schema: &EntityWriteSchema,
    planned_write: &PlannedWrite,
    current_row: &ExactEffectiveStateRow,
) -> Result<(), WriteResolveError> {
    let snapshot = parse_snapshot_object(&current_row.values)?;
    for (key, value) in &planned_write.command.selector.exact_filters {
        if !entity_schema
            .property_columns
            .iter()
            .any(|column| column == key)
        {
            continue;
        }
        if !json_value_matches_engine_value(snapshot.get(key), value) {
            return Err(WriteResolveError {
                message: format!(
                    "sql2 entity live slice requires exact property filters to match the visible row for '{}'",
                    key
                ),
            });
        }
    }
    Ok(())
}

fn merged_entity_update_values(
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
    current_row: &ExactEffectiveStateRow,
) -> Result<BTreeMap<String, Value>, WriteResolveError> {
    let MutationPayload::Patch(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "sql2 entity update resolver requires a patch payload".to_string(),
        });
    };

    let mut snapshot = parse_snapshot_object(&current_row.values)?;
    let mut values = current_row.values.clone();
    for (key, value) in payload {
        if entity_schema
            .primary_key_paths
            .iter()
            .any(|path| path.len() == 1 && path[0] == *key)
        {
            return Err(WriteResolveError {
                message: "sql2 entity live slice does not yet support primary-key property updates"
                    .to_string(),
            });
        }
        if entity_schema
            .property_columns
            .iter()
            .any(|column| column == key)
        {
            snapshot.insert(key.clone(), engine_value_to_json_value(value)?);
            continue;
        }
        return Err(WriteResolveError {
            message: format!(
                "sql2 entity live slice does not yet support updating state column '{}'",
                key
            ),
        });
    }

    let expected_entity_id = derive_entity_id_from_snapshot(
        &snapshot,
        &entity_schema.primary_key_paths,
    )
    .map_err(|_| WriteResolveError {
        message: "sql2 entity update resolver requires a stable primary-key-derived entity_id"
            .to_string(),
    })?;
    if expected_entity_id != current_row.entity_id {
        return Err(WriteResolveError {
            message:
                "sql2 entity live slice does not yet support updates that change entity identity"
                    .to_string(),
        });
    }

    values.insert(
        "snapshot_content".to_string(),
        Value::Text(
            serde_json::to_string(&JsonValue::Object(snapshot)).map_err(|error| {
                WriteResolveError {
                    message: format!(
                        "sql2 entity update resolver could not serialize snapshot: {error}"
                    ),
                }
            })?,
        ),
    );
    ensure_identity_columns_preserved(
        &current_row.entity_id,
        &current_row.schema_key,
        &current_row.file_id,
        &current_row.version_id,
        &values,
    )?;
    Ok(values)
}

fn resolved_entity_id(planned_write: &PlannedWrite) -> Result<String, WriteResolveError> {
    if let Some(TargetSetProof::Exact(entity_ids)) = &planned_write.target_set_proof {
        if entity_ids.len() == 1 {
            return Ok(entity_ids
                .iter()
                .next()
                .expect("singleton exact target-set proof")
                .clone());
        }
    }

    payload_text_value(planned_write, "entity_id").ok_or_else(|| WriteResolveError {
        message: "sql2 day-1 write resolver requires an exact entity target".to_string(),
    })
}

fn resolved_schema_key(planned_write: &PlannedWrite) -> Result<String, WriteResolveError> {
    match &planned_write.schema_proof {
        SchemaProof::Exact(schema_keys) if schema_keys.len() == 1 => Ok(schema_keys
            .iter()
            .next()
            .expect("singleton exact schema proof")
            .clone()),
        _ => payload_text_value(planned_write, "schema_key").ok_or_else(|| WriteResolveError {
            message: "sql2 write resolver requires an exact schema proof or schema_key literal"
                .to_string(),
        }),
    }
}

fn resolved_version_id(planned_write: &PlannedWrite) -> Result<Option<String>, WriteResolveError> {
    match &planned_write.scope_proof {
        ScopeProof::ActiveVersion => planned_write
            .command
            .execution_context
            .requested_version_id
            .clone()
            .map(Some)
            .ok_or_else(|| WriteResolveError {
                message:
                    "sql2 write resolver requires requested_version_id for ActiveVersion writes"
                        .to_string(),
            }),
        ScopeProof::SingleVersion(version_id) => Ok(Some(version_id.clone())),
        ScopeProof::FiniteVersionSet(version_ids) if version_ids.len() == 1 => {
            Ok(version_ids.iter().next().cloned())
        }
        ScopeProof::FiniteVersionSet(_) => Err(WriteResolveError {
            message: "sql2 day-1 write resolver cannot resolve multi-version writes".to_string(),
        }),
        ScopeProof::Unknown | ScopeProof::Unbounded => Err(WriteResolveError {
            message: "sql2 day-1 write resolver requires a bounded scope proof".to_string(),
        }),
    }
}

fn write_lane_from_scope(scope_proof: &ScopeProof) -> Result<WriteLane, WriteResolveError> {
    match scope_proof {
        ScopeProof::ActiveVersion => Ok(WriteLane::ActiveVersion),
        ScopeProof::SingleVersion(version_id) => Ok(WriteLane::SingleVersion(version_id.clone())),
        ScopeProof::FiniteVersionSet(version_ids) if version_ids.len() == 1 => {
            Ok(WriteLane::SingleVersion(
                version_ids
                    .iter()
                    .next()
                    .expect("singleton version set")
                    .clone(),
            ))
        }
        ScopeProof::FiniteVersionSet(_) => Err(WriteResolveError {
            message: "sql2 day-1 tracked writes require exactly one write lane".to_string(),
        }),
        ScopeProof::Unknown | ScopeProof::Unbounded => Err(WriteResolveError {
            message: "sql2 day-1 tracked writes require a bounded write lane".to_string(),
        }),
    }
}

fn snapshot_from_entity_payload(
    payload: &BTreeMap<String, Value>,
    entity_schema: &EntityWriteSchema,
) -> Result<JsonMap<String, JsonValue>, WriteResolveError> {
    let mut snapshot = JsonMap::new();
    for key in &entity_schema.property_columns {
        if let Some(value) = payload.get(key) {
            snapshot.insert(key.clone(), engine_value_to_json_value(value)?);
        }
    }
    Ok(snapshot)
}

fn snapshot_from_exact_filters(
    exact_filters: &BTreeMap<String, Value>,
    property_columns: &[String],
) -> JsonMap<String, JsonValue> {
    let mut snapshot = JsonMap::new();
    for key in property_columns {
        if let Some(value) = exact_filters.get(key) {
            if let Ok(json_value) = engine_value_to_json_value(value) {
                snapshot.insert(key.clone(), json_value);
            }
        }
    }
    snapshot
}

fn parse_snapshot_object(
    values: &BTreeMap<String, Value>,
) -> Result<JsonMap<String, JsonValue>, WriteResolveError> {
    let Some(snapshot_text) = values.get("snapshot_content").and_then(text_from_value) else {
        return Err(WriteResolveError {
            message: "sql2 entity resolver requires snapshot_content in authoritative pre-state"
                .to_string(),
        });
    };
    let JsonValue::Object(object) =
        serde_json::from_str::<JsonValue>(&snapshot_text).map_err(|error| WriteResolveError {
            message: format!("sql2 entity resolver could not parse snapshot_content JSON: {error}"),
        })?
    else {
        return Err(WriteResolveError {
            message: "sql2 entity resolver requires object snapshot_content".to_string(),
        });
    };
    Ok(object)
}

fn derive_entity_id_from_snapshot(
    snapshot: &JsonMap<String, JsonValue>,
    primary_key_paths: &[Vec<String>],
) -> Result<String, WriteResolveError> {
    if primary_key_paths.is_empty() {
        return Err(WriteResolveError {
            message: "sql2 entity resolver requires x-lix-primary-key for entity writes"
                .to_string(),
        });
    }

    let snapshot = JsonValue::Object(snapshot.clone());
    let mut parts = Vec::with_capacity(primary_key_paths.len());
    for path in primary_key_paths {
        if path.is_empty() {
            return Err(WriteResolveError {
                message: "sql2 entity resolver does not support empty primary-key pointers"
                    .to_string(),
            });
        }
        let value = json_pointer_get(&snapshot, path).ok_or_else(|| WriteResolveError {
            message: "sql2 entity resolver could not derive entity_id from the primary-key fields"
                .to_string(),
        })?;
        parts.push(entity_id_component_from_json_value(value)?);
    }

    Ok(if parts.len() == 1 {
        parts.into_iter().next().expect("single primary key part")
    } else {
        parts.join("~")
    })
}

fn resolved_entity_state_text(
    payload: &BTreeMap<String, Value>,
    entity_schema: &EntityWriteSchema,
    key: &str,
) -> Result<String, WriteResolveError> {
    resolved_entity_state_value(payload, entity_schema, key)
        .and_then(|value| text_from_value(&value))
        .map(|value| value.to_string())
        .ok_or_else(|| WriteResolveError {
            message: format!(
                "sql2 entity resolver requires a concrete '{}' value or schema override",
                key
            ),
        })
}

fn resolved_entity_state_value(
    payload: &BTreeMap<String, Value>,
    entity_schema: &EntityWriteSchema,
    key: &str,
) -> Option<Value> {
    payload
        .get(key)
        .cloned()
        .or_else(|| entity_schema.state_defaults.get(key).cloned())
}

fn entity_state_column_name(column: &str) -> Option<&'static str> {
    match column.to_ascii_lowercase().as_str() {
        "lixcol_entity_id" => Some("entity_id"),
        "lixcol_schema_key" => Some("schema_key"),
        "lixcol_file_id" => Some("file_id"),
        "lixcol_version_id" => Some("version_id"),
        "lixcol_plugin_key" => Some("plugin_key"),
        "lixcol_schema_version" => Some("schema_version"),
        "lixcol_global" => Some("global"),
        "lixcol_writer_key" => Some("writer_key"),
        "lixcol_untracked" => Some("untracked"),
        "lixcol_metadata" => Some("metadata"),
        _ => None,
    }
}

fn parse_literal_override(expr: &str) -> Result<Value, crate::LixError> {
    let parsed: JsonValue = serde_json::from_str(expr).map_err(|error| crate::LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("sql2 entity resolver requires literal lixcol overrides: {error}"),
    })?;
    Ok(json_value_to_engine_value(&parsed))
}

fn engine_value_to_json_value(value: &Value) -> Result<JsonValue, WriteResolveError> {
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Text(value) => Ok(JsonValue::String(value.clone())),
        Value::Boolean(value) => Ok(JsonValue::Bool(*value)),
        Value::Integer(value) => Ok(JsonValue::Number((*value).into())),
        Value::Real(value) => JsonNumber::from_f64(*value)
            .map(JsonValue::Number)
            .ok_or_else(|| WriteResolveError {
                message: "sql2 entity resolver cannot represent NaN/inf JSON numbers".to_string(),
            }),
        Value::Blob(_) => Err(WriteResolveError {
            message: "sql2 entity resolver does not support blob entity properties".to_string(),
        }),
    }
}

fn json_value_to_engine_value(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Boolean(*value),
        JsonValue::String(value) => Value::Text(value.clone()),
        JsonValue::Number(number) => number
            .as_i64()
            .map(Value::Integer)
            .or_else(|| number.as_f64().map(Value::Real))
            .unwrap_or_else(|| Value::Text(number.to_string())),
        JsonValue::Array(_) | JsonValue::Object(_) => Value::Text(value.to_string()),
    }
}

fn json_value_matches_engine_value(actual: Option<&JsonValue>, expected: &Value) -> bool {
    match (actual, expected) {
        (Some(JsonValue::Null), Value::Null) => true,
        (Some(JsonValue::Bool(actual)), Value::Boolean(expected)) => actual == expected,
        (Some(JsonValue::String(actual)), Value::Text(expected)) => actual == expected,
        (Some(JsonValue::Number(actual)), Value::Integer(expected)) => {
            actual.as_i64() == Some(*expected)
        }
        (Some(JsonValue::Number(actual)), Value::Real(expected)) => {
            actual.as_f64().is_some_and(|value| value == *expected)
        }
        (None, Value::Null) => true,
        _ => false,
    }
}

fn entity_id_component_from_json_value(value: &JsonValue) -> Result<String, WriteResolveError> {
    match value {
        JsonValue::Null => Err(WriteResolveError {
            message: "sql2 entity resolver cannot derive entity_id from null primary-key values"
                .to_string(),
        }),
        JsonValue::String(text) => Ok(text.clone()),
        JsonValue::Bool(flag) => Ok(flag.to_string()),
        JsonValue::Number(number) => Ok(number.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => Ok(value.to_string()),
    }
}

fn parse_json_pointer(pointer: &str) -> Result<Vec<String>, crate::LixError> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(crate::LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("invalid JSON pointer '{pointer}'"),
        });
    }
    pointer[1..]
        .split('/')
        .map(decode_json_pointer_segment)
        .collect()
}

fn decode_json_pointer_segment(segment: &str) -> Result<String, crate::LixError> {
    let mut out = String::new();
    let mut chars = segment.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') => out.push('~'),
                Some('1') => out.push('/'),
                _ => {
                    return Err(crate::LixError {
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
            JsonValue::Object(object) => current = object.get(segment)?,
            JsonValue::Array(array) => {
                let index = segment.parse::<usize>().ok()?;
                current = array.get(index)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(value) => Some(*value),
        Value::Integer(value) => Some(*value != 0),
        Value::Text(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Some(true),
            "0" | "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn write_resolve_to_lix_error(error: WriteResolveError) -> crate::LixError {
    crate::LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.message,
    }
}

fn payload_map(planned_write: &PlannedWrite) -> Result<BTreeMap<String, Value>, WriteResolveError> {
    match &planned_write.command.payload {
        MutationPayload::FullSnapshot(payload) | MutationPayload::Patch(payload) => {
            Ok(payload.clone())
        }
        MutationPayload::Tombstone => Ok(Default::default()),
    }
}

fn payload_text_value(planned_write: &PlannedWrite, key: &str) -> Option<String> {
    let (MutationPayload::FullSnapshot(payload) | MutationPayload::Patch(payload)) =
        &planned_write.command.payload
    else {
        return None;
    };

    match payload.get(key) {
        Some(Value::Text(value)) => Some(value.clone()),
        _ => None,
    }
}

fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(value) => Some(value.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(value.to_string()),
        Value::Real(value) => Some(value.to_string()),
        _ => None,
    }
}

fn write_resolve_backend_error(error: crate::LixError) -> WriteResolveError {
    WriteResolveError {
        message: error.description,
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_write_plan;
    use crate::sql2::catalog::SurfaceRegistry;
    use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql2::core::parser::parse_sql_script;
    use crate::sql2::planner::canonicalize::canonicalize_write;
    use crate::sql2::planner::ir::WriteLane;
    use crate::sql2::planner::semantics::proof_engine::prove_write;
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;

    #[derive(Default)]
    struct FakeBackend {
        state_rows: Vec<Vec<Value>>,
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM \"lix_internal_state_materialized_v1_lix_key_value\"") {
                let entity_filter = extract_sql_string_filter(sql, "entity_id");
                let version_filter = extract_sql_string_filter(sql, "version_id");
                let file_filter = extract_sql_string_filter(sql, "file_id");
                let plugin_filter = extract_sql_string_filter(sql, "plugin_key");
                return Ok(QueryResult {
                        rows: self
                            .state_rows
                        .iter()
                        .filter(|row| {
                            let entity_matches = match entity_filter.as_ref() {
                                Some(entity_id) => {
                                    matches!(row.first(), Some(Value::Text(value)) if value == entity_id)
                                }
                                None => true,
                            };
                            let version_matches = match version_filter.as_ref() {
                                Some(version_id) => {
                                    matches!(row.get(4), Some(Value::Text(value)) if value == version_id)
                                }
                                None => true,
                            };
                            let file_matches = match file_filter.as_ref() {
                                Some(file_id) => {
                                    matches!(row.get(3), Some(Value::Text(value)) if value == file_id)
                                }
                                None => true,
                            };
                            let plugin_matches = match plugin_filter.as_ref() {
                                Some(plugin_key) => {
                                    matches!(row.get(5), Some(Value::Text(value)) if value == plugin_key)
                                }
                                None => true,
                            };
                            entity_matches && version_matches && file_matches && plugin_matches
                        })
                        .cloned()
                        .collect(),
                        columns: vec![
                            "entity_id".to_string(),
                            "schema_key".to_string(),
                            "schema_version".to_string(),
                            "file_id".to_string(),
                            "version_id".to_string(),
                            "plugin_key".to_string(),
                            "snapshot_content".to_string(),
                            "metadata".to_string(),
                            "change_id".to_string(),
                        ],
                    });
            }

            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn crate::LixTransaction + '_>, LixError> {
            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "transactions are not needed in this test backend".to_string(),
            })
        }
    }

    fn extract_sql_string_filter(sql: &str, column: &str) -> Option<String> {
        let marker = format!("{column} = '");
        let start = sql.find(&marker)? + marker.len();
        let rest = &sql[start..];
        let end = rest.find('\'')?;
        Some(rest[..end].to_string())
    }

    fn planned_write(
        sql: &str,
        requested_version_id: &str,
    ) -> crate::sql2::planner::ir::PlannedWrite {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let mut statements = parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let bound = BoundStatement::from_statement(
            statement,
            Vec::new(),
            ExecutionContext {
                requested_version_id: Some(requested_version_id.to_string()),
                ..ExecutionContext::default()
            },
        );
        let canonicalized =
            canonicalize_write(bound, &registry).expect("write should canonicalize");
        prove_write(&canonicalized).expect("proofs should succeed")
    }

    #[tokio::test]
    async fn resolves_active_version_insert_with_active_lane() {
        let backend = FakeBackend::default();
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "INSERT INTO lix_state (entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version) \
                 VALUES ('entity-1', 'lix_key_value', 'lix', 'lix', '{\"key\":\"hello\"}', '1')",
                "main",
            ),
        )
        .await
        .expect("write should resolve");

        assert_eq!(
            resolved.intended_post_state[0].version_id.as_deref(),
            Some("main")
        );
        assert_eq!(resolved.target_write_lane, Some(WriteLane::ActiveVersion));
    }

    #[tokio::test]
    async fn resolves_explicit_version_insert_with_single_version_lane() {
        let backend = FakeBackend::default();
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "INSERT INTO lix_state_by_version (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) \
                 VALUES ('entity-1', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"hello\"}', '1')",
                "main",
            ),
        )
        .await
        .expect("write should resolve");

        assert_eq!(
            resolved.target_write_lane,
            Some(WriteLane::SingleVersion("version-a".to_string()))
        );
        assert_eq!(
            resolved.intended_post_state[0].version_id.as_deref(),
            Some("version-a")
        );
    }

    #[tokio::test]
    async fn resolves_update_from_authoritative_pre_state() {
        let backend = FakeBackend {
            state_rows: vec![vec![
                Value::Text("entity-1".to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("1".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("version-a".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("{\"value\":\"before\"}".to_string()),
                Value::Text("{\"m\":1}".to_string()),
                Value::Text("change-1".to_string()),
            ]],
        };
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect("write should resolve");

        assert_eq!(resolved.authoritative_pre_state.len(), 1);
        assert_eq!(
            resolved.intended_post_state[0]
                .values
                .get("file_id")
                .and_then(super::text_from_value)
                .as_deref(),
            Some("lix")
        );
        assert_eq!(
            resolved.intended_post_state[0]
                .values
                .get("snapshot_content")
                .and_then(super::text_from_value)
                .as_deref(),
            Some("{\"value\":\"after\"}")
        );
    }

    #[tokio::test]
    async fn resolves_delete_from_authoritative_pre_state() {
        let backend = FakeBackend {
            state_rows: vec![vec![
                Value::Text("entity-1".to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("1".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("version-a".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("{\"value\":\"before\"}".to_string()),
                Value::Null,
                Value::Text("change-1".to_string()),
            ]],
        };
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "DELETE FROM lix_state_by_version \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect("write should resolve");

        assert_eq!(resolved.authoritative_pre_state.len(), 1);
        assert_eq!(resolved.tombstones.len(), 1);
        assert!(resolved.intended_post_state[0].tombstone);
    }

    #[tokio::test]
    async fn leaves_noop_update_with_no_rows_to_append() {
        let backend = FakeBackend::default();
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-missing' \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect("missing rows should resolve as a no-op");

        assert_eq!(
            resolved.target_write_lane,
            Some(WriteLane::SingleVersion("version-a".into()))
        );
        assert!(resolved.authoritative_pre_state.is_empty());
        assert!(resolved.intended_post_state.is_empty());
    }

    #[tokio::test]
    async fn rejects_update_that_changes_identity_columns() {
        let backend = FakeBackend {
            state_rows: vec![vec![
                Value::Text("entity-1".to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("1".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("version-a".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("{\"value\":\"before\"}".to_string()),
                Value::Null,
                Value::Text("change-1".to_string()),
            ]],
        };
        let error = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state_by_version \
                 SET file_id = 'other-file' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect_err("identity-changing update should stay off the sql2 live slice");

        assert!(error
            .message
            .contains("does not support changing 'file_id'"));
    }

    #[tokio::test]
    async fn exact_file_filter_prevents_mismatched_updates() {
        let backend = FakeBackend {
            state_rows: vec![vec![
                Value::Text("entity-1".to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("1".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("version-a".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("{\"value\":\"before\"}".to_string()),
                Value::Null,
                Value::Text("change-1".to_string()),
            ]],
        };
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND file_id = 'other-file' \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect("mismatched exact filters should resolve as a no-op");

        assert!(resolved.intended_post_state.is_empty());
    }

    #[tokio::test]
    async fn rejects_non_exact_or_selectors() {
        let backend = FakeBackend {
            state_rows: vec![vec![
                Value::Text("entity-1".to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("1".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("version-a".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("{\"value\":\"before\"}".to_string()),
                Value::Null,
                Value::Text("change-1".to_string()),
            ]],
        };
        let error = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND (entity_id = 'entity-1' OR entity_id = 'entity-2') \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect_err("unsupported selectors should stay off the live sql2 slice");

        assert!(error.message.contains("exact conjunctive selectors"));
    }
}
