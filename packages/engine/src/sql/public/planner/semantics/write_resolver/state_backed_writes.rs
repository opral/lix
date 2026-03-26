use super::*;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::schema::builtin::builtin_schema_definition;
use crate::schema::{SchemaProvider, SqlRegisteredSchemaProvider};
use crate::transaction::PendingTransactionView;
use crate::sql::public::planner::ir::CanonicalStateAssignments;
use crate::sql::public::planner::ir::CanonicalStateRowKey;
use crate::sql::public::planner::semantics::effective_state_resolver::resolve_exact_effective_state_row_with_pending_transaction_view;
use crate::sql::public::planner::semantics::state_assignments::{
    apply_entity_state_assignments, apply_state_assignments, assignments_from_payload,
    build_entity_insert_rows_with_functions, build_state_insert_row, ensure_identity_columns_preserved,
    EntityAssignmentsSemantics, EntityInsertSemantics,
};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

fn authoritative_version_id_for_effective_row(current_row: &ExactEffectiveStateRow) -> String {
    match current_row.overlay_lane {
        OverlayLane::GlobalTracked | OverlayLane::GlobalUntracked => GLOBAL_VERSION_ID.to_string(),
        OverlayLane::LocalTracked | OverlayLane::LocalUntracked => current_row.version_id.clone(),
    }
}

async fn query_entity_selector_rows(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    pending_transaction_view: Option<&PendingTransactionView>,
) -> Result<Vec<CanonicalStateRowKey>, WriteResolveError> {
    let selector = canonical_state_selector(planned_write);
    let mut selector_columns = vec!["lixcol_entity_id"];
    if let Some(version_column) = selector.version_column.as_deref() {
        selector_columns.push(version_column);
    }
    let query_result = execute_public_selector_query_strict(
        backend,
        planned_write,
        pending_transaction_view,
        build_public_selector_query(
            &planned_write.command.target.descriptor.public_name,
            &selector,
            &selector_columns,
        ),
    )
    .await
    .map_err(write_resolve_backend_error)?;

    let mut selector_rows = Vec::new();
    for row in query_result.rows {
        let selector_row = CanonicalStateRowKey {
            entity_id: required_text_value_index(&row, 0, "lixcol_entity_id")?,
            file_id: None,
            plugin_key: None,
            schema_version: None,
            version_id: selector
                .version_column
                .as_deref()
                .map(|version_column| required_text_value_index(&row, 1, version_column))
                .transpose()?,
            global: None,
            untracked: None,
            writer_key: None,
        };
        if !selector_rows
            .iter()
            .any(|existing| existing == &selector_row)
        {
            selector_rows.push(selector_row);
        }
    }
    Ok(selector_rows)
}

async fn query_state_selector_rows(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    pending_transaction_view: Option<&PendingTransactionView>,
) -> Result<Vec<CanonicalStateRowKey>, WriteResolveError> {
    let selector = canonical_state_selector(planned_write);
    let mut selector_columns = vec!["entity_id", "file_id", "plugin_key", "schema_version"];
    if let Some(version_column) = selector.version_column.as_deref() {
        selector_columns.push(version_column);
    }
    selector_columns.push("global");
    selector_columns.push("untracked");
    let query_result = execute_public_selector_query_strict(
        backend,
        planned_write,
        pending_transaction_view,
        build_public_selector_query(
            &planned_write.command.target.descriptor.public_name,
            &selector,
            &selector_columns,
        ),
    )
    .await
    .map_err(write_resolve_backend_error)?;

    let mut selector_rows = Vec::new();
    for row in query_result.rows {
        let version_offset = usize::from(selector.version_column.is_some());
        let selector_row = CanonicalStateRowKey {
            entity_id: required_text_value_index(&row, 0, "entity_id")?,
            file_id: Some(required_text_value_index(&row, 1, "file_id")?),
            plugin_key: Some(required_text_value_index(&row, 2, "plugin_key")?),
            schema_version: Some(required_text_value_index(&row, 3, "schema_version")?),
            version_id: selector
                .version_column
                .as_deref()
                .map(|version_column| required_text_value_index(&row, 4, version_column))
                .transpose()?,
            global: Some(required_bool_value_index(
                &row,
                4 + version_offset,
                "global",
            )?),
            untracked: Some(required_bool_value_index(
                &row,
                5 + version_offset,
                "untracked",
            )?),
            writer_key: None,
        };
        if !selector_rows
            .iter()
            .any(|existing| existing == &selector_row)
        {
            selector_rows.push(selector_row);
        }
    }
    Ok(selector_rows)
}

fn assign_state_row_key_value(
    row_key: &mut CanonicalStateRowKey,
    column: &str,
    value: &Value,
) -> Result<(), WriteResolveError> {
    match column {
        "entity_id" => {
            row_key.entity_id = exact_text_value(
                value,
                "public state row key requires text-compatible 'entity_id'",
            )?;
        }
        "file_id" => {
            row_key.file_id = Some(exact_text_value(
                value,
                "public state row key requires text-compatible 'file_id'",
            )?);
        }
        "plugin_key" => {
            row_key.plugin_key = Some(exact_text_value(
                value,
                "public state row key requires text-compatible 'plugin_key'",
            )?);
        }
        "schema_version" => {
            row_key.schema_version = Some(exact_text_value(
                value,
                "public state row key requires text-compatible 'schema_version'",
            )?);
        }
        "version_id" => {
            row_key.version_id = Some(exact_text_value(
                value,
                "public state row key requires text-compatible 'version_id'",
            )?);
        }
        "writer_key" => {
            row_key.writer_key = Some(exact_text_value(
                value,
                "public state row key requires text-compatible 'writer_key'",
            )?);
        }
        "global" => {
            row_key.global = Some(exact_bool_value(
                value,
                "public state row key requires boolean-compatible 'global'",
            )?);
        }
        "untracked" => {
            row_key.untracked = Some(exact_bool_value(
                value,
                "public state row key requires boolean-compatible 'untracked'",
            )?);
        }
        _ => {}
    }
    Ok(())
}

fn exact_filter_text(
    filters: &std::collections::BTreeMap<String, Value>,
    key: &str,
    error_message: &str,
) -> Result<Option<String>, WriteResolveError> {
    filters
        .get(key)
        .map(|value| exact_text_value(value, error_message))
        .transpose()
}

fn exact_text_value(value: &Value, error_message: &str) -> Result<String, WriteResolveError> {
    text_from_value(value).ok_or_else(|| WriteResolveError {
        message: error_message.to_string(),
    })
}

fn exact_bool_value(value: &Value, error_message: &str) -> Result<bool, WriteResolveError> {
    bool_from_value(value).ok_or_else(|| WriteResolveError {
        message: error_message.to_string(),
    })
}

fn authoritative_pre_state_row_for_effective_row(
    current_row: &ExactEffectiveStateRow,
    authoritative_version_id: &str,
) -> PlannedStateRow {
    let mut values = current_row.values.clone();
    values.insert(
        "version_id".to_string(),
        Value::Text(authoritative_version_id.to_string()),
    );
    PlannedStateRow {
        entity_id: current_row.entity_id.clone(),
        schema_key: current_row.schema_key.clone(),
        version_id: Some(authoritative_version_id.to_string()),
        values,
        tombstone: false,
    }
}

pub(super) async fn resolve_state_write<P>(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    pending_transaction_view: Option<&PendingTransactionView>,
    functions: SharedFunctionProvider<P>,
) -> Result<ResolvedWritePlan, WriteResolveError>
where
    P: LixFunctionProvider + Send + 'static,
{
    resolve_state_backed_write(
        backend,
        planned_write,
        pending_transaction_view,
        StateBackedSurface::State,
        functions,
    )
    .await
}

pub(super) async fn resolve_entity_write<P>(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    pending_transaction_view: Option<&PendingTransactionView>,
    functions: SharedFunctionProvider<P>,
) -> Result<ResolvedWritePlan, WriteResolveError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let mut provider = SqlRegisteredSchemaProvider::new(backend);
    let entity_schema = load_entity_schema(&mut provider, planned_write)
        .await
        .map_err(write_resolve_backend_error)?;
    reject_unsupported_entity_overrides(planned_write, &entity_schema)?;
    resolve_state_backed_write(
        backend,
        planned_write,
        pending_transaction_view,
        StateBackedSurface::Entity(&entity_schema),
        functions,
    )
    .await
}

#[derive(Debug, Clone)]
struct EntityWriteSchema {
    schema: JsonValue,
    schema_key: String,
    schema_version: String,
    property_columns: Vec<String>,
    primary_key_paths: Vec<Vec<String>>,
    state_defaults: BTreeMap<String, Value>,
}

#[derive(Clone, Copy)]
enum StateBackedSurface<'a> {
    State,
    Entity(&'a EntityWriteSchema),
}

impl StateBackedSurface<'_> {
    fn update_context(self) -> &'static str {
        match self {
            Self::State => "public update resolver",
            Self::Entity(_) => "public entity update resolver",
        }
    }

    fn build_insert_rows<P>(
        self,
        planned_write: &PlannedWrite,
        functions: SharedFunctionProvider<P>,
    ) -> Result<Vec<PlannedStateRow>, WriteResolveError>
    where
        P: LixFunctionProvider + Send + 'static,
    {
        match self {
            Self::State => build_state_insert_rows(planned_write),
            Self::Entity(entity_schema) => build_entity_insert_rows_with_functions(
                payload_maps(planned_write)?,
                resolved_version_id(planned_write)?,
                EntityInsertSemantics {
                    schema: &entity_schema.schema,
                    schema_key: &entity_schema.schema_key,
                    schema_version: &entity_schema.schema_version,
                    property_columns: &entity_schema.property_columns,
                    primary_key_paths: &entity_schema.primary_key_paths,
                    state_defaults: &entity_schema.state_defaults,
                },
                functions,
            )
            .map_err(write_resolve_state_assignments_error),
        }
    }

    fn prepare_update_assignments(
        self,
        planned_write: &PlannedWrite,
    ) -> Result<CanonicalStateAssignments, WriteResolveError> {
        assignments_from_payload(&planned_write.command.payload, self.update_context())
            .map_err(write_resolve_state_assignments_error)
    }

    fn apply_update_assignments(
        self,
        assignments: &CanonicalStateAssignments,
        current_row: &ExactEffectiveStateRow,
    ) -> Result<BTreeMap<String, Value>, WriteResolveError> {
        match self {
            Self::State => {
                let values = apply_state_assignments(&current_row.values, assignments);
                ensure_identity_columns_preserved(
                    &current_row.entity_id,
                    &current_row.schema_key,
                    &current_row.file_id,
                    &current_row.version_id,
                    &values,
                )
                .map_err(write_resolve_state_assignments_error)?;
                Ok(values)
            }
            Self::Entity(entity_schema) => apply_entity_state_assignments(
                current_row,
                assignments,
                EntityAssignmentsSemantics {
                    property_columns: &entity_schema.property_columns,
                    primary_key_paths: &entity_schema.primary_key_paths,
                },
            )
            .map_err(write_resolve_state_assignments_error),
        }
    }

    async fn resolve_insert_conflict_row(
        self,
        backend: &dyn LixBackend,
        pending_transaction_view: Option<&PendingTransactionView>,
        _planned_write: &PlannedWrite,
        row: &PlannedStateRow,
    ) -> Result<Option<ExactEffectiveStateRow>, WriteResolveError> {
        match self {
            Self::State => {
                let version_id = row.version_id.clone().ok_or_else(|| WriteResolveError {
                    message: "public state insert resolver requires a concrete version_id"
                        .to_string(),
                })?;
                resolve_exact_effective_state_row_with_pending_transaction_view(
                    backend,
                    &ExactEffectiveStateRowRequest {
                        schema_key: row.schema_key.clone(),
                        version_id,
                        row_key: state_insert_row_key(row),
                        include_global_overlay: true,
                        include_untracked_overlay: true,
                    },
                    pending_transaction_view,
                )
                .await
                .map_err(write_resolve_backend_error)
            }
            Self::Entity(entity_schema) => {
                let version_id = row.version_id.clone().ok_or_else(|| WriteResolveError {
                    message: "public entity insert resolver requires a concrete version_id"
                        .to_string(),
                })?;
                resolve_exact_effective_state_row_with_pending_transaction_view(
                    backend,
                    &ExactEffectiveStateRowRequest {
                        schema_key: entity_schema.schema_key.clone(),
                        version_id,
                        row_key: entity_insert_row_key(entity_schema, row)?,
                        include_global_overlay: true,
                        include_untracked_overlay: true,
                    },
                    pending_transaction_view,
                )
                .await
                .map_err(write_resolve_backend_error)
            }
        }
    }

    async fn resolve_target_rows(
        self,
        backend: &dyn LixBackend,
        planned_write: &PlannedWrite,
        pending_transaction_view: Option<&PendingTransactionView>,
    ) -> Result<Vec<ExactEffectiveStateRow>, WriteResolveError> {
        match self {
            Self::State => {
                resolve_target_state_rows(backend, planned_write, pending_transaction_view).await
            }
            Self::Entity(entity_schema) => {
                resolve_target_entity_rows(
                    backend,
                    planned_write,
                    entity_schema,
                    pending_transaction_view,
                )
                .await
            }
        }
    }
}

async fn resolve_state_backed_write<P>(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    pending_transaction_view: Option<&PendingTransactionView>,
    surface: StateBackedSurface<'_>,
    functions: SharedFunctionProvider<P>,
) -> Result<ResolvedWritePlan, WriteResolveError>
where
    P: LixFunctionProvider + Send + 'static,
{
    match planned_write.command.operation_kind {
        WriteOperationKind::Insert => {
            resolve_state_backed_insert_write(
                backend,
                planned_write,
                pending_transaction_view,
                surface,
                functions,
            )
            .await
        }
        WriteOperationKind::Update | WriteOperationKind::Delete => {
            resolve_state_backed_existing_write(
                backend,
                planned_write,
                pending_transaction_view,
                surface,
            )
            .await
        }
    }
}

async fn resolve_state_backed_insert_write<P>(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    pending_transaction_view: Option<&PendingTransactionView>,
    surface: StateBackedSurface<'_>,
    functions: SharedFunctionProvider<P>,
) -> Result<ResolvedWritePlan, WriteResolveError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let rows = surface.build_insert_rows(planned_write, functions)?;
    let mut partitions = ResolvedWritePlanBuilder::default();
    let default_execution_mode =
        default_execution_mode_for_request(planned_write.command.requested_mode);

    for row in rows {
        if let Some(conflict) = planned_write.command.on_conflict.as_ref() {
            if let Some(current_row) = surface
                .resolve_insert_conflict_row(backend, pending_transaction_view, planned_write, &row)
                .await?
            {
                if conflict.action == InsertOnConflictAction::DoNothing {
                    continue;
                }
                let row_execution_mode = resolve_execution_mode_for_effective_row(
                    planned_write.command.requested_mode,
                    &current_row,
                )?;
                let row_ref = ResolvedRowRef {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
                    source_change_id: current_row.source_change_id.clone(),
                    source_commit_id: None,
                };
                let target_write_lane = target_write_lane_for_effective_row(
                    planned_write,
                    row_execution_mode,
                    &current_row,
                )?;
                let partition = partitions.partition_mut(row_execution_mode, target_write_lane);
                partition.authoritative_pre_state.push(row_ref.clone());
                partition.authoritative_pre_state_rows.push(
                    authoritative_pre_state_row_for_effective_row(
                        &current_row,
                        &authoritative_version_id_for_effective_row(&current_row),
                    ),
                );
                partition.lineage.push(RowLineage {
                    entity_id: row_ref.entity_id,
                    source_change_id: row_ref.source_change_id,
                    source_commit_id: None,
                });
                partition.intended_post_state.push(row);
                continue;
            }
        }
        let target_write_lane = target_write_lane_for_planned_row(
            planned_write,
            default_execution_mode,
            row.version_id.as_deref(),
        )?;
        let partition = partitions.partition_mut(default_execution_mode, target_write_lane);
        partition.lineage.push(RowLineage {
            entity_id: row.entity_id.clone(),
            source_change_id: None,
            source_commit_id: None,
        });
        partition.intended_post_state.push(row);
    }

    Ok(partitions.into_resolved_write_plan(planned_write.command.requested_mode))
}

async fn resolve_state_backed_existing_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    pending_transaction_view: Option<&PendingTransactionView>,
    surface: StateBackedSurface<'_>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let current_rows = match surface {
        StateBackedSurface::State
            if planned_write.command.selector.exact_only
                && state_selector_targets_single_effective_row(planned_write) =>
        {
            resolve_exact_state_target_rows(backend, planned_write, pending_transaction_view)
                .await?
        }
        _ => {
            surface
                .resolve_target_rows(backend, planned_write, pending_transaction_view)
                .await?
        }
    };
    resolve_state_backed_existing_write_from_rows(surface, planned_write, current_rows)
}

fn resolve_state_backed_existing_write_from_rows(
    surface: StateBackedSurface<'_>,
    planned_write: &PlannedWrite,
    current_rows: Vec<ExactEffectiveStateRow>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    if current_rows.is_empty() {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    }

    let mut partitions = ResolvedWritePlanBuilder::default();
    let assignments = matches!(
        planned_write.command.operation_kind,
        WriteOperationKind::Update
    )
    .then(|| surface.prepare_update_assignments(planned_write))
    .transpose()?;

    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let assignments = assignments.as_ref().expect("update assignments prepared");
            for current_row in current_rows {
                let execution_mode = resolve_execution_mode_for_effective_row(
                    planned_write.command.requested_mode,
                    &current_row,
                )?;
                let authoritative_version_id =
                    authoritative_version_id_for_effective_row(&current_row);
                let target_version_id = current_row.version_id.clone();
                let row_ref = ResolvedRowRef {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(authoritative_version_id.clone()),
                    source_change_id: current_row.source_change_id.clone(),
                    source_commit_id: None,
                };
                let mut values = surface.apply_update_assignments(assignments, &current_row)?;
                values.insert(
                    "version_id".to_string(),
                    Value::Text(target_version_id.clone()),
                );
                let target_write_lane = target_write_lane_for_effective_row(
                    planned_write,
                    execution_mode,
                    &current_row,
                )?;
                let partition = partitions.partition_mut(execution_mode, target_write_lane);
                partition.authoritative_pre_state.push(row_ref.clone());
                partition.authoritative_pre_state_rows.push(
                    authoritative_pre_state_row_for_effective_row(
                        &current_row,
                        &authoritative_version_id,
                    ),
                );
                partition.intended_post_state.push(PlannedStateRow {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(target_version_id),
                    values,
                    tombstone: false,
                });
                partition.lineage.push(RowLineage {
                    entity_id: current_row.entity_id,
                    source_change_id: row_ref.source_change_id,
                    source_commit_id: None,
                });
            }
            Ok(partitions.into_resolved_write_plan(planned_write.command.requested_mode))
        }
        WriteOperationKind::Delete => {
            for current_row in current_rows {
                let execution_mode = resolve_execution_mode_for_effective_row(
                    planned_write.command.requested_mode,
                    &current_row,
                )?;
                let authoritative_version_id =
                    authoritative_version_id_for_effective_row(&current_row);
                let target_version_id = current_row.version_id.clone();
                let row_ref = ResolvedRowRef {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(authoritative_version_id.clone()),
                    source_change_id: current_row.source_change_id.clone(),
                    source_commit_id: None,
                };
                let target_write_lane = target_write_lane_for_effective_row(
                    planned_write,
                    execution_mode,
                    &current_row,
                )?;
                let partition = partitions.partition_mut(execution_mode, target_write_lane);
                partition.authoritative_pre_state.push(row_ref.clone());
                partition.intended_post_state.push(PlannedStateRow {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(target_version_id),
                    values: current_row.values,
                    tombstone: true,
                });
                partition.tombstones.push(row_ref.clone());
                partition.lineage.push(RowLineage {
                    entity_id: current_row.entity_id,
                    source_change_id: row_ref.source_change_id,
                    source_commit_id: None,
                });
            }
            Ok(partitions.into_resolved_write_plan(planned_write.command.requested_mode))
        }
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "public existing-row resolver does not handle inserts".to_string(),
        }),
    }
}

fn build_state_insert_rows(
    planned_write: &PlannedWrite,
) -> Result<Vec<PlannedStateRow>, WriteResolveError> {
    let payloads = payload_maps(planned_write)?;
    let single_row = payloads.len() == 1;
    let schema_key = resolved_schema_key(planned_write)?;
    let version_id = resolved_version_id(planned_write)?;
    let mut rows = Vec::with_capacity(payloads.len());

    for payload in payloads {
        let entity_id = payload
            .get("entity_id")
            .and_then(text_from_value)
            .or_else(|| {
                single_row
                    .then(|| resolved_entity_id(planned_write))
                    .transpose()
                    .ok()
                    .flatten()
            })
            .ok_or_else(|| WriteResolveError {
                message: "public write resolver requires an exact entity target".to_string(),
            })?;
        rows.push(build_state_insert_row(
            entity_id,
            schema_key.clone(),
            version_id.clone(),
            payload,
        ));
    }

    Ok(rows)
}

fn state_selector_targets_single_effective_row(planned_write: &PlannedWrite) -> bool {
    exact_selector_row_key(planned_write)
        .map(|row_key| {
            row_key.targets_single_effective_row(
                planned_write
                    .command
                    .target
                    .implicit_overrides
                    .expose_version_id,
            )
        })
        .unwrap_or(false)
}

fn exact_selector_row_key(
    planned_write: &PlannedWrite,
) -> Result<CanonicalStateRowKey, WriteResolveError> {
    let entity_id = exact_filter_text(
        &planned_write.command.selector.exact_filters,
        "entity_id",
        "public state selector requires text-compatible 'entity_id'",
    )?
    .ok_or_else(|| WriteResolveError {
        message: "public state selector requires an exact 'entity_id'".to_string(),
    })?;

    let mut row_key = CanonicalStateRowKey {
        entity_id,
        file_id: None,
        plugin_key: None,
        schema_version: None,
        version_id: None,
        global: None,
        untracked: None,
        writer_key: None,
    };

    for (column, value) in &planned_write.command.selector.exact_filters {
        assign_state_row_key_value(&mut row_key, column, value)?;
    }

    Ok(row_key)
}

async fn resolve_exact_state_target_rows(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    pending_transaction_view: Option<&PendingTransactionView>,
) -> Result<Vec<ExactEffectiveStateRow>, WriteResolveError> {
    let schema_key = resolved_schema_key(planned_write)?;
    let version_id = resolved_version_id(planned_write)?.ok_or_else(|| WriteResolveError {
        message: "public existing-row write resolver requires a concrete version_id".to_string(),
    })?;
    let current_row = resolve_exact_effective_state_row_with_pending_transaction_view(
        backend,
        &ExactEffectiveStateRowRequest {
            schema_key,
            version_id,
            row_key: exact_selector_row_key(planned_write)?,
            include_global_overlay: true,
            include_untracked_overlay: true,
        },
        pending_transaction_view,
    )
    .await
    .map_err(write_resolve_backend_error)?;
    Ok(current_row.into_iter().collect())
}

fn state_insert_row_key(row: &PlannedStateRow) -> CanonicalStateRowKey {
    CanonicalStateRowKey {
        entity_id: row.entity_id.clone(),
        file_id: row.values.get("file_id").and_then(text_from_value),
        plugin_key: row.values.get("plugin_key").and_then(text_from_value),
        schema_version: row.values.get("schema_version").and_then(text_from_value),
        version_id: row.version_id.clone(),
        global: row.values.get("global").and_then(bool_from_value),
        untracked: row.values.get("untracked").and_then(bool_from_value),
        writer_key: row.values.get("writer_key").and_then(text_from_value),
    }
}

async fn resolve_target_entity_rows(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
    pending_transaction_view: Option<&PendingTransactionView>,
) -> Result<Vec<ExactEffectiveStateRow>, WriteResolveError> {
    let selector_rows =
        query_entity_selector_rows(backend, planned_write, pending_transaction_view).await?;
    let mut rows = Vec::new();
    for selector_row in selector_rows {
        let version_id =
            selector_row_version_id(planned_write, selector_row.version_id.as_deref())?;
        let row_key = entity_state_row_key(planned_write, entity_schema, &selector_row.entity_id)?;
        let Some(current_row) = resolve_exact_effective_state_row_with_pending_transaction_view(
            backend,
            &ExactEffectiveStateRowRequest {
                schema_key: entity_schema.schema_key.clone(),
                version_id,
                row_key,
                include_global_overlay: true,
                include_untracked_overlay: true,
            },
            pending_transaction_view,
        )
        .await
        .map_err(write_resolve_backend_error)?
        else {
            continue;
        };
        rows.push(current_row);
    }
    Ok(rows)
}

async fn resolve_target_state_rows(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    pending_transaction_view: Option<&PendingTransactionView>,
) -> Result<Vec<ExactEffectiveStateRow>, WriteResolveError> {
    let schema_key = resolved_schema_key(planned_write)?;
    let selector_rows =
        query_state_selector_rows(backend, planned_write, pending_transaction_view).await?;
    let mut rows = Vec::new();
    for selector_row in selector_rows {
        let version_id =
            selector_row_version_id(planned_write, selector_row.version_id.as_deref())?;
        let Some(current_row) = resolve_exact_effective_state_row_with_pending_transaction_view(
            backend,
            &ExactEffectiveStateRowRequest {
                schema_key: schema_key.clone(),
                version_id,
                row_key: selector_row,
                include_global_overlay: true,
                include_untracked_overlay: true,
            },
            pending_transaction_view,
        )
        .await
        .map_err(write_resolve_backend_error)?
        else {
            continue;
        };
        rows.push(current_row);
    }
    Ok(rows)
}

fn selector_row_version_id(
    planned_write: &PlannedWrite,
    selector_version_id: Option<&str>,
) -> Result<String, WriteResolveError> {
    if let Some(version_id) = selector_version_id {
        return Ok(version_id.to_string());
    }
    resolved_version_id(planned_write)?.ok_or_else(|| WriteResolveError {
        message: "public write resolver requires a concrete version_id for the selected row"
            .to_string(),
    })
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
        if overrides.contains_key("lixcol_version_id") {
            return Err(crate::LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "schema '{}' uses removed x-lix-override-lixcols.lixcol_version_id support; use lixcol_global for global write scope",
                    schema_key
                ),
            });
        }
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
        schema,
        schema_key,
        schema_version,
        property_columns,
        primary_key_paths,
        state_defaults,
    })
}

fn reject_unsupported_entity_overrides(
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
) -> Result<(), WriteResolveError> {
    if entity_schema
        .state_defaults
        .get("global")
        .and_then(bool_from_value)
        == Some(true)
    {
        let version_id = resolved_version_id(planned_write)?;
        if version_id.as_deref() != Some(GLOBAL_VERSION_ID) {
            return Err(WriteResolveError {
                message:
                    "public entity write resolver requires a concrete global version_id for lixcol_global write overrides"
                        .to_string(),
            });
        }
    }
    if entity_schema
        .state_defaults
        .get("untracked")
        .and_then(bool_from_value)
        == Some(true)
    {
        return Err(WriteResolveError {
            message:
                "public entity live slice does not yet support lixcol_untracked write overrides"
                    .to_string(),
        });
    }
    Ok(())
}

fn entity_state_row_key(
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
    entity_id: &str,
) -> Result<CanonicalStateRowKey, WriteResolveError> {
    let mut row_key = CanonicalStateRowKey {
        entity_id: entity_id.to_string(),
        file_id: None,
        plugin_key: None,
        schema_version: None,
        version_id: None,
        global: None,
        untracked: None,
        writer_key: None,
    };
    for key in [
        "file_id",
        "plugin_key",
        "schema_version",
        "global",
        "untracked",
    ] {
        if let Some(value) = planned_write.command.selector.exact_filters.get(key) {
            assign_state_row_key_value(&mut row_key, key, value)?;
            continue;
        }
        if let Some(default) = entity_schema.state_defaults.get(key) {
            assign_state_row_key_value(&mut row_key, key, default)?;
        }
    }
    Ok(row_key)
}

fn entity_insert_row_key(
    entity_schema: &EntityWriteSchema,
    row: &PlannedStateRow,
) -> Result<CanonicalStateRowKey, WriteResolveError> {
    let mut row_key = CanonicalStateRowKey {
        entity_id: row.entity_id.clone(),
        file_id: None,
        plugin_key: None,
        schema_version: None,
        version_id: row.version_id.clone(),
        global: None,
        untracked: None,
        writer_key: None,
    };
    for key in [
        "file_id",
        "plugin_key",
        "schema_version",
        "global",
        "untracked",
    ] {
        if let Some(value) = row.values.get(key) {
            assign_state_row_key_value(&mut row_key, key, value)?;
            continue;
        }
        if let Some(default) = entity_schema.state_defaults.get(key) {
            assign_state_row_key_value(&mut row_key, key, default)?;
        }
    }
    Ok(row_key)
}

fn entity_state_column_name(column: &str) -> Option<&'static str> {
    match column.to_ascii_lowercase().as_str() {
        "lixcol_entity_id" => Some("entity_id"),
        "lixcol_schema_key" => Some("schema_key"),
        "lixcol_file_id" => Some("file_id"),
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
        description: format!("public entity resolver requires literal lixcol overrides: {error}"),
    })?;
    Ok(json_value_to_engine_value(&parsed))
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
        JsonValue::Array(_) | JsonValue::Object(_) => Value::Json(value.clone()),
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
