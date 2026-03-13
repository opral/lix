use crate::account::{
    active_account_file_id, active_account_plugin_key, active_account_schema_key,
    active_account_schema_version, active_account_snapshot_content,
    active_account_storage_version_id, parse_active_account_snapshot,
};
use crate::schema::builtin::builtin_schema_definition;
use crate::schema::{SchemaProvider, SqlStoredSchemaProvider};
use crate::sql::public::catalog::SurfaceFamily;
use crate::sql::public::planner::ir::{
    CanonicalStateRowKey, CanonicalStateSelector, InsertOnConflictAction, MutationPayload,
    PlannedStateRow, PlannedWrite, ResolvedRowRef, ResolvedWritePartition, ResolvedWritePlan,
    RowLineage, SchemaProof, ScopeProof, TargetSetProof, WriteLane, WriteMode, WriteModeRequest,
    WriteOperationKind,
};
use crate::sql::public::planner::semantics::effective_state_resolver::{
    resolve_exact_effective_state_row, ExactEffectiveStateRow, ExactEffectiveStateRowRequest,
};
use crate::sql::public::planner::semantics::filesystem_assignments::FilesystemAssignmentsError;
use crate::sql::public::planner::semantics::filesystem_planning::FilesystemPlanningError;
use crate::sql::public::planner::semantics::filesystem_queries::FilesystemQueryError;
use crate::sql::public::planner::semantics::state_assignments::{
    apply_entity_state_assignments, apply_state_assignments, assignments_from_payload,
    build_entity_insert_rows as build_entity_insert_rows_from_assignments, build_state_insert_row,
    ensure_identity_columns_preserved, EntityAssignmentsSemantics, EntityInsertSemantics,
    StateAssignmentsError,
};
use crate::sql::public::planner::semantics::surface_semantics::{
    public_selector_column_name, public_selector_version_column, OverlayLane,
};
use crate::sql::public::runtime::execute_public_read_query_strict;
use crate::version::{
    active_version_file_id, active_version_plugin_key, active_version_schema_key,
    active_version_schema_version, active_version_snapshot_content,
    active_version_storage_version_id, parse_active_version_snapshot, version_descriptor_file_id,
    version_descriptor_plugin_key, version_descriptor_schema_key,
    version_descriptor_schema_version, version_descriptor_snapshot_content,
    version_descriptor_storage_version_id, version_pointer_file_id, version_pointer_plugin_key,
    version_pointer_schema_key, version_pointer_schema_version, version_pointer_snapshot_content,
    version_pointer_storage_version_id, GLOBAL_VERSION_ID,
};
use crate::{LixBackend, LixError, QueryResult, Value};
use serde_json::Value as JsonValue;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Ident, ObjectName, ObjectNamePart, Query, Select,
    SelectFlavor, SelectItem, SetExpr, TableFactor, TableWithJoins, Value as SqlValue,
    ValueWithSpan,
};
use std::collections::BTreeMap;

mod filesystem_writes;

use filesystem_writes::resolve_filesystem_write;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteResolveError {
    pub(crate) message: String,
}

impl From<FilesystemQueryError> for WriteResolveError {
    fn from(error: FilesystemQueryError) -> Self {
        Self {
            message: error.message,
        }
    }
}

#[derive(Default, Clone)]
struct ResolvedWritePartitionBuilder {
    authoritative_pre_state: Vec<ResolvedRowRef>,
    intended_post_state: Vec<PlannedStateRow>,
    tombstones: Vec<ResolvedRowRef>,
    lineage: Vec<RowLineage>,
}

impl ResolvedWritePartitionBuilder {
    fn is_empty(&self) -> bool {
        self.authoritative_pre_state.is_empty()
            && self.intended_post_state.is_empty()
            && self.tombstones.is_empty()
            && self.lineage.is_empty()
    }

    fn into_partition(self, execution_mode: WriteMode) -> Option<ResolvedWritePartition> {
        (!self.is_empty()).then_some(ResolvedWritePartition {
            execution_mode,
            authoritative_pre_state: self.authoritative_pre_state,
            intended_post_state: self.intended_post_state,
            tombstones: self.tombstones,
            lineage: self.lineage,
            target_write_lane: None,
        })
    }
}

#[derive(Default, Clone)]
struct ResolvedWritePlanBuilder {
    partitions: Vec<ResolvedWritePlanEntry>,
}

#[derive(Clone)]
struct ResolvedWritePlanEntry {
    execution_mode: WriteMode,
    target_write_lane: Option<WriteLane>,
    partition: ResolvedWritePartitionBuilder,
}

impl ResolvedWritePlanBuilder {
    fn partition_mut(
        &mut self,
        execution_mode: WriteMode,
        target_write_lane: Option<WriteLane>,
    ) -> &mut ResolvedWritePartitionBuilder {
        if let Some(index) = self.partitions.iter().position(|entry| {
            entry.execution_mode == execution_mode && entry.target_write_lane == target_write_lane
        }) {
            return &mut self.partitions[index].partition;
        }
        self.partitions.push(ResolvedWritePlanEntry {
            execution_mode,
            target_write_lane,
            partition: ResolvedWritePartitionBuilder::default(),
        });
        &mut self
            .partitions
            .last_mut()
            .expect("partition entry was just pushed")
            .partition
    }

    fn into_resolved_write_plan(self, requested_mode: WriteModeRequest) -> ResolvedWritePlan {
        let mut partitions = Vec::new();
        for entry in self.partitions {
            if let Some(mut partition) = entry.partition.into_partition(entry.execution_mode) {
                partition.target_write_lane = entry.target_write_lane;
                partitions.push(partition);
            }
        }
        if partitions.is_empty() {
            return noop_resolved_write_plan(default_execution_mode_for_request(requested_mode));
        }
        ResolvedWritePlan::from_partitions(partitions)
    }
}

pub(crate) async fn resolve_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let resolved = match planned_write.command.target.descriptor.surface_family {
        SurfaceFamily::State => match planned_write.command.operation_kind {
            WriteOperationKind::Insert => {
                resolve_state_insert_write_plan(backend, planned_write).await
            }
            WriteOperationKind::Update | WriteOperationKind::Delete => {
                resolve_existing_state_write(backend, planned_write).await
            }
        },
        SurfaceFamily::Entity => {
            let mut provider = SqlStoredSchemaProvider::new(backend);
            let entity_schema = load_entity_schema(&mut provider, planned_write)
                .await
                .map_err(write_resolve_backend_error)?;
            match planned_write.command.operation_kind {
                WriteOperationKind::Insert => {
                    resolve_entity_insert_write_plan(backend, planned_write, &entity_schema).await
                }
                WriteOperationKind::Update | WriteOperationKind::Delete => {
                    resolve_existing_entity_write(backend, planned_write, &entity_schema).await
                }
            }
        }
        SurfaceFamily::Admin => resolve_admin_write(backend, planned_write).await,
        SurfaceFamily::Filesystem => resolve_filesystem_write(backend, planned_write).await,
        SurfaceFamily::Change => Err(WriteResolveError {
            message: format!(
                "public write resolver does not support '{}' writes",
                planned_write.command.target.descriptor.public_name
            ),
        }),
    }?;

    finalize_resolved_write_plan(planned_write, resolved)
}

async fn resolve_state_insert_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let entity_id = resolved_entity_id(planned_write)?;
    let schema_key = resolved_schema_key(planned_write)?;
    let version_id = resolved_version_id(planned_write)?;
    let row = build_state_insert_row(
        entity_id.clone(),
        schema_key,
        version_id,
        payload_map(planned_write)?,
    );

    if insert_on_conflict_action(planned_write) == Some(InsertOnConflictAction::DoNothing)
        && state_insert_conflicts_with_existing_row(backend, &row).await?
    {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    }

    Ok(single_partition_write_plan(
        default_execution_mode_for_request(planned_write.command.requested_mode),
        Vec::new(),
        vec![row],
        Vec::new(),
        vec![RowLineage {
            entity_id,
            source_change_id: None,
            source_commit_id: None,
        }],
    ))
}

async fn resolve_existing_state_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    if planned_write.command.selector.exact_only
        && state_selector_targets_single_effective_row(planned_write)
    {
        return resolve_existing_state_write_exact(backend, planned_write).await;
    }
    if !supports_selector_driven_state_resolution(planned_write) {
        return Err(WriteResolveError {
            message: "public update/delete resolver only supports exact conjunctive selectors"
                .to_string(),
        });
    }
    let current_rows = resolve_target_state_rows(backend, planned_write).await?;
    resolve_existing_state_write_from_rows(planned_write, current_rows)
}

fn state_selector_targets_single_effective_row(planned_write: &PlannedWrite) -> bool {
    exact_selector_row_key(planned_write)
        .map(|row_key| {
            row_key.targets_single_effective_row(state_selector_exposes_version_id(planned_write))
        })
        .unwrap_or(false)
}

fn state_selector_exposes_version_id(planned_write: &PlannedWrite) -> bool {
    planned_write
        .command
        .target
        .implicit_overrides
        .expose_version_id
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

async fn resolve_existing_state_write_exact(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let schema_key = resolved_schema_key(planned_write)?;
    let version_id = resolved_version_id(planned_write)?.ok_or_else(|| WriteResolveError {
        message: "public existing-row write resolver requires a concrete version_id".to_string(),
    })?;
    let Some(current_row) = resolve_exact_effective_state_row(
        backend,
        &ExactEffectiveStateRowRequest {
            schema_key: schema_key.clone(),
            version_id,
            row_key: exact_selector_row_key(planned_write)?,
            include_global_overlay: true,
            include_untracked_overlay: true,
        },
    )
    .await
    .map_err(write_resolve_backend_error)?
    else {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    };
    resolve_existing_state_write_from_rows(planned_write, vec![current_row])
}

fn resolve_existing_state_write_from_rows(
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
    .then(|| assignments_from_payload(&planned_write.command.payload, "public update resolver"))
    .transpose()
    .map_err(write_resolve_state_assignments_error)?;

    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let assignments = assignments.as_ref().expect("update assignments prepared");
            for current_row in current_rows {
                let execution_mode = resolve_execution_mode_for_effective_row(
                    planned_write.command.requested_mode,
                    &current_row,
                )?;
                let values = apply_state_assignments(&current_row.values, assignments);
                ensure_identity_columns_preserved(
                    &current_row.entity_id,
                    &current_row.schema_key,
                    &current_row.file_id,
                    &current_row.version_id,
                    &values,
                )
                .map_err(write_resolve_state_assignments_error)?;
                let row_ref = ResolvedRowRef {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
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
                    version_id: Some(current_row.version_id.clone()),
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
                let row_ref = ResolvedRowRef {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
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
                    version_id: Some(current_row.version_id.clone()),
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

async fn resolve_existing_entity_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    reject_unsupported_entity_overrides(planned_write, entity_schema)?;
    let current_rows = resolve_target_entity_rows(backend, planned_write, entity_schema).await?;
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
    .then(|| {
        assignments_from_payload(
            &planned_write.command.payload,
            "public entity update resolver",
        )
    })
    .transpose()
    .map_err(write_resolve_state_assignments_error)?;

    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let assignments = assignments.as_ref().expect("update assignments prepared");
            for current_row in current_rows {
                let execution_mode = resolve_execution_mode_for_effective_row(
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
                let values = apply_entity_state_assignments(
                    &current_row,
                    assignments,
                    EntityAssignmentsSemantics {
                        property_columns: &entity_schema.property_columns,
                        primary_key_paths: &entity_schema.primary_key_paths,
                    },
                )
                .map_err(write_resolve_state_assignments_error)?;
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
                    version_id: Some(current_row.version_id.clone()),
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
                let row_ref = ResolvedRowRef {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
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
                    version_id: Some(current_row.version_id.clone()),
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
            message: "public entity existing-row resolver does not handle inserts".to_string(),
        }),
    }
}

async fn resolve_entity_insert_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    reject_unsupported_entity_overrides(planned_write, entity_schema)?;
    let rows = build_entity_insert_rows_from_assignments(
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
    )
    .map_err(write_resolve_state_assignments_error)?;
    let mut partitions = ResolvedWritePlanBuilder::default();
    let default_execution_mode =
        default_execution_mode_for_request(planned_write.command.requested_mode);

    for row in rows {
        if let Some(conflict) = planned_write.command.on_conflict.as_ref() {
            let row_key = entity_insert_row_key(entity_schema, &row)?;
            if let Some(current_row) = resolve_exact_effective_state_row(
                backend,
                &ExactEffectiveStateRowRequest {
                    schema_key: entity_schema.schema_key.clone(),
                    version_id: row.version_id.clone().ok_or_else(|| WriteResolveError {
                        message: "public entity insert resolver requires a concrete version_id"
                            .to_string(),
                    })?,
                    row_key,
                    include_global_overlay: true,
                    include_untracked_overlay: true,
                },
            )
            .await
            .map_err(write_resolve_backend_error)?
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

#[derive(Debug, Clone)]
struct VersionAdminRow {
    id: String,
    name: String,
    hidden: bool,
    commit_id: String,
    descriptor_change_id: Option<String>,
    pointer_change_id: Option<String>,
}

#[derive(Debug, Clone)]
struct ActiveVersionAdminRow {
    id: String,
    version_id: String,
}

#[derive(Debug, Clone)]
struct ActiveAccountAdminRow {
    account_id: String,
}

async fn resolve_admin_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    match planned_write.command.target.descriptor.public_name.as_str() {
        "lix_active_version" => match planned_write.command.operation_kind {
            WriteOperationKind::Update => {
                resolve_active_version_update_write_plan(backend, planned_write).await
            }
            _ => Err(WriteResolveError {
                message: "public write resolver only supports UPDATE for 'lix_active_version'"
                    .to_string(),
            }),
        },
        "lix_active_account" => match planned_write.command.operation_kind {
            WriteOperationKind::Insert => resolve_active_account_insert_write_plan(planned_write),
            WriteOperationKind::Delete => {
                resolve_active_account_delete_write_plan(backend, planned_write).await
            }
            WriteOperationKind::Update => Err(WriteResolveError {
                message: "public write resolver does not support UPDATE for 'lix_active_account'"
                    .to_string(),
            }),
        },
        "lix_version" => match planned_write.command.operation_kind {
            WriteOperationKind::Insert => {
                resolve_version_insert_write_plan(backend, planned_write).await
            }
            WriteOperationKind::Update | WriteOperationKind::Delete => {
                resolve_existing_version_write(backend, planned_write).await
            }
        },
        other => Err(WriteResolveError {
            message: format!(
                "public write resolver does not yet support '{}' writes",
                other
            ),
        }),
    }
}

async fn resolve_active_version_update_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let MutationPayload::Patch(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "public active-version update resolver requires a patch payload".to_string(),
        });
    };
    if payload.contains_key("id") {
        return Err(WriteResolveError {
            message: "public active-version update cannot modify id".to_string(),
        });
    }
    if payload.keys().any(|key| key != "version_id") {
        return Err(WriteResolveError {
            message: "public active-version update only supports version_id assignments"
                .to_string(),
        });
    }
    let next_version_id =
        payload_text_value(planned_write, "version_id").ok_or_else(|| WriteResolveError {
            message: "public active-version update must set version_id".to_string(),
        })?;
    if next_version_id.is_empty() {
        return Err(WriteResolveError {
            message: "public active-version update cannot set empty version_id".to_string(),
        });
    }
    let version_exists = load_version_admin_row(backend, &next_version_id)
        .await
        .map_err(write_resolve_backend_error)?
        .is_some();
    if !version_exists {
        return Err(WriteResolveError {
            message: format!(
                "Foreign key constraint violation: lix_active_version.version_id '{}' references missing lix_version_descriptor.id",
                next_version_id
            ),
        });
    }

    let matching_ids = query_text_selector_values_for_write_selector(
        backend,
        planned_write,
        "id",
        "public active-version selector resolver expected id text rows",
    )
    .await?;
    let current_rows = load_active_version_admin_rows(backend)
        .await
        .map_err(write_resolve_backend_error)?;
    let matching_rows = current_rows
        .into_iter()
        .filter(|row| matching_ids.iter().any(|id| id == &row.id))
        .collect::<Vec<_>>();
    if matching_rows.is_empty() {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    }

    let authoritative_pre_state = matching_rows
        .iter()
        .map(active_version_admin_pre_state_ref)
        .collect::<Vec<_>>();
    let intended_post_state = matching_rows
        .iter()
        .map(|row| active_version_admin_row(&row.id, &next_version_id))
        .collect::<Vec<_>>();
    let lineage = matching_rows
        .into_iter()
        .map(|row| RowLineage {
            entity_id: row.id,
            source_change_id: None,
            source_commit_id: None,
        })
        .collect::<Vec<_>>();

    Ok(single_partition_write_plan(
        default_execution_mode_for_request(planned_write.command.requested_mode),
        authoritative_pre_state,
        intended_post_state,
        Vec::new(),
        lineage,
    ))
}

fn resolve_active_account_insert_write_plan(
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let MutationPayload::FullSnapshot(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "public active-account insert resolver requires a full payload".to_string(),
        });
    };
    if payload.keys().any(|key| key != "account_id") {
        return Err(WriteResolveError {
            message: "public active-account insert only supports the account_id column".to_string(),
        });
    }
    let account_id =
        payload_text_value(planned_write, "account_id").ok_or_else(|| WriteResolveError {
            message: "public active-account insert requires column 'account_id'".to_string(),
        })?;
    if account_id.is_empty() {
        return Err(WriteResolveError {
            message: "public active-account insert requires non-empty account_id".to_string(),
        });
    }

    Ok(single_partition_write_plan(
        default_execution_mode_for_request(planned_write.command.requested_mode),
        Vec::new(),
        vec![active_account_admin_row(&account_id)],
        Vec::new(),
        vec![RowLineage {
            entity_id: account_id,
            source_change_id: None,
            source_commit_id: None,
        }],
    ))
}

async fn resolve_active_account_delete_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let matching_account_ids = query_text_selector_values_for_write_selector(
        backend,
        planned_write,
        "account_id",
        "public active-account selector resolver expected account_id text rows",
    )
    .await?;
    let current_rows = load_active_account_admin_rows(backend)
        .await
        .map_err(write_resolve_backend_error)?;
    let matching_rows = current_rows
        .into_iter()
        .filter(|row| {
            matching_account_ids
                .iter()
                .any(|account_id| account_id == &row.account_id)
        })
        .collect::<Vec<_>>();
    if matching_rows.is_empty() {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    }

    let authoritative_pre_state = matching_rows
        .iter()
        .map(active_account_admin_pre_state_ref)
        .collect::<Vec<_>>();
    let intended_post_state = matching_rows
        .iter()
        .map(|row| active_account_admin_tombstone_row(&row.account_id))
        .collect::<Vec<_>>();
    let tombstones = matching_rows
        .iter()
        .map(active_account_admin_pre_state_ref)
        .collect::<Vec<_>>();
    let lineage = matching_rows
        .into_iter()
        .map(|row| RowLineage {
            entity_id: row.account_id,
            source_change_id: None,
            source_commit_id: None,
        })
        .collect::<Vec<_>>();

    Ok(single_partition_write_plan(
        default_execution_mode_for_request(planned_write.command.requested_mode),
        authoritative_pre_state,
        intended_post_state,
        tombstones,
        lineage,
    ))
}

async fn load_active_version_admin_rows(
    backend: &dyn LixBackend,
) -> Result<Vec<ActiveVersionAdminRow>, crate::LixError> {
    let sql = format!(
        "SELECT entity_id, snapshot_content \
         FROM lix_internal_state_untracked \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{storage_version_id}' \
           AND snapshot_content IS NOT NULL \
         ORDER BY updated_at DESC, entity_id ASC",
        schema_key = active_version_schema_key(),
        file_id = active_version_file_id(),
        storage_version_id = active_version_storage_version_id(),
    );
    let result = backend.execute(&sql, &[]).await?;
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let Some(id) = row.first().and_then(text_from_value) else {
            continue;
        };
        let Some(snapshot_content) = row.get(1).and_then(text_from_value) else {
            continue;
        };
        let version_id = parse_active_version_snapshot(&snapshot_content)?;
        rows.push(ActiveVersionAdminRow { id, version_id });
    }
    Ok(rows)
}

fn active_version_admin_pre_state_ref(row: &ActiveVersionAdminRow) -> ResolvedRowRef {
    ResolvedRowRef {
        entity_id: row.id.clone(),
        schema_key: active_version_schema_key().to_string(),
        version_id: Some(active_version_storage_version_id().to_string()),
        source_change_id: None,
        source_commit_id: None,
    }
}

fn active_version_admin_row(id: &str, version_id: &str) -> PlannedStateRow {
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(active_version_schema_key().to_string()),
    );
    values.insert(
        "file_id".to_string(),
        Value::Text(active_version_file_id().to_string()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(active_version_plugin_key().to_string()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(active_version_schema_version().to_string()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(active_version_snapshot_content(id, version_id)),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(active_version_storage_version_id().to_string()),
    );
    values.insert("global".to_string(), Value::Boolean(true));
    PlannedStateRow {
        entity_id: id.to_string(),
        schema_key: active_version_schema_key().to_string(),
        version_id: Some(active_version_storage_version_id().to_string()),
        values,
        tombstone: false,
    }
}

async fn load_active_account_admin_rows(
    backend: &dyn LixBackend,
) -> Result<Vec<ActiveAccountAdminRow>, crate::LixError> {
    let sql = format!(
        "SELECT entity_id, snapshot_content \
         FROM lix_internal_state_untracked \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{storage_version_id}' \
           AND snapshot_content IS NOT NULL \
         ORDER BY updated_at DESC, entity_id ASC",
        schema_key = active_account_schema_key(),
        file_id = active_account_file_id(),
        storage_version_id = active_account_storage_version_id(),
    );
    let result = backend.execute(&sql, &[]).await?;
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let Some(snapshot_content) = row.get(1).and_then(text_from_value) else {
            continue;
        };
        let account_id = parse_active_account_snapshot(&snapshot_content)?;
        rows.push(ActiveAccountAdminRow { account_id });
    }
    Ok(rows)
}

fn active_account_admin_pre_state_ref(row: &ActiveAccountAdminRow) -> ResolvedRowRef {
    ResolvedRowRef {
        entity_id: row.account_id.clone(),
        schema_key: active_account_schema_key().to_string(),
        version_id: Some(active_account_storage_version_id().to_string()),
        source_change_id: None,
        source_commit_id: None,
    }
}

fn active_account_admin_row(account_id: &str) -> PlannedStateRow {
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(account_id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(active_account_schema_key().to_string()),
    );
    values.insert(
        "file_id".to_string(),
        Value::Text(active_account_file_id().to_string()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(active_account_plugin_key().to_string()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(active_account_schema_version().to_string()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(active_account_snapshot_content(account_id)),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(active_account_storage_version_id().to_string()),
    );
    values.insert("global".to_string(), Value::Boolean(true));
    PlannedStateRow {
        entity_id: account_id.to_string(),
        schema_key: active_account_schema_key().to_string(),
        version_id: Some(active_account_storage_version_id().to_string()),
        values,
        tombstone: false,
    }
}

fn active_account_admin_tombstone_row(account_id: &str) -> PlannedStateRow {
    let mut row = active_account_admin_row(account_id);
    row.values.remove("snapshot_content");
    row.tombstone = true;
    row
}

async fn resolve_version_insert_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let rows = payload_maps(planned_write)?;
    let mut authoritative_pre_state = Vec::new();
    let mut intended_post_state = Vec::new();
    let mut lineage = Vec::new();

    for row in rows {
        let version_id = version_admin_id_from_payload_map(&row)?;
        let name = version_admin_required_text_from_payload_map(&row, "name")?;
        let commit_id = version_admin_required_text_from_payload_map(&row, "commit_id")?;
        let hidden = version_admin_hidden_from_payload_map(&row)?;
        let existing = load_version_admin_row(backend, &version_id)
            .await
            .map_err(write_resolve_backend_error)?;

        if let Some(existing) = existing.as_ref() {
            authoritative_pre_state.extend(version_admin_pre_state_refs(existing));
        }
        intended_post_state.push(version_descriptor_row(&version_id, &name, hidden));
        intended_post_state.push(version_pointer_row(&version_id, &commit_id));
        lineage.push(RowLineage {
            entity_id: version_id,
            source_change_id: None,
            source_commit_id: None,
        });
    }

    Ok(single_partition_write_plan(
        default_execution_mode_for_request(planned_write.command.requested_mode),
        authoritative_pre_state,
        intended_post_state,
        Vec::new(),
        lineage,
    ))
}

async fn resolve_existing_version_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let version_ids = query_text_selector_values_for_write_selector(
        backend,
        planned_write,
        "id",
        "public version selector resolver expected id text rows",
    )
    .await?;
    if version_ids.is_empty() {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    }

    let mut current_rows = Vec::new();
    for version_id in version_ids {
        let Some(current_row) = load_version_admin_row(backend, &version_id)
            .await
            .map_err(write_resolve_backend_error)?
        else {
            continue;
        };
        current_rows.push(current_row);
    }
    if current_rows.is_empty() {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    }

    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let MutationPayload::Patch(payload) = &planned_write.command.payload else {
                return Err(WriteResolveError {
                    message: "public version update resolver requires a patch payload".to_string(),
                });
            };
            if payload.contains_key("id") {
                return Err(WriteResolveError {
                    message: "public version update cannot modify id".to_string(),
                });
            }
            let mut authoritative_pre_state = Vec::new();
            let mut intended_post_state = Vec::new();
            let mut lineage = Vec::new();

            for current_row in current_rows {
                let next_name = payload
                    .get("name")
                    .and_then(text_from_value)
                    .unwrap_or_else(|| current_row.name.clone());
                if next_name.is_empty() {
                    return Err(WriteResolveError {
                        message: "public version update cannot set empty name".to_string(),
                    });
                }
                let next_hidden = payload
                    .get("hidden")
                    .and_then(value_as_bool)
                    .unwrap_or(current_row.hidden);
                let next_commit_id = payload
                    .get("commit_id")
                    .and_then(text_from_value)
                    .unwrap_or_else(|| current_row.commit_id.clone());
                if next_commit_id.is_empty() {
                    return Err(WriteResolveError {
                        message: "public version update cannot set empty commit_id".to_string(),
                    });
                }

                authoritative_pre_state.extend(version_admin_pre_state_refs(&current_row));
                lineage.push(RowLineage {
                    entity_id: current_row.id.clone(),
                    source_change_id: current_row
                        .descriptor_change_id
                        .clone()
                        .or_else(|| current_row.pointer_change_id.clone()),
                    source_commit_id: None,
                });
                if payload.contains_key("name") || payload.contains_key("hidden") {
                    intended_post_state.push(version_descriptor_row(
                        &current_row.id,
                        &next_name,
                        next_hidden,
                    ));
                }
                if payload.contains_key("commit_id") {
                    intended_post_state.push(version_pointer_row(&current_row.id, &next_commit_id));
                }
            }

            Ok(single_partition_write_plan(
                default_execution_mode_for_request(planned_write.command.requested_mode),
                authoritative_pre_state,
                intended_post_state,
                Vec::new(),
                lineage,
            ))
        }
        WriteOperationKind::Delete => {
            let mut authoritative_pre_state = Vec::new();
            let mut intended_post_state = Vec::new();
            let mut tombstones = Vec::new();
            let mut lineage = Vec::new();
            for current_row in current_rows {
                authoritative_pre_state.extend(version_admin_pre_state_refs(&current_row));
                intended_post_state.push(version_descriptor_tombstone_row(&current_row.id));
                intended_post_state.push(version_pointer_tombstone_row(&current_row.id));
                tombstones.extend(version_admin_tombstone_refs(&current_row));
                lineage.push(RowLineage {
                    entity_id: current_row.id.clone(),
                    source_change_id: current_row
                        .descriptor_change_id
                        .clone()
                        .or_else(|| current_row.pointer_change_id.clone()),
                    source_commit_id: None,
                });
            }
            Ok(single_partition_write_plan(
                default_execution_mode_for_request(planned_write.command.requested_mode),
                authoritative_pre_state,
                intended_post_state,
                tombstones,
                lineage,
            ))
        }
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "public version existing-row resolver does not handle inserts".to_string(),
        }),
    }
}

async fn load_version_admin_row(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<VersionAdminRow>, crate::LixError> {
    let descriptor_sql = format!(
        "SELECT snapshot_content, change_id \
         FROM lix_internal_state_materialized_v1_lix_version_descriptor \
         WHERE schema_key = '{schema_key}' \
           AND entity_id = '{entity_id}' \
           AND file_id = '{file_id}' \
           AND plugin_key = '{plugin_key}' \
           AND version_id = '{storage_version_id}' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL \
         LIMIT 1",
        schema_key = version_descriptor_schema_key(),
        entity_id = version_id.replace('\'', "''"),
        file_id = version_descriptor_file_id(),
        plugin_key = version_descriptor_plugin_key(),
        storage_version_id = version_descriptor_storage_version_id(),
    );
    let descriptor_result = backend.execute(&descriptor_sql, &[]).await?;
    let Some(descriptor_row) = descriptor_result.rows.first() else {
        return Ok(None);
    };
    let pointer_sql = format!(
        "SELECT snapshot_content, change_id \
         FROM lix_internal_state_materialized_v1_lix_version_pointer \
         WHERE schema_key = '{schema_key}' \
           AND entity_id = '{entity_id}' \
           AND file_id = '{file_id}' \
           AND plugin_key = '{plugin_key}' \
           AND version_id = '{storage_version_id}' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL \
         LIMIT 1",
        schema_key = version_pointer_schema_key(),
        entity_id = version_id.replace('\'', "''"),
        file_id = version_pointer_file_id(),
        plugin_key = version_pointer_plugin_key(),
        storage_version_id = version_pointer_storage_version_id(),
    );
    let pointer_result = backend.execute(&pointer_sql, &[]).await?;
    let pointer_row = pointer_result.rows.first();
    Ok(Some(VersionAdminRow {
        id: version_id.to_string(),
        name: row_snapshot_name(descriptor_row).unwrap_or_default(),
        hidden: row_snapshot_hidden(descriptor_row).unwrap_or(false),
        commit_id: pointer_row
            .and_then(|row| row_snapshot_commit_id(row))
            .unwrap_or_default(),
        descriptor_change_id: descriptor_row.get(1).and_then(text_from_value),
        pointer_change_id: pointer_row
            .and_then(|row| row.get(1))
            .and_then(text_from_value),
    }))
}

fn version_admin_pre_state_refs(row: &VersionAdminRow) -> Vec<ResolvedRowRef> {
    vec![
        ResolvedRowRef {
            entity_id: row.id.clone(),
            schema_key: version_descriptor_schema_key().to_string(),
            version_id: Some(GLOBAL_VERSION_ID.to_string()),
            source_change_id: row.descriptor_change_id.clone(),
            source_commit_id: None,
        },
        ResolvedRowRef {
            entity_id: row.id.clone(),
            schema_key: version_pointer_schema_key().to_string(),
            version_id: Some(GLOBAL_VERSION_ID.to_string()),
            source_change_id: row.pointer_change_id.clone(),
            source_commit_id: None,
        },
    ]
}

fn version_admin_tombstone_refs(row: &VersionAdminRow) -> Vec<ResolvedRowRef> {
    version_admin_pre_state_refs(row)
}

fn version_admin_id_from_payload_map(
    payload: &BTreeMap<String, Value>,
) -> Result<String, WriteResolveError> {
    payload
        .get("id")
        .and_then(text_from_value)
        .ok_or_else(|| WriteResolveError {
            message: "public version insert requires column 'id'".to_string(),
        })
}

fn version_admin_required_text_from_payload_map(
    payload: &BTreeMap<String, Value>,
    key: &str,
) -> Result<String, WriteResolveError> {
    let value = payload
        .get(key)
        .and_then(text_from_value)
        .ok_or_else(|| WriteResolveError {
            message: format!("public version insert requires column '{key}'"),
        })?;
    if value.is_empty() {
        return Err(WriteResolveError {
            message: format!("public version insert cannot set empty {key}"),
        });
    }
    Ok(value)
}

fn version_admin_hidden_from_payload_map(
    payload: &BTreeMap<String, Value>,
) -> Result<bool, WriteResolveError> {
    Ok(payload
        .get("hidden")
        .and_then(value_as_bool)
        .unwrap_or(false))
}

fn version_descriptor_row(id: &str, name: &str, hidden: bool) -> PlannedStateRow {
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(version_descriptor_schema_key().to_string()),
    );
    values.insert(
        "file_id".to_string(),
        Value::Text(version_descriptor_file_id().to_string()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(version_descriptor_plugin_key().to_string()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(version_descriptor_schema_version().to_string()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(version_descriptor_snapshot_content(id, name, hidden)),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(GLOBAL_VERSION_ID.to_string()),
    );
    PlannedStateRow {
        entity_id: id.to_string(),
        schema_key: version_descriptor_schema_key().to_string(),
        version_id: Some(GLOBAL_VERSION_ID.to_string()),
        values,
        tombstone: false,
    }
}

fn version_pointer_row(id: &str, commit_id: &str) -> PlannedStateRow {
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(version_pointer_schema_key().to_string()),
    );
    values.insert(
        "file_id".to_string(),
        Value::Text(version_pointer_file_id().to_string()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(version_pointer_plugin_key().to_string()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(version_pointer_schema_version().to_string()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(version_pointer_snapshot_content(id, commit_id)),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(GLOBAL_VERSION_ID.to_string()),
    );
    PlannedStateRow {
        entity_id: id.to_string(),
        schema_key: version_pointer_schema_key().to_string(),
        version_id: Some(GLOBAL_VERSION_ID.to_string()),
        values,
        tombstone: false,
    }
}

fn version_descriptor_tombstone_row(id: &str) -> PlannedStateRow {
    let mut row = version_descriptor_row(id, id, false);
    row.values.remove("snapshot_content");
    row.tombstone = true;
    row
}

fn version_pointer_tombstone_row(id: &str) -> PlannedStateRow {
    let mut row = version_pointer_row(id, "deleted");
    row.values.remove("snapshot_content");
    row.tombstone = true;
    row
}

fn ensure_exact_selector(planned_write: &PlannedWrite) -> Result<(), WriteResolveError> {
    if !planned_write.command.selector.exact_only {
        return Err(WriteResolveError {
            message: "public update/delete resolver only supports exact conjunctive selectors"
                .to_string(),
        });
    }
    Ok(())
}

fn default_execution_mode_for_request(requested_mode: WriteModeRequest) -> WriteMode {
    match requested_mode {
        WriteModeRequest::Auto | WriteModeRequest::ForceTracked => WriteMode::Tracked,
        WriteModeRequest::ForceUntracked => WriteMode::Untracked,
    }
}

fn single_partition_write_plan(
    execution_mode: WriteMode,
    authoritative_pre_state: Vec<ResolvedRowRef>,
    intended_post_state: Vec<PlannedStateRow>,
    tombstones: Vec<ResolvedRowRef>,
    lineage: Vec<RowLineage>,
) -> ResolvedWritePlan {
    ResolvedWritePlan::from_partition(ResolvedWritePartition {
        execution_mode,
        authoritative_pre_state,
        intended_post_state,
        tombstones,
        lineage,
        target_write_lane: None,
    })
}

fn execution_mode_for_overlay_lane(overlay_lane: OverlayLane) -> WriteMode {
    match overlay_lane {
        OverlayLane::LocalTracked | OverlayLane::GlobalTracked => WriteMode::Tracked,
        OverlayLane::LocalUntracked | OverlayLane::GlobalUntracked => WriteMode::Untracked,
    }
}

fn resolve_execution_mode_for_untracked_flag(
    requested_mode: WriteModeRequest,
    untracked: bool,
    tracked_error: &str,
    untracked_error: &str,
) -> Result<WriteMode, WriteResolveError> {
    let execution_mode = if untracked {
        WriteMode::Untracked
    } else {
        WriteMode::Tracked
    };
    match (requested_mode, execution_mode) {
        (WriteModeRequest::ForceTracked, WriteMode::Untracked) => Err(WriteResolveError {
            message: tracked_error.to_string(),
        }),
        (WriteModeRequest::ForceUntracked, WriteMode::Tracked) => Err(WriteResolveError {
            message: untracked_error.to_string(),
        }),
        _ => Ok(execution_mode),
    }
}

fn resolve_execution_mode_for_effective_row(
    requested_mode: WriteModeRequest,
    current_row: &ExactEffectiveStateRow,
) -> Result<WriteMode, WriteResolveError> {
    let execution_mode = execution_mode_for_overlay_lane(current_row.overlay_lane);
    match (requested_mode, execution_mode) {
        (WriteModeRequest::ForceTracked, WriteMode::Untracked) => Err(WriteResolveError {
            message: format!(
                "public tracked write requires a tracked effective-state winner, found {:?}",
                current_row.overlay_lane
            ),
        }),
        (WriteModeRequest::ForceUntracked, WriteMode::Tracked) => Err(WriteResolveError {
            message: format!(
                "public untracked write requires an untracked effective-state winner, found {:?}",
                current_row.overlay_lane
            ),
        }),
        _ => Ok(execution_mode),
    }
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

fn target_write_lane_for_effective_row(
    planned_write: &PlannedWrite,
    execution_mode: WriteMode,
    current_row: &ExactEffectiveStateRow,
) -> Result<Option<WriteLane>, WriteResolveError> {
    target_write_lane_for_version(
        planned_write,
        execution_mode,
        Some(current_row.version_id.as_str()),
    )
}

fn target_write_lane_for_planned_row(
    planned_write: &PlannedWrite,
    execution_mode: WriteMode,
    version_id: Option<&str>,
) -> Result<Option<WriteLane>, WriteResolveError> {
    target_write_lane_for_version(planned_write, execution_mode, version_id)
}

fn target_write_lane_for_version(
    planned_write: &PlannedWrite,
    execution_mode: WriteMode,
    version_id: Option<&str>,
) -> Result<Option<WriteLane>, WriteResolveError> {
    if execution_mode == WriteMode::Untracked {
        return Ok(None);
    }
    match &planned_write.scope_proof {
        ScopeProof::ActiveVersion => Ok(Some(WriteLane::ActiveVersion)),
        ScopeProof::SingleVersion(version_id) => Ok(Some(WriteLane::SingleVersion(
            version_id.clone(),
        ))),
        ScopeProof::FiniteVersionSet(_) => version_id
            .map(|version_id| Some(WriteLane::SingleVersion(version_id.to_string())))
            .ok_or_else(|| WriteResolveError {
                message:
                    "public tracked write could not determine a concrete version lane for a selected row"
                        .to_string(),
            }),
        ScopeProof::GlobalAdmin => Ok(Some(WriteLane::GlobalAdmin)),
        ScopeProof::Unknown | ScopeProof::Unbounded => version_id
            .map(|version_id| Some(WriteLane::SingleVersion(version_id.to_string())))
            .ok_or_else(|| WriteResolveError {
                message: "public tracked write requires a bounded version lane".to_string(),
            }),
    }
}

fn resolve_execution_mode_for_effective_rows(
    requested_mode: WriteModeRequest,
    current_rows: &[ExactEffectiveStateRow],
) -> Result<WriteMode, WriteResolveError> {
    let mut resolved_mode = None;
    for current_row in current_rows {
        let row_mode = resolve_execution_mode_for_effective_row(requested_mode, current_row)?;
        ensure_consistent_execution_mode(
            &mut resolved_mode,
            row_mode,
            "public write resolver does not yet support mixing tracked and untracked effective-state winners",
        )?;
    }
    Ok(resolved_mode.unwrap_or(default_execution_mode_for_request(requested_mode)))
}

fn ensure_consistent_execution_mode(
    resolved_mode: &mut Option<WriteMode>,
    row_mode: WriteMode,
    mixed_mode_error: &str,
) -> Result<(), WriteResolveError> {
    match resolved_mode {
        Some(existing) if *existing != row_mode => Err(WriteResolveError {
            message: mixed_mode_error.to_string(),
        }),
        Some(_) => Ok(()),
        None => {
            *resolved_mode = Some(row_mode);
            Ok(())
        }
    }
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
        .and_then(value_as_bool)
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
        .and_then(value_as_bool)
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

async fn resolve_target_entity_rows(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
) -> Result<Vec<ExactEffectiveStateRow>, WriteResolveError> {
    let selector_rows = query_entity_selector_rows(backend, planned_write).await?;
    let mut rows = Vec::new();
    for selector_row in selector_rows {
        let version_id =
            selector_row_version_id(planned_write, selector_row.version_id.as_deref())?;
        let row_key = entity_state_row_key(planned_write, entity_schema, &selector_row.entity_id)?;
        let Some(current_row) = resolve_exact_effective_state_row(
            backend,
            &ExactEffectiveStateRowRequest {
                schema_key: entity_schema.schema_key.clone(),
                version_id,
                row_key,
                include_global_overlay: true,
                include_untracked_overlay: true,
            },
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

async fn query_entity_selector_rows(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<Vec<CanonicalStateRowKey>, WriteResolveError> {
    let selector = canonical_state_selector(planned_write);
    let mut selector_columns = vec!["lixcol_entity_id"];
    if let Some(version_column) = selector.version_column.as_deref() {
        selector_columns.push(version_column);
    }
    let query_result = execute_public_selector_query_strict(
        backend,
        planned_write,
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

async fn query_text_selector_values_for_write_selector(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    selector_column: &str,
    error_message: &str,
) -> Result<Vec<String>, WriteResolveError> {
    let selector = canonical_state_selector(planned_write);
    let query_result = execute_public_selector_query_strict(
        backend,
        planned_write,
        build_public_selector_query(
            &planned_write.command.target.descriptor.public_name,
            &selector,
            &[selector_column],
        ),
    )
    .await
    .map_err(write_resolve_backend_error)?;

    let mut values = Vec::new();
    for row in query_result.rows {
        let Some(value) = row.first().and_then(text_from_value) else {
            return Err(WriteResolveError {
                message: error_message.to_string(),
            });
        };
        if !values.iter().any(|existing| existing == &value) {
            values.push(value);
        }
    }
    Ok(values)
}

async fn query_state_selector_rows(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
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

fn canonical_state_selector(planned_write: &PlannedWrite) -> CanonicalStateSelector {
    let predicates = if planned_write.command.selector.exact_only {
        exact_selector_predicates(planned_write)
            .unwrap_or_else(|| planned_write.command.selector.residual_predicates.clone())
    } else {
        planned_write.command.selector.residual_predicates.clone()
    };
    let version_column = state_selector_exposes_version_id(planned_write).then(|| {
        public_selector_version_column(planned_write.command.target.descriptor.surface_family)
            .to_string()
    });
    CanonicalStateSelector {
        predicates,
        version_column,
    }
}

fn exact_selector_predicates(planned_write: &PlannedWrite) -> Option<Vec<Expr>> {
    let mut predicates = Vec::with_capacity(planned_write.command.selector.exact_filters.len());
    for (column, value) in &planned_write.command.selector.exact_filters {
        let public_column = public_selector_column_name(
            planned_write.command.target.descriptor.surface_family,
            column,
        )?;
        predicates.push(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new(public_column))),
            op: BinaryOperator::Eq,
            right: Box::new(engine_value_to_sql_expr(value)),
        });
    }
    Some(predicates)
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
    filters: &BTreeMap<String, Value>,
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

fn engine_value_to_sql_expr(value: &Value) -> Expr {
    match value {
        Value::Null => Expr::Value(ValueWithSpan::from(SqlValue::Null)),
        Value::Boolean(value) => Expr::Value(ValueWithSpan::from(SqlValue::Boolean(*value))),
        Value::Text(value) => Expr::Value(ValueWithSpan::from(SqlValue::SingleQuotedString(
            value.clone(),
        ))),
        Value::Json(value) => Expr::Value(ValueWithSpan::from(SqlValue::SingleQuotedString(
            value.to_string(),
        ))),
        Value::Integer(value) => Expr::Value(ValueWithSpan::from(SqlValue::Number(
            value.to_string(),
            false,
        ))),
        Value::Real(value) => Expr::Value(ValueWithSpan::from(SqlValue::Number(
            value.to_string(),
            false,
        ))),
        Value::Blob(value) => Expr::Value(ValueWithSpan::from(
            SqlValue::SingleQuotedByteStringLiteral(String::from_utf8_lossy(value).to_string()),
        )),
    }
}

fn build_public_selector_query(
    surface_name: &str,
    selector: &CanonicalStateSelector,
    selector_columns: &[&str],
) -> Query {
    let selection = selector
        .predicates
        .iter()
        .cloned()
        .reduce(|left, right| Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        });

    Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection: selector_columns
                .iter()
                .map(|column| SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(*column))))
                .collect(),
            exclude: None,
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(surface_name))]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    with_ordinality: false,
                    partitions: vec![],
                    json_path: None,
                    sample: None,
                    index_hints: vec![],
                },
                joins: Vec::new(),
            }],
            lateral_views: Vec::new(),
            prewhere: None,
            selection,
            group_by: GroupByExpr::Expressions(Vec::new(), Vec::new()),
            cluster_by: Vec::new(),
            distribute_by: Vec::new(),
            sort_by: Vec::new(),
            having: None,
            named_window: Vec::new(),
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            connect_by: None,
            flavor: SelectFlavor::Standard,
        }))),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    }
}

async fn execute_public_selector_query_strict(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    query: Query,
) -> Result<QueryResult, LixError> {
    execute_public_read_query_strict(backend, query, &planned_write.command.bound_parameters).await
}

async fn resolve_target_state_rows(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<Vec<ExactEffectiveStateRow>, WriteResolveError> {
    let schema_key = resolved_schema_key(planned_write)?;
    let selector_rows = query_state_selector_rows(backend, planned_write).await?;
    let mut rows = Vec::new();
    for selector_row in selector_rows {
        let version_id =
            selector_row_version_id(planned_write, selector_row.version_id.as_deref())?;
        let Some(current_row) = resolve_exact_effective_state_row(
            backend,
            &ExactEffectiveStateRowRequest {
                schema_key: schema_key.clone(),
                version_id,
                row_key: selector_row,
                include_global_overlay: true,
                include_untracked_overlay: true,
            },
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

fn supports_selector_driven_state_resolution(planned_write: &PlannedWrite) -> bool {
    planned_write.command.target.descriptor.surface_family == SurfaceFamily::State
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
        message: "public write resolver requires an exact entity target".to_string(),
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
            message: "public write resolver requires an exact schema proof or schema_key literal"
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
                    "public write resolver requires requested_version_id for ActiveVersion writes"
                        .to_string(),
            }),
        ScopeProof::SingleVersion(version_id) => Ok(Some(version_id.clone())),
        ScopeProof::FiniteVersionSet(version_ids) if version_ids.len() == 1 => {
            Ok(version_ids.iter().next().cloned())
        }
        ScopeProof::FiniteVersionSet(version_ids) if version_ids.is_empty() => {
            Err(WriteResolveError {
                message: "public write resolver requires a concrete version_id".to_string(),
            })
        }
        ScopeProof::FiniteVersionSet(_) => Err(WriteResolveError {
            message: "public write resolver cannot resolve multi-version writes".to_string(),
        }),
        ScopeProof::GlobalAdmin => Ok(Some(GLOBAL_VERSION_ID.to_string())),
        ScopeProof::Unknown | ScopeProof::Unbounded => Err(WriteResolveError {
            message: "public write resolver requires a bounded scope proof".to_string(),
        }),
    }
}

fn insert_on_conflict_action(planned_write: &PlannedWrite) -> Option<InsertOnConflictAction> {
    planned_write
        .command
        .on_conflict
        .as_ref()
        .map(|conflict| conflict.action)
}

fn finalize_resolved_write_plan(
    planned_write: &PlannedWrite,
    mut resolved: ResolvedWritePlan,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    resolved
        .partitions
        .retain(|partition| !partition.intended_post_state.is_empty());
    for partition in &mut resolved.partitions {
        if partition.execution_mode == WriteMode::Untracked {
            partition.target_write_lane = None;
            continue;
        }
        if partition.target_write_lane.is_none() {
            partition.target_write_lane = Some(write_lane_from_scope(&planned_write.scope_proof)?);
        }
    }
    Ok(resolved)
}

fn noop_resolved_write_plan(execution_mode: WriteMode) -> ResolvedWritePlan {
    single_partition_write_plan(
        execution_mode,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
}

async fn state_insert_conflicts_with_existing_row(
    backend: &dyn LixBackend,
    row: &PlannedStateRow,
) -> Result<bool, WriteResolveError> {
    let version_id = row.version_id.clone().ok_or_else(|| WriteResolveError {
        message: "public state insert resolver requires a concrete version_id".to_string(),
    })?;
    let current_row = resolve_exact_effective_state_row(
        backend,
        &ExactEffectiveStateRowRequest {
            schema_key: row.schema_key.clone(),
            version_id,
            row_key: state_insert_row_key(row),
            include_global_overlay: true,
            include_untracked_overlay: true,
        },
    )
    .await
    .map_err(write_resolve_backend_error)?;
    Ok(current_row.is_some())
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
            message: "public tracked writes require exactly one write lane".to_string(),
        }),
        ScopeProof::GlobalAdmin => Ok(WriteLane::GlobalAdmin),
        ScopeProof::Unknown | ScopeProof::Unbounded => Err(WriteResolveError {
            message: "public tracked writes require a bounded write lane".to_string(),
        }),
    }
}

fn row_snapshot_name(row: &[Value]) -> Option<String> {
    row.first()
        .and_then(text_from_value)
        .and_then(|snapshot| serde_json::from_str::<JsonValue>(&snapshot).ok())
        .and_then(|snapshot| {
            snapshot
                .get("name")
                .and_then(JsonValue::as_str)
                .map(ToString::to_string)
        })
}

fn row_snapshot_hidden(row: &[Value]) -> Option<bool> {
    row.first()
        .and_then(text_from_value)
        .and_then(|snapshot| serde_json::from_str::<JsonValue>(&snapshot).ok())
        .and_then(|snapshot| snapshot.get("hidden").and_then(JsonValue::as_bool))
}

fn row_snapshot_commit_id(row: &[Value]) -> Option<String> {
    row.first()
        .and_then(text_from_value)
        .and_then(|snapshot| serde_json::from_str::<JsonValue>(&snapshot).ok())
        .and_then(|snapshot| {
            snapshot
                .get("commit_id")
                .and_then(JsonValue::as_str)
                .map(ToString::to_string)
        })
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

fn required_text_value(row: &[Value], label: &str) -> Result<String, WriteResolveError> {
    required_text_value_index(row, 0, label)
}

fn required_text_value_index(
    row: &[Value],
    index: usize,
    label: &str,
) -> Result<String, WriteResolveError> {
    row.get(index)
        .and_then(text_from_value)
        .ok_or_else(|| WriteResolveError {
            message: format!("public filesystem resolver expected text {}", label),
        })
}

fn required_bool_value_index(
    row: &[Value],
    index: usize,
    label: &str,
) -> Result<bool, WriteResolveError> {
    row.get(index)
        .and_then(value_as_bool)
        .ok_or_else(|| WriteResolveError {
            message: format!("public selector resolver expected bool {}", label),
        })
}

fn optional_text_value(value: Option<&Value>) -> Option<String> {
    value.and_then(text_from_value)
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
        MutationPayload::BulkFullSnapshot(_) => Err(WriteResolveError {
            message: "public resolver expected a single-row payload".to_string(),
        }),
        MutationPayload::Tombstone => Ok(Default::default()),
    }
}

fn payload_text_value(planned_write: &PlannedWrite, key: &str) -> Option<String> {
    match &planned_write.command.payload {
        MutationPayload::FullSnapshot(payload) | MutationPayload::Patch(payload) => {
            match payload.get(key) {
                Some(Value::Text(value)) => Some(value.clone()),
                _ => None,
            }
        }
        MutationPayload::BulkFullSnapshot(payloads) => {
            let mut values = payloads
                .iter()
                .filter_map(|payload| match payload.get(key) {
                    Some(Value::Text(value)) => Some(value.clone()),
                    _ => None,
                });
            let first = values.next()?;
            values.all(|candidate| candidate == first).then_some(first)
        }
        MutationPayload::Tombstone => None,
    }
}

fn payload_maps(
    planned_write: &PlannedWrite,
) -> Result<Vec<BTreeMap<String, Value>>, WriteResolveError> {
    match &planned_write.command.payload {
        MutationPayload::FullSnapshot(payload) => Ok(vec![payload.clone()]),
        MutationPayload::BulkFullSnapshot(payloads) => Ok(payloads.clone()),
        MutationPayload::Patch(_) | MutationPayload::Tombstone => Err(WriteResolveError {
            message: "public resolver expected insert payload rows".to_string(),
        }),
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

fn bool_from_value(value: &Value) -> Option<bool> {
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

fn write_resolve_backend_error(error: crate::LixError) -> WriteResolveError {
    WriteResolveError {
        message: error.description,
    }
}

fn write_resolve_state_assignments_error(error: StateAssignmentsError) -> WriteResolveError {
    WriteResolveError {
        message: error.message,
    }
}

fn write_resolve_filesystem_assignments_error(
    error: FilesystemAssignmentsError,
) -> WriteResolveError {
    WriteResolveError {
        message: error.message,
    }
}

fn write_resolve_filesystem_planning_error(error: FilesystemPlanningError) -> WriteResolveError {
    WriteResolveError {
        message: error.message,
    }
}
