use super::*;
use crate::contracts::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::contracts::traits::{PendingSemanticStorage, PendingView};
use crate::live_state::{decode_registered_schema_row, scan_rows, RowQuery, RowReadMode};
use crate::schema::{
    apply_schema_defaults_with_shared_runtime, builtin_schema_definition,
    collect_state_column_overrides_with_shared_runtime, schema_from_registered_snapshot, SchemaKey,
};
use crate::session::write_resolution::prepared_artifacts::build_entity_insert_rows_with_functions;
use crate::session::write_resolution::prepared_artifacts::{
    apply_entity_state_assignments, apply_state_assignments, assignments_from_payload,
    build_state_insert_row, ensure_identity_columns_preserved, CanonicalStateAssignments,
    CanonicalStateRowKey, EntityAssignmentsSemantics, EntityInsertSemantics,
    InsertOnConflictAction,
};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

fn authoritative_version_id_for_effective_row(current_row: &ExactEffectiveStateRow) -> String {
    match current_row.overlay_lane {
        OverlayLane::GlobalTracked | OverlayLane::GlobalUntracked => GLOBAL_VERSION_ID.to_string(),
        OverlayLane::LocalTracked | OverlayLane::LocalUntracked => current_row.version_id.clone(),
    }
}

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const REGISTERED_SCHEMA_VERSION_ID: &str = "global";

async fn load_latest_registered_schema(
    backend: &dyn LixBackend,
    pending_view: Option<&dyn PendingView>,
    schema_key: &str,
) -> Result<Option<JsonValue>, crate::LixError> {
    let mut latest = None::<(SchemaKey, JsonValue)>;

    let rows = scan_rows(
        backend,
        &RowQuery {
            schema_key: REGISTERED_SCHEMA_KEY.to_string(),
            version_id: REGISTERED_SCHEMA_VERSION_ID.to_string(),
            mode: RowReadMode::Tracked,
            constraints: Vec::new(),
            include_tombstones: false,
        },
    )
    .await?;

    for row in &rows {
        let Some((key, schema)) = decode_registered_schema_row(row)? else {
            continue;
        };
        if key.schema_key != schema_key {
            continue;
        }
        if should_replace_latest_schema(latest.as_ref().map(|(key, _)| key), &key) {
            latest = Some((key, schema));
        }
    }

    if let Some(pending_view) = pending_view {
        for (_, snapshot_content) in pending_view.visible_registered_schema_entries() {
            let Some(snapshot_content) = snapshot_content else {
                continue;
            };
            remember_latest_registered_schema_from_snapshot_content(
                &mut latest,
                schema_key,
                &snapshot_content,
            )?;
        }

        for storage in [
            PendingSemanticStorage::Tracked,
            PendingSemanticStorage::Untracked,
        ] {
            for row in pending_view.visible_semantic_rows(storage, REGISTERED_SCHEMA_KEY) {
                let Some(snapshot_content) = row.snapshot_content else {
                    continue;
                };
                remember_latest_registered_schema_from_snapshot_content(
                    &mut latest,
                    schema_key,
                    &snapshot_content,
                )?;
            }
        }
    }

    Ok(latest.map(|(_, schema)| schema))
}

fn remember_latest_registered_schema_from_snapshot_content(
    latest: &mut Option<(SchemaKey, JsonValue)>,
    schema_key: &str,
    snapshot_content: &str,
) -> Result<(), crate::LixError> {
    let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        crate::LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("registered schema snapshot_content invalid JSON: {error}"),
        )
    })?;
    let (key, schema) = schema_from_registered_snapshot(&snapshot)?;
    if key.schema_key != schema_key {
        return Ok(());
    }
    if should_replace_latest_schema(latest.as_ref().map(|(key, _)| key), &key) {
        *latest = Some((key, schema));
    }
    Ok(())
}

fn should_replace_latest_schema(existing: Option<&SchemaKey>, candidate: &SchemaKey) -> bool {
    existing
        .map(|existing| compare_schema_keys(candidate, existing).is_ge())
        .unwrap_or(true)
}

fn compare_schema_keys(left: &SchemaKey, right: &SchemaKey) -> std::cmp::Ordering {
    match (left.version_number(), right.version_number()) {
        (Some(left_version), Some(right_version)) => left_version.cmp(&right_version),
        _ => left.schema_version.cmp(&right.schema_version),
    }
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
    let mut values = state_values_without_writer_key(&current_row.values);
    values.insert(
        "version_id".to_string(),
        Value::Text(authoritative_version_id.to_string()),
    );
    PlannedStateRow {
        entity_id: current_row.entity_id.clone(),
        schema_key: current_row.schema_key.clone(),
        version_id: Some(authoritative_version_id.to_string()),
        values,
        writer_key: current_row
            .values
            .get("writer_key")
            .and_then(text_from_value),
        tombstone: false,
    }
}

pub(super) async fn resolve_state_write<P>(
    hydrator: &mut PublicWriteHydrator<'_>,
    planned_write: &PlannedWrite,
    pending_view: Option<&dyn PendingView>,
    functions: SharedFunctionProvider<P>,
    selector_resolver: &dyn WriteSelectorResolver,
) -> Result<ResolvedWritePlan, WriteResolveError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let state_schema =
        load_optional_annotation_schema(hydrator.backend(), pending_view, planned_write)
            .await
            .map_err(write_resolve_backend_error)?;
    resolve_state_backed_write(
        hydrator,
        planned_write,
        StateBackedSurface::State(state_schema.as_ref()),
        functions,
        selector_resolver,
    )
    .await
}

pub(super) async fn resolve_entity_write<P>(
    hydrator: &mut PublicWriteHydrator<'_>,
    planned_write: &PlannedWrite,
    pending_view: Option<&dyn PendingView>,
    functions: SharedFunctionProvider<P>,
    selector_resolver: &dyn WriteSelectorResolver,
) -> Result<ResolvedWritePlan, WriteResolveError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let entity_schema = load_entity_schema(hydrator.backend(), pending_view, planned_write)
        .await
        .map_err(write_resolve_backend_error)?;
    reject_unsupported_entity_overrides(planned_write, &entity_schema)?;
    resolve_state_backed_write(
        hydrator,
        planned_write,
        StateBackedSurface::Entity(&entity_schema),
        functions,
        selector_resolver,
    )
    .await
}

#[derive(Debug, Clone)]
struct LoadedAnnotationSchema {
    schema: JsonValue,
    schema_key: String,
    schema_version: String,
    state_defaults: BTreeMap<String, Value>,
}

#[derive(Debug, Clone)]
struct EntityWriteSchema {
    annotations: LoadedAnnotationSchema,
    property_columns: Vec<String>,
    primary_key_paths: Vec<Vec<String>>,
}

#[derive(Clone, Copy)]
enum StateBackedSurface<'a> {
    State(Option<&'a LoadedAnnotationSchema>),
    Entity(&'a EntityWriteSchema),
}

impl StateBackedSurface<'_> {
    fn update_context(self) -> &'static str {
        match self {
            Self::State(_) => "public update resolver",
            Self::Entity(_) => "public entity update resolver",
        }
    }

    fn build_insert_rows<P>(
        self,
        planned_write: &PlannedWrite,
        row_version_ids: &[Option<String>],
        functions: SharedFunctionProvider<P>,
    ) -> Result<Vec<PlannedStateRow>, WriteResolveError>
    where
        P: LixFunctionProvider + Send + 'static,
    {
        match self {
            Self::State(schema) => build_state_insert_rows_with_functions(
                planned_write,
                row_version_ids,
                schema,
                functions,
            ),
            Self::Entity(entity_schema) => {
                let mut rows = build_entity_insert_rows_with_functions(
                    payload_maps(planned_write)?,
                    row_version_ids.to_vec(),
                    EntityInsertSemantics {
                        schema: &entity_schema.annotations.schema,
                        schema_key: &entity_schema.annotations.schema_key,
                        schema_version: &entity_schema.annotations.schema_version,
                        property_columns: &entity_schema.property_columns,
                        primary_key_paths: &entity_schema.primary_key_paths,
                        state_defaults: &entity_schema.annotations.state_defaults,
                    },
                    functions,
                )
                .map_err(write_resolve_state_assignments_error)?;
                for row in &mut rows {
                    row.writer_key = planned_write.command.execution_context.writer_key.clone();
                }
                Ok(rows)
            }
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
        planned_write: &PlannedWrite,
        assignments: &CanonicalStateAssignments,
        current_row: &ExactEffectiveStateRow,
    ) -> Result<(BTreeMap<String, Value>, Option<String>), WriteResolveError> {
        match self {
            Self::State(_) => {
                let values = apply_state_assignments(
                    &state_values_without_writer_key(&current_row.values),
                    &state_assignments_without_writer_key(assignments),
                );
                ensure_identity_columns_preserved(
                    &current_row.entity_id,
                    &current_row.schema_key,
                    &current_row.file_id,
                    &current_row.version_id,
                    &values,
                )
                .map_err(write_resolve_state_assignments_error)?;
                Ok((
                    values,
                    state_writer_key_from_assignments(
                        assignments,
                        planned_write
                            .command
                            .execution_context
                            .writer_key
                            .as_deref(),
                        self.update_context(),
                    )?,
                ))
            }
            Self::Entity(entity_schema) => apply_entity_state_assignments(
                current_row,
                assignments,
                EntityAssignmentsSemantics {
                    property_columns: &entity_schema.property_columns,
                    primary_key_paths: &entity_schema.primary_key_paths,
                },
            )
            .map(|values| {
                (
                    values,
                    planned_write.command.execution_context.writer_key.clone(),
                )
            })
            .map_err(write_resolve_state_assignments_error),
        }
    }

    async fn resolve_insert_conflict_row(
        self,
        hydrator: &PublicWriteHydrator<'_>,
        _planned_write: &PlannedWrite,
        row: &PlannedStateRow,
    ) -> Result<Option<ExactEffectiveStateRow>, WriteResolveError> {
        match self {
            Self::State(_) => {
                let version_id = row.version_id.clone().ok_or_else(|| WriteResolveError {
                    message: "public state insert resolver requires a concrete version_id"
                        .to_string(),
                })?;
                hydrator
                    .resolve_exact_effective_state_row(&ExactEffectiveStateRowRequest {
                        schema_key: row.schema_key.clone(),
                        version_id,
                        row_key: state_insert_row_key(row),
                        include_global_overlay: true,
                        include_untracked_overlay: true,
                    })
                    .await
                    .map_err(write_resolve_backend_error)
            }
            Self::Entity(entity_schema) => {
                let version_id = row.version_id.clone().ok_or_else(|| WriteResolveError {
                    message: "public entity insert resolver requires a concrete version_id"
                        .to_string(),
                })?;
                hydrator
                    .resolve_exact_effective_state_row(&ExactEffectiveStateRowRequest {
                        schema_key: entity_schema.annotations.schema_key.clone(),
                        version_id,
                        row_key: entity_insert_row_key(entity_schema, row)?,
                        include_global_overlay: true,
                        include_untracked_overlay: true,
                    })
                    .await
                    .map_err(write_resolve_backend_error)
            }
        }
    }

    async fn resolve_target_rows(
        self,
        hydrator: &PublicWriteHydrator<'_>,
        planned_write: &PlannedWrite,
        selector_resolver: &dyn WriteSelectorResolver,
    ) -> Result<Vec<ExactEffectiveStateRow>, WriteResolveError> {
        match self {
            Self::State(_) => {
                resolve_target_state_rows(hydrator, planned_write, selector_resolver).await
            }
            Self::Entity(entity_schema) => {
                resolve_target_entity_rows(
                    hydrator,
                    planned_write,
                    entity_schema,
                    selector_resolver,
                )
                .await
            }
        }
    }
}

async fn resolve_state_backed_write<P>(
    hydrator: &mut PublicWriteHydrator<'_>,
    planned_write: &PlannedWrite,
    surface: StateBackedSurface<'_>,
    functions: SharedFunctionProvider<P>,
    selector_resolver: &dyn WriteSelectorResolver,
) -> Result<ResolvedWritePlan, WriteResolveError>
where
    P: LixFunctionProvider + Send + 'static,
{
    match planned_write.command.operation_kind {
        WriteOperationKind::Insert => {
            resolve_state_backed_insert_write(hydrator, planned_write, surface, functions).await
        }
        WriteOperationKind::Update | WriteOperationKind::Delete => {
            resolve_state_backed_existing_write(hydrator, planned_write, surface, selector_resolver)
                .await
        }
    }
}

async fn resolve_state_backed_insert_write<P>(
    hydrator: &mut PublicWriteHydrator<'_>,
    planned_write: &PlannedWrite,
    surface: StateBackedSurface<'_>,
    functions: SharedFunctionProvider<P>,
) -> Result<ResolvedWritePlan, WriteResolveError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let row_version_ids = resolved_insert_version_ids(hydrator, planned_write).await?;
    let rows = surface.build_insert_rows(planned_write, &row_version_ids, functions)?;
    let payloads = payload_maps(planned_write)?;
    if rows.len() != payloads.len() {
        return Err(WriteResolveError {
            message: "public insert resolver requires one planned row per payload row".to_string(),
        });
    }
    let mut partitions = ResolvedWritePlanBuilder::default();

    for (row, payload) in rows.into_iter().zip(payloads.into_iter()) {
        let row_requested_mode = write_mode_request_for_insert_payload(planned_write, &payload);
        let default_execution_mode = default_execution_mode_for_request(row_requested_mode);
        if let Some(conflict) = planned_write.command.on_conflict.as_ref() {
            if let Some(current_row) = surface
                .resolve_insert_conflict_row(hydrator, planned_write, &row)
                .await?
            {
                if conflict.action == InsertOnConflictAction::DoNothing {
                    continue;
                }
                let row_execution_mode =
                    resolve_execution_mode_for_effective_row(row_requested_mode, &current_row)?;
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
    hydrator: &mut PublicWriteHydrator<'_>,
    planned_write: &PlannedWrite,
    surface: StateBackedSurface<'_>,
    selector_resolver: &dyn WriteSelectorResolver,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let _ = resolved_existing_version_ids(hydrator, planned_write).await?;
    let current_rows = match surface {
        StateBackedSurface::State(_)
            if planned_write.command.selector.exact_only
                && state_selector_targets_single_effective_row(planned_write) =>
        {
            resolve_exact_state_target_rows(hydrator, planned_write).await?
        }
        _ => {
            surface
                .resolve_target_rows(hydrator, planned_write, selector_resolver)
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
                let (mut values, writer_key) =
                    surface.apply_update_assignments(planned_write, assignments, &current_row)?;
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
                    writer_key,
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
                    values: state_values_without_writer_key(&current_row.values),
                    writer_key: planned_write.command.execution_context.writer_key.clone(),
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

fn build_state_insert_rows_with_functions<P>(
    planned_write: &PlannedWrite,
    row_version_ids: &[Option<String>],
    schema: Option<&LoadedAnnotationSchema>,
    functions: SharedFunctionProvider<P>,
) -> Result<Vec<PlannedStateRow>, WriteResolveError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let payloads =
        apply_state_insert_schema_annotations(payload_maps(planned_write)?, schema, functions)?;
    if payloads.len() != row_version_ids.len() {
        return Err(WriteResolveError {
            message: "public state insert resolver requires one version target per payload row"
                .to_string(),
        });
    }
    let single_row = payloads.len() == 1;
    let schema_key = resolved_schema_key(planned_write)?;
    let mut rows = Vec::with_capacity(payloads.len());

    for (mut payload, version_id) in payloads.into_iter().zip(row_version_ids.iter()) {
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
        let writer_key = state_writer_key_from_values(
            &mut payload,
            planned_write
                .command
                .execution_context
                .writer_key
                .as_deref(),
            "public state insert resolver",
        )?;
        rows.push(build_state_insert_row(
            entity_id,
            schema_key.clone(),
            version_id.clone(),
            payload,
            writer_key,
        ));
    }

    Ok(rows)
}

fn state_values_without_writer_key(values: &BTreeMap<String, Value>) -> BTreeMap<String, Value> {
    values
        .iter()
        .filter(|(key, _)| key.as_str() != "writer_key")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn state_assignments_without_writer_key(
    assignments: &CanonicalStateAssignments,
) -> CanonicalStateAssignments {
    CanonicalStateAssignments {
        columns: assignments
            .columns
            .iter()
            .filter(|(key, _)| key.as_str() != "writer_key")
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    }
}

fn state_writer_key_from_assignments(
    assignments: &CanonicalStateAssignments,
    default_writer_key: Option<&str>,
    context: &str,
) -> Result<Option<String>, WriteResolveError> {
    match assignments.columns.get("writer_key") {
        Some(value) => state_writer_key_value(value, context),
        None => Ok(default_writer_key.map(str::to_string)),
    }
}

fn state_writer_key_from_values(
    values: &mut BTreeMap<String, Value>,
    default_writer_key: Option<&str>,
    context: &str,
) -> Result<Option<String>, WriteResolveError> {
    match values.remove("writer_key") {
        Some(value) => state_writer_key_value(&value, context),
        None => Ok(default_writer_key.map(str::to_string)),
    }
}

fn state_writer_key_value(
    value: &Value,
    context: &str,
) -> Result<Option<String>, WriteResolveError> {
    match value {
        Value::Text(text) => Ok(Some(text.clone())),
        Value::Null => Ok(None),
        other => Err(WriteResolveError {
            message: format!(
                "{context} treats 'writer_key' as workspace annotation text or null, got {other:?}"
            ),
        }),
    }
}

fn apply_state_insert_schema_annotations<P>(
    payloads: Vec<BTreeMap<String, Value>>,
    schema: Option<&LoadedAnnotationSchema>,
    functions: SharedFunctionProvider<P>,
) -> Result<Vec<BTreeMap<String, Value>>, WriteResolveError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let Some(schema) = schema else {
        return Ok(payloads);
    };

    let mut annotated = Vec::with_capacity(payloads.len());
    for mut payload in payloads {
        let Some(snapshot_text) = payload.get("snapshot_content").and_then(text_from_value) else {
            annotated.push(payload);
            continue;
        };
        let JsonValue::Object(mut snapshot) = serde_json::from_str::<JsonValue>(&snapshot_text)
            .map_err(|error| WriteResolveError {
                message: format!(
                    "public state insert resolver could not parse snapshot_content JSON: {error}"
                ),
            })?
        else {
            return Err(WriteResolveError {
                message: format!(
                    "public state insert resolver requires object snapshot_content for schema '{}'",
                    schema.schema_key
                ),
            });
        };

        apply_schema_defaults_with_shared_runtime(
            &mut snapshot,
            &schema.schema,
            functions.clone(),
            &schema.schema_key,
            &schema.schema_version,
        )
        .map_err(write_resolve_backend_error)?;

        payload.insert(
            "snapshot_content".to_string(),
            Value::Text(
                serde_json::to_string(&JsonValue::Object(snapshot)).map_err(|error| {
                    WriteResolveError {
                        message: format!(
                            "public state insert resolver could not serialize snapshot_content: {error}"
                        ),
                    }
                })?,
            ),
        );
        annotated.push(payload);
    }

    Ok(annotated)
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
    hydrator: &PublicWriteHydrator<'_>,
    planned_write: &PlannedWrite,
) -> Result<Vec<ExactEffectiveStateRow>, WriteResolveError> {
    let schema_key = resolved_schema_key(planned_write)?;
    let version_id = resolved_version_id(planned_write)?.ok_or_else(|| WriteResolveError {
        message: "public existing-row write resolver requires a concrete version_id".to_string(),
    })?;
    let current_row = hydrator
        .resolve_exact_effective_state_row(&ExactEffectiveStateRowRequest {
            schema_key,
            version_id,
            row_key: exact_selector_row_key(planned_write)?,
            include_global_overlay: true,
            include_untracked_overlay: true,
        })
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
        writer_key: None,
    }
}

async fn resolve_target_entity_rows(
    hydrator: &PublicWriteHydrator<'_>,
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
    selector_resolver: &dyn WriteSelectorResolver,
) -> Result<Vec<ExactEffectiveStateRow>, WriteResolveError> {
    let selector_rows = selector_resolver
        .load_entity_selector_rows(planned_write)
        .await?;
    let mut rows = Vec::new();
    for selector_row in selector_rows {
        let version_id =
            selector_row_version_id(planned_write, selector_row.version_id.as_deref())?;
        let row_key = entity_state_row_key(planned_write, entity_schema, &selector_row.entity_id)?;
        let Some(current_row) = hydrator
            .resolve_exact_effective_state_row(&ExactEffectiveStateRowRequest {
                schema_key: entity_schema.annotations.schema_key.clone(),
                version_id,
                row_key,
                include_global_overlay: true,
                include_untracked_overlay: true,
            })
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
    hydrator: &PublicWriteHydrator<'_>,
    planned_write: &PlannedWrite,
    selector_resolver: &dyn WriteSelectorResolver,
) -> Result<Vec<ExactEffectiveStateRow>, WriteResolveError> {
    let schema_key = resolved_schema_key(planned_write)?;
    let selector_rows = selector_resolver
        .load_state_selector_rows(planned_write)
        .await?;
    let mut rows = Vec::new();
    for selector_row in selector_rows {
        let version_id =
            selector_row_version_id(planned_write, selector_row.version_id.as_deref())?;
        let Some(current_row) = hydrator
            .resolve_exact_effective_state_row(&ExactEffectiveStateRowRequest {
                schema_key: schema_key.clone(),
                version_id,
                row_key: selector_row,
                include_global_overlay: true,
                include_untracked_overlay: true,
            })
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

async fn load_optional_annotation_schema(
    backend: &dyn LixBackend,
    pending_view: Option<&dyn PendingView>,
    planned_write: &PlannedWrite,
) -> Result<Option<LoadedAnnotationSchema>, crate::LixError> {
    let schema_key = resolved_schema_key(planned_write).map_err(write_resolve_to_lix_error)?;
    let schema = if let Some(schema) = builtin_schema_definition(&schema_key) {
        schema.clone()
    } else {
        match load_latest_registered_schema(backend, pending_view, &schema_key).await? {
            Some(schema) => schema,
            None => {
                return Ok(None);
            }
        }
    };
    load_annotation_schema_from_json(schema_key, schema).map(Some)
}

async fn load_entity_schema(
    backend: &dyn LixBackend,
    pending_view: Option<&dyn PendingView>,
    planned_write: &PlannedWrite,
) -> Result<EntityWriteSchema, crate::LixError> {
    let schema_key = resolved_schema_key(planned_write).map_err(write_resolve_to_lix_error)?;
    let schema = if let Some(schema) = builtin_schema_definition(&schema_key) {
        schema.clone()
    } else {
        load_latest_registered_schema(backend, pending_view, &schema_key)
            .await?
            .ok_or_else(|| {
                crate::LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("schema '{}' is not stored", schema_key),
                )
            })?
    };
    let annotations = load_annotation_schema_from_json(schema_key.clone(), schema)?;
    let mut property_columns = annotations
        .schema
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

    let primary_key_paths = annotations
        .schema
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

    Ok(EntityWriteSchema {
        annotations,
        property_columns,
        primary_key_paths,
    })
}

fn load_annotation_schema_from_json(
    schema_key: String,
    schema: JsonValue,
) -> Result<LoadedAnnotationSchema, crate::LixError> {
    let schema_version = schema
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| crate::LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("schema '{}' is missing string x-lix-version", schema_key),
        })?
        .to_string();
    let mut state_defaults = BTreeMap::new();
    state_defaults.insert(
        "schema_version".to_string(),
        Value::Text(schema_version.clone()),
    );
    state_defaults.extend(collect_state_column_overrides_with_shared_runtime(
        &schema,
        &schema_key,
    )?);
    Ok(LoadedAnnotationSchema {
        schema,
        schema_key,
        schema_version,
        state_defaults,
    })
}

fn reject_unsupported_entity_overrides(
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
) -> Result<(), WriteResolveError> {
    if entity_schema
        .annotations
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
        .annotations
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
        if let Some(default) = entity_schema.annotations.state_defaults.get(key) {
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
        if let Some(default) = entity_schema.annotations.state_defaults.get(key) {
            assign_state_row_key_value(&mut row_key, key, default)?;
        }
    }
    Ok(row_key)
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
