use crate::contracts::artifacts::{
    EffectiveStateRequest, EffectiveStateVersionScope, ExactUntrackedLookupRequest,
    LiveQueryEffectiveRow, LiveQueryOverlayLane, TrackedTombstoneLookupRequest,
};
use crate::canonical::read::{
    load_exact_committed_state_row_at_version_head as load_exact_committed_state_row,
    ExactCommittedStateRow, ExactCommittedStateRowRequest,
};
use crate::contracts::traits::{
    LiveStateQueryBackend, PendingSemanticRow, PendingSemanticStorage, PendingView,
};
use crate::sql::logical_plan::public_ir::{
    CanonicalStateRowKey, CanonicalStateScan, ReadPlan, StructuredPublicRead, VersionScope,
};
use crate::sql::logical_plan::DependencySpec;
use crate::sql::semantic_ir::semantics::surface_semantics::{
    canonical_filter_column_name, effective_state_pushdown_predicates, overlay_lanes,
    overlay_lanes_for_version, OverlayLane,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::workspace::writer_key::load_workspace_writer_key_annotation_for_state_row;
use crate::{LixBackend, LixError, Value};
use sqlparser::ast::{Expr, OrderBy, OrderByKind, SelectItem, Visit, Visitor};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StateSourceAuthority {
    AuthoritativeCommitted,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EffectiveStatePlan {
    pub(crate) state_source: StateSourceAuthority,
    pub(crate) overlay_lanes: Vec<OverlayLane>,
    pub(crate) pushdown_safe_predicates: Vec<Expr>,
    pub(crate) residual_predicates: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExactEffectiveStateRowRequest {
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) row_key: CanonicalStateRowKey,
    pub(crate) include_global_overlay: bool,
    pub(crate) include_untracked_overlay: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExactEffectiveStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) values: BTreeMap<String, Value>,
    pub(crate) source_change_id: Option<String>,
    pub(crate) overlay_lane: OverlayLane,
}

enum TrackedExactEffectiveRowLookup {
    Matched(ExactEffectiveStateRow),
    Shadowed,
    Missing,
}

#[derive(Debug, Clone, PartialEq)]
struct WorkspaceAnnotatedTrackedExactRow {
    // Canonical committed row plus a workspace-owned annotation overlay applied
    // only for effective/public state reads.
    committed: ExactCommittedStateRow,
    writer_key: Option<String>,
}

pub(crate) fn build_effective_state(
    structured_read: &StructuredPublicRead,
    dependency_spec: Option<&DependencySpec>,
) -> Option<(EffectiveStateRequest, EffectiveStatePlan)> {
    let scan = canonical_state_scan(&structured_read.read_command.root)?;
    let request = EffectiveStateRequest {
        schema_set: schema_set_for_read(structured_read, dependency_spec),
        version_scope: effective_state_version_scope(scan.version_scope),
        include_global_overlay: true,
        include_untracked_overlay: true,
        include_tombstones: scan.include_tombstones,
        predicate_classes: predicate_classes_for_read(structured_read),
        required_columns: required_columns_for_read(structured_read, scan),
    };
    let all_predicates = structured_read.query.selection_predicates.clone();
    let pushdown_safe_predicates =
        effective_state_pushdown_predicates(&structured_read.surface_binding, &all_predicates);
    let plan = EffectiveStatePlan {
        state_source: StateSourceAuthority::AuthoritativeCommitted,
        overlay_lanes: overlay_lanes(
            request.include_global_overlay,
            request.include_untracked_overlay,
        ),
        pushdown_safe_predicates: pushdown_safe_predicates.clone(),
        residual_predicates: all_predicates
            .into_iter()
            .filter(|predicate| {
                !pushdown_safe_predicates
                    .iter()
                    .any(|candidate| candidate == predicate)
            })
            .collect(),
    };
    Some((request, plan))
}

fn effective_state_version_scope(version_scope: VersionScope) -> EffectiveStateVersionScope {
    match version_scope {
        VersionScope::ActiveVersion => EffectiveStateVersionScope::ActiveVersion,
        VersionScope::ExplicitVersion => EffectiveStateVersionScope::ExplicitVersion,
        VersionScope::History => EffectiveStateVersionScope::History,
    }
}

fn canonical_state_scan(read_plan: &ReadPlan) -> Option<&CanonicalStateScan> {
    match read_plan {
        ReadPlan::Scan(scan) => Some(scan),
        ReadPlan::FilesystemScan(_)
        | ReadPlan::AdminScan(_)
        | ReadPlan::ChangeScan(_)
        | ReadPlan::WorkingChangesScan(_) => None,
        ReadPlan::Filter { input, .. }
        | ReadPlan::Project { input, .. }
        | ReadPlan::Sort { input, .. }
        | ReadPlan::Limit { input, .. } => canonical_state_scan(input),
    }
}

fn schema_set_for_read(
    structured_read: &StructuredPublicRead,
    dependency_spec: Option<&DependencySpec>,
) -> BTreeSet<String> {
    let mut schema_set = BTreeSet::new();
    if let Some(schema_key) = structured_read
        .surface_binding
        .implicit_overrides
        .fixed_schema_key
        .clone()
    {
        schema_set.insert(schema_key);
    }
    if let Some(spec) = dependency_spec {
        schema_set.extend(spec.schema_keys.iter().cloned());
    }
    schema_set
}

fn predicate_classes_for_read(structured_read: &StructuredPublicRead) -> Vec<String> {
    struct Collector {
        classes: BTreeSet<String>,
    }

    impl Visitor for Collector {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if let Some(column) = canonical_filter_column_name(expr) {
                self.classes.insert(format!("column:{column}"));
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector {
        classes: BTreeSet::new(),
    };
    for predicate in &structured_read.query.selection_predicates {
        let _ = predicate.visit(&mut collector);
    }
    collector.classes.into_iter().collect()
}

fn required_columns_for_read(
    structured_read: &StructuredPublicRead,
    scan: &CanonicalStateScan,
) -> Vec<String> {
    let mut required = BTreeSet::new();

    if let Some(entity_projection) = &scan.entity_projection {
        required.extend(entity_projection.visible_columns.iter().cloned());
    }

    collect_projection_columns(&structured_read.query.projection, &mut required);
    collect_expression_columns(
        structured_read.query.selection.as_ref(),
        structured_read.query.order_by.as_ref(),
        &mut required,
    );
    if required.is_empty() {
        required.extend(scan.binding.exposed_columns.iter().cloned());
    }
    required.insert("entity_id".to_string());
    required.insert("schema_key".to_string());
    if scan.expose_version_id || scan.version_scope != VersionScope::ActiveVersion {
        required.insert("version_id".to_string());
    }

    required.into_iter().collect()
}

fn collect_projection_columns(projection: &[SelectItem], required: &mut BTreeSet<String>) {
    let wildcard_projection = projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
        )
    });
    if wildcard_projection {
        return;
    }

    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) => collect_columns_from_expr(expr, required),
            SelectItem::ExprWithAlias { expr, .. } => collect_columns_from_expr(expr, required),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
        }
    }
}

fn collect_expression_columns(
    selection: Option<&Expr>,
    order_by: Option<&OrderBy>,
    required: &mut BTreeSet<String>,
) {
    if let Some(selection) = selection {
        collect_columns_from_expr(selection, required);
    }
    if let Some(order_by) = order_by {
        let OrderByKind::Expressions(ordering) = &order_by.kind else {
            return;
        };
        for item in ordering {
            collect_columns_from_expr(&item.expr, required);
        }
    }
}

fn collect_columns_from_expr(expr: &Expr, required: &mut BTreeSet<String>) {
    struct Collector<'a> {
        required: &'a mut BTreeSet<String>,
    }

    impl Visitor for Collector<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if let Some(column) = canonical_filter_column_name(expr) {
                self.required.insert(column.to_string());
            } else if let Expr::Identifier(ident) = expr {
                self.required.insert(ident.value.clone());
            } else if let Expr::CompoundIdentifier(parts) = expr {
                if let Some(last) = parts.last() {
                    self.required.insert(last.value.clone());
                }
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector { required };
    let _ = expr.visit(&mut collector);
}

pub(crate) async fn resolve_exact_effective_state_row_with_pending_transaction_view(
    backend: &dyn LixBackend,
    request: &ExactEffectiveStateRowRequest,
    pending_transaction_view: Option<&dyn PendingView>,
) -> Result<Option<ExactEffectiveStateRow>, LixError> {
    let requested_untracked = request.row_key.untracked;
    let mut requested_global = request.row_key.global;
    if request.version_id == GLOBAL_VERSION_ID {
        if requested_global == Some(false) {
            return Ok(None);
        }
        requested_global = None;
    }

    let lanes = overlay_lanes_for_version(
        &request.version_id,
        request.include_global_overlay,
        request.include_untracked_overlay,
    );

    for lane in lanes {
        if !lane_matches_global_filter(lane, requested_global)
            || !lane_matches_untracked_filter(lane, requested_untracked)
        {
            continue;
        }

        let internal_version_id = if matches!(
            lane,
            OverlayLane::GlobalTracked | OverlayLane::GlobalUntracked
        ) {
            GLOBAL_VERSION_ID.to_string()
        } else {
            request.version_id.clone()
        };

        if let Some(row) = load_exact_pending_effective_row(
            backend,
            pending_transaction_view,
            request,
            &internal_version_id,
            lane,
        )
        .await?
        {
            return Ok(row);
        }

        match lane {
            OverlayLane::LocalTracked | OverlayLane::GlobalTracked => {
                match load_exact_tracked_effective_row(
                    backend,
                    pending_transaction_view,
                    request,
                    &internal_version_id,
                    lane,
                )
                .await?
                {
                    TrackedExactEffectiveRowLookup::Matched(row) => return Ok(Some(row)),
                    TrackedExactEffectiveRowLookup::Shadowed => return Ok(None),
                    TrackedExactEffectiveRowLookup::Missing => {}
                }
            }
            OverlayLane::LocalUntracked | OverlayLane::GlobalUntracked => {
                if let Some(row) =
                    load_exact_untracked_effective_row(backend, request, &internal_version_id, lane)
                        .await?
                {
                    return Ok(Some(row));
                }
            }
        }
    }

    Ok(None)
}

async fn load_exact_tracked_effective_row(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    request: &ExactEffectiveStateRowRequest,
    internal_version_id: &str,
    overlay_lane: OverlayLane,
) -> Result<TrackedExactEffectiveRowLookup, LixError> {
    let row = load_exact_committed_state_row(
        backend,
        &ExactCommittedStateRowRequest {
            entity_id: request.row_key.entity_id.clone(),
            schema_key: request.schema_key.clone(),
            version_id: internal_version_id.to_string(),
            exact_filters: request.row_key.committed_exact_filters(),
        },
    )
    .await?;

    if let Some(row) = row {
        let row = annotate_tracked_exact_row_with_workspace_writer_key(
            backend,
            pending_transaction_view,
            row,
        )
        .await?;
        if tracked_exact_row_matches_row_key(&row, &request.row_key) {
            return Ok(TrackedExactEffectiveRowLookup::Matched(
                exact_effective_state_row_from_tracked(row, &request.version_id, overlay_lane),
            ));
        }
        return Ok(TrackedExactEffectiveRowLookup::Shadowed);
    }

    if backend
        .tracked_tombstone_shadows_exact_row(&TrackedTombstoneLookupRequest {
            schema_key: request.schema_key.clone(),
            version_id: internal_version_id.to_string(),
            entity_id: request.row_key.entity_id.clone(),
            file_id: request.row_key.file_id.clone(),
            plugin_key: request.row_key.plugin_key.clone(),
            schema_version: request.row_key.schema_version.clone(),
        })
        .await?
    {
        return Ok(TrackedExactEffectiveRowLookup::Shadowed);
    }

    Ok(TrackedExactEffectiveRowLookup::Missing)
}

async fn load_exact_untracked_effective_row(
    backend: &dyn LixBackend,
    request: &ExactEffectiveStateRowRequest,
    version_id: &str,
    overlay_lane: OverlayLane,
) -> Result<Option<ExactEffectiveStateRow>, LixError> {
    backend
        .load_exact_untracked_effective_row(
            &ExactUntrackedLookupRequest {
            schema_key: request.schema_key.clone(),
            version_id: version_id.to_string(),
            entity_id: request.row_key.entity_id.clone(),
            file_id: request.row_key.file_id.clone(),
            plugin_key: request.row_key.plugin_key.clone(),
            schema_version: request.row_key.schema_version.clone(),
            writer_key: request.row_key.writer_key.clone(),
            },
            &request.version_id,
            live_state_overlay_lane(overlay_lane),
        )
        .await
        .map(|row| row.map(|row| exact_effective_state_row_from_effective_untracked(row, overlay_lane)))
}

fn exact_effective_state_row_from_tracked(
    row: WorkspaceAnnotatedTrackedExactRow,
    requested_version_id: &str,
    overlay_lane: OverlayLane,
) -> ExactEffectiveStateRow {
    let WorkspaceAnnotatedTrackedExactRow {
        committed: row,
        writer_key,
    } = row;
    let projected_version_id = if matches!(overlay_lane, OverlayLane::GlobalTracked)
        && row.version_id == GLOBAL_VERSION_ID
    {
        requested_version_id.to_string()
    } else {
        row.version_id.clone()
    };
    let mut values = row.values;
    values.insert(
        "version_id".to_string(),
        Value::Text(projected_version_id.clone()),
    );
    values.insert(
        "writer_key".to_string(),
        writer_key.clone().map(Value::Text).unwrap_or(Value::Null),
    );
    ExactEffectiveStateRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        version_id: projected_version_id,
        values,
        source_change_id: row.source_change_id,
        overlay_lane,
    }
}

fn exact_effective_state_row_from_effective_untracked(
    row: LiveQueryEffectiveRow,
    overlay_lane: OverlayLane,
) -> ExactEffectiveStateRow {
    let mut values = row.values;
    values.insert("entity_id".to_string(), Value::Text(row.entity_id.clone()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(row.schema_key.clone()),
    );
    values.insert("file_id".to_string(), Value::Text(row.file_id.clone()));
    values.insert(
        "version_id".to_string(),
        Value::Text(row.version_id.clone()),
    );
    if let Some(schema_version) = row.schema_version.as_ref() {
        values.insert(
            "schema_version".to_string(),
            Value::Text(schema_version.clone()),
        );
    }
    if let Some(plugin_key) = row.plugin_key.as_ref() {
        values.insert("plugin_key".to_string(), Value::Text(plugin_key.clone()));
    }
    values.insert(
        "metadata".to_string(),
        row.metadata.clone().map(Value::Text).unwrap_or(Value::Null),
    );
    values.insert(
        "writer_key".to_string(),
        row.writer_key
            .clone()
            .map(Value::Text)
            .unwrap_or(Value::Null),
    );
    values.insert("global".to_string(), Value::Boolean(row.global));
    values.insert("untracked".to_string(), Value::Boolean(row.untracked));
    ExactEffectiveStateRow {
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        file_id: row.file_id,
        version_id: row.version_id,
        values,
        source_change_id: row.source_change_id,
        overlay_lane,
    }
}

fn live_state_overlay_lane(lane: OverlayLane) -> LiveQueryOverlayLane {
    match lane {
        OverlayLane::GlobalTracked => LiveQueryOverlayLane::GlobalTracked,
        OverlayLane::LocalTracked => LiveQueryOverlayLane::LocalTracked,
        OverlayLane::GlobalUntracked => LiveQueryOverlayLane::GlobalUntracked,
        OverlayLane::LocalUntracked => LiveQueryOverlayLane::LocalUntracked,
    }
}

async fn annotate_tracked_exact_row_with_workspace_writer_key(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    row: ExactCommittedStateRow,
) -> Result<WorkspaceAnnotatedTrackedExactRow, LixError> {
    let writer_key = if let Some(writer_key) = pending_workspace_writer_key_annotation(
        pending_transaction_view,
        &row.version_id,
        &row.schema_key,
        &row.entity_id,
        &row.file_id,
    ) {
        writer_key
    } else {
        load_workspace_writer_key_annotation_for_state_row(
            backend,
            &row.version_id,
            &row.schema_key,
            &row.entity_id,
            &row.file_id,
        )
        .await?
    };
    Ok(WorkspaceAnnotatedTrackedExactRow {
        committed: row,
        writer_key,
    })
}

fn pending_workspace_writer_key_annotation(
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    schema_key: &str,
    entity_id: &str,
    file_id: &str,
) -> Option<Option<String>> {
    pending_transaction_view.and_then(|view| {
        view.workspace_writer_key_annotation_for_state_row(
            version_id, schema_key, entity_id, file_id,
        )
    })
}

fn tracked_exact_row_matches_row_key(
    row: &WorkspaceAnnotatedTrackedExactRow,
    row_key: &CanonicalStateRowKey,
) -> bool {
    row_key
        .writer_key
        .as_deref()
        .is_none_or(|writer_key| row.writer_key.as_deref() == Some(writer_key))
}

fn lane_matches_global_filter(lane: OverlayLane, requested_global: Option<bool>) -> bool {
    match requested_global {
        Some(true) => matches!(
            lane,
            OverlayLane::GlobalTracked | OverlayLane::GlobalUntracked
        ),
        Some(false) => matches!(
            lane,
            OverlayLane::LocalTracked | OverlayLane::LocalUntracked
        ),
        None => true,
    }
}

fn lane_matches_untracked_filter(lane: OverlayLane, requested_untracked: Option<bool>) -> bool {
    match requested_untracked {
        Some(true) => matches!(
            lane,
            OverlayLane::LocalUntracked | OverlayLane::GlobalUntracked
        ),
        Some(false) => matches!(lane, OverlayLane::LocalTracked | OverlayLane::GlobalTracked),
        None => true,
    }
}

fn row_key_text_value<'a>(row_key: &'a CanonicalStateRowKey, column: &str) -> Option<&'a str> {
    match column {
        "entity_id" => Some(row_key.entity_id.as_str()),
        "file_id" => row_key.file_id.as_deref(),
        "plugin_key" => row_key.plugin_key.as_deref(),
        "schema_version" => row_key.schema_version.as_deref(),
        "writer_key" => row_key.writer_key.as_deref(),
        _ => None,
    }
}

async fn load_exact_pending_effective_row(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    request: &ExactEffectiveStateRowRequest,
    internal_version_id: &str,
    overlay_lane: OverlayLane,
) -> Result<Option<Option<ExactEffectiveStateRow>>, LixError> {
    let storage = match overlay_lane {
        OverlayLane::LocalTracked | OverlayLane::GlobalTracked => PendingSemanticStorage::Tracked,
        OverlayLane::LocalUntracked | OverlayLane::GlobalUntracked => {
            PendingSemanticStorage::Untracked
        }
    };
    let Some(pending) = pending_transaction_view.and_then(|view| {
        view.visible_semantic_rows(storage, &request.schema_key)
            .into_iter()
            .find(|row| pending_row_matches_exact_request(row, request, internal_version_id))
    }) else {
        return Ok(None);
    };

    if pending.tombstone && matches!(storage, PendingSemanticStorage::Tracked) {
        return Ok(Some(None));
    }

    let row = exact_effective_state_row_from_pending(
        backend,
        pending_transaction_view,
        &pending,
        &request.version_id,
        overlay_lane,
    )
    .await?;
    if !pending_effective_row_matches_row_key(&row, &request.row_key) {
        return Ok(Some(None));
    }

    Ok(Some(Some(row)))
}

fn pending_row_matches_exact_request(
    row: &PendingSemanticRow,
    request: &ExactEffectiveStateRowRequest,
    internal_version_id: &str,
) -> bool {
    row.entity_id == request.row_key.entity_id
        && row.version_id == internal_version_id
        && row.file_id == row_key_text_value(&request.row_key, "file_id").unwrap_or(&row.file_id)
        && row.plugin_key
            == row_key_text_value(&request.row_key, "plugin_key").unwrap_or(&row.plugin_key)
        && row.schema_version
            == row_key_text_value(&request.row_key, "schema_version").unwrap_or(&row.schema_version)
}

async fn exact_effective_state_row_from_pending(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    row: &PendingSemanticRow,
    requested_version_id: &str,
    overlay_lane: OverlayLane,
) -> Result<ExactEffectiveStateRow, LixError> {
    let projected_version_id = if matches!(
        overlay_lane,
        OverlayLane::GlobalTracked | OverlayLane::GlobalUntracked
    ) && row.version_id == GLOBAL_VERSION_ID
    {
        requested_version_id.to_string()
    } else {
        row.version_id.clone()
    };
    let mut values = backend
        .normalize_live_snapshot_values(&row.schema_key, row.snapshot_content.as_deref())
        .await?;
    values.insert("entity_id".to_string(), Value::Text(row.entity_id.clone()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(row.schema_key.clone()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(row.schema_version.clone()),
    );
    values.insert("file_id".to_string(), Value::Text(row.file_id.clone()));
    values.insert(
        "version_id".to_string(),
        Value::Text(projected_version_id.clone()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(row.plugin_key.clone()),
    );
    values.insert(
        "metadata".to_string(),
        row.metadata.clone().map(Value::Text).unwrap_or(Value::Null),
    );
    let writer_key = pending_workspace_writer_key_annotation(
        pending_transaction_view,
        &row.version_id,
        &row.schema_key,
        &row.entity_id,
        &row.file_id,
    )
    .flatten();
    values.insert(
        "writer_key".to_string(),
        writer_key.map(Value::Text).unwrap_or(Value::Null),
    );

    Ok(ExactEffectiveStateRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        version_id: projected_version_id,
        values,
        source_change_id: Some("pending".to_string()),
        overlay_lane,
    })
}

fn pending_effective_row_matches_row_key(
    row: &ExactEffectiveStateRow,
    row_key: &CanonicalStateRowKey,
) -> bool {
    row_key.writer_key.as_deref().is_none_or(|writer_key| {
        matches!(
            row.values.get("writer_key"),
            Some(Value::Text(actual)) if actual == writer_key
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{build_effective_state, OverlayLane, StateSourceAuthority};
    use crate::contracts::surface::SurfaceRegistry;
    use crate::sql::binder::bind_statement;
    use crate::sql::logical_plan::public_ir::StructuredPublicRead;
    use crate::sql::semantic_ir::canonicalize::canonicalize_read;
    use crate::sql::semantic_ir::semantics::dependency_spec::derive_dependency_spec_from_structured_public_read;
    use crate::sql::semantic_ir::ExecutionContext;
    use crate::{SqlDialect, Value};

    fn structured_read(
        registry: &SurfaceRegistry,
        sql: &str,
        params: Vec<Value>,
    ) -> StructuredPublicRead {
        let mut statements = crate::sql::parser::parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let bound = bind_statement(
            statement,
            params,
            ExecutionContext::with_dialect(SqlDialect::Sqlite),
        );
        canonicalize_read(bound, registry)
            .expect("query should canonicalize")
            .structured_read()
    }

    #[test]
    fn builds_effective_state_request_for_entity_surface() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let structured_read = structured_read(
            &registry,
            "SELECT key, value FROM lix_key_value WHERE key = 'hello'",
            Vec::new(),
        );
        let dependency_spec = derive_dependency_spec_from_structured_public_read(&structured_read)
            .expect("dependency spec");

        let (request, plan) = build_effective_state(&structured_read, Some(&dependency_spec))
            .expect("effective-state plan should build");

        assert_eq!(
            request.schema_set.into_iter().collect::<Vec<_>>(),
            vec!["lix_key_value".to_string()]
        );
        assert!(!request.include_tombstones);
        assert!(request.required_columns.contains(&"key".to_string()));
        assert!(request.required_columns.contains(&"value".to_string()));
        assert_eq!(
            plan.state_source,
            StateSourceAuthority::AuthoritativeCommitted
        );
        assert_eq!(
            plan.overlay_lanes,
            vec![
                OverlayLane::LocalUntracked,
                OverlayLane::LocalTracked,
                OverlayLane::GlobalUntracked,
                OverlayLane::GlobalTracked,
            ]
        );
        assert_eq!(
            plan.residual_predicates
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["key = 'hello'".to_string()]
        );
    }

    #[test]
    fn history_surfaces_include_tombstones_and_version_columns() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let structured_read = structured_read(
            &registry,
            "SELECT entity_id, version_id FROM lix_state_history WHERE schema_key = 'message'",
            Vec::new(),
        );
        let dependency_spec = derive_dependency_spec_from_structured_public_read(&structured_read)
            .expect("dependency spec");

        let (request, _plan) = build_effective_state(&structured_read, Some(&dependency_spec))
            .expect("effective-state plan should build");

        assert!(request.include_tombstones);
        assert!(request.required_columns.contains(&"version_id".to_string()));
        assert!(request
            .predicate_classes
            .contains(&"column:schema_key".to_string()));
    }

    #[test]
    fn extracts_exact_state_pushdown_predicates_from_top_level_conjunctions() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let structured_read = structured_read(
            &registry,
            "SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value' AND file_id = 'lix'",
            Vec::new(),
        );

        let (_request, plan) = build_effective_state(&structured_read, None)
            .expect("effective-state plan should build");

        assert_eq!(
            plan.pushdown_safe_predicates
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec![
                "schema_key = 'lix_key_value'".to_string(),
                "file_id = 'lix'".to_string()
            ]
        );
        assert!(plan.residual_predicates.is_empty());
    }
}
