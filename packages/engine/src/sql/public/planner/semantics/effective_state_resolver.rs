use crate::live_state::constraints::{ScanConstraint, ScanField, ScanOperator};
use crate::live_state::raw::{
    scan_rows_with_executor as scan_raw_rows_with_executor, RawRow, RawStorage,
};
use crate::live_state::tracked::{
    load_exact_tombstone_with_executor, scan_tombstones_with_executor, ExactTrackedRowRequest,
    TrackedScanRequest, TrackedTombstoneMarker,
};
use crate::live_state::{load_live_row_access_with_backend, normalized_live_column_values};
use crate::sql::common::dependency_spec::DependencySpec;
use crate::sql::execution::shared_path::PendingTransactionView;
use crate::sql::public::planner::ir::{
    CanonicalStateRowKey, CanonicalStateScan, ReadPlan, StructuredPublicRead, VersionScope,
};
use crate::sql::public::planner::semantics::surface_semantics::{
    canonical_filter_column_name, effective_state_pushdown_predicates, overlay_lanes,
    overlay_lanes_for_version, OverlayLane,
};
use crate::state::commit::{
    load_exact_committed_state_row_from_live_state, CommitQueryExecutor, ExactCommittedStateRow,
    ExactCommittedStateRowRequest,
};
use crate::transaction::{PendingSemanticRow, PendingSemanticStorage};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError, Value};
use sqlparser::ast::{Expr, OrderBy, OrderByKind, SelectItem, Visit, Visitor};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StateSourceAuthority {
    AuthoritativeCommitted,
    Untracked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectiveStateRequest {
    pub(crate) schema_set: BTreeSet<String>,
    pub(crate) version_scope: VersionScope,
    pub(crate) include_global_overlay: bool,
    pub(crate) include_untracked_overlay: bool,
    pub(crate) include_tombstones: bool,
    pub(crate) predicate_classes: Vec<String>,
    pub(crate) required_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EffectiveStatePlan {
    pub(crate) state_source: StateSourceAuthority,
    pub(crate) overlay_lanes: Vec<OverlayLane>,
    pub(crate) pushdown_safe_predicates: Vec<Expr>,
    pub(crate) residual_predicates: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) version_id: Option<String>,
    pub(crate) lineage_commit_id: Option<String>,
    pub(crate) lineage_change_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ResolvedStateRows {
    pub(crate) visible_rows: Vec<ResolvedStateRow>,
    pub(crate) hidden_rows: Vec<ResolvedStateRow>,
    pub(crate) lineage_metadata: Vec<String>,
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

pub(crate) fn build_effective_state(
    structured_read: &StructuredPublicRead,
    dependency_spec: Option<&DependencySpec>,
) -> Option<(EffectiveStateRequest, EffectiveStatePlan)> {
    let scan = canonical_state_scan(&structured_read.read_command.root)?;
    let request = EffectiveStateRequest {
        schema_set: schema_set_for_read(structured_read, dependency_spec),
        version_scope: scan.version_scope,
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
        schema_set.extend(
            spec.schema_keys
                .iter()
                .filter(|schema_key| schema_key.as_str() != "lix_active_version")
                .cloned(),
        );
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

pub(crate) async fn resolve_exact_effective_state_row(
    backend: &dyn LixBackend,
    request: &ExactEffectiveStateRowRequest,
) -> Result<Option<ExactEffectiveStateRow>, LixError> {
    resolve_exact_effective_state_row_with_pending_transaction_view(backend, request, None).await
}

pub(crate) async fn resolve_exact_effective_state_row_with_pending_transaction_view(
    backend: &dyn LixBackend,
    request: &ExactEffectiveStateRowRequest,
    pending_transaction_view: Option<&PendingTransactionView>,
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

        let row = match lane {
            OverlayLane::LocalTracked | OverlayLane::GlobalTracked => {
                load_exact_tracked_effective_row(backend, request, &internal_version_id, lane)
                    .await?
            }
            OverlayLane::LocalUntracked | OverlayLane::GlobalUntracked => {
                load_exact_untracked_effective_row(backend, request, &internal_version_id, lane)
                    .await?
            }
        };

        if row.is_some() {
            return Ok(row);
        }

        if matches!(lane, OverlayLane::LocalTracked | OverlayLane::GlobalTracked)
            && tracked_exact_row_exists_including_tombstones(backend, request, &internal_version_id)
                .await?
        {
            return Ok(None);
        }
    }

    Ok(None)
}

async fn tracked_exact_row_exists_including_tombstones(
    backend: &dyn LixBackend,
    request: &ExactEffectiveStateRowRequest,
    version_id: &str,
) -> Result<bool, LixError> {
    let mut executor = backend;
    let exact_request = ExactTrackedRowRequest {
        schema_key: request.schema_key.clone(),
        version_id: version_id.to_string(),
        entity_id: request.row_key.entity_id.clone(),
        file_id: request.row_key.file_id.clone(),
    };
    if let Some(tombstone) =
        load_exact_tombstone_with_executor(&mut executor, &exact_request).await?
    {
        return Ok(tombstone_matches_row_key(&tombstone, &request.row_key));
    }

    let tombstones = scan_tombstones_with_executor(
        &mut executor,
        &TrackedScanRequest {
            schema_key: request.schema_key.clone(),
            version_id: version_id.to_string(),
            constraints: row_key_constraints(&request.row_key),
            required_columns: Vec::new(),
        },
    )
    .await?;
    Ok(tombstones
        .iter()
        .any(|tombstone| tombstone_matches_row_key(tombstone, &request.row_key)))
}

async fn load_exact_tracked_effective_row(
    backend: &dyn LixBackend,
    request: &ExactEffectiveStateRowRequest,
    internal_version_id: &str,
    overlay_lane: OverlayLane,
) -> Result<Option<ExactEffectiveStateRow>, LixError> {
    let row = load_exact_committed_state_row_from_live_state(
        backend,
        &ExactCommittedStateRowRequest {
            entity_id: request.row_key.entity_id.clone(),
            schema_key: request.schema_key.clone(),
            version_id: internal_version_id.to_string(),
            exact_filters: request.row_key.committed_exact_filters(),
        },
    )
    .await?;

    Ok(row
        .map(|row| exact_effective_state_row_from_tracked(row, &request.version_id, overlay_lane)))
}

async fn load_exact_untracked_effective_row(
    backend: &dyn LixBackend,
    request: &ExactEffectiveStateRowRequest,
    version_id: &str,
    overlay_lane: OverlayLane,
) -> Result<Option<ExactEffectiveStateRow>, LixError> {
    let mut executor = backend;
    let row = load_exact_untracked_state_row(
        &mut executor,
        &ExactUntrackedStateRowRequest {
            schema_key: request.schema_key.clone(),
            version_id: version_id.to_string(),
            row_key: request.row_key.clone(),
        },
    )
    .await?;

    Ok(row.map(|row| {
        exact_effective_state_row_from_untracked(row, &request.version_id, overlay_lane)
    }))
}

fn exact_effective_state_row_from_tracked(
    row: ExactCommittedStateRow,
    requested_version_id: &str,
    overlay_lane: OverlayLane,
) -> ExactEffectiveStateRow {
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

fn exact_effective_state_row_from_untracked(
    row: ExactUntrackedStateRow,
    requested_version_id: &str,
    overlay_lane: OverlayLane,
) -> ExactEffectiveStateRow {
    let projected_version_id = if matches!(overlay_lane, OverlayLane::GlobalUntracked)
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
    ExactEffectiveStateRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        version_id: projected_version_id,
        values,
        source_change_id: Some("untracked".to_string()),
        overlay_lane,
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ExactUntrackedStateRowRequest {
    schema_key: String,
    version_id: String,
    row_key: CanonicalStateRowKey,
}

#[derive(Debug, Clone, PartialEq)]
struct ExactUntrackedStateRow {
    entity_id: String,
    schema_key: String,
    file_id: String,
    version_id: String,
    values: BTreeMap<String, Value>,
}

async fn load_exact_untracked_state_row(
    executor: &mut dyn CommitQueryExecutor,
    request: &ExactUntrackedStateRowRequest,
) -> Result<Option<ExactUntrackedStateRow>, LixError> {
    let mut rows = scan_raw_rows_with_executor(
        executor,
        RawStorage::Untracked,
        &request.schema_key,
        &request.version_id,
        &row_key_constraints(&request.row_key),
        &[],
    )
    .await?;
    rows.retain(|row| untracked_row_matches_row_key(row, &request.row_key));
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };

    let mut values = BTreeMap::new();
    values.insert(
        "entity_id".to_string(),
        Value::Text(row.entity_id().to_string()),
    );
    values.insert(
        "schema_key".to_string(),
        Value::Text(row.schema_key().to_string()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(row.schema_version().to_string()),
    );
    values.insert(
        "file_id".to_string(),
        Value::Text(row.file_id().to_string()),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(row.version_id().to_string()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(row.plugin_key().to_string()),
    );
    if let Some(metadata) = row.metadata() {
        values.insert("metadata".to_string(), Value::Text(metadata.to_string()));
    }
    values.extend(row.values().clone());

    Ok(Some(ExactUntrackedStateRow {
        entity_id: row.entity_id().to_string(),
        schema_key: row.schema_key().to_string(),
        file_id: row.file_id().to_string(),
        version_id: row.version_id().to_string(),
        values,
    }))
}

fn row_key_constraints(row_key: &CanonicalStateRowKey) -> Vec<ScanConstraint> {
    let mut constraints = vec![ScanConstraint {
        field: ScanField::EntityId,
        operator: ScanOperator::Eq(Value::Text(row_key.entity_id.clone())),
    }];
    if let Some(file_id) = row_key.file_id.as_ref() {
        constraints.push(ScanConstraint {
            field: ScanField::FileId,
            operator: ScanOperator::Eq(Value::Text(file_id.clone())),
        });
    }
    if let Some(plugin_key) = row_key.plugin_key.as_ref() {
        constraints.push(ScanConstraint {
            field: ScanField::PluginKey,
            operator: ScanOperator::Eq(Value::Text(plugin_key.clone())),
        });
    }
    if let Some(schema_version) = row_key.schema_version.as_ref() {
        constraints.push(ScanConstraint {
            field: ScanField::SchemaVersion,
            operator: ScanOperator::Eq(Value::Text(schema_version.clone())),
        });
    }
    constraints
}

fn tombstone_matches_row_key(row: &TrackedTombstoneMarker, row_key: &CanonicalStateRowKey) -> bool {
    row_key
        .writer_key
        .as_deref()
        .is_none_or(|writer_key| row.writer_key.as_deref() == Some(writer_key))
        && row_key
            .plugin_key
            .as_deref()
            .is_none_or(|plugin_key| row.plugin_key.as_deref() == Some(plugin_key))
        && row_key
            .schema_version
            .as_deref()
            .is_none_or(|schema_version| row.schema_version.as_deref() == Some(schema_version))
}

fn untracked_row_matches_row_key(row: &RawRow, row_key: &CanonicalStateRowKey) -> bool {
    row_key
        .writer_key
        .as_deref()
        .is_none_or(|writer_key| row.writer_key() == Some(writer_key))
        && row_key
            .plugin_key
            .as_deref()
            .is_none_or(|plugin_key| row.plugin_key() == plugin_key)
        && row_key
            .schema_version
            .as_deref()
            .is_none_or(|schema_version| row.schema_version() == schema_version)
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
    pending_transaction_view: Option<&PendingTransactionView>,
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
    let Some(overlay) = pending_transaction_view.and_then(PendingTransactionView::semantic_overlay)
    else {
        return Ok(None);
    };

    let pending = overlay
        .visible_rows(storage, &request.schema_key)
        .find(|row| pending_row_matches_exact_request(row, request, internal_version_id));
    let Some(pending) = pending else {
        return Ok(None);
    };

    if pending.tombstone && matches!(storage, PendingSemanticStorage::Tracked) {
        return Ok(Some(None));
    }

    Ok(Some(Some(
        exact_effective_state_row_from_pending(backend, pending, &request.version_id, overlay_lane)
            .await?,
    )))
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
    let access = load_live_row_access_with_backend(backend, &row.schema_key).await?;
    let mut values =
        normalized_live_column_values(access.layout(), row.snapshot_content.as_deref())?;
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

#[cfg(test)]
mod tests {
    use super::{build_effective_state, OverlayLane, StateSourceAuthority};
    use crate::sql::public::catalog::SurfaceRegistry;
    use crate::sql::public::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql::public::core::parser::parse_sql_script;
    use crate::sql::public::planner::canonicalize::canonicalize_read;
    use crate::sql::public::planner::ir::StructuredPublicRead;
    use crate::sql::public::planner::semantics::dependency_spec::derive_dependency_spec_from_structured_public_read;
    use crate::{SqlDialect, Value};

    fn structured_read(
        registry: &SurfaceRegistry,
        sql: &str,
        params: Vec<Value>,
    ) -> StructuredPublicRead {
        let mut statements = parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let bound = BoundStatement::from_statement(
            statement,
            params,
            ExecutionContext::with_dialect(SqlDialect::Sqlite),
        );
        canonicalize_read(bound, registry)
            .expect("query should canonicalize")
            .into_structured_read()
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
