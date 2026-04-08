use std::collections::{BTreeMap, BTreeSet};

use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value as SqlValue, ValueWithSpan};

use crate::contracts::artifacts::{PendingViewFilter, ScanConstraint, ScanField, ScanOperator};
use crate::contracts::traits::{LiveStateQueryBackend, PendingSemanticStorage, PendingView};
use crate::sql::logical_plan::public_ir::{CanonicalStateRowKey, PlannedWrite, ScopeProof};
use crate::sql::parser::placeholders::{resolve_placeholder_index, PlaceholderState};
use crate::{LixBackend, LixError, Value};

const GLOBAL_VERSION_ID: &str = "global";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SelectorOverlayLane {
    LocalUntracked,
    LocalTracked,
    GlobalUntracked,
    GlobalTracked,
}

impl SelectorOverlayLane {
    fn is_global(self) -> bool {
        matches!(self, Self::GlobalTracked | Self::GlobalUntracked)
    }

    fn is_untracked(self) -> bool {
        matches!(self, Self::LocalUntracked | Self::GlobalUntracked)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SelectorRowIdentity {
    entity_id: String,
    file_id: String,
}

#[derive(Debug, Clone)]
struct SelectorCandidateRow {
    entity_id: String,
    schema_key: String,
    schema_version: String,
    file_id: String,
    version_id: String,
    global: bool,
    untracked: bool,
    plugin_key: String,
    metadata: Option<String>,
    writer_key: Option<String>,
    snapshot_content: Option<String>,
    values: BTreeMap<String, Value>,
}

impl SelectorCandidateRow {
    fn identity(&self) -> SelectorRowIdentity {
        SelectorRowIdentity {
            entity_id: self.entity_id.clone(),
            file_id: self.file_id.clone(),
        }
    }
}

#[derive(Debug, Clone)]
enum LaneSelectorResult {
    Visible(SelectorCandidateRow),
    Tombstone,
    Missing,
}

pub(crate) async fn try_resolve_state_selector_rows_with_backend(
    backend: &dyn LixBackend,
    pending_view: Option<&dyn PendingView>,
    planned_write: &PlannedWrite,
) -> Result<Option<Vec<CanonicalStateRowKey>>, LixError> {
    if !matches!(
        planned_write.command.target.descriptor.public_name.as_str(),
        "lix_state" | "lix_state_by_version"
    ) {
        return Ok(None);
    }

    let Some(schema_key) = resolved_selector_schema_key(planned_write) else {
        return Ok(None);
    };
    let Some(version_ids) = resolved_selector_version_ids(planned_write) else {
        return Ok(None);
    };
    let Some(filters) = compile_selector_filters(planned_write) else {
        return Ok(None);
    };
    let constraints = selector_scan_constraints(planned_write);

    let mut selector_rows = Vec::new();
    for version_id in version_ids {
        selector_rows.extend(
            resolve_rows_for_version(
                backend,
                pending_view,
                &schema_key,
                &version_id,
                &constraints,
                &filters,
            )
            .await?,
        );
    }

    let expose_version_id = planned_write
        .command
        .target
        .implicit_overrides
        .expose_version_id;
    let mut row_keys = Vec::new();
    for row in selector_rows {
        let row_key = CanonicalStateRowKey {
            entity_id: row.entity_id,
            file_id: Some(row.file_id),
            plugin_key: Some(row.plugin_key),
            schema_version: Some(row.schema_version),
            version_id: expose_version_id.then_some(row.version_id),
            global: Some(row.global),
            untracked: Some(row.untracked),
            writer_key: row.writer_key,
        };
        if !row_keys.iter().any(|existing| existing == &row_key) {
            row_keys.push(row_key);
        }
    }

    Ok(Some(row_keys))
}

async fn resolve_rows_for_version(
    backend: &dyn LixBackend,
    pending_view: Option<&dyn PendingView>,
    schema_key: &str,
    requested_version_id: &str,
    constraints: &[ScanConstraint],
    filters: &[PendingViewFilter],
) -> Result<Vec<SelectorCandidateRow>, LixError> {
    let mut visible = BTreeMap::<SelectorRowIdentity, SelectorCandidateRow>::new();
    let mut hidden = BTreeSet::<SelectorRowIdentity>::new();

    for lane in selector_overlay_lanes_for_version(requested_version_id) {
        let lane_rows = scan_selector_lane(
            backend,
            pending_view,
            schema_key,
            requested_version_id,
            lane,
            constraints,
        )
        .await?;

        for (identity, result) in lane_rows {
            if visible.contains_key(&identity) || hidden.contains(&identity) {
                continue;
            }

            match result {
                LaneSelectorResult::Visible(row) => {
                    visible.insert(identity, row);
                }
                LaneSelectorResult::Tombstone => {
                    hidden.insert(identity);
                }
                LaneSelectorResult::Missing => {}
            }
        }
    }

    Ok(visible
        .into_values()
        .filter(|row| {
            filters
                .iter()
                .all(|filter| selector_filter_matches_row(filter, row))
        })
        .collect())
}

async fn scan_selector_lane(
    backend: &dyn LixBackend,
    pending_view: Option<&dyn PendingView>,
    schema_key: &str,
    requested_version_id: &str,
    lane: SelectorOverlayLane,
    constraints: &[ScanConstraint],
) -> Result<BTreeMap<SelectorRowIdentity, LaneSelectorResult>, LixError> {
    let storage_version_id = selector_storage_version_id(requested_version_id, lane);
    let request = crate::contracts::artifacts::ScanRequest {
        schema_key: schema_key.to_string(),
        version_id: storage_version_id.clone(),
        constraints: constraints.to_vec(),
        required_columns: Vec::new(),
    };
    let mut lane_rows = BTreeMap::<SelectorRowIdentity, LaneSelectorResult>::new();

    if lane.is_untracked() {
        for row in crate::live_state::scan_untracked_rows_with_backend(backend, &request).await? {
            let row = candidate_row_from_untracked(row, requested_version_id, lane, None, None);
            lane_rows.insert(row.identity(), LaneSelectorResult::Visible(row));
        }
    } else {
        for row in crate::live_state::scan_tracked_rows_with_backend(backend, &request).await? {
            let row = candidate_row_from_tracked(row, requested_version_id, lane, None, None);
            lane_rows.insert(row.identity(), LaneSelectorResult::Visible(row));
        }
        let mut executor = backend;
        for tombstone in
            crate::live_state::scan_tracked_tombstones_with_executor(&mut executor, &request)
                .await?
        {
            lane_rows.insert(
                SelectorRowIdentity {
                    entity_id: tombstone.entity_id,
                    file_id: tombstone.file_id,
                },
                LaneSelectorResult::Tombstone,
            );
        }
    }

    let Some(pending_view) = pending_view else {
        return Ok(lane_rows);
    };

    let pending_storage = if lane.is_untracked() {
        PendingSemanticStorage::Untracked
    } else {
        PendingSemanticStorage::Tracked
    };

    for pending in pending_view.visible_semantic_rows(pending_storage, schema_key) {
        if pending.version_id != storage_version_id
            || !crate::contracts::artifacts::matches_constraints(
                &pending.entity_id,
                &pending.file_id,
                &pending.plugin_key,
                &pending.schema_version,
                constraints,
            )
        {
            continue;
        }

        let identity = SelectorRowIdentity {
            entity_id: pending.entity_id.clone(),
            file_id: pending.file_id.clone(),
        };

        if pending.tombstone {
            if lane.is_untracked() {
                lane_rows.insert(identity, LaneSelectorResult::Missing);
            } else {
                lane_rows.insert(identity, LaneSelectorResult::Tombstone);
            }
            continue;
        }

        let writer_key = pending_view.workspace_writer_key_annotation_for_state_row(
            &pending.version_id,
            &pending.schema_key,
            &pending.entity_id,
            &pending.file_id,
        );
        let values = LiveStateQueryBackend::normalize_live_snapshot_values(
            backend,
            &pending.schema_key,
            pending.snapshot_content.as_deref(),
        )
        .await?;
        let row = SelectorCandidateRow {
            entity_id: pending.entity_id,
            schema_key: pending.schema_key,
            schema_version: pending.schema_version,
            file_id: pending.file_id,
            version_id: selector_projected_version_id(
                requested_version_id,
                lane,
                &pending.version_id,
            ),
            global: lane.is_global() || pending.version_id == GLOBAL_VERSION_ID,
            untracked: lane.is_untracked(),
            plugin_key: pending.plugin_key,
            metadata: pending.metadata,
            writer_key: writer_key.flatten(),
            snapshot_content: pending.snapshot_content,
            values,
        };
        lane_rows.insert(identity, LaneSelectorResult::Visible(row));
    }

    Ok(lane_rows)
}

fn candidate_row_from_tracked(
    row: crate::contracts::artifacts::TrackedRow,
    requested_version_id: &str,
    lane: SelectorOverlayLane,
    snapshot_content: Option<String>,
    writer_key_override: Option<String>,
) -> SelectorCandidateRow {
    SelectorCandidateRow {
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        file_id: row.file_id,
        version_id: selector_projected_version_id(requested_version_id, lane, &row.version_id),
        global: lane.is_global() || row.global,
        untracked: false,
        plugin_key: row.plugin_key,
        metadata: row.metadata,
        writer_key: writer_key_override.or(row.writer_key),
        snapshot_content,
        values: row.values,
    }
}

fn candidate_row_from_untracked(
    row: crate::contracts::artifacts::UntrackedRow,
    requested_version_id: &str,
    lane: SelectorOverlayLane,
    snapshot_content: Option<String>,
    writer_key_override: Option<String>,
) -> SelectorCandidateRow {
    SelectorCandidateRow {
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        file_id: row.file_id,
        version_id: selector_projected_version_id(requested_version_id, lane, &row.version_id),
        global: lane.is_global() || row.global,
        untracked: true,
        plugin_key: row.plugin_key,
        metadata: row.metadata,
        writer_key: writer_key_override.or(row.writer_key),
        snapshot_content,
        values: row.values,
    }
}

fn selector_scan_constraints(planned_write: &PlannedWrite) -> Vec<ScanConstraint> {
    let mut constraints = Vec::new();
    for (column, value) in &planned_write.command.selector.exact_filters {
        let field = match column.as_str() {
            "entity_id" => ScanField::EntityId,
            "file_id" => ScanField::FileId,
            "plugin_key" => ScanField::PluginKey,
            "schema_version" => ScanField::SchemaVersion,
            _ => continue,
        };
        constraints.push(ScanConstraint {
            field,
            operator: ScanOperator::Eq(value.clone()),
        });
    }
    constraints
}

fn resolved_selector_schema_key(planned_write: &PlannedWrite) -> Option<String> {
    match &planned_write.schema_proof {
        crate::session::write_resolution::prepared_artifacts::SchemaProof::Exact(schema_keys)
            if schema_keys.len() == 1 =>
        {
            schema_keys.iter().next().cloned()
        }
        _ => planned_write
            .command
            .selector
            .exact_filters
            .get("schema_key")
            .and_then(text_from_value),
    }
}

fn resolved_selector_version_ids(planned_write: &PlannedWrite) -> Option<Vec<String>> {
    match &planned_write.scope_proof {
        ScopeProof::ActiveVersion => planned_write
            .command
            .execution_context
            .requested_version_id
            .clone()
            .map(|version_id| vec![version_id]),
        ScopeProof::SingleVersion(version_id) => Some(vec![version_id.clone()]),
        ScopeProof::FiniteVersionSet(version_ids) if !version_ids.is_empty() => {
            Some(version_ids.iter().cloned().collect())
        }
        ScopeProof::GlobalAdmin => Some(vec![GLOBAL_VERSION_ID.to_string()]),
        ScopeProof::FiniteVersionSet(_) | ScopeProof::Unknown | ScopeProof::Unbounded => None,
    }
}

fn selector_overlay_lanes_for_version(version_id: &str) -> Vec<SelectorOverlayLane> {
    let mut lanes = vec![
        SelectorOverlayLane::LocalUntracked,
        SelectorOverlayLane::LocalTracked,
    ];
    if version_id != GLOBAL_VERSION_ID {
        lanes.push(SelectorOverlayLane::GlobalUntracked);
        lanes.push(SelectorOverlayLane::GlobalTracked);
    }
    lanes
}

fn selector_storage_version_id(requested_version_id: &str, lane: SelectorOverlayLane) -> String {
    if lane.is_global() {
        GLOBAL_VERSION_ID.to_string()
    } else {
        requested_version_id.to_string()
    }
}

fn selector_projected_version_id(
    requested_version_id: &str,
    lane: SelectorOverlayLane,
    source_version_id: &str,
) -> String {
    if lane.is_global() && source_version_id == GLOBAL_VERSION_ID {
        requested_version_id.to_string()
    } else {
        source_version_id.to_string()
    }
}

fn compile_selector_filters(planned_write: &PlannedWrite) -> Option<Vec<PendingViewFilter>> {
    let mut placeholder_state = PlaceholderState::new();
    planned_write
        .command
        .selector
        .residual_predicates
        .iter()
        .map(|predicate| {
            selector_filter_from_expr(
                predicate,
                &planned_write.command.bound_parameters,
                &mut placeholder_state,
            )
        })
        .collect()
}

fn selector_filter_from_expr(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Option<PendingViewFilter> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => Some(PendingViewFilter::And(vec![
            selector_filter_from_expr(left, params, placeholder_state)?,
            selector_filter_from_expr(right, params, placeholder_state)?,
        ])),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => Some(PendingViewFilter::Or(vec![
            selector_filter_from_expr(left, params, placeholder_state)?,
            selector_filter_from_expr(right, params, placeholder_state)?,
        ])),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => match (
            left.as_ref(),
            selector_value_from_expr(right, params, placeholder_state),
            right.as_ref(),
            selector_value_from_expr(left, params, placeholder_state),
        ) {
            (left, Some(value), _, _) => Some(PendingViewFilter::Equals(
                selector_identifier_name(left)?,
                value,
            )),
            (_, _, right, Some(value)) => Some(PendingViewFilter::Equals(
                selector_identifier_name(right)?,
                value,
            )),
            _ => None,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => Some(PendingViewFilter::In(
            selector_identifier_name(expr)?,
            list.iter()
                .map(|expr| selector_value_from_expr(expr, params, placeholder_state))
                .collect::<Option<Vec<_>>>()?,
        )),
        Expr::IsNull(expr) => Some(PendingViewFilter::IsNull(selector_identifier_name(expr)?)),
        Expr::IsNotNull(expr) => Some(PendingViewFilter::IsNotNull(selector_identifier_name(
            expr,
        )?)),
        Expr::Like {
            expr,
            pattern,
            negated: false,
            ..
        } => Some(PendingViewFilter::Like {
            column: selector_identifier_name(expr)?,
            pattern: selector_filter_text(&selector_value_from_expr(
                pattern,
                params,
                placeholder_state,
            )?)?,
            case_insensitive: false,
        }),
        Expr::ILike {
            expr,
            pattern,
            negated: false,
            ..
        } => Some(PendingViewFilter::Like {
            column: selector_identifier_name(expr)?,
            pattern: selector_filter_text(&selector_value_from_expr(
                pattern,
                params,
                placeholder_state,
            )?)?,
            case_insensitive: true,
        }),
        Expr::Nested(inner) => selector_filter_from_expr(inner, params, placeholder_state),
        _ => None,
    }
}

fn selector_identifier_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.clone()),
        Expr::Nested(inner) => selector_identifier_name(inner),
        _ => None,
    }
}

fn selector_value_from_expr(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Option<Value> {
    match expr {
        Expr::Nested(inner) => selector_value_from_expr(inner, params, placeholder_state),
        Expr::UnaryOp { op, expr } => {
            let value = selector_value_from_expr(expr, params, placeholder_state)?;
            match (op, value) {
                (UnaryOperator::Minus, Value::Integer(value)) => Some(Value::Integer(-value)),
                (UnaryOperator::Minus, Value::Real(value)) => Some(Value::Real(-value)),
                (UnaryOperator::Plus, value) => Some(value),
                _ => None,
            }
        }
        Expr::Value(value) => match &value.value {
            SqlValue::Placeholder(token) => {
                let index =
                    resolve_placeholder_index(token, params.len(), placeholder_state).ok()?;
                params.get(index).cloned()
            }
            _ => selector_sql_value_as_engine_value(value),
        },
        _ => None,
    }
}

fn selector_sql_value_as_engine_value(value: &ValueWithSpan) -> Option<Value> {
    match &value.value {
        SqlValue::Null => Some(Value::Null),
        SqlValue::Boolean(value) => Some(Value::Boolean(*value)),
        SqlValue::SingleQuotedString(text)
        | SqlValue::TripleSingleQuotedString(text)
        | SqlValue::EscapedStringLiteral(text)
        | SqlValue::DollarQuotedString(sqlparser::ast::DollarQuotedString {
            value: text, ..
        }) => Some(Value::Text(text.clone())),
        SqlValue::Number(value, _) => value
            .parse::<i64>()
            .map(Value::Integer)
            .or_else(|_| value.parse::<f64>().map(Value::Real))
            .ok(),
        _ => None,
    }
}

fn selector_filter_matches_row(filter: &PendingViewFilter, row: &SelectorCandidateRow) -> bool {
    match filter {
        PendingViewFilter::And(filters) => filters
            .iter()
            .all(|filter| selector_filter_matches_row(filter, row)),
        PendingViewFilter::Or(filters) => filters
            .iter()
            .any(|filter| selector_filter_matches_row(filter, row)),
        PendingViewFilter::Equals(column, expected) => {
            selector_row_value(row, column).is_some_and(|actual| actual == *expected)
        }
        PendingViewFilter::In(column, expected) => selector_row_value(row, column)
            .is_some_and(|actual| expected.iter().any(|candidate| candidate == &actual)),
        PendingViewFilter::IsNull(column) => {
            matches!(selector_row_value(row, column), Some(Value::Null) | None)
        }
        PendingViewFilter::IsNotNull(column) => {
            !matches!(selector_row_value(row, column), Some(Value::Null) | None)
        }
        PendingViewFilter::Like {
            column,
            pattern,
            case_insensitive,
        } => selector_row_value(row, column)
            .and_then(|actual| selector_filter_text(&actual))
            .is_some_and(|actual| sql_like_matches(&actual, pattern, *case_insensitive)),
    }
}

fn selector_row_value(row: &SelectorCandidateRow, column: &str) -> Option<Value> {
    match column {
        "entity_id" | "lixcol_entity_id" => Some(Value::Text(row.entity_id.clone())),
        "schema_key" | "lixcol_schema_key" => Some(Value::Text(row.schema_key.clone())),
        "schema_version" | "lixcol_schema_version" => Some(Value::Text(row.schema_version.clone())),
        "file_id" | "lixcol_file_id" => Some(Value::Text(row.file_id.clone())),
        "version_id" | "lixcol_version_id" => Some(Value::Text(row.version_id.clone())),
        "global" | "lixcol_global" => Some(Value::Boolean(row.global)),
        "untracked" | "lixcol_untracked" => Some(Value::Boolean(row.untracked)),
        "plugin_key" | "lixcol_plugin_key" => Some(Value::Text(row.plugin_key.clone())),
        "metadata" | "lixcol_metadata" => {
            Some(row.metadata.clone().map(Value::Text).unwrap_or(Value::Null))
        }
        "writer_key" => Some(
            row.writer_key
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        "snapshot_content" => Some(
            row.snapshot_content
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        other => row.values.get(other).cloned(),
    }
}

fn selector_filter_text(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(if *value { "1" } else { "0" }.to_string()),
        Value::Real(value) => Some(value.to_string()),
        Value::Json(value) => Some(value.to_string()),
        Value::Null | Value::Blob(_) => None,
    }
}

fn sql_like_matches(input: &str, pattern: &str, case_insensitive: bool) -> bool {
    let (input, pattern) = if case_insensitive {
        (input.to_ascii_lowercase(), pattern.to_ascii_lowercase())
    } else {
        (input.to_string(), pattern.to_string())
    };
    sql_like_matches_inner(input.as_bytes(), pattern.as_bytes())
}

fn sql_like_matches_inner(input: &[u8], pattern: &[u8]) -> bool {
    if pattern.is_empty() {
        return input.is_empty();
    }

    match pattern[0] {
        b'%' => {
            sql_like_matches_inner(input, &pattern[1..])
                || (!input.is_empty() && sql_like_matches_inner(&input[1..], pattern))
        }
        b'_' => !input.is_empty() && sql_like_matches_inner(&input[1..], &pattern[1..]),
        byte => {
            !input.is_empty()
                && input[0] == byte
                && sql_like_matches_inner(&input[1..], &pattern[1..])
        }
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
