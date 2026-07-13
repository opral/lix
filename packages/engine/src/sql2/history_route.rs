use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{Expr, Operator};
use tokio::sync::Mutex;

use crate::LixError;
use crate::changelog::CommitId;
use crate::commit_graph::{CommitGraphChangeHistoryRequest, CommitGraphReader};
use crate::entity_pk::EntityPk;

use super::SqlJsonReader;
use crate::sql2::change_materialization::{MaterializedChange, materialize_located_history_change};
use crate::storage::StorageRead;

/// Shared routing state for commit-shaped history SQL surfaces.
///
/// History providers differ in how they shape rows, but they should not drift
/// in how they interpret filters such as `start_commit_id IN (...)`, entity
/// filters, or depth ranges.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HistoryRoute {
    pub(crate) start_commit_ids: Vec<String>,
    pub(crate) entity_pks: Vec<String>,
    pub(crate) schema_keys: Vec<String>,
    pub(crate) file_ids: Vec<String>,
    pub(crate) min_depth: Option<i64>,
    pub(crate) max_depth: Option<i64>,
    pub(crate) contradictory: bool,
}

impl HistoryRoute {
    pub(crate) fn from_filters(filters: &[Expr], column_style: HistoryColumnStyle) -> Self {
        let mut route = Self::default();
        for filter in filters {
            apply_history_filter(filter, &mut route, column_style);
        }
        route
    }

    /// Returns the part of the route that is safe to apply before a shaped
    /// history provider has built its output rows.
    ///
    /// Surface providers such as `lix_file_history` may be caused by different
    /// canonical event schemas than the schema they expose. For those providers,
    /// identity/schema filters must be evaluated against the shaped output row,
    /// not against the canonical event row.
    pub(crate) fn traversal_only(&self) -> Self {
        Self {
            start_commit_ids: self.start_commit_ids.clone(),
            min_depth: self.min_depth,
            max_depth: self.max_depth,
            contradictory: self.contradictory,
            ..Self::default()
        }
    }

    /// Returns only the explicit history starts.
    ///
    /// Shaped history providers use this for context loading: path/data shaping
    /// often needs ancestor descriptor rows even when the event route is
    /// restricted to a specific depth.
    pub(crate) fn starts_only(&self) -> Self {
        Self {
            start_commit_ids: self.start_commit_ids.clone(),
            contradictory: self.contradictory,
            ..Self::default()
        }
    }

    pub(crate) fn is_contradictory(&self) -> bool {
        self.contradictory
            || self
                .min_depth
                .zip(self.max_depth)
                .is_some_and(|(min, max)| min > max)
            || self.min_depth.is_some_and(|depth| depth < 0)
            || self.max_depth.is_some_and(|depth| depth < 0)
    }

    /// Checks filters that refer to the row exposed by a shaped history surface.
    pub(crate) fn matches_surface_row(
        &self,
        schema_key: &str,
        entity_pk: &str,
        file_id: Option<&str>,
        depth: u32,
    ) -> bool {
        if self.is_contradictory() {
            return false;
        }
        if !self.schema_keys.is_empty()
            && !self
                .schema_keys
                .iter()
                .any(|candidate| candidate == schema_key)
        {
            return false;
        }
        if !self.entity_pks.is_empty()
            && !self
                .entity_pks
                .iter()
                .any(|candidate| candidate == entity_pk)
        {
            return false;
        }
        if !self.file_ids.is_empty() {
            let Some(file_id) = file_id else {
                return false;
            };
            if !self.file_ids.iter().any(|candidate| candidate == file_id) {
                return false;
            }
        }
        if self
            .min_depth
            .is_some_and(|min_depth| i64::from(depth) < min_depth)
        {
            return false;
        }
        if self
            .max_depth
            .is_some_and(|max_depth| i64::from(depth) > max_depth)
        {
            return false;
        }
        true
    }
}

/// Commit-graph history entry enriched with commit metadata needed by SQL
/// history surfaces.
#[derive(Debug, Clone)]
pub(crate) struct HistoryEntry {
    pub(crate) change: MaterializedChange,
    pub(crate) observed_commit_id: String,
    pub(crate) commit_created_at: String,
    pub(crate) start_commit_id: String,
    pub(crate) depth: u32,
}

pub(crate) const HISTORY_COL_ENTITY_PK: &str = "lixcol_entity_pk";
pub(crate) const HISTORY_COL_SCHEMA_KEY: &str = "lixcol_schema_key";
pub(crate) const HISTORY_COL_FILE_ID: &str = "lixcol_file_id";
pub(crate) const HISTORY_COL_SNAPSHOT_CONTENT: &str = "lixcol_snapshot_content";
pub(crate) const HISTORY_COL_METADATA: &str = "lixcol_metadata";
pub(crate) const HISTORY_COL_CHANGE_ID: &str = "lixcol_change_id";
pub(crate) const HISTORY_COL_ORIGIN_KEY: &str = "lixcol_origin_key";
pub(crate) const HISTORY_COL_OBSERVED_COMMIT_ID: &str = "lixcol_observed_commit_id";
pub(crate) const HISTORY_COL_COMMIT_CREATED_AT: &str = "lixcol_commit_created_at";
pub(crate) const HISTORY_COL_START_COMMIT_ID: &str = "lixcol_start_commit_id";
pub(crate) const HISTORY_COL_DEPTH: &str = "lixcol_depth";

pub(crate) struct HistoryViewDescriptor<'a> {
    pub(crate) view_name: &'a str,
    pub(crate) start_commit_column: &'a str,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum HistoryColumnStyle {
    Bare,
    Prefixed,
}

/// Commit metadata that a history scan must materialize for its projected
/// columns and residual filters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct HistoryMetadataProjection {
    commit_created_at: bool,
}

impl HistoryMetadataProjection {
    pub(crate) fn from_scan(
        projected_schema: &SchemaRef,
        filters: &[Expr],
        column_style: HistoryColumnStyle,
    ) -> Self {
        let column_name = match column_style {
            HistoryColumnStyle::Bare => "commit_created_at",
            HistoryColumnStyle::Prefixed => HISTORY_COL_COMMIT_CREATED_AT,
        };
        let commit_created_at = projected_schema.field_with_name(column_name).is_ok()
            || filters.iter().any(|filter| {
                filter
                    .column_refs()
                    .iter()
                    .any(|column| column.name == column_name)
            });
        Self { commit_created_at }
    }

    #[cfg(test)]
    fn commit_created_at(self) -> bool {
        self.commit_created_at
    }
}

/// Shaped history views expose delete events as tombstone rows.
///
/// If the current event is the descriptor tombstone itself, the provider must
/// use that tombstone row instead of looking through to an earlier live
/// descriptor. This keeps one contract across typed entity, file, directory,
/// and state history: `snapshot_content IS NULL` means projected user/domain
/// columns are NULL while metadata columns still identify the event.
pub(crate) fn history_descriptor_event_matches(
    descriptor_entry: &HistoryEntry,
    event_depth: u32,
    event_change_id: &str,
) -> bool {
    descriptor_entry.depth == event_depth && descriptor_entry.change.id == event_change_id
}

pub(crate) fn parse_history_filter(expr: &Expr, column_style: HistoryColumnStyle) -> Option<()> {
    parse_history_filter_terms(expr, column_style).map(|_| ())
}

pub(crate) fn commit_graph_history_request(
    route: &HistoryRoute,
    schema_keys: Vec<String>,
) -> Option<CommitGraphChangeHistoryRequest> {
    let schema_keys = effective_schema_keys(route, schema_keys)?;
    Some(CommitGraphChangeHistoryRequest {
        entity_pks: route
            .entity_pks
            .iter()
            .filter_map(|entity_pk| EntityPk::from_json_array_text(entity_pk).ok())
            .collect(),
        schema_keys,
        file_ids: route.file_ids.clone(),
        min_depth: route.min_depth.and_then(nonnegative_u32),
        max_depth: route.max_depth.and_then(nonnegative_u32),
        include_tombstones: true,
    })
}

/// Loads reachability-aware commit-graph history once for all SQL history providers.
///
/// Providers pass the schema keys they know how to shape. An empty list means
/// "do not constrain by provider schema"; this is what `lix_state_history` uses.
pub(crate) async fn load_history_entries<S>(
    descriptor: HistoryViewDescriptor<'_>,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    mut json_reader: SqlJsonReader<S>,
    route: &HistoryRoute,
    schema_keys: Vec<String>,
    metadata_projection: HistoryMetadataProjection,
) -> Result<Vec<HistoryEntry>, LixError>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    if route.is_contradictory() {
        return Ok(Vec::new());
    }
    if route.start_commit_ids.is_empty() {
        return Err(LixError::new(
            LixError::CODE_HISTORY_FILTER_REQUIRED,
            format!(
                "{} requires a {} filter",
                descriptor.view_name, descriptor.start_commit_column
            ),
        )
        .with_hint(format!(
            "Use WHERE {} = lix_active_branch_commit_id() to inspect {} from the active branch head.",
            descriptor.start_commit_column, descriptor.view_name
        )));
    }
    let Some(request) = commit_graph_history_request(route, schema_keys) else {
        return Ok(Vec::new());
    };

    let mut rows = Vec::new();
    for start_commit_id in &route.start_commit_ids {
        let start_commit_id = CommitId::parse_lix(start_commit_id, "history start_commit_id")?;
        let (entries, reachable_commits) = {
            let mut guard = commit_graph.lock().await;
            let entries = guard
                .change_history_from_commit(&start_commit_id, &request)
                .await?;
            let reachable_commits = if metadata_projection.commit_created_at {
                guard.reachable_commits(&start_commit_id).await?
            } else {
                Vec::new()
            };
            (entries, reachable_commits)
        };
        let commit_created_at_by_id = reachable_commits
            .into_iter()
            .map(|reachable| {
                (
                    reachable.commit.commit_id,
                    reachable.commit.change.created_at.to_string(),
                )
            })
            .collect::<BTreeMap<_, _>>();

        for entry in entries {
            let change = materialize_located_history_change(&mut json_reader, entry.change).await?;
            rows.push(HistoryEntry {
                commit_created_at: commit_created_at_by_id
                    .get(&entry.observed_commit_id)
                    .cloned()
                    .unwrap_or_else(|| change.created_at.clone()),
                change,
                observed_commit_id: entry.observed_commit_id.to_string(),
                start_commit_id: entry.start_commit_id.to_string(),
                depth: entry.depth,
            });
        }
    }

    Ok(rows)
}

fn effective_schema_keys(
    route: &HistoryRoute,
    surface_schema_keys: Vec<String>,
) -> Option<Vec<String>> {
    if surface_schema_keys.is_empty() {
        return Some(route.schema_keys.clone());
    }
    if route.schema_keys.is_empty() {
        return Some(surface_schema_keys);
    }

    let mut effective = Vec::new();
    for schema_key in surface_schema_keys {
        if route.schema_keys.contains(&schema_key) && !effective.contains(&schema_key) {
            effective.push(schema_key);
        }
    }
    if effective.is_empty() {
        None
    } else {
        Some(effective)
    }
}

fn parse_history_filter_terms(
    expr: &Expr,
    column_style: HistoryColumnStyle,
) -> Option<Vec<HistoryFilterTerm>> {
    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
            let mut terms = parse_history_filter_terms(&binary_expr.left, column_style)?;
            terms.extend(parse_history_filter_terms(
                &binary_expr.right,
                column_style,
            )?);
            Some(terms)
        }
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => {
            parse_history_disjunction(binary_expr, column_style)
        }
        Expr::BinaryExpr(binary_expr) => {
            parse_history_binary_filter(binary_expr, column_style).map(|term| vec![term])
        }
        Expr::InList(in_list) => {
            parse_history_in_list_filter(in_list, column_style).map(|term| vec![term])
        }
        _ => None,
    }
}

fn collect_history_route_terms(
    expr: &Expr,
    column_style: HistoryColumnStyle,
) -> Vec<HistoryFilterTerm> {
    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
            let mut terms = collect_history_route_terms(&binary_expr.left, column_style);
            terms.extend(collect_history_route_terms(
                &binary_expr.right,
                column_style,
            ));
            terms
        }
        // OR filters are only safe to route when the entire disjunction is a
        // supported history predicate. Partially routing one side would change
        // SQL semantics before DataFusion can apply the residual filter.
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => {
            parse_history_disjunction(binary_expr, column_style).unwrap_or_default()
        }
        Expr::BinaryExpr(binary_expr) => parse_history_binary_filter(binary_expr, column_style)
            .map(|term| vec![term])
            .unwrap_or_default(),
        Expr::InList(in_list) => parse_history_in_list_filter(in_list, column_style)
            .map(|term| vec![term])
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn parse_history_disjunction(
    binary_expr: &datafusion::logical_expr::BinaryExpr,
    column_style: HistoryColumnStyle,
) -> Option<Vec<HistoryFilterTerm>> {
    let left = parse_history_filter_terms(&binary_expr.left, column_style)?;
    let right = parse_history_filter_terms(&binary_expr.right, column_style)?;
    let [left] = left.as_slice() else {
        return None;
    };
    let [right] = right.as_slice() else {
        return None;
    };
    merge_history_disjunction_terms(left.clone(), right.clone()).map(|term| vec![term])
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HistoryFilterTerm {
    StartCommitIds(Vec<String>),
    EntityPks(Vec<String>),
    SchemaKeys(Vec<String>),
    FileIds(Vec<String>),
    MinDepth(i64),
    MaxDepth(i64),
    ExactDepth(i64),
}

fn merge_history_disjunction_terms(
    left: HistoryFilterTerm,
    right: HistoryFilterTerm,
) -> Option<HistoryFilterTerm> {
    match (left, right) {
        (HistoryFilterTerm::StartCommitIds(mut left), HistoryFilterTerm::StartCommitIds(right)) => {
            extend_unique(&mut left, right);
            Some(HistoryFilterTerm::StartCommitIds(left))
        }
        (HistoryFilterTerm::EntityPks(mut left), HistoryFilterTerm::EntityPks(right)) => {
            extend_unique(&mut left, right);
            Some(HistoryFilterTerm::EntityPks(left))
        }
        (HistoryFilterTerm::FileIds(mut left), HistoryFilterTerm::FileIds(right)) => {
            extend_unique(&mut left, right);
            Some(HistoryFilterTerm::FileIds(left))
        }
        (HistoryFilterTerm::SchemaKeys(mut left), HistoryFilterTerm::SchemaKeys(right)) => {
            extend_unique(&mut left, right);
            Some(HistoryFilterTerm::SchemaKeys(left))
        }
        _ => None,
    }
}

fn parse_history_binary_filter(
    binary_expr: &datafusion::logical_expr::BinaryExpr,
    column_style: HistoryColumnStyle,
) -> Option<HistoryFilterTerm> {
    let Expr::Column(column) = &*binary_expr.left else {
        return None;
    };
    let column_name = canonical_history_column_name(column.name.as_str(), column_style)?;
    let right = &*binary_expr.right;
    match (column_name, &binary_expr.op, right) {
        (
            "start_commit_id" | "schema_key" | "file_id",
            Operator::Eq,
            Expr::Literal(ScalarValue::Utf8(Some(value)), _),
        ) => Some(match column_name {
            "start_commit_id" => HistoryFilterTerm::StartCommitIds(vec![value.clone()]),
            "schema_key" => HistoryFilterTerm::SchemaKeys(vec![value.clone()]),
            "file_id" => HistoryFilterTerm::FileIds(vec![value.clone()]),
            _ => unreachable!(),
        }),
        ("entity_pk", Operator::Eq, Expr::Literal(ScalarValue::Utf8(Some(value)), _)) => {
            canonical_entity_pk_value(value).map(|value| HistoryFilterTerm::EntityPks(vec![value]))
        }
        ("depth", Operator::Eq, depth_expr) => {
            scalar_i64_literal(depth_expr).map(HistoryFilterTerm::ExactDepth)
        }
        ("depth", Operator::Gt, depth_expr) => {
            scalar_i64_literal(depth_expr).map(|value| HistoryFilterTerm::MinDepth(value + 1))
        }
        ("depth", Operator::GtEq, depth_expr) => {
            scalar_i64_literal(depth_expr).map(HistoryFilterTerm::MinDepth)
        }
        ("depth", Operator::Lt, depth_expr) => {
            scalar_i64_literal(depth_expr).map(|value| HistoryFilterTerm::MaxDepth(value - 1))
        }
        ("depth", Operator::LtEq, depth_expr) => {
            scalar_i64_literal(depth_expr).map(HistoryFilterTerm::MaxDepth)
        }
        _ => None,
    }
}

fn parse_history_in_list_filter(
    in_list: &InList,
    column_style: HistoryColumnStyle,
) -> Option<HistoryFilterTerm> {
    if in_list.negated {
        return None;
    }

    let Expr::Column(column) = in_list.expr.as_ref() else {
        return None;
    };
    let column_name = canonical_history_column_name(column.name.as_str(), column_style)?;
    let values = in_list
        .list
        .iter()
        .map(string_literal)
        .collect::<Option<Vec<_>>>()?;
    if values.is_empty() {
        return None;
    }

    match column_name {
        "start_commit_id" => Some(HistoryFilterTerm::StartCommitIds(values)),
        "entity_pk" => canonical_entity_pk_values(values).map(HistoryFilterTerm::EntityPks),
        "schema_key" => Some(HistoryFilterTerm::SchemaKeys(values)),
        "file_id" => Some(HistoryFilterTerm::FileIds(values)),
        _ => None,
    }
}

fn apply_history_filter(expr: &Expr, route: &mut HistoryRoute, column_style: HistoryColumnStyle) {
    for term in collect_history_route_terms(expr, column_style) {
        match term {
            HistoryFilterTerm::StartCommitIds(values) => {
                route.contradictory |=
                    apply_conjunctive_values_filter(&mut route.start_commit_ids, values);
            }
            HistoryFilterTerm::EntityPks(values) => {
                route.contradictory |=
                    apply_conjunctive_values_filter(&mut route.entity_pks, values);
            }
            HistoryFilterTerm::SchemaKeys(values) => {
                route.contradictory |=
                    apply_conjunctive_values_filter(&mut route.schema_keys, values);
            }
            HistoryFilterTerm::FileIds(values) => {
                route.contradictory |= apply_conjunctive_values_filter(&mut route.file_ids, values);
            }
            HistoryFilterTerm::ExactDepth(value) => {
                route.min_depth = Some(value);
                route.max_depth = Some(value);
            }
            HistoryFilterTerm::MinDepth(value) => {
                route.min_depth = Some(route.min_depth.map_or(value, |current| current.max(value)));
            }
            HistoryFilterTerm::MaxDepth(value) => {
                route.max_depth = Some(route.max_depth.map_or(value, |current| current.min(value)));
            }
        }
    }
}

fn apply_conjunctive_values_filter(bucket: &mut Vec<String>, incoming_values: Vec<String>) -> bool {
    let mut values = Vec::new();
    extend_unique(&mut values, incoming_values);
    if values.is_empty() {
        return true;
    }
    if bucket.is_empty() {
        extend_unique(bucket, values);
        return false;
    }

    bucket.retain(|existing| values.contains(existing));
    bucket.is_empty()
}

fn canonical_entity_pk_values(values: Vec<String>) -> Option<Vec<String>> {
    values
        .into_iter()
        .map(|value| canonical_entity_pk_value(&value))
        .collect()
}

fn canonical_entity_pk_value(value: &str) -> Option<String> {
    EntityPk::from_json_array_text(value)
        .ok()?
        .as_json_array_text()
        .ok()
}

fn canonical_history_column_name(name: &str, column_style: HistoryColumnStyle) -> Option<&str> {
    match (column_style, name) {
        (HistoryColumnStyle::Bare, "start_commit_id")
        | (HistoryColumnStyle::Prefixed, "lixcol_start_commit_id") => Some("start_commit_id"),
        (HistoryColumnStyle::Bare, "entity_pk")
        | (HistoryColumnStyle::Prefixed, "lixcol_entity_pk") => Some("entity_pk"),
        (HistoryColumnStyle::Bare, "schema_key")
        | (HistoryColumnStyle::Prefixed, "lixcol_schema_key") => Some("schema_key"),
        (HistoryColumnStyle::Bare, "file_id")
        | (HistoryColumnStyle::Prefixed, "lixcol_file_id") => Some("file_id"),
        (HistoryColumnStyle::Bare, "depth") | (HistoryColumnStyle::Prefixed, "lixcol_depth") => {
            Some("depth")
        }
        _ => None,
    }
}

fn nonnegative_u32(value: i64) -> Option<u32> {
    u32::try_from(value).ok()
}

fn extend_unique(bucket: &mut Vec<String>, values: Vec<String>) {
    for value in values {
        if !bucket.contains(&value) {
            bucket.push(value);
        }
    }
}

fn string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _) => Some(value.clone()),
        _ => None,
    }
}

fn scalar_i64_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int8(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int16(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Some(*value),
        Expr::Literal(ScalarValue::UInt8(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt16(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => i64::try_from(*value).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{Column, ScalarValue};
    use datafusion::logical_expr::{BinaryExpr, Expr, Like, Operator};
    use tokio::sync::Mutex;

    use crate::LixError;
    use crate::changelog::{ChangeId, CommitId};
    use crate::commit_graph::{
        CommitGraphChange, CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest,
        CommitGraphCommit, CommitGraphReader, ReachableCommitGraphCommit,
    };
    use crate::entity_pk::EntityPk;
    use crate::json_store::{JsonSlot, JsonStoreContext};
    use crate::storage::{
        InMemoryStorageBackend, InMemoryStorageRead, SharedStorageRead, StorageContext,
        StorageReadOptions,
    };

    use super::{
        HistoryColumnStyle, HistoryMetadataProjection, HistoryRoute, HistoryViewDescriptor,
        load_history_entries, parse_history_filter,
    };

    #[test]
    fn route_extraction_keeps_supported_terms_from_mixed_and_filter() {
        let filter = and(
            eq(col("start_commit_id"), str_lit("commit-1")),
            Expr::Like(Like::new(
                false,
                Box::new(col("path")),
                Box::new(str_lit("/docs/%")),
                None,
                false,
            )),
        );

        assert!(
            parse_history_filter(&filter, HistoryColumnStyle::Bare).is_none(),
            "mixed filters must not be advertised as exact pushdown"
        );

        let route = HistoryRoute::from_filters(&[filter], HistoryColumnStyle::Bare);
        assert_eq!(route.start_commit_ids, vec!["commit-1".to_string()]);
    }

    #[test]
    fn route_extraction_does_not_partially_route_mixed_or_filter() {
        let filter = or(
            eq(col("start_commit_id"), str_lit("commit-1")),
            Expr::Like(Like::new(
                false,
                Box::new(col("path")),
                Box::new(str_lit("/docs/%")),
                None,
                false,
            )),
        );

        let route = HistoryRoute::from_filters(&[filter], HistoryColumnStyle::Bare);
        assert!(
            route.start_commit_ids.is_empty(),
            "partial OR pushdown would change SQL semantics"
        );
    }

    #[test]
    fn commit_metadata_projection_tracks_projection_and_filters() {
        let unrelated_schema = Arc::new(Schema::new(vec![Field::new(
            "depth",
            DataType::Int64,
            false,
        )]));
        assert!(
            !HistoryMetadataProjection::from_scan(
                &unrelated_schema,
                &[],
                HistoryColumnStyle::Bare,
            )
            .commit_created_at()
        );

        let projected_schema = Arc::new(Schema::new(vec![Field::new(
            "lixcol_commit_created_at",
            DataType::Utf8,
            false,
        )]));
        assert!(
            HistoryMetadataProjection::from_scan(
                &projected_schema,
                &[],
                HistoryColumnStyle::Prefixed,
            )
            .commit_created_at()
        );

        let residual_filter = eq(col("commit_created_at"), str_lit("2026-07-12T00:00:00Z"));
        assert!(
            HistoryMetadataProjection::from_scan(
                &unrelated_schema,
                &[residual_filter],
                HistoryColumnStyle::Bare,
            )
            .commit_created_at()
        );
    }

    #[tokio::test]
    async fn history_loader_skips_unprojected_commit_metadata_walk() {
        let reachable_calls = Arc::new(AtomicUsize::new(0));
        let start_commit_id = CommitId::for_test_label("start");
        let rows = load_history_entries(
            HistoryViewDescriptor {
                view_name: "test_history",
                start_commit_column: "start_commit_id",
            },
            test_commit_graph(Arc::clone(&reachable_calls), start_commit_id),
            empty_json_reader().await,
            &HistoryRoute {
                start_commit_ids: vec![start_commit_id.to_string()],
                ..HistoryRoute::default()
            },
            vec!["message".to_string()],
            HistoryMetadataProjection::default(),
        )
        .await
        .expect("history load should succeed without commit metadata");

        assert_eq!(reachable_calls.load(Ordering::SeqCst), 0);
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn history_loader_preserves_projected_commit_timestamp() {
        let reachable_calls = Arc::new(AtomicUsize::new(0));
        let start_commit_id = CommitId::for_test_label("start");
        let metadata_schema = Arc::new(Schema::new(vec![Field::new(
            "commit_created_at",
            DataType::Utf8,
            false,
        )]));
        let rows = load_history_entries(
            HistoryViewDescriptor {
                view_name: "test_history",
                start_commit_column: "start_commit_id",
            },
            test_commit_graph(Arc::clone(&reachable_calls), start_commit_id),
            empty_json_reader().await,
            &HistoryRoute {
                start_commit_ids: vec![start_commit_id.to_string()],
                ..HistoryRoute::default()
            },
            vec!["message".to_string()],
            HistoryMetadataProjection::from_scan(&metadata_schema, &[], HistoryColumnStyle::Bare),
        )
        .await
        .expect("history load should enrich commit metadata");

        assert_eq!(reachable_calls.load(Ordering::SeqCst), 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].commit_created_at, commit_timestamp().to_string());
    }

    struct CountingCommitGraphReader {
        reachable_calls: Arc<AtomicUsize>,
        start_commit_id: CommitId,
    }

    #[async_trait::async_trait]
    impl CommitGraphReader for CountingCommitGraphReader {
        async fn load_commit(
            &mut self,
            _commit_id: &CommitId,
        ) -> Result<Option<CommitGraphCommit>, LixError> {
            Ok(None)
        }

        async fn reachable_commits(
            &mut self,
            _head_commit_id: &CommitId,
        ) -> Result<Vec<ReachableCommitGraphCommit>, LixError> {
            self.reachable_calls.fetch_add(1, Ordering::SeqCst);
            let change = test_change("commit-change", commit_timestamp());
            Ok(vec![ReachableCommitGraphCommit {
                commit: CommitGraphCommit {
                    canonical_change: change.clone(),
                    change,
                    commit_id: self.start_commit_id,
                    change_ids: vec![ChangeId::for_test_label("entity-change")],
                    author_account_ids: Vec::new(),
                    parent_commit_ids: Vec::new(),
                },
                depth: 0,
            }])
        }

        async fn change_history_from_commit(
            &mut self,
            _start_commit_id: &CommitId,
            _request: &CommitGraphChangeHistoryRequest,
        ) -> Result<Vec<CommitGraphChangeHistoryEntry>, LixError> {
            Ok(vec![CommitGraphChangeHistoryEntry {
                change: test_change("entity-change", event_timestamp()),
                observed_commit_id: self.start_commit_id,
                start_commit_id: self.start_commit_id,
                depth: 0,
            }])
        }
    }

    fn test_commit_graph(
        reachable_calls: Arc<AtomicUsize>,
        start_commit_id: CommitId,
    ) -> Arc<Mutex<Box<dyn CommitGraphReader>>> {
        Arc::new(Mutex::new(Box::new(CountingCommitGraphReader {
            reachable_calls,
            start_commit_id,
        })))
    }

    fn test_change(label: &str, created_at: crate::common::LixTimestamp) -> CommitGraphChange {
        CommitGraphChange {
            id: ChangeId::for_test_label(label),
            entity_pk: EntityPk::single("entity-1"),
            schema_key: "message".to_string(),
            file_id: None,
            snapshot: JsonSlot::None,
            metadata: JsonSlot::None,
            created_at,
            origin_key: None,
        }
    }

    fn event_timestamp() -> crate::common::LixTimestamp {
        crate::common::LixTimestamp::expect_parse("event timestamp", "2026-07-11T00:00:00Z")
    }

    fn commit_timestamp() -> crate::common::LixTimestamp {
        crate::common::LixTimestamp::expect_parse("commit timestamp", "2026-07-12T00:00:00Z")
    }

    async fn empty_json_reader()
    -> crate::sql2::SqlJsonReader<SharedStorageRead<InMemoryStorageRead>> {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let read_scope = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        JsonStoreContext::new().reader(SharedStorageRead::new(read_scope))
    }

    fn and(left: Expr, right: Expr) -> Expr {
        binary(left, Operator::And, right)
    }

    fn or(left: Expr, right: Expr) -> Expr {
        binary(left, Operator::Or, right)
    }

    fn eq(left: Expr, right: Expr) -> Expr {
        binary(left, Operator::Eq, right)
    }

    fn binary(left: Expr, op: Operator, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
    }

    fn col(name: &str) -> Expr {
        Expr::Column(Column::from_name(name))
    }

    fn str_lit(value: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(value.to_string())), None)
    }
}
