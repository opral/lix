use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{Expr, Operator};
use tokio::sync::Mutex;

use crate::changelog::CanonicalChange;
use crate::commit_graph::{CommitGraphChangeHistoryRequest, CommitGraphReader};
use crate::entity_identity::EntityIdentity;
use crate::LixError;

/// Shared routing state for commit-shaped history SQL surfaces.
///
/// History providers differ in how they shape rows, but they should not drift
/// in how they interpret filters such as `start_commit_id IN (...)`, entity
/// filters, or depth ranges.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HistoryRoute {
    pub(crate) start_commit_ids: Vec<String>,
    pub(crate) entity_ids: Vec<String>,
    pub(crate) schema_keys: Vec<String>,
    pub(crate) file_ids: Vec<String>,
    pub(crate) min_depth: Option<i64>,
    pub(crate) max_depth: Option<i64>,
    pub(crate) contradictory: bool,
}

impl HistoryRoute {
    pub(crate) fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            apply_history_filter(filter, &mut route);
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
        entity_id: &str,
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
        if !self.entity_ids.is_empty()
            && !self
                .entity_ids
                .iter()
                .any(|candidate| candidate == entity_id)
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
    pub(crate) change: CanonicalChange,
    pub(crate) commit_id: String,
    pub(crate) commit_created_at: String,
    pub(crate) start_commit_id: String,
    pub(crate) depth: u32,
}

pub(crate) struct HistoryViewErrorContext<'a> {
    pub(crate) view_name: &'a str,
    pub(crate) start_commit_column: &'a str,
}

pub(crate) fn parse_history_filter(expr: &Expr) -> Option<()> {
    parse_history_filter_terms(expr).map(|_| ())
}

pub(crate) fn commit_graph_history_request(
    route: &HistoryRoute,
    schema_keys: Vec<String>,
) -> Option<CommitGraphChangeHistoryRequest> {
    let schema_keys = effective_schema_keys(route, schema_keys)?;
    Some(CommitGraphChangeHistoryRequest {
        entity_ids: route
            .entity_ids
            .iter()
            .map(|entity_id| EntityIdentity::single(entity_id))
            .collect(),
        schema_keys,
        file_ids: route.file_ids.clone(),
        min_depth: route.min_depth.and_then(nonnegative_u32),
        max_depth: route.max_depth.and_then(nonnegative_u32),
        include_tombstones: true,
    })
}

/// Loads commit-graph history once for all SQL history providers.
///
/// Providers pass the schema keys they know how to shape. An empty list means
/// "do not constrain by provider schema"; this is what `lix_state_history` uses.
pub(crate) async fn load_history_entries(
    error_context: HistoryViewErrorContext<'_>,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    route: &HistoryRoute,
    schema_keys: Vec<String>,
) -> Result<Vec<HistoryEntry>, LixError> {
    if route.is_contradictory() {
        return Ok(Vec::new());
    }
    if route.start_commit_ids.is_empty() {
        return Err(LixError::new(
            LixError::CODE_HISTORY_FILTER_REQUIRED,
            format!(
                "{} requires a {} filter",
                error_context.view_name, error_context.start_commit_column
            ),
        )
        .with_hint(format!(
            "Use WHERE {} = lix_active_version_commit_id() to inspect {} from the active version head.",
            error_context.start_commit_column, error_context.view_name
        )));
    }
    let Some(request) = commit_graph_history_request(route, schema_keys) else {
        return Ok(Vec::new());
    };

    let mut rows = Vec::new();
    let mut guard = commit_graph.lock().await;
    for start_commit_id in &route.start_commit_ids {
        let entries = guard
            .change_history_from_commit(start_commit_id, &request)
            .await?;
        let reachable_commits = guard.reachable_commits(start_commit_id).await?;
        let commit_created_at_by_id = reachable_commits
            .into_iter()
            .map(|reachable| {
                (
                    reachable.commit.commit_id.clone(),
                    reachable.commit.change.created_at.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();

        rows.extend(entries.into_iter().map(|entry| {
            HistoryEntry {
                commit_created_at: commit_created_at_by_id
                    .get(&entry.commit_id)
                    .cloned()
                    .unwrap_or_else(|| entry.change.created_at.clone()),
                change: entry.change,
                commit_id: entry.commit_id,
                start_commit_id: entry.start_commit_id,
                depth: entry.depth,
            }
        }));
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

fn parse_history_filter_terms(expr: &Expr) -> Option<Vec<HistoryFilterTerm>> {
    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
            let mut terms = parse_history_filter_terms(&binary_expr.left)?;
            terms.extend(parse_history_filter_terms(&binary_expr.right)?);
            Some(terms)
        }
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => {
            parse_history_disjunction(binary_expr)
        }
        Expr::BinaryExpr(binary_expr) => {
            parse_history_binary_filter(binary_expr).map(|term| vec![term])
        }
        Expr::InList(in_list) => parse_history_in_list_filter(in_list).map(|term| vec![term]),
        _ => None,
    }
}

fn collect_history_route_terms(expr: &Expr) -> Vec<HistoryFilterTerm> {
    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
            let mut terms = collect_history_route_terms(&binary_expr.left);
            terms.extend(collect_history_route_terms(&binary_expr.right));
            terms
        }
        // OR filters are only safe to route when the entire disjunction is a
        // supported history predicate. Partially routing one side would change
        // SQL semantics before DataFusion can apply the residual filter.
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => {
            parse_history_disjunction(binary_expr).unwrap_or_default()
        }
        Expr::BinaryExpr(binary_expr) => parse_history_binary_filter(binary_expr)
            .map(|term| vec![term])
            .unwrap_or_default(),
        Expr::InList(in_list) => parse_history_in_list_filter(in_list)
            .map(|term| vec![term])
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn parse_history_disjunction(
    binary_expr: &datafusion::logical_expr::BinaryExpr,
) -> Option<Vec<HistoryFilterTerm>> {
    let left = parse_history_filter_terms(&binary_expr.left)?;
    let right = parse_history_filter_terms(&binary_expr.right)?;
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
    EntityIds(Vec<String>),
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
        (HistoryFilterTerm::EntityIds(mut left), HistoryFilterTerm::EntityIds(right)) => {
            extend_unique(&mut left, right);
            Some(HistoryFilterTerm::EntityIds(left))
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
) -> Option<HistoryFilterTerm> {
    let Expr::Column(column) = &*binary_expr.left else {
        return None;
    };
    let column_name = canonical_history_column_name(column.name.as_str())?;
    let right = &*binary_expr.right;
    match (column_name, &binary_expr.op, right) {
        ("start_commit_id", Operator::Eq, Expr::Literal(ScalarValue::Utf8(Some(value)), _))
        | ("entity_id", Operator::Eq, Expr::Literal(ScalarValue::Utf8(Some(value)), _))
        | ("schema_key", Operator::Eq, Expr::Literal(ScalarValue::Utf8(Some(value)), _))
        | ("file_id", Operator::Eq, Expr::Literal(ScalarValue::Utf8(Some(value)), _)) => {
            Some(match column_name {
                "start_commit_id" => HistoryFilterTerm::StartCommitIds(vec![value.clone()]),
                "entity_id" => HistoryFilterTerm::EntityIds(vec![value.clone()]),
                "schema_key" => HistoryFilterTerm::SchemaKeys(vec![value.clone()]),
                "file_id" => HistoryFilterTerm::FileIds(vec![value.clone()]),
                _ => unreachable!(),
            })
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

fn parse_history_in_list_filter(in_list: &InList) -> Option<HistoryFilterTerm> {
    if in_list.negated {
        return None;
    }

    let Expr::Column(column) = in_list.expr.as_ref() else {
        return None;
    };
    let column_name = canonical_history_column_name(column.name.as_str())?;
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
        "entity_id" => Some(HistoryFilterTerm::EntityIds(values)),
        "schema_key" => Some(HistoryFilterTerm::SchemaKeys(values)),
        "file_id" => Some(HistoryFilterTerm::FileIds(values)),
        _ => None,
    }
}

fn apply_history_filter(expr: &Expr, route: &mut HistoryRoute) {
    for term in collect_history_route_terms(expr) {
        match term {
            HistoryFilterTerm::StartCommitIds(values) => {
                route.contradictory |=
                    apply_conjunctive_values_filter(&mut route.start_commit_ids, values)
            }
            HistoryFilterTerm::EntityIds(values) => {
                route.contradictory |=
                    apply_conjunctive_values_filter(&mut route.entity_ids, values)
            }
            HistoryFilterTerm::SchemaKeys(values) => {
                route.contradictory |=
                    apply_conjunctive_values_filter(&mut route.schema_keys, values)
            }
            HistoryFilterTerm::FileIds(values) => {
                route.contradictory |= apply_conjunctive_values_filter(&mut route.file_ids, values)
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

fn canonical_history_column_name(name: &str) -> Option<&str> {
    match name {
        "lixcol_start_commit_id" | "start_commit_id" => Some("start_commit_id"),
        "lixcol_entity_id" | "entity_id" => Some("entity_id"),
        "lixcol_schema_key" | "schema_key" => Some("schema_key"),
        "lixcol_file_id" | "file_id" => Some("file_id"),
        "lixcol_depth" | "depth" => Some("depth"),
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
    use datafusion::common::{Column, ScalarValue};
    use datafusion::logical_expr::{BinaryExpr, Expr, Like, Operator};

    use super::{parse_history_filter, HistoryRoute};

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
            parse_history_filter(&filter).is_none(),
            "mixed filters must not be advertised as exact pushdown"
        );

        let route = HistoryRoute::from_filters(&[filter]);
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

        let route = HistoryRoute::from_filters(&[filter]);
        assert!(
            route.start_commit_ids.is_empty(),
            "partial OR pushdown would change SQL semantics"
        );
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
