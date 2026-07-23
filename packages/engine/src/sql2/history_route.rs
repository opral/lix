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

use super::SqlHistoryQuerySource;
use crate::sql2::change_materialization::{MaterializedChange, materialize_located_history_change};
use crate::storage_adapter::StorageAdapterRead;

/// Shared routing state for commit-shaped history SQL surfaces.
///
/// History providers differ in how they shape rows, but they should not drift
/// in how they interpret filters such as `lixcol_as_of_commit_id IN (...)`, entity
/// filters, or depth ranges.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HistoryRoute {
    pub(crate) as_of_commit_ids: Vec<String>,
    pub(crate) entity_pks: Vec<String>,
    pub(crate) schema_keys: Vec<String>,
    pub(crate) file_ids: Vec<String>,
    pub(crate) min_depth: Option<i64>,
    pub(crate) max_depth: Option<i64>,
    /// An anchor column appeared in a predicate that cannot be routed exactly.
    ///
    /// This must be rejected rather than treated as an anchor-free query,
    /// because anchor-free queries default to the pinned active head.
    pub(crate) invalid_as_of_commit_filter: bool,
    pub(crate) contradictory: bool,
}

impl HistoryRoute {
    pub(crate) fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            route.invalid_as_of_commit_filter |= !history_anchor_filter_is_exact(filter);
            apply_history_filter(filter, &mut route);
        }
        route
    }

    /// Materializes the session-pinned head when no explicit time-travel
    /// anchor was routed.
    ///
    /// Filesystem history consumers inspect the route before loading rows to
    /// resolve commit parents and ancestor projection changes, so the default
    /// must be visible on the route itself rather than only inside the loader.
    pub(crate) fn default_to_as_of_commit_id(&mut self, commit_id: &str) {
        if self.as_of_commit_ids.is_empty() && !self.invalid_as_of_commit_filter {
            self.as_of_commit_ids.push(commit_id.to_string());
        }
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
            as_of_commit_ids: self.as_of_commit_ids.clone(),
            min_depth: self.min_depth,
            max_depth: self.max_depth,
            invalid_as_of_commit_filter: self.invalid_as_of_commit_filter,
            contradictory: self.contradictory,
            ..Self::default()
        }
    }

    /// Returns only the explicit history anchors.
    ///
    /// Shaped history providers use this for context loading: path/data shaping
    /// often needs ancestor descriptor rows even when the event route is
    /// restricted to a specific depth.
    pub(crate) fn anchors_only(&self) -> Self {
        Self {
            as_of_commit_ids: self.as_of_commit_ids.clone(),
            invalid_as_of_commit_filter: self.invalid_as_of_commit_filter,
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

    pub(crate) fn constrain_entity_pks(&mut self, entity_pks: Vec<String>) {
        self.contradictory |= apply_conjunctive_values_filter(&mut self.entity_pks, entity_pks);
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
    pub(crate) commit_created_at: Option<String>,
    pub(crate) as_of_commit_id: String,
    pub(crate) depth: u32,
}

pub(crate) const HISTORY_COL_ENTITY_PK: &str = "lixcol_entity_pk";
pub(crate) const HISTORY_COL_SCHEMA_KEY: &str = "lixcol_schema_key";
pub(crate) const HISTORY_COL_FILE_ID: &str = "lixcol_file_id";
pub(crate) const HISTORY_COL_SNAPSHOT_CONTENT: &str = "lixcol_snapshot_content";
pub(crate) const HISTORY_COL_METADATA: &str = "lixcol_metadata";
pub(crate) const HISTORY_COL_CHANGE_ID: &str = "lixcol_change_id";
pub(crate) const HISTORY_COL_CHANGE_CREATED_AT: &str = "lixcol_change_created_at";
pub(crate) const HISTORY_COL_SOURCE_CHANGES: &str = "lixcol_source_changes";
pub(crate) const HISTORY_COL_ORIGIN_KEY: &str = "lixcol_origin_key";
pub(crate) const HISTORY_COL_OBSERVED_COMMIT_ID: &str = "lixcol_observed_commit_id";
pub(crate) const HISTORY_COL_COMMIT_CREATED_AT: &str = "lixcol_commit_created_at";
pub(crate) const HISTORY_COL_AS_OF_COMMIT_ID: &str = "lixcol_as_of_commit_id";
pub(crate) const HISTORY_COL_DEPTH: &str = "lixcol_depth";
pub(crate) const HISTORY_COL_IS_DELETED: &str = "lixcol_is_deleted";

/// Serializes the deterministic provenance set for one composed history row.
///
/// Each object mirrors the public `lix_change` fields. Composed history uses
/// an array because one logical revision can be caused by multiple source
/// changes in the same commit.
pub(crate) fn serialize_history_source_changes(
    changes: &[MaterializedChange],
    surface_name: &str,
) -> Result<String, LixError> {
    let mut ordered_changes = changes.iter().collect::<Vec<_>>();
    ordered_changes.sort_by(|left, right| left.id.cmp(&right.id));
    let source_changes = ordered_changes
        .into_iter()
        .map(|change| {
            let entity_pk =
                serde_json::from_str::<serde_json::Value>(&change.entity_pk.as_json_array_text()?)
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("{surface_name} source entity_pk is invalid JSON: {error}"),
                        )
                    })?;
            let snapshot_content = parse_optional_source_json(
                change.snapshot_content.as_deref(),
                surface_name,
                "snapshot_content",
            )?;
            let metadata =
                parse_optional_source_json(change.metadata.as_deref(), surface_name, "metadata")?;
            Ok(serde_json::json!({
                "id": change.id,
                "entity_pk": entity_pk,
                "schema_key": change.schema_key,
                "file_id": change.file_id,
                "snapshot_content": snapshot_content,
                "metadata": metadata,
                "created_at": change.created_at,
                "origin_key": change.origin_key,
            }))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    serde_json::to_string(&source_changes).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to serialize {surface_name} source changes: {error}"),
        )
    })
}

fn parse_optional_source_json(
    value: Option<&str>,
    surface_name: &str,
    field: &str,
) -> Result<Option<serde_json::Value>, LixError> {
    value
        .map(|value| {
            serde_json::from_str(value).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("{surface_name} source {field} is invalid JSON: {error}"),
                )
            })
        })
        .transpose()
}

pub(crate) struct HistoryViewDescriptor<'a> {
    pub(crate) view_name: &'a str,
    pub(crate) as_of_commit_column: &'a str,
}

/// Commit metadata that a history scan must materialize for its projected
/// columns and residual filters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct HistoryMetadataProjection {
    commit_created_at: bool,
}

impl HistoryMetadataProjection {
    pub(crate) fn from_scan(projected_schema: &SchemaRef, filters: &[Expr]) -> Self {
        let column_name = HISTORY_COL_COMMIT_CREATED_AT;
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

pub(crate) fn parse_history_filter(expr: &Expr) -> Option<()> {
    parse_history_filter_terms(expr).map(|_| ())
}

/// Rejects an anchor predicate unless every occurrence can be routed exactly.
///
/// Without this validation an unsupported predicate could be left for
/// DataFusion as a residual filter while the provider silently defaulted its
/// traversal to the active head. That would make a time-travel query inspect
/// the wrong commit before the residual predicate removed the rows.
pub(crate) fn validate_history_anchor_filter(expr: &Expr) -> Result<(), LixError> {
    if history_anchor_filter_is_exact(expr) {
        return Ok(());
    }
    Err(invalid_history_anchor_error(
        HISTORY_COL_AS_OF_COMMIT_ID,
        None,
    ))
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
    query_source: SqlHistoryQuerySource<S>,
    route: &HistoryRoute,
    schema_keys: Vec<String>,
    metadata_projection: HistoryMetadataProjection,
) -> Result<Vec<HistoryEntry>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    if route.invalid_as_of_commit_filter {
        return Err(invalid_history_anchor_error(
            descriptor.as_of_commit_column,
            Some(descriptor.view_name),
        ));
    }
    if route.is_contradictory() {
        return Ok(Vec::new());
    }
    let Some(request) = commit_graph_history_request(route, schema_keys) else {
        return Ok(Vec::new());
    };
    let as_of_commit_ids = if route.as_of_commit_ids.is_empty() {
        std::slice::from_ref(&query_source.default_as_of_commit_id)
    } else {
        route.as_of_commit_ids.as_slice()
    };
    let mut json_reader = query_source.json_reader;

    let mut rows = Vec::new();
    for as_of_commit_id in as_of_commit_ids {
        let as_of_commit_id =
            CommitId::parse_lix(as_of_commit_id, "history lixcol_as_of_commit_id")?;
        let (entries, reachable_commits) = {
            let mut guard = commit_graph.lock().await;
            let entries = guard
                .change_history_from_commit(&as_of_commit_id, &request)
                .await?;
            let reachable_commits = if metadata_projection.commit_created_at {
                guard.reachable_commits(&as_of_commit_id).await?
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
            let commit_created_at = if metadata_projection.commit_created_at {
                Some(
                    commit_created_at_by_id
                        .get(&entry.observed_commit_id)
                        .cloned()
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!(
                                    "history commit '{}' is missing its commit timestamp",
                                    entry.observed_commit_id
                                ),
                            )
                        })?,
                )
            } else {
                None
            };
            rows.push(HistoryEntry {
                commit_created_at,
                change,
                observed_commit_id: entry.observed_commit_id.to_string(),
                as_of_commit_id: entry.start_commit_id.to_string(),
                depth: entry.depth,
            });
        }
    }

    Ok(rows)
}

pub(crate) fn invalid_history_anchor_error(
    as_of_commit_column: &str,
    view_name: Option<&str>,
) -> LixError {
    let surface = view_name.map_or_else(String::new, |view_name| format!("{view_name}: "));
    LixError::new(
        LixError::CODE_UNSUPPORTED_SQL,
        format!(
            "{surface}history anchor '{as_of_commit_column}' only supports exact equality or non-empty IN predicates that resolve directly to a history scan"
        ),
    )
    .with_hint(format!(
        "Omit {as_of_commit_column} to use the pinned active branch head, or use WHERE {as_of_commit_column} = $1 (or {as_of_commit_column} IN ($1, $2)) for time travel."
    ))
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
    AsOfCommitIds(Vec<String>),
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
        (HistoryFilterTerm::AsOfCommitIds(mut left), HistoryFilterTerm::AsOfCommitIds(right)) => {
            extend_unique(&mut left, right);
            Some(HistoryFilterTerm::AsOfCommitIds(left))
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
) -> Option<HistoryFilterTerm> {
    let (column, right) = match (&*binary_expr.left, &binary_expr.op, &*binary_expr.right) {
        (Expr::Column(column), _, right) => (column, right),
        (left, Operator::Eq, Expr::Column(column)) => (column, left),
        _ => return None,
    };
    let column_name = canonical_history_column_name(column.name.as_str())?;
    match (column_name, &binary_expr.op, right) {
        (
            "as_of_commit_id" | "schema_key" | "file_id",
            Operator::Eq,
            Expr::Literal(ScalarValue::Utf8(Some(value)), _),
        ) => Some(match column_name {
            "as_of_commit_id" => HistoryFilterTerm::AsOfCommitIds(vec![value.clone()]),
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

fn history_anchor_filter_is_exact(expr: &Expr) -> bool {
    if !history_filter_references_anchor(expr) {
        return true;
    }

    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
            history_anchor_filter_is_exact(&binary_expr.left)
                && history_anchor_filter_is_exact(&binary_expr.right)
        }
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => matches!(
            parse_history_disjunction(binary_expr).as_deref(),
            Some([HistoryFilterTerm::AsOfCommitIds(_)])
        ),
        Expr::BinaryExpr(binary_expr) => matches!(
            parse_history_binary_filter(binary_expr),
            Some(HistoryFilterTerm::AsOfCommitIds(_))
        ),
        Expr::InList(in_list) => matches!(
            parse_history_in_list_filter(in_list),
            Some(HistoryFilterTerm::AsOfCommitIds(_))
        ),
        _ => false,
    }
}

fn history_filter_references_anchor(expr: &Expr) -> bool {
    expr.column_refs().iter().any(|column| {
        canonical_history_column_name(column.name.as_str()) == Some("as_of_commit_id")
    })
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
        "as_of_commit_id" => Some(HistoryFilterTerm::AsOfCommitIds(values)),
        "entity_pk" => canonical_entity_pk_values(values).map(HistoryFilterTerm::EntityPks),
        "schema_key" => Some(HistoryFilterTerm::SchemaKeys(values)),
        "file_id" => Some(HistoryFilterTerm::FileIds(values)),
        _ => None,
    }
}

fn apply_history_filter(expr: &Expr, route: &mut HistoryRoute) {
    for term in collect_history_route_terms(expr) {
        match term {
            HistoryFilterTerm::AsOfCommitIds(values) => {
                route.contradictory |=
                    apply_conjunctive_values_filter(&mut route.as_of_commit_ids, values);
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

fn canonical_history_column_name(name: &str) -> Option<&str> {
    match name {
        HISTORY_COL_AS_OF_COMMIT_ID => Some("as_of_commit_id"),
        HISTORY_COL_ENTITY_PK => Some("entity_pk"),
        HISTORY_COL_SCHEMA_KEY => Some("schema_key"),
        HISTORY_COL_FILE_ID => Some("file_id"),
        HISTORY_COL_DEPTH => Some("depth"),
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
    use crate::sql2::HistoryQuerySource;
    use crate::storage_adapter::{
        Memory, MemoryRead, SharedStorageAdapterRead, StorageAdapter, StorageReadOptions,
    };

    use super::{
        HISTORY_COL_AS_OF_COMMIT_ID, HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH,
        HistoryMetadataProjection, HistoryRoute, HistoryViewDescriptor, load_history_entries,
        parse_history_filter,
    };

    #[test]
    fn route_extraction_keeps_supported_terms_from_mixed_and_filter() {
        let filter = and(
            eq(col(HISTORY_COL_AS_OF_COMMIT_ID), str_lit("commit-1")),
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
        assert_eq!(route.as_of_commit_ids, vec!["commit-1".to_string()]);
    }

    #[test]
    fn route_extraction_does_not_partially_route_mixed_or_filter() {
        let filter = or(
            eq(col(HISTORY_COL_AS_OF_COMMIT_ID), str_lit("commit-1")),
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
            route.as_of_commit_ids.is_empty(),
            "partial OR pushdown would change SQL semantics"
        );
    }

    #[test]
    fn routing_rejects_retired_history_column_names() {
        for retired in [
            "start_commit_id",
            "lixcol_start_commit_id",
            "entity_pk",
            "depth",
        ] {
            let filter = eq(col(retired), str_lit("value"));
            assert!(
                parse_history_filter(&filter).is_none(),
                "retired column '{retired}' must not route"
            );
            assert!(
                HistoryRoute::from_filters(&[filter])
                    .as_of_commit_ids
                    .is_empty()
            );
        }
    }

    #[test]
    fn commit_metadata_projection_tracks_projection_and_filters() {
        let unrelated_schema = Arc::new(Schema::new(vec![Field::new(
            HISTORY_COL_DEPTH,
            DataType::Int64,
            false,
        )]));
        assert!(!HistoryMetadataProjection::from_scan(&unrelated_schema, &[]).commit_created_at());

        let projected_schema = Arc::new(Schema::new(vec![Field::new(
            HISTORY_COL_COMMIT_CREATED_AT,
            DataType::Utf8,
            false,
        )]));
        assert!(HistoryMetadataProjection::from_scan(&projected_schema, &[]).commit_created_at());

        let residual_filter = eq(
            col(HISTORY_COL_COMMIT_CREATED_AT),
            str_lit("2026-07-12T00:00:00Z"),
        );
        assert!(
            HistoryMetadataProjection::from_scan(&unrelated_schema, &[residual_filter])
                .commit_created_at()
        );
    }

    #[tokio::test]
    async fn history_loader_defaults_to_pinned_head_without_metadata_walk() {
        let reachable_calls = Arc::new(AtomicUsize::new(0));
        let start_commit_id = CommitId::for_test_label("start");
        let rows = load_history_entries(
            HistoryViewDescriptor {
                view_name: "test_history",
                as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
            },
            test_commit_graph(Arc::clone(&reachable_calls), start_commit_id),
            empty_history_query_source(start_commit_id).await,
            &HistoryRoute::default(),
            vec!["message".to_string()],
            HistoryMetadataProjection::default(),
        )
        .await
        .expect("history load should succeed without commit metadata");

        assert_eq!(reachable_calls.load(Ordering::SeqCst), 0);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].commit_created_at, None);
    }

    #[tokio::test]
    async fn history_loader_preserves_projected_commit_timestamp() {
        let reachable_calls = Arc::new(AtomicUsize::new(0));
        let start_commit_id = CommitId::for_test_label("start");
        let metadata_schema = Arc::new(Schema::new(vec![Field::new(
            HISTORY_COL_COMMIT_CREATED_AT,
            DataType::Utf8,
            false,
        )]));
        let rows = load_history_entries(
            HistoryViewDescriptor {
                view_name: "test_history",
                as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
            },
            test_commit_graph(Arc::clone(&reachable_calls), start_commit_id),
            empty_history_query_source(start_commit_id).await,
            &HistoryRoute {
                as_of_commit_ids: vec![start_commit_id.to_string()],
                ..HistoryRoute::default()
            },
            vec!["message".to_string()],
            HistoryMetadataProjection::from_scan(&metadata_schema, &[]),
        )
        .await
        .expect("history load should enrich commit metadata");

        assert_eq!(reachable_calls.load(Ordering::SeqCst), 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].commit_created_at,
            Some(commit_timestamp().to_string())
        );
        assert_eq!(rows[0].change.created_at, event_timestamp().to_string());
    }

    #[tokio::test]
    async fn history_loader_does_not_substitute_change_time_for_missing_commit_time() {
        let reachable_calls = Arc::new(AtomicUsize::new(0));
        let as_of_commit_id = CommitId::for_test_label("start");
        let metadata_schema = Arc::new(Schema::new(vec![Field::new(
            HISTORY_COL_COMMIT_CREATED_AT,
            DataType::Utf8,
            false,
        )]));
        let error = load_history_entries(
            HistoryViewDescriptor {
                view_name: "test_history",
                as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
            },
            Arc::new(Mutex::new(Box::new(CountingCommitGraphReader {
                reachable_calls,
                start_commit_id: as_of_commit_id,
                include_reachable_commit: false,
            }))),
            empty_history_query_source(as_of_commit_id).await,
            &HistoryRoute {
                as_of_commit_ids: vec![as_of_commit_id.to_string()],
                ..HistoryRoute::default()
            },
            vec!["message".to_string()],
            HistoryMetadataProjection::from_scan(&metadata_schema, &[]),
        )
        .await
        .expect_err("missing commit metadata must be an explicit error");

        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
        assert!(error.message.contains("missing its commit timestamp"));
    }

    struct CountingCommitGraphReader {
        reachable_calls: Arc<AtomicUsize>,
        start_commit_id: CommitId,
        include_reachable_commit: bool,
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
            if !self.include_reachable_commit {
                return Ok(Vec::new());
            }
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
            include_reachable_commit: true,
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

    async fn empty_history_query_source(
        default_as_of_commit_id: CommitId,
    ) -> HistoryQuerySource<SharedStorageAdapterRead<MemoryRead>> {
        let storage = StorageAdapter::new(Memory::new());
        let read_scope = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let read_scope = SharedStorageAdapterRead::new(read_scope);
        HistoryQuerySource {
            store: read_scope.clone(),
            json_reader: JsonStoreContext::new().reader(read_scope),
            default_as_of_commit_id: default_as_of_commit_id.to_string(),
        }
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
