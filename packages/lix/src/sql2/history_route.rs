use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{Expr, Operator};
use tokio::sync::Mutex;

use crate::LixError;
use crate::changelog::{ChangeId, ChangeRecord, CommitId};
use crate::commit_graph::{CommitGraphChangeHistoryRequest, CommitGraphReader};
use crate::entity_pk::EntityPk;
use crate::forktree::{StateKey, encode_state_entity_prefix_bounds};

use super::SqlChangelogQuerySource;
use crate::sql2::change_materialization::{MaterializedChange, materialize_located_history_change};
use crate::storage_adapter::StorageAdapterRead;

struct AuthenticatedMemberSource {
    source_commit_id: CommitId,
    owner_depth: u32,
}

async fn authenticated_history_metadata<R>(
    reader: &crate::forktree::ForkTreeReadFacade<R>,
    commit_id: CommitId,
    depth: u32,
    created_at: String,
    account_id: Option<String>,
    include_created_at: bool,
) -> Result<(u32, String, String), LixError>
where
    R: StorageAdapterRead,
{
    if account_id.is_some() && (!include_created_at || !created_at.is_empty()) {
        return Ok((
            depth,
            created_at,
            account_id.expect("account presence checked above"),
        ));
    }
    let record = reader.load_required_commit_record(commit_id).await?;
    if record.commit_id != commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "authenticated history commit '{}' changed identity",
                commit_id
            ),
        ));
    }
    Ok((
        depth,
        if include_created_at {
            record.created_at.to_string()
        } else {
            created_at
        },
        record.account_id,
    ))
}

/// Shared routing state for commit-shaped history SQL surfaces.
///
/// History providers differ in how they shape rows, but they should not drift
/// in how they interpret filters such as `lixcol_as_of_commit_id IN (...)`, entity
/// filters, or depth ranges.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HistoryRoute {
    pub(crate) as_of_commit_ids: Vec<String>,
    pub(crate) entity_pks: Vec<String>,
    /// Schema-resolved physical identities for traversal.
    ///
    /// `entity_pks` remains the canonical JSON surface representation used to
    /// match projected rows. Keeping the typed form alongside it avoids
    /// re-inferring component types from values after routing.
    pub(crate) resolved_entity_pks: Vec<EntityPk>,
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

    pub(crate) fn set_resolved_entity_pks(&mut self, entity_pks: Vec<EntityPk>) {
        self.resolved_entity_pks = entity_pks;
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

fn certified_state_keys(
    request: &CommitGraphChangeHistoryRequest,
    entries: &[crate::commit_graph::CommitGraphChangeHistoryEntry],
    observed_commit_id: CommitId,
) -> Option<Vec<StateKey>> {
    if request.schema_keys.is_empty() || request.file_ids.is_empty() {
        return None;
    }
    let schema_keys = request
        .schema_keys
        .iter()
        .filter(|schema_key| schema_key.as_str() != "lix_commit")
        .collect::<Vec<_>>();
    if schema_keys.is_empty() {
        return Some(Vec::new());
    }
    let entries = entries
        .iter()
        .filter(|entry| entry.observed_commit_id == observed_commit_id)
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return Some(Vec::new());
    }
    let entity_pks = if request.entity_pks.is_empty() {
        entries
            .iter()
            .filter(|entry| {
                schema_keys
                    .iter()
                    .any(|key| *key == &entry.change.schema_key)
            })
            .map(|entry| entry.change.entity_pk.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    } else {
        request.entity_pks.clone()
    };
    Some(
        schema_keys
            .into_iter()
            .flat_map(|schema_key| {
                entity_pks.iter().flat_map(move |entity_pk| {
                    request.file_ids.iter().map(move |file_id| StateKey {
                        schema_key: schema_key.clone(),
                        file_id: Some(file_id.clone()),
                        entity_pk: entity_pk.clone(),
                    })
                })
            })
            .collect(),
    )
}

fn certified_state_ranges(
    request: &CommitGraphChangeHistoryRequest,
    entries: &[crate::commit_graph::CommitGraphChangeHistoryEntry],
    observed_commit_id: CommitId,
) -> Option<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    if request.schema_keys.is_empty() {
        return None;
    }
    let schema_keys = request
        .schema_keys
        .iter()
        .filter(|schema_key| schema_key.as_str() != "lix_commit")
        .collect::<Vec<_>>();
    if schema_keys.is_empty() {
        return Some(Vec::new());
    }
    let entries = entries
        .iter()
        .filter(|entry| entry.observed_commit_id == observed_commit_id)
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return Some(Vec::new());
    }
    let entity_pks = if request.entity_pks.is_empty() {
        entries
            .iter()
            .filter(|entry| {
                schema_keys
                    .iter()
                    .any(|key| *key == &entry.change.schema_key)
            })
            .map(|entry| entry.change.entity_pk.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    } else {
        request.entity_pks.clone()
    };
    if entity_pks.is_empty() {
        return Some(Vec::new());
    }
    let mut ranges = Vec::new();
    for schema_key in schema_keys {
        for entity_pk in &entity_pks {
            let bounds = encode_state_entity_prefix_bounds(schema_key, entity_pk);
            ranges.push((bounds.lower, bounds.upper));
        }
    }
    ranges.sort();
    ranges.dedup();
    Some(ranges)
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
        entity_pks: if route.resolved_entity_pks.is_empty() {
            route
                .entity_pks
                .iter()
                .filter_map(|entity_pk| EntityPk::from_json_array_text(entity_pk).ok())
                .collect()
        } else {
            route.resolved_entity_pks.clone()
        },
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
/// "do not constrain by provider schema".
pub(crate) async fn load_history_entries<S>(
    descriptor: HistoryViewDescriptor<'_>,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlChangelogQuerySource<S>,
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
    if route.as_of_commit_ids.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "history provider omitted its pinned default commit anchor",
        ));
    }
    let as_of_commit_ids = route.as_of_commit_ids.as_slice();
    let mut forktree_reader = query_source.forktree_reader;
    let mut rows = Vec::new();
    for as_of_commit_id in as_of_commit_ids {
        let as_of_commit_id =
            CommitId::parse_lix(as_of_commit_id, "history lixcol_as_of_commit_id")?;
        let (entries, reachable_by_id, certified_commit_ids) = {
            let mut guard = commit_graph.lock().await;
            let history = guard
                .change_history_from_commit(&as_of_commit_id, &request)
                .await?;
            // Reachability is also the authenticated source of certified
            // event/plugin rows.  It must not depend on whether the caller
            // projected the optional created_at column: omitting metadata is
            // a SQL projection choice, not permission to drop the topology
            // and its commit identities.
            let reachable_nodes = history.reachable_nodes;
            let mut reachable_by_id = BTreeMap::new();
            if !reachable_nodes.is_empty() {
                let certified_commit_ids = history
                    .entries
                    .iter()
                    .map(|entry| entry.observed_commit_id)
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                let authenticated_ids = if request.schema_keys.is_empty() {
                    reachable_nodes
                        .iter()
                        .map(|reachable| reachable.commit.commit_id)
                        .collect::<Vec<_>>()
                } else {
                    certified_commit_ids.clone()
                };
                let records = guard.load_commit_records(&authenticated_ids).await?;
                if records.len() != authenticated_ids.len() {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "history commit record batch length does not match reachable commit IDs",
                    ));
                }
                let mut records_by_id = BTreeMap::new();
                for (commit_id, record) in authenticated_ids.into_iter().zip(records) {
                    let Some(record) = record else {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "history commit '{}' is missing its authenticated record",
                                commit_id
                            ),
                        ));
                    };
                    if record.commit_id != commit_id {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "history commit metadata identity mismatch for '{}'",
                                commit_id
                            ),
                        ));
                    }
                    records_by_id.insert(commit_id, record);
                }
                for reachable in reachable_nodes.iter() {
                    let record = records_by_id.get(&reachable.commit.commit_id);
                    let created_at = record
                        .filter(|_| metadata_projection.commit_created_at)
                        .map_or_else(String::new, |record| record.created_at.to_string());
                    reachable_by_id.insert(
                        reachable.commit.commit_id,
                        (
                            reachable.depth,
                            created_at,
                            record.map(|record| record.account_id.clone()),
                        ),
                    );
                }
                (history.entries, reachable_by_id, certified_commit_ids)
            } else {
                (history.entries, reachable_by_id, Vec::new())
            }
        };
        // A compacting checkpoint may carry selected Change members whose
        // authenticated source commit is not on the checkpoint's first-parent
        // walk.  Keep that existing member/source edge as the only additional
        // provenance closure; do not treat an arbitrary state-row commit ID as
        // reachable merely because it is present in a state root.
        let mut member_sources_by_change = BTreeMap::<ChangeId, AuthenticatedMemberSource>::new();
        let mut member_records_by_owner_change =
            BTreeMap::<(CommitId, ChangeId), ChangeRecord>::new();
        for owner_commit_id in &certified_commit_ids {
            let owner_depth = reachable_by_id
                .get(owner_commit_id)
                .map(|(depth, _, _)| *depth)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("certified commit '{owner_commit_id}' is not reachable"),
                    )
                })?;
            let members = forktree_reader
                .load_commit_member_sources(*owner_commit_id)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "reachable commit '{owner_commit_id}' has no authenticated member closure"
                        ),
                    )
                })?;
            for (source_commit_id, record) in members {
                let change_id = record.change_id;
                if member_records_by_owner_change
                    .insert((*owner_commit_id, change_id), record)
                    .is_some()
                {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "authenticated commit '{owner_commit_id}' repeats Change '{change_id}'"
                        ),
                    ));
                }
                if let Some(previous) = member_sources_by_change.get(&change_id) {
                    if previous.source_commit_id != source_commit_id {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "authenticated Change '{change_id}' has conflicting source closure"
                            ),
                        ));
                    }
                } else {
                    member_sources_by_change.insert(
                        change_id,
                        AuthenticatedMemberSource {
                            source_commit_id,
                            owner_depth,
                        },
                    );
                }
            }
        }
        for entry in &entries {
            let change =
                materialize_located_history_change(&mut forktree_reader, entry.change.clone())
                    .await?;
            let commit_created_at = if metadata_projection.commit_created_at {
                Some(
                    reachable_by_id
                        .get(&entry.observed_commit_id)
                        .map(|(_, created_at, _)| created_at)
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
        let mut existing_change_ids = rows
            .iter()
            .map(|entry| entry.change.id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        for certified_commit_id in certified_commit_ids {
            if !reachable_by_id.contains_key(&certified_commit_id) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("certified commit '{certified_commit_id}' is not reachable"),
                ));
            }
            let certified_rows =
                if let Some(keys) = certified_state_keys(&request, &entries, certified_commit_id) {
                    forktree_reader
                        .load_state_rows_at_commit(&certified_commit_id.to_string(), &keys)
                        .await?
                        .into_iter()
                        .flatten()
                        .collect()
                } else {
                    let certified_ranges =
                        certified_state_ranges(&request, &entries, certified_commit_id);
                    match certified_ranges.as_deref() {
                        None => {
                            forktree_reader
                                .scan_state_rows_at_commit(certified_commit_id)
                                .await?
                        }
                        Some(ranges) => {
                            let mut rows = Vec::new();
                            for (lower, upper) in ranges {
                                rows.extend(
                                    forktree_reader
                                        .scan_state_rows_at_commit_range(
                                            certified_commit_id,
                                            lower,
                                            upper.as_deref(),
                                        )
                                        .await?,
                                );
                            }
                            rows
                        }
                    }
                };
            for row in certified_rows {
                let (row_depth, row_commit_created_at, account_id) = if let Some(source) =
                    member_sources_by_change.get(&row.change_id)
                {
                    let owner_record = member_records_by_owner_change
                        .get(&(row.commit_id, row.change_id))
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!(
                                    "certified historical row Change '{}' has no authenticated owner projection",
                                    row.change_id
                                ),
                            )
                        })?;
                    validate_authenticated_member_row(&forktree_reader, &row, owner_record).await?;
                    if let Some((depth, created_at, account_id)) =
                        reachable_by_id.get(&row.commit_id)
                    {
                        authenticated_history_metadata(
                            &forktree_reader,
                            row.commit_id,
                            *depth,
                            created_at.clone(),
                            account_id.clone(),
                            metadata_projection.commit_created_at,
                        )
                        .await?
                    } else if source.source_commit_id == row.commit_id {
                        let source_commit = forktree_reader
                            .load_required_commit_record(source.source_commit_id)
                            .await?;
                        (
                            source.owner_depth,
                            source_commit.created_at.to_string(),
                            source_commit.account_id,
                        )
                    } else {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "certified historical row Change '{}' disagrees with its authenticated source commit",
                                row.change_id
                            ),
                        ));
                    }
                } else if let Some((depth, created_at, account_id)) =
                    reachable_by_id.get(&row.commit_id)
                {
                    authenticated_history_metadata(
                        &forktree_reader,
                        row.commit_id,
                        *depth,
                        created_at.clone(),
                        account_id.clone(),
                        metadata_projection.commit_created_at,
                    )
                    .await?
                } else {
                    // A compacted state root can retain a row whose source
                    // commit is not on the public parent walk and was not
                    // selected again by the current checkpoint. Authenticate
                    // that row through the source commit's own member/catalog
                    // closure on this same retained read; this is provenance,
                    // not an extension of public chronology.
                    let direct_sources = forktree_reader
                        .load_commit_member_sources(row.commit_id)
                        .await?
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!(
                                    "certified historical row references commit '{}' without an authenticated source closure",
                                    row.commit_id
                                ),
                            )
                        })?;
                    let (source_commit_id, source_record) = direct_sources
                        .into_iter()
                        .find(|(_, record)| record.change_id == row.change_id)
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!(
                                    "certified historical row Change '{}' is absent from its authenticated source commit",
                                    row.change_id
                                ),
                            )
                        })?;
                    if source_commit_id != row.commit_id {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "certified historical row Change '{}' has a substituted source commit",
                                row.change_id
                            ),
                        ));
                    }
                    let source_commit = forktree_reader
                        .load_required_commit_record(source_commit_id)
                        .await?;
                    validate_authenticated_member_row(&forktree_reader, &row, &source_record)
                        .await?;
                    (
                        reachable_by_id
                            .get(&certified_commit_id)
                            .map(|(depth, _, _)| *depth)
                            .ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_INTERNAL_ERROR,
                                    format!(
                                        "certified owner commit '{}' is missing its authenticated depth",
                                        certified_commit_id
                                    ),
                                )
                            })?,
                        source_commit.created_at.to_string(),
                        source_commit.account_id,
                    )
                };
                if !historical_row_matches_request(&row, &request) {
                    continue;
                }
                if request.min_depth.is_some_and(|minimum| row_depth < minimum)
                    || request.max_depth.is_some_and(|maximum| row_depth > maximum)
                {
                    continue;
                }
                let change_id = row.change_id.to_string();
                if !existing_change_ids.insert(change_id.clone()) {
                    continue;
                }
                rows.push(HistoryEntry {
                    change: MaterializedChange {
                        id: change_id,
                        account_id,
                        entity_pk: row.key.entity_pk,
                        schema_key: row.key.schema_key,
                        file_id: row.key.file_id,
                        snapshot_content: row.snapshot_content,
                        metadata: row.metadata,
                        created_at: row.created_at.to_string(),
                        origin_key: None,
                    },
                    observed_commit_id: row.commit_id.to_string(),
                    commit_created_at: metadata_projection
                        .commit_created_at
                        .then(|| row_commit_created_at.clone()),
                    as_of_commit_id: as_of_commit_id.to_string(),
                    depth: row_depth,
                });
            }
        }
    }

    Ok(rows)
}

async fn validate_authenticated_member_row(
    reader: &crate::forktree::ForkTreeReadFacade<impl StorageAdapterRead>,
    row: &crate::forktree::HistoricalStateRow,
    record: &ChangeRecord,
) -> Result<(), LixError> {
    if record.change_id != row.change_id
        || record.schema_key != row.key.schema_key
        || record.entity_pk != row.key.entity_pk
        || record.file_id != row.key.file_id
        || record.created_at != row.created_at
        || record.snapshot.is_none() != row.deleted
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "certified historical row '{}' disagrees with its authenticated Change payload",
                row.change_id
            ),
        ));
    }
    let snapshot = reader.load_json_slot(&record.snapshot).await?;
    if snapshot.as_deref() != row.snapshot_content.as_deref() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "certified historical row '{}' snapshot disagrees with its authenticated Change payload",
                row.change_id
            ),
        ));
    }
    let metadata = reader.load_json_slot(&record.metadata).await?;
    if metadata.as_deref() != row.metadata.as_deref() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "certified historical row '{}' metadata disagrees with its authenticated Change payload",
                row.change_id
            ),
        ));
    }
    Ok(())
}

fn historical_row_matches_request(
    row: &crate::forktree::HistoricalStateRow,
    request: &CommitGraphChangeHistoryRequest,
) -> bool {
    (request.schema_keys.is_empty() || request.schema_keys.contains(&row.key.schema_key))
        && (request.entity_pks.is_empty() || request.entity_pks.contains(&row.key.entity_pk))
        && (request.file_ids.is_empty()
            || row
                .key
                .file_id
                .as_ref()
                .is_some_and(|file_id| request.file_ids.contains(file_id)))
        && (request.include_tombstones || !row.deleted)
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
    use crate::changelog::{ChangeId, CommitId, CommitRecord};
    use crate::commit_graph::{
        CommitGraphChange, CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest,
        CommitGraphNode, CommitGraphReader, ReachableCommitGraphNode,
    };
    use crate::entity_pk::EntityPk;
    use crate::json_store::JsonSlot;
    use crate::sql2::ChangelogQuerySource;
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
        let (query_source, start_commit_id) = empty_changelog_query_source().await;
        let mut route = HistoryRoute::default();
        route.default_to_as_of_commit_id(&start_commit_id.to_string());
        let rows = load_history_entries(
            HistoryViewDescriptor {
                view_name: "test_history",
                as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
            },
            test_commit_graph(Arc::clone(&reachable_calls), start_commit_id),
            query_source,
            &route,
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
    async fn history_loader_reuses_history_topology_for_projected_commit_timestamp() {
        let reachable_calls = Arc::new(AtomicUsize::new(0));
        let (query_source, start_commit_id) = empty_changelog_query_source().await;
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
            query_source,
            &HistoryRoute {
                as_of_commit_ids: vec![start_commit_id.to_string()],
                ..HistoryRoute::default()
            },
            vec!["message".to_string()],
            HistoryMetadataProjection::from_scan(&metadata_schema, &[]),
        )
        .await
        .expect("history load should enrich commit metadata");

        assert_eq!(
            reachable_calls.load(Ordering::SeqCst),
            0,
            "commit metadata must not trigger a second topology walk",
        );
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
        let (query_source, as_of_commit_id) = empty_changelog_query_source().await;
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
            query_source,
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
        async fn load_node(
            &mut self,
            _commit_id: &CommitId,
        ) -> Result<Option<CommitGraphNode>, LixError> {
            Ok(None)
        }

        async fn reachable_nodes(
            &mut self,
            _head_commit_id: &CommitId,
        ) -> Result<Arc<[ReachableCommitGraphNode]>, LixError> {
            self.reachable_calls.fetch_add(1, Ordering::SeqCst);
            if !self.include_reachable_commit {
                return Ok(Arc::from([]));
            }
            Ok(Arc::from([ReachableCommitGraphNode {
                commit: CommitGraphNode {
                    commit_id: self.start_commit_id,
                    generation: 0,
                    parent_commit_ids: Vec::new(),
                },
                depth: 0,
            }]))
        }

        async fn snapshot_roots(&mut self) -> Result<Vec<(String, CommitId)>, LixError> {
            Ok(Vec::new())
        }

        async fn load_commit_records(
            &mut self,
            commit_ids: &[CommitId],
        ) -> Result<Vec<Option<CommitRecord>>, LixError> {
            Ok(commit_ids
                .iter()
                .map(|commit_id| {
                    (self.include_reachable_commit && *commit_id == self.start_commit_id).then(
                        || CommitRecord {
                            format_version: 2,
                            commit_id: *commit_id,
                            generation: 0,
                            parent_commit_ids: Vec::new(),
                            change_id: ChangeId::for_test_label("commit-change"),
                            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                            created_at: commit_timestamp(),
                        },
                    )
                })
                .collect())
        }

        async fn change_history_from_commit(
            &mut self,
            _start_commit_id: &CommitId,
            _request: &CommitGraphChangeHistoryRequest,
        ) -> Result<crate::commit_graph::CommitGraphHistory, LixError> {
            let reachable_nodes = self
                .include_reachable_commit
                .then(|| ReachableCommitGraphNode {
                    commit: CommitGraphNode {
                        commit_id: self.start_commit_id,
                        generation: 0,
                        parent_commit_ids: Vec::new(),
                    },
                    depth: 0,
                });
            Ok(crate::commit_graph::CommitGraphHistory {
                entries: vec![CommitGraphChangeHistoryEntry {
                    change: test_change("entity-change", event_timestamp()),
                    observed_commit_id: self.start_commit_id,
                    start_commit_id: self.start_commit_id,
                    depth: 0,
                }],
                reachable_nodes: reachable_nodes.into_iter().collect::<Vec<_>>().into(),
            })
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
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
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

    async fn empty_changelog_query_source() -> (
        ChangelogQuerySource<SharedStorageAdapterRead<MemoryRead>>,
        CommitId,
    ) {
        let storage = StorageAdapter::new(Memory::new());
        let receipt = crate::forktree::initialize_empty_repository(storage.clone())
            .await
            .expect("authenticated ForkTree bootstrap should succeed");
        let initial_commit_id = CommitId::new(
            uuid::Uuid::parse_str(&receipt.initial_commit_id)
                .expect("bootstrap receipt contains a UUID commit id"),
        );
        let read_scope = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let read_scope = SharedStorageAdapterRead::new(read_scope);
        (
            ChangelogQuerySource {
                forktree_reader: crate::forktree::ForkTreeReadFacade::new(read_scope),
            },
            initial_commit_id,
        )
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
