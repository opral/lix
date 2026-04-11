use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::backend::QueryExecutor;
use crate::canonical::graph::{
    build_commit_graph_node_prepared_batch, resolve_commit_graph_node_write_rows_with_executor,
};
use crate::canonical::journal::{
    build_prepared_batch_from_canonical_output, CanonicalCommitOutput, ChangeRow,
};
use crate::canonical::read::{
    build_state_history_source_sql, load_canonical_change_row_by_id,
    load_commit_lineage_entry_by_id, load_exact_committed_change_from_commit_with_executor,
    CanonicalHistoryContentMode as ReadHistoryContentMode,
    CanonicalHistoryRootFacts as ReadHistoryRootFacts,
    CanonicalHistoryRootSelection as ReadHistoryRootSelection,
    CanonicalRootCommit as ReadCanonicalRootCommit, CommitLineageEntry,
    ExactCommittedStateRowRequest,
};
use crate::common::escape_sql_string;
use crate::contracts::LixFunctionProvider;
use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, Value};

pub(crate) type CanonicalChangeWrite = ChangeRow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalCommit {
    pub(crate) id: String,
    pub(crate) change_set_id: Option<String>,
    pub(crate) change_ids: Vec<String>,
    pub(crate) parent_commit_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalChange {
    pub(crate) id: String,
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalStateIdentity {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: String,
    pub(crate) metadata: Option<String>,
    pub(crate) source_change_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct CanonicalAppendSummary {
    pub(crate) latest_change_id: Option<String>,
    pub(crate) latest_created_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum CanonicalHistoryContentMode {
    #[default]
    MetadataOnly,
    IncludeSnapshotContent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalRootCommit {
    pub(crate) commit_id: String,
    pub(crate) version_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum CanonicalHistoryRootSelection {
    #[default]
    AllRoots,
    RequestedRootCommitIds(Vec<String>),
    ResolvedRootCommits(Vec<CanonicalRootCommit>),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct CanonicalHistoryRequest {
    pub(crate) root_selection: CanonicalHistoryRootSelection,
    pub(crate) root_version_refs: Vec<CanonicalRootCommit>,
    pub(crate) entity_ids: Vec<String>,
    pub(crate) file_ids: Vec<String>,
    pub(crate) schema_keys: Vec<String>,
    pub(crate) plugin_keys: Vec<String>,
    pub(crate) min_depth: Option<i64>,
    pub(crate) max_depth: Option<i64>,
    pub(crate) content_mode: CanonicalHistoryContentMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalHistoryRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: String,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) schema_version: String,
    pub(crate) change_id: String,
    pub(crate) commit_id: String,
    pub(crate) commit_created_at: String,
    pub(crate) root_commit_id: String,
    pub(crate) depth: i64,
    pub(crate) version_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct CanonicalVisibleStateRequest {
    pub(crate) commit_ids: Vec<String>,
    pub(crate) filter: CanonicalVisibleStateFilter,
    pub(crate) content_mode: CanonicalContentMode,
    pub(crate) tombstones: CanonicalTombstoneMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct CanonicalVisibleStateFilter {
    pub(crate) schema_keys: BTreeSet<String>,
    pub(crate) entity_ids: BTreeSet<String>,
    pub(crate) file_ids: BTreeSet<String>,
    pub(crate) plugin_keys: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum CanonicalContentMode {
    #[default]
    MetadataOnly,
    IncludeSnapshotContent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum CanonicalTombstoneMode {
    #[default]
    ExcludeTombstones,
    IncludeTombstones,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalVisibleStateRow {
    pub(crate) root_commit_id: String,
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) source_change_id: String,
    pub(crate) source_commit_id: String,
    pub(crate) depth: usize,
    pub(crate) visibility: CanonicalVisibility,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CanonicalVisibility {
    Visible,
    Tombstone,
}

pub(crate) async fn append_changes(
    tx: &mut dyn LixBackendTransaction,
    changes: &[CanonicalChangeWrite],
    functions: &mut dyn LixFunctionProvider,
) -> Result<CanonicalAppendSummary, LixError> {
    if changes.is_empty() {
        return Ok(CanonicalAppendSummary::default());
    }

    let canonical_output = CanonicalCommitOutput {
        changes: changes.to_vec(),
    };
    let mut executor = &mut *tx;
    let commit_graph_rows =
        resolve_commit_graph_node_write_rows_with_executor(&mut executor, &canonical_output)
            .await?;
    let mut prepared =
        build_prepared_batch_from_canonical_output(&canonical_output, functions, tx.dialect())?;
    prepared.extend(build_commit_graph_node_prepared_batch(
        &commit_graph_rows,
        tx.dialect(),
    )?);
    tx.execute_batch(&prepared).await?;

    Ok(CanonicalAppendSummary {
        latest_change_id: canonical_output
            .changes
            .last()
            .map(|change| change.id.clone()),
        latest_created_at: canonical_output
            .changes
            .last()
            .map(|change| change.created_at.clone()),
    })
}

pub(crate) async fn load_commit(
    executor: &mut dyn QueryExecutor,
    commit_id: &str,
) -> Result<Option<CanonicalCommit>, LixError> {
    Ok(load_commit_lineage_entry_by_id(executor, commit_id)
        .await?
        .map(canonical_commit_from_entry))
}

pub(crate) async fn load_change(
    executor: &mut dyn QueryExecutor,
    change_id: &str,
) -> Result<Option<CanonicalChange>, LixError> {
    Ok(load_canonical_change_row_by_id(executor, change_id)
        .await?
        .map(canonical_change_from_row))
}

pub(crate) async fn load_exact_row_at_commit(
    executor: &mut dyn QueryExecutor,
    commit_id: &str,
    identity: &CanonicalStateIdentity,
) -> Result<Option<CanonicalStateRow>, LixError> {
    let request = ExactCommittedStateRowRequest {
        entity_id: identity.entity_id.clone(),
        schema_key: identity.schema_key.clone(),
        version_id: String::new(),
        exact_filters: BTreeMap::from([(
            "file_id".to_string(),
            Value::Text(identity.file_id.clone()),
        )]),
    };
    let Some(change) =
        load_exact_committed_change_from_commit_with_executor(executor, commit_id, &request)
            .await?
    else {
        return Ok(None);
    };
    let Some(snapshot_content) = change.snapshot_content else {
        return Ok(None);
    };

    Ok(Some(CanonicalStateRow {
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        schema_version: change.schema_version,
        file_id: change.file_id,
        plugin_key: change.plugin_key,
        snapshot_content,
        metadata: change.metadata,
        source_change_id: change.id,
    }))
}

pub(crate) async fn load_history(
    backend: &dyn LixBackend,
    request: &CanonicalHistoryRequest,
) -> Result<Vec<CanonicalHistoryRow>, LixError> {
    let sql = build_canonical_history_query_sql(backend.dialect(), request)?;
    let result = backend.execute(&sql, &[]).await?;
    parse_canonical_history_rows(result)
}

pub(crate) async fn load_visible_state(
    executor: &mut dyn QueryExecutor,
    request: &CanonicalVisibleStateRequest,
) -> Result<Vec<CanonicalVisibleStateRow>, LixError> {
    let sql = build_visible_state_query_sql(executor.dialect(), request)?;
    let result = executor.execute(&sql, &[]).await?;
    parse_visible_state_rows(result)
}

pub(crate) async fn resolve_merge_base(
    executor: &mut dyn QueryExecutor,
    left_head_commit_id: &str,
    right_head_commit_id: &str,
) -> Result<Option<String>, LixError> {
    let left_depths = load_commit_depths(executor, left_head_commit_id).await?;
    let right_depths = load_commit_depths(executor, right_head_commit_id).await?;

    Ok(left_depths
        .iter()
        .filter_map(|(commit_id, left_depth)| {
            right_depths.get(commit_id).map(|right_depth| {
                (
                    left_depth + right_depth,
                    std::cmp::max(*left_depth, *right_depth),
                    commit_id.clone(),
                )
            })
        })
        .min()
        .map(|(_, _, commit_id)| commit_id))
}

fn canonical_commit_from_entry(entry: CommitLineageEntry) -> CanonicalCommit {
    CanonicalCommit {
        id: entry.id,
        change_set_id: entry.change_set_id,
        change_ids: entry.change_ids,
        parent_commit_ids: entry.parent_commit_ids,
    }
}

fn canonical_change_from_row(
    row: crate::canonical::read::CommittedCanonicalChangeRow,
) -> CanonicalChange {
    CanonicalChange {
        id: row.id,
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        file_id: row.file_id,
        plugin_key: row.plugin_key,
        snapshot_content: row.snapshot_content,
        metadata: row.metadata,
        created_at: row.created_at,
    }
}

fn build_canonical_history_query_sql(
    dialect: crate::SqlDialect,
    request: &CanonicalHistoryRequest,
) -> Result<String, LixError> {
    let source_sql = build_state_history_source_sql(
        dialect,
        &ReadHistoryRootFacts {
            traversal: map_history_root_selection(&request.root_selection),
            root_version_refs: request
                .root_version_refs
                .iter()
                .cloned()
                .map(|row| ReadCanonicalRootCommit {
                    commit_id: row.commit_id,
                    version_id: row.version_id,
                })
                .collect(),
        },
        match request.content_mode {
            CanonicalHistoryContentMode::MetadataOnly => ReadHistoryContentMode::MetadataOnly,
            CanonicalHistoryContentMode::IncludeSnapshotContent => {
                ReadHistoryContentMode::IncludeSnapshotContent
            }
        },
        false,
        request.max_depth,
    )?;
    let predicates = render_history_predicates(
        &request.entity_ids,
        &request.file_ids,
        &request.schema_keys,
        &request.plugin_keys,
        request.min_depth,
        request.max_depth,
        "history",
    );
    let where_sql = render_where_clause_sql(&predicates, "WHERE ");

    Ok(format!(
        "SELECT \
           history.entity_id, \
           history.schema_key, \
           history.file_id, \
           history.plugin_key, \
           history.snapshot_content, \
           history.metadata, \
           history.schema_version, \
           history.change_id, \
           history.commit_id, \
           history.commit_created_at, \
           history.root_commit_id, \
           history.depth, \
           history.version_id \
         FROM ({source_sql}) history \
         {where_sql} \
         ORDER BY \
           history.root_commit_id ASC, \
           history.entity_id ASC, \
           history.schema_key ASC, \
           history.file_id ASC, \
           history.depth ASC",
        source_sql = source_sql,
        where_sql = where_sql,
    ))
}

fn build_visible_state_query_sql(
    dialect: crate::SqlDialect,
    request: &CanonicalVisibleStateRequest,
) -> Result<String, LixError> {
    let history_sql = build_state_history_source_sql(
        dialect,
        &ReadHistoryRootFacts {
            traversal: ReadHistoryRootSelection::RequestedRootCommitIds(request.commit_ids.clone()),
            root_version_refs: Vec::new(),
        },
        match request.content_mode {
            CanonicalContentMode::MetadataOnly => ReadHistoryContentMode::MetadataOnly,
            CanonicalContentMode::IncludeSnapshotContent => {
                ReadHistoryContentMode::IncludeSnapshotContent
            }
        },
        matches!(
            request.tombstones,
            CanonicalTombstoneMode::IncludeTombstones
        ),
        None,
    )?;
    let requested_commit_roots_sql = build_requested_commit_roots_select_sql(&request.commit_ids);
    let (parent_join_sql, parent_value_expr) = visible_state_parent_join_sql(dialect);
    let predicates = render_history_predicates(
        &request
            .filter
            .entity_ids
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        &request.filter.file_ids.iter().cloned().collect::<Vec<_>>(),
        &request
            .filter
            .schema_keys
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        &request
            .filter
            .plugin_keys
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        None,
        None,
        "visible",
    );
    let where_sql = render_where_clause_sql(&predicates, "WHERE ");

    Ok(format!(
        "WITH RECURSIVE \
           visible_history AS ({history_sql}), \
           canonical_commit_headers AS ( \
             SELECT \
               commit_change.id AS change_id, \
               commit_change.entity_id AS commit_id, \
               commit_change.schema_key AS schema_key, \
               commit_change.schema_version AS schema_version, \
               commit_change.file_id AS file_id, \
               commit_change.plugin_key AS plugin_key, \
               commit_change.metadata AS metadata, \
               commit_snapshot.content AS commit_snapshot_content \
             FROM lix_internal_change commit_change \
             LEFT JOIN lix_internal_snapshot commit_snapshot \
               ON commit_snapshot.id = commit_change.snapshot_id \
             WHERE commit_change.schema_key = 'lix_commit' \
           ), \
           requested_commit_roots AS ({requested_commit_roots_sql}), \
           reachable_commit_walk AS ( \
             SELECT \
               requested.commit_id AS commit_id, \
               requested.root_commit_id AS root_commit_id, \
               0 AS commit_depth \
             FROM requested_commit_roots requested \
             UNION ALL \
             SELECT \
               {parent_value_expr} AS commit_id, \
               walk.root_commit_id AS root_commit_id, \
               walk.commit_depth + 1 AS commit_depth \
             FROM reachable_commit_walk walk \
             JOIN canonical_commit_headers commit_headers \
               ON commit_headers.commit_id = walk.commit_id \
             {parent_join_sql} \
             WHERE {parent_value_expr} IS NOT NULL \
           ), \
           reachable_commits AS ( \
             SELECT \
               commit_id, \
               root_commit_id, \
               MIN(commit_depth) AS commit_depth \
             FROM reachable_commit_walk \
             GROUP BY commit_id, root_commit_id \
           ), \
           visible_candidates AS ( \
             SELECT \
               history.root_commit_id, \
               history.entity_id, \
               history.schema_key, \
               history.schema_version, \
               history.file_id, \
               history.plugin_key, \
               history.snapshot_content, \
               history.metadata, \
               history.change_id, \
               history.commit_id, \
               history.depth, \
               history.is_tombstone \
             FROM visible_history history \
             UNION ALL \
             SELECT \
               reachable.root_commit_id AS root_commit_id, \
               commit_headers.commit_id AS entity_id, \
               commit_headers.schema_key AS schema_key, \
               commit_headers.schema_version AS schema_version, \
               commit_headers.file_id AS file_id, \
               commit_headers.plugin_key AS plugin_key, \
               {commit_snapshot_projection}, \
               commit_headers.metadata AS metadata, \
               commit_headers.change_id AS change_id, \
               reachable.commit_id AS commit_id, \
               reachable.commit_depth AS depth, \
               0 AS is_tombstone \
             FROM reachable_commits reachable \
             JOIN canonical_commit_headers commit_headers \
               ON commit_headers.commit_id = reachable.commit_id \
           ), \
           ranked_visible AS ( \
           SELECT \
             visible.root_commit_id, \
             visible.entity_id, \
             visible.schema_key, \
             visible.schema_version, \
             visible.file_id, \
             visible.plugin_key, \
             visible.snapshot_content, \
             visible.metadata, \
             visible.change_id, \
             visible.commit_id, \
             visible.depth, \
             visible.is_tombstone, \
             ROW_NUMBER() OVER ( \
               PARTITION BY visible.root_commit_id, visible.entity_id, visible.schema_key, visible.file_id \
               ORDER BY visible.depth ASC \
             ) AS rn \
           FROM visible_candidates visible \
         ) \
         SELECT \
           visible.root_commit_id, \
           visible.entity_id, \
           visible.schema_key, \
           visible.schema_version, \
           visible.file_id, \
           visible.plugin_key, \
           visible.snapshot_content, \
           visible.metadata, \
           visible.change_id, \
           visible.commit_id, \
           visible.depth, \
           visible.is_tombstone \
         FROM ranked_visible visible \
         WHERE visible.rn = 1 \
           {visibility_filter_sql} \
         {and_where_sql} \
         ORDER BY \
           visible.root_commit_id ASC, \
           visible.entity_id ASC, \
           visible.schema_key ASC, \
           visible.file_id ASC",
        history_sql = history_sql,
        requested_commit_roots_sql = requested_commit_roots_sql,
        parent_join_sql = parent_join_sql,
        parent_value_expr = parent_value_expr,
        commit_snapshot_projection = match request.content_mode {
            CanonicalContentMode::MetadataOnly => "NULL AS snapshot_content",
            CanonicalContentMode::IncludeSnapshotContent => {
                "commit_headers.commit_snapshot_content AS snapshot_content"
            }
        },
        visibility_filter_sql = match request.tombstones {
            CanonicalTombstoneMode::ExcludeTombstones => "AND visible.is_tombstone = 0",
            CanonicalTombstoneMode::IncludeTombstones => "",
        },
        and_where_sql = if where_sql.is_empty() {
            String::new()
        } else {
            format!(" AND {}", where_sql.trim_start_matches("WHERE "))
        },
    ))
}

fn build_requested_commit_roots_select_sql(commit_ids: &[String]) -> String {
    if commit_ids.is_empty() {
        return "SELECT CAST(NULL AS TEXT) AS commit_id, CAST(NULL AS TEXT) AS root_commit_id WHERE 1 = 0"
            .to_string();
    }

    let mut sql = String::new();
    for (index, commit_id) in commit_ids.iter().enumerate() {
        if index == 0 {
            sql.push_str("SELECT ");
            sql.push_str(&format!(
                "'{}' AS commit_id, '{}' AS root_commit_id",
                escape_sql_string(commit_id),
                escape_sql_string(commit_id),
            ));
        } else {
            sql.push_str(" UNION ALL SELECT ");
            sql.push_str(&format!(
                "'{}', '{}'",
                escape_sql_string(commit_id),
                escape_sql_string(commit_id),
            ));
        }
    }
    sql
}

fn visible_state_parent_join_sql(dialect: crate::SqlDialect) -> (&'static str, &'static str) {
    match dialect {
        crate::SqlDialect::Sqlite => (
            "JOIN json_each(COALESCE(commit_headers.commit_snapshot_content, '{}'), '$.parent_commit_ids') AS parent_rows",
            "parent_rows.value",
        ),
        crate::SqlDialect::Postgres => (
            "JOIN LATERAL jsonb_array_elements_text(CAST(COALESCE(commit_headers.commit_snapshot_content, '{}') AS JSONB) -> 'parent_commit_ids') AS parent_rows(parent_commit_id) ON TRUE",
            "parent_rows.parent_commit_id",
        ),
    }
}

fn map_history_root_selection(
    selection: &CanonicalHistoryRootSelection,
) -> ReadHistoryRootSelection {
    match selection {
        CanonicalHistoryRootSelection::AllRoots => ReadHistoryRootSelection::AllRoots,
        CanonicalHistoryRootSelection::RequestedRootCommitIds(root_commit_ids) => {
            ReadHistoryRootSelection::RequestedRootCommitIds(root_commit_ids.clone())
        }
        CanonicalHistoryRootSelection::ResolvedRootCommits(rows) => {
            ReadHistoryRootSelection::ResolvedRootCommits(
                rows.iter()
                    .cloned()
                    .map(|row| ReadCanonicalRootCommit {
                        commit_id: row.commit_id,
                        version_id: row.version_id,
                    })
                    .collect(),
            )
        }
    }
}

fn render_history_predicates(
    entity_ids: &[String],
    file_ids: &[String],
    schema_keys: &[String],
    plugin_keys: &[String],
    min_depth: Option<i64>,
    max_depth: Option<i64>,
    alias: &str,
) -> Vec<String> {
    let mut predicates = Vec::new();
    if !entity_ids.is_empty() {
        predicates.push(render_text_in_predicate(
            &format!("{alias}.entity_id"),
            entity_ids,
        ));
    }
    if !file_ids.is_empty() {
        predicates.push(render_text_in_predicate(
            &format!("{alias}.file_id"),
            file_ids,
        ));
    }
    if !schema_keys.is_empty() {
        predicates.push(render_text_in_predicate(
            &format!("{alias}.schema_key"),
            schema_keys,
        ));
    }
    if !plugin_keys.is_empty() {
        predicates.push(render_text_in_predicate(
            &format!("{alias}.plugin_key"),
            plugin_keys,
        ));
    }
    if let Some(min_depth) = min_depth {
        predicates.push(format!("{alias}.depth >= {min_depth}"));
    }
    if let Some(max_depth) = max_depth {
        predicates.push(format!("{alias}.depth <= {max_depth}"));
    }
    predicates
}

fn render_text_in_predicate(column: &str, values: &[String]) -> String {
    if values.len() == 1 {
        return format!("{column} = '{}'", escape_sql_string(&values[0]));
    }

    format!(
        "{column} IN ({})",
        values
            .iter()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn render_where_clause_sql(predicates: &[String], prefix: &str) -> String {
    if predicates.is_empty() {
        String::new()
    } else {
        format!("{prefix}{}", predicates.join(" AND "))
    }
}

fn parse_canonical_history_rows(result: QueryResult) -> Result<Vec<CanonicalHistoryRow>, LixError> {
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        rows.push(CanonicalHistoryRow {
            entity_id: required_text_value(&row, 0, "entity_id")?,
            schema_key: required_text_value(&row, 1, "schema_key")?,
            file_id: required_text_value(&row, 2, "file_id")?,
            plugin_key: required_text_value(&row, 3, "plugin_key")?,
            snapshot_content: optional_text_value(&row, 4, "snapshot_content")?,
            metadata: optional_text_value(&row, 5, "metadata")?,
            schema_version: required_text_value(&row, 6, "schema_version")?,
            change_id: required_text_value(&row, 7, "change_id")?,
            commit_id: required_text_value(&row, 8, "commit_id")?,
            commit_created_at: required_text_value(&row, 9, "commit_created_at")?,
            root_commit_id: required_text_value(&row, 10, "root_commit_id")?,
            depth: required_integer_value(&row, 11, "depth")?,
            version_id: required_text_value(&row, 12, "version_id")?,
        });
    }
    Ok(rows)
}

fn parse_visible_state_rows(
    result: QueryResult,
) -> Result<Vec<CanonicalVisibleStateRow>, LixError> {
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        rows.push(CanonicalVisibleStateRow {
            root_commit_id: required_text_value(&row, 0, "root_commit_id")?,
            entity_id: required_text_value(&row, 1, "entity_id")?,
            schema_key: required_text_value(&row, 2, "schema_key")?,
            schema_version: required_text_value(&row, 3, "schema_version")?,
            file_id: required_text_value(&row, 4, "file_id")?,
            plugin_key: required_text_value(&row, 5, "plugin_key")?,
            snapshot_content: optional_text_value(&row, 6, "snapshot_content")?,
            metadata: optional_text_value(&row, 7, "metadata")?,
            source_change_id: required_text_value(&row, 8, "change_id")?,
            source_commit_id: required_text_value(&row, 9, "commit_id")?,
            depth: required_integer_value(&row, 10, "depth")? as usize,
            visibility: match required_integer_value(&row, 11, "is_tombstone")? {
                0 => CanonicalVisibility::Visible,
                _ => CanonicalVisibility::Tombstone,
            },
        });
    }
    Ok(rows)
}

async fn load_commit_depths(
    executor: &mut dyn QueryExecutor,
    head_commit_id: &str,
) -> Result<BTreeMap<String, usize>, LixError> {
    let mut depths = BTreeMap::new();
    let mut queue = VecDeque::from([(head_commit_id.to_string(), 0usize)]);
    while let Some((commit_id, depth)) = queue.pop_front() {
        if depths.contains_key(&commit_id) {
            continue;
        }
        depths.insert(commit_id.clone(), depth);
        let entry = load_commit_lineage_entry_by_id(executor, &commit_id)
            .await?
            .ok_or_else(|| {
                LixError::unknown(format!("missing commit lineage entry for '{commit_id}'"))
            })?;
        let mut parents = entry.parent_commit_ids;
        parents.sort();
        for parent_id in parents {
            queue.push_back((parent_id, depth + 1));
        }
    }
    Ok(depths)
}

fn required_text_value(row: &[Value], index: usize, field: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(value.clone()),
        Some(other) => Err(LixError::unknown(format!(
            "expected text for {field}, got {other:?}"
        ))),
        None => Err(LixError::unknown(format!(
            "missing column {field} at index {index}"
        ))),
    }
}

fn optional_text_value(
    row: &[Value],
    index: usize,
    field: &str,
) -> Result<Option<String>, LixError> {
    match row.get(index) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::Text(value)) => Ok(Some(value.clone())),
        Some(other) => Err(LixError::unknown(format!(
            "expected nullable text for {field}, got {other:?}"
        ))),
    }
}

fn required_integer_value(row: &[Value], index: usize, field: &str) -> Result<i64, LixError> {
    match row.get(index) {
        Some(Value::Integer(value)) => Ok(*value),
        Some(other) => Err(LixError::unknown(format!(
            "expected integer for {field}, got {other:?}"
        ))),
        None => Err(LixError::unknown(format!(
            "missing column {field} at index {index}"
        ))),
    }
}
