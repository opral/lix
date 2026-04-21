use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::canonical::graph::{
    build_commit_graph_node_prepared_batch, resolve_commit_graph_node_write_rows_with_executor,
};
use crate::canonical::journal::{
    build_prepared_batch_from_canonical_output, build_prepared_batch_from_visibility_rows,
    CanonicalCommitOutput, ChangeRow, UntrackedChangeVisibilityKind, UntrackedChangeVisibilityRow,
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
use crate::canonical::store::{
    CanonicalBackendRef, CanonicalExecutorRef, CanonicalTransactionRef,
};
use crate::canonical::store_sql::{
    execute_batch_with_transaction, execute_query_with_backend, execute_query_with_executor,
    execute_query_with_transaction, load_durable_state_commit_low_watermark_in_transaction,
};
use crate::common::escape_sql_string;
use crate::functions::LixFunctionProvider;
use crate::streams::DurableStateCommitCursor;
use crate::{LixError, QueryResult, Value};

#[cfg(test)]
use crate::QueryExecutor;

pub(crate) type CanonicalChangeWrite = ChangeRow;
pub(crate) type CanonicalUntrackedVisibilityWrite = UntrackedChangeVisibilityRow;
pub(crate) type CanonicalUntrackedVisibilityKind = UntrackedChangeVisibilityKind;

pub(crate) fn canonical_untracked_visibility_kind(
    global: bool,
) -> CanonicalUntrackedVisibilityKind {
    if global {
        CanonicalUntrackedVisibilityKind::Global
    } else {
        CanonicalUntrackedVisibilityKind::Version
    }
}

pub(crate) fn canonical_untracked_visibility_row_id_for_change(change_id: &str) -> String {
    format!("visibility:{change_id}")
}

pub(crate) fn canonical_untracked_visibility_write_from_change_visibility(
    change: &CanonicalChangeWrite,
    version_id: &str,
    global: bool,
    created_at: Option<&str>,
) -> CanonicalUntrackedVisibilityWrite {
    CanonicalUntrackedVisibilityWrite {
        id: canonical_untracked_visibility_row_id_for_change(&change.id),
        change_id: change.id.clone(),
        version_id: version_id.to_string(),
        visibility_kind: canonical_untracked_visibility_kind(global),
        entity_id: change.entity_id.clone(),
        schema_key: change.schema_key.clone(),
        file_id: change.file_id.clone(),
        created_at: created_at.unwrap_or(&change.created_at).to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalCommit {
    pub(crate) id: String,
    pub(crate) change_set_id: Option<String>,
    pub(crate) change_ids: Vec<String>,
    pub(crate) parent_commit_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalChange {
    /// Immutable canonical change fact loaded from the journal.
    pub(crate) id: String,
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalStateIdentity {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalUntrackedVisibilityIdentity {
    pub(crate) version_id: String,
    pub(crate) visibility_kind: CanonicalUntrackedVisibilityKind,
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalUntrackedVisibility {
    pub(crate) id: String,
    pub(crate) append_seq: i64,
    pub(crate) change_id: String,
    pub(crate) version_id: String,
    pub(crate) visibility_kind: CanonicalUntrackedVisibilityKind,
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
    pub(crate) snapshot_content: String,
    pub(crate) metadata: Option<String>,
    pub(crate) source_change_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct CanonicalAppendSummary {
    pub(crate) latest_change_id: Option<String>,
    pub(crate) latest_created_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CanonicalUntrackedIdentity {
    version_id: String,
    visibility_kind: CanonicalUntrackedVisibilityKind,
    entity_id: String,
    schema_key: String,
    file_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CanonicalUntrackedChangeRow {
    visibility_id: String,
    #[allow(dead_code)]
    visibility_append_seq: i64,
    change_id: String,
    identity: CanonicalUntrackedIdentity,
    snapshot_id: String,
    change_created_at: String,
    visibility_created_at: String,
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
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
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
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
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
    tx: CanonicalTransactionRef<'_>,
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
    execute_batch_with_transaction(tx, &prepared).await?;

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

pub(crate) async fn append_untracked_change_visibility_rows(
    tx: CanonicalTransactionRef<'_>,
    visibility_rows: &[CanonicalUntrackedVisibilityWrite],
) -> Result<(), LixError> {
    if visibility_rows.is_empty() {
        return Ok(());
    }
    let prepared = build_prepared_batch_from_visibility_rows(visibility_rows, tx.dialect())?;
    execute_batch_with_transaction(tx, &prepared).await
}

pub(crate) async fn replace_snapshot_content_in_transaction(
    tx: CanonicalTransactionRef<'_>,
    snapshot_id: &str,
    snapshot_content: &str,
) -> Result<(), LixError> {
    execute_query_with_transaction(
        tx,
        "UPDATE lix_internal_snapshot \
         SET content = $1 \
         WHERE id = $2",
        &[
            Value::Text(snapshot_content.to_string()),
            Value::Text(snapshot_id.to_string()),
        ],
    )
    .await
    .map(|_| ())
}

pub(crate) async fn compact_untracked_changes_for_touched_rows_in_transaction(
    transaction: CanonicalTransactionRef<'_>,
    visibility_rows: &[CanonicalUntrackedVisibilityWrite],
) -> Result<usize, LixError> {
    let touched = visibility_rows
        .iter()
        .map(canonical_untracked_identity_from_visibility)
        .collect::<BTreeSet<_>>();
    if touched.is_empty() {
        return Ok(0);
    }
    compact_untracked_changes_in_transaction(transaction, Some(&touched)).await
}

#[allow(dead_code)]
pub(crate) async fn compact_stale_untracked_changes_in_transaction(
    transaction: CanonicalTransactionRef<'_>,
) -> Result<usize, LixError> {
    compact_untracked_changes_in_transaction(transaction, None).await
}

pub(crate) async fn load_commit(
    executor: CanonicalExecutorRef<'_>,
    commit_id: &str,
) -> Result<Option<CanonicalCommit>, LixError> {
    Ok(load_commit_lineage_entry_by_id(executor, commit_id)
        .await?
        .map(canonical_commit_from_entry))
}

pub(crate) async fn load_change(
    executor: CanonicalExecutorRef<'_>,
    change_id: &str,
) -> Result<Option<CanonicalChange>, LixError> {
    Ok(load_canonical_change_row_by_id(executor, change_id)
        .await?
        .map(canonical_change_from_row))
}

#[cfg(test)]
pub(crate) async fn change_has_untracked_visibility(
    executor: &mut dyn QueryExecutor,
    change_id: &str,
) -> Result<bool, LixError> {
    untracked_visibility_exists(executor, change_id).await
}

#[cfg(test)]
pub(crate) async fn change_is_untracked_visibility_reachable(
    executor: &mut dyn QueryExecutor,
    change_id: &str,
) -> Result<bool, LixError> {
    untracked_visibility_exists(executor, change_id).await
}

#[cfg(test)]
pub(crate) async fn load_latest_untracked_change_visibility_for_identity(
    executor: &mut dyn QueryExecutor,
    identity: &CanonicalUntrackedVisibilityIdentity,
) -> Result<Option<CanonicalUntrackedVisibility>, LixError> {
    let dialect = executor.dialect();
    let mut params = vec![
        Value::Text(identity.version_id.clone()),
        Value::Text(identity.visibility_kind.as_str().to_string()),
        Value::Text(identity.entity_id.clone()),
        Value::Text(identity.schema_key.clone()),
    ];
    let file_predicate = if let Some(file_id) = identity.file_id.as_ref() {
        params.push(Value::Text(file_id.clone()));
        format!("file_id = {}", dialect.placeholder(params.len()))
    } else {
        "file_id IS NULL".to_string()
    };
    let sql = format!(
        "SELECT id, change_id, version_id, visibility_kind, entity_id, schema_key, file_id, created_at, append_seq \
         FROM lix_internal_untracked_change_visibility \
         WHERE version_id = {p1} \
           AND visibility_kind = {p2} \
           AND entity_id = {p3} \
           AND schema_key = {p4} \
           AND {file_predicate} \
         ORDER BY append_seq DESC \
         LIMIT 1",
        p1 = dialect.placeholder(1),
        p2 = dialect.placeholder(2),
        p3 = dialect.placeholder(3),
        p4 = dialect.placeholder(4),
        file_predicate = file_predicate,
    );
    let result = executor.execute(&sql, &params).await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    Ok(Some(CanonicalUntrackedVisibility {
        id: required_text_value(row, 0, "lix_internal_untracked_change_visibility.id")?,
        append_seq: required_integer_value(
            row,
            8,
            "lix_internal_untracked_change_visibility.append_seq",
        )?,
        change_id: required_text_value(
            row,
            1,
            "lix_internal_untracked_change_visibility.change_id",
        )?,
        version_id: required_text_value(
            row,
            2,
            "lix_internal_untracked_change_visibility.version_id",
        )?,
        visibility_kind: CanonicalUntrackedVisibilityKind::parse(&required_text_value(
            row,
            3,
            "lix_internal_untracked_change_visibility.visibility_kind",
        )?)?,
        entity_id: required_text_value(
            row,
            4,
            "lix_internal_untracked_change_visibility.entity_id",
        )?,
        schema_key: required_text_value(
            row,
            5,
            "lix_internal_untracked_change_visibility.schema_key",
        )?,
        file_id: optional_text_value(row, 6, "lix_internal_untracked_change_visibility.file_id")?,
        created_at: required_text_value(
            row,
            7,
            "lix_internal_untracked_change_visibility.created_at",
        )?,
    }))
}

pub(crate) async fn load_commit_member_change(
    executor: CanonicalExecutorRef<'_>,
    commit_id: &str,
    change_id: &str,
) -> Result<Option<CanonicalChange>, LixError> {
    let change = load_canonical_change_row_by_id(executor, change_id).await?;
    if change.is_some() && untracked_visibility_exists(executor, change_id).await? {
        return Err(LixError::unknown(format!(
            "canonical commit '{}' references untracked-visible change '{}' as a commit member",
            commit_id, change_id
        )));
    }
    Ok(change.map(canonical_change_from_row))
}

pub(crate) async fn load_exact_row_at_commit(
    executor: CanonicalExecutorRef<'_>,
    commit_id: &str,
    identity: &CanonicalStateIdentity,
) -> Result<Option<CanonicalStateRow>, LixError> {
    let request = ExactCommittedStateRowRequest {
        entity_id: identity.entity_id.clone(),
        schema_key: identity.schema_key.clone(),
        version_id: String::new(),
        exact_filters: BTreeMap::from([(
            "file_id".to_string(),
            identity
                .file_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
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
    backend: CanonicalBackendRef<'_>,
    request: &CanonicalHistoryRequest,
) -> Result<Vec<CanonicalHistoryRow>, LixError> {
    let sql = build_canonical_history_query_sql(backend.dialect(), request)?;
    let result = execute_query_with_backend(backend, &sql, &[]).await?;
    parse_canonical_history_rows(result)
}

pub(crate) async fn load_visible_state(
    executor: CanonicalExecutorRef<'_>,
    request: &CanonicalVisibleStateRequest,
) -> Result<Vec<CanonicalVisibleStateRow>, LixError> {
    let sql = build_visible_state_query_sql(executor.dialect(), request)?;
    let result = execute_query_with_executor(executor, &sql, &[]).await?;
    parse_visible_state_rows(result)
}

pub(crate) async fn resolve_merge_base(
    executor: CanonicalExecutorRef<'_>,
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

async fn untracked_visibility_exists(
    executor: CanonicalExecutorRef<'_>,
    change_id: &str,
) -> Result<bool, LixError> {
    let sql = format!(
        "SELECT 1 \
         FROM {} \
         WHERE change_id = {} \
         LIMIT 1",
        crate::canonical::journal::UNTRACKED_CHANGE_VISIBILITY_TABLE,
        executor.dialect().placeholder(1),
    );
    let result =
        execute_query_with_executor(executor, &sql, &[Value::Text(change_id.to_string())]).await?;
    Ok(!result.rows.is_empty())
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

async fn compact_untracked_changes_in_transaction(
    transaction: CanonicalTransactionRef<'_>,
    identities: Option<&BTreeSet<CanonicalUntrackedIdentity>>,
) -> Result<usize, LixError> {
    let low_watermark = load_durable_state_commit_low_watermark_in_transaction(transaction).await?;
    let rows = {
        let mut executor = &mut *transaction;
        load_untracked_change_rows_for_compaction(&mut executor, identities).await?
    };
    let delete_rows = select_compactable_untracked_change_rows(&rows, low_watermark.as_ref());
    delete_visibility_rows_and_orphaned_changes(transaction, &delete_rows).await?;
    Ok(delete_rows.len())
}

async fn load_untracked_change_rows_for_compaction(
    executor: CanonicalExecutorRef<'_>,
    identities: Option<&BTreeSet<CanonicalUntrackedIdentity>>,
) -> Result<Vec<CanonicalUntrackedChangeRow>, LixError> {
    let dialect = executor.dialect();
    let mut params = Vec::new();
    let mut next_placeholder = 1usize;
    let mut predicates = Vec::new();

    if let Some(identities) = identities {
        if identities.is_empty() {
            return Ok(Vec::new());
        }
        let mut groups = Vec::with_capacity(identities.len());
        for identity in identities {
            let version = dialect.placeholder(next_placeholder);
            params.push(Value::Text(identity.version_id.clone()));
            next_placeholder += 1;
            let visibility_kind = dialect.placeholder(next_placeholder);
            params.push(Value::Text(identity.visibility_kind.as_str().to_string()));
            next_placeholder += 1;
            let entity = dialect.placeholder(next_placeholder);
            params.push(Value::Text(identity.entity_id.clone()));
            next_placeholder += 1;
            let schema = dialect.placeholder(next_placeholder);
            params.push(Value::Text(identity.schema_key.clone()));
            next_placeholder += 1;
            let file = dialect.placeholder(next_placeholder);
            params.push(Value::Text(identity.file_id.clone()));
            next_placeholder += 1;
            groups.push(format!(
                "(v.version_id = {version} AND v.visibility_kind = {visibility_kind} AND v.entity_id = {entity} AND v.schema_key = {schema} AND COALESCE(v.file_id, '') = {file})"
            ));
        }
        predicates.push(format!("({})", groups.join(" OR ")));
    }

    // Visibility append order is the retention authority. Payload timestamps stay
    // available for debugging, but compaction must never pick winners by created_at.
    let sql = format!(
        "SELECT v.id, v.append_seq, c.id, v.version_id, v.visibility_kind, v.entity_id, v.schema_key, v.file_id, c.snapshot_id, c.created_at, v.created_at \
         FROM lix_internal_untracked_change_visibility v \
         JOIN lix_internal_change c \
           ON c.id = v.change_id \
         {where_sql} \
         ORDER BY v.version_id ASC, v.visibility_kind ASC, v.entity_id ASC, v.schema_key ASC, v.file_id ASC, v.append_seq DESC",
        where_sql = if predicates.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", predicates.join(" AND "))
        }
    );
    let result = execute_query_with_executor(executor, &sql, &params).await?;
    result
        .rows
        .iter()
        .map(|row| {
            Ok(CanonicalUntrackedChangeRow {
                visibility_id: required_text_value(
                    row,
                    0,
                    "lix_internal_untracked_change_visibility.id",
                )?,
                visibility_append_seq: required_integer_value(
                    row,
                    1,
                    "lix_internal_untracked_change_visibility.append_seq",
                )?,
                change_id: required_text_value(row, 2, "lix_internal_change.id")?,
                identity: CanonicalUntrackedIdentity {
                    version_id: required_text_value(
                        row,
                        3,
                        "lix_internal_untracked_change_visibility.version_id",
                    )?,
                    visibility_kind: CanonicalUntrackedVisibilityKind::parse(
                        &required_text_value(
                            row,
                            4,
                            "lix_internal_untracked_change_visibility.visibility_kind",
                        )?,
                    )?,
                    entity_id: required_text_value(
                        row,
                        5,
                        "lix_internal_untracked_change_visibility.entity_id",
                    )?,
                    schema_key: required_text_value(
                        row,
                        6,
                        "lix_internal_untracked_change_visibility.schema_key",
                    )?,
                    file_id: optional_text_value(
                        row,
                        7,
                        "lix_internal_untracked_change_visibility.file_id",
                    )?
                    .unwrap_or_default(),
                },
                snapshot_id: required_text_value(row, 8, "lix_internal_change.snapshot_id")?,
                change_created_at: required_text_value(row, 9, "lix_internal_change.created_at")?,
                visibility_created_at: required_text_value(
                    row,
                    10,
                    "lix_internal_untracked_change_visibility.created_at",
                )?,
            })
        })
        .collect()
}

fn select_compactable_untracked_change_rows(
    rows: &[CanonicalUntrackedChangeRow],
    low_watermark: Option<&DurableStateCommitCursor>,
) -> Vec<CanonicalUntrackedChangeRow> {
    let mut delete_rows = Vec::new();
    let mut index = 0usize;

    while index < rows.len() {
        let identity = rows[index].identity.clone();
        let start = index;
        while index < rows.len() && rows[index].identity == identity {
            index += 1;
        }
        let group = &rows[start..index];
        let latest = &group[0];
        let latest_is_delete = latest.snapshot_id == "no-content";
        let latest_is_eligible =
            untracked_change_row_is_at_or_below_watermark(latest, low_watermark);

        if latest_is_delete && latest_is_eligible {
            delete_rows.extend(group.iter().cloned());
            continue;
        }

        for row in group.iter().skip(1) {
            if untracked_change_row_is_at_or_below_watermark(row, low_watermark) {
                delete_rows.push(row.clone());
            }
        }
    }

    delete_rows
}

fn untracked_change_row_is_at_or_below_watermark(
    row: &CanonicalUntrackedChangeRow,
    low_watermark: Option<&DurableStateCommitCursor>,
) -> bool {
    low_watermark
        .is_none_or(|watermark| row.visibility_append_seq <= watermark.visibility_append_seq)
}

async fn delete_visibility_rows_and_orphaned_changes(
    transaction: CanonicalTransactionRef<'_>,
    rows: &[CanonicalUntrackedChangeRow],
) -> Result<(), LixError> {
    const DELETE_CHUNK_SIZE: usize = 256;
    if rows.is_empty() {
        return Ok(());
    }

    let dialect = transaction.dialect();
    let visibility_ids = rows
        .iter()
        .map(|row| row.visibility_id.clone())
        .collect::<Vec<_>>();
    for chunk in visibility_ids.chunks(DELETE_CHUNK_SIZE) {
        let mut params = Vec::with_capacity(chunk.len());
        let placeholders = chunk
            .iter()
            .enumerate()
            .map(|(index, visibility_id)| {
                params.push(Value::Text(visibility_id.clone()));
                dialect.placeholder(index + 1)
            })
            .collect::<Vec<_>>();
        execute_query_with_transaction(
            transaction,
            &format!(
                "DELETE FROM lix_internal_untracked_change_visibility \
                 WHERE id IN ({})",
                placeholders.join(", ")
            ),
            &params,
        )
        .await?;
    }

    let change_ids = rows
        .iter()
        .map(|row| row.change_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let change_commit_ctes =
        crate::canonical::build_lazy_change_commit_by_change_id_ctes_sql(dialect);
    for chunk in change_ids.chunks(DELETE_CHUNK_SIZE) {
        let mut params = Vec::with_capacity(chunk.len());
        let placeholders = chunk
            .iter()
            .enumerate()
            .map(|(index, change_id)| {
                params.push(Value::Text(change_id.clone()));
                dialect.placeholder(index + 1)
            })
            .collect::<Vec<_>>();
        execute_query_with_transaction(
            transaction,
            &format!(
                "WITH {change_commit_ctes} \
                 DELETE FROM lix_internal_change \
                 WHERE id IN ({placeholders}) \
                   AND NOT EXISTS (\
                     SELECT 1 FROM lix_internal_untracked_change_visibility v \
                     WHERE v.change_id = lix_internal_change.id\
                   ) \
                   AND NOT EXISTS (\
                     SELECT 1 FROM change_commit_by_change_id cc \
                     WHERE cc.change_id = lix_internal_change.id\
                   )",
                change_commit_ctes = change_commit_ctes,
                placeholders = placeholders.join(", "),
            ),
            &params,
        )
        .await?;
    }

    let snapshot_ids = rows
        .iter()
        .map(|row| row.snapshot_id.clone())
        .filter(|snapshot_id| snapshot_id != "no-content")
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    for chunk in snapshot_ids.chunks(DELETE_CHUNK_SIZE) {
        let mut params = Vec::with_capacity(chunk.len());
        let placeholders = chunk
            .iter()
            .enumerate()
            .map(|(index, snapshot_id)| {
                params.push(Value::Text(snapshot_id.clone()));
                dialect.placeholder(index + 1)
            })
            .collect::<Vec<_>>();
        execute_query_with_transaction(
            transaction,
            &format!(
                "DELETE FROM lix_internal_snapshot \
                 WHERE id IN ({}) \
                   AND NOT EXISTS (\
                     SELECT 1 FROM lix_internal_change c \
                     WHERE c.snapshot_id = lix_internal_snapshot.id\
                   )",
                placeholders.join(", ")
            ),
            &params,
        )
        .await?;
    }

    Ok(())
}

fn canonical_untracked_identity_from_visibility(
    visibility: &CanonicalUntrackedVisibilityWrite,
) -> CanonicalUntrackedIdentity {
    CanonicalUntrackedIdentity {
        version_id: visibility.version_id.clone(),
        visibility_kind: visibility.visibility_kind,
        entity_id: visibility.entity_id.to_string(),
        schema_key: visibility.schema_key.to_string(),
        file_id: visibility
            .file_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default(),
    }
}

fn parse_canonical_history_rows(result: QueryResult) -> Result<Vec<CanonicalHistoryRow>, LixError> {
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        rows.push(CanonicalHistoryRow {
            entity_id: required_text_value(&row, 0, "entity_id")?,
            schema_key: required_text_value(&row, 1, "schema_key")?,
            file_id: optional_text_value(&row, 2, "file_id")?,
            plugin_key: optional_text_value(&row, 3, "plugin_key")?,
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
            file_id: optional_text_value(&row, 4, "file_id")?,
            plugin_key: optional_text_value(&row, 5, "plugin_key")?,
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
    executor: CanonicalExecutorRef<'_>,
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

#[cfg(test)]
mod tests {
    use super::{
        append_untracked_change_visibility_rows, change_has_untracked_visibility,
        change_is_untracked_visibility_reachable, compact_stale_untracked_changes_in_transaction,
        compact_untracked_changes_for_touched_rows_in_transaction,
        load_latest_untracked_change_visibility_for_identity, required_integer_value,
        required_text_value, CanonicalUntrackedVisibilityIdentity,
        CanonicalUntrackedVisibilityKind, CanonicalUntrackedVisibilityWrite,
    };
    use crate::backend::LixBackend;
    use crate::streams::upsert_durable_state_commit_consumer_cursor_in_transaction;
    use crate::test_support::{
        init_test_backend_core, seed_canonical_change_row, CanonicalChangeSeed, TestSqliteBackend,
    };
    use crate::{TransactionBeginMode, Value};

    async fn init_canonical_compaction_backend() -> TestSqliteBackend {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");
        backend
    }

    async fn append_visibility_rows(
        backend: &TestSqliteBackend,
        visibility_rows: &[CanonicalUntrackedVisibilityWrite],
    ) {
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should begin");
        append_untracked_change_visibility_rows(transaction.as_mut(), visibility_rows)
            .await
            .expect("visibility rows should append");
        transaction
            .commit()
            .await
            .expect("transaction commit should succeed");
    }

    async fn load_untracked_change_ids_for_entity(
        backend: &TestSqliteBackend,
        entity_id: &str,
    ) -> Vec<String> {
        backend
            .execute(
                "SELECT change_id \
                 FROM lix_internal_untracked_change_visibility \
                 WHERE entity_id = ?1 \
                   AND schema_key = 'lix_key_value' \
                 ORDER BY created_at ASC, change_id ASC",
                &[Value::Text(entity_id.to_string())],
            )
            .await
            .expect("change ids should load")
            .rows
            .into_iter()
            .map(|row| match &row[0] {
                Value::Text(value) => value.clone(),
                other => panic!("expected text change id, got {other:?}"),
            })
            .collect()
    }

    async fn count_snapshot_row(backend: &TestSqliteBackend, snapshot_id: &str) -> i64 {
        match &backend
            .execute(
                "SELECT COUNT(*) FROM lix_internal_snapshot WHERE id = ?1",
                &[Value::Text(snapshot_id.to_string())],
            )
            .await
            .expect("snapshot count should load")
            .rows[0][0]
        {
            Value::Integer(value) => *value,
            other => panic!("expected integer snapshot count, got {other:?}"),
        }
    }

    async fn count_change_row(backend: &TestSqliteBackend, change_id: &str) -> i64 {
        match &backend
            .execute(
                "SELECT COUNT(*) FROM lix_internal_change WHERE id = ?1",
                &[Value::Text(change_id.to_string())],
            )
            .await
            .expect("change count should load")
            .rows[0][0]
        {
            Value::Integer(value) => *value,
            other => panic!("expected integer change count, got {other:?}"),
        }
    }

    fn seed_untracked_change(
        id: &'static str,
        entity_id: &'static str,
        snapshot_id: &'static str,
        snapshot_content: &'static str,
        created_at: &'static str,
    ) -> CanonicalChangeSeed<'static> {
        CanonicalChangeSeed {
            id,
            entity_id,
            schema_key: "lix_key_value",
            schema_version: "1",
            file_id: None,
            plugin_key: None,
            snapshot_id,
            snapshot_content: Some(snapshot_content),
            metadata: None,
            created_at,
        }
    }

    fn seed_untracked_delete(
        id: &'static str,
        entity_id: &'static str,
        created_at: &'static str,
    ) -> CanonicalChangeSeed<'static> {
        CanonicalChangeSeed {
            id,
            entity_id,
            schema_key: "lix_key_value",
            schema_version: "1",
            file_id: None,
            plugin_key: None,
            snapshot_id: "no-content",
            snapshot_content: None,
            metadata: None,
            created_at,
        }
    }

    fn seed_commit_header(
        id: &'static str,
        snapshot_id: &'static str,
        snapshot_content: &'static str,
        created_at: &'static str,
    ) -> CanonicalChangeSeed<'static> {
        CanonicalChangeSeed {
            id,
            entity_id: id,
            schema_key: "lix_commit",
            schema_version: "1",
            file_id: None,
            plugin_key: None,
            snapshot_id,
            snapshot_content: Some(snapshot_content),
            metadata: None,
            created_at,
        }
    }

    fn visibility_row(
        id: &'static str,
        change_id: &'static str,
        version_id: &'static str,
        entity_id: &'static str,
        created_at: &'static str,
    ) -> CanonicalUntrackedVisibilityWrite {
        CanonicalUntrackedVisibilityWrite {
            id: id.to_string(),
            change_id: change_id.to_string(),
            version_id: version_id.to_string(),
            visibility_kind: CanonicalUntrackedVisibilityKind::Version,
            entity_id: entity_id.to_string().try_into().unwrap(),
            schema_key: "lix_key_value".to_string().try_into().unwrap(),
            file_id: None,
            created_at: created_at.to_string(),
        }
    }

    fn version_scope_identity(
        version_id: &'static str,
        entity_id: &'static str,
    ) -> CanonicalUntrackedVisibilityIdentity {
        CanonicalUntrackedVisibilityIdentity {
            version_id: version_id.to_string(),
            visibility_kind: CanonicalUntrackedVisibilityKind::Version,
            entity_id: entity_id.to_string().try_into().unwrap(),
            schema_key: "lix_key_value".to_string().try_into().unwrap(),
            file_id: None,
        }
    }

    #[tokio::test]
    async fn append_untracked_change_visibility_rows_marks_change_as_visible_and_reachable() {
        let backend = init_canonical_compaction_backend().await;
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-1",
                "visible-key",
                "snapshot-1",
                "{\"key\":\"visible-key\",\"value\":\"v1\"}",
                "2026-04-15T00:00:01Z",
            ),
        )
        .await
        .expect("untracked-visible change should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-2",
                "no-visibility-key",
                "snapshot-2",
                "{\"key\":\"no-visibility-key\",\"value\":\"v1\"}",
                "2026-04-15T00:00:02Z",
            ),
        )
        .await
        .expect("change without untracked visibility should seed");

        append_visibility_rows(
            &backend,
            &[visibility_row(
                "visibility-1",
                "change-1",
                "version-main",
                "visible-key",
                "2026-04-15T00:00:03Z",
            )],
        )
        .await;

        let mut executor = &backend;
        assert!(
            change_has_untracked_visibility(&mut executor, "change-1")
                .await
                .expect("untracked-visible change should resolve"),
            "untracked-visible change should be marked visible"
        );
        assert!(
            change_is_untracked_visibility_reachable(&mut executor, "change-1")
                .await
                .expect("untracked-visible change reachability should resolve"),
            "untracked-visible change should be visibility-reachable"
        );
        assert!(
            !change_has_untracked_visibility(&mut executor, "change-2")
                .await
                .expect("change without untracked visibility should resolve"),
            "change without untracked visibility should not be marked visible"
        );
        assert!(
            !change_is_untracked_visibility_reachable(&mut executor, "change-2")
                .await
                .expect("change without untracked visibility reachability should resolve"),
            "change without untracked visibility should not be visibility-reachable"
        );
    }

    #[tokio::test]
    async fn latest_untracked_change_visibility_for_identity_returns_latest_matching_visibility_row(
    ) {
        let backend = init_canonical_compaction_backend().await;
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-1",
                "visible-key",
                "snapshot-1",
                "{\"key\":\"visible-key\",\"value\":\"v1\"}",
                "2026-04-15T00:00:01Z",
            ),
        )
        .await
        .expect("first change should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-2",
                "visible-key",
                "snapshot-2",
                "{\"key\":\"visible-key\",\"value\":\"v2\"}",
                "2026-04-15T00:00:02Z",
            ),
        )
        .await
        .expect("second change should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-3",
                "other-key",
                "snapshot-3",
                "{\"key\":\"other-key\",\"value\":\"v1\"}",
                "2026-04-15T00:00:03Z",
            ),
        )
        .await
        .expect("third change should seed");

        append_visibility_rows(
            &backend,
            &[
                visibility_row(
                    "visibility-1",
                    "change-1",
                    "version-main",
                    "visible-key",
                    "2026-04-15T00:00:04Z",
                ),
                visibility_row(
                    "visibility-2",
                    "change-2",
                    "version-main",
                    "visible-key",
                    "2026-04-15T00:00:05Z",
                ),
                visibility_row(
                    "visibility-3",
                    "change-3",
                    "version-other",
                    "other-key",
                    "2026-04-15T00:00:06Z",
                ),
            ],
        )
        .await;

        let mut executor = &backend;
        let latest = load_latest_untracked_change_visibility_for_identity(
            &mut executor,
            &version_scope_identity("version-main", "visible-key"),
        )
        .await
        .expect("latest visibility row should load")
        .expect("matching visibility row should exist");

        assert_eq!(latest.id, "visibility-2");
        assert_eq!(latest.change_id, "change-2");
        assert_eq!(latest.version_id, "version-main");
        assert_eq!(
            latest.visibility_kind,
            CanonicalUntrackedVisibilityKind::Version
        );
        assert_eq!(latest.entity_id.to_string(), "visible-key");
        assert_eq!(latest.schema_key.to_string(), "lix_key_value");
        assert_eq!(latest.file_id, None);
        assert_eq!(latest.created_at, "2026-04-15T00:00:05Z");
        assert!(
            latest.append_seq > 0,
            "latest visibility row should have a durable append sequence"
        );

        let missing = load_latest_untracked_change_visibility_for_identity(
            &mut executor,
            &version_scope_identity("version-missing", "visible-key"),
        )
        .await
        .expect("missing identity lookup should succeed");
        assert!(
            missing.is_none(),
            "non-matching version identity should not load a visibility row"
        );
    }

    #[tokio::test]
    async fn append_untracked_change_visibility_rows_assign_append_seq_in_insert_order() {
        let backend = init_canonical_compaction_backend().await;
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-1",
                "visible-key",
                "snapshot-1",
                "{\"key\":\"visible-key\",\"value\":\"v1\"}",
                "2026-04-15T00:00:01Z",
            ),
        )
        .await
        .expect("first change should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-2",
                "visible-key",
                "snapshot-2",
                "{\"key\":\"visible-key\",\"value\":\"v2\"}",
                "2026-04-15T00:00:02Z",
            ),
        )
        .await
        .expect("second change should seed");

        append_visibility_rows(
            &backend,
            &[
                visibility_row(
                    "visibility-1",
                    "change-1",
                    "version-main",
                    "visible-key",
                    "2026-04-15T00:00:03Z",
                ),
                visibility_row(
                    "visibility-2",
                    "change-2",
                    "version-main",
                    "visible-key",
                    "2026-04-15T00:00:04Z",
                ),
            ],
        )
        .await;

        let rows = backend
            .execute(
                "SELECT id, append_seq \
                 FROM lix_internal_untracked_change_visibility \
                 ORDER BY append_seq ASC",
                &[],
            )
            .await
            .expect("visibility rows should load");

        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            required_text_value(
                &rows.rows[0],
                0,
                "lix_internal_untracked_change_visibility.id"
            )
            .expect("first visibility id should load"),
            "visibility-1"
        );
        assert_eq!(
            required_text_value(
                &rows.rows[1],
                0,
                "lix_internal_untracked_change_visibility.id"
            )
            .expect("second visibility id should load"),
            "visibility-2"
        );
        let first_seq = required_integer_value(
            &rows.rows[0],
            1,
            "lix_internal_untracked_change_visibility.append_seq",
        )
        .expect("first visibility append_seq should load");
        let second_seq = required_integer_value(
            &rows.rows[1],
            1,
            "lix_internal_untracked_change_visibility.append_seq",
        )
        .expect("second visibility append_seq should load");
        assert!(
            first_seq < second_seq,
            "append sequence should preserve insert order"
        );
    }

    #[tokio::test]
    async fn latest_untracked_change_visibility_prefers_visibility_order_over_change_created_at() {
        let backend = init_canonical_compaction_backend().await;
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-newer-fact",
                "visible-key",
                "snapshot-1",
                "{\"key\":\"visible-key\",\"value\":\"fact-newer\"}",
                "2026-04-15T00:00:02Z",
            ),
        )
        .await
        .expect("newer canonical fact should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-older-fact",
                "visible-key",
                "snapshot-2",
                "{\"key\":\"visible-key\",\"value\":\"fact-older\"}",
                "2026-04-15T00:00:01Z",
            ),
        )
        .await
        .expect("older canonical fact should seed");

        append_visibility_rows(
            &backend,
            &[
                visibility_row(
                    "visibility-1",
                    "change-newer-fact",
                    "version-main",
                    "visible-key",
                    "2026-04-15T00:00:05Z",
                ),
                visibility_row(
                    "visibility-2",
                    "change-older-fact",
                    "version-main",
                    "visible-key",
                    "2026-04-15T00:00:04Z",
                ),
            ],
        )
        .await;

        let mut executor = &backend;
        let latest = load_latest_untracked_change_visibility_for_identity(
            &mut executor,
            &version_scope_identity("version-main", "visible-key"),
        )
        .await
        .expect("latest visibility row should load")
        .expect("matching visibility row should exist");

        assert_eq!(latest.id, "visibility-2");
        assert_eq!(latest.change_id, "change-older-fact");
        assert_eq!(latest.created_at, "2026-04-15T00:00:04Z");
        assert!(
            latest.append_seq > 0,
            "latest visibility row should still expose append order metadata"
        );
    }

    #[tokio::test]
    async fn stale_untracked_compaction_keeps_latest_row_per_identity_and_prunes_snapshots() {
        let backend = init_canonical_compaction_backend().await;
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-1",
                "gc-key",
                "snapshot-1",
                "{\"key\":\"gc-key\",\"value\":\"v1\"}",
                "2026-04-15T00:00:01Z",
            ),
        )
        .await
        .expect("first row should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-2",
                "gc-key",
                "snapshot-2",
                "{\"key\":\"gc-key\",\"value\":\"v2\"}",
                "2026-04-15T00:00:02Z",
            ),
        )
        .await
        .expect("second row should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-3",
                "other-key",
                "snapshot-3",
                "{\"key\":\"other-key\",\"value\":\"v1\"}",
                "2026-04-15T00:00:03Z",
            ),
        )
        .await
        .expect("third row should seed");
        append_visibility_rows(
            &backend,
            &[
                visibility_row(
                    "visibility-1",
                    "change-1",
                    "version-main",
                    "gc-key",
                    "2026-04-15T00:00:03Z",
                ),
                visibility_row(
                    "visibility-2",
                    "change-2",
                    "version-main",
                    "gc-key",
                    "2026-04-15T00:00:02Z",
                ),
                visibility_row(
                    "visibility-3",
                    "change-3",
                    "version-main",
                    "other-key",
                    "2026-04-15T00:00:03Z",
                ),
            ],
        )
        .await;

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should begin");
        let deleted = compact_stale_untracked_changes_in_transaction(transaction.as_mut())
            .await
            .expect("maintenance sweep should succeed");
        transaction
            .commit()
            .await
            .expect("transaction commit should succeed");

        assert_eq!(deleted, 1);
        assert_eq!(
            load_untracked_change_ids_for_entity(&backend, "gc-key").await,
            vec!["change-2".to_string()]
        );
        assert_eq!(
            load_untracked_change_ids_for_entity(&backend, "other-key").await,
            vec!["change-3".to_string()]
        );
        assert_eq!(count_snapshot_row(&backend, "snapshot-1").await, 0);
        assert_eq!(count_snapshot_row(&backend, "snapshot-2").await, 1);
    }

    #[tokio::test]
    async fn stale_untracked_compaction_prefers_visibility_append_order_over_created_at() {
        let backend = init_canonical_compaction_backend().await;
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-1",
                "gc-key",
                "snapshot-1",
                "{\"key\":\"gc-key\",\"value\":\"v1\"}",
                "2026-04-15T00:00:01Z",
            ),
        )
        .await
        .expect("first row should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-2",
                "gc-key",
                "snapshot-2",
                "{\"key\":\"gc-key\",\"value\":\"v2\"}",
                "2026-04-15T00:00:02Z",
            ),
        )
        .await
        .expect("second row should seed");

        append_visibility_rows(
            &backend,
            &[
                visibility_row(
                    "visibility-1",
                    "change-1",
                    "version-main",
                    "gc-key",
                    "2026-04-15T00:00:05Z",
                ),
                visibility_row(
                    "visibility-2",
                    "change-2",
                    "version-main",
                    "gc-key",
                    "2026-04-15T00:00:04Z",
                ),
            ],
        )
        .await;

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should begin");
        let deleted = compact_stale_untracked_changes_in_transaction(transaction.as_mut())
            .await
            .expect("maintenance sweep should succeed");
        transaction
            .commit()
            .await
            .expect("transaction commit should succeed");

        assert_eq!(deleted, 1);
        assert_eq!(
            load_untracked_change_ids_for_entity(&backend, "gc-key").await,
            vec!["change-2".to_string()]
        );
        assert_eq!(count_snapshot_row(&backend, "snapshot-1").await, 0);
        assert_eq!(count_snapshot_row(&backend, "snapshot-2").await, 1);
    }

    #[tokio::test]
    async fn stale_untracked_compaction_only_deletes_rows_at_or_below_low_watermark() {
        let backend = init_canonical_compaction_backend().await;
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-1",
                "gc-key",
                "snapshot-1",
                "{\"key\":\"gc-key\",\"value\":\"v1\"}",
                "2026-04-15T00:00:01Z",
            ),
        )
        .await
        .expect("first row should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-2",
                "gc-key",
                "snapshot-2",
                "{\"key\":\"gc-key\",\"value\":\"v2\"}",
                "2026-04-15T00:00:02Z",
            ),
        )
        .await
        .expect("second row should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-3",
                "gc-key",
                "snapshot-3",
                "{\"key\":\"gc-key\",\"value\":\"v3\"}",
                "2026-04-15T00:00:03Z",
            ),
        )
        .await
        .expect("third row should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-4",
                "old-but-current",
                "snapshot-4",
                "{\"key\":\"old-but-current\",\"value\":\"v1\"}",
                "2026-04-15T00:00:01Z",
            ),
        )
        .await
        .expect("fourth row should seed");
        append_visibility_rows(
            &backend,
            &[
                visibility_row(
                    "visibility-1",
                    "change-1",
                    "version-main",
                    "gc-key",
                    "2026-04-15T00:00:01Z",
                ),
                visibility_row(
                    "visibility-2",
                    "change-2",
                    "version-main",
                    "gc-key",
                    "2026-04-15T00:00:02Z",
                ),
                visibility_row(
                    "visibility-3",
                    "change-3",
                    "version-main",
                    "gc-key",
                    "2026-04-15T00:00:03Z",
                ),
                visibility_row(
                    "visibility-4",
                    "change-4",
                    "version-main",
                    "old-but-current",
                    "2026-04-15T00:00:01Z",
                ),
            ],
        )
        .await;

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should begin");
        upsert_durable_state_commit_consumer_cursor_in_transaction(
            transaction.as_mut(),
            "live-state",
            &crate::streams::DurableStateCommitCursor {
                change_id: "change-2".to_string(),
                created_at: "2026-04-15T00:00:02Z".to_string(),
                visibility_append_seq: 2,
            },
        )
        .await
        .expect("consumer cursor should persist");
        let deleted = compact_stale_untracked_changes_in_transaction(transaction.as_mut())
            .await
            .expect("maintenance sweep should succeed");
        transaction
            .commit()
            .await
            .expect("transaction commit should succeed");

        assert_eq!(deleted, 2);
        assert_eq!(
            load_untracked_change_ids_for_entity(&backend, "gc-key").await,
            vec!["change-3".to_string()]
        );
        assert_eq!(
            load_untracked_change_ids_for_entity(&backend, "old-but-current").await,
            vec!["change-4".to_string()]
        );
        assert_eq!(count_snapshot_row(&backend, "snapshot-1").await, 0);
        assert_eq!(count_snapshot_row(&backend, "snapshot-2").await, 0);
        assert_eq!(count_snapshot_row(&backend, "snapshot-3").await, 1);
        assert_eq!(count_snapshot_row(&backend, "snapshot-4").await, 1);
    }

    #[tokio::test]
    async fn touched_untracked_compaction_only_prunes_requested_identities() {
        let backend = init_canonical_compaction_backend().await;
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-1",
                "gc-key",
                "snapshot-1",
                "{\"key\":\"gc-key\",\"value\":\"v1\"}",
                "2026-04-15T00:00:01Z",
            ),
        )
        .await
        .expect("first row should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-2",
                "gc-key",
                "snapshot-2",
                "{\"key\":\"gc-key\",\"value\":\"v2\"}",
                "2026-04-15T00:00:02Z",
            ),
        )
        .await
        .expect("second row should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-3",
                "untouched-key",
                "snapshot-3",
                "{\"key\":\"untouched-key\",\"value\":\"v1\"}",
                "2026-04-15T00:00:01Z",
            ),
        )
        .await
        .expect("third row should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-4",
                "untouched-key",
                "snapshot-4",
                "{\"key\":\"untouched-key\",\"value\":\"v2\"}",
                "2026-04-15T00:00:02Z",
            ),
        )
        .await
        .expect("fourth row should seed");
        append_visibility_rows(
            &backend,
            &[
                visibility_row(
                    "visibility-1",
                    "change-1",
                    "version-main",
                    "gc-key",
                    "2026-04-15T00:00:01Z",
                ),
                visibility_row(
                    "visibility-2",
                    "change-2",
                    "version-main",
                    "gc-key",
                    "2026-04-15T00:00:02Z",
                ),
                visibility_row(
                    "visibility-3",
                    "change-3",
                    "version-main",
                    "untouched-key",
                    "2026-04-15T00:00:01Z",
                ),
                visibility_row(
                    "visibility-4",
                    "change-4",
                    "version-main",
                    "untouched-key",
                    "2026-04-15T00:00:02Z",
                ),
            ],
        )
        .await;

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should begin");
        let deleted = compact_untracked_changes_for_touched_rows_in_transaction(
            transaction.as_mut(),
            &[visibility_row(
                "visibility-5",
                "change-5",
                "version-main",
                "gc-key",
                "2026-04-15T00:00:03Z",
            )],
        )
        .await
        .expect("touched compaction should succeed");
        transaction
            .commit()
            .await
            .expect("transaction commit should succeed");

        assert_eq!(deleted, 1);
        assert_eq!(
            load_untracked_change_ids_for_entity(&backend, "gc-key").await,
            vec!["change-2".to_string()]
        );
        assert_eq!(
            load_untracked_change_ids_for_entity(&backend, "untouched-key").await,
            vec!["change-3".to_string(), "change-4".to_string()]
        );
    }

    #[tokio::test]
    async fn stale_untracked_compaction_keeps_commit_referenced_changes_reachable() {
        let backend = init_canonical_compaction_backend().await;
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-1",
                "gc-key",
                "snapshot-1",
                "{\"key\":\"gc-key\",\"value\":\"v1\"}",
                "2026-04-15T00:00:01Z",
            ),
        )
        .await
        .expect("first row should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-2",
                "gc-key",
                "snapshot-2",
                "{\"key\":\"gc-key\",\"value\":\"v2\"}",
                "2026-04-15T00:00:02Z",
            ),
        )
        .await
        .expect("second row should seed");
        seed_canonical_change_row(
            &backend,
            seed_commit_header(
                "commit-1",
                "snapshot-commit-1",
                "{\"id\":\"commit-1\",\"change_set_id\":\"cs-1\",\"change_ids\":[\"change-1\"],\"parent_commit_ids\":[]}",
                "2026-04-15T00:00:03Z",
            ),
        )
        .await
        .expect("commit header should seed");
        append_visibility_rows(
            &backend,
            &[
                visibility_row(
                    "visibility-1",
                    "change-1",
                    "version-main",
                    "gc-key",
                    "2026-04-15T00:00:01Z",
                ),
                visibility_row(
                    "visibility-2",
                    "change-2",
                    "version-main",
                    "gc-key",
                    "2026-04-15T00:00:02Z",
                ),
            ],
        )
        .await;

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should begin");
        let deleted = compact_stale_untracked_changes_in_transaction(transaction.as_mut())
            .await
            .expect("maintenance sweep should succeed");
        transaction
            .commit()
            .await
            .expect("transaction commit should succeed");

        assert_eq!(deleted, 1);
        assert_eq!(
            load_untracked_change_ids_for_entity(&backend, "gc-key").await,
            vec!["change-2".to_string()]
        );
        assert_eq!(count_change_row(&backend, "change-1").await, 1);
        assert_eq!(count_snapshot_row(&backend, "snapshot-1").await, 1);
    }

    #[tokio::test]
    async fn latest_untracked_delete_below_watermark_compacts_identity_to_absence() {
        let backend = init_canonical_compaction_backend().await;
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-1",
                "delete-key",
                "snapshot-1",
                "{\"key\":\"delete-key\",\"value\":\"v1\"}",
                "2026-04-15T00:00:01Z",
            ),
        )
        .await
        .expect("upsert row should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_delete("change-2", "delete-key", "2026-04-15T00:00:02Z"),
        )
        .await
        .expect("delete row should seed");
        append_visibility_rows(
            &backend,
            &[
                visibility_row(
                    "visibility-1",
                    "change-1",
                    "version-main",
                    "delete-key",
                    "2026-04-15T00:00:01Z",
                ),
                visibility_row(
                    "visibility-2",
                    "change-2",
                    "version-main",
                    "delete-key",
                    "2026-04-15T00:00:02Z",
                ),
            ],
        )
        .await;

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should begin");
        let deleted = compact_stale_untracked_changes_in_transaction(transaction.as_mut())
            .await
            .expect("maintenance sweep should succeed");
        transaction
            .commit()
            .await
            .expect("transaction commit should succeed");

        assert_eq!(deleted, 2);
        assert!(
            load_untracked_change_ids_for_entity(&backend, "delete-key")
                .await
                .is_empty(),
            "latest delete below watermark should fold into absence"
        );
        assert_eq!(count_snapshot_row(&backend, "snapshot-1").await, 0);
    }

    #[tokio::test]
    async fn latest_untracked_delete_above_watermark_survives_until_safe() {
        let backend = init_canonical_compaction_backend().await;
        seed_canonical_change_row(
            &backend,
            seed_untracked_change(
                "change-1",
                "delete-key",
                "snapshot-1",
                "{\"key\":\"delete-key\",\"value\":\"v1\"}",
                "2026-04-15T00:00:01Z",
            ),
        )
        .await
        .expect("upsert row should seed");
        seed_canonical_change_row(
            &backend,
            seed_untracked_delete("change-2", "delete-key", "2026-04-15T00:00:02Z"),
        )
        .await
        .expect("delete row should seed");
        append_visibility_rows(
            &backend,
            &[
                visibility_row(
                    "visibility-1",
                    "change-1",
                    "version-main",
                    "delete-key",
                    "2026-04-15T00:00:01Z",
                ),
                visibility_row(
                    "visibility-2",
                    "change-2",
                    "version-main",
                    "delete-key",
                    "2026-04-15T00:00:02Z",
                ),
            ],
        )
        .await;

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should begin");
        upsert_durable_state_commit_consumer_cursor_in_transaction(
            transaction.as_mut(),
            "live-state",
            &crate::streams::DurableStateCommitCursor {
                change_id: "change-1".to_string(),
                created_at: "2026-04-15T00:00:01Z".to_string(),
                visibility_append_seq: 1,
            },
        )
        .await
        .expect("consumer cursor should persist");
        let deleted = compact_stale_untracked_changes_in_transaction(transaction.as_mut())
            .await
            .expect("maintenance sweep should succeed");
        transaction
            .commit()
            .await
            .expect("transaction commit should succeed");

        assert_eq!(deleted, 1);
        assert_eq!(
            load_untracked_change_ids_for_entity(&backend, "delete-key").await,
            vec!["change-2".to_string()]
        );
        assert_eq!(count_snapshot_row(&backend, "snapshot-1").await, 0);
    }
}
