use crate::canonical::read::{
    build_state_history_source_sql, CanonicalHistoryContentMode, CanonicalHistoryRootFacts,
    CanonicalHistoryRootSelection, CanonicalRootCommit,
};
use crate::common::text::escape_sql_string;
use crate::contracts::artifacts::{
    StateHistoryContentMode, StateHistoryLineageScope, StateHistoryOrder, StateHistoryRequest,
    StateHistoryRootScope, StateHistoryRow, StateHistoryVersionScope,
};
use crate::version_state::{
    resolve_history_root_facts_with_backend, HistoryRootFacts, HistoryRootTraversal,
    RootCommitResolutionRequest, RootCommitScope, RootLineageScope, RootVersionScope,
};
use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};

use super::context::resolve_target_version_with_backend;

pub(crate) async fn load_state_history_rows(
    backend: &dyn LixBackend,
    request: &StateHistoryRequest,
) -> Result<Vec<StateHistoryRow>, LixError> {
    let resolved_active_version_id = resolve_active_version_id(backend, request).await?;
    let root_facts = resolve_history_root_facts_with_backend(
        backend,
        root_commit_resolution_request(request, resolved_active_version_id.as_deref()),
    )
    .await?;
    let sql = build_state_history_query_sql(
        backend.dialect(),
        request,
        &canonical_history_root_facts(root_facts),
    )?;
    let result = backend.execute(&sql, &[]).await?;
    parse_state_history_rows(result)
}

async fn resolve_active_version_id(
    backend: &dyn LixBackend,
    request: &StateHistoryRequest,
) -> Result<Option<String>, LixError> {
    match request.lineage_scope {
        StateHistoryLineageScope::Standard => Ok(None),
        StateHistoryLineageScope::ActiveVersion => {
            if let Some(active_version_id) = request.active_version_id.clone() {
                return Ok(Some(active_version_id));
            }
            Ok(Some(
                resolve_target_version_with_backend(backend, None, "active_version_id")
                    .await?
                    .version_id,
            ))
        }
    }
}

fn build_state_history_query_sql(
    dialect: SqlDialect,
    request: &StateHistoryRequest,
    root_facts: &CanonicalHistoryRootFacts,
) -> Result<String, LixError> {
    let source_sql = build_state_history_source_sql(
        dialect,
        root_facts,
        match request.content_mode {
            StateHistoryContentMode::MetadataOnly => CanonicalHistoryContentMode::MetadataOnly,
            StateHistoryContentMode::IncludeSnapshotContent => {
                CanonicalHistoryContentMode::IncludeSnapshotContent
            }
        },
        request.max_depth,
    )?;

    let mut predicates = Vec::new();
    if !request.entity_ids.is_empty() {
        predicates.push(render_text_in_predicate(
            "history.entity_id",
            &request.entity_ids,
        ));
    }
    if !request.file_ids.is_empty() {
        predicates.push(render_text_in_predicate(
            "history.file_id",
            &request.file_ids,
        ));
    }
    if !request.schema_keys.is_empty() {
        predicates.push(render_text_in_predicate(
            "history.schema_key",
            &request.schema_keys,
        ));
    }
    if !request.plugin_keys.is_empty() {
        predicates.push(render_text_in_predicate(
            "history.plugin_key",
            &request.plugin_keys,
        ));
    }
    if let Some(min_depth) = request.min_depth {
        predicates.push(format!("history.depth >= {min_depth}"));
    }
    if let Some(max_depth) = request.max_depth {
        predicates.push(format!("history.depth <= {max_depth}"));
    }

    let where_sql = render_where_clause_sql(&predicates, "WHERE ");
    let order_sql = match request.order {
        StateHistoryOrder::EntityFileSchemaDepthAsc => {
            "ORDER BY history.entity_id ASC, history.file_id ASC, history.schema_key ASC, history.depth ASC"
        }
    };

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
         {order_sql}",
        source_sql = source_sql,
        where_sql = where_sql,
        order_sql = order_sql,
    ))
}

fn root_commit_resolution_request<'a>(
    request: &'a StateHistoryRequest,
    active_version_id: Option<&'a str>,
) -> RootCommitResolutionRequest<'a> {
    RootCommitResolutionRequest {
        lineage_scope: match request.lineage_scope {
            StateHistoryLineageScope::Standard => RootLineageScope::Standard,
            StateHistoryLineageScope::ActiveVersion => RootLineageScope::ActiveVersion,
        },
        active_version_id,
        root_scope: match &request.root_scope {
            StateHistoryRootScope::AllRoots => RootCommitScope::AllRoots,
            StateHistoryRootScope::RequestedRoots(root_commit_ids) => {
                RootCommitScope::RequestedRoots(root_commit_ids)
            }
        },
        version_scope: match &request.version_scope {
            StateHistoryVersionScope::Any => RootVersionScope::Any,
            StateHistoryVersionScope::RequestedVersions(version_ids) => {
                RootVersionScope::RequestedVersions(version_ids)
            }
        },
    }
}

fn canonical_history_root_facts(root_facts: HistoryRootFacts) -> CanonicalHistoryRootFacts {
    CanonicalHistoryRootFacts {
        traversal: match root_facts.traversal {
            HistoryRootTraversal::AllRoots => CanonicalHistoryRootSelection::AllRoots,
            HistoryRootTraversal::RequestedRootCommitIds(root_commit_ids) => {
                CanonicalHistoryRootSelection::RequestedRootCommitIds(root_commit_ids)
            }
            HistoryRootTraversal::ResolvedRootCommits(root_commits) => {
                CanonicalHistoryRootSelection::ResolvedRootCommits(
                    root_commits
                        .into_iter()
                        .map(|root| CanonicalRootCommit {
                            commit_id: root.commit_id,
                            version_id: root.version_id,
                        })
                        .collect(),
                )
            }
        },
        root_version_refs: root_facts
            .root_version_refs
            .into_iter()
            .map(|root| CanonicalRootCommit {
                commit_id: root.commit_id,
                version_id: root.version_id,
            })
            .collect(),
    }
}

fn parse_state_history_rows(result: QueryResult) -> Result<Vec<StateHistoryRow>, LixError> {
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        rows.push(StateHistoryRow {
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

fn required_text_value(row: &[Value], index: usize, field: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(value.clone()),
        Some(other) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("expected text for {field}, got {other:?}"),
        }),
        None => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("missing column {field} at index {index}"),
        }),
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
        Some(other) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("expected nullable text for {field}, got {other:?}"),
        }),
    }
}

fn required_integer_value(row: &[Value], index: usize, field: &str) -> Result<i64, LixError> {
    match row.get(index) {
        Some(value) => match value {
            Value::Integer(value) => Ok(*value),
            Value::Real(value) => Ok(*value as i64),
            Value::Text(value) => value.parse::<i64>().map_err(|_| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("expected integer for {field}, got {value:?}"),
            }),
            other => Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("expected integer for {field}, got {other:?}"),
            }),
        },
        None => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("missing column {field} at index {index}"),
        }),
    }
}
