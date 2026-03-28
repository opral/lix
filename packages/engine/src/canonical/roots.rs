use crate::backend::QueryExecutor;
use crate::errors::classification::is_missing_relation_error;
use crate::live_state::constraints::{ScanConstraint, ScanField, ScanOperator};
use crate::live_state::raw::{load_exact_row_with_executor, scan_rows_with_executor, RawStorage};
use crate::schema::builtin::types::LixVersionRef;
use crate::version::{
    version_ref_file_id, version_ref_plugin_key, version_ref_schema_key,
    version_ref_schema_version, version_ref_storage_version_id,
};
use crate::{LixBackend, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionRefRow {
    pub(crate) version_id: String,
    pub(crate) commit_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedRootCommit {
    pub(crate) commit_id: String,
    pub(crate) version_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RootLineageScope {
    Standard,
    ActiveVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RootCommitScope<'a> {
    AllRoots,
    RequestedRoots(&'a [String]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RootVersionScope<'a> {
    Any,
    RequestedVersions(&'a [String]),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RootCommitResolutionRequest<'a> {
    pub(crate) lineage_scope: RootLineageScope,
    pub(crate) active_version_id: Option<&'a str>,
    pub(crate) root_scope: RootCommitScope<'a>,
    pub(crate) version_scope: RootVersionScope<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HistoryRootTraversal {
    AllRoots,
    RequestedRootCommitIds(Vec<String>),
    ResolvedRootCommits(Vec<ResolvedRootCommit>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistoryRootFacts {
    pub(crate) traversal: HistoryRootTraversal,
    pub(crate) root_version_refs: Vec<ResolvedRootCommit>,
}

pub(crate) async fn load_committed_version_head_commit_id(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    if let Some(version_ref) = load_version_ref_with_executor(executor, version_id).await? {
        if version_ref.commit_id.is_empty() {
            return Ok(None);
        }
        return Ok(Some(version_ref.commit_id));
    }

    let Some(version_ref) = load_version_ref_from_canonical(executor, version_id).await? else {
        return Ok(None);
    };
    if version_ref.commit_id.is_empty() {
        return Ok(None);
    }
    Ok(Some(version_ref.commit_id))
}

pub(crate) async fn load_version_ref_with_backend(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<VersionRefRow>, LixError> {
    let mut executor = backend;
    load_version_ref_with_executor(&mut executor, version_id).await
}

async fn load_version_ref_from_canonical(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<VersionRefRow>, LixError> {
    let sql = format!(
        "SELECT s.content AS snapshot_content \
         FROM lix_internal_change c \
         LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
         WHERE c.schema_key = '{schema_key}' \
           AND c.schema_version = '{schema_version}' \
           AND c.entity_id = '{entity_id}' \
           AND c.file_id = '{file_id}' \
           AND c.plugin_key = '{plugin_key}' \
           AND s.content IS NOT NULL \
         ORDER BY c.created_at DESC, c.id DESC \
         LIMIT 1",
        schema_key = escape_sql_string(version_ref_schema_key()),
        schema_version = escape_sql_string(version_ref_schema_version()),
        entity_id = escape_sql_string(version_id),
        file_id = escape_sql_string(version_ref_file_id()),
        plugin_key = escape_sql_string(version_ref_plugin_key()),
    );

    let snapshot_content = match executor.execute(&sql, &[]).await {
        Ok(result) => result.rows.first().and_then(|row| row.first()).cloned(),
        Err(err) if is_missing_relation_error(&err) => None,
        Err(err) => return Err(err),
    };
    parse_version_ref_snapshot(snapshot_content.as_ref()).map(|version_ref| {
        version_ref.map(|version_ref| VersionRefRow {
            version_id: version_ref.id,
            commit_id: version_ref.commit_id,
        })
    })
}

fn parse_version_ref_snapshot(value: Option<&Value>) -> Result<Option<LixVersionRef>, LixError> {
    let Some(raw_snapshot) = value else {
        return Ok(None);
    };
    let raw_snapshot = match raw_snapshot {
        Value::Text(value) => value,
        Value::Null => return Ok(None),
        _ => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "version ref snapshot_content must be text".to_string(),
            });
        }
    };

    let snapshot: LixVersionRef = serde_json::from_str(raw_snapshot).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("version ref snapshot_content invalid JSON: {error}"),
    })?;
    Ok(Some(snapshot))
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

pub(crate) async fn load_head_commit_id_for_version(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    load_committed_version_head_commit_id(executor, version_id).await
}

pub(crate) async fn load_all_version_head_commit_ids(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<ResolvedRootCommit>, LixError> {
    let facts = resolve_history_root_facts_with_executor(
        executor,
        RootCommitResolutionRequest {
            lineage_scope: RootLineageScope::Standard,
            active_version_id: None,
            root_scope: RootCommitScope::AllRoots,
            version_scope: RootVersionScope::Any,
        },
    )
    .await?;
    Ok(facts.root_version_refs)
}

pub(crate) async fn resolve_history_root_facts_with_backend(
    backend: &dyn LixBackend,
    request: RootCommitResolutionRequest<'_>,
) -> Result<HistoryRootFacts, LixError> {
    let mut executor = backend;
    resolve_history_root_facts_with_executor(&mut executor, request).await
}

pub(crate) async fn resolve_history_root_facts_with_executor(
    executor: &mut dyn QueryExecutor,
    request: RootCommitResolutionRequest<'_>,
) -> Result<HistoryRootFacts, LixError> {
    let scoped_version_ids = scoped_root_version_ids(request)?;
    let root_version_refs =
        load_scoped_root_version_refs_with_executor(executor, scoped_version_ids.as_deref())
            .await?;
    Ok(build_history_root_facts(request, root_version_refs))
}

fn scoped_root_version_ids(
    request: RootCommitResolutionRequest<'_>,
) -> Result<Option<Vec<String>>, LixError> {
    match request.lineage_scope {
        RootLineageScope::Standard => match request.version_scope {
            RootVersionScope::Any => Ok(None),
            RootVersionScope::RequestedVersions(version_ids) => Ok(Some(version_ids.to_vec())),
        },
        RootLineageScope::ActiveVersion => {
            let active_version_id = request.active_version_id.ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "active-version root resolution requires an active version id",
                )
            })?;
            if let RootVersionScope::RequestedVersions(version_ids) = request.version_scope {
                if !version_ids.iter().any(|value| value == active_version_id) {
                    return Ok(Some(Vec::new()));
                }
            }
            Ok(Some(vec![active_version_id.to_string()]))
        }
    }
}

async fn load_scoped_root_version_refs_with_executor(
    executor: &mut dyn QueryExecutor,
    scoped_version_ids: Option<&[String]>,
) -> Result<Vec<ResolvedRootCommit>, LixError> {
    let mut rows = match scoped_version_ids {
        Some(version_ids) => {
            let mut rows = Vec::new();
            for version_id in version_ids {
                if let Some(row) = load_version_ref_with_executor(executor, version_id).await? {
                    if !row.commit_id.is_empty() {
                        rows.push(ResolvedRootCommit {
                            commit_id: row.commit_id,
                            version_id: row.version_id,
                        });
                    }
                }
            }
            rows
        }
        None => load_all_root_version_refs_with_executor(executor).await?,
    };
    rows.sort_by(|left, right| {
        left.commit_id
            .cmp(&right.commit_id)
            .then(left.version_id.cmp(&right.version_id))
    });
    Ok(rows)
}

async fn load_all_root_version_refs_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<ResolvedRootCommit>, LixError> {
    let constraints = vec![
        ScanConstraint {
            field: ScanField::FileId,
            operator: ScanOperator::Eq(Value::Text(version_ref_file_id().to_string())),
        },
        ScanConstraint {
            field: ScanField::PluginKey,
            operator: ScanOperator::Eq(Value::Text(version_ref_plugin_key().to_string())),
        },
    ];
    let required_columns = vec!["commit_id".to_string()];
    let rows = scan_rows_with_executor(
        executor,
        RawStorage::Untracked,
        version_ref_schema_key(),
        version_ref_storage_version_id(),
        &constraints,
        &required_columns,
    )
    .await?;
    let mut resolved = Vec::new();
    for row in rows {
        let Some(commit_id) = row.property_text("commit_id") else {
            continue;
        };
        if commit_id.is_empty() {
            continue;
        }
        resolved.push(ResolvedRootCommit {
            commit_id,
            version_id: row.entity_id().to_string(),
        });
    }
    Ok(resolved)
}

async fn load_version_ref_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<VersionRefRow>, LixError> {
    let Some(row) = load_exact_row_with_executor(
        executor,
        RawStorage::Untracked,
        version_ref_schema_key(),
        version_ref_storage_version_id(),
        version_id,
        Some(version_ref_file_id()),
    )
    .await?
    else {
        return Ok(None);
    };
    if row.plugin_key() != version_ref_plugin_key() {
        return Ok(None);
    }
    let commit_id = row.property_text("commit_id").ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("version ref row for '{version_id}' is missing commit_id"),
        )
    })?;
    Ok(Some(VersionRefRow {
        version_id: row.entity_id().to_string(),
        commit_id,
    }))
}

fn build_history_root_facts(
    request: RootCommitResolutionRequest<'_>,
    root_version_refs: Vec<ResolvedRootCommit>,
) -> HistoryRootFacts {
    let traversal = match (
        &request.root_scope,
        &request.version_scope,
        request.lineage_scope,
    ) {
        (RootCommitScope::AllRoots, RootVersionScope::Any, RootLineageScope::Standard) => {
            HistoryRootTraversal::AllRoots
        }
        (RootCommitScope::AllRoots, _, _) => {
            HistoryRootTraversal::ResolvedRootCommits(root_version_refs.clone())
        }
        (RootCommitScope::RequestedRoots(root_commit_ids), RootVersionScope::Any, _) => {
            HistoryRootTraversal::RequestedRootCommitIds(normalize_requested_root_ids(
                root_commit_ids,
            ))
        }
        (
            RootCommitScope::RequestedRoots(root_commit_ids),
            RootVersionScope::RequestedVersions(_),
            _,
        ) => HistoryRootTraversal::ResolvedRootCommits(filter_root_version_refs(
            &root_version_refs,
            root_commit_ids,
        )),
    };

    HistoryRootFacts {
        traversal,
        root_version_refs,
    }
}

fn filter_root_version_refs(
    root_version_refs: &[ResolvedRootCommit],
    requested_root_ids: &[String],
) -> Vec<ResolvedRootCommit> {
    let requested = requested_root_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    root_version_refs
        .iter()
        .filter(|row| requested.contains(&row.commit_id))
        .cloned()
        .collect()
}

fn normalize_requested_root_ids(root_commit_ids: &[String]) -> Vec<String> {
    root_commit_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_version_root_resolution_requires_active_version_id() {
        let error = scoped_root_version_ids(RootCommitResolutionRequest {
            lineage_scope: RootLineageScope::ActiveVersion,
            active_version_id: None,
            root_scope: RootCommitScope::AllRoots,
            version_scope: RootVersionScope::Any,
        })
        .expect_err("active-version root resolution should require an active version id");

        assert!(error.description.contains("active version id"));
    }

    #[test]
    fn active_version_root_resolution_scopes_to_the_active_version_default_root() {
        let facts = build_history_root_facts(
            RootCommitResolutionRequest {
                lineage_scope: RootLineageScope::ActiveVersion,
                active_version_id: Some("main"),
                root_scope: RootCommitScope::AllRoots,
                version_scope: RootVersionScope::Any,
            },
            vec![ResolvedRootCommit {
                commit_id: "commit-main".to_string(),
                version_id: "main".to_string(),
            }],
        );

        assert_eq!(
            facts.traversal,
            HistoryRootTraversal::ResolvedRootCommits(vec![ResolvedRootCommit {
                commit_id: "commit-main".to_string(),
                version_id: "main".to_string(),
            }])
        );
        assert_eq!(
            facts.root_version_refs,
            vec![ResolvedRootCommit {
                commit_id: "commit-main".to_string(),
                version_id: "main".to_string(),
            }]
        );
    }

    #[test]
    fn requested_version_root_resolution_adds_requested_version_filters() {
        let facts = build_history_root_facts(
            RootCommitResolutionRequest {
                lineage_scope: RootLineageScope::Standard,
                active_version_id: None,
                root_scope: RootCommitScope::AllRoots,
                version_scope: RootVersionScope::RequestedVersions(&[
                    "main".to_string(),
                    "feature".to_string(),
                ]),
            },
            vec![
                ResolvedRootCommit {
                    commit_id: "commit-main".to_string(),
                    version_id: "main".to_string(),
                },
                ResolvedRootCommit {
                    commit_id: "commit-feature".to_string(),
                    version_id: "feature".to_string(),
                },
            ],
        );

        assert_eq!(
            facts.traversal,
            HistoryRootTraversal::ResolvedRootCommits(vec![
                ResolvedRootCommit {
                    commit_id: "commit-main".to_string(),
                    version_id: "main".to_string(),
                },
                ResolvedRootCommit {
                    commit_id: "commit-feature".to_string(),
                    version_id: "feature".to_string(),
                },
            ])
        );
    }

    #[test]
    fn explicit_requested_roots_keep_requested_ids_and_version_ref_mapping() {
        let facts = build_history_root_facts(
            RootCommitResolutionRequest {
                lineage_scope: RootLineageScope::ActiveVersion,
                active_version_id: Some("main"),
                root_scope: RootCommitScope::RequestedRoots(&[
                    "commit-feature".to_string(),
                    "commit-main".to_string(),
                ]),
                version_scope: RootVersionScope::Any,
            },
            vec![ResolvedRootCommit {
                commit_id: "commit-main".to_string(),
                version_id: "main".to_string(),
            }],
        );

        assert_eq!(
            facts.traversal,
            HistoryRootTraversal::RequestedRootCommitIds(vec![
                "commit-feature".to_string(),
                "commit-main".to_string(),
            ])
        );
        assert_eq!(
            facts.root_version_refs,
            vec![ResolvedRootCommit {
                commit_id: "commit-main".to_string(),
                version_id: "main".to_string(),
            }]
        );
    }

    #[test]
    fn explicit_roots_with_requested_versions_intersect_with_scoped_version_refs() {
        let facts = build_history_root_facts(
            RootCommitResolutionRequest {
                lineage_scope: RootLineageScope::Standard,
                active_version_id: None,
                root_scope: RootCommitScope::RequestedRoots(&[
                    "commit-main".to_string(),
                    "commit-feature".to_string(),
                ]),
                version_scope: RootVersionScope::RequestedVersions(&["main".to_string()]),
            },
            vec![ResolvedRootCommit {
                commit_id: "commit-main".to_string(),
                version_id: "main".to_string(),
            }],
        );

        assert_eq!(
            facts.traversal,
            HistoryRootTraversal::ResolvedRootCommits(vec![ResolvedRootCommit {
                commit_id: "commit-main".to_string(),
                version_id: "main".to_string(),
            }])
        );
    }
}
