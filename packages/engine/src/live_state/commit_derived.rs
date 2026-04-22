//! Classification seam for commit-family query-serving surfaces.
//!
//! `lix_commit` is the base live row. Other commit-family query surfaces are
//! lazily derivable from visible `lix_commit` rows and should be expanded by
//! `live_state` rather than by SQL/DataFusion callers.

use std::collections::BTreeMap;

use crate::canonical::load_change;
use crate::live_state::storage_metadata::builtin_schema_storage_metadata;
use crate::live_state::store::LiveStateBackendRef;
use crate::live_state::{matches_constraints, LiveRow, LiveRowQuery, LiveRowSource};
use crate::schema::LixCommit;
use crate::LixError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitQuerySurface {
    BaseCommit,
    LazyDerived(CommitDerivedSurface),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitDerivedSurface {
    ChangeSet,
    ChangeSetElement,
    CommitEdge,
    ChangeAuthor,
}

impl CommitDerivedSurface {
    pub(crate) fn schema_key(self) -> &'static str {
        match self {
            Self::ChangeSet => "lix_change_set",
            Self::ChangeSetElement => "lix_change_set_element",
            Self::CommitEdge => "lix_commit_edge",
            Self::ChangeAuthor => "lix_change_author",
        }
    }
}

pub(crate) fn classify_commit_query_surface(schema_key: &str) -> Option<CommitQuerySurface> {
    match schema_key {
        "lix_commit" => Some(CommitQuerySurface::BaseCommit),
        "lix_change_set" => Some(CommitQuerySurface::LazyDerived(
            CommitDerivedSurface::ChangeSet,
        )),
        "lix_change_set_element" => Some(CommitQuerySurface::LazyDerived(
            CommitDerivedSurface::ChangeSetElement,
        )),
        "lix_commit_edge" => Some(CommitQuerySurface::LazyDerived(
            CommitDerivedSurface::CommitEdge,
        )),
        "lix_change_author" => Some(CommitQuerySurface::LazyDerived(
            CommitDerivedSurface::ChangeAuthor,
        )),
        _ => None,
    }
}

pub(crate) fn is_lazy_commit_derived_surface(schema_key: &str) -> bool {
    matches!(
        classify_commit_query_surface(schema_key),
        Some(CommitQuerySurface::LazyDerived(_))
    )
}

pub(crate) async fn scan_commit_derived_rows(
    backend: LiveStateBackendRef<'_>,
    request: &LiveRowQuery,
    scan_base_commit_rows: impl for<'a> Fn(
        LiveStateBackendRef<'a>,
        &LiveRowQuery,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<LiveRow>, LixError>> + 'a>,
    >,
) -> Result<Vec<LiveRow>, LixError> {
    let Some(CommitQuerySurface::LazyDerived(surface)) =
        classify_commit_query_surface(&request.schema_key)
    else {
        return Ok(Vec::new());
    };

    if request.source == LiveRowSource::Untracked {
        return Ok(Vec::new());
    }

    let base_request = LiveRowQuery {
        schema_key: "lix_commit".to_string(),
        version_id: request.version_id.clone(),
        source: request.source,
        constraints: Vec::new(),
        include_tombstones: request.include_tombstones,
    };
    let commit_rows = scan_base_commit_rows(backend, &base_request).await?;
    if commit_rows.is_empty() {
        return Ok(Vec::new());
    }

    let member_changes = load_member_changes_for_commit_rows(backend, &commit_rows).await?;
    let mut rows =
        expand_commit_rows_to_lazy_derived_surface(surface, &commit_rows, &member_changes)?
            .into_iter()
            .filter(|row| {
                matches_constraints(
                    &row.entity_id,
                    row.file_id.as_deref(),
                    row.plugin_key.as_deref(),
                    &row.schema_version,
                    &request.constraints,
                )
            })
            .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.entity_id
            .cmp(&right.entity_id)
            .then_with(|| left.file_id.cmp(&right.file_id))
            .then_with(|| {
                left.snapshot_content
                    .is_none()
                    .cmp(&right.snapshot_content.is_none())
            })
    });
    Ok(rows)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitDerivedMemberChange {
    pub(crate) id: String,
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) created_at: Option<String>,
    pub(crate) updated_at: Option<String>,
}

pub(crate) fn expand_commit_rows_to_lazy_derived_surface(
    surface: CommitDerivedSurface,
    commit_rows: &[LiveRow],
    member_changes: &BTreeMap<String, CommitDerivedMemberChange>,
) -> Result<Vec<LiveRow>, LixError> {
    let mut rows = Vec::new();
    for commit_row in commit_rows {
        rows.extend(expand_commit_row_to_lazy_derived_surface(
            surface,
            commit_row,
            member_changes,
        )?);
    }
    Ok(rows)
}

fn expand_commit_row_to_lazy_derived_surface(
    surface: CommitDerivedSurface,
    commit_row: &LiveRow,
    member_changes: &BTreeMap<String, CommitDerivedMemberChange>,
) -> Result<Vec<LiveRow>, LixError> {
    if commit_row.schema_key != "lix_commit" {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "commit-derived expansion expected lix_commit rows, got '{}'",
                commit_row.schema_key
            ),
        ));
    }

    let Some(snapshot_content) = commit_row.snapshot_content.as_deref() else {
        return Ok(Vec::new());
    };

    let commit: LixCommit = serde_json::from_str(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("invalid lix_commit snapshot_content JSON: {error}"),
        )
    })?;

    let storage = builtin_schema_storage_metadata(surface.schema_key()).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "builtin storage metadata missing for commit-derived surface '{}'",
                surface.schema_key()
            ),
        )
    })?;

    match surface {
        CommitDerivedSurface::ChangeSet => {
            let Some(change_set_id) = commit
                .change_set_id
                .as_ref()
                .filter(|value| !value.is_empty())
            else {
                return Ok(Vec::new());
            };

            Ok(vec![LiveRow {
                entity_id: change_set_id.clone(),
                file_id: storage.file_id.clone(),
                schema_key: storage.schema_key,
                schema_version: storage.schema_version,
                version_id: commit_row.version_id.clone(),
                plugin_key: storage.plugin_key,
                metadata: commit_row.metadata.clone(),
                change_id: commit_row.change_id.clone(),
                global: commit_row.global,
                untracked: commit_row.untracked,
                created_at: commit_row.created_at.clone(),
                updated_at: commit_row.updated_at.clone(),
                snapshot_content: Some(json_string(serde_json::json!({
                    "id": change_set_id,
                }))?),
            }])
        }
        CommitDerivedSurface::ChangeSetElement => {
            let Some(change_set_id) = commit
                .change_set_id
                .as_ref()
                .filter(|value| !value.is_empty())
            else {
                return Ok(Vec::new());
            };

            let mut rows = Vec::new();
            for change_id in &commit.change_ids {
                let Some(change) = member_changes.get(change_id) else {
                    continue;
                };

                rows.push(LiveRow {
                    entity_id: format!("{}~{}", change_set_id, change.id),
                    file_id: storage.file_id.clone(),
                    schema_key: storage.schema_key.clone(),
                    schema_version: storage.schema_version.clone(),
                    version_id: commit_row.version_id.clone(),
                    plugin_key: storage.plugin_key.clone(),
                    metadata: change.metadata.clone(),
                    change_id: Some(change.id.clone()),
                    global: commit_row.global,
                    untracked: commit_row.untracked,
                    created_at: change.created_at.clone(),
                    updated_at: change
                        .updated_at
                        .clone()
                        .or_else(|| change.created_at.clone()),
                    snapshot_content: Some(json_string(serde_json::json!({
                        "change_set_id": change_set_id,
                        "change_id": change.id,
                        "entity_id": change.entity_id,
                        "schema_key": change.schema_key,
                        "file_id": change.file_id,
                    }))?),
                });
            }
            Ok(rows)
        }
        CommitDerivedSurface::CommitEdge => {
            let mut rows = Vec::new();
            for parent_id in &commit.parent_commit_ids {
                if parent_id.is_empty() {
                    continue;
                }

                rows.push(LiveRow {
                    entity_id: format!("{}~{}", parent_id, commit.id),
                    file_id: storage.file_id.clone(),
                    schema_key: storage.schema_key.clone(),
                    schema_version: storage.schema_version.clone(),
                    version_id: commit_row.version_id.clone(),
                    plugin_key: storage.plugin_key.clone(),
                    metadata: commit_row.metadata.clone(),
                    change_id: commit_row.change_id.clone(),
                    global: commit_row.global,
                    untracked: commit_row.untracked,
                    created_at: commit_row.created_at.clone(),
                    updated_at: commit_row.updated_at.clone(),
                    snapshot_content: Some(json_string(serde_json::json!({
                        "parent_id": parent_id,
                        "child_id": commit.id,
                    }))?),
                });
            }
            Ok(rows)
        }
        CommitDerivedSurface::ChangeAuthor => {
            let mut rows = Vec::new();
            for change_id in &commit.change_ids {
                let Some(change) = member_changes.get(change_id) else {
                    continue;
                };

                for account_id in &commit.author_account_ids {
                    if account_id.is_empty() {
                        continue;
                    }

                    rows.push(LiveRow {
                        entity_id: format!("{}~{}", change.id, account_id),
                        file_id: storage.file_id.clone(),
                        schema_key: storage.schema_key.clone(),
                        schema_version: storage.schema_version.clone(),
                        version_id: commit_row.version_id.clone(),
                        plugin_key: storage.plugin_key.clone(),
                        metadata: change.metadata.clone(),
                        change_id: Some(change.id.clone()),
                        global: commit_row.global,
                        untracked: commit_row.untracked,
                        created_at: change.created_at.clone(),
                        updated_at: change
                            .updated_at
                            .clone()
                            .or_else(|| change.created_at.clone()),
                        snapshot_content: Some(json_string(serde_json::json!({
                            "change_id": change.id,
                            "account_id": account_id,
                        }))?),
                    });
                }
            }
            Ok(rows)
        }
    }
}

fn json_string(value: serde_json::Value) -> Result<String, LixError> {
    serde_json::to_string(&value).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to serialize commit-derived snapshot_content: {error}"),
        )
    })
}

async fn load_member_changes_for_commit_rows(
    backend: LiveStateBackendRef<'_>,
    commit_rows: &[LiveRow],
) -> Result<BTreeMap<String, CommitDerivedMemberChange>, LixError> {
    let mut member_changes = BTreeMap::new();
    let mut executor = backend;

    for commit_row in commit_rows {
        let Some(snapshot_content) = commit_row.snapshot_content.as_deref() else {
            continue;
        };
        let commit: LixCommit = serde_json::from_str(snapshot_content).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("invalid lix_commit snapshot_content JSON: {error}"),
            )
        })?;

        for change_id in commit.change_ids {
            if member_changes.contains_key(&change_id) {
                continue;
            }
            let Some(change) = load_change(&mut executor, &change_id).await? else {
                continue;
            };
            member_changes.insert(
                change_id,
                CommitDerivedMemberChange {
                    id: change.id,
                    entity_id: change.entity_id,
                    schema_key: change.schema_key,
                    file_id: change.file_id,
                    metadata: change.metadata,
                    created_at: Some(change.created_at.clone()),
                    updated_at: Some(change.created_at),
                },
            );
        }
    }

    Ok(member_changes)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::pin::Pin;

    use super::{
        classify_commit_query_surface, expand_commit_rows_to_lazy_derived_surface,
        is_lazy_commit_derived_surface, scan_commit_derived_rows, CommitDerivedMemberChange,
        CommitDerivedSurface, CommitQuerySurface,
    };
    use crate::live_state::LiveRow;
    use crate::live_state::{LiveRowQuery, LiveRowSource, ScanConstraint, ScanField, ScanOperator};
    use crate::schema::LixCommit;
    use crate::test_support::{
        init_test_backend_core, seed_canonical_change_row, CanonicalChangeSeed, TestSqliteBackend,
    };
    use crate::{LixError, Value};

    #[test]
    fn classifies_base_commit_surface() {
        assert_eq!(
            classify_commit_query_surface("lix_commit"),
            Some(CommitQuerySurface::BaseCommit)
        );
        assert!(!is_lazy_commit_derived_surface("lix_commit"));
    }

    #[test]
    fn classifies_all_lazy_derived_commit_surfaces() {
        assert_eq!(
            classify_commit_query_surface("lix_change_set"),
            Some(CommitQuerySurface::LazyDerived(
                CommitDerivedSurface::ChangeSet
            ))
        );
        assert_eq!(
            classify_commit_query_surface("lix_change_set_element"),
            Some(CommitQuerySurface::LazyDerived(
                CommitDerivedSurface::ChangeSetElement
            ))
        );
        assert_eq!(
            classify_commit_query_surface("lix_commit_edge"),
            Some(CommitQuerySurface::LazyDerived(
                CommitDerivedSurface::CommitEdge
            ))
        );
        assert_eq!(
            classify_commit_query_surface("lix_change_author"),
            Some(CommitQuerySurface::LazyDerived(
                CommitDerivedSurface::ChangeAuthor
            ))
        );

        assert!(is_lazy_commit_derived_surface("lix_change_set"));
        assert!(is_lazy_commit_derived_surface("lix_change_set_element"));
        assert!(is_lazy_commit_derived_surface("lix_commit_edge"));
        assert!(is_lazy_commit_derived_surface("lix_change_author"));
    }

    #[test]
    fn ignores_non_commit_family_surfaces() {
        assert_eq!(classify_commit_query_surface("lix_key_value"), None);
        assert!(!is_lazy_commit_derived_surface("lix_key_value"));
    }

    #[test]
    fn derived_surface_schema_keys_round_trip() {
        for surface in [
            CommitDerivedSurface::ChangeSet,
            CommitDerivedSurface::ChangeSetElement,
            CommitDerivedSurface::CommitEdge,
            CommitDerivedSurface::ChangeAuthor,
        ] {
            assert_eq!(
                classify_commit_query_surface(surface.schema_key()),
                Some(CommitQuerySurface::LazyDerived(surface))
            );
        }
    }

    fn commit_row(snapshot: &LixCommit) -> LiveRow {
        LiveRow {
            entity_id: snapshot.id.clone(),
            file_id: None,
            schema_key: "lix_commit".to_string(),
            schema_version: "1".to_string(),
            version_id: "global".to_string(),
            plugin_key: None,
            metadata: Some("{\"meta\":true}".to_string()),
            change_id: Some("change-commit".to_string()),
            global: false,
            untracked: false,
            created_at: Some("2026-01-01T00:00:00Z".to_string()),
            updated_at: Some("2026-01-01T00:00:00Z".to_string()),
            snapshot_content: Some(
                serde_json::to_string(snapshot).expect("commit snapshot should serialize"),
            ),
        }
    }

    fn member_change(id: &str, entity_id: &str) -> CommitDerivedMemberChange {
        CommitDerivedMemberChange {
            id: id.to_string(),
            entity_id: entity_id.to_string(),
            schema_key: "test_schema".to_string(),
            file_id: Some("file-a".to_string()),
            metadata: Some("{\"member\":true}".to_string()),
            created_at: Some("2026-01-01T00:01:00Z".to_string()),
            updated_at: Some("2026-01-01T00:01:00Z".to_string()),
        }
    }

    fn parse_json(text: &str) -> serde_json::Value {
        serde_json::from_str(text).expect("snapshot_content should be valid json")
    }

    #[test]
    fn expands_change_set_rows_from_commit() {
        let commit = commit_row(&LixCommit {
            id: "commit-1".to_string(),
            change_set_id: Some("cs-1".to_string()),
            change_ids: vec![],
            author_account_ids: vec![],
            parent_commit_ids: vec![],
        });

        let rows = expand_commit_rows_to_lazy_derived_surface(
            CommitDerivedSurface::ChangeSet,
            &[commit],
            &BTreeMap::new(),
        )
        .expect("change_set expansion should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].schema_key, "lix_change_set");
        assert_eq!(rows[0].entity_id, "cs-1");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"id\":\"cs-1\"}")
        );
    }

    #[test]
    fn expands_change_set_element_rows_from_commit_and_member_changes() {
        let commit = commit_row(&LixCommit {
            id: "commit-1".to_string(),
            change_set_id: Some("cs-1".to_string()),
            change_ids: vec!["chg-1".to_string(), "chg-2".to_string()],
            author_account_ids: vec![],
            parent_commit_ids: vec![],
        });
        let changes = BTreeMap::from([
            ("chg-1".to_string(), member_change("chg-1", "entity-a")),
            ("chg-2".to_string(), member_change("chg-2", "entity-b")),
        ]);

        let rows = expand_commit_rows_to_lazy_derived_surface(
            CommitDerivedSurface::ChangeSetElement,
            &[commit],
            &changes,
        )
        .expect("change_set_element expansion should succeed");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].schema_key, "lix_change_set_element");
        assert_eq!(rows[0].entity_id, "cs-1~chg-1");
        assert_eq!(rows[0].change_id.as_deref(), Some("chg-1"));
        assert_eq!(rows[0].created_at.as_deref(), Some("2026-01-01T00:01:00Z"));
        assert_eq!(
            rows[0].snapshot_content.as_deref().map(parse_json),
            Some(serde_json::json!({
                "change_set_id": "cs-1",
                "change_id": "chg-1",
                "entity_id": "entity-a",
                "schema_key": "test_schema",
                "file_id": "file-a",
            }))
        );
    }

    #[test]
    fn expands_commit_edge_rows_from_commit_parents() {
        let commit = commit_row(&LixCommit {
            id: "commit-child".to_string(),
            change_set_id: Some("cs-1".to_string()),
            change_ids: vec![],
            author_account_ids: vec![],
            parent_commit_ids: vec!["commit-a".to_string(), "commit-b".to_string()],
        });

        let rows = expand_commit_rows_to_lazy_derived_surface(
            CommitDerivedSurface::CommitEdge,
            &[commit],
            &BTreeMap::new(),
        )
        .expect("commit_edge expansion should succeed");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].schema_key, "lix_commit_edge");
        assert_eq!(rows[0].entity_id, "commit-a~commit-child");
        assert_eq!(
            rows[0].snapshot_content.as_deref().map(parse_json),
            Some(serde_json::json!({
                "parent_id": "commit-a",
                "child_id": "commit-child",
            }))
        );
    }

    #[test]
    fn expands_change_author_rows_from_commit_authors_and_member_changes() {
        let commit = commit_row(&LixCommit {
            id: "commit-1".to_string(),
            change_set_id: Some("cs-1".to_string()),
            change_ids: vec!["chg-1".to_string()],
            author_account_ids: vec!["acct-1".to_string(), "acct-2".to_string()],
            parent_commit_ids: vec![],
        });
        let changes = BTreeMap::from([("chg-1".to_string(), member_change("chg-1", "entity-a"))]);

        let rows = expand_commit_rows_to_lazy_derived_surface(
            CommitDerivedSurface::ChangeAuthor,
            &[commit],
            &changes,
        )
        .expect("change_author expansion should succeed");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].schema_key, "lix_change_author");
        assert_eq!(rows[0].entity_id, "chg-1~acct-1");
        assert_eq!(
            rows[0].snapshot_content.as_deref().map(parse_json),
            Some(serde_json::json!({
                "change_id": "chg-1",
                "account_id": "acct-1",
            }))
        );
    }

    #[tokio::test]
    async fn scan_commit_derived_rows_expands_and_filters_by_constraints() {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");
        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "chg-1",
                entity_id: "entity-a",
                schema_key: "test_schema",
                schema_version: "1",
                file_id: Some("file-a"),
                plugin_key: None,
                snapshot_id: "snapshot-1",
                snapshot_content: Some(r#"{"key":"a"}"#),
                metadata: Some(r#"{"member":true}"#),
                created_at: "2026-01-01T00:01:00Z",
            },
        )
        .await
        .expect("canonical change should seed");

        let commit_rows = vec![commit_row(&LixCommit {
            id: "commit-1".to_string(),
            change_set_id: Some("cs-1".to_string()),
            change_ids: vec!["chg-1".to_string()],
            author_account_ids: vec![],
            parent_commit_ids: vec![],
        })];

        let rows = scan_commit_derived_rows(
            &backend,
            &LiveRowQuery {
                schema_key: "lix_change_set_element".to_string(),
                version_id: "global".to_string(),
                source: LiveRowSource::Effective,
                constraints: vec![ScanConstraint {
                    field: ScanField::EntityId,
                    operator: ScanOperator::Eq(Value::Text("cs-1~chg-1".to_string())),
                }],
                include_tombstones: false,
            },
            |_backend, _request| {
                let commit_rows = commit_rows.clone();
                Box::pin(async move { Ok(commit_rows) })
                    as Pin<Box<dyn std::future::Future<Output = Result<Vec<LiveRow>, LixError>>>>
            },
        )
        .await
        .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].schema_key, "lix_change_set_element");
        assert_eq!(rows[0].entity_id, "cs-1~chg-1");
        assert_eq!(rows[0].change_id.as_deref(), Some("chg-1"));
    }
}
