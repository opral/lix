//! Replica-local root resolution over committed version-head state.
//!
//! This module maps requested lineage scopes and version scopes onto the local
//! committed heads selected by `version/heads.rs`.

use crate::backend::QueryExecutor;
use crate::{LixBackend, LixError};

use super::heads::{
    load_all_committed_version_refs_with_executor, load_committed_version_ref_with_executor,
};

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
                if let Some(row) =
                    load_committed_version_ref_with_executor(executor, version_id).await?
                {
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
    Ok(load_all_committed_version_refs_with_executor(executor)
        .await?
        .into_iter()
        .map(|row| ResolvedRootCommit {
            commit_id: row.commit_id,
            version_id: row.version_id,
        })
        .collect())
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
