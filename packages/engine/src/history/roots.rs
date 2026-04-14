use crate::backend::QueryExecutor;
use crate::live_state::load_version_head_commit_map_with_executor;
use crate::{LixBackend, LixError};

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
    pub(crate) lineage_version_id: Option<&'a str>,
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

pub(crate) async fn load_history_root_commit_id_for_lineage_version_with_executor(
    executor: &mut dyn QueryExecutor,
    lineage_version_id: &str,
) -> Result<Option<String>, LixError> {
    let scoped_version_ids = vec![lineage_version_id.to_string()];
    Ok(
        load_scoped_root_version_refs_with_executor(executor, Some(&scoped_version_ids))
            .await?
            .into_iter()
            .find(|row| row.version_id == lineage_version_id)
            .map(|row| row.commit_id),
    )
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
            let lineage_version_id = request.lineage_version_id.ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "active-version root resolution requires a lineage version id",
                )
            })?;
            if let RootVersionScope::RequestedVersions(version_ids) = request.version_scope {
                if !version_ids.iter().any(|value| value == lineage_version_id) {
                    return Ok(Some(Vec::new()));
                }
            }
            Ok(Some(vec![lineage_version_id.to_string()]))
        }
    }
}

async fn load_scoped_root_version_refs_with_executor(
    executor: &mut dyn QueryExecutor,
    scoped_version_ids: Option<&[String]>,
) -> Result<Vec<ResolvedRootCommit>, LixError> {
    let version_head_map = load_version_head_commit_map_with_executor(executor)
        .await?
        .unwrap_or_default();
    let mut rows = match scoped_version_ids {
        Some(version_ids) => version_ids
            .iter()
            .filter_map(|version_id| {
                version_head_map
                    .get(version_id)
                    .map(|commit_id| ResolvedRootCommit {
                        commit_id: commit_id.clone(),
                        version_id: version_id.clone(),
                    })
            })
            .collect::<Vec<_>>(),
        None => version_head_map
            .into_iter()
            .map(|(version_id, commit_id)| ResolvedRootCommit {
                commit_id,
                version_id,
            })
            .collect::<Vec<_>>(),
    };
    rows.sort_by(|left, right| {
        left.commit_id
            .cmp(&right.commit_id)
            .then(left.version_id.cmp(&right.version_id))
    });
    Ok(rows)
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
