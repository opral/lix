use std::collections::{BTreeMap, BTreeSet};

use crate::canonical::read::{
    load_exact_committed_state_row_from_commit_with_executor, CommitQueryExecutor,
    ExactCommittedStateRow, ExactCommittedStateRowRequest,
};
use crate::version_state::load_local_version_head_commit_id_with_executor;
use crate::{LixError, VersionId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionSnapshot {
    pub id: VersionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionInfo {
    pub parent_commit_ids: Vec<String>,
    pub snapshot: VersionSnapshot,
}

pub(crate) async fn load_version_info_for_versions(
    executor: &mut dyn CommitQueryExecutor,
    version_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, VersionInfo>, LixError> {
    let mut versions = BTreeMap::new();
    if version_ids.is_empty() {
        return Ok(versions);
    }

    for version_id in version_ids {
        versions.insert(
            version_id.clone(),
            VersionInfo {
                parent_commit_ids: Vec::new(),
                snapshot: VersionSnapshot {
                    id: VersionId::new(version_id.clone())?,
                },
            },
        );
    }
    for version_id in version_ids {
        if let Some(commit_id) =
            load_local_version_head_commit_id_with_executor(executor, version_id).await?
        {
            versions.insert(
                version_id.clone(),
                VersionInfo {
                    parent_commit_ids: vec![commit_id],
                    snapshot: VersionSnapshot {
                        id: VersionId::new(version_id.clone())?,
                    },
                },
            );
        }
    }

    Ok(versions)
}

pub(crate) async fn load_exact_committed_state_row_at_version_head_with_executor(
    executor: &mut dyn CommitQueryExecutor,
    request: &ExactCommittedStateRowRequest,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    let Some(head_commit_id) =
        load_local_version_head_commit_id_with_executor(executor, &request.version_id).await?
    else {
        return Ok(None);
    };

    load_exact_committed_state_row_from_commit_with_executor(executor, &head_commit_id, request)
        .await
}
