use std::collections::{BTreeMap, BTreeSet};

use crate::backend::QueryExecutor;
use crate::canonical::{load_exact_row_at_commit, CanonicalStateIdentity, CanonicalStateRow};
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
    executor: &mut dyn QueryExecutor,
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

pub(crate) async fn load_exact_canonical_row_at_version_head_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
    identity: &CanonicalStateIdentity,
) -> Result<Option<CanonicalStateRow>, LixError> {
    let Some(head_commit_id) =
        load_local_version_head_commit_id_with_executor(executor, version_id).await?
    else {
        return Ok(None);
    };

    load_exact_row_at_commit(executor, &head_commit_id, identity).await
}
