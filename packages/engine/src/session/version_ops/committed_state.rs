use std::collections::{BTreeMap, BTreeSet};

use crate::backend::QueryExecutor;
use crate::canonical::{load_exact_row_at_commit, CanonicalStateIdentity, CanonicalStateRow};
use crate::live_state::{load_exact_untracked_row_with_executor, ExactUntrackedRowRequest};
use crate::transaction::PendingOverlay;
use crate::version::parse_version_ref_snapshot;
use crate::{LixError, NullableKeyFilter, VersionId};

use crate::version::{version_ref_schema_key, version_ref_storage_version_id};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionSnapshot {
    pub id: VersionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionInfo {
    pub parent_commit_ids: Vec<String>,
    pub snapshot: VersionSnapshot,
}

pub(crate) async fn load_version_head_commit_id_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    load_version_head_commit_id_with_pending_overlay(executor, None, version_id).await
}

pub(crate) async fn load_version_head_commit_id_with_pending_overlay(
    executor: &mut dyn QueryExecutor,
    pending_overlay: Option<&dyn PendingOverlay>,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    if let Some(pending) = pending_version_head_commit_id(pending_overlay, version_id)? {
        return Ok(pending);
    }

    let Some(row) = load_exact_untracked_row_with_executor(
        executor,
        &ExactUntrackedRowRequest {
            schema_key: version_ref_schema_key().to_string(),
            version_id: version_ref_storage_version_id().to_string(),
            entity_id: version_id.to_string(),
            file_id: NullableKeyFilter::Null,
        },
    )
    .await?
    else {
        return Ok(None);
    };

    let Some(commit_id) = row
        .property_text("commit_id")
        .filter(|value| !value.trim().is_empty())
    else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("local version head for '{version_id}' has empty commit_id"),
        ));
    };

    Ok(Some(commit_id))
}

fn pending_version_head_commit_id(
    pending_overlay: Option<&dyn PendingOverlay>,
    version_id: &str,
) -> Result<Option<Option<String>>, LixError> {
    let Some(row) = pending_overlay.and_then(|overlay| {
        overlay
            .visible_semantic_rows(true, version_ref_schema_key())
            .into_iter()
            .find(|row| {
                row.entity_id == version_id
                    && row.version_id == version_ref_storage_version_id()
                    && row.file_id.is_none()
                    && row.plugin_key.is_none()
            })
    }) else {
        return Ok(None);
    };

    if row.tombstone {
        return Ok(Some(None));
    }

    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("pending local version head for '{version_id}' is missing snapshot_content"),
        ));
    };

    let snapshot = parse_version_ref_snapshot(snapshot_content)?;
    if snapshot.commit_id.trim().is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("pending local version head for '{version_id}' has empty commit_id"),
        ));
    }

    Ok(Some(Some(snapshot.commit_id)))
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
            load_version_head_commit_id_with_executor(executor, version_id).await?
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
        load_version_head_commit_id_with_executor(executor, version_id).await?
    else {
        return Ok(None);
    };

    load_exact_row_at_commit(executor, &head_commit_id, identity).await
}
