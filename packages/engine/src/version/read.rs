use std::collections::{BTreeMap, BTreeSet};

use crate::canonical::{
    load_exact_committed_change_from_commit_with_executor, load_exact_row_at_commit,
    CanonicalStateIdentity, CanonicalStateRow, ExactCommittedStateRowRequest,
};
use crate::live_state::{
    load_exact_untracked_row_with_executor, load_version_head_commit_map_with_executor,
    ExactUntrackedRowRequest,
};
use crate::transaction::PendingOverlay;
use crate::{LixBackend, LixError, NullableKeyFilter, QueryExecutor, Value, VersionId};

use super::{
    parse_version_descriptor_snapshot, parse_version_ref_snapshot, version_descriptor_file_id,
    version_descriptor_plugin_key, version_descriptor_schema_key,
    version_descriptor_schema_version, version_ref_schema_key, version_ref_storage_version_id,
    GLOBAL_VERSION_ID,
};

pub(crate) type VersionReadRef<'a> = &'a mut (dyn QueryExecutor + 'a);
pub(crate) type VersionBackendRef<'a> = &'a dyn LixBackend;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionDescriptorRow {
    pub(crate) version_id: String,
    pub(crate) name: String,
    pub(crate) hidden: bool,
    pub(crate) change_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionHeadFact {
    pub(crate) version_id: String,
    pub(crate) head_commit_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionSnapshot {
    pub id: VersionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionInfo {
    pub parent_commit_ids: Vec<String>,
    pub snapshot: VersionSnapshot,
}

pub(crate) async fn load_version_descriptor_with_backend(
    backend: VersionBackendRef<'_>,
    version_id: &str,
) -> Result<Option<VersionDescriptorRow>, LixError> {
    let mut executor = backend;
    load_version_descriptor_with_executor(&mut executor, version_id).await
}

pub(crate) async fn load_version_descriptor_with_executor(
    executor: VersionReadRef<'_>,
    version_id: &str,
) -> Result<Option<VersionDescriptorRow>, LixError> {
    load_version_descriptor_with_pending_overlay(executor, None, version_id).await
}

pub(crate) async fn load_version_descriptor_with_pending_overlay(
    executor: VersionReadRef<'_>,
    pending_overlay: Option<&dyn PendingOverlay>,
    version_id: &str,
) -> Result<Option<VersionDescriptorRow>, LixError> {
    if let Some(pending) = pending_version_descriptor_row(pending_overlay, version_id)? {
        return Ok(pending);
    }

    let Some(global_head_commit_id) =
        load_version_head_commit_id_with_executor(executor, GLOBAL_VERSION_ID).await?
    else {
        return Ok(None);
    };
    let row = load_exact_committed_change_from_commit_with_executor(
        executor,
        &global_head_commit_id,
        &ExactCommittedStateRowRequest {
            entity_id: version_id.to_string(),
            schema_key: version_descriptor_schema_key().to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            exact_filters: BTreeMap::from([
                (
                    "file_id".to_string(),
                    version_descriptor_file_id()
                        .map(|value| Value::Text(value.to_string()))
                        .unwrap_or(Value::Null),
                ),
                (
                    "plugin_key".to_string(),
                    version_descriptor_plugin_key()
                        .map(|value| Value::Text(value.to_string()))
                        .unwrap_or(Value::Null),
                ),
                (
                    "schema_version".to_string(),
                    Value::Text(version_descriptor_schema_version().to_string()),
                ),
            ]),
        },
    )
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
        return Ok(None);
    };
    Ok(Some(parse_descriptor_row(snapshot_content, Some(row.id))?))
}

pub(crate) async fn load_all_version_descriptors_with_executor(
    executor: VersionReadRef<'_>,
) -> Result<Vec<VersionDescriptorRow>, LixError> {
    let mut descriptors = Vec::new();
    let Some(version_heads) = load_version_head_commit_map_with_executor(executor).await? else {
        return Ok(descriptors);
    };
    for version_id in version_heads.keys() {
        if let Some(descriptor) =
            load_version_descriptor_with_executor(executor, version_id).await?
        {
            descriptors.push(descriptor);
        }
    }
    descriptors.sort_by(|left, right| left.version_id.cmp(&right.version_id));
    Ok(descriptors)
}

pub(crate) async fn load_checkpoint_version_heads_with_executor(
    executor: VersionReadRef<'_>,
) -> Result<Vec<VersionHeadFact>, LixError> {
    let mut heads = Vec::new();

    let Some(global_head_commit_id) =
        load_version_head_commit_id_with_executor(executor, GLOBAL_VERSION_ID).await?
    else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "global version is missing a committed head",
        ));
    };
    heads.push(VersionHeadFact {
        version_id: GLOBAL_VERSION_ID.to_string(),
        head_commit_id: global_head_commit_id,
    });

    for descriptor in load_all_version_descriptors_with_executor(executor).await? {
        if descriptor.version_id == GLOBAL_VERSION_ID {
            continue;
        }
        let Some(head_commit_id) =
            load_version_head_commit_id_with_executor(executor, &descriptor.version_id).await?
        else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "version '{}' is missing a committed head",
                    descriptor.version_id
                ),
            ));
        };
        heads.push(VersionHeadFact {
            version_id: descriptor.version_id,
            head_commit_id,
        });
    }

    heads.sort_by(|left, right| left.version_id.cmp(&right.version_id));
    Ok(heads)
}

pub(crate) async fn version_exists_with_backend(
    backend: VersionBackendRef<'_>,
    version_id: &str,
) -> Result<bool, LixError> {
    Ok(load_version_descriptor_with_backend(backend, version_id)
        .await?
        .is_some())
}

pub(crate) async fn version_exists_with_executor(
    executor: VersionReadRef<'_>,
    version_id: &str,
) -> Result<bool, LixError> {
    if load_version_descriptor_with_executor(executor, version_id)
        .await?
        .is_some()
    {
        return Ok(true);
    }

    Ok(
        load_version_head_commit_id_with_executor(executor, version_id)
            .await?
            .is_some(),
    )
}

pub(crate) async fn load_version_head_commit_id_with_executor(
    executor: VersionReadRef<'_>,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    load_version_head_commit_id_with_pending_overlay(executor, None, version_id).await
}

pub(crate) async fn load_version_head_commit_id_with_pending_overlay(
    executor: VersionReadRef<'_>,
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
            untracked: true,
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

pub(crate) async fn load_version_info_for_versions(
    executor: VersionReadRef<'_>,
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
    executor: VersionReadRef<'_>,
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

fn pending_version_descriptor_row(
    pending_overlay: Option<&dyn PendingOverlay>,
    version_id: &str,
) -> Result<Option<Option<VersionDescriptorRow>>, LixError> {
    let Some(row) = pending_overlay.and_then(|overlay| {
        overlay
            .visible_semantic_rows(false, version_descriptor_schema_key())
            .into_iter()
            .find(|row| {
                row.entity_id == version_id
                    && row.version_id == GLOBAL_VERSION_ID
                    && row.file_id.is_none()
                    && row.plugin_key.is_none()
                    && row.schema_version == version_descriptor_schema_version()
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
            format!("pending version descriptor for '{version_id}' is missing snapshot_content"),
        ));
    };

    Ok(Some(Some(parse_descriptor_row(
        snapshot_content,
        row.change_id.clone(),
    )?)))
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

fn parse_descriptor_row(
    snapshot_content: &str,
    change_id: Option<String>,
) -> Result<VersionDescriptorRow, LixError> {
    let snapshot = parse_version_descriptor_snapshot(snapshot_content)?;
    Ok(VersionDescriptorRow {
        version_id: snapshot.id,
        name: snapshot.name.unwrap_or_default(),
        hidden: snapshot.hidden,
        change_id,
    })
}
