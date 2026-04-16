use std::collections::BTreeMap;

use crate::canonical::{
    load_exact_committed_change_from_commit_with_executor, ExactCommittedStateRowRequest,
};
use crate::live_state::{load_exact_untracked_row_with_executor, ExactUntrackedRowRequest};
use crate::transaction::PendingOverlay;
use crate::version::{
    parse_version_descriptor_snapshot, parse_version_ref_snapshot, version_descriptor_file_id,
    version_descriptor_plugin_key, version_descriptor_schema_key,
    version_descriptor_schema_version, version_ref_schema_key, version_ref_storage_version_id,
    GLOBAL_VERSION_ID,
};
use crate::{LixBackend, LixError, NullableKeyFilter, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedVersionAdminState {
    pub(crate) version_id: String,
    pub(crate) name: String,
    pub(crate) hidden: bool,
    pub(crate) descriptor_change_id: Option<String>,
    pub(crate) head_commit_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VersionDescriptorRow {
    version_id: String,
    name: String,
    hidden: bool,
    change_id: Option<String>,
}

pub(crate) async fn load_version_admin_state_with_backend(
    backend: &dyn LixBackend,
    pending_overlay: Option<&dyn PendingOverlay>,
    version_id: &str,
) -> Result<Option<ResolvedVersionAdminState>, LixError> {
    let mut executor = backend;
    let Some(descriptor) =
        load_version_descriptor_with_pending_overlay(&mut executor, pending_overlay, version_id)
            .await?
    else {
        return Ok(None);
    };
    let head_commit_id = load_version_head_commit_id_with_pending_overlay(
        &mut executor,
        pending_overlay,
        version_id,
    )
    .await?;

    Ok(Some(ResolvedVersionAdminState {
        version_id: descriptor.version_id,
        name: descriptor.name,
        hidden: descriptor.hidden,
        descriptor_change_id: descriptor.change_id,
        head_commit_id,
    }))
}

async fn load_version_head_commit_id_with_pending_overlay(
    executor: &mut dyn crate::backend::QueryExecutor,
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

async fn load_version_descriptor_with_pending_overlay(
    executor: &mut dyn crate::backend::QueryExecutor,
    pending_overlay: Option<&dyn PendingOverlay>,
    version_id: &str,
) -> Result<Option<VersionDescriptorRow>, LixError> {
    if let Some(pending) = pending_version_descriptor_row(pending_overlay, version_id)? {
        return Ok(pending);
    }

    let Some(global_head_commit_id) =
        load_version_head_commit_id_with_pending_overlay(executor, None, GLOBAL_VERSION_ID).await?
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
