use crate::catalog::load_version_surface_row_with_backend;
use crate::live_state::load_version_head_commit_map_with_executor;
use crate::live_state::tracked::{
    scan_rows_with_backend as scan_tracked_rows_with_backend, TrackedScanRequest,
};
use crate::transaction::PendingOverlay;
use crate::version::{
    parse_version_descriptor_snapshot, parse_version_ref_snapshot, version_descriptor_schema_key,
    version_descriptor_schema_version, version_ref_schema_key, version_ref_storage_version_id,
    GLOBAL_VERSION_ID,
};
use crate::{LixBackend, LixError};

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
    let descriptor = match pending_version_descriptor_row(pending_overlay, version_id)? {
        Some(descriptor) => descriptor,
        None => load_effective_version_descriptor_with_backend(backend, version_id).await?,
    };
    let head_commit_id = match pending_version_head_commit_id(pending_overlay, version_id)? {
        Some(head_commit_id) => head_commit_id,
        None => load_effective_version_head_commit_id_with_backend(backend, version_id).await?,
    };

    match (descriptor, head_commit_id) {
        (Some(descriptor), head_commit_id) => Ok(Some(ResolvedVersionAdminState {
            version_id: descriptor.version_id,
            name: descriptor.name,
            hidden: descriptor.hidden,
            descriptor_change_id: descriptor.change_id,
            head_commit_id,
        })),
        (None, Some(commit_id)) => Ok(Some(ResolvedVersionAdminState {
            version_id: version_id.to_string(),
            name: version_id.to_string(),
            hidden: false,
            descriptor_change_id: None,
            head_commit_id: Some(commit_id),
        })),
        (None, None) => {
            let Some(row) = load_version_surface_row_with_backend(backend, version_id).await?
            else {
                return Ok(None);
            };
            Ok(Some(ResolvedVersionAdminState {
                version_id: row.id,
                name: row.name,
                hidden: row.hidden,
                descriptor_change_id: None,
                head_commit_id: Some(row.commit_id),
            }))
        }
    }
}

async fn load_effective_version_descriptor_with_backend(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<VersionDescriptorRow>, LixError> {
    let row = scan_tracked_rows_with_backend(
        backend,
        &TrackedScanRequest {
            schema_key: version_descriptor_schema_key().to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            constraints: Vec::new(),
            required_columns: vec!["id".to_string(), "name".to_string(), "hidden".to_string()],
        },
    )
    .await?
    .into_iter()
    .find(|row| {
        row.entity_id == version_id
            && row.file_id.is_none()
            && row.plugin_key.is_none()
            && row.schema_version == version_descriptor_schema_version()
    });

    let Some(row) = row else {
        return Ok(None);
    };

    let version_id = row
        .values
        .get("id")
        .and_then(|value| match value {
            crate::Value::Text(value) => Some(value.clone()),
            _ => None,
        })
        .unwrap_or(row.entity_id);
    let name = row
        .values
        .get("name")
        .and_then(|value| match value {
            crate::Value::Text(value) => Some(value.clone()),
            _ => None,
        })
        .unwrap_or_default();
    let hidden = row
        .values
        .get("hidden")
        .and_then(|value| match value {
            crate::Value::Boolean(value) => Some(*value),
            crate::Value::Integer(value) => Some(*value != 0),
            _ => None,
        })
        .unwrap_or(false);

    Ok(Some(VersionDescriptorRow {
        version_id,
        name,
        hidden,
        change_id: row.change_id,
    }))
}

async fn load_effective_version_head_commit_id_with_backend(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    let mut executor = backend;
    Ok(load_version_head_commit_map_with_executor(&mut executor)
        .await?
        .and_then(|heads| heads.get(version_id).cloned()))
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
