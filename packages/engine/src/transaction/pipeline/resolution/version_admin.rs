use crate::canonical::{
    load_exact_committed_change_from_commit_with_executor, ExactCommittedStateRowRequest,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::version::{
    parse_version_descriptor_snapshot, version_descriptor_file_id, version_descriptor_plugin_key,
    version_descriptor_schema_key, version_descriptor_schema_version,
};
use crate::{LixBackend, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedVersionAdminState {
    pub(crate) version_id: String,
    pub(crate) name: String,
    pub(crate) hidden: bool,
    pub(crate) descriptor_change_id: Option<String>,
    pub(crate) head_commit_id: Option<String>,
}

pub(crate) async fn load_version_admin_state_with_backend(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<ResolvedVersionAdminState>, LixError> {
    let Some(global_head_commit_id) =
        crate::live_state::load_version_head_commit_id_with_backend(backend, GLOBAL_VERSION_ID)
            .await?
    else {
        return Ok(None);
    };

    let mut executor = backend;
    let row = load_exact_committed_change_from_commit_with_executor(
        &mut executor,
        &global_head_commit_id,
        &ExactCommittedStateRowRequest {
            entity_id: version_id.to_string(),
            schema_key: version_descriptor_schema_key().to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            exact_filters: std::collections::BTreeMap::from([
                (
                    "file_id".to_string(),
                    Value::Text(version_descriptor_file_id().to_string()),
                ),
                (
                    "plugin_key".to_string(),
                    Value::Text(version_descriptor_plugin_key().to_string()),
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
    let snapshot = parse_version_descriptor_snapshot(snapshot_content)?;
    let head_commit_id =
        crate::live_state::load_version_head_commit_id_with_backend(backend, version_id).await?;

    Ok(Some(ResolvedVersionAdminState {
        version_id: snapshot.id,
        name: snapshot.name.unwrap_or_default(),
        hidden: snapshot.hidden,
        descriptor_change_id: Some(row.id),
        head_commit_id,
    }))
}
