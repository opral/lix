use std::sync::Arc;

use serde_json::json;

use crate::engine2::functions::FunctionContext;
use crate::engine2::transaction::types::StageRow;
use crate::engine2::transaction::Transaction;
use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

use super::context::SessionContext;

const VERSION_DESCRIPTOR_SCHEMA_KEY: &str = "lix_version_descriptor";
const VERSION_DESCRIPTOR_SCHEMA_VERSION: &str = "1";
const VERSION_REF_SCHEMA_KEY: &str = "lix_version_ref";
const VERSION_REF_SCHEMA_VERSION: &str = "1";

/// Options for creating a new version from the session's active version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateVersionOptions {
    /// Optional caller-provided version id. If omitted, engine2 generates one.
    pub id: Option<String>,
    /// User-facing version name.
    pub name: String,
}

/// Receipt returned after creating a version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateVersionReceipt {
    pub version_id: String,
}

impl SessionContext {
    /// Creates a new version from this session's current version head.
    ///
    /// Version descriptors are tracked global facts so every version agrees on
    /// which versions exist. Version refs are untracked global moving pointers,
    /// so creating a ref does not add another changelog fact.
    pub async fn create_version(
        &self,
        options: CreateVersionOptions,
    ) -> Result<CreateVersionReceipt, LixError> {
        let live_state: Arc<dyn crate::engine2::live_state::LiveStateReader> =
            Arc::new(self.live_state.reader(Arc::clone(&self.backend)));
        let runtime_functions = FunctionContext::prepare(live_state.as_ref()).await?;
        let functions = runtime_functions.provider();
        let version_id = options.id.unwrap_or_else(|| functions.call_uuid_v7());

        let mut transaction = Transaction::open(
            self.active_version_id().to_string(),
            &self.backend,
            Arc::clone(&self.live_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.changelog),
            Arc::clone(&self.version_ref),
            Arc::clone(&self.schema_registry),
            functions.clone(),
        )
        .await?;

        let source_head = {
            let reader = self.version_ref.reader(transaction.kv_store());
            reader
                .load_head_commit_id(self.active_version_id())
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "cannot create version from missing active version ref '{}'",
                            self.active_version_id()
                        ),
                    )
                })?
        };

        transaction.stage_rows(vec![
            version_descriptor_stage_row(&version_id, &options.name)?,
            version_ref_stage_row(&version_id, &source_head)?,
        ])?;
        transaction.commit(&runtime_functions).await?;

        Ok(CreateVersionReceipt { version_id })
    }
}

fn version_descriptor_stage_row(version_id: &str, name: &str) -> Result<StageRow, LixError> {
    Ok(StageRow {
        entity_id: version_id.to_string(),
        schema_key: VERSION_DESCRIPTOR_SCHEMA_KEY.to_string(),
        file_id: None,
        plugin_key: None,
        snapshot_content: Some(encode_snapshot(json!({
            "id": version_id,
            "name": name,
            "hidden": false,
        }))?),
        metadata: None,
        schema_version: VERSION_DESCRIPTOR_SCHEMA_VERSION.to_string(),
        created_at: None,
        updated_at: None,
        global: true,
        change_id: None,
        commit_id: None,
        untracked: false,
        version_id: GLOBAL_VERSION_ID.to_string(),
    })
}

fn version_ref_stage_row(version_id: &str, commit_id: &str) -> Result<StageRow, LixError> {
    Ok(StageRow {
        entity_id: version_id.to_string(),
        schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
        file_id: None,
        plugin_key: None,
        snapshot_content: Some(encode_snapshot(json!({
            "id": version_id,
            "commit_id": commit_id,
        }))?),
        metadata: None,
        schema_version: VERSION_REF_SCHEMA_VERSION.to_string(),
        created_at: None,
        updated_at: None,
        global: true,
        change_id: None,
        commit_id: None,
        untracked: true,
        version_id: GLOBAL_VERSION_ID.to_string(),
    })
}

fn encode_snapshot(value: serde_json::Value) -> Result<String, LixError> {
    serde_json::to_string(&value).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("engine2 create_version snapshot serialization failed: {error}"),
        )
    })
}
