use std::sync::Arc;

use serde_json::json;

use crate::engine2::functions::FunctionContext;
use crate::engine2::transaction::types::StageRow;
use crate::engine2::transaction::Transaction;
use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

use super::context::{SessionContext, SessionMode, WORKSPACE_VERSION_KEY};

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
const KEY_VALUE_SCHEMA_VERSION: &str = "1";

/// Options for switching a session to another version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchVersionOptions {
    pub version_id: String,
}

/// Receipt returned after switching to another version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchVersionReceipt {
    pub version_id: String,
}

impl SessionContext {
    /// Switches the session's active version selector.
    ///
    /// Pinned sessions switch in memory and return a new pinned session.
    /// Workspace sessions update the shared workspace selector so other
    /// workspace sessions observe the new active version on their next use.
    pub async fn switch_version(
        &self,
        options: SwitchVersionOptions,
    ) -> Result<(SessionContext, SwitchVersionReceipt), LixError> {
        self.ensure_version_ref_exists(&options.version_id).await?;

        let next_mode = match &self.mode {
            SessionMode::Pinned { .. } => SessionMode::Pinned {
                version_id: options.version_id.clone(),
            },
            SessionMode::Workspace => {
                self.write_workspace_version_selector(&options.version_id)
                    .await?;
                SessionMode::Workspace
            }
        };

        let session = SessionContext::new(
            next_mode,
            Arc::clone(&self.backend),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.changelog),
            Arc::clone(&self.version_ref),
            Arc::clone(&self.schema_registry),
        );
        Ok((
            session,
            SwitchVersionReceipt {
                version_id: options.version_id,
            },
        ))
    }

    async fn ensure_version_ref_exists(&self, version_id: &str) -> Result<(), LixError> {
        let head = self
            .version_ref
            .reader(Arc::clone(&self.backend))
            .load_head_commit_id(version_id)
            .await?;
        if head.is_none() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("cannot switch to missing version ref '{version_id}'"),
            ));
        }
        Ok(())
    }

    async fn write_workspace_version_selector(&self, version_id: &str) -> Result<(), LixError> {
        let live_state: Arc<dyn crate::engine2::live_state::LiveStateReader> =
            Arc::new(self.live_state.reader(Arc::clone(&self.backend)));
        let runtime_functions = FunctionContext::prepare(live_state.as_ref()).await?;
        let functions = runtime_functions.provider();
        let active_version_id = self.active_version_id().await?;

        let transaction = Transaction::open(
            active_version_id,
            &self.backend,
            Arc::clone(&self.live_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.changelog),
            Arc::clone(&self.version_ref),
            Arc::clone(&self.schema_registry),
            functions,
        )
        .await?;

        transaction.stage_rows(vec![workspace_version_stage_row(version_id)?])?;
        transaction.commit(&runtime_functions).await?;
        Ok(())
    }
}

fn workspace_version_stage_row(version_id: &str) -> Result<StageRow, LixError> {
    Ok(StageRow {
        entity_id: Some(crate::engine2::entity_identity::EntityIdentity::single(
            WORKSPACE_VERSION_KEY,
        )),
        schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
        file_id: None,
        plugin_key: None,
        snapshot_content: Some(encode_snapshot(json!({
            "key": WORKSPACE_VERSION_KEY,
            "value": version_id,
        }))?),
        metadata: None,
        schema_version: KEY_VALUE_SCHEMA_VERSION.to_string(),
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
            format!("engine2 switch_version snapshot serialization failed: {error}"),
        )
    })
}
