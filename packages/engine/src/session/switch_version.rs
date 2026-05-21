use std::sync::Arc;

use serde_json::json;

use crate::storage::StorageBackend;
use crate::transaction::types::{TransactionJson, TransactionWriteRow};
use crate::version::{VersionLifecycle, VersionOperation, VersionReferenceRole};
use crate::LixError;
use crate::GLOBAL_VERSION_ID;

use super::context::{SessionContext, SessionMode, WORKSPACE_VERSION_KEY};

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";

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

impl<B> SessionContext<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    /// Switches the session's active version selector.
    ///
    /// Pinned sessions switch in memory and return a new pinned session.
    /// Workspace sessions update the shared workspace selector so other
    /// workspace sessions observe the new active version on their next use.
    pub async fn switch_version(
        &self,
        options: SwitchVersionOptions,
    ) -> Result<(SessionContext<B>, SwitchVersionReceipt), LixError> {
        let version_id = options.version_id;
        let receipt_version_id = version_id.clone();
        let current_mode = self.mode.clone();
        let next_mode = self
            .with_write_transaction(|transaction| {
                Box::pin(async move {
                    {
                        let reader = transaction.version_ref_reader();
                        VersionLifecycle::new(&reader)
                            .require_existing_commit_id(
                                &version_id,
                                VersionOperation::SwitchVersion,
                                VersionReferenceRole::Target,
                            )
                            .await?
                    };

                    match current_mode {
                        SessionMode::Pinned { .. } => Ok(SessionMode::Pinned {
                            version_id: version_id.clone(),
                        }),
                        SessionMode::Workspace => {
                            transaction
                                .stage_rows(vec![workspace_version_stage_row(&version_id)?])
                                .await?;
                            Ok(SessionMode::Workspace)
                        }
                    }
                })
            })
            .await?;

        let session = SessionContext::new_with_transaction_manager(
            next_mode,
            self.storage.clone(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.version_ctx),
            Arc::clone(&self.catalog_context),
            self.write_lock.clone(),
            self.transaction_manager(),
        );
        Ok((
            session,
            SwitchVersionReceipt {
                version_id: receipt_version_id,
            },
        ))
    }
}

fn workspace_version_stage_row(version_id: &str) -> Result<TransactionWriteRow, LixError> {
    Ok(TransactionWriteRow {
        entity_pk: Some(crate::entity_pk::EntityPk::single(WORKSPACE_VERSION_KEY)),
        schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(json!({
            "key": WORKSPACE_VERSION_KEY,
            "value": version_id,
        }))),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: true,
        change_id: None,
        commit_id: None,
        untracked: true,
        version_id: GLOBAL_VERSION_ID.to_string(),
    })
}
