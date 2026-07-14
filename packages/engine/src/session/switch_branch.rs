use std::sync::Arc;

use serde_json::json;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::branch::{BranchLifecycle, BranchOperation, BranchReferenceRole};
use crate::storage_adapter::Storage;
use crate::transaction::types::{TransactionJson, TransactionWriteRow};

use super::context::{SessionContext, SessionMode, WORKSPACE_BRANCH_KEY};

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";

/// Options for switching a session to another branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchBranchOptions {
    pub branch_id: String,
}

/// Receipt returned after switching to another branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchBranchReceipt {
    pub branch_id: String,
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Switches the session's active branch selector.
    ///
    /// Pinned sessions switch in memory and return a new pinned session.
    /// Workspace sessions update the shared workspace selector so other
    /// workspace sessions observe the new active branch on their next use.
    pub async fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<(Self, SwitchBranchReceipt), LixError> {
        let branch_id = options.branch_id;
        let receipt_branch_id = branch_id.clone();
        let current_mode = self.mode.clone();
        let next_mode = self
            .with_write_transaction(|transaction| {
                Box::pin(async move {
                    {
                        let reader = transaction.branch_ref_reader().await;
                        BranchLifecycle::new(&reader)
                            .require_existing_commit_id(
                                &branch_id,
                                BranchOperation::SwitchBranch,
                                BranchReferenceRole::Target,
                            )
                            .await?
                    };

                    match current_mode {
                        SessionMode::Pinned { .. } => Ok(SessionMode::Pinned {
                            branch_id: branch_id.clone(),
                        }),
                        SessionMode::Workspace => {
                            transaction
                                .stage_rows(vec![workspace_branch_stage_row(&branch_id)?])
                                .await?;
                            Ok(SessionMode::Workspace)
                        }
                    }
                })
            })
            .await?;

        let session = Self::new_with_transaction_manager(
            next_mode,
            self.storage.clone(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.branch_ctx),
            Arc::clone(&self.catalog_context),
            Arc::clone(&self.deterministic_runtime_gate),
            Arc::clone(&self.observe_coordinator),
            Arc::clone(&self.observe_invalidation),
            self.plugin_host.clone(),
            self.transaction_manager(),
        );
        Ok((
            session,
            SwitchBranchReceipt {
                branch_id: receipt_branch_id,
            },
        ))
    }
}

#[expect(clippy::unnecessary_wraps)]
fn workspace_branch_stage_row(branch_id: &str) -> Result<TransactionWriteRow, LixError> {
    Ok(TransactionWriteRow {
        entity_pk: Some(crate::entity_pk::EntityPk::single(WORKSPACE_BRANCH_KEY)),
        schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(json!({
            "key": WORKSPACE_BRANCH_KEY,
            "value": branch_id,
        }))),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: true,
        change_id: None,
        commit_id: None,
        untracked: true,
        branch_id: GLOBAL_BRANCH_ID.to_string(),
    })
}
