use serde_json::json;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::branch::{BranchLifecycle, BranchOperation, BranchReferenceRole};
use crate::storage_adapter::Storage;
use crate::transaction::types::{RawWriteBatch, TransactionJson, TransactionWriteRow};

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
    /// Pinned sessions update their in-memory selector. Workspace sessions
    /// additionally persist the workspace selector. Clones of this session
    /// observe the switch in place; independently opened sessions retain the
    /// branch snapshot they opened with.
    pub async fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<SwitchBranchReceipt, LixError> {
        let branch_id = options.branch_id;
        let receipt_branch_id = branch_id.clone();
        let current_mode = self.mode.clone();
        let selector = match &self.mode {
            SessionMode::Pinned { branch_id } | SessionMode::Workspace { branch_id } => {
                branch_id.clone()
            }
        };
        let observe_invalidation = self.observe_invalidation.clone();
        let write_access = self.begin_session_write_access().await?;
        self.with_write_transaction_reserved_lending(
            write_access,
            async move |transaction| {
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

                match &current_mode {
                    SessionMode::Pinned { .. } => Ok(()),
                    SessionMode::Workspace { .. } => {
                        let mut rows = RawWriteBatch::with_capacity(1);
                        rows.push(workspace_branch_stage_row(&branch_id)?);
                        transaction.stage_rows(rows).await?;
                        Ok(())
                    }
                }
            },
            |()| {
                *selector.write().map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "session branch selector is poisoned",
                    )
                })? = receipt_branch_id.clone();
                observe_invalidation.bump();
                Ok(())
            },
        )
        .await?;

        Ok(SwitchBranchReceipt {
            branch_id: receipt_branch_id,
        })
    }
}

#[expect(clippy::unnecessary_wraps)]
fn workspace_branch_stage_row(branch_id: &str) -> Result<TransactionWriteRow, LixError> {
    Ok(TransactionWriteRow {
        entity_pk: Some(crate::entity_pk::EntityPk::single(WORKSPACE_BRANCH_KEY)),
        schema_key: KEY_VALUE_SCHEMA_KEY.into(),
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
        branch_id: GLOBAL_BRANCH_ID.into(),
    })
}
