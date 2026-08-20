use crate::LixError;
use crate::branch::{BranchLifecycle, BranchOperation, BranchReferenceRole};
use crate::storage_adapter::Storage;

use super::context::SessionContext;

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Restores the active branch to an ancestor commit.
    ///
    /// This moves the branch ref without creating a commit. Commits that were
    /// reachable only from the previous HEAD remain stored but are no longer
    /// part of this branch's history.
    pub async fn restore(&self, commit_id: String) -> Result<(), LixError> {
        self.with_write_transaction_lending(async move |transaction| {
            let branch_id = transaction.active_branch_id().to_string();
            let target_commit_id = BranchLifecycle::parse_commit_id(
                &commit_id,
                BranchOperation::Restore,
                BranchReferenceRole::Target,
            )?;
            let head_commit_id = {
                let reader = transaction.branch_ref_reader().await;
                BranchLifecycle::new(&reader)
                    .require_existing_commit_id(
                        &branch_id,
                        BranchOperation::Restore,
                        BranchReferenceRole::Source,
                    )
                    .await?
            };

            let mut commit_graph = transaction.commit_graph_reader().await;
            BranchLifecycle::require_existing_commit(
                &mut commit_graph,
                target_commit_id,
                BranchOperation::Restore,
                BranchReferenceRole::Target,
            )
            .await?;

            if target_commit_id == head_commit_id {
                return Ok(());
            }

            let target_is_ancestor = commit_graph
                .reachable_nodes(&head_commit_id)
                .await?
                .iter()
                .any(|reachable| reachable.commit.commit_id == target_commit_id);
            if !target_is_ancestor {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "restore target commit '{target_commit_id}' is not an ancestor of branch '{branch_id}' HEAD '{head_commit_id}'"
                    ),
                ));
            }
            drop(commit_graph);

            transaction
                .restore_branch_ref(&branch_id, head_commit_id, target_commit_id)
                .await?;

            Ok(())
        })
        .await
    }
}
