use crate::LixError;
use crate::branch::{
    BranchLifecycle, BranchOperation, BranchReferenceRole, branch_descriptor_stage_row,
    branch_ref_stage_row,
};
use crate::storage::StorageBackend;
use crate::transaction::types::{TransactionWrite, TransactionWriteMode};

use super::context::SessionContext;

/// Options for creating a new branch from the session's active branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateBranchOptions {
    /// Optional caller-provided branch id. If omitted, engine generates one.
    pub id: Option<String>,
    /// User-facing branch name.
    pub name: String,
    /// Optional commit id for the new branch head. If omitted, the current
    /// active branch head is used.
    pub from_commit_id: Option<String>,
}

/// Receipt returned after creating a branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateBranchReceipt {
    pub id: String,
    pub name: String,
    pub hidden: bool,
    pub commit_id: String,
}

impl<B> SessionContext<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    /// Creates a new branch from this session's current branch head.
    ///
    /// Branch descriptors are tracked global facts so every branch agrees on
    /// which branches exist. Branch refs are untracked global moving pointers,
    /// so creating a ref does not add another changelog fact.
    pub async fn create_branch(
        &self,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceipt, LixError> {
        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                let branch_id = options
                    .id
                    .unwrap_or_else(|| transaction.functions().call_uuid_v7().to_string());
                let source_head = if let Some(from_commit_id) = options.from_commit_id {
                    let from_commit_id = BranchLifecycle::parse_commit_id(
                        &from_commit_id,
                        BranchOperation::CreateBranch,
                        BranchReferenceRole::CommitSource,
                    )?;
                    let mut commit_graph = transaction.commit_graph_reader();
                    let commit = BranchLifecycle::require_existing_commit(
                        &mut commit_graph,
                        from_commit_id,
                        BranchOperation::CreateBranch,
                        BranchReferenceRole::CommitSource,
                    )
                    .await?;
                    commit.commit_id
                } else {
                    let active_branch_id = transaction.active_branch_id().to_string();
                    let reader = transaction.branch_ref_reader();
                    BranchLifecycle::new(&reader)
                        .require_existing_commit_id(
                            &active_branch_id,
                            BranchOperation::CreateBranch,
                            BranchReferenceRole::Source,
                        )
                        .await?
                };

                transaction
                    .stage_write(TransactionWrite::Rows {
                        mode: TransactionWriteMode::Insert,
                        rows: vec![
                            branch_descriptor_stage_row(&branch_id, &options.name, false),
                            branch_ref_stage_row(&branch_id, &source_head),
                        ],
                    })
                    .await?;

                Ok(CreateBranchReceipt {
                    id: branch_id,
                    name: options.name,
                    hidden: false,
                    commit_id: source_head.to_string(),
                })
            })
        })
        .await
    }
}
