use crate::LixError;
use crate::branch::{
    BranchLifecycle, BranchOperation, BranchReferenceRole, branch_descriptor_stage_row,
};
use crate::row_pk::RowPk;
use crate::forktree::{
    StateCell, StateKeyRef, encode_state_row_prefix_bounds, encode_state_key,
};
use crate::storage_adapter::Storage;
use crate::transaction::types::{RawWriteBatch, TransactionWrite, TransactionWriteMode};

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

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Creates a new branch from this session's current branch head.
    ///
    /// Branch descriptors are tracked global facts so every branch agrees on
    /// which branches exist. Branch refs are untracked global moving pointers:
    /// each update is a changelog fact, but never a commit member.
    pub async fn create_branch(
        &self,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceipt, LixError> {
        self.with_write_transaction_lending(async move |transaction| {
            let branch_id = options
                .id
                .unwrap_or_else(|| transaction.functions().call_uuid_v7().to_string());
            let branch_pk = RowPk::uuid_from_canonical(&branch_id).map_err(|error| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("branch ID must be a canonical UUID: {error}"),
                )
            })?;
            let branch_key = encode_state_key(StateKeyRef {
                schema_key: "lix_branch_descriptor",
                file_id: None,
                row_pk: &branch_pk,
            });
            let committed_state_view = transaction.committed_state_view().await?;
            if committed_state_view
                .points(&[branch_key], false)
                .await?
                .into_iter()
                .next()
                .flatten()
                .is_some()
            {
                let row_pk = branch_pk
                    .as_json_array_text()
                    .unwrap_or_else(|_| "<invalid row_pk>".to_string());
                return Err(LixError::new(
                    LixError::CODE_UNIQUE,
                    format!(
                        "primary-key constraint violation on schema 'lix_branch_descriptor': INSERT would duplicate row_pk '{row_pk}'"
                    ),
                ));
            }
            let empty_pk = RowPk {
                components: crate::row_pk::RowPkComponents::Empty,
            };
            let bounds = encode_state_row_prefix_bounds("lix_branch_descriptor", &empty_pk);
            let existing_descriptors = committed_state_view
                .range(
                    Some(&bounds.lower),
                    bounds.upper.as_deref(),
                    None,
                    false,
                )
                .await?;
            for row in existing_descriptors {
                let Some(snapshot) = row.seed_logical_snapshot(transaction.active_branch_id())?
                else {
                    continue;
                };
                let value = serde_json::from_str::<serde_json::Value>(snapshot.as_ref())
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_STORAGE_ERROR,
                            format!("branch descriptor snapshot is invalid JSON: {error}"),
                        )
                    })?;
                if value.get("name").and_then(serde_json::Value::as_str)
                    == Some(options.name.as_str())
                {
                    return Err(LixError::new(
                        LixError::CODE_UNIQUE,
                        format!(
                            "unique constraint violation on schema 'lix_branch_descriptor' property '/name': branch name '{}' already exists",
                            options.name
                        ),
                    ));
                }
            }
            let source_head = if let Some(from_commit_id) = options.from_commit_id {
                let from_commit_id = BranchLifecycle::parse_commit_id(
                    &from_commit_id,
                    BranchOperation::CreateBranch,
                    BranchReferenceRole::CommitSource,
                )?;
                let mut commit_graph = transaction.commit_graph_reader_on_opening_read();
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
                let reader = transaction.branch_ref_reader_on_opening_read();
                BranchLifecycle::new(&reader)
                    .require_existing_commit_id(
                        &active_branch_id,
                        BranchOperation::CreateBranch,
                        BranchReferenceRole::Source,
                    )
                    .await?
            };

            let mut rows = RawWriteBatch::with_capacity(1);
            rows.push(branch_descriptor_stage_row(
                &branch_id,
                &options.name,
                false,
            ));
            transaction
                .stage_write(TransactionWrite::Rows {
                    mode: TransactionWriteMode::Insert,
                    rows,
                })
                .await?;
            transaction.stage_branch_ref_intent(&branch_id, Some(source_head), true)?;

            Ok(CreateBranchReceipt {
                id: branch_id,
                name: options.name,
                hidden: false,
                commit_id: source_head.to_string(),
            })
        })
        .await
    }
}
