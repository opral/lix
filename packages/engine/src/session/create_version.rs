use crate::transaction::types::{TransactionWrite, TransactionWriteMode};
use crate::version::{
    version_descriptor_stage_row, version_ref_stage_row, VersionLifecycle, VersionOperation,
    VersionReferenceRole,
};
use crate::LixError;

use super::context::SessionContext;

/// Options for creating a new version from the session's active version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateVersionOptions {
    /// Optional caller-provided version id. If omitted, engine generates one.
    pub id: Option<String>,
    /// User-facing version name.
    pub name: String,
    /// Optional commit id for the new version head. If omitted, the current
    /// active version head is used.
    pub from_commit_id: Option<String>,
}

/// Receipt returned after creating a version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateVersionReceipt {
    pub id: String,
    pub name: String,
    pub hidden: bool,
    pub commit_id: String,
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
        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                let version_id = options
                    .id
                    .unwrap_or_else(|| transaction.functions().call_uuid_v7());
                let source_head = if let Some(from_commit_id) = options.from_commit_id {
                    let mut commit_graph = transaction.commit_graph_reader();
                    VersionLifecycle::require_existing_commit(
                        &mut commit_graph,
                        &from_commit_id,
                        VersionOperation::CreateVersion,
                        VersionReferenceRole::CommitSource,
                    )
                    .await?;
                    from_commit_id
                } else {
                    let active_version_id = transaction.active_version_id().to_string();
                    let reader = transaction.version_ref_reader();
                    VersionLifecycle::new(&reader)
                        .require_existing_commit_id(
                            &active_version_id,
                            VersionOperation::CreateVersion,
                            VersionReferenceRole::Source,
                        )
                        .await?
                };

                transaction
                    .stage_write(TransactionWrite::Rows {
                        mode: TransactionWriteMode::Insert,
                        rows: vec![
                            version_descriptor_stage_row(&version_id, &options.name, false),
                            version_ref_stage_row(&version_id, &source_head),
                        ],
                    })
                    .await?;

                Ok(CreateVersionReceipt {
                    id: version_id,
                    name: options.name,
                    hidden: false,
                    commit_id: source_head,
                })
            })
        })
        .await
    }
}
