use crate::transaction::types::{StageWrite, StageWriteMode};
use crate::version::{version_descriptor_stage_row, version_ref_stage_row, VersionRefReader};
use crate::LixError;

use super::context::SessionContext;

/// Options for creating a new version from the session's active version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateVersionOptions {
    /// Optional caller-provided version id. If omitted, engine2 generates one.
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
                    from_commit_id
                } else {
                    let active_version_id = transaction.active_version_id().to_string();
                    let reader = transaction.version_ref_reader();
                    reader
                        .load_head_commit_id(&active_version_id)
                        .await?
                        .ok_or_else(|| {
                            LixError::version_not_found(
                                active_version_id.clone(),
                                "create_version",
                                "source",
                            )
                        })?
                };

                transaction
                    .stage_write(StageWrite::Rows {
                        mode: StageWriteMode::Insert,
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
