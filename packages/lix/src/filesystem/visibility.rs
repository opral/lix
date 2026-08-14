use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

use crate::LixError;
use crate::state::ForkTreeStateView;

use super::keys::{
    BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY,
};
use super::planner::{FilesystemBlobRefKey, FilesystemDescriptorKey, FilesystemRowContext};
use super::{FilesystemStateRows, merge_filesystem_state_rows};

/// Execution-visible filesystem metadata decoded from a concrete state view.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct VisibleFilesystem {
    pub(crate) directory_children_by_parent_id:
        BTreeMap<Option<FilesystemDescriptorKey>, BTreeSet<String>>,
    pub(crate) files_by_directory_id: BTreeMap<Option<FilesystemDescriptorKey>, BTreeSet<String>>,
    pub(crate) blob_refs_by_key: BTreeSet<FilesystemBlobRefKey>,
}

impl VisibleFilesystem {
    /// Loads filesystem rows for one authenticated branch-bound state view.
    /// Transaction callers resolve their `TransactionStateView` overlay first
    /// and then pass the resulting rows to `from_state_rows`.
    pub(crate) async fn from_state_view<R>(
        state: &ForkTreeStateView<R>,
        branch_id: &str,
    ) -> Result<Self, LixError>
    where
        R: crate::storage_adapter::StorageAdapterRead,
    {
        let tracked_rows =
            super::filesystem_state_rows_for_branch(state, branch_id, true).await?;
        let rows = FilesystemStateRows::from_view_rows(tracked_rows, branch_id, false)?;
        Self::from_state_rows(&merge_filesystem_state_rows(rows, false))
    }

    pub(crate) fn from_state_rows(rows: &FilesystemStateRows) -> Result<Self, LixError> {
        let mut visible = Self::default();

        for row in rows.iter() {
            let Some(snapshot_content) = row.snapshot_content().map(|value| value.as_str()) else {
                continue;
            };
            match row.schema_key() {
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                    let snapshot: DirectoryDescriptorSnapshot =
                        serde_json::from_str(snapshot_content).map_err(|error| {
                            LixError::new(
                                LixError::CODE_UNKNOWN,
                                format!("invalid lix_directory_descriptor snapshot JSON: {error}"),
                            )
                        })?;
                    let key = FilesystemDescriptorKey::from_state_row_ref(row, snapshot.id.clone());
                    visible
                        .directory_children_by_parent_id
                        .entry(snapshot.parent_id.map(|id| key.in_same_scope(&id)))
                        .or_default()
                        .insert(snapshot.id);
                }
                FILE_DESCRIPTOR_SCHEMA_KEY => {
                    let snapshot: FileDescriptorSnapshot = serde_json::from_str(snapshot_content)
                        .map_err(|error| {
                        LixError::new(
                            LixError::CODE_UNKNOWN,
                            format!("invalid lix_file_descriptor snapshot JSON: {error}"),
                        )
                    })?;
                    let key = FilesystemDescriptorKey::from_file_descriptor_state_row_ref(
                        row,
                        snapshot.id.clone(),
                    );
                    visible
                        .files_by_directory_id
                        .entry(snapshot.directory_id.map(|id| key.in_same_scope(&id)))
                        .or_default()
                        .insert(snapshot.id);
                }
                BLOB_REF_SCHEMA_KEY => {
                    let snapshot: BlobRefSnapshot = serde_json::from_str(snapshot_content)
                        .map_err(|error| {
                            LixError::new(
                                LixError::CODE_UNKNOWN,
                                format!("invalid lix_binary_blob_ref snapshot JSON: {error}"),
                            )
                        })?;
                    visible
                        .blob_refs_by_key
                        .insert(FilesystemBlobRefKey::from_state_row_ref(row, snapshot.id));
                }
                _ => {}
            }
        }

        Ok(visible)
    }

    pub(crate) fn has_blob_ref(&self, context: &FilesystemRowContext, file_id: &str) -> bool {
        self.blob_refs_by_key
            .contains(&FilesystemBlobRefKey::from_context(context, file_id))
    }
}

#[derive(Debug, Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FileDescriptorSnapshot {
    id: String,
    directory_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BlobRefSnapshot {
    id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;
    use crate::filesystem::FilesystemStateRow;

    fn row(
        schema_key: &str,
        entity_pk: &str,
        file_id: Option<&str>,
        value: Option<&str>,
    ) -> FilesystemStateRow {
        FilesystemStateRow {
            entity_pk: EntityPk::single(entity_pk),
            schema_key: schema_key.to_owned(),
            file_id: file_id.map(str::to_owned),
            snapshot_content: value.map(Into::into),
            metadata: None,
            deleted: value.is_none(),
            created_at: LixTimestamp::expect_parse(
                "filesystem test timestamp",
                "2026-04-23T00:00:00Z",
            ),
            updated_at: LixTimestamp::expect_parse(
                "filesystem test timestamp",
                "2026-04-23T01:00:00Z",
            ),
            global: false,
            change_id: Some(ChangeId::for_test_label(entity_pk)),
            commit_id: Some(CommitId::for_test_label(entity_pk)),
            untracked: false,
            branch_id: "01920000-0000-7000-8000-0000000000a1".to_owned(),
        }
    }

    #[test]
    fn state_rows_preserve_descriptor_scope_and_blob_refs() {
        let rows = FilesystemStateRows::from_rows(vec![
            row(
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                "01920000-0000-7000-8000-0000000000d3",
                None,
                Some(r#"{"id":"01920000-0000-7000-8000-0000000000d3","parent_id":null}"#),
            ),
            row(
                FILE_DESCRIPTOR_SCHEMA_KEY,
                "01920000-0000-7000-8000-0000000000f4",
                None,
                Some(r#"{"id":"01920000-0000-7000-8000-0000000000f4","directory_id":null}"#),
            ),
            row(
                BLOB_REF_SCHEMA_KEY,
                "01920000-0000-7000-8000-0000000000f4",
                Some("01920000-0000-7000-8000-0000000000f4"),
                Some(r#"{"id":"01920000-0000-7000-8000-0000000000f4"}"#),
            ),
        ]);
        let visible = VisibleFilesystem::from_state_rows(&rows).unwrap();
        assert!(visible.directory_children_by_parent_id.contains_key(&None));
        assert!(visible.files_by_directory_id.contains_key(&None));
        assert!(visible.has_blob_ref(
            &FilesystemRowContext::active_branch("01920000-0000-7000-8000-0000000000a1"),
            "01920000-0000-7000-8000-0000000000f4",
        ));
    }

    #[test]
    fn malformed_descriptor_is_rejected_before_indexing() {
        let rows = FilesystemStateRows::from_rows(vec![row(
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            "bad",
            None,
            Some("{not-json"),
        )]);
        let error = VisibleFilesystem::from_state_rows(&rows).unwrap_err();
        assert_eq!(error.code, LixError::CODE_UNKNOWN);
    }
}
