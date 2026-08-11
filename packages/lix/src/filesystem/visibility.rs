use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::Deserialize;

use crate::LixError;
use crate::hot_state::{
    HotStateFilter, HotStateReader, HotStateScanRequest, MaterializedHotStateBatch,
};

use super::keys::{
    BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY,
};
use super::planner::{FilesystemBlobRefKey, FilesystemDescriptorKey, FilesystemRowContext};

/// Execution-visible filesystem metadata decoded from live-state rows.
///
/// The helper intentionally depends only on `HotStateReader`. In engine
/// write execution that context may include staged rows, so filesystem planning
/// sees pending writes without reaching into write-execution internals.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct VisibleFilesystem {
    pub(crate) directory_children_by_parent_id:
        BTreeMap<Option<FilesystemDescriptorKey>, BTreeSet<String>>,
    pub(crate) files_by_directory_id: BTreeMap<Option<FilesystemDescriptorKey>, BTreeSet<String>>,
    pub(crate) blob_refs_by_key: BTreeSet<FilesystemBlobRefKey>,
}

impl VisibleFilesystem {
    /// Loads filesystem rows for a single branch from execution-visible live
    /// state and builds lookup indexes used by filesystem write planning.
    pub(crate) async fn load(
        hot_state: Arc<dyn HotStateReader>,
        branch_id: &str,
    ) -> Result<Self, LixError> {
        let rows = hot_state
            .scan_batch(&HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec![
                        DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
                        FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                        BLOB_REF_SCHEMA_KEY.to_string(),
                    ],
                    branch_ids: vec![branch_id.to_string()],
                    ..HotStateFilter::default()
                },
                ..HotStateScanRequest::default()
            })
            .await?;
        Self::from_live_batch(&rows)
    }

    pub(crate) fn from_live_batch(rows: &MaterializedHotStateBatch) -> Result<Self, LixError> {
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
                                "LIX_ERROR_UNKNOWN",
                                format!("invalid lix_directory_descriptor snapshot JSON: {error}"),
                            )
                        })?;
                    let key = FilesystemDescriptorKey::from_live_row_ref(row, snapshot.id.clone());
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
                            "LIX_ERROR_UNKNOWN",
                            format!("invalid lix_file_descriptor snapshot JSON: {error}"),
                        )
                    })?;
                    let key = FilesystemDescriptorKey::from_file_descriptor_live_row_ref(
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
                                "LIX_ERROR_UNKNOWN",
                                format!("invalid lix_binary_blob_ref snapshot JSON: {error}"),
                            )
                        })?;
                    visible
                        .blob_refs_by_key
                        .insert(FilesystemBlobRefKey::from_live_row_ref(row, snapshot.id));
                }
                _ => {}
            }
        }

        Ok(visible)
    }
}

impl VisibleFilesystem {
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
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use crate::LixError;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::filesystem::{FilesystemDescriptorKey, FilesystemRowContext};
    use crate::hot_state::{
        HotStateReader, HotStateScanRequest, MaterializedHotStateBatch, MaterializedHotStateRow,
    };

    use super::{
        BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY,
        VisibleFilesystem,
    };

    fn visible_filesystem_from_rows(
        rows: Vec<MaterializedHotStateRow>,
    ) -> Result<VisibleFilesystem, LixError> {
        VisibleFilesystem::from_live_batch(&MaterializedHotStateBatch::from_rows(rows))
    }

    #[tokio::test]
    async fn load_uses_expected_scan_filter() {
        let reader = Arc::new(RecordingHotStateReader {
            rows: vec![
                directory_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    r#"{"id":"01920000-0000-7000-8000-0000000000d3","parent_id":null,"name":"docs"}"#,
                ),
                live_row(
                    "other",
                    "other_schema",
                    None,
                    Some(r#"{"id":"other"}"#),
                    "01920000-0000-7000-8000-0000000000a1",
                ),
                live_row(
                    "01920000-0000-7000-8000-000000000383-branch",
                    DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                    None,
                    Some(
                        r#"{"id":"01920000-0000-7000-8000-000000000383-branch","parent_id":null,"name":"docs"}"#,
                    ),
                    "01920000-0000-7000-8000-0000000000b1",
                ),
            ],
            last_request: Mutex::new(None),
        });

        let filesystem =
            VisibleFilesystem::load(reader.clone(), "01920000-0000-7000-8000-0000000000a1")
                .await
                .expect("visible filesystem should load");

        let request = reader
            .last_request
            .lock()
            .expect("recorded request lock should not be poisoned")
            .clone()
            .expect("scan request should be recorded");
        assert_eq!(
            request.filter.schema_keys,
            vec![
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
                FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                BLOB_REF_SCHEMA_KEY.to_string(),
            ]
        );
        assert_eq!(
            request.filter.branch_ids,
            vec!["01920000-0000-7000-8000-0000000000a1".to_string()]
        );
        assert!(
            filesystem
                .directory_children_by_parent_id
                .get(&None)
                .is_some_and(|children| children.contains("01920000-0000-7000-8000-0000000000d3"))
        );
        assert!(
            !filesystem
                .directory_children_by_parent_id
                .get(&None)
                .is_some_and(
                    |children| children.contains("01920000-0000-7000-8000-000000000383-branch")
                )
        );
    }

    #[tokio::test]
    async fn nested_directories_resolve_correctly() {
        let filesystem = VisibleFilesystem::load(
            hot_state(vec![
                directory_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    r#"{"id":"01920000-0000-7000-8000-0000000000d3","parent_id":null,"name":"docs"}"#,
                ),
                directory_row(
                    "01920000-0000-7000-8000-000000000313",
                    r#"{"id":"01920000-0000-7000-8000-000000000313","parent_id":"01920000-0000-7000-8000-0000000000d3","name":"guides"}"#,
                ),
            ]),
            "01920000-0000-7000-8000-0000000000a1",
        )
        .await
        .expect("visible filesystem should load");

        assert!(
            filesystem
                .directory_children_by_parent_id
                .get(&None)
                .is_some_and(|children| children.contains("01920000-0000-7000-8000-0000000000d3"))
        );
        assert!(
            filesystem
                .directory_children_by_parent_id
                .get(&Some(descriptor_key(
                    "01920000-0000-7000-8000-0000000000a1",
                    "01920000-0000-7000-8000-0000000000d3"
                )))
                .is_some_and(|children| children.contains("01920000-0000-7000-8000-000000000313"))
        );
    }

    #[tokio::test]
    async fn files_attach_to_directory_ids() {
        let filesystem = VisibleFilesystem::load(
            hot_state(vec![file_row(
                "01920000-0000-7000-8000-0000000000d2",
                r#"{"id":"01920000-0000-7000-8000-0000000000d2","directory_id":"01920000-0000-7000-8000-000000000313","name":"readme.md"}"#,
            )]),
            "01920000-0000-7000-8000-0000000000a1",
        )
        .await
        .expect("visible filesystem should load");

        let files = filesystem
            .files_by_directory_id
            .get(&Some(descriptor_key(
                "01920000-0000-7000-8000-0000000000a1",
                "01920000-0000-7000-8000-000000000313",
            )))
            .expect("directory should have attached files");
        assert!(files.contains("01920000-0000-7000-8000-0000000000d2"));
    }

    #[tokio::test]
    async fn blob_refs_attach_to_file_ids() {
        let filesystem = VisibleFilesystem::load(
            hot_state(vec![blob_ref_row(
                "01920000-0000-7000-8000-0000000000d2",
                r#"{"id":"01920000-0000-7000-8000-0000000000d2","blob_hash":"abc123","size_bytes":5}"#,
            )]),
            "01920000-0000-7000-8000-0000000000a1",
        )
        .await
        .expect("visible filesystem should load");

        assert!(filesystem.has_blob_ref(
            &FilesystemRowContext::active_branch("01920000-0000-7000-8000-0000000000a1"),
            "01920000-0000-7000-8000-0000000000d2"
        ));
    }

    #[test]
    fn from_live_rows_ignores_tombstones_unrelated_schemas_and_indexes_root_files() {
        let filesystem = visible_filesystem_from_rows(vec![
            live_row(
                "dir-tombstone",
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                None,
                None,
                "01920000-0000-7000-8000-0000000000a1",
            ),
            live_row(
                "file-tombstone",
                FILE_DESCRIPTOR_SCHEMA_KEY,
                None,
                None,
                "01920000-0000-7000-8000-0000000000a1",
            ),
            live_row(
                "blob-tombstone",
                BLOB_REF_SCHEMA_KEY,
                Some("blob-tombstone".to_string()),
                None,
                "01920000-0000-7000-8000-0000000000a1",
            ),
            live_row(
                "other",
                "other_schema",
                None,
                Some(r#"{"id":"other"}"#),
                "01920000-0000-7000-8000-0000000000a1",
            ),
            file_row(
                "01920000-0000-7000-8000-000000000142",
                r#"{"id":"01920000-0000-7000-8000-000000000142","directory_id":null,"name":"readme.md"}"#,
            ),
        ])
        .expect("visible filesystem should load from edge rows");

        assert!(filesystem.directory_children_by_parent_id.is_empty());
        let root_files = filesystem
            .files_by_directory_id
            .get(&None)
            .expect("root files should be indexed under None");
        assert_eq!(
            root_files,
            &std::collections::BTreeSet::from(["01920000-0000-7000-8000-000000000142".to_string()])
        );
        assert!(!filesystem.has_blob_ref(
            &FilesystemRowContext::active_branch("01920000-0000-7000-8000-0000000000a1"),
            "blob-tombstone"
        ));
    }

    #[test]
    fn from_live_rows_rejects_invalid_filesystem_json() {
        let error = visible_filesystem_from_rows(vec![live_row(
            "dir-invalid",
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            None,
            Some("{not-json"),
            "01920000-0000-7000-8000-0000000000a1",
        )])
        .expect_err("invalid directory JSON should be rejected");

        assert_eq!(error.code, LixError::CODE_UNKNOWN);
        assert!(
            error
                .message
                .contains("invalid lix_directory_descriptor snapshot JSON")
        );
    }

    fn hot_state(rows: Vec<MaterializedHotStateRow>) -> Arc<dyn HotStateReader> {
        Arc::new(RowsHotStateReader { rows })
    }

    struct RecordingHotStateReader {
        rows: Vec<MaterializedHotStateRow>,
        last_request: Mutex<Option<HotStateScanRequest>>,
    }

    #[async_trait]
    impl HotStateReader for RecordingHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &crate::hot_state::HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            *self
                .last_request
                .lock()
                .expect("recorded request lock should not be poisoned") = Some(request.clone());
            Ok(MaterializedHotStateBatch::from_rows(
                self.rows
                    .iter()
                    .filter(|row| {
                        (request.filter.schema_keys.is_empty()
                            || request.filter.schema_keys.contains(&row.schema_key))
                            && (request.filter.branch_ids.is_empty()
                                || request
                                    .filter
                                    .branch_ids
                                    .iter()
                                    .any(|branch_id| branch_id.as_str() == row.branch_id.as_ref()))
                    })
                    .cloned()
                    .collect(),
            ))
        }
    }

    struct RowsHotStateReader {
        rows: Vec<MaterializedHotStateRow>,
    }

    #[async_trait]
    impl HotStateReader for RowsHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &crate::hot_state::HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(MaterializedHotStateBatch::from_rows(
                self.rows
                    .iter()
                    .filter(|row| {
                        (request.filter.schema_keys.is_empty()
                            || request.filter.schema_keys.contains(&row.schema_key))
                            && (request.filter.branch_ids.is_empty()
                                || request
                                    .filter
                                    .branch_ids
                                    .iter()
                                    .any(|branch_id| branch_id.as_str() == row.branch_id.as_ref()))
                    })
                    .cloned()
                    .collect(),
            ))
        }
    }

    fn directory_row(row_pk: &str, snapshot_content: &str) -> MaterializedHotStateRow {
        live_row(
            row_pk,
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            None,
            Some(snapshot_content),
            "01920000-0000-7000-8000-0000000000a1",
        )
    }

    fn file_row(row_pk: &str, snapshot_content: &str) -> MaterializedHotStateRow {
        live_row(
            row_pk,
            FILE_DESCRIPTOR_SCHEMA_KEY,
            None,
            Some(snapshot_content),
            "01920000-0000-7000-8000-0000000000a1",
        )
    }

    fn blob_ref_row(row_pk: &str, snapshot_content: &str) -> MaterializedHotStateRow {
        live_row(
            row_pk,
            BLOB_REF_SCHEMA_KEY,
            Some(row_pk.to_string()),
            Some(snapshot_content),
            "01920000-0000-7000-8000-0000000000a1",
        )
    }

    fn descriptor_key(branch_id: &str, descriptor_id: &str) -> FilesystemDescriptorKey {
        FilesystemDescriptorKey::from_context(
            &FilesystemRowContext::active_branch(branch_id),
            descriptor_id,
        )
    }

    fn live_row(
        row_pk: &str,
        schema_key: &str,
        file_id: Option<String>,
        snapshot_content: Option<&str>,
        branch_id: &str,
    ) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            row_pk: crate::row_pk::RowPk::single(row_pk),
            schema_key: schema_key.to_string(),
            file_id,
            snapshot_content: snapshot_content.map(Into::into),
            metadata: None,
            deleted: false,
            branch_id: branch_id.into(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{row_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{row_pk}"))),
            global: false,
            untracked: false,
            created_at: LixTimestamp::expect_parse(
                "filesystem visibility test created_at",
                "2026-04-23T00:00:00Z",
            ),
            updated_at: LixTimestamp::expect_parse(
                "filesystem visibility test updated_at",
                "2026-04-23T01:00:00Z",
            ),
        }
    }
}
