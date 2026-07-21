use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::Deserialize;

use crate::LixError;
use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{LiveStateFilter, LiveStateReader, LiveStateScanRequest};

use super::keys::{
    BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY,
};
use super::planner::{FilesystemBlobRefKey, FilesystemDescriptorKey, FilesystemRowContext};

/// Execution-visible filesystem metadata decoded from live-state rows.
///
/// The helper intentionally depends only on `LiveStateReader`. In engine
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
        live_state: Arc<dyn LiveStateReader>,
        branch_id: &str,
    ) -> Result<Self, LixError> {
        let rows = live_state
            .scan_rows(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![
                        DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
                        FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                        BLOB_REF_SCHEMA_KEY.to_string(),
                    ],
                    branch_ids: vec![branch_id.to_string()],
                    ..LiveStateFilter::default()
                },
                ..LiveStateScanRequest::default()
            })
            .await?;
        Self::from_live_rows(rows)
    }

    /// Builds filesystem lookup indexes from rows that are already known to be
    /// transaction-visible.
    pub(crate) fn from_live_rows(rows: Vec<MaterializedLiveStateRow>) -> Result<Self, LixError> {
        let mut visible = Self::default();

        for row in rows {
            let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                continue;
            };
            match row.schema_key.as_str() {
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                    let snapshot: DirectoryDescriptorSnapshot =
                        serde_json::from_str(snapshot_content).map_err(|error| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                format!("invalid lix_directory_descriptor snapshot JSON: {error}"),
                            )
                        })?;
                    let key = FilesystemDescriptorKey::from_live_row(&row, snapshot.id.clone());
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
                    let key = FilesystemDescriptorKey::from_live_row(&row, snapshot.id.clone());
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
                        .insert(FilesystemBlobRefKey::from_live_row(&row, snapshot.id));
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
    use crate::filesystem::{FilesystemDescriptorKey, FilesystemRowContext};
    use crate::live_state::MaterializedLiveStateRow;
    use crate::live_state::{LiveStateReader, LiveStateRowRequest, LiveStateScanRequest};

    use super::{
        BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY,
        VisibleFilesystem,
    };

    #[tokio::test]
    async fn load_uses_expected_scan_filter() {
        let reader = Arc::new(RecordingLiveStateReader {
            rows: vec![
                directory_row(
                    "dir-docs",
                    r#"{"id":"dir-docs","parent_id":null,"name":"docs"}"#,
                ),
                live_row(
                    "other",
                    "other_schema",
                    None,
                    Some(r#"{"id":"other"}"#),
                    "branch-a",
                ),
                live_row(
                    "dir-other-branch",
                    DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                    None,
                    Some(r#"{"id":"dir-other-branch","parent_id":null,"name":"docs"}"#),
                    "branch-b",
                ),
            ],
            last_request: Mutex::new(None),
        });

        let filesystem = VisibleFilesystem::load(reader.clone(), "branch-a")
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
        assert_eq!(request.filter.branch_ids, vec!["branch-a".to_string()]);
        assert!(
            filesystem
                .directory_children_by_parent_id
                .get(&None)
                .is_some_and(|children| children.contains("dir-docs"))
        );
        assert!(
            !filesystem
                .directory_children_by_parent_id
                .get(&None)
                .is_some_and(|children| children.contains("dir-other-branch"))
        );
    }

    #[tokio::test]
    async fn nested_directories_resolve_correctly() {
        let filesystem = VisibleFilesystem::load(
            live_state(vec![
                directory_row(
                    "dir-docs",
                    r#"{"id":"dir-docs","parent_id":null,"name":"docs"}"#,
                ),
                directory_row(
                    "dir-guides",
                    r#"{"id":"dir-guides","parent_id":"dir-docs","name":"guides"}"#,
                ),
            ]),
            "branch-a",
        )
        .await
        .expect("visible filesystem should load");

        assert!(
            filesystem
                .directory_children_by_parent_id
                .get(&None)
                .is_some_and(|children| children.contains("dir-docs"))
        );
        assert!(
            filesystem
                .directory_children_by_parent_id
                .get(&Some(descriptor_key("branch-a", "dir-docs")))
                .is_some_and(|children| children.contains("dir-guides"))
        );
    }

    #[tokio::test]
    async fn files_attach_to_directory_ids() {
        let filesystem = VisibleFilesystem::load(
            live_state(vec![file_row(
                "file-readme",
                r#"{"id":"file-readme","directory_id":"dir-guides","name":"readme.md"}"#,
            )]),
            "branch-a",
        )
        .await
        .expect("visible filesystem should load");

        let files = filesystem
            .files_by_directory_id
            .get(&Some(descriptor_key("branch-a", "dir-guides")))
            .expect("directory should have attached files");
        assert!(files.contains("file-readme"));
    }

    #[tokio::test]
    async fn blob_refs_attach_to_file_ids() {
        let filesystem = VisibleFilesystem::load(
            live_state(vec![blob_ref_row(
                "file-readme",
                r#"{"id":"file-readme","blob_hash":"abc123","size_bytes":5}"#,
            )]),
            "branch-a",
        )
        .await
        .expect("visible filesystem should load");

        assert!(filesystem.has_blob_ref(
            &FilesystemRowContext::active_branch("branch-a"),
            "file-readme"
        ));
    }

    #[test]
    fn from_live_rows_ignores_tombstones_unrelated_schemas_and_indexes_root_files() {
        let filesystem = VisibleFilesystem::from_live_rows(vec![
            live_row(
                "dir-tombstone",
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                None,
                None,
                "branch-a",
            ),
            live_row(
                "file-tombstone",
                FILE_DESCRIPTOR_SCHEMA_KEY,
                None,
                None,
                "branch-a",
            ),
            live_row(
                "blob-tombstone",
                BLOB_REF_SCHEMA_KEY,
                Some("blob-tombstone".to_string()),
                None,
                "branch-a",
            ),
            live_row(
                "other",
                "other_schema",
                None,
                Some(r#"{"id":"other"}"#),
                "branch-a",
            ),
            file_row(
                "file-root",
                r#"{"id":"file-root","directory_id":null,"name":"readme.md"}"#,
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
            &std::collections::BTreeSet::from(["file-root".to_string()])
        );
        assert!(!filesystem.has_blob_ref(
            &FilesystemRowContext::active_branch("branch-a"),
            "blob-tombstone"
        ));
    }

    #[test]
    fn from_live_rows_rejects_invalid_filesystem_json() {
        let error = VisibleFilesystem::from_live_rows(vec![live_row(
            "dir-invalid",
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            None,
            Some("{not-json"),
            "branch-a",
        )])
        .expect_err("invalid directory JSON should be rejected");

        assert_eq!(error.code, LixError::CODE_UNKNOWN);
        assert!(
            error
                .message
                .contains("invalid lix_directory_descriptor snapshot JSON")
        );
    }

    fn live_state(rows: Vec<MaterializedLiveStateRow>) -> Arc<dyn LiveStateReader> {
        Arc::new(RowsLiveStateReader { rows })
    }

    struct RecordingLiveStateReader {
        rows: Vec<MaterializedLiveStateRow>,
        last_request: Mutex<Option<LiveStateScanRequest>>,
    }

    #[async_trait]
    impl LiveStateReader for RecordingLiveStateReader {
        async fn load_exact_rows(
            &self,
            request: &crate::live_state::LiveStateExactBatchRequest,
        ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
            crate::live_state::load_exact_rows_via_scan_for_test(self, request).await
        }

        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            *self
                .last_request
                .lock()
                .expect("recorded request lock should not be poisoned") = Some(request.clone());
            Ok(self
                .rows
                .iter()
                .filter(|row| {
                    (request.filter.schema_keys.is_empty()
                        || request.filter.schema_keys.contains(&row.schema_key))
                        && (request.filter.branch_ids.is_empty()
                            || request.filter.branch_ids.contains(&row.branch_id))
                })
                .cloned()
                .collect())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }

    struct RowsLiveStateReader {
        rows: Vec<MaterializedLiveStateRow>,
    }

    #[async_trait]
    impl LiveStateReader for RowsLiveStateReader {
        async fn load_exact_rows(
            &self,
            request: &crate::live_state::LiveStateExactBatchRequest,
        ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
            crate::live_state::load_exact_rows_via_scan_for_test(self, request).await
        }

        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(self
                .rows
                .iter()
                .filter(|row| {
                    (request.filter.schema_keys.is_empty()
                        || request.filter.schema_keys.contains(&row.schema_key))
                        && (request.filter.branch_ids.is_empty()
                            || request.filter.branch_ids.contains(&row.branch_id))
                })
                .cloned()
                .collect())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }

    fn directory_row(entity_pk: &str, snapshot_content: &str) -> MaterializedLiveStateRow {
        live_row(
            entity_pk,
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            None,
            Some(snapshot_content),
            "branch-a",
        )
    }

    fn file_row(entity_pk: &str, snapshot_content: &str) -> MaterializedLiveStateRow {
        live_row(
            entity_pk,
            FILE_DESCRIPTOR_SCHEMA_KEY,
            None,
            Some(snapshot_content),
            "branch-a",
        )
    }

    fn blob_ref_row(entity_pk: &str, snapshot_content: &str) -> MaterializedLiveStateRow {
        live_row(
            entity_pk,
            BLOB_REF_SCHEMA_KEY,
            Some(entity_pk.to_string()),
            Some(snapshot_content),
            "branch-a",
        )
    }

    fn descriptor_key(branch_id: &str, descriptor_id: &str) -> FilesystemDescriptorKey {
        FilesystemDescriptorKey::from_context(
            &FilesystemRowContext::active_branch(branch_id),
            descriptor_id,
        )
    }

    fn live_row(
        entity_pk: &str,
        schema_key: &str,
        file_id: Option<String>,
        snapshot_content: Option<&str>,
        branch_id: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: crate::entity_pk::EntityPk::single(entity_pk),
            schema_key: schema_key.to_string(),
            file_id,
            snapshot_content: snapshot_content.map(ToOwned::to_owned),
            metadata: None,
            deleted: false,
            branch_id: branch_id.to_string(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{entity_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{entity_pk}"))),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }
}
