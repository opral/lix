use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::Deserialize;

use crate::LixError;
use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{LiveStateFilter, LiveStateReader, LiveStateScanRequest};

use super::filesystem_planner::{
    BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY,
};

/// Execution-visible filesystem metadata decoded from live-state rows.
///
/// The helper intentionally depends only on `LiveStateReader`. In engine
/// write execution that context may include staged rows, so filesystem planning
/// sees pending writes without reaching into write-execution internals.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct VisibleFilesystem {
    pub(crate) directory_children_by_parent_id: BTreeMap<Option<String>, BTreeSet<String>>,
    pub(crate) files_by_directory_id: BTreeMap<Option<String>, BTreeSet<String>>,
    pub(crate) blob_refs_by_file_id: BTreeSet<String>,
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
                    visible
                        .directory_children_by_parent_id
                        .entry(snapshot.parent_id)
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
                    visible
                        .files_by_directory_id
                        .entry(snapshot.directory_id)
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
                    visible.blob_refs_by_file_id.insert(snapshot.id);
                }
                _ => {}
            }
        }

        Ok(visible)
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
    use async_trait::async_trait;

    use crate::LixError;
    use crate::changelog::{ChangeId, CommitId};
    use crate::live_state::MaterializedLiveStateRow;
    use crate::live_state::{LiveStateReader, LiveStateRowRequest, LiveStateScanRequest};

    use super::{
        BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY,
        VisibleFilesystem,
    };

    #[tokio::test]
    async fn nested_directories_resolve_correctly() {
        let filesystem = VisibleFilesystem::load(
            live_state(vec![
                directory_row(
                    "dir-docs",
                    r#"{"id":"dir-docs","parent_id":null,"name":"docs","hidden":false}"#,
                ),
                directory_row(
                    "dir-guides",
                    r#"{"id":"dir-guides","parent_id":"dir-docs","name":"guides","hidden":false}"#,
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
                .get(&Some("dir-docs".to_string()))
                .is_some_and(|children| children.contains("dir-guides"))
        );
    }

    #[tokio::test]
    async fn files_attach_to_directory_ids() {
        let filesystem = VisibleFilesystem::load(
            live_state(vec![file_row(
                "file-readme",
                r#"{"id":"file-readme","directory_id":"dir-guides","name":"readme.md","hidden":false}"#,
            )]),
            "branch-a",
        )
        .await
        .expect("visible filesystem should load");

        let files = filesystem
            .files_by_directory_id
            .get(&Some("dir-guides".to_string()))
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

        assert!(filesystem.blob_refs_by_file_id.contains("file-readme"));
    }

    fn live_state(rows: Vec<MaterializedLiveStateRow>) -> std::sync::Arc<dyn LiveStateReader> {
        std::sync::Arc::new(RowsLiveStateReader { rows })
    }

    struct RowsLiveStateReader {
        rows: Vec<MaterializedLiveStateRow>,
    }

    #[async_trait]
    impl LiveStateReader for RowsLiveStateReader {
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
            snapshot_content,
        )
    }

    fn file_row(entity_pk: &str, snapshot_content: &str) -> MaterializedLiveStateRow {
        live_row(
            entity_pk,
            FILE_DESCRIPTOR_SCHEMA_KEY,
            None,
            snapshot_content,
        )
    }

    fn blob_ref_row(entity_pk: &str, snapshot_content: &str) -> MaterializedLiveStateRow {
        live_row(
            entity_pk,
            BLOB_REF_SCHEMA_KEY,
            Some(entity_pk.to_string()),
            snapshot_content,
        )
    }

    fn live_row(
        entity_pk: &str,
        schema_key: &str,
        file_id: Option<String>,
        snapshot_content: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: crate::entity_pk::EntityPk::single(entity_pk),
            schema_key: schema_key.to_string(),
            file_id,
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: None,
            deleted: false,
            branch_id: "branch-a".to_string(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{entity_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{entity_pk}"))),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }
}
