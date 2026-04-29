use crate::backend::{KvStore, KvWriter};
use crate::engine2::commit_graph::CommitGraphContext;
use crate::engine2::tracked_state::rebuild::TrackedStateRebuildReport;
use crate::engine2::tracked_state::storage;
use crate::engine2::tracked_state::tree::TrackedStateTree;
use crate::engine2::tracked_state::tree_types::{
    TrackedStateKey, TrackedStateMutation, TrackedStateTreeScanRequest, TrackedStateValue,
};
use crate::engine2::tracked_state::{
    TrackedStateRow, TrackedStateRowRequest, TrackedStateScanRequest,
};
use crate::LixError;

/// Factory for rebuildable tracked-state readers and writers.
///
/// Tracked state is stored as content-addressed roots. Version refs
/// choose which commit/root to read; this context only owns root operations.
#[derive(Clone)]
pub(crate) struct TrackedStateContext {
    tree: TrackedStateTree,
}

impl TrackedStateContext {
    pub(crate) fn new() -> Self {
        Self {
            tree: TrackedStateTree::new(),
        }
    }

    /// Creates a commit-id-addressed tracked-state reader.
    pub(crate) fn reader<S>(&self, store: S) -> TrackedStateStoreReader<S>
    where
        S: KvStore,
    {
        TrackedStateStoreReader {
            store,
            tree: self.tree.clone(),
        }
    }

    /// Creates a tracked-state writer over a caller-provided KV writer.
    pub(crate) fn writer<S>(&self, store: S) -> TrackedStateWriter<S>
    where
        S: KvWriter,
    {
        TrackedStateWriter {
            store,
            tree: self.tree.clone(),
        }
    }

    /// Rebuilds tracked state at one commit from commit-graph entities.
    pub(crate) async fn rebuild_state_at_commit<R, W>(
        &self,
        commit_graph: &CommitGraphContext,
        read_store: R,
        write_store: W,
        head_commit_id: &str,
    ) -> Result<TrackedStateRebuildReport, LixError>
    where
        R: KvStore,
        W: KvWriter,
    {
        crate::engine2::tracked_state::rebuild::rebuild_state_at_commit(
            self,
            commit_graph,
            read_store,
            write_store,
            head_commit_id,
        )
        .await
    }
}

/// Read side for rebuildable tracked-state rows.
#[async_trait::async_trait]
pub(crate) trait TrackedStateReader {
    async fn scan_rows_at_commit(
        &mut self,
        commit_id: &str,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<TrackedStateRow>, LixError>;

    async fn load_row_at_commit(
        &mut self,
        commit_id: &str,
        request: &TrackedStateRowRequest,
    ) -> Result<Option<TrackedStateRow>, LixError>;
}

/// Store-backed tracked-state reader created by `TrackedStateContext`.
pub(crate) struct TrackedStateStoreReader<S> {
    store: S,
    tree: TrackedStateTree,
}

impl<S> TrackedStateStoreReader<S>
where
    S: KvStore,
{
    pub(crate) async fn scan_rows_at_commit(
        &mut self,
        commit_id: &str,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<TrackedStateRow>, LixError> {
        let Some(root_id) = self.tree.load_root(&mut self.store, commit_id).await? else {
            return Ok(Vec::new());
        };
        let rows = self
            .tree
            .scan(
                &mut self.store,
                &root_id,
                &tree_scan_request_from_tracked(request),
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|(key, value)| value.into_row(key))
            .collect())
    }

    pub(crate) async fn load_row_at_commit(
        &mut self,
        commit_id: &str,
        request: &TrackedStateRowRequest,
    ) -> Result<Option<TrackedStateRow>, LixError> {
        let key = tracked_key_from_request(request)?;
        let Some(root_id) = self.tree.load_root(&mut self.store, commit_id).await? else {
            return Ok(None);
        };
        Ok(self
            .tree
            .get(&mut self.store, &root_id, &key)
            .await?
            .map(|value| value.into_row(key)))
    }

    pub(crate) async fn root_exists(&mut self, commit_id: &str) -> Result<bool, LixError> {
        Ok(self
            .tree
            .load_root(&mut self.store, commit_id)
            .await?
            .is_some())
    }

    #[cfg(test)]
    pub(crate) async fn load_root_for_test(
        &mut self,
        commit_id: &str,
    ) -> Result<Option<crate::engine2::tracked_state::tree_types::TrackedStateRootId>, LixError>
    {
        self.tree.load_root(&mut self.store, commit_id).await
    }
}

#[async_trait::async_trait]
impl<S> TrackedStateReader for TrackedStateStoreReader<S>
where
    S: KvStore + Send,
{
    async fn scan_rows_at_commit(
        &mut self,
        commit_id: &str,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<TrackedStateRow>, LixError> {
        TrackedStateStoreReader::scan_rows_at_commit(self, commit_id, request).await
    }

    async fn load_row_at_commit(
        &mut self,
        commit_id: &str,
        request: &TrackedStateRowRequest,
    ) -> Result<Option<TrackedStateRow>, LixError> {
        TrackedStateStoreReader::load_row_at_commit(self, commit_id, request).await
    }
}

/// Writer for rebuildable tracked-state roots.
pub(crate) struct TrackedStateWriter<S> {
    store: S,
    tree: TrackedStateTree,
}

impl<S> TrackedStateWriter<S>
where
    S: KvWriter,
{
    /// Writes one root for `commit_id` from the provided row set.
    ///
    /// `parent_commit_id` is the tracked-state root to layer mutations on top
    /// of. Rebuild passes `None` because it has already materialized the full
    /// entity set for the requested head.
    pub(crate) async fn write_root(
        &mut self,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        rows: &[TrackedStateRow],
    ) -> Result<TrackedStateWriteReceipt, LixError> {
        let base_root = match parent_commit_id {
            Some(parent_commit_id) => {
                let Some(root) = self
                    .tree
                    .load_root(&mut self.store, parent_commit_id)
                    .await?
                else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "tracked-state parent root for commit '{parent_commit_id}' is missing"
                        ),
                    ));
                };
                Some(root)
            }
            None => None,
        };
        let mutations = rows
            .iter()
            .map(|row| {
                TrackedStateMutation::put(
                    TrackedStateKey::from_row(row),
                    TrackedStateValue::from_row(row),
                )
            })
            .collect::<Vec<_>>();
        let result = self
            .tree
            .apply_mutations(
                &mut self.store,
                base_root.as_ref(),
                mutations,
                Some(commit_id),
            )
            .await?;
        Ok(TrackedStateWriteReceipt {
            commit_id: commit_id.to_string(),
            row_count: result.row_count,
        })
    }

    /// Deletes the root pointer for one commit.
    ///
    /// This is intentionally root-scoped, not row-scoped. It is useful for
    /// rebuild/corruption tests where the changelog remains authoritative and
    /// the tracked-state projection must be recreated from the commit id.
    pub(crate) async fn delete_root_for_rebuild(
        &mut self,
        commit_id: &str,
    ) -> Result<(), LixError> {
        storage::delete_root(&mut self.store, commit_id).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateWriteReceipt {
    pub(crate) commit_id: String,
    pub(crate) row_count: usize,
}

fn tree_scan_request_from_tracked(
    request: &TrackedStateScanRequest,
) -> TrackedStateTreeScanRequest {
    TrackedStateTreeScanRequest {
        schema_keys: request.filter.schema_keys.clone(),
        entity_ids: request.filter.entity_ids.clone(),
        file_ids: request.filter.file_ids.clone(),
        include_tombstones: request.filter.include_tombstones,
        limit: request.limit,
    }
}

fn tracked_key_from_request(request: &TrackedStateRowRequest) -> Result<TrackedStateKey, LixError> {
    let file_id = match &request.file_id {
        crate::NullableKeyFilter::Null => None,
        crate::NullableKeyFilter::Value(value) => Some(value.clone()),
        crate::NullableKeyFilter::Any => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state tree exact lookup requires a concrete file_id filter",
            ))
        }
    };
    Ok(TrackedStateKey {
        schema_key: request.schema_key.clone(),
        file_id,
        entity_id: request.entity_id.clone(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};

    #[tokio::test]
    async fn write_root_rejects_missing_parent_root() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let tracked_state = TrackedStateContext::new();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        let error = tracked_state
            .writer(transaction.as_mut())
            .write_root(
                "commit-child",
                Some("missing-parent"),
                &[row("entity-child", "change-child", "commit-child")],
            )
            .await
            .expect_err("parent root must exist when parent_commit_id is provided");

        assert!(
            error.description.contains("parent root")
                && error.description.contains("missing-parent"),
            "unexpected error: {error:?}"
        );
    }

    fn row(entity_id: &str, change_id: &str, commit_id: &str) -> TrackedStateRow {
        TrackedStateRow {
            entity_id: entity_id.to_string(),
            schema_key: "test_schema".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some("{}".to_string()),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            change_id: change_id.to_string(),
            commit_id: commit_id.to_string(),
        }
    }
}
