use std::collections::BTreeMap;

use crate::LixError;
use crate::storage::{StorageRead, StorageWriteSet};
use crate::tracked_state::{
    TrackedRowMaterialization, TrackedStateChunkOverlay, TrackedStateIndexValue,
    TrackedStateIndexValueRef, TrackedStateKey, TrackedStateKeyRef, TrackedStateMutation,
    TrackedStateRootId, TrackedStateTree, TrackedStateTreeScanRequest, encode_key_ref,
    encode_value_ref, materialize_rows_from_index_entries,
};

use super::storage::{load_branch_root, stage_branch_root, stage_delete_branch_root};
use super::{
    LiveStateIndexDeltaRef, LiveStateIndexRow, LiveStateIndexRowRequest, LiveStateIndexScanRequest,
    MaterializedLiveStateIndexRow,
};

/// Factory for canonical current index readers and writers.
#[derive(Clone)]
pub(crate) struct LiveStateIndexContext {
    tree: TrackedStateTree,
}

impl LiveStateIndexContext {
    pub(crate) fn new() -> Self {
        Self {
            tree: TrackedStateTree::new(),
        }
    }

    pub(crate) fn reader<S>(&self, store: S) -> LiveStateIndexStoreReader<S>
    where
        S: StorageRead + Send + Sync,
    {
        LiveStateIndexStoreReader {
            store,
            tree: self.tree.clone(),
        }
    }

    pub(crate) fn writer<'a, S>(
        &'a self,
        store: &'a S,
        writes: &'a mut StorageWriteSet,
    ) -> LiveStateIndexWriter<'a, S>
    where
        S: StorageRead + Send + Sync + ?Sized,
    {
        LiveStateIndexWriter {
            chunk_overlay: TrackedStateChunkOverlay::new(),
            staged_roots: BTreeMap::new(),
            tree: self.tree.clone(),
            store,
            writes,
        }
    }
}

pub(crate) struct LiveStateIndexStoreReader<S> {
    store: S,
    tree: TrackedStateTree,
}

impl<S> LiveStateIndexStoreReader<S>
where
    S: StorageRead + Send + Sync,
{
    pub(crate) fn load_branch_root(
        &self,
        branch_id: &str,
    ) -> Result<Option<TrackedStateRootId>, LixError> {
        load_branch_root(&self.store, branch_id)
    }

    pub(crate) async fn scan_rows(
        &self,
        request: &LiveStateIndexScanRequest,
    ) -> Result<Vec<MaterializedLiveStateIndexRow>, LixError> {
        let Some(root_id) = self.load_branch_root(&request.branch_id)? else {
            return Ok(Vec::new());
        };
        let entries = self
            .tree
            .scan(
                &self.store,
                &root_id,
                &TrackedStateTreeScanRequest {
                    schema_keys: request.filter.schema_keys.clone(),
                    entity_pks: request.filter.entity_pks.clone(),
                    file_ids: request.filter.file_ids.clone(),
                    include_tombstones: request.filter.include_tombstones,
                    limit: request.limit,
                },
            )
            .await?;
        let rows = materialize_rows_from_index_entries(
            &self.store,
            entries,
            &TrackedRowMaterialization::from_columns(&request.projection),
        )
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| materialize_current_row(&request.branch_id, row))
            .collect())
    }

    pub(crate) async fn load_row(
        &self,
        request: &LiveStateIndexRowRequest,
    ) -> Result<Option<MaterializedLiveStateIndexRow>, LixError> {
        let Some(root_id) = self.load_branch_root(&request.branch_id)? else {
            return Ok(None);
        };
        let Some((key, value)) = self.load_index_entry(&root_id, request).await? else {
            return Ok(None);
        };
        let mut rows = materialize_rows_from_index_entries(
            &self.store,
            vec![(key, value)],
            &TrackedRowMaterialization::full(),
        )
        .await?;
        Ok(rows
            .pop()
            .map(|row| materialize_current_row(&request.branch_id, row)))
    }

    /// Loads one current index header without hydrating changelog payloads.
    pub(crate) async fn load_index_row(
        &self,
        request: &LiveStateIndexRowRequest,
    ) -> Result<Option<LiveStateIndexRow>, LixError> {
        let Some(root_id) = self.load_branch_root(&request.branch_id)? else {
            return Ok(None);
        };
        Ok(self
            .load_index_entry(&root_id, request)
            .await?
            .map(|(key, value)| materialize_index_entry(&request.branch_id, key, value)))
    }

    async fn load_index_entry(
        &self,
        root_id: &TrackedStateRootId,
        request: &LiveStateIndexRowRequest,
    ) -> Result<Option<(TrackedStateKey, TrackedStateIndexValue)>, LixError> {
        let key = TrackedStateKey {
            schema_key: request.schema_key.clone(),
            file_id: request.file_id.clone(),
            entity_pk: request.entity_pk.clone(),
        };
        let value = self
            .tree
            .get_many(&self.store, root_id, std::slice::from_ref(&key))
            .await?
            .into_iter()
            .next()
            .flatten();
        Ok(value.map(|value| (key, value)))
    }
}

pub(crate) struct LiveStateIndexWriter<'a, S: ?Sized> {
    chunk_overlay: TrackedStateChunkOverlay,
    staged_roots: BTreeMap<String, TrackedStateRootId>,
    tree: TrackedStateTree,
    store: &'a S,
    writes: &'a mut StorageWriteSet,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveStateIndexWriteReport {
    pub(crate) branch_id: String,
    pub(crate) root_id: TrackedStateRootId,
    pub(crate) changed_rows: usize,
}

impl<S> LiveStateIndexWriter<'_, S>
where
    S: StorageRead + Send + Sync + ?Sized,
{
    /// Deletes the mutable current index pointer for one branch.
    ///
    /// Immutable tracked roots remain available through retained commits.
    pub(crate) fn stage_delete_branch_root(&mut self, branch_id: &str) {
        stage_delete_branch_root(self.writes, branch_id);
        self.staged_roots.remove(branch_id);
    }

    /// Points a branch's canonical live state at an existing tree root.
    ///
    /// This shares immutable tracked-state chunks without copying rows and is
    /// used when initialization or branch creation already knows the desired
    /// base root.
    pub(crate) fn stage_branch_root_from_existing(
        &mut self,
        branch_id: &str,
        root_id: &TrackedStateRootId,
    ) -> Result<LiveStateIndexWriteReport, LixError> {
        stage_branch_root(self.writes, branch_id, root_id)?;
        self.staged_roots
            .insert(branch_id.to_string(), root_id.clone());
        Ok(LiveStateIndexWriteReport {
            branch_id: branch_id.to_string(),
            root_id: root_id.clone(),
            changed_rows: 0,
        })
    }

    pub(crate) async fn stage_branch_rows<'a, I>(
        &mut self,
        branch_id: &str,
        deltas: I,
    ) -> Result<LiveStateIndexWriteReport, LixError>
    where
        I: IntoIterator<Item = LiveStateIndexDeltaRef<'a>>,
    {
        let base_root = match self.staged_roots.get(branch_id) {
            Some(root_id) => Some(root_id.clone()),
            None => load_branch_root(self.store, branch_id)?,
        };
        self.stage_branch_rows_with_base(branch_id, base_root, deltas)
            .await
    }

    /// Applies rows to an explicit existing root and stages only the resulting
    /// branch pointer.
    ///
    /// The supplied root must already be readable from `store`. This combines
    /// a branch-head reset plus same-transaction deltas into one canonical
    /// branch-root mutation.
    pub(crate) async fn stage_branch_rows_from_existing_root<'a, I>(
        &mut self,
        branch_id: &str,
        base_root: &TrackedStateRootId,
        deltas: I,
    ) -> Result<LiveStateIndexWriteReport, LixError>
    where
        I: IntoIterator<Item = LiveStateIndexDeltaRef<'a>>,
    {
        self.stage_branch_rows_with_base(branch_id, Some(base_root.clone()), deltas)
            .await
    }

    async fn stage_branch_rows_with_base<'a, I>(
        &mut self,
        branch_id: &str,
        base_root: Option<TrackedStateRootId>,
        deltas: I,
    ) -> Result<LiveStateIndexWriteReport, LixError>
    where
        I: IntoIterator<Item = LiveStateIndexDeltaRef<'a>>,
    {
        let mut final_deltas = BTreeMap::<TrackedStateKey, LiveStateIndexDeltaRef<'a>>::new();
        for delta in deltas {
            if delta
                .commit_id
                .is_some_and(|commit_id| commit_id.as_uuid().is_nil())
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "current index tracked commit_id must not be the reserved nil UUID",
                ));
            }
            final_deltas.insert(
                TrackedStateKey {
                    schema_key: delta.schema_key.to_string(),
                    file_id: delta.file_id.map(str::to_string),
                    entity_pk: delta.entity_pk.clone(),
                },
                delta,
            );
        }
        if final_deltas.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "current index stage_branch_rows requires at least one delta",
            ));
        }

        let keys = final_deltas.keys().cloned().collect::<Vec<_>>();
        let prior_values = if let Some(root_id) = base_root.as_ref() {
            // Current callers stage one final batch per branch. This read also
            // preserves the original created_at across durable overwrites.
            self.tree.get_many(self.store, root_id, &keys).await?
        } else {
            vec![None; keys.len()]
        };

        let mutations = final_deltas
            .into_iter()
            .zip(prior_values)
            .map(|((key, delta), prior)| {
                let created_at = prior
                    .as_ref()
                    .map(TrackedStateIndexValue::created_at)
                    .unwrap_or(delta.created_at);
                TrackedStateMutation::put_encoded(
                    encode_key_ref(TrackedStateKeyRef {
                        schema_key: &key.schema_key,
                        file_id: key.file_id.as_deref(),
                        entity_pk: &key.entity_pk,
                    }),
                    encode_value_ref(TrackedStateIndexValueRef {
                        change_id: delta.change_id,
                        commit_id: delta.commit_id.unwrap_or_default(),
                        deleted: delta.deleted,
                        created_at,
                        updated_at: delta.updated_at,
                    }),
                )
            })
            .collect::<Vec<_>>();
        let changed_rows = mutations.len();
        let result = self
            .tree
            .apply_mutations_with_overlay(
                self.store,
                self.writes,
                &mut self.chunk_overlay,
                base_root.as_ref(),
                mutations,
                None,
            )
            .await?;
        stage_branch_root(self.writes, branch_id, &result.root_id)?;
        self.staged_roots
            .insert(branch_id.to_string(), result.root_id.clone());
        Ok(LiveStateIndexWriteReport {
            branch_id: branch_id.to_string(),
            root_id: result.root_id,
            changed_rows,
        })
    }
}

fn materialize_index_entry(
    branch_id: &str,
    key: TrackedStateKey,
    value: TrackedStateIndexValue,
) -> LiveStateIndexRow {
    let commit_id = (!value.commit_id.as_uuid().is_nil()).then_some(value.commit_id);
    LiveStateIndexRow {
        branch_id: branch_id.to_string(),
        schema_key: key.schema_key,
        file_id: key.file_id,
        entity_pk: key.entity_pk,
        change_id: value.change_id,
        commit_id,
        deleted: value.deleted,
        created_at: value.created_at(),
        updated_at: value.updated_at(),
    }
}

fn materialize_current_row(
    branch_id: &str,
    row: crate::tracked_state::MaterializedTrackedStateRow,
) -> MaterializedLiveStateIndexRow {
    let commit_id = (!row.commit_id.as_uuid().is_nil()).then_some(row.commit_id);
    MaterializedLiveStateIndexRow {
        branch_id: branch_id.to_string(),
        schema_key: row.schema_key,
        file_id: row.file_id,
        entity_pk: row.entity_pk,
        snapshot_content: row.snapshot_content,
        metadata: row.metadata,
        deleted: row.deleted,
        created_at: row.created_at,
        updated_at: row.updated_at,
        change_id: row.change_id,
        commit_id,
        untracked: commit_id.is_none(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{
        ChangeId, ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogWriter, CommitId,
    };
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;
    use crate::live_state::index::{
        LiveStateIndexFilter, LiveStateIndexRowRequest, LiveStateIndexScanRequest,
    };
    use crate::storage::{
        InMemoryStorageBackend, StorageContext, StorageReadOptions, StorageWriteOptions,
    };

    fn timestamp(value: &str) -> LixTimestamp {
        LixTimestamp::expect_parse("test timestamp", value)
    }

    fn delta<'a>(
        entity_pk: &'a EntityPk,
        change_label: &str,
        commit_label: Option<&str>,
        deleted: bool,
        created_at: &'a str,
        updated_at: &'a str,
    ) -> LiveStateIndexDeltaRef<'a> {
        LiveStateIndexDeltaRef {
            schema_key: "test_schema",
            file_id: None,
            entity_pk,
            change_id: ChangeId::for_test_label(change_label),
            commit_id: commit_label.map(CommitId::for_test_label),
            deleted,
            created_at: timestamp(created_at),
            updated_at: timestamp(updated_at),
        }
    }

    async fn stage_and_commit(
        storage: &StorageContext,
        branch_id: &str,
        deltas: Vec<LiveStateIndexDeltaRef<'_>>,
    ) -> LiveStateIndexWriteReport {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let report = LiveStateIndexContext::new()
            .writer(&read, &mut writes)
            .stage_branch_rows(branch_id, deltas)
            .await
            .expect("current rows should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("current rows should commit");
        report
    }

    fn row_request(branch_id: &str, entity_pk: &EntityPk) -> LiveStateIndexRowRequest {
        LiveStateIndexRowRequest {
            branch_id: branch_id.to_string(),
            schema_key: "test_schema".to_string(),
            entity_pk: entity_pk.clone(),
            file_id: None,
        }
    }

    #[tokio::test]
    async fn tracked_and_untracked_rows_share_one_current_index() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tracked_pk = EntityPk::single("tracked");
        let untracked_pk = EntityPk::single("untracked");
        stage_and_commit(
            &storage,
            "branch-a",
            vec![
                delta(
                    &tracked_pk,
                    "tracked-change",
                    Some("tracked-commit"),
                    false,
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:00:00Z",
                ),
                delta(
                    &untracked_pk,
                    "untracked-change",
                    None,
                    false,
                    "2026-01-01T00:00:01Z",
                    "2026-01-01T00:00:01Z",
                ),
            ],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let reader = LiveStateIndexContext::new().reader(read);
        let rows = reader
            .scan_rows(&LiveStateIndexScanRequest {
                branch_id: "branch-a".to_string(),
                filter: LiveStateIndexFilter::default(),
                projection: vec!["change_id".to_string()],
                limit: None,
            })
            .await
            .expect("current rows should scan");
        assert_eq!(rows.len(), 2);
        let tracked = rows
            .iter()
            .find(|row| row.entity_pk == tracked_pk)
            .expect("tracked row should exist");
        assert_eq!(
            tracked.commit_id,
            Some(CommitId::for_test_label("tracked-commit"))
        );
        assert!(!tracked.untracked);
        let untracked = rows
            .iter()
            .find(|row| row.entity_pk == untracked_pk)
            .expect("untracked row should exist");
        assert_eq!(untracked.commit_id, None);
        assert!(untracked.untracked);
        assert_ne!(tracked.change_id, untracked.change_id);
    }

    #[tokio::test]
    async fn tombstone_stays_in_index_but_default_scan_hides_it() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let entity_pk = EntityPk::single("deleted");
        stage_and_commit(
            &storage,
            "branch-a",
            vec![delta(
                &entity_pk,
                "delete-change",
                None,
                true,
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
            )],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let reader = LiveStateIndexContext::new().reader(read);
        let default_rows = reader
            .scan_rows(&LiveStateIndexScanRequest {
                branch_id: "branch-a".to_string(),
                filter: LiveStateIndexFilter::default(),
                projection: Vec::new(),
                limit: None,
            })
            .await
            .expect("current rows should scan");
        assert!(default_rows.is_empty());

        let tombstone = reader
            .load_row(&row_request("branch-a", &entity_pk))
            .await
            .expect("tombstone should load")
            .expect("tombstone should remain indexed");
        assert!(tombstone.deleted);
        assert!(tombstone.untracked);

        let rows_with_tombstones = reader
            .scan_rows(&LiveStateIndexScanRequest {
                branch_id: "branch-a".to_string(),
                filter: LiveStateIndexFilter {
                    include_tombstones: true,
                    ..LiveStateIndexFilter::default()
                },
                projection: Vec::new(),
                limit: None,
            })
            .await
            .expect("tombstones should scan");
        assert_eq!(rows_with_tombstones, vec![tombstone]);
    }

    #[tokio::test]
    async fn overwrite_creates_a_new_root_and_replaces_the_current_entry() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let entity_pk = EntityPk::single("entity");
        let first = stage_and_commit(
            &storage,
            "branch-a",
            vec![delta(
                &entity_pk,
                "change-1",
                None,
                false,
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
            )],
        )
        .await;
        let second = stage_and_commit(
            &storage,
            "branch-a",
            vec![delta(
                &entity_pk,
                "change-2",
                Some("commit-2"),
                false,
                "2026-01-02T00:00:00Z",
                "2026-01-02T00:00:00Z",
            )],
        )
        .await;
        assert_ne!(first.root_id, second.root_id);
        assert_eq!(second.changed_rows, 1);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let reader = LiveStateIndexContext::new().reader(read);
        assert_eq!(
            reader
                .load_branch_root("branch-a")
                .expect("current root should load"),
            Some(second.root_id.clone())
        );
        let row = reader
            .load_index_row(&row_request("branch-a", &entity_pk))
            .await
            .expect("row should load")
            .expect("row should exist");
        assert_eq!(row.change_id, ChangeId::for_test_label("change-2"));
        assert_eq!(row.commit_id, Some(CommitId::for_test_label("commit-2")));
        assert_eq!(row.created_at, timestamp("2026-01-01T00:00:00Z"));
        assert_eq!(row.updated_at, timestamp("2026-01-02T00:00:00Z"));
    }

    #[tokio::test]
    async fn branch_roots_keep_same_identity_separate() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let entity_pk = EntityPk::single("shared");
        let branch_a = stage_and_commit(
            &storage,
            "branch-a",
            vec![delta(
                &entity_pk,
                "change-a",
                None,
                false,
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
            )],
        )
        .await;
        let branch_b = stage_and_commit(
            &storage,
            "branch-b",
            vec![delta(
                &entity_pk,
                "change-b",
                Some("commit-b"),
                false,
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
            )],
        )
        .await;
        assert_ne!(branch_a.root_id, branch_b.root_id);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let reader = LiveStateIndexContext::new().reader(read);
        let row_a = reader
            .load_index_row(&row_request("branch-a", &entity_pk))
            .await
            .expect("branch-a row should load")
            .expect("branch-a row should exist");
        let row_b = reader
            .load_index_row(&row_request("branch-b", &entity_pk))
            .await
            .expect("branch-b row should load")
            .expect("branch-b row should exist");
        assert_eq!(row_a.change_id, ChangeId::for_test_label("change-a"));
        assert_eq!(row_b.change_id, ChangeId::for_test_label("change-b"));
        assert!(row_a.untracked());
        assert!(!row_b.untracked());
    }

    #[tokio::test]
    async fn materialized_row_hydrates_payloads_from_the_change_ledger() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let entity_pk = EntityPk::single("hydrated");
        let change_id = ChangeId::for_test_label("hydrated-change");
        let created_at = timestamp("2026-01-01T00:00:00Z");
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        ChangelogContext::new()
            .writer(&mut read, &mut writes)
            .stage_append(ChangelogAppend {
                commits: Vec::new(),
                changes: vec![ChangeRecord {
                    format_version: 2,
                    change_id,
                    schema_key: "test_schema".to_string(),
                    entity_pk: entity_pk.clone(),
                    file_id: None,
                    snapshot: crate::json_store::JsonSlot::from_json(r#"{"value":1}"#),
                    metadata: crate::json_store::JsonSlot::from_json(r#"{"tag":"current"}"#),
                    created_at,
                    origin_key: None,
                }],
                commit_change_refs: Vec::new(),
            })
            .await
            .expect("change should stage");
        LiveStateIndexContext::new()
            .writer(&read, &mut writes)
            .stage_branch_rows(
                "branch-a",
                [LiveStateIndexDeltaRef {
                    schema_key: "test_schema",
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id,
                    commit_id: None,
                    deleted: false,
                    created_at,
                    updated_at: created_at,
                }],
            )
            .await
            .expect("current row should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("current row and change should commit atomically");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let row = LiveStateIndexContext::new()
            .reader(read)
            .load_row(&row_request("branch-a", &entity_pk))
            .await
            .expect("current row should load")
            .expect("current row should exist");
        assert_eq!(row.snapshot_content.as_deref(), Some(r#"{"value":1}"#));
        assert_eq!(row.metadata.as_deref(), Some(r#"{"tag":"current"}"#));
        assert_eq!(row.change_id, change_id);
        assert!(row.untracked);
    }

    #[tokio::test]
    async fn branch_pointer_can_share_an_existing_root_without_copying_rows() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let entity_pk = EntityPk::single("shared-root");
        let source = stage_and_commit(
            &storage,
            "branch-a",
            vec![delta(
                &entity_pk,
                "shared-root-change",
                Some("shared-root-commit"),
                false,
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
            )],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let report = LiveStateIndexContext::new()
            .writer(&read, &mut writes)
            .stage_branch_root_from_existing("branch-b", &source.root_id)
            .expect("existing root should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("branch pointer should commit");
        assert_eq!(report.changed_rows, 0);
        assert_eq!(report.root_id, source.root_id);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let row = LiveStateIndexContext::new()
            .reader(read)
            .load_index_row(&row_request("branch-b", &entity_pk))
            .await
            .expect("shared-root row should load")
            .expect("shared-root row should exist");
        assert_eq!(
            row.change_id,
            ChangeId::for_test_label("shared-root-change")
        );

        let overlay_pk = EntityPk::single("overlay");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        LiveStateIndexContext::new()
            .writer(&read, &mut writes)
            .stage_branch_rows_from_existing_root(
                "branch-c",
                &source.root_id,
                [delta(
                    &overlay_pk,
                    "overlay-change",
                    None,
                    false,
                    "2026-01-01T00:00:01Z",
                    "2026-01-01T00:00:01Z",
                )],
            )
            .await
            .expect("overlay on existing root should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("overlay branch pointer should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let reader = LiveStateIndexContext::new().reader(read);
        assert!(
            reader
                .load_index_row(&row_request("branch-c", &entity_pk))
                .await
                .expect("base row should load")
                .is_some()
        );
        let current = reader
            .load_index_row(&row_request("branch-c", &overlay_pk))
            .await
            .expect("current row should load")
            .expect("current row should exist");
        assert_eq!(
            current.change_id,
            ChangeId::for_test_label("overlay-change")
        );
        assert!(current.untracked());
    }
}
