use std::collections::{BTreeMap, BTreeSet};

use crate::LixError;
use crate::changelog::ChangeId;
use crate::storage::{StorageRead, StorageWriteSet};
use crate::tracked_state::{
    TrackedRowMaterialization, TrackedStateIndexValue, TrackedStateKey,
    materialize_rows_from_index_entries,
};

use super::storage::{
    FlatIdentity, FlatValue, load_value, load_values, scan_values, stage_delete, stage_put,
};
use super::{
    LiveStateIndexDeltaRef, LiveStateIndexRow, LiveStateIndexRowRequest, LiveStateIndexScanRequest,
    MaterializedLiveStateIndexRow,
};

/// Flat mutable index for untracked and engine-owned current rows.
#[derive(Clone, Copy, Default)]
pub(crate) struct LiveStateIndexContext;

impl LiveStateIndexContext {
    pub(crate) fn new() -> Self {
        Self
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn reader<S>(&self, store: S) -> LiveStateIndexStoreReader<S>
    where
        S: StorageRead,
    {
        LiveStateIndexStoreReader { store }
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn writer<'a, S>(
        &'a self,
        store: &'a S,
        writes: &'a mut StorageWriteSet,
    ) -> LiveStateIndexWriter<'a, S>
    where
        S: StorageRead + ?Sized,
    {
        LiveStateIndexWriter {
            store,
            writes,
            staged: BTreeMap::new(),
        }
    }
}

pub(crate) struct LiveStateIndexStoreReader<S> {
    store: S,
}

impl<S> LiveStateIndexStoreReader<S>
where
    S: StorageRead,
{
    pub(crate) async fn scan_rows(
        &self,
        request: &LiveStateIndexScanRequest,
    ) -> Result<Vec<MaterializedLiveStateIndexRow>, LixError> {
        let entries = scan_values(
            &self.store,
            &request.branch_id,
            &request.filter,
            request.limit,
        )
        .await?;
        materialize_entries(&self.store, entries, &request.projection).await
    }

    pub(crate) async fn load_row(
        &self,
        request: &LiveStateIndexRowRequest,
    ) -> Result<Option<MaterializedLiveStateIndexRow>, LixError> {
        let identity = FlatIdentity::from_request(request);
        let Some(value) = load_value(&self.store, &identity).await? else {
            return Ok(None);
        };
        Ok(
            materialize_entries(&self.store, vec![(identity, value)], &[])
                .await?
                .pop(),
        )
    }

    pub(crate) async fn load_index_row(
        &self,
        request: &LiveStateIndexRowRequest,
    ) -> Result<Option<LiveStateIndexRow>, LixError> {
        Ok(self
            .load_index_rows(std::slice::from_ref(request))
            .await?
            .pop()
            .flatten())
    }

    pub(crate) async fn load_index_rows(
        &self,
        requests: &[LiveStateIndexRowRequest],
    ) -> Result<Vec<Option<LiveStateIndexRow>>, LixError> {
        let identities = requests
            .iter()
            .map(FlatIdentity::from_request)
            .collect::<Vec<_>>();
        let values = load_values(&self.store, &identities).await?;
        Ok(identities
            .into_iter()
            .zip(values)
            .map(|(identity, value)| value.map(|value| index_row(identity, value)))
            .collect())
    }
}

pub(crate) struct LiveStateIndexWriter<'a, S: ?Sized> {
    store: &'a S,
    writes: &'a mut StorageWriteSet,
    staged: BTreeMap<FlatIdentity, Option<FlatValue>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveStateIndexWriteReport {
    pub(crate) branch_id: String,
    pub(crate) changed_rows: usize,
    pub(crate) superseded_untracked_change_ids: Vec<ChangeId>,
}

impl<S> LiveStateIndexWriter<'_, S>
where
    S: StorageRead + ?Sized,
{
    pub(crate) async fn stage_branch_rows<'a, I>(
        &mut self,
        branch_id: &str,
        deltas: I,
    ) -> Result<LiveStateIndexWriteReport, LixError>
    where
        I: IntoIterator<Item = LiveStateIndexDeltaRef<'a>>,
    {
        self.stage_branch_rows_with_known_absent(branch_id, deltas, &[])
            .await
    }

    pub(crate) async fn stage_branch_rows_with_known_absent<'a, I>(
        &mut self,
        branch_id: &str,
        deltas: I,
        known_absent: &[LiveStateIndexRowRequest],
    ) -> Result<LiveStateIndexWriteReport, LixError>
    where
        I: IntoIterator<Item = LiveStateIndexDeltaRef<'a>>,
    {
        let known_absent = known_absent
            .iter()
            .map(FlatIdentity::from_request)
            .collect::<BTreeSet<_>>();
        let mut final_deltas = BTreeMap::<FlatIdentity, LiveStateIndexDeltaRef<'a>>::new();
        for delta in deltas {
            final_deltas.insert(
                FlatIdentity {
                    branch_id: branch_id.to_string(),
                    schema_key: delta.schema_key.to_string(),
                    entity_pk: delta.entity_pk.clone(),
                    file_id: delta.file_id.map(str::to_string),
                },
                delta,
            );
        }

        let identities_to_load = final_deltas
            .keys()
            .filter(|identity| {
                !known_absent.contains(*identity) && !self.staged.contains_key(*identity)
            })
            .cloned()
            .collect::<Vec<_>>();
        let loaded = load_values(self.store, &identities_to_load).await?;
        let mut prior = identities_to_load
            .into_iter()
            .zip(loaded)
            .collect::<BTreeMap<_, _>>();
        let mut superseded = BTreeSet::new();

        for (identity, delta) in final_deltas {
            let previous = self
                .staged
                .get(&identity)
                .cloned()
                .unwrap_or_else(|| prior.remove(&identity).flatten());
            if let Some(previous) = previous.as_ref() {
                superseded.insert(previous.change_id);
            }

            // Tracked rows live in commit roots. Applying one here only clears
            // a prior untracked row during an internal durability promotion.
            if delta.commit_id.is_some() || delta.deleted {
                if previous.is_some() {
                    stage_delete(self.writes, &identity)?;
                }
                self.staged.insert(identity, None);
                continue;
            }

            let value = FlatValue {
                change_id: delta.change_id,
                created_at: previous
                    .as_ref()
                    .map_or(delta.created_at, |value| value.created_at),
                updated_at: delta.updated_at,
            };
            stage_put(self.writes, &identity, &value)?;
            self.staged.insert(identity, Some(value));
        }

        Ok(LiveStateIndexWriteReport {
            branch_id: branch_id.to_string(),
            changed_rows: self.staged.len(),
            superseded_untracked_change_ids: superseded.into_iter().collect(),
        })
    }
}

async fn materialize_entries<S>(
    store: &S,
    entries: Vec<(FlatIdentity, FlatValue)>,
    projection: &[String],
) -> Result<Vec<MaterializedLiveStateIndexRow>, LixError>
where
    S: StorageRead,
{
    let mut branch_ids = Vec::with_capacity(entries.len());
    let mut tracked_entries = Vec::with_capacity(entries.len());
    for (identity, value) in entries {
        branch_ids.push(identity.branch_id);
        tracked_entries.push((
            TrackedStateKey {
                schema_key: identity.schema_key,
                file_id: identity.file_id,
                entity_pk: identity.entity_pk,
            },
            TrackedStateIndexValue {
                change_id: value.change_id,
                commit_id: crate::changelog::CommitId::new(uuid::Uuid::nil()),
                deleted: false,
                created_at: value.created_at,
                updated_at: value.updated_at,
            },
        ));
    }
    let rows = materialize_rows_from_index_entries(
        store,
        tracked_entries,
        &TrackedRowMaterialization::from_columns(projection),
    )
    .await?;
    Ok(branch_ids
        .into_iter()
        .zip(rows)
        .map(|(branch_id, row)| MaterializedLiveStateIndexRow {
            branch_id,
            schema_key: row.schema_key,
            file_id: row.file_id,
            entity_pk: row.entity_pk,
            snapshot_content: row.snapshot_content,
            metadata: row.metadata,
            deleted: false,
            created_at: row.created_at,
            updated_at: row.updated_at,
            change_id: row.change_id,
            commit_id: None,
            untracked: true,
        })
        .collect())
}

fn index_row(identity: FlatIdentity, value: FlatValue) -> LiveStateIndexRow {
    LiveStateIndexRow {
        branch_id: identity.branch_id,
        schema_key: identity.schema_key,
        file_id: identity.file_id,
        entity_pk: identity.entity_pk,
        change_id: value.change_id,
        commit_id: None,
        deleted: false,
        created_at: value.created_at,
        updated_at: value.updated_at,
    }
}
