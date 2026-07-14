use std::collections::{BTreeMap, BTreeSet};

use crate::LixError;
use crate::changelog::{ChangeId, ChangeRecordProjection, materialize_change_payloads};
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};

use super::storage::{
    FlatIdentity, FlatValue, LIVE_STATE_INDEX_ROW_SPACE, load_value, load_values, scan_values,
    stage_delete, stage_put,
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
        S: StorageAdapterRead,
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
        S: StorageAdapterRead + ?Sized,
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
    S: StorageAdapterRead,
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

    pub(crate) async fn scan_index_rows(
        &self,
        request: &LiveStateIndexScanRequest,
    ) -> Result<Vec<LiveStateIndexRow>, LixError> {
        Ok(scan_values(
            &self.store,
            &request.branch_id,
            &request.filter,
            request.limit,
        )
        .await?
        .into_iter()
        .map(|(identity, value)| index_row(identity, value))
        .collect())
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

impl<S> LiveStateIndexWriter<'_, S>
where
    S: StorageAdapterRead + ?Sized,
{
    pub(crate) async fn stage_branch_rows<'a, I>(
        &mut self,
        branch_id: &str,
        deltas: I,
    ) -> Result<Vec<ChangeId>, LixError>
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
    ) -> Result<Vec<ChangeId>, LixError>
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
        let delete_count = final_deltas
            .values()
            .filter(|delta| delta.commit_id.is_some() || delta.deleted)
            .count();
        self.writes.reserve_space(
            LIVE_STATE_INDEX_ROW_SPACE,
            final_deltas.len() - delete_count,
            delete_count,
        );

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

        Ok(superseded.into_iter().collect())
    }
}

async fn materialize_entries<S>(
    store: &S,
    entries: Vec<(FlatIdentity, FlatValue)>,
    projection: &[String],
) -> Result<Vec<MaterializedLiveStateIndexRow>, LixError>
where
    S: StorageAdapterRead,
{
    let materialization = ChangeRecordProjection::from_columns(projection);
    let mut payloads = materialize_change_payloads(
        store,
        entries.iter().map(|(_, value)| value.change_id),
        materialization,
        "flat live-state entry",
    )
    .await?;
    entries
        .into_iter()
        .map(|(identity, value)| {
            let payload = payloads.remove(&value.change_id).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "flat live-state entry references ChangeRecord '{}' that was not materialized",
                        value.change_id
                    ),
                )
            })?;
            if let Some(change_identity) = payload.identity.as_ref() {
                validate_change_identity(&identity, value.change_id, change_identity)?;
            }
            Ok(MaterializedLiveStateIndexRow {
                branch_id: identity.branch_id,
                schema_key: identity.schema_key,
                file_id: identity.file_id,
                entity_pk: identity.entity_pk,
                snapshot_content: payload.snapshot_content,
                metadata: payload.metadata,
                created_at: value.created_at.to_string(),
                updated_at: value.updated_at.to_string(),
                change_id: value.change_id,
            })
        })
        .collect()
}

fn validate_change_identity(
    index_identity: &FlatIdentity,
    change_id: ChangeId,
    change_identity: &crate::changelog::MaterializedChangeIdentity,
) -> Result<(), LixError> {
    if change_identity.schema_key == index_identity.schema_key
        && change_identity.entity_pk == index_identity.entity_pk
        && change_identity.file_id == index_identity.file_id
    {
        return Ok(());
    }
    Err(LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "flat live-state entry identity does not match referenced ChangeRecord '{change_id}'"
        ),
    ))
}

fn index_row(identity: FlatIdentity, value: FlatValue) -> LiveStateIndexRow {
    LiveStateIndexRow {
        branch_id: identity.branch_id,
        schema_key: identity.schema_key,
        file_id: identity.file_id,
        entity_pk: identity.entity_pk,
        change_id: value.change_id,
        created_at: value.created_at,
        updated_at: value.updated_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    fn ts(value: &str) -> crate::common::LixTimestamp {
        crate::common::LixTimestamp::expect_parse("test timestamp", value)
    }

    fn request() -> LiveStateIndexRowRequest {
        LiveStateIndexRowRequest {
            branch_id: "branch-a".to_string(),
            schema_key: "schema".to_string(),
            entity_pk: crate::entity_pk::EntityPk::single("entity"),
            file_id: None,
        }
    }

    #[tokio::test]
    async fn replacement_preserves_created_at_and_reports_superseded_change() {
        let storage = StorageAdapter::new(Memory::new());
        let identity = FlatIdentity::from_request(&request());
        let old_change = ChangeId::for_test_label("old");
        let new_change = ChangeId::for_test_label("new");
        let mut writes = StorageWriteSet::new();
        stage_put(
            &mut writes,
            &identity,
            &FlatValue {
                change_id: old_change,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
            },
        )
        .expect("seed should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("seed should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = StorageWriteSet::new();
        let superseded = LiveStateIndexContext::new()
            .writer(&read, &mut writes)
            .stage_branch_rows(
                "branch-a",
                [LiveStateIndexDeltaRef {
                    schema_key: "schema",
                    file_id: None,
                    entity_pk: &identity.entity_pk,
                    change_id: new_change,
                    commit_id: None,
                    deleted: false,
                    created_at: ts("2026-01-02T00:00:00Z"),
                    updated_at: ts("2026-01-02T00:00:00Z"),
                }],
            )
            .await
            .expect("replacement should stage");
        assert_eq!(superseded, vec![old_change]);
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("replacement should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should reopen");
        let value = load_value(&read, &identity)
            .await
            .expect("value should load")
            .expect("value should exist");
        assert_eq!(value.change_id, new_change);
        assert_eq!(value.created_at, ts("2026-01-01T00:00:00Z"));
        assert_eq!(value.updated_at, ts("2026-01-02T00:00:00Z"));
    }

    #[tokio::test]
    async fn missing_change_record_has_flat_index_error() {
        let storage = StorageAdapter::new(Memory::new());
        let identity = FlatIdentity::from_request(&request());
        let mut writes = StorageWriteSet::new();
        stage_put(
            &mut writes,
            &identity,
            &FlatValue {
                change_id: ChangeId::for_test_label("missing"),
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
            },
        )
        .expect("row should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("row should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = LiveStateIndexContext::new()
            .reader(&read)
            .load_row(&request())
            .await
            .expect_err("missing ChangeRecord should fail");
        assert!(error.message.contains("flat live-state entry"));
        assert!(error.message.contains("missing from the changelog"));
    }

    #[test]
    fn mismatched_change_record_identity_is_rejected() {
        let identity = FlatIdentity::from_request(&request());
        let error = validate_change_identity(
            &identity,
            ChangeId::for_test_label("wrong-identity"),
            &crate::changelog::MaterializedChangeIdentity {
                schema_key: "other-schema".to_string(),
                entity_pk: identity.entity_pk.clone(),
                file_id: identity.file_id.clone(),
            },
        )
        .expect_err("mismatched identity should fail");

        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
        assert!(error.message.contains("identity does not match"));
    }
}
