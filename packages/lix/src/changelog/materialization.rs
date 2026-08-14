use std::collections::{HashMap, HashSet};

use crate::LixError;
use crate::common::SharedStr;
use crate::forktree::ForkTreeReadFacade;
use crate::storage_adapter::StorageAdapterRead;

use super::{ChangeId, ChangeRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChangeRecordProjection {
    pub(crate) snapshot_content: bool,
    pub(crate) metadata: bool,
}

impl ChangeRecordProjection {
    pub(crate) fn full() -> Self {
        Self {
            snapshot_content: true,
            metadata: true,
        }
    }

    pub(crate) fn requires_payload(self) -> bool {
        self.snapshot_content || self.metadata
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedChangePayload {
    pub(crate) identity: Option<MaterializedChangeIdentity>,
    pub(crate) snapshot_content: Option<SharedStr>,
    pub(crate) metadata: Option<SharedStr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedChangeIdentity {
    pub(crate) schema_key: String,
    pub(crate) row_pk: crate::row_pk::RowPk,
    pub(crate) file_id: Option<String>,
}

/// Batched point read of change records by deduplicated change id.
pub(crate) async fn load_change_records<S>(
    store: &S,
    change_ids: impl Iterator<Item = ChangeId>,
) -> Result<HashMap<ChangeId, ChangeRecord>, LixError>
where
    S: StorageAdapterRead,
{
    let mut unique = Vec::new();
    let mut seen = HashSet::new();
    for change_id in change_ids {
        if seen.insert(change_id) {
            unique.push(change_id);
        }
    }
    if unique.is_empty() {
        return Ok(HashMap::new());
    }
    let records = crate::forktree::load_change_records(store, &unique).await?;
    let mut by_id = HashMap::with_capacity(unique.len());
    for (change_id, record) in unique.into_iter().zip(records) {
        if let Some(record) = record {
            by_id.insert(change_id, record);
        }
    }
    Ok(by_id)
}

/// Hydrates records that a caller already retained from their authoritative
/// storage owner. Packed commit deltas use this path so lifecycle and HOT
/// publication never discard commit-local payloads and fall back to the
/// standalone changelog namespace.
pub(crate) async fn materialize_known_change_payloads<S>(
    reader: &mut ForkTreeReadFacade<S>,
    changes: impl Iterator<Item = ChangeRecord>,
    projection: ChangeRecordProjection,
) -> Result<HashMap<ChangeId, MaterializedChangePayload>, LixError>
where
    S: StorageAdapterRead,
{
    Ok(
        materialize_known_change_payloads_in_order(reader, changes, projection)
            .await?
            .into_iter()
            .collect(),
    )
}

/// Hydrates retained change records without collapsing repeated change ids.
///
/// Eager plugin materialization can produce multiple semantic identitys from
/// one source change. Lifecycle operations such as checkpoints must preserve
/// each identity-specific payload even though those rows share a change id.
pub(crate) async fn materialize_known_change_payloads_in_order<S>(
    reader: &mut ForkTreeReadFacade<S>,
    changes: impl Iterator<Item = ChangeRecord>,
    projection: ChangeRecordProjection,
) -> Result<Vec<(ChangeId, MaterializedChangePayload)>, LixError>
where
    S: StorageAdapterRead,
{
    let changes = changes.collect::<Vec<_>>();
    if !projection.requires_payload() {
        return Ok(changes
            .into_iter()
            .map(|change| {
                (
                    change.change_id,
                    MaterializedChangePayload {
                        identity: None,
                        snapshot_content: None,
                        metadata: None,
                    },
                )
            })
            .collect());
    }

    let mut payloads = Vec::with_capacity(changes.len());
    for change in changes {
        let change_id = change.change_id;
        let snapshot_content = if projection.snapshot_content {
            reader
                .load_json_slot(&change.snapshot)
                .await?
                .map(SharedStr::from)
        } else {
            None
        };
        let metadata = if projection.metadata {
            reader
                .load_json_slot(&change.metadata)
                .await?
                .map(SharedStr::from)
        } else {
            None
        };
        payloads.push((
            change_id,
            MaterializedChangePayload {
                identity: Some(MaterializedChangeIdentity {
                    schema_key: change.schema_key,
                    row_pk: change.row_pk,
                    file_id: change.file_id,
                }),
                snapshot_content,
                metadata,
            },
        ));
    }
    Ok(payloads)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::ChangeLoadBatch;
    use crate::changelog::test_support::test_change_record;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions};

    #[test]
    fn decoded_change_records_preserve_unique_id_order_and_missing_slots() {
        let change_ids = [
            ChangeId::for_test_label("ordered-a"),
            ChangeId::for_test_label("ordered-missing"),
            ChangeId::for_test_label("ordered-c"),
        ];
        let mut alpha = test_change_record();
        alpha.change_id = change_ids[0];
        alpha.schema_key = "alpha".to_owned();
        let mut charlie = test_change_record();
        charlie.change_id = change_ids[2];
        charlie.schema_key = "charlie".to_owned();
        let records = ChangeLoadBatch::try_new(
            "ForkTree ChangeCatalog",
            &change_ids,
            vec![Some(alpha), None, Some(charlie)],
        )
        .expect("ordered change records should decode")
        .into_iter()
        .map(|(_, record)| record)
        .collect::<Vec<_>>();

        assert_eq!(records.len(), change_ids.len());
        let first = records[0].as_ref().expect("first record");
        assert_eq!(first.change_id, change_ids[0]);
        assert_eq!(first.schema_key, "alpha");
        assert!(records[1].is_none());
        let third = records[2].as_ref().expect("third record");
        assert_eq!(third.change_id, change_ids[2]);
        assert_eq!(third.schema_key, "charlie");
    }

    #[test]
    fn decoded_change_records_reject_storage_cardinality_mismatch() {
        let change_ids = [
            ChangeId::for_test_label("cardinality-a"),
            ChangeId::for_test_label("cardinality-b"),
        ];
        let error = ChangeLoadBatch::try_new("ForkTree ChangeCatalog", &change_ids, vec![None])
            .expect_err("short storage response must fail");

        assert!(
            error
                .to_string()
                .contains("returned 1 values for 2 requested keys")
        );
    }

    #[tokio::test]
    async fn ordered_materialization_preserves_repeated_change_ids() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = ForkTreeReadFacade::new(&read);
        let first = test_change_record();
        let mut second = first.clone();
        second.schema_key = "derived-row".to_owned();
        second.row_pk = crate::row_pk::RowPk::single("row-2");

        let payloads = materialize_known_change_payloads_in_order(
            &mut reader,
            [first.clone(), second.clone()].into_iter(),
            ChangeRecordProjection::full(),
        )
        .await
        .expect("repeated source ids should materialize");

        assert_eq!(payloads.len(), 2);
        assert_eq!(payloads[0].0, first.change_id);
        assert_eq!(payloads[1].0, first.change_id);
        assert_eq!(
            payloads[0]
                .1
                .identity
                .as_ref()
                .expect("first identity")
                .schema_key,
            first.schema_key
        );
        assert_eq!(
            payloads[1]
                .1
                .identity
                .as_ref()
                .expect("second identity")
                .schema_key,
            second.schema_key
        );
    }
}
