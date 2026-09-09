#[cfg(feature = "storage-benches")]
use std::collections::HashMap;

use crate::LixError;
use crate::branch::BranchHeadControlContext;
use crate::changelog::CommitId;
#[cfg(feature = "storage-benches")]
use crate::changelog::{ChangelogContext, ChangelogReader, CommitScanRequest};
#[cfg(test)]
use crate::commit_graph::CommitGraphContext;
#[cfg(feature = "storage-benches")]
use crate::commit_graph::CommitGraphNode;
use crate::storage_adapter::StorageAdapterRead;

pub(crate) const CHECKPOINT_SCHEMA_KEY: &str = "lix_checkpoint";

#[cfg(feature = "storage-benches")]
const CHECKPOINT_RECORD_SCAN_PAGE_SIZE: usize = 1_024;

#[cfg(feature = "storage-benches")]
pub(crate) type CheckpointCommitRecords = HashMap<CommitId, CommitGraphNode>;

/// Loads the private compaction cursor bound to an exact branch head.
///
/// Checkpoints are immutable commit metadata. Branch-relative working-diff
/// baselines are control-plane state and must never be reconstructed by
/// searching checkpoint history.
pub(crate) async fn checkpoint_commit_id_at_head<S>(
    store: S,
    branch_id: &str,
    head_commit_id: CommitId,
) -> Result<CommitId, LixError>
where
    S: StorageAdapterRead,
{
    let control = BranchHeadControlContext::new()
        .reader(store)
        .load(branch_id)
        .await?
        .ok_or_else(|| LixError::branch_not_found(branch_id, "load checkpoint cursor", "branch"))?;
    if control.head_commit_id != head_commit_id {
        return Err(LixError::new(
            LixError::CODE_TRANSACTION_CONFLICT,
            format!("branch '{branch_id}' head changed while loading its checkpoint cursor"),
        ));
    }
    control.working_diff_checkpoint_commit_id.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("branch '{branch_id}' has no checkpoint cursor"),
        )
    })
}

#[cfg(feature = "storage-benches")]
pub(crate) async fn scan_checkpoint_commit_records<S>(
    store: S,
) -> Result<CheckpointCommitRecords, LixError>
where
    S: StorageAdapterRead,
{
    let mut reader = ChangelogContext::new().reader(store);
    let mut records = CheckpointCommitRecords::new();
    let mut start_after = None::<String>;

    loop {
        let batch = reader
            .scan_commits(CommitScanRequest {
                start_after: start_after.as_deref(),
                limit: Some(CHECKPOINT_RECORD_SCAN_PAGE_SIZE),
            })
            .await?;
        records.reserve(batch.entries.len());
        for record in batch.entries {
            records.insert(
                record.commit_id,
                CommitGraphNode {
                    is_checkpoint: record.is_checkpoint,
                    commit_id: record.commit_id,
                    change_id: record.change_id(),
                    account_id: record.account_id,
                    generation: record.generation,
                    parent_commit_ids: record.parent_commit_ids,
                    base_commit_id: record.base_commit_id,
                    first_parent_jump_commit_id: record.first_parent_jump_commit_id,
                    first_parent_jump_span: record.first_parent_jump_span,
                    created_at: record.created_at,
                    touched_scope_digest: record.touched_scope_digest,
                },
            );
        }
        let Some(next) = batch.next_start_after else {
            break;
        };
        start_after = Some(next.to_string());
    }

    Ok(records)
}

pub(crate) use crate::changelog::CHECKPOINT_INVENTORY_SPACE;

/// Bounded inventory page, validated against immutable commit authority.
pub(crate) async fn checkpoint_commit_page<S>(
    store: S,
    after: Option<CommitId>,
    limit: usize,
) -> Result<(Vec<crate::changelog::CommitRecord>, Option<CommitId>), LixError>
where
    S: StorageAdapterRead,
{
    use crate::changelog::{ChangelogContext, ChangelogReader, CommitLoadRequest};
    use crate::storage_adapter::{StorageBeginScanOptions, StorageKey, StoragePrefix};
    let mut range = StoragePrefix {
        bytes: bytes::Bytes::new(),
    }
    .to_range()?;
    if let Some(after) = after {
        range.lower = std::ops::Bound::Excluded(StorageKey(bytes::Bytes::copy_from_slice(
            after.as_uuid().as_bytes(),
        )));
    }
    let mut cursor = store
        .begin_scan(
            CHECKPOINT_INVENTORY_SPACE,
            range,
            StorageBeginScanOptions::default(),
        )
        .await?;
    let (page, more) = cursor.next_page(limit).await?.into_parts();
    let ids = page
        .iter()
        .map(|entry| {
            uuid::Uuid::from_slice(&entry.key.0)
                .map(CommitId::new)
                .map_err(|_| LixError::unknown("invalid checkpoint inventory key"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    drop(cursor);
    let mut reader = ChangelogContext::new().reader(store);
    let records = reader
        .load_commits(CommitLoadRequest { commit_ids: &ids })
        .await?;
    let mut result = Vec::with_capacity(ids.len());
    for (id, record) in records.iter() {
        let record = record.ok_or_else(|| {
            LixError::unknown(format!("checkpoint inventory commit '{id}' is missing"))
        })?;
        if !record.is_checkpoint {
            return Err(LixError::unknown(format!(
                "checkpoint inventory commit '{id}' is unmarked"
            )));
        }
        result.push(record.clone());
    }
    Ok((result, more.then(|| ids.last().copied()).flatten()))
}

pub(crate) async fn checkpoint_commit_ids<S>(
    store: S,
) -> Result<std::collections::BTreeSet<CommitId>, LixError>
where
    S: StorageAdapterRead + Clone,
{
    let mut result = std::collections::BTreeSet::new();
    let mut after = None;
    loop {
        let (page, next) = checkpoint_commit_page(store.clone(), after, 1024).await?;
        result.extend(page.into_iter().map(|commit| commit.commit_id));
        let Some(next) = next else {
            break;
        };
        after = Some(next);
    }
    Ok(result)
}

#[cfg(test)]
mod metadata_tests {
    use super::*;
    use crate::storage_adapter::StorageReadOptions;

    #[tokio::test]
    async fn checkpoint_flags_inventory_and_partial_remainder_are_atomic() {
        let lix = crate::open_lix().await.expect("open repository");
        lix.execute("INSERT INTO lix_key_value (key, value) VALUES ('selected', 'one'), ('remaining', 'two')", &[])
            .await.expect("write two rows");
        let selected = lix.execute(
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value') WHERE key = 'selected'))", &[])
            .await.expect("partial checkpoint").rows()[0].get::<String>("commit_id").unwrap();
        let head = lix
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        assert_ne!(selected, head);
        let adapter = lix.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let mut graph = CommitGraphContext::new().reader(&read);
        let selected_id = CommitId::parse_lix(&selected, "selected").unwrap();
        let head_id = CommitId::parse_lix(&head, "head").unwrap();
        assert!(
            graph
                .load_node(&selected_id)
                .await
                .unwrap()
                .unwrap()
                .is_checkpoint
        );
        let head_node = graph.load_node(&head_id).await.unwrap().unwrap();
        assert!(!head_node.is_checkpoint);
        assert_eq!(head_node.parent_commit_ids.first(), Some(&selected_id));
        let (page, next) = checkpoint_commit_page(&read, None, 1).await.unwrap();
        assert_eq!(
            page.iter()
                .map(|record| record.commit_id)
                .collect::<Vec<_>>(),
            vec![selected_id]
        );
        assert_eq!(next, None);
        drop(graph);
        drop(read);
        let full = lix.create_checkpoint().await.unwrap().commit_id;
        let empty = lix.create_checkpoint().await.unwrap().commit_id;
        assert_ne!(full, empty, "empty checkpoint is a new immutable commit");
        lix.execute(
            "INSERT INTO lix_restore (commit_id) VALUES ($1)",
            &[crate::Value::Text(selected)],
        )
        .await
        .unwrap();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let inventory = checkpoint_commit_ids(&read).await.unwrap();
        assert_eq!(
            inventory.len(),
            3,
            "abandoned checkpoints remain globally retained"
        );
        let mut writes = adapter.new_write_set();
        let plan = crate::gc::stage_repository_gc(&read, &mut writes)
            .await
            .unwrap();
        for checkpoint in &inventory {
            assert!(
                !plan.sweep.tracked_commit_roots.contains(checkpoint),
                "checkpoint state must survive GC"
            );
        }
    }
}
