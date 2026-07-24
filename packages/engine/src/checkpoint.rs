use std::collections::{HashMap, HashSet};

use serde_json::json;

use crate::LixError;
use crate::changelog::{
    ChangeRecordProjection, ChangelogContext, ChangelogReader, CommitId, CommitLoadEntry,
    CommitProjection, CommitScanRequest,
};
use crate::commit_graph::{
    CommitGraphChangeHistoryRequest, CommitGraphCommitRecord, CommitGraphReader,
};
use crate::entity_pk::EntityPk;
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{TrackedStateKey, TrackedStateStoreReader};
use crate::transaction::types::{TransactionJson, TransactionWriteRow};

pub(crate) const CHECKPOINT_MARKER_SCHEMA_KEY: &str = "lix_checkpoint_marker";
const CHECKPOINT_RECORD_SCAN_PAGE_SIZE: usize = 1_024;

/// Record-only index used while materializing an unbounded checkpoint history.
///
/// Checkpoint commits form a first-parent chain, but following that chain with
/// point reads turns a K-checkpoint history into K serial storage requests. A
/// paged record scan trades that N+1 pattern for work linear in retained
/// commits, which is substantially cheaper for remote LSM-backed storage.
pub(crate) type CheckpointCommitRecords = HashMap<CommitId, CommitGraphCommitRecord>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckpointHistoryEntry {
    pub(crate) commit_id: CommitId,
    pub(crate) created_at: String,
    pub(crate) depth: u32,
}

pub(crate) fn checkpoint_marker_stage_row(branch_id: &str) -> TransactionWriteRow {
    TransactionWriteRow {
        entity_pk: Some(EntityPk::single(branch_id)),
        schema_key: CHECKPOINT_MARKER_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(json!({
            "branch_id": branch_id,
        }))),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: false,
        change_id: None,
        commit_id: None,
        untracked: false,
        branch_id: branch_id.to_string(),
    }
}

/// Loads the retained commit records in bounded scan pages for checkpoint history.
///
/// The SQL provider constructs this once for an unbounded checkpoint query and
/// shares it between branch heads. Bounded `LIMIT` queries retain the smaller
/// point-walk path below.
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
                projection: CommitProjection::Record,
            })
            .await?;
        records.reserve(batch.entries.len());
        for entry in batch.entries {
            let CommitLoadEntry::Record(record) = entry else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "checkpoint commit scan returned a non-record entry",
                ));
            };
            records.insert(
                record.commit_id,
                CommitGraphCommitRecord {
                    commit_id: record.commit_id,
                    parent_commit_ids: record.parent_commit_ids,
                    created_at: record.created_at,
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

/// Resolves the latest checkpoint with one tracked-state point read.
///
/// The marker's tracked index value carries the commit that most recently
/// changed it. Auto-commits do not touch the marker, so this remains stable
/// until the next checkpoint.
pub(crate) async fn latest_checkpoint_at_head<S>(
    tracked: &mut TrackedStateStoreReader<S>,
    head: &CommitId,
    branch_id: &str,
) -> Result<Option<CommitId>, LixError>
where
    S: StorageAdapterRead,
{
    let rows = tracked
        .load_projected_rows_at_commit(
            &head.to_string(),
            &[TrackedStateKey {
                schema_key: CHECKPOINT_MARKER_SCHEMA_KEY.to_string(),
                file_id: None,
                entity_pk: EntityPk::single(branch_id),
            }],
            &ChangeRecordProjection::from_columns(&["commit_id".to_string()]),
        )
        .await?;
    Ok(rows
        .into_iter()
        .next()
        .flatten()
        .filter(|row| !row.deleted)
        .map(|row| row.commit_id))
}

/// Resolves only the latest checkpoint, avoiding a checkpoint-chain walk.
pub(crate) async fn latest_checkpoint_for_branch<S>(
    reader: &mut dyn CommitGraphReader,
    tracked: &mut TrackedStateStoreReader<S>,
    head: &CommitId,
    branch_id: &str,
) -> Result<Option<CommitId>, LixError>
where
    S: StorageAdapterRead,
{
    if let Some(checkpoint_id) = latest_checkpoint_at_head(tracked, head, branch_id).await? {
        return Ok(Some(checkpoint_id));
    }
    Ok(checkpoint_history_from_head(reader, head)
        .await?
        .into_iter()
        .next()
        .map(|entry| entry.commit_id))
}

/// Returns checkpoint history, optionally using a page-scanned commit record map.
///
/// The branch marker is an O(1) anchor. Checkpoint commits directly parent the
/// previous checkpoint, so a supplied record map avoids one storage point read
/// per historical checkpoint. The map must originate from the same read
/// snapshot as `reader` and `tracked`.
pub(crate) async fn checkpoint_history_for_branch<S>(
    reader: &mut dyn CommitGraphReader,
    tracked: &mut TrackedStateStoreReader<S>,
    head: &CommitId,
    branch_id: &str,
    limit: Option<usize>,
    records: Option<&CheckpointCommitRecords>,
) -> Result<Vec<CheckpointHistoryEntry>, LixError>
where
    S: StorageAdapterRead,
{
    if limit == Some(0) {
        return Ok(Vec::new());
    }
    if let Some(checkpoint_id) = latest_checkpoint_at_head(tracked, head, branch_id).await? {
        if let Some(records) = records {
            if let Some(depth) = first_parent_distance_from_records(records, head, &checkpoint_id)?
            {
                return checkpoint_history_from_checkpoint_records(
                    records,
                    &checkpoint_id,
                    depth,
                    limit,
                );
            }
        } else if let Some(depth) = first_parent_distance(reader, head, &checkpoint_id).await? {
            return checkpoint_history_from_checkpoint(reader, &checkpoint_id, depth, limit).await;
        }
    }
    let mut checkpoints = checkpoint_history_from_head(reader, head).await?;
    if let Some(limit) = limit {
        checkpoints.truncate(limit);
    }
    Ok(checkpoints)
}

fn first_parent_distance_from_records(
    records: &CheckpointCommitRecords,
    head: &CommitId,
    ancestor: &CommitId,
) -> Result<Option<u32>, LixError> {
    let mut current = *head;
    let mut depth = 0_u32;
    let mut visited = HashSet::new();
    loop {
        if current == *ancestor {
            return Ok(Some(depth));
        }
        if !visited.insert(current) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "cycle encountered while finding the latest checkpoint",
            ));
        }
        let Some(commit) = records.get(&current) else {
            return Ok(None);
        };
        let Some(parent) = commit.parent_commit_ids.first().copied() else {
            return Ok(None);
        };
        current = parent;
        depth = depth.checked_add(1).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "checkpoint history depth overflow",
            )
        })?;
    }
}

async fn first_parent_distance(
    reader: &mut dyn CommitGraphReader,
    head: &CommitId,
    ancestor: &CommitId,
) -> Result<Option<u32>, LixError> {
    let mut current = *head;
    let mut depth = 0_u32;
    let mut visited = HashSet::new();
    loop {
        if current == *ancestor {
            return Ok(Some(depth));
        }
        if !visited.insert(current) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "cycle encountered while finding the latest checkpoint",
            ));
        }
        let Some(commit) = reader.load_commit_record(&current).await? else {
            return Ok(None);
        };
        let Some(parent) = commit.parent_commit_ids.first().copied() else {
            return Ok(None);
        };
        current = parent;
        depth = depth.checked_add(1).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "checkpoint history depth overflow",
            )
        })?;
    }
}

async fn checkpoint_history_from_checkpoint(
    reader: &mut dyn CommitGraphReader,
    checkpoint_id: &CommitId,
    initial_depth: u32,
    limit: Option<usize>,
) -> Result<Vec<CheckpointHistoryEntry>, LixError> {
    let mut checkpoints = Vec::new();
    let mut current = Some(*checkpoint_id);
    let mut depth = initial_depth;
    let mut visited = HashSet::new();
    while let Some(commit_id) = current {
        if limit.is_some_and(|limit| checkpoints.len() >= limit) {
            break;
        }
        if !visited.insert(commit_id) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "cycle encountered while walking checkpoint history",
            ));
        }
        let commit = reader
            .load_commit_record(&commit_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("checkpoint history references missing commit '{commit_id}'"),
                )
            })?;
        checkpoints.push(CheckpointHistoryEntry {
            commit_id,
            created_at: commit.created_at.to_string(),
            depth,
        });
        current = commit.parent_commit_ids.first().copied();
        depth = depth.checked_add(1).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "checkpoint history depth overflow",
            )
        })?;
    }
    Ok(checkpoints)
}

fn checkpoint_history_from_checkpoint_records(
    records: &CheckpointCommitRecords,
    checkpoint_id: &CommitId,
    initial_depth: u32,
    limit: Option<usize>,
) -> Result<Vec<CheckpointHistoryEntry>, LixError> {
    let mut checkpoints = Vec::new();
    let mut current = Some(*checkpoint_id);
    let mut depth = initial_depth;
    let mut visited = HashSet::new();
    while let Some(commit_id) = current {
        if limit.is_some_and(|limit| checkpoints.len() >= limit) {
            break;
        }
        if !visited.insert(commit_id) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "cycle encountered while walking checkpoint history",
            ));
        }
        let commit = records.get(&commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("checkpoint history references missing commit '{commit_id}'"),
            )
        })?;
        checkpoints.push(CheckpointHistoryEntry {
            commit_id,
            created_at: commit.created_at.to_string(),
            depth,
        });
        current = commit.parent_commit_ids.first().copied();
        depth = depth.checked_add(1).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "checkpoint history depth overflow",
            )
        })?;
    }
    Ok(checkpoints)
}

/// Returns checkpoints on the first-parent history of `head`, newest first.
///
/// The graph root is also an implicit checkpoint. That gives repositories
/// created before checkpoint markers existed the same useful baseline as new
/// repositories, whose initial commit carries an explicit marker.
pub(crate) async fn checkpoint_history_from_head(
    reader: &mut dyn CommitGraphReader,
    head: &CommitId,
) -> Result<Vec<CheckpointHistoryEntry>, LixError> {
    let marker_commits = reader
        .change_history_from_commit(
            head,
            &CommitGraphChangeHistoryRequest {
                schema_keys: vec![CHECKPOINT_MARKER_SCHEMA_KEY.to_string()],
                include_tombstones: true,
                ..CommitGraphChangeHistoryRequest::default()
            },
        )
        .await?
        .into_iter()
        .map(|entry| entry.observed_commit_id)
        .collect::<HashSet<_>>();

    let mut checkpoints = Vec::new();
    let mut current = Some(*head);
    let mut depth = 0_u32;
    let mut visited = HashSet::new();
    while let Some(commit_id) = current {
        if !visited.insert(commit_id) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "cycle encountered while walking checkpoint first-parent history",
            ));
        }
        let commit = reader.load_commit(&commit_id).await?.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("checkpoint history references missing commit '{commit_id}'"),
            )
        })?;
        let is_root = commit.parent_commit_ids.is_empty();
        if is_root || marker_commits.contains(&commit_id) {
            checkpoints.push(CheckpointHistoryEntry {
                commit_id,
                created_at: commit.canonical_change.created_at.to_string(),
                depth,
            });
        }
        current = commit.parent_commit_ids.first().copied();
        depth = depth.checked_add(1).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "checkpoint history depth overflow",
            )
        })?;
    }
    Ok(checkpoints)
}

#[cfg(test)]
mod tests {
    use super::{
        checkpoint_history_from_checkpoint_records, first_parent_distance_from_records,
        scan_checkpoint_commit_records,
    };
    use crate::changelog::{
        ChangeId, ChangelogAppend, ChangelogContext, ChangelogWriter, CommitChangeRefSet, CommitId,
        CommitRecord,
    };
    use crate::common::LixTimestamp;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    fn timestamp() -> LixTimestamp {
        LixTimestamp::expect_parse("checkpoint scan timestamp", "2026-07-24T00:00:00Z")
    }

    fn commit_record(id: CommitId, parent: Option<CommitId>) -> CommitRecord {
        CommitRecord {
            format_version: 1,
            commit_id: id,
            parent_commit_ids: parent.into_iter().collect(),
            change_id: ChangeId::for_test_label(&format!("{id}-change")),
            author_account_ids: Vec::new(),
            created_at: timestamp(),
        }
    }

    #[tokio::test]
    async fn checkpoint_record_scan_crosses_pages_and_preserves_first_parent_history() {
        let storage = StorageAdapter::new(Memory::new());
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mut append = ChangelogAppend::default();
        let mut parent = None;

        // 1,025 checkpoint records exceed the changelog's 1,024-record scan
        // page. Add two auto commits above the latest checkpoint to exercise
        // the map-backed depth calculation too.
        for index in 0..1_025 {
            let commit_id = CommitId::for_test_label(&format!("checkpoint-{index}"));
            append.commits.push(commit_record(commit_id, parent));
            append.commit_change_refs.push(CommitChangeRefSet {
                commit_id,
                entries: Vec::new(),
            });
            parent = Some(commit_id);
        }
        let latest_checkpoint = parent.expect("fixture should create checkpoints");
        let first_auto = CommitId::for_test_label("checkpoint-auto-1");
        let second_auto = CommitId::for_test_label("checkpoint-auto-2");
        append
            .commits
            .push(commit_record(first_auto, Some(latest_checkpoint)));
        append.commit_change_refs.push(CommitChangeRefSet {
            commit_id: first_auto,
            entries: Vec::new(),
        });
        append
            .commits
            .push(commit_record(second_auto, Some(first_auto)));
        append.commit_change_refs.push(CommitChangeRefSet {
            commit_id: second_auto,
            entries: Vec::new(),
        });

        ChangelogContext::new()
            .writer(&mut read, &mut writes)
            .stage_append(append)
            .await
            .expect("fixture append should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("fixture should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let records = scan_checkpoint_commit_records(read)
            .await
            .expect("record scan should succeed");
        assert_eq!(records.len(), 1_027);
        assert_eq!(
            first_parent_distance_from_records(&records, &second_auto, &latest_checkpoint)
                .expect("depth should resolve"),
            Some(2)
        );

        let history =
            checkpoint_history_from_checkpoint_records(&records, &latest_checkpoint, 2, None)
                .expect("checkpoint history should resolve");
        assert_eq!(history.len(), 1_025);
        assert_eq!(history[0].commit_id, latest_checkpoint);
        assert_eq!(history[0].depth, 2);
        assert_eq!(
            history.last().expect("root checkpoint should exist").depth,
            1_026
        );
    }
}
