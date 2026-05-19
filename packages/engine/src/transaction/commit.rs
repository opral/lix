use crate::binary_cas::BinaryCasContext;
use crate::commit_store::{ChangeRef, CommitDraftRef, CommitStoreContext, StagedCommitStoreCommit};
use crate::functions::FunctionContext;
use crate::json_store::{JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
use crate::storage::{StorageRead, StorageWriteSet};
use crate::tracked_state::{TrackedStateContext, TrackedStateDeltaRef};
use crate::transaction::prepare_version_ref_row;
use crate::transaction::staging::PreparedWriteSet;
use crate::transaction::types::{PreparedAdoptedStateRow, PreparedStateRow, StagedCommitMembers};
use crate::untracked_state::{
    UntrackedStateContext, UntrackedStateIdentity, UntrackedStateIdentityRef, UntrackedStateRowRef,
};
use crate::version::{VersionContext, VersionRefReader};
use crate::LixError;
use std::collections::BTreeMap;

type RowIndex = usize;
type AdoptedRowIndex = usize;

/// Commits prepared transaction rows into durable tracked and untracked stores.
///
/// Providers decode DataFusion DML into hydrated `PreparedStateRow`s. Untracked
/// rows are durable local overlay state and bypass commit-store rows. Tracked
/// rows stage canonical commit-store facts, then update the live-state serving
/// projection. The tracked side of that projection is a prolly root keyed by
/// the new commit id.
pub(crate) async fn commit_prepared_writes(
    binary_cas: &BinaryCasContext,
    commit_store: &CommitStoreContext,
    version_ctx: &VersionContext,
    runtime_functions: Option<&FunctionContext>,
    read: &(impl StorageRead + Send + Sync + ?Sized),
    prepared_writes: PreparedWriteSet,
) -> Result<StorageWriteSet, LixError> {
    let mut writes = StorageWriteSet::new();
    let mut json_writer = JsonStoreContext::new().writer();

    if !prepared_writes.file_data_writes.is_empty() {
        let mut blob_writer = binary_cas.writer(&mut writes);
        for write in &prepared_writes.file_data_writes {
            blob_writer.stage_bytes(&write.data)?;
        }
    }

    let state_rows = prepared_writes.state_rows;
    let adopted_rows = prepared_writes.adopted_rows;
    let finalized = finalize_commit_rows(
        prepared_writes.commit_members_by_version,
        prepared_writes.extra_commit_parents_by_version,
        version_ctx,
        read,
    )
    .await?;
    let commit_rows = finalized.commit_rows;
    let version_heads = finalized.version_heads;
    let tracked_roots = finalized.tracked_roots;
    let row_index = index_prepared_rows(&state_rows)?;
    let adopted_index = index_adopted_rows(&adopted_rows);

    if let Some(runtime_functions) = runtime_functions {
        runtime_functions
            .stage_persist_if_needed(&mut writes)
            .await?;
    }

    if state_rows.is_empty()
        && adopted_rows.is_empty()
        && commit_rows.is_empty()
        && version_heads.is_empty()
        && writes.is_empty()
    {
        return Ok(writes);
    }

    let staged_commits = stage_commit_store_commits(
        commit_store,
        read,
        &mut writes,
        &state_rows,
        &row_index.tracked_row_indices_by_commit,
        &adopted_rows,
        &adopted_index.tracked_row_indices_by_commit,
        &commit_rows,
    )
    .await?;

    let json_pack_indexes_by_commit = stage_prepared_json_payloads(
        &mut json_writer,
        &mut writes,
        &state_rows,
        &row_index.tracked_row_indices_by_commit,
        &staged_commits,
        &row_index.untracked_row_indices,
    )?;

    // The serving projection is updated in the same backend transaction as the
    // commit-store append. Tracked rows become prolly mutations under their owning
    // commit root; untracked rows remain in the separate local overlay store.
    {
        let untracked_overlay_delete_identities = existing_untracked_overlay_delete_identities(
            read,
            row_index
                .canonical_row_indices
                .iter()
                .map(|&row_index| untracked_identity_ref_from_state_row(&state_rows[row_index]))
                .chain(
                    adopted_rows
                        .iter()
                        .map(untracked_identity_ref_from_adopted_row),
                ),
        )
        .await?;
        UntrackedStateContext::new()
            .writer(&mut writes)
            .stage_rows(
                row_index
                    .untracked_row_indices
                    .iter()
                    .map(|&row_index| untracked_row_ref_from_state_row(&state_rows[row_index])),
            )?;
        UntrackedStateContext::new()
            .writer(&mut writes)
            .stage_delete_rows(
                untracked_overlay_delete_identities
                    .iter()
                    .map(UntrackedStateIdentity::as_ref),
            )?;
        stage_tracked_roots(
            read,
            &mut writes,
            &state_rows,
            row_index.tracked_row_indices_by_commit,
            &adopted_rows,
            adopted_index.tracked_row_indices_by_commit,
            tracked_roots,
            staged_commits,
            json_pack_indexes_by_commit,
        )
        .await?;
    }

    for version_head in version_heads {
        let canonical_row = prepare_version_ref_row(
            &version_head.version_id,
            &version_head.commit_id,
            &version_head.timestamp,
        )?;
        version_ctx.stage_canonical_ref_rows(&mut writes, &[canonical_row.row])?;
    }

    Ok(writes)
}

fn stage_prepared_json_payloads(
    json_writer: &mut crate::json_store::JsonStoreWriter,
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    tracked_row_indices_by_commit: &BTreeMap<String, Vec<RowIndex>>,
    staged_commits: &BTreeMap<String, StagedCommitStoreCommit>,
    untracked_row_indices: &[RowIndex],
) -> Result<BTreeMap<String, BTreeMap<u32, std::collections::HashMap<[u8; 32], usize>>>, LixError> {
    let mut pack_indexes_by_commit = BTreeMap::new();
    for (commit_id, row_indices) in tracked_row_indices_by_commit {
        let staged_commit = staged_commits.get(commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("commit '{commit_id}' has tracked JSON rows but no staged commit-store locators"),
            )
        })?;
        if row_indices.len() != staged_commit.authored_locators.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "commit '{commit_id}' has {} tracked JSON rows but {} authored locators",
                    row_indices.len(),
                    staged_commit.authored_locators.len()
                ),
            ));
        }
        let mut row_indices_by_pack = BTreeMap::<u32, Vec<RowIndex>>::new();
        for (&row_index, locator) in row_indices.iter().zip(&staged_commit.authored_locators) {
            row_indices_by_pack
                .entry(locator.source_pack_id)
                .or_default()
                .push(row_index);
        }
        for (pack_id, pack_row_indices) in row_indices_by_pack {
            let report = json_writer.stage_batch_report(
                writes,
                JsonWritePlacementRef::CommitPack { commit_id, pack_id },
                pack_row_indices
                    .iter()
                    .flat_map(|&row_index| json_payloads_from_state_row(&state_rows[row_index])),
            )?;
            pack_indexes_by_commit
                .entry(commit_id.clone())
                .or_insert_with(BTreeMap::new)
                .insert(pack_id, report.pack_indexes);
        }
    }
    json_writer.stage_batch(
        writes,
        JsonWritePlacementRef::OutOfBand,
        untracked_row_indices
            .iter()
            .flat_map(|&row_index| json_payloads_from_state_row(&state_rows[row_index])),
    )?;
    Ok(pack_indexes_by_commit)
}

fn json_payloads_from_state_row(
    row: &PreparedStateRow,
) -> impl Iterator<Item = NormalizedJsonRef<'_>> {
    row.snapshot
        .iter()
        .chain(row.metadata.iter())
        .map(|json| NormalizedJsonRef::trusted_prehashed(json.normalized.as_ref(), json.json_ref))
}

async fn existing_untracked_overlay_delete_identities<'a>(
    read: &(impl StorageRead + Send + Sync + ?Sized),
    identities: impl IntoIterator<Item = UntrackedStateIdentityRef<'a>>,
) -> Result<Vec<UntrackedStateIdentity>, LixError> {
    UntrackedStateContext::new()
        .reader(read)
        .existing_identities(identities)
        .await
}

struct PreparedRowIndex {
    canonical_row_indices: Vec<RowIndex>,
    untracked_row_indices: Vec<RowIndex>,
    tracked_row_indices_by_commit: BTreeMap<String, Vec<RowIndex>>,
}

struct PreparedAdoptedRowIndex {
    tracked_row_indices_by_commit: BTreeMap<String, Vec<AdoptedRowIndex>>,
}

fn index_prepared_rows(rows: &[PreparedStateRow]) -> Result<PreparedRowIndex, LixError> {
    let mut canonical_row_indices = Vec::new();
    let mut untracked_row_indices = Vec::new();
    let mut tracked_row_indices_by_commit = BTreeMap::<String, Vec<RowIndex>>::new();

    for (row_index, row) in rows.iter().enumerate() {
        if row.untracked {
            untracked_row_indices.push(row_index);
            continue;
        }
        let Some(commit_id) = row.commit_id.as_ref() else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked prepared row is missing commit_id before commit indexing",
            ));
        };
        canonical_row_indices.push(row_index);
        tracked_row_indices_by_commit
            .entry(commit_id.clone())
            .or_default()
            .push(row_index);
    }

    Ok(PreparedRowIndex {
        canonical_row_indices,
        untracked_row_indices,
        tracked_row_indices_by_commit,
    })
}

fn index_adopted_rows(rows: &[PreparedAdoptedStateRow]) -> PreparedAdoptedRowIndex {
    let mut tracked_row_indices_by_commit = BTreeMap::<String, Vec<AdoptedRowIndex>>::new();
    for (row_index, row) in rows.iter().enumerate() {
        tracked_row_indices_by_commit
            .entry(row.commit_id.clone())
            .or_default()
            .push(row_index);
    }
    PreparedAdoptedRowIndex {
        tracked_row_indices_by_commit,
    }
}

async fn stage_commit_store_commits(
    commit_store: &CommitStoreContext,
    read: &(impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    tracked_row_indices_by_commit: &BTreeMap<String, Vec<RowIndex>>,
    adopted_rows: &[PreparedAdoptedStateRow],
    adopted_row_indices_by_commit: &BTreeMap<String, Vec<AdoptedRowIndex>>,
    commit_rows: &[FinalizedCommitRow],
) -> Result<BTreeMap<String, StagedCommitStoreCommit>, LixError> {
    let mut commits = Vec::with_capacity(commit_rows.len());
    let mut commit_ids = Vec::with_capacity(commit_rows.len());
    for commit_row in commit_rows {
        let state_row_indices = tracked_row_indices_by_commit
            .get(&commit_row.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let adopted_row_indices = adopted_row_indices_by_commit
            .get(&commit_row.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let mut authored_changes = Vec::with_capacity(state_row_indices.len());
        for &row_index in state_row_indices {
            authored_changes.push(change_ref_from_state_row(&state_rows[row_index])?);
        }
        let mut adopted_changes = Vec::with_capacity(adopted_row_indices.len());
        for &row_index in adopted_row_indices {
            adopted_changes.push(change_ref_from_adopted_row(&adopted_rows[row_index]));
        }

        let commit = CommitDraftRef {
            id: &commit_row.commit_id,
            change_id: &commit_row.change_id,
            parent_ids: &commit_row.parent_commit_ids,
            author_account_ids: &[],
            created_at: &commit_row.created_at,
        };
        commit_ids.push(commit_row.commit_id.clone());
        commits.push((commit, authored_changes, adopted_changes));
    }
    let staged = commit_store
        .writer(read, writes)
        .stage_tracked_commit_drafts(commits)
        .await?;
    if staged.len() != commit_ids.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "commit-store staged {} commits for {} finalized commit rows",
                staged.len(),
                commit_ids.len()
            ),
        ));
    }
    Ok(commit_ids.into_iter().zip(staged).collect())
}

fn change_ref_from_state_row(row: &PreparedStateRow) -> Result<ChangeRef<'_>, LixError> {
    let Some(change_id) = row.change_id.as_deref() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked staged row is missing change_id before commit-store append",
        ));
    };

    Ok(ChangeRef {
        id: change_id,
        entity_id: &row.entity_id,
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        snapshot_ref: row.snapshot.as_ref().map(|snapshot| &snapshot.json_ref),
        metadata_ref: row.metadata.as_ref().map(|metadata| &metadata.json_ref),
        created_at: &row.updated_at,
    })
}

fn change_ref_from_adopted_row(row: &PreparedAdoptedStateRow) -> ChangeRef<'_> {
    ChangeRef {
        id: &row.change_id,
        entity_id: &row.entity_id,
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        snapshot_ref: row.snapshot.as_ref().map(|snapshot| &snapshot.json_ref),
        metadata_ref: row.metadata.as_ref().map(|metadata| &metadata.json_ref),
        created_at: &row.updated_at,
    }
}

async fn stage_tracked_roots(
    read: &(impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    mut tracked_row_indices_by_commit: BTreeMap<String, Vec<RowIndex>>,
    adopted_rows: &[PreparedAdoptedStateRow],
    mut adopted_row_indices_by_commit: BTreeMap<String, Vec<AdoptedRowIndex>>,
    tracked_roots: Vec<PendingTrackedRoot>,
    mut staged_commits: BTreeMap<String, StagedCommitStoreCommit>,
    json_pack_indexes_by_commit: BTreeMap<
        String,
        BTreeMap<u32, std::collections::HashMap<[u8; 32], usize>>,
    >,
) -> Result<(), LixError> {
    let tracked_state = TrackedStateContext::new();
    let mut writer = tracked_state.writer(read, writes);
    for root in tracked_roots {
        let staged = staged_commits.remove(&root.commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked-state root for commit '{}' has no staged commit-store locators",
                    root.commit_id
                ),
            )
        })?;
        let state_row_indices = tracked_row_indices_by_commit
            .remove(&root.commit_id)
            .unwrap_or_default();
        let adopted_row_indices = adopted_row_indices_by_commit
            .remove(&root.commit_id)
            .unwrap_or_default();
        if state_row_indices.len() != staged.authored_locators.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "commit '{}' has {} tracked authored rows but {} commit-store authored locators",
                    root.commit_id,
                    state_row_indices.len(),
                    staged.authored_locators.len()
                ),
            ));
        }
        if adopted_row_indices.len() != staged.adopted_locators.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "commit '{}' has {} tracked adopted rows but {} commit-store adopted locators",
                    root.commit_id,
                    adopted_row_indices.len(),
                    staged.adopted_locators.len()
                ),
            ));
        }
        let authored_changes = state_row_indices
            .iter()
            .map(|&row_index| change_ref_from_state_row(&state_rows[row_index]))
            .collect::<Result<Vec<_>, _>>()?;
        let adopted_changes = adopted_row_indices
            .iter()
            .map(|&row_index| change_ref_from_adopted_row(&adopted_rows[row_index]))
            .collect::<Vec<_>>();
        let authored_updated_at = state_row_indices
            .iter()
            .map(|&row_index| state_rows[row_index].updated_at.as_str())
            .collect::<Vec<_>>();
        let authored_created_at = state_row_indices
            .iter()
            .map(|&row_index| state_rows[row_index].created_at.as_str())
            .collect::<Vec<_>>();
        let adopted_updated_at = adopted_row_indices
            .iter()
            .map(|&row_index| adopted_rows[row_index].updated_at.as_str())
            .collect::<Vec<_>>();
        let adopted_created_at = adopted_row_indices
            .iter()
            .map(|&row_index| adopted_rows[row_index].created_at.as_str())
            .collect::<Vec<_>>();
        let mut deltas = Vec::with_capacity(authored_changes.len() + adopted_changes.len());
        deltas.extend(
            authored_changes
                .iter()
                .zip(&staged.authored_locators)
                .zip(authored_created_at)
                .zip(authored_updated_at)
                .map(
                    |(((change, locator), created_at), updated_at)| TrackedStateDeltaRef {
                        change: *change,
                        locator: locator.as_ref(),
                        created_at,
                        updated_at,
                    },
                ),
        );
        deltas.extend(
            adopted_changes
                .iter()
                .zip(&staged.adopted_locators)
                .zip(adopted_created_at)
                .zip(adopted_updated_at)
                .map(
                    |(((change, locator), created_at), updated_at)| TrackedStateDeltaRef {
                        change: *change,
                        locator: locator.as_ref(),
                        created_at,
                        updated_at,
                    },
                ),
        );
        if let Some(indexes) = json_pack_indexes_by_commit
            .get(&root.commit_id)
            .and_then(|packs| packs.get(&0))
        {
            writer
                .stage_delta_with_json_pack_indexes(
                    &root.commit_id,
                    root.parent_commit_id.as_deref(),
                    &deltas,
                    crate::tracked_state::DeltaJsonPackIndexesRef {
                        commit_id: &root.commit_id,
                        pack_id: 0,
                        indexes,
                    },
                )
                .await?;
        } else {
            writer
                .stage_delta(&root.commit_id, root.parent_commit_id.as_deref(), &deltas)
                .await?;
        }
    }
    if !tracked_row_indices_by_commit.is_empty() || !adopted_row_indices_by_commit.is_empty() {
        let mut commit_ids = tracked_row_indices_by_commit
            .keys()
            .chain(adopted_row_indices_by_commit.keys())
            .cloned()
            .collect::<Vec<_>>();
        commit_ids.sort();
        commit_ids.dedup();
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked live_state rows have no finalized root metadata for commit ids: {}",
                commit_ids.join(", ")
            ),
        ));
    }
    if !staged_commits.is_empty() {
        let commit_ids = staged_commits.keys().cloned().collect::<Vec<_>>();
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "commit-store staged commits without tracked root metadata: {}",
                commit_ids.join(", ")
            ),
        ));
    }
    Ok(())
}

fn untracked_row_ref_from_state_row(row: &PreparedStateRow) -> UntrackedStateRowRef<'_> {
    UntrackedStateRowRef {
        entity_id: &row.entity_id,
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        snapshot_content: row
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.normalized.as_ref()),
        metadata: row
            .metadata
            .as_ref()
            .map(|metadata| metadata.normalized.as_ref()),
        created_at: &row.created_at,
        updated_at: &row.updated_at,
        global: row.global,
        version_id: &row.version_id,
    }
}

fn untracked_identity_ref_from_state_row(row: &PreparedStateRow) -> UntrackedStateIdentityRef<'_> {
    UntrackedStateIdentityRef {
        version_id: &row.version_id,
        schema_key: &row.schema_key,
        entity_id: &row.entity_id,
        file_id: row.file_id.as_deref(),
    }
}

fn untracked_identity_ref_from_adopted_row(
    row: &PreparedAdoptedStateRow,
) -> UntrackedStateIdentityRef<'_> {
    UntrackedStateIdentityRef {
        version_id: &row.version_id,
        schema_key: &row.schema_key,
        entity_id: &row.entity_id,
        file_id: row.file_id.as_deref(),
    }
}

/// Materializes tracked staged membership into commit-store commits.
///
/// Staging only accumulates `version_id -> change_ids` because commit ids,
/// parent heads, and commit-row timestamps belong to transaction finalization.
/// The `change_ids` list is the ordered set of canonical changes whose effects
/// the commit introduces relative to its first parent; merge commits may later
/// populate this list with existing source-parent changes instead of copied
/// change payloads.
/// This function turns those membership sets into finalized commit facts.
///
/// Commit finalization output split by durability target.
///
/// `commit_rows` are canonical commit-store facts. live_state later projects
/// commit SQL surfaces from commit_store; tracked_state roots do not store
/// commit graph facts.
///
/// `version_heads` are moving refs. They are written through `VersionContext`,
/// not the canonical commit store.
struct FinalizedCommitRows {
    commit_rows: Vec<FinalizedCommitRow>,
    version_heads: Vec<PendingVersionHead>,
    tracked_roots: Vec<PendingTrackedRoot>,
}

struct FinalizedCommitRow {
    commit_id: String,
    parent_commit_ids: Vec<String>,
    created_at: String,
    change_id: String,
}

struct PendingVersionHead {
    version_id: String,
    commit_id: String,
    timestamp: String,
}

struct PendingTrackedRoot {
    commit_id: String,
    parent_commit_id: Option<String>,
}

async fn finalize_commit_rows(
    commit_members_by_version: BTreeMap<String, StagedCommitMembers>,
    extra_commit_parents_by_version: BTreeMap<String, Vec<String>>,
    version_ctx: &VersionContext,
    read: &(impl StorageRead + Send + Sync + ?Sized),
) -> Result<FinalizedCommitRows, LixError> {
    let mut commit_rows = Vec::new();
    let mut version_heads = Vec::new();
    let mut tracked_roots = Vec::new();

    for (version_id, members) in commit_members_by_version {
        if members.is_empty() && !members.allow_empty {
            continue;
        }

        let commit_id = members.commit_id;
        let commit_change_id = members.commit_change_id;
        let timestamp = members.created_at;
        let _change_ids = members.change_ids;
        let parent_commit_ids = version_ctx
            .ref_reader(read)
            .load_head_commit_id(&version_id)
            .await?
            .into_iter()
            .collect::<Vec<_>>();
        let parent_commit_ids = merge_parent_commit_ids(
            parent_commit_ids,
            extra_commit_parents_by_version
                .get(&version_id)
                .cloned()
                .unwrap_or_default(),
        );
        let parent_commit_id = parent_commit_ids.first().cloned();

        commit_rows.push(FinalizedCommitRow {
            commit_id: commit_id.clone(),
            parent_commit_ids: parent_commit_ids.clone(),
            created_at: timestamp.clone(),
            change_id: commit_change_id,
        });
        version_heads.push(PendingVersionHead {
            version_id: version_id.clone(),
            commit_id: commit_id.clone(),
            timestamp,
        });
        tracked_roots.push(PendingTrackedRoot {
            commit_id,
            parent_commit_id,
        });
    }

    Ok(FinalizedCommitRows {
        commit_rows,
        version_heads,
        tracked_roots,
    })
}

fn merge_parent_commit_ids(mut base: Vec<String>, extra: Vec<String>) -> Vec<String> {
    for parent in extra {
        if !base.contains(&parent) {
            base.push(parent);
        }
    }
    base
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use super::*;
    use crate::backend::{
        Backend, BackendCapabilities, BackendError, BackendWrite, CommitResult, KeyRange, PutBatch,
    };
    use crate::catalog::SchemaPlanId;
    use crate::commit_store::{ChangeIndexEntry, ChangeLocator};
    use crate::live_state::{LiveStateContext, LiveStateRowRequest};
    use crate::storage::{
        InMemoryStorageBackend, InMemoryStorageRead, InMemoryStorageWrite, StorageContext,
        StorageKey, StorageReadOptions, StorageWriteOptions,
    };
    use crate::transaction::types::PreparedRowFacts;
    use crate::untracked_state::{
        MaterializedUntrackedStateRow, UntrackedStateContext, UntrackedStateRowRequest,
    };
    use crate::version::VersionContext;
    use crate::NullableKeyFilter;
    use crate::GLOBAL_VERSION_ID;

    const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
    const DETERMINISTIC_SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            crate::tracked_state::TrackedStateContext::new(),
            crate::untracked_state::UntrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        )
    }

    #[tokio::test]
    async fn commit_staged_writes_appends_commit_store_and_updates_serving_projection() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let binary_cas = BinaryCasContext::new();
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");

        let state_rows = vec![tracked_global_row("change-1")];
        let writes = commit_prepared_writes(
            &binary_cas,
            &crate::commit_store::CommitStoreContext::new(),
            &version_ctx,
            None,
            &read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    members(["change-1"]),
                )]),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("commit should flush staged rows");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("writes should commit");

        let commit_reader = crate::commit_store::CommitStoreContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let commit = commit_reader
            .load_commit("test-uuid-1")
            .await
            .expect("commit-store commit should load")
            .expect("commit-store commit should exist");
        assert_eq!(commit.change_id, "test-uuid-2");
        assert_eq!(commit.change_pack_count, 1);
        assert_eq!(commit.membership_pack_count, 0);
        let index_entries = commit_reader
            .load_change_index_entries(&["change-1".to_string(), "test-uuid-2".to_string()])
            .await
            .expect("commit-store change index should load");
        assert_eq!(
            index_entries,
            vec![
                Some(ChangeIndexEntry::PackedChange {
                    locator: ChangeLocator {
                        source_commit_id: "test-uuid-1".to_string(),
                        source_pack_id: 0,
                        source_ordinal: 0,
                        change_id: "change-1".to_string(),
                    },
                }),
                Some(ChangeIndexEntry::CommitHeader {
                    commit_id: "test-uuid-1".to_string(),
                    change_id: "test-uuid-2".to_string(),
                }),
            ]
        );
        let change_pack = commit_reader
            .load_change_pack("test-uuid-1", 0)
            .await
            .expect("commit-store change pack should load")
            .expect("commit-store change pack should exist");
        assert_eq!(change_pack.len(), 1);
        assert_eq!(change_pack[0].id, "change-1");
        assert_eq!(change_pack[0].schema_key, "test_schema");

        let loaded_head = version_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref load should succeed");
        assert_eq!(loaded_head.as_deref(), Some("test-uuid-1"));
    }

    #[tokio::test]
    async fn commit_with_only_untracked_writes_does_not_create_lix_commit() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let binary_cas = BinaryCasContext::new();
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        let untracked_state = UntrackedStateContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");

        let state_rows = vec![untracked_global_row("change-untracked")];
        let writes = commit_prepared_writes(
            &binary_cas,
            &crate::commit_store::CommitStoreContext::new(),
            &version_ctx,
            None,
            &read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::new(),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("commit should flush untracked row");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("writes should commit");

        let commit_reader = crate::commit_store::CommitStoreContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let index_entries = commit_reader
            .load_change_index_entries(&["change-untracked".to_string()])
            .await
            .expect("commit-store change index should load");
        assert_eq!(index_entries, vec![None]);

        let loaded = {
            let mut untracked_reader = untracked_state.reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            );
            untracked_reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "test_schema".to_string(),
                    version_id: GLOBAL_VERSION_ID.to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("untracked row load should succeed")
        .expect("untracked row should be persisted");
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );
    }

    #[tokio::test]
    async fn tracked_write_deletes_matching_untracked_overlay() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let binary_cas = BinaryCasContext::new();
        let untracked_state = UntrackedStateContext::new();
        let live_state = Arc::new(live_state_context());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));

        let mut writes = StorageWriteSet::new();
        let staged_row = untracked_global_row("change-untracked");
        let canonical_row = crate::test_support::untracked_state_row_from_materialized(
            &mut writes,
            &MaterializedUntrackedStateRow::from(staged_row),
        )
        .expect("untracked seed should canonicalize");
        untracked_state
            .writer(&mut writes)
            .stage_rows(std::iter::once(canonical_row.as_ref()))
            .expect("untracked seed should write");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("untracked seed should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let state_rows = vec![tracked_global_row("change-tracked")];
        let writes = commit_prepared_writes(
            &binary_cas,
            &crate::commit_store::CommitStoreContext::new(),
            &version_ctx,
            None,
            &read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    members(["change-tracked"]),
                )]),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("tracked commit should flush");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("writes should commit");

        let untracked = {
            let mut untracked_reader = untracked_state.reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            );
            untracked_reader.load_row(&untracked_request()).await
        }
        .expect("untracked load should succeed");
        assert_eq!(untracked, None);

        let visible = live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .load_row(&live_state_request())
            .await
            .expect("live-state load should succeed")
            .expect("tracked row should be visible");
        assert!(!visible.untracked);
        assert_eq!(visible.change_id.as_deref(), Some("change-tracked"));
        assert_eq!(visible.snapshot_content.as_deref(), Some("{\"value\":1}"));
    }

    #[tokio::test]
    async fn commit_staged_writes_applies_cross_subsystem_rows_as_one_backend_batch() {
        let counting_backend = CountingBackend::new();
        let write_batches = counting_backend.write_batches();
        let storage = StorageContext::new(counting_backend);
        let binary_cas = BinaryCasContext::new();
        let live_state = Arc::new(live_state_context());
        let untracked_state = UntrackedStateContext::new();
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .expect("seed read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_tracked_root_from_materialized(
                &read,
                &mut writes,
                &crate::tracked_state::TrackedStateContext::new(),
                crate::test_support::TEST_EMPTY_ROOT_COMMIT_ID,
                None,
                &[],
            )
            .await
            .expect("empty tracked root should stage");
            let version_ref_row = crate::transaction::prepare_version_ref_row(
                GLOBAL_VERSION_ID,
                crate::test_support::TEST_EMPTY_ROOT_COMMIT_ID,
                "1970-01-01T00:00:00.000Z",
            )
            .expect("global version ref should stage");
            UntrackedStateContext::new()
                .writer(&mut writes)
                .stage_rows([version_ref_row.row.as_ref()])
                .expect("global version ref should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("global version ref should commit");
        }
        {
            let mut writes = StorageWriteSet::new();
            let mode_snapshot = serde_json::to_string(&serde_json::json!({
                "key": DETERMINISTIC_MODE_KEY,
                "value": { "enabled": true },
            }))
            .expect("mode snapshot should serialize");
            JsonStoreContext::new()
                .writer()
                .stage_batch(
                    &mut writes,
                    JsonWritePlacementRef::OutOfBand,
                    [NormalizedJsonRef::new(mode_snapshot.as_str())],
                )
                .expect("deterministic mode snapshot should stage");
            let row = crate::untracked_state::UntrackedStateRow {
                entity_id: crate::entity_identity::EntityIdentity::single(DETERMINISTIC_MODE_KEY),
                schema_key: "lix_key_value".to_string(),
                file_id: None,
                snapshot_content: Some(mode_snapshot.to_string()),
                metadata: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
                global: true,
                version_id: GLOBAL_VERSION_ID.to_string(),
            };
            UntrackedStateContext::new()
                .writer(&mut writes)
                .stage_rows(std::iter::once(row.as_ref()))
                .expect("deterministic mode should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("deterministic mode should commit");
        }
        write_batches.store(0, Ordering::SeqCst);
        let runtime_functions = {
            let reader = live_state.reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            );
            FunctionContext::prepare(&reader)
                .await
                .expect("runtime context should prepare")
        };
        runtime_functions.provider().call_uuid_v7();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");

        let tracked_row = tracked_global_row("change-tracked");
        let mut untracked_row = untracked_global_row("change-untracked");
        untracked_row.entity_id = crate::entity_identity::EntityIdentity::single("entity-2");

        let writes = commit_prepared_writes(
            &binary_cas,
            &crate::commit_store::CommitStoreContext::new(),
            &version_ctx,
            Some(&runtime_functions),
            &read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![tracked_row, untracked_row],
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    members(["change-tracked"]),
                )]),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("cross-subsystem commit should stage and apply");

        assert_eq!(
            write_batches.load(Ordering::SeqCst),
            0,
            "prepared writes should not touch the backend before the write set is committed"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("writes should commit");
        assert_eq!(write_batches.load(Ordering::SeqCst), 1);

        let commit_reader = crate::commit_store::CommitStoreContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let commit = commit_reader
            .load_commit("test-uuid-1")
            .await
            .expect("commit-store commit should load")
            .expect("commit-store commit should exist");
        assert_eq!(commit.change_id, "test-uuid-2");
        let index_entries = commit_reader
            .load_change_index_entries(&["change-tracked".to_string()])
            .await
            .expect("commit-store change index should load");
        assert!(matches!(
            index_entries.as_slice(),
            [Some(ChangeIndexEntry::PackedChange { .. })]
        ));

        let loaded_head = version_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref load should succeed");
        assert_eq!(loaded_head.as_deref(), Some("test-uuid-1"));

        let untracked = {
            let mut untracked_reader = untracked_state.reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            );
            untracked_reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "test_schema".to_string(),
                    version_id: GLOBAL_VERSION_ID.to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single("entity-2"),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("untracked row load should succeed")
        .expect("untracked row should persist");
        assert_eq!(
            untracked.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );

        let sequence_row = live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single(
                    DETERMINISTIC_SEQUENCE_KEY,
                ),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("deterministic sequence should load")
            .expect("deterministic sequence should persist");
        assert_eq!(
            sequence_row.snapshot_content.as_deref(),
            Some("{\"key\":\"lix_deterministic_sequence_number\",\"value\":0}")
        );
    }

    #[tokio::test]
    async fn non_global_tracked_write_creates_one_commit_and_advances_only_touched_version() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let binary_cas = BinaryCasContext::new();
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(storage.clone(), GLOBAL_VERSION_ID, "global-before")
            .await;
        crate::test_support::seed_version_head(storage.clone(), "version-a", "version-a-before")
            .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let state_rows = vec![tracked_version_row("version-a", "change-version-a")];
        let writes = commit_prepared_writes(
            &binary_cas,
            &crate::commit_store::CommitStoreContext::new(),
            &version_ctx,
            None,
            &read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::from([(
                    "version-a".to_string(),
                    members(["change-version-a"]),
                )]),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("version commit should flush");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("writes should commit");

        let commit_reader = crate::commit_store::CommitStoreContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let commit = commit_reader
            .load_commit("test-uuid-1")
            .await
            .expect("commit-store commit should load")
            .expect("commit-store commit should exist");
        assert_eq!(commit.change_id, "test-uuid-2");
        assert_eq!(commit.parent_ids, vec!["version-a-before"]);
        let index_entries = commit_reader
            .load_change_index_entries(&["change-version-a".to_string()])
            .await
            .expect("commit-store change index should load");
        assert!(matches!(
            index_entries.as_slice(),
            [Some(ChangeIndexEntry::PackedChange { .. })]
        ));

        let global_head = version_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("global head should load");
        let version_head = version_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .expect("read should open"),
            )
            .load_head_commit_id("version-a")
            .await
            .expect("version head should load");
        assert_eq!(global_head.as_deref(), Some("global-before"));
        assert_eq!(version_head.as_deref(), Some("test-uuid-1"));
    }

    #[tokio::test]
    async fn finalize_commit_rows_parents_global_commit_to_existing_version_ref() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(
            storage.clone(),
            GLOBAL_VERSION_ID,
            "initial-commit",
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_VERSION_ID.to_string(),
                members(["change-a", "change-b"]),
            )]),
            BTreeMap::new(),
            &version_ctx,
            &read,
        )
        .await
        .expect("global commit row should finalize");

        assert_eq!(rows.commit_rows.len(), 1);
        assert_eq!(rows.version_heads.len(), 1);
        let row = &rows.commit_rows[0];
        assert_eq!(row.commit_id, "test-uuid-1");
        assert_eq!(row.change_id, "test-uuid-2");
        assert_eq!(row.created_at, "test-timestamp-1");
        assert_eq!(row.parent_commit_ids, vec!["initial-commit"]);

        let version_head = &rows.version_heads[0];
        assert_eq!(version_head.version_id, GLOBAL_VERSION_ID);
        assert_eq!(version_head.commit_id, "test-uuid-1");
    }

    #[tokio::test]
    async fn finalize_commit_rows_skips_empty_members() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_VERSION_ID.to_string(),
                StagedCommitMembers::default(),
            )]),
            BTreeMap::new(),
            &version_ctx,
            &read,
        )
        .await
        .expect("empty members should be ignored");

        assert!(rows.commit_rows.is_empty());
        assert!(rows.version_heads.is_empty());
    }

    #[tokio::test]
    async fn finalize_commit_rows_uses_existing_version_ref_as_parent() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(storage.clone(), GLOBAL_VERSION_ID, "global-before")
            .await;
        crate::test_support::seed_version_head(storage.clone(), "version-a", "previous-commit")
            .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([("version-a".to_string(), members(["change-a"]))]),
            BTreeMap::new(),
            &version_ctx,
            &read,
        )
        .await
        .expect("active-version commit finalization should resolve parent");

        assert_eq!(
            rows.commit_rows[0].parent_commit_ids,
            vec!["previous-commit"]
        );
        assert_eq!(rows.version_heads[0].version_id, "version-a");
    }

    #[tokio::test]
    async fn finalize_commit_rows_appends_extra_merge_parent_after_target_head() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(storage.clone(), "version-a", "target-head").await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([("version-a".to_string(), members(["change-a"]))]),
            BTreeMap::from([("version-a".to_string(), vec!["source-head".to_string()])]),
            &version_ctx,
            &read,
        )
        .await
        .expect("merge commit finalization should resolve parents");

        assert_eq!(
            rows.commit_rows[0].parent_commit_ids,
            vec!["target-head", "source-head"]
        );
    }

    fn members<const N: usize>(change_ids: [&str; N]) -> StagedCommitMembers {
        let mut members = StagedCommitMembers::new(
            "test-uuid-1".to_string(),
            "test-uuid-2".to_string(),
            "test-timestamp-1".to_string(),
        );
        for change_id in change_ids {
            members.add_change_id(change_id.to_string());
        }
        members
    }

    fn tracked_global_row(change_id: &str) -> PreparedStateRow {
        tracked_version_row(GLOBAL_VERSION_ID, change_id)
    }

    fn tracked_version_row(version_id: &str, change_id: &str) -> PreparedStateRow {
        PreparedStateRow {
            schema_plan_id: SchemaPlanId::for_test(0),
            facts: PreparedRowFacts::default(),
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            schema_key: "test_schema".to_string(),
            file_id: None,
            snapshot: Some(
                crate::transaction::types::stage_json_from_value(
                    crate::transaction::types::TransactionJson::from_value_for_test(
                        serde_json::json!({ "value": 1 }),
                    ),
                    "test tracked row snapshot",
                )
                .expect("test snapshot should stage"),
            ),
            metadata: None,
            origin: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: version_id == GLOBAL_VERSION_ID,
            change_id: Some(change_id.to_string()),
            commit_id: Some("test-uuid-1".to_string()),
            untracked: false,
            version_id: version_id.to_string(),
        }
    }

    fn untracked_global_row(change_id: &str) -> PreparedStateRow {
        let mut row = tracked_global_row(change_id);
        row.snapshot = Some(
            crate::transaction::types::stage_json_from_value(
                crate::transaction::types::TransactionJson::from_value_for_test(
                    serde_json::json!({ "value": "untracked" }),
                ),
                "test untracked row snapshot",
            )
            .expect("test snapshot should stage"),
        );
        PreparedStateRow {
            change_id: None,
            commit_id: None,
            untracked: true,
            ..row
        }
    }

    fn untracked_request() -> UntrackedStateRowRequest {
        UntrackedStateRowRequest {
            schema_key: "test_schema".to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            file_id: NullableKeyFilter::Null,
        }
    }

    fn live_state_request() -> LiveStateRowRequest {
        LiveStateRowRequest {
            schema_key: "test_schema".to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            file_id: NullableKeyFilter::Null,
        }
    }

    struct CountingBackend {
        inner: InMemoryStorageBackend,
        write_batches: Arc<AtomicUsize>,
    }

    impl CountingBackend {
        fn new() -> Self {
            Self {
                inner: InMemoryStorageBackend::new(),
                write_batches: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn write_batches(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.write_batches)
        }
    }

    impl Backend for CountingBackend {
        type Read<'a>
            = InMemoryStorageRead
        where
            Self: 'a;

        type Write<'a>
            = CountingWrite
        where
            Self: 'a;

        fn capabilities(&self) -> BackendCapabilities {
            self.inner.capabilities()
        }

        fn begin_read(&self, opts: StorageReadOptions) -> Result<Self::Read<'_>, BackendError> {
            self.inner.begin_read(opts)
        }

        fn begin_write(&self, opts: StorageWriteOptions) -> Result<Self::Write<'_>, BackendError> {
            Ok(CountingWrite {
                inner: self.inner.begin_write(opts)?,
                write_batches: Arc::clone(&self.write_batches),
            })
        }
    }

    struct CountingWrite {
        inner: InMemoryStorageWrite,
        write_batches: Arc<AtomicUsize>,
    }

    impl BackendWrite for CountingWrite {
        fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
            self.inner.put_many(entries)
        }

        fn delete_many(&mut self, keys: &[StorageKey]) -> Result<(), BackendError> {
            self.inner.delete_many(keys)
        }

        fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError> {
            self.inner.delete_range(range)
        }

        fn commit(self) -> Result<CommitResult, BackendError> {
            self.write_batches.fetch_add(1, Ordering::SeqCst);
            self.inner.commit()
        }

        fn rollback(self) -> Result<(), BackendError> {
            self.inner.rollback()
        }
    }
}
