use crate::binary_cas::BinaryCasContext;
use crate::changelog::{
    ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogWriter, CommitChangeRef,
    CommitChangeRefSet, CommitRecord,
};
use crate::entity_identity::EntityIdentity;
use crate::functions::FunctionContext;
use crate::json_store::{JsonRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
use crate::storage::{StorageRead, StorageWriteSet};
use crate::tracked_state::{TrackedStateContext, TrackedStateDeltaRef};
use crate::transaction::prepare_version_ref_row;
use crate::transaction::staging::PreparedWriteSet;
use crate::transaction::types::{PreparedStateRow, StagedCommitChangeRef, StagedCommitChangeRefs};
use crate::untracked_state::{
    UntrackedStateContext, UntrackedStateIdentity, UntrackedStateIdentityRef, UntrackedStateRowRef,
};
use crate::version::{VersionContext, VersionRefReader};
use crate::LixError;
use std::collections::{BTreeMap, BTreeSet};

type RowIndex = usize;

/// Commits prepared transaction rows into durable tracked and untracked stores.
///
/// Providers decode DataFusion DML into hydrated `PreparedStateRow`s. Untracked
/// rows are durable local overlay state and bypass changelog change refs. Tracked
/// rows stage canonical changelog facts, then update the live-state serving
/// commit root. The tracked side of that commit root is a prolly root keyed by
/// the new commit id.
pub(crate) async fn commit_prepared_writes(
    binary_cas: &BinaryCasContext,
    version_ctx: &VersionContext,
    runtime_functions: Option<&FunctionContext>,
    read: &mut (impl StorageRead + Send + Sync),
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
    let finalized = finalize_commit_rows(
        prepared_writes.commit_change_refs_by_version,
        prepared_writes.extra_commit_parents_by_version,
        version_ctx,
        &*read,
    )
    .await?;
    let commit_rows = finalized.commit_rows;
    let version_heads = finalized.version_heads;
    let tracked_roots = finalized.tracked_roots;
    let row_index = index_prepared_rows(&state_rows)?;

    if let Some(runtime_functions) = runtime_functions {
        runtime_functions
            .stage_persist_if_needed(&mut writes)
            .await?;
    }

    if state_rows.is_empty()
        && commit_rows.is_empty()
        && version_heads.is_empty()
        && writes.is_empty()
    {
        return Ok(writes);
    }

    let staged_commits = stage_changelog_commits(
        read,
        &mut writes,
        &state_rows,
        &row_index.tracked_row_indices_by_commit,
        &commit_rows,
    )
    .await?;

    stage_state_json_payloads(
        &mut json_writer,
        &mut writes,
        &state_rows,
        &row_index.canonical_row_indices,
    )?;

    // The serving commit root is updated in the same backend transaction as the
    // changelog append. Tracked rows become prolly mutations under their owning
    // commit root; untracked rows remain in the separate local overlay store.
    {
        let untracked_overlay_delete_identities = existing_untracked_overlay_delete_identities(
            &*read,
            row_index
                .canonical_row_indices
                .iter()
                .map(|&row_index| untracked_identity_ref_from_state_row(&state_rows[row_index])),
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
            tracked_roots,
            staged_commits,
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

fn stage_state_json_payloads(
    json_writer: &mut crate::json_store::JsonStoreWriter,
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    row_indices: &[RowIndex],
) -> Result<(), LixError> {
    json_writer.stage_batch(
        writes,
        JsonWritePlacementRef::OutOfBand,
        row_indices
            .iter()
            .flat_map(|&row_index| json_payloads_from_state_row(&state_rows[row_index])),
    )?;
    Ok(())
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

#[derive(Clone, Debug)]
struct StagedChangelogCommit {
    change_ids: Vec<String>,
    selected_change_refs: Vec<StagedCommitChangeRef>,
    commit_change_id: String,
    commit_created_at: String,
}

async fn stage_changelog_commits(
    read: &mut (impl StorageRead + Send + Sync),
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    tracked_row_indices_by_commit: &BTreeMap<String, Vec<RowIndex>>,
    commit_rows: &[FinalizedCommitRow],
) -> Result<BTreeMap<String, StagedChangelogCommit>, LixError> {
    if commit_rows.is_empty() {
        return Ok(BTreeMap::new());
    }

    let mut commits = Vec::with_capacity(commit_rows.len());
    let mut changes = Vec::new();
    let mut commit_change_refs = Vec::with_capacity(commit_rows.len());
    let mut staged = BTreeMap::<String, StagedChangelogCommit>::new();
    for commit_row in commit_rows {
        let state_row_indices = tracked_row_indices_by_commit
            .get(&commit_row.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let mut refs = Vec::with_capacity(state_row_indices.len());
        let mut change_ids =
            Vec::with_capacity(state_row_indices.len() + commit_row.selected_change_refs.len());
        for &row_index in state_row_indices {
            let row = &state_rows[row_index];
            let change_id = row.change_id.as_ref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked staged row is missing change_id before changelog append",
                )
            })?;
            refs.push(commit_change_ref_from_state_row(row, change_id));
            change_ids.push(change_id.clone());
            changes.push(change_record_from_state_row(row)?);
        }
        for change_ref in &commit_row.selected_change_refs {
            refs.push(commit_change_ref_from_selected_change_ref(change_ref));
            change_ids.push(change_ref.change_id.clone());
        }
        commits.push(CommitRecord {
            format_version: 1,
            commit_id: commit_row.commit_id.clone(),
            parent_commit_ids: commit_row.parent_commit_ids.clone(),
            change_id: commit_row.change_id.clone(),
            author_account_ids: Vec::new(),
            created_at: commit_row.created_at.clone(),
        });
        commit_change_refs.push(CommitChangeRefSet {
            commit_id: commit_row.commit_id.clone(),
            entries: refs,
        });
        staged.insert(
            commit_row.commit_id.clone(),
            StagedChangelogCommit {
                change_ids,
                selected_change_refs: commit_row.selected_change_refs.clone(),
                commit_change_id: commit_row.change_id.clone(),
                commit_created_at: commit_row.created_at.clone(),
            },
        );
    }

    let append = ChangelogAppend {
        commits,
        changes,
        commit_change_refs,
    };

    let mut writer = ChangelogContext::new().writer(read, writes);
    writer.stage_append(append).await?;
    stage_commit_row_json_payloads(writes, commit_rows)?;
    Ok(staged)
}

fn change_record_from_state_row(row: &PreparedStateRow) -> Result<ChangeRecord, LixError> {
    let Some(change_id) = row.change_id.as_ref() else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked staged row is missing change_id before changelog change construction",
        ));
    };
    Ok(ChangeRecord {
        format_version: 1,
        change_id: change_id.clone(),
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot_ref: row.snapshot.as_ref().map(|snapshot| snapshot.json_ref),
        metadata_ref: row.metadata.as_ref().map(|metadata| metadata.json_ref),
        created_at: row.updated_at.clone(),
    })
}

fn stage_commit_row_json_payloads(
    writes: &mut StorageWriteSet,
    commit_rows: &[FinalizedCommitRow],
) -> Result<(), LixError> {
    let snapshots = commit_rows
        .iter()
        .map(|row| commit_row_snapshot_content(&row.commit_id))
        .collect::<Result<Vec<_>, _>>()?;
    JsonStoreContext::new().writer().stage_batch(
        writes,
        JsonWritePlacementRef::OutOfBand,
        snapshots
            .iter()
            .map(|snapshot| NormalizedJsonRef::new(snapshot.as_str())),
    )?;
    Ok(())
}

fn commit_row_snapshot_content(commit_id: &str) -> Result<String, LixError> {
    serde_json::to_string(&serde_json::json!({
        "id": commit_id,
    }))
    .map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to encode lix_commit snapshot: {error}"),
        )
    })
}

fn commit_change_ref_from_state_row(row: &PreparedStateRow, change_id: &str) -> CommitChangeRef {
    CommitChangeRef {
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        entity_id: row.entity_id.clone(),
        change_id: change_id.to_string(),
    }
}

fn commit_change_ref_from_selected_change_ref(
    change_ref: &StagedCommitChangeRef,
) -> CommitChangeRef {
    CommitChangeRef {
        schema_key: change_ref.schema_key.clone(),
        file_id: change_ref.file_id.clone(),
        entity_id: change_ref.entity_id.clone(),
        change_id: change_ref.change_id.clone(),
    }
}

fn tracked_delta_from_state_row(
    row: &PreparedStateRow,
) -> Result<TrackedStateDeltaRef<'_>, LixError> {
    let Some(change_id) = row.change_id.as_deref() else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked staged row is missing change_id before tracked root staging",
        ));
    };
    let Some(commit_id) = row.commit_id.as_deref() else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked staged row is missing commit_id before tracked root staging",
        ));
    };
    Ok(TrackedStateDeltaRef {
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        entity_id: &row.entity_id,
        change_id,
        commit_id,
        snapshot_ref: row.snapshot.as_ref().map(|snapshot| &snapshot.json_ref),
        metadata_ref: row.metadata.as_ref().map(|metadata| &metadata.json_ref),
        deleted: row.snapshot.is_none(),
        created_at: &row.created_at,
        updated_at: &row.updated_at,
    })
}

fn tracked_delta_from_selected_change_ref<'a>(
    change_ref: &'a StagedCommitChangeRef,
    commit_id: &'a str,
) -> Result<TrackedStateDeltaRef<'a>, LixError> {
    Ok(TrackedStateDeltaRef {
        schema_key: &change_ref.schema_key,
        file_id: change_ref.file_id.as_deref(),
        entity_id: &change_ref.entity_id,
        change_id: &change_ref.change_id,
        commit_id,
        snapshot_ref: change_ref.snapshot_ref.as_ref(),
        metadata_ref: change_ref.metadata_ref.as_ref(),
        deleted: change_ref.deleted,
        created_at: &change_ref.created_at,
        updated_at: &change_ref.updated_at,
    })
}

async fn stage_tracked_roots(
    read: &(impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    tracked_row_indices_by_commit: BTreeMap<String, Vec<RowIndex>>,
    tracked_roots: Vec<PendingTrackedRoot>,
    staged_commits: BTreeMap<String, StagedChangelogCommit>,
) -> Result<(), LixError> {
    let tracked_state = TrackedStateContext::new();
    let mut tracked_writer = tracked_state.writer(read, writes);
    for root in tracked_roots_parent_first(&tracked_roots)? {
        let staged = staged_commits.get(&root.commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked-state root for commit '{}' has no staged changelog facts",
                    root.commit_id
                ),
            )
        })?;
        let state_row_indices = tracked_row_indices_by_commit
            .get(&root.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if state_row_indices.len() > staged.change_ids.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "commit '{}' has {} tracked rows but only {} changelog changes",
                    root.commit_id,
                    state_row_indices.len(),
                    staged.change_ids.len()
                ),
            ));
        }
        let commit_snapshot = commit_row_snapshot_content(&root.commit_id)?;
        let commit_snapshot_ref = JsonRef::for_content(commit_snapshot.as_bytes());
        let commit_entity_id = EntityIdentity::single(root.commit_id.clone());
        let mut deltas = state_row_indices
            .iter()
            .map(|&row_index| tracked_delta_from_state_row(&state_rows[row_index]))
            .chain(staged.selected_change_refs.iter().map(|change_ref| {
                tracked_delta_from_selected_change_ref(change_ref, &root.commit_id)
            }))
            .collect::<Result<Vec<_>, _>>()?;
        deltas.push(TrackedStateDeltaRef {
            schema_key: "lix_commit",
            file_id: None,
            entity_id: &commit_entity_id,
            change_id: &staged.commit_change_id,
            commit_id: &root.commit_id,
            snapshot_ref: Some(&commit_snapshot_ref),
            metadata_ref: None,
            deleted: false,
            created_at: &staged.commit_created_at,
            updated_at: &staged.commit_created_at,
        });
        tracked_writer
            .stage_commit_root(&root.commit_id, root.parent_commit_id.as_deref(), deltas)
            .await?;
    }
    let rooted_commit_ids = tracked_roots
        .iter()
        .map(|root| root.commit_id.as_str())
        .collect::<BTreeSet<_>>();
    let extra_tracked = tracked_row_indices_by_commit
        .keys()
        .filter(|commit_id| !rooted_commit_ids.contains(commit_id.as_str()))
        .cloned()
        .collect::<BTreeSet<_>>();
    if !extra_tracked.is_empty() {
        let mut commit_ids = tracked_row_indices_by_commit
            .keys()
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
        let commit_ids = staged_commits
            .keys()
            .filter(|commit_id| !rooted_commit_ids.contains(commit_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if commit_ids.is_empty() {
            return Ok(());
        }
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "changelog staged commits without tracked root metadata: {}",
                commit_ids.join(", ")
            ),
        ));
    }
    Ok(())
}

fn tracked_roots_parent_first(
    tracked_roots: &[PendingTrackedRoot],
) -> Result<Vec<&PendingTrackedRoot>, LixError> {
    let mut roots_by_id = BTreeMap::new();
    for root in tracked_roots {
        if roots_by_id.insert(root.commit_id.as_str(), root).is_some() {
            return Err(LixError::unknown(format!(
                "cannot stage duplicate tracked_state root '{}'",
                root.commit_id
            )));
        }
    }

    let mut ordered = Vec::with_capacity(tracked_roots.len());
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for root in tracked_roots {
        visit_tracked_root_parent_first(
            root.commit_id.as_str(),
            &roots_by_id,
            &mut visiting,
            &mut visited,
            &mut ordered,
        )?;
    }
    Ok(ordered)
}

fn visit_tracked_root_parent_first<'a>(
    commit_id: &str,
    roots_by_id: &BTreeMap<&'a str, &'a PendingTrackedRoot>,
    visiting: &mut BTreeSet<&'a str>,
    visited: &mut BTreeSet<&'a str>,
    ordered: &mut Vec<&'a PendingTrackedRoot>,
) -> Result<(), LixError> {
    if visited.contains(commit_id) {
        return Ok(());
    }
    let Some(root) = roots_by_id.get(commit_id).copied() else {
        return Ok(());
    };
    if !visiting.insert(root.commit_id.as_str()) {
        return Err(LixError::unknown(format!(
            "cannot stage tracked_state root '{}' because staged root parents contain a cycle",
            root.commit_id
        )));
    }
    if let Some(parent_id) = root.parent_commit_id.as_deref() {
        if roots_by_id.contains_key(parent_id) {
            visit_tracked_root_parent_first(parent_id, roots_by_id, visiting, visited, ordered)?;
        }
    }
    visiting.remove(root.commit_id.as_str());
    visited.insert(root.commit_id.as_str());
    ordered.push(root);
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

/// Materializes tracked staged change refs into changelog commits.
///
/// Staging only accumulates `version_id -> change_ids` because commit ids,
/// parent heads, and commit-row timestamps belong to transaction finalization.
/// The `change_ids` list is the ordered set of canonical changes whose effects
/// the commit introduces relative to its first parent.
/// This function turns those change-ref sets into finalized commit facts.
///
/// Commit finalization output split by durability target.
///
/// `commit_rows` are canonical changelog commit facts. tracked_state roots store
/// serving commit roots keyed by the corresponding commit id.
///
/// `version_heads` are moving refs. They are written through `VersionContext`,
/// not the canonical changelog.
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
    selected_change_refs: Vec<StagedCommitChangeRef>,
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
    commit_change_refs_by_version: BTreeMap<String, StagedCommitChangeRefs>,
    extra_commit_parents_by_version: BTreeMap<String, Vec<String>>,
    version_ctx: &VersionContext,
    read: &(impl StorageRead + Send + Sync + ?Sized),
) -> Result<FinalizedCommitRows, LixError> {
    let mut commit_rows = Vec::new();
    let mut version_heads = Vec::new();
    let mut tracked_roots = Vec::new();

    for (version_id, change_refs) in commit_change_refs_by_version {
        if change_refs.is_empty() && !change_refs.allow_empty {
            continue;
        }

        let commit_id = change_refs.commit_id;
        let commit_change_id = change_refs.commit_change_id;
        let timestamp = change_refs.created_at;
        let selected_change_refs = change_refs.selected_change_refs;
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
            selected_change_refs,
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
        Backend, BackendCapabilities, BackendError, BackendWrite, CommitResult, DurableWriteLock,
        KeyRange, PutBatch,
    };
    use crate::catalog::SchemaPlanId;
    use crate::changelog::ChangelogReader;
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
    async fn commit_staged_writes_appends_changelog_and_updates_commit_root() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let binary_cas = BinaryCasContext::new();
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");

        let state_rows = vec![tracked_global_row("change-1")];
        let writes = commit_prepared_writes(
            &binary_cas,
            &version_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                commit_change_refs_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    change_refs(["change-1"]),
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

        let mut changelog_reader = crate::changelog::ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &["test-uuid-1".to_string()],
                projection: crate::changelog::CommitProjection::Full,
            })
            .await
            .expect("changelog commit should load");
        let Some(crate::changelog::CommitLoadEntry::Full {
            record,
            change_ref_chunks,
        }) = commits.entries.into_iter().next().flatten()
        else {
            panic!("changelog commit should exist");
        };
        assert_eq!(record.change_id, "test-uuid-2");
        assert!(change_ref_chunks
            .iter()
            .flat_map(|chunk| chunk.entries.iter())
            .any(|entry| entry.change_id == "change-1"));
        let changes = changelog_reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &["change-1".to_string(), record.change_id.clone()],
            })
            .await
            .expect("changelog change should load");
        let mut loaded_changes = changes.entries.into_iter();
        let Some(change) = loaded_changes.next().flatten() else {
            panic!("changelog change should exist");
        };
        assert_eq!(change.change_id, "change-1");
        assert_eq!(change.schema_key, "test_schema");
        assert!(
            loaded_changes.next().flatten().is_none(),
            "commit row change is derived from changelog.commit, not stored as changelog.change"
        );

        let mut tracked_reader = crate::tracked_state::TrackedStateContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let commit_rows = tracked_reader
            .scan_rows_at_commit(
                "test-uuid-1",
                &crate::tracked_state::TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["lix_commit".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("commit root should scan");
        assert!(
            commit_rows
                .iter()
                .any(|row| row.change_id == record.change_id && row.snapshot_content.is_some()),
            "commit root should surface the derived lix_commit row"
        );

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
    async fn stage_changelog_commits_orders_staged_parents_before_children() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let mut writes = StorageWriteSet::new();
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut parent_row = tracked_global_row("parent-change");
        parent_row.commit_id = Some("parent-commit".to_string());
        let mut child_row = tracked_global_row("child-change");
        child_row.commit_id = Some("child-commit".to_string());

        let commits = vec![
            FinalizedCommitRow {
                commit_id: "child-commit".to_string(),
                parent_commit_ids: vec!["parent-commit".to_string()],
                created_at: "2026-01-01T00:00:01Z".to_string(),
                change_id: "child-commit-change".to_string(),
                selected_change_refs: Vec::new(),
            },
            FinalizedCommitRow {
                commit_id: "parent-commit".to_string(),
                parent_commit_ids: Vec::new(),
                created_at: "2026-01-01T00:00:00Z".to_string(),
                change_id: "parent-commit-change".to_string(),
                selected_change_refs: Vec::new(),
            },
        ];
        stage_changelog_commits(
            &mut read,
            &mut writes,
            &[parent_row, child_row],
            &BTreeMap::from([
                ("parent-commit".to_string(), vec![0]),
                ("child-commit".to_string(), vec![1]),
            ]),
            &commits,
        )
        .await
        .expect("child-before-parent input should still stage parent first");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("writes should persist");

        let mut changelog_reader = crate::changelog::ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &["parent-commit".to_string(), "child-commit".to_string()],
                projection: crate::changelog::CommitProjection::Record,
            })
            .await
            .expect("commits should load");
        assert!(commits.entries.iter().all(Option::is_some));
    }

    #[tokio::test]
    async fn commit_with_only_untracked_writes_does_not_create_lix_commit() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let binary_cas = BinaryCasContext::new();
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        let untracked_state = UntrackedStateContext::new();
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");

        let state_rows = vec![untracked_global_row("change-untracked")];
        let writes = commit_prepared_writes(
            &binary_cas,
            &version_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                commit_change_refs_by_version: BTreeMap::new(),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("commit should flush untracked row");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("writes should commit");

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

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let state_rows = vec![tracked_global_row("change-tracked")];
        let writes = commit_prepared_writes(
            &binary_cas,
            &version_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                commit_change_refs_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    change_refs(["change-tracked"]),
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
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .expect("seed read should open");
            let mut writes = storage.new_write_set();
            crate::test_support::stage_tracked_root_from_materialized(
                &mut read,
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
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");

        let tracked_row = tracked_global_row("change-tracked");
        let mut untracked_row = untracked_global_row("change-untracked");
        untracked_row.entity_id = crate::entity_identity::EntityIdentity::single("entity-2");

        let writes = commit_prepared_writes(
            &binary_cas,
            &version_ctx,
            Some(&runtime_functions),
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![tracked_row, untracked_row],
                commit_change_refs_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    change_refs(["change-tracked"]),
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

        let mut changelog_reader = crate::changelog::ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &["test-uuid-1".to_string()],
                projection: crate::changelog::CommitProjection::Record,
            })
            .await
            .expect("changelog commit should load");
        let Some(crate::changelog::CommitLoadEntry::Record(commit)) =
            commits.entries.into_iter().next().flatten()
        else {
            panic!("changelog commit should exist");
        };
        assert_eq!(commit.change_id, "test-uuid-2");
        let changes = changelog_reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &["change-tracked".to_string()],
            })
            .await
            .expect("changelog change should load");
        assert!(matches!(
            changes.entries.as_slice(),
            [Some(change)] if change.change_id == "change-tracked"
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

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let state_rows = vec![tracked_version_row("version-a", "change-version-a")];
        let writes = commit_prepared_writes(
            &binary_cas,
            &version_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                commit_change_refs_by_version: BTreeMap::from([(
                    "version-a".to_string(),
                    change_refs(["change-version-a"]),
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

        let mut changelog_reader = crate::changelog::ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &["test-uuid-1".to_string()],
                projection: crate::changelog::CommitProjection::Record,
            })
            .await
            .expect("changelog commit should load");
        let Some(crate::changelog::CommitLoadEntry::Record(commit)) =
            commits.entries.into_iter().next().flatten()
        else {
            panic!("changelog commit should exist");
        };
        assert_eq!(commit.change_id, "test-uuid-2");
        assert_eq!(commit.parent_commit_ids, vec!["version-a-before"]);

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

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_VERSION_ID.to_string(),
                change_refs(["change-a", "change-b"]),
            )]),
            BTreeMap::new(),
            &version_ctx,
            &mut read,
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
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_VERSION_ID.to_string(),
                StagedCommitChangeRefs::default(),
            )]),
            BTreeMap::new(),
            &version_ctx,
            &mut read,
        )
        .await
        .expect("empty change_refs should be ignored");

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

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([("version-a".to_string(), change_refs(["change-a"]))]),
            BTreeMap::new(),
            &version_ctx,
            &mut read,
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

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([("version-a".to_string(), change_refs(["change-a"]))]),
            BTreeMap::from([("version-a".to_string(), vec!["source-head".to_string()])]),
            &version_ctx,
            &mut read,
        )
        .await
        .expect("merge commit finalization should resolve parents");

        assert_eq!(
            rows.commit_rows[0].parent_commit_ids,
            vec!["target-head", "source-head"]
        );
    }

    fn change_refs<const N: usize>(change_ids: [&str; N]) -> StagedCommitChangeRefs {
        let mut change_refs = StagedCommitChangeRefs::new(
            "test-uuid-1".to_string(),
            "test-uuid-2".to_string(),
            "test-timestamp-1".to_string(),
        );
        for change_id in change_ids {
            change_refs.add_change_id(change_id.to_string());
        }
        change_refs
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

        fn durable_write_lock(&self) -> DurableWriteLock {
            self.inner.durable_write_lock()
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
