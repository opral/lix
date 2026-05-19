use crate::binary_cas::BinaryCasContext;
use crate::changelog::{
    ChangeLoadEntry, ChangeLoadRequest, ChangeLocator as ChangelogChangeLocator, ChangeProjection,
    ChangeRef as ChangelogChangeRef, ChangeVisibilityMode, ChangelogContext, CommitBody,
    CommitHeader, MembershipRecord, MembershipRole, Segment, SegmentChange, SegmentChangeDirectory,
    SegmentCommit, SegmentCommitDirectory, SegmentDirectory, SegmentHeader, SegmentInlinePayload,
    SegmentObjectLocation, StateRowIdentity, COMMIT_VISIBILITY_SPACE, VISIBLE_CHANGE_PROOF_SPACE,
};
use crate::common::{CanonicalSchemaKey, EntityId, FileId};
use crate::entity_identity::EntityIdentity;
use crate::functions::FunctionContext;
use crate::json_store::{JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
use crate::storage::{StorageRead, StorageWriteSet};
use crate::tracked_state::{
    TrackedStateContext, TrackedStateDeltaRef, TrackedStateDiffRow, TrackedStateRowRequest,
};
use crate::transaction::prepare_version_ref_row;
use crate::transaction::staging::PreparedWriteSet;
use crate::transaction::types::{PreparedAdoptedStateRow, PreparedStateRow, StagedCommitMembers};
use crate::untracked_state::{
    UntrackedStateContext, UntrackedStateIdentity, UntrackedStateIdentityRef, UntrackedStateRowRef,
};
use crate::version::{VersionContext, VersionRefReader};
use crate::LixError;
use std::collections::{BTreeMap, BTreeSet};

type RowIndex = usize;
type AdoptedRowIndex = usize;

/// Commits prepared transaction rows into durable tracked and untracked stores.
///
/// Providers decode DataFusion DML into hydrated `PreparedStateRow`s. Untracked
/// rows are durable local overlay state and bypass changelog membership. Tracked
/// rows stage canonical changelog facts, then update the live-state serving
/// projection. The tracked side of that projection is a prolly root keyed by
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
    let adopted_rows = prepared_writes.adopted_rows;
    let finalized = finalize_commit_rows(
        prepared_writes.commit_members_by_version,
        prepared_writes.extra_commit_parents_by_version,
        version_ctx,
        &*read,
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

    let (staged_commits, _staged_commit_locations) = stage_changelog_commits(
        read,
        &mut writes,
        &state_rows,
        &row_index.tracked_row_indices_by_commit,
        &adopted_rows,
        &adopted_index.tracked_row_indices_by_commit,
        &commit_rows,
    )
    .await?;

    stage_untracked_json_payloads(
        &mut json_writer,
        &mut writes,
        &state_rows,
        &row_index.untracked_row_indices,
    )?;

    // The serving projection is updated in the same backend transaction as the
    // changelog append. Tracked rows become prolly mutations under their owning
    // commit root; untracked rows remain in the separate local overlay store.
    {
        let untracked_overlay_delete_identities = existing_untracked_overlay_delete_identities(
            &*read,
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

    writes.move_space_to_end(VISIBLE_CHANGE_PROOF_SPACE);
    writes.move_space_to_end(COMMIT_VISIBILITY_SPACE);

    Ok(writes)
}

fn stage_untracked_json_payloads(
    json_writer: &mut crate::json_store::JsonStoreWriter,
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    untracked_row_indices: &[RowIndex],
) -> Result<(), LixError> {
    json_writer.stage_batch(
        writes,
        JsonWritePlacementRef::OutOfBand,
        untracked_row_indices
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

async fn validate_adopted_rows_match_source_parent(
    read: &mut (impl StorageRead + Send + Sync),
    state_rows: &[PreparedStateRow],
    tracked_row_indices_by_commit: &BTreeMap<String, Vec<RowIndex>>,
    rows: &[PreparedAdoptedStateRow],
) -> Result<(), LixError> {
    if rows.is_empty() {
        return Ok(());
    }

    let staged_source_rows =
        staged_source_rows_by_commit_and_key(state_rows, tracked_row_indices_by_commit)?;
    let mut reader = TrackedStateContext::new().reader(read);
    let mut requests_by_source_parent =
        BTreeMap::<String, Vec<(usize, TrackedStateRowRequest)>>::new();
    for (index, row) in rows.iter().enumerate() {
        validate_adopted_row_payload_refs(row)?;
        let key = adopted_row_tracked_key(row);
        if let Some(source_row) =
            staged_source_rows.get(&(row.source_parent_commit_id.clone(), key.clone()))
        {
            validate_adopted_row_matches_staged_source(row, source_row)?;
            continue;
        }
        requests_by_source_parent
            .entry(row.source_parent_commit_id.clone())
            .or_default()
            .push((
                index,
                TrackedStateRowRequest {
                    schema_key: row.schema_key.clone(),
                    entity_id: row.entity_id.clone(),
                    file_id: match &row.file_id {
                        Some(file_id) => crate::NullableKeyFilter::Value(file_id.clone()),
                        None => crate::NullableKeyFilter::Null,
                    },
                },
            ));
    }

    for (source_parent_commit_id, indexed_requests) in requests_by_source_parent {
        let requests = indexed_requests
            .iter()
            .map(|(_, request)| request.clone())
            .collect::<Vec<_>>();
        let source_entries = reader
            .load_index_entries_at_commit(&source_parent_commit_id, &requests)
            .await?;
        for ((row_index, _), source_entry) in indexed_requests.iter().zip(&source_entries) {
            if source_entry.is_none() {
                let row = &rows[*row_index];
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "adopted change '{}' is missing from source parent '{}' tracked projection",
                        row.change_id, row.source_parent_commit_id
                    ),
                ));
            }
        }
        let mut validation_rows = Vec::new();
        for (source_entry, (row_index, _)) in source_entries.iter().zip(&indexed_requests) {
            if let Some(source_row) = source_entry {
                validation_rows.push((
                    source_row,
                    rows[*row_index].source_parent_commit_id.as_str(),
                ));
            }
        }
        let validation_refs = validation_rows
            .iter()
            .map(|(row, commit_id)| (*row, *commit_id))
            .collect::<Vec<_>>();
        reader
            .validate_diff_rows_for_commits_against_changelog(&validation_refs)
            .await?;
        for ((row_index, _), source_entry) in indexed_requests.into_iter().zip(source_entries) {
            let row = &rows[row_index];
            let Some(source_row) = source_entry else { unreachable!() };
            validate_adopted_row_matches_source_parent(row, &source_row)?;
        }
    }
    Ok(())
}

fn validate_adopted_rows_are_parented_by_commits(
    adopted_rows: &[PreparedAdoptedStateRow],
    adopted_row_indices_by_commit: &BTreeMap<String, Vec<AdoptedRowIndex>>,
    commit_rows: &[FinalizedCommitRow],
) -> Result<(), LixError> {
    let mut indexed_rows = vec![false; adopted_rows.len()];
    let commit_rows_by_id = commit_rows
        .iter()
        .map(|row| (row.commit_id.as_str(), row))
        .collect::<BTreeMap<_, _>>();
    for (commit_id, row_indices) in adopted_row_indices_by_commit {
        let Some(commit_row) = commit_rows_by_id.get(commit_id.as_str()) else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("adopted rows for commit '{commit_id}' have no finalized commit row"),
            ));
        };
        for &row_index in row_indices {
            let Some(row_seen) = indexed_rows.get_mut(row_index) else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("adopted row index {row_index} for commit '{commit_id}' is out of bounds"),
                ));
            };
            *row_seen = true;
            adopted_source_parent_ordinal(commit_row, &adopted_rows[row_index])?;
        }
    }
    if let Some(row_index) = indexed_rows.iter().position(|seen| !seen) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("adopted row index {row_index} is not assigned to a finalized commit"),
        ));
    }
    Ok(())
}

fn staged_source_rows_by_commit_and_key<'a>(
    state_rows: &'a [PreparedStateRow],
    tracked_row_indices_by_commit: &BTreeMap<String, Vec<RowIndex>>,
) -> Result<BTreeMap<(String, AdoptedProjectionKey), &'a PreparedStateRow>, LixError> {
    let mut out = BTreeMap::new();
    for (commit_id, indices) in tracked_row_indices_by_commit {
        for &index in indices {
            let row = &state_rows[index];
            out.insert((commit_id.clone(), state_row_tracked_key(row)), row);
        }
    }
    Ok(out)
}

fn adopted_row_tracked_key(row: &PreparedAdoptedStateRow) -> AdoptedProjectionKey {
    AdoptedProjectionKey {
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        entity_id: row.entity_id.clone(),
    }
}

fn state_row_tracked_key(row: &PreparedStateRow) -> AdoptedProjectionKey {
    AdoptedProjectionKey {
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        entity_id: row.entity_id.clone(),
    }
}

fn validate_adopted_row_matches_staged_source(
    row: &PreparedAdoptedStateRow,
    source_row: &PreparedStateRow,
) -> Result<(), LixError> {
    if source_row.change_id.as_deref() != Some(row.change_id.as_str())
        || source_row.commit_id.as_deref() != Some(row.source_commit_id.as_str())
        || source_row.snapshot.as_ref().map(|json| json.json_ref) != row.snapshot_ref
        || source_row.metadata.as_ref().map(|json| json.json_ref) != row.metadata_ref
        || source_row.created_at != row.created_at
        || source_row.updated_at != row.updated_at
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "adopted change '{}' projection does not match source parent '{}' tracked row",
                row.change_id, row.source_parent_commit_id
            ),
        ));
    }
    Ok(())
}

fn validate_adopted_row_matches_source_parent(
    row: &PreparedAdoptedStateRow,
    source_row: &TrackedStateDiffRow,
) -> Result<(), LixError> {
    if source_row.change_id != row.change_id
        || source_row.commit_id != row.source_commit_id
        || source_row.deleted != row.snapshot_ref.is_none()
        || source_row.snapshot_ref != row.snapshot_ref
        || source_row.metadata_ref != row.metadata_ref
        || source_row.created_at != row.created_at
        || source_row.updated_at != row.updated_at
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "adopted change '{}' projection does not match source parent '{}' tracked row",
                row.change_id, row.source_parent_commit_id
            ),
        ));
    }
    Ok(())
}

fn validate_adopted_row_payload_refs(row: &PreparedAdoptedStateRow) -> Result<(), LixError> {
    let snapshot_ref = row.snapshot.as_ref().map(|json| json.json_ref);
    if snapshot_ref != row.snapshot_ref {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "adopted change '{}' snapshot_ref does not match materialized snapshot",
                row.change_id
            ),
        ));
    }
    let metadata_ref = row.metadata.as_ref().map(|json| json.json_ref);
    if metadata_ref != row.metadata_ref {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "adopted change '{}' metadata_ref does not match materialized metadata",
                row.change_id
            ),
        ));
    }
    Ok(())
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

#[derive(Clone, Debug)]
struct StagedChangelogCommit {
    authored_locators: Vec<ChangelogChangeLocator>,
    adopted_locators: Vec<ChangelogChangeLocator>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct AdoptedProjectionKey {
    schema_key: String,
    file_id: Option<String>,
    entity_id: EntityIdentity,
}

async fn stage_changelog_commits(
    read: &mut (impl StorageRead + Send + Sync),
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    tracked_row_indices_by_commit: &BTreeMap<String, Vec<RowIndex>>,
    adopted_rows: &[PreparedAdoptedStateRow],
    adopted_row_indices_by_commit: &BTreeMap<String, Vec<AdoptedRowIndex>>,
    commit_rows: &[FinalizedCommitRow],
) -> Result<
    (
        BTreeMap<String, StagedChangelogCommit>,
        BTreeMap<String, SegmentObjectLocation>,
    ),
    LixError,
> {
    validate_adopted_rows_are_parented_by_commits(
        adopted_rows,
        adopted_row_indices_by_commit,
        commit_rows,
    )?;
    if commit_rows.is_empty() {
        return Ok((BTreeMap::new(), BTreeMap::new()));
    }
    validate_adopted_rows_match_source_parent(
        read,
        state_rows,
        tracked_row_indices_by_commit,
        adopted_rows,
    )
    .await?;

    let mut commits = Vec::with_capacity(commit_rows.len());
    let mut changes = Vec::new();
    let mut authored_change_ids_by_commit = BTreeMap::<String, Vec<String>>::new();
    let mut adopted_change_ids_by_commit = BTreeMap::<String, Vec<String>>::new();
    for commit_row in commit_rows {
        let state_row_indices = tracked_row_indices_by_commit
            .get(&commit_row.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let adopted_row_indices = adopted_row_indices_by_commit
            .get(&commit_row.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let mut membership =
            Vec::with_capacity(state_row_indices.len() + adopted_row_indices.len());
        let mut state_row_identities = Vec::new();
        let mut membership_ordinals = Vec::new();
        let mut authored_change_ids = Vec::with_capacity(state_row_indices.len());
        for &row_index in state_row_indices {
            let row = &state_rows[row_index];
            let change_id = row.change_id.as_ref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked staged row is missing change_id before changelog append",
                )
            })?;
            membership.push(MembershipRecord {
                member_change_id: change_id.clone(),
                role: MembershipRole::Authored,
                source_parent_ordinal: None,
            });
            membership_ordinals.push((change_id.clone(), (membership.len() - 1) as u32));
            if let Some(identity) = state_row_identity_from_state_row(row)? {
                state_row_identities.push((identity, change_id.clone()));
            }
            authored_change_ids.push(change_id.clone());
            changes.push(segment_change_from_state_row(row, &commit_row.commit_id)?);
        }
        let mut adopted_change_ids = Vec::with_capacity(adopted_row_indices.len());
        for &row_index in adopted_row_indices {
            let row = &adopted_rows[row_index];
            membership.push(MembershipRecord {
                member_change_id: row.change_id.clone(),
                role: MembershipRole::Adopted,
                source_parent_ordinal: adopted_source_parent_ordinal(commit_row, row)?,
            });
            membership_ordinals.push((row.change_id.clone(), (membership.len() - 1) as u32));
            state_row_identities.push((
                state_row_identity_from_adopted_row(row)?,
                row.change_id.clone(),
            ));
            adopted_change_ids.push(row.change_id.clone());
        }
        authored_change_ids_by_commit.insert(commit_row.commit_id.clone(), authored_change_ids);
        adopted_change_ids_by_commit.insert(commit_row.commit_id.clone(), adopted_change_ids);
        commits.push(SegmentCommit {
            header: CommitHeader {
                id: commit_row.commit_id.clone(),
                parent_commit_ids: commit_row.parent_commit_ids.clone(),
                derivable_change_id: commit_row.change_id.clone(),
                author_account_ids: Vec::new(),
                created_at: commit_row.created_at.clone(),
                membership_count: 0,
            },
            body: CommitBody { membership },
            directory: SegmentCommitDirectory {
                state_row_identities,
                membership_ordinals,
            },
            checksum: String::new(),
        });
    }

    let segment = Segment {
        header: SegmentHeader {
            segment_id: segment_id_for_commit_rows(commit_rows),
            format_version: 0,
            commit_count: 0,
            change_count: 0,
            byte_count: 0,
            payload_count: 0,
            checksum: String::new(),
        },
        directory: SegmentDirectory::default(),
        commits,
        changes,
    };

    let mut writer = ChangelogContext::new().writer(read, writes);
    let report = writer.stage_segment(segment).await?;
    for commit_row in commit_rows_parent_first(commit_rows)? {
        writer.stage_publish_commit(&commit_row.commit_id).await?;
    }
    drop(writer);

    let change_locations = report
        .change_locations
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let commit_locations = report
        .commit_locations
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let adopted_locators_by_change = load_existing_changelog_locators(
        read,
        adopted_change_ids_by_commit
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<_>>(),
    )
    .await?;

    let mut staged = BTreeMap::new();
    for commit_row in commit_rows {
        let authored_locators = authored_change_ids_by_commit
            .remove(&commit_row.commit_id)
            .unwrap_or_default()
            .into_iter()
            .map(|change_id| {
                let location = change_locations.get(&change_id).cloned().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "changelog segment report is missing authored change '{change_id}'"
                        ),
                    )
                })?;
                Ok(ChangelogChangeLocator {
                    change_id,
                    commit_id: commit_row.commit_id.clone(),
                    location,
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let adopted_locators = adopted_change_ids_by_commit
            .remove(&commit_row.commit_id)
            .unwrap_or_default()
            .into_iter()
            .map(|change_id| {
                let locator = adopted_locators_by_change
                    .get(&change_id)
                    .cloned()
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("changelog locator is missing adopted change '{change_id}'"),
                        )
                    })?;
                Ok(locator)
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        staged.insert(
            commit_row.commit_id.clone(),
            StagedChangelogCommit {
                authored_locators,
                adopted_locators,
            },
        );
    }
    Ok((staged, commit_locations))
}

fn segment_id_for_commit_rows(commit_rows: &[FinalizedCommitRow]) -> String {
    let first = commit_rows
        .first()
        .map(|row| row.commit_id.as_str())
        .unwrap_or("empty");
    let last = commit_rows
        .last()
        .map(|row| row.commit_id.as_str())
        .unwrap_or(first);
    format!("txn-{first}-{last}-{}", commit_rows.len())
}

fn commit_rows_parent_first(
    commit_rows: &[FinalizedCommitRow],
) -> Result<Vec<&FinalizedCommitRow>, LixError> {
    let mut rows_by_id = BTreeMap::new();
    for row in commit_rows {
        if rows_by_id.insert(row.commit_id.as_str(), row).is_some() {
            return Err(LixError::unknown(format!(
                "cannot publish duplicate changelog commit '{}'",
                row.commit_id
            )));
        }
    }

    let mut ordered = Vec::with_capacity(commit_rows.len());
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for row in commit_rows {
        visit_commit_row_parent_first(
            row.commit_id.as_str(),
            &rows_by_id,
            &mut visiting,
            &mut visited,
            &mut ordered,
        )?;
    }
    Ok(ordered)
}

fn visit_commit_row_parent_first<'a>(
    commit_id: &str,
    rows_by_id: &BTreeMap<&'a str, &'a FinalizedCommitRow>,
    visiting: &mut BTreeSet<&'a str>,
    visited: &mut BTreeSet<&'a str>,
    ordered: &mut Vec<&'a FinalizedCommitRow>,
) -> Result<(), LixError> {
    if visited.contains(commit_id) {
        return Ok(());
    }
    let Some(row) = rows_by_id.get(commit_id).copied() else {
        return Ok(());
    };
    if !visiting.insert(row.commit_id.as_str()) {
        return Err(LixError::unknown(format!(
            "cannot publish changelog commit '{}' because staged commit parents contain a cycle",
            row.commit_id
        )));
    }
    for parent_id in &row.parent_commit_ids {
        if rows_by_id.contains_key(parent_id.as_str()) {
            visit_commit_row_parent_first(
                parent_id.as_str(),
                rows_by_id,
                visiting,
                visited,
                ordered,
            )?;
        }
    }
    visiting.remove(row.commit_id.as_str());
    visited.insert(row.commit_id.as_str());
    ordered.push(row);
    Ok(())
}

fn segment_change_from_state_row(
    row: &PreparedStateRow,
    commit_id: &str,
) -> Result<SegmentChange, LixError> {
    let Some(change_id) = row.change_id.as_ref() else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked staged row is missing change_id before changelog change construction",
        ));
    };
    Ok(SegmentChange {
        id: change_id.clone(),
        authored_commit_id: Some(commit_id.to_string()),
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot_ref: row.snapshot.as_ref().map(|snapshot| snapshot.json_ref),
        metadata_ref: row.metadata.as_ref().map(|metadata| metadata.json_ref),
        created_at: row.updated_at.clone(),
        inline_payloads: inline_payloads_from_state_row(row),
        directory: SegmentChangeDirectory::default(),
    })
}

fn inline_payloads_from_state_row(row: &PreparedStateRow) -> Vec<SegmentInlinePayload> {
    row.snapshot
        .iter()
        .map(|snapshot| SegmentInlinePayload {
            json_ref: snapshot.json_ref,
            bytes: snapshot.normalized.as_bytes().to_vec(),
        })
        .chain(row.metadata.iter().map(|metadata| SegmentInlinePayload {
            json_ref: metadata.json_ref,
            bytes: metadata.normalized.as_bytes().to_vec(),
        }))
        .collect()
}

fn state_row_identity_from_state_row(
    row: &PreparedStateRow,
) -> Result<Option<StateRowIdentity>, LixError> {
    Ok(Some(StateRowIdentity {
        schema_key: CanonicalSchemaKey::new(row.schema_key.clone())?,
        file_id: FileId::new(
            row.file_id
                .clone()
                .unwrap_or_else(|| "__global__".to_string()),
        )?,
        entity_id: state_row_entity_id(&row.entity_id)?,
    }))
}

fn state_row_identity_from_adopted_row(
    row: &PreparedAdoptedStateRow,
) -> Result<StateRowIdentity, LixError> {
    Ok(StateRowIdentity {
        schema_key: CanonicalSchemaKey::new(row.schema_key.clone())?,
        file_id: FileId::new(
            row.file_id
                .clone()
                .unwrap_or_else(|| "__global__".to_string()),
        )?,
        entity_id: state_row_entity_id(&row.entity_id)?,
    })
}

fn state_row_entity_id(entity_id: &EntityIdentity) -> Result<EntityId, LixError> {
    EntityId::new(entity_id.as_json_array_text()?)
}

async fn load_existing_changelog_locators(
    read: &(impl StorageRead + Send + Sync + ?Sized),
    change_ids: Vec<String>,
) -> Result<BTreeMap<String, ChangelogChangeLocator>, LixError> {
    if change_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut unique = change_ids;
    unique.sort();
    unique.dedup();
    let mut reader = ChangelogContext::new().reader(read);
    let logical = reader
        .load_changes(ChangeLoadRequest {
            change_ids: &unique,
            projection: ChangeProjection::Logical,
            visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
        })
        .await?;
    let physical = reader
        .load_changes(ChangeLoadRequest {
            change_ids: &unique,
            projection: ChangeProjection::PhysicalLocation,
            visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
        })
        .await?;
    let mut out = BTreeMap::new();
    for ((change_id, logical), physical) in unique
        .into_iter()
        .zip(logical.entries)
        .zip(physical.entries)
    {
        let Some(ChangeLoadEntry::Logical(change)) = logical else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog adopted change '{change_id}' is not visible"),
            ));
        };
        let Some(ChangeLoadEntry::PhysicalLocation(location)) = physical else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog location is missing adopted change '{change_id}'"),
            ));
        };
        let Some(authored_commit_id) = change.authored_commit_id else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog adopted change '{change_id}' has no authored commit"),
            ));
        };
        if change.id != change_id {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "changelog adopted change lookup for '{change_id}' returned '{}'",
                    change.id
                ),
            ));
        }
        out.insert(
            change_id.clone(),
            ChangelogChangeLocator {
                change_id,
                commit_id: authored_commit_id,
                location,
            },
        );
    }
    Ok(out)
}

fn adopted_source_parent_ordinal(
    commit_row: &FinalizedCommitRow,
    row: &PreparedAdoptedStateRow,
) -> Result<Option<u32>, LixError> {
    let Some(ordinal) = commit_row
        .parent_commit_ids
        .iter()
        .position(|parent_id| parent_id == &row.source_parent_commit_id)
    else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "adopted change '{}' from source parent '{}' is not parented by adopting commit '{}'",
                row.change_id, row.source_parent_commit_id, commit_row.commit_id
            ),
        ));
    };
    Ok(Some(ordinal as u32))
}

fn changelog_change_ref_from_adopted_row(row: &PreparedAdoptedStateRow) -> ChangelogChangeRef<'_> {
    ChangelogChangeRef {
        id: &row.change_id,
        authored_commit_id: Some(&row.source_commit_id),
        entity_id: &row.entity_id,
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        snapshot_ref: row.snapshot_ref.as_ref(),
        metadata_ref: row.metadata_ref.as_ref(),
        created_at: &row.created_at,
    }
}

fn changelog_change_ref_from_state_row(
    row: &PreparedStateRow,
) -> Result<ChangelogChangeRef<'_>, LixError> {
    let Some(change_id) = row.change_id.as_deref() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked staged row is missing change_id before changelog delta staging",
        ));
    };

    Ok(ChangelogChangeRef {
        id: change_id,
        authored_commit_id: row.commit_id.as_ref(),
        entity_id: &row.entity_id,
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        snapshot_ref: row.snapshot.as_ref().map(|snapshot| &snapshot.json_ref),
        metadata_ref: row.metadata.as_ref().map(|metadata| &metadata.json_ref),
        created_at: &row.updated_at,
    })
}

async fn stage_tracked_roots(
    read: &(impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    mut tracked_row_indices_by_commit: BTreeMap<String, Vec<RowIndex>>,
    adopted_rows: &[PreparedAdoptedStateRow],
    mut adopted_row_indices_by_commit: BTreeMap<String, Vec<AdoptedRowIndex>>,
    tracked_roots: Vec<PendingTrackedRoot>,
    mut staged_commits: BTreeMap<String, StagedChangelogCommit>,
) -> Result<(), LixError> {
    let tracked_state = TrackedStateContext::new();
    let mut writer = tracked_state.writer(read, writes);
    for root in tracked_roots_parent_first(&tracked_roots)? {
        let staged = staged_commits.remove(&root.commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked-state root for commit '{}' has no staged changelog locators",
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
                    "commit '{}' has {} tracked authored rows but {} changelog authored locators",
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
                    "commit '{}' has {} tracked adopted rows but {} changelog adopted locators",
                    root.commit_id,
                    adopted_row_indices.len(),
                    staged.adopted_locators.len()
                ),
            ));
        }
        let authored_changes = state_row_indices
            .iter()
            .map(|&row_index| changelog_change_ref_from_state_row(&state_rows[row_index]))
            .collect::<Result<Vec<_>, _>>()?;
        let adopted_changes = adopted_row_indices
            .iter()
            .map(|&row_index| changelog_change_ref_from_adopted_row(&adopted_rows[row_index]))
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
        writer
            .stage_projection_root(&root.commit_id, root.parent_commit_id.as_deref(), deltas)
            .await?;
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

/// Materializes tracked staged membership into changelog commits.
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
/// `commit_rows` are canonical changelog commit facts. tracked_state roots store
/// serving projections keyed by the corresponding commit id.
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
    async fn commit_staged_writes_appends_changelog_and_updates_serving_projection() {
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

        let mut changelog_reader = crate::changelog::ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &["test-uuid-1".to_string()],
                projection: crate::changelog::CommitProjection::Full,
                visibility: crate::changelog::CommitVisibilityMode::RequireVisible,
            })
            .await
            .expect("changelog commit should load");
        let Some(crate::changelog::CommitLoadEntry::Full { header, body }) =
            commits.entries.into_iter().next().flatten()
        else {
            panic!("changelog commit should exist");
        };
        assert_eq!(header.derivable_change_id, "test-uuid-2");
        assert_eq!(body.membership.len(), 1);
        let changes = changelog_reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: crate::changelog::ChangeProjection::Segment,
                visibility:
                    crate::changelog::ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await
            .expect("changelog change should load");
        let Some(crate::changelog::ChangeLoadEntry::Segment(change)) =
            changes.entries.into_iter().next().flatten()
        else {
            panic!("changelog change should exist");
        };
        assert_eq!(change.id, "change-1");
        assert_eq!(change.schema_key, "test_schema");

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
    async fn changelog_adopted_membership_records_source_parent_ordinal() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let mut writes = StorageWriteSet::new();
        let mut source_read = storage
            .begin_read(StorageReadOptions::default())
            .expect("source read should open");
        let mut source_row = tracked_global_row("source-change");
        source_row.commit_id = Some("source-commit".to_string());
        let source_commits = vec![
            FinalizedCommitRow {
                commit_id: "target-commit".to_string(),
                parent_commit_ids: Vec::new(),
                created_at: "2026-01-01T00:00:00Z".to_string(),
                change_id: "target-commit-change".to_string(),
            },
            FinalizedCommitRow {
                commit_id: "source-commit".to_string(),
                parent_commit_ids: Vec::new(),
                created_at: "2026-01-01T00:00:00Z".to_string(),
                change_id: "source-commit-change".to_string(),
            },
            FinalizedCommitRow {
                commit_id: "source-head".to_string(),
                parent_commit_ids: vec!["source-commit".to_string()],
                created_at: "2026-01-01T00:00:01Z".to_string(),
                change_id: "source-head-change".to_string(),
            },
        ];
        stage_changelog_commits(
            &mut source_read,
            &mut writes,
            &[source_row],
            &BTreeMap::from([("source-commit".to_string(), vec![0])]),
            &[],
            &BTreeMap::new(),
            &source_commits,
        )
        .await
        .expect("source commit should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("source commit should persist");
        let mut root_read = storage
            .begin_read(StorageReadOptions::default())
            .expect("root rebuild read should open");
        let mut root_writes = StorageWriteSet::new();
        crate::tracked_state::TrackedStateContext::new()
            .root_rebuilder(&mut root_read, &mut root_writes)
            .ensure_projection_root("source-commit")
            .await
            .expect("source-commit root should rebuild");
        storage
            .commit_write_set(root_writes, StorageWriteOptions::default())
            .expect("source-commit root should persist");

        let mut root_read = storage
            .begin_read(StorageReadOptions::default())
            .expect("root rebuild read should open");
        let mut root_writes = StorageWriteSet::new();
        crate::tracked_state::TrackedStateContext::new()
            .root_rebuilder(&mut root_read, &mut root_writes)
            .ensure_projection_root("source-head")
            .await
            .expect("source-head root should rebuild");
        storage
            .commit_write_set(root_writes, StorageWriteOptions::default())
            .expect("source-head root should persist");

        let mut adopted_writes = StorageWriteSet::new();
        let mut adopted_read = storage
            .begin_read(StorageReadOptions::default())
            .expect("adopted read should open");
        let adopted_row = adopted_global_row(
            "source-change",
            "source-commit",
            "source-head",
            "merge-commit",
        );
        let merge_commits = vec![FinalizedCommitRow {
            commit_id: "merge-commit".to_string(),
            parent_commit_ids: vec!["target-commit".to_string(), "source-head".to_string()],
            created_at: "2026-01-01T00:00:01Z".to_string(),
            change_id: "merge-commit-change".to_string(),
        }];
        stage_changelog_commits(
            &mut adopted_read,
            &mut adopted_writes,
            &[],
            &BTreeMap::new(),
            &[adopted_row],
            &BTreeMap::from([("merge-commit".to_string(), vec![0])]),
            &merge_commits,
        )
        .await
        .expect("adopting commit should stage");
        storage
            .commit_write_set(adopted_writes, StorageWriteOptions::default())
            .expect("adopting commit should persist");

        let mut changelog_reader = crate::changelog::ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &["merge-commit".to_string()],
                projection: crate::changelog::CommitProjection::Body,
                visibility: crate::changelog::CommitVisibilityMode::RequireVisible,
            })
            .await
            .expect("adopting commit should load");
        let Some(crate::changelog::CommitLoadEntry::Body(body)) =
            commits.entries.into_iter().next().flatten()
        else {
            panic!("adopting commit body should exist");
        };
        assert_eq!(body.membership.len(), 1);
        assert_eq!(body.membership[0].member_change_id, "source-change");
        assert_eq!(body.membership[0].role, MembershipRole::Adopted);
        assert_eq!(body.membership[0].source_parent_ordinal, Some(1));
    }

    #[tokio::test]
    async fn commit_rejects_adopted_projection_that_differs_from_source_parent() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let binary_cas = BinaryCasContext::new();
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));

        let mut source_read = storage
            .begin_read(StorageReadOptions::default())
            .expect("source read should open");
        let mut source_row = tracked_global_row("source-change");
        source_row.commit_id = Some("source-commit".to_string());
        let source_writes = commit_prepared_writes(
            &binary_cas,
            &version_ctx,
            None,
            &mut source_read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![source_row],
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    members_with_commit(
                        "source-commit",
                        "source-commit-change",
                        ["source-change"],
                    ),
                )]),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("source commit should prepare");
        storage
            .commit_write_set(source_writes, StorageWriteOptions::default())
            .expect("source commit should persist");

        let forged_snapshot = crate::transaction::types::stage_json_from_value(
            crate::transaction::types::TransactionJson::from_value_for_test(
                serde_json::json!({ "value": 999 }),
            ),
            "forged adopted row snapshot",
        )
        .expect("forged snapshot should stage");
        let mut adopted_row =
            adopted_global_row("source-change", "source-commit", "source-commit", "adopt-commit");
        adopted_row.snapshot_ref = Some(forged_snapshot.json_ref);
        adopted_row.snapshot = Some(forged_snapshot);

        let mut adopt_read = storage
            .begin_read(StorageReadOptions::default())
            .expect("adopt read should open");
        let err = commit_prepared_writes(
            &binary_cas,
            &version_ctx,
            None,
            &mut adopt_read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: Vec::new(),
                adopted_rows: vec![adopted_row],
                commit_members_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    members_with_commit("adopt-commit", "adopt-commit-change", ["source-change"]),
                )]),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect_err("forged adopted projection should be rejected");
        assert!(
            err.message
                .contains("projection does not match source parent"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn commit_rejects_adopted_projection_matching_corrupt_source_root() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let binary_cas = BinaryCasContext::new();
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));

        let mut source_read = storage
            .begin_read(StorageReadOptions::default())
            .expect("source read should open");
        let mut source_row = tracked_global_row("source-change");
        source_row.commit_id = Some("source-commit".to_string());
        let source_writes = commit_prepared_writes(
            &binary_cas,
            &version_ctx,
            None,
            &mut source_read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![source_row],
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    members_with_commit(
                        "source-commit",
                        "source-commit-change",
                        ["source-change"],
                    ),
                )]),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("source commit should prepare");
        storage
            .commit_write_set(source_writes, StorageWriteOptions::default())
            .expect("source commit should persist");

        let request = TrackedStateRowRequest {
            schema_key: "test_schema".to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            file_id: crate::NullableKeyFilter::Null,
        };
        let corrupt_row = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open");
            let entries = crate::tracked_state::TrackedStateContext::new()
                .reader(read)
                .load_index_entries_at_commit("source-commit", &[request])
                .await
                .expect("source row should load");
            entries
                .into_iter()
                .next()
                .flatten()
                .expect("source row should exist")
        };
        let forged_snapshot = crate::transaction::types::stage_json_from_value(
            crate::transaction::types::TransactionJson::from_value_for_test(
                serde_json::json!({ "value": 999 }),
            ),
            "forged adopted row snapshot",
        )
        .expect("forged snapshot should stage");
        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .expect("corrupt read should open");
            let mut writes = storage.new_write_set();
            let entity_id = crate::entity_identity::EntityIdentity::single("entity-1");
            let source_commit_id = "source-commit".to_string();
            let change = crate::changelog::ChangeRef {
                id: "source-change",
                authored_commit_id: Some(&source_commit_id),
                entity_id: &entity_id,
                schema_key: "test_schema",
                file_id: None,
                snapshot_ref: Some(&forged_snapshot.json_ref),
                metadata_ref: None,
                created_at: "2026-01-01T00:00:00Z",
            };
            let locator = crate::changelog::ChangeLocatorRef {
                change_id: &corrupt_row.change_id,
                commit_id: &corrupt_row.commit_id,
                location: corrupt_row.change_location.as_ref(),
            };
            crate::tracked_state::TrackedStateContext::new()
                .writer(&mut read, &mut writes)
                .stage_projection_root(
                    "source-commit",
                    None,
                    [TrackedStateDeltaRef {
                        change,
                        locator,
                        created_at: "2026-01-01T00:00:00Z",
                        updated_at: "2026-01-01T00:00:00Z",
                    }],
                )
                .await
                .expect("corrupt source root should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("corrupt source root should commit");
        }
        {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .expect("corrupt verification read should open");
            let request = TrackedStateRowRequest {
                schema_key: "test_schema".to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
                file_id: crate::NullableKeyFilter::Null,
            };
            let entries = crate::tracked_state::TrackedStateContext::new()
                .reader(read)
                .load_index_entries_at_commit("source-commit", &[request])
                .await
                .expect("corrupt source row should load");
            let stored_row = entries
                .into_iter()
                .next()
                .flatten()
                .expect("corrupt source row should exist");
            assert_eq!(stored_row.snapshot_ref, Some(forged_snapshot.json_ref));
        }

        let mut adopted_row =
            adopted_global_row("source-change", "source-commit", "source-commit", "adopt-commit");
        adopted_row.snapshot_ref = Some(forged_snapshot.json_ref);
        adopted_row.snapshot = Some(forged_snapshot);
        let mut adopt_read = storage
            .begin_read(StorageReadOptions::default())
            .expect("adopt read should open");
        let err = commit_prepared_writes(
            &binary_cas,
            &version_ctx,
            None,
            &mut adopt_read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: Vec::new(),
                adopted_rows: vec![adopted_row],
                commit_members_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    members_with_commit("adopt-commit", "adopt-commit-change", ["source-change"]),
                )]),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect_err("corrupt source projection should be rejected");
        assert!(
            err.message
                .contains("payload refs do not match changelog change"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn commit_rejects_adopted_change_that_is_not_source_parent_winner() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let mut writes = StorageWriteSet::new();
        let mut source_read = storage
            .begin_read(StorageReadOptions::default())
            .expect("source read should open");
        let mut source_row = tracked_global_row("source-change");
        source_row.commit_id = Some("source-commit".to_string());
        let source_commits = vec![
            FinalizedCommitRow {
                commit_id: "target-commit".to_string(),
                parent_commit_ids: Vec::new(),
                created_at: "2026-01-01T00:00:00Z".to_string(),
                change_id: "target-commit-change".to_string(),
            },
            FinalizedCommitRow {
                commit_id: "source-commit".to_string(),
                parent_commit_ids: Vec::new(),
                created_at: "2026-01-01T00:00:00Z".to_string(),
                change_id: "source-commit-change".to_string(),
            },
            FinalizedCommitRow {
                commit_id: "source-head".to_string(),
                parent_commit_ids: vec!["source-commit".to_string()],
                created_at: "2026-01-01T00:00:01Z".to_string(),
                change_id: "source-head-change".to_string(),
            },
        ];
        stage_changelog_commits(
            &mut source_read,
            &mut writes,
            &[source_row],
            &BTreeMap::from([("source-commit".to_string(), vec![0])]),
            &[],
            &BTreeMap::new(),
            &source_commits,
        )
        .await
        .expect("source history should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("source history should persist");
        let mut root_read = storage
            .begin_read(StorageReadOptions::default())
            .expect("root rebuild read should open");
        let mut root_writes = StorageWriteSet::new();
        crate::tracked_state::TrackedStateContext::new()
            .root_rebuilder(&mut root_read, &mut root_writes)
            .ensure_projection_root("source-head")
            .await
            .expect("source-head root should rebuild");
        storage
            .commit_write_set(root_writes, StorageWriteOptions::default())
            .expect("source-head root should persist");

        let mut adopted_writes = StorageWriteSet::new();
        let mut adopted_read = storage
            .begin_read(StorageReadOptions::default())
            .expect("adopted read should open");
        let adopted_row =
            adopted_global_row("source-change", "wrong-source", "source-head", "merge-commit");
        let merge_commits = vec![FinalizedCommitRow {
            commit_id: "merge-commit".to_string(),
            parent_commit_ids: vec!["target-commit".to_string(), "source-head".to_string()],
            created_at: "2026-01-01T00:00:02Z".to_string(),
            change_id: "merge-commit-change".to_string(),
        }];
        let err = stage_changelog_commits(
            &mut adopted_read,
            &mut adopted_writes,
            &[],
            &BTreeMap::new(),
            &[adopted_row],
            &BTreeMap::from([("merge-commit".to_string(), vec![0])]),
            &merge_commits,
        )
        .await
        .expect_err("non-winning source change should be rejected");
        assert!(
            err.message.contains("not the first-parent winner"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn commit_rejects_adopted_projection_missing_from_source_parent() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let mut writes = StorageWriteSet::new();
        let mut source_read = storage
            .begin_read(StorageReadOptions::default())
            .expect("source read should open");
        let source_commits = vec![
            FinalizedCommitRow {
                commit_id: "target-commit".to_string(),
                parent_commit_ids: Vec::new(),
                created_at: "2026-01-01T00:00:00Z".to_string(),
                change_id: "target-commit-change".to_string(),
            },
            FinalizedCommitRow {
                commit_id: "empty-source-parent".to_string(),
                parent_commit_ids: Vec::new(),
                created_at: "2026-01-01T00:00:00Z".to_string(),
                change_id: "empty-source-change".to_string(),
            },
        ];
        stage_changelog_commits(
            &mut source_read,
            &mut writes,
            &[],
            &BTreeMap::new(),
            &[],
            &BTreeMap::new(),
            &source_commits,
        )
        .await
        .expect("empty source parent should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("empty source parent should persist");
        let mut root_read = storage
            .begin_read(StorageReadOptions::default())
            .expect("empty root rebuild read should open");
        let mut root_writes = StorageWriteSet::new();
        crate::tracked_state::TrackedStateContext::new()
            .root_rebuilder(&mut root_read, &mut root_writes)
            .ensure_projection_root("empty-source-parent")
            .await
            .expect("empty source parent root should rebuild");
        storage
            .commit_write_set(root_writes, StorageWriteOptions::default())
            .expect("empty source parent root should persist");

        let mut adopted_writes = StorageWriteSet::new();
        let mut adopted_read = storage
            .begin_read(StorageReadOptions::default())
            .expect("adopted read should open");
        let adopted_row = adopted_global_row(
            "source-change",
            "source-commit",
            "empty-source-parent",
            "merge-commit",
        );
        let merge_commits = vec![FinalizedCommitRow {
            commit_id: "merge-commit".to_string(),
            parent_commit_ids: vec![
                "target-commit".to_string(),
                "empty-source-parent".to_string(),
            ],
            created_at: "2026-01-01T00:00:02Z".to_string(),
            change_id: "merge-commit-change".to_string(),
        }];
        let err = stage_changelog_commits(
            &mut adopted_read,
            &mut adopted_writes,
            &[],
            &BTreeMap::new(),
            &[adopted_row],
            &BTreeMap::from([("merge-commit".to_string(), vec![0])]),
            &merge_commits,
        )
        .await
        .expect_err("missing source parent identity should be rejected");
        assert!(
            err.message.contains("is missing from source parent"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn commit_rejects_adopted_row_for_non_finalized_commit() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = StorageWriteSet::new();
        let adopted_row = adopted_global_row(
            "source-change",
            "source-commit",
            "source-parent",
            "missing-commit",
        );
        let err = stage_changelog_commits(
            &mut read,
            &mut writes,
            &[],
            &BTreeMap::new(),
            &[adopted_row],
            &BTreeMap::from([("missing-commit".to_string(), vec![0])]),
            &[FinalizedCommitRow {
                commit_id: "other-commit".to_string(),
                parent_commit_ids: vec!["source-parent".to_string()],
                created_at: "2026-01-01T00:00:01Z".to_string(),
                change_id: "other-commit-change".to_string(),
            }],
        )
        .await
        .expect_err("adopted rows without finalized commit should be rejected");
        assert!(
            err.message.contains("have no finalized commit row"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn commit_rejects_adopted_row_without_any_finalized_commit() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = StorageWriteSet::new();
        let adopted_row = adopted_global_row(
            "source-change",
            "source-commit",
            "source-parent",
            "missing-commit",
        );
        let err = stage_changelog_commits(
            &mut read,
            &mut writes,
            &[],
            &BTreeMap::new(),
            &[adopted_row],
            &BTreeMap::from([("missing-commit".to_string(), vec![0])]),
            &[],
        )
        .await
        .expect_err("adopted rows without any finalized commit should be rejected");
        assert!(
            err.message.contains("have no finalized commit row"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn commit_rejects_adopted_row_whose_source_parent_is_not_parented() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = StorageWriteSet::new();
        let adopted_row =
            adopted_global_row("source-change", "source-commit", "source-parent", "merge-commit");
        let err = stage_changelog_commits(
            &mut read,
            &mut writes,
            &[],
            &BTreeMap::new(),
            &[adopted_row],
            &BTreeMap::from([("merge-commit".to_string(), vec![0])]),
            &[FinalizedCommitRow {
                commit_id: "merge-commit".to_string(),
                parent_commit_ids: vec!["target-parent".to_string()],
                created_at: "2026-01-01T00:00:01Z".to_string(),
                change_id: "merge-commit-change".to_string(),
            }],
        )
        .await
        .expect_err("unparented adopted source parent should be rejected");
        assert!(
            err.message.contains("is not parented by adopting commit"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn commit_rejects_unindexed_adopted_row_before_source_validation() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = StorageWriteSet::new();
        let adopted_row =
            adopted_global_row("source-change", "source-commit", "source-parent", "merge-commit");
        let err = stage_changelog_commits(
            &mut read,
            &mut writes,
            &[],
            &BTreeMap::new(),
            &[adopted_row],
            &BTreeMap::new(),
            &[FinalizedCommitRow {
                commit_id: "merge-commit".to_string(),
                parent_commit_ids: vec!["source-parent".to_string()],
                created_at: "2026-01-01T00:00:01Z".to_string(),
                change_id: "merge-commit-change".to_string(),
            }],
        )
        .await
        .expect_err("unindexed adopted row should be rejected");
        assert!(
            err.message
                .contains("is not assigned to a finalized commit"),
            "{err:?}"
        );
    }

    #[test]
    fn adopted_projection_rejects_metadata_ref_mismatch() {
        let metadata = crate::transaction::types::stage_json_from_value(
            crate::transaction::types::TransactionJson::from_value_for_test(
                serde_json::json!({ "meta": true }),
            ),
            "test adopted row metadata",
        )
        .expect("metadata should stage");
        let mut row = adopted_global_row(
            "source-change",
            "source-commit",
            "source-parent",
            "adopt-commit",
        );
        row.metadata = Some(metadata);
        row.metadata_ref = Some(crate::json_store::JsonRef::for_content(
            br#"{"meta":"forged-ref"}"#,
        ));

        let err = validate_adopted_row_payload_refs(&row)
            .expect_err("metadata ref corruption should be rejected");
        assert!(
            err.message
                .contains("metadata_ref does not match materialized metadata"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn commit_rejects_adopted_metadata_ref_mismatch() {
        let mut row = adopted_global_row(
            "source-change",
            "source-commit",
            "source-parent",
            "merge-commit",
        );
        let metadata = crate::transaction::types::stage_json_from_value(
            crate::transaction::types::TransactionJson::from_value_for_test(
                serde_json::json!({ "meta": true }),
            ),
            "test adopted row metadata",
        )
        .expect("metadata should stage");
        row.metadata = Some(metadata);
        row.metadata_ref = Some(crate::json_store::JsonRef::for_content(
            br#"{"meta":"forged-ref"}"#,
        ));

        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = StorageWriteSet::new();
        let err = stage_changelog_commits(
            &mut read,
            &mut writes,
            &[],
            &BTreeMap::new(),
            &[row],
            &BTreeMap::from([("merge-commit".to_string(), vec![0])]),
            &[FinalizedCommitRow {
                commit_id: "merge-commit".to_string(),
                parent_commit_ids: vec!["source-parent".to_string()],
                created_at: "2026-01-01T00:00:01Z".to_string(),
                change_id: "merge-commit-change".to_string(),
            }],
        )
        .await
        .expect_err("metadata ref corruption should be rejected in commit path");
        assert!(
            err.message
                .contains("metadata_ref does not match materialized metadata"),
            "{err:?}"
        );
    }

    #[test]
    fn adopted_projection_rejects_tombstone_snapshot_inconsistency() {
        let mut row = adopted_global_row(
            "source-change",
            "source-commit",
            "source-parent",
            "adopt-commit",
        );
        row.snapshot_ref = None;

        let err = validate_adopted_row_payload_refs(&row)
            .expect_err("snapshot tombstone inconsistency should be rejected");
        assert!(
            err.message
                .contains("snapshot_ref does not match materialized snapshot"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn commit_rejects_adopted_tombstone_snapshot_inconsistency() {
        let mut row = adopted_global_row(
            "source-change",
            "source-commit",
            "source-parent",
            "merge-commit",
        );
        row.snapshot_ref = None;

        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = StorageWriteSet::new();
        let err = stage_changelog_commits(
            &mut read,
            &mut writes,
            &[],
            &BTreeMap::new(),
            &[row],
            &BTreeMap::from([("merge-commit".to_string(), vec![0])]),
            &[FinalizedCommitRow {
                commit_id: "merge-commit".to_string(),
                parent_commit_ids: vec!["source-parent".to_string()],
                created_at: "2026-01-01T00:00:01Z".to_string(),
                change_id: "merge-commit-change".to_string(),
            }],
        )
        .await
        .expect_err("tombstone inconsistency should be rejected in commit path");
        assert!(
            err.message
                .contains("snapshot_ref does not match materialized snapshot"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn commit_rejects_adopted_snapshot_ref_mismatch() {
        let mut row = adopted_global_row(
            "source-change",
            "source-commit",
            "source-parent",
            "merge-commit",
        );
        row.snapshot_ref = Some(crate::json_store::JsonRef::for_content(
            br#"{"value":"forged-ref"}"#,
        ));

        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = StorageWriteSet::new();
        let err = stage_changelog_commits(
            &mut read,
            &mut writes,
            &[],
            &BTreeMap::new(),
            &[row],
            &BTreeMap::from([("merge-commit".to_string(), vec![0])]),
            &[FinalizedCommitRow {
                commit_id: "merge-commit".to_string(),
                parent_commit_ids: vec!["source-parent".to_string()],
                created_at: "2026-01-01T00:00:01Z".to_string(),
                change_id: "merge-commit-change".to_string(),
            }],
        )
        .await
        .expect_err("snapshot ref corruption should be rejected in commit path");
        assert!(
            err.message
                .contains("snapshot_ref does not match materialized snapshot"),
            "{err:?}"
        );
    }

    #[test]
    fn adopted_projection_rejects_payload_ref_mismatch() {
        let mut row = adopted_global_row(
            "source-change",
            "source-commit",
            "source-parent",
            "adopt-commit",
        );
        row.snapshot_ref = Some(crate::json_store::JsonRef::for_content(
            br#"{"value":"forged-ref"}"#,
        ));

        let err = validate_adopted_row_payload_refs(&row)
            .expect_err("ref-only corruption should be rejected");
        assert!(
            err.message
                .contains("snapshot_ref does not match materialized snapshot"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn stage_changelog_commits_publishes_staged_parents_before_children() {
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
            },
            FinalizedCommitRow {
                commit_id: "parent-commit".to_string(),
                parent_commit_ids: Vec::new(),
                created_at: "2026-01-01T00:00:00Z".to_string(),
                change_id: "parent-commit-change".to_string(),
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
            &[],
            &BTreeMap::new(),
            &commits,
        )
        .await
        .expect("child-before-parent input should still publish parent first");
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
                projection: crate::changelog::CommitProjection::Header,
                visibility: crate::changelog::CommitVisibilityMode::RequireVisible,
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

        let mut changelog_reader = crate::changelog::ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &["test-uuid-1".to_string()],
                projection: crate::changelog::CommitProjection::Header,
                visibility: crate::changelog::CommitVisibilityMode::RequireVisible,
            })
            .await
            .expect("changelog commit should load");
        let Some(crate::changelog::CommitLoadEntry::Header(commit)) =
            commits.entries.into_iter().next().flatten()
        else {
            panic!("changelog commit should exist");
        };
        assert_eq!(commit.derivable_change_id, "test-uuid-2");
        let changes = changelog_reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &["change-tracked".to_string()],
                projection: crate::changelog::ChangeProjection::PhysicalLocation,
                visibility:
                    crate::changelog::ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await
            .expect("changelog change should load");
        assert!(matches!(
            changes.entries.as_slice(),
            [Some(crate::changelog::ChangeLoadEntry::PhysicalLocation(_))]
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

        let mut changelog_reader = crate::changelog::ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &["test-uuid-1".to_string()],
                projection: crate::changelog::CommitProjection::Header,
                visibility: crate::changelog::CommitVisibilityMode::RequireVisible,
            })
            .await
            .expect("changelog commit should load");
        let Some(crate::changelog::CommitLoadEntry::Header(commit)) =
            commits.entries.into_iter().next().flatten()
        else {
            panic!("changelog commit should exist");
        };
        assert_eq!(commit.derivable_change_id, "test-uuid-2");
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
                members(["change-a", "change-b"]),
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
                StagedCommitMembers::default(),
            )]),
            BTreeMap::new(),
            &version_ctx,
            &mut read,
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

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([("version-a".to_string(), members(["change-a"]))]),
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
            BTreeMap::from([("version-a".to_string(), members(["change-a"]))]),
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

    fn members<const N: usize>(change_ids: [&str; N]) -> StagedCommitMembers {
        members_with_commit("test-uuid-1", "test-uuid-2", change_ids)
    }

    fn members_with_commit<const N: usize>(
        commit_id: &str,
        commit_change_id: &str,
        change_ids: [&str; N],
    ) -> StagedCommitMembers {
        let mut members = StagedCommitMembers::new(
            commit_id.to_string(),
            commit_change_id.to_string(),
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

    fn adopted_global_row(
        change_id: &str,
        source_commit_id: &str,
        source_parent_commit_id: &str,
        commit_id: &str,
    ) -> PreparedAdoptedStateRow {
        PreparedAdoptedStateRow {
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
                    "test adopted row snapshot",
                )
                .expect("test adopted snapshot should stage"),
            ),
            metadata: None,
            snapshot_ref: Some(crate::json_store::JsonRef::for_content(
                serde_json::json!({ "value": 1 }).to_string().as_bytes(),
            )),
            metadata_ref: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: true,
            change_id: change_id.to_string(),
            source_commit_id: source_commit_id.to_string(),
            source_parent_commit_id: source_parent_commit_id.to_string(),
            commit_id: commit_id.to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
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
