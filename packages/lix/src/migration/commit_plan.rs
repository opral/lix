use std::collections::BTreeMap;

use bytes::Bytes;

use crate::changelog::{ChangeId, ChangeRecord, CommitId};
use crate::json_store::{
    JsonLoadRequestRef, JsonReadScopeRef, JsonSlot, JsonStoreContext, JsonWritePlacementRef,
    NormalizedJsonRef,
};
use crate::migration::row_rewrite::{
    HistoricalSchemaCatalog, MaterializedV68Change, RewrittenChange,
};
use crate::migration::v68::{
    CommitDeltaPayloadDescriptor, CommitDeltaSegmentBounds, decode_commit_delta_segment,
    decode_key, decode_replacement_part, load_columnar_changes, load_commit_state_manifest,
};
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StorageProjectedValue,
    StorageWriteSet,
};
use crate::tracked_state::{
    CommitStateManifest, CommitStateTouchedScopeFilter, TRACKED_STATE_CHANGE_LOCATOR_SPACE,
    TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
    TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE, TrackedStateBaseCoordinate,
    TrackedStateCommitDeltaRef, TrackedStateDeltaRef, TrackedStateIndexValue, TrackedStateKey,
    ReplacementPartRowRef, direct_change_locator, encode_key_ref,
    encode_replacement_part_with_compressor, scan_commit_state_manifest_commit_ids,
    stage_addressable_commit_deltas,
    stage_addressable_commit_deltas_with_selected_source, stage_change_locators,
    stage_commit_state_manifest, stage_preencoded_ordered_addressable_replacement_parts,
    CommitDeltaReplacementGeneration, TrackedStateKeyRef,
};
use crate::LixError;

use super::publish::PublicationPlan;

#[derive(Debug)]
struct OwnedMember {
    key: TrackedStateKey,
    value: TrackedStateIndexValue,
    snapshot: JsonSlot,
    metadata: JsonSlot,
    typed_payload: Option<Vec<u8>>,
    origin_key: Option<String>,
    base_coordinate: Option<TrackedStateBaseCoordinate>,
    authored: bool,
    authored_change: Option<RewrittenChange>,
}

pub(super) struct CommitAuthorityPlan {
    pub(super) member_count: u64,
    pub(super) recovered_changes: Vec<RewrittenChange>,
}

impl OwnedMember {
    fn as_ref(&self) -> TrackedStateCommitDeltaRef<'_> {
        TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: &self.key.schema_key,
                file_id: self.key.file_id.as_deref(),
                row_pk: &self.key.row_pk,
                change_id: self.value.change_id,
                commit_id: self.value.commit_id,
                deleted: self.value.deleted,
                created_at: self.value.created_at,
                updated_at: self.value.updated_at,
            },
            snapshot: self.snapshot.as_ref_slot(),
            metadata: self.metadata.as_ref_slot(),
            typed_snapshot: None,
            typed_payload: self.typed_payload.as_deref(),
            origin_key: self.origin_key.as_deref(),
            base_coordinate: self.base_coordinate,
            authored: self.authored,
        }
    }
}

pub(super) async fn plan_commit_authorities(
    read: &(impl StorageAdapterRead + ?Sized),
    rewritten_changes: &[RewrittenChange],
    catalog: &HistoricalSchemaCatalog,
    max_members: usize,
    publication: &mut PublicationPlan,
) -> Result<CommitAuthorityPlan, LixError> {
    let changes = rewritten_changes
        .iter()
        .map(|change| (change.record.change_id, change))
        .collect::<BTreeMap<_, _>>();
    let commit_ids = scan_commit_state_manifest_commit_ids(read).await?;
    let mut writes = StorageWriteSet::new();
    let mut member_count = 0u64;
    let mut recovered_changes = BTreeMap::new();
    for commit_id in commit_ids.iter().copied() {
        let old = load_commit_state_manifest(read, commit_id)
            .await?
            .ok_or_else(|| migration_error(format!("commit '{commit_id}' lost its v68 authority")))?;
        let declared_members = usize::try_from(old.mutations.member_count)
            .expect("u32 member count fits usize");
        if usize::try_from(member_count)
            .ok()
            .and_then(|count| count.checked_add(declared_members))
            .is_none_or(|count| count > max_members)
        {
            return Err(LixError::new(
                "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
                "commit history exceeds the configured migration row bound",
            ));
        }
        let members = load_members(read, &old, &changes, catalog).await?;
        for member in &members {
            if let Some(change) = member.authored_change.as_ref() {
                recovered_changes
                    .entry(change.record.change_id)
                    .or_insert_with(|| change.clone());
            }
        }
        member_count = member_count.saturating_add(members.len() as u64);
        let refs = members.iter().map(OwnedMember::as_ref).collect::<Vec<_>>();
        let addressable = refs
            .iter()
            .enumerate()
            .map(|(index, member)| {
                direct_change_locator(member.delta.change_id)
                    .is_some_and(|locator| {
                        locator.commit_id == commit_id
                            && locator.segment_index as usize == index / 512
                            && usize::from(locator.ordinal) == index % 512
                    })
            })
            .collect::<Vec<_>>();
        let mutations = if old.mutations.columnar_parts.is_some()
            || !old.mutations.replacement_part_digests.is_empty()
        {
            let generation = replacement_generation(&old)?;
            let parts = encode_replacement_parts_preserving_addresses(
                &members,
                &old.mutations.direct_part_row_counts,
            )?;
            let updated_at = members
                .first()
                .map(|member| member.value.updated_at)
                .ok_or_else(|| migration_error("replacement commit has no members"))?;
            let staged = stage_preencoded_ordered_addressable_replacement_parts(
                &mut writes,
                commit_id,
                updated_at,
                refs.len(),
                parts,
                &generation,
            )?;
            if staged.assigned_change_ids().ne(refs.iter().map(|member| member.delta.change_id)) {
                return Err(migration_error(format!(
                    "replacement commit '{commit_id}' changed its durable member identities"
                )));
            }
            staged.mutation_inventory().clone()
        } else {
            let staged = if let Some(selected_source) = old.mutations.selected_source_commit_id() {
                stage_addressable_commit_deltas_with_selected_source(
                    &mut writes,
                    &refs,
                    &addressable,
                    selected_source,
                )?
            } else {
                stage_addressable_commit_deltas(&mut writes, &refs, &addressable)?
            };
            stage_change_locators(&mut writes, &staged.locators);
            staged.mutation_inventory().clone()
        };
        let manifest = CommitStateManifest {
            commit_id,
            change_account_id: old.change_account_id,
            replay_debt: old.replay_debt,
            mutations,
            touched_scope_filter: CommitStateTouchedScopeFilter::default(),
            current_state_scoped_ranges: None,
            snapshot_root: old.snapshot_root,
        };
        stage_commit_state_manifest(&mut writes, &manifest)?;
    }
    publication.clear_space(TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE);
    publication.clear_space(TRACKED_STATE_CHANGE_LOCATOR_SPACE);
    publication.clear_space(TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE);
    publication.clear_space(TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE);
    publication.clear_space(crate::tracked_state::MUTATION_DIRECTORY_NODE_SPACE);
    publication.clear_space(crate::tracked_state::SCOPED_RANGE_NODE_SPACE);
    publication.clear_space(crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE);
    publication.clear_space(crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE);
    let recovered_json = recovered_changes
        .values()
        .filter_map(|change| change.staged_json.as_deref())
        .collect::<Vec<_>>();
    if !recovered_json.is_empty() {
        JsonStoreContext::new().writer().stage_batch(
            &mut writes,
            JsonWritePlacementRef::OutOfBand,
            recovered_json.into_iter().map(NormalizedJsonRef::new),
        )?;
    }
    for (space, batch) in writes
        .into_migration_put_batches()
        .map_err(|error| migration_error(error.to_string()))?
    {
        publication.add_builder_batch(space, batch)?;
    }
    Ok(CommitAuthorityPlan {
        member_count,
        recovered_changes: recovered_changes.into_values().collect(),
    })
}

fn encode_replacement_parts_preserving_addresses(
    members: &[OwnedMember],
    part_rows: &[u16],
) -> Result<Vec<crate::tracked_state::EncodedReplacementPart>, LixError> {
    if part_rows
        .iter()
        .map(|&rows| usize::from(rows))
        .sum::<usize>()
        != members.len()
    {
        return Err(migration_error(
            "replacement direct-part rows disagree with decoded members",
        ));
    }
    let mut parts = Vec::with_capacity(part_rows.len());
    let mut compressor = None;
    let mut offset = 0usize;
    for &row_count in part_rows {
        let end = offset + usize::from(row_count);
        let group = &members[offset..end];
        let encoded_keys = group
            .iter()
            .map(|member| {
                encode_key_ref(TrackedStateKeyRef {
                    schema_key: &member.key.schema_key,
                    file_id: member.key.file_id.as_deref(),
                    row_pk: &member.key.row_pk,
                })
            })
            .collect::<Vec<_>>();
        let rows = group
            .iter()
            .zip(&encoded_keys)
            .map(|(member, encoded_key)| ReplacementPartRowRef {
                encoded_key,
                snapshot: member.snapshot.as_ref_slot(),
                metadata: member.metadata.as_ref_slot(),
                typed_snapshot: None,
                typed_payload: member.typed_payload.as_deref(),
            })
            .collect::<Vec<_>>();
        parts.push(encode_replacement_part_with_compressor(
            &rows,
            &mut compressor,
        )?);
        offset = end;
    }
    Ok(parts)
}

fn replacement_generation(
    manifest: &CommitStateManifest,
) -> Result<CommitDeltaReplacementGeneration, LixError> {
    let lifecycle_summary = manifest
        .mutations
        .lifecycle_summary
        .clone()
        .ok_or_else(|| migration_error("replacement inventory is missing lifecycle metadata"))?;
    let scope = manifest
        .mutations
        .single_partition
        .clone()
        .ok_or_else(|| migration_error("replacement inventory is missing its scope"))?;
    if lifecycle_summary.scope != scope {
        return Err(migration_error(
            "replacement lifecycle scope disagrees with its partition",
        ));
    }
    let fallback_commit_id = manifest
        .mutations
        .replacement_generation
        .as_ref()
        .and_then(|generation| generation.fallback_commit_id)
        .map(|bytes| CommitId::new(uuid::Uuid::from_bytes(bytes)));
    Ok(CommitDeltaReplacementGeneration {
        scope,
        fallback_commit_id,
        lifecycle_summary,
    })
}

async fn load_members(
    read: &(impl StorageAdapterRead + ?Sized),
    manifest: &CommitStateManifest,
    changes: &BTreeMap<ChangeId, &RewrittenChange>,
    catalog: &HistoricalSchemaCatalog,
) -> Result<Vec<OwnedMember>, LixError> {
    if let Some(columnar) = manifest.mutations.columnar_parts.as_ref() {
        return columnar_members(read, manifest, columnar, changes, catalog).await;
    }
    if !manifest.mutations.replacement_part_digests.is_empty() {
        let lifecycle = manifest.mutations.lifecycle_summary.as_ref().ok_or_else(|| {
            migration_error("replacement inventory is missing lifecycle metadata")
        })?;
        let authority = manifest.mutations.replacement_parts.as_ref().ok_or_else(|| {
            migration_error("replacement inventory is missing update metadata")
        })?;
        return replacement_members(
            read,
            manifest,
            lifecycle.uniform_created_at,
            authority.uniform_updated_at,
            changes,
            catalog,
        )
        .await;
    }

    let mut decoded = Vec::new();
    if !manifest.mutations.inline_part.is_empty() {
        decoded.extend(decode_commit_delta_segment(
            &manifest.mutations.inline_part,
            None,
            manifest.commit_id,
        )?);
    } else {
        for (index, part) in manifest.mutations.parts.iter().enumerate() {
            let key = segment_key(manifest.commit_id, index, part.replacement_part.as_ref().map(|part| part.content_digest));
            let bytes = get_one(read, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, key).await?
                .ok_or_else(|| migration_error(format!("commit '{}' is missing segment {index}", manifest.commit_id)))?;
            decoded.extend(decode_commit_delta_segment(
                &bytes,
                Some(&CommitDeltaSegmentBounds {
                    first_key: part.first_key.clone(),
                    last_key: part.last_key.clone(),
                }),
                manifest.commit_id,
            )?);
        }
    }
    let mut members = Vec::with_capacity(decoded.len());
    for member in decoded {
            let (
                snapshot,
                metadata,
                typed_payload,
                origin_key,
                base_coordinate,
                authored,
                authored_change,
            ) =
                match member.payload {
                    CommitDeltaPayloadDescriptor::Authored {
                        snapshot: old_snapshot,
                        metadata: old_metadata,
                        origin_key,
                        base_coordinate,
                        ..
                    } => {
                        let rewritten = if let Some(change) = changes.get(&member.value.change_id) {
                            (*change).clone()
                        } else {
                            let snapshot_json = materialize_json_slot(read, &old_snapshot).await?;
                            catalog
                                .rewrite(&MaterializedV68Change {
                                    snapshot_json,
                                    record: ChangeRecord {
                                        format_version: 1,
                                        change_id: member.value.change_id,
                                        account_id: manifest.change_account_id.clone(),
                                        schema_key: member.key.schema_key.clone(),
                                        row_pk: member.key.row_pk.clone(),
                                        file_id: member.key.file_id.clone(),
                                        snapshot: old_snapshot,
                                        metadata: old_metadata,
                                        typed_payload: None,
                                        created_at: member.value.created_at,
                                        origin_key: origin_key.clone(),
                                    },
                                })?
                        };
                        let change = &rewritten.record;
                        validate_envelope(change, &member.key)?;
                        (
                            change.snapshot.clone(),
                            change.metadata.clone(),
                            change.typed_payload.clone(),
                            origin_key,
                            base_coordinate,
                            true,
                            Some(rewritten),
                        )
                    }
                    CommitDeltaPayloadDescriptor::SelectedRef { base_coordinate } => (
                        JsonSlot::None,
                        JsonSlot::None,
                        None,
                        None,
                        base_coordinate,
                        false,
                        None,
                    ),
                    CommitDeltaPayloadDescriptor::SelectedTombstone { base_coordinate } => (
                        JsonSlot::None,
                        JsonSlot::None,
                        None,
                        None,
                        base_coordinate,
                        false,
                        None,
                    ),
                };
            members.push(OwnedMember {
                key: member.key,
                value: member.value,
                snapshot,
                metadata,
                typed_payload,
                origin_key,
                base_coordinate,
                authored,
                authored_change,
            });
    }
    Ok(members)
}

async fn columnar_members(
    read: &(impl StorageAdapterRead + ?Sized),
    manifest: &CommitStateManifest,
    columnar: &crate::tracked_state::ColumnarMutationPartSet,
    changes: &BTreeMap<ChangeId, &RewrittenChange>,
    catalog: &HistoricalSchemaCatalog,
) -> Result<Vec<OwnedMember>, LixError> {
    let decoded = load_columnar_changes(
        read,
        manifest.commit_id,
        columnar,
        &manifest.change_account_id,
    )
    .await?;
    let mut members = Vec::with_capacity(decoded.len());
    for decoded in decoded {
        let change_id = decoded.record.change_id;
        let rewritten = if let Some(change) = changes.get(&change_id) {
            (*change).clone()
        } else {
            let snapshot_json = materialize_json_slot(read, &decoded.record.snapshot).await?;
            catalog.rewrite(&MaterializedV68Change {
                snapshot_json,
                record: decoded.record,
            })?
        };
        let change = &rewritten.record;
        members.push(OwnedMember {
            key: TrackedStateKey {
                schema_key: change.schema_key.clone(),
                file_id: change.file_id.clone(),
                row_pk: change.row_pk.clone(),
            },
            value: TrackedStateIndexValue {
                change_id,
                commit_id: manifest.commit_id,
                deleted: false,
                created_at: columnar.uniform_created_at,
                updated_at: columnar.uniform_updated_at,
            },
            snapshot: change.snapshot.clone(),
            metadata: change.metadata.clone(),
            typed_payload: change.typed_payload.clone(),
            origin_key: columnar.origin_key.clone(),
            base_coordinate: Some(decoded.base_coordinate),
            authored: true,
            authored_change: Some(rewritten),
        });
    }
    members.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    Ok(members)
}

async fn replacement_members(
    read: &(impl StorageAdapterRead + ?Sized),
    manifest: &CommitStateManifest,
    created_at: crate::common::LixTimestamp,
    updated_at: crate::common::LixTimestamp,
    changes: &BTreeMap<ChangeId, &RewrittenChange>,
    catalog: &HistoricalSchemaCatalog,
) -> Result<Vec<OwnedMember>, LixError> {
    let mut members = Vec::new();
    for (segment_index, (&digest, &row_count)) in manifest
        .mutations
        .replacement_part_digests
        .iter()
        .zip(&manifest.mutations.direct_part_row_counts)
        .enumerate()
    {
        let key = segment_key(manifest.commit_id, segment_index, Some(digest));
        let bytes = get_one(read, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, key)
            .await?
            .ok_or_else(|| {
                migration_error(format!(
                    "commit '{}' is missing replacement part {segment_index}",
                    manifest.commit_id
                ))
            })?;
        let rows = decode_replacement_part(&digest, &bytes)?;
        if rows.len() != usize::from(row_count) {
            return Err(migration_error(format!(
                "commit '{}' replacement part {segment_index} row count disagrees with its authority",
                manifest.commit_id
            )));
        }
        for (ordinal, row) in rows.into_iter().enumerate() {
            let change_id = addressable_change_id(manifest.commit_id, segment_index, ordinal)?;
            let key = decode_key(&row.encoded_key)?;
            let rewritten = if let Some(change) = changes.get(&change_id) {
                (*change).clone()
            } else {
                let snapshot_json = materialize_json_slot(read, &row.snapshot).await?;
                catalog.rewrite(&MaterializedV68Change {
                    snapshot_json,
                    record: ChangeRecord {
                        format_version: 1,
                        change_id,
                        account_id: manifest.change_account_id.clone(),
                        schema_key: key.schema_key.clone(),
                        row_pk: key.row_pk.clone(),
                        file_id: key.file_id.clone(),
                        snapshot: row.snapshot,
                        metadata: row.metadata,
                        typed_payload: None,
                        created_at,
                        origin_key: None,
                    },
                })?
            };
            validate_envelope(&rewritten.record, &key)?;
            members.push(OwnedMember {
                key,
                value: TrackedStateIndexValue {
                    change_id,
                    commit_id: manifest.commit_id,
                    deleted: false,
                    created_at,
                    updated_at,
                },
                snapshot: rewritten.record.snapshot.clone(),
                metadata: rewritten.record.metadata.clone(),
                typed_payload: rewritten.record.typed_payload.clone(),
                origin_key: None,
                base_coordinate: None,
                authored: true,
                authored_change: Some(rewritten),
            });
        }
    }
    members.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    Ok(members)
}

fn validate_envelope(change: &ChangeRecord, key: &TrackedStateKey) -> Result<(), LixError> {
    if change.schema_key != key.schema_key
        || change.file_id != key.file_id
        || change.row_pk != key.row_pk
    {
        return Err(migration_error(format!(
            "change '{}' disagrees with its commit-delta identity",
            change.change_id
        )));
    }
    Ok(())
}

fn addressable_change_id(
    commit_id: CommitId,
    segment_index: usize,
    ordinal: usize,
) -> Result<ChangeId, LixError> {
    let packed = u32::try_from(segment_index)
        .ok()
        .and_then(|segment| segment.checked_mul(512))
        .and_then(|base| base.checked_add(u32::try_from(ordinal).ok()?))
        .and_then(|address| address.checked_add(1))
        .ok_or_else(|| migration_error("direct change address overflows"))?;
    let mut bytes = *commit_id.as_uuid().as_bytes();
    if bytes[12..] != [0; 4] {
        return Err(migration_error("commit id has no direct-address space"));
    }
    bytes[12..].copy_from_slice(&packed.to_be_bytes());
    Ok(ChangeId::new(uuid::Uuid::from_bytes(bytes)))
}

fn segment_key(commit_id: CommitId, index: usize, digest: Option<[u8; 32]>) -> Vec<u8> {
    let mut key = commit_id.as_uuid().as_bytes().to_vec();
    key.extend_from_slice(&(index as u32).to_be_bytes());
    if let Some(digest) = digest {
        key.extend_from_slice(&digest);
    }
    key
}

async fn get_one(
    read: &(impl StorageAdapterRead + ?Sized),
    space: crate::storage_adapter::StorageSpace,
    key: Vec<u8>,
) -> Result<Option<Bytes>, LixError> {
    let values = PointReadPlan::new(space, &[StorageKey(Bytes::from(key))])
        .materialize(read, StorageGetOptions::default())
        .await?;
    Ok(values.value.into_iter().next().flatten().and_then(|value| match value {
        StorageProjectedValue::FullValue(bytes) => Some(bytes),
        StorageProjectedValue::KeyOnly => None,
    }))
}

async fn materialize_json_slot(
    read: &(impl StorageAdapterRead + ?Sized),
    slot: &JsonSlot,
) -> Result<Option<String>, LixError> {
    match slot {
        JsonSlot::None => Ok(None),
        JsonSlot::Inline(json) => Ok(Some(json.to_string())),
        JsonSlot::Ref(reference) => {
            let value = JsonStoreContext::new()
                .load_bytes_many(
                    read,
                    JsonLoadRequestRef {
                        refs: std::slice::from_ref(reference),
                        scope: JsonReadScopeRef::OutOfBand,
                    },
                )
                .await?
                .into_values()
                .into_iter()
                .next()
                .flatten()
                .ok_or_else(|| migration_error("commit payload references missing JSON"))?;
            String::from_utf8(value.to_vec())
                .map(Some)
                .map_err(|error| migration_error(format!("commit payload JSON is not UTF-8: {error}")))
        }
    }
}

fn migration_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_ERROR_MIGRATION_FAILED", message.into())
}
