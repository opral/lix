//! Detached, bounded source qualification for the v74/v77/v78 canonical chain.
//! The planning copy predicts exact output; independent source invariants below
//! reject destructive transformations even if both executions share a defect.
use super::MigrationOptions;
use crate::{LixError, storage_adapter::*};
use bytes::Bytes;
use std::collections::BTreeMap;

type Records = BTreeMap<(u32, Bytes), Bytes>;

fn failure(message: impl Into<String>) -> LixError {
    LixError::new("LIX_MIGRATION_PRESERVATION_FAILED", message)
}

pub(super) struct Witness {
    source: Records,
    expected: Records,
    pub(super) expected_digest: String,
}

async fn capture<S: Storage + Clone + Send + Sync + 'static>(
    storage: &S,
    options: MigrationOptions,
) -> Result<Records, LixError> {
    let adapter = super::epoch::inspect_existing_epoch_adapter(storage).await?;
    capture_adapter(&adapter, options).await
}

async fn capture_adapter<S: Storage + Clone + Send + Sync + 'static>(
    adapter: &StorageAdapter<S>,
    options: MigrationOptions,
) -> Result<Records, LixError> {
    let read = super::MigrationPlanningRead::new(adapter).await?;
    let mut records = Records::new();
    let mut bytes = 0usize;
    for space in crate::storage_spaces::SNAPSHOT_STORAGE_SPACES {
        let mut cursor = read
            .begin_scan(
                *space,
                StoragePrefix {
                    bytes: Bytes::new(),
                }
                .to_range()?,
                Default::default(),
            )
            .await?;
        while let Some(page) = cursor.next_chunk().await? {
            for entry in page {
                let StorageProjectedValue::FullValue(value) = entry.value else {
                    return Err(failure("source witness omitted value"));
                };
                bytes = bytes
                    .saturating_add(entry.key.0.len())
                    .saturating_add(value.len());
                if records.len() >= options.max_changes || bytes > options.max_preflight_bytes {
                    return Err(LixError::new(
                        "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
                        "complete source witness exceeds configured record/byte bounds",
                    ));
                }
                records.insert((space.id.0, entry.key.0), value);
            }
        }
    }
    read.finish()?;
    Ok(records)
}

async fn copy(records: &Records) -> Result<Memory, LixError> {
    let memory = Memory::default();
    let adapter = StorageAdapter::new(memory.clone());
    let mut write = adapter.begin_migration_write(Default::default()).await?;
    for space in crate::storage_spaces::SNAPSHOT_STORAGE_SPACES {
        let entries = records
            .iter()
            .filter(|((id, _), _)| *id == space.id.0)
            .map(|((_, key), value)| PutEntry {
                key: StorageKey(key.clone()),
                value: StorageValue {
                    bytes: value.clone(),
                },
            })
            .collect();
        write.put_many(*space, PutBatch { entries }).await?;
    }
    write.commit().await?;
    Ok(memory)
}

pub(super) async fn plan<S: Storage + Clone + Send + Sync + 'static>(
    storage: &S,
    options: MigrationOptions,
    authority: bool,
) -> Result<Witness, LixError> {
    let source = capture(storage, options).await?;
    let memory = StorageSession::acquire(copy(&source).await?).await?;
    let adapter = StorageAdapter::new(memory.clone());
    super::api::migrate_lix_with_adapter(memory.clone(), adapter, options).await?;
    if authority {
        super::authority_baseline_fence::upgrade_authority_native_baseline_fence(&memory).await?;
    }
    let expected = capture(&memory, options).await?;
    independent_invariants(&source, &expected)?;
    descriptors(&source, &expected, options).await?;
    let expected_digest = super::public_api::content_digest(&memory).await?;
    Ok(Witness {
        source,
        expected,
        expected_digest,
    })
}

/// Validate the unpublished epoch against an independently qualified source.
/// Legacy source markers are fenced during migration, so restore the recognized
/// original marker only in the detached planning copy.
pub(super) async fn verify_candidate<S: Storage + Clone + Send + Sync + 'static>(
    source: &StorageAdapter<S>,
    target: &StorageAdapter<S>,
    from_format: u32,
    options: MigrationOptions,
) -> Result<(), LixError> {
    let mut source_records = capture_adapter(source, options).await?;
    source_records.insert(
        (
            crate::init::REPOSITORY_PROTOCOL_SPACE.id.0,
            Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_KEY),
        ),
        Bytes::from(format!("tracked-default-branch.v{from_format}")),
    );
    let authority = source_records.contains_key(&(
        crate::sync::SYNC_AUTHORITY_STATE_SPACE.id.0,
        crate::sync::authority_state_key().0,
    ));
    let detached = copy(&source_records).await?;
    let witness = plan(&detached, options, authority).await?;
    let actual = capture_adapter(target, options).await?;
    witness.verify_records(actual, options).await
}

pub(super) async fn plan_adapter<S: Storage + Clone + Send + Sync + 'static>(
    adapter: &StorageAdapter<S>,
    options: MigrationOptions,
) -> Result<Witness, LixError> {
    let detached = copy(&capture_adapter(adapter, options).await?).await?;
    Box::pin(plan(&detached, options, false)).await
}

pub(super) async fn verify_v72_source_history<S: Storage + Clone + Send + Sync + 'static>(
    source: &StorageAdapter<S>,
    target: &StorageAdapter<S>,
    options: MigrationOptions,
) -> Result<(), LixError> {
    let mut source = capture_adapter(source, options).await?;
    source.insert(
        (
            crate::init::REPOSITORY_PROTOCOL_SPACE.id.0,
            Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_KEY),
        ),
        Bytes::from_static(b"tracked-default-branch.v72"),
    );
    let target = capture_adapter(target, options).await?;
    // The amendment may append commits, but never discard the original payload
    // chunks, file bytes, or source history. Descriptor comparison below also
    // verifies every original commit and manifest through codec conversion.
    for ((space, key), value) in &source {
        if [
            crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE.id.0,
            crate::binary_cas::BINARY_CAS_MANIFEST_SPACE.id.0,
            crate::binary_cas::BINARY_CAS_MANIFEST_CHUNK_SPACE.id.0,
            crate::binary_cas::BINARY_CAS_CHUNK_SPACE.id.0,
        ]
        .contains(space)
            && target.get(&(*space, key.clone())) != Some(value)
        {
            return Err(failure("v72 amendment dropped original historical payload"));
        }
    }
    descriptors(&source, &target, options).await
}

impl Witness {
    pub(super) async fn verify_adapter<S: Storage + Clone + Send + Sync + 'static>(
        &self,
        adapter: &StorageAdapter<S>,
        options: MigrationOptions,
    ) -> Result<(), LixError> {
        self.verify_records(capture_adapter(adapter, options).await?, options)
            .await
    }

    pub(super) async fn verify<S: Storage + Clone + Send + Sync + 'static>(
        &self,
        storage: &S,
        options: MigrationOptions,
    ) -> Result<(), LixError> {
        let actual = capture(storage, options).await?;
        self.verify_records(actual, options).await
    }

    async fn verify_records(
        &self,
        actual: Records,
        options: MigrationOptions,
    ) -> Result<(), LixError> {
        independent_invariants(&self.source, &actual)?;
        descriptors(&self.source, &actual, options).await?;
        // Only the exact protocol marker and physical mutation counter are
        // intentionally absent from the portable content witness.
        let portable = |records: &Records| {
            records
                .iter()
                .filter(|((space, key), _)| !ignored(*space, key))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Records>()
        };
        if portable(&actual) != portable(&self.expected) {
            return Err(failure(
                "candidate differs from source-derived canonical output",
            ));
        }
        Ok(())
    }
}

fn ignored(space: u32, key: &[u8]) -> bool {
    (space == crate::init::REPOSITORY_PROTOCOL_SPACE.id.0
        && key == crate::init::REPOSITORY_PROTOCOL_KEY)
        || (space == REVISION_SPACE.id.0 && key == b"m")
}

fn independent_invariants(source: &Records, target: &Records) -> Result<(), LixError> {
    // Derived metadata is checked below or by the canonical bounded plan. All
    // remaining spaces, including every receipt and pending state, are exact.
    let derived = [
        crate::changelog::COMMIT_SPACE.id.0,
        crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE
            .id
            .0,
        crate::tracked_state::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE
            .id
            .0,
        crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE.id.0,
        crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE
            .id
            .0,
        crate::tracked_state::TRACKED_STATE_COMMIT_HISTORY_DEFERRED_SPACE
            .id
            .0,
        crate::hot_state::DETERMINISTIC_IDENTITY_WITNESS_SPACE.id.0,
        crate::checkpoint::CHECKPOINT_INVENTORY_SPACE.id.0,
    ];
    for ((space, key), value) in source.iter().chain(target.iter()) {
        if *space == crate::hot_state::DETERMINISTIC_IDENTITY_WITNESS_SPACE.id.0
            && let Some(original) = source.get(&(*space, key.clone()))
            && target.get(&(*space, key.clone())) != Some(original)
        {
            return Err(failure(
                "existing deterministic witness changed or disappeared",
            ));
        }
        if ignored(*space, key) || derived.contains(space) {
            continue;
        }
        if *space == crate::init::REPOSITORY_PROTOCOL_SPACE.id.0
            && key.as_ref() == b"checkpoint-migration.v78"
        {
            continue;
        }
        if *space == crate::sync::SYNC_AUTHORITY_STATE_SPACE.id.0
            && *key == crate::sync::authority_state_key().0
        {
            let old = source.get(&(*space, key.clone()));
            let new = target.get(&(*space, key.clone()));
            if matches!((old,new), (Some(a),Some(b)) if (a.as_ref() == b"certified-authority-v4" || a.as_ref() == crate::sync::AUTHORITY_STATE_VALUE) && b.as_ref() == crate::sync::AUTHORITY_STATE_VALUE)
            {
                continue;
            }
        }
        if source.get(&(*space, key.clone())) != Some(value)
            || target.get(&(*space, key.clone())) != Some(value)
        {
            return Err(failure(format!(
                "protected record changed in space {space:#x}"
            )));
        }
    }
    // Existing immutable payload chunks are never discarded or rewritten.
    for ((space, key), value) in source {
        if *space == crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE.id.0
            && target.get(&(*space, key.clone())) != Some(value)
        {
            return Err(failure("historical payload chunk changed"));
        }
    }
    Ok(())
}

#[derive(musli::Decode)]
#[musli(packed)]
struct V5 {
    format_version: u32,
    commit_id: crate::changelog::CommitId,
    generation: u64,
    parent_commit_ids: Vec<crate::changelog::CommitId>,
    first_parent_jump_commit_id: crate::changelog::CommitId,
    first_parent_jump_span: u64,
    account_id: String,
    created_at: crate::common::LixTimestamp,
    touched_scope_digest: crate::changelog::CommitTouchedScopeDigest,
}

fn commit(raw: &[u8]) -> Result<(crate::changelog::CommitRecord, bool), LixError> {
    use crate::changelog::CommitRecord;
    if let Ok(record) = crate::storage_codec::decode::<CommitRecord>("witness commit", raw)
        && record.format_version == 7
    {
        return Ok((record, false));
    }
    if let Some(record) = super::checkpoint_metadata::decode_v6(raw) {
        return Ok((record, false));
    }
    let old: V5 = crate::storage_codec::decode("witness v5 commit", raw)?;
    if old.format_version != 5 {
        return Err(failure("unrecognized source commit codec"));
    }
    Ok((
        CommitRecord {
            format_version: 7,
            commit_id: old.commit_id,
            generation: old.generation,
            parent_commit_ids: old.parent_commit_ids,
            base_commit_id: None,
            first_parent_jump_commit_id: old.first_parent_jump_commit_id,
            first_parent_jump_span: old.first_parent_jump_span,
            account_id: old.account_id,
            created_at: old.created_at,
            touched_scope_digest: old.touched_scope_digest,
            is_checkpoint: false,
        },
        true,
    ))
}

async fn descriptors(
    source: &Records,
    target: &Records,
    options: MigrationOptions,
) -> Result<(), LixError> {
    use crate::tracked_state::{
        TrackedStateContext, TrackedStateFilter, TrackedStateReadColumns, TrackedStateScanRequest,
    };
    let protocol = source
        .get(&(
            crate::init::REPOSITORY_PROTOCOL_SPACE.id.0,
            Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_KEY),
        ))
        .ok_or_else(|| failure("source repository protocol absent"))?;
    let logical_amendment = matches!(
        crate::init::parse_repository_protocol(protocol),
        crate::init::RepositoryProtocolStatus::MigrationRequired { found_version: 72 }
    );
    let native_checkpoints = match crate::init::parse_repository_protocol(protocol) {
        crate::init::RepositoryProtocolStatus::MigrationRequired { found_version: 78 } => true,
        crate::init::RepositoryProtocolStatus::MigrationRequired {
            found_version: 72 | 73 | 74 | 75 | 76 | 77,
        } => false,
        _ => {
            return Err(failure(
                "source witness requires repository format 73 through 78",
            ));
        }
    };
    let mut normalized = source.clone();
    let source_memory = copy(source).await?;
    let source_adapter = StorageAdapter::new(source_memory);
    let read = super::MigrationPlanningRead::new(&source_adapter).await?;
    let control = crate::branch::BranchHeadControlContext::new()
        .reader(read.clone())
        .load(crate::GLOBAL_BRANCH_ID)
        .await?
        .ok_or_else(|| failure("source global branch absent"))?;
    let mut commits = BTreeMap::new();
    for ((space, key), raw) in source {
        if *space == crate::changelog::COMMIT_SPACE.id.0 {
            let (record, legacy) = commit(raw)?;
            if key.as_ref() != record.commit_id.as_uuid().as_bytes() {
                return Err(failure("source commit key differs from identity"));
            }
            commits.insert(record.commit_id, (record, legacy));
        }
    }
    let mut global = std::collections::BTreeSet::new();
    // Only v5 lacks native base authority. v6/v7 must retain their explicit
    // bases, and a previously collected older ancestor need not be rehydrated.
    let mut next = commits
        .values()
        .any(|(_, legacy)| *legacy)
        .then_some(control.head_commit_id);
    while let Some(id) = next {
        if !global.insert(id) {
            return Err(failure("source first-parent cycle"));
        }
        next = commits
            .get(&id)
            .ok_or_else(|| failure("source global ancestry missing"))?
            .0
            .parent_commit_ids
            .first()
            .copied();
    }
    let mut chronology = global
        .iter()
        .map(|id| {
            let r = &commits[id].0;
            (r.created_at, r.generation, *id)
        })
        .collect::<Vec<_>>();
    chronology.sort();
    for (record, legacy) in commits.values_mut() {
        if *legacy && !global.contains(&record.commit_id) {
            record.base_commit_id = Some(
                chronology
                    .iter()
                    .rev()
                    .find(|(time, _, _)| *time <= record.created_at)
                    .ok_or_else(|| failure("source global base cannot be proven"))?
                    .2,
            );
        }
        normalized.insert(
            (
                crate::changelog::COMMIT_SPACE.id.0,
                Bytes::copy_from_slice(record.commit_id.as_uuid().as_bytes()),
            ),
            Bytes::from(crate::storage_codec::encode(
                "witness canonical commit",
                record,
            )?),
        );
    }
    read.finish()?;
    // Decode-only projection: source records/payloads are retained unchanged;
    // only commit arity/base semantics are normalized to permit typed reads.
    let normalized_memory = copy(&normalized).await?;
    let normalized_adapter = StorageAdapter::new(normalized_memory);
    let source_read = super::MigrationPlanningRead::new(&normalized_adapter).await?;
    let mut checkpoint_ids = std::collections::BTreeSet::new();
    if native_checkpoints {
        // v78 retired lix_checkpoint markers. Its canonical commit flags are
        // the source authority, independently corroborated by its inventory.
        checkpoint_ids.extend(
            commits
                .values()
                .filter_map(|(record, _)| record.is_checkpoint.then_some(record.commit_id)),
        );
    } else {
        let mut reader = TrackedStateContext::new().reader(source_read.clone());
        let checkpoints = reader
            .scan_batch_at_commit(
                &control.head_commit_id.to_string(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["lix_checkpoint".to_owned()],
                        ..Default::default()
                    },
                    read_columns: TrackedStateReadColumns {
                        columns: vec!["row_pk".to_owned()],
                    },
                    limit: Some(options.max_changes.saturating_add(1)),
                },
            )
            .await?
            .into_rows();
        if checkpoints.len() > options.max_changes {
            return Err(failure("checkpoint witness exceeded bounds"));
        }
        for marker in checkpoints {
            if marker.deleted {
                continue;
            }
            let parts = marker.row_pk.into_parts();
            let [id] = parts.as_slice() else {
                return Err(failure("source checkpoint identity invalid"));
            };
            checkpoint_ids.insert(
                id.parse::<crate::changelog::CommitId>()
                    .map_err(|_| failure("source checkpoint UUID invalid"))?,
            );
        }
        drop(reader);
    }
    let expected_inventory = checkpoint_ids
        .iter()
        .map(|id| {
            (
                Bytes::copy_from_slice(id.as_uuid().as_bytes()),
                Bytes::new(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if native_checkpoints {
        let source_inventory = source
            .iter()
            .filter(|((space, _), _)| *space == crate::checkpoint::CHECKPOINT_INVENTORY_SPACE.id.0)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        if source_inventory != expected_inventory {
            return Err(failure(
                "source checkpoint inventory differs from native commit flags",
            ));
        }
    }
    let actual_inventory = target
        .iter()
        .filter(|((space, _), _)| *space == crate::checkpoint::CHECKPOINT_INVENTORY_SPACE.id.0)
        .map(|((_, key), value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    if actual_inventory != expected_inventory {
        return Err(failure(
            "candidate checkpoint inventory differs from source checkpoint identities",
        ));
    }
    for (id, (mut expected, _)) in commits.clone() {
        expected.is_checkpoint = checkpoint_ids.contains(&id);
        let key = (
            crate::changelog::COMMIT_SPACE.id.0,
            Bytes::copy_from_slice(id.as_uuid().as_bytes()),
        );
        let raw = target
            .get(&key)
            .ok_or_else(|| failure("candidate dropped source commit"))?;
        let (actual, legacy) = commit(raw)?;
        if legacy || actual != expected {
            return Err(failure(format!(
                "candidate changed source commit descriptor {id}"
            )));
        }
    }
    if !logical_amendment && target
        .keys()
        .filter(|(s, _)| *s == crate::changelog::COMMIT_SPACE.id.0)
        .count()
        != commits.len()
    {
        return Err(failure("candidate invented a commit"));
    }
    let target_memory = copy(target).await?;
    let target_adapter = StorageAdapter::new(target_memory);
    let target_read = super::MigrationPlanningRead::new(&target_adapter).await?;
    crate::hot_state::verify_migrated_deterministic_witness(
        &source_read,
        &target_read,
        control.tracked_generation,
        options.max_changes,
        options.max_preflight_bytes,
    )
    .await?;
    let source_ids =
        crate::tracked_state::scan_commit_state_manifest_commit_ids(&source_read).await?;
    let target_ids =
        crate::tracked_state::scan_commit_state_manifest_commit_ids(&target_read).await?;
    if (!logical_amendment && source_ids != target_ids)
        || source_ids.iter().any(|id| !target_ids.contains(id))
    {
        return Err(failure("candidate changed manifest identities"));
    }
    let mut indexed_rows = 0usize;
    let mut indexed_bytes = 0u64;
    for id in source_ids {
        let mut old = crate::tracked_state::load_commit_state_manifest(&source_read, id)
            .await?
            .ok_or_else(|| failure("source manifest absent"))?;
        let new = crate::tracked_state::load_commit_state_manifest(&target_read, id)
            .await?
            .ok_or_else(|| failure("target manifest absent"))?;
        // Only the authenticated native incorporation fact and rebuilt lookup
        // catalog may change. In particular mutation membership, snapshot roots,
        // scope, account and payload references remain source-authoritative.
        let topology = crate::tracked_state::load_published_commit_state_topology(&source_read, id)
            .await?
            .ok_or_else(|| failure("source topology absent"))?;
        old.incorporation = topology.incorporation();
        let mut writes = normalized_adapter.new_write_set();
        let (root, rows) = crate::tracked_state::backfill_row_pk_index_for_commit(
            &source_read,
            &mut writes,
            &old,
            options.max_changes.saturating_sub(indexed_rows),
        )
        .await?;
        indexed_rows = indexed_rows.saturating_add(rows);
        indexed_bytes = indexed_bytes.saturating_add(writes.stats().written_bytes);
        if indexed_rows > options.max_changes || indexed_bytes > options.max_preflight_bytes as u64
        {
            return Err(failure(
                "source index qualification exceeds aggregate bounds",
            ));
        }
        // Verify every byte emitted for the independently rebuilt index,
        // not just its root pointer. This also detects corrupt or missing
        // content-addressed child chunks in the candidate.
        let chunks = Memory::default();
        let chunk_adapter = StorageAdapter::new(chunks.clone());
        let mut chunk_write = chunk_adapter
            .begin_migration_write(Default::default())
            .await?;
        writes.lower_into(&mut chunk_write).await?;
        chunk_write.commit().await?;
        for (key, value) in capture(&chunks, options).await? {
            if target.get(&key) != Some(&value) {
                return Err(failure(
                    "candidate row-PK index chunk differs from source-derived content",
                ));
            }
        }
        old.row_pk_index_root_id = root;
        if old != new {
            return Err(failure(format!(
                "candidate changed source manifest semantics {id}; a separately certified closure repair is required"
            )));
        }
    }
    source_read.finish()?;
    target_read.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn v77_source() -> Memory {
        let memory = Memory::default();
        let session = StorageSession::acquire(memory.clone()).await.unwrap();
        let lix = crate::open_lix()
            .with_storage(session.clone())
            .await
            .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('witness-user','retained')",
            &[],
        )
        .await
        .unwrap();
        lix.close().await.unwrap();
        let mut records = capture(&session, MigrationOptions::default())
            .await
            .unwrap();
        records.insert(
            (
                crate::init::REPOSITORY_PROTOCOL_SPACE.id.0,
                Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_KEY),
            ),
            Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_V77),
        );
        for ((space, _), raw) in &mut records {
            if *space != crate::changelog::COMMIT_SPACE.id.0 {
                continue;
            }
            let (r, _) = commit(raw).unwrap();
            let old = super::super::checkpoint_metadata::CommitRecordV6 {
                format_version: 6,
                commit_id: r.commit_id,
                generation: r.generation,
                parent_commit_ids: r.parent_commit_ids,
                base_commit_id: r.base_commit_id,
                first_parent_jump_commit_id: r.first_parent_jump_commit_id,
                first_parent_jump_span: r.first_parent_jump_span,
                account_id: r.account_id,
                created_at: r.created_at,
                touched_scope_digest: r.touched_scope_digest,
            };
            *raw = Bytes::from(crate::storage_codec::encode("legacy fixture", &old).unwrap());
        }
        records.insert(
            (
                crate::sync::SYNC_AUTHORITY_STATE_SPACE.id.0,
                crate::sync::authority_state_key().0,
            ),
            Bytes::from_static(b"certified-authority-v4"),
        );
        copy(&records).await.unwrap()
    }

    async fn v78_native_checkpoint_source() -> (Memory, crate::changelog::CommitId) {
        let memory = Memory::default();
        let session = StorageSession::acquire(memory).await.unwrap();
        let lix = crate::open_lix()
            .with_storage(session.clone())
            .await
            .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value(key,value) VALUES('native-checkpoint','retained')",
            &[],
        )
        .await
        .unwrap();
        let checkpoint = lix
            .create_checkpoint()
            .await
            .unwrap()
            .commit_id
            .parse()
            .unwrap();
        lix.close().await.unwrap();
        let mut records = capture(&session, MigrationOptions::default())
            .await
            .unwrap();
        records.insert(
            (
                crate::init::REPOSITORY_PROTOCOL_SPACE.id.0,
                Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_KEY),
            ),
            Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_V78),
        );
        let source = copy(&records).await.unwrap();
        let adapter = StorageAdapter::new(source.clone());
        let read = super::super::MigrationPlanningRead::new(&adapter)
            .await
            .unwrap();
        let global = crate::branch::BranchHeadControlContext::new()
            .reader(read.clone())
            .load(crate::GLOBAL_BRANCH_ID)
            .await
            .unwrap()
            .unwrap();
        let markers = crate::tracked_state::TrackedStateContext::new()
            .reader(read.clone())
            .scan_batch_at_commit(
                &global.head_commit_id.to_string(),
                &crate::tracked_state::TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["lix_checkpoint".into()],
                        ..Default::default()
                    },
                    read_columns: crate::tracked_state::TrackedStateReadColumns {
                        columns: vec!["row_pk".into()],
                    },
                    limit: Some(10),
                },
            )
            .await
            .unwrap()
            .into_rows();
        assert!(
            markers.iter().all(|marker| marker.deleted),
            "native fixture has no live retired checkpoint marker"
        );
        read.finish().unwrap();
        (source, checkpoint)
    }

    #[tokio::test]
    async fn v78_native_checkpoint_without_legacy_marker_migrates() {
        let (source, checkpoint) = v78_native_checkpoint_source().await;
        let report = super::super::public_api::migrate_repository(source.clone())
            .await
            .unwrap();
        assert!(report.semantic_preservation_verified);
        assert_eq!(report.before.format, Some(78));
        // Migration acquires a physical storage fence. Inspect the result with
        // a fresh session instead of the now-fenced bare adapter.
        let source = StorageSession::acquire(source).await.unwrap();
        let records = capture(&source, MigrationOptions::default()).await.unwrap();
        let checkpoint_key = Bytes::copy_from_slice(checkpoint.as_uuid().as_bytes());
        let (record, _) =
            commit(&records[&(crate::changelog::COMMIT_SPACE.id.0, checkpoint_key.clone())])
                .unwrap();
        assert!(record.is_checkpoint);
        assert_eq!(
            records.get(&(
                crate::checkpoint::CHECKPOINT_INVENTORY_SPACE.id.0,
                checkpoint_key
            )),
            Some(&Bytes::new())
        );
    }

    #[tokio::test]
    async fn v78_witness_rejects_tampered_checkpoint_inventory_and_flags() {
        let (source, checkpoint) = v78_native_checkpoint_source().await;
        let options = MigrationOptions::default();
        let witness = plan(&source, options, false).await.unwrap();
        let id = Bytes::copy_from_slice(checkpoint.as_uuid().as_bytes());
        let inventory_key = (
            crate::checkpoint::CHECKPOINT_INVENTORY_SPACE.id.0,
            id.clone(),
        );
        let commit_key = (crate::changelog::COMMIT_SPACE.id.0, id);
        for tamper_source in [true, false] {
            for tamper_inventory in [true, false] {
                let mut original = witness.source.clone();
                let mut candidate = witness.expected.clone();
                let records = if tamper_source {
                    &mut original
                } else {
                    &mut candidate
                };
                if tamper_inventory {
                    assert!(records.remove(&inventory_key).is_some());
                } else {
                    let (mut record, _) = commit(&records[&commit_key]).unwrap();
                    assert!(record.is_checkpoint);
                    record.is_checkpoint = false;
                    records.insert(
                        commit_key.clone(),
                        Bytes::from(
                            crate::storage_codec::encode("tampered checkpoint", &record).unwrap(),
                        ),
                    );
                }
                let error = descriptors(&original, &candidate, options)
                    .await
                    .unwrap_err();
                assert_eq!(error.code, "LIX_MIGRATION_PRESERVATION_FAILED");
            }
        }
        // Membership alone is insufficient: the inventory's canonical empty
        // values also have to survive qualification unchanged.
        let mut broken = witness.source.clone();
        broken.insert(
            inventory_key,
            Bytes::from_static(b"invalid inventory value"),
        );
        assert!(
            descriptors(&broken, &witness.expected, options)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn v77_authority_source_witness_and_capability_upgrade() {
        let source = v77_source().await;
        let report = super::super::public_api::migrate_repository(source)
            .await
            .unwrap();
        assert!(report.semantic_preservation_verified);
        assert_eq!(report.before.format, Some(77));
        assert_eq!(
            report.after.role,
            super::super::public_api::RepositoryRole::Authority
        );
    }

    #[cfg(feature = "server-protocol")]
    #[tokio::test]
    async fn ordinary_serve_upgrades_legacy_authority_capability() {
        for current_format in [false, true] {
            let source = v77_source().await;
            let source = if current_format {
                let witness = plan(&source, MigrationOptions::default(), true)
                    .await
                    .unwrap();
                let mut records = witness.expected;
                records.insert(
                    (
                        crate::sync::SYNC_AUTHORITY_STATE_SPACE.id.0,
                        crate::sync::authority_state_key().0,
                    ),
                    Bytes::from_static(b"certified-authority-v4"),
                );
                copy(&records).await.unwrap()
            } else {
                source
            };
            let server = crate::open_lix()
                .with_storage(source.clone())
                .serve()
                .with_embedded_lix_id()
                .await
                .unwrap();
            server.close().await.unwrap();
            let storage = StorageSession::acquire(source).await.unwrap();
            let records = capture(&storage, MigrationOptions::default())
                .await
                .unwrap();
            assert_eq!(
                records.get(&(
                    crate::sync::SYNC_AUTHORITY_STATE_SPACE.id.0,
                    crate::sync::authority_state_key().0
                )),
                Some(&Bytes::from_static(crate::sync::AUTHORITY_STATE_VALUE))
            );
            let lix = crate::open_lix().with_storage(storage).await.unwrap();
            lix.partial_replica_descriptor(None).await.unwrap();
            lix.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn source_invariants_reject_payload_receipt_and_parent_mutations() {
        let source = v77_source().await;
        let options = MigrationOptions::default();
        let witness = plan(&source, options, true).await.unwrap();
        for space in [
            crate::hot_state::ROW_SPACE,
            crate::binary_cas::BINARY_CAS_CHUNK_SPACE,
            crate::session::EXECUTE_IDEMPOTENCY_RECEIPT_SPACE,
            crate::sync::PARTIAL_BRANCH_PUSH_SPACE,
        ] {
            let mut before = witness.source.clone();
            let mut after = witness.expected.clone();
            let key = (
                space.id.0,
                Bytes::from_static(b"preservation-negative-fixture"),
            );
            before.insert(key.clone(), Bytes::from_static(b"source-owned"));
            after.insert(key.clone(), Bytes::from_static(b"source-owned"));
            independent_invariants(&before, &after).unwrap();
            after.remove(&key);
            assert!(
                independent_invariants(&before, &after).is_err(),
                "missing {}",
                space.id.0
            );
            after.insert(key, Bytes::from_static(b"unplanned"));
            assert!(
                independent_invariants(&before, &after).is_err(),
                "changed {}",
                space.id.0
            );
        }
        let key = witness
            .expected
            .keys()
            .find(|(space, _)| *space == crate::changelog::COMMIT_SPACE.id.0)
            .unwrap()
            .clone();
        for change_base in [false, true] {
            let mut broken = witness.expected.clone();
            let (mut record, _) = commit(&broken[&key]).unwrap();
            if change_base {
                record.base_commit_id = Some(record.commit_id);
            } else {
                record.parent_commit_ids.push(record.commit_id);
            }
            broken.insert(
                key.clone(),
                Bytes::from(crate::storage_codec::encode("tampered commit", &record).unwrap()),
            );
            assert!(
                descriptors(&witness.source, &broken, options)
                    .await
                    .is_err()
            );
        }
    }
}
