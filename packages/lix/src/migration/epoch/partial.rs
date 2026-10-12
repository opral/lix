//! Current-format partial admission is bounded and read-only. Older replica
//! caches can be replaced with authenticated opening coordinates in a new bank.

use super::*;
use crate::storage_adapter::{StorageReadDurability, StorageWriteSetError};
use crate::sync::PartialReplicaState;

pub(crate) struct PartialEpochAdmission<S> {
    pub(crate) adapter: StorageAdapter<S>,
    pub(crate) state: PartialReplicaState,
}

fn migration_required(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_REPLICA_MIGRATION_REQUIRED", message)
}

const PARTIAL_RECEIPT_VERSION_PROBE_MAX_BYTES: usize = 16 * 1024;

/// Classify only the explicitly supported historical receipt versions. This
/// is a routing probe for an owned epoch migration, never an admission path:
/// ordinary readers still decode and validate only the current receipt.
pub(super) async fn historical_partial_receipt_version(
    read: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
) -> Result<Option<u32>, LixError> {
    let value = crate::storage_adapter::PointReadPlan::new(
        crate::sync::PARTIAL_REPLICA_STATE_SPACE,
        &[crate::sync::partial_replica_state_key()],
    )
    .materialize(read, Default::default())
    .await?
    .value
    .pop()
    .flatten();
    let Some(ProjectedValue::FullValue(bytes)) = value else {
        return Ok(None);
    };
    if bytes.len() > PARTIAL_RECEIPT_VERSION_PROBE_MAX_BYTES {
        return Ok(None);
    }
    #[derive(serde::Deserialize)]
    struct VersionEnvelope {
        version: u32,
    }
    let Ok(envelope) = serde_json::from_slice::<VersionEnvelope>(&bytes) else {
        return Ok(None);
    };
    Ok(matches!(envelope.version, 1 | 2).then_some(envelope.version))
}

fn historical_receipt_migration_required(version: u32) -> LixError {
    migration_required("partial receipt version requires explicit epoch migration").with_details(
        serde_json::json!({
            "receiptVersion": version,
            "expectedReceiptVersion": 3,
            "migrationPhase": "partial_receipt",
            "failureReason": "unsupported_receipt_version",
            "failurePath": "$",
        }),
    )
}

pub(super) async fn durable_pointer<S: Storage>(
    storage: &S,
) -> Result<Option<(PointerState, Bytes)>, LixError> {
    let read = storage
        .begin_read(ReadOptions {
            durability: StorageReadDurability::Durable,
            ..Default::default()
        })
        .await?;
    let keys = [Key(Bytes::from_static(REPOSITORY_EPOCH_KEY))];
    let values = read
        .get_many(&[GetManyRequest {
            space: REPOSITORY_EPOCH_SPACE,
            keys: &keys,
            opts: GetOptions::default(),
        }])
        .await?
        .values;
    if values.len() != 1 {
        return Err(epoch_error(
            "epoch pointer read returned incorrect cardinality",
        ));
    }
    match values.into_iter().next().flatten() {
        None => Ok(None),
        Some(ProjectedValue::FullValue(bytes)) => Ok(Some((decode_pointer(&bytes)?, bytes))),
        Some(ProjectedValue::KeyOnly) => Err(epoch_error("epoch pointer read omitted its payload")),
    }
}

/// A fixed metadata probe used before an authority handshake. This is not a
/// claim of empty storage: fresh publication still enforces atomic RangeEmpty
/// predicates. Missing markers with other orphaned bytes fail that publication.
pub(crate) async fn partial_epoch_has_no_markers<S>(storage: &S) -> Result<bool, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if durable_pointer(storage).await?.is_some() {
        return Ok(false);
    }
    let read = storage
        .begin_read(ReadOptions {
            durability: StorageReadDurability::Durable,
            ..Default::default()
        })
        .await?;
    let markers = [
        (
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            Key(Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_KEY)),
        ),
        (
            crate::sync::PARTIAL_REPLICA_STATE_SPACE,
            crate::sync::partial_replica_state_key(),
        ),
        (
            crate::sync::SYNC_REPLICA_STATE_SPACE,
            crate::sync::replica_state_key(),
        ),
        (
            crate::sync::SYNC_AUTHORITY_STATE_SPACE,
            crate::sync::authority_state_key(),
        ),
    ];
    let requests = markers
        .iter()
        .map(|(space, key)| GetManyRequest {
            space: *space,
            keys: std::slice::from_ref(key),
            opts: GetOptions {
                projection: CoreProjection::KeyOnly,
            },
        })
        .collect::<Vec<_>>();
    let values = read.get_many(&requests).await?.values;
    if values.len() != markers.len() {
        return Err(epoch_error(
            "partial marker probe returned incorrect cardinality",
        ));
    }
    Ok(values.into_iter().all(|value| value.is_none()))
}

/// Resolve only a durable current-format active epoch and its partial receipt.
/// Three coherent reads contain a fixed number of point reads. A concurrent
/// pointer replacement is rejected by the returned adapter's exact-byte fence.
/// The caller separately binds the receipt's remote/account to its transport.
pub(crate) async fn admit_partial_epoch<S>(
    storage: &S,
) -> Result<PartialEpochAdmission<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let Some((PointerState::Active { bank, format, .. }, pointer)) =
        durable_pointer(storage).await?
    else {
        return Err(migration_required(
            "partial replica requires an active epoch; missing or interrupted repositories need explicit initialization or recovery",
        ));
    };
    if format != crate::init::CURRENT_FORMAT_VERSION {
        return Err(migration_required(
            "partial replica epoch format requires explicit migration",
        ));
    }
    let adapter = StorageAdapter::for_epoch(storage.clone(), bank, pointer);
    let read = adapter
        .begin_read(ReadOptions {
            durability: StorageReadDurability::Durable,
            ..Default::default()
        })
        .await?;
    if !crate::init::is_partial_repository_protocol(&read).await? {
        return Err(migration_required(
            "partial replica repository protocol requires explicit migration",
        ));
    }
    let (state, _) = match crate::sync::load_partial_replica_state(&read).await {
        Ok(Some(state)) => state,
        Ok(None) => {
            return Err(migration_required(
                "existing full repositories require explicit conversion to a partial replica",
            ));
        }
        Err(error) => {
            if let Some(version) = historical_partial_receipt_version(&read).await? {
                return Err(historical_receipt_migration_required(version));
            }
            return Err(error);
        }
    };
    // Valid prerelease v3 (and released v2) journals must be rewritten inside
    // the detached epoch migration before Engine restores the strict v4
    // journal. The upgrader is also the validator here: malformed or unknown
    // journal data remains a corruption error and is never hidden by routing
    // it through migration.
    if crate::sync::legacy_read_interest_journal_upgrade(&read)
        .await?
        .is_some()
    {
        return Err(migration_required(
            "partial read-interest journal requires explicit epoch migration",
        ));
    }
    drop(read);
    Ok(PartialEpochAdmission { adapter, state })
}

/// Atomically publish a fresh partial epoch and its bounded canonical opening
/// coordinates. Legacy is a valid physical epoch bank; later migrations retain
/// the same exact-pointer fencing as A/B. No temporary bank, heartbeat or bank
/// sweep is necessary because every opening record fits one atomic commit.
///
/// Freshness is checked inside that commit with three RangeEmpty predicates per
/// registered current/retired logical space (Legacy/A/B), plus epoch control.
/// Each predicate needs only an indexed existence check; no rows are returned
/// or enumerated. Valid retained Generation banks always retain epoch control
/// metadata, which the control-space predicate rejects. Arbitrary orphaned
/// Generation bytes without their control records are outside valid lifecycle.
/// The predicate count depends on the engine's fixed catalog,
/// never repository contents. An existing repository is rejected untouched.
pub(crate) async fn install_fresh_partial_epoch<S>(
    storage: S,
    state: &PartialReplicaState,
) -> Result<PartialEpochAdmission<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    // Verify durable-read support before publishing anything. This also rejects
    // an existing active/interrupted epoch without starting migration recovery.
    if durable_pointer(&storage).await?.is_some() {
        return Err(migration_required(
            "partial initialization requires fresh storage; existing epochs need explicit migration",
        ));
    }
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter.begin_read(Default::default()).await?;
    let mut writes = adapter.new_write_set();
    crate::init::stage_partial_repository_protocol(&mut writes);
    let mut preconditions = crate::sync::stage_partial_bootstrap(&read, &mut writes, state)?;
    drop(read);
    let pointer = encode_pointer(PointerState::Active {
        bank: EpochBank::Legacy,
        generation: 1,
        format: crate::init::CURRENT_FORMAT_VERSION,
        publication: Some(uuid::Uuid::now_v7()),
    });
    writes.put(
        REPOSITORY_EPOCH_SPACE,
        REPOSITORY_EPOCH_KEY,
        pointer.as_ref(),
    );
    preconditions.push(Precondition::RangeEmpty {
        space: REPOSITORY_EPOCH_SPACE,
        range: KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        },
    });
    preconditions.extend(
        crate::storage_spaces::SNAPSHOT_STORAGE_SPACES
            .iter()
            .copied()
            .chain(
                crate::storage_spaces::RETIRED_STORAGE_SPACES
                    .iter()
                    .filter(|entry| !entry.emitted_in_lixsnap_v1)
                    .map(|entry| entry.space),
            )
            .flat_map(|space| {
                [EpochBank::Legacy, EpochBank::A, EpochBank::B]
                    .into_iter()
                    .map(move |bank| Precondition::RangeEmpty {
                        space: bank.map_space(space),
                        range: KeyRange {
                            lower: Bound::Unbounded,
                            upper: Bound::Unbounded,
                        },
                    })
            }),
    );
    match adapter
        .commit_partial_replica_write_set(
            crate::sync::partial_replica_write_capability(),
            writes,
            WriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => {}
        Err(StorageWriteSetError::Storage(StorageError::PreconditionFailed(_))) => {
            return Err(migration_required(
                "partial initialization found existing storage; explicit migration is required",
            ));
        }
        Err(error) => return Err(error.into()),
    }
    let admitted = admit_partial_epoch(&storage).await?;
    if &admitted.state != state {
        return Err(epoch_error(
            "published partial receipt changed before admission",
        ));
    }
    Ok(admitted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    async fn state() -> PartialReplicaState {
        let authority = crate::open_lix().await.unwrap();
        PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            authority.active_account_id().to_owned(),
            "00000000-0000-7000-8000-000000000599".into(),
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn legacy_partial_epoch_migrates_resident_state_without_full_inputs() {
        let state = state().await;
        let backing = crate::Memory::new();
        let storage = crate::sync::durable_memory_for_test(backing.clone());
        let admitted = install_fresh_partial_epoch(storage.clone(), &state)
            .await
            .unwrap();
        crate::migration::downgrade_headers_for_test(&admitted.adapter, true).await;
        let legacy = encode_pointer(PointerState::Active {
            bank: EpochBank::Legacy,
            generation: 1,
            format: 79,
            publication: None,
        });
        let mut write = backing.begin_write(WriteOptions::default()).await.unwrap();
        put_pointer(&mut write, legacy).await.unwrap();
        write.commit().await.unwrap();
        assert!(admit_partial_epoch(&storage).await.is_err());
        admit_repository(&storage, None).await.unwrap();
        let reopened = admit_partial_epoch(&storage).await.unwrap();
        assert_eq!(reopened.state, state);
        assert!(matches!(
            durable_pointer(&storage).await.unwrap().unwrap().0,
            PointerState::Active {
                bank: EpochBank::A,
                format: crate::init::CURRENT_FORMAT_VERSION,
                ..
            }
        ));
        let read = reopened
            .adapter
            .begin_read(Default::default())
            .await
            .unwrap();
        assert_eq!(
            crate::sync::load_partial_replica_state(&read)
                .await
                .unwrap()
                .unwrap()
                .0,
            state
        );
        drop(read);
        assert_eq!(admit_partial_epoch(&storage).await.unwrap().state, state);
        assert!(matches!(
            admitted.adapter.begin_read(Default::default()).await,
            Err(StorageError::Fenced)
        ));
    }

    #[tokio::test]
    async fn pointerless_partial_repository_migrates_with_partial_role() {
        let state = state().await;
        for from_format in [79, crate::init::CURRENT_FORMAT_VERSION] {
            let storage = crate::sync::durable_memory_for_test(crate::Memory::new());
            let initial = install_fresh_partial_epoch(storage.clone(), &state)
                .await
                .unwrap();
            if from_format == 79 {
                crate::migration::downgrade_headers_for_test(&initial.adapter, true).await;
                let mut writes = crate::storage_adapter::StorageWriteSet::new();
                writes.put(
                    EpochBank::Legacy.map_space(crate::init::REPOSITORY_PROTOCOL_SPACE),
                    crate::init::REPOSITORY_PROTOCOL_KEY,
                    crate::init::PARTIAL_REPOSITORY_PROTOCOL_V79,
                );
                writes
                    .commit(&storage, WriteOptions::default())
                    .await
                    .unwrap();
            }
            let (_, pointer) = durable_pointer(&storage).await.unwrap().unwrap();
            delete_pointer(&storage, &pointer).await.unwrap();

            assert!(matches!(
                crate::migration::inspect_lix(&storage).await.unwrap(),
                crate::migration::MigrationStatus::Malformed
            ));
            let source = inspect_partial_replacement(&storage).await.unwrap();
            assert!(source.is_partial());
            assert_eq!(source.format(), from_format);

            let events = Arc::new(Mutex::new(Vec::<OpenProgress>::new()));
            let captured = Arc::clone(&events);
            let sink: Arc<dyn OpenProgressSink> =
                Arc::new(crate::CallbackOpenProgressSink::new(move |event| {
                    captured.lock().unwrap().push(event)
                }));
            let migrated = admit_partial_repository(&storage, (from_format == 79).then_some(&sink))
                .await
                .unwrap();
            assert_eq!(migrated.report.migration.unwrap().from_format, from_format);
            if from_format == 79 {
                let events = events.lock().unwrap();
                let validation = events
                    .iter()
                    .position(|event| event.phase == OpenPhase::Validating)
                    .expect("successful sparse migration validates after repair steps");
                let zeroes: Vec<_> = events[..validation]
                    .iter()
                    .enumerate()
                    .filter(|(_, event)| {
                        event.phase == OpenPhase::Migrating && event.completed == Some(0)
                    })
                    .map(|(index, _)| index)
                    .collect();
                let postcopy = *zeroes
                    .last()
                    .expect("sparse repair progress starts after the candidate copy");
                assert!(
                    events[..postcopy].iter().any(|event| {
                        event.phase == OpenPhase::Migrating
                            && event.completed.is_some_and(|completed| completed > 0)
                    }),
                    "candidate copy reports work before repair progress restarts"
                );
                let completed: Vec<_> = events[postcopy..validation]
                    .iter()
                    .filter(|event| event.phase == OpenPhase::Migrating)
                    .map(|event| event.completed)
                    .collect();
                assert_eq!(
                    completed,
                    (0_u64..=9).map(Some).collect::<Vec<_>>(),
                    "repair completion advances only after each awaited sparse migration step succeeds"
                );
                assert!(events[postcopy..validation].iter().all(|event| {
                    event.scope == crate::OpenScope::Local
                        && event.from_format == Some(79)
                        && event.to_format == crate::init::CURRENT_FORMAT_VERSION
                }));
            }
            let reopened = admit_partial_epoch(&storage).await.unwrap();
            assert_eq!(reopened.state, state);
            let read = reopened
                .adapter
                .begin_read(ReadOptions::default())
                .await
                .unwrap();
            assert!(
                crate::init::is_partial_repository_protocol(&read)
                    .await
                    .unwrap()
            );
        }
    }

    #[tokio::test]
    async fn ordinary_partial_open_migrates_v3_journal_and_keeps_pending_push_state() {
        let state = state().await;
        let storage = crate::sync::durable_memory_for_test(crate::Memory::new());
        let installed = install_fresh_partial_epoch(storage.clone(), &state)
            .await
            .unwrap();
        let selected = &state.descriptor().selected_branch;
        let mut history = serde_json::to_value(crate::hot_state::LogicalReadInterest::History {
            branch_id: selected.branch_id.clone(),
            commit_ids: vec![selected.head.commit_id.clone()],
            relation: "lix_file".into(),
            filter: Default::default(),
            retain_payloads: false,
            projected_columns: vec!["id".into()],
            limit: None,
        })
        .unwrap();
        history["anchor"] = serde_json::json!(selected.head.commit_id);
        let paths = serde_json::to_value(crate::hot_state::LogicalReadInterest::FilesystemPaths {
            scope: crate::filesystem::FilesystemPathIndexScope::All,
            branch_ids: vec![selected.branch_id.clone()],
            include_blob_refs: false,
            cache_small_blob_data: false,
        })
        .unwrap();
        let old_journal = serde_json::to_vec(&serde_json::json!({
            "version": 3,
            "epochId": state.epoch_id(),
            "recipes": [history, paths],
        }))
        .unwrap();
        let journal_key =
            crate::storage_codec::id_string::uuid_bytes_from_canonical(state.epoch_id()).unwrap();
        let push_key =
            crate::storage_codec::id_string::uuid_bytes_from_canonical(&selected.branch_id)
                .unwrap();
        let pending_push = serde_json::to_vec(&serde_json::json!({
            "version": 2,
            "epochId": state.epoch_id(),
            "branchId": selected.branch_id,
            "confirmed": {
                "head": selected.head.commit_id,
                "checkpoint": selected.checkpoint.commit_id,
            },
            "prepared": {
                "attemptId": "00000000-0000-7000-8000-000000000455",
                "createdRefs": [],
                "expected": {
                    "head": selected.head.commit_id,
                    "checkpoint": selected.checkpoint.commit_id,
                },
                "target": {
                    "head": selected.head.commit_id,
                    "checkpoint": selected.checkpoint.commit_id,
                },
            },
            "bodiesAcknowledged": false,
        }))
        .unwrap();
        let mut writes = installed.adapter.new_write_set();
        writes.put(
            crate::sync::PARTIAL_READ_INTEREST_SPACE,
            journal_key.as_slice(),
            old_journal.as_slice(),
        );
        writes.put(
            crate::sync::PARTIAL_BRANCH_PUSH_SPACE,
            push_key.as_slice(),
            pending_push.as_slice(),
        );
        use crate::storage_adapter::StorageWrite as _;
        let mut write = installed
            .adapter
            .begin_migration_write(Default::default())
            .await
            .unwrap();
        writes.lower_into(&mut write).await.unwrap();
        write.commit().await.unwrap();

        let mut opened = crate::sync::prepare_partial_open(storage, None, None)
            .await
            .unwrap();
        assert!(
            opened.migration.is_some(),
            "ordinary open must take the detached migration route"
        );
        assert_eq!(opened.state.as_ref(), &state);
        let read = opened
            .adapter
            .begin_read(ReadOptions::default())
            .await
            .unwrap();
        crate::sync::validate_partial_read_interest_journal(&read, &state)
            .await
            .unwrap();
        let journal = crate::storage_adapter::PointReadPlan::new(
            crate::sync::PARTIAL_READ_INTEREST_SPACE,
            &[crate::storage_adapter::StorageKey(Bytes::copy_from_slice(
                &journal_key,
            ))],
        )
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value
        .pop()
        .flatten();
        let Some(ProjectedValue::FullValue(journal)) = journal else {
            panic!("migrated journal is missing");
        };
        let journal: serde_json::Value = serde_json::from_slice(&journal).unwrap();
        assert_eq!(journal["version"], 4);
        assert_eq!(journal["recipes"].as_array().unwrap().len(), 2);
        assert!(
            journal["recipes"]
                .as_array()
                .unwrap()
                .iter()
                .all(|recipe| recipe.get("anchor").is_none())
        );
        let push = crate::storage_adapter::PointReadPlan::new(
            crate::sync::PARTIAL_BRANCH_PUSH_SPACE,
            &[crate::storage_adapter::StorageKey(Bytes::copy_from_slice(
                &push_key,
            ))],
        )
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value
        .pop()
        .flatten();
        let Some(ProjectedValue::FullValue(push)) = push else {
            panic!("pending upload state was lost during migration");
        };
        let push: serde_json::Value = serde_json::from_slice(&push).unwrap();
        assert_eq!(
            push["prepared"]["attemptId"],
            "00000000-0000-7000-8000-000000000455"
        );
        drop(read);
        opened.close_after_error().await;
    }

    #[tokio::test]
    async fn malformed_v3_interest_journal_is_corruption_not_migration_routing() {
        let state = state().await;
        let storage = crate::sync::durable_memory_for_test(crate::Memory::new());
        let installed = install_fresh_partial_epoch(storage.clone(), &state)
            .await
            .unwrap();
        let journal_key =
            crate::storage_codec::id_string::uuid_bytes_from_canonical(state.epoch_id()).unwrap();
        let malformed = serde_json::to_vec(&serde_json::json!({
            "version": 3,
            "epochId": state.epoch_id(),
            "recipes": [{"kind":"history", "anchor":"noncanonical"}],
        }))
        .unwrap();
        let mut writes = installed.adapter.new_write_set();
        writes.put(
            crate::sync::PARTIAL_READ_INTEREST_SPACE,
            journal_key.as_slice(),
            malformed.as_slice(),
        );
        use crate::storage_adapter::StorageWrite as _;
        let mut write = installed
            .adapter
            .begin_migration_write(Default::default())
            .await
            .unwrap();
        writes.lower_into(&mut write).await.unwrap();
        write.commit().await.unwrap();

        let error = match admit_partial_epoch(&storage).await {
            Ok(_) => panic!("malformed legacy journal was admitted"),
            Err(error) => error,
        };
        assert_eq!(error.code, "LIX_PARTIAL_INTEREST_JOURNAL_INVALID");
        assert!(
            !error
                .to_string()
                .contains("requires explicit epoch migration")
        );
    }

    #[tokio::test]
    async fn fresh_partial_epoch_is_atomic_durable_and_reopens_fenced() {
        let state = state().await;
        let backing = crate::Memory::new();
        let storage = crate::sync::durable_memory_for_test(backing.clone());
        let admitted = install_fresh_partial_epoch(storage.clone(), &state)
            .await
            .unwrap();
        assert_eq!(admitted.state, state);
        let reopened = admit_partial_epoch(&storage).await.unwrap();
        assert_eq!(reopened.state, state);
        assert!(
            install_fresh_partial_epoch(storage.clone(), &state)
                .await
                .is_err()
        );
        let (engine, session) =
            Engine::new_partial_replica(reopened.adapter, EngineOptions::new(), &state)
                .await
                .unwrap();
        assert_eq!(engine.lix_id(), state.repository_id());
        drop(session);
        drop(engine);
        let replacement = encode_pointer(PointerState::Active {
            bank: EpochBank::B,
            generation: 2,
            format: crate::init::CURRENT_FORMAT_VERSION,
            publication: None,
        });
        let mut write = backing.begin_write(WriteOptions::default()).await.unwrap();
        put_pointer(&mut write, replacement).await.unwrap();
        write.commit().await.unwrap();
        assert!(matches!(
            admitted.adapter.begin_read(ReadOptions::default()).await,
            Err(StorageError::Fenced)
        ));
    }

    #[tokio::test]
    async fn current_format_historical_receipts_request_owned_migration() {
        let state = state().await;
        for version in [1, 2, 99] {
            let storage = crate::sync::durable_memory_for_test(crate::Memory::new());
            let admitted = install_fresh_partial_epoch(storage.clone(), &state)
                .await
                .unwrap();
            let historical = if version == 99 {
                let mut current = serde_json::to_value(&state).unwrap();
                current["version"] = serde_json::json!(version);
                current
            } else {
                let mut legacy: serde_json::Value =
                    serde_json::from_slice(&crate::sync::released_v2_receipt_bytes_for_test(
                        &state,
                        if version == 1 { 1 } else { 2 },
                    ))
                    .unwrap();
                legacy["version"] = serde_json::json!(version);
                if version == 1 {
                    legacy.as_object_mut().unwrap().remove("archivedBranchIds");
                }
                legacy
            };
            let mut writes = admitted.adapter.new_write_set();
            writes.put(
                crate::sync::PARTIAL_REPLICA_STATE_SPACE,
                crate::sync::partial_replica_state_key(),
                serde_json::to_vec(&historical).unwrap(),
            );
            use crate::storage_adapter::StorageWrite as _;
            let mut write = admitted
                .adapter
                .begin_migration_write(Default::default())
                .await
                .unwrap();
            writes.lower_into(&mut write).await.unwrap();
            write.commit().await.unwrap();

            let pointer_before = durable_pointer(&storage).await.unwrap().unwrap().1;
            let read = admitted
                .adapter
                .begin_read(Default::default())
                .await
                .unwrap();
            assert_eq!(
                crate::sync::load_partial_replica_state(&read)
                    .await
                    .err()
                    .unwrap()
                    .code,
                "LIX_PARTIAL_REPLICA_STATE_INVALID",
                "ordinary readers must continue to reject v{version}"
            );
            drop(read);
            let error = admit_partial_epoch(&storage).await.err().unwrap();
            assert_eq!(
                error.code,
                if version == 1 || version == 2 {
                    "LIX_PARTIAL_REPLICA_MIGRATION_REQUIRED"
                } else {
                    "LIX_PARTIAL_REPLICA_STATE_INVALID"
                },
                "only supported historical versions may route to migration"
            );
            if version == 1 || version == 2 {
                let details = error.details.as_ref().unwrap();
                assert_eq!(details["receiptVersion"], version);
                assert_eq!(details["expectedReceiptVersion"], 3);
                assert_eq!(details["migrationPhase"], "partial_receipt");
                assert_eq!(details["failureReason"], "unsupported_receipt_version");
                assert_eq!(details["failurePath"], "$");
            }
            assert_eq!(
                durable_pointer(&storage).await.unwrap().unwrap().1,
                pointer_before,
                "classification must not mutate the active epoch"
            );
        }
    }

    #[tokio::test]
    async fn partial_epoch_rejects_full_old_and_interrupted_repositories_without_recovery() {
        for kind in 0..4 {
            let backing = crate::Memory::new();
            let storage = crate::sync::durable_memory_for_test(backing.clone());
            let pointer = match kind {
                0 => None,
                1 => Some(PointerState::Active {
                    bank: EpochBank::Legacy,
                    generation: 1,
                    format: crate::init::CURRENT_FORMAT_VERSION,
                    publication: None,
                }),
                2 => Some(PointerState::Active {
                    bank: EpochBank::Legacy,
                    generation: 1,
                    format: crate::init::CURRENT_FORMAT_VERSION - 1,
                    publication: None,
                }),
                _ => Some(PointerState::Migrating {
                    source: EpochBank::Legacy,
                    source_format: 0,
                    target: EpochBank::A,
                    generation: 1,
                    attempt: uuid::Uuid::now_v7(),
                }),
            };
            if let Some(pointer) = pointer {
                let adapter = StorageAdapter::new(storage.clone());
                let mut writes = adapter.new_write_set();
                crate::init::stage_repository_protocol(&mut writes);
                writes.put(
                    REPOSITORY_EPOCH_SPACE,
                    REPOSITORY_EPOCH_KEY,
                    encode_pointer(pointer).as_ref(),
                );
                adapter
                    .commit_write_set(writes, WriteOptions::default())
                    .await
                    .unwrap();
            }
            let before = durable_pointer(&storage)
                .await
                .unwrap()
                .map(|(_, bytes)| bytes);
            let error = admit_partial_epoch(&storage).await.err().unwrap();
            assert_eq!(error.code, "LIX_PARTIAL_REPLICA_MIGRATION_REQUIRED");
            assert_eq!(
                durable_pointer(&storage)
                    .await
                    .unwrap()
                    .map(|(_, bytes)| bytes),
                before
            );
        }
    }

    #[tokio::test]
    async fn fresh_partial_epoch_does_not_overwrite_pointerless_repository_data() {
        let state = state().await;
        let backing = crate::Memory::new();
        let storage = crate::sync::durable_memory_for_test(backing.clone());
        let adapter = StorageAdapter::new(storage.clone());
        let mut writes = adapter.new_write_set();
        crate::init::stage_repository_protocol(&mut writes);
        adapter
            .commit_write_set(writes, WriteOptions::default())
            .await
            .unwrap();
        assert!(
            install_fresh_partial_epoch(storage.clone(), &state)
                .await
                .is_err()
        );
        assert!(durable_pointer(&storage).await.unwrap().is_none());
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert!(
            crate::sync::load_partial_replica_state(&read)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            crate::init::repository_protocol_status(&read)
                .await
                .unwrap(),
            crate::init::RepositoryProtocolStatus::Current
        );
    }

    #[tokio::test]
    async fn fresh_partial_epoch_rejects_orphaned_a_and_b_payloads() {
        let state = state().await;
        for bank in [EpochBank::A, EpochBank::B] {
            let backing = crate::Memory::new();
            let storage = crate::sync::durable_memory_for_test(backing.clone());
            let adapter = StorageAdapter::for_epoch_unfenced(storage.clone(), bank);
            let mut writes = adapter.new_write_set();
            crate::init::stage_repository_protocol(&mut writes);
            adapter
                .commit_write_set(writes, WriteOptions::default())
                .await
                .unwrap();
            assert!(
                install_fresh_partial_epoch(storage.clone(), &state)
                    .await
                    .is_err()
            );
            assert!(durable_pointer(&storage).await.unwrap().is_none());
            let read = adapter.begin_read(Default::default()).await.unwrap();
            assert_eq!(
                crate::init::repository_protocol_status(&read)
                    .await
                    .unwrap(),
                crate::init::RepositoryProtocolStatus::Current
            );
            assert!(
                crate::sync::load_partial_replica_state(&read)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn partial_epoch_rejects_temporary_full_layout_marker_without_conversion() {
        let state = state().await;
        let backing = crate::Memory::new();
        let storage = crate::sync::durable_memory_for_test(backing.clone());
        let admitted = install_fresh_partial_epoch(storage.clone(), &state)
            .await
            .unwrap();
        let before = durable_pointer(&storage).await.unwrap().unwrap().1;
        // Simulate the temporary development layout without using an admitted
        // engine to overwrite its own marker.
        let mut write = backing.begin_write(WriteOptions::default()).await.unwrap();
        write
            .put_many(
                crate::init::REPOSITORY_PROTOCOL_SPACE,
                PutBatch {
                    entries: vec![PutEntry {
                        key: Key(Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_KEY)),
                        value: StoredValue {
                            bytes: Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_VALUE),
                        },
                    }],
                },
            )
            .await
            .unwrap();
        write.commit().await.unwrap();
        assert_eq!(
            admit_partial_epoch(&storage).await.err().unwrap().code,
            "LIX_PARTIAL_REPLICA_MIGRATION_REQUIRED"
        );
        assert_eq!(
            Engine::new_partial_replica(admitted.adapter, EngineOptions::new(), &state)
                .await
                .err()
                .unwrap()
                .code,
            "LIX_PARTIAL_REPLICA_MIGRATION_REQUIRED"
        );
        assert_eq!(durable_pointer(&storage).await.unwrap().unwrap().1, before);
    }

    #[tokio::test]
    async fn fresh_partial_epoch_requires_durable_reads_before_mutation() {
        let state = state().await;
        let storage = crate::Memory::new();
        assert!(
            install_fresh_partial_epoch(storage.clone(), &state)
                .await
                .is_err()
        );
        assert!(load_pointer(&storage).await.unwrap().is_none());
    }
}

/// Routing hint only, never admission or repair. Default reads preserve the
/// standalone Memory opening contract; partial admission later uses Durable.
pub(crate) async fn has_partial_replica_marker<S>(storage: &S) -> Result<bool, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let banks = match load_pointer(storage).await? {
        Some((PointerState::Active { bank, .. }, _)) => vec![bank],
        Some((PointerState::Migrating { source, target, .. }, _)) => vec![source, target],
        None => vec![EpochBank::Legacy],
    };
    for bank in banks {
        // Unfenced is safe for this read-only routing hint: the opening path
        // MUST re-read and validate the pointer before granting capabilities.
        let adapter = StorageAdapter::for_epoch_unfenced(storage.clone(), bank);
        let read = adapter.begin_read(ReadOptions::default()).await?;
        let marker = crate::storage_adapter::PointReadPlan::new(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            &[Key(Bytes::from_static(
                crate::init::REPOSITORY_PROTOCOL_KEY,
            ))],
        )
        .materialize(&read, GetOptions::default())
        .await?
        .value;
        if let Some(ProjectedValue::FullValue(marker)) = marker.into_iter().next().flatten() {
            // Also route older partial formats to explicit partial migration;
            // they must never enter ordinary whole-repository admission.
            if marker
                .windows(b"-partial-replica.".len())
                .any(|part| part == b"-partial-replica.")
            {
                return Ok(true);
            }
        }
        let receipt = crate::storage_adapter::PointReadPlan::new(
            crate::sync::PARTIAL_REPLICA_STATE_SPACE,
            &[crate::sync::partial_replica_state_key()],
        )
        .materialize(
            &read,
            GetOptions {
                projection: CoreProjection::KeyOnly,
            },
        )
        .await?
        .value;
        if receipt.into_iter().next().flatten().is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}
