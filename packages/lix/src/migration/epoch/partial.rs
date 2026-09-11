//! Bounded epoch admission for partial replicas. Ordinary migration/recovery
//! remains explicit: this path never scans, clears banks, or follows history.

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

async fn durable_pointer<S: Storage>(
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
    drop(read);
    let Some(state) = crate::sync::upgrade_owned_partial_receipt(&adapter).await? else {
        return Err(migration_required(
            "existing full repositories require explicit conversion to a partial replica",
        ));
    };
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
        .commit_write_set(
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
