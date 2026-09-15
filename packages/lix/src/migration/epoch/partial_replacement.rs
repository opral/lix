//! Rebuild a disposable replica cache from bounded authenticated coordinates.
//! Source banks remain detached and intact; opening never migrates their rows.

use super::*;
use crate::storage_adapter::{StorageReadDurability, StorageWriteSet, StorageWriteSetError};

pub(crate) struct PartialReplacementSource {
    pointer: Option<Bytes>,
    bank: EpochBank,
    interrupted_target: Option<EpochBank>,
    generation: u64,
    format: u32,
    protocol: Bytes,
    repository_id: String,
    // A partial receipt is small and already contains the durable account and
    // remote binding. Complete-replica receipts can grow with history, so only
    // their role key is probed; no old account's content enters the new cache.
    partial_receipt: Option<Bytes>,
    partial_account: Option<String>,
    partial_remote: Option<String>,
}

async fn metadata(
    read: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
    space: StorageSpace,
    key: Key,
    projection: CoreProjection,
) -> Result<Option<ProjectedValue>, LixError> {
    let values = read
        .get_many(&[GetManyRequest {
            space,
            keys: std::slice::from_ref(&key),
            opts: GetOptions { projection },
        }])
        .await?
        .values;
    if values.len() != 1 {
        return Err(epoch_error(
            "replica metadata read returned incorrect cardinality",
        ));
    }
    Ok(values.into_iter().next().flatten())
}

/// Fixed point reads plus one exact indexed identity lookup. In particular,
/// the potentially history-sized full-replica receipt is never materialized.
pub(crate) async fn inspect_partial_replacement<S>(
    storage: &S,
) -> Result<PartialReplacementSource, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let (bank, generation, format, pointer, interrupted_target) =
        match partial::durable_pointer(storage).await? {
            None => (EpochBank::Legacy, 0, 0, None, None),
            Some((
                PointerState::Active {
                    bank,
                    generation,
                    format,
                    ..
                },
                bytes,
            )) => (bank, generation, format, Some(bytes), None),
            // Partial opening owns the exclusive lifecycle gate. CAS activation
            // fences an interrupted migrator without reading its incomplete target.
            Some((
                PointerState::Migrating {
                    source,
                    source_format,
                    target,
                    generation,
                    ..
                },
                bytes,
            )) => (source, generation, source_format, Some(bytes), Some(target)),
        };
    let adapter = StorageAdapter::for_epoch_unfenced(storage.clone(), bank);
    let read = adapter
        .begin_read(ReadOptions {
            durability: StorageReadDurability::Durable,
            ..Default::default()
        })
        .await?;
    let unknown = || crate::sync::replica_replacement_unavailable("unknown_state");
    if metadata(
        &read,
        crate::sync::SYNC_AUTHORITY_STATE_SPACE,
        crate::sync::authority_state_key(),
        CoreProjection::KeyOnly,
    )
    .await?
    .is_some()
    {
        return Err(unknown());
    }
    let Some(ProjectedValue::FullValue(protocol)) = metadata(
        &read,
        crate::init::REPOSITORY_PROTOCOL_SPACE,
        Key(Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_KEY)),
        CoreProjection::FullValue,
    )
    .await?
    else {
        return Err(unknown());
    };
    let effective_protocol = if protocol.as_ref() == LEGACY_FENCE && interrupted_target.is_some() {
        match metadata(
            &read,
            REPOSITORY_EPOCH_SPACE,
            Key(Bytes::from_static(REPOSITORY_EPOCH_SOURCE_MARKER_KEY)),
            CoreProjection::FullValue,
        )
        .await?
        {
            Some(ProjectedValue::FullValue(marker)) => marker,
            _ => return Err(unknown()),
        }
    } else {
        protocol.clone()
    };
    let format = if format == 0 {
        let full_marker = effective_protocol
            .as_ref()
            .strip_suffix(b"-partial-replica.v1")
            .unwrap_or(effective_protocol.as_ref());
        match crate::init::parse_repository_protocol(full_marker) {
            crate::init::RepositoryProtocolStatus::Current => crate::init::CURRENT_FORMAT_VERSION,
            crate::init::RepositoryProtocolStatus::MigrationRequired { found_version } => {
                found_version
            }
            _ => return Err(unknown()),
        }
    } else {
        format
    };
    let partial = metadata(
        &read,
        crate::sync::PARTIAL_REPLICA_STATE_SPACE,
        crate::sync::partial_replica_state_key(),
        CoreProjection::FullValue,
    )
    .await?;
    let full = metadata(
        &read,
        crate::sync::SYNC_REPLICA_STATE_SPACE,
        crate::sync::replica_state_key(),
        CoreProjection::KeyOnly,
    )
    .await?
    .is_some();
    let (repository_id, partial_receipt, partial_account, partial_remote) = match partial {
        Some(ProjectedValue::FullValue(raw)) if !full && raw.len() <= 16 * 1024 => {
            #[derive(serde::Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct Identity {
                active_account_id: String,
                remote_id: String,
                descriptor: Repository,
            }
            #[derive(serde::Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct Repository {
                lix_id: String,
            }
            let identity: Identity = serde_json::from_slice(&raw).map_err(|_| unknown())?;
            if !effective_protocol.starts_with(b"tracked-default-branch.v")
                || !effective_protocol.ends_with(b"-partial-replica.v1")
            {
                return Err(unknown());
            }
            (
                identity.descriptor.lix_id,
                Some(raw),
                Some(identity.active_account_id),
                Some(identity.remote_id),
            )
        }
        None if full => {
            if !matches!(
                crate::init::parse_repository_protocol(&effective_protocol),
                crate::init::RepositoryProtocolStatus::Current
                    | crate::init::RepositoryProtocolStatus::MigrationRequired { .. }
            ) {
                return Err(unknown());
            }
            (
                crate::sync::replica_repository_identity(&read).await?,
                None,
                None,
                None,
            )
        }
        _ => return Err(unknown()),
    };
    if uuid::Uuid::parse_str(&repository_id).is_err() {
        return Err(unknown());
    }
    Ok(PartialReplacementSource {
        pointer,
        bank,
        interrupted_target,
        generation,
        format,
        protocol,
        repository_id,
        partial_receipt,
        partial_account,
        partial_remote,
    })
}

/// One atomic publication both installs the small opening receipt and changes
/// the epoch fence. Candidate collisions retain old banks and try another
/// physical namespace; each check is an indexed existence predicate, never a
/// scan or cleanup of the old cache. There are at most 4093 generation banks.
pub(crate) async fn install_replacement_partial_epoch<S>(
    storage: S,
    source: PartialReplacementSource,
    state: &crate::sync::PartialReplicaState,
) -> Result<PartialEpochAdmission<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if source.repository_id != state.repository_id()
        || source
            .partial_account
            .as_deref()
            .is_some_and(|account| account != state.active_account_id())
        || source
            .partial_remote
            .as_deref()
            .is_some_and(|remote| remote != state.remote_id())
    {
        return Err(LixError::new(
            "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
            "authenticated authority differs from the retained replica identity",
        ));
    }
    let pointer_guard = match &source.pointer {
        Some(expected) => Precondition::KeyValueEquals {
            space: REPOSITORY_EPOCH_SPACE,
            key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
            expected: expected.clone(),
        },
        None => Precondition::KeyAbsent {
            space: REPOSITORY_EPOCH_SPACE,
            key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
        },
    };
    let mut source_guards = vec![
        pointer_guard,
        Precondition::KeyAbsent {
            space: source
                .bank
                .map_space(crate::sync::SYNC_AUTHORITY_STATE_SPACE),
            key: crate::sync::authority_state_key(),
        },
        Precondition::KeyValueEquals {
            space: source
                .bank
                .map_space(crate::init::REPOSITORY_PROTOCOL_SPACE),
            key: Key(Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_KEY)),
            expected: source.protocol.clone(),
        },
    ];
    source_guards.push(match &source.partial_receipt {
        Some(expected) => Precondition::KeyValueEquals {
            space: source
                .bank
                .map_space(crate::sync::PARTIAL_REPLICA_STATE_SPACE),
            key: crate::sync::partial_replica_state_key(),
            expected: expected.clone(),
        },
        None => Precondition::KeyPresent {
            space: source.bank.map_space(crate::sync::SYNC_REPLICA_STATE_SPACE),
            key: crate::sync::replica_state_key(),
        },
    });
    for generation in source.generation.saturating_add(1)..=4093 {
        let bank = replica_generation_bank(generation)?;
        if bank == source.bank || Some(bank) == source.interrupted_target {
            continue;
        }
        let adapter = StorageAdapter::for_epoch_unfenced(storage.clone(), bank);
        let read = adapter.begin_read(Default::default()).await?;
        let mut bootstrap = StorageWriteSet::new();
        crate::init::stage_partial_repository_protocol(&mut bootstrap);
        // Stronger whole-bank RangeEmpty guards below subsume bootstrap's
        // individual absent-key guards, including journals and controls.
        crate::sync::stage_partial_bootstrap(&read, &mut bootstrap, state)?;
        drop(read);
        let mut writes = StorageWriteSet::new();
        let mut preconditions = source_guards.clone();
        preconditions.push(Precondition::KeyAbsent {
            space: REPOSITORY_EPOCH_SPACE,
            key: Key(Bytes::from(format!("retained/{}", bank_code(bank)))),
        });
        let mut copied = 0;
        for space in crate::storage_spaces::SNAPSHOT_STORAGE_SPACES
            .iter()
            .copied()
            .chain(
                crate::storage_spaces::RETIRED_STORAGE_SPACES
                    .iter()
                    .filter(|entry| !entry.emitted_in_lixsnap_v1)
                    .map(|entry| entry.space),
            )
        {
            for (key, value) in bootstrap.staged_values_in_space(space) {
                writes.put(bank.map_space(space), key.as_ref(), value.as_ref());
                copied += 1;
            }
            preconditions.push(Precondition::RangeEmpty {
                space: bank.map_space(space),
                range: KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
            });
        }
        if copied != bootstrap.stats().staged_puts || bootstrap.stats().staged_deletes != 0 {
            return Err(epoch_error(
                "partial bootstrap contains unregistered or non-put mutations",
            ));
        }
        let pointer = encode_pointer(PointerState::Active {
            bank,
            generation,
            format: crate::init::CURRENT_FORMAT_VERSION,
            publication: Some(uuid::Uuid::now_v7()),
        });
        writes.put(
            REPOSITORY_EPOCH_SPACE,
            REPOSITORY_EPOCH_KEY,
            pointer.as_ref(),
        );
        // This descriptor records retention, not the previous account identity:
        // no old full-replica payload is read or imported on this path.
        let retained = RetainedReplicaSource {
            bank: bank_code(source.bank),
            source_format: source.format,
            repository_id: source.repository_id.clone(),
            account_id: source.partial_account.clone().unwrap_or_default(),
            recovery_required: true,
        };
        writes.put(
            REPOSITORY_EPOCH_SPACE,
            format!("retained/{}", retained.bank).as_bytes(),
            serde_json::to_vec(&retained).map_err(|error| epoch_error(error.to_string()))?,
        );
        if let Some(target) = source.interrupted_target {
            let abandoned = RetainedReplicaSource {
                bank: bank_code(target),
                source_format: crate::init::CURRENT_FORMAT_VERSION,
                repository_id: source.repository_id.clone(),
                account_id: String::new(),
                recovery_required: true,
            };
            writes.put(
                REPOSITORY_EPOCH_SPACE,
                format!("retained/{}", abandoned.bank).as_bytes(),
                serde_json::to_vec(&abandoned).map_err(|error| epoch_error(error.to_string()))?,
            );
        }
        match writes
            .commit(
                &storage,
                WriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {
                let admitted = admit_partial_epoch(&storage).await;
                if partial::durable_pointer(&storage)
                    .await?
                    .map(|(_, bytes)| bytes)
                    != Some(pointer)
                {
                    return Err(LixError::new(
                        "LIX_PARTIAL_OPEN_RETRY",
                        "replica epoch changed after publication",
                    ));
                }
                return admitted;
            }
            Err(StorageWriteSetError::Storage(StorageError::PreconditionFailed(_))) => {
                // Distinguish an occupied candidate from a concurrent source
                // replacement. Do not silently overwrite a newly active epoch.
                let actual = partial::durable_pointer(&storage)
                    .await?
                    .map(|(_, bytes)| bytes);
                if actual != source.pointer {
                    return Err(LixError::new(
                        "LIX_PARTIAL_OPEN_RETRY",
                        "replica epoch changed during opening",
                    ));
                }
                // Exact source marker changes must not turn into a bank search.
                let verify = inspect_partial_replacement(&storage).await?;
                if verify.protocol != source.protocol
                    || verify.partial_receipt != source.partial_receipt
                    || verify.repository_id != source.repository_id
                {
                    return Err(LixError::new(
                        "LIX_PARTIAL_OPEN_RETRY",
                        "replica identity changed during opening",
                    ));
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(epoch_error(
        "retained replica generation capacity exhausted",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn opening_state() -> crate::sync::PartialReplicaState {
        let authority = crate::open_lix().await.unwrap();
        crate::sync::PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            authority.active_account_id().to_owned(),
            uuid::Uuid::now_v7().to_string(),
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn authoritative_partial_reopening_skips_populated_banks_and_fences_old_writer() {
        let state = opening_state().await;
        let memory = crate::Memory::new();
        let storage = crate::sync::durable_memory_for_test(memory.clone());
        let original = install_fresh_partial_epoch(storage.clone(), &state)
            .await
            .unwrap();
        crate::migration::downgrade_headers_for_test(&original.adapter, true).await;
        let old_pointer = encode_pointer(PointerState::Active {
            bank: EpochBank::Legacy,
            generation: 1,
            format: 79,
            publication: None,
        });
        let mut writes = StorageWriteSet::new();
        writes.put(
            REPOSITORY_EPOCH_SPACE,
            REPOSITORY_EPOCH_KEY,
            old_pointer.as_ref(),
        );
        let occupied = replica_generation_bank(2).unwrap();
        writes.put(
            occupied.map_space(crate::init::REPOSITORY_PROTOCOL_SPACE),
            crate::init::REPOSITORY_PROTOCOL_KEY,
            b"retained-unrelated-data".as_slice(),
        );
        writes
            .commit(
                &storage,
                WriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let stale = StorageAdapter::for_epoch(storage.clone(), EpochBank::Legacy, old_pointer);
        let source = inspect_partial_replacement(&storage).await.unwrap();
        let replacement = install_replacement_partial_epoch(storage.clone(), source, &state)
            .await
            .unwrap();
        assert_eq!(
            replacement.adapter.epoch_bank(),
            replica_generation_bank(3).unwrap()
        );
        assert_eq!(replacement.state, state);
        assert!(stale.begin_read(Default::default()).await.is_err());
        let retained = StorageAdapter::for_epoch_unfenced(storage.clone(), occupied);
        let read = retained.begin_read(Default::default()).await.unwrap();
        assert_eq!(
            metadata(
                &read,
                crate::init::REPOSITORY_PROTOCOL_SPACE,
                Key(Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_KEY)),
                CoreProjection::FullValue
            )
            .await
            .unwrap(),
            Some(ProjectedValue::FullValue(Bytes::from_static(
                b"retained-unrelated-data"
            )))
        );
        let sources = list_retained_replica_sources(&storage).await.unwrap();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].bank, bank_code(EpochBank::Legacy));
    }

    #[tokio::test]
    async fn partial_replacement_rejects_wrong_authority_and_concurrent_epoch_change() {
        let state = opening_state().await;
        let storage = crate::sync::durable_memory_for_test(crate::Memory::new());
        install_fresh_partial_epoch(storage.clone(), &state)
            .await
            .unwrap();
        let source = inspect_partial_replacement(&storage).await.unwrap();
        let wrong = opening_state().await;
        assert_eq!(
            install_replacement_partial_epoch(storage.clone(), source, &wrong)
                .await
                .err()
                .unwrap()
                .code,
            "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH"
        );
        assert_eq!(admit_partial_epoch(&storage).await.unwrap().state, state);
        let source = inspect_partial_replacement(&storage).await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            REPOSITORY_EPOCH_SPACE,
            REPOSITORY_EPOCH_KEY,
            encode_pointer(PointerState::Active {
                bank: EpochBank::Legacy,
                generation: 1,
                format: crate::init::CURRENT_FORMAT_VERSION,
                publication: Some(uuid::Uuid::now_v7()),
            })
            .as_ref(),
        );
        writes
            .commit(
                &storage,
                WriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(
            install_replacement_partial_epoch(storage.clone(), source, &state)
                .await
                .is_err()
        );
        assert_eq!(admit_partial_epoch(&storage).await.unwrap().state, state);
    }

    #[tokio::test]
    async fn full_replica_replacement_never_decodes_history_sized_receipt() {
        let memory = crate::Memory::new();
        let lix = crate::open_lix()
            .with_storage(memory.clone())
            .await
            .unwrap();
        let state = crate::sync::PartialReplicaState::new(
            format!("https://example.test/lix/{}", lix.lix_id()),
            lix.active_account_id().to_owned(),
            uuid::Uuid::now_v7().to_string(),
            lix.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap();
        let bank = lix.storage_adapter().epoch_bank();
        lix.close().await.unwrap();
        let storage = crate::storage_adapter::StorageSession::acquire(
            crate::sync::durable_memory_for_test(memory),
        )
        .await
        .unwrap();
        // Deliberately opaque payload: role is sufficient because absolutely
        // none of this account's old rows or pending work enter the new epoch.
        let mut writes = StorageWriteSet::new();
        writes.put(
            bank.map_space(crate::sync::SYNC_REPLICA_STATE_SPACE),
            crate::sync::replica_state_key(),
            vec![b'x'; 1024 * 1024],
        );
        writes.put(
            bank.map_space(crate::init::REPOSITORY_PROTOCOL_SPACE),
            crate::init::REPOSITORY_PROTOCOL_KEY,
            crate::init::REPOSITORY_PROTOCOL_V78,
        );
        writes.put(
            REPOSITORY_EPOCH_SPACE,
            REPOSITORY_EPOCH_KEY,
            encode_pointer(PointerState::Active {
                bank,
                generation: 1,
                format: 78,
                publication: None,
            })
            .as_ref(),
        );
        writes
            .commit(
                &storage,
                WriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let source = inspect_partial_replacement(&storage).await.unwrap();
        let replacement = install_replacement_partial_epoch(storage.clone(), source, &state)
            .await
            .unwrap();
        assert_eq!(replacement.state, state);
        assert_ne!(replacement.adapter.epoch_bank(), bank);
    }

    #[tokio::test]
    async fn interrupted_partial_migration_reopens_from_source_and_retains_target() {
        let state = opening_state().await;
        let storage = crate::sync::durable_memory_for_test(crate::Memory::new());
        let initial = install_fresh_partial_epoch(storage.clone(), &state)
            .await
            .unwrap();
        crate::migration::downgrade_headers_for_test(&initial.adapter, true).await;
        let mut writes = StorageWriteSet::new();
        writes.put(
            REPOSITORY_EPOCH_SPACE,
            REPOSITORY_EPOCH_KEY,
            encode_pointer(PointerState::Migrating {
                source: EpochBank::Legacy,
                source_format: 79,
                target: EpochBank::A,
                generation: 2,
                attempt: uuid::Uuid::now_v7(),
            })
            .as_ref(),
        );
        writes.put(
            EpochBank::A.map_space(crate::init::REPOSITORY_PROTOCOL_SPACE),
            crate::init::REPOSITORY_PROTOCOL_KEY,
            b"incomplete-migration".as_slice(),
        );
        writes
            .commit(
                &storage,
                WriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let source = inspect_partial_replacement(&storage).await.unwrap();
        assert_eq!(source.format, 79);
        let replacement = install_replacement_partial_epoch(storage.clone(), source, &state)
            .await
            .unwrap();
        assert_eq!(replacement.state, state);
        assert_eq!(
            list_retained_replica_sources(&storage).await.unwrap().len(),
            2
        );
    }

    #[tokio::test]
    async fn authority_and_local_only_repositories_are_never_replaced() {
        let memory = crate::Memory::new();
        let lix = crate::open_lix()
            .with_storage(memory.clone())
            .await
            .unwrap();
        let bank = lix.storage_adapter().epoch_bank();
        lix.close().await.unwrap();
        let storage = crate::storage_adapter::StorageSession::acquire(
            crate::sync::durable_memory_for_test(memory),
        )
        .await
        .unwrap();
        assert!(inspect_partial_replacement(&storage).await.is_err());
        let mut writes = StorageWriteSet::new();
        writes.put(
            bank.map_space(crate::sync::SYNC_AUTHORITY_STATE_SPACE),
            crate::sync::authority_state_key(),
            crate::sync::AUTHORITY_STATE_VALUE,
        );
        writes.put(
            bank.map_space(crate::sync::SYNC_REPLICA_STATE_SPACE),
            crate::sync::replica_state_key(),
            b"replica-marker".as_slice(),
        );
        writes
            .commit(
                &storage,
                WriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(inspect_partial_replacement(&storage).await.is_err());
    }
}
