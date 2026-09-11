//! Hydrate an exact native frontier using bounded transport batches. No read
//! transaction survives a network await, and no coverage is inferred here.
use super::super::native_object::{NativeObjectResponse, validate_request, validate_response};
use super::*;
use crate::storage_adapter::{StorageAdapterRead, StorageGetManyRequest, StorageValue};

async fn resident<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    expected: &PartialReplicaState,
    addresses: &[NativeObjectRef],
) -> Result<(Vec<bool>, Bytes), LixError> {
    let read = storage.begin_read(Default::default()).await?;
    let (actual, raw) = load_partial_replica_state(&read)
        .await?
        .ok_or_else(stale_admission)?;
    if !same_admission(&actual, expected) {
        return Err(stale_admission());
    }
    let keys = addresses
        .iter()
        .map(|a| [StorageKey(Bytes::from(a.storage_key()))])
        .collect::<Vec<_>>();
    let requests = addresses
        .iter()
        .zip(&keys)
        .map(|(a, keys)| StorageGetManyRequest {
            space: a.space(),
            keys,
            opts: crate::storage_adapter::StorageGetOptions {
                projection: crate::storage_adapter::StorageCoreProjection::KeyOnly,
            },
        })
        .collect::<Vec<_>>();
    let values = read.get_many(&requests).await?.values;
    if values.len() != addresses.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native batch residency returned incorrect cardinality",
        ));
    }
    let mut present = Vec::with_capacity(addresses.len());
    for ((value, address), keys) in values.into_iter().zip(addresses).zip(&keys) {
        if value.is_none() {
            present.push(false);
            continue;
        }
        // Range-hydrated objects can each be large. Validate resident payloads
        // one at a time rather than materializing 32 full objects together.
        let mut loaded = read
            .get_many(&[StorageGetManyRequest {
                space: address.space(),
                keys,
                opts: Default::default(),
            }])
            .await?
            .values;
        if loaded.len() != 1 {
            return Err(LixError::unknown(
                "native resident validation returned incorrect cardinality",
            ));
        }
        present.push(is_resident(loaded.pop().flatten(), *address)?);
    }
    Ok((present, raw))
}

#[cfg(test)]
mod tests {
    use super::super::tests::{fixture, response};
    use super::*;
    use crate::sync::native_object::{NativeObject, NativeObjectResponse};

    fn object(bytes: Vec<u8>) -> NativeObject {
        NativeObject {
            address: NativeObjectRef::TrackedStateTreeChunk(*blake3::hash(&bytes).as_bytes()),
            bytes,
        }
    }
    fn no_range(
        _: NativeObjectRangeRequest,
    ) -> std::future::Ready<Result<NativeObjectRangeResponse, LixError>> {
        panic!("small native batch must not use range requests")
    }

    #[tokio::test]
    async fn batch_rejects_epoch_change_before_installing_any_member() {
        let (storage, state) = fixture().await;
        let objects = vec![object(vec![1; 16]), object(vec![2; 16])];
        let addresses = objects.iter().map(|o| o.address).collect::<Vec<_>>();
        let replacement = PartialReplicaState::new(
            state.remote_id().into(),
            state.active_account_id().into(),
            "00000000-0000-7000-8000-000000000499".into(),
            state.descriptor().clone(),
        )
        .unwrap();
        let error = hydrate_native_objects(
            &storage,
            &state,
            &addresses,
            1024,
            |_| {
                let storage = storage.clone();
                let replacement = replacement.clone();
                let answer = NativeObjectResponse {
                    lix_id: state.repository_id().into(),
                    objects: objects.clone(),
                };
                async move {
                    let read = storage.begin_read(Default::default()).await?;
                    let raw = load_partial_replica_state(&read).await?.unwrap().1;
                    let mut writes = storage.new_write_set();
                    let condition = crate::sync::partial_state::stage_partial_replica_state(
                        &mut writes,
                        &replacement,
                        Some(raw),
                    )?;
                    drop(read);
                    storage
                        .commit_partial_replica_write_set(
                            crate::sync::partial_replica_write_capability(),
                            writes,
                            StorageWriteOptions {
                                preconditions: vec![condition],
                                ..Default::default()
                            },
                        )
                        .await?;
                    Ok(answer)
                }
            },
            no_range,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH");
        for address in addresses {
            assert!(
                !native_object_is_resident(&storage, &replacement, address)
                    .await
                    .unwrap()
            );
        }
    }

    #[tokio::test]
    async fn exact_batch_skips_resident_inputs_and_warm_queries_do_not_fetch() {
        let (storage, state) = fixture().await;
        let objects = vec![
            object(vec![1; 16]),
            object(vec![2; 16]),
            object(vec![3; 16]),
        ];
        hydrate_native_object(&storage, &state, objects[0].address, 1024, |r| {
            std::future::ready(Ok(response(&state, &r, &objects[0].bytes)))
        })
        .await
        .unwrap();
        let addresses = objects.iter().map(|o| o.address).collect::<Vec<_>>();
        let report = hydrate_native_objects(
            &storage,
            &state,
            &addresses,
            1024,
            |requested| {
                assert_eq!(requested, addresses[1..]);
                std::future::ready(Ok(NativeObjectResponse {
                    lix_id: state.repository_id().into(),
                    objects: objects[1..].to_vec(),
                }))
            },
            no_range,
        )
        .await
        .unwrap();
        assert_eq!(
            report,
            NativeHydrationReport {
                requests: 1,
                payload_bytes: 32
            }
        );
        let warm = hydrate_native_objects(
            &storage,
            &state,
            &addresses,
            1024,
            |_| std::future::ready(Err(LixError::unknown("warm batch fetched"))),
            no_range,
        )
        .await
        .unwrap();
        assert_eq!(warm, NativeHydrationReport::default());
    }

    #[tokio::test]
    async fn ordinary_batch_enforces_caller_object_budget_before_installing_any_member() {
        let (storage, state) = fixture().await;
        let objects = vec![object(vec![1; 16]), object(vec![2; 17])];
        let addresses = objects
            .iter()
            .map(|object| object.address)
            .collect::<Vec<_>>();
        let error = hydrate_native_objects(
            &storage,
            &state,
            &addresses,
            16,
            |_| {
                std::future::ready(Ok(NativeObjectResponse {
                    lix_id: state.repository_id().into(),
                    objects: objects.clone(),
                }))
            },
            no_range,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("caller assembly budget"));
        for address in addresses {
            assert!(
                !native_object_is_resident(&storage, &state, address)
                    .await
                    .unwrap()
            );
        }
    }

    #[tokio::test]
    async fn corrupt_batch_member_installs_no_valid_prefix() {
        let (storage, state) = fixture().await;
        let mut objects = vec![object(vec![1; 16]), object(vec![2; 16])];
        let addresses = objects.iter().map(|o| o.address).collect::<Vec<_>>();
        objects[1].bytes[0] ^= 1;
        assert!(
            hydrate_native_objects(
                &storage,
                &state,
                &addresses,
                1024,
                |_| std::future::ready(Ok(NativeObjectResponse {
                    lix_id: state.repository_id().into(),
                    objects: objects.clone()
                })),
                no_range
            )
            .await
            .is_err()
        );
        for address in addresses {
            assert!(
                !native_object_is_resident(&storage, &state, address)
                    .await
                    .unwrap()
            );
        }
    }

    #[tokio::test]
    async fn batch_splits_only_explicit_payload_limit_and_ranges_large_singletons() {
        let (storage, state) = fixture().await;
        let objects = vec![
            object(vec![1; 600 * 1024]),
            object(vec![2; 600 * 1024]),
            object(vec![3; MAX_NATIVE_OBJECT_PAYLOAD_BYTES + 17]),
        ];
        let addresses = objects.iter().map(|o| o.address).collect::<Vec<_>>();
        let report = hydrate_native_objects(
            &storage,
            &state,
            &addresses,
            2 * MAX_NATIVE_OBJECT_PAYLOAD_BYTES,
            |requested| {
                let selected = objects
                    .iter()
                    .filter(|o| requested.contains(&o.address))
                    .cloned()
                    .collect::<Vec<_>>();
                std::future::ready(
                    if selected.iter().map(|o| o.bytes.len()).sum::<usize>()
                        > MAX_NATIVE_OBJECT_PAYLOAD_BYTES
                    {
                        Err(LixError::new("LIX_NATIVE_OBJECT_BATCH_TOO_LARGE", "split"))
                    } else {
                        Ok(NativeObjectResponse {
                            lix_id: state.repository_id().into(),
                            objects: selected,
                        })
                    },
                )
            },
            |range| {
                let object = objects.iter().find(|o| o.address == range.address).unwrap();
                std::future::ready(Ok(response(&state, &range, &object.bytes)))
            },
        )
        .await
        .unwrap();
        assert_eq!(report.requests, 7);
        assert_eq!(
            report.payload_bytes,
            objects.iter().map(|o| o.bytes.len()).sum::<usize>()
        );
        for address in addresses {
            assert!(
                native_object_is_resident(&storage, &state, address)
                    .await
                    .unwrap()
            );
        }
    }

    #[tokio::test]
    async fn batch_accepts_identical_concurrent_subset_without_refetching() {
        let (storage, state) = fixture().await;
        let objects = vec![object(vec![1; 16]), object(vec![2; 16])];
        let addresses = objects.iter().map(|o| o.address).collect::<Vec<_>>();
        let report = hydrate_native_objects(
            &storage,
            &state,
            &addresses,
            1024,
            |_| {
                let storage = &storage;
                let state = &state;
                let objects = &objects;
                async move {
                    hydrate_native_object(storage, state, objects[0].address, 1024, |r| {
                        std::future::ready(Ok(response(state, &r, &objects[0].bytes)))
                    })
                    .await?;
                    Ok(NativeObjectResponse {
                        lix_id: state.repository_id().into(),
                        objects: objects.clone(),
                    })
                }
            },
            no_range,
        )
        .await
        .unwrap();
        assert_eq!(report.requests, 1);
        for address in addresses {
            assert!(
                native_object_is_resident(&storage, &state, address)
                    .await
                    .unwrap()
            );
        }
    }
}

pub(in crate::sync) async fn hydrate_native_objects<S, Fetch, F, Range, RF>(
    storage: &StorageAdapter<S>,
    expected: &PartialReplicaState,
    addresses: &[NativeObjectRef],
    max_object_bytes: usize,
    mut fetch: Fetch,
    mut fetch_range: Range,
) -> Result<NativeHydrationReport, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    Fetch: FnMut(Vec<NativeObjectRef>) -> F,
    F: Future<Output = Result<NativeObjectResponse, LixError>>,
    Range: FnMut(NativeObjectRangeRequest) -> RF,
    RF: Future<Output = Result<NativeObjectRangeResponse, LixError>>,
{
    validate_request(addresses)?;
    let (present, _) = resident(storage, expected, addresses).await?;
    let missing = addresses
        .iter()
        .zip(present)
        .filter_map(|(a, p)| (!p).then_some(*a))
        .collect::<Vec<_>>();
    let mut report = NativeHydrationReport::default();
    if missing.is_empty() {
        return Ok(report);
    }
    let mut pending = vec![missing.clone()];
    // At most 32 independently bounded objects. Normally the whole frontier
    // fits one 1 MiB response. Oversized responses split without weakening
    // response validation; oversized single objects retain range assembly.
    while let Some(request) = pending.pop() {
        report.requests += 1;
        match fetch(request.clone()).await {
            Ok(response) => {
                validate_response(expected.repository_id(), &request, &response)?;
                if response
                    .objects
                    .iter()
                    .any(|object| object.bytes.len() > max_object_bytes)
                {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "native object exceeds caller assembly budget",
                    ));
                }
                report.payload_bytes += response
                    .objects
                    .iter()
                    .map(|o| o.bytes.len())
                    .sum::<usize>();
                let mut staged = storage.new_write_set();
                super::super::native_object::stage_native_objects(
                    expected.repository_id(),
                    &request,
                    &response,
                    &mut staged,
                )?;
                install_validated(storage, expected, &request, &staged).await?;
            }
            Err(error) if error.code == "LIX_NATIVE_OBJECT_BATCH_TOO_LARGE" => {
                if request.len() > 1 {
                    let mid = request.len() / 2;
                    pending.push(request[mid..].to_vec());
                    pending.push(request[..mid].to_vec());
                    continue;
                }
                let address = request[0];
                let mut assembler = NativeObjectAssembler::new(
                    expected.repository_id().to_owned(),
                    address,
                    max_object_bytes,
                )?;
                let mut offset = 0usize;
                loop {
                    let range = NativeObjectRangeRequest {
                        address,
                        offset: offset as u64,
                        max_bytes: MAX_NATIVE_OBJECT_PAYLOAD_BYTES as u32,
                    };
                    let response = fetch_range(range.clone()).await?;
                    report.requests += 1;
                    report.payload_bytes += response.bytes.len();
                    offset += response.bytes.len();
                    if let Some(complete) = assembler.accept(&range, &response)? {
                        let mut staged = storage.new_write_set();
                        complete.stage_into(&mut staged);
                        install_validated(storage, expected, &[address], &staged).await?;
                        break;
                    }
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(report)
}

async fn install_validated<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    expected: &PartialReplicaState,
    missing: &[NativeObjectRef],
    staged: &crate::storage_adapter::StorageWriteSet,
) -> Result<(), LixError> {
    // Each bounded response is validated in full before installation. Earlier
    // valid immutable groups can remain cached if a later group fails; no
    // logical coverage or serving state is advanced by any group.
    // A concurrent installer may supply a subset of this frontier. Retry only
    // local admission, at most once per address plus one receipt-only race.
    // All bytes in this group were validated before its first durable write.
    for _ in 0..=missing.len() + 1 {
        let (present, raw) = resident(storage, expected, &missing).await?;
        if present.iter().all(|p| *p) {
            return Ok(());
        }
        let mut writes = storage.new_write_set();
        let mut preconditions = vec![StoragePrecondition::KeyValueEquals {
            space: PARTIAL_REPLICA_STATE_SPACE,
            key: partial_replica_state_key(),
            expected: raw,
        }];
        for (address, present) in missing.iter().zip(present) {
            if present {
                continue;
            }
            let key = StorageKey(Bytes::from(address.storage_key()));
            let bytes = staged
                .staged_value(address.space(), &key.0)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "validated native batch omitted an object",
                    )
                })?;
            writes.put_content_addressed_batch(
                address.space(),
                [(key.clone(), StorageValue { bytes })],
            );
            preconditions.push(StoragePrecondition::KeyAbsent {
                space: address.space(),
                key,
            });
        }
        match storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(crate::storage_adapter::StorageWriteSetError::Storage(
                crate::storage_adapter::StorageError::PreconditionFailed(_),
            )) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(LixError::new(
        LixError::CODE_TRANSACTION_CONFLICT,
        "native batch admission kept changing during local installation",
    ))
}
