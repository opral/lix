//! Native object admission into an existing partial replica epoch.
//!
//! Network I/O never holds a local storage read or transaction. A completed
//! object is hash checked before an atomic epoch-fenced install. No branch,
//! coverage or completeness claim is changed by loading these bytes.

use std::future::Future;

use bytes::Bytes;

use crate::LixError;
use crate::storage_adapter::{
    PointReadPlan, Storage, StorageAdapter, StorageKey, StoragePrecondition, StorageProjectedValue,
    StorageWriteOptions,
};
use crate::tracked_state::NativeObjectRef;

use super::native_object::MAX_NATIVE_OBJECT_PAYLOAD_BYTES;
use super::native_object_range::{
    NativeObjectAssembler, NativeObjectRangeRequest, NativeObjectRangeResponse,
};
use super::partial_state::{
    PARTIAL_REPLICA_STATE_SPACE, PartialReplicaState, load_partial_replica_state,
    partial_replica_state_key,
};

mod batch;
pub(super) use batch::hydrate_native_objects;

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct NativeHydrationReport {
    pub(super) requests: usize,
    pub(super) payload_bytes: usize,
}

fn same_admission(actual: &PartialReplicaState, expected: &PartialReplicaState) -> bool {
    actual.repository_id() == expected.repository_id()
        && actual.remote_id() == expected.remote_id()
        && actual.active_account_id() == expected.active_account_id()
        && actual.epoch_id() == expected.epoch_id()
}

fn stale_admission() -> LixError {
    LixError::new(
        "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
        "native hydration belongs to a different partial replica admission",
    )
}

fn is_resident(
    value: Option<StorageProjectedValue>,
    address: NativeObjectRef,
) -> Result<bool, LixError> {
    match value {
        None => Ok(false),
        Some(StorageProjectedValue::FullValue(bytes)) => {
            address.validate(&bytes)?;
            Ok(true)
        }
        Some(_) => Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native cache read omitted an existing payload",
        )),
    }
}

pub(super) async fn native_object_is_resident<S>(
    storage: &StorageAdapter<S>,
    expected: &PartialReplicaState,
    address: NativeObjectRef,
) -> Result<bool, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = storage.begin_read(Default::default()).await?;
    let actual = load_partial_replica_state(&read)
        .await?
        .ok_or_else(stale_admission)?
        .0;
    if !same_admission(&actual, expected) {
        return Err(stale_admission());
    }
    let value = PointReadPlan::new(
        address.space(),
        &[StorageKey(Bytes::from(address.storage_key()))],
    )
    .materialize(&read, Default::default())
    .await?
    .value
    .pop()
    .flatten();
    is_resident(value, address)
}

/// The caller binds `fetch` to the same authenticated remote/account as
/// `expected`. A local miss alone is never sufficient authority to fetch.
/// The explicit object budget limits assembly; each response has a fixed cap.
pub(super) async fn hydrate_native_object<S, Fetch, F>(
    storage: &StorageAdapter<S>,
    expected: &PartialReplicaState,
    address: NativeObjectRef,
    max_object_bytes: usize,
    mut fetch: Fetch,
) -> Result<NativeHydrationReport, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    Fetch: FnMut(NativeObjectRangeRequest) -> F,
    F: Future<Output = Result<NativeObjectRangeResponse, LixError>>,
{
    if native_object_is_resident(storage, expected, address).await? {
        return Ok(NativeHydrationReport::default());
    }
    let mut assembler = NativeObjectAssembler::new(
        expected.repository_id().to_owned(),
        address,
        max_object_bytes,
    )?;
    let mut report = NativeHydrationReport::default();
    let completed = loop {
        let request = NativeObjectRangeRequest {
            address,
            offset: report.payload_bytes as u64,
            max_bytes: MAX_NATIVE_OBJECT_PAYLOAD_BYTES as u32,
        };
        let response = fetch(request.clone()).await?;
        let complete = assembler.accept(&request, &response)?;
        report.requests += 1;
        report.payload_bytes += response.bytes.len();
        if let Some(complete) = complete {
            break complete;
        }
    };
    let read = storage.begin_read(Default::default()).await?;
    let (actual, raw) = load_partial_replica_state(&read)
        .await?
        .ok_or_else(stale_admission)?;
    if !same_admission(&actual, expected) {
        return Err(stale_admission());
    }
    let key = StorageKey(Bytes::from(address.storage_key()));
    let existing = PointReadPlan::new(address.space(), std::slice::from_ref(&key))
        .materialize(&read, Default::default())
        .await?
        .value
        .pop()
        .flatten();
    if is_resident(existing, address)? {
        return Ok(report);
    }
    let mut writes = storage.new_write_set();
    completed.stage_into(&mut writes);
    let availability =
        super::native_object::validated_native_dependency_availability_from_staged(
            &[address],
            &writes,
            raw.clone(),
        )?;
    drop(read);
    let installed = storage
        .commit_partial_native_dependency_availability_write_set(
            super::partial_replica_write_capability(),
            availability,
            writes,
            StorageWriteOptions {
                preconditions: vec![
                    StoragePrecondition::KeyAbsent {
                        space: address.space(),
                        key,
                    },
                    StoragePrecondition::KeyValueEquals {
                        space: PARTIAL_REPLICA_STATE_SPACE,
                        key: partial_replica_state_key(),
                        expected: raw,
                    },
                ],
                await_durable: true,
                ..Default::default()
            },
        )
        .await;
    if let Err(error) = installed {
        if matches!(
            &error,
            crate::storage_adapter::StorageWriteSetError::Storage(
                crate::storage_adapter::StorageError::PreconditionFailed(_)
            )
        ) {
            // An identical concurrent fetch may win installation. Recheck once
            // locally; never retry the network or swallow unrelated I/O errors.
            let read = storage.begin_read(Default::default()).await?;
            let actual = load_partial_replica_state(&read)
                .await?
                .ok_or_else(stale_admission)?
                .0;
            if !same_admission(&actual, expected) {
                return Err(stale_admission());
            }
            let value = PointReadPlan::new(
                address.space(),
                &[StorageKey(Bytes::from(address.storage_key()))],
            )
            .materialize(&read, Default::default())
            .await?
            .value
            .pop()
            .flatten();
            if is_resident(value, address)? {
                return Ok(report);
            }
        }
        return Err(error.into());
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::super::partial_bootstrap::stage_partial_bootstrap;
    use super::super::partial_state::stage_partial_replica_state;
    use super::*;
    use crate::sync::native_object::NativeObjectResponse;
    use crate::{Memory, open_lix};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{Notify, watch};

    #[derive(Clone)]
    struct SignalChangeWatchStorage {
        inner: Memory,
        changes: watch::Sender<u64>,
        wait_count: Arc<AtomicUsize>,
        wait_entered: Arc<Notify>,
    }

    impl SignalChangeWatchStorage {
        fn signal_physical_change(&self) {
            self.changes.send_modify(|generation| *generation += 1);
        }

        async fn wait_for_watch_wait(&self, expected: usize) {
            loop {
                let notified = self.wait_entered.notified();
                if self.wait_count.load(Ordering::Acquire) >= expected {
                    return;
                }
                notified.await;
            }
        }
    }

    struct SignalChangeSource {
        changes: watch::Receiver<u64>,
        wait_count: Arc<AtomicUsize>,
        wait_entered: Arc<Notify>,
    }

    impl crate::storage::StorageChangeSource for SignalChangeSource {
        fn changed(
            &mut self,
        ) -> Pin<Box<dyn Future<Output = Result<(), crate::storage::StorageError>> + Send + '_>>
        {
            let wait_count = Arc::clone(&self.wait_count);
            let wait_entered = Arc::clone(&self.wait_entered);
            Box::pin(async move {
                wait_count.fetch_add(1, Ordering::AcqRel);
                wait_entered.notify_one();
                self.changes
                    .changed()
                    .await
                    .map_err(|_| crate::storage::StorageError::Closed("watch closed".into()))?;
                Ok(())
            })
        }
    }

    impl Storage for SignalChangeWatchStorage {
        type Read<'a>
            = crate::storage::MemoryRead
        where
            Self: 'a;
        type Write<'a>
            = crate::storage::MemoryWrite
        where
            Self: 'a;

        async fn acquire_session(
            &self,
        ) -> Result<crate::storage::StorageSessionToken, crate::storage::StorageError> {
            self.inner.acquire_session().await
        }

        async fn begin_read(
            &self,
            options: crate::storage::ReadOptions,
        ) -> Result<Self::Read<'_>, crate::storage::StorageError> {
            self.inner.begin_read(options).await
        }

        async fn begin_write(
            &self,
            options: crate::storage::WriteOptions,
        ) -> Result<Self::Write<'_>, crate::storage::StorageError> {
            self.inner.begin_write(options).await
        }

        async fn watch_for_changes(
            &self,
        ) -> Result<crate::storage::StorageChangeWatch, crate::storage::StorageError> {
            Ok(crate::storage::StorageChangeWatch::from_source(
                SignalChangeSource {
                    changes: self.changes.subscribe(),
                    wait_count: Arc::clone(&self.wait_count),
                    wait_entered: Arc::clone(&self.wait_entered),
                },
            ))
        }
    }

    pub(super) async fn fixture() -> (StorageAdapter<Memory>, PartialReplicaState) {
        let authority = open_lix().await.unwrap();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            authority.active_account_id().into(),
            "00000000-0000-7000-8000-000000000399".into(),
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap();
        let storage = StorageAdapter::new(Memory::new());
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let preconditions = stage_partial_bootstrap(&read, &mut writes, &state).unwrap();
        crate::init::stage_partial_repository_protocol(&mut writes);
        drop(read);
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        (storage, state)
    }

    pub(super) fn response(
        state: &PartialReplicaState,
        request: &NativeObjectRangeRequest,
        bytes: &[u8],
    ) -> NativeObjectRangeResponse {
        let start = request.offset as usize;
        let end = (start + request.max_bytes as usize).min(bytes.len());
        NativeObjectRangeResponse {
            lix_id: state.repository_id().into(),
            address: request.address,
            offset: request.offset,
            total_bytes: bytes.len() as u64,
            bytes: bytes[start..end].to_vec(),
        }
    }

    #[tokio::test]
    async fn partial_native_hydration_is_durable_and_warm_calls_make_no_requests() {
        let (storage, state) = fixture().await;
        let before_mutation = storage.load_mutation_revision().await.unwrap();
        let before_observable = storage.load_observable_revision().await.unwrap();
        let bytes = vec![42; MAX_NATIVE_OBJECT_PAYLOAD_BYTES + 17];
        let address = NativeObjectRef::TrackedStateTreeChunk(*blake3::hash(&bytes).as_bytes());
        let report = hydrate_native_object(&storage, &state, address, bytes.len(), |request| {
            std::future::ready(Ok(response(&state, &request, &bytes)))
        })
        .await
        .unwrap();
        assert_eq!(
            report,
            NativeHydrationReport {
                requests: 2,
                payload_bytes: bytes.len()
            }
        );
        let warm = hydrate_native_object(&storage, &state, address, bytes.len(), |_| {
            panic!("resident data must not request a network round trip");
            #[allow(unreachable_code)]
            std::future::ready(Err(LixError::unknown("unexpected fetch")))
        })
        .await
        .unwrap();
        assert_eq!(warm, NativeHydrationReport::default());
        assert_ne!(
            storage.load_mutation_revision().await.unwrap(),
            before_mutation,
            "physical hydration still advances the mutation revision"
        );
        assert_eq!(
            storage.load_observable_revision().await.unwrap(),
            before_observable,
            "validated dependency availability does not notify repository observers"
        );

        let visible_bytes = b"ordinary visible native write";
        let visible_address =
            NativeObjectRef::TrackedStateTreeChunk(*blake3::hash(visible_bytes).as_bytes());
        let read = storage.begin_read(Default::default()).await.unwrap();
        let raw = load_partial_replica_state(&read).await.unwrap().unwrap().1;
        drop(read);
        let mut visible = storage.new_write_set();
        visible.put_content_addressed_batch(
            visible_address.space(),
            [(
                StorageKey(Bytes::from(visible_address.storage_key())),
                crate::storage_adapter::StorageValue {
                    bytes: Bytes::from_static(visible_bytes),
                },
            )],
        );
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                visible,
                StorageWriteOptions {
                    preconditions: vec![
                        StoragePrecondition::KeyAbsent {
                            space: visible_address.space(),
                            key: StorageKey(Bytes::from(visible_address.storage_key())),
                        },
                        StoragePrecondition::KeyValueEquals {
                            space: PARTIAL_REPLICA_STATE_SPACE,
                            key: partial_replica_state_key(),
                            expected: raw,
                        },
                    ],
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_ne!(
            storage.load_observable_revision().await.unwrap(),
            before_observable,
            "ordinary visible writes still advance the observer revision"
        );
        let mut unauthorized = storage.new_write_set();
        crate::init::stage_repository_protocol(&mut unauthorized);
        assert!(
            storage
                .commit_write_set(unauthorized, Default::default())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn dependency_visibility_intent_rejects_mixed_public_mutations() {
        let (storage, state) = fixture().await;
        let native_bytes = b"validated native dependency";
        let native_address =
            NativeObjectRef::TrackedStateTreeChunk(*blake3::hash(native_bytes).as_bytes());
        let response = NativeObjectResponse {
            lix_id: state.repository_id().into(),
            objects: vec![super::super::native_object::NativeObject {
                address: native_address,
                bytes: native_bytes.to_vec(),
            }],
        };
        let read = storage.begin_read(Default::default()).await.unwrap();
        let raw = load_partial_replica_state(&read).await.unwrap().unwrap().1;
        drop(read);
        let public_bytes = b"unrelated public mutation";
        let public_address =
            NativeObjectRef::TrackedStateTreeChunk(*blake3::hash(public_bytes).as_bytes());
        let mut writes = storage.new_write_set();
        super::super::native_object::stage_native_objects(
            state.repository_id(),
            &[native_address],
            &response,
            &mut writes,
        )
        .unwrap();
        let availability =
            crate::sync::native_object::validated_native_dependency_availability_from_staged(
                &[native_address],
                &writes,
                raw.clone(),
            )
            .unwrap();
        writes.put_content_addressed_batch(
            public_address.space(),
            [(
                StorageKey(Bytes::from(public_address.storage_key())),
                crate::storage_adapter::StorageValue {
                    bytes: Bytes::from_static(public_bytes),
                },
            )],
        );
        let before_observable = storage.load_observable_revision().await.unwrap();
        let error = storage
            .commit_partial_native_dependency_availability_write_set(
                super::super::partial_replica_write_capability(),
                availability,
                writes,
                StorageWriteOptions {
                    preconditions: vec![
                        StoragePrecondition::KeyAbsent {
                            space: native_address.space(),
                            key: StorageKey(Bytes::from(native_address.storage_key())),
                        },
                        StoragePrecondition::KeyValueEquals {
                            space: PARTIAL_REPLICA_STATE_SPACE,
                            key: partial_replica_state_key(),
                            expected: raw,
                        },
                    ],
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            crate::storage_adapter::StorageWriteSetError::Admission(_)
        ));
        assert_eq!(
            storage.load_observable_revision().await.unwrap(),
            before_observable
        );
        assert!(
            !native_object_is_resident(&storage, &state, native_address)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn observer_ignores_hydration_wake_but_sees_later_visible_write() {
        let (base, state) = fixture().await;
        let (changes, _) = watch::channel(0_u64);
        let watched = SignalChangeWatchStorage {
            inner: base.storage().clone(),
            changes,
            wait_count: Arc::new(AtomicUsize::new(0)),
            wait_entered: Arc::new(Notify::new()),
        };
        let storage = StorageAdapter::new(watched.clone());
        let invalidation = Arc::new(crate::observe_invalidation::ObserveInvalidation::new());
        let mut observer = invalidation.subscribe();
        invalidation
            .ensure_external_watcher(storage.clone())
            .await
            .unwrap();
        watched.wait_for_watch_wait(1).await;
        observer.borrow_and_update();

        let bytes = b"unrelated same-epoch native object";
        let address = NativeObjectRef::TrackedStateTreeChunk(*blake3::hash(bytes).as_bytes());
        hydrate_native_object(&storage, &state, address, bytes.len(), |request| {
            std::future::ready(Ok(response(&state, &request, bytes)))
        })
        .await
        .unwrap();
        watched.signal_physical_change();
        // The watcher returns to its next physical wait only after comparing
        // the shared observable revision and deciding this hydration was private.
        watched.wait_for_watch_wait(2).await;
        assert_eq!(invalidation.generation(), 0);
        assert!(!observer.has_changed().unwrap());

        let visible_bytes = b"later visible native object";
        let visible_address =
            NativeObjectRef::TrackedStateTreeChunk(*blake3::hash(visible_bytes).as_bytes());
        let read = storage.begin_read(Default::default()).await.unwrap();
        let raw = load_partial_replica_state(&read).await.unwrap().unwrap().1;
        drop(read);
        let mut visible = storage.new_write_set();
        visible.put_content_addressed_batch(
            visible_address.space(),
            [(
                StorageKey(Bytes::from(visible_address.storage_key())),
                crate::storage_adapter::StorageValue {
                    bytes: Bytes::from_static(visible_bytes),
                },
            )],
        );
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                visible,
                StorageWriteOptions {
                    preconditions: vec![
                        StoragePrecondition::KeyAbsent {
                            space: visible_address.space(),
                            key: StorageKey(Bytes::from(visible_address.storage_key())),
                        },
                        StoragePrecondition::KeyValueEquals {
                            space: PARTIAL_REPLICA_STATE_SPACE,
                            key: partial_replica_state_key(),
                            expected: raw,
                        },
                    ],
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        watched.signal_physical_change();
        watched.wait_for_watch_wait(3).await;
        assert_eq!(invalidation.generation(), 1);
        assert!(observer.has_changed().unwrap());
        assert!(matches!(
            observer.borrow_and_update().clone(),
            crate::observe_invalidation::ObserveInvalidationEvent::Generation(1)
        ));
    }

    #[tokio::test]
    async fn partial_native_hydration_rejects_epoch_change_during_fetch() {
        let (storage, state) = fixture().await;
        let bytes = b"native object";
        let address = NativeObjectRef::TrackedStateTreeChunk(*blake3::hash(bytes).as_bytes());
        let replacement = PartialReplicaState::new(
            state.remote_id().into(),
            state.active_account_id().into(),
            "00000000-0000-7000-8000-000000000499".into(),
            state.descriptor().clone(),
        )
        .unwrap();
        let result = hydrate_native_object(&storage, &state, address, bytes.len(), |request| {
            let storage = storage.clone();
            let replacement = replacement.clone();
            let answer = response(&state, &request, bytes);
            async move {
                let read = storage.begin_read(Default::default()).await?;
                let raw = load_partial_replica_state(&read).await?.unwrap().1;
                let mut writes = storage.new_write_set();
                let condition = stage_partial_replica_state(&mut writes, &replacement, Some(raw))?;
                drop(read);
                storage
                    .commit_partial_replica_write_set(
                        super::super::partial_replica_write_capability(),
                        writes,
                        StorageWriteOptions {
                            preconditions: vec![condition],
                            ..Default::default()
                        },
                    )
                    .await?;
                Ok(answer)
            }
        })
        .await;
        assert_eq!(
            result.unwrap_err().code,
            "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH"
        );
        let read = storage.begin_read(Default::default()).await.unwrap();
        assert!(
            PointReadPlan::new(
                address.space(),
                &[StorageKey(Bytes::from(address.storage_key()))]
            )
            .materialize(&read, Default::default())
            .await
            .unwrap()
            .value
            .pop()
            .flatten()
            .is_none()
        );
    }

    #[tokio::test]
    async fn concurrent_partial_native_hydration_accepts_identical_installation() {
        let (storage, state) = fixture().await;
        let bytes = b"concurrent native object";
        let address = NativeObjectRef::TrackedStateTreeChunk(*blake3::hash(bytes).as_bytes());
        let gate = Arc::new(tokio::sync::Barrier::new(2));
        let fetch = |request| {
            let gate = gate.clone();
            let answer = response(&state, &request, bytes);
            async move {
                gate.wait().await;
                Ok(answer)
            }
        };
        let (first, second) = tokio::join!(
            hydrate_native_object(&storage, &state, address, bytes.len(), fetch),
            hydrate_native_object(&storage, &state, address, bytes.len(), fetch),
        );
        assert_eq!(first.unwrap().requests, 1);
        assert_eq!(second.unwrap().requests, 1);
    }

    #[test]
    fn missing_and_incomplete_native_cache_values_are_distinct() {
        let address = NativeObjectRef::TrackedStateTreeChunk(*blake3::hash(b"valid").as_bytes());
        assert!(!is_resident(None, address).unwrap());
        assert!(is_resident(Some(StorageProjectedValue::KeyOnly), address).is_err());
        assert!(
            is_resident(
                Some(StorageProjectedValue::FullValue(Bytes::from_static(
                    b"corrupt"
                ))),
                address
            )
            .is_err()
        );
    }
}
