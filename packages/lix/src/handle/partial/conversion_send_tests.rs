//! Safety obligations for the single owned conversion Send assertion.
use super::*;
use crate::session::borrowing_proof_storage::{BorrowingRead, BorrowingStorage};
use crate::storage_adapter::{SharedStorageAdapterRead, StorageAdapterReadScope};
fn is_send<T: Send>(_: &T) {}
fn assert_send<T: Send + ?Sized>() {}
fn assert_sync<T: Sync + ?Sized>() {}

#[test]
fn complete_unwrapped_memory_conversion_future_is_send() {
    let raw = convert_full_replica_owned(
        crate::Memory::new(),
        ServerOptions::new("https://example.invalid/lix/test"),
        Some(uuid::Uuid::nil().to_string()),
    );
    is_send(&raw);
}

// A free lifetime proves these obligations universally, rather than only at
// 'static. The actual borrowing adapter reproduces native RocksDB/Filesystem
// read-handle lifetimes; whole-future inference at this shape is the compiler
// limitation documented by session::assume_send_future_proofs_borrowing.
#[allow(dead_code)]
fn native_storage_obligations<'a, S: Storage + Clone + Send + Sync + 'a>() {
    assert_send::<S::Read<'a>>();
    assert_sync::<S::Read<'a>>();
    assert_send::<S::Write<'a>>();
    assert_send::<StorageAdapterReadScope<S::Read<'a>>>();
    assert_sync::<StorageAdapterReadScope<S::Read<'a>>>();
    assert_send::<SharedStorageAdapterRead<S::Read<'a>>>();
    assert_sync::<SharedStorageAdapterRead<S::Read<'a>>>();
    assert_send::<StorageSession<S>>();
    assert_sync::<StorageSession<S>>();
    assert_sync::<crate::storage_adapter::StorageAdapter<S>>();
}
#[allow(dead_code)]
fn borrowing_adapter_obligations<'a>() {
    native_storage_obligations::<'a, BorrowingStorage>();
    assert_send::<SharedStorageAdapterRead<BorrowingRead<'a>>>();
    assert_sync::<SharedStorageAdapterRead<BorrowingRead<'a>>>();
}
#[test]
fn owned_and_borrowed_capture_types_meet_send_contract() {
    assert_send::<ServerOptions>();
    assert_sync::<ServerOptions>();
    assert_send::<crate::storage::StorageOwnerLease>();
    assert_sync::<bytes::Bytes>();
    assert_sync::<Arc<dyn crate::OpenProgressSink>>();
    assert_sync::<crate::sync::InspectedFullConversion>();
    assert_sync::<crate::sync::AuthenticatedPartialConversion>();
    assert_sync::<crate::sync::PartialReplicaState>();
    assert_send::<crate::migration::PendingConversionJournal>();
    assert_send::<crate::migration::GlobalConversionJournal>();
}
