use super::super::partial_state::stage_partial_replica_state;
use super::*;
use crate::binary_cas::{BinaryCasContext, BlobDataReader, BlobManifestRequired};
use crate::{Memory, open_lix};

async fn fixture() -> (StorageAdapter<Memory>, PartialReplicaState) {
    let authority = open_lix().await.unwrap();
    let state = PartialReplicaState::new(
        format!("https://example.test/lix/{}", authority.lix_id()),
        authority.active_account_id().into(),
        "00000000-0000-7000-8000-000000000399".into(),
        authority.partial_replica_descriptor(None).await.unwrap(),
    )
    .unwrap();
    let storage = StorageAdapter::new(Memory::new());
    let mut writes = storage.new_write_set();
    let condition = stage_partial_replica_state(&mut writes, &state, None).unwrap();
    storage
        .commit_write_set(
            writes,
            StorageWriteOptions {
                preconditions: vec![condition],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    (storage, state)
}

fn wire(bytes: &[u8]) -> SyncBlobManifest {
    SyncBlobManifest {
        blob_id: BlobId::from_content(bytes).to_hex(),
        size_bytes: bytes.len() as u64,
        chunks: vec![super::super::SyncBlobChunk {
            chunk_id: ChunkHash::from_content(bytes).to_hex(),
            size_bytes: bytes.len() as u64,
        }],
        inline_bytes_base64: None,
    }
}

#[tokio::test]
async fn referenced_partial_read_demands_manifest_then_chunk_and_stays_warm() {
    let (storage, state) = fixture().await;
    let bytes = b"partial file content";
    let hash = BlobId::from_content(bytes);
    let context = BinaryCasContext::new();
    let read = storage.begin_read(Default::default()).await.unwrap();
    // Generic existence probes and ordinary readers keep absent semantics.
    context
        .reader(&read)
        .require_referenced_manifests(&[hash])
        .await
        .unwrap();
    assert!(
        crate::binary_cas::load_bytes_many(&read, &[hash])
            .await
            .unwrap()
            .into_vec()[0]
            .is_none()
    );
    context.enable_referenced_manifest_demands();
    let error = context
        .reader(&read)
        .require_referenced_manifests(&[hash])
        .await
        .unwrap_err();
    assert_eq!(
        BlobManifestRequired::from_error(&error).unwrap(),
        Some(BlobManifestRequired(hash))
    );
    drop(read);
    let result = install_manifest(&storage, &state, hash, &wire(bytes))
        .await
        .unwrap();
    assert_eq!(
        result.missing_chunk_ids,
        vec![ChunkHash::from_content(bytes).to_hex()]
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert_eq!(
        crate::binary_cas::load_bytes_many(&read, &[hash])
            .await
            .unwrap_err()
            .code,
        "LIX_SYNC_CHUNKS_REQUIRED"
    );
    drop(read);
    install_chunk(&storage, &state, ChunkHash::from_content(bytes), bytes)
        .await
        .unwrap();
    for _ in 0..3 {
        let read = storage.begin_read(Default::default()).await.unwrap();
        context
            .reader(&read)
            .require_referenced_manifests(&[hash])
            .await
            .unwrap();
        assert_eq!(
            crate::binary_cas::load_bytes_many(&read, &[hash])
                .await
                .unwrap()
                .into_vec(),
            vec![Some(bytes.to_vec())]
        );
    }
}

#[tokio::test]
async fn bad_hash_and_old_epoch_never_publish_blob_bytes() {
    let (storage, state) = fixture().await;
    let bytes = b"validated content";
    let hash = BlobId::from_content(bytes);
    let mut wrong = wire(bytes);
    wrong.blob_id = BlobId::from_content(b"other").to_hex();
    assert!(
        install_manifest(&storage, &state, hash, &wrong)
            .await
            .is_err()
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert!(load_metadata_many(&read, &[hash]).await.unwrap().into_vec()[0].is_none());
    drop(read);
    install_manifest(&storage, &state, hash, &wire(bytes))
        .await
        .unwrap();
    assert!(
        install_chunk(&storage, &state, ChunkHash::from_content(bytes), b"corrupt")
            .await
            .is_err()
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (_, raw) = load_partial_replica_state(&read).await.unwrap().unwrap();
    assert!(
        crate::binary_cas::load_verified_chunk(&read, ChunkHash::from_content(bytes))
            .await
            .unwrap()
            .is_none()
    );
    drop(read);
    let replacement = PartialReplicaState::new(
        state.remote_id().into(),
        state.active_account_id().into(),
        "00000000-0000-7000-8000-000000000499".into(),
        state.descriptor().clone(),
    )
    .unwrap();
    let mut writes = storage.new_write_set();
    let condition = stage_partial_replica_state(&mut writes, &replacement, Some(raw)).unwrap();
    storage
        .commit_partial_replica_write_set(
            super::super::partial_replica_write_capability(),
            writes,
            StorageWriteOptions {
                preconditions: vec![condition],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        install_chunk(&storage, &state, ChunkHash::from_content(bytes), bytes)
            .await
            .unwrap_err()
            .code,
        "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH"
    );
}

#[tokio::test]
async fn duplicate_manifest_and_chunk_installs_are_idempotent_without_overwrite() {
    let (storage, state) = fixture().await;
    let bytes = b"same concurrent content";
    let hash = BlobId::from_content(bytes);
    let manifest = wire(bytes);
    let (first, second) = futures_util::join!(
        install_manifest(&storage, &state, hash, &manifest),
        install_manifest(&storage, &state, hash, &manifest),
    );
    first.unwrap();
    second.unwrap();
    let chunk = ChunkHash::from_content(bytes);
    let (first, second) = futures_util::join!(
        install_chunk(&storage, &state, chunk, bytes),
        install_chunk(&storage, &state, chunk, bytes),
    );
    first.unwrap();
    second.unwrap();
    assert!(manifest_is_resident(&storage, &state, hash).await.unwrap());
    assert!(chunk_is_resident(&storage, &state, chunk).await.unwrap());
}

#[tokio::test]
async fn corrupt_resident_manifest_is_not_a_demand_or_a_repair() {
    let (storage, state) = fixture().await;
    let hash = BlobId::from_content(b"corrupted manifest");
    let mut writes = storage.new_write_set();
    writes.put(
        BINARY_CAS_MANIFEST_SPACE,
        StorageKey(Bytes::copy_from_slice(hash.as_bytes())),
        b"invalid manifest".as_slice(),
    );
    storage
        .commit_partial_replica_write_set(
            super::super::partial_replica_write_capability(),
            writes,
            Default::default(),
        )
        .await
        .unwrap();
    let context = BinaryCasContext::new();
    context.enable_referenced_manifest_demands();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let error = context
        .reader(&read)
        .require_referenced_manifests(&[hash])
        .await
        .unwrap_err();
    assert!(BlobManifestRequired::from_error(&error).unwrap().is_none());
    drop(read);
    assert!(
        install_manifest(&storage, &state, hash, &wire(b"corrupted manifest"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn preconnect_chunk_check_requires_explicit_demand_or_verified_presence() {
    let (storage, state) = fixture().await;
    let bytes = b"content requested by authenticated manifest";
    let hash = BlobId::from_content(bytes);
    let chunk = ChunkHash::from_content(bytes);
    let error = chunk_is_resident(&storage, &state, chunk)
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_STORAGE_ERROR);
    assert!(error.message.contains("demand marker"));
    // A bare demand error cannot authorize fetching an arbitrary missing key.
    assert!(install_chunk(&storage, &state, chunk, bytes).await.is_err());
    install_manifest(&storage, &state, hash, &wire(bytes))
        .await
        .unwrap();
    assert!(!chunk_is_resident(&storage, &state, chunk).await.unwrap());
    install_chunk(&storage, &state, chunk, bytes).await.unwrap();
    assert!(chunk_is_resident(&storage, &state, chunk).await.unwrap());
}
