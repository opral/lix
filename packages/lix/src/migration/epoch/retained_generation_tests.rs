use super::*;

fn active(bank: EpochBank, generation: u64) -> Bytes {
    encode_pointer(PointerState::Active {
        bank,
        generation,
        format: 75,
        publication: None,
    })
}

fn source_proof() -> crate::sync::ReplicaRebuildSource {
    crate::sync::ReplicaRebuildSource {
        repository_id: "retained-repository".to_owned(),
        account_id: "retained-account".to_owned(),
        recovery_required: true,
    }
}

async fn seed_pointer(storage: &crate::Memory, pointer: &Bytes) {
    let mut write = storage.begin_write(WriteOptions::default()).await.unwrap();
    put_pointer(&mut write, pointer.clone()).await.unwrap();
    write.commit().await.unwrap();
}

async fn seed_data(adapter: &StorageAdapter<crate::Memory>, value: &'static [u8]) {
    let mut write = adapter
        .begin_migration_write(WriteOptions::default())
        .await
        .unwrap();
    write
        .put_many(
            crate::json_store::JSON_SPACE,
            single_put(b"local-only", Bytes::from_static(value)),
        )
        .await
        .unwrap();
    write.commit().await.unwrap();
}

async fn local_value(adapter: &StorageAdapter<crate::Memory>) -> Bytes {
    let read = adapter.begin_read(ReadOptions::default()).await.unwrap();
    let value = read
        .get_many(&[GetManyRequest {
            space: crate::json_store::JSON_SPACE,
            keys: &[Key(Bytes::from_static(b"local-only"))],
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .unwrap()
        .values
        .into_iter()
        .next()
        .flatten()
        .unwrap();
    let ProjectedValue::FullValue(bytes) = value else {
        panic!("expected full value")
    };
    bytes
}

#[test]
fn retained_generation_ids_are_disjoint_and_never_wrap() {
    let mut ids = std::collections::HashSet::new();
    for generation in 1..=4093 {
        let bank = replica_generation_bank(generation).unwrap();
        assert!(ids.insert(bank_code(bank)));
        assert!(!matches!(
            bank,
            EpochBank::Legacy | EpochBank::A | EpochBank::B
        ));
        assert_eq!(parse_bank(&bank_code(bank)).unwrap(), bank);
    }
    for invalid in [0, 4094, u64::MAX] {
        assert!(replica_generation_bank(invalid).is_err());
    }
    for invalid in ["g0", "g1024", "g2048", "g4096"] {
        assert!(parse_bank(invalid).is_err());
    }
}

#[tokio::test]
async fn repeated_replacement_keeps_all_sources_and_fences_old_writers() {
    let storage = crate::Memory::new();
    let mut source_bank = EpochBank::Legacy;
    let mut pointer = active(source_bank, 0);
    seed_pointer(&storage, &pointer).await;
    for generation in 1..=4 {
        let source = StorageAdapter::for_epoch(storage.clone(), source_bank, pointer.clone());
        seed_data(&source, b"unique local content").await;
        let target_bank = replica_generation_bank(generation).unwrap();
        let claim = encode_pointer(PointerState::Migrating {
            source: source_bank,
            source_format: 75,
            target: target_bank,
            generation,
            attempt: uuid::Uuid::now_v7(),
        });
        replace_pointer(&storage, &pointer, &claim).await.unwrap();
        let frozen =
            StorageAdapter::for_epoch_migration(storage.clone(), source_bank, claim.clone());
        retain_replica_source(&storage, &claim, &frozen, 75, &source_proof())
            .await
            .unwrap();
        let target =
            StorageAdapter::for_epoch_migration(storage.clone(), target_bank, claim.clone());
        clear_bank(&target).await.unwrap();
        assert!(
            list_retained_replica_sources(&storage)
                .await
                .unwrap()
                .is_empty()
        );
        let published = active(target_bank, generation);
        replace_pointer(&storage, &claim, &published).await.unwrap();
        assert!(matches!(
            source.begin_read(ReadOptions::default()).await,
            Err(StorageError::Fenced)
        ));
        retire_legacy_layout(&storage, &published).await.unwrap();
        let retained = list_retained_replica_sources(&storage).await.unwrap();
        assert_eq!(retained.len(), generation as usize);
        for record in retained {
            let reader = open_retained_replica_source(&storage, &record)
                .await
                .unwrap();
            assert_eq!(local_value(&reader).await.as_ref(), b"unique local content");
            assert!(matches!(
                reader.begin_migration_write(WriteOptions::default()).await,
                Err(StorageError::Fenced)
            ));
        }
        source_bank = target_bank;
        pointer = published;
    }
}

#[tokio::test]
async fn failed_replacement_retention_is_not_reported_as_a_frozen_archive() {
    let storage = crate::Memory::new();
    let pointer = active(EpochBank::A, 1);
    seed_pointer(&storage, &pointer).await;
    let source = StorageAdapter::for_epoch(storage.clone(), EpochBank::A, pointer.clone());
    seed_data(&source, b"saved before upgrade").await;
    let claim = encode_pointer(PointerState::Migrating {
        source: EpochBank::A,
        source_format: 75,
        target: replica_generation_bank(2).unwrap(),
        generation: 2,
        attempt: uuid::Uuid::now_v7(),
    });
    replace_pointer(&storage, &pointer, &claim).await.unwrap();
    let frozen = StorageAdapter::for_epoch_migration(storage.clone(), EpochBank::A, claim.clone());
    retain_replica_source(&storage, &claim, &frozen, 75, &source_proof())
        .await
        .unwrap();
    // Bootstrap failure/crash recovery republishes the original source. Its
    // retention record must not misrepresent this now-writable source as frozen.
    replace_pointer(&storage, &claim, &pointer).await.unwrap();
    assert!(
        list_retained_replica_sources(&storage)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(local_value(&source).await.as_ref(), b"saved before upgrade");
    seed_data(&source, b"saved after retry").await;
    assert_eq!(local_value(&source).await.as_ref(), b"saved after retry");
}
