use super::*;
use crate::storage::StorageWrite;
use crate::storage_adapter::{StorageAdapter, StorageWriteOptions};
use crate::sync::{PartialReplicaState, stage_partial_bootstrap};
use crate::tracked_state::NativeMetadataRef;
use crate::{Memory, open_lix};

async fn publish_fixture(adapter: &StorageAdapter<Memory>, writes: StorageWriteSet) {
    let mut raw = adapter
        .begin_migration_write(StorageWriteOptions::default())
        .await
        .unwrap();
    writes.lower_into(&mut raw).await.unwrap();
    raw.commit().await.unwrap();
}

async fn partial_fixture() -> (StorageAdapter<Memory>, PartialReplicaState) {
    let authority = open_lix().await.unwrap();
    let state = PartialReplicaState::new(
        format!("https://example.test/lix/{}", authority.lix_id()),
        authority.active_account_id().to_owned(),
        uuid::Uuid::now_v7().to_string(),
        authority.partial_replica_descriptor(None).await.unwrap(),
    )
    .unwrap();
    let adapter = StorageAdapter::new(Memory::new());
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let mut writes = adapter.new_write_set();
    stage_partial_bootstrap(&read, &mut writes, &state).unwrap();
    crate::init::stage_partial_repository_protocol(&mut writes);
    drop(read);
    publish_fixture(&adapter, writes).await;
    (adapter, state)
}

fn checkpoint(index: usize) -> CommitId {
    CommitId::parse(&format!("00000000-0000-7000-8000-{index:012x}")).unwrap()
}

#[tokio::test]
async fn unknown_conversation_frontier_is_bounded_without_hiding_late_corruption() {
    let (adapter, _) = partial_fixture().await;
    let ids = (1..=64).map(checkpoint).collect::<Vec<_>>();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let error = load_checkpoint_conversations(&read, &ids)
        .await
        .unwrap_err();
    let demands = NativeMetadataRef::batch_from_missing_error(&error)
        .unwrap()
        .unwrap();
    assert_eq!(
        demands,
        ids[..32]
            .iter()
            .map(|id| NativeMetadataRef::CheckpointConversation(id.to_string()))
            .collect::<Vec<_>>()
    );
    drop(read);

    // Corruption after the first demand batch must be diagnosed before any
    // missing frontier is returned; a retry cannot repair malformed bytes.
    let mut writes = adapter.new_write_set();
    writes.put(
        PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE,
        partial_null_coverage_key(ids[63]),
        StorageValue {
            bytes: Bytes::from_static(b"malformed"),
        },
    );
    publish_fixture(&adapter, writes).await;
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let error = load_checkpoint_conversations(&read, &ids)
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
    assert!(
        NativeMetadataRef::from_missing_error(&error)
            .unwrap()
            .is_none()
    );
    assert!(
        NativeMetadataRef::batch_from_missing_error(&error)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn partial_null_is_known_only_for_the_current_source_epoch() {
    let (adapter, state) = partial_fixture().await;
    let id = checkpoint(1);
    let mut writes = adapter.new_write_set();
    stage_partial_null_coverage(&mut writes, &uuid::Uuid::now_v7().to_string(), id).unwrap();
    publish_fixture(&adapter, writes).await;
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let error = load_checkpoint_conversation(&read, id).await.unwrap_err();
    assert_eq!(
        NativeMetadataRef::from_missing_error(&error).unwrap(),
        Some(NativeMetadataRef::CheckpointConversation(id.to_string()))
    );
    drop(read);

    let mut writes = adapter.new_write_set();
    stage_partial_null_coverage(&mut writes, state.epoch_id(), id).unwrap();
    publish_fixture(&adapter, writes).await;
    let read = adapter.begin_read(Default::default()).await.unwrap();
    assert_eq!(load_checkpoint_conversation(&read, id).await.unwrap(), None);
    assert_eq!(load_checkpoint_conversation(&read, id).await.unwrap(), None);
}

#[tokio::test]
async fn full_absence_and_partial_canonical_pointer_keep_distinct_meanings() {
    let full = StorageAdapter::new(Memory::new());
    let id = checkpoint(1);
    let read = full.begin_read(Default::default()).await.unwrap();
    assert_eq!(load_checkpoint_conversation(&read, id).await.unwrap(), None);

    let (partial, _) = partial_fixture().await;
    let conversation_id = uuid::Uuid::now_v7().to_string();
    let mut writes = partial.new_write_set();
    stage_checkpoint_conversation(&mut writes, id, &conversation_id).unwrap();
    // A canonical pointer remains authoritative when an old NULL proof has
    // become stale; it does not require a new negative-coverage record.
    stage_partial_null_coverage(&mut writes, &uuid::Uuid::now_v7().to_string(), id).unwrap();
    publish_fixture(&partial, writes).await;
    let read = partial.begin_read(Default::default()).await.unwrap();
    assert_eq!(
        load_checkpoint_conversation(&read, id).await.unwrap(),
        Some(conversation_id)
    );
}

#[tokio::test]
async fn competing_partial_some_and_null_imports_cannot_publish_both_facts() {
    for null_wins in [true, false] {
        let (adapter, state) = partial_fixture().await;
        let id = checkpoint(1);
        let conversation_id = uuid::Uuid::now_v7().to_string();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let (_, receipt) = crate::sync::load_partial_replica_state(&read)
            .await
            .unwrap()
            .unwrap();
        let mut positive = adapter.new_write_set();
        let mut positive_guards = Vec::new();
        stage_checkpoint_conversation_fact(
            &read,
            &mut positive,
            &mut positive_guards,
            id,
            true,
            Some(&conversation_id),
            Some((state.epoch_id(), &receipt)),
        )
        .await
        .unwrap();
        let mut negative = adapter.new_write_set();
        let mut negative_guards = Vec::new();
        stage_checkpoint_conversation_fact(
            &read,
            &mut negative,
            &mut negative_guards,
            id,
            true,
            None,
            Some((state.epoch_id(), &receipt)),
        )
        .await
        .unwrap();
        drop(read);

        let (winner, winner_guards, loser, loser_guards) = if null_wins {
            (negative, negative_guards, positive, positive_guards)
        } else {
            (positive, positive_guards, negative, negative_guards)
        };
        adapter
            .commit_migration_write_set(
                winner,
                StorageWriteOptions {
                    preconditions: winner_guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(
            adapter
                .commit_migration_write_set(
                    loser,
                    StorageWriteOptions {
                        preconditions: loser_guards,
                        ..Default::default()
                    },
                )
                .await
                .is_err(),
            "the losing import must fail its original read preconditions"
        );
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert_eq!(
            load_checkpoint_conversation(&read, id).await.unwrap(),
            if null_wins {
                None
            } else {
                Some(conversation_id)
            }
        );
    }
}

#[tokio::test]
async fn existing_full_checkpoint_null_cannot_be_rewritten_by_import() {
    let authority = open_lix().await.unwrap();
    authority
        .execute(
            "INSERT INTO lix_key_value(key, value) VALUES ('description-proof', 'seed')",
            &[],
        )
        .await
        .unwrap();
    authority.create_checkpoint().await.unwrap();
    let result = authority
        .execute(
            "SELECT commit_id FROM lix_log() WHERE is_checkpoint LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    let id = CommitId::parse(&result.rows()[0].get::<String>("commit_id").unwrap()).unwrap();
    let adapter = authority.storage_adapter();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    assert_eq!(load_checkpoint_conversation(&read, id).await.unwrap(), None);
    let mut writes = adapter.new_write_set();
    let mut preconditions = Vec::new();
    let conversation_id = uuid::Uuid::now_v7().to_string();
    let error = stage_checkpoint_conversation_fact(
        &read,
        &mut writes,
        &mut preconditions,
        id,
        true,
        Some(&conversation_id),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    assert_eq!(load_checkpoint_conversation(&read, id).await.unwrap(), None);
}
