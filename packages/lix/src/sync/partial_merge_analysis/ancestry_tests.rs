use super::*;
use crate::storage_adapter::StorageAdapter;

fn append(records: &mut BTreeMap<CommitId, CommitRecord>, parents: Vec<CommitId>) -> CommitId {
    let id = CommitId::for_test_label(&format!("merge-ancestry-{}", records.len()));
    let generation = parents
        .iter()
        .map(|id| records[id].generation + 1)
        .max()
        .unwrap_or(0);
    let parent = parents.first().map(|id| &records[id]);
    let jump = parent.map(|parent| &records[&parent.first_parent_jump_commit_id]);
    let (jump_id, jump_span) =
        crate::changelog::next_first_parent_jump(id, &parents, parent, jump).unwrap();
    records.insert(
        id,
        CommitRecord {
            format_version: crate::changelog::COMMIT_RECORD_FORMAT_VERSION,
            commit_id: id,
            generation,
            parent_commit_ids: parents,
            base_commit_id: None,
            first_parent_jump_commit_id: jump_id,
            first_parent_jump_span: jump_span,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.into(),
            created_at: crate::common::LixTimestamp::parse("2026-09-12T00:00:00Z").unwrap(),
            touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
            is_checkpoint: false,
        },
    );
    id
}

async fn store(records: &BTreeMap<CommitId, CommitRecord>) -> StorageAdapter<crate::Memory> {
    let storage = StorageAdapter::new(crate::Memory::new());
    let mut writes = storage.new_write_set();
    for node in records.values() {
        writes.put(
            crate::changelog::COMMIT_SPACE,
            crate::changelog::commit_key(node.commit_id),
            crate::changelog::encode_commit_record(node).unwrap(),
        );
    }
    storage
        .commit_write_set(writes, Default::default())
        .await
        .unwrap();
    storage
}

#[tokio::test]
async fn long_authority_history_uses_bounded_native_jump_reads() {
    let mut records = BTreeMap::new();
    let mut chain = vec![append(&mut records, vec![])];
    for _ in 0..4096 {
        chain.push(append(&mut records, vec![*chain.last().unwrap()]));
    }
    let unrelated = append(&mut records, vec![]);
    let storage = store(&records).await;
    let read = storage.begin_read(Default::default()).await.unwrap();
    for index in [0, 1, 97, 1023, 2048, 4000] {
        let mut cache = BTreeMap::new();
        assert!(
            bounded_ancestor(&read, &records[&chain[index]], chain[4096], &mut cache, 64)
                .await
                .unwrap()
        );
        assert!(cache.len() < 64, "{index}: {} reads", cache.len());
    }
    assert!(
        !bounded_ancestor(
            &read,
            &records[&unrelated],
            chain[4096],
            &mut BTreeMap::new(),
            64,
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn linear_jumps_preserve_secondary_merge_ancestry() {
    let mut records = BTreeMap::new();
    let base = append(&mut records, vec![]);
    let side = append(&mut records, vec![base]);
    let mut main = append(&mut records, vec![base]);
    for _ in 0..2048 {
        main = append(&mut records, vec![main]);
    }
    let merged = append(&mut records, vec![main, side]);
    let mut head = merged;
    for _ in 0..2048 {
        head = append(&mut records, vec![head]);
    }
    let storage = store(&records).await;
    let read = storage.begin_read(Default::default()).await.unwrap();
    for ancestor in [side, base, main] {
        assert!(
            bounded_ancestor(&read, &records[&ancestor], head, &mut BTreeMap::new(), 64)
                .await
                .unwrap()
        );
    }
}

#[tokio::test]
async fn native_jump_target_generation_is_validated() {
    let mut records = BTreeMap::new();
    let base = append(&mut records, vec![]);
    let mut head = base;
    for _ in 0..63 {
        head = append(&mut records, vec![head]);
    }
    assert!(records[&head].first_parent_jump_span > 1);
    records.get_mut(&head).unwrap().first_parent_jump_span -= 1;
    let storage = store(&records).await;
    let read = storage.begin_read(Default::default()).await.unwrap();
    let error = bounded_ancestor(&read, &records[&base], head, &mut BTreeMap::new(), 64)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("jump target generation"),
        "{error}"
    );
}
