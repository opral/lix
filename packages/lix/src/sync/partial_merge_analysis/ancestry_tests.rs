use super::*;
use crate::storage_adapter::StorageAdapter;

struct CountedTopologyRead<R> {
    inner: R,
    graph_keys: std::sync::atomic::AtomicUsize,
    header_keys: std::sync::atomic::AtomicUsize,
}

impl<R: StorageAdapterRead> StorageAdapterRead for CountedTopologyRead<R> {
    async fn get_many(
        &self,
        requests: &[crate::storage::GetManyRequest<'_>],
    ) -> Result<crate::storage::GetManyResult, crate::storage::StorageError> {
        use std::sync::atomic::Ordering;
        for request in requests {
            if request.space == crate::changelog::COMMIT_SPACE {
                self.graph_keys
                    .fetch_add(request.keys.len(), Ordering::Relaxed);
            }
            if request.space == crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE {
                self.header_keys
                    .fetch_add(request.keys.len(), Ordering::Relaxed);
            }
        }
        self.inner.get_many(requests).await
    }

    async fn begin_scan(
        &self,
        _space: crate::storage::StorageSpace,
        _range: crate::storage::KeyRange,
        _opts: crate::storage::BeginScanOptions,
    ) -> Result<crate::storage::ScanCursor<'_>, crate::storage::StorageError> {
        panic!("an incorporation proof must use exact topology reads");
    }
}

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
    install(&storage, &records.values().collect::<Vec<_>>()).await;
    storage
}

async fn install(storage: &StorageAdapter<crate::Memory>, records: &[&CommitRecord]) {
    let mut writes = storage.new_write_set();
    for node in records {
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
async fn repeated_authority_merges_preserve_offline_ancestor_admission() {
    for merge_count in [64, 256, 1024, 4096] {
        let mut records = BTreeMap::new();
        let base = append(&mut records, vec![]);
        let mut head = base;
        // Each accepted edit creates an authority merge, even when the editor
        // started from the latest authority head and introduced no conflict.
        for _ in 0..merge_count {
            let local = append(&mut records, vec![head]);
            head = append(&mut records, vec![head, local]);
        }
        let storage = store(&records).await;
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut cache = BTreeMap::new();
        let started = std::time::Instant::now();
        let result = bounded_ancestor(&read, &records[&base], head, &mut cache, 1024).await;
        eprintln!(
            "authority_merge_ancestry_profile merges={merge_count} loaded_records={} elapsed_us={} result={result:?}",
            cache.len(),
            started.elapsed().as_micros(),
        );
        assert!(
            matches!(result, Ok(true)),
            "an offline base remains an authority ancestor after {merge_count} accepted edits: {result:?}"
        );
    }
}

#[tokio::test]
async fn cold_merge_ancestry_resumes_without_replaying_completed_edges() {
    let mut records = BTreeMap::new();
    let base = append(&mut records, vec![]);
    let mut head = base;
    for _ in 0..256 {
        let local = append(&mut records, vec![head]);
        head = append(&mut records, vec![head, local]);
    }
    let storage = store(&BTreeMap::from([(base, records[&base].clone())])).await;
    let mut walk = ancestry::Walk::new(records[&base].clone(), head);
    let mut cache = BTreeMap::new();
    let mut fetched = BTreeSet::new();
    loop {
        let read = storage.begin_read(Default::default()).await.unwrap();
        let result = walk.run(&read, &mut cache, 16).await;
        drop(read);
        match result {
            Ok(included) => {
                assert!(included);
                break;
            }
            Err(error) => {
                let Some(NativeMetadataRef::CommitGraphRecord(id)) =
                    NativeMetadataRef::from_missing_error(&error).unwrap()
                else {
                    panic!("unexpected proof failure: {error:?}");
                };
                let id = CommitId::parse_lix(&id, "test graph record").unwrap();
                assert!(
                    fetched.insert(id),
                    "a hydrated graph record must not be requested twice"
                );
                install(&storage, &[&records[&id]]).await;
            }
        }
    }
    assert_eq!(fetched.len(), 512);
    assert!(
        walk.steps <= 3 * records.len(),
        "cold proof replayed its completed prefix: {} steps",
        walk.steps
    );
    assert_eq!(cache.len(), fetched.len());
    eprintln!(
        "cold_authority_ancestry_profile fetched={} steps={}",
        fetched.len(),
        walk.steps
    );
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

#[tokio::test]
async fn checkpoint_incorporation_requires_complete_state_not_selected_sources() {
    for full in [false, true] {
        let lix = crate::open_lix().await.unwrap();
        lix.execute(
            "INSERT INTO lix_key_value(key,value) VALUES ('selected','zero'),('remaining','zero')",
            &[],
        )
        .await
        .unwrap();
        for index in 0..5 {
            lix.execute(
                &format!("UPDATE lix_key_value SET value='value-{index}' WHERE key='selected'"),
                &[],
            )
            .await
            .unwrap();
        }
        let source = lix
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        let checkpoint_sql = if full {
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))"
        } else {
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value') WHERE key='selected'))"
        };
        let checkpoint = lix.execute(checkpoint_sql, &[]).await.unwrap().rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        let head = lix
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let source = record(
            &read,
            CommitId::parse_lix(&source, "source").unwrap(),
            false,
        )
        .await
        .unwrap();
        let checkpoint = CommitId::parse_lix(&checkpoint, "checkpoint").unwrap();
        let head = CommitId::parse_lix(&head, "head").unwrap();
        let checkpoint_record = record(&read, checkpoint, false).await.unwrap();
        assert!(
            source.generation > checkpoint_record.generation,
            "fixture must exercise source generations above the compacted checkpoint"
        );
        assert!(
            !bounded_ancestor(&read, &source, checkpoint, &mut BTreeMap::new(), 4)
                .await
                .unwrap()
        );
        assert_eq!(
            incorporated(&read, &source, checkpoint, &mut BTreeMap::new(), 4)
                .await
                .unwrap(),
            full,
            "a selected checkpoint cannot incorporate the source's unselected rows"
        );
        assert!(
            incorporated(&read, &source, head, &mut BTreeMap::new(), 4)
                .await
                .unwrap(),
            "both a full checkpoint and a partial checkpoint's working continuation retain the complete source"
        );
        drop(read);
        lix.execute(
            "UPDATE lix_key_value SET value='later' WHERE key='remaining'",
            &[],
        )
        .await
        .unwrap();
        let later = lix
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert!(
            incorporated(
                &read,
                &source,
                CommitId::parse_lix(&later, "later").unwrap(),
                &mut BTreeMap::new(),
                4
            )
            .await
            .unwrap()
        );
    }
}

#[tokio::test]
async fn recent_divergence_incorporation_does_not_scan_old_authority_history() {
    use std::sync::atomic::Ordering;
    for old_commits in [4, 400] {
        let memory = crate::Memory::new();
        let authority = crate::open_lix()
            .with_storage(memory.clone())
            .await
            .unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value(key,value) VALUES('local','base'),('remote','base')",
                &[],
            )
            .await
            .unwrap();
        for index in 0..old_commits {
            authority
                .execute(
                    &format!("UPDATE lix_key_value SET value='old-{index}' WHERE key='remote'"),
                    &[],
                )
                .await
                .unwrap();
        }
        let base = authority
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        let local = crate::open_lix()
            .with_storage(memory.fork().unwrap())
            .await
            .unwrap();
        local
            .execute(
                "UPDATE lix_key_value SET value='incoming' WHERE key='local'",
                &[],
            )
            .await
            .unwrap();
        let incoming = local
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        let local_adapter = local.storage_adapter();
        let local_read = local_adapter.begin_read(Default::default()).await.unwrap();
        let incoming = record(
            &local_read,
            CommitId::parse_lix(&incoming, "incoming").unwrap(),
            false,
        )
        .await
        .unwrap();
        drop(local_read);
        authority
            .execute(
                "UPDATE lix_key_value SET value='authority' WHERE key='remote'",
                &[],
            )
            .await
            .unwrap();
        let remote = authority
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        let adapter = authority.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let base = record(&read, CommitId::parse_lix(&base, "base").unwrap(), false)
            .await
            .unwrap();
        let read = CountedTopologyRead {
            inner: read,
            graph_keys: Default::default(),
            header_keys: Default::default(),
        };
        let mut cache = BTreeMap::from([(incoming.commit_id, incoming.clone())]);
        assert!(
            !ancestry::incorporated_since(
                &read,
                &incoming,
                CommitId::parse_lix(&remote, "remote").unwrap(),
                &base,
                &mut cache,
                4
            )
            .await
            .unwrap()
        );
        let graph_keys = read.graph_keys.load(Ordering::Relaxed);
        let header_keys = read.header_keys.load(Ordering::Relaxed);
        assert!(
            graph_keys <= 3,
            "old history leaked into recent proof: {graph_keys}"
        );
        assert_eq!(
            header_keys, 1,
            "only the new remote commit needs its source header"
        );
        eprintln!(
            "recent_divergence_incorporation_profile old_commits={old_commits} graph_keys={graph_keys} header_keys={header_keys}"
        );
    }
}
