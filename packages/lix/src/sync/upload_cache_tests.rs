// Included in repository tests to exercise the runtime's retained wave against
// real durable receipts, local writes, and authoritative ref publication.

async fn upload_cache_head(lix: &Lix<Memory>) -> String {
    lix.execute("SELECT lix_active_branch_commit_id() AS head", &[])
        .await
        .expect("head query")
        .rows()[0]
        .get::<String>("head")
        .expect("head value")
}

#[tokio::test]
async fn cached_upload_wave_publishes_captured_refs_despite_continuous_appends() {
    let authority = open_lix().await.expect("authority");
    let snapshot = authority
        .pull_sync_repository(None, 1)
        .await
        .expect("snapshot");
    let replica = replica_from_snapshot(&authority, &snapshot).await;
    for i in 0..6 {
        write_key_value(&replica, "wave", &format!("initial-{i}")).await;
    }
    let captured_head = upload_cache_head(&replica).await;
    let mut cache = None;
    let mut published_captured = false;
    let mut page_count = 0;
    loop {
        let request = replica
            .build_sync_push_with_plan(TEST_REMOTE, 1, &mut cache)
            .await
            .expect("retained page")
            .expect("initial wave remains pending");
        page_count += 1;
        assert_eq!(request.commits.len() + request.ref_updates.len(), 1);
        // Every page races another ordinary append. None may restart the wave
        // or postpone the captured ref behind an ever-growing chain.
        write_key_value(&replica, "wave", &format!("concurrent-{page_count}")).await;
        published_captured |= request
            .ref_updates
            .iter()
            .any(|update| update.head_commit_id.as_deref() == Some(captured_head.as_str()));
        frontier_apply_upload(&authority, &replica, &request).await;
        cache
            .as_mut()
            .expect("cache retained")
            .acknowledge()
            .expect("durable ack");
        if cache.as_ref().unwrap().is_complete() {
            break;
        }
        assert!(
            page_count < 24,
            "continuous appends must not restart a finite wave"
        );
    }
    assert!(page_count > 2);
    assert!(published_captured);
    assert_eq!(upload_cache_head(&authority).await, captured_head);
    let latest_head = upload_cache_head(&replica).await;
    assert_ne!(
        latest_head, captured_head,
        "own prefix acknowledgment preserves appends"
    );
    // Losing the in-memory plan is equivalent to reopening the worker. The
    // durable receipt reconstructs only the next pending suffix.
    cache = None;
    for _ in 0..32 {
        let Some(request) = replica
            .build_sync_push_with_plan(TEST_REMOTE, 2, &mut cache)
            .await
            .expect("next wave page")
        else {
            break;
        };
        assert!(
            request
                .commits
                .iter()
                .all(|commit| commit.commit_id != captured_head)
        );
        frontier_apply_upload(&authority, &replica, &request).await;
        cache
            .as_mut()
            .unwrap()
            .acknowledge()
            .expect("next wave ack");
    }
    assert_eq!(upload_cache_head(&authority).await, latest_head);
    assert!(
        replica
            .build_sync_push_with_plan(TEST_REMOTE, 2, &mut cache)
            .await
            .expect("fully converged")
            .is_none()
    );
}

#[tokio::test]
async fn cached_upload_retry_and_smaller_page_advance_only_after_durable_ack() {
    let authority = open_lix().await.expect("authority");
    let snapshot = authority
        .pull_sync_repository(None, 1)
        .await
        .expect("snapshot");
    let replica = replica_from_snapshot(&authority, &snapshot).await;
    for i in 0..5 {
        write_key_value(&replica, "retry", &i.to_string()).await;
    }
    let mut cache = None;
    let large = replica
        .build_sync_push_with_plan(TEST_REMOTE, 3, &mut cache)
        .await
        .expect("large page")
        .unwrap();
    assert_eq!(large.commits.len(), 3);
    let small = replica
        .build_sync_push_with_plan(TEST_REMOTE, 1, &mut cache)
        .await
        .expect("413 smaller page")
        .unwrap();
    assert_eq!(small.commits, large.commits[..1]);
    // Authority accepts the body, but transport loses its response. Without
    // importing that receipt, retry must send the identical bounded page.
    authority
        .push_sync_repository(&small)
        .await
        .expect("lost-response acceptance");
    let retry = replica
        .build_sync_push_with_plan(TEST_REMOTE, 1, &mut cache)
        .await
        .expect("lost-response retry")
        .unwrap();
    assert_eq!(retry, small);
    frontier_apply_upload(&authority, &replica, &retry).await;
    cache
        .as_mut()
        .unwrap()
        .acknowledge()
        .expect("durable retry acknowledgment");
    assert!(
        cache.as_mut().unwrap().acknowledge().is_err(),
        "duplicate ack cannot skip a page"
    );
    let next = replica
        .build_sync_push_with_plan(TEST_REMOTE, 1, &mut cache)
        .await
        .expect("next body")
        .unwrap();
    assert_eq!(next.commits, large.commits[1..2]);
}

#[tokio::test]
async fn cached_upload_checkpoint_midwave_survives_gc_and_keeps_generation() {
    for scoped in [false, true] {
        let authority = open_lix().await.expect("authority");
        write_key_value(&authority, "authority-working", "uncheckpointed").await;
        let snapshot = authority
            .pull_sync_repository(None, 1)
            .await
            .expect("snapshot");
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        for i in 0..5 {
            write_key_value(&replica, "checkpoint-wave", &i.to_string()).await;
        }
        let captured_head = upload_cache_head(&replica).await;
        let mut cache = None;
        let first = replica
            .build_sync_push_with_plan(TEST_REMOTE, 1, &mut cache)
            .await
            .expect("first wave body")
            .unwrap();
        let generation = cache.as_ref().unwrap().plan.generation();
        replica.execute(if scoped {
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))"
        } else { "SELECT commit_id FROM lix_create_checkpoint()" }, &[])
            .await.expect("checkpoint while body upload is in flight");
        let checkpoint_head = upload_cache_head(&replica).await;
        assert_ne!(checkpoint_head, captured_head);
        let adapter = replica.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            super::super::upload_plan::load_generation(&read)
                .await
                .unwrap(),
            generation,
            "checkpoint appends must preserve the existing finite wave"
        );
        let mut gc_writes = adapter.new_write_set();
        crate::gc::stage_repository_gc(SharedStorageAdapterRead::new(read), &mut gc_writes)
            .await
            .expect("GC with pending checkpoint wave");
        adapter
            .commit_certified_replica_write_set(
                super::super::certified_replica_write_capability(),
                gc_writes,
                StorageWriteOptions::default(),
            )
            .await
            .expect("GC commits");
        frontier_apply_upload(&authority, &replica, &first).await;
        cache.as_mut().unwrap().acknowledge().unwrap();
        let mut published_captured = false;
        for _ in 0..32 {
            if cache.as_ref().unwrap().is_complete() {
                break;
            }
            let request = replica
                .build_sync_push_with_plan(TEST_REMOTE, 1, &mut cache)
                .await
                .expect("retained checkpoint wave")
                .unwrap();
            published_captured |= request
                .ref_updates
                .iter()
                .any(|update| update.head_commit_id.as_deref() == Some(captured_head.as_str()));
            frontier_apply_upload(&authority, &replica, &request).await;
            cache.as_mut().unwrap().acknowledge().unwrap();
        }
        assert!(
            published_captured,
            "checkpoint must not replace the already captured wave"
        );
        assert_eq!(
            upload_cache_head(&replica).await,
            checkpoint_head,
            "own prefix ack must preserve a compact checkpoint's private source lineage"
        );
        for _ in 0..32 {
            let Some(request) = replica
                .build_sync_push_with_plan(TEST_REMOTE, 2, &mut cache)
                .await
                .expect("checkpoint suffix")
            else {
                break;
            };
            frontier_apply_upload(&authority, &replica, &request).await;
            cache.as_mut().unwrap().acknowledge().unwrap();
        }
        assert_eq!(upload_cache_head(&authority).await, checkpoint_head);
    }
}

#[tokio::test]
async fn cached_upload_restore_discards_abandoned_wave_before_next_page() {
    let authority = open_lix().await.expect("authority");
    write_key_value(&authority, "restore-cache", "historical").await;
    let target = upload_cache_head(&authority).await;
    write_key_value(&authority, "restore-cache", "authoritative").await;
    let snapshot = authority
        .pull_sync_repository(None, 1)
        .await
        .expect("snapshot");
    let replica = replica_from_snapshot(&authority, &snapshot).await;
    hydrate_history_commit(&authority, &replica, &target).await;
    for i in 0..5 {
        write_key_value(&replica, "restore-cache", &format!("abandoned-{i}")).await;
    }
    let abandoned = upload_cache_head(&replica).await;
    let mut cache = None;
    let first = replica
        .build_sync_push_with_plan(TEST_REMOTE, 1, &mut cache)
        .await
        .unwrap()
        .unwrap();
    let generation = cache.as_ref().unwrap().plan.generation();
    frontier_apply_upload(&authority, &replica, &first).await;
    cache.as_mut().unwrap().acknowledge().unwrap();
    replica
        .execute(
            "INSERT INTO lix_restore (commit_id) VALUES ($1)",
            &[Value::Text(target.clone())],
        )
        .await
        .expect("restore invalidates old wave");
    let request = replica
        .build_sync_push_with_plan(TEST_REMOTE, 128, &mut cache)
        .await
        .expect("restored wave builds")
        .unwrap();
    assert_ne!(cache.as_ref().unwrap().plan.generation(), generation);
    assert!(
        request
            .commits
            .iter()
            .all(|commit| commit.commit_id != abandoned)
    );
    assert!(
        request
            .ref_updates
            .iter()
            .all(|update| update.head_commit_id.as_deref() != Some(abandoned.as_str()))
    );
    frontier_apply_upload(&authority, &replica, &request).await;
    cache.as_mut().unwrap().acknowledge().unwrap();
    assert_eq!(upload_cache_head(&authority).await, target);
}

#[tokio::test]
async fn cached_upload_foreign_authority_ref_invalidates_body_acknowledged_wave() {
    let authority = open_lix().await.expect("authority");
    authority
        .set_sync_role(super::super::SyncRole::Authority)
        .unwrap();
    let snapshot = authority
        .pull_sync_repository(None, 1)
        .await
        .expect("snapshot");
    let replica = replica_from_snapshot(&authority, &snapshot).await;
    for i in 0..5 {
        write_key_value(&replica, "foreign-cache", &format!("pending-{i}")).await;
    }
    let mut cache = None;
    let first = replica
        .build_sync_push_with_plan(TEST_REMOTE, 1, &mut cache)
        .await
        .unwrap()
        .unwrap();
    frontier_apply_upload(&authority, &replica, &first).await;
    cache.as_mut().unwrap().acknowledge().unwrap();
    assert!(!cache.as_ref().unwrap().is_complete());
    write_key_value(&authority, "foreign-cache", "server-wins").await;
    let cursor = replica
        .load_sync_repository_cursor(TEST_REMOTE)
        .await
        .unwrap()
        .unwrap();
    let delta = authority
        .pull_sync_repository(Some(cursor), 128)
        .await
        .unwrap();
    replica
        .apply_sync_repository_pull(TEST_REMOTE, &delta)
        .await
        .unwrap();
    assert_eq!(
        read_key_value(&replica, "foreign-cache").await,
        "server-wins"
    );
    assert!(
        replica
            .build_sync_push_with_plan(TEST_REMOTE, 1, &mut cache)
            .await
            .unwrap()
            .is_none()
    );
    assert!(cache.is_none(), "foreign ref retires the stale upload wave");
}

#[tokio::test]
async fn cached_upload_deleted_and_recreated_branch_cannot_publish_old_intent() {
    let authority = open_lix().await.expect("authority");
    let snapshot = authority
        .pull_sync_repository(None, 1)
        .await
        .expect("snapshot");
    let replica = replica_from_snapshot(&authority, &snapshot).await;
    let main = replica.active_branch_id().await.unwrap();
    let main_head = upload_cache_head(&replica).await;
    let branch = replica
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-000000007911".to_owned()),
            name: "old-upload-intent".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    replica
        .switch_branch(SwitchBranchOptions {
            branch_id: branch.id.clone(),
        })
        .await
        .unwrap();
    for i in 0..4 {
        write_key_value(&replica, "deleted-cache", &i.to_string()).await;
    }
    let abandoned = upload_cache_head(&replica).await;
    replica
        .switch_branch(SwitchBranchOptions { branch_id: main })
        .await
        .unwrap();
    let mut cache = None;
    let first = replica
        .build_sync_push_with_plan(TEST_REMOTE, 1, &mut cache)
        .await
        .unwrap()
        .unwrap();
    let generation = cache.as_ref().unwrap().plan.generation();
    frontier_apply_upload(&authority, &replica, &first).await;
    cache.as_mut().unwrap().acknowledge().unwrap();
    replica
        .execute(
            "DELETE FROM lix_branch WHERE id = $1",
            &[Value::Text(branch.id.clone())],
        )
        .await
        .expect("delete captured branch");
    replica
        .create_branch(CreateBranchOptions {
            id: Some(branch.id.clone()),
            name: "replacement-upload-intent".to_owned(),
            from_commit_id: Some(main_head.clone()),
        })
        .await
        .expect("recreate exact branch ID");
    let request = replica
        .build_sync_push_with_plan(TEST_REMOTE, 128, &mut cache)
        .await
        .expect("replacement wave")
        .unwrap();
    assert_ne!(cache.as_ref().unwrap().plan.generation(), generation);
    assert!(
        request
            .commits
            .iter()
            .all(|commit| commit.commit_id != abandoned)
    );
    let recreated = request
        .ref_updates
        .iter()
        .find(|update| update.branch_id == branch.id)
        .expect("replacement branch ref");
    assert_eq!(
        recreated.head_commit_id.as_deref(),
        Some(main_head.as_str())
    );
    frontier_apply_upload(&authority, &replica, &request).await;
    cache.as_mut().unwrap().acknowledge().unwrap();
}

#[tokio::test]
async fn cached_upload_multiple_branch_deletes_share_one_atomic_generation() {
    let authority = open_lix().await.expect("authority");
    let mut branches = Vec::new();
    for name in ["delete-first", "delete-second"] {
        branches.push(
            authority
                .create_branch(CreateBranchOptions {
                    id: None,
                    name: name.to_owned(),
                    from_commit_id: None,
                })
                .await
                .unwrap()
                .id,
        );
    }
    let snapshot = authority.pull_sync_repository(None, 1).await.unwrap();
    let replica = replica_from_snapshot(&authority, &snapshot).await;
    for i in 0..4 {
        write_key_value(&replica, "pending", &format!("value-{i}")).await;
    }
    let mut cache = None;
    let first = replica
        .build_sync_push_with_plan(TEST_REMOTE, 1, &mut cache)
        .await
        .unwrap()
        .unwrap();
    let generation = cache.as_ref().unwrap().plan.generation();
    frontier_apply_upload(&authority, &replica, &first).await;
    cache.as_mut().unwrap().acknowledge().unwrap();
    replica
        .execute(
            "DELETE FROM lix_branch WHERE id IN ($1, $2)",
            &[
                Value::Text(branches[0].clone()),
                Value::Text(branches[1].clone()),
            ],
        )
        .await
        .expect("multiple branch deletions share one generation key");
    let request = replica
        .build_sync_push_with_plan(TEST_REMOTE, 128, &mut cache)
        .await
        .expect("deletions invalidate the wave")
        .unwrap();
    assert_ne!(cache.as_ref().unwrap().plan.generation(), generation);
    for branch in &branches {
        assert!(
            request
                .ref_updates
                .iter()
                .any(|update| &update.branch_id == branch && update.head_commit_id.is_none())
        );
    }
    frontier_apply_upload(&authority, &replica, &request).await;
    cache.as_mut().unwrap().acknowledge().unwrap();
    assert!(
        authority
            .execute(
                "SELECT id FROM lix_branch WHERE id IN ($1, $2)",
                &[
                    Value::Text(branches[0].clone()),
                    Value::Text(branches[1].clone())
                ],
            )
            .await
            .unwrap()
            .rows()
            .is_empty()
    );
    assert_eq!(read_key_value(&authority, "pending").await, "value-3");
}

#[tokio::test]
async fn cached_upload_recreated_branch_invalidates_an_already_captured_deletion() {
    let authority = open_lix().await.expect("authority");
    let branch = authority
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-000000007913".to_owned()),
            name: "captured-deletion".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let snapshot = authority.pull_sync_repository(None, 1).await.unwrap();
    let replica = replica_from_snapshot(&authority, &snapshot).await;
    for i in 0..8 {
        write_key_value(&replica, "recreated-during-upload", &format!("value-{i}")).await;
    }
    let replacement_head = upload_cache_head(&replica).await;
    replica
        .execute(
            "DELETE FROM lix_branch WHERE id = $1",
            &[Value::Text(branch.id.clone())],
        )
        .await
        .expect("delete an authority-known branch before capturing the wave");
    let mut cache = None;
    let first = replica
        .build_sync_push_with_plan(TEST_REMOTE, 1, &mut cache)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.commits.len(), 1);
    assert!(first.ref_updates.is_empty());
    let plan = &cache.as_ref().unwrap().plan;
    let generation = plan.generation();
    assert!(
        plan.page(128)
            .unwrap()
            .unwrap()
            .ref_updates
            .iter()
            .any(|update| update.branch_id == branch.id && update.head_commit_id.is_none()),
        "the retained wave must already contain the now-obsolete deletion"
    );
    frontier_apply_upload(&authority, &replica, &first).await;
    cache.as_mut().unwrap().acknowledge().unwrap();

    // This is absent-to-headed locally: checking only existing.is_some()
    // misses the destructive change to the previously captured deletion.
    replica
        .create_branch(CreateBranchOptions {
            id: Some(branch.id.clone()),
            name: "recreated-during-upload".to_owned(),
            from_commit_id: Some(replacement_head.clone()),
        })
        .await
        .expect("recreate the same branch while the deletion wave drains");
    let mut rebuilt = false;
    let mut published_replacement = false;
    for _ in 0..32 {
        let Some(request) = replica
            .build_sync_push_with_plan(TEST_REMOTE, 2, &mut cache)
            .await
            .expect("recreated branch rebuilds the retained wave")
        else {
            break;
        };
        if !rebuilt {
            assert_ne!(cache.as_ref().unwrap().plan.generation(), generation);
            rebuilt = true;
        }
        for update in &request.ref_updates {
            if update.branch_id == branch.id {
                assert_eq!(
                    update.head_commit_id.as_deref(),
                    Some(replacement_head.as_str()),
                    "an obsolete deletion must never reach the authority"
                );
                published_replacement = true;
            }
        }
        frontier_apply_upload(&authority, &replica, &request).await;
        cache.as_mut().unwrap().acknowledge().unwrap();
    }
    assert!(rebuilt && published_replacement);
    for lix in [&authority, &replica] {
        assert_eq!(
            lix.execute(
                "SELECT name FROM lix_branch WHERE id = $1",
                &[Value::Text(branch.id.clone())]
            )
            .await
            .unwrap()
            .rows()[0]
                .get::<String>("name")
                .unwrap(),
            "recreated-during-upload"
        );
        lix.switch_branch(SwitchBranchOptions {
            branch_id: branch.id.clone(),
        })
        .await
        .unwrap();
        assert_eq!(upload_cache_head(lix).await, replacement_head);
        assert_eq!(read_key_value(lix, "recreated-during-upload").await, "value-7");
    }
}
