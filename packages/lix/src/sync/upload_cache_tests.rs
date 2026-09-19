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
async fn cached_upload_sql_checkpoint_before_first_page_preserves_source_order() {
    for full in [false, true] {
        let authority = open_lix().await.unwrap();
        let snapshot = authority.pull_sync_repository(None, 1).await.unwrap();
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        write_key_value(&replica, "checkpoint-first-a", "selected").await;
        write_key_value(&replica, "checkpoint-first-a", "selected-updated").await;
        write_key_value(&replica, "checkpoint-first-b", "working").await;
        write_key_value(&replica, "checkpoint-first-b", "working-updated").await;
        let source = upload_cache_head(&replica).await;
        replica.execute(if full {
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))"
        } else {
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value') WHERE key='checkpoint-first-a'))"
        }, &[]).await.unwrap();
        let target = upload_cache_head(&replica).await;
        let mut cache = None;
        let mut uploaded = BTreeSet::new();
        let mut claims = 0;
        for _ in 0..32 {
            let Some(request) = replica
                .build_sync_push_with_plan(TEST_REMOTE, 1, &mut cache)
                .await
                .unwrap()
            else {
                break;
            };
            for commit in &request.commits {
                if let Some(source) = &commit.complete_incorporation_source_commit_id {
                    assert!(
                        uploaded.contains(source),
                        "complete source must be accepted before its claimant"
                    );
                    claims += 1;
                    let mut forged = commit.clone();
                    forged.commit_id =
                        CommitId::for_test_label(&format!("forged-added-lifetime-{full}"))
                            .to_string();
                    let selected = forged
                        .members
                        .iter_mut()
                        .find(|member| !member.authored && member.schema_key == "lix_key_value")
                        .unwrap();
                    selected.row_created_at = "2000-01-01T00:00:00Z".to_owned();
                    let error = authority
                        .push_sync_repository(&SyncPushRequest {
                            commits: vec![forged],
                            ref_updates: Vec::new(),
                            inline_blobs: Vec::new(),
                        })
                        .await
                        .unwrap_err();
                    assert_eq!(error.code, LixError::CODE_INVALID_PARAM, "{error:?}");
                    assert!(
                        error.message.contains("complete native source state"),
                        "{error:?}"
                    );
                }
                uploaded.insert(commit.commit_id.clone());
            }
            frontier_apply_upload(&authority, &replica, &request).await;
            cache.as_mut().unwrap().acknowledge().unwrap();
            if cache.as_ref().unwrap().is_complete() {
                break;
            }
        }
        assert!(uploaded.contains(&source));
        assert!(claims > 0);
        assert_eq!(upload_cache_head(&authority).await, target);
    }
}

#[tokio::test]
async fn legacy_checkpoint_known_wire_import_is_independent_of_local_nomination() {
    for physical_alias in [false, true] {
        let memory = Memory::new();
        let authority = open_lix().with_storage(memory.clone()).await.unwrap();
        write_key_value(&authority, "migration-wire", "preserved").await;
        let sql = if physical_alias {
            "SELECT commit_id FROM lix_create_checkpoint()"
        } else {
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))"
        };
        let checkpoint = authority.execute(sql, &[]).await.unwrap().rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        let other = open_lix()
            .with_storage(memory.fork().unwrap())
            .await
            .unwrap();
        let id = CommitId::parse_lix(&checkpoint, "migration checkpoint").unwrap();
        let adapter = other.storage_adapter();
        let mut writes = adapter.new_write_set();
        writes.delete(
            crate::sync::SYNC_CHECKPOINT_SOURCE_SPACE,
            id.as_uuid().as_bytes().to_vec(),
        );
        adapter
            .commit_certified_replica_write_set(
                super::super::certified_replica_write_capability(),
                writes,
                Default::default(),
            )
            .await
            .unwrap();
        for adapter in [authority.storage_adapter(), other.storage_adapter()] {
            crate::migration::downgrade_headers_for_test(&adapter, false).await;
            crate::migration::migrate_headers_for_test(&adapter, false).await;
        }
        let body = export_sync_commit(&other, &checkpoint)
            .await
            .unwrap()
            .unwrap();
        assert!(body.incorporation_unknown);
        assert_eq!(body.state_alias.is_some(), physical_alias);
        assert_eq!(
            body,
            export_sync_commit(&authority, &checkpoint)
                .await
                .unwrap()
                .unwrap()
        );
        authority
            .push_sync_repository(&SyncPushRequest {
                commits: vec![body],
                ref_updates: Vec::new(),
                inline_blobs: Vec::new(),
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn snapshot_omitted_source_survives_gc_then_hydrates_normal_history() {
    for sql in [
        "SELECT commit_id FROM lix_create_checkpoint()",
        "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))",
        "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value') WHERE key='omitted-source'))",
    ] {
        let authority = open_lix().await.unwrap();
        authority.execute("INSERT INTO lix_key_value (key,value) VALUES ('omitted-source','preserved'),('omitted-other','retained')", &[]).await.unwrap();
        let source = upload_cache_head(&authority).await;
        let source_body = export_sync_commit(&authority, &source)
            .await
            .unwrap()
            .unwrap();
        let selected_changes = source_body
            .members
            .iter()
            .map(|member| {
                ChangeId::parse_lix(&member.change_id, "snapshot authored change").unwrap()
            })
            .collect::<Vec<_>>();
        authority.execute(sql, &[]).await.unwrap();
        let head = upload_cache_head(&authority).await;
        let snapshot = authority.pull_sync_repository(None, 1).await.unwrap();
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        let id = CommitId::parse_lix(&source, "omitted source").unwrap();
        let adapter = replica.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let head_id = CommitId::parse_lix(&head, "complete snapshot head").unwrap();
        assert_eq!(
            load_published_commit_state_topology(&read, head_id)
                .await
                .unwrap()
                .unwrap()
                .incorporation(),
            crate::tracked_state::CommitStateIncorporation::Complete(id)
        );
        assert!(
            load_published_commit_state_topology(&read, id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            crate::tracked_state::commit_history_is_omitted(&read, id)
                .await
                .unwrap()
        );
        assert_eq!(
            deferred_commit_global_scope(&read, id).await.unwrap(),
            Some(false)
        );
        let error = load_sync_commit(&read, id).await.unwrap_err();
        assert_eq!(
            crate::tracked_state::NativeMetadataRef::from_missing_error(&error).unwrap(),
            Some(crate::tracked_state::NativeMetadataRef::CommitGraphRecord(
                source.clone()
            ))
        );
        drop(read);
        for pass in 0..2 {
            let read = adapter.begin_read(Default::default()).await.unwrap();
            let standalone = ChangelogContext::new()
                .reader(&read)
                .load_changes(ChangeLoadRequest {
                    change_ids: &selected_changes,
                })
                .await
                .unwrap();
            assert!(
                standalone.iter().all(|(_, record)| record.is_some()),
                "checkpoint mode {sql}, GC pass {pass}: imported canonical standalone payloads must be resident before owner discovery"
            );
            let mut writes = adapter.new_write_set();
            let mut preconditions = Vec::new();
            crate::gc::stage_repository_gc_with_preconditions(
                &read,
                &mut writes,
                &mut preconditions,
            )
            .await
            .unwrap_or_else(|error| panic!("checkpoint mode {sql}, GC pass {pass}: {error:?}"));
            drop(read);
            adapter
                .commit_write_set(
                    writes,
                    StorageWriteOptions {
                        preconditions,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let standalone = ChangelogContext::new()
            .reader(&read)
            .load_changes(ChangeLoadRequest {
                change_ids: &selected_changes,
            })
            .await
            .unwrap();
        assert!(
            standalone.iter().all(|(_, record)| record.is_some()),
            "checkpoint mode {sql}: both GC passes must retain canonical selected payloads"
        );
        assert!(
            load_published_commit_state_topology(&read, id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            crate::tracked_state::commit_history_is_omitted(&read, id)
                .await
                .unwrap()
        );
        assert_eq!(
            load_published_commit_state_topology(&read, head_id)
                .await
                .unwrap()
                .unwrap()
                .incorporation(),
            crate::tracked_state::CommitStateIncorporation::Complete(id)
        );
        drop(read);
        // Migration may know a semantic omission without knowing the source lane.
        let mut writes = adapter.new_write_set();
        crate::tracked_state::stage_commit_history_omitted(&mut writes, id, None);
        adapter
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert!(commit_history_is_deferred(&read, id).await.unwrap());
        assert_eq!(deferred_commit_global_scope(&read, id).await.unwrap(), None);
        drop(read);
        hydrate_history_commit(&authority, &replica, &source).await;
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert!(!commit_history_is_deferred(&read, id).await.unwrap());
        assert_eq!(
            load_sync_commit(&read, id).await.unwrap(),
            Some(source_body)
        );
        drop(read);
        assert_eq!(
            read_key_value(&replica, "omitted-source").await,
            "preserved"
        );
        assert_eq!(read_key_value(&replica, "omitted-other").await, "retained");
    }
}

#[tokio::test]
async fn malformed_deferred_marker_is_not_a_missing_history_permission() {
    let lix = open_lix().await.unwrap();
    let adapter = lix.storage_adapter();
    let id = CommitId::for_test_label("invalid-omission-marker");
    let mut writes = adapter.new_write_set();
    writes.put(
        crate::tracked_state::TRACKED_STATE_COMMIT_HISTORY_DEFERRED_SPACE,
        commit_key(id),
        b"not-a-deferred-state".to_vec(),
    );
    adapter
        .commit_write_set(writes, Default::default())
        .await
        .unwrap();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    assert!(commit_history_is_deferred(&read, id).await.is_err());
    assert!(deferred_commit_global_scope(&read, id).await.is_err());
}

#[tokio::test]
async fn materialized_legacy_alias_is_shared_by_incorporation_and_cycle_proofs() {
    let authority = open_lix().await.unwrap();
    write_key_value(&authority, "materialized-proof", "preserved").await;
    let source = upload_cache_head(&authority).await;
    let checkpoint = authority.create_checkpoint().await.unwrap().commit_id;
    let snapshot = authority.pull_sync_repository(None, 1).await.unwrap();
    let replica = replica_from_snapshot(&authority, &snapshot).await;
    let source = CommitId::parse_lix(&source, "materialized source").unwrap();
    let checkpoint = CommitId::parse_lix(&checkpoint, "materialized checkpoint").unwrap();
    let adapter = replica.storage_adapter();
    crate::migration::mark_header_incorporation_unknown_for_test(&adapter).await;
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let topology = load_published_commit_state_topology(&read, checkpoint)
        .await
        .unwrap()
        .unwrap();
    assert!(topology.complete_state_source_commit_id().is_none());
    assert_eq!(
        crate::sync::commit::load_complete_state_alias_source(&read, checkpoint, None)
            .await
            .unwrap(),
        Some(source)
    );
    let authority_read = authority
        .storage_adapter()
        .begin_read(Default::default())
        .await
        .unwrap();
    let source_record = crate::sync::partial_merge_analysis::record(&authority_read, source, false)
        .await
        .unwrap();
    assert!(
        crate::sync::partial_merge_analysis::incorporated(
            &read,
            &source_record,
            checkpoint,
            &mut BTreeMap::new(),
            32,
        )
        .await
        .unwrap()
    );
    let incoming = BTreeMap::from([(
        source,
        incorporation_cycle::Dependencies {
            edges: vec![checkpoint],
        },
    )]);
    assert!(
        incorporation_cycle::Guard::new([source])
            .run(&read, &incoming, 32)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn authority_rejects_complete_source_cycle_through_existing_deferred_checkpoint() {
    let authority = open_lix().await.unwrap();
    write_key_value(&authority, "cycle-native", "unchanged").await;
    let checkpoint = authority.execute("SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))", &[]).await.unwrap().rows()[0].get::<String>("commit_id").unwrap();
    let mut body = export_sync_commit(&authority, &checkpoint)
        .await
        .unwrap()
        .unwrap();
    let source = authority
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("commit_id")
        .unwrap();
    assert_ne!(checkpoint, source);
    body.complete_incorporation_source_commit_id = Some(source);
    let id = CommitId::parse_lix(&checkpoint, "cycle checkpoint").unwrap();
    let adapter = authority.storage_adapter();
    let mut writes = adapter.new_write_set();
    writes.delete(
        crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        id.as_uuid().as_bytes().to_vec(),
    );
    writes.delete(
        crate::tracked_state::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
        id.as_uuid().as_bytes().to_vec(),
    );
    stage_commit_history_deferred_with_scope(&mut writes, id, body.global_scope);
    adapter
        .commit_certified_replica_write_set(
            super::super::certified_replica_write_capability(),
            writes,
            Default::default(),
        )
        .await
        .unwrap();
    let error = authority
        .push_sync_repository(&SyncPushRequest {
            commits: vec![body],
            ref_updates: Vec::new(),
            inline_blobs: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_INVALID_PARAM, "{error:?}");
    assert!(error.message.contains("incorporation cycle"), "{error:?}");
}

#[tokio::test]
async fn checkpoint_source_certificate_rejects_modified_lifetime_and_rootless_value_changes() {
    let authority = open_lix().await.unwrap();
    write_key_value(&authority, "lifetime-existing", "base").await;
    let base = authority.create_checkpoint().await.unwrap().commit_id;
    write_key_value(&authority, "lifetime-existing", "modified").await;
    let source = upload_cache_head(&authority).await;
    let adapter = authority.storage_adapter();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let context = TrackedStateContext::new();
    let rows = context
        .reader(&read)
        .scan_batch_at_commit(&source, &TrackedStateScanRequest::default())
        .await
        .unwrap();
    let forged = CommitId::for_test_label("forged-modified-native-lifetime");
    let mut scratch = adapter.new_write_set();
    let mut writer = context.writer(&read, &mut scratch);
    writer
        .stage_commit_root(
            &forged.to_string(),
            None,
            rows.iter().map(|row| TrackedStateDeltaRef {
                schema_key: row.schema_key(),
                file_id: row.file_id(),
                row_pk: row.row_pk(),
                change_id: row.change_id(),
                commit_id: forged,
                deleted: row.deleted(),
                created_at: row.updated_at(),
                updated_at: row.updated_at(),
            }),
        )
        .await
        .unwrap();
    assert!(
        !writer
            .complete_state_matches_source(
                forged,
                CommitId::parse_lix(&source, "source").unwrap(),
                Some(CommitId::parse_lix(&base, "base").unwrap())
            )
            .await
            .unwrap()
    );
    drop(writer);
    drop(read);
    authority.create_checkpoint().await.unwrap();
    write_key_value(&authority, "rootless-other-key", "different").await;
    let other = upload_cache_head(&authority).await;
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let mut scratch = adapter.new_write_set();
    assert!(
        !context
            .writer(&read, &mut scratch)
            .complete_state_matches_source(
                CommitId::parse_lix(&source, "left").unwrap(),
                CommitId::parse_lix(&other, "right").unwrap(),
                None
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn checkpoint_source_certificate_preserves_recreated_row_lifetimes() {
    for deleted_base in [false, true] {
        for full in [false, true] {
            let authority = open_lix().await.unwrap();
            write_key_value(&authority, "recreated", "base").await;
            if deleted_base {
                authority
                    .execute("DELETE FROM lix_key_value WHERE key='recreated'", &[])
                    .await
                    .unwrap();
            }
            authority.create_checkpoint().await.unwrap();
            let snapshot = authority.pull_sync_repository(None, 1).await.unwrap();
            let replica = replica_from_snapshot(&authority, &snapshot).await;
            if !deleted_base {
                replica
                    .execute("DELETE FROM lix_key_value WHERE key='recreated'", &[])
                    .await
                    .unwrap();
            }
            write_key_value(&replica, "recreated", "new-lifetime").await;
            write_key_value(&replica, "recreated", "modified-again").await;
            write_key_value(&replica, "unselected-lifetime", "working").await;
            write_key_value(&replica, "transient-before-checkpoint", "temporary").await;
            replica
                .execute(
                    "DELETE FROM lix_key_value WHERE key='transient-before-checkpoint'",
                    &[],
                )
                .await
                .unwrap();
            replica.execute(if full {
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))"
            } else {
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value') WHERE key='recreated'))"
            }, &[]).await.unwrap();
            let target = upload_cache_head(&replica).await;
            let mut cache = None;
            for _ in 0..32 {
                let Some(request) = replica
                    .build_sync_push_with_plan(TEST_REMOTE, 1, &mut cache)
                    .await
                    .unwrap()
                else {
                    break;
                };
                frontier_apply_upload(&authority, &replica, &request).await;
                cache.as_mut().unwrap().acknowledge().unwrap();
                if cache.as_ref().unwrap().is_complete() {
                    break;
                }
            }
            assert_eq!(upload_cache_head(&authority).await, target);
        }
    }
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
    reset_branch_for_test(&replica, &target).await;
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
        assert_eq!(
            read_key_value(lix, "recreated-during-upload").await,
            "value-7"
        );
    }
}
