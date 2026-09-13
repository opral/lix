// Included beside the real snapshot/history fixtures in repository tests.

#[tokio::test]
async fn materialized_legacy_alias_gc_preserves_resident_proof_without_hydrating_history() {
    let authority = open_lix().await.unwrap();
    write_key_value(&authority, "alias-gc", "anchor").await;
    let anchor = upload_cache_head(&authority).await;
    write_key_value(&authority, "alias-gc", "temporary").await;
    let obsolete = upload_cache_head(&authority).await;
    write_key_value(&authority, "alias-gc", "current").await;
    let source = upload_cache_head(&authority).await;
    let checkpoint = authority.create_checkpoint().await.unwrap().commit_id;
    let snapshot = authority.pull_sync_repository(None, 1).await.unwrap();
    let obsolete_id = CommitId::parse_lix(&obsolete, "obsolete alias ancestor").unwrap();
    let source_id = CommitId::parse_lix(&source, "alias source").unwrap();
    let checkpoint_id = CommitId::parse_lix(&checkpoint, "materialized alias").unwrap();
    let authority_adapter = authority.storage_adapter();
    let authority_read = authority_adapter
        .begin_read(Default::default())
        .await
        .unwrap();
    let obsolete_record =
        crate::sync::partial_merge_analysis::record(&authority_read, obsolete_id, false)
            .await
            .unwrap();
    drop(authority_read);

    for hydrated in [false, true] {
        let replica = replica_from_snapshot(&authority, &snapshot).await;
        if hydrated {
            for id in [&anchor, &obsolete, &source] {
                hydrate_history_commit(&authority, &replica, id).await;
            }
        }
        let adapter = replica.storage_adapter();
        crate::migration::mark_header_incorporation_unknown_for_test(&adapter).await;
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let topology = load_published_commit_state_topology(&read, checkpoint_id)
            .await
            .unwrap()
            .unwrap();
        assert!(topology.complete_state_source_commit_id().is_none());
        assert_eq!(
            crate::sync::load_complete_state_alias_source(&read, checkpoint_id, None)
                .await
                .unwrap(),
            Some(source_id)
        );
        assert_eq!(
            load_published_commit_state_topology(&read, source_id)
                .await
                .unwrap()
                .is_some(),
            hydrated
        );
        let projections = BranchHeadControlContext::new()
            .reader(&read)
            .scan()
            .await
            .unwrap()
            .into_iter()
            .map(|(branch, control)| (branch, control.tracked_reachability()))
            .collect::<Vec<_>>();
        let provenance = TrackedHeadContext::new()
            .reader(&read)
            .tracked_serving_commit_dependencies(&projections)
            .await
            .unwrap();
        assert!(
            provenance.contains(&source_id),
            "materialized checkpoint {checkpoint_id} must retain semantic row source {source_id}"
        );
        for id in provenance {
            if load_commit_state_manifest(&read, id)
                .await
                .unwrap()
                .is_none()
            {
                assert!(
                    commit_history_is_deferred(&read, id).await.unwrap(),
                    "missing semantic provenance {id} (checkpoint={checkpoint_id}, source={source_id}, obsolete={obsolete_id}) must be explicitly deferred, not a missing physical owner"
                );
            }
        }
        if !hydrated {
            assert!(
                commit_history_is_deferred(&read, source_id).await.unwrap(),
                "snapshot source S={source_id} must be a deliberately deferred history projection"
            );
        }
        if hydrated {
            assert!(
                crate::sync::partial_merge_analysis::incorporated(
                    &read,
                    &obsolete_record,
                    checkpoint_id,
                    &mut BTreeMap::new(),
                    32
                )
                .await
                .unwrap()
            );
        }
        drop(read);

        for _ in 0..2 {
            let read = adapter.begin_read(Default::default()).await.unwrap();
            let mut writes = adapter.new_write_set();
            let mut preconditions = Vec::new();
            let plan = crate::gc::stage_repository_gc_with_preconditions(
                &read,
                &mut writes,
                &mut preconditions,
            )
            .await
            .unwrap();
            assert!(!plan.sweep.has_more);
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
        assert_eq!(
            load_published_commit_state_topology(&read, source_id)
                .await
                .unwrap()
                .is_some(),
            hydrated,
            "GC must neither fetch absent source history nor erase resident proof"
        );
        if hydrated {
            assert!(
                crate::sync::partial_merge_analysis::incorporated(
                    &read,
                    &obsolete_record,
                    checkpoint_id,
                    &mut BTreeMap::new(),
                    32
                )
                .await
                .unwrap()
            );
            assert!(
                !crate::tracked_state::scan_commit_delta_inventory(&read)
                    .await
                    .unwrap()
                    .commits
                    .contains_key(&obsolete_id),
                "obsolete payload should retire while incorporation proof survives"
            );
        }
    }
}
