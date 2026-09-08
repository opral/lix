// Included inside repository.rs's tests module so these fixtures exercise the
// durable acknowledgment receipt without widening production visibility.

async fn frontier_apply_upload(
    authority: &Lix<Memory>,
    replica: &Lix<Memory>,
    request: &SyncPushRequest,
) {
    let receipt = authority
        .push_sync_repository(request)
        .await
        .expect("authority accepts dependency-complete upload");
    let cursor = replica
        .load_sync_repository_cursor(TEST_REMOTE)
        .await
        .expect("replica cursor loads")
        .expect("replica is initialized");
    let delta = authority
        .pull_sync_repository(Some(cursor), super::super::MAX_SYNC_REQUEST_ITEMS)
        .await
        .expect("upload acknowledgment loads");
    assert!(
        matches!(&delta, SyncRepositoryPullResponse::Delta { cursor, .. } if *cursor >= receipt.cursor)
    );
    replica
        .apply_sync_repository_pull(TEST_REMOTE, &delta)
        .await
        .expect("replica persists acknowledgment");
}

#[tokio::test]
async fn fully_acknowledged_checkpoint_waves_retire_source_frontiers() {
    let authority = open_lix().await.expect("authority opens");
    let snapshot = authority
        .pull_sync_repository(None, 1)
        .await
        .expect("snapshot");
    let replica = replica_from_snapshot(&authority, &snapshot).await;
    let mut saw_alias = false;
    for wave in 0..4 {
        write_key_value(&replica, "frontier-checkpoint", &format!("wave-{wave}")).await;
        replica
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .expect("full checkpoint captures working source");
        let mut drained = false;
        for _ in 0..32 {
            let Some(request) = replica
                .build_sync_push(TEST_REMOTE, 1)
                .await
                .expect("page builds")
            else {
                drained = true;
                break;
            };
            assert_eq!(request.commits.len() + request.ref_updates.len(), 1);
            saw_alias |= request
                .commits
                .iter()
                .any(|commit| commit.state_alias.is_some());
            frontier_apply_upload(&authority, &replica, &request).await;
        }
        assert!(
            drained,
            "checkpoint wave must drain within its bounded fixture budget"
        );
        let adapter = replica.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("receipt read");
        let state = load_replica_state(&read)
            .await
            .expect("receipt loads")
            .0
            .expect("receipt exists");
        let controls = BranchHeadControlContext::new()
            .reader(&read)
            .scan()
            .await
            .expect("controls load");
        for (branch, control) in controls {
            let authority = state
                .authoritative_branches
                .get(&branch)
                .expect("branch acknowledged");
            assert_eq!(
                authority.head_commit_id(),
                Some(control.head_commit_id.to_string().as_str())
            );
            assert_eq!(
                authority.checkpoint_commit_id(),
                control
                    .working_diff_checkpoint_commit_id
                    .map(|id| id.to_string())
                    .as_deref()
            );
        }
        assert!(state.pending_resets.is_empty());
        assert!(
            state.authority_known_commit_ids.is_empty(),
            "wave {wave} converged, but captured checkpoint sources still inflate the receipt: {:?}",
            state.authority_known_commit_ids
        );
    }
    assert!(
        saw_alias,
        "fixture must exercise complete-state checkpoint source edges"
    );
}

#[tokio::test]
async fn checkpoint_body_ack_keeps_source_boundary_for_pending_sibling_branch() {
    let authority = open_lix().await.expect("authority opens");
    let snapshot = authority
        .pull_sync_repository(None, 1)
        .await
        .expect("snapshot");
    let replica = replica_from_snapshot(&authority, &snapshot).await;
    write_key_value(&replica, "shared-source", "pending").await;
    let source = replica
        .execute("SELECT lix_active_branch_commit_id() AS head", &[])
        .await
        .expect("working source loads")
        .rows()[0]
        .get::<String>("head")
        .expect("head decodes");
    replica
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("full checkpoint");
    replica
        .create_branch(CreateBranchOptions {
            id: None,
            name: "pending-source-sibling".to_owned(),
            from_commit_id: Some(source.clone()),
        })
        .await
        .expect("sibling points directly to the captured source");
    let mut bodies = replica
        .build_sync_push(TEST_REMOTE, 128)
        .await
        .expect("upload builds")
        .expect("pending checkpoint and sibling");
    assert!(
        bodies.commits.iter().any(|commit| commit
            .state_alias
            .as_ref()
            .is_some_and(|alias| alias.source_commit_id == source)),
        "fixture exports the source alias"
    );
    bodies.ref_updates.clear();
    frontier_apply_upload(&authority, &replica, &bodies).await;
    let remaining = replica
        .build_sync_push(TEST_REMOTE, 128)
        .await
        .expect("remaining upload builds")
        .expect("body acknowledgment must not retire unpublished refs");
    assert!(
        remaining
            .ref_updates
            .iter()
            .any(|update| update.head_commit_id.as_deref() == Some(source.as_str())),
        "pending sibling ref must remain publishable from its acknowledged source boundary"
    );
    assert!(
        !remaining
            .commits
            .iter()
            .any(|commit| commit.commit_id == source),
        "acknowledging a checkpoint must not cause its pending sibling to reupload the captured source"
    );
    frontier_apply_upload(&authority, &replica, &remaining).await;
    assert!(
        replica
            .build_sync_push(TEST_REMOTE, 128)
            .await
            .expect("final upload check")
            .is_none()
    );
}
