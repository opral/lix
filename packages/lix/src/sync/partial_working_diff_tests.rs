//! Native root-backed working-diff regression; complete fixture bytes are
//! present. This tests the index-coverage distinction, not hydration bounds.
#[tokio::test]
async fn root_backed_dirty_base_remains_visible_with_empty_overlay_coverage() {
    use crate::branch::{BranchHeadControlContext, stage_branch_head_control};
    use crate::hot_state::{
        TrackedHeadContext, TrackedWorkingDiffEpoch, WorkingDiffIndexCoverage,
        stage_tracked_working_diff_epoch,
    };
    use crate::storage_adapter::StorageWriteOptions;
    let lix = crate::open_lix().await.unwrap();
    lix.execute("INSERT INTO lix_key_value (key, value) VALUES ('base-dirty', 'original'), ('local-edit', 'before')", &[]).await.unwrap();
    let branch = lix
        .partial_replica_descriptor(None)
        .await
        .unwrap()
        .selected_branch
        .branch_id;
    let adapter = lix.storage_adapter();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let mut control = BranchHeadControlContext::new()
        .reader(&read)
        .load(&branch)
        .await
        .unwrap()
        .unwrap();
    let checkpoint = control.working_diff_checkpoint_commit_id.unwrap();
    assert_ne!(control.head_commit_id, checkpoint);
    control.tracked_generation =
        crate::changelog::CommitId::parse("00000000-0000-7000-8000-000000004001").unwrap();
    control.schema_presence_bloom = [u64::MAX; 4];
    let mut writes = adapter.new_write_set();
    stage_branch_head_control(&mut writes, &branch, control).unwrap();
    TrackedHeadContext::new()
        .writer(&read, &mut writes)
        .stage_root_current_base(&branch, control.tracked_generation, control.head_commit_id);
    stage_tracked_working_diff_epoch(
        &mut writes,
        &branch,
        TrackedWorkingDiffEpoch {
            checkpoint_commit_id: checkpoint,
            generation: control.tracked_generation,
            coverage: WorkingDiffIndexCoverage::default(),
        },
    )
    .unwrap();
    drop(read);
    adapter
        .commit_write_set(writes, StorageWriteOptions::default())
        .await
        .unwrap();
    let query = "SELECT diff_type FROM lix_diff('lix_key_value') WHERE key = 'base-dirty'";
    assert_eq!(
        lix.execute(query, &[]).await.unwrap().len(),
        1,
        "empty overlay index cannot hide dirty immutable baseline"
    );
    lix.execute(
        "UPDATE lix_key_value SET value = 'after' WHERE key = 'local-edit'",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        lix.execute(query, &[]).await.unwrap().len(),
        1,
        "unrelated local edit must retain baseline dirty identity"
    );
    assert_eq!(
        lix.execute(
            "SELECT diff_type FROM lix_diff('lix_key_value') WHERE key = 'local-edit'",
            &[]
        )
        .await
        .unwrap()
        .len(),
        1
    );
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let control = BranchHeadControlContext::new()
        .reader(&read)
        .load(&branch)
        .await
        .unwrap()
        .unwrap();
    assert!(
        TrackedHeadContext::new()
            .reader(&read)
            .root_current_base_commit(&branch, control.tracked_generation)
            .await
            .unwrap()
            .is_some(),
        "fixture must remain root-backed after local edit"
    );
}

#[tokio::test]
async fn missing_local_standalone_ref_remains_corruption_while_remote_cache_retirement_is_optional()
{
    use crate::storage_adapter::{StorageAdapter, StorageWriteOptions};
    let adapter = StorageAdapter::new(crate::Memory::new());
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let missing =
        crate::changelog::ChangeId::parse("00000000-0000-7000-8000-000000004002").unwrap();
    let mut local = adapter.new_write_set();
    let error = crate::changelog::stage_delete_standalone_change(&read, &mut local, missing)
        .await
        .unwrap_err();
    assert_eq!(error.code, crate::LixError::CODE_INTERNAL_ERROR);
    assert!(error.message.contains("missing standalone change"));
    let mut remote = adapter.new_write_set();
    crate::changelog::stage_delete_cached_standalone_change(&mut remote, missing);
    drop(read);
    adapter
        .commit_write_set(remote, StorageWriteOptions::default())
        .await
        .unwrap();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let mut still_local = adapter.new_write_set();
    assert!(
        crate::changelog::stage_delete_standalone_change(&read, &mut still_local, missing)
            .await
            .is_err(),
        "optional remote cache retirement must never make strict local retirement permissive"
    );
}
