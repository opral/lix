//! Test-only candidate-root read recipe. All immutable fixture bytes are
//! available; this proves logical scope evaluation without publication, not
//! transfer bounds, completed range coverage, or background SQL execution.
use crate::storage::{
    BeginScanOptions, CoreProjection, GetManyRequest, GetManyResult, KeyRange, ProjectedValue,
    ScanCursor, StorageError, StorageSpace,
};
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};

/// A point-only overlay for the two control-plane families. Scanning those
/// families is rejected rather than returning an incoherent merged view.
struct CandidateRead<R> {
    base: R,
    staged: StorageWriteSet,
}
impl<R: StorageAdapterRead> StorageAdapterRead for CandidateRead<R> {
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        let mut loaded = self.base.get_many(requests).await?;
        let mut index = 0;
        for request in requests {
            for key in request.keys {
                if let Some(bytes) = self.staged.staged_value(request.space, &key.0) {
                    loaded.values[index] = Some(match request.opts.projection {
                        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
                        CoreProjection::FullValue => ProjectedValue::FullValue(bytes),
                    });
                }
                index += 1;
            }
        }
        Ok(loaded)
    }
    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        if space == crate::branch::BRANCH_HEAD_CONTROL_SPACE
            || space == crate::hot_state::ROOT_CURRENT_BASE_SPACE
        {
            return Err(StorageError::Io(
                "candidate control scan needs an explicit overlay cursor".into(),
            ));
        }
        self.base.begin_scan(space, range, opts).await
    }
}

#[tokio::test]
async fn candidate_root_prepares_new_match_without_changing_published_current_state() {
    use crate::branch::{BranchHeadControlContext, stage_branch_head_control};
    use crate::changelog::CommitId;
    use crate::commit_graph::CommitGraphContext;
    use crate::hot_state::{
        HotStateContext, HotStateFilter, HotStateReader, HotStateScanRequest, TrackedHeadContext,
    };
    use crate::storage_adapter::StorageWriteOptions;
    use crate::tracked_state::TrackedStateContext;
    use crate::{Memory, open_lix};
    let memory = Memory::new();
    let authority = open_lix().with_storage(memory.clone()).await.unwrap();
    let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
    let branch = descriptor.selected_branch.branch_id.clone();
    let before_read = authority.storage_adapter();
    let read = before_read.begin_read(Default::default()).await.unwrap();
    let old_selected = BranchHeadControlContext::new()
        .reader(&read)
        .load(&branch)
        .await
        .unwrap()
        .unwrap();
    let old_global = BranchHeadControlContext::new()
        .reader(&read)
        .load(crate::GLOBAL_BRANCH_ID)
        .await
        .unwrap()
        .unwrap();
    drop(read);
    authority.execute("INSERT INTO lix_key_value (key, value) VALUES ('future-match', 'new'), ('other-match', 'other')", &[]).await.unwrap();
    let read = before_read.begin_read(Default::default()).await.unwrap();
    let new_selected = BranchHeadControlContext::new()
        .reader(&read)
        .load(&branch)
        .await
        .unwrap()
        .unwrap();
    let new_global = BranchHeadControlContext::new()
        .reader(&read)
        .load(crate::GLOBAL_BRANCH_ID)
        .await
        .unwrap()
        .unwrap();
    drop(read);
    let local = open_lix()
        .with_storage(memory.fork().unwrap())
        .await
        .unwrap();
    let adapter = local.storage_adapter();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let mut baseline = adapter.new_write_set();
    let mut candidate = adapter.new_write_set();
    for (index, (id, old, new)) in [
        (&branch[..], old_selected, new_selected),
        (crate::GLOBAL_BRANCH_ID, old_global, new_global),
    ]
    .into_iter()
    .enumerate()
    {
        for (candidate_flag, control, writes) in
            [(false, old, &mut baseline), (true, new, &mut candidate)]
        {
            let mut control = control;
            // Fresh local serving generations exclude authority HOT overlays;
            // canonical roots remain the actual old/new authority commit IDs.
            control.tracked_generation = CommitId::parse(&format!(
                "00000000-0000-7000-8000-{:012}",
                1100 + index * 2 + usize::from(candidate_flag)
            ))
            .unwrap();
            control.schema_presence_bloom = [u64::MAX; 4];
            stage_branch_head_control(writes, id, control).unwrap();
            TrackedHeadContext::new()
                .writer(&read, writes)
                .stage_root_current_base(id, control.tracked_generation, control.head_commit_id);
        }
    }
    drop(read);
    adapter
        .commit_write_set(baseline, StorageWriteOptions::default())
        .await
        .unwrap();
    let request = HotStateScanRequest {
        filter: HotStateFilter {
            branch_ids: vec![branch.clone()],
            schema_keys: vec!["lix_key_value".into()],
            row_pks: vec![crate::row_pk::RowPk::single("future-match")],
            untracked: Some(false),
            ..Default::default()
        },
        limit: Some(1),
        ..Default::default()
    };
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let published = BranchHeadControlContext::new()
        .reader(&read)
        .load(&branch)
        .await
        .unwrap();
    let hot = || HotStateContext::new(TrackedStateContext::new(), CommitGraphContext::new());
    let old_result = hot().reader(&read).scan_batch(&request).await.unwrap();
    assert_eq!(
        old_result.len(),
        0,
        "negative lookup must retain its recipe"
    );
    let candidate_view = CandidateRead {
        base: &read,
        staged: candidate,
    };
    let candidate_result = hot()
        .reader(candidate_view)
        .scan_batch(&request)
        .await
        .unwrap();
    assert_eq!(
        candidate_result.len(),
        1,
        "same logical request discovers a newly matching native row"
    );
    assert_eq!(
        candidate_result.iter().next().unwrap().row_pk(),
        &crate::row_pk::RowPk::single("future-match")
    );
    assert_eq!(
        request.limit,
        Some(1),
        "preparation recipe retains its requested limit; no complete-range claim"
    );
    assert_eq!(
        BranchHeadControlContext::new()
            .reader(&read)
            .load(&branch)
            .await
            .unwrap(),
        published
    );
    assert_eq!(
        hot()
            .reader(&read)
            .scan_batch(&request)
            .await
            .unwrap()
            .len(),
        0
    );
    drop(read);
    let fresh = adapter.begin_read(Default::default()).await.unwrap();
    assert_eq!(
        BranchHeadControlContext::new()
            .reader(&fresh)
            .load(&branch)
            .await
            .unwrap(),
        published
    );
    assert_eq!(
        hot()
            .reader(&fresh)
            .scan_batch(&request)
            .await
            .unwrap()
            .len(),
        0,
        "candidate preparation must not publish controls or poison live caches"
    );
}
