//! Bounded ordinary local-chain export for partial replication.
//! This deliberately rejects checkpoint/merge preparation until their
//! native dependency closures are represented. It never walks beyond confirmed
//! authority boundaries or reads whole branch state.
use super::partial_push_state::{
    PartialPushCoordinate, PreparedPartialUpload, load_partial_push_state,
};
use super::partial_state::PartialReplicaState;
use super::protocol::{SyncPushRequest, SyncRefUpdate};
use crate::LixError;
use crate::changelog::{
    ChangelogContext, ChangelogReader, CommitId, CommitLoadRequest, CommitRecord,
};
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition};
use std::collections::BTreeSet;

pub(super) struct PreparedPartialPush {
    pub(super) upload: PreparedPartialUpload,
    pub(super) request: SyncPushRequest,
    /// Commit with preparation. A newer local publication can be planned on
    /// the next cycle, but must not be mistaken for the captured target.
    pub(super) control_guard: Option<StoragePrecondition>,
    pub(super) encoded_bytes: usize,
}
fn blocked(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_UPLOAD_PREPARATION_REQUIRED", message)
}
fn id(value: &str) -> Result<CommitId, LixError> {
    CommitId::parse_lix(value, "partial upload commit")
}
struct ByteBudget {
    remaining: usize,
    written: usize,
}
impl std::io::Write for ByteBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(std::io::Error::other("partial upload byte budget exceeded"));
        }
        self.remaining -= bytes.len();
        self.written += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Locally authored graph records must remain durable until upload. A missing
/// one is not a demand for an object the authority has never received.
pub(super) async fn local_record(
    read: &(impl StorageAdapterRead + ?Sized),
    commit: CommitId,
) -> Result<CommitRecord, LixError> {
    ChangelogContext::new()
        .reader(read)
        .load_commits(CommitLoadRequest {
            commit_ids: &[commit],
        })
        .await?
        .into_iter()
        .next()
        .and_then(|(_, value)| value)
        .ok_or_else(|| blocked("prepared upload graph record is missing locally"))
}

/// Select the oldest bounded wave using the native linear jump index. Work
/// grows with the local suffix's index height, never authority history size.
pub(super) async fn wave_target(
    read: &(impl StorageAdapterRead + ?Sized),
    head: CommitId,
    boundary: CommitId,
    max_commits: usize,
) -> Result<CommitId, LixError> {
    let base = local_record(read, boundary).await?;
    let mut current = local_record(read, head).await?;
    let goal = base
        .generation
        .checked_add(max_commits as u64)
        .ok_or_else(|| blocked("upload generation overflow"))?;
    if current.generation <= goal {
        return Ok(head);
    }
    for _ in 0..256 {
        if current.generation == goal {
            return Ok(current.commit_id);
        }
        if current.generation < goal
            || current.is_checkpoint
            || current.parent_commit_ids.len() != 1
        {
            return Err(blocked("bounded upload requires a linear ordinary suffix"));
        }
        let jump_generation = current
            .generation
            .checked_sub(current.first_parent_jump_span)
            .ok_or_else(|| blocked("invalid local upload jump span"))?;
        let (next, expected) = if current.first_parent_jump_span > 0 && jump_generation >= goal {
            (current.first_parent_jump_commit_id, jump_generation)
        } else {
            (current.parent_commit_ids[0], current.generation - 1)
        };
        if next == current.commit_id {
            return Err(blocked("local upload jump contains a cycle"));
        }
        current = local_record(read, next).await?;
        if current.generation != expected {
            return Err(blocked("local upload jump generation mismatch"));
        }
    }
    Err(blocked("local upload jump traversal exceeded its bound"))
}

/// Plan one selected-branch suffix from a coherent local snapshot. Targets
/// are observed locally, never supplied by the network. Persist `upload` plus
/// `control_guard` before sending, then acknowledge only that exact attempt.
/// The byte budget bounds wire output, not native decode allocation for one
/// commit; oversized user commits require the separate multipart upload lane.
pub(super) async fn prepare_partial_ordinary_upload(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
    branch_id: &str,
    attempt_id: String,
    max_commits: usize,
    max_wire_bytes: usize,
) -> Result<Option<PreparedPartialPush>, LixError> {
    prepare_partial_ordinary_budgeted(
        read,
        state,
        branch_id,
        attempt_id,
        max_commits,
        max_wire_bytes,
        None,
    )
    .await
}

/// Publish only ordinary ancestors of a locally captured checkpoint source.
/// The authority's checkpoint remains unchanged until its full closure fits.
pub(super) async fn prepare_partial_checkpoint_prefix(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
    branch_id: &str,
    attempt_id: String,
    max_commits: usize,
    max_wire_bytes: usize,
    checkpoint: CommitId,
) -> Result<Option<PreparedPartialPush>, LixError> {
    prepare_partial_ordinary_budgeted(
        read,
        state,
        branch_id,
        attempt_id,
        max_commits,
        max_wire_bytes,
        Some(checkpoint),
    )
    .await
}

async fn prepare_partial_ordinary_budgeted(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
    branch_id: &str,
    attempt_id: String,
    max_commits: usize,
    max_wire_bytes: usize,
    checkpoint_prefix: Option<CommitId>,
) -> Result<Option<PreparedPartialPush>, LixError> {
    let can_shrink = load_partial_push_state(read, state, branch_id)
        .await?
        .0
        .prepared
        .is_none();
    let mut limit = max_commits;
    loop {
        match prepare_partial_ordinary_upload_inner(
            read,
            state,
            branch_id,
            attempt_id.clone(),
            limit,
            max_wire_bytes,
            checkpoint_prefix,
        )
        .await
        {
            Err(error)
                if error.code == "LIX_PARTIAL_UPLOAD_PAGE_REQUIRED" && can_shrink && limit > 1 =>
            {
                limit = (limit / 2).max(1);
            }
            Err(error) if error.code == "LIX_PARTIAL_UPLOAD_PAGE_REQUIRED" => {
                return Err(blocked(if can_shrink {
                    "one native commit exceeds the upload request budget; multipart body preparation is required"
                } else {
                    "captured upload exceeds the requested byte budget; retry the immutable attempt with its original supported budget"
                }));
            }
            result => return result,
        }
    }
}

async fn prepare_partial_ordinary_upload_inner(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
    branch_id: &str,
    attempt_id: String,
    max_commits: usize,
    max_wire_bytes: usize,
    checkpoint_prefix: Option<CommitId>,
) -> Result<Option<PreparedPartialPush>, LixError> {
    if max_commits == 0 || max_commits > super::MAX_SYNC_REQUEST_ITEMS || max_wire_bytes == 0 {
        return Err(blocked("invalid partial upload work budget"));
    }
    if crate::storage_codec::id_string::uuid_bytes_from_canonical(&attempt_id).is_none() {
        return Err(blocked("upload attempt must be a canonical UUID"));
    }
    let (branch_state, _, _) = load_partial_push_state(read, state, branch_id).await?;
    // A durable attempt owns an immutable target. Later local edits must not
    // change a retry's request, including after an ambiguous network outcome.
    let resumed = branch_state.prepared.clone();
    let branches = [branch_id.to_owned()];
    let observation = crate::branch::BranchHeadControlContext::new()
        .reader(read)
        .load_observed(&branches)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| blocked("missing local branch observation"))?;
    let control = observation
        .control
        .ok_or_else(|| blocked("partial upload branch was deleted"))?;
    let checkpoint = control
        .working_diff_checkpoint_commit_id
        .ok_or_else(|| blocked("partial upload branch has no checkpoint"))?;
    let mut target = resumed
        .as_ref()
        .map(|upload| upload.target.clone())
        .unwrap_or_else(|| PartialPushCoordinate {
            head: control.head_commit_id.to_string(),
            checkpoint: checkpoint.to_string(),
        });
    if let Some(checkpoint) = checkpoint_prefix {
        if resumed.is_some() {
            return Err(blocked(
                "checkpoint prefix cannot replace a durable upload attempt",
            ));
        }
        let (source_branch, source) = super::commit::load_sync_checkpoint_source(read, checkpoint)
            .await?
            .ok_or_else(|| blocked("checkpoint prefix requires its captured native source"))?;
        if source_branch != branch_id {
            return Err(blocked("checkpoint source belongs to another branch"));
        }
        target.head = source.to_string();
        target.checkpoint = branch_state.confirmed.checkpoint.clone();
    }
    if target == branch_state.confirmed {
        return Ok(None);
    }
    if target.checkpoint != branch_state.confirmed.checkpoint {
        return Err(blocked(
            "checkpoint publication requires captured source preparation",
        ));
    }
    let (global, _, _) = load_partial_push_state(read, state, crate::GLOBAL_BRANCH_ID).await?;
    let boundary = id(&branch_state.confirmed.head)?;
    if resumed.is_none() {
        target.head = wave_target(read, id(&target.head)?, boundary, max_commits)
            .await?
            .to_string();
    }
    let mut known = BTreeSet::from([
        boundary,
        id(&branch_state.confirmed.checkpoint)?,
        id(&global.confirmed.head)?,
        id(&global.confirmed.checkpoint)?,
        id(&state.descriptor().global_branch.head.commit_id)?,
        id(&state.descriptor().global_branch.checkpoint.commit_id)?,
    ]);
    if branch_id != crate::GLOBAL_BRANCH_ID {
        known.extend(
            super::partial_global_merge_state::confirmed_global_merge_bases(read, state).await?,
        );
    }

    let mut records = Vec::new();
    let mut seen = BTreeSet::new();
    let mut current = id(&target.head)?;
    while current != boundary {
        if records.len() == max_commits {
            return Err(blocked(
                "local suffix exceeds bounded upload; prepare a paged body wave",
            ));
        }
        if !seen.insert(current) {
            return Err(blocked("local upload chain contains a cycle"));
        }
        let record = local_record(read, current).await?;
        if record.is_checkpoint || record.parent_commit_ids.len() != 1 {
            return Err(blocked(
                "ordinary upload requires a linear non-checkpoint suffix to confirmed head",
            ));
        }
        if record.account_id != state.active_account_id() {
            return Err(blocked(
                "local suffix contains a commit outside admitted account",
            ));
        }
        current = record.parent_commit_ids[0];
        records.push(record);
    }
    records.reverse();
    let mut commits: Vec<super::commit::SyncCommit> = Vec::with_capacity(records.len());
    let mut budget = ByteBudget {
        remaining: max_wire_bytes,
        written: 0,
    };
    let mut global_ancestry = std::collections::BTreeMap::new();
    for record in records {
        // Preparation proved this immutable suffix's dependencies before the
        // tuple was persisted. A later global ACK can retire that coordinate
        // from the tiny confirmed frontier; it does not undo the proof.
        if resumed.is_none()
            && branch_id != crate::GLOBAL_BRANCH_ID
            && let Some(base) = record.base_commit_id.filter(|base| !known.contains(base))
            && is_confirmed_global_base(
                read,
                base,
                id(&global.confirmed.head)?,
                &mut global_ancestry,
            )
            .await?
        {
            known.insert(base);
        }
        if resumed.is_none()
            && record
                .base_commit_id
                .is_some_and(|base| !known.contains(&base))
        {
            if branch_id != crate::GLOBAL_BRANCH_ID && !commits.is_empty() {
                // S may supply a child in GLOBAL L2, while a later S2 already
                // depends on L2. Freeze the eligible selected prefix first.
                target.head = commits
                    .last()
                    .expect("nonempty selected prefix")
                    .commit_id
                    .clone();
                break;
            }
            return Err(blocked(
                "local commit requires unconfirmed global base preparation",
            ));
        }
        let commit = super::commit::load_sync_commit(read, record.commit_id)
            .await?
            .ok_or_else(|| blocked("prepared local commit disappeared"))?;
        if commit.is_checkpoint
            || commit.state_alias.is_some()
            || commit.selected_source_commit_id.is_some()
        {
            return Err(blocked(
                "local commit requires physical source closure preparation",
            ));
        }
        serde_json::to_writer(&mut budget, &commit).map_err(|_| {
            LixError::new(
                "LIX_PARTIAL_UPLOAD_PAGE_REQUIRED",
                "local suffix exceeds wire byte budget",
            )
        })?;
        known.insert(record.commit_id);
        commits.push(commit);
    }
    let is_resume = resumed.is_some();
    let mut upload = resumed.unwrap_or_else(|| PreparedPartialUpload {
        attempt_id,
        created_refs: Vec::new(),
        expected: branch_state.confirmed,
        target,
    });
    if !is_resume {
        loop {
            match super::partial_created_refs::capture_created_refs(
                read,
                state,
                branch_id,
                &upload.expected,
                &upload.target,
                &commits,
            )
            .await
            {
                Ok(created) => {
                    upload.created_refs = created;
                    break;
                }
                Err(error)
                    if branch_id == crate::GLOBAL_BRANCH_ID
                        && error.code == "LIX_PARTIAL_CREATED_REF_SOURCE_PENDING"
                        && commits.len() > 1 =>
                {
                    // Freeze a nonempty eligible prefix. Publishing it can make
                    // a selected commit's GLOBAL basis available before that
                    // selected commit supplies a later child's source head.
                    commits.pop();
                    upload.target.head = commits.last().expect("nonempty prefix").commit_id.clone();
                }
                Err(error) => return Err(error),
            }
        }
    }
    let mut request = SyncPushRequest {
        commits,
        ref_updates: vec![SyncRefUpdate {
            branch_id: branch_id.into(),
            expected_head_commit_id: Some(upload.expected.head.clone()),
            expected_checkpoint_commit_id: Some(upload.expected.checkpoint.clone()),
            head_commit_id: Some(upload.target.head.clone()),
            checkpoint_commit_id: Some(upload.target.checkpoint.clone()),
        }],
        inline_blobs: Vec::new(),
    };
    upload.append_created_ref_updates(&mut request);
    // Include commas, field names and refs in the actual request budget.
    let mut budget = ByteBudget {
        remaining: max_wire_bytes,
        written: 0,
    };
    serde_json::to_writer(&mut budget, &request).map_err(|_| {
        LixError::new(
            "LIX_PARTIAL_UPLOAD_PAGE_REQUIRED",
            "partial upload request exceeds wire byte budget",
        )
    })?;
    Ok(Some(PreparedPartialPush {
        upload,
        request,
        control_guard: if is_resume {
            None
        } else {
            Some(crate::branch::branch_head_control_precondition(
                branch_id,
                observation.raw_token,
            )?)
        },
        encoded_bytes: budget.written,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn ordinary_upload_exports_only_local_suffix_with_explicit_budgets() {
        let lix = crate::open_lix().await.unwrap();
        let descriptor = lix.partial_replica_descriptor(None).await.unwrap();
        let branch = descriptor.selected_branch.branch_id.clone();
        let confirmed = descriptor.selected_branch.head.commit_id.clone();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", lix.lix_id()),
            lix.active_account_id().into(),
            "00000000-0000-7000-8000-000000002001".into(),
            descriptor,
        )
        .unwrap();
        for index in 0..3 {
            lix.execute(
                &format!(
                    "INSERT INTO lix_key_value (key, value) VALUES ('upload-{index}', 'value')"
                ),
                &[],
            )
            .await
            .unwrap();
        }
        let adapter = lix.storage_adapter();
        let mut writes = adapter.new_write_set();
        let mut guards = super::super::partial_push_state::stage_initial_partial_push_states(
            &mut writes,
            &state,
        )
        .unwrap();
        guards.push(
            super::super::partial_state::stage_partial_replica_state(&mut writes, &state, None)
                .unwrap(),
        );
        adapter
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let attempt = "00000000-0000-7000-8000-000000002002";
        let prepared =
            prepare_partial_ordinary_upload(&read, &state, &branch, attempt.into(), 3, 1024 * 1024)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(prepared.request.commits.len(), 3);
        assert_eq!(
            prepared.request.commits[0].parent_commit_ids,
            vec![confirmed.clone()]
        );
        assert_eq!(prepared.upload.expected.head, confirmed);
        assert_eq!(
            prepared.upload.target.head,
            prepared.request.commits[2].commit_id
        );
        assert_eq!(
            prepared.encoded_bytes,
            serde_json::to_vec(&prepared.request).unwrap().len()
        );
        let wave =
            prepare_partial_ordinary_upload(&read, &state, &branch, attempt.into(), 2, 1024 * 1024)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(wave.request.commits.len(), 2);
        assert_eq!(
            wave.upload.target.head,
            prepared.request.commits[1].commit_id
        );
        assert_ne!(wave.upload.target.head, prepared.upload.target.head);
        assert!(
            prepare_partial_ordinary_upload(&read, &state, &branch, attempt.into(), 3, 1)
                .await
                .is_err()
        );
        assert!(
            load_partial_push_state(&read, &state, &branch)
                .await
                .unwrap()
                .0
                .prepared
                .is_none(),
            "planning itself must not publish preparation or acknowledge a ref"
        );
    }
}

#[cfg(test)]
mod paging_and_resume_tests {
    use super::*;
    use crate::storage_adapter::{StorageAdapter, StorageWriteOptions};

    #[tokio::test]
    async fn native_jump_pages_1024_local_commits_without_authority_ancestry() {
        let storage = StorageAdapter::new(crate::Memory::new());
        let mut records: Vec<CommitRecord> = Vec::with_capacity(1025);
        for index in 0..=1024usize {
            let commit_id = CommitId::for_test_label(&format!("partial-upload-wave-{index}"));
            let parents = if index == 0 {
                vec![]
            } else {
                vec![records[index - 1].commit_id]
            };
            let parent = index.checked_sub(1).map(|i| &records[i]);
            let jump = parent.map(|parent| {
                records
                    .iter()
                    .find(|record| record.commit_id == parent.first_parent_jump_commit_id)
                    .unwrap()
            });
            let (jump_id, jump_span) =
                crate::changelog::next_first_parent_jump(commit_id, &parents, parent, jump)
                    .unwrap();
            records.push(CommitRecord {
                format_version: crate::changelog::COMMIT_RECORD_FORMAT_VERSION,
                commit_id,
                generation: index as u64,
                parent_commit_ids: parents,
                base_commit_id: None,
                first_parent_jump_commit_id: jump_id,
                first_parent_jump_span: jump_span,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.into(),
                created_at: crate::common::LixTimestamp::parse("2026-08-11T00:00:00Z").unwrap(),
                touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                is_checkpoint: false,
            });
        }
        let mut writes = storage.new_write_set();
        for record in &records {
            writes.put(
                crate::changelog::COMMIT_SPACE,
                crate::changelog::commit_key(record.commit_id),
                crate::changelog::encode_commit_record(record).unwrap(),
            );
        }
        storage
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        // Only the boundary and the locally authored suffix exist. No rows,
        // commit state, or authority ancestry are available to this planner.
        for boundary in (0..1024usize).step_by(32) {
            let target = wave_target(
                &read,
                records[1024].commit_id,
                records[boundary].commit_id,
                32,
            )
            .await
            .unwrap();
            assert_eq!(target, records[boundary + 32].commit_id);
        }
        assert_eq!(
            wave_target(&read, records[1024].commit_id, records[1000].commit_id, 32)
                .await
                .unwrap(),
            records[1024].commit_id
        );
    }

    async fn confirm_global<S: crate::storage_adapter::Storage + Clone + Send + Sync + 'static>(
        storage: &StorageAdapter<S>,
        state: &PartialReplicaState,
        coordinate: PartialPushCoordinate,
        attempt_id: &str,
    ) {
        use super::super::partial_push_state::{
            stage_acknowledge_partial_upload, stage_prepare_partial_upload,
        };
        let read = storage.begin_read(Default::default()).await.unwrap();
        let expected = load_partial_push_state(&read, state, crate::GLOBAL_BRANCH_ID)
            .await
            .unwrap()
            .0
            .confirmed;
        let upload = PreparedPartialUpload {
            created_refs: Vec::new(),
            attempt_id: attempt_id.into(),
            expected,
            target: coordinate,
        };
        let mut writes = storage.new_write_set();
        let guards = stage_prepare_partial_upload(
            &read,
            &mut writes,
            state,
            crate::GLOBAL_BRANCH_ID,
            &upload,
        )
        .await
        .unwrap();
        drop(read);
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let guards = stage_acknowledge_partial_upload(
            &read,
            &mut writes,
            state,
            crate::GLOBAL_BRANCH_ID,
            &upload,
            true,
        )
        .await
        .unwrap();
        drop(read);
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn resumed_capture_survives_retirement_of_its_confirmed_global_base() {
        let lix = crate::open_lix().await.unwrap();
        let descriptor = lix.partial_replica_descriptor(None).await.unwrap();
        let branch = descriptor.selected_branch.branch_id.clone();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", lix.lix_id()),
            lix.active_account_id().into(),
            "00000000-0000-7000-8000-000000004200".into(),
            descriptor,
        )
        .unwrap();
        let global = lix
            .open_another_session()
            .with_branch(crate::GLOBAL_BRANCH_ID)
            .await
            .unwrap();
        global
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('global-wave-one','one')",
                &[],
            )
            .await
            .unwrap();
        let g1 = lix
            .partial_replica_descriptor(None)
            .await
            .unwrap()
            .global_branch;
        lix.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('selected-wave','value')",
            &[],
        )
        .await
        .unwrap();
        global
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('global-wave-two','two')",
                &[],
            )
            .await
            .unwrap();
        let g2 = lix
            .partial_replica_descriptor(None)
            .await
            .unwrap()
            .global_branch;
        let storage = lix.storage_adapter();
        let mut writes = storage.new_write_set();
        let mut guards = super::super::partial_push_state::stage_initial_partial_push_states(
            &mut writes,
            &state,
        )
        .unwrap();
        guards.push(
            super::super::partial_state::stage_partial_replica_state(&mut writes, &state, None)
                .unwrap(),
        );
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        confirm_global(
            &storage,
            &state,
            PartialPushCoordinate {
                head: g1.head.commit_id.clone(),
                checkpoint: g1.checkpoint.commit_id,
            },
            "00000000-0000-7000-8000-000000004201",
        )
        .await;
        let read = storage.begin_read(Default::default()).await.unwrap();
        let first = prepare_partial_ordinary_upload(
            &read,
            &state,
            &branch,
            "00000000-0000-7000-8000-000000004202".into(),
            32,
            1024 * 1024,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            first
                .request
                .commits
                .iter()
                .any(|commit| commit.base_commit_id.as_deref() == Some(g1.head.commit_id.as_str())),
            "fixture must depend on the intermediate confirmed global base"
        );
        let mut writes = storage.new_write_set();
        let mut guards = super::super::partial_push_state::stage_prepare_partial_upload(
            &read,
            &mut writes,
            &state,
            &branch,
            &first.upload,
        )
        .await
        .unwrap();
        guards.extend(first.control_guard);
        drop(read);
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        confirm_global(
            &storage,
            &state,
            PartialPushCoordinate {
                head: g2.head.commit_id,
                checkpoint: g2.checkpoint.commit_id,
            },
            "00000000-0000-7000-8000-000000004203",
        )
        .await;
        let read = storage.begin_read(Default::default()).await.unwrap();
        let resumed = prepare_partial_ordinary_upload(
            &read,
            &state,
            &branch,
            "00000000-0000-7000-8000-000000004204".into(),
            32,
            1024 * 1024,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(resumed.upload, first.upload);
        assert_eq!(
            serde_json::to_vec(&resumed.request).unwrap(),
            serde_json::to_vec(&first.request).unwrap()
        );
        assert!(resumed.control_guard.is_none());
    }
}

#[cfg(test)]
mod created_ref_prefix_tests {
    use super::*;
    #[tokio::test]
    async fn global_prefix_unblocks_selected_source_before_later_child_creation() {
        let lix = crate::open_lix().await.unwrap();
        let descriptor = lix.partial_replica_descriptor(None).await.unwrap();
        let selected = descriptor.selected_branch.branch_id.clone();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", lix.lix_id()),
            lix.active_account_id().into(),
            uuid::Uuid::now_v7().to_string(),
            descriptor,
        )
        .unwrap();
        let first = lix
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "prefix-first".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('prefix-source','pending')",
            &[],
        )
        .await
        .unwrap();
        let second = lix
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "prefix-second".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        // A later selected edit must not strand the already captured child S.
        lix.execute(
            "UPDATE lix_key_value SET value='later' WHERE key='prefix-source'",
            &[],
        )
        .await
        .unwrap();
        let storage = lix.storage_adapter();
        let mut writes = storage.new_write_set();
        let mut guards = super::super::partial_push_state::stage_initial_partial_push_states(
            &mut writes,
            &state,
        )
        .unwrap();
        guards.push(
            super::super::partial_state::stage_partial_replica_state(&mut writes, &state, None)
                .unwrap(),
        );
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let prefix = prepare_partial_ordinary_upload(
            &read,
            &state,
            crate::GLOBAL_BRANCH_ID,
            uuid::Uuid::now_v7().to_string(),
            32,
            1024 * 1024,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(prefix.upload.created_refs.len(), 1);
        assert_eq!(prefix.upload.created_refs[0].branch_id, first.id);
        assert!(
            !prefix
                .request
                .ref_updates
                .iter()
                .any(|r| r.branch_id == second.id)
        );
        let mut writes = storage.new_write_set();
        let mut guards = super::super::partial_push_state::stage_prepare_partial_upload(
            &read,
            &mut writes,
            &state,
            crate::GLOBAL_BRANCH_ID,
            &prefix.upload,
        )
        .await
        .unwrap();
        guards.extend(prefix.control_guard);
        drop(read);
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let guards = super::super::partial_push_state::stage_acknowledge_partial_upload(
            &read,
            &mut writes,
            &state,
            crate::GLOBAL_BRANCH_ID,
            &prefix.upload,
            true,
        )
        .await
        .unwrap();
        drop(read);
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let selected_upload = prepare_partial_ordinary_upload(
            &read,
            &state,
            &selected,
            uuid::Uuid::now_v7().to_string(),
            32,
            1024 * 1024,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            !selected_upload.request.commits.is_empty(),
            "the eligible GLOBAL prefix makes selected source upload possible"
        );
        assert!(selected_upload.upload.created_refs.is_empty());
        let latest_selected = crate::branch::observe_branch_control_coordinate(&read, &selected)
            .await
            .unwrap()
            .control
            .unwrap()
            .head_commit_id
            .to_string();
        assert_ne!(
            selected_upload.upload.target.head, latest_selected,
            "S2 must remain pending until its GLOBAL L2 base is acknowledged"
        );
        let mut writes = storage.new_write_set();
        let mut guards = super::super::partial_push_state::stage_prepare_partial_upload(
            &read,
            &mut writes,
            &state,
            &selected,
            &selected_upload.upload,
        )
        .await
        .unwrap();
        guards.extend(selected_upload.control_guard);
        drop(read);
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let guards = super::super::partial_push_state::stage_acknowledge_partial_upload(
            &read,
            &mut writes,
            &state,
            &selected,
            &selected_upload.upload,
            true,
        )
        .await
        .unwrap();
        drop(read);
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let remaining = prepare_partial_ordinary_upload(
            &read,
            &state,
            crate::GLOBAL_BRANCH_ID,
            uuid::Uuid::now_v7().to_string(),
            32,
            1024 * 1024,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(remaining.upload.created_refs.len(), 1);
        assert_eq!(
            remaining.upload.created_refs[0].branch_id, second.id,
            "confirming the eligible selected prefix makes child S publishable"
        );
        let mut writes = storage.new_write_set();
        let mut guards = super::super::partial_push_state::stage_prepare_partial_upload(
            &read,
            &mut writes,
            &state,
            crate::GLOBAL_BRANCH_ID,
            &remaining.upload,
        )
        .await
        .unwrap();
        guards.extend(remaining.control_guard);
        drop(read);
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let guards = super::super::partial_push_state::stage_acknowledge_partial_upload(
            &read,
            &mut writes,
            &state,
            crate::GLOBAL_BRANCH_ID,
            &remaining.upload,
            true,
        )
        .await
        .unwrap();
        drop(read);
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let final_selected = prepare_partial_ordinary_upload(
            &read,
            &state,
            &selected,
            uuid::Uuid::now_v7().to_string(),
            32,
            1024 * 1024,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            final_selected.upload.target.head, latest_selected,
            "after L2 acknowledgment the remaining S2 suffix is dependency-closed"
        );
    }
}

/// Only ancestry of the confirmed authority GLOBAL coordinate can extend the
/// tiny known-base frontier. Local presence alone is not an upload proof.
pub(super) async fn is_confirmed_global_base(
    read: &(impl StorageAdapterRead + ?Sized),
    base: CommitId,
    confirmed: CommitId,
    cache: &mut std::collections::BTreeMap<CommitId, CommitRecord>,
) -> Result<bool, LixError> {
    let header = crate::tracked_state::load_published_commit_state_topology(read, base)
        .await?
        .ok_or_else(|| {
            crate::tracked_state::NativeMetadataRef::CommitStateHeader(base.to_string())
                .annotate_missing(blocked("historical GLOBAL base header must be hydrated"))
        })?;
    if !header.global_scope() {
        return Ok(false);
    }
    let source = super::partial_merge_analysis::record(read, base, true).await?;
    if source.base_commit_id.is_some() {
        return Ok(false);
    }
    super::partial_merge_analysis::bounded_ancestor(read, &source, confirmed, cache, 1024).await
}
#[cfg(test)]
mod confirmed_global_base_tests {
    use super::*;
    use crate::storage_adapter::{Storage, StorageAdapter, StorageWriteOptions};
    async fn ack<S: Storage + Clone + Send + Sync + 'static>(
        storage: &StorageAdapter<S>,
        state: &PartialReplicaState,
        branch: &str,
        upload: &PreparedPartialPush,
    ) {
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let mut guards = super::super::partial_push_state::stage_prepare_partial_upload(
            &read,
            &mut writes,
            state,
            branch,
            &upload.upload,
        )
        .await
        .unwrap();
        guards.extend(upload.control_guard.clone());
        drop(read);
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let guards = super::super::partial_push_state::stage_acknowledge_partial_upload(
            &read,
            &mut writes,
            state,
            branch,
            &upload.upload,
            true,
        )
        .await
        .unwrap();
        drop(read);
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn older_global_base_requires_confirmed_ancestry_not_local_presence() {
        let lix = crate::open_lix().await.unwrap();
        let descriptor = lix.partial_replica_descriptor(None).await.unwrap();
        let selected = descriptor.selected_branch.branch_id.clone();
        let source = descriptor.selected_branch.head.commit_id.clone();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", lix.lix_id()),
            lix.active_account_id().into(),
            uuid::Uuid::now_v7().to_string(),
            descriptor,
        )
        .unwrap();
        lix.create_branch(crate::CreateBranchOptions {
            id: None,
            name: "basis-L1".into(),
            from_commit_id: Some(source.clone()),
        })
        .await
        .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('older-basis','S')",
            &[],
        )
        .await
        .unwrap();
        lix.create_branch(crate::CreateBranchOptions {
            id: None,
            name: "basis-L2".into(),
            from_commit_id: Some(source.clone()),
        })
        .await
        .unwrap();
        // L3 exists only locally. Its presence must not authorize the next S2 upload.
        lix.create_branch(crate::CreateBranchOptions {
            id: None,
            name: "basis-unconfirmed-L3".into(),
            from_commit_id: Some(source),
        })
        .await
        .unwrap();
        lix.execute(
            "UPDATE lix_key_value SET value='S2' WHERE key='older-basis'",
            &[],
        )
        .await
        .unwrap();
        let storage = lix.storage_adapter();
        let mut writes = storage.new_write_set();
        let mut guards = super::super::partial_push_state::stage_initial_partial_push_states(
            &mut writes,
            &state,
        )
        .unwrap();
        guards.push(
            super::super::partial_state::stage_partial_replica_state(&mut writes, &state, None)
                .unwrap(),
        );
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let global = prepare_partial_ordinary_upload(
            &read,
            &state,
            crate::GLOBAL_BRANCH_ID,
            uuid::Uuid::now_v7().to_string(),
            2,
            1024 * 1024,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(global.upload.created_refs.len(), 2);
        drop(read);
        ack(&storage, &state, crate::GLOBAL_BRANCH_ID, &global).await;
        let read = storage.begin_read(Default::default()).await.unwrap();
        let selected_upload = prepare_partial_ordinary_upload(
            &read,
            &state,
            &selected,
            uuid::Uuid::now_v7().to_string(),
            32,
            1024 * 1024,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            selected_upload
                .request
                .commits
                .iter()
                .any(|c| c.base_commit_id.as_deref() != Some(global.upload.target.head.as_str())),
            "selected S retains its original older GLOBAL basis"
        );
        drop(read);
        ack(&storage, &state, &selected, &selected_upload).await;
        let read = storage.begin_read(Default::default()).await.unwrap();
        let error = match prepare_partial_ordinary_upload(
            &read,
            &state,
            &selected,
            uuid::Uuid::now_v7().to_string(),
            32,
            1024 * 1024,
        )
        .await
        {
            Err(error) => error,
            Ok(_) => panic!("unconfirmed local GLOBAL L3 cannot authorize upload"),
        };
        assert_eq!(error.code, "LIX_PARTIAL_UPLOAD_PREPARATION_REQUIRED");
    }
}
