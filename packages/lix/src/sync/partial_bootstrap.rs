//! Staging of root-backed opening coordinates for a partial replica.
//!
//! This helper is only for fresh local storage. It publishes no complete-state
//! certificate, materializes no native objects, and does not activate an engine.
//! A dedicated opener must also bind repository identity, admission and object
//! ownership before exposing these coordinates to SQL or garbage collection.

use bytes::Bytes;

use crate::LixError;
use crate::branch::{
    BranchHeadControl, branch_head_control_precondition, stage_branch_head_control,
};
use crate::changelog::{ChangeId, CommitId};
use crate::common::LixTimestamp;
use crate::hot_state::{ROOT_CURRENT_BASE_SPACE, TrackedHeadContext, hot_generation_scope_prefix};
use crate::storage_adapter::{
    StorageAdapterRead, StorageKey, StoragePrecondition, StorageWriteSet,
};

use super::partial_state::{PartialReplicaState, stage_partial_replica_state};

/// Canonical local control for an admitted immutable authority basis. The
/// local serving generation is separate from authority identity/revision.
pub(super) fn partial_branch_control(
    state: &PartialReplicaState,
    branch: &super::partial_replica::PartialReplicaBranch,
) -> Result<BranchHeadControl, LixError> {
    let invalid = |field: &str| {
        LixError::new(
            "LIX_PARTIAL_REPLICA_STATE_INVALID",
            format!("partial replica has invalid {field}"),
        )
    };
    let head = CommitId::parse(&branch.head.commit_id).map_err(|_| invalid("head ID"))?;
    let checkpoint =
        CommitId::parse(&branch.checkpoint.commit_id).map_err(|_| invalid("checkpoint ID"))?;
    Ok(BranchHeadControl {
        head_commit_id: head,
        tracked_generation: state.serving_generation(&branch.branch_id)?,
        current_state_revision: 0,
        working_diff_checkpoint_commit_id: Some(checkpoint),
        created_at: LixTimestamp::parse(&branch.created_at).map_err(|_| invalid("createdAt"))?,
        updated_at: LixTimestamp::parse(&branch.updated_at).map_err(|_| invalid("updatedAt"))?,
        ref_change_id: ChangeId::parse(&branch.ref_change_id)
            .map_err(|_| invalid("refChangeId"))?,
        schema_presence_bloom: [u64::MAX; 4],
    })
}

/// Stage the opening receipt, selected/global controls and root-base markers
/// in one caller-owned write set. Commit every returned precondition atomically
/// with that set. They reject existing controls/markers/receipts; this is not a
/// replacement, migration, reopen or branch-reset API.
///
/// Head IDs select fresh *local* serving generations. The authority's mutable
/// generation/revision is never transferred. All-one schema bloom bits avoid
/// claiming absence for schemas whose objects have not been loaded.
/// On error the caller must discard the write set.
#[must_use = "opening writes must be committed with every returned precondition"]
pub(crate) fn stage_partial_bootstrap(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let descriptor = state.descriptor();
    descriptor.validate(
        state.repository_id(),
        Some(&descriptor.selected_branch.branch_id),
    )?;
    let mut controls = Vec::with_capacity(2);
    let mut preconditions = Vec::with_capacity(8);
    for branch in [&descriptor.selected_branch, &descriptor.global_branch] {
        if controls
            .iter()
            .any(|(id, _)| *id == branch.branch_id.as_str())
        {
            continue;
        }
        let control = partial_branch_control(state, branch)?;
        preconditions.push(branch_head_control_precondition(&branch.branch_id, None)?);
        preconditions.push(StoragePrecondition::KeyAbsent {
            space: ROOT_CURRENT_BASE_SPACE,
            key: StorageKey(Bytes::from(hot_generation_scope_prefix(
                &branch.branch_id,
                control.tracked_generation,
            ))),
        });
        controls.push((branch.branch_id.as_str(), control));
    }
    preconditions.push(stage_partial_replica_state(writes, state, None)?);
    // This fresh local token identifies the fixed baseline's catalog; it does
    // not certify catalog coverage. Compilation still completes its native
    // scans before caching. Local schema commits rotate it atomically. Any
    // future remote baseline adoption must also rotate it in the same write.
    crate::catalog::stage_catalog_revision(writes);
    // Account proofs are disposable and revision-bound. A new partial epoch
    // needs its own unique token just like full initialization; actual account
    // mutations and remote global publication rotate this token atomically.
    crate::account::stage_account_revision(writes);
    preconditions
        .push(super::partial_interest_journal::stage_initial_read_interest_journal(writes, state)?);
    preconditions
        .extend(super::partial_push_state::stage_initial_partial_push_states(writes, state)?);
    for (branch_id, control) in controls {
        stage_branch_head_control(writes, branch_id, control)?;
        TrackedHeadContext::new()
            .writer(read, writes)
            .stage_empty_root_deterministic_witness(branch_id, control.tracked_generation)?;
        TrackedHeadContext::new()
            .writer(read, writes)
            .stage_root_current_base(
                branch_id,
                control.tracked_generation,
                control.head_commit_id,
            );
    }
    Ok(preconditions)
}

#[cfg(test)]
mod tests {
    use super::super::partial_state::load_partial_replica_state;
    use super::*;
    use crate::branch::BranchHeadControlContext;
    use crate::storage_adapter::{
        PointReadPlan, StorageAdapter, StorageGetOptions, StorageProjectedValue,
        StorageWriteOptions,
    };
    use crate::{CreateBranchOptions, Memory, open_lix};

    async fn state(select_global: bool) -> PartialReplicaState {
        let authority = open_lix().await.unwrap();
        let selected = if select_global {
            crate::GLOBAL_BRANCH_ID.to_owned()
        } else {
            authority
                .create_branch(CreateBranchOptions {
                    id: None,
                    name: "partial-selected".into(),
                    from_commit_id: None,
                })
                .await
                .unwrap()
                .id
        };
        PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            "00000000-0000-7000-8000-000000000199".to_owned(),
            authority
                .partial_replica_descriptor(Some(&selected))
                .await
                .unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn partial_engine_open_requires_exact_admission_without_native_rows() {
        let state = state(false).await;
        let adapter = StorageAdapter::new(Memory::new());
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = adapter.new_write_set();
        let preconditions = stage_partial_bootstrap(&read, &mut writes, &state).unwrap();
        crate::init::stage_partial_repository_protocol(&mut writes);
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

        // No native row, commit header, graph record or payload is installed.
        // Construction therefore cannot fall back to SQL identity/account reads.
        let (engine, session) = crate::engine::Engine::new_partial_replica(
            adapter.clone(),
            crate::engine::EngineOptions::new(),
            &state,
        )
        .await
        .unwrap();
        assert_eq!(engine.lix_id(), state.repository_id());
        assert_eq!(session.active_account_id(), state.active_account_id());
        assert_eq!(
            session.active_branch_id().await.unwrap(),
            state.descriptor().selected_branch.branch_id
        );

        let ordinary = crate::engine::Engine::new_with_adapter(
            adapter.clone(),
            crate::engine::EngineOptions::new(),
        )
        .await;
        assert!(
            matches!(ordinary, Err(error) if error.code == "LIX_PARTIAL_REPLICA_REQUIRES_ON_DEMAND_SYNC")
        );

        for (remote, account, epoch) in [
            (
                "https://different.test/repo",
                state.active_account_id(),
                state.epoch_id(),
            ),
            (
                state.remote_id(),
                crate::SYSTEM_ACCOUNT_ID,
                state.epoch_id(),
            ),
            (
                state.remote_id(),
                state.active_account_id(),
                "00000000-0000-7000-8000-000000000299",
            ),
        ] {
            let different = PartialReplicaState::new(
                remote.into(),
                account.into(),
                epoch.into(),
                state.descriptor().clone(),
            )
            .unwrap();
            let opened = crate::engine::Engine::new_partial_replica(
                adapter.clone(),
                crate::engine::EngineOptions::new(),
                &different,
            )
            .await;
            assert!(
                matches!(opened, Err(error) if error.code == "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH")
            );
        }
        // Engine admission alone must not grant permission to publish writes.
        let mut unauthorized = adapter.new_write_set();
        crate::init::stage_repository_protocol(&mut unauthorized);
        assert!(
            adapter
                .commit_write_set(unauthorized, Default::default())
                .await
                .is_err()
        );
        let branch = &state.descriptor().selected_branch;
        let base = CommitId::parse(&branch.head.commit_id).unwrap();
        let key = StorageKey(Bytes::from(hot_generation_scope_prefix(
            &branch.branch_id,
            base,
        )));
        let mut corrupt = adapter.new_write_set();
        corrupt.put(
            ROOT_CURRENT_BASE_SPACE,
            key,
            crate::storage_adapter::StorageValue {
                bytes: Bytes::from(vec![7u8; 16]),
            },
        );
        adapter
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                corrupt,
                Default::default(),
            )
            .await
            .unwrap();
        let opened = crate::engine::Engine::new_partial_replica(
            adapter,
            crate::engine::EngineOptions::new(),
            &state,
        )
        .await;
        assert!(
            matches!(opened, Err(error) if error.code == "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH"),
            "a valid but unrelated root UUID must not be admitted"
        );
    }

    #[tokio::test]
    async fn partial_bootstrap_stages_only_canonical_root_backed_coordinates() {
        for select_global in [false, true] {
            let state = state(select_global).await;
            let adapter = StorageAdapter::new(Memory::new());
            let read = adapter.begin_read(Default::default()).await.unwrap();
            let mut writes = adapter.new_write_set();
            let preconditions = stage_partial_bootstrap(&read, &mut writes, &state).unwrap();
            assert_eq!(preconditions.len(), if select_global { 5 } else { 8 });
            drop(read);
            adapter
                .commit_write_set(
                    writes,
                    StorageWriteOptions {
                        preconditions,
                        await_durable: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let read = adapter.begin_read(Default::default()).await.unwrap();
            assert_eq!(
                load_partial_replica_state(&read).await.unwrap().unwrap().0,
                state
            );
            let account_revision = crate::account::load_account_revision(&read)
                .await
                .unwrap()
                .expect("partial bootstrap must seed the account proof token");
            assert_eq!(account_revision.len(), 16);
            for branch in [
                &state.descriptor().selected_branch,
                &state.descriptor().global_branch,
            ] {
                let control = BranchHeadControlContext::new()
                    .reader(&read)
                    .load(&branch.branch_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(control.head_commit_id.to_string(), branch.head.commit_id);
                assert_eq!(control.tracked_generation, control.head_commit_id);
                assert_eq!(control.current_state_revision, 0);
                assert_eq!(
                    control
                        .working_diff_checkpoint_commit_id
                        .unwrap()
                        .to_string(),
                    branch.checkpoint.commit_id
                );
                assert_eq!(control.created_at.to_string(), branch.created_at);
                assert_eq!(control.updated_at.to_string(), branch.updated_at);
                assert_eq!(control.ref_change_id.to_string(), branch.ref_change_id);
                assert!(
                    control
                        .schema_presence_bloom
                        .iter()
                        .all(|bits| *bits == u64::MAX)
                );
                let marker_key = StorageKey(Bytes::from(hot_generation_scope_prefix(
                    &branch.branch_id,
                    control.tracked_generation,
                )));
                let marker = PointReadPlan::new(ROOT_CURRENT_BASE_SPACE, &[marker_key])
                    .materialize(&read, StorageGetOptions::default())
                    .await
                    .unwrap()
                    .value
                    .pop()
                    .flatten()
                    .unwrap();
                assert!(
                    matches!(marker, StorageProjectedValue::FullValue(bytes) if bytes.as_ref() == control.head_commit_id.as_uuid().as_bytes())
                );
            }
            let full_receipt = PointReadPlan::new(
                super::super::SYNC_REPLICA_STATE_SPACE,
                &[super::super::replica_state_key()],
            )
            .materialize(&read, StorageGetOptions::default())
            .await
            .unwrap();
            assert!(
                full_receipt.value.into_iter().all(|value| value.is_none()),
                "opening must not create a full-sync receipt"
            );
            let controls = BranchHeadControlContext::new().reader(&read);
            assert_eq!(
                controls.scan().await.unwrap_err().code,
                "LIX_SYNC_BRANCH_INVENTORY_REQUIRED"
            );
            let unknown_branch = controls
                .load("00000000-0000-7000-8000-000000000299")
                .await
                .expect_err("unloaded native descriptor cannot prove branch absence");
            assert_eq!(unknown_branch.code, "LIX_COMMIT_NOT_FOUND");
            assert_eq!(
                crate::tracked_state::NativeMetadataRef::from_missing_error(&unknown_branch)
                    .unwrap(),
                Some(crate::tracked_state::NativeMetadataRef::CommitGraphRecord(
                    state.descriptor().global_branch.head.commit_id.clone(),
                )),
                "descriptor-only opening must demand the exact global native graph record, not invent an absent branch"
            );
            assert!(
                super::super::repository::has_any_sync_replica_state(&read)
                    .await
                    .unwrap()
            );
            assert_eq!(
                super::super::repository::inspect_sync_replica_binding(&read)
                    .await
                    .unwrap_err()
                    .code,
                "LIX_PARTIAL_REPLICA_REQUIRES_ON_DEMAND_SYNC"
            );
            let mut duplicate = adapter.new_write_set();
            let preconditions = stage_partial_bootstrap(&read, &mut duplicate, &state).unwrap();
            drop(read);
            assert_eq!(
                super::super::repository::admit_sync_authority_storage(&adapter, None)
                    .await
                    .unwrap_err()
                    .code,
                super::super::SYNC_PROTOCOL_MISMATCH_CODE
            );
            assert!(
                adapter
                    .commit_write_set(
                        duplicate,
                        StorageWriteOptions {
                            preconditions,
                            ..Default::default()
                        }
                    )
                    .await
                    .is_err(),
                "installer must not reset an existing partial replica"
            );
        }
    }
}
