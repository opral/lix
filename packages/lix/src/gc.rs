//! Deferred repository-GC boundary.
//!
//! The former repository collector owned deleted tracked-state, changelog,
//! branch-control, and binary-CAS spaces.  Those physical owners are no
//! longer part of the storage layout.  GC publication will be lowered through
//! the transaction-owned ForkTree reachability plan in the W5 wave; until
//! then every old entry point fails closed before producing a write plan.

use crate::LixError;
use crate::changelog::CommitId;
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::forktree::{
    CanonicalBranchId, PreparedPublication, StateCell, StateCellRef, StateKeyRef,
    UntrackedValueRef, open_coherent_view_on_read,
};
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};

const CHECKPOINT_RECOVERY_SCHEMA_KEY: &str = "lix_checkpoint_recovery";
const CHECKPOINT_GC_STATE_SCHEMA_KEY: &str = "lix_checkpoint_gc_state";
const CHECKPOINT_CONTROL_VERSION: u64 = 1;

fn gc_not_lowered() -> LixError {
    LixError::new(
        LixError::CODE_UNSUPPORTED_SQL,
        "repository GC and checkpoint recovery publication are not lowered through ForkTree yet",
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointRecoveryRef {
    pub(crate) branch_id: String,
    pub(crate) recovered_head_commit_id: CommitId,
    pub(crate) checkpoint_commit_id: CommitId,
    pub(crate) interval_has_commits: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CheckpointGcState {
    pub(crate) checkpoint_sequence: u64,
    pub(crate) last_gc_sequence: u64,
    pub(crate) collectible_interval_count: u64,
}

impl CheckpointGcState {
    pub(crate) fn add_collectible_interval(&mut self, interval_has_commits: bool) {
        if interval_has_commits {
            self.collectible_interval_count = self.collectible_interval_count.saturating_add(1);
        }
    }

    pub(crate) fn has_collectible_debt(self) -> bool {
        self.collectible_interval_count > 0
    }

    pub(crate) fn mark_collected(&mut self) {
        self.last_gc_sequence = self.checkpoint_sequence;
        self.collectible_interval_count = 0;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointPublication {
    pub(crate) recovery_ref: CheckpointRecoveryRef,
    pub(crate) gc_state: CheckpointGcState,
}

/// Stages checkpoint controls in the same authenticated untracked ForkTree
/// publication as the checkpoint commit. These rows are mutable current state;
/// the commit/root/catalog remain the semantic history authority.
pub(crate) fn stage_checkpoint_publication(
    publication: &mut PreparedPublication,
    checkpoint: &CheckpointPublication,
) -> Result<(), LixError> {
    let global_branch_id = global_branch_id();
    let recovery_entity_pk = EntityPk::single(checkpoint.recovery_ref.branch_id.clone());
    let recovery_value = serde_json::to_string(&serde_json::json!({
        "version": CHECKPOINT_CONTROL_VERSION,
        "branch_id": checkpoint.recovery_ref.branch_id,
        "recovered_head_commit_id": checkpoint.recovery_ref.recovered_head_commit_id.to_string(),
        "checkpoint_commit_id": checkpoint.recovery_ref.checkpoint_commit_id.to_string(),
        "interval_has_commits": checkpoint.recovery_ref.interval_has_commits,
    }))
    .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()))?;
    publication.put_untracked_row(
        global_branch_id,
        StateKeyRef {
            schema_key: CHECKPOINT_RECOVERY_SCHEMA_KEY,
            file_id: None,
            entity_pk: &recovery_entity_pk,
        },
        UntrackedValueRef {
            created_at: control_timestamp(),
            updated_at: control_timestamp(),
            cell: StateCellRef::Value(&recovery_value),
            metadata: None,
            origin_key: None,
            blob_manifest_object_ids: &[],
        },
    )?;

    let repository_entity_pk = EntityPk::single("repository");
    let gc_value = serde_json::to_string(&serde_json::json!({
        "version": CHECKPOINT_CONTROL_VERSION,
        "checkpoint_sequence": checkpoint.gc_state.checkpoint_sequence,
        "last_gc_sequence": checkpoint.gc_state.last_gc_sequence,
        "collectible_interval_count": checkpoint.gc_state.collectible_interval_count,
    }))
    .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()))?;
    publication.put_untracked_row(
        global_branch_id,
        StateKeyRef {
            schema_key: CHECKPOINT_GC_STATE_SCHEMA_KEY,
            file_id: None,
            entity_pk: &repository_entity_pk,
        },
        UntrackedValueRef {
            created_at: control_timestamp(),
            updated_at: control_timestamp(),
            cell: StateCellRef::Value(&gc_value),
            metadata: None,
            origin_key: None,
            blob_manifest_object_ids: &[],
        },
    )?;
    Ok(())
}

fn global_branch_id() -> CanonicalBranchId {
    CanonicalBranchId::from_bytes(
        *uuid::Uuid::parse_str(crate::GLOBAL_BRANCH_ID)
            .expect("global branch ID is canonical")
            .as_bytes(),
    )
}

fn control_timestamp() -> LixTimestamp {
    LixTimestamp::from_unix_millis_utc_lossy(0)
}

async fn load_checkpoint_control_rows<R>(
    read: &R,
) -> Result<Vec<(crate::forktree::StateKey, crate::forktree::UntrackedValue)>, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let view = open_coherent_view_on_read(read, global_branch_id()).await?;
    view.scan_untracked_rows().await
}

fn find_control_value<'a>(
    rows: &'a [(crate::forktree::StateKey, crate::forktree::UntrackedValue)],
    schema_key: &str,
    entity_pk: &EntityPk,
) -> Result<Option<&'a str>, LixError> {
    let mut value = None;
    for (key, row) in rows {
        if key.schema_key != schema_key || key.file_id.is_some() || key.entity_pk != *entity_pk {
            continue;
        }
        let StateCell::Value(cell) = &row.cell else {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                format!("checkpoint control '{schema_key}' is not a value"),
            ));
        };
        if value.replace(cell.as_str()).is_some() {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                format!("checkpoint control '{schema_key}' is duplicated"),
            ));
        }
    }
    Ok(value)
}

fn parse_recovery_value(value: &str, branch_id: &str) -> Result<CheckpointRecoveryRef, LixError> {
    let object = serde_json::from_str::<serde_json::Value>(value).map_err(|error| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("checkpoint recovery control is malformed: {error}"),
        )
    })?;
    if object.get("version").and_then(serde_json::Value::as_u64) != Some(CHECKPOINT_CONTROL_VERSION)
        || object.get("branch_id").and_then(serde_json::Value::as_str) != Some(branch_id)
    {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "checkpoint recovery control identity is inconsistent",
        ));
    }
    let recovered_head_commit_id = CommitId::parse_lix(
        object
            .get("recovered_head_commit_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                LixError::new(LixError::CODE_STORAGE_ERROR, "recovery head is absent")
            })?,
        "checkpoint recovery head",
    )?;
    let checkpoint_commit_id = CommitId::parse_lix(
        object
            .get("checkpoint_commit_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "checkpoint recovery checkpoint is absent",
                )
            })?,
        "checkpoint recovery checkpoint",
    )?;
    let interval_has_commits = object
        .get("interval_has_commits")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "checkpoint recovery interval flag is absent",
            )
        })?;
    Ok(CheckpointRecoveryRef {
        branch_id: branch_id.to_string(),
        recovered_head_commit_id,
        checkpoint_commit_id,
        interval_has_commits,
    })
}

fn parse_gc_state_value(value: &str) -> Result<CheckpointGcState, LixError> {
    let object = serde_json::from_str::<serde_json::Value>(value).map_err(|error| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("checkpoint GC control is malformed: {error}"),
        )
    })?;
    if object.get("version").and_then(serde_json::Value::as_u64) != Some(CHECKPOINT_CONTROL_VERSION)
    {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "checkpoint GC control version is unsupported",
        ));
    }
    let state = CheckpointGcState {
        checkpoint_sequence: object
            .get("checkpoint_sequence")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "checkpoint sequence is absent",
                )
            })?,
        last_gc_sequence: object
            .get("last_gc_sequence")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                LixError::new(LixError::CODE_STORAGE_ERROR, "last GC sequence is absent")
            })?,
        collectible_interval_count: object
            .get("collectible_interval_count")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| LixError::new(LixError::CODE_STORAGE_ERROR, "GC debt is absent"))?,
    };
    validate_checkpoint_gc_state(state)?;
    Ok(state)
}

fn validate_checkpoint_gc_state(state: CheckpointGcState) -> Result<(), LixError> {
    let Some(age) = state
        .checkpoint_sequence
        .checked_sub(state.last_gc_sequence)
    else {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "checkpoint GC sequence is ahead of checkpoint sequence",
        ));
    };
    if (state.checkpoint_sequence == 0 && state.has_collectible_debt())
        || (age == 0 && state.has_collectible_debt())
        || state.collectible_interval_count > age
    {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "checkpoint GC control has inconsistent sequence or debt",
        ));
    }
    Ok(())
}

pub(crate) async fn load_checkpoint_publication_state<R>(
    read: &R,
    branch_id: &str,
) -> Result<(Option<CheckpointRecoveryRef>, CheckpointGcState), LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let rows = load_checkpoint_control_rows(read).await?;
    let recovery_entity_pk = EntityPk::single(branch_id);
    let repository_entity_pk = EntityPk::single("repository");
    let recovery = find_control_value(&rows, CHECKPOINT_RECOVERY_SCHEMA_KEY, &recovery_entity_pk)?
        .map(|value| parse_recovery_value(value, branch_id))
        .transpose()?;
    let state = find_control_value(&rows, CHECKPOINT_GC_STATE_SCHEMA_KEY, &repository_entity_pk)?
        .map(parse_gc_state_value)
        .transpose()?
        .unwrap_or_default();
    Ok((recovery, state))
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RepositoryGcPlan {
    pub(crate) changelog: RepositoryGcChangelog,
    pub(crate) sweep: RepositoryGcSweep,
    pub(crate) profile: RepositoryGcProfile,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RepositoryGcChangelog {
    pub(crate) sweep: RepositoryGcChangelogSweep,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RepositoryGcChangelogSweep {
    pub(crate) commits: Vec<()>,
    pub(crate) changes: Vec<()>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RepositoryGcSweep {
    pub(crate) tracked_commit_roots: Vec<()>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RepositoryGcProfile {
    pub(crate) root_discovery_us: u64,
    pub(crate) changelog_us: u64,
    pub(crate) tracked_root_stage_us: u64,
    pub(crate) total_us: u64,
}

pub(crate) fn stage_checkpoint_gc_state(
    _writes: &mut StorageWriteSet,
    _state: &CheckpointGcState,
) -> Result<(), LixError> {
    Err(gc_not_lowered())
}

pub(crate) async fn load_checkpoint_gc_state<R>(read: &R) -> Result<CheckpointGcState, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    Ok(
        load_checkpoint_publication_state(read, crate::GLOBAL_BRANCH_ID)
            .await?
            .1,
    )
}

pub(crate) async fn stage_repository_gc_with_preconditions<S>(
    _store: S,
    _writes: &mut StorageWriteSet,
    _preconditions: &mut Vec<crate::storage_adapter::StoragePrecondition>,
) -> Result<RepositoryGcPlan, LixError>
where
    S: StorageAdapterRead,
{
    Err(gc_not_lowered())
}
