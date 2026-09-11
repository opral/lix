//! Bounded local creation capture from the immutable GLOBAL upload body.
use super::partial_push_state::{
    CreatedPartialRef, PartialPushCoordinate, load_partial_push_state,
};
use super::{PartialReplicaState, SyncCommit};
use crate::{LixError, row_pk::RowPk, storage_adapter::StorageAdapterRead};
use std::collections::BTreeSet;
fn invalid(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_UPLOAD_PREPARATION_REQUIRED", message)
}
pub(super) async fn capture_created_refs(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
    branch: &str,
    expected: &PartialPushCoordinate,
    target: &PartialPushCoordinate,
    commits: &[SyncCommit],
) -> Result<Vec<CreatedPartialRef>, LixError> {
    if branch != crate::GLOBAL_BRANCH_ID {
        return Ok(Vec::new());
    }
    let mut candidates = BTreeSet::new();
    for member in commits
        .iter()
        .filter(|commit| commit.global_scope)
        .flat_map(|commit| &commit.members)
    {
        if member.schema_key != crate::branch::BRANCH_DESCRIPTOR_SCHEMA_KEY
            || member.file_id.is_some()
        {
            continue;
        }
        let typed = RowPk::from_typed_json_array_value(&member.row_pk)
            .map_err(|_| invalid("invalid descriptor identity"))?;
        let value = member
            .row_pk
            .as_array()
            .filter(|v| v.len() == 1)
            .and_then(|v| v[0].get("value"))
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| invalid("descriptor identity is not one UUID"))?;
        if RowPk::uuid_from_canonical(value)
            .map_err(|_| invalid("descriptor UUID is not canonical"))?
            != typed
        {
            return Err(invalid("descriptor native identity is not UUID typed"));
        }
        candidates.insert(value.to_owned());
        if candidates.len() > 32 {
            return Err(invalid("branch creation wave exceeds 32 named refs"));
        }
    }
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let keys = candidates
        .iter()
        .map(|id| {
            Ok(crate::tracked_state::TrackedStateKey {
                schema_key: crate::branch::BRANCH_DESCRIPTOR_SCHEMA_KEY.into(),
                file_id: None,
                row_pk: RowPk::uuid_from_canonical(id)
                    .map_err(|_| invalid("descriptor UUID invalid"))?,
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let mut native = crate::tracked_state::TrackedStateContext::new().reader(read);
    let before = native.index_values_at_commit(&expected.head, &keys).await?;
    let after = native.index_values_at_commit(&target.head, &keys).await?;
    if before.len() != keys.len() || after.len() != keys.len() {
        return Err(invalid("descriptor point proof cardinality differs"));
    }
    let selected =
        load_partial_push_state(read, state, &state.descriptor().selected_branch.branch_id)
            .await?
            .0;
    let mut known = BTreeSet::from([
        selected.confirmed.head.clone(),
        selected.confirmed.checkpoint.clone(),
        expected.head.clone(),
        expected.checkpoint.clone(),
    ]);
    // Bodies are taken from the already dependency-closed native upload request.
    known.extend(commits.iter().map(|commit| commit.commit_id.clone()));
    let selected_id = &state.descriptor().selected_branch.branch_id;
    let selected_control = crate::branch::observe_branch_control_coordinate(read, selected_id)
        .await?
        .control
        .ok_or_else(|| invalid("selected source control is missing"))?;
    let selected_head = selected_control.head_commit_id.to_string();
    let selected_checkpoint = selected_control
        .working_diff_checkpoint_commit_id
        .map(|id| id.to_string());
    let selected_can_progress = selected_id != crate::GLOBAL_BRANCH_ID
        && (selected.prepared.is_some()
            || selected_head != selected.confirmed.head
            || selected_checkpoint.as_deref() != Some(selected.confirmed.checkpoint.as_str()));
    let mut result = Vec::new();
    for ((branch_id, before), after) in candidates.into_iter().zip(before).zip(after) {
        if before.is_some_and(|value| !value.deleted) || after.is_none_or(|value| value.deleted) {
            continue;
        }
        if branch_id == crate::GLOBAL_BRANCH_ID {
            return Err(invalid("GLOBAL cannot be a created child"));
        }
        let control = crate::branch::observe_branch_control_coordinate(read, &branch_id)
            .await?
            .control
            .ok_or_else(|| invalid("created branch lacks a local control"))?;
        let checkpoint = control
            .working_diff_checkpoint_commit_id
            .ok_or_else(|| invalid("created branch checkpoint missing"))?;
        let head = control.head_commit_id.to_string();
        let checkpoint = checkpoint.to_string();
        let mut pending_source = false;
        for source in BTreeSet::from([&head, &checkpoint]) {
            if known.contains(source) {
                continue;
            }
            let source_record = super::partial_merge_analysis::record(
                read,
                crate::changelog::CommitId::parse_lix(source, "created branch source")?,
                true,
            )
            .await?;
            if super::partial_merge_analysis::bounded_ancestor(
                read,
                &source_record,
                crate::changelog::CommitId::parse_lix(
                    &selected.confirmed.head,
                    "confirmed selected source",
                )?,
                &mut Default::default(),
                1024,
            )
            .await?
            {
                known.insert(source.clone());
                continue;
            }
            let will_be_uploaded = selected_can_progress
                && (Some(source.as_str()) == selected_checkpoint.as_deref()
                    || super::partial_merge_analysis::bounded_ancestor(
                        read,
                        &source_record,
                        crate::changelog::CommitId::parse_lix(
                            &selected_head,
                            "pending selected source",
                        )?,
                        &mut Default::default(),
                        1024,
                    )
                    .await?);
            if !will_be_uploaded {
                return Err(invalid(
                    "created branch source is neither confirmed nor supplied by the pending selected upload",
                ));
            }
            pending_source = true;
        }
        if pending_source {
            return Err(LixError::new(
                "LIX_PARTIAL_CREATED_REF_SOURCE_PENDING",
                "created branch source awaits the pending selected upload",
            ));
        }
        result.push(CreatedPartialRef {
            branch_id,
            head_commit_id: head,
            checkpoint_commit_id: checkpoint,
        });
    }
    Ok(result)
}
