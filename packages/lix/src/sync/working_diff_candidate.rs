//! Batch immutable working-diff dependencies before publishing a moving head.
//!
//! This stage owns no serving controls or write frontier. The ordinary
//! candidate evaluator still proves those after dependency installation.

use super::http::{HttpSyncTransport, RawHttpClient};
use super::partial_state::PartialReplicaState;
use super::read_fulfillment::{ReadFulfillmentRequest, ReadInputAddress};
use crate::LixError;
use crate::engine::Engine;
use crate::storage_adapter::Storage;
use crate::tracked_state::NativeMetadataRef;

fn selected_working_diff_recipes<'a>(
    interests: impl IntoIterator<Item = &'a crate::hot_state::LogicalReadInterest>,
    selected_branch_id: &str,
) -> Vec<crate::hot_state::LogicalReadInterest> {
    interests
        .into_iter()
        .filter(|interest| {
            matches!(
                interest,
                crate::hot_state::LogicalReadInterest::Diff {
                    branch_id: Some(branch_id),
                    ..
                } if branch_id == selected_branch_id
            )
        })
        .filter(|interest| super::working_diff_recipe::is_supported_working_diff_recipe(interest))
        .cloned()
        .collect()
}

/// Called at most once after a candidate discovers a missing native input.
/// A declined replay leaves the original native demand path in charge.
pub(super) async fn hydrate_working_diff_dependencies<S, C>(
    engine: &Engine<S>,
    previous: &PartialReplicaState,
    next: &PartialReplicaState,
    transport: &HttpSyncTransport<C>,
) -> Result<bool, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
{
    let Some(registry) = engine.sync_mode().read_interests() else {
        return Ok(false);
    };
    let snapshot = registry.moving_current_snapshot()?;
    let selected_branch_id = next.descriptor().selected_branch.branch_id.as_str();
    // A captured moving Diff may legitimately target GLOBAL or an archived
    // branch. Candidate replay is only for this exact leased selected branch,
    // so discard unrelated scopes before aggregate validation instead of
    // letting them poison a valid selected batch.
    let interests = selected_working_diff_recipes(
        snapshot
            .as_read_snapshot()
            .interests
            .iter()
            .map(|interest| interest.as_ref()),
        selected_branch_id,
    );
    if interests.is_empty()
        || super::working_diff_recipe::validate_working_diff_recipes(
            &interests,
            &next.descriptor().selected_branch.branch_id,
        )
        .is_err()
    {
        return Ok(false);
    }
    let request = ReadFulfillmentRequest {
        operation_id: uuid::Uuid::now_v7().to_string(),
        release: false,
        release_completed: false,
        operation_expires_at_ms: next.baseline_lease().expires_at_ms,
        epoch_id: next.epoch_id().to_owned(),
        descriptor: next.descriptor().clone(),
        interests,
        required: vec![ReadInputAddress::Metadata(
            NativeMetadataRef::CommitStateHeader(
                next.descriptor().selected_branch.head.commit_id.clone(),
            ),
        )],
        continuation: None,
    };
    if super::read_fulfillment::request_closure_is_ineligible(&request)? {
        return Ok(false);
    }
    let storage = engine.storage();
    let mut response =
        super::read_fulfillment::staging::fetch_staged(&storage, previous, transport, &request)
            .await?;
    if response.outcome() != super::read_fulfillment::ReadFulfillmentOutcome::Complete {
        super::read_fulfillment::remember_request_closure_ineligible(&request, response.outcome())?;
        return Ok(false);
    }
    super::read_fulfillment::validate_candidate_basis(previous, next, &request)?;
    response.promote(&request, true).await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hot_state::{DiffInterestEndpoint, LogicalReadInterest};
    use crate::tracked_state::TrackedStateFilter;

    fn recipe(branch_id: &str) -> LogicalReadInterest {
        LogicalReadInterest::Diff {
            branch_id: Some(branch_id.to_owned()),
            relation: "lix_file".to_owned(),
            from: DiffInterestEndpoint::WorkingCheckpoint,
            to: DiffInterestEndpoint::ActiveHead,
            filter: TrackedStateFilter {
                include_tombstones: true,
                ..Default::default()
            },
            retain_payloads: false,
            projected_columns: vec!["id".to_owned()],
            limit: None,
        }
    }

    #[test]
    fn unrelated_global_or_archived_diff_does_not_block_selected_batch() {
        let selected = recipe("selected");
        let global = recipe(crate::GLOBAL_BRANCH_ID);
        let archived = recipe("archived");
        let selected_batch =
            selected_working_diff_recipes([&selected, &global, &archived], "selected");
        assert_eq!(selected_batch, vec![selected.clone()]);
        super::super::working_diff_recipe::validate_working_diff_recipes(
            &selected_batch,
            "selected",
        )
        .unwrap();

        let unrelated = (0..=super::super::working_diff_recipe::MAX_WORKING_DIFF_RECIPE_COUNT)
            .map(|_| recipe(crate::GLOBAL_BRANCH_ID))
            .collect::<Vec<_>>();
        let mut mixed = vec![&selected];
        mixed.extend(unrelated.iter());
        let selected_batch = selected_working_diff_recipes(mixed, "selected");
        assert_eq!(selected_batch, vec![selected]);
        super::super::working_diff_recipe::validate_working_diff_recipes(
            &selected_batch,
            "selected",
        )
        .unwrap();
    }
}
