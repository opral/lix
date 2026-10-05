//! Narrow authority proof for moving working-diff recipes.
//!
//! This module validates recipe shape and proves only that the leased
//! selected-branch checkpoint is on the leased selected-branch first-parent
//! lane. It does not turn diff output into row authority or write coverage.

use crate::LixError;
use crate::changelog::CommitId;
use crate::commit_graph::{CommitGraphContext, CommitGraphReader};
use crate::hot_state::{DiffInterestEndpoint, LogicalReadInterest};
use crate::storage_adapter::StorageAdapterRead;
use crate::sync::PartialReplicaDescriptor;
use std::collections::BTreeSet;

pub(crate) const MAX_WORKING_DIFF_RECIPE_COUNT: usize = 8;
pub(crate) const WORKING_DIFF_RECIPE_FALLBACK_CODE: &str = "LIX_WORKING_DIFF_RECIPE_FALLBACK";

fn invalid(message: &str) -> LixError {
    LixError::new("LIX_READ_FULFILLMENT_INVALID", message)
}

fn fallback(message: &str) -> LixError {
    LixError::new(WORKING_DIFF_RECIPE_FALLBACK_CODE, message)
}

/// Validate the only moving diff recipes eligible for bounded authority
/// replay. Non-diff recipes are left to their existing validators.
pub(crate) fn validate_working_diff_recipes(
    interests: &[LogicalReadInterest],
    selected_branch_id: &str,
) -> Result<(), LixError> {
    let mut count = 0usize;
    for interest in interests {
        let LogicalReadInterest::Diff {
            branch_id,
            relation,
            from,
            to,
            filter,
            retain_payloads,
            projected_columns,
            limit,
        } = interest
        else {
            continue;
        };
        count += 1;
        if count > MAX_WORKING_DIFF_RECIPE_COUNT {
            return Err(invalid("working diff recipe count limit exceeded"));
        }
        if branch_id.as_deref() != Some(selected_branch_id)
            || !matches!(from, DiffInterestEndpoint::WorkingCheckpoint)
            || !matches!(to, DiffInterestEndpoint::ActiveHead)
            || !matches!(relation.as_str(), "lix_file" | "lix_directory")
            || *retain_payloads
            || limit.is_some()
            || projected_columns
                .iter()
                .any(|column| matches!(column.as_str(), "from_content" | "to_content"))
        {
            return Err(invalid("working diff recipe is outside the bounded route"));
        }
        crate::sql2::validate_bounded_history_recipe_shape(
            relation,
            filter,
            projected_columns,
            false,
        )?;
    }
    Ok(())
}

/// Whether this recipe has the moving-diff shape that may be salvaged during
/// capture. This is only a syntactic eligibility check: request validation
/// must still match the recipe branch to the leased descriptor branch.
pub(crate) fn is_supported_working_diff_recipe(interest: &LogicalReadInterest) -> bool {
    let LogicalReadInterest::Diff {
        branch_id: Some(branch_id),
        ..
    } = interest
    else {
        return false;
    };
    validate_working_diff_recipes(std::slice::from_ref(interest), branch_id).is_ok()
}

/// Prove the exact selected-branch checkpoint/head pair in this descriptor
/// lies on one bounded, strictly generation-decreasing first-parent lane.
/// Missing ancestry and exhausted budget are fallback outcomes; structural
/// corruption is an invalid-parameter error. Neither result grants coverage.
pub(crate) async fn prove_selected_branch_checkpoint_ancestry<R>(
    read: R,
    descriptor: &PartialReplicaDescriptor,
    remaining_graph_nodes: &mut usize,
) -> Result<(), LixError>
where
    R: StorageAdapterRead,
{
    let branch = &descriptor.selected_branch;
    let parse_descriptor_commit = |value: &str| -> Result<CommitId, LixError> {
        let parsed = CommitId::parse_lix(value, "working diff descriptor commit ID")?;
        if !parsed.has_canonical_text(value) {
            return Err(LixError::new(
                crate::sync::SYNC_PROTOCOL_MISMATCH_CODE,
                "working diff descriptor contains a noncanonical commit ID",
            ));
        }
        Ok(parsed)
    };
    let head = parse_descriptor_commit(&branch.head.commit_id)?;
    let checkpoint = parse_descriptor_commit(&branch.checkpoint.commit_id)?;
    let mut graph = CommitGraphContext::new().reader(read);
    prove_first_parent_ancestor(&mut graph, head, checkpoint, remaining_graph_nodes).await
}

async fn prove_first_parent_ancestor<R: CommitGraphReader>(
    graph: &mut R,
    head: CommitId,
    checkpoint: CommitId,
    remaining_graph_nodes: &mut usize,
) -> Result<(), LixError> {
    let mut current = head;
    let mut previous_generation = None;
    let mut visited = BTreeSet::new();
    loop {
        if !visited.insert(current) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "working diff first-parent lane contains a cycle",
            ));
        }
        if *remaining_graph_nodes == 0 {
            return Err(fallback("working diff first-parent node limit exceeded"));
        }
        *remaining_graph_nodes -= 1;
        let loaded = graph.load_node(&current).await;
        let loaded = match loaded {
            Ok(loaded) => loaded,
            Err(error) if error.code == "LIX_SYNC_HISTORY_REQUIRED" => {
                return Err(fallback(
                    "working diff first-parent lane requires unavailable history",
                ));
            }
            Err(error) => return Err(error),
        };
        let Some(node) = loaded else {
            return Err(fallback("working diff first-parent lane is incomplete"));
        };
        if node.commit_id != current {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "working diff first-parent node ID disagrees with its requested address",
            ));
        }
        if previous_generation.is_some_and(|generation| node.generation >= generation) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "working diff first-parent generations are invalid",
            ));
        }
        if current == checkpoint {
            return Ok(());
        }
        let Some(parent) = node.parent_commit_ids.first().copied() else {
            return Err(fallback(
                "working diff checkpoint is not on the leased first-parent lane",
            ));
        };
        previous_generation = Some(node.generation);
        current = parent;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_graph::{CommitGraphNode, CommitGraphReader, ReachableCommitGraphNode};
    use crate::common::LixTimestamp;
    use crate::tracked_state::TrackedStateFilter;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn id(label: &str) -> CommitId {
        CommitId::for_test_label(label)
    }

    fn node(id: CommitId, parent: Option<CommitId>, generation: u64) -> CommitGraphNode {
        CommitGraphNode {
            is_checkpoint: false,
            first_parent_checkpoint_summary: None,
            commit_id: id,
            change_id: id.commit_change_id(),
            account_id: "account".to_owned(),
            generation,
            parent_commit_ids: parent.into_iter().collect(),
            base_commit_id: None,
            first_parent_jump_commit_id: id,
            first_parent_jump_span: 0,
            created_at: LixTimestamp::expect_parse("timestamp", "2024-01-01T00:00:00.000Z"),
            touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
        }
    }

    #[derive(Default)]
    struct Graph {
        nodes: HashMap<CommitId, CommitGraphNode>,
        missing_as_history_demand: bool,
    }

    #[async_trait::async_trait]
    impl CommitGraphReader for Graph {
        async fn load_node(
            &mut self,
            commit_id: &CommitId,
        ) -> Result<Option<CommitGraphNode>, LixError> {
            match self.nodes.get(commit_id) {
                Some(node) => Ok(Some(node.clone())),
                None if self.missing_as_history_demand => Err(LixError::new(
                    "LIX_SYNC_HISTORY_REQUIRED",
                    "test graph node is deferred",
                )),
                None => Ok(None),
            }
        }

        async fn reachable_nodes(
            &mut self,
            _head_commit_id: &CommitId,
        ) -> Result<Arc<[ReachableCommitGraphNode]>, LixError> {
            unreachable!("ancestry proof must only load first-parent nodes")
        }
    }

    fn working_file_interest() -> LogicalReadInterest {
        LogicalReadInterest::Diff {
            branch_id: Some("branch".to_owned()),
            relation: "lix_file".to_owned(),
            from: DiffInterestEndpoint::WorkingCheckpoint,
            to: DiffInterestEndpoint::ActiveHead,
            filter: TrackedStateFilter {
                include_tombstones: true,
                ..TrackedStateFilter::default()
            },
            retain_payloads: false,
            projected_columns: vec!["id".to_owned()],
            limit: None,
        }
    }

    #[test]
    fn accepts_only_selected_branch_dynamic_builtin_metadata_diff() {
        validate_working_diff_recipes(&[working_file_interest()], "branch").unwrap();

        let mut fixed = working_file_interest();
        if let LogicalReadInterest::Diff { from, .. } = &mut fixed {
            *from = DiffInterestEndpoint::Fixed(id("old").to_string());
        }
        assert!(validate_working_diff_recipes(&[fixed], "branch").is_err());

        let mut payload = working_file_interest();
        if let LogicalReadInterest::Diff {
            retain_payloads, ..
        } = &mut payload
        {
            *retain_payloads = true;
        }
        assert!(validate_working_diff_recipes(&[payload], "branch").is_err());

        let mut content = working_file_interest();
        if let LogicalReadInterest::Diff {
            projected_columns, ..
        } = &mut content
        {
            projected_columns.push("from_content".to_owned());
        }
        assert!(validate_working_diff_recipes(&[content], "branch").is_err());

        let mut wrong_branch = working_file_interest();
        if let LogicalReadInterest::Diff { branch_id, .. } = &mut wrong_branch {
            *branch_id = Some("other-branch".to_owned());
        }
        assert!(validate_working_diff_recipes(&[wrong_branch], "branch").is_err());

        let mut custom_relation = working_file_interest();
        if let LogicalReadInterest::Diff { relation, .. } = &mut custom_relation {
            *relation = "private_table".to_owned();
        }
        assert!(validate_working_diff_recipes(&[custom_relation], "branch").is_err());
    }

    #[test]
    fn rejects_more_than_eight_working_diff_recipes() {
        let recipes = vec![working_file_interest(); MAX_WORKING_DIFF_RECIPE_COUNT + 1];
        assert!(validate_working_diff_recipes(&recipes, "branch").is_err());
    }

    #[tokio::test]
    async fn proves_first_parent_ancestry_and_falls_back_on_wrong_lane() {
        let head = id("head");
        let middle = id("middle");
        let checkpoint = id("checkpoint");
        let mut graph = Graph::default();
        graph.nodes.insert(head, node(head, Some(middle), 3));
        graph
            .nodes
            .insert(middle, node(middle, Some(checkpoint), 2));
        graph.nodes.insert(checkpoint, node(checkpoint, None, 1));
        let mut budget = 3;
        prove_first_parent_ancestor(&mut graph, head, checkpoint, &mut budget)
            .await
            .unwrap();
        assert_eq!(budget, 0);

        let unrelated = id("unrelated");
        let mut shared_budget = 3;
        let error = prove_first_parent_ancestor(&mut graph, head, unrelated, &mut shared_budget)
            .await
            .unwrap_err();
        assert_eq!(error.code, WORKING_DIFF_RECIPE_FALLBACK_CODE);
        assert_eq!(shared_budget, 0);

        let mut shared_budget = 2;
        prove_first_parent_ancestor(&mut graph, middle, checkpoint, &mut shared_budget)
            .await
            .unwrap();
        assert_eq!(shared_budget, 0);
        let error = prove_first_parent_ancestor(&mut graph, head, checkpoint, &mut shared_budget)
            .await
            .unwrap_err();
        assert_eq!(error.code, WORKING_DIFF_RECIPE_FALLBACK_CODE);
    }

    #[tokio::test]
    async fn rejects_non_decreasing_generation_and_bounds_traversal() {
        let head = id("bad-head");
        let parent = id("bad-parent");
        let mut graph = Graph::default();
        graph.nodes.insert(head, node(head, Some(parent), 4));
        graph.nodes.insert(parent, node(parent, None, 4));
        let mut budget = 2;
        let error = prove_first_parent_ancestor(&mut graph, head, parent, &mut budget)
            .await
            .unwrap_err();
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);

        let mut graph = Graph::default();
        let ids = (0..=3)
            .map(|index| id(&format!("overbudget-{index}")))
            .collect::<Vec<_>>();
        for index in 0..3 {
            graph.nodes.insert(
                ids[index],
                node(ids[index], Some(ids[index + 1]), (3 - index) as u64),
            );
        }
        let mut budget = 2;
        let error =
            prove_first_parent_ancestor(&mut graph, ids[0], id("absent-checkpoint"), &mut budget)
                .await
                .unwrap_err();
        assert_eq!(error.code, WORKING_DIFF_RECIPE_FALLBACK_CODE);
        assert_eq!(budget, 0);
    }

    #[tokio::test]
    async fn cycles_are_corruption_and_recipe_predicate_is_branch_independent() {
        let head = id("cycle-head");
        let mut graph = Graph::default();
        graph.nodes.insert(head, node(head, Some(head), 1));
        let mut budget = 4;
        let error = prove_first_parent_ancestor(&mut graph, head, id("cycle-target"), &mut budget)
            .await
            .unwrap_err();
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);

        let addressed = id("addressed-head");
        let mismatched = id("mismatched-node");
        let mut graph = Graph::default();
        graph.nodes.insert(addressed, node(mismatched, None, 1));
        let mut budget = 1;
        let error = prove_first_parent_ancestor(&mut graph, addressed, addressed, &mut budget)
            .await
            .unwrap_err();
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);

        assert!(is_supported_working_diff_recipe(&working_file_interest()));
        let mut missing_branch = working_file_interest();
        if let LogicalReadInterest::Diff { branch_id, .. } = &mut missing_branch {
            *branch_id = None;
        }
        assert!(!is_supported_working_diff_recipe(&missing_branch));

        let mut deferred = Graph {
            missing_as_history_demand: true,
            ..Graph::default()
        };
        let mut budget = 1;
        let error = prove_first_parent_ancestor(
            &mut deferred,
            id("deferred-head"),
            id("deferred-checkpoint"),
            &mut budget,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, WORKING_DIFF_RECIPE_FALLBACK_CODE);
    }
}
