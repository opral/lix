use std::collections::{HashMap, HashSet};

use async_trait::async_trait;

use super::types::{SegmentCommit, StateRowIdentity};
use crate::LixError;

#[derive(Default)]
pub(super) struct SourceParentFacts {
    pub(super) reachable_memberships: HashSet<String>,
    pub(super) first_parent_winners: HashMap<StateRowIdentity, String>,
}

#[async_trait(?Send)]
pub(super) trait CommitGraphLoader {
    async fn load_commit(&mut self, commit_id: &str) -> Result<Option<SegmentCommit>, LixError>;
}

pub(super) async fn source_parent_facts<L>(
    loader: &mut L,
    root_commit_id: &str,
) -> Result<SourceParentFacts, LixError>
where
    L: CommitGraphLoader + ?Sized,
{
    let mut facts = SourceParentFacts::default();
    let mut stack = vec![root_commit_id.to_string()];
    let mut visited = HashSet::new();
    while let Some(commit_id) = stack.pop() {
        if !visited.insert(commit_id.clone()) {
            continue;
        }
        let Some(commit) = loader.load_commit(&commit_id).await? else {
            continue;
        };
        facts.reachable_memberships.extend(
            commit
                .body
                .membership
                .iter()
                .map(|membership| membership.member_change_id.clone()),
        );
        stack.extend(commit.header.parent_commit_ids);
    }

    let mut next_commit_id = Some(root_commit_id.to_string());
    let mut visited = HashSet::new();
    while let Some(commit_id) = next_commit_id.take() {
        if !visited.insert(commit_id.clone()) {
            return Err(LixError::unknown(format!(
                "cannot resolve source parent facts because first-parent history contains parent cycle at commit '{commit_id}'"
            )));
        }
        let Some(commit) = loader.load_commit(&commit_id).await? else {
            break;
        };
        for (identity, change_id) in &commit.directory.state_row_identities {
            facts
                .first_parent_winners
                .entry(identity.clone())
                .or_insert_with(|| change_id.clone());
        }
        next_commit_id = commit.header.parent_commit_ids.first().cloned();
    }
    Ok(facts)
}

pub(super) async fn commit_history_contains_membership<L>(
    loader: &mut L,
    root_commit_id: &str,
    change_id: &str,
) -> Result<bool, LixError>
where
    L: CommitGraphLoader + ?Sized,
{
    let mut stack = vec![root_commit_id.to_string()];
    let mut visited = HashSet::new();
    while let Some(commit_id) = stack.pop() {
        if !visited.insert(commit_id.clone()) {
            continue;
        }
        let Some(commit) = loader.load_commit(&commit_id).await? else {
            continue;
        };
        if commit
            .body
            .membership
            .iter()
            .any(|membership| membership.member_change_id == change_id)
        {
            return Ok(true);
        }
        stack.extend(commit.header.parent_commit_ids);
    }
    Ok(false)
}

pub(super) async fn commit_history_projects_state_row<L>(
    loader: &mut L,
    root_commit_id: &str,
    identity: &StateRowIdentity,
    change_id: &str,
) -> Result<bool, LixError>
where
    L: CommitGraphLoader + ?Sized,
{
    let mut next_commit_id = Some(root_commit_id.to_string());
    let mut visited = HashSet::new();
    while let Some(commit_id) = next_commit_id.take() {
        if !visited.insert(commit_id.clone()) {
            return Err(LixError::unknown(format!(
                "cannot resolve StateRowIdentity winner for {:?} because first-parent history contains cycle at commit '{}'",
                identity, commit_id
            )));
        }
        let Some(commit) = loader.load_commit(&commit_id).await? else {
            return Ok(false);
        };
        if let Some((_, winner_change_id)) = commit
            .directory
            .state_row_identities
            .iter()
            .find(|(candidate, _)| candidate == identity)
        {
            return Ok(winner_change_id == change_id);
        }
        next_commit_id = commit.header.parent_commit_ids.first().cloned();
    }
    Ok(false)
}
