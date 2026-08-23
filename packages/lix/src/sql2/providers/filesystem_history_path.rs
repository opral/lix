use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::LixError;
use crate::changelog::CommitId;
use crate::commit_graph::CommitGraphReader;
use crate::common::compose_directory_path;

pub(super) trait DirectoryPathRecord {
    fn id(&self) -> &str;
    fn parent_id(&self) -> Option<&str>;
    fn name(&self) -> Option<&str>;
}

/// Immutable child index for one exact observed filesystem state.
///
/// Composed file and directory history use this to fan a directory descriptor
/// change out to every descendant whose public path depends on it. The index is
/// built from an observed commit root, so equal-depth sibling commits never
/// share ancestry.
#[derive(Debug, Default)]
pub(super) struct HistoryDirectoryTree {
    children_by_parent: BTreeMap<String, BTreeSet<String>>,
    parent_by_directory: BTreeMap<String, String>,
}

impl HistoryDirectoryTree {
    pub(super) fn from_records<R: DirectoryPathRecord>(directories: &[R]) -> Self {
        let mut children_by_parent = BTreeMap::<String, BTreeSet<String>>::new();
        let mut parent_by_directory = BTreeMap::new();
        for directory in directories {
            if let Some(parent_id) = directory.parent_id() {
                children_by_parent
                    .entry(parent_id.to_string())
                    .or_default()
                    .insert(directory.id().to_string());
                parent_by_directory.insert(directory.id().to_string(), parent_id.to_string());
            }
        }
        Self {
            children_by_parent,
            parent_by_directory,
        }
    }

    /// Returns the changed directory and every directory below it.
    ///
    /// Including the root is useful for files directly owned by the changed
    /// directory. A visited set makes corrupt cycles terminate deterministically
    /// instead of multiplying history rows.
    pub(super) fn descendants_including(&self, directory_id: &str) -> BTreeSet<String> {
        let mut descendants = BTreeSet::new();
        let mut pending = vec![directory_id.to_string()];
        while let Some(candidate) = pending.pop() {
            if !descendants.insert(candidate.clone()) {
                continue;
            }
            if let Some(children) = self.children_by_parent.get(&candidate) {
                pending.extend(children.iter().rev().cloned());
            }
        }
        descendants
    }

    pub(super) fn has_ancestor_including(&self, directory_id: &str, ancestor_id: &str) -> bool {
        let mut current = Some(directory_id);
        let mut visited = BTreeSet::new();
        while let Some(candidate) = current {
            if candidate == ancestor_id {
                return true;
            }
            if !visited.insert(candidate) {
                return false;
            }
            current = self.parent_by_directory.get(candidate).map(String::as_str);
        }
        false
    }
}

/// Resolves a path inside one exact observed commit-root batch.
///
/// Unlike traversal history records, every row in this collection already
/// belongs to the same commit and depth. Avoid synthesizing a `HistoryEntry`
/// per row solely to satisfy those predicates.
pub(super) fn resolve_observed_directory_path<R: DirectoryPathRecord>(
    directory_id: &str,
    directories: &[R],
    cache: &mut BTreeMap<String, Option<String>>,
    visiting: &mut BTreeSet<String>,
) -> Option<String> {
    if let Some(path) = cache.get(directory_id) {
        return path.clone();
    }
    if !visiting.insert(directory_id.to_string()) {
        cache.insert(directory_id.to_string(), None);
        return None;
    }

    let directory = directories
        .iter()
        .find(|directory| directory.name().is_some() && directory.id() == directory_id)?;
    let name = directory.name()?;
    let path = match directory.parent_id() {
        Some(parent_id) => {
            let parent_path =
                resolve_observed_directory_path(parent_id, directories, cache, visiting)?;
            compose_directory_path(Some(&parent_path), name).ok()?
        }
        None => compose_directory_path(None, name).ok()?,
    };
    visiting.remove(directory_id);
    cache.insert(directory_id.to_string(), Some(path.clone()));
    Some(path)
}

/// Loads direct-parent edges for every commit reachable from the requested
/// history anchors.
///
/// Ancestor deletion and move-out events need both sides of the revision:
/// descendants may no longer be linked to the changed directory in the
/// observed root. Direct-parent roots are sufficient and preserve DAG
/// isolation; no depth-based predecessor is inferred.
pub(super) async fn load_history_commit_parents(
    commit_graph: &Arc<Mutex<Box<dyn CommitGraphReader>>>,
    as_of_commit_ids: &[String],
    max_depth: Option<u32>,
) -> Result<BTreeMap<String, Vec<String>>, LixError> {
    let mut parents_by_commit = BTreeMap::new();
    let mut commit_graph = commit_graph.lock().await;
    for as_of_commit_id in as_of_commit_ids {
        let as_of_commit_id =
            CommitId::parse_lix(as_of_commit_id, "history lixcol_as_of_commit_id")?;
        let reachable = match max_depth {
            Some(max_depth) => {
                commit_graph
                    .reachable_nodes_through_depth(&as_of_commit_id, max_depth)
                    .await?
            }
            None => commit_graph.reachable_nodes(&as_of_commit_id).await?,
        };
        for reachable in reachable.iter() {
            parents_by_commit.insert(
                reachable.commit.commit_id.to_string(),
                reachable
                    .commit
                    .parent_commit_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            );
        }
    }
    Ok(parents_by_commit)
}
