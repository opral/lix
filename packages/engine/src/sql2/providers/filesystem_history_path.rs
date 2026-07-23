use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::LixError;
use crate::changelog::CommitId;
use crate::commit_graph::CommitGraphReader;
use crate::common::compose_directory_path;
use crate::sql2::history_route::HistoryEntry;

pub(super) trait HistoryDirectoryPathRecord {
    fn id(&self) -> &str;
    fn parent_id(&self) -> Option<&str>;
    fn name(&self) -> Option<&str>;
    fn entry(&self) -> &HistoryEntry;
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
    pub(super) fn from_records<R: HistoryDirectoryPathRecord>(directories: &[R]) -> Self {
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

/// Loads direct-parent edges for every commit reachable from the requested
/// history starts.
///
/// Ancestor deletion and move-out events need both sides of the revision:
/// descendants may no longer be linked to the changed directory in the
/// observed root. Direct-parent roots are sufficient and preserve DAG
/// isolation; no depth-based predecessor is inferred.
pub(super) async fn load_history_commit_parents(
    commit_graph: &Arc<Mutex<Box<dyn CommitGraphReader>>>,
    start_commit_ids: &[String],
) -> Result<BTreeMap<String, Vec<String>>, LixError> {
    let mut parents_by_commit = BTreeMap::new();
    let mut commit_graph = commit_graph.lock().await;
    for start_commit_id in start_commit_ids {
        let start_commit_id = CommitId::parse_lix(start_commit_id, "history start_commit_id")?;
        for reachable in commit_graph.reachable_commits(&start_commit_id).await? {
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

pub(super) fn resolve_history_directory_path<R: HistoryDirectoryPathRecord>(
    directory_id: &str,
    start_commit_id: &str,
    target_depth: u32,
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
        .filter(|directory| {
            let entry = directory.entry();
            directory.name().is_some()
                && directory.id() == directory_id
                && entry.start_commit_id == start_commit_id
                && entry.depth >= target_depth
        })
        .min_by(|left, right| {
            let left_entry = left.entry();
            let right_entry = right.entry();
            left_entry
                .depth
                .cmp(&right_entry.depth)
                .then(left_entry.change.id.cmp(&right_entry.change.id))
        })?;

    let name = directory.name()?;
    let path = match directory.parent_id() {
        Some(parent_id) => {
            let parent_path = resolve_history_directory_path(
                parent_id,
                start_commit_id,
                target_depth,
                directories,
                cache,
                visiting,
            )?;
            compose_directory_path(Some(&parent_path), name).ok()?
        }
        None => compose_directory_path(None, name).ok()?,
    };
    visiting.remove(directory_id);
    cache.insert(directory_id.to_string(), Some(path.clone()));
    Some(path)
}
