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
    /// directory. A depth-first active set makes corrupt cycles fail closed
    /// instead of silently truncating the history fan-out.
    pub(super) fn descendants_including(
        &self,
        directory_id: &str,
    ) -> Result<BTreeSet<String>, LixError> {
        let mut descendants = BTreeSet::new();
        let mut completed = BTreeSet::new();
        let mut active = BTreeSet::new();
        let mut pending = vec![(directory_id.to_string(), true)];
        while let Some((candidate, entering)) = pending.pop() {
            if !entering {
                active.remove(&candidate);
                completed.insert(candidate);
                continue;
            }
            if active.contains(&candidate) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("directory descendant graph contains a cycle at '{candidate}'"),
                ));
            }
            if completed.contains(&candidate) {
                continue;
            }
            active.insert(candidate.clone());
            descendants.insert(candidate.clone());
            pending.push((candidate.clone(), false));
            if let Some(children) = self.children_by_parent.get(&candidate) {
                pending.extend(children.iter().rev().cloned().map(|child| (child, true)));
            }
        }
        Ok(descendants)
    }

    pub(super) fn has_ancestor_including(
        &self,
        directory_id: &str,
        ancestor_id: &str,
    ) -> Result<bool, LixError> {
        let mut current = Some(directory_id);
        let mut visited = BTreeSet::new();
        while let Some(candidate) = current {
            if candidate == ancestor_id {
                return Ok(true);
            }
            if !visited.insert(candidate) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("directory ancestry contains a cycle at '{candidate}'"),
                ));
            }
            current = self.parent_by_directory.get(candidate).map(String::as_str);
        }
        Ok(false)
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
) -> Result<Option<String>, LixError> {
    if let Some(path) = cache.get(directory_id) {
        return Ok(path.clone());
    }
    if !visiting.insert(directory_id.to_string()) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("directory parent cycle while resolving '{directory_id}'"),
        ));
    }

    let directory = directories
        .iter()
        .find(|directory| directory.id() == directory_id)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "directory '{directory_id}' is missing from the authenticated history root"
                ),
            )
        })?;
    let name = directory.name().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("directory '{directory_id}' is missing its authenticated name"),
        )
    })?;
    let path = match directory.parent_id() {
        Some(parent_id) => {
            let parent_path =
                resolve_observed_directory_path(parent_id, directories, cache, visiting)?
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("directory '{directory_id}' has no authenticated parent path"),
                        )
                    })?;
            compose_directory_path(Some(&parent_path), name)?
        }
        None => compose_directory_path(None, name)?,
    };
    visiting.remove(directory_id);
    cache.insert(directory_id.to_string(), Some(path.clone()));
    Ok(Some(path))
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
) -> Result<BTreeMap<String, Vec<String>>, LixError> {
    let mut parents_by_commit = BTreeMap::new();
    let mut commit_graph = commit_graph.lock().await;
    for as_of_commit_id in as_of_commit_ids {
        let as_of_commit_id =
            CommitId::parse_lix(as_of_commit_id, "history lixcol_as_of_commit_id")?;
        for reachable in commit_graph.reachable_nodes(&as_of_commit_id).await?.iter() {
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
    for (commit_id, parents) in &parents_by_commit {
        for parent_id in parents {
            if !parents_by_commit.contains_key(parent_id) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("history commit '{commit_id}' references missing parent '{parent_id}'"),
                ));
            }
        }
        let mut current = Some(commit_id.as_str());
        let mut visiting = BTreeSet::new();
        while let Some(current_id) = current {
            if !visiting.insert(current_id) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("history commit ancestry contains a cycle at '{current_id}'"),
                ));
            }
            let parent_list: Option<&Vec<String>> = parents_by_commit.get(current_id);
            current = parent_list
                .and_then(|parents| parents.first())
                .map(String::as_str);
        }
    }
    Ok(parents_by_commit)
}

#[cfg(test)]
mod tests {
    use super::{DirectoryPathRecord, resolve_observed_directory_path};
    use std::collections::{BTreeMap, BTreeSet};

    struct Directory {
        id: &'static str,
        parent_id: Option<&'static str>,
        name: Option<&'static str>,
    }

    impl DirectoryPathRecord for Directory {
        fn id(&self) -> &str {
            self.id
        }

        fn parent_id(&self) -> Option<&str> {
            self.parent_id
        }

        fn name(&self) -> Option<&str> {
            self.name
        }
    }

    #[test]
    fn missing_parent_is_typed_failure() {
        let directories = [Directory {
            id: "child",
            parent_id: Some("missing"),
            name: Some("child"),
        }];
        let result = resolve_observed_directory_path(
            "child",
            &directories,
            &mut BTreeMap::new(),
            &mut BTreeSet::new(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn parent_cycle_is_typed_failure() {
        let directories = [
            Directory {
                id: "a",
                parent_id: Some("b"),
                name: Some("a"),
            },
            Directory {
                id: "b",
                parent_id: Some("a"),
                name: Some("b"),
            },
        ];
        let result = resolve_observed_directory_path(
            "a",
            &directories,
            &mut BTreeMap::new(),
            &mut BTreeSet::new(),
        );
        assert!(result.is_err());
    }
}
