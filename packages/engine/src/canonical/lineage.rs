use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::roots::ResolvedRootCommit;

// Canonical owns commit lineage semantics even when live_state consumes the
// resulting maps for query-serving rebuilds.
pub(crate) type VersionHeadMap = BTreeMap<String, Vec<String>>;
pub(crate) type VersionCommitDepthMap = BTreeMap<(String, String), usize>;

pub(crate) fn collect_commit_edges<I, J>(
    commit_parent_sets: I,
    explicit_edges: J,
) -> BTreeSet<(String, String)>
where
    I: IntoIterator<Item = (String, Vec<String>)>,
    J: IntoIterator<Item = (String, String)>,
{
    let mut edges = BTreeSet::new();

    for (child_id, parent_ids) in commit_parent_sets {
        if child_id.is_empty() {
            continue;
        }
        for parent_id in parent_ids {
            if parent_id.is_empty() {
                continue;
            }
            edges.insert((parent_id, child_id.clone()));
        }
    }

    for (parent_id, child_id) in explicit_edges {
        if parent_id.is_empty() || child_id.is_empty() {
            continue;
        }
        edges.insert((parent_id, child_id));
    }

    edges
}

pub(crate) fn build_version_head_map(root_version_refs: &[ResolvedRootCommit]) -> VersionHeadMap {
    let mut heads = BTreeMap::new();

    for row in root_version_refs {
        if row.version_id.is_empty() || row.commit_id.is_empty() {
            continue;
        }
        heads
            .entry(row.version_id.clone())
            .or_insert_with(Vec::new)
            .push(row.commit_id.clone());
    }

    for commit_ids in heads.values_mut() {
        commit_ids.sort();
        commit_ids.dedup();
    }

    heads
}

pub(crate) fn build_version_commit_depth_map(
    version_heads: &VersionHeadMap,
    all_commit_edges: &BTreeSet<(String, String)>,
) -> VersionCommitDepthMap {
    let parents_by_child = parents_by_child(all_commit_edges);
    let mut queue = VecDeque::new();

    for (version_id, tips) in version_heads {
        for tip in tips {
            queue.push_back((version_id.clone(), tip.clone(), 0usize));
        }
    }

    let mut min_depth = BTreeMap::new();
    while let Some((version_id, commit_id, depth)) = queue.pop_front() {
        let key = (version_id.clone(), commit_id.clone());
        if let Some(existing_depth) = min_depth.get(&key) {
            if *existing_depth <= depth {
                continue;
            }
        }
        min_depth.insert(key, depth);

        if let Some(parents) = parents_by_child.get(&commit_id) {
            for parent_id in parents {
                queue.push_back((version_id.clone(), parent_id.clone(), depth + 1));
            }
        }
    }

    min_depth
}

pub(crate) fn min_depth_by_commit(
    commit_depths: &VersionCommitDepthMap,
) -> BTreeMap<String, usize> {
    let mut min_depth = BTreeMap::new();

    for ((_, commit_id), depth) in commit_depths {
        min_depth
            .entry(commit_id.clone())
            .and_modify(|existing: &mut usize| {
                if *depth < *existing {
                    *existing = *depth;
                }
            })
            .or_insert(*depth);
    }

    min_depth
}

fn parents_by_child(
    all_commit_edges: &BTreeSet<(String, String)>,
) -> BTreeMap<String, Vec<String>> {
    let mut parent_by_child = BTreeMap::new();

    for (parent_id, child_id) in all_commit_edges {
        parent_by_child
            .entry(child_id.clone())
            .or_insert_with(Vec::new)
            .push(parent_id.clone());
    }

    for parents in parent_by_child.values_mut() {
        parents.sort();
        parents.dedup();
    }

    parent_by_child
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_commit_edges_merges_snapshot_and_explicit_edges() {
        let edges = collect_commit_edges(
            [
                ("commit-2".to_string(), vec!["commit-1".to_string()]),
                (
                    "commit-3".to_string(),
                    vec!["commit-2".to_string(), "commit-1".to_string()],
                ),
            ],
            [
                ("commit-1".to_string(), "commit-2".to_string()),
                ("commit-2".to_string(), "commit-3".to_string()),
            ],
        );

        assert_eq!(
            edges,
            BTreeSet::from([
                ("commit-1".to_string(), "commit-2".to_string()),
                ("commit-1".to_string(), "commit-3".to_string()),
                ("commit-2".to_string(), "commit-3".to_string()),
            ])
        );
    }

    #[test]
    fn version_head_map_sorts_and_deduplicates_heads() {
        let heads = build_version_head_map(&[
            ResolvedRootCommit {
                version_id: "main".to_string(),
                commit_id: "commit-b".to_string(),
            },
            ResolvedRootCommit {
                version_id: "main".to_string(),
                commit_id: "commit-a".to_string(),
            },
            ResolvedRootCommit {
                version_id: "main".to_string(),
                commit_id: "commit-a".to_string(),
            },
        ]);

        assert_eq!(
            heads.get("main"),
            Some(&vec!["commit-a".to_string(), "commit-b".to_string()])
        );
    }

    #[test]
    fn version_commit_depth_map_walks_parents_per_version() {
        let version_heads = BTreeMap::from([
            ("main".to_string(), vec!["commit-3".to_string()]),
            ("feature".to_string(), vec!["commit-2".to_string()]),
        ]);
        let edges = BTreeSet::from([
            ("commit-1".to_string(), "commit-2".to_string()),
            ("commit-2".to_string(), "commit-3".to_string()),
        ]);

        let commit_depths = build_version_commit_depth_map(&version_heads, &edges);

        assert_eq!(
            commit_depths.get(&("main".to_string(), "commit-3".to_string())),
            Some(&0)
        );
        assert_eq!(
            commit_depths.get(&("main".to_string(), "commit-2".to_string())),
            Some(&1)
        );
        assert_eq!(
            commit_depths.get(&("main".to_string(), "commit-1".to_string())),
            Some(&2)
        );
        assert_eq!(
            commit_depths.get(&("feature".to_string(), "commit-2".to_string())),
            Some(&0)
        );
        assert_eq!(
            commit_depths.get(&("feature".to_string(), "commit-1".to_string())),
            Some(&1)
        );
    }
}
