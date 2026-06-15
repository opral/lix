use std::collections::{BTreeMap, BTreeSet};

use crate::common::compose_directory_path;
use crate::sql2::history_route::HistoryEntry;

pub(super) trait HistoryDirectoryPathRecord {
    fn id(&self) -> &str;
    fn parent_id(&self) -> Option<&str>;
    fn name(&self) -> Option<&str>;
    fn entry(&self) -> &HistoryEntry;
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
