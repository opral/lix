use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use crate::LixError;
use crate::common::compose_directory_path;

pub(crate) trait DirectoryPathRecord {
    type Key: Clone + Debug + Ord;

    fn parent_key(&self, key: &Self::Key) -> Option<Self::Key>;
    fn name(&self) -> &str;
}

pub(crate) fn derive_directory_paths<'a, R>(
    rows: impl IntoIterator<Item = (R::Key, &'a R)>,
) -> Result<BTreeMap<R::Key, String>, LixError>
where
    R: DirectoryPathRecord + 'a,
{
    let records = rows.into_iter().collect::<BTreeMap<_, _>>();
    let mut paths = BTreeMap::new();
    for directory_id in records.keys() {
        derive_directory_path_for(directory_id, &records, &mut paths, &mut BTreeSet::new())?;
    }
    Ok(paths)
}

fn derive_directory_path_for<R>(
    directory_id: &R::Key,
    records: &BTreeMap<R::Key, &R>,
    paths: &mut BTreeMap<R::Key, String>,
    visiting: &mut BTreeSet<R::Key>,
) -> Result<Option<String>, LixError>
where
    R: DirectoryPathRecord,
{
    if let Some(path) = paths.get(directory_id) {
        return Ok(Some(path.clone()));
    }
    if !visiting.insert(directory_id.clone()) {
        return Err(directory_parent_cycle_error(directory_id));
    }
    let Some(row) = records.get(directory_id) else {
        visiting.remove(directory_id);
        return Ok(None);
    };
    let path = match row.parent_key(directory_id) {
        Some(parent_key) => {
            let Some(parent_path) =
                derive_directory_path_for(&parent_key, records, paths, visiting)?
            else {
                visiting.remove(directory_id);
                return Ok(None);
            };
            compose_directory_path(Some(&parent_path), row.name())?
        }
        None => compose_directory_path(None, row.name())?,
    };
    visiting.remove(directory_id);
    paths.insert(directory_id.clone(), path.clone());
    Ok(Some(path))
}

fn directory_parent_cycle_error(directory_id: &impl Debug) -> LixError {
    LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        format!(
            "lix_directory_descriptor parent_id cycle while resolving directory {directory_id:?}"
        ),
    )
}
