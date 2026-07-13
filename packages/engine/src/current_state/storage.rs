use bytes::Bytes;

use crate::LixError;
use crate::storage::{
    PointReadPlan, StorageGetOptions, StorageKey, StorageProjectedValue, StorageRead, StorageSpace,
    StorageSpaceId, StorageValue, StorageWriteSet,
};
use crate::tracked_state::TrackedStateRootId;

pub(crate) const CURRENT_STATE_BRANCH_ROOT_NAMESPACE: &str = "current_state.branch_root.v1";
pub(crate) const CURRENT_STATE_BRANCH_ROOT_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_0005),
    CURRENT_STATE_BRANCH_ROOT_NAMESPACE,
);

pub(crate) fn load_branch_root(
    store: &(impl StorageRead + ?Sized),
    branch_id: &str,
) -> Result<Option<TrackedStateRootId>, LixError> {
    let result = PointReadPlan::new(
        CURRENT_STATE_BRANCH_ROOT_SPACE,
        &[branch_root_key(branch_id)],
    )
    .materialize(store, StorageGetOptions::default())?;
    let value = result.value.into_iter().next().flatten();
    match value {
        Some(StorageProjectedValue::FullValue(bytes)) => {
            crate::storage_codec::decode("current-state branch root", &bytes).map(Some)
        }
        Some(StorageProjectedValue::KeyOnly) | None => Ok(None),
    }
}

pub(crate) fn stage_branch_root(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    root_id: &TrackedStateRootId,
) -> Result<(), LixError> {
    writes.put(
        CURRENT_STATE_BRANCH_ROOT_SPACE,
        branch_root_key(branch_id),
        StorageValue {
            bytes: Bytes::from(crate::storage_codec::encode(
                "current-state branch root",
                root_id,
            )?),
        },
    );
    Ok(())
}

pub(crate) fn stage_delete_branch_root(writes: &mut StorageWriteSet, branch_id: &str) {
    writes.delete(CURRENT_STATE_BRANCH_ROOT_SPACE, branch_root_key(branch_id));
}

fn branch_root_key(branch_id: &str) -> StorageKey {
    StorageKey(Bytes::copy_from_slice(branch_id.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{
        InMemoryStorageBackend, StorageContext, StorageReadOptions, StorageWriteOptions,
    };

    #[test]
    fn branch_root_roundtrips() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let root_id = TrackedStateRootId::new([7; 32]);
        let mut writes = storage.new_write_set();
        stage_branch_root(&mut writes, "branch-東京", &root_id).expect("branch root should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("branch root should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        assert_eq!(
            load_branch_root(&read, "branch-東京").expect("branch root should load"),
            Some(root_id)
        );
        assert_eq!(
            load_branch_root(&read, "missing").expect("missing branch should load"),
            None
        );
    }
}
