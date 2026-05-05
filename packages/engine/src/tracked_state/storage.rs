use crate::storage::{
    KvGetGroup, KvGetRequest, KvWriteBatch, KvWriteGroup, StorageReader, StorageWriter,
};
use crate::tracked_state::codec::PendingChunkWrite;
use crate::tracked_state::tree_types::{TrackedStateRootId, TRACKED_STATE_HASH_BYTES};
use crate::LixError;

pub(crate) const TRACKED_STATE_CHUNK_NAMESPACE: &str = "tracked_state.tree.chunk";
pub(crate) const TRACKED_STATE_ROOT_NAMESPACE: &str = "tracked_state.tree.root";
pub(crate) const TRACKED_STATE_BY_FILE_ROOT_NAMESPACE: &str = "tracked_state.tree.root.by_file";

async fn get_one(
    store: &mut (impl StorageReader + ?Sized),
    namespace: &str,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, LixError> {
    Ok(store
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: namespace.to_string(),
                keys: vec![key],
            }],
        })
        .await?
        .groups
        .into_iter()
        .next()
        .and_then(|group| group.single_value_owned()))
}

async fn put_one(
    writer: &mut (impl StorageWriter + ?Sized),
    namespace: &str,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), LixError> {
    writer
        .write_kv_batch(KvWriteBatch {
            groups: {
                let mut group = KvWriteGroup::new(namespace);
                group.put(key, value);
                vec![group]
            },
        })
        .await?;
    Ok(())
}

pub(crate) async fn load_root(
    store: &mut (impl StorageReader + ?Sized),
    commit_id: &str,
) -> Result<Option<TrackedStateRootId>, LixError> {
    let Some(bytes) = get_one(
        store,
        TRACKED_STATE_ROOT_NAMESPACE,
        commit_id.as_bytes().to_vec(),
    )
    .await?
    else {
        return Ok(None);
    };
    TrackedStateRootId::from_slice(&bytes).map(Some)
}

pub(crate) async fn store_root(
    writer: &mut impl StorageWriter,
    commit_id: &str,
    root_id: &TrackedStateRootId,
) -> Result<(), LixError> {
    put_one(
        writer,
        TRACKED_STATE_ROOT_NAMESPACE,
        commit_id.as_bytes().to_vec(),
        root_id.as_bytes().to_vec(),
    )
    .await
}

pub(crate) async fn load_by_file_root(
    store: &mut (impl StorageReader + ?Sized),
    commit_id: &str,
) -> Result<Option<TrackedStateRootId>, LixError> {
    let Some(bytes) = get_one(
        store,
        TRACKED_STATE_BY_FILE_ROOT_NAMESPACE,
        commit_id.as_bytes().to_vec(),
    )
    .await?
    else {
        return Ok(None);
    };
    TrackedStateRootId::from_slice(&bytes).map(Some)
}

pub(crate) async fn store_by_file_root(
    writer: &mut impl StorageWriter,
    commit_id: &str,
    root_id: &TrackedStateRootId,
) -> Result<(), LixError> {
    put_one(
        writer,
        TRACKED_STATE_BY_FILE_ROOT_NAMESPACE,
        commit_id.as_bytes().to_vec(),
        root_id.as_bytes().to_vec(),
    )
    .await
}

#[cfg(test)]
pub(crate) async fn delete_root(
    writer: &mut (impl StorageWriter + ?Sized),
    commit_id: &str,
) -> Result<(), LixError> {
    writer
        .write_kv_batch(KvWriteBatch {
            groups: {
                let mut root = KvWriteGroup::new(TRACKED_STATE_ROOT_NAMESPACE);
                root.delete(commit_id.as_bytes());
                let mut by_file = KvWriteGroup::new(TRACKED_STATE_BY_FILE_ROOT_NAMESPACE);
                by_file.delete(commit_id.as_bytes());
                vec![root, by_file]
            },
        })
        .await?;
    Ok(())
}

pub(crate) async fn read_chunk(
    store: &mut impl StorageReader,
    hash: &[u8; TRACKED_STATE_HASH_BYTES],
) -> Result<Option<Vec<u8>>, LixError> {
    get_one(store, TRACKED_STATE_CHUNK_NAMESPACE, hash.to_vec()).await
}

pub(crate) fn verify_chunk_hash(
    expected: &[u8; TRACKED_STATE_HASH_BYTES],
    bytes: &[u8],
) -> Result<(), LixError> {
    let actual = crate::tracked_state::codec::hash_bytes(bytes);
    if &actual != expected {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state chunk hash mismatch",
        ));
    }
    Ok(())
}

pub(crate) async fn write_chunks(
    writer: &mut impl StorageWriter,
    chunks: &[PendingChunkWrite],
) -> Result<(), LixError> {
    for chunk in chunks {
        put_one(
            writer,
            TRACKED_STATE_CHUNK_NAMESPACE,
            chunk.hash.to_vec(),
            chunk.data.clone(),
        )
        .await?;
    }
    Ok(())
}
