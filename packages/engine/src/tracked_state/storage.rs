use crate::backend::{KvScanRange, KvStore, KvWriter};
use crate::tracked_state::codec::PendingChunkWrite;
use crate::tracked_state::tree_types::{TrackedStateRootId, TRACKED_STATE_HASH_BYTES};
use crate::LixError;

pub(crate) const TRACKED_STATE_CHUNK_NAMESPACE: &str = "tracked_state.tree.chunk";
pub(crate) const TRACKED_STATE_ROOT_NAMESPACE: &str = "tracked_state.tree.root";

pub(crate) async fn load_root(
    store: &mut (impl KvStore + ?Sized),
    commit_id: &str,
) -> Result<Option<TrackedStateRootId>, LixError> {
    let Some(bytes) = store
        .kv_get(TRACKED_STATE_ROOT_NAMESPACE, commit_id.as_bytes())
        .await?
    else {
        return Ok(None);
    };
    TrackedStateRootId::from_slice(&bytes).map(Some)
}

pub(crate) async fn store_root(
    writer: &mut impl KvWriter,
    commit_id: &str,
    root_id: &TrackedStateRootId,
) -> Result<(), LixError> {
    writer
        .kv_put(
            TRACKED_STATE_ROOT_NAMESPACE,
            commit_id.as_bytes(),
            root_id.as_bytes(),
        )
        .await
}

/// Deletes the root pointer for a commit.
///
/// Chunks remain content-addressed facts. Removing the root only makes that
/// commit's tracked-state projection unavailable until it is rebuilt from the
/// changelog.
#[cfg(test)]
pub(crate) async fn delete_root(
    writer: &mut (impl KvWriter + ?Sized),
    commit_id: &str,
) -> Result<(), LixError> {
    writer
        .kv_delete(TRACKED_STATE_ROOT_NAMESPACE, commit_id.as_bytes())
        .await
}

pub(crate) async fn read_chunk(
    store: &mut impl KvStore,
    hash: &[u8; TRACKED_STATE_HASH_BYTES],
) -> Result<Option<Vec<u8>>, LixError> {
    store.kv_get(TRACKED_STATE_CHUNK_NAMESPACE, hash).await
}

pub(crate) async fn write_chunks(
    writer: &mut impl KvWriter,
    chunks: &[PendingChunkWrite],
) -> Result<(), LixError> {
    for chunk in chunks {
        writer
            .kv_put(TRACKED_STATE_CHUNK_NAMESPACE, &chunk.hash, &chunk.data)
            .await?;
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) async fn scan_roots(
    store: &mut impl KvStore,
) -> Result<Vec<(String, TrackedStateRootId)>, LixError> {
    let pairs = store
        .kv_scan(
            TRACKED_STATE_ROOT_NAMESPACE,
            KvScanRange::prefix(Vec::new()),
            None,
        )
        .await?;
    pairs
        .into_iter()
        .map(|pair| {
            let commit_id = String::from_utf8(pair.key).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("tracked-state tree root key is invalid UTF-8: {error}"),
                )
            })?;
            let root_id = TrackedStateRootId::from_slice(&pair.value)?;
            Ok((commit_id, root_id))
        })
        .collect()
}

pub(crate) fn verify_chunk_hash(
    expected_hash: &[u8; TRACKED_STATE_HASH_BYTES],
    bytes: &[u8],
) -> Result<(), LixError> {
    let actual = crate::tracked_state::codec::hash_bytes(bytes);
    if &actual == expected_hash {
        return Ok(());
    }
    Err(LixError::new(
        "LIX_ERROR_UNKNOWN",
        "tracked-state tree chunk hash mismatch",
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};

    #[tokio::test]
    async fn root_roundtrips_through_kv_storage() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let root = TrackedStateRootId::new([7_u8; TRACKED_STATE_HASH_BYTES]);

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        store_root(&mut transaction.as_mut(), "commit-1", &root)
            .await
            .expect("root should store");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut store = Arc::clone(&backend);
        assert_eq!(
            load_root(&mut store, "commit-1")
                .await
                .expect("root should load"),
            Some(root)
        );
    }

    #[tokio::test]
    async fn chunk_roundtrips_through_kv_storage() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let data = b"chunk-data".to_vec();
        let chunk = PendingChunkWrite {
            hash: crate::tracked_state::codec::hash_bytes(&data),
            data: data.clone(),
        };

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        write_chunks(&mut transaction.as_mut(), std::slice::from_ref(&chunk))
            .await
            .expect("chunk should write");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut store = Arc::clone(&backend);
        assert_eq!(
            read_chunk(&mut store, &chunk.hash)
                .await
                .expect("chunk should read"),
            Some(data)
        );
    }
}
