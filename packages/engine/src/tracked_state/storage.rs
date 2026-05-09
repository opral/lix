use std::collections::HashMap;

use crate::storage::{KvGetGroup, KvGetRequest, StorageReader, StorageWriteSet};
use crate::tracked_state::codec::PendingChunkWrite;
use crate::tracked_state::types::{
    TrackedStateDeltaEntry, TrackedStateRootId, TRACKED_STATE_HASH_BYTES,
};
use crate::LixError;

pub(crate) const TRACKED_STATE_CHUNK_NAMESPACE: &'static str = "tracked_state.tree.chunk";
pub(crate) const TRACKED_STATE_ROOT_NAMESPACE: &'static str = "tracked_state.tree.root";
pub(crate) const TRACKED_STATE_BY_FILE_ROOT_NAMESPACE: &'static str =
    "tracked_state.tree.root.by_file";
pub(crate) const TRACKED_STATE_DELTA_PACK_NAMESPACE: &'static str = "tracked_state.delta_pack";

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

pub(crate) fn stage_root(
    writes: &mut StorageWriteSet,
    commit_id: &str,
    root_id: &TrackedStateRootId,
) {
    writes.put(
        TRACKED_STATE_ROOT_NAMESPACE,
        commit_id.as_bytes().to_vec(),
        root_id.as_bytes().to_vec(),
    );
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

pub(crate) fn stage_by_file_root(
    writes: &mut StorageWriteSet,
    commit_id: &str,
    root_id: &TrackedStateRootId,
) {
    writes.put(
        TRACKED_STATE_BY_FILE_ROOT_NAMESPACE,
        commit_id.as_bytes().to_vec(),
        root_id.as_bytes().to_vec(),
    );
}

pub(crate) async fn load_delta_pack(
    store: &mut (impl StorageReader + ?Sized),
    commit_id: &str,
) -> Result<Option<Vec<TrackedStateDeltaEntry>>, LixError> {
    let Some(bytes) = get_one(
        store,
        TRACKED_STATE_DELTA_PACK_NAMESPACE,
        commit_id.as_bytes().to_vec(),
    )
    .await?
    else {
        return Ok(None);
    };
    crate::tracked_state::codec::decode_delta_pack(&bytes).map(Some)
}

pub(crate) fn stage_delta_pack(
    writes: &mut StorageWriteSet,
    commit_id: &str,
    entries: &[TrackedStateDeltaEntry],
) -> Result<(), LixError> {
    writes.put(
        TRACKED_STATE_DELTA_PACK_NAMESPACE,
        commit_id.as_bytes().to_vec(),
        crate::tracked_state::codec::encode_delta_pack(entries)?,
    );
    Ok(())
}

pub(crate) async fn read_chunk(
    store: &mut (impl StorageReader + ?Sized),
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

pub(crate) fn stage_chunks(writes: &mut StorageWriteSet, chunks: &[PendingChunkWrite]) {
    for chunk in chunks {
        writes.put(
            TRACKED_STATE_CHUNK_NAMESPACE,
            chunk.hash.to_vec(),
            chunk.data.clone(),
        );
    }
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub(crate) struct TrackedStateChunkOverlay {
    chunks: HashMap<[u8; TRACKED_STATE_HASH_BYTES], Vec<u8>>,
}

impl TrackedStateChunkOverlay {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn read_chunk(
        &self,
        store: &mut (impl StorageReader + ?Sized),
        hash: &[u8; TRACKED_STATE_HASH_BYTES],
    ) -> Result<Option<Vec<u8>>, LixError> {
        if let Some(bytes) = self.chunks.get(hash) {
            return Ok(Some(bytes.clone()));
        }
        read_chunk(store, hash).await
    }

    pub(crate) fn stage_chunks(
        &mut self,
        writes: &mut StorageWriteSet,
        chunks: &[PendingChunkWrite],
    ) {
        for chunk in chunks {
            self.chunks.insert(chunk.hash, chunk.data.clone());
        }
        stage_chunks(writes, chunks);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    #[test]
    fn production_tracked_state_sources_do_not_call_storage_batch_writer() {
        let tracked_state_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/tracked_state");
        let forbidden = ["write", "kv", "batch"].join("_");

        for path in rust_sources(&tracked_state_dir) {
            let source =
                fs::read_to_string(&path).expect("tracked_state source should be readable");
            for (line_number, line) in production_lines(&source) {
                assert!(
                    !line.contains(&forbidden),
                    "production tracked_state source must stage into StorageWriteSet instead of calling {forbidden}: {}:{}",
                    path.display(),
                    line_number
                );
            }
        }
    }

    fn rust_sources(dir: &Path) -> Vec<PathBuf> {
        let mut sources = Vec::new();
        for entry in fs::read_dir(dir).expect("tracked_state source dir should be readable") {
            let path = entry
                .expect("tracked_state source entry should be readable")
                .path();
            if path.is_dir() {
                sources.extend(rust_sources(&path));
            } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
                sources.push(path);
            }
        }
        sources
    }

    fn production_lines(source: &str) -> Vec<(usize, &str)> {
        let mut lines = Vec::new();
        let mut skipping_cfg_test_item = false;
        let mut pending_cfg_test = false;
        let mut item_started = false;
        let mut brace_depth = 0i32;

        for (index, line) in source.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed == "#[cfg(test)]" {
                pending_cfg_test = true;
                continue;
            }

            if pending_cfg_test || skipping_cfg_test_item {
                if pending_cfg_test && !item_started && trimmed.ends_with(';') {
                    pending_cfg_test = false;
                    continue;
                }
                let opens = line.matches('{').count() as i32;
                let closes = line.matches('}').count() as i32;
                if opens > 0 {
                    item_started = true;
                    skipping_cfg_test_item = true;
                }
                if item_started {
                    brace_depth += opens - closes;
                    if brace_depth <= 0 {
                        pending_cfg_test = false;
                        skipping_cfg_test_item = false;
                        item_started = false;
                        brace_depth = 0;
                    }
                }
                continue;
            }

            lines.push((index + 1, line));
        }

        lines
    }
}
