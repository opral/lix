use std::collections::HashMap;

use crate::storage::{PointReadPlan, StorageRead, StorageSpace, StorageWriteSet};
use crate::storage::{
    StorageGetOptions, StorageKey, StorageProjectedValue, StorageSpaceId, StorageValue,
};
use crate::tracked_state::codec::PendingChunkWrite;
use crate::tracked_state::types::{
    TrackedStateProjectionMetadata, TrackedStateProjectionParent, TrackedStateRootId,
    TRACKED_STATE_HASH_BYTES,
};
use crate::LixError;
use bytes::Bytes;

pub(crate) const TRACKED_STATE_CHUNK_NAMESPACE: &'static str = "tracked_state.tree.chunk";
pub(crate) const TRACKED_STATE_PROJECTION_NAMESPACE: &'static str = "tracked_state.projection";
pub(crate) const TRACKED_STATE_BY_FILE_ROOT_NAMESPACE: &'static str =
    "tracked_state.tree.root.by_file";
pub(crate) const TRACKED_STATE_CHUNK_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_0001), TRACKED_STATE_CHUNK_NAMESPACE);
pub(crate) const TRACKED_STATE_BY_FILE_ROOT_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_0003),
    TRACKED_STATE_BY_FILE_ROOT_NAMESPACE,
);
pub(crate) const TRACKED_STATE_PROJECTION_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_0004),
    TRACKED_STATE_PROJECTION_NAMESPACE,
);

const PROJECTION_METADATA_MAGIC: &[u8; 5] = b"LXTP1";

async fn get_one(
    store: &(impl StorageRead + ?Sized),
    space: StorageSpace,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, LixError> {
    let result = PointReadPlan::new(space, &[StorageKey(Bytes::from(key))])
        .materialize(store, StorageGetOptions::default())?;
    Ok(result
        .value
        .into_iter()
        .next()
        .flatten()
        .and_then(full_value))
}

pub(crate) async fn load_root(
    store: &(impl StorageRead + ?Sized),
    commit_id: &str,
) -> Result<Option<TrackedStateRootId>, LixError> {
    Ok(load_projection_metadata(store, commit_id)
        .await?
        .map(|metadata| metadata.root_id))
}

pub(crate) async fn load_projection_metadata(
    store: &(impl StorageRead + ?Sized),
    commit_id: &str,
) -> Result<Option<TrackedStateProjectionMetadata>, LixError> {
    let Some(bytes) = get_one(
        store,
        TRACKED_STATE_PROJECTION_SPACE,
        commit_id.as_bytes().to_vec(),
    )
    .await?
    else {
        return Ok(None);
    };
    let metadata = decode_projection_metadata(&bytes)?;
    if metadata.commit_id != commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state projection key for commit '{commit_id}' contains metadata for commit '{}'",
                metadata.commit_id
            ),
        ));
    }
    Ok(Some(metadata))
}

pub(crate) fn stage_projection_metadata(
    writes: &mut StorageWriteSet,
    metadata: &TrackedStateProjectionMetadata,
) -> Result<(), LixError> {
    writes.put(
        TRACKED_STATE_PROJECTION_SPACE,
        key(metadata.commit_id.as_bytes().to_vec()),
        value(encode_projection_metadata(metadata)?),
    );
    Ok(())
}

pub(crate) async fn load_by_file_root(
    store: &(impl StorageRead + ?Sized),
    commit_id: &str,
) -> Result<Option<TrackedStateRootId>, LixError> {
    let Some(bytes) = get_one(
        store,
        TRACKED_STATE_BY_FILE_ROOT_SPACE,
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
        TRACKED_STATE_BY_FILE_ROOT_SPACE,
        key(commit_id.as_bytes().to_vec()),
        value(root_id.as_bytes().to_vec()),
    );
}

pub(crate) async fn read_chunk(
    store: &(impl StorageRead + ?Sized),
    hash: &[u8; TRACKED_STATE_HASH_BYTES],
) -> Result<Option<Vec<u8>>, LixError> {
    get_one(store, TRACKED_STATE_CHUNK_SPACE, hash.to_vec()).await
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
            TRACKED_STATE_CHUNK_SPACE,
            key(chunk.hash.to_vec()),
            value(chunk.data.clone()),
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
        store: &(impl StorageRead + ?Sized),
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

fn key(bytes: Vec<u8>) -> StorageKey {
    StorageKey(Bytes::from(bytes))
}

fn value(bytes: Vec<u8>) -> StorageValue {
    StorageValue {
        bytes: Bytes::from(bytes),
    }
}

fn full_value(value: StorageProjectedValue) -> Option<Vec<u8>> {
    match value {
        StorageProjectedValue::FullValue(bytes) => Some(bytes.to_vec()),
        StorageProjectedValue::KeyOnly => None,
    }
}

fn encode_projection_metadata(
    metadata: &TrackedStateProjectionMetadata,
) -> Result<Vec<u8>, LixError> {
    let mut out = Vec::new();
    out.extend_from_slice(PROJECTION_METADATA_MAGIC);
    write_string(&mut out, &metadata.commit_id)?;
    out.extend_from_slice(metadata.root_id.as_bytes());
    write_u32(
        &mut out,
        metadata.parent_roots.len(),
        "parent projection count",
    )?;
    for parent in &metadata.parent_roots {
        write_string(&mut out, &parent.commit_id)?;
        out.extend_from_slice(parent.root_id.as_bytes());
    }
    out.extend_from_slice(&metadata.changed_key_count.to_le_bytes());
    out.extend_from_slice(&metadata.row_count_estimate.to_le_bytes());
    out.extend_from_slice(&metadata.tree_height.to_le_bytes());
    out.extend_from_slice(&metadata.primary_chunk_count.to_le_bytes());
    out.extend_from_slice(&metadata.primary_chunk_bytes.to_le_bytes());
    Ok(out)
}

fn decode_projection_metadata(bytes: &[u8]) -> Result<TrackedStateProjectionMetadata, LixError> {
    let mut cursor = ProjectionMetadataCursor { bytes, offset: 0 };
    cursor.expect_magic()?;
    let commit_id = cursor.read_string("commit_id")?;
    let root_id = cursor.read_root_id("root_id")?;
    let parent_count = cursor.read_u32("parent projection count")? as usize;
    let mut parent_roots = Vec::with_capacity(parent_count);
    for _ in 0..parent_count {
        parent_roots.push(TrackedStateProjectionParent {
            commit_id: cursor.read_string("parent commit_id")?,
            root_id: cursor.read_root_id("parent root_id")?,
        });
    }
    let changed_key_count = cursor.read_u64("changed_key_count")?;
    let row_count_estimate = cursor.read_u64("row_count_estimate")?;
    let tree_height = cursor.read_u32("tree_height")?;
    let primary_chunk_count = cursor.read_u64("primary_chunk_count")?;
    let primary_chunk_bytes = cursor.read_u64("primary_chunk_bytes")?;
    cursor.expect_end()?;
    Ok(TrackedStateProjectionMetadata {
        commit_id,
        root_id,
        parent_roots,
        changed_key_count,
        row_count_estimate,
        tree_height,
        primary_chunk_count,
        primary_chunk_bytes,
    })
}

fn write_string(out: &mut Vec<u8>, value: &str) -> Result<(), LixError> {
    write_u32(out, value.len(), "string length")?;
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_u32(out: &mut Vec<u8>, value: usize, field: &str) -> Result<(), LixError> {
    let value = u32::try_from(value).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("tracked_state projection metadata {field} exceeds u32"),
        )
    })?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

struct ProjectionMetadataCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl ProjectionMetadataCursor<'_> {
    fn expect_magic(&mut self) -> Result<(), LixError> {
        let magic = self.read_exact(PROJECTION_METADATA_MAGIC.len(), "magic")?;
        if magic != PROJECTION_METADATA_MAGIC {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to decode tracked_state projection metadata: bad magic",
            ));
        }
        Ok(())
    }

    fn expect_end(&self) -> Result<(), LixError> {
        if self.offset != self.bytes.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to decode tracked_state projection metadata: trailing bytes",
            ));
        }
        Ok(())
    }

    fn read_string(&mut self, field: &str) -> Result<String, LixError> {
        let len = self.read_u32(field)? as usize;
        let bytes = self.read_exact(len, field)?;
        String::from_utf8(bytes.to_vec()).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode tracked_state projection metadata {field}: {error}"),
            )
        })
    }

    fn read_root_id(&mut self, field: &str) -> Result<TrackedStateRootId, LixError> {
        let bytes = self.read_exact(TRACKED_STATE_HASH_BYTES, field)?;
        TrackedStateRootId::from_slice(bytes)
    }

    fn read_u32(&mut self, field: &str) -> Result<u32, LixError> {
        let bytes = self.read_exact(std::mem::size_of::<u32>(), field)?;
        Ok(u32::from_le_bytes(bytes.try_into().expect("fixed u32")))
    }

    fn read_u64(&mut self, field: &str) -> Result<u64, LixError> {
        let bytes = self.read_exact(std::mem::size_of::<u64>(), field)?;
        Ok(u64::from_le_bytes(bytes.try_into().expect("fixed u64")))
    }

    fn read_exact(&mut self, len: usize, field: &str) -> Result<&[u8], LixError> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode tracked_state projection metadata {field}: overflow"),
            )
        })?;
        if end > self.bytes.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode tracked_state projection metadata {field}: truncated"),
            ));
        }
        let out = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::binary_cas::kv::{
        BINARY_CAS_CHUNK_SPACE, BINARY_CAS_MANIFEST_CHUNK_SPACE, BINARY_CAS_MANIFEST_SPACE,
    };
    use crate::changelog::{
        BY_CHANGE_INDEX_SPACE, BY_CHANGE_MEMBERSHIP_INDEX_SPACE, BY_COMMIT_INDEX_SPACE,
        COMMIT_VISIBILITY_SPACE, SEGMENT_SPACE, VISIBLE_CHANGE_PROOF_SPACE,
    };
    use crate::json_store::store::JSON_SPACE;
    use crate::untracked_state::storage::UNTRACKED_STATE_ROW_SPACE;

    use super::{
        TRACKED_STATE_BY_FILE_ROOT_SPACE, TRACKED_STATE_CHUNK_SPACE, TRACKED_STATE_PROJECTION_SPACE,
    };

    #[test]
    fn native_storage_space_ids_are_unique_across_owner_layouts() {
        let spaces = [
            UNTRACKED_STATE_ROW_SPACE,
            JSON_SPACE,
            TRACKED_STATE_CHUNK_SPACE,
            TRACKED_STATE_PROJECTION_SPACE,
            TRACKED_STATE_BY_FILE_ROOT_SPACE,
            BINARY_CAS_MANIFEST_SPACE,
            BINARY_CAS_MANIFEST_CHUNK_SPACE,
            BINARY_CAS_CHUNK_SPACE,
            SEGMENT_SPACE,
            COMMIT_VISIBILITY_SPACE,
            BY_COMMIT_INDEX_SPACE,
            BY_CHANGE_INDEX_SPACE,
            BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
            VISIBLE_CHANGE_PROOF_SPACE,
        ];
        let mut seen = BTreeMap::new();
        for space in spaces {
            assert_eq!(
                seen.insert(space.id, space.name),
                None,
                "storage space id {:?} is reused by {} and {}",
                space.id,
                seen.get(&space.id).copied().unwrap_or(space.name),
                space.name
            );
        }
    }

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
