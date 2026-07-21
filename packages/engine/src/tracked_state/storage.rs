#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cmp_owned
)]

use std::collections::HashMap;

use crate::changelog::CommitId;
use crate::storage::{
    CoreProjection, GetManyResult, GetOptions, Key, KeyRange, ProjectedValue, ScanChunk,
    ScanOptions, SpaceId, StorageError,
};
use crate::storage_adapter::{PointReadPlan, StorageAdapterRead, StorageSpace, StorageWriteSet};
use crate::storage_adapter::{
    StorageGetOptions, StorageKey, StorageProjectedValue, StorageSpaceId, StorageValue,
};
use crate::tracked_state::codec::PendingChunkWrite;
use crate::tracked_state::types::{
    TRACKED_STATE_HASH_BYTES, TrackedStateCommitRoot, TrackedStateRootId,
};
use crate::{LixError, storage_codec};
use bytes::Bytes;

pub(crate) const TRACKED_STATE_TREE_CHUNK_NAMESPACE: &str = "tracked_state.tree_chunk";
pub(crate) const TRACKED_STATE_COMMIT_ROOT_NAMESPACE: &str = "tracked_state.commit_root";
pub(crate) const TRACKED_STATE_TREE_CHUNK_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_0001),
    TRACKED_STATE_TREE_CHUNK_NAMESPACE,
);
pub(crate) const TRACKED_STATE_COMMIT_ROOT_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_0004),
    TRACKED_STATE_COMMIT_ROOT_NAMESPACE,
);

// Version the root metadata independently of storage backends. Version 3 is a
// hard cut for derived commit rows, prefix-friendly keys, and compact tree
// nodes. Reject older roots before their differently ordered state can be
// inherited or traversed.
const TRACKED_STATE_COMMIT_ROOT_MAGIC: &[u8] = b"LXTR3";

async fn get_one(
    store: &(impl StorageAdapterRead + ?Sized),
    space: StorageSpace,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, LixError> {
    let result = PointReadPlan::new(space, &[StorageKey(Bytes::from(key))])
        .materialize(store, StorageGetOptions::default())
        .await?;
    Ok(result
        .value
        .into_iter()
        .next()
        .flatten()
        .and_then(full_value))
}

pub(crate) async fn load_root(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: &str,
) -> Result<Option<TrackedStateRootId>, LixError> {
    Ok(load_commit_root(store, commit_id)
        .await?
        .map(|metadata| metadata.root_id))
}

/// Commit-root keys are the raw 16 UUID bytes of the commit id; binary
/// UUIDv7 order matches the former hyphenated-text key order.
fn commit_root_key(commit_id: CommitId) -> Vec<u8> {
    commit_id.as_uuid().as_bytes().to_vec()
}

pub(crate) async fn load_commit_root(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: &str,
) -> Result<Option<TrackedStateCommitRoot>, LixError> {
    // parse_lix canonicalizes test labels to the same synthetic UUID the
    // staging path produces, so label-keyed test fixtures keep matching.
    let typed_commit_id = CommitId::parse_lix(commit_id, "tracked-state commit root lookup")?;
    let Some(bytes) = get_one(
        store,
        TRACKED_STATE_COMMIT_ROOT_SPACE,
        commit_root_key(typed_commit_id),
    )
    .await?
    else {
        return Ok(None);
    };
    let metadata = decode_commit_root(&bytes)?;
    if metadata.commit_id != typed_commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_root key for commit '{commit_id}' contains root metadata for commit '{}'",
                metadata.commit_id
            ),
        ));
    }
    Ok(Some(metadata))
}

pub(crate) fn stage_commit_root(
    writes: &mut StorageWriteSet,
    metadata: &TrackedStateCommitRoot,
) -> Result<(), LixError> {
    writes.put(
        TRACKED_STATE_COMMIT_ROOT_SPACE,
        key(commit_root_key(metadata.commit_id)),
        value(encode_commit_root(metadata)?),
    );
    Ok(())
}

pub(crate) async fn read_chunk(
    store: &(impl StorageAdapterRead + ?Sized),
    hash: &[u8; TRACKED_STATE_HASH_BYTES],
) -> Result<Option<Vec<u8>>, LixError> {
    get_one(store, TRACKED_STATE_TREE_CHUNK_SPACE, hash.to_vec()).await
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

pub(crate) fn debug_verify_chunk_hash(
    expected: &[u8; TRACKED_STATE_HASH_BYTES],
    bytes: &[u8],
) -> Result<(), LixError> {
    if cfg!(debug_assertions) {
        verify_chunk_hash(expected, bytes)?;
    }
    Ok(())
}

pub(crate) fn stage_chunks(writes: &mut StorageWriteSet, chunks: &[PendingChunkWrite]) {
    for chunk in chunks {
        writes.put_content_addressed(
            TRACKED_STATE_TREE_CHUNK_SPACE,
            key(chunk.hash.to_vec()),
            value(chunk.data.clone()),
        );
    }
}

#[derive(Debug, Default)]
pub(crate) struct TrackedStateChunkOverlay {
    chunks: HashMap<[u8; TRACKED_STATE_HASH_BYTES], Vec<u8>>,
}

impl TrackedStateChunkOverlay {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn staged_chunk(&self, hash: &[u8; TRACKED_STATE_HASH_BYTES]) -> Option<&[u8]> {
        self.chunks.get(hash).map(Vec::as_slice)
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

/// Point-read overlay used to audit rebuilt roots before their write set is
/// published. Changelog reads fall through to the coherent base snapshot;
/// commit-root and tree-chunk reads see bytes staged by the root writer first.
#[derive(Debug)]
pub(crate) struct TrackedStateStagedRead<'a, S: ?Sized> {
    store: &'a S,
    commit_roots: HashMap<[u8; 16], Bytes>,
    chunks: &'a TrackedStateChunkOverlay,
}

impl<'a, S> TrackedStateStagedRead<'a, S>
where
    S: StorageAdapterRead + ?Sized,
{
    pub(crate) fn new<'root>(
        store: &'a S,
        commit_roots: impl IntoIterator<Item = &'root TrackedStateCommitRoot>,
        chunks: &'a TrackedStateChunkOverlay,
    ) -> Result<Self, LixError> {
        let mut encoded_roots = HashMap::new();
        for metadata in commit_roots {
            let key = *metadata.commit_id.as_uuid().as_bytes();
            let value = Bytes::from(encode_commit_root(metadata)?);
            if let Some(existing) = encoded_roots.insert(key, value.clone())
                && existing != value
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked-state staged audit contains conflicting commit roots",
                ));
            }
        }
        Ok(Self {
            store,
            commit_roots: encoded_roots,
            chunks,
        })
    }

    fn staged_bytes(&self, space: SpaceId, key: &Key) -> Option<&[u8]> {
        if space == TRACKED_STATE_COMMIT_ROOT_SPACE.id {
            let key = <&[u8; 16]>::try_from(key.0.as_ref()).ok()?;
            return self.commit_roots.get(key).map(AsRef::as_ref);
        }
        if space == TRACKED_STATE_TREE_CHUNK_SPACE.id {
            let key = <&[u8; TRACKED_STATE_HASH_BYTES]>::try_from(key.0.as_ref()).ok()?;
            return self.chunks.staged_chunk(key);
        }
        None
    }
}

impl<S> StorageAdapterRead for TrackedStateStagedRead<'_, S>
where
    S: StorageAdapterRead + ?Sized,
{
    async fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> Result<GetManyResult, StorageError> {
        let mut result = self.store.get_many(space, keys, opts).await?;
        if result.values.len() != keys.len() {
            return Err(StorageError::Corruption(format!(
                "tracked-state staged audit requested {} point reads but storage returned {} slots",
                keys.len(),
                result.values.len()
            )));
        }
        for (key, slot) in keys.iter().zip(&mut result.values) {
            let Some(bytes) = self.staged_bytes(space, key) else {
                continue;
            };
            *slot = Some(match opts.projection {
                CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
                CoreProjection::FullValue => {
                    ProjectedValue::FullValue(Bytes::copy_from_slice(bytes))
                }
            });
        }
        Ok(result)
    }

    async fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        if space == TRACKED_STATE_COMMIT_ROOT_SPACE.id || space == TRACKED_STATE_TREE_CHUNK_SPACE.id
        {
            return Err(StorageError::Io(
                "tracked-state staged audit supports point reads only for overlay spaces"
                    .to_string(),
            ));
        }
        self.store.scan(space, range, opts).await
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

fn encode_commit_root(metadata: &TrackedStateCommitRoot) -> Result<Vec<u8>, LixError> {
    let payload = storage_codec::encode("tracked_state commit_root", metadata)?;
    let mut encoded = Vec::with_capacity(TRACKED_STATE_COMMIT_ROOT_MAGIC.len() + payload.len());
    encoded.extend_from_slice(TRACKED_STATE_COMMIT_ROOT_MAGIC);
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn decode_commit_root(bytes: &[u8]) -> Result<TrackedStateCommitRoot, LixError> {
    let Some(payload) = bytes.strip_prefix(TRACKED_STATE_COMMIT_ROOT_MAGIC) else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked_state commit_root has an unsupported format; recreate the repository",
        ));
    };
    storage_codec::decode("tracked_state commit_root", payload)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::LixError;
    use crate::binary_cas::kv::{
        BINARY_CAS_CHUNK_SPACE, BINARY_CAS_MANIFEST_CHUNK_SPACE, BINARY_CAS_MANIFEST_SPACE,
    };
    use crate::changelog::{CHANGE_SPACE, COMMIT_CHANGE_REF_CHUNK_SPACE, COMMIT_SPACE, CommitId};
    use crate::json_store::store::JSON_SPACE;
    use crate::live_state::LIVE_STATE_INDEX_ROW_SPACE;
    use crate::tracked_state::types::{
        TrackedStateCommitRoot, TrackedStateCommitRootParent, TrackedStateRootId,
    };

    use super::{
        TRACKED_STATE_COMMIT_ROOT_MAGIC, TRACKED_STATE_COMMIT_ROOT_SPACE,
        TRACKED_STATE_TREE_CHUNK_SPACE, decode_commit_root, encode_commit_root,
    };

    #[test]
    fn native_storage_space_ids_are_unique_across_owner_layouts() {
        let spaces = [
            LIVE_STATE_INDEX_ROW_SPACE,
            JSON_SPACE,
            TRACKED_STATE_TREE_CHUNK_SPACE,
            TRACKED_STATE_COMMIT_ROOT_SPACE,
            BINARY_CAS_MANIFEST_SPACE,
            BINARY_CAS_MANIFEST_CHUNK_SPACE,
            BINARY_CAS_CHUNK_SPACE,
            COMMIT_SPACE,
            CHANGE_SPACE,
            COMMIT_CHANGE_REF_CHUNK_SPACE,
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
    fn commit_root_codec_roundtrips_with_parent_metadata() {
        let metadata = TrackedStateCommitRoot {
            commit_id: CommitId::for_test_label("child"),
            root_id: TrackedStateRootId::new([2; 32]),
            parent_roots: vec![TrackedStateCommitRootParent {
                commit_id: CommitId::for_test_label("parent"),
                root_id: TrackedStateRootId::new([1; 32]),
            }],
            changed_key_count: 7,
            row_count_estimate: 42,
            tree_height: 3,
            primary_chunk_count: 5,
            primary_chunk_bytes: 4096,
        };

        let encoded = encode_commit_root(&metadata).expect("commit root should encode");
        assert!(encoded.starts_with(TRACKED_STATE_COMMIT_ROOT_MAGIC));
        let decoded = decode_commit_root(&encoded).expect("commit root should decode");

        assert_eq!(decoded, metadata);
    }

    #[test]
    fn commit_root_codec_rejects_malformed_storage_bytes() {
        let error = decode_commit_root(b"LXTR1not-musli")
            .expect_err("old commit-root versions must fail loudly");

        assert!(
            error
                .to_string()
                .contains("unsupported format; recreate the repository")
        );
    }

    #[test]
    fn commit_root_codec_rejects_pre_v3_roots() {
        let metadata = TrackedStateCommitRoot {
            commit_id: CommitId::for_test_label("legacy"),
            root_id: TrackedStateRootId::new([7; 32]),
            parent_roots: Vec::new(),
            changed_key_count: 1,
            row_count_estimate: 1,
            tree_height: 1,
            primary_chunk_count: 1,
            primary_chunk_bytes: 128,
        };
        let unversioned = crate::storage_codec::encode("tracked_state commit_root", &metadata)
            .expect("pre-v3 commit root should encode");
        let mut v2 = b"LXTR2".to_vec();
        v2.extend_from_slice(&unversioned);

        for old_bytes in [&unversioned, &v2] {
            let error = decode_commit_root(old_bytes)
                .expect_err("pre-v3 roots must not enter the v3 tree layout");

            assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
            assert!(
                error
                    .message
                    .contains("unsupported format; recreate the repository")
            );
        }
    }

    #[test]
    fn commit_root_codec_rejects_trailing_bytes() {
        let metadata = TrackedStateCommitRoot {
            commit_id: CommitId::for_test_label("commit"),
            root_id: TrackedStateRootId::new([9; 32]),
            parent_roots: Vec::new(),
            changed_key_count: 1,
            row_count_estimate: 2,
            tree_height: 1,
            primary_chunk_count: 1,
            primary_chunk_bytes: 128,
        };
        let mut encoded = encode_commit_root(&metadata).expect("commit root should encode");
        encoded.push(0);

        let error = decode_commit_root(&encoded)
            .expect_err("trailing bytes should fail commit root decode");

        assert!(
            error
                .to_string()
                .contains("failed to decode tracked_state commit_root")
        );
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
