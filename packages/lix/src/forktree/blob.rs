use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

use crate::storage::StorageError;
use crate::storage_adapter::StorageAdapterRead;

use super::codec::corruption;
use super::model::{
    BlobChunkRefV1, BlobChunkV1, BlobManifestV1, CanonicalBranchId, CanonicalUploadId,
    UploadPartV1, UploadProgressV1, UploadSelectorV1, upload_binding_digest, upload_selector_key,
};
use super::object::ObjectId;
use super::state::{StateCell, StateKey};
use super::tree::{
    ImmutableObjectSet, ReceiptTreeEdit, ReceiptTreeRoot, empty_receipt_tree, scan_page_on_read,
    validate_receipt_root_on_read,
};
use super::view::{CoherentView, load_object_bytes, load_object_map};

pub(super) const CANONICAL_BLOB_CHUNK_BYTES: usize = 1024 * 1024;
const UPLOAD_PART_BYTES: u64 = CANONICAL_BLOB_CHUNK_BYTES as u64 * 16;
const UPLOAD_PART_WINDOW: u64 = 4;

/// Computes the existing public fixed-chunk BlobId while upload completion is
/// already authenticating payload bytes. Memory is one canonical chunk plus
/// O(chunk-count) hashes; the result is copied into the manifest only as an
/// integrity claim checked against the selected state owner.
#[derive(Default)]
pub(super) struct CanonicalBlobIdBuilder {
    total_size: u64,
    pending: Vec<u8>,
    chunks: Vec<(crate::binary_cas::ChunkHash, u64)>,
}

impl CanonicalBlobIdBuilder {
    pub(super) fn update(&mut self, mut bytes: &[u8]) -> Result<(), StorageError> {
        self.total_size = self
            .total_size
            .checked_add(
                u64::try_from(bytes.len())
                    .map_err(|_| corruption("blob fragment length exceeds u64"))?,
            )
            .ok_or_else(|| corruption("blob length overflows u64"))?;
        while !bytes.is_empty() {
            let remaining = CANONICAL_BLOB_CHUNK_BYTES - self.pending.len();
            let take = remaining.min(bytes.len());
            self.pending.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.pending.len() == CANONICAL_BLOB_CHUNK_BYTES {
                self.chunks.push((
                    crate::binary_cas::ChunkHash::from_content(&self.pending),
                    CANONICAL_BLOB_CHUNK_BYTES as u64,
                ));
                self.pending.clear();
            }
        }
        Ok(())
    }

    /// Adds one already canonical fixed-width chunk without copying it into
    /// the streaming pending buffer. Inline publication owns these boundaries
    /// and rejects a partial non-final chunk on the following update.
    pub(super) fn update_fixed_chunk(&mut self, chunk: &[u8]) -> Result<(), StorageError> {
        if chunk.is_empty()
            || chunk.len() > CANONICAL_BLOB_CHUNK_BYTES
            || !self.pending.is_empty()
            || self
                .chunks
                .last()
                .is_some_and(|(_, size)| *size != CANONICAL_BLOB_CHUNK_BYTES as u64)
        {
            return Err(corruption(
                "inline blob identity update is not canonically chunked",
            ));
        }
        self.total_size = self
            .total_size
            .checked_add(
                u64::try_from(chunk.len())
                    .map_err(|_| corruption("blob fragment length exceeds u64"))?,
            )
            .ok_or_else(|| corruption("blob length overflows u64"))?;
        self.chunks.push((
            crate::binary_cas::ChunkHash::from_content(chunk),
            chunk.len() as u64,
        ));
        Ok(())
    }

    pub(super) fn finish(mut self) -> crate::binary_cas::BlobId {
        if self.total_size <= CANONICAL_BLOB_CHUNK_BYTES as u64 {
            let hash = self
                .chunks
                .first()
                .map(|(hash, _)| *hash)
                .unwrap_or_else(|| crate::binary_cas::ChunkHash::from_content(&self.pending));
            return crate::binary_cas::BlobId::from_single_chunk(hash);
        }
        if !self.pending.is_empty() {
            self.chunks.push((
                crate::binary_cas::ChunkHash::from_content(&self.pending),
                self.pending.len() as u64,
            ));
        }
        crate::binary_cas::BlobId::from_chunks(self.total_size, self.chunks)
    }
}

/// A public blob identity bound to one immutable manifest by an authenticated
/// state-tree row. Fields stay owner-private so sibling subsystems cannot
/// construct an object-space capability or substitute a manifest ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AuthenticatedBlobRef {
    semantic_id: crate::binary_cas::BlobId,
    expected_size: u64,
    manifest_object_id: ObjectId,
    branch_id: CanonicalBranchId,
    view_id: [u8; 32],
    view_instance_id: u64,
}

#[derive(Deserialize)]
struct BlobRefOwnerValue {
    id: String,
    blob_hash: String,
    size_bytes: u64,
}

#[derive(Deserialize)]
struct HistoricalBlobRefOwnerValue {
    id: String,
    blob_hash: String,
    size_bytes: u64,
}

impl AuthenticatedBlobRef {
    pub(crate) fn semantic_id(self) -> crate::binary_cas::BlobId {
        self.semantic_id
    }

    pub(crate) fn expected_size(self) -> u64 {
        self.expected_size
    }

    pub(crate) fn manifest_object_id(self) -> ObjectId {
        self.manifest_object_id
    }
}

/// Blob payload reader backed by the authenticated current ForkTree view.
///
/// Callers provide the exact authenticated state keys selected for each file
/// row. A BlobId is only a semantic field checked after that row's manifest
/// edge has been authenticated; it is never used as a physical lookup key.
pub(crate) struct ForkTreeBlobReader<R> {
    read: R,
    branch_id: CanonicalBranchId,
}

#[async_trait]
pub(crate) trait AuthenticatedBlobReader: Send + Sync {
    async fn load_bytes_for_rows(
        &self,
        rows: &[StateKey],
    ) -> Result<crate::binary_cas::BlobBytesBatch, crate::LixError>;

    async fn load_ranges_for_rows(
        &self,
        requests: &[(StateKey, Range<u64>)],
    ) -> Result<crate::binary_cas::BlobRangeBytesBatch, crate::LixError>;
}

#[async_trait]
impl<R> AuthenticatedBlobReader for ForkTreeBlobReader<R>
where
    R: StorageAdapterRead + Clone + Sync,
{
    async fn load_bytes_for_rows(
        &self,
        rows: &[StateKey],
    ) -> Result<crate::binary_cas::BlobBytesBatch, crate::LixError> {
        self.load_bytes_for_state_keys(rows).await
    }

    async fn load_ranges_for_rows(
        &self,
        requests: &[(StateKey, Range<u64>)],
    ) -> Result<crate::binary_cas::BlobRangeBytesBatch, crate::LixError> {
        self.load_ranges_for_state_keys(requests).await
    }
}

pub(crate) fn blob_reader_on_read<R>(
    read: R,
    branch_id: &str,
) -> Result<ForkTreeBlobReader<R>, crate::LixError> {
    let uuid = uuid::Uuid::parse_str(branch_id).map_err(|error| {
        crate::LixError::new(
            crate::LixError::CODE_INVALID_PARAM,
            format!("branch ID must be a UUID: {error}"),
        )
    })?;
    Ok(ForkTreeBlobReader {
        read,
        branch_id: CanonicalBranchId::from_bytes(*uuid.as_bytes()),
    })
}

impl<R> ForkTreeBlobReader<R>
where
    R: StorageAdapterRead + Clone,
{
    async fn bind_state_keys(
        &self,
        view: &CoherentView<R>,
        keys: &[StateKey],
    ) -> Result<Vec<AuthenticatedBlobRef>, crate::LixError>
    where
        R: Sync,
    {
        let mut refs = Vec::with_capacity(keys.len());
        for key in keys {
            let reference = view
                .bind_blob_at_state_key(key)
                .await?
                .ok_or_else(|| {
                    crate::LixError::new(
                        crate::LixError::CODE_STORAGE_ERROR,
                        "selected filesystem BlobRef owner is absent from the authenticated ForkTree view",
                    )
                })?;
            refs.push(reference);
        }
        Ok(refs)
    }

    pub(crate) async fn load_bytes_for_state_keys(
        &self,
        keys: &[StateKey],
    ) -> Result<crate::binary_cas::BlobBytesBatch, crate::LixError>
    where
        R: Sync,
    {
        if keys.is_empty() {
            return Ok(crate::binary_cas::BlobBytesBatch::new(Vec::new()));
        }
        let view =
            super::view::open_coherent_view_on_read(self.read.clone(), self.branch_id).await?;
        let refs = self.bind_state_keys(&view, keys).await?;
        view.load_blob_bytes_many(&refs).await
    }

    pub(crate) async fn load_ranges_for_state_keys(
        &self,
        requests: &[(StateKey, Range<u64>)],
    ) -> Result<crate::binary_cas::BlobRangeBytesBatch, crate::LixError>
    where
        R: Sync,
    {
        if requests.is_empty() {
            return Ok(crate::binary_cas::BlobRangeBytesBatch::new(Vec::new()));
        }
        let view =
            super::view::open_coherent_view_on_read(self.read.clone(), self.branch_id).await?;
        let keys = requests
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let refs = self.bind_state_keys(&view, &keys).await?;
        view.load_blob_ranges_many(
            &refs
                .into_iter()
                .zip(requests.iter().map(|(_, range)| range.clone()))
                .collect::<Vec<_>>(),
        )
        .await
    }
}

fn bind_state_blob_ref(
    row: &super::serving::VisibleStateRow,
    expected_key: Option<&StateKey>,
    branch_id: CanonicalBranchId,
    view_id: [u8; 32],
    view_instance_id: u64,
) -> Result<AuthenticatedBlobRef, crate::LixError> {
    let key = super::state::decode_state_key(&row.encoded_key)?;
    if let Some(expected_key) = expected_key {
        if &key != expected_key {
            return Err(corruption(
                "authenticated blob-reference row key does not match requested StateKey",
            )
            .into());
        }
    }
    if key.schema_key != "lix_binary_blob_ref" {
        return Err(corruption("authenticated state row is not a blob-reference owner").into());
    }
    let file_id = key
        .file_id
        .as_deref()
        .ok_or_else(|| corruption("blob-reference owner has no file identity"))?;
    let expected_entity_pk = crate::entity_pk::EntityPk::uuid_from_canonical(file_id)
        .map_err(|_| corruption("blob-reference owner file identity is not a canonical UUID"))?;
    if key.entity_pk != expected_entity_pk {
        return Err(corruption("blob-reference key identity is inconsistent").into());
    }
    let value = match &row.value.cell {
        StateCell::Value(value) => value,
        StateCell::Null | StateCell::Tombstone => {
            return Err(corruption("blob-reference owner has no live semantic value").into());
        }
    };
    let owner: BlobRefOwnerValue = serde_json::from_str(value).map_err(|error| {
        corruption(format!(
            "blob-reference owner semantic value is malformed: {error}"
        ))
    })?;
    if owner.id != file_id {
        return Err(
            corruption("blob-reference payload identity does not match its StateKey").into(),
        );
    }
    let semantic_id = crate::binary_cas::BlobId::from_hex(&owner.blob_hash)?;
    if row.value.blob_manifest_object_ids.len() != 1 {
        return Err(
            corruption("blob-reference owner must contain exactly one manifest edge").into(),
        );
    }
    let manifest_object_id = row
        .value
        .blob_manifest_object_ids
        .first()
        .copied()
        .ok_or_else(|| corruption("blob-reference owner manifest edge is absent"))?;
    Ok(AuthenticatedBlobRef {
        semantic_id,
        expected_size: owner.size_bytes,
        manifest_object_id,
        branch_id,
        view_id,
        view_instance_id,
    })
}

impl<R> CoherentView<R>
where
    R: StorageAdapterRead,
{
    /// Binds a public blob identity to an immutable manifest edge carried by
    /// a row authenticated on this exact coherent view.
    #[cfg(test)]
    pub(crate) fn bind_blob(
        &self,
        row: &super::serving::VisibleStateRow,
    ) -> Result<AuthenticatedBlobRef, crate::LixError> {
        if row.view_instance_id != self.view_instance_id() {
            return Err(StorageError::InvalidCursor.into());
        }
        bind_state_blob_ref(
            row,
            None,
            self.branch_id(),
            self.view_id(),
            self.view_instance_id(),
        )
    }

    /// Re-resolves a filesystem blob owner through this view's authenticated
    /// state roots before exposing its manifest capability. The materialized
    /// row is only a terminal index projection; it cannot authorize payload
    /// reads by itself.
    pub(crate) async fn bind_blob_at_state_key(
        &self,
        key: &StateKey,
    ) -> Result<Option<AuthenticatedBlobRef>, crate::LixError>
    where
        R: Sync,
    {
        let encoded_key = super::state::encode_state_key(super::state::StateKeyRef {
            schema_key: &key.schema_key,
            file_id: key.file_id.as_deref(),
            entity_pk: &key.entity_pk,
        });
        let row = super::serving::state_point(self, &encoded_key, false)
            .await
            .map_err(crate::LixError::from)?;
        row.map(|row| {
            bind_state_blob_ref(
                &row,
                Some(key),
                self.branch_id(),
                self.view_id(),
                self.view_instance_id(),
            )
        })
        .transpose()
    }

    /// Loads complete payloads without allowing the authenticated row edge to
    /// be detached from the StorageRead that selected it.
    pub(crate) async fn load_blob_bytes_many(
        &self,
        refs: &[AuthenticatedBlobRef],
    ) -> Result<crate::binary_cas::BlobBytesBatch, crate::LixError>
    where
        R: Sync,
    {
        self.load_blob_bytes_many_on_view(refs).await
    }

    /// Loads payload ranges on the same StorageRead that authenticated their
    /// state rows and manifest edges.
    pub(crate) async fn load_blob_ranges_many(
        &self,
        requests: &[(AuthenticatedBlobRef, Range<u64>)],
    ) -> Result<crate::binary_cas::BlobRangeBytesBatch, crate::LixError>
    where
        R: Sync,
    {
        self.load_blob_ranges_many_on_view(requests).await
    }
}

/// Loads complete payloads through state-authenticated manifest edges. One
/// object batch authenticates all manifests and one batch authenticates all
/// distinct chunks, so adapter calls scale with object levels rather than rows.
pub(super) async fn load_blob_bytes_many_on_read<R>(
    read: &R,
    branch_id: CanonicalBranchId,
    view_id: [u8; 32],
    view_instance_id: u64,
    refs: &[AuthenticatedBlobRef],
) -> Result<crate::binary_cas::BlobBytesBatch, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    validate_blob_ref_views(branch_id, view_id, view_instance_id, refs.iter())?;
    let manifests = load_manifests(read, refs).await?;
    for reference in refs {
        validate_manifest_owner(
            required_manifest(&manifests, reference.manifest_object_id)?,
            reference,
        )?;
    }
    let mut required_ids = BTreeSet::new();
    for manifest in manifests.values() {
        for chunk in &manifest.ordered_chunks {
            required_ids.insert(chunk.chunk_object_id);
        }
    }
    let chunks = load_required_chunks(read, required_ids).await?;
    let mut entries = Vec::with_capacity(refs.len());
    for reference in refs {
        let manifest = required_manifest(&manifests, reference.manifest_object_id)?;
        let capacity = usize::try_from(manifest.logical_bytes)
            .map_err(|_| corruption("blob payload length cannot be represented by this process"))?;
        let mut bytes = Vec::with_capacity(capacity);
        let mut digest = blake3::Hasher::new();
        for chunk_ref in &manifest.ordered_chunks {
            let chunk = required_chunk(&chunks, chunk_ref.chunk_object_id)?;
            validate_chunk_len(chunk, chunk_ref)?;
            digest.update(&chunk.bytes);
            bytes.extend_from_slice(&chunk.bytes);
        }
        if bytes.len() != capacity || digest.finalize().as_bytes() != &manifest.content_digest {
            return Err(corruption("blob manifest payload length or digest is invalid").into());
        }
        if crate::binary_cas::BlobId::from_content(&bytes) != reference.semantic_id {
            return Err(corruption(
                "authenticated state blob identity does not match its manifest payload",
            )
            .into());
        }
        entries.push(Some(bytes));
    }
    Ok(crate::binary_cas::BlobBytesBatch::new(entries))
}

/// Authenticates a complete fixed-chunk blob without materializing its
/// payload. The manifest and each child chunk are read through the same
/// retained StorageRead; every child is decoded and checked against its
/// domain/object identity, declared length, manifest digest, and canonical
/// BlobId before its bytes are dropped. Only unchanged prefix/suffix slices
/// are compared with the requested successor payload.
pub(super) async fn authenticate_blob_for_splice_on_read<R>(
    read: &R,
    branch_id: CanonicalBranchId,
    view_id: [u8; 32],
    view_instance_id: u64,
    reference: &AuthenticatedBlobRef,
    successor_bytes: &[u8],
    prefix_len: usize,
    replacement_len: usize,
    suffix_len: usize,
) -> Result<[u8; 32], crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    validate_blob_ref_views(
        branch_id,
        view_id,
        view_instance_id,
        [reference].into_iter(),
    )?;
    let manifests = load_manifests(read, std::slice::from_ref(reference)).await?;
    let manifest = required_manifest(&manifests, reference.manifest_object_id)?;
    validate_manifest_owner(manifest, reference)?;
    let expected_len = usize::try_from(manifest.logical_bytes)
        .map_err(|_| corruption("blob payload length cannot be represented by this process"))?;
    if successor_bytes.len() != expected_len {
        return Err(corruption("verified blob splice successor length is invalid").into());
    }
    let replace_end = prefix_len
        .checked_add(replacement_len)
        .ok_or_else(|| corruption("verified blob splice replacement range overflows"))?;
    if replacement_len == 0
        || prefix_len > expected_len
        || replace_end > expected_len
        || suffix_len != expected_len - replace_end
    {
        return Err(corruption("verified blob splice comparison range is invalid").into());
    }
    let expected_chunk_count = expected_len.div_ceil(CANONICAL_BLOB_CHUNK_BYTES);
    if manifest.ordered_chunks.len() != expected_chunk_count {
        return Err(corruption(
            "verified blob splice base manifest is not canonical fixed-chunk layout",
        )
        .into());
    }

    let mut sha256 = Sha256::new();
    let mut content_digest = blake3::Hasher::new();
    let mut semantic_id = CanonicalBlobIdBuilder::default();
    let mut offset = 0usize;
    for (index, chunk_ref) in manifest.ordered_chunks.iter().enumerate() {
        let chunk_start = index
            .checked_mul(CANONICAL_BLOB_CHUNK_BYTES)
            .ok_or_else(|| corruption("verified blob splice chunk offset overflows"))?;
        if chunk_start != offset {
            return Err(
                corruption("verified blob splice chunk ordinals are not contiguous").into(),
            );
        }
        let declared_len = usize::try_from(chunk_ref.declared_len)
            .map_err(|_| corruption("verified blob splice chunk length overflows"))?;
        let chunk_end = chunk_start
            .checked_add(declared_len)
            .ok_or_else(|| corruption("verified blob splice chunk end overflows"))?;
        let expected_chunk_len = chunk_end.min(expected_len) - chunk_start;
        if chunk_ref.declared_len != expected_chunk_len as u64
            || (index + 1 < expected_chunk_count
                && expected_chunk_len != CANONICAL_BLOB_CHUNK_BYTES)
        {
            return Err(corruption(
                "verified blob splice base manifest has a noncanonical chunk length",
            )
            .into());
        }
        let chunk_bytes = load_object_bytes(read, chunk_ref.chunk_object_id).await?;
        let chunk = BlobChunkV1::decode(chunk_ref.chunk_object_id, &chunk_bytes)?;
        if chunk.bytes.len() as u64 != chunk_ref.declared_len {
            return Err(corruption(
                "verified blob splice child bytes do not match declared length",
            )
            .into());
        }
        compare_successor_slice(&chunk.bytes, chunk_start, successor_bytes, 0, prefix_len)?;
        compare_successor_slice(
            &chunk.bytes,
            chunk_start,
            successor_bytes,
            replace_end,
            expected_len,
        )?;
        sha256.update(&chunk.bytes);
        content_digest.update(&chunk.bytes);
        semantic_id.update(&chunk.bytes)?;
        offset = chunk_end;
    }
    if offset != expected_len {
        return Err(corruption("verified blob splice manifest length is invalid").into());
    }
    if content_digest.finalize().as_bytes() != &manifest.content_digest {
        return Err(corruption("blob manifest payload digest is invalid").into());
    }
    if semantic_id.finish() != reference.semantic_id {
        return Err(corruption(
            "authenticated state blob identity does not match its manifest payload",
        )
        .into());
    }
    Ok(sha256.finalize().into())
}

fn compare_successor_slice(
    chunk: &[u8],
    chunk_start: usize,
    successor: &[u8],
    range_start: usize,
    range_end: usize,
) -> Result<(), crate::LixError> {
    let chunk_end = chunk_start
        .checked_add(chunk.len())
        .ok_or_else(|| corruption("verified blob splice chunk range overflows"))?;
    let start = chunk_start.max(range_start);
    let end = chunk_end.min(range_end);
    if start < end {
        let chunk_start_index = start - chunk_start;
        let chunk_end_index = end - chunk_start;
        if chunk[chunk_start_index..chunk_end_index] != successor[start..end] {
            return Err(corruption(
                "verified blob splice unchanged bytes do not match authenticated base",
            )
            .into());
        }
    }
    Ok(())
}

/// Loads historical file payloads from exact ForkTree state keys. The state
/// value and its manifest edge are authenticated by the same retained read;
/// the BlobId is only an integrity claim and is never a physical lookup key.
pub(crate) async fn load_historical_blob_bytes_for_state_values<R>(
    read: &R,
    values: &[(StateKey, super::state::StateValue)],
) -> Result<crate::binary_cas::BlobBytesBatch, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let refs = values
        .iter()
        .map(|(key, value)| bind_historical_state_blob_ref(key, value))
        .collect::<Result<Vec<_>, _>>()?;
    let manifests = load_historical_manifests(read, &refs).await?;
    for reference in &refs {
        let manifest = required_manifest_by_id(&manifests, reference.manifest_object_id)?;
        validate_manifest_fields(manifest, reference.expected_size, reference.semantic_id)?;
    }
    let required_ids = manifests
        .values()
        .flat_map(|manifest| {
            manifest
                .ordered_chunks
                .iter()
                .map(|chunk| chunk.chunk_object_id)
        })
        .collect::<BTreeSet<_>>();
    let chunks = load_required_chunks(read, required_ids).await?;
    let mut entries = Vec::with_capacity(refs.len());
    for reference in refs {
        let manifest = required_manifest_by_id(&manifests, reference.manifest_object_id)?;
        let capacity = usize::try_from(manifest.logical_bytes)
            .map_err(|_| corruption("blob payload length cannot be represented by this process"))?;
        let mut bytes = Vec::with_capacity(capacity);
        let mut digest = blake3::Hasher::new();
        for chunk_ref in &manifest.ordered_chunks {
            let chunk = required_chunk(&chunks, chunk_ref.chunk_object_id)?;
            validate_chunk_len(chunk, chunk_ref)?;
            digest.update(&chunk.bytes);
            bytes.extend_from_slice(&chunk.bytes);
        }
        if bytes.len() != capacity || digest.finalize().as_bytes() != &manifest.content_digest {
            return Err(corruption("blob manifest payload length or digest is invalid").into());
        }
        if crate::binary_cas::BlobId::from_content(&bytes) != reference.semantic_id {
            return Err(corruption(
                "historical state blob identity does not match its manifest payload",
            )
            .into());
        }
        entries.push(Some(bytes));
    }
    Ok(crate::binary_cas::BlobBytesBatch::new(entries))
}

#[derive(Clone, Copy)]
struct HistoricalAuthenticatedBlobRef {
    semantic_id: crate::binary_cas::BlobId,
    expected_size: u64,
    manifest_object_id: ObjectId,
}

fn bind_historical_state_blob_ref(
    key: &StateKey,
    value: &super::state::StateValue,
) -> Result<HistoricalAuthenticatedBlobRef, crate::LixError> {
    if key.schema_key != "lix_binary_blob_ref" {
        return Err(corruption("historical state row is not a blob-reference owner").into());
    }
    let file_id = key
        .file_id
        .as_deref()
        .ok_or_else(|| corruption("historical blob-reference owner has no file identity"))?;
    let expected_entity_pk =
        crate::entity_pk::EntityPk::uuid_from_canonical(file_id).map_err(|_| {
            corruption("historical blob-reference owner file identity is not a canonical UUID")
        })?;
    if key.entity_pk != expected_entity_pk {
        return Err(corruption("historical blob-reference key identity is inconsistent").into());
    }
    let snapshot = match &value.cell {
        StateCell::Value(snapshot) => snapshot,
        StateCell::Null | StateCell::Tombstone => {
            return Err(corruption("historical blob-reference owner is not live").into());
        }
    };
    let owner: HistoricalBlobRefOwnerValue = serde_json::from_str(snapshot).map_err(|error| {
        corruption(format!(
            "historical blob-reference owner semantic value is malformed: {error}"
        ))
    })?;
    if owner.id != file_id {
        return Err(corruption(
            "historical blob-reference payload identity does not match its state key",
        )
        .into());
    }
    if value.blob_manifest_object_ids.len() != 1 {
        return Err(corruption(
            "historical blob-reference owner must contain exactly one manifest edge",
        )
        .into());
    }
    let manifest_object_id = value
        .blob_manifest_object_ids
        .first()
        .copied()
        .ok_or_else(|| corruption("historical blob-reference manifest edge is absent"))?;
    Ok(HistoricalAuthenticatedBlobRef {
        semantic_id: crate::binary_cas::BlobId::from_hex(&owner.blob_hash)?,
        expected_size: owner.size_bytes,
        manifest_object_id,
    })
}

/// Loads only chunks intersecting each requested range while preserving the
/// same authenticated state/manifest ownership as full reads.
pub(super) async fn load_blob_ranges_many_on_read<R>(
    read: &R,
    branch_id: CanonicalBranchId,
    view_id: [u8; 32],
    view_instance_id: u64,
    requests: &[(AuthenticatedBlobRef, Range<u64>)],
) -> Result<crate::binary_cas::BlobRangeBytesBatch, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    validate_blob_ref_views(
        branch_id,
        view_id,
        view_instance_id,
        requests.iter().map(|(reference, _)| reference),
    )?;
    let refs = requests
        .iter()
        .map(|(reference, _)| *reference)
        .collect::<Vec<_>>();
    let manifests = load_manifests(read, &refs).await?;
    let mut required_ids = BTreeSet::new();
    for (reference, requested) in requests {
        let manifest = required_manifest(&manifests, reference.manifest_object_id)?;
        validate_manifest_owner(manifest, reference)?;
        let range = validated_range(requested.clone(), manifest.logical_bytes)?;
        let mut offset = 0_u64;
        for chunk in &manifest.ordered_chunks {
            let end = offset
                .checked_add(chunk.declared_len)
                .ok_or_else(|| corruption("blob chunk offsets overflow u64"))?;
            if offset < range.end && end > range.start {
                required_ids.insert(chunk.chunk_object_id);
            }
            offset = end;
        }
    }
    let chunks = load_required_chunks(read, required_ids).await?;
    let mut entries = Vec::with_capacity(requests.len());
    for (reference, requested) in requests {
        let manifest = required_manifest(&manifests, reference.manifest_object_id)?;
        validate_manifest_owner(manifest, reference)?;
        let range = validated_range(requested.clone(), manifest.logical_bytes)?;
        let expected_len = usize::try_from(range.end - range.start)
            .map_err(|_| corruption("blob range length cannot be represented by this process"))?;
        let mut bytes = Vec::with_capacity(expected_len);
        let mut offset = 0_u64;
        for chunk_ref in &manifest.ordered_chunks {
            let end = offset
                .checked_add(chunk_ref.declared_len)
                .ok_or_else(|| corruption("blob chunk offsets overflow u64"))?;
            if offset < range.end && end > range.start {
                let chunk = required_chunk(&chunks, chunk_ref.chunk_object_id)?;
                validate_chunk_len(chunk, chunk_ref)?;
                let start_in_chunk = usize::try_from(range.start.saturating_sub(offset))
                    .map_err(|_| corruption("blob range start exceeds usize"))?;
                let end_in_chunk = usize::try_from(range.end.min(end) - offset)
                    .map_err(|_| corruption("blob range end exceeds usize"))?;
                bytes.extend_from_slice(&chunk.bytes[start_in_chunk..end_in_chunk]);
            }
            offset = end;
        }
        if bytes.len() != expected_len {
            return Err(corruption("blob range materialization length is invalid").into());
        }
        entries.push(Some(crate::binary_cas::BlobRangeBytes {
            bytes,
            total_size: manifest.logical_bytes,
            range,
        }));
    }
    Ok(crate::binary_cas::BlobRangeBytesBatch::new(entries))
}

fn validate_blob_ref_views<'a>(
    branch_id: CanonicalBranchId,
    view_id: [u8; 32],
    view_instance_id: u64,
    refs: impl IntoIterator<Item = &'a AuthenticatedBlobRef>,
) -> Result<(), crate::LixError> {
    if refs.into_iter().any(|reference| {
        reference.branch_id != branch_id
            || reference.view_id != view_id
            || reference.view_instance_id != view_instance_id
    }) {
        return Err(StorageError::InvalidCursor.into());
    }
    Ok(())
}

async fn load_manifests<R>(
    read: &R,
    refs: &[AuthenticatedBlobRef],
) -> Result<BTreeMap<ObjectId, BlobManifestV1>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    load_manifests_for_ids(
        read,
        refs.iter().map(|reference| reference.manifest_object_id),
    )
    .await
}

async fn load_historical_manifests<R>(
    read: &R,
    refs: &[HistoricalAuthenticatedBlobRef],
) -> Result<BTreeMap<ObjectId, BlobManifestV1>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    load_manifests_for_ids(
        read,
        refs.iter().map(|reference| reference.manifest_object_id),
    )
    .await
}

async fn load_manifests_for_ids<R>(
    read: &R,
    ids: impl IntoIterator<Item = ObjectId>,
) -> Result<BTreeMap<ObjectId, BlobManifestV1>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let ids = ids.into_iter().collect::<BTreeSet<_>>();
    let objects = load_object_map(read, ids.iter().copied()).await?;
    ids.into_iter()
        .map(|id| {
            let bytes = objects
                .get(&id)
                .ok_or_else(|| corruption(format!("blob manifest {id} is absent")))?;
            BlobManifestV1::decode(id, bytes).map(|manifest| (id, manifest))
        })
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

async fn load_required_chunks<R>(
    read: &R,
    ids: impl IntoIterator<Item = ObjectId>,
) -> Result<BTreeMap<ObjectId, BlobChunkV1>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let ids = ids.into_iter().collect::<BTreeSet<_>>();
    let objects = load_object_map(read, ids.iter().copied()).await?;
    ids.into_iter()
        .map(|id| {
            let bytes = objects
                .get(&id)
                .ok_or_else(|| corruption(format!("blob chunk {id} is absent")))?;
            BlobChunkV1::decode(id, bytes).map(|chunk| (id, chunk))
        })
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

fn required_manifest(
    manifests: &BTreeMap<ObjectId, BlobManifestV1>,
    id: ObjectId,
) -> Result<&BlobManifestV1, crate::LixError> {
    required_manifest_by_id(manifests, id)
}

fn required_manifest_by_id(
    manifests: &BTreeMap<ObjectId, BlobManifestV1>,
    id: ObjectId,
) -> Result<&BlobManifestV1, crate::LixError> {
    manifests
        .get(&id)
        .ok_or_else(|| corruption(format!("blob manifest {id} is absent")).into())
}

fn validate_manifest_owner(
    manifest: &BlobManifestV1,
    reference: &AuthenticatedBlobRef,
) -> Result<(), crate::LixError> {
    validate_manifest_fields(manifest, reference.expected_size, reference.semantic_id)
}

fn validate_manifest_fields(
    manifest: &BlobManifestV1,
    expected_size: u64,
    semantic_id: crate::binary_cas::BlobId,
) -> Result<(), crate::LixError> {
    if manifest.logical_bytes != expected_size {
        return Err(corruption(
            "blob-reference owner size does not match its authenticated manifest",
        )
        .into());
    }
    if manifest.canonical_blob_id != semantic_id {
        return Err(corruption(
            "blob-reference owner identity does not match its authenticated manifest",
        )
        .into());
    }
    Ok(())
}

fn required_chunk(
    chunks: &BTreeMap<ObjectId, BlobChunkV1>,
    id: ObjectId,
) -> Result<&BlobChunkV1, crate::LixError> {
    chunks
        .get(&id)
        .ok_or_else(|| corruption(format!("blob chunk {id} is absent")).into())
}

fn validate_chunk_len(
    chunk: &BlobChunkV1,
    reference: &BlobChunkRefV1,
) -> Result<(), crate::LixError> {
    if chunk.bytes.len() as u64 != reference.declared_len {
        return Err(corruption("blob chunk bytes do not match their declared length").into());
    }
    Ok(())
}

fn validated_range(requested: Range<u64>, total: u64) -> Result<Range<u64>, crate::LixError> {
    if requested.start >= requested.end || requested.start >= total {
        return Err(crate::LixError::new(
            crate::LixError::CODE_INVALID_PARAM,
            "binary blob range is not satisfiable",
        ));
    }
    Ok(requested.start..requested.end.min(total))
}

/// Binding material supplied by the same public upload request that created
/// the receipt. Completion recomputes this digest instead of trusting a second
/// persisted description of repository/path/size/hash identity.
#[derive(Clone, Copy, Debug)]
pub(crate) struct UploadBindingRef<'a> {
    pub(crate) repository_identity: &'a [u8],
    pub(crate) path: &'a [u8],
    pub(crate) payload_domain: &'a [u8],
    pub(crate) declared_total_size: u64,
    pub(crate) declared_final_hash: Option<[u8; 32]>,
}

/// An authenticated receipt-to-manifest proof. Fields are visible only to the
/// publication owner, preventing callers from fabricating a completion edge.
#[derive(Debug)]
pub(crate) struct CompletedUpload {
    pub(super) view_id: [u8; 32],
    pub(super) raw_upload_selector: Bytes,
    pub(super) selector: UploadSelectorV1,
    pub(super) manifest: BlobManifestV1,
}

/// One part publication prepared from one coherent upload view. The caller
/// either persists the typed receipt selector or, when the receipt is complete,
/// hands `complete_receipt` to the ordinary file-row transaction lowering.
#[derive(Clone, Debug)]
pub(crate) struct PreparedUploadPart {
    pub(crate) part: UploadPartV1,
    pub(crate) chunks: Vec<BlobChunkV1>,
    pub(crate) receipt: ReceiptTreeEdit,
    pub(crate) progress: UploadProgressV1,
    pub(crate) selector: UploadSelectorV1,
    pub(crate) raw_selector: Option<Bytes>,
    pub(crate) complete_manifest: Option<BlobManifestV1>,
    pub(crate) complete_receipt: Option<crate::binary_cas::BlobWriteReceipt>,
    pub(crate) already_present: bool,
}

/// Prepares one aligned upload part without opening a second read. Existing
/// receipt objects are authenticated from `view`; new chunks, the part, and
/// path-copied receipt nodes remain operation-local until the publication is
/// merged into the transaction's sole storage plan.
pub(crate) async fn prepare_upload_part<R>(
    view: &CoherentView<R>,
    upload_id: CanonicalUploadId,
    binding: UploadBindingRef<'_>,
    part_number: u64,
    byte_offset: u64,
    content: &[u8],
) -> Result<PreparedUploadPart, StorageError>
where
    R: StorageAdapterRead,
{
    let expected_binding = upload_binding_digest(
        binding.repository_identity,
        binding.path,
        binding.payload_domain,
        binding.declared_total_size,
        binding.declared_final_hash,
    )?;
    let selector_key = upload_selector_key(&upload_id)?;
    let raw_selector = view.load_selector_value(&selector_key).await?;

    let (root, prior_progress, selector_generation) = if let Some(raw) = &raw_selector {
        let selector = UploadSelectorV1::decode(raw)?;
        if selector.upload_id != upload_id || selector.binding_digest != expected_binding {
            return Err(corruption(
                "upload selector binding does not match this request",
            ));
        }
        let progress_bytes = view.load_object_bytes(selector.progress_object_id).await?;
        let progress = UploadProgressV1::decode(selector.progress_object_id, &progress_bytes)?;
        if progress.upload_id != upload_id || progress.binding_digest != expected_binding {
            return Err(corruption(
                "upload progress binding does not match its selector",
            ));
        }
        view.validate_receipt_root(ReceiptTreeRoot {
            object_id: progress.receipt_tree_root,
            completed_part_count: progress.completed_part_count,
            received_bytes: progress.received_bytes,
            contiguous_prefix_bytes: progress.contiguous_prefix_bytes,
        })
        .await?;
        (
            ReceiptTreeRoot {
                object_id: progress.receipt_tree_root,
                completed_part_count: progress.completed_part_count,
                received_bytes: progress.received_bytes,
                contiguous_prefix_bytes: progress.contiguous_prefix_bytes,
            },
            Some(progress),
            selector.selector_generation,
        )
    } else {
        let empty = empty_receipt_tree()?;
        (empty.root, None, 0)
    };

    let mut chunks = Vec::with_capacity(content.len().div_ceil(CANONICAL_BLOB_CHUNK_BYTES));
    let mut ordered_chunks = Vec::with_capacity(chunks.capacity());
    let mut part_hasher = blake3::Hasher::new();
    for fragment in content.chunks(CANONICAL_BLOB_CHUNK_BYTES) {
        let chunk = BlobChunkV1 {
            bytes: Bytes::copy_from_slice(fragment),
        };
        let (chunk_object_id, _) = chunk.encode()?;
        part_hasher.update(fragment);
        ordered_chunks.push(BlobChunkRefV1 {
            chunk_object_id,
            declared_len: fragment.len() as u64,
        });
        chunks.push(chunk);
    }
    let part = UploadPartV1 {
        upload_id: upload_id.clone(),
        part_number,
        byte_offset,
        declared_part_len: content.len() as u64,
        ordered_chunks,
        part_digest: *part_hasher.finalize().as_bytes(),
    };
    let (part_object_id, _) = part.encode()?;

    let (already_present, receipt, progress) = if let Some(ref prior) = prior_progress {
        let existing = view
            .lookup_tree_value(
                prior.receipt_tree_root,
                "receipt",
                &part_number.to_be_bytes(),
            )
            .await?;
        if let Some(existing) = existing {
            let existing_id = ObjectId::from_bytes(
                existing
                    .as_slice()
                    .try_into()
                    .map_err(|_| corruption("receipt part edge is not an object ID"))?,
            );
            let existing_bytes = view.load_object_bytes(existing_id).await?;
            let existing_part = UploadPartV1::decode(existing_id, &existing_bytes)?;
            if existing_part != part {
                return Err(corruption("upload part was replayed with different bytes"));
            }
            (
                true,
                ReceiptTreeEdit {
                    root,
                    objects: ImmutableObjectSet::default(),
                    copied_nodes: 0,
                    inserted: false,
                },
                prior.clone(),
            )
        } else {
            let edit = view
                .insert_receipt_part(root, part_object_id, &part, &ImmutableObjectSet::default())
                .await?;
            let progress = UploadProgressV1 {
                upload_id: upload_id.clone(),
                binding_digest: expected_binding,
                receipt_tree_root: edit.root.object_id,
                completed_part_count: prior
                    .completed_part_count
                    .checked_add(1)
                    .ok_or_else(|| corruption("upload part count overflows u64"))?,
                received_bytes: prior
                    .received_bytes
                    .checked_add(content.len() as u64)
                    .ok_or_else(|| corruption("upload byte count overflows u64"))?,
                contiguous_prefix_bytes: edit.root.contiguous_prefix_bytes,
            };
            (false, edit, progress)
        }
    } else {
        let empty = empty_receipt_tree()?;
        let mut overlay = empty.objects.clone();
        let edit = view
            .insert_receipt_part(empty.root, part_object_id, &part, &overlay)
            .await?;
        let progress = UploadProgressV1 {
            upload_id: upload_id.clone(),
            binding_digest: expected_binding,
            receipt_tree_root: edit.root.object_id,
            completed_part_count: 1,
            received_bytes: content.len() as u64,
            contiguous_prefix_bytes: edit.root.contiguous_prefix_bytes,
        };
        overlay.extend(edit.objects.clone())?;
        (false, edit, progress)
    };

    let next_part = prior_progress.as_ref().map_or(0, |progress| {
        progress.contiguous_prefix_bytes / UPLOAD_PART_BYTES
    });
    if part_number >= next_part.saturating_add(UPLOAD_PART_WINDOW) {
        return Err(corruption(
            "upload part is outside the four-part completion window",
        ));
    }

    let selector = UploadSelectorV1 {
        upload_id: upload_id.clone(),
        binding_digest: expected_binding,
        progress_object_id: progress.encode()?.0,
        selector_generation: selector_generation
            .checked_add(1)
            .ok_or_else(|| corruption("upload selector generation overflows u64"))?,
    };
    let complete_manifest = if progress.received_bytes == binding.declared_total_size
        && progress.contiguous_prefix_bytes == binding.declared_total_size
    {
        Some(
            build_completed_manifest(
                view,
                raw_selector.is_some().then_some(root),
                &part,
                &chunks,
                binding.declared_total_size,
            )
            .await?,
        )
    } else {
        None
    };
    let complete_receipt = if let Some(manifest) = &complete_manifest {
        let (manifest_id, _) = manifest.encode()?;
        Some(crate::binary_cas::BlobWriteReceipt {
            hash: manifest.canonical_blob_id,
            size_bytes: manifest.logical_bytes,
            layout: if manifest.logical_bytes == 0 {
                crate::binary_cas::BlobLayout::Empty
            } else if manifest.logical_bytes <= CANONICAL_BLOB_CHUNK_BYTES as u64 {
                crate::binary_cas::BlobLayout::SingleChunk {
                    chunk_hash: chunks
                        .first()
                        .map(|chunk| crate::binary_cas::ChunkHash::from_content(&chunk.bytes))
                        .ok_or_else(|| corruption("completed single-chunk upload has no chunk"))?,
                }
            } else {
                crate::binary_cas::BlobLayout::Chunked {
                    chunk_count: u32::try_from(manifest.ordered_chunks.len())
                        .map_err(|_| corruption("upload manifest has too many chunks"))?,
                }
            },
            manifest_object_id: *manifest_id.as_bytes(),
            manifest_was_existing: false,
        })
    } else {
        None
    };
    Ok(PreparedUploadPart {
        part,
        chunks,
        receipt,
        progress,
        selector,
        raw_selector,
        complete_manifest,
        complete_receipt,
        already_present,
    })
}

async fn build_completed_manifest<R>(
    view: &CoherentView<R>,
    prior_root: Option<ReceiptTreeRoot>,
    new_part: &UploadPartV1,
    new_chunks: &[BlobChunkV1],
    total_size: u64,
) -> Result<BlobManifestV1, StorageError>
where
    R: StorageAdapterRead,
{
    let mut parts = BTreeMap::<u64, UploadPartV1>::new();
    if let Some(prior_root) = prior_root {
        let mut start_after = None;
        loop {
            let page = view
                .scan_tree_page(
                    prior_root.object_id,
                    "receipt",
                    start_after.as_deref(),
                    super::tree::RECEIPT_TREE_LEAF_ENTRIES,
                )
                .await?;
            if page.is_empty() {
                break;
            }
            for (key, value) in &page {
                let part_number = u64::from_be_bytes(
                    key.as_slice()
                        .try_into()
                        .map_err(|_| corruption("receipt key is not a part number"))?,
                );
                let part_id = ObjectId::from_bytes(
                    value
                        .as_slice()
                        .try_into()
                        .map_err(|_| corruption("receipt value is not an object ID"))?,
                );
                let part_bytes = view.load_object_bytes(part_id).await?;
                let part = UploadPartV1::decode(part_id, &part_bytes)?;
                if parts.insert(part_number, part).is_some() {
                    return Err(corruption("receipt tree contains a duplicate part number"));
                }
            }
            if page.len() < super::tree::RECEIPT_TREE_LEAF_ENTRIES {
                break;
            }
            start_after = page.last().map(|(key, _)| key.clone());
        }
    }
    parts.insert(new_part.part_number, new_part.clone());
    let mut next_offset = 0_u64;
    let mut digest = blake3::Hasher::new();
    let mut blob_id = CanonicalBlobIdBuilder::default();
    let mut ordered_chunks = Vec::new();
    for (_, part) in parts {
        if part.byte_offset != next_offset {
            return Err(corruption("upload parts are not contiguous at completion"));
        }
        let mut part_digest = blake3::Hasher::new();
        for chunk_ref in &part.ordered_chunks {
            let bytes = if part.part_number == new_part.part_number {
                new_chunks
                    .iter()
                    .find_map(|chunk| {
                        chunk.encode().ok().and_then(|(id, _)| {
                            (id == chunk_ref.chunk_object_id).then_some(chunk.bytes.clone())
                        })
                    })
                    .ok_or_else(|| corruption("new upload chunk is absent"))?
            } else {
                let bytes = view.load_object_bytes(chunk_ref.chunk_object_id).await?;
                BlobChunkV1::decode(chunk_ref.chunk_object_id, &bytes)?.bytes
            };
            if bytes.len() as u64 != chunk_ref.declared_len {
                return Err(corruption("upload chunk length does not match its receipt"));
            }
            part_digest.update(&bytes);
            digest.update(&bytes);
            blob_id.update(&bytes)?;
            ordered_chunks.push(chunk_ref.clone());
        }
        if part_digest.finalize().as_bytes() != &part.part_digest {
            return Err(corruption("upload part digest does not match its chunks"));
        }
        next_offset = next_offset
            .checked_add(part.declared_part_len)
            .ok_or_else(|| corruption("upload completion byte count overflows u64"))?;
    }
    if next_offset != total_size {
        return Err(corruption(
            "upload completion size does not match its binding",
        ));
    }
    let manifest = BlobManifestV1::from_authenticated_chunks(
        next_offset,
        ordered_chunks,
        blob_id.finish(),
        *digest.finalize().as_bytes(),
    );
    let _ = manifest.encode()?;
    Ok(manifest)
}

/// Authenticates an open upload and streams its path-copied ReceiptTree in
/// canonical part order. Work is `O(P + chunks)`, reads are page bounded, and
/// memory is `O(page + manifest chunk references + one chunk)`: payload bytes
/// are hashed incrementally and never concatenated.
pub(crate) async fn prepare_upload_completion<R>(
    view: &CoherentView<R>,
    upload_id: &CanonicalUploadId,
    binding: UploadBindingRef<'_>,
) -> Result<CompletedUpload, StorageError>
where
    R: StorageAdapterRead,
{
    let raw_upload_selector = view
        .load_selector_value(&upload_selector_key(upload_id)?)
        .await?
        .ok_or_else(|| corruption("upload selector is absent"))?;
    let selector = UploadSelectorV1::decode(&raw_upload_selector)?;
    if &selector.upload_id != upload_id {
        return Err(corruption(
            "upload selector key and embedded upload ID differ",
        ));
    }
    let expected_binding = upload_binding_digest(
        binding.repository_identity,
        binding.path,
        binding.payload_domain,
        binding.declared_total_size,
        binding.declared_final_hash,
    )?;
    if selector.binding_digest != expected_binding {
        return Err(corruption(
            "upload completion binding does not match its receipt",
        ));
    }
    let progress_bytes = view.load_object_bytes(selector.progress_object_id).await?;
    let progress = UploadProgressV1::decode(selector.progress_object_id, &progress_bytes)?;
    if progress.upload_id != selector.upload_id
        || progress.binding_digest != selector.binding_digest
    {
        return Err(corruption(
            "upload selector does not authenticate its progress binding",
        ));
    }
    if progress.received_bytes != binding.declared_total_size
        || progress.contiguous_prefix_bytes != binding.declared_total_size
    {
        return Err(corruption(
            "upload receipt is incomplete or exceeds declared size",
        ));
    }

    let receipt_root = ReceiptTreeRoot {
        object_id: progress.receipt_tree_root,
        completed_part_count: progress.completed_part_count,
        received_bytes: progress.received_bytes,
        contiguous_prefix_bytes: progress.contiguous_prefix_bytes,
    };
    view.validate_receipt_root(receipt_root).await?;
    let mut ordered_chunks = Vec::new();
    let mut final_hasher = blake3::Hasher::new();
    let mut semantic_id_builder = CanonicalBlobIdBuilder::default();
    let mut next_offset = 0_u64;
    let mut part_count = 0_u64;
    let mut start_after: Option<Vec<u8>> = None;
    loop {
        let page = view
            .scan_tree_page(
                receipt_root.object_id,
                "receipt",
                start_after.as_deref(),
                super::tree::RECEIPT_TREE_LEAF_ENTRIES,
            )
            .await?;
        if page.is_empty() {
            break;
        }
        for (key, value) in &page {
            let part_number = u64::from_be_bytes(
                key.as_slice()
                    .try_into()
                    .map_err(|_| corruption("receipt key is not a part number"))?,
            );
            let part_id = ObjectId::from_bytes(
                value
                    .as_slice()
                    .try_into()
                    .map_err(|_| corruption("receipt value is not an object ID"))?,
            );
            let part_bytes = view.load_object_bytes(part_id).await?;
            let part = UploadPartV1::decode(part_id, &part_bytes)?;
            if part.upload_id != selector.upload_id
                || part.part_number != part_number
                || part.byte_offset != next_offset
            {
                return Err(corruption(
                    "receipt part key/upload/contiguous offset is inconsistent",
                ));
            }
            let mut part_hasher = blake3::Hasher::new();
            for chunk_ref in &part.ordered_chunks {
                view.authenticate_chunk(
                    chunk_ref,
                    &mut part_hasher,
                    &mut final_hasher,
                    &mut semantic_id_builder,
                )
                .await?;
            }
            if part_hasher.finalize().as_bytes() != &part.part_digest {
                return Err(corruption("upload part digest does not match its chunks"));
            }
            next_offset = next_offset
                .checked_add(part.declared_part_len)
                .ok_or_else(|| corruption("upload completion byte count overflows u64"))?;
            part_count = part_count
                .checked_add(1)
                .ok_or_else(|| corruption("upload completion part count overflows u64"))?;
            ordered_chunks.extend(part.ordered_chunks);
        }
        start_after = page.last().map(|(key, _)| key.clone());
        if page.len() < super::tree::RECEIPT_TREE_LEAF_ENTRIES {
            break;
        }
    }
    if part_count != progress.completed_part_count
        || next_offset != progress.received_bytes
        || next_offset != binding.declared_total_size
    {
        return Err(corruption(
            "receipt tree rows do not match progress aggregates",
        ));
    }
    let content_digest = *final_hasher.finalize().as_bytes();
    if binding
        .declared_final_hash
        .is_some_and(|expected| expected != content_digest)
    {
        return Err(corruption("upload final digest does not match its binding"));
    }
    let manifest = BlobManifestV1 {
        logical_bytes: next_offset,
        ordered_chunks,
        canonical_blob_id: semantic_id_builder.finish(),
        content_digest,
    };
    let _ = manifest.encode()?;
    Ok(CompletedUpload {
        view_id: view.view_id(),
        raw_upload_selector,
        selector,
        manifest,
    })
}

/// Validates the complete live receipt closure without materializing payload
/// bytes or a cumulative part list. GC uses this same serving validator, so an
/// authenticated but malformed part layout cannot be treated as a root no-op.
pub(super) async fn authenticate_open_upload_progress<R>(
    read: &R,
    progress: &UploadProgressV1,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let root = ReceiptTreeRoot {
        object_id: progress.receipt_tree_root,
        completed_part_count: progress.completed_part_count,
        received_bytes: progress.received_bytes,
        contiguous_prefix_bytes: progress.contiguous_prefix_bytes,
    };
    validate_receipt_root_on_read(root, read).await?;
    let mut start_after: Option<Vec<u8>> = None;
    let mut next_offset = 0_u64;
    let mut part_count = 0_u64;
    loop {
        let page = scan_page_on_read(
            root.object_id,
            "receipt",
            start_after.as_deref(),
            super::tree::RECEIPT_TREE_LEAF_ENTRIES,
            read,
        )
        .await?;
        if page.is_empty() {
            break;
        }
        for (key, value) in &page {
            let number = u64::from_be_bytes(
                key.as_slice()
                    .try_into()
                    .map_err(|_| corruption("receipt key is not a part number"))?,
            );
            let part_id = ObjectId::from_bytes(
                value
                    .as_slice()
                    .try_into()
                    .map_err(|_| corruption("receipt value is not an object ID"))?,
            );
            let bytes = load_object_bytes(read, part_id).await?;
            let part = UploadPartV1::decode(part_id, &bytes)?;
            if part.upload_id != progress.upload_id
                || part.part_number != number
                || part.byte_offset != next_offset
            {
                return Err(corruption(
                    "live receipt part identity/layout is inconsistent",
                ));
            }
            let mut digest = blake3::Hasher::new();
            for chunk_ref in &part.ordered_chunks {
                let bytes = load_object_bytes(read, chunk_ref.chunk_object_id).await?;
                let chunk = BlobChunkV1::decode(chunk_ref.chunk_object_id, &bytes)?;
                if chunk.bytes.len() as u64 != chunk_ref.declared_len {
                    return Err(corruption("live receipt chunk declared length is invalid"));
                }
                digest.update(&chunk.bytes);
            }
            if digest.finalize().as_bytes() != &part.part_digest {
                return Err(corruption("live receipt part digest is invalid"));
            }
            next_offset = next_offset
                .checked_add(part.declared_part_len)
                .ok_or_else(|| corruption("live receipt byte count overflows u64"))?;
            part_count = part_count
                .checked_add(1)
                .ok_or_else(|| corruption("live receipt part count overflows u64"))?;
        }
        start_after = page.last().map(|(key, _)| key.clone());
        if page.len() < super::tree::RECEIPT_TREE_LEAF_ENTRIES {
            break;
        }
    }
    if part_count != progress.completed_part_count
        || next_offset != progress.received_bytes
        || next_offset != progress.contiguous_prefix_bytes
    {
        return Err(corruption(
            "live receipt rows do not match progress aggregates",
        ));
    }
    Ok(())
}

pub(super) async fn authenticate_chunk<R>(
    read: &R,
    chunk_ref: &BlobChunkRefV1,
    part_hasher: &mut blake3::Hasher,
    final_hasher: &mut blake3::Hasher,
    semantic_id_builder: &mut CanonicalBlobIdBuilder,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let chunk_bytes = load_object_bytes(read, chunk_ref.chunk_object_id).await?;
    let chunk = BlobChunkV1::decode(chunk_ref.chunk_object_id, &chunk_bytes)?;
    if chunk.bytes.len() as u64 != chunk_ref.declared_len {
        return Err(corruption(
            "upload chunk bytes do not match declared length",
        ));
    }
    part_hasher.update(&chunk.bytes);
    final_hasher.update(&chunk.bytes);
    semantic_id_builder.update(&chunk.bytes)?;
    Ok(())
}

#[cfg(test)]
mod canonical_blob_id_tests {
    use super::{
        CANONICAL_BLOB_CHUNK_BYTES, CanonicalBlobIdBuilder, bind_historical_state_blob_ref,
    };
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;
    use crate::forktree::ObjectId;
    use crate::forktree::state::{StateCell, StateKey, StateValue};

    fn key(id: &str) -> StateKey {
        StateKey {
            schema_key: "lix_binary_blob_ref".to_owned(),
            file_id: Some(id.to_owned()),
            entity_pk: EntityPk::uuid_from_canonical(id).expect("canonical historical blob id"),
        }
    }

    fn value(_id: &str, cell: StateCell, manifest_count: usize) -> StateValue {
        let timestamp = LixTimestamp::from_unix_millis_utc_lossy(0);
        StateValue {
            change_id: crate::changelog::ChangeId::for_test_label("change"),
            commit_id: crate::changelog::CommitId::for_test_label("commit"),
            created_at: timestamp,
            updated_at: timestamp,
            cell,
            metadata: None,
            origin_key: None,
            blob_manifest_object_ids: (0..manifest_count)
                .map(|index| ObjectId::from_bytes([index as u8 + 1; 32]))
                .collect(),
        }
    }

    #[test]
    fn streaming_identity_matches_complete_multi_chunk_content() {
        let payload = vec![0x5a; 2 * 1024 * 1024 + 17];
        let mut builder = CanonicalBlobIdBuilder::default();
        for fragment in payload.chunks(333_333) {
            builder.update(fragment).expect("streaming identity update");
        }
        assert_eq!(
            builder.finish(),
            crate::binary_cas::BlobId::from_content(&payload)
        );
    }

    #[test]
    fn inline_identity_vectors_match_legacy_ids_and_digest() {
        for size in [0, 1024, 1024 * 1024, 1024 * 1024 + 1, 64 * 1024 * 1024] {
            let payload: Vec<u8> = (0..size)
                .map(|index| (index as u64).wrapping_mul(37).wrapping_add(11) as u8)
                .collect();
            let mut builder = CanonicalBlobIdBuilder::default();
            let mut digest = blake3::Hasher::new();
            for chunk in payload.chunks(CANONICAL_BLOB_CHUNK_BYTES) {
                builder
                    .update_fixed_chunk(chunk)
                    .expect("inline identity update");
                digest.update(chunk);
            }
            assert_eq!(
                builder.finish(),
                crate::binary_cas::BlobId::from_content(&payload),
                "fixed-chunk BlobId changed at {size} bytes"
            );
            assert_eq!(
                *digest.finalize().as_bytes(),
                *blake3::hash(&payload).as_bytes(),
                "content digest changed at {size} bytes"
            );
        }
    }

    #[test]
    fn inline_same_size_substitution_changes_both_identity_claims() {
        let first = vec![0x11; CANONICAL_BLOB_CHUNK_BYTES + 1];
        let mut second = first.clone();
        second[CANONICAL_BLOB_CHUNK_BYTES] = 0x22;

        let identity = |payload: &[u8]| {
            let mut builder = CanonicalBlobIdBuilder::default();
            let mut digest = blake3::Hasher::new();
            for chunk in payload.chunks(CANONICAL_BLOB_CHUNK_BYTES) {
                builder.update_fixed_chunk(chunk).expect("identity update");
                digest.update(chunk);
            }
            (builder.finish(), *digest.finalize().as_bytes())
        };

        assert_ne!(identity(&first), identity(&second));
    }

    #[test]
    fn historical_blob_binding_rejects_wrong_row_and_non_live_cells() {
        let id = "01920000-0000-7000-8000-0000000000a1";
        let hash = crate::binary_cas::BlobId::from_content(b"payload").to_hex();
        let live = StateCell::Value(
            serde_json::json!({"id": id, "blob_hash": hash, "size_bytes": 7})
                .to_string()
                .into(),
        );
        assert!(bind_historical_state_blob_ref(&key(id), &value(id, live.clone(), 1)).is_ok());

        let substituted = StateCell::Value(
            serde_json::json!({
                "id": "01920000-0000-7000-8000-0000000000b2",
                "blob_hash": hash,
                "size_bytes": 7
            })
            .to_string()
            .into(),
        );
        assert!(bind_historical_state_blob_ref(&key(id), &value(id, substituted, 1)).is_err());
        assert!(
            bind_historical_state_blob_ref(&key(id), &value(id, StateCell::Tombstone, 1)).is_err()
        );
        assert!(bind_historical_state_blob_ref(&key(id), &value(id, StateCell::Null, 1)).is_err());
        assert!(
            bind_historical_state_blob_ref(&key(id), &value(id, StateCell::Value("{}".into()), 2))
                .is_err()
        );

        let wrong_typed_key = StateKey {
            schema_key: "lix_binary_blob_ref".to_owned(),
            file_id: Some(id.to_owned()),
            entity_pk: EntityPk::single(id),
        };
        assert!(bind_historical_state_blob_ref(&wrong_typed_key, &value(id, live, 1)).is_err());

        let missing_id = StateCell::Value(
            serde_json::json!({"blob_hash": hash, "size_bytes": 7})
                .to_string()
                .into(),
        );
        assert!(bind_historical_state_blob_ref(&key(id), &value(id, missing_id, 1)).is_err());
    }
}
