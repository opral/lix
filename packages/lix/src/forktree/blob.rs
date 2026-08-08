use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use serde::Deserialize;

use crate::storage::{
    CoreProjection, GetManyRequest, GetOptions, Key, ProjectedValue, StorageError,
};
use crate::storage_adapter::StorageAdapterRead;

use super::codec::corruption;
use super::model::{
    BlobChunkRefV1, BlobChunkV1, BlobManifestV1, CanonicalBranchId, CanonicalUploadId,
    UploadPartV1, UploadProgressV1, UploadSelectorV1, upload_binding_digest, upload_selector_key,
};
use super::object::ObjectId;
use super::state::{StateCell, StateKey};
use super::tree::{ReceiptTreeRoot, scan_page_on_read, validate_receipt_root_on_read};
use super::view::{CoherentView, SELECTOR_SPACE, load_object_bytes, load_object_map};

const CANONICAL_BLOB_CHUNK_BYTES: usize = 1024 * 1024;

/// Computes the existing public fixed-chunk BlobId while upload completion is
/// already authenticating payload bytes. Memory is one canonical chunk plus
/// O(chunk-count) hashes; the result is copied into the manifest only as an
/// integrity claim checked against the selected state owner.
#[derive(Default)]
struct CanonicalBlobIdBuilder {
    total_size: u64,
    pending: Vec<u8>,
    chunks: Vec<(crate::binary_cas::ChunkHash, u64)>,
}

impl CanonicalBlobIdBuilder {
    fn update(&mut self, mut bytes: &[u8]) -> Result<(), StorageError> {
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

    fn finish(mut self) -> crate::binary_cas::BlobId {
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
    view_id: [u8; 32],
    view_instance_id: u64,
}

#[derive(Deserialize)]
struct BlobRefOwnerValue {
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
    view_id: [u8; 32],
    view_instance_id: u64,
) -> Result<AuthenticatedBlobRef, crate::LixError> {
    let key = super::state::decode_state_key(&row.encoded_key)?;
    if key.schema_key != "lix_binary_blob_ref" {
        return Err(corruption("authenticated state row is not a blob-reference owner").into());
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
    pub(crate) fn bind_blob(
        &self,
        row: &super::serving::VisibleStateRow,
    ) -> Result<AuthenticatedBlobRef, crate::LixError> {
        if row.view_instance_id != self.view_instance_id() {
            return Err(StorageError::InvalidCursor.into());
        }
        bind_state_blob_ref(row, self.view_id(), self.view_instance_id())
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
        row.map(|row| self.bind_blob(&row)).transpose()
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
        load_blob_bytes_many_on_read(
            self.storage_read(),
            self.view_id(),
            self.view_instance_id(),
            refs,
        )
        .await
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
        load_blob_ranges_many_on_read(
            self.storage_read(),
            self.view_id(),
            self.view_instance_id(),
            requests,
        )
        .await
    }
}

/// Loads complete payloads through state-authenticated manifest edges. One
/// object batch authenticates all manifests and one batch authenticates all
/// distinct chunks, so adapter calls scale with object levels rather than rows.
async fn load_blob_bytes_many_on_read<R>(
    read: &R,
    view_id: [u8; 32],
    view_instance_id: u64,
    refs: &[AuthenticatedBlobRef],
) -> Result<crate::binary_cas::BlobBytesBatch, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    validate_blob_ref_views(view_id, view_instance_id, refs.iter())?;
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

/// Loads only chunks intersecting each requested range while preserving the
/// same authenticated state/manifest ownership as full reads.
async fn load_blob_ranges_many_on_read<R>(
    read: &R,
    view_id: [u8; 32],
    view_instance_id: u64,
    requests: &[(AuthenticatedBlobRef, Range<u64>)],
) -> Result<crate::binary_cas::BlobRangeBytesBatch, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    validate_blob_ref_views(
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
    view_id: [u8; 32],
    view_instance_id: u64,
    refs: impl IntoIterator<Item = &'a AuthenticatedBlobRef>,
) -> Result<(), crate::LixError> {
    if refs.into_iter().any(|reference| {
        reference.view_id != view_id || reference.view_instance_id != view_instance_id
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
    let ids = refs
        .iter()
        .map(|reference| reference.manifest_object_id)
        .collect::<BTreeSet<_>>();
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
    manifests
        .get(&id)
        .ok_or_else(|| corruption(format!("blob manifest {id} is absent")).into())
}

fn validate_manifest_owner(
    manifest: &BlobManifestV1,
    reference: &AuthenticatedBlobRef,
) -> Result<(), crate::LixError> {
    if manifest.logical_bytes != reference.expected_size {
        return Err(corruption(
            "blob-reference owner size does not match its authenticated manifest",
        )
        .into());
    }
    if manifest.canonical_blob_id != reference.semantic_id {
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
    let selector_key = [Key(upload_selector_key(upload_id)?)];
    let loaded = view
        .storage_read()
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &selector_key,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    let raw_upload_selector = match loaded.values.as_slice() {
        [Some(ProjectedValue::FullValue(bytes))] => bytes.clone(),
        [Some(ProjectedValue::KeyOnly)] => {
            return Err(corruption(
                "upload selector point read returned key-only data",
            ));
        }
        [None] => return Err(corruption("upload selector is absent")),
        _ => return Err(corruption("upload selector read cardinality is invalid")),
    };
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
    let progress_bytes =
        load_object_bytes(view.storage_read(), selector.progress_object_id).await?;
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
    validate_receipt_root_on_read(receipt_root, view.storage_read()).await?;
    let mut ordered_chunks = Vec::new();
    let mut final_hasher = blake3::Hasher::new();
    let mut semantic_id_builder = CanonicalBlobIdBuilder::default();
    let mut next_offset = 0_u64;
    let mut part_count = 0_u64;
    let mut start_after: Option<Vec<u8>> = None;
    loop {
        let page = scan_page_on_read(
            receipt_root.object_id,
            "receipt",
            start_after.as_deref(),
            super::tree::RECEIPT_TREE_LEAF_ENTRIES,
            view.storage_read(),
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
            let part_bytes = load_object_bytes(view.storage_read(), part_id).await?;
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
                authenticate_chunk(
                    view.storage_read(),
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

async fn authenticate_chunk<R>(
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
    use super::CanonicalBlobIdBuilder;

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
}
