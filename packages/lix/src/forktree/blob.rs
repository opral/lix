use bytes::Bytes;

use crate::storage::{
    CoreProjection, GetManyRequest, GetOptions, Key, ProjectedValue, StorageError,
};
use crate::storage_adapter::StorageAdapterRead;

use super::codec::corruption;
use super::model::{
    BlobChunkRefV1, BlobChunkV1, BlobManifestV1, CanonicalUploadId, UploadPartV1, UploadProgressV1,
    UploadSelectorV1, upload_binding_digest, upload_selector_key,
};
use super::object::ObjectId;
use super::tree::{ReceiptTreeRoot, scan_page_on_read, validate_receipt_root_on_read};
use super::view::{CoherentView, SELECTOR_SPACE, load_object_bytes};

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
        .read()
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
    let progress_bytes = load_object_bytes(view.read(), selector.progress_object_id).await?;
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
    validate_receipt_root_on_read(receipt_root, view.read()).await?;
    let mut ordered_chunks = Vec::new();
    let mut final_hasher = blake3::Hasher::new();
    let mut next_offset = 0_u64;
    let mut part_count = 0_u64;
    let mut start_after: Option<Vec<u8>> = None;
    loop {
        let page = scan_page_on_read(
            receipt_root.object_id,
            "receipt",
            start_after.as_deref(),
            super::tree::RECEIPT_TREE_LEAF_ENTRIES,
            view.read(),
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
            let part_bytes = load_object_bytes(view.read(), part_id).await?;
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
                authenticate_chunk(view.read(), chunk_ref, &mut part_hasher, &mut final_hasher)
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
    Ok(())
}
