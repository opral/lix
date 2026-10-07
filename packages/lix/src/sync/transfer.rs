//! Shared admission and packing budgets at the transfer boundary.
//! Operation validators retain ownership of proof, coverage and publication.
use crate::LixError;
use std::future::Future;

pub(crate) const CONTENT_GROUP_ITEMS: usize = 32;
pub(crate) const CONTENT_GROUP_BYTES: usize = 1024 * 1024;
pub(super) const CHUNK_CONCURRENCY: usize = 6;

#[derive(Debug)]
pub(super) struct TransferBatch<T> {
    pub(super) items: Vec<T>,
    item_count: usize,
    encoded: usize,
    decoded: usize,
    item_limit: usize,
    encoded_limit: usize,
    decoded_limit: usize,
}

impl<T> TransferBatch<T> {
    pub(super) fn new() -> Self {
        Self::with_limits(
            CONTENT_GROUP_ITEMS,
            CONTENT_GROUP_BYTES,
            CONTENT_GROUP_BYTES,
        )
    }

    /// Construct a batch with the caller's explicit policy limits. Items may
    /// represent several indivisible transfer members; `push_counted` keeps
    /// the member budget meaningful when those validation units are packed.
    pub(super) fn with_limits(
        item_limit: usize,
        encoded_limit: usize,
        decoded_limit: usize,
    ) -> Self {
        Self {
            items: Vec::new(),
            item_count: 0,
            encoded: 2,
            decoded: 0,
            item_limit,
            encoded_limit,
            decoded_limit,
        }
    }

    /// A non-fitting member is returned to the caller for the next page.
    /// A single oversized member must use the operation's streaming lane.
    pub(super) fn push(
        &mut self,
        item: T,
        encoded: usize,
        decoded: usize,
    ) -> Result<Option<T>, LixError> {
        self.push_counted(item, 1, encoded, decoded)
    }

    /// Push one indivisible unit that contains `item_count` transfer members.
    /// The unit is either retained whole or returned whole for the next page.
    pub(super) fn push_counted(
        &mut self,
        item: T,
        item_count: usize,
        encoded: usize,
        decoded: usize,
    ) -> Result<Option<T>, LixError> {
        if item_count == 0
            || item_count > self.item_limit
            || encoded.saturating_add(2) > self.encoded_limit
            || decoded > self.decoded_limit
        {
            return Err(LixError::new(
                "LIX_TRANSFER_MEMBER_TOO_LARGE",
                "transfer member requires the streaming lane",
            ));
        }
        let encoded = encoded.saturating_add(usize::from(!self.items.is_empty()));
        if self.item_count.saturating_add(item_count) > self.item_limit
            || self.encoded.saturating_add(encoded) > self.encoded_limit
            || self.decoded.saturating_add(decoded) > self.decoded_limit
        {
            return Ok(Some(item));
        }
        self.encoded += encoded;
        self.decoded += decoded;
        self.item_count += item_count;
        self.items.push(item);
        Ok(None)
    }
}

/// Common negotiation boundary for partial, full and migration uploads.
/// Even custom transports must preserve response order and requested hashes.
pub(super) async fn register_group<T: super::SyncTransport>(
    transport: &T,
    manifests: &[super::SyncBlobManifest],
) -> Result<Vec<super::SyncBlobRegistration>, LixError> {
    if manifests.is_empty() {
        return Ok(Vec::new());
    }
    super::blob::validate_manifest_group(manifests)?;
    // A body limit is an explicit rejection before admission. Split only that
    // response; ambiguous failures retain their ordinary retry identity.
    let mut pending = vec![manifests];
    let mut registrations = Vec::with_capacity(manifests.len());
    while let Some(group) = pending.pop() {
        match transport.register_blobs(group).await {
            Ok(results) if results.len() == group.len() => registrations.extend(results),
            Ok(_) => {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "blob registration group cardinality differs",
                ));
            }
            Err(error) if group.len() > 1 && explicit_body_limit(&error) => {
                let middle = group.len() / 2;
                pending.push(&group[middle..]);
                pending.push(&group[..middle]);
            }
            Err(error) => return Err(error),
        }
    }
    if registrations.len() != manifests.len() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "blob registration group cardinality differs",
        ));
    }
    for (manifest, registration) in manifests.iter().zip(&registrations) {
        if registration
            .missing_chunk_ids
            .iter()
            .any(|id| !manifest.chunks.iter().any(|chunk| &chunk.chunk_id == id))
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "authority requested an unrelated upload chunk",
            ));
        }
    }
    Ok(registrations)
}

pub(super) async fn register_inline_group<T: super::SyncTransport>(
    transport: &T,
    batch: &TransferBatch<super::SyncBlobManifest>,
) -> Result<(), LixError> {
    if batch
        .items
        .iter()
        .any(|manifest| manifest.inline_bytes_base64.is_none())
    {
        return Err(LixError::unknown("inline transfer group lacks content"));
    }
    if register_group(transport, &batch.items)
        .await?
        .iter()
        .any(|registration| !registration.missing_chunk_ids.is_empty())
    {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "authority inline content group remains incomplete",
        ));
    }
    Ok(())
}

/// One bounded content page. Raw chunk requests have a 4 MiB body ceiling;
/// six in flight therefore bound payload residency to 24 MiB. Validate every
/// present member before callers handle a missing sibling or install anything.
pub(super) async fn fetch_chunk_page<T: super::SyncTransport>(
    transport: &T,
    ids: &[String],
) -> Result<Vec<Option<Vec<u8>>>, LixError> {
    if ids.len() > CHUNK_CONCURRENCY {
        return Err(LixError::unknown("chunk page exceeds concurrency budget"));
    }
    let hashes = ids
        .iter()
        .map(|id| crate::binary_cas::ChunkHash::from_hex(id))
        .collect::<Result<Vec<_>, _>>()?;
    let values =
        futures_util::future::try_join_all(ids.iter().map(|id| transport.get_chunk(id))).await?;
    for (hash, value) in hashes.iter().zip(&values) {
        if let Some(bytes) = value {
            if bytes.len() > 4 * 1024 * 1024
                || bytes.is_empty()
                || crate::binary_cas::ChunkHash::from_content(bytes) != *hash
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "content page chunk violates its size or hash",
                ));
            }
        }
    }
    Ok(values)
}

pub(super) fn explicit_body_limit(error: &LixError) -> bool {
    error
        .details
        .as_ref()
        .and_then(|details| details.get("httpStatus"))
        .and_then(serde_json::Value::as_u64)
        == Some(413)
        || (error.code == "LIX_ERROR_REQUEST_BODY_TOO_LARGE" && error.details.is_none())
}

/// Upload independently hashed chunks under the same payload and concurrency
/// bounds as downloads. Validate the complete page before starting any RPC.
pub(super) async fn upload_chunk_page<T: super::SyncTransport>(
    transport: &T,
    chunks: &[(String, &[u8])],
) -> Result<(), LixError> {
    if chunks.len() > CHUNK_CONCURRENCY {
        return Err(LixError::unknown("chunk page exceeds concurrency budget"));
    }
    let mut seen = std::collections::BTreeSet::new();
    for (id, bytes) in chunks {
        let hash = crate::binary_cas::ChunkHash::from_hex(id)?;
        if bytes.is_empty()
            || bytes.len() > 4 * 1024 * 1024
            || hash != crate::binary_cas::ChunkHash::from_content(bytes)
            || !seen.insert(id)
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "upload page violates chunk size, hash or uniqueness",
            ));
        }
    }
    futures_util::future::try_join_all(
        chunks
            .iter()
            .map(|(id, bytes)| transport.put_chunk(id, bytes)),
    )
    .await?;
    Ok(())
}

/// Confirm sparse, authority-backed references without downloading content.
/// The caller packs IDs while discovering dependencies, keeping the buffer finite.
pub(super) async fn confirm_authority_group<T: super::SyncTransport>(
    transport: &T,
    ids: &[String],
) -> Result<(), LixError> {
    if ids.is_empty() {
        return Ok(());
    }
    if ids.len() > super::MAX_SYNC_BLOB_BATCH_ITEMS {
        return Err(LixError::unknown(
            "authority confirmation exceeds group bound",
        ));
    }
    let manifests = transport.get_blobs(ids).await?;
    if manifests.len() != ids.len()
        || manifests
            .iter()
            .zip(ids)
            .any(|(manifest, id)| &manifest.blob_id != id)
    {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "authority must confirm exactly the referenced blob group",
        ));
    }
    for manifest in &manifests {
        super::blob::validate_sync_blob_manifest(manifest)?;
    }
    Ok(())
}

/// One explicitly bounded manifest lane also admits a large receipt inventory.
/// Payload bytes always travel in the chunk lane; ordinary manifests use groups.
pub(super) async fn register_manifest_page<T: super::SyncTransport>(
    transport: &T,
    manifests: &[super::SyncBlobManifest],
) -> Result<Vec<super::SyncBlobRegistration>, LixError> {
    let encoded = serde_json::to_vec(manifests)
        .map_err(|error| LixError::unknown(error.to_string()))?
        .len();
    if encoded <= CONTENT_GROUP_BYTES {
        return register_group(transport, manifests).await;
    }
    if manifests.len() != 1
        || encoded > 2 * CONTENT_GROUP_BYTES
        || manifests[0].inline_bytes_base64.is_some()
    {
        return Err(LixError::new(
            "LIX_TRANSFER_MEMBER_TOO_LARGE",
            "manifest inventory exceeds its bounded transfer lane",
        ));
    }
    super::blob::validate_sync_blob_manifest(&manifests[0])?;
    let result = transport.register_blob(&manifests[0]).await?;
    if result.missing_chunk_ids.iter().any(|id| {
        !manifests[0]
            .chunks
            .iter()
            .any(|chunk| &chunk.chunk_id == id)
    }) {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "authority requested an unrelated upload chunk",
        ));
    }
    Ok(vec![result])
}

/// Canonical content from flat or historical delta storage uses the same
/// bounded anchor decoder and chunk executor. The caller supplies a fresh,
/// guarded read; it is dropped before any network transfer.
#[derive(Clone)]
pub(super) struct CanonicalUploadPlan {
    pub(super) metadata: crate::binary_cas::BlobMetadata,
    pub(super) canonical: crate::binary_cas::CanonicalBlobManifest,
}

impl CanonicalUploadPlan {
    pub(super) fn manifest(&self) -> super::SyncBlobManifest {
        super::SyncBlobManifest {
            blob_id: self.canonical.blob_id.to_hex(),
            size_bytes: self.canonical.size_bytes,
            chunks: self
                .canonical
                .chunks
                .iter()
                .map(|chunk| super::SyncBlobChunk {
                    chunk_id: chunk.hash.to_hex(),
                    size_bytes: chunk.size_bytes,
                })
                .collect(),
            inline_bytes_base64: None,
        }
    }
    pub(super) fn encoded_bytes(&self) -> Result<usize, LixError> {
        serde_json::to_vec(&self.manifest())
            .map(|bytes| bytes.len())
            .map_err(|error| LixError::unknown(error.to_string()))
    }
}

/// Negotiate all compatible manifests before transferring their shared missing
/// chunk frontier. Receipt indexes are bounded independently of blob payloads.
pub(super) async fn upload_canonical_page<T, R, F, Fut>(
    transport: &T,
    plans: &[CanonicalUploadPlan],
    begin_read: F,
) -> Result<(), LixError>
where
    T: super::SyncTransport,
    R: crate::storage_adapter::StorageAdapterRead,
    F: Fn() -> Fut,
    Fut: Future<Output = Result<R, LixError>>,
{
    if plans.is_empty() {
        return Ok(());
    }
    let manifests = plans
        .iter()
        .map(CanonicalUploadPlan::manifest)
        .collect::<Vec<_>>();
    let registrations = register_manifest_page(transport, &manifests).await?;
    let incomplete = manifests
        .iter()
        .zip(&registrations)
        .filter_map(|(manifest, registration)| {
            (!registration.missing_chunk_ids.is_empty()).then(|| manifest.clone())
        })
        .collect::<Vec<_>>();
    let mut missing = registrations
        .into_iter()
        .flat_map(|registration| registration.missing_chunk_ids)
        .collect::<std::collections::BTreeSet<_>>();
    for (plan, manifest) in plans.iter().zip(&manifests) {
        let mut offset = 0u64;
        let mut first = 0;
        while first < manifest.chunks.len() && !missing.is_empty() {
            let mut end = first;
            let mut size = 0u64;
            while end < manifest.chunks.len() && size < crate::binary_cas::CHUNK_ANCHOR_BYTES as u64
            {
                size += manifest.chunks[end].size_bytes;
                end += 1;
            }
            let expected = &manifest.chunks[first..end];
            if expected
                .iter()
                .any(|chunk| missing.contains(&chunk.chunk_id))
            {
                let read = begin_read().await?;
                let chunks =
                    crate::binary_cas::load_canonical_blob_anchor(&read, &plan.metadata, offset)
                        .await?;
                drop(read);
                if chunks.len() != expected.len()
                    || chunks.iter().zip(expected).any(|(chunk, receipt)| {
                        chunk.receipt.hash.to_hex() != receipt.chunk_id
                            || chunk.receipt.size_bytes != receipt.size_bytes
                    })
                {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "canonical upload anchor changed after manifest preparation",
                    ));
                }
                let uploads = chunks
                    .iter()
                    .filter_map(|chunk| {
                        let id = chunk.receipt.hash.to_hex();
                        missing.remove(&id).then_some((id, chunk.bytes.as_slice()))
                    })
                    .collect::<Vec<_>>();
                for page in uploads.chunks(CHUNK_CONCURRENCY) {
                    upload_chunk_page(transport, page).await?;
                }
            }
            offset += size;
            first = end;
        }
    }
    if !missing.is_empty() {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "canonical upload plan did not supply its negotiated chunk frontier",
        ));
    }
    if !incomplete.is_empty()
        && register_manifest_page(transport, &incomplete)
            .await?
            .iter()
            .any(|registration| !registration.missing_chunk_ids.is_empty())
    {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "authority content group remains incomplete after canonical upload",
        ));
    }
    Ok(())
}

/// Pack canonical plans through the same byte and member budgets for every
/// upload owner. Oversized receipt inventories have their explicit bounded lane.
pub(super) async fn pack_canonical_plan<T, R, F, Fut>(
    transport: &T,
    group: &mut TransferBatch<CanonicalUploadPlan>,
    plan: CanonicalUploadPlan,
    begin_read: F,
) -> Result<(), LixError>
where
    T: super::SyncTransport,
    R: crate::storage_adapter::StorageAdapterRead,
    F: Fn() -> Fut,
    Fut: Future<Output = Result<R, LixError>>,
{
    let encoded = plan.encoded_bytes()?;
    if encoded.saturating_add(2) > CONTENT_GROUP_BYTES {
        upload_canonical_page(transport, &group.items, &begin_read).await?;
        *group = TransferBatch::new();
        return upload_canonical_page(transport, &[plan], &begin_read).await;
    }
    if let Some(plan) = group.push(plan, encoded, 0)? {
        upload_canonical_page(transport, &group.items, &begin_read).await?;
        *group = TransferBatch::new();
        if group.push(plan, encoded, 0)?.is_some() {
            return Err(LixError::unknown(
                "single canonical upload plan did not fit",
            ));
        }
    }
    Ok(())
}
