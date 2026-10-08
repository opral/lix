//! Shared admission and packing budgets at the transfer boundary.
//! Operation validators retain ownership of proof, coverage and publication.
use crate::LixError;
use serde::Serialize;
use std::future::Future;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) const CONTENT_GROUP_ITEMS: usize = 32;
pub(crate) const CONTENT_GROUP_BYTES: usize = 1024 * 1024;
pub(super) const CHUNK_CONCURRENCY: usize = 6;
pub(super) const MANIFEST_PAGE_ITEMS: usize = super::MAX_SYNC_BLOB_BATCH_ITEMS;
pub(super) const MANIFEST_PAGE_ENCODED_BYTES: usize = CONTENT_GROUP_BYTES;
pub(super) const MANIFEST_PAGE_DECODED_BYTES: usize = CONTENT_GROUP_BYTES;
pub(super) const MAX_MANIFEST_SINGLETON_ENCODED_BYTES: usize = 2 * CONTENT_GROUP_BYTES;

/// One read closure may retain at most one transport page per native process
/// or WASM instance. Requests that cannot reserve this capacity use their
/// existing durable scratch path without waiting.
pub(crate) const RETAINED_READ_CLOSURE_BYTES: usize = 4 * 1024 * 1024;
static RETAINED_READ_CLOSURE_RESERVED: AtomicBool = AtomicBool::new(false);

pub(crate) struct RetainedPayloadPermit;

impl RetainedPayloadPermit {
    pub(crate) fn try_acquire() -> Option<Self> {
        RETAINED_READ_CLOSURE_RESERVED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| Self)
    }
}

impl Drop for RetainedPayloadPermit {
    fn drop(&mut self) {
        RETAINED_READ_CLOSURE_RESERVED.store(false, Ordering::Release);
    }
}

struct BoundedJsonSizeWriter {
    written: usize,
    limit: usize,
}

impl std::io::Write for BoundedJsonSizeWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self.written.checked_add(bytes.len()).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "encoded size overflow")
        })?;
        if next > self.limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "encoded transfer page exceeds its byte budget",
            ));
        }
        self.written = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Measures encoded JSON through a capped writer so rejected pages never
/// allocate an unbounded serialized copy.
pub(super) fn bounded_json_size<T: Serialize + ?Sized>(value: &T, limit: usize) -> Result<usize, LixError> {
    let mut writer = BoundedJsonSizeWriter { written: 0, limit };
    serde_json::to_writer(&mut writer, value).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("serialized transfer page exceeds its {limit}-byte bound: {error}"),
        )
    })?;
    Ok(writer.written)
}

fn manifest_inline_decoded_bytes(manifests: &[super::SyncBlobManifest]) -> Result<usize, LixError> {
    manifests.iter().try_fold(0usize, |total, manifest| {
        let Some(encoded) = manifest.inline_bytes_base64.as_deref() else {
            return Ok(total);
        };
        if encoded.len() > super::blob::MAX_INLINE_SYNC_BLOB_BYTES.div_ceil(3) * 4 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync inline blob exceeds its per-manifest byte bound",
            ));
        }
        // Valid base64 is a multiple of four bytes. Use a conservative rounded
        // estimate for malformed lengths; normal decode below reports syntax
        // errors after page admission. Subtract legal padding so an exact
        // decoded-byte budget does not reject one or two padded bytes.
        let encoded_groups = encoded.len().div_ceil(4);
        let upper_bound = encoded_groups.checked_mul(3).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync inline blob decoded size overflowed",
            )
        })?;
        let padding = encoded
            .as_bytes()
            .iter()
            .rev()
            .take(2)
            .take_while(|byte| **byte == b'=')
            .count();
        let decoded = upper_bound.saturating_sub(padding);
        total.checked_add(decoded).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync inline page decoded size overflowed",
            )
        })
    })
}

/// Validates one incoming manifest page against the normal group budget or
/// the existing large, no-inline singleton inventory lane.
pub(super) fn validate_manifest_page(
    manifests: &[super::SyncBlobManifest],
) -> Result<(), LixError> {
    if manifests.len() > MANIFEST_PAGE_ITEMS {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "manifest page exceeds its item budget",
        ));
    }
    let encoded = bounded_json_size(manifests, MAX_MANIFEST_SINGLETON_ENCODED_BYTES)?;
    let decoded = manifest_inline_decoded_bytes(manifests)?;
    if encoded <= MANIFEST_PAGE_ENCODED_BYTES && decoded <= MANIFEST_PAGE_DECODED_BYTES {
        return Ok(());
    }
    if manifests.len() == 1
        && manifests[0].inline_bytes_base64.is_none()
        && encoded <= MAX_MANIFEST_SINGLETON_ENCODED_BYTES
    {
        return Ok(());
    }
    Err(LixError::new(
        LixError::CODE_INVALID_PARAM,
        "manifest page exceeds its bounded encoded or decoded byte budget",
    ))
}

fn append_manifest_page_range(batch: &TransferBatch<usize>, pages: &mut Vec<Range<usize>>) {
    if let (Some(start), Some(end)) = (batch.items.first(), batch.items.last()) {
        pages.push(*start..end.saturating_add(1));
    }
}

/// Partitions one fetched, ordered manifest slice into installable bounded
/// pages. The returned ranges borrow no data and preserve all response slots.
pub(super) fn partition_manifest_pages(
    manifests: &[super::SyncBlobManifest],
) -> Result<Vec<Range<usize>>, LixError> {
    if manifests.len() > MANIFEST_PAGE_ITEMS {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "manifest response exceeds its item budget",
        ));
    }
    let mut pages = Vec::new();
    let mut batch = TransferBatch::<usize>::with_limits(
        MANIFEST_PAGE_ITEMS,
        MANIFEST_PAGE_ENCODED_BYTES,
        MANIFEST_PAGE_DECODED_BYTES,
    );

    for (index, manifest) in manifests.iter().enumerate() {
        let encoded = bounded_json_size(
            manifest,
            MAX_MANIFEST_SINGLETON_ENCODED_BYTES.saturating_sub(2),
        )?;
        let decoded = manifest_inline_decoded_bytes(std::slice::from_ref(manifest))?;
        let single_encoded = encoded.saturating_add(2);
        if single_encoded > MANIFEST_PAGE_ENCODED_BYTES || decoded > MANIFEST_PAGE_DECODED_BYTES {
            append_manifest_page_range(&batch, &mut pages);
            batch = TransferBatch::with_limits(
                MANIFEST_PAGE_ITEMS,
                MANIFEST_PAGE_ENCODED_BYTES,
                MANIFEST_PAGE_DECODED_BYTES,
            );
            if manifest.inline_bytes_base64.is_some()
                || single_encoded > MAX_MANIFEST_SINGLETON_ENCODED_BYTES
            {
                return Err(LixError::new(
                    "LIX_TRANSFER_MEMBER_TOO_LARGE",
                    "manifest requires a smaller payload lane",
                ));
            }
            pages.push(index..index + 1);
            continue;
        }

        if let Some(returned_index) = batch.push(index, encoded, decoded)? {
            append_manifest_page_range(&batch, &mut pages);
            batch = TransferBatch::with_limits(
                MANIFEST_PAGE_ITEMS,
                MANIFEST_PAGE_ENCODED_BYTES,
                MANIFEST_PAGE_DECODED_BYTES,
            );
            if batch.push(returned_index, encoded, decoded)?.is_some() {
                return Err(LixError::new(
                    "LIX_TRANSFER_MEMBER_TOO_LARGE",
                    "manifest does not fit an empty transfer page",
                ));
            }
        }
    }
    append_manifest_page_range(&batch, &mut pages);
    for page in &pages {
        validate_manifest_page(&manifests[page.clone()])?;
    }
    Ok(pages)
}

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

/// One bounded content page. The binary-CAS owner supplies the per-chunk raw
/// payload ceiling; six in flight therefore bound payload residency to 24 MiB.
/// Validate every present member before callers handle a missing sibling or
/// install anything.
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
    let max_chunk_bytes = crate::binary_cas::raw_chunk_transfer_bounds().max_payload_bytes;
    for (hash, value) in hashes.iter().zip(&values) {
        if let Some(bytes) = value {
            if bytes.len() > max_chunk_bytes
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
    let max_chunk_bytes = crate::binary_cas::raw_chunk_transfer_bounds().max_payload_bytes;
    for (id, bytes) in chunks {
        let hash = crate::binary_cas::ChunkHash::from_hex(id)?;
        if bytes.is_empty()
            || bytes.len() > max_chunk_bytes
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
    let encoded =
        bounded_json_size(manifests, MAX_MANIFEST_SINGLETON_ENCODED_BYTES).map_err(|_| {
            LixError::new(
                "LIX_TRANSFER_MEMBER_TOO_LARGE",
                "manifest inventory exceeds its bounded transfer lane",
            )
        })?;
    if encoded <= CONTENT_GROUP_BYTES {
        return register_group(transport, manifests).await;
    }
    if manifests.len() != 1 || manifests[0].inline_bytes_base64.is_some() {
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
