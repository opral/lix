//! Lazy binary-CAS sync outside the live commit/ref cursor.

use base64::Engine as _;

use crate::binary_cas::{
    BlobChunkReceipt, BlobId, CanonicalBlobChunk, CanonicalBlobManifest, ChunkHash,
    chunk_presence_many, load_canonical_blob_chunks, load_metadata_many, load_verified_chunk,
    stage_deferred_canonical_manifest, stage_transfer_publication_fence,
    stage_verified_canonical_manifest, stage_verified_inline_canonical_blob,
    stage_verified_raw_chunk,
};
use crate::storage_adapter::{Storage, StorageReadOptions, StorageWriteOptions};
use crate::{Lix, LixError};

use super::{SyncBlobChunk, SyncBlobManifest, SyncBlobRegistration};

const MAX_SYNC_BLOB_CHUNKS: usize = 16_384;
const MAX_INLINE_SYNC_BLOB_BYTES: usize = 64 * 1024;

fn decode_manifest(manifest: &SyncBlobManifest) -> Result<CanonicalBlobManifest, LixError> {
    if manifest.chunks.len() > MAX_SYNC_BLOB_CHUNKS {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync blob manifest exceeds {MAX_SYNC_BLOB_CHUNKS} chunks"),
        ));
    }
    let chunks = manifest
        .chunks
        .iter()
        .map(|chunk| {
            Ok(BlobChunkReceipt {
                hash: ChunkHash::from_hex(&chunk.chunk_id)?,
                size_bytes: chunk.size_bytes,
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    Ok(CanonicalBlobManifest {
        blob_id: BlobId::from_hex(&manifest.blob_id)?,
        size_bytes: manifest.size_bytes,
        chunks,
    })
}

fn encode_manifest(
    blob_id: BlobId,
    chunks: &[CanonicalBlobChunk],
) -> Result<SyncBlobManifest, LixError> {
    let size_bytes = chunks.iter().try_fold(0_u64, |size, chunk| {
        size.checked_add(chunk.receipt.size_bytes)
            .ok_or_else(|| LixError::unknown("canonical sync blob size overflow"))
    })?;
    Ok(SyncBlobManifest {
        blob_id: blob_id.to_hex(),
        size_bytes,
        chunks: chunks
            .iter()
            .map(|chunk| SyncBlobChunk {
                chunk_id: chunk.receipt.hash.to_hex(),
                size_bytes: chunk.receipt.size_bytes,
            })
            .collect(),
        inline_bytes_base64: (size_bytes <= MAX_INLINE_SYNC_BLOB_BYTES as u64 && chunks.len() == 1)
            .then(|| base64::engine::general_purpose::STANDARD.encode(&chunks[0].bytes)),
    })
}

fn decode_inline_bytes(wire: &SyncBlobManifest) -> Result<Option<Vec<u8>>, LixError> {
    let Some(encoded) = wire.inline_bytes_base64.as_deref() else {
        return Ok(None);
    };
    if encoded.len() > MAX_INLINE_SYNC_BLOB_BYTES.div_ceil(3) * 4 {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync inline blob exceeds {MAX_INLINE_SYNC_BLOB_BYTES} bytes"),
        ));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("sync inline blob is not valid base64: {error}"),
            )
        })?;
    if bytes.len() > MAX_INLINE_SYNC_BLOB_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync inline blob exceeds {MAX_INLINE_SYNC_BLOB_BYTES} bytes"),
        ));
    }
    Ok(Some(bytes))
}

impl<StorageImpl> Lix<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Checks only authenticated manifest metadata. Unlike the outbound
    /// transfer accessor, this never materializes the blob or requires its
    /// chunks to be present.
    pub(crate) async fn has_sync_blob_manifest(&self, blob_id: &str) -> Result<bool, LixError> {
        let blob_id = BlobId::from_hex(blob_id)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        Ok(load_metadata_many(&read, &[blob_id])
            .await?
            .into_vec()
            .into_iter()
            .next()
            .flatten()
            .is_some())
    }

    /// Returns a canonical flat manifest and ensures all chunks it names can
    /// subsequently be fetched, flattening a delta-backed physical layout on
    /// first transfer.
    pub(crate) async fn get_sync_blob_manifest(
        &self,
        blob_id: &str,
    ) -> Result<Option<SyncBlobManifest>, LixError> {
        let blob_id = BlobId::from_hex(blob_id)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let Some(chunks) = load_canonical_blob_chunks(&read, blob_id).await? else {
            return Ok(None);
        };
        // `load_canonical_blob_chunks` has already materialized, rechunked,
        // and authenticated the complete blob. Derive its manifest from those
        // receipts instead of loading and materializing the blob a second time.
        let manifest = encode_manifest(blob_id, &chunks)?;
        let present = chunk_presence_many(
            &read,
            &chunks
                .iter()
                .map(|chunk| chunk.receipt.hash)
                .collect::<Vec<_>>(),
        )
        .await?;
        if present.iter().any(|present| !present) {
            let mut writes = adapter.new_write_set();
            let mut preconditions = Vec::new();
            for (chunk, present) in chunks.iter().zip(present) {
                if !present {
                    stage_verified_raw_chunk(&mut writes, chunk.receipt.hash, &chunk.bytes)?;
                }
            }
            stage_transfer_publication_fence(&read, &mut writes, &mut preconditions).await?;
            drop(read);
            adapter
                .commit_write_set(
                    writes,
                    StorageWriteOptions {
                        preconditions,
                        await_durable: true,
                        ..StorageWriteOptions::default()
                    },
                )
                .await?;
        }
        Ok(Some(manifest))
    }

    pub(crate) async fn get_sync_chunk(&self, chunk_id: &str) -> Result<Option<Vec<u8>>, LixError> {
        let chunk_id = ChunkHash::from_hex(chunk_id)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        load_verified_chunk(&read, chunk_id).await
    }

    pub(crate) async fn put_sync_chunk(
        &self,
        chunk_id: &str,
        bytes: &[u8],
    ) -> Result<(), LixError> {
        let chunk_id = ChunkHash::from_hex(chunk_id)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let mut writes = adapter.new_write_set();
        let mut preconditions = Vec::new();
        stage_verified_raw_chunk(&mut writes, chunk_id, bytes)?;
        stage_transfer_publication_fence(&read, &mut writes, &mut preconditions).await?;
        drop(read);
        adapter
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..StorageWriteOptions::default()
                },
            )
            .await?;
        Ok(())
    }

    /// Authority-side manifest registration. The manifest becomes readable
    /// only after every referenced chunk is already present.
    pub(crate) async fn register_sync_blob_manifest(
        &self,
        wire: &SyncBlobManifest,
    ) -> Result<SyncBlobRegistration, LixError> {
        if wire.inline_bytes_base64.is_some() {
            return self.register_deferred_sync_blob_manifest(wire).await;
        }
        let manifest = decode_manifest(wire)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let presence = chunk_presence_many(
            &read,
            &manifest
                .chunks
                .iter()
                .map(|chunk| chunk.hash)
                .collect::<Vec<_>>(),
        )
        .await?;
        let missing_chunk_ids = manifest
            .chunks
            .iter()
            .zip(presence)
            .filter_map(|(chunk, present)| (!present).then(|| chunk.hash.to_hex()))
            .collect::<Vec<_>>();
        if !missing_chunk_ids.is_empty() {
            return Ok(SyncBlobRegistration { missing_chunk_ids });
        }
        let mut writes = adapter.new_write_set();
        let mut preconditions = Vec::new();
        stage_verified_canonical_manifest(&read, &mut writes, &manifest).await?;
        stage_transfer_publication_fence(&read, &mut writes, &mut preconditions).await?;
        drop(read);
        adapter
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..StorageWriteOptions::default()
                },
            )
            .await?;
        Ok(SyncBlobRegistration {
            missing_chunk_ids: Vec::new(),
        })
    }

    /// Durably registers a validated canonical manifest immediately, marking
    /// absent chunks for lazy hydration in the same atomic publication.
    ///
    /// Missing chunks do not roll back the manifest: reads name them with
    /// `LIX_SYNC_CHUNKS_REQUIRED`, and each later [`Self::put_sync_chunk`]
    /// clears its marker atomically with the payload.
    pub(crate) async fn register_deferred_sync_blob_manifest(
        &self,
        wire: &SyncBlobManifest,
    ) -> Result<SyncBlobRegistration, LixError> {
        let manifest = decode_manifest(wire)?;
        let inline = decode_inline_bytes(wire)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let mut writes = adapter.new_write_set();
        let mut preconditions = Vec::new();
        let missing_chunk_ids = if let Some(bytes) = inline {
            stage_verified_inline_canonical_blob(&mut writes, &manifest, &bytes)?;
            Vec::new()
        } else {
            stage_deferred_canonical_manifest(&read, &mut writes, &manifest)
                .await?
                .into_iter()
                .map(|chunk| chunk.to_hex())
                .collect::<Vec<_>>()
        };
        stage_transfer_publication_fence(&read, &mut writes, &mut preconditions).await?;
        drop(read);
        adapter
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..StorageWriteOptions::default()
                },
            )
            .await?;
        Ok(SyncBlobRegistration { missing_chunk_ids })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::StorageReadOptions;

    fn wire_manifest(
        manifest: CanonicalBlobManifest,
        inline_bytes: Option<&[u8]>,
    ) -> SyncBlobManifest {
        SyncBlobManifest {
            blob_id: manifest.blob_id.to_hex(),
            size_bytes: manifest.size_bytes,
            chunks: manifest
                .chunks
                .into_iter()
                .map(|chunk| SyncBlobChunk {
                    chunk_id: chunk.hash.to_hex(),
                    size_bytes: chunk.size_bytes,
                })
                .collect(),
            inline_bytes_base64: inline_bytes
                .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes)),
        }
    }

    #[test]
    fn canonical_chunk_materialization_already_contains_the_complete_manifest() {
        let first_bytes = b"first canonical transfer chunk".to_vec();
        let second_bytes = b"second canonical transfer chunk".to_vec();
        let chunks = vec![
            CanonicalBlobChunk {
                receipt: BlobChunkReceipt {
                    hash: ChunkHash::from_content(&first_bytes),
                    size_bytes: first_bytes.len() as u64,
                },
                bytes: first_bytes,
            },
            CanonicalBlobChunk {
                receipt: BlobChunkReceipt {
                    hash: ChunkHash::from_content(&second_bytes),
                    size_bytes: second_bytes.len() as u64,
                },
                bytes: second_bytes,
            },
        ];
        let expected_size = chunks
            .iter()
            .map(|chunk| chunk.receipt.size_bytes)
            .sum::<u64>();
        let blob_id = BlobId::from_chunks(
            expected_size,
            chunks
                .iter()
                .map(|chunk| (chunk.receipt.hash, chunk.receipt.size_bytes)),
        );

        let manifest = encode_manifest(blob_id, &chunks)
            .expect("canonical chunks encode without another blob read");

        assert_eq!(manifest.blob_id, blob_id.to_hex());
        assert_eq!(manifest.size_bytes, expected_size);
        assert_eq!(
            manifest.chunks,
            chunks
                .iter()
                .map(|chunk| SyncBlobChunk {
                    chunk_id: chunk.receipt.hash.to_hex(),
                    size_bytes: chunk.receipt.size_bytes,
                })
                .collect::<Vec<_>>()
        );
        assert!(manifest.inline_bytes_base64.is_none());
    }

    #[tokio::test]
    async fn inline_manifest_registers_small_payload_without_chunk_demand() {
        let lix = crate::open_lix()
            .await
            .expect("test repository should open");
        let bytes = b"small realtime markdown payload".to_vec();
        let canonical = CanonicalBlobManifest::from_bytes(&bytes);
        assert_eq!(canonical.chunks.len(), 1);
        let manifest = wire_manifest(canonical.clone(), Some(&bytes));

        let deferred = wire_manifest(canonical.clone(), None);
        let registration = lix
            .register_deferred_sync_blob_manifest(&deferred)
            .await
            .expect("manifest-only registration should stage demand");
        assert_eq!(registration.missing_chunk_ids.len(), 1);
        let registration = lix
            .register_deferred_sync_blob_manifest(&manifest)
            .await
            .expect("inline registration should satisfy staged demand");
        assert!(registration.missing_chunk_ids.is_empty());
        let registration = lix
            .register_deferred_sync_blob_manifest(&manifest)
            .await
            .expect("repeated inline registration should be idempotent");
        assert!(registration.missing_chunk_ids.is_empty());

        let adapter = lix.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verification read should open");
        let loaded = crate::binary_cas::load_bytes_many(&read, &[canonical.blob_id])
            .await
            .expect("inline payload should already be readable")
            .into_vec();
        assert_eq!(loaded, vec![Some(bytes.clone())]);

        let authority = crate::open_lix()
            .await
            .expect("authority repository should open");
        let registration = authority
            .register_sync_blob_manifest(&manifest)
            .await
            .expect("authority should accept a self-contained manifest");
        assert!(registration.missing_chunk_ids.is_empty());
        let registration = authority
            .register_sync_blob_manifest(&manifest)
            .await
            .expect("authority inline registration should be idempotent");
        assert!(registration.missing_chunk_ids.is_empty());
        let read = authority
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("authority verification read should open");
        let loaded = crate::binary_cas::load_bytes_many(&read, &[canonical.blob_id])
            .await
            .expect("authority inline payload should already be readable")
            .into_vec();
        assert_eq!(loaded, vec![Some(bytes)]);
    }

    #[tokio::test]
    async fn inline_manifest_rejects_tampered_payload() {
        let lix = crate::open_lix()
            .await
            .expect("test repository should open");
        let bytes = b"authenticated inline payload".to_vec();
        let canonical = CanonicalBlobManifest::from_bytes(&bytes);
        let manifest = wire_manifest(canonical, Some(b"xuthenticated inline payload"));

        let error = lix
            .register_sync_blob_manifest(&manifest)
            .await
            .expect_err("tampered inline payload must fail");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }

    #[tokio::test]
    async fn inline_manifest_enforces_the_sixty_four_kibibyte_cap() {
        let lix = crate::open_lix()
            .await
            .expect("test repository should open");
        let at_limit = vec![7; MAX_INLINE_SYNC_BLOB_BYTES];
        let canonical = CanonicalBlobManifest::from_bytes(&at_limit);
        assert_eq!(canonical.chunks.len(), 1);
        let manifest = wire_manifest(canonical, Some(&at_limit));
        lix.register_sync_blob_manifest(&manifest)
            .await
            .expect("an exact-cap one-chunk payload should register inline");

        let over_limit = vec![7; MAX_INLINE_SYNC_BLOB_BYTES + 1];
        let canonical = CanonicalBlobManifest::from_bytes(&over_limit);
        assert_eq!(canonical.chunks.len(), 1);
        let manifest = wire_manifest(canonical, Some(&over_limit));
        let error = lix
            .register_sync_blob_manifest(&manifest)
            .await
            .expect_err("an oversized inline payload must fail");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }

    #[tokio::test]
    async fn deferred_registration_publishes_manifest_and_demand_without_payload_rows() {
        let lix = crate::open_lix()
            .await
            .expect("test repository should open");
        let bytes = (0..5 * 1024 * 1024 + 19)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let canonical = CanonicalBlobManifest::from_bytes(&bytes);
        assert!(canonical.chunks.len() > 1);
        let manifest = wire_manifest(canonical.clone(), None);

        let registration = lix
            .register_deferred_sync_blob_manifest(&manifest)
            .await
            .expect("deferred registration should commit");
        let expected_missing = canonical
            .chunks
            .iter()
            .map(|chunk| chunk.hash.to_hex())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(registration.missing_chunk_ids, expected_missing);

        let adapter = lix.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verification read should open");
        for chunk in &canonical.chunks {
            assert!(
                load_verified_chunk(&read, chunk.hash)
                    .await
                    .expect("chunk lookup should succeed")
                    .is_none()
            );
        }
        let error = crate::binary_cas::load_bytes_many(&read, &[canonical.blob_id])
            .await
            .expect_err("read must demand absent payloads");
        assert_eq!(error.code, "LIX_SYNC_CHUNKS_REQUIRED");
        assert_eq!(
            error.details.unwrap()["chunkIds"],
            serde_json::json!(registration.missing_chunk_ids)
        );
    }
}
