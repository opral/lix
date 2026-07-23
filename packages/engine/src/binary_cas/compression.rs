use std::borrow::Cow;

use crate::LixError;
#[cfg(not(target_family = "wasm"))]
use crate::compression::compress_zstd_level_1;
use crate::compression::decompress_zstd;

use super::codec::BinaryChunkCodec;
use super::types::BlobHash;

#[cfg(not(target_family = "wasm"))]
const ZSTD_MIN_CHUNK_BYTES: usize = 512;
#[cfg(not(target_family = "wasm"))]
const MIN_ZSTD_SAVINGS_BYTES: usize = 128;
// At 2 MiB, zstd-1 costs roughly 1 ms more than the storage layer's LZ4.
// Requiring 12.5% relative savings amortizes that CPU by avoiding marginal
// frames while the absolute floor protects sub-KiB chunks.
#[cfg(not(target_family = "wasm"))]
const MIN_ZSTD_SAVINGS_DIVISOR: usize = 8;

#[derive(Debug)]
pub(super) struct EncodedChunkPayload<'a> {
    pub(super) codec: BinaryChunkCodec,
    pub(super) data: Cow<'a, [u8]>,
}

#[cfg(not(target_family = "wasm"))]
pub(super) fn encode_chunk_payload(
    chunk_hash: BlobHash,
    chunk_data: &[u8],
) -> Result<EncodedChunkPayload<'_>, LixError> {
    if chunk_data.len() < ZSTD_MIN_CHUNK_BYTES {
        return Ok(raw_payload(chunk_data));
    }

    let compressed = compress_zstd_level_1(chunk_data).map_err(|error| {
        LixError::new(
            LixError::CODE_UNKNOWN,
            format!(
                "binary CAS chunk '{}' compression failed: {error}",
                chunk_hash.to_hex()
            ),
        )
    })?;
    if !compression_is_worthwhile(chunk_data.len(), compressed.len()) {
        return Ok(raw_payload(chunk_data));
    }

    Ok(EncodedChunkPayload {
        codec: BinaryChunkCodec::Zstd,
        data: Cow::Owned(compressed),
    })
}

#[cfg(target_family = "wasm")]
#[expect(
    clippy::unnecessary_wraps,
    reason = "keep the native and WASM binary CAS write paths identical"
)]
pub(super) fn encode_chunk_payload(
    _chunk_hash: BlobHash,
    chunk_data: &[u8],
) -> Result<EncodedChunkPayload<'_>, LixError> {
    // ruzstd's only implemented encoder is substantially slower and produces
    // frames close to SlateDB's existing LZ4 output for representative plugin
    // binaries. Keep browser writes raw, while retaining Zstd decode support so
    // databases written by native/server runtimes remain portable.
    Ok(raw_payload(chunk_data))
}

#[cfg(not(target_family = "wasm"))]
fn compression_is_worthwhile(uncompressed_len: usize, compressed_len: usize) -> bool {
    let minimum_savings =
        MIN_ZSTD_SAVINGS_BYTES.max(uncompressed_len.div_ceil(MIN_ZSTD_SAVINGS_DIVISOR));
    uncompressed_len.saturating_sub(compressed_len) >= minimum_savings
}

pub(super) fn decode_zstd_chunk(
    chunk_hash: BlobHash,
    compressed_payload: &[u8],
    uncompressed_len: usize,
) -> Result<Vec<u8>, LixError> {
    let decoded = decompress_zstd(compressed_payload, uncompressed_len).map_err(|error| {
        LixError::new(
            LixError::CODE_UNKNOWN,
            format!(
                "binary CAS chunk '{}' decompression failed: {error}",
                chunk_hash.to_hex()
            ),
        )
    })?;
    if decoded.len() != uncompressed_len {
        return Err(LixError::new(
            LixError::CODE_UNKNOWN,
            format!(
                "binary CAS chunk '{}' decoded to {} bytes, expected {}",
                chunk_hash.to_hex(),
                decoded.len(),
                uncompressed_len
            ),
        ));
    }
    Ok(decoded)
}

fn raw_payload(chunk_data: &[u8]) -> EncodedChunkPayload<'_> {
    EncodedChunkPayload {
        codec: BinaryChunkCodec::Raw,
        data: Cow::Borrowed(chunk_data),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deterministic_high_entropy_bytes(len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut counter = 0u64;
        while out.len() < len {
            out.extend_from_slice(blake3::hash(&counter.to_le_bytes()).as_bytes());
            counter += 1;
        }
        out.truncate(len);
        out
    }

    #[test]
    #[cfg(not(target_family = "wasm"))]
    fn compresses_repetitive_chunks_and_roundtrips() {
        let data = b"component-section:function-signature\n".repeat(4096);
        let chunk_hash = BlobHash::from_content(&data);

        let encoded = encode_chunk_payload(chunk_hash, &data).expect("chunk should encode");

        assert_eq!(encoded.codec, BinaryChunkCodec::Zstd);
        assert!(encoded.data.len() < data.len() / 4);
        assert_eq!(
            decode_zstd_chunk(chunk_hash, &encoded.data, data.len()).expect("chunk should decode"),
            data
        );
    }

    #[test]
    #[cfg(not(target_family = "wasm"))]
    fn keeps_small_and_high_entropy_chunks_raw() {
        let small = vec![b'a'; ZSTD_MIN_CHUNK_BYTES - 1];
        let small_hash = BlobHash::from_content(&small);
        let high_entropy = deterministic_high_entropy_bytes(32 * 1024);
        let high_entropy_hash = BlobHash::from_content(&high_entropy);

        let small_encoded =
            encode_chunk_payload(small_hash, &small).expect("small chunk should encode");
        let high_entropy_encoded = encode_chunk_payload(high_entropy_hash, &high_entropy)
            .expect("high-entropy chunk should encode");

        assert_eq!(small_encoded.codec, BinaryChunkCodec::Raw);
        assert_eq!(small_encoded.data.as_ref(), small);
        assert_eq!(high_entropy_encoded.codec, BinaryChunkCodec::Raw);
        assert_eq!(high_entropy_encoded.data.as_ref(), high_entropy);
    }

    #[test]
    #[cfg(not(target_family = "wasm"))]
    fn savings_policy_enforces_absolute_and_relative_floors() {
        assert!(!compression_is_worthwhile(512, 385));
        assert!(compression_is_worthwhile(512, 384));
        assert!(!compression_is_worthwhile(4096, 3585));
        assert!(compression_is_worthwhile(4096, 3584));
    }

    #[test]
    #[cfg(not(target_family = "wasm"))]
    fn rejects_truncated_zstd_payload() {
        let data = b"compressible binary CAS bytes".repeat(4096);
        let chunk_hash = BlobHash::from_content(&data);
        let encoded = encode_chunk_payload(chunk_hash, &data).expect("chunk should encode");
        assert_eq!(encoded.codec, BinaryChunkCodec::Zstd);
        let mut corrupted = encoded.data.into_owned();
        corrupted.truncate(corrupted.len() / 2);

        let error = decode_zstd_chunk(chunk_hash, &corrupted, data.len())
            .expect_err("truncated zstd should fail");

        assert!(error.message.contains("decompression failed"));
    }
}
