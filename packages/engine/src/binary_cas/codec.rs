use crate::LixError;

const ZSTD_MIN_CHUNK_BYTES: usize = 32 * 1024;
const BINARY_CHUNK_CODEC_RAW: &str = "raw";
const BINARY_CHUNK_CODEC_ZSTD: &str = "zstd";

#[derive(Debug, Clone)]
pub(crate) struct EncodedBinaryChunkPayload {
    pub(crate) codec: &'static str,
    pub(crate) codec_dict_id: Option<String>,
    pub(crate) data: Vec<u8>,
}

pub(crate) fn binary_blob_hash_hex(data: &[u8]) -> String {
    crate::common::fingerprint::stable_content_fingerprint_hex(data)
}

pub(crate) fn encode_binary_chunk_payload(
    chunk_data: &[u8],
) -> Result<EncodedBinaryChunkPayload, LixError> {
    if chunk_data.len() < ZSTD_MIN_CHUNK_BYTES {
        return Ok(EncodedBinaryChunkPayload {
            codec: BINARY_CHUNK_CODEC_RAW,
            codec_dict_id: None,
            data: chunk_data.to_vec(),
        });
    }

    let compressed = compress_binary_chunk_payload(chunk_data)?;
    if compressed.len() < chunk_data.len() {
        return Ok(EncodedBinaryChunkPayload {
            codec: BINARY_CHUNK_CODEC_ZSTD,
            codec_dict_id: None,
            data: compressed,
        });
    }

    Ok(EncodedBinaryChunkPayload {
        codec: BINARY_CHUNK_CODEC_RAW,
        codec_dict_id: None,
        data: chunk_data.to_vec(),
    })
}

pub(crate) fn decode_binary_chunk_payload(
    chunk_data: &[u8],
    codec: Option<&str>,
    expected_chunk_size: usize,
    blob_hash: &str,
    chunk_hash: &str,
    context: &str,
) -> Result<Vec<u8>, LixError> {
    match codec {
        Some(BINARY_CHUNK_CODEC_RAW) => Ok(chunk_data.to_vec()),
        Some(BINARY_CHUNK_CODEC_ZSTD) => decode_binary_chunk_zstd_payload(
            chunk_data,
            expected_chunk_size,
            blob_hash,
            chunk_hash,
            context,
        ),
        Some(other) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "{context}: unsupported chunk codec '{}' for blob hash '{}' chunk '{}'",
                other, blob_hash, chunk_hash
            ),
        }),
        None => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "{context}: missing chunk codec for blob hash '{}' chunk '{}'",
                blob_hash, chunk_hash
            ),
        }),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn compress_binary_chunk_payload(chunk_data: &[u8]) -> Result<Vec<u8>, LixError> {
    zstd::bulk::compress(chunk_data, 3).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("binary chunk compression failed: {error}"),
    })
}

#[cfg(target_arch = "wasm32")]
fn compress_binary_chunk_payload(chunk_data: &[u8]) -> Result<Vec<u8>, LixError> {
    Ok(ruzstd::encoding::compress_to_vec(
        chunk_data,
        ruzstd::encoding::CompressionLevel::Fastest,
    ))
}

#[cfg(not(target_arch = "wasm32"))]
fn decode_binary_chunk_zstd_payload(
    compressed_payload: &[u8],
    expected_chunk_size: usize,
    blob_hash: &str,
    chunk_hash: &str,
    context: &str,
) -> Result<Vec<u8>, LixError> {
    zstd::bulk::decompress(compressed_payload, expected_chunk_size).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "{context}: chunk decompression failed for blob hash '{}' chunk '{}': {error}",
            blob_hash, chunk_hash
        ),
    })
}

#[cfg(target_arch = "wasm32")]
fn decode_binary_chunk_zstd_payload(
    compressed_payload: &[u8],
    _expected_chunk_size: usize,
    blob_hash: &str,
    chunk_hash: &str,
    context: &str,
) -> Result<Vec<u8>, LixError> {
    use std::io::Read as _;

    let mut decoder =
        ruzstd::decoding::StreamingDecoder::new(compressed_payload).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "{context}: chunk decompression failed for blob hash '{}' chunk '{}': {error}",
                blob_hash, chunk_hash
            ),
        })?;

    let mut output = Vec::new();
    decoder.read_to_end(&mut output).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "{context}: chunk decompression failed for blob hash '{}' chunk '{}': {error}",
            blob_hash, chunk_hash
        ),
    })?;
    Ok(output)
}
