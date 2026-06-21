use musli::{Decode, Encode};

use super::types::BinaryCasChunkView;
use crate::LixError;
use crate::storage_codec;

const HASH_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub(crate) enum BinaryChunkCodec {
    Raw,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub(crate) enum BinaryCasManifest {
    Empty {
        size_bytes: u64,
    },
    SingleChunk {
        size_bytes: u64,
        chunk_hash: [u8; HASH_BYTES],
    },
    Chunked {
        size_bytes: u64,
        chunk_count: u32,
    },
}

#[derive(Encode, Decode)]
#[musli(packed)]
struct StorageBinaryCasManifestChunk {
    chunk_hash: [u8; HASH_BYTES],
    chunk_size: u64,
}

#[derive(Encode)]
#[musli(packed)]
struct BinaryCasChunkRef<'a> {
    codec: BinaryChunkCodec,
    uncompressed_len: u64,
    #[musli(bytes)]
    payload: &'a [u8],
}

impl BinaryCasManifest {
    pub(crate) fn size_bytes(&self) -> u64 {
        match self {
            Self::Empty { size_bytes }
            | Self::SingleChunk { size_bytes, .. }
            | Self::Chunked { size_bytes, .. } => *size_bytes,
        }
    }
}

#[cfg(test)]
pub(crate) fn binary_blob_hash_hex(data: &[u8]) -> String {
    hash_bytes_to_hex(&binary_blob_hash_bytes(data))
}

pub(crate) fn binary_blob_hash_bytes(data: &[u8]) -> [u8; HASH_BYTES] {
    *blake3::hash(data).as_bytes()
}

pub(crate) fn hash_hex_to_bytes(hash_hex: &str, label: &str) -> Result<[u8; HASH_BYTES], LixError> {
    if hash_hex.len() != HASH_BYTES * 2 {
        return Err(codec_error(format!(
            "{label} hash must be {} hex characters, got {}",
            HASH_BYTES * 2,
            hash_hex.len()
        )));
    }

    let mut out = [0u8; HASH_BYTES];
    let bytes = hash_hex.as_bytes();
    for index in 0..HASH_BYTES {
        out[index] =
            (hex_value(bytes[index * 2], label)? << 4) | hex_value(bytes[index * 2 + 1], label)?;
    }
    Ok(out)
}

pub(crate) fn hash_bytes_to_hex(bytes: &[u8; HASH_BYTES]) -> String {
    blake3::Hash::from_bytes(*bytes).to_hex().to_string()
}

pub(crate) fn encode_binary_cas_manifest(manifest: &BinaryCasManifest) -> Vec<u8> {
    storage_codec::encode("binary CAS manifest", manifest)
        .expect("binary CAS manifest storage encoding should not fail")
}

pub(crate) fn decode_binary_cas_manifest(bytes: &[u8]) -> Result<BinaryCasManifest, LixError> {
    storage_codec::decode("binary CAS manifest", bytes)
}

pub(crate) fn encode_binary_cas_manifest_chunk(
    chunk_hash: &[u8; HASH_BYTES],
    chunk_size: u64,
) -> Vec<u8> {
    storage_codec::encode(
        "binary CAS manifest chunk",
        &StorageBinaryCasManifestChunk {
            chunk_hash: *chunk_hash,
            chunk_size,
        },
    )
    .expect("binary CAS manifest chunk storage encoding should not fail")
}

pub(crate) fn decode_binary_cas_manifest_chunk(
    bytes: &[u8],
) -> Result<([u8; HASH_BYTES], u64), LixError> {
    let StorageBinaryCasManifestChunk {
        chunk_hash,
        chunk_size,
    } = storage_codec::decode("binary CAS manifest chunk", bytes)?;
    Ok((chunk_hash, chunk_size))
}

pub(crate) fn encode_binary_cas_chunk(
    codec: BinaryChunkCodec,
    uncompressed_len: u64,
    payload: &[u8],
) -> Vec<u8> {
    storage_codec::encode(
        "binary CAS chunk",
        &BinaryCasChunkRef {
            codec,
            uncompressed_len,
            payload,
        },
    )
    .expect("binary CAS chunk storage encoding should not fail")
}

pub(crate) fn decode_binary_cas_chunk(
    bytes: &[u8],
) -> Result<(BinaryChunkCodec, u64, &[u8]), LixError> {
    let BinaryCasChunkView {
        codec,
        uncompressed_len,
        payload,
    } = storage_codec::decode("binary CAS chunk", bytes)?;
    Ok((codec, uncompressed_len, payload))
}

fn hex_value(byte: u8, label: &str) -> Result<u8, LixError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(codec_error(format!("{label} hash contains non-hex bytes"))),
    }
}

fn codec_error(message: String) -> LixError {
    LixError::new("LIX_ERROR_UNKNOWN", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EMPTY_MANIFEST_STORAGE_BYTES: usize = 4;
    const SINGLE_CHUNK_MANIFEST_STORAGE_BYTES: usize = 38;
    const CHUNKED_MANIFEST_STORAGE_BYTES: usize = 6;
    const MANIFEST_CHUNK_STORAGE_BYTES: usize = 35;
    const CHUNK_STORAGE_OVERHEAD_BYTES: usize = 3;

    #[test]
    fn manifests_roundtrip_fixed_binary_rows() {
        let chunk_hash = binary_blob_hash_bytes(b"chunk");
        let cases = [
            (
                BinaryCasManifest::Empty { size_bytes: 0 },
                EMPTY_MANIFEST_STORAGE_BYTES,
            ),
            (
                BinaryCasManifest::SingleChunk {
                    size_bytes: 42,
                    chunk_hash,
                },
                SINGLE_CHUNK_MANIFEST_STORAGE_BYTES,
            ),
            (
                BinaryCasManifest::Chunked {
                    size_bytes: 42,
                    chunk_count: 7,
                },
                CHUNKED_MANIFEST_STORAGE_BYTES,
            ),
        ];
        for (manifest, expected_len) in cases {
            let encoded = encode_binary_cas_manifest(&manifest);
            assert_eq!(encoded.len(), expected_len);
            assert_eq!(decode_binary_cas_manifest(&encoded).unwrap(), manifest);
        }
    }

    #[test]
    fn manifest_chunk_roundtrips_fixed_binary_row() {
        let hash = binary_blob_hash_bytes(b"chunk");
        let encoded = encode_binary_cas_manifest_chunk(&hash, 1024);
        assert_eq!(encoded.len(), MANIFEST_CHUNK_STORAGE_BYTES);
        assert_eq!(
            decode_binary_cas_manifest_chunk(&encoded).unwrap(),
            (hash, 1024)
        );
    }

    #[test]
    fn chunk_roundtrips_payload_as_remaining_bytes() {
        let payload = b"hello payload";
        let encoded = encode_binary_cas_chunk(BinaryChunkCodec::Raw, payload.len() as u64, payload);
        assert_eq!(encoded.len(), CHUNK_STORAGE_OVERHEAD_BYTES + payload.len());
        let (codec, uncompressed_len, decoded_payload) = decode_binary_cas_chunk(&encoded).unwrap();
        assert_eq!(codec, BinaryChunkCodec::Raw);
        assert_eq!(uncompressed_len, payload.len() as u64);
        assert_eq!(decoded_payload, payload);
    }

    #[test]
    fn malformed_storage_bytes_are_rejected() {
        let mut encoded = encode_binary_cas_manifest(&BinaryCasManifest::Empty { size_bytes: 0 });
        encoded.truncate(encoded.len() - 1);
        let error = decode_binary_cas_manifest(&encoded).unwrap_err();
        assert!(
            error
                .message
                .contains("failed to decode binary CAS manifest")
        );
    }

    #[test]
    fn hex_hashes_roundtrip_to_32_byte_keys() {
        let hash_hex = binary_blob_hash_hex(b"blob");
        let hash_bytes = hash_hex_to_bytes(&hash_hex, "test").unwrap();
        assert_eq!(hash_bytes.len(), 32);
        assert_eq!(hash_bytes_to_hex(&hash_bytes), hash_hex);
    }
}
