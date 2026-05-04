use crate::LixError;

// Binary CAS physical rows:
// - manifest:       BCM2 | kind:u8 | blob_size:u64 | kind payload
//   - empty payload:   []
//   - single payload:  chunk_hash:[u8;32]
//   - chunked payload: chunk_count:u32
// - manifest chunk: BCC1 | chunk_hash:[u8;32] | uncompressed_len:u64
// - chunk:          BCK1 | codec:u8 | uncompressed_len:u64 | payload:[u8]
const MANIFEST_MAGIC: &[u8; 4] = b"BCM2";
const MANIFEST_CHUNK_MAGIC: &[u8; 4] = b"BCC1";
const CHUNK_MAGIC: &[u8; 4] = b"BCK1";
const MANIFEST_KIND_EMPTY: u8 = 0;
const MANIFEST_KIND_SINGLE_CHUNK: u8 = 1;
const MANIFEST_KIND_CHUNKED: u8 = 2;
const CHUNK_CODEC_RAW_TAG: u8 = 0;
const HASH_BYTES: usize = 32;
const MANIFEST_HEADER_BYTES: usize = 4 + 1 + 8;
const EMPTY_MANIFEST_BYTES: usize = MANIFEST_HEADER_BYTES;
const SINGLE_CHUNK_MANIFEST_BYTES: usize = MANIFEST_HEADER_BYTES + HASH_BYTES;
const CHUNKED_MANIFEST_BYTES: usize = MANIFEST_HEADER_BYTES + 4;
const MANIFEST_CHUNK_BYTES: usize = 4 + HASH_BYTES + 8;
const CHUNK_HEADER_BYTES: usize = 4 + 1 + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BinaryChunkCodec {
    Raw,
}

impl BinaryChunkCodec {
    fn tag(self) -> u8 {
        match self {
            Self::Raw => CHUNK_CODEC_RAW_TAG,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, LixError> {
        match tag {
            CHUNK_CODEC_RAW_TAG => Ok(Self::Raw),
            other => Err(codec_error(format!(
                "unsupported binary CAS chunk codec tag {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EncodedBinaryChunkPayload {
    pub(crate) codec: BinaryChunkCodec,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

impl BinaryCasManifest {
    pub(crate) fn size_bytes(&self) -> u64 {
        match self {
            Self::Empty { size_bytes }
            | Self::SingleChunk { size_bytes, .. }
            | Self::Chunked { size_bytes, .. } => *size_bytes,
        }
    }
}

pub(crate) fn binary_blob_hash_hex(data: &[u8]) -> String {
    crate::common::stable_content_fingerprint_hex(data)
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
    let capacity = match manifest {
        BinaryCasManifest::Empty { .. } => EMPTY_MANIFEST_BYTES,
        BinaryCasManifest::SingleChunk { .. } => SINGLE_CHUNK_MANIFEST_BYTES,
        BinaryCasManifest::Chunked { .. } => CHUNKED_MANIFEST_BYTES,
    };
    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(MANIFEST_MAGIC);
    match manifest {
        BinaryCasManifest::Empty { size_bytes } => {
            out.push(MANIFEST_KIND_EMPTY);
            out.extend_from_slice(&size_bytes.to_be_bytes());
        }
        BinaryCasManifest::SingleChunk {
            size_bytes,
            chunk_hash,
        } => {
            out.push(MANIFEST_KIND_SINGLE_CHUNK);
            out.extend_from_slice(&size_bytes.to_be_bytes());
            out.extend_from_slice(chunk_hash);
        }
        BinaryCasManifest::Chunked {
            size_bytes,
            chunk_count,
        } => {
            out.push(MANIFEST_KIND_CHUNKED);
            out.extend_from_slice(&size_bytes.to_be_bytes());
            out.extend_from_slice(&chunk_count.to_be_bytes());
        }
    }
    out
}

pub(crate) fn decode_binary_cas_manifest(bytes: &[u8]) -> Result<BinaryCasManifest, LixError> {
    if bytes.len() < MANIFEST_HEADER_BYTES {
        return Err(codec_error(format!(
            "binary CAS manifest must be at least {MANIFEST_HEADER_BYTES} bytes, got {}",
            bytes.len()
        )));
    }
    require_magic(bytes, MANIFEST_MAGIC, "binary CAS manifest")?;
    let size_bytes = u64::from_be_bytes(bytes[5..13].try_into().expect("fixed slice"));
    match bytes[4] {
        MANIFEST_KIND_EMPTY => {
            require_len(bytes, EMPTY_MANIFEST_BYTES, "binary CAS empty manifest")?;
            Ok(BinaryCasManifest::Empty { size_bytes })
        }
        MANIFEST_KIND_SINGLE_CHUNK => {
            require_len(
                bytes,
                SINGLE_CHUNK_MANIFEST_BYTES,
                "binary CAS single-chunk manifest",
            )?;
            let chunk_hash = bytes[13..45].try_into().expect("fixed slice");
            Ok(BinaryCasManifest::SingleChunk {
                size_bytes,
                chunk_hash,
            })
        }
        MANIFEST_KIND_CHUNKED => {
            require_len(bytes, CHUNKED_MANIFEST_BYTES, "binary CAS chunked manifest")?;
            let chunk_count = u32::from_be_bytes(bytes[13..17].try_into().expect("fixed slice"));
            Ok(BinaryCasManifest::Chunked {
                size_bytes,
                chunk_count,
            })
        }
        other => Err(codec_error(format!(
            "unsupported binary CAS manifest kind {other}"
        ))),
    }
}

pub(crate) fn encode_binary_cas_manifest_chunk(
    chunk_hash: &[u8; HASH_BYTES],
    chunk_size: u64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(MANIFEST_CHUNK_BYTES);
    out.extend_from_slice(MANIFEST_CHUNK_MAGIC);
    out.extend_from_slice(chunk_hash);
    out.extend_from_slice(&chunk_size.to_be_bytes());
    out
}

pub(crate) fn decode_binary_cas_manifest_chunk(
    bytes: &[u8],
) -> Result<([u8; HASH_BYTES], u64), LixError> {
    if bytes.len() != MANIFEST_CHUNK_BYTES {
        return Err(codec_error(format!(
            "binary CAS manifest chunk must be {MANIFEST_CHUNK_BYTES} bytes, got {}",
            bytes.len()
        )));
    }
    require_magic(bytes, MANIFEST_CHUNK_MAGIC, "binary CAS manifest chunk")?;
    let chunk_hash = bytes[4..36].try_into().expect("fixed slice");
    let chunk_size = u64::from_be_bytes(bytes[36..44].try_into().expect("fixed slice"));
    Ok((chunk_hash, chunk_size))
}

pub(crate) fn encode_binary_cas_chunk(
    codec: BinaryChunkCodec,
    uncompressed_len: u64,
    payload: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(CHUNK_HEADER_BYTES + payload.len());
    out.extend_from_slice(CHUNK_MAGIC);
    out.push(codec.tag());
    out.extend_from_slice(&uncompressed_len.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

pub(crate) fn decode_binary_cas_chunk(
    bytes: &[u8],
) -> Result<(BinaryChunkCodec, u64, &[u8]), LixError> {
    if bytes.len() < CHUNK_HEADER_BYTES {
        return Err(codec_error(format!(
            "binary CAS chunk must be at least {CHUNK_HEADER_BYTES} bytes, got {}",
            bytes.len()
        )));
    }
    require_magic(bytes, CHUNK_MAGIC, "binary CAS chunk")?;
    let codec = BinaryChunkCodec::from_tag(bytes[4])?;
    let uncompressed_len = u64::from_be_bytes(bytes[5..13].try_into().expect("fixed slice"));
    Ok((codec, uncompressed_len, &bytes[CHUNK_HEADER_BYTES..]))
}

fn require_magic(bytes: &[u8], expected: &[u8; 4], label: &str) -> Result<(), LixError> {
    if &bytes[..4] == expected {
        return Ok(());
    }
    Err(codec_error(format!(
        "{label} has unsupported binary format"
    )))
}

fn require_len(bytes: &[u8], expected: usize, label: &str) -> Result<(), LixError> {
    if bytes.len() == expected {
        return Ok(());
    }
    Err(codec_error(format!(
        "{label} must be {expected} bytes, got {}",
        bytes.len()
    )))
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

pub(crate) fn encode_binary_chunk_payload(chunk_data: &[u8]) -> EncodedBinaryChunkPayload {
    EncodedBinaryChunkPayload {
        codec: BinaryChunkCodec::Raw,
        data: chunk_data.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifests_roundtrip_fixed_binary_rows() {
        let chunk_hash = binary_blob_hash_bytes(b"chunk");
        let cases = vec![
            (
                BinaryCasManifest::Empty { size_bytes: 0 },
                EMPTY_MANIFEST_BYTES,
            ),
            (
                BinaryCasManifest::SingleChunk {
                    size_bytes: 42,
                    chunk_hash,
                },
                SINGLE_CHUNK_MANIFEST_BYTES,
            ),
            (
                BinaryCasManifest::Chunked {
                    size_bytes: 42,
                    chunk_count: 7,
                },
                CHUNKED_MANIFEST_BYTES,
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
        assert_eq!(encoded.len(), MANIFEST_CHUNK_BYTES);
        assert_eq!(
            decode_binary_cas_manifest_chunk(&encoded).unwrap(),
            (hash, 1024)
        );
    }

    #[test]
    fn chunk_roundtrips_payload_as_remaining_bytes() {
        let payload = b"hello payload";
        let encoded = encode_binary_cas_chunk(BinaryChunkCodec::Raw, payload.len() as u64, payload);
        assert_eq!(&encoded[..4], CHUNK_MAGIC);
        let (codec, uncompressed_len, decoded_payload) = decode_binary_cas_chunk(&encoded).unwrap();
        assert_eq!(codec, BinaryChunkCodec::Raw);
        assert_eq!(uncompressed_len, payload.len() as u64);
        assert_eq!(decoded_payload, payload);
    }

    #[test]
    fn wrong_magic_is_rejected() {
        let mut encoded = encode_binary_cas_manifest(&BinaryCasManifest::Empty { size_bytes: 0 });
        encoded[0] = b'X';
        let error = decode_binary_cas_manifest(&encoded).unwrap_err();
        assert!(error.message.contains("unsupported binary format"));
    }

    #[test]
    fn hex_hashes_roundtrip_to_32_byte_keys() {
        let hash_hex = binary_blob_hash_hex(b"blob");
        let hash_bytes = hash_hex_to_bytes(&hash_hex, "test").unwrap();
        assert_eq!(hash_bytes.len(), 32);
        assert_eq!(hash_bytes_to_hex(&hash_bytes), hash_hex);
    }
}
