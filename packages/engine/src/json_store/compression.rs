use crate::LixError;

#[cfg(not(target_family = "wasm"))]
pub(crate) fn compress_json_payload(json_data: &[u8]) -> Result<Vec<u8>, LixError> {
    zstd::bulk::compress(json_data, 1).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!("json compression failed: {error}"),
        hint: None,
        details: None,
    })
}

#[cfg(target_family = "wasm")]
pub(crate) fn compress_json_payload(json_data: &[u8]) -> Result<Vec<u8>, LixError> {
    Ok(ruzstd::encoding::compress_to_vec(
        json_data,
        ruzstd::encoding::CompressionLevel::Fastest,
    ))
}

#[cfg(not(target_family = "wasm"))]
pub(crate) fn decode_json_zstd_payload(
    compressed_payload: &[u8],
    uncompressed_len: usize,
    hash_hex: &str,
) -> Result<Vec<u8>, LixError> {
    zstd::bulk::decompress(compressed_payload, uncompressed_len).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!("json decompression failed for ref '{hash_hex}': {error}"),
        hint: None,
        details: None,
    })
}

#[cfg(target_family = "wasm")]
pub(crate) fn decode_json_zstd_payload(
    compressed_payload: &[u8],
    _uncompressed_len: usize,
    _hash_hex: &str,
) -> Result<Vec<u8>, LixError> {
    use std::io::Read as _;

    let mut decoder =
        ruzstd::decoding::StreamingDecoder::new(compressed_payload).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!("json decompression failed: {error}"),
            hint: None,
            details: None,
        })?;

    let mut output = Vec::new();
    decoder.read_to_end(&mut output).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!("json decompression failed: {error}"),
        hint: None,
        details: None,
    })?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zstd_payload_roundtrips() {
        let json = "zstd-friendly text ".repeat(2048);
        let compressed = compress_json_payload(json.as_bytes()).expect("should compress");
        assert!(compressed.len() < json.len());

        let decoded =
            decode_json_zstd_payload(&compressed, json.len(), "test").expect("should decode");

        assert_eq!(decoded, json.as_bytes());
    }
}
