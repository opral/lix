use crate::LixError;
use crate::compression::{compress_zstd_level_1, decompress_zstd};

pub(crate) fn compress_json_payload(json_data: &[u8]) -> Result<Vec<u8>, LixError> {
    compress_zstd_level_1(json_data).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!("json compression failed: {error}"),
        hint: None,
        details: None,
    })
}

pub(crate) fn decode_json_zstd_payload(
    compressed_payload: &[u8],
    uncompressed_len: usize,
    hash_hex: &str,
) -> Result<Vec<u8>, LixError> {
    decompress_zstd(compressed_payload, uncompressed_len).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!("json decompression failed for ref '{hash_hex}': {error}"),
        hint: None,
        details: None,
    })
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
