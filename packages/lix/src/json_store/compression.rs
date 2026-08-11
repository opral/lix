use crate::LixError;
#[cfg(test)]
use crate::compression::compress_zstd_level_1;
use crate::compression::decompress_zstd;

#[cfg(test)]
pub(crate) fn compress_json_payload(json_data: &[u8]) -> Result<Vec<u8>, LixError> {
    compress_zstd_level_1(json_data).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!("json compression failed: {error}"),
        hint: None,
        details: None,
    })
}

/// One compression context and one output scratch allocation for a complete
/// JSON-store batch.
///
/// The native bulk compressor retains its zstd context across rows. Both
/// native and WASM writers place each frame in the same caller-sized scratch
/// vector, so staging never retains one compression allocation per payload.
pub(crate) struct JsonBatchCompressor {
    scratch: Vec<u8>,
    #[cfg(not(target_family = "wasm"))]
    compressor: Option<zstd::bulk::Compressor<'static>>,
    #[cfg(test)]
    compression_attempts: usize,
}

impl std::fmt::Debug for JsonBatchCompressor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JsonBatchCompressor")
            .field("scratch_len", &self.scratch.len())
            .field("scratch_capacity", &self.scratch.capacity())
            .finish_non_exhaustive()
    }
}

impl JsonBatchCompressor {
    pub(crate) fn with_max_input_len(max_input_len: usize) -> Result<Self, LixError> {
        let scratch_capacity = if max_input_len == 0 {
            0
        } else {
            json_compression_bound(max_input_len).ok_or_else(|| {
                LixError::unknown("JSON compression scratch exceeds addressable memory")
            })?
        };
        if scratch_capacity > isize::MAX.unsigned_abs() {
            return Err(LixError::unknown(
                "JSON compression scratch exceeds addressable memory",
            ));
        }

        #[cfg(not(target_family = "wasm"))]
        let compressor = if max_input_len == 0 {
            None
        } else {
            Some(
                zstd::bulk::Compressor::new(1)
                    .map_err(|error| json_compression_error(error.to_string()))?,
            )
        };

        Ok(Self {
            scratch: Vec::with_capacity(scratch_capacity),
            #[cfg(not(target_family = "wasm"))]
            compressor,
            #[cfg(test)]
            compression_attempts: 0,
        })
    }

    pub(crate) fn compress(&mut self, json_data: &[u8]) -> Result<&[u8], LixError> {
        self.scratch.clear();

        #[cfg(not(target_family = "wasm"))]
        {
            let compressor = self.compressor.as_mut().ok_or_else(|| {
                LixError::unknown("JSON batch compressor was not planned for compressed payloads")
            })?;
            let written = compressor
                .compress_to_buffer(json_data, &mut self.scratch)
                .map_err(|error| json_compression_error(error.to_string()))?;
            debug_assert_eq!(written, self.scratch.len());
        }

        #[cfg(target_family = "wasm")]
        ruzstd::encoding::compress(
            json_data,
            &mut self.scratch,
            ruzstd::encoding::CompressionLevel::Fastest,
        );

        #[cfg(test)]
        {
            self.compression_attempts += 1;
        }
        Ok(&self.scratch)
    }

    #[cfg(test)]
    pub(crate) fn scratch_allocation(&self) -> (*const u8, usize) {
        (self.scratch.as_ptr(), self.scratch.capacity())
    }

    #[cfg(test)]
    pub(crate) fn compression_attempts(&self) -> usize {
        self.compression_attempts
    }
}

#[cfg(not(target_family = "wasm"))]
fn json_compression_bound(input_len: usize) -> Option<usize> {
    let bound = zstd::zstd_safe::compress_bound(input_len);
    (bound != 0).then_some(bound)
}

#[cfg(target_family = "wasm")]
fn json_compression_bound(input_len: usize) -> Option<usize> {
    const ZSTD_SMALL_INPUT_CUTOFF: usize = 128 << 10;

    // ZSTD_COMPRESSBOUND from zstd.h. ruzstd writes standards-compatible
    // frames into a growable Vec; reserving the same single-pass bound keeps
    // that Vec on one allocation for every row in the planned batch.
    let small_input_margin = if input_len < ZSTD_SMALL_INPUT_CUTOFF {
        (ZSTD_SMALL_INPUT_CUTOFF - input_len) >> 11
    } else {
        0
    };
    input_len
        .checked_add(input_len >> 8)?
        .checked_add(small_input_margin)
}

#[cfg(not(target_family = "wasm"))]
fn json_compression_error(error: String) -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!("json compression failed: {error}"),
        hint: None,
        details: None,
    }
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
