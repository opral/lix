#[cfg(not(target_family = "wasm"))]
pub(crate) fn compress_zstd_level_1(data: &[u8]) -> Result<Vec<u8>, String> {
    zstd::bulk::compress(data, 1).map_err(|error| error.to_string())
}

#[cfg(target_family = "wasm")]
#[expect(
    clippy::unnecessary_wraps,
    reason = "keep the native and WASM compression APIs identical"
)]
pub(crate) fn compress_zstd_level_1(data: &[u8]) -> Result<Vec<u8>, String> {
    Ok(ruzstd::encoding::compress_to_vec(
        data,
        ruzstd::encoding::CompressionLevel::Fastest,
    ))
}

#[cfg(not(target_family = "wasm"))]
pub(crate) fn decompress_zstd(
    compressed_payload: &[u8],
    uncompressed_len: usize,
) -> Result<Vec<u8>, String> {
    zstd::bulk::decompress(compressed_payload, uncompressed_len).map_err(|error| error.to_string())
}

#[cfg(target_family = "wasm")]
pub(crate) fn decompress_zstd(
    compressed_payload: &[u8],
    uncompressed_len: usize,
) -> Result<Vec<u8>, String> {
    use std::io::Read as _;

    validate_zstd_frame_limits(compressed_payload, uncompressed_len)?;
    let decoder = ruzstd::decoding::StreamingDecoder::new(compressed_payload)
        .map_err(|error| error.to_string())?;
    let output_limit = u64::try_from(uncompressed_len)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut limited = decoder.take(output_limit);
    let mut output = Vec::new();
    limited
        .read_to_end(&mut output)
        .map_err(|error| error.to_string())?;
    Ok(output)
}

#[cfg(any(test, target_family = "wasm"))]
fn validate_zstd_frame_limits(
    compressed_payload: &[u8],
    uncompressed_len: usize,
) -> Result<(), String> {
    const MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
    const MAX_BLOCK_BYTES: u64 = 128 * 1024;

    if compressed_payload.get(..MAGIC.len()) != Some(MAGIC.as_slice()) {
        return Err("invalid zstd frame magic".to_string());
    }
    let descriptor = *compressed_payload
        .get(MAGIC.len())
        .ok_or_else(|| "truncated zstd frame descriptor".to_string())?;
    if descriptor & 0x18 != 0 {
        return Err("invalid zstd frame descriptor".to_string());
    }

    let single_segment = descriptor & 0x20 != 0;
    let mut cursor = MAGIC.len() + 1;
    let window_size = if single_segment {
        None
    } else {
        let window_descriptor = *compressed_payload
            .get(cursor)
            .ok_or_else(|| "truncated zstd window descriptor".to_string())?;
        cursor += 1;
        let exponent = window_descriptor >> 3;
        let mantissa = window_descriptor & 0x07;
        let window_base = 1_u64 << (10 + u32::from(exponent));
        Some(window_base + (window_base / 8) * u64::from(mantissa))
    };

    let dictionary_id_bytes = match descriptor & 0x03 {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => 4,
        _ => unreachable!(),
    };
    cursor = cursor
        .checked_add(dictionary_id_bytes)
        .ok_or_else(|| "zstd frame header length overflow".to_string())?;

    let frame_content_size_bytes = match descriptor >> 6 {
        0 if single_segment => 1,
        0 => 0,
        1 => 2,
        2 => 4,
        3 => 8,
        _ => unreachable!(),
    };
    let frame_content_size = if frame_content_size_bytes == 0 {
        None
    } else {
        let end = cursor
            .checked_add(frame_content_size_bytes)
            .ok_or_else(|| "zstd frame header length overflow".to_string())?;
        let encoded = compressed_payload
            .get(cursor..end)
            .ok_or_else(|| "truncated zstd frame content size".to_string())?;
        let mut bytes = [0_u8; 8];
        bytes[..encoded.len()].copy_from_slice(encoded);
        let mut decoded = u64::from_le_bytes(bytes);
        if frame_content_size_bytes == 2 {
            decoded += 256;
        }
        Some(decoded)
    };

    let expected = u64::try_from(uncompressed_len).unwrap_or(u64::MAX);
    if let Some(frame_content_size) = frame_content_size
        && frame_content_size != expected
    {
        return Err(format!(
            "zstd frame content size {frame_content_size} does not match expected {expected}"
        ));
    }
    let window_size = window_size.or(frame_content_size).unwrap_or_default();
    let maximum_window_size = expected.max(MAX_BLOCK_BYTES);
    if window_size > maximum_window_size {
        return Err(format!(
            "zstd frame window size {window_size} exceeds limit {maximum_window_size}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_zstd_frames_fit_the_wasm_decoder_limits() {
        for size in [512, 128 * 1024, 4 * 1024 * 1024] {
            let data = vec![b'a'; size];
            let compressed = compress_zstd_level_1(&data).expect("test frame should compress");

            validate_zstd_frame_limits(&compressed, data.len())
                .expect("writer frame should fit the WASM decoder limits");
        }
    }

    #[test]
    fn wasm_rejects_zstd_frames_with_oversized_history_windows() {
        let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00];
        frame.push(14 << 3); // 16 MiB window.

        let error = validate_zstd_frame_limits(&frame, 4 * 1024 * 1024)
            .expect_err("oversized zstd window should fail before decoder allocation");

        assert!(error.contains("window size"));
        assert!(error.contains("exceeds limit"));
    }

    #[test]
    fn wasm_rejects_zstd_frames_with_wrong_content_size() {
        let frame = [0x28, 0xb5, 0x2f, 0xfd, 0x20, 5];

        let error = validate_zstd_frame_limits(&frame, 4)
            .expect_err("mismatched zstd content size should fail");

        assert!(error.contains("content size 5"));
        assert!(error.contains("expected 4"));
    }
}
