// Values through 32 KiB are a one-row write with no existence probe. The
// 32–128 KiB band stays inline for one-phase reads, but probes the manifest key
// before staging so repeat-content writes do not rewrite the large value.
pub(super) const UNGUARDED_INLINE_BINARY_CAS_MAX_BYTES: usize = 32 * 1024;
pub(super) const INLINE_BINARY_CAS_MAX_BYTES: usize = 128 * 1024;
pub(super) const SINGLE_CHUNK_FAST_PATH_MAX_BYTES: usize = 64 * 1024;
pub(super) const MAX_BINARY_CAS_CHUNK_BYTES: usize = 4096 * 1024;
const FASTCDC_WRITE_MAX_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BinaryCasChunking {
    min_chunk_bytes: usize,
    avg_chunk_bytes: usize,
    max_chunk_bytes: usize,
    single_chunk_fast_path_max_bytes: usize,
}

impl BinaryCasChunking {
    pub(crate) const fn fastcdc_1m_v2() -> Self {
        Self {
            min_chunk_bytes: 256 * 1024,
            avg_chunk_bytes: 1024 * 1024,
            max_chunk_bytes: FASTCDC_WRITE_MAX_CHUNK_BYTES,
            single_chunk_fast_path_max_bytes: SINGLE_CHUNK_FAST_PATH_MAX_BYTES,
        }
    }
}

impl Default for BinaryCasChunking {
    fn default() -> Self {
        Self::fastcdc_1m_v2()
    }
}

#[cfg(test)]
pub(crate) fn fastcdc_chunk_ranges(data: &[u8]) -> Vec<(usize, usize)> {
    fastcdc_chunk_ranges_with_chunking(data, BinaryCasChunking::default())
}

#[expect(clippy::cast_possible_truncation)]
pub(crate) fn fastcdc_chunk_ranges_with_chunking(
    data: &[u8],
    chunking: BinaryCasChunking,
) -> Vec<(usize, usize)> {
    if data.is_empty() {
        return Vec::new();
    }
    if data.len() <= chunking.single_chunk_fast_path_max_bytes {
        return vec![(0, data.len())];
    }

    fastcdc::v2020::FastCDC::new(
        data,
        chunking.min_chunk_bytes as u32,
        chunking.avg_chunk_bytes as u32,
        chunking.max_chunk_bytes as u32,
    )
    .map(|chunk| {
        let start = chunk.offset;
        let end = start + chunk.length;
        (start, end)
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_chunking_is_fixed_fastcdc_1m_profile() {
        let chunking = BinaryCasChunking::default();

        assert_eq!(chunking.min_chunk_bytes, 256 * 1024);
        assert_eq!(chunking.avg_chunk_bytes, 1024 * 1024);
        assert_eq!(chunking.max_chunk_bytes, FASTCDC_WRITE_MAX_CHUNK_BYTES);
        assert_eq!(chunking.single_chunk_fast_path_max_bytes, 64 * 1024);
    }

    #[test]
    fn single_chunk_fast_path_applies_through_64kib() {
        let at_boundary = vec![0; 64 * 1024];
        assert_eq!(
            fastcdc_chunk_ranges(&at_boundary),
            vec![(0, at_boundary.len())]
        );
    }

    #[test]
    fn single_chunk_fast_path_does_not_apply_above_64kib() {
        let above_boundary = vec![0; 64 * 1024 + 1];
        let chunking = BinaryCasChunking {
            min_chunk_bytes: 16 * 1024,
            avg_chunk_bytes: 32 * 1024,
            max_chunk_bytes: 64 * 1024,
            single_chunk_fast_path_max_bytes: 64 * 1024,
        };

        assert_ne!(
            fastcdc_chunk_ranges_with_chunking(&above_boundary, chunking),
            vec![(0, above_boundary.len())]
        );
    }

    #[test]
    fn multi_megabyte_payloads_cannot_collapse_to_one_rewritten_chunk() {
        let payload = vec![b'a'; 3_500_000];
        let ranges = fastcdc_chunk_ranges(&payload);

        assert!(ranges.len() >= 4);
        assert!(
            ranges
                .iter()
                .all(|(start, end)| end - start <= FASTCDC_WRITE_MAX_CHUNK_BYTES)
        );
    }
}
