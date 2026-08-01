pub(super) const SINGLE_CHUNK_FAST_PATH_MAX_BYTES: usize = 64 * 1024;
pub(super) const MAX_BINARY_CAS_CHUNK_BYTES: usize = 4096 * 1024;
pub(crate) const MEDIA_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BinaryCasChunking {
    chunk_bytes: usize,
    single_chunk_fast_path_max_bytes: usize,
}

impl BinaryCasChunking {
    pub(crate) const fn fixed_1m_v3() -> Self {
        Self {
            chunk_bytes: MEDIA_CHUNK_BYTES,
            single_chunk_fast_path_max_bytes: SINGLE_CHUNK_FAST_PATH_MAX_BYTES,
        }
    }
}

impl Default for BinaryCasChunking {
    fn default() -> Self {
        Self::fixed_1m_v3()
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

    (0..data.len())
        .step_by(chunking.chunk_bytes)
        .map(|start| {
            (
                start,
                start.saturating_add(chunking.chunk_bytes).min(data.len()),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_chunking_is_fixed_1m_profile() {
        let chunking = BinaryCasChunking::default();

        assert_eq!(chunking.chunk_bytes, MEDIA_CHUNK_BYTES);
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
            chunk_bytes: 64 * 1024,
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
                .all(|(start, end)| end - start <= MEDIA_CHUNK_BYTES)
        );
    }

    #[test]
    fn media_chunks_have_stable_offsets() {
        let data = vec![0; MEDIA_CHUNK_BYTES * 2 + 7];
        assert_eq!(
            fastcdc_chunk_ranges(&data),
            vec![
                (0, MEDIA_CHUNK_BYTES),
                (MEDIA_CHUNK_BYTES, MEDIA_CHUNK_BYTES * 2),
                (MEDIA_CHUNK_BYTES * 2, MEDIA_CHUNK_BYTES * 2 + 7),
            ]
        );
    }
}
