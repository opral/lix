const SINGLE_CHUNK_FAST_PATH_MAX_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BinaryCasChunking {
    min_chunk_bytes: usize,
    avg_chunk_bytes: usize,
    max_chunk_bytes: usize,
    single_chunk_fast_path_max_bytes: usize,
}

impl BinaryCasChunking {
    pub(crate) const fn fastcdc_1m_v1() -> Self {
        Self {
            min_chunk_bytes: 256 * 1024,
            avg_chunk_bytes: 1024 * 1024,
            max_chunk_bytes: 4096 * 1024,
            single_chunk_fast_path_max_bytes: SINGLE_CHUNK_FAST_PATH_MAX_BYTES,
        }
    }
}

impl Default for BinaryCasChunking {
    fn default() -> Self {
        Self::fastcdc_1m_v1()
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
