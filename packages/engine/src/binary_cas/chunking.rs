const FASTCDC_MIN_CHUNK_BYTES: usize = 16 * 1024;
const FASTCDC_AVG_CHUNK_BYTES: usize = 64 * 1024;
const FASTCDC_MAX_CHUNK_BYTES: usize = 256 * 1024;
const SINGLE_CHUNK_FAST_PATH_MAX_BYTES: usize = 64 * 1024;

pub(crate) fn should_materialize_chunk_cas(data: &[u8]) -> bool {
    data.len() > SINGLE_CHUNK_FAST_PATH_MAX_BYTES
}

pub(crate) fn fastcdc_chunk_ranges(data: &[u8]) -> Vec<(usize, usize)> {
    if data.is_empty() {
        return Vec::new();
    }
    if data.len() <= SINGLE_CHUNK_FAST_PATH_MAX_BYTES {
        return vec![(0, data.len())];
    }

    fastcdc::v2020::FastCDC::new(
        data,
        FASTCDC_MIN_CHUNK_BYTES as u32,
        FASTCDC_AVG_CHUNK_BYTES as u32,
        FASTCDC_MAX_CHUNK_BYTES as u32,
    )
    .map(|chunk| {
        let start = chunk.offset as usize;
        let end = start + (chunk.length as usize);
        (start, end)
    })
    .collect()
}
