//! The single authority for binary-CAS chunk boundaries.
//!
//! Boundaries are decided exactly once per payload, in
//! [`crate::binary_cas::BlobPayload::from_bytes`] for a whole buffer or in
//! `stage_upload_part_skipping_existing` for one resumable part, and travel
//! onward as chunk receipts. Staging reconstructs the ranges from those receipt
//! sizes rather than asking this module again, so a write never searches for the
//! same boundaries twice.
//!
//! Boundaries come from FastCDC's rolling gear hash, so an insert or a delete
//! only re-chunks the bytes around the edit: the search resynchronises within
//! roughly one chunk and every later chunk keeps the hash it already had.
//! Fixed-offset boundaries cannot do that -- shifting the payload by one byte
//! renames every chunk after the edit -- and shifts are what ordinary media
//! edits produce. Rewriting an MP4's title moves the whole payload; so does
//! trimming a clip, splicing audio, or cropping a bitmap.
//!
//! One rule qualifies the search, and it is what keeps the media ingest path
//! intact. A boundary is forced at every [`CHUNK_ANCHOR_BYTES`] offset, which is
//! the resumable upload part size. The consequences are all load-bearing:
//!
//! * The boundary search never looks outside one 16 MiB window, so chunking a
//!   20 GiB file never needs more than one part resident. Bounded upload memory
//!   survives unchanged.
//! * A part can be chunked in isolation, so parts stay independent and up to
//!   four may still complete out of order.
//! * Chunking a payload whole and chunking it part-by-part produce the identical
//!   layout, and therefore the identical blob identity.
//! * Every part-aligned offset is guaranteed to be a real chunk start, which is
//!   what a ranged read seeks to instead of computing an index by division.

use fastcdc::v2020::FastCDC;

pub(super) const SINGLE_CHUNK_FAST_PATH_MAX_BYTES: usize = 64 * 1024;
pub(super) const MAX_BINARY_CAS_CHUNK_BYTES: usize = 4096 * 1024;

/// Forced boundary interval. Must equal `FILE_UPLOAD_PART_BYTES`.
pub(crate) const CHUNK_ANCHOR_BYTES: usize = 16 * 1024 * 1024;

/// Target average chunk size. Held at the previous fixed chunk size on purpose,
/// so manifest row count, payload row count and the seek unit the movie
/// repository profile qualified all stay where they were; content-defined
/// boundaries are the only difference.
pub(crate) const MEDIA_CHUNK_BYTES: usize = 1024 * 1024;
const MIN_BINARY_CAS_CHUNK_BYTES: usize = 256 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BinaryCasChunking {
    min_bytes: usize,
    average_bytes: usize,
    max_bytes: usize,
    anchor_bytes: usize,
    single_chunk_fast_path_max_bytes: usize,
}

impl BinaryCasChunking {
    pub(super) const fn content_defined_v4() -> Self {
        Self {
            min_bytes: MIN_BINARY_CAS_CHUNK_BYTES,
            average_bytes: MEDIA_CHUNK_BYTES,
            max_bytes: MAX_BINARY_CAS_CHUNK_BYTES,
            anchor_bytes: CHUNK_ANCHOR_BYTES,
            single_chunk_fast_path_max_bytes: SINGLE_CHUNK_FAST_PATH_MAX_BYTES,
        }
    }
}

impl Default for BinaryCasChunking {
    fn default() -> Self {
        Self::content_defined_v4()
    }
}

pub(crate) fn chunk_ranges(data: &[u8]) -> Vec<(usize, usize)> {
    chunk_ranges_with_chunking(data, BinaryCasChunking::default())
}

pub(super) fn chunk_ranges_with_chunking(
    data: &[u8],
    chunking: BinaryCasChunking,
) -> Vec<(usize, usize)> {
    if data.is_empty() {
        return Vec::new();
    }
    if data.len() <= chunking.single_chunk_fast_path_max_bytes {
        return vec![(0, data.len())];
    }

    let mut out = Vec::with_capacity(data.len() / chunking.average_bytes + 2);
    let mut anchor = 0usize;
    while anchor < data.len() {
        let anchor_end = anchor.saturating_add(chunking.anchor_bytes).min(data.len());
        for chunk in FastCDC::new(
            &data[anchor..anchor_end],
            chunking.min_bytes as u32,
            chunking.average_bytes as u32,
            chunking.max_bytes as u32,
        ) {
            let start = anchor + chunk.offset;
            out.push((start, start + chunk.length));
        }
        anchor = anchor_end;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn structured_bytes(len: usize, seed: u64) -> Vec<u8> {
        let mut bytes = vec![0; len];
        let mut state = seed ^ 0xd1b5_4a32_d192_ed03;
        for chunk in bytes.chunks_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
        }
        bytes
    }

    #[test]
    fn default_chunking_targets_the_media_average() {
        let chunking = BinaryCasChunking::default();

        assert_eq!(chunking.average_bytes, MEDIA_CHUNK_BYTES);
        assert_eq!(chunking.anchor_bytes, CHUNK_ANCHOR_BYTES);
        assert_eq!(chunking.single_chunk_fast_path_max_bytes, 64 * 1024);
    }

    #[test]
    fn single_chunk_fast_path_applies_through_64kib() {
        let at_boundary = vec![0; 64 * 1024];
        assert_eq!(chunk_ranges(&at_boundary), vec![(0, at_boundary.len())]);
    }

    #[test]
    fn ranges_tile_the_payload_without_gaps_or_overlap() {
        let data = structured_bytes(37 * 1024 * 1024, 7);
        let ranges = chunk_ranges(&data);

        assert!(ranges.len() > 4);
        let mut cursor = 0;
        for (start, end) in &ranges {
            assert_eq!(*start, cursor);
            assert!(end > start);
            assert!(end - start <= MAX_BINARY_CAS_CHUNK_BYTES);
            cursor = *end;
        }
        assert_eq!(cursor, data.len());
    }

    #[test]
    fn every_anchor_offset_starts_a_chunk() {
        let data = structured_bytes(37 * 1024 * 1024, 11);
        let starts = chunk_ranges(&data)
            .into_iter()
            .map(|(start, _)| start)
            .collect::<Vec<_>>();

        for anchor in (0..data.len()).step_by(CHUNK_ANCHOR_BYTES) {
            assert!(
                starts.contains(&anchor),
                "anchor {anchor} must be a forced boundary"
            );
        }
    }

    #[test]
    fn an_insert_near_the_start_keeps_the_later_boundaries() {
        let base = structured_bytes(20 * 1024 * 1024, 3);
        let mut edited = Vec::with_capacity(base.len() + 4096);
        edited.extend_from_slice(&base[..4096]);
        edited.extend_from_slice(&structured_bytes(4096, 99));
        edited.extend_from_slice(&base[4096..]);

        let base_chunks = chunk_ranges(&base)
            .into_iter()
            .map(|(start, end)| blake3::hash(&base[start..end]))
            .collect::<std::collections::HashSet<_>>();
        let shared = chunk_ranges(&edited)
            .into_iter()
            .filter(|(start, end)| base_chunks.contains(&blake3::hash(&edited[*start..*end])))
            .map(|(start, end)| end - start)
            .sum::<usize>();

        // Fixed-offset boundaries would share nothing at all here.
        assert!(
            shared * 4 > edited.len() * 3,
            "content-defined boundaries must resynchronise after an insert, shared {shared} of {}",
            edited.len()
        );
    }

    #[test]
    fn part_sized_prefixes_chunk_the_same_way_as_the_whole_payload() {
        let data = structured_bytes(35 * 1024 * 1024, 5);
        let whole = chunk_ranges(&data);

        let mut assembled = Vec::new();
        let mut anchor = 0usize;
        while anchor < data.len() {
            let end = (anchor + CHUNK_ANCHOR_BYTES).min(data.len());
            assembled.extend(
                chunk_ranges(&data[anchor..end])
                    .into_iter()
                    .map(|(start, stop)| (anchor + start, anchor + stop)),
            );
            anchor = end;
        }

        assert_eq!(whole, assembled);
    }

    #[test]
    fn boundary_search_never_looks_past_one_upload_part() {
        // Bounded upload memory depends on this: a part's chunk layout must not
        // change when bytes outside that part change.
        let data = structured_bytes(3 * CHUNK_ANCHOR_BYTES, 13);
        let mut altered = data.clone();
        altered[2 * CHUNK_ANCHOR_BYTES..]
            .copy_from_slice(&structured_bytes(CHUNK_ANCHOR_BYTES, 71));

        let first_two = |bytes: &[u8]| {
            chunk_ranges(bytes)
                .into_iter()
                .filter(|(start, _)| *start < 2 * CHUNK_ANCHOR_BYTES)
                .collect::<Vec<_>>()
        };
        assert_eq!(first_two(&data), first_two(&altered));
    }
}
