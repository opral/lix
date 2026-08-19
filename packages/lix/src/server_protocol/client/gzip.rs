//! Optional gzip for large JSON request bodies.

use flate2::{Compression, write::GzEncoder};
use std::io::Write as _;

const MIN_COMPRESSIBLE_JSON_BYTES: usize = 32 * 1024;
const COMPRESSION_SAMPLE_BYTES: usize = 32 * 1024;
const MAX_COMPRESSION_SAMPLE_RATIO: f64 = 0.7;
const MAX_COMPRESSED_BODY_RATIO: f64 = 0.9;

pub(crate) fn maybe_gzip_json(body: &[u8]) -> (Vec<u8>, bool) {
    if body.len() < MIN_COMPRESSIBLE_JSON_BYTES {
        return (body.to_vec(), false);
    }
    let sample = &body[..body.len().min(COMPRESSION_SAMPLE_BYTES)];
    let compressed_sample = gzip(sample);
    if (compressed_sample.len() as f64) > (sample.len() as f64) * MAX_COMPRESSION_SAMPLE_RATIO {
        return (body.to_vec(), false);
    }
    let compressed = gzip(body);
    if (compressed.len() as f64) > (body.len() as f64) * MAX_COMPRESSED_BODY_RATIO {
        return (body.to_vec(), false);
    }
    (compressed, true)
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(1));
    encoder.write_all(bytes).expect("gzip write");
    encoder.finish().expect("gzip finish")
}
