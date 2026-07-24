use std::time::Duration;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use lix_sdk::{Blob, RequestBlobSpliceProvenance, VerifiedRequestBlob};
use sha2::{Digest as _, Sha256};

const TEN_MIB_JSON_BYTES: usize = 10 * 1024 * 1024;

fn request_blob_transport(c: &mut Criterion) {
    let base = ten_mib_json();
    let offset = base.len() / 2;
    let insert: Blob = b"b".as_slice().into();
    let mut expected = base.to_vec();
    expected[offset] = b'b';
    let expected: Blob = expected.into();
    let base_sha256 = sha256_hex(&base);
    let result_sha256 = sha256_hex(&expected);
    let suffix = base.len() - offset - insert.len();
    let verified_base = VerifiedRequestBlob::verify(base.clone());

    let mut group = c.benchmark_group("request_blob_transport/10_mib_json_one_byte_edit");
    group.throughput(Throughput::Bytes(TEN_MIB_JSON_BYTES as u64));
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));

    group.bench_function(
        "previous_reconstruct_rehash_validate_and_cache_copy",
        |bench| {
            bench.iter(|| {
                let mut reconstructed = Vec::with_capacity(base.len());
                reconstructed.extend_from_slice(&base[..offset]);
                reconstructed.extend_from_slice(&insert);
                reconstructed.extend_from_slice(&base[base.len() - suffix..]);
                let reconstructed: Blob = reconstructed.into();
                let actual_sha256 = sha256_hex(&reconstructed);
                assert_eq!(actual_sha256, result_sha256);
                let provenance = RequestBlobSpliceProvenance::new_validated(
                    &base,
                    &reconstructed,
                    &base_sha256,
                    &result_sha256,
                    offset,
                    suffix,
                    insert.to_vec(),
                )
                .expect("baseline splice should validate");
                let successor_cache_copy: Blob = reconstructed.to_vec().into();
                black_box((reconstructed, provenance, successor_cache_copy));
            });
        },
    );

    group.bench_function("verified_reconstruct_hash_and_share", |bench| {
        bench.iter(|| {
            let (successor, provenance) = verified_base
                .reconstruct_splice(
                    verified_base.sha256(),
                    &result_sha256,
                    offset,
                    suffix,
                    insert.clone(),
                )
                .expect("verified splice should validate");
            black_box((successor, provenance));
        });
    });
    group.finish();
}

fn ten_mib_json() -> Blob {
    const PREFIX: &[u8] = b"{\"payload\":\"";
    const SUFFIX: &[u8] = b"\"}";
    let payload_len = TEN_MIB_JSON_BYTES - PREFIX.len() - SUFFIX.len();
    let mut json = Vec::with_capacity(TEN_MIB_JSON_BYTES);
    json.extend_from_slice(PREFIX);
    json.extend(std::iter::repeat_n(b'a', payload_len));
    json.extend_from_slice(SUFFIX);
    debug_assert_eq!(json.len(), TEN_MIB_JSON_BYTES);
    json.into()
}

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

criterion_group!(benches, request_blob_transport);
criterion_main!(benches);
