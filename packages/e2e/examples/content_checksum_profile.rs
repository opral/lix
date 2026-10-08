//! cargo run --manifest-path tooling/Cargo.toml -p lix_e2e --release \
//!   --example content_checksum_profile
//! LIX_PROFILE_MEMORY=1 selects the in-memory control. Emits JSON lines.
use lix::storage::Storage;
use lix::{Lix, Value, open_lix};
use sha2::{Digest, Sha256};
use std::time::Instant;

const READ: &str = "SELECT content FROM lix_file WHERE path = $1";
const HASH: &str =
    "SELECT encode(sha256(content), 'hex') AS content_sha256 FROM lix_file WHERE path = $1";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        if std::env::var_os("LIX_PROFILE_MEMORY").is_some() {
            profile(open_lix().await?, "memory").await
        } else {
            profile(
                open_lix()
                    .with_storage(lix_storage_rocksdb::RocksDB::open(directory.path())?)
                    .await?,
                "rocksdb",
            )
            .await
        }
    })
}

async fn profile<S: Storage + Clone + Send + Sync + 'static>(
    lix: Lix<S>,
    backend: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Leave room for row overhead below the fixed 64 MiB buffered-result budget.
    for size in [45 * 1024, 1024 * 1024, 16 * 1024 * 1024, 63 * 1024 * 1024] {
        // Deterministic pseudorandom bytes avoid highly compressible fixtures.
        let mut state = 0x1234_5678_u32 ^ size as u32;
        let bytes = (0..size)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state as u8
            })
            .collect::<Vec<_>>();
        let expected = format!("{:x}", Sha256::digest(&bytes));
        let path = format!("/profile-{size}.bin");
        lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
            &[Value::Text(path.clone()), Value::Blob(bytes.into())],
        )
        .await?;
        let params = [Value::Text(path)];
        let mut reads = Vec::new();
        let mut hashes = Vec::new();
        // Warm both plans and verify the digest before timing. These are warm
        // process/storage measurements, not cold disk or HTTP transfer timings.
        lix.execute(READ, &params).await?;
        assert_eq!(
            lix.execute(HASH, &params).await?.rows()[0].get::<String>("content_sha256")?,
            expected
        );
        for iteration in 0..20 {
            // Alternate order to reduce systematic cache/order bias.
            for hashing in if iteration % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                let started = Instant::now();
                let result = lix
                    .execute(if hashing { HASH } else { READ }, &params)
                    .await?;
                let elapsed = started.elapsed().as_secs_f64() * 1000.0;
                assert_eq!(result.rows().len(), 1);
                std::hint::black_box(&result);
                if hashing {
                    hashes.push(elapsed);
                } else {
                    reads.push(elapsed);
                }
            }
        }
        for (query, mut samples, result_bytes) in
            [("content_read", reads, size), ("sha256_hex", hashes, 64)]
        {
            samples.sort_by(f64::total_cmp);
            let median = (samples[9] + samples[10]) / 2.0;
            println!(
                "{}",
                serde_json::json!({"backend":backend,"query":query,"file_bytes":size,"samples":20,"median_ms":median,"p95_ms":samples[18],"result_value_bytes":result_bytes})
            );
        }
    }
    Ok(())
}
