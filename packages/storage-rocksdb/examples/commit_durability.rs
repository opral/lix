//! Compare acknowledgement latency for a small sequential SQL write workload.
//! Run with `cargo run -p lix-storage-rocksdb --example commit_durability --release`.
//! This measures the current machine, not physical power-loss survival.
use lix::{Durability, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    futures_lite::future::block_on(async {
        for durability in [Durability::Buffered, Durability::Durable] {
            let directory = tempfile::tempdir()?;
            let storage = RocksDB::open(directory.path().join("repository"))?;
            let lix = open_lix()
                .with_storage(storage)
                .with_durability(durability)
                .await?;
            let mut samples = Vec::new();
            for index in 0..220 {
                let started = Instant::now();
                lix.execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ($1, 'true'::jsonb)",
                    &[Value::Text(format!("write-{index}"))],
                )
                .await?;
                if index >= 20 {
                    samples.push(started.elapsed());
                }
            }
            samples.sort();
            println!(
                "{durability:?}: 200 sequential single-row commits; median={}us p95={}us",
                samples[100].as_micros(),
                samples[189].as_micros(),
            );
            lix.close().await?;
        }
        Ok(())
    })
}
