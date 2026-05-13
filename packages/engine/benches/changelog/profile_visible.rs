use std::hint::black_box;
use std::time::{Duration, Instant};

use lix_engine::changelog::bench as changelog_bench;
use lix_engine::LixError;

mod backends;

use backends::ChangelogBenchBackend;

fn main() {
    let mut args = std::env::args().skip(1);
    let backend = args
        .next()
        .as_deref()
        .map(parse_backend)
        .transpose()
        .expect("parse changelog visible profile backend")
        .unwrap_or(ChangelogBenchBackend::Unit);
    let seconds = args
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(15);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for changelog visible profile");

    runtime
        .block_on(run(backend, Duration::from_secs(seconds)))
        .expect("run changelog visible profile workload");
}

async fn run(backend: ChangelogBenchBackend, duration: Duration) -> Result<(), LixError> {
    let segment = changelog_bench::segment_1c_1000ch()?;
    let change_ids = segment.change_ids();
    let store = changelog_bench::prepare_store(backend.create(), &segment, true).await?;
    let deadline = Instant::now() + duration;
    let mut iterations = 0u64;

    while Instant::now() < deadline {
        black_box(changelog_bench::load_changes_visible(&store, &change_ids).await?);
        iterations += 1;
    }

    eprintln!(
        "changelog_visible_profile backend={} duration_ms={} iterations={iterations}",
        backend.label(),
        duration.as_millis()
    );
    Ok(())
}

fn parse_backend(value: &str) -> Result<ChangelogBenchBackend, LixError> {
    match value {
        "unit" | "mem" | "mem_unit" => Ok(ChangelogBenchBackend::Unit),
        "sqlite" | "sqlite_tempfile" => Ok(ChangelogBenchBackend::SqliteTempfile),
        "rocksdb" | "rocksdb_tempdir" => Ok(ChangelogBenchBackend::RocksDbTempdir),
        _ => Err(LixError::unknown(format!(
            "unknown changelog visible profile backend '{value}', expected unit, sqlite, or rocksdb"
        ))),
    }
}
