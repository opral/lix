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
    let op = args.next().unwrap_or_else(|| "visible".to_string());
    let seconds = args
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(15);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for changelog visible profile");

    runtime
        .block_on(run(backend, &op, Duration::from_secs(seconds)))
        .expect("run changelog visible profile workload");
}

async fn run(backend: ChangelogBenchBackend, op: &str, duration: Duration) -> Result<(), LixError> {
    let segment = changelog_bench::segment_1c_1000ch()?;
    let corpus = changelog_bench::corpus_100seg_100c_1000ch()?;
    let change_ids = segment.change_ids();
    let visible_store = changelog_bench::prepare_store(backend.create(), &segment, true).await?;
    let physical_store = changelog_bench::prepare_store(backend.create(), &segment, false).await?;
    let corpus_store =
        changelog_bench::prepare_corpus_store(backend.create(), &corpus, true).await?;
    let corpus_change_ids = corpus.change_ids().to_vec();
    let deadline = Instant::now() + duration;
    let mut iterations = 0u64;

    while Instant::now() < deadline {
        match op {
            "visible" | "load_visible" => {
                black_box(
                    changelog_bench::load_changes_visible(&visible_store, &change_ids).await?,
                );
            }
            "physical" | "load_physical" => {
                black_box(
                    changelog_bench::load_changes_physical(&physical_store, &change_ids).await?,
                );
            }
            "visible_scattered" => {
                black_box(
                    changelog_bench::load_changes_visible(&corpus_store, &corpus_change_ids)
                        .await?,
                );
            }
            "physical_scattered" => {
                black_box(
                    changelog_bench::load_changes_physical(&corpus_store, &corpus_change_ids)
                        .await?,
                );
            }
            "stage_segment" => {
                black_box(changelog_bench::stage_segment_once(backend.create(), &segment).await?);
            }
            "stage_segment_raw" => {
                black_box(
                    changelog_bench::stage_segment_raw_once(backend.create(), &segment).await?,
                );
            }
            "stage_corpus" => {
                black_box(changelog_bench::stage_corpus_once(backend.create(), &corpus).await?);
            }
            "rebuild" => {
                let store =
                    changelog_bench::prepare_corpus_store(backend.create(), &corpus, false).await?;
                black_box(changelog_bench::rebuild_mandatory_indexes(&store).await?);
            }
            "plan_gc" => {
                let (store, root_commit_id) =
                    changelog_bench::prepare_gc_store(backend.create(), 50, 50, 10).await?;
                black_box(changelog_bench::plan_gc(&store, &root_commit_id).await?);
            }
            "collect_gc" => {
                let (store, root_commit_id) =
                    changelog_bench::prepare_gc_store(backend.create(), 50, 50, 10).await?;
                black_box(changelog_bench::collect_garbage(&store, &root_commit_id).await?);
            }
            _ => {
                return Err(LixError::unknown(format!(
                    "unknown changelog profile op '{op}', expected visible, physical, visible_scattered, physical_scattered, stage_segment, stage_segment_raw, stage_corpus, rebuild, plan_gc, or collect_gc"
                )));
            }
        }
        iterations += 1;
    }

    eprintln!(
        "changelog_visible_profile backend={} op={op} duration_ms={} iterations={iterations}",
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
        "redb" | "redb_tempfile" => Ok(ChangelogBenchBackend::RedbTempfile),
        _ => Err(LixError::unknown(format!(
            "unknown changelog visible profile backend '{value}', expected unit, sqlite, rocksdb, or redb"
        ))),
    }
}
