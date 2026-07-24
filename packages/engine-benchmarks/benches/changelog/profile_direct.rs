use std::hint::black_box;
use std::time::{Duration, Instant};

use lix_engine::LixError;
use lix_engine::changelog::bench as changelog_bench;

mod storage;

use storage::ChangelogBenchStorage;

fn main() {
    let mut args = std::env::args().skip(1);
    let storage = args
        .next()
        .as_deref()
        .map(parse_storage)
        .transpose()
        .expect("parse changelog direct profile storage")
        .unwrap_or(ChangelogBenchStorage::Unit);
    let op = args.next().unwrap_or_else(|| "direct".to_string());
    let seconds = args
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(15);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for changelog direct profile");

    runtime
        .block_on(run(storage, &op, Duration::from_secs(seconds)))
        .expect("run changelog direct profile workload");
}

async fn run(storage: ChangelogBenchStorage, op: &str, duration: Duration) -> Result<(), LixError> {
    let append = changelog_bench::append_1c_1000ch()?;
    let corpus = changelog_bench::corpus_100append_100c_1000ch()?;
    let change_ids = append.change_ids();
    let direct_store = changelog_bench::prepare_store(storage.create(), &append).await?;
    let direct_by_id_store = changelog_bench::prepare_store(storage.create(), &append).await?;
    let corpus_store = changelog_bench::prepare_corpus_store(storage.create(), &corpus).await?;
    let corpus_change_ids = corpus.change_ids().clone();
    let deadline = Instant::now() + duration;
    let mut iterations = 0u64;

    while Instant::now() < deadline {
        match op {
            "direct" | "load_direct" => {
                black_box(changelog_bench::load_changes_direct(&direct_store, &change_ids).await?);
            }
            "direct_by_id" | "load_direct_by_id" => {
                black_box(
                    changelog_bench::load_changes_direct_by_id(&direct_by_id_store, &change_ids)
                        .await?,
                );
            }
            "direct_scattered" => {
                black_box(
                    changelog_bench::load_changes_direct(&corpus_store, &corpus_change_ids).await?,
                );
            }
            "direct_by_id_scattered" => {
                black_box(
                    changelog_bench::load_changes_direct_by_id(&corpus_store, &corpus_change_ids)
                        .await?,
                );
            }
            "stage_append" => {
                black_box(changelog_bench::stage_append_once(storage.create(), &append).await?);
            }
            "stage_append_raw" => {
                black_box(changelog_bench::stage_append_raw_once(storage.create(), &append).await?);
            }
            "stage_corpus" => {
                black_box(changelog_bench::stage_corpus_once(storage.create(), &corpus).await?);
            }
            "rebuild" => {
                let store =
                    changelog_bench::prepare_corpus_store(storage.create(), &corpus).await?;
                black_box(changelog_bench::rebuild_mandatory_indexes(&store).await?);
            }
            "plan_gc" => {
                let (store, root_commit_id) =
                    changelog_bench::prepare_gc_store(storage.create(), 50, 50, 10).await?;
                black_box(changelog_bench::plan_gc(&store, &root_commit_id).await?);
            }
            "collect_gc" => {
                let (store, root_commit_id) =
                    changelog_bench::prepare_gc_store(storage.create(), 50, 50, 10).await?;
                black_box(changelog_bench::collect_garbage(&store, &root_commit_id).await?);
            }
            _ => {
                return Err(LixError::unknown(format!(
                    "unknown changelog profile op '{op}', expected direct, direct_by_id, direct_scattered, direct_by_id_scattered, stage_append, stage_append_raw, stage_corpus, rebuild, plan_gc, or collect_gc"
                )));
            }
        }
        iterations += 1;
    }

    eprintln!(
        "changelog_direct_profile storage={} op={op} duration_ms={} iterations={iterations}",
        storage_label(storage),
        duration.as_millis()
    );
    Ok(())
}

fn storage_label(storage: ChangelogBenchStorage) -> &'static str {
    match storage {
        ChangelogBenchStorage::Unit => "mem_unit",
        ChangelogBenchStorage::SQLiteTempfile => "sqlite_tempfile",
        ChangelogBenchStorage::RocksDBTempdir => "rocksdb_tempdir",
    }
}

fn parse_storage(value: &str) -> Result<ChangelogBenchStorage, LixError> {
    match value {
        "unit" | "mem" | "mem_unit" => Ok(ChangelogBenchStorage::Unit),
        "sqlite" | "sqlite_tempfile" => Ok(ChangelogBenchStorage::SQLiteTempfile),
        "rocksdb" | "rocksdb_tempdir" => Ok(ChangelogBenchStorage::RocksDBTempdir),
        _ => Err(LixError::unknown(format!(
            "unknown changelog direct profile storage '{value}', expected unit, sqlite, or rocksdb"
        ))),
    }
}
