use std::future::Future;
use std::hint::black_box;
use std::pin::Pin;
use std::time::{Duration, Instant};

use lix_engine::changelog::bench as changelog_bench;
use lix_engine::LixError;

mod backends;

use backends::ChangelogBenchBackend;

type TimedFuture = Pin<Box<dyn Future<Output = Result<Duration, LixError>>>>;

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for changelog scorecard");

    let cpu = cpu_scorecard().expect("run changelog CPU scorecard");
    let backend = runtime
        .block_on(backend_scorecard())
        .expect("run changelog backend scorecard");

    print_scorecard(&cpu, &backend);
}

fn cpu_scorecard() -> Result<Vec<(&'static str, Duration)>, LixError> {
    let append = changelog_bench::append_1c_1000ch()?;
    let encoded = changelog_bench::encode_bench_append(&append)?;
    let samples = 10;

    Ok(vec![
        (
            "encode_append / 1c_1000ch",
            measure_cpu(samples, || {
                black_box(changelog_bench::encode_bench_append(&append)?);
                Ok(())
            })?,
        ),
        (
            "decode_append / 1c_1000ch",
            measure_cpu(samples, || {
                black_box(changelog_bench::decode_bench_append(&encoded)?);
                Ok(())
            })?,
        ),
        (
            "view_append / 1c_1000ch",
            measure_cpu(samples, || {
                black_box(changelog_bench::view_bench_append(&encoded)?);
                Ok(())
            })?,
        ),
        (
            "validate_append_shape / 1c_1000ch",
            measure_cpu(samples, || {
                black_box(changelog_bench::validate_bench_append_shape(&append)?);
                Ok(())
            })?,
        ),
        (
            "build_decoded_append_index / 1c_1000ch",
            measure_cpu(samples, || {
                black_box(changelog_bench::build_decoded_append_index(&append)?);
                Ok(())
            })?,
        ),
        (
            "build_direct_change_records / 1c_1000ch",
            measure_cpu(samples, || {
                black_box(changelog_bench::build_direct_change_record_entries(
                    &append,
                )?);
                Ok(())
            })?,
        ),
        (
            "build_commit_change_refs / 1c_1000ch",
            measure_cpu(samples, || {
                black_box(changelog_bench::build_commit_change_ref_entries(&append));
                Ok(())
            })?,
        ),
    ])
}

async fn backend_scorecard() -> Result<Vec<BackendScoreRow>, LixError> {
    let mut rows = Vec::new();
    let samples = 1;

    rows.push(
        backend_row(
            "stage_append_raw_no_indexes / 1c_1000ch",
            samples,
            |backend| {
                Box::pin(async move {
                    let append = changelog_bench::append_1c_1000ch()?;
                    let start = Instant::now();
                    black_box(
                        changelog_bench::stage_append_raw_once(backend.create(), &append).await?,
                    );
                    Ok(start.elapsed())
                })
            },
        )
        .await?,
    );

    rows.push(
        backend_row("stage_append / 1c_1000ch", samples, |backend| {
            Box::pin(async move {
                let append = changelog_bench::append_1c_1000ch()?;
                let start = Instant::now();
                black_box(changelog_bench::stage_append_once(backend.create(), &append).await?);
                Ok(start.elapsed())
            })
        })
        .await?,
    );

    rows.push(
        stage_commit_noop_row(
            "stage_commit_noop / 1c_1ch",
            || changelog_bench::append_1c_1ch(),
            samples,
        )
        .await?,
    );
    rows.push(
        stage_commit_noop_row(
            "stage_commit_noop / 1c_100ch",
            || changelog_bench::append_1c_100ch(),
            samples,
        )
        .await?,
    );
    rows.push(
        stage_commit_noop_row(
            "stage_commit_noop / 1c_1000ch single-shot",
            || changelog_bench::append_1c_1000ch(),
            1,
        )
        .await?,
    );

    rows.push(
        backend_row(
            "load_commits_direct_batched / 1c_100ch",
            samples,
            |backend| {
                Box::pin(async move {
                    let append = changelog_bench::append_1c_100ch()?;
                    let store = changelog_bench::prepare_store(backend.create(), &append).await?;
                    let commit_ids = append.commit_ids();
                    let start = Instant::now();
                    black_box(changelog_bench::load_commits_direct(&store, &commit_ids).await?);
                    Ok(start.elapsed())
                })
            },
        )
        .await?,
    );

    rows.push(
        backend_row(
            "load_changes_direct_batched / 1c_100ch",
            samples,
            |backend| {
                Box::pin(async move {
                    let append = changelog_bench::append_1c_100ch()?;
                    let store = changelog_bench::prepare_store(backend.create(), &append).await?;
                    let change_ids = append.change_ids();
                    let start = Instant::now();
                    black_box(changelog_bench::load_changes_direct(&store, &change_ids).await?);
                    Ok(start.elapsed())
                })
            },
        )
        .await?,
    );

    rows.push(
        backend_row(
            "load_changes_direct_batched / 1c_1000ch",
            samples,
            |backend| {
                Box::pin(async move {
                    let append = changelog_bench::append_1c_1000ch()?;
                    let store = changelog_bench::prepare_store(backend.create(), &append).await?;
                    let change_ids = append.change_ids();
                    let start = Instant::now();
                    black_box(changelog_bench::load_changes_direct(&store, &change_ids).await?);
                    Ok(start.elapsed())
                })
            },
        )
        .await?,
    );

    rows.push(
        backend_row(
            "load_changes_direct_by_id_scattered / 100append_100c_1000ch",
            samples,
            |backend| {
                Box::pin(async move {
                    let corpus = changelog_bench::corpus_100append_100c_1000ch()?;
                    let store =
                        changelog_bench::prepare_corpus_store(backend.create(), &corpus).await?;
                    let change_ids = corpus.change_ids().to_vec();
                    let start = Instant::now();
                    black_box(
                        changelog_bench::load_changes_direct_by_id(&store, &change_ids).await?,
                    );
                    Ok(start.elapsed())
                })
            },
        )
        .await?,
    );

    rows.push(
        backend_row(
            "load_changes_direct_scattered / 100append_100c_1000ch",
            samples,
            |backend| {
                Box::pin(async move {
                    let corpus = changelog_bench::corpus_100append_100c_1000ch()?;
                    let store =
                        changelog_bench::prepare_corpus_store(backend.create(), &corpus).await?;
                    let change_ids = corpus.change_ids().to_vec();
                    let start = Instant::now();
                    black_box(changelog_bench::load_changes_direct(&store, &change_ids).await?);
                    Ok(start.elapsed())
                })
            },
        )
        .await?,
    );

    rows.push(
        backend_row(
            "rebuild_mandatory_indexes / 100append_100c_1000ch",
            samples,
            |backend| {
                Box::pin(async move {
                    let corpus = changelog_bench::corpus_100append_100c_1000ch()?;
                    let store = changelog_bench::prepare_rebuild_store(
                        backend.create(),
                        &corpus,
                        changelog_bench::BenchRebuildMode::EmptyIndexes,
                    )
                    .await?;
                    let start = Instant::now();
                    black_box(changelog_bench::rebuild_mandatory_indexes(&store).await?);
                    Ok(start.elapsed())
                })
            },
        )
        .await?,
    );

    rows.push(
        backend_row(
            "plan_gc / live_50pct_mixed_append_batches",
            samples,
            |backend| {
                Box::pin(async move {
                    let (store, root_commit_id) =
                        changelog_bench::prepare_gc_store(backend.create(), 50, 50, 10).await?;
                    let start = Instant::now();
                    black_box(changelog_bench::plan_gc(&store, &root_commit_id).await?);
                    Ok(start.elapsed())
                })
            },
        )
        .await?,
    );

    rows.push(
        backend_row(
            "collect_garbage / live_50pct_mixed_append_batches",
            samples,
            |backend| {
                Box::pin(async move {
                    let (store, root_commit_id) =
                        changelog_bench::prepare_gc_store(backend.create(), 50, 50, 10).await?;
                    let start = Instant::now();
                    black_box(changelog_bench::collect_garbage(&store, &root_commit_id).await?);
                    Ok(start.elapsed())
                })
            },
        )
        .await?,
    );

    Ok(rows)
}

async fn stage_commit_noop_row(
    label: &'static str,
    append: fn() -> Result<changelog_bench::BenchAppend, LixError>,
    samples: usize,
) -> Result<BackendScoreRow, LixError> {
    backend_row(label, samples, move |backend| {
        Box::pin(async move {
            let append = append()?;
            let store = changelog_bench::prepare_store(backend.create(), &append).await?;
            let start = Instant::now();
            black_box(changelog_bench::stage_first_commit_noop_in_store(&store, &append).await?);
            Ok(start.elapsed())
        })
    })
    .await
}

async fn backend_row(
    label: &'static str,
    samples: usize,
    mut op: impl FnMut(ChangelogBenchBackend) -> TimedFuture,
) -> Result<BackendScoreRow, LixError> {
    let mut row = BackendScoreRow {
        label,
        mem_unit: Duration::ZERO,
        sqlite_tempfile: Duration::ZERO,
        rocksdb_tempdir: Duration::ZERO,
        redb_tempfile: Duration::ZERO,
    };

    for backend in ChangelogBenchBackend::CI {
        eprintln!("scorecard: {label} / {backend:?}");
        let duration = measure_async(samples, || op(backend)).await?;
        match backend {
            ChangelogBenchBackend::Unit => row.mem_unit = duration,
            ChangelogBenchBackend::SqliteTempfile => row.sqlite_tempfile = duration,
            ChangelogBenchBackend::RocksDbTempdir => row.rocksdb_tempdir = duration,
            ChangelogBenchBackend::RedbTempfile => row.redb_tempfile = duration,
        }
    }

    Ok(row)
}

async fn measure_async(
    samples: usize,
    mut op: impl FnMut() -> TimedFuture,
) -> Result<Duration, LixError> {
    let mut durations = Vec::with_capacity(samples);
    for _ in 0..samples {
        durations.push(op().await?);
    }
    Ok(median(durations))
}

fn measure_cpu(
    samples: usize,
    mut op: impl FnMut() -> Result<(), LixError>,
) -> Result<Duration, LixError> {
    let mut durations = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        op()?;
        durations.push(start.elapsed());
    }
    Ok(median(durations))
}

fn median(mut durations: Vec<Duration>) -> Duration {
    durations.sort();
    durations[durations.len() / 2]
}

#[derive(Debug)]
struct BackendScoreRow {
    label: &'static str,
    mem_unit: Duration,
    sqlite_tempfile: Duration,
    rocksdb_tempdir: Duration,
    redb_tempfile: Duration,
}

fn print_scorecard(cpu: &[(&'static str, Duration)], backend: &[BackendScoreRow]) {
    println!("## CPU Append Scoreboard");
    println!();
    println!("| row | baseline_ms |");
    println!("| --- | ---: |");
    for (label, duration) in cpu {
        println!("| {label} | {} |", ms(*duration));
    }
    println!();
    println!("## Backend Smoke Scoreboard");
    println!();
    println!("| row | mem_unit_ms | sqlite_tempfile_ms | rocksdb_tempdir_ms | redb_tempfile_ms |");
    println!("| --- | ---: | ---: | ---: | ---: |");
    for row in backend {
        println!(
            "| {} | {} | {} | {} | {} |",
            row.label,
            ms(row.mem_unit),
            ms(row.sqlite_tempfile),
            ms(row.rocksdb_tempdir),
            ms(row.redb_tempfile)
        );
    }
}

fn ms(duration: Duration) -> String {
    format!("{:.3}", duration.as_secs_f64() * 1000.0)
}
