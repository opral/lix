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
    let segment = changelog_bench::segment_1c_1000ch()?;
    let encoded = changelog_bench::encode_bench_segment(&segment)?;
    let samples = 10;

    Ok(vec![
        (
            "encode_segment / 1c_1000ch",
            measure_cpu(samples, || {
                black_box(changelog_bench::encode_bench_segment(&segment)?);
                Ok(())
            })?,
        ),
        (
            "decode_segment / 1c_1000ch",
            measure_cpu(samples, || {
                black_box(changelog_bench::decode_bench_segment(&encoded)?);
                Ok(())
            })?,
        ),
        (
            "validate_segment_shape / 1c_1000ch",
            measure_cpu(samples, || {
                black_box(changelog_bench::validate_bench_segment_shape(&segment)?);
                Ok(())
            })?,
        ),
        (
            "build_decoded_segment_index / 1c_1000ch",
            measure_cpu(samples, || {
                black_box(changelog_bench::build_decoded_segment_index(&segment)?);
                Ok(())
            })?,
        ),
        (
            "build_by_change / 1c_1000ch",
            measure_cpu(samples, || {
                black_box(changelog_bench::build_by_change_entries(&segment)?);
                Ok(())
            })?,
        ),
        (
            "build_by_change_membership / 1c_1000ch",
            measure_cpu(samples, || {
                black_box(changelog_bench::build_by_change_membership_entries(
                    &segment,
                ));
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
            "stage_segment_raw_no_indexes / 1c_1000ch",
            samples,
            |backend| {
                Box::pin(async move {
                    let segment = changelog_bench::segment_1c_1000ch()?;
                    let start = Instant::now();
                    black_box(
                        changelog_bench::stage_segment_raw_once(backend.create(), &segment).await?,
                    );
                    Ok(start.elapsed())
                })
            },
        )
        .await?,
    );

    rows.push(
        backend_row("stage_segment / 1c_1000ch", samples, |backend| {
            Box::pin(async move {
                let segment = changelog_bench::segment_1c_1000ch()?;
                let start = Instant::now();
                black_box(changelog_bench::stage_segment_once(backend.create(), &segment).await?);
                Ok(start.elapsed())
            })
        })
        .await?,
    );

    rows.push(
        stage_publish_row(
            "stage_publish_commit / 1c_1ch",
            || changelog_bench::segment_1c_1ch(),
            samples,
        )
        .await?,
    );
    rows.push(
        stage_publish_row(
            "stage_publish_commit / 1c_100ch",
            || changelog_bench::segment_1c_100ch(),
            samples,
        )
        .await?,
    );
    rows.push(
        stage_publish_row(
            "stage_publish_commit / 1c_1000ch single-shot",
            || changelog_bench::segment_1c_1000ch(),
            1,
        )
        .await?,
    );

    rows.push(
        backend_row(
            "load_commits_visible_batched / 1c_100ch",
            samples,
            |backend| {
                Box::pin(async move {
                    let segment = changelog_bench::segment_1c_100ch()?;
                    let store =
                        changelog_bench::prepare_store(backend.create(), &segment, true).await?;
                    let commit_ids = segment.commit_ids();
                    let start = Instant::now();
                    black_box(changelog_bench::load_commits_visible(&store, &commit_ids).await?);
                    Ok(start.elapsed())
                })
            },
        )
        .await?,
    );

    rows.push(
        backend_row(
            "load_changes_visible_batched / 1c_100ch",
            samples,
            |backend| {
                Box::pin(async move {
                    let segment = changelog_bench::segment_1c_100ch()?;
                    let store =
                        changelog_bench::prepare_store(backend.create(), &segment, true).await?;
                    let change_ids = segment.change_ids();
                    let start = Instant::now();
                    black_box(changelog_bench::load_changes_visible(&store, &change_ids).await?);
                    Ok(start.elapsed())
                })
            },
        )
        .await?,
    );

    rows.push(
        backend_row(
            "load_changes_visible_batched / 1c_1000ch",
            samples,
            |backend| {
                Box::pin(async move {
                    let segment = changelog_bench::segment_1c_1000ch()?;
                    let store =
                        changelog_bench::prepare_store(backend.create(), &segment, true).await?;
                    let change_ids = segment.change_ids();
                    let start = Instant::now();
                    black_box(changelog_bench::load_changes_visible(&store, &change_ids).await?);
                    Ok(start.elapsed())
                })
            },
        )
        .await?,
    );

    rows.push(
        backend_row(
            "load_changes_physical_scattered / 100seg_100c_1000ch",
            samples,
            |backend| {
                Box::pin(async move {
                    let corpus = changelog_bench::corpus_100seg_100c_1000ch()?;
                    let store =
                        changelog_bench::prepare_corpus_store(backend.create(), &corpus, false)
                            .await?;
                    let change_ids = corpus.change_ids().to_vec();
                    let start = Instant::now();
                    black_box(changelog_bench::load_changes_physical(&store, &change_ids).await?);
                    Ok(start.elapsed())
                })
            },
        )
        .await?,
    );

    rows.push(
        backend_row(
            "load_changes_visible_scattered / 100seg_100c_1000ch",
            samples,
            |backend| {
                Box::pin(async move {
                    let corpus = changelog_bench::corpus_100seg_100c_1000ch()?;
                    let store =
                        changelog_bench::prepare_corpus_store(backend.create(), &corpus, true)
                            .await?;
                    let change_ids = corpus.change_ids().to_vec();
                    let start = Instant::now();
                    black_box(changelog_bench::load_changes_visible(&store, &change_ids).await?);
                    Ok(start.elapsed())
                })
            },
        )
        .await?,
    );

    rows.push(
        backend_row(
            "rebuild_mandatory_indexes / 100seg_100c_1000ch",
            samples,
            |backend| {
                Box::pin(async move {
                    let corpus = changelog_bench::corpus_100seg_100c_1000ch()?;
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
        backend_row("plan_gc / live_50pct_mixed_segments", samples, |backend| {
            Box::pin(async move {
                let (store, root_commit_id) =
                    changelog_bench::prepare_gc_store(backend.create(), 50, 50, 10).await?;
                let start = Instant::now();
                black_box(changelog_bench::plan_gc(&store, &root_commit_id).await?);
                Ok(start.elapsed())
            })
        })
        .await?,
    );

    rows.push(
        backend_row(
            "collect_garbage / live_50pct_mixed_segments",
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

async fn stage_publish_row(
    label: &'static str,
    segment: fn() -> Result<changelog_bench::BenchSegment, LixError>,
    samples: usize,
) -> Result<BackendScoreRow, LixError> {
    backend_row(label, samples, move |backend| {
        Box::pin(async move {
            let segment = segment()?;
            let store = changelog_bench::prepare_store(backend.create(), &segment, false).await?;
            let start = Instant::now();
            black_box(
                changelog_bench::stage_publish_first_commit_in_store(&store, &segment).await?,
            );
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
    };

    for backend in ChangelogBenchBackend::CI {
        eprintln!("scorecard: {label} / {backend:?}");
        let duration = measure_async(samples, || op(backend)).await?;
        match backend {
            ChangelogBenchBackend::Unit => row.mem_unit = duration,
            ChangelogBenchBackend::SqliteTempfile => row.sqlite_tempfile = duration,
            ChangelogBenchBackend::RocksDbTempdir => row.rocksdb_tempdir = duration,
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
}

fn print_scorecard(cpu: &[(&'static str, Duration)], backend: &[BackendScoreRow]) {
    println!("## CPU Segment Scoreboard");
    println!();
    println!("| row | baseline_ms |");
    println!("| --- | ---: |");
    for (label, duration) in cpu {
        println!("| {label} | {} |", ms(*duration));
    }
    println!();
    println!("## Backend Smoke Scoreboard");
    println!();
    println!("| row | mem_unit_ms | sqlite_tempfile_ms | rocksdb_tempdir_ms |");
    println!("| --- | ---: | ---: | ---: |");
    for row in backend {
        println!(
            "| {} | {} | {} | {} |",
            row.label,
            ms(row.mem_unit),
            ms(row.sqlite_tempfile),
            ms(row.rocksdb_tempdir)
        );
    }
}

fn ms(duration: Duration) -> String {
    format!("{:.3}", duration.as_secs_f64() * 1000.0)
}
