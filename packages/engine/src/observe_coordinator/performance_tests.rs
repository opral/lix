use std::hint::black_box;
use std::time::{Duration, Instant};

use super::{ObserveQueryEvaluation, ObserveQueryState, ObserveSharedContent};
use crate::{ExecuteResult, Value};

fn sized_blob_result(size: usize, changed: bool) -> ExecuteResult {
    let mut bytes = vec![0; size];
    if changed {
        bytes[size / 2] = 1;
    }
    ExecuteResult::from_rows(vec!["data".to_string()], vec![vec![Value::Blob(bytes)]])
}

#[test]
#[ignore = "manual performance probe"]
fn observe_result_equivalence_baseline_performance_probe() {
    run_observe_result_equivalence_performance_probe(false);
}

#[test]
#[ignore = "manual performance probe"]
fn observe_result_equivalence_shared_performance_probe() {
    run_observe_result_equivalence_performance_probe(true);
}

fn run_observe_result_equivalence_performance_probe(share_comparison: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build performance probe runtime");
    let mode = if share_comparison {
        "candidate"
    } else {
        "baseline"
    };
    println!("size_mib\tobservers\tworkload\tmode\tmedian_ms\tp10_ms\tp90_ms\titerations");
    for size_mib in [1_usize, 10] {
        for observers in [1_usize, 2, 4, 16] {
            for changed in [false, true] {
                let state = ObserveQueryState::new();
                let initial = runtime
                    .block_on(
                        state.evaluate(1, share_comparison && observers > 1, || async {
                            Ok(sized_blob_result(size_mib * 1024 * 1024, false))
                        }),
                    )
                    .expect("seed performance probe result");
                let mut last_rows = vec![initial.rows; observers];
                let mut last_content = vec![initial.shared_content; observers];
                let mut generation = 1_u64;

                for _ in 0..3 {
                    run_probe_iteration(
                        &runtime,
                        &state,
                        &mut generation,
                        size_mib * 1024 * 1024,
                        changed,
                        share_comparison,
                        &mut last_rows,
                        &mut last_content,
                    );
                }
                let calibration_start = Instant::now();
                for _ in 0..3 {
                    run_probe_iteration(
                        &runtime,
                        &state,
                        &mut generation,
                        size_mib * 1024 * 1024,
                        changed,
                        share_comparison,
                        &mut last_rows,
                        &mut last_content,
                    );
                }
                let per_iteration = calibration_start.elapsed() / 3;
                let iterations = u32::try_from(
                    (Duration::from_millis(30).as_nanos() / per_iteration.as_nanos().max(1))
                        .clamp(2, 100),
                )
                .expect("bounded performance probe iteration count should fit u32");

                let mut samples = Vec::with_capacity(21);
                for _ in 0..21 {
                    let start = Instant::now();
                    for _ in 0..iterations {
                        run_probe_iteration(
                            &runtime,
                            &state,
                            &mut generation,
                            size_mib * 1024 * 1024,
                            changed,
                            share_comparison,
                            &mut last_rows,
                            &mut last_content,
                        );
                    }
                    samples.push(start.elapsed().as_secs_f64() * 1000.0 / f64::from(iterations));
                }
                samples.sort_by(f64::total_cmp);
                println!(
                    "{size_mib}\t{observers}\t{}\t{mode}\t{:.3}\t{:.3}\t{:.3}\t{iterations}",
                    if changed { "changed" } else { "unchanged" },
                    samples[10],
                    samples[2],
                    samples[18]
                );
            }
        }
    }
}

fn run_probe_iteration(
    runtime: &tokio::runtime::Runtime,
    state: &ObserveQueryState,
    generation: &mut u64,
    size: usize,
    changed: bool,
    share_comparison: bool,
    last_rows: &mut [ExecuteResult],
    last_content: &mut [Option<ObserveSharedContent>],
) {
    *generation += 1;
    let variant = changed && (*generation).is_multiple_of(2);
    let evaluation = runtime
        .block_on(state.evaluate(
            *generation,
            share_comparison && last_rows.len() > 1,
            || async move { Ok(sized_blob_result(size, variant)) },
        ))
        .expect("performance probe evaluation");
    for (previous_rows, previous_content) in last_rows.iter_mut().zip(last_content.iter_mut()) {
        let rows_changed = observed_rows_changed(previous_rows, *previous_content, &evaluation);
        *previous_content = evaluation.shared_content;
        if rows_changed {
            *previous_rows = evaluation.rows.clone();
        }
        black_box(rows_changed);
    }
}

fn observed_rows_changed(
    previous_rows: &ExecuteResult,
    previous_content: Option<ObserveSharedContent>,
    evaluation: &ObserveQueryEvaluation,
) -> bool {
    evaluation.rows_changed_since(Some(previous_rows), previous_content)
}
