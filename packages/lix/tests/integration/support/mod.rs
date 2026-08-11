pub mod simulation_test;

/// Seed budget for the randomized replay suites (`*_fuzz`).
///
/// The shipped list is the default, so CI keeps paying for exactly the seeds
/// it pays for today. `LIX_FUZZ_SEEDS=<count>` replaces it with `<count>`
/// sequential seeds starting at `LIX_FUZZ_SEED_START` (default `0`), which is
/// how an on-demand soak sweeps an arbitrary range without editing source.
///
/// The override **replaces** the shipped seeds instead of extending them. A
/// sequential range chained onto the shipped list revisits seeds that are
/// already in it, and a revisited seed re-enters its own per-seed key
/// namespace: the suites then report constraint failures that are artifacts of
/// the harness rather than of the engine, which is exactly the noise that
/// hides a real failure in a wide sweep.
///
/// ```text
/// LIX_FUZZ_SEEDS=512 cargo test -p lix --all-features --lib -- fuzz
/// LIX_FUZZ_SEEDS=512 LIX_FUZZ_SEED_START=512 cargo test ...   # next disjoint window
/// ```
pub fn fuzz_seeds(default: &[u64]) -> Vec<u64> {
    let Some(count) = env_u64("LIX_FUZZ_SEEDS") else {
        return default.to_vec();
    };
    let start = env_u64("LIX_FUZZ_SEED_START").unwrap_or(0);
    (0..count)
        .map(|offset| start.saturating_add(offset))
        .collect()
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().map(|raw| {
        raw.trim().parse::<u64>().unwrap_or_else(|error| {
            panic!("{name} must be a u64 seed budget, got {raw:?}: {error}")
        })
    })
}

macro_rules! simulation_test {
    ($name:ident, |$sim:ident| $body:expr) => {
        simulation_test!(
            $name,
            options =
                $crate::support::simulation_test::engine::SimulationOptions::default(),
            |$sim| $body
        );
    };
    ($name:ident, options = $options:expr, |$sim:ident| $body:expr) => {
        simulation_test!(
            @single $name,
            base,
            Base,
            $options,
            |$sim| $body
        );
        #[cfg(feature = "all-simulations")]
        simulation_test!(
            @single $name,
            tracked_state_rebuild,
            TrackedStateRebuild,
            $options,
            |$sim| $body
        );
    };
    (@single $name:ident, $simulation:ident, $mode:ident, $options:expr, |$sim:ident| $body:expr) => {
        paste::paste! {
                #[test]
                fn [<$name _ $simulation>]() {
                    let simulation_mode =
                        $crate::support::simulation_test::engine::SimulationMode::$mode;
                    let simulation_name = stringify!($simulation);
                    let timeout_secs = std::env::var("LIX_SIMULATION_TEST_TIMEOUT_SECS")
                        .ok()
                        .and_then(|raw| raw.parse::<u64>().ok())
                        .unwrap_or(120);
                    let case_id = concat!(module_path!(), "::", stringify!($name));
                    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
                    let thread = std::thread::Builder::new()
                        .name(format!("{}_{}", stringify!($name), simulation_name))
                        .stack_size(32 * 1024 * 1024)
                        .spawn(move || {
                            let run_result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    let runtime = tokio::runtime::Builder::new_current_thread()
                                        .enable_all()
                                        .build()
                                        .expect("failed to build tokio runtime");
                                    runtime.block_on(async {
                                        $crate::support::simulation_test::engine::run_simulation_test(
                                            simulation_mode,
                                            $options,
                                            case_id,
                                            |$sim| $body,
                                        )
                                        .await;
                                    });
                                }));
                            let _ = result_tx.send(run_result);
                        })
                        .expect(concat!(
                            "failed to spawn ",
                            stringify!($name),
                            " simulation_test thread"
                        ));

                    match result_rx.recv_timeout(std::time::Duration::from_secs(timeout_secs)) {
                        Ok(Ok(())) => {
                            thread.join().expect(concat!(
                                stringify!($name),
                                " simulation_test thread panicked"
                            ));
                        }
                        Ok(Err(payload)) => {
                            let _ = thread.join();
                            std::panic::resume_unwind(payload);
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            panic!(
                                "simulation_test timed out after {}s (simulation={}, case={})",
                                timeout_secs, simulation_name, case_id
                            );
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            if let Err(payload) = thread.join() {
                                std::panic::resume_unwind(payload);
                            }
                            panic!(
                                "simulation_test thread exited without reporting result (simulation={}, case={})",
                                simulation_name, case_id
                            );
                        }
                    }
                }
        }
    };
}
