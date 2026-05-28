pub mod simulation_test;

#[macro_export]
macro_rules! simulation_test {
    ($name:ident, |$sim:ident| $body:expr) => {
        $crate::simulation_test!(
            $name,
            options =
                $crate::support::simulation_test::engine::SimulationOptions::default(),
            |$sim| $body
        );
    };
    ($name:ident, options = $options:expr, |$sim:ident| $body:expr) => {
        $crate::simulation_test!(
            @single $name,
            base,
            Base,
            $options,
            |$sim| $body
        );
        $crate::simulation_test!(
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
