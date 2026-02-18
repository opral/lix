pub mod simulation_test;
pub mod simulations;
pub mod wasmtime_runtime;

#[macro_export]
macro_rules! simulation_test {
    ($name:ident, |$sim:ident| $body:expr) => {
        $crate::simulation_test!(
            $name,
            simulations = [sqlite, postgres, materialization],
            |$sim| $body
        );
    };
    ($name:ident, simulations = [$($simulation:ident),+ $(,)?], |$sim:ident| $body:expr) => {
        paste::paste! {
            $(
                #[test]
                fn [<$name _ $simulation>]() {
                    let simulation_name = stringify!($simulation);
                    let case_id = concat!(module_path!(), "::", stringify!($name));
                    let timeout_secs = std::env::var("LIX_SIMULATION_TEST_TIMEOUT_SECS")
                        .ok()
                        .and_then(|raw| raw.parse::<u64>().ok())
                        .unwrap_or(120);
                    let simulation_name_for_thread = simulation_name;
                    let case_id_for_thread = case_id;
                    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
                    let thread = std::thread::Builder::new()
                        .name(format!("{}_{}", stringify!($name), simulation_name))
                        .stack_size(8 * 1024 * 1024)
                        .spawn(move || {
                            let run_result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    let runtime = tokio::runtime::Builder::new_current_thread()
                                        .enable_all()
                                        .build()
                                        .expect("failed to build tokio runtime");
                                    runtime.block_on(async {
                                        $crate::support::simulation_test::run_single_simulation_test(
                                            simulation_name_for_thread,
                                            case_id_for_thread,
                                            |$sim| $body,
                                        )
                                        .await;
                                    });
                                }));
                            let _ = result_tx.send(run_result);
                        })
                        .expect(concat!(
                            "failed to spawn ",
                            stringify!($simulation),
                            " test thread"
                        ));

                    match result_rx.recv_timeout(std::time::Duration::from_secs(timeout_secs)) {
                        Ok(Ok(())) => {
                            thread.join().expect(concat!(
                                stringify!($simulation),
                                " simulation test thread panicked"
                            ));
                        }
                        Ok(Err(payload)) => {
                            let _ = thread.join();
                            std::panic::resume_unwind(payload);
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            panic!(
                                "simulation test timed out after {}s (simulation={}, case={})",
                                timeout_secs, simulation_name, case_id
                            );
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            if let Err(payload) = thread.join() {
                                std::panic::resume_unwind(payload);
                            }
                            panic!(
                                "simulation test thread exited without reporting result (simulation={}, case={})",
                                simulation_name, case_id
                            );
                        }
                    }
                }
            )+
        }
    };
}
