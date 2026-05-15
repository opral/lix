#[macro_use]
#[path = "support/mod.rs"]
mod support;

macro_rules! simulation_test {
    ($name:ident, |$sim:ident| $body:expr) => {
        simulation_test!(
            $name,
            options = crate::support::simulation_test::engine::SimulationOptions::default(),
            |$sim| $body
        );
    };
    ($name:ident, options = $options:expr, |$sim:ident| $body:expr) => {
        simulation_test!(@single $name, base, Base, $options, |$sim| $body);
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
            #[ignore = "Phase 1 disables public SQL writes; re-enable this SQL integration harness through the bound write pipeline"]
            fn [<$name _ $simulation>]() {
                let simulation_mode =
                    crate::support::simulation_test::engine::SimulationMode::$mode;
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
                                    crate::support::simulation_test::engine::run_single_simulation_test(
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

#[path = "sql/entity_history.rs"]
mod entity_history;
#[path = "sql/errors.rs"]
mod errors;
#[path = "sql/history_conformance.rs"]
mod history_conformance;
#[path = "sql/lix_change.rs"]
mod lix_change;
#[path = "sql/lix_commit.rs"]
mod lix_commit;
#[path = "sql/lix_directory.rs"]
mod lix_directory;
#[path = "sql/lix_directory_history.rs"]
mod lix_directory_history;
#[path = "sql/lix_file.rs"]
mod lix_file;
#[path = "sql/lix_file_history.rs"]
mod lix_file_history;
#[path = "sql/lix_json.rs"]
mod lix_json;
#[path = "sql/lix_key_value.rs"]
mod lix_key_value;
#[path = "sql/lix_label_assignment.rs"]
mod lix_label_assignment;
#[path = "sql/lix_registered_schema.rs"]
mod lix_registered_schema;
#[path = "sql/lix_state.rs"]
mod lix_state;
#[path = "sql/lix_state_history.rs"]
mod lix_state_history;
#[path = "sql/lix_version.rs"]
mod lix_version;
#[path = "sql/metadata.rs"]
mod metadata;
#[path = "sql/read_only.rs"]
mod read_only;
#[path = "sql/udfs.rs"]
mod udfs;

use lix_engine::ExecuteResult;
use lix_engine::Value;

async fn select_rows(
    session: &crate::support::simulation_test::engine::SimSession,
    sql: &str,
) -> Vec<Vec<Value>> {
    let result = session
        .execute(sql, &[])
        .await
        .expect("SELECT should succeed");
    rows_from_result(result)
}

fn assert_rows_eq(result: ExecuteResult, expected: Vec<Vec<Value>>) {
    assert_eq!(rows_from_result(result), expected);
}

fn rows_from_result(result: ExecuteResult) -> Vec<Vec<Value>> {
    let row_set = result;
    row_set
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}
