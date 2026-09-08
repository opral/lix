//! Test-only, task-scoped upload instrumentation; concurrent tests are isolated.

use std::cell::Cell;
use std::future::Future;

tokio::task_local! {
    static COMMIT_PAYLOAD_LOADS: Cell<u64>;
}

pub(super) fn record_commit_payload_load() {
    let _ = COMMIT_PAYLOAD_LOADS.try_with(|count| count.set(count.get() + 1));
}

pub(super) async fn measure_commit_payload_loads<F: Future>(future: F) -> (F::Output, u64) {
    COMMIT_PAYLOAD_LOADS
        .scope(Cell::new(0), async move {
            let output = future.await;
            let count = COMMIT_PAYLOAD_LOADS.with(Cell::get);
            (output, count)
        })
        .await
}
