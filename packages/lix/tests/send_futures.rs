#![recursion_limit = "256"]

use lix::{ExecuteBatchStatement, Lix, SwitchBranchOptions, open_lix};

fn assert_send<T: Send>(_: T) {}
fn assert_send_sync<T: Send + Sync>() {}

#[tokio::test]
async fn public_execution_and_observation_futures_are_send() {
    let lix = open_lix().await.expect("open Lix");
    assert_send_sync::<Lix>();

    assert_send(lix.execute("SELECT 1", &[]));
    assert_send(lix.execute_batch(&[ExecuteBatchStatement {
        label: None,
        sql: "SELECT 1".to_owned(),
        params: Vec::new(),
    }]));
    assert_send(lix.begin_transaction());
    assert_send(lix.switch_branch(SwitchBranchOptions {
        branch_id: lix.active_branch_id().await.expect("active branch"),
    }));

    let mut events = lix.observe("SELECT 1", &[]).expect("observe query");
    assert_send(events.next());

    let mut transaction = lix.begin_transaction().await.expect("begin transaction");
    assert_send(transaction.execute("SELECT 1", &[]));
    assert_send(transaction.commit());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cloned_lix_queries_and_independent_observers_spawn_on_tokio() {
    let lix = open_lix().await.expect("open Lix");
    let query_lix = lix.clone();
    let query = tokio::spawn(async move {
        for _ in 0..32 {
            query_lix.execute("SELECT 1", &[]).await?;
        }
        Ok::<_, lix::LixError>(())
    });

    let mut first = lix.observe("SELECT 1", &[]).expect("first observer");
    let mut second = lix.observe("SELECT 2", &[]).expect("second observer");
    let first = tokio::spawn(async move { first.next().await });
    let second = tokio::spawn(async move { second.next().await });

    query
        .await
        .expect("query task should join")
        .expect("queries");
    assert!(
        first
            .await
            .expect("first observer task should join")
            .expect("first observer next")
            .is_some()
    );
    assert!(
        second
            .await
            .expect("second observer task should join")
            .expect("second observer next")
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_and_closing_spawned_observers_is_deterministic() {
    let lix = open_lix().await.expect("open Lix");

    let mut cancelled_events = lix.observe("SELECT 1", &[]).expect("cancel observer");
    cancelled_events
        .next()
        .await
        .expect("initial cancel observer next")
        .expect("initial cancel observer event");
    let cancelled = tokio::spawn(async move { cancelled_events.next().await });
    tokio::task::yield_now().await;
    cancelled.abort();
    assert!(
        cancelled
            .await
            .expect_err("cancelled observer task should not complete")
            .is_cancelled()
    );
    lix.execute("SELECT 1", &[])
        .await
        .expect("session remains usable after observer cancellation");

    let mut closing_events = lix.observe("SELECT 2", &[]).expect("closing observer");
    closing_events
        .next()
        .await
        .expect("initial closing observer next")
        .expect("initial closing observer event");
    let closing = tokio::spawn(async move { closing_events.next().await });
    tokio::task::yield_now().await;
    lix.close().await.expect("close Lix");
    assert!(
        closing
            .await
            .expect("closing observer task should join")
            .expect("closing observer next")
            .is_none()
    );
}
