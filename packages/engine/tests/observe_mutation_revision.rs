use std::time::Duration;

use lix_engine::{Backend, Engine, InMemoryBackend, ObserveEvent, ObserveEvents, Value};
use serde_json::json;

#[allow(dead_code)]
#[path = "backend/support/sqlite_backend.rs"]
mod sqlite_backend;

use sqlite_backend::SqliteBackend;

const NEXT_TIMEOUT: Duration = Duration::from_secs(1);
const NO_EVENT_TIMEOUT: Duration = Duration::from_millis(250);
const KEY_VALUE_SQL: &str = "SELECT key, value FROM lix_key_value WHERE key = $1 ORDER BY key";

async fn open_two_engines() -> (Engine, Engine) {
    let backend = InMemoryBackend::new();
    Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let observer_engine = Engine::new(backend.clone())
        .await
        .expect("observer engine should open");
    let writer_engine = Engine::new(backend)
        .await
        .expect("writer engine should open");
    (observer_engine, writer_engine)
}

fn observe_key<B>(session: &lix_engine::SessionContext<B>, key: &str) -> ObserveEvents<B>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    session
        .observe(KEY_VALUE_SQL, &[Value::Text(key.to_string())])
        .expect("observe should open")
}

async fn next_event<B>(events: &mut ObserveEvents<B>, label: &str) -> ObserveEvent
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    tokio::time::timeout(NEXT_TIMEOUT, events.next())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for observe event: {label}"))
        .unwrap_or_else(|error| panic!("observe next failed for {label}: {error:?}"))
        .unwrap_or_else(|| panic!("observe closed before event: {label}"))
}

async fn expect_no_event<B>(events: &mut ObserveEvents<B>, label: &str)
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    match tokio::time::timeout(NO_EVENT_TIMEOUT, events.next()).await {
        Err(_) => {}
        Ok(Ok(Some(event))) => panic!("unexpected observe event for {label}: {event:?}"),
        Ok(Ok(None)) => panic!("observe closed unexpectedly while waiting for no event: {label}"),
        Ok(Err(error)) => panic!("observe errored while waiting for no event {label}: {error:?}"),
    }
}

fn assert_key_value_row(event: &ObserveEvent, key: &str, value: &str) {
    assert_eq!(event.rows.columns(), &["key", "value"]);
    assert_eq!(event.rows.len(), 1);
    assert_eq!(
        event.rows.rows()[0].values(),
        &[Value::Text(key.to_string()), Value::Json(json!(value)),]
    );
}

#[tokio::test]
async fn observe_emits_when_another_engine_commits() {
    let (observer_engine, writer_engine) = open_two_engines().await;
    let observer_session = observer_engine
        .open_workspace_session()
        .await
        .expect("observer session should open");
    let writer_session = writer_engine
        .open_workspace_session()
        .await
        .expect("writer session should open");
    let mut events = observe_key(&observer_session, "mutation-revision-external");

    let initial = next_event(&mut events, "initial empty snapshot").await;
    assert_eq!(initial.sequence, 0);
    assert!(initial.rows.is_empty());

    writer_session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('mutation-revision-external', 'v0')",
            &[],
        )
        .await
        .expect("external engine insert should commit");

    let update = next_event(&mut events, "external engine commit").await;
    assert_eq!(update.sequence, 1);
    assert_key_value_row(&update, "mutation-revision-external", "v0");
}

#[tokio::test]
async fn observe_external_transaction_emits_only_after_commit() {
    let (observer_engine, writer_engine) = open_two_engines().await;
    let observer_session = observer_engine
        .open_workspace_session()
        .await
        .expect("observer session should open");
    let writer_session = writer_engine
        .open_workspace_session()
        .await
        .expect("writer session should open");
    let mut events = observe_key(&observer_session, "mutation-revision-transaction");

    let initial = next_event(&mut events, "initial empty snapshot").await;
    assert!(initial.rows.is_empty());

    let mut transaction = writer_session
        .begin_transaction()
        .await
        .expect("external transaction should open");
    transaction
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('mutation-revision-transaction', 'v0')",
            &[],
        )
        .await
        .expect("external transaction write should stage");

    expect_no_event(&mut events, "external staged write before commit").await;

    transaction
        .commit()
        .await
        .expect("external transaction should commit");

    let update = next_event(&mut events, "external transaction commit").await;
    assert_eq!(update.sequence, 1);
    assert_key_value_row(&update, "mutation-revision-transaction", "v0");
}

#[tokio::test]
async fn observe_external_rollback_does_not_emit() {
    let (observer_engine, writer_engine) = open_two_engines().await;
    let observer_session = observer_engine
        .open_workspace_session()
        .await
        .expect("observer session should open");
    let writer_session = writer_engine
        .open_workspace_session()
        .await
        .expect("writer session should open");
    let mut events = observe_key(&observer_session, "mutation-revision-rollback");

    let initial = next_event(&mut events, "initial empty snapshot").await;
    assert!(initial.rows.is_empty());

    let mut transaction = writer_session
        .begin_transaction()
        .await
        .expect("external transaction should open");
    transaction
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('mutation-revision-rollback', 'v0')",
            &[],
        )
        .await
        .expect("external transaction write should stage");
    transaction
        .rollback()
        .await
        .expect("external transaction should roll back");

    expect_no_event(&mut events, "external transaction rollback").await;
}

#[tokio::test]
async fn observe_external_writes_can_coalesce_to_latest_snapshot() {
    let (observer_engine, writer_engine) = open_two_engines().await;
    let observer_session = observer_engine
        .open_workspace_session()
        .await
        .expect("observer session should open");
    let writer_session = writer_engine
        .open_workspace_session()
        .await
        .expect("writer session should open");
    let mut events = observe_key(&observer_session, "mutation-revision-coalesce");

    let initial = next_event(&mut events, "initial empty snapshot").await;
    assert!(initial.rows.is_empty());

    writer_session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('mutation-revision-coalesce', 'v0')",
            &[],
        )
        .await
        .expect("external insert should commit");
    writer_session
        .execute(
            "UPDATE lix_key_value SET value = 'v1' WHERE key = 'mutation-revision-coalesce'",
            &[],
        )
        .await
        .expect("external update should commit");

    let update = next_event(&mut events, "coalesced external writes").await;
    assert_eq!(update.sequence, 1);
    assert_key_value_row(&update, "mutation-revision-coalesce", "v1");
}

#[tokio::test]
async fn observe_emits_when_independently_opened_sqlite_backend_commits() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let path = tempdir.path().join("repo.sqlite");
    Engine::initialize(SqliteBackend::open(&path).expect("init backend should open"))
        .await
        .expect("backend should initialize");
    let observer_engine =
        Engine::new(SqliteBackend::open(&path).expect("observer sqlite backend should open"))
            .await
            .expect("observer engine should open");
    let writer_engine =
        Engine::new(SqliteBackend::open(&path).expect("writer sqlite backend should open"))
            .await
            .expect("writer engine should open");
    let observer_session = observer_engine
        .open_workspace_session()
        .await
        .expect("observer session should open");
    let writer_session = writer_engine
        .open_workspace_session()
        .await
        .expect("writer session should open");
    let mut events = observe_key(&observer_session, "mutation-revision-sqlite");

    let initial = next_event(&mut events, "initial empty sqlite snapshot").await;
    assert!(initial.rows.is_empty());

    writer_session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('mutation-revision-sqlite', 'v0')",
            &[],
        )
        .await
        .expect("external sqlite insert should commit");

    let update = next_event(&mut events, "external sqlite backend commit").await;
    assert_eq!(update.sequence, 1);
    assert_key_value_row(&update, "mutation-revision-sqlite", "v0");
}
