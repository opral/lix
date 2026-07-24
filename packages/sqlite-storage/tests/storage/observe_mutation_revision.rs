use std::time::Duration;

use lix_engine::{Engine, ObserveEvent, ObserveEvents, Storage, Value};
use lix_sqlite_storage::SQLite;
use serde_json::json;

const NEXT_TIMEOUT: Duration = Duration::from_secs(1);
const KEY_VALUE_SQL: &str = "SELECT key, value FROM lix_key_value WHERE key = $1 ORDER BY key";

#[tokio::test]
async fn observe_emits_when_independently_opened_sqlite_commits() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let path = tempdir.path().join("repo.sqlite");
    Engine::initialize(SQLite::open(&path).expect("init storage should open"))
        .await
        .expect("storage should initialize");
    let observer_engine =
        Engine::new(SQLite::open(&path).expect("observer sqlite storage should open"))
            .await
            .expect("observer engine should open");
    let writer_engine =
        Engine::new(SQLite::open(&path).expect("writer sqlite storage should open"))
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

    let update = next_event(&mut events, "external sqlite storage commit").await;
    assert_eq!(update.sequence, 1);
    assert_key_value_row(&update, "mutation-revision-sqlite", "v0");
}

fn observe_key<StorageImpl>(
    session: &lix_engine::SessionContext<StorageImpl>,
    key: &str,
) -> ObserveEvents<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    session
        .observe(KEY_VALUE_SQL, &[Value::Text(key.to_string())])
        .expect("observe should open")
}

async fn next_event<StorageImpl>(
    events: &mut ObserveEvents<StorageImpl>,
    label: &str,
) -> ObserveEvent
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    tokio::time::timeout(NEXT_TIMEOUT, events.next())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for observe event: {label}"))
        .unwrap_or_else(|error| panic!("observe next failed for {label}: {error:?}"))
        .unwrap_or_else(|| panic!("observe closed before event: {label}"))
}

fn assert_key_value_row(event: &ObserveEvent, key: &str, value: &str) {
    assert_eq!(event.rows.columns(), &["key", "value"]);
    assert_eq!(event.rows.len(), 1);
    assert_eq!(
        event.rows.rows()[0].values(),
        &[Value::Text(key.to_string()), Value::Json(json!(value)),]
    );
}
