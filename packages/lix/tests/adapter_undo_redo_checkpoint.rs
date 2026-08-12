use lix::Value;
use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;

const A_KEY: &str = "checkpointed-a";
const B_KEY: &str = "undo-target-b";

async fn value<S>(session: &SessionContext<S>, key: &str) -> Option<String>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = session
        .execute(
            "SELECT value FROM lix_key_value WHERE key = $1",
            &[Value::Text(key.to_string())],
        )
        .await
        .expect("key-value row reads");
    result
        .rows()
        .first()
        .and_then(|row| row.get::<Value>("value").ok())
        .and_then(|value| match value {
            Value::Text(value) => Some(value),
            Value::Json(value) => value.as_json_string(),
            _ => None,
        })
}

async fn assert_values<S>(session: &SessionContext<S>, a: &str, b: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    assert_eq!(value(session, A_KEY).await.as_deref(), Some(a));
    assert_eq!(value(session, B_KEY).await.as_deref(), Some(b));
}

pub async fn stage_checkpointed_a_and_undo_b<S>(engine: &Engine<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let session = engine
        .open_session()
        .await
        .expect("session opens");
    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, 'a0'), ($2, 'b0')",
            &[
                Value::Text(A_KEY.to_string()),
                Value::Text(B_KEY.to_string()),
            ],
        )
        .await
        .expect("seed rows commit");
    session
        .create_checkpoint()
        .await
        .expect("seed checkpoint commits");
    session
        .execute(
            "UPDATE lix_key_value SET value = 'a1' WHERE key = $1",
            &[Value::Text(A_KEY.to_string())],
        )
        .await
        .expect("A commits");
    session
        .create_checkpoint()
        .await
        .expect("A checkpoint commits");
    session
        .execute(
            "UPDATE lix_key_value SET value = 'b1' WHERE key = $1",
            &[Value::Text(B_KEY.to_string())],
        )
        .await
        .expect("B commits");
    assert_values(&session, "a1", "b1").await;

    session.undo().await.expect("B undoes");
    assert_values(&session, "a1", "b0").await;

    session
        .active_branch_id()
        .await
        .expect("active branch resolves")
}

pub async fn assert_cold_undo_then_redo<S>(engine: &Engine<S>, branch_id: String)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let session = engine
        .open_session_at(branch_id)
        .await
        .expect("cold session opens after undo");
    assert_values(&session, "a1", "b0").await;
    session.redo().await.expect("B redoes after cold reopen");
    assert_values(&session, "a1", "b1").await;
}

pub async fn assert_cold_redo<S>(engine: &Engine<S>, branch_id: String)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let session = engine
        .open_session_at(branch_id)
        .await
        .expect("cold session opens after redo");
    assert_values(&session, "a1", "b1").await;
}
