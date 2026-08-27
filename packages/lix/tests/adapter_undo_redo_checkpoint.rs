use lix::storage::Storage;
use lix::{Lix, SwitchBranchOptions, Value};

const A_KEY: &str = "checkpointed-a";
const B_KEY: &str = "undo-target-b";

async fn value<S>(lix: &Lix<S>, key: &str) -> Option<String>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
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
            Value::Jsonb(value) => value.as_json_string(),
            _ => None,
        })
}

async fn assert_values<S>(lix: &Lix<S>, a: &str, b: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    assert_eq!(value(lix, A_KEY).await.as_deref(), Some(a));
    assert_eq!(value(lix, B_KEY).await.as_deref(), Some(b));
}

pub async fn stage_checkpointed_a_and_undo_b<S>(lix: &Lix<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, 'a0'), ($2, 'b0')",
        &[
            Value::Text(A_KEY.to_string()),
            Value::Text(B_KEY.to_string()),
        ],
    )
    .await
    .expect("seed rows commit");
    lix.execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("seed checkpoint commits");
    lix.execute(
        "UPDATE lix_key_value SET value = 'a1' WHERE key = $1",
        &[Value::Text(A_KEY.to_string())],
    )
    .await
    .expect("A commits");
    lix.execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("A checkpoint commits");
    lix.execute(
        "UPDATE lix_key_value SET value = 'b1' WHERE key = $1",
        &[Value::Text(B_KEY.to_string())],
    )
    .await
    .expect("B commits");
    assert_values(lix, "a1", "b1").await;

    lix.undo().await.expect("B undoes");
    assert_values(lix, "a1", "b0").await;

    lix.active_branch_id()
        .await
        .expect("active branch resolves")
}

pub async fn assert_cold_undo_then_redo<S>(lix: &Lix<S>, branch_id: String)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.switch_branch(SwitchBranchOptions { branch_id })
        .await
        .expect("cold repository switches to the branch after undo");
    assert_values(lix, "a1", "b0").await;
    lix.redo().await.expect("B redoes after cold reopen");
    assert_values(lix, "a1", "b1").await;
}

pub async fn assert_cold_redo<S>(lix: &Lix<S>, branch_id: String)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.switch_branch(SwitchBranchOptions { branch_id })
        .await
        .expect("cold repository switches to the branch after redo");
    assert_values(lix, "a1", "b1").await;
}
