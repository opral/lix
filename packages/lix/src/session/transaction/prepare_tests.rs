
use crate::Value;
use crate::sql2::validate_sql_preparation as admitted_update;
#[test]
fn preparation_rejects_volatile_and_unsupported_writes_before_execution() {
    for sql in [
        "UPDATE lix_key_value SET value=uuidv7() WHERE key='x'",
        "UPDATE lix_key_value SET key='new' WHERE key='old'",
        "DELETE FROM lix_key_value WHERE key='x'",
        "INSERT INTO lix_key_value(key,value) VALUES('x','v')",
        "SELECT uuidv7()",
        "WITH x AS (SELECT 1) SELECT * FROM x",
    ] {
        assert_eq!(
            admitted_update(sql).unwrap_err().code,
            "LIX_SQL_PREPARATION_UNSUPPORTED",
            "{sql}"
        );
    }
    assert!(admitted_update("UPDATE lix_key_value SET value=$1 WHERE key=$2").unwrap());
    assert!(admitted_update("UPDATE lix_file SET content=$1 WHERE id=$2").unwrap());
    assert!(!admitted_update("SELECT value FROM lix_key_value WHERE key=$1").unwrap());
}
#[tokio::test]
async fn successful_and_rejected_preparation_leave_storage_revision_unchanged() {
    let lix = crate::open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value(key,value) VALUES('prepare-preserved','before')",
        &[],
    )
    .await
    .unwrap();
    let storage = lix.storage_adapter();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let before = crate::storage_adapter::load_repository_mutation_revision(&read)
        .await
        .unwrap();
    drop(read);
    lix.prepare(
        "UPDATE lix_key_value SET value='after' WHERE key='prepare-preserved'",
        &[],
    )
    .await
    .unwrap();
    lix.prepare(
        "SELECT value FROM lix_key_value WHERE key='prepare-preserved'",
        &[],
    )
    .await
    .unwrap();
    assert!(
        lix.prepare(
            "UPDATE lix_key_value SET value=uuidv7() WHERE key='prepare-preserved'",
            &[]
        )
        .await
        .is_err()
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert_eq!(
        crate::storage_adapter::load_repository_mutation_revision(&read)
            .await
            .unwrap(),
        before
    );
    drop(read);
    assert_eq!(
        lix.execute(
            "SELECT value FROM lix_key_value WHERE key='prepare-preserved'",
            &[]
        )
        .await
        .unwrap()
        .rows()[0]
            .get::<serde_json::Value>("value")
            .unwrap(),
        serde_json::json!("before")
    );
    lix.close().await.unwrap();
}
#[tokio::test]
async fn deterministic_preparation_does_not_consume_durable_identity_sequence() {
    let lix = crate::open_lix().await.unwrap();
    lix.execute("INSERT INTO lix_key_value(key,value,lixcol_global,lixcol_untracked) VALUES('lix_deterministic_mode',CAST($1 AS JSONB),true,true)", &[Value::Text(r#"{"enabled":true}"#.into())]).await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value(key,value) VALUES('deterministic-preparation','before')",
        &[],
    )
    .await
    .unwrap();
    let sequence = "SELECT value FROM lix_key_value WHERE key='lix_deterministic_sequence_number' AND lixcol_global AND lixcol_untracked";
    let before = lix.execute(sequence, &[]).await.unwrap().rows()[0]
        .get::<serde_json::Value>("value")
        .unwrap();
    for _ in 0..3 {
        lix.prepare(
            "UPDATE lix_key_value SET value='prospective' WHERE key='deterministic-preparation'",
            &[],
        )
        .await
        .unwrap();
    }
    assert!(
        lix.prepare(
            "UPDATE lix_key_value SET value=uuidv7() WHERE key='deterministic-preparation'",
            &[]
        )
        .await
        .is_err()
    );
    assert_eq!(
        lix.execute(sequence, &[]).await.unwrap().rows()[0]
            .get::<serde_json::Value>("value")
            .unwrap(),
        before
    );
    lix.execute(
        "UPDATE lix_key_value SET value='actual' WHERE key='deterministic-preparation'",
        &[],
    )
    .await
    .unwrap();
    assert_ne!(
        lix.execute(sequence, &[]).await.unwrap().rows()[0]
            .get::<serde_json::Value>("value")
            .unwrap(),
        before
    );
    lix.close().await.unwrap();
}
