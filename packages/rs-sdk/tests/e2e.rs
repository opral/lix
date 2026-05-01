use lix_rs_sdk::{
    open_lix, CreateVersionOptions, MergeVersionOptions, MergeVersionOutcome, OpenLixOptions,
    SwitchVersionOptions, Value,
};

#[tokio::test]
async fn rs_sdk_open_register_write_query_version_and_merge_flow() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    let main_version_id = lix.active_version_id().await.unwrap();

    register_crm_task_schema(&lix).await;

    lix.execute(
        "INSERT INTO crm_task (id, title, done) VALUES ($1, $2, $3)",
        &[
            Value::Text("task-1".to_string()),
            Value::Text("Draft RS SDK flow".to_string()),
            Value::Boolean(false),
        ],
    )
    .await
    .unwrap();

    assert_eq!(task_done(&lix, "task-1").await, false);

    let draft = lix
        .create_version(CreateVersionOptions {
            id: Some("draft-version".to_string()),
            name: "Draft".to_string(),
        })
        .await
        .unwrap();

    lix.switch_version(SwitchVersionOptions {
        version_id: draft.version_id.clone(),
    })
    .await
    .unwrap();

    lix.execute(
        "UPDATE crm_task SET done = $1 WHERE id = $2",
        &[Value::Boolean(true), Value::Text("task-1".to_string())],
    )
    .await
    .unwrap();

    assert_eq!(task_done(&lix, "task-1").await, true);

    lix.switch_version(SwitchVersionOptions {
        version_id: main_version_id.clone(),
    })
    .await
    .unwrap();

    assert_eq!(task_done(&lix, "task-1").await, false);

    let merge = lix
        .merge_version(MergeVersionOptions {
            source_version_id: draft.version_id,
        })
        .await
        .unwrap();

    assert_eq!(merge.outcome, MergeVersionOutcome::MergeCommitted);
    assert_eq!(merge.target_version_id, main_version_id);
    assert!(merge.applied_change_count > 0);
    assert_eq!(task_done(&lix, "task-1").await, true);

    lix.close().await.unwrap();
}

async fn register_crm_task_schema(lix: &lix_rs_sdk::Lix) {
    let schema = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "x-lix-key": "crm_task",
        "x-lix-version": "1",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "title", "done"],
        "properties": {
            "id": { "type": "string" },
            "title": { "type": "string" },
            "done": { "type": "boolean" }
        },
        "additionalProperties": false
    }"#;

    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
        &[Value::Text(schema.to_string())],
    )
    .await
    .unwrap();
}

async fn task_done(lix: &lix_rs_sdk::Lix, task_id: &str) -> bool {
    let result = lix
        .execute(
            "SELECT done FROM crm_task WHERE id = $1",
            &[Value::Text(task_id.to_string())],
        )
        .await
        .unwrap();

    let rows = result;
    assert_eq!(rows.len(), 1);

    match rows.rows()[0].values().first() {
        Some(Value::Boolean(done)) => *done,
        value => panic!("expected boolean done value, got {value:?}"),
    }
}
