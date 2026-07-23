use lix_engine::ExecuteResult;
use lix_engine::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    entity_filter_pushdown_plan_smoke_for_payload_equality,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_pushdown_note_schema(&session).await;

        let explain = session
            .execute(
                "EXPLAIN VERBOSE SELECT id FROM pushdown_note WHERE kind = 'todo'",
                &[],
            )
            .await
            .expect("EXPLAIN should succeed");
        let plan = explain_plan_text(&explain);

        assert!(
            plan.contains("TableScan: pushdown_note"),
            "plan should scan pushdown_note:\n{plan}"
        );
        assert!(
            plan.contains("full_filters=[pushdown_note.kind = Utf8(\"todo\")]"),
            "payload equality should be pushed into the table scan:\n{plan}"
        );
    }
);

simulation_test!(
    entity_filter_pushdown_keeps_filter_only_payload_available,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_pushdown_note_schema(&session).await;
        insert_pushdown_note(&session, "n1", "todo", "First", "7", "NULL").await;

        let result = session
            .execute(
                "SELECT lixcol_entity_pk FROM pushdown_note WHERE kind = 'todo'",
                &[],
            )
            .await
            .expect("filter-only payload query should succeed");

        assert_rows_eq(result, vec![vec![Value::Json(json!(["n1"]))]]);
    }
);

simulation_test!(
    entity_filter_pushdown_applies_limit_after_payload_filter,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_pushdown_note_schema(&session).await;
        insert_pushdown_note(&session, "n1", "done", "Already done", "1", "NULL").await;
        insert_pushdown_note(&session, "n2", "todo", "Still todo", "2", "NULL").await;

        let result = session
            .execute(
                "SELECT id FROM pushdown_note WHERE kind = 'todo' ORDER BY id LIMIT 1",
                &[],
            )
            .await
            .expect("filtered LIMIT query should succeed");

        assert_rows_eq(result, vec![vec![Value::Text("n2".to_string())]]);
    }
);

simulation_test!(
    entity_filter_pushdown_preserves_sql_null_equality_semantics,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_pushdown_note_schema(&session).await;
        insert_pushdown_note(&session, "n1", "todo", "Nullable", "1", "NULL").await;

        let equals_null = session
            .execute("SELECT id FROM pushdown_note WHERE optional = NULL", &[])
            .await
            .expect("NULL equality query should succeed");
        assert_rows_eq(equals_null, Vec::<Vec<Value>>::new());

        let in_null = session
            .execute("SELECT id FROM pushdown_note WHERE optional IN (NULL)", &[])
            .await
            .expect("NULL IN query should succeed");
        assert_rows_eq(in_null, Vec::<Vec<Value>>::new());
    }
);

simulation_test!(
    entity_filter_pushdown_preserves_number_equality_semantics,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_pushdown_note_schema(&session).await;
        insert_pushdown_note(&session, "n1", "todo", "Scored", "7", "NULL").await;

        let result = session
            .execute("SELECT id FROM pushdown_note WHERE score = 7.0", &[])
            .await
            .expect("numeric equality query should succeed");

        assert_rows_eq(result, vec![vec![Value::Text("n1".to_string())]]);
    }
);

simulation_test!(
    entity_filter_pushdown_leaves_unsupported_range_as_residual_filter,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_pushdown_note_schema(&session).await;

        let explain = session
            .execute(
                "EXPLAIN VERBOSE SELECT id FROM pushdown_note WHERE score > 5",
                &[],
            )
            .await
            .expect("EXPLAIN should succeed");
        let plan = explain_plan_text(&explain);

        assert!(
            !plan.contains("full_filters=[pushdown_note.score >"),
            "range predicate must not be advertised as exact pushdown:\n{plan}"
        );
        assert!(
            plan.contains("Filter: pushdown_note.score >"),
            "unsupported range predicate should remain as a residual filter:\n{plan}"
        );
    }
);

async fn register_pushdown_note_schema(
    session: &crate::support::simulation_test::engine::SimSession,
) {
    session
        .execute(
            "INSERT INTO lix_schema_definition (definition) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"pushdown_note\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"kind\":{\"type\":\"string\"},\"title\":{\"type\":\"string\"},\"score\":{\"type\":\"number\"},\"optional\":{\"type\":[\"string\",\"null\"]}},\"required\":[\"id\",\"kind\",\"title\",\"score\"],\"additionalProperties\":false}')\
             )",
            &[],
        )
        .await
        .expect("pushdown_note schema should register");
}

async fn insert_pushdown_note(
    session: &crate::support::simulation_test::engine::SimSession,
    id: &str,
    kind: &str,
    title: &str,
    score_json: &str,
    optional_sql: &str,
) {
    session
        .execute(
            &format!(
                "INSERT INTO pushdown_note (id, kind, title, score, optional) \
                 VALUES ('{id}', '{kind}', '{title}', {score_json}, {optional_sql})"
            ),
            &[],
        )
        .await
        .expect("pushdown_note row should insert");
}

fn explain_plan_text(result: &ExecuteResult) -> String {
    result
        .rows()
        .iter()
        .flat_map(|row| row.values().iter())
        .map(|value| match value {
            Value::Text(value) => value.clone(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}
