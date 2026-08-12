use lix::ExecuteResult;
use lix::Value;
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
            plan.contains("partial_filters=[pushdown_note.kind = Utf8(\"todo\")]"),
            "payload equality should reach the table scan while retaining a DataFusion residual:\n{plan}"
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

        assert_rows_eq(result, vec![vec![Value::Json(json!(["n1"]).into())]]);
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

simulation_test!(
    entity_point_read_order_by_pk_elides_physical_sort,
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
        insert_pushdown_note(&session, "n1", "todo", "First", "1", "NULL").await;
        insert_pushdown_note(&session, "n2", "done", "Second", "2", "NULL").await;

        // A fully-applied primary-key equality pins the sort column to one
        // literal, so the ORDER BY over the at-most-one matching row must not
        // build a physical sort operator.
        for point_sql in [
            "SELECT id, title FROM pushdown_note WHERE id = 'n2' ORDER BY id",
            "SELECT id, title FROM pushdown_note WHERE id IN ('n2') ORDER BY id",
        ] {
            let explain = session
                .execute(&format!("EXPLAIN {point_sql}"), &[])
                .await
                .expect("EXPLAIN should succeed");
            let plan = explain_plan_text(&explain);
            assert!(
                !plan.contains("SortExec"),
                "point read with ORDER BY on the pinned pk must elide the sort:\n{plan}"
            );

            let result = session
                .execute(point_sql, &[])
                .await
                .expect("point read should succeed");
            assert_rows_eq(
                result,
                vec![vec![
                    Value::Text("n2".to_string()),
                    Value::Text("Second".to_string()),
                ]],
            );
        }
    }
);

simulation_test!(
    entity_multi_key_and_unpinned_order_by_keep_physical_sort,
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
        insert_pushdown_note(&session, "n1", "todo", "First", "1", "NULL").await;
        insert_pushdown_note(&session, "n2", "done", "Second", "2", "NULL").await;

        // A multi-value IN pins nothing: ordering across the matched keys is
        // real work and the sort must stay.
        let multi_sql = "SELECT id FROM pushdown_note WHERE id IN ('n2', 'n1') ORDER BY id";
        let explain = session
            .execute(&format!("EXPLAIN {multi_sql}"), &[])
            .await
            .expect("EXPLAIN should succeed");
        let plan = explain_plan_text(&explain);
        assert!(
            plan.contains("SortExec"),
            "multi-key IN with ORDER BY must keep its physical sort:\n{plan}"
        );
        let result = session
            .execute(multi_sql, &[])
            .await
            .expect("multi-key read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![Value::Text("n1".to_string())],
                vec![Value::Text("n2".to_string())],
            ],
        );

        // An inexact residual predicate proves nothing about the scan output;
        // ordering by an unpinned column must keep its physical sort.
        let range_sql = "SELECT id FROM pushdown_note WHERE score > 0 ORDER BY id";
        let explain = session
            .execute(&format!("EXPLAIN {range_sql}"), &[])
            .await
            .expect("EXPLAIN should succeed");
        let plan = explain_plan_text(&explain);
        assert!(
            plan.contains("SortExec"),
            "range-filtered ORDER BY must keep its physical sort:\n{plan}"
        );
        let result = session
            .execute(range_sql, &[])
            .await
            .expect("range read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![Value::Text("n1".to_string())],
                vec![Value::Text("n2".to_string())],
            ],
        );
    }
);

async fn register_pushdown_note_schema(
    session: &crate::support::simulation_test::engine::SimSession,
) {
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"pushdown_note\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"kind\":{\"type\":\"string\"},\"title\":{\"type\":\"string\"},\"score\":{\"type\":\"number\"},\"optional\":{\"type\":[\"string\",\"null\"]}},\"required\":[\"id\",\"kind\",\"title\",\"score\"],\"additionalProperties\":false}'),\
             false,\
             false\
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
