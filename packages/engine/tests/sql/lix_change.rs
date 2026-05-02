use std::collections::BTreeSet;

use lix_engine::Value;
use serde_json::json;

use super::select_rows;

simulation_test!(lix_change_queries_tracked_changes, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('change-query', 'one')",
            &[],
        )
        .await
        .expect("tracked write should succeed");

    let result = session
        .execute(
            "SELECT entity_id, schema_key, snapshot_content \
             FROM lix_change \
             WHERE entity_id = 'change-query'",
            &[],
        )
        .await
        .expect("lix_change should read");
    let rows = result;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.rows()[0].values(),
        &[
            Value::Text("change-query".to_string()),
            Value::Text("lix_key_value".to_string()),
            Value::Json(json!({"key": "change-query", "value": "one"})),
        ]
    );
});

simulation_test!(
    lix_change_sql_surface_matches_builtin_schema,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        assert_eq!(
            non_system_column_names(&session, "lix_change").await,
            builtin_schema_property_names(),
        );
    }
);

simulation_test!(
    lix_change_count_handles_empty_projection,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let rows = select_rows(&session, "SELECT count(*) FROM lix_change").await;
        assert_single_count(rows);
    }
);

fn assert_single_count(rows: Vec<Vec<Value>>) {
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 1);
    let Value::Integer(count) = rows[0][0] else {
        panic!("expected integer count, got {:?}", rows[0][0]);
    };
    assert!(count >= 0);
}

fn builtin_schema_property_names() -> BTreeSet<String> {
    let schema = serde_json::from_str::<serde_json::Value>(include_str!(
        "../../src/schema/builtin/lix_change.json"
    ))
    .expect("builtin lix_change schema should parse");
    schema
        .get("properties")
        .and_then(serde_json::Value::as_object)
        .expect("builtin lix_change schema should define properties")
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>()
}

async fn non_system_column_names(
    session: &crate::support::simulation_test::engine::SimSession,
    table_name: &str,
) -> BTreeSet<String> {
    let result = session
        .execute(
            &format!(
                "SELECT column_name \
                 FROM information_schema.columns \
                 WHERE table_name = '{table_name}'"
            ),
            &[],
        )
        .await
        .expect("information_schema.columns should read");
    result
        .rows()
        .iter()
        .map(|row| {
            let Value::Text(column_name) = &row.values()[0] else {
                panic!("expected text column name, got {:?}", row.values()[0]);
            };
            column_name.clone()
        })
        .filter(|column_name| !column_name.starts_with("lixcol_"))
        .collect()
}
