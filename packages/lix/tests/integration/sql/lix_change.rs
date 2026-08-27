use std::collections::BTreeSet;

use lix::Value;
use serde_json::json;

use super::select_rows;

simulation_test!(lix_change_queries_durable_change_facts, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
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
            "SELECT row_ref, schema_key, snapshot_content \
             FROM lix_change \
             WHERE row_ref = lix_row_ref('lix_key_value', 'change-query')",
            &[],
        )
        .await
        .expect("lix_change should read");
    let rows = result;
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows.rows()[0].values()[0], Value::RowRef(_)));
    assert_eq!(
        &rows.rows()[0].values()[1..],
        &[
            Value::Text("lix_key_value".to_string()),
            Value::Jsonb(json!({"key": "change-query", "value": "one"}).into()),
        ]
    );
});

simulation_test!(lix_change_includes_commit_changes, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('commit-change-query', 'one')",
            &[],
        )
        .await
        .expect("tracked write should succeed");

    let result = session
        .execute(
            "SELECT schema_key FROM lix_change WHERE schema_key = 'lix_commit' LIMIT 1",
            &[],
        )
        .await
        .expect("lix_change should include commit changes");

    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("lix_commit".to_string())]
    );
});

simulation_test!(
    lix_change_row_ref_is_lossless_for_composite_primary_keys,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_composite_message\",\"columns\":[{\"name\":\"key\",\"type\":\"text\",\"nullable\":false},{\"name\":\"locale\",\"type\":\"text\",\"nullable\":false},{\"name\":\"text\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"key\",\"locale\"]}' AS JSONB),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("composite schema insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('01950000-0000-7000-8000-000000000031', '/messages.json', CAST('{}' AS BYTEA))",
                &[],
            )
            .await
            .expect("owning file insert should succeed");
        session
            .execute(
                "INSERT INTO engine_composite_message (key, locale, text, lixcol_file_id) \
                 VALUES ('welcome.title', 'en', 'Welcome', '01950000-0000-7000-8000-000000000031')",
                &[],
            )
            .await
            .expect("file-owned composite row insert should succeed");

        let result = session
            .execute(
                "SELECT row_ref, \
                        row_ref = lix_row_ref('engine_composite_message', 'welcome.title', 'en') AS expected_ref \
                 FROM lix_change \
                 WHERE schema_key = 'engine_composite_message'",
                &[],
            )
            .await
            .expect("lix_change should expose the semantic composite row_ref");

        assert_eq!(result.len(), 1);
        assert!(matches!(result.rows()[0].values()[0], Value::RowRef(_)));
        assert_eq!(result.rows()[0].values()[1], Value::Boolean(true));
    }
);

simulation_test!(
    lix_change_rejects_float_primary_key_schemas,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_numeric_message\",\"columns\":[{\"name\":\"id\",\"type\":\"float8\",\"nullable\":false},{\"name\":\"text\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect_err("numeric primary-key schema should be rejected");

        assert_eq!(error.code, lix::LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error
                .message
                .contains("must use text, uuid, or int8"),
            "error should explain unsupported primary-key schema: {error:?}"
        );
    }
);

simulation_test!(
    lix_change_sql_surface_matches_builtin_schema,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
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
                .open_session()
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
        "../../../src/schema/builtin/lix_change.json"
    ))
    .expect("builtin lix_change schema should parse");
    schema
        .get("columns")
        .and_then(serde_json::Value::as_array)
        .expect("builtin lix_change schema should define columns")
        .iter()
        .map(|column| column["name"].as_str().expect("column name").to_string())
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
