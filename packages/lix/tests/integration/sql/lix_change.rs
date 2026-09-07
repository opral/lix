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
            "SELECT row_pk, schema_key, snapshot_content \
             FROM lix_change \
             WHERE schema_key = 'lix_key_value' AND row_pk = CAST('[\"change-query\"]' AS JSONB)",
            &[],
        )
        .await
        .expect("lix_change should read");
    let rows = result;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.rows()[0].values()[0],
        Value::Jsonb(json!(["change-query"]).into())
    );
    assert_eq!(
        &rows.rows()[0].values()[1..],
        &[
            Value::Text("lix_key_value".to_string()),
            Value::Jsonb(json!({"key": "change-query", "value": "one"}).into()),
        ]
    );
});

simulation_test!(
    lix_change_exposes_tracked_schema_identity_and_not_public_row_refs,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        // Bootstrap may retain ref facts; ordinary writes must not append head movements.
        let initial_ref_changes = session.execute(
            "SELECT count(*) AS n FROM lix_change WHERE schema_key = 'lix_branch_ref'", &[],
        ).await.unwrap().rows()[0].get::<i64>("n").unwrap();
        let file_id = "01950000-0000-7000-8000-000000000041";
        session.execute(
        "INSERT INTO lix_file (id, path, content) VALUES ($1, '/record.txt', CAST('hello' AS BYTEA))",
        &[Value::Text(file_id.into())],
    ).await.unwrap();
        let changes = session.execute(
        "SELECT schema_key, row_pk, file_id, snapshot_content FROM lix_change WHERE file_id = $1",
        &[Value::Text(file_id.into())],
    ).await.unwrap();
        assert_eq!(changes.column_types()[1], lix::ResultColumnType::Jsonb);
        let descriptor = changes
            .rows()
            .iter()
            .find(|row| row.get::<String>("schema_key").unwrap() == "lix_file_descriptor")
            .expect("descriptor change should remain visible");
        assert_eq!(
            descriptor.values()[1],
            Value::Jsonb(json!([file_id]).into())
        );
        assert_eq!(descriptor.values()[2], Value::Text(file_id.into()));
        let Value::Jsonb(snapshot) = &descriptor.values()[3] else {
            panic!("expected snapshot");
        };
        assert_eq!(snapshot.to_value()["id"], file_id);
        assert_eq!(snapshot.to_value()["name"], "record.txt");

        let columns = session.execute(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'lix_change' AND column_name IN ('row_pk', 'row_ref')",
        &[],
    ).await.unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(
            columns.rows()[0].get::<String>("column_name").unwrap(),
            "row_pk"
        );
        assert!(
            session
                .execute("SELECT row_ref FROM lix_change", &[])
                .await
                .is_err()
        );
        let refs = session
            .execute(
                "SELECT row_ref FROM lix_diff('lix_file') WHERE id = $1",
                &[Value::Text(file_id.into())],
            )
            .await
            .unwrap();
        assert_eq!(refs.column_types()[0], lix::ResultColumnType::RowRef);
        assert_eq!(
            session
                .execute(
                    "SELECT count(*) AS n FROM lix_change WHERE schema_key = 'lix_branch_ref'",
                    &[]
                )
                .await
                .unwrap()
                .rows()[0]
                .get::<i64>("n")
                .unwrap(),
            initial_ref_changes
        );
    }
);

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
    lix_change_row_pk_is_lossless_for_composite_primary_keys,
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
                "SELECT row_pk, file_id \
                 FROM lix_change \
                 WHERE schema_key = 'engine_composite_message'",
                &[],
            )
            .await
            .expect("lix_change should expose the schema record identity");

        assert_eq!(result.len(), 1);
        assert_eq!(
            result.rows()[0].values()[0],
            Value::Jsonb(json!(["welcome.title", "en"]).into())
        );
        assert_eq!(
            result.rows()[0].values()[1],
            Value::Text("01950000-0000-7000-8000-000000000031".into())
        );
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
            error.message.contains("must use text, uuid, or int8"),
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
