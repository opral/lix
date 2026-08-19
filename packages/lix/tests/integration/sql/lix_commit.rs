use std::collections::BTreeSet;

use lix::{CreateBranchOptions, LixError, Value};

use super::select_rows;

simulation_test!(
    lix_commit_surfaces_expose_commits_and_edges,
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
                "INSERT INTO lix_key_value (key, value) VALUES ('commit-surface', 'one')",
                &[],
            )
            .await
            .expect("first tracked write should succeed");
        let first_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("branch head should load")
            .expect("branch head should exist");

        session
            .execute(
                "UPDATE lix_key_value SET value = 'two' WHERE key = 'commit-surface'",
                &[],
            )
            .await
            .expect("second tracked write should succeed");
        let second_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("branch head should load")
            .expect("branch head should exist");

        let commit_rows = select_rows(
            &session,
            &format!(
                "SELECT id, lixcol_global, lixcol_untracked \
                 FROM lix_commit WHERE id = '{second_head}'"
            ),
        )
        .await;
        assert_eq!(
            commit_rows,
            vec![vec![
                Value::Text(second_head.clone()),
                Value::Boolean(true),
                Value::Boolean(false),
            ]]
        );

        let edge_rows = select_rows(
            &session,
            &format!(
                "SELECT parent_id, child_id, parent_order, lixcol_global, lixcol_untracked \
                 FROM lix_commit_edge WHERE child_id = '{second_head}'"
            ),
        )
        .await;
        assert_eq!(
            edge_rows,
            vec![vec![
                Value::Text(first_head.clone()),
                Value::Text(second_head.clone()),
                Value::Integer(0),
                Value::Boolean(true),
                Value::Boolean(false),
            ]]
        );

        for table in ["lix_commit_by_branch", "lix_commit_edge_by_branch"] {
            let error = session
                .execute(&format!("SELECT * FROM {table}"), &[])
                .await
                .expect_err("retired by-branch commit surfaces must fail closed");
            assert_eq!(error.code, LixError::CODE_TABLE_NOT_FOUND);
        }
    }
);

simulation_test!(
    lix_commit_is_plain_global_row_not_active_reachability_view,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('main-only', 'main')",
            &[],
        )
        .await
        .expect("main write should succeed");

        main.create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-000000000006".to_string()),
            name: "Commit branch".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("branch branch should be created");

        let branch = sim.wrap_session(
            engine
                .open_session_at("01930000-0000-7000-8000-000000000006")
                .await
                .expect("branch session should open"),
            &engine,
        );
        branch
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('6272616e-6368-8d6f-8e6c-790000000000', 'branch')",
                &[],
            )
            .await
            .expect("branch write should succeed");

        let branch_head = engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-000000000006")
            .await
            .expect("branch head should load")
            .expect("branch head should exist");

        let main_commit_rows = select_rows(
            &main,
            &format!("SELECT id FROM lix_commit WHERE id = '{branch_head}'"),
        )
        .await;
        let branch_commit_rows = select_rows(
            &branch,
            &format!("SELECT id FROM lix_commit WHERE id = '{branch_head}'"),
        )
        .await;
        assert_eq!(
            main_commit_rows, branch_commit_rows,
            "lix_commit should not depend on the active branch"
        );
        assert_eq!(
            main_commit_rows,
            vec![vec![Value::Text(branch_head.clone())]]
        );

        let main_edge_rows = select_rows(
            &main,
            &format!("SELECT child_id FROM lix_commit_edge WHERE child_id = '{branch_head}'"),
        )
        .await;
        let branch_edge_rows = select_rows(
            &branch,
            &format!("SELECT child_id FROM lix_commit_edge WHERE child_id = '{branch_head}'"),
        )
        .await;
        assert_eq!(
            main_edge_rows, branch_edge_rows,
            "derived commit surfaces should also expose global commit-derived rows"
        );
        assert_eq!(main_edge_rows, vec![vec![Value::Text(branch_head)]]);
    }
);


simulation_test!(
    lix_commit_surfaces_match_canonical_schema_definitions,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (schema_key, tables) in [
            ("lix_commit", vec!["lix_commit"]),
            ("lix_commit_edge", vec!["lix_commit_edge"]),
        ] {
            let schema_properties = builtin_schema_property_names(schema_key);
            for table in tables {
                let surface_columns = non_system_column_names(&session, table).await;
                assert_eq!(
                    surface_columns, schema_properties,
                    "{table} data columns should match {schema_key} properties"
                );
            }
        }
    }
);

simulation_test!(
    lix_commit_surfaces_count_handle_empty_projection,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for table in [
            "lix_commit",
            "lix_commit_edge",
        ] {
            let rows = select_rows(&session, &format!("SELECT count(*) FROM {table}")).await;
            assert_single_count(rows, table);
        }
    }
);

fn assert_single_count(rows: Vec<Vec<Value>>, table: &str) {
    assert_eq!(rows.len(), 1, "{table} should return one count row");
    assert_eq!(rows[0].len(), 1, "{table} should return one count column");
    let Value::Integer(count) = rows[0][0] else {
        panic!(
            "{table} should return an integer count, got {:?}",
            rows[0][0]
        );
    };
    assert!(count >= 0, "{table} count should be non-negative");
}

fn text_value(value: &Value) -> String {
    let Value::Text(value) = value else {
        panic!("expected text value, got {value:?}");
    };
    value.clone()
}

fn builtin_schema_property_names(schema_key: &str) -> BTreeSet<String> {
    let schema = match schema_key {
        "lix_commit" => include_str!("../../../src/schema/builtin/lix_commit.json"),
        "lix_commit_edge" => include_str!("../../../src/schema/builtin/lix_commit_edge.json"),
        other => panic!("unexpected builtin schema key: {other}"),
    };
    let schema = serde_json::from_str::<serde_json::Value>(schema)
        .expect("builtin schema fixture should parse");
    schema
        .get("columns")
        .and_then(serde_json::Value::as_array)
        .expect("builtin schema should define columns")
        .iter()
        .map(|column| column["name"].as_str().expect("column name").to_string())
        .collect::<BTreeSet<_>>()
}

async fn non_system_column_names(
    session: &crate::support::simulation_test::engine::SimSession,
    table_name: &str,
) -> BTreeSet<String> {
    let rows = select_rows(
        session,
        &format!(
            "SELECT column_name \
             FROM information_schema.columns \
             WHERE table_name = '{table_name}'"
        ),
    )
    .await;
    rows.into_iter()
        .map(|row| text_value(&row[0]))
        .filter(|column_name| !column_name.starts_with("lixcol_"))
        .collect()
}
