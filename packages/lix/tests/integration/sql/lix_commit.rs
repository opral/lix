use std::collections::BTreeSet;

use lix::{CreateBranchOptions, LixError, MergeBranchOptions, Value};

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
    lix_commit_ancestry_defaults_to_active_head_and_accepts_explicit_anchor,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let initial_commit_id = sim.initial_commit_id().to_string();

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('ancestry', 'one')",
                &[],
            )
            .await
            .expect("first commit should succeed");
        let first_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head should load")
            .expect("head should exist");
        session
            .execute(
                "UPDATE lix_key_value SET value = 'two' WHERE key = 'ancestry'",
                &[],
            )
            .await
            .expect("second commit should succeed");
        let second_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head should load")
            .expect("head should exist");

        assert_eq!(
            select_rows(
                &session,
                "SELECT commit_id, depth FROM lix_commit_ancestry() ORDER BY depth, commit_id",
            )
            .await,
            vec![
                vec![Value::Text(second_head.clone()), Value::Integer(0)],
                vec![Value::Text(first_head.clone()), Value::Integer(1)],
                vec![Value::Text(initial_commit_id.clone()), Value::Integer(2)],
            ]
        );

        let explicit = session
            .execute(
                "SELECT commit_id, depth FROM lix_commit_ancestry($1) ORDER BY depth, commit_id",
                &[Value::Text(first_head.clone())],
            )
            .await
            .expect("parameterized explicit ancestry should read");
        assert_eq!(
            explicit
                .rows()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![Value::Text(first_head.clone()), Value::Integer(0)],
                vec![Value::Text(initial_commit_id), Value::Integer(1)],
            ]
        );

        session
            .execute(
                "INSERT INTO lix_restore (commit_id) VALUES ($1)",
                &[Value::Text(first_head.clone())],
            )
            .await
            .expect("restore should succeed");
        let restored = select_rows(
            &session,
            "SELECT commit_id, depth FROM lix_commit_ancestry() ORDER BY depth, commit_id",
        )
        .await;
        assert_eq!(
            restored[0],
            vec![Value::Text(first_head.clone()), Value::Integer(0)]
        );
        assert!(
            restored
                .iter()
                .all(|row| row[0] != Value::Text(second_head.clone())),
            "zero-argument ancestry must stop exposing an abandoned descendant after restore"
        );

        let function_contract = select_rows(
            &session,
            "SELECT argument_signature, result_column, data_type, is_nullable \
             FROM information_schema.table_functions \
             WHERE function_name = 'lix_commit_ancestry' \
             ORDER BY ordinal_position",
        )
        .await;
        assert_eq!(
            function_contract,
            vec![
                vec![
                    Value::Text("() | (commit_id TEXT)".to_string()),
                    Value::Text("commit_id".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("NO".to_string()),
                ],
                vec![
                    Value::Text("() | (commit_id TEXT)".to_string()),
                    Value::Text("depth".to_string()),
                    Value::Text("BIGINT".to_string()),
                    Value::Text("NO".to_string()),
                ],
            ]
        );

        for sql in [
            "SELECT * FROM lix_commit_ancestry(NULL)",
            "SELECT * FROM lix_commit_ancestry(1)",
            "SELECT * FROM lix_commit_ancestry('a', 'b')",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("invalid ancestry call should fail closed");
            assert_eq!(error.code, LixError::CODE_PARSE_ERROR, "{sql}");
        }
        let malformed = session
            .execute("SELECT * FROM lix_commit_ancestry('not-a-commit-id')", &[])
            .await
            .expect_err("malformed ancestry anchor should fail");
        assert_eq!(malformed.code, LixError::CODE_INVALID_PARAM);
        let missing = session
            .execute(
                "SELECT * FROM lix_commit_ancestry('01990000-0000-7000-8000-00000000dead')",
                &[],
            )
            .await
            .expect_err("unknown ancestry anchor should fail");
        assert_eq!(missing.code, LixError::CODE_COMMIT_NOT_FOUND);

        assert_eq!(
            select_rows(&session, "SELECT * FROM lix_commit_ancestry() LIMIT 1").await,
            vec![vec![Value::Text(first_head.clone()), Value::Integer(0)]],
            "a bounded ancestry scan should return only the active anchor"
        );

        assert_eq!(
            select_rows(
                &session,
                "SELECT commit_id, depth FROM PUBLIC.LIX_COMMIT_ANCESTRY() ORDER BY depth, commit_id",
            )
            .await,
            restored,
            "public schema and unquoted case normalization must preserve table-function semantics"
        );
    }
);

simulation_test!(
    lix_commit_ancestry_deduplicates_merge_ancestors_at_shortest_depth,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let initial_commit_id = sim.initial_commit_id().to_string();
        let draft_id = "01930000-0000-7000-8000-000000000076";
        main.create_branch(CreateBranchOptions {
            id: Some(draft_id.to_string()),
            name: "ancestry draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created");
        let draft = sim.wrap_session(
            engine
                .open_session_at(draft_id)
                .await
                .expect("draft session should open"),
            &engine,
        );

        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('ancestry-main', 'main')",
            &[],
        )
        .await
        .expect("main commit should succeed");
        let main_parent = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('ancestry-draft', 'draft')",
                &[],
            )
            .await
            .expect("draft commit should succeed");
        let draft_parent = engine
            .load_branch_head_commit_id(draft_id)
            .await
            .expect("draft head should load")
            .expect("draft head should exist");

        assert_eq!(
            select_rows(
                &main,
                &format!(
                    "SELECT commit_id, depth FROM lix_commit_ancestry('{draft_parent}') \
                     ORDER BY depth, commit_id"
                ),
            )
            .await,
            vec![
                vec![Value::Text(draft_parent.clone()), Value::Integer(0)],
                vec![Value::Text(initial_commit_id.clone()), Value::Integer(1)],
            ],
            "an explicit anchor may be outside the active branch ancestry"
        );

        main.merge_branch(MergeBranchOptions {
            source_branch_id: draft_id.to_string(),
        })
        .await
        .expect("divergent branch merge should succeed");
        let merge_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("merge head should load")
            .expect("merge head should exist");
        let mut parents = [main_parent, draft_parent];
        parents.sort();

        assert_eq!(
            select_rows(
                &main,
                "SELECT commit_id, depth FROM lix_commit_ancestry() ORDER BY depth, commit_id",
            )
            .await,
            vec![
                vec![Value::Text(merge_head), Value::Integer(0)],
                vec![Value::Text(parents[0].clone()), Value::Integer(1)],
                vec![Value::Text(parents[1].clone()), Value::Integer(1)],
                vec![Value::Text(initial_commit_id), Value::Integer(2)],
            ],
            "the shared root must appear once at its shortest merge distance"
        );
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

        for table in ["lix_commit", "lix_commit_edge"] {
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
