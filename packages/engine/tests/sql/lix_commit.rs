use std::collections::BTreeSet;

use lix_engine::{CreateVersionOptions, Value};
use serde_json::json;

use super::select_rows;

simulation_test!(
    lix_commit_surfaces_expose_commits_edges_and_change_sets,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let initial_head = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("version head should load")
            .expect("version head should exist");

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('commit-surface', 'one')",
                &[],
            )
            .await
            .expect("first tracked write should succeed");
        let first_head = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("version head should load")
            .expect("version head should exist");

        session
            .execute(
                "UPDATE lix_key_value SET value = 'two' WHERE key = 'commit-surface'",
                &[],
            )
            .await
            .expect("second tracked write should succeed");
        let second_head = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("version head should load")
            .expect("version head should exist");

        let commit_rows = select_rows(
            &session,
            &format!(
                "SELECT id, change_set_id, change_ids, author_account_ids, parent_commit_ids, \
                 lixcol_global, lixcol_untracked \
                 FROM lix_commit WHERE id = '{second_head}'"
            ),
        )
        .await;
        assert_eq!(commit_rows.len(), 1);
        let change_set_id = text_value(&commit_rows[0][1]);
        assert_json_array_is_non_empty(&commit_rows[0][2], "change_ids");
        assert_eq!(commit_rows[0][3], Value::Json(json!([])));
        assert_eq!(commit_rows[0][4], Value::Json(json!([first_head.clone()])));
        assert_global_tracked(&commit_rows[0][5..]);

        let change_set_rows = select_rows(
            &session,
            &format!(
                "SELECT id, lixcol_global, lixcol_untracked \
                 FROM lix_change_set WHERE id = '{change_set_id}'"
            ),
        )
        .await;
        assert_eq!(
            change_set_rows,
            vec![vec![
                Value::Text(change_set_id.clone()),
                Value::Boolean(true),
                Value::Boolean(false),
            ]]
        );

        let change_set_by_version_rows = select_rows(
            &session,
            &format!(
                "SELECT id, lixcol_version_id, lixcol_global, lixcol_untracked \
                 FROM lix_change_set_by_version \
                 WHERE id = '{change_set_id}' \
                 ORDER BY lixcol_version_id"
            ),
        )
        .await;
        assert_eq!(
            change_set_by_version_rows,
            vec![
                vec![
                    Value::Text(change_set_id.clone()),
                    Value::Text(sim.main_version_id().to_string()),
                    Value::Boolean(true),
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text(change_set_id.clone()),
                    Value::Text("global".to_string()),
                    Value::Boolean(true),
                    Value::Boolean(false),
                ],
            ]
        );

        let edge_rows = select_rows(
            &session,
            &format!(
                "SELECT parent_id, child_id, lixcol_global, lixcol_untracked \
                 FROM lix_commit_edge WHERE child_id = '{second_head}'"
            ),
        )
        .await;
        assert_eq!(
            edge_rows,
            vec![vec![
                Value::Text(first_head.clone()),
                Value::Text(second_head.clone()),
                Value::Boolean(true),
                Value::Boolean(false),
            ]]
        );

        let change_set_element_rows = select_rows(
            &session,
            &format!(
                "SELECT entity_id, schema_key, lixcol_global, lixcol_untracked \
                 FROM lix_change_set_element \
                 WHERE change_set_id = '{change_set_id}' \
                 ORDER BY entity_id, schema_key"
            ),
        )
        .await;
        assert!(
            change_set_element_rows.contains(&vec![
                Value::Text("commit-surface".to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Boolean(true),
                Value::Boolean(false),
            ]),
            "expected key-value change in change-set elements: {change_set_element_rows:?}"
        );

        let change_set_element_by_version_rows = select_rows(
            &session,
            &format!(
                "SELECT entity_id, schema_key, lixcol_version_id, lixcol_global, lixcol_untracked \
                 FROM lix_change_set_element_by_version \
                 WHERE change_set_id = '{change_set_id}' \
                 AND entity_id = 'commit-surface' \
                 AND schema_key = 'lix_key_value' \
                 ORDER BY lixcol_version_id"
            ),
        )
        .await;
        assert_eq!(
            change_set_element_by_version_rows,
            vec![
                vec![
                    Value::Text("commit-surface".to_string()),
                    Value::Text("lix_key_value".to_string()),
                    Value::Text(sim.main_version_id().to_string()),
                    Value::Boolean(true),
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text("commit-surface".to_string()),
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("global".to_string()),
                    Value::Boolean(true),
                    Value::Boolean(false),
                ],
            ]
        );

        let by_version_rows = select_rows(
            &session,
            &format!(
                "SELECT id, lixcol_version_id, lixcol_global, lixcol_untracked \
                 FROM lix_commit_by_version \
                 WHERE id IN ('{initial_head}', '{first_head}', '{second_head}') \
                 ORDER BY id, lixcol_version_id"
            ),
        )
        .await;
        assert!(by_version_rows.contains(&vec![
            Value::Text(initial_head.clone()),
            Value::Text(sim.main_version_id().to_string()),
            Value::Boolean(true),
            Value::Boolean(false),
        ]));
        assert!(by_version_rows.contains(&vec![
            Value::Text(initial_head),
            Value::Text("global".to_string()),
            Value::Boolean(true),
            Value::Boolean(false),
        ]));
        assert!(by_version_rows.contains(&vec![
            Value::Text(first_head.clone()),
            Value::Text(sim.main_version_id().to_string()),
            Value::Boolean(true),
            Value::Boolean(false),
        ]));
        assert!(by_version_rows.contains(&vec![
            Value::Text(first_head.clone()),
            Value::Text("global".to_string()),
            Value::Boolean(true),
            Value::Boolean(false),
        ]));
        assert!(by_version_rows.contains(&vec![
            Value::Text(second_head.clone()),
            Value::Text(sim.main_version_id().to_string()),
            Value::Boolean(true),
            Value::Boolean(false),
        ]));
        assert!(by_version_rows.contains(&vec![
            Value::Text(second_head.clone()),
            Value::Text("global".to_string()),
            Value::Boolean(true),
            Value::Boolean(false),
        ]));

        let edge_by_version_rows = select_rows(
            &session,
            &format!(
                "SELECT parent_id, child_id, lixcol_version_id, lixcol_global, lixcol_untracked \
                 FROM lix_commit_edge_by_version \
                 WHERE child_id = '{second_head}' \
                 ORDER BY lixcol_version_id"
            ),
        )
        .await;
        assert_eq!(
            edge_by_version_rows,
            vec![
                vec![
                    Value::Text(first_head.clone()),
                    Value::Text(second_head.clone()),
                    Value::Text(sim.main_version_id().to_string()),
                    Value::Boolean(true),
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text(first_head),
                    Value::Text(second_head),
                    Value::Text("global".to_string()),
                    Value::Boolean(true),
                    Value::Boolean(false),
                ],
            ]
        );
    }
);

simulation_test!(
    lix_commit_is_plain_global_entity_not_active_reachability_view,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_workspace_session()
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

        main.create_version(CreateVersionOptions {
            id: Some("commit-branch".to_string()),
            name: "Commit branch".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("branch version should be created");

        let branch = sim.wrap_session(
            engine
                .open_session("commit-branch")
                .await
                .expect("branch session should open"),
            &engine,
        );
        branch
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('branch-only', 'branch')",
                &[],
            )
            .await
            .expect("branch write should succeed");

        let branch_head = engine
            .load_version_head_commit_id("commit-branch")
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
            "lix_commit should not depend on the active version"
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
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (schema_key, tables) in [
            ("lix_commit", vec!["lix_commit", "lix_commit_by_version"]),
            (
                "lix_commit_edge",
                vec!["lix_commit_edge", "lix_commit_edge_by_version"],
            ),
            (
                "lix_change_set",
                vec!["lix_change_set", "lix_change_set_by_version"],
            ),
            (
                "lix_change_set_element",
                vec![
                    "lix_change_set_element",
                    "lix_change_set_element_by_version",
                ],
            ),
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
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for table in [
            "lix_commit",
            "lix_commit_by_version",
            "lix_commit_edge",
            "lix_commit_edge_by_version",
            "lix_change_set",
            "lix_change_set_by_version",
            "lix_change_set_element",
            "lix_change_set_element_by_version",
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

fn assert_json_array_is_non_empty(value: &Value, column_name: &str) {
    let Value::Json(value) = value else {
        panic!("{column_name} should be a JSON array, got {value:?}");
    };
    let array = value
        .as_array()
        .unwrap_or_else(|| panic!("{column_name} should be a JSON array, got {value:?}"));
    assert!(!array.is_empty(), "{column_name} should not be empty");
}

fn assert_global_tracked(values: &[Value]) {
    assert_eq!(values, &[Value::Boolean(true), Value::Boolean(false)]);
}

fn builtin_schema_property_names(schema_key: &str) -> BTreeSet<String> {
    let schema = match schema_key {
        "lix_commit" => include_str!("../../src/schema/builtin/lix_commit.json"),
        "lix_commit_edge" => include_str!("../../src/schema/builtin/lix_commit_edge.json"),
        "lix_change_set" => include_str!("../../src/schema/builtin/lix_change_set.json"),
        "lix_change_set_element" => {
            include_str!("../../src/schema/builtin/lix_change_set_element.json")
        }
        other => panic!("unexpected builtin schema key: {other}"),
    };
    let schema = serde_json::from_str::<serde_json::Value>(schema)
        .expect("builtin schema fixture should parse");
    schema
        .get("properties")
        .and_then(serde_json::Value::as_object)
        .expect("builtin schema should define properties")
        .keys()
        .cloned()
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
