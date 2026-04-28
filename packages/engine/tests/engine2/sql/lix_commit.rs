use crate::simulation_test2;
use lix_engine::Value;

use super::select_rows;

simulation_test2!(
    lix_commit_surfaces_expose_commits_edges_and_change_sets,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim
            .open_main_session(&engine)
            .await
            .expect("main session should open");

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
                "SELECT id, change_set_id, lixcol_global, lixcol_untracked \
                 FROM lix_commit WHERE id = '{second_head}'"
            ),
        )
        .await;
        assert_eq!(commit_rows.len(), 1);
        let change_set_id = text_value(&commit_rows[0][1]);
        assert_global_tracked(&commit_rows[0][2..]);

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

fn text_value(value: &Value) -> String {
    let Value::Text(value) = value else {
        panic!("expected text value, got {value:?}");
    };
    value.clone()
}

fn assert_global_tracked(values: &[Value]) {
    assert_eq!(values, &[Value::Boolean(true), Value::Boolean(false)]);
}
