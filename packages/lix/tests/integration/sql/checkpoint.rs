use lix::{LixError, Value};
use serde_json::json;

use super::select_rows;

#[tokio::test(flavor = "current_thread")]
async fn checkpoints_example_sql_contract() {
    let lix = crate::open_lix().await.expect("example repository opens");
    let root_commit_id = lix
        .execute("SELECT lix_root_commit_id() AS commit_id", &[])
        .await
        .expect("repository root should resolve")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("root commit ID should decode");

    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
        &[
            Value::Text("checkpoint-demo".to_string()),
            Value::Text("draft".to_string()),
        ],
    )
    .await
    .expect("example tracked write succeeds");

    let head_commit_id = lix
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("active head should resolve")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("active head commit ID should decode");
    let working_diffs = lix
        .execute(
            "SELECT key, diff_type
             FROM lix_diff('lix_key_value', $1, $2)
             ORDER BY key",
            &[Value::Text(root_commit_id), Value::Text(head_commit_id)],
        )
        .await
        .expect("example working diff query succeeds");
    assert_eq!(working_diffs.len(), 1);
    assert_eq!(
        working_diffs.rows()[0]
            .get::<String>("key")
            .expect("key is text"),
        "checkpoint-demo"
    );
    assert_eq!(
        working_diffs.rows()[0]
            .get::<String>("diff_type")
            .expect("diff_type is text"),
        "added"
    );

    let checkpoint = lix
        .create_checkpoint()
        .await
        .expect("example checkpoint succeeds");
    let checkpoints = lix
        .execute(
            "SELECT commit_id, position
             FROM lix_log() WHERE is_checkpoint
             ORDER BY position",
            &[],
        )
        .await
        .expect("example checkpoint history query succeeds");
    assert_eq!(
        checkpoints.rows()[0]
            .get::<String>("commit_id")
            .expect("commit_id is text"),
        checkpoint.commit_id
    );
    assert_eq!(
        checkpoints.rows()[0]
            .get::<i64>("position")
            .expect("position is an integer"),
        0
    );

    let remaining = lix
        .execute(
            "SELECT COUNT(*) AS count FROM lix_diff('lix_key_value', $1, $2)",
            &[
                Value::Text(checkpoint.commit_id.clone()),
                Value::Text(checkpoint.commit_id),
            ],
        )
        .await
        .expect("example final working diff query succeeds");
    assert_eq!(
        remaining.rows()[0]
            .get::<i64>("count")
            .expect("count is an integer"),
        0
    );

    lix.close().await.expect("example repository closes");
}

simulation_test!(
    checkpoint_marks_working_interval_and_projects_sql_surfaces,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let initial_commit_id = sim.initial_commit_id().to_string();

        assert!(
            select_rows(&session, "SELECT id FROM lix_commit WHERE is_checkpoint")
                .await
                .is_empty()
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-key', 'one')",
                &[],
            )
            .await
            .expect("tracked insert should succeed");
        session
            .execute(
                "UPDATE lix_key_value SET value = 'two' WHERE key = 'checkpoint-key'",
                &[],
            )
            .await
            .expect("tracked update should succeed");
        let old_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head should load")
            .expect("head should exist");

        assert!(
            select_rows(&session, "SELECT id FROM lix_commit WHERE is_checkpoint")
                .await
                .is_empty(),
            "ordinary branch commits do not become checkpoints"
        );
        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT key, diff_type FROM {} ORDER BY key",
                    key_value_diff_relation(&session).await
                ),
            )
            .await,
            vec![vec![
                Value::Text("checkpoint-key".to_string()),
                Value::Text("added".to_string()),
            ]]
        );
        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT key, diff_type FROM {} \
                     WHERE key = 'checkpoint-key'",
                    key_value_diff_relation(&session).await
                ),
            )
            .await,
            vec![vec![
                Value::Text("checkpoint-key".to_string()),
                Value::Text("added".to_string()),
            ]]
        );
        assert!(
            select_rows(
                &session,
                &format!(
                    "SELECT key FROM {} \
                     WHERE key = 'other-key'",
                    key_value_diff_relation(&session).await
                ),
            )
            .await
            .is_empty()
        );

        let receipt = session
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");
        assert_ne!(receipt.commit_id, old_head);
        assert_eq!(
            engine
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("head should load"),
            Some(receipt.commit_id.clone())
        );

        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT COUNT(*) FROM {}",
                    key_value_diff_relation(&session).await
                ),
            )
            .await,
            vec![vec![Value::Integer(0)]]
        );
        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT id, is_checkpoint FROM lix_commit WHERE id = '{}'",
                    receipt.commit_id
                )
            )
            .await,
            vec![vec![
                Value::Text(receipt.commit_id.clone()),
                Value::Boolean(true)
            ]]
        );
        assert!(
            select_rows(
                &session,
                "SELECT id FROM lix_change WHERE schema_key = 'lix_checkpoint'"
            )
            .await
            .is_empty(),
            "checkpoint publication must not create artificial marker changes"
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT commit_id FROM lix_log() WHERE is_checkpoint ORDER BY position"
            )
            .await,
            vec![vec![Value::Text(receipt.commit_id.clone())]]
        );
        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT parent_commit_ids ->> 0 FROM lix_commit \
                     WHERE id = '{}'",
                    receipt.commit_id
                ),
            )
            .await,
            vec![vec![Value::Text(initial_commit_id.clone())]],
            "checkpoint severs the working interval at the prior checkpoint"
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT value FROM lix_key_value WHERE key = 'checkpoint-key'",
            )
            .await,
            vec![vec![Value::Jsonb(json!("two").into())]]
        );

        let timestamps_before_rebuild = select_rows(
            &session,
            "SELECT lixcol_created_at, lixcol_updated_at, lixcol_commit_id \
             FROM lix_key_value WHERE key = 'checkpoint-key'",
        )
        .await;
        assert_eq!(timestamps_before_rebuild.len(), 1);
        assert_eq!(
            timestamps_before_rebuild[0][0], timestamps_before_rebuild[0][1],
            "a newly added row must use the changelog's canonical timestamp"
        );
        assert_eq!(
            timestamps_before_rebuild[0][2],
            Value::Text(receipt.commit_id.clone()),
            "retained HOT rows must project the checkpoint as their live commit owner"
        );

        engine
            .rebuild_tracked_state_for_branch(sim.main_branch_id())
            .await
            .expect("checkpoint tracked state should rebuild");
        assert_eq!(
            select_rows(
                &session,
                "SELECT lixcol_created_at, lixcol_updated_at, lixcol_commit_id \
                 FROM lix_key_value WHERE key = 'checkpoint-key'",
            )
            .await,
            timestamps_before_rebuild,
            "checkpoint timestamps must remain stable after tracked-state rebuild"
        );
    }
);

simulation_test!(
    scoped_checkpoint_closes_schema_and_foreign_key_dependencies,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let parent_schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "checkpoint_parent",
            "columns": [{ "name": "id", "type": "text", "nullable": false }],
            "primary_key": ["id"]
        });
        let child_schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "checkpoint_child",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "parent_id", "type": "text", "nullable": false }
            ],
            "primary_key": ["id"],
            "foreign_keys": [{
                "columns": ["parent_id"],
                "references": { "schema_key": "checkpoint_parent", "columns": ["id"] }
            }]
        });
        for schema in [parent_schema, child_schema] {
            session
                .execute(
                    "INSERT INTO lix_registered_schema (schema_key, value) \
                     VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                    &[Value::Text(schema.to_string())],
                )
                .await
                .expect("schema registration should succeed");
        }
        session
            .execute(
                "INSERT INTO checkpoint_parent (id) VALUES ('parent-a')",
                &[],
            )
            .await
            .expect("parent insert should succeed");
        session
            .execute(
                "INSERT INTO checkpoint_child (id, parent_id) VALUES ('child-a', 'parent-a')",
                &[],
            )
            .await
            .expect("child insert should succeed");

        let checkpoint = session
            .execute(
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY[\
                    lix_row_ref('checkpoint_child', 'child-a')\
                 ])",
                &[],
            )
            .await
            .expect("scoped checkpoint should close dependencies");
        assert_eq!(checkpoint.len(), 1);
        for relation in ["checkpoint_child", "checkpoint_parent"] {
            assert_eq!(
                select_rows(
                    &session,
                    &format!("SELECT COUNT(*) FROM lix_diff('{relation}')"),
                )
                .await,
                vec![vec![Value::Integer(0)]],
                "selected child and its changed parent must cross together"
            );
        }
        session
            .execute(
                "INSERT INTO checkpoint_child (id, parent_id) VALUES ('child-b', 'parent-a')",
                &[],
            )
            .await
            .expect("schema registrations must remain visible after partial checkpoint");
    }
);

simulation_test!(
    scoped_checkpoint_closes_unique_value_swaps,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "checkpoint_unique_swap",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "slug", "type": "text", "nullable": false }
            ],
            "primary_key": ["id"],
            "unique": [["slug"]]
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1, CAST($2 AS JSONB))",
                &[
                    Value::Text("checkpoint_unique_swap".into()),
                    Value::Text(schema.to_string()),
                ],
            )
            .await
            .expect("schema registration should succeed");
        session
            .execute(
                "INSERT INTO checkpoint_unique_swap (id, slug) VALUES ('a', 'x'), ('b', 'y')",
                &[],
            )
            .await
            .expect("baseline rows should insert");
        session
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .expect("baseline checkpoint should succeed");

        session
            .execute(
                "DELETE FROM checkpoint_unique_swap WHERE id IN ('a', 'b')",
                &[],
            )
            .await
            .expect("baseline owners should delete");
        session
            .execute(
                "INSERT INTO checkpoint_unique_swap (id, slug) VALUES ('a', 'y'), ('b', 'x')",
                &[],
            )
            .await
            .expect("swapped rows should insert together");
        session
            .execute(
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY[lix_row_ref('checkpoint_unique_swap', 'a')])",
                &[],
            )
            .await
            .expect("checkpoint planner should close over the other unique owner");
        assert_eq!(
            select_rows(
                &session,
                "SELECT COUNT(*) FROM lix_diff('checkpoint_unique_swap')",
            )
            .await,
            vec![vec![Value::Integer(0)]],
            "a unique-value swap must cross the checkpoint boundary atomically"
        );
    }
);

simulation_test!(
    scoped_checkpoint_closes_file_ownership_from_semantic_rows,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "checkpoint_file_member",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false }
            ],
            "primary_key": ["id"]
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1, CAST($2 AS JSONB))",
                &[
                    Value::Text("checkpoint_file_member".into()),
                    Value::Text(schema.to_string()),
                ],
            )
            .await
            .expect("schema registration should succeed");
        let file_id = "01950000-0000-7000-8000-000000000021";
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES ($1, '/owned.txt', CAST('x' AS BYTEA))",
                &[Value::Text(file_id.into())],
            )
            .await
            .expect("file should insert");
        session
            .execute(
                "INSERT INTO checkpoint_file_member (id, value, lixcol_file_id) VALUES ('member', 'value', $1)",
                &[Value::Text(file_id.into())],
            )
            .await
            .expect("file-owned semantic row should insert");
        session
            .execute(
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY[lix_row_ref('checkpoint_file_member', 'member')])",
                &[],
            )
            .await
            .expect("direct semantic selection should include its file descriptor");
        for relation in ["checkpoint_file_member", "lix_file"] {
            assert_eq!(
                select_rows(
                    &session,
                    &format!("SELECT COUNT(*) FROM lix_diff('{relation}')")
                )
                .await,
                vec![vec![Value::Integer(0)]],
                "owned row and file descriptor must cross together"
            );
        }
    }
);

simulation_test!(scoped_checkpoint_closes_file_path_swaps, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );
    let first_id = "01950000-0000-7000-8000-000000000031";
    let second_id = "01950000-0000-7000-8000-000000000032";
    session
        .execute(
            "INSERT INTO lix_file (id, path, content) VALUES \
                 ($1, '/first.txt', CAST('first' AS BYTEA)), \
                 ($2, '/second.txt', CAST('second' AS BYTEA))",
            &[Value::Text(first_id.into()), Value::Text(second_id.into())],
        )
        .await
        .expect("baseline files should insert");
    session
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("baseline checkpoint should succeed");

    session
        .execute(
            "DELETE FROM lix_file WHERE id IN ($1, $2)",
            &[Value::Text(first_id.into()), Value::Text(second_id.into())],
        )
        .await
        .expect("baseline path owners should delete together");
    session
        .execute(
            "INSERT INTO lix_file (id, path, content) VALUES \
                 ($1, '/second.txt', CAST('first' AS BYTEA)), \
                 ($2, '/first.txt', CAST('second' AS BYTEA))",
            &[Value::Text(first_id.into()), Value::Text(second_id.into())],
        )
        .await
        .expect("swapped files should insert together");
    session
        .execute(
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY[lix_row_ref('lix_file', $1)])",
            &[Value::Text(first_id.into())],
        )
        .await
        .expect("checkpoint planner should close over the other path owner");
    assert_eq!(
        select_rows(&session, "SELECT COUNT(*) FROM lix_diff('lix_file')").await,
        vec![vec![Value::Integer(0)]],
        "a path swap must cross the checkpoint boundary atomically"
    );
});

simulation_test!(
    scoped_checkpoint_keeps_equal_file_names_in_distinct_directories_independent,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) VALUES \
                 ('01950000-0000-7000-8000-000000000041', NULL, 'left'), \
                 ('01950000-0000-7000-8000-000000000042', NULL, 'right')",
                &[],
            )
            .await
            .expect("directories should insert");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES \
                 ('01950000-0000-7000-8000-000000000043', '/left/same.txt', CAST('left' AS BYTEA)), \
                 ('01950000-0000-7000-8000-000000000044', '/right/same.txt', CAST('right' AS BYTEA))",
                &[],
            )
            .await
            .expect("same names in different directories should insert");
        session
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .expect("baseline checkpoint should succeed");

        session
            .execute(
                "UPDATE lix_file SET content = CAST('changed' AS BYTEA) WHERE id IN (\
                 '01950000-0000-7000-8000-000000000043', \
                 '01950000-0000-7000-8000-000000000044')",
                &[],
            )
            .await
            .expect("both files should change");
        session
            .execute(
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY[lix_row_ref(\
                 'lix_file', '01950000-0000-7000-8000-000000000043')])",
                &[],
            )
            .await
            .expect("distinct directory namespaces must not collide");
        assert_eq!(
            select_rows(&session, "SELECT id FROM lix_diff('lix_file') ORDER BY id",).await,
            vec![vec![Value::Text(
                "01950000-0000-7000-8000-000000000044".into(),
            )]],
            "the independently changed file stays in the working interval"
        );
    }
);

simulation_test!(
    scoped_checkpoint_closes_nested_file_directory_name_swaps,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let parent_id = "01950000-0000-7000-8000-000000000051";
        let directory_id = "01950000-0000-7000-8000-000000000052";
        let file_id = "01950000-0000-7000-8000-000000000053";
        session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) VALUES \
                 ($1, NULL, 'parent'), ($2, $1, 'directory-name')",
                &[
                    Value::Text(parent_id.into()),
                    Value::Text(directory_id.into()),
                ],
            )
            .await
            .expect("nested directories should insert");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES \
                 ($1, '/parent/file-name', CAST('file' AS BYTEA))",
                &[Value::Text(file_id.into())],
            )
            .await
            .expect("nested file should insert");
        session
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .expect("baseline checkpoint should succeed");

        session
            .execute(
                "DELETE FROM lix_file WHERE id = $1",
                &[Value::Text(file_id.into())],
            )
            .await
            .expect("file should delete");
        session
            .execute(
                "DELETE FROM lix_directory WHERE id = $1",
                &[Value::Text(directory_id.into())],
            )
            .await
            .expect("empty child directory should delete");
        session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) VALUES ($1, $2, 'file-name')",
                &[
                    Value::Text(directory_id.into()),
                    Value::Text(parent_id.into()),
                ],
            )
            .await
            .expect("directory should take the former file name");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES \
                 ($1, '/parent/directory-name', CAST('file' AS BYTEA))",
                &[Value::Text(file_id.into())],
            )
            .await
            .expect("file should take the former directory name");
        session
            .execute(
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY[lix_row_ref('lix_file', $1)])",
                &[Value::Text(file_id.into())],
            )
            .await
            .expect("cross-kind nested namespace swap should close atomically");
        for relation in ["lix_file", "lix_directory"] {
            assert_eq!(
                select_rows(
                    &session,
                    &format!("SELECT COUNT(*) FROM lix_diff('{relation}')")
                )
                .await,
                vec![vec![Value::Integer(0)]],
                "both sides of the nested namespace swap must cross together"
            );
        }
    }
);

simulation_test!(
    scoped_checkpoint_accepts_directory_and_mixed_relation_row_refs,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name) VALUES \
                 ('01950000-0000-7000-8000-000000000010', NULL, 'parent'), \
                 ('01950000-0000-7000-8000-000000000011', \
                  '01950000-0000-7000-8000-000000000010', 'child')",
                &[],
            )
            .await
            .expect("directories should insert");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('mixed-row', 'value')",
                &[],
            )
            .await
            .expect("custom row should insert");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES \
                 ('01950000-0000-7000-8000-000000000012', '/unselected.txt', CAST('x' AS BYTEA))",
                &[],
            )
            .await
            .expect("unselected file should insert");

        session
            .execute(
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY[\
                    lix_row_ref('lix_directory', '01950000-0000-7000-8000-000000000011'),\
                    lix_row_ref('lix_key_value', 'mixed-row')\
                 ])",
                &[],
            )
            .await
            .expect("mixed relation checkpoint should succeed");
        assert_eq!(
            select_rows(&session, "SELECT COUNT(*) FROM lix_diff('lix_directory')").await,
            vec![vec![Value::Integer(0)]],
            "direct directory selection includes its changed ancestor"
        );
        assert_eq!(
            select_rows(&session, "SELECT COUNT(*) FROM lix_diff('lix_key_value')").await,
            vec![vec![Value::Integer(0)]]
        );
        assert_eq!(
            select_rows(&session, "SELECT COUNT(*) FROM lix_diff('lix_file')").await,
            vec![vec![Value::Integer(1)]],
            "unselected relation rows remain in the working interval"
        );
    }
);

simulation_test!(
    scoped_checkpoint_closes_reverse_foreign_keys_for_deleted_targets,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        for schema in [
            json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "checkpoint_delete_parent",
                "columns": [{ "name": "id", "type": "text", "nullable": false }],
                "primary_key": ["id"]
            }),
            json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "checkpoint_delete_child",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "parent_id", "type": "text", "nullable": false }
                ],
                "primary_key": ["id"],
                "foreign_keys": [{
                    "columns": ["parent_id"],
                    "references": {
                        "schema_key": "checkpoint_delete_parent",
                        "columns": ["id"]
                    }
                }]
            }),
        ] {
            session
                .execute(
                    "INSERT INTO lix_registered_schema (schema_key, value) \
                     VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
                    &[Value::Text(schema.to_string())],
                )
                .await
                .expect("schema registration should succeed");
        }
        session
            .execute(
                "INSERT INTO checkpoint_delete_parent (id) VALUES ('parent-a')",
                &[],
            )
            .await
            .expect("parent should insert");
        session
            .execute(
                "INSERT INTO checkpoint_delete_child (id, parent_id) \
                 VALUES ('child-a', 'parent-a')",
                &[],
            )
            .await
            .expect("child should insert");
        session
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .expect("baseline checkpoint should succeed");

        session
            .execute(
                "DELETE FROM checkpoint_delete_child WHERE id = 'child-a'",
                &[],
            )
            .await
            .expect("child should delete first");
        session
            .execute(
                "DELETE FROM checkpoint_delete_parent WHERE id = 'parent-a'",
                &[],
            )
            .await
            .expect("parent should delete after child");
        session
            .execute(
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY[\
                    lix_row_ref('checkpoint_delete_parent', 'parent-a')\
                 ])",
                &[],
            )
            .await
            .expect("parent deletion must close over the changed child deletion");

        for relation in ["checkpoint_delete_parent", "checkpoint_delete_child"] {
            assert_eq!(
                select_rows(
                    &session,
                    &format!("SELECT COUNT(*) FROM lix_diff('{relation}')"),
                )
                .await,
                vec![vec![Value::Integer(0)]],
                "parent and child deletions must cross the checkpoint together"
            );
        }
    }
);

simulation_test!(
    checkpoint_function_obeys_outer_transaction_and_empty_scope,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('transaction-row', 'value')",
                &[],
            )
            .await
            .expect("working row should insert");
        let head_before_empty = select_rows(&session, "SELECT lix_active_branch_commit_id()").await;
        assert!(
            session
                .execute("SELECT commit_id FROM lix_create_checkpoint(ARRAY[])", &[],)
                .await
                .expect("empty scoped checkpoint is a no-op")
                .is_empty()
        );
        assert_eq!(
            select_rows(&session, "SELECT lix_active_branch_commit_id()").await,
            head_before_empty,
            "empty scope must never alias full checkpoint"
        );

        let mut rolled_back = session
            .begin_transaction()
            .await
            .expect("transaction opens");
        rolled_back
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .expect("checkpoint may stage in transaction");
        rolled_back.rollback().await.expect("rollback succeeds");
        assert_eq!(
            select_rows(&session, "SELECT COUNT(*) FROM lix_diff('lix_key_value')").await,
            vec![vec![Value::Integer(1)]],
            "rollback publishes neither checkpoint nor post-commit effects"
        );

        let mut write_then_checkpoint = session
            .begin_transaction()
            .await
            .expect("transaction opens");
        write_then_checkpoint
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('same-transaction', 'value')",
                &[],
            )
            .await
            .expect("tracked write should stage");
        let error = write_then_checkpoint
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .expect_err("checkpoint planning cannot ignore an earlier staged write");
        assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");
        write_then_checkpoint
            .rollback()
            .await
            .expect("rollback succeeds");

        let mut committed = session
            .begin_transaction()
            .await
            .expect("transaction opens");
        committed
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .expect("first checkpoint stages");
        let duplicate = committed
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .expect_err("one transaction cannot publish two checkpoints");
        assert_eq!(duplicate.code, "LIX_INVALID_TRANSACTION_STATE");
        committed.commit().await.expect("first checkpoint commits");
        assert_eq!(
            select_rows(&session, "SELECT COUNT(*) FROM lix_diff('lix_key_value')").await,
            vec![vec![Value::Integer(0)]]
        );
    }
);

simulation_test!(
    checkpoint_membership_is_immutable_and_legacy_surface_removed,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        assert!(
            select_rows(&session, "SELECT id FROM lix_commit WHERE is_checkpoint")
                .await
                .is_empty()
        );
        for sql in [
            "SELECT * FROM lix_checkpoint",
            "SELECT * FROM lix_checkpoint_by_branch",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("retired checkpoint table is absent");
            assert_eq!(error.code, LixError::CODE_TABLE_NOT_FOUND);
        }
        let checkpoint = session
            .create_checkpoint()
            .await
            .expect("checkpoint publishes");
        let error = session
            .execute(
                "UPDATE lix_commit SET is_checkpoint = false WHERE id = $1",
                &[Value::Text(checkpoint.commit_id)],
            )
            .await
            .expect_err("immutable membership cannot be edited");
        assert_eq!(error.code, LixError::CODE_READ_ONLY);

        for sql in [
            "SELECT * FROM lix_working_diff_by_branch",
            "SELECT * FROM lix_file_working_diff",
            "SELECT * FROM lix_directory_working_diff",
            "SELECT * FROM lix_directory_working_diff_by_branch",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("retired by-branch surfaces must fail closed");
            assert_eq!(error.code, LixError::CODE_TABLE_NOT_FOUND);
        }
    }
);

simulation_test!(
    checkpoint_inventory_survives_restore_while_log_follows_mainline,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-history', 'one')",
                &[],
            )
            .await
            .expect("first value should commit");
        let first_checkpoint = session
            .create_checkpoint()
            .await
            .expect("first checkpoint should commit");

        session
            .execute(
                "UPDATE lix_key_value SET value = 'two' WHERE key = 'checkpoint-history'",
                &[],
            )
            .await
            .expect("second value should commit");
        let abandoned_checkpoint = session
            .create_checkpoint()
            .await
            .expect("second checkpoint should commit");

        session
            .execute(
                "INSERT INTO lix_restore (commit_id) VALUES ($1)",
                &[Value::Text(first_checkpoint.commit_id.clone())],
            )
            .await
            .expect("restore to first checkpoint should succeed");

        let global_rows = select_rows(
            &session,
            "SELECT id FROM lix_commit WHERE is_checkpoint ORDER BY id",
        )
        .await;
        assert!(
            global_rows.contains(&vec![Value::Text(abandoned_checkpoint.commit_id.clone())]),
            "the normal global table must retain the abandoned checkpoint marker"
        );

        let reachable_history = select_rows(
            &session,
            &format!(
                "SELECT commit_id FROM lix_log('{}') WHERE is_checkpoint ORDER BY position",
                first_checkpoint.commit_id
            ),
        )
        .await;
        assert_eq!(
            reachable_history,
            vec![vec![Value::Text(first_checkpoint.commit_id.clone())]]
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT commit_id FROM lix_log() WHERE is_checkpoint ORDER BY position"
            )
            .await,
            vec![vec![Value::Text(first_checkpoint.commit_id)]]
        );
    }
);

simulation_test!(
    working_diff_reports_net_tracked_adds_and_removals_after_a_revert,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        for (key, value) in [("working-removed", "old"), ("working-reverted", "old")] {
            session
                .execute(
                    &format!("INSERT INTO lix_key_value (key, value) VALUES ('{key}', '{value}')"),
                    &[],
                )
                .await
                .expect("tracked baseline insert should succeed");
        }
        session
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should succeed");

        for sql in [
            "UPDATE lix_key_value SET value = 'new' WHERE key = 'working-reverted'",
            "UPDATE lix_key_value SET value = 'old' WHERE key = 'working-reverted'",
            "DELETE FROM lix_key_value WHERE key = 'working-removed'",
            "INSERT INTO lix_key_value (key, value) VALUES ('working-added', 'new')",
        ] {
            session
                .execute(sql, &[])
                .await
                .expect("tracked working diff should succeed");
        }

        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT key, diff_type FROM {} ORDER BY key",
                    key_value_diff_relation(&session).await
                ),
            )
            .await,
            vec![
                vec![
                    Value::Text("working-added".to_string()),
                    Value::Text("added".to_string()),
                ],
                vec![
                    Value::Text("working-removed".to_string()),
                    Value::Text("removed".to_string()),
                ],
            ],
            "the direct working-diff path must collapse a payload revert",
        );
        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT diff_type FROM {} \
                     WHERE key = 'working-removed'",
                    key_value_diff_relation(&session).await
                ),
            )
            .await,
            vec![vec![Value::Text("removed".to_string())]],
            "an exact PK filter must preserve the same net diff",
        );
    }
);

async fn key_value_diff_relation(
    session: &crate::support::simulation_test::engine::SimSession,
) -> String {
    let _ = session;
    "lix_diff('lix_key_value')".to_owned()
}

#[cfg(any())]
simulation_test!(
    file_working_diff_reports_root_file_changes_without_directory_changes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let file_id = "01950000-0000-7000-8000-000000000099";
        let nested_file_id = "01950000-0000-7000-8000-000000000100";

        session
            .execute(
                &format!(
                    "INSERT INTO lix_file (id, path, content) \
                     VALUES ('{file_id}', '/a.md', CAST('old' AS BYTEA))"
                ),
                &[],
            )
            .await
            .expect("root file insert should succeed");

        assert_eq!(
            select_rows(
                &session,
                "SELECT id, path, previous_path, change_kind \
                 FROM lix_file_working_diff",
            )
            .await,
            vec![vec![
                Value::Text(file_id.to_string()),
                Value::Text("/a.md".to_string()),
                Value::Null,
                Value::Text("added".to_string()),
            ]],
            "a root file add must not require an unrelated directory change",
        );

        session
            .execute(
                &format!(
                    "INSERT INTO lix_file (id, path, content) \
                     VALUES ('{nested_file_id}', '/existing/b.md', CAST('old' AS BYTEA))"
                ),
                &[],
            )
            .await
            .expect("nested baseline file insert should succeed");
        session
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should succeed");
        for changed_file_id in [file_id, nested_file_id] {
            session
                .execute(
                    &format!(
                        "UPDATE lix_file SET content = CAST('new' AS BYTEA) WHERE id = '{changed_file_id}'"
                    ),
                    &[],
                )
                .await
                .expect("file data update should succeed");
        }

        assert_eq!(
            select_rows(
                &session,
                "SELECT id, path, previous_path, change_kind \
                 FROM lix_file_working_diff ORDER BY id",
            )
            .await,
            vec![
                vec![
                    Value::Text(file_id.to_string()),
                    Value::Text("/a.md".to_string()),
                    Value::Text("/a.md".to_string()),
                    Value::Text("modified".to_string()),
                ],
                vec![
                    Value::Text(nested_file_id.to_string()),
                    Value::Text("/existing/b.md".to_string()),
                    Value::Text("/existing/b.md".to_string()),
                    Value::Text("modified".to_string()),
                ],
            ],
            "data-only file changes must resolve targeted file and ancestor descriptors",
        );

        session
            .create_checkpoint()
            .await
            .expect("second baseline checkpoint should succeed");
        session
            .execute(&format!("DELETE FROM lix_file WHERE id = '{file_id}'"), &[])
            .await
            .expect("root file delete should succeed");
        session
            .execute(
                &format!(
                    "UPDATE lix_file SET path = '/existing/c.md' WHERE id = '{nested_file_id}'"
                ),
                &[],
            )
            .await
            .expect("nested file rename should succeed");

        assert_eq!(
            select_rows(
                &session,
                "SELECT id, path, previous_path, change_kind \
                 FROM lix_file_working_diff ORDER BY id",
            )
            .await,
            vec![
                vec![
                    Value::Text(file_id.to_string()),
                    Value::Null,
                    Value::Text("/a.md".to_string()),
                    Value::Text("removed".to_string()),
                ],
                vec![
                    Value::Text(nested_file_id.to_string()),
                    Value::Text("/existing/c.md".to_string()),
                    Value::Text("/existing/b.md".to_string()),
                    Value::Text("modified".to_string()),
                ],
            ],
            "descriptor-only removes and renames must use typed targeted keys",
        );
    }
);

#[cfg(any())]
simulation_test!(
    filesystem_working_diff_surfaces_compose_paths_and_directory_moves,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('01950000-0000-7000-8000-000000000001', '/docs/readme.md', CAST('hello' AS BYTEA))",
                &[],
            )
            .await
            .expect("file insert should succeed");

        assert_eq!(
            select_rows(
                &session,
                "SELECT id, path, previous_path, change_kind \
                 FROM lix_file_working_diff ORDER BY id",
            )
            .await,
            vec![vec![
                Value::Text("01950000-0000-7000-8000-000000000001".to_string()),
                Value::Text("/docs/readme.md".to_string()),
                Value::Null,
                Value::Text("added".to_string()),
            ]]
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT path, previous_path, change_kind \
                 FROM lix_directory_working_diff",
            )
            .await,
            vec![vec![
                Value::Text("/docs".to_string()),
                Value::Null,
                Value::Text("added".to_string()),
            ]]
        );

        session
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");
        assert_eq!(
            select_rows(&session, "SELECT COUNT(*) FROM lix_file_working_diff",).await,
            vec![vec![Value::Integer(0)]]
        );

        session
            .execute(
                "UPDATE lix_directory SET path = '/writing' WHERE path = '/docs'",
                &[],
            )
            .await
            .expect("directory move should succeed");
        assert_eq!(
            select_rows(
                &session,
                "SELECT id, path, previous_path, change_kind \
                 FROM lix_file_working_diff",
            )
            .await,
            vec![vec![
                Value::Text("01950000-0000-7000-8000-000000000001".to_string()),
                Value::Text("/writing/readme.md".to_string()),
                Value::Text("/docs/readme.md".to_string()),
                Value::Text("modified".to_string()),
            ]],
            "ancestor directory moves expand to descendant logical files"
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT path, previous_path FROM lix_directory_working_diff",
            )
            .await,
            vec![vec![
                Value::Text("/writing".to_string()),
                Value::Text("/docs".to_string()),
            ]]
        );
    }
);
