use lix::{CreateBranchOptions, MergeBranchOptions, MergeBranchPreviewOptions, Value};
use serde_json::{Value as JsonValue, json};

use super::assert_rows_eq;

fn schema(key: &str, columns: JsonValue, row_refs: Option<JsonValue>) -> JsonValue {
    let mut value = json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": key,
        "columns": columns,
        "primary_key": ["id"],
    });
    if let Some(row_refs) = row_refs {
        value["row_refs"] = row_refs;
    }
    value
}

async fn register(session: &crate::support::simulation_test::engine::SimSession, value: JsonValue) {
    session
        .execute(
            "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
            &[Value::Text(value.to_string())],
        )
        .await
        .expect("schema registration should succeed");
}

async fn register_global(
    session: &crate::support::simulation_test::engine::SimSession,
    value: JsonValue,
) {
    session
        .execute(
            "INSERT INTO lix_registered_schema(value,lixcol_global) VALUES ($1::jsonb,true)",
            &[Value::Text(value.to_string())],
        )
        .await
        .expect("global schema registration should succeed");
}

async fn register_untracked(
    session: &crate::support::simulation_test::engine::SimSession,
    value: JsonValue,
) {
    session
        .execute(
            "INSERT INTO lix_registered_schema(value,lixcol_global,lixcol_untracked) \
             VALUES ($1::jsonb,false,true)",
            &[Value::Text(value.to_string())],
        )
        .await
        .expect("untracked schema registration should succeed");
}

fn parent_schema(key: &str) -> JsonValue {
    schema(
        key,
        json!([{"name":"id","type":"text","nullable":false}]),
        None,
    )
}

fn row_ref_child_schema(key: &str, on_delete: &str) -> JsonValue {
    schema(
        key,
        json!([
            {"name":"id","type":"text","nullable":false},
            {"name":"target","type":"text"}
        ]),
        Some(json!([{"column":"target","on_delete":on_delete}])),
    )
}

fn row_ref_child_value_schema(key: &str, on_delete: &str) -> JsonValue {
    schema(
        key,
        json!([
            {"name":"id","type":"text","nullable":false},
            {"name":"target","type":"text"},
            {"name":"value","type":"text"}
        ]),
        Some(json!([{"column":"target","on_delete":on_delete}])),
    )
}

fn row_ref_child_default_schema(key: &str) -> JsonValue {
    schema(
        key,
        json!([
            {"name":"id","type":"text","nullable":false},
            {"name":"target","type":"text"}
        ]),
        Some(json!([{"column":"target"}])),
    )
}

fn foreign_key_child_schema(key: &str, parent_key: &str, on_delete: &str) -> JsonValue {
    let mut value = schema(
        key,
        json!([
            {"name":"id","type":"text","nullable":false},
            {"name":"parent_id","type":"text","nullable":false}
        ]),
        None,
    );
    value["foreign_keys"] = json!([{
        "columns": ["parent_id"],
        "references": {"schema_key": parent_key, "columns": ["id"]},
        "on_delete": on_delete
    }]);
    value
}

simulation_test!(
    row_ref_cascade_pending_visibility_and_rollback,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register(&session, parent_schema("rr_pending_parent")).await;
        register(
            &session,
            row_ref_child_schema("rr_pending_child", "cascade"),
        )
        .await;
        register(&session, row_ref_child_schema("rr_pending_leaf", "cascade")).await;

        session
            .execute("INSERT INTO rr_pending_parent(id) VALUES ('p')", &[])
            .await
            .unwrap();
        session
        .execute(
            "INSERT INTO rr_pending_child(id,target) VALUES ('c', lix_row_ref('rr_pending_parent', NULL, 'p'))",
            &[],
        )
        .await
        .unwrap();
        session
        .execute(
            "INSERT INTO rr_pending_leaf(id,target) VALUES ('l', lix_row_ref('rr_pending_child', NULL, 'c'))",
            &[],
        )
        .await
        .unwrap();

        let mut pending = session.begin_transaction().await.unwrap();
        pending
            .execute("INSERT INTO rr_pending_parent(id) VALUES ('pending')", &[])
            .await
            .unwrap();
        pending
        .execute(
            "INSERT INTO rr_pending_child(id,target) VALUES ('pending-child', lix_row_ref('rr_pending_parent', NULL, 'pending'))",
            &[],
        )
        .await
        .unwrap();
        pending
        .execute(
            "INSERT INTO rr_pending_leaf(id,target) VALUES ('pending-leaf', lix_row_ref('rr_pending_child', NULL, 'pending-child'))",
            &[],
        )
        .await
        .unwrap();
        assert_rows_eq(
            pending
                .execute("SELECT id FROM rr_pending_leaf ORDER BY id", &[])
                .await
                .unwrap(),
            vec![
                vec![Value::Text("l".into())],
                vec![Value::Text("pending-leaf".into())],
            ],
        );
        pending
            .execute("DELETE FROM rr_pending_parent WHERE id='pending'", &[])
            .await
            .unwrap();
        assert_rows_eq(
            pending
                .execute("SELECT id FROM rr_pending_child ORDER BY id", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("c".into())]],
        );
        assert_rows_eq(
            pending
                .execute("SELECT id FROM rr_pending_leaf ORDER BY id", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("l".into())]],
        );
        pending.rollback().await.unwrap();
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_pending_leaf ORDER BY id", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("l".into())]],
        );

        let mut deletion = session.begin_transaction().await.unwrap();
        deletion
            .execute("DELETE FROM rr_pending_parent WHERE id='p'", &[])
            .await
            .unwrap();
        assert_rows_eq(
            deletion
                .execute("SELECT id FROM rr_pending_child", &[])
                .await
                .unwrap(),
            vec![],
        );
        deletion.rollback().await.unwrap();
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_pending_child", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("c".into())]],
        );
    }
);

simulation_test!(
    row_ref_cascade_closes_broad_multilevel_frontier,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register(&session, parent_schema("rr_frontier_parent")).await;
        register(
            &session,
            row_ref_child_schema("rr_frontier_child", "cascade"),
        )
        .await;
        register(
            &session,
            row_ref_child_schema("rr_frontier_leaf", "cascade"),
        )
        .await;

        session
            .execute("INSERT INTO rr_frontier_parent(id) VALUES ('p')", &[])
            .await
            .unwrap();
        let children = (0..130)
            .map(|index| {
                format!("('child-{index:03}', lix_row_ref('rr_frontier_parent', NULL, 'p'))")
            })
            .collect::<Vec<_>>()
            .join(", ");
        session
            .execute(
                &format!("INSERT INTO rr_frontier_child(id,target) VALUES {children}"),
                &[],
            )
            .await
            .unwrap();
        let leaves = (0..130)
            .map(|index| {
                format!(
                    "('leaf-{index:03}', lix_row_ref('rr_frontier_child', NULL, 'child-{index:03}'))"
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        session
            .execute(
                &format!("INSERT INTO rr_frontier_leaf(id,target) VALUES {leaves}"),
                &[],
            )
            .await
            .unwrap();

        session
            .execute("DELETE FROM rr_frontier_parent WHERE id='p'", &[])
            .await
            .unwrap();
        assert_rows_eq(
            session
                .execute("SELECT COUNT(*) FROM rr_frontier_child", &[])
                .await
                .unwrap(),
            vec![vec![Value::Integer(0)]],
        );
        assert_rows_eq(
            session
                .execute("SELECT COUNT(*) FROM rr_frontier_leaf", &[])
                .await
                .unwrap(),
            vec![vec![Value::Integer(0)]],
        );
    }
);

simulation_test!(
    row_ref_cascade_follows_static_fk_into_dynamic_leaf,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register(&session, parent_schema("rr_mixed_parent")).await;
        register(
            &session,
            foreign_key_child_schema("rr_mixed_child", "rr_mixed_parent", "cascade"),
        )
        .await;
        register(&session, row_ref_child_schema("rr_mixed_leaf", "cascade")).await;

        session
            .execute("INSERT INTO rr_mixed_parent(id) VALUES ('p')", &[])
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO rr_mixed_child(id,parent_id) VALUES ('c','p')",
                &[],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO rr_mixed_leaf(id,target) \
                 VALUES ('l',lix_row_ref('rr_mixed_child',NULL,'c'))",
                &[],
            )
            .await
            .unwrap();

        session
            .execute("DELETE FROM rr_mixed_parent WHERE id='p'", &[])
            .await
            .unwrap();
        for relation in ["rr_mixed_parent", "rr_mixed_child", "rr_mixed_leaf"] {
            assert_rows_eq(
                session
                    .execute(&format!("SELECT id FROM {relation}"), &[])
                    .await
                    .unwrap(),
                vec![],
            );
        }
    }
);

simulation_test!(
    row_ref_constructor_update_returning_refreshes_reference,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register(&session, parent_schema("rr_native_ref_parent")).await;
        register(
            &session,
            row_ref_child_schema("rr_native_ref_child", "cascade"),
        )
        .await;

        session
            .execute(
                "INSERT INTO rr_native_ref_parent(id) VALUES ('p'), ('q')",
                &[],
            )
            .await
            .unwrap();
        let inserted = session
            .execute(
                "INSERT INTO rr_native_ref_child(id,target) \
                 VALUES ('c', lix_row_ref('rr_native_ref_parent',NULL,'p')) \
                 RETURNING lix_row_ref('rr_native_ref_child',NULL,id) AS child_ref",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(inserted.rows()[0].values(), [Value::RowRef(_)]));

        let updated = session
            .execute(
                "UPDATE rr_native_ref_child \
                 SET target = lix_row_ref('rr_native_ref_parent',NULL,'q') \
                 WHERE id = 'c' \
                 RETURNING target",
                &[],
            )
            .await
            .unwrap();
        let expected = session
            .execute(
                "SELECT lix_row_ref('rr_native_ref_parent',NULL,'q') AS target",
                &[],
            )
            .await
            .unwrap();
        let [Value::RowRef(expected)] = expected.rows()[0].values() else {
            panic!("constructor result must retain row_ref metadata");
        };
        assert_eq!(
            updated.rows()[0].values(),
            &[Value::Text(expected.as_str().to_owned())],
            "RETURNING a declared text column preserves its text type",
        );

        session
            .execute("DELETE FROM rr_native_ref_parent WHERE id = 'p'", &[])
            .await
            .unwrap();
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_native_ref_child", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("c".into())]],
        );

        session
            .execute("DELETE FROM rr_native_ref_parent WHERE id = 'q'", &[])
            .await
            .unwrap();
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_native_ref_child", &[])
                .await
                .unwrap(),
            vec![],
        );
    }
);

simulation_test!(
    row_ref_cascade_is_file_exact_and_supports_fileless_rows,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register(&session, parent_schema("rr_scope_parent")).await;
        register(&session, row_ref_child_schema("rr_scope_child", "cascade")).await;
        let file_a = "01991b1d-6d8b-7000-8000-0000000000a1";
        let file_b = "01991b1d-6d8b-7000-8000-0000000000a2";
        let file_c = "01991b1d-6d8b-7000-8000-0000000000a3";

        // File-scoped rows must have a corresponding public file descriptor.
        // The row-ref test uses fixed ids so its two same-key rows remain
        // distinguishable across the file scope boundary, including duplicate
        // child primary keys and a fileless child.
        session
            .execute(
                "INSERT INTO lix_file(id,path,content) VALUES \
                 ($1,'/row-ref-scope-a.txt',CAST('a' AS BYTEA)), \
                 ($2,'/row-ref-scope-b.txt',CAST('b' AS BYTEA)), \
                 ($3,'/row-ref-scope-c.txt',CAST('c' AS BYTEA))",
                &[
                    Value::Text(file_a.into()),
                    Value::Text(file_b.into()),
                    Value::Text(file_c.into()),
                ],
            )
            .await
            .unwrap();
        session
        .execute(
            "INSERT INTO rr_scope_parent(id,lixcol_file_id) VALUES ('p',$1),('p',$2),('fileless',NULL)",
            &[Value::Text(file_a.into()), Value::Text(file_b.into())],
        )
        .await
        .unwrap();
        session
            .execute(
                "INSERT INTO rr_scope_child(id,target,lixcol_file_id) VALUES \
             ('c',lix_row_ref('rr_scope_parent',$1,'p'),$1), \
             ('c',lix_row_ref('rr_scope_parent',$1,'p'),$2), \
             ('c',lix_row_ref('rr_scope_parent',$2,'p'),$3), \
             ('c',lix_row_ref('rr_scope_parent',NULL,'fileless'),NULL)",
                &[
                    Value::Text(file_a.into()),
                    Value::Text(file_b.into()),
                    Value::Text(file_c.into()),
                ],
            )
            .await
            .unwrap();

        session
            .execute(
                "DELETE FROM rr_scope_parent WHERE id='p' AND lixcol_file_id=$1",
                &[Value::Text(file_a.into())],
            )
            .await
            .unwrap();
        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM rr_scope_child WHERE lixcol_file_id IN ($1,$2)",
                    &[Value::Text(file_a.into()), Value::Text(file_b.into())],
                )
                .await
                .unwrap(),
            vec![],
        );
        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM rr_scope_child WHERE lixcol_file_id=$1",
                    &[Value::Text(file_c.into())],
                )
                .await
                .unwrap(),
            vec![vec![Value::Text("c".into())]],
        );
        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM rr_scope_child WHERE lixcol_file_id IS NULL",
                    &[],
                )
                .await
                .unwrap(),
            vec![vec![Value::Text("c".into())]],
        );
        session
            .execute(
                "DELETE FROM rr_scope_parent WHERE id='fileless' AND lixcol_file_id IS NULL",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_scope_child", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("c".into())]],
        );
    }
);

simulation_test!(
    row_ref_cascade_concurrent_reply_is_atomic,
    options = crate::support::simulation_test::engine::SimulationOptions {
        deterministic: false
    },
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register(&session, parent_schema("rr_race_parent")).await;
        register(&session, row_ref_child_schema("rr_race_child", "cascade")).await;
        session
            .execute("INSERT INTO rr_race_parent(id) VALUES ('p')", &[])
            .await
            .unwrap();

        let other = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let mut deletion = session.begin_transaction().await.unwrap();
        deletion
            .execute("DELETE FROM rr_race_parent WHERE id='p'", &[])
            .await
            .unwrap();
        // The committed child was absent when the DELETE planned its cascade.
        other
            .execute(
                "INSERT INTO rr_race_child(id,target) VALUES ('concurrent', lix_row_ref('rr_race_parent',NULL,'p'))",
                &[],
            )
            .await
            .unwrap();

        match deletion.commit().await {
            Ok(_) => assert_rows_eq(
                session
                    .execute("SELECT id FROM rr_race_child", &[])
                    .await
                    .unwrap(),
                vec![],
            ),
            Err(error) => {
                assert_eq!(error.code, lix::LixError::CODE_TRANSACTION_CONFLICT);
                assert_rows_eq(
                    session
                        .execute("SELECT id FROM rr_race_parent", &[])
                        .await
                        .unwrap(),
                    vec![vec![Value::Text("p".into())]],
                );
                assert_rows_eq(
                    session
                        .execute("SELECT id FROM rr_race_child", &[])
                        .await
                        .unwrap(),
                    vec![vec![Value::Text("concurrent".into())]],
                );
                session
                    .execute("DELETE FROM rr_race_parent WHERE id='p'", &[])
                    .await
                    .unwrap();
                assert_rows_eq(
                    session
                        .execute("SELECT id FROM rr_race_child", &[])
                        .await
                        .unwrap(),
                    vec![],
                );
            }
        }
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_race_parent", &[])
                .await
                .unwrap(),
            vec![],
        );
        let error = other
            .execute(
                "INSERT INTO rr_race_child(id,target) VALUES ('late', lix_row_ref('rr_race_parent',NULL,'p'))",
                &[],
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, lix::LixError::CODE_FOREIGN_KEY);
    }
);

simulation_test!(
    row_ref_cascade_resolves_public_filesystem_aliases,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register(&session, row_ref_child_schema("rr_alias_child", "cascade")).await;
        let file_id = "01991b1d-6d8b-7000-8000-0000000000b1";
        let directory_id = "01991b1d-6d8b-7000-8000-0000000000b2";
        session
            .execute(
                "INSERT INTO lix_directory(id,path) VALUES ($1,'/alias-dir')",
                &[Value::Text(directory_id.into())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO lix_file(id,path,content) VALUES ($1,'/alias-file.txt',CAST('x' AS BYTEA))",
                &[Value::Text(file_id.into())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO rr_alias_child(id,target) VALUES ('file',lix_row_ref('lix_file',NULL,$1)), ('directory',lix_row_ref('lix_directory',NULL,$2))",
                &[Value::Text(file_id.into()), Value::Text(directory_id.into())],
            )
            .await
            .unwrap();

        session
            .execute(
                "DELETE FROM lix_file WHERE id=$1",
                &[Value::Text(file_id.into())],
            )
            .await
            .unwrap();
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_alias_child", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("directory".into())]],
        );
        session
            .execute(
                "DELETE FROM lix_directory WHERE id=$1",
                &[Value::Text(directory_id.into())],
            )
            .await
            .unwrap();
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_alias_child", &[])
                .await
                .unwrap(),
            vec![],
        );
    }
);

simulation_test!(
    tracked_row_ref_does_not_accept_untracked_only_target,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register(&session, parent_schema("rr_lane_parent")).await;
        register(&session, row_ref_child_default_schema("rr_lane_child")).await;

        session
            .execute(
                "INSERT INTO rr_lane_parent(id,lixcol_untracked) VALUES ('p',true)",
                &[],
            )
            .await
            .unwrap();
        let error = session
            .execute(
                "INSERT INTO rr_lane_child(id,target) VALUES ('c',lix_row_ref('rr_lane_parent',NULL,'p'))",
                &[],
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, lix::LixError::CODE_FOREIGN_KEY);
    }
);

simulation_test!(
    tracked_parent_delete_cascades_untracked_only_row_ref_child,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register(&session, parent_schema("rr_untracked_cascade_parent")).await;
        register_untracked(
            &session,
            row_ref_child_schema("rr_untracked_cascade_child", "cascade"),
        )
        .await;

        session
            .execute(
                "INSERT INTO rr_untracked_cascade_parent(id) VALUES ('p')",
                &[],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO rr_untracked_cascade_child(id,target,lixcol_untracked) \
                 VALUES ('c',lix_row_ref('rr_untracked_cascade_parent',NULL,'p'),true)",
                &[],
            )
            .await
            .unwrap();

        session
            .execute("DELETE FROM rr_untracked_cascade_parent WHERE id='p'", &[])
            .await
            .unwrap();
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_untracked_cascade_parent", &[])
                .await
                .unwrap(),
            vec![],
        );
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_untracked_cascade_child", &[])
                .await
                .unwrap(),
            vec![],
        );
    }
);

simulation_test!(
    tracked_parent_delete_rejects_untracked_only_no_action_row_ref_child,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register(&session, parent_schema("rr_untracked_restrict_parent")).await;
        register_untracked(
            &session,
            row_ref_child_schema("rr_untracked_restrict_child", "no_action"),
        )
        .await;

        session
            .execute(
                "INSERT INTO rr_untracked_restrict_parent(id) VALUES ('p')",
                &[],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO rr_untracked_restrict_child(id,target,lixcol_untracked) \
                 VALUES ('c',lix_row_ref('rr_untracked_restrict_parent',NULL,'p'),true)",
                &[],
            )
            .await
            .unwrap();

        let error = session
            .execute("DELETE FROM rr_untracked_restrict_parent WHERE id='p'", &[])
            .await
            .expect_err("untracked no_action child must reject parent deletion");
        assert_eq!(error.code, lix::LixError::CODE_FOREIGN_KEY);
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_untracked_restrict_parent", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("p".into())]],
        );
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_untracked_restrict_child", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("c".into())]],
        );
    }
);

simulation_test!(
    row_ref_checkpoint_and_restore_close_over_exact_target,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register(
            &session,
            schema(
                "rr_checkpoint_parent",
                json!([
                    {"name":"id","type":"text","nullable":false},
                    {"name":"value","type":"text"}
                ]),
                None,
            ),
        )
        .await;
        register(
            &session,
            row_ref_child_value_schema("rr_checkpoint_child", "cascade"),
        )
        .await;
        session
            .execute(
                "INSERT INTO rr_checkpoint_parent(id,value) VALUES ('p','before')",
                &[],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO rr_checkpoint_child(id,target,value) VALUES ('c', lix_row_ref('rr_checkpoint_parent',NULL,'p'), 'before')",
                &[],
            )
            .await
            .unwrap();

        let checkpoint = session
            .execute(
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY[\
                   lix_row_ref('rr_checkpoint_child', NULL, 'c')])",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        assert_rows_eq(
            session
                .execute("SELECT COUNT(*) FROM lix_diff('rr_checkpoint_parent')", &[])
                .await
                .unwrap(),
            vec![vec![Value::Integer(0)]],
        );
        assert_rows_eq(
            session
                .execute("SELECT COUNT(*) FROM lix_diff('rr_checkpoint_child')", &[])
                .await
                .unwrap(),
            vec![vec![Value::Integer(0)]],
        );

        session
            .execute(
                "UPDATE rr_checkpoint_parent SET value='after-parent' WHERE id='p'",
                &[],
            )
            .await
            .unwrap();
        session
            .execute(
                "UPDATE rr_checkpoint_child SET value='after-child' WHERE id='c'",
                &[],
            )
            .await
            .unwrap();
        session
            .execute(
                "SELECT commit_id FROM lix_restore($1, ARRAY[lix_row_ref('rr_checkpoint_child',NULL,'c')])",
                &[Value::Text(checkpoint)],
            )
            .await
            .unwrap();
        assert_rows_eq(
            session
                .execute("SELECT value FROM rr_checkpoint_parent WHERE id='p'", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("before".into())]],
        );
        assert_rows_eq(
            session
                .execute("SELECT value FROM rr_checkpoint_child WHERE id='c'", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("before".into())]],
        );
    }
);

simulation_test!(
    row_ref_cascade_supports_null_and_composite_targets,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let mut composite_parent = schema(
            "rr_composite_parent",
            json!([
                {"name":"tenant","type":"text","nullable":false},
                {"name":"number","type":"int8","nullable":false}
            ]),
            None,
        );
        composite_parent["primary_key"] = json!(["tenant", "number"]);
        register(&session, composite_parent).await;
        register(
            &session,
            row_ref_child_schema("rr_composite_child", "cascade"),
        )
        .await;
        session
            .execute(
                "INSERT INTO rr_composite_parent(tenant,number) VALUES ('acme',7)",
                &[],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO rr_composite_child(id,target) VALUES \
                 ('linked',lix_row_ref('rr_composite_parent',NULL,'acme',7)), \
                 ('null-target',NULL)",
                &[],
            )
            .await
            .unwrap();
        session
            .execute(
                "DELETE FROM rr_composite_parent WHERE tenant='acme' AND number=7",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_composite_child ORDER BY id", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("null-target".into())]],
        );
    }
);

simulation_test!(
    row_ref_cascade_cycles_and_no_action_reject_invalid_targets_and_deletes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register(&session, row_ref_child_schema("rr_cycle", "cascade")).await;
        register(&session, parent_schema("rr_restrict_parent")).await;
        register(&session, row_ref_child_default_schema("rr_restrict_child")).await;

        let mut cycle = session.begin_transaction().await.unwrap();
        cycle
            .execute(
                "INSERT INTO rr_cycle(id,target) VALUES \
             ('a',lix_row_ref('rr_cycle',NULL,'b')), \
             ('b',lix_row_ref('rr_cycle',NULL,'a'))",
                &[],
            )
            .await
            .unwrap();
        cycle.commit().await.unwrap();
        session
            .execute("DELETE FROM rr_cycle WHERE id='a'", &[])
            .await
            .unwrap();
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_cycle", &[])
                .await
                .unwrap(),
            vec![],
        );

        let error = session
        .execute(
            "INSERT INTO rr_restrict_child(id,target) VALUES ('invalid', lix_row_ref('rr_restrict_parent',NULL,'missing'))",
            &[],
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, lix::LixError::CODE_FOREIGN_KEY);

        session
            .execute("INSERT INTO rr_restrict_parent(id) VALUES ('p')", &[])
            .await
            .unwrap();
        session
        .execute(
            "INSERT INTO rr_restrict_child(id,target) VALUES ('c', lix_row_ref('rr_restrict_parent',NULL,'p'))",
            &[],
        )
        .await
        .unwrap();
        let error = session
            .execute("DELETE FROM rr_restrict_parent WHERE id='p'", &[])
            .await
            .unwrap_err();
        assert_eq!(error.code, lix::LixError::CODE_FOREIGN_KEY);

        let error = session
            .execute("DELETE FROM rr_restrict_parent", &[])
            .await
            .unwrap_err();
        assert_eq!(error.code, lix::LixError::CODE_FOREIGN_KEY);
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_restrict_parent", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("p".into())]],
        );
        assert_rows_eq(
            session
                .execute("SELECT id FROM rr_restrict_child", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("c".into())]],
        );
    }
);

simulation_test!(row_ref_cascade_is_branch_isolated, |sim| async move {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    register(&main, parent_schema("rr_branch_parent")).await;
    register(&main, row_ref_child_schema("rr_branch_child", "cascade")).await;
    main.execute("INSERT INTO rr_branch_parent(id) VALUES ('p')", &[])
        .await
        .unwrap();
    main.execute(
        "INSERT INTO rr_branch_child(id,target) VALUES ('c', lix_row_ref('rr_branch_parent',NULL,'p'))",
        &[],
    )
    .await
    .unwrap();
    let branch = main
        .create_branch(CreateBranchOptions {
            id: None,
            name: "row-ref-branch-isolation".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let branch_session =
        sim.wrap_session(engine.open_session_at(branch.id).await.unwrap(), &engine);

    branch_session
        .execute("DELETE FROM rr_branch_parent WHERE id='p'", &[])
        .await
        .unwrap();
    assert_rows_eq(
        main.execute("SELECT id FROM rr_branch_parent", &[])
            .await
            .unwrap(),
        vec![vec![Value::Text("p".into())]],
    );
    assert_rows_eq(
        main.execute("SELECT id FROM rr_branch_child", &[])
            .await
            .unwrap(),
        vec![vec![Value::Text("c".into())]],
    );
    assert_rows_eq(
        branch_session
            .execute("SELECT id FROM rr_branch_parent", &[])
            .await
            .unwrap(),
        vec![],
    );
    assert_rows_eq(
        branch_session
            .execute("SELECT id FROM rr_branch_child", &[])
            .await
            .unwrap(),
        vec![],
    );
});

simulation_test!(
    row_ref_cascade_merge_preview_matches_execution_destination_delete,
    |sim| async move {
        assert_row_ref_cascade_merge(&sim, true).await;
    }
);

simulation_test!(
    row_ref_cascade_merge_preview_matches_execution_source_delete,
    |sim| async move {
        assert_row_ref_cascade_merge(&sim, false).await;
    }
);

async fn assert_row_ref_cascade_merge(
    sim: &crate::support::simulation_test::engine::Simulation,
    deletion_on_destination: bool,
) {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    register(&main, parent_schema("rr_merge_parent")).await;
    register(&main, row_ref_child_schema("rr_merge_child", "cascade")).await;
    main.execute("INSERT INTO rr_merge_parent(id) VALUES ('p')", &[])
        .await
        .unwrap();
    let branch = main
        .create_branch(CreateBranchOptions {
            id: None,
            name: "row-ref-cascade-merge".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let source = sim.wrap_session(
        engine.open_session_at(branch.id.clone()).await.unwrap(),
        &engine,
    );
    let (deleted, replying) = if deletion_on_destination {
        (&main, &source)
    } else {
        (&source, &main)
    };
    deleted
        .execute("DELETE FROM rr_merge_parent WHERE id='p'", &[])
        .await
        .unwrap();
    replying
        .execute(
            "INSERT INTO rr_merge_child(id,target) VALUES ('c', lix_row_ref('rr_merge_parent',NULL,'p'))",
            &[],
        )
        .await
        .unwrap();

    let preview = main
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: branch.id.clone(),
        })
        .await
        .unwrap();
    let receipt = main
        .merge_branch(MergeBranchOptions {
            source_branch_id: branch.id,
        })
        .await
        .unwrap();
    assert_eq!(preview.change_stats, receipt.change_stats);
    assert_rows_eq(
        main.execute("SELECT id FROM rr_merge_parent", &[])
            .await
            .unwrap(),
        vec![],
    );
    assert_rows_eq(
        main.execute("SELECT id FROM rr_merge_child", &[])
            .await
            .unwrap(),
        vec![],
    );
}

simulation_test!(
    row_ref_cascade_merge_incoming_schema_destination_delete,
    |sim| async move {
        assert_row_ref_incoming_schema_merge(&sim, true).await;
    }
);

simulation_test!(
    row_ref_cascade_merge_incoming_schema_source_delete,
    |sim| async move {
        assert_row_ref_incoming_schema_merge(&sim, false).await;
    }
);

async fn assert_row_ref_incoming_schema_merge(
    sim: &crate::support::simulation_test::engine::Simulation,
    deletion_on_destination: bool,
) {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    register(&main, parent_schema("rr_incoming_parent")).await;
    main.execute("INSERT INTO rr_incoming_parent(id) VALUES ('p')", &[])
        .await
        .unwrap();
    let branch = main
        .create_branch(CreateBranchOptions {
            id: None,
            name: "row-ref-incoming-schema".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let source = sim.wrap_session(
        engine.open_session_at(branch.id.clone()).await.unwrap(),
        &engine,
    );
    let (deleted, replying) = if deletion_on_destination {
        (&main, &source)
    } else {
        (&source, &main)
    };
    deleted
        .execute("DELETE FROM rr_incoming_parent WHERE id='p'", &[])
        .await
        .unwrap();
    register(
        replying,
        row_ref_child_schema("rr_incoming_child", "cascade"),
    )
    .await;
    replying
        .execute(
            "INSERT INTO rr_incoming_child(id,target) VALUES ('c', lix_row_ref('rr_incoming_parent',NULL,'p'))",
            &[],
        )
        .await
        .unwrap();

    let preview = main
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: branch.id.clone(),
        })
        .await
        .unwrap();
    let receipt = main
        .merge_branch(MergeBranchOptions {
            source_branch_id: branch.id,
        })
        .await
        .unwrap();
    assert_eq!(preview.change_stats, receipt.change_stats);
    assert_rows_eq(
        main.execute("SELECT id FROM rr_incoming_parent", &[])
            .await
            .unwrap(),
        vec![],
    );
    assert_rows_eq(
        main.execute("SELECT id FROM rr_incoming_child", &[])
            .await
            .unwrap(),
        vec![],
    );
}

simulation_test!(
    row_ref_cascade_merge_incoming_generation_destination_delete,
    |sim| async move {
        assert_row_ref_incoming_generation_merge(&sim, true).await;
    }
);

simulation_test!(
    row_ref_cascade_merge_incoming_generation_source_delete,
    |sim| async move {
        assert_row_ref_incoming_generation_merge(&sim, false).await;
    }
);

simulation_test!(
    global_row_ref_source_does_not_expand_local_generation_delete_on_merge,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let global = sim.wrap_session(
            engine.open_session_at(lix::GLOBAL_BRANCH_ID).await.unwrap(),
            &engine,
        );

        register_global(&global, parent_schema("rr_global_probe_parent")).await;
        register_global(
            &global,
            row_ref_child_schema("rr_global_probe_child", "cascade"),
        )
        .await;
        register(&main, parent_schema("rr_local_generation_probe")).await;

        global
            .execute(
            "INSERT INTO rr_global_probe_parent(id,lixcol_global) VALUES ('global-parent',true)",
            &[],
        )
        .await
        .expect("global target row should insert");
        global
            .execute(
            "INSERT INTO rr_global_probe_child(id,target,lixcol_global) \
             VALUES ('global-child',lix_row_ref('rr_global_probe_parent',NULL,'global-parent'),true)",
            &[],
        )
        .await
        .expect("global row-ref source should insert");
        main.execute(
            "INSERT INTO rr_local_generation_probe(id) VALUES ('local-a'),('local-b')",
            &[],
        )
        .await
        .expect("local collection rows should insert");

        let branch = main
            .create_branch(CreateBranchOptions {
                id: None,
                name: "local-generation-with-global-row-ref".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let source = sim.wrap_session(
            engine.open_session_at(branch.id.clone()).await.unwrap(),
            &engine,
        );
        source
            .execute("DELETE FROM rr_local_generation_probe", &[])
            .await
            .expect("local collection should be deleted on the branch");

        let preview = main
            .merge_branch_preview(MergeBranchPreviewOptions {
                source_branch_id: branch.id.clone(),
            })
            .await
            .unwrap();
        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: branch.id,
            })
            .await
            .unwrap();
        assert_eq!(preview.change_stats, receipt.change_stats);

        assert_rows_eq(
            main.execute("SELECT id FROM rr_local_generation_probe", &[])
                .await
                .unwrap(),
            vec![],
        );
        assert_rows_eq(
            main.execute("SELECT id FROM rr_global_probe_child", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("global-child".into())]],
        );
        let member_tombstones = main
            .execute(
                "SELECT count(*) AS n FROM lix_change \
                 WHERE schema_key = 'rr_local_generation_probe' \
                   AND row_pk IN (CAST('[\"local-a\"]' AS JSONB), CAST('[\"local-b\"]' AS JSONB)) \
                   AND snapshot_content IS NULL",
                &[],
            )
            .await
            .expect("lix_change should expose collection row tombstones")
            .rows()[0]
            .get::<i64>("n")
            .unwrap();
        assert_eq!(
            member_tombstones, 0,
            "collection deletion should stay a generation marker"
        );
    }
);

simulation_test!(
    local_row_ref_source_is_not_hidden_by_an_earlier_global_source,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let global = sim.wrap_session(
            engine.open_session_at(lix::GLOBAL_BRANCH_ID).await.unwrap(),
            &engine,
        );

        register_global(&global, parent_schema("rr_mixed_global_parent")).await;
        register_global(
            &global,
            row_ref_child_schema("rr_mixed_scope_child", "cascade"),
        )
        .await;
        register(&main, parent_schema("rr_mixed_local_parent")).await;
        register(
            &main,
            row_ref_child_schema("rr_mixed_scope_child", "cascade"),
        )
        .await;
        global
            .execute(
                "INSERT INTO rr_mixed_global_parent(id,lixcol_global) \
                 VALUES ('global-target',true)",
                &[],
            )
            .await
            .expect("global parent should insert");
        global
            .execute(
                "INSERT INTO rr_mixed_scope_child(id,target,lixcol_global) \
                 VALUES ('a-global-source',lix_row_ref('rr_mixed_global_parent',NULL,'global-target'),true)",
                &[],
            )
            .await
            .expect("global source should insert");
        main.execute(
            "INSERT INTO rr_mixed_local_parent(id) VALUES ('local-target')",
            &[],
        )
        .await
        .expect("local parent should insert");
        main.execute(
            "INSERT INTO rr_mixed_scope_child(id,target) \
             VALUES ('z-local-source',lix_row_ref('rr_mixed_local_parent',NULL,'local-target'))",
            &[],
        )
        .await
        .expect("local source should insert");

        main.execute("DELETE FROM rr_mixed_local_parent", &[])
            .await
            .expect("local parent deletion should cascade its local source");
        assert_rows_eq(
            main.execute("SELECT id FROM rr_mixed_local_parent", &[])
                .await
                .unwrap(),
            vec![],
        );
        assert_rows_eq(
            main.execute("SELECT id FROM rr_mixed_scope_child", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("a-global-source".into())]],
        );
        assert_rows_eq(
            global
                .execute("SELECT id FROM rr_mixed_global_parent", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("global-target".into())]],
        );
    }
);

async fn assert_row_ref_incoming_generation_merge(
    sim: &crate::support::simulation_test::engine::Simulation,
    deletion_on_destination: bool,
) {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    register(&main, parent_schema("rr_generation_parent")).await;
    main.execute("INSERT INTO rr_generation_parent(id) VALUES ('p')", &[])
        .await
        .unwrap();
    let branch = main
        .create_branch(CreateBranchOptions {
            id: None,
            name: "row-ref-incoming-generation".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let source = sim.wrap_session(
        engine.open_session_at(branch.id.clone()).await.unwrap(),
        &engine,
    );
    if deletion_on_destination {
        main.execute("DELETE FROM rr_generation_parent", &[])
            .await
            .unwrap();
        register(
            &source,
            row_ref_child_schema("rr_generation_child", "cascade"),
        )
        .await;
        source
            .execute(
                "INSERT INTO rr_generation_child(id,target) VALUES ('c', lix_row_ref('rr_generation_parent',NULL,'p'))",
                &[],
            )
            .await
            .unwrap();
    } else {
        source
            .execute("DELETE FROM rr_generation_parent", &[])
            .await
            .unwrap();
        register(
            &main,
            row_ref_child_schema("rr_generation_child", "cascade"),
        )
        .await;
        main.execute(
            "INSERT INTO rr_generation_child(id,target) VALUES ('c', lix_row_ref('rr_generation_parent',NULL,'p'))",
            &[],
        )
        .await
        .unwrap();
    }

    let preview = main
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: branch.id.clone(),
        })
        .await
        .unwrap();
    let receipt = main
        .merge_branch(MergeBranchOptions {
            source_branch_id: branch.id,
        })
        .await
        .unwrap();
    assert_eq!(preview.change_stats, receipt.change_stats);
    assert_rows_eq(
        main.execute("SELECT id FROM rr_generation_parent", &[])
            .await
            .unwrap(),
        vec![],
    );
    assert_rows_eq(
        main.execute("SELECT id FROM rr_generation_child", &[])
            .await
            .unwrap(),
        vec![],
    );
}

simulation_test!(
    row_ref_cascade_removes_plugin_produced_target,
    |sim| async move {
        // `Simulation::boot_engine` is intentionally a bare engine and does
        // not install the optional component runtime. Plugin-backed fixtures
        // use the same public open path as the plugin compatibility tests.
        let workspace = lix::open_lix()
            .with_storage(sim.storage())
            .with_wasm_runtime(lix::default_wasm_runtime().unwrap())
            .await
            .unwrap();
        workspace
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/.lix/plugins/plugin_csv.lixplugin', $1)",
                &[Value::Blob(
                    include_bytes!("../../fixtures/plugin-api/v2/plugin_csv.lixplugin")
                        .to_vec()
                        .into(),
                )],
            )
            .await
            .unwrap();
        workspace
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/people.csv', $1)",
                &[Value::Blob(b"name,age\nAda,36\nGrace,37\n".to_vec().into())],
            )
            .await
            .unwrap();
        let value = row_ref_child_schema("rr_plugin_link", "cascade");
        workspace
            .execute(
                "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
                &[Value::Text(value.to_string())],
            )
            .await
            .unwrap();

        let rows = workspace
            .execute(
                "SELECT id, lixcol_file_id FROM csv_row ORDER BY order_key",
                &[],
            )
            .await
            .unwrap();
        assert!(
            rows.rows().len() >= 3,
            "the CSV plugin should produce header and data rows"
        );
        let target = rows.rows().last().unwrap();
        let target_id = target.get::<String>("id").unwrap();
        let file_id = target.get::<String>("lixcol_file_id").unwrap();
        workspace
            .execute(
                "INSERT INTO rr_plugin_link(id,target) VALUES ('link', lix_row_ref('csv_row',$1,$2))",
                &[Value::Text(file_id.clone()), Value::Text(target_id.clone())],
            )
            .await
            .unwrap();

        // The plugin update removes the final CSV row and emits its deletion
        // as a generated state write. The row-reference cascade must consume
        // that generated deletion in the same publication.
        workspace
            .execute(
                "UPDATE lix_file SET content = $1 WHERE path = '/people.csv'",
                &[Value::Blob(b"name,age\nAda,36\n".to_vec().into())],
            )
            .await
            .unwrap();
        assert_rows_eq(
            workspace
                .execute(
                    "SELECT id FROM csv_row WHERE id = $1",
                    &[Value::Text(target_id)],
                )
                .await
                .unwrap(),
            vec![],
        );
        assert_rows_eq(
            workspace
                .execute("SELECT id FROM rr_plugin_link", &[])
                .await
                .unwrap(),
            vec![],
        );
    }
);

simulation_test!(
    row_ref_cascade_removes_plugin_target_on_file_delete,
    |sim| async move {
        assert_row_ref_plugin_file_delete(&sim, "cascade", false).await;
    }
);

simulation_test!(
    row_ref_no_action_rejects_plugin_target_file_delete,
    |sim| async move {
        assert_row_ref_plugin_file_delete(&sim, "no_action", true).await;
    }
);

simulation_test!(
    row_ref_cascade_merge_plugin_file_delete_destination_delete,
    |sim| async move {
        assert_row_ref_plugin_file_delete_merge(&sim, true, "cascade").await;
    }
);

simulation_test!(
    row_ref_cascade_merge_plugin_file_delete_source_delete,
    |sim| async move {
        assert_row_ref_plugin_file_delete_merge(&sim, false, "cascade").await;
    }
);

simulation_test!(
    row_ref_no_action_merge_plugin_file_delete_destination_delete,
    |sim| async move {
        assert_row_ref_plugin_file_delete_merge(&sim, true, "no_action").await;
    }
);

simulation_test!(
    row_ref_no_action_merge_plugin_file_delete_source_delete,
    |sim| async move {
        assert_row_ref_plugin_file_delete_merge(&sim, false, "no_action").await;
    }
);

simulation_test!(
    row_ref_cascade_merge_plugin_file_delete_incoming_schema_destination_delete,
    |sim| async move {
        assert_row_ref_plugin_file_delete_merge_incoming_schema(&sim, true).await;
    }
);

simulation_test!(
    row_ref_cascade_merge_plugin_file_delete_incoming_schema_source_delete,
    |sim| async move {
        assert_row_ref_plugin_file_delete_merge_incoming_schema(&sim, false).await;
    }
);

async fn assert_row_ref_plugin_file_delete(
    sim: &crate::support::simulation_test::engine::Simulation,
    on_delete: &str,
    expect_restriction: bool,
) {
    let (workspace, file_id, target_id) =
        open_row_ref_plugin_workspace(sim, "rr_plugin_file_delete_link", on_delete, true).await;
    workspace
        .execute(
            "INSERT INTO rr_plugin_file_delete_link(id,target) \
             VALUES ('link', lix_row_ref('csv_row',$1,$2))",
            &[Value::Text(file_id.clone()), Value::Text(target_id.clone())],
        )
        .await
        .unwrap();

    let delete = workspace
        .execute(
            "DELETE FROM lix_file WHERE id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await;
    if expect_restriction {
        let error = delete.expect_err("no_action must block deleting a referenced file");
        assert_eq!(error.code, lix::LixError::CODE_FOREIGN_KEY);
        assert_rows_eq(
            workspace
                .execute("SELECT id FROM rr_plugin_file_delete_link", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("link".into())]],
        );
        assert_rows_eq(
            workspace
                .execute(
                    "SELECT id FROM csv_row WHERE id = $1",
                    &[Value::Text(target_id.clone())],
                )
                .await
                .unwrap(),
            vec![vec![Value::Text(target_id)]],
        );
    } else {
        delete.expect("cascade should allow deleting the file and remove its target");
        assert_rows_eq(
            workspace
                .execute("SELECT id FROM rr_plugin_file_delete_link", &[])
                .await
                .unwrap(),
            Vec::<Vec<Value>>::new(),
        );
        assert_rows_eq(
            workspace
                .execute(
                    "SELECT id FROM csv_row WHERE id = $1",
                    &[Value::Text(target_id)],
                )
                .await
                .unwrap(),
            Vec::<Vec<Value>>::new(),
        );
    }
}

async fn open_row_ref_plugin_workspace(
    sim: &crate::support::simulation_test::engine::Simulation,
    schema_key: &str,
    on_delete: &str,
    register_schema: bool,
) -> (lix::Lix<lix::Memory>, String, String) {
    let workspace = lix::open_lix()
        .with_storage(sim.storage())
        .with_wasm_runtime(lix::default_wasm_runtime().unwrap())
        .await
        .unwrap();
    workspace
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/.lix/plugins/plugin_csv.lixplugin', $1)",
            &[Value::Blob(
                include_bytes!("../../fixtures/plugin-api/v2/plugin_csv.lixplugin")
                    .to_vec()
                    .into(),
            )],
        )
        .await
        .unwrap();
    workspace
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/people.csv', $1)",
            &[Value::Blob(b"name,age\nAda,36\nGrace,37\n".to_vec().into())],
        )
        .await
        .unwrap();
    if register_schema {
        let value = row_ref_child_schema(schema_key, on_delete);
        workspace
            .execute(
                "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
                &[Value::Text(value.to_string())],
            )
            .await
            .unwrap();
    }

    let rows = workspace
        .execute(
            "SELECT id, lixcol_file_id FROM csv_row ORDER BY order_key",
            &[],
        )
        .await
        .unwrap();
    assert!(rows.rows().len() >= 3, "the CSV plugin should produce rows");
    let target = rows.rows().last().unwrap();
    (
        workspace,
        target.get::<String>("lixcol_file_id").unwrap(),
        target.get::<String>("id").unwrap(),
    )
}

async fn assert_row_ref_plugin_file_delete_merge(
    sim: &crate::support::simulation_test::engine::Simulation,
    deletion_on_destination: bool,
    on_delete: &str,
) {
    let (workspace, file_id, target_id) =
        open_row_ref_plugin_workspace(sim, "rr_plugin_merge_link", on_delete, true).await;
    let branch = workspace
        .create_branch(CreateBranchOptions {
            id: None,
            name: "row-ref-plugin-file-delete-merge".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let source = workspace
        .open_another_session()
        .with_branch(branch.id.clone())
        .await
        .unwrap();

    let delete_sql = "DELETE FROM lix_file WHERE id = $1";
    let insert_sql =
        "INSERT INTO rr_plugin_merge_link(id,target) VALUES ('link', lix_row_ref('csv_row',$1,$2))";
    if deletion_on_destination {
        workspace
            .execute(delete_sql, &[Value::Text(file_id.clone())])
            .await
            .unwrap();
        source
            .execute(
                insert_sql,
                &[Value::Text(file_id.clone()), Value::Text(target_id.clone())],
            )
            .await
            .unwrap();
    } else {
        source
            .execute(delete_sql, &[Value::Text(file_id.clone())])
            .await
            .unwrap();
        workspace
            .execute(
                insert_sql,
                &[Value::Text(file_id.clone()), Value::Text(target_id.clone())],
            )
            .await
            .unwrap();
    }

    let preview = workspace
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: branch.id.clone(),
        })
        .await;
    if on_delete == "no_action" {
        let preview_error = preview.expect_err("no_action merge preview must reject the orphan");
        let execution_error = workspace
            .merge_branch(MergeBranchOptions {
                source_branch_id: branch.id,
            })
            .await
            .expect_err("no_action merge execution must reject the orphan");
        assert_eq!(preview_error.code, lix::LixError::CODE_FOREIGN_KEY);
        assert_eq!(execution_error.code, preview_error.code);
        if deletion_on_destination {
            assert_rows_eq(
                workspace
                    .execute(
                        "SELECT id FROM lix_file WHERE id = $1",
                        &[Value::Text(file_id)],
                    )
                    .await
                    .unwrap(),
                vec![],
            );
            assert_rows_eq(
                workspace
                    .execute("SELECT id FROM rr_plugin_merge_link", &[])
                    .await
                    .unwrap(),
                vec![],
            );
            assert_rows_eq(
                workspace
                    .execute(
                        "SELECT id FROM csv_row WHERE id = $1",
                        &[Value::Text(target_id.clone())],
                    )
                    .await
                    .unwrap(),
                vec![],
            );
        } else {
            assert_rows_eq(
                workspace
                    .execute(
                        "SELECT id FROM lix_file WHERE id = $1",
                        &[Value::Text(file_id.clone())],
                    )
                    .await
                    .unwrap(),
                vec![vec![Value::Text(file_id)]],
            );
            assert_rows_eq(
                workspace
                    .execute("SELECT id FROM rr_plugin_merge_link", &[])
                    .await
                    .unwrap(),
                vec![vec![Value::Text("link".into())]],
            );
            assert_rows_eq(
                workspace
                    .execute(
                        "SELECT id FROM csv_row WHERE id = $1",
                        &[Value::Text(target_id.clone())],
                    )
                    .await
                    .unwrap(),
                vec![vec![Value::Text(target_id)]],
            );
        }
        return;
    }
    let preview = preview.unwrap();
    let receipt = workspace
        .merge_branch(MergeBranchOptions {
            source_branch_id: branch.id,
        })
        .await
        .unwrap();
    assert_eq!(preview.change_stats, receipt.change_stats);
    assert_rows_eq(
        workspace
            .execute(
                "SELECT id FROM csv_row WHERE id = $1",
                &[Value::Text(target_id)],
            )
            .await
            .unwrap(),
        vec![],
    );
    assert_rows_eq(
        workspace
            .execute("SELECT id FROM rr_plugin_merge_link", &[])
            .await
            .unwrap(),
        vec![],
    );
}

async fn assert_row_ref_plugin_file_delete_merge_incoming_schema(
    sim: &crate::support::simulation_test::engine::Simulation,
    deletion_on_destination: bool,
) {
    let (workspace, file_id, target_id) =
        open_row_ref_plugin_workspace(sim, "rr_plugin_incoming_link", "cascade", false).await;
    let branch = workspace
        .create_branch(CreateBranchOptions {
            id: None,
            name: "row-ref-plugin-incoming-schema-file-delete".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let source = workspace
        .open_another_session()
        .with_branch(branch.id.clone())
        .await
        .unwrap();
    let schema_value = row_ref_child_schema("rr_plugin_incoming_link", "cascade");
    let register_sql = "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)";
    let delete_sql = "DELETE FROM lix_file WHERE id = $1";
    let insert_sql = "INSERT INTO rr_plugin_incoming_link(id,target) \
         VALUES ('link', lix_row_ref('csv_row',$1,$2))";

    if deletion_on_destination {
        workspace
            .execute(delete_sql, &[Value::Text(file_id.clone())])
            .await
            .unwrap();
        source
            .execute(register_sql, &[Value::Text(schema_value.to_string())])
            .await
            .unwrap();
        source
            .execute(
                insert_sql,
                &[Value::Text(file_id.clone()), Value::Text(target_id.clone())],
            )
            .await
            .unwrap();
    } else {
        source
            .execute(delete_sql, &[Value::Text(file_id.clone())])
            .await
            .unwrap();
        workspace
            .execute(register_sql, &[Value::Text(schema_value.to_string())])
            .await
            .unwrap();
        workspace
            .execute(
                insert_sql,
                &[Value::Text(file_id.clone()), Value::Text(target_id.clone())],
            )
            .await
            .unwrap();
    }

    let preview = workspace
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: branch.id.clone(),
        })
        .await
        .unwrap();
    let receipt = workspace
        .merge_branch(MergeBranchOptions {
            source_branch_id: branch.id,
        })
        .await
        .unwrap();
    assert_eq!(preview.change_stats, receipt.change_stats);
    assert_rows_eq(
        workspace
            .execute(
                "SELECT id FROM csv_row WHERE id = $1",
                &[Value::Text(target_id)],
            )
            .await
            .unwrap(),
        vec![],
    );
    assert_rows_eq(
        workspace
            .execute("SELECT id FROM rr_plugin_incoming_link", &[])
            .await
            .unwrap(),
        vec![],
    );
}

simulation_test!(
    row_ref_apply_closes_source_only_historical_row_ref_target,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let target = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let root = sim.initial_commit_id().to_string();
        let source_branch = target
            .create_branch(CreateBranchOptions {
                id: None,
                name: "row-ref-historical-source".into(),
                from_commit_id: Some(root.clone()),
            })
            .await
            .unwrap();
        let source = sim.wrap_session(
            engine
                .open_session_at(source_branch.id.clone())
                .await
                .unwrap(),
            &engine,
        );
        register(&source, parent_schema("rr_historical_only_parent")).await;
        register(
            &source,
            row_ref_child_schema("rr_historical_only_child", "no_action"),
        )
        .await;
        source
            .execute(
                "INSERT INTO rr_historical_only_parent(id) VALUES ('p')",
                &[],
            )
            .await
            .unwrap();
        source
            .execute(
                "INSERT INTO rr_historical_only_child(id,target) VALUES ('c', lix_row_ref('rr_historical_only_parent',NULL,'p'))",
                &[],
            )
            .await
            .unwrap();
        let parent_ref = source
            .execute(
                "SELECT lix_row_ref('rr_historical_only_parent',NULL,'p') AS row_ref",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<lix::RowRef>("row_ref")
            .unwrap();
        let source_head = source
            .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        let source_diff = source
            .execute(
                "SELECT row_ref FROM lix_diff('rr_historical_only_child',$1,$2) WHERE id='c'",
                &[Value::Text(root.clone()), Value::Text(source_head.clone())],
            )
            .await
            .unwrap();
        let [Value::RowRef(source_row_ref)] = source_diff.rows()[0].values() else {
            panic!("historical source diff must return a typed row reference");
        };

        target
            .execute(
                "SELECT lix_row_ref('rr_historical_only_child',NULL,'c')",
                &[],
            )
            .await
            .expect_err("the target catalog must not know the source-only relation");
        target
            .execute(
                "SELECT commit_id FROM lix_apply($1,$2,ARRAY[$3])",
                &[
                    Value::Text(root),
                    Value::Text(source_head),
                    Value::RowRef(source_row_ref.clone()),
                ],
            )
            .await
            .expect("apply must decode the selected row with the historical source catalog");
        assert_rows_eq(
            target
                .execute("SELECT id FROM rr_historical_only_parent", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("p".into())]],
        );
        assert_rows_eq(
            target
                .execute("SELECT id,target FROM rr_historical_only_child", &[])
                .await
                .unwrap(),
            vec![vec![
                Value::Text("c".into()),
                Value::Text(parent_ref.as_str().to_owned()),
            ]],
        );
    }
);
