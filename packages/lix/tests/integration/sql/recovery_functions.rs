use super::select_rows;
use crate::support::simulation_test::engine::SimSession;
use lix::Value;

async fn commit(session: &SimSession, sql: &str, params: &[Value]) -> String {
    let result = session
        .execute(sql, params)
        .await
        .expect("command succeeds");
    assert_eq!(result.rows().len(), 1);
    match result.rows()[0].values() {
        [Value::Text(id)] => id.clone(),
        other => panic!("expected commit receipt, got {other:?}"),
    }
}

async fn head(session: &SimSession) -> String {
    commit(
        session,
        "SELECT lix_active_branch_commit_id() AS commit_id",
        &[],
    )
    .await
}

simulation_test!(
    recovery_functions_explicit_versions_and_row_refs,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('a','old'), ('b','old')",
                &[],
            )
            .await
            .unwrap();
        let baseline = commit(
            &session,
            "SELECT commit_id FROM lix_create_checkpoint()",
            &[],
        )
        .await;
        session
            .execute(
                "UPDATE lix_key_value SET value = 'new' WHERE key = 'a'",
                &[],
            )
            .await
            .unwrap();
        let change = head(&session).await;
        session
            .execute(
                "UPDATE lix_key_value SET value = 'unrelated' WHERE key = 'b'",
                &[],
            )
            .await
            .unwrap();
        let before_revert = head(&session).await;
        let inverse = commit(
            &session,
            "SELECT commit_id FROM lix_revert($1, ARRAY[lix_row_ref('lix_key_value', 'a')])",
            &[Value::Text(change.clone())],
        )
        .await;
        assert_ne!(inverse, baseline);
        let rows = select_rows(
            &session,
            "SELECT key,value FROM lix_key_value WHERE key IN ('a','b') ORDER BY key",
        )
        .await;
        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Text("a".into()),
                    Value::Jsonb(serde_json::json!("old").into())
                ],
                vec![
                    Value::Text("b".into()),
                    Value::Jsonb(serde_json::json!("unrelated").into())
                ]
            ]
        );
        let parents = select_rows(
            &session,
            &format!("SELECT parent_commit_id FROM lix_log() WHERE commit_id = '{inverse}'"),
        )
        .await;
        assert_eq!(parents, vec![vec![Value::Text(before_revert)]]);
        // A restored version has a new change identity; use its actual endpoint for replay.
        commit(
            &session,
            "SELECT commit_id FROM lix_apply($1, $2, ARRAY[lix_row_ref('lix_key_value', 'a')])",
            &[Value::Text(inverse), Value::Text(change.clone())],
        )
        .await;
        let before_restore = head(&session).await;
        let restored = commit(
            &session,
            "SELECT commit_id FROM lix_restore($1)",
            &[Value::Text(baseline.clone())],
        )
        .await;
        assert_ne!(restored, baseline);
        assert_eq!(
            select_rows(
                &session,
                &format!("SELECT parent_commit_id FROM lix_log() WHERE commit_id = '{restored}'")
            )
            .await,
            vec![vec![Value::Text(before_restore)]]
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT (SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()) AS id"
            )
            .await,
            vec![vec![Value::Text(baseline)]]
        );
        for sql in [
            "SELECT commit_id FROM lix_restore($1)",
            "SELECT commit_id FROM lix_restore($1, ARRAY[lix_row_ref('lix_key_value', 'a')])",
            "SELECT commit_id FROM lix_revert($1, ARRAY[])",
            "SELECT commit_id FROM lix_apply($1, $1, ARRAY[])",
            "SELECT commit_id FROM lix_revert_range($1, $1)",
        ] {
            let receipt = session
                .execute(sql, &[Value::Text(restored.clone())])
                .await
                .unwrap();
            assert_eq!(receipt.rows().len(), 1);
            assert_eq!(receipt.rows()[0].values(), &[Value::Null]);
            assert_eq!(receipt.rows_affected(), 0);
            assert_eq!(head(&session).await, restored);
        }
        let receipt = session
            .execute(
                "SELECT commit_id FROM lix_restore($2, ARRAY[lix_row_ref('lix_key_value', $1)])",
                &[Value::Text("a".into()), Value::Text(restored.clone())],
            )
            .await
            .unwrap();
        assert_eq!(receipt.rows()[0].values(), &[Value::Null]);
        session
            .execute(
                "SELECT commit_id FROM lix_restore($1)",
                &[Value::Text(restored), Value::Text("unused".into())],
            )
            .await
            .expect_err("extra outer bindings must be rejected");
    }
);

simulation_test!(
    recovery_functions_range_conflicts_and_query_selection,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('a','old'),('b','old')",
                &[],
            )
            .await
            .unwrap();
        let before = head(&session).await;
        session
            .execute(
                "UPDATE lix_key_value SET value = 'new' WHERE key IN ('a','b')",
                &[],
            )
            .await
            .unwrap();
        let after = head(&session).await;
        session
            .execute(
                "UPDATE lix_key_value SET value = 'later' WHERE key = 'b'",
                &[],
            )
            .await
            .unwrap();
        let current = head(&session).await;
        session
            .execute(
                "SELECT commit_id FROM lix_revert_range($1,$2)",
                &[Value::Text(before.clone()), Value::Text(after.clone())],
            )
            .await
            .expect_err("conflicting range fails atomically");
        assert_eq!(head(&session).await, current);
        assert_eq!(
            select_rows(&session, "SELECT value FROM lix_key_value WHERE key = 'a'").await,
            vec![vec![Value::Jsonb(serde_json::json!("new").into())]]
        );
        commit(&session, "SELECT commit_id FROM lix_revert_range($1,$2,ARRAY(SELECT row_ref FROM lix_diff('lix_key_value',$1,$2) WHERE key='a'))", &[Value::Text(before.clone()),Value::Text(after)]).await;
        assert_eq!(
            select_rows(&session, "SELECT value FROM lix_key_value WHERE key = 'a'").await,
            vec![vec![Value::Jsonb(serde_json::json!("old").into())]]
        );
        for sql in [
            "SELECT commit_id FROM lix_restore($1,NULL)",
            "SELECT commit_id FROM lix_revert($1,NULL)",
            "SELECT commit_id FROM lix_apply($1,$1,NULL)",
            "SELECT commit_id FROM lix_revert_range($1,$1,NULL)",
            "SELECT commit_id FROM lix_restore(NULL)",
            "SELECT * FROM lix_restore($1)",
            "SELECT commit_id FROM lix_restore($1) WHERE false",
            "SELECT commit_id FROM lix_restore($1) LIMIT 0",
            "INSERT INTO lix_restore(commit_id) VALUES ($1)",
            "INSERT INTO lix_revert(row_ref) VALUES (lix_row_ref('lix_key_value','a'))",
            "INSERT INTO lix_apply(row_ref) VALUES (lix_row_ref('lix_key_value','a'))",
        ] {
            let previous = head(&session).await;
            session
                .execute(sql, &[Value::Text(before.clone())])
                .await
                .expect_err(sql);
            assert_eq!(
                head(&session).await,
                previous,
                "failed command must not mutate: {sql}"
            );
        }
    }
);

simulation_test!(
    recovery_functions_revert_checkpoint_uses_its_first_parent,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let source = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        source
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('base','keep-base')",
                &[],
            )
            .await
            .unwrap();
        let base = head(&source).await;
        let branch_id = "01930000-0000-7000-8000-000000000060";
        source
            .create_branch(lix::CreateBranchOptions {
                id: Some(branch_id.to_owned()),
                name: "Checkpoint revert target".to_owned(),
                from_commit_id: Some(base.clone()),
            })
            .await
            .unwrap();
        let session = sim.wrap_session(engine.open_session_at(branch_id).await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('checkpointed','before')",
                &[],
            )
            .await
            .unwrap();
        let checkpoint = commit(
            &session,
            "SELECT commit_id FROM lix_create_checkpoint()",
            &[],
        )
        .await;
        session
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('later','keep')",
                &[],
            )
            .await
            .unwrap();
        let before_revert = head(&session).await;

        let reverted = commit(
            &session,
            "SELECT commit_id FROM lix_revert($1)",
            &[Value::Text(checkpoint.clone())],
        )
        .await;
        assert_eq!(
            select_rows(&session, "SELECT key,value FROM lix_key_value WHERE key IN ('base','later','a','b') ORDER BY key",).await,
            vec![
                vec![
                    Value::Text("base".into()),
                    Value::Jsonb(serde_json::json!("keep-base").into()),
                ],
                vec![
                    Value::Text("later".into()),
                    Value::Jsonb(serde_json::json!("keep").into()),
                ],
            ]
        );
        assert_eq!(
            select_rows(
                &session,
                &format!("SELECT parent_commit_id FROM lix_log() WHERE commit_id = '{reverted}'"),
            )
            .await,
            vec![vec![Value::Text(before_revert)]],
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT (SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()) AS id",
            )
            .await,
            vec![vec![Value::Text(checkpoint)]],
        );
    }
);

simulation_test!(
    recovery_functions_full_apply_preserves_later_unrelated_rows,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let source = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        source
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('a','before'),('b','before')",
                &[],
            )
            .await
            .unwrap();
        let before = head(&source).await;
        let target_id = "01930000-0000-7000-8000-000000000061";
        source
            .create_branch(lix::CreateBranchOptions {
                id: Some(target_id.to_owned()),
                name: "Full apply target".to_owned(),
                from_commit_id: Some(before.clone()),
            })
            .await
            .unwrap();
        source
            .execute(
                "UPDATE lix_key_value SET value = 'after' WHERE key IN ('a','b')",
                &[],
            )
            .await
            .unwrap();
        let after = head(&source).await;

        let target = sim.wrap_session(engine.open_session_at(target_id).await.unwrap(), &engine);
        target
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('later','keep')",
                &[],
            )
            .await
            .unwrap();
        commit(
            &target,
            "SELECT commit_id FROM lix_apply($1,$2)",
            &[Value::Text(before), Value::Text(after)],
        )
        .await;
        assert_eq!(
            select_rows(&target, "SELECT key,value FROM lix_key_value WHERE key IN ('base','later','a','b') ORDER BY key",).await,
            vec![
                vec![
                    Value::Text("a".into()),
                    Value::Jsonb(serde_json::json!("after").into()),
                ],
                vec![
                    Value::Text("b".into()),
                    Value::Jsonb(serde_json::json!("after").into()),
                ],
                vec![
                    Value::Text("later".into()),
                    Value::Jsonb(serde_json::json!("keep").into()),
                ],
            ]
        );
    }
);

simulation_test!(
    recovery_functions_restore_saved_row_ref_with_source_only_schema,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let source = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let target_id = "01930000-0000-7000-8000-000000000062";
        source
            .create_branch(lix::CreateBranchOptions {
                id: Some(target_id.to_owned()),
                name: "Source-only schema target".to_owned(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "recovery_source_only",
            "columns": [
                {"name":"id","type":"uuid","nullable":false},
                {"name":"value","type":"text","nullable":false}
            ],
            "primary_key": ["id"]
        });
        source.execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES ($1, false, false)",
            &[Value::Jsonb(schema.into())],
        ).await.unwrap();
        source.execute(
            "INSERT INTO recovery_source_only (id,value) VALUES ('01930000-0000-7000-8000-000000000063','restore me')",
            &[],
        ).await.unwrap();
        let source_commit = head(&source).await;
        let row_ref = select_rows(&source,
            "SELECT lix_row_ref('recovery_source_only', '01930000-0000-7000-8000-000000000063') AS row_ref"
        ).await[0][0].clone();
        let target = sim.wrap_session(engine.open_session_at(target_id).await.unwrap(), &engine);
        commit(
            &target,
            "SELECT commit_id FROM lix_restore($1, ARRAY[$2])",
            &[Value::Text(source_commit), row_ref],
        )
        .await;
        assert_eq!(
            select_rows(&target, "SELECT value FROM recovery_source_only").await,
            vec![vec![Value::Text("restore me".into())]]
        );
    }
);
