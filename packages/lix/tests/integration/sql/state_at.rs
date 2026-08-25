use lix::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(state_at_reads_tracked_schema_state_at_a_commit, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );

    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('state-at-key', CAST('{\"v\":1}' AS JSONB))",
            &[],
        )
        .await
        .expect("row should insert");
    let commit = session
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("commit should load");
    let [Value::Text(commit_id)] = commit.rows()[0].values() else {
        panic!("expected commit id");
    };

    session
        .execute(
            "UPDATE lix_key_value SET value = CAST('{\"v\":2}' AS JSONB) WHERE key = 'state-at-key'",
            &[],
        )
        .await
        .expect("row should update");

    crate::sql2::arm_state_at_traversal_probe(commit_id);
    let result = session
        .execute(
            "SELECT key, value, lixcol_untracked FROM lix_state_at('lix_key_value', $1) \
             WHERE key IN ('state-at-key', 'absent')",
            &[Value::Text(commit_id.clone())],
        )
        .await
        .expect("historical state should load");
    assert_rows_eq(
        result,
        vec![vec![
            Value::Text("state-at-key".into()),
            Value::Jsonb(json!({ "v": 1 }).into()),
            Value::Boolean(false),
        ]],
    );
    assert_eq!(
        crate::sql2::take_state_at_traversal_probe(commit_id),
        vec![(2, 1)],
        "schema primary-key IN must resolve only matching identities before exact tree reads"
    );

    let root = session
        .execute(
            "SELECT key FROM lix_state_at('lix_key_value', lix_root_commit_id())",
            &[],
        )
        .await
        .expect("root state should load");
    assert!(root.rows().is_empty());

    let contradiction = session
        .execute(
            "SELECT key FROM lix_state_at('lix_key_value', $1) WHERE key = 'a' AND key = 'b'",
            &[Value::Text(commit_id.clone())],
        )
        .await
        .expect("contradictory point state should load");
    assert!(contradiction.rows().is_empty());
});

simulation_test!(state_at_primary_key_keeps_duplicate_file_scopes, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );
    let first = "01991b1d-6d8b-7000-8000-000000000011";
    let second = "01991b1d-6d8b-7000-8000-000000000012";
    session
        .execute(
            "INSERT INTO lix_file (id, path, content) VALUES \
             ($1, '/state-at-first', CAST('a' AS BYTEA)), \
             ($2, '/state-at-second', CAST('b' AS BYTEA))",
            &[Value::Text(first.into()), Value::Text(second.into())],
        )
        .await
        .expect("files should insert");
    session
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_file_id) VALUES \
             ('shared-pk', 'first', $1)",
            &[Value::Text(first.into())],
        )
        .await
        .expect("first scoped row should insert");
    session
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_file_id) VALUES \
             ('shared-pk', 'second', $1)",
            &[Value::Text(second.into())],
        )
        .await
        .expect("second scoped row should insert");
    let commit = session
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("commit should load");
    let [Value::Text(commit_id)] = commit.rows()[0].values() else {
        panic!("expected commit id");
    };

    crate::sql2::arm_state_at_traversal_probe(commit_id);
    let result = session
        .execute(
            "SELECT lixcol_file_id, value \
             FROM lix_state_at('lix_key_value', $1) \
             WHERE key = 'shared-pk' ORDER BY lixcol_file_id",
            &[Value::Text(commit_id.clone())],
        )
        .await
        .expect("both scoped identities should load");
    assert_rows_eq(
        result,
        vec![
            vec![Value::Text(first.into()), Value::Jsonb(json!("first").into())],
            vec![Value::Text(second.into()), Value::Jsonb(json!("second").into())],
        ],
    );
    assert_eq!(
        crate::sql2::take_state_at_traversal_probe(commit_id),
        vec![(1, 2)],
        "one primary-key prefix must discover both file-scoped identities"
    );
});

simulation_test!(state_at_materializes_historical_file_content_and_path, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );
    let id = "01991b1d-6d8b-7000-8000-000000000001";

    session
        .execute(
            &format!(
                "INSERT INTO lix_file (id, path, content) VALUES ('{id}', '/nested/before.txt', CAST('before' AS BYTEA))"
            ),
            &[],
        )
        .await
        .expect("file should insert");
    let commit = session
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("commit should load");
    let [Value::Text(commit_id)] = commit.rows()[0].values() else {
        panic!("expected commit id");
    };

    session
        .execute(
            &format!("UPDATE lix_file SET path = '/after.txt', content = CAST('after' AS BYTEA) WHERE id = '{id}'"),
            &[],
        )
        .await
        .expect("file should update");

    let result = session
        .execute(
            "SELECT id, path, content FROM lix_state_at('lix_file', $1) WHERE id = $2",
            &[Value::Text(commit_id.clone()), Value::Text(id.into())],
        )
        .await
        .expect("historical file should load");
    assert_rows_eq(
        result,
        vec![vec![
            Value::Text(id.into()),
            Value::Text("/nested/before.txt".into()),
            Value::Blob(b"before".to_vec().into()),
        ]],
    );
});

simulation_test!(state_at_matches_live_columns_and_unchanged_row_metadata, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );

    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('stable', 'value')",
            &[],
        )
        .await
        .expect("stable row should insert");
    let commit = session
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("commit should load");
    let [Value::Text(commit_id)] = commit.rows()[0].values() else {
        panic!("expected commit id");
    };
    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('later', 'unrelated')",
            &[],
        )
        .await
        .expect("unrelated row should insert");

    let live = session
        .execute("SELECT * FROM lix_key_value WHERE key = 'stable'", &[])
        .await
        .expect("live row should load");
    let historical = session
        .execute(
            "SELECT * FROM lix_state_at('lix_key_value', $1) WHERE key = 'stable'",
            &[Value::Text(commit_id.clone())],
        )
        .await
        .expect("historical row should load");
    assert_eq!(historical.columns(), live.columns());
    assert_eq!(historical.rows(), live.rows());
});

simulation_test!(state_at_omits_deleted_and_untracked_rows, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );

    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('deleted', 'before')",
            &[],
        )
        .await
        .expect("tracked row should insert");
    let before_delete = session
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("commit should load");
    let [Value::Text(before_delete_id)] = before_delete.rows()[0].values() else {
        panic!("expected commit id");
    };
    session
        .execute("DELETE FROM lix_key_value WHERE key = 'deleted'", &[])
        .await
        .expect("tracked row should delete");
    session
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
             VALUES ('untracked', 'local', true)",
            &[],
        )
        .await
        .expect("untracked row should insert");

    let before = session
        .execute(
            "SELECT key FROM lix_state_at('lix_key_value', $1) WHERE key = 'deleted'",
            &[Value::Text(before_delete_id.clone())],
        )
        .await
        .expect("pre-deletion row should load");
    assert_eq!(before.rows().len(), 1);

    let head = session
        .execute(
            "SELECT key FROM lix_state_at('lix_key_value', lix_active_branch_commit_id()) \
             WHERE key IN ('deleted', 'untracked')",
            &[],
        )
        .await
        .expect("head state should load");
    assert!(head.rows().is_empty());
});

simulation_test!(state_at_rejects_internal_relations_and_unknown_commits, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );

    let internal = session
        .execute(
            "SELECT * FROM lix_state_at('lix_file_descriptor', lix_active_branch_commit_id())",
            &[],
        )
        .await
        .expect_err("internal descriptor relation must be rejected");
    assert!(internal.message.contains("does not support relation"));

    let unknown = session
        .execute(
            "SELECT * FROM lix_state_at('lix_key_value', '0000000000000000000000000000000000000000000000000000000000000000')",
            &[],
        )
        .await
        .expect_err("unknown commit must fail");
    assert!(
        unknown.message.contains("commit") || unknown.message.contains("root"),
        "unknown commit should produce a commit/root error: {}",
        unknown.message
    );
});

simulation_test!(state_at_branch_scope_is_session_independent, |sim| async move {
    let engine = sim.boot_engine().await;
    let local = sim.wrap_session(
        engine.open_session().await.expect("local session should open"),
        &engine,
    );
    let global = sim.wrap_session(
        engine
            .open_session_at(lix::GLOBAL_BRANCH_ID)
            .await
            .expect("global session should open"),
        &engine,
    );
    let global_before = global
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("global parent commit should load");
    let [Value::Text(global_before_id)] = global_before.rows()[0].values() else {
        panic!("expected global parent commit id");
    };

    global
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_global) \
             VALUES ('composed', 'global-value', true)",
            &[],
        )
        .await
        .expect("global row should insert");
    let global_commit = global
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("global commit should load");
    let [Value::Text(global_commit_id)] = global_commit.rows()[0].values() else {
        panic!("expected global commit id");
    };
    local
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('composed', 'local-value')",
            &[],
        )
        .await
        .expect("local override should insert");
    let local_commit = local
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("local commit should load");
    let [Value::Text(local_commit_id)] = local_commit.rows()[0].values() else {
        panic!("expected local commit id");
    };

    let global_state = local
        .execute(
            "SELECT value, lixcol_global \
             FROM lix_state_at('lix_key_value', $1) \
             WHERE key = 'composed'",
            &[Value::Text(global_commit_id.clone())],
        )
        .await
        .expect("global branch state should load from a local session");
    assert_rows_eq(
        global_state,
        vec![vec![
            Value::Jsonb(json!("global-value").into()),
            Value::Boolean(true),
        ]],
    );
    let global_diff = local
        .execute(
            "SELECT to_lixcol_global \
             FROM lix_diff('lix_key_value', $1, $2) \
             WHERE to_key = 'composed'",
            &[
                Value::Text(global_before_id.clone()),
                Value::Text(global_commit_id.clone()),
            ],
        )
        .await
        .expect("global diff metadata should load");
    assert_rows_eq(global_diff, vec![vec![Value::Boolean(true)]]);

    global
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_global) \
             VALUES ('later-global', 'later', true)",
            &[],
        )
        .await
        .expect("global head should advance");
    let pinned_again = local
        .execute(
            "SELECT lixcol_global FROM lix_state_at('lix_key_value', $1) \
             WHERE key = 'composed'",
            &[Value::Text(global_commit_id.clone())],
        )
        .await
        .expect("pinned global metadata should remain immutable");
    assert_rows_eq(pinned_again, vec![vec![Value::Boolean(true)]]);

    let local_state = global
        .execute(
            "SELECT value, lixcol_global \
             FROM lix_state_at('lix_key_value', $1) \
             WHERE key = 'composed'",
            &[Value::Text(local_commit_id.clone())],
        )
        .await
        .expect("local branch state should load from a global session");
    assert_rows_eq(
        local_state,
        vec![vec![
            Value::Jsonb(json!("local-value").into()),
            Value::Boolean(false),
        ]],
    );

    let live = local
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'composed'",
            &[],
        )
        .await
        .expect("composed live view should load");
    assert_rows_eq(live, vec![vec![Value::Jsonb(json!("local-value").into())]]);
});

simulation_test!(state_at_branch_scopes_compose_with_local_shadow_history, |sim| async move {
    let engine = sim.boot_engine().await;
    let local = sim.wrap_session(
        engine.open_session().await.expect("local session should open"),
        &engine,
    );
    let global = sim.wrap_session(
        engine
            .open_session_at(lix::GLOBAL_BRANCH_ID)
            .await
            .expect("global session should open"),
        &engine,
    );

    global
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_global) VALUES \
             ('global-only', 'global-only-value', true), \
             ('overridden', 'global-old-value', true), \
             ('suppressed', 'global-suppressed-value', true)",
            &[],
        )
        .await
        .expect("global rows should insert");
    let global_commit = global
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("global commit should load");
    let [Value::Text(global_commit_id)] = global_commit.rows()[0].values() else {
        panic!("expected global commit id");
    };

    local
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES \
             ('overridden', 'local-value'), ('suppressed', 'local-before-delete')",
            &[],
        )
        .await
        .expect("local override should insert");
    local
        .execute("DELETE FROM lix_key_value WHERE key = 'suppressed'", &[])
        .await
        .expect("local tombstone should suppress the global row");
    let local_commit = local
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("local commit should load");
    let [Value::Text(local_commit_id)] = local_commit.rows()[0].values() else {
        panic!("expected local commit id");
    };

    let composed = local
        .execute(
            "WITH ranked_local_history AS ( \
                 SELECT lixcol_row_pk, lixcol_file_id, \
                        row_number() OVER ( \
                            PARTITION BY lixcol_row_pk, lixcol_file_id \
                            ORDER BY lixcol_depth, lixcol_observed_commit_id, lixcol_change_id \
                        ) AS rn \
                 FROM lix_history('lix_key_value', $1) \
             ), local_shadow AS ( \
                 SELECT lixcol_row_pk, lixcol_file_id \
                 FROM ranked_local_history WHERE rn = 1 \
             ), local_state AS ( \
                 SELECT * FROM lix_state_at('lix_key_value', $1) \
                 WHERE key IN ('global-only', 'overridden', 'suppressed') \
             ), global_state AS ( \
                 SELECT * FROM lix_state_at('lix_key_value', $2) \
                 WHERE key IN ('global-only', 'overridden', 'suppressed') \
             ) \
             SELECT key, value FROM local_state \
             UNION ALL \
             SELECT global_state.key, global_state.value \
             FROM global_state \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM local_shadow \
                 WHERE local_shadow.lixcol_row_pk = global_state.lixcol_row_pk \
                   AND local_shadow.lixcol_file_id \
                       IS NOT DISTINCT FROM global_state.lixcol_file_id \
             ) \
             ORDER BY key",
            &[
                Value::Text(local_commit_id.clone()),
                Value::Text(global_commit_id.clone()),
            ],
        )
        .await
        .expect("pinned branch states should compose with the local shadow set");
    let expected = vec![
            vec![
                Value::Text("global-only".into()),
                Value::Jsonb(json!("global-only-value").into()),
            ],
            vec![
                Value::Text("overridden".into()),
                Value::Jsonb(json!("local-value").into()),
            ],
        ];
    assert_rows_eq(composed, expected.clone());

    let live = local
        .execute(
            "SELECT key, value FROM lix_key_value \
             WHERE key IN ('global-only', 'overridden', 'suppressed') ORDER BY key",
            &[],
        )
        .await
        .expect("live composed view should load");
    assert_rows_eq(live, expected);
});
