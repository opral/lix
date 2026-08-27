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

    let columns = "key, value, lixcol_schema_key, lixcol_file_id, lixcol_metadata, \
                   lixcol_created_at, lixcol_updated_at, lixcol_global, lixcol_change_id, \
                   lixcol_commit_id, lixcol_untracked";
    let live = session
        .execute(
            &format!("SELECT {columns} FROM lix_key_value WHERE key = 'stable'"),
            &[],
        )
        .await
        .expect("live row should load");
    let historical = session
        .execute(
            &format!(
                "SELECT {columns} FROM lix_state_at('lix_key_value', $1) WHERE key = 'stable'"
            ),
            &[Value::Text(commit_id.clone())],
        )
        .await
        .expect("historical row should load");
    assert_eq!(historical.columns(), live.columns());
    assert_eq!(historical.rows(), live.rows());
    let historical_star = session
        .execute(
            "SELECT * FROM lix_state_at('lix_key_value', $1) WHERE key = 'stable'",
            &[Value::Text(commit_id.clone())],
        )
        .await
        .expect("historical wildcard should load");
    assert!(
        historical_star
            .columns()
            .iter()
            .all(|column| column != "lixcol_row_pk"),
        "lix_state_at must not expose the durable JSON row key"
    );
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

    let local_base = local
        .execute(
            "SELECT base_commit_id FROM lix_commit WHERE id = $1",
            &[Value::Text(local_commit_id.clone())],
        )
        .await
        .expect("local base should load");
    assert_rows_eq(
        local_base,
        vec![vec![Value::Text(global_commit_id.clone())]],
    );
    let complete_local_state = local
        .execute(
            "SELECT key, value, lixcol_global \
             FROM lix_state_at('lix_key_value', $1) \
             WHERE key IN ('global-only', 'overridden', 'suppressed') ORDER BY key",
            &[Value::Text(local_commit_id.clone())],
        )
        .await
        .expect("one local commit id should resolve the complete effective state");
    assert_rows_eq(
        complete_local_state,
        vec![
            vec![
                Value::Text("global-only".into()),
                Value::Jsonb(json!("global-only-value").into()),
                Value::Boolean(true),
            ],
            vec![
                Value::Text("overridden".into()),
                Value::Jsonb(json!("local-value").into()),
                Value::Boolean(false),
            ],
        ],
    );
    let global_history = local
        .execute(
            "SELECT key FROM lix_history('lix_key_value', $1) \
             WHERE key = 'global-only' LIMIT 1",
            &[Value::Text(local_commit_id.clone())],
        )
        .await
        .expect("history should follow the commit's state dependency");
    assert_rows_eq(
        global_history,
        vec![vec![Value::Text("global-only".into())]],
    );
    let ancestry = local
        .execute(
            "SELECT commit_id FROM lix_commit_ancestry($1) WHERE commit_id = $2",
            &[
                Value::Text(local_commit_id.clone()),
                Value::Text(global_commit_id.clone()),
            ],
        )
        .await
        .expect("ancestry should remain chronology-only");
    assert!(
        ancestry.rows().is_empty(),
        "base_commit_id is a state dependency, not a merge/ancestry parent"
    );

    let composed = local
        .execute(
            "WITH ranked_local_history AS ( \
                 SELECT lixcol_row_ref, lixcol_file_id, \
                        row_number() OVER ( \
                            PARTITION BY lixcol_row_ref, lixcol_file_id \
                            ORDER BY lixcol_depth, lixcol_observed_commit_id, lixcol_change_id \
                        ) AS rn \
                 FROM lix_history('lix_key_value', $1) \
             ), local_shadow AS ( \
                 SELECT lixcol_row_ref, lixcol_file_id \
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
                 WHERE local_shadow.lixcol_row_ref = global_state.lixcol_row_ref \
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

    global
        .execute(
            "UPDATE lix_key_value SET value = 'global-new-value' \
             WHERE key = 'overridden'",
            &[],
        )
        .await
        .expect("global head should advance");
    global
        .execute(
            "UPDATE lix_key_value SET value = 'global-new-suppressed-value' \
             WHERE key = 'suppressed'",
            &[],
        )
        .await
        .expect("global suppressed row should advance");
    global
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_global) \
             VALUES ('later-global', 'later', true)",
            &[],
        )
        .await
        .expect("a later global row should insert");
    let latest_global = global
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("latest global commit should load");
    let [Value::Text(latest_global_id)] = latest_global.rows()[0].values() else {
        panic!("expected latest global commit id");
    };

    local
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('local-after-global', 'local')",
            &[],
        )
        .await
        .expect("the next local commit should fold in the latest global base");

    let live = local
        .execute(
            "SELECT key, value FROM lix_key_value \
             WHERE key IN ('global-only', 'later-global', 'overridden', 'suppressed') ORDER BY key",
            &[],
        )
        .await
        .expect("live composed view should load");
    let mut latest_expected = expected;
    latest_expected.insert(
        1,
        vec![
            Value::Text("later-global".into()),
            Value::Jsonb(json!("later").into()),
        ],
    );
    assert_rows_eq(live, latest_expected.clone());

    let refreshed_head = local
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("refreshed local head should load");
    let [Value::Text(refreshed_head_id)] = refreshed_head.rows()[0].values() else {
        panic!("expected refreshed local head id");
    };
    assert_ne!(refreshed_head_id, local_commit_id);
    let effective_diff = local
        .execute(
            "SELECT to_key, diff_type, to_lixcol_global \
             FROM lix_diff('lix_key_value', $1, $2) \
             WHERE COALESCE(to_key, from_key) IN \
                 ('global-only', 'later-global', 'overridden', 'suppressed') \
             ORDER BY COALESCE(to_key, from_key)",
            &[
                Value::Text(local_commit_id.clone()),
                Value::Text(refreshed_head_id.clone()),
            ],
        )
        .await
        .expect("effective diff should include only visible base changes");
    assert_rows_eq(
        effective_diff,
        vec![vec![
            Value::Text("later-global".into()),
            Value::Text("added".into()),
            Value::Boolean(true),
        ]],
    );
    let refreshed_base = local
        .execute(
            "SELECT base_commit_id FROM lix_commit WHERE id = $1",
            &[Value::Text(refreshed_head_id.clone())],
        )
        .await
        .expect("refreshed base should load");
    assert_rows_eq(
        refreshed_base,
        vec![vec![Value::Text(latest_global_id.clone())]],
    );
    let refreshed_state = local
        .execute(
            "SELECT key, value FROM lix_state_at('lix_key_value', $1) \
             WHERE key IN ('global-only', 'later-global', 'overridden', 'suppressed') ORDER BY key",
            &[Value::Text(refreshed_head_id.clone())],
        )
        .await
        .expect("refreshed commit should be a complete state handle");
    assert_rows_eq(refreshed_state, latest_expected);
});

simulation_test!(local_overlay_does_not_inherit_a_global_causal_parent, |sim| async move {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(
        engine.open_session().await.expect("main session should open"),
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
            "INSERT INTO lix_key_value (key, value, lixcol_global) \
             VALUES ('branch-base', 'v1', true)",
            &[],
        )
        .await
        .expect("global base row should insert");
    let g1 = global
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("global head should load");
    let [Value::Text(g1)] = g1.rows()[0].values() else {
        panic!("expected global commit id");
    };
    let branch_id = "73716c2d-6272-816e-8368-2d6f76657200";
    main.execute(
        "INSERT INTO lix_branch (id, name, commit_id) VALUES ($1, 'Overlay', $2)",
        &[Value::Text(branch_id.into()), Value::Text(g1.clone())],
    )
    .await
    .expect("branch should start at the global commit");
    let local = sim.wrap_session(
        engine
            .open_session_at(branch_id)
            .await
            .expect("local branch session should open"),
        &engine,
    );
    local
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('local-one', 'one')",
            &[],
        )
        .await
        .expect("first local overlay row should insert");
    global
        .execute(
            "UPDATE lix_key_value SET value = 'v2' WHERE key = 'branch-base'",
            &[],
        )
        .await
        .expect("global row should advance");
    let g2 = global
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("advanced global head should load");
    let [Value::Text(g2)] = g2.rows()[0].values() else {
        panic!("expected global commit id");
    };
    let live_after_global = local
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'branch-base'",
            &[],
        )
        .await
        .expect("live read should lazily refresh the composite handle");
    assert_rows_eq(
        live_after_global,
        vec![vec![Value::Jsonb(json!("v2").into())]],
    );
    let refreshed = local
        .execute(
            "SELECT base_commit_id FROM lix_commit WHERE id = lix_active_branch_commit_id()",
            &[],
        )
        .await
        .expect("refreshed base should load");
    assert_rows_eq(refreshed, vec![vec![Value::Text(g2.clone())]]);
    local
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('local-two', 'two')",
            &[],
        )
        .await
        .expect("second local overlay row should insert");
    let head = local
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("local head should load");
    let [Value::Text(head)] = head.rows()[0].values() else {
        panic!("expected local commit id");
    };
    let historical = local
        .execute(
            "SELECT key, value FROM lix_state_at('lix_key_value', $1) \
             WHERE key IN ('branch-base', 'local-one', 'local-two') ORDER BY key",
            &[Value::Text(head.clone())],
        )
        .await
        .expect("composite state should load");
    assert_rows_eq(
        historical,
        vec![
            vec![Value::Text("branch-base".into()), Value::Jsonb(json!("v2").into())],
            vec![Value::Text("local-one".into()), Value::Jsonb(json!("one").into())],
            vec![Value::Text("local-two".into()), Value::Jsonb(json!("two").into())],
        ],
    );
});

simulation_test!(local_collection_replacement_fences_the_global_base, |sim| async move {
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
             ('retired-global', 'v1', true), ('also-retired', 'v1', true)",
            &[],
        )
        .await
        .expect("global rows should insert");
    local
        .execute("DELETE FROM lix_key_value", &[])
        .await
        .expect("local collection should be replaced with empty state");
    local
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('local-survivor', 'local')",
            &[],
        )
        .await
        .expect("sparse local replacement member should insert");
    let before = local
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("replacement commit should load");
    let [Value::Text(before)] = before.rows()[0].values() else {
        panic!("expected local commit id");
    };
    global
        .execute(
            "UPDATE lix_key_value SET value = 'v2' WHERE key = 'retired-global'",
            &[],
        )
        .await
        .expect("retired global row should advance");
    local
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('local-after-base', 'later')",
            &[],
        )
        .await
        .expect("local head should pin the advanced base");
    let head = local
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("local head should load");
    let [Value::Text(head)] = head.rows()[0].values() else {
        panic!("expected local commit id");
    };
    let state = local
        .execute(
            "SELECT key FROM lix_state_at('lix_key_value', $1) ORDER BY key",
            &[Value::Text(head.clone())],
        )
        .await
        .expect("replacement state should load");
    assert_rows_eq(
        state,
        vec![
            vec![Value::Text("local-after-base".into())],
            vec![Value::Text("local-survivor".into())],
        ],
    );
    let hidden_base_diff = local
        .execute(
            "SELECT COALESCE(to_key, from_key) \
             FROM lix_diff('lix_key_value', $1, $2) \
             WHERE COALESCE(to_key, from_key) = 'retired-global'",
            &[Value::Text(before.clone()), Value::Text(head.clone())],
        )
        .await
        .expect("effective diff should respect the replacement fence");
    assert!(hidden_base_diff.rows().is_empty());
});
