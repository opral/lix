use lix::Value;
use serde_json::json;

use super::select_rows;

simulation_test!(relation_diff_pairs_schema_columns_and_inverts_sides, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );
    let baseline = sim.initial_commit_id().to_string();

    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('note', 'first')",
            &[],
        )
        .await
        .expect("tracked insert should succeed");
    let inserted = engine
        .load_branch_head_commit_id(sim.main_branch_id())
        .await
        .expect("inserted head should load")
        .expect("inserted head should exist")
        .to_string();

    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT lixcol_row_pk, diff_type, from_key, to_key, from_value, to_value, row_count \
                 FROM lix_diff('lix_key_value', '{baseline}', '{inserted}')"
            ),
        )
        .await,
        vec![vec![
            Value::Jsonb(json!(["note"]).into()),
            Value::Text("added".to_string()),
            Value::Null,
            Value::Text("note".to_string()),
            Value::Null,
            Value::Jsonb(json!("first").into()),
            Value::Integer(1),
        ]]
    );

    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT diff_type, from_key, to_key, from_value, to_value \
                 FROM lix_diff('lix_key_value', '{inserted}', '{baseline}')"
            ),
        )
        .await,
        vec![vec![
            Value::Text("removed".to_string()),
            Value::Text("note".to_string()),
            Value::Null,
            Value::Jsonb(json!("first").into()),
            Value::Null,
        ]]
    );

    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT count(*) FROM lix_diff('lix_key_value', '{inserted}', '{inserted}')"
            ),
        )
        .await,
        vec![vec![Value::Integer(0)]]
    );

    assert_eq!(
        select_rows(
            &session,
            "SELECT count(*) FROM lix_diff(\
                 'lix_key_value', lix_root_commit_id(), lix_active_branch_commit_id()\
             ) WHERE lixcol_row_pk = CAST('[\"note\"]' AS JSONB)",
        )
        .await,
        vec![vec![Value::Integer(1)]],
        "commit graph accessors are valid explicit table-function arguments",
    );

    session
        .execute(
            "UPDATE lix_key_value SET value = 'second' WHERE key = 'note'",
            &[],
        )
        .await
        .expect("tracked update should succeed");
    let updated = engine
        .load_branch_head_commit_id(sim.main_branch_id())
        .await
        .expect("updated head should load")
        .expect("updated head should exist")
        .to_string();
    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT diff_type, from_value, to_value, row_count \
                 FROM lix_diff('lix_key_value', '{inserted}', '{updated}') \
                 WHERE lixcol_row_pk = CAST('[\"note\"]' AS JSONB)"
            ),
        )
        .await,
        vec![vec![
            Value::Text("modified".to_string()),
            Value::Jsonb(json!("first").into()),
            Value::Jsonb(json!("second").into()),
            Value::Integer(1),
        ]]
    );
});

simulation_test!(relation_diff_preserves_global_scope_on_both_sides, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );
    let local_before = sim.initial_commit_id().to_string();
    let global_before = engine
        .load_branch_head_commit_id(lix::GLOBAL_BRANCH_ID)
        .await
        .expect("global head should load")
        .expect("global head should exist")
        .to_string();

    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('local-scope', 'value')",
            &[],
        )
        .await
        .expect("branch-local insert should succeed");
    let local_after = engine
        .load_branch_head_commit_id(sim.main_branch_id())
        .await
        .expect("local head should load")
        .expect("local head should exist")
        .to_string();
    session
        .create_checkpoint()
        .await
        .expect("checkpoint should publish a global row");
    let global_after = engine
        .load_branch_head_commit_id(lix::GLOBAL_BRANCH_ID)
        .await
        .expect("updated global head should load")
        .expect("updated global head should exist")
        .to_string();

    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT from_lixcol_global, to_lixcol_global \
                 FROM lix_diff('lix_checkpoint', '{global_before}', '{global_after}')"
            ),
        )
        .await,
        vec![vec![Value::Null, Value::Boolean(true)]],
        "global checkpoint additions preserve the source relation's scope",
    );
    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT from_lixcol_global, to_lixcol_global \
                 FROM lix_diff('lix_checkpoint', '{global_after}', '{global_before}')"
            ),
        )
        .await,
        vec![vec![Value::Boolean(true), Value::Null]],
        "reversing a global checkpoint diff preserves its global before-side",
    );
    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT from_lixcol_global, to_lixcol_global \
                 FROM lix_diff('lix_key_value', '{local_before}', '{local_after}')"
            ),
        )
        .await,
        vec![vec![Value::Null, Value::Boolean(false)]],
        "branch-local relation rows remain local in diff metadata",
    );
});

simulation_test!(working_diff_span_reports_global_provenance, |sim| async move {
    // Regression: the checkpoint-to-head working span with an identity-only
    // provenance projection selected the HOT fast path, which carries no
    // provenance sets — a local edit shadowing an inherited global row
    // reported its global before-side as local.
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );
    session
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_global) \
             VALUES ('shadowed', 'global-value', TRUE)",
            &[],
        )
        .await
        .expect("global insert should succeed");
    session
        .create_checkpoint()
        .await
        .expect("checkpoint should pin the working-diff cursor");
    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('shadowed', 'local-shadow') \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            &[],
        )
        .await
        .expect("active upsert should shadow the inherited global row");

    assert_eq!(
        select_rows(
            &session,
            "SELECT from_lixcol_global, to_lixcol_global \
             FROM lix_diff('lix_key_value', lix_latest_checkpoint_commit_id(), lix_active_branch_commit_id()) \
             WHERE lixcol_row_pk = CAST('[\"shadowed\"]' AS JSONB)",
        )
        .await,
        vec![vec![Value::Boolean(true), Value::Boolean(false)]],
        "the shadowed row's before-side is the inherited global version",
    );
});

simulation_test!(relation_diff_aggregates_files_and_preserves_removed_paths, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );
    let baseline = sim.initial_commit_id().to_string();

    session
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/note.txt', CAST('hello' AS BYTEA))",
            &[],
        )
        .await
        .expect("file insert should succeed");
    let inserted = engine
        .load_branch_head_commit_id(sim.main_branch_id())
        .await
        .expect("inserted head should load")
        .expect("inserted head should exist")
        .to_string();
    let added = select_rows(
        &session,
        &format!(
            "SELECT lixcol_row_pk, diff_type, from_path, to_path, row_count \
             FROM lix_diff('lix_file', '{baseline}', '{inserted}')"
        ),
    )
    .await;
    assert_eq!(added.len(), 1, "descriptor and content form one file diff");
    assert_eq!(added[0][1], Value::Text("added".to_string()));
    assert_eq!(added[0][2], Value::Null);
    assert_eq!(added[0][3], Value::Text("/note.txt".to_string()));
    assert!(matches!(added[0][4], Value::Integer(count) if count >= 2));
    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT diff_type, from_path, to_path, row_count \
                 FROM lix_diff('lix_file', '{inserted}', '{baseline}')"
            ),
        )
        .await,
        vec![vec![
            Value::Text("removed".to_string()),
            Value::Text("/note.txt".to_string()),
            Value::Null,
            added[0][4].clone(),
        ]],
        "reversing a file addition swaps sides and inverts its classification",
    );
    let file_id = match &added[0][0] {
        Value::Jsonb(row_pk) => row_pk.to_value()[0]
            .as_str()
            .expect("file row identity is a UUID")
            .to_string(),
        other => panic!("file row identity should be JSONB, got {other:?}"),
    };
    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT count(*) FROM lix_diff('lix_file', '{baseline}', '{inserted}') \
                 WHERE lixcol_row_pk ->> 0 = '{file_id}'"
            ),
        )
        .await,
        vec![vec![Value::Integer(1)]],
        "JSON row-identity extraction remains a pushed file-scoped filter",
    );
    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT count(*), sum(row_count) \
                 FROM lix_diff('lix_file', '{baseline}', '{inserted}')"
            ),
        )
        .await,
        vec![vec![Value::Integer(1), added[0][4].clone()]],
        "counts operate on aggregated file rows and retain underlying atom counts",
    );

    session
        .execute("UPDATE lix_file SET path = '/renamed.txt' WHERE path = '/note.txt'", &[])
        .await
        .expect("file rename should succeed");
    let renamed = engine
        .load_branch_head_commit_id(sim.main_branch_id())
        .await
        .expect("renamed head should load")
        .expect("renamed head should exist")
        .to_string();
    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT diff_type, from_path, to_path FROM lix_diff('lix_file', '{inserted}', '{renamed}')"
            ),
        )
        .await,
        vec![vec![
            Value::Text("modified".to_string()),
            Value::Text("/note.txt".to_string()),
            Value::Text("/renamed.txt".to_string()),
        ]]
    );

    session
        .execute("DELETE FROM lix_file WHERE path = '/renamed.txt'", &[])
        .await
        .expect("file removal should succeed");
    let removed = engine
        .load_branch_head_commit_id(sim.main_branch_id())
        .await
        .expect("removed head should load")
        .expect("removed head should exist")
        .to_string();
    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT diff_type, from_path, to_path FROM lix_diff('lix_file', '{renamed}', '{removed}')"
            ),
        )
        .await,
        vec![vec![
            Value::Text("removed".to_string()),
            Value::Text("/renamed.txt".to_string()),
            Value::Null,
        ]]
    );
    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT diff_type, from_path, to_path \
                 FROM lix_diff('lix_file', '{removed}', '{renamed}')"
            ),
        )
        .await,
        vec![vec![
            Value::Text("added".to_string()),
            Value::Null,
            Value::Text("/renamed.txt".to_string()),
        ]],
        "reversing a file removal restores its historical path",
    );
});

simulation_test!(relation_diff_tracks_directory_descriptor_add_rename_and_remove, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );
    let baseline = sim.initial_commit_id().to_string();

    session
        .execute("INSERT INTO lix_directory (path) VALUES ('/docs')", &[])
        .await
        .expect("directory insert should succeed");
    let inserted = engine
        .load_branch_head_commit_id(sim.main_branch_id())
        .await
        .expect("inserted head should load")
        .expect("inserted head should exist")
        .to_string();
    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT diff_type, from_path, to_path, row_count \
                 FROM lix_diff('lix_directory', '{baseline}', '{inserted}')"
            ),
        )
        .await,
        vec![vec![
            Value::Text("added".to_string()),
            Value::Null,
            Value::Text("/docs".to_string()),
            Value::Integer(1),
        ]]
    );

    session
        .execute("UPDATE lix_directory SET path = '/archive' WHERE path = '/docs'", &[])
        .await
        .expect("directory rename should succeed");
    let renamed = engine
        .load_branch_head_commit_id(sim.main_branch_id())
        .await
        .expect("renamed head should load")
        .expect("renamed head should exist")
        .to_string();
    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT diff_type, from_path, to_path \
                 FROM lix_diff('lix_directory', '{inserted}', '{renamed}')"
            ),
        )
        .await,
        vec![vec![
            Value::Text("modified".to_string()),
            Value::Text("/docs".to_string()),
            Value::Text("/archive".to_string()),
        ]]
    );

    session
        .execute("DELETE FROM lix_directory WHERE path = '/archive'", &[])
        .await
        .expect("directory deletion should succeed");
    let removed = engine
        .load_branch_head_commit_id(sim.main_branch_id())
        .await
        .expect("removed head should load")
        .expect("removed head should exist")
        .to_string();
    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT diff_type, from_path, to_path \
                 FROM lix_diff('lix_directory', '{renamed}', '{removed}')"
            ),
        )
        .await,
        vec![vec![
            Value::Text("removed".to_string()),
            Value::Text("/archive".to_string()),
            Value::Null,
        ]]
    );
});

simulation_test!(relation_diff_requires_explicit_relation_and_commit_ids, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );

    let missing_relation = session
        .execute("SELECT * FROM lix_diff('a', 'b')", &[])
        .await
        .expect_err("legacy two-argument lix_diff must be rejected");
    assert!(missing_relation.message.contains("relation and exactly two commit ID arguments"));

    let unsupported = session
        .execute(
            "SELECT * FROM lix_diff('not_a_relation', 'a', 'b')",
            &[],
        )
        .await
        .expect_err("unknown relations must be rejected");
    assert!(unsupported.message.contains("does not support relation"));

    let file_content = session
        .execute(
            "SELECT from_content, to_content FROM lix_diff(\
                 'lix_file', lix_root_commit_id(), lix_active_branch_commit_id()\
             )",
            &[],
        )
        .await
        .expect_err("aggregate file bytes are deliberately unsupported");
    assert!(file_content.message.contains("does not support content projection"));
    assert!(file_content.message.contains("lix_history"));
});

simulation_test!(relation_diff_fences_machinery_from_user_column_names, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );

    let reserved = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"reserved_name_collision\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"from_lixcol_user_value\",\"type\":\"text\",\"nullable\":true}],\"primary_key\":[\"id\"]}' AS JSONB), false, false)",
            &[],
        )
        .await
        .expect_err("schema registration must reject the reserved lixcol_ segment");
    assert!(reserved.message.contains("reserved lixcol_ segment"));

    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"diff_name_collision\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"diff_type\",\"type\":\"text\",\"nullable\":false},{\"name\":\"row_count\",\"type\":\"int8\",\"nullable\":false},{\"name\":\"depth\",\"type\":\"int8\",\"nullable\":false},{\"name\":\"from_path\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB), false, false)",
            &[],
        )
        .await
        .expect("collision schema should register");
    let baseline = engine
        .load_branch_head_commit_id(sim.main_branch_id())
        .await
        .expect("baseline head should load")
        .expect("baseline head should exist")
        .to_string();

    session
        .execute(
            "INSERT INTO diff_name_collision (id, diff_type, row_count, depth, from_path) \
             VALUES ('row-1', 'user-kind', 7, 3, '/user-path')",
            &[],
        )
        .await
        .expect("collision row should insert");
    let head = engine
        .load_branch_head_commit_id(sim.main_branch_id())
        .await
        .expect("updated head should load")
        .expect("updated head should exist")
        .to_string();

    assert_eq!(
        select_rows(
            &session,
            &format!(
                "SELECT lixcol_row_pk, diff_type, row_count, \
                 from_diff_type, to_diff_type, from_row_count, to_row_count, \
                 from_depth, to_depth, from_from_path, to_from_path \
                 FROM lix_diff('diff_name_collision', '{baseline}', '{head}')"
            ),
        )
        .await,
        vec![vec![
            Value::Jsonb(json!(["row-1"]).into()),
            Value::Text("added".to_string()),
            Value::Integer(1),
            Value::Null,
            Value::Text("user-kind".to_string()),
            Value::Null,
            Value::Integer(7),
            Value::Null,
            Value::Integer(3),
            Value::Null,
            Value::Text("/user-path".to_string()),
        ]]
    );

    for retired in ["lixcol_diff_type", "lixcol_row_count"] {
        let error = session
            .execute(
                &format!(
                    "SELECT {retired} FROM lix_diff('diff_name_collision', '{baseline}', '{head}')"
                ),
                &[],
            )
            .await
            .expect_err("retired diff machinery alias must not exist");
        assert!(
            error.message.contains(retired),
            "error should identify retired column {retired}: {}",
            error.message
        );
    }
});
