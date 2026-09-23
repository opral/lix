use lix::Value;

use super::assert_rows_eq;

simulation_test!(
    registered_row_returning_uses_generated_postimages_for_insert_update_and_upsert,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"returning_task\",\"columns\":[{\"name\":\"id\",\"type\":\"uuid\",\"nullable\":false,\"default_expression\":\"uuidv7()\"},{\"name\":\"title\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB))",
                &[],
            )
            .await
            .expect("returning-task schema registration should succeed");

        let inserted = session
            .execute(
                "INSERT INTO returning_task (title) VALUES ($1) RETURNING id, title",
                &[Value::Text("Created through RETURNING".to_string())],
            )
            .await
            .expect("registered row INSERT RETURNING should succeed");
        assert_eq!(inserted.rows_affected(), 1);
        assert_eq!(inserted.columns(), ["id", "title"]);
        let [Value::Text(id), Value::Text(title)] = inserted.rows()[0].values() else {
            panic!("INSERT RETURNING should expose a generated text id and title")
        };
        assert!(!id.is_empty(), "generated id should not be empty");
        assert_eq!(title, "Created through RETURNING");
        let id = id.clone();

        // `RETURNING *` includes transaction-derived audit fields and uses
        // the staged postimage path. Exercise more than one row so its
        // identity lookup remains correct for bulk writes.
        let wildcard = session
            .execute(
                "INSERT INTO returning_task (title) \
                 VALUES ('Wildcard one'), ('Wildcard two') RETURNING *",
                &[],
            )
            .await
            .expect("multi-row INSERT RETURNING * should succeed");
        assert_eq!(wildcard.rows_affected(), 2);
        assert_eq!(wildcard.rows().len(), 2);
        assert!(
            wildcard
                .columns()
                .iter()
                .any(|column| column == "lixcol_commit_id"),
            "wildcard should include the final audit columns"
        );

        let updated = session
            .execute(
                "UPDATE returning_task SET title = $1 WHERE id = $2 \
                 RETURNING id, title, lixcol_created_at, lixcol_updated_at, \
                 lixcol_change_id, lixcol_commit_id",
                &[
                    Value::Text("Updated through RETURNING".to_string()),
                    Value::Text(id.clone()),
                ],
            )
            .await
            .expect("registered row UPDATE RETURNING should succeed");
        assert_eq!(updated.rows_affected(), 1);
        assert_eq!(
            updated.columns(),
            [
                "id",
                "title",
                "lixcol_created_at",
                "lixcol_updated_at",
                "lixcol_change_id",
                "lixcol_commit_id",
            ]
        );
        let [
            Value::Text(returned_id),
            Value::Text(updated_title),
            Value::Text(created_at),
            Value::Text(updated_at),
            Value::Null,
            Value::Text(commit_id),
        ] = updated.rows()[0].values()
        else {
            panic!("UPDATE RETURNING should expose final audit fields")
        };
        assert_eq!(returned_id, &id);
        assert_eq!(updated_title, "Updated through RETURNING");
        // Addressable tracked writes intentionally hide the staged change ID
        // from transaction-visible state. RETURNING matches SELECT and keeps
        // it NULL until that visibility boundary exposes it.
        for value in [created_at, updated_at, commit_id] {
            assert!(
                !value.is_empty(),
                "returned audit value should not be empty"
            );
        }

        let upserted = session
            .execute(
                "INSERT INTO returning_task (id, title) VALUES ($1, $2) \
                 ON CONFLICT (id) DO UPDATE SET title = excluded.title \
                 RETURNING id, title",
                &[
                    Value::Text(id.clone()),
                    Value::Text("Upserted through RETURNING".to_string()),
                ],
            )
            .await
            .expect("row UPSERT RETURNING should expose its postimage");
        assert_eq!(upserted.rows_affected(), 1);
        assert_rows_eq(
            upserted,
            vec![vec![
                Value::Text(id.clone()),
                Value::Text("Upserted through RETURNING".to_string()),
            ]],
        );

        let no_op = session
            .execute(
                "INSERT INTO returning_task (id, title) VALUES ($1, $2) \
                 ON CONFLICT (id) DO NOTHING RETURNING id, title",
                &[
                    Value::Text(id.clone()),
                    Value::Text("Ignored through RETURNING".to_string()),
                ],
            )
            .await
            .expect("DO NOTHING RETURNING should succeed");
        assert_eq!(no_op.rows_affected(), 0);
        assert_eq!(no_op.columns(), ["id", "title"]);
        assert!(no_op.rows().is_empty());
    }
);

simulation_test!(
    filesystem_and_branch_catalog_surfaces_return_postimages_for_insert_update_and_upsert,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        let inserted_file = session
            .execute(
                "INSERT INTO lix_file (path, content) \
                 VALUES ('/returning-file.txt', CAST('before' AS BYTEA)) \
                 RETURNING id, path, content",
                &[],
            )
            .await
            .expect("file INSERT RETURNING should succeed");
        assert_eq!(inserted_file.rows_affected(), 1);
        let [Value::Text(file_id), Value::Text(path), Value::Blob(data)] =
            inserted_file.rows()[0].values()
        else {
            panic!("file INSERT RETURNING should expose the generated id and bytes")
        };
        assert!(!file_id.is_empty());
        assert_eq!(path, "/returning-file.txt");
        assert_eq!(data.as_ref(), b"before");
        let file_id = file_id.clone();

        let updated_file = session
            .execute(
                "UPDATE lix_file SET content = CAST('after' AS BYTEA) WHERE id = $1 \
                 RETURNING id, path, content",
                &[Value::Text(file_id.clone())],
            )
            .await
            .expect("file UPDATE RETURNING should expose the postimage");
        assert_rows_eq(
            updated_file,
            vec![vec![
                Value::Text(file_id.clone()),
                Value::Text("/returning-file.txt".to_string()),
                Value::Blob(b"after".to_vec().into()),
            ]],
        );

        let upserted_file = session
            .execute(
                "INSERT INTO lix_file (path, content) \
                 VALUES ('/returning-file.txt', CAST('final' AS BYTEA)) \
                 ON CONFLICT (path) DO UPDATE SET content = excluded.content \
                 RETURNING id, content",
                &[],
            )
            .await
            .expect("file UPSERT RETURNING should expose the updated row");
        assert_rows_eq(
            upserted_file,
            vec![vec![
                Value::Text(file_id),
                Value::Blob(b"final".to_vec().into()),
            ]],
        );

        let inserted_directory = session
            .execute(
                "INSERT INTO lix_directory (path) VALUES ('/returning-directory') \
                 RETURNING id, path",
                &[],
            )
            .await
            .expect("directory INSERT RETURNING should succeed");
        let [Value::Text(directory_id), Value::Text(directory_path)] =
            inserted_directory.rows()[0].values()
        else {
            panic!("directory INSERT RETURNING should expose the generated id and path")
        };
        assert!(!directory_id.is_empty());
        assert_eq!(directory_path, "/returning-directory");
        let directory_id = directory_id.clone();

        let updated_directory = session
            .execute(
                "UPDATE lix_directory SET path = '/returning-directory-renamed' \
                 WHERE id = $1 RETURNING id, path",
                &[Value::Text(directory_id.clone())],
            )
            .await
            .expect("directory UPDATE RETURNING should expose the postimage");
        assert_rows_eq(
            updated_directory,
            vec![vec![
                Value::Text(directory_id.clone()),
                Value::Text("/returning-directory-renamed".to_string()),
            ]],
        );

        let upserted_directory = session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ($1, '/returning-directory-upserted') \
                 ON CONFLICT (id) DO UPDATE SET path = excluded.path \
                 RETURNING id, path",
                &[Value::Text(directory_id.clone())],
            )
            .await
            .expect("directory UPSERT RETURNING should expose the updated row");
        assert_rows_eq(
            upserted_directory,
            vec![vec![
                Value::Text(directory_id),
                Value::Text("/returning-directory-upserted".to_string()),
            ]],
        );

        let branch_id = "72657475-726e-896e-872d-6272616e6301";
        let inserted_branch = session
            .execute(
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('72657475-726e-896e-872d-6272616e6301', 'Returning branch') \
                 RETURNING id, name, hidden",
                &[],
            )
            .await
            .expect("branch INSERT RETURNING should succeed");
        assert_rows_eq(
            inserted_branch,
            vec![vec![
                Value::Text(branch_id.to_string()),
                Value::Text("Returning branch".to_string()),
                Value::Boolean(false),
            ]],
        );

        let updated_branch = session
            .execute(
                "UPDATE lix_branch SET name = 'Updated returning branch' \
                 WHERE id = $1 RETURNING id, name",
                &[Value::Text(branch_id.to_string())],
            )
            .await
            .expect("branch UPDATE RETURNING should expose the postimage");
        assert_rows_eq(
            updated_branch,
            vec![vec![
                Value::Text(branch_id.to_string()),
                Value::Text("Updated returning branch".to_string()),
            ]],
        );

        let upserted_branch = session
            .execute(
                "INSERT INTO lix_branch (id, name) \
                 VALUES ($1, 'Upserted returning branch') \
                 ON CONFLICT (id) DO UPDATE SET name = excluded.name \
                 RETURNING id, name, hidden",
                &[Value::Text(branch_id.to_string())],
            )
            .await
            .expect("branch UPSERT RETURNING should expose the updated row");
        assert_rows_eq(
            upserted_branch,
            vec![vec![
                Value::Text(branch_id.to_string()),
                Value::Text("Upserted returning branch".to_string()),
                Value::Boolean(false),
            ]],
        );
    }
);

simulation_test!(
    explicit_transaction_returning_errors_rollback_only_the_failing_statement,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"atomic_returning_task\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"title\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB))",
                &[],
            )
            .await
            .expect("atomic-returning schema registration should succeed");
        session
            .execute(
                "INSERT INTO atomic_returning_task (id, title) VALUES ('task', '42')",
                &[],
            )
            .await
            .expect("atomic-returning row seed should succeed");
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/42', CAST('byte-01' AS BYTEA))",
                &[],
            )
            .await
            .expect("atomic-returning file seed should succeed");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute(
                "INSERT INTO lix_file (path, content) \
                 VALUES ('/successful-before-returning-error.txt', CAST('byte-02' AS BYTEA))",
                &[],
            )
            .await
            .expect("prior transaction write should stage");
        // Keep a copy-on-write schema catalog from an earlier successful
        // statement in this transaction. The rollback below must invalidate
        // only the failed statement's catalog state, then rebuild this schema
        // from the restored journal at commit.
        transaction
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"checkpoint_after_returning_error\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"title\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB))",
                &[],
            )
            .await
            .expect("prior transaction schema registration should stage");

        // `name` is `42` before the update and `not-a-number` afterwards.
        // A pre-stage/preimage RETURNING check would therefore succeed; the
        // real postimage cast must fail and leave the transaction unchanged.
        let error = transaction
            .execute(
                "UPDATE lix_file SET path = '/not-a-number' WHERE path = '/42' \
                 RETURNING CAST(name AS BIGINT) AS x",
                &[],
            )
            .await
            .expect_err("postimage file RETURNING cast should fail");
        assert_eq!(error.code, "LIX_TYPE_MISMATCH");
        assert_rows_eq(
            transaction
                .execute("SELECT path FROM lix_file WHERE path = '/42'", &[])
                .await
                .expect("failed RETURNING should restore the file row"),
            vec![vec![Value::Text("/42".to_string())]],
        );
        assert_rows_eq(
            transaction
                .execute(
                    "SELECT path FROM lix_file \
                     WHERE path = '/successful-before-returning-error.txt'",
                    &[],
                )
                .await
                .expect("failed RETURNING should retain earlier transaction writes"),
            vec![vec![Value::Text(
                "/successful-before-returning-error.txt".to_string(),
            )]],
        );

        // Row audit fields require the direct executor's staged postimage
        // path too. Keep the same cast to prove it is rolled back by the
        // shared statement checkpoint rather than a provider-specific guard.
        let error = transaction
            .execute(
                "UPDATE atomic_returning_task SET title = 'not-a-number' \
                 WHERE id = 'task' \
                 RETURNING CAST(title AS BIGINT) AS x, lixcol_commit_id",
                &[],
            )
            .await
            .expect_err("staged row RETURNING cast should fail");
        assert_eq!(error.code, "LIX_TYPE_MISMATCH");
        assert_rows_eq(
            transaction
                .execute(
                    "SELECT id, title FROM atomic_returning_task WHERE id = 'task'",
                    &[],
                )
                .await
                .expect("failed row RETURNING should restore the postimage"),
            vec![vec![
                Value::Text("task".to_string()),
                Value::Text("42".to_string()),
            ]],
        );

        transaction
            .commit()
            .await
            .expect("transaction should commit its earlier successful write");
        assert_rows_eq(
            session
                .execute("SELECT path FROM lix_file WHERE path = '/42'", &[])
                .await
                .expect("failed file RETURNING must not persist"),
            vec![vec![Value::Text("/42".to_string())]],
        );
        assert_rows_eq(
            session
                .execute(
                    "SELECT id, title FROM atomic_returning_task WHERE id = 'task'",
                    &[],
                )
                .await
                .expect("failed row RETURNING must not persist"),
            vec![vec![
                Value::Text("task".to_string()),
                Value::Text("42".to_string()),
            ]],
        );
        let committed_schema = session
            .execute(
                "INSERT INTO checkpoint_after_returning_error (id, title) \
                 VALUES ('checkpoint', 'retained')",
                &[],
            )
            .await
            .expect("schema staged before failed RETURNING should commit");
        assert_eq!(committed_schema.rows_affected(), 1);
    }
);

simulation_test!(
    failed_explicit_returning_rewinds_deterministic_function_state,
    options = crate::support::simulation_test::engine::SimulationOptions {
        deterministic: false,
    },
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        // Register before deterministic mode is enabled so the transaction
        // below starts at a known, persisted sequence position.
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"deterministic_returning_task\",\"columns\":[{\"name\":\"id\",\"type\":\"uuid\",\"nullable\":false,\"default_expression\":\"uuidv7()\"},{\"name\":\"title\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB))",
                &[],
            )
            .await
            .expect("deterministic-returning schema registration should succeed");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
                 VALUES ('lix_deterministic_mode', CAST('{\"enabled\":true}' AS JSONB), true, true)",
                &[],
            )
            .await
            .expect("deterministic mode should enable");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        let error = transaction
            .execute(
                "INSERT INTO deterministic_returning_task (title) VALUES ('not-a-number') \
                 RETURNING CAST(title AS BIGINT) AS x",
                &[],
            )
            .await
            .expect_err("direct deterministic RETURNING cast should fail");
        assert_eq!(error.code, "LIX_TYPE_MISMATCH");

        // Audit columns use the staged postimage path, while the cast still
        // makes the statement fail. Both paths must restore the same runtime
        // function sequence before the next statement executes.
        let error = transaction
            .execute(
                "INSERT INTO deterministic_returning_task (title) VALUES ('not-a-number') \
                 RETURNING CAST(title AS BIGINT) AS x, lixcol_commit_id",
                &[],
            )
            .await
            .expect_err("staged deterministic RETURNING cast should fail");
        assert_eq!(error.code, "LIX_TYPE_MISMATCH");

        let inserted = transaction
            .execute(
                "INSERT INTO deterministic_returning_task (title) VALUES ('restored') \
                 RETURNING id, title",
                &[],
            )
            .await
            .expect("successful statement should reuse the failed statement sequence");
        assert_rows_eq(
            inserted,
            vec![vec![
                Value::Text("01920000-0000-7000-8000-000000000000".to_string()),
                Value::Text("restored".to_string()),
            ]],
        );

        transaction
            .commit()
            .await
            .expect("transaction should commit the successful statement");
    }
);

simulation_test!(
    returning_old_new_global_and_untracked_rows,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let global = sim.wrap_session(
            engine.open_session_at(lix::GLOBAL_BRANCH_ID).await.unwrap(),
            &engine,
        );
        global.execute("INSERT INTO lix_key_value (key, value, lixcol_global) VALUES ('image-inherited', 'global-before', true)", &[]).await.unwrap();
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let inherited = global.execute("INSERT INTO lix_key_value (key, value, lixcol_global) VALUES ('image-inherited', 'local-after', true) ON CONFLICT(key) DO UPDATE SET value = excluded.value RETURNING OLD.value, NEW.value", &[]).await.unwrap();
        assert_rows_eq(
            inherited,
            vec![vec![
                Value::Jsonb(serde_json::json!("global-before").into()),
                Value::Jsonb(serde_json::json!("local-after").into()),
            ]],
        );
        session.execute("INSERT INTO lix_key_value (key, value, lixcol_untracked) VALUES ('image-untracked', 'before', true)", &[]).await.unwrap();
        let updated = session.execute("UPDATE lix_key_value SET value = 'after' WHERE key = 'image-untracked' RETURNING OLD.value, NEW.value, OLD.lixcol_untracked, NEW.lixcol_untracked", &[]).await.unwrap();
        assert_rows_eq(
            updated,
            vec![vec![
                Value::Jsonb(serde_json::json!("before").into()),
                Value::Jsonb(serde_json::json!("after").into()),
                Value::Boolean(true),
                Value::Boolean(true),
            ]],
        );
        let deleted = session.execute("DELETE FROM lix_key_value WHERE key = 'image-untracked' RETURNING OLD.value, NEW.value", &[]).await.unwrap();
        assert_rows_eq(
            deleted,
            vec![vec![
                Value::Jsonb(serde_json::json!("after").into()),
                Value::Null,
            ]],
        );
    }
);

simulation_test!(returning_old_new_transaction_images, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    session
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/cycle.txt', CAST('A' AS BYTEA))",
            &[],
        )
        .await
        .unwrap();
    let before = session
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .unwrap()
        .rows()[0]
        .values()[0]
        .clone();
    let mut transaction = session.begin_transaction().await.unwrap();
    for (old, new) in [("A", "B"), ("B", "A")] {
        let result = transaction.execute("UPDATE lix_file SET content = $1 WHERE path = '/cycle.txt' RETURNING OLD.content, NEW.content", &[Value::Blob(new.as_bytes().to_vec().into())]).await.unwrap();
        assert_rows_eq(
            result,
            vec![vec![
                Value::Blob(old.as_bytes().to_vec().into()),
                Value::Blob(new.as_bytes().to_vec().into()),
            ]],
        );
    }
    // Failure in an image expression restores this statement and retains both
    // successful statements that preceded it in the transaction.
    let error = transaction.execute("UPDATE lix_file SET path = '/invalid' WHERE path = '/cycle.txt' RETURNING OLD.path, CAST(NEW.name AS BIGINT)", &[]).await.unwrap_err();
    assert_eq!(error.code, "LIX_TYPE_MISMATCH");
    transaction.commit().await.unwrap();
    let after = session
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .unwrap()
        .rows()[0]
        .values()[0]
        .clone();
    let diff = session
        .execute(
            "SELECT id FROM lix_diff('lix_file', $1, $2)",
            &[before, after],
        )
        .await
        .unwrap();
    assert!(
        diff.rows().is_empty(),
        "statement transitions can have an empty net file diff"
    );
    assert_rows_eq(
        session
            .execute(
                "SELECT path, content FROM lix_file WHERE path = '/cycle.txt'",
                &[],
            )
            .await
            .unwrap(),
        vec![vec![
            Value::Text("/cycle.txt".into()),
            Value::Blob(b"A".to_vec().into()),
        ]],
    );
});

simulation_test!(returning_old_new_file_images, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    let inserted = session.execute("INSERT INTO lix_file (path, content) VALUES ('/images.txt', CAST('' AS BYTEA)) RETURNING OLD.content AS before, NEW.content AS after", &[]).await.unwrap();
    assert_rows_eq(
        inserted,
        vec![vec![Value::Null, Value::Blob(vec![].into())]],
    );
    let upsert = session.execute("INSERT INTO lix_file (path, content) VALUES ('/images.txt', CAST('updated' AS BYTEA)), ('/inserted.txt', CAST('new' AS BYTEA)) ON CONFLICT (path) DO UPDATE SET content = excluded.content RETURNING path, OLD.content AS before, NEW.content AS after", &[]).await.unwrap();
    assert_rows_eq(
        upsert,
        vec![
            vec![
                Value::Text("/images.txt".into()),
                Value::Blob(vec![].into()),
                Value::Blob(b"updated".to_vec().into()),
            ],
            vec![
                Value::Text("/inserted.txt".into()),
                Value::Null,
                Value::Blob(b"new".to_vec().into()),
            ],
        ],
    );
    let renamed = session.execute("UPDATE lix_file SET path = '/renamed.txt', content = CAST('final' AS BYTEA) WHERE path = '/images.txt' RETURNING OLD.path AS before_path, NEW.path AS after_path, OLD.content AS before, NEW.content AS after", &[]).await.unwrap();
    assert_rows_eq(
        renamed,
        vec![vec![
            Value::Text("/images.txt".into()),
            Value::Text("/renamed.txt".into()),
            Value::Blob(b"updated".to_vec().into()),
            Value::Blob(b"final".to_vec().into()),
        ]],
    );
    let deleted = session.execute("DELETE FROM lix_file WHERE path = '/renamed.txt' RETURNING OLD.content AS before, NEW.content AS after, path", &[]).await.unwrap();
    assert_rows_eq(
        deleted,
        vec![vec![
            Value::Blob(b"final".to_vec().into()),
            Value::Null,
            Value::Text("/renamed.txt".into()),
        ]],
    );
    let empty = session.execute("DELETE FROM lix_file WHERE path = '/absent.txt' RETURNING OLD.content AS before, NEW.content AS after", &[]).await.unwrap();
    assert!(empty.rows().is_empty());
    assert_eq!(
        empty.column_types(),
        &[lix::ResultColumnType::Blob, lix::ResultColumnType::Blob]
    );
});

simulation_test!(
    returning_old_new_native_rows_and_expressions,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session.execute(r#"INSERT INTO lix_registered_schema (value) VALUES (CAST('{"$schema":"https://lix.dev/schema-v1.json","key":"image_counter","columns":[{"name":"id","type":"text","nullable":false},{"name":"n","type":"int8","nullable":false}],"primary_key":["id"]}' AS JSONB))"#, &[]).await.unwrap();
        let inserted = session.execute("INSERT INTO image_counter (id,n) VALUES ('a',10) RETURNING OLD.n AS before, NEW.n AS after", &[]).await.unwrap();
        assert_rows_eq(inserted, vec![vec![Value::Null, Value::Integer(10)]]);
        let upsert = session.execute("INSERT INTO image_counter (id,n) VALUES ('a',20),('b',30) ON CONFLICT (id) DO UPDATE SET n = excluded.n RETURNING id, OLD.n AS before, NEW.n AS after", &[]).await.unwrap();
        assert_rows_eq(
            upsert,
            vec![
                vec![
                    Value::Text("a".into()),
                    Value::Integer(10),
                    Value::Integer(20),
                ],
                vec![Value::Text("b".into()), Value::Null, Value::Integer(30)],
            ],
        );
        let updated = session.execute("UPDATE image_counter SET n = n+1 WHERE id = 'a' RETURNING OLD.n AS before, NEW.n AS after, OLD.lixcol_change_id AS old_change, NEW.lixcol_change_id AS new_change", &[]).await.unwrap();
        assert_eq!(
            &updated.rows()[0].values()[0..2],
            &[Value::Integer(20), Value::Integer(21)]
        );
        assert_ne!(updated.rows()[0].values()[2], updated.rows()[0].values()[3]);
        let wildcard = session
            .execute(
                "UPDATE image_counter SET n = n WHERE id = 'a' RETURNING OLD.*, NEW.*",
                &[],
            )
            .await
            .unwrap();
        let width = wildcard.columns().len() / 2;
        assert_eq!(wildcard.columns()[..width], wildcard.columns()[width..]);
        assert_eq!(wildcard.rows_affected(), 1);
        let deleted = session.execute("DELETE FROM image_counter WHERE id = 'a' RETURNING OLD.n AS before, NEW.n AS after, n", &[]).await.unwrap();
        assert_rows_eq(
            deleted,
            vec![vec![Value::Integer(21), Value::Null, Value::Integer(21)]],
        );
        assert!(
            session
                .execute("UPDATE image_counter SET n = OLD.n", &[])
                .await
                .is_err(),
            "image qualifiers are scoped to RETURNING"
        );
    }
);

simulation_test!(returning_old_new_directory_and_branch, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    let inserted = session.execute("INSERT INTO lix_directory (path) VALUES ('/old-images') RETURNING OLD.path AS before, NEW.path AS after", &[]).await.unwrap();
    assert_rows_eq(
        inserted,
        vec![vec![Value::Null, Value::Text("/old-images".into())]],
    );
    let updated = session.execute("UPDATE lix_directory SET path = '/new-images' WHERE path = '/old-images' RETURNING OLD.path AS before, NEW.path AS after", &[]).await.unwrap();
    assert_rows_eq(
        updated,
        vec![vec![
            Value::Text("/old-images".into()),
            Value::Text("/new-images".into()),
        ]],
    );
    let branch = session.execute("INSERT INTO lix_branch (id, name) VALUES ('72657475-726e-896e-872d-6272616e6302', 'old-images') RETURNING NEW.id AS id, OLD.name AS before, NEW.name AS after", &[]).await.unwrap();
    assert_eq!(
        &branch.rows()[0].values()[1..],
        &[Value::Null, Value::Text("old-images".into())]
    );
    let id = branch.rows()[0].values()[0].clone();
    let renamed = session.execute("UPDATE lix_branch SET name = 'new-images' WHERE id = $1 RETURNING OLD.name AS before, NEW.name AS after", &[id.clone()]).await.unwrap();
    assert_rows_eq(
        renamed,
        vec![vec![
            Value::Text("old-images".into()),
            Value::Text("new-images".into()),
        ]],
    );
    let deleted = session
        .execute(
            "DELETE FROM lix_branch WHERE id = $1 RETURNING OLD.name AS before, NEW.name AS after",
            &[id],
        )
        .await
        .unwrap();
    assert_rows_eq(
        deleted,
        vec![vec![Value::Text("new-images".into()), Value::Null]],
    );
});

simulation_test!(
    returning_delegates_generic_expressions_to_datafusion,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                r#"INSERT INTO lix_registered_schema (value) VALUES (CAST('{"$schema":"https://lix.dev/schema-v1.json","key":"datafusion_returning","columns":[{"name":"id","type":"text","nullable":false},{"name":"n","type":"int8","nullable":false},{"name":"label","type":"text","nullable":false}],"primary_key":["id"]}' AS JSONB))"#,
                &[],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO datafusion_returning (id, n, label) VALUES ('a', 10, 'not-an-int')",
                &[],
            )
            .await
            .unwrap();

        let inserted = session
            .execute(
                "INSERT INTO datafusion_returning (id, n, label) VALUES ('b', 5, 'five') \
                 RETURNING (SELECT old.n FROM (SELECT 7 AS n) old) AS local_old, \
                           (SELECT new.n FROM (SELECT 8 AS n) new) AS local_new, \
                           OLD.n AS before, NEW.n AS after",
                &[],
            )
            .await
            .expect("deferred RETURNING should keep registered-row INSERT support");
        assert_rows_eq(
            inserted,
            vec![vec![
                Value::Integer(7),
                Value::Integer(8),
                Value::Null,
                Value::Integer(5),
            ]],
        );

        let result = session
            .execute(
                "UPDATE datafusion_returning SET n = 11 WHERE id = $1 \
                 RETURNING (SELECT 1) AS scalar, \
                           id IS NOT NULL AS present, -length(id) + $2 AS adjusted_length, \
                           CAST('7' AS INTEGER) AS casted, \
                           OLD.n AS before, NEW.n AS after",
                &[Value::Text("a".into()), Value::Integer(10)],
            )
            .await
            .expect("DataFusion-supported expressions should work in RETURNING");
        assert_eq!(
            result.columns(),
            [
                "scalar",
                "present",
                "adjusted_length",
                "casted",
                "before",
                "after",
            ]
        );
        assert_rows_eq(
            result,
            vec![vec![
                Value::Integer(1),
                Value::Boolean(true),
                Value::Integer(9),
                Value::Integer(7),
                Value::Integer(10),
                Value::Integer(11),
            ]],
        );

        session
            .execute(
                "UPDATE datafusion_returning SET n = 12 WHERE id = 'a' \
                 RETURNING CAST(label AS INTEGER)",
                &[],
            )
            .await
            .expect_err("a failing DataFusion RETURNING projection should fail the statement");
        let after_error = session
            .execute("SELECT n FROM datafusion_returning WHERE id = 'a'", &[])
            .await
            .unwrap();
        assert_rows_eq(after_error, vec![vec![Value::Integer(11)]]);
    }
);

simulation_test!(
    returning_unnamed_expression_names_match_select,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/returning-names.txt', CAST('x' AS BYTEA))",
                &[],
            )
            .await
            .unwrap();

        let selected = session
            .execute("SELECT 1 + 2, lower('A'), 1, 'x' AS named", &[])
            .await
            .unwrap();
        let returned = session
            .execute(
                "UPDATE lix_file SET path = '/returning-names.txt' WHERE path = '/returning-names.txt' \
                 RETURNING 1 + 2, lower('A'), 1, 'x' AS named",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(selected.columns(), returned.columns());

        let selected_old = session
            .execute(
                "SELECT lower(old.path) FROM (SELECT path FROM lix_file WHERE path = '/returning-names.txt') old",
                &[],
            )
            .await
            .unwrap();
        let returned_old = session
            .execute(
                "UPDATE lix_file SET path = '/returning-names.txt' WHERE path = '/returning-names.txt' RETURNING lower(OLD.path)",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(selected_old.columns(), returned_old.columns());
    }
);

simulation_test!(
    writes_register_selected_read_only_relation_providers_from_the_read_snapshot,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let path = "/returning-read-only-dependency.txt";

        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[Value::Text(path.into()), Value::Blob(b"before".to_vec().into())],
            )
            .await
            .unwrap();

        let expected_changes = session
            .execute("SELECT COUNT(*) AS n FROM lix_change", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<i64>("n")
            .unwrap();
        let expected_commits = session
            .execute("SELECT COUNT(*) AS n FROM lix_commit", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<i64>("n")
            .unwrap();

        let updated = session
            .execute(
                "UPDATE lix_file SET path = path WHERE path = $1 \
                 RETURNING (SELECT COUNT(*) FROM lix_change) AS prior_changes, \
                           (SELECT COUNT(*) FROM lix_commit) AS prior_commits",
                &[Value::Text(path.into())],
            )
            .await
            .expect("RETURNING subqueries should read selected read-only relations");
        assert_rows_eq(
            updated,
            vec![vec![
                Value::Integer(expected_changes),
                Value::Integer(expected_commits),
            ]],
        );

        let inserted = session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 SELECT 'read-only-source', 'from change' FROM lix_change LIMIT 1 \
                 RETURNING key",
                &[],
            )
            .await
            .expect("INSERT source should read a selected read-only relation");
        assert_rows_eq(inserted, vec![vec![Value::Text("read-only-source".into())]]);
    }
);

simulation_test!(
    returning_subqueries_respect_nested_local_columns,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let path = "/returning-local-content.txt";
        let original_content = Value::Blob(b"stored file content".to_vec().into());

        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[Value::Text(path.into()), original_content.clone()],
            )
            .await
            .unwrap();

        let returned = session
            .execute(
                "UPDATE lix_file SET path = path WHERE path = $1 \
                 RETURNING (SELECT content FROM (SELECT 'local' AS content) nested) AS local_content, path",
                &[Value::Text(path.into())],
            )
            .await
            .unwrap();
        assert_rows_eq(
            returned,
            vec![vec![Value::Text("local".into()), Value::Text(path.into())]],
        );

        let stored = session
            .execute("SELECT content FROM lix_file WHERE path = $1", &[Value::Text(path.into())])
            .await
            .unwrap();
        assert_rows_eq(stored, vec![vec![original_content]]);
    }
);
