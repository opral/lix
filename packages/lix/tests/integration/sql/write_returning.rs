use lix::Value;

use super::assert_rows_eq;

simulation_test!(
    registered_entity_returning_uses_generated_postimages_for_insert_update_and_upsert,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
                 lix_json('{\"x-lix-key\":\"returning_task\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\",\"x-lix-default\":\"lix_uuid_v7()\"},\"title\":{\"type\":\"string\"}},\"required\":[\"id\",\"title\"],\"additionalProperties\":false}'))",
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
            .expect("registered entity INSERT RETURNING should succeed");
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
            .expect("multi-row entity INSERT RETURNING * should succeed");
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
            .expect("registered entity UPDATE RETURNING should succeed");
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
            .expect("entity UPSERT RETURNING should expose its postimage");
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

        let by_branch = session
            .execute(
                "INSERT INTO returning_task_by_branch (id, title, lixcol_branch_id) \
                 VALUES ('returning-by-branch', 'By branch', $1) \
                 RETURNING id, title, lixcol_branch_id",
                &[Value::Text(sim.main_branch_id().to_string())],
            )
            .await
            .expect("by-branch entity INSERT RETURNING should succeed");
        assert_rows_eq(
            by_branch,
            vec![vec![
                Value::Text("returning-by-branch".to_string()),
                Value::Text("By branch".to_string()),
                Value::Text(sim.main_branch_id().to_string()),
            ]],
        );
    }
);

simulation_test!(
    filesystem_and_branch_surfaces_return_postimages_for_insert_update_and_upsert,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("session should open"),
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

        let explicit_branch_id = sim.main_branch_id().to_string();
        let inserted_file_by_branch = session
            .execute(
                "INSERT INTO lix_file_by_branch (path, content, lixcol_branch_id) \
                 VALUES ('/returning-by-branch-file.txt', CAST('byte-01' AS BYTEA), $1) \
                 RETURNING id, path, lixcol_branch_id",
                &[Value::Text(explicit_branch_id.clone())],
            )
            .await
            .expect("by-branch file INSERT RETURNING should succeed");
        let [
            Value::Text(file_by_branch_id),
            Value::Text(file_by_branch_path),
            Value::Text(returned_branch_id),
        ] = inserted_file_by_branch.rows()[0].values()
        else {
            panic!("by-branch file INSERT RETURNING should expose its postimage")
        };
        assert_eq!(file_by_branch_path, "/returning-by-branch-file.txt");
        assert_eq!(returned_branch_id, &explicit_branch_id);
        let file_by_branch_id = file_by_branch_id.clone();

        let upserted_file_by_branch = session
            .execute(
                "INSERT INTO lix_file_by_branch (path, content, lixcol_branch_id) \
                 VALUES ('/returning-by-branch-file.txt', $2, $1) \
                 ON CONFLICT (path, lixcol_branch_id) DO UPDATE SET content = excluded.content \
                 RETURNING id, content, lixcol_branch_id",
                &[
                    Value::Text(explicit_branch_id.clone()),
                    Value::Blob(vec![2].into()),
                ],
            )
            .await
            .expect("by-branch file UPSERT RETURNING should expose its postimage");
        assert_rows_eq(
            upserted_file_by_branch,
            vec![vec![
                Value::Text(file_by_branch_id),
                Value::Blob(vec![2].into()),
                Value::Text(explicit_branch_id.clone()),
            ]],
        );

        let global_file_id = "72657475-726e-896e-872d-676c6f62616c";
        session
            .execute(
                "INSERT INTO lix_file_by_branch \
                 (id, path, content, lixcol_global, lixcol_branch_id) \
                 VALUES ($1, '/returning-global-file.txt', CAST('byte-01' AS BYTEA), true, \
                         'ffffffff-ffff-7fff-bfff-ffffffffffff')",
                &[Value::Text(global_file_id.to_string())],
            )
            .await
            .expect("global file seed should succeed");

        // A global file is rendered in an explicit branch with that consumer
        // branch's id. RETURNING must preserve that readback identity rather
        // than re-validate the rendered row as a new global write.
        let updated_global_projection = session
            .execute(
                "UPDATE lix_file_by_branch SET content = CAST('byte-03' AS BYTEA) \
                 WHERE id = $1 AND lixcol_branch_id = $2 \
                 RETURNING id, path, lixcol_branch_id, lixcol_global",
                &[
                    Value::Text(global_file_id.to_string()),
                    Value::Text(explicit_branch_id.clone()),
                ],
            )
            .await
            .expect("global file projection UPDATE RETURNING should succeed");
        assert_rows_eq(
            updated_global_projection,
            vec![vec![
                Value::Text(global_file_id.to_string()),
                Value::Text("/returning-global-file.txt".to_string()),
                Value::Text(explicit_branch_id.clone()),
                Value::Boolean(true),
            ]],
        );

        let inserted_directory_by_branch = session
            .execute(
                "INSERT INTO lix_directory_by_branch (path, lixcol_branch_id) \
                 VALUES ('/returning-by-branch-directory', $1) \
                 RETURNING id, path, lixcol_branch_id",
                &[Value::Text(explicit_branch_id.clone())],
            )
            .await
            .expect("by-branch directory INSERT RETURNING should succeed");
        let [
            Value::Text(directory_by_branch_id),
            Value::Text(directory_by_branch_path),
            Value::Text(returned_branch_id),
        ] = inserted_directory_by_branch.rows()[0].values()
        else {
            panic!("by-branch directory INSERT RETURNING should expose its postimage")
        };
        assert_eq!(directory_by_branch_path, "/returning-by-branch-directory");
        assert_eq!(returned_branch_id, &explicit_branch_id);
        let directory_by_branch_id = directory_by_branch_id.clone();

        let upserted_directory_by_branch = session
            .execute(
                "INSERT INTO lix_directory_by_branch (id, path, lixcol_branch_id) \
                 VALUES ($1, '/returning-by-branch-directory-upserted', $2) \
                 ON CONFLICT (id) DO UPDATE SET path = excluded.path \
                 RETURNING id, path, lixcol_branch_id",
                &[
                    Value::Text(directory_by_branch_id.clone()),
                    Value::Text(explicit_branch_id.clone()),
                ],
            )
            .await
            .expect("by-branch directory UPSERT RETURNING should expose its postimage");
        assert_rows_eq(
            upserted_directory_by_branch,
            vec![vec![
                Value::Text(directory_by_branch_id),
                Value::Text("/returning-by-branch-directory-upserted".to_string()),
                Value::Text(explicit_branch_id),
            ]],
        );
    }
);

simulation_test!(
    explicit_transaction_returning_errors_rollback_only_the_failing_statement,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
                 lix_json('{\"x-lix-key\":\"atomic_returning_task\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"title\":{\"type\":\"string\"}},\"required\":[\"id\",\"title\"],\"additionalProperties\":false}'))",
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
            .expect("atomic-returning entity seed should succeed");
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
                 lix_json('{\"x-lix-key\":\"checkpoint_after_returning_error\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"title\":{\"type\":\"string\"}},\"required\":[\"id\",\"title\"],\"additionalProperties\":false}'))",
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

        // Entity audit fields require the direct executor's staged postimage
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
            .expect_err("staged entity RETURNING cast should fail");
        assert_eq!(error.code, "LIX_TYPE_MISMATCH");
        assert_rows_eq(
            transaction
                .execute(
                    "SELECT id, title FROM atomic_returning_task WHERE id = 'task'",
                    &[],
                )
                .await
                .expect("failed entity RETURNING should restore the postimage"),
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
                .expect("failed entity RETURNING must not persist"),
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
            engine
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );

        // Register before deterministic mode is enabled so the transaction
        // below starts at a known, persisted sequence position.
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (\
                 lix_json('{\"x-lix-key\":\"deterministic_returning_task\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\",\"x-lix-default\":\"lix_uuid_v7()\"},\"title\":{\"type\":\"string\"}},\"required\":[\"id\",\"title\"],\"additionalProperties\":false}'))",
                &[],
            )
            .await
            .expect("deterministic-returning schema registration should succeed");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
                 VALUES ('lix_deterministic_mode', lix_json('{\"enabled\":true}'), true, true)",
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
