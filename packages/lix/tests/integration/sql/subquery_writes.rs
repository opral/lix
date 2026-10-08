use lix::Value;
use serde_json::json;

use super::assert_rows_eq;
use crate::support::simulation_test::engine::SimSession;

async fn register_inlang_test_tables(session: &SimSession) {
    for schema in [
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "inlang_message",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "bundle_id", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        }),
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "inlang_variant",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "message_id", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        }),
    ] {
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .expect("tracked source and target schemas should register");
    }
}

async fn register_predicate_test_table(session: &SimSession) {
    let schema = json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "sql_predicate_target",
        "columns": [
            { "name": "id", "type": "text", "nullable": false },
            { "name": "score", "type": "int8", "nullable": true },
            { "name": "label", "type": "text", "nullable": true },
        ],
        "primary_key": ["id"],
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES ($1)",
            &[Value::Jsonb(schema.into())],
        )
        .await
        .expect("predicate test schema should register");
}

// This mirrors the batched inlang sync DELETE. Running the same statement with
// different parameters also exercises the warm write-plan path.
simulation_test!(
    parameterized_cross_table_delete_matches_inlang_sync_shape,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register_inlang_test_tables(&session).await;

        session
            .execute(
                "INSERT INTO inlang_message (id, bundle_id) VALUES \
                 ('m-a', 'a'), ('m-b', 'b'), ('m-c', 'c'), ('m-d', 'd'), \
                 ('m-e', 'e'), ('m-f', 'f')",
                &[],
            )
            .await
            .expect("message rows should insert");
        session
            .execute(
                "INSERT INTO inlang_variant (id, message_id) VALUES \
                 ('v-a', 'm-a'), ('v-b', 'm-b'), ('v-c', 'm-c'), \
                 ('v-d', 'm-d'), ('v-e', 'm-e'), ('v-f', 'm-f')",
                &[],
            )
            .await
            .expect("variant rows should insert");

        let sql = "DELETE FROM inlang_variant \
                   WHERE message_id IN (SELECT id FROM inlang_message \
                                        WHERE bundle_id IN ($1, $2))";
        for (first_bundle, second_bundle) in [("a", "b"), ("c", "d")] {
            let deleted = session
                .execute(
                    sql,
                    &[
                        Value::Text(first_bundle.into()),
                        Value::Text(second_bundle.into()),
                    ],
                )
                .await
                .expect("cross-table parameterized DELETE with IN subquery should succeed");
            assert_eq!(deleted.rows_affected(), 2);
        }

        let updated = session
            .execute(
                "UPDATE inlang_variant SET message_id = 'm-e-updated' \
                 WHERE message_id IN (SELECT id FROM inlang_message WHERE bundle_id = $1)",
                &[Value::Text("e".into())],
            )
            .await
            .expect("UPDATE with a cross-table IN subquery should succeed");
        assert_eq!(updated.rows_affected(), 1);

        let correlated = session
            .execute(
                "DELETE FROM inlang_variant AS target \
                 WHERE EXISTS (SELECT 1 FROM inlang_message AS source \
                               WHERE source.id = target.message_id \
                                 AND source.bundle_id = $1)",
                &[Value::Text("f".into())],
            )
            .await
            .expect("correlated EXISTS in DELETE should succeed");
        assert_eq!(correlated.rows_affected(), 1);

        let remaining = session
            .execute("SELECT id FROM inlang_variant ORDER BY id", &[])
            .await
            .expect("remaining variants should be readable");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining.rows()[0].get::<String>("id").unwrap(), "v-e");

        let updated_row = session
            .execute(
                "SELECT message_id FROM inlang_variant WHERE id = 'v-e'",
                &[],
            )
            .await
            .expect("updated variant should be readable");
        assert_eq!(
            updated_row.rows()[0].get::<String>("message_id").unwrap(),
            "m-e-updated"
        );

        let mixed_case_alias = session
            .execute(
                "DELETE FROM inlang_variant AS Target \
                 WHERE Target.id IN (SELECT id FROM inlang_variant \
                                WHERE message_id = 'm-e-updated')",
                &[],
            )
            .await
            .expect("unquoted mixed-case target alias should resolve with SQL folding");
        assert_eq!(mixed_case_alias.rows_affected(), 1);

        // Keep INSERT ... SELECT coverage alongside the new DML-subquery paths.
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('sqw-select', 'present')",
                &[],
            )
            .await
            .expect("INSERT ... SELECT source should exist");
        for (index, id) in [
            "01920000-0000-7000-8000-0000000009a1",
            "01920000-0000-7000-8000-0000000009a2",
        ]
        .into_iter()
        .enumerate()
        {
            let inserted = session
                .execute(
                    "INSERT INTO lix_file (id, path) \
                     SELECT $1, $2 WHERE EXISTS \
                       (SELECT 1 FROM lix_key_value WHERE key = 'sqw-select')",
                    &[
                        Value::Text(id.into()),
                        Value::Text(format!("/subquery-{index}.txt")),
                    ],
                )
                .await
                .expect("INSERT ... SELECT with EXISTS should succeed");
            assert_eq!(inserted.rows_affected(), 1);
        }
    }
);

// Re-run each supported subquery DML shape in one session. A query plan cache
// must not retain a storage read handle from any nested provider; the follow-up
// read after every rollback verifies the transaction scope was released.
simulation_test!(
    subquery_writes_do_not_leak_storage_read_handles,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        for key in ["sqw-a", "sqw-b", "sqw-c"] {
            session
                .execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
                    &[Value::Text(key.into()), Value::Text("present".into())],
                )
                .await
                .expect("subquery source rows should insert");
        }

        let cases = [
            (
                "IN",
                "DELETE FROM lix_key_value WHERE key IN \
                 (SELECT key FROM lix_key_value WHERE key = $1)",
                Some("sqw-a"),
            ),
            (
                "EXISTS",
                "DELETE FROM lix_key_value AS target WHERE EXISTS \
                 (SELECT 1 FROM lix_key_value AS source \
                  WHERE source.key = target.key AND source.key = $1)",
                Some("sqw-b"),
            ),
            (
                "scalar",
                "DELETE FROM lix_key_value \
                 WHERE key = (SELECT MIN(key) FROM lix_key_value WHERE key LIKE 'sqw-%')",
                None,
            ),
        ];
        for (label, sql, parameter) in cases {
            for attempt in 0..2 {
                let mut tx = session
                    .begin_transaction()
                    .await
                    .unwrap_or_else(|error| panic!("{label} attempt {attempt}: {error:?}"));
                let params = parameter
                    .map(|value| vec![Value::Text(value.into())])
                    .unwrap_or_default();
                let deleted = tx
                    .execute(sql, &params)
                    .await
                    .unwrap_or_else(|error| panic!("{label} attempt {attempt} failed: {error:?}"));
                assert_eq!(deleted.rows_affected(), 1, "{label} should select one row");
                tx.rollback()
                    .await
                    .unwrap_or_else(|error| panic!("{label} rollback failed: {error:?}"));

                let remaining = session
                    .execute(
                        "SELECT count(*) AS n FROM lix_key_value WHERE key LIKE 'sqw-%'",
                        &[],
                    )
                    .await
                    .unwrap_or_else(|error| {
                        panic!("read after {label} attempt {attempt} failed: {error:?}")
                    });
                assert_eq!(remaining.rows()[0].get::<i64>("n").unwrap(), 3);
            }
        }
    }
);

simulation_test!(
    delete_subquery_sees_transaction_writes_and_rollback_restores_rows,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register_inlang_test_tables(&session).await;

        session
            .execute(
                "INSERT INTO inlang_message (id, bundle_id) VALUES ('m-kept', 'keep')",
                &[],
            )
            .await
            .expect("baseline message should insert");
        session
            .execute(
                "INSERT INTO inlang_variant (id, message_id) VALUES ('v-kept', 'm-kept')",
                &[],
            )
            .await
            .expect("baseline variant should insert");

        let mut tx = session.begin_transaction().await.unwrap();
        tx.execute(
            "INSERT INTO inlang_message (id, bundle_id) VALUES ('m-tx', 'tx')",
            &[],
        )
        .await
        .expect("transactional message should insert");
        tx.execute(
            "INSERT INTO inlang_variant (id, message_id) VALUES ('v-tx', 'm-tx')",
            &[],
        )
        .await
        .expect("transactional variant should insert");

        let deleted = tx
            .execute(
                "DELETE FROM inlang_variant \
                 WHERE message_id IN (SELECT id FROM inlang_message WHERE bundle_id = $1)",
                &[Value::Text("tx".into())],
            )
            .await
            .expect("subquery should see writes staged earlier in this transaction");
        assert_eq!(deleted.rows_affected(), 1);

        let within_transaction = tx
            .execute("SELECT id FROM inlang_variant ORDER BY id", &[])
            .await
            .expect("transaction should read its deletion");
        assert_eq!(within_transaction.len(), 1);
        assert_eq!(
            within_transaction.rows()[0].get::<String>("id").unwrap(),
            "v-kept"
        );

        tx.rollback().await.expect("transaction should roll back");
        let after_rollback = session
            .execute("SELECT id FROM inlang_variant ORDER BY id", &[])
            .await
            .expect("committed rows should remain readable after rollback");
        assert_eq!(after_rollback.len(), 1);
        assert_eq!(
            after_rollback.rows()[0].get::<String>("id").unwrap(),
            "v-kept"
        );
    }
);

simulation_test!(
    update_and_delete_subquery_fallback_preserves_returning_images,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register_inlang_test_tables(&session).await;
        session
            .execute(
                "INSERT INTO inlang_message (id, bundle_id) VALUES \
                 ('m-update', 'update'), ('m-delete', 'delete')",
                &[],
            )
            .await
            .expect("messages should insert");
        session
            .execute(
                "INSERT INTO inlang_variant (id, message_id) VALUES \
                 ('v-update', 'm-update'), ('v-delete', 'm-delete')",
                &[],
            )
            .await
            .expect("variants should insert");

        let updated = session
            .execute(
                "UPDATE inlang_variant SET message_id = 'm-updated' \
                 WHERE message_id IN (SELECT id FROM inlang_message WHERE bundle_id = 'update') \
                 RETURNING OLD.message_id AS before, NEW.message_id AS after",
                &[],
            )
            .await
            .expect("UPDATE subquery fallback should preserve RETURNING images");
        assert_eq!(updated.rows_affected(), 1);
        assert_rows_eq(
            updated,
            vec![vec![
                Value::Text("m-update".into()),
                Value::Text("m-updated".into()),
            ]],
        );

        let deleted = session
            .execute(
                "DELETE FROM inlang_variant \
                 WHERE message_id IN (SELECT id FROM inlang_message WHERE bundle_id = 'delete') \
                 RETURNING OLD.id AS before, NEW.id AS after",
                &[],
            )
            .await
            .expect("DELETE subquery fallback should preserve RETURNING images");
        assert_eq!(deleted.rows_affected(), 1);
        assert_rows_eq(
            deleted,
            vec![vec![Value::Text("v-delete".into()), Value::Null]],
        );
    }
);

simulation_test!(
    mutation_subquery_predicates_match_common_sql_semantics,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register_predicate_test_table(&session).await;

        session
            .execute(
                "INSERT INTO sql_predicate_target (id, score, label) VALUES \
                 ('row-lt', 1, 'low'), ('row-gt', 6, 'high'), \
                 ('row-ge', 2, 'ge'), ('row-ne', 3, 'ne'), \
                 ('row-between', 4, 'between'), ('row-distinct', NULL, NULL), \
                 ('row-lower', 5, 'ALPHA'), ('row-coalesce', 7, NULL), \
                 ('row-not', 8, 'not')",
                &[],
            )
            .await
            .expect("predicate fixture rows should insert");

        // NOT IN follows PostgreSQL's three-valued logic: one NULL in the
        // nested result makes the predicate unknown for every candidate row.
        let null_not_in = session
            .execute(
                "DELETE FROM sql_predicate_target \
                 WHERE id NOT IN (SELECT label FROM sql_predicate_target \
                                  WHERE id = 'row-distinct')",
                &[],
            )
            .await
            .expect("NOT IN with a NULL subquery result should execute");
        assert_eq!(null_not_in.rows_affected(), 0);

        for (sql, label) in [
            (
                "UPDATE sql_predicate_target SET label = 'checked' WHERE id = 'row-lt' AND score < 2",
                "less-than",
            ),
            (
                "UPDATE sql_predicate_target SET label = 'checked' WHERE id = 'row-gt' AND score > 5",
                "greater-than",
            ),
            (
                "UPDATE sql_predicate_target SET label = 'checked' WHERE id = 'row-ge' AND score >= 2",
                "greater-than-or-equal",
            ),
            (
                "UPDATE sql_predicate_target SET label = 'checked' WHERE id = 'row-ne' AND score <> 2",
                "not-equal",
            ),
            (
                "UPDATE sql_predicate_target SET label = 'checked' WHERE id = 'row-between' AND score BETWEEN 4 AND 4",
                "between",
            ),
            (
                "UPDATE sql_predicate_target SET label = 'checked' WHERE id = 'row-distinct' AND label IS DISTINCT FROM 'x'",
                "is-distinct-from",
            ),
            (
                "UPDATE sql_predicate_target SET label = 'checked' WHERE id = 'row-lower' AND lower(label) = 'alpha'",
                "lower-function",
            ),
            (
                "UPDATE sql_predicate_target SET label = 'checked' WHERE id = 'row-coalesce' AND COALESCE(label, '') = ''",
                "coalesce-function",
            ),
            (
                "UPDATE sql_predicate_target SET label = 'checked' WHERE id = 'row-not' AND NOT (score < 2)",
                "not-expression",
            ),
        ] {
            let updated = session
                .execute(sql, &[])
                .await
                .unwrap_or_else(|error| panic!("{label} mutation predicate failed: {error:?}"));
            assert_eq!(updated.rows_affected(), 1, "{label} should match its row");
        }

        // A scalar subquery is a common predicate shape, and an uncorrelated
        // aggregate should select the unique maximum row.
        let scalar = session
            .execute(
                "DELETE FROM sql_predicate_target \
                 WHERE score = (SELECT MAX(score) FROM sql_predicate_target)",
                &[],
            )
            .await
            .expect("scalar subquery in DELETE should execute");
        assert_eq!(scalar.rows_affected(), 1);

        let not_in = session
            .execute(
                "DELETE FROM sql_predicate_target \
                 WHERE id NOT IN (SELECT id FROM sql_predicate_target WHERE id = 'row-lt')",
                &[],
            )
            .await
            .expect("NOT IN with non-null results should execute");
        assert_eq!(not_in.rows_affected(), 7);
        let remaining = session
            .execute("SELECT id FROM sql_predicate_target", &[])
            .await
            .expect("remaining row should be readable");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining.rows()[0].get::<String>("id").unwrap(), "row-lt");
    }
);

simulation_test!(
    aliased_subquery_delete_preserves_duplicate_file_scoped_identity,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let first_file = "01991b1d-6d8b-7000-8000-0000000000a1";
        let second_file = "01991b1d-6d8b-7000-8000-0000000000a2";
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES \
                 ($1, '/subquery-identity-first', CAST('a' AS BYTEA)), \
                 ($2, '/subquery-identity-second', CAST('b' AS BYTEA))",
                &[
                    Value::Text(first_file.into()),
                    Value::Text(second_file.into()),
                ],
            )
            .await
            .expect("file scopes should insert");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_file_id) VALUES \
                 ('shared-subquery-key', 'first', $1), \
                 ('shared-subquery-key', 'second', $2)",
                &[
                    Value::Text(first_file.into()),
                    Value::Text(second_file.into()),
                ],
            )
            .await
            .expect("duplicate file-scoped primary keys should insert");

        let deleted = session
            .execute(
                "DELETE FROM lix_key_value AS target \
                 WHERE EXISTS (SELECT 1 FROM lix_key_value AS source \
                               WHERE source.key = target.key \
                                 AND source.lixcol_file_id = target.lixcol_file_id \
                                 AND source.lixcol_file_id = $1)",
                &[Value::Text(first_file.into())],
            )
            .await
            .expect("aliased correlated DELETE should execute");
        assert_eq!(deleted.rows_affected(), 1);

        let remaining = session
            .execute(
                "SELECT key, lixcol_file_id FROM lix_key_value \
                 WHERE key = 'shared-subquery-key'",
                &[],
            )
            .await
            .expect("the other file-scoped row should remain");
        assert_eq!(remaining.len(), 1);
        assert_eq!(
            remaining.rows()[0].get::<String>("lixcol_file_id").unwrap(),
            second_file
        );
    }
);

simulation_test!(
    fallback_errors_do_not_mutate_rows_or_poison_the_transaction,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register_inlang_test_tables(&session).await;
        session
            .execute(
                "INSERT INTO inlang_message (id, bundle_id) VALUES \
                 ('m-error-a', 'a'), ('m-error-b', 'b')",
                &[],
            )
            .await
            .expect("messages should insert");
        session
            .execute(
                "INSERT INTO inlang_variant (id, message_id) VALUES \
                 ('v-error-a', 'm-error-a'), ('v-error-b', 'm-error-b')",
                &[],
            )
            .await
            .expect("variants should insert");

        let mut tx = session.begin_transaction().await.unwrap();
        let unknown_column = tx
            .execute(
                "DELETE FROM inlang_variant \
                 WHERE missing_column IN (SELECT id FROM inlang_message)",
                &[],
            )
            .await
            .expect_err("unknown predicate columns should fail before mutation");
        assert!(!unknown_column.message.is_empty());

        let multiple_rows = tx
            .execute(
                "DELETE FROM inlang_variant \
                 WHERE message_id = (SELECT id FROM inlang_message)",
                &[],
            )
            .await
            .expect_err("scalar subquery returning multiple rows should fail");
        assert!(!multiple_rows.message.is_empty());

        let after_errors = tx
            .execute("SELECT id FROM inlang_variant ORDER BY id", &[])
            .await
            .expect("transaction should remain readable after rejected mutations");
        assert_eq!(after_errors.len(), 2);

        let valid = tx
            .execute(
                "DELETE FROM inlang_variant \
                 WHERE message_id IN (SELECT id FROM inlang_message WHERE bundle_id = 'a')",
                &[],
            )
            .await
            .expect("a valid mutation should still work in the same transaction");
        assert_eq!(valid.rows_affected(), 1);
        tx.rollback()
            .await
            .expect("transaction should still roll back");

        let after_rollback = session
            .execute("SELECT id FROM inlang_variant ORDER BY id", &[])
            .await
            .expect("rollback should preserve original rows");
        assert_eq!(after_rollback.len(), 2);
    }
);

simulation_test!(
    file_content_subquery_delete_loads_lazy_content_and_honors_read_only_sources,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let first_file = "01991b1d-6d8b-7000-8000-0000000000b1";
        let second_file = "01991b1d-6d8b-7000-8000-0000000000b2";
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES \
                 ($1, '/subquery-content-a', CAST('a' AS BYTEA)), \
                 ($2, '/subquery-content-b', CAST('b' AS BYTEA))",
                &[
                    Value::Text(first_file.into()),
                    Value::Text(second_file.into()),
                ],
            )
            .await
            .expect("file rows with content should insert");

        let wrong_content = session
            .execute(
                "DELETE FROM lix_file \
                 WHERE content = CAST('b' AS BYTEA) \
                   AND id IN (SELECT id FROM lix_file WHERE path = '/subquery-content-a')",
                &[],
            )
            .await
            .expect("content predicate with same-table subquery should execute");
        assert_eq!(wrong_content.rows_affected(), 0);

        let matching_content = session
            .execute(
                "DELETE FROM lix_file \
                 WHERE content = CAST('a' AS BYTEA) \
                   AND id IN (SELECT id FROM lix_file WHERE path = '/subquery-content-a')",
                &[],
            )
            .await
            .expect("lazy content should be available to the residual predicate");
        assert_eq!(matching_content.rows_affected(), 1);

        // `lix_change` is read-only and exposes file_id as the file descriptor's
        // identity. This exercises a mutation whose relational source provider
        // differs from the target table provider.
        let read_only_source = session
            .execute(
                "DELETE FROM lix_file \
                 WHERE content = CAST('b' AS BYTEA) \
                   AND id IN (SELECT file_id FROM lix_change \
                              WHERE schema_key = 'lix_file_descriptor' AND file_id = $1)",
                &[Value::Text(second_file.into())],
            )
            .await
            .expect("read-only relation should be usable as a DELETE subquery source");
        assert_eq!(read_only_source.rows_affected(), 1);

        let remaining_files = session
            .execute(
                "SELECT id FROM lix_file WHERE id IN ($1, $2)",
                &[
                    Value::Text(first_file.into()),
                    Value::Text(second_file.into()),
                ],
            )
            .await
            .expect("file state should remain readable");
        assert!(remaining_files.is_empty());
    }
);

simulation_test!(
    directory_delete_subquery_respects_the_selected_identity,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES \
                 ('01991b1d-6d8b-7000-8000-0000000000c1', '/subquery-dir-a'), \
                 ('01991b1d-6d8b-7000-8000-0000000000c2', '/subquery-dir-b')",
                &[],
            )
            .await
            .expect("directory rows should insert");

        let deleted = session
            .execute(
                "DELETE FROM lix_directory AS target \
                 WHERE id IN (SELECT id FROM lix_directory WHERE path = '/subquery-dir-a')",
                &[],
            )
            .await
            .expect("directory DELETE with a subquery should execute");
        assert_eq!(deleted.rows_affected(), 1);
        let remaining = session
            .execute(
                "SELECT id, path FROM lix_directory \
                 WHERE id IN (\
                   '01991b1d-6d8b-7000-8000-0000000000c1', \
                   '01991b1d-6d8b-7000-8000-0000000000c2'\
                 ) ORDER BY path",
                &[],
            )
            .await
            .expect("remaining directory should be readable");
        assert_eq!(remaining.len(), 1);
        assert_eq!(
            remaining.rows()[0].get::<String>("id").unwrap(),
            "01991b1d-6d8b-7000-8000-0000000000c2"
        );
        assert_eq!(
            remaining.rows()[0].get::<String>("path").unwrap(),
            "/subquery-dir-b"
        );
    }
);

simulation_test!(
    correlated_update_subquery_uses_old_candidate_rows_and_returns_both_images,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register_predicate_test_table(&session).await;
        session
            .execute(
                "INSERT INTO sql_predicate_target (id, score, label) VALUES \
                 ('row-update-a', 1, 'before-a'), \
                 ('row-update-b', 2, 'before-b'), \
                 ('row-update-c', 3, 'before-c')",
                &[],
            )
            .await
            .expect("correlated update fixture rows should insert");

        let updated = session
            .execute(
                "UPDATE sql_predicate_target AS target \
                 SET score = target.score + 10, label = 'after' \
                 WHERE EXISTS (SELECT 1 FROM sql_predicate_target AS source \
                               WHERE source.id = target.id AND source.score < 3) \
                 RETURNING OLD.id, OLD.score, NEW.score, NEW.label, \
                           target.score + 10 AS target_score_plus_ten",
                &[],
            )
            .await
            .expect("correlated UPDATE with computed assignments and RETURNING should execute");
        assert_eq!(updated.rows_affected(), 2);
        assert_eq!(
            updated.columns(),
            &["id", "score", "score", "label", "target_score_plus_ten"]
        );
        let mut returned_rows = updated
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>();
        returned_rows.sort_by(|left, right| {
            let (Value::Text(left_id), Value::Text(right_id)) = (&left[0], &right[0]) else {
                panic!("RETURNING should contain text ids");
            };
            left_id.cmp(right_id)
        });
        assert_eq!(
            returned_rows,
            vec![
                vec![
                    Value::Text("row-update-a".into()),
                    Value::Integer(1),
                    Value::Integer(11),
                    Value::Text("after".into()),
                    Value::Integer(21),
                ],
                vec![
                    Value::Text("row-update-b".into()),
                    Value::Integer(2),
                    Value::Integer(12),
                    Value::Text("after".into()),
                    Value::Integer(22),
                ],
            ]
        );

        let remaining = session
            .execute(
                "SELECT id, score, label FROM sql_predicate_target WHERE id = 'row-update-c'",
                &[],
            )
            .await
            .expect("nonmatching correlated UPDATE candidate should remain readable");
        assert_rows_eq(
            remaining,
            vec![vec![
                Value::Text("row-update-c".into()),
                Value::Integer(3),
                Value::Text("before-c".into()),
            ]],
        );
    }
);
