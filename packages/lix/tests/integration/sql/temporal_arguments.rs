use super::assert_rows_eq;
use lix::Value;

simulation_test!(
    temporal_scalar_subqueries_resolve_in_the_statement_session,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('scalar-ref', '1'::jsonb)",
                &[],
            )
            .await
            .unwrap();
        let head = session
            .execute("SELECT lix_active_branch_commit_id()", &[])
            .await
            .unwrap();
        let commit = head.rows()[0].values()[0].clone();
        for sql in [
            "SELECT key FROM lix_as_of('lix_key_value', (SELECT $1::text)) WHERE key = 'scalar-ref'",
            "SELECT key FROM lix_as_of('lix_key_value', (SELECT $1)) WHERE key = 'scalar-ref'",
            "SELECT key FROM lix_as_of('lix_key_value', COALESCE((SELECT NULL::text), (SELECT $1::text))) WHERE key = 'scalar-ref'",
            "WITH ref AS (SELECT $1::text AS id) SELECT key FROM lix_as_of('lix_key_value', (SELECT id FROM ref)) WHERE key = 'scalar-ref'",
            "WITH ref AS (SELECT $1::text AS id), snapshot AS (SELECT * FROM lix_as_of('lix_key_value', (SELECT id FROM ref))) SELECT key FROM snapshot WHERE key = 'scalar-ref'",
            "WITH unrelated_ref AS (SELECT 'invalid'::text AS id) SELECT key FROM (WITH ref AS (SELECT $1::text AS id) SELECT * FROM lix_as_of('lix_key_value', (SELECT id FROM ref))) snapshot WHERE key = 'scalar-ref'",
        ] {
            let rows = session
                .execute(sql, std::slice::from_ref(&commit))
                .await
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
            assert_rows_eq(rows, vec![vec![Value::Text("scalar-ref".into())]]);
        }
        let branch_read = session.execute("SELECT key FROM lix_as_of('lix_key_value', (SELECT commit_id FROM lix_branch WHERE name = $1)) WHERE key = 'scalar-ref'", &[Value::Text("main".into())]).await.unwrap();
        assert_rows_eq(branch_read, vec![vec![Value::Text("scalar-ref".into())]]);
        let diff = session.execute("SELECT key, diff_type FROM lix_diff('lix_key_value', (SELECT lix_root_commit_id()), (SELECT $1::text)) WHERE key = 'scalar-ref'", std::slice::from_ref(&commit)).await.unwrap();
        assert_rows_eq(
            diff,
            vec![vec![
                Value::Text("scalar-ref".into()),
                Value::Text("added".into()),
            ]],
        );

        // Repeating identical SQL must resolve a newly advanced head, not reuse a
        // value retained in a pooled session or cached logical plan.
        let sql = "SELECT key FROM lix_as_of('lix_key_value', (SELECT lix_active_branch_commit_id())) WHERE key = 'later-ref'";
        assert!(session.execute(sql, &[]).await.unwrap().rows().is_empty());
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('later-ref', '2'::jsonb)",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(
            session.execute(sql, &[]).await.unwrap(),
            vec![vec![Value::Text("later-ref".into())]],
        );
    }
);

simulation_test!(
    temporal_scalar_subqueries_reject_invalid_endpoints,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let correlated = "SELECT * FROM (SELECT lix_root_commit_id() AS id) outer_ref, lix_as_of('lix_file', (SELECT outer_ref.id))";
        assert!(session.execute(correlated, &[]).await.is_err());
        for expression in [
            "(SELECT NULL::text)",
            "(SELECT 'missing'::text WHERE false)",
            "(SELECT 42)",
            "(SELECT lix_active_branch_commit_id() FROM (VALUES (1), (2)) refs(id))",
            "(SELECT 'a', 'b')",
            "(SELECT outer_ref.id)",
        ] {
            for sql in [
                format!("SELECT * FROM lix_as_of('lix_file', {expression})"),
                format!("SELECT * FROM lix_diff('lix_file', lix_root_commit_id(), {expression})"),
            ] {
                assert!(
                    session.execute(&sql, &[]).await.is_err(),
                    "must reject {sql}"
                );
            }
        }
    }
);
