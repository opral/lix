use lix::Value;

use super::assert_rows_eq;

simulation_test!(
    case_returning_supports_searched_and_simple_forms,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

        session
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(
                r#"{"$schema":"https://lix.dev/schema-v1.json","key":"case_returning","columns":[{"name":"id","type":"text","nullable":false},{"name":"value","type":"text","nullable":true}],"primary_key":["id"]}"#.into(),
            )],
        )
        .await
        .expect("CASE fixture schema should register");

        let inserted = session
            .execute(
                "INSERT INTO case_returning (id, value) VALUES ($1, $2) \
             RETURNING CASE WHEN $3 THEN value ELSE NULL END AS selected, \
                       CASE value WHEN $4 THEN $5 ELSE NULL END AS simple, upper(value) AS upper_value",
                &[
                    Value::Text("case-1".into()),
                    Value::Text("before".into()),
                    Value::Boolean(true),
                    Value::Text("before".into()),
                    Value::Text("matched".into()),
                ],
            )
            .await
            .expect("INSERT RETURNING CASE should succeed");
        assert_rows_eq(
            inserted,
            vec![vec![
                Value::Text("before".into()),
                Value::Text("matched".into()),
                Value::Text("BEFORE".into()),
            ]],
        );

        session
            .execute(
                "INSERT INTO case_returning (id, value) VALUES ('case-conflict', 'old')",
                &[],
            )
            .await
            .unwrap();
        let conflicted = session
            .execute(
                "INSERT INTO case_returning (id, value) VALUES ('case-conflict', 'new') \
             ON CONFLICT (id) DO UPDATE SET value = excluded.value \
             RETURNING CASE WHEN old.value = 'old' THEN new.value ELSE 'wrong' END",
                &[],
            )
            .await
            .expect("UPSERT RETURNING CASE preserves both row images");
        assert_rows_eq(conflicted, vec![vec![Value::Text("new".into())]]);

        session.execute(
            "INSERT INTO case_returning (id, value) VALUES ('invalid-returning', 'not-an-int') RETURNING CASE WHEN true THEN CAST(value AS BIGINT) ELSE NULL END",
            &[],
        ).await.expect_err("RETURNING evaluation failure must roll back the insert");
        let rejected = session
            .execute(
                "SELECT id FROM case_returning WHERE id = 'invalid-returning'",
                &[],
            )
            .await
            .unwrap();
        assert!(
            rejected.is_empty(),
            "failed RETURNING must not retain a staged row"
        );

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute(
                "INSERT INTO case_returning (id, value) VALUES ('query-prior', 'retained')",
                &[],
            )
            .await
            .expect("prior transaction write should stage");
        transaction
            .execute(
                "INSERT INTO case_returning (id, value) SELECT 'query-invalid', 'not-an-int' \
                 RETURNING CASE WHEN true THEN CAST(value AS BIGINT) ELSE NULL END",
                &[],
            )
            .await
            .expect_err("query INSERT with invalid CASE RETURNING must fail");
        assert_rows_eq(
            transaction
                .execute(
                    "SELECT id FROM case_returning WHERE id IN ('query-prior', 'query-invalid') ORDER BY id",
                    &[],
                )
                .await
                .expect("failed query INSERT should retain only the prior write"),
            vec![vec![Value::Text("query-prior".into())]],
        );
        transaction
            .commit()
            .await
            .expect("prior transaction write should commit");
        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM case_returning WHERE id IN ('query-prior', 'query-invalid') ORDER BY id",
                    &[],
                )
                .await
                .expect("only the prior write should persist"),
            vec![vec![Value::Text("query-prior".into())]],
        );

        let updated = session
        .execute(
            "UPDATE case_returning SET value = $1 WHERE id = $2 \
             RETURNING CASE WHEN old.value = $3 AND new.value > $4 THEN new.value ELSE old.value END AS transition, \
                       CASE WHEN $5 <> $6 OR new.value >= $7 THEN new.value ELSE NULL END AS disjunction, \
                       CASE WHEN new.value <= $8 THEN new.value ELSE NULL END AS upper_bound, \
                       CASE WHEN new.value < $9 THEN new.value ELSE NULL END AS lower_bound, \
                       CASE WHEN NOT (new.value = $10) THEN new.value ELSE NULL END AS negated, \
                       CASE WHEN $11 THEN CAST('invalid-uuid' AS UUID) ELSE new.value END AS short_circuit, \
                       CASE WHEN NULL THEN new.value ELSE NULL END AS null_when, \
                       CASE WHEN NULL OR TRUE THEN new.value ELSE NULL END AS null_or_true, \
                       CASE WHEN NULL AND TRUE THEN new.value ELSE NULL END AS null_and_true, \
                       CASE WHEN NOT NULL THEN new.value ELSE NULL END AS not_null",
            &[
                Value::Text("after".into()),
                Value::Text("case-1".into()),
                Value::Text("before".into()),
                Value::Text("a".into()),
                Value::Text("before".into()),
                Value::Text("before".into()),
                Value::Text("a".into()),
                Value::Text("after".into()),
                Value::Text("zzzz".into()),
                Value::Text("before".into()),
                Value::Boolean(false),
            ],
        )
        .await
        .expect("UPDATE RETURNING searched CASE should support OLD and NEW");
        assert_rows_eq(
            updated,
            vec![vec![
                Value::Text("after".into()),
                Value::Text("after".into()),
                Value::Text("after".into()),
                Value::Text("after".into()),
                Value::Text("after".into()),
                Value::Text("after".into()),
                Value::Null,
                Value::Text("after".into()),
                Value::Null,
                Value::Null,
            ]],
        );

        let deleted = session
            .execute(
                "DELETE FROM case_returning WHERE id = $1 \
             RETURNING CASE WHEN old.value IS NOT NULL THEN old.value END AS deleted_value",
                &[Value::Text("case-1".into())],
            )
            .await
            .expect("DELETE RETURNING CASE should support OLD and implicit NULL ELSE");
        assert_rows_eq(deleted, vec![vec![Value::Text("after".into())]]);
    }
);
