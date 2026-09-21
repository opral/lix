use lix::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    bigint_predicates_preserve_decimal_literal_spelling,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "bigint_predicate_contract",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "n", "type": "int8", "nullable": false}
            ],
            "primary_key": ["id"]
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .expect("BIGINT predicate schema should register");
        session
            .execute(
                "INSERT INTO bigint_predicate_contract (id, n) VALUES ($1, $2), ($3, $4), ($5, $6), ($7, $8)",
                &[
                    Value::Text("wide".into()),
                    Value::Integer(9_007_199_254_740_993),
                    Value::Text("minimum".into()),
                    Value::Integer(i64::MIN),
                    Value::Text("neighbor".into()),
                    Value::Integer(9_007_199_254_740_992),
                Value::Text("zero".into()),
                Value::Integer(0),
                ],
            )
            .await
            .expect("BIGINT predicate fixture should insert");

        for sql in [
            "SELECT id FROM bigint_predicate_contract WHERE n = 9007199254740993.0",
            "SELECT id FROM bigint_predicate_contract WHERE 9007199254740993.0 = n",
            "SELECT id FROM bigint_predicate_contract WHERE n IN (9007199254740993e0)",
            "SELECT id FROM bigint_predicate_contract WHERE 9007199254740993.0 IN (n)",
            "SELECT id FROM bigint_predicate_contract WHERE n = (9007199254740993.0)",
            "SELECT id FROM bigint_predicate_contract WHERE n = +9007199254740993.0",
            "SELECT id FROM bigint_predicate_contract WHERE n = -(-9007199254740993.0)",
        ] {
            assert_rows_eq(
                session
                    .execute(sql, &[])
                    .await
                    .expect("exact wide BIGINT predicate should match"),
                vec![vec![Value::Text("wide".into())]],
            );
        }
        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM bigint_predicate_contract \
                 WHERE n = -9223372036854775808.0",
                    &[],
                )
                .await
                .expect("exact BIGINT minimum decimal predicate should match"),
            vec![vec![Value::Text("minimum".into())]],
        );
        assert_rows_eq(
        session
            .execute(
                "SELECT id FROM bigint_predicate_contract \
                 WHERE CAST(n AS DOUBLE PRECISION) IS NOT DISTINCT FROM 9007199254740993.0 ORDER BY id",
                &[],
            )
            .await
            .expect("explicit DOUBLE comparison should retain floating semantics"),
        vec![vec![Value::Text("neighbor".into())], vec![Value::Text("wide".into())]],
    );

        for sql in [
            "SELECT id FROM bigint_predicate_contract WHERE n = 9007199254740992.5",
            "SELECT id FROM bigint_predicate_contract WHERE n IN (9007199254740992.5)",
            "SELECT id FROM bigint_predicate_contract WHERE 9007199254740992.5 IN (n)",
            "SELECT id FROM bigint_predicate_contract WHERE n = 9223372036854775808.0",
            "SELECT id FROM bigint_predicate_contract WHERE n = 9223372036854775808",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("inexact or out-of-range BIGINT predicate should fail");
            assert_eq!(error.code, lix::LixError::CODE_TYPE_MISMATCH, "{sql}");
        }

        for sql in [
            "UPDATE bigint_predicate_contract SET n = n \
         WHERE n = 9007199254740993.0 AND id LIKE '%' RETURNING id",
            "UPDATE bigint_predicate_contract SET n = n \
         WHERE 9007199254740993.0 = n AND id LIKE '%' RETURNING id",
            "UPDATE bigint_predicate_contract SET n = n \
         WHERE n IN (9007199254740993e0) AND id LIKE '%' RETURNING id",
            "UPDATE bigint_predicate_contract SET n = n \
         WHERE 9007199254740993.0 IN (n) AND id LIKE '%' RETURNING id",
        ] {
            let result = session
                .execute(sql, &[])
                .await
                .expect("generic LIKE BIGINT predicate should match exactly");
            assert_rows_eq(result, vec![vec![Value::Text("wide".into())]]);
        }

        for sql in [
            "UPDATE bigint_predicate_contract SET n = n \
         WHERE n = 9007199254740992.5 AND id LIKE '%'",
            "UPDATE bigint_predicate_contract SET n = n \
         WHERE n IN (9007199254740992.5) AND id LIKE '%'",
            "UPDATE bigint_predicate_contract SET n = n \
         WHERE 9007199254740992.5 IN (n) AND id LIKE '%'",
            "UPDATE bigint_predicate_contract SET n = n \
         WHERE n = 9223372036854775808.0 AND id LIKE '%'",
            "UPDATE bigint_predicate_contract SET n = n \
         WHERE n = 9223372036854775808 AND id LIKE '%'",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .err()
                .unwrap_or_else(|| panic!("generic LIKE should reject {sql}"));
            assert_eq!(error.code, lix::LixError::CODE_TYPE_MISMATCH, "{sql}");
        }
        // Validation must not depend on an earlier IN match, OR branch, or whether
        // the table contains rows. Bare fractions are still implicit BIGINT values;
        // an explicit cast of a different IN member does not cast this operand.
        for populated in [true, false] {
            if !populated {
                session
                    .execute("DELETE FROM bigint_predicate_contract", &[])
                    .await
                    .unwrap();
            }
            for predicate in [
                "n IN (CAST(0.0 AS DOUBLE PRECISION), 1.5)",
                "id = 'wide' OR n = 1.5",
            ] {
                for sql in [
                    format!("SELECT id FROM bigint_predicate_contract WHERE {predicate}"),
                    format!(
                        "UPDATE bigint_predicate_contract SET n=n WHERE {predicate} RETURNING id"
                    ),
                    format!(
                        "UPDATE bigint_predicate_contract SET n=n WHERE ({predicate}) AND id LIKE '%' RETURNING id"
                    ),
                ] {
                    let error = session.execute(&sql, &[]).await.err().unwrap_or_else(|| {
                        panic!("invalid literal must be rejected: {sql}, populated={populated}")
                    });
                    assert_eq!(error.code, lix::LixError::CODE_TYPE_MISMATCH, "{sql}");
                }
            }
        }
    }
);
