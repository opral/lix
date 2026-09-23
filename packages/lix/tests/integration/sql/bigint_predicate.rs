use lix::Value;
use serde_json::json;

simulation_test!(
    bigint_predicates_follow_datafusion_numeric_coercion,
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
                "INSERT INTO bigint_predicate_contract (id, n) \
                 VALUES ('wide', 9007199254740993), \
                        ('neighbor', 9007199254740992)",
                &[],
            )
            .await
            .expect("BIGINT predicate fixtures should insert");

        // DataFusion parses decimal/exponent literals as Float64 by default.
        // The literal rounds to 9007199254740992.0, as does the adjacent
        // BIGINT value 9007199254740993 during comparison coercion.
        let float_literals = session
            .execute(
                "SELECT 1.0000000000000001 = 1.0, \
                        1.0000000000000001 IN (1.0), \
                        1.0000000000000001 BETWEEN 1.0 AND 1.0000000000000000",
                &[],
            )
            .await
            .expect("ordinary numeric expressions should use DataFusion coercion");
        assert_eq!(
            float_literals.rows()[0].values(),
            &[
                Value::Boolean(true),
                Value::Boolean(true),
                Value::Boolean(true),
            ]
        );

        let expected_ids = ["neighbor", "wide"];
        for predicate in [
            "n = 9007199254740993.0",
            "9007199254740993.0 = n",
            "n IN (9007199254740993e0)",
            "9007199254740993.0 IN (n)",
            "n = (9007199254740993.0)",
            "n = +9007199254740993.0",
            "n = -(-9007199254740993.0)",
            "n = 9007199254740992.5",
        ] {
            let sql = format!(
                "SELECT id FROM bigint_predicate_contract WHERE {predicate} ORDER BY id"
            );
            let rows = session
                .execute(&sql, &[])
                .await
                .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
            let ids = rows
                .rows()
                .iter()
                .map(|row| match &row.values()[0] {
                    Value::Text(value) => value.as_str(),
                    value => panic!("unexpected id value: {value:?}"),
                })
                .collect::<Vec<_>>();
            assert_eq!(ids, expected_ids, "{sql}");
        }

        let returned = session
            .execute(
                "UPDATE bigint_predicate_contract SET n = n \
                 WHERE n = 9007199254740993.0 AND id LIKE '%' RETURNING id",
                &[],
            )
            .await
            .expect("mutation predicates should use the same comparison coercion");
        assert_eq!(returned.rows().len(), 2);
        let mut returned_ids = returned
            .rows()
            .iter()
            .map(|row| match &row.values()[0] {
                Value::Text(value) => value.clone(),
                value => panic!("unexpected id value: {value:?}"),
            })
            .collect::<Vec<_>>();
        returned_ids.sort();
        assert_eq!(returned_ids, expected_ids);
    }
);
