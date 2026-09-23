use lix::{ResultColumnType, Value};

const DEFAULT_SEEDS: [u64; 6] = [0, 1, 2, 0x51ce_deed, u64::MAX - 1, u64::MAX];

simulation_test!(sql_expression_shapes_preserve_values_and_lix_types, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

    for seed in crate::support::fuzz_seeds(&DEFAULT_SEEDS) {
        // The seed varies literals and grammar choices while keeping every query
        // bounded and reproducible. This intentionally stays within ordinary
        // DataFusion SELECT support plus Lix's two logical SQL value types.
        let a = (seed % 17) as i64;
        let b = ((seed >> 8) % 13) as i64;
        let op = if seed & 1 == 0 { "+" } else { "*" };
        let expected = if op == "+" { a + b } else { a * b };
        let sql = format!(
            "SELECT derived.answer AS aliased FROM \
             (SELECT left_value {op} right_value AS answer FROM \
              (SELECT {a}::BIGINT AS left_value, {b}::BIGINT AS right_value) input) derived"
        );
        let result = session
            .execute(&sql, &[])
            .await
            .unwrap_or_else(|error| panic!("seed {seed:#018x}, {sql}: {error:?}"));
        assert_eq!(result.column_types(), &[ResultColumnType::Integer], "{sql}");
        assert_eq!(result.rows()[0].values(), &[Value::Integer(expected)], "{sql}");

        let cte = session
            .execute(
                "WITH first_value AS (SELECT CAST(NULL AS BIGINT) AS n), \
                 second_value AS (SELECT COALESCE(n, 7) AS n FROM first_value) \
                 SELECT n AS value FROM second_value \
                 UNION ALL SELECT CAST(8 AS BIGINT)",
                &[],
            )
            .await
            .unwrap_or_else(|error| panic!("seed {seed:#018x}, CTE/set query: {error:?}"));
        assert_eq!(cte.column_types(), &[ResultColumnType::Integer]);
        assert_eq!(
            cte.rows().iter().map(|row| row.values().to_vec()).collect::<Vec<_>>(),
            vec![vec![Value::Integer(7)], vec![Value::Integer(8)]]
        );

        // JSONB's logical result type must survive aliases, CTEs, and a derived
        // table. NULL and CAST are included without relying on JSONB set ops.
        let jsonb = session
            .execute(
                "WITH source AS (SELECT COALESCE(NULL::jsonb, '{\"seed\":1}'::jsonb) AS payload) \
                 SELECT nested.payload AS result FROM (SELECT payload FROM source) nested",
                &[],
            )
            .await
            .unwrap_or_else(|error| panic!("seed {seed:#018x}, JSONB query: {error:?}"));
        assert_eq!(jsonb.column_types(), &[ResultColumnType::Jsonb]);
        assert_eq!(
            jsonb.rows()[0].values(),
            &[Value::Jsonb(serde_json::json!({"seed": 1}).into())]
        );

        // ROW_REF may flow through projection/CTE/set operations when both
        // branches have the same opaque type. It must not become text implicitly.
        let row_ref = session
            .execute(
                "WITH refs AS (SELECT lix_row_ref('lix_key_value', NULL, 'typed') AS r) \
                 SELECT r AS value FROM refs UNION ALL \
                 SELECT lix_row_ref('lix_key_value', NULL, 'typed')",
                &[],
            )
            .await
            .unwrap_or_else(|error| panic!("seed {seed:#018x}, ROW_REF query: {error:?}"));
        assert_eq!(row_ref.column_types(), &[ResultColumnType::RowRef]);
        assert_eq!(row_ref.rows().len(), 2);
        assert_eq!(row_ref.rows()[0].values(), row_ref.rows()[1].values());

        let opaque_cast = session
            .execute(
                "SELECT LOWER(lix_row_ref('lix_key_value', NULL, 'typed'))",
                &[],
            )
            .await
            .expect_err("ROW_REF must not use UTF-8 text functions implicitly");
        assert_eq!(opaque_cast.code, lix::LixError::CODE_UNSUPPORTED_SQL);
    }
});
