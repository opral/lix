use super::assert_rows_eq;
use lix::Value;

simulation_test!(
    unsupported_correlated_scalar_projection_surfaces_datafusion_limit,
    |sim| async move {
        let sql = "SELECT (SELECT n) AS correlated FROM (VALUES (10)) AS input(n)";
        let dataframe_context = datafusion::prelude::SessionContext::new();
        let native_error = dataframe_context
            .sql(sql)
            .await
            .expect("vanilla DataFusion should plan the query")
            .collect()
            .await
            .expect_err("DataFusion 55.1 does not execute this correlated scalar shape");
        assert!(
            native_error
                .to_string()
                .contains("Physical plan does not support logical expression ScalarSubquery"),
            "{native_error}"
        );

        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let lix_error = session
            .execute(sql, &[])
            .await
            .expect_err("Lix should surface DataFusion's unsupported SQL shape");
        assert_eq!(lix_error.code, lix::LixError::CODE_DIALECT_UNSUPPORTED);
        assert!(
            lix_error.message.contains("Physical plan does not support logical expression ScalarSubquery"),
            "{lix_error:?}"
        );
    }
);

// Reads whose logical plan carries a nested subquery plan must not park
// snapshot-bound table providers in the engine planning cache.
//
// `LogicalPlan`'s tree traversal only walks plan inputs, so the plans inside
// `Expr::ScalarSubquery`, `Expr::InSubquery` and `Expr::Exists` never reach
// `detach_cached_read_plan`. Caching such a plan left live providers — and
// therefore live storage-read handles — in an engine-lifetime LRU, which made
// the read scope fail with `LIX_STORAGE_ERROR: shared storage read still has
// N active handles` and left a released read reachable from the cache.
//
// Each statement is executed twice so a cache entry written by the first run
// would be exercised by the second.
simulation_test!(
    subquery_reads_do_not_leak_storage_read_handles,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (index, key) in ["sq-a", "sq-b", "sq-c"].into_iter().enumerate() {
            session
                .execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
                    &[
                        Value::Text(key.to_string()),
                        Value::Text(format!("value-{index}")),
                    ],
                )
                .await
                .expect("seed insert should succeed");
        }

        let statements = [
            "SELECT key FROM lix_key_value \
         WHERE key IN (SELECT key FROM lix_key_value WHERE key = 'sq-a')",
            "SELECT key FROM lix_key_value AS outer_kv \
         WHERE EXISTS (SELECT 1 FROM lix_key_value AS inner_kv WHERE inner_kv.key = outer_kv.key) \
           AND key = 'sq-b'",
            "SELECT key FROM lix_key_value \
         WHERE key = (SELECT MIN(key) FROM lix_key_value WHERE key LIKE 'sq-%')",
            "SELECT key FROM lix_key_value \
         WHERE key NOT IN (SELECT key FROM lix_key_value WHERE key <> 'sq-c') \
           AND key LIKE 'sq-%'",
        ];

        for sql in statements {
            for attempt in 0..2 {
                let result = session.execute(sql, &[]).await.unwrap_or_else(|error| {
                    panic!("attempt {attempt} of `{sql}` failed: {error:?}")
                });
                assert_eq!(
                    result.len(),
                    1,
                    "attempt {attempt} of `{sql}` returned the wrong row count"
                );
            }
        }
    }
);

simulation_test!(
    empty_scalar_subqueries_surface_datafusion_error,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

        let error = session
            .execute(
                "SELECT \
                    (SELECT x FROM (VALUES (1)) t(x) WHERE false) AS empty_value, \
                    (SELECT x FROM (VALUES (1)) t(x) WHERE false) + 1 AS empty_expression, \
                    (SELECT x FROM (VALUES (1)) t(x)) AS present_value",
                &[],
            )
            .await
            .expect_err(
                "DataFusion 55.1 fails while collecting an empty scalar subquery because its output is marked non-nullable",
            );
        assert!(error.message.contains("non-nullable"), "{error:?}");
    }
);

simulation_test!(
    set_operation_all_uses_datafusion_native_path,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

        for sql in [
            "(SELECT x FROM (VALUES (1),(1),(1)) t(x)) INTERSECT ALL (SELECT x FROM (VALUES (1)) u(x))",
            "(SELECT x FROM (VALUES (1),(1),(1)) t(x)) EXCEPT ALL (SELECT x FROM (VALUES (1)) u(x))",
            "SELECT x FROM (SELECT y AS x FROM (VALUES (1),(1)) t(y) INTERSECT ALL SELECT z FROM (VALUES (1)) u(z)) nested",
        ] {
            if let Err(error) = session.execute(sql, &[]).await {
                assert!(
                    !error.message.contains("multiplicity semantics are not implemented"),
                    "Lix must surface DataFusion's native result or limitation: {error:?}"
                );
            }
        }

        let distinct = session
            .execute(
                "(SELECT x FROM (VALUES (1),(1),(2)) t(x)) INTERSECT (SELECT x FROM (VALUES (1),(3)) u(x))",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(distinct, vec![vec![Value::Integer(1)]]);
    }
);
