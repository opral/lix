use lix::Value;

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
