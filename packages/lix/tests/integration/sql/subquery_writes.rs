use lix::Value;

// Companion audit for `subquery_reads`: the write-plan cache (`write_plans`,
// `LogicalWritePlan`) must not repeat the read-plan cache's traversal hole.
//
// The read cache stores a DataFusion `LogicalPlan` and has to detach every
// `TableScan` source before caching. `LogicalWritePlan` stores only lix's own
// bound IR plus sqlparser AST, so there is no provider to detach — but that is
// only safe while write statements cannot smuggle a live plan into the LRU.
// This test pins both halves of that argument:
//
//   * subquery predicates are rejected at bind time, before a template can be
//     cached, and stay rejected on the second (warm-cache) attempt;
//   * `INSERT ... SELECT` — the one write shape that does carry a nested query
//     into the cached template — re-plans its input from the stored AST on
//     every execution and therefore never parks a provider in the cache.
//
// Every statement runs twice so a template written by the first attempt is
// exercised by the second.
simulation_test!(
    subquery_writes_do_not_leak_storage_read_handles,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (index, key) in ["sqw-a", "sqw-b", "sqw-c"].into_iter().enumerate() {
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

        // Subquery-carrying write predicates never reach the write-plan cache.
        let rejected = [
            "UPDATE lix_key_value SET value = 'rewritten' \
         WHERE key IN (SELECT key FROM lix_key_value WHERE key = 'sqw-a')",
            "DELETE FROM lix_key_value \
         WHERE EXISTS (SELECT 1 FROM lix_key_value WHERE key = 'sqw-b')",
            "UPDATE lix_key_value \
         SET value = (SELECT value FROM lix_key_value WHERE key = 'sqw-c') \
         WHERE key = 'sqw-a'",
            "DELETE FROM lix_key_value \
         WHERE key = (SELECT MIN(key) FROM lix_key_value WHERE key LIKE 'sqw-%')",
        ];
        for sql in rejected {
            for attempt in 0..2 {
                let error = session
                    .execute(sql, &[])
                    .await
                    .expect_err("subquery write predicates are not part of the write surface");
                assert_eq!(
                    error.code,
                    lix::LixError::CODE_UNSUPPORTED_SQL,
                    "attempt {attempt} of `{sql}` must be refused by the binder, got {error:?}"
                );
            }
        }

        // The rejected statements must not have mutated anything, and the session
        // must still be usable afterwards.
        let surviving = session
            .execute(
                "SELECT key, value FROM lix_key_value WHERE key LIKE 'sqw-%' ORDER BY key",
                &[],
            )
            .await
            .expect("read after rejected writes should succeed");
        assert_eq!(surviving.len(), 3, "rejected writes must not mutate rows");

        // `INSERT ... SELECT` is the one write shape whose cached template retains
        // a nested query. Run it twice with a subquery in the source so a cached
        // template is exercised on the second attempt.
        for (index, id) in [
            "01920000-0000-7000-8000-0000000009a1",
            "01920000-0000-7000-8000-0000000009a2",
        ]
        .into_iter()
        .enumerate()
        {
            let result = session
                .execute(
                    "INSERT INTO lix_file (id, path) \
                 SELECT $1, $2 WHERE EXISTS (SELECT 1 FROM lix_key_value WHERE key = 'sqw-a')",
                    &[
                        Value::Text(id.to_string()),
                        Value::Text(format!("/subquery-{index}.txt")),
                    ],
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("INSERT ... SELECT attempt {index} failed: {error:?}")
                });
            assert_eq!(
                result.rows_affected(),
                1,
                "INSERT ... SELECT attempt {index} inserted the wrong row count"
            );
        }

        let inserted = session
            .execute(
                "SELECT path FROM lix_file WHERE path LIKE '/subquery-%' ORDER BY path",
                &[],
            )
            .await
            .expect("read of INSERT ... SELECT rows should succeed");
        assert_eq!(
            inserted.len(),
            2,
            "both INSERT ... SELECT attempts must be visible"
        );
    }
);
