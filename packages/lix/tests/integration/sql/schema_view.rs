use lix::ExecuteResult;
use lix::Value;

use super::assert_rows_eq;

simulation_test!(
    registered_schema_upsert_rejects_cross_scope_collision,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let global_session = sim.wrap_session(
            engine
                .open_session_at("ffffffff-ffff-7fff-bfff-ffffffffffff")
                .await
                .unwrap(),
            &engine,
        );
        let schema = "{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"upsert_scope_note\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"body\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}";
        global_session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global) VALUES ($1::jsonb, true)",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("schema registration should succeed");
        global_session
            .execute(
                "INSERT INTO upsert_scope_note (id, body, lixcol_global) VALUES ('one', 'global', true)",
                &[],
            )
            .await
            .expect("global row should insert");
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1::jsonb)",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("branch schema registration should succeed");

        for action in [
            "DO UPDATE SET body = excluded.body",
            "DO NOTHING",
        ] {
            let error = session
                .execute(
                    &format!(
                        "INSERT INTO upsert_scope_note (id, body) VALUES ('one', 'local') \
                         ON CONFLICT (id) {action}"
                    ),
                    &[],
                )
                .await
                .expect_err("cross-scope upsert should fail");
            assert_eq!(error.code, lix::LixError::CODE_CONSTRAINT_VIOLATION);
        }
        session
            .execute(
                "INSERT INTO upsert_scope_note (id, body, lixcol_global) VALUES ('one', 'updated', true) \
                 ON CONFLICT (id) DO UPDATE SET body = excluded.body",
                &[],
            )
            .await
            .expect("same-scope global upsert should succeed");
        assert_rows_eq(
            global_session
                .execute("SELECT body FROM upsert_scope_note WHERE id = 'one'", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("updated".to_string())]],
        );
    }
);

simulation_test!(
    row_filter_pushdown_plan_smoke_for_payload_equality,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_pushdown_note_schema(&session).await;

        let explain = session
            .execute(
                "EXPLAIN VERBOSE SELECT id FROM pushdown_note WHERE kind = 'todo'",
                &[],
            )
            .await
            .expect("EXPLAIN should succeed");
        let plan = explain_plan_text(&explain);

        assert!(
            plan.contains("TableScan: pushdown_note"),
            "plan should scan pushdown_note:\n{plan}"
        );
        assert!(
            plan.contains("partial_filters=[pushdown_note.kind = Utf8(\"todo\")]"),
            "payload equality should reach the table scan while retaining a DataFusion residual:\n{plan}"
        );
    }
);

simulation_test!(
    row_filter_pushdown_keeps_filter_only_payload_available,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_pushdown_note_schema(&session).await;
        insert_pushdown_note(&session, "n1", "todo", "First", "7", "NULL").await;

        let result = session
            .execute("SELECT id FROM pushdown_note WHERE kind = 'todo'", &[])
            .await
            .expect("filter-only payload query should succeed");

        assert_rows_eq(result, vec![vec![Value::Text("n1".to_string())]]);
    }
);

simulation_test!(
    row_filter_pushdown_applies_limit_after_payload_filter,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_pushdown_note_schema(&session).await;
        insert_pushdown_note(&session, "n1", "done", "Already done", "1", "NULL").await;
        insert_pushdown_note(&session, "n2", "todo", "Still todo", "2", "NULL").await;

        let result = session
            .execute(
                "SELECT id FROM pushdown_note WHERE kind = 'todo' ORDER BY id LIMIT 1",
                &[],
            )
            .await
            .expect("filtered LIMIT query should succeed");

        assert_rows_eq(result, vec![vec![Value::Text("n2".to_string())]]);
    }
);

simulation_test!(
    row_filter_pushdown_preserves_sql_null_equality_semantics,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_pushdown_note_schema(&session).await;
        insert_pushdown_note(&session, "n1", "todo", "Nullable", "1", "NULL").await;

        let equals_null = session
            .execute("SELECT id FROM pushdown_note WHERE optional = NULL", &[])
            .await
            .expect("NULL equality query should succeed");
        assert_rows_eq(equals_null, Vec::<Vec<Value>>::new());

        let in_null = session
            .execute("SELECT id FROM pushdown_note WHERE optional IN (NULL)", &[])
            .await
            .expect("NULL IN query should succeed");
        assert_rows_eq(in_null, Vec::<Vec<Value>>::new());
    }
);

simulation_test!(
    row_filter_pushdown_preserves_number_equality_semantics,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_pushdown_note_schema(&session).await;
        insert_pushdown_note(&session, "n1", "todo", "Scored", "7", "NULL").await;

        let result = session
            .execute("SELECT id FROM pushdown_note WHERE score = 7.0", &[])
            .await
            .expect("numeric equality query should succeed");

        assert_rows_eq(result, vec![vec![Value::Text("n1".to_string())]]);
    }
);

simulation_test!(
    row_filter_pushdown_leaves_unsupported_range_as_residual_filter,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_pushdown_note_schema(&session).await;

        let explain = session
            .execute(
                "EXPLAIN VERBOSE SELECT id FROM pushdown_note WHERE score > 5",
                &[],
            )
            .await
            .expect("EXPLAIN should succeed");
        let plan = explain_plan_text(&explain);

        assert!(
            !plan.contains("full_filters=[pushdown_note.score >"),
            "range predicate must not be advertised as exact pushdown:\n{plan}"
        );
        assert!(
            plan.contains("Filter: pushdown_note.score >"),
            "unsupported range predicate should remain as a residual filter:\n{plan}"
        );
    }
);

simulation_test!(
    row_point_read_order_by_pk_elides_physical_sort,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_pushdown_note_schema(&session).await;
        insert_pushdown_note(&session, "n1", "todo", "First", "1", "NULL").await;
        insert_pushdown_note(&session, "n2", "done", "Second", "2", "NULL").await;

        // A fully-applied primary-key equality pins the sort column to one
        // literal, so the ORDER BY over the at-most-one matching row must not
        // build a physical sort operator.
        for point_sql in [
            "SELECT id, title FROM pushdown_note WHERE id = 'n2' ORDER BY id",
            "SELECT id, title FROM pushdown_note WHERE id IN ('n2') ORDER BY id",
        ] {
            let explain = session
                .execute(&format!("EXPLAIN {point_sql}"), &[])
                .await
                .expect("EXPLAIN should succeed");
            let plan = explain_plan_text(&explain);
            assert!(
                !plan.contains("SortExec"),
                "point read with ORDER BY on the pinned pk must elide the sort:\n{plan}"
            );

            let result = session
                .execute(point_sql, &[])
                .await
                .expect("point read should succeed");
            assert_rows_eq(
                result,
                vec![vec![
                    Value::Text("n2".to_string()),
                    Value::Text("Second".to_string()),
                ]],
            );
        }
    }
);

simulation_test!(
    row_multi_key_and_unpinned_order_by_keep_physical_sort,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_pushdown_note_schema(&session).await;
        insert_pushdown_note(&session, "n1", "todo", "First", "1", "NULL").await;
        insert_pushdown_note(&session, "n2", "done", "Second", "2", "NULL").await;

        // A multi-value IN pins nothing: ordering across the matched keys is
        // real work and the sort must stay.
        let multi_sql = "SELECT id FROM pushdown_note WHERE id IN ('n2', 'n1') ORDER BY id";
        let explain = session
            .execute(&format!("EXPLAIN {multi_sql}"), &[])
            .await
            .expect("EXPLAIN should succeed");
        let plan = explain_plan_text(&explain);
        assert!(
            plan.contains("SortExec"),
            "multi-key IN with ORDER BY must keep its physical sort:\n{plan}"
        );
        let result = session
            .execute(multi_sql, &[])
            .await
            .expect("multi-key read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![Value::Text("n1".to_string())],
                vec![Value::Text("n2".to_string())],
            ],
        );

        // An inexact residual predicate proves nothing about the scan output;
        // ordering by an unpinned column must keep its physical sort.
        let range_sql = "SELECT id FROM pushdown_note WHERE score > 0 ORDER BY id";
        let explain = session
            .execute(&format!("EXPLAIN {range_sql}"), &[])
            .await
            .expect("EXPLAIN should succeed");
        let plan = explain_plan_text(&explain);
        assert!(
            plan.contains("SortExec"),
            "range-filtered ORDER BY must keep its physical sort:\n{plan}"
        );
        let result = session
            .execute(range_sql, &[])
            .await
            .expect("range read should succeed");
        assert_rows_eq(
            result,
            vec![
                vec![Value::Text("n1".to_string())],
                vec![Value::Text("n2".to_string())],
            ],
        );
    }
);

// The bound-exactness differential.
//
// `StoragePrefix::to_range` yields a half-open `[lo, hi)`, so an inclusive
// upper bound is the one place a range access path silently loses rows: every
// row equal to `hi` vanishes and the answer is still plausibly shaped. This
// sweeps every `(lo, hi)` pair over a fixture that brackets the data on both
// sides and compares against the set computed directly from the fixture, so a
// bound error at either end fails here rather than in a benchmark.
//
// The ordinals deliberately span zero and negatives: the order-preserving
// integer key encoding is `value ^ (1 << 63)`, and a naive encoder that skips
// the sign flip orders every negative above every positive.
simulation_test!(
    row_range_pushdown_matches_full_scan_at_every_bound,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_range_note_schema(&session).await;
        let ordinals: Vec<i64> = vec![-3, -2, -1, 0, 1, 2, 3, 4, 5, 6];
        for (index, ordinal) in ordinals.iter().enumerate() {
            insert_range_note(&session, &format!("n{index}"), *ordinal).await;
        }

        for lower in -5_i64..=8 {
            for upper in -5_i64..=8 {
                let result = session
                    .execute(
                        &format!(
                            "SELECT id FROM range_note \
                             WHERE ordinal BETWEEN {lower} AND {upper} ORDER BY id"
                        ),
                        &[],
                    )
                    .await
                    .expect("range query should succeed");
                let expected = expected_range_note_ids(&ordinals, |ordinal| {
                    ordinal >= lower && ordinal <= upper
                });
                assert_eq!(
                    range_note_ids(&result),
                    expected,
                    "BETWEEN {lower} AND {upper} must return exactly the full-scan answer"
                );
            }
        }
    }
);

// The same differential for each half-bounded operator, in both operand
// orders.
//
// `5 < ordinal` is `ordinal > 5`, so the literal-on-the-left spelling has to
// reverse the comparison. Reusing the operator returns the complement of the
// requested rows — an error that a one-sided test with the column always on
// the left cannot see.
simulation_test!(
    row_range_pushdown_matches_full_scan_for_each_operator,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_range_note_schema(&session).await;
        let ordinals: Vec<i64> = vec![-3, -2, -1, 0, 1, 2, 3, 4, 5, 6];
        for (index, ordinal) in ordinals.iter().enumerate() {
            insert_range_note(&session, &format!("n{index}"), *ordinal).await;
        }

        for bound in -5_i64..=8 {
            for (operator, reversed) in [("<", ">"), ("<=", ">="), (">", "<"), (">=", "<=")] {
                let expected = expected_range_note_ids(&ordinals, |ordinal| match operator {
                    "<" => ordinal < bound,
                    "<=" => ordinal <= bound,
                    ">" => ordinal > bound,
                    _ => ordinal >= bound,
                });

                let column_first = session
                    .execute(
                        &format!(
                            "SELECT id FROM range_note WHERE ordinal {operator} {bound} ORDER BY id"
                        ),
                        &[],
                    )
                    .await
                    .expect("range query should succeed");
                assert_eq!(
                    range_note_ids(&column_first),
                    expected,
                    "ordinal {operator} {bound} must match the full-scan answer"
                );

                // The mirrored spelling of the identical predicate.
                let literal_first = session
                    .execute(
                        &format!(
                            "SELECT id FROM range_note WHERE {bound} {reversed} ordinal ORDER BY id"
                        ),
                        &[],
                    )
                    .await
                    .expect("reversed range query should succeed");
                assert_eq!(
                    range_note_ids(&literal_first),
                    expected,
                    "{bound} {reversed} ordinal must match ordinal {operator} {bound}"
                );
            }
        }
    }
);

// Engagement, asserted at the plan.
//
// Before this change a range predicate was `Unsupported`: the provider never
// saw it, so it could not reach row-group pruning or any index. `Exact` would
// be wrong — the hot index returns a candidate superset and the open/closed
// bound distinction is enforced by the residual — so the predicate must appear
// as a *partial* filter, meaning pushed down **and** still re-checked above.
simulation_test!(
    row_range_pushdown_reaches_the_table_scan_inexactly,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_range_note_schema(&session).await;

        let explain = session
            .execute(
                "EXPLAIN VERBOSE SELECT id FROM range_note WHERE ordinal BETWEEN 2 AND 4",
                &[],
            )
            .await
            .expect("EXPLAIN should succeed");
        let plan = explain_plan_text(&explain);

        let partial = partial_filters_text(&plan)
            .unwrap_or_else(|| panic!("range predicate should reach the scan:\n{plan}"));
        assert!(
            partial.contains("ordinal"),
            "the ordinal range should be pushed to the scan, got partial_filters={partial}:\n{plan}"
        );
        assert!(
            plan.contains("Filter:") || plan.contains("FilterExec"),
            "an Inexact pushdown must retain a residual filter above the scan:\n{plan}"
        );
    }
);

// The rejection cases, asserted as hard as the acceptance cases.
//
// A `Number` column has no total order (NaN), and `Boolean`/`Jsonb` have no
// useful range, so none of them may become a pushed range. Over-claiming here
// is a wrong answer rather than a slow one, which is why this is asserted
// rather than left to the residual.
simulation_test!(
    row_range_pushdown_refuses_columns_without_a_total_order,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        register_range_note_schema(&session).await;
        insert_range_note(&session, "n0", 1).await;

        for predicate in ["weight > 1.5", "weight BETWEEN 0.5 AND 2.5"] {
            let explain = session
                .execute(
                    &format!("EXPLAIN VERBOSE SELECT id FROM range_note WHERE {predicate}"),
                    &[],
                )
                .await
                .expect("EXPLAIN should succeed");
            let plan = explain_plan_text(&explain);
            if let Some(partial) = partial_filters_text(&plan) {
                assert!(
                    !partial.contains("weight"),
                    "a float column must not become a pushed range, got partial_filters={partial}:\n{plan}"
                );
            }
        }

        // Refusing to push it must not change the answer.
        let result = session
            .execute(
                "SELECT id FROM range_note WHERE weight > 0.5 ORDER BY id",
                &[],
            )
            .await
            .expect("float range query should still answer");
        assert_eq!(range_note_ids(&result), vec!["n0".to_string()]);
    }
);

async fn register_range_note_schema(session: &crate::support::simulation_test::engine::SimSession) {
    // `ordinal` is declared unique so this one fixture also carries hot-index
    // entries, letting the same differential cover the index range seek when
    // that path lands. `weight` is the deliberate negative control: a float
    // column that must never become a pushed range.
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"range_note\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"ordinal\",\"type\":\"int8\",\"nullable\":false},{\"name\":\"lane\",\"type\":\"text\",\"nullable\":false},{\"name\":\"weight\",\"type\":\"float8\",\"nullable\":false}],\"primary_key\":[\"id\"],\"unique\":[[\"ordinal\"]]}' AS JSONB),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("range_note schema should register");
}

async fn insert_range_note(
    session: &crate::support::simulation_test::engine::SimSession,
    id: &str,
    ordinal: i64,
) {
    session
        .execute(
            &format!(
                "INSERT INTO range_note (id, ordinal, lane, weight) \
                 VALUES ('{id}', {ordinal}, 'lane-{}', 1.5)",
                ordinal.rem_euclid(4)
            ),
            &[],
        )
        .await
        .expect("range_note row should insert");
}

fn range_note_ids(result: &ExecuteResult) -> Vec<String> {
    result
        .rows()
        .iter()
        .filter_map(|row| match row.values().first() {
            Some(Value::Text(value)) => Some(value.clone()),
            _ => None,
        })
        .collect()
}

/// The answer computed straight from the fixture, ordered the way the query
/// orders it. This is the "full scan" side of the differential.
fn expected_range_note_ids(ordinals: &[i64], matches: impl Fn(i64) -> bool) -> Vec<String> {
    let mut ids: Vec<String> = ordinals
        .iter()
        .enumerate()
        .filter(|(_, ordinal)| matches(**ordinal))
        .map(|(index, _)| format!("n{index}"))
        .collect();
    ids.sort();
    ids
}

/// The `partial_filters=[...]` list from an EXPLAIN, when the scan has one.
fn partial_filters_text(plan: &str) -> Option<String> {
    let start = plan.find("partial_filters=[")? + "partial_filters=[".len();
    let rest = &plan[start..];
    let end = rest.find(']')?;
    Some(rest[..end].to_string())
}

async fn register_pushdown_note_schema(
    session: &crate::support::simulation_test::engine::SimSession,
) {
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"pushdown_note\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"kind\",\"type\":\"text\",\"nullable\":false},{\"name\":\"title\",\"type\":\"text\",\"nullable\":false},{\"name\":\"score\",\"type\":\"float8\",\"nullable\":false},{\"name\":\"optional\",\"type\":\"jsonb\",\"nullable\":true}],\"primary_key\":[\"id\"]}' AS JSONB),\
             false,\
             false\
             )",
            &[],
        )
        .await
        .expect("pushdown_note schema should register");
}

async fn insert_pushdown_note(
    session: &crate::support::simulation_test::engine::SimSession,
    id: &str,
    kind: &str,
    title: &str,
    score_json: &str,
    optional_sql: &str,
) {
    session
        .execute(
            &format!(
                "INSERT INTO pushdown_note (id, kind, title, score, optional) \
                 VALUES ('{id}', '{kind}', '{title}', {score_json}, {optional_sql})"
            ),
            &[],
        )
        .await
        .expect("pushdown_note row should insert");
}

fn explain_plan_text(result: &ExecuteResult) -> String {
    result
        .rows()
        .iter()
        .flat_map(|row| row.values().iter())
        .map(|value| match value {
            Value::Text(value) => value.clone(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// https://github.com/opral/inlang/issues/4417
// Deterministic mode adds an untracked sequence write, which prevents the
// tracked-only packed publication route this regression needs to exercise.
simulation_test!(
    small_import_preserves_foreign_key_filters_and_nested_joins,
    options = crate::support::simulation_test::engine::SimulationOptions {
        deterministic: false
    },
    |sim| async move { assert_import_indexes(&sim, 34).await }
);

simulation_test!(
    packed_import_preserves_foreign_key_filters_and_nested_joins,
    options = crate::support::simulation_test::engine::SimulationOptions {
        deterministic: false
    },
    |sim| async move { assert_import_indexes(&sim, 35).await }
);

async fn assert_import_indexes(
    sim: &crate::support::simulation_test::engine::Simulation,
    bundle_count: usize,
) {
    // 34 * (1 + 7 + 7) = 510 rows; 35 * 15 = 525 crosses the
    // packed-current-base publication threshold in a single commit.
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    for (key, columns, foreign_keys) in [
        (
            "bundle",
            serde_json::json!([
                {"name":"id","type":"text","nullable":false}
            ]),
            serde_json::json!([]),
        ),
        (
            "message",
            serde_json::json!([
                {"name":"id","type":"text","nullable":false},
                {"name":"bundle_id","type":"text","nullable":false}
            ]),
            serde_json::json!([
                {"columns":["bundle_id"],"references":{"schema_key":"bundle","columns":["id"]}}
            ]),
        ),
        (
            "variant",
            serde_json::json!([
                {"name":"id","type":"text","nullable":false},
                {"name":"message_id","type":"text","nullable":false},
                {"name":"pattern","type":"text","nullable":false}
            ]),
            serde_json::json!([
                {"columns":["message_id"],"references":{"schema_key":"message","columns":["id"]}}
            ]),
        ),
    ] {
        let schema = serde_json::json!({
            "$schema":"https://lix.dev/schema-v1.json",
            "key":key,"columns":columns,"primary_key":["id"],"foreign_keys":foreign_keys
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
    }
    let mut bundles = Vec::new();
    let mut messages = Vec::new();
    let mut variants = Vec::new();
    let mut expected = Vec::new();
    for bundle in 0..bundle_count {
        let bundle_id = format!("b{bundle:02}");
        bundles.push(format!("('{bundle_id}')"));
        for locale in 0..7 {
            let message_id = format!("{bundle_id}_m{locale}");
            let variant_id = format!("{message_id}_v");
            messages.push(format!("('{message_id}','{bundle_id}')"));
            variants.push(format!(
                "('{variant_id}','{message_id}','Translated {message_id}')"
            ));
            expected.push(vec![
                Value::Text(bundle_id.clone()),
                Value::Text(message_id.clone()),
                Value::Text(variant_id),
                Value::Text(format!("Translated {message_id}")),
            ]);
        }
    }
    let mut tx = session.begin_transaction().await.unwrap();
    for (table, columns, values) in [
        ("bundle", "id", bundles),
        ("message", "id,bundle_id", messages),
        ("variant", "id,message_id,pattern", variants),
    ] {
        tx.execute(
            &format!(
                "INSERT INTO {table} ({columns}) VALUES {}",
                values.join(",")
            ),
            &[],
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();

    let scan = session
        .execute(
            "SELECT id FROM message WHERE concat(bundle_id, '') = 'b00' ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(scan.len(), 7);
    let indexed = session
        .execute(
            "SELECT id FROM message WHERE bundle_id = 'b00' ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    assert_rows_eq(
        indexed,
        scan.rows().iter().map(|r| r.values().to_vec()).collect(),
    );
    let nested = session
        .execute(
            "SELECT b.id, m.id, v.id, v.pattern FROM bundle b \
         LEFT JOIN message m ON m.bundle_id = b.id \
         LEFT JOIN variant v ON v.message_id = m.id ORDER BY b.id, m.id, v.id",
            &[],
        )
        .await
        .unwrap();
    assert_rows_eq(nested, expected);
}

simulation_test!(
    packed_writes_preserve_unique_column_index,
    options = crate::support::simulation_test::engine::SimulationOptions {
        deterministic: false
    },
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let schema = serde_json::json!({
            "$schema":"https://lix.dev/schema-v1.json", "key":"indexed_note",
            "columns":[
                {"name":"id","type":"text","nullable":false},
                {"name":"label","type":"text","nullable":false}
            ],
            "primary_key":["id"], "unique":[["label"]]
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        // Seed the same generation's index before the packed append.
        session
            .execute(
                "INSERT INTO indexed_note (id, label) VALUES ('seed', 'seed')",
                &[],
            )
            .await
            .unwrap();
        let values = (0..512)
            .map(|i| format!("('n{i:03}', 'label{i:03}')"))
            .collect::<Vec<_>>();
        session
            .execute(
                &format!(
                    "INSERT INTO indexed_note (id, label) VALUES {}",
                    values.join(",")
                ),
                &[],
            )
            .await
            .unwrap();
        for label in ["seed", "label000", "label511"] {
            let result = session
                .execute(
                    "SELECT id FROM indexed_note WHERE label = $1",
                    &[Value::Text(label.into())],
                )
                .await
                .unwrap();
            assert_eq!(result.len(), 1, "packed append must index {label}");
        }
        let duplicate = session
            .execute(
                "INSERT INTO indexed_note (id, label) VALUES ('duplicate', 'label000')",
                &[],
            )
            .await
            .expect_err("packed rows must still enforce unique constraints");
        assert_eq!(duplicate.code, lix::LixError::CODE_UNIQUE);
        session
            .execute("UPDATE indexed_note SET label = concat('new_', label)", &[])
            .await
            .unwrap();
        for label in ["new_seed", "new_label000", "new_label511"] {
            let result = session
                .execute(
                    "SELECT id FROM indexed_note WHERE label = $1",
                    &[Value::Text(label.into())],
                )
                .await
                .unwrap();
            assert_eq!(result.len(), 1, "packed replacement must index {label}");
        }
        assert_eq!(
            session
                .execute("SELECT id FROM indexed_note WHERE label = 'label000'", &[])
                .await
                .unwrap()
                .len(),
            0
        );
    }
);

simulation_test!(
    indexed_range_projects_nullable_typed_primitives,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let schema = serde_json::json!({
            "$schema":"https://lix.dev/schema-v1.json", "key":"nullable_range_note",
            "columns":[
                {"name":"id","type":"text","nullable":false},
                {"name":"count","type":"int8","nullable":true},
                {"name":"ratio","type":"float8","nullable":true},
                {"name":"active","type":"boolean","nullable":true},
                {"name":"time","type":"timestamptz","nullable":true}
            ],
            "primary_key":["id"]
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        // A selective key range over a larger table uses the typed-row projection.
        let values = (0..255)
            .map(|i| {
                if i == 68 {
                    format!("('n{i:03}',NULL,NULL,NULL,NULL)")
                } else {
                    format!("('n{i:03}',7,4.5,TRUE,'2026-09-15T01:02:03.123456Z')")
                }
            })
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(
                &format!(
                    "INSERT INTO nullable_range_note (id,count,ratio,active,time) VALUES {values}"
                ),
                &[],
            )
            .await
            .unwrap();
        for stage in 0..3 {
            if stage == 1 {
                session.execute("UPDATE nullable_range_note SET count=NULL,ratio=NULL,active=NULL,time=NULL WHERE id='n067'", &[]).await.unwrap();
            } else if stage == 2 {
                session
                    .execute("DELETE FROM nullable_range_note WHERE id='n066'", &[])
                    .await
                    .unwrap();
            }
            let full = session
                .execute(
                    "SELECT id,count,ratio,active,time FROM nullable_range_note ORDER BY id",
                    &[],
                )
                .await
                .unwrap();
            let expected = full.rows().iter().filter(|row| matches!(&row.values()[0], Value::Text(id) if id.as_str() >= "n060" && id.as_str() < "n070")).map(|row| row.values().to_vec()).collect::<Vec<_>>();
            for _ in 0..2 {
                let range = session.execute("SELECT id,count,ratio,active,time FROM nullable_range_note WHERE id >= 'n060' AND id < 'n070' ORDER BY id", &[]).await.expect("key range must project nullable typed values");
                assert_rows_eq(range, expected.clone());
            }
        }
    }
);


simulation_test!(foreign_key_restricts_parent_point_delete, |sim| async move {
    assert_parent_delete_restricted(&sim, "DELETE FROM fk_delete_parent WHERE id = 'p1'").await;
});

simulation_test!(foreign_key_restricts_parent_collection_delete, |sim| async move {
    assert_parent_delete_restricted(&sim, "DELETE FROM fk_delete_parent").await;
});

async fn assert_parent_delete_restricted(sim: &crate::support::simulation_test::engine::Simulation, delete_sql: &str) {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    for schema in [
        serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json", "key": "fk_delete_parent",
            "columns": [{"name":"id", "type":"text", "nullable":false}],
            "primary_key": ["id"]
        }),
        serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json", "key": "fk_delete_child",
            "columns": [{"name":"id", "type":"text", "nullable":false},
                        {"name":"parent_id", "type":"text", "nullable":false}],
            "primary_key": ["id"],
            "foreign_keys": [{"columns":["parent_id"], "references":{
                "schema_key":"fk_delete_parent", "columns":["id"]}}]
        }),
    ] {
        session.execute("INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
            &[Value::Text(schema.to_string())]).await.unwrap();
    }
    let error = session.execute("INSERT INTO fk_delete_child (id, parent_id) VALUES ('bad', 'missing')", &[])
        .await.expect_err("missing parent must be rejected");
    assert_eq!(error.code, lix::LixError::CODE_FOREIGN_KEY);
    session.execute("INSERT INTO fk_delete_parent (id) VALUES ('p1'), ('p2')", &[]).await.unwrap();
    session.execute("INSERT INTO fk_delete_child (id, parent_id) VALUES ('c1', 'p1')", &[]).await.unwrap();
    let error = session.execute(delete_sql, &[]).await.expect_err("referenced parent must not be deleted");
    assert_eq!(error.code, lix::LixError::CODE_FOREIGN_KEY);
    assert_rows_eq(session.execute("SELECT id FROM fk_delete_parent ORDER BY id", &[]).await.unwrap(),
        vec![vec![Value::Text("p1".into())], vec![Value::Text("p2".into())]]);
    assert_rows_eq(session.execute("SELECT parent_id FROM fk_delete_child", &[]).await.unwrap(),
        vec![vec![Value::Text("p1".into())]]);
    // Final-state validation must still allow removing both sides together.
    let mut tx = session.begin_transaction().await.unwrap();
    tx.execute("DELETE FROM fk_delete_child", &[]).await.unwrap();
    tx.execute(delete_sql, &[]).await.unwrap();
    tx.commit().await.expect("deleting the child and parent together is valid");
}

simulation_test!(foreign_key_cascade_statement_visibility_and_rollback, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    for (key, target) in [("cascade_parent", None), ("cascade_child", Some("cascade_parent")), ("cascade_leaf", Some("cascade_child"))] {
        let mut schema = serde_json::json!({
            "$schema":"https://lix.dev/schema-v1.json", "key":key,
            "columns":[{"name":"id","type":"text","nullable":false}, {"name":"parent_id","type":"text"}],
            "primary_key":["id"]
        });
        if let Some(target) = target {
            schema["foreign_keys"] = serde_json::json!([{"columns":["parent_id"],"references":{"schema_key":target,"columns":["id"]},"on_delete":"cascade"}]);
        }
        session.execute("INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)", &[Value::Text(schema.to_string())]).await.unwrap();
    }
    session.execute("INSERT INTO cascade_parent(id) VALUES ('p')", &[]).await.unwrap();
    session.execute("INSERT INTO cascade_child(id,parent_id) VALUES ('c','p'), ('null',NULL)", &[]).await.unwrap();
    session.execute("INSERT INTO cascade_leaf(id,parent_id) VALUES ('l','c')", &[]).await.unwrap();
    let mut tx = session.begin_transaction().await.unwrap();
    tx.execute("INSERT INTO cascade_child(id,parent_id) VALUES ('pending','p')", &[]).await.unwrap();
    tx.execute("INSERT INTO cascade_leaf(id,parent_id) VALUES ('pending_leaf','pending')", &[]).await.unwrap();
    tx.execute("INSERT INTO cascade_parent(id) VALUES ('new_parent')", &[]).await.unwrap();
    tx.execute("INSERT INTO cascade_child(id,parent_id) VALUES ('new_child','new_parent')", &[]).await.unwrap();
    tx.execute("DELETE FROM cascade_parent WHERE id=$1", &[Value::Text("new_parent".into())]).await.unwrap();
    tx.execute("DELETE FROM cascade_parent WHERE id=$1", &[Value::Text("p".into())]).await.unwrap();
    assert_rows_eq(tx.execute("SELECT id FROM cascade_child", &[]).await.unwrap(), vec![vec![Value::Text("null".into())]]);
    assert_rows_eq(tx.execute("SELECT id FROM cascade_leaf", &[]).await.unwrap(), vec![]);
    tx.rollback().await.unwrap();
    assert_rows_eq(session.execute("SELECT id FROM cascade_leaf", &[]).await.unwrap(), vec![vec![Value::Text("l".into())]]);
    let mut invalid = session.begin_transaction().await.unwrap();
    invalid.execute("DELETE FROM cascade_parent", &[]).await.unwrap();
    invalid.execute("INSERT INTO cascade_child(id,parent_id) VALUES ('too_late','p')", &[]).await.unwrap();
    assert_eq!(invalid.commit().await.unwrap_err().code, lix::LixError::CODE_FOREIGN_KEY);
    session.execute("DELETE FROM cascade_parent", &[]).await.unwrap();
    assert_rows_eq(session.execute("SELECT id FROM cascade_child", &[]).await.unwrap(), vec![vec![Value::Text("null".into())]]);
    assert_rows_eq(session.execute("SELECT id FROM cascade_leaf", &[]).await.unwrap(), vec![]);
    let error = session.execute("INSERT INTO cascade_child(id,parent_id) VALUES ('bad','never_existed')", &[]).await.unwrap_err();
    assert_eq!(error.code, lix::LixError::CODE_FOREIGN_KEY);
});

simulation_test!(foreign_key_cascade_merge_destination_delete, |sim| async move {
    assert_cascade_merge(&sim, true).await;
});
simulation_test!(foreign_key_cascade_merge_source_delete, |sim| async move {
    assert_cascade_merge(&sim, false).await;
});
async fn assert_cascade_merge(sim: &crate::support::simulation_test::engine::Simulation, deletion_on_destination: bool) {
    use lix::{CreateBranchOptions, MergeBranchOptions, MergeBranchPreviewOptions};
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        for schema in [
            serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"cascade_conversation","columns":[{"name":"id","type":"text","nullable":false}],"primary_key":["id"]}),
            serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"cascade_comment","columns":[{"name":"id","type":"text","nullable":false},{"name":"conversation_id","type":"text"}],"primary_key":["id"],"foreign_keys":[{"columns":["conversation_id"],"references":{"schema_key":"cascade_conversation","columns":["id"]},"on_delete":"cascade"}]}),
        ] {
            main.execute("INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)", &[Value::Text(schema.to_string())]).await.unwrap();
        }
        main.execute("INSERT INTO cascade_conversation(id) VALUES ('p')", &[]).await.unwrap();
        let branch = main.create_branch(CreateBranchOptions { id: None, name: "reply".into(), from_commit_id: None }).await.unwrap();
        let source = sim.wrap_session(engine.open_session_at(branch.id.clone()).await.unwrap(), &engine);
        let (deleted, replied) = if deletion_on_destination { (&main, &source) } else { (&source, &main) };
        deleted.execute("DELETE FROM cascade_conversation", &[]).await.unwrap();
        replied.execute("INSERT INTO cascade_comment(id,conversation_id) VALUES ('c','p')", &[]).await.unwrap();
        // The deletion is scoped to its branch.
        assert_rows_eq(replied.execute("SELECT id FROM cascade_conversation", &[]).await.unwrap(), vec![vec![Value::Text("p".into())]]);
        let preview = main.merge_branch_preview(MergeBranchPreviewOptions { source_branch_id: branch.id.clone() }).await.unwrap();
        assert_rows_eq(replied.execute("SELECT id FROM cascade_comment", &[]).await.unwrap(), vec![vec![Value::Text("c".into())]]);
        let receipt = main.merge_branch(MergeBranchOptions { source_branch_id: branch.id }).await.unwrap();
        assert_eq!(preview.change_stats, receipt.change_stats);
        assert_rows_eq(main.execute("SELECT id FROM cascade_comment", &[]).await.unwrap(), vec![]);
        assert_rows_eq(main.execute("SELECT id FROM cascade_conversation", &[]).await.unwrap(), vec![]);
}

simulation_test!(foreign_key_cascade_composite_unique_and_cycles, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    for schema in [
        serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"cascade_pair","columns":[{"name":"id","type":"text","nullable":false},{"name":"a","type":"text","nullable":false},{"name":"b","type":"int8","nullable":false}],"primary_key":["id"],"unique":[["a","b"]]}),
        serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"cascade_link","columns":[{"name":"id","type":"text","nullable":false},{"name":"a","type":"text"},{"name":"b","type":"int8"}],"primary_key":["id"],"foreign_keys":[{"columns":["a","b"],"references":{"schema_key":"cascade_pair","columns":["a","b"]},"on_delete":"cascade"}]}),
        serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"cascade_cycle","columns":[{"name":"id","type":"text","nullable":false},{"name":"parent_id","type":"text"}],"primary_key":["id"],"foreign_keys":[{"columns":["parent_id"],"references":{"schema_key":"cascade_cycle","columns":["id"]},"on_delete":"cascade"}]}),
    ] {
        session.execute("INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)", &[Value::Text(schema.to_string())]).await.unwrap();
    }
    session.execute("INSERT INTO cascade_pair(id,a,b) VALUES ('p','a',1),('q','a',2)", &[]).await.unwrap();
    session.execute("INSERT INTO cascade_link(id,a,b) VALUES ('match','a',1),('other','a',2),('null','a',NULL)", &[]).await.unwrap();
    session.execute("DELETE FROM cascade_pair WHERE id='p'", &[]).await.unwrap();
    assert_rows_eq(session.execute("SELECT id FROM cascade_link ORDER BY id", &[]).await.unwrap(), vec![vec![Value::Text("null".into())],vec![Value::Text("other".into())]]);
    // Insert both ends before final validation, then traverse the cycle once.
    let mut tx = session.begin_transaction().await.unwrap();
    tx.execute("INSERT INTO cascade_cycle(id,parent_id) VALUES ('a','b'),('b','a')", &[]).await.unwrap();
    tx.commit().await.unwrap();
    session.execute("DELETE FROM cascade_cycle WHERE id='a'", &[]).await.unwrap();
    assert_rows_eq(session.execute("SELECT id FROM cascade_cycle", &[]).await.unwrap(), vec![]);
});

simulation_test!(foreign_key_cascade_concurrent_reply_is_atomic,
    // Deterministic mode deliberately serializes writers for stable IDs.
    options = crate::support::simulation_test::engine::SimulationOptions { deterministic: false },
    |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    for schema in [
        serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"race_parent","columns":[{"name":"id","type":"text","nullable":false}],"primary_key":["id"]}),
        serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"race_child","columns":[{"name":"id","type":"text","nullable":false},{"name":"parent_id","type":"text","nullable":false}],"primary_key":["id"],"foreign_keys":[{"columns":["parent_id"],"references":{"schema_key":"race_parent","columns":["id"]},"on_delete":"cascade"}]}),
    ] {
        session.execute("INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)", &[Value::Text(schema.to_string())]).await.unwrap();
    }
    session.execute("INSERT INTO race_parent(id) VALUES ('p')", &[]).await.unwrap();
    let other = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    let mut deletion = session.begin_transaction().await.unwrap();
    deletion.execute("DELETE FROM race_parent WHERE id='p'", &[]).await.unwrap();
    // The committed child was absent when the DELETE statement planned actions.
    other.execute("INSERT INTO race_child(id,parent_id) VALUES ('concurrent','p')", &[]).await.unwrap();
    match deletion.commit().await {
        Ok(_) => assert_rows_eq(session.execute("SELECT id FROM race_child", &[]).await.unwrap(), vec![]),
        Err(error) => {
            assert_eq!(error.code, lix::LixError::CODE_TRANSACTION_CONFLICT);
            assert_rows_eq(session.execute("SELECT id FROM race_child", &[]).await.unwrap(), vec![vec![Value::Text("concurrent".into())]]);
            assert_rows_eq(session.execute("SELECT id FROM race_parent", &[]).await.unwrap(), vec![vec![Value::Text("p".into())]]);
            session.execute("DELETE FROM race_parent WHERE id='p'", &[]).await.unwrap();
            assert_rows_eq(session.execute("SELECT id FROM race_child", &[]).await.unwrap(), vec![]);
        }
    }
    assert_rows_eq(session.execute("SELECT id FROM race_parent", &[]).await.unwrap(), vec![]);
    let error = other.execute("INSERT INTO race_child(id,parent_id) VALUES ('late','p')", &[]).await.unwrap_err();
    assert_eq!(error.code, lix::LixError::CODE_FOREIGN_KEY);
});

simulation_test!(foreign_key_no_action_merge_rejects_orphan_in_preview_and_execution, |sim| async move {
    use lix::{CreateBranchOptions, MergeBranchOptions, MergeBranchPreviewOptions};
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    for schema in [
        serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"merge_parent","columns":[{"name":"id","type":"text","nullable":false}],"primary_key":["id"]}),
        serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"merge_child","columns":[{"name":"id","type":"text","nullable":false},{"name":"parent_id","type":"text","nullable":false}],"primary_key":["id"],"foreign_keys":[{"columns":["parent_id"],"references":{"schema_key":"merge_parent","columns":["id"]}}]}),
    ] {
        main.execute("INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)", &[Value::Text(schema.to_string())]).await.unwrap();
    }
    main.execute("INSERT INTO merge_parent(id) VALUES ('p')", &[]).await.unwrap();
    let branch = main.create_branch(CreateBranchOptions { id: None, name: "reply".into(), from_commit_id: None }).await.unwrap();
    let source = sim.wrap_session(engine.open_session_at(branch.id.clone()).await.unwrap(), &engine);
    main.execute("DELETE FROM merge_parent", &[]).await.unwrap();
    source.execute("INSERT INTO merge_child(id,parent_id) VALUES ('c','p')", &[]).await.unwrap();
    let preview = main.merge_branch_preview(MergeBranchPreviewOptions { source_branch_id: branch.id.clone() }).await.unwrap_err();
    let execution = main.merge_branch(MergeBranchOptions { source_branch_id: branch.id }).await.unwrap_err();
    assert_eq!(preview.code, lix::LixError::CODE_FOREIGN_KEY);
    assert_eq!(execution.code, preview.code);
    assert_rows_eq(main.execute("SELECT id FROM merge_child", &[]).await.unwrap(), vec![]);
    assert_rows_eq(source.execute("SELECT id FROM merge_child", &[]).await.unwrap(), vec![vec![Value::Text("c".into())]]);
});

simulation_test!(foreign_key_cascade_merge_incoming_schema, |sim| async move {
    assert_generation_cascade_merge(&sim, false, "cascade").await;
});
simulation_test!(foreign_key_cascade_merge_incoming_generation, |sim| async move {
    assert_generation_cascade_merge(&sim, true, "cascade").await;
});
simulation_test!(foreign_key_no_action_merge_incoming_generation, |sim| async move {
    assert_generation_cascade_merge(&sim, true, "no_action").await;
});
async fn assert_generation_cascade_merge(
    sim: &crate::support::simulation_test::engine::Simulation,
    incoming_delete: bool,
    action: &str,
) {
    use lix::{CreateBranchOptions, MergeBranchOptions, MergeBranchPreviewOptions};
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    let parent = serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"incoming_parent","columns":[{"name":"id","type":"text","nullable":false}],"primary_key":["id"]});
    main.execute("INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)", &[Value::Text(parent.to_string())]).await.unwrap();
    main.execute("INSERT INTO incoming_parent(id) VALUES ('p')", &[]).await.unwrap();
    let branch = main.create_branch(CreateBranchOptions { id: None, name: "new-schema".into(), from_commit_id: None }).await.unwrap();
    let source = sim.wrap_session(engine.open_session_at(branch.id.clone()).await.unwrap(), &engine);
    let deleting = if incoming_delete { &source } else { &main };
    let replying = if incoming_delete { &main } else { &source };
    deleting.execute("DELETE FROM incoming_parent", &[]).await.unwrap();
    let child = serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"incoming_child","columns":[{"name":"id","type":"text","nullable":false},{"name":"parent_id","type":"text"}],"primary_key":["id"],"foreign_keys":[{"columns":["parent_id"],"references":{"schema_key":"incoming_parent","columns":["id"]},"on_delete":action}]});
    replying.execute("INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)", &[Value::Text(child.to_string())]).await.unwrap();
    replying.execute("INSERT INTO incoming_child(id,parent_id) VALUES ('c','p')", &[]).await.unwrap();
    if action == "no_action" {
        let preview = main.merge_branch_preview(MergeBranchPreviewOptions { source_branch_id: branch.id.clone() }).await.unwrap_err();
        let execution = main.merge_branch(MergeBranchOptions { source_branch_id: branch.id }).await.unwrap_err();
        assert_eq!(preview.code, lix::LixError::CODE_FOREIGN_KEY);
        assert_eq!(execution.code, preview.code);
        assert_rows_eq(main.execute("SELECT id FROM incoming_parent", &[]).await.unwrap(), vec![vec![Value::Text("p".into())]]);
        assert_rows_eq(main.execute("SELECT id FROM incoming_child", &[]).await.unwrap(), vec![vec![Value::Text("c".into())]]);
        return;
    }
    let preview = main.merge_branch_preview(MergeBranchPreviewOptions { source_branch_id: branch.id.clone() }).await.unwrap();
    let receipt = main.merge_branch(MergeBranchOptions { source_branch_id: branch.id }).await.unwrap();
    assert_eq!(preview.change_stats, receipt.change_stats);
    assert_rows_eq(main.execute("SELECT id FROM incoming_child", &[]).await.unwrap(), vec![]);
    assert_rows_eq(main.execute("SELECT id FROM incoming_parent", &[]).await.unwrap(), vec![]);
    if !incoming_delete {
        assert_rows_eq(source.execute("SELECT id FROM incoming_child", &[]).await.unwrap(), vec![vec![Value::Text("c".into())]]);
        let schema_diff = main.execute(
            "SELECT COUNT(*) AS n FROM lix_diff('lix_registered_schema', $1, $2)",
            &[Value::Text(receipt.source_head_before_commit_id), Value::Text(receipt.target_head_after_commit_id)],
        ).await.unwrap();
        assert_eq!(schema_diff.rows()[0].get::<i64>("n").unwrap(), 0, "merge must retain the selected schema change identity");
    }
}

simulation_test!(foreign_key_cascade_stops_at_no_action_atomically, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    for (key, target, action) in [("boundary_parent", None, "no_action"), ("boundary_child", Some("boundary_parent"), "cascade"), ("boundary_leaf", Some("boundary_child"), "no_action")] {
        let mut schema = serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":key,"columns":[{"name":"id","type":"text","nullable":false},{"name":"parent_id","type":"text"}],"primary_key":["id"]});
        if let Some(target) = target {
            schema["foreign_keys"] = serde_json::json!([{"columns":["parent_id"],"references":{"schema_key":target,"columns":["id"]},"on_delete":action}]);
        }
        session.execute("INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)", &[Value::Text(schema.to_string())]).await.unwrap();
    }
    session.execute("INSERT INTO boundary_parent(id) VALUES ('p')", &[]).await.unwrap();
    session.execute("INSERT INTO boundary_child(id,parent_id) VALUES ('c','p')", &[]).await.unwrap();
    session.execute("INSERT INTO boundary_leaf(id,parent_id) VALUES ('l','c')", &[]).await.unwrap();
    let error = session.execute("DELETE FROM boundary_parent", &[]).await.unwrap_err();
    assert_eq!(error.code, lix::LixError::CODE_FOREIGN_KEY);
    assert_rows_eq(session.execute("SELECT id FROM boundary_parent", &[]).await.unwrap(), vec![vec![Value::Text("p".into())]]);
    assert_rows_eq(session.execute("SELECT id FROM boundary_child", &[]).await.unwrap(), vec![vec![Value::Text("c".into())]]);
    let mut tx = session.begin_transaction().await.unwrap();
    tx.execute("DELETE FROM boundary_parent", &[]).await.unwrap();
    tx.execute("DELETE FROM boundary_leaf", &[]).await.unwrap();
    tx.commit().await.unwrap();
    assert_rows_eq(session.execute("SELECT id FROM boundary_child", &[]).await.unwrap(), vec![]);
});

simulation_test!(foreign_key_cascade_merge_changed_referenced_key, |sim| async move {
    use lix::{CreateBranchOptions, MergeBranchOptions, MergeBranchPreviewOptions};
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    for schema in [
        serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"mutable_parent","columns":[{"name":"id","type":"text","nullable":false},{"name":"code","type":"text","nullable":false}],"primary_key":["id"],"unique":[["code"]]}),
        serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"mutable_child","columns":[{"name":"id","type":"text","nullable":false},{"name":"parent_code","type":"text"}],"primary_key":["id"],"foreign_keys":[{"columns":["parent_code"],"references":{"schema_key":"mutable_parent","columns":["code"]},"on_delete":"cascade"}]}),
    ] {
        main.execute("INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)", &[Value::Text(schema.to_string())]).await.unwrap();
    }
    main.execute("INSERT INTO mutable_parent(id,code) VALUES ('p','old')", &[]).await.unwrap();
    let branch = main.create_branch(CreateBranchOptions { id: None, name: "changed-key".into(), from_commit_id: None }).await.unwrap();
    let source = sim.wrap_session(engine.open_session_at(branch.id.clone()).await.unwrap(), &engine);
    main.execute("UPDATE mutable_parent SET code='new' WHERE id='p'", &[]).await.unwrap();
    main.execute("INSERT INTO mutable_child(id,parent_code) VALUES ('c','new')", &[]).await.unwrap();
    // Incoming deletion wins whole-row reconciliation against the target's
    // changed unique key; cascade matching must use the target's live value.
    source.execute("DELETE FROM mutable_parent", &[]).await.unwrap();
    let preview = main.merge_branch_preview(MergeBranchPreviewOptions { source_branch_id: branch.id.clone() }).await.unwrap();
    let receipt = main.merge_branch(MergeBranchOptions { source_branch_id: branch.id }).await.unwrap();
    assert_eq!(preview.change_stats, receipt.change_stats);
    assert_rows_eq(main.execute("SELECT id FROM mutable_parent", &[]).await.unwrap(), vec![]);
    assert_rows_eq(main.execute("SELECT id FROM mutable_child", &[]).await.unwrap(), vec![]);
});


simulation_test!(foreign_key_cascade_merge_rejects_tracked_untracked_identity_collision, |sim| async move {
    use lix::{CreateBranchOptions, MergeBranchOptions, MergeBranchPreviewOptions};
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    for schema in [
        serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"lane_parent","columns":[{"name":"id","type":"text","nullable":false}],"primary_key":["id"]}),
        serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"lane_child","columns":[{"name":"id","type":"text","nullable":false},{"name":"parent_id","type":"text","nullable":false}],"primary_key":["id"],"foreign_keys":[{"columns":["parent_id"],"references":{"schema_key":"lane_parent","columns":["id"]},"on_delete":"cascade"}]}),
    ] {
        main.execute("INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)", &[Value::Text(schema.to_string())]).await.unwrap();
    }
    main.execute("INSERT INTO lane_parent(id) VALUES ('p'),('q')", &[]).await.unwrap();
    let branch = main.create_branch(CreateBranchOptions { id: None, name: "tracked-child".into(), from_commit_id: None }).await.unwrap();
    let source = sim.wrap_session(engine.open_session_at(branch.id.clone()).await.unwrap(), &engine);
    // Diverge tracked history so this uses merge preparation, not a fast-forward.
    main.execute("INSERT INTO lane_parent(id) VALUES ('target-only')", &[]).await.unwrap();
    main.execute("INSERT INTO lane_child(id,parent_id,lixcol_untracked) VALUES ('c','p',true)", &[]).await.unwrap();
    source.execute("INSERT INTO lane_child(id,parent_id) VALUES ('c','q')", &[]).await.unwrap();
    source.execute("DELETE FROM lane_parent WHERE id='p'", &[]).await.unwrap();
    let preview_error = main.merge_branch_preview(MergeBranchPreviewOptions { source_branch_id: branch.id.clone() }).await.unwrap_err();
    let merge_error = main.merge_branch(MergeBranchOptions { source_branch_id: branch.id.clone() }).await.unwrap_err();
    assert_eq!(preview_error.code, lix::LixError::CODE_MERGE_CONFLICT);
    assert_eq!(merge_error.code, preview_error.code);
    assert_rows_eq(main.execute("SELECT id,parent_id,lixcol_untracked FROM lane_child", &[]).await.unwrap(), vec![vec![Value::Text("c".into()), Value::Text("p".into()), Value::Boolean(true)]]);
    assert_rows_eq(main.execute("SELECT id FROM lane_parent ORDER BY id", &[]).await.unwrap(), vec![vec![Value::Text("p".into())], vec![Value::Text("q".into())], vec![Value::Text("target-only".into())]]);
    // Resolve the existing durability conflict. Other untracked dependents
    // still cascade, without inflating tracked merge statistics.
    main.execute("DELETE FROM lane_child WHERE id='c'", &[]).await.unwrap();
    main.execute("INSERT INTO lane_child(id,parent_id,lixcol_untracked) VALUES ('d','p',true)", &[]).await.unwrap();
    let preview = main.merge_branch_preview(MergeBranchPreviewOptions { source_branch_id: branch.id.clone() }).await.unwrap();
    assert_rows_eq(main.execute("SELECT id FROM lane_child", &[]).await.unwrap(), vec![vec![Value::Text("d".into())]]);
    let receipt = main.merge_branch(MergeBranchOptions { source_branch_id: branch.id }).await.unwrap();
    assert_eq!(preview.change_stats, receipt.change_stats);
    assert_eq!(receipt.change_stats.added, 1);
    assert_eq!(receipt.change_stats.removed, 1);
    assert_rows_eq(main.execute("SELECT id,parent_id,lixcol_untracked FROM lane_child", &[]).await.unwrap(), vec![vec![Value::Text("c".into()), Value::Text("q".into()), Value::Boolean(false)]]);
    assert_rows_eq(main.execute("SELECT id FROM lane_parent ORDER BY id", &[]).await.unwrap(), vec![vec![Value::Text("q".into())], vec![Value::Text("target-only".into())]]);
});

simulation_test!(
    registered_insert_select_streams_typed_rows_and_self_reads,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register_pushdown_note_schema(&session).await;
        insert_pushdown_note(
            &session,
            "a",
            "todo",
            "First",
            "7.5",
            "CAST('{\"ok\":true}' AS JSONB)",
        )
        .await;
        let result = session.execute(
            "INSERT INTO pushdown_note (id,kind,title,score,optional) SELECT id || '-copy',kind,title,score,optional FROM pushdown_note RETURNING id,score,optional", &[]
        ).await.expect("registered self-copy should succeed");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].get::<String>("id").unwrap(), "a-copy");
        assert_eq!(result.rows()[0].get::<f64>("score").unwrap(), 7.5);
        assert_rows_eq(
            session
                .execute("SELECT count(*) AS n FROM pushdown_note", &[])
                .await
                .unwrap(),
            vec![vec![Value::Integer(2)]],
        );
        assert_rows_eq(
            session
                .execute(
                    "SELECT optional FROM pushdown_note WHERE id = 'a-copy'",
                    &[],
                )
                .await
                .unwrap(),
            vec![vec![Value::Jsonb(serde_json::json!({"ok": true}).into())]],
        );
        let duplicate = session.execute(
            "INSERT INTO pushdown_note (id,kind,title,score) (SELECT 'new','todo','First',1 UNION ALL SELECT 'new','todo','Second',2) ON CONFLICT (id) DO NOTHING RETURNING id", &[]
        ).await.expect("DO NOTHING should suppress statement duplicates");
        assert_eq!(duplicate.rows().len(), 1);
        session.execute(
            "INSERT INTO pushdown_note (id,kind,title,score) (SELECT 'other','todo','First',1 UNION ALL SELECT 'other','todo','Second',2) ON CONFLICT (id) DO UPDATE SET score = excluded.score", &[]
        ).await.expect_err("DO UPDATE must reject duplicate statement identities");
        assert_rows_eq(
            session
                .execute(
                    "SELECT count(*) AS n FROM pushdown_note WHERE id = 'other'",
                    &[],
                )
                .await
                .unwrap(),
            vec![vec![Value::Integer(0)]],
        );
    }
);

simulation_test!(
    registered_insert_select_late_failure_is_atomic,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        register_pushdown_note_schema(&session).await;
        session.execute("INSERT INTO pushdown_note (id,kind,title,score) SELECT 'a','todo','First',1 UNION ALL SELECT 'b','todo',NULL,2", &[])
            .await.expect_err("a late invalid row must fail the complete statement");
        assert_rows_eq(
            session
                .execute("SELECT count(*) AS n FROM pushdown_note", &[])
                .await
                .unwrap(),
            vec![vec![Value::Integer(0)]],
        );
        session
            .execute(
                "INSERT INTO pushdown_note (id,kind,title,score) SELECT 'a','todo','First',1",
                &[],
            )
            .await
            .unwrap();
    }
);
