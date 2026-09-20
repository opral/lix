use lix::ExecuteResult;
use lix::Value;

use super::assert_rows_eq;

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
