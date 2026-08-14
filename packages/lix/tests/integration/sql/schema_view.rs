use lix::ExecuteResult;
use lix::Value;
use serde_json::json;

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
            .execute(
                "SELECT lixcol_row_pk FROM pushdown_note WHERE kind = 'todo'",
                &[],
            )
            .await
            .expect("filter-only payload query should succeed");

        assert_rows_eq(result, vec![vec![Value::Jsonb(json!(["n1"]).into())]]);
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
            .execute("SELECT id FROM range_note WHERE weight > 0.5 ORDER BY id", &[])
            .await
            .expect("float range query should still answer");
        assert_eq!(range_note_ids(&result), vec!["n0".to_string()]);
    }
);

async fn register_range_note_schema(
    session: &crate::support::simulation_test::engine::SimSession,
) {
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
