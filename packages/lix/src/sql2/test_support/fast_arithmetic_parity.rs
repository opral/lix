//! Parity and admission tests for arithmetic assignment on the fast entity
//! writer.
//!
//! # ⚠️ UNVERIFIED — THIS FILE HAS NEVER BEEN COMPILED OR RUN
//!
//! It was written and committed after the last build on this branch. Read it as
//! a **specification of the cases that must pass** before the widening in
//! `bound_public_write.rs` can land — never as evidence that they do. It may not
//! even compile. Nothing on this branch may be merged until this file builds and
//! passes: admission is decided statically from the plan while operand types are
//! dynamic, and there is **no runtime fallback**, so a wrongly-admitted
//! expression computes a wrong value or errors instead of re-routing.
//!
//! Arithmetic assignment is the one entity write shape both writers accept:
//! `write_target_table_name` admits an entity write to the DataFusion writer
//! only when the statement carries a LIKE predicate or a binary expression.
//! Every case below therefore runs the *same* statement under
//! `ForceFast` and under `ForceDataFusion` on two identical engines and
//! compares the resulting row and the Ok/Err signature.

#[cfg(test)]
mod tests {
    use crate::engine::Engine;
    use crate::session::SessionContext;
    use crate::sql2::exec::bound_public_write::take_fast_binary_arithmetic_evaluations;
    use crate::sql2::{WriteExecutorMode, WriteExecutorPath};
    use crate::storage_adapter::Memory;
    use crate::Value;

    const SCHEMA_SQL: &str = "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
         VALUES (\
         lix_json('{\"x-lix-key\":\"arith_probe\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"c\":{\"type\":\"string\"},\"k\":{\"type\":\"integer\"},\"r\":{\"type\":\"number\"}},\"required\":[\"id\",\"c\",\"k\"],\"additionalProperties\":false}'),\
         false,false)";

    async fn open_engine() -> Engine {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("parity storage should initialize");
        Engine::new(storage)
            .await
            .expect("parity engine should open")
    }

    async fn seeded_session(seed_sql: &str) -> SessionContext {
        let engine = open_engine().await;
        let session = engine.open_session().await.expect("session should open");
        session
            .execute(SCHEMA_SQL, &[])
            .await
            .expect("schema registration should succeed");
        session
            .execute(seed_sql, &[])
            .await
            .expect("seed insert should succeed");
        // The engine is dropped at the end of this function; the session keeps
        // its own storage handle, which is what every other sql2 test relies on.
        session
    }

    #[derive(Debug, PartialEq)]
    enum Outcome {
        Ok { rows: Vec<Vec<Value>>, path: Option<WriteExecutorPath> },
        Err { code: String },
    }

    async fn run(seed_sql: &str, sql: &str, params: &[Value], mode: WriteExecutorMode) -> Outcome {
        let session = seeded_session(seed_sql).await;
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should open");
        let executed =
            Box::pin(transaction.execute_with_write_executor_mode_and_trace(sql, params, mode))
                .await;
        match executed {
            Ok((_result, path)) => {
                transaction.commit().await.expect("commit should succeed");
                let rows = session
                    .execute("SELECT k, r, c FROM arith_probe ORDER BY id", &[])
                    .await
                    .expect("probe select should succeed")
                    .rows;
                Outcome::Ok { rows, path }
            }
            Err(error) => {
                transaction.rollback().await.expect("rollback should succeed");
                Outcome::Err { code: error.code }
            }
        }
    }

    async fn assert_route_parity(seed_sql: &str, sql: &str, params: &[Value], expected: WriteExecutorPath) {
        let fast_mode = match expected {
            WriteExecutorPath::Fast => WriteExecutorMode::ForceFast,
            WriteExecutorPath::DataFusion => WriteExecutorMode::ForceDataFusion,
        };
        let auto = run(seed_sql, sql, params, WriteExecutorMode::Auto).await;
        if let Outcome::Ok { path, .. } = &auto {
            assert_eq!(*path, Some(expected), "route for `{sql}`");
        }
        let reference = run(seed_sql, sql, params, WriteExecutorMode::ForceDataFusion).await;
        let candidate = run(seed_sql, sql, params, fast_mode).await;
        assert_eq!(
            strip_path(candidate),
            strip_path(reference),
            "arithmetic write `{sql}` diverged between the two writers"
        );
    }

    fn strip_path(outcome: Outcome) -> Outcome {
        match outcome {
            Outcome::Ok { rows, .. } => Outcome::Ok { rows, path: None },
            other => other,
        }
    }

    const SEED_INT: &str =
        "INSERT INTO arith_probe (id, c, k, r) VALUES ('a', 'seed', 7, 2.5)";
    const SEED_MAX: &str = "INSERT INTO arith_probe (id, c, k, r) VALUES ('a', 'seed', 9223372036854775807, 1.0)";
    const SEED_NULL_R: &str = "INSERT INTO arith_probe (id, c, k) VALUES ('a', 'seed', 7)";

    #[tokio::test]
    async fn admitted_arithmetic_shapes_match_the_datafusion_writer() {
        for (seed, sql) in [
            (SEED_INT, "UPDATE arith_probe SET k = k + 1 WHERE id = 'a'"),
            (SEED_INT, "UPDATE arith_probe SET k = k - 2 WHERE id = 'a'"),
            (SEED_INT, "UPDATE arith_probe SET k = k * 3 WHERE id = 'a'"),
            (SEED_INT, "UPDATE arith_probe SET k = k / 2 WHERE id = 'a'"),
            (SEED_INT, "UPDATE arith_probe SET k = k % 3 WHERE id = 'a'"),
            (SEED_INT, "UPDATE arith_probe SET k = 1 + k WHERE id = 'a'"),
            (SEED_INT, "UPDATE arith_probe SET k = 10 - k WHERE id = 'a'"),
            (SEED_INT, "UPDATE arith_probe SET r = r * 2 WHERE id = 'a'"),
            (SEED_INT, "UPDATE arith_probe SET r = r + 1 WHERE id = 'a'"),
            (SEED_INT, "UPDATE arith_probe SET k = k + 0 WHERE id = 'a'"),
            // Integer overflow: both writers use the wrapping kernel.
            (SEED_MAX, "UPDATE arith_probe SET k = k + 1 WHERE id = 'a'"),
            // Division by zero: both writers use the checked kernel.
            (SEED_INT, "UPDATE arith_probe SET k = k / 0 WHERE id = 'a'"),
            // NULL propagation.
            (SEED_NULL_R, "UPDATE arith_probe SET r = r + 1 WHERE id = 'a'"),
            // Non-numeric operand: both writers must refuse.
            (SEED_INT, "UPDATE arith_probe SET c = c + 1 WHERE id = 'a'"),
            (SEED_INT, "UPDATE arith_probe SET k = k + 'x' WHERE id = 'a'"),
            // Zero matched rows.
            (SEED_INT, "UPDATE arith_probe SET k = k + 1 WHERE id = 'missing'"),
        ] {
            Box::pin(assert_route_parity(seed, sql, &[], WriteExecutorPath::Fast)).await;
        }
    }

    #[tokio::test]
    async fn parameterized_arithmetic_matches_the_datafusion_writer() {
        Box::pin(assert_route_parity(
            SEED_INT,
            "UPDATE arith_probe SET k = k + $1 WHERE id = 'a'",
            &[Value::Integer(5)],
            WriteExecutorPath::Fast,
        ))
        .await;
    }

    #[tokio::test]
    async fn compound_arithmetic_stays_on_the_datafusion_writer() {
        for sql in [
            "UPDATE arith_probe SET k = (k + 1) * 2 WHERE id = 'a'",
            "UPDATE arith_probe SET k = k + 1 + 1 WHERE id = 'a'",
            "UPDATE arith_probe SET k = CAST(k AS BIGINT) + 1 WHERE id = 'a'",
        ] {
            let auto = run(SEED_INT, sql, &[], WriteExecutorMode::Auto).await;
            match auto {
                Outcome::Ok { path, .. } => assert_eq!(
                    path,
                    Some(WriteExecutorPath::DataFusion),
                    "compound arithmetic `{sql}` must not be admitted to the fast writer"
                ),
                Outcome::Err { code } => {
                    panic!("compound arithmetic `{sql}` failed unexpectedly: {code}")
                }
            }
        }
    }

    /// Engagement check. The counter sits inside the fast writer's arithmetic
    /// arm, one layer below the router that the route assertion reads, so a hit
    /// proves the new evaluation code ran and not merely that the router chose
    /// the fast path.
    #[tokio::test]
    async fn fast_arithmetic_counter_records_hits_and_misses() {
        let _ = take_fast_binary_arithmetic_evaluations();

        let session = seeded_session(SEED_INT).await;
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should open");
        let (_result, path) = Box::pin(transaction.execute_with_write_executor_mode_and_trace(
            "UPDATE arith_probe SET k = k + 1 WHERE id = 'a'",
            &[],
            WriteExecutorMode::Auto,
        ))
        .await
        .expect("arithmetic update should execute");
        transaction.commit().await.expect("commit should succeed");
        assert_eq!(path, Some(WriteExecutorPath::Fast));
        assert!(
            take_fast_binary_arithmetic_evaluations() >= 1,
            "the fast arithmetic arm must have evaluated at least once"
        );
        let rows = session
            .execute("SELECT k FROM arith_probe WHERE id = 'a'", &[])
            .await
            .expect("probe select should succeed")
            .rows;
        assert_eq!(rows, vec![vec![Value::Integer(8)]]);

        // Inversion: a compound expression is refused by the admission guard,
        // so the arithmetic arm must not run at all.
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should open");
        let (_result, path) = Box::pin(transaction.execute_with_write_executor_mode_and_trace(
            "UPDATE arith_probe SET k = (k + 1) * 2 WHERE id = 'a'",
            &[],
            WriteExecutorMode::Auto,
        ))
        .await
        .expect("compound arithmetic update should execute");
        transaction.commit().await.expect("commit should succeed");
        assert_eq!(path, Some(WriteExecutorPath::DataFusion));
        assert_eq!(
            take_fast_binary_arithmetic_evaluations(),
            0,
            "the fast arithmetic arm must not run for a refused shape"
        );
        let rows = session
            .execute("SELECT k FROM arith_probe WHERE id = 'a'", &[])
            .await
            .expect("probe select should succeed")
            .rows;
        assert_eq!(rows, vec![vec![Value::Integer(18)]]);
    }
}
