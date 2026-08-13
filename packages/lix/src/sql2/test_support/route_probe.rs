//! Route census + route-cost probe for sql2 write execution.
//!
//! `write_route_census` is a cheap deterministic assertion of which executor
//! path each canonical statement shape takes. `write_route_cost` times the two
//! routes on the *same* statement shape by forcing the executor mode, so the
//! only difference between the arms is the route itself.
//!
//! Run with:
//! `cargo test -p lix --lib sql2::test_support::route_probe -- --ignored --nocapture --test-threads=1`

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::engine::Engine;
    use crate::session::SessionContext;
    use crate::sql2::{WriteExecutorMode, WriteExecutorPath};
    use crate::storage_adapter::Memory;
    use crate::Value;

    const SCHEMA_SQL: &str = "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
         VALUES (\
         lix_json('{\"x-lix-key\":\"route_probe\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"c\":{\"type\":\"string\"},\"k\":{\"type\":\"integer\"}},\"required\":[\"id\",\"c\",\"k\"],\"additionalProperties\":false}'),\
         false,false)";

    async fn open_engine() -> Engine {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("probe storage should initialize");
        Engine::new(storage).await.expect("probe engine should open")
    }

    fn row_id(index: usize) -> String {
        format!("row-{index:06}")
    }

    fn params_for(shape: &str, index: usize) -> Vec<Value> {
        match shape {
            "update-literal" => vec![Value::Text(format!("v{index}")), Value::Text(row_id(index))],
            "update-arith" => vec![Value::Text(row_id(index))],
            "delete" => vec![Value::Text(row_id(index))],
            "insert" => vec![
                Value::Text(row_id(index)),
                Value::Text("seed".to_string()),
                Value::Integer(index as i64),
            ],
            other => panic!("unknown shape {other}"),
        }
    }

    fn shapes() -> Vec<(&'static str, &'static str)> {
        vec![
            (
                "update-literal",
                "UPDATE route_probe SET c = $1 WHERE id = $2",
            ),
            (
                "update-arith",
                "UPDATE route_probe SET k = k + 1 WHERE id = $1",
            ),
            ("delete", "DELETE FROM route_probe WHERE id = $1"),
            (
                "insert",
                "INSERT INTO route_probe (id, c, k) VALUES ($1, $2, $3)",
            ),
        ]
    }

    async fn seed(session: &SessionContext, count: usize) {
        session
            .execute(SCHEMA_SQL, &[])
            .await
            .expect("schema registration should succeed");
        for index in 0..count {
            session
                .execute(
                    "INSERT INTO route_probe (id, c, k) VALUES ($1, 'seed', 0)",
                    &[Value::Text(row_id(index))],
                )
                .await
                .expect("seed insert should succeed");
        }
    }

    #[tokio::test]
    #[ignore = "probe"]
    async fn write_route_census() {
        let engine = open_engine().await;
        let session = engine.open_session().await.expect("session should open");
        seed(&session, 8).await;

        for (name, sql) in shapes() {
            let mut transaction = session
                .begin_transaction()
                .await
                .expect("probe transaction should open");
            let index = if name == "insert" { 5000 } else { 0 };
            let params = params_for(name, index);
            let traced = Box::pin(transaction.execute_with_write_executor_mode_and_trace(
                sql,
                &params,
                WriteExecutorMode::Auto,
            ))
            .await;
            match traced {
                Ok((_result, path)) => println!("ROUTE shape={name} path={path:?} sql={sql}"),
                Err(error) => println!("ROUTE shape={name} ERROR={error:?} sql={sql}"),
            }
            transaction.rollback().await.expect("rollback should work");
        }
    }

    async fn time_shape(
        session: &SessionContext,
        sql: &str,
        shape: &str,
        mode: WriteExecutorMode,
        reps: usize,
        offset: usize,
    ) -> (f64, Option<WriteExecutorPath>) {
        let mut path = None;
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("probe transaction should open");
        let start = Instant::now();
        for rep in 0..reps {
            let params = params_for(shape, offset + rep);
            let (_result, observed) =
                Box::pin(transaction.execute_with_write_executor_mode_and_trace(sql, &params, mode))
                    .await
                    .unwrap_or_else(|error| {
                        panic!("shape {shape} under {mode:?} failed: {error:?}")
                    });
            path = observed;
        }
        let elapsed = start.elapsed();
        transaction.commit().await.expect("commit should work");
        (elapsed.as_secs_f64() * 1e6 / reps as f64, path)
    }

    #[tokio::test]
    #[ignore = "probe"]
    async fn write_route_cost() {
        let reps: usize = std::env::var("LIX_ROUTE_PROBE_REPS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(100);
        let rounds: usize = std::env::var("LIX_ROUTE_PROBE_ROUNDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(5);
        let shape = std::env::var("LIX_ROUTE_PROBE_SHAPE")
            .unwrap_or_else(|_| "update-arith".to_string());
        let modes: Vec<WriteExecutorMode> = std::env::var("LIX_ROUTE_PROBE_MODES")
            .unwrap_or_else(|_| "auto".to_string())
            .split(',')
            .map(|name| match name.trim() {
                "fast" => WriteExecutorMode::ForceFast,
                "datafusion" => WriteExecutorMode::ForceDataFusion,
                "auto" => WriteExecutorMode::Auto,
                other => panic!("unknown probe mode {other}"),
            })
            .collect();
        let sql = shapes()
            .into_iter()
            .find(|(name, _)| *name == shape)
            .map(|(_, sql)| sql)
            .expect("probe shape should exist");

        let engine = open_engine().await;
        let session = engine.open_session().await.expect("session should open");
        seed(&session, reps * (rounds * modes.len() + modes.len()) + 16).await;

        let mut offset = 0usize;
        for mode in modes.iter().copied() {
            let warm = reps.min(20);
            let _ = time_shape(&session, sql, &shape, mode, warm, offset).await;
            offset += warm;
        }

        for round in 0..rounds {
            let mut order = modes.clone();
            if round % 2 == 1 {
                order.reverse();
            }
            for mode in order {
                let (us, path) = time_shape(&session, sql, &shape, mode, reps, offset).await;
                offset += reps;
                println!(
                    "COST shape={shape} round={round} mode={mode:?} path={path:?} us_per_stmt={us:.1}"
                );
            }
        }
    }
}
