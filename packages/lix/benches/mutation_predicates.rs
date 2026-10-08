//! Cross-table sync DELETE versus the plain-list and two-step controls.
//! Run with `cargo bench -p lix --features storage-benches --bench mutation_predicates`.
//! Fixture creation and transaction admission/rollback are outside the timer.
//! Every case deletes the same ten rows and verifies the affected count.
use lix::{Value, open_lix, storage_bench::measure_checkpoint_foreground};
use std::time::Instant;

fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}

async fn run() {
    let sizes = std::env::var("LIX_MUTATION_PROFILE_ROWS").unwrap_or_else(|_| "1000,10000".into());
    let rounds: usize = std::env::var("LIX_MUTATION_PROFILE_ROUNDS")
        .unwrap_or_else(|_| "9".into())
        .parse()
        .unwrap();
    assert!(rounds > 0);
    for total in sizes.split(',').map(|n| n.parse::<usize>().unwrap()) {
        assert!(total >= 10);
        let lix = open_lix().await.unwrap();
        for (key, columns) in [
            (
                "perf_message",
                serde_json::json!([
                {"name":"id","type":"text","nullable":false},
                {"name":"bundle_id","type":"text","nullable":false}]),
            ),
            (
                "perf_variant",
                serde_json::json!([
                {"name":"id","type":"text","nullable":false},
                {"name":"message_id","type":"text","nullable":false}]),
            ),
        ] {
            let schema = serde_json::json!({"$schema":"https://lix.dev/schema-v1.json",
                "key":key,"columns":columns,"primary_key":["id"]});
            lix.execute(
                "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        }
        for start in (0..total).step_by(1000) {
            let end = (start + 1000).min(total);
            let messages = (start..end)
                .map(|n| format!("('m{n}', '{}')", if n < 10 { "target" } else { "other" }))
                .collect::<Vec<_>>()
                .join(",");
            let variants = (start..end)
                .map(|n| format!("('v{n}', 'm{n}')"))
                .collect::<Vec<_>>()
                .join(",");
            lix.execute(
                &format!("INSERT INTO perf_message(id,bundle_id) VALUES {messages}"),
                &[],
            )
            .await
            .unwrap();
            lix.execute(
                &format!("INSERT INTO perf_variant(id,message_id) VALUES {variants}"),
                &[],
            )
            .await
            .unwrap();
        }
        let params = (0..10)
            .map(|n| Value::Text(format!("m{n}")))
            .collect::<Vec<_>>();
        let list = (1..=10)
            .map(|n| format!("${n}"))
            .collect::<Vec<_>>()
            .join(",");
        let plain = format!("DELETE FROM perf_variant WHERE message_id IN ({list})");
        let mut samples = [Vec::new(), Vec::new(), Vec::new()];
        for round in 0..=rounds {
            for offset in 0..3 {
                let case = (round + offset) % 3;
                let mut tx = lix.begin_transaction().await.unwrap();
                let started = Instant::now();
                let (result, work) = measure_checkpoint_foreground(async {
                    match case {
                        0 => tx.execute(&plain, &params).await,
                        1 => tx.execute("DELETE FROM perf_variant WHERE message_id IN (SELECT id FROM perf_message WHERE bundle_id IN ($1))", &[Value::Text("target".into())]).await,
                        _ => {
                            let selected = tx.execute("SELECT id FROM perf_message WHERE bundle_id IN ($1)", &[Value::Text("target".into())]).await?;
                            let ids = selected.rows().iter().map(|row| row.get::<String>("id").map(Value::Text)).collect::<Result<Vec<_>, _>>()?;
                            assert_eq!(ids.len(), 10);
                            tx.execute(&plain, &ids).await
                        }
                    }
                }).await;
                let elapsed = started.elapsed();
                assert_eq!(result.unwrap().rows_affected(), 10);
                let remaining = tx
                    .execute("SELECT count(*) AS n FROM perf_variant", &[])
                    .await
                    .unwrap();
                assert_eq!(
                    remaining.rows()[0].get::<i64>("n").unwrap(),
                    (total - 10) as i64
                );
                let targets = tx.execute("SELECT id FROM perf_variant WHERE message_id IN (SELECT id FROM perf_message WHERE bundle_id = 'target')", &[]).await.unwrap();
                assert!(targets.is_empty(), "the selected ten rows must be deleted");
                tx.rollback().await.unwrap();
                if round > 0 {
                    samples[case].push(elapsed.as_nanos());
                    if round == rounds {
                        println!(
                            "rows={total} case={} storage={work:?}",
                            ["plain", "subquery", "two_step"][case]
                        );
                    }
                }
            }
        }
        for (case, times) in samples.iter_mut().enumerate() {
            times.sort_unstable();
            println!(
                "rows={total} case={} rounds={rounds} p50_us={} p90_us={}",
                ["plain", "subquery", "two_step"][case],
                times[times.len() / 2] / 1000,
                times[(times.len() * 9 / 10).min(times.len() - 1)] / 1000
            );
        }
    }
}
