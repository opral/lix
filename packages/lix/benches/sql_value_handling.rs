//! Reproducible SQL value-handling profile. Fixture/seed time is excluded.
//! Run with `cargo bench -p lix --bench sql_value_handling`.
use lix::storage::Memory;
use lix::{Value, open_lix};
use std::time::Instant;

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let rows = 500;
        let mut samples: [Vec<u128>; 4] = std::array::from_fn(|_| Vec::new());
        for round in 0..10 {
            for offset in 0..4 {
                let case = (round + offset) % 4;
                let db = open_lix().with_storage(Memory::new()).await.unwrap();
                db.execute("INSERT INTO lix_registered_schema (schema_key, value) VALUES ('typed_profile', CAST($1 AS JSONB))", &[Value::Text(r#"{"$schema":"https://lix.dev/schema-v1.json","key":"typed_profile","columns":[{"name":"id","type":"text","nullable":false},{"name":"n","type":"int8","nullable":false},{"name":"payload","type":"jsonb","nullable":true},{"name":"stamp","type":"timestamptz","nullable":false}],"primary_key":["id"]}"#.into())]).await.unwrap();
                let values = (0..rows).map(|i| format!("('{i}', 7, '{{\"a\":1}}'::jsonb, '2025-01-02T03:04:05.123456Z')")).collect::<Vec<_>>().join(",");
                db.execute(&format!("INSERT INTO typed_profile (id,n,payload,stamp) VALUES {values}"), &[]).await.unwrap();
                let sql = match case {
                    0 => "UPDATE typed_profile SET n = $1, payload = $2",
                    1 => "UPDATE typed_profile SET n = $1, payload = $2 RETURNING id, n, payload, stamp",
                    2 => "UPDATE typed_profile SET n = $1, payload = $2 WHERE id LIKE '%'",
                    _ => "UPDATE typed_profile SET n = $1 WHERE stamp = $2",
                };
                let params = if case == 3 { vec![Value::Integer(9), Value::Text("2025-01-02T03:04:05.123456Z".into())] }
                    else { vec![Value::Integer(9), Value::Jsonb(lix::Json::parse(r#"{"b":2}"#).unwrap())] };
                let start = Instant::now();
                let result = db.execute(sql, &params).await.unwrap();
                let elapsed = start.elapsed().as_nanos();
                assert_eq!(result.rows_affected(), rows);
                std::hint::black_box(result);
                if round > 0 { samples[case].push(elapsed); }
            }
        }
        for (case, mut values) in samples.into_iter().enumerate() {
            values.sort();
            println!("case={case} rows={rows} samples={} median_ns={} min_ns={} max_ns={}", values.len(),values[values.len()/2],values[0],values[values.len()-1]);
        }
    });
}
