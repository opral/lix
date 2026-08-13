//! The hot-index range seek must actually seek, and must refuse when it cannot.
//!
//! This lives in its own `[[test]]` target because `storage_bench` counters are
//! **process-global**. In the shared suite a concurrent test can only inflate
//! them — harmless for a `>=` assertion, but fatal for the two assertions that
//! matter here: "resolved fewer candidates than the collection" and "did not
//! engage at all" are both *upper* bounds, which inflation pushes toward a
//! spurious failure. A dedicated target is its own process, and both arms run
//! sequentially inside one `#[test]` so that even libtest's own parallelism
//! cannot interleave them.
//!
//! Without these assertions the 144-pair bound differential in the integration
//! suite is vacuous: it would pass identically against a silently-refusing
//! seek, proving nothing about the seek while looking like thorough coverage.

use std::future::Future;

use lix::integration::{Engine, SessionContext};
use lix::{Memory, Value};

/// Building and optimizing real DataFusion plans recurses per plan node and
/// overflows libtest's 2 MiB worker stack in the `test` profile.
const RANGE_PROBE_TEST_STACK_SIZE: usize = 32 * 1024 * 1024;

/// Ordinals seeded into both fixtures. Spans zero and negatives so the
/// order-preserving sign flip is exercised, not just the positive half.
const ORDINALS: [i64; 10] = [-3, -2, -1, 0, 1, 2, 3, 4, 5, 6];

fn run_on_sized_stack<Body, Fut>(name: &str, body: Body)
where
    Body: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()>,
{
    std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(RANGE_PROBE_TEST_STACK_SIZE)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build range-probe test runtime")
                .block_on(body());
        })
        .expect("spawn range-probe test thread")
        .join()
        .expect("range-probe test body panicked");
}

#[test]
fn range_seek_engages_on_a_declared_column_and_refuses_on_an_undeclared_one() {
    run_on_sized_stack("hot_index_range_probe", || async {
        // ---- hit: the column declares `x-lix-unique`, so entries exist -----
        let session = seeded_session("range_seek_note", true).await;
        // Drain what seeding wrote — uniqueness validation probes the same
        // index — so the census below describes the read, not the writes.
        let _ = lix::storage_bench::take_hot_index_probe_census();
        let answered = ordinal_range_ids(&session, "range_seek_note", 2, 4).await;
        let hit = lix::storage_bench::take_hot_index_probe_census();

        assert_eq!(
            answered,
            vec!["n5".to_string(), "n6".to_string(), "n7".to_string()],
            "ordinals 2,3,4 are the sixth through eighth rows of the fixture"
        );
        assert!(
            hit.range_probes_engaged >= 1,
            "the range seek should engage on a declared column; census={hit:?}"
        );
        // A closed interval over 3 of 10 values that resolves all 10 is a walk
        // wearing a counter, not a seek.
        assert!(
            hit.range_probe_candidates < ORDINALS.len() as u64,
            "seek over 3 of 10 values resolved {} candidates; census={hit:?}",
            hit.range_probe_candidates
        );

        // ---- miss: no declaration, so there is no index to seek ------------
        let session = seeded_session("range_scan_note", false).await;
        let _ = lix::storage_bench::take_hot_index_probe_census();
        let answered = ordinal_range_ids(&session, "range_scan_note", 2, 4).await;
        let miss = lix::storage_bench::take_hot_index_probe_census();

        assert_eq!(
            answered,
            vec!["n5".to_string(), "n6".to_string(), "n7".to_string()],
            "refusing to seek must not change the answer"
        );
        // Exact zero is only safe because this is a dedicated process and the
        // two arms are sequential.
        assert_eq!(
            miss.range_probes_engaged, 0,
            "an undeclared column has no index entries and must not seek; census={miss:?}"
        );
    });
}

async fn ordinal_range_ids(
    session: &SessionContext<Memory>,
    table: &str,
    lower: i64,
    upper: i64,
) -> Vec<String> {
    let result = session
        .execute(
            &format!(
                "SELECT id FROM {table} WHERE ordinal BETWEEN {lower} AND {upper} ORDER BY id"
            ),
            &[],
        )
        .await
        .expect("range query should succeed");
    result
        .rows()
        .iter()
        .filter_map(|row| match row.values().first() {
            Some(Value::Text(value)) => Some(value.clone()),
            _ => None,
        })
        .collect()
}

async fn seeded_session(schema_key: &str, declare_unique: bool) -> SessionContext<Memory> {
    let storage = Memory::default();
    Engine::initialize(storage.clone())
        .await
        .expect("initialize fixture");
    let engine = Engine::new(storage).await.expect("open engine");
    let session = engine.open_session().await.expect("open session");

    let unique = if declare_unique {
        r#""x-lix-unique":[["/ordinal"]],"#
    } else {
        ""
    };
    let schema = format!(
        r#"{{"x-lix-key":"{schema_key}","x-lix-primary-key":["/id"],{unique}"type":"object","properties":{{"id":{{"type":"string"}},"ordinal":{{"type":"integer"}}}},"required":["id","ordinal"],"additionalProperties":false}}"#
    );
    session
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
            &[Value::Text(schema)],
        )
        .await
        .expect("register schema");

    for (index, ordinal) in ORDINALS.iter().enumerate() {
        session
            .execute(
                &format!("INSERT INTO {schema_key} (id, ordinal) VALUES ($1, {ordinal})"),
                &[Value::Text(format!("n{index}"))],
            )
            .await
            .expect("insert row");
    }
    session
}
