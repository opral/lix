//! The hot-index range seek must actually seek, must read a number of entries
//! set by its **answer** rather than by the collection, and must refuse when it
//! has no index to seek.
//!
//! This lives in its own `[[test]]` target because `storage_bench` counters are
//! **process-global**. In the shared suite a concurrent test can only inflate
//! them — harmless for a `>=` assertion, but fatal for the assertions that
//! matter here, which are *upper* bounds that inflation pushes toward a
//! spurious failure. A dedicated target is its own process; and every arm runs
//! sequentially inside one `#[test]` because libtest runs `#[test]` functions
//! within a single binary concurrently, so a dedicated target alone is
//! necessary but not sufficient.
//!
//! Without the engagement assertions the 144-pair bound differential in the
//! integration suite is vacuous: it passes identically against a
//! silently-refusing seek, proving nothing about the seek while looking like
//! thorough coverage. That is not hypothetical — it is what happened during
//! development, when the direct-route mitigation and the seek cancelled each
//! other and every correctness test stayed green.

// The whole target reads `storage_bench` counters, which exist only under
// `storage-benches`. Without this the target breaks `cargo check -p lix --tests`
// while `--all-features` stays green. Same line as `preimage_route_census`.
#![cfg(feature = "storage-benches")]

use std::future::Future;

use lix::integration::{Engine, SessionContext};
use lix::{Memory, Value};

/// Building and optimizing real DataFusion plans recurses per plan node and
/// overflows libtest's 2 MiB worker stack in the `test` profile.
const RANGE_PROBE_TEST_STACK_SIZE: usize = 32 * 1024 * 1024;

/// The measured query selects a closed interval of three ordinals.
const ANSWER_ROWS: u64 = 3;

/// Collection sizes the same three-row answer is read from. The point of the
/// pair is the *slope*: a seek's candidate count is set by its answer, so it
/// must not move when the collection grows 20x. A scan's would grow with it.
const COLLECTION_SIZES: [usize; 2] = [10, 200];

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
fn range_seek_reads_candidates_set_by_its_answer_and_refuses_without_an_index() {
    run_on_sized_stack("hot_index_range_probe", || async {
        // ---- the access-path claim, as a curve rather than a point ---------
        let mut candidates_by_size = Vec::new();
        for rows in COLLECTION_SIZES {
            let session = seeded_session("range_seek_note", true, rows).await;
            // Seeding a declared-unique collection probes this same index
            // during uniqueness validation, so drain immediately before the
            // measured read or the census describes the writes too.
            let _ = lix::storage_bench::take_hot_index_probe_census();
            let answered = ordinal_range_ids(&session, "range_seek_note", 2, 4).await;
            let census = lix::storage_bench::take_hot_index_probe_census();

            assert_eq!(
                answered,
                vec!["n5".to_string(), "n6".to_string(), "n7".to_string()],
                "ordinals 2,3,4 are the sixth through eighth rows at {rows} rows"
            );
            assert!(
                census.range_probes_engaged >= 1,
                "the range seek should engage on a declared column at {rows} rows; \
                 census={census:?}"
            );
            candidates_by_size.push(census.range_probe_candidates);
        }

        // A seek reads entries proportional to its answer. A scan would read
        // the collection, so this is the assertion that distinguishes them.
        assert_eq!(
            candidates_by_size,
            vec![ANSWER_ROWS, ANSWER_ROWS],
            "candidates must be set by the {ANSWER_ROWS}-row answer, not by the \
             collection: {candidates_by_size:?} at sizes {COLLECTION_SIZES:?}"
        );

        // ---- the refusal: no declaration means no index to seek ------------
        let session = seeded_session("range_scan_note", false, COLLECTION_SIZES[0]).await;
        let _ = lix::storage_bench::take_hot_index_probe_census();
        let answered = ordinal_range_ids(&session, "range_scan_note", 2, 4).await;
        let miss = lix::storage_bench::take_hot_index_probe_census();

        assert_eq!(
            answered,
            vec!["n5".to_string(), "n6".to_string(), "n7".to_string()],
            "refusing to seek must not change the answer"
        );
        // Exact zero is only defensible because this is a dedicated process
        // and every arm above it ran sequentially.
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

async fn seeded_session(
    schema_key: &str,
    declare_unique: bool,
    rows: usize,
) -> SessionContext<Memory> {
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

    // Ordinals start at -3 so the range spans zero and negatives, exercising
    // the order-preserving sign flip rather than only the positive half.
    for index in 0..rows {
        let ordinal = index as i64 - 3;
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
