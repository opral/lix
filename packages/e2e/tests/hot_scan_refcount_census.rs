//! Deterministic census of refcounted-buffer handle clones on the HOT scan
//! read path.
//!
//! **Why a count and not a nanosecond figure.** A rate only travels within a
//! host class — this fleet spans a 5.4x gap between its slowest and fastest
//! machines, so "585 ns per scanned row" is a fact about one box. The number
//! of times the scan duplicates a refcounted buffer handle is identical
//! everywhere, so it is the half of a "we stopped cloning" claim that can be
//! re-run anywhere and compared across agents and rounds.
//!
//! **Own process on purpose.** The counters in `lix::storage_bench` are
//! process-global; an exact assertion on them inside the shared suite reads
//! whatever every other concurrently running test happened to scan. A
//! dedicated `[[test]]` target is its own process, which is what makes the
//! exact per-row equalities below legitimate rather than flaky.
//!
//! The census counters sit on the clone expressions themselves, not on the
//! functions containing them, so a site that stops cloning stops counting.

use lix::storage_bench::take_hot_scan_refcount_census;
use lix::{Value, open_lix};

const ROWS: usize = 2_000;
const SCHEMA_KEY: &str = "hot_scan_refcount_census_probe";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_scan_clones_a_bounded_number_of_shared_handles_per_row() {
    let lix = open_lix().await.expect("open in-memory lix");

    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": SCHEMA_KEY,
        "columns": [
            { "name": "id", "type": "text", "nullable": false },
            { "name": "ordinal", "type": "float8", "nullable": false },
            { "name": "lane", "type": "text", "nullable": false },
        ],
        "primary_key": ["id"],
    });
    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
        &[Value::Text(schema.to_string())],
    )
    .await
    .expect("register census schema");

    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(500) {
        let mut sql = format!("INSERT INTO {SCHEMA_KEY} (id, ordinal, lane) VALUES ");
        let mut params = Vec::new();
        for (offset, ordinal) in chunk.iter().enumerate() {
            if offset > 0 {
                sql.push(',');
            }
            let base = offset * 3;
            sql.push_str(&format!("(${}, ${}, ${})", base + 1, base + 2, base + 3));
            params.push(Value::Text(format!("/census/{ordinal:09}")));
            params.push(Value::Integer(i64::try_from(*ordinal).expect("fits i64")));
            params.push(Value::Text(format!("lane-{:02}", ordinal % 8)));
        }
        let result = lix.execute(&sql, &params).await.expect("seed census rows");
        assert!(
            result.rows_affected() > 0,
            "a seed statement that affects no rows produces the same zero counters as a route that never ran"
        );
    }
    lix.create_checkpoint()
        .await
        .expect("drive the census fixture to packed-base steady state");

    // Discard everything the seed and the checkpoint decoded.
    let _ = take_hot_scan_refcount_census();

    // A predicate on a non-key column, so the entity provider parses each
    // row's snapshot and filters it — the shape that used to rebuild the whole
    // materialized batch row by row.
    let result = lix
        .execute(
            &format!(
                "SELECT id, ordinal, lane FROM {SCHEMA_KEY} WHERE ordinal >= $1 AND ordinal < $2"
            ),
            &[
                Value::Integer(0),
                Value::Integer(i64::try_from(ROWS).expect("fits i64")),
            ],
        )
        .await
        .expect("scan census rows");
    assert_eq!(
        result.rows().len(),
        ROWS,
        "the census must observe a full scan"
    );

    let (rows_decoded, key_clones, value_clones, row_clones) = take_hot_scan_refcount_census();

    // The route ran at all. A zero here is indistinguishable from a census
    // that never fired, which is why it is asserted before any ratio.
    assert!(
        rows_decoded >= ROWS as u64,
        "hot scan decoded {rows_decoded} rows for a {ROWS}-row answer; the scan route was not taken"
    );

    let per_row = |count: u64| count as f64 / rows_decoded as f64;
    println!(
        "HOT_SCAN_REFCOUNT_CENSUS rows_decoded={rows_decoded} \
         key_handle_clones={key_clones} ({:.3}/row) \
         value_handle_clones={value_clones} ({:.3}/row) \
         row_handle_clones={row_clones} ({:.3}/row) \
         total={} ({:.3}/row)",
        per_row(key_clones),
        per_row(value_clones),
        per_row(row_clones),
        key_clones + value_clones + row_clones,
        per_row(key_clones + value_clones + row_clones),
    );

    // One string primary-key component per row, cloned onto the row's own
    // physical key buffer while decoding it. This is the traffic that is still
    // standing after the in-place filter landed.
    assert_eq!(
        key_clones, rows_decoded,
        "each decoded row should duplicate exactly one handle onto its own key"
    );

    // The ceiling that actually regresses, and the reason this file exists.
    // Before materialized batches were compacted in place, each surviving row
    // was cloned into a second columnar owner twice over — once by the entity
    // provider's filter rebuild and once by the materialization path — and
    // this lane measured **5.015 batch-rebuild handle clones per decoded
    // row**. It now measures 0.043, from the handful of point-shaped reads the
    // statement still issues around the scan.
    //
    // The threshold is a tenth of a clone per row: fifty times the observed
    // value and fifty times below the pre-fix value, so it cannot be tripped
    // by fixture drift but cannot survive the rebuild coming back either.
    // It is a threshold and not an equality because these counters are
    // process-global and the surrounding statement is not the only thing in
    // the process that materializes a row.
    assert!(
        row_clones * 10 <= rows_decoded,
        "materialized batches should be compacted in place, not rebuilt row by row; \
         got {row_clones} batch-rebuild handle clones for {rows_decoded} decoded rows \
         ({:.3}/row, ceiling 0.100/row)",
        per_row(row_clones)
    );

    // Bounded, not exact: a row's inline JSON slots are snapshot content and
    // metadata, and this schema writes no metadata.
    assert!(
        value_clones <= rows_decoded,
        "got {value_clones} value handle clones for {rows_decoded} rows"
    );
}
