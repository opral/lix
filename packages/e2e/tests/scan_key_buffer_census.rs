//! Deterministic census of heap buffers allocated for scanned key bytes.
//!
//! The handle-clone census in `hot_scan_refcount_census` **cannot see this
//! change**, and that is the point of a second file. Carving keys out of a page
//! arena does not alter how many times a key is cloned; it alters what each
//! clone costs. A `Bytes` produced by `copy_from_slice` is `Vec`-backed, and a
//! `Vec`-backed `Bytes` cannot be cloned without first being *promoted* — the
//! clone allocates a refcount control block and installs it with a CAS. So the
//! per-row cost was two allocations and a promotion to hand out bytes the page
//! already had in memory.
//!
//! Allocations per scanned row is the portable half of that claim: identical on
//! every machine, where the nanoseconds are not.
//!
//! Own process, like its sibling: `lix::storage_bench` counters are
//! process-global.

use lix::storage_bench::{take_hot_scan_refcount_census, take_scan_key_buffer_census};
use lix::{Value, open_lix};
use lix_storage_rocksdb::RocksDB;

const ROWS: usize = 20_000;
const SCHEMA_KEY: &str = "scan_key_buffer_census_probe";

/// One chunk holds many keys, so allocations per scanned row must land far
/// below one. The ceiling is deliberately loose — an eighth of an allocation
/// per row — because chunk occupancy depends on key width, which is a property
/// of the fixture. A per-row copy scores 1.000 and cannot pass.
const MAX_ALLOCATIONS_PER_ROW: f64 = 0.125;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scan_allocates_far_fewer_key_buffers_than_it_returns_rows() {
    let dir = tempfile::TempDir::new().expect("create census tempdir");
    let storage = RocksDB::open(dir.path().join("census.rocksdb")).expect("open census storage");
    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("open census lix");

    let schema = serde_json::json!({
        "x-lix-key": SCHEMA_KEY,
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "ordinal": { "type": "number" }
        },
        "required": ["id", "ordinal"],
        "additionalProperties": false
    });
    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
        &[Value::Text(schema.to_string())],
    )
    .await
    .expect("register census schema");

    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(1_000) {
        let mut sql = format!("INSERT INTO {SCHEMA_KEY} (id, ordinal) VALUES ");
        let mut params = Vec::new();
        for (offset, ordinal) in chunk.iter().enumerate() {
            if offset > 0 {
                sql.push(',');
            }
            let base = offset * 2;
            sql.push_str(&format!("(${}, ${})", base + 1, base + 2));
            params.push(Value::Text(format!("/census/{ordinal:09}")));
            params.push(Value::Integer(i64::try_from(*ordinal).expect("fits i64")));
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

    // Discard everything the seed and the checkpoint scanned.
    let _ = take_scan_key_buffer_census();
    let _ = take_hot_scan_refcount_census();

    let result = lix
        .execute(&format!("SELECT id, ordinal FROM {SCHEMA_KEY}"), &[])
        .await
        .expect("scan census rows");
    assert_eq!(result.rows().len(), ROWS, "the census must observe a full scan");

    let (allocations, allocated_bytes) = take_scan_key_buffer_census();
    let (rows_decoded, ..) = take_hot_scan_refcount_census();

    // A zero here is indistinguishable from a census that never fired.
    assert!(
        rows_decoded >= ROWS as u64,
        "hot scan decoded {rows_decoded} rows for a {ROWS}-row answer; the scan route was not taken"
    );
    assert!(
        allocations > 0,
        "no key buffer allocation was recorded; the RocksDB scan source was not the one exercised"
    );

    let per_row = allocations as f64 / rows_decoded as f64;
    println!(
        "SCAN_KEY_BUFFER_CENSUS rows_decoded={rows_decoded} allocations={allocations} \
         ({per_row:.5}/row) allocated_bytes={allocated_bytes} \
         bytes_per_row={:.1}",
        allocated_bytes as f64 / rows_decoded as f64
    );

    assert!(
        per_row <= MAX_ALLOCATIONS_PER_ROW,
        "scan allocated {allocations} key buffers for {rows_decoded} decoded rows \
         ({per_row:.5}/row, ceiling {MAX_ALLOCATIONS_PER_ROW:.5}/row); \
         a per-row key copy scores 1.00000"
    );
}
