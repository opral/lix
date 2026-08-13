//! Which physical route the write path's pre-image read takes, as a function
//! of the predicate shape.
//!
//! `scan_entity_candidates` binds `entity_pks` only when the WHERE clause is a
//! primary-key predicate. When it does not, `hot_scan_entries` has no identity
//! to seek with and falls through to the primary-prefix arm, which walks every
//! row of the schema in the branch and rejects non-matches with
//! `identity.matches_filter` in memory.
//!
//! Every counter asserted here is taken INSIDE that per-entry decode loop.
//! That layer is the whole point: a count taken above it -- at `scan_batch`'s
//! return value -- is already post `matches_filter` and therefore reads
//! identically under a seek and under a full-prefix walk. A census with that
//! defect once reported `scanned=10 returned=10` for a walk over every row in
//! the branch and retired the true culprit for three cycles.
//!
//! This is a dedicated `[[test]]` target, and deliberately so: the counters in
//! `storage_bench` are process-global, so a test sharing a process with the
//! rest of the suite would see other tests' scans. Its own process makes the
//! counts attributable. Assertions are still thresholds scaled to this
//! fixture rather than exact values.
#![cfg(feature = "storage-benches")]

use lix::storage_bench::{
    CRUD_PHASE_WRITE_READ, HotScanRouteCensus, take_hot_scan_route_census,
};
use lix::{Memory, Value, open_lix};

/// Rows in the fixture. Kept below the point where the fixture's rows leave
/// the HOT row space for the packed base: above that threshold the remaining
/// rows are served by a read path this census does not hook, and the decode
/// counts stop reflecting the collection size.
const FIXTURE_ROWS: usize = 200;

async fn measure(rows: usize, sql: &str, params: Vec<Value>) -> HotScanRouteCensus {
    let lix = open_lix()
        .with_storage(Memory::new())
        .await
        .expect("open lix");

    let values = (0..rows)
        .map(|index| format!("('seed-{index:06}', '\"v0\"')"))
        .collect::<Vec<_>>()
        .join(", ");
    lix.execute(
        &format!("INSERT INTO lix_key_value (key, value) VALUES {values}"),
        &[],
    )
    .await
    .expect("fixture rows should commit");

    let _ = take_hot_scan_route_census();
    let result = lix
        .execute(sql, &params)
        .await
        .unwrap_or_else(|error| panic!("statement `{sql}` should execute: {error:?}"));
    let census = take_hot_scan_route_census()[CRUD_PHASE_WRITE_READ];

    // A statement that touched nothing is a vacuous lane, and its zero
    // counters would be a false negative rather than a measurement.
    assert!(
        result.rows_affected() > 0,
        "statement `{sql}` must affect rows, got {}",
        result.rows_affected()
    );
    census
}

fn text(value: &str) -> Value {
    Value::Text(value.to_string())
}

#[tokio::test]
async fn primary_key_writes_seek_and_non_key_writes_walk() {
    // MISS arm: the predicate binds an identity, so the scan seeks.
    let seek = measure(
        FIXTURE_ROWS,
        "UPDATE lix_key_value SET value = $1 WHERE key = $2",
        vec![text("\"v1\""), text("seed-000000")],
    )
    .await;

    // HIT arm: equality on a non-key column binds no identity, so the scan
    // walks the schema prefix.
    let walk = measure(
        FIXTURE_ROWS,
        "UPDATE lix_key_value SET value = $1 WHERE value = $2",
        vec![text("\"v1\""), text("\"v0\"")],
    )
    .await;

    // Twice the rows, to show the walk's cost tracks the collection and the
    // seek's does not.
    let walk_double = measure(
        FIXTURE_ROWS * 2,
        "UPDATE lix_key_value SET value = $1 WHERE value = $2",
        vec![text("\"v1\""), text("\"v0\"")],
    )
    .await;
    let seek_double = measure(
        FIXTURE_ROWS * 2,
        "UPDATE lix_key_value SET value = $1 WHERE key = $2",
        vec![text("\"v1\""), text("seed-000000")],
    )
    .await;

    println!("PREIMAGEROUTE seek={seek:?}");
    println!("PREIMAGEROUTE walk={walk:?}");
    println!("PREIMAGEROUTE seek_double={seek_double:?}");
    println!("PREIMAGEROUTE walk_double={walk_double:?}");

    // Connectivity first: both arms reach the pre-image read, so every zero
    // below is readable as "that route did not run" rather than "this
    // instrument did not run".
    assert!(
        seek.calls > 0 && walk.calls > 0,
        "both arms must issue a pre-image scan: seek {} calls, walk {} calls",
        seek.calls,
        walk.calls
    );

    // MISS: the seek arm takes the point-batch route and its decode loop never
    // runs. The threshold is scaled far below what the walk arm decodes.
    assert!(
        seek.point_batch > 0,
        "primary-key update should take the point-batch arm, got {} point reads",
        seek.point_batch
    );
    assert!(
        seek.fallback_entries_decoded < (FIXTURE_ROWS / 20) as u64,
        "primary-key update should not walk the schema prefix, decoded {}",
        seek.fallback_entries_decoded
    );

    // HIT: the walk arm takes the primary-prefix route and decodes the whole
    // collection. Without this the assertion above would pass on a dead
    // instrument.
    assert!(
        walk.fallback > 0,
        "non-key update should take the primary-prefix arm, got {} fallbacks",
        walk.fallback
    );
    assert!(
        walk.fallback_entries_decoded >= FIXTURE_ROWS as u64,
        "primary-prefix walk should decode the whole collection: {} rows, decoded {}",
        FIXTURE_ROWS,
        walk.fallback_entries_decoded
    );

    // The two routes must be separated by far more than measurement noise.
    assert!(
        walk.fallback_entries_decoded > seek.fallback_entries_decoded.saturating_mul(10) + 10,
        "seek and walk must be distinguishable: seek decoded {}, walk decoded {}",
        seek.fallback_entries_decoded,
        walk.fallback_entries_decoded
    );

    // Scaling: doubling the collection roughly doubles what the walk decodes
    // and leaves the seek flat.
    assert!(
        walk_double.fallback_entries_decoded > walk.fallback_entries_decoded * 3 / 2,
        "walk should scale with the collection: {} rows decoded {}, {} rows decoded {}",
        FIXTURE_ROWS,
        walk.fallback_entries_decoded,
        FIXTURE_ROWS * 2,
        walk_double.fallback_entries_decoded
    );
    assert!(
        seek_double.fallback_entries_decoded < (FIXTURE_ROWS / 20) as u64,
        "seek should stay flat as the collection grows, decoded {}",
        seek_double.fallback_entries_decoded
    );
}

/// The point-batch arm asks `FILE_SPACE` whether the schema has any
/// file-backed member before it will serve a null-file point batch. That guard
/// is uncached and its only caller is this arm, so a primary-key write pays one
/// such point read per pre-image scan on top of the row reads it came for.
#[tokio::test]
async fn point_batch_arm_pays_an_uncached_file_member_guard_read() {
    let seek = measure(
        FIXTURE_ROWS,
        "UPDATE lix_key_value SET value = $1 WHERE key = $2",
        vec![text("\"v1\""), text("seed-000000")],
    )
    .await;

    println!("PREIMAGEGUARD seek={seek:?}");

    assert!(
        seek.calls > 0,
        "the primary-key update must issue a pre-image scan; instrument is dead"
    );
    assert!(
        seek.file_member_guard_reads >= seek.calls,
        "the point-batch arm should issue at least one FILE_SPACE guard read per scan: \
         {} scans, {} guard reads",
        seek.calls,
        seek.file_member_guard_reads
    );
}
