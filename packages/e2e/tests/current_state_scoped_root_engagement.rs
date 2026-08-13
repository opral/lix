//! End-to-end coverage for `current_state_scoped_ranges`, the per-scope
//! current-state read accelerator.
//!
//! Before this test the mechanism had no production coverage at all. Every
//! unconditional `Some(..)` write to `current_state_scoped_ranges` in the tree
//! is `#[cfg(test)]` fixture state, so its correctness was only ever exercised
//! against roots the tests fabricated — never against a root the engine
//! actually published.
//!
//! Reaching it from the public API requires one specific shape:
//!
//! * `TYPED_CERTIFIED_INSERT_MIN_ROWS` (`32 * 1024`, in
//!   `sql2/exec/bound_public_write.rs`) rows,
//! * in a single `execute_batch` of same-shape parameterized single-row
//!   INSERTs,
//! * against a **user-registered** entity schema.
//!
//! `PACKED_CURRENT_BASE_MIN_ROWS` (`512`, in `transaction/commit.rs`) is
//! necessary but *not* sufficient, and reading it as the threshold is why this
//! mechanism looked untestable: at 512–32,767 rows the batch still certifies
//! and the columnar arm is still never taken. `certified_entity_insert_*`
//! counters therefore cannot stand in for the assertions below — they are
//! already non-zero at 511 rows with no columnar publication anywhere.
//!
//! The test asserts both halves, which fail independently:
//!
//! 1. **bootstrap** — the bulk batch mints a scoped root out of the
//!    `parent_root == None` fixed point;
//! 2. **sustain** — later commits *in the same scope* find that parent root
//!    and carry it forward. Nothing else in the tree asserts this, and a
//!    regression that bootstraps and then self-extinguishes leaves half 1
//!    green.

use lix::storage_bench::certified_current_state_publication_counters;
use lix::{ExecuteBatchStatement, Value, open_lix};

/// Must stay at or above `TYPED_CERTIFIED_INSERT_MIN_ROWS`. Below it the
/// columnar arm is not reached and half 1 of this test cannot pass.
const BULK_ROWS: usize = 32 * 1024;

/// Same-scope commits issued after the bulk load. Four is enough to
/// distinguish "sustains" from "engaged once and died": an accelerator that
/// self-extinguishes yields at most one parent-root hit.
const FOLLOW_UP_COMMITS: usize = 4;

const SCHEMA_KEY: &str = "scoped_root_engagement_probe";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_typed_insert_bootstraps_and_sustains_a_scoped_current_state_root() {
    let lix = open_lix().await.expect("open in-memory lix");

    let schema = serde_json::json!({
        "x-lix-key": SCHEMA_KEY,
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "value": { "type": "string" }
        },
        "required": ["id", "value"],
        "additionalProperties": false
    });
    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
        &[Value::Text(schema.to_string())],
    )
    .await
    .expect("register the scoped-root probe schema");

    // The counters are process-global and read without reset, so every
    // assertion below is a threshold on a snapshot delta rather than an exact
    // value. A concurrent contributor can only inflate a delta, never hide a
    // mechanism that stopped engaging. This target also runs as its own test
    // binary with a single test, so in practice nothing else contributes.
    let before_bulk = certified_current_state_publication_counters();

    let insert_sql = format!("INSERT INTO {SCHEMA_KEY} (id, value) VALUES ($1, $2)");
    let statements = (0..BULK_ROWS)
        .map(|row| ExecuteBatchStatement {
            label: None,
            sql: insert_sql.clone(),
            params: vec![
                Value::Text(format!("bulk-{row:08}")),
                Value::Text(format!("value-{row}")),
            ],
        })
        .collect::<Vec<_>>();
    let results = lix
        .execute_batch(&statements)
        .await
        .expect("bulk typed INSERT batch");
    assert_eq!(
        results.len(),
        BULK_ROWS,
        "the batch must carry at least TYPED_CERTIFIED_INSERT_MIN_ROWS rows, \
         or the columnar arm is never reached"
    );

    let after_bulk = certified_current_state_publication_counters();
    let columnar_publications = after_bulk
        .columnar_root_publications
        .saturating_sub(before_bulk.columnar_root_publications);
    assert!(
        columnar_publications >= 1,
        "half 1 (bootstrap): a {BULK_ROWS}-row certified typed INSERT batch published no columnar \
         scoped current-state root (delta {columnar_publications}). The accelerator never left the \
         parent_root == None fixed point, so no later commit can inherit one."
    );

    // Same-scope follow-up commits. Each one must find the root the bulk load
    // published; a commit in a *different* scope would legitimately drop the
    // accelerator, because roots are per-scope.
    for round in 0..FOLLOW_UP_COMMITS {
        lix.execute(
            &insert_sql,
            &[
                Value::Text(format!("follow-{round:08}")),
                Value::Text(format!("follow-value-{round}")),
            ],
        )
        .await
        .expect("same-scope follow-up insert");
    }

    let after_follow_ups = certified_current_state_publication_counters();
    let parent_root_hits = after_follow_ups
        .parent_root_hits
        .saturating_sub(after_bulk.parent_root_hits);
    assert!(
        parent_root_hits >= 2,
        "half 2 (sustain): {FOLLOW_UP_COMMITS} same-scope commits after the bulk load found a \
         parent scoped root only {parent_root_hits} time(s). The accelerator bootstrapped and then \
         self-extinguished, which leaves the bootstrap assertion above green."
    );

    let count = lix
        .execute(&format!("SELECT COUNT(*) AS count FROM {SCHEMA_KEY}"), &[])
        .await
        .expect("read the probe collection back");
    assert_eq!(
        count.rows()[0].get::<i64>("count").unwrap(),
        (BULK_ROWS + FOLLOW_UP_COMMITS) as i64
    );

    lix.close().await.expect("close the probe repository");
}
