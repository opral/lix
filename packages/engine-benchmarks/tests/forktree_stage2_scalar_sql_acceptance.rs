//! Test/report-only scalar SQL acceptance contract.
//!
//! This target intentionally contains no production implementation and no
//! adapter setup. It freezes the logical fixture digest and the required
//! public semantic cases for the later runnable R5 successor. The source gate
//! script is the d6b calibration entry point because d6b is not compile-green.

use sha2::{Digest, Sha256};

const LOGICAL_FIXTURE: &str = concat!(
    "entity=scalar_demo|id=row-0001|kind=alpha|score=7|optional=NULL|tombstone=0\n",
    "entity=scalar_demo|id=row-0002|kind=beta|score=11|optional=x|tombstone=0\n",
    "entity=scalar_demo|id=row-0003|kind=gamma|score=13|optional=NULL|tombstone=1\n",
    "entity=scalar_demo|id=row-0004|kind=delta|score=17|optional=y|tombstone=0\n",
);

const LOGICAL_FIXTURE_SHA256: &str =
    "c9a948fd503d674738d12ad03d88e3506957bb299894f202392fb68ce8eadcde";

const REQUIRED_CASES: &[&str] = &[
    "point",
    "bounded_range",
    "entity_projection_filter_order",
    "sql_null_vs_tombstone",
    "returning_insert_update_delete_upsert",
    "branch_snapshot_identity",
    "cold_reopen",
    "selector_corruption",
    "catalog_or_topology_corruption",
    "leaf_or_value_corruption",
    "blob_manifest_substitution",
];

#[test]
fn logical_fixture_digest_is_frozen() {
    let digest = Sha256::digest(LOGICAL_FIXTURE.as_bytes());
    let actual = format!("{digest:x}");
    assert_eq!(actual, LOGICAL_FIXTURE_SHA256);
}
#[test]
fn scalar_sql_acceptance_case_set_is_complete() {
    assert_eq!(REQUIRED_CASES.len(), 11);
    for case in REQUIRED_CASES {
        assert!(!case.is_empty());
    }
}
