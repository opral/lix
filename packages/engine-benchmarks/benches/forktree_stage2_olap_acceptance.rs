//! Closed compile boundary for the external Stage 2 OLAP acceptance oracle.
//!
//! This benchmark contains no model or alternate implementation. The
//! production Stage 2 owner must expose the sealed, storage-benches-only
//! `AcceptancePhysicalLayout` SPI and its sole concrete owner. Exact a12 is
//! expected to fail compilation here because it has no Stage 2 owner.

use lix::storage_bench::{AcceptancePhysicalLayout, Stage2ProductionPhysicalLayout};

fn main() {
    // `run_cli` must execute public SQL/DataFusion queries. The SPI may expose
    // only owner identity, authenticated physical counters, and deterministic
    // malformed/substituted-block injection; it must not supply query results.
    <Stage2ProductionPhysicalLayout as AcceptancePhysicalLayout>::run_cli()
        .expect("Stage 2 OLAP acceptance cell failed");
}
