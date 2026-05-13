//! Shared metric wrappers for changelog benchmarks.
//!
//! The current benches return compact counts from the feature-gated
//! `changelog_bench` facade. Keep additional accounting helpers here when we
//! start reporting namespace bytes, segment gets, or staged put/delete counts.
