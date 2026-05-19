#![cfg(feature = "storage-benches")]

#[test]
fn storage_benches_accounting_placeholder() {
    // Historical storage accounting tests depended on the removed storage v1
    // API. The promoted storage-benches feature now builds the storage_v2 bench
    // target directly; keep this file as a feature-gated smoke test so
    // `cargo check --features storage-benches --tests` exercises the feature.
}
