use std::fs;
use std::path::PathBuf;

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn engine_source_path(relative: &str) -> PathBuf {
    manifest_dir().join("src").join(relative)
}

fn read_engine_source(relative: &str) -> String {
    fs::read_to_string(engine_source_path(relative))
        .unwrap_or_else(|error| panic!("failed to read {relative}: {error}"))
}

fn pre_test_region(relative: &str) -> String {
    read_engine_source(relative)
        .split("#[cfg(test)]")
        .next()
        .expect("source should have a pre-test region")
        .to_string()
}

fn assert_absent(source: &str, needles: &[&str], context: &str) {
    for needle in needles {
        assert!(
            !source.contains(needle),
            "{context} should not contain `{needle}`"
        );
    }
}

#[test]
fn binary_cas_ddl_is_not_owned_by_filesystem_or_live_state_bootstrap() {
    let filesystem_init = pre_test_region("filesystem/init.rs");
    assert_absent(
        &filesystem_init,
        &["lix_internal_binary_"],
        "filesystem/init.rs",
    );

    let bootstrap_tables = pre_test_region("live_state/bootstrap_tables.rs");
    assert!(
        bootstrap_tables.contains("crate::binary_cas::init(backend)"),
        "live_state/bootstrap_tables.rs should delegate CAS bootstrap to binary_cas::init"
    );
    assert_absent(
        &bootstrap_tables,
        &[
            "CREATE TABLE IF NOT EXISTS lix_internal_binary_",
            "CREATE INDEX IF NOT EXISTS idx_lix_internal_binary_",
        ],
        "live_state/bootstrap_tables.rs",
    );
}

#[test]
fn remaining_consumers_use_binary_cas_owner_paths() {
    let plugin_runtime = pre_test_region("plugin/runtime.rs");
    assert!(
        plugin_runtime.contains("use crate::binary_cas::read::load_binary_blob_data_by_hash;"),
        "plugin/runtime.rs should load CAS blobs through binary_cas::read"
    );
    assert!(
        plugin_runtime.contains("use crate::binary_cas::schema::INTERNAL_BINARY_FILE_VERSION_REF;"),
        "plugin/runtime.rs should import the canonical CAS ref table constant"
    );
    assert_absent(
        &plugin_runtime,
        &[
            "fn load_binary_blob_data_by_hash(",
            "decode_binary_chunk_payload(",
            "binary_blob_hash_hex(",
            "lix_internal_binary_",
        ],
        "plugin/runtime.rs",
    );

    let live_projection = pre_test_region("filesystem/live_projection.rs");
    assert!(
        live_projection.contains("use crate::binary_cas::schema::INTERNAL_BINARY_BLOB_STORE;"),
        "filesystem/live_projection.rs should import the canonical CAS blob-store constant"
    );
    assert_absent(
        &live_projection,
        &["lix_internal_binary_blob_store"],
        "filesystem/live_projection.rs",
    );
}

#[test]
fn filesystem_runtime_no_longer_owns_cas_codec_persistence_or_gc_helpers() {
    let filesystem_runtime = pre_test_region("filesystem/runtime.rs");
    assert!(
        filesystem_runtime.contains(
            "crate::binary_cas::gc::garbage_collect_unreachable_binary_cas_in_transaction"
        ),
        "filesystem/runtime.rs should delegate CAS GC to binary_cas::gc"
    );
    assert_absent(
        &filesystem_runtime,
        &[
            "fastcdc_chunk_ranges(",
            "should_materialize_chunk_cas(",
            "encode_binary_chunk_payload(",
            "decode_binary_chunk_payload(",
            "build_binary_blob_fastcdc_write_program(",
            "persist_resolved_binary_blob_writes_in_transaction(",
            "trait BinaryCasExecutor",
            "struct TransactionBinaryCasExecutor",
            "delete_unreferenced_binary_",
        ],
        "filesystem/runtime.rs",
    );
}

#[test]
fn validation_uses_binary_cas_boundary_instead_of_raw_cas_tables() {
    let validation = pre_test_region("sql/semantic_ir/validation.rs");
    assert!(
        validation.contains("crate::binary_cas::read::blob_exists("),
        "validation.rs should check blob existence through binary_cas::read"
    );
    assert_absent(
        &validation,
        &["lix_internal_binary_"],
        "sql/semantic_ir/validation.rs",
    );
}

#[test]
fn obsolete_filesystem_owned_binary_cas_modules_stay_deleted() {
    let manifest_dir = manifest_dir();
    for relative in [
        "src/sql/storage/queries/filesystem.rs",
        "src/sql/storage/queries/history.rs",
        "src/sql/storage/tables/filesystem.rs",
    ] {
        let path = manifest_dir.join(relative);
        assert!(
            !path.exists(),
            "{relative} should stay deleted once binary_cas/ owns the service boundary"
        );
    }

    let queries_mod = read_engine_source("sql/storage/queries/mod.rs");
    assert_absent(
        &queries_mod,
        &["mod filesystem;", "mod history;"],
        "sql/storage/queries/mod.rs",
    );
}
