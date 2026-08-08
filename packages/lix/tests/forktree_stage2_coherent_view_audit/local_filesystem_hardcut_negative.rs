//! Compile-fail input for the LocalFilesystem hard cut.
//!
//! The candidate qualifier compiles this as an external consumer and requires
//! unresolved import/associated-item diagnostics for every predecessor API.

use lix_storage_filesystem::{LocalFilesystem, LocalFilesystemOpenOptions};

fn old_api_must_not_compile() {
    let options = LocalFilesystemOpenOptions::new("/tmp/workspace", false);
    let _ = LocalFilesystem::open_with_options(options);
    let _ = LocalFilesystem::open_with_options_and_wasm_runtime;
    let _ = LocalFilesystem::import_paths::<Vec<&str>, &str>;
    let _ = LocalFilesystem::sync_disk_to_lix;
}

fn positive_signature_must_compile() {
    let _future = LocalFilesystem::open("/tmp/workspace");
}

fn main() {
    old_api_must_not_compile();
    positive_signature_must_compile();
}
