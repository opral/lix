use lix_storage_filesystem::{LocalFilesystem, LocalFilesystemOpenOptions};

fn removed_api_must_not_compile(storage: &LocalFilesystem) {
    let options = LocalFilesystemOpenOptions::new("/tmp/workspace", false);
    let _ = LocalFilesystem::open_with_options(options.clone());
    let _ = LocalFilesystem::open_with_options_and_wasm_runtime;
    let _ = storage.import_paths(["note.md"]);
    let _ = storage.sync_disk_to_lix();
}

fn main() {}
