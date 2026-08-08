use std::path::Path;

use lix_storage_filesystem::LocalFilesystem;

fn positional_path_only(path: &Path) {
    let _open = LocalFilesystem::open(path);
}

fn main() {
    positional_path_only(Path::new("/tmp/local-filesystem-oracle"));
}
