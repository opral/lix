use std::path::PathBuf;

use lix_sdk::SqliteBackend;

fn main() {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .expect("usage: create_sqlite_fixture <path>");

    let backend = SqliteBackend::open(&path).expect("sqlite backend should create fixture");
    backend
        .checkpoint()
        .expect("sqlite backend fixture should checkpoint");
}
