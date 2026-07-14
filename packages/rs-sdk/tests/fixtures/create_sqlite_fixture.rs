use std::path::PathBuf;

use lix_sdk::SQLite;

fn main() {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .expect("usage: create_sqlite_fixture <path>");

    let storage = SQLite::open(&path).expect("sqlite storage should create fixture");
    storage
        .checkpoint()
        .expect("sqlite storage fixture should checkpoint");
}
