// In-place cold-open profiling harness for the filesystem backend.
//
// Usage:
//   cargo run --release --example profile_fs_open_in_place --features sqlite -- <dir>
//
// Deletes <dir>/.lix, then times FsBackend::open on <dir>. This is intended
// for local profiling of large mounted folders where copying the directory
// would dominate the measurement.

use std::path::PathBuf;
use std::time::Instant;

use lix_sdk::FsBackend;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let dir = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .expect("usage: profile_fs_open_in_place <dir>");

    let lix_dir = dir.join(".lix");
    if lix_dir.exists() {
        let t = Instant::now();
        std::fs::remove_dir_all(&lix_dir)
            .unwrap_or_else(|error| panic!("failed to remove {}: {error}", lix_dir.display()));
        eprintln!("remove .lix: {:?}", t.elapsed());
    }

    let t = Instant::now();
    let backend = FsBackend::open(&dir).await.unwrap();
    let elapsed = t.elapsed();
    eprintln!("cold open: {elapsed:?}");
    println!("COLD_OPEN_MS={}", elapsed.as_millis());

    drop(backend);
}
