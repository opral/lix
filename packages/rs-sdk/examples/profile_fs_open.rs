// Cold-open profiling harness for the filesystem backend.
//
// Usage:
//   cargo run --release --example profile_fs_open --features sqlite -- <src_dir>
//
// Copies <src_dir> (sans any existing .lix) into a fresh temp dir, then times
// FsBackend::open on the cold workspace.

use std::path::Path;
use std::time::Instant;

use lix_sdk::FsBackend;

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if name == ".lix" {
            continue;
        }
        let from = entry.path();
        let to = dst.join(&name);
        if from.is_dir() {
            copy_dir(&from, &to);
        } else {
            std::fs::copy(&from, &to).unwrap();
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let src = std::env::args()
        .nth(1)
        .expect("usage: profile_fs_open <src_dir>");
    let src = Path::new(&src);

    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path().join("workspace");

    let t_copy = Instant::now();
    copy_dir(src, &work);
    let copy_elapsed = t_copy.elapsed();
    eprintln!("copy: {copy_elapsed:?}");

    let repeat: usize = std::env::var("REPEAT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    let t_open = Instant::now();
    let backend = FsBackend::open(&work).await.unwrap();
    let open_elapsed = t_open.elapsed();
    eprintln!("cold open: {open_elapsed:?}");
    drop(backend);

    // Warm reopen (now .lix exists).
    let t_warm = Instant::now();
    let backend = FsBackend::open(&work).await.unwrap();
    let warm_elapsed = t_warm.elapsed();
    eprintln!("warm reopen: {warm_elapsed:?}");
    drop(backend);

    // Repeated cold opens into fresh temp dirs for profiling sample density.
    for i in 0..repeat {
        let tmp_i = tempfile::tempdir().unwrap();
        let work_i = tmp_i.path().join("workspace");
        copy_dir(src, &work_i);
        let t = Instant::now();
        let backend = FsBackend::open(&work_i).await.unwrap();
        if i == repeat - 1 {
            let elapsed = t.elapsed();
            eprintln!("cold open (repeat {repeat}): {elapsed:?}");
        }
        drop(backend);
    }

    println!("COLD_OPEN_MS={}", open_elapsed.as_millis());
}
