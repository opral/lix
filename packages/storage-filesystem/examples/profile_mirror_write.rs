//! Compare the former in-place write with the production atomic replacement helper.
//! cargo run -p lix-storage-filesystem --example profile_mirror_write
//! Reports warm-cache file-operation latency, excluding SQL, watchers, and fsync.
#[allow(dead_code)]
#[path = "../src/atomic_write.rs"]
mod atomic_write;

use std::time::Instant;

fn report(name: &str, samples: &mut [f64]) {
    samples.sort_by(f64::total_cmp);
    println!(
        "{name}: median={:.1}us p95={:.1}us",
        samples[samples.len() / 2],
        samples[samples.len() * 95 / 100]
    );
}

fn main() {
    let directory = tempfile::tempdir().unwrap();
    println!(
        "OS={} temp_root={} samples=200 warmup=20 alternating_order=true",
        std::env::consts::OS,
        directory.path().display()
    );
    for bytes in [4096, 1024 * 1024, 5 * 1024 * 1024] {
        let data = vec![0x5a; bytes];
        let old = directory.path().join("in-place");
        let new = directory.path().join("atomic");
        let mut old_samples = Vec::new();
        let mut new_samples = Vec::new();
        for iteration in 0..220 {
            for atomic in if iteration % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                let start = Instant::now();
                if atomic {
                    atomic_write::atomic_write(&new, &data).unwrap();
                } else {
                    std::fs::write(&old, &data).unwrap();
                }
                let micros = start.elapsed().as_secs_f64() * 1e6;
                if iteration >= 20 {
                    if atomic {
                        new_samples.push(micros);
                    } else {
                        old_samples.push(micros);
                    }
                }
            }
        }
        assert_eq!(std::fs::read(&new).unwrap(), data);
        println!("bytes={bytes}");
        report("in-place", &mut old_samples);
        report("atomic", &mut new_samples);
    }
}
