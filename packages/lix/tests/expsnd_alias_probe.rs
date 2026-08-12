#![recursion_limit = "2048"]
//! EXPSND reachability probe, v2.
//!
//! v1 was vacuous: `INSERT INTO lix_file (path, content) VALUES ...` routes
//! through the native fast path and bypasses the DataFusion write session, so
//! only 2 `SqlWriteContext`s were ever constructed. Predicated UPDATE/DELETE
//! does force the write session, so this drives those, on a multi-threaded
//! runtime, which the lix suite otherwise almost never uses.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

async fn seed(lix: &lix::Lix, tag: &str, rows: usize) {
    let values = (0..rows)
        .map(|i| format!("('/{tag}-{i:06}.bin', CAST('byte-00' AS BYTEA))"))
        .collect::<Vec<_>>()
        .join(",");
    lix.execute(
        &format!("INSERT INTO lix_file (path, content) VALUES {values}"),
        &[],
    )
    .await
    .expect("seed insert should succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn expsnd_predicated_writes_multithread() {
    let lix = lix::open_lix().await.expect("open lix");
    seed(&lix, "wide", 2048).await;

    // Wide predicated UPDATE: forces the DataFusion write session and gives the
    // planner the most rows to spread across partitions.
    for round in 0..8 {
        lix.execute(
            &format!(
                "UPDATE lix_file SET content = CAST('round-{round:02}' AS BYTEA) \
                 WHERE path LIKE '/wide-%'"
            ),
            &[],
        )
        .await
        .expect("wide update should succeed");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn expsnd_concurrent_predicated_writes() {
    let lix = lix::open_lix().await.expect("open lix");
    for task in 0..8 {
        seed(&lix, &format!("c{task}"), 128).await;
    }

    let ok = Arc::new(AtomicUsize::new(0));
    let err = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for task in 0..8u32 {
        let lix = lix.clone();
        let ok = Arc::clone(&ok);
        let err = Arc::clone(&err);
        handles.push(tokio::spawn(async move {
            for round in 0..16u32 {
                let updated = lix
                    .execute(
                        &format!(
                            "UPDATE lix_file SET content = CAST('t{task}r{round}' AS BYTEA) \
                             WHERE path LIKE '/c{task}-%'"
                        ),
                        &[],
                    )
                    .await;
                match updated {
                    Ok(_) => ok.fetch_add(1, Ordering::Relaxed),
                    Err(_) => err.fetch_add(1, Ordering::Relaxed),
                };
                let _ = lix
                    .execute(
                        "SELECT path, content FROM lix_file WHERE path LIKE '/c%' LIMIT 16",
                        &[],
                    )
                    .await;
            }
        }));
    }
    for handle in handles {
        handle.await.expect("task should not panic");
    }

    let ok = ok.load(Ordering::Relaxed);
    let err = err.load(Ordering::Relaxed);
    eprintln!("EXPSND_CONCURRENT_WRITES ok={ok} err={err}");
    assert!(
        ok > 0,
        "the concurrent phase must actually execute writes, else the probe is vacuous"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn expsnd_predicated_delete_multithread() {
    let lix = lix::open_lix().await.expect("open lix");
    seed(&lix, "del", 512).await;
    lix.execute("DELETE FROM lix_file WHERE path LIKE '/del-%'", &[])
        .await
        .expect("predicated delete should succeed");
}
