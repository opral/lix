//! Site A: the `lix_file ... RETURNING` readback route.
//!
//! `returning_post_image` reloads the just-staged rows through
//! `scan_lix_file_live_batch`. It is the one caller that reaches that function
//! with `FileIdConstraint::Ids` and no path index, so it is the only caller
//! whose readback can degrade to a full branch walk.

use lix::engine::Engine;
use lix::storage::Memory;
use lix::storage_bench::take_file_live_scan_accounting;
use lix::Value;

async fn seed(files: usize) -> lix::session::SessionContext<Memory> {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("engine should open");
    let session = engine.open_session().await.expect("session should open");
    for chunk_start in (0..files).step_by(500) {
        let values = (chunk_start..(chunk_start + 500).min(files))
            .map(|i| format!("('/seed-{i:08}.bin', CAST('byte-00' AS BYTEA))"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(
                &format!("INSERT INTO lix_file (path, content) VALUES {values}"),
                &[],
            )
            .await
            .expect("seed insert should succeed");
    }
    session
}

fn text(value: &Value) -> String {
    match value {
        Value::Text(text) => text.clone(),
        other => panic!("expected text, got {other:?}"),
    }
}

/// Decoded entries per answer row, swept over branch size.
///
/// This is a *count*, not a timing: it is deterministic, so one rep settles it,
/// and it cannot be confused by scheduler noise. The counters sit at both
/// per-entry decode loops (`hot_scan_entries`' wide arm and
/// `scan_hot_file_entries`' prefix arm), so a low number here means "few rows
/// were decoded", never "the census did not run" -- the route counters prove
/// separately which arm executed.
#[tokio::test(flavor = "current_thread")]
async fn returning_readback_decodes_a_bounded_number_of_entries() {
    let mut observed = Vec::new();
    for &files in &[250_usize, 500, 1000] {
        let session = seed(files).await;
        let id = text(
            &session
                .execute("SELECT id FROM lix_file WHERE path = '/seed-00000007.bin'", &[])
                .await
                .expect("target file should resolve")
                .rows()[0]
                .values()[0],
        );

        // Drain anything the seed produced so the measurement is this
        // statement's alone.
        let _ = take_file_live_scan_accounting();

        session
            .execute(
                &format!(
                    "UPDATE lix_file SET content = CAST('byte-01' AS BYTEA) \
                     WHERE id = '{id}' RETURNING id, path"
                ),
                &[],
            )
            .await
            .expect("returning update should succeed");

        let (calls, point_batch, file_prefix, fallback, decoded, matched) =
            take_file_live_scan_accounting();

        assert!(
            calls > 0,
            "the RETURNING readback must reach scan_lix_file_live_batch; \
             a zero here means the lane never executed and every other \
             number below is vacuous"
        );
        assert_eq!(
            matched, 2,
            "one descriptor + one blob ref should survive the filter at {files} files"
        );
        observed.push((files, calls, point_batch, file_prefix, fallback, decoded));
        session.close().await.expect("session should close");
    }

    for &(files, calls, point_batch, file_prefix, fallback, decoded) in &observed {
        println!(
            "files={files} calls={calls} point_batch={point_batch} \
             file_prefix={file_prefix} fallback={fallback} decoded={decoded}"
        );
    }

    // Routing, positively: the pinned request must take the file-prefix arm and
    // must NOT take the widest arm. Asking which symbol is *missing* is the
    // attribution -- a fallback of zero is what the unpinned request could
    // never produce.
    for &(files, _, _, file_prefix, fallback, _) in &observed {
        assert!(
            file_prefix > 0,
            "at {files} files the pinned request should resolve as a file prefix seek"
        );
        assert_eq!(
            fallback, 0,
            "at {files} files the request still fell through to the full branch walk"
        );
    }

    // The slope is the claim. Decoded entries must not grow with branch size.
    let smallest = observed.first().expect("swept at least one size").5;
    let largest = observed.last().expect("swept at least one size").5;
    assert!(
        largest <= smallest * 2,
        "decoded entries grew with branch size ({observed:?}); the read is still O(files)"
    );
}

/// The pin must not narrow the answer.
///
/// The oracle is the **unfiltered** read. `SELECT * FROM lix_file` resolves no
/// target ids at all, so `scan_lix_file_live_batch` takes its
/// `FileIdConstraint::All` early return and structurally cannot execute the
/// pin. Every `RETURNING` post-image must agree with it.
///
/// The shapes are the ones that could plausibly carry a null or divergent
/// `file_id`: an insert with **no explicit id**, where the planner really does
/// stage the descriptor with `file_id: None` and only
/// `canonicalize_descriptor_file_id` fills it in; a rename, which rewrites the
/// descriptor without touching content; a content clear, which tombstones the
/// blob ref while the descriptor survives; a delete, which tombstones both; and
/// a recreation at the same path under a **different** id.
#[tokio::test(flavor = "current_thread")]
async fn pinned_returning_readback_agrees_with_the_unfiltered_read() {
    let session = seed(8).await;

    async fn check(
        session: &lix::session::SessionContext<Memory>,
        id: &str,
        path: &str,
    ) {
        let oracle = session
            .execute("SELECT id, path FROM lix_file", &[])
            .await
            .expect("unfiltered read should succeed");
        let found = oracle
            .rows()
            .iter()
            .find(|row| text(&row.values()[0]) == id)
            .map(|row| text(&row.values()[1]));
        assert_eq!(
            found.as_deref(),
            Some(path),
            "RETURNING post-image disagreed with the unfiltered read for {id}"
        );
    }

    // 1. Insert with no explicit id.
    let inserted = session
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/oracle/minted.md', \
             CAST('v0' AS BYTEA)) RETURNING id, path",
            &[],
        )
        .await
        .expect("id-less insert should return its post-image");
    let minted = text(&inserted.rows()[0].values()[0]);
    assert_eq!(text(&inserted.rows()[0].values()[1]), "/oracle/minted.md");
    check(&session, &minted, "/oracle/minted.md").await;

    // 2. Rename: descriptor rewrite, no content change.
    let renamed = session
        .execute(
            &format!(
                "UPDATE lix_file SET path = '/oracle/renamed.md' WHERE id = '{minted}' \
                 RETURNING id, path"
            ),
            &[],
        )
        .await
        .expect("rename should return its post-image");
    assert_eq!(text(&renamed.rows()[0].values()[1]), "/oracle/renamed.md");
    check(&session, &minted, "/oracle/renamed.md").await;

    // 3. Content clear: blob-ref tombstone, descriptor survives.
    let cleared = session
        .execute(
            &format!(
                "UPDATE lix_file SET content = CAST('' AS BYTEA) WHERE id = '{minted}' \
                 RETURNING id, path"
            ),
            &[],
        )
        .await
        .expect("content clear should return its post-image");
    assert_eq!(cleared.rows().len(), 1, "content clear should return one row");
    check(&session, &minted, "/oracle/renamed.md").await;

    // 4. Delete: tombstones both rows.
    let deleted = session
        .execute(
            &format!("DELETE FROM lix_file WHERE id = '{minted}' RETURNING id"),
            &[],
        )
        .await
        .expect("delete should return its pre-image");
    assert_eq!(text(&deleted.rows()[0].values()[0]), minted);
    let after = session
        .execute("SELECT id FROM lix_file", &[])
        .await
        .expect("unfiltered read should succeed");
    assert!(
        !after.rows().iter().any(|row| text(&row.values()[0]) == minted),
        "deleted file should be absent from the unfiltered read"
    );

    // 5. Recreation at the same path under a different id, so a path lookup
    //    resolves two ids over history and per-file discrimination must be
    //    exact rather than vacuous.
    let recreated = session
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/oracle/renamed.md', \
             CAST('v9' AS BYTEA)) RETURNING id, path",
            &[],
        )
        .await
        .expect("recreation should return its post-image");
    let second = text(&recreated.rows()[0].values()[0]);
    assert_ne!(second, minted, "recreation should mint a fresh id");
    check(&session, &second, "/oracle/renamed.md").await;

    session.close().await.expect("session should close");
}
