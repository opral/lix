use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use lix::Value;
use lix::engine::Engine;
use lix::storage::{
    BeginScanOptions, GetManyRequest, GetManyResult, KeyRange, Memory, MemoryRead, MemoryWrite,
    ReadOptions, ScanChunk, ScanCursor, Storage, StorageError, StorageRead, StorageScanSource,
    WriteOptions,
};
use lix::{CreateBranchOptions, LixError, MergeBranchOptions};
use serde_json::json;

use super::assert_rows_eq;

#[derive(Clone, Default)]
struct CountingStorage {
    inner: Memory,
    get_many_requested_keys: Arc<AtomicU64>,
    scan_calls: Arc<AtomicU64>,
    scanned_rows: Arc<AtomicU64>,
}

struct CountingRead {
    inner: MemoryRead,
    get_many_requested_keys: Arc<AtomicU64>,
    scan_calls: Arc<AtomicU64>,
    scanned_rows: Arc<AtomicU64>,
}

impl CountingStorage {
    fn reset_counters(&self) {
        self.get_many_requested_keys.store(0, Ordering::Relaxed);
        self.scan_calls.store(0, Ordering::Relaxed);
        self.scanned_rows.store(0, Ordering::Relaxed);
    }

    fn counters(&self) -> (u64, u64, u64) {
        (
            self.get_many_requested_keys.load(Ordering::Relaxed),
            self.scan_calls.load(Ordering::Relaxed),
            self.scanned_rows.load(Ordering::Relaxed),
        )
    }
}

impl Storage for CountingStorage {
    type Read<'a>
        = CountingRead
    where
        Self: 'a;
    type Write<'a>
        = MemoryWrite
    where
        Self: 'a;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        Ok(CountingRead {
            inner: self.inner.begin_read(options).await?,
            get_many_requested_keys: Arc::clone(&self.get_many_requested_keys),
            scan_calls: Arc::clone(&self.scan_calls),
            scanned_rows: Arc::clone(&self.scanned_rows),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.inner.begin_write(options).await
    }
}

impl StorageRead for CountingRead {
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        self.get_many_requested_keys.fetch_add(
            requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum(),
            Ordering::Relaxed,
        );
        self.inner.get_many(requests).await
    }

    async fn begin_scan(
        &self,
        space: lix::storage::StorageSpace,
        range: KeyRange,
        options: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        let order = options.order;
        let inner = self.inner.begin_scan(space, range.clone(), options).await?;
        self.scan_calls.fetch_add(1, Ordering::Relaxed);
        ScanCursor::from_source(
            range,
            order,
            CountingScanSource {
                inner,
                scanned_rows: Arc::clone(&self.scanned_rows),
            },
        )
    }
}

struct CountingScanSource<'a> {
    inner: ScanCursor<'a>,
    scanned_rows: Arc<AtomicU64>,
}

impl StorageScanSource for CountingScanSource<'_> {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            let (chunk, chunk_has_more) = self.inner.next_page(limit_rows).await?.into_parts();
            self.scanned_rows
                .fetch_add(chunk.len() as u64, Ordering::Relaxed);
            Ok(ScanChunk::new(chunk, chunk_has_more))
        })
    }
}

#[tokio::test]
async fn lix_file_history_point_lookup_does_not_rescan_unrelated_observed_state() {
    const UNRELATED_FILE_COUNT: usize = 64;
    const UNRELATED_DIRECTORY_COUNT: usize = 32;
    const UNRELATED_ADDITIONAL_FILE_COUNT: usize = 16;
    // Event provenance still walks the commit's change refs. The observed-root
    // reconstruction must not load the unrelated descriptor/blob/directory,
    // or other unrelated file rows a second time.
    // The total point reads include the authenticated commit/member-page
    // closure required by the canonical history authority. Bound that
    // envelope separately from the state-tree proof, whose scan/row counters
    // below must remain independent of unrelated rows.
    const MAX_AUTHENTICATED_HISTORY_KEYS: u64 = 16_384;
    const MAX_SCAN_CALLS: u64 = 128;

    let storage = CountingStorage::default();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("engine should open");
    let session = engine.open_session().await.expect("session should open");

    let unrelated_values = (0..UNRELATED_FILE_COUNT)
        .map(|index| {
            format!(
                "('01940000-0000-7000-8000-{index:012x}', '/unrelated-history-{index:03}.txt', CAST('x' AS BYTEA))"
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute(
            &format!("INSERT INTO lix_file (id, path, content) VALUES {unrelated_values}"),
            &[],
        )
        .await
        .expect("unrelated files should insert in one commit");
    let unrelated_directories = (0..UNRELATED_DIRECTORY_COUNT)
        .map(|index| {
            format!("('01940001-0000-7000-8000-{index:012x}', '/unrelated-directory-{index:03}')")
        })
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute(
            &format!("INSERT INTO lix_directory (id, path) VALUES {unrelated_directories}"),
            &[],
        )
        .await
        .expect("unrelated directories should insert in one commit");
    let unrelated_additional_files = (0..UNRELATED_ADDITIONAL_FILE_COUNT)
        .map(|index| {
            format!(
                "('01940002-0000-7000-8000-{index:012x}', '/unrelated-additional-{index:03}.bin', CAST('x' AS BYTEA))"
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute(
            &format!(
                "INSERT INTO lix_file (id, path, content) VALUES {unrelated_additional_files}"
            ),
            &[],
        )
        .await
        .expect("unrelated additional files should insert in one commit");
    session
        .execute(
            "INSERT INTO lix_file (id, path, content) \
             VALUES ('3faa577b-02e3-7c30-8b7d-30a9698cba93', '/3faa577b-02e3-7c30-8b7d-30a9698cba93.txt', CAST('target' AS BYTEA))",
            &[],
        )
        .await
        .expect("target file should insert in its own commit");
    let commit_id_rows = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("target commit head should load");
    let [Value::Text(commit_id)] = commit_id_rows.rows()[0].values() else {
        panic!(
            "expected active branch commit id row, got {:?}",
            commit_id_rows.rows()[0].values()
        );
    };

    storage.reset_counters();
    let result = session
        .execute(
            &format!(
                "SELECT id, path \
                 FROM lix_file_history('{commit_id}') \
                   WHERE lixcol_depth = 0 \
                   AND id = '3faa577b-02e3-7c30-8b7d-30a9698cba93'"
            ),
            &[],
        )
        .await
        .expect("point-routed file history should load");

    assert_rows_eq(
        result,
        vec![vec![
            Value::Text("3faa577b-02e3-7c30-8b7d-30a9698cba93".to_string()),
            Value::Text("/3faa577b-02e3-7c30-8b7d-30a9698cba93.txt".to_string()),
        ]],
    );
    let (requested_keys, scan_calls, scanned_rows) = storage.counters();
    assert!(
        requested_keys <= MAX_AUTHENTICATED_HISTORY_KEYS,
        "point-routed history exceeded its authenticated history-read budget: \
         {requested_keys} keys with {UNRELATED_FILE_COUNT} unrelated files, \
         {UNRELATED_DIRECTORY_COUNT} directories, and {UNRELATED_ADDITIONAL_FILE_COUNT} \
         additional files; expected at most {MAX_AUTHENTICATED_HISTORY_KEYS}"
    );
    assert!(
        scan_calls <= MAX_SCAN_CALLS,
        "point-routed history performed {scan_calls} scans; \
         expected at most {MAX_SCAN_CALLS}"
    );
    // Point-routed file history resolves entirely through `get_many`: it opens
    // scan cursors but never draws a row from one. An upper bound on rows is
    // therefore vacuous here — it cannot fail, so it reads as coverage without
    // being any. Asserting the exact zero makes it a real guard that fails the
    // moment this path starts serving rows out of a scan.
    assert_eq!(
        scanned_rows, 0,
        "point-routed history drew {scanned_rows} rows from scan cursors; \
         this path is expected to read exclusively through get_many"
    );

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn lix_file_history_path_lookup_does_not_rescan_unrelated_observed_state() {
    const UNRELATED_FILE_COUNT: usize = 64;
    const UNRELATED_DIRECTORY_COUNT: usize = 32;
    const UNRELATED_ADDITIONAL_FILE_COUNT: usize = 16;
    // A `path` predicate identifies one file exactly as an `id` predicate does,
    // so it must cost the same: the unrelated descriptors, directories and
    // blobs must not be reconstructed once per observed commit.
    const MAX_REQUESTED_KEYS: u64 = 416;
    const MAX_SCAN_CALLS: u64 = 128;

    let storage = CountingStorage::default();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("engine should open");
    let session = engine.open_session().await.expect("session should open");

    let unrelated_values = (0..UNRELATED_FILE_COUNT)
        .map(|index| {
            format!(
                "('01940000-0000-7000-8000-{index:012x}', '/unrelated-history-{index:03}.txt', CAST('x' AS BYTEA))"
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute(
            &format!("INSERT INTO lix_file (id, path, content) VALUES {unrelated_values}"),
            &[],
        )
        .await
        .expect("unrelated files should insert in one commit");
    let unrelated_directories = (0..UNRELATED_DIRECTORY_COUNT)
        .map(|index| {
            format!("('01940001-0000-7000-8000-{index:012x}', '/unrelated-directory-{index:03}')")
        })
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute(
            &format!("INSERT INTO lix_directory (id, path) VALUES {unrelated_directories}"),
            &[],
        )
        .await
        .expect("unrelated directories should insert in one commit");
    let unrelated_additional_files = (0..UNRELATED_ADDITIONAL_FILE_COUNT)
        .map(|index| {
            format!(
                "('01940002-0000-7000-8000-{index:012x}', '/unrelated-additional-{index:03}.bin', CAST('x' AS BYTEA))"
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute(
            &format!(
                "INSERT INTO lix_file (id, path, content) VALUES {unrelated_additional_files}"
            ),
            &[],
        )
        .await
        .expect("unrelated additional files should insert in one commit");
    session
        .execute(
            "INSERT INTO lix_file (id, path, content) \
             VALUES ('3faa577b-02e3-7c30-8b7d-30a9698cba93', '/target-by-path.txt', CAST('target' AS BYTEA))",
            &[],
        )
        .await
        .expect("target file should insert in its own commit");
    let commit_id_rows = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("target commit head should load");
    let [Value::Text(commit_id)] = commit_id_rows.rows()[0].values() else {
        panic!(
            "expected active branch commit id row, got {:?}",
            commit_id_rows.rows()[0].values()
        );
    };

    storage.reset_counters();
    let result = session
        .execute(
            &format!(
                "SELECT id, path \
                 FROM lix_file_history('{commit_id}') \
                   WHERE lixcol_depth = 0 \
                   AND path = '/target-by-path.txt'"
            ),
            &[],
        )
        .await
        .expect("path-routed file history should load");

    assert_rows_eq(
        result,
        vec![vec![
            Value::Text("3faa577b-02e3-7c30-8b7d-30a9698cba93".to_string()),
            Value::Text("/target-by-path.txt".to_string()),
        ]],
    );
    let (requested_keys, scan_calls, scanned_rows) = storage.counters();
    assert!(
        requested_keys <= MAX_REQUESTED_KEYS,
        "path-routed history requested {requested_keys} storage keys with \
         {UNRELATED_FILE_COUNT} unrelated files, {UNRELATED_DIRECTORY_COUNT} directories, and \
         {UNRELATED_ADDITIONAL_FILE_COUNT} additional files; expected at most {MAX_REQUESTED_KEYS}"
    );
    assert!(
        scan_calls <= MAX_SCAN_CALLS,
        "path-routed history performed {scan_calls} scans; \
         expected at most {MAX_SCAN_CALLS}"
    );
    // See the point-routed guard: a row upper bound cannot fail on this path,
    // so assert the exact zero instead.
    assert_eq!(
        scanned_rows, 0,
        "path-routed history drew {scanned_rows} rows from scan cursors; \
         this path is expected to read exclusively through get_many"
    );

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn lix_file_history_path_filter_equals_unfiltered_scan_over_many_files() {
    const BULK_FILE_COUNT: usize = 40;

    let storage = Memory::default();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("engine should open");
    let session = engine.open_session().await.expect("session should open");

    let bulk_values = (0..BULK_FILE_COUNT)
        .map(|index| format!("('/bulk/f{index:03}.txt', CAST('bulk' AS BYTEA))"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute(
            &format!("INSERT INTO lix_file (path, content) VALUES {bulk_values}"),
            &[],
        )
        .await
        .expect("bulk files should insert");
    // Same basename in two directories: the name-derived candidate set must not
    // be mistaken for the answer.
    for path in ["/bulk/target.txt", "/other/target.txt"] {
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, CAST('v0' AS BYTEA))",
                &[Value::Text(path.to_string())],
            )
            .await
            .expect("target file should insert");
    }
    for revision in 1..4u32 {
        session
            .execute(
                "UPDATE lix_file SET content = $1 WHERE path = '/bulk/target.txt'",
                &[Value::Blob(format!("v{revision}").into_bytes().into())],
            )
            .await
            .expect("target revision should commit");
    }
    session
        .execute(
            "UPDATE lix_file SET name = 'renamed.txt' WHERE path = '/bulk/target.txt'",
            &[],
        )
        .await
        .expect("target rename should commit");
    session
        .execute("DELETE FROM lix_file WHERE path = '/bulk/f005.txt'", &[])
        .await
        .expect("bulk delete should commit");
    let directory_id: String = session
        .execute("SELECT id FROM lix_directory WHERE path = '/bulk'", &[])
        .await
        .expect("bulk directory should load")
        .rows()[0]
        .get("id")
        .expect("directory id should be text");
    session
        .execute(
            "UPDATE lix_directory SET name = 'bulk-renamed' WHERE id = $1",
            &[Value::Text(directory_id)],
        )
        .await
        .expect("directory rename should commit");

    let head: String = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("head should load")
        .rows()[0]
        .get("commit_id")
        .expect("head should be text");

    let projection = "SELECT id, path, lixcol_depth, lixcol_observed_commit_id \
                      FROM lix_file_history($1)";
    let unfiltered = session
        .execute(projection, &[Value::Text(head.clone())])
        .await
        .expect("unfiltered history should load");
    let row_key = |row: &lix::Row| {
        let id: String = row.get("id").expect("id");
        let depth: i64 = row.get("lixcol_depth").expect("depth");
        let observed: String = row
            .get("lixcol_observed_commit_id")
            .expect("observed commit");
        let path = match row.value("path").expect("path column") {
            Value::Text(path) => path.clone(),
            _ => String::new(),
        };
        format!("{id}|{depth}|{observed}|{path}")
    };

    let probes = [
        "/bulk/target.txt",
        "/bulk/renamed.txt",
        "/bulk-renamed/renamed.txt",
        "/other/target.txt",
        "/bulk/f005.txt",
        "/bulk/f012.txt",
        "/bulk-renamed/f012.txt",
        "/never/existed.txt",
    ];
    let mut matched_any = false;
    for probe in probes {
        let expected = unfiltered
            .rows()
            .iter()
            .filter(|row| match row.value("path").expect("path column") {
                Value::Text(path) => path == probe,
                _ => false,
            })
            .map(row_key)
            .collect::<BTreeSet<_>>();
        let actual = session
            .execute(
                &format!("{projection} WHERE path = $2"),
                &[Value::Text(head.clone()), Value::Text(probe.to_string())],
            )
            .await
            .expect("path-filtered history should load")
            .rows()
            .iter()
            .map(row_key)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            expected, actual,
            "path pushdown changed the result set for '{probe}'"
        );
        matched_any |= !expected.is_empty();
    }
    assert!(
        matched_any,
        "the probe corpus must produce at least one matching history row"
    );

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn lix_file_history_ancestor_point_lookup_keeps_parent_evidence_bounded() {
    const UNRELATED_DIRECTORY_COUNT: usize = 256;
    const MAX_AUTHENTICATED_HISTORY_KEYS: u64 = 16_384;
    const MAX_SCAN_CALLS: u64 = 48;

    let storage = CountingStorage::default();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("engine should open");
    let session = engine.open_session().await.expect("session should open");

    let unrelated_directories = (0..UNRELATED_DIRECTORY_COUNT)
        .map(|index| {
            format!("('01940003-0000-7000-8000-{index:012x}', '/ancestor-noise-{index:03}')")
        })
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute(
            &format!("INSERT INTO lix_directory (id, path) VALUES {unrelated_directories}"),
            &[],
        )
        .await
        .expect("unrelated directories should insert");
    session
        .execute(
            "INSERT INTO lix_directory (id, path) VALUES \
             ('863f406b-3ce8-724d-8548-6dc1e41d451d', '/bounded'), \
             ('2b6a56e8-13dc-763d-8686-3f21011153ed', '/bounded/child')",
            &[],
        )
        .await
        .expect("target ancestors should insert");
    session
        .execute(
            "INSERT INTO lix_file (id, path, content) \
             VALUES ('626f756e-6465-842d-8669-6c6500000000', '/bounded/child/target.txt', CAST('x' AS BYTEA))",
            &[],
        )
        .await
        .expect("target file should insert");
    session
        .execute(
            "UPDATE lix_directory SET name = 'renamed' WHERE id = '863f406b-3ce8-724d-8548-6dc1e41d451d'",
            &[],
        )
        .await
        .expect("target ancestor should rename");
    let commit_rows = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("renamed head should load");
    let [Value::Text(commit_id)] = commit_rows.rows()[0].values() else {
        panic!("renamed head should be text");
    };

    storage.reset_counters();
    let result = session
        .execute(
            &format!(
                "SELECT path, lixcol_source_changes \
                 FROM lix_file_history('{commit_id}') \
                   WHERE lixcol_depth = 0 \
                   AND id = '626f756e-6465-842d-8669-6c6500000000'"
            ),
            &[],
        )
        .await
        .expect("ancestor-projected point history should load");

    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0].get::<Value>("path").unwrap(),
        Value::Text("/renamed/child/target.txt".to_string())
    );
    let Value::Json(sources) = result.rows()[0]
        .get::<Value>("lixcol_source_changes")
        .unwrap()
    else {
        panic!("ancestor source changes should be JSON");
    };
    let sources = sources.to_value();
    assert_eq!(
        sources[0]["entity_pk"],
        json!(["863f406b-3ce8-724d-8548-6dc1e41d451d"])
    );

    let (requested_keys, scan_calls, scanned_rows) = storage.counters();
    assert!(
        requested_keys <= MAX_AUTHENTICATED_HISTORY_KEYS,
        "ancestor point history exceeded its authenticated history-read budget: \
         {requested_keys} keys with {UNRELATED_DIRECTORY_COUNT} unrelated directories; \
         expected at most {MAX_AUTHENTICATED_HISTORY_KEYS}"
    );
    assert!(
        scan_calls <= MAX_SCAN_CALLS,
        "ancestor point history performed {scan_calls} scans; \
         expected at most {MAX_SCAN_CALLS}"
    );
    // See the point-routed guard: a row upper bound cannot fail on this path,
    // so assert the exact zero instead.
    assert_eq!(
        scanned_rows, 0,
        "ancestor point history drew {scanned_rows} rows from scan cursors; \
         this path is expected to read exclusively through get_many"
    );

    session.close().await.expect("session should close");
}

simulation_test!(
    lix_filesystem_history_propagates_nested_ancestor_rename_and_move,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES \
                 ('7813628c-6493-7241-80fe-c63337c5d3f9', '/workspace'), \
                 ('e93d7695-7bde-7b9c-8fa1-e84cc0642112', '/workspace/docs'), \
                 ('f6105c07-3ce2-7baf-884d-30da343db297', '/workspace/docs/guides'), \
                 ('9ddb5236-2a85-74a2-8ae1-1d34ac01b82f', '/destination')",
                &[],
            )
            .await
            .expect("nested projection directories should insert");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('70726f6a-6563-8469-8f6e-2d66696c6500', '/workspace/docs/guides/readme.md', CAST('x' AS BYTEA))",
                &[],
            )
            .await
            .expect("nested projection file should insert");

        session
            .execute(
                "UPDATE lix_directory SET name = 'archive' WHERE id = '7813628c-6493-7241-80fe-c63337c5d3f9'",
                &[],
            )
            .await
            .expect("ancestor rename should succeed");
        let rename_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("rename head should load")
            .expect("rename head should exist");

        let renamed_file = session
            .execute(
                &format!(
                    "SELECT path, lixcol_source_changes \
                     FROM lix_file_history('{rename_commit_id}') \
                       WHERE lixcol_depth = 0 \
                       AND id = '70726f6a-6563-8469-8f6e-2d66696c6500'"
                ),
                &[],
            )
            .await
            .expect("renamed descendant file history should load");
        assert_eq!(renamed_file.len(), 1);
        assert_eq!(
            renamed_file.rows()[0].get::<Value>("path").unwrap(),
            Value::Text("/archive/docs/guides/readme.md".to_string())
        );
        let Value::Json(rename_sources) = renamed_file.rows()[0]
            .get::<Value>("lixcol_source_changes")
            .unwrap()
        else {
            panic!("rename sources should be JSON");
        };
        let rename_sources = rename_sources.to_value();
        assert_eq!(rename_sources.as_array().map(Vec::len), Some(1));
        assert_eq!(
            rename_sources[0]["entity_pk"],
            json!(["7813628c-6493-7241-80fe-c63337c5d3f9"])
        );

        let renamed_directory = session
            .execute(
                &format!(
                    "SELECT path, lixcol_source_changes \
                     FROM lix_directory_history('{rename_commit_id}') \
                       WHERE lixcol_depth = 0 \
                       AND id = 'f6105c07-3ce2-7baf-884d-30da343db297'"
                ),
                &[],
            )
            .await
            .expect("renamed descendant directory history should load");
        assert_eq!(renamed_directory.len(), 1);
        assert_eq!(
            renamed_directory.rows()[0].get::<Value>("path").unwrap(),
            Value::Text("/archive/docs/guides".to_string())
        );

        session
            .execute(
                "UPDATE lix_directory \
                 SET path = '/destination/archive' \
                 WHERE id = '7813628c-6493-7241-80fe-c63337c5d3f9'",
                &[],
            )
            .await
            .expect("ancestor subtree move should succeed");
        let move_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("move head should load")
            .expect("move head should exist");

        let moved = session
            .execute(
                &format!(
                    "SELECT path FROM lix_file_history('{move_commit_id}') \
                       WHERE lixcol_depth = 0 \
                       AND id = '70726f6a-6563-8469-8f6e-2d66696c6500'"
                ),
                &[],
            )
            .await
            .expect("moved descendant file history should load");
        assert_rows_eq(
            moved,
            vec![vec![Value::Text(
                "/destination/archive/docs/guides/readme.md".to_string(),
            )]],
        );
    }
);

simulation_test!(
    lix_filesystem_history_groups_same_commit_ancestor_sources,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES \
                 ('945ddc79-6ca8-7a97-8ece-ecbde7f1358e', '/grouped'), \
                 ('46d97a70-d3ec-7d27-8b28-8c62f72869dd', '/grouped/child')",
                &[],
            )
            .await
            .expect("grouped directories should insert");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('67726f75-7065-842d-8669-6c6500000000', '/grouped/child/file.txt', CAST('x' AS BYTEA))",
                &[],
            )
            .await
            .expect("grouped file should insert");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("grouped transaction should begin");
        transaction
            .execute(
                "UPDATE lix_directory SET name = 'renamed-root' WHERE id = '945ddc79-6ca8-7a97-8ece-ecbde7f1358e'",
                &[],
            )
            .await
            .expect("root rename should stage");
        transaction
            .execute(
                "UPDATE lix_directory SET name = 'renamed-child' WHERE id = '46d97a70-d3ec-7d27-8b28-8c62f72869dd'",
                &[],
            )
            .await
            .expect("child rename should stage");
        transaction
            .execute(
                "UPDATE lix_file SET name = 'renamed.txt' WHERE id = '67726f75-7065-842d-8669-6c6500000000'",
                &[],
            )
            .await
            .expect("file rename should stage");
        transaction
            .commit()
            .await
            .expect("grouped transaction should commit");
        let commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("grouped head should load")
            .expect("grouped head should exist");

        let file_row = session
            .execute(
                &format!(
                    "SELECT path, lixcol_source_changes \
                     FROM lix_file_history('{commit_id}') \
                       WHERE lixcol_depth = 0 \
                       AND id = '67726f75-7065-842d-8669-6c6500000000'"
                ),
                &[],
            )
            .await
            .expect("grouped file history should load");
        assert_eq!(file_row.len(), 1);
        assert_eq!(
            file_row.rows()[0].get::<Value>("path").unwrap(),
            Value::Text("/renamed-root/renamed-child/renamed.txt".to_string())
        );
        let Value::Json(file_sources) = file_row.rows()[0]
            .get::<Value>("lixcol_source_changes")
            .unwrap()
        else {
            panic!("grouped file sources should be JSON");
        };
        let file_sources = file_sources.to_value();
        let source_ids = file_sources
            .as_array()
            .expect("grouped file sources should be an array")
            .iter()
            .map(|source| source["entity_pk"][0].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            source_ids,
            BTreeSet::from([
                "945ddc79-6ca8-7a97-8ece-ecbde7f1358e",
                "46d97a70-d3ec-7d27-8b28-8c62f72869dd",
                "67726f75-7065-842d-8669-6c6500000000"
            ])
        );

        let directory_row = session
            .execute(
                &format!(
                    "SELECT path, lixcol_source_changes \
                     FROM lix_directory_history('{commit_id}') \
                       WHERE lixcol_depth = 0 \
                       AND id = '46d97a70-d3ec-7d27-8b28-8c62f72869dd'"
                ),
                &[],
            )
            .await
            .expect("grouped directory history should load");
        assert_eq!(directory_row.len(), 1);
        let Value::Json(directory_sources) = directory_row.rows()[0]
            .get::<Value>("lixcol_source_changes")
            .unwrap()
        else {
            panic!("grouped directory sources should be JSON");
        };
        let directory_sources = directory_sources.to_value();
        assert_eq!(directory_sources.as_array().map(Vec::len), Some(2));
    }
);

simulation_test!(
    lix_filesystem_history_preserves_ancestor_sibling_revisions,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_directory (id, path) VALUES \
             ('7afd096d-2680-7fd4-8467-c866b7474f8d', '/before'), \
             ('dd2d37ad-1bd0-7592-855e-9fcd50a55e1a', '/before/child')",
            &[],
        )
        .await
        .expect("sibling directories should insert");
        main.execute(
            "INSERT INTO lix_file (id, path, content) \
             VALUES ('616e6365-7374-8f72-8d73-69626c696e00', '/before/child/file.txt', CAST('x' AS BYTEA))",
            &[],
        )
        .await
        .expect("sibling file should insert");
        main.create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-00000000000b".to_string()),
            name: "Ancestor sibling draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("sibling branch should create");
        let draft = sim.wrap_session(
            engine
                .open_session_at("01930000-0000-7000-8000-00000000000b")
                .await
                .expect("draft session should open"),
            &engine,
        );

        main.execute(
            "UPDATE lix_directory SET name = 'same' WHERE id = '7afd096d-2680-7fd4-8467-c866b7474f8d'",
            &[],
        )
        .await
        .expect("main ancestor rename should succeed");
        let main_sibling = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main sibling should load")
            .expect("main sibling should exist");
        draft
            .execute(
                "UPDATE lix_directory SET name = 'same' WHERE id = '7afd096d-2680-7fd4-8467-c866b7474f8d'",
                &[],
            )
            .await
            .expect("draft ancestor rename should succeed");
        let draft_sibling = engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-00000000000b")
            .await
            .expect("draft sibling should load")
            .expect("draft sibling should exist");
        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-00000000000b".to_string(),
            })
            .await
            .expect("convergent ancestor renames should merge");
        let merge_commit_id = receipt
            .created_merge_commit_id
            .expect("convergent ancestor renames should create a merge commit");

        let rows = main
            .execute(
                &format!(
                    "SELECT path, lixcol_observed_commit_id \
                     FROM lix_file_history('{merge_commit_id}') \
                       WHERE lixcol_depth = 1 \
                       AND id = '616e6365-7374-8f72-8d73-69626c696e00' \
                     ORDER BY lixcol_observed_commit_id"
                ),
                &[],
            )
            .await
            .expect("sibling descendant history should load");
        assert_eq!(rows.len(), 2);
        let mut actual_commits = rows
            .rows()
            .iter()
            .map(|row| {
                assert_eq!(
                    row.get::<Value>("path").unwrap(),
                    Value::Text("/same/child/file.txt".to_string())
                );
                match row.get::<Value>("lixcol_observed_commit_id").unwrap() {
                    Value::Text(commit_id) => commit_id,
                    value => panic!("observed commit should be text, got {value:?}"),
                }
            })
            .collect::<Vec<_>>();
        actual_commits.sort();
        let mut expected_commits = vec![main_sibling, draft_sibling];
        expected_commits.sort();
        assert_eq!(actual_commits, expected_commits);
    }
);

simulation_test!(
    lix_filesystem_history_attributes_recursive_delete_and_restore_to_ancestors,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES \
                 ('e800ebc8-3b94-759f-8aa3-07fcaadc46a3', '/restore'), \
                 ('262b5268-af6a-7225-8de7-5619a47c547a', '/restore/child')",
                &[],
            )
            .await
            .expect("restore directories should insert");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('72657374-6f72-852d-8669-6c6500000000', '/restore/child/file.txt', CAST('x' AS BYTEA))",
                &[],
            )
            .await
            .expect("restore file should insert");
        session
            .execute(
                "DELETE FROM lix_directory WHERE id = 'e800ebc8-3b94-759f-8aa3-07fcaadc46a3'",
                &[],
            )
            .await
            .expect("recursive delete should succeed");
        let delete_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("delete head should load")
            .expect("delete head should exist");

        let deleted = session
            .execute(
                &format!(
                    "SELECT path, lixcol_source_changes \
                     FROM lix_file_history('{delete_commit_id}') \
                       WHERE lixcol_depth = 0 \
                       AND id = '72657374-6f72-852d-8669-6c6500000000'"
                ),
                &[],
            )
            .await
            .expect("deleted descendant file history should load");
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted.rows()[0].get::<Value>("path").unwrap(), Value::Null);
        let Value::Json(delete_sources) = deleted.rows()[0]
            .get::<Value>("lixcol_source_changes")
            .unwrap()
        else {
            panic!("delete sources should be JSON");
        };
        let delete_sources = delete_sources.to_value();
        let deleted_directory_ids = delete_sources
            .as_array()
            .expect("delete sources should be an array")
            .iter()
            .filter(|source| source["schema_key"] == json!("lix_directory_descriptor"))
            .map(|source| source["entity_pk"][0].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            deleted_directory_ids,
            BTreeSet::from([
                "e800ebc8-3b94-759f-8aa3-07fcaadc46a3",
                "262b5268-af6a-7225-8de7-5619a47c547a"
            ])
        );

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("restore transaction should begin");
        transaction
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES \
                 ('e800ebc8-3b94-759f-8aa3-07fcaadc46a3', '/restored'), \
                 ('262b5268-af6a-7225-8de7-5619a47c547a', '/restored/child')",
                &[],
            )
            .await
            .expect("directories should restore");
        transaction
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('72657374-6f72-852d-8669-6c6500000000', '/restored/child/file.txt', CAST('y' AS BYTEA))",
                &[],
            )
            .await
            .expect("file should restore");
        transaction
            .commit()
            .await
            .expect("restore transaction should commit");
        let restore_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("restore head should load")
            .expect("restore head should exist");
        let restored = session
            .execute(
                &format!(
                    "SELECT path, content, lixcol_source_changes \
                     FROM lix_file_history('{restore_commit_id}') \
                       WHERE lixcol_depth = 0 \
                       AND id = '72657374-6f72-852d-8669-6c6500000000'"
                ),
                &[],
            )
            .await
            .expect("restored descendant file history should load");
        assert_eq!(restored.len(), 1);
        assert_eq!(
            restored.rows()[0].get::<Value>("path").unwrap(),
            Value::Text("/restored/child/file.txt".to_string())
        );
        assert_eq!(
            restored.rows()[0].get::<Value>("content").unwrap(),
            Value::Blob(b"y".to_vec().into())
        );
        let Value::Json(restore_sources) = restored.rows()[0]
            .get::<Value>("lixcol_source_changes")
            .unwrap()
        else {
            panic!("restore sources should be JSON");
        };
        let restore_sources = restore_sources.to_value();
        let restored_directory_ids = restore_sources
            .as_array()
            .expect("restore sources should be an array")
            .iter()
            .filter(|source| source["schema_key"] == json!("lix_directory_descriptor"))
            .map(|source| source["entity_pk"][0].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            restored_directory_ids,
            BTreeSet::from([
                "e800ebc8-3b94-759f-8aa3-07fcaadc46a3",
                "262b5268-af6a-7225-8de7-5619a47c547a"
            ])
        );
    }
);

simulation_test!(
    lix_file_history_reads_path_and_content_from_commit_graph,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('68697374-6f72-892d-8669-6c6500000000', '/docs/guides/readme.md', CAST('hello' AS BYTEA))",
                &[],
            )
            .await
            .expect("file insert should succeed");
        let first_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("first file commit head should load")
            .expect("first file commit head should exist");

        session
            .execute(
                "UPDATE lix_file \
                 SET path = '/docs/readme-renamed.md' \
                 WHERE id = '68697374-6f72-892d-8669-6c6500000000'",
                &[],
            )
            .await
            .expect("file path update should succeed");
        let second_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("second file commit head should load")
            .expect("second file commit head should exist");

        assert_ne!(first_commit_id, second_commit_id);

        let result = session
            .execute(
                "SELECT id, path, name, content, lixcol_depth \
                 FROM lix_file_history($1) \
                   WHERE id = $2 \
                   AND path LIKE $3 \
                 ORDER BY lixcol_depth",
                &[
                    Value::Text(second_commit_id.clone()),
                    Value::Text("68697374-6f72-892d-8669-6c6500000000".to_string()),
                    Value::Text("/docs/%".to_string()),
                ],
            )
            .await
            .expect("file history read should succeed");
        assert!(
            result.notices().is_empty(),
            "ordinary path predicates should not emit identity heuristics"
        );

        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("68697374-6f72-892d-8669-6c6500000000".to_string()),
                    Value::Text("/docs/readme-renamed.md".to_string()),
                    Value::Text("readme-renamed.md".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                    Value::Integer(0),
                ],
                vec![
                    Value::Text("68697374-6f72-892d-8669-6c6500000000".to_string()),
                    Value::Text("/docs/guides/readme.md".to_string()),
                    Value::Text("readme.md".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                    Value::Integer(1),
                ],
            ],
        );

        let old_path_result = session
            .execute(
                "SELECT id, path, lixcol_depth \
                 FROM lix_file_history($1) \
                   WHERE path = '/docs/guides/readme.md' \
                 ORDER BY lixcol_depth",
                &[Value::Text(second_commit_id.clone())],
            )
            .await
            .expect("historical path predicate should execute");
        assert_rows_eq(
            old_path_result,
            vec![vec![
                Value::Text("68697374-6f72-892d-8669-6c6500000000".to_string()),
                Value::Text("/docs/guides/readme.md".to_string()),
                Value::Integer(1),
            ]],
        );

        let source_changes_result = session
            .execute(
                &format!(
                    "SELECT lixcol_source_changes \
                     FROM lix_file_history('{second_commit_id}') \
                       WHERE id = '68697374-6f72-892d-8669-6c6500000000' \
                       AND lixcol_depth = 0"
                ),
                &[],
            )
            .await
            .expect("file history source changes should be selectable");
        let source_changes = source_changes_result.rows()[0]
            .get::<Value>("lixcol_source_changes")
            .expect("source_changes should be present");
        let Value::Json(source_changes) = source_changes else {
            panic!("source_changes should be semantic JSON, got {source_changes:?}");
        };
        let source_changes = source_changes.to_value();
        assert_eq!(source_changes.as_array().map(Vec::len), Some(1));
        assert_eq!(
            source_changes[0]["schema_key"],
            json!("lix_file_descriptor")
        );
        assert_eq!(
            source_changes[0]["snapshot_content"]["name"],
            json!("readme-renamed.md")
        );

        let result = session
            .execute(
                &format!(
                    "SELECT id \
                     FROM lix_file_history('{first_commit_id}') \
                       WHERE path LIKE '/missing/%'"
                ),
                &[],
            )
            .await
            .expect("file history should route the as-of commit and leave path LIKE as residual");
        assert_rows_eq(result, Vec::<Vec<Value>>::new());
    }
);

simulation_test!(
    lix_file_history_treats_path_only_file_as_empty,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (path) VALUES ('/empty-history.txt')",
                &[],
            )
            .await
            .expect("path-only file insert should succeed");
        let commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("file commit head should load")
            .expect("file commit head should exist");

        let result = session
            .execute(
                &format!(
                    "SELECT path, content \
                     FROM lix_file_history('{commit_id}') \
                       WHERE path = '/empty-history.txt' \
                       AND lixcol_depth = 0"
                ),
                &[],
            )
            .await
            .expect("file history read should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("/empty-history.txt".to_string()),
                Value::Blob(Vec::new().into()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_history_preserves_equal_depth_siblings_in_a_diamond,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_file (id, path, content) \
             VALUES ('6469616d-6f6e-842d-8669-6c6500000000', '/before.md', CAST('base' AS BYTEA))",
            &[],
        )
        .await
        .expect("base file should insert");
        main.create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-00000000000c".to_string()),
            name: "Diamond draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created");
        let draft = sim.wrap_session(
            engine
                .open_session_at("01930000-0000-7000-8000-00000000000c")
                .await
                .expect("draft session should open"),
            &engine,
        );

        main.execute(
            "UPDATE lix_file SET path = '/same.md' WHERE id = '6469616d-6f6e-842d-8669-6c6500000000'",
            &[],
        )
        .await
        .expect("main path update should succeed");
        let main_sibling = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main sibling should load")
            .expect("main sibling should exist");
        draft
            .execute(
                "UPDATE lix_file SET path = '/same.md' WHERE id = '6469616d-6f6e-842d-8669-6c6500000000'",
                &[],
            )
            .await
            .expect("draft path update should succeed");
        let draft_sibling = engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-00000000000c")
            .await
            .expect("draft sibling should load")
            .expect("draft sibling should exist");

        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-00000000000c".to_string(),
            })
            .await
            .expect("convergent sibling updates should merge");
        let merge_commit_id = receipt
            .created_merge_commit_id
            .expect("convergent sibling updates should create an empty merge commit");

        let result = main
            .execute(
                &format!(
                    "SELECT path, lixcol_observed_commit_id, lixcol_depth, lixcol_source_changes \
                     FROM lix_file_history('{merge_commit_id}') \
                       WHERE id = '6469616d-6f6e-842d-8669-6c6500000000' \
                       AND lixcol_depth = 1 \
                     ORDER BY lixcol_observed_commit_id"
                ),
                &[],
            )
            .await
            .expect("diamond history should load");

        assert_eq!(
            result.len(),
            2,
            "both equal-depth sibling revisions survive"
        );
        let mut observed = result
            .rows()
            .iter()
            .map(|row| {
                assert_eq!(
                    row.get::<Value>("path").expect("path should decode"),
                    Value::Text("/same.md".to_string())
                );
                assert_eq!(
                    row.get::<Value>("lixcol_depth")
                        .expect("history depth should decode"),
                    Value::Integer(1)
                );
                let source_changes = row
                    .get::<Value>("lixcol_source_changes")
                    .expect("source changes should exist");
                let Value::Json(source_changes) = source_changes else {
                    panic!("source changes should be JSON, got {source_changes:?}");
                };
                let source_changes = source_changes.to_value();
                assert_eq!(source_changes.as_array().map(Vec::len), Some(1));
                match row
                    .get::<Value>("lixcol_observed_commit_id")
                    .expect("observed commit should exist")
                {
                    Value::Text(commit_id) => commit_id,
                    value => panic!("observed commit should be text, got {value:?}"),
                }
            })
            .collect::<Vec<_>>();
        observed.sort();
        let mut expected = vec![main_sibling, draft_sibling];
        expected.sort();
        assert_eq!(observed, expected);
    }
);

simulation_test!(
    joined_history_filters_keep_relation_local_sql_semantics,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('68697374-6f72-892d-8a6f-696e2d646900', '/joined')",
                &[],
            )
            .await
            .expect("directory insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('68697374-6f72-892d-8a6f-696e2d666900', '/joined/old.txt', CAST('one' AS BYTEA))",
                &[],
            )
            .await
            .expect("file insert should succeed");
        session
            .execute(
                "UPDATE lix_file \
                 SET path = '/joined/new.txt' \
                 WHERE id = '68697374-6f72-892d-8a6f-696e2d666900'",
                &[],
            )
            .await
            .expect("file rename should succeed");

        let result = session
            .execute(
                "SELECT file.id, file.path, directory.id \
                 FROM lix_file_history() AS file \
                 JOIN lix_directory_history() AS directory \
                   ON file.directory_id = directory.id \
                 WHERE file.path = '/joined/old.txt'",
                &[],
            )
            .await
            .expect("joined history query should succeed");

        assert!(
            result.notices().is_empty(),
            "join predicates must not be attributed to unrelated history relations"
        );
        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("68697374-6f72-892d-8a6f-696e2d666900".to_string()),
                Value::Text("/joined/old.txt".to_string()),
                Value::Text("68697374-6f72-892d-8a6f-696e2d646900".to_string()),
            ]],
        );
    }
);

simulation_test!(lix_file_history_reads_bound_id_in_list, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    session
        .execute(
            "INSERT INTO lix_file (id, path, content) VALUES \
                    ('01940000-0000-7000-8000-000000000004', '/history/in-a.txt', CAST('a' AS BYTEA)), \
                    ('01940000-0000-7000-8000-000000000005', '/history/in-b.txt', CAST('b' AS BYTEA))",
            &[],
        )
        .await
        .expect("file inserts should succeed");
    let commit_id = engine
        .load_branch_head_commit_id(sim.main_branch_id())
        .await
        .expect("file commit head should load")
        .expect("file commit head should exist");

    let result = session
        .execute(
            "SELECT id, path, content \
                 FROM lix_file_history($1) \
                   WHERE id IN ($2, $3) \
                 ORDER BY id",
            &[
                Value::Text(commit_id),
                Value::Text("01940000-0000-7000-8000-000000000005".to_string()),
                Value::Text("01940000-0000-7000-8000-000000000004".to_string()),
            ],
        )
        .await
        .expect("bound ID IN history read should succeed");

    assert_rows_eq(
        result,
        vec![
            vec![
                Value::Text("01940000-0000-7000-8000-000000000004".to_string()),
                Value::Text("/history/in-a.txt".to_string()),
                Value::Blob(b"a".to_vec().into()),
            ],
            vec![
                Value::Text("01940000-0000-7000-8000-000000000005".to_string()),
                Value::Text("/history/in-b.txt".to_string()),
                Value::Blob(b"b".to_vec().into()),
            ],
        ],
    );
});

simulation_test!(
    lix_file_history_limit_applies_after_sql_ordering,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('6161612d-6f6c-8465-822d-686973746f00', '/older.txt', CAST('older' AS BYTEA))",
                &[],
            )
            .await
            .expect("older file insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('7a7a7a2d-6e65-8765-822d-686973746f00', '/newer.txt', CAST('newer' AS BYTEA))",
                &[],
            )
            .await
            .expect("newer file insert should succeed");
        let commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("file commit head should load")
            .expect("file commit head should exist");

        let result = session
            .execute(
                &format!(
                    "SELECT id, path, lixcol_depth \
                     FROM lix_file_history('{commit_id}') \
                     ORDER BY lixcol_depth \
                     LIMIT 1"
                ),
                &[],
            )
            .await
            .expect("file history read should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("7a7a7a2d-6e65-8765-822d-686973746f00".to_string()),
                Value::Text("/newer.txt".to_string()),
                Value::Integer(0),
            ]],
        );
    }
);

simulation_test!(
    lix_file_history_limit_applies_after_residual_path_filters,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES \
                    ('a71ca839-a7d4-7529-899d-470f3e2d56eb', '/noise/one.txt', CAST('one' AS BYTEA)), \
                    ('4fa4f740-1d46-781f-87b7-8d6347ada462', '/noise/two.txt', CAST('two' AS BYTEA)), \
                    ('74242a12-7491-7df8-8cfc-0a484bbfd0cb', '/target/three.txt', CAST('three' AS BYTEA))",
                &[],
            )
            .await
            .expect("file inserts should succeed");
        let commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("file commit head should load")
            .expect("file commit head should exist");

        let result = session
            .execute(
                &format!(
                    "SELECT id, path, content \
                     FROM lix_file_history('{commit_id}') \
                       WHERE path LIKE '/target/%' \
                     LIMIT 1"
                ),
                &[],
            )
            .await
            .expect("file history read should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("74242a12-7491-7df8-8cfc-0a484bbfd0cb".to_string()),
                Value::Text("/target/three.txt".to_string()),
                Value::Blob(b"three".to_vec().into()),
            ]],
        );
    }
);

simulation_test!(lix_file_history_defaults_to_active_head, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    session
        .execute(
            "INSERT INTO lix_file (id, path, content) \
                 VALUES ('68697374-6f72-892d-8465-6661756c7401', '/history-default.txt', CAST('default' AS BYTEA))",
            &[],
        )
        .await
        .expect("file insert should succeed");
    let result = session
        .execute(
            "SELECT id, lixcol_depth \
                 FROM lix_file_history() \
                 WHERE id = '68697374-6f72-892d-8465-6661756c7401'",
            &[],
        )
        .await
        .expect("file history should default to the active head");

    assert_rows_eq(
        result,
        vec![vec![
            Value::Text("68697374-6f72-892d-8465-6661756c7401".to_string()),
            Value::Integer(0),
        ]],
    );
});

simulation_test!(
    lix_file_history_ignores_unrelated_file_scoped_state_events,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('6f726469-6e61-8279-8d68-6973746f7200', '/ordinary-history.txt', CAST('hello' AS BYTEA))",
                &[],
            )
            .await
            .expect("file insert should succeed");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_file_id) \
                 VALUES ('ordinary-sidecar', 'noise', '6f726469-6e61-8279-8d68-6973746f7200')",
                &[],
            )
            .await
            .expect("unrelated file-scoped state insert should succeed");
        let commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head commit should load")
            .expect("head commit should exist");

        let result = session
            .execute(
                &format!(
                    "SELECT path, content, lixcol_depth \
                     FROM lix_file_history('{commit_id}') \
                       WHERE id = '6f726469-6e61-8279-8d68-6973746f7200' \
                     ORDER BY lixcol_depth"
                ),
                &[],
            )
            .await
            .expect("file history read should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("/ordinary-history.txt".to_string()),
                Value::Blob(b"hello".to_vec().into()),
                Value::Integer(1),
            ]],
        );
    }
);

simulation_test!(
    lix_file_history_aggregates_composed_source_changes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, content) \
                 VALUES ('68697374-6f72-892d-8669-6c652d626c00', '/blob-filter.txt', CAST('blob' AS BYTEA))",
                &[],
            )
            .await
            .expect("file insert should succeed");
        session
            .execute(
                "UPDATE lix_file SET content = CAST('blob2' AS BYTEA) \
                 WHERE id = '68697374-6f72-892d-8669-6c652d626c00'",
                &[],
            )
            .await
            .expect("file data update should succeed");
        let commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("file commit head should load")
            .expect("file commit head should exist");

        let result = session
            .execute(
                &format!(
                    "SELECT id, path, content, lixcol_source_changes \
                     FROM lix_file_history('{commit_id}') \
                       WHERE id = '68697374-6f72-892d-8669-6c652d626c00' \
                     ORDER BY lixcol_depth"
                ),
                &[],
            )
            .await
            .expect("file history read should succeed");

        assert_eq!(result.len(), 2);
        let latest = result.rows()[0].values();
        assert_eq!(
            &latest[..3],
            &[
                Value::Text("68697374-6f72-892d-8669-6c652d626c00".to_string()),
                Value::Text("/blob-filter.txt".to_string()),
                Value::Blob(b"blob2".to_vec().into()),
            ]
        );
        let Value::Json(latest_sources) = &latest[3] else {
            panic!("latest source changes should be JSON, got {:?}", latest[3]);
        };
        let latest_sources = latest_sources.to_value();
        assert_eq!(latest_sources.as_array().map(Vec::len), Some(1));
        assert_eq!(
            latest_sources[0]["schema_key"],
            json!("lix_binary_blob_ref")
        );

        let Value::Json(initial_sources) = &result.rows()[1].values()[3] else {
            panic!(
                "initial source changes should be JSON, got {:?}",
                result.rows()[1].values()[3]
            );
        };
        let initial_sources = initial_sources.to_value();
        assert_eq!(initial_sources.as_array().map(Vec::len), Some(2));
        let source_schema_keys = initial_sources
            .as_array()
            .unwrap()
            .iter()
            .map(|source| source["schema_key"].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            source_schema_keys,
            BTreeSet::from(["lix_binary_blob_ref", "lix_file_descriptor",])
        );
        let source_ids = initial_sources
            .as_array()
            .unwrap()
            .iter()
            .map(|source| source["id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(
            source_ids.windows(2).all(|ids| ids[0] <= ids[1]),
            "source changes must be ordered by change id: {source_ids:?}"
        );
        for source in initial_sources.as_array().unwrap() {
            assert_eq!(
                source
                    .as_object()
                    .unwrap()
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>(),
                vec![
                    "created_at",
                    "entity_pk",
                    "file_id",
                    "id",
                    "metadata",
                    "origin_key",
                    "schema_key",
                    "snapshot_content",
                ],
                "source change objects must mirror the stable lix_change field set"
            );
        }

        for retired in [
            "lixcol_change_id",
            "lixcol_schema_key",
            "lixcol_origin_key",
            "lixcol_snapshot_content",
            "lixcol_metadata",
        ] {
            let error = session
                .execute(
                    &format!(
                        "SELECT {retired} \
                         FROM lix_file_history('{commit_id}')"
                    ),
                    &[],
                )
                .await
                .expect_err("composed history singular provenance must fail");
            assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);
        }
    }
);
