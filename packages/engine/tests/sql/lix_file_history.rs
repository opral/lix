use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use lix_engine::Value;
use lix_engine::{
    CreateBranchOptions, Engine, GetManyResult, GetOptions, Key, KeyRange, LixError, Memory,
    MemoryRead, MemoryWrite, MergeBranchOptions, ReadOptions, ScanChunk, ScanOptions, SpaceId,
    Storage, StorageError, StorageRead, WriteOptions,
};
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
        space: SpaceId,
        keys: &[Key],
        options: GetOptions,
    ) -> Result<GetManyResult, StorageError> {
        self.get_many_requested_keys
            .fetch_add(keys.len() as u64, Ordering::Relaxed);
        self.inner.get_many(space, keys, options).await
    }

    async fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        let chunk = self.inner.scan(space, range, options).await?;
        self.scan_calls.fetch_add(1, Ordering::Relaxed);
        self.scanned_rows
            .fetch_add(chunk.entries.len() as u64, Ordering::Relaxed);
        Ok(chunk)
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
    const MAX_REQUESTED_KEYS: u64 = 416;
    const MAX_SCAN_CALLS: u64 = 128;
    const MAX_SCANNED_ROWS: u64 = 512;

    let storage = CountingStorage::default();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    let unrelated_values = (0..UNRELATED_FILE_COUNT)
        .map(|index| {
            format!("('unrelated-history-{index:03}', '/unrelated-history-{index:03}.txt', X'78')")
        })
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute(
            &format!("INSERT INTO lix_file (id, path, data) VALUES {unrelated_values}"),
            &[],
        )
        .await
        .expect("unrelated files should insert in one commit");
    let unrelated_directories = (0..UNRELATED_DIRECTORY_COUNT)
        .map(|index| {
            format!("('unrelated-directory-{index:03}', '/unrelated-directory-{index:03}/')")
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
                "('unrelated-additional-{index:03}', '/unrelated-additional-{index:03}.bin', X'78')"
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute(
            &format!("INSERT INTO lix_file (id, path, data) VALUES {unrelated_additional_files}"),
            &[],
        )
        .await
        .expect("unrelated additional files should insert in one commit");
    session
        .execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('history-point-target', '/history-point-target.txt', X'746172676574')",
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
                 FROM lix_file_history \
                 WHERE lixcol_as_of_commit_id = '{commit_id}' \
                   AND lixcol_depth = 0 \
                   AND id = 'history-point-target'"
            ),
            &[],
        )
        .await
        .expect("point-routed file history should load");

    assert_rows_eq(
        result,
        vec![vec![
            Value::Text("history-point-target".to_string()),
            Value::Text("/history-point-target.txt".to_string()),
        ]],
    );
    let (requested_keys, scan_calls, scanned_rows) = storage.counters();
    assert!(
        requested_keys <= MAX_REQUESTED_KEYS,
        "point-routed history requested {requested_keys} storage keys with \
         {UNRELATED_FILE_COUNT} unrelated files, {UNRELATED_DIRECTORY_COUNT} directories, and \
         {UNRELATED_ADDITIONAL_FILE_COUNT} additional files; expected at most {MAX_REQUESTED_KEYS}"
    );
    assert!(
        scan_calls <= MAX_SCAN_CALLS && scanned_rows <= MAX_SCANNED_ROWS,
        "point-routed history performed {scan_calls} scans returning {scanned_rows} rows; \
         expected at most {MAX_SCAN_CALLS} scans and {MAX_SCANNED_ROWS} rows"
    );

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn lix_file_history_ancestor_point_lookup_keeps_parent_evidence_bounded() {
    const UNRELATED_DIRECTORY_COUNT: usize = 256;
    const MAX_REQUESTED_KEYS: u64 = 400;
    const MAX_SCAN_CALLS: u64 = 48;
    const MAX_SCANNED_ROWS: u64 = 48;

    let storage = CountingStorage::default();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    let unrelated_directories = (0..UNRELATED_DIRECTORY_COUNT)
        .map(|index| format!("('ancestor-noise-{index:03}', '/ancestor-noise-{index:03}/')"))
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
             ('bounded-root', '/bounded/'), \
             ('bounded-child', '/bounded/child/')",
            &[],
        )
        .await
        .expect("target ancestors should insert");
    session
        .execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('bounded-file', '/bounded/child/target.txt', X'78')",
            &[],
        )
        .await
        .expect("target file should insert");
    session
        .execute(
            "UPDATE lix_directory SET name = 'renamed' WHERE id = 'bounded-root'",
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
                 FROM lix_file_history \
                 WHERE lixcol_as_of_commit_id = '{commit_id}' \
                   AND lixcol_depth = 0 \
                   AND id = 'bounded-file'"
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
    assert_eq!(sources[0]["entity_pk"], json!(["bounded-root"]));

    let (requested_keys, scan_calls, scanned_rows) = storage.counters();
    assert!(
        requested_keys <= MAX_REQUESTED_KEYS,
        "ancestor point history requested {requested_keys} keys with \
         {UNRELATED_DIRECTORY_COUNT} unrelated directories; expected at most {MAX_REQUESTED_KEYS}"
    );
    assert!(
        scan_calls <= MAX_SCAN_CALLS && scanned_rows <= MAX_SCANNED_ROWS,
        "ancestor point history performed {scan_calls} scans returning {scanned_rows} rows; \
         expected at most {MAX_SCAN_CALLS} scans and {MAX_SCANNED_ROWS} rows"
    );

    session.close().await.expect("session should close");
}

simulation_test!(
    lix_filesystem_history_propagates_nested_ancestor_rename_and_move,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES \
                 ('projection-root', '/workspace/'), \
                 ('projection-docs', '/workspace/docs/'), \
                 ('projection-guides', '/workspace/docs/guides/'), \
                 ('projection-destination', '/destination/')",
                &[],
            )
            .await
            .expect("nested projection directories should insert");
        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('projection-file', '/workspace/docs/guides/readme.md', X'78')",
                &[],
            )
            .await
            .expect("nested projection file should insert");

        session
            .execute(
                "UPDATE lix_directory SET name = 'archive' WHERE id = 'projection-root'",
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
                     FROM lix_file_history \
                     WHERE lixcol_as_of_commit_id = '{rename_commit_id}' \
                       AND lixcol_depth = 0 \
                       AND id = 'projection-file'"
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
        assert_eq!(rename_sources.as_array().map(Vec::len), Some(1));
        assert_eq!(rename_sources[0]["entity_pk"], json!(["projection-root"]));

        let renamed_directory = session
            .execute(
                &format!(
                    "SELECT path, lixcol_source_changes \
                     FROM lix_directory_history \
                     WHERE lixcol_as_of_commit_id = '{rename_commit_id}' \
                       AND lixcol_depth = 0 \
                       AND id = 'projection-guides'"
                ),
                &[],
            )
            .await
            .expect("renamed descendant directory history should load");
        assert_eq!(renamed_directory.len(), 1);
        assert_eq!(
            renamed_directory.rows()[0].get::<Value>("path").unwrap(),
            Value::Text("/archive/docs/guides/".to_string())
        );

        session
            .execute(
                "UPDATE lix_directory \
                 SET path = '/destination/archive/' \
                 WHERE id = 'projection-root'",
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
                    "SELECT path FROM lix_file_history \
                     WHERE lixcol_as_of_commit_id = '{move_commit_id}' \
                       AND lixcol_depth = 0 \
                       AND id = 'projection-file'"
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
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES \
                 ('grouped-root', '/grouped/'), \
                 ('grouped-child', '/grouped/child/')",
                &[],
            )
            .await
            .expect("grouped directories should insert");
        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('grouped-file', '/grouped/child/file.txt', X'78')",
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
                "UPDATE lix_directory SET name = 'renamed-root' WHERE id = 'grouped-root'",
                &[],
            )
            .await
            .expect("root rename should stage");
        transaction
            .execute(
                "UPDATE lix_directory SET name = 'renamed-child' WHERE id = 'grouped-child'",
                &[],
            )
            .await
            .expect("child rename should stage");
        transaction
            .execute(
                "UPDATE lix_file SET name = 'renamed.txt' WHERE id = 'grouped-file'",
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
                     FROM lix_file_history \
                     WHERE lixcol_as_of_commit_id = '{commit_id}' \
                       AND lixcol_depth = 0 \
                       AND id = 'grouped-file'"
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
        let source_ids = file_sources
            .as_array()
            .expect("grouped file sources should be an array")
            .iter()
            .map(|source| source["entity_pk"][0].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            source_ids,
            BTreeSet::from(["grouped-root", "grouped-child", "grouped-file"])
        );

        let directory_row = session
            .execute(
                &format!(
                    "SELECT path, lixcol_source_changes \
                     FROM lix_directory_history \
                     WHERE lixcol_as_of_commit_id = '{commit_id}' \
                       AND lixcol_depth = 0 \
                       AND id = 'grouped-child'"
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
        assert_eq!(directory_sources.as_array().map(Vec::len), Some(2));
    }
);

simulation_test!(
    lix_filesystem_history_preserves_ancestor_sibling_revisions,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_directory (id, path) VALUES \
             ('ancestor-sibling-root', '/before/'), \
             ('ancestor-sibling-child', '/before/child/')",
            &[],
        )
        .await
        .expect("sibling directories should insert");
        main.execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('ancestor-sibling-file', '/before/child/file.txt', X'78')",
            &[],
        )
        .await
        .expect("sibling file should insert");
        main.create_branch(CreateBranchOptions {
            id: Some("ancestor-sibling-draft".to_string()),
            name: "Ancestor sibling draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("sibling branch should create");
        let draft = sim.wrap_session(
            engine
                .open_session("ancestor-sibling-draft")
                .await
                .expect("draft session should open"),
            &engine,
        );

        main.execute(
            "UPDATE lix_directory SET name = 'same' WHERE id = 'ancestor-sibling-root'",
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
                "UPDATE lix_directory SET name = 'same' WHERE id = 'ancestor-sibling-root'",
                &[],
            )
            .await
            .expect("draft ancestor rename should succeed");
        let draft_sibling = engine
            .load_branch_head_commit_id("ancestor-sibling-draft")
            .await
            .expect("draft sibling should load")
            .expect("draft sibling should exist");
        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "ancestor-sibling-draft".to_string(),
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
                     FROM lix_file_history \
                     WHERE lixcol_as_of_commit_id = '{merge_commit_id}' \
                       AND lixcol_depth = 1 \
                       AND id = 'ancestor-sibling-file' \
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
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES \
                 ('restore-root', '/restore/'), \
                 ('restore-child', '/restore/child/')",
                &[],
            )
            .await
            .expect("restore directories should insert");
        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('restore-file', '/restore/child/file.txt', X'78')",
                &[],
            )
            .await
            .expect("restore file should insert");
        session
            .execute("DELETE FROM lix_directory WHERE id = 'restore-root'", &[])
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
                     FROM lix_file_history \
                     WHERE lixcol_as_of_commit_id = '{delete_commit_id}' \
                       AND lixcol_depth = 0 \
                       AND id = 'restore-file'"
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
        let deleted_directory_ids = delete_sources
            .as_array()
            .expect("delete sources should be an array")
            .iter()
            .filter(|source| source["schema_key"] == json!("lix_directory_descriptor"))
            .map(|source| source["entity_pk"][0].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            deleted_directory_ids,
            BTreeSet::from(["restore-root", "restore-child"])
        );

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("restore transaction should begin");
        transaction
            .execute(
                "INSERT INTO lix_directory (id, path) VALUES \
                 ('restore-root', '/restored/'), \
                 ('restore-child', '/restored/child/')",
                &[],
            )
            .await
            .expect("directories should restore");
        transaction
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('restore-file', '/restored/child/file.txt', X'79')",
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
                    "SELECT path, data, lixcol_source_changes \
                     FROM lix_file_history \
                     WHERE lixcol_as_of_commit_id = '{restore_commit_id}' \
                       AND lixcol_depth = 0 \
                       AND id = 'restore-file'"
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
            restored.rows()[0].get::<Value>("data").unwrap(),
            Value::Blob(b"y".to_vec().into())
        );
        let Value::Json(restore_sources) = restored.rows()[0]
            .get::<Value>("lixcol_source_changes")
            .unwrap()
        else {
            panic!("restore sources should be JSON");
        };
        let restored_directory_ids = restore_sources
            .as_array()
            .expect("restore sources should be an array")
            .iter()
            .filter(|source| source["schema_key"] == json!("lix_directory_descriptor"))
            .map(|source| source["entity_pk"][0].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            restored_directory_ids,
            BTreeSet::from(["restore-root", "restore-child"])
        );
    }
);

simulation_test!(
    lix_file_history_reads_path_and_data_from_commit_graph,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('history-file', '/docs/guides/readme.md', X'68656C6C6F')",
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
                 WHERE id = 'history-file'",
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
                "SELECT id, path, name, data, lixcol_as_of_commit_id, lixcol_depth \
                 FROM lix_file_history \
                 WHERE lixcol_as_of_commit_id = $1 \
                   AND id = $2 \
                   AND path LIKE $3 \
                 ORDER BY lixcol_depth",
                &[
                    Value::Text(second_commit_id.clone()),
                    Value::Text("history-file".to_string()),
                    Value::Text("/docs/%".to_string()),
                ],
            )
            .await
            .expect("file history read should succeed");

        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("history-file".to_string()),
                    Value::Text("/docs/readme-renamed.md".to_string()),
                    Value::Text("readme-renamed.md".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                    Value::Text(second_commit_id.clone()),
                    Value::Integer(0),
                ],
                vec![
                    Value::Text("history-file".to_string()),
                    Value::Text("/docs/guides/readme.md".to_string()),
                    Value::Text("readme.md".to_string()),
                    Value::Blob(b"hello".to_vec().into()),
                    Value::Text(second_commit_id.clone()),
                    Value::Integer(1),
                ],
            ],
        );

        let source_changes_result = session
            .execute(
                &format!(
                    "SELECT lixcol_source_changes \
                     FROM lix_file_history \
                     WHERE lixcol_as_of_commit_id = '{second_commit_id}' \
                       AND id = 'history-file' \
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
                     FROM lix_file_history \
                     WHERE lixcol_as_of_commit_id = '{first_commit_id}' \
                       AND path LIKE '/missing/%'"
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
                .open_workspace_session()
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
                    "SELECT path, data \
                     FROM lix_file_history \
                     WHERE lixcol_as_of_commit_id = '{commit_id}' \
                       AND path = '/empty-history.txt' \
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
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('diamond-file', '/before.md', X'62617365')",
            &[],
        )
        .await
        .expect("base file should insert");
        main.create_branch(CreateBranchOptions {
            id: Some("diamond-draft".to_string()),
            name: "Diamond draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created");
        let draft = sim.wrap_session(
            engine
                .open_session("diamond-draft")
                .await
                .expect("draft session should open"),
            &engine,
        );

        main.execute(
            "UPDATE lix_file SET path = '/same.md' WHERE id = 'diamond-file'",
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
                "UPDATE lix_file SET path = '/same.md' WHERE id = 'diamond-file'",
                &[],
            )
            .await
            .expect("draft path update should succeed");
        let draft_sibling = engine
            .load_branch_head_commit_id("diamond-draft")
            .await
            .expect("draft sibling should load")
            .expect("draft sibling should exist");

        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "diamond-draft".to_string(),
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
                     FROM lix_file_history \
                     WHERE lixcol_as_of_commit_id = '{merge_commit_id}' \
                       AND id = 'diamond-file' \
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

simulation_test!(lix_file_history_reads_bound_id_in_list, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    session
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES \
                    ('history-in-a', '/history/in-a.txt', X'61'), \
                    ('history-in-b', '/history/in-b.txt', X'62')",
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
            "SELECT id, path, data \
                 FROM lix_file_history \
                 WHERE lixcol_as_of_commit_id = $1 \
                   AND id IN ($2, $3) \
                 ORDER BY id",
            &[
                Value::Text(commit_id),
                Value::Text("history-in-b".to_string()),
                Value::Text("history-in-a".to_string()),
            ],
        )
        .await
        .expect("bound ID IN history read should succeed");

    assert_rows_eq(
        result,
        vec![
            vec![
                Value::Text("history-in-a".to_string()),
                Value::Text("/history/in-a.txt".to_string()),
                Value::Blob(b"a".to_vec().into()),
            ],
            vec![
                Value::Text("history-in-b".to_string()),
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
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('aaa-older-history-file', '/older.txt', X'6F6C646572')",
                &[],
            )
            .await
            .expect("older file insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('zzz-newer-history-file', '/newer.txt', X'6E65776572')",
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
                     FROM lix_file_history \
                     WHERE lixcol_as_of_commit_id = '{commit_id}' \
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
                Value::Text("zzz-newer-history-file".to_string()),
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
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES \
                    ('aaa-history-noise-1', '/noise/one.txt', X'6F6E65'), \
                    ('aaa-history-noise-2', '/noise/two.txt', X'74776F'), \
                    ('zzz-history-target', '/target/three.txt', X'7468726565')",
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
                    "SELECT id, path, data \
                     FROM lix_file_history \
                     WHERE lixcol_as_of_commit_id = '{commit_id}' \
                       AND path LIKE '/target/%' \
                     LIMIT 1"
                ),
                &[],
            )
            .await
            .expect("file history read should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("zzz-history-target".to_string()),
                Value::Text("/target/three.txt".to_string()),
                Value::Blob(b"three".to_vec().into()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_history_requires_as_of_commit_id,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute("SELECT id FROM lix_file_history", &[])
            .await
            .expect_err("file history queries must provide an as-of commit");

        assert!(
            error
                .to_string()
                .contains("requires a lixcol_as_of_commit_id filter"),
            "unexpected error: {error}"
        );
        assert!(
            error
                .hint()
                .is_some_and(|hint| hint.contains("WHERE lixcol_as_of_commit_id")),
            "unexpected error: {error}"
        );
    }
);

simulation_test!(
    lix_file_history_ignores_unrelated_file_scoped_state_events,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('ordinary-history-file', '/ordinary-history.txt', X'68656C6C6F')",
                &[],
            )
            .await
            .expect("file insert should succeed");
        session
            .execute(
                "INSERT INTO lix_state (entity_pk, schema_key, file_id, snapshot_content) \
                 VALUES (lix_json('[\"ordinary-sidecar\"]'), 'lix_key_value', \
                         'ordinary-history-file', \
                         lix_json('{\"key\":\"ordinary-sidecar\",\"value\":\"noise\"}'))",
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
                    "SELECT path, data, lixcol_depth \
                     FROM lix_file_history \
                     WHERE lixcol_as_of_commit_id = '{commit_id}' \
                       AND id = 'ordinary-history-file' \
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
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('history-file-blob-filter', '/blob-filter.txt', X'626C6F62')",
                &[],
            )
            .await
            .expect("file insert should succeed");
        session
            .execute(
                "UPDATE lix_file SET data = X'626C6F6232' \
                 WHERE id = 'history-file-blob-filter'",
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
                    "SELECT id, path, data, lixcol_source_changes \
                     FROM lix_file_history \
                     WHERE lixcol_as_of_commit_id = '{commit_id}' \
                       AND id = 'history-file-blob-filter' \
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
                Value::Text("history-file-blob-filter".to_string()),
                Value::Text("/blob-filter.txt".to_string()),
                Value::Blob(b"blob2".to_vec().into()),
            ]
        );
        let Value::Json(latest_sources) = &latest[3] else {
            panic!("latest source changes should be JSON, got {:?}", latest[3]);
        };
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
                         FROM lix_file_history \
                         WHERE lixcol_as_of_commit_id = '{commit_id}'"
                    ),
                    &[],
                )
                .await
                .expect_err("composed history singular provenance must fail");
            assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);
        }
    }
);
