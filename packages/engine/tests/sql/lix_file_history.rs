use std::collections::BTreeSet;
use std::io::{Cursor, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use lix_engine::Value;
use lix_engine::wasm::{
    WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime,
};
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
    const UNRELATED_PLUGIN_FILE_COUNT: usize = 16;
    // Event provenance still walks the commit's change refs. The observed-root
    // reconstruction must not load the unrelated descriptor/blob/directory,
    // plugin-state, or durable-owner rows a second time.
    const MAX_REQUESTED_KEYS: u64 = 416;
    const MAX_SCAN_CALLS: u64 = 128;
    const MAX_SCANNED_ROWS: u64 = 512;

    let storage = CountingStorage::default();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine =
        Engine::new_with_wasm_runtime(storage.clone(), Arc::new(HistoryRenderPluginRuntime))
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
    session
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text("/.lix/plugins/plugin_history_render.lixplugin".to_string()),
                Value::Blob(history_render_plugin_archive().into()),
            ],
        )
        .await
        .expect("performance fixture plugin should install");
    let unrelated_plugin_files = (0..UNRELATED_PLUGIN_FILE_COUNT)
        .map(|index| {
            format!(
                "('unrelated-plugin-{index:03}', '/unrelated-plugin-{index:03}.history-render', X'78')"
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute(
            &format!("INSERT INTO lix_file (id, path, data) VALUES {unrelated_plugin_files}"),
            &[],
        )
        .await
        .expect("unrelated plugin-owned files should insert in one commit");
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
         {UNRELATED_PLUGIN_FILE_COUNT} plugin-owned files; expected at most {MAX_REQUESTED_KEYS}"
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
        assert!(
            result.notices().is_empty(),
            "ordinary path predicates should not emit identity heuristics"
        );

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

        let old_path_result = session
            .execute(
                "SELECT id, path, lixcol_depth \
                 FROM lix_file_history \
                 WHERE lixcol_as_of_commit_id = $1 \
                   AND path = '/docs/guides/readme.md' \
                 ORDER BY lixcol_depth",
                &[Value::Text(second_commit_id.clone())],
            )
            .await
            .expect("historical path predicate should execute");
        assert_rows_eq(
            old_path_result,
            vec![vec![
                Value::Text("history-file".to_string()),
                Value::Text("/docs/guides/readme.md".to_string()),
                Value::Integer(1),
            ]],
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

simulation_test!(
    joined_history_filters_keep_relation_local_sql_semantics,
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
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('history-join-dir', '/joined/')",
                &[],
            )
            .await
            .expect("directory insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('history-join-file', '/joined/old.txt', X'6F6E65')",
                &[],
            )
            .await
            .expect("file insert should succeed");
        session
            .execute(
                "UPDATE lix_file \
                 SET path = '/joined/new.txt' \
                 WHERE id = 'history-join-file'",
                &[],
            )
            .await
            .expect("file rename should succeed");

        let result = session
            .execute(
                "SELECT file.id, file.path, directory.id \
                 FROM lix_file_history AS file \
                 JOIN lix_directory_history AS directory \
                   ON file.directory_id = directory.id \
                 WHERE file.lixcol_as_of_commit_id = lix_active_branch_commit_id() \
                   AND directory.lixcol_as_of_commit_id = lix_active_branch_commit_id() \
                   AND file.path = '/joined/old.txt'",
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
                Value::Text("history-join-file".to_string()),
                Value::Text("/joined/old.txt".to_string()),
                Value::Text("history-join-dir".to_string()),
            ]],
        );
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

#[tokio::test]
async fn lix_file_history_renders_plugin_state_at_each_depth() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new_with_wasm_runtime(storage, Arc::new(HistoryRenderPluginRuntime))
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text("/.lix/plugins/plugin_history_render.lixplugin".to_string()),
                Value::Blob(history_render_plugin_archive().into()),
            ],
        )
        .await
        .expect("plugin archive write should install plugin");
    session
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text("/note.history-render".to_string()),
                Value::Blob(b"first".to_vec().into()),
            ],
        )
        .await
        .expect("plugin file write should succeed");
    session
        .execute(
            "UPDATE lix_file SET data = $1 WHERE path = $2",
            &[
                Value::Blob(b"second".to_vec().into()),
                Value::Text("/note.history-render".to_string()),
            ],
        )
        .await
        .expect("plugin file update should succeed");
    session
        .execute(
            "INSERT INTO lix_key_value (key, value) \
             VALUES ('history-render-sidecar', 'newer non-file commit')",
            &[],
        )
        .await
        .expect("non-file commit should succeed");

    let commit_id_rows = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("active branch commit id should load");
    let [Value::Text(commit_id)] = commit_id_rows.rows()[0].values() else {
        panic!(
            "expected active branch commit id row, got {:?}",
            commit_id_rows.rows()[0].values()
        );
    };
    let file_id_rows = session
        .execute(
            "SELECT id FROM lix_file WHERE path = '/note.history-render'",
            &[],
        )
        .await
        .expect("file id read should succeed");
    let [Value::Text(file_id)] = file_id_rows.rows()[0].values() else {
        panic!(
            "expected file id row, got {:?}",
            file_id_rows.rows()[0].values()
        );
    };

    let result = session
        .execute(
            &format!(
                "SELECT path, data, lixcol_depth \
                 FROM lix_file_history \
                 WHERE lixcol_as_of_commit_id = '{commit_id}' \
                   AND id = '{file_id}' \
                 ORDER BY lixcol_depth \
                 LIMIT 2"
            ),
            &[],
        )
        .await
        .expect("plugin file history read should succeed");

    assert_rows_eq(
        result,
        vec![
            vec![
                Value::Text("/note.history-render".to_string()),
                Value::Blob(b"rendered:second-a|second-b".to_vec().into()),
                Value::Integer(1),
            ],
            vec![
                Value::Text("/note.history-render".to_string()),
                Value::Blob(b"rendered:first-a|first-b".to_vec().into()),
                Value::Integer(2),
            ],
        ],
    );

    // The plugin was installed before this narrow event depth. The provider
    // must retain plugin discovery/state history from the broader context
    // route instead of applying the file-ID fast path to a plugin history.
    let depth_filtered = session
        .execute(
            &format!(
                "SELECT data, lixcol_depth \
                 FROM lix_file_history \
                 WHERE lixcol_as_of_commit_id = '{commit_id}' \
                   AND id = '{file_id}' \
                   AND lixcol_depth = 1"
            ),
            &[],
        )
        .await
        .expect("depth-filtered plugin history should retain materialization context");
    assert_rows_eq(
        depth_filtered,
        vec![vec![
            Value::Blob(b"rendered:second-a|second-b".to_vec().into()),
            Value::Integer(1),
        ]],
    );

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn lix_file_history_uses_each_siblings_observed_plugin_registry() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new_with_wasm_runtime(storage, Arc::new(HistoryRenderPluginRuntime))
        .await
        .expect("engine should open with plugin runtime");
    let main = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    main.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
        &[
            Value::Text("/.lix/plugins/plugin_history_render.lixplugin".to_string()),
            Value::Blob(history_render_plugin_archive().into()),
        ],
    )
    .await
    .expect("plugin archive write should install plugin");
    main.create_branch(CreateBranchOptions {
        id: Some("plugin-history-sibling".to_string()),
        name: "Plugin history sibling".to_string(),
        from_commit_id: None,
    })
    .await
    .expect("plugin history sibling should be created");
    let sibling = engine
        .open_session("plugin-history-sibling")
        .await
        .expect("plugin history sibling should open");

    main.execute(
        "DELETE FROM lix_file \
         WHERE path = '/.lix/plugins/plugin_history_render.lixplugin'",
        &[],
    )
    .await
    .expect("main sibling should uninstall the plugin");
    sibling
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ($1, $2, $3)",
            &[
                Value::Text("plugin-sibling-note".to_string()),
                Value::Text("/note.history-render".to_string()),
                Value::Blob(b"sibling".to_vec().into()),
            ],
        )
        .await
        .expect("other sibling should materialize a plugin file");
    let sibling_commit_id = engine
        .load_branch_head_commit_id("plugin-history-sibling")
        .await
        .expect("plugin sibling head should load")
        .expect("plugin sibling head should exist");

    let receipt = main
        .merge_branch(MergeBranchOptions {
            source_branch_id: "plugin-history-sibling".to_string(),
        })
        .await
        .expect("independent plugin uninstall and file insert should merge");
    let merge_commit_id = receipt
        .created_merge_commit_id
        .expect("sibling merge should create a merge commit");

    let result = main
        .execute(
            &format!(
                "SELECT data, lixcol_observed_commit_id, lixcol_source_changes \
                 FROM lix_file_history \
                 WHERE lixcol_as_of_commit_id = '{merge_commit_id}' \
                   AND path = '/note.history-render' \
                   AND lixcol_depth = 1"
            ),
            &[],
        )
        .await
        .expect("plugin-backed sibling history should load from its observed registry");

    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0]
            .get::<Value>("data")
            .expect("rendered data should decode"),
        Value::Blob(b"rendered:sibling-a|sibling-b".to_vec().into())
    );
    assert_eq!(
        result.rows()[0]
            .get::<Value>("lixcol_observed_commit_id")
            .expect("observed commit should decode"),
        Value::Text(sibling_commit_id)
    );
    let Value::Json(source_changes) = result.rows()[0]
        .get::<Value>("lixcol_source_changes")
        .expect("source changes should decode")
    else {
        panic!("source changes should be JSON");
    };
    assert!(
        source_changes.as_array().is_some_and(|changes| changes
            .iter()
            .any(|change| change["schema_key"] == json!("plugin_history_note"))),
        "the plugin state changes from the sibling commit must remain provenance"
    );

    sibling.close().await.expect("sibling should close");
    main.close().await.expect("main should close");
}

#[tokio::test]
async fn lix_file_history_projects_owned_file_plugin_lifecycle_without_glob_reassignment() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new_with_wasm_runtime(storage, Arc::new(HistoryRenderPluginRuntime))
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text("/.lix/plugins/plugin_history_render.lixplugin".to_string()),
                Value::Blob(history_render_plugin_archive().into()),
            ],
        )
        .await
        .expect("initial plugin should install");
    session
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ($1, $2, $3)",
            &[
                Value::Text("plugin-lifecycle-note".to_string()),
                Value::Text("/note.history-render".to_string()),
                Value::Blob(b"owned".to_vec().into()),
            ],
        )
        .await
        .expect("plugin-owned file should insert");

    session
        .execute(
            "UPDATE lix_file SET data = $1 \
             WHERE path = '/.lix/plugins/plugin_history_render.lixplugin'",
            &[Value::Blob(history_render_plugin_upgrade_archive().into())],
        )
        .await
        .expect("plugin should upgrade without rewriting the owned file");
    let upgrade_commit_rows = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("upgrade commit should load");
    let [Value::Text(upgrade_commit_id)] = upgrade_commit_rows.rows()[0].values() else {
        panic!("upgrade commit id should be text");
    };

    let upgraded = session
        .execute(
            &format!(
                "SELECT data, lixcol_source_changes \
                 FROM lix_file_history \
                 WHERE lixcol_as_of_commit_id = '{upgrade_commit_id}' \
                   AND lixcol_depth = 0 \
                   AND id = 'plugin-lifecycle-note'"
            ),
            &[],
        )
        .await
        .expect("plugin upgrade should project an owned-file revision");
    assert_eq!(upgraded.len(), 1);
    assert_eq!(
        upgraded.rows()[0].get::<Value>("data").unwrap(),
        Value::Blob(b"upgraded:owned-a|owned-b".to_vec().into())
    );
    let Value::Json(upgrade_sources) = upgraded.rows()[0]
        .get::<Value>("lixcol_source_changes")
        .unwrap()
    else {
        panic!("upgrade source changes should be JSON");
    };
    assert!(
        upgrade_sources.as_array().is_some_and(|sources| {
            sources.iter().any(|source| {
                source["schema_key"] == json!("lix_key_value")
                    && source["entity_pk"] == json!(["lix_plugin_registry_v1"])
            })
        }),
        "the registry upgrade must be the owned-file projection source"
    );

    session
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text("/.lix/plugins/plugin_history_overlap.lixplugin".to_string()),
                Value::Blob(history_overlap_plugin_archive().into()),
            ],
        )
        .await
        .expect("overlapping plugin should install");
    let overlap_commit_rows = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("overlap commit should load");
    let [Value::Text(overlap_commit_id)] = overlap_commit_rows.rows()[0].values() else {
        panic!("overlap commit id should be text");
    };
    let overlap_history = session
        .execute(
            &format!(
                "SELECT id \
                 FROM lix_file_history \
                 WHERE lixcol_as_of_commit_id = '{overlap_commit_id}' \
                   AND lixcol_depth = 0 \
                   AND id = 'plugin-lifecycle-note'"
            ),
            &[],
        )
        .await
        .expect("unrelated overlapping install should not project an owned-file revision");
    assert_rows_eq(overlap_history, Vec::<Vec<Value>>::new());
    let still_owned = session
        .execute(
            "SELECT data FROM lix_file WHERE id = 'plugin-lifecycle-note'",
            &[],
        )
        .await
        .expect("overlapping plugin must not steal the durable owner");
    assert_rows_eq(
        still_owned,
        vec![vec![Value::Blob(
            b"upgraded:owned-a|owned-b".to_vec().into(),
        )]],
    );

    session
        .execute(
            "INSERT INTO plugin_history_overlap_note \
             (id, value, lixcol_file_id, lixcol_global, lixcol_untracked) \
             VALUES ('overlap-sidecar', 'noise', 'plugin-lifecycle-note', false, false)",
            &[],
        )
        .await
        .expect("non-owner plugin state should remain independently writable");
    let non_owner_state_commit_rows = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("non-owner plugin state commit should load");
    let [Value::Text(non_owner_state_commit_id)] = non_owner_state_commit_rows.rows()[0].values()
    else {
        panic!("non-owner plugin state commit id should be text");
    };
    let non_owner_state_history = session
        .execute(
            &format!(
                "SELECT id \
                 FROM lix_file_history \
                 WHERE lixcol_as_of_commit_id = '{non_owner_state_commit_id}' \
                   AND lixcol_depth = 0 \
                   AND id = 'plugin-lifecycle-note'"
            ),
            &[],
        )
        .await
        .expect("non-owner plugin state should not revise the owned file projection");
    assert_rows_eq(non_owner_state_history, Vec::<Vec<Value>>::new());
    let still_owned_after_non_owner_write = session
        .execute(
            "SELECT data FROM lix_file WHERE id = 'plugin-lifecycle-note'",
            &[],
        )
        .await
        .expect("non-owner plugin state must not affect the durable owner's rendering");
    assert_rows_eq(
        still_owned_after_non_owner_write,
        vec![vec![Value::Blob(
            b"upgraded:owned-a|owned-b".to_vec().into(),
        )]],
    );

    session
        .execute(
            "DELETE FROM lix_file \
             WHERE path = '/.lix/plugins/plugin_history_render.lixplugin'",
            &[],
        )
        .await
        .expect("owning plugin should uninstall");
    let uninstall_commit_rows = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("uninstall commit should load");
    let [Value::Text(uninstall_commit_id)] = uninstall_commit_rows.rows()[0].values() else {
        panic!("uninstall commit id should be text");
    };
    let unavailable_projection = session
        .execute(
            &format!(
                "SELECT id, lixcol_source_changes \
                 FROM lix_file_history \
                 WHERE lixcol_as_of_commit_id = '{uninstall_commit_id}' \
                   AND lixcol_depth = 0 \
                   AND id = 'plugin-lifecycle-note'"
            ),
            &[],
        )
        .await
        .expect("uninstall should remain queryable when data is not projected");
    assert_eq!(unavailable_projection.len(), 1);
    let history_error = session
        .execute(
            &format!(
                "SELECT data \
                 FROM lix_file_history \
                 WHERE lixcol_as_of_commit_id = '{uninstall_commit_id}' \
                   AND lixcol_depth = 0 \
                   AND id = 'plugin-lifecycle-note'"
            ),
            &[],
        )
        .await
        .expect_err("unavailable historical owner must not silently render or reassign");
    assert_eq!(history_error.code, LixError::CODE_PLUGIN_UNAVAILABLE);
    let live_error = session
        .execute(
            "SELECT data FROM lix_file WHERE id = 'plugin-lifecycle-note'",
            &[],
        )
        .await
        .expect_err("live file should use the same unavailable-owner contract");
    assert_eq!(live_error.code, LixError::CODE_PLUGIN_UNAVAILABLE);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn lix_file_history_keeps_plugin_state_tombstones_in_deleted_file_provenance() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new_with_wasm_runtime(storage, Arc::new(HistoryRenderPluginRuntime))
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text("/.lix/plugins/plugin_history_render.lixplugin".to_string()),
                Value::Blob(history_render_plugin_archive().into()),
            ],
        )
        .await
        .expect("plugin should install");
    session
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES ($1, $2, $3)",
            &[
                Value::Text("plugin-deleted-note".to_string()),
                Value::Text("/deleted.history-render".to_string()),
                Value::Blob(b"deleted".to_vec().into()),
            ],
        )
        .await
        .expect("plugin-owned file should insert");
    session
        .execute("DELETE FROM lix_file WHERE id = 'plugin-deleted-note'", &[])
        .await
        .expect("plugin-owned file should delete");
    let delete_commit_rows = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("delete commit should load");
    let [Value::Text(delete_commit_id)] = delete_commit_rows.rows()[0].values() else {
        panic!("delete commit id should be text");
    };

    let result = session
        .execute(
            &format!(
                "SELECT path, data, lixcol_source_changes \
                 FROM lix_file_history \
                 WHERE lixcol_as_of_commit_id = '{delete_commit_id}' \
                   AND lixcol_depth = 0 \
                   AND id = 'plugin-deleted-note'"
            ),
            &[],
        )
        .await
        .expect("deleted plugin-file provenance should remain queryable");
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<Value>("path").unwrap(), Value::Null);
    assert_eq!(result.rows()[0].get::<Value>("data").unwrap(), Value::Null);
    let Value::Json(sources) = result.rows()[0]
        .get::<Value>("lixcol_source_changes")
        .unwrap()
    else {
        panic!("delete source changes should be JSON");
    };
    let sources = sources
        .as_array()
        .expect("delete source changes should be an array");
    let plugin_tombstones = sources
        .iter()
        .filter(|source| source["schema_key"] == json!("plugin_history_note"))
        .collect::<Vec<_>>();
    assert_eq!(plugin_tombstones.len(), 2);
    assert!(
        plugin_tombstones
            .iter()
            .all(|source| source["snapshot_content"].is_null()),
        "plugin entity tombstones must remain in composed provenance"
    );
    assert!(
        sources.iter().any(|source| {
            source["schema_key"] == json!("lix_key_value")
                && source["entity_pk"] == json!(["lix_plugin_owner_v1"])
                && source["snapshot_content"].is_null()
        }),
        "durable owner tombstone must remain in composed provenance"
    );

    session.close().await.expect("session should close");
}

simulation_test!(lix_file_history_defaults_to_active_head, |sim| async move {
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
                 VALUES ('history-default-file', '/history-default.txt', X'64656661756C74')",
            &[],
        )
        .await
        .expect("file insert should succeed");
    let active_head = engine
        .load_branch_head_commit_id(sim.main_branch_id())
        .await
        .expect("active head should load")
        .expect("active head should exist");

    let result = session
        .execute(
            "SELECT id, lixcol_as_of_commit_id, lixcol_depth \
                 FROM lix_file_history \
                 WHERE id = 'history-default-file'",
            &[],
        )
        .await
        .expect("file history should default to the active head");

    assert_rows_eq(
        result,
        vec![vec![
            Value::Text("history-default-file".to_string()),
            Value::Text(active_head),
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

struct HistoryRenderPluginRuntime;

struct HistoryRenderPluginComponent {
    prefix: &'static str,
}

#[async_trait]
impl WasmRuntime for HistoryRenderPluginRuntime {
    async fn init_component(
        &self,
        bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        let prefix = if bytes.ends_with(b"upgrade") {
            "upgraded"
        } else if bytes.ends_with(b"overlap") {
            "overlap"
        } else {
            "rendered"
        };
        Ok(Arc::new(HistoryRenderPluginComponent { prefix }))
    }
}

#[async_trait]
impl WasmComponentInstance for HistoryRenderPluginComponent {
    async fn detect_changes(
        &self,
        _state: Vec<WasmPluginEntityState>,
        file: WasmPluginFile,
    ) -> Result<Vec<WasmPluginDetectedChange>, LixError> {
        let value = String::from_utf8(file.data).map_err(|error| {
            LixError::unknown(format!("plugin test data was not UTF-8: {error}"))
        })?;
        Ok(["a", "b"]
            .into_iter()
            .map(|suffix| WasmPluginDetectedChange {
                entity_pk: vec![format!("note-{suffix}")],
                schema_key: "plugin_history_note".to_string(),
                snapshot_content: Some(format!(
                    "{{\"id\":\"note-{suffix}\",\"value\":{}}}",
                    serde_json::to_string(&format!("{value}-{suffix}"))
                        .expect("test value should serialize")
                )),
                metadata: None,
            })
            .collect())
    }

    async fn render(&self, state: Vec<WasmPluginEntityState>) -> Result<Vec<u8>, LixError> {
        let mut values = state
            .iter()
            .filter(|row| row.schema_key == "plugin_history_note")
            .filter_map(|row| serde_json::from_str::<serde_json::Value>(&row.snapshot_content).ok())
            .filter_map(|snapshot| {
                snapshot
                    .get("value")
                    .and_then(|value| value.as_str())
                    .map(ToOwned::to_owned)
            })
            .collect::<Vec<_>>();
        values.sort();
        Ok(format!("{}:{}", self.prefix, values.join("|")).into_bytes())
    }
}

fn history_render_plugin_archive() -> Vec<u8> {
    history_plugin_archive(
        "plugin_history_render",
        "*.history-render",
        "plugin_history_note",
        b"",
    )
}

fn history_render_plugin_upgrade_archive() -> Vec<u8> {
    history_plugin_archive(
        "plugin_history_render",
        "*.history-render",
        "plugin_history_note",
        b"upgrade",
    )
}

fn history_overlap_plugin_archive() -> Vec<u8> {
    history_plugin_archive(
        "plugin_history_overlap",
        "note.history-render",
        "plugin_history_overlap_note",
        b"overlap",
    )
}

fn history_plugin_archive(
    plugin_key: &str,
    path_glob: &str,
    schema_key: &str,
    wasm_marker: &[u8],
) -> Vec<u8> {
    let manifest_json = serde_json::to_vec(&json!({
        "key": plugin_key,
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": { "path_glob": path_glob },
        "entry": "plugin.wasm",
        "schemas": [format!("schema/{schema_key}.json")],
    }))
    .expect("plugin manifest fixture should serialize");
    let schema_json = serde_json::to_vec(&json!({
        "x-lix-key": schema_key,
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "value": { "type": "string" },
        },
        "required": ["id", "value"],
        "additionalProperties": false,
    }))
    .expect("plugin schema fixture should serialize");
    let mut wasm = b"\0asm\x01\0\0\0".to_vec();
    wasm.extend_from_slice(wasm_marker);
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        ("manifest.json".to_string(), manifest_json),
        (format!("schema/{schema_key}.json"), schema_json),
        ("plugin.wasm".to_string(), wasm),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(&bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

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
