// Several benchmark/profiling helpers remain useful to ignored and ad-hoc
// probes even when their former public-SDK tests are not compiled.
#![recursion_limit = "512"]
#![allow(dead_code, unused_imports, unused_attributes)]

mod benchmark_metrics;

use bytes::Bytes;
use lix::plugin::runtime::{
    PluginCapabilities, WasmComponentFactory, WasmRuntime, WasmTransitionCounters,
};
use lix::storage::{
    BeginScanOptions, CoreProjection, Key, KeyRange, ProjectedValue, PutBatch, PutEntry,
    ReadOptions, Storage, StorageRead, StorageSpace, StorageWrite, StoredValue, WriteOptions,
};
use lix::storage_adapter::{
    StorageAdapter, StorageKey, StorageReadOptions, StorageValue, StorageWriteOptions,
};
use lix::storage_bench::{layout_space_catalog, space_inventory};
use lix::wasm::WasmLimits;
use lix::{
    CreateBranchOptions, ExecuteBatchStatement, Lix, LixError, MergeBranchOptions,
    MergeBranchPreviewOptions, SwitchBranchOptions,
};
use lix::{Value, open_lix};
use lix_storage_filesystem::FilesystemStorage;
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::io::{Cursor, Read, Write};
use std::ops::{Bound, Deref};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::Subscriber;
use tracing::span::{Attributes, Id};
use tracing::subscriber::Interest;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context as TracingContext, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

use benchmark_metrics::{
    AllocationScope, BenchmarkFixture, BenchmarkGate, BenchmarkMeasurement, current_live_bytes,
    emit_sample, emit_summary, emit_transition_profile,
};

#[derive(Clone, Default)]
struct PerfSpanCollector {
    samples: Arc<Mutex<Vec<PerfSpanSample>>>,
}

#[derive(Debug)]
struct PerfSpanSample {
    name: &'static str,
    elapsed: Duration,
    live_bytes_at_close: u64,
}

#[derive(Debug)]
struct StartedPerfSpan {
    name: &'static str,
    started: Instant,
}

fn is_exported_or_debug_perf_target(target: &str) -> bool {
    matches!(target, "lix_perf" | "lix_sql")
}

fn jsonb_column_contains(row: &lix::Row, column: &str, needle: &str) -> bool {
    matches!(
        row.get::<Value>(column),
        Ok(Value::Jsonb(value)) if value.to_value().to_string().contains(needle)
    )
}

fn is_import_perf_span(name: &str) -> bool {
    matches!(
        name,
        "lix.perf.transaction_plan_and_stage"
            | "lix.perf.plugin_reconciliation"
            | "lix.perf.plugin_factory_compile"
            | "lix.perf.plugin_file_changed"
            | "lix.perf.plugin_drain_changes"
            | "lix.perf.plugin_suppress_noops"
            | "lix.perf.plugin_create_rows"
            | "lix.perf.plugin_splice_discovery"
            | "lix.perf.v3_guest_file_changed"
            | "lix.perf.v3_arena_prepare"
            | "lix.perf.v3_arena_commit"
            | "lix.perf.plugin_open_file"
            | "lix.perf.plugin_open_file_drain"
            | "lix.perf.plugin_drain_next_page"
            | "lix.perf.plugin_drain_guest_next"
            | "lix.perf.plugin_drain_decode_packet"
            | "lix.perf.plugin_drain_prevalidate_page"
            | "lix.perf.plugin_drain_resolve_page"
            | "lix.perf.plugin_drain_finish"
            | "lix.perf.plugin_change_rows"
            | "lix.perf.plugin_semantic_prepare_rows"
            | "lix.perf.transaction_validation"
            | "lix.perf.validation.registered_schema_identity"
            | "lix.perf.validation.file_owner"
            | "lix.perf.validation.committed_foreign_keys"
            | "lix.perf.validation.delete_restrictions"
            | "lix.perf.validation.branch_ref_delete_restrictions"
            | "lix.perf.validation.insert_identities"
            | "lix.perf.validation.unique_constraints"
            | "lix.perf.validation.directory_parent_graph"
            | "lix.perf.validation.filesystem_namespace"
            | "lix.perf.transaction_materialization"
            | "lix.perf.materialization.finalize_commit_rows"
            | "lix.perf.materialization.changelog"
            | "lix.perf.materialization.tracked_roots"
            | "lix.perf.materialization.tracked_head"
            | "lix.perf.materialization.tracked_head.lifecycle"
            | "lix.perf.materialization.tracked_head.deltas"
            | "lix.perf.materialization.tracked_head.absence_guards"
            | "lix.perf.materialization.tracked_head.stage_current_state"
            | "lix.perf.materialization.hot.sort"
            | "lix.perf.materialization.hot.identities"
            | "lix.perf.materialization.hot.previous"
            | "lix.perf.materialization.hot.values"
            | "lix.perf.materialization.hot.stage"
            | "lix.perf.storage_lowering"
            | "lix.perf.storage_lowering.deferred_next_page"
            | "lix.perf.storage_lowering.deferred_put_page"
            | "lix.perf.transaction_prepare_rows"
            | "lix.perf.transaction_path_preflight"
            | "lix.perf.transaction_buffer_stage"
            | "lix.perf.transaction_storage_prepare"
            | "lix.perf.transaction_storage_commit"
    )
}

impl PerfSpanCollector {
    fn clear(&self) {
        self.samples
            .lock()
            .expect("performance span collector should not poison")
            .clear();
    }

    fn take_aggregate_millis(&self) -> BTreeMap<&'static str, f64> {
        let samples = std::mem::take(
            &mut *self
                .samples
                .lock()
                .expect("performance span collector should not poison"),
        );
        let mut aggregate = BTreeMap::new();
        for sample in samples {
            *aggregate.entry(sample.name).or_insert(0.0) += sample.elapsed.as_secs_f64() * 1_000.0;
        }
        aggregate
    }

    fn take_close_live_bytes(&self) -> BTreeMap<&'static str, u64> {
        let samples = self
            .samples
            .lock()
            .expect("performance span collector should not poison");
        let mut live = BTreeMap::<&'static str, u64>::new();
        for sample in samples.iter() {
            live.entry(sample.name)
                .and_modify(|current| *current = (*current).max(sample.live_bytes_at_close))
                .or_insert(sample.live_bytes_at_close);
        }
        live
    }
}

impl<S> Layer<S> for PerfSpanCollector
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn register_callsite(&self, metadata: &'static tracing::Metadata<'static>) -> Interest {
        if is_exported_or_debug_perf_target(metadata.target()) && is_import_perf_span(metadata.name())
        {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: TracingContext<'_, S>) {
        let name = attributes.metadata().name();
        if !is_import_perf_span(name) {
            return;
        }
        if let Some(span) = context.span(id) {
            span.extensions_mut().insert(StartedPerfSpan {
                name,
                started: Instant::now(),
            });
        }
    }

    fn on_close(&self, id: Id, context: TracingContext<'_, S>) {
        let Some(span) = context.span(&id) else {
            return;
        };
        let Some(started) = span.extensions_mut().remove::<StartedPerfSpan>() else {
            return;
        };
        self.samples
            .lock()
            .expect("performance span collector should not poison")
            .push(PerfSpanSample {
                name: started.name,
                elapsed: started.started.elapsed(),
                live_bytes_at_close: current_live_bytes(),
            });
    }
}

#[derive(Default)]
struct HistoryRejectingRuntime {
    compile_calls: AtomicUsize,
}

#[async_trait::async_trait]
impl WasmRuntime for HistoryRejectingRuntime {
    async fn compile_component(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
        _capabilities: PluginCapabilities,
    ) -> Result<Arc<dyn WasmComponentFactory>, LixError> {
        self.compile_calls.fetch_add(1, Ordering::SeqCst);
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "file history must not execute a plugin",
        ))
    }
}

#[tokio::test]
async fn v2_file_history_reads_durable_materialized_bytes_without_plugin_execution() {
    let storage = lix::Memory::new();
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("workspace should open with the production runtime");
    let archive = build_csv_plugin_archive();
    install_plugin(&lix, "plugin_csv", &archive)
        .await
        .expect("CSV v2 plugin should install");

    let path = "/history-materialized.csv";
    let first = b"name,value\nrow,first\n".to_vec();
    let second = b"name,value\nrow,second\n".to_vec();
    write_file(&lix, path, first.clone())
        .await
        .expect("initial plugin file should materialize");
    let file_id = file_id_at_path(&lix, path).await;
    let edited_row_id = csv_row_id(&active_csv_rows(&lix, &file_id).await, &["row", "first"]);
    lix.execute(
        "UPDATE csv_row SET cells = $1 \
         WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Jsonb(serde_json::json!(["row", "second"]).into()),
            Value::Text(edited_row_id),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("sparse semantic edit should materialize durable bytes");
    assert_eq!(
        read_file(&lix, path)
            .await
            .expect("current file should read"),
        Some(second.clone()),
    );
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('history-sidecar', 'later commit')",
        &[],
    )
    .await
    .expect("sidecar commit should advance history depth");
    let head = lix
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("active branch head should load")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("active branch head should be text");
    lix.close().await.expect("production session should close");

    let rejecting_runtime = Arc::new(HistoryRejectingRuntime::default());
    let wasm_runtime: Arc<dyn WasmRuntime> = rejecting_runtime.clone();
    let history_lix = open_lix()
        .with_storage(storage)
        .with_wasm_runtime(wasm_runtime)
        .await
        .expect("workspace should reopen without compiling installed plugins");
    let result = history_lix
        .execute(
            "SELECT content, lixcol_depth \
             FROM lix_history('lix_file', $1) \
             WHERE id = $2 \
             ORDER BY lixcol_depth \
             LIMIT 2",
            &[Value::Text(head), Value::Text(file_id)],
        )
        .await
        .expect("V2 file history should read durable materialized bytes");

    assert_eq!(result.len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Blob(second.into()), Value::Integer(1)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Blob(first.into()), Value::Integer(2)]
    );
    assert_eq!(
        rejecting_runtime.compile_calls.load(Ordering::SeqCst),
        0,
        "file history must not compile or invoke an installed V2 plugin",
    );
    history_lix
        .close()
        .await
        .expect("history session should close");
}

#[tokio::test]
async fn mixed_file_content_batch_preserves_rows_staged_before_and_after_it() {
    const FILE_ID: &str = "01900000-0000-7000-8000-0000000007f1";
    const PATH: &str = "/mixed-batch-order.json";

    let lix = open_lix().await.expect("mixed-batch workspace should open");
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &build_json_plugin_archive(),
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let mut transaction = lix
        .begin_transaction()
        .await
        .expect("mixed-batch transaction should open");
    assert_eq!(
        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('mixed-order', 'before')",
                &[],
            )
            .await
            .expect("row before file-data batch should stage")
            .rows_affected(),
        1
    );
    assert_eq!(
        transaction
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
                &[
                    Value::Text(FILE_ID.to_owned()),
                    Value::Text(PATH.to_owned()),
                    Value::Blob(br#"{"alpha":"plugin"}"#.to_vec().into()),
                ],
            )
            .await
            .expect("file-data batch should stage")
            .rows_affected(),
        1
    );
    assert_eq!(
        transaction
            .execute(
                "UPDATE lix_key_value SET value = 'after' WHERE key = 'mixed-order'",
                &[],
            )
            .await
            .expect("row after file-data batch should stage")
            .rows_affected(),
        1
    );
    transaction
        .commit()
        .await
        .expect("mixed row and file-data batches should commit");

    let sidecar = lix
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'mixed-order'",
            &[],
        )
        .await
        .expect("mixed-batch sidecar should query");
    assert_eq!(sidecar.len(), 1);
    assert_eq!(
        sidecar.rows()[0].get::<serde_json::Value>("value").unwrap(),
        serde_json::json!("after"),
        "the row staged after RowsWithFileContent must remain the final replacement"
    );
    let member = lix
        .execute(
            "SELECT scalar_json FROM json_object_member \
             WHERE parent_id = 'root' AND key = 'alpha' AND lixcol_file_id = $1",
            &[Value::Text(FILE_ID.to_owned())],
        )
        .await
        .expect("plugin-derived row should query");
    assert_eq!(member.len(), 1);
    assert_eq!(
        member.rows()[0].get::<Value>("scalar_json").unwrap(),
        Value::Jsonb(serde_json::json!("plugin").into())
    );
    assert_eq!(
        read_file(&lix, PATH).await.unwrap(),
        Some(br#"{"alpha":"plugin"}"#.to_vec())
    );
    lix.close()
        .await
        .expect("mixed-batch workspace should close");
}

/// Plugins behave identically irrespective of lane.
///
/// The tracked and untracked arms are deliberately kept in one test so that
/// "identical behaviour" is *asserted* rather than asserted-about: the same
/// bytes go through the same plugin and the same projection, and only the lane
/// differs. Before plugin reconciliation was unskipped for untracked writes,
/// the untracked arm produced no rows at all — an untracked JSON file
/// was a descriptor plus a content blob whose contents were unqueryable.
///
/// The untracked arm's row shape is the one #1346 established: a real
/// `change_id` (identity) with a NULL `commit_id` (no history). The change id
/// is asserted as a property, never as a literal — its value is a function of
/// UUID draw order.
#[tokio::test]
async fn untracked_json_file_produces_the_same_plugin_rows_as_a_tracked_one() {
    const TRACKED_FILE_ID: &str = "01900000-0000-7000-8000-0000000008a1";
    const UNTRACKED_FILE_ID: &str = "01900000-0000-7000-8000-0000000008a2";
    const TRACKED_PATH: &str = "/lane-parity-tracked.json";
    const UNTRACKED_PATH: &str = "/lane-parity-untracked.json";
    const CONTENT: &[u8] = br#"{"alpha":"plugin"}"#;

    let lix = open_lix().await.expect("lane-parity workspace should open");
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &build_json_plugin_archive(),
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    lix.execute(
        "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
        &[
            Value::Text(TRACKED_FILE_ID.to_owned()),
            Value::Text(TRACKED_PATH.to_owned()),
            Value::Blob(CONTENT.to_vec().into()),
        ],
    )
    .await
    .expect("tracked json file should write");
    lix.execute(
        "INSERT INTO lix_file (id, path, content, lixcol_untracked) VALUES ($1, $2, $3, true)",
        &[
            Value::Text(UNTRACKED_FILE_ID.to_owned()),
            Value::Text(UNTRACKED_PATH.to_owned()),
            Value::Blob(CONTENT.to_vec().into()),
        ],
    )
    .await
    .expect("untracked json file should write");

    // Both files must round-trip their bytes regardless of lane.
    assert_eq!(
        read_file(&lix, TRACKED_PATH).await.unwrap(),
        Some(CONTENT.to_vec())
    );
    assert_eq!(
        read_file(&lix, UNTRACKED_PATH).await.unwrap(),
        Some(CONTENT.to_vec())
    );

    let member_row = async |file_id: &str| {
        let result = lix
            .execute(
                "SELECT scalar_json, lixcol_untracked, lixcol_change_id, lixcol_commit_id \
                 FROM json_object_member \
                 WHERE parent_id = 'root' AND key = 'alpha' AND lixcol_file_id = $1",
                &[Value::Text(file_id.to_owned())],
            )
            .await
            .expect("plugin-derived rows should query");
        let [row] = result.rows() else {
            panic!(
                "expected exactly one plugin row for file '{file_id}', got {}",
                result.len()
            );
        };
        row.values().to_vec()
    };

    // Tracked arm: the pre-existing behaviour, kept beside the untracked arm as
    // the reference the untracked arm has to match.
    let [
        tracked_scalar,
        tracked_untracked,
        tracked_change_id,
        tracked_commit_id,
    ] = member_row(TRACKED_FILE_ID)
        .await
        .try_into()
        .unwrap_or_else(|_| panic!("expected four projected columns for the tracked plugin row"));
    assert_eq!(
        tracked_scalar,
        Value::Jsonb(serde_json::json!("plugin").into())
    );
    assert_eq!(tracked_untracked, Value::Boolean(false));
    assert!(
        matches!(&tracked_change_id, Value::Text(value)
            if uuid::Uuid::parse_str(value).is_ok_and(|parsed| !parsed.is_nil())),
        "tracked plugin rows must carry a real change id, got {tracked_change_id:?}"
    );
    assert!(
        matches!(&tracked_commit_id, Value::Text(value) if !value.is_empty()),
        "tracked plugin rows enter the commit graph, got {tracked_commit_id:?}"
    );

    // Untracked arm: the same plugin, the same bytes, the same projection.
    let [
        untracked_scalar,
        untracked_untracked,
        untracked_change_id,
        untracked_commit_id,
    ] = member_row(UNTRACKED_FILE_ID)
        .await
        .try_into()
        .unwrap_or_else(|_| panic!("expected four projected columns for the untracked plugin row"));
    assert_eq!(
        untracked_scalar, tracked_scalar,
        "the same JSON must parse to the same row value irrespective of lane"
    );
    assert_eq!(
        untracked_untracked,
        Value::Boolean(true),
        "rows inherit their file's lane"
    );
    assert!(
        matches!(&untracked_change_id, Value::Text(value)
            if uuid::Uuid::parse_str(value).is_ok_and(|parsed| !parsed.is_nil())),
        "untracked plugin rows are identity-bearing, got {untracked_change_id:?}"
    );
    assert_eq!(
        untracked_commit_id,
        Value::Null,
        "untracked plugin rows must stay outside the commit graph"
    );

    // Editing a row must round-trip back into the file's bytes on both
    // lanes. This probes the read-path boundary deliberately left tracked-only
    // in `sql2/providers/file.rs`: if an untracked file's content depended on
    // being re-rendered from rows through that owner lookup, it would fail
    // here rather than silently later.
    for (file_id, path) in [
        (TRACKED_FILE_ID, TRACKED_PATH),
        (UNTRACKED_FILE_ID, UNTRACKED_PATH),
    ] {
        lix.execute(
            "UPDATE json_object_member SET scalar_json = $1 \
             WHERE parent_id = 'root' AND key = 'alpha' AND lixcol_file_id = $2",
            &[
                Value::Jsonb(serde_json::json!("edited").into()),
                Value::Text(file_id.to_owned()),
            ],
        )
        .await
        .unwrap_or_else(|error| panic!("row edit on '{path}' should commit: {error:?}"));
        assert_eq!(
            read_file(&lix, path).await.unwrap(),
            Some(br#"{"alpha":"edited"}"#.to_vec()),
            "a row edit must re-render '{path}' irrespective of lane"
        );
    }

    lix.close()
        .await
        .expect("lane-parity workspace should close");
}

/// Foreign-key equivalence across lanes on the ordinary decode path.
///
/// A tracked complete parse is retained as a typed row packet, whose foreign
/// keys are checked within the batch.
/// An untracked complete parse takes the ordinary decode path instead, where
/// foreign keys are resolved against live state by ordinary transaction
/// validation. Those are different mechanisms, so equivalence has to be
/// measured rather than argued.
///
/// `markdown_node` is the vehicle: it declares a self-referential foreign key
/// `/parent_id` -> `markdown_node./id`.
#[tokio::test]
async fn untracked_plugin_rows_enforce_foreign_keys_like_tracked_ones() {
    const TRACKED_FILE_ID: &str = "01900000-0000-7000-8000-0000000008b1";
    const UNTRACKED_FILE_ID: &str = "01900000-0000-7000-8000-0000000008b2";
    const CONTENT: &[u8] = b"# Title\n\nA paragraph with *emphasis*.\n\n- one\n- two\n";

    let lix = open_lix().await.expect("fk workspace should open");
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown",
        &build_markdown_plugin_archive(),
        &["markdown_node"],
    )
    .await;

    lix.execute(
        "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
        &[
            Value::Text(TRACKED_FILE_ID.to_owned()),
            Value::Text("/fk-tracked.md".to_owned()),
            Value::Blob(CONTENT.to_vec().into()),
        ],
    )
    .await
    .expect("tracked markdown file should write");
    lix.execute(
        "INSERT INTO lix_file (id, path, content, lixcol_untracked) VALUES ($1, $2, $3, true)",
        &[
            Value::Text(UNTRACKED_FILE_ID.to_owned()),
            Value::Text("/fk-untracked.md".to_owned()),
            Value::Blob(CONTENT.to_vec().into()),
        ],
    )
    .await
    .expect("untracked markdown file should write");

    // Same bytes through the same plugin must yield the same graph shape on
    // both lanes. A decode path that dropped or altered rows shows up here.
    //
    // Node ids are per-file UUIDs, so the shape is compared as each node's kind
    // paired with its parent's *kind* — lane-independent, while still sensitive
    // to a lost row or a reparented one.
    let shape = async |file_id: &str| {
        let result = lix
            .execute(
                "SELECT child.kind AS kind, parent.kind AS parent_kind \
                 FROM markdown_node AS child \
                 LEFT JOIN markdown_node AS parent \
                   ON parent.lixcol_file_id = child.lixcol_file_id \
                  AND parent.id = child.parent_id \
                 WHERE child.lixcol_file_id = $1 \
                 ORDER BY child.kind, parent.kind",
                &[Value::Text(file_id.to_owned())],
            )
            .await
            .expect("markdown nodes should query");
        result
            .rows()
            .iter()
            .map(|row| {
                (
                    row.get::<String>("kind").unwrap(),
                    row.get::<Value>("parent_kind").unwrap(),
                )
            })
            .collect::<Vec<_>>()
    };
    let tracked_shape = shape(TRACKED_FILE_ID).await;
    let untracked_shape = shape(UNTRACKED_FILE_ID).await;
    assert!(
        tracked_shape.len() > 3,
        "the fixture must produce a non-trivial node graph, got {tracked_shape:?}"
    );
    assert_eq!(
        untracked_shape, tracked_shape,
        "the ordinary decode path must produce the same node graph as the certified packet"
    );

    // Every foreign key must resolve inside its own file, on both lanes. A
    // dangling parent_id is exactly what weaker validation would let through.
    for file_id in [TRACKED_FILE_ID, UNTRACKED_FILE_ID] {
        let dangling = lix
            .execute(
                "SELECT child.id FROM markdown_node AS child \
                 WHERE child.lixcol_file_id = $1 AND child.parent_id IS NOT NULL \
                   AND NOT EXISTS ( \
                     SELECT 1 FROM markdown_node AS parent \
                     WHERE parent.lixcol_file_id = $1 AND parent.id = child.parent_id)",
                &[Value::Text(file_id.to_owned())],
            )
            .await
            .expect("foreign-key resolution should query");
        assert_eq!(
            dangling.len(),
            0,
            "file '{file_id}' has markdown_node rows whose parent_id does not resolve"
        );
    }

    // Enforcement must also be live, not merely satisfied by construction:
    // deleting a referenced parent has to be rejected on both lanes alike.
    let delete_parent = async |file_id: &str| {
        let parents = lix
            .execute(
                "SELECT DISTINCT parent_id FROM markdown_node \
                 WHERE lixcol_file_id = $1 AND parent_id IS NOT NULL LIMIT 1",
                &[Value::Text(file_id.to_owned())],
            )
            .await
            .expect("parent lookup should query");
        let parent_id = parents.rows()[0]
            .get::<String>("parent_id")
            .expect("a referenced parent id");
        lix.execute(
            "DELETE FROM markdown_node WHERE lixcol_file_id = $1 AND id = $2",
            &[
                Value::Text(file_id.to_owned()),
                Value::Text(parent_id.clone()),
            ],
        )
        .await
        .err()
        .map(|error| error.code)
    };
    let tracked_delete = delete_parent(TRACKED_FILE_ID).await;
    let untracked_delete = delete_parent(UNTRACKED_FILE_ID).await;
    assert_eq!(
        untracked_delete, tracked_delete,
        "deleting a referenced parent must fail (or succeed) identically on both lanes"
    );

    lix.close().await.expect("fk workspace should close");
}

/// Prior rows plus a fresh parse must compose on the ordinary decode path.
///
/// The collection-replacement marker a certified packet carries
/// (`complete_file_state`) is not produced on the decode path. The argument
/// that this is harmless is that `open_file` only runs when no prior row
/// state exists under the selected plugin, because a previous owner's rows are
/// tombstoned separately. This measures that argument in the case where it
/// would fail: rows already exist for the file when it is parsed fresh again.
#[tokio::test]
async fn a_fresh_reparse_after_ownership_loss_replaces_untracked_rows_like_tracked_ones() {
    const TRACKED_FILE_ID: &str = "01900000-0000-7000-8000-0000000008c1";
    const UNTRACKED_FILE_ID: &str = "01900000-0000-7000-8000-0000000008c2";
    const TRACKED_PATH: &str = "/replace-tracked.json";
    const UNTRACKED_PATH: &str = "/replace-untracked.json";

    let lix = open_lix().await.expect("replacement workspace should open");
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &build_json_plugin_archive(),
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let write = async |file_id: &str, path: &str, untracked: bool, body: &str| {
        let sql = if untracked {
            "INSERT INTO lix_file (id, path, content, lixcol_untracked) VALUES ($1, $2, $3, true) \
             ON CONFLICT (path) DO UPDATE SET content = excluded.content"
        } else {
            "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3) \
             ON CONFLICT (path) DO UPDATE SET content = excluded.content"
        };
        lix.execute(
            sql,
            &[
                Value::Text(file_id.to_owned()),
                Value::Text(path.to_owned()),
                Value::Blob(body.as_bytes().to_vec().into()),
            ],
        )
        .await
        .expect("json file should write");
    };
    let member_keys = async |file_id: &str| {
        let result = lix
            .execute(
                "SELECT key FROM json_object_member \
                 WHERE lixcol_file_id = $1 ORDER BY key",
                &[Value::Text(file_id.to_owned())],
            )
            .await
            .expect("members should query");
        result
            .rows()
            .iter()
            .map(|row| row.get::<String>("key").unwrap())
            .collect::<Vec<_>>()
    };

    write(TRACKED_FILE_ID, TRACKED_PATH, false, r#"{"alpha":"one"}"#).await;
    write(
        UNTRACKED_FILE_ID,
        UNTRACKED_PATH,
        true,
        r#"{"alpha":"one"}"#,
    )
    .await;
    assert_eq!(member_keys(TRACKED_FILE_ID).await, vec!["alpha".to_owned()]);
    assert_eq!(
        member_keys(UNTRACKED_FILE_ID).await,
        vec!["alpha".to_owned()]
    );

    // Dropping the archive retires the owner and tombstones the prior rows, so
    // the next write parses each file fresh again with history behind it.
    lix.execute(
        "DELETE FROM lix_file WHERE path = '/.lix/plugins/plugin_json.lixplugin'",
        &[],
    )
    .await
    .expect("plugin uninstall should commit");
    install_plugin(&lix, "plugin_json", &build_json_plugin_archive())
        .await
        .expect("plugin reinstall should commit");

    write(TRACKED_FILE_ID, TRACKED_PATH, false, r#"{"beta":"two"}"#).await;
    write(UNTRACKED_FILE_ID, UNTRACKED_PATH, true, r#"{"beta":"two"}"#).await;

    let tracked_after = member_keys(TRACKED_FILE_ID).await;
    let untracked_after = member_keys(UNTRACKED_FILE_ID).await;
    assert_eq!(
        tracked_after,
        vec!["beta".to_owned()],
        "the tracked reparse must not leave the superseded member behind"
    );
    assert_eq!(
        untracked_after, tracked_after,
        "the untracked reparse must replace its rows exactly like the tracked one"
    );

    lix.close()
        .await
        .expect("replacement workspace should close");
}

/// Scopes the blast radius of unskipping reconciliation for untracked writes.
///
/// An untracked file whose path matches no installed plugin must keep working
/// exactly as before: bytes in, bytes out, no rows, no plugin involved.
/// This separates "untracked plugin-owned files are blocked" from the far worse
/// "untracked files are blocked".
#[tokio::test]
async fn untracked_file_matching_no_plugin_is_unaffected_by_reconciliation() {
    const FILE_ID: &str = "01900000-0000-7000-8000-0000000008a3";
    const PATH: &str = "/lane-parity-unmatched.bin";
    const CONTENT: &[u8] = b"\x00\x01\x02 not json, not csv";

    let lix = open_lix().await.expect("workspace should open");
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &build_json_plugin_archive(),
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    lix.execute(
        "INSERT INTO lix_file (id, path, content, lixcol_untracked) VALUES ($1, $2, $3, true)",
        &[
            Value::Text(FILE_ID.to_owned()),
            Value::Text(PATH.to_owned()),
            Value::Blob(CONTENT.to_vec().into()),
        ],
    )
    .await
    .expect("an untracked file matching no plugin must still write");

    assert_eq!(read_file(&lix, PATH).await.unwrap(), Some(CONTENT.to_vec()));

    let untracked = lix
        .execute(
            "SELECT lixcol_untracked FROM lix_file WHERE id = $1",
            &[Value::Text(FILE_ID.to_owned())],
        )
        .await
        .expect("descriptor should query");
    assert_eq!(untracked.len(), 1);
    assert_eq!(untracked.rows()[0].values(), &[Value::Boolean(true)]);

    lix.close().await.expect("workspace should close");
}

#[tokio::test]
async fn v2_csv_blob_api_preserves_multiplayer_authority_and_rollback() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &archive,
        &["csv_table", "csv_row"],
    )
    .await;

    let path = "/multiplayer.csv";
    let initial = b"first,one\nsecond,two\nthird,three\n".to_vec();
    write_file(&lix, path, initial.clone()).await.unwrap();
    let _file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();

    let first = lix.open_another_session().await.unwrap();
    let second = lix.open_another_session().await.unwrap();
    assert_eq!(
        read_file(&first, path).await.unwrap(),
        Some(initial.clone())
    );
    assert_eq!(
        read_file(&second, path).await.unwrap(),
        Some(initial.clone())
    );

    let first_edit = b"first,ONE\nsecond,two\nthird,three\n".to_vec();
    let second_edit = b"first,one\nsecond,TWO\nthird,three\n".to_vec();

    write_file(&first, path, first_edit).await.unwrap();

    // This session still edits its exact accepted observation, so the
    // validated submitted bytes are already the authoritative successor. The
    // shared renderer is needed only when replaying a historical sparse delta
    // onto a newer accepted document.

    write_file(&second, path, second_edit).await.unwrap();

    let composed = b"first,ONE\nsecond,TWO\nthird,three\n".to_vec();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(composed.clone()));

    // Both sessions observed the same row version. Transaction commit order is
    // the deterministic LWW tiebreaker for their edits to that row.
    let lww_first = lix.open_another_session().await.unwrap();
    let lww_second = lix.open_another_session().await.unwrap();
    assert_eq!(
        read_file(&lww_first, path).await.unwrap(),
        Some(composed.clone())
    );
    assert_eq!(read_file(&lww_second, path).await.unwrap(), Some(composed));
    write_file(
        &lww_first,
        path,
        b"first,ONE\nsecond,TWO\nthird,THREE-A\n".to_vec(),
    )
    .await
    .unwrap();
    write_file(
        &lww_second,
        path,
        b"first,ONE\nsecond,TWO\nthird,THREE-B\n".to_vec(),
    )
    .await
    .unwrap();
    let lww = b"first,ONE\nsecond,TWO\nthird,THREE-B\n".to_vec();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(lww.clone()));

    // A deletion detected from a historical private view is applied to the
    // current renderer document, so an earlier same-row edit does not revive
    // the deleted identity.
    let edit_session = lix.open_another_session().await.unwrap();
    let delete_session = lix.open_another_session().await.unwrap();
    assert_eq!(
        read_file(&edit_session, path).await.unwrap(),
        Some(lww.clone())
    );
    assert_eq!(read_file(&delete_session, path).await.unwrap(), Some(lww));
    write_file(
        &edit_session,
        path,
        b"first,ONE\nsecond,TWO-A\nthird,THREE-B\n".to_vec(),
    )
    .await
    .unwrap();
    write_file(
        &delete_session,
        path,
        b"first,ONE\nthird,THREE-B\n".to_vec(),
    )
    .await
    .unwrap();
    let deleted = b"first,ONE\nthird,THREE-B\n".to_vec();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(deleted.clone()));

    // Conflict objects are not modeled yet. A session that never received the
    // file therefore applies its complete submitted document as last-write-wins.
    let blind = lix.open_another_session().await.unwrap();
    write_file(&blind, path, b"first,ONE\n".to_vec())
        .await
        .unwrap();
    let one_row = b"first,ONE\n".to_vec();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(one_row.clone()));

    // A rolled-back successor is discarded; the accepted actor and its exact
    // observation remain usable for a later committed transition.
    let rollback_session = lix.open_another_session().await.unwrap();
    assert_eq!(
        read_file(&rollback_session, path).await.unwrap(),
        Some(one_row.clone())
    );
    let mut transaction = rollback_session.begin_transaction().await.unwrap();
    transaction
        .execute(
            "UPDATE lix_file SET content = $1 WHERE path = $2",
            &[
                Value::Blob(b"first,ROLLED-BACK\ninserted,ROLLBACK\n".to_vec().into()),
                Value::Text(path.to_string()),
            ],
        )
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(one_row));
    write_file(&rollback_session, path, b"first,COMMITTED\n".to_vec())
        .await
        .unwrap();
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(b"first,COMMITTED\n".to_vec())
    );

    let insert_session = lix.open_another_session().await.unwrap();
    assert_eq!(
        read_file(&insert_session, path).await.unwrap(),
        Some(b"first,COMMITTED\n".to_vec())
    );
    write_file(
        &insert_session,
        path,
        b"first,COMMITTED\ninserted,COMMITTED\n".to_vec(),
    )
    .await
    .unwrap();

    for session in [
        first,
        second,
        lww_first,
        lww_second,
        edit_session,
        delete_session,
        blind,
        rollback_session,
        insert_session,
    ] {
        session.close().await.unwrap();
    }
    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_stale_observation_composes_a_keyless_create_with_a_concurrent_edit() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &archive,
        &["csv_table", "csv_row"],
    )
    .await;

    let path = "/concurrent-create.csv";
    let initial = b"first,one\n".to_vec();
    write_file(&lix, path, initial.clone()).await.unwrap();
    let _file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();

    let edit_session = lix.open_another_session().await.unwrap();
    let create_session = lix.open_another_session().await.unwrap();
    assert_eq!(
        read_file(&edit_session, path).await.unwrap(),
        Some(initial.clone())
    );
    assert_eq!(
        read_file(&create_session, path).await.unwrap(),
        Some(initial)
    );

    write_file(&edit_session, path, b"first,ONE\n".to_vec())
        .await
        .unwrap();
    write_file(&create_session, path, b"first,one\nsecond,two\n".to_vec())
        .await
        .expect("a keyless create from a stale observation should render onto current bytes");

    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(b"first,ONE\nsecond,two\n".to_vec())
    );

    edit_session.close().await.unwrap();
    create_session.close().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn csv_byte_edit_after_semantic_render_uses_successor_row_boundaries() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &build_csv_plugin_archive(),
        &["csv_table", "csv_row"],
    )
    .await;

    let path = "/semantic-then-byte.csv";
    write_file(&lix, path, b"short,x\nsecond,y\n".to_vec())
        .await
        .unwrap();
    let file_id = file_id_at_path(&lix, path).await;
    let initial = active_csv_rows(&lix, &file_id).await;
    let first_id = csv_row_id(&initial, &["short", "x"]);
    let second_id = csv_row_id(&initial, &["second", "y"]);

    lix.execute(
        "UPDATE csv_row SET cells = $1 WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Jsonb(serde_json::json!(["much-longer", "x"]).into()),
            Value::Text(first_id.clone()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .unwrap();
    let after_semantic = b"much-longer,x\nsecond,y\n".to_vec();
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(after_semantic.clone())
    );

    let after_followup = b"much-longer,x\nsecond,z\n".to_vec();
    write_file(&lix, path, after_followup.clone())
        .await
        .expect("a byte edit after semantic rendering must not use stale CSV row offsets");
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(after_followup));
    let rows = active_csv_rows(&lix, &file_id).await;
    assert_eq!(csv_row_id(&rows, &["much-longer", "x"]), first_id);
    assert_eq!(csv_row_id(&rows, &["second", "z"]), second_id);

    lix.close().await.unwrap();
}

#[tokio::test]
async fn csv_row_structure_edits_use_full_reconciliation() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &build_csv_plugin_archive(),
        &["csv_table", "csv_row"],
    )
    .await;

    let path = "/row-structure.csv";
    write_file(&lix, path, b"a\nb\n".to_vec()).await.unwrap();
    let file_id = file_id_at_path(&lix, path).await;
    assert_eq!(active_csv_rows(&lix, &file_id).await.len(), 2);

    write_file(&lix, path, b"a,b\n".to_vec())
        .await
        .expect("replacing a row terminator must use full CSV reconciliation");
    let rows = active_csv_rows(&lix, &file_id).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cells, ["a", "b"]);
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(b"a,b\n".to_vec())
    );

    write_file(&lix, path, b"old,one\nlast,two\n".to_vec())
        .await
        .unwrap();
    let before_insert = active_csv_rows(&lix, &file_id).await;
    let old_id = csv_row_id(&before_insert, &["old", "one"]);
    write_file(&lix, path, b"new,zero\nold,one\nlast,two\n".to_vec())
        .await
        .expect("prepending a row should use structural reconciliation");
    lix.execute(
        "UPDATE csv_row SET cells = $1 WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Jsonb(serde_json::json!(["old", "ONE"]).into()),
            Value::Text(old_id.clone()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("semantic update after structural insert must use durable row identities");
    let after_semantic = active_csv_rows(&lix, &file_id).await;
    assert_eq!(csv_row_id(&after_semantic, &["old", "ONE"]), old_id);
    assert!(
        after_semantic
            .iter()
            .any(|row| row.cells == ["new", "zero"])
    );
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(b"new,zero\nold,ONE\nlast,two\n".to_vec())
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_markdown_roundtrips_gfm_and_renders_one_direct_row_edit() {
    let archive = build_markdown_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown",
        &archive,
        &["markdown_node"],
    )
    .await;

    let path = "/component-v2.md";
    let source = b"# Heading\n\nParagraph with **bold** text.\n".to_vec();
    write_file(&lix, path, source.clone()).await.unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(source));

    let nodes = lix
        .execute(
            "SELECT id, kind, payload_json FROM markdown_node ORDER BY kind",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        nodes
            .rows()
            .iter()
            .map(|row| row.get::<String>("kind").unwrap())
            .collect::<Vec<_>>(),
        vec![
            "document".to_owned(),
            "heading".to_owned(),
            "paragraph".to_owned()
        ]
    );
    assert!(
        nodes
            .rows()
            .iter()
            .all(|row| row.get::<String>("id").is_ok_and(|id| {
                uuid::Uuid::parse_str(&id).is_ok_and(|id| id.get_version_num() == 7)
            })),
        "every Markdown v2 node, including the document root, must use a UUIDv7"
    );
    let paragraph = nodes
        .rows()
        .iter()
        .find(|row| {
            row.get::<String>("kind")
                .is_ok_and(|kind| kind == "paragraph")
        })
        .unwrap();
    let paragraph_id = paragraph.get::<String>("id").unwrap();
    assert_eq!(paragraph_id.len(), 36);

    let payload_json = serde_json::json!({
        "inline": [{
            "type": "text",
            "value": "Edited paragraph with a much longer tail."
        }]
    })
    .to_string();
    lix.execute(
        "UPDATE markdown_node SET payload_json = $1 WHERE id = $2",
        &[Value::Text(payload_json), Value::Text(paragraph_id)],
    )
    .await
    .unwrap();
    let after_semantic = b"# Heading\n\nEdited paragraph with a much longer tail.\n".to_vec();
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(after_semantic.clone())
    );
    let after_followup = String::from_utf8(after_semantic)
        .unwrap()
        .replacen("tail", "TAIL", 1)
        .into_bytes();
    write_file(&lix, path, after_followup.clone())
        .await
        .expect("a byte edit after semantic rendering must not use stale Markdown spans");
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(after_followup));
    assert!(
        lix.execute(
            "SELECT payload_json FROM markdown_node WHERE kind = 'paragraph'",
            &[],
        )
        .await
        .unwrap()
        .rows()
        .first()
        .is_some_and(|row| jsonb_column_contains(row, "payload_json", "TAIL"))
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v3_markdown_certified_open_sparse_successor_history_and_reopen() {
    let root = tempfile::tempdir().expect("create v3 Markdown directory");
    let lix = open_rocksdb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown",
        &build_markdown_plugin_archive(),
        &["markdown_node"],
    )
    .await;

    let path = "/component-v3.md";
    let before = b"# Heading\n\nParagraph with **bold** text.\n".to_vec();

    write_file(&lix, path, before.clone()).await.unwrap();

    assert_eq!(read_file(&lix, path).await.unwrap(), Some(before));
    assert_eq!(
        lix.execute("SELECT COUNT(*) AS count FROM markdown_node", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<i64>("count")
            .unwrap(),
        3
    );

    let after = b"# Heading\n\nParagraph with **bold** text and a tail.\n".to_vec();

    write_file(&lix, path, after.clone()).await.unwrap();

    assert_eq!(read_file(&lix, path).await.unwrap(), Some(after.clone()));
    let current = lix
        .execute(
            "SELECT kind, payload_json FROM markdown_node ORDER BY kind",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(current.rows().len(), 3);
    assert!(
        current
            .rows()
            .iter()
            .any(|row| jsonb_column_contains(row, "payload_json", "a tail")),
        "the sparse successor must overlay the immutable opening segment"
    );
    let historical = lix
        .execute(
            "SELECT kind, payload_json FROM lix_history('markdown_node') \
             WHERE lixcol_depth = 1 ORDER BY kind",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(historical.rows().len(), 3);
    assert!(
        historical
            .rows()
            .iter()
            .any(|row| jsonb_column_contains(row, "payload_json", "bold")),
        "the opening certified segment must remain queryable through history"
    );
    lix.close().await.unwrap();

    let reopened = open_rocksdb_lix(root.path()).await;
    assert_eq!(
        read_file(&reopened, path).await.unwrap(),
        Some(after),
        "exact Markdown bytes must survive RocksDB reopen"
    );
    assert_eq!(
        reopened
            .execute("SELECT COUNT(*) AS count FROM markdown_node", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<i64>("count")
            .unwrap(),
        3
    );
    let after_reopen = b"# Heading\n\nParagraph with **bold** text and a TAIL.\n".to_vec();

    write_file(&reopened, path, after_reopen.clone())
        .await
        .unwrap();

    assert_eq!(
        read_file(&reopened, path).await.unwrap(),
        Some(after_reopen)
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn v3_markdown_cold_hydration_preserves_later_namespace_ids() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown",
        &build_markdown_plugin_archive(),
        &["markdown_node"],
    )
    .await;
    let path = "/markdown-multi-namespace.md";
    write_file(&lix, path, b"Old paragraph.\n".to_vec())
        .await
        .unwrap();
    let file_id = file_id_at_path(&lix, path).await;
    let original = lix
        .execute(
            "SELECT id FROM markdown_node \
             WHERE kind = 'paragraph' AND lixcol_file_id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();

    write_file(&lix, path, b"Old paragraph.\n\nNew paragraph.\n".to_vec())
        .await
        .unwrap();
    let rows = lix
        .execute(
            "SELECT id, payload_json FROM markdown_node \
             WHERE kind = 'paragraph' AND lixcol_file_id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    let inserted = rows
        .rows()
        .iter()
        .find(|row| jsonb_column_contains(row, "payload_json", "New paragraph."))
        .and_then(|row| row.get::<String>("id").ok())
        .expect("inserted paragraph identity");
    assert_ne!(inserted, original);

    for index in 0..20 {
        write_file(
            &lix,
            &format!("/markdown-evict-{index}.md"),
            format!("Eviction paragraph {index}.\n").into_bytes(),
        )
        .await
        .unwrap();
    }
    lix.execute(
        "UPDATE markdown_node SET payload_json = $1 \
         WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Text(
                serde_json::json!({
                    "inline": [{"type": "text", "value": "Edited after hydration."}]
                })
                .to_string(),
            ),
            Value::Text(inserted.clone()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("semantic edit must address the hydrated later-namespace identity");
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(b"Old paragraph.\n\nEdited after hydration.\n".to_vec())
    );

    write_file(
        &lix,
        path,
        b"OLD paragraph.\n\nEdited after hydration.\n\nThird paragraph.\n".to_vec(),
    )
    .await
    .expect("full file reconciliation must reuse the hydrated identity graph");
    let retained = lix
        .execute(
            "SELECT id, payload_json FROM markdown_node \
             WHERE kind = 'paragraph' AND lixcol_file_id = $1",
            &[Value::Text(file_id)],
        )
        .await
        .unwrap();
    let retained_id = retained
        .rows()
        .iter()
        .find(|row| jsonb_column_contains(row, "payload_json", "Edited after hydration."))
        .and_then(|row| row.get::<String>("id").ok())
        .expect("edited paragraph remains queryable");
    assert_eq!(retained_id, inserted);

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v3_markdown_one_large_block_spans_state_pages() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown",
        &build_markdown_plugin_archive(),
        &["markdown_node"],
    )
    .await;
    let path = "/markdown-large-block.md";
    let mut source = b"```\n".to_vec();
    source.extend(std::iter::repeat_n(b'x', 1024 * 1024 + 257));
    source.extend_from_slice(b"\n```\n");

    write_file(&lix, path, source.clone())
        .await
        .expect("one block may span bounded Markdown state pages");
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(source));

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v3_markdown_noncanonical_source_stays_in_file_arena_not_semantic_root() {
    let root = tempfile::tempdir().expect("create noncanonical v3 Markdown directory");
    let lix = open_rocksdb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown",
        &build_markdown_plugin_archive(),
        &["markdown_node"],
    )
    .await;

    let path = "/noncanonical-v3.md";
    let before =
        b"---\nDateApproved: 6/10/2020\n---\n\n\n# Title\n\nA large untouched suffix.\n".to_vec();
    write_file(&lix, path, before.clone()).await.unwrap();
    let document = lix
        .execute(
            "SELECT format_json FROM markdown_node WHERE kind = 'document'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(document.rows().len(), 1);
    assert!(
        !document.rows()[0]
            .get::<serde_json::Value>("format_json")
            .unwrap()
            .to_string()
            .contains("lexical_fallback_base64"),
        "v3 semantic state must not duplicate accepted source bytes"
    );

    let after = String::from_utf8(before)
        .unwrap()
        .replacen("6/10", "7/9", 1)
        .into_bytes();

    write_file(&lix, path, after.clone()).await.unwrap();

    assert_eq!(read_file(&lix, path).await.unwrap(), Some(after.clone()));
    lix.close().await.unwrap();

    let reopened = open_rocksdb_lix(root.path()).await;
    assert_eq!(read_file(&reopened, path).await.unwrap(), Some(after));
    reopened.close().await.unwrap();
}

fn markdown_byte_roundtrip_fixture() -> Vec<u8> {
    let mut source = b"---\nDateApproved: 6/10/2020\nOwner: team\n---\n\n# Competitors\n\n*Counter:\n\n(~26 users)\n\nA paragraph directly followed by\n- list item\n\n**knowledge base / shared workspace agents read and\nwrite to.**\n\n```rust\nlet value = *Counter;\n```\n\n".to_vec();
    for index in 0..24 {
        source.extend_from_slice(
            format!(
                "## Peer {index}\n\nPeer {index} has *single-asterisk emphasis*, Unicode λ 😀, and `code`.\n\n"
            )
            .as_bytes(),
        );
    }
    let mut padding_index = 0;
    while source.len() < 3_210 {
        source.extend_from_slice(format!("Padding paragraph {padding_index}.\n\n").as_bytes());
        padding_index += 1;
    }
    source
}

async fn qualify_markdown_semantic_child_edit_after_reopen<S>(lix: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let path = "/company/competitors.md";
    let file_id = file_id_at_path(lix, path).await;
    let paragraphs = lix
        .execute(
            "SELECT id, payload_json FROM markdown_node \
             WHERE kind = 'paragraph' AND lixcol_file_id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .expect("restored Markdown paragraphs should query");
    let paragraph_count = paragraphs.rows().len();
    let edited_id = paragraphs
        .rows()
        .iter()
        .find(|row| jsonb_column_contains(row, "payload_json", "Peer 12 has"))
        .and_then(|row| row.get::<String>("id").ok())
        .expect("restored Markdown target paragraph");
    lix.execute(
        "UPDATE markdown_node SET payload_json = $1 \
         WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Text(
                serde_json::json!({
                    "inline": [{
                        "type": "text",
                        "value": "Peer 12 was edited after durable restore."
                    }]
                })
                .to_string(),
            ),
            Value::Text(edited_id),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("semantic child edit after durable restore should commit");

    let rendered = String::from_utf8(
        read_file(lix, path)
            .await
            .expect("restored Markdown file should read")
            .expect("restored Markdown file should exist"),
    )
    .expect("rendered Markdown should be UTF-8");
    assert!(rendered.contains("Peer 12 was edited after durable restore."));
    for index in (0..24).filter(|index| *index != 12) {
        assert!(
            rendered.contains(&format!("Peer {index} has")),
            "unrelated Peer {index} block was lost"
        );
    }
    let successor_count = lix
        .execute(
            "SELECT COUNT(*) AS count FROM markdown_node \
             WHERE kind = 'paragraph' AND lixcol_file_id = $1",
            &[Value::Text(file_id)],
        )
        .await
        .expect("successor Markdown paragraphs should query")
        .rows()[0]
        .get::<i64>("count")
        .expect("successor Markdown paragraph count");
    assert_eq!(successor_count, paragraph_count as i64);
}

async fn qualify_markdown_server_style_branch<S>(lix: &Lix<S>, expected: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let main_branch_id = lix.active_branch_id().await.unwrap();
    let branch = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-0000000005f1".to_owned()),
            name: "Markdown byte roundtrip branch".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("Markdown branch should create");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: branch.id,
    })
    .await
    .expect("Markdown branch should activate");
    assert_eq!(
        read_file(lix, "/company/competitors.md").await.unwrap(),
        Some(expected.to_vec())
    );
    let branch_bytes = b"# Branch\n\nBranch bytes stay exact.\n".to_vec();
    write_file(lix, "/branch-only.md", branch_bytes.clone())
        .await
        .expect("branch Markdown write should commit");
    assert_eq!(
        read_file(lix, "/branch-only.md").await.unwrap(),
        Some(branch_bytes)
    );
    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id,
    })
    .await
    .expect("main Markdown branch should reactivate");
}

async fn qualify_markdown_byte_roundtrip<S>(lix: &Lix<S>, expected: &[u8], exercise_branch: bool)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let path = "/company/competitors.md";
    write_file(lix, path, expected.to_vec())
        .await
        .expect("Markdown fixture write should commit");
    assert_eq!(read_file(lix, path).await.unwrap(), Some(expected.to_vec()));
    if exercise_branch {
        let main_branch_id = lix.active_branch_id().await.unwrap();
        let branch = lix
            .create_branch(CreateBranchOptions {
                id: Some("01920000-0000-7000-8000-0000000005f1".to_owned()),
                name: "Markdown byte roundtrip branch".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("Markdown branch should create");
        lix.switch_branch(SwitchBranchOptions {
            branch_id: branch.id.clone(),
        })
        .await
        .expect("Markdown branch should activate");
        assert_eq!(read_file(lix, path).await.unwrap(), Some(expected.to_vec()));
        let branch_path = "/branch-only.md";
        let branch_bytes = b"# Branch\n\nBranch bytes stay exact.\n".to_vec();
        write_file(lix, branch_path, branch_bytes.clone())
            .await
            .expect("branch Markdown write should commit");
        assert_eq!(
            read_file(lix, branch_path).await.unwrap(),
            Some(branch_bytes)
        );
        lix.switch_branch(SwitchBranchOptions {
            branch_id: main_branch_id,
        })
        .await
        .expect("main Markdown branch should reactivate");
    }
    lix.create_checkpoint()
        .await
        .expect("Markdown checkpoint should commit");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(read_file(lix, path).await.unwrap(), Some(expected.to_vec()));

    let batch = (0..17)
        .map(|index| {
            let path = format!("/batch/peer-{index}.md");
            let content = format!(
                "# Peer {index}\n\n*Counter:\n\n(~{} users)\n\nparagraph {index}\n- list item\n\n**wrapped strong {index}\nand Unicode λ 😀.**\n",
                index + 1
            )
            .into_bytes();
            (path, content)
        })
        .collect::<Vec<_>>();
    let mut transaction = lix
        .begin_transaction()
        .await
        .expect("17-file Markdown transaction should open");
    for (path, content) in &batch {
        transaction
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    Value::Text(path.clone()),
                    Value::Blob(content.clone().into()),
                ],
            )
            .await
            .expect("17-file Markdown batch row should stage");
    }
    transaction
        .commit()
        .await
        .expect("17-file Markdown batch should commit");
    for (path, content) in &batch {
        assert_eq!(
            read_file(lix, path).await.unwrap(),
            Some(content.clone()),
            "batch file {path} must not inherit another file's parser state"
        );
    }
}

#[tokio::test]
async fn v3_markdown_byte_roundtrip_rocksdb_lifecycle_and_17_file_batch() {
    let root = tempfile::tempdir().expect("create RocksDB Markdown roundtrip directory");
    let lix = open_rocksdb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown",
        &build_markdown_plugin_archive(),
        &["markdown_node"],
    )
    .await;
    let expected = markdown_byte_roundtrip_fixture();
    qualify_markdown_byte_roundtrip(&lix, &expected, true).await;
    lix.close().await.unwrap();

    let reopened = open_rocksdb_lix(root.path()).await;
    assert_eq!(
        read_file(&reopened, "/company/competitors.md")
            .await
            .unwrap(),
        Some(expected)
    );
    qualify_markdown_semantic_child_edit_after_reopen(&reopened).await;
    reopened.close().await.unwrap();
}

#[test]
fn v3_markdown_byte_roundtrip_slatedb_lifecycle_and_17_file_batch() {
    std::thread::Builder::new()
        .name("markdown-slatedb-roundtrip".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build SlateDB Markdown runtime");
            runtime.block_on(async {
                let root =
                    tempfile::tempdir().expect("create SlateDB Markdown roundtrip directory");
                let lix = open_slatedb_lix(root.path()).await;
                install_reference_plugin_in_blank_registry(
                    &lix,
                    "plugin_markdown",
                    &build_markdown_plugin_archive(),
                    &["markdown_node"],
                )
                .await;
                let expected = markdown_byte_roundtrip_fixture();
                qualify_markdown_byte_roundtrip(&lix, &expected, false).await;
                lix.close().await.unwrap();

                let reopened = open_slatedb_lix(root.path()).await;
                assert_eq!(
                    read_file(&reopened, "/company/competitors.md")
                        .await
                        .unwrap(),
                    Some(expected)
                );
                qualify_markdown_semantic_child_edit_after_reopen(&reopened).await;
                reopened.close().await.unwrap();
            });
        })
        .expect("spawn SlateDB Markdown roundtrip thread")
        .join()
        .expect("SlateDB Markdown roundtrip thread should finish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_markdown_byte_roundtrip_slatedb_server_style_runtime_stack_guard() {
    let root = tempfile::tempdir().expect("create diagnostic SlateDB directory");
    let lix = open_slatedb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown",
        &build_markdown_plugin_archive(),
        &["markdown_node"],
    )
    .await;
    let expected = markdown_byte_roundtrip_fixture();
    write_file(&lix, "/company/competitors.md", expected.clone())
        .await
        .expect("server-style SlateDB Markdown write");
    assert_eq!(
        read_file(&lix, "/company/competitors.md").await.unwrap(),
        Some(expected.clone())
    );
    qualify_markdown_server_style_branch(&lix, &expected).await;
    lix.create_checkpoint()
        .await
        .expect("server-style SlateDB branch checkpoint");
    tokio::time::sleep(Duration::from_millis(100)).await;
    lix.close().await.unwrap();

    let reopened = open_slatedb_lix(root.path()).await;
    assert_eq!(
        read_file(&reopened, "/company/competitors.md")
            .await
            .unwrap(),
        Some(expected)
    );
    qualify_markdown_semantic_child_edit_after_reopen(&reopened).await;
    reopened.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_markdown_byte_roundtrip_slatedb_server_style_checkpoint_guard() {
    let root = tempfile::tempdir().expect("create SlateDB checkpoint guard directory");
    let lix = open_slatedb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown",
        &build_markdown_plugin_archive(),
        &["markdown_node"],
    )
    .await;
    let expected = markdown_byte_roundtrip_fixture();
    write_file(&lix, "/company/competitors.md", expected.clone())
        .await
        .expect("server-style SlateDB Markdown write");
    lix.create_checkpoint()
        .await
        .expect("server-style SlateDB Markdown checkpoint");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        read_file(&lix, "/company/competitors.md").await.unwrap(),
        Some(expected.clone())
    );
    lix.close().await.unwrap();

    let reopened = open_slatedb_lix(root.path()).await;
    assert_eq!(
        read_file(&reopened, "/company/competitors.md")
            .await
            .unwrap(),
        Some(expected)
    );
    reopened.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_markdown_byte_roundtrip_slatedb_server_style_batch_guard() {
    let root = tempfile::tempdir().expect("create SlateDB batch guard directory");
    let lix = open_slatedb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown",
        &build_markdown_plugin_archive(),
        &["markdown_node"],
    )
    .await;
    let expected = markdown_byte_roundtrip_fixture();
    write_file(&lix, "/company/competitors.md", expected)
        .await
        .expect("server-style SlateDB Markdown write");
    let mut transaction = lix.begin_transaction().await.unwrap();
    for index in 0..17 {
        let path = format!("/batch/peer-{index}.md");
        let content = format!(
            "# Peer {index}\n\n*Counter:\n\n(~{} users)\n\nparagraph {index}\n- list item\n\n**wrapped strong {index}\nand Unicode λ 😀.**\n",
            index + 1
        );
        transaction
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[Value::Text(path), Value::Blob(content.into_bytes().into())],
            )
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_markdown_merges_unrelated_rows_and_regenerates_derived_bytes() {
    let archive = build_markdown_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown",
        &archive,
        &["markdown_node"],
    )
    .await;

    let path = "/merge-v2.md";
    write_file(
        &lix,
        path,
        b"First paragraph.\n\nSecond paragraph.\n".to_vec(),
    )
    .await
    .unwrap();
    let paragraphs = lix
        .execute(
            "SELECT id, payload_json FROM markdown_node WHERE kind = 'paragraph'",
            &[],
        )
        .await
        .unwrap();
    let paragraph_id = |needle: &str| {
        paragraphs
            .rows()
            .iter()
            .find(|row| jsonb_column_contains(row, "payload_json", needle))
            .and_then(|row| row.get::<String>("id").ok())
            .unwrap_or_else(|| panic!("paragraph containing '{needle}' should exist"))
    };
    let first_id = paragraph_id("First paragraph.");
    let second_id = paragraph_id("Second paragraph.");
    let main_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-000000000507".to_owned()),
            name: "Markdown derived blob source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    lix.execute(
        "UPDATE markdown_node SET payload_json = $1 WHERE id = $2",
        &[
            Value::Text(
                serde_json::json!({"inline":[{"type":"text","value":"First from target."}]})
                    .to_string(),
            ),
            Value::Text(first_id),
        ],
    )
    .await
    .unwrap();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    lix.execute(
        "UPDATE markdown_node SET payload_json = $1 WHERE id = $2",
        &[
            Value::Text(
                serde_json::json!({"inline":[{"type":"text","value":"Second from source."}]})
                    .to_string(),
            ),
            Value::Text(second_id),
        ],
    )
    .await
    .unwrap();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id,
    })
    .await
    .unwrap();

    let preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .unwrap();
    assert!(
        preview.conflicts.is_empty(),
        "the materialized blob is derived plugin state: {:?}",
        preview.conflicts
    );
    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, path).await.unwrap().as_deref(),
        Some(b"First from target.\n\nSecond from source.\n".as_slice())
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_markdown_same_paragraph_branch_merge_composes_word_edge_inserts() {
    let archive = build_markdown_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown",
        &archive,
        &["markdown_node"],
    )
    .await;

    let path = "/paragraph-conflict.md";
    write_file(&lix, path, b"wonder\n".to_vec())
        .await
        .expect("base paragraph should import");
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-000000000508".to_owned()),
            name: "Markdown paragraph conflict source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    write_file(&lix, path, b"prewonder\n".to_vec())
        .await
        .expect("target prefix insertion should commit");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    write_file(&lix, path, b"wonderful\n".to_vec())
        .await
        .expect("source suffix insertion should commit");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id,
    })
    .await
    .unwrap();

    let preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .expect("plugin-owned paragraph conflict should preview");
    assert!(
        preview.conflicts.is_empty(),
        "the static Markdown resolver owns the paragraph conflict: {:?}",
        preview.conflicts
    );

    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("disjoint paragraph inserts should merge");
    assert_eq!(
        read_file(&lix, path).await.unwrap().as_deref(),
        Some(b"prewonderful\n".as_slice()),
        "the prefix and suffix insertions must both survive",
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v3_markdown_same_paragraph_branch_merge_composes_word_edge_inserts() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown",
        &build_markdown_plugin_archive(),
        &["markdown_node"],
    )
    .await;

    let path = "/v3-paragraph-conflict.md";
    write_file(&lix, path, b"wonder\n".to_vec())
        .await
        .expect("base paragraph should import");
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-000000000608".to_owned()),
            name: "Markdown v3 paragraph conflict source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    write_file(&lix, path, b"prewonder\n".to_vec())
        .await
        .expect("target prefix insertion should commit");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    write_file(&lix, path, b"wonderful\n".to_vec())
        .await
        .expect("source suffix insertion should commit");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id,
    })
    .await
    .unwrap();

    let preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .expect("v3 plugin-owned paragraph conflict should preview");
    assert!(preview.conflicts.is_empty(), "{:?}", preview.conflicts);

    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("v3 disjoint paragraph inserts should merge");
    assert_eq!(
        read_file(&lix, path).await.unwrap().as_deref(),
        Some(b"prewonderful\n".as_slice())
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_same_row_branch_merge_composes_distinct_cells() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &archive,
        &["csv_table", "csv_row"],
    )
    .await;

    let path = "/row-conflict.csv";
    write_file(&lix, path, b"alpha,one,red\n".to_vec())
        .await
        .expect("base row should import");
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-000000000509".to_owned()),
            name: "CSV row conflict source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    write_file(&lix, path, b"ALPHA,one,red\n".to_vec())
        .await
        .expect("target first-cell edit should commit");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    write_file(&lix, path, b"alpha,one,BLUE\n".to_vec())
        .await
        .expect("source third-cell edit should commit");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id,
    })
    .await
    .unwrap();

    let preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .expect("plugin-owned row conflict should preview");
    assert!(
        preview.conflicts.is_empty(),
        "the static CSV resolver owns the row conflict: {:?}",
        preview.conflicts
    );

    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("distinct CSV cell edits should merge");
    assert_eq!(
        read_file(&lix, path).await.unwrap().as_deref(),
        Some(b"ALPHA,one,BLUE\n".as_slice()),
        "both same-row cell edits must survive",
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v3_csv_same_row_branch_merge_composes_distinct_cells() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &build_csv_plugin_archive(),
        &["csv_table", "csv_row"],
    )
    .await;

    let path = "/v3-row-conflict.csv";
    write_file(&lix, path, b"alpha,one,red\n".to_vec())
        .await
        .expect("base row should import");
    let file_id = file_id_at_path(&lix, path).await;
    let base_row_id = csv_row_id(
        &active_csv_rows(&lix, &file_id).await,
        &["alpha", "one", "red"],
    );
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-00000000060a".to_owned()),
            name: "CSV v3 row conflict source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    write_file(&lix, path, b"ALPHA,one,red\n".to_vec())
        .await
        .expect("target first-cell edit should commit");
    assert_eq!(
        csv_row_id(
            &active_csv_rows(&lix, &file_id).await,
            &["ALPHA", "one", "red"],
        ),
        base_row_id
    );
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    write_file(&lix, path, b"alpha,one,BLU\n".to_vec())
        .await
        .expect("source third-cell edit should commit");
    assert_eq!(
        csv_row_id(
            &active_csv_rows(&lix, &file_id).await,
            &["alpha", "one", "BLU"],
        ),
        base_row_id
    );
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id,
    })
    .await
    .unwrap();

    let preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .expect("v3 plugin-owned row conflict should preview");
    assert!(preview.conflicts.is_empty(), "{:?}", preview.conflicts);

    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("distinct CSV cell edits should merge");

    assert_eq!(
        read_file(&lix, path).await.unwrap().as_deref(),
        Some(b"ALPHA,one,BLU\n".as_slice()),
        "both same-row cell edits must survive",
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_json_unrelated_row_branch_merge_accepts_typed_rows() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &build_json_plugin_archive(),
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/certified-unrelated-merge.json";
    write_file(&lix, path, br#"{"left":"base","right":"base"}"#.to_vec())
        .await
        .expect("base JSON should import");
    let file_id = file_id_at_path(&lix, path).await;
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-00000000050a".to_owned()),
            name: "JSON certified unrelated source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    lix.execute(
        "UPDATE json_object_member SET scalar_json = $1 \
         WHERE parent_id = 'root' AND key = 'left' AND lixcol_file_id = $2",
        &[
            Value::Jsonb(serde_json::json!("target").into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("target JSON member should update");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    lix.execute(
        "UPDATE json_object_member SET scalar_json = $1 \
         WHERE parent_id = 'root' AND key = 'right' AND lixcol_file_id = $2",
        &[
            Value::Jsonb(serde_json::json!("source").into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("source JSON member should update");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id,
    })
    .await
    .unwrap();

    let preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .expect("unrelated typed JSON rows should preview");
    assert!(preview.conflicts.is_empty());
    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("typed JSON rows must remain native while fingerprinting the merge batch");

    let merged = lix
        .execute(
            "SELECT key, scalar_json FROM json_object_member \
             WHERE parent_id = 'root' AND key IN ('left', 'right') \
             AND lixcol_file_id = $1 ORDER BY key",
            &[Value::Text(file_id)],
        )
        .await
        .expect("merged JSON rows should query");
    assert_eq!(merged.len(), 2);
    assert_eq!(
        merged.rows()[0].get::<Value>("scalar_json").unwrap(),
        Value::Jsonb(serde_json::json!("target").into())
    );
    assert_eq!(
        merged.rows()[1].get::<Value>("scalar_json").unwrap(),
        Value::Jsonb(serde_json::json!("source").into())
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_json_same_row_branch_merge_runs_static_resolver_on_typed_rows() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &build_json_plugin_archive(),
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/certified-conflict-merge.json";
    write_file(&lix, path, br#"{"pick":"base"}"#.to_vec())
        .await
        .expect("base JSON should import");
    let file_id = file_id_at_path(&lix, path).await;
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-00000000050b".to_owned()),
            name: "JSON certified conflict source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    lix.execute(
        "UPDATE json_object_member SET scalar_json = $1 \
         WHERE parent_id = 'root' AND key = 'pick' AND lixcol_file_id = $2",
        &[
            Value::Jsonb(serde_json::json!("target").into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .unwrap();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    lix.execute(
        "UPDATE json_object_member SET scalar_json = $1 \
         WHERE parent_id = 'root' AND key = 'pick' AND lixcol_file_id = $2",
        &[
            Value::Jsonb(serde_json::json!("source").into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .unwrap();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id,
    })
    .await
    .unwrap();

    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("the JSON static resolver should accept typed rows");

    let merged = lix
        .execute(
            "SELECT scalar_json FROM json_object_member \
             WHERE parent_id = 'root' AND key = 'pick' AND lixcol_file_id = $1",
            &[Value::Text(file_id)],
        )
        .await
        .unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(
        merged.rows()[0].get::<Value>("scalar_json").unwrap(),
        Value::Jsonb(serde_json::json!("source").into())
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v3_json_same_row_branch_merge_uses_fused_conflict_and_renderer_sinks() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &build_json_plugin_archive(),
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/v3-certified-conflict-merge.json";
    write_file(&lix, path, br#"{"pick":"base"}"#.to_vec())
        .await
        .expect("base JSON should import");
    let file_id = file_id_at_path(&lix, path).await;
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-00000000060b".to_owned()),
            name: "JSON v3 conflict source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    lix.execute(
        "UPDATE json_object_member SET scalar_json = $1 \
         WHERE parent_id = 'root' AND key = 'pick' AND lixcol_file_id = $2",
        &[
            Value::Jsonb(serde_json::json!("target").into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .unwrap();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    lix.execute(
        "UPDATE json_object_member SET scalar_json = $1 \
         WHERE parent_id = 'root' AND key = 'pick' AND lixcol_file_id = $2",
        &[
            Value::Jsonb(serde_json::json!("source").into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .unwrap();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id,
    })
    .await
    .unwrap();

    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("the v3 JSON resolver and renderer should complete atomically");

    let merged = lix
        .execute(
            "SELECT scalar_json FROM json_object_member \
             WHERE parent_id = 'root' AND key = 'pick' AND lixcol_file_id = $1",
            &[Value::Text(file_id)],
        )
        .await
        .unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(
        merged.rows()[0].get::<Value>("scalar_json").unwrap(),
        Value::Jsonb(serde_json::json!("source").into())
    );
    assert_eq!(
        read_file(&lix, path).await.unwrap().as_deref(),
        Some(br#"{"pick":"source"}"#.as_slice())
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_same_cell_merge_uses_canonical_stored_rank() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &archive,
        &["csv_table", "csv_row"],
    )
    .await;

    let path = "/row-canonical-fallback-conflict.csv";
    let base = b"alpha,one,red\n".to_vec();
    let target_bytes = b"TARGET,one,red\n".to_vec();
    let source_bytes = b"SOURCE,one,red\n".to_vec();
    write_file(&lix, path, base).await.unwrap();
    let file_id = file_id_at_path(&lix, path).await;
    let row_id = csv_row_id(
        &active_csv_rows(&lix, &file_id).await,
        &["alpha", "one", "red"],
    );
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-00000000050a".to_owned()),
            name: "CSV row canonical B source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    write_file(&lix, path, source_bytes.clone())
        .await
        .expect("source same-cell edit should commit");
    let source_order = csv_row_ordering(&lix, &file_id, &row_id).await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id.clone(),
    })
    .await
    .unwrap();
    write_file(&lix, path, target_bytes.clone())
        .await
        .expect("target same-cell edit should commit");
    let target_order = csv_row_ordering(&lix, &file_id, &row_id).await;
    assert_ne!(
        source_order, target_order,
        "distinct conflicting rows must have distinct durable ordering tuples"
    );
    let expected = if source_order < target_order {
        target_bytes
    } else {
        source_bytes
    };
    let preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .expect("plugin-owned same-cell conflict should preview");
    assert!(preview.conflicts.is_empty(), "{:?}", preview.conflicts);

    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("same-cell CSV conflict should resolve deterministically");
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(expected),
        "the resolver must take the canonical higher-ranked variant, independent of branch labels"
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v3_csv_same_cell_merge_uses_canonical_stored_rank() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &build_csv_plugin_archive(),
        &["csv_table", "csv_row"],
    )
    .await;

    let path = "/v3-row-canonical-fallback-conflict.csv";
    let base = b"alpha,one,red\n".to_vec();
    let target_bytes = b"TARGE,one,red\n".to_vec();
    let source_bytes = b"SOURC,one,red\n".to_vec();
    write_file(&lix, path, base).await.unwrap();
    let file_id = file_id_at_path(&lix, path).await;
    let row_id = csv_row_id(
        &active_csv_rows(&lix, &file_id).await,
        &["alpha", "one", "red"],
    );
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-00000000060b".to_owned()),
            name: "CSV v3 row canonical B source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    write_file(&lix, path, source_bytes.clone())
        .await
        .expect("source same-cell edit should commit");
    let source_order = csv_row_ordering(&lix, &file_id, &row_id).await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id.clone(),
    })
    .await
    .unwrap();
    write_file(&lix, path, target_bytes.clone())
        .await
        .expect("target same-cell edit should commit");
    let target_order = csv_row_ordering(&lix, &file_id, &row_id).await;
    assert_ne!(source_order, target_order);
    let expected = if source_order < target_order {
        target_bytes
    } else {
        source_bytes
    };

    let preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .expect("v3 plugin-owned same-cell conflict should preview");
    assert!(preview.conflicts.is_empty(), "{:?}", preview.conflicts);

    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("same-cell CSV conflict should resolve deterministically");
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(expected));

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_delete_vs_edit_fails_ownership_without_a_plugin_conflict_api() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &archive,
        &["csv_table", "csv_row"],
    )
    .await;

    let path = "/delete-vs-edit.csv";
    write_file(&lix, path, b"alpha,one,red\n".to_vec())
        .await
        .expect("base CSV should import");
    let file_id = file_id_at_path(&lix, path).await;
    let row_id = csv_row_id(
        &active_csv_rows(&lix, &file_id).await,
        &["alpha", "one", "red"],
    );
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-00000000050b".to_owned()),
            name: "CSV delete versus edit source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    lix.execute(
        "DELETE FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .expect("target file deletion should commit");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    lix.execute(
        "UPDATE csv_row SET cells = $1 \
         WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Jsonb(serde_json::json!(["alpha", "ONE", "red"]).into()),
            Value::Text(row_id.clone()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("source semantic row edit should commit");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id,
    })
    .await
    .unwrap();

    let preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .expect("delete-vs-edit should preview with host-native LWW");
    assert!(preview.conflicts.is_empty(), "{:?}", preview.conflicts);

    let error = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .expect_err("delete-vs-edit currently fails the ordinary ownership constraint");
    assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);

    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        None,
        "the target-side file deletion wins while the source row edit cannot restore its descriptor"
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_rename_and_same_row_edit_fail_without_a_cross_row_conflict_api() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &archive,
        &["csv_table", "csv_row"],
    )
    .await;

    let csv_path = "/descriptor-conflict.csv";
    let tsv_path = "/descriptor-conflict.tsv";
    let base = b"alpha,one,red\n".to_vec();
    write_file(&lix, csv_path, base.clone())
        .await
        .expect("base CSV should import");
    let file_id = file_id_at_path(&lix, csv_path).await;
    let row_id = csv_row_id(
        &active_csv_rows(&lix, &file_id).await,
        &["alpha", "one", "red"],
    );
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-00000000050c".to_owned()),
            name: "CSV rename versus same-row edit source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    lix.execute(
        "UPDATE csv_row SET cells = $1 WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Jsonb(serde_json::json!(["TARGET", "one", "red"]).into()),
            Value::Text(row_id.clone()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("target same-row edit should commit");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    lix.execute(
        "UPDATE lix_file SET path = $1 WHERE path = $2",
        &[
            Value::Text(tsv_path.to_owned()),
            Value::Text(csv_path.to_owned()),
        ],
    )
    .await
    .expect("source descriptor rename should commit");
    lix.execute(
        "UPDATE csv_row SET cells = $1 WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Jsonb(serde_json::json!(["SOURCE", "one", "red"]).into()),
            Value::Text(row_id.clone()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("source same-row edit should commit under the TSV descriptor");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id,
    })
    .await
    .unwrap();

    let preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .expect("descriptor rename and row edit should preview");
    assert!(preview.conflicts.is_empty(), "{:?}", preview.conflicts);

    let error = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .expect_err("descriptor-plus-row reconciliation needs a future cross-row API");
    assert_eq!(error.code, LixError::CODE_UNIQUE);

    assert_eq!(
        read_file(&lix, csv_path).await.unwrap(),
        Some(b"TARGET,one,red\n".to_vec())
    );
    assert_eq!(read_file(&lix, tsv_path).await.unwrap(), None);

    lix.close().await.unwrap();
}

#[tokio::test]
async fn json_first_structural_fallback_preserves_accepted_array_identities() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &build_json_plugin_archive(),
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/array-identity.json";
    write_file(&lix, path, br#"["removed","alpha","beta"]"#.to_vec())
        .await
        .unwrap();
    let file_id = file_id_at_path(&lix, path).await;
    let before = lix
        .execute(
            "SELECT id, scalar_json FROM json_array_item \
             WHERE lixcol_file_id = $1 ORDER BY order_key",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    let alpha_id = before.rows()[1].get::<String>("id").unwrap();
    let beta_id = before.rows()[2].get::<String>("id").unwrap();

    write_file(&lix, path, br#"["alpha","beta"]"#.to_vec())
        .await
        .expect("the first structural fallback should preserve accepted identities");
    let after = lix
        .execute(
            "SELECT id, scalar_json FROM json_array_item \
             WHERE lixcol_file_id = $1 ORDER BY order_key",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(after.rows().len(), 2);
    assert_eq!(
        after.rows()[0].get::<Value>("scalar_json").unwrap(),
        Value::Jsonb(serde_json::json!("alpha").into())
    );
    assert_eq!(after.rows()[0].get::<String>("id").unwrap(), alpha_id);
    assert_eq!(after.rows()[1].get::<String>("id").unwrap(), beta_id);

    lix.execute(
        "UPDATE json_array_item SET scalar_json = $1 \
         WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Jsonb(serde_json::json!("BETA").into()),
            Value::Text(beta_id),
            Value::Text(file_id),
        ],
    )
    .await
    .expect("the preserved durable identity should remain semantically writable");
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(br#"["alpha","BETA"]"#.to_vec())
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn json_scalar_to_container_edit_uses_full_reconciliation() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &build_json_plugin_archive(),
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/scalar-to-container.json";
    write_file(&lix, path, br#"{"value":1}"#.to_vec())
        .await
        .unwrap();
    write_file(&lix, path, br#"{"value":{}}"#.to_vec())
        .await
        .expect("a scalar-to-container edit must use full JSON reconciliation");

    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(br#"{"value":{}}"#.to_vec())
    );
    let member = lix
        .execute(
            "SELECT kind, scalar_json FROM json_object_member WHERE key = 'value'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(member.len(), 1);
    assert_eq!(member.rows()[0].get::<String>("kind").unwrap(), "object");
    assert_eq!(
        member.rows()[0].get::<Value>("scalar_json").unwrap(),
        Value::Null
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn json_scalar_boundary_insert_adds_sibling_through_full_reconciliation() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &build_json_plugin_archive(),
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/scalar-sibling.json";
    write_file(&lix, path, br#"{"a":1}"#.to_vec())
        .await
        .unwrap();
    let successor = br#"{"a":1,"b":2}"#.to_vec();
    write_file(&lix, path, successor.clone())
        .await
        .expect("a scalar-boundary sibling insertion must fall back to full reconciliation");

    assert_eq!(read_file(&lix, path).await.unwrap(), Some(successor));
    let members = lix
        .execute(
            "SELECT key, scalar_json FROM json_object_member ORDER BY key",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(members.len(), 2);
    assert_eq!(members.rows()[0].get::<String>("key").unwrap(), "a");
    assert_eq!(members.rows()[1].get::<String>("key").unwrap(), "b");
    assert_eq!(
        members.rows()[1].get::<Value>("scalar_json").unwrap(),
        Value::Jsonb(serde_json::json!(2).into())
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_json_roundtrips_recursive_state_and_keeps_leaf_edits_sparse() {
    let archive = build_json_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/component-v2.json";
    let source = br#"{
  "profile": {"name": "Ada", "active": true},
  "items": [{"label": "one"}, {"label": "two"}]
}
"#
    .to_vec();
    write_file(&lix, path, source.clone()).await.unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(source.clone()));

    let members = lix
        .execute(
            "SELECT key, kind, scalar_json FROM json_object_member \
             WHERE key = 'name'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members.rows()[0].get::<String>("kind").unwrap(), "string");
    assert_eq!(
        members.rows()[0].get::<Value>("scalar_json").unwrap(),
        Value::Jsonb(serde_json::json!("Ada").into())
    );

    let edited = String::from_utf8(source)
        .unwrap()
        .replacen(r#""Ada""#, r#""Lin""#, 1)
        .into_bytes();

    write_file(&lix, path, edited.clone()).await.unwrap();

    assert_eq!(read_file(&lix, path).await.unwrap(), Some(edited.clone()));

    lix.execute(
        "UPDATE json_object_member SET scalar_json = $1 WHERE key = 'name'",
        &[Value::Jsonb(serde_json::json!("Grace").into())],
    )
    .await
    .unwrap();
    let rendered = String::from_utf8(read_file(&lix, path).await.unwrap().unwrap()).unwrap();
    assert_eq!(
        rendered,
        String::from_utf8(edited)
            .unwrap()
            .replacen(r#""Lin""#, r#""Grace""#, 1)
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_json_scalar_lww_composes_and_stale_structure_does_not_resurrect_nodes() {
    let archive = build_json_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/json-lifecycle.json";
    let initial = b"{\"left\":\"one\",\"right\":\"two\",\"gone\":\"three\"}".to_vec();
    write_file(&lix, path, initial.clone()).await.unwrap();
    let file_id = file_id_at_path(&lix, path).await;

    // Different scalar changes from the same observed document compose.
    let left_writer = lix.open_another_session().await.unwrap();
    let right_writer = lix.open_another_session().await.unwrap();
    assert_eq!(
        read_file(&left_writer, path).await.unwrap(),
        Some(initial.clone())
    );
    assert_eq!(read_file(&right_writer, path).await.unwrap(), Some(initial));
    left_writer
        .execute(
            "UPDATE json_object_member SET scalar_json = $1 \
             WHERE parent_id = 'root' AND key = 'left' AND lixcol_file_id = $2",
            &[
                Value::Jsonb(serde_json::json!("ONE-A").into()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .unwrap();
    right_writer
        .execute(
            "UPDATE json_object_member SET scalar_json = $1 \
             WHERE parent_id = 'root' AND key = 'right' AND lixcol_file_id = $2",
            &[
                Value::Jsonb(serde_json::json!("TWO-B").into()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .unwrap();
    let composed = b"{\"left\":\"ONE-A\",\"right\":\"TWO-B\",\"gone\":\"three\"}".to_vec();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(composed.clone()));

    // Commit order is the deterministic LWW tiebreaker for the same scalar.
    let first_lww = lix.open_another_session().await.unwrap();
    let second_lww = lix.open_another_session().await.unwrap();
    assert_eq!(
        read_file(&first_lww, path).await.unwrap(),
        Some(composed.clone())
    );
    assert_eq!(read_file(&second_lww, path).await.unwrap(), Some(composed));
    first_lww
        .execute(
            "UPDATE json_object_member SET scalar_json = $1 \
             WHERE parent_id = 'root' AND key = 'left' AND lixcol_file_id = $2",
            &[
                Value::Jsonb(serde_json::json!("LWW-A").into()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .unwrap();
    second_lww
        .execute(
            "UPDATE json_object_member SET scalar_json = $1 \
             WHERE parent_id = 'root' AND key = 'left' AND lixcol_file_id = $2",
            &[
                Value::Jsonb(serde_json::json!("LWW-B").into()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .unwrap();
    let lww = b"{\"left\":\"LWW-B\",\"right\":\"TWO-B\",\"gone\":\"three\"}".to_vec();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(lww.clone()));

    // Structure is not a direct semantic SQL operation. Its rejection must
    // roll back the staged row and leave the actor usable for a later scalar.
    let direct_structure_error = lix
        .execute(
            "DELETE FROM json_object_member \
             WHERE parent_id = 'root' AND key = 'gone' AND lixcol_file_id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .expect_err("direct JSON semantic deletion must use an authoritative byte write");
    assert_eq!(direct_structure_error.code, LixError::CODE_INVALID_PLUGIN);
    assert!(
        direct_structure_error
            .message
            .contains("existing scalar values only")
    );
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(lww));
    lix.execute(
        "UPDATE json_object_member SET scalar_json = $1 \
         WHERE parent_id = 'root' AND key = 'right' AND lixcol_file_id = $2",
        &[
            Value::Jsonb(serde_json::json!("AFTER-DIRECT-REJECT").into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .unwrap();
    let scalar_after_direct_reject =
        b"{\"left\":\"LWW-B\",\"right\":\"AFTER-DIRECT-REJECT\",\"gone\":\"three\"}".to_vec();
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(scalar_after_direct_reject.clone())
    );
    lix.execute(
        "UPDATE json_object_member SET scalar_json = $1 \
             WHERE parent_id = 'root' AND lixcol_file_id = $2",
        &[
            Value::Jsonb(serde_json::json!("BULK").into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("a direct JSON semantic transition accepts an existing-scalar batch");
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(b"{\"left\":\"BULK\",\"right\":\"BULK\",\"gone\":\"BULK\"}".to_vec())
    );
    write_file(&lix, path, scalar_after_direct_reject.clone())
        .await
        .unwrap();

    // Structure is byte-owned. A stale scalar delta is not allowed to
    // recreate a row after another writer removes its containing slot.
    let stale_writer = lix.open_another_session().await.unwrap();
    let structure_writer = lix.open_another_session().await.unwrap();
    assert_eq!(
        read_file(&stale_writer, path).await.unwrap(),
        Some(scalar_after_direct_reject.clone())
    );
    assert_eq!(
        read_file(&structure_writer, path).await.unwrap(),
        Some(scalar_after_direct_reject)
    );
    let without_gone = b"{\"left\":\"LWW-B\",\"right\":\"AFTER-DIRECT-REJECT\"}".to_vec();
    write_file(&structure_writer, path, without_gone.clone())
        .await
        .unwrap();
    let error = write_file(
        &stale_writer,
        path,
        b"{\"left\":\"LWW-B\",\"right\":\"AFTER-DIRECT-REJECT\",\"gone\":\"STALE\"}".to_vec(),
    )
    .await
    .expect_err("a stale scalar must not resurrect a byte-deleted JSON node");
    assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
    assert!(error.message.contains("existing scalar values only"));
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(without_gone.clone())
    );
    let gone = lix
        .execute(
            "SELECT key FROM json_object_member \
             WHERE parent_id = 'root' AND key = 'gone' AND lixcol_file_id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert!(gone.is_empty(), "the deleted node must not be resurrected");

    // A clean semantic scalar write still works after the rejected replay;
    // returned invalid-input errors discard only the prospective transition.
    lix.execute(
        "UPDATE json_object_member SET scalar_json = $1 \
         WHERE parent_id = 'root' AND key = 'left' AND lixcol_file_id = $2",
        &[
            Value::Jsonb(serde_json::json!("AFTER-FENCE").into()),
            Value::Text(file_id),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(b"{\"left\":\"AFTER-FENCE\",\"right\":\"AFTER-DIRECT-REJECT\"}".to_vec())
    );

    for session in [
        left_writer,
        right_writer,
        first_lww,
        second_lww,
        stale_writer,
        structure_writer,
    ] {
        session.close().await.unwrap();
    }
    lix.close().await.unwrap();
}

#[tokio::test]
#[ignore = "10 MiB JSON unrelated-row merge benchmark"]
async fn v2_json_ten_mib_unrelated_row_merge_benchmark() {
    init_perf_tracing();
    const SAMPLES: usize = 7;
    const BENCHMARK: &str = "v2_json_ten_mib_unrelated_row_merge_benchmark";

    let root = tempfile::tempdir().expect("create JSON merge benchmark directory");
    let archive = build_json_plugin_archive();
    let lix = open_rocksdb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/merge-ten-mib.json";
    let (bytes, _, _) = json_ten_mib_flat_fixture();
    write_file(&lix, path, bytes)
        .await
        .expect("real JSON v2 Wasm should import the 10 MiB fixture");
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let mut preview_elapsed_ms = Vec::with_capacity(SAMPLES);
    let mut preview_measurements = Vec::with_capacity(SAMPLES);
    let mut elapsed_ms = Vec::with_capacity(SAMPLES);
    let mut measurements = Vec::with_capacity(SAMPLES);
    let fixture = BenchmarkFixture {
        input_bytes: JSON_TEN_MIB_BYTES,
        logical_rows: JSON_TEN_MIB_PROPERTY_COUNT + 1,
    };

    for sample in 0..SAMPLES {
        let source = lix
            .create_branch(CreateBranchOptions {
                id: Some(format!("01900000-0000-7000-8100-{sample:012x}")),
                name: format!("JSON merge source {sample}"),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let target_key = format!("batch-merge-target-{sample}");
        let source_key = format!("batch-merge-source-{sample}");
        let target_value = format!("target-{sample}");
        let source_value = format!("source-{sample}");

        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
            &[
                Value::Text(target_key.clone()),
                Value::Text(target_value.clone()),
            ],
        )
        .await
        .expect("target merge control row should insert");
        lix.switch_branch(SwitchBranchOptions {
            branch_id: source.id.clone(),
        })
        .await
        .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
            &[
                Value::Text(source_key.clone()),
                Value::Text(source_value.clone()),
            ],
        )
        .await
        .expect("source merge control row should insert");
        lix.switch_branch(SwitchBranchOptions {
            branch_id: target_branch_id.clone(),
        })
        .await
        .unwrap();

        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        let preview = lix
            .merge_branch_preview(MergeBranchPreviewOptions {
                source_branch_id: source.id.clone(),
            })
            .await
            .expect("unrelated JSON properties should produce a merge preview");
        let preview_measurement =
            BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
        assert!(
            preview.conflicts.is_empty(),
            "unrelated JSON properties must remain conflict-free"
        );
        preview_elapsed_ms.push(preview_measurement.elapsed_ms);
        preview_measurements.push(preview_measurement);
        emit_sample(
            BENCHMARK,
            "tracked_diff_preview",
            sample,
            fixture,
            BenchmarkGate::ElapsedRegression,
            preview_measurement,
        );

        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        lix.merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .expect("unrelated JSON properties should merge cleanly");
        let measurement = BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
        elapsed_ms.push(measurement.elapsed_ms);
        measurements.push(measurement);
        emit_sample(
            BENCHMARK,
            "unrelated_row",
            sample,
            fixture,
            BenchmarkGate::ElapsedRegression,
            measurement,
        );

        let merged = lix
            .execute(
                "SELECT key, value FROM lix_key_value WHERE key IN ($1, $2)",
                &[
                    Value::Text(target_key.clone()),
                    Value::Text(source_key.clone()),
                ],
            )
            .await
            .expect("merged control rows should query");
        let values = merged
            .rows()
            .iter()
            .map(|row| {
                (
                    row.get::<String>("key").unwrap(),
                    row.get::<serde_json::Value>("value").unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            values.get(&target_key),
            Some(&serde_json::json!(target_value))
        );
        assert_eq!(
            values.get(&source_key),
            Some(&serde_json::json!(source_value))
        );
    }

    preview_elapsed_ms.sort_by(f64::total_cmp);
    elapsed_ms.sort_by(f64::total_cmp);
    let merge_p50_ms = elapsed_ms[elapsed_ms.len() / 2];
    let p95_index = ((elapsed_ms.len() * 95).div_ceil(100)).saturating_sub(1);
    let merge_p95_ms = elapsed_ms[p95_index];
    eprintln!(
        "v2_json_ten_mib_unrelated_row_merge bytes={JSON_TEN_MIB_BYTES} samples={SAMPLES} \
         preview_p50_ms={:.3} preview_p95_ms={:.3} merge_p50_ms={merge_p50_ms:.3} merge_p95_ms={merge_p95_ms:.3}",
        p50_ms(&preview_elapsed_ms),
        p95_ms(&preview_elapsed_ms),
    );
    emit_summary(
        BENCHMARK,
        "tracked_diff_preview",
        fixture,
        BenchmarkGate::ElapsedRegression,
        &preview_measurements,
    );
    emit_summary(
        BENCHMARK,
        "unrelated_row",
        fixture,
        BenchmarkGate::ElapsedRegression,
        &measurements,
    );

    lix.close().await.expect("JSON benchmark should close");
}

/// End-to-end RocksDB gate for a same-row conflict over the same large
/// tracked tree as the adjacent unrelated-row benchmark. The tiny built-in
/// control row keeps the frozen reference runnable even when its JSON plugin
/// merge path cannot fingerprint typed plugin rows.
#[tokio::test]
#[ignore = "10 MiB JSON same-row conflict-resolution merge benchmark"]
async fn v2_json_ten_mib_same_row_canonical_b_merge_benchmark() {
    init_perf_tracing();
    const SAMPLES: usize = 7;
    const BENCHMARK: &str = "v2_json_ten_mib_same_row_canonical_b_merge_benchmark";

    let root = tempfile::tempdir().expect("create JSON conflict benchmark directory");
    let archive = build_json_plugin_archive();
    let lix = open_rocksdb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/merge-conflict-ten-mib.json";
    let (bytes, _, _) = json_ten_mib_flat_fixture();
    write_file(&lix, path, bytes)
        .await
        .expect("real JSON v2 Wasm should import the 10 MiB fixture");
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let mut elapsed_ms = Vec::with_capacity(SAMPLES);
    let mut measurements = Vec::with_capacity(SAMPLES);
    let fixture = BenchmarkFixture {
        input_bytes: JSON_TEN_MIB_BYTES,
        logical_rows: JSON_TEN_MIB_PROPERTY_COUNT + 1,
    };

    for sample in 0..SAMPLES {
        let source = lix
            .create_branch(CreateBranchOptions {
                id: Some(format!("01900000-0000-7000-8200-{sample:012x}")),
                name: format!("JSON conflict merge source {sample}"),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let key = format!("batch-merge-conflict-{sample}");
        let target_value = format!("target-{sample}");
        let source_value = format!("source-{sample}");
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
            &[Value::Text(key.clone()), Value::Text(target_value.clone())],
        )
        .await
        .expect("target conflict control row should insert");
        lix.switch_branch(SwitchBranchOptions {
            branch_id: source.id.clone(),
        })
        .await
        .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
            &[Value::Text(key.clone()), Value::Text(source_value)],
        )
        .await
        .expect("source conflict control row should insert");
        lix.switch_branch(SwitchBranchOptions {
            branch_id: target_branch_id.clone(),
        })
        .await
        .unwrap();

        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        let error = lix
            .merge_branch(MergeBranchOptions {
                source_branch_id: source.id,
            })
            .await
            .expect_err("same control-row identity should remain a merge conflict");
        let measurement = BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
        assert_eq!(error.code, LixError::CODE_MERGE_CONFLICT);
        elapsed_ms.push(measurement.elapsed_ms);
        measurements.push(measurement);
        emit_sample(
            BENCHMARK,
            "same_row_conflict",
            sample,
            fixture,
            BenchmarkGate::ElapsedRegression,
            measurement,
        );
        let target = lix
            .execute(
                "SELECT value FROM lix_key_value WHERE key = $1",
                &[Value::Text(key)],
            )
            .await
            .expect("target control row should remain queryable after conflict");
        assert_eq!(target.len(), 1);
        assert_eq!(
            target.rows()[0].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!(target_value)
        );
    }

    let raw_ms = elapsed_ms.clone();
    elapsed_ms.sort_by(f64::total_cmp);
    eprintln!(
        "v2_json_ten_mib_same_row_canonical_b_merge bytes={JSON_TEN_MIB_BYTES} samples={SAMPLES} \
         raw_ms={raw_ms:?} p50_ms={:.3} p95_ms={:.3}",
        p50_ms(&elapsed_ms),
        p95_ms(&elapsed_ms),
    );
    emit_summary(
        BENCHMARK,
        "same_row_conflict",
        fixture,
        BenchmarkGate::ElapsedRegression,
        &measurements,
    );

    lix.close()
        .await
        .expect("JSON conflict benchmark should close");
}

#[tokio::test]
async fn v3_json_reopen_uses_one_export_for_cold_successor() {
    const PATH: &str = "/v3-json-cold-successor-regression.json";
    let before = br#"{"keep":1,"edit":2}"#.to_vec();
    let after = br#"{"keep":1,"edit":3}"#.to_vec();
    let root = tempfile::tempdir().expect("create cold JSON regression directory");
    let storage =
        RocksDB::open(root.path().join(".lix")).expect("open cold JSON regression RocksDB");
    let lix = open_lix().with_storage(storage.clone()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &build_json_plugin_archive(),
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;
    write_file(&lix, PATH, before).await.unwrap();
    storage.flush().expect("flush cold JSON regression import");
    lix.close().await.unwrap();

    let reopened = open_rocksdb_lix(root.path()).await;

    write_file(&reopened, PATH, after.clone()).await.unwrap();

    assert_eq!(read_file(&reopened, PATH).await.unwrap(), Some(after));
    let rows = reopened
        .execute(
            "SELECT scalar_json FROM json_object_member WHERE key = 'edit'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        rows.rows()[0].get::<Value>("scalar_json").unwrap(),
        Value::Jsonb(serde_json::json!(3).into())
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn universal_row_page_streams_oversized_jsonb_value() {
    const PATH: &str = "/universal-oversized-output.json";
    let value = "x".repeat(3 * 1024 * 1024);
    let bytes = serde_json::to_vec(&serde_json::json!({ "large": value })).unwrap();
    let root = tempfile::tempdir().expect("create oversized output directory");
    let lix = open_rocksdb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &build_json_plugin_archive(),
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    write_file(&lix, PATH, bytes.clone()).await.unwrap();
    assert_eq!(read_file(&lix, PATH).await.unwrap(), Some(bytes.clone()));
    let rows = lix
        .execute(
            "SELECT scalar_json FROM json_object_member WHERE key = 'large'",
            &[],
        )
        .await
        .unwrap();
    let Value::Jsonb(scalar) = rows.rows()[0].get::<Value>("scalar_json").unwrap() else {
        panic!("oversized scalar_json must project as native JSONB");
    };
    assert_eq!(scalar.to_value().as_str().unwrap().len(), 3 * 1024 * 1024);
    lix.close().await.unwrap();

    let reopened = open_rocksdb_lix(root.path()).await;
    assert_eq!(read_file(&reopened, PATH).await.unwrap(), Some(bytes));
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn v3_json_certified_batch_survives_sparse_successor_and_time_travel() {
    let root = tempfile::tempdir().expect("create v3 JSON successor directory");
    let lix = open_rocksdb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &build_json_plugin_archive(),
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;
    let path = "/v3-json-successor.json";
    let before = br#"{"a":"one","b":"two"}"#.to_vec();
    write_file(&lix, path, before.clone()).await.unwrap();
    assert_eq!(
        lix.execute("SELECT COUNT(*) AS count FROM json_object_member", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<i64>("count")
            .unwrap(),
        2
    );

    let after = br#"{"a":"ONE","b":"two"}"#.to_vec();
    write_file(&lix, path, after.clone()).await.unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(after.clone()));
    let current = lix
        .execute(
            "SELECT key, scalar_json FROM json_object_member ORDER BY key",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(current.rows().len(), 2);
    assert_eq!(current.rows()[0].get::<String>("key").unwrap(), "a");
    assert_eq!(
        current.rows()[0].get::<Value>("scalar_json").unwrap(),
        Value::Jsonb(serde_json::json!("ONE").into())
    );
    assert_eq!(current.rows()[1].get::<String>("key").unwrap(), "b");

    let historical = lix
        .execute(
            "SELECT key, scalar_json FROM lix_history('json_object_member') \
             WHERE lixcol_depth = 1 ORDER BY key",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(historical.rows().len(), 2);
    assert_eq!(
        historical.rows()[0].get::<Value>("scalar_json").unwrap(),
        Value::Jsonb(serde_json::json!("one").into())
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn v3_json_cold_hydration_after_actor_eviction_preserves_sparse_successor() {
    let root = tempfile::tempdir().expect("create v3 JSON eviction directory");
    let lix = open_rocksdb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &build_json_plugin_archive(),
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;
    let path = "/v3-json-evicted.json";
    let before = br#"{"a":"one","b":"two"}"#.to_vec();
    write_file(&lix, path, before).await.unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap().unwrap().len(), 21);

    for index in 0..20 {
        write_file(
            &lix,
            &format!("/v3-json-eviction-{index}.json"),
            format!(r#"{{"value":"{index:04}"}}"#).into_bytes(),
        )
        .await
        .unwrap();
    }

    let after = br#"{"a":"ONE","b":"two"}"#.to_vec();
    write_file(&lix, path, after.clone()).await.unwrap();

    assert_eq!(read_file(&lix, path).await.unwrap(), Some(after.clone()));

    assert_eq!(
        lix.execute(
            "SELECT scalar_json FROM json_object_member WHERE key = 'a'",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
            .get::<Value>("scalar_json")
            .unwrap(),
        Value::Jsonb(serde_json::json!("ONE").into())
    );
    lix.close().await.unwrap();

    let reopened = open_rocksdb_lix(root.path()).await;
    assert_eq!(read_file(&reopened, path).await.unwrap(), Some(after));
    let after_reopen = br#"{"a":"ONE","b":"TWO"}"#.to_vec();

    write_file(&reopened, path, after_reopen.clone())
        .await
        .unwrap();
    assert_eq!(
        read_file(&reopened, path).await.unwrap(),
        Some(after_reopen)
    );
    assert_eq!(
        reopened
            .execute(
                "SELECT scalar_json FROM json_object_member WHERE key = 'b'",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<Value>("scalar_json")
            .unwrap(),
        Value::Jsonb(serde_json::json!("TWO").into())
    );

    reopened.close().await.unwrap();
}

#[tokio::test]
async fn v3_csv_cold_successor_after_eviction_and_reopen_preserves_identity() {
    let root = tempfile::tempdir().expect("create v3 CSV eviction directory");
    let lix = open_rocksdb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &build_csv_plugin_archive(),
        &["csv_table", "csv_row"],
    )
    .await;
    let path = "/v3-csv-evicted.csv";
    write_file(&lix, path, b"alpha,one\nbeta,two\n".to_vec())
        .await
        .unwrap();
    let first_id = lix
        .execute("SELECT id FROM csv_row ORDER BY order_key LIMIT 1", &[])
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();

    for index in 0..20 {
        write_file(
            &lix,
            &format!("/v3-csv-eviction-{index}.csv"),
            format!("row,{index:04}\n").into_bytes(),
        )
        .await
        .unwrap();
    }

    let after_eviction = b"alpha,ONE\nbeta,two\n".to_vec();
    write_file(&lix, path, after_eviction).await.unwrap();

    assert_eq!(
        lix.execute("SELECT id FROM csv_row ORDER BY order_key LIMIT 1", &[],)
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap(),
        first_id
    );
    lix.close().await.unwrap();

    let reopened = open_rocksdb_lix(root.path()).await;

    let after_reopen = b"alpha,One\nbeta,two\n".to_vec();
    write_file(&reopened, path, after_reopen.clone())
        .await
        .unwrap();

    assert_eq!(
        read_file(&reopened, path).await.unwrap(),
        Some(after_reopen)
    );
    assert_eq!(
        reopened
            .execute("SELECT id FROM csv_row ORDER BY order_key LIMIT 1", &[],)
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap(),
        first_id
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn v3_csv_cold_hydration_preserves_multiple_create_namespaces() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &build_csv_plugin_archive(),
        &["csv_table", "csv_row"],
    )
    .await;
    let path = "/multi-namespace.csv";
    write_file(&lix, path, b"old,one\nlast,two\n".to_vec())
        .await
        .unwrap();
    let file_id = file_id_at_path(&lix, path).await;
    let original_id = csv_row_id(&active_csv_rows(&lix, &file_id).await, &["old", "one"]);
    write_file(&lix, path, b"new,zero\nold,one\nlast,two\n".to_vec())
        .await
        .unwrap();
    let inserted_id = csv_row_id(&active_csv_rows(&lix, &file_id).await, &["new", "zero"]);
    assert_ne!(inserted_id, original_id);

    for index in 0..20 {
        write_file(
            &lix,
            &format!("/multi-namespace-evict-{index}.csv"),
            format!("row,{index}\n").into_bytes(),
        )
        .await
        .unwrap();
    }
    write_file(&lix, path, b"new,ZERO\nold,one\nlast,two\n".to_vec())
        .await
        .expect("cold hydration must retain IDs from every create namespace");
    let rows = active_csv_rows(&lix, &file_id).await;
    assert_eq!(csv_row_id(&rows, &["new", "ZERO"]), inserted_id);
    assert_eq!(csv_row_id(&rows, &["old", "one"]), original_id);

    lix.close().await.unwrap();
}

#[tokio::test]
#[ignore = "10 MiB JSON public-SQL read benchmark on RocksDB"]
async fn v2_json_ten_mib_rocksdb_read_benchmark() {
    const WARM_SAMPLES: usize = 20;
    const COLD_SAMPLES: usize = 7;
    const BENCHMARK: &str = "v2_json_ten_mib_rocksdb_read_benchmark";

    let root = tempfile::tempdir().expect("create JSON read benchmark directory");
    let archive = build_json_plugin_archive();
    let lix = open_rocksdb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/read-ten-mib.json";
    let (bytes, _, _) = json_ten_mib_flat_fixture();
    write_file(&lix, path, bytes.clone())
        .await
        .expect("real JSON v2 Wasm should import the 10 MiB fixture");

    for _ in 0..3 {
        let read = read_file(&lix, path)
            .await
            .expect("warm materialized JSON should read")
            .expect("warm materialized JSON should exist");
        assert_eq!(read.len(), JSON_TEN_MIB_BYTES);
        black_box(read);
    }

    let mut warm_ms = Vec::with_capacity(WARM_SAMPLES);
    let mut warm_measurements = Vec::with_capacity(WARM_SAMPLES);
    let fixture = BenchmarkFixture {
        input_bytes: JSON_TEN_MIB_BYTES,
        logical_rows: JSON_TEN_MIB_PROPERTY_COUNT + 1,
    };
    for sample in 0..WARM_SAMPLES {
        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        let read = read_file(&lix, path)
            .await
            .expect("warm materialized JSON should read")
            .expect("warm materialized JSON should exist");
        let measurement = BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
        warm_ms.push(measurement.elapsed_ms);
        warm_measurements.push(measurement);
        emit_sample(
            BENCHMARK,
            "warm_read",
            sample,
            fixture,
            BenchmarkGate::ElapsedRegression,
            measurement,
        );
        assert_eq!(read.len(), JSON_TEN_MIB_BYTES);
        black_box(read);
    }
    lix.close().await.expect("warm JSON benchmark should close");

    let mut cold_total_ms = Vec::with_capacity(COLD_SAMPLES);
    let mut cold_storage_open_ms = Vec::with_capacity(COLD_SAMPLES);
    let mut cold_engine_open_ms = Vec::with_capacity(COLD_SAMPLES);
    let mut cold_read_ms = Vec::with_capacity(COLD_SAMPLES);
    let mut cold_measurements = Vec::with_capacity(COLD_SAMPLES);
    for sample in 0..COLD_SAMPLES {
        let allocation_scope = AllocationScope::start();
        let total_started = Instant::now();
        let storage_started = Instant::now();
        let storage =
            RocksDB::open(root.path().join(".lix")).expect("reopen JSON benchmark RocksDB");
        cold_storage_open_ms.push(storage_started.elapsed().as_secs_f64() * 1_000.0);

        let engine_started = Instant::now();
        let reopened = open_lix()
            .with_storage(storage)
            .await
            .expect("reopen JSON benchmark workspace");
        cold_engine_open_ms.push(engine_started.elapsed().as_secs_f64() * 1_000.0);

        let read_started = Instant::now();
        let read = read_file(&reopened, path)
            .await
            .expect("cold materialized JSON should read")
            .expect("cold materialized JSON should exist");
        cold_read_ms.push(read_started.elapsed().as_secs_f64() * 1_000.0);
        assert_eq!(read, bytes);
        black_box(&read);
        reopened
            .close()
            .await
            .expect("cold JSON benchmark should close");
        let measurement =
            BenchmarkMeasurement::new(total_started.elapsed(), allocation_scope.finish());
        cold_total_ms.push(measurement.elapsed_ms);
        cold_measurements.push(measurement);
        emit_sample(
            BENCHMARK,
            "cold_open_read",
            sample,
            fixture,
            BenchmarkGate::ElapsedRegression,
            measurement,
        );
    }

    for samples in [
        &mut warm_ms,
        &mut cold_total_ms,
        &mut cold_storage_open_ms,
        &mut cold_engine_open_ms,
        &mut cold_read_ms,
    ] {
        samples.sort_by(f64::total_cmp);
    }
    eprintln!(
        "v2_json_ten_mib_rocksdb_read bytes={JSON_TEN_MIB_BYTES} \
         warm_samples={WARM_SAMPLES} warm_p50_ms={:.3} warm_p95_ms={:.3} \
         cold_samples={COLD_SAMPLES} cold_total_p50_ms={:.3} \
         cold_storage_open_p50_ms={:.3} cold_engine_open_p50_ms={:.3} \
         cold_read_p50_ms={:.3}",
        p50_ms(&warm_ms),
        p95_ms(&warm_ms),
        p50_ms(&cold_total_ms),
        p50_ms(&cold_storage_open_ms),
        p50_ms(&cold_engine_open_ms),
        p50_ms(&cold_read_ms),
    );
    emit_summary(
        BENCHMARK,
        "warm_read",
        fixture,
        BenchmarkGate::ElapsedRegression,
        &warm_measurements,
    );
    emit_summary(
        BENCHMARK,
        "cold_open_read",
        fixture,
        BenchmarkGate::ElapsedRegression,
        &cold_measurements,
    );
}

#[derive(Debug)]
struct ColdMaterializedOpenSample {
    measurement: BenchmarkMeasurement,
    counters: WasmTransitionCounters,
}

fn report_cold_materialized_open(
    label: &str,
    expected_bytes: usize,
    samples: &[ColdMaterializedOpenSample],
) {
    let mut elapsed_ms = samples
        .iter()
        .map(|sample| sample.measurement.elapsed_ms)
        .collect::<Vec<_>>();
    elapsed_ms.sort_by(f64::total_cmp);
    let p50_ms = elapsed_ms[elapsed_ms.len() / 2];
    let p95_index = ((elapsed_ms.len() * 95).div_ceil(100)).saturating_sub(1);
    let p95_ms = elapsed_ms[p95_index];

    for sample in samples {
        let counters = sample.counters;
        assert_eq!(
            counters.guest_export_calls, 1,
            "{label} cold successor must not hydrate and re-enter the guest"
        );
        assert_eq!(
            counters.full_state_semantic_rows_materialized, 0,
            "{label} typed cold-open path must avoid semantic-row hydration"
        );
        assert_eq!(counters.private_document_cache_hits, 1);
        assert_eq!(counters.full_document_reparses, 0);
        assert!(
            counters.component_boundary_bytes > 0,
            "{label} cold successor must account for its bounded row pages"
        );
    }

    let representative = samples[elapsed_ms.len() / 2].counters;
    let mut peak_live = samples
        .iter()
        .map(|sample| sample.measurement.allocations.peak_live_bytes_delta)
        .collect::<Vec<_>>();
    peak_live.sort_unstable();
    let mut allocated = samples
        .iter()
        .map(|sample| sample.measurement.allocations.allocated_bytes)
        .collect::<Vec<_>>();
    allocated.sort_unstable();
    eprintln!(
        "v3_cold_successor label={label} bytes={expected_bytes} samples={} \
         p50_ms={p50_ms:.3} p95_ms={p95_ms:.3} source_read_calls={} source_bytes_read={} \
         packet_pages={} packet_records={} attachment_reads={} attachment_bytes_read={} \
         boundary_bytes={} guest_high_water_bytes={} full_renderer_invocations={} \
         host_peak_live_mb={:.3} host_allocated_mb={:.3}",
        samples.len(),
        representative.source_read_calls,
        representative.source_bytes_read,
        representative.packet_pages,
        representative.packet_records,
        representative.attachment_reads,
        representative.attachment_bytes_read,
        representative.component_boundary_bytes,
        representative.guest_linear_memory_high_water_bytes,
        representative.full_renderer_invocations,
        peak_live[peak_live.len() / 2] as f64 / 1_000_000.0,
        allocated[allocated.len() / 2] as f64 / 1_000_000.0,
    );
}

#[tokio::test]
async fn v2_json_cold_row_write_is_scoped_by_file_despite_shared_root_keys() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive = build_json_plugin_archive();
    let lix = open_filesystem_lix(tempdir.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let first_path = "/first.json";
    let second_path = "/second.json";
    let first = b"{\"value\":\"first\"}\n".to_vec();
    let second = b"{\"value\":\"second\"}\n".to_vec();
    write_file(&lix, first_path, first).await.unwrap();
    write_file(&lix, second_path, second.clone()).await.unwrap();
    let first_id = file_id_at_path(&lix, first_path).await;
    let second_id = file_id_at_path(&lix, second_path).await;
    lix.close().await.unwrap();

    // No exact file read warms an actor after reopen. Both files use the same
    // plugin schemas and the same recursive root/member identities; file_id is
    // therefore the required ownership boundary.
    let lix = open_filesystem_lix(tempdir.path()).await;

    lix.execute(
        "UPDATE json_object_member SET scalar_json = $1 \
         WHERE parent_id = 'root' AND key = 'value' AND lixcol_file_id = $2",
        &[
            Value::Jsonb(serde_json::json!("FIRST").into()),
            Value::Text(first_id.clone()),
        ],
    )
    .await
    .unwrap();

    assert_eq!(
        read_file(&lix, first_path).await.unwrap(),
        Some(b"{\"value\":\"FIRST\"}\n".to_vec())
    );
    assert_eq!(read_file(&lix, second_path).await.unwrap(), Some(second));
    let untouched = lix
        .execute(
            "SELECT scalar_json FROM json_object_member \
             WHERE parent_id = 'root' AND key = 'value' AND lixcol_file_id = $1",
            &[Value::Text(second_id)],
        )
        .await
        .unwrap();
    assert_eq!(
        untouched.rows()[0].get::<Value>("scalar_json").unwrap(),
        Value::Jsonb(serde_json::json!("second").into())
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_json_row_write_rollback_keeps_original_bytes_and_actor() {
    let archive = build_json_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;
    let path = "/row-rollback.json";
    let original = b"{\"value\":\"before\"}\n".to_vec();
    let committed = b"{\"value\":\"after\"}\n".to_vec();
    write_file(&lix, path, original.clone()).await.unwrap();
    let file_id = file_id_at_path(&lix, path).await;

    let mut transaction = lix.begin_transaction().await.unwrap();
    transaction
        .execute(
            "UPDATE json_object_member SET scalar_json = $1 \
             WHERE parent_id = 'root' AND key = 'value' AND lixcol_file_id = $2",
            &[
                Value::Jsonb(serde_json::json!("rolled-back").into()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .unwrap();
    let staged = transaction
        .execute(
            "SELECT content FROM lix_file WHERE id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(
        staged.rows()[0].get::<Vec<u8>>("content").unwrap(),
        b"{\"value\":\"rolled-back\"}\n"
    );
    transaction.rollback().await.unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(original));

    // Discarding the pending successor must leave the accepted actor reusable.
    lix.execute(
        "UPDATE json_object_member SET scalar_json = $1 \
         WHERE parent_id = 'root' AND key = 'value' AND lixcol_file_id = $2",
        &[
            Value::Jsonb(serde_json::json!("after").into()),
            Value::Text(file_id),
        ],
    )
    .await
    .unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(committed));
    lix.close().await.unwrap();
}

#[tokio::test]
async fn same_base_json_transactions_resolve_overlap_and_converge() {
    let archive = build_json_plugin_archive();
    let first = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &first,
        "plugin_json",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;
    let path = "/transaction-conflict.json";
    write_file(&first, path, b"{\"value\":\"base\"}\n".to_vec())
        .await
        .unwrap();
    let file_id = file_id_at_path(&first, path).await;
    let second = first.open_another_session().await.unwrap();
    let mut first_transaction = first.begin_transaction().await.unwrap();
    let mut second_transaction = second.begin_transaction().await.unwrap();
    for (transaction, value) in [
        (&mut first_transaction, serde_json::json!("first")),
        (&mut second_transaction, serde_json::json!("second")),
    ] {
        transaction
            .execute(
                "UPDATE json_object_member SET scalar_json = $1 \
                 WHERE parent_id = 'root' AND key = 'value' AND lixcol_file_id = $2",
                &[Value::Jsonb(value.into()), Value::Text(file_id.clone())],
            )
            .await
            .unwrap();
    }

    first_transaction.commit().await.unwrap();
    second_transaction
        .commit()
        .await
        .expect("stale plugin overlap should resolve at commit");

    let first_bytes = read_file(&first, path).await.unwrap().unwrap();
    let second_bytes = read_file(&second, path).await.unwrap().unwrap();
    assert_eq!(first_bytes, second_bytes);
    assert_ne!(first_bytes, b"{\"value\":\"base\"}\n");
    second.close().await.unwrap();
    first.close().await.unwrap();
}

#[tokio::test]
async fn same_base_json_file_edits_compose_disjoint_semantics_without_resolution() {
    let archive = build_json_plugin_archive();
    let first = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &first,
        "plugin_json",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;
    let path = "/disjoint-file-transactions.json";
    write_file(&first, path, b"{\"a\":\"base\",\"b\":\"base\"}\n".to_vec())
        .await
        .unwrap();
    let second = first.open_another_session().await.unwrap();
    let mut first_transaction = first.begin_transaction().await.unwrap();
    let mut second_transaction = second.begin_transaction().await.unwrap();
    first_transaction
        .execute(
            "UPDATE lix_file SET content = $1 WHERE path = $2",
            &[
                Value::Blob(b"{\"a\":\"first\",\"b\":\"base\"}\n".to_vec().into()),
                Value::Text(path.to_owned()),
            ],
        )
        .await
        .unwrap();
    second_transaction
        .execute(
            "UPDATE lix_file SET content = $1 WHERE path = $2",
            &[
                Value::Blob(b"{\"a\":\"base\",\"b\":\"second\"}\n".to_vec().into()),
                Value::Text(path.to_owned()),
            ],
        )
        .await
        .unwrap();

    first_transaction.commit().await.unwrap();
    second_transaction.commit().await.unwrap();

    let bytes = read_file(&first, path).await.unwrap().unwrap();
    let document: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(document, serde_json::json!({ "a": "first", "b": "second" }));
    assert_eq!(read_file(&second, path).await.unwrap(), Some(bytes));
    second.close().await.unwrap();
    first.close().await.unwrap();
}

#[tokio::test]
async fn stale_json_transaction_renders_retained_same_file_edits_with_resolutions() {
    let archive = build_json_plugin_archive();
    let stale_client = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &stale_client,
        "plugin_json",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;
    let path = "/partial-transaction-conflict.json";
    write_file(
        &stale_client,
        path,
        b"{\"overlap\":\"base\",\"retained\":\"base\"}\n".to_vec(),
    )
    .await
    .unwrap();
    let winner_client = stale_client.open_another_session().await.unwrap();
    let mut stale = stale_client.begin_transaction().await.unwrap();
    let mut winner = winner_client.begin_transaction().await.unwrap();
    stale
        .execute(
            "UPDATE lix_file SET content = $1 WHERE path = $2",
            &[
                Value::Blob(
                    b"{\"overlap\":\"stale\",\"retained\":\"stale\"}\n"
                        .to_vec()
                        .into(),
                ),
                Value::Text(path.to_owned()),
            ],
        )
        .await
        .unwrap();
    winner
        .execute(
            "UPDATE lix_file SET content = $1 WHERE path = $2",
            &[
                Value::Blob(
                    b"{\"overlap\":\"winner\",\"retained\":\"base\"}\n"
                        .to_vec()
                        .into(),
                ),
                Value::Text(path.to_owned()),
            ],
        )
        .await
        .unwrap();

    winner.commit().await.unwrap();

    stale.commit().await.unwrap();

    let bytes = read_file(&stale_client, path).await.unwrap().unwrap();
    let rendered: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(rendered["retained"], "stale");
    assert_eq!(read_file(&winner_client, path).await.unwrap(), Some(bytes));
    winner_client.close().await.unwrap();
    stale_client.close().await.unwrap();
}

#[tokio::test]
async fn stale_json_transaction_batches_conflicts_into_one_render_transition() {
    const CONFLICTS: usize = 32;
    let archive = build_json_plugin_archive();
    let stale_client = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &stale_client,
        "plugin_json",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;
    let path = "/batched-transaction-conflict.json";
    let base = serde_json::Value::Object(
        (0..CONFLICTS)
            .map(|index| (format!("key-{index:02}"), serde_json::json!("base")))
            .collect(),
    );
    write_file(
        &stale_client,
        path,
        serde_json::to_vec(&base).expect("base JSON should encode"),
    )
    .await
    .unwrap();
    let winner_client = stale_client.open_another_session().await.unwrap();
    let mut stale = stale_client.begin_transaction().await.unwrap();
    let mut winner = winner_client.begin_transaction().await.unwrap();
    for (transaction, value) in [(&mut stale, "stale"), (&mut winner, "winner")] {
        let changed = serde_json::Value::Object(
            (0..CONFLICTS)
                .map(|index| (format!("key-{index:02}"), serde_json::json!(value)))
                .collect(),
        );
        transaction
            .execute(
                "UPDATE lix_file SET content = $1 WHERE path = $2",
                &[
                    Value::Blob(
                        serde_json::to_vec(&changed)
                            .expect("changed JSON should encode")
                            .into(),
                    ),
                    Value::Text(path.to_owned()),
                ],
            )
            .await
            .unwrap();
    }

    winner.commit().await.unwrap();

    stale.commit().await.unwrap();

    assert_eq!(
        read_file(&stale_client, path).await.unwrap(),
        read_file(&winner_client, path).await.unwrap()
    );
    winner_client.close().await.unwrap();
    stale_client.close().await.unwrap();
}

#[tokio::test]
async fn same_base_transactions_resolve_reference_plugin_file_overlaps() {
    let cases = [
        (
            "json",
            "plugin_json",
            build_json_plugin_archive(),
            vec!["json_root", "json_object_member", "json_array_item"],
            b"{\"a\":\"base\",\"b\":\"base\"}\n".to_vec(),
            b"{\"a\":\"first\",\"b\":\"first\"}\n".to_vec(),
            b"{\"a\":\"second\",\"b\":\"second\"}\n".to_vec(),
        ),
        (
            "csv",
            "plugin_csv",
            build_csv_plugin_archive(),
            vec!["csv_table", "csv_row"],
            b"name,value\nitem,base\nother,base\n".to_vec(),
            b"name,value\nitem,first\nother,first\n".to_vec(),
            b"name,value\nitem,second\nother,second\n".to_vec(),
        ),
        (
            "markdown",
            "plugin_markdown",
            build_markdown_plugin_archive(),
            vec!["markdown_node"],
            b"# Base\n\nBase paragraph\n".to_vec(),
            b"# First\n\nFirst paragraph\n".to_vec(),
            b"# Second\n\nSecond paragraph\n".to_vec(),
        ),
        (
            "text",
            "plugin_text",
            build_text_plugin_archive(),
            vec!["text_line"],
            b"base one\nbase two\n".to_vec(),
            b"first one\nfirst two\n".to_vec(),
            b"second one\nsecond two\n".to_vec(),
        ),
    ];

    for (extension, plugin_key, archive, schemas, base, first_edit, second_edit) in cases {
        let first = open_lix().await.unwrap();
        install_reference_plugin_in_blank_registry(&first, plugin_key, &archive, &schemas).await;
        let path = format!("/transaction-conflict.{extension}");
        write_file(&first, &path, base.clone()).await.unwrap();
        let second = first.open_another_session().await.unwrap();
        let mut first_transaction = first.begin_transaction().await.unwrap();
        let mut second_transaction = second.begin_transaction().await.unwrap();
        for (transaction, bytes) in [
            (&mut first_transaction, first_edit),
            (&mut second_transaction, second_edit),
        ] {
            transaction
                .execute(
                    "UPDATE lix_file SET content = $1 WHERE path = $2",
                    &[Value::Blob(bytes.into()), Value::Text(path.clone())],
                )
                .await
                .unwrap();
        }

        first_transaction.commit().await.unwrap();

        second_transaction
            .commit()
            .await
            .unwrap_or_else(|error| panic!("{extension} overlap should resolve: {error}"));

        let first_bytes = read_file(&first, &path).await.unwrap().unwrap();
        let second_bytes = read_file(&second, &path).await.unwrap().unwrap();
        assert_eq!(
            first_bytes, second_bytes,
            "{extension} clients must converge"
        );
        assert_ne!(first_bytes, base, "{extension} must commit a resolved edit");
        second.close().await.unwrap();
        first.close().await.unwrap();
    }
}

#[tokio::test]
async fn v2_json_rejects_mixed_byte_and_row_transitions_in_one_transaction() {
    let archive = build_json_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;
    let path = "/mixed-transition.json";
    let first = b"{\"value\":\"first\"}\n".to_vec();
    let bytes_only = b"{\"value\":\"bytes\"}\n".to_vec();
    write_file(&lix, path, first).await.unwrap();
    let file_id = file_id_at_path(&lix, path).await;

    let mut transaction = lix.begin_transaction().await.unwrap();
    transaction
        .execute(
            "UPDATE lix_file SET content = $1 WHERE path = $2",
            &[
                Value::Blob(bytes_only.clone().into()),
                Value::Text(path.to_string()),
            ],
        )
        .await
        .unwrap();
    let error = transaction
        .execute(
            "UPDATE json_object_member SET scalar_json = $1 \
             WHERE parent_id = 'root' AND key = 'value' AND lixcol_file_id = $2",
            &[
                Value::Jsonb(serde_json::json!("row").into()),
                Value::Text(file_id),
            ],
        )
        .await
        .expect_err("one transaction must choose byte or semantic authority per file");
    assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);

    // Rejection restores the earlier pending byte successor; committing the
    // still-valid transaction publishes exactly that first transition.
    transaction.commit().await.unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(bytes_only));
    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_excalidraw_roundtrips_and_renders_local_element_edits() {
    let archive = build_excalidraw_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_excalidraw",
        &archive,
        &["excalidraw_scene", "excalidraw_element", "excalidraw_file"],
    )
    .await;

    let path = "/component-v2.excalidraw";
    let source = br##"{
  "type": "excalidraw",
  "version": 2,
  "source": "https://excalidraw.com",
  "elements": [
    {"id":"a","type":"rectangle","x":1.25,"y":2,"width":100,"height":80,"isDeleted":false},
    {"id":"b","type":"ellipse","x":20,"y":30,"width":50,"height":40,"isDeleted":false}
  ],
  "appState": {"gridSize":20,"viewBackgroundColor":"#ffffff"},
  "files": {
    "file-1": {"id":"file-1","mimeType":"image/png","dataURL":"data:image/png;base64,AA==","created":123}
  }
}
"##
    .to_vec();
    write_file(&lix, path, source.clone()).await.unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(source.clone()));

    let elements = lix
        .execute(
            "SELECT id, element_type FROM excalidraw_element ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(elements.len(), 2);
    assert_eq!(elements.rows()[0].get::<String>("id").unwrap(), "a");
    assert_eq!(
        elements.rows()[0].get::<String>("element_type").unwrap(),
        "rectangle"
    );

    let geometry_edit = String::from_utf8(source)
        .unwrap()
        .replacen(r#""x":1.25"#, r#""x":123.5"#, 1)
        .into_bytes();

    write_file(&lix, path, geometry_edit.clone()).await.unwrap();

    let element = lix
        .execute(
            "SELECT element_json FROM excalidraw_element WHERE id = 'b'",
            &[],
        )
        .await
        .unwrap();
    let mut element_json = element.rows()[0]
        .get::<serde_json::Value>("element_json")
        .unwrap();
    element_json["isDeleted"] = serde_json::Value::Bool(true);
    lix.execute(
        "UPDATE excalidraw_element \
         SET element_json = $1, is_deleted = $2 \
         WHERE id = 'b'",
        &[Value::Jsonb(element_json.into()), Value::Boolean(true)],
    )
    .await
    .unwrap();

    let rendered = read_file(&lix, path).await.unwrap().unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&rendered).unwrap();
    assert_eq!(parsed["elements"][0]["x"], serde_json::json!(123.5));
    assert_eq!(
        parsed["elements"][1]["isDeleted"],
        serde_json::Value::Bool(true)
    );

    let mut first_element = lix
        .execute(
            "SELECT element_json FROM excalidraw_element WHERE id = 'a'",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<serde_json::Value>("element_json")
        .unwrap();
    first_element["x"] = serde_json::json!(123456.75);
    lix.execute(
        "UPDATE excalidraw_element SET element_json = $1 WHERE id = 'a'",
        &[Value::Jsonb(first_element.into())],
    )
    .await
    .expect("semantic element growth should render");
    let after_semantic = read_file(&lix, path).await.unwrap().unwrap();
    let after_followup = String::from_utf8(after_semantic)
        .unwrap()
        .replacen(r#""x":20"#, r#""x":21"#, 1)
        .into_bytes();
    write_file(&lix, path, after_followup.clone())
        .await
        .expect("a byte edit after semantic rendering must not use stale Excalidraw spans");
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(after_followup.clone())
    );
    let elements = lix
        .execute(
            "SELECT id, element_json FROM excalidraw_element ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    assert!(
        elements.rows()[0]
            .get::<serde_json::Value>("element_json")
            .unwrap()
            .to_string()
            .contains("123456.75")
    );
    assert!(
        elements.rows()[1]
            .get::<serde_json::Value>("element_json")
            .unwrap()
            .to_string()
            .contains(r#""x":21"#)
    );

    let renamed = String::from_utf8(after_followup)
        .unwrap()
        .replacen(r#""id":"a""#, r#""id":"c""#, 1)
        .into_bytes();
    write_file(&lix, path, renamed.clone())
        .await
        .expect("an element ID edit must use full Excalidraw reconciliation");
    let ids = lix
        .execute("SELECT id FROM excalidraw_element ORDER BY id", &[])
        .await
        .unwrap()
        .rows()
        .iter()
        .map(|row| row.get::<String>("id").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(ids, ["b", "c"]);
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(renamed));

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v3_excalidraw_cold_successor_after_reopen_rebuilds_span_state() {
    let root = tempfile::tempdir().expect("create v3 Excalidraw reopen directory");
    let lix = open_rocksdb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_excalidraw",
        &build_excalidraw_plugin_archive(),
        &["excalidraw_scene", "excalidraw_element", "excalidraw_file"],
    )
    .await;
    let path = "/v3-excalidraw-cold.excalidraw";
    let before = br#"{"type":"excalidraw","version":2,"source":"test","elements":[{"id":"a","type":"rectangle","x":1,"y":2,"width":3,"height":4,"isDeleted":false}],"appState":{},"files":{}}"#.to_vec();
    write_file(&lix, path, before).await.unwrap();
    lix.close().await.unwrap();

    let reopened = open_rocksdb_lix(root.path()).await;

    let cold = br#"{"type":"excalidraw","version":2,"source":"test","elements":[{"id":"a","type":"rectangle","x":10,"y":2,"width":3,"height":4,"isDeleted":false}],"appState":{},"files":{}}"#.to_vec();
    write_file(&reopened, path, cold.clone()).await.unwrap();

    assert_eq!(read_file(&reopened, path).await.unwrap(), Some(cold));

    // The cold successor must publish spans for its own bytes. A following
    // localized edit would address the wrong range if it inherited the
    // predecessor's index.
    let warm = br#"{"type":"excalidraw","version":2,"source":"test","elements":[{"id":"a","type":"rectangle","x":100,"y":2,"width":3,"height":4,"isDeleted":false}],"appState":{},"files":{}}"#.to_vec();

    write_file(&reopened, path, warm.clone()).await.unwrap();

    assert_eq!(read_file(&reopened, path).await.unwrap(), Some(warm));
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn excalidraw_element_boundary_insert_adds_element_through_full_reconciliation() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_excalidraw",
        &build_excalidraw_plugin_archive(),
        &["excalidraw_scene", "excalidraw_element", "excalidraw_file"],
    )
    .await;

    let path = "/element-insert.excalidraw";
    let before =
        br#"{"type":"excalidraw","version":2,"elements":[{"id":"a","type":"rectangle"}]}"#.to_vec();
    write_file(&lix, path, before).await.unwrap();
    let successor = br#"{"type":"excalidraw","version":2,"elements":[{"id":"a","type":"rectangle"},{"id":"b","type":"ellipse"}]}"#.to_vec();
    write_file(&lix, path, successor.clone())
        .await
        .expect("an element-boundary insertion must fall back to full reconciliation");

    assert_eq!(read_file(&lix, path).await.unwrap(), Some(successor));
    let elements = lix
        .execute(
            "SELECT id, element_type FROM excalidraw_element ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(elements.len(), 2);
    assert_eq!(elements.rows()[0].get::<String>("id").unwrap(), "a");
    assert_eq!(elements.rows()[1].get::<String>("id").unwrap(), "b");
    assert_eq!(
        elements.rows()[1].get::<String>("element_type").unwrap(),
        "ellipse"
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v3_excalidraw_certified_open_sparse_successor_history_and_reopen() {
    let root = tempfile::tempdir().expect("create Excalidraw v3 RocksDB directory");
    let lix = open_rocksdb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_excalidraw",
        &build_excalidraw_plugin_archive(),
        &["excalidraw_scene", "excalidraw_element", "excalidraw_file"],
    )
    .await;

    let path = "/component-v3.excalidraw";
    let before = br##"{
  "type": "excalidraw",
  "version": 2,
  "source": "https://excalidraw.com",
  "elements": [
    {"id":"a","type":"rectangle","x":1.25,"y":2,"width":100,"height":80,"isDeleted":false},
    {"id":"b","type":"ellipse","x":20,"y":30,"width":50,"height":40,"isDeleted":false}
  ],
  "appState": {"gridSize":20,"viewBackgroundColor":"#ffffff"},
  "files": {
    "file-1": {"id":"file-1","mimeType":"image/png","dataURL":"data:image/png;base64,AA==","created":123}
  }
}
"##
    .to_vec();

    write_file(&lix, path, before.clone()).await.unwrap();

    assert_eq!(read_file(&lix, path).await.unwrap(), Some(before.clone()));
    assert_eq!(
        lix.execute("SELECT COUNT(*) AS count FROM excalidraw_element", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<i64>("count")
            .unwrap(),
        2
    );

    let after = String::from_utf8(before)
        .unwrap()
        .replacen(r#""x":1.25"#, r#""x":123.5"#, 1)
        .into_bytes();

    write_file(&lix, path, after.clone()).await.unwrap();

    assert_eq!(read_file(&lix, path).await.unwrap(), Some(after.clone()));
    assert!(
        lix.execute(
            "SELECT element_json FROM excalidraw_element WHERE id = 'a'",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
            .get::<serde_json::Value>("element_json")
            .unwrap()
            .to_string()
            .contains("123.5")
    );
    assert!(
        lix.execute(
            "SELECT element_json FROM lix_history('excalidraw_element') \
             WHERE id = 'a' AND lixcol_depth = 1",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
            .get::<serde_json::Value>("element_json")
            .unwrap()
            .to_string()
            .contains("1.25")
    );
    lix.close().await.unwrap();

    let reopened = open_rocksdb_lix(root.path()).await;
    assert_eq!(read_file(&reopened, path).await.unwrap(), Some(after));
    assert_eq!(
        reopened
            .execute("SELECT COUNT(*) AS count FROM excalidraw_element", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<i64>("count")
            .unwrap(),
        2
    );
    reopened.close().await.unwrap();
}

// A space id has exactly one value semantics, declared once in Lix
// registry. These read it back instead of restating id, name and semantics.
fn certified_row_batch_space() -> StorageSpace {
    lix::storage_bench::storage_space_by_name("hot_state.certified_row_batch.v1")
}

fn certified_row_batch_page_space() -> StorageSpace {
    lix::storage_bench::storage_space_by_name("hot_state.certified_row_batch_page.v1")
}
const CEB2_FIXTURE_PATH: &str = "/ceb2-hard-cut.excalidraw";
const CEB2_FIXTURE_BYTES: &[u8] = br#"{"type":"excalidraw","version":2,"elements":[{"id":"a","type":"rectangle","x":1,"y":2,"width":3,"height":4,"isDeleted":false}]}"#;

async fn storage_space_entries<StorageImpl>(
    storage: &StorageImpl,
    space: StorageSpace,
) -> Vec<(Key, Bytes)>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("open raw certified storage read");
    let mut entries = Vec::new();
    let mut cursor = read
        .begin_scan(
            space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            BeginScanOptions {
                projection: CoreProjection::FullValue,
                ..BeginScanOptions::default()
            },
        )
        .await
        .expect("begin raw certified storage scan");
    entries.extend(
        cursor
            .collect_all()
            .await
            .expect("scan raw certified storage space")
            .into_iter()
            .map(|entry| {
                let ProjectedValue::FullValue(bytes) = entry.value else {
                    panic!("full certified storage projection must return bytes");
                };
                (entry.key, bytes)
            }),
    );
    entries
}

async fn write_and_verify_ceb2_fixture<StorageImpl>(storage: &StorageImpl)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open CEB2 fixture workspace");
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_excalidraw",
        &build_excalidraw_plugin_archive(),
        &["excalidraw_scene", "excalidraw_element", "excalidraw_file"],
    )
    .await;
    write_file(&lix, CEB2_FIXTURE_PATH, CEB2_FIXTURE_BYTES.to_vec())
        .await
        .expect("write CEB2 fixture");
    assert_eq!(
        read_file(&lix, CEB2_FIXTURE_PATH).await.unwrap(),
        Some(CEB2_FIXTURE_BYTES.to_vec())
    );
    assert_eq!(
        lix.execute("SELECT COUNT(*) AS count FROM excalidraw_element", &[])
            .await
            .expect("read CEB2 semantic row")
            .rows()[0]
            .get::<i64>("count")
            .unwrap(),
        1
    );
    lix.close().await.expect("close CEB2 fixture workspace");

    let contents = storage_space_entries(storage, certified_row_batch_space()).await;
    assert!(
        !contents.is_empty(),
        "writer must publish a certified batch"
    );
    assert!(
        contents.iter().all(|(_, value)| value.starts_with(b"CEB2")),
        "current writers must emit only CEB2"
    );
    assert!(
        !storage_space_entries(storage, certified_row_batch_page_space())
            .await
            .is_empty(),
        "CEB2 writer must publish external pages"
    );
}

async fn verify_reopened_ceb2_fixture<StorageImpl>(storage: &StorageImpl)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("reopen CEB2 fixture workspace");
    assert_eq!(
        read_file(&lix, CEB2_FIXTURE_PATH).await.unwrap(),
        Some(CEB2_FIXTURE_BYTES.to_vec())
    );
    assert_eq!(
        lix.execute("SELECT COUNT(*) AS count FROM excalidraw_element", &[])
            .await
            .expect("read reopened CEB2 semantic row")
            .rows()[0]
            .get::<i64>("count")
            .unwrap(),
        1
    );
    lix.close().await.expect("close reopened CEB2 workspace");
}

async fn corrupt_first_ceb2_page<StorageImpl>(storage: &StorageImpl)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut pages = storage_space_entries(storage, certified_row_batch_page_space()).await;
    let (key, mut bytes) = pages
        .drain(..)
        .next()
        .expect("CEB2 fixture must own an external page");
    assert!(bytes.len() > 1, "CEB2 fixture page must be non-empty");
    bytes.truncate(bytes.len() - 1);
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("open CEB2 corruption write");
    write
        .put_many(
            certified_row_batch_page_space(),
            PutBatch {
                entries: vec![PutEntry {
                    key,
                    value: StoredValue { bytes },
                }],
            },
        )
        .await
        .expect("stage corrupt CEB2 page");
    write.commit().await.expect("commit corrupt CEB2 page");
}

async fn verify_corrupt_ceb2_fails_closed<StorageImpl>(storage: &StorageImpl)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("reopen corrupt CEB2 workspace");
    let error = lix
        .execute("SELECT COUNT(*) AS count FROM excalidraw_element", &[])
        .await
        .expect_err("corrupt CEB2 page must fail closed");
    assert!(
        error.to_string().contains("certified row batch"),
        "unexpected CEB2 corruption error: {error}"
    );
    lix.close().await.expect("close corrupt CEB2 workspace");
}

#[tokio::test]
#[ignore = "CEB2 was retired by the protocol-v69 typed snapshot hard cut"]
async fn v3_ceb2_roundtrip_corruption_and_reopen_memory() {
    let storage = lix::Memory::new();
    write_and_verify_ceb2_fixture(&storage).await;
    verify_reopened_ceb2_fixture(&storage).await;
    corrupt_first_ceb2_page(&storage).await;
    verify_corrupt_ceb2_fails_closed(&storage).await;
}

#[tokio::test]
#[ignore = "CEB2 was retired by the protocol-v69 typed snapshot hard cut"]
async fn v3_ceb2_roundtrip_corruption_and_reopen_rocksdb() {
    let root = tempfile::tempdir().expect("create CEB2 RocksDB directory");
    let path = root.path().join("ceb2.rocksdb");
    {
        let storage = RocksDB::open(&path).expect("open CEB2 RocksDB storage");
        write_and_verify_ceb2_fixture(&storage).await;
        storage.flush().expect("flush CEB2 RocksDB write");
    }
    {
        let storage = RocksDB::open(&path).expect("reopen CEB2 RocksDB storage");
        verify_reopened_ceb2_fixture(&storage).await;
        corrupt_first_ceb2_page(&storage).await;
        storage.flush().expect("flush corrupt CEB2 RocksDB page");
    }
    let storage = RocksDB::open(&path).expect("reopen corrupt CEB2 RocksDB storage");
    verify_corrupt_ceb2_fails_closed(&storage).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "CEB2 was retired by the protocol-v69 typed snapshot hard cut"]
async fn v3_ceb2_roundtrip_corruption_and_reopen_slatedb() {
    let root = tempfile::tempdir().expect("create CEB2 SlateDB directory");
    let path = root.path().join("ceb2.slatedb");
    {
        let storage = SlateDB::open(&path).expect("open CEB2 SlateDB storage");
        write_and_verify_ceb2_fixture(&storage).await;
        storage.flush().await.expect("flush CEB2 SlateDB write");
    }
    {
        let storage = SlateDB::open(&path).expect("reopen CEB2 SlateDB storage");
        verify_reopened_ceb2_fixture(&storage).await;
        corrupt_first_ceb2_page(&storage).await;
        storage
            .flush()
            .await
            .expect("flush corrupt CEB2 SlateDB page");
    }
    let storage = SlateDB::open(&path).expect("reopen corrupt CEB2 SlateDB storage");
    verify_corrupt_ceb2_fails_closed(&storage).await;
}

#[tokio::test]
#[ignore = "focused CEB2 certified-row read benchmark probe"]
async fn v3_ceb2_certified_row_read_benchmark() {
    const READS_PER_SAMPLE: usize = 100;
    const SAMPLES: usize = 9;
    let lix = open_lix().await.expect("open CEB2 benchmark workspace");
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_excalidraw",
        &build_excalidraw_plugin_archive(),
        &["excalidraw_scene", "excalidraw_element", "excalidraw_file"],
    )
    .await;
    write_file(
        &lix,
        "/ceb2-benchmark.excalidraw",
        br#"{"type":"excalidraw","version":2,"elements":[{"id":"a","type":"rectangle","x":1,"y":2,"width":3,"height":4,"isDeleted":false}]}"#
            .to_vec(),
    )
    .await
    .expect("write certified CEB2 benchmark fixture");

    let mut measurements = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        for _ in 0..READS_PER_SAMPLE {
            let result = lix
                .execute(
                    "SELECT element_json FROM excalidraw_element WHERE id = 'a'",
                    &[],
                )
                .await
                .expect("read one certified CEB2 row");
            black_box(
                result.rows()[0]
                    .get::<serde_json::Value>("element_json")
                    .unwrap(),
            );
        }
        let measurement = BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
        eprintln!(
            "CEB2_READ_SAMPLE sample={sample} reads={READS_PER_SAMPLE} elapsed_ms={} allocations={} allocated_bytes={} peak_live_bytes={}",
            measurement.elapsed_ms,
            measurement.allocations.allocation_count,
            measurement.allocations.allocated_bytes,
            measurement.allocations.peak_live_bytes_delta,
        );
        measurements.push(measurement);
    }
    let mut elapsed = measurements
        .iter()
        .map(|measurement| measurement.elapsed_ms)
        .collect::<Vec<_>>();
    elapsed.sort_by(f64::total_cmp);
    let mut allocations = measurements
        .iter()
        .map(|measurement| measurement.allocations.allocation_count)
        .collect::<Vec<_>>();
    allocations.sort_unstable();
    let mut allocated_bytes = measurements
        .iter()
        .map(|measurement| measurement.allocations.allocated_bytes)
        .collect::<Vec<_>>();
    allocated_bytes.sort_unstable();
    let mut peak_live = measurements
        .iter()
        .map(|measurement| measurement.allocations.peak_live_bytes_delta)
        .collect::<Vec<_>>();
    peak_live.sort_unstable();
    eprintln!(
        "CEB2_READ_SUMMARY samples={SAMPLES} reads_per_sample={READS_PER_SAMPLE} elapsed_ms_p50={} allocations_p50={} allocated_bytes_p50={} peak_live_bytes_p50={}",
        elapsed[SAMPLES / 2],
        allocations[SAMPLES / 2],
        allocated_bytes[SAMPLES / 2],
        peak_live[SAMPLES / 2],
    );
    lix.close().await.expect("close CEB2 benchmark workspace");
}

#[tokio::test]
async fn v2_excalidraw_same_element_branch_merge_uses_canonical_b() {
    let archive = build_excalidraw_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_excalidraw",
        &archive,
        &["excalidraw_scene", "excalidraw_element", "excalidraw_file"],
    )
    .await;

    let path = "/element-conflict.excalidraw";
    write_file(
        &lix,
        path,
        br#"{"type":"excalidraw","version":2,"source":"https://excalidraw.com","elements":[{"id":"shape","type":"rectangle","x":1,"y":2,"width":3,"height":4,"isDeleted":false}],"appState":{},"files":{}}"#.to_vec(),
    )
    .await
    .expect("base Excalidraw scene should import");
    let file_id = file_id_at_path(&lix, path).await;
    let original = lix
        .execute(
            "SELECT element_json FROM excalidraw_element \
             WHERE id = 'shape' AND lixcol_file_id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<serde_json::Value>("element_json")
        .unwrap();
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-00000000050d".to_owned()),
            name: "Excalidraw element conflict source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    let mut target_json = original.clone();
    target_json["x"] = serde_json::json!(111);
    lix.execute(
        "UPDATE excalidraw_element SET element_json = $1 \
         WHERE id = 'shape' AND lixcol_file_id = $2",
        &[
            Value::Jsonb(target_json.into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("target element edit should commit");
    let target_order = excalidraw_v2_element_ordering(&lix, &file_id, "shape").await;

    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    let mut source_json = original;
    source_json["x"] = serde_json::json!(222);
    lix.execute(
        "UPDATE excalidraw_element SET element_json = $1 \
         WHERE id = 'shape' AND lixcol_file_id = $2",
        &[
            Value::Jsonb(source_json.into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("source element edit should commit");
    let source_order = excalidraw_v2_element_ordering(&lix, &file_id, "shape").await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id,
    })
    .await
    .unwrap();

    let expected_x = if source_order < target_order {
        111
    } else {
        222
    };
    let preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .expect("same-element Excalidraw conflict should preview");
    assert!(preview.conflicts.is_empty(), "{:?}", preview.conflicts);

    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("same-element Excalidraw conflict should resolve deterministically");
    let rendered: serde_json::Value =
        serde_json::from_slice(&read_file(&lix, path).await.unwrap().unwrap()).unwrap();
    assert_eq!(rendered["elements"][0]["x"], serde_json::json!(expected_x));

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v3_excalidraw_same_element_branch_merge_uses_canonical_b() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_excalidraw",
        &build_excalidraw_plugin_archive(),
        &["excalidraw_scene", "excalidraw_element", "excalidraw_file"],
    )
    .await;

    let path = "/v3-element-conflict.excalidraw";
    write_file(
        &lix,
        path,
        br#"{"type":"excalidraw","version":2,"source":"https://excalidraw.com","elements":[{"id":"shape","type":"rectangle","x":1,"y":2,"width":3,"height":4,"isDeleted":false}],"appState":{},"files":{}}"#.to_vec(),
    )
    .await
    .expect("base Excalidraw scene should import");
    let file_id = file_id_at_path(&lix, path).await;
    let original = lix
        .execute(
            "SELECT element_json FROM excalidraw_element \
             WHERE id = 'shape' AND lixcol_file_id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<serde_json::Value>("element_json")
        .unwrap();
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-00000000060c".to_owned()),
            name: "Excalidraw v3 element conflict source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    let mut target_json = original.clone();
    target_json["x"] = serde_json::json!(111);
    lix.execute(
        "UPDATE excalidraw_element SET element_json = $1 \
         WHERE id = 'shape' AND lixcol_file_id = $2",
        &[
            Value::Jsonb(target_json.into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("target element edit should commit");
    let target_order = excalidraw_v2_element_ordering(&lix, &file_id, "shape").await;

    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    let mut source_json = original;
    source_json["x"] = serde_json::json!(222);
    lix.execute(
        "UPDATE excalidraw_element SET element_json = $1 \
         WHERE id = 'shape' AND lixcol_file_id = $2",
        &[
            Value::Jsonb(source_json.into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("source element edit should commit");
    let source_order = excalidraw_v2_element_ordering(&lix, &file_id, "shape").await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id,
    })
    .await
    .unwrap();

    let expected_x = if source_order < target_order {
        111
    } else {
        222
    };
    let preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .expect("same-element v3 Excalidraw conflict should preview");
    assert!(preview.conflicts.is_empty(), "{:?}", preview.conflicts);

    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("same-element v3 Excalidraw conflict should resolve deterministically");
    let rendered: serde_json::Value =
        serde_json::from_slice(&read_file(&lix, path).await.unwrap().unwrap()).unwrap();
    assert_eq!(rendered["elements"][0]["x"], serde_json::json!(expected_x));

    lix.close().await.unwrap();
}

#[tokio::test]
async fn partial_checkpoint_rebases_all_plugin_rows_for_one_file() {
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &build_csv_plugin_archive(),
        &["csv_table", "csv_row"],
    )
    .await;
    write_file(&lix, "/selected.csv", b"name,value\na,one\nb,two\n".to_vec())
        .await
        .unwrap();
    write_file(
        &lix,
        "/remaining.csv",
        b"name,value\nx,ten\ny,twenty\n".to_vec(),
    )
    .await
    .unwrap();
    let baseline_checkpoint = lix.create_checkpoint().await.unwrap();
    let selected_file_id = file_id_at_path(&lix, "/selected.csv").await;
    let remaining_file_id = file_id_at_path(&lix, "/remaining.csv").await;

    write_file(
        &lix,
        "/selected.csv",
        b"name,value\na,ONE\nb,TWO\nc,THREE\n".to_vec(),
    )
    .await
    .unwrap();
    write_file(
        &lix,
        "/remaining.csv",
        b"name,value\nx,TEN\ny,TWENTY\nz,THIRTY\n".to_vec(),
    )
    .await
    .unwrap();
    let selected_diff_count = lix
        .execute(
            "SELECT coalesce(sum(row_count), 0) AS count \
             FROM lix_diff('lix_file', $2, lix_active_branch_commit_id()) \
             WHERE lixcol_row_pk ->> 0 = $1",
            &[
                Value::Text(selected_file_id.clone()),
                Value::Text(baseline_checkpoint.commit_id),
            ],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<i64>("count")
        .unwrap();
    assert!(selected_diff_count > 1, "CSV must fan out beyond lix_file");
    let selected_checkpoint = lix.execute(
        "INSERT INTO lix_create_checkpoint (relation, row_pk) \
         SELECT 'lix_file', lixcol_row_pk \
         FROM lix_diff('lix_file', lix_root_commit_id(), lix_active_branch_commit_id()) \
         WHERE lixcol_row_pk ->> 0 = $1 \
         RETURNING commit_id",
        &[Value::Text(selected_file_id.clone())],
    )
    .await
    .expect("checkpoint all selected plugin rows");

    assert_eq!(
        lix.execute(
            "SELECT COUNT(*) AS count \
             FROM lix_diff('lix_file', $2, lix_active_branch_commit_id()) \
             WHERE lixcol_row_pk ->> 0 = $1",
            &[
                Value::Text(selected_file_id.clone()),
                Value::Text(selected_checkpoint.rows()[0].get::<String>("commit_id").unwrap()),
            ],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<i64>("count")
        .unwrap(),
        0,
    );
    assert!(
        lix.execute(
            "SELECT COUNT(*) AS count \
             FROM lix_diff('lix_file', $2, lix_active_branch_commit_id()) \
             WHERE lixcol_row_pk ->> 0 = $1",
            &[
                Value::Text(remaining_file_id),
                Value::Text(selected_checkpoint.rows()[0].get::<String>("commit_id").unwrap()),
            ],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<i64>("count")
        .unwrap()
            > 0,
    );
    assert_eq!(active_csv_rows(&lix, &selected_file_id).await.len(), 4);
    assert_eq!(
        read_file(&lix, "/selected.csv").await.unwrap().unwrap(),
        b"name,value\na,ONE\nb,TWO\nc,THREE\n",
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_ids_survive_insert_edit_reorder_delete_eviction_and_cold_reopen() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive = build_csv_plugin_archive();
    let path = "/identity-lifecycle.csv";
    let lix = open_filesystem_lix(tempdir.path()).await;
    install_plugin(&lix, "plugin_csv", &archive).await.unwrap();

    let initial = b"alpha,one\ndup,same\ndup,same\nomega,last\n".to_vec();
    write_file(&lix, path, initial).await.unwrap();
    let file_id = file_id_at_path(&lix, path).await;
    let initial_rows = active_csv_rows(&lix, &file_id).await;
    let alpha_id = csv_row_id(&initial_rows, &["alpha", "one"]);
    let omega_id = csv_row_id(&initial_rows, &["omega", "last"]);
    let duplicate_ids = csv_row_ids(&initial_rows, &["dup", "same"]);
    assert_eq!(duplicate_ids.len(), 2);
    assert_ne!(duplicate_ids[0], duplicate_ids[1]);

    let inserted = b"alpha,one\ninserted,new\ndup,same\ndup,same\nomega,last\n".to_vec();
    write_file(&lix, path, inserted).await.unwrap();
    let after_insert = active_csv_rows(&lix, &file_id).await;
    assert_eq!(csv_row_id(&after_insert, &["alpha", "one"]), alpha_id);
    assert_eq!(csv_row_id(&after_insert, &["omega", "last"]), omega_id);
    assert_eq!(csv_row_ids(&after_insert, &["dup", "same"]), duplicate_ids);
    let inserted_id = csv_row_id(&after_insert, &["inserted", "new"]);
    assert!(
        !initial_rows.iter().any(|row| row.id == inserted_id),
        "an inserted row must receive a fresh compact identity"
    );

    let edited = b"alpha,ONE\ninserted,new\ndup,same\ndup,same\nomega,last\n".to_vec();
    write_file(&lix, path, edited).await.unwrap();
    let after_edit = active_csv_rows(&lix, &file_id).await;
    assert_eq!(csv_row_id(&after_edit, &["alpha", "ONE"]), alpha_id);

    let reordered = b"omega,last\ndup,same\nalpha,ONE\ninserted,new\ndup,same\n".to_vec();
    write_file(&lix, path, reordered).await.unwrap();
    let after_reorder = active_csv_rows(&lix, &file_id).await;
    assert_eq!(csv_row_id(&after_reorder, &["omega", "last"]), omega_id);
    assert_eq!(csv_row_id(&after_reorder, &["alpha", "ONE"]), alpha_id);
    assert_eq!(
        csv_row_id(&after_reorder, &["inserted", "new"]),
        inserted_id
    );
    assert_eq!(csv_row_ids(&after_reorder, &["dup", "same"]), duplicate_ids);

    let final_bytes = b"omega,last\ndup,same\ninserted,new\n".to_vec();
    write_file(&lix, path, final_bytes.clone()).await.unwrap();
    let final_rows = active_csv_rows(&lix, &file_id).await;
    assert_eq!(csv_row_id(&final_rows, &["omega", "last"]), omega_id);
    assert_eq!(csv_row_id(&final_rows, &["inserted", "new"]), inserted_id);
    let remaining_duplicate_ids = csv_row_ids(&final_rows, &["dup", "same"]);
    assert_eq!(remaining_duplicate_ids.len(), 1);
    assert!(duplicate_ids.contains(&remaining_duplicate_ids[0]));
    assert!(!final_rows.iter().any(|row| row.id == alpha_id));

    // The production cache admits eight file actors. Opening more distinct
    // files forces the lifecycle actor out, so this read exercises semantic
    // cold-open/render equivalence without a test-only eviction hook.
    for index in 0..12 {
        write_file(
            &lix,
            &format!("/eviction-{index}.csv"),
            format!("eviction,{index}\n").into_bytes(),
        )
        .await
        .unwrap();
    }
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(final_bytes.clone())
    );
    assert_eq!(active_csv_rows(&lix, &file_id).await, final_rows);
    lix.close().await.unwrap();

    let lix = open_filesystem_lix(tempdir.path()).await;
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(final_bytes));
    assert_eq!(active_csv_rows(&lix, &file_id).await, final_rows);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_exact_read_replaces_a_stale_actor_after_an_independent_engine_commit() {
    let tempdir = tempfile::tempdir().unwrap();
    let storage_a = FilesystemStorage::new(tempdir.path())
        .open()
        .expect("first shared filesystem storage opens");
    let lix_a = open_lix()
        .with_storage(storage_a.clone())
        .await
        .expect("first independent Lix opens");
    storage_a
        .start_sync(&lix_a)
        .await
        .expect("first shared filesystem sync starts");
    let archive = build_csv_plugin_archive();
    install_plugin(&lix_a, "plugin_csv", &archive)
        .await
        .unwrap();

    let path = "/cross-lix-root.csv";
    let initial = b"first,one\nsecond,two\n".to_vec();
    write_file(&lix_a, path, initial.clone()).await.unwrap();
    assert_eq!(
        read_file(&lix_a, path).await.unwrap(),
        Some(initial.clone())
    );

    // A separately opened Lix owns a distinct plugin runtime/actor cache while
    // sharing the same durable RocksDB-backed workspace.
    let storage_b = FilesystemStorage::new(tempdir.path())
        .open()
        .expect("second shared filesystem storage opens");
    let lix_b = open_lix()
        .with_storage(storage_b.clone())
        .await
        .expect("second independent Lix opens");
    storage_b
        .start_sync(&lix_b)
        .await
        .expect("second shared filesystem sync starts");
    assert_eq!(read_file(&lix_b, path).await.unwrap(), Some(initial));
    let advanced = b"first,ONE\nsecond,two\n".to_vec();
    write_file(&lix_b, path, advanced.clone()).await.unwrap();

    // The first Lix still owns the root-old actor. Its exact SQL read returns the
    // durable materialized bytes without hydrating Wasm; the next write
    // cold-opens root-new and replaces only that captured stale slot.

    assert_eq!(
        read_file(&lix_a, path).await.unwrap(),
        Some(advanced.clone())
    );

    let final_bytes = b"first,ONE\nsecond,TWO\n".to_vec();

    write_file(&lix_a, path, final_bytes.clone())
        .await
        .expect("the next write restores root-new authority and applies the sparse edit");

    assert_eq!(read_file(&lix_a, path).await.unwrap(), Some(final_bytes));

    lix_b.close().await.unwrap();
    lix_a.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_file_incarnation_fences_old_observations_after_delete_and_recreate() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_plugin(&lix, "plugin_csv", &archive).await.unwrap();
    let path = "/recreated.csv";
    let old_bytes = b"old,incarnation\n".to_vec();
    write_file(&lix, path, old_bytes.clone()).await.unwrap();
    let old_file_id = file_id_at_path(&lix, path).await;
    let stale = lix.open_another_session().await.unwrap();
    assert_eq!(read_file(&stale, path).await.unwrap(), Some(old_bytes));

    lix.execute(
        "DELETE FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_string())],
    )
    .await
    .unwrap();
    let new_bytes = b"new,incarnation\n".to_vec();
    lix.execute(
        "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
        &[
            Value::Text(old_file_id.clone()),
            Value::Text(path.to_string()),
            Value::Blob(new_bytes.clone().into()),
        ],
    )
    .await
    .expect("recreation should deliberately reuse the durable file identity");
    let new_file_id = file_id_at_path(&lix, path).await;
    assert_eq!(old_file_id, new_file_id);

    let stale_error = write_file(&stale, path, b"stale,overwrite\n".to_vec())
        .await
        .expect_err(
            "an observation for a deleted file incarnation must not authorize its successor",
        );
    assert_eq!(stale_error.code, LixError::CODE_PLUGIN_OBSERVATION_STALE);
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(new_bytes));

    stale.close().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn csv_sql_insert_materializes_nullable_typed_null_and_reopens() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = "/nullable-layout.csv";
    let lix = open_filesystem_lix(tempdir.path()).await;
    install_plugin(&lix, "plugin_csv", &build_csv_plugin_archive())
        .await
        .unwrap();
    write_file(&lix, path, b"base,one\n".to_vec())
        .await
        .unwrap();
    let file_id = file_id_at_path(&lix, path).await;

    lix.execute(
        "INSERT INTO csv_row (id, order_key, cells, lixcol_file_id) VALUES ($1, $2, $3, $4)",
        &[
            Value::Text("019c6b89-bb18-77a8-9164-000000000001".to_string()),
            Value::Text("fffffffffffffff0".to_string()),
            Value::Jsonb(serde_json::json!(["created", "without-layout"]).into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("omitted nullable layout should materialize as native SQL NULL");
    assert_query_count(
        &lix,
        "SELECT COUNT(*) AS count FROM csv_row WHERE id = $1 AND layout IS NULL",
        &[Value::Text(
            "019c6b89-bb18-77a8-9164-000000000001".to_string(),
        )],
        1,
    )
    .await;
    assert!(
        read_file(&lix, path)
            .await
            .unwrap()
            .unwrap()
            .windows(b"created,without-layout".len())
            .any(|window| window == b"created,without-layout")
    );
    lix.close().await.unwrap();

    let reopened = open_filesystem_lix(tempdir.path()).await;
    assert_query_count(
        &reopened,
        "SELECT COUNT(*) AS count FROM csv_row WHERE id = $1 AND layout IS NULL AND lixcol_file_id = $2",
        &[
            Value::Text("019c6b89-bb18-77a8-9164-000000000001".to_string()),
            Value::Text(file_id),
        ],
        1,
    )
    .await;
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_actor_state_isolated_by_branch_root() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_plugin(&lix, "plugin_csv", &archive).await.unwrap();
    let path = "/branch-isolation.csv";
    let main_bytes = b"main,one\nshared,row\n".to_vec();
    write_file(&lix, path, main_bytes.clone()).await.unwrap();
    let main_file_id = file_id_at_path(&lix, path).await;
    let main_rows = active_csv_rows(&lix, &main_file_id).await;
    let main_branch_id = lix.active_branch_id().await.unwrap();

    let branch = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-00000000050e".to_string()),
            name: "v2 actor isolation".to_string(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: branch.id.clone(),
    })
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(main_bytes.clone())
    );
    assert_eq!(active_csv_rows(&lix, &main_file_id).await, main_rows);

    let branch_bytes = b"branch,ONE\nshared,row\ninserted,branch\n".to_vec();
    write_file(&lix, path, branch_bytes.clone()).await.unwrap();
    let branch_rows = active_csv_rows(&lix, &main_file_id).await;
    assert_ne!(branch_rows, main_rows);

    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id,
    })
    .await
    .unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(main_bytes));
    assert_eq!(active_csv_rows(&lix, &main_file_id).await, main_rows);

    lix.switch_branch(SwitchBranchOptions {
        branch_id: branch.id,
    })
    .await
    .unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(branch_bytes));
    assert_eq!(active_csv_rows(&lix, &main_file_id).await, branch_rows);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_generation_upgrade_preflights_owned_files_and_fences_stale_sessions() {
    let original = build_csv_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_plugin(&lix, "plugin_csv", &original).await.unwrap();
    let path = "/upgrade.csv";
    let bytes = b"first,one\nsecond,two\n".to_vec();
    write_file(&lix, path, bytes.clone()).await.unwrap();

    let stale = lix.open_another_session().await.unwrap();
    assert_eq!(read_file(&stale, path).await.unwrap(), Some(bytes.clone()));

    // A packaging-only archive generation change exercises the complete
    // owner preflight while retaining the same compiled component contract.
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv"));
    let wasm = std::fs::read(wasm_path).unwrap();
    let compatible = build_csv_plugin_archive_variant(
        &wasm,
        include_str!("../../../plugins/csv/schema/csv_row.json").as_bytes(),
        Some(b"compatible-generation"),
    );
    assert_ne!(original, compatible);
    install_plugin(&lix, "plugin_csv", &compatible)
        .await
        .expect("byte-stable compatible generation should commit");
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(bytes.clone()));
    assert_eq!(
        read_file(&lix, "/.lix/plugins/plugin_csv.lixplugin")
            .await
            .unwrap(),
        Some(compatible.clone())
    );

    let stale_error = write_file(&stale, path, b"first,STALE\nsecond,two\n".to_vec())
        .await
        .expect_err("a session acknowledged under the previous generation must fail closed");
    assert_eq!(stale_error.code, LixError::CODE_PLUGIN_OBSERVATION_STALE);

    let mut changed_schema: serde_json::Value =
        serde_json::from_str(include_str!("../../../plugins/csv/schema/csv_row.json")).unwrap();
    changed_schema["description"] =
        serde_json::Value::String("incompatible replacement definition".to_string());
    let changed_schema = serde_json::to_vec(&changed_schema).unwrap();
    let schema_changing =
        build_csv_plugin_archive_variant(&wasm, &changed_schema, Some(b"schema-changing"));
    let schema_error = install_plugin(&lix, "plugin_csv", &schema_changing)
        .await
        .expect_err("an owned schema definition change must be rejected");
    assert_eq!(schema_error.code, LixError::CODE_CONSTRAINT_VIOLATION);

    // The archive validator intentionally performs only a bounded header
    // check. This component reaches the production compiler and is rejected
    // before the replacement registry generation can become authoritative.
    let invalid_component = b"\0asm\x0a\0\0\0";
    let trapping = build_csv_plugin_archive_variant(
        invalid_component,
        include_str!("../../../plugins/csv/schema/csv_row.json").as_bytes(),
        Some(b"invalid-component"),
    );
    install_plugin(&lix, "plugin_csv", &trapping)
        .await
        .expect_err("invalid replacement component must fail preflight");

    assert_eq!(
        read_file(&lix, "/.lix/plugins/plugin_csv.lixplugin")
            .await
            .unwrap(),
        Some(compatible),
        "failed upgrades must leave the compatible generation authoritative"
    );
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(bytes.clone()));
    let fresh = lix.open_another_session().await.unwrap();
    assert_eq!(read_file(&fresh, path).await.unwrap(), Some(bytes));

    write_file(&fresh, path, b"first,ONE\nsecond,two\n".to_vec())
        .await
        .expect("the retained authoritative generation should remain writable");

    stale.close().await.unwrap();
    fresh.close().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_generation_upgrade_with_disjoint_edits_remains_a_merge_conflict() {
    let original = build_csv_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_plugin(&lix, "plugin_csv", &original)
        .await
        .expect("base CSV generation should install");

    let path = "/generation-conflict.csv";
    let base = b"first,one\nsecond,two\n".to_vec();
    write_file(&lix, path, base.clone())
        .await
        .expect("base CSV should import");
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-00000000050f".to_owned()),
            name: "CSV generation conflict source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    let target_bytes = b"first,ONE\nsecond,two\n".to_vec();
    write_file(&lix, path, target_bytes.clone())
        .await
        .expect("target disjoint row edit should commit");

    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv"));
    let wasm = std::fs::read(wasm_path).unwrap();
    let upgraded = build_csv_plugin_archive_variant(
        &wasm,
        include_str!("../../../plugins/csv/schema/csv_row.json").as_bytes(),
        Some(b"source-generation"),
    );
    install_plugin(&lix, "plugin_csv", &upgraded)
        .await
        .expect("compatible source generation should preflight");
    write_file(&lix, path, b"first,one\nsecond,TWO\n".to_vec())
        .await
        .expect("source disjoint row edit should commit under the upgraded generation");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id,
    })
    .await
    .unwrap();

    let preview_error = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .expect_err("generation-divergent preview must abort before row reconciliation");
    assert_eq!(preview_error.code, LixError::CODE_MERGE_CONFLICT);

    let error = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .expect_err(
            "derived bytes must not be rendered by a different generation than is committed",
        );
    assert_eq!(error.code, LixError::CODE_MERGE_CONFLICT);

    assert_eq!(read_file(&lix, path).await.unwrap(), Some(target_bytes));
    assert_eq!(
        read_file(&lix, "/.lix/plugins/plugin_csv.lixplugin")
            .await
            .unwrap(),
        Some(original),
        "a rejected merge must retain the target generation"
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v4_csv_dialect_rename_after_eviction_uses_predecessor_descriptor() {
    let lix = open_lix().await.unwrap();
    install_plugin(&lix, "plugin_csv", &build_csv_plugin_archive())
        .await
        .unwrap();
    let before_path = "/dialect-before.csv";
    let after_path = "/dialect-after.tsv";
    let source = b"first,one\n".to_vec();
    write_file(&lix, before_path, source.clone()).await.unwrap();
    let file_id = file_id_at_path(&lix, before_path).await;
    assert_eq!(
        active_csv_rows(&lix, &file_id).await[0].cells,
        ["first", "one"]
    );

    for index in 0..20 {
        write_file(
            &lix,
            &format!("/dialect-evict-{index}.csv"),
            format!("eviction,{index}\n").into_bytes(),
        )
        .await
        .unwrap();
    }

    lix.execute(
        "UPDATE lix_file SET path = $1 WHERE path = $2",
        &[
            Value::Text(after_path.to_owned()),
            Value::Text(before_path.to_owned()),
        ],
    )
    .await
    .expect("an evicted CSV actor must observe the CSV to TSV descriptor transition");

    assert_eq!(read_file(&lix, before_path).await.unwrap(), None);
    assert_eq!(read_file(&lix, after_path).await.unwrap(), Some(source));
    assert_eq!(
        active_csv_rows(&lix, &file_id).await[0].cells,
        ["first,one"],
        "the successor must be reparsed with TSV delimiter semantics"
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_path_only_rename_rekeys_actor_and_cleans_owner_on_unmatch() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_plugin(&lix, "plugin_csv", &archive).await.unwrap();

    let before_path = "/before-rename.csv";
    let after_path = "/after-rename.csv";
    let raw_path = "/after-rename.txt";
    let initial = b"first,one\nsecond,two\n".to_vec();
    write_file(&lix, before_path, initial.clone())
        .await
        .unwrap();
    let file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(before_path.to_string())],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();

    // This reader must become stale solely because the accepted actor moves
    // to the descriptor-successor key, not because file bytes changed.
    let stale = lix.open_another_session().await.unwrap();
    assert_eq!(
        read_file(&stale, before_path).await.unwrap(),
        Some(initial.clone())
    );

    // A path-only UPDATE is ordinary SQL. Its DML source reads the exact
    // materialized bytes and establishes the observation needed for the warm
    // empty-splice descriptor transition.
    let renamer = lix.open_another_session().await.unwrap();
    let renamed = renamer
        .execute(
            "UPDATE lix_file SET path = $1 WHERE path = $2",
            &[
                Value::Text(after_path.to_string()),
                Value::Text(before_path.to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(renamed.rows_affected(), 1);
    assert_eq!(read_file(&lix, before_path).await.unwrap(), None);
    assert_eq!(
        read_file(&lix, after_path).await.unwrap(),
        Some(initial.clone())
    );

    let stale_error = stale
        .execute(
            "UPDATE lix_file SET content = $1 WHERE id = $2",
            &[
                Value::Blob(b"first,STALE\nsecond,two\n".to_vec().into()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .expect_err("the old-path observation must fail closed after actor rekey");
    assert_eq!(stale_error.code, LixError::CODE_PLUGIN_OBSERVATION_STALE);

    // The rename session received the post-commit observation under the new
    // key and can immediately perform the next warm blob update.
    let edited = b"first,ONE\nsecond,two\n".to_vec();
    write_file(&renamer, after_path, edited.clone())
        .await
        .unwrap();
    assert_eq!(
        read_file(&lix, after_path).await.unwrap(),
        Some(edited.clone())
    );

    // Moving outside the plugin's matcher removes semantic state/ownership
    // while retaining the exact validated materialized blob as a raw file.
    let unselected = renamer
        .execute(
            "UPDATE lix_file SET path = $1 WHERE path = $2",
            &[
                Value::Text(raw_path.to_string()),
                Value::Text(after_path.to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(unselected.rows_affected(), 1);
    assert_eq!(read_file(&lix, after_path).await.unwrap(), None);
    assert_eq!(read_file(&lix, raw_path).await.unwrap(), Some(edited));
    let active_table_rows = lix
        .execute(
            "SELECT lixcol_file_id FROM csv_table WHERE lixcol_file_id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    let active_plugin_rows = lix
        .execute(
            "SELECT lixcol_file_id FROM csv_row WHERE lixcol_file_id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(active_table_rows.len() + active_plugin_rows.len(), 0);
    let active_owner_rows = lix
        .execute(
            "SELECT key FROM lix_key_value \
             WHERE lixcol_file_id = $1 AND key = 'lix_plugin_owner_v2'",
            &[Value::Text(file_id)],
        )
        .await
        .unwrap();
    assert_eq!(active_owner_rows.len(), 0);

    stale.close().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn transaction_lix_file_content_uses_session_plugin_runtime() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix().await.unwrap();

    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &archive,
        &["csv_table", "csv_row"],
    )
    .await;
    let csv = b"name,age\nAda,37\nGrace,85\n".to_vec();
    write_file(&lix, "/tx-plugin.csv", csv.clone())
        .await
        .unwrap();
    let file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text("/tx-plugin.csv".to_string())],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();

    let mut tx = lix.begin_transaction().await.unwrap();
    let files = tx
        .execute(
            "SELECT content FROM lix_file WHERE id = $1",
            &[Value::Text(file_id)],
        )
        .await
        .unwrap();

    assert_eq!(files.len(), 1);
    assert_eq!(files.rows()[0].values(), &[Value::Blob(csv.into())]);

    tx.rollback().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn filesystem_materializes_internal_lix_plugin_paths() {
    let tempdir = tempfile::tempdir().unwrap();
    let lix = open_filesystem_lix(tempdir.path()).await;
    let archive = build_csv_plugin_archive();

    install_plugin(&lix, "plugin_csv", &archive).await.unwrap();

    wait_for_disk_file(
        &tempdir.path().join(".lix/plugins/plugin_csv.lixplugin"),
        Some(archive.as_slice()),
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn filesystem_imports_lix_plugin_archives_from_disk() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive = build_csv_plugin_archive();
    let plugin_path = tempdir.path().join(".lix/plugins/plugin_csv.lixplugin");
    std::fs::create_dir_all(plugin_path.parent().unwrap()).unwrap();
    std::fs::write(&plugin_path, &archive).unwrap();

    let lix = open_filesystem_lix(tempdir.path()).await;

    let plugins = list_installed_plugins(&lix).await;
    assert_eq!(plugins.len(), 1);
    assert_eq!(plugins[0].key, "plugin_csv");
    assert_eq!(
        read_file(&lix, "/.lix/plugins/plugin_csv.lixplugin")
            .await
            .unwrap()
            .as_deref(),
        Some(archive.as_slice())
    );
    lix.close().await.unwrap();
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CsvV2Row {
    id: String,
    order_key: String,
    cells: Vec<String>,
}

async fn file_id_at_path<StorageImpl>(lix: &Lix<StorageImpl>, path: &str) -> String
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 1, "expected one file at {path}");
    result.rows()[0].get::<String>("id").unwrap()
}

async fn active_csv_rows<StorageImpl>(lix: &Lix<StorageImpl>, file_id: &str) -> Vec<CsvV2Row>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let rows = lix
        .execute(
            "SELECT lixcol_row_pk, id, order_key, cells FROM csv_row \
             WHERE lixcol_file_id = $1",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .unwrap();
    let mut rows = rows
        .rows()
        .iter()
        .map(|row| {
            let row_pk = row
                .get::<serde_json::Value>("lixcol_row_pk")
                .unwrap()
                .as_array()
                .cloned()
                .expect("csv_row row_pk must be an array");
            let id = row.get::<String>("id").unwrap();
            assert_eq!(
                row_pk,
                vec![serde_json::Value::String(id.clone())],
                "csv_row typed identity must equal its durable primary key"
            );
            CsvV2Row {
                id,
                order_key: row.get::<String>("order_key").unwrap(),
                cells: row
                    .get::<serde_json::Value>("cells")
                    .unwrap()
                    .as_array()
                    .expect("csv_row typed row must have cells")
                    .iter()
                    .map(|cell| {
                        cell.as_str()
                            .expect("csv_row cells must be strings")
                            .to_string()
                    })
                    .collect(),
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.order_key.cmp(&right.order_key));
    rows
}

/// Returns the exact durable tuple used by the merge planner to canonically
/// order two competing live versions of one semantic CSV row.
async fn csv_row_ordering<StorageImpl>(
    lix: &Lix<StorageImpl>,
    file_id: &str,
    row_id: &str,
) -> (String, String)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT lixcol_updated_at, lixcol_change_id FROM csv_row \
             WHERE lixcol_file_id = $1 AND id = $2",
            &[
                Value::Text(file_id.to_owned()),
                Value::Text(row_id.to_owned()),
            ],
        )
        .await
        .expect("CSV row version should query");
    assert_eq!(result.len(), 1, "expected one CSV row version");
    (
        result.rows()[0]
            .get::<String>("lixcol_updated_at")
            .expect("CSV row must have an update timestamp"),
        result.rows()[0]
            .get::<String>("lixcol_change_id")
            .expect("CSV row must have a change id"),
    )
}

/// Returns the exact durable tuple used by the merge planner to canonically
/// order two competing live versions of one Excalidraw element.
async fn excalidraw_v2_element_ordering<StorageImpl>(
    lix: &Lix<StorageImpl>,
    file_id: &str,
    element_id: &str,
) -> (String, String)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT lixcol_updated_at, lixcol_change_id FROM excalidraw_element WHERE lixcol_file_id = $1 AND id = $2",
            &[
                Value::Text(file_id.to_owned()),
                Value::Text(element_id.to_owned()),
            ],
        )
        .await
        .expect("Excalidraw element version should query");
    assert_eq!(result.len(), 1, "expected one Excalidraw element version");
    (
        result.rows()[0]
            .get::<String>("lixcol_updated_at")
            .expect("Excalidraw element must have an update timestamp"),
        result.rows()[0]
            .get::<String>("lixcol_change_id")
            .expect("Excalidraw element must have a change id"),
    )
}

fn csv_row_ids(rows: &[CsvV2Row], cells: &[&str]) -> Vec<String> {
    let mut ids = rows
        .iter()
        .filter(|row| {
            row.cells
                .iter()
                .map(String::as_str)
                .eq(cells.iter().copied())
        })
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn csv_row_id(rows: &[CsvV2Row], cells: &[&str]) -> String {
    let ids = csv_row_ids(rows, cells);
    assert_eq!(ids.len(), 1, "expected one csv_row with cells {cells:?}");
    ids[0].clone()
}

struct SyncedFilesystemLix {
    lix: Lix<FilesystemStorage>,
    _storage: FilesystemStorage,
}

impl Deref for SyncedFilesystemLix {
    type Target = Lix<FilesystemStorage>;

    fn deref(&self) -> &Self::Target {
        &self.lix
    }
}

async fn open_filesystem_lix(path: &Path) -> SyncedFilesystemLix {
    let storage = FilesystemStorage::new(path).open().unwrap();
    let lix = open_lix().with_storage(storage.clone()).await.unwrap();
    storage.start_sync(&lix).await.unwrap();
    SyncedFilesystemLix {
        lix,
        _storage: storage,
    }
}

async fn open_rocksdb_lix(path: &Path) -> Lix<RocksDB> {
    let storage = RocksDB::open(path.join(".lix")).expect("open Lix RocksDB storage");
    open_lix()
        .with_storage(storage)
        .await
        .expect("open Lix workspace")
}

async fn open_slatedb_lix(path: &Path) -> Lix<SlateDB> {
    let storage = SlateDB::open(path.join(".lix")).expect("open Lix SlateDB storage");
    open_lix()
        .with_storage(storage)
        .await
        .expect("open Lix workspace")
}

async fn qualify_lix_owned_sql_write_semantics<S>(lix: &Lix<S>, prefix: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
        &[Value::Text(
            r#"{"$schema":"https://lix.dev/schema-v1.json","key":"write_owner_task","columns":[{"name":"id","type":"uuid","nullable":false,"default_expression":"uuidv7()"},{"name":"title","type":"text","nullable":false}],"primary_key":["id"]}"#.to_string(),
        )],
    )
    .await
    .expect("register generated-default write-owner schema");
    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB)), (CAST($2 AS JSONB))",
        &[
            Value::Text(
                r#"{"$schema":"https://lix.dev/schema-v1.json","key":"write_owner_parent","columns":[{"name":"id","type":"text","nullable":false}],"primary_key":["id"]}"#.to_string(),
            ),
            Value::Text(
                r#"{"$schema":"https://lix.dev/schema-v1.json","key":"write_owner_child","columns":[{"name":"id","type":"text","nullable":false},{"name":"parent_id","type":"text","nullable":false}],"primary_key":["id"],"foreign_keys":[{"columns":["parent_id"],"references":{"schema_key":"write_owner_parent","columns":["id"]}}]}"#.to_string(),
            ),
        ],
    )
    .await
    .expect("register foreign-key write-owner schemas");

    let inserted = lix
        .execute(
            "INSERT INTO write_owner_task (title) VALUES ($1) RETURNING id, title",
            &[Value::Text(format!("{prefix}-inserted"))],
        )
        .await
        .expect("INSERT RETURNING with generated default");
    assert_eq!(inserted.rows_affected(), 1);
    let id = inserted.rows()[0]
        .get::<String>("id")
        .expect("generated RETURNING id");

    let updated = lix
        .execute(
            "UPDATE write_owner_task SET title = $1 WHERE id = $2 RETURNING id, title",
            &[
                Value::Text(format!("{prefix}-updated")),
                Value::Text(id.clone()),
            ],
        )
        .await
        .expect("UPDATE RETURNING");
    assert_eq!(updated.rows_affected(), 1);

    let upserted = lix
        .execute(
            "INSERT INTO write_owner_task (id, title) VALUES ($1, $2) \
             ON CONFLICT (id) DO UPDATE SET title = excluded.title RETURNING id, title",
            &[
                Value::Text(id.clone()),
                Value::Text(format!("{prefix}-upserted")),
            ],
        )
        .await
        .expect("ON CONFLICT RETURNING");
    assert_eq!(upserted.rows_affected(), 1);
    assert_eq!(
        upserted.rows()[0].get::<String>("title").unwrap(),
        format!("{prefix}-upserted")
    );

    let deleted = lix
        .execute(
            "DELETE FROM write_owner_task WHERE id = $1 RETURNING id, title",
            &[Value::Text(id)],
        )
        .await
        .expect("DELETE RETURNING");
    assert_eq!(deleted.rows_affected(), 1);
    assert_eq!(deleted.rows().len(), 1);

    let fk_error = lix
        .execute(
            "INSERT INTO write_owner_child (id, parent_id) VALUES ($1, $2)",
            &[
                Value::Text(format!("{prefix}-child")),
                Value::Text(format!("{prefix}-missing-parent")),
            ],
        )
        .await
        .expect_err("missing foreign-key owner must fail");
    assert_eq!(fk_error.code, LixError::CODE_FOREIGN_KEY);

    let batch_key = format!("{prefix}-batch");
    let batch = lix
        .execute_batch(&[
            ExecuteBatchStatement {
                label: Some("write".to_string()),
                sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2) RETURNING key, value"
                    .to_string(),
                params: vec![
                    Value::Text(batch_key.clone()),
                    Value::Text("one".to_string()),
                ],
            },
            ExecuteBatchStatement {
                label: Some("upsert".to_string()),
                sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2) \
                      ON CONFLICT (key) DO UPDATE SET value = excluded.value RETURNING key, value"
                    .to_string(),
                params: vec![
                    Value::Text(batch_key.clone()),
                    Value::Text("two".to_string()),
                ],
            },
        ])
        .await
        .expect("execute_batch write semantics");
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].statement_index(), Some(0));
    assert_eq!(batch[0].label(), Some("write"));
    assert_eq!(batch[1].statement_index(), Some(1));
    assert_eq!(batch[1].label(), Some("upsert"));

    let rollback_key = format!("{prefix}-rollback");
    let mut rollback = lix.begin_transaction().await.expect("begin rollback tx");
    rollback
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, 'rollback') RETURNING key",
            &[Value::Text(rollback_key.clone())],
        )
        .await
        .expect("stage rollback RETURNING");
    rollback
        .rollback()
        .await
        .expect("rollback write transaction");
    assert!(
        lix.execute(
            "SELECT key FROM lix_key_value WHERE key = $1",
            &[Value::Text(rollback_key)],
        )
        .await
        .unwrap()
        .is_empty()
    );
}

async fn qualify_stale_sql_write_owner<S>(stale_lix: &Lix<S>, winner_lix: &Lix<S>, prefix: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let stale_key = format!("{prefix}-stale");
    let mut stale = stale_lix.begin_transaction().await.expect("begin stale tx");
    stale
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, 'stale')",
            &[Value::Text(stale_key.clone())],
        )
        .await
        .expect("stage stale owner");
    winner_lix
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, 'winner')",
            &[Value::Text(stale_key.clone())],
        )
        .await
        .expect("publish winner owner");
    stale
        .commit()
        .await
        .expect("same-row stale inserts resolve by deterministic row-existence LWW");
    let result = stale_lix
        .execute(
            "SELECT value FROM lix_key_value WHERE key = $1",
            &[Value::Text(stale_key)],
        )
        .await
        .expect("read reconciled owner");
    assert_eq!(result.len(), 1);
    let value = result.rows()[0].get::<serde_json::Value>("value").unwrap();
    assert!(value == "stale" || value == "winner");
}

#[tokio::test]
async fn lix_owned_sql_write_semantics_rocksdb_reopen() {
    let root = tempfile::tempdir().expect("create SQL write-owner RocksDB directory");
    let lix = open_rocksdb_lix(root.path()).await;
    qualify_lix_owned_sql_write_semantics(&lix, "rocks").await;
    let winner = lix
        .open_another_session()
        .await
        .expect("open winner RocksDB session");
    qualify_stale_sql_write_owner(&lix, &winner, "rocks").await;
    winner.close().await.expect("close winner RocksDB handle");
    lix.close().await.expect("close SQL write-owner RocksDB");
    let reopened = open_rocksdb_lix(root.path()).await;
    let result = reopened
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'rocks-batch'",
            &[],
        )
        .await
        .expect("read SQL write-owner RocksDB after reopen");
    assert_eq!(
        result.rows()[0].get::<serde_json::Value>("value").unwrap(),
        serde_json::json!("two")
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn lix_owned_sql_write_semantics_slatedb_reopen() {
    let root = tempfile::tempdir().expect("create SQL write-owner SlateDB directory");
    let storage = SlateDB::open(root.path().join(".lix")).expect("open SQL write-owner SlateDB");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open SQL write-owner workspace");
    qualify_lix_owned_sql_write_semantics(&lix, "slate").await;
    let winner = lix
        .open_another_session()
        .await
        .expect("open winner SlateDB session");
    qualify_stale_sql_write_owner(&lix, &winner, "slate").await;
    winner.close().await.expect("close winner SlateDB handle");
    storage
        .flush()
        .await
        .expect("flush SQL write-owner SlateDB before reopen");
    lix.close().await.expect("close SQL write-owner SlateDB");
    let reopened = open_slatedb_lix(root.path()).await;
    let result = reopened
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'slate-batch'",
            &[],
        )
        .await
        .expect("read SQL write-owner SlateDB after reopen");
    assert_eq!(
        result.rows()[0].get::<serde_json::Value>("value").unwrap(),
        serde_json::json!("two")
    );
    reopened.close().await.unwrap();
}

fn p50_ms(sorted: &[f64]) -> f64 {
    sorted[sorted.len() / 2]
}

fn p95_ms(sorted: &[f64]) -> f64 {
    let index = ((sorted.len() * 95).div_ceil(100)).saturating_sub(1);
    sorted[index]
}

#[derive(Clone, Copy, Debug)]
struct BenchmarkMedians {
    elapsed_ms: f64,
    allocated_bytes: u64,
    peak_live_bytes: u64,
}

fn benchmark_medians(measurements: &[BenchmarkMeasurement]) -> BenchmarkMedians {
    assert!(!measurements.is_empty());
    let mut elapsed = measurements
        .iter()
        .map(|measurement| measurement.elapsed_ms)
        .collect::<Vec<_>>();
    elapsed.sort_by(f64::total_cmp);
    let mut allocated = measurements
        .iter()
        .map(|measurement| measurement.allocations.allocated_bytes)
        .collect::<Vec<_>>();
    allocated.sort_unstable();
    let mut peak = measurements
        .iter()
        .map(|measurement| measurement.allocations.peak_live_bytes_delta)
        .collect::<Vec<_>>();
    peak.sort_unstable();
    BenchmarkMedians {
        elapsed_ms: elapsed[elapsed.len() / 2],
        allocated_bytes: allocated[allocated.len() / 2],
        peak_live_bytes: peak[peak.len() / 2],
    }
}

fn assert_candidate_benchmark_win(
    benchmark: &str,
    baseline: BenchmarkMedians,
    candidate: BenchmarkMedians,
) {
    assert!(
        candidate.elapsed_ms < baseline.elapsed_ms * 0.9,
        "{benchmark}: candidate must be at least 10% faster; baseline={:.3}ms candidate={:.3}ms",
        baseline.elapsed_ms,
        candidate.elapsed_ms,
    );
    assert!(
        u128::from(candidate.allocated_bytes) * 100 <= u128::from(baseline.allocated_bytes) * 105,
        "{benchmark}: candidate cumulative host allocation regressed by more than 5%; \
         baseline={} candidate={}",
        baseline.allocated_bytes,
        candidate.allocated_bytes,
    );
    assert!(
        u128::from(candidate.peak_live_bytes) * 100 <= u128::from(baseline.peak_live_bytes) * 105,
        "{benchmark}: candidate peak live host allocation regressed by more than 5%; \
         baseline={} candidate={}",
        baseline.peak_live_bytes,
        candidate.peak_live_bytes,
    );
}

async fn install_plugin<StorageImpl>(
    lix: &Lix<StorageImpl>,
    key: &str,
    archive: &[u8],
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    write_file(
        lix,
        &format!("/.lix/plugins/{key}.lixplugin"),
        archive.to_vec(),
    )
    .await
}

async fn install_reference_plugin_in_blank_registry<StorageImpl>(
    lix: &Lix<StorageImpl>,
    key: &str,
    archive: &[u8],
    expected_schema_keys: &[&str],
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    assert!(
        list_installed_plugins(lix).await.is_empty(),
        "reference v2 tests must select the plugin from a blank registry"
    );
    install_plugin(lix, key, archive).await.unwrap();
    assert_eq!(
        list_installed_plugins(lix).await,
        vec![InstalledPluginInfo {
            key: key.to_owned(),
            schema_keys: expected_schema_keys
                .iter()
                .map(|schema_key| (*schema_key).to_owned())
                .collect(),
        }]
    );
}

async fn write_file<StorageImpl>(
    lix: &Lix<StorageImpl>,
    path: &str,
    data: Vec<u8>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        &[Value::Text(path.to_string()), Value::Blob(data.into())],
    )
    .await?;
    Ok(())
}

async fn read_file<StorageImpl>(
    lix: &Lix<StorageImpl>,
    path: &str,
) -> Result<Option<Vec<u8>>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    result
        .rows()
        .first()
        .map(|row| row.get::<Vec<u8>>("content"))
        .transpose()
}

async fn assert_query_count<StorageImpl>(
    lix: &Lix<StorageImpl>,
    sql: &str,
    params: &[Value],
    expected: i64,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(sql, params)
        .await
        .expect("count query should execute");
    assert_eq!(
        result.rows()[0]
            .get::<i64>("count")
            .expect("count query should return an integer"),
        expected
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InstalledPluginInfo {
    key: String,
    schema_keys: Vec<String>,
}

async fn list_installed_plugins<StorageImpl>(lix: &Lix<StorageImpl>) -> Vec<InstalledPluginInfo>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let archives = lix
        .execute("SELECT path, content FROM lix_file ORDER BY path", &[])
        .await
        .unwrap();
    archives
        .rows()
        .iter()
        .filter_map(|row| {
            let path = row.get::<String>("path").unwrap();
            if !path.starts_with("/.lix/plugins/") || !path.ends_with(".lixplugin") {
                return None;
            }
            Some(plugin_info_from_archive(
                row.get::<Vec<u8>>("content").unwrap(),
            ))
        })
        .collect()
}

fn plugin_info_from_archive(archive_bytes: Vec<u8>) -> InstalledPluginInfo {
    let mut archive = zip::ZipArchive::new(Cursor::new(archive_bytes)).unwrap();
    let mut manifest_json = String::new();
    archive
        .by_name("manifest.json")
        .unwrap()
        .read_to_string(&mut manifest_json)
        .unwrap();
    let manifest: serde_json::Value = serde_json::from_str(&manifest_json).unwrap();
    let key = manifest["key"].as_str().unwrap().to_string();
    let schema_paths = manifest["schemas"].as_array().unwrap();
    let mut schema_keys = Vec::with_capacity(schema_paths.len());
    for schema_path in schema_paths {
        let mut schema_json = String::new();
        archive
            .by_name(schema_path.as_str().unwrap())
            .unwrap()
            .read_to_string(&mut schema_json)
            .unwrap();
        let schema: serde_json::Value = serde_json::from_str(&schema_json).unwrap();
        schema_keys.push(schema["key"].as_str().unwrap().to_string());
    }
    InstalledPluginInfo { key, schema_keys }
}

fn wait_for_disk_file(path: &Path, expected: Option<&[u8]>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let path_display = path.display();
    loop {
        let actual = std::fs::read(path).ok();
        if actual.as_deref() == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for disk file {path_display} to be {expected:?}, got {actual:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn build_csv_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv"));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built CSV v3 wasm at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/csv/manifest.json").as_bytes(),
        ),
        (
            "schema/csv_table.json",
            include_str!("../../../plugins/csv/schema/csv_table.json").as_bytes(),
        ),
        (
            "schema/csv_row.json",
            include_str!("../../../plugins/csv/schema/csv_row.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn build_json_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_plugin_json"));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built JSON v3 wasm at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/json/manifest.json").as_bytes(),
        ),
        (
            "schema/json_root.json",
            include_str!("../../../plugins/json/schema/json_root.json").as_bytes(),
        ),
        (
            "schema/json_object_member.json",
            include_str!("../../../plugins/json/schema/json_object_member.json").as_bytes(),
        ),
        (
            "schema/json_array_item.json",
            include_str!("../../../plugins/json/schema/json_array_item.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn build_markdown_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_plugin_markdown"));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built Markdown v3 wasm at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/markdown/manifest.json").as_bytes(),
        ),
        (
            "schema/markdown_node.json",
            include_str!("../../../plugins/markdown/schema/markdown_node.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn build_text_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_TEXT_plugin_text"));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built text v3 wasm at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/text/manifest.json").as_bytes(),
        ),
        (
            "schema/text_line.json",
            include_str!("../../../plugins/text/schema/text_line.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn init_perf_tracing() {
    if std::env::var_os("LIX_PLUGIN_V2_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("lix_perf=debug")
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .with_test_writer()
            .try_init();
    }
}

const JSON_TEN_MIB_BYTES: usize = 10 * 1024 * 1024;
const JSON_TEN_MIB_PROPERTY_COUNT: usize = 39_870;
const JSON_V2_GUEST_MEMORY_LIMIT_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Clone, Debug)]
struct NativeJsonControlMember {
    key: String,
    order_key: String,
    scalar_json: serde_json::Value,
}

/// A prebuilt public-SQL statement. SQL text and bound values are assembled
/// before timing so the control measures planning, row staging, and commit;
/// it does not charge the direct lane for serializing the caller's rows.
#[derive(Debug)]
struct NativeJsonControlStatement {
    sql: String,
    params: Vec<Value>,
}

async fn register_native_json_control_schemas<StorageImpl>(lix: &Lix<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    // Deliberately register only the public JSON schema surfaces. The direct
    // control must exercise normal SQL/transaction/RocksDB work without
    // installing a component that would reconcile or render the rows.
    let mut transaction = lix
        .begin_transaction()
        .await
        .expect("open JSON control schema transaction");
    for schema in [
        include_str!("../../../plugins/json/schema/json_root.json"),
        include_str!("../../../plugins/json/schema/json_object_member.json"),
        include_str!("../../../plugins/json/schema/json_array_item.json"),
    ] {
        let inserted = transaction
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (CAST($1 AS JSONB), false, false)",
                &[Value::Text(schema.to_owned())],
            )
            .await
            .expect("register JSON control schema");
        assert_eq!(inserted.rows_affected(), 1);
    }
    transaction
        .commit()
        .await
        .expect("commit JSON control schemas");
}

async fn register_native_csv_control_schemas<StorageImpl>(lix: &Lix<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut transaction = lix
        .begin_transaction()
        .await
        .expect("open CSV control schema transaction");
    for schema in [
        include_str!("../../../plugins/csv/schema/csv_table.json"),
        include_str!("../../../plugins/csv/schema/csv_row.json"),
    ] {
        assert_eq!(
            transaction
                .execute(
                    "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                     VALUES (CAST($1 AS JSONB), false, false)",
                    &[Value::Text(schema.to_owned())],
                )
                .await
                .expect("register CSV control schema")
                .rows_affected(),
            1
        );
    }
    transaction
        .commit()
        .await
        .expect("commit CSV control schemas");
}

fn native_csv_control_table_insert(file_id: &str) -> NativeJsonControlStatement {
    NativeJsonControlStatement {
        sql: "INSERT INTO csv_table (id, dialect, lixcol_file_id) VALUES ('root', $1, $2)"
            .to_owned(),
        params: vec![
            Value::Jsonb(
                serde_json::json!({
                    "delimiter": ",",
                    "quote": "\"",
                    "terminator": "\n",
                })
                .into(),
            ),
            Value::Text(file_id.to_owned()),
        ],
    }
}

fn native_csv_control_row_insert_chunks(
    file_id: &str,
    row_count: usize,
    chunk_rows: usize,
) -> Vec<NativeJsonControlStatement> {
    const LONG_ROW_COUNT: usize = 120_000;
    assert!(chunk_rows > 0, "native CSV SQL chunk size must be positive");
    let denominator = u128::try_from(row_count + 1).expect("CSV row count fits u128");
    (0..row_count)
        .collect::<Vec<_>>()
        .chunks(chunk_rows)
        .map(|chunk| {
            let mut params = Vec::with_capacity(chunk.len() * 4);
            let values = chunk
                .iter()
                .enumerate()
                .map(|(offset, index)| {
                    let first = offset * 4 + 1;
                    let numerator = u128::try_from(*index + 1).expect("CSV row index fits u128")
                        * u128::from(u64::MAX);
                    let order_rank = u64::try_from(numerator / denominator)
                        .expect("CSV order rank fits u64")
                        | 1;
                    params.push(Value::Text(format!("019a0000-0000-7000-8000-{index:012x}")));
                    params.push(Value::Text(format!("{order_rank:016x}")));
                    params.push(Value::Jsonb(
                        serde_json::json!([
                            if *index < LONG_ROW_COUNT {
                                "000000000000000"
                            } else {
                                "00000000000000"
                            },
                            "1111111111",
                            "2222222222",
                            "3333333333",
                        ])
                        .into(),
                    ));
                    params.push(Value::Text(file_id.to_owned()));
                    format!("(${first}, ${}, ${}, ${})", first + 1, first + 2, first + 3)
                })
                .collect::<Vec<_>>()
                .join(",");
            NativeJsonControlStatement {
                sql: format!(
                    "INSERT INTO csv_row (id, order_key, cells, lixcol_file_id) VALUES {values}"
                ),
                params,
            }
        })
        .collect()
}

fn native_json_control_members(source: &[u8]) -> Vec<NativeJsonControlMember> {
    let object = serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(source)
        .expect("flat JSON fixture must parse as an object");
    assert_eq!(object.len(), JSON_TEN_MIB_PROPERTY_COUNT);
    let denominator =
        u128::try_from(JSON_TEN_MIB_PROPERTY_COUNT + 1).expect("property count fits u128");

    (0..JSON_TEN_MIB_PROPERTY_COUNT)
        .map(|index| {
            let key = format!("property_{index:06}");
            let scalar = object
                .get(&key)
                .and_then(serde_json::Value::as_str)
                .expect("flat JSON fixture values must be strings");
            let scalar_json = serde_json::json!(scalar);
            let numerator =
                u128::try_from(index + 1).expect("property index fits u128") * u128::from(u64::MAX);
            let order_rank =
                u64::try_from(numerator / denominator).expect("fractional rank fits u64") | 1;
            NativeJsonControlMember {
                key,
                order_key: format!("{order_rank:016x}"),
                scalar_json,
            }
        })
        .collect()
}

fn native_json_control_root_insert(file_id: Option<&str>) -> NativeJsonControlStatement {
    match file_id {
        None => NativeJsonControlStatement {
            sql: "INSERT INTO json_root (id, kind) VALUES ($1, $2)".to_owned(),
            params: vec![
                Value::Text("root".to_owned()),
                Value::Text("object".to_owned()),
            ],
        },
        Some(file_id) => NativeJsonControlStatement {
            sql: "INSERT INTO json_root (id, kind, lixcol_file_id) VALUES ($1, $2, $3)".to_owned(),
            params: vec![
                Value::Text("root".to_owned()),
                Value::Text("object".to_owned()),
                Value::Text(file_id.to_owned()),
            ],
        },
    }
}

fn native_json_control_member_insert_chunks(
    members: &[NativeJsonControlMember],
    file_id: Option<&str>,
    chunk_rows: usize,
) -> Vec<NativeJsonControlStatement> {
    assert!(
        chunk_rows > 0,
        "native JSON SQL chunk size must be positive"
    );
    members
        .chunks(chunk_rows)
        .map(|chunk| {
            let columns = match file_id {
                None => String::from(
                    "INSERT INTO json_object_member (parent_id, key, order_key, kind, scalar_json) VALUES ",
                ),
                Some(_) => String::from(
                    "INSERT INTO json_object_member (parent_id, key, order_key, kind, scalar_json, lixcol_file_id) VALUES ",
                ),
            };
            let params_per_member = if file_id.is_some() { 4 } else { 3 };
            let mut params = Vec::with_capacity(chunk.len() * params_per_member);
            let values = chunk
                .iter()
                .enumerate()
                .map(|(index, member)| {
                    let first = index * params_per_member + 1;
                    params.push(Value::Text(member.key.clone()));
                    params.push(Value::Text(member.order_key.clone()));
                    params.push(Value::Jsonb(member.scalar_json.clone().into()));
                    match file_id {
                        None => {
                            format!("('root', ${first}, ${}, 'string', ${})", first + 1, first + 2)
                        }
                        Some(file_id) => {
                            params.push(Value::Text(file_id.to_owned()));
                            format!(
                                "('root', ${first}, ${}, 'string', ${}, ${})",
                                first + 1,
                                first + 2,
                                first + 3
                            )
                        }
                    }
                })
                .collect::<Vec<_>>()
                .join(",");
            NativeJsonControlStatement {
                sql: format!("{columns}{values}"),
                params,
            }
        })
        .collect()
}

fn csv_ten_mib_fixture() -> Vec<u8> {
    const LONG_ROW_COUNT: usize = 120_000;
    const SHORT_ROW_COUNT: usize = 100_000;
    const LONG_ROW: &[u8] = b"000000000000000,1111111111,2222222222,3333333333\n";
    const SHORT_ROW: &[u8] = b"00000000000000,1111111111,2222222222,3333333333\n";

    let expected_len = LONG_ROW_COUNT * LONG_ROW.len() + SHORT_ROW_COUNT * SHORT_ROW.len();
    let mut bytes = Vec::with_capacity(expected_len);
    for _ in 0..LONG_ROW_COUNT {
        bytes.extend_from_slice(LONG_ROW);
    }
    for _ in 0..SHORT_ROW_COUNT {
        bytes.extend_from_slice(SHORT_ROW);
    }
    assert_eq!(bytes.len(), 10_680_000);
    bytes
}

fn json_ten_mib_flat_fixture() -> (Vec<u8>, usize, String) {
    const BASE_MEMBER_BYTES: usize = 44;
    let base_bytes =
        2 + JSON_TEN_MIB_PROPERTY_COUNT * BASE_MEMBER_BYTES + JSON_TEN_MIB_PROPERTY_COUNT - 1;
    let padding = JSON_TEN_MIB_BYTES
        .checked_sub(base_bytes)
        .expect("10 MiB target should accommodate the fixed JSON members");
    let padding_per_property = padding / JSON_TEN_MIB_PROPERTY_COUNT;
    let extra_padding_properties = padding % JSON_TEN_MIB_PROPERTY_COUNT;

    let mut bytes = Vec::with_capacity(JSON_TEN_MIB_BYTES);
    let mut state = 0x6a73_6f6e_2d31_306du64;
    let edited_index = JSON_TEN_MIB_PROPERTY_COUNT / 2;
    let edited_key = format!("property_{edited_index:06}");
    let mut edit_offset = None;
    bytes.push(b'{');
    for index in 0..JSON_TEN_MIB_PROPERTY_COUNT {
        if index > 0 {
            bytes.push(b',');
        }
        state = splitmix64(state);
        let first = state;
        state = splitmix64(state);
        let second = u32::try_from(state & u64::from(u32::MAX)).expect("masked value fits u32");
        write!(
            &mut bytes,
            "\"property_{index:06}\":\"{first:016x}{second:08x}"
        )
        .expect("write deterministic JSON property");
        if index == edited_index {
            edit_offset = Some(bytes.len() - 24);
        }
        let property_padding = padding_per_property + usize::from(index < extra_padding_properties);
        bytes.extend(std::iter::repeat_n(b'f', property_padding));
        bytes.push(b'"');
    }
    bytes.push(b'}');
    assert_eq!(bytes.len(), JSON_TEN_MIB_BYTES);
    (
        bytes,
        edit_offset.expect("middle property should have an edit offset"),
        edited_key,
    )
}

fn alternate_ascii_hex(byte: u8) -> u8 {
    if byte == b'0' { b'1' } else { b'0' }
}

fn splitmix64(mut state: u64) -> u64 {
    state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn sha256_lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn json_scalar_at_offset(bytes: &[u8], offset: usize) -> String {
    let start = bytes[..offset]
        .iter()
        .rposition(|byte| *byte == b'"')
        .expect("edited JSON scalar should have an opening quote");
    let end = offset
        + bytes[offset..]
            .iter()
            .position(|byte| *byte == b'"')
            .expect("edited JSON scalar should have a closing quote");
    std::str::from_utf8(&bytes[start..=end])
        .expect("fixture scalar should be UTF-8")
        .to_owned()
}

fn build_excalidraw_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!(
        "CARGO_CDYLIB_FILE_PLUGIN_EXCALIDRAW_plugin_excalidraw"
    ));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built Excalidraw v3 plugin wasm at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/excalidraw/manifest.json").as_bytes(),
        ),
        (
            "schema/excalidraw_scene.json",
            include_str!("../../../plugins/excalidraw/schema/excalidraw_scene.json").as_bytes(),
        ),
        (
            "schema/excalidraw_element.json",
            include_str!("../../../plugins/excalidraw/schema/excalidraw_element.json").as_bytes(),
        ),
        (
            "schema/excalidraw_file.json",
            include_str!("../../../plugins/excalidraw/schema/excalidraw_file.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn build_csv_plugin_archive_variant(
    wasm: &[u8],
    csv_row_schema: &[u8],
    generation_marker: Option<&[u8]>,
) -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/csv/manifest.json").as_bytes(),
        ),
        (
            "schema/csv_table.json",
            include_str!("../../../plugins/csv/schema/csv_table.json").as_bytes(),
        ),
        ("schema/csv_row.json", csv_row_schema),
        ("plugin.wasm", wasm),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    if let Some(marker) = generation_marker {
        writer.start_file("generation.txt", options).unwrap();
        writer.write_all(marker).unwrap();
    }
    writer.finish().unwrap().into_inner()
}
