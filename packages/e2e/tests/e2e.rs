mod benchmark_metrics;

use bytes::Bytes;
use lix::storage::{
    BeginScanOptions, CoreProjection, Key, KeyRange, ProjectedValue, PutBatch, PutEntry,
    ReadOptions, Storage, StorageRead, StorageSpace, StorageWrite, StoredValue, WriteOptions,
};
use lix::storage_adapter::{
    StorageAdapter, StorageKey, StorageReadOptions, StorageValue, StorageWriteOptions,
};
use lix::storage_bench::{layout_space_catalog, space_inventory};
use lix::plugin::runtime::{
    WasmByteSource, WasmColdFileUpdate, WasmComponentActor, WasmComponentFactory,
    WasmCreateContext, WasmEntity, WasmEntityChange, WasmEntityKey, WasmEntityPage,
    WasmEntitySource, WasmFileDescriptor, WasmFileTransition, WasmFileUpdate, WasmHostBytes,
    WasmHostEntity, WasmInputBytes, WasmInputSplice, WasmLimits, WasmOpenEntitiesInput,
    WasmPluginSelection, WasmRuntime, WasmSourceRange, WasmSourceSlice, WasmTransitionCounters,
    WasmTransitionLimits,
};
use lix::{
    CreateBranchOptions, ExecuteBatchStatement, ExecuteOptions, ExecuteStatementMetadata, Lix,
    LixError, MergeBranchOptions, MergeBranchPreviewOptions, MergeConflictChangeKind,
    MutationIdentity, RequestBlobSpliceProvenance, SwitchBranchOptions, VerifiedRequestBlob,
};
use lix::{Value, open_lix};
use lix_storage_filesystem::LocalFilesystem;
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::io::{Cursor, Read, Write};
use std::ops::Bound;
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
        if metadata.target() == "lix_perf" && is_import_perf_span(metadata.name()) {
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
            Value::Json(serde_json::json!(["row", "second"]).into()),
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
             FROM lix_file_history($1) \
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
        member.rows()[0].get::<String>("scalar_json").unwrap(),
        r#""plugin""#
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
/// the untracked arm produced no entity rows at all — an untracked JSON file
/// was a descriptor plus a content blob whose contents were unqueryable.
///
/// The untracked arm's row shape is the one #1346 established: a real
/// `change_id` (identity) with a NULL `commit_id` (no history). The change id
/// is asserted as a property, never as a literal — its value is a function of
/// UUID draw order.
#[tokio::test]
async fn untracked_json_file_produces_the_same_plugin_entity_rows_as_a_tracked_one() {
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
                "expected exactly one plugin entity row for file '{file_id}', got {}",
                result.len()
            );
        };
        row.values().to_vec()
    };

    // Tracked arm: the pre-existing behaviour, kept beside the untracked arm as
    // the reference the untracked arm has to match.
    let [tracked_scalar, tracked_untracked, tracked_change_id, tracked_commit_id] =
        member_row(TRACKED_FILE_ID).await.try_into().unwrap_or_else(|_| {
            panic!("expected four projected columns for the tracked plugin entity row")
        });
    assert_eq!(tracked_scalar, Value::Text(r#""plugin""#.to_owned()));
    assert_eq!(tracked_untracked, Value::Boolean(false));
    assert!(
        matches!(&tracked_change_id, Value::Text(value)
            if uuid::Uuid::parse_str(value).is_ok_and(|parsed| !parsed.is_nil())),
        "tracked plugin entity rows must carry a real change id, got {tracked_change_id:?}"
    );
    assert!(
        matches!(&tracked_commit_id, Value::Text(value) if !value.is_empty()),
        "tracked plugin entity rows enter the commit graph, got {tracked_commit_id:?}"
    );

    // Untracked arm: the same plugin, the same bytes, the same projection.
    let [untracked_scalar, untracked_untracked, untracked_change_id, untracked_commit_id] =
        member_row(UNTRACKED_FILE_ID)
            .await
            .try_into()
            .unwrap_or_else(|_| {
                panic!("expected four projected columns for the untracked plugin entity row")
            });
    assert_eq!(
        untracked_scalar, tracked_scalar,
        "the same JSON must parse to the same entity value irrespective of lane"
    );
    assert_eq!(
        untracked_untracked,
        Value::Boolean(true),
        "entity rows inherit their file's lane"
    );
    assert!(
        matches!(&untracked_change_id, Value::Text(value)
            if uuid::Uuid::parse_str(value).is_ok_and(|parsed| !parsed.is_nil())),
        "untracked plugin entity rows are identity-bearing, got {untracked_change_id:?}"
    );
    assert_eq!(
        untracked_commit_id,
        Value::Null,
        "untracked plugin entity rows must stay outside the commit graph"
    );

    // Editing an entity row must round-trip back into the file's bytes on both
    // lanes. This probes the read-path boundary deliberately left tracked-only
    // in `sql2/providers/file.rs`: if an untracked file's content depended on
    // being re-rendered from entities through that owner lookup, it would fail
    // here rather than silently later.
    for (file_id, path) in [
        (TRACKED_FILE_ID, TRACKED_PATH),
        (UNTRACKED_FILE_ID, UNTRACKED_PATH),
    ] {
        lix.execute(
            "UPDATE json_object_member SET scalar_json = '\"edited\"' \
             WHERE parent_id = 'root' AND key = 'alpha' AND lixcol_file_id = $1",
            &[Value::Text(file_id.to_owned())],
        )
        .await
        .unwrap_or_else(|error| panic!("entity edit on '{path}' should commit: {error:?}"));
        assert_eq!(
            read_file(&lix, path).await.unwrap(),
            Some(br#"{"alpha":"edited"}"#.to_vec()),
            "an entity edit must re-render '{path}' irrespective of lane"
        );
    }

    lix.close()
        .await
        .expect("lane-parity workspace should close");
}

/// Foreign-key equivalence across lanes on the ordinary decode path.
///
/// A tracked complete parse is retained as a certified packet, whose foreign
/// keys are checked *within the batch* (`validate_certified_snapshot_packets`).
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
/// that this is harmless is that `open_file` only runs when no prior entity
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
/// exactly as before: bytes in, bytes out, no entity rows, no plugin involved.
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

    let first = lix.open_session().await.unwrap();
    let second = lix.open_session().await.unwrap();
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
    first.reset_plugin_transition_counters();
    write_file(&first, path, first_edit).await.unwrap();
    let counters = first.plugin_transition_counters();
    assert_eq!(counters.full_state_semantic_rows_materialized, 0);
    assert_eq!(counters.durable_semantic_changes, 1);
    assert_eq!(counters.private_document_cache_hits, 1);
    // This session still edits its exact accepted observation, so the
    // validated submitted bytes are already the authoritative successor. The
    // shared renderer is needed only when replaying a historical sparse delta
    // onto a newer accepted document.
    assert_eq!(counters.shared_renderer_cache_hits, 0);
    write_file(&second, path, second_edit).await.unwrap();

    let composed = b"first,ONE\nsecond,TWO\nthird,three\n".to_vec();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(composed.clone()));

    // Both sessions observed the same row version. Transaction commit order is
    // the deterministic LWW tiebreaker for their edits to that row.
    let lww_first = lix.open_session().await.unwrap();
    let lww_second = lix.open_session().await.unwrap();
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
    let edit_session = lix.open_session().await.unwrap();
    let delete_session = lix.open_session().await.unwrap();
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
    let blind = lix.open_session().await.unwrap();
    write_file(&blind, path, b"first,ONE\n".to_vec())
        .await
        .unwrap();
    let one_row = b"first,ONE\n".to_vec();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(one_row.clone()));

    // A rolled-back successor is discarded; the accepted actor and its exact
    // observation remain usable for a later committed transition.
    let rollback_session = lix.open_session().await.unwrap();
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

    let insert_session = lix.open_session().await.unwrap();
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

    let edit_session = lix.open_session().await.unwrap();
    let create_session = lix.open_session().await.unwrap();
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
async fn v2_transport_splice_provenance_is_bound_to_the_observed_file() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &archive,
        &["csv_table", "csv_row"],
    )
    .await;

    let path_a = "/splice-a.csv";
    let path_b = "/splice-b.csv";
    let before_a = b"alpha,one\n".to_vec();
    let after_a = b"alpha,ONE\n".to_vec();
    let before_b = b"bravo,two\n".to_vec();
    write_file(&lix, path_a, before_a.clone()).await.unwrap();
    write_file(&lix, path_b, before_b.clone()).await.unwrap();
    assert_eq!(
        read_file(&lix, path_a).await.unwrap(),
        Some(before_a.clone())
    );
    assert_eq!(read_file(&lix, path_b).await.unwrap(), Some(before_b));

    let file_a_id = file_id_at_path(&lix, path_a).await;
    let file_b_id = file_id_at_path(&lix, path_b).await;
    let after_a_blob = after_a.clone().into();
    let provenance_from_a = RequestBlobSpliceProvenance::new_validated(
        &before_a,
        &after_a_blob,
        // SHA-256("alpha,one\n") and SHA-256("alpha,ONE\n"), matching the
        // sidecar that a transport cache slot for file A would produce.
        "905915ed876fff69efeef0b434d9409a07cd94b5ad2d9739a985f254a34f1f5c",
        "eebb840e4dd5b3c48988125488ea7ee757710ea1a6ed4cd9edf7fdb5a1fe2ea5",
        6,
        1,
        b"ONE".to_vec(),
    )
    .unwrap();

    // Deliberately submit file A's reconstructed result to warm file B using
    // the same SQL shape and blob-parameter slot. The engine must reject A's
    // base proof for B and derive the complete B -> submitted-byte delta.
    lix.reset_plugin_transition_counters();
    lix.execute_with_options_and_metadata(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        &[Value::Text(path_b.to_owned()), Value::Blob(after_a_blob)],
        ExecuteOptions::default(),
        ExecuteStatementMetadata {
            parameter_blob_splices: vec![None, Some(provenance_from_a)],
            ..ExecuteStatementMetadata::default()
        },
    )
    .await
    .unwrap();
    let counters = lix.plugin_transition_counters();
    assert!(
        counters.host_full_diff_bytes_compared > 0,
        "cross-file provenance must use the safe full-diff fallback"
    );
    assert_eq!(
        read_file(&lix, path_b).await.unwrap(),
        Some(after_a.clone())
    );
    let expected_b_rows = active_csv_rows(&lix, &file_b_id).await;
    assert_eq!(expected_b_rows.len(), 1);
    assert_eq!(
        expected_b_rows[0].cells,
        vec!["alpha".to_owned(), "ONE".to_owned()]
    );

    // The provenance must neither mutate file A nor leave B's actor/durable
    // graph divergent. Evict B, then force a semantic cold reopen and compare
    // both its rendered bytes and durable rows.
    assert_eq!(read_file(&lix, path_a).await.unwrap(), Some(before_a));
    assert_eq!(
        active_csv_rows(&lix, &file_a_id).await[0].cells,
        vec!["alpha".to_owned(), "one".to_owned()]
    );
    for index in 0..12 {
        write_file(
            &lix,
            &format!("/splice-eviction-{index}.csv"),
            format!("eviction,{index}\n").into_bytes(),
        )
        .await
        .unwrap();
    }
    assert_eq!(read_file(&lix, path_b).await.unwrap(), Some(after_a));
    assert_eq!(active_csv_rows(&lix, &file_b_id).await, expected_b_rows);

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
            Value::Json(serde_json::json!(["much-longer", "x"]).into()),
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
            Value::Json(serde_json::json!(["old", "ONE"]).into()),
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
async fn v2_markdown_roundtrips_gfm_and_renders_one_direct_entity_edit() {
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
        .rows()[0]
            .get::<String>("payload_json")
            .unwrap()
            .contains("TAIL")
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
    lix.reset_plugin_transition_counters();
    write_file(&lix, path, before.clone()).await.unwrap();
    let open_counters = lix.plugin_transition_counters();
    assert_eq!(open_counters.guest_export_calls, 1);
    assert_eq!(open_counters.durable_semantic_changes, 3);
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
    lix.reset_plugin_transition_counters();
    write_file(&lix, path, after.clone()).await.unwrap();
    let successor_counters = lix.plugin_transition_counters();
    assert_eq!(successor_counters.guest_export_calls, 1);
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
        current.rows().iter().any(|row| {
            row.get::<String>("payload_json")
                .is_ok_and(|payload| payload.contains("a tail"))
        }),
        "the sparse successor must overlay the immutable opening segment"
    );
    let historical = lix
        .execute(
            "SELECT kind, payload_json FROM markdown_node_history() \
             WHERE lixcol_depth = 1 ORDER BY kind",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(historical.rows().len(), 3);
    assert!(
        historical.rows().iter().any(|row| {
            row.get::<String>("payload_json")
                .is_ok_and(|payload| payload.contains("bold"))
        }),
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
    reopened.reset_plugin_transition_counters();
    write_file(&reopened, path, after_reopen.clone())
        .await
        .unwrap();
    let counters = reopened.plugin_transition_counters();
    assert_eq!(counters.full_state_semantic_rows_materialized, 0);
    assert_eq!(counters.private_document_cache_hits, 1);
    assert_eq!(counters.full_document_reparses, 0);
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
        .find(|row| {
            row.get::<String>("payload_json")
                .is_ok_and(|payload| payload.contains("New paragraph."))
        })
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
        .find(|row| {
            row.get::<String>("payload_json")
                .is_ok_and(|payload| payload.contains("Edited after hydration."))
        })
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
            .get::<String>("format_json")
            .unwrap()
            .contains("lexical_fallback_base64"),
        "v3 semantic state must not duplicate accepted source bytes"
    );

    let after = String::from_utf8(before)
        .unwrap()
        .replacen("6/10", "7/9", 1)
        .into_bytes();
    lix.reset_plugin_transition_counters();
    write_file(&lix, path, after.clone()).await.unwrap();
    let counters = lix.plugin_transition_counters();
    assert_eq!(counters.guest_export_calls, 1);
    assert_eq!(
        counters.durable_semantic_changes, 1,
        "only the changed frontmatter entity should be durable"
    );
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
        .find(|row| {
            row.get::<String>("payload_json")
                .is_ok_and(|payload| payload.contains("Peer 12 has"))
        })
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
#[ignore = "exact VS Code Docs d5badf Markdown transition benchmark"]
async fn v3_markdown_vscode_api_exact_transition_benchmark() {
    const BENCHMARK: &str = "v3_markdown_vscode_api_exact_transition_benchmark";
    const PATH: &str = "/api/references/vscode-api.md";
    let samples = std::env::var("LIX_BENCH_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|samples| *samples > 0)
        .unwrap_or(3);
    let before_path = std::env::var("LIX_VSCODE_API_BEFORE")
        .unwrap_or_else(|_| "/tmp/vscode-api-before.md".to_owned());
    let after_path = std::env::var("LIX_VSCODE_API_AFTER")
        .unwrap_or_else(|_| "/tmp/vscode-api-after.md".to_owned());
    let before = std::fs::read(&before_path)
        .unwrap_or_else(|error| panic!("read VS Code before fixture {before_path}: {error}"));
    let after = std::fs::read(&after_path)
        .unwrap_or_else(|error| panic!("read VS Code after fixture {after_path}: {error}"));
    assert_eq!(before.len(), 1_237_841);
    assert_eq!(after.len(), 1_237_840);
    let collector = PerfSpanCollector::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(collector.clone()));
    let _dispatcher = tracing::dispatcher::set_default(&dispatch);

    let lanes = [(
        "v3_push_sink",
        "plugin_markdown",
        build_markdown_plugin_archive(),
    )];
    let mut expected_rows = None;
    for (label, plugin_key, archive) in lanes {
        if std::env::var("LIX_BENCH_LANE").is_ok_and(|lane| lane != label) {
            continue;
        }
        let mut measurements = Vec::with_capacity(samples);
        let mut elapsed_ms = Vec::with_capacity(samples);
        for sample in 0..samples {
            let root = tempfile::tempdir().expect("create VS Code Markdown benchmark directory");
            let lix = open_rocksdb_lix(root.path()).await;
            install_reference_plugin_in_blank_registry(
                &lix,
                plugin_key,
                &archive,
                &["markdown_node"],
            )
            .await;
            write_file(&lix, PATH, before.clone())
                .await
                .unwrap_or_else(|error| panic!("{label} opening import failed: {error}"));
            let before_ids = lix
                .execute("SELECT id FROM markdown_node", &[])
                .await
                .unwrap()
                .rows()
                .iter()
                .map(|row| row.get::<String>("id").unwrap())
                .collect::<std::collections::BTreeSet<_>>();

            lix.reset_plugin_transition_counters();
            collector.clear();
            let allocation_scope = AllocationScope::start();
            let started = Instant::now();
            write_file(&lix, PATH, after.clone())
                .await
                .unwrap_or_else(|error| panic!("{label} successor failed: {error}"));
            let measurement =
                BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
            let counters = lix.plugin_transition_counters();
            assert_eq!(read_file(&lix, PATH).await.unwrap(), Some(after.clone()));
            let rows = lix
                .execute("SELECT COUNT(*) AS count FROM markdown_node", &[])
                .await
                .unwrap()
                .rows()[0]
                .get::<i64>("count")
                .unwrap() as usize;
            let after_ids = lix
                .execute("SELECT id FROM markdown_node", &[])
                .await
                .unwrap()
                .rows()
                .iter()
                .map(|row| row.get::<String>("id").unwrap())
                .collect::<std::collections::BTreeSet<_>>();
            eprintln!(
                "vscode_markdown_identity lane={label} removed={:?} added={:?}",
                before_ids.difference(&after_ids).collect::<Vec<_>>(),
                after_ids.difference(&before_ids).collect::<Vec<_>>(),
            );
            if let Some(expected) = expected_rows {
                assert_eq!(rows, expected, "{label} semantic row count");
            } else {
                expected_rows = Some(rows);
            }
            assert_eq!(counters.guest_export_calls, 1);
            eprintln!(
                "vscode_markdown lane={label} sample={sample} elapsed_ms={:.3} \
                 allocations={} allocated_mb={:.3} peak_live_mb={:.3} \
                 guest_exports={} imports={} boundary_mb={:.3} guest_high_water_mb={:.3} \
                 semantic_changes={} full_rows_materialized={} rows={rows}",
                measurement.elapsed_ms,
                measurement.allocations.allocation_count,
                measurement.allocations.allocated_bytes as f64 / 1_000_000.0,
                measurement.allocations.peak_live_bytes_delta as f64 / 1_000_000.0,
                counters.guest_export_calls,
                counters.component_import_calls,
                counters.component_boundary_bytes as f64 / 1_000_000.0,
                counters.guest_linear_memory_high_water_bytes as f64 / 1_000_000.0,
                counters.durable_semantic_changes,
                counters.full_state_semantic_rows_materialized,
            );
            eprintln!(
                "vscode_markdown_phases lane={label} sample={sample} phases_ms={:?} \
                 phase_close_live_bytes={:?}",
                collector.take_aggregate_millis(),
                collector.take_close_live_bytes(),
            );
            let fixture = BenchmarkFixture {
                input_bytes: after.len(),
                logical_rows: rows,
            };
            emit_sample(
                BENCHMARK,
                label,
                sample,
                fixture,
                BenchmarkGate::BulkWrite,
                measurement,
            );
            emit_transition_profile(
                BENCHMARK,
                label,
                sample,
                counters,
                serde_json::json!({
                    "before_sha256": sha256_lower_hex(&before),
                    "file_sha256": sha256_lower_hex(&after),
                    "file_bytes": after.len(),
                    "entity_rows": rows,
                    "identity_set_sha256": sha256_lower_hex(
                        after_ids.iter().flat_map(|id| id.as_bytes().iter().copied()).collect::<Vec<_>>().as_slice()
                    )
                }),
            );
            elapsed_ms.push(measurement.elapsed_ms);
            measurements.push(measurement);
            lix.close().await.unwrap();
        }
        elapsed_ms.sort_by(f64::total_cmp);
        eprintln!(
            "vscode_markdown lane={label} raw_ms={elapsed_ms:?} p50_ms={:.3} p95_ms={:.3}",
            p50_ms(&elapsed_ms),
            p95_ms(&elapsed_ms)
        );
        emit_summary(
            BENCHMARK,
            label,
            BenchmarkFixture {
                input_bytes: after.len(),
                logical_rows: expected_rows.unwrap_or_default(),
            },
            BenchmarkGate::BulkWrite,
            &measurements,
        );
    }
}

#[tokio::test]
async fn v2_markdown_merges_unrelated_entities_and_regenerates_derived_bytes() {
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
            .find(|row| {
                row.get::<String>("payload_json")
                    .is_ok_and(|payload| payload.contains(needle))
            })
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

    lix.reset_plugin_transition_counters();
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
    let counters = lix.plugin_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, 1);
    assert_eq!(
        counters.conflict_resolution_takes, 0,
        "the composed paragraph is one replacement, not a side selection"
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

    lix.reset_plugin_transition_counters();
    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("v3 disjoint paragraph inserts should merge");
    assert_eq!(
        read_file(&lix, path).await.unwrap().as_deref(),
        Some(b"prewonderful\n".as_slice())
    );
    let counters = lix.plugin_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, 1);
    assert_eq!(counters.conflict_resolution_takes, 0);

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

    lix.reset_plugin_transition_counters();
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
    let counters = lix.plugin_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, 1);
    assert_eq!(
        counters.conflict_resolution_takes, 0,
        "the composed row is one replacement, not a side selection"
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

    lix.reset_plugin_transition_counters();
    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("distinct CSV cell edits should merge");
    let counters = lix.plugin_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, 1);
    assert_eq!(
        counters.conflict_resolution_takes, 0,
        "the composed row is one replacement, not a side selection"
    );
    assert_eq!(
        read_file(&lix, path).await.unwrap().as_deref(),
        Some(b"ALPHA,one,BLU\n".as_slice()),
        "both same-row cell edits must survive",
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_json_unrelated_entity_branch_merge_accepts_certified_snapshots() {
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
        "UPDATE json_object_member SET scalar_json = '\"target\"' \
         WHERE parent_id = 'root' AND key = 'left' AND lixcol_file_id = $1",
        &[Value::Text(file_id.clone())],
    )
    .await
    .expect("target JSON member should update");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    lix.execute(
        "UPDATE json_object_member SET scalar_json = '\"source\"' \
         WHERE parent_id = 'root' AND key = 'right' AND lixcol_file_id = $1",
        &[Value::Text(file_id.clone())],
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
        .expect("unrelated certified JSON rows should preview");
    assert!(preview.conflicts.is_empty());
    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("certified JSON rows must not be decoded while fingerprinting the merge batch");

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
        merged.rows()[0].get::<String>("scalar_json").unwrap(),
        r#""target""#
    );
    assert_eq!(
        merged.rows()[1].get::<String>("scalar_json").unwrap(),
        r#""source""#
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_json_same_entity_branch_merge_runs_static_resolver_on_certified_snapshots() {
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
        "UPDATE json_object_member SET scalar_json = '\"target\"' \
         WHERE parent_id = 'root' AND key = 'pick' AND lixcol_file_id = $1",
        &[Value::Text(file_id.clone())],
    )
    .await
    .unwrap();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    lix.execute(
        "UPDATE json_object_member SET scalar_json = '\"source\"' \
         WHERE parent_id = 'root' AND key = 'pick' AND lixcol_file_id = $1",
        &[Value::Text(file_id.clone())],
    )
    .await
    .unwrap();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id,
    })
    .await
    .unwrap();

    lix.reset_plugin_transition_counters();
    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("the JSON static resolver should accept certified snapshots");
    let counters = lix.plugin_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, 1);
    assert_eq!(counters.conflict_resolution_takes, 1);

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
        merged.rows()[0].get::<String>("scalar_json").unwrap(),
        r#""source""#
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v3_json_same_entity_branch_merge_uses_fused_conflict_and_renderer_sinks() {
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
        "UPDATE json_object_member SET scalar_json = '\"target\"' \
         WHERE parent_id = 'root' AND key = 'pick' AND lixcol_file_id = $1",
        &[Value::Text(file_id.clone())],
    )
    .await
    .unwrap();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    lix.execute(
        "UPDATE json_object_member SET scalar_json = '\"source\"' \
         WHERE parent_id = 'root' AND key = 'pick' AND lixcol_file_id = $1",
        &[Value::Text(file_id.clone())],
    )
    .await
    .unwrap();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id,
    })
    .await
    .unwrap();

    lix.reset_plugin_transition_counters();
    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("the v3 JSON resolver and renderer should complete atomically");
    let counters = lix.plugin_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, 1);
    assert_eq!(counters.conflict_resolution_takes, 1);

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
        merged.rows()[0].get::<String>("scalar_json").unwrap(),
        r#""source""#
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

    lix.reset_plugin_transition_counters();
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
    let counters = lix.plugin_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, 1);
    assert_eq!(
        counters.conflict_resolution_takes, 1,
        "same-cell canonical fallback should retain the selected host snapshot zero-copy"
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

    lix.reset_plugin_transition_counters();
    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("same-cell CSV conflict should resolve deterministically");
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(expected));
    let counters = lix.plugin_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, 1);
    assert_eq!(
        counters.conflict_resolution_takes, 1,
        "same-cell canonical fallback should retain the selected host snapshot zero-copy"
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_delete_vs_edit_remains_a_file_lifecycle_conflict() {
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
            Value::Json(serde_json::json!(["alpha", "ONE", "red"]).into()),
            Value::Text(row_id),
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
        .expect("lifecycle conflict should still preview");
    let lifecycle_conflict = preview
        .conflicts
        .iter()
        .find(|conflict| {
            conflict.schema_key == "lix_binary_blob_ref"
                && conflict.file_id.as_deref() == Some(file_id.as_str())
        })
        .expect("delete-vs-edit must remain visible as a file-lifecycle conflict");
    assert_eq!(
        lifecycle_conflict.target.kind,
        MergeConflictChangeKind::Removed
    );
    assert_eq!(
        lifecycle_conflict.source.kind,
        MergeConflictChangeKind::Modified
    );

    lix.reset_plugin_transition_counters();
    let error = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .expect_err("delete-vs-edit requires a first-class lifecycle decision");
    assert_eq!(error.code, LixError::CODE_MERGE_CONFLICT);
    let counters = lix.plugin_transition_counters();
    assert_eq!(
        counters.conflict_resolution_calls, 0,
        "the plugin must not resolve a divergent file lifetime"
    );
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        None,
        "a rejected lifecycle merge must not partially restore the file"
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_rename_vs_same_row_edit_remains_a_descriptor_conflict() {
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
            Value::Json(serde_json::json!(["TARGET", "one", "red"]).into()),
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
            Value::Json(serde_json::json!(["SOURCE", "one", "red"]).into()),
            Value::Text(row_id),
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
        .expect("descriptor-divergent merge should still preview");
    assert!(preview.conflicts.iter().any(|conflict| {
        conflict.schema_key == "csv_row" && conflict.file_id.as_deref() == Some(file_id.as_str())
    }));

    lix.reset_plugin_transition_counters();
    let error = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .expect_err("a resolver must not mix target CSV bytes with source TSV metadata");
    assert_eq!(error.code, LixError::CODE_MERGE_CONFLICT);
    let counters = lix.plugin_transition_counters();
    assert_eq!(
        counters.conflict_resolution_calls, 0,
        "the plugin must not resolve across divergent paths or descriptors"
    );
    assert_eq!(
        read_file(&lix, csv_path).await.unwrap(),
        Some(b"TARGET,one,red\n".to_vec()),
        "a rejected merge must preserve the target descriptor and bytes"
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
        after.rows()[0].get::<String>("scalar_json").unwrap(),
        r#""alpha""#
    );
    assert_eq!(after.rows()[0].get::<String>("id").unwrap(), alpha_id);
    assert_eq!(after.rows()[1].get::<String>("id").unwrap(), beta_id);

    lix.execute(
        "UPDATE json_array_item SET scalar_json = $1 \
         WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Text(r#""BETA""#.to_owned()),
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
    assert!(member.rows()[0].get::<String>("scalar_json").is_err());

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
    assert_eq!(members.rows()[1].get::<String>("scalar_json").unwrap(), "2");

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
        members.rows()[0].get::<String>("scalar_json").unwrap(),
        r#""Ada""#
    );

    let edited = String::from_utf8(source)
        .unwrap()
        .replacen(r#""Ada""#, r#""Lin""#, 1)
        .into_bytes();
    lix.reset_plugin_transition_counters();
    write_file(&lix, path, edited.clone()).await.unwrap();
    let counters = lix.plugin_transition_counters();
    assert_eq!(counters.full_state_semantic_rows_materialized, 0);
    assert_eq!(counters.durable_semantic_changes, 1);
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(edited.clone()));

    lix.execute(
        "UPDATE json_object_member SET scalar_json = $1 WHERE key = 'name'",
        &[Value::Text(r#""Grace""#.to_string())],
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
    let left_writer = lix.open_session().await.unwrap();
    let right_writer = lix.open_session().await.unwrap();
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
                Value::Text(r#""ONE-A""#.to_owned()),
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
                Value::Text(r#""TWO-B""#.to_owned()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .unwrap();
    let composed = b"{\"left\":\"ONE-A\",\"right\":\"TWO-B\",\"gone\":\"three\"}".to_vec();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(composed.clone()));

    // Commit order is the deterministic LWW tiebreaker for the same scalar.
    let first_lww = lix.open_session().await.unwrap();
    let second_lww = lix.open_session().await.unwrap();
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
                Value::Text(r#""LWW-A""#.to_owned()),
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
                Value::Text(r#""LWW-B""#.to_owned()),
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
            Value::Text(r#""AFTER-DIRECT-REJECT""#.to_owned()),
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
            Value::Text(r#""BULK""#.to_owned()),
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
    // recreate an entity after another writer removes its containing slot.
    let stale_writer = lix.open_session().await.unwrap();
    let structure_writer = lix.open_session().await.unwrap();
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
            Value::Text(r#""AFTER-FENCE""#.to_owned()),
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
#[ignore = "10 MiB end-to-end Wasm acceptance gate"]
async fn v2_json_ten_mib_real_wasm_edit_stays_sparse_and_bounded() {
    init_perf_tracing();
    let archive = build_json_plugin_archive();
    let lix = open_lix()
        .await
        .expect("workspace should open with the production Wasmtime runtime");
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/ten-mib.json";
    let (before, edit_offset, edited_key) = json_ten_mib_flat_fixture();
    let replacement = alternate_ascii_hex(before[edit_offset]);
    let mut after = before.clone();
    after[edit_offset] = replacement;

    lix.reset_plugin_transition_counters();
    let cold_started = Instant::now();
    write_file(&lix, path, before.clone())
        .await
        .expect("real JSON v2 Wasm should import the 10 MiB fixture");
    let cold_elapsed = cold_started.elapsed();
    let cold = lix.plugin_transition_counters();
    assert_eq!(
        cold.source_bytes_read, JSON_TEN_MIB_BYTES as u64,
        "cold hydration must stream the complete fixture through the Component boundary",
    );
    assert_eq!(cold.source_read_calls, 10);
    assert_eq!(cold.component_import_calls, 10);
    assert_eq!(
        cold.host_content_classification_bytes,
        JSON_TEN_MIB_BYTES as u64,
    );
    assert_eq!(
        cold.packet_records,
        (JSON_TEN_MIB_PROPERTY_COUNT + 1) as u64,
        "cold hydration must emit the root plus every top-level property",
    );
    assert_eq!(
        cold.durable_semantic_changes,
        (JSON_TEN_MIB_PROPERTY_COUNT + 1) as u64,
    );
    assert_eq!(cold.full_document_reparses, 1);
    assert_eq!(cold.full_state_semantic_rows_materialized, 0);
    assert!(
        (1..=JSON_V2_GUEST_MEMORY_LIMIT_BYTES).contains(&cold.guest_linear_memory_high_water_bytes),
        "cold guest high-water {} must remain within the configured 128 MiB actor limit",
        cold.guest_linear_memory_high_water_bytes,
    );

    let file_id = file_id_at_path(&lix, path).await;
    let cold_bytes = read_file(&lix, path)
        .await
        .expect("materialized JSON should read")
        .expect("materialized JSON should exist");
    assert_eq!(cold_bytes, before);

    // A full remote request admitted this base before the later splice arrives.
    // Its one-time full hash is deliberately outside the hot splice timing.
    let verified_base = VerifiedRequestBlob::verify(cold_bytes.clone().into());
    let after_sha256 = sha256_lower_hex(&after);
    let warm_request_started = Instant::now();
    let transport_started = Instant::now();
    let (verified_after, provenance) = verified_base
        .reconstruct_splice(
            verified_base.sha256(),
            &after_sha256,
            edit_offset,
            cold_bytes.len() - edit_offset - 1,
            [replacement].as_slice().into(),
        )
        .expect("the one-byte JSON transport splice should validate");
    let warm_transport_elapsed = transport_started.elapsed();
    let after_blob = verified_after.blob().clone();

    lix.reset_plugin_transition_counters();
    let warm_engine_started = Instant::now();
    lix.execute_with_options_and_metadata(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        &[Value::Text(path.to_owned()), Value::Blob(after_blob)],
        ExecuteOptions::default(),
        ExecuteStatementMetadata {
            parameter_blob_splices: vec![None, Some(provenance)],
            ..ExecuteStatementMetadata::default()
        },
    )
    .await
    .expect("one localized edit should pass through the real JSON v2 component");
    let warm_engine_elapsed = warm_engine_started.elapsed();
    let warm_request_elapsed = warm_request_started.elapsed();
    let warm = lix.plugin_transition_counters();

    assert_eq!(warm.host_full_diff_bytes_compared, 0);
    assert_eq!(warm.host_content_classification_bytes, 0);
    assert_eq!(warm.source_read_calls, 0);
    assert_eq!(warm.source_bytes_read, 0);
    assert_eq!(warm.component_import_calls, 0);
    assert_eq!(warm.full_state_semantic_rows_materialized, 0);
    assert_eq!(warm.packet_pages, 1);
    assert_eq!(warm.packet_records, 1);
    assert_eq!(warm.attachment_reads, 0);
    assert_eq!(warm.attachment_bytes_read, 0);
    assert_eq!(warm.durable_semantic_changes, 1);
    assert_eq!(warm.private_document_cache_hits, 1);
    assert_eq!(warm.full_document_reparses, 0);
    assert_eq!(warm.full_renderer_invocations, 0);
    assert_eq!(warm.shared_renderer_cache_hits, 0);
    assert_eq!(warm.filesystem_sync_full_renders, 0);
    assert!(
        warm.component_boundary_bytes < 64 * 1024,
        "one scalar edit crossed {} Component-boundary bytes",
        warm.component_boundary_bytes,
    );
    assert!(
        (1..=JSON_V2_GUEST_MEMORY_LIMIT_BYTES).contains(&warm.guest_linear_memory_high_water_bytes),
        "warm guest high-water {} must remain within the configured 128 MiB actor limit",
        warm.guest_linear_memory_high_water_bytes,
    );

    assert_eq!(
        read_file(&lix, path)
            .await
            .expect("edited materialized JSON should read"),
        Some(after.clone()),
    );
    let expected_scalar_json = json_scalar_at_offset(&after, edit_offset);
    let edited_member = lix
        .execute(
            "SELECT scalar_json FROM json_object_member \
             WHERE parent_id = 'root' AND key = $1 AND lixcol_file_id = $2",
            &[Value::Text(edited_key), Value::Text(file_id)],
        )
        .await
        .expect("the edited semantic member should query");
    assert_eq!(edited_member.len(), 1);
    assert_eq!(
        edited_member.rows()[0]
            .get::<String>("scalar_json")
            .expect("edited scalar_json should be text"),
        expected_scalar_json,
    );

    eprintln!(
        "v2_json_ten_mib bytes={} properties={} cold_ms={:.3} cold_guest_high_water_bytes={} \
         warm_request_ms={:.3} warm_transport_ms={:.3} warm_engine_transition_ms={:.3} warm_boundary_bytes={} \
         warm_guest_high_water_bytes={}",
        JSON_TEN_MIB_BYTES,
        JSON_TEN_MIB_PROPERTY_COUNT,
        cold_elapsed.as_secs_f64() * 1_000.0,
        cold.guest_linear_memory_high_water_bytes,
        warm_request_elapsed.as_secs_f64() * 1_000.0,
        warm_transport_elapsed.as_secs_f64() * 1_000.0,
        warm_engine_elapsed.as_secs_f64() * 1_000.0,
        warm.component_boundary_bytes,
        warm.guest_linear_memory_high_water_bytes,
    );

    lix.close().await.expect("workspace should close");
}

#[tokio::test]
#[ignore = "10 MiB ordinary public-SQL JSON byte-write benchmark"]
async fn v2_json_ten_mib_ordinary_sql_byte_edit_benchmark() {
    init_perf_tracing();
    const SAMPLES: usize = 7;
    const BENCHMARK: &str = "v2_json_ten_mib_ordinary_sql_byte_edit_benchmark";

    let root = tempfile::tempdir().expect("create JSON benchmark directory");
    let archive = build_json_plugin_archive();
    let lix = open_rocksdb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/ordinary-sql-ten-mib.json";
    let (mut bytes, edit_offset, _) = json_ten_mib_flat_fixture();
    write_file(&lix, path, bytes.clone())
        .await
        .expect("real JSON v2 Wasm should import the 10 MiB fixture");
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(bytes.clone()),
        "the initial read must acknowledge the exact materialized base",
    );

    let mut elapsed_ms = Vec::with_capacity(SAMPLES);
    let mut measurements = Vec::with_capacity(SAMPLES);
    let fixture = BenchmarkFixture {
        input_bytes: JSON_TEN_MIB_BYTES,
        logical_rows: 1,
    };
    for sample in 0..SAMPLES {
        bytes[edit_offset] = alternate_ascii_hex(bytes[edit_offset]);
        lix.reset_plugin_transition_counters();
        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        write_file(&lix, path, bytes.clone())
            .await
            .expect("ordinary SQL full-byte JSON edit should succeed");
        let measurement = BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
        elapsed_ms.push(measurement.elapsed_ms);
        measurements.push(measurement);
        emit_sample(
            BENCHMARK,
            "sparse_plugin_update",
            sample,
            fixture,
            BenchmarkGate::ElapsedRegression,
            measurement,
        );

        let counters = lix.plugin_transition_counters();
        assert_eq!(counters.packet_records, 1, "sample {sample}");
        assert_eq!(counters.durable_semantic_changes, 1, "sample {sample}");
        assert_eq!(counters.private_document_cache_hits, 1, "sample {sample}");
        assert_eq!(counters.full_document_reparses, 0, "sample {sample}");
        assert_eq!(counters.full_renderer_invocations, 0, "sample {sample}");
        assert!(
            counters.host_full_diff_bytes_compared >= JSON_TEN_MIB_BYTES as u64,
            "sample {sample} must exercise the ordinary full-byte fallback",
        );
    }
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(bytes),
        "ordinary SQL JSON edits must remain byte-exact",
    );

    elapsed_ms.sort_by(f64::total_cmp);
    let p50_ms = elapsed_ms[elapsed_ms.len() / 2];
    let p95_index = ((elapsed_ms.len() * 95).div_ceil(100)).saturating_sub(1);
    let p95_ms = elapsed_ms[p95_index];
    eprintln!(
        "v2_json_ordinary_sql_hot_edit bytes={JSON_TEN_MIB_BYTES} samples={SAMPLES} \
         p50_ms={p50_ms:.3} p95_ms={p95_ms:.3}"
    );
    emit_summary(
        BENCHMARK,
        "sparse_plugin_update",
        fixture,
        BenchmarkGate::ElapsedRegression,
        &measurements,
    );

    lix.close().await.expect("JSON benchmark should close");
}

#[tokio::test]
#[ignore = "10 MiB JSON unrelated-entity merge benchmark"]
async fn v2_json_ten_mib_unrelated_entity_merge_benchmark() {
    init_perf_tracing();
    const SAMPLES: usize = 7;
    const BENCHMARK: &str = "v2_json_ten_mib_unrelated_entity_merge_benchmark";

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
            "unrelated_entity",
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
        "v2_json_ten_mib_unrelated_entity_merge bytes={JSON_TEN_MIB_BYTES} samples={SAMPLES} \
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
        "unrelated_entity",
        fixture,
        BenchmarkGate::ElapsedRegression,
        &measurements,
    );

    lix.close().await.expect("JSON benchmark should close");
}

/// End-to-end RocksDB gate for a same-entity conflict over the same large
/// tracked tree as the adjacent unrelated-entity benchmark. The tiny built-in
/// control row keeps the frozen reference runnable even when its JSON plugin
/// merge path cannot fingerprint certified snapshots.
#[tokio::test]
#[ignore = "10 MiB JSON same-entity conflict-resolution merge benchmark"]
async fn v2_json_ten_mib_same_entity_canonical_b_merge_benchmark() {
    init_perf_tracing();
    const SAMPLES: usize = 7;
    const BENCHMARK: &str = "v2_json_ten_mib_same_entity_canonical_b_merge_benchmark";

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
            "same_entity_conflict",
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
        "v2_json_ten_mib_same_entity_canonical_b_merge bytes={JSON_TEN_MIB_BYTES} samples={SAMPLES} \
         raw_ms={raw_ms:?} p50_ms={:.3} p95_ms={:.3}",
        p50_ms(&elapsed_ms),
        p95_ms(&elapsed_ms),
    );
    emit_summary(
        BENCHMARK,
        "same_entity_conflict",
        fixture,
        BenchmarkGate::ElapsedRegression,
        &measurements,
    );

    lix.close()
        .await
        .expect("JSON conflict benchmark should close");
}

/// Compares an ordinary v2 file import with the same semantic rows supplied
/// directly through the public typed-SQL surface.
///
/// The no-file lane is the semantic-row durability floor. The file-scoped
/// lane is the meaningful parity comparison: it stages the same 10 MiB
/// `lix_file` payload, the root plus 39,870 member rows, and commits them in
/// one ordinary transaction. Parsing the fixture and constructing SQL happen
/// outside each timed lane, just as the caller has already constructed the
/// input blob before `write_file` begins.
#[tokio::test]
#[ignore = "10 MiB JSON plugin versus direct semantic-row import parity benchmark"]
async fn v2_json_ten_mib_rocksdb_import_parity_benchmark() {
    init_perf_tracing();
    const SAMPLES: usize = 7;
    const SQL_CHUNK_ROWS: usize = 500;
    const FILE_ID: &str = "01900000-0000-7000-8000-000000000701";
    const FILE_PATH: &str = "/native-json-semantic-control.json";
    const BENCHMARK: &str = "v2_json_ten_mib_rocksdb_import_parity_benchmark";
    const MUTATION_BENCHMARK: &str = "v2_json_ten_mib_bulk_sql_mutation_benchmark";

    let archive = build_json_plugin_archive();
    let (source, _, _) = json_ten_mib_flat_fixture();
    let members = native_json_control_members(&source);
    assert_eq!(members.len(), JSON_TEN_MIB_PROPERTY_COUNT);

    let no_file_root_statement = native_json_control_root_insert(None);
    let no_file_member_statements =
        native_json_control_member_insert_chunks(&members, None, SQL_CHUNK_ROWS);
    let file_scoped_root_statement = native_json_control_root_insert(Some(FILE_ID));
    let file_scoped_member_statements =
        native_json_control_member_insert_chunks(&members, Some(FILE_ID), SQL_CHUNK_ROWS);
    let bulk_update_params = [Value::Text(r#""batch-updated""#.to_owned())];

    let collector = PerfSpanCollector::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(collector.clone()));
    let _dispatcher = tracing::dispatcher::set_default(&dispatch);
    let mut plugin_ms = Vec::with_capacity(SAMPLES);
    let mut plugin_measurements = Vec::with_capacity(SAMPLES);
    let file_fixture = BenchmarkFixture {
        input_bytes: JSON_TEN_MIB_BYTES,
        logical_rows: JSON_TEN_MIB_PROPERTY_COUNT + 1,
    };
    let no_file_fixture = BenchmarkFixture {
        input_bytes: 0,
        logical_rows: JSON_TEN_MIB_PROPERTY_COUNT + 1,
    };
    for sample in 0..SAMPLES {
        let root = tempfile::tempdir().expect("create plugin import benchmark directory");
        let lix = open_rocksdb_lix(root.path()).await;
        install_reference_plugin_in_blank_registry(
            &lix,
            "plugin_json",
            &archive,
            &["json_root", "json_object_member", "json_array_item"],
        )
        .await;

        // Plugin installation and the caller's input clone are deliberately
        // outside the timer. The timed operation is one normal public file
        // write on an otherwise fresh RocksDB database.
        let input = source.clone();
        lix.reset_plugin_transition_counters();
        collector.clear();
        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        let inserted = lix
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
                &[
                    Value::Text(FILE_ID.to_owned()),
                    Value::Text(FILE_PATH.to_owned()),
                    Value::Blob(input.into()),
                ],
            )
            .await
            .expect("real JSON v2 plugin import should succeed");
        let measurement = BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
        assert_eq!(inserted.rows_affected(), 1, "plugin sample {sample}");
        let elapsed_ms = measurement.elapsed_ms;
        plugin_ms.push(elapsed_ms);
        plugin_measurements.push(measurement);
        emit_sample(
            BENCHMARK,
            "plugin",
            sample,
            file_fixture,
            BenchmarkGate::BulkWrite,
            measurement,
        );
        eprintln!(
            "v2_json_import_phases sample={sample} elapsed_ms={elapsed_ms:.3} phases_ms={:?}",
            collector.take_aggregate_millis()
        );

        let counters = lix.plugin_transition_counters();
        assert_eq!(
            counters.durable_semantic_changes,
            (JSON_TEN_MIB_PROPERTY_COUNT + 1) as u64,
            "plugin sample {sample} must commit one root plus every member"
        );
        assert_eq!(
            read_file(&lix, FILE_PATH)
                .await
                .expect("plugin file should read"),
            Some(source.clone()),
            "plugin sample {sample} must preserve exact materialized bytes"
        );
        lix.close().await.expect("close plugin import benchmark");
    }

    let mut direct_no_file_ms = Vec::with_capacity(SAMPLES);
    let mut direct_file_scoped_ms = Vec::with_capacity(SAMPLES);
    let mut direct_no_file_measurements = Vec::with_capacity(SAMPLES);
    let mut direct_file_scoped_measurements = Vec::with_capacity(SAMPLES);
    let mut bulk_update_measurements = Vec::with_capacity(SAMPLES);
    let mut bulk_delete_measurements = Vec::with_capacity(SAMPLES);
    let bulk_mutation_fixture = BenchmarkFixture {
        input_bytes: JSON_TEN_MIB_BYTES,
        logical_rows: JSON_TEN_MIB_PROPERTY_COUNT,
    };
    for (label, file_scoped, root_statement, member_statements, samples, measurements) in [
        (
            "direct_no_file",
            false,
            &no_file_root_statement,
            &no_file_member_statements,
            &mut direct_no_file_ms,
            &mut direct_no_file_measurements,
        ),
        (
            "direct_file_scoped",
            true,
            &file_scoped_root_statement,
            &file_scoped_member_statements,
            &mut direct_file_scoped_ms,
            &mut direct_file_scoped_measurements,
        ),
    ] {
        for sample in 0..SAMPLES {
            let root = tempfile::tempdir().expect("create direct import benchmark directory");
            let lix = open_rocksdb_lix(root.path()).await;
            register_native_json_control_schemas(&lix).await;

            // Both the full file payload and every exact semantic snapshot
            // have been prebuilt before timing. The transaction below stays
            // exclusively on the public typed entity surface.
            let file_input = file_scoped.then(|| source.clone());
            let allocation_scope = AllocationScope::start();
            let started = Instant::now();
            let mut transaction = lix
                .begin_transaction()
                .await
                .expect("open direct semantic-row transaction");
            if let Some(file_input) = file_input {
                let inserted = transaction
                    .execute(
                        "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
                        &[
                            Value::Text(FILE_ID.to_owned()),
                            Value::Text(FILE_PATH.to_owned()),
                            Value::Blob(file_input.into()),
                        ],
                    )
                    .await
                    .expect("stage direct file payload");
                assert_eq!(inserted.rows_affected(), 1, "{label} sample {sample}");
            }
            let inserted_root = transaction
                .execute(&root_statement.sql, &root_statement.params)
                .await
                .expect("stage direct root row");
            assert_eq!(inserted_root.rows_affected(), 1, "{label} sample {sample}");
            let mut inserted_members = 0_u64;
            for statement in member_statements {
                inserted_members += transaction
                    .execute(&statement.sql, &statement.params)
                    .await
                    .expect("stage direct member rows")
                    .rows_affected();
            }
            assert_eq!(
                inserted_members, JSON_TEN_MIB_PROPERTY_COUNT as u64,
                "{label} sample {sample}"
            );
            transaction
                .commit()
                .await
                .expect("commit direct semantic rows");
            let measurement =
                BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
            samples.push(measurement.elapsed_ms);
            measurements.push(measurement);
            emit_sample(
                BENCHMARK,
                label,
                sample,
                if file_scoped {
                    file_fixture
                } else {
                    no_file_fixture
                },
                BenchmarkGate::ElapsedRegression,
                measurement,
            );

            let member_count = lix
                .execute("SELECT COUNT(*) AS count FROM json_object_member", &[])
                .await
                .expect("count direct member rows")
                .rows()[0]
                .get::<i64>("count")
                .expect("member count must be an integer");
            assert_eq!(
                member_count, JSON_TEN_MIB_PROPERTY_COUNT as i64,
                "{label} sample {sample} must retain every member"
            );
            if !file_scoped {
                let allocation_scope = AllocationScope::start();
                let started = Instant::now();
                let updated = lix
                    .execute(
                        "UPDATE json_object_member SET scalar_json = $1",
                        &bulk_update_params,
                    )
                    .await
                    .expect("bulk-update direct JSON members");
                let measurement =
                    BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
                assert_eq!(
                    updated.rows_affected(),
                    JSON_TEN_MIB_PROPERTY_COUNT as u64,
                    "bulk update sample {sample}"
                );
                bulk_update_measurements.push(measurement);
                emit_sample(
                    MUTATION_BENCHMARK,
                    "bulk_update",
                    sample,
                    bulk_mutation_fixture,
                    BenchmarkGate::ElapsedRegression,
                    measurement,
                );
                let updated_count = lix
                    .execute(
                        "SELECT COUNT(*) AS count FROM json_object_member WHERE scalar_json = $1",
                        &bulk_update_params,
                    )
                    .await
                    .expect("verify bulk-updated direct JSON members")
                    .rows()[0]
                    .get::<i64>("count")
                    .expect("updated member count must be an integer");
                assert_eq!(
                    updated_count, JSON_TEN_MIB_PROPERTY_COUNT as i64,
                    "bulk update sample {sample} must retain every updated snapshot"
                );

                let allocation_scope = AllocationScope::start();
                let started = Instant::now();
                let deleted = lix
                    .execute("DELETE FROM json_object_member", &[])
                    .await
                    .expect("bulk-delete direct JSON members");
                let measurement =
                    BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
                assert_eq!(
                    deleted.rows_affected(),
                    JSON_TEN_MIB_PROPERTY_COUNT as u64,
                    "bulk delete sample {sample}"
                );
                bulk_delete_measurements.push(measurement);
                emit_sample(
                    MUTATION_BENCHMARK,
                    "bulk_delete",
                    sample,
                    bulk_mutation_fixture,
                    BenchmarkGate::ElapsedRegression,
                    measurement,
                );
                let remaining_count = lix
                    .execute("SELECT COUNT(*) AS count FROM json_object_member", &[])
                    .await
                    .expect("verify bulk-deleted direct JSON members")
                    .rows()[0]
                    .get::<i64>("count")
                    .expect("remaining member count must be an integer");
                assert_eq!(remaining_count, 0, "bulk delete sample {sample}");
            }
            if file_scoped {
                assert_eq!(
                    read_file(&lix, FILE_PATH)
                        .await
                        .expect("direct file should read"),
                    Some(source.clone()),
                    "{label} sample {sample} must retain the same payload"
                );
            }
            lix.close().await.expect("close direct import benchmark");
        }
    }
    emit_summary(
        MUTATION_BENCHMARK,
        "bulk_update",
        bulk_mutation_fixture,
        BenchmarkGate::ElapsedRegression,
        &bulk_update_measurements,
    );
    emit_summary(
        MUTATION_BENCHMARK,
        "bulk_delete",
        bulk_mutation_fixture,
        BenchmarkGate::ElapsedRegression,
        &bulk_delete_measurements,
    );

    for samples in [
        &mut plugin_ms,
        &mut direct_no_file_ms,
        &mut direct_file_scoped_ms,
    ] {
        samples.sort_by(f64::total_cmp);
    }
    let plugin_p50_ms = p50_ms(&plugin_ms);
    let direct_no_file_p50_ms = p50_ms(&direct_no_file_ms);
    let direct_file_scoped_p50_ms = p50_ms(&direct_file_scoped_ms);
    eprintln!(
        "v2_json_ten_mib_import_parity bytes={JSON_TEN_MIB_BYTES} rows={} samples={SAMPLES} \\
         plugin_raw_ms={plugin_ms:?} plugin_p50_ms={plugin_p50_ms:.3} plugin_p95_ms={:.3} \\
         direct_no_file_raw_ms={direct_no_file_ms:?} direct_no_file_p50_ms={direct_no_file_p50_ms:.3} \\
         direct_file_scoped_raw_ms={direct_file_scoped_ms:?} direct_file_scoped_p50_ms={direct_file_scoped_p50_ms:.3} \\
         plugin_to_direct_file_scoped_ratio={:.3}",
        JSON_TEN_MIB_PROPERTY_COUNT + 1,
        p95_ms(&plugin_ms),
        plugin_p50_ms / direct_file_scoped_p50_ms,
    );
    emit_summary(
        BENCHMARK,
        "plugin",
        file_fixture,
        BenchmarkGate::BulkWrite,
        &plugin_measurements,
    );
    emit_summary(
        BENCHMARK,
        "direct_no_file",
        no_file_fixture,
        BenchmarkGate::ElapsedRegression,
        &direct_no_file_measurements,
    );
    emit_summary(
        BENCHMARK,
        "direct_file_scoped",
        file_fixture,
        BenchmarkGate::ElapsedRegression,
        &direct_file_scoped_measurements,
    );
    assert!(
        plugin_p50_ms <= direct_file_scoped_p50_ms * 1.5,
        "10 MiB JSON plugin import p50 {plugin_p50_ms:.3} ms exceeds the 1.5x direct file-scoped semantic-row gate ({direct_file_scoped_p50_ms:.3} ms)"
    );
}

/// Compares the 10.68 MiB / 220,000-row CSV v2 file import with the same
/// table and row entities written directly through typed SQL. Each pair
/// alternates lane order so machine drift cannot systematically favor either
/// path. Plugin installation, schema registration, fixture construction, and
/// SQL construction remain outside the measured transaction.
#[tokio::test]
#[ignore = "10 MiB CSV plugin versus direct semantic-row import parity benchmark"]
async fn v2_csv_ten_mib_rocksdb_import_parity_benchmark() {
    const PAIRS: usize = 5;
    const SQL_CHUNK_ROWS: usize = 500;
    const CSV_ROW_COUNT: usize = 220_000;
    const FILE_ID: &str = "019a0000-0000-7000-8000-000000000220";
    const FILE_PATH: &str = "/native-csv-semantic-control.csv";
    const BENCHMARK: &str = "v2_csv_ten_mib_rocksdb_import_parity_benchmark";

    let archive = build_csv_plugin_archive();
    let source = csv_ten_mib_fixture();
    let table_statement = native_csv_control_table_insert(FILE_ID);
    let row_statements =
        native_csv_control_row_insert_chunks(FILE_ID, CSV_ROW_COUNT, SQL_CHUNK_ROWS);
    let collector = PerfSpanCollector::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(collector.clone()));
    let _dispatcher = tracing::dispatcher::set_default(&dispatch);
    let mut plugin_ms = Vec::with_capacity(PAIRS);
    let mut direct_ms = Vec::with_capacity(PAIRS);
    let mut paired_samples_ms = Vec::with_capacity(PAIRS);
    let mut paired_ratios = Vec::with_capacity(PAIRS);
    let mut plugin_measurements = Vec::with_capacity(PAIRS);
    let mut direct_measurements = Vec::with_capacity(PAIRS);
    let fixture = BenchmarkFixture {
        input_bytes: source.len(),
        logical_rows: CSV_ROW_COUNT + 1,
    };

    for sample in 0..PAIRS {
        let lanes = if sample % 2 == 0 {
            [true, false]
        } else {
            [false, true]
        };
        let mut plugin_sample_ms = None;
        let mut direct_sample_ms = None;
        for plugin_lane in lanes {
            let root = tempfile::tempdir().expect("create CSV import benchmark directory");
            let lix = open_rocksdb_lix(root.path()).await;
            if plugin_lane {
                install_reference_plugin_in_blank_registry(
                    &lix,
                    "plugin_csv",
                    &archive,
                    &["csv_table", "csv_row"],
                )
                .await;
            } else {
                register_native_csv_control_schemas(&lix).await;
            }

            let file_input = source.clone();
            lix.reset_plugin_transition_counters();
            collector.clear();
            let allocation_scope = AllocationScope::start();
            let started = Instant::now();
            if plugin_lane {
                let inserted = lix
                    .execute(
                        "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
                        &[
                            Value::Text(FILE_ID.to_owned()),
                            Value::Text(FILE_PATH.to_owned()),
                            Value::Blob(file_input.into()),
                        ],
                    )
                    .await
                    .expect("real CSV v2 plugin import should succeed");
                assert_eq!(inserted.rows_affected(), 1, "plugin sample {sample}");
            } else {
                let mut transaction = lix
                    .begin_transaction()
                    .await
                    .expect("open direct CSV semantic-row transaction");
                let inserted = transaction
                    .execute(
                        "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
                        &[
                            Value::Text(FILE_ID.to_owned()),
                            Value::Text(FILE_PATH.to_owned()),
                            Value::Blob(file_input.into()),
                        ],
                    )
                    .await
                    .expect("stage direct CSV file payload");
                assert_eq!(inserted.rows_affected(), 1, "direct sample {sample}");
                assert_eq!(
                    transaction
                        .execute(&table_statement.sql, &table_statement.params)
                        .await
                        .expect("stage direct CSV table row")
                        .rows_affected(),
                    1,
                    "direct sample {sample}"
                );
                let mut inserted_rows = 0_u64;
                for statement in &row_statements {
                    inserted_rows += transaction
                        .execute(&statement.sql, &statement.params)
                        .await
                        .expect("stage direct CSV rows")
                        .rows_affected();
                }
                assert_eq!(
                    inserted_rows, CSV_ROW_COUNT as u64,
                    "direct sample {sample}"
                );
                transaction
                    .commit()
                    .await
                    .expect("commit direct CSV semantic rows");
            }
            let measurement =
                BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
            let elapsed_ms = measurement.elapsed_ms;
            let phases_ms = collector.take_aggregate_millis();
            eprintln!(
                "v2_csv_import_phases sample={sample} lane={} elapsed_ms={elapsed_ms:.3} phases_ms={phases_ms:?}",
                if plugin_lane { "plugin" } else { "direct" },
            );

            if plugin_lane {
                assert_eq!(
                    lix.plugin_transition_counters().durable_semantic_changes,
                    (CSV_ROW_COUNT + 1) as u64,
                    "plugin sample {sample} must commit one table plus every row"
                );
                plugin_ms.push(elapsed_ms);
                plugin_measurements.push(measurement);
                plugin_sample_ms = Some(elapsed_ms);
                emit_sample(
                    BENCHMARK,
                    "plugin",
                    sample,
                    fixture,
                    BenchmarkGate::BulkWrite,
                    measurement,
                );
            } else {
                let row_count = lix
                    .execute("SELECT COUNT(*) AS count FROM csv_row", &[])
                    .await
                    .expect("count direct CSV rows")
                    .rows()[0]
                    .get::<i64>("count")
                    .expect("CSV row count must be an integer");
                assert_eq!(row_count, CSV_ROW_COUNT as i64, "direct sample {sample}");
                direct_ms.push(elapsed_ms);
                direct_measurements.push(measurement);
                direct_sample_ms = Some(elapsed_ms);
                emit_sample(
                    BENCHMARK,
                    "direct_file_scoped",
                    sample,
                    fixture,
                    BenchmarkGate::ElapsedRegression,
                    measurement,
                );
            }
            assert_eq!(
                read_file(&lix, FILE_PATH)
                    .await
                    .expect("CSV benchmark file should read"),
                Some(source.clone()),
                "sample {sample} must preserve exact CSV bytes"
            );
            lix.close().await.expect("close CSV import benchmark");
        }

        let plugin_sample_ms = plugin_sample_ms.expect("plugin lane must run once per pair");
        let direct_sample_ms = direct_sample_ms.expect("direct lane must run once per pair");
        paired_samples_ms.push((plugin_sample_ms, direct_sample_ms));
        paired_ratios.push(plugin_sample_ms / direct_sample_ms);
    }

    let mut plugin_sorted_ms = plugin_ms.clone();
    let mut direct_sorted_ms = direct_ms.clone();
    let mut paired_ratios_sorted = paired_ratios.clone();
    plugin_sorted_ms.sort_by(f64::total_cmp);
    direct_sorted_ms.sort_by(f64::total_cmp);
    paired_ratios_sorted.sort_by(f64::total_cmp);
    let plugin_p50_ms = p50_ms(&plugin_sorted_ms);
    let direct_p50_ms = p50_ms(&direct_sorted_ms);
    let paired_p50_ratio = p50_ms(&paired_ratios_sorted);
    eprintln!(
        "v2_csv_ten_mib_import_parity bytes={} rows={} pairs={PAIRS} \
         paired_plugin_direct_ms={paired_samples_ms:?} \
         plugin_raw_ms={plugin_ms:?} plugin_p50_ms={plugin_p50_ms:.3} \
         direct_raw_ms={direct_ms:?} direct_p50_ms={direct_p50_ms:.3} \
         aggregate_ratio={:.3} paired_ratios={paired_ratios:?} \
         paired_p50_ratio={paired_p50_ratio:.3}",
        source.len(),
        CSV_ROW_COUNT + 1,
        plugin_p50_ms / direct_p50_ms,
    );
    emit_summary(
        BENCHMARK,
        "plugin",
        fixture,
        BenchmarkGate::BulkWrite,
        &plugin_measurements,
    );
    emit_summary(
        BENCHMARK,
        "direct_file_scoped",
        fixture,
        BenchmarkGate::ElapsedRegression,
        &direct_measurements,
    );
    assert!(
        paired_p50_ratio <= 1.5,
        "10 MiB CSV plugin import paired p50 ratio {paired_p50_ratio:.3} exceeds the 1.5x direct semantic-row gate"
    );
}

/// Profiles CSV initial import through the universal entity output API.
#[tokio::test]
#[ignore = "10 MiB CSV universal entity import benchmark"]
async fn csv_ten_mib_universal_entity_benchmark() {
    const CSV_ROW_COUNT: usize = 220_000;
    const FILE_ID: &str = "019a0000-0000-7000-8000-000000000320";
    const FILE_PATH: &str = "/v3-typed-batch.csv";
    const BENCHMARK: &str = "csv_ten_mib_universal_entity_benchmark";

    let samples = std::env::var("LIX_BENCH_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|samples| *samples > 0)
        .unwrap_or(5);
    let archive = build_csv_plugin_archive();
    let source = csv_ten_mib_fixture();
    let fixture = BenchmarkFixture {
        input_bytes: source.len(),
        logical_rows: CSV_ROW_COUNT + 1,
    };
    let collector = PerfSpanCollector::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(collector.clone()));
    let _dispatcher = tracing::dispatcher::set_default(&dispatch);
    let mut measurements = Vec::with_capacity(samples);
    let mut elapsed_ms = Vec::with_capacity(samples);

    for sample in 0..samples {
        let root = tempfile::tempdir().expect("create universal CSV benchmark directory");
        let lix = open_rocksdb_lix(root.path()).await;
        install_reference_plugin_in_blank_registry(
            &lix,
            "plugin_csv",
            &archive,
            &["csv_table", "csv_row"],
        )
        .await;

        lix.reset_plugin_transition_counters();
        collector.clear();
        let input = source.clone();
        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        let inserted = lix
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
                &[
                    Value::Text(FILE_ID.to_owned()),
                    Value::Text(FILE_PATH.to_owned()),
                    Value::Blob(input.into()),
                ],
            )
            .await
            .expect("universal CSV import should succeed");
        let measurement = BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
        assert_eq!(inserted.rows_affected(), 1, "v3 sample {sample}");

        let counters = lix.plugin_transition_counters();
        assert_eq!(
            counters.guest_export_calls, 1,
            "universal CSV import must enter the guest exactly once"
        );
        assert_eq!(
            counters.actor_executor_threads_created, 0,
            "universal CSV import must not create actor executor threads"
        );
        assert_eq!(
            counters.packet_records,
            (CSV_ROW_COUNT + 1) as u64,
            "universal CSV import must retain exact row cardinality"
        );
        assert_eq!(
            counters.durable_semantic_changes,
            (CSV_ROW_COUNT + 1) as u64,
            "universal CSV import must commit the exact semantic row count"
        );
        assert!(
            counters.packet_pages > 1,
            "the large fixture must exercise bounded push pages"
        );
        assert_eq!(
            read_file(&lix, FILE_PATH)
                .await
                .expect("universal CSV file should read"),
            Some(source.clone()),
            "universal CSV import must preserve exact file bytes"
        );
        let row_count = lix
            .execute("SELECT COUNT(*) AS count FROM csv_row", &[])
            .await
            .expect("universal CSV semantic rows should query")
            .rows()[0]
            .get::<i64>("count")
            .expect("CSV row count must be an integer");
        assert_eq!(row_count, CSV_ROW_COUNT as i64);
        let projected = lix
            .execute(
                "SELECT lixcol_entity_pk, id, order_key, cells \
                 FROM csv_row WHERE lixcol_file_id = $1 LIMIT 1",
                &[Value::Text(FILE_ID.to_owned())],
            )
            .await
            .expect("v3 certified CSV row should project after commit");
        assert_eq!(projected.len(), 1);
        let projected = &projected.rows()[0];
        let id = projected
            .get::<String>("id")
            .expect("certified CSV id should project");
        assert_eq!(
            projected
                .get::<serde_json::Value>("lixcol_entity_pk")
                .expect("certified CSV primary key should project"),
            serde_json::json!([id]),
        );
        assert_eq!(
            projected
                .get::<serde_json::Value>("cells")
                .expect("certified CSV cells should project"),
            serde_json::json!(["000000000000000", "1111111111", "2222222222", "3333333333"]),
        );

        let phase_close_live_bytes = collector.take_close_live_bytes();
        eprintln!(
            "csv_universal_entity_phases sample={sample} elapsed_ms={:.3} \
             guest_export_calls={} actor_executor_threads_created={} \
             source_sink_import_calls={} packet_pages={} packet_records={} \
             boundary_bytes={} guest_high_water_bytes={} phase_close_live_bytes={:?} phases_ms={:?}",
            measurement.elapsed_ms,
            counters.guest_export_calls,
            counters.actor_executor_threads_created,
            counters.component_import_calls,
            counters.packet_pages,
            counters.packet_records,
            counters.component_boundary_bytes,
            counters.guest_linear_memory_high_water_bytes,
            phase_close_live_bytes,
            collector.take_aggregate_millis(),
        );
        emit_sample(
            BENCHMARK,
            "universal_entities",
            sample,
            fixture,
            BenchmarkGate::BulkWrite,
            measurement,
        );
        elapsed_ms.push(measurement.elapsed_ms);
        measurements.push(measurement);
        lix.close().await.expect("close universal CSV benchmark");
        let reopened = open_rocksdb_lix(root.path()).await;
        let reopened_count = reopened
            .execute("SELECT COUNT(*) AS count FROM csv_row", &[])
            .await
            .expect("reopened certified CSV rows should query")
            .rows()[0]
            .get::<i64>("count")
            .expect("reopened CSV row count must be an integer");
        assert_eq!(reopened_count, CSV_ROW_COUNT as i64);
        emit_transition_profile(
            BENCHMARK,
            "universal_entities",
            sample,
            counters,
            serde_json::json!({
                "file_sha256": sha256_lower_hex(&source),
                "file_bytes": source.len(),
                "entity_rows": CSV_ROW_COUNT + 1,
                "reopen_verified": true
            }),
        );
        reopened
            .close()
            .await
            .expect("close reopened universal CSV benchmark");
    }

    elapsed_ms.sort_by(f64::total_cmp);
    eprintln!(
        "v3_csv_ten_mib_typed_batch bytes={} rows={} samples={samples} \
         raw_ms={elapsed_ms:?} p50_ms={:.3} p95_ms={:.3}",
        source.len(),
        CSV_ROW_COUNT + 1,
        p50_ms(&elapsed_ms),
        p95_ms(&elapsed_ms),
    );
    emit_summary(
        BENCHMARK,
        "universal_entities",
        fixture,
        BenchmarkGate::BulkWrite,
        &measurements,
    );
}

/// Large JSON import through the current host-imported push sink.
///
/// The removed v2 runtime must be benchmarked from its frozen revision; using
/// the current archive for both labels would only compare v3 against itself.
#[tokio::test]
#[ignore = "10 MiB JSON v3 push-sink import benchmark"]
async fn v3_json_ten_mib_push_sink_benchmark() {
    const FILE_ID: &str = "019a0000-0000-7000-8000-000000000330";
    const FILE_PATH: &str = "/v3-json-large.json";
    const BENCHMARK: &str = "v3_json_ten_mib_push_sink_benchmark";

    let samples = std::env::var("LIX_BENCH_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|samples| *samples > 0)
        .unwrap_or(5);
    let source = json_ten_mib_flat_fixture().0;
    let projected_key = format!("property_{:06}", JSON_TEN_MIB_PROPERTY_COUNT / 2);
    let parsed_source: serde_json::Value =
        serde_json::from_slice(&source).expect("benchmark JSON should parse");
    let projected_scalar = serde_json::to_string(
        parsed_source
            .get(&projected_key)
            .expect("projected benchmark property should exist"),
    )
    .expect("projected benchmark scalar should encode");
    let fixture = BenchmarkFixture {
        input_bytes: source.len(),
        logical_rows: JSON_TEN_MIB_PROPERTY_COUNT + 1,
    };
    let lanes = [("v3_push_sink", "plugin_json", build_json_plugin_archive())];
    let collector = PerfSpanCollector::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(collector.clone()));
    let _dispatcher = tracing::dispatcher::set_default(&dispatch);
    for (label, plugin_key, archive) in lanes {
        let mut measurements = Vec::with_capacity(samples);
        let mut elapsed_ms = Vec::with_capacity(samples);
        for sample in 0..samples {
            let root = tempfile::tempdir().expect("create JSON v3 benchmark directory");
            let lix = open_rocksdb_lix(root.path()).await;
            install_reference_plugin_in_blank_registry(
                &lix,
                plugin_key,
                &archive,
                &["json_root", "json_object_member", "json_array_item"],
            )
            .await;

            lix.reset_plugin_transition_counters();
            collector.clear();
            let allocation_scope = AllocationScope::start();
            let started = Instant::now();
            let inserted = lix
                .execute(
                    "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
                    &[
                        Value::Text(FILE_ID.to_owned()),
                        Value::Text(FILE_PATH.to_owned()),
                        Value::Blob(source.clone().into()),
                    ],
                )
                .await
                .unwrap_or_else(|error| panic!("{label} JSON import failed: {error}"));
            let measurement =
                BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
            assert_eq!(inserted.rows_affected(), 1, "{label} sample {sample}");
            let counters = lix.plugin_transition_counters();
            assert_eq!(counters.guest_export_calls, 1);
            assert_eq!(
                counters.durable_semantic_changes,
                (JSON_TEN_MIB_PROPERTY_COUNT + 1) as u64
            );
            assert_eq!(
                read_file(&lix, FILE_PATH).await.unwrap(),
                Some(source.clone())
            );
            let members = lix
                .execute("SELECT COUNT(*) AS count FROM json_object_member", &[])
                .await
                .expect("count JSON members")
                .rows()[0]
                .get::<i64>("count")
                .expect("JSON member count must be an integer");
            assert_eq!(members, JSON_TEN_MIB_PROPERTY_COUNT as i64);
            let projected = lix
                .execute(
                    "SELECT key, kind, scalar_json FROM json_object_member WHERE key = $1",
                    &[Value::Text(projected_key.clone())],
                )
                .await
                .expect("project exact JSON member");
            assert_eq!(projected.rows().len(), 1);
            assert_eq!(
                projected.rows()[0].get::<String>("key").unwrap(),
                projected_key
            );
            assert_eq!(projected.rows()[0].get::<String>("kind").unwrap(), "string");
            assert_eq!(
                projected.rows()[0].get::<String>("scalar_json").unwrap(),
                projected_scalar
            );
            let history_count = lix
                .execute(
                    "SELECT COUNT(*) AS count FROM json_object_member_history()",
                    &[],
                )
                .await
                .expect("certified JSON rows should participate in history")
                .rows()[0]
                .get::<i64>("count")
                .expect("JSON history count must be an integer");
            assert_eq!(history_count, JSON_TEN_MIB_PROPERTY_COUNT as i64);
            eprintln!(
                "v3_json_large_phases lane={label} sample={sample} elapsed_ms={:.3} \
                 guest_exports={} imports={} pages={} records={} boundary_bytes={} \
                 guest_high_water_bytes={} phase_close_live_bytes={:?} phases_ms={:?}",
                measurement.elapsed_ms,
                counters.guest_export_calls,
                counters.component_import_calls,
                counters.packet_pages,
                counters.packet_records,
                counters.component_boundary_bytes,
                counters.guest_linear_memory_high_water_bytes,
                collector.take_close_live_bytes(),
                collector.take_aggregate_millis(),
            );
            emit_sample(
                BENCHMARK,
                label,
                sample,
                fixture,
                BenchmarkGate::BulkWrite,
                measurement,
            );
            elapsed_ms.push(measurement.elapsed_ms);
            measurements.push(measurement);
            lix.close().await.expect("close JSON benchmark");
            let reopened = open_rocksdb_lix(root.path()).await;
            let reopened_count = reopened
                .execute("SELECT COUNT(*) AS count FROM json_object_member", &[])
                .await
                .expect("reopened JSON members should query")
                .rows()[0]
                .get::<i64>("count")
                .expect("reopened JSON member count must be an integer");
            assert_eq!(reopened_count, JSON_TEN_MIB_PROPERTY_COUNT as i64);
            emit_transition_profile(
                BENCHMARK,
                label,
                sample,
                counters,
                serde_json::json!({
                    "file_sha256": sha256_lower_hex(&source),
                    "file_bytes": source.len(),
                    "entity_rows": JSON_TEN_MIB_PROPERTY_COUNT + 1,
                    "history_rows": history_count,
                    "projected_key": projected_key,
                    "projected_scalar": projected_scalar,
                    "reopen_verified": true
                }),
            );
            reopened
                .close()
                .await
                .expect("close reopened JSON benchmark");
        }
        elapsed_ms.sort_by(f64::total_cmp);
        eprintln!(
            "v3_json_ten_mib lane={label} samples={samples} raw_ms={elapsed_ms:?} \
             p50_ms={:.3} p95_ms={:.3}",
            p50_ms(&elapsed_ms),
            p95_ms(&elapsed_ms)
        );
        emit_summary(
            BENCHMARK,
            label,
            fixture,
            BenchmarkGate::BulkWrite,
            &measurements,
        );
    }
}

#[tokio::test]
#[ignore = "10 MiB JSON sparse successor v3 arena benchmark"]
async fn v3_json_ten_mib_sparse_successor_benchmark() {
    const PATH: &str = "/v3-json-sparse.json";
    let samples = std::env::var("LIX_BENCH_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|samples| *samples > 0)
        .unwrap_or(5);
    let (before, edit_offset, edited_key) = json_ten_mib_flat_fixture();
    let mut after = before.clone();
    after[edit_offset] = alternate_ascii_hex(after[edit_offset]);
    let collector = PerfSpanCollector::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(collector.clone()));
    let _dispatcher = tracing::dispatcher::set_default(&dispatch);
    for (label, plugin_key, archive) in [("v3_arena", "plugin_json", build_json_plugin_archive())] {
        if std::env::var("LIX_BENCH_LANE").is_ok_and(|lane| lane != label) {
            continue;
        }
        let mut elapsed_ms = Vec::with_capacity(samples);
        let mut measurements = Vec::with_capacity(samples);
        for sample in 0..samples {
            let root = tempfile::tempdir().expect("create sparse JSON benchmark directory");
            let storage = RocksDB::open(root.path().join(".lix"))
                .expect("open sparse JSON benchmark RocksDB");
            let lix = open_lix()
                .with_storage(storage.clone())
                .await
                .expect("open sparse JSON benchmark workspace");
            install_reference_plugin_in_blank_registry(
                &lix,
                plugin_key,
                &archive,
                &["json_root", "json_object_member", "json_array_item"],
            )
            .await;
            write_file(&lix, PATH, before.clone())
                .await
                .unwrap_or_else(|error| panic!("{label} opening import failed: {error}"));
            storage
                .flush()
                .expect("flush sparse JSON benchmark cold import");

            lix.reset_plugin_transition_counters();
            collector.clear();
            let allocation_scope = AllocationScope::start();
            let started = Instant::now();
            write_file(&lix, PATH, after.clone())
                .await
                .unwrap_or_else(|error| panic!("{label} successor failed: {error}"));
            let measurement =
                BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
            let counters = lix.plugin_transition_counters();
            assert_eq!(read_file(&lix, PATH).await.unwrap(), Some(after.clone()));
            assert_eq!(counters.durable_semantic_changes, 1);
            let row = lix
                .execute(
                    "SELECT scalar_json FROM json_object_member WHERE key = $1",
                    &[Value::Text(edited_key.clone())],
                )
                .await
                .unwrap();
            assert_eq!(row.rows().len(), 1);
            assert!(
                row.rows()[0]
                    .get::<String>("scalar_json")
                    .unwrap()
                    .contains(char::from(after[edit_offset]))
            );
            eprintln!(
                "json_sparse lane={label} sample={sample} elapsed_ms={:.3} \
                 allocations={} allocated_mb={:.3} peak_live_mb={:.3} \
                 guest_exports={} imports={} boundary_bytes={} guest_high_water_mb={:.3}",
                measurement.elapsed_ms,
                measurement.allocations.allocation_count,
                measurement.allocations.allocated_bytes as f64 / 1_000_000.0,
                measurement.allocations.peak_live_bytes_delta as f64 / 1_000_000.0,
                counters.guest_export_calls,
                counters.component_import_calls,
                counters.component_boundary_bytes,
                counters.guest_linear_memory_high_water_bytes as f64 / 1_000_000.0,
            );
            eprintln!(
                "json_sparse_phases lane={label} sample={sample} phases_ms={:?} \
                 phase_close_live_bytes={:?}",
                collector.take_aggregate_millis(),
                collector.take_close_live_bytes(),
            );
            let fixture = BenchmarkFixture {
                input_bytes: after.len(),
                logical_rows: 1,
            };
            emit_sample(
                "v3_json_ten_mib_sparse_successor_benchmark",
                label,
                sample,
                fixture,
                BenchmarkGate::ElapsedRegression,
                measurement,
            );
            emit_transition_profile(
                "v3_json_ten_mib_sparse_successor_benchmark",
                label,
                sample,
                counters,
                serde_json::json!({
                    "before_sha256": sha256_lower_hex(&before),
                    "file_sha256": sha256_lower_hex(&after),
                    "file_bytes": after.len(),
                    "semantic_changes": 1,
                    "edited_key": edited_key
                }),
            );
            elapsed_ms.push(measurement.elapsed_ms);
            measurements.push(measurement);
            lix.close().await.unwrap();
        }
        elapsed_ms.sort_by(f64::total_cmp);
        eprintln!(
            "json_sparse lane={label} raw_ms={elapsed_ms:?} p50_ms={:.3} p95_ms={:.3}",
            p50_ms(&elapsed_ms),
            p95_ms(&elapsed_ms),
        );
        emit_summary(
            "v3_json_ten_mib_sparse_successor_benchmark",
            label,
            BenchmarkFixture {
                input_bytes: after.len(),
                logical_rows: 1,
            },
            BenchmarkGate::ElapsedRegression,
            &measurements,
        );
    }
}

#[tokio::test]
#[ignore = "10 MiB JSON process-cold hydrate plus sparse successor benchmark"]
async fn v3_json_ten_mib_cold_successor_benchmark() {
    const PATH: &str = "/v3-json-cold-successor.json";
    let samples = std::env::var("LIX_BENCH_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|samples| *samples > 0)
        .unwrap_or(3);
    let (before, edit_offset, edited_key) = json_ten_mib_flat_fixture();
    let mut after = before.clone();
    after[edit_offset] = alternate_ascii_hex(after[edit_offset]);
    let mut lane_medians = BTreeMap::new();

    for (label, plugin_key, archive) in [
        (
            "hydrate_then_update",
            "plugin_json",
            build_json_plugin_archive(),
        ),
        ("cold_successor", "plugin_json", build_json_plugin_archive()),
    ] {
        if std::env::var("LIX_BENCH_LANE").is_ok_and(|lane| lane != label) {
            continue;
        }
        let mut elapsed_ms = Vec::with_capacity(samples);
        let mut measurements = Vec::with_capacity(samples);
        for sample in 0..samples {
            let root = tempfile::tempdir().expect("create cold JSON benchmark directory");
            let storage =
                RocksDB::open(root.path().join(".lix")).expect("open cold JSON benchmark RocksDB");
            let lix = open_lix().with_storage(storage.clone()).await.unwrap();
            install_reference_plugin_in_blank_registry(
                &lix,
                plugin_key,
                &archive,
                &["json_root", "json_object_member", "json_array_item"],
            )
            .await;
            write_file(&lix, PATH, before.clone()).await.unwrap();
            storage.flush().expect("flush cold JSON benchmark import");
            lix.close().await.unwrap();

            let reopened = open_rocksdb_lix(root.path()).await;
            reopened.reset_plugin_transition_counters();
            let allocation_scope = AllocationScope::start();
            let started = Instant::now();
            if label == "hydrate_then_update" {
                assert_eq!(
                    read_file(&reopened, PATH).await.unwrap(),
                    Some(before.clone())
                );
            }
            write_file(&reopened, PATH, after.clone()).await.unwrap();
            let measurement =
                BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
            let counters = reopened.plugin_transition_counters();
            assert_eq!(
                read_file(&reopened, PATH).await.unwrap(),
                Some(after.clone())
            );
            assert_eq!(counters.durable_semantic_changes, 1);
            assert!(
                reopened
                    .execute(
                        "SELECT scalar_json FROM json_object_member WHERE key = $1",
                        &[Value::Text(edited_key.clone())],
                    )
                    .await
                    .unwrap()
                    .rows()[0]
                    .get::<String>("scalar_json")
                    .unwrap()
                    .contains(char::from(after[edit_offset]))
            );
            eprintln!(
                "json_cold_successor lane={label} sample={sample} elapsed_ms={:.3} \
                 allocations={} allocated_mb={:.3} peak_live_mb={:.3} \
                 guest_exports={} imports={} boundary_bytes={} guest_high_water_mb={:.3} \
                 semantic_rows_hydrated={} full_renders={}",
                measurement.elapsed_ms,
                measurement.allocations.allocation_count,
                measurement.allocations.allocated_bytes as f64 / 1_000_000.0,
                measurement.allocations.peak_live_bytes_delta as f64 / 1_000_000.0,
                counters.guest_export_calls,
                counters.component_import_calls,
                counters.component_boundary_bytes,
                counters.guest_linear_memory_high_water_bytes as f64 / 1_000_000.0,
                counters.full_state_semantic_rows_materialized,
                counters.full_renderer_invocations,
            );
            let fixture = BenchmarkFixture {
                input_bytes: after.len(),
                logical_rows: JSON_TEN_MIB_PROPERTY_COUNT,
            };
            emit_sample(
                "v3_json_ten_mib_cold_successor_benchmark",
                label,
                sample,
                fixture,
                BenchmarkGate::ElapsedRegression,
                measurement,
            );
            emit_transition_profile(
                "v3_json_ten_mib_cold_successor_benchmark",
                label,
                sample,
                counters,
                serde_json::json!({
                    "before_sha256": sha256_lower_hex(&before),
                    "file_sha256": sha256_lower_hex(&after),
                    "file_bytes": after.len(),
                    "semantic_changes": 1,
                    "edited_key": edited_key,
                    "cold_successor": label == "cold_successor"
                }),
            );
            elapsed_ms.push(measurement.elapsed_ms);
            measurements.push(measurement);
            reopened.close().await.unwrap();
        }
        elapsed_ms.sort_by(f64::total_cmp);
        eprintln!(
            "json_cold_successor lane={label} raw_ms={elapsed_ms:?} p50_ms={:.3} p95_ms={:.3}",
            p50_ms(&elapsed_ms),
            p95_ms(&elapsed_ms),
        );
        emit_summary(
            "v3_json_ten_mib_cold_successor_benchmark",
            label,
            BenchmarkFixture {
                input_bytes: after.len(),
                logical_rows: JSON_TEN_MIB_PROPERTY_COUNT,
            },
            BenchmarkGate::ElapsedRegression,
            &measurements,
        );
        lane_medians.insert(label, benchmark_medians(&measurements));
    }
    if let (Some(hydrate), Some(cold)) = (
        lane_medians.get("hydrate_then_update"),
        lane_medians.get("cold_successor"),
    ) {
        assert_candidate_benchmark_win("v3_json_ten_mib_cold_successor_benchmark", *hydrate, *cold);
    }
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
    reopened.reset_plugin_transition_counters();
    write_file(&reopened, PATH, after.clone()).await.unwrap();
    let counters = reopened.plugin_transition_counters();
    assert_eq!(counters.guest_export_calls, 1);
    assert_eq!(counters.full_document_reparses, 0);
    assert_eq!(counters.full_state_semantic_rows_materialized, 0);
    assert_eq!(counters.private_document_cache_hits, 1);
    assert_eq!(read_file(&reopened, PATH).await.unwrap(), Some(after));
    let rows = reopened
        .execute(
            "SELECT scalar_json FROM json_object_member WHERE key = 'edit'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.rows()[0].get::<String>("scalar_json").unwrap(), "3");
    reopened.close().await.unwrap();
}

#[async_trait::async_trait]
trait CurrentPluginCheckpointCorruptionBackend:
    Storage + Clone + Send + Sync + Sized + 'static
{
    fn open_checkpoint_fixture(path: &Path) -> Self;
    async fn flush_checkpoint_fixture(&self);
}

#[async_trait::async_trait]
impl CurrentPluginCheckpointCorruptionBackend for RocksDB {
    fn open_checkpoint_fixture(path: &Path) -> Self {
        Self::open(path).expect("open RocksDB plugin-checkpoint corruption fixture")
    }

    async fn flush_checkpoint_fixture(&self) {
        self.flush()
            .expect("flush RocksDB plugin-checkpoint corruption fixture");
    }
}

#[async_trait::async_trait]
impl CurrentPluginCheckpointCorruptionBackend for SlateDB {
    fn open_checkpoint_fixture(path: &Path) -> Self {
        Self::open(path).expect("open SlateDB plugin-checkpoint corruption fixture")
    }

    async fn flush_checkpoint_fixture(&self) {
        self.flush_memtable_for_diagnostics()
            .await
            .expect("flush SlateDB plugin-checkpoint corruption fixture");
    }
}

#[tokio::test]
async fn rocksdb_corrupt_current_plugin_checkpoint_fails_public_actor_path() {
    qualify_corrupt_current_plugin_checkpoint::<RocksDB>().await;
}

#[tokio::test]
async fn slatedb_corrupt_current_plugin_checkpoint_fails_public_actor_path() {
    qualify_corrupt_current_plugin_checkpoint::<SlateDB>().await;
}

async fn qualify_corrupt_current_plugin_checkpoint<B: CurrentPluginCheckpointCorruptionBackend>() {
    const FILE_ID: &str = "01920000-0000-7000-8000-0000000000c1";
    const PATH: &str = "/authenticated-checkpoint.json";
    const CHECKPOINT_SPACE: &str = "plugin.current_checkpoint.v2";

    let directory = tempfile::tempdir().expect("create plugin-checkpoint corruption fixture");
    let storage_path = directory.path().join(".lix");
    let database = B::open_checkpoint_fixture(&storage_path);
    let lix = open_lix()
        .with_storage(database.clone())
        .await
        .expect("open plugin-checkpoint corruption fixture");
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
            Value::Text(FILE_ID.to_owned()),
            Value::Text(PATH.to_owned()),
            Value::Blob(br#"{"before":1}"#.to_vec().into()),
        ],
    )
    .await
    .expect("materialize plugin file and its durable checkpoint");
    let branch_id = lix
        .active_branch_id()
        .await
        .expect("load plugin-checkpoint branch owner");
    database.flush_checkpoint_fixture().await;
    lix.close()
        .await
        .expect("close healthy plugin-checkpoint fixture");
    drop(database);

    let database = B::open_checkpoint_fixture(&storage_path);
    let storage = StorageAdapter::new(database.clone());
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open plugin-checkpoint corruption read");
    let branch_id_bytes =
        uuid::Uuid::parse_str(&branch_id).expect("plugin-checkpoint branch owner is a UUID");
    let file_id_bytes =
        uuid::Uuid::parse_str(FILE_ID).expect("plugin-checkpoint file owner is a UUID");
    let mut expected_key = Vec::with_capacity(32);
    expected_key.extend_from_slice(branch_id_bytes.as_bytes());
    expected_key.extend_from_slice(file_id_bytes.as_bytes());
    let entries = space_inventory(&read, CHECKPOINT_SPACE).await;
    drop(read);
    let (_, mut value) = entries
        .into_iter()
        .find(|(key, _)| key == &expected_key)
        .expect("real plugin operation must persist its current checkpoint");
    assert!(
        value.len() > 92,
        "checkpoint payload must include authenticated bytes"
    );
    value[92] ^= 1;
    let (space_id, _) = layout_space_catalog()
        .into_iter()
        .find(|(_, name)| *name == CHECKPOINT_SPACE)
        .expect("plugin current-checkpoint space must be catalogued");
    let mut writes = storage.new_write_set();
    writes.put(
        // A space id has exactly one value semantics; read it from the engine
        // registry rather than restating it here.
        lix::storage_bench::storage_space_by_id(space_id),
        StorageKey(Bytes::from(expected_key)),
        StorageValue {
            bytes: Bytes::from(value),
        },
    );
    storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .await
        .expect("commit physical plugin-checkpoint corruption fixture");
    database.flush_checkpoint_fixture().await;
    drop(storage);
    drop(database);

    let database = B::open_checkpoint_fixture(&storage_path);
    let lix = open_lix()
        .with_storage(database.clone())
        .await
        .expect("cold reopen plugin-checkpoint corruption fixture");
    let error = write_file(&lix, PATH, br#"{"before":2}"#.to_vec())
        .await
        .expect_err("public plugin actor path must reject corrupt current checkpoint");
    assert!(
        error
            .to_string()
            .contains("plugin current checkpoint authentication digest mismatch"),
        "unexpected public plugin-checkpoint corruption error: {error}"
    );
    lix.close()
        .await
        .expect("close rejected plugin-checkpoint fixture");
    drop(database);
}

/// Lane parity for the durable actor checkpoint.
///
/// #1353 made plugin reconciliation lane-neutral, but the checkpoint was still
/// withheld from untracked files, so every session re-parsed them from scratch.
/// `plugin.current_checkpoint.v2` is a dedicated mutable side space keyed by
/// `branch_id ++ file_id` — no lane column, no commit link, no changelog — and
/// its validity is content-addressed, so publishing one for an untracked file
/// creates no cross-lane state and can never serve stale rows: it either
/// matches the current generation, blob hash and semantic root, or the load
/// degrades into the cold rebuild it was accelerating.
///
/// The second half is the correctness crux. A restored actor that served stale
/// entity rows would be silent corruption, so the edit after cold reopen has to
/// re-render exactly the bytes a cold rebuild would have produced.
#[tokio::test]
async fn untracked_plugin_file_publishes_and_restores_its_durable_checkpoint() {
    const FILE_ID: &str = "01920000-0000-7000-8000-0000000000c3";
    const PATH: &str = "/untracked-durable-checkpoint.json";
    const CHECKPOINT_SPACE: &str = "plugin.current_checkpoint.v2";

    let root = tempfile::tempdir().expect("create untracked checkpoint fixture");
    let lix = open_rocksdb_lix(root.path()).await;
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
            Value::Blob(br#"{"alpha":"before"}"#.to_vec().into()),
        ],
    )
    .await
    .expect("untracked plugin file should write");
    let branch_id = lix
        .active_branch_id()
        .await
        .expect("load untracked checkpoint branch owner");
    lix.close()
        .await
        .expect("close untracked checkpoint fixture");

    let mut expected_key = Vec::with_capacity(32);
    expected_key.extend_from_slice(
        uuid::Uuid::parse_str(&branch_id)
            .expect("untracked checkpoint branch owner is a UUID")
            .as_bytes(),
    );
    expected_key.extend_from_slice(
        uuid::Uuid::parse_str(FILE_ID)
            .expect("untracked checkpoint file owner is a UUID")
            .as_bytes(),
    );
    let database =
        RocksDB::open(root.path().join(".lix")).expect("reopen untracked checkpoint inventory");
    let storage = StorageAdapter::new(database.clone());
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open untracked checkpoint inventory read");
    let entries = space_inventory(&read, CHECKPOINT_SPACE).await;
    drop(read);
    drop(storage);
    drop(database);
    assert!(
        entries.iter().any(|(key, _)| key == &expected_key),
        "an untracked plugin file must publish its durable checkpoint like a tracked one"
    );

    let lix = open_rocksdb_lix(root.path()).await;
    lix.execute(
        "UPDATE json_object_member SET scalar_json = '\"after\"' \
         WHERE parent_id = 'root' AND key = 'alpha' AND lixcol_file_id = $1",
        &[Value::Text(FILE_ID.to_owned())],
    )
    .await
    .expect("a restored untracked actor should accept an entity edit");
    assert_eq!(
        read_file(&lix, PATH).await.unwrap(),
        Some(br#"{"alpha":"after"}"#.to_vec()),
        "a restored checkpoint must re-render current bytes, never stale ones"
    );
    let still_untracked = lix
        .execute(
            "SELECT lixcol_untracked FROM lix_file WHERE id = $1",
            &[Value::Text(FILE_ID.to_owned())],
        )
        .await
        .expect("the untracked file should still be visible")
        .rows()[0]
        .get::<bool>("lixcol_untracked")
        .expect("lane column should project");
    assert!(
        still_untracked,
        "a durable checkpoint must not promote the file out of the untracked lane"
    );
    lix.close()
        .await
        .expect("close restored untracked checkpoint fixture");
}

#[tokio::test]
async fn universal_entity_page_streams_oversized_output_snapshot() {
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
    let scalar = rows.rows()[0].get::<String>("scalar_json").unwrap();
    assert_eq!(
        serde_json::from_str::<String>(&scalar).unwrap().len(),
        3 * 1024 * 1024
    );
    lix.close().await.unwrap();

    let reopened = open_rocksdb_lix(root.path()).await;
    assert_eq!(read_file(&reopened, PATH).await.unwrap(), Some(bytes));
    reopened.close().await.unwrap();
}

#[derive(Debug)]
struct BenchmarkByteSource(Vec<u8>);

impl WasmByteSource for BenchmarkByteSource {
    fn len(&self) -> u64 {
        self.0.len() as u64
    }

    fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>, LixError> {
        let start = usize::try_from(offset)
            .map_err(|_| LixError::new(LixError::CODE_INVALID_PARAM, "benchmark source offset"))?;
        let end = start
            .checked_add(length as usize)
            .ok_or_else(|| LixError::new(LixError::CODE_INVALID_PARAM, "benchmark source range"))?;
        self.0
            .get(start..end)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| LixError::new(LixError::CODE_INVALID_PARAM, "benchmark source range"))
    }
}

struct BenchmarkEntitySource {
    entities: Vec<WasmHostEntity>,
    next: usize,
}

impl WasmEntitySource for BenchmarkEntitySource {
    fn next_page(&mut self, max_bytes: u32) -> Result<Option<WasmEntityPage>, LixError> {
        if self.next == self.entities.len() {
            return Ok(None);
        }
        let start = self.next;
        let mut bytes = 0usize;
        while self.next < self.entities.len() {
            let entity = &self.entities[self.next];
            let size = entity.key.schema_key.len()
                + entity
                    .key
                    .entity_pk
                    .iter()
                    .map(|part| part.len())
                    .sum::<usize>()
                + entity.snapshot_content.len() as usize
                + 64;
            if self.next > start && bytes.saturating_add(size) > max_bytes as usize {
                break;
            }
            bytes = bytes.saturating_add(size);
            self.next += 1;
        }
        Ok(Some(WasmEntityPage {
            entities: self.entities[start..self.next].to_vec(),
        }))
    }
}

#[tokio::test]
async fn v3_json_direct_cold_successor_preserves_durable_identity() {
    let wasm = std::fs::read(Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_plugin_json")))
        .expect("read JSON component");
    let runtime = lix::default_wasm_runtime().expect("default Wasm runtime");
    let factory = runtime
        .compile_component(wasm, WasmLimits::default())
        .await
        .expect("compile JSON component");
    let descriptor = WasmFileDescriptor {
        path: Some("/direct-cold.json".to_owned()),
        plugin: WasmPluginSelection {
            plugin_key: "plugin_json".to_owned(),
            generation: "direct".to_owned(),
        },
    };
    let large_value = "x".repeat(96 * 1024);
    let before = format!(r#"{{"a":"one","b":"{large_value}"}}"#).into_bytes();
    let after = format!(r#"{{"a":"ONE","b":"{large_value}"}}"#).into_bytes();
    let oversized_snapshot = serde_json::to_vec(&serde_json::json!({
        "parent_id": "root",
        "key": "b",
        "order_key": "80",
        "kind": "string",
        "scalar_json": serde_json::to_string(&large_value).unwrap(),
    }))
    .unwrap();
    let oversized_snapshot_len = oversized_snapshot.len() as u64;
    let entities = vec![
        WasmEntity {
            key: WasmEntityKey::from_owned_parts("json_root", vec!["root".to_owned()]),
            snapshot_content: WasmHostBytes::Inline(Bytes::from_static(
                br#"{"id":"root","kind":"object"}"#,
            )),
        },
        WasmEntity {
            key: WasmEntityKey::from_owned_parts(
                "json_object_member",
                vec!["root".to_owned(), "a".to_owned()],
            ),
            snapshot_content: WasmHostBytes::Inline(Bytes::from_static(
                br#"{"parent_id":"root","key":"a","order_key":"40","kind":"string","scalar_json":"\"one\""}"#,
            )),
        },
        WasmEntity {
            key: WasmEntityKey::from_owned_parts(
                "json_object_member",
                vec!["root".to_owned(), "b".to_owned()],
            ),
            snapshot_content: WasmHostBytes::Source(WasmSourceSlice {
                source: Arc::new(BenchmarkByteSource(oversized_snapshot)),
                range: WasmSourceRange {
                    offset: 0,
                    length: oversized_snapshot_len,
                },
            }),
        },
    ];
    let original_keys = entities
        .iter()
        .map(|entity| entity.key.clone())
        .collect::<Vec<_>>();

    let limits = WasmTransitionLimits {
        max_page_bytes: 32 * 1024,
        max_record_bytes: 32 * 1024,
        max_total_bytes: 1024 * 1024,
        max_inline_input_bytes: 32 * 1024,
        ..WasmTransitionLimits::default()
    };
    let mut actor = factory.instantiate_actor().await.unwrap();
    let cold = actor
        .cold_file_changed(
            limits,
            WasmColdFileUpdate {
                before_descriptor: descriptor.clone(),
                after_descriptor: descriptor,
                before: Some(Arc::new(BenchmarkByteSource(before))),
                edits: vec![WasmInputSplice {
                    offset: 6,
                    delete_len: 3,
                    insert: WasmInputBytes::Inline(b"ONE".to_vec()),
                }],
                after: Arc::new(BenchmarkByteSource(after)),
                creates: WasmCreateContext { high: 13, low: 17 },
                entities: Box::new(BenchmarkEntitySource { entities, next: 0 }),
            },
        )
        .await
        .unwrap();
    let mut changed_keys = Vec::new();
    while let Some(page) = actor
        .next_change_page(cold.transition, cold.changes, 2 * 1024 * 1024)
        .await
        .unwrap()
    {
        changed_keys.extend(
            page.changes
                .changes
                .iter()
                .filter_map(WasmEntityChange::entity_key)
                .cloned(),
        );
    }
    actor.finish_transition(cold.transition).await.unwrap();
    assert_eq!(changed_keys.len(), 1);
    assert!(original_keys.contains(&changed_keys[0]));
    actor.retire().await.unwrap();
}

fn json_ten_mib_durable_entities() -> Vec<WasmHostEntity> {
    let mut entities = Vec::with_capacity(JSON_TEN_MIB_PROPERTY_COUNT + 1);
    entities.push(WasmEntity {
        key: WasmEntityKey::from_owned_parts("json_root", vec!["root".to_owned()]),
        snapshot_content: WasmHostBytes::Inline(Bytes::from_static(
            br#"{"id":"root","kind":"object"}"#,
        )),
    });
    let mut state = 0x6a73_6f6e_2d31_306du64;
    let base_bytes = 2 + JSON_TEN_MIB_PROPERTY_COUNT * 44 + JSON_TEN_MIB_PROPERTY_COUNT - 1;
    let padding = JSON_TEN_MIB_BYTES - base_bytes;
    let padding_per_property = padding / JSON_TEN_MIB_PROPERTY_COUNT;
    let extra_padding_properties = padding % JSON_TEN_MIB_PROPERTY_COUNT;
    for index in 0..JSON_TEN_MIB_PROPERTY_COUNT {
        state = splitmix64(state);
        let first = state;
        state = splitmix64(state);
        let second = state as u32;
        let padding = padding_per_property + usize::from(index < extra_padding_properties);
        let key = format!("property_{index:06}");
        let value = format!("{first:016x}{second:08x}{}", "f".repeat(padding));
        let order_key = format!("{:016x}", (index as u64).saturating_mul(2) + 1);
        let snapshot = serde_json::to_vec(&serde_json::json!({
            "parent_id": "root",
            "key": key,
            "order_key": order_key,
            "kind": "string",
            "scalar_json": serde_json::to_string(&value).unwrap(),
        }))
        .unwrap();
        entities.push(WasmEntity {
            key: WasmEntityKey::from_owned_parts(
                "json_object_member",
                vec!["root".to_owned(), key],
            ),
            snapshot_content: WasmHostBytes::Inline(Bytes::from(snapshot)),
        });
    }
    entities
}

async fn drain_direct_file_transition(
    actor: &mut dyn WasmComponentActor,
    transition: WasmFileTransition,
) -> WasmTransitionCounters {
    while actor
        .next_change_page(transition.transition, transition.changes, 2 * 1024 * 1024)
        .await
        .unwrap()
        .is_some()
    {}
    let _ = actor.take_certified_entity_batches(transition.transition);
    actor
        .finish_transition(transition.transition)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "10 MiB direct Wasm hydrate+update versus cold-successor A/B"]
async fn v3_json_direct_cold_successor_benchmark() {
    let samples = std::env::var("LIX_BENCH_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3);
    let wasm = std::fs::read(Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_plugin_json")))
        .expect("read JSON component");
    let factory = lix::default_wasm_runtime()
        .unwrap()
        .compile_component(
            wasm,
            WasmLimits {
                max_memory_bytes: 128 * 1024 * 1024,
                ..WasmLimits::default()
            },
        )
        .await
        .unwrap();
    let descriptor = WasmFileDescriptor {
        path: Some("/direct-cold-large.json".to_owned()),
        plugin: WasmPluginSelection {
            plugin_key: "plugin_json".to_owned(),
            generation: "direct".to_owned(),
        },
    };
    let creates = WasmCreateContext { high: 13, low: 17 };
    let (before, edit_offset, _) = json_ten_mib_flat_fixture();
    let mut after = before.clone();
    after[edit_offset] = alternate_ascii_hex(after[edit_offset]);
    let entities = json_ten_mib_durable_entities();
    for lane in ["hydrate_then_update", "cold_successor"] {
        if std::env::var("LIX_BENCH_LANE").is_ok_and(|selected| selected != lane) {
            continue;
        }
        let mut elapsed = Vec::new();
        for sample in 0..samples {
            let source_entities = entities.clone();
            let before_source = Arc::new(BenchmarkByteSource(before.clone()));
            let after_source = Arc::new(BenchmarkByteSource(after.clone()));
            let mut actor = factory.instantiate_actor().await.unwrap();
            let allocation_scope = AllocationScope::start();
            let started = Instant::now();
            let counters = if lane == "hydrate_then_update" {
                let hydrated = actor
                    .open_entities(
                        WasmTransitionLimits::default(),
                        WasmOpenEntitiesInput {
                            descriptor: descriptor.clone(),
                            entities: Box::new(BenchmarkEntitySource {
                                entities: source_entities,
                                next: 0,
                            }),
                            accepted: Some(before_source.clone()),
                        },
                    )
                    .await
                    .unwrap();
                assert!(
                    actor
                        .next_edit_page(hydrated.transition, hydrated.edits, 1024, 2 * 1024 * 1024,)
                        .await
                        .unwrap()
                        .is_none()
                );
                let mut counters = actor.finish_transition(hydrated.transition).await.unwrap();
                let transition = actor
                    .file_changed(
                        hydrated.document,
                        WasmTransitionLimits::default(),
                        WasmFileUpdate {
                            before_descriptor: descriptor.clone(),
                            after_descriptor: descriptor.clone(),
                            before: before_source,
                            edits: vec![WasmInputSplice {
                                offset: edit_offset as u64,
                                delete_len: 1,
                                insert: WasmInputBytes::Inline(vec![after[edit_offset]]),
                            }],
                            after: after_source,
                            creates,
                        },
                    )
                    .await
                    .unwrap();
                counters.accumulate(drain_direct_file_transition(actor.as_mut(), transition).await);
                counters
            } else {
                let transition = actor
                    .cold_file_changed(
                        WasmTransitionLimits::default(),
                        WasmColdFileUpdate {
                            before_descriptor: descriptor.clone(),
                            after_descriptor: descriptor.clone(),
                            before: Some(before_source),
                            edits: vec![WasmInputSplice {
                                offset: edit_offset as u64,
                                delete_len: 1,
                                insert: WasmInputBytes::Inline(vec![after[edit_offset]]),
                            }],
                            after: after_source,
                            creates,
                            entities: Box::new(BenchmarkEntitySource {
                                entities: source_entities,
                                next: 0,
                            }),
                        },
                    )
                    .await
                    .unwrap();
                drain_direct_file_transition(actor.as_mut(), transition).await
            };
            let measurement =
                BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
            eprintln!(
                "json_direct_cold lane={lane} sample={sample} elapsed_ms={:.3} peak_live_mb={:.3} \
                 allocated_mb={:.3} exports={} imports={} boundary_mb={:.3} guest_hwm_mb={:.3}",
                measurement.elapsed_ms,
                measurement.allocations.peak_live_bytes_delta as f64 / 1_000_000.0,
                measurement.allocations.allocated_bytes as f64 / 1_000_000.0,
                counters.guest_export_calls,
                counters.component_import_calls,
                counters.component_boundary_bytes as f64 / 1_000_000.0,
                counters.guest_linear_memory_high_water_bytes as f64 / 1_000_000.0,
            );
            elapsed.push(measurement.elapsed_ms);
            actor.retire().await.unwrap();
        }
        elapsed.sort_by(f64::total_cmp);
        eprintln!(
            "json_direct_cold lane={lane} raw_ms={elapsed:?} p50_ms={:.3} p95_ms={:.3}",
            p50_ms(&elapsed),
            p95_ms(&elapsed),
        );
    }
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
        current.rows()[0].get::<String>("scalar_json").unwrap(),
        r#""ONE""#
    );
    assert_eq!(current.rows()[1].get::<String>("key").unwrap(), "b");

    let historical = lix
        .execute(
            "SELECT key, scalar_json FROM json_object_member_history() \
             WHERE lixcol_depth = 1 ORDER BY key",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(historical.rows().len(), 2);
    assert_eq!(
        historical.rows()[0].get::<String>("scalar_json").unwrap(),
        r#""one""#
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

    lix.reset_plugin_transition_counters();
    let after = br#"{"a":"ONE","b":"two"}"#.to_vec();
    write_file(&lix, path, after.clone()).await.unwrap();
    let counters = lix.plugin_transition_counters();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(after.clone()));
    assert_eq!(counters.durable_semantic_changes, 1);
    assert_eq!(
        lix.execute(
            "SELECT scalar_json FROM json_object_member WHERE key = 'a'",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
            .get::<String>("scalar_json")
            .unwrap(),
        r#""ONE""#
    );
    lix.close().await.unwrap();

    let reopened = open_rocksdb_lix(root.path()).await;
    assert_eq!(read_file(&reopened, path).await.unwrap(), Some(after));
    let after_reopen = br#"{"a":"ONE","b":"TWO"}"#.to_vec();
    reopened.reset_plugin_transition_counters();
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
            .get::<String>("scalar_json")
            .unwrap(),
        r#""TWO""#
    );
    assert_eq!(
        reopened
            .plugin_transition_counters()
            .durable_semantic_changes,
        1
    );
    assert_eq!(
        reopened
            .plugin_transition_counters()
            .full_state_semantic_rows_materialized,
        0,
        "durable JSON checkpoint restore must skip semantic-row hydration",
    );
    assert_eq!(
        reopened
            .plugin_transition_counters()
            .private_document_cache_hits,
        1,
    );
    assert_eq!(
        reopened.plugin_transition_counters().full_document_reparses,
        0,
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

    lix.reset_plugin_transition_counters();
    let after_eviction = b"alpha,ONE\nbeta,two\n".to_vec();
    write_file(&lix, path, after_eviction).await.unwrap();
    let eviction_counters = lix.plugin_transition_counters();
    assert_eq!(
        eviction_counters.guest_export_calls, 1,
        "an acknowledged but evicted CSV actor must use cold successor directly"
    );
    assert_eq!(
        eviction_counters.full_state_semantic_rows_materialized, 0,
        "an in-process decoded checkpoint must avoid durable entity hydration after Store eviction"
    );
    assert_eq!(eviction_counters.private_document_cache_hits, 1);
    assert_eq!(eviction_counters.full_document_reparses, 0);
    assert_eq!(eviction_counters.durable_semantic_changes, 1);
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
    reopened.reset_plugin_transition_counters();
    let after_reopen = b"alpha,One\nbeta,two\n".to_vec();
    write_file(&reopened, path, after_reopen.clone())
        .await
        .unwrap();
    assert_eq!(
        reopened.plugin_transition_counters().guest_export_calls,
        1,
        "cold CSV reconciliation must not hydrate and re-enter the guest"
    );
    assert_eq!(
        reopened
            .plugin_transition_counters()
            .full_state_semantic_rows_materialized,
        0,
        "a durable checkpoint must avoid semantic-row hydration after process restart"
    );
    assert_eq!(
        reopened
            .plugin_transition_counters()
            .private_document_cache_hits,
        1
    );
    assert_eq!(
        reopened.plugin_transition_counters().full_document_reparses,
        0
    );
    assert_eq!(
        reopened
            .plugin_transition_counters()
            .durable_semantic_changes,
        1
    );
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

/// Warm file transitions through the current CSV push sink.
///
/// A real v2 comparison is run from the frozen v2 revision. The hard-cut tree
/// no longer contains the returned-cursor runtime.
#[tokio::test]
#[ignore = "Component v3 push-sink benchmark"]
async fn v3_file_changed_push_sink_benchmark() {
    const ROW_COUNT: usize = 220_000;
    const BENCHMARK: &str = "v3_file_changed_push_sink_benchmark";

    let samples = std::env::var("LIX_BENCH_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|samples| *samples > 0)
        .unwrap_or(10);
    let source = csv_ten_mib_fixture();
    let fixture = BenchmarkFixture {
        input_bytes: source.len(),
        logical_rows: 1,
    };
    let collector = PerfSpanCollector::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(collector.clone()));
    let _dispatcher = tracing::dispatcher::set_default(&dispatch);
    let root = tempfile::tempdir().expect("create v3 push-sink benchmark directory");
    let lix = open_rocksdb_lix(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv",
        &build_csv_plugin_archive(),
        &["csv_table", "csv_row"],
    )
    .await;
    let path = "/push-sink.csv";
    write_file(&lix, path, source.clone())
        .await
        .expect("v3 benchmark import should succeed");
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(source.clone()));

    let mut bytes = source;
    let mut measurements = Vec::with_capacity(samples);
    let mut elapsed_ms = Vec::with_capacity(samples);

    for sample in 0..samples {
        let next = if sample % 2 == 0 { b'9' } else { b'0' };
        bytes[0] = next;
        lix.reset_plugin_transition_counters();
        collector.clear();
        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        write_file(&lix, path, bytes.clone())
            .await
            .unwrap_or_else(|error| panic!("v3 sample {sample} should succeed: {error:?}"));
        let measurement = BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
        let counters = lix.plugin_transition_counters();
        eprintln!(
            "v3_file_changed_phases lane=v3_push_sink sample={sample} elapsed_ms={:.3} \
             guest_exports={} imports={} boundary_bytes={} guest_high_water_bytes={} \
             phase_close_live_bytes={:?} phases_ms={:?}",
            measurement.elapsed_ms,
            counters.guest_export_calls,
            counters.component_import_calls,
            counters.component_boundary_bytes,
            counters.guest_linear_memory_high_water_bytes,
            collector.take_close_live_bytes(),
            collector.take_aggregate_millis(),
        );
        assert_eq!(counters.packet_records, 1, "sample {sample}");
        assert_eq!(counters.durable_semantic_changes, 1, "sample {sample}");
        assert_eq!(
            counters.guest_export_calls, 1,
            "v3 must use one guest export"
        );
        emit_sample(
            BENCHMARK,
            "v3_push_sink",
            sample,
            fixture,
            BenchmarkGate::ElapsedRegression,
            measurement,
        );
        emit_transition_profile(
            BENCHMARK,
            "v3_push_sink",
            sample,
            counters,
            serde_json::json!({
                "file_sha256": sha256_lower_hex(&bytes),
                "file_bytes": bytes.len(),
                "semantic_changes": 1
            }),
        );
        elapsed_ms.push(measurement.elapsed_ms);
        measurements.push(measurement);
    }

    assert_eq!(read_file(&lix, path).await.unwrap(), Some(bytes));
    let row_count = lix
        .execute("SELECT COUNT(*) AS count FROM csv_row", &[])
        .await
        .expect("v3 semantic rows should query")
        .rows()[0]
        .get::<i64>("count")
        .expect("v3 row count should be integer");
    assert_eq!(row_count, ROW_COUNT as i64);

    elapsed_ms.sort_by(f64::total_cmp);
    eprintln!(
        "v3_file_changed_push_sink bytes={} rows={} samples={samples} \
         raw_ms={elapsed_ms:?} p50_ms={:.3} guest_exports=1",
        fixture.input_bytes,
        ROW_COUNT,
        p50_ms(&elapsed_ms),
    );
    emit_summary(
        BENCHMARK,
        "v3_push_sink",
        fixture,
        BenchmarkGate::ElapsedRegression,
        &measurements,
    );
    lix.close().await.expect("close v3 benchmark");
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

#[tokio::test]
#[ignore = "large v3 cold-successor CSV and JSON benchmark"]
async fn v3_cold_successor_csv_and_json_benchmark() {
    const BENCHMARK: &str = "v3_cold_successor_csv_and_json_benchmark";
    const CSV_COLD_ROWS: usize = 220_000;
    let samples = std::env::var("LIX_BENCH_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|samples| *samples > 0)
        .unwrap_or(5);
    let collector = PerfSpanCollector::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(collector.clone()));
    let _dispatcher = tracing::dispatcher::set_default(&dispatch);

    let storage = lix::Memory::new();
    let seed = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("benchmark workspace should open");
    install_plugin(&seed, "plugin_csv", &build_csv_plugin_archive())
        .await
        .expect("CSV v2 plugin should install");
    install_plugin(&seed, "plugin_json", &build_json_plugin_archive())
        .await
        .expect("JSON v2 plugin should install");

    let csv = csv_ten_mib_fixture();
    let (json_flat, json_edit_offset, _) = json_ten_mib_flat_fixture();
    let nested_prefix = br#"{"outer":"#;
    let mut json_nested = Vec::with_capacity(json_flat.len() + 10);
    json_nested.extend_from_slice(nested_prefix);
    json_nested.extend_from_slice(&json_flat);
    json_nested.push(b'}');

    for (path, bytes) in [
        ("/cold-materialized.csv", csv.as_slice()),
        ("/cold-materialized-flat.json", json_flat.as_slice()),
        ("/cold-materialized-nested.json", json_nested.as_slice()),
    ] {
        write_file(&seed, path, bytes.to_vec())
            .await
            .unwrap_or_else(|error| panic!("seed import for {path} should succeed: {error:?}"));
    }
    seed.close().await.expect("seed workspace should close");

    // An exact file read deliberately returns durable materialized bytes without
    // waking Wasm. Measure the operation that genuinely needs a cold actor:
    // hydrate the materialized semantic base, then apply one localized ordinary
    // byte write. Each sample toggles the same valid scalar byte, so every
    // reopen has a new accepted base and cannot become a no-op.
    for (label, path, initial, edit_offset) in [
        ("csv-220k-rows", "/cold-materialized.csv", csv.as_slice(), 0),
        (
            "json-flat-39870-properties",
            "/cold-materialized-flat.json",
            json_flat.as_slice(),
            json_edit_offset,
        ),
        (
            "json-nested-39870-properties",
            "/cold-materialized-nested.json",
            json_nested.as_slice(),
            nested_prefix.len() + json_edit_offset,
        ),
    ] {
        if std::env::var("LIX_BENCH_LANE").is_ok_and(|selected| selected != label) {
            continue;
        }
        let mut lane_samples = Vec::with_capacity(samples);
        let mut accepted = initial.to_vec();
        for sample in 0..samples {
            let lix = open_lix()
                .with_storage(storage.clone())
                .await
                .expect("cold benchmark workspace should reopen");
            lix.reset_plugin_transition_counters();
            collector.clear();
            let mut after = accepted.clone();
            after[edit_offset] = alternate_ascii_hex(after[edit_offset]);
            let allocation_scope = AllocationScope::start();
            let started = Instant::now();
            write_file(&lix, path, after.clone())
                .await
                .unwrap_or_else(|error| panic!("cold write for {path} should succeed: {error:?}"));
            let measurement =
                BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
            let phases_ms = collector.take_aggregate_millis();
            let phase_close_live_bytes = collector.take_close_live_bytes();
            let actual = read_file(&lix, path)
                .await
                .unwrap_or_else(|error| {
                    panic!("cold write result for {path} should read: {error:?}")
                })
                .unwrap_or_else(|| panic!("cold write result for {path} should exist"));
            assert_eq!(actual, after, "cold write must remain byte-exact");
            eprintln!(
                "v3_cold_successor_phases label={label} sample={sample} phases_ms={:?} phase_close_live_bytes={:?}",
                phases_ms, phase_close_live_bytes,
            );
            let counters = lix.plugin_transition_counters();
            let fixture = BenchmarkFixture {
                input_bytes: after.len(),
                logical_rows: if label == "csv-220k-rows" {
                    CSV_COLD_ROWS + 1
                } else {
                    JSON_TEN_MIB_PROPERTY_COUNT + 1
                },
            };
            emit_sample(
                BENCHMARK,
                label,
                sample,
                fixture,
                BenchmarkGate::ElapsedRegression,
                measurement,
            );
            emit_transition_profile(
                BENCHMARK,
                label,
                sample,
                counters,
                serde_json::json!({
                    "file_sha256": sha256_lower_hex(&after),
                    "file_bytes": after.len(),
                    "entity_rows": fixture.logical_rows,
                    "cold_successor": true
                }),
            );
            lane_samples.push(ColdMaterializedOpenSample {
                measurement,
                counters,
            });
            lix.close()
                .await
                .expect("cold benchmark workspace should close");
            accepted = after;
        }
        report_cold_materialized_open(label, accepted.len(), &lane_samples);
        emit_summary(
            BENCHMARK,
            label,
            BenchmarkFixture {
                input_bytes: accepted.len(),
                logical_rows: if label == "csv-220k-rows" {
                    CSV_COLD_ROWS + 1
                } else {
                    JSON_TEN_MIB_PROPERTY_COUNT + 1
                },
            },
            BenchmarkGate::ElapsedRegression,
            &lane_samples
                .iter()
                .map(|sample| sample.measurement)
                .collect::<Vec<_>>(),
        );
    }
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
            "{label} durable checkpoint must avoid semantic-row hydration"
        );
        assert_eq!(counters.private_document_cache_hits, 1);
        assert_eq!(counters.full_document_reparses, 0);
        assert!(
            counters.component_boundary_bytes > 0,
            "{label} cold successor must account for its bounded entity pages"
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
async fn v2_json_cold_entity_write_is_scoped_by_file_despite_shared_root_keys() {
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
    lix.reset_plugin_transition_counters();
    lix.execute(
        "UPDATE json_object_member SET scalar_json = $1 \
         WHERE parent_id = 'root' AND key = 'value' AND lixcol_file_id = $2",
        &[
            Value::Text(r#""FIRST""#.to_string()),
            Value::Text(first_id.clone()),
        ],
    )
    .await
    .unwrap();
    let counters = lix.plugin_transition_counters();
    assert_eq!(
        counters.full_state_semantic_rows_materialized, 2,
        "cold reconstruction must hydrate only the target file's root and member"
    );
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
        untouched.rows()[0].get::<String>("scalar_json").unwrap(),
        r#""second""#
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_json_entity_write_rollback_keeps_original_bytes_and_actor() {
    let archive = build_json_plugin_archive();
    let lix = open_lix().await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;
    let path = "/entity-rollback.json";
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
                Value::Text(r#""rolled-back""#.to_string()),
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
        &[Value::Text(r#""after""#.to_string()), Value::Text(file_id)],
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
    let second = first.open_session().await.unwrap();
    let mut first_transaction = first.begin_transaction().await.unwrap();
    let mut second_transaction = second.begin_transaction().await.unwrap();
    for (transaction, value) in [
        (&mut first_transaction, r#""first""#),
        (&mut second_transaction, r#""second""#),
    ] {
        transaction
            .execute(
                "UPDATE json_object_member SET scalar_json = $1 \
                 WHERE parent_id = 'root' AND key = 'value' AND lixcol_file_id = $2",
                &[Value::Text(value.to_owned()), Value::Text(file_id.clone())],
            )
            .await
            .unwrap();
    }

    first.reset_plugin_transition_counters();
    first_transaction.commit().await.unwrap();
    second_transaction
        .commit()
        .await
        .expect("stale plugin overlap should resolve at commit");
    let counters = first.plugin_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, 1);
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
    let second = first.open_session().await.unwrap();
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

    first.reset_plugin_transition_counters();
    first_transaction.commit().await.unwrap();
    second_transaction.commit().await.unwrap();
    assert_eq!(
        first.plugin_transition_counters().conflict_resolution_calls,
        0
    );
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
    let winner_client = stale_client.open_session().await.unwrap();
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

    stale_client.reset_plugin_transition_counters();
    winner.commit().await.unwrap();
    stale_client.reset_plugin_transition_counters();
    stale.commit().await.unwrap();
    let counters = stale_client.plugin_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, 1);
    assert_eq!(counters.guest_export_calls, 3);
    assert_eq!(counters.durable_semantic_changes, 2);
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
    let winner_client = stale_client.open_session().await.unwrap();
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
    stale_client.reset_plugin_transition_counters();
    stale.commit().await.unwrap();
    let counters = stale_client.plugin_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, CONFLICTS as u64);
    assert_eq!(counters.guest_export_calls, 3);
    assert_eq!(counters.durable_semantic_changes, CONFLICTS as u64);
    assert_eq!(
        read_file(&stale_client, path).await.unwrap(),
        read_file(&winner_client, path).await.unwrap()
    );
    winner_client.close().await.unwrap();
    stale_client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "release-only stale plugin replay benchmark"]
async fn stale_plugin_replay_batch_benchmark_probe() {
    let conflicts = std::env::var("LIX_STALE_REPLAY_BENCH_CONFLICTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(64);
    let rounds = std::env::var("LIX_STALE_REPLAY_BENCH_ROUNDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(8);
    let archive = build_json_plugin_archive();
    let mut samples = Vec::with_capacity(rounds);
    let mut guest_export_calls = Vec::with_capacity(rounds);
    for round in 0..rounds {
        let stale_client = open_lix().await.unwrap();
        install_reference_plugin_in_blank_registry(
            &stale_client,
            "plugin_json",
            &archive,
            &["json_root", "json_object_member", "json_array_item"],
        )
        .await;
        let path = format!("/batched-transaction-conflict-{round}.json");
        let document = |value: &str| {
            serde_json::Value::Object(
                (0..conflicts)
                    .map(|index| (format!("key-{index:04}"), serde_json::json!(value)))
                    .collect(),
            )
        };
        write_file(
            &stale_client,
            &path,
            serde_json::to_vec(&document("base")).unwrap(),
        )
        .await
        .unwrap();
        let winner_client = stale_client.open_session().await.unwrap();
        let mut stale = stale_client.begin_transaction().await.unwrap();
        let mut winner = winner_client.begin_transaction().await.unwrap();
        for (transaction, value) in [(&mut stale, "stale"), (&mut winner, "winner")] {
            transaction
                .execute(
                    "UPDATE lix_file SET content = $1 WHERE path = $2",
                    &[
                        Value::Blob(serde_json::to_vec(&document(value)).unwrap().into()),
                        Value::Text(path.clone()),
                    ],
                )
                .await
                .unwrap();
        }
        winner.commit().await.unwrap();
        stale_client.reset_plugin_transition_counters();
        let started = Instant::now();
        stale.commit().await.unwrap();
        samples.push(started.elapsed());
        let counters = stale_client.plugin_transition_counters();
        assert_eq!(counters.conflict_resolution_calls, 1);
        assert_eq!(counters.conflict_resolution_records, conflicts as u64);
        assert_eq!(counters.durable_semantic_changes, conflicts as u64);
        guest_export_calls.push(counters.guest_export_calls);
        assert_eq!(
            read_file(&stale_client, &path).await.unwrap(),
            read_file(&winner_client, &path).await.unwrap()
        );
        winner_client.close().await.unwrap();
        stale_client.close().await.unwrap();
    }
    samples.sort_unstable();
    guest_export_calls.sort_unstable();
    let percentile = |values: &[Duration], percentile: usize| {
        values[(values.len() - 1).saturating_mul(percentile) / 100]
    };
    println!(
        "{}",
        serde_json::json!({
            "schema": "lix.stale-plugin-replay.v1",
            "conflicts": conflicts,
            "rounds": rounds,
            "commit_p50_us": percentile(&samples, 50).as_micros(),
            "commit_p95_us": percentile(&samples, 95).as_micros(),
            "guest_export_calls_min": guest_export_calls[0],
            "guest_export_calls_max": guest_export_calls[guest_export_calls.len() - 1],
        })
    );
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
        let second = first.open_session().await.unwrap();
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

        first.reset_plugin_transition_counters();
        first_transaction.commit().await.unwrap();
        first.reset_plugin_transition_counters();
        second_transaction
            .commit()
            .await
            .unwrap_or_else(|error| panic!("{extension} overlap should resolve: {error}"));
        let counters = first.plugin_transition_counters();
        assert!(
            counters.conflict_resolution_calls > 0,
            "{extension} must invoke its conflict resolver"
        );
        assert!(
            counters.conflict_resolution_records >= 2,
            "{extension} must resolve the multi-entity fixture as one batch"
        );
        assert_eq!(
            counters.guest_export_calls, 3,
            "{extension} must use one resolver and one render transition"
        );
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
async fn v2_json_rejects_mixed_byte_and_entity_transitions_in_one_transaction() {
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
            &[Value::Text(r#""entity""#.to_string()), Value::Text(file_id)],
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
    lix.reset_plugin_transition_counters();
    write_file(&lix, path, geometry_edit.clone()).await.unwrap();
    let counters = lix.plugin_transition_counters();
    assert_eq!(counters.full_state_semantic_rows_materialized, 0);
    assert_eq!(counters.durable_semantic_changes, 1);

    let element = lix
        .execute(
            "SELECT element_json FROM excalidraw_element WHERE id = 'b'",
            &[],
        )
        .await
        .unwrap();
    let element_json = element.rows()[0]
        .get::<String>("element_json")
        .unwrap()
        .replacen(r#""isDeleted":false"#, r#""isDeleted":true"#, 1);
    lix.execute(
        "UPDATE excalidraw_element \
         SET element_json = $1, is_deleted = $2 \
         WHERE id = 'b'",
        &[Value::Text(element_json), Value::Boolean(true)],
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

    let first_element = lix
        .execute(
            "SELECT element_json FROM excalidraw_element WHERE id = 'a'",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("element_json")
        .unwrap()
        .replacen(r#""x":123.5"#, r#""x":123456.75"#, 1);
    lix.execute(
        "UPDATE excalidraw_element SET element_json = $1 WHERE id = 'a'",
        &[Value::Text(first_element)],
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
            .get::<String>("element_json")
            .unwrap()
            .contains("123456.75")
    );
    assert!(
        elements.rows()[1]
            .get::<String>("element_json")
            .unwrap()
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
    reopened.reset_plugin_transition_counters();
    let cold = br#"{"type":"excalidraw","version":2,"source":"test","elements":[{"id":"a","type":"rectangle","x":10,"y":2,"width":3,"height":4,"isDeleted":false}],"appState":{},"files":{}}"#.to_vec();
    write_file(&reopened, path, cold.clone()).await.unwrap();
    let counters = reopened.plugin_transition_counters();
    assert_eq!(
        counters.guest_export_calls, 1,
        "cold Excalidraw reconciliation must not hydrate and re-enter the guest"
    );
    assert_eq!(counters.full_state_semantic_rows_materialized, 0);
    assert_eq!(counters.private_document_cache_hits, 1);
    assert_eq!(counters.full_document_reparses, 0);
    assert_eq!(counters.durable_semantic_changes, 1);
    assert_eq!(read_file(&reopened, path).await.unwrap(), Some(cold));

    // The cold successor must publish spans for its own bytes. A following
    // localized edit would address the wrong range if it inherited the
    // predecessor's index.
    let warm = br#"{"type":"excalidraw","version":2,"source":"test","elements":[{"id":"a","type":"rectangle","x":100,"y":2,"width":3,"height":4,"isDeleted":false}],"appState":{},"files":{}}"#.to_vec();
    reopened.reset_plugin_transition_counters();
    write_file(&reopened, path, warm.clone()).await.unwrap();
    assert_eq!(
        reopened
            .plugin_transition_counters()
            .durable_semantic_changes,
        1
    );
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
    lix.reset_plugin_transition_counters();
    write_file(&lix, path, before.clone()).await.unwrap();
    let open_counters = lix.plugin_transition_counters();
    assert_eq!(open_counters.guest_export_calls, 1);
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
    lix.reset_plugin_transition_counters();
    write_file(&lix, path, after.clone()).await.unwrap();
    let successor_counters = lix.plugin_transition_counters();
    assert_eq!(successor_counters.guest_export_calls, 1);
    assert_eq!(successor_counters.durable_semantic_changes, 1);
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(after.clone()));
    assert!(
        lix.execute(
            "SELECT element_json FROM excalidraw_element WHERE id = 'a'",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
            .get::<String>("element_json")
            .unwrap()
            .contains("123.5")
    );
    assert!(
        lix.execute(
            "SELECT element_json FROM excalidraw_element_history() \
             WHERE id = 'a' AND lixcol_depth = 1",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
            .get::<String>("element_json")
            .unwrap()
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

// A space id has exactly one value semantics, declared once in the engine
// registry. These read it back instead of restating id, name and semantics.
fn certified_entity_batch_space() -> StorageSpace {
    lix::storage_bench::storage_space_by_name("hot_state.certified_entity_batch.v1")
}

fn certified_entity_batch_page_space() -> StorageSpace {
    lix::storage_bench::storage_space_by_name("hot_state.certified_entity_batch_page.v1")
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

    let contents = storage_space_entries(storage, certified_entity_batch_space()).await;
    assert!(
        !contents.is_empty(),
        "writer must publish a certified batch"
    );
    assert!(
        contents.iter().all(|(_, value)| value.starts_with(b"CEB2")),
        "current writers must emit only CEB2"
    );
    assert!(
        !storage_space_entries(storage, certified_entity_batch_page_space())
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
    let mut pages = storage_space_entries(storage, certified_entity_batch_page_space()).await;
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
            certified_entity_batch_page_space(),
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
        error.to_string().contains("certified entity batch"),
        "unexpected CEB2 corruption error: {error}"
    );
    lix.close().await.expect("close corrupt CEB2 workspace");
}

#[tokio::test]
async fn v3_ceb2_roundtrip_corruption_and_reopen_memory() {
    let storage = lix::Memory::new();
    write_and_verify_ceb2_fixture(&storage).await;
    verify_reopened_ceb2_fixture(&storage).await;
    corrupt_first_ceb2_page(&storage).await;
    verify_corrupt_ceb2_fails_closed(&storage).await;
}

#[tokio::test]
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
            black_box(result.rows()[0].get::<String>("element_json").unwrap());
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
#[ignore = "large Excalidraw local-edit v3 transition benchmark"]
async fn v3_excalidraw_large_transition_benchmark() {
    const ELEMENTS: usize = 20_000;
    const PATH: &str = "/large.excalidraw";
    const BENCHMARK: &str = "v3_excalidraw_large_transition_benchmark";
    let samples = std::env::var("LIX_BENCH_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|samples| *samples > 0)
        .unwrap_or(3);
    let mut source = String::from(
        r#"{"type":"excalidraw","version":2,"source":"https://excalidraw.com","elements":["#,
    );
    for index in 0..ELEMENTS {
        if index != 0 {
            source.push(',');
        }
        source.push_str(&format!(
            r#"{{"id":"e-{index}","type":"rectangle","x":1.25,"y":2,"width":100,"height":80,"isDeleted":false}}"#
        ));
    }
    source.push_str(r##"],"appState":{"viewBackgroundColor":"#ffffff"},"files":{}}"##);
    let before = source.into_bytes();
    let after = String::from_utf8(before.clone())
        .unwrap()
        .replacen(
            r#""id":"e-10000","type":"rectangle","x":1.25"#,
            r#""id":"e-10000","type":"rectangle","x":123.5"#,
            1,
        )
        .into_bytes();
    assert_eq!(before.len() + 1, after.len());
    let collector = PerfSpanCollector::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(collector.clone()));
    let _dispatcher = tracing::dispatcher::set_default(&dispatch);
    let fixture = BenchmarkFixture {
        input_bytes: after.len(),
        logical_rows: ELEMENTS,
    };
    for (label, plugin_key, archive) in [(
        "v3_push_sink",
        "plugin_excalidraw",
        build_excalidraw_plugin_archive(),
    )] {
        if std::env::var("LIX_BENCH_LANE").is_ok_and(|lane| lane != label) {
            continue;
        }
        let mut measurements = Vec::with_capacity(samples);
        for sample in 0..samples {
            let root = tempfile::tempdir().expect("create Excalidraw benchmark directory");
            let lix = open_rocksdb_lix(root.path()).await;
            install_reference_plugin_in_blank_registry(
                &lix,
                plugin_key,
                &archive,
                &["excalidraw_scene", "excalidraw_element", "excalidraw_file"],
            )
            .await;
            write_file(&lix, PATH, before.clone()).await.unwrap();

            lix.reset_plugin_transition_counters();
            collector.clear();
            let allocation_scope = AllocationScope::start();
            let started = Instant::now();
            write_file(&lix, PATH, after.clone()).await.unwrap();
            let measurement =
                BenchmarkMeasurement::new(started.elapsed(), allocation_scope.finish());
            let counters = lix.plugin_transition_counters();
            assert_eq!(read_file(&lix, PATH).await.unwrap(), Some(after.clone()));
            assert_eq!(
                lix.execute("SELECT COUNT(*) AS count FROM excalidraw_element", &[])
                    .await
                    .unwrap()
                    .rows()[0]
                    .get::<i64>("count")
                    .unwrap(),
                ELEMENTS as i64
            );
            assert_eq!(counters.durable_semantic_changes, 1);
            assert_eq!(counters.guest_export_calls, 1);
            eprintln!(
                "large_excalidraw lane={label} sample={sample} input_mb={:.3} \
                 elapsed_ms={:.3} allocations={} allocated_mb={:.3} peak_live_mb={:.3} \
                 guest_exports={} imports={} boundary_mb={:.3} guest_high_water_mb={:.3} \
                 phase_close_live_bytes={:?} phases_ms={:?}",
                after.len() as f64 / 1_000_000.0,
                measurement.elapsed_ms,
                measurement.allocations.allocation_count,
                measurement.allocations.allocated_bytes as f64 / 1_000_000.0,
                measurement.allocations.peak_live_bytes_delta as f64 / 1_000_000.0,
                counters.guest_export_calls,
                counters.component_import_calls,
                counters.component_boundary_bytes as f64 / 1_000_000.0,
                counters.guest_linear_memory_high_water_bytes as f64 / 1_000_000.0,
                collector.take_close_live_bytes(),
                collector.take_aggregate_millis(),
            );
            emit_sample(
                BENCHMARK,
                label,
                sample,
                fixture,
                BenchmarkGate::ElapsedRegression,
                measurement,
            );
            emit_transition_profile(
                BENCHMARK,
                label,
                sample,
                counters,
                serde_json::json!({
                    "before_sha256": sha256_lower_hex(&before),
                    "file_sha256": sha256_lower_hex(&after),
                    "file_bytes": after.len(),
                    "entity_rows": ELEMENTS,
                    "semantic_changes": 1
                }),
            );
            measurements.push(measurement);
            lix.close().await.unwrap();
        }
        emit_summary(
            BENCHMARK,
            label,
            fixture,
            BenchmarkGate::ElapsedRegression,
            &measurements,
        );
        let mut elapsed_ms = measurements
            .iter()
            .map(|measurement| measurement.elapsed_ms)
            .collect::<Vec<_>>();
        elapsed_ms.sort_by(f64::total_cmp);
        eprintln!(
            "large_excalidraw lane={label} raw_ms={elapsed_ms:?} p50_ms={:.3}",
            elapsed_ms[elapsed_ms.len() / 2]
        );
    }
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
        .get::<String>("element_json")
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

    let target_json = original.replacen(r#""x":1"#, r#""x":111"#, 1);
    lix.execute(
        "UPDATE excalidraw_element SET element_json = $1 \
         WHERE id = 'shape' AND lixcol_file_id = $2",
        &[Value::Text(target_json), Value::Text(file_id.clone())],
    )
    .await
    .expect("target element edit should commit");
    let target_order = excalidraw_v2_element_ordering(&lix, &file_id, "shape").await;

    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    let source_json = original.replacen(r#""x":1"#, r#""x":222"#, 1);
    lix.execute(
        "UPDATE excalidraw_element SET element_json = $1 \
         WHERE id = 'shape' AND lixcol_file_id = $2",
        &[Value::Text(source_json), Value::Text(file_id.clone())],
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

    lix.reset_plugin_transition_counters();
    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("same-element Excalidraw conflict should resolve deterministically");
    let rendered: serde_json::Value =
        serde_json::from_slice(&read_file(&lix, path).await.unwrap().unwrap()).unwrap();
    assert_eq!(rendered["elements"][0]["x"], serde_json::json!(expected_x));
    let counters = lix.plugin_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, 1);
    assert_eq!(counters.conflict_resolution_takes, 1);

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
        .get::<String>("element_json")
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

    let target_json = original.replacen(r#""x":1"#, r#""x":111"#, 1);
    lix.execute(
        "UPDATE excalidraw_element SET element_json = $1 \
         WHERE id = 'shape' AND lixcol_file_id = $2",
        &[Value::Text(target_json), Value::Text(file_id.clone())],
    )
    .await
    .expect("target element edit should commit");
    let target_order = excalidraw_v2_element_ordering(&lix, &file_id, "shape").await;

    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    let source_json = original.replacen(r#""x":1"#, r#""x":222"#, 1);
    lix.execute(
        "UPDATE excalidraw_element SET element_json = $1 \
         WHERE id = 'shape' AND lixcol_file_id = $2",
        &[Value::Text(source_json), Value::Text(file_id.clone())],
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

    lix.reset_plugin_transition_counters();
    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("same-element v3 Excalidraw conflict should resolve deterministically");
    let rendered: serde_json::Value =
        serde_json::from_slice(&read_file(&lix, path).await.unwrap().unwrap()).unwrap();
    assert_eq!(rendered["elements"][0]["x"], serde_json::json!(expected_x));
    let counters = lix.plugin_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, 1);
    assert_eq!(counters.conflict_resolution_takes, 1);

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_create_reservations_survive_restart_and_tombstone_with_file() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive = build_csv_plugin_archive();
    let path = "/durable-ids.csv";

    let lix = open_filesystem_lix(tempdir.path()).await;
    install_plugin(&lix, "plugin_csv", &archive).await.unwrap();
    write_file(&lix, path, b"first,one\n".to_vec())
        .await
        .unwrap();
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
    let inserted_identity = MutationIdentity {
        namespace_seed: uuid::Uuid::parse_str("01920000-0000-7000-8000-000000000031")
            .expect("fixture UUIDv7")
            .into_bytes(),
        operation_proof: [0x41; 32],
    };
    write_file_with_mutation_identity(
        &lix,
        path,
        b"first,one\nsecond,two\n".to_vec(),
        inserted_identity,
    )
    .await
    .unwrap();
    lix.close().await.unwrap();

    let lix = open_filesystem_lix(tempdir.path()).await;
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(b"first,one\nsecond,two\n".to_vec())
    );
    write_file_with_mutation_identity(
        &lix,
        path,
        b"first,one\nsecond,two\n".to_vec(),
        inserted_identity,
    )
    .await
    .expect("an exact same-proof retry after reopen should be accepted");

    let collision = write_file_with_mutation_identity(
        &lix,
        path,
        b"first,one\nsecond,two\nthird,three\n".to_vec(),
        MutationIdentity {
            namespace_seed: inserted_identity.namespace_seed,
            operation_proof: [0x42; 32],
        },
    )
    .await
    .expect_err("a reused namespace seed with a different proof must fail after restart");
    assert_eq!(
        collision.code,
        LixError::CODE_CONSTRAINT_VIOLATION,
        "unexpected namespace-collision error: {collision:?}"
    );
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(b"first,one\nsecond,two\n".to_vec())
    );
    lix.execute(
        "DELETE FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_string())],
    )
    .await
    .unwrap();
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
    let storage_a = LocalFilesystem::open(tempdir.path())
        .await
        .expect("first shared filesystem storage opens");
    let lix_a = open_lix()
        .with_storage(storage_a)
        .await
        .expect("first independent Lix engine opens");
    let archive = build_csv_plugin_archive();
    install_plugin(&lix_a, "plugin_csv", &archive)
        .await
        .unwrap();

    let path = "/cross-engine-root.csv";
    let initial = b"first,one\nsecond,two\n".to_vec();
    write_file(&lix_a, path, initial.clone()).await.unwrap();
    assert_eq!(
        read_file(&lix_a, path).await.unwrap(),
        Some(initial.clone())
    );

    // A separately opened Lix owns a distinct plugin runtime/actor cache while
    // sharing the same durable RocksDB-backed workspace.
    let storage_b = LocalFilesystem::open(tempdir.path())
        .await
        .expect("second shared filesystem storage opens");
    let lix_b = open_lix()
        .with_storage(storage_b)
        .await
        .expect("second independent Lix engine opens");
    assert_eq!(read_file(&lix_b, path).await.unwrap(), Some(initial));
    let advanced = b"first,ONE\nsecond,two\n".to_vec();
    write_file(&lix_b, path, advanced.clone()).await.unwrap();

    // Engine A still owns the root-old actor. Its exact SQL read returns the
    // durable materialized bytes without hydrating Wasm; the next write
    // cold-opens root-new and replaces only that captured stale slot.
    lix_a.reset_plugin_transition_counters();
    assert_eq!(
        read_file(&lix_a, path).await.unwrap(),
        Some(advanced.clone())
    );
    let counters = lix_a.plugin_transition_counters();
    assert_eq!(counters.full_state_semantic_rows_materialized, 0);
    assert_eq!(counters.full_renderer_invocations, 0);

    let final_bytes = b"first,ONE\nsecond,TWO\n".to_vec();
    lix_a.reset_plugin_transition_counters();
    write_file(&lix_a, path, final_bytes.clone())
        .await
        .expect("the next write restores root-new authority and applies the sparse edit");
    let counters = lix_a.plugin_transition_counters();
    assert_eq!(
        counters.full_state_semantic_rows_materialized, 0,
        "the durable checkpoint avoids materializing the table and row entities"
    );
    assert_eq!(counters.full_renderer_invocations, 0);
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
    let stale = lix.open_session().await.unwrap();
    assert_eq!(read_file(&stale, path).await.unwrap(), Some(old_bytes));

    lix.execute(
        "DELETE FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_string())],
    )
    .await
    .unwrap();
    let new_bytes = b"new,incarnation\n".to_vec();
    write_file(&lix, path, new_bytes.clone()).await.unwrap();
    let new_file_id = file_id_at_path(&lix, path).await;
    assert_ne!(old_file_id, new_file_id);

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

    let stale = lix.open_session().await.unwrap();
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
    let fresh = lix.open_session().await.unwrap();
    assert_eq!(read_file(&fresh, path).await.unwrap(), Some(bytes));
    fresh.reset_plugin_transition_counters();
    write_file(&fresh, path, b"first,ONE\nsecond,two\n".to_vec())
        .await
        .expect("the retained authoritative generation should remain writable");
    let counters = fresh.plugin_transition_counters();
    assert!(
        counters.full_state_semantic_rows_materialized > 0,
        "a predecessor-generation checkpoint must fall back to durable entity hydration"
    );
    assert_eq!(counters.private_document_cache_hits, 0);
    assert_eq!(counters.full_document_reparses, 1);

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
    let file_id = file_id_at_path(&lix, path).await;
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

    let preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .expect("generation-divergent merge should still preview");
    assert!(preview.conflicts.iter().any(|conflict| {
        conflict.schema_key == "lix_binary_blob_ref"
            && conflict.file_id.as_deref() == Some(file_id.as_str())
    }));

    lix.reset_plugin_transition_counters();
    let error = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .expect_err(
            "derived bytes must not be rendered by a different generation than is committed",
        );
    assert_eq!(error.code, LixError::CODE_MERGE_CONFLICT);
    let counters = lix.plugin_transition_counters();
    assert_eq!(
        counters.conflict_resolution_calls, 0,
        "disjoint rows are not a resolver decision; the generation boundary stays visible"
    );
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
    let stale = lix.open_session().await.unwrap();
    assert_eq!(
        read_file(&stale, before_path).await.unwrap(),
        Some(initial.clone())
    );

    // A path-only UPDATE is ordinary SQL. Its DML source reads the exact
    // materialized bytes and establishes the observation needed for the warm
    // empty-splice descriptor transition.
    let renamer = lix.open_session().await.unwrap();
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
    let active_row_rows = lix
        .execute(
            "SELECT lixcol_file_id FROM csv_row WHERE lixcol_file_id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(active_table_rows.len() + active_row_rows.len(), 0);
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
            "SELECT lixcol_entity_pk, id, order_key, cells FROM csv_row \
             WHERE lixcol_file_id = $1",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .unwrap();
    let mut rows = rows
        .rows()
        .iter()
        .map(|row| {
            let entity_pk = row
                .get::<serde_json::Value>("lixcol_entity_pk")
                .unwrap()
                .as_array()
                .cloned()
                .expect("csv_row entity_pk must be an array");
            let id = row.get::<String>("id").unwrap();
            assert_eq!(
                entity_pk,
                vec![serde_json::Value::String(id.clone())],
                "csv_row snapshot identity must equal its durable primary key"
            );
            CsvV2Row {
                id,
                order_key: row.get::<String>("order_key").unwrap(),
                cells: row
                    .get::<serde_json::Value>("cells")
                    .unwrap()
                    .as_array()
                    .expect("csv_row snapshot must have cells")
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

async fn open_filesystem_lix(path: &Path) -> Lix<LocalFilesystem> {
    let storage = LocalFilesystem::open(path).await.unwrap();
    open_lix().with_storage(storage).await.unwrap()
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
        "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
        &[Value::Text(
            r#"{"x-lix-key":"write_owner_task","x-lix-primary-key":["/id"],"type":"object","properties":{"id":{"type":"string","x-lix-default":"lix_uuid_v7()"},"title":{"type":"string"}},"required":["id","title"],"additionalProperties":false}"#.to_string(),
        )],
    )
    .await
    .expect("register generated-default write-owner schema");
    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1)), (lix_json($2))",
        &[
            Value::Text(
                r#"{"x-lix-key":"write_owner_parent","x-lix-primary-key":["/id"],"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false}"#.to_string(),
            ),
            Value::Text(
                r#"{"x-lix-key":"write_owner_child","x-lix-primary-key":["/id"],"x-lix-foreign-keys":[{"properties":["/parent_id"],"references":{"schemaKey":"write_owner_parent","properties":["/id"]}}],"type":"object","properties":{"id":{"type":"string"},"parent_id":{"type":"string"}},"required":["id","parent_id"],"additionalProperties":false}"#.to_string(),
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
            &[Value::Text(stale_key)],
        )
        .await
        .expect("publish winner owner");
    assert_eq!(
        stale
            .commit()
            .await
            .expect_err("stale owner must conflict")
            .code,
        LixError::CODE_UNIQUE
    );
}

#[tokio::test]
async fn lix_owned_sql_write_semantics_rocksdb_reopen() {
    let root = tempfile::tempdir().expect("create SQL write-owner RocksDB directory");
    let lix = open_rocksdb_lix(root.path()).await;
    qualify_lix_owned_sql_write_semantics(&lix, "rocks").await;
    let winner = lix
        .open_session()
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
        .open_session()
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

async fn write_file_with_mutation_identity<StorageImpl>(
    lix: &Lix<StorageImpl>,
    path: &str,
    data: Vec<u8>,
    mutation_identity: MutationIdentity,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute_with_options_and_metadata(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        &[Value::Text(path.to_string()), Value::Blob(data.into())],
        ExecuteOptions::default(),
        ExecuteStatementMetadata {
            mutation_identity: Some(mutation_identity),
            ..ExecuteStatementMetadata::default()
        },
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
        schema_keys.push(schema["x-lix-key"].as_str().unwrap().to_string());
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
    scalar_json: String,
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
    // Deliberately register only the public JSON entity surfaces. The direct
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
                 VALUES (lix_json($1), false, false)",
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
                     VALUES (lix_json($1), false, false)",
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
            Value::Json(serde_json::json!({
                "delimiter": ",",
                "quote": "\"",
                "terminator": "\n",
            }).into()),
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
                    params.push(Value::Json(serde_json::json!([
                        if *index < LONG_ROW_COUNT {
                            "000000000000000"
                        } else {
                            "00000000000000"
                        },
                        "1111111111",
                        "2222222222",
                        "3333333333",
                    ]).into()));
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
            let scalar_json =
                serde_json::to_string(scalar).expect("flat JSON fixture scalar must serialize");
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
                    params.push(Value::Text(member.scalar_json.clone()));
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
