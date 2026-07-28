use lix_rocksdb_storage::RocksDB;
use lix_sdk::{
    CreateBranchOptions, ExecuteOptions, ExecuteStatementMetadata, Lix, LixError,
    MergeBranchOptions, MergeBranchPreviewOptions, MergeConflictChangeKind, MutationIdentity,
    RequestBlobSpliceProvenance, Storage, SwitchBranchOptions, VerifiedRequestBlob,
    WasmComponentV2Factory, WasmLimits, WasmRuntime, WasmTransitionCounters,
};
use lix_sdk::{LocalFilesystem, open_lix_with_storage};
use lix_sdk::{OpenLixOptions, Value, open_lix};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::io::{Cursor, Read, Write};
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

#[derive(Clone, Default)]
struct PerfSpanCollector {
    samples: Arc<Mutex<Vec<PerfSpanSample>>>,
}

#[derive(Debug)]
struct PerfSpanSample {
    name: &'static str,
    elapsed: Duration,
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
            | "lix.perf.transaction_prepare_rows"
            | "lix.perf.transaction_path_preflight"
            | "lix.perf.transaction_buffer_stage"
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
            });
    }
}

#[derive(Default)]
struct HistoryRejectingRuntime {
    compile_calls: AtomicUsize,
}

#[async_trait::async_trait]
impl WasmRuntime for HistoryRejectingRuntime {
    async fn compile_component_v2(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentV2Factory>, LixError> {
        self.compile_calls.fetch_add(1, Ordering::SeqCst);
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "file history must not execute a plugin",
        ))
    }
}

#[tokio::test]
async fn v2_file_history_reads_durable_materialized_bytes_without_plugin_execution() {
    let storage = lix_sdk::Memory::new();
    let lix = open_lix(OpenLixOptions::new(storage.clone()))
        .await
        .expect("workspace should open with the production runtime");
    let archive = build_csv_v2_plugin_archive();
    install_plugin(&lix, "plugin_csv_v2", &archive)
        .await
        .expect("CSV v2 plugin should install");

    let path = "/history-materialized.csv";
    let first = b"name,value\nrow,first\n".to_vec();
    let second = b"name,value\nrow,second\n".to_vec();
    write_file(&lix, path, first.clone())
        .await
        .expect("initial plugin file should materialize");
    let file_id = file_id_at_path(&lix, path).await;
    let edited_row_id = csv_v2_row_id(&active_csv_v2_rows(&lix, &file_id).await, &["row", "first"]);
    lix.execute(
        "UPDATE csv_v2_row SET cells = $1 \
         WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Json(serde_json::json!(["row", "second"])),
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
    let history_lix = open_lix(OpenLixOptions::new(storage).with_wasm_runtime(wasm_runtime))
        .await
        .expect("workspace should reopen without compiling installed plugins");
    let result = history_lix
        .execute(
            "SELECT data, lixcol_depth \
             FROM lix_file_history \
             WHERE lixcol_as_of_commit_id = $1 AND id = $2 \
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
async fn v2_csv_blob_api_preserves_multiplayer_authority_and_rollback() {
    let archive = build_csv_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv_v2",
        &archive,
        &["csv_v2_table", "csv_v2_row"],
    )
    .await;

    let path = "/multiplayer.csv";
    let initial = b"first,one\nsecond,two\nthird,three\n".to_vec();
    write_file(&lix, path, initial.clone()).await.unwrap();
    let file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();
    assert_eq!(plugin_namespace_reservation_count(&lix, &file_id).await, 1);

    let first = lix.open_workspace_session().await.unwrap();
    let second = lix.open_workspace_session().await.unwrap();
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
    first.reset_plugin_v2_transition_counters();
    write_file(&first, path, first_edit).await.unwrap();
    let counters = first.plugin_v2_transition_counters();
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
    let lww_first = lix.open_workspace_session().await.unwrap();
    let lww_second = lix.open_workspace_session().await.unwrap();
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
    let edit_session = lix.open_workspace_session().await.unwrap();
    let delete_session = lix.open_workspace_session().await.unwrap();
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
    let blind = lix.open_workspace_session().await.unwrap();
    write_file(&blind, path, b"first,ONE\n".to_vec())
        .await
        .unwrap();
    let one_row = b"first,ONE\n".to_vec();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(one_row.clone()));

    // A rolled-back successor is discarded; the accepted actor and its exact
    // observation remain usable for a later committed transition.
    let rollback_session = lix.open_workspace_session().await.unwrap();
    assert_eq!(
        read_file(&rollback_session, path).await.unwrap(),
        Some(one_row.clone())
    );
    let mut transaction = rollback_session.begin_transaction().await.unwrap();
    transaction
        .execute(
            "UPDATE lix_file SET data = $1 WHERE path = $2",
            &[
                Value::Blob(b"first,ROLLED-BACK\ninserted,ROLLBACK\n".to_vec().into()),
                Value::Text(path.to_string()),
            ],
        )
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(one_row));
    assert_eq!(plugin_namespace_reservation_count(&lix, &file_id).await, 1);
    write_file(&rollback_session, path, b"first,COMMITTED\n".to_vec())
        .await
        .unwrap();
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(b"first,COMMITTED\n".to_vec())
    );
    assert_eq!(plugin_namespace_reservation_count(&lix, &file_id).await, 1);

    let insert_session = lix.open_workspace_session().await.unwrap();
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
    assert_eq!(plugin_namespace_reservation_count(&lix, &file_id).await, 2);

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
async fn v2_transport_splice_provenance_is_bound_to_the_observed_file() {
    let archive = build_csv_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv_v2",
        &archive,
        &["csv_v2_table", "csv_v2_row"],
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
    lix.reset_plugin_v2_transition_counters();
    lix.execute_with_options_and_metadata(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET data = excluded.data",
        &[Value::Text(path_b.to_owned()), Value::Blob(after_a_blob)],
        ExecuteOptions::default(),
        ExecuteStatementMetadata {
            parameter_blob_splices: vec![None, Some(provenance_from_a)],
            ..ExecuteStatementMetadata::default()
        },
    )
    .await
    .unwrap();
    let counters = lix.plugin_v2_transition_counters();
    assert!(
        counters.host_full_diff_bytes_compared > 0,
        "cross-file provenance must use the safe full-diff fallback"
    );
    assert_eq!(
        read_file(&lix, path_b).await.unwrap(),
        Some(after_a.clone())
    );
    let expected_b_rows = active_csv_v2_rows(&lix, &file_b_id).await;
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
        active_csv_v2_rows(&lix, &file_a_id).await[0].cells,
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
    assert_eq!(active_csv_v2_rows(&lix, &file_b_id).await, expected_b_rows);

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_markdown_roundtrips_gfm_and_renders_one_direct_entity_edit() {
    let archive = build_markdown_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown_incremental_v2",
        &archive,
        &["markdown_node_v2"],
    )
    .await;

    let path = "/component-v2.md";
    let source = b"# Heading\n\nParagraph with **bold** text.\n".to_vec();
    write_file(&lix, path, source.clone()).await.unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(source));

    let nodes = lix
        .execute(
            "SELECT id, kind, payload_json FROM markdown_node_v2 ORDER BY kind",
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
            .all(|row| row.get::<String>("id").is_ok_and(|id| id.len() == 32)),
        "every Markdown v2 node, including the document root, must use the host namespace"
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
    assert_eq!(paragraph_id.len(), 32);

    let payload_json =
        serde_json::json!({"inline":[{"type":"text","value":"Edited paragraph."}]}).to_string();
    lix.execute(
        "UPDATE markdown_node_v2 SET payload_json = $1 WHERE id = $2",
        &[Value::Text(payload_json), Value::Text(paragraph_id)],
    )
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, path).await.unwrap().as_deref(),
        Some(b"# Heading\n\nEdited paragraph.\n".as_slice())
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_markdown_merges_unrelated_entities_and_regenerates_derived_bytes() {
    let archive = build_markdown_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown_incremental_v2",
        &archive,
        &["markdown_node_v2"],
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
            "SELECT id, payload_json FROM markdown_node_v2 WHERE kind = 'paragraph'",
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
            id: Some("markdown-derived-blob-source".to_owned()),
            name: "Markdown derived blob source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    lix.execute(
        "UPDATE markdown_node_v2 SET payload_json = $1 WHERE id = $2",
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
        "UPDATE markdown_node_v2 SET payload_json = $1 WHERE id = $2",
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
    let archive = build_markdown_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_markdown_incremental_v2",
        &archive,
        &["markdown_node_v2"],
    )
    .await;

    let path = "/paragraph-conflict.md";
    write_file(&lix, path, b"wonder\n".to_vec())
        .await
        .expect("base paragraph should import");
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("markdown-paragraph-conflict-source".to_owned()),
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

    lix.reset_plugin_v2_transition_counters();
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
    let counters = lix.plugin_v2_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, 1);
    assert_eq!(
        counters.conflict_resolution_takes, 0,
        "the composed paragraph is one replacement, not a side selection"
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_same_row_branch_merge_composes_distinct_cells() {
    let archive = build_csv_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv_v2",
        &archive,
        &["csv_v2_table", "csv_v2_row"],
    )
    .await;

    let path = "/row-conflict.csv";
    write_file(&lix, path, b"alpha,one,red\n".to_vec())
        .await
        .expect("base row should import");
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("csv-row-conflict-source".to_owned()),
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

    lix.reset_plugin_v2_transition_counters();
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
    let counters = lix.plugin_v2_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, 1);
    assert_eq!(
        counters.conflict_resolution_takes, 0,
        "the composed row is one replacement, not a side selection"
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_same_cell_merge_uses_canonical_stored_rank() {
    let archive = build_csv_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv_v2",
        &archive,
        &["csv_v2_table", "csv_v2_row"],
    )
    .await;

    let path = "/row-canonical-fallback-conflict.csv";
    let base = b"alpha,one,red\n".to_vec();
    let target_bytes = b"TARGET,one,red\n".to_vec();
    let source_bytes = b"SOURCE,one,red\n".to_vec();
    write_file(&lix, path, base).await.unwrap();
    let file_id = file_id_at_path(&lix, path).await;
    let row_id = csv_v2_row_id(
        &active_csv_v2_rows(&lix, &file_id).await,
        &["alpha", "one", "red"],
    );
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("csv-row-canonical-b-source".to_owned()),
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
    let source_order = csv_v2_row_ordering(&lix, &file_id, &row_id).await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id.clone(),
    })
    .await
    .unwrap();
    write_file(&lix, path, target_bytes.clone())
        .await
        .expect("target same-cell edit should commit");
    let target_order = csv_v2_row_ordering(&lix, &file_id, &row_id).await;
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

    lix.reset_plugin_v2_transition_counters();
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
    let counters = lix.plugin_v2_transition_counters();
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
    let archive = build_csv_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv_v2",
        &archive,
        &["csv_v2_table", "csv_v2_row"],
    )
    .await;

    let path = "/delete-vs-edit.csv";
    write_file(&lix, path, b"alpha,one,red\n".to_vec())
        .await
        .expect("base CSV should import");
    let file_id = file_id_at_path(&lix, path).await;
    let row_id = csv_v2_row_id(
        &active_csv_v2_rows(&lix, &file_id).await,
        &["alpha", "one", "red"],
    );
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("csv-delete-vs-edit-source".to_owned()),
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
        "UPDATE csv_v2_row SET cells = $1 \
         WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Json(serde_json::json!(["alpha", "ONE", "red"])),
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
    let row_conflict = preview
        .conflicts
        .iter()
        .find(|conflict| {
            conflict.schema_key == "csv_v2_row"
                && conflict.file_id.as_deref() == Some(file_id.as_str())
        })
        .expect("delete-vs-edit semantic row must remain visible before the resolver");
    assert_eq!(row_conflict.target.kind, MergeConflictChangeKind::Removed);
    assert_eq!(row_conflict.source.kind, MergeConflictChangeKind::Modified);

    lix.reset_plugin_v2_transition_counters();
    let error = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .expect_err("delete-vs-edit requires a first-class lifecycle decision");
    assert_eq!(error.code, LixError::CODE_MERGE_CONFLICT);
    let counters = lix.plugin_v2_transition_counters();
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
    let archive = build_csv_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv_v2",
        &archive,
        &["csv_v2_table", "csv_v2_row"],
    )
    .await;

    let csv_path = "/descriptor-conflict.csv";
    let tsv_path = "/descriptor-conflict.tsv";
    let base = b"alpha,one,red\n".to_vec();
    write_file(&lix, csv_path, base.clone())
        .await
        .expect("base CSV should import");
    let file_id = file_id_at_path(&lix, csv_path).await;
    let row_id = csv_v2_row_id(
        &active_csv_v2_rows(&lix, &file_id).await,
        &["alpha", "one", "red"],
    );
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("csv-rename-vs-edit-source".to_owned()),
            name: "CSV rename versus same-row edit source".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    lix.execute(
        "UPDATE csv_v2_row SET cells = $1 WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Json(serde_json::json!(["TARGET", "one", "red"])),
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
        "UPDATE csv_v2_row SET cells = $1 WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Json(serde_json::json!(["SOURCE", "one", "red"])),
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
        conflict.schema_key == "csv_v2_row" && conflict.file_id.as_deref() == Some(file_id.as_str())
    }));

    lix.reset_plugin_v2_transition_counters();
    let error = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .expect_err("a resolver must not mix target CSV bytes with source TSV metadata");
    assert_eq!(error.code, LixError::CODE_MERGE_CONFLICT);
    let counters = lix.plugin_v2_transition_counters();
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
async fn v2_json_roundtrips_recursive_state_and_keeps_leaf_edits_sparse() {
    let archive = build_json_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json_incremental_v2",
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
    lix.reset_plugin_v2_transition_counters();
    write_file(&lix, path, edited.clone()).await.unwrap();
    let counters = lix.plugin_v2_transition_counters();
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
    let archive = build_json_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json_incremental_v2",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/json-lifecycle.json";
    let initial = b"{\"left\":\"one\",\"right\":\"two\",\"gone\":\"three\"}".to_vec();
    write_file(&lix, path, initial.clone()).await.unwrap();
    let file_id = file_id_at_path(&lix, path).await;

    // Different scalar changes from the same observed document compose.
    let left_writer = lix.open_workspace_session().await.unwrap();
    let right_writer = lix.open_workspace_session().await.unwrap();
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
    let first_lww = lix.open_workspace_session().await.unwrap();
    let second_lww = lix.open_workspace_session().await.unwrap();
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
    assert_eq!(direct_structure_error.code, LixError::CODE_INVALID_PARAM);
    assert!(
        direct_structure_error
            .message
            .contains("one existing scalar value only")
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
    let direct_batch_error = lix
        .execute(
            "UPDATE json_object_member SET scalar_json = $1 \
             WHERE parent_id = 'root' AND lixcol_file_id = $2",
            &[
                Value::Text(r#""BULK""#.to_owned()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .expect_err("a direct JSON semantic transition must contain one scalar change");
    assert_eq!(direct_batch_error.code, LixError::CODE_INVALID_PARAM);
    assert!(
        direct_batch_error
            .message
            .contains("one existing scalar value only")
    );
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        Some(scalar_after_direct_reject.clone())
    );

    // Structure is byte-owned. A stale scalar delta is not allowed to
    // recreate an entity after another writer removes its containing slot.
    let stale_writer = lix.open_workspace_session().await.unwrap();
    let structure_writer = lix.open_workspace_session().await.unwrap();
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
    assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    assert!(error.message.contains("one existing scalar value only"));
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
    let archive = build_json_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default())
        .await
        .expect("workspace should open with the production Wasmtime runtime");
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json_incremental_v2",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/ten-mib.json";
    let (before, edit_offset, edited_key) = json_ten_mib_flat_fixture();
    let replacement = alternate_ascii_hex(before[edit_offset]);
    let mut after = before.clone();
    after[edit_offset] = replacement;

    lix.reset_plugin_v2_transition_counters();
    let cold_started = Instant::now();
    write_file(&lix, path, before.clone())
        .await
        .expect("real JSON v2 Wasm should import the 10 MiB fixture");
    let cold_elapsed = cold_started.elapsed();
    let cold = lix.plugin_v2_transition_counters();
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

    lix.reset_plugin_v2_transition_counters();
    let warm_engine_started = Instant::now();
    lix.execute_with_options_and_metadata(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET data = excluded.data",
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
    let warm = lix.plugin_v2_transition_counters();

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

    let root = tempfile::tempdir().expect("create JSON benchmark directory");
    let archive = build_json_v2_plugin_archive();
    let lix = open_lix_with_rocksdb(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json_incremental_v2",
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
    for sample in 0..SAMPLES {
        bytes[edit_offset] = alternate_ascii_hex(bytes[edit_offset]);
        lix.reset_plugin_v2_transition_counters();
        let started = Instant::now();
        write_file(&lix, path, bytes.clone())
            .await
            .expect("ordinary SQL full-byte JSON edit should succeed");
        elapsed_ms.push(started.elapsed().as_secs_f64() * 1_000.0);

        let counters = lix.plugin_v2_transition_counters();
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

    lix.close().await.expect("JSON benchmark should close");
}

#[tokio::test]
#[ignore = "10 MiB JSON unrelated-entity merge benchmark"]
async fn v2_json_ten_mib_unrelated_entity_merge_benchmark() {
    init_perf_tracing();
    const SAMPLES: usize = 7;

    let root = tempfile::tempdir().expect("create JSON merge benchmark directory");
    let archive = build_json_v2_plugin_archive();
    let lix = open_lix_with_rocksdb(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json_incremental_v2",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/merge-ten-mib.json";
    let (bytes, _, _) = json_ten_mib_flat_fixture();
    write_file(&lix, path, bytes)
        .await
        .expect("real JSON v2 Wasm should import the 10 MiB fixture");
    let file_id = file_id_at_path(&lix, path).await;
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let mut elapsed_ms = Vec::with_capacity(SAMPLES);

    for sample in 0..SAMPLES {
        let source = lix
            .create_branch(CreateBranchOptions {
                id: Some(format!("json-merge-source-{sample}")),
                name: format!("JSON merge source {sample}"),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let target_key = format!("property_{:06}", sample * 2);
        let source_key = format!("property_{:06}", sample * 2 + 1);
        let target_value = format!("\"target-{sample}\"");
        let source_value = format!("\"source-{sample}\"");

        lix.execute(
            "UPDATE json_object_member SET scalar_json = $1 \
             WHERE parent_id = 'root' AND key = $2 AND lixcol_file_id = $3",
            &[
                Value::Text(target_value.clone()),
                Value::Text(target_key.clone()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .expect("target JSON property should update");
        lix.switch_branch(SwitchBranchOptions {
            branch_id: source.id.clone(),
        })
        .await
        .unwrap();
        lix.execute(
            "UPDATE json_object_member SET scalar_json = $1 \
             WHERE parent_id = 'root' AND key = $2 AND lixcol_file_id = $3",
            &[
                Value::Text(source_value.clone()),
                Value::Text(source_key.clone()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .expect("source JSON property should update");
        lix.switch_branch(SwitchBranchOptions {
            branch_id: target_branch_id.clone(),
        })
        .await
        .unwrap();

        let started = Instant::now();
        lix.merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .expect("unrelated JSON properties should merge cleanly");
        elapsed_ms.push(started.elapsed().as_secs_f64() * 1_000.0);

        let merged = lix
            .execute(
                "SELECT key, scalar_json FROM json_object_member \
                 WHERE parent_id = 'root' AND key IN ($1, $2) AND lixcol_file_id = $3",
                &[
                    Value::Text(target_key),
                    Value::Text(source_key),
                    Value::Text(file_id.clone()),
                ],
            )
            .await
            .expect("merged JSON properties should query");
        let values = merged
            .rows()
            .iter()
            .map(|row| {
                (
                    row.get::<String>("key").unwrap(),
                    row.get::<String>("scalar_json").unwrap(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(values.len(), 2);
        assert!(values.values().any(|value| value == &target_value));
        assert!(values.values().any(|value| value == &source_value));
    }

    elapsed_ms.sort_by(f64::total_cmp);
    let p50_ms = elapsed_ms[elapsed_ms.len() / 2];
    let p95_index = ((elapsed_ms.len() * 95).div_ceil(100)).saturating_sub(1);
    let p95_ms = elapsed_ms[p95_index];
    eprintln!(
        "v2_json_ten_mib_unrelated_entity_merge bytes={JSON_TEN_MIB_BYTES} samples={SAMPLES} \
         p50_ms={p50_ms:.3} p95_ms={p95_ms:.3}"
    );

    lix.close().await.expect("JSON benchmark should close");
}

/// End-to-end RocksDB gate for the chosen static lazy resolver. The adjacent
/// unrelated-entity benchmark is the no-conflict merge lower bound; this test
/// changes the same tiny JSON member on both branches so it exercises one real
/// Wasm `take(b)` resolution without moving any 10 MiB file bytes through
/// the resolver.
#[tokio::test]
#[ignore = "10 MiB JSON same-entity conflict-resolution merge benchmark"]
async fn v2_json_ten_mib_same_entity_canonical_b_merge_benchmark() {
    init_perf_tracing();
    const SAMPLES: usize = 7;

    let root = tempfile::tempdir().expect("create JSON conflict benchmark directory");
    let archive = build_json_v2_plugin_archive();
    let lix = open_lix_with_rocksdb(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json_incremental_v2",
        &archive,
        &["json_root", "json_object_member", "json_array_item"],
    )
    .await;

    let path = "/merge-conflict-ten-mib.json";
    let (bytes, _, key) = json_ten_mib_flat_fixture();
    write_file(&lix, path, bytes)
        .await
        .expect("real JSON v2 Wasm should import the 10 MiB fixture");
    let file_id = file_id_at_path(&lix, path).await;
    let target_branch_id = lix.active_branch_id().await.unwrap();
    let mut elapsed_ms = Vec::with_capacity(SAMPLES);
    let mut resolver_boundary_bytes = Vec::with_capacity(SAMPLES);

    for sample in 0..SAMPLES {
        let source = lix
            .create_branch(CreateBranchOptions {
                id: Some(format!("json-conflict-merge-source-{sample}")),
                name: format!("JSON conflict merge source {sample}"),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let target_value = format!("\"target-{sample}\"");
        let source_value = format!("\"source-{sample}\"");
        lix.execute(
            "UPDATE json_object_member SET scalar_json = $1 \
             WHERE parent_id = 'root' AND key = $2 AND lixcol_file_id = $3",
            &[
                Value::Text(target_value),
                Value::Text(key.clone()),
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
             WHERE parent_id = 'root' AND key = $2 AND lixcol_file_id = $3",
            &[
                Value::Text(source_value),
                Value::Text(key.clone()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .expect("source JSON member should update");
        lix.switch_branch(SwitchBranchOptions {
            branch_id: target_branch_id.clone(),
        })
        .await
        .unwrap();

        lix.reset_plugin_v2_transition_counters();
        let started = Instant::now();
        lix.merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .expect("same JSON member should resolve deterministically");
        elapsed_ms.push(started.elapsed().as_secs_f64() * 1_000.0);

        let counters = lix.plugin_v2_transition_counters();
        assert_eq!(counters.conflict_resolution_calls, 1, "sample {sample}");
        assert_eq!(counters.conflict_resolution_records, 1, "sample {sample}");
        assert_eq!(counters.conflict_resolution_takes, 1, "sample {sample}");
        assert_eq!(counters.source_bytes_read, 0, "sample {sample}");
        assert_eq!(counters.attachment_bytes_read, 0, "sample {sample}");
        assert_eq!(
            counters.full_state_semantic_rows_materialized, 0,
            "sample {sample}"
        );
        resolver_boundary_bytes.push(counters.component_boundary_bytes);
    }

    let raw_ms = elapsed_ms.clone();
    elapsed_ms.sort_by(f64::total_cmp);
    eprintln!(
        "v2_json_ten_mib_same_entity_canonical_b_merge bytes={JSON_TEN_MIB_BYTES} samples={SAMPLES} \
         raw_ms={raw_ms:?} p50_ms={:.3} p95_ms={:.3} resolver_boundary_bytes={resolver_boundary_bytes:?}",
        p50_ms(&elapsed_ms),
        p95_ms(&elapsed_ms),
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
    const FILE_ID: &str = "native-json-semantic-control";
    const FILE_PATH: &str = "/native-json-semantic-control.json";

    let archive = build_json_v2_plugin_archive();
    let (source, _, _) = json_ten_mib_flat_fixture();
    let members = native_json_control_members(&source);
    assert_eq!(members.len(), JSON_TEN_MIB_PROPERTY_COUNT);

    let no_file_root_statement = native_json_control_root_insert(None);
    let no_file_member_statements =
        native_json_control_member_insert_chunks(&members, None, SQL_CHUNK_ROWS);
    let file_scoped_root_statement = native_json_control_root_insert(Some(FILE_ID));
    let file_scoped_member_statements =
        native_json_control_member_insert_chunks(&members, Some(FILE_ID), SQL_CHUNK_ROWS);

    let collector = PerfSpanCollector::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(collector.clone()));
    let _dispatcher = tracing::dispatcher::set_default(&dispatch);
    let mut plugin_ms = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        let root = tempfile::tempdir().expect("create plugin import benchmark directory");
        let lix = open_lix_with_rocksdb(root.path()).await;
        install_reference_plugin_in_blank_registry(
            &lix,
            "plugin_json_incremental_v2",
            &archive,
            &["json_root", "json_object_member", "json_array_item"],
        )
        .await;

        // Plugin installation and the caller's input clone are deliberately
        // outside the timer. The timed operation is one normal public file
        // write on an otherwise fresh RocksDB database.
        let input = source.clone();
        lix.reset_plugin_v2_transition_counters();
        collector.clear();
        let started = Instant::now();
        let inserted = lix
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ($1, $2, $3)",
                &[
                    Value::Text(FILE_ID.to_owned()),
                    Value::Text(FILE_PATH.to_owned()),
                    Value::Blob(input.into()),
                ],
            )
            .await
            .expect("real JSON v2 plugin import should succeed");
        assert_eq!(inserted.rows_affected(), 1, "plugin sample {sample}");
        let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
        plugin_ms.push(elapsed_ms);
        eprintln!(
            "v2_json_import_phases sample={sample} elapsed_ms={elapsed_ms:.3} phases_ms={:?}",
            collector.take_aggregate_millis()
        );

        let counters = lix.plugin_v2_transition_counters();
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
    for (label, file_scoped, root_statement, member_statements, samples) in [
        (
            "direct_no_file",
            false,
            &no_file_root_statement,
            &no_file_member_statements,
            &mut direct_no_file_ms,
        ),
        (
            "direct_file_scoped",
            true,
            &file_scoped_root_statement,
            &file_scoped_member_statements,
            &mut direct_file_scoped_ms,
        ),
    ] {
        for sample in 0..SAMPLES {
            let root = tempfile::tempdir().expect("create direct import benchmark directory");
            let lix = open_lix_with_rocksdb(root.path()).await;
            register_native_json_control_schemas(&lix).await;

            // Both the full file payload and every exact semantic snapshot
            // have been prebuilt before timing. The transaction below stays
            // exclusively on the public typed entity surface.
            let file_input = file_scoped.then(|| source.clone());
            let started = Instant::now();
            let mut transaction = lix
                .begin_transaction()
                .await
                .expect("open direct semantic-row transaction");
            if let Some(file_input) = file_input {
                let inserted = transaction
                    .execute(
                        "INSERT INTO lix_file (id, path, data) VALUES ($1, $2, $3)",
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
            samples.push(started.elapsed().as_secs_f64() * 1_000.0);

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
    const FILE_ID: &str = "native-csv-semantic-control";
    const FILE_PATH: &str = "/native-csv-semantic-control.csv";

    let archive = build_csv_v2_plugin_archive();
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
            let lix = open_lix_with_rocksdb(root.path()).await;
            if plugin_lane {
                install_reference_plugin_in_blank_registry(
                    &lix,
                    "plugin_csv_v2",
                    &archive,
                    &["csv_v2_table", "csv_v2_row"],
                )
                .await;
            } else {
                register_native_csv_control_schemas(&lix).await;
            }

            let file_input = source.clone();
            lix.reset_plugin_v2_transition_counters();
            collector.clear();
            let started = Instant::now();
            if plugin_lane {
                let inserted = lix
                    .execute(
                        "INSERT INTO lix_file (id, path, data) VALUES ($1, $2, $3)",
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
                        "INSERT INTO lix_file (id, path, data) VALUES ($1, $2, $3)",
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
            let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
            let phases_ms = collector.take_aggregate_millis();
            eprintln!(
                "v2_csv_import_phases sample={sample} lane={} elapsed_ms={elapsed_ms:.3} phases_ms={phases_ms:?}",
                if plugin_lane { "plugin" } else { "direct" },
            );

            if plugin_lane {
                assert_eq!(
                    lix.plugin_v2_transition_counters().durable_semantic_changes,
                    (CSV_ROW_COUNT + 1) as u64,
                    "plugin sample {sample} must commit one table plus every row"
                );
                plugin_ms.push(elapsed_ms);
                plugin_sample_ms = Some(elapsed_ms);
            } else {
                let row_count = lix
                    .execute("SELECT COUNT(*) AS count FROM csv_v2_row", &[])
                    .await
                    .expect("count direct CSV rows")
                    .rows()[0]
                    .get::<i64>("count")
                    .expect("CSV row count must be an integer");
                assert_eq!(row_count, CSV_ROW_COUNT as i64, "direct sample {sample}");
                direct_ms.push(elapsed_ms);
                direct_sample_ms = Some(elapsed_ms);
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
    assert!(
        paired_p50_ratio <= 1.5,
        "10 MiB CSV plugin import paired p50 ratio {paired_p50_ratio:.3} exceeds the 1.5x direct semantic-row gate"
    );
}

#[tokio::test]
#[ignore = "10 MiB JSON public-SQL read benchmark on RocksDB"]
async fn v2_json_ten_mib_rocksdb_read_benchmark() {
    const WARM_SAMPLES: usize = 20;
    const COLD_SAMPLES: usize = 7;

    let root = tempfile::tempdir().expect("create JSON read benchmark directory");
    let archive = build_json_v2_plugin_archive();
    let lix = open_lix_with_rocksdb(root.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json_incremental_v2",
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
    for _ in 0..WARM_SAMPLES {
        let started = Instant::now();
        let read = read_file(&lix, path)
            .await
            .expect("warm materialized JSON should read")
            .expect("warm materialized JSON should exist");
        warm_ms.push(started.elapsed().as_secs_f64() * 1_000.0);
        assert_eq!(read.len(), JSON_TEN_MIB_BYTES);
        black_box(read);
    }
    lix.close().await.expect("warm JSON benchmark should close");

    let mut cold_total_ms = Vec::with_capacity(COLD_SAMPLES);
    let mut cold_storage_open_ms = Vec::with_capacity(COLD_SAMPLES);
    let mut cold_engine_open_ms = Vec::with_capacity(COLD_SAMPLES);
    let mut cold_read_ms = Vec::with_capacity(COLD_SAMPLES);
    for _ in 0..COLD_SAMPLES {
        let total_started = Instant::now();
        let storage_started = Instant::now();
        let storage =
            RocksDB::open(root.path().join(".lix")).expect("reopen JSON benchmark RocksDB");
        cold_storage_open_ms.push(storage_started.elapsed().as_secs_f64() * 1_000.0);

        let engine_started = Instant::now();
        let reopened = open_lix_with_storage(storage)
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
        cold_total_ms.push(total_started.elapsed().as_secs_f64() * 1_000.0);
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
}

#[derive(Debug)]
struct ColdMaterializedOpenSample {
    elapsed: Duration,
    counters: WasmTransitionCounters,
}

#[tokio::test]
#[ignore = "large cold-open materialized-base benchmark"]
async fn v2_cold_open_materialized_csv_and_json_benchmark() {
    const SAMPLES: usize = 5;

    let storage = lix_sdk::Memory::new();
    let seed = open_lix(OpenLixOptions::new(storage.clone()))
        .await
        .expect("benchmark workspace should open");
    install_plugin(&seed, "plugin_csv_v2", &build_csv_v2_plugin_archive())
        .await
        .expect("CSV v2 plugin should install");
    install_plugin(
        &seed,
        "plugin_json_incremental_v2",
        &build_json_v2_plugin_archive(),
    )
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
        let mut samples = Vec::with_capacity(SAMPLES);
        let mut accepted = initial.to_vec();
        for _ in 0..SAMPLES {
            let lix = open_lix(OpenLixOptions::new(storage.clone()))
                .await
                .expect("cold benchmark workspace should reopen");
            lix.reset_plugin_v2_transition_counters();
            let mut after = accepted.clone();
            after[edit_offset] = alternate_ascii_hex(after[edit_offset]);
            let started = Instant::now();
            write_file(&lix, path, after.clone())
                .await
                .unwrap_or_else(|error| panic!("cold write for {path} should succeed: {error:?}"));
            let elapsed = started.elapsed();
            let actual = read_file(&lix, path)
                .await
                .unwrap_or_else(|error| {
                    panic!("cold write result for {path} should read: {error:?}")
                })
                .unwrap_or_else(|| panic!("cold write result for {path} should exist"));
            assert_eq!(actual, after, "cold write must remain byte-exact");
            samples.push(ColdMaterializedOpenSample {
                elapsed,
                counters: lix.plugin_v2_transition_counters(),
            });
            lix.close()
                .await
                .expect("cold benchmark workspace should close");
            accepted = after;
        }
        report_cold_materialized_open(label, accepted.len(), &samples);
    }
}

fn report_cold_materialized_open(
    label: &str,
    expected_bytes: usize,
    samples: &[ColdMaterializedOpenSample],
) {
    let mut elapsed_ms = samples
        .iter()
        .map(|sample| sample.elapsed.as_secs_f64() * 1_000.0)
        .collect::<Vec<_>>();
    elapsed_ms.sort_by(f64::total_cmp);
    let p50_ms = elapsed_ms[elapsed_ms.len() / 2];
    let p95_index = ((elapsed_ms.len() * 95).div_ceil(100)).saturating_sub(1);
    let p95_ms = elapsed_ms[p95_index];

    for sample in samples {
        let counters = sample.counters;
        assert_eq!(
            counters.full_renderer_invocations, 0,
            "{label} cold materialized read must not render through a plugin"
        );
        assert_eq!(
            counters.full_state_semantic_rows_materialized, 0,
            "{label} cold materialized read must not hydrate semantic entities"
        );
        assert_eq!(
            counters.component_boundary_bytes, 0,
            "{label} cold materialized read must not cross the Component boundary"
        );
    }

    let representative = samples[elapsed_ms.len() / 2].counters;
    eprintln!(
        "v2_cold_materialized_open label={label} bytes={expected_bytes} samples={} \
         p50_ms={p50_ms:.3} p95_ms={p95_ms:.3} source_read_calls={} source_bytes_read={} \
         packet_pages={} packet_records={} attachment_reads={} attachment_bytes_read={} \
         boundary_bytes={} guest_high_water_bytes={} full_renderer_invocations={}",
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
    );
}

#[tokio::test]
async fn v2_json_cold_entity_write_is_scoped_by_file_despite_shared_root_keys() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive = build_json_v2_plugin_archive();
    let lix = open_lix_with_filesystem(tempdir.path()).await;
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json_incremental_v2",
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
    let lix = open_lix_with_filesystem(tempdir.path()).await;
    lix.reset_plugin_v2_transition_counters();
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
    let counters = lix.plugin_v2_transition_counters();
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
    let archive = build_json_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json_incremental_v2",
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
            "SELECT data FROM lix_file WHERE id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(
        staged.rows()[0].get::<Vec<u8>>("data").unwrap(),
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
async fn v2_json_rejects_mixed_byte_and_entity_transitions_in_one_transaction() {
    let archive = build_json_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_json_incremental_v2",
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
            "UPDATE lix_file SET data = $1 WHERE path = $2",
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
    let archive = build_excalidraw_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_excalidraw_v2",
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
    lix.reset_plugin_v2_transition_counters();
    write_file(&lix, path, geometry_edit.clone()).await.unwrap();
    let counters = lix.plugin_v2_transition_counters();
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

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_excalidraw_same_element_branch_merge_uses_canonical_b() {
    let archive = build_excalidraw_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_excalidraw_v2",
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
            id: Some("excalidraw-element-conflict-source".to_owned()),
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

    lix.reset_plugin_v2_transition_counters();
    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("same-element Excalidraw conflict should resolve deterministically");
    let rendered: serde_json::Value =
        serde_json::from_slice(&read_file(&lix, path).await.unwrap().unwrap()).unwrap();
    assert_eq!(rendered["elements"][0]["x"], serde_json::json!(expected_x));
    let counters = lix.plugin_v2_transition_counters();
    assert_eq!(counters.conflict_resolution_calls, 1);
    assert_eq!(counters.conflict_resolution_records, 1);
    assert_eq!(counters.conflict_resolution_takes, 1);

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_id_namespace_reservations_survive_restart_and_tombstone_with_file() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive = build_csv_v2_plugin_archive();
    let path = "/durable-ids.csv";

    let lix = open_lix_with_filesystem(tempdir.path()).await;
    install_plugin(&lix, "plugin_csv_v2", &archive)
        .await
        .unwrap();
    write_file(&lix, path, b"first,one\n".to_vec())
        .await
        .unwrap();
    let file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();
    assert_eq!(plugin_namespace_reservation_count(&lix, &file_id).await, 1);
    let inserted_identity = MutationIdentity {
        namespace_seed: [0x31; 16],
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
    assert_eq!(plugin_namespace_reservation_count(&lix, &file_id).await, 2);
    lix.close().await.unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;
    assert_eq!(plugin_namespace_reservation_count(&lix, &file_id).await, 2);
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
    assert_eq!(plugin_namespace_reservation_count(&lix, &file_id).await, 2);

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
    assert_eq!(plugin_namespace_reservation_count(&lix, &file_id).await, 2);
    lix.execute(
        "DELETE FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_string())],
    )
    .await
    .unwrap();
    assert_eq!(plugin_namespace_reservation_count(&lix, &file_id).await, 0);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_ids_survive_insert_edit_reorder_delete_eviction_and_cold_reopen() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive = build_csv_v2_plugin_archive();
    let path = "/identity-lifecycle.csv";
    let lix = open_lix_with_filesystem(tempdir.path()).await;
    install_plugin(&lix, "plugin_csv_v2", &archive)
        .await
        .unwrap();

    let initial = b"alpha,one\ndup,same\ndup,same\nomega,last\n".to_vec();
    write_file(&lix, path, initial).await.unwrap();
    let file_id = file_id_at_path(&lix, path).await;
    let initial_rows = active_csv_v2_rows(&lix, &file_id).await;
    let alpha_id = csv_v2_row_id(&initial_rows, &["alpha", "one"]);
    let omega_id = csv_v2_row_id(&initial_rows, &["omega", "last"]);
    let duplicate_ids = csv_v2_row_ids(&initial_rows, &["dup", "same"]);
    assert_eq!(duplicate_ids.len(), 2);
    assert_ne!(duplicate_ids[0], duplicate_ids[1]);

    let inserted = b"alpha,one\ninserted,new\ndup,same\ndup,same\nomega,last\n".to_vec();
    write_file(&lix, path, inserted).await.unwrap();
    let after_insert = active_csv_v2_rows(&lix, &file_id).await;
    assert_eq!(csv_v2_row_id(&after_insert, &["alpha", "one"]), alpha_id);
    assert_eq!(csv_v2_row_id(&after_insert, &["omega", "last"]), omega_id);
    assert_eq!(
        csv_v2_row_ids(&after_insert, &["dup", "same"]),
        duplicate_ids
    );
    let inserted_id = csv_v2_row_id(&after_insert, &["inserted", "new"]);
    assert!(
        !initial_rows.iter().any(|row| row.id == inserted_id),
        "an inserted row must receive a fresh compact identity"
    );

    let edited = b"alpha,ONE\ninserted,new\ndup,same\ndup,same\nomega,last\n".to_vec();
    write_file(&lix, path, edited).await.unwrap();
    let after_edit = active_csv_v2_rows(&lix, &file_id).await;
    assert_eq!(csv_v2_row_id(&after_edit, &["alpha", "ONE"]), alpha_id);

    let reordered = b"omega,last\ndup,same\nalpha,ONE\ninserted,new\ndup,same\n".to_vec();
    write_file(&lix, path, reordered).await.unwrap();
    let after_reorder = active_csv_v2_rows(&lix, &file_id).await;
    assert_eq!(csv_v2_row_id(&after_reorder, &["omega", "last"]), omega_id);
    assert_eq!(csv_v2_row_id(&after_reorder, &["alpha", "ONE"]), alpha_id);
    assert_eq!(
        csv_v2_row_id(&after_reorder, &["inserted", "new"]),
        inserted_id
    );
    assert_eq!(
        csv_v2_row_ids(&after_reorder, &["dup", "same"]),
        duplicate_ids
    );

    let final_bytes = b"omega,last\ndup,same\ninserted,new\n".to_vec();
    write_file(&lix, path, final_bytes.clone()).await.unwrap();
    let final_rows = active_csv_v2_rows(&lix, &file_id).await;
    assert_eq!(csv_v2_row_id(&final_rows, &["omega", "last"]), omega_id);
    assert_eq!(
        csv_v2_row_id(&final_rows, &["inserted", "new"]),
        inserted_id
    );
    let remaining_duplicate_ids = csv_v2_row_ids(&final_rows, &["dup", "same"]);
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
    assert_eq!(active_csv_v2_rows(&lix, &file_id).await, final_rows);
    lix.close().await.unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(final_bytes));
    assert_eq!(active_csv_v2_rows(&lix, &file_id).await, final_rows);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_exact_read_replaces_a_stale_actor_after_an_independent_engine_commit() {
    let tempdir = tempfile::tempdir().unwrap();
    let storage_a = LocalFilesystem::open(tempdir.path())
        .await
        .expect("first shared filesystem storage opens");
    let lix_a = open_lix_with_storage(storage_a)
        .await
        .expect("first independent Lix engine opens");
    let archive = build_csv_v2_plugin_archive();
    install_plugin(&lix_a, "plugin_csv_v2", &archive)
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
    let lix_b = open_lix_with_storage(storage_b)
        .await
        .expect("second independent Lix engine opens");
    assert_eq!(read_file(&lix_b, path).await.unwrap(), Some(initial));
    let advanced = b"first,ONE\nsecond,two\n".to_vec();
    write_file(&lix_b, path, advanced.clone()).await.unwrap();

    // Engine A still owns the root-old actor. Its exact SQL read returns the
    // durable materialized bytes without hydrating Wasm; the next write
    // cold-opens root-new and replaces only that captured stale slot.
    lix_a.reset_plugin_v2_transition_counters();
    assert_eq!(
        read_file(&lix_a, path).await.unwrap(),
        Some(advanced.clone())
    );
    let counters = lix_a.plugin_v2_transition_counters();
    assert_eq!(counters.full_state_semantic_rows_materialized, 0);
    assert_eq!(counters.full_renderer_invocations, 0);

    let final_bytes = b"first,ONE\nsecond,TWO\n".to_vec();
    lix_a.reset_plugin_v2_transition_counters();
    write_file(&lix_a, path, final_bytes.clone())
        .await
        .expect("the next write restores root-new authority and applies the sparse edit");
    let counters = lix_a.plugin_v2_transition_counters();
    assert_eq!(
        counters.full_state_semantic_rows_materialized, 3,
        "cold reconstruction materializes the table entity and both row entities"
    );
    assert_eq!(counters.full_renderer_invocations, 1);
    assert_eq!(read_file(&lix_a, path).await.unwrap(), Some(final_bytes));

    lix_b.close().await.unwrap();
    lix_a.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_file_incarnation_fences_old_observations_after_delete_and_recreate() {
    let archive = build_csv_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_plugin(&lix, "plugin_csv_v2", &archive)
        .await
        .unwrap();
    let path = "/recreated.csv";
    let old_bytes = b"old,incarnation\n".to_vec();
    write_file(&lix, path, old_bytes.clone()).await.unwrap();
    let old_file_id = file_id_at_path(&lix, path).await;
    let stale = lix.open_workspace_session().await.unwrap();
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
    let archive = build_csv_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_plugin(&lix, "plugin_csv_v2", &archive)
        .await
        .unwrap();
    let path = "/branch-isolation.csv";
    let main_bytes = b"main,one\nshared,row\n".to_vec();
    write_file(&lix, path, main_bytes.clone()).await.unwrap();
    let main_file_id = file_id_at_path(&lix, path).await;
    let main_rows = active_csv_v2_rows(&lix, &main_file_id).await;
    let main_branch_id = lix.active_branch_id().await.unwrap();

    let branch = lix
        .create_branch(CreateBranchOptions {
            id: Some("v2-actor-isolation".to_string()),
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
    assert_eq!(active_csv_v2_rows(&lix, &main_file_id).await, main_rows);

    let branch_bytes = b"branch,ONE\nshared,row\ninserted,branch\n".to_vec();
    write_file(&lix, path, branch_bytes.clone()).await.unwrap();
    let branch_rows = active_csv_v2_rows(&lix, &main_file_id).await;
    assert_ne!(branch_rows, main_rows);

    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id,
    })
    .await
    .unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(main_bytes));
    assert_eq!(active_csv_v2_rows(&lix, &main_file_id).await, main_rows);

    lix.switch_branch(SwitchBranchOptions {
        branch_id: branch.id,
    })
    .await
    .unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(branch_bytes));
    assert_eq!(active_csv_v2_rows(&lix, &main_file_id).await, branch_rows);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_generation_upgrade_preflights_owned_files_and_fences_stale_sessions() {
    let original = build_csv_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_plugin(&lix, "plugin_csv_v2", &original)
        .await
        .unwrap();
    let path = "/upgrade.csv";
    let bytes = b"first,one\nsecond,two\n".to_vec();
    write_file(&lix, path, bytes.clone()).await.unwrap();

    let stale = lix.open_workspace_session().await.unwrap();
    assert_eq!(read_file(&stale, path).await.unwrap(), Some(bytes.clone()));

    // A packaging-only archive generation change exercises the complete
    // owner preflight while retaining the same compiled component contract.
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_V2_plugin_csv_v2"));
    let wasm = std::fs::read(wasm_path).unwrap();
    let compatible = build_csv_v2_plugin_archive_variant(
        &wasm,
        include_str!("../../../plugins/csv-v2/schema/csv_v2_row.json").as_bytes(),
        Some(b"compatible-generation"),
    );
    assert_ne!(original, compatible);
    install_plugin(&lix, "plugin_csv_v2", &compatible)
        .await
        .expect("byte-stable compatible generation should commit");
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(bytes.clone()));
    assert_eq!(
        read_file(&lix, "/.lix/plugins/plugin_csv_v2.lixplugin")
            .await
            .unwrap(),
        Some(compatible.clone())
    );

    let stale_error = write_file(&stale, path, b"first,STALE\nsecond,two\n".to_vec())
        .await
        .expect_err("a session acknowledged under the previous generation must fail closed");
    assert_eq!(stale_error.code, LixError::CODE_PLUGIN_OBSERVATION_STALE);

    let mut changed_schema: serde_json::Value = serde_json::from_str(include_str!(
        "../../../plugins/csv-v2/schema/csv_v2_row.json"
    ))
    .unwrap();
    changed_schema["description"] =
        serde_json::Value::String("incompatible replacement definition".to_string());
    let changed_schema = serde_json::to_vec(&changed_schema).unwrap();
    let schema_changing =
        build_csv_v2_plugin_archive_variant(&wasm, &changed_schema, Some(b"schema-changing"));
    let schema_error = install_plugin(&lix, "plugin_csv_v2", &schema_changing)
        .await
        .expect_err("an owned schema definition change must be rejected");
    assert_eq!(schema_error.code, LixError::CODE_CONSTRAINT_VIOLATION);

    // The archive validator intentionally performs only a bounded header
    // check. This component reaches the production compiler and is rejected
    // before the replacement registry generation can become authoritative.
    let invalid_component = b"\0asm\x0a\0\0\0";
    let trapping = build_csv_v2_plugin_archive_variant(
        invalid_component,
        include_str!("../../../plugins/csv-v2/schema/csv_v2_row.json").as_bytes(),
        Some(b"invalid-component"),
    );
    install_plugin(&lix, "plugin_csv_v2", &trapping)
        .await
        .expect_err("invalid replacement component must fail preflight");

    assert_eq!(
        read_file(&lix, "/.lix/plugins/plugin_csv_v2.lixplugin")
            .await
            .unwrap(),
        Some(compatible),
        "failed upgrades must leave the compatible generation authoritative"
    );
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(bytes.clone()));
    let fresh = lix.open_workspace_session().await.unwrap();
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
    let original = build_csv_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_plugin(&lix, "plugin_csv_v2", &original)
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
            id: Some("csv-generation-conflict-source".to_owned()),
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
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_V2_plugin_csv_v2"));
    let wasm = std::fs::read(wasm_path).unwrap();
    let upgraded = build_csv_v2_plugin_archive_variant(
        &wasm,
        include_str!("../../../plugins/csv-v2/schema/csv_v2_row.json").as_bytes(),
        Some(b"source-generation"),
    );
    install_plugin(&lix, "plugin_csv_v2", &upgraded)
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

    lix.reset_plugin_v2_transition_counters();
    let error = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .expect_err(
            "derived bytes must not be rendered by a different generation than is committed",
        );
    assert_eq!(error.code, LixError::CODE_MERGE_CONFLICT);
    let counters = lix.plugin_v2_transition_counters();
    assert_eq!(
        counters.conflict_resolution_calls, 0,
        "disjoint rows are not a resolver decision; the generation boundary stays visible"
    );
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(target_bytes));
    assert_eq!(
        read_file(&lix, "/.lix/plugins/plugin_csv_v2.lixplugin")
            .await
            .unwrap(),
        Some(original),
        "a rejected merge must retain the target generation"
    );

    lix.close().await.unwrap();
}

#[tokio::test]
async fn v2_csv_path_only_rename_rekeys_actor_and_cleans_owner_on_unmatch() {
    let archive = build_csv_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_plugin(&lix, "plugin_csv_v2", &archive)
        .await
        .unwrap();

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
    assert_eq!(plugin_namespace_reservation_count(&lix, &file_id).await, 1);

    // This reader must become stale solely because the accepted actor moves
    // to the descriptor-successor key, not because file bytes changed.
    let stale = lix.open_workspace_session().await.unwrap();
    assert_eq!(
        read_file(&stale, before_path).await.unwrap(),
        Some(initial.clone())
    );

    // A path-only UPDATE is ordinary SQL. Its DML source reads the exact
    // materialized bytes and establishes the observation needed for the warm
    // empty-splice descriptor transition.
    let renamer = lix.open_workspace_session().await.unwrap();
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
            "UPDATE lix_file SET data = $1 WHERE id = $2",
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
            "SELECT lixcol_file_id FROM csv_v2_table WHERE lixcol_file_id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    let active_row_rows = lix
        .execute(
            "SELECT lixcol_file_id FROM csv_v2_row WHERE lixcol_file_id = $1",
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
async fn transaction_lix_file_data_uses_session_plugin_runtime() {
    let archive = build_csv_v2_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();

    install_reference_plugin_in_blank_registry(
        &lix,
        "plugin_csv_v2",
        &archive,
        &["csv_v2_table", "csv_v2_row"],
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
            "SELECT data FROM lix_file WHERE id = $1",
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
    let lix = open_lix_with_filesystem(tempdir.path()).await;
    let archive = build_csv_v2_plugin_archive();

    install_plugin(&lix, "plugin_csv_v2", &archive)
        .await
        .unwrap();

    wait_for_disk_file(
        &tempdir.path().join(".lix/plugins/plugin_csv_v2.lixplugin"),
        Some(archive.as_slice()),
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn filesystem_imports_lix_plugin_archives_from_disk() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive = build_csv_v2_plugin_archive();
    let plugin_path = tempdir.path().join(".lix/plugins/plugin_csv_v2.lixplugin");
    std::fs::create_dir_all(plugin_path.parent().unwrap()).unwrap();
    std::fs::write(&plugin_path, &archive).unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;

    let plugins = list_installed_plugins(&lix).await;
    assert_eq!(plugins.len(), 1);
    assert_eq!(plugins[0].key, "plugin_csv_v2");
    assert_eq!(
        read_file(&lix, "/.lix/plugins/plugin_csv_v2.lixplugin")
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

async fn active_csv_v2_rows<StorageImpl>(lix: &Lix<StorageImpl>, file_id: &str) -> Vec<CsvV2Row>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let rows = lix
        .execute(
            "SELECT lixcol_entity_pk, id, order_key, cells FROM csv_v2_row \
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
                .expect("csv_v2_row entity_pk must be an array");
            let id = row.get::<String>("id").unwrap();
            assert_eq!(
                entity_pk,
                vec![serde_json::Value::String(id.clone())],
                "csv_v2_row snapshot identity must equal its durable primary key"
            );
            CsvV2Row {
                id,
                order_key: row.get::<String>("order_key").unwrap(),
                cells: row
                    .get::<serde_json::Value>("cells")
                    .unwrap()
                    .as_array()
                    .expect("csv_v2_row snapshot must have cells")
                    .iter()
                    .map(|cell| {
                        cell.as_str()
                            .expect("csv_v2_row cells must be strings")
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
async fn csv_v2_row_ordering<StorageImpl>(
    lix: &Lix<StorageImpl>,
    file_id: &str,
    row_id: &str,
) -> (String, String)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT lixcol_updated_at, lixcol_change_id FROM csv_v2_row \
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

fn csv_v2_row_ids(rows: &[CsvV2Row], cells: &[&str]) -> Vec<String> {
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

fn csv_v2_row_id(rows: &[CsvV2Row], cells: &[&str]) -> String {
    let ids = csv_v2_row_ids(rows, cells);
    assert_eq!(ids.len(), 1, "expected one csv_v2_row with cells {cells:?}");
    ids[0].clone()
}

async fn plugin_namespace_reservation_count<StorageImpl>(
    lix: &Lix<StorageImpl>,
    file_id: &str,
) -> usize
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT key FROM lix_key_value WHERE lixcol_file_id = $1",
        &[Value::Text(file_id.to_string())],
    )
    .await
    .unwrap()
    .rows()
    .iter()
    .filter(|row| {
        row.get::<String>("key")
            .ok()
            .is_some_and(|key| key.starts_with("lix_plugin_id_namespace_v2:"))
    })
    .count()
}

async fn open_lix_with_filesystem(path: &Path) -> Lix<LocalFilesystem> {
    let storage = LocalFilesystem::open(path).await.unwrap();
    open_lix_with_storage(storage).await.unwrap()
}

async fn open_lix_with_rocksdb(path: &Path) -> Lix<RocksDB> {
    let storage = RocksDB::open(path.join(".lix")).expect("open Lix RocksDB storage");
    open_lix_with_storage(storage)
        .await
        .expect("open Lix workspace")
}

fn p50_ms(sorted: &[f64]) -> f64 {
    sorted[sorted.len() / 2]
}

fn p95_ms(sorted: &[f64]) -> f64 {
    let index = ((sorted.len() * 95).div_ceil(100)).saturating_sub(1);
    sorted[index]
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
        "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET data = excluded.data",
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
        "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET data = excluded.data",
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
            "SELECT data FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    result
        .rows()
        .first()
        .map(|row| row.get::<Vec<u8>>("data"))
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
        .execute("SELECT path, data FROM lix_file ORDER BY path", &[])
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
                row.get::<Vec<u8>>("data").unwrap(),
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

fn build_csv_v2_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_V2_plugin_csv_v2"));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built CSV v2 plugin wasm at {}: {error}",
            wasm_path.display()
        )
    });
    build_csv_v2_plugin_archive_variant(
        &wasm,
        include_str!("../../../plugins/csv-v2/schema/csv_v2_row.json").as_bytes(),
        None,
    )
}

fn build_markdown_v2_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!(
        "CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_INCREMENTAL_V2_plugin_markdown_incremental_v2"
    ));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built Markdown v2 plugin wasm at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/markdown-v2/manifest.json").as_bytes(),
        ),
        (
            "schema/markdown_node_v2.json",
            include_str!("../../../plugins/markdown-v2/schema/markdown_node_v2.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn build_json_v2_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!(
        "CARGO_CDYLIB_FILE_PLUGIN_JSON_INCREMENTAL_V2_plugin_json_incremental_v2"
    ));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built JSON v2 plugin wasm at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/json-v2/manifest.json").as_bytes(),
        ),
        (
            "schema/json_root.json",
            include_str!("../../../plugins/json-v2/schema/json_root.json").as_bytes(),
        ),
        (
            "schema/json_object_member.json",
            include_str!("../../../plugins/json-v2/schema/json_object_member.json").as_bytes(),
        ),
        (
            "schema/json_array_item.json",
            include_str!("../../../plugins/json-v2/schema/json_array_item.json").as_bytes(),
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
        include_str!("../../../plugins/json-v2/schema/json_root.json"),
        include_str!("../../../plugins/json-v2/schema/json_object_member.json"),
        include_str!("../../../plugins/json-v2/schema/json_array_item.json"),
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
        include_str!("../../../plugins/csv-v2/schema/csv_v2_table.json"),
        include_str!("../../../plugins/csv-v2/schema/csv_v2_row.json"),
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
        sql: "INSERT INTO csv_v2_table (id, dialect, lixcol_file_id) VALUES ('root', $1, $2)"
            .to_owned(),
        params: vec![
            Value::Json(serde_json::json!({
                "delimiter": ",",
                "quote": "\"",
                "terminator": "\n",
            })),
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
                    params.push(Value::Text(format!("{index:032x}")));
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
                    ])));
                    params.push(Value::Text(file_id.to_owned()));
                    format!("(${first}, ${}, ${}, ${})", first + 1, first + 2, first + 3)
                })
                .collect::<Vec<_>>()
                .join(",");
            NativeJsonControlStatement {
                sql: format!(
                    "INSERT INTO csv_v2_row (id, order_key, cells, lixcol_file_id) VALUES {values}"
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

fn build_excalidraw_v2_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!(
        "CARGO_CDYLIB_FILE_PLUGIN_EXCALIDRAW_V2_plugin_excalidraw_v2"
    ));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built Excalidraw v2 plugin wasm at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/excalidraw-v2/manifest.json").as_bytes(),
        ),
        (
            "schema/excalidraw_scene.json",
            include_str!("../../../plugins/excalidraw-v2/schema/excalidraw_scene.json").as_bytes(),
        ),
        (
            "schema/excalidraw_element.json",
            include_str!("../../../plugins/excalidraw-v2/schema/excalidraw_element.json")
                .as_bytes(),
        ),
        (
            "schema/excalidraw_file.json",
            include_str!("../../../plugins/excalidraw-v2/schema/excalidraw_file.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn build_csv_v2_plugin_archive_variant(
    wasm: &[u8],
    csv_v2_row_schema: &[u8],
    generation_marker: Option<&[u8]>,
) -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/csv-v2/manifest.json").as_bytes(),
        ),
        (
            "schema/csv_v2_table.json",
            include_str!("../../../plugins/csv-v2/schema/csv_v2_table.json").as_bytes(),
        ),
        ("schema/csv_v2_row.json", csv_v2_row_schema),
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
