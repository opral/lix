use lix_sdk::{
    CreateBranchOptions, ExecuteOptions, ExecuteStatementMetadata, Lix, LixError, MutationIdentity,
    RequestBlobSpliceProvenance, Storage, SwitchBranchOptions, VerifiedRequestBlob,
    WasmComponentV2Factory, WasmLimits, WasmRuntime,
};
use lix_sdk::{LocalFilesystem, open_lix_with_storage};
use lix_sdk::{OpenLixOptions, Value, open_lix};
use sha2::{Digest as _, Sha256};
use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

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

    // A session that never received the file has no omission authority. V2
    // fails that blind replacement closed and leaves durable bytes untouched.
    let blind = lix.open_workspace_session().await.unwrap();
    let error = write_file(&blind, path, b"first,ONE\n".to_vec())
        .await
        .expect_err("blind v2 overwrite must require an exact observation");
    assert_eq!(error.code, LixError::CODE_PLUGIN_OBSERVATION_STALE);
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(deleted.clone()));

    // Once the session receives the complete blob, omitting the row is an
    // acknowledged deletion.
    assert_eq!(read_file(&blind, path).await.unwrap(), Some(deleted));
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
        cold.host_full_content_classification_bytes,
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
    assert_eq!(warm.host_full_content_classification_bytes, 0);
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

    // Engine A still owns the root-old actor. Its exact SQL read must cold-open
    // root-new and replace only that captured stale slot, rather than returning
    // observation-stale forever.
    lix_a.reset_plugin_v2_transition_counters();
    assert_eq!(
        read_file(&lix_a, path).await.unwrap(),
        Some(advanced.clone())
    );
    let counters = lix_a.plugin_v2_transition_counters();
    assert_eq!(
        counters.full_state_semantic_rows_materialized, 3,
        "cold reconstruction materializes the table entity and both row entities"
    );
    assert_eq!(counters.full_renderer_invocations, 1);

    // The recovered read published root-new authority into A's session.
    let final_bytes = b"first,ONE\nsecond,TWO\n".to_vec();
    write_file(&lix_a, path, final_bytes.clone())
        .await
        .expect("the recovered root-new observation authorizes the next sparse edit");
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
    let active_plugin_rows = lix
        .execute(
            "SELECT schema_key FROM lix_state \
             WHERE file_id = $1 AND schema_key IN ('csv_v2_table', 'csv_v2_row')",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(active_plugin_rows.len(), 0);
    let active_owner_rows = lix
        .execute(
            "SELECT schema_key FROM lix_state \
             WHERE file_id = $1 AND schema_key = 'lix_key_value'",
            &[Value::Text(file_id)],
        )
        .await
        .unwrap();
    assert_eq!(active_owner_rows.len(), 0);

    stale.close().await.unwrap();
    renamer.close().await.unwrap();
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
            "SELECT entity_pk, snapshot_content FROM lix_state \
             WHERE file_id = $1 AND schema_key = 'csv_v2_row'",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .unwrap();
    let mut rows = rows
        .rows()
        .iter()
        .map(|row| {
            let entity_pk = row
                .get::<serde_json::Value>("entity_pk")
                .unwrap()
                .as_array()
                .cloned()
                .expect("csv_v2_row entity_pk must be an array");
            let snapshot = row.get::<serde_json::Value>("snapshot_content").unwrap();
            let id = snapshot
                .get("id")
                .and_then(serde_json::Value::as_str)
                .expect("csv_v2_row snapshot must have a string id")
                .to_string();
            assert_eq!(
                entity_pk,
                vec![serde_json::Value::String(id.clone())],
                "csv_v2_row snapshot identity must equal its durable primary key"
            );
            CsvV2Row {
                id,
                order_key: snapshot
                    .get("order_key")
                    .and_then(serde_json::Value::as_str)
                    .expect("csv_v2_row snapshot must have a string order_key")
                    .to_string(),
                cells: snapshot
                    .get("cells")
                    .and_then(serde_json::Value::as_array)
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
        "SELECT entity_pk FROM lix_state \
         WHERE file_id = $1 AND schema_key = 'lix_key_value'",
        &[Value::Text(file_id.to_string())],
    )
    .await
    .unwrap()
    .rows()
    .iter()
    .filter(|row| {
        row.get::<serde_json::Value>("entity_pk")
            .ok()
            .and_then(|value| value.as_array().cloned())
            .and_then(|parts| parts.into_iter().next())
            .and_then(|part| part.as_str().map(str::to_string))
            .is_some_and(|key| key.starts_with("lix_plugin_id_namespace_v2:"))
    })
    .count()
}

async fn open_lix_with_filesystem(path: &Path) -> Lix<LocalFilesystem> {
    let storage = LocalFilesystem::open(path).await.unwrap();
    open_lix_with_storage(storage).await.unwrap()
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

const JSON_TEN_MIB_BYTES: usize = 10 * 1024 * 1024;
const JSON_TEN_MIB_PROPERTY_COUNT: usize = 39_870;
const JSON_V2_GUEST_MEMORY_LIMIT_BYTES: u64 = 128 * 1024 * 1024;

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
