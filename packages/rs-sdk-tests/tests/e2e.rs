use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use lix_sdk::{
    CreateBranchOptions, ExecuteOptions, ExecuteStatementMetadata, Lix, LixError, MutationIdentity,
    RequestBlobSpliceProvenance, Storage, SwitchBranchOptions,
};
use lix_sdk::{LocalFilesystem, open_lix_with_storage};
use lix_sdk::{OpenLixOptions, Value, open_lix};
use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::time::{Duration, Instant};

#[tokio::test]
async fn rs_sdk_installs_built_csv_plugin_archive_and_uses_schema() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();

    install_plugin(&lix, "plugin_csv", &archive).await.unwrap();
    let plugins = list_installed_plugins(&lix).await;
    assert_eq!(plugins.len(), 1);
    assert_eq!(plugins[0].key, "plugin_csv");
    assert_eq!(
        plugins[0].schema_keys,
        vec!["csv_table".to_string(), "csv_row".to_string()]
    );

    let stored_archive = read_file(&lix, "/.lix/plugins/plugin_csv.lixplugin")
        .await
        .unwrap();
    assert_eq!(stored_archive.as_deref(), Some(archive.as_slice()));

    let schemas = lix
        .execute(
            "SELECT table_name \
             FROM information_schema.tables \
             WHERE table_name IN ('csv_row', 'csv_table') \
             ORDER BY table_name",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        schemas
            .rows()
            .iter()
            .map(|row| row.get::<String>("table_name").unwrap())
            .collect::<Vec<_>>(),
        vec!["csv_row".to_string(), "csv_table".to_string()]
    );

    let original_csv = b"name,age\nAda,37\n".to_vec();
    write_file(&lix, "/people.csv", original_csv.clone())
        .await
        .unwrap();
    assert_eq!(
        read_file(&lix, "/people.csv").await.unwrap().as_deref(),
        Some(original_csv.as_slice())
    );

    let file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text("/people.csv".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(file_id.len(), 1);
    let file_id = file_id.rows()[0].get::<String>("id").unwrap();
    let file_changes_before_update = file_changes(&lix, &file_id).await;

    let updated_csv = b"name,age\nAda,37\nGrace,85\n".to_vec();
    write_file(&lix, "/people.csv", updated_csv.clone())
        .await
        .unwrap();
    assert_eq!(
        read_file(&lix, "/people.csv").await.unwrap().as_deref(),
        Some(updated_csv.as_slice())
    );

    let file_changes_after_update = file_changes(&lix, &file_id).await;
    let resulting_diff_changes = file_changes_after_update
        .into_iter()
        .skip(file_changes_before_update.len())
        .collect::<Vec<_>>();
    assert_eq!(resulting_diff_changes.len(), 1);
    let change = &resulting_diff_changes[0];
    assert_eq!(change.schema_key, "csv_row");
    let snapshot = change
        .snapshot_content
        .as_ref()
        .expect("updated file write should produce a csv row snapshot");
    assert_eq!(
        snapshot
            .get("cells")
            .and_then(serde_json::Value::as_array)
            .unwrap(),
        &vec![
            serde_json::Value::String("Grace".to_string()),
            serde_json::Value::String("85".to_string())
        ]
    );

    let files = lix
        .execute(
            "SELECT path, data FROM lix_file WHERE path = $1",
            &[Value::Text("/people.csv".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(
        files.rows()[0].values(),
        &[
            Value::Text("/people.csv".to_string()),
            Value::Blob(updated_csv.clone().into())
        ]
    );

    let files_by_id = lix
        .execute(
            "SELECT data FROM lix_file WHERE id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(files_by_id.len(), 1);
    assert_eq!(
        files_by_id.rows()[0].values(),
        &[Value::Blob(updated_csv.clone().into())]
    );

    let file_changes_before_empty = file_changes(&lix, &file_id).await;
    let empty_csv = Vec::new();
    write_file(&lix, "/people.csv", empty_csv.clone())
        .await
        .unwrap();
    assert_eq!(
        read_file(&lix, "/people.csv").await.unwrap().as_deref(),
        Some(empty_csv.as_slice())
    );
    let files_empty_by_id = lix
        .execute(
            "SELECT data FROM lix_file WHERE id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(files_empty_by_id.len(), 1);
    assert_eq!(
        files_empty_by_id.rows()[0].values(),
        &[Value::Blob(empty_csv.into())]
    );
    let empty_changes = file_changes(&lix, &file_id)
        .await
        .into_iter()
        .skip(file_changes_before_empty.len())
        .collect::<Vec<_>>();
    assert!(
        empty_changes
            .iter()
            .any(|change| change.schema_key == "csv_row" && change.snapshot_content.is_none())
    );

    let sql_csv = b"name,age\nLin,44\n".to_vec();
    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
        &[
            Value::Text("/sql-people.csv".to_string()),
            Value::Blob(sql_csv.clone().into()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, "/sql-people.csv").await.unwrap().as_deref(),
        Some(sql_csv.as_slice())
    );

    let sql_file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text("/sql-people.csv".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(sql_file_id.len(), 1);
    let sql_file_id = sql_file_id.rows()[0].get::<String>("id").unwrap();
    let sql_insert_changes = file_changes(&lix, &sql_file_id).await;
    assert!(
        sql_insert_changes
            .iter()
            .any(|change| change.schema_key == "csv_table")
    );
    assert!(
        sql_insert_changes
            .iter()
            .any(|change| change.schema_key == "csv_row")
    );
    assert!(
        !sql_insert_changes
            .iter()
            .any(|change| change.schema_key == "lix_binary_blob_ref")
    );

    let sql_changes_before_update = sql_insert_changes.len();
    let sql_updated_csv = b"name,age\nLin,44\nMina,29\n".to_vec();
    lix.execute(
        "UPDATE lix_file SET data = $1 WHERE path = $2",
        &[
            Value::Blob(sql_updated_csv.clone().into()),
            Value::Text("/sql-people.csv".to_string()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, "/sql-people.csv").await.unwrap().as_deref(),
        Some(sql_updated_csv.as_slice())
    );
    let sql_update_changes = file_changes(&lix, &sql_file_id)
        .await
        .into_iter()
        .skip(sql_changes_before_update)
        .collect::<Vec<_>>();
    assert!(sql_update_changes.iter().any(|change| {
        change.schema_key == "csv_row"
            && change
                .snapshot_content
                .as_ref()
                .and_then(|snapshot| snapshot.get("cells"))
                .and_then(serde_json::Value::as_array)
                == Some(&vec![
                    serde_json::Value::String("Mina".to_string()),
                    serde_json::Value::String("29".to_string()),
                ])
    }));
    assert!(
        !sql_update_changes
            .iter()
            .any(|change| change.schema_key == "lix_binary_blob_ref")
    );

    let sql_changes_before_predicate_update = sql_changes_before_update + sql_update_changes.len();
    let sql_predicate_updated_csv = b"name,age\nLin,44\nMina,29\nKatherine,101\n".to_vec();
    lix.execute(
        "UPDATE lix_file SET data = $1 WHERE path = $2 AND data = $3",
        &[
            Value::Blob(sql_predicate_updated_csv.clone().into()),
            Value::Text("/sql-people.csv".to_string()),
            Value::Blob(sql_updated_csv.clone().into()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, "/sql-people.csv").await.unwrap().as_deref(),
        Some(sql_predicate_updated_csv.as_slice())
    );
    let sql_predicate_update_changes = file_changes(&lix, &sql_file_id)
        .await
        .into_iter()
        .skip(sql_changes_before_predicate_update)
        .collect::<Vec<_>>();
    assert!(sql_predicate_update_changes.iter().any(|change| {
        change.schema_key == "csv_row"
            && change
                .snapshot_content
                .as_ref()
                .and_then(|snapshot| snapshot.get("cells"))
                .and_then(serde_json::Value::as_array)
                == Some(&vec![
                    serde_json::Value::String("Katherine".to_string()),
                    serde_json::Value::String("101".to_string()),
                ])
    }));
    assert!(
        !sql_predicate_update_changes
            .iter()
            .any(|change| change.schema_key == "lix_binary_blob_ref")
    );

    let sql_empty_target = b"name,age\nNoor,10\n".to_vec();
    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
        &[
            Value::Text("/sql-empty.csv".to_string()),
            Value::Blob(sql_empty_target.into()),
        ],
    )
    .await
    .unwrap();
    let sql_empty_file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text("/sql-empty.csv".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(sql_empty_file_id.len(), 1);
    let sql_empty_file_id = sql_empty_file_id.rows()[0].get::<String>("id").unwrap();
    let sql_empty_changes_before_update = file_changes(&lix, &sql_empty_file_id).await;
    let sql_empty_bytes = Vec::new();
    lix.execute(
        "UPDATE lix_file SET data = $1 WHERE path = $2",
        &[
            Value::Blob(sql_empty_bytes.clone().into()),
            Value::Text("/sql-empty.csv".to_string()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, "/sql-empty.csv").await.unwrap().as_deref(),
        Some(sql_empty_bytes.as_slice())
    );
    let sql_empty_update_changes = file_changes(&lix, &sql_empty_file_id)
        .await
        .into_iter()
        .skip(sql_empty_changes_before_update.len())
        .collect::<Vec<_>>();
    assert!(
        sql_empty_update_changes
            .iter()
            .any(|change| change.schema_key == "csv_row" && change.snapshot_content.is_none())
    );

    let sql_rename_csv = b"name,age\nRuth,99\n".to_vec();
    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
        &[
            Value::Text("/sql-rename.csv".to_string()),
            Value::Blob(sql_rename_csv.clone().into()),
        ],
    )
    .await
    .unwrap();
    let sql_rename_file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text("/sql-rename.csv".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(sql_rename_file_id.len(), 1);
    let sql_rename_file_id = sql_rename_file_id.rows()[0].get::<String>("id").unwrap();
    let rename = lix
        .execute(
            "UPDATE lix_file SET path = $1 WHERE path = $2",
            &[
                Value::Text("/sql-rename.txt".to_string()),
                Value::Text("/sql-rename.csv".to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(rename.rows_affected(), 1);
    assert_eq!(read_file(&lix, "/sql-rename.csv").await.unwrap(), None);
    assert_eq!(
        read_file(&lix, "/sql-rename.txt").await.unwrap().as_deref(),
        Some(sql_rename_csv.as_slice())
    );
    let renamed_files = lix
        .execute(
            "SELECT data FROM lix_file WHERE path = $1",
            &[Value::Text("/sql-rename.txt".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(renamed_files.len(), 1);
    assert_eq!(
        renamed_files.rows()[0].values(),
        &[Value::Blob(sql_rename_csv.clone().into())]
    );
    let active_plugin_rows_after_rename = lix
        .execute(
            "SELECT schema_key FROM lix_state \
             WHERE file_id = $1 AND schema_key IN ('csv_table', 'csv_row')",
            &[Value::Text(sql_rename_file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(active_plugin_rows_after_rename.len(), 0);
    let active_blob_rows_after_rename = lix
        .execute(
            "SELECT schema_key FROM lix_state \
             WHERE file_id = $1 AND schema_key = 'lix_binary_blob_ref'",
            &[Value::Text(sql_rename_file_id)],
        )
        .await
        .unwrap();
    assert_eq!(active_blob_rows_after_rename.len(), 1);

    let sql_changes_before_delete =
        sql_changes_before_predicate_update + sql_predicate_update_changes.len();
    lix.execute(
        "DELETE FROM lix_file WHERE path = $1 AND data = $2",
        &[
            Value::Text("/sql-people.csv".to_string()),
            Value::Blob(sql_predicate_updated_csv.into()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(read_file(&lix, "/sql-people.csv").await.unwrap(), None);
    let sql_delete_changes = file_changes(&lix, &sql_file_id)
        .await
        .into_iter()
        .skip(sql_changes_before_delete)
        .collect::<Vec<_>>();
    assert!(
        sql_delete_changes.iter().any(|change| {
            change.schema_key == "csv_table" && change.snapshot_content.is_none()
        })
    );
    assert!(
        sql_delete_changes
            .iter()
            .filter(|change| change.schema_key == "csv_row" && change.snapshot_content.is_none())
            .count()
            >= 2
    );
    let active_plugin_rows_after_delete = lix
        .execute(
            "SELECT schema_key FROM lix_state \
             WHERE file_id = $1 AND schema_key IN ('csv_table', 'csv_row')",
            &[Value::Text(sql_file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(active_plugin_rows_after_delete.len(), 0);

    lix.close().await.unwrap();
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
async fn v2_csv_cold_open_preserves_legacy_uuid_rows_while_allocating_new_compact_ids() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive = build_csv_v2_plugin_archive();
    let path = "/legacy-identities.csv";
    let lix = open_lix_with_filesystem(tempdir.path()).await;
    install_plugin(&lix, "plugin_csv_v2", &archive)
        .await
        .unwrap();
    let initial = b"old,one\nkeep,two\n".to_vec();
    write_file(&lix, path, initial.clone()).await.unwrap();
    let file_id = file_id_at_path(&lix, path).await;
    let compact_rows = active_csv_v2_rows(&lix, &file_id).await;
    let legacy_ids = [
        "018f47d2-7b2e-7b4c-8e3a-0123456789ab",
        "018f47d2-7b2e-7b4c-8e3a-0123456789ac",
    ];

    // Public semantic DML models a repository created by an older plugin.
    // Reopening below ensures the v2 component receives these identities from
    // durable state rather than from the actor that imported the raw bytes.
    let mut transaction = lix.begin_transaction().await.unwrap();
    for (row, legacy_id) in compact_rows.iter().zip(legacy_ids) {
        transaction
            .execute(
                "DELETE FROM csv_v2_row WHERE id = $1 AND lixcol_file_id = $2",
                &[Value::Text(row.id.clone()), Value::Text(file_id.clone())],
            )
            .await
            .unwrap();
        transaction
            .execute(
                "INSERT INTO csv_v2_row (id, order_key, cells, lixcol_file_id) \
                 VALUES ($1, $2, $3, $4)",
                &[
                    Value::Text(legacy_id.to_string()),
                    Value::Text(row.order_key.clone()),
                    Value::Json(serde_json::json!(row.cells)),
                    Value::Text(file_id.clone()),
                ],
            )
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();
    lix.close().await.unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(initial));
    let cold_rows = active_csv_v2_rows(&lix, &file_id).await;
    assert_eq!(csv_v2_row_id(&cold_rows, &["old", "one"]), legacy_ids[0]);
    assert_eq!(csv_v2_row_id(&cold_rows, &["keep", "two"]), legacy_ids[1]);

    let changed = b"keep,TWO\nnew,three\nold,one\n".to_vec();
    write_file(&lix, path, changed).await.unwrap();
    let after_change = active_csv_v2_rows(&lix, &file_id).await;
    assert_eq!(
        csv_v2_row_id(&after_change, &["keep", "TWO"]),
        legacy_ids[1]
    );
    assert_eq!(csv_v2_row_id(&after_change, &["old", "one"]), legacy_ids[0]);
    let compact_id = csv_v2_row_id(&after_change, &["new", "three"]);
    assert_eq!(compact_id.len(), 32);
    assert_eq!(URL_SAFE_NO_PAD.decode(&compact_id).unwrap().len(), 24);

    let final_bytes = b"keep,TWO\nnew,three\n".to_vec();
    write_file(&lix, path, final_bytes.clone()).await.unwrap();
    let final_rows = active_csv_v2_rows(&lix, &file_id).await;
    assert_eq!(csv_v2_row_id(&final_rows, &["keep", "TWO"]), legacy_ids[1]);
    assert_eq!(csv_v2_row_id(&final_rows, &["new", "three"]), compact_id);
    assert!(!final_rows.iter().any(|row| row.id == legacy_ids[0]));
    lix.close().await.unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;
    assert_eq!(read_file(&lix, path).await.unwrap(), Some(final_bytes));
    assert_eq!(active_csv_v2_rows(&lix, &file_id).await, final_rows);
    lix.close().await.unwrap();
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
    let archive = build_csv_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();

    install_plugin(&lix, "plugin_csv", &archive).await.unwrap();
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

    let lix = open_lix_with_filesystem(tempdir.path()).await;

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

#[derive(Debug, Clone, PartialEq)]
struct FileChange {
    schema_key: String,
    entity_pk: serde_json::Value,
    snapshot_content: Option<serde_json::Value>,
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

async fn file_changes(lix: &Lix, file_id: &str) -> Vec<FileChange> {
    let changes = lix
        .execute(
            "SELECT schema_key, entity_pk, snapshot_content \
             FROM lix_change \
             WHERE file_id = $1 \
             ORDER BY created_at, id",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .unwrap();

    changes
        .rows()
        .iter()
        .map(|row| {
            let snapshot_content = match row.value("snapshot_content").unwrap() {
                Value::Json(value) => Some(value.clone()),
                Value::Null => None,
                other => panic!("expected JSON or null snapshot_content, got {other:?}"),
            };
            FileChange {
                schema_key: row.get::<String>("schema_key").unwrap(),
                entity_pk: row.get::<serde_json::Value>("entity_pk").unwrap(),
                snapshot_content,
            }
        })
        .collect()
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
            .is_some_and(|key| key.starts_with("lix_plugin_id_namespace_v1:"))
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

fn build_csv_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv"));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built CSV plugin wasm at {}: {error}",
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
