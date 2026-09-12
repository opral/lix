//! Registered row merger parity between explicit branch merge and partial sync.
use super::*;
use std::io::Write as _;

#[tokio::test]
async fn partial_sync_uses_registered_row_merger_and_preserves_offline_pending_rows() {
    let (storage, setup) = open_authority().await;
    setup.execute(
        "INSERT INTO lix_file(path,content) VALUES('/.lix/plugins/test_plugin_column_merger.lixplugin',$1)",
        &[Value::Blob(column_merger_archive().into())],
    ).await.unwrap();
    setup.execute("INSERT INTO merge_test_row(id,body,label) VALUES('0198b7a1-0000-7000-8000-000000000001','Alice said hello.\n\nBob said goodbye.','base')", &[]).await.unwrap();
    setup.close().await.unwrap();
    let probe = Arc::new(HttpProbe::default());
    let (url, server, authority) = serve_with_authority_session(storage, probe.clone()).await;
    let directory = TempDir::new().unwrap();
    let replica = open_replica(directory.path(), &url).await;
    let read =
        "SELECT body,label FROM merge_test_row WHERE id='0198b7a1-0000-7000-8000-000000000001'";
    replica.execute(read, &[]).await.unwrap();
    probe.set_offline(true);
    replica.execute("UPDATE merge_test_row SET body='Alice said hello.\n\nBob said GOODBYE.',label='incoming' WHERE id='0198b7a1-0000-7000-8000-000000000001'", &[]).await.unwrap();
    replica.close().await.unwrap();
    let replica = open_replica(directory.path(), &url).await;
    let pending = replica.execute(read, &[]).await.unwrap();
    assert_eq!(
        pending.rows()[0].get::<String>("label").unwrap(),
        "incoming"
    );
    authority.execute("UPDATE merge_test_row SET body='Alice said HELLO.\n\nBob said goodbye.',label='authority-first' WHERE id='0198b7a1-0000-7000-8000-000000000001'", &[]).await;
    probe.set_offline(false);
    let expected = "Alice said HELLO.\n\nBob said GOODBYE.";
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            let rows = authority.execute(read, &[]).await;
            if rows
                == vec![vec![
                    Value::Text(expected.into()),
                    Value::Text("incoming".into()),
                ]]
            {
                let local = replica.execute(read, &[]).await.unwrap();
                if local.rows()[0].get::<String>("body").unwrap() == expected
                    && local.rows()[0].get::<String>("label").unwrap() == "incoming"
                {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the authority and replica must converge to the plugin's third value");
    replica.close().await.unwrap();
    stop_server(server).await;
}

fn column_merger_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!(
        "CARGO_CDYLIB_FILE_TEST_PLUGIN_COLUMN_MERGER_test_plugin_column_merger"
    ));
    let wasm = std::fs::read(wasm_path).unwrap();
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_bytes!("../../fixtures/column-merger/manifest.json").as_slice(),
        ),
        (
            "schema/merge_test_row.json",
            include_bytes!("../../fixtures/column-merger/schema/merge_test_row.json").as_slice(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

#[tokio::test]
async fn partial_sync_materializes_csv_bytes_from_merged_semantic_rows() {
    let (storage, setup) = open_authority().await;
    setup
        .execute(
            "INSERT INTO lix_file(path,content) VALUES('/.lix/plugins/plugin_csv.lixplugin',$1)",
            &[Value::Blob(csv_archive().into())],
        )
        .await
        .unwrap();
    setup
        .execute("INSERT INTO lix_directory(path) VALUES('/nested')", &[])
        .await
        .unwrap();
    setup
        .execute(
            "INSERT INTO lix_file(path,content) VALUES('/nested/shared.csv',$1)",
            &[Value::Blob(b"quick,dog\n".to_vec().into())],
        )
        .await
        .unwrap();
    setup.close().await.unwrap();
    let probe = Arc::new(HttpProbe::default());
    let (url, server, authority) = serve_with_authority_session(storage, probe.clone()).await;
    let directory = TempDir::new().unwrap();
    let replica = open_replica(directory.path(), &url).await;
    let initial_read = "SELECT content FROM lix_file WHERE path='/nested/shared.csv'";
    let read = "SELECT content FROM lix_file WHERE path='/nested/renamed.csv'";
    let first = replica.execute(initial_read, &[]).await.unwrap();
    assert_eq!(
        first.rows()[0].get::<Vec<u8>>("content").unwrap(),
        b"quick,dog\n"
    );
    // Retain the semantic scope as well as the derived content projection.
    replica
        .execute("SELECT cells FROM csv_row", &[])
        .await
        .unwrap();
    replica
        .execute("SELECT id,path FROM lix_directory", &[])
        .await
        .unwrap();
    probe.set_offline(true);
    replica
        .execute(
            "UPDATE lix_file SET content=$1 WHERE path='/nested/shared.csv'",
            &[Value::Blob(b"quick,sleepy dog\n".to_vec().into())],
        )
        .await
        .unwrap();
    replica
        .execute(
            "UPDATE lix_file SET path='/nested/renamed.csv' WHERE path='/nested/shared.csv'",
            &[],
        )
        .await
        .unwrap();
    authority
        .execute(
            "UPDATE lix_file SET content=$1 WHERE path='/nested/shared.csv'",
            &[Value::Blob(b"very quick,dog\n".to_vec().into())],
        )
        .await;
    probe.set_offline(false);
    let expected = b"very quick,sleepy dog\n";
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            let rows = authority.execute(read, &[]).await;
            if rows == vec![vec![Value::Blob(expected.to_vec().into())]] {
                let local = replica.execute(read, &[]).await.unwrap();
                if local.rows()[0].get::<Vec<u8>>("content").unwrap() == expected {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("both row edits must appear in the authority and replica file bytes");
    assert!(authority.execute(initial_read, &[]).await.is_empty());
    assert!(
        replica
            .execute(initial_read, &[])
            .await
            .unwrap()
            .rows()
            .is_empty()
    );
    let native = authority.execute("SELECT cells FROM csv_row", &[]).await;
    let local = replica
        .execute("SELECT cells FROM csv_row", &[])
        .await
        .unwrap();
    assert!(format!("{native:?}").contains("very quick"));
    assert!(format!("{native:?}").contains("sleepy dog"));
    assert!(format!("{local:?}").contains("very quick"));
    assert!(format!("{local:?}").contains("sleepy dog"));
    replica.close().await.unwrap();
    stop_server(server).await;
}

fn csv_archive() -> Vec<u8> {
    let wasm = std::fs::read(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv")).unwrap();
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_bytes!("../../../../plugins/csv/manifest.json").as_slice(),
        ),
        (
            "schema/csv_table.json",
            include_bytes!("../../../../plugins/csv/schema/csv_table.json").as_slice(),
        ),
        (
            "schema/csv_row.json",
            include_bytes!("../../../../plugins/csv/schema/csv_row.json").as_slice(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

#[tokio::test]
async fn incoming_parsed_file_delete_retires_authority_only_rows() {
    parsed_file_lifecycle(true).await;
}

#[tokio::test]
async fn incoming_parsed_file_edit_restores_complete_captured_rows_after_delete() {
    parsed_file_lifecycle(false).await;
}

async fn parsed_file_lifecycle(incoming_delete: bool) {
    let (storage, setup) = open_authority().await;
    setup
        .execute(
            "INSERT INTO lix_file(path,content) VALUES('/.lix/plugins/plugin_csv.lixplugin',$1)",
            &[Value::Blob(csv_archive().into())],
        )
        .await
        .unwrap();
    setup
        .execute(
            "INSERT INTO lix_file(path,content) VALUES('/lifecycle.csv',$1)",
            &[Value::Blob(b"first,one\nsecond,two\n".to_vec().into())],
        )
        .await
        .unwrap();
    setup.close().await.unwrap();
    let probe = Arc::new(HttpProbe::default());
    let (url, server, authority) = serve_with_authority_session(storage, probe.clone()).await;
    let directory = TempDir::new().unwrap();
    let replica = open_replica(directory.path(), &url).await;
    let initial = replica
        .execute(
            "SELECT id,content FROM lix_file WHERE path='/lifecycle.csv'",
            &[],
        )
        .await
        .unwrap();
    let id = initial.rows()[0].get::<String>("id").unwrap();
    let rows_sql = "SELECT id,cells FROM csv_row WHERE lixcol_file_id=$1 ORDER BY id";
    let before = replica
        .execute(rows_sql, &[Value::Text(id.clone())])
        .await
        .unwrap();
    assert_eq!(before.rows().len(), 2);
    probe.set_offline(true);
    if incoming_delete {
        replica
            .execute(
                "DELETE FROM lix_file WHERE id=$1",
                &[Value::Text(id.clone())],
            )
            .await
            .unwrap();
        authority
            .execute(
                "UPDATE lix_file SET content=$1 WHERE id=$2",
                &[
                    Value::Blob(
                        b"first,one\nsecond,two\nthird,remote-only\n"
                            .to_vec()
                            .into(),
                    ),
                    Value::Text(id.clone()),
                ],
            )
            .await;
    } else {
        replica
            .execute(
                "UPDATE lix_file SET content=$1 WHERE id=$2",
                &[
                    Value::Blob(b"first,INCOMING\nsecond,two\n".to_vec().into()),
                    Value::Text(id.clone()),
                ],
            )
            .await
            .unwrap();
        authority
            .execute(
                "DELETE FROM lix_file WHERE id=$1",
                &[Value::Text(id.clone())],
            )
            .await;
    }
    probe.set_offline(false);
    let content_sql = "SELECT content FROM lix_file WHERE id=$1";
    let expected = if incoming_delete {
        Vec::new()
    } else {
        vec![vec![Value::Blob(
            b"first,INCOMING\nsecond,two\n".to_vec().into(),
        )]]
    };
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            if authority
                .execute(content_sql, &[Value::Text(id.clone())])
                .await
                == expected
            {
                let local = replica
                    .execute(content_sql, &[Value::Text(id.clone())])
                    .await
                    .unwrap();
                let settled = if incoming_delete {
                    local.rows().is_empty()
                } else {
                    local.rows().len() == 1
                        && local.rows()[0].get::<Vec<u8>>("content").unwrap()
                            == b"first,INCOMING\nsecond,two\n"
                };
                if settled {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("accepted parsed file lifecycle must converge");
    let remote_rows = authority
        .execute(rows_sql, &[Value::Text(id.clone())])
        .await;
    let local_rows = replica
        .execute(rows_sql, &[Value::Text(id.clone())])
        .await
        .unwrap();
    if incoming_delete {
        assert!(
            remote_rows.is_empty(),
            "normal deletion must remove authority-only plugin rows"
        );
        assert!(local_rows.rows().is_empty());
    } else {
        assert_eq!(remote_rows.len(), 2);
        assert_eq!(local_rows.rows().len(), 2);
        for (index, original) in before.rows().iter().enumerate() {
            assert_eq!(
                local_rows.rows()[index].get::<String>("id").unwrap(),
                original.get::<String>("id").unwrap()
            );
        }
        assert!(format!("{remote_rows:?}").contains("INCOMING"));
        assert!(
            format!("{remote_rows:?}").contains("two"),
            "unchanged source row must be restored"
        );
    }
    replica.close().await.unwrap();
    stop_server(server).await;
}
