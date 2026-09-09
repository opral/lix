use lix::storage::Storage;
use lix::{ExecuteBatchStatement, Lix, Memory, Value, open_lix};
use lix_storage_filesystem::FilesystemStorage;
use std::io::{Cursor, Write};

async fn install_markdown<S: Storage + Clone + Send + Sync + 'static>(lix: &Lix<S>) {
    let wasm = std::fs::read(env!("CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_plugin_markdown")).unwrap();
    let mut archive = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, content) in [
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
        archive.start_file(path, options).unwrap();
        archive.write_all(content).unwrap();
    }
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES($1,$2)",
        &[
            Value::Text("/.lix/plugins/plugin_markdown.lixplugin".into()),
            Value::Blob(archive.finish().unwrap().into_inner().into()),
        ],
    )
    .await
    .unwrap();
}

async fn semantic_edit<S: Storage + Clone + Send + Sync + 'static>(lix: &Lix<S>, text: &str) {
    lix.execute(
        "UPDATE markdown_node SET payload_json=$1 WHERE kind='paragraph'",
        &[Value::Text(
            serde_json::json!({"inline": [{"type": "text", "value": text}]}).to_string(),
        )],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn mixed_read_batches_refresh_backing_session_plugin_observations() {
    for (coherent, files_first) in [(true, false), (true, true), (false, false), (false, true)] {
        let storage = Memory::new();
        let primary = open_lix().with_storage(storage.clone()).await.unwrap();
        install_markdown(&primary).await;
        let backing = primary.open_storage_session(storage).await.unwrap();
        backing
            .execute(
                "INSERT INTO lix_file(path,content) VALUES('/a.md',$1)",
                &[Value::Blob(b"initial\n".to_vec().into())],
            )
            .await
            .unwrap();
        for index in 0..3 {
            semantic_edit(&primary, &format!("manual {index}")).await;
            let mut statements: [(&str, &[Value]); 2] = [
                ("SELECT path FROM lix_directory ORDER BY path", &[]),
                ("SELECT path, content FROM lix_file ORDER BY path", &[]),
            ];
            if files_first {
                statements.reverse();
            }
            if coherent {
                backing
                    .execute_coherent_read_batch(&statements)
                    .await
                    .unwrap();
            } else {
                backing
                    .execute_batch(
                        &statements
                            .iter()
                            .map(|(sql, params)| ExecuteBatchStatement {
                                label: None,
                                sql: (*sql).to_owned(),
                                params: params.to_vec(),
                            })
                            .collect::<Vec<_>>(),
                    )
                    .await
                    .unwrap();
            }
            let external = format!("external {index}\n");
            backing
                .execute(
                    "UPDATE lix_file SET content=$1 WHERE path='/a.md'",
                    &[Value::Blob(external.clone().into_bytes().into())],
                )
                .await
                .expect(
                    "a mixed snapshot delivers current bytes and must refresh their observation",
                );
            let rows = primary
                .execute("SELECT content FROM lix_file WHERE path='/a.md'", &[])
                .await
                .unwrap();
            assert_eq!(
                rows.rows()[0].get::<Vec<u8>>("content").unwrap(),
                external.as_bytes()
            );
        }
        backing.close().await.unwrap();
        primary.close().await.unwrap();
    }
}

#[tokio::test]
async fn filesystem_import_follows_primary_semantic_edits() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.md");
    std::fs::write(&path, b"initial\n").unwrap();
    let storage = FilesystemStorage::new(directory.path()).open().unwrap();
    let primary = open_lix().with_storage(storage.clone()).await.unwrap();
    install_markdown(&primary).await;
    storage.start_sync(&primary).await.unwrap();
    for index in 0..3 {
        semantic_edit(&primary, &format!("manual {index}")).await;
        assert_eq!(
            std::fs::read(&path).unwrap(),
            format!("manual {index}\n").as_bytes()
        );
        let external = format!("external {index}\n");
        std::fs::write(&path, external.as_bytes()).unwrap();
        storage
            .sync_disk_to_lix()
            .await
            .expect("external edits must import after primary semantic edits");
        let rows = primary
            .execute("SELECT content FROM lix_file WHERE path='/a.md'", &[])
            .await
            .unwrap();
        assert_eq!(
            rows.rows()[0].get::<Vec<u8>>("content").unwrap(),
            external.as_bytes()
        );
    }
    storage.stop_sync().await.unwrap();
    primary.close().await.unwrap();
}

#[tokio::test]
async fn mixed_read_batches_do_not_acknowledge_aggregate_file_bytes() {
    for (coherent, aggregate_first) in [(true, false), (true, true), (false, false), (false, true)]
    {
        let storage = Memory::new();
        let primary = open_lix().with_storage(storage.clone()).await.unwrap();
        install_markdown(&primary).await;
        let backing = primary.open_storage_session(storage).await.unwrap();
        backing
            .execute(
                "INSERT INTO lix_file(path,content) VALUES('/a.md',$1),('/b.md',$1)",
                &[Value::Blob(b"initial\n".to_vec().into())],
            )
            .await
            .unwrap();
        semantic_edit(&primary, "primary edit").await;
        let mut statements: [(&str, &[Value]); 2] = [
            ("SELECT content FROM lix_file WHERE path='/a.md'", &[]),
            (
                "SELECT sum(length(content)) FROM lix_file WHERE path='/b.md'",
                &[],
            ),
        ];
        if aggregate_first {
            statements.reverse();
        }
        if coherent {
            backing
                .execute_coherent_read_batch(&statements)
                .await
                .unwrap();
        } else {
            backing
                .execute_batch(
                    &statements
                        .iter()
                        .map(|(sql, params)| ExecuteBatchStatement {
                            label: None,
                            sql: (*sql).to_owned(),
                            params: params.to_vec(),
                        })
                        .collect::<Vec<_>>(),
                )
                .await
                .unwrap();
        }
        let error = backing
            .execute(
                "UPDATE lix_file SET content=$1 WHERE path='/b.md'",
                &[Value::Blob(b"stale replacement\n".to_vec().into())],
            )
            .await
            .expect_err("aggregate results must not authorize replacing unseen current file bytes");
        assert_eq!(error.code, lix::LixError::CODE_PLUGIN_OBSERVATION_STALE);
        let rows = primary
            .execute("SELECT content FROM lix_file WHERE path='/b.md'", &[])
            .await
            .unwrap();
        assert_eq!(
            rows.rows()[0].get::<Vec<u8>>("content").unwrap(),
            b"primary edit\n"
        );
        backing.close().await.unwrap();
        primary.close().await.unwrap();
    }
}

#[tokio::test]
async fn mixed_read_batches_only_acknowledge_returned_file_bytes() {
    for coherent in [true, false] {
        for file_query in [
            "SELECT content FROM lix_file WHERE name='a.md' LIMIT 1",
            "SELECT content FROM lix_file WHERE name LIKE '_.md' ORDER BY name LIMIT 1",
        ] {
            for files_first in [true, false] {
                let storage = Memory::new();
                let primary = open_lix().with_storage(storage.clone()).await.unwrap();
                install_markdown(&primary).await;
                let backing = primary.open_storage_session(storage).await.unwrap();
                backing
                    .execute(
                        "INSERT INTO lix_file(path,content) VALUES('/a.md',$1),('/b.md',$1)",
                        &[Value::Blob(b"initial\n".to_vec().into())],
                    )
                    .await
                    .unwrap();
                semantic_edit(&primary, "primary edit").await;
                let mut statements: [(&str, &[Value]); 2] = [
                    ("SELECT path FROM lix_directory ORDER BY path", &[]),
                    (file_query, &[]),
                ];
                if files_first {
                    statements.reverse();
                }
                let results = if coherent {
                    backing
                        .execute_coherent_read_batch(&statements)
                        .await
                        .unwrap()
                        .results
                } else {
                    backing
                        .execute_batch(
                            &statements
                                .iter()
                                .map(|(sql, params)| ExecuteBatchStatement {
                                    label: None,
                                    sql: (*sql).to_owned(),
                                    params: params.to_vec(),
                                })
                                .collect::<Vec<_>>(),
                        )
                        .await
                        .unwrap()
                };
                let result = &results[usize::from(!files_first)];
                assert_eq!(result.len(), 1);
                assert_eq!(
                    result.rows()[0].get::<Vec<u8>>("content").unwrap(),
                    b"primary edit\n"
                );
                backing
                    .execute(
                        "UPDATE lix_file SET content=$1 WHERE path='/a.md'",
                        &[Value::Blob(b"seen replacement\n".to_vec().into())],
                    )
                    .await
                    .expect("the returned file must be acknowledged");
                let error = backing
                    .execute(
                        "UPDATE lix_file SET content=$1 WHERE path='/b.md'",
                        &[Value::Blob(b"unseen replacement\n".to_vec().into())],
                    )
                    .await
                    .expect_err("a filtered or limited-out file must retain its stale observation");
                assert_eq!(error.code, lix::LixError::CODE_PLUGIN_OBSERVATION_STALE);
                let rows = primary
                    .execute("SELECT content FROM lix_file WHERE path='/b.md'", &[])
                    .await
                    .unwrap();
                assert_eq!(
                    rows.rows()[0].get::<Vec<u8>>("content").unwrap(),
                    b"primary edit\n"
                );
                backing.close().await.unwrap();
                primary.close().await.unwrap();
            }
        }
    }
}
