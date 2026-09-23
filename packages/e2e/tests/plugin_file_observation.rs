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
async fn transaction_read_refreshes_plugin_observation_after_two_foreign_edits() {
    let lix = open_lix().with_storage(Memory::new()).await.unwrap();
    install_markdown(&lix).await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/doc.md',$1)",
        &[Value::Blob(
            b"# Title\n\nAlpha.\n\nBravo.\n".to_vec().into(),
        )],
    )
    .await
    .unwrap();
    let other = lix.open_another_session().await.unwrap();
    let read = |result: &lix::ExecuteResult| {
        String::from_utf8(result.rows()[0].get::<Vec<u8>>("content").unwrap()).unwrap()
    };
    let query = "SELECT content FROM lix_file WHERE path='/doc.md'";
    let update = "UPDATE lix_file SET content=$1 WHERE path='/doc.md'";
    read(&lix.execute(query, &[]).await.unwrap());
    for suffix in ["1", "2"] {
        let text = read(&other.execute(query, &[]).await.unwrap())
            .replace("Bravo.", &format!("Bravo {suffix}."))
            .replace("Bravo 1.", &format!("Bravo {suffix}."));
        other
            .execute(update, &[Value::Blob(text.into_bytes().into())])
            .await
            .unwrap();
    }
    let mut transaction = lix.begin_transaction().await.unwrap();
    let text = read(&transaction.execute(query, &[]).await.unwrap()).replace("Alpha.", "Alpha A.");
    transaction
        .execute(update, &[Value::Blob(text.into_bytes().into())])
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(
        read(&other.execute(query, &[]).await.unwrap()),
        "# Title\n\nAlpha A.\n\nBravo 2.\n"
    );
    other.close().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn committed_transaction_read_refreshes_session_observation() {
    let lix = open_lix().with_storage(Memory::new()).await.unwrap();
    install_markdown(&lix).await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/doc.md',$1)",
        &[Value::Blob(b"Original.\n".to_vec().into())],
    )
    .await
    .unwrap();
    let other = lix.open_another_session().await.unwrap();
    let query = "SELECT content FROM lix_file WHERE path='/doc.md'";
    let update = "UPDATE lix_file SET content=$1 WHERE path='/doc.md'";
    lix.execute(query, &[]).await.unwrap();
    for text in [b"Other 1.\n".as_slice(), b"Other 2.\n".as_slice()] {
        other.execute(query, &[]).await.unwrap();
        other
            .execute(update, &[Value::Blob(text.to_vec().into())])
            .await
            .unwrap();
    }
    let mut transaction = lix.begin_transaction().await.unwrap();
    let bytes = transaction.execute(query, &[]).await.unwrap().rows()[0]
        .get::<Vec<u8>>("content")
        .unwrap();
    assert_eq!(bytes, b"Other 2.\n");
    transaction.commit().await.unwrap();
    lix.execute(update, &[Value::Blob(b"After commit.\n".to_vec().into())])
        .await
        .expect("the committed read must refresh the session's write base");
    other.close().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn rolled_back_transaction_read_does_not_refresh_session_observation() {
    let lix = open_lix().with_storage(Memory::new()).await.unwrap();
    install_markdown(&lix).await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/doc.md',$1)",
        &[Value::Blob(b"Original.\n".to_vec().into())],
    )
    .await
    .unwrap();
    let other = lix.open_another_session().await.unwrap();
    let query = "SELECT content FROM lix_file WHERE path='/doc.md'";
    let update = "UPDATE lix_file SET content=$1 WHERE path='/doc.md'";
    lix.execute(query, &[]).await.unwrap();
    for text in [b"Other 1.\n".as_slice(), b"Other 2.\n".as_slice()] {
        other.execute(query, &[]).await.unwrap();
        other
            .execute(update, &[Value::Blob(text.to_vec().into())])
            .await
            .unwrap();
    }
    let mut transaction = lix.begin_transaction().await.unwrap();
    transaction.execute(query, &[]).await.unwrap();
    transaction.rollback().await.unwrap();
    let error = lix
        .execute(update, &[Value::Blob(b"Unseen.\n".to_vec().into())])
        .await
        .unwrap_err();
    assert_eq!(error.code, lix::LixError::CODE_PLUGIN_OBSERVATION_STALE);
    other.close().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn transformed_transaction_read_does_not_acknowledge_plugin_bytes() {
    let lix = open_lix().with_storage(Memory::new()).await.unwrap();
    install_markdown(&lix).await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/doc.md',$1)",
        &[Value::Blob(b"Original.\n".to_vec().into())],
    )
    .await
    .unwrap();
    let other = lix.open_another_session().await.unwrap();
    lix.execute("SELECT content FROM lix_file WHERE path='/doc.md'", &[])
        .await
        .unwrap();
    for text in [b"Other 1.\n".as_slice(), b"Other 2.\n".as_slice()] {
        other
            .execute("SELECT content FROM lix_file WHERE path='/doc.md'", &[])
            .await
            .unwrap();
        other
            .execute(
                "UPDATE lix_file SET content=$1 WHERE path='/doc.md'",
                &[Value::Blob(text.to_vec().into())],
            )
            .await
            .unwrap();
    }
    let mut transaction = lix.begin_transaction().await.unwrap();
    transaction
        .execute(
            "SELECT length(content) FROM lix_file WHERE path='/doc.md'",
            &[],
        )
        .await
        .unwrap();
    let error = transaction
        .execute(
            "UPDATE lix_file SET content=$1 WHERE path='/doc.md'",
            &[Value::Blob(b"Unseen.\n".to_vec().into())],
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, lix::LixError::CODE_PLUGIN_OBSERVATION_STALE);
    transaction.rollback().await.unwrap();
    other.close().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn repeated_semantic_writes_chain_after_detaching_in_one_transaction() {
    let lix = open_lix().with_storage(Memory::new()).await.unwrap();
    install_markdown(&lix).await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/doc.md',$1)",
        &[Value::Blob(b"Initial.\n".to_vec().into())],
    )
    .await
    .unwrap();
    let file_id: String = lix
        .execute("SELECT id FROM lix_file WHERE path='/doc.md'", &[])
        .await
        .unwrap()
        .rows()[0]
        .get("id")
        .unwrap();

    let mut transaction = lix.begin_transaction().await.unwrap();
    for text in ["First edit.", "Second edit."] {
        transaction
            .execute(
                "UPDATE markdown_node SET payload_json=$1 WHERE kind='paragraph' AND lixcol_file_id=$2",
                &[
                    Value::Text(
                        serde_json::json!({"inline": [{"type": "text", "value": text}]})
                            .to_string(),
                    ),
                    Value::Text(file_id.clone()),
                ],
            )
            .await
            .expect("the second semantic statement should reattach and chain the detached actor");
    }
    transaction.commit().await.unwrap();

    let content = lix
        .execute("SELECT content FROM lix_file WHERE path='/doc.md'", &[])
        .await
        .unwrap()
        .rows()[0]
        .get::<Vec<u8>>("content")
        .unwrap();
    assert_eq!(content, b"Second edit.\n");
    lix.close().await.unwrap();
}

#[tokio::test]
async fn explicit_transaction_admits_semantic_writes_under_store_pressure() {
    // The default repository actor budget is ten live Stores. Touch one more
    // Markdown file than that, while each prior transaction statement has
    // detached its completed publication.
    let lix = open_lix().with_storage(Memory::new()).await.unwrap();
    install_markdown(&lix).await;
    let mut files = Vec::new();
    for index in 0..11 {
        let path = format!("/resource-{index}.md");
        lix.execute(
            "INSERT INTO lix_file(path,content) VALUES($1,$2)",
            &[
                Value::Text(path.clone()),
                Value::Blob(format!("Initial {index}.\n").into_bytes().into()),
            ],
        )
        .await
        .unwrap();
        let file_id: String = lix
            .execute(
                "SELECT id FROM lix_file WHERE path=$1",
                &[Value::Text(path.clone())],
            )
            .await
            .unwrap()
            .rows()[0]
            .get("id")
            .unwrap();
        files.push((path, file_id));
    }

    let mut transaction = lix.begin_transaction().await.unwrap();
    for (index, (_, file_id)) in files.iter().enumerate() {
        transaction
            .execute(
                "UPDATE markdown_node SET payload_json=$1 WHERE kind='paragraph' AND lixcol_file_id=$2",
                &[
                    Value::Text(
                        serde_json::json!({"inline": [{"type": "text", "value": format!("Edited {index}.")}]})
                            .to_string(),
                    ),
                    Value::Text(file_id.clone()),
                ],
            )
            .await
            .unwrap_or_else(|error| {
                panic!("semantic edit {index} should fit through Store admission: {error}")
            });
    }
    transaction.commit().await.unwrap();

    for (index, (path, _)) in files.iter().enumerate() {
        let content = lix
            .execute(
                "SELECT content FROM lix_file WHERE path=$1",
                &[Value::Text(path.clone())],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<Vec<u8>>("content")
            .unwrap();
        assert_eq!(content, format!("Edited {index}.\n").as_bytes());
    }
    lix.close().await.unwrap();
}

#[tokio::test]
async fn concurrent_plugin_writes_replay_auto_commit_snapshot_conflicts() {
    let lix = open_lix().with_storage(Memory::new()).await.unwrap();
    install_markdown(&lix).await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/doc.md',$1)",
        &[Value::Blob(b"Alpha 0.\n\nBravo 0.\n".to_vec().into())],
    )
    .await
    .unwrap();
    let other = lix.open_another_session().await.unwrap();
    let query = "SELECT content FROM lix_file WHERE path='/doc.md'";
    let update = "UPDATE lix_file SET content=$1 WHERE path='/doc.md'";
    for index in 1..=30 {
        let a_text = String::from_utf8(
            lix.execute(query, &[]).await.unwrap().rows()[0]
                .get::<Vec<u8>>("content")
                .unwrap(),
        )
        .unwrap();
        let b_text = String::from_utf8(
            other.execute(query, &[]).await.unwrap().rows()[0]
                .get::<Vec<u8>>("content")
                .unwrap(),
        )
        .unwrap();
        let mut transaction = lix.begin_transaction().await.unwrap();
        let a_text = a_text
            .lines()
            .map(|line| {
                if line.starts_with("Alpha ") {
                    format!("Alpha {index}.")
                } else {
                    line.to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let b_text = b_text
            .lines()
            .map(|line| {
                if line.starts_with("Bravo ") {
                    format!("Bravo {index}.")
                } else {
                    line.to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        transaction
            .execute(update, &[Value::Blob(a_text.into_bytes().into())])
            .await
            .unwrap();
        let b_params = [Value::Blob(b_text.into_bytes().into())];
        let (commit, auto_commit) =
            tokio::join!(transaction.commit(), other.execute(update, &b_params));
        let committed_alpha = commit.is_ok();
        if let Err(error) = commit {
            assert_eq!(error.code, lix::LixError::CODE_TRANSACTION_CONFLICT);
        }
        auto_commit.expect("the auto-commit plugin edit must replay a snapshot conflict");
        let current = String::from_utf8(
            other.execute(query, &[]).await.unwrap().rows()[0]
                .get::<Vec<u8>>("content")
                .unwrap(),
        )
        .unwrap();
        assert!(current.contains(&format!("Bravo {index}.")));
        if committed_alpha {
            assert!(current.contains(&format!("Alpha {index}.")));
        }
    }
    other.close().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn stale_transaction_snapshot_conflicts_and_keeps_current_actor_usable() {
    let lix = open_lix().with_storage(Memory::new()).await.unwrap();
    install_markdown(&lix).await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/doc.md',$1)",
        &[Value::Blob(b"Alpha.\n\nBravo.\n".to_vec().into())],
    )
    .await
    .unwrap();
    let other = lix.open_another_session().await.unwrap();
    let query = "SELECT content FROM lix_file WHERE path='/doc.md'";
    let update = "UPDATE lix_file SET content=$1 WHERE path='/doc.md'";
    let before = lix.execute(query, &[]).await.unwrap().rows()[0]
        .get::<Vec<u8>>("content")
        .unwrap();
    let mut transaction = lix.begin_transaction().await.unwrap();
    transaction.execute(query, &[]).await.unwrap();
    other.execute(query, &[]).await.unwrap();
    other
        .execute(
            update,
            &[Value::Blob(b"Alpha.\n\nBravo 1.\n".to_vec().into())],
        )
        .await
        .unwrap();
    let attempted = String::from_utf8(before)
        .unwrap()
        .replace("Alpha.", "Alpha A.");
    let write = transaction
        .execute(update, &[Value::Blob(attempted.into_bytes().into())])
        .await;
    let error = match write {
        Ok(_) => transaction.commit().await.unwrap_err(),
        Err(error) => {
            transaction.rollback().await.unwrap();
            error
        }
    };
    assert_eq!(error.code, lix::LixError::CODE_TRANSACTION_CONFLICT);
    lix.execute(
        update,
        &[Value::Blob(b"Alpha A.\n\nBravo.\n".to_vec().into())],
    )
    .await
    .expect("the stale observation must still rebase through the current actor");
    let content = other.execute(query, &[]).await.unwrap().rows()[0]
        .get::<Vec<u8>>("content")
        .unwrap();
    assert_eq!(content, b"Alpha A.\n\nBravo 1.\n");
    other.close().await.unwrap();
    lix.close().await.unwrap();
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
                        .results
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

#[tokio::test]
async fn fresh_import_and_plugin_reselection_preserve_rows_across_rollback() {
    let lix = open_lix().with_storage(Memory::new()).await.unwrap();
    install_markdown(&lix).await;
    let wasm = std::fs::read(env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_plugin_json")).unwrap();
    let mut archive = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, content) in [
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
        archive.start_file(path, options).unwrap();
        archive.write_all(content).unwrap();
    }
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/.lix/plugins/plugin_json.lixplugin',$1)",
        &[Value::Blob(archive.finish().unwrap().into_inner().into())],
    )
    .await
    .unwrap();

    // Both ownerless files enter the fresh-import worker batch in one statement.
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/switch.md',$1),('/keep.md',$2)",
        &[
            Value::Blob(b"switch original\n".to_vec().into()),
            Value::Blob(b"keep original\n".to_vec().into()),
        ],
    )
    .await
    .unwrap();
    let file_id: String = lix
        .execute("SELECT id FROM lix_file WHERE path='/switch.md'", &[])
        .await
        .unwrap()
        .rows()[0]
        .get("id")
        .unwrap();
    let original_rows = lix
        .execute(
            "SELECT id FROM markdown_node WHERE lixcol_file_id=$1 ORDER BY id",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap()
        .rows()
        .iter()
        .map(|row| row.get::<String>("id").unwrap())
        .collect::<Vec<_>>();
    assert!(!original_rows.is_empty());

    let json_bytes = br#"{"replacement":"accepted"}"#.to_vec();
    let params = [
        Value::Blob(json_bytes.clone().into()),
        Value::Text(file_id.clone()),
    ];
    let mut transaction = lix.begin_transaction().await.unwrap();
    transaction
        .execute(
            "UPDATE lix_file SET path='/switch.json',content=$1 WHERE id=$2",
            &params,
        )
        .await
        .expect("a different selected plugin must open a fresh actor");
    assert!(
        transaction
            .execute(
                "SELECT id FROM markdown_node WHERE lixcol_file_id=$1",
                &[Value::Text(file_id.clone())],
            )
            .await
            .unwrap()
            .rows()
            .is_empty()
    );
    assert_eq!(
        transaction
            .execute(
                "SELECT key FROM json_object_member WHERE lixcol_file_id=$1",
                &[Value::Text(file_id.clone())],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("key")
            .unwrap(),
        "replacement"
    );
    transaction.rollback().await.unwrap();

    let after_rollback = lix
        .execute(
            "SELECT id FROM markdown_node WHERE lixcol_file_id=$1 ORDER BY id",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap()
        .rows()
        .iter()
        .map(|row| row.get::<String>("id").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        after_rollback, original_rows,
        "rollback restores the original semantic identities"
    );
    assert!(
        lix.execute(
            "SELECT key FROM json_object_member WHERE lixcol_file_id=$1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap()
        .rows()
        .is_empty()
    );
    let original = lix
        .execute(
            "SELECT path,content FROM lix_file WHERE id=$1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(
        original.rows()[0].get::<String>("path").unwrap(),
        "/switch.md"
    );
    assert_eq!(
        original.rows()[0].get::<Vec<u8>>("content").unwrap(),
        b"switch original\n"
    );

    lix.execute(
        "UPDATE lix_file SET path='/switch.json',content=$1 WHERE id=$2",
        &params,
    )
    .await
    .expect("retry after rollback must publish the new plugin's actor");
    assert!(
        lix.execute(
            "SELECT id FROM markdown_node WHERE lixcol_file_id=$1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap()
        .rows()
        .is_empty()
    );
    let members = lix
        .execute(
            "SELECT key FROM json_object_member WHERE lixcol_file_id=$1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(members.rows().len(), 1);
    assert_eq!(
        members.rows()[0].get::<String>("key").unwrap(),
        "replacement"
    );
    let owner = lix
        .execute(
            "SELECT value FROM lix_key_value WHERE lixcol_file_id=$1 AND key='lix_plugin_owner_v2'",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(owner.rows().len(), 1);
    let Value::Jsonb(owner) = owner.rows()[0].get::<Value>("value").unwrap() else {
        panic!("owner must be JSON");
    };
    assert_eq!(owner.to_value()["plugin_key"], "plugin_json");
    let files = lix.execute("SELECT path,content FROM lix_file WHERE path IN ('/switch.json','/keep.md') ORDER BY path", &[]).await.unwrap();
    assert_eq!(files.rows().len(), 2);
    assert_eq!(
        files.rows()[0].get::<Vec<u8>>("content").unwrap(),
        b"keep original\n"
    );
    assert_eq!(
        files.rows()[1].get::<Vec<u8>>("content").unwrap(),
        json_bytes
    );

    // A subsequent ordinary edit exercises the newly published actor's authority.
    lix.execute(
        "UPDATE lix_file SET content=$1 WHERE id=$2",
        &[
            Value::Blob(br#"{"followup":"warm"}"#.to_vec().into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .unwrap();
    let members = lix
        .execute(
            "SELECT key FROM json_object_member WHERE lixcol_file_id=$1",
            &[Value::Text(file_id)],
        )
        .await
        .unwrap();
    assert_eq!(members.rows().len(), 1);
    assert_eq!(members.rows()[0].get::<String>("key").unwrap(), "followup");
    lix.close().await.unwrap();
}

mod json_row_mapping_qa;

#[tokio::test]
async fn markdown_sql_edits_and_documented_insert_survive_reopen() {
    let storage = Memory::new();
    let lix = open_lix().with_storage(storage.clone()).await.unwrap();
    install_markdown(&lix).await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/sql-edit.md',$1)",
        &[Value::Blob(b"`old`\n".to_vec().into())],
    )
    .await
    .unwrap();
    let rows = lix.execute("SELECT id, parent_id, lixcol_file_id, payload_json FROM markdown_node WHERE kind='paragraph'", &[]).await.unwrap();
    let row = &rows.rows()[0];
    let id: String = row.get("id").unwrap();
    let root: String = row.get("parent_id").unwrap();
    let file: String = row.get("lixcol_file_id").unwrap();
    let Value::Jsonb(payload) = row.get::<Value>("payload_json").unwrap() else {
        panic!("native JSONB payload")
    };
    let mut payload = payload.to_value();
    payload["inline"][0]["value"] = serde_json::json!("new");
    lix.execute(
        "UPDATE markdown_node SET payload_json=$1 WHERE id=$2 AND lixcol_file_id=$3",
        &[
            Value::Text(payload.to_string()),
            Value::Text(id.clone()),
            Value::Text(file.clone()),
        ],
    )
    .await
    .unwrap();
    lix.execute(
        "UPDATE markdown_node SET order_key='40' WHERE id=$1",
        &[Value::Text(id.clone())],
    )
    .await
    .unwrap();
    lix.execute("INSERT INTO markdown_node (kind,parent_id,order_key,payload_json,format_json,lixcol_file_id) VALUES ('paragraph',$1,'60','{\"inline\":[{\"type\":\"text\",\"value\":\"New paragraph.\"}]}','{}',$2)", &[Value::Text(root), Value::Text(file.clone())]).await.unwrap();
    lix.close().await.unwrap();
    let reopened = open_lix().with_storage(storage).await.unwrap();
    let bytes = reopened
        .execute(
            "SELECT content FROM lix_file WHERE path='/sql-edit.md'",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<Vec<u8>>("content")
        .unwrap();
    assert_eq!(bytes, b"`new`\n\nNew paragraph.\n");
    let rows = reopened
        .execute(
            "SELECT payload_json FROM markdown_node WHERE id=$1 AND lixcol_file_id=$2",
            &[Value::Text(id), Value::Text(file)],
        )
        .await
        .unwrap();
    let Value::Jsonb(payload) = rows.rows()[0].get::<Value>("payload_json").unwrap() else {
        panic!("native JSONB payload")
    };
    let payload = payload.to_value();
    assert_eq!(payload["inline"][0]["value"], "new");
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn markdown_rejected_semantic_edit_preserves_durable_rows_and_bytes() {
    let storage = Memory::new();
    let lix = open_lix().with_storage(storage.clone()).await.unwrap();
    install_markdown(&lix).await;
    let original = b"<div>keep</div>\n".to_vec();
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/guard.md',$1)",
        &[Value::Blob(original.clone().into())],
    )
    .await
    .unwrap();
    let before = lix
        .execute(
            "SELECT id,payload_json FROM markdown_node WHERE kind='html_block'",
            &[],
        )
        .await
        .unwrap();
    let id: String = before.rows()[0].get("id").unwrap();
    let payload: Value = before.rows()[0].get("payload_json").unwrap();
    let result = lix
        .execute(
            "UPDATE markdown_node SET payload_json=$1 WHERE id=$2",
            &[
                Value::Text(serde_json::json!({"value":"ordinary paragraph\n"}).to_string()),
                Value::Text(id.clone()),
            ],
        )
        .await;
    assert!(
        result.is_err(),
        "HTML row edit must not silently become a paragraph"
    );
    lix.close().await.unwrap();
    let reopened = open_lix().with_storage(storage).await.unwrap();
    let bytes = reopened
        .execute("SELECT content FROM lix_file WHERE path='/guard.md'", &[])
        .await
        .unwrap()
        .rows()[0]
        .get::<Vec<u8>>("content")
        .unwrap();
    assert_eq!(bytes, original);
    let after = reopened
        .execute(
            "SELECT payload_json FROM markdown_node WHERE id=$1",
            &[Value::Text(id)],
        )
        .await
        .unwrap();
    assert_eq!(
        after.rows()[0].get::<Value>("payload_json").unwrap(),
        payload
    );
    reopened.close().await.unwrap();
}
