//! Plugin-backed file writes in explicit transactions conflict at file
//! granularity and rebase onto unrelated concurrent commits (#1900).

use lix::{Lix, LixError, Value, open_lix};
use std::io::{Cursor, Write};

async fn workspace() -> Lix {
    let lix = open_lix().await.unwrap();
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
    lix
}

async fn insert_file(lix: &Lix, path: &str, content: &str) -> String {
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) RETURNING id",
        &[
            Value::Text(path.into()),
            Value::Blob(content.as_bytes().to_vec().into()),
        ],
    )
    .await
    .unwrap()
    .rows()[0]
        .get("id")
        .unwrap()
}

async fn write_file(lix: &Lix, id: &str, content: &str) {
    lix.execute(
        "UPDATE lix_file SET content = $2 WHERE id = $1",
        &[
            Value::Text(id.into()),
            Value::Blob(content.as_bytes().to_vec().into()),
        ],
    )
    .await
    .unwrap();
}

async fn file_text(lix: &Lix, id: &str) -> String {
    let bytes: Vec<u8> = lix
        .execute(
            "SELECT content FROM lix_file WHERE id = $1",
            &[Value::Text(id.into())],
        )
        .await
        .unwrap()
        .rows()[0]
        .get("content")
        .unwrap();
    String::from_utf8(bytes).unwrap()
}

async fn node_count(lix: &Lix, file_id: &str) -> i64 {
    lix.execute(
        "SELECT count(*) AS n FROM markdown_node WHERE lixcol_file_id = $1",
        &[Value::Text(file_id.into())],
    )
    .await
    .unwrap()
    .rows()[0]
        .get("n")
        .unwrap()
}

#[tokio::test]
async fn markdown_write_conflicts_with_a_concurrent_write_to_other_blocks_of_the_same_file() {
    let lix = workspace().await;
    let file = insert_file(&lix, "/doc.md", "# Title\n\nFirst.\n\nSecond.\n").await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "UPDATE lix_file SET content = $2 WHERE id = $1",
        &[
            Value::Text(file.clone()),
            Value::Blob(b"# Title\n\nFirst, edited.\n\nSecond.\n".to_vec().into()),
        ],
    )
    .await
    .unwrap();
    // The concurrent edit touches a different paragraph; the plugin still
    // re-derived the whole file from stale content in the transaction.
    write_file(&lix, &file, "# Title\n\nFirst.\n\nSecond, concurrent.\n").await;

    let error = tx.commit().await.unwrap_err();
    assert_eq!(error.code, LixError::CODE_TRANSACTION_CONFLICT, "{error:?}");
    let details = error.details.as_ref().unwrap();
    // The transaction's own write resolved the file through its
    // materialization root, so the overlap may surface as a read or a write.
    assert!(
        details["overlapKind"] == "write" || details["overlapKind"] == "read",
        "{details}"
    );
    assert!(
        details["overlaps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|overlap| overlap["fileId"] == file.as_str()),
        "{details}"
    );
    assert_eq!(
        file_text(&lix, &file).await,
        "# Title\n\nFirst.\n\nSecond, concurrent.\n"
    );
}

#[tokio::test]
async fn markdown_writes_to_different_files_rebase_onto_each_other() {
    let lix = workspace().await;
    let a = insert_file(&lix, "/a.md", "# A\n\nOne.\n").await;
    let b = insert_file(&lix, "/b.md", "# B\n\nOne.\n").await;
    let other = lix.open_another_session().await.unwrap();

    let mut first = lix.begin_transaction().await.unwrap();
    let mut second = other.begin_transaction().await.unwrap();
    first
        .execute(
            "UPDATE lix_file SET content = $2 WHERE id = $1",
            &[
                Value::Text(a.clone()),
                Value::Blob(b"# A\n\nOne.\n\nTwo.\n".to_vec().into()),
            ],
        )
        .await
        .unwrap();
    second
        .execute(
            "UPDATE lix_file SET content = $2 WHERE id = $1",
            &[
                Value::Text(b.clone()),
                Value::Blob(b"# B\n\nOne, edited.\n".to_vec().into()),
            ],
        )
        .await
        .unwrap();
    first.commit().await.unwrap();
    second.commit().await.unwrap();

    assert_eq!(file_text(&lix, &a).await, "# A\n\nOne.\n\nTwo.\n");
    assert_eq!(file_text(&lix, &b).await, "# B\n\nOne, edited.\n");
    // The derived rows of both files match a fresh derivation of their
    // committed content.
    let reference_a = insert_file(&lix, "/reference-a.md", "# A\n\nOne.\n\nTwo.\n").await;
    let reference_b = insert_file(&lix, "/reference-b.md", "# B\n\nOne, edited.\n").await;
    assert_eq!(
        node_count(&lix, &a).await,
        node_count(&lix, &reference_a).await
    );
    assert_eq!(
        node_count(&lix, &b).await,
        node_count(&lix, &reference_b).await
    );
    other.close().await.unwrap();
}

#[tokio::test]
async fn markdown_write_and_dependent_row_update_rebase_over_unrelated_writes() {
    let lix = workspace().await;
    let file = insert_file(&lix, "/doc.md", "# Doc\n\nBody.\n").await;
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('doc-target', 'old')",
        &[],
    )
    .await
    .unwrap();

    // Atelier's shape: save a file and retarget rows that depend on it in
    // one transaction while unrelated writes (a reply, a pasted image, an
    // edit of another document) land concurrently.
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "UPDATE lix_file SET content = $2 WHERE id = $1",
        &[
            Value::Text(file.clone()),
            Value::Blob(b"# Doc\n\nBody, saved.\n".to_vec().into()),
        ],
    )
    .await
    .unwrap();
    tx.execute(
        "UPDATE lix_key_value SET value = 'new' WHERE key = 'doc-target'",
        &[],
    )
    .await
    .unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('reply', 'hi')",
        &[],
    )
    .await
    .unwrap();
    insert_file(&lix, "/image.png", "png").await;
    insert_file(&lix, "/other.md", "# Other\n").await;

    tx.commit().await.unwrap();
    assert_eq!(file_text(&lix, &file).await, "# Doc\n\nBody, saved.\n");
    let target: serde_json::Value = lix
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'doc-target'",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
        .get("value")
        .unwrap();
    assert_eq!(target, "new");
}

async fn read_path(lix: &Lix, path: &str) -> String {
    let bytes: Vec<u8> = lix
        .execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text(path.into())],
        )
        .await
        .unwrap()
        .rows()[0]
        .get("content")
        .unwrap();
    String::from_utf8(bytes).unwrap()
}

async fn write_path(lix: &Lix, path: &str, content: &str) -> Result<(), LixError> {
    lix.execute(
        "UPDATE lix_file SET content = $2 WHERE path = $1",
        &[
            Value::Text(path.into()),
            Value::Blob(content.as_bytes().to_vec().into()),
        ],
    )
    .await
    .map(|_| ())
}

/// #1904 repro 1: bytes read inside an explicit transaction are the base for
/// that transaction's own write, even after another session committed the
/// file more often than the plugin keeps superseded revisions.
#[tokio::test]
async fn transaction_read_is_the_base_for_its_markdown_write() {
    let a = workspace().await;
    insert_file(&a, "/doc.md", "# Title\n\nAlpha.\n\nBravo.\n").await;
    let b = a.open_another_session().await.unwrap();

    read_path(&a, "/doc.md").await;
    let text = read_path(&b, "/doc.md").await.replace("Bravo.", "Bravo 1.");
    write_path(&b, "/doc.md", &text).await.unwrap();
    let text = read_path(&b, "/doc.md")
        .await
        .replace("Bravo 1.", "Bravo 2.");
    write_path(&b, "/doc.md", &text).await.unwrap();

    let mut tx = a.begin_transaction().await.unwrap();
    let bytes: Vec<u8> = tx
        .execute("SELECT content FROM lix_file WHERE path = '/doc.md'", &[])
        .await
        .unwrap()
        .rows()[0]
        .get("content")
        .unwrap();
    let text = String::from_utf8(bytes)
        .unwrap()
        .replace("Alpha.", "Alpha A.");
    tx.execute(
        "UPDATE lix_file SET content = $1 WHERE path = '/doc.md'",
        &[Value::Blob(text.into_bytes().into())],
    )
    .await
    .expect("the transaction's own read must be a valid plugin base");
    tx.commit().await.unwrap();

    assert_eq!(
        read_path(&a, "/doc.md").await,
        "# Title\n\nAlpha A.\n\nBravo 2.\n"
    );
    b.close().await.unwrap();
}

/// #1904 repro 2: an auto-commit write that waited for the plugin actor while
/// another session published a newer document replays on a fresh snapshot
/// instead of failing (and losing the edit).
#[tokio::test]
async fn autocommit_markdown_write_replays_after_a_concurrent_commit_of_the_same_file() {
    let a = workspace().await;
    insert_file(&a, "/doc.md", "# Title\n\nAlpha.\n\nBravo.\n").await;
    let b = a.open_another_session().await.unwrap();

    for round in 0..20 {
        let a_text = read_path(&a, "/doc.md").await;
        let a_text = replace_line(&a_text, "Alpha", &format!("Alpha {round}."));
        let b_text = read_path(&b, "/doc.md").await;
        let b_text = replace_line(&b_text, "Bravo", &format!("Bravo {round}."));
        let mut tx = a.begin_transaction().await.unwrap();
        tx.execute(
            "UPDATE lix_file SET content = $1 WHERE path = '/doc.md'",
            &[Value::Blob(a_text.into_bytes().into())],
        )
        .await
        .unwrap();
        let (a_commit, b_write) = tokio::join!(tx.commit(), write_path(&b, "/doc.md", &b_text));
        b_write.unwrap_or_else(|error| panic!("round {round}: b's write was lost: {error:?}"));
        let content = read_path(&a, "/doc.md").await;
        assert!(
            content.contains(&format!("Bravo {round}.")),
            "round {round}: {content}"
        );
        match a_commit {
            Ok(_) => assert!(
                content.contains(&format!("Alpha {round}.")),
                "round {round}: {content}"
            ),
            Err(error) => assert_eq!(
                error.code,
                LixError::CODE_TRANSACTION_CONFLICT,
                "round {round}: {error:?}"
            ),
        }
    }
    b.close().await.unwrap();
}

fn replace_line(text: &str, prefix: &str, replacement: &str) -> String {
    text.lines()
        .map(|line| {
            if line.starts_with(prefix) {
                replacement.to_owned()
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

const CONVERSATION: &str = "01950000-0000-7000-8000-00000000c101";
const REPLY: &str = "01950000-0000-7000-8000-00000000c102";
const REPLY_BODY: &str = r#"{"_type":"zettel_doc","blocks":[{"_type":"zettel_block","_key":"p1","style":"normal","markDefs":[],"children":[{"_type":"zettel_span","_key":"s1","text":"reply","marks":[]}]}]}"#;

async fn first_paragraph_node(lix: &Lix, file_id: &str) -> String {
    lix.execute(
        "SELECT id FROM markdown_node WHERE lixcol_file_id = $1 AND kind = 'paragraph' \
         ORDER BY order_key LIMIT 1",
        &[Value::Text(file_id.into())],
    )
    .await
    .unwrap()
    .rows()[0]
        .get("id")
        .unwrap()
}

/// #1900's real-world case with real conversations on Markdown blocks:
/// saving the file and re-targeting a conversation in one transaction
/// commits while a reply, an image and another document land concurrently.
#[tokio::test]
async fn markdown_save_and_conversation_retarget_commit_after_a_concurrent_reply() {
    let lix = workspace().await;
    let file = insert_file(&lix, "/doc.md", "# Doc\n\nBody.\n").await;
    let block = first_paragraph_node(&lix, &file).await;
    lix.execute(
        "INSERT INTO lix_conversation (id, target) VALUES ($1, lix_row_ref('lix_file', NULL, $2))",
        &[Value::Text(CONVERSATION.into()), Value::Text(file.clone())],
    )
    .await
    .unwrap();

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "UPDATE lix_file SET content = $2 WHERE id = $1",
        &[
            Value::Text(file.clone()),
            Value::Blob(b"# Doc\n\nBody, saved.\n".to_vec().into()),
        ],
    )
    .await
    .unwrap();
    tx.execute(
        "UPDATE lix_conversation SET target = lix_row_ref('markdown_node', $2, $3) WHERE id = $1",
        &[
            Value::Text(CONVERSATION.into()),
            Value::Text(file.clone()),
            Value::Text(block.clone()),
        ],
    )
    .await
    .unwrap();
    lix.execute(
        "INSERT INTO lix_comment (id, conversation_id, body) VALUES ($1, $2, CAST($3 AS JSONB))",
        &[
            Value::Text(REPLY.into()),
            Value::Text(CONVERSATION.into()),
            Value::Text(REPLY_BODY.into()),
        ],
    )
    .await
    .unwrap();
    insert_file(&lix, "/image.png", "png").await;
    insert_file(&lix, "/other.md", "# Other\n").await;

    tx.commit().await.unwrap();
    assert_eq!(file_text(&lix, &file).await, "# Doc\n\nBody, saved.\n");
    let retargeted = lix
        .execute(
            "SELECT CAST(target AS TEXT) = CAST(lix_row_ref('markdown_node', $2, $3) AS TEXT) \
             AS retargeted FROM lix_conversation WHERE id = $1",
            &[
                Value::Text(CONVERSATION.into()),
                Value::Text(file.clone()),
                Value::Text(block.clone()),
            ],
        )
        .await
        .unwrap();
    assert!(retargeted.rows()[0].get::<bool>("retargeted").unwrap());
    let replies = lix
        .execute(
            "SELECT id FROM lix_comment WHERE conversation_id = $1",
            &[Value::Text(CONVERSATION.into())],
        )
        .await
        .unwrap();
    assert_eq!(replies.rows().len(), 1);
}

/// Bytes of a plugin-backed file that the transaction read are part of its
/// read set: a concurrent edit of that file conflicts even though the
/// transaction wrote something else.
#[tokio::test]
async fn reading_markdown_content_conflicts_with_a_concurrent_edit_of_that_file() {
    let lix = workspace().await;
    let file = insert_file(&lix, "/doc.md", "# Doc\n\nFirst.\n\nSecond.\n").await;
    let other = insert_file(&lix, "/other.md", "# Other\n").await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "SELECT content FROM lix_file WHERE id = $1",
        &[Value::Text(file.clone())],
    )
    .await
    .unwrap();
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('summary', 'two paragraphs')",
        &[],
    )
    .await
    .unwrap();
    write_file(&lix, &file, "# Doc\n\nFirst.\n\nSecond.\n\nThird.\n").await;
    let error = tx.commit().await.unwrap_err();
    assert_eq!(error.code, LixError::CODE_TRANSACTION_CONFLICT, "{error:?}");

    // An edit of a different file does not interfere with the same read.
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "SELECT content FROM lix_file WHERE id = $1",
        &[Value::Text(file.clone())],
    )
    .await
    .unwrap();
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('summary', 'three paragraphs')",
        &[],
    )
    .await
    .unwrap();
    write_file(&lix, &other, "# Other, edited\n").await;
    tx.commit().await.unwrap();
}
