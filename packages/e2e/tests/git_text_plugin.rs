use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use lix::storage::Storage;
use lix::{CreateBranchOptions, MergeBranchOptions, SwitchBranchOptions, Value, open_lix};
use std::io::{Cursor, Write};
use std::path::Path;

const PLUGIN_KEY: &str = "plugin_text";

#[derive(Debug, Clone, PartialEq, Eq)]
struct GitTextLine {
    id: String,
    order_key: String,
    content: Option<String>,
    content_base64: Option<String>,
    line_ending: String,
}

#[tokio::test]
async fn git_text_same_line_branch_conflict_uses_static_canonical_resolver() {
    let lix = open_lix().await.expect("workspace should open");
    install_plugin(&lix, &build_plugin_archive())
        .await
        .expect("Git text plugin should install");

    let path = "/same-line.txt";
    write_file(&lix, path, b"base\n")
        .await
        .expect("base line should import");
    let target_branch_id = lix.active_branch_id().await.expect("target branch");
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-000000000507".to_owned()),
            name: "Git text conflict source".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("source branch should create");

    write_file(&lix, path, b"target\n")
        .await
        .expect("target line should change");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .expect("source branch should activate");
    write_file(&lix, path, b"source\n")
        .await
        .expect("source line should change");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: target_branch_id,
    })
    .await
    .expect("target branch should reactivate");

    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("the default static resolver should resolve the line conflict");
    let merged = read_file(&lix, path)
        .await
        .expect("merged file should exist");
    assert!(
        matches!(merged.as_slice(), b"target\n" | b"source\n"),
        "canonical resolution must select one complete side"
    );
    lix.close().await.expect("workspace should close");
}

#[tokio::test]
async fn git_text_plugin_persists_lossless_line_rows_and_leaves_binary_raw() {
    let storage = lix::Memory::new();
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("workspace should open");
    install_plugin(&lix, &build_plugin_archive())
        .await
        .expect("Git text plugin should install");

    // Git considers this text despite invalid UTF-8 because its first 8 KiB
    // contain no NUL. The row payload is base64url so it remains byte-exact.
    let git_text = [0xff, b'\n', 0xfe, b'\n'].to_vec();
    let text_path = "/git-text.invalid-utf8";
    write_file(&lix, text_path, &git_text)
        .await
        .expect("NUL-free Git text should write");
    let text_file_id = file_id_at_path(&lix, text_path).await;
    assert_plugin_owned(&lix, &text_file_id, true).await;
    assert_semantic_rows(&lix, &text_file_id, &git_text).await;
    let text_rows = git_text_rows(&lix, &text_file_id).await;
    assert_eq!(text_rows.len(), 2);
    assert_eq!(render_rows(&text_rows), git_text);
    assert_eq!(read_file(&lix, text_path).await, Some(git_text.clone()));

    // A NUL within Git's scan window remains an ordinary raw binary file.
    let binary = b"opaque\0payload".to_vec();
    let binary_path = "/binary.dat";
    write_file(&lix, binary_path, &binary)
        .await
        .expect("Git binary should write without the text plugin");
    let binary_file_id = file_id_at_path(&lix, binary_path).await;
    assert_plugin_owned(&lix, &binary_file_id, false).await;
    assert_raw_blob_materialization(&lix, &binary_file_id).await;
    assert_eq!(read_file(&lix, binary_path).await, Some(binary.clone()));

    // Empty files are Git text too; their semantic representation is an
    // intentionally empty line table rather than a synthetic blank line.
    let empty_path = "/empty.txt";
    write_file(&lix, empty_path, b"")
        .await
        .expect("empty Git text should write");
    let empty_file_id = file_id_at_path(&lix, empty_path).await;
    assert_plugin_owned(&lix, &empty_file_id, true).await;
    assert_semantic_rows(&lix, &empty_file_id, b"").await;
    assert!(git_text_rows(&lix, &empty_file_id).await.is_empty());
    assert_eq!(read_file(&lix, empty_path).await, Some(Vec::new()));

    // Advance beyond the text write so the history surface must reconstruct
    // the exact bytes from the semantic snapshot at an earlier commit.
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('git-text-history-sidecar', 'later')",
        &[],
    )
    .await
    .expect("history sidecar should commit");
    let history_head = active_branch_head(&lix).await;

    lix.close().await.expect("workspace should close");
    let reopened = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("workspace should reopen");
    assert_history_file(&reopened, &history_head, &text_file_id, &git_text).await;
    assert_eq!(read_file(&reopened, text_path).await, Some(git_text));
    assert_eq!(read_file(&reopened, binary_path).await, Some(binary));
    assert_eq!(read_file(&reopened, empty_path).await, Some(Vec::new()));
    assert_eq!(
        render_rows(&git_text_rows(&reopened, &text_file_id).await),
        [0xff, b'\n', 0xfe, b'\n']
    );
    // This write forces a cold actor to rebuild its document from durable
    // rows after reopen and apply the successor in the same guest export.
    let reopened_successor = [0xff, b'\n', b'X', b'\n'].to_vec();
    write_file(&reopened, text_path, &reopened_successor)
        .await
        .expect("cold row reconstruction should accept a later line edit");
    let reopened_rows = git_text_rows(&reopened, &text_file_id).await;
    assert_eq!(reopened_rows[0].id, text_rows[0].id);
    assert_eq!(reopened_rows[1].id, text_rows[1].id);
    assert_eq!(render_rows(&reopened_rows), reopened_successor);
    assert_semantic_rows(&reopened, &text_file_id, &reopened_successor).await;

    // An inserted line receives a fresh durable identity. A following byte
    // edit must reopen from the persisted ID/order mapping rather than derive
    // identities from the original namespace and current ordinal.
    let inserted = [0xff, b'\n', b'M', b'\n', b'X', b'\n'].to_vec();
    write_file(&reopened, text_path, &inserted)
        .await
        .expect("line insertion should write");
    let inserted_rows = git_text_rows(&reopened, &text_file_id).await;
    assert_eq!(inserted_rows.len(), 3);
    let inserted_id = inserted_rows[1].id.clone();
    let edited_insert = [0xff, b'\n', b'N', b'\n', b'X', b'\n'].to_vec();
    write_file(&reopened, text_path, &edited_insert)
        .await
        .expect("second edit of inserted line should write");
    let edited_rows = git_text_rows(&reopened, &text_file_id).await;
    assert_eq!(edited_rows.len(), 3);
    assert_eq!(edited_rows[1].id, inserted_id);
    assert_eq!(render_rows(&edited_rows), edited_insert);

    // Crossing Git's NUL boundary must retire semantic ownership and make a
    // raw successor visible to history.
    let raw_successor = b"now\0binary\n".to_vec();
    write_file(&reopened, text_path, &raw_successor)
        .await
        .expect("NUL-bearing successor should become raw");
    assert_plugin_owned(&reopened, &text_file_id, false).await;
    assert_raw_blob_materialization(&reopened, &text_file_id).await;
    let raw_history_head = active_branch_head(&reopened).await;
    reopened
        .close()
        .await
        .expect("reopened workspace should close");
    let raw_reopened = open_lix()
        .with_storage(storage)
        .await
        .expect("workspace should reopen with the raw successor");
    assert_history_file(
        &raw_reopened,
        &raw_history_head,
        &text_file_id,
        &raw_successor,
    )
    .await;
    assert_eq!(
        read_file(&raw_reopened, text_path).await,
        Some(raw_successor)
    );
    raw_reopened
        .close()
        .await
        .expect("raw successor workspace should close");
}

#[tokio::test]
async fn git_text_plugin_reads_only_a_large_after_range_and_updates_one_line_row() {
    const MIB: usize = 1024 * 1024;

    let lix = open_lix().await.expect("workspace should open");
    install_plugin(&lix, &build_plugin_archive())
        .await
        .expect("Git text plugin should install");

    // This single line is deliberately large enough that the replacement is
    // sent as an `AfterRange`, not copied through the inline Component input.
    let mut before = vec![b'a'; 4 * MIB];
    before.extend_from_slice(b"\nuntouched\n");
    let path = "/large.txt";
    write_file(&lix, path, &before)
        .await
        .expect("initial Git text should write");
    let file_id = file_id_at_path(&lix, path).await;
    let before_rows = git_text_rows(&lix, &file_id).await;
    assert_eq!(before_rows.len(), 2);

    let replacement_offset = MIB;
    let replacement_len = MIB + 1;
    let mut after = before.clone();
    after[replacement_offset..replacement_offset + replacement_len].fill(b'b');

    write_file(&lix, path, &after)
        .await
        .expect("large localized line edit should write");
    let after_rows = git_text_rows(&lix, &file_id).await;
    assert_eq!(after_rows.len(), 2);
    assert_eq!(after_rows[0].id, before_rows[0].id);
    assert_eq!(after_rows[1], before_rows[1]);
    assert_eq!(render_rows(&after_rows), after);
    assert_eq!(read_file(&lix, path).await, Some(after.clone()));
    assert_semantic_rows(&lix, &file_id, &after).await;
    lix.close().await.expect("workspace should close");
}

async fn install_plugin<StorageImpl>(
    lix: &lix::Lix<StorageImpl>,
    archive: &[u8],
) -> Result<(), lix::LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    write_file(
        lix,
        &format!("/.lix/plugins/{PLUGIN_KEY}.lixplugin"),
        archive,
    )
    .await
}

async fn write_file<StorageImpl>(
    lix: &lix::Lix<StorageImpl>,
    path: &str,
    data: &[u8],
) -> Result<(), lix::LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        &[
            Value::Text(path.to_owned()),
            Value::Blob(data.to_vec().into()),
        ],
    )
    .await?;
    Ok(())
}

async fn read_file<StorageImpl>(lix: &lix::Lix<StorageImpl>, path: &str) -> Option<Vec<u8>>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_owned())],
        )
        .await
        .expect("file read should succeed");
    result.rows().first().map(|row| {
        row.get::<Vec<u8>>("content")
            .expect("data should be a blob")
    })
}

async fn file_id_at_path<StorageImpl>(lix: &lix::Lix<StorageImpl>, path: &str) -> String
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_owned())],
        )
        .await
        .expect("file ID query should succeed");
    assert_eq!(result.len(), 1, "expected one file at {path}");
    result.rows()[0]
        .get::<String>("id")
        .expect("file id should be text")
}

async fn active_branch_head<StorageImpl>(lix: &lix::Lix<StorageImpl>) -> String
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("active branch head should load")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("active branch head should be text")
}

async fn assert_history_file<StorageImpl>(
    lix: &lix::Lix<StorageImpl>,
    as_of_commit_id: &str,
    file_id: &str,
    expected: &[u8],
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT content FROM lix_as_of('lix_file', $1) \
             WHERE id = $2",
            &[
                Value::Text(as_of_commit_id.to_owned()),
                Value::Text(file_id.to_owned()),
            ],
        )
        .await
        .expect("file history should render");
    let rendered = result
        .rows()
        .iter()
        .filter_map(|row| row.get::<Vec<u8>>("content").ok())
        .collect::<Vec<_>>();
    assert!(
        rendered.iter().any(|bytes| bytes == expected),
        "history must reconstruct the exact bytes; rendered versions: {}",
        rendered.len(),
    );
}

async fn git_text_rows<StorageImpl>(lix: &lix::Lix<StorageImpl>, file_id: &str) -> Vec<GitTextLine>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT id, order_key, content, content_base64, line_ending \
             FROM text_line WHERE lixcol_file_id = $1",
            &[Value::Text(file_id.to_owned())],
        )
        .await
        .expect("Git text rows should query");
    let mut rows = result
        .rows()
        .iter()
        .map(|row| {
            let id = row.get::<String>("id").expect("line id should be text");
            GitTextLine {
                id,
                order_key: row
                    .get::<String>("order_key")
                    .expect("line order key should be text"),
                content: optional_text(row.get::<Value>("content").unwrap()),
                content_base64: optional_text(row.get::<Value>("content_base64").unwrap()),
                line_ending: row.get::<String>("line_ending").unwrap(),
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| (&left.order_key, &left.id).cmp(&(&right.order_key, &right.id)));
    rows
}

fn render_rows(rows: &[GitTextLine]) -> Vec<u8> {
    rows.iter()
        .flat_map(|row| {
            let mut bytes = match (&row.content, &row.content_base64) {
                (Some(content), None) => content.as_bytes().to_vec(),
                (None, Some(encoded)) => URL_SAFE_NO_PAD.decode(encoded).unwrap(),
                _ => panic!("exactly one content representation"),
            };
            bytes.extend_from_slice(row.line_ending.as_bytes());
            bytes
        })
        .collect()
}

async fn assert_plugin_owned<StorageImpl>(
    lix: &lix::Lix<StorageImpl>,
    file_id: &str,
    expected: bool,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let owners = lix
        .execute(
            "SELECT key FROM lix_key_value \
             WHERE lixcol_file_id = $1 AND key = 'lix_plugin_owner_v2'",
            &[Value::Text(file_id.to_owned())],
        )
        .await
        .expect("plugin owner query should succeed");
    assert_eq!(owners.len() == 1, expected);
}

async fn assert_semantic_rows<StorageImpl>(
    lix: &lix::Lix<StorageImpl>,
    file_id: &str,
    expected: &[u8],
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    assert_eq!(render_rows(&git_text_rows(lix, file_id).await), expected);
}

async fn assert_raw_blob_materialization<StorageImpl>(lix: &lix::Lix<StorageImpl>, file_id: &str)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    assert!(
        git_text_rows(lix, file_id).await.is_empty(),
        "a raw binary successor must retire all Git-text semantic rows"
    );
}

fn build_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_TEXT_plugin_text"));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built Git text plugin wasm at {}: {error}",
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
        writer
            .start_file(path, options)
            .expect("archive entry should start");
        writer.write_all(bytes).expect("archive entry should write");
    }
    writer.finish().expect("archive should finish").into_inner()
}

#[tokio::test]
async fn sql_line_edits_survive_file_edits_reopen_and_history() {
    let storage = lix::Memory::new();
    let lix = open_lix().with_storage(storage.clone()).await.unwrap();
    install_plugin(&lix, &build_plugin_archive()).await.unwrap();
    let path = "/sql.txt";
    write_file(&lix, path, b"first\nlast\n").await.unwrap();
    let file_id = file_id_at_path(&lix, path).await;
    let rows = git_text_rows(&lix, &file_id).await;
    lix.execute(
        "UPDATE text_line SET content = 'edited' WHERE id = $1 AND lixcol_file_id = $2",
        &[
            Value::Text(rows[0].id.clone()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(read_file(&lix, path).await.unwrap(), b"edited\nlast\n");
    lix.execute(
        "INSERT INTO text_line (content, order_key, lixcol_file_id) VALUES ('middle', '60', $1)",
        &[Value::Text(file_id.clone())],
    )
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        b"edited\nmiddle\nlast\n"
    );
    // Reordering only changes the order key. Delete and failed updates must
    // keep accepted bytes and identity state coherent for the next operation.
    lix.execute(
        "UPDATE text_line SET order_key = '20' WHERE content = 'middle' AND lixcol_file_id = $1",
        &[Value::Text(file_id.clone())],
    )
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, path).await.unwrap(),
        b"middle\nedited\nlast\n"
    );
    assert!(lix.execute("UPDATE text_line SET line_ending = '' WHERE content = 'middle' AND lixcol_file_id = $1",
        &[Value::Text(file_id.clone())]).await.is_err());
    lix.execute(
        "DELETE FROM text_line WHERE content = 'middle' AND lixcol_file_id = $1",
        &[Value::Text(file_id.clone())],
    )
    .await
    .unwrap();
    let historical = active_branch_head(&lix).await;
    write_file(&lix, path, b"edited\nFINAL\n").await.unwrap();
    lix.close().await.unwrap();
    let reopened = open_lix().with_storage(storage).await.unwrap();
    assert_eq!(
        read_file(&reopened, path).await.unwrap(),
        b"edited\nFINAL\n"
    );
    assert_history_file(&reopened, &historical, &file_id, b"edited\nlast\n").await;
    reopened.execute("UPDATE text_line SET content = 'again' WHERE content = 'FINAL' AND lixcol_file_id = $1",
        &[Value::Text(file_id)]).await.unwrap();
    assert_eq!(
        read_file(&reopened, path).await.unwrap(),
        b"edited\nagain\n"
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn representation_switch_merges_keep_exactly_one_payload() {
    for (base, edited, switched, both_switch) in [
        (
            b"base\n".as_slice(),
            b"edit\n".as_slice(),
            b"\xff\n".as_slice(),
            false,
        ),
        (
            b"\xff\n".as_slice(),
            b"\xfe\n".as_slice(),
            b"text\n".as_slice(),
            false,
        ),
        (
            b"base\n".as_slice(),
            b"\xfe\n".as_slice(),
            b"\xff\n".as_slice(),
            true,
        ),
        (
            b"\xff\n".as_slice(),
            b"left\n".as_slice(),
            b"right\n".as_slice(),
            true,
        ),
    ] {
        for reverse in [false, true] {
            let lix = open_lix().await.unwrap();
            install_plugin(&lix, &build_plugin_archive()).await.unwrap();
            let path = "/encoding.txt";
            write_file(&lix, path, base).await.unwrap();
            let file_id = file_id_at_path(&lix, path).await;
            let target = lix.active_branch_id().await.unwrap();
            let source = lix
                .create_branch(CreateBranchOptions {
                    id: None,
                    name: "encoding branch".to_owned(),
                    from_commit_id: None,
                })
                .await
                .unwrap();
            write_file(&lix, path, if reverse { switched } else { edited })
                .await
                .unwrap();
            lix.switch_branch(SwitchBranchOptions {
                branch_id: source.id.clone(),
            })
            .await
            .unwrap();
            write_file(&lix, path, if reverse { edited } else { switched })
                .await
                .unwrap();
            lix.switch_branch(SwitchBranchOptions { branch_id: target })
                .await
                .unwrap();
            lix.merge_branch(MergeBranchOptions {
                source_branch_id: source.id,
            })
            .await
            .unwrap();
            let merged = read_file(&lix, path).await.unwrap();
            if both_switch {
                assert!(merged == edited || merged == switched);
            } else {
                assert_eq!(merged, switched);
            }
            assert_eq!(render_rows(&git_text_rows(&lix, &file_id).await), merged);
            write_file(&lix, path, b"later\n").await.unwrap();
            assert_eq!(read_file(&lix, path).await.unwrap(), b"later\n");
            lix.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn concurrent_gap_insertions_merge_and_remain_editable() {
    let storage = lix::Memory::new();
    let lix = open_lix().with_storage(storage.clone()).await.unwrap();
    install_plugin(&lix, &build_plugin_archive()).await.unwrap();
    let path = "/insertions.txt";
    write_file(&lix, path, b"A\nZ\n").await.unwrap();
    let file_id = file_id_at_path(&lix, path).await;
    let target = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: None,
            name: "concurrent insert".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    write_file(&lix, path, b"A\nX\nZ\n").await.unwrap();
    let x = git_text_rows(&lix, &file_id).await[1].clone();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    write_file(&lix, path, b"A\nY\nZ\n").await.unwrap();
    let y = git_text_rows(&lix, &file_id).await[1].clone();
    assert_eq!(x.order_key, y.order_key);
    lix.switch_branch(SwitchBranchOptions { branch_id: target })
        .await
        .unwrap();
    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .unwrap();
    let rows = git_text_rows(&lix, &file_id).await;
    assert_eq!(rows.len(), 4);
    assert!(rows.iter().any(|row| row.id == x.id));
    assert!(rows.iter().any(|row| row.id == y.id));
    let mut expected = if x.id < y.id {
        b"A\nX\nY\nZ\n".to_vec()
    } else {
        b"A\nY\nX\nZ\n".to_vec()
    };
    assert_eq!(render_rows(&rows), expected);
    assert_eq!(read_file(&lix, path).await.unwrap(), expected);
    expected[0] = b'a';
    write_file(&lix, path, &expected).await.unwrap();
    lix.close().await.unwrap();
    let reopened = open_lix().with_storage(storage).await.unwrap();
    assert_eq!(read_file(&reopened, path).await.unwrap(), expected);
    assert_eq!(
        render_rows(&git_text_rows(&reopened, &file_id).await),
        expected
    );
    reopened.close().await.unwrap();
}

fn optional_text(value: Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Text(text) => Some(text),
        _ => panic!("expected nullable text"),
    }
}

#[tokio::test]
async fn conflicting_line_boundary_merge_fails_without_changing_target() {
    let lix = open_lix().await.unwrap();
    install_plugin(&lix, &build_plugin_archive()).await.unwrap();
    let path = "/boundary.txt";
    write_file(&lix, path, b"a\n").await.unwrap();
    let file_id = file_id_at_path(&lix, path).await;
    let target = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: None,
            name: "append".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    write_file(&lix, path, b"a").await.unwrap();
    let target_rows = git_text_rows(&lix, &file_id).await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    write_file(&lix, path, b"a\nb\n").await.unwrap();
    lix.switch_branch(SwitchBranchOptions { branch_id: target })
        .await
        .unwrap();
    assert!(
        lix.merge_branch(MergeBranchOptions {
            source_branch_id: source.id
        })
        .await
        .is_err()
    );
    assert_eq!(read_file(&lix, path).await.unwrap(), b"a");
    assert_eq!(git_text_rows(&lix, &file_id).await, target_rows);
    write_file(&lix, path, b"next\n").await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
#[ignore = "manual end-to-end scaling probe; includes Wasm and in-memory storage"]
async fn text_sql_scaling_probe() {
    use std::time::Instant;
    println!("lines,bytes,import_ms,file_update_ms,sql_update_ms");
    for count in [1_000, 10_000, 100_000] {
        let lix = open_lix().await.unwrap();
        install_plugin(&lix, &build_plugin_archive()).await.unwrap();
        let mut bytes = (0..count)
            .map(|n| format!("line {n:08} with example content\n"))
            .collect::<String>()
            .into_bytes();
        let start = Instant::now();
        write_file(&lix, "/scale.txt", &bytes).await.unwrap();
        let import = start.elapsed().as_secs_f64() * 1000.0;
        let file_id = file_id_at_path(&lix, "/scale.txt").await;
        let rows = git_text_rows(&lix, &file_id).await;
        let id = &rows[count / 2].id;
        let mut file_samples = Vec::new();
        let mut sql_samples = Vec::new();
        for n in 0..3 {
            bytes[0] = b'A' + n;
            let start = Instant::now();
            write_file(&lix, "/scale.txt", &bytes).await.unwrap();
            file_samples.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        for n in 0..3 {
            let start = Instant::now();
            lix.execute(
                "UPDATE text_line SET content = $1 WHERE lixcol_file_id = $2 AND id = $3",
                &[
                    Value::Text(format!("SQL edit {n}")),
                    Value::Text(file_id.clone()),
                    Value::Text(id.clone()),
                ],
            )
            .await
            .unwrap();
            sql_samples.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        file_samples.sort_by(f64::total_cmp);
        sql_samples.sort_by(f64::total_cmp);
        assert_eq!(git_text_rows(&lix, &file_id).await.len(), count);
        println!(
            "{count},{},{import:.3},{:.3},{:.3}",
            bytes.len(),
            file_samples[1],
            sql_samples[1]
        );
        lix.close().await.unwrap();
    }
}

#[tokio::test]
async fn late_nul_uses_byte_fallback_and_survives_sql_and_reopen() {
    let storage = lix::Memory::new();
    let lix = open_lix().with_storage(storage.clone()).await.unwrap();
    install_plugin(&lix, &build_plugin_archive()).await.unwrap();
    let path = "/late-nul.txt";
    let mut bytes = vec![b'x'; 8_000];
    bytes.extend_from_slice(b"\0late\n");
    write_file(&lix, path, &bytes).await.unwrap();
    let file_id = file_id_at_path(&lix, path).await;
    let rows = git_text_rows(&lix, &file_id).await;
    assert_eq!(rows.len(), 1);
    assert!(rows[0].content.is_none());
    assert_eq!(render_rows(&rows), bytes);
    bytes[8_001] = b'L';
    lix.execute(
        "UPDATE text_line SET content_base64 = $1 WHERE lixcol_file_id = $2 AND id = $3",
        &[
            Value::Text(URL_SAFE_NO_PAD.encode(&bytes[..bytes.len() - 1])),
            Value::Text(file_id.clone()),
            Value::Text(rows[0].id.clone()),
        ],
    )
    .await
    .unwrap();
    lix.close().await.unwrap();
    let reopened = open_lix().with_storage(storage).await.unwrap();
    assert_eq!(read_file(&reopened, path).await.unwrap(), bytes);
    assert_eq!(
        render_rows(&git_text_rows(&reopened, &file_id).await),
        bytes
    );
    bytes[8_002] = b'A';
    write_file(&reopened, path, &bytes).await.unwrap();
    assert_eq!(read_file(&reopened, path).await.unwrap(), bytes);
    reopened.close().await.unwrap();
}
