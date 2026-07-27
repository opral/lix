use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use lix_sdk::{OpenLixOptions, Value, open_lix};
use std::io::{Cursor, Write};
use std::path::Path;

const PLUGIN_KEY: &str = "plugin_git_text_v2";
const GIT_TEXT_SCAN_BYTES: u64 = 8_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct GitTextLine {
    id: String,
    order_key: String,
    content_base64: String,
}

#[tokio::test]
async fn git_text_plugin_persists_lossless_line_rows_and_leaves_binary_raw() {
    let storage = lix_sdk::Memory::new();
    let lix = open_lix(OpenLixOptions::new(storage.clone()))
        .await
        .expect("workspace should open");
    install_plugin(&lix, &build_plugin_archive())
        .await
        .expect("Git text plugin should install");

    // Git considers this text despite invalid UTF-8 because its first 8 KiB
    // contain no NUL. The row payload is base64url so it remains byte-exact.
    let git_text = [0xff, b'\n', 0xfe, b'\n'].to_vec();
    let text_path = "/git-text.invalid-utf8";
    lix.reset_plugin_v2_transition_counters();
    write_file(&lix, text_path, &git_text)
        .await
        .expect("NUL-free Git text should write");
    let counters = lix.plugin_v2_transition_counters();
    assert_eq!(counters.source_bytes_read, git_text.len() as u64);
    assert_eq!(counters.durable_semantic_changes, 2);
    assert_eq!(
        counters.host_content_classification_bytes,
        git_text.len() as u64,
        "small payloads need only Git's bounded NUL scan"
    );

    let text_file_id = file_id_at_path(&lix, text_path).await;
    assert_plugin_owned(&lix, &text_file_id, true).await;
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
    assert_eq!(read_file(&lix, binary_path).await, Some(binary.clone()));

    // Empty files are Git text too; their semantic representation is an
    // intentionally empty line table rather than a synthetic blank line.
    let empty_path = "/empty.txt";
    write_file(&lix, empty_path, b"")
        .await
        .expect("empty Git text should write");
    let empty_file_id = file_id_at_path(&lix, empty_path).await;
    assert_plugin_owned(&lix, &empty_file_id, true).await;
    assert!(git_text_rows(&lix, &empty_file_id).await.is_empty());
    assert_eq!(read_file(&lix, empty_path).await, Some(Vec::new()));

    lix.close().await.expect("workspace should close");
    let reopened = open_lix(OpenLixOptions::new(storage))
        .await
        .expect("workspace should reopen");
    assert_eq!(read_file(&reopened, text_path).await, Some(git_text));
    assert_eq!(read_file(&reopened, binary_path).await, Some(binary));
    assert_eq!(read_file(&reopened, empty_path).await, Some(Vec::new()));
    assert_eq!(
        render_rows(&git_text_rows(&reopened, &text_file_id).await),
        [0xff, b'\n', 0xfe, b'\n']
    );
    // This write forces a cold actor to rebuild its document from durable
    // rows after reopen, then proves the resulting semantic update is exact.
    let reopened_successor = [0xff, b'\n', b'X', b'\n'].to_vec();
    write_file(&reopened, text_path, &reopened_successor)
        .await
        .expect("cold row reconstruction should accept a later line edit");
    let reopened_rows = git_text_rows(&reopened, &text_file_id).await;
    assert_eq!(reopened_rows[0].id, text_rows[0].id);
    assert_eq!(reopened_rows[1].id, text_rows[1].id);
    assert_eq!(render_rows(&reopened_rows), reopened_successor);
    reopened
        .close()
        .await
        .expect("reopened workspace should close");
}

#[tokio::test]
async fn git_text_plugin_reads_only_a_large_after_range_and_updates_one_line_row() {
    const MIB: usize = 1024 * 1024;

    let lix = open_lix(OpenLixOptions::new(lix_sdk::Memory::new()))
        .await
        .expect("workspace should open");
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

    lix.reset_plugin_v2_transition_counters();
    write_file(&lix, path, &after)
        .await
        .expect("large localized line edit should write");
    let counters = lix.plugin_v2_transition_counters();
    assert_eq!(
        counters.source_bytes_read, replacement_len as u64,
        "the plugin must read only the `AfterRange` insert, never the full 4 MiB successor"
    );
    assert!(
        counters.source_bytes_read * 100 < before.len() as u64 * 30,
        "the warm source read should stay below 30% of the full document"
    );
    assert_eq!(counters.durable_semantic_changes, 1);
    assert_eq!(counters.full_document_reparses, 0);
    assert_eq!(
        counters.host_content_classification_bytes, GIT_TEXT_SCAN_BYTES,
        "Git text selection must remain bounded even for a multi-megabyte file"
    );

    let after_rows = git_text_rows(&lix, &file_id).await;
    assert_eq!(after_rows.len(), 2);
    assert_eq!(after_rows[0].id, before_rows[0].id);
    assert_eq!(after_rows[1], before_rows[1]);
    assert_eq!(render_rows(&after_rows), after);
    assert_eq!(read_file(&lix, path).await, Some(after));
    lix.close().await.expect("workspace should close");
}

async fn install_plugin<StorageImpl>(
    lix: &lix_sdk::Lix<StorageImpl>,
    archive: &[u8],
) -> Result<(), lix_sdk::LixError>
where
    StorageImpl: lix_sdk::Storage + Clone + Send + Sync + 'static,
{
    write_file(
        lix,
        &format!("/.lix/plugins/{PLUGIN_KEY}.lixplugin"),
        archive,
    )
    .await
}

async fn write_file<StorageImpl>(
    lix: &lix_sdk::Lix<StorageImpl>,
    path: &str,
    data: &[u8],
) -> Result<(), lix_sdk::LixError>
where
    StorageImpl: lix_sdk::Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET data = excluded.data",
        &[
            Value::Text(path.to_owned()),
            Value::Blob(data.to_vec().into()),
        ],
    )
    .await?;
    Ok(())
}

async fn read_file<StorageImpl>(lix: &lix_sdk::Lix<StorageImpl>, path: &str) -> Option<Vec<u8>>
where
    StorageImpl: lix_sdk::Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT data FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_owned())],
        )
        .await
        .expect("file read should succeed");
    result
        .rows()
        .first()
        .map(|row| row.get::<Vec<u8>>("data").expect("data should be a blob"))
}

async fn file_id_at_path<StorageImpl>(lix: &lix_sdk::Lix<StorageImpl>, path: &str) -> String
where
    StorageImpl: lix_sdk::Storage + Clone + Send + Sync + 'static,
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

async fn git_text_rows<StorageImpl>(
    lix: &lix_sdk::Lix<StorageImpl>,
    file_id: &str,
) -> Vec<GitTextLine>
where
    StorageImpl: lix_sdk::Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT lixcol_entity_pk, id, order_key, content_base64 \
             FROM git_text_line_v2 WHERE lixcol_file_id = $1",
            &[Value::Text(file_id.to_owned())],
        )
        .await
        .expect("Git text rows should query");
    let mut rows = result
        .rows()
        .iter()
        .map(|row| {
            let id = row.get::<String>("id").expect("line id should be text");
            assert_eq!(
                row.get::<serde_json::Value>("lixcol_entity_pk")
                    .expect("line primary key should be JSON"),
                serde_json::json!([id.clone()]),
                "line snapshot identity must equal its durable primary key"
            );
            GitTextLine {
                id,
                order_key: row
                    .get::<String>("order_key")
                    .expect("line order key should be text"),
                content_base64: row
                    .get::<String>("content_base64")
                    .expect("line content should be text"),
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.order_key.cmp(&right.order_key));
    rows
}

fn render_rows(rows: &[GitTextLine]) -> Vec<u8> {
    rows.iter()
        .flat_map(|row| {
            URL_SAFE_NO_PAD
                .decode(&row.content_base64)
                .expect("line content must be base64url")
        })
        .collect()
}

async fn assert_plugin_owned<StorageImpl>(
    lix: &lix_sdk::Lix<StorageImpl>,
    file_id: &str,
    expected: bool,
) where
    StorageImpl: lix_sdk::Storage + Clone + Send + Sync + 'static,
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

fn build_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!(
        "CARGO_CDYLIB_FILE_PLUGIN_GIT_TEXT_V2_plugin_git_text_v2"
    ));
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
            include_str!("../../../plugins/text-v2/manifest.json").as_bytes(),
        ),
        (
            "schema/git_text_line_v2.json",
            include_str!("../../../plugins/text-v2/schema/git_text_line_v2.json").as_bytes(),
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
