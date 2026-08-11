use std::fs;
use std::io::{Cursor, Write};
use std::path::Path;

use lix::{Value, open_lix};

#[tokio::test]
async fn row_only_merger_composes_conversation_text_without_a_file() {
    let lix = open_lix().await.expect("workspace should open");
    let archive = build_plugin_archive();
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            Value::Text("/.lix/plugins/plugin_conversation.lixplugin".to_owned()),
            Value::Blob(archive.into()),
        ],
    )
    .await
    .expect("row-only plugin should install");
    lix.execute(
        "INSERT INTO conversation (id, title, body) VALUES ($1, $2, $3)",
        &[
            Value::Text("0198b7a1-0000-7000-8000-000000000001".to_owned()),
            Value::Text("Planning".to_owned()),
            Value::Text("Alice said hello.\n\nBob said goodbye.".to_owned()),
        ],
    )
    .await
    .expect("conversation row should insert");

    let peer = lix.open_another_session().await.expect("peer should open");
    let mut a = lix.begin_transaction().await.expect("transaction A");
    let mut b = peer.begin_transaction().await.expect("transaction B");
    a.execute(
        "UPDATE conversation SET body = $1 WHERE id = $2",
        &[
            Value::Text("Alice said HELLO.\n\nBob said goodbye.".to_owned()),
            Value::Text("0198b7a1-0000-7000-8000-000000000001".to_owned()),
        ],
    )
    .await
    .expect("A edit should stage");
    b.execute(
        "UPDATE conversation SET body = $1 WHERE id = $2",
        &[
            Value::Text("Alice said hello.\n\nBob said GOODBYE.".to_owned()),
            Value::Text("0198b7a1-0000-7000-8000-000000000001".to_owned()),
        ],
    )
    .await
    .expect("B edit should stage");
    a.commit().await.expect("A should commit");
    b.commit()
        .await
        .expect("B should invoke the row-only merger");

    let result = lix
        .execute(
            "SELECT body FROM conversation WHERE id = $1",
            &[Value::Text(
                "0198b7a1-0000-7000-8000-000000000001".to_owned(),
            )],
        )
        .await
        .expect("merged conversation should read");
    assert_eq!(
        result.rows()[0].get::<String>("body").unwrap(),
        "Alice said HELLO.\n\nBob said GOODBYE."
    );

    peer.close().await.expect("peer should close");
    lix.close().await.expect("workspace should close");
}

fn build_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!(
        "CARGO_CDYLIB_FILE_PLUGIN_CONVERSATION_plugin_conversation"
    ));
    let wasm =
        fs::read(wasm_path).unwrap_or_else(|error| panic!("read {}: {error}", wasm_path.display()));
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer.start_file("manifest.json", options).unwrap();
    writer
        .write_all(include_bytes!(
            "../../../plugins/conversation/manifest.json"
        ))
        .unwrap();
    writer
        .start_file("schema/conversation.json", options)
        .unwrap();
    writer
        .write_all(include_bytes!(
            "../../../plugins/conversation/schema/conversation.json"
        ))
        .unwrap();
    writer.start_file("plugin.wasm", options).unwrap();
    writer.write_all(&wasm).unwrap();
    writer.finish().unwrap().into_inner()
}
