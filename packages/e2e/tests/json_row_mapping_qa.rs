use super::*;

async fn install_json(lix: &Lix<Memory>) {
    let wasm = std::fs::read(env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_plugin_json")).unwrap();
    let mut archive = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, content) in [
        (
            "manifest.json",
            include_bytes!("../../../plugins/json/manifest.json").as_slice(),
        ),
        (
            "schema/json_root.json",
            include_bytes!("../../../plugins/json/schema/json_root.json").as_slice(),
        ),
        (
            "schema/json_object_member.json",
            include_bytes!("../../../plugins/json/schema/json_object_member.json").as_slice(),
        ),
        (
            "schema/json_array_item.json",
            include_bytes!("../../../plugins/json/schema/json_array_item.json").as_slice(),
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
}

#[tokio::test]
async fn json_row_mapping_sql_defaults_decoded_keys_and_file_scoped_joins() {
    let lix = open_lix().with_storage(Memory::new()).await.unwrap();
    install_json(&lix).await;
    let source = br#"{"settings":{"a/b~":true},"":null}"#;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/first.json',$1),('/second.json',$1),('/array.json',$2)",
        &[Value::Blob(source.to_vec().into()), Value::Blob(b"[]".to_vec().into())],
    ).await.unwrap();
    let files = lix.execute("SELECT id, path FROM lix_file WHERE path IN ('/first.json','/second.json','/array.json') ORDER BY path", &[]).await.unwrap();
    let array_id = files.rows()[0].get::<String>("id").unwrap();
    let first_id = files.rows()[1].get::<String>("id").unwrap();
    let joined = lix.execute(
        "SELECT child.key, child.scalar_json FROM json_object_member AS parent JOIN json_object_member AS child ON child.parent_id = parent.container_id AND child.lixcol_file_id = parent.lixcol_file_id WHERE parent.lixcol_file_id=$1 AND parent.parent_id='root' AND parent.key='settings'",
        &[Value::Text(first_id.clone())],
    ).await.unwrap();
    assert_eq!(joined.len(), 1);
    assert_eq!(joined.rows()[0].get::<String>("key").unwrap(), "a/b~");
    assert_eq!(
        joined.rows()[0]
            .get::<serde_json::Value>("scalar_json")
            .unwrap(),
        serde_json::json!(true)
    );
    lix.execute(
        "UPDATE json_object_member SET kind='string', scalar_json='\"renamed\"'::jsonb WHERE lixcol_file_id=$1 AND parent_id='root' AND key=''",
        &[Value::Text(first_id.clone())],
    ).await.unwrap();
    // Default parent points at root. Default order is a deterministic tie, not append.
    lix.execute(
        "INSERT INTO json_object_member(key,kind,scalar_json,lixcol_file_id) VALUES('z','null','null'::jsonb,$1),('a','number','42'::jsonb,$1)",
        &[Value::Text(first_id)],
    ).await.unwrap();
    lix.execute(
        "INSERT INTO json_array_item(kind,scalar_json,lixcol_file_id) VALUES('string','\"item\"'::jsonb,$1)",
        &[Value::Text(array_id)],
    ).await.unwrap();
    let content = lix.execute("SELECT path,content FROM lix_file WHERE path IN ('/first.json','/second.json','/array.json') ORDER BY path", &[]).await.unwrap();
    assert_eq!(
        content.rows()[0].get::<Vec<u8>>("content").unwrap(),
        br#"["item"]"#
    );
    assert_eq!(
        content.rows()[1].get::<Vec<u8>>("content").unwrap(),
        br#"{"settings":{"a/b~":true},"a":42,"z":null,"":"renamed"}"#
    );
    assert_eq!(content.rows()[2].get::<Vec<u8>>("content").unwrap(), source);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn json_row_mapping_sql_creates_and_renames_nested_container_without_rekeying_children() {
    let storage = Memory::new();
    let lix = open_lix().with_storage(storage.clone()).await.unwrap();
    install_json(&lix).await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/nested.json',$1)",
        &[Value::Blob(b"{}".to_vec().into())],
    )
    .await
    .unwrap();
    let file_id = lix
        .execute("SELECT id FROM lix_file WHERE path='/nested.json'", &[])
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();
    // A caller-chosen ID permits ordinary SQL creation; no plugin-specific hash is needed.
    lix.execute("INSERT INTO json_object_member(key,kind,container_id,lixcol_file_id) VALUES('settings','object','settings-container',$1)", &[Value::Text(file_id.clone())]).await.unwrap();
    lix.execute("INSERT INTO json_object_member(parent_id,key,kind,scalar_json,lixcol_file_id) VALUES('settings-container','enabled','boolean','true'::jsonb,$1)", &[Value::Text(file_id.clone())]).await.unwrap();
    // Commit each valid tree separately: execute_batch cannot delete and recreate
    // a descendant primary key in the same batch.
    for statement in [
        ExecuteBatchStatement { label: None, sql: "DELETE FROM json_object_member WHERE parent_id='settings-container' AND key='enabled' AND occurrence=0 AND lixcol_file_id=$1".into(), params: vec![Value::Text(file_id.clone())] },
        ExecuteBatchStatement { label: None, sql: "DELETE FROM json_object_member WHERE parent_id='root' AND key='settings' AND occurrence=0 AND lixcol_file_id=$1".into(), params: vec![Value::Text(file_id.clone())] },
        ExecuteBatchStatement { label: None, sql: "INSERT INTO json_object_member(key,kind,container_id,lixcol_file_id) VALUES('preferences','object','settings-container',$1)".into(), params: vec![Value::Text(file_id.clone())] },
        ExecuteBatchStatement { label: None, sql: "INSERT INTO json_object_member(parent_id,key,occurrence,order_key,kind,scalar_json,lixcol_file_id) VALUES('settings-container','enabled',0,'80','boolean','true'::jsonb,$1)".into(), params: vec![Value::Text(file_id.clone())] },
    ] {
        lix.execute(&statement.sql, &statement.params).await.unwrap();
    }
    let content = lix
        .execute(
            "SELECT content FROM lix_file WHERE id=$1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(
        content.rows()[0].get::<Vec<u8>>("content").unwrap(),
        br#"{"preferences":{"enabled":true}}"#
    );
    lix.execute(
        "UPDATE lix_file SET content=$1 WHERE id=$2",
        &[
            Value::Blob(br#"{"preferences":{"enabled":false,"new":null}}"#.to_vec().into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .unwrap();
    let children = lix.execute("SELECT key FROM json_object_member WHERE parent_id='settings-container' AND lixcol_file_id=$1 ORDER BY key", &[Value::Text(file_id.clone())]).await.unwrap();
    assert_eq!(children.len(), 2);
    assert_eq!(children.rows()[0].get::<String>("key").unwrap(), "enabled");
    assert_eq!(children.rows()[1].get::<String>("key").unwrap(), "new");
    lix.close().await.unwrap();
    let reopened = open_lix().with_storage(storage).await.unwrap();
    // The first mutation after reopening must reconstruct custom identities from durable rows.
    let changed = reopened.execute("UPDATE json_object_member SET scalar_json='true'::jsonb WHERE parent_id='settings-container' AND key='enabled' AND lixcol_file_id=$1", &[Value::Text(file_id.clone())]).await.unwrap();
    assert_eq!(changed.rows_affected(), 1);
    let content = reopened
        .execute(
            "SELECT content FROM lix_file WHERE id=$1",
            &[Value::Text(file_id)],
        )
        .await
        .unwrap();
    assert_eq!(
        content.rows()[0].get::<Vec<u8>>("content").unwrap(),
        br#"{"preferences":{"enabled":true,"new":null}}"#
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn json_row_mapping_sql_duplicate_decoded_keys_roundtrip_and_edit_one_occurrence() {
    let storage = Memory::new();
    let lix = open_lix().with_storage(storage.clone()).await.unwrap();
    install_json(&lix).await;
    let source = br#"{ "same": 1, "\u0073ame": 2, "same": 3 }"#;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/duplicates.json',$1)",
        &[Value::Blob(source.to_vec().into())],
    )
    .await
    .unwrap();
    let before = lix
        .execute(
            "SELECT id,content FROM lix_file WHERE path='/duplicates.json'",
            &[],
        )
        .await
        .unwrap();
    let file_id = before.rows()[0].get::<String>("id").unwrap();
    assert_eq!(before.rows()[0].get::<Vec<u8>>("content").unwrap(), source);
    let rows = lix.execute("SELECT occurrence,scalar_json FROM json_object_member WHERE parent_id='root' AND key='same' AND lixcol_file_id=$1 ORDER BY occurrence", &[Value::Text(file_id.clone())]).await.unwrap();
    assert_eq!(rows.len(), 3);
    for (index, row) in rows.rows().iter().enumerate() {
        assert_eq!(row.get::<i64>("occurrence").unwrap(), index as i64);
        assert_eq!(
            row.get::<serde_json::Value>("scalar_json").unwrap(),
            serde_json::json!(index + 1)
        );
    }
    lix.execute("UPDATE json_object_member SET scalar_json='42'::jsonb WHERE parent_id='root' AND key='same' AND occurrence=1 AND lixcol_file_id=$1", &[Value::Text(file_id.clone())]).await.unwrap();
    let after = lix
        .execute(
            "SELECT content FROM lix_file WHERE id=$1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(
        after.rows()[0].get::<Vec<u8>>("content").unwrap(),
        br#"{ "same": 1, "\u0073ame": 42, "same": 3 }"#
    );
    lix.execute("DELETE FROM json_object_member WHERE parent_id='root' AND key='same' AND occurrence=1 AND lixcol_file_id=$1", &[Value::Text(file_id.clone())]).await.unwrap();
    lix.close().await.unwrap();
    let reopened = open_lix().with_storage(storage).await.unwrap();
    // The remaining occurrence 2 must not be renumbered from reparsing the two-member file.
    let changed = reopened.execute("UPDATE json_object_member SET scalar_json='99'::jsonb WHERE parent_id='root' AND key='same' AND occurrence=2 AND lixcol_file_id=$1", &[Value::Text(file_id.clone())]).await.unwrap();
    assert_eq!(changed.rows_affected(), 1);
    let content = reopened
        .execute(
            "SELECT content FROM lix_file WHERE id=$1",
            &[Value::Text(file_id)],
        )
        .await
        .unwrap();
    assert_eq!(
        content.rows()[0].get::<Vec<u8>>("content").unwrap(),
        br#"{ "same": 1, "same": 99 }"#
    );
    reopened.close().await.unwrap();
}
