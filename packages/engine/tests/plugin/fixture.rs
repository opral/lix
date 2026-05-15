use std::io::{Cursor, Write};

use serde_json::json;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

pub(crate) fn test_plugin_archive(plugin_key: &str) -> Vec<u8> {
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut zip = ZipWriter::new(&mut cursor);
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);

        zip.start_file("manifest.json", options)
            .expect("manifest zip entry should start");
        zip.write_all(test_manifest(plugin_key).to_string().as_bytes())
            .expect("manifest should write");

        zip.start_file("plugin.wasm", options)
            .expect("wasm zip entry should start");
        zip.write_all(b"\0asm\x01\0\0\0")
            .expect("minimal wasm header should write");

        zip.start_file("schema/test_json_entity.json", options)
            .expect("schema zip entry should start");
        zip.write_all(test_schema().to_string().as_bytes())
            .expect("schema should write");

        zip.finish().expect("zip should finish");
    }
    cursor.into_inner()
}

fn test_manifest(plugin_key: &str) -> serde_json::Value {
    json!({
        "key": plugin_key,
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": {
            "path_glob": "*.json",
            "content_type": "text"
        },
        "entry": "plugin.wasm",
        "schemas": ["schema/test_json_entity.json"]
    })
}

fn test_schema() -> serde_json::Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "x-lix-key": "test_json_entity",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "value": { "type": "string" }
        },
        "required": ["id", "value"],
        "additionalProperties": false
    })
}
