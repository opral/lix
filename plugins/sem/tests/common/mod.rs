use lix_sdk::{OpenLixOptions, Value, open_lix};
use std::io::{Cursor, Write};
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;

pub const ORIGINAL_RUST_SOURCE: &[u8] = b"use std::fmt;\n\nfn hello(name: &str) -> String {\n    format!(\"Hello, {}!\", name)\n}\n\nstruct Greeter;\n";
pub const UPDATED_RUST_SOURCE: &[u8] = b"use std::fmt;\n\nfn hello(name: &str) -> String {\n    format!(\"Hi, {}!\", name)\n}\n\nstruct Greeter;\n";

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct FileChange {
    pub schema_key: String,
    pub snapshot_content: Option<serde_json::Value>,
}

pub async fn open_lix_with_sem_plugin() -> lix_sdk::Lix {
    let archive = sem_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();

    lix.install_plugin_archive(archive).await.unwrap();
    let plugins = lix.list_installed_plugins().await.unwrap();
    assert_eq!(plugins.len(), 1);
    assert_eq!(plugins[0].key, "plugin_sem");
    assert_eq!(plugins[0].schema_keys, vec!["sem_entity".to_string()]);
    lix
}

fn sem_plugin_archive() -> &'static [u8] {
    static ARCHIVE: OnceLock<Vec<u8>> = OnceLock::new();
    ARCHIVE.get_or_init(build_sem_plugin_archive).as_slice()
}

pub async fn file_id_for_path(lix: &lix_sdk::Lix, path: &str) -> String {
    let rows = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    rows.rows()[0].get::<String>("id").unwrap()
}

#[allow(dead_code)]
pub async fn file_changes(lix: &lix_sdk::Lix, file_id: &str) -> Vec<FileChange> {
    let changes = lix
        .execute(
            "SELECT schema_key, snapshot_content \
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
                snapshot_content,
            }
        })
        .collect()
}

fn build_sem_plugin_archive() -> Vec<u8> {
    let wasm_path = plugin_wasm_path();
    let wasm = std::fs::read(&wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built sem plugin wasm at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../manifest.json").as_bytes(),
        ),
        (
            "schema/sem_entity.json",
            include_str!("../../schema/sem_entity.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn plugin_wasm_path() -> std::path::PathBuf {
    if let Ok(path) = std::env::var("CARGO_CDYLIB_FILE_PLUGIN_SEM_WASM_plugin_sem") {
        return path.into();
    }

    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("plugins/sem should live two levels below the workspace root");
    let status = Command::new("cargo")
        .args(["build", "-p", "plugin_sem", "--target", "wasm32-wasip2"])
        .current_dir(workspace_root)
        .status()
        .expect("failed to spawn cargo build for plugin_sem wasm artifact");
    assert!(
        status.success(),
        "cargo build -p plugin_sem --target wasm32-wasip2 failed"
    );
    workspace_root
        .join("target")
        .join("wasm32-wasip2")
        .join("debug")
        .join("plugin_sem.wasm")
}
