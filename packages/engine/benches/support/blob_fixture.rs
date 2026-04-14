use super::trace::{file_backed_backend, SqlTraceCollector};
use lix_engine::wasm::NoopWasmRuntime;
use lix_engine::{BootKeyValue, Lix, LixConfig};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

pub fn temp_db(filename: &str) -> (TempDir, PathBuf) {
    let tempdir = TempDir::new().expect("tempdir should be created");
    let db_path = tempdir.path().join(filename);
    (tempdir, db_path)
}

pub fn boot_new_file_backed_lix(
    runtime: &Runtime,
    db_path: &Path,
    trace_collector: Option<Arc<SqlTraceCollector>>,
    deterministic: bool,
) -> Arc<Lix> {
    let backend =
        file_backed_backend(db_path, trace_collector).expect("file-backed sqlite backend");
    let mut config = LixConfig::new(backend, Arc::new(NoopWasmRuntime));
    if deterministic {
        config.key_values.push(BootKeyValue {
            key: "lix_deterministic_mode".to_string(),
            value: json!({ "enabled": true }),
            lixcol_global: Some(true),
            lixcol_untracked: None,
        });
    }

    let lix = Arc::new(Lix::boot(config));
    runtime
        .block_on(lix.initialize())
        .expect("lix initialization should succeed");
    lix
}

#[allow(dead_code)]
pub fn open_existing_file_backed_lix(
    runtime: &Runtime,
    db_path: &Path,
    trace_collector: Option<Arc<SqlTraceCollector>>,
) -> Arc<Lix> {
    let backend =
        file_backed_backend(db_path, trace_collector).expect("file-backed sqlite backend");
    let lix = Arc::new(Lix::boot(LixConfig::new(
        backend,
        Arc::new(NoopWasmRuntime),
    )));
    runtime
        .block_on(lix.open_existing())
        .expect("existing template db should open");
    lix
}

pub fn repeated_payload(prefix: &str, byte_len: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(byte_len);
    while payload.len() < byte_len {
        payload.extend_from_slice(prefix.as_bytes());
    }
    payload.truncate(byte_len);
    payload
}
