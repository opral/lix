//! Detached qualification only. Never points at a serving physical repository.
use lix_storage_slatedb::SlateDB;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let directory = std::path::PathBuf::from(
        arguments
            .next()
            .ok_or("expected copied physical directory")?,
    );
    let report_path = arguments.next().ok_or("expected output report path")?;
    if arguments.next().as_deref() != Some("--isolated-copy") || arguments.next().is_some() {
        return Err("explicit --isolated-copy required; serving storage is forbidden".into());
    }
    if !directory.is_dir() {
        return Err("source copy must already exist".into());
    }
    let source_objects = objects(&directory)?;
    let root = directory.parent().ok_or("directory needs parent")?;
    let prefix = directory
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("directory name must be UTF-8")?;
    let physical_objects =
        std::sync::Arc::new(object_store::local::LocalFileSystem::new_with_prefix(root)?);
    let storage =
        SlateDB::open_object_store_with_options(prefix, physical_objects, Default::default())?;
    let result = lix::migration::migrate_repository(storage.clone()).await;
    drop(storage);
    let report = result?;
    let mut output = serde_json::to_value(&report)?;
    output["sourceObjects"] = serde_json::to_value(source_objects)?;
    output["verifiedDestinationObjects"] = serde_json::to_value(objects(&directory)?)?;
    std::fs::write(report_path, serde_json::to_vec_pretty(&output)?)?;
    if !report.semantic_preservation_verified {
        return Err("migration output is not preservation-qualified".into());
    }
    println!(
        "preservation verified; hosted identity and authority promotion remain explicit operator steps"
    );
    Ok(())
}

fn objects(root: &std::path::Path) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    use std::io::Read;
    let mut pending = vec![root.to_owned()];
    let mut paths = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                paths.push(entry.path());
            } else {
                return Err("nonregular physical object in isolated source".into());
            }
        }
    }
    paths.sort();
    let mut records = Vec::new();
    for path in paths {
        let key = path
            .strip_prefix(root)?
            .to_str()
            .ok_or("object path is not UTF-8")?;
        let mut file = std::fs::File::open(&path)?;
        let mut digest = blake3::Hasher::new();
        let mut bytes = 0u64;
        let mut buffer = [0u8; 65536];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            digest.update(&buffer[..count]);
            bytes += count as u64;
        }
        records.push(serde_json::json!({"key":key,"bytes":bytes,"blake3":digest.finalize().to_hex().to_string()}));
    }
    Ok(records)
}
