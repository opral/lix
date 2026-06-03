#[allow(dead_code)]
mod common;

use lix_sdk::FsWriteOptions;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
struct Fixture {
    label: String,
    path: String,
    bytes: Vec<u8>,
}

#[tokio::test]
async fn roundtrips_create_fixtures_byte_for_byte() {
    let lix = common::open_lix_with_sem_plugin().await;

    for fixture in load_fixtures("roundtrip") {
        lix.write_file(
            &fixture.path,
            fixture.bytes.clone(),
            FsWriteOptions::default(),
        )
        .await
        .unwrap_or_else(|error| panic!("write failed for {}: {error:?}", fixture.label));

        let rendered = lix
            .read_file(&fixture.path)
            .await
            .unwrap_or_else(|error| panic!("read failed for {}: {error:?}", fixture.label))
            .unwrap_or_else(|| panic!("{} was not found after write", fixture.label));

        assert_eq!(
            rendered, fixture.bytes,
            "roundtrip mismatch for {}",
            fixture.label
        );
    }

    lix.close().await.unwrap();
}

#[tokio::test]
async fn roundtrips_update_fixtures_byte_for_byte() {
    let lix = common::open_lix_with_sem_plugin().await;

    for pair in load_update_fixture_pairs() {
        lix.write_file(
            &pair.before.path,
            pair.before.bytes,
            FsWriteOptions::default(),
        )
        .await
        .unwrap_or_else(|error| panic!("initial write failed for {}: {error:?}", pair.label));
        lix.write_file(
            &pair.after.path,
            pair.after.bytes.clone(),
            FsWriteOptions::default(),
        )
        .await
        .unwrap_or_else(|error| panic!("update write failed for {}: {error:?}", pair.label));

        let rendered = lix
            .read_file(&pair.after.path)
            .await
            .unwrap_or_else(|error| panic!("read failed for {}: {error:?}", pair.label))
            .unwrap_or_else(|| panic!("{} was not found after update", pair.label));

        assert_eq!(
            rendered, pair.after.bytes,
            "updated roundtrip mismatch for {}",
            pair.label
        );
    }

    lix.close().await.unwrap();
}

#[derive(Debug)]
struct UpdateFixturePair {
    label: String,
    before: Fixture,
    after: Fixture,
}

fn load_fixtures(group: &str) -> Vec<Fixture> {
    let root = fixtures_root().join(group);
    let mut files = Vec::new();
    collect_fixture_files(&root, &mut files);
    files
        .into_iter()
        .map(|path| fixture_from_path(&root, path))
        .collect()
}

fn load_update_fixture_pairs() -> Vec<UpdateFixturePair> {
    let root = fixtures_root().join("updates");
    let mut files = Vec::new();
    collect_fixture_files(&root, &mut files);

    let mut pairs = Vec::new();
    for before_path in files.iter().filter(|path| {
        path.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.contains(".before."))
    }) {
        let before_name = before_path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("fixture filename should be UTF-8");
        let after_name = before_name.replace(".before.", ".after.");
        let after_path = before_path.with_file_name(after_name);
        assert!(
            after_path.exists(),
            "missing after fixture for {}",
            before_path.display()
        );

        let before = fixture_from_path(&root, before_path.clone());
        let mut after = fixture_from_path(&root, after_path);
        after.path = before.path.clone();
        pairs.push(UpdateFixturePair {
            label: before.label.clone(),
            before,
            after,
        });
    }

    pairs.sort_by(|left, right| left.label.cmp(&right.label));
    pairs
}

fn collect_fixture_files(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("failed to read fixture dir {}: {error}", dir.display()))
    {
        let entry = entry.unwrap_or_else(|error| panic!("failed to read fixture entry: {error}"));
        let path = entry.path();
        if path.is_dir() {
            collect_fixture_files(&path, files);
        } else {
            files.push(path);
        }
    }
    files.sort();
}

fn fixture_from_path(root: &Path, path: PathBuf) -> Fixture {
    let relative = path
        .strip_prefix(root)
        .expect("fixture should be inside root")
        .to_string_lossy()
        .replace(".before.", ".")
        .replace(".after.", ".");
    Fixture {
        label: relative.clone(),
        path: format!("/fixtures/{relative}"),
        bytes: std::fs::read(&path)
            .unwrap_or_else(|error| panic!("failed to read fixture {}: {error}", path.display())),
    }
}

fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}
