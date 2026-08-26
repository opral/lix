//! Golden repository-format fixture coverage for filesystem commit membership
//! across the v72 -> v74 migration.
//!
//! `fixtures/v72_filesystem_checkpoints.snapshot` is the raw
//! `Memory::export_snapshot` output generated from revision
//! `4816fdba591d7165ff1b0195e74471aa8fc73660` by a generator that creates
//! `/sales/playbook.md` and `/docs/handbook/inside.md`, takes a full
//! checkpoint, creates `/brand/logo.md`, takes a partial (file-selected)
//! checkpoint, and leaves one working edit. The generator asserts — on the
//! v72 engine — that both checkpoint trees contain their directory
//! descriptors before exporting, so any inconsistency observed after
//! migration was introduced by the migration itself.
//!
//! Regression: a repository migrated to v74 served checkpoint trees whose
//! file descriptors referenced directories missing from the same tree
//! ("filesystem descriptor references missing directory"), which fails every
//! `lix_diff` / `lix_state_at` read touching the relation at that commit.

use lix::migration::{MigrationOptions, MigrationStatus, inspect_lix, migrate_lix};
use lix::{Memory, Value, open_lix};
use std::collections::HashSet;

const V72_FILESYSTEM_SNAPSHOT: &[u8] =
    include_bytes!("fixtures/v72_filesystem_checkpoints.snapshot");

#[tokio::test]
async fn checkpointed_directories_survive_the_v74_migration() {
    let storage = Memory::from_snapshot(V72_FILESYSTEM_SNAPSHOT)
        .expect("v72 filesystem fixture should decode");
    assert_eq!(
        inspect_lix(&storage).await.expect("inspect fixture"),
        MigrationStatus::Required {
            from_version: 72,
            to_version: 75,
        }
    );

    let report = migrate_lix(storage.clone(), MigrationOptions::default())
        .await
        .expect("v72 filesystem fixture should migrate");
    assert_eq!(report.from_version, 72);
    assert_eq!(report.to_version, 75);

    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("migrated repository should open");

    let checkpoints = lix
        .execute(
            "SELECT commit_id FROM lix_checkpoint ORDER BY lixcol_created_at ASC",
            &[],
        )
        .await
        .expect("checkpoint listing");
    // Bootstrap checkpoint + full + partial.
    assert_eq!(checkpoints.rows().len(), 3, "fixture carries three checkpoints");

    for row in checkpoints.rows() {
        let commit_id = row.get::<String>("commit_id").expect("commit id");

        // Every file descriptor in the checkpoint tree must resolve its
        // ancestor directories within the same tree.
        let files = lix
            .execute(
                "SELECT id, name, directory_id FROM lix_state_at('lix_file', $1)",
                &[Value::Text(commit_id.clone())],
            )
            .await
            .unwrap_or_else(|error| {
                panic!("lix_state_at('lix_file') at {commit_id} should read: {error}")
            });
        let directories = lix
            .execute(
                "SELECT id FROM lix_state_at('lix_directory', $1)",
                &[Value::Text(commit_id.clone())],
            )
            .await
            .unwrap_or_else(|error| {
                panic!("lix_state_at('lix_directory') at {commit_id} should read: {error}")
            });
        let directory_ids: HashSet<String> = directories
            .rows()
            .iter()
            .map(|row| row.get::<String>("id").expect("directory id"))
            .collect();
        for file in files.rows() {
            let name = file.get::<String>("name").expect("file name");
            if let Ok(directory_id) = file.get::<String>("directory_id") {
                assert!(
                    directory_ids.contains(&directory_id),
                    "file '{name}' at checkpoint {commit_id} references directory \
                     '{directory_id}' that is missing from the same tree"
                );
            }
        }

        // The path-projecting read the product runs on every checkpoint open.
        lix.execute(
            "SELECT lixcol_row_pk, to_path FROM lix_diff('lix_file', lix_root_commit_id(), $1)",
            &[Value::Text(commit_id.clone())],
        )
        .await
        .unwrap_or_else(|error| {
            panic!("root diff with paths at {commit_id} should read: {error}")
        });
    }

    // Authoring after migration: a partial checkpoint on a migrated
    // repository must produce a cumulative, internally consistent tree —
    // the selected file, its swept ancestor directory, and every row the
    // previous checkpoint already contained.
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ('/notes/today.md', CAST('note' AS BYTEA))",
        &[],
    )
    .await
    .expect("create /notes/today.md on the migrated repository");
    let new_file = lix
        .execute("SELECT id FROM lix_file WHERE path = '/notes/today.md'", &[])
        .await
        .expect("read new file id");
    let new_file_id = new_file.rows()[0].get::<String>("id").expect("file id");
    let partial = lix
        .execute(
            "INSERT INTO lix_create_checkpoint (relation, row_pk)
             SELECT 'lix_file', lixcol_row_pk
             FROM lix_diff('lix_file', lix_latest_checkpoint_commit_id(), lix_active_branch_commit_id())
             WHERE lixcol_row_pk ->> 0 = $1
             RETURNING commit_id",
            &[Value::Text(new_file_id)],
        )
        .await
        .expect("partial checkpoint on the migrated repository");
    let partial_commit_id = partial.rows()[0]
        .get::<String>("commit_id")
        .expect("partial checkpoint commit id");

    let files = lix
        .execute(
            "SELECT name, directory_id FROM lix_state_at('lix_file', $1)",
            &[Value::Text(partial_commit_id.clone())],
        )
        .await
        .expect("file tree at the post-migration checkpoint");
    let file_names: Vec<String> = files
        .rows()
        .iter()
        .map(|row| row.get::<String>("name").expect("file name"))
        .collect();
    for expected in ["inside.md", "logo.md", "playbook.md", "today.md"] {
        assert!(
            file_names.iter().any(|name| name == expected),
            "post-migration checkpoint tree should be cumulative; missing '{expected}' \
             (got {file_names:?})"
        );
    }
    let directories = lix
        .execute(
            "SELECT id, name FROM lix_state_at('lix_directory', $1)",
            &[Value::Text(partial_commit_id.clone())],
        )
        .await
        .expect("directory tree at the post-migration checkpoint");
    let directory_names: Vec<String> = directories
        .rows()
        .iter()
        .map(|row| row.get::<String>("name").expect("directory name"))
        .collect();
    for expected in ["brand", "docs", "handbook", "notes", "sales"] {
        assert!(
            directory_names.iter().any(|name| name == expected),
            "post-migration checkpoint tree should contain directory '{expected}' \
             (got {directory_names:?})"
        );
    }
    let directory_ids: HashSet<String> = directories
        .rows()
        .iter()
        .map(|row| row.get::<String>("id").expect("directory id"))
        .collect();
    for file in files.rows() {
        if let Ok(directory_id) = file.get::<String>("directory_id") {
            assert!(
                directory_ids.contains(&directory_id),
                "post-migration checkpoint file references directory '{directory_id}' \
                 missing from the same tree"
            );
        }
    }

    lix.close().await.expect("close migrated repository");
}
