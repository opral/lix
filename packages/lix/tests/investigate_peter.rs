//! One-off investigation over an exported live repository snapshot.
//! Points at the snapshot via PETER_SNAPSHOT; not part of the regular suite.

use lix::{Memory, Value, open_lix};

#[tokio::test]
async fn investigate_exported_repository() {
    let Ok(path) = std::env::var("PETER_SNAPSHOT") else {
        eprintln!("PETER_SNAPSHOT not set; skipping");
        return;
    };
    let bytes = std::fs::read(&path).expect("read snapshot");
    let storage = Memory::from_snapshot(&bytes).expect("decode snapshot");
    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("open exported repository");

    let checkpoints = lix
        .execute(
            "SELECT commit_id FROM lix_checkpoint ORDER BY lixcol_created_at ASC",
            &[],
        )
        .await
        .expect("checkpoint listing");
    for row in checkpoints.rows() {
        let commit_id = row.get::<String>("commit_id").expect("commit id");
        let files = lix
            .execute(
                "SELECT name, directory_id FROM lix_state_at('lix_file', $1)",
                &[Value::Text(commit_id.clone())],
            )
            .await;
        let dirs = lix
            .execute(
                "SELECT id, name FROM lix_state_at('lix_directory', $1)",
                &[Value::Text(commit_id.clone())],
            )
            .await;
        match (&files, &dirs) {
            (Ok(f), Ok(d)) => {
                let dir_ids: std::collections::HashSet<String> = d
                    .rows()
                    .iter()
                    .map(|r| r.get::<String>("id").expect("dir id"))
                    .collect();
                let orphans = f
                    .rows()
                    .iter()
                    .filter(|r| {
                        r.get::<String>("directory_id")
                            .map(|id| !dir_ids.contains(&id))
                            .unwrap_or(false)
                    })
                    .map(|r| r.get::<String>("name").unwrap_or_default())
                    .collect::<Vec<_>>();
                println!(
                    "AUDIT {commit_id}: files={} dirs={} orphans={orphans:?}",
                    f.rows().len(),
                    d.rows().len()
                );
            }
            _ => println!(
                "AUDIT {commit_id}: files_err={:?} dirs_err={:?}",
                files.as_ref().err().map(|e| e.to_string()),
                dirs.as_ref().err().map(|e| e.to_string())
            ),
        }
        let diff = lix
            .execute(
                "SELECT to_path FROM lix_diff('lix_file', lix_root_commit_id(), $1)",
                &[Value::Text(commit_id.clone())],
            )
            .await;
        match diff {
            Ok(d) => println!("  root-diff paths ok: {} rows", d.rows().len()),
            Err(e) => println!("  root-diff FAILED: {e}"),
        }
    }
    lix.close().await.expect("close");
}
