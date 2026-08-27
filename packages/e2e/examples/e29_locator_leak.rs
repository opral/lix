//! Does the shipping sweep reclaim `TRACKED_STATE_CHANGE_LOCATOR_SPACE`?
//!
//! `stage_delete_change_locators` is `#[cfg(test)]`
//! (`tracked_state/storage.rs:5521`) and its only caller is the `#[cfg(test)]`
//! `plan_and_stage_authority_gc` (`gc.rs:1420`, delete at `gc.rs:1888`). The
//! shipping sweep `stage_repository_gc_with_preconditions` retires a commit's
//! physical state — manifest, delta segments, mutation-directory nodes — via
//! `stage_retire_commit_physical_state` and never touches the locator plane.
//!
//! Two questions, one run:
//!
//! 1. **Leak.** How many locator rows survive a sweep that retired their owning
//!    commits? Deterministic, so one rep.
//! 2. **Correctness class.** A surviving locator names a commit whose manifest
//!    and segments are gone. What does a read that follows it do — error,
//!    wrong answer, or clean miss? Every consumer identity-checks the row it
//!    lands on (`storage.rs:6648`, `:6830`, `:7958`), so the static answer is
//!    "never a wrong answer"; this measures which of error/miss it actually is
//!    through the public SQL surface.
//!
//! ```text
//! e29_locator_leak <rewrites> [<rewrites> ...]
//! ```

#![allow(clippy::large_futures)]

use lix::Value;
use lix::registered_spaces::{
    CHANGE_SPACE, TRACKED_STATE_CHANGE_LOCATOR_SPACE, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
    TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
};
use lix::storage::Storage;
use lix::storage_adapter::{Memory, StorageAdapter, StorageReadOptions};
use lix::storage_bench::{collect_repository_gc_for_bench, space_inventory};
use lix::{Lix, open_lix};

/// Hyphenated lowercase UUID from 16 raw bytes, matching how Lix
/// renders a `ChangeId` on the public surface.
fn format_uuid(bytes: &[u8]) -> Option<String> {
    if bytes.len() != 16 {
        return None;
    }
    let hex = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Some(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
}

async fn register_schema<S>(session: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "locator_row",
        "columns": [
            { "name": "path", "type": "text", "nullable": false },
            { "name": "value", "type": "jsonb", "nullable": false },
        ],
        "primary_key": ["path"],
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (CAST($1 AS JSONB), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register locator schema");
}

/// `(rows, bytes)` in one space.
async fn space_rows(storage: &StorageAdapter<Memory>, space: &str) -> (usize, usize) {
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open inventory read");
    let entries = space_inventory(&read, space).await;
    let bytes = entries
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum();
    (entries.len(), bytes)
}

async fn locator_keys(storage: &StorageAdapter<Memory>) -> Vec<Vec<u8>> {
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open locator read");
    space_inventory(&read, TRACKED_STATE_CHANGE_LOCATOR_SPACE.name)
        .await
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let counts = std::env::args()
        .skip(1)
        .map(|arg| arg.parse::<usize>().expect("rewrite count"))
        .collect::<Vec<_>>();
    let counts = if counts.is_empty() {
        vec![10, 100, 1_000]
    } else {
        counts
    };

    for rewrites in counts {
        let memory = Memory::new();
        open_lix()
            .with_storage(memory.clone())
            .await
            .expect("initialize locator repository");
        let lix = open_lix()
            .with_storage(memory.clone())
            .await
            .expect("open locator lix");
        let session = lix
            .open_another_session()
            .await
            .expect("open locator workspace");
        register_schema(&session).await;

        session
            .execute(
                "INSERT INTO locator_row (path, value) VALUES ($1, CAST($2 AS JSONB))",
                &[
                    Value::Text("/agent/file".to_owned()),
                    Value::Text(serde_json::json!({ "revision": 0 }).to_string()),
                ],
            )
            .await
            .expect("insert the first revision");

        // Two churn shapes, because they produce different change-id
        // populations: an in-place rewrite stream reuses one row key, while
        // distinct inserts mint a new row (and a new change) every time.
        //
        // Checkpoints matter more than either. Without one the shipping sweep
        // proves nothing retirable — every commit stays a physical authority of
        // the live head — so a probe that never checkpoints measures a sweep
        // with no work to do and cannot tell "this plane is not wired into GC"
        // apart from "GC retired nothing at all".
        let checkpoint_every = std::env::var("E29_CHECKPOINT_EVERY")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(10);
        let insert_arm = std::env::var("E29_SHAPE").as_deref() == Ok("insert");
        for revision in 1..=rewrites {
            if insert_arm {
                session
                    .execute(
                        "INSERT INTO locator_row (path, value) VALUES ($1, CAST($2 AS JSONB))",
                        &[
                            Value::Text(format!("/agent/file-{revision}")),
                            Value::Text(serde_json::json!({ "revision": revision }).to_string()),
                        ],
                    )
                    .await
                    .expect("insert a distinct row");
            } else {
                session
                    .execute(
                        "UPDATE locator_row SET value = CAST($2 AS JSONB) WHERE path = $1",
                        &[
                            Value::Text("/agent/file".to_owned()),
                            Value::Text(serde_json::json!({ "revision": revision }).to_string()),
                        ],
                    )
                    .await
                    .expect("rewrite the same row");
            }
            if checkpoint_every > 0 && revision % checkpoint_every == 0 {
                session
                    .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
                    .await
                    .expect("create a checkpoint");
            }
        }
        if checkpoint_every > 0 {
            // A checkpoint releases the *previous* checkpoint's pins, so the
            // last one needs a successor before its predecessors are provably
            // retirable.
            session
                .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
                .await
                .expect("create the releasing checkpoint");
        }

        let storage = StorageAdapter::new(memory.clone());
        let (loc_before, loc_bytes_before) =
            space_rows(&storage, TRACKED_STATE_CHANGE_LOCATOR_SPACE.name).await;
        let (man_before, _) =
            space_rows(&storage, TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE.name).await;
        let (seg_before, _) =
            space_rows(&storage, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE.name).await;
        let (chg_before, _) = space_rows(&storage, CHANGE_SPACE.name).await;

        collect_repository_gc_for_bench(&storage)
            .await
            .expect("run the shipping repository sweep");

        let (loc_after, loc_bytes_after) =
            space_rows(&storage, TRACKED_STATE_CHANGE_LOCATOR_SPACE.name).await;
        let (man_after, _) =
            space_rows(&storage, TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE.name).await;
        let (seg_after, _) =
            space_rows(&storage, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE.name).await;
        let (chg_after, _) = space_rows(&storage, CHANGE_SPACE.name).await;

        println!(
            "E29LOC shape={} rewrites={rewrites} \
             locators_before={loc_before} locators_after={loc_after} \
             locator_bytes_before={loc_bytes_before} locator_bytes_after={loc_bytes_after} \
             manifests_before={man_before} manifests_after={man_after} \
             segments_before={seg_before} segments_after={seg_after} \
             changes_before={chg_before} changes_after={chg_after} \
             reclaimed_locators={} retired_manifests={}",
            if insert_arm { "insert" } else { "rewrite" },
            loc_before.saturating_sub(loc_after),
            man_before.saturating_sub(man_after)
        );

        // Which surviving locators name a change the public ledger no longer
        // carries? Those are the dangling ones.
        let live_change_ids = session
            .execute("SELECT id FROM lix_change", &[])
            .await
            .expect("read the public change ledger")
            .rows()
            .iter()
            .filter_map(|row| match row.values().first() {
                Some(Value::Text(id)) => Some(id.clone()),
                _ => None,
            })
            .collect::<std::collections::HashSet<_>>();

        let surviving = locator_keys(&storage).await;
        let dangling = surviving
            .iter()
            .filter_map(|key| format_uuid(key))
            .filter(|id| !live_change_ids.contains(id))
            .collect::<Vec<_>>();
        println!(
            "E29LOC rewrites={rewrites} surviving_locators={} live_ledger_changes={} dangling_locators={}",
            surviving.len(),
            live_change_ids.len(),
            dangling.len()
        );

        // What does a read that follows a dangling locator do?
        for id in dangling.iter().take(3) {
            let outcome = match session
                .execute(
                    "SELECT id FROM lix_change WHERE id = $1",
                    &[Value::Text(id.clone())],
                )
                .await
            {
                Ok(rows) if rows.rows().is_empty() => "clean_miss".to_owned(),
                Ok(rows) => format!("returned_{}_rows", rows.rows().len()),
                Err(error) => format!("error:{error}"),
            };
            println!("E29LOC rewrites={rewrites} dangling_read change_id={id} outcome={outcome}");
        }
    }
}
