//! Does `json_store` reclaim superseded out-of-band payloads?
//!
//! `stage_delete_refs` is `#[cfg(test)]` and its only caller is the
//! `#[cfg(test)]` `plan_and_stage_authority_gc`, so the shipping sweep
//! (`stage_repository_gc_with_preconditions`) never stages a JSON delete. This
//! probe measures what that costs on the workload being advertised: an agent
//! rewriting the same file over and over.
//!
//! **This probe's original form produced misleading evidence and was corrected.**
//! It never created a checkpoint, and the shipping sweep retires nothing
//! without one: a 1000-edit stream with no checkpoint retires 0 commit-state
//! manifests, while the same stream with a checkpoint every 10 edits retires
//! 1095 (measured by `e29_locator_leak`). The original "0 rows reclaimed at
//! 10/100/1000 rewrites" was therefore consistent with two different worlds —
//! "`json_store` is not wired into the sweep" and "the sweep had no work to do
//! in this fixture" — and could not separate them. The checkpoint cadence below
//! exists to separate them, so **do not remove it**.
//!
//! Three fixture facts the measurement depends on:
//!
//! 1. **The out-of-band threshold is `JSON_INLINE_MAX_BYTES` (1 KiB) on the raw
//!    normalized snapshot.** The 512-byte/128-byte constants are the *zstd*
//!    thresholds inside the store and do not decide whether a payload reaches
//!    it at all. A payload at or under 1 KiB stays inline in the hot row and
//!    this probe would measure an empty table.
//! 2. **The store is content-addressed** (`JsonRef::for_content`). Rewriting
//!    byte-identical content dedups to one row and leaks nothing. Every
//!    rewrite here is a *distinct* payload, which is what an agent editing a
//!    file actually produces.
//! 3. **A checkpoint is what makes a commit retirable.** Without one every
//!    commit remains a physical authority of the live head, so no retirement is
//!    proven and no plane — JSON or otherwise — can be observed being swept.
//!
//! Two churn shapes, because they bound the answer from both sides:
//!
//! * `rewrite` — one entity, rewritten `n` times. Exactly **one** payload is
//!   live at the end and the other `n` are superseded. This is the leak arm.
//! * `insert` — `n` distinct entities, each written once. **All** payloads are
//!   live. This is the control: a sweep that reclaims anything here is wrong,
//!   so it separates "reclaims dead payloads" from "reclaims payloads".
//!
//! ```text
//! e1_json_leak [<edits> ...]              # default 10 100 1000
//! E1_SHAPES=rewrite,insert
//! E1_CHECKPOINT_CADENCES=0,10,100         # 0 means "never checkpoint"
//! ```

#![allow(clippy::large_futures)]

use lix::Value;
use lix::integration::{Engine, SessionContext};
use lix::registered_spaces::{JSON_SPACE, TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE};
use lix::storage::Storage;
use lix::storage_adapter::{Memory, StorageAdapter, StorageReadOptions};
use lix::storage_bench::{collect_repository_gc_for_bench, space_inventory};

/// A snapshot whose normalized JSON is comfortably past the 1 KiB inline
/// threshold, and distinct for every `revision`.
fn payload(revision: usize) -> String {
    let filler = format!("rev-{revision:08}-");
    let mut body = String::with_capacity(2_048);
    while body.len() < 1_800 {
        body.push_str(&filler);
    }
    serde_json::json!({ "revision": revision, "body": body }).to_string()
}

async fn register_schema<S>(session: &SessionContext<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "x-lix-key": "leak_row",
        "x-lix-primary-key": ["/path"],
        "type": "object",
        "required": ["path", "value"],
        "properties": {
            "path": { "type": "string" },
            "value": {
                "type": ["object", "array", "string", "number", "integer", "boolean", "null"]
            }
        },
        "additionalProperties": false
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register leak schema");
}

/// `(rows, bytes)` in one space.
async fn space_rows(storage: &StorageAdapter<Memory>, space: &str) -> (usize, usize) {
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open space inventory read");
    let entries = space_inventory(&read, space).await;
    let bytes = entries
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum();
    (entries.len(), bytes)
}

fn list_from_env(var: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(var).map_or_else(
        |_| default.to_vec(),
        |raw| {
            raw.split(',')
                .filter(|part| !part.trim().is_empty())
                .map(|part| part.trim().parse::<usize>().expect("numeric list entry"))
                .collect()
        },
    )
}

/// One matrix cell: `edits` mutations of `shape`, checkpointing every
/// `cadence` edits, then one shipping sweep.
async fn run_cell(shape: &str, cadence: usize, edits: usize) {
    let memory = Memory::new();
    Engine::initialize(memory.clone())
        .await
        .expect("initialize leak repository");
    let engine = Engine::new(memory.clone())
        .await
        .expect("open leak engine");
    let session = engine
        .open_session()
        .await
        .expect("open leak workspace");
    register_schema(&session).await;

    session
        .execute(
            "INSERT INTO leak_row (path, value) VALUES ($1, lix_json($2))",
            &[
                Value::Text("/agent/file".to_owned()),
                Value::Text(payload(0)),
            ],
        )
        .await
        .expect("insert the first revision");

    for revision in 1..=edits {
        if shape == "insert" {
            session
                .execute(
                    "INSERT INTO leak_row (path, value) VALUES ($1, lix_json($2))",
                    &[
                        Value::Text(format!("/agent/file-{revision}")),
                        Value::Text(payload(revision)),
                    ],
                )
                .await
                .expect("insert a distinct row");
        } else {
            session
                .execute(
                    "UPDATE leak_row SET value = lix_json($2) WHERE path = $1",
                    &[
                        Value::Text("/agent/file".to_owned()),
                        Value::Text(payload(revision)),
                    ],
                )
                .await
                .expect("rewrite the same row");
        }
        if cadence > 0 && revision % cadence == 0 {
            session
                .create_checkpoint()
                .await
                .expect("create a checkpoint");
        }
    }
    let mut checkpoints = 0;
    if cadence > 0 {
        // A checkpoint releases the *previous* checkpoint's pins, so the last
        // one needs a successor before its predecessors are provably retirable.
        checkpoints = edits / cadence + 1;
        session
            .create_checkpoint()
            .await
            .expect("create the releasing checkpoint");
    }

    let storage = StorageAdapter::new(memory.clone());
    let (json_before, json_bytes_before) = space_rows(&storage, JSON_SPACE.name).await;
    let (manifests_before, _) =
        space_rows(&storage, TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE.name).await;

    collect_repository_gc_for_bench(&storage)
        .await
        .expect("run the shipping repository sweep");

    let (json_after, json_bytes_after) = space_rows(&storage, JSON_SPACE.name).await;
    let (manifests_after, _) =
        space_rows(&storage, TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE.name).await;

    // `rewrite` leaves one live payload; `insert` leaves one per entity.
    let live_payloads = if shape == "insert" { edits + 1 } else { 1 };
    let leaked = json_after.saturating_sub(live_payloads);

    println!(
        "E1LEAK shape={shape} cadence={cadence} edits={edits} checkpoints={checkpoints} \
         live_payloads={live_payloads} \
         json_before={json_before} json_after={json_after} \
         json_bytes_before={json_bytes_before} json_bytes_after={json_bytes_after} \
         manifests_before={manifests_before} manifests_after={manifests_after} \
         retired_manifests={} reclaimed_json={} leaked_payloads={leaked} \
         leaked_per_edit={:.3}",
        manifests_before.saturating_sub(manifests_after),
        json_before.saturating_sub(json_after),
        leaked as f64 / edits as f64
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let counts = std::env::args()
        .skip(1)
        .map(|arg| arg.parse::<usize>().expect("edit count"))
        .collect::<Vec<_>>();
    let counts = if counts.is_empty() {
        vec![10, 100, 1_000]
    } else {
        counts
    };
    let cadences = list_from_env("E1_CHECKPOINT_CADENCES", &[0, 10, 100]);
    let shapes = std::env::var("E1_SHAPES")
        .unwrap_or_else(|_| "rewrite,insert".to_owned())
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();

    for shape in &shapes {
        for &cadence in &cadences {
            for &edits in &counts {
                run_cell(shape, cadence, edits).await;
            }
        }
    }
}
