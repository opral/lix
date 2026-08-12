//! Does `json_store` reclaim superseded out-of-band payloads?
//!
//! `stage_delete_refs` is `#[cfg(test)]` and its only caller is the
//! `#[cfg(test)]` `plan_and_stage_authority_gc`, so the shipping sweep
//! (`stage_repository_gc_with_preconditions`) never stages a JSON delete. This
//! probe measures what that costs on the workload being advertised: an agent
//! rewriting the same file over and over.
//!
//! Two fixture facts the measurement depends on:
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
//!
//! ```text
//! e1_json_leak <rewrites> [<rewrites> ...]
//! ```

#![allow(clippy::large_futures)]

use lix::Value;
use lix::integration::{Engine, SessionContext};
use lix::registered_spaces::JSON_SPACE;
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

async fn json_rows(storage: &StorageAdapter<Memory>) -> (usize, usize) {
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open json inventory read");
    let entries = space_inventory(&read, JSON_SPACE.name).await;
    let bytes = entries
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum();
    (entries.len(), bytes)
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
        Engine::initialize(memory.clone())
            .await
            .expect("initialize leak repository");
        let engine = Engine::new(memory.clone())
            .await
            .expect("open leak engine");
        let session = engine
            .open_workspace_session()
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

        for revision in 1..=rewrites {
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

        let storage = StorageAdapter::new(memory.clone());
        let (rows_before, bytes_before) = json_rows(&storage).await;
        collect_repository_gc_for_bench(&storage)
            .await
            .expect("run the shipping repository sweep");
        let (rows_after, bytes_after) = json_rows(&storage).await;

        // Exactly one revision is live: the row was rewritten in place.
        println!(
            "E1LEAK rewrites={rewrites} live_payloads=1 \
             rows_before_gc={rows_before} rows_after_gc={rows_after} \
             bytes_before_gc={bytes_before} bytes_after_gc={bytes_after} \
             reclaimed_rows={} orphan_rows={}",
            rows_before.saturating_sub(rows_after),
            rows_after.saturating_sub(1),
        );
    }
}
