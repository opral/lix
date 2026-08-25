//! Golden repository-format fixture coverage for protocol v68.
//!
//! `fixtures/v68_bundled_csv_history.snapshot` is the raw, deterministic
//! `Memory::export_snapshot` output generated from `origin/main` commit
//! `89aea5d55773586ea60f77c1d9dddcfc8b394dd1`, whose repository protocol is
//! v68. The baseline helper was temporary because production v68 initialization
//! and revision fences use wall-clock UUIDs: it replaced `SystemFunctionProvider`
//! UUID/time calls and the seven direct revision/fence `Uuid::now_v7` call sites
//! with one fixed UUID sequence and timestamp. It then registered the bundled
//! `csv_row` schema, inserted one row, updated it once, exported, cold-reopened,
//! asserted one current row and two history rows, and asserted that re-export
//! after reopen was byte-identical. Two independent helper runs produced the
//! same 36,275-byte fixture with SHA-256
//! `00ca0adb275e77d9b76563501fae5158707c3aa18ef68ca766bef61297ea8a10`.
//! The temporary helper and baseline patches are intentionally not retained.

use lix::Memory;
use lix::migration::{MigrationOptions, MigrationStatus, inspect_lix, migrate_lix};
use lix::open_lix;
#[cfg(feature = "server-protocol")]
use lix::server_protocol::{ServerProtocolBody, ServerProtocolContext};
#[cfg(feature = "server-protocol")]
use lix::telemetry::{CallbackTelemetrySink, CompletedTelemetrySpan};

const V68_SNAPSHOT: &[u8] = include_bytes!("fixtures/v68_bundled_csv_history.snapshot");
// Generated with the protocol-v68 engine in `lix-engine-typed-main-current`:
// one 700-row custom Schema v1 INSERT (an LXMD1 external authority), followed
// by one plugin delete and one `lix_key_value` insert/delete. The 53,577-byte
// Memory snapshot has SHA-256
// 45bdd1115e5dd6a8f063b73006c00fadc17ed9fcf537f8bd2004cf89c31c62b8.
const V68_EXTERNAL_TOMBSTONES: &[u8] = include_bytes!("fixtures/v68_external_tombstones.snapshot");

#[tokio::test]
async fn current_format_inspection_recognizes_v68_golden_snapshot() {
    let storage = Memory::from_snapshot(V68_SNAPSHOT).expect("v68 golden snapshot should decode");

    assert_eq!(
        inspect_lix(&storage)
            .await
            .expect("v68 format inspection should succeed"),
        MigrationStatus::Required {
            from_version: 68,
            to_version: 72,
        }
    );
}

#[tokio::test]
async fn migrates_external_v68_authority_and_plugin_and_engine_tombstones() {
    let storage = Memory::from_snapshot(V68_EXTERNAL_TOMBSTONES)
        .expect("external v68 golden snapshot should decode");
    assert_eq!(
        inspect_lix(&storage).await.unwrap(),
        MigrationStatus::Required {
            from_version: 68,
            to_version: 72,
        }
    );
    let report = migrate_lix(storage.clone(), MigrationOptions::default())
        .await
        .expect("external v68 authority should migrate");
    assert!(report.commit_members_rewritten >= 704);

    let migrated = storage.export_snapshot().unwrap();
    drop(storage);
    let reopened = Memory::from_snapshot(&migrated).unwrap();
    let lix = open_lix()
        .with_storage(reopened)
        .await
        .expect("migrated external repository should cold-open");

    let current = lix
        .execute("SELECT id, body FROM migration_external_item", &[])
        .await
        .expect("external plugin rows should decode from typed payloads");
    assert_eq!(current.rows().len(), 699);
    assert!(
        lix.execute(
            "SELECT id FROM migration_external_item WHERE id = 'row-0000'",
            &[],
        )
        .await
        .unwrap()
        .rows()
        .is_empty()
    );
    assert_eq!(
        lix.execute(
            "SELECT id FROM lix_change WHERE schema_key = 'migration_external_item' AND row_pk = CAST('[\"row-0000\"]' AS JSONB)",
            &[],
        )
        .await
        .expect("plugin live and tombstone history should replay")
        .rows()
        .len(),
        2
    );
    assert_eq!(
        lix.execute(
            "SELECT id FROM lix_change WHERE schema_key = 'migration_external_item' AND row_pk = CAST('[\"row-0000\"]' AS JSONB) AND snapshot_content IS NULL",
            &[],
        )
        .await
        .expect("plugin tombstone should carry no payload")
        .rows()
        .len(),
        1
    );

    assert!(
        lix.execute(
            "SELECT key FROM lix_key_value WHERE key = 'migration_deleted_engine'",
            &[],
        )
        .await
        .unwrap()
        .rows()
        .is_empty()
    );
    assert_eq!(
        lix.execute(
            "SELECT id FROM lix_change WHERE schema_key = 'lix_key_value' AND row_pk = CAST('[\"migration_deleted_engine\"]' AS JSONB)",
            &[],
        )
        .await
        .expect("engine live and tombstone history should replay")
        .rows()
        .len(),
        2
    );
    assert_eq!(
        lix.execute(
            "SELECT id FROM lix_change WHERE schema_key = 'lix_key_value' AND row_pk = CAST('[\"migration_deleted_engine\"]' AS JSONB) AND snapshot_content IS NULL",
            &[],
        )
        .await
        .expect("engine tombstone should carry no payload")
        .rows()
        .len(),
        1
    );
}

#[tokio::test]
async fn migrates_v68_fixture_and_reopens_with_current_and_history_rows() {
    let storage = Memory::from_snapshot(V68_SNAPSHOT).expect("v68 golden snapshot should decode");
    let Err(open_error) = open_lix().with_storage(storage.clone()).await else {
        panic!("normal engine open must require migration");
    };
    assert_eq!(open_error.code, "LIX_ERROR_REPOSITORY_MIGRATION_REQUIRED");
    let report = migrate_lix(storage.clone(), MigrationOptions::default())
        .await
        .expect("v68 fixture should migrate");
    assert_eq!(report.from_version, 68);
    assert_eq!(report.to_version, 72);
    assert!(report.changes_rewritten >= 2);
    assert!(report.commit_members_rewritten >= 2);
    assert!(report.hot_rows_rewritten >= 1);
    assert_eq!(
        inspect_lix(&storage).await.unwrap(),
        MigrationStatus::Current { version: 72 }
    );
    let retry = migrate_lix(storage.clone(), MigrationOptions::default())
        .await
        .expect("migration retry should be idempotent");
    assert_eq!(retry.from_version, 72);
    assert_eq!(retry.to_version, 72);
    assert_eq!(retry.changes_rewritten, 0);

    let migrated_snapshot = storage
        .export_snapshot()
        .expect("migrated repository should export");
    drop(storage);
    let reopened = Memory::from_snapshot(&migrated_snapshot)
        .expect("migrated repository should import into a fresh backend");
    let lix = open_lix()
        .with_storage(reopened)
        .await
        .expect("migrated repository should cold-open");
    lix.create_checkpoint()
        .await
        .expect("migrated v68 branch head should support a checkpoint alias");
    let current = lix
        .execute("SELECT id, cells, order_key FROM csv_row", &[])
        .await
        .expect("migrated typed current row should read");
    assert_eq!(current.rows().len(), 1);
    let history = lix
        .execute(
            "SELECT id FROM lix_change WHERE schema_key = 'csv_row' ORDER BY created_at",
            &[],
        )
        .await
        .expect("migrated history should read");
    assert_eq!(history.rows().len(), 2);

    // The same v68 typed-row rewrite must convert engine-owned rows, not
    // merely the bundled plugin row used by this fixture. Reading both their
    // current and changelog projections forces native payload decoding after
    // the cold reopen.
    for (schema_key, current_query) in [
        ("lix_key_value", "SELECT key, value FROM lix_key_value"),
        ("lix_account", "SELECT id, name FROM lix_account"),
        (
            "lix_registered_schema",
            "SELECT schema_key, value FROM lix_registered_schema",
        ),
    ] {
        let current = lix
            .execute(current_query, &[])
            .await
            .unwrap_or_else(|error| {
                panic!("migrated {schema_key} current rows should read: {error}")
            });
        assert!(
            !current.rows().is_empty(),
            "fixture should contain current {schema_key} rows"
        );
        let history = lix
            .execute(
                &format!(
                    "SELECT snapshot_content FROM lix_change WHERE schema_key = '{schema_key}' AND snapshot_content IS NOT NULL"
                ),
                &[],
            )
            .await
            .unwrap_or_else(|error| panic!("migrated {schema_key} history should read: {error}"));
        assert!(
            !history.rows().is_empty(),
            "fixture should contain live {schema_key} history"
        );
    }
}

#[cfg(feature = "server-protocol")]
#[tokio::test]
async fn migrated_v68_fixture_serves_two_independent_protocol_sessions() {
    use http::StatusCode;
    use http_body_util::BodyExt as _;
    use std::sync::{Arc, Mutex};

    let storage = Memory::from_snapshot(V68_SNAPSHOT).expect("v68 golden snapshot should decode");
    let Err(open_error) = open_lix().with_storage(storage.clone()).serve().await else {
        panic!("protocol serving must require an explicit migration");
    };
    assert_eq!(open_error.code, "LIX_ERROR_REPOSITORY_MIGRATION_REQUIRED");
    migrate_lix(storage.clone(), MigrationOptions::default())
        .await
        .expect("v68 fixture should migrate");

    let spans = Arc::new(Mutex::new(Vec::<CompletedTelemetrySpan>::new()));
    let captured_spans = Arc::clone(&spans);
    let protocol = open_lix()
        .with_storage(storage.clone())
        .with_telemetry(Arc::new(CallbackTelemetrySink::new(move |span| {
            captured_spans.lock().expect("telemetry spans").push(span);
        })))
        .serve()
        .await
        .expect("migrated repository should serve without a root session");
    assert_eq!(
        spans
            .lock()
            .expect("telemetry spans")
            .iter()
            .filter(|span| span.start.name == "lix.repository.opened")
            .count(),
        0,
        "serving a migrated repository must not create a hidden root session"
    );
    let before_handshake = storage
        .export_snapshot()
        .expect("capture migrated repository before handshake");

    let mut session_ids = Vec::new();
    for index in 0..2 {
        let response = protocol
            .handle(
                http::Request::builder()
                    .method("GET")
                    .uri("/lix/v1")
                    .body(ServerProtocolBody::empty())
                    .expect("build handshake"),
                ServerProtocolContext::anonymous(),
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect handshake")
            .to_bytes();
        let body: serde_json::Value =
            serde_json::from_slice(&body).expect("decode handshake response");
        let session_id = body["sessionId"]
            .as_str()
            .expect("handshake session id")
            .to_owned();
        session_ids.push(session_id.clone());
        if index == 0 {
            assert_eq!(
                storage
                    .export_snapshot()
                    .expect("capture repository after handshake"),
                before_handshake,
                "a v72 handshake must not repair or mutate repository storage"
            );
        }

        for (query_index, (sql, expected_rows)) in [
            ("SELECT id, cells, order_key FROM csv_row", 1),
            ("SELECT id FROM lix_history('csv_row')", 1),
            ("SELECT schema_key FROM lix_registered_schema", 1),
        ]
        .into_iter()
        .enumerate()
        {
            let response = protocol
                .handle(
                    http::Request::builder()
                        .method("POST")
                        .uri("/lix/v1/execute")
                        .header("lix-session-id", &session_id)
                        .header(
                            "idempotency-key",
                            format!("migrated-v68-{index}-{query_index}"),
                        )
                        .header("content-type", "application/json")
                        .body(ServerProtocolBody::from(
                            serde_json::json!({ "sql": sql, "params": [] }).to_string(),
                        ))
                        .expect("build migrated query"),
                    ServerProtocolContext::anonymous(),
                )
                .await;
            assert_eq!(response.status(), StatusCode::OK, "query failed: {sql}");
            let body = response
                .into_body()
                .collect()
                .await
                .expect("collect migrated query")
                .to_bytes();
            let body: serde_json::Value =
                serde_json::from_slice(&body).expect("decode migrated query");
            assert!(
                body["rows"]
                    .as_array()
                    .is_some_and(|rows| rows.len() >= expected_rows),
                "migrated query returned too few rows: {sql}: {body}"
            );
        }
    }
    assert_ne!(session_ids[0], session_ids[1]);
    assert_eq!(
        spans
            .lock()
            .expect("telemetry spans")
            .iter()
            .filter(|span| span.start.name == "lix.repository.opened")
            .count(),
        2,
        "each successful handshake must bind exactly one application session"
    );
}
