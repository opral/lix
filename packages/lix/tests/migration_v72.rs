//! Golden repository-format fixture coverage for the v72 account schema.
//!
//! `fixtures/v72_account_without_profile_uri.lixsnap` is a `LIXSNAP` artifact
//! converted from state generated at parent revision
//! `4816fdba591d7165ff1b0195e74471aa8fc73660`, before `profile_uri` was added.
//! It contains the v72 bootstrap account rows and persisted account schema.
//! SHA-256: `92456923e13bdd5d171e68cd3cb0f1860cd06d8275aad133269598178ed0ed94`.

use std::sync::{Arc, Mutex};

use futures_lite::io::Cursor;
use lix::{ANONYMOUS_ACCOUNT_ID, OpenPhase, OpenProgress, OpenProgressSink, Value, open_lix};

const V72_ACCOUNT_SNAPSHOT: &[u8] =
    include_bytes!("fixtures/v72_account_without_profile_uri.lixsnap");

#[derive(Default)]
struct RecordingProgress {
    events: Mutex<Vec<OpenProgress>>,
}

impl OpenProgressSink for RecordingProgress {
    fn report(&self, progress: OpenProgress) {
        self.events.lock().expect("progress events").push(progress);
    }
}

impl RecordingProgress {
    fn events(&self) -> Vec<OpenProgress> {
        self.events.lock().expect("progress events").clone()
    }
}

#[tokio::test]
async fn fresh_open_reports_initialization_without_migration() {
    let progress = Arc::new(RecordingProgress::default());
    let lix = open_lix()
        .with_open_progress_sink(progress.clone())
        .await
        .expect("fresh repository should initialize and open");

    assert_eq!(lix.open_report().format, 76);
    assert!(lix.open_report().initialized);
    assert_eq!(lix.open_report().migration, None);
    assert_eq!(
        progress
            .events()
            .iter()
            .map(|event| event.phase)
            .collect::<Vec<_>>(),
        vec![
            OpenPhase::Inspecting,
            OpenPhase::Opening,
            OpenPhase::Complete,
        ],
    );
    assert!(
        progress
            .events()
            .iter()
            .all(|event| event.from_format.is_none()),
        "initialization is not a format migration",
    );
}

#[tokio::test]
async fn migrates_profile_uri_and_persists_updates_across_cold_reopen() {
    let progress = Arc::new(RecordingProgress::default());
    let lix = open_lix()
        .with_open_progress_sink(progress.clone())
        .from_snapshot(Cursor::new(V72_ACCOUNT_SNAPSHOT))
        .await
        .expect("opening a v72 repository should migrate it automatically");
    assert_eq!(lix.open_report().format, 76);
    assert!(!lix.open_report().initialized);
    let migration = lix
        .open_report()
        .migration
        .expect("the open report should record the automatic migration");
    assert_eq!(migration.from_format, 72);
    assert_eq!(migration.to_format, 76);

    let events = progress.events();
    assert_eq!(
        events.iter().map(|event| event.phase).collect::<Vec<_>>(),
        vec![
            OpenPhase::Inspecting,
            OpenPhase::Migrating,
            OpenPhase::Validating,
            OpenPhase::Opening,
            OpenPhase::Complete,
        ],
        "automatic migration progress should be deterministic and ordered",
    );
    assert_eq!(events[1].from_format, Some(72));
    assert_eq!(events[1].to_format, 76);
    assert_eq!(events[1].completed, Some(0));
    assert_eq!(events[1].total, None);
    assert!(
        events
            .iter()
            .skip(1)
            .all(|event| event.from_format == Some(72)),
        "every phase after inspection should retain the migration source format",
    );
    let accounts = lix
        .execute("SELECT id, profile_uri FROM lix_account ORDER BY id", &[])
        .await
        .expect("migrated account rows should expose profile_uri");
    assert!(!accounts.rows().is_empty());
    assert!(
        accounts
            .rows()
            .iter()
            .all(|row| matches!(row.values(), [Value::Text(_), Value::Null])),
        "historical account rows must materialize the appended nullable column as NULL"
    );

    let profile_uri = "https://profiles.example/anonymous.json";
    let updated = lix
        .execute(
            "UPDATE lix_account SET profile_uri = $1 WHERE id = $2",
            &[
                Value::Text(profile_uri.to_owned()),
                Value::Text(ANONYMOUS_ACCOUNT_ID.to_owned()),
            ],
        )
        .await
        .expect("profile_uri update should succeed");
    assert_eq!(updated.rows_affected(), 1);
    let mut migrated = Vec::new();
    lix.export_snapshot()
        .write_to(&mut migrated)
        .await
        .expect("migrated Lix should export");
    lix.close().await.expect("migrated Lix should close");
    let lix = open_lix()
        .from_snapshot(Cursor::new(migrated))
        .await
        .expect("migrated repository should cold-open");
    assert_eq!(lix.open_report().format, 76);
    assert_eq!(lix.open_report().migration, None);
    assert!(!lix.open_report().initialized);
    let result = lix
        .execute(
            "SELECT profile_uri FROM lix_account WHERE id = $1",
            &[Value::Text(ANONYMOUS_ACCOUNT_ID.to_owned())],
        )
        .await
        .expect("profile_uri should remain queryable after cold reopen");
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text(profile_uri.to_owned())]
    );
}
