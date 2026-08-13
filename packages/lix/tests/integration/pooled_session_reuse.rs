//! A pooled DataFusion session outlives the statement that borrowed it, and its
//! execution functions are registered once for the life of the session rather
//! than per statement. These tests pin the resulting hazard: a second execution
//! of the same statement shape must never observe the first execution's account,
//! branch, commit or snapshot.

use lix::engine::Engine;
use lix::storage::Memory;
use lix::{CreateBranchOptions, SwitchBranchOptions, Value};

const DRAFT_BRANCH_ID: &str = "01930000-0000-7000-8000-0000000000d1";

const ACTIVE_FACTS_SQL: &str = "SELECT lix_active_branch_id() AS branch_id, \
    lix_active_branch_commit_id() AS commit_id, \
    lix_active_account_id() AS account_id";

async fn open_engine() -> Engine {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    Engine::new(storage)
        .await
        .expect("initialized storage should open")
}

fn text(rows: &lix::ExecuteResult, column: &str) -> Option<String> {
    match rows.rows()[0].value(column).expect("column is present") {
        Value::Text(value) => Some(value.clone()),
        Value::Null => None,
        other => panic!("unexpected {column} value: {other:?}"),
    }
}

/// Switching branches between two executions of the same statement shape must
/// change what `lix_active_branch_id()` reports.
///
/// Before the execution functions moved into a per-session slot they were
/// deregistered and re-registered per statement, so a stale registration was
/// impossible by construction. The slot is what preserves that property now.
#[tokio::test(flavor = "current_thread")]
async fn execution_functions_follow_a_branch_switch_on_a_reused_session() {
    let engine = open_engine().await;
    let session = engine.open_session().await.expect("session should open");

    session
        .create_branch(CreateBranchOptions {
            id: Some(DRAFT_BRANCH_ID.to_string()),
            name: "Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("branch should be created");

    // Warm the pooled session and every planning cache on this exact shape.
    let before = session
        .execute(ACTIVE_FACTS_SQL, &[])
        .await
        .expect("active facts should read");
    let main_branch_id = text(&before, "branch_id").expect("branch id is present");
    let before_commit = text(&before, "commit_id").expect("commit id is present");
    let account = text(&before, "account_id").expect("account id is present");

    session
        .switch_branch(SwitchBranchOptions {
            branch_id: DRAFT_BRANCH_ID.to_string(),
        })
        .await
        .expect("switch should succeed");

    let after = session
        .execute(ACTIVE_FACTS_SQL, &[])
        .await
        .expect("active facts should read after the switch");
    assert_eq!(
        text(&after, "branch_id").as_deref(),
        Some(DRAFT_BRANCH_ID),
        "the reused session reported the previous statement's branch"
    );
    assert_ne!(text(&after, "branch_id"), Some(main_branch_id.clone()));
    assert_eq!(text(&after, "account_id"), Some(account));
    // The draft branch was created from main's head, so the commit id is only
    // required to still be present and to track the branch that is now active.
    assert!(text(&after, "commit_id").is_some());

    // Switching back must not leave the draft's identity behind either.
    session
        .switch_branch(SwitchBranchOptions {
            branch_id: main_branch_id.clone(),
        })
        .await
        .expect("switch back should succeed");
    let restored = session
        .execute(ACTIVE_FACTS_SQL, &[])
        .await
        .expect("active facts should read after switching back");
    assert_eq!(text(&restored, "branch_id"), Some(main_branch_id));
    assert_eq!(text(&restored, "commit_id"), Some(before_commit));
}

/// `lix_active_branch_commit_id()` must advance when the active branch commits,
/// even though the SQL text and parameter bytes are unchanged.
#[tokio::test(flavor = "current_thread")]
async fn active_commit_id_advances_between_executions_of_one_shape() {
    let engine = open_engine().await;
    let session = engine.open_session().await.expect("session should open");

    let sql = "SELECT lix_active_branch_commit_id() AS commit_id";
    let first = text(
        &session.execute(sql, &[]).await.expect("first read"),
        "commit_id",
    )
    .expect("commit id is present");

    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('pooled-session-probe', 'v1')",
            &[],
        )
        .await
        .expect("write should commit");

    let second = text(
        &session.execute(sql, &[]).await.expect("second read"),
        "commit_id",
    )
    .expect("commit id is present");
    assert_ne!(
        first, second,
        "a reused session reported the commit id captured by an earlier statement"
    );
}

/// The volatile execution functions must produce a fresh value per invocation,
/// per row and per statement — never one frozen into the session.
#[tokio::test(flavor = "current_thread")]
async fn volatile_execution_functions_stay_fresh_on_a_reused_session() {
    let engine = open_engine().await;
    let session = engine.open_session().await.expect("session should open");

    let sql = "SELECT uuidv7() AS first, uuidv7() AS second, \
        lix_timestamp() AS stamp";
    let mut uuids = Vec::new();
    for _ in 0..3 {
        let rows = session.execute(sql, &[]).await.expect("volatile read");
        let first = text(&rows, "first").expect("uuid is present");
        let second = text(&rows, "second").expect("uuid is present");
        assert_ne!(
            first, second,
            "two calls in one statement returned the same uuid"
        );
        assert!(text(&rows, "stamp").is_some());
        uuids.push(first);
        uuids.push(second);
    }
    let unique = uuids.iter().collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        unique.len(),
        uuids.len(),
        "a reused session replayed uuids from an earlier statement"
    );
}

/// A row committed between two executions of one statement shape must be
/// visible to the second, on the same pooled session and the same cached plan.
#[tokio::test(flavor = "current_thread")]
async fn a_reused_session_reads_rows_committed_after_its_plan_was_cached() {
    let engine = open_engine().await;
    let session = engine.open_session().await.expect("session should open");

    let sql = "SELECT key FROM lix_key_value WHERE key LIKE 'pooled-visibility-%' ORDER BY key";
    for round in 0..4 {
        let rows = session
            .execute(sql, &[])
            .await
            .expect("read should execute");
        assert_eq!(
            rows.len(),
            round,
            "the reused session served a stale snapshot"
        );
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v')",
                &[Value::Text(format!("pooled-visibility-{round}"))],
            )
            .await
            .expect("write should commit");
    }
    assert_eq!(
        session
            .execute(sql, &[])
            .await
            .expect("final read should execute")
            .len(),
        4
    );
}

/// `information_schema` is registered only for statements that can reach it.
/// Interleaving it with ordinary statements on a pooled session must not make
/// its rows go missing or go stale.
#[tokio::test(flavor = "current_thread")]
async fn information_schema_stays_available_across_pooled_statements() {
    let engine = open_engine().await;
    let session = engine.open_session().await.expect("session should open");

    let information_schema_sql =
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'lix_key_value'";
    for _ in 0..3 {
        assert_eq!(
            session
                .execute(information_schema_sql, &[])
                .await
                .expect("information_schema should read")
                .len(),
            1
        );
        session
            .execute("SELECT key FROM lix_key_value LIMIT 1", &[])
            .await
            .expect("ordinary read should execute");
    }

    session
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(
                serde_json::json!({
                    "x-lix-key": "pooled_probe",
                    "x-lix-primary-key": ["/id"],
                    "type": "object",
                    "properties": { "id": { "type": "string" } },
                    "required": ["id"],
                    "additionalProperties": false
                })
                .to_string(),
            )],
        )
        .await
        .expect("schema should register");

    assert_eq!(
        session
            .execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'pooled_probe'",
                &[],
            )
            .await
            .expect("information_schema should see the new surface")
            .len(),
        1
    );
}
