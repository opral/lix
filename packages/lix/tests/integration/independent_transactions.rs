use std::time::Duration;

use lix::{CreateBranchOptions, ObserveEvent, ObserveEvents, SwitchBranchOptions, Value, open_lix};

const FILES_SQL: &str =
    "SELECT path, content FROM lix_file WHERE path NOT LIKE '/.lix/%' ORDER BY path";

async fn next_event(events: &mut ObserveEvents) -> ObserveEvent {
    tokio::time::timeout(Duration::from_secs(2), events.next())
        .await
        .expect("observation must not stall")
        .expect("observation must remain usable during a transaction")
        .expect("observation must remain open")
}

async fn expect_no_event(events: &mut ObserveEvents) {
    assert!(
        tokio::time::timeout(Duration::from_millis(100), events.next())
            .await
            .is_err(),
        "uncommitted changes must neither publish nor invalidate an observer"
    );
}

#[tokio::test]
async fn public_transaction_keeps_parent_reads_and_observers_on_committed_state() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ('/source/note.txt', CAST('keep me' AS BYTEA))",
        &[],
    )
    .await
    .unwrap();
    let mut existing = lix.observe(FILES_SQL, &[]).unwrap();
    let initial = next_event(&mut existing).await;

    let mut transaction = lix.begin_transaction().await.unwrap();
    transaction
        .execute(
            "UPDATE lix_file SET path = '/target/source/note.txt' WHERE path = '/source/note.txt'",
            &[],
        )
        .await
        .unwrap();
    let staged = transaction.execute(FILES_SQL, &[]).await.unwrap();
    assert_eq!(
        staged.rows()[0].get::<String>("path").unwrap(),
        "/target/source/note.txt"
    );

    let committed = lix.execute(FILES_SQL, &[]).await.unwrap();
    assert_eq!(
        committed.rows()[0].values(),
        initial.rows.rows()[0].values()
    );
    expect_no_event(&mut existing).await;
    let mut registered_during_transaction = lix.observe(FILES_SQL, &[]).unwrap();
    let fresh_initial = next_event(&mut registered_during_transaction).await;
    assert_eq!(
        fresh_initial.rows.rows()[0].values(),
        initial.rows.rows()[0].values()
    );

    transaction.commit().await.unwrap();
    for events in [&mut existing, &mut registered_during_transaction] {
        let moved = next_event(events).await;
        assert_eq!(moved.rows.len(), 1);
        assert_eq!(moved.rows.rows()[0].values(), staged.rows()[0].values());
        expect_no_event(events).await;
        events.close();
    }
    lix.close().await.unwrap();
}

#[tokio::test]
async fn public_transaction_rollback_publishes_no_partial_changes_and_observer_recovers() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ('/one.txt', CAST('one' AS BYTEA)), ('/two.txt', CAST('two' AS BYTEA))",
        &[],
    )
    .await
    .unwrap();
    let mut events = lix.observe(FILES_SQL, &[]).unwrap();
    let initial = next_event(&mut events).await;
    let mut transaction = lix.begin_transaction().await.unwrap();
    transaction
        .execute("DELETE FROM lix_file WHERE path = '/one.txt'", &[])
        .await
        .unwrap();
    transaction
        .execute(
            "UPDATE lix_file SET path = '/moved/two.txt' WHERE path = '/two.txt'",
            &[],
        )
        .await
        .unwrap();
    expect_no_event(&mut events).await;
    transaction.rollback().await.unwrap();
    let committed = lix.execute(FILES_SQL, &[]).await.unwrap();
    assert_eq!(committed.rows().len(), 2);
    for (actual, expected) in committed.rows().iter().zip(initial.rows.rows()) {
        assert_eq!(actual.values(), expected.values());
    }
    expect_no_event(&mut events).await;
    lix.execute("DELETE FROM lix_file WHERE path = '/one.txt'", &[])
        .await
        .unwrap();
    let changed = next_event(&mut events).await;
    assert_eq!(changed.rows.len(), 1);
    assert_eq!(
        changed.rows.rows()[0].get::<String>("path").unwrap(),
        "/two.txt"
    );
    events.close();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn public_transaction_and_parent_writes_have_independent_overlays() {
    let lix = open_lix().await.unwrap();
    let mut first = lix.begin_transaction().await.unwrap();
    assert!(
        lix.clone().begin_transaction().await.is_err(),
        "aliases share one explicit transaction slot"
    );
    let other = lix.open_another_session().await.unwrap();
    let mut second = other.begin_transaction().await.unwrap();
    first
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('first', 'first')",
            &[],
        )
        .await
        .unwrap();
    second
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('second', 'second')",
            &[],
        )
        .await
        .unwrap();
    assert!(
        second
            .execute("SELECT key FROM lix_key_value WHERE key = 'first'", &[])
            .await
            .unwrap()
            .rows()
            .is_empty()
    );
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('parent', 'parent')",
        &[],
    )
    .await
    .unwrap();
    first.commit().await.unwrap();
    second.commit().await.unwrap();
    let result = lix
        .execute(
            "SELECT key FROM lix_key_value WHERE key IN ('first', 'second', 'parent') ORDER BY key",
            &[],
        )
        .await
        .unwrap();
    let keys: Vec<String> = result
        .rows()
        .iter()
        .map(|row| row.get("key").unwrap())
        .collect();
    assert_eq!(keys, ["first", "parent", "second"]);
    lix.begin_transaction()
        .await
        .unwrap()
        .rollback()
        .await
        .unwrap();
    other.close().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn public_transaction_retains_origin_branch_and_account_when_parent_switches() {
    const AUTHOR: &str = "01920000-0000-7000-8000-000000000691";
    let root = open_lix().await.unwrap();
    root.ensure_account(AUTHOR, "Transaction author", "human")
        .await
        .unwrap();
    let lix = root
        .open_another_session()
        .with_account(AUTHOR)
        .await
        .unwrap();
    let origin = lix.active_branch_id().await.unwrap();
    let other = lix
        .create_branch(CreateBranchOptions {
            id: None,
            name: "Other branch".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let mut transaction = lix.begin_transaction().await.unwrap();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: other.id.clone(),
    })
    .await
    .unwrap();
    let context = transaction
        .execute(
            "SELECT lix_active_branch_id() AS branch_id, lix_active_account_id() AS account_id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        context.rows()[0].get::<String>("branch_id").unwrap(),
        origin
    );
    assert_eq!(
        context.rows()[0].get::<String>("account_id").unwrap(),
        AUTHOR
    );
    transaction
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('origin-only', 'value')",
            &[],
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(lix.active_branch_id().await.unwrap(), other.id);
    assert!(
        lix.execute(
            "SELECT key FROM lix_key_value WHERE key = 'origin-only'",
            &[]
        )
        .await
        .unwrap()
        .rows()
        .is_empty()
    );
    lix.switch_branch(SwitchBranchOptions { branch_id: origin })
        .await
        .unwrap();
    assert_eq!(
        lix.execute(
            "SELECT key FROM lix_key_value WHERE key = 'origin-only'",
            &[]
        )
        .await
        .unwrap()
        .rows()
        .len(),
        1
    );
    let attribution = lix
        .execute(
            "SELECT account_id FROM lix_change WHERE schema_key = 'lix_key_value'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        attribution.rows().last().unwrap().values(),
        &[Value::Text(AUTHOR.to_owned())]
    );
    lix.close().await.unwrap();
    root.close().await.unwrap();
}

#[tokio::test]
async fn public_transaction_blocks_parent_close_until_commit_rollback_or_drop() {
    for completion in ["commit", "rollback", "drop"] {
        let lix = open_lix().await.unwrap();
        let alias = lix.clone();
        let mut transaction = lix.begin_transaction().await.unwrap();
        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('pending', 'value')",
                &[],
            )
            .await
            .unwrap();
        alias
            .close()
            .await
            .expect_err("a parent alias must not close a live transaction");
        lix.execute("SELECT 1", &[])
            .await
            .expect("rejected close must leave parent usable");
        match completion {
            "commit" => transaction.commit().await.unwrap(),
            "rollback" => transaction.rollback().await.unwrap(),
            _ => drop(transaction),
        }
        let committed = lix
            .execute("SELECT key FROM lix_key_value WHERE key = 'pending'", &[])
            .await
            .unwrap();
        assert_eq!(committed.rows().len(), usize::from(completion == "commit"));
        lix.begin_transaction()
            .await
            .unwrap()
            .rollback()
            .await
            .unwrap();
        alias.close().await.unwrap();
        lix.execute("SELECT 1", &[])
            .await
            .expect_err("parent aliases must share close state");
    }
}
