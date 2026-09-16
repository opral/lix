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
    assert_eq!(
        second.commit().await.unwrap_err().code,
        lix::LixError::CODE_TRANSACTION_CONFLICT
    );
    // Reopen and revalidate the read on the now-current snapshot before replaying.
    let mut retry = other.begin_transaction().await.unwrap();
    assert_eq!(
        retry
            .execute("SELECT key FROM lix_key_value WHERE key = 'first'", &[])
            .await
            .unwrap()
            .rows()
            .len(),
        1
    );
    assert!(
        retry
            .execute("SELECT key FROM lix_key_value WHERE key = 'second'", &[])
            .await
            .unwrap()
            .rows()
            .is_empty()
    );
    retry
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('second', 'second')",
            &[],
        )
        .await
        .unwrap();
    retry.commit().await.unwrap();
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
            "commit" => {
                transaction.commit().await.unwrap();
            }
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

#[tokio::test]
async fn explicit_transaction_receipt_describes_the_durable_transition() {
    let lix = open_lix().await.unwrap();
    let seed = lix
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/receipt.txt', CAST('A' AS BYTEA))",
            &[],
        )
        .await
        .unwrap();
    let before = seed.commit().unwrap().after().to_owned();
    let mut tx = lix.begin_transaction().await.unwrap();
    let first = tx
        .execute(
            "UPDATE lix_file SET content = CAST('B' AS BYTEA) WHERE path = '/receipt.txt' RETURNING content",
            &[],
        )
        .await
        .unwrap();
    assert!(first.commit().is_none());
    let second = tx
        .execute(
            "UPDATE lix_file SET content = CAST('A' AS BYTEA) WHERE path = '/receipt.txt' RETURNING content",
            &[],
        )
        .await
        .unwrap();
    assert!(second.commit().is_none());
    let receipt = tx.commit().await.unwrap();
    let span = receipt.commit.unwrap();
    assert_eq!(span.before(), before);
    let diff = lix
        .execute(
            "SELECT id FROM lix_diff('lix_file', $1, $2)",
            &[
                Value::Text(span.before().to_owned()),
                Value::Text(span.after().to_owned()),
            ],
        )
        .await
        .unwrap();
    assert!(diff.rows().is_empty());
}

#[tokio::test]
async fn explicit_read_guard_fences_writes_and_retry_accepts_unrelated_changes() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('guard-target', 'A')",
        &[],
    )
    .await
    .unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "SELECT value FROM lix_key_value WHERE key = 'guard-target'",
        &[],
    )
    .await
    .unwrap();
    // This write is disjoint from the guarded row and from the eventual insert.
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('unrelated', 'X')",
        &[],
    )
    .await
    .unwrap();
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('guard-result', 'B')",
        &[],
    )
    .await
    .unwrap();
    let error = tx.commit().await.unwrap_err();
    assert_eq!(error.code, lix::LixError::CODE_TRANSACTION_CONFLICT);
    assert!(
        lix.execute(
            "SELECT * FROM lix_key_value WHERE key = 'guard-result'",
            &[]
        )
        .await
        .unwrap()
        .rows()
        .is_empty()
    );
    let mut retry = lix.begin_transaction().await.unwrap();
    let guarded = retry
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'guard-target'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        guarded.rows()[0].get::<serde_json::Value>("value").unwrap(),
        "A"
    );
    retry
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('guard-result', 'B')",
            &[],
        )
        .await
        .unwrap();
    assert!(retry.commit().await.unwrap().commit.is_some());
}

#[tokio::test]
async fn explicit_read_guard_detects_phantoms_but_read_only_commit_remains_valid() {
    let lix = open_lix().await.unwrap();
    let mut guarded = lix.begin_transaction().await.unwrap();
    assert!(
        guarded
            .execute("SELECT id FROM lix_file WHERE path = '/missing.txt'", &[])
            .await
            .unwrap()
            .rows()
            .is_empty()
    );
    let read_only_session = lix.open_another_session().await.unwrap();
    let mut read_only = read_only_session.begin_transaction().await.unwrap();
    read_only
        .execute("SELECT id FROM lix_file WHERE path = '/missing.txt'", &[])
        .await
        .unwrap();
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ('/missing.txt', CAST('concurrent' AS BYTEA))",
        &[],
    )
    .await
    .unwrap();
    guarded
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('phantom-side-effect', 'never')",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        guarded.commit().await.unwrap_err().code,
        lix::LixError::CODE_TRANSACTION_CONFLICT
    );
    assert_eq!(read_only.commit().await.unwrap().commit, None);
    let mut retry = lix.begin_transaction().await.unwrap();
    assert_eq!(
        retry
            .execute("SELECT id FROM lix_file WHERE path = '/missing.txt'", &[])
            .await
            .unwrap()
            .rows()
            .len(),
        1
    );
    retry.rollback().await.unwrap();
    assert!(
        lix.execute(
            "SELECT key FROM lix_key_value WHERE key = 'phantom-side-effect'",
            &[]
        )
        .await
        .unwrap()
        .rows()
        .is_empty()
    );
}

#[tokio::test]
async fn rejected_checkpoint_preserves_staged_write_and_commit_receipt() {
    let lix = open_lix().await.unwrap();
    let seed = lix.execute("INSERT INTO lix_file (path, content) VALUES ('/checkpoint.txt', CAST('before' AS BYTEA))", &[]).await.unwrap();
    let before = seed.commit().unwrap().after().to_owned();
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "UPDATE lix_file SET content = CAST('after' AS BYTEA) WHERE path = '/checkpoint.txt'",
        &[],
    )
    .await
    .unwrap();
    let checkpoint_error = tx
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .unwrap_err();
    assert_eq!(checkpoint_error.code, "LIX_INVALID_TRANSACTION_STATE");
    let receipt = tx.commit().await.unwrap().commit.unwrap();
    assert_eq!(receipt.before(), before);
    assert_ne!(receipt.before(), receipt.after());
    let content = lix
        .execute(
            "SELECT content FROM lix_file WHERE path = '/checkpoint.txt'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        content.rows()[0].get::<Vec<u8>>("content").unwrap(),
        b"after"
    );
    let mut checkpoint_only = lix.begin_transaction().await.unwrap();
    checkpoint_only
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .unwrap();
    assert!(checkpoint_only.commit().await.unwrap().commit.is_some());
    let mut no_op = lix.begin_transaction().await.unwrap();
    no_op
        .execute(
            "UPDATE lix_file SET path = '/never.txt' WHERE path = '/absent.txt'",
            &[],
        )
        .await
        .unwrap();
    let span = no_op.commit().await.unwrap().commit.unwrap();
    assert_eq!(span.before(), span.after());
}

#[tokio::test]
async fn explicit_guard_rechecks_target_edits_before_publishing_side_effects() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('guard-target-edit', 'A')",
        &[],
    )
    .await
    .unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "SELECT value FROM lix_key_value WHERE key = 'guard-target-edit'",
        &[],
    )
    .await
    .unwrap();
    lix.execute(
        "UPDATE lix_key_value SET value = 'C' WHERE key = 'guard-target-edit'",
        &[],
    )
    .await
    .unwrap();
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('guard-side-effect', 'B')",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        tx.commit().await.unwrap_err().code,
        lix::LixError::CODE_TRANSACTION_CONFLICT
    );
    let mut retry = lix.begin_transaction().await.unwrap();
    assert_eq!(
        retry
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'guard-target-edit'",
                &[]
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<serde_json::Value>("value")
            .unwrap(),
        "C"
    );
    retry.rollback().await.unwrap();
    assert!(
        lix.execute(
            "SELECT key FROM lix_key_value WHERE key = 'guard-side-effect'",
            &[]
        )
        .await
        .unwrap()
        .rows()
        .is_empty()
    );
}

#[tokio::test]
async fn explicit_first_prepared_write_returns_a_receipt() {
    let lix = open_lix().await.unwrap();
    // Warm the public insert shape before opening independent transaction handles.
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('warm-receipt', 'one')",
        &[],
    )
    .await
    .unwrap();
    lix.execute_batch(&[
        lix::ExecuteBatchStatement { label: None, sql: "INSERT INTO lix_key_value (key, value) VALUES ('batch-warm-receipt', 'one')".into(), params: vec![] },
        lix::ExecuteBatchStatement { label: None, sql: "UPDATE lix_key_value SET value = 'two' WHERE key = 'batch-warm-receipt' RETURNING key".into(), params: vec![] },
    ]).await.unwrap();
    for (sql, params) in [
        (
            "INSERT INTO lix_key_value (key, value) VALUES ('prepared-receipt', 'one')",
            vec![],
        ),
        (
            "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
            vec![
                Value::Text("parameter-receipt".into()),
                Value::Text("one".into()),
            ],
        ),
    ] {
        let before = lix
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        let mut tx = lix.begin_transaction().await.unwrap();
        let staged = tx.execute(sql, &params).await.unwrap();
        assert_eq!(staged.rows_affected(), 1);
        assert!(staged.commit().is_none());
        let span = tx
            .commit()
            .await
            .unwrap()
            .commit
            .expect("even the first prepared write must publish its receipt");
        assert_eq!(span.before(), before);
        assert_ne!(span.before(), span.after());
        let after = lix
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        assert_eq!(span.after(), after);
    }
}

#[tokio::test]
async fn concurrent_explicit_commit_receipts_contain_only_their_own_file() {
    let lix = open_lix().await.unwrap();
    let mut left = lix.begin_transaction().await.unwrap();
    let right_session = lix.open_another_session().await.unwrap();
    let mut right = right_session.begin_transaction().await.unwrap();
    left.execute(
        "INSERT INTO lix_file (path, content) VALUES ('/receipt-left.txt', CAST('left' AS BYTEA))",
        &[],
    )
    .await
    .unwrap();
    right.execute("INSERT INTO lix_file (path, content) VALUES ('/receipt-right.txt', CAST('right' AS BYTEA))", &[]).await.unwrap();
    let (left, right) = tokio::join!(left.commit(), right.commit());
    for (receipt, path) in [
        (left.unwrap(), "/receipt-left.txt"),
        (right.unwrap(), "/receipt-right.txt"),
    ] {
        let span = receipt.commit.unwrap();
        let diff = lix
            .execute(
                "SELECT to_path FROM lix_diff('lix_file', $1, $2)",
                &[
                    Value::Text(span.before().into()),
                    Value::Text(span.after().into()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(diff.rows().len(), 1);
        assert_eq!(diff.rows()[0].get::<String>("to_path").unwrap(), path);
    }
}

#[tokio::test]
async fn repeated_updates_return_the_published_transaction_commit_id() {
    let assert_rows_eq = |result: lix::ExecuteResult, expected: Vec<Vec<Value>>| {
        let actual = result
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    };
    let session = open_lix().await.unwrap();
    register_commit_counter(&session).await;

    for explicit in [false, true] {
        let inserted = session
            .execute("INSERT INTO commit_counter (id, n) VALUES ('a', 0)", &[])
            .await
            .unwrap();
        let previous_commit = inserted.commit().unwrap().after().to_string();
        let sql = "UPDATE commit_counter SET n = n + 1 WHERE id = 'a' \
                       RETURNING OLD.n AS before_n, NEW.n AS after_n, \
                       OLD.lixcol_commit_id AS before_commit, \
                       NEW.lixcol_commit_id AS after_commit, lixcol_commit_id";
        let (results, commit) = if explicit {
            let mut transaction = session.begin_transaction().await.unwrap();
            let first = transaction.execute(sql, &[]).await.unwrap();
            let second = transaction.execute(sql, &[]).await.unwrap();
            let receipt = transaction.commit().await.unwrap();
            (vec![first, second], receipt.commit.unwrap())
        } else {
            let statement = || lix::ExecuteBatchStatement {
                sql: sql.into(),
                params: vec![],
                label: None,
            };
            let batch = session
                .execute_batch(&[statement(), statement()])
                .await
                .unwrap();
            (batch.results, batch.commit.unwrap())
        };
        assert_eq!(commit.before(), previous_commit);
        assert_ne!(commit.after(), previous_commit);
        for (index, result) in results.into_iter().enumerate() {
            assert_rows_eq(
                result,
                vec![vec![
                    Value::Integer(index as i64),
                    Value::Integer(index as i64 + 1),
                    Value::Text(if index == 0 {
                        previous_commit.clone()
                    } else {
                        commit.after().into()
                    }),
                    Value::Text(commit.after().into()),
                    Value::Text(commit.after().into()),
                ]],
            );
        }
        let head = session
            .execute("SELECT lix_active_branch_commit_id()", &[])
            .await
            .unwrap();
        assert_rows_eq(head, vec![vec![Value::Text(commit.after().into())]]);
        let persisted = session
            .execute(
                "SELECT n, lixcol_commit_id FROM commit_counter WHERE id = 'a'",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(
            persisted,
            vec![vec![Value::Integer(2), Value::Text(commit.after().into())]],
        );

        // A deletion returns the old row's provenance, not its own commit.
        let deleted = session
            .execute(
                "DELETE FROM commit_counter WHERE id = 'a' \
                 RETURNING OLD.lixcol_commit_id, NEW.lixcol_commit_id, lixcol_commit_id",
                &[],
            )
            .await
            .unwrap();
        assert_ne!(deleted.commit().unwrap().after(), commit.after());
        assert_rows_eq(
            deleted,
            vec![vec![
                Value::Text(commit.after().into()),
                Value::Null,
                Value::Text(commit.after().into()),
            ]],
        );
    }
}

async fn register_commit_counter(session: &lix::Lix) {
    session.execute(
        r#"INSERT INTO lix_registered_schema (value) VALUES (CAST('{"$schema":"https://lix.dev/schema-v1.json","key":"commit_counter","columns":[{"name":"id","type":"text","nullable":false},{"name":"n","type":"int8","nullable":false}],"primary_key":["id"]}' AS JSONB))"#,
        &[],
    ).await.unwrap();
}

#[tokio::test]
async fn reserved_commit_survives_statement_rollback_and_row_replacement() {
    for initially_present in [false, true] {
        let session = open_lix().await.unwrap();
        register_commit_counter(&session).await;
        if initially_present {
            session
                .execute("INSERT INTO commit_counter (id, n) VALUES ('a', 0)", &[])
                .await
                .unwrap();
        }
        let mut tx = session.begin_transaction().await.unwrap();
        let inserted_commit = if initially_present {
            None
        } else {
            let inserted = tx
                .execute(
                    "INSERT INTO commit_counter (id, n) VALUES ('a', 0) RETURNING lixcol_commit_id",
                    &[],
                )
                .await
                .unwrap();
            Some(
                inserted.rows()[0]
                    .get::<String>("lixcol_commit_id")
                    .unwrap(),
            )
        };
        let first = tx
            .execute(
                "UPDATE commit_counter SET n = 1 WHERE id = 'a' RETURNING lixcol_commit_id",
                &[],
            )
            .await
            .unwrap();
        let reserved = first.rows()[0].get::<String>("lixcol_commit_id").unwrap();
        if let Some(inserted_commit) = inserted_commit {
            assert_eq!(inserted_commit, reserved);
        }
        let error = tx.execute("UPDATE commit_counter SET n = 100 WHERE id = 'a' RETURNING lixcol_commit_id, CAST(id AS BIGINT)", &[]).await.unwrap_err();
        assert_eq!(error.code, "LIX_TYPE_MISMATCH");
        let after_error = tx
            .execute(
                "SELECT n, lixcol_commit_id FROM commit_counter WHERE id = 'a'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(after_error.rows()[0].get::<i64>("n").unwrap(), 1);
        assert_eq!(
            after_error.rows()[0]
                .get::<String>("lixcol_commit_id")
                .unwrap(),
            reserved
        );
        let deleted = tx
            .execute(
                "DELETE FROM commit_counter WHERE id = 'a' RETURNING lixcol_commit_id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            deleted.rows()[0].get::<String>("lixcol_commit_id").unwrap(),
            reserved
        );
        let inserted = tx
            .execute(
                "INSERT INTO commit_counter (id, n) VALUES ('a', 2) RETURNING lixcol_commit_id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            inserted.rows()[0]
                .get::<String>("lixcol_commit_id")
                .unwrap(),
            reserved
        );
        let receipt = tx.commit().await.unwrap();
        assert_eq!(receipt.commit.unwrap().after(), reserved);
        let persisted = session
            .execute(
                "SELECT n, lixcol_commit_id FROM commit_counter WHERE id = 'a'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(persisted.rows()[0].get::<i64>("n").unwrap(), 2);
        assert_eq!(
            persisted.rows()[0]
                .get::<String>("lixcol_commit_id")
                .unwrap(),
            reserved
        );
    }
}

#[tokio::test]
async fn reinserted_new_row_cannot_overwrite_a_concurrent_insert() {
    let session = open_lix().await.unwrap();
    register_commit_counter(&session).await;
    let mut tx = session.begin_transaction().await.unwrap();
    tx.execute("INSERT INTO commit_counter (id, n) VALUES ('a', 0)", &[])
        .await
        .unwrap();
    tx.execute("DELETE FROM commit_counter WHERE id = 'a'", &[])
        .await
        .unwrap();
    tx.execute("INSERT INTO commit_counter (id, n) VALUES ('a', 1)", &[])
        .await
        .unwrap();
    let concurrent = session
        .execute("INSERT INTO commit_counter (id, n) VALUES ('a', 2)", &[])
        .await
        .unwrap();
    assert_eq!(
        tx.commit().await.unwrap_err().code,
        lix::LixError::CODE_TRANSACTION_CONFLICT
    );
    let persisted = session
        .execute(
            "SELECT n, lixcol_commit_id FROM commit_counter WHERE id = 'a'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(persisted.rows()[0].get::<i64>("n").unwrap(), 2);
    assert_eq!(
        persisted.rows()[0]
            .get::<String>("lixcol_commit_id")
            .unwrap(),
        concurrent.commit().unwrap().after()
    );
}
