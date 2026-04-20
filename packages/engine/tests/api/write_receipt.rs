use std::time::Duration;

use lix_engine::{ObserveQuery, Value, WriteReceipt};

fn insert_key_value_sql(key: &str, value: &str) -> String {
    format!("INSERT INTO lix_key_value (key, value) VALUES ('{key}', '{value}')")
}

fn first_text(rows: &lix_engine::QueryResult) -> String {
    match &rows.rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected first query cell to be text, got {other:?}"),
    }
}

fn latest_canonical_change(receipt: &WriteReceipt) -> (&str, &str) {
    let canonical = receipt
        .canonical_commit
        .as_ref()
        .expect("write receipt should include canonical commit metadata");
    let latest = canonical
        .updated_version_refs
        .iter()
        .max_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.change_id.cmp(&right.change_id))
        })
        .expect("canonical receipt should include updated version refs");
    (&latest.change_id, &latest.created_at)
}

fn assert_write_receipt_shape(receipt: &WriteReceipt) {
    assert!(
        receipt.state_commit_sequence.is_some(),
        "write receipt should include a state commit sequence"
    );
    let canonical = receipt
        .canonical_commit
        .as_ref()
        .expect("write receipt should include canonical commit metadata");
    assert!(
        !canonical.commit_id.is_empty(),
        "canonical commit id should be populated"
    );
    assert!(
        !canonical.updated_version_refs.is_empty(),
        "canonical receipt should include updated version refs"
    );
}

simulation_test!(
    execute_returns_write_receipt_without_active_listeners,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let result = engine
            .execute(
                &insert_key_value_sql("write-receipt-no-listener", "v0"),
                &[],
            )
            .await
            .expect("insert should succeed");

        let receipt = result
            .write_receipt
            .expect("write execution should return a receipt");
        assert_write_receipt_shape(&receipt);

        tokio::time::timeout(
            Duration::from_secs(1),
            engine.wait_for_write_receipt(&receipt),
        )
        .await
        .expect("wait_for_write_receipt should not time out")
        .expect("wait_for_write_receipt should succeed");
    }
);

simulation_test!(
    observe_wait_for_write_receipt_returns_matching_event,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let mut observed = engine
            .observe(ObserveQuery::new(
                "SELECT value FROM lix_key_value WHERE key = 'write-receipt-observe' LIMIT 1",
                vec![],
            ))
            .expect("observe should build");

        let initial = tokio::time::timeout(Duration::from_secs(1), observed.next())
            .await
            .expect("initial observe event should not time out")
            .expect("initial observe event should succeed")
            .expect("initial observe event should exist");
        assert!(
            initial.rows.rows.is_empty(),
            "initial observe query should be empty before the insert"
        );

        let result = engine
            .execute(&insert_key_value_sql("write-receipt-observe", "v0"), &[])
            .await
            .expect("insert should succeed");
        let receipt = result
            .write_receipt
            .expect("write execution should return a receipt");
        assert_write_receipt_shape(&receipt);

        let event = tokio::time::timeout(
            Duration::from_secs(1),
            observed.wait_for_write_receipt(&receipt),
        )
        .await
        .expect("wait_for_write_receipt should not time out")
        .expect("wait_for_write_receipt should succeed")
        .expect("observe stream should emit a matching event");

        let frontier = event
            .frontier
            .as_ref()
            .expect("matching observe event should include a frontier");
        let (change_id, created_at) = latest_canonical_change(&receipt);
        assert_eq!(frontier.change_id, change_id);
        assert_eq!(frontier.created_at, created_at);
        assert_eq!(first_text(&event.rows), "v0");
    }
);

simulation_test!(
    transaction_commit_returns_write_receipt,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let mut tx = engine
            .begin_transaction_with_options(Default::default())
            .await
            .expect("begin transaction should succeed");
        tx.execute(
            &insert_key_value_sql("write-receipt-transaction", "v0"),
            &[],
        )
        .await
        .expect("transactional insert should succeed");

        let receipt = tx
            .commit()
            .await
            .expect("transaction commit should succeed")
            .expect("transaction commit should return a receipt");
        assert_write_receipt_shape(&receipt);
    }
);
