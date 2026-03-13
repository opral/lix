mod support;

use lix_engine::{BootAccount, Value};
use support::simulation_test::SimulationBootArgs;

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
    }
}

async fn read_active_account_rows(
    engine: &support::simulation_test::SimulationEngine,
) -> Vec<(String, String)> {
    let result = engine
        .execute(
            "SELECT id, account_id FROM lix_active_account ORDER BY account_id",
            &[],
        )
        .await
        .unwrap();

    result.statements[0]
        .rows
        .iter()
        .map(|row| {
            let id = match &row[0] {
                Value::Text(value) => value.clone(),
                other => panic!("expected text id, got {other:?}"),
            };
            let account_id = match &row[1] {
                Value::Text(value) => value.clone(),
                other => panic!("expected text account_id, got {other:?}"),
            };
            (id, account_id)
        })
        .collect()
}

simulation_test!(
    active_account_view_select_reads_boot_account,
    simulations = [sqlite, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(Some(SimulationBootArgs {
                active_account: Some(BootAccount {
                    id: "acct-boot".to_string(),
                    name: "Boot Account".to_string(),
                }),
                ..SimulationBootArgs::default()
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let rows = read_active_account_rows(&engine).await;
        assert_eq!(
            rows,
            vec![("acct-boot".to_string(), "acct-boot".to_string())]
        );
    }
);

simulation_test!(
    active_account_view_insert_creates_untracked_row,
    simulations = [sqlite, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_active_account (account_id) VALUES ('acct-insert')",
                &[],
            )
            .await
            .unwrap();

        let rows = read_active_account_rows(&engine).await;
        assert_eq!(
            rows,
            vec![("acct-insert".to_string(), "acct-insert".to_string())]
        );

        let stored = engine
            .execute(
                "SELECT entity_id, snapshot_content \
             FROM lix_internal_live_untracked_v1 \
             WHERE schema_key = 'lix_active_account' \
               AND file_id = 'lix' \
               AND version_id = 'global'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(stored.statements[0].rows.len(), 1);
        assert_text(&stored.statements[0].rows[0][0], "acct-insert");
    }
);

simulation_test!(
    active_account_view_delete_removes_matching_row,
    simulations = [sqlite, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(Some(SimulationBootArgs {
                active_account: Some(BootAccount {
                    id: "acct-delete".to_string(),
                    name: "Delete Me".to_string(),
                }),
                ..SimulationBootArgs::default()
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "DELETE FROM lix_active_account WHERE account_id = 'acct-delete'",
                &[],
            )
            .await
            .unwrap();

        let rows = read_active_account_rows(&engine).await;
        assert!(rows.is_empty());
    }
);

simulation_test!(
    active_account_view_delete_supports_or_selector,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(Some(SimulationBootArgs {
                active_account: Some(BootAccount {
                    id: "acct-delete-or".to_string(),
                    name: "Delete Me".to_string(),
                }),
                ..SimulationBootArgs::default()
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "DELETE FROM lix_active_account \
                 WHERE account_id = 'acct-delete-or' OR account_id = 'missing-account'",
                &[],
            )
            .await
            .unwrap();

        let rows = read_active_account_rows(&engine).await;
        assert!(rows.is_empty());
    }
);
