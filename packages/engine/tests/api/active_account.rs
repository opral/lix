use crate::support;

use lix_engine::Value;
use support::simulation_test::SimulatedLixBootArgs;

fn first_text(result: &lix_engine::ExecuteResult) -> String {
    match &result.statements[0].rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected first result cell to be text, got {other:?}"),
    }
}

fn first_string_vec(result: &lix_engine::ExecuteResult) -> Vec<String> {
    serde_json::from_str(&first_text(result))
        .unwrap_or_else(|error| panic!("expected JSON array text, got parse error: {error}"))
}

simulation_test!(
    active_account_ids_function_defaults_to_empty_selection,
    simulations = [sqlite, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(Some(SimulatedLixBootArgs::default()))
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let result = engine
            .execute("SELECT lix_active_account_ids()", &[])
            .await
            .expect("active account ids query should succeed");
        assert_eq!(first_string_vec(&result), Vec::<String>::new());
    }
);

simulation_test!(
    set_active_account_ids_updates_runtime_function_and_workspace_metadata,
    simulations = [sqlite, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .set_active_account_ids(vec!["acct-first".to_string(), "acct-second".to_string()])
            .await
            .expect("set_active_account_ids should succeed");

        let result = engine
            .execute("SELECT lix_active_account_ids()", &[])
            .await
            .expect("active account ids query should succeed");
        assert_eq!(
            first_string_vec(&result),
            vec!["acct-first".to_string(), "acct-second".to_string()]
        );
        assert_eq!(
            engine
                .active_account_ids()
                .await
                .expect("public active_account_ids API should succeed"),
            vec!["acct-first".to_string(), "acct-second".to_string()]
        );
    }
);

simulation_test!(
    set_active_account_ids_can_clear_selection,
    simulations = [sqlite, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(Some(SimulatedLixBootArgs::default()))
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .set_active_account_ids(vec!["acct-clear".to_string()])
            .await
            .expect("seeding active account ids should succeed");
        engine
            .set_active_account_ids(Vec::new())
            .await
            .expect("clearing active account ids should succeed");

        let result = engine
            .execute("SELECT lix_active_account_ids()", &[])
            .await
            .expect("active account ids query should succeed");
        assert_eq!(first_string_vec(&result), Vec::<String>::new());
        assert_eq!(
            engine
                .active_account_ids()
                .await
                .expect("public active_account_ids API should succeed"),
            Vec::<String>::new()
        );
    }
);

simulation_test!(
    active_account_surface_is_not_publicly_queryable,
    simulations = [sqlite, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let error = engine
            .execute("SELECT account_id FROM lix_active_account", &[])
            .await
            .expect_err("removed active account surface should not be queryable");
        assert_eq!(error.code, "LIX_ERROR_SQL_UNKNOWN_TABLE");
        assert!(error.description.contains("lix_active_account"));
    }
);
