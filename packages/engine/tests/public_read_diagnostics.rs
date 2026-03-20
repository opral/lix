mod support;

use lix_engine::Value;

simulation_test!(
    public_self_join_error_hides_lowered_sql,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine
            .initialize()
            .await
            .expect("engine init should succeed");
        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('a', '\"1\"')",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        let error = engine
            .execute("SELECT * FROM lix_key_value, lix_key_value", &[])
            .await
            .expect_err("self-join ambiguity should fail");

        assert!(
            error.description.contains("ambiguous column name:"),
            "unexpected error message: {}",
            error.description
        );
        assert!(
            !error.description.contains("lix_internal_"),
            "internal table names must not leak: {}",
            error.description
        );
        assert!(
            !error.description.contains("WITH target_versions"),
            "lowered SQL must not leak: {}",
            error.description
        );
        assert!(
            error.description.len() < 512,
            "error should be bounded, got {} chars: {}",
            error.description.len(),
            error.description
        );
    }
);

simulation_test!(
    public_exists_header_uses_public_expression_label,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine
            .initialize()
            .await
            .expect("engine init should succeed");
        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('a', '\"1\"')",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        let result = engine
            .execute(
                "SELECT EXISTS (SELECT 1 FROM lix_key_value WHERE key = 'a')",
                &[],
            )
            .await
            .expect("exists query should succeed");

        assert_eq!(result.statements.len(), 1);
        assert_eq!(
            result.statements[0].columns,
            vec!["EXISTS (SELECT 1 FROM lix_key_value WHERE key = 'a')".to_string()]
        );
        assert!(
            !result.statements[0].columns[0].contains("lix_internal_"),
            "internal SQL must not leak into headers: {}",
            result.statements[0].columns[0]
        );
        assert!(
            result.statements[0].columns[0].len() < 256,
            "header should be bounded, got {} chars: {}",
            result.statements[0].columns[0].len(),
            result.statements[0].columns[0]
        );
        assert_eq!(result.statements[0].rows.len(), 1);
        match &result.statements[0].rows[0][0] {
            Value::Integer(1) | Value::Boolean(true) => {}
            other => panic!("expected EXISTS truthy result, got {other:?}"),
        }
    }
);
