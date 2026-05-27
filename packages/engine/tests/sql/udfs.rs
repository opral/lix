simulation_test!(
    lix_active_branch_commit_id_returns_active_head,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('active-head', 'one')",
                &[],
            )
            .await
            .expect("tracked write should succeed");
        let expected = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head should load")
            .expect("head should exist");

        let result = session
            .execute("SELECT lix_active_branch_commit_id()", &[])
            .await
            .expect("active head UDF should execute");

        assert_eq!(
            result.rows()[0]
                .get::<String>("lix_active_branch_commit_id()")
                .unwrap(),
            expected
        );
    }
);
