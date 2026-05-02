use lix_engine::LixError;

simulation_test!(sql_missing_table_has_lix_error_code, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let error = session
        .execute("SELECT * FROM missing_table", &[])
        .await
        .expect_err("missing table should fail");

    assert_eq!(error.code, LixError::CODE_TABLE_NOT_FOUND);
    assert!(error.hint().is_some(), "expected discovery hint: {error}");
});

simulation_test!(sql_missing_column_has_lix_error_code, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let error = session
        .execute("SELECT missing_column FROM lix_file", &[])
        .await
        .expect_err("missing column should fail");

    assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);
});

simulation_test!(sql_question_mark_placeholder_has_hint, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let error = session
        .execute("SELECT * FROM lix_file WHERE id = ?", &[])
        .await
        .expect_err("question mark placeholders should fail");

    assert_eq!(error.code, LixError::CODE_BINDING_ERROR);
    assert!(
        error.hint().is_some_and(|hint| hint.contains("$1")),
        "expected placeholder hint: {error}"
    );
});
