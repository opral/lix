use lix::{CreateBranchOptions, LixError, Value};

const GRANT_ID: &str = "01940000-0000-7000-8000-000000000001";
const DRAFT_ID: &str = "01940000-0000-7000-8000-000000000002";

simulation_test!(permission_grants_are_tracked_global_and_visible_from_every_branch, |sim| async move {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(
        engine.open_session().await.expect("main session should open"),
        &engine,
    );

    main.execute(
        "INSERT INTO lix_permission_grant \
         (id, principal_type, principal_id, access_level, resource_type, lixcol_global) \
         VALUES ($1, 'account', $2, 'manager', 'repository', true)",
        &[
            Value::Text(GRANT_ID.to_string()),
            Value::Text(lix::SYSTEM_ACCOUNT_ID.to_string()),
        ],
    )
    .await
    .expect("tracked global permission grant should insert");

    main.create_branch(CreateBranchOptions {
        id: Some(DRAFT_ID.to_string()),
        name: "Permission visibility draft".to_string(),
        from_commit_id: None,
    })
    .await
    .expect("draft branch should be created");
    let draft = sim.wrap_session(
        engine
            .open_session_at(DRAFT_ID)
            .await
            .expect("draft session should open"),
        &engine,
    );

    for session in [&main, &draft] {
        let result = session
            .execute(
                "SELECT access_level, lixcol_global, lixcol_untracked \
                 FROM lix_permission_grant WHERE id = $1",
                &[Value::Text(GRANT_ID.to_string())],
            )
            .await
            .expect("permission grant should be inherited by every branch");
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Text("manager".to_string()),
                Value::Boolean(true),
                Value::Boolean(false),
            ]
        );
    }

    let physical = main
        .execute(
            "SELECT lixcol_branch_id FROM lix_permission_grant_by_branch \
             WHERE id = $1 AND lixcol_branch_id = $2 AND lixcol_global = true",
            &[
                Value::Text(GRANT_ID.to_string()),
                Value::Text(lix::GLOBAL_BRANCH_ID.to_string()),
            ],
        )
        .await
        .expect("permission grant home branch should be readable");
    assert_eq!(
        physical.rows()[0].values(),
        &[Value::Text(lix::GLOBAL_BRANCH_ID.to_string())]
    );
});

simulation_test!(permission_grants_reject_non_global_untracked_and_malformed_rows, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("main session should open"),
        &engine,
    );

    let local = session
        .execute(
            "INSERT INTO lix_permission_grant \
             (id, principal_type, principal_id, access_level, resource_type) \
             VALUES ('01940000-0000-7000-8000-000000000010', 'account', $1, 'viewer', 'repository')",
            &[Value::Text(lix::SYSTEM_ACCOUNT_ID.to_string())],
        )
        .await
        .expect_err("branch-local permission grant must fail");
    assert_eq!(local.code, LixError::CODE_INVALID_PARAM);
    assert!(local.message.contains("tracked global rows"));

    let untracked = session
        .execute(
            "INSERT INTO lix_permission_grant \
             (id, principal_type, principal_id, access_level, resource_type, lixcol_global, lixcol_untracked) \
             VALUES ('01940000-0000-7000-8000-000000000011', 'account', $1, 'viewer', 'repository', true, true)",
            &[Value::Text(lix::SYSTEM_ACCOUNT_ID.to_string())],
        )
        .await
        .expect_err("untracked permission grant must fail");
    assert_eq!(untracked.code, LixError::CODE_INVALID_PARAM);
    assert!(untracked.message.contains("tracked global rows"));

    let malformed = session
        .execute(
            "INSERT INTO lix_permission_grant \
             (id, principal_type, access_level, resource_type, lixcol_global) \
             VALUES ('01940000-0000-7000-8000-000000000012', 'anonymous', 'viewer', 'file', true)",
            &[],
        )
        .await
        .expect_err("file grant without file_id must fail");
    assert_eq!(malformed.code, LixError::CODE_INVALID_PARAM);
    assert!(malformed.message.contains("resource columns"));
});

simulation_test!(permission_grants_model_repository_directory_file_table_and_row, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("main session should open"),
        &engine,
    );

    session
        .execute(
            "INSERT INTO lix_permission_grant \
             (id, principal_type, principal_id, access_level, resource_type, directory_id, file_id, schema_key, row_pk, lixcol_global) VALUES \
             ('01940000-0000-7000-8000-000000000020', 'account', $1, 'manager', 'repository', NULL, NULL, NULL, NULL, true), \
             ('01940000-0000-7000-8000-000000000021', 'group', 'sales', 'contributor', 'directory', '01940000-0000-7000-8000-000000000030', NULL, NULL, NULL, true), \
             ('01940000-0000-7000-8000-000000000022', 'anonymous', NULL, 'viewer', 'file', NULL, '01940000-0000-7000-8000-000000000031', NULL, NULL, true), \
             ('01940000-0000-7000-8000-000000000023', 'group', 'agency', 'viewer', 'table', NULL, '01940000-0000-7000-8000-000000000031', 'lead', NULL, true), \
             ('01940000-0000-7000-8000-000000000024', 'account', $1, 'commenter', 'row', NULL, '01940000-0000-7000-8000-000000000031', 'lead', CAST('[\"lead-123\"]' AS JSONB), true)",
            &[Value::Text(lix::SYSTEM_ACCOUNT_ID.to_string())],
        )
        .await
        .expect("all canonical permission resource shapes should insert");

    let result = session
        .execute(
            "SELECT resource_type FROM lix_permission_grant ORDER BY resource_type",
            &[],
        )
        .await
        .expect("permission resource shapes should read");
    assert_eq!(result.len(), 5);
});
