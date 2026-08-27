use lix::{ResultColumnType, RowRef, Value};
use serde_json::json;

simulation_test!(row_ref_constructor_and_default_diff_are_typed_and_canonical, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );
    let id = "01991b1d-6d8b-7000-8000-0000000000f1";

    session
        .execute(
            "INSERT INTO lix_file (id, path, content) VALUES ($1, '/row-ref.txt', CAST('hello' AS BYTEA))",
            &[Value::Text(id.into())],
        )
        .await
        .expect("file should insert before the first explicit checkpoint");

    let direct = session
        .execute("SELECT lix_row_ref('lix_file', $1) AS row_ref", &[Value::Text(id.into())])
        .await
        .expect("UUID file identity should construct");
    assert_eq!(direct.column_types(), &[ResultColumnType::RowRef]);
    let [Value::RowRef(direct_ref)] = direct.rows()[0].values() else {
        panic!("constructor must return the opaque RowRef value kind");
    };
    assert_eq!(
        direct.rows()[0].get::<RowRef>("row_ref").unwrap(),
        direct_ref.clone()
    );

    let diff = session
        .execute(
            "SELECT row_ref, id, diff_type, from_path, to_path, row_count \
             FROM lix_diff('lix_file') WHERE id = $1",
            &[Value::Text(id.into())],
        )
        .await
        .expect("one-argument diff should default from checkpoint (or root) to head");
    assert_eq!(
        diff.columns(),
        ["row_ref", "id", "diff_type", "from_path", "to_path", "row_count"]
    );
    assert_eq!(diff.column_types()[0], ResultColumnType::RowRef);
    assert_eq!(diff.rows().len(), 1);
    assert_eq!(diff.rows()[0].values()[0], Value::RowRef(direct_ref.clone()));
    assert_eq!(diff.rows()[0].values()[1], Value::Text(id.into()));
    assert_eq!(diff.rows()[0].values()[2], Value::Text("added".into()));
    assert!(
        diff.columns().iter().all(|column| !column.contains("row_pk")),
        "the public diff must not leak the JSON row-key representation"
    );

    let inserted = head(&engine, sim.main_branch_id()).await;
    session
        .execute("DELETE FROM lix_file WHERE id = $1", &[Value::Text(id.into())])
        .await
        .expect("file should delete");
    let deleted = head(&engine, sim.main_branch_id()).await;
    let removal = session
        .execute(
            "SELECT row_ref, id, diff_type, from_path, to_path \
             FROM lix_diff('lix_file', $1, $2) WHERE id = $3",
            &[
                Value::Text(inserted),
                Value::Text(deleted),
                Value::Text(id.into()),
            ],
        )
        .await
        .expect("removed file identity should remain typed and addressable");
    assert_eq!(removal.rows().len(), 1);
    assert_eq!(
        removal.rows()[0].values()[0],
        Value::RowRef(direct_ref.clone())
    );
    assert_eq!(removal.rows()[0].values()[1], Value::Text(id.into()));
    assert_eq!(
        removal.rows()[0].values()[2],
        Value::Text("removed".into())
    );
    assert_eq!(
        removal.rows()[0].values()[3],
        Value::Text("/row-ref.txt".into())
    );
    assert_eq!(removal.rows()[0].values()[4], Value::Null);

    for (sql, expected) in [
        ("SELECT lix_row_ref('missing_relation', 'x')", "does not exist"),
        ("SELECT lix_row_ref('lix_file', 'not-a-uuid')", "invalid primary key"),
        ("SELECT lix_row_ref('lix_file', NULL)", "non-null"),
        ("SELECT lix_row_ref('lix_file', 'a', 'b')", "requires 1 primary-key values"),
    ] {
        let error = session.execute(sql, &[]).await.expect_err(sql);
        assert!(error.to_string().contains(expected), "unexpected error: {error}");
    }
});

simulation_test!(composite_diff_exposes_typed_keys_once_for_every_diff_kind, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );
    let schema = json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "row_ref_composite_member",
        "columns": [
            { "name": "parent_id", "type": "text", "nullable": false },
            { "name": "key", "type": "int8", "nullable": false },
            { "name": "value", "type": "text", "nullable": false }
        ],
        "primary_key": ["parent_id", "key"]
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES ($1)",
            &[Value::Jsonb(schema.into())],
        )
        .await
        .expect("composite schema should register");
    let baseline = head(&engine, sim.main_branch_id()).await;

    session
        .execute(
            "INSERT INTO row_ref_composite_member (parent_id, key, value) VALUES ('parent', 7, 'one')",
            &[],
        )
        .await
        .expect("composite row should insert");
    let inserted = head(&engine, sim.main_branch_id()).await;
    assert_diff(
        &session,
        &baseline,
        &inserted,
        "added",
        Value::Null,
        Value::Text("one".into()),
    )
    .await;

    session
        .execute(
            "UPDATE row_ref_composite_member SET value = 'two' WHERE parent_id = 'parent' AND key = 7",
            &[],
        )
        .await
        .expect("composite row should update");
    let updated = head(&engine, sim.main_branch_id()).await;
    assert_diff(
        &session,
        &inserted,
        &updated,
        "modified",
        Value::Text("one".into()),
        Value::Text("two".into()),
    )
    .await;

    session
        .execute(
            "DELETE FROM row_ref_composite_member WHERE parent_id = 'parent' AND key = 7",
            &[],
        )
        .await
        .expect("composite row should delete");
    let deleted = head(&engine, sim.main_branch_id()).await;
    assert_diff(
        &session,
        &updated,
        &deleted,
        "removed",
        Value::Text("two".into()),
        Value::Null,
    )
    .await;
});

async fn assert_diff(
    session: &crate::support::simulation_test::engine::SimSession,
    from: &str,
    to: &str,
    kind: &str,
    from_value: Value,
    to_value: Value,
) {
    let identity_only = session
        .execute(
            "SELECT row_ref, parent_id, key, diff_type \
             FROM lix_diff('row_ref_composite_member', $1, $2) \
             WHERE parent_id = 'parent' AND key = 7",
            &[Value::Text(from.into()), Value::Text(to.into())],
        )
        .await
        .expect("key-only composite diff should not require either side payload");
    assert_eq!(identity_only.rows().len(), 1);
    assert!(matches!(
        identity_only.rows()[0].values(),
        [Value::RowRef(_), Value::Text(parent), Value::Integer(7), Value::Text(actual_kind)]
            if parent == "parent" && actual_kind == kind
    ));

    let result = session
        .execute(
            "SELECT row_ref, parent_id, key, diff_type, from_value, to_value \
             FROM lix_diff('row_ref_composite_member', $1, $2) \
             WHERE parent_id = 'parent' AND key = 7",
            &[Value::Text(from.into()), Value::Text(to.into())],
        )
        .await
        .expect("composite diff should execute");
    assert_eq!(
        result.columns(),
        ["row_ref", "parent_id", "key", "diff_type", "from_value", "to_value"]
    );
    assert_eq!(result.column_types()[0], ResultColumnType::RowRef);
    assert_eq!(result.rows().len(), 1);
    let values = result.rows()[0].values();
    assert!(matches!(values[0], Value::RowRef(_)));
    assert_eq!(values[1], Value::Text("parent".into()));
    assert_eq!(values[2], Value::Integer(7));
    assert_eq!(values[3], Value::Text(kind.into()));
    assert_eq!(values[4], from_value);
    assert_eq!(values[5], to_value);

    let direct = session
        .execute(
            "SELECT lix_row_ref('row_ref_composite_member', 'parent', 7)",
            &[],
        )
        .await
        .expect("typed composite identity should construct");
    assert_eq!(values[0], direct.rows()[0].values()[0]);
}

async fn head(
    engine: &lix::engine::Engine,
    branch_id: &str,
) -> String {
    engine
        .load_branch_head_commit_id(branch_id)
        .await
        .expect("head should load")
        .expect("head should exist")
        .to_string()
}
