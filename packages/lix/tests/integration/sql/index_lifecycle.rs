use futures_util::io::Cursor;
use lix::{CreateBranchOptions, SwitchBranchOptions, Value};

/// Compare declared-column index reads with a full-scan expression after
/// transitions that can replace the hot generation or replay packed rows.
#[tokio::test]
async fn packed_indexes_survive_checkpoint_fork_and_snapshot_reopen() {
    let db = crate::open_lix().await.unwrap();
    let main = db.active_branch_id().await.unwrap();
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json", "key": "lifecycle_note",
        "columns": [
            {"name": "id", "type": "text", "nullable": false},
            {"name": "label", "type": "text", "nullable": true}
        ],
        "primary_key": ["id"], "unique": [["label"]]
    });
    db.execute(
        "INSERT INTO lix_registered_schema (value) VALUES ($1)",
        &[Value::Jsonb(schema.into())],
    )
    .await
    .unwrap();
    let values = (0..513)
        .map(|i| format!("('n{i:03}', 'v{i:03}')"))
        .collect::<Vec<_>>()
        .join(",");
    db.execute(
        &format!("INSERT INTO lifecycle_note (id, label) VALUES {values}"),
        &[],
    )
    .await
    .unwrap();
    assert_index_matches_scan(&db, "insert", "v", 1).await;
    db.execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .unwrap();
    assert_index_matches_scan(&db, "checkpoint", "v", 1).await;
    let branch = db
        .create_branch(CreateBranchOptions {
            id: None,
            name: "index-fork".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    db.switch_branch(SwitchBranchOptions {
        branch_id: branch.id.clone(),
    })
    .await
    .unwrap();
    assert_index_matches_scan(&db, "fork", "v", 1).await;
    db.execute(
        "UPDATE lifecycle_note SET label = concat('new_', label)",
        &[],
    )
    .await
    .unwrap();
    assert_index_matches_scan(&db, "replacement", "new_v", 1).await;
    assert_index_matches_scan(&db, "stale entries", "v", 0).await;
    db.execute("SELECT commit_id FROM lix_undo()", &[])
        .await
        .unwrap();
    assert_index_matches_scan(&db, "undo", "v", 1).await;
    db.execute("SELECT commit_id FROM lix_redo()", &[])
        .await
        .unwrap();
    assert_index_matches_scan(&db, "redo", "new_v", 1).await;
    db.switch_branch(SwitchBranchOptions { branch_id: main })
        .await
        .unwrap();
    assert_index_matches_scan(&db, "main isolation", "v", 1).await;
    db.switch_branch(SwitchBranchOptions {
        branch_id: branch.id,
    })
    .await
    .unwrap();
    let mut snapshot = Vec::new();
    db.export_snapshot().write_to(&mut snapshot).await.unwrap();
    let reopened = crate::open_lix()
        .from_snapshot(Cursor::new(snapshot))
        .await
        .unwrap();
    // Snapshot opening selects its default branch; explicitly select the fork.
    let fork_id = db.active_branch_id().await.unwrap();
    reopened
        .switch_branch(SwitchBranchOptions { branch_id: fork_id })
        .await
        .unwrap();
    assert_index_matches_scan(&reopened, "snapshot reopen", "new_v", 1).await;
    assert_index_matches_scan(&reopened, "snapshot stale entries", "v", 0).await;
    reopened.close().await.unwrap();
    db.close().await.unwrap();
}

async fn assert_index_matches_scan(db: &lix::Lix, stage: &str, prefix: &str, count: usize) {
    for id in [0, 63, 64, 511, 512] {
        let params = [Value::Text(format!("{prefix}{id:03}"))];
        let scan = db
            .execute(
                "SELECT id FROM lifecycle_note WHERE concat(label, '') = $1 ORDER BY id",
                &params,
            )
            .await
            .unwrap();
        assert_eq!(scan.len(), count, "oracle at {stage}, {prefix}{id}");
        let indexed = db
            .execute(
                "SELECT id FROM lifecycle_note WHERE label = $1 ORDER BY id",
                &params,
            )
            .await
            .unwrap();
        assert_eq!(
            indexed.rows(),
            scan.rows(),
            "index at {stage}, {prefix}{id}"
        );
    }
}

#[tokio::test]
async fn compatible_schema_amendment_revalidates_existing_rows_for_reads_and_updates() {
    for untracked in [false, true] {
        let db = crate::open_lix().await.unwrap();
        let mut schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json", "key": "amended_note",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "label", "type": "text", "nullable": false}
            ],
            "primary_key": ["id"], "unique": [["label"]]
        });
        db.execute(
            "INSERT INTO lix_registered_schema (value) VALUES ($1)",
            &[Value::Jsonb(schema.clone().into())],
        )
        .await
        .unwrap();
        let values = (0..513)
            .map(|i| format!("('n{i:03}', 'v{i:03}', {untracked})"))
            .collect::<Vec<_>>()
            .join(",");
        db.execute(
            &format!("INSERT INTO amended_note (id, label, lixcol_untracked) VALUES {values}"),
            &[],
        )
        .await
        .unwrap();
        schema["description"] = serde_json::json!("Updated documentation");
        db.execute(
            "UPDATE lix_registered_schema SET value = $1 WHERE schema_key = 'amended_note'",
            &[Value::Jsonb(schema.clone().into())],
        )
        .await
        .unwrap();
        for predicate in [
            "id = 'n000'",
            "label = 'v000'",
            "concat(label, '') = 'v000'",
        ] {
            let result = db
                .execute(
                    &format!("SELECT id, label FROM amended_note WHERE {predicate}"),
                    &[],
                )
                .await
                .unwrap();
            super::assert_rows_eq(
                result,
                vec![vec![Value::Text("n000".into()), Value::Text("v000".into())]],
            );
        }
        let columns = schema["columns"].as_array_mut().unwrap();
        columns.push(serde_json::json!({"name": "optional", "type": "text", "nullable": true}));
        columns.push(serde_json::json!({"name": "priority", "type": "int8", "nullable": false, "default_value": 7}));
        db.execute(
            "UPDATE lix_registered_schema SET value = $1 WHERE schema_key = 'amended_note'",
            &[Value::Jsonb(schema.into())],
        )
        .await
        .unwrap();
        let query =
            "SELECT id, optional, priority FROM amended_note WHERE priority = 7 AND id = 'n000'";
        let expected = vec![vec![
            Value::Text("n000".into()),
            Value::Null,
            Value::Integer(7),
        ]];
        super::assert_rows_eq(db.execute(query, &[]).await.unwrap(), expected.clone());
        db.execute(
            "UPDATE amended_note SET label = 'changed' WHERE id = 'n000'",
            &[],
        )
        .await
        .unwrap();
        super::assert_rows_eq(db.execute(query, &[]).await.unwrap(), expected.clone());
        let all = db
            .execute("SELECT id FROM amended_note WHERE priority = 7", &[])
            .await
            .unwrap();
        assert_eq!(all.len(), 513);
        let mut snapshot = Vec::new();
        db.export_snapshot().write_to(&mut snapshot).await.unwrap();
        let reopened = crate::open_lix()
            .from_snapshot(Cursor::new(snapshot))
            .await
            .unwrap();
        super::assert_rows_eq(reopened.execute(query, &[]).await.unwrap(), expected);
        assert_eq!(
            reopened
                .execute("SELECT id FROM amended_note WHERE priority = 7", &[])
                .await
                .unwrap()
                .len(),
            513
        );
        reopened.close().await.unwrap();
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn amended_defaults_are_visible_to_write_predicates_and_expressions() {
    let db = crate::open_lix().await.unwrap();
    let mut schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json", "key": "amended_write",
        "columns": [
            {"name": "id", "type": "text", "nullable": false},
            {"name": "value", "type": "int8", "nullable": false}
        ], "primary_key": ["id"]
    });
    db.execute(
        "INSERT INTO lix_registered_schema (value) VALUES ($1)",
        &[Value::Jsonb(schema.clone().into())],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO amended_write (id, value) VALUES ('a', 1), ('b', 1), ('c', 1)",
        &[],
    )
    .await
    .unwrap();
    schema["columns"].as_array_mut().unwrap().push(serde_json::json!({"name": "priority", "type": "int8", "nullable": false, "default_value": 7}));
    db.execute(
        "UPDATE lix_registered_schema SET value = $1 WHERE schema_key = 'amended_write'",
        &[Value::Jsonb(schema.into())],
    )
    .await
    .unwrap();
    assert_eq!(
        db.execute("SELECT id FROM amended_write WHERE priority = 7", &[])
            .await
            .unwrap()
            .len(),
        3
    );
    db.execute("UPDATE amended_write SET value = 2 WHERE priority = 7", &[])
        .await
        .unwrap();
    assert_eq!(
        db.execute("SELECT id FROM amended_write WHERE value = 2", &[])
            .await
            .unwrap()
            .len(),
        3
    );
    db.execute(
        "UPDATE amended_write SET priority = priority + 1 WHERE id = 'a'",
        &[],
    )
    .await
    .unwrap();
    super::assert_rows_eq(
        db.execute("SELECT priority FROM amended_write WHERE id = 'a'", &[])
            .await
            .unwrap(),
        vec![vec![Value::Integer(8)]],
    );
    db.execute("DELETE FROM amended_write WHERE priority = 7", &[])
        .await
        .unwrap();
    super::assert_rows_eq(
        db.execute("SELECT id FROM amended_write ORDER BY id", &[])
            .await
            .unwrap(),
        vec![vec![Value::Text("a".into())]],
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn legacy_sparse_defaults_are_visible_to_update_delete_and_conflict_candidates() {
    let snapshot = include_bytes!("../../fixtures/schema-amendments/sparse-default-v81.lix");
    let db = crate::open_lix()
        .from_snapshot(Cursor::new(snapshot.as_slice()))
        .await
        .unwrap();
    assert_eq!(
        db.execute("SELECT id FROM legacy_default WHERE priority = 7", &[])
            .await
            .unwrap()
            .len(),
        3
    );
    db.execute(
        "UPDATE legacy_default SET value = 2 WHERE priority = 7",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        db.execute("SELECT id FROM legacy_default WHERE value = 2", &[])
            .await
            .unwrap()
            .len(),
        3
    );
    db.close().await.unwrap();

    // Reopen the untouched fixture for each route so earlier writes cannot
    // accidentally backfill the rows under test.
    let db = crate::open_lix()
        .from_snapshot(Cursor::new(snapshot.as_slice()))
        .await
        .unwrap();
    db.execute(
        "UPDATE legacy_default SET priority = priority + 1 WHERE id = 'a'",
        &[],
    )
    .await
    .unwrap();
    super::assert_rows_eq(
        db.execute("SELECT priority FROM legacy_default WHERE id = 'a'", &[])
            .await
            .unwrap(),
        vec![vec![Value::Integer(8)]],
    );
    db.execute("INSERT INTO legacy_default (id, value) VALUES ('b', 3) ON CONFLICT (id) DO UPDATE SET value = legacy_default.priority", &[]).await.unwrap();
    super::assert_rows_eq(
        db.execute("SELECT value FROM legacy_default WHERE id = 'b'", &[])
            .await
            .unwrap(),
        vec![vec![Value::Integer(7)]],
    );
    db.close().await.unwrap();

    let db = crate::open_lix()
        .from_snapshot(Cursor::new(snapshot.as_slice()))
        .await
        .unwrap();
    db.execute("DELETE FROM legacy_default WHERE priority = 7", &[])
        .await
        .unwrap();
    assert!(
        db.execute("SELECT id FROM legacy_default", &[])
            .await
            .unwrap()
            .is_empty()
    );
    db.close().await.unwrap();
}
