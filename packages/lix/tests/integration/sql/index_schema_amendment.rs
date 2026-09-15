use lix::{CreateBranchOptions, SwitchBranchOptions, Value};

/// Fork publication intentionally leaves inherited collections unwitnessed.
/// Amending their schema must not turn that safe scan into an empty index.
#[tokio::test]
async fn schema_amendment_preserves_inherited_indexed_rows() {
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
        "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES ($1, false, false)",
        &[Value::Jsonb(schema.clone().into())],
    ).await.unwrap();
    db.execute(
        "INSERT INTO amended_note (id, label) VALUES ('old', 'old')",
        &[],
    )
    .await
    .unwrap();
    let branch = db
        .create_branch(CreateBranchOptions {
            id: None,
            name: "amendment-fork".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    db.switch_branch(SwitchBranchOptions {
        branch_id: branch.id,
    })
    .await
    .unwrap();
    for amend in [false, true] {
        if amend {
            schema["description"] = "Compatible metadata amendment".into();
            db.execute(
                "UPDATE lix_registered_schema SET value = $1 WHERE schema_key = 'amended_note'",
                &[Value::Jsonb(schema.clone().into())],
            )
            .await
            .unwrap();
            db.execute(
                "INSERT INTO amended_note (id, label) VALUES ('new', 'new')",
                &[],
            )
            .await
            .unwrap();
        }
        let indexed = db
            .execute("SELECT id FROM amended_note WHERE label = 'old'", &[])
            .await
            .unwrap();
        let scanned = db
            .execute(
                "SELECT id FROM amended_note WHERE concat('', label) = 'old'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            indexed, scanned,
            "schema amendment must not witness inherited rows, amend={amend}"
        );
        assert_eq!(indexed.len(), 1);
        let duplicate = db
            .execute(
                "INSERT INTO amended_note (id, label) VALUES ('duplicate', 'old')",
                &[],
            )
            .await
            .expect_err("inherited unique value must stay visible to validation");
        assert_eq!(duplicate.code, lix::LixError::CODE_UNIQUE);
    }
}
