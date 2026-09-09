use crate::branch::{BranchRefReader as _, branch_descriptor_stage_row, branch_ref_stage_row};
use crate::transaction_types::{
    RawWriteBatch, TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteRow,
};

fn local_row(branch: &str, key: &str) -> TransactionWriteRow {
    TransactionWriteRow {
        row_pk: Some(crate::row_pk::RowPk::single(key)),
        schema_key: "lix_key_value".into(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(
            serde_json::json!({"key": key, "value": "must be atomic"}),
        )),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: false,
        change_id: None,
        commit_id: None,
        untracked: false,
        branch_id: branch.to_owned().into(),
    }
}

#[tokio::test]
async fn new_branch_with_missing_source_cannot_publish_partial_recovery() {
    let lix = crate::open_lix().await.unwrap();
    let branch = uuid::Uuid::now_v7().to_string();
    let absent = crate::changelog::CommitId::new(uuid::Uuid::now_v7());
    let result = lix
        .session
        .with_write_transaction_lending(async |transaction| {
            let mut creation = RawWriteBatch::new();
            creation.push(branch_descriptor_stage_row(
                &branch,
                "invalid recovery",
                false,
            ));
            creation.push(branch_ref_stage_row(&branch, &absent));
            transaction
                .stage_write(TransactionWrite::Rows {
                    mode: TransactionWriteMode::Insert,
                    rows: creation,
                })
                .await?;
            let mut rows = RawWriteBatch::new();
            rows.push(local_row(&branch, "invalid-recovery-row"));
            transaction
                .stage_recovery_write(TransactionWrite::Rows {
                    mode: TransactionWriteMode::Replace,
                    rows,
                })
                .await?;
            let mut receipt = local_row(crate::GLOBAL_BRANCH_ID, "invalid-recovery-receipt");
            receipt.global = true;
            let mut receipts = RawWriteBatch::new();
            receipts.push(receipt);
            transaction
                .stage_write(TransactionWrite::Rows {
                    mode: TransactionWriteMode::Insert,
                    rows: receipts,
                })
                .await?;
            Ok(())
        })
        .await;
    assert!(result.is_err(), "an absent parent must fail validation");
    assert!(
        lix.execute(
            "SELECT id FROM lix_branch WHERE id = $1",
            &[crate::Value::Text(branch)]
        )
        .await
        .unwrap()
        .rows()
        .is_empty()
    );
    assert!(
        lix.execute(
            "SELECT value FROM lix_key_value WHERE key = 'invalid-recovery-receipt'",
            &[]
        )
        .await
        .unwrap()
        .rows()
        .is_empty()
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn existing_branch_ref_and_normal_commit_remain_rejected_atomically() {
    let lix = crate::open_lix().await.unwrap();
    let branch = lix.active_branch_id().await.unwrap();
    let result = lix
        .session
        .with_write_transaction_lending(async |transaction| {
            let head = transaction
                .branch_ref_reader()
                .await?
                .load_head_commit_id(&branch)
                .await?
                .unwrap();
            let mut lifecycle = RawWriteBatch::new();
            lifecycle.push(branch_descriptor_stage_row(
                &branch,
                "must not rename",
                false,
            ));
            lifecycle.push(branch_ref_stage_row(&branch, &head));
            transaction
                .stage_write(TransactionWrite::Rows {
                    mode: TransactionWriteMode::Replace,
                    rows: lifecycle,
                })
                .await?;
            let mut rows = RawWriteBatch::new();
            rows.push(local_row(&branch, "invalid-existing-ref-write"));
            transaction
                .stage_write(TransactionWrite::Rows {
                    mode: TransactionWriteMode::Replace,
                    rows,
                })
                .await?;
            Ok(())
        })
        .await;
    let error = result.unwrap_err();
    assert!(error.message.contains("explicit branch ref"), "{error}");
    assert!(
        lix.execute(
            "SELECT value FROM lix_key_value WHERE key = 'invalid-existing-ref-write'",
            &[]
        )
        .await
        .unwrap()
        .rows()
        .is_empty()
    );
    let rows = lix
        .execute(
            "SELECT name FROM lix_branch WHERE id = $1",
            &[crate::Value::Text(branch)],
        )
        .await
        .unwrap();
    assert_ne!(
        rows.rows()[0].get::<String>("name").unwrap(),
        "must not rename"
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn recovery_staging_keeps_normal_raw_plugin_writes_protected() {
    let lix = crate::open_lix().await.unwrap();
    let branch = lix.active_branch_id().await.unwrap();
    for key in ["lix_plugin_registry_v2", "lix_plugin_owner_v2"] {
        let error = lix
            .session
            .with_write_transaction_lending(async |transaction| {
                let mut rows = RawWriteBatch::new();
                rows.push(local_row(&branch, key));
                transaction
                    .stage_write(TransactionWrite::Rows {
                        mode: TransactionWriteMode::Insert,
                        rows,
                    })
                    .await?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, crate::LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(error.message.contains("reserved"), "{error}");
    }
    lix.close().await.unwrap();
}

#[tokio::test]
async fn recovery_content_does_not_inherit_the_callers_branch() {
    let lix = crate::open_lix().await.unwrap();
    let other = lix
        .create_branch(crate::CreateBranchOptions {
            id: None,
            name: "other caller".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let other = lix
        .open_internal_session(&other.id, lix.active_account_id())
        .await
        .unwrap();
    let captured = crate::ReplicaRecoveryRow {
        row_pk: crate::row_pk::RowPk::single("captured")
            .as_typed_json_array_value()
            .unwrap(),
        schema_key: "lix_key_value".to_owned(),
        file_id: None,
        snapshot: Some(serde_json::json!({"key": "captured", "value": "local work"})),
        metadata: None,
        deleted: false,
        untracked: false,
        global: false,
        change_id: None,
        commit_id: None,
    };
    let mut roots = Vec::new();
    for (index, caller) in [&lix, &other].into_iter().enumerate() {
        caller
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    crate::Value::Text(format!("/caller-{index}.txt")),
                    crate::Value::Blob(b"unrelated caller content".to_vec().into()),
                ],
            )
            .await
            .unwrap();
        caller
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('unrelated', $1)",
                &[crate::Value::Text(format!("caller {index}"))],
            )
            .await
            .unwrap();
        let branch = uuid::Uuid::now_v7().to_string();
        caller
            .restore_replica_rows_atomic(
                &branch,
                &format!("recovery-{index}"),
                std::slice::from_ref(&captured),
                Vec::new(),
                &format!("test-receipt-{index}"),
                serde_json::json!({"branch":branch}),
            )
            .await
            .unwrap();
        let recovered = caller
            .open_internal_session(&branch, caller.active_account_id())
            .await
            .unwrap();
        assert!(
            recovered
                .execute("SELECT path FROM lix_file", &[])
                .await
                .unwrap()
                .rows()
                .is_empty()
        );
        assert!(
            recovered
                .execute(
                    "SELECT value FROM lix_key_value WHERE key = 'unrelated'",
                    &[]
                )
                .await
                .unwrap()
                .rows()
                .is_empty()
        );
        assert_eq!(
            recovered
                .execute(
                    "SELECT value FROM lix_key_value WHERE key = 'captured'",
                    &[]
                )
                .await
                .unwrap()
                .rows()[0]
                .get::<serde_json::Value>("value")
                .unwrap(),
            serde_json::json!("local work")
        );
        roots.push(
            recovered
                .execute("SELECT lix_root_commit_id() AS root", &[])
                .await
                .unwrap()
                .rows()[0]
                .get::<String>("root")
                .unwrap(),
        );
        recovered.close().await.unwrap();
    }
    assert_eq!(roots[0], roots[1]);
    other.close().await.unwrap();
    lix.close().await.unwrap();
}
