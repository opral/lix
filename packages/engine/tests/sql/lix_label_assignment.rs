use lix_engine::{LixError, Value};
use serde_json::json;

use super::select_rows;

simulation_test!(
    lix_label_assignment_generates_id_and_enforces_mapping_uniqueness,
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
                "INSERT INTO lix_key_value (key, value) VALUES ('label-target', 'one')",
                &[],
            )
            .await
            .expect("target entity insert should succeed");
        session
            .execute(
                "INSERT INTO lix_label (id, name) VALUES ('label-a', 'Needs review')",
                &[],
            )
            .await
            .expect("label insert should succeed");
        session
            .execute(
                "INSERT INTO lix_label_assignment \
                 (target_entity_pk, target_schema_key, target_file_id, label_id) \
                 VALUES (lix_json('[\"label-target\"]'), 'lix_key_value', NULL, 'label-a')",
                &[],
            )
            .await
            .expect("label assignment insert should succeed");

        let rows = select_rows(
            &session,
            "SELECT id, target_entity_pk, target_schema_key, target_file_id, label_id \
             FROM lix_label_assignment \
             WHERE target_entity_pk = lix_json('[\"label-target\"]')",
        )
        .await;

        assert_eq!(rows.len(), 1);
        let id = match &rows[0][0] {
            Value::Text(value) => value,
            other => panic!("expected generated string id, got {other:?}"),
        };
        assert!(!id.is_empty());
        assert_eq!(
            &rows[0][1..],
            &[
                Value::Json(json!(["label-target"])),
                Value::Text("lix_key_value".to_string()),
                Value::Null,
                Value::Text("label-a".to_string()),
            ]
        );

        let error = session
            .execute(
                "INSERT INTO lix_label_assignment \
                 (target_entity_pk, target_schema_key, target_file_id, label_id) \
                 VALUES (lix_json('[\"label-target\"]'), 'lix_key_value', NULL, 'label-a')",
                &[],
            )
            .await
            .expect_err("duplicate label assignment should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);

        let error = session
            .execute(
                "INSERT INTO lix_label (id, name) VALUES ('label-b', 'Needs review')",
                &[],
            )
            .await
            .expect_err("duplicate label name should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }
);

simulation_test!(
    lix_label_assignment_rejects_missing_target_state_row,
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
                "INSERT INTO lix_label (id, name) VALUES ('label-a', 'Needs review')",
                &[],
            )
            .await
            .expect("label insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_label_assignment \
                 (target_entity_pk, target_schema_key, target_file_id, label_id) \
                 VALUES (lix_json('[\"missing-target\"]'), 'lix_key_value', NULL, 'label-a')",
                &[],
            )
            .await
            .expect_err("label assignment to missing live state row should be rejected");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }
);
