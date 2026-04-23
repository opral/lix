use crate::support;

use lix_engine::Value;
use serde_json::json;

async fn register_parent_schema(engine: &support::simulation_test::SimulatedLix, schema_key: &str) {
    engine
        .register_schema(&json!({
            "x-lix-key": schema_key,
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "name": { "type": "string" }
            },
            "required": ["id", "name"],
            "additionalProperties": false
        }))
        .await
        .unwrap();
}

async fn register_child_schema(
    engine: &support::simulation_test::SimulatedLix,
    schema_key: &str,
    parent_schema_key: &str,
) {
    engine
        .register_schema(&json!({
            "x-lix-key": schema_key,
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "x-lix-foreign-keys": [
                {
                    "properties": ["/parent_id"],
                    "references": {
                        "schemaKey": parent_schema_key,
                        "properties": ["/id"]
                    }
                }
            ],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "parent_id": { "type": "string" },
                "name": { "type": "string" }
            },
            "required": ["id", "parent_id", "name"],
            "additionalProperties": false
        }))
        .await
        .unwrap();
}

async fn register_state_ref_schema(
    engine: &support::simulation_test::SimulatedLix,
    schema_key: &str,
) {
    engine
        .register_schema(&json!({
            "x-lix-key": schema_key,
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "x-lix-foreign-keys": [
                {
                    "properties": ["/target_entity_id", "/target_schema_key", "/target_file_id"],
                    "references": {
                        "schemaKey": "lix_state",
                        "properties": ["/entity_id", "/schema_key", "/file_id"]
                    }
                }
            ],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "target_entity_id": { "type": "string" },
                "target_schema_key": { "type": "string" },
                "target_file_id": { "type": "string" }
            },
            "required": ["id", "target_entity_id", "target_schema_key", "target_file_id"],
            "additionalProperties": false
        }))
        .await
        .unwrap();
}

async fn seed_file_descriptor(
    engine: &support::simulation_test::SimulatedLix,
    version_id: &str,
    file_id: &str,
) {
    let (name, extension) = file_id
        .rsplit_once('.')
        .map(|(name, extension)| (name, Some(extension)))
        .unwrap_or((file_id, None));
    let snapshot = json!({
        "id": file_id,
        "directory_id": null,
        "name": name,
        "extension": extension,
        "hidden": false
    })
    .to_string();

    engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
             ) VALUES (\
             $1, 'lix_file_descriptor', NULL, $2, NULL, '1', $3\
             )",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
                Value::Text(snapshot),
            ],
        )
        .await
        .unwrap();
}

simulation_test!(
    foreign_key_requires_target_in_same_version,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_parent_schema(&engine, "fk_parent_same_version").await;
        register_child_schema(&engine, "fk_child_same_version", "fk_parent_same_version").await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();
        seed_file_descriptor(&engine, "version-a", "alpha.md").await;
        seed_file_descriptor(&engine, "version-b", "alpha.md").await;

        engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
             ) VALUES (\
             'parent-1', 'fk_parent_same_version', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"parent-1\",\"name\":\"parent\"}'\
             )",
            &[],
        )
        .await
        .unwrap();

        let child = engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
             ) VALUES (\
             'child-1', 'fk_child_same_version', 'alpha.md', 'version-b', NULL, '1', '{\"id\":\"child-1\",\"parent_id\":\"parent-1\",\"name\":\"child\"}'\
             )",
            &[],
        )
        .await;

        assert!(
            child.is_err(),
            "foreign keys should only resolve within the same concrete version"
        );
    }
);

simulation_test!(
    foreign_key_defaults_to_same_file_scope,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_parent_schema(&engine, "fk_parent_same_file").await;
        register_child_schema(&engine, "fk_child_same_file", "fk_parent_same_file").await;
        engine.create_named_version("version-a").await.unwrap();
        seed_file_descriptor(&engine, "version-a", "alpha.md").await;
        seed_file_descriptor(&engine, "version-a", "beta.md").await;

        engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
             ) VALUES (\
             'parent-1', 'fk_parent_same_file', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"parent-1\",\"name\":\"parent\"}'\
             )",
            &[],
        )
        .await
        .unwrap();

        let child = engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
             ) VALUES (\
             'child-1', 'fk_child_same_file', 'beta.md', 'version-a', NULL, '1', '{\"id\":\"child-1\",\"parent_id\":\"parent-1\",\"name\":\"child\"}'\
             )",
            &[],
        )
        .await;

        assert!(
            child.is_err(),
            "foreign keys should default to the source row's file scope"
        );
    }
);

simulation_test!(
    foreign_key_can_target_explicit_file_id_via_lix_state_tuple,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_parent_schema(&engine, "fk_state_target_doc").await;
        register_state_ref_schema(&engine, "fk_state_ref_meta").await;
        engine.create_named_version("version-a").await.unwrap();
        seed_file_descriptor(&engine, "version-a", "alpha.md").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'doc-1', 'fk_state_target_doc', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"doc-1\",\"name\":\"doc\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'ref-1', 'fk_state_ref_meta', NULL, 'version-a', NULL, '1', '{\"id\":\"ref-1\",\"target_entity_id\":\"doc-1\",\"target_schema_key\":\"fk_state_target_doc\",\"target_file_id\":\"alpha.md\"}'\
                 )",
                &[],
            )
            .await
            .expect("explicit target_file_id should allow cross-file metadata references");

        let wrong_target_file = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'ref-2', 'fk_state_ref_meta', NULL, 'version-a', NULL, '1', '{\"id\":\"ref-2\",\"target_entity_id\":\"doc-1\",\"target_schema_key\":\"fk_state_target_doc\",\"target_file_id\":\"beta.md\"}'\
                 )",
                &[],
            )
            .await;

        assert!(
            wrong_target_file.is_err(),
            "explicit file_id in the foreign-key tuple should constrain the target file"
        );
    }
);

simulation_test!(
    foreign_key_restricts_delete_of_referenced_target,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_parent_schema(&engine, "fk_parent_delete_restrict").await;
        register_child_schema(
            &engine,
            "fk_child_delete_restrict",
            "fk_parent_delete_restrict",
        )
        .await;
        engine.create_named_version("version-a").await.unwrap();
        seed_file_descriptor(&engine, "version-a", "alpha.md").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'parent-1', 'fk_parent_delete_restrict', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"parent-1\",\"name\":\"parent\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'child-1', 'fk_child_delete_restrict', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"child-1\",\"parent_id\":\"parent-1\",\"name\":\"child\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let delete_parent = engine
            .execute(
                "DELETE FROM lix_state_by_version \
                 WHERE entity_id = 'parent-1' \
                   AND schema_key = 'fk_parent_delete_restrict' \
                   AND file_id = 'alpha.md' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await;

        assert!(
            delete_parent.is_err(),
            "deleting a referenced target should be restricted by foreign-key enforcement"
        );
    }
);

simulation_test!(
    foreign_key_missing_target_error_names_pointers_and_schemas,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_parent_schema(&engine, "fk_msg_parent").await;
        register_child_schema(&engine, "fk_msg_child", "fk_msg_parent").await;
        engine.create_named_version("version-a").await.unwrap();
        seed_file_descriptor(&engine, "version-a", "alpha.md").await;

        let err = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'child-1', 'fk_msg_child', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"child-1\",\"parent_id\":\"ghost\",\"name\":\"child\"}'\
                 )",
                &[],
            )
            .await
            .expect_err("missing parent should surface an FK violation");

        assert_eq!(
            err.code, "LIX_ERROR_FOREIGN_KEY",
            "FK violations should carry the categorized code"
        );
        let desc = &err.description;
        assert!(
            desc.contains("foreign key on fk_msg_child./parent_id"),
            "expected local pointer in message: {desc}"
        );
        assert!(
            desc.contains("→ fk_msg_parent./id"),
            "expected target schema + pointer in message: {desc}"
        );
        assert!(
            desc.contains("no matching row"),
            "expected 'no matching row' in message: {desc}"
        );
        assert!(
            desc.contains("\"ghost\""),
            "expected looked-up value in message: {desc}"
        );
        assert!(
            !desc.contains("constraint 0"),
            "new message should not expose the internal FK index: {desc}"
        );
        assert!(
            !desc.contains("version '"),
            "new message should not expose the internal version UUID: {desc}"
        );
        assert!(
            !desc.contains("'NULL'"),
            "new message should not print the literal 'NULL' file reference: {desc}"
        );
    }
);

simulation_test!(
    foreign_key_composite_error_renders_pointer_tuples,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_parent_schema(&engine, "fk_composite_doc").await;
        register_state_ref_schema(&engine, "fk_composite_ref").await;
        engine.create_named_version("version-a").await.unwrap();
        seed_file_descriptor(&engine, "version-a", "alpha.md").await;

        let err = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'ref-1', 'fk_composite_ref', NULL, 'version-a', NULL, '1', '{\"id\":\"ref-1\",\"target_entity_id\":\"ghost\",\"target_schema_key\":\"fk_composite_doc\",\"target_file_id\":\"alpha.md\"}'\
                 )",
                &[],
            )
            .await
            .expect_err("missing target tuple should surface an FK violation");

        let desc = &err.description;
        assert!(
            desc.contains(
                "foreign key on fk_composite_ref.(/target_entity_id, /target_schema_key, /target_file_id)"
            ),
            "expected composite local tuple in message: {desc}"
        );
        assert!(
            desc.contains("→ fk_composite_doc.(/entity_id, /schema_key, /file_id)"),
            "expected composite target tuple (resolved effective schema) in message: {desc}"
        );
        assert!(
            desc.contains("(\"ghost\", \"fk_composite_doc\", \"alpha.md\")"),
            "expected composite looked-up value tuple in message: {desc}"
        );
    }
);

simulation_test!(
    foreign_key_restrict_delete_error_names_source_and_target,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_parent_schema(&engine, "fk_msg_parent_restrict").await;
        register_child_schema(&engine, "fk_msg_child_restrict", "fk_msg_parent_restrict").await;
        engine.create_named_version("version-a").await.unwrap();
        seed_file_descriptor(&engine, "version-a", "alpha.md").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'parent-1', 'fk_msg_parent_restrict', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"parent-1\",\"name\":\"parent\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'child-1', 'fk_msg_child_restrict', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"child-1\",\"parent_id\":\"parent-1\",\"name\":\"child\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let err = engine
            .execute(
                "DELETE FROM lix_state_by_version \
                 WHERE entity_id = 'parent-1' \
                   AND schema_key = 'fk_msg_parent_restrict' \
                   AND file_id = 'alpha.md' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .expect_err("deleting a referenced target should fail with a restrict error");

        assert_eq!(
            err.code, "LIX_ERROR_FOREIGN_KEY",
            "FK restrict violations should carry the categorized code"
        );
        let desc = &err.description;
        assert!(
            desc.contains("foreign key restrict"),
            "expected restrict prefix in message: {desc}"
        );
        assert!(
            desc.contains("cannot delete fk_msg_parent_restrict entity 'parent-1'"),
            "expected target schema + entity_id in message: {desc}"
        );
        assert!(
            desc.contains(
                "still referenced by fk_msg_child_restrict./parent_id → fk_msg_parent_restrict./id"
            ),
            "expected source→target FK descriptor in message: {desc}"
        );
        assert!(
            !desc.contains("version '"),
            "new message should not expose the internal version UUID: {desc}"
        );
        assert!(
            !desc.contains("'NULL'"),
            "new message should not print the literal 'NULL' file reference: {desc}"
        );
    }
);
