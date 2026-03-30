use crate::support;

use serde_json::json;

async fn register_parent_schema(
    engine: &support::simulation_test::SimulationEngine,
    schema_key: &str,
) {
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
    engine: &support::simulation_test::SimulationEngine,
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
    engine: &support::simulation_test::SimulationEngine,
    schema_key: &str,
) {
    engine
        .register_schema(&json!({
            "x-lix-key": schema_key,
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "x-lix-override-lixcols": {
                "lixcol_file_id": "\"lix\"",
                "lixcol_plugin_key": "\"lix\""
            },
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

simulation_test!(
    foreign_key_requires_target_in_same_version,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_parent_schema(&engine, "fk_parent_same_version").await;
        register_child_schema(&engine, "fk_child_same_version", "fk_parent_same_version").await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
             ) VALUES (\
             'parent-1', 'fk_parent_same_version', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"parent-1\",\"name\":\"parent\"}'\
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
             'child-1', 'fk_child_same_version', 'alpha.md', 'version-b', 'lix', '1', '{\"id\":\"child-1\",\"parent_id\":\"parent-1\",\"name\":\"child\"}'\
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
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_parent_schema(&engine, "fk_parent_same_file").await;
        register_child_schema(&engine, "fk_child_same_file", "fk_parent_same_file").await;
        engine.create_named_version("version-a").await.unwrap();

        engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
             ) VALUES (\
             'parent-1', 'fk_parent_same_file', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"parent-1\",\"name\":\"parent\"}'\
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
             'child-1', 'fk_child_same_file', 'beta.md', 'version-a', 'lix', '1', '{\"id\":\"child-1\",\"parent_id\":\"parent-1\",\"name\":\"child\"}'\
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
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_parent_schema(&engine, "fk_state_target_doc").await;
        register_state_ref_schema(&engine, "fk_state_ref_meta").await;
        engine.create_named_version("version-a").await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'doc-1', 'fk_state_target_doc', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"doc-1\",\"name\":\"doc\"}'\
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
                 'ref-1', 'fk_state_ref_meta', 'lix', 'version-a', 'lix', '1', '{\"id\":\"ref-1\",\"target_entity_id\":\"doc-1\",\"target_schema_key\":\"fk_state_target_doc\",\"target_file_id\":\"alpha.md\"}'\
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
                 'ref-2', 'fk_state_ref_meta', 'lix', 'version-a', 'lix', '1', '{\"id\":\"ref-2\",\"target_entity_id\":\"doc-1\",\"target_schema_key\":\"fk_state_target_doc\",\"target_file_id\":\"beta.md\"}'\
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
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_parent_schema(&engine, "fk_parent_delete_restrict").await;
        register_child_schema(
            &engine,
            "fk_child_delete_restrict",
            "fk_parent_delete_restrict",
        )
        .await;
        engine.create_named_version("version-a").await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'parent-1', 'fk_parent_delete_restrict', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"parent-1\",\"name\":\"parent\"}'\
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
                 'child-1', 'fk_child_delete_restrict', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"child-1\",\"parent_id\":\"parent-1\",\"name\":\"child\"}'\
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
