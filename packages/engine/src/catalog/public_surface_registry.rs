use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use crate::catalog::{
    build_builtin_surface_registry, dynamic_entity_surface_spec_from_schema,
    register_dynamic_entity_surface_spec, remove_dynamic_entity_surfaces_for_schema_key,
    SurfaceRegistry,
};
use crate::live_state::{
    decode_registered_schema_row, load_current_committed_version_frontier_with_backend,
    scan_live_rows, LiveRowQuery, LiveRowSource,
};
use crate::schema::schema_from_registered_snapshot;
use crate::schema::SchemaKey;
use crate::{LixBackend, LixError};

pub(crate) async fn load_public_surface_registry_with_backend(
    backend: &dyn LixBackend,
    requested_version_id: Option<&str>,
) -> Result<SurfaceRegistry, LixError> {
    let mut registry = build_builtin_surface_registry();
    let visible_version_ids = visible_registered_schema_version_ids(backend, requested_version_id)
        .await?;
    for (_, schema) in load_latest_registered_schemas(backend, &visible_version_ids).await? {
        let spec = dynamic_entity_surface_spec_from_schema(&schema)?;
        register_dynamic_entity_surface_spec(&mut registry, spec);
    }
    Ok(registry)
}

pub(crate) fn apply_registered_schema_snapshot_to_surface_registry(
    registry: &mut SurfaceRegistry,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    let (key, schema) = schema_from_registered_snapshot(snapshot)?;
    remove_dynamic_entity_surfaces_for_schema_key(registry, &key.schema_key);
    let spec = dynamic_entity_surface_spec_from_schema(&schema)?;
    register_dynamic_entity_surface_spec(registry, spec);
    Ok(())
}

async fn load_latest_registered_schemas(
    backend: &dyn LixBackend,
    visible_version_ids: &[String],
) -> Result<Vec<(SchemaKey, JsonValue)>, LixError> {
    let mut latest_by_schema_key = BTreeMap::<String, (SchemaKey, JsonValue)>::new();
    for version_id in visible_version_ids {
        let rows = scan_live_rows(
            backend,
            &LiveRowQuery {
                schema_key: "lix_registered_schema".to_string(),
                version_id: version_id.clone(),
                source: LiveRowSource::Tracked,
                constraints: Vec::new(),
                include_tombstones: false,
            },
        )
        .await?;

        for row in &rows {
            let Some((key, schema)) = decode_registered_schema_row(row)? else {
                continue;
            };

            let should_replace = latest_by_schema_key
                .get(&key.schema_key)
                .map(|(existing, _)| !schema_key_is_older(&key, existing))
                .unwrap_or(true);
            if should_replace {
                latest_by_schema_key.insert(key.schema_key.clone(), (key, schema));
            }
        }
    }

    Ok(latest_by_schema_key.into_values().collect())
}

async fn visible_registered_schema_version_ids(
    backend: &dyn LixBackend,
    requested_version_id: Option<&str>,
) -> Result<Vec<String>, LixError> {
    let mut version_ids =
        std::collections::BTreeSet::from([crate::version::GLOBAL_VERSION_ID.to_string()]);
    if let Some(requested_version_id) = requested_version_id {
        version_ids.insert(requested_version_id.to_string());
    } else {
        let frontier = load_current_committed_version_frontier_with_backend(backend).await?;
        version_ids.extend(frontier.version_heads.into_keys());
    }
    Ok(version_ids.into_iter().collect())
}

fn schema_key_is_older(candidate: &SchemaKey, existing: &SchemaKey) -> bool {
    match (candidate.version_number(), existing.version_number()) {
        (Some(candidate_version), Some(existing_version)) => candidate_version < existing_version,
        _ => candidate.schema_version < existing.schema_version,
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::dynamic_entity_surface_spec_from_schema;
    use serde_json::json;

    #[test]
    fn entity_surface_spec_is_derived_from_schema_properties() {
        let spec = dynamic_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "properties": {
                "message": { "type": "string" },
                "id": { "type": "string" }
            }
        }))
        .expect("schema spec should derive");

        assert_eq!(spec.schema_key, "project_message");
        assert_eq!(
            spec.visible_columns,
            vec!["id".to_string(), "message".to_string()]
        );
    }
}
