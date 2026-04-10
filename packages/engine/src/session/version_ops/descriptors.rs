use std::collections::BTreeMap;

use crate::backend::QueryExecutor;
use crate::canonical::{
    load_exact_committed_change_from_commit_with_executor, ExactCommittedStateRowRequest,
};
use crate::catalog::{bind_named_relation, RelationBindContext};
use crate::common::text::escape_sql_string;
use crate::contracts::GLOBAL_VERSION_ID;
use crate::contracts::{
    parse_version_descriptor_snapshot, version_descriptor_file_id, version_descriptor_plugin_key,
    version_descriptor_schema_key, version_descriptor_schema_version,
};
use crate::session::version_ops::{
    load_version_head_commit_id_with_executor, load_version_head_commit_map_with_executor,
};
use crate::sql::lower_catalog_relation_binding_to_source_sql;
use crate::{LixBackend, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionDescriptorRow {
    pub(crate) version_id: String,
    pub(crate) name: String,
    pub(crate) hidden: bool,
    pub(crate) change_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionHeadFact {
    pub(crate) version_id: String,
    pub(crate) head_commit_id: String,
}

pub(crate) async fn load_version_descriptor_with_backend(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<VersionDescriptorRow>, LixError> {
    let mut executor = backend;
    load_version_descriptor_with_executor(&mut executor, version_id).await
}

pub(crate) async fn load_version_descriptor_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<VersionDescriptorRow>, LixError> {
    let Some(global_head_commit_id) =
        load_version_head_commit_id_with_executor(executor, GLOBAL_VERSION_ID).await?
    else {
        return Ok(None);
    };
    let row = load_exact_committed_change_from_commit_with_executor(
        executor,
        &global_head_commit_id,
        &ExactCommittedStateRowRequest {
            entity_id: version_id.to_string(),
            schema_key: version_descriptor_schema_key().to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            exact_filters: BTreeMap::from([
                (
                    "file_id".to_string(),
                    Value::Text(version_descriptor_file_id().to_string()),
                ),
                (
                    "plugin_key".to_string(),
                    Value::Text(version_descriptor_plugin_key().to_string()),
                ),
                (
                    "schema_version".to_string(),
                    Value::Text(version_descriptor_schema_version().to_string()),
                ),
            ]),
        },
    )
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
        return Ok(None);
    };
    Ok(Some(parse_descriptor_row(snapshot_content, Some(row.id))?))
}

pub(crate) async fn load_all_version_descriptors_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<VersionDescriptorRow>, LixError> {
    let mut descriptors = Vec::new();
    let Some(version_heads) = load_version_head_commit_map_with_executor(executor).await? else {
        return Ok(descriptors);
    };
    for version_id in version_heads.keys() {
        if let Some(descriptor) =
            load_version_descriptor_with_executor(executor, version_id).await?
        {
            descriptors.push(descriptor);
        }
    }
    descriptors.sort_by(|left, right| left.version_id.cmp(&right.version_id));
    Ok(descriptors)
}

pub(crate) async fn find_version_id_by_name_with_executor(
    executor: &mut dyn QueryExecutor,
    name: &str,
) -> Result<Option<String>, LixError> {
    for descriptor in load_all_version_descriptors_with_executor(executor).await? {
        if descriptor.name == name {
            return Ok(Some(descriptor.version_id));
        }
    }
    Ok(None)
}

pub(crate) async fn load_checkpoint_version_heads_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<VersionHeadFact>, LixError> {
    let mut heads = Vec::new();

    let Some(global_head_commit_id) =
        load_version_head_commit_id_with_executor(executor, GLOBAL_VERSION_ID).await?
    else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "global version is missing a committed head",
        ));
    };
    heads.push(VersionHeadFact {
        version_id: GLOBAL_VERSION_ID.to_string(),
        head_commit_id: global_head_commit_id,
    });

    for descriptor in load_all_version_descriptors_with_executor(executor).await? {
        if descriptor.version_id == GLOBAL_VERSION_ID {
            continue;
        }
        let Some(head_commit_id) =
            load_version_head_commit_id_with_executor(executor, &descriptor.version_id).await?
        else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "version '{}' is missing a committed head",
                    descriptor.version_id
                ),
            ));
        };
        heads.push(VersionHeadFact {
            version_id: descriptor.version_id,
            head_commit_id,
        });
    }

    heads.sort_by(|left, right| left.version_id.cmp(&right.version_id));
    Ok(heads)
}

pub(crate) async fn version_exists_with_backend(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<bool, LixError> {
    Ok(load_version_descriptor_with_backend(backend, version_id)
        .await?
        .is_some())
}

pub(crate) async fn version_exists_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<bool, LixError> {
    if load_version_descriptor_with_executor(executor, version_id)
        .await?
        .is_some()
    {
        return Ok(true);
    }

    version_exists_in_descriptor_inventory_with_executor(executor, version_id).await
}

fn parse_descriptor_row(
    snapshot_content: &str,
    change_id: Option<String>,
) -> Result<VersionDescriptorRow, LixError> {
    let snapshot = parse_version_descriptor_snapshot(snapshot_content)?;
    Ok(VersionDescriptorRow {
        version_id: snapshot.id,
        name: snapshot.name.unwrap_or_default(),
        hidden: snapshot.hidden,
        change_id,
    })
}

async fn version_exists_in_descriptor_inventory_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<bool, LixError> {
    let empty_heads = BTreeMap::new();
    let binding = bind_named_relation(
        "lix_version",
        RelationBindContext {
            active_version_id: None,
            current_heads: Some(&empty_heads),
        },
    )?
    .expect("lix_version must bind to a catalog relation");
    let sql = format!(
        "SELECT id \
         FROM ({source_sql}) version_inventory \
         WHERE id = '{version_id}' \
         LIMIT 1",
        source_sql = lower_catalog_relation_binding_to_source_sql(executor.dialect(), &binding)?,
        version_id = escape_sql_string(version_id),
    );
    let result = executor.execute(&sql, &[]).await?;
    Ok(!result.rows.is_empty())
}
