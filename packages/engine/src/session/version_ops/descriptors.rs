use std::collections::BTreeMap;

use crate::backend::QueryExecutor;
use crate::canonical::CanonicalStateIdentity;
use crate::common::text::escape_sql_string;
use crate::session::version_ops::load_exact_canonical_row_at_version_head_with_executor;
use crate::surface_sql::version::build_admin_version_source_sql_with_current_heads;
use crate::version_state::{
    load_all_local_version_refs_with_executor, load_local_version_head_commit_id_with_executor,
    parse_version_descriptor_snapshot, version_descriptor_file_id, version_descriptor_schema_key,
    GLOBAL_VERSION_ID,
};
use crate::{LixBackend, LixError};

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
    let row = load_exact_canonical_row_at_version_head_with_executor(
        executor,
        GLOBAL_VERSION_ID,
        &CanonicalStateIdentity {
            entity_id: version_id.to_string(),
            schema_key: version_descriptor_schema_key().to_string(),
            file_id: version_descriptor_file_id().to_string(),
        },
    )
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some(parse_descriptor_row(
        &row.snapshot_content,
        Some(row.source_change_id),
    )?))
}

pub(crate) async fn load_all_version_descriptors_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<VersionDescriptorRow>, LixError> {
    let mut descriptors = Vec::new();
    for version_ref in load_all_local_version_refs_with_executor(executor).await? {
        if let Some(descriptor) =
            load_version_descriptor_with_executor(executor, &version_ref.version_id).await?
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
        load_local_version_head_commit_id_with_executor(executor, GLOBAL_VERSION_ID).await?
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
            load_local_version_head_commit_id_with_executor(executor, &descriptor.version_id)
                .await?
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
    let sql = format!(
        "SELECT id \
         FROM ({source_sql}) version_inventory \
         WHERE id = '{version_id}' \
         LIMIT 1",
        source_sql = build_admin_version_source_sql_with_current_heads(
            executor.dialect(),
            Some(&empty_heads),
        ),
        version_id = escape_sql_string(version_id),
    );
    let result = executor.execute(&sql, &[]).await?;
    Ok(!result.rows.is_empty())
}
