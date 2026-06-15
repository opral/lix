//! Plugin archive installation.
//!
//! Installing a plugin is a normal tracked write: the declared schemas become
//! `lix_registered_schema` rows and the original archive is stored under the
//! reserved plugin filesystem root.

use serde_json::{Value as JsonValue, json};

use crate::LixError;
use crate::filesystem::{
    DirectoryPathResolver, FilePathWriteInput, FilesystemRowContext,
    directory_path_resolvers_from_state_rows, filesystem_storage_scope_key, plan_file_path_write,
};
use crate::plugin::{
    ParsedPluginArchive, parse_plugin_archive_for_install, plugin_key_from_archive_path,
    plugin_storage_archive_file_id, plugin_storage_archive_path,
};
use crate::schema::{
    registered_schema_entity_pk, schema_key_from_definition, validate_lix_schema_definition,
};
use crate::session::scan_filesystem_rows;
use crate::storage::StorageBackend;
use crate::transaction::Transaction;
use crate::transaction::types::{
    TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteRow,
};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

pub(crate) async fn install_plugin_archive_with_transaction<B>(
    parsed: &ParsedPluginArchive,
    archive_bytes: &[u8],
    transaction: &mut Transaction<B>,
) -> Result<(), LixError>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    stage_plugin_archive_file(transaction, &parsed, archive_bytes).await?;
    Ok(())
}

pub(crate) fn plugin_schema_rows_from_archive_path(
    archive_path: &str,
    archive_bytes: &[u8],
    branch_id: &str,
    global: bool,
    untracked: bool,
) -> Result<Vec<TransactionWriteRow>, LixError> {
    let plugin_key = plugin_key_from_archive_path(archive_path).ok_or_else(|| {
        LixError::new(
            LixError::CODE_CONSTRAINT_VIOLATION,
            format!("plugin archive path '{archive_path}' is not a valid plugin storage path"),
        )
    })?;
    let parsed = parse_plugin_archive_for_install(archive_bytes)?;
    if parsed.manifest.key != plugin_key {
        return Err(LixError::new(
            LixError::CODE_CONSTRAINT_VIOLATION,
            format!(
                "plugin archive path key '{}' does not match manifest key '{}'",
                plugin_key, parsed.manifest.key
            ),
        ));
    }
    plugin_schema_rows(&parsed, branch_id, global, untracked)
}

async fn stage_plugin_archive_file<B>(
    transaction: &mut Transaction<B>,
    parsed: &ParsedPluginArchive,
    archive_bytes: &[u8],
) -> Result<(), LixError>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    let branch_id = transaction.active_branch_id().to_string();
    let filesystem_rows = scan_filesystem_rows(transaction, &branch_id).await?;
    let mut resolvers = directory_path_resolvers_from_state_rows(filesystem_rows)?;
    let resolver_key = filesystem_storage_scope_key(&branch_id, false, false, None);
    let resolver = resolvers
        .entry(resolver_key)
        .or_insert_with(DirectoryPathResolver::default);
    let archive_id = plugin_storage_archive_file_id(parsed.manifest.key.as_str());
    let archive_path = plugin_storage_archive_path(parsed.manifest.key.as_str())?;
    let plan = plan_file_path_write(
        resolver,
        FilePathWriteInput {
            id: Some(archive_id),
            path: archive_path,
            data: Some(archive_bytes.to_vec()),
            context: FilesystemRowContext {
                branch_id,
                global: false,
                untracked: false,
                file_id: None,
                metadata: None,
            },
        },
        &mut || transaction.functions().call_uuid_v7().to_string(),
    )?;

    transaction
        .stage_write(TransactionWrite::RowsWithFileData {
            mode: TransactionWriteMode::Replace,
            rows: plan.rows,
            file_data: plan.file_data,
            count: plan.count,
        })
        .await?;
    Ok(())
}

fn plugin_schema_rows(
    parsed: &ParsedPluginArchive,
    branch_id: &str,
    global: bool,
    untracked: bool,
) -> Result<Vec<TransactionWriteRow>, LixError> {
    parsed
        .schemas
        .iter()
        .map(|schema| registered_schema_row(schema, branch_id, global, untracked))
        .collect()
}

fn registered_schema_row(
    schema: &JsonValue,
    branch_id: &str,
    global: bool,
    untracked: bool,
) -> Result<TransactionWriteRow, LixError> {
    validate_lix_schema_definition(schema)?;
    let schema_key = schema_key_from_definition(schema)?;
    let entity_pk = registered_schema_entity_pk(&schema_key.schema_key)?;
    Ok(TransactionWriteRow {
        entity_pk: Some(entity_pk),
        schema_key: REGISTERED_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value(
            json!({ "value": schema }),
            "plugin install registered schema snapshot",
        )?),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global,
        change_id: None,
        commit_id: None,
        untracked,
        branch_id: branch_id.to_string(),
    })
}
