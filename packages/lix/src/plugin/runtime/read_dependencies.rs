//! Executable inputs belonging to rows returned by a safe current SQL read.
use super::{PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginFileOwner, PluginRegistry};
use crate::binary_cas::{BlobDataReader, BlobId};
use crate::hot_state::{
    HotStateExactBatchRequest, HotStateExactRowRequest, HotStateProjection, HotStateReader,
};
use crate::{LixError, row_pk::RowPk};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) async fn prepare_returned_row_executables(
    reader: &dyn HotStateReader,
    blobs: &dyn BlobDataReader,
    rows: &[(String, crate::tracked_state::TrackedStateKey)],
) -> Result<(), LixError> {
    // Every transaction opens the current plugin registry, including writes
    // to fileless user schemas. Keep this dependency in the plugin owner and
    // resolve only exact branches represented by the completed current read.
    let mut registries = BTreeMap::new();
    for branch in rows
        .iter()
        .map(|(branch, _)| branch)
        .collect::<BTreeSet<_>>()
    {
        let registry_rows = reader
            .load_exact_batch(&HotStateExactBatchRequest {
                rows: vec![HotStateExactRowRequest {
                    schema_key: "lix_key_value".into(),
                    branch_id: branch.clone(),
                    row_pk: RowPk::single(PLUGIN_REGISTRY_KEY),
                    file_id: None,
                }],
                projection: HotStateProjection {
                    columns: vec!["snapshot_content".into()],
                },
                untracked: Some(false),
                include_tombstones: false,
            })
            .await?;
        registries.insert(
            branch.clone(),
            PluginRegistry::from_optional_hot_state_row(registry_rows.row(0), branch)?,
        );
    }
    let mut files = BTreeMap::<(String, String), BTreeSet<String>>::new();
    for (branch, key) in rows {
        if let Some(file) = &key.file_id {
            files
                .entry((branch.clone(), file.clone()))
                .or_default()
                .insert(key.schema_key.clone());
        }
    }
    let mut hashes = BTreeSet::new();
    for ((branch, file), schemas) in files {
        let owner_rows = reader
            .load_exact_batch(&HotStateExactBatchRequest {
                rows: vec![HotStateExactRowRequest {
                    schema_key: "lix_key_value".into(),
                    branch_id: branch.clone(),
                    row_pk: RowPk::single(PLUGIN_OWNER_KEY),
                    file_id: Some(file),
                }],
                projection: HotStateProjection {
                    columns: vec!["snapshot_content".into()],
                },
                untracked: Some(false),
                include_tombstones: false,
            })
            .await?;
        let Some(row) = owner_rows.row(0) else {
            continue;
        };
        let Some(owner) = PluginFileOwner::from_hot_state_row(&row.to_owned(), &branch, false)?
        else {
            continue;
        };
        if !owner
            .schema_keys()
            .iter()
            .any(|schema| schemas.contains(schema))
        {
            continue;
        }
        let Some(plugin) = registries[&branch].get(owner.plugin_key()) else {
            continue;
        };
        if let Some(hash) = plugin.wasm_blob_hash() {
            hashes.insert(BlobId::from_hex(hash)?);
        }
    }
    prepare_executable_blobs(blobs, hashes).await
}

pub(crate) async fn prepare_executable_blobs(
    blobs: &dyn BlobDataReader,
    hashes: impl IntoIterator<Item = BlobId>,
) -> Result<(), LixError> {
    if !blobs.requires_referenced_content_preparation() {
        return Ok(());
    }
    for hash in hashes {
        blobs.require_referenced_content(&[hash]).await?;
    }
    Ok(())
}

/// A whole-file content read acknowledges the native state required by that
/// file's cold plugin actor. Unlike a schema-row read, its scope is the whole
/// returned document, but never another file or unrelated plugin schema.
pub(crate) async fn prepare_file_content_state(
    reader: &dyn HotStateReader,
    blobs: &dyn BlobDataReader,
    branch_id: &str,
    file_id: &str,
    schema_keys: &[String],
) -> Result<(), LixError> {
    if !blobs.requires_referenced_content_preparation() || schema_keys.is_empty() {
        return Ok(());
    }
    reader
        .scan_tracked_batch(&crate::hot_state::HotStateScanRequest {
            filter: crate::hot_state::HotStateFilter {
                schema_keys: schema_keys.to_vec(),
                branch_ids: vec![branch_id.to_owned()],
                file_ids: vec![crate::NullableKeyFilter::Value(file_id.to_owned())],
                untracked: Some(false),
                ..Default::default()
            },
            projection: super::plugin_state_hot_state_projection(),
            limit: None,
        })
        .await?;
    // This read is captured by the same pinned SQL reader. Its returned native
    // identities therefore pass existing mutation-input preparation before
    // the outer SELECT completes, and survive candidate replay/publication.
    Ok(())
}
