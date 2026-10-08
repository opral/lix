//! Executable inputs belonging to rows returned by a safe current SQL read.
use super::{PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginFileOwner, PluginRegistry};
use crate::binary_cas::{BlobDataReader, BlobId};
use crate::hot_state::{
    HotStateExactBatchRequest, HotStateExactRowRequest, HotStateProjection, HotStateReader,
};
use crate::{LixError, row_pk::RowPk};
use std::collections::{BTreeMap, BTreeSet};

const REGISTRY_EXACT_BATCH_ROWS: usize = 4;
const OWNER_EXACT_BATCH_ROWS: usize = 32;
const MAX_PLUGIN_OWNER_BATCH_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
const MAX_PLUGIN_OWNER_BATCH_DECODED_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const MAX_EXECUTABLE_OWNER_ROWS: usize = 4096;
const MAX_EXECUTABLE_SCHEMA_REFERENCES: usize = 4096;

#[derive(Debug)]
pub(crate) struct PluginOwnerLookupRow {
    pub(crate) owner: PluginFileOwner,
    pub(crate) change_id: Option<String>,
}

pub(crate) fn executable_dependency_work_bound() -> LixError {
    LixError::new(
        "LIX_NATIVE_RECIPE_WORK_BOUND",
        "returned plugin dependency count limit exceeded",
    )
}

fn executable_dependency_payload_bound() -> LixError {
    LixError::new(
        "LIX_NATIVE_RECIPE_WORK_BOUND",
        "returned plugin dependency payload exceeds its byte bound",
    )
}

pub(crate) async fn prepare_returned_row_executables(
    reader: &dyn HotStateReader,
    blobs: &dyn BlobDataReader,
    rows: &[(String, crate::tracked_state::TrackedStateKey)],
) -> Result<(), LixError> {
    // Every transaction opens the current plugin registry, including writes
    // to fileless user schemas. Keep this dependency in the plugin owner and
    // resolve only exact branches represented by the completed current read.
    let mut branches = BTreeSet::new();
    let mut files = BTreeMap::<String, BTreeMap<String, BTreeSet<String>>>::new();
    let mut file_count = 0usize;
    let mut schema_references = 0usize;
    for (branch, key) in rows {
        if !branches.contains(branch) {
            if branches.len() >= MAX_EXECUTABLE_OWNER_ROWS {
                return Err(executable_dependency_work_bound());
            }
            branches.insert(branch.clone());
        }
        if let Some(file) = &key.file_id {
            let already_selected = files
                .get(branch)
                .is_some_and(|branch_files| branch_files.contains_key(file));
            if !already_selected && file_count >= MAX_EXECUTABLE_OWNER_ROWS {
                return Err(executable_dependency_work_bound());
            }
            let schemas = files
                .entry(branch.clone())
                .or_default()
                .entry(file.clone())
                .or_default();
            if !schemas.contains(&key.schema_key) {
                schema_references = schema_references
                    .checked_add(1)
                    .ok_or_else(executable_dependency_work_bound)?;
                if schema_references > MAX_EXECUTABLE_SCHEMA_REFERENCES {
                    return Err(executable_dependency_work_bound());
                }
                schemas.insert(key.schema_key.clone());
            }
            if !already_selected {
                file_count += 1;
            }
        }
    }

    let branches = branches.into_iter().collect::<Vec<_>>();
    let registries = load_plugin_registry_pages(reader, &branches).await?;
    let owner_targets = files
        .iter()
        .flat_map(|(branch, branch_files)| {
            branch_files
                .keys()
                .map(move |file| (branch.clone(), file.clone()))
        })
        .collect::<Vec<_>>();
    let owners = load_plugin_owner_pages(reader, &owner_targets).await?;
    let mut hashes = BTreeSet::new();
    for ((branch, file), loaded) in owners {
        let Some(schemas) = files.get(&branch).and_then(|files| files.get(&file)) else {
            continue;
        };
        if !loaded
            .owner
            .schema_keys()
            .iter()
            .any(|schema| schemas.contains(schema))
        {
            continue;
        }
        let Some(plugin) = registries
            .get(&branch)
            .and_then(|registry| registry.get(loaded.owner.plugin_key()))
        else {
            continue;
        };
        if let Some(hash) = plugin.wasm_blob_hash() {
            let hash = BlobId::from_hex(hash)?;
            if !hashes.contains(&hash)
                && hashes.len() >= crate::binary_cas::MAX_REFERENCED_BLOB_HASHES
            {
                return Err(crate::binary_cas::work_bound_error());
            }
            hashes.insert(hash);
        }
    }
    prepare_executable_blobs(blobs, hashes).await
}

/// Load selected branch registries through bounded raw-only exact pages. The
/// aggregate raw and declared-decoded-byte checks run before payload parsing.
pub(crate) async fn load_plugin_registry_pages(
    reader: &dyn HotStateReader,
    branches: &[String],
) -> Result<BTreeMap<String, PluginRegistry>, LixError> {
    if branches.len() > MAX_EXECUTABLE_OWNER_ROWS {
        return Err(executable_dependency_work_bound());
    }
    let branches = branches.iter().cloned().collect::<BTreeSet<_>>();
    if branches.len() > MAX_EXECUTABLE_OWNER_ROWS {
        return Err(executable_dependency_work_bound());
    }
    let branches = branches.into_iter().collect::<Vec<_>>();
    let mut registries = BTreeMap::new();
    let mut total_bytes = 0usize;
    let mut total_decoded_bytes = 0usize;
    for page in branches.chunks(REGISTRY_EXACT_BATCH_ROWS) {
        let rows = reader
            .load_exact_batch(&HotStateExactBatchRequest {
                rows: page
                    .iter()
                    .map(|branch| HotStateExactRowRequest {
                        schema_key: "lix_key_value".into(),
                        branch_id: branch.clone(),
                        row_pk: RowPk::single(PLUGIN_REGISTRY_KEY),
                        file_id: None,
                    })
                    .collect(),
                projection: HotStateProjection {
                    columns: vec!["raw_snapshot".into()],
                },
                untracked: Some(false),
                include_tombstones: false,
            })
            .await?;
        if rows.len() != page.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin registry exact batch returned an invalid result cardinality",
            ));
        }
        let (page_bytes, page_decoded_bytes) = raw_page_sizes(
            rows.len(),
            crate::plugin::runtime::MAX_PLUGIN_REGISTRY_DURABLE_ROW_BYTES,
            |slot| rows.row(slot),
        )?;
        total_bytes = total_bytes
            .checked_add(page_bytes)
            .filter(|bytes| *bytes <= crate::plugin::runtime::MAX_PLUGIN_REGISTRY_SNAPSHOT_BYTES)
            .ok_or_else(executable_dependency_payload_bound)?;
        total_decoded_bytes = total_decoded_bytes
            .checked_add(page_decoded_bytes)
            .filter(|bytes| *bytes <= crate::plugin::runtime::MAX_PLUGIN_REGISTRY_SNAPSHOT_BYTES)
            .ok_or_else(executable_dependency_payload_bound)?;
        for (slot, branch) in page.iter().enumerate() {
            registries.insert(
                branch.clone(),
                PluginRegistry::from_optional_hot_state_row(rows.row(slot), branch)?,
            );
        }
    }
    Ok(registries)
}

/// Load exact plugin owners through bounded raw-only pages, retaining only
/// their validated owner and revision metadata after each page is decoded.
pub(crate) async fn load_plugin_owner_pages(
    reader: &dyn HotStateReader,
    targets: &[(String, String)],
) -> Result<BTreeMap<(String, String), PluginOwnerLookupRow>, LixError> {
    if targets.len() > MAX_EXECUTABLE_OWNER_ROWS {
        return Err(executable_dependency_work_bound());
    }
    let targets = targets.iter().cloned().collect::<BTreeSet<_>>();
    if targets.len() > MAX_EXECUTABLE_OWNER_ROWS {
        return Err(executable_dependency_work_bound());
    }
    let targets = targets.into_iter().collect::<Vec<_>>();
    let mut owners = BTreeMap::new();
    let mut total_bytes = 0usize;
    let mut total_decoded_bytes = 0usize;
    let owner_total_byte_limit = MAX_PLUGIN_OWNER_BATCH_SNAPSHOT_BYTES;
    for page in targets.chunks(OWNER_EXACT_BATCH_ROWS) {
        let rows = reader
            .load_exact_batch(&HotStateExactBatchRequest {
                rows: page
                    .iter()
                    .map(|(branch, file)| HotStateExactRowRequest {
                        schema_key: "lix_key_value".into(),
                        branch_id: branch.clone(),
                        row_pk: RowPk::single(PLUGIN_OWNER_KEY),
                        file_id: Some(file.clone()),
                    })
                    .collect(),
                projection: HotStateProjection {
                    columns: vec!["raw_snapshot".into()],
                },
                untracked: Some(false),
                include_tombstones: false,
            })
            .await?;
        if rows.len() != page.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin owner exact batch returned an invalid result cardinality",
            ));
        }
        let (page_bytes, page_decoded_bytes) = raw_page_sizes(
            rows.len(),
            crate::plugin::runtime::MAX_PLUGIN_OWNER_SNAPSHOT_BYTES,
            |slot| rows.row(slot),
        )?;
        total_bytes = total_bytes
            .checked_add(page_bytes)
            .filter(|bytes| *bytes <= owner_total_byte_limit)
            .ok_or_else(executable_dependency_payload_bound)?;
        total_decoded_bytes = total_decoded_bytes
            .checked_add(page_decoded_bytes)
            .filter(|bytes| *bytes <= MAX_PLUGIN_OWNER_BATCH_DECODED_BYTES)
            .ok_or_else(executable_dependency_payload_bound)?;
        for (slot, (branch, file)) in page.iter().enumerate() {
            let Some(row) = rows.row(slot) else {
                continue;
            };
            if row.file_id() != Some(file.as_str()) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "plugin owner exact batch returned a row for the wrong file identity",
                ));
            }
            let Some(owner) = PluginFileOwner::from_hot_state_row_ref(row, branch, false)? else {
                continue;
            };
            owners.insert(
                (branch.clone(), file.clone()),
                PluginOwnerLookupRow {
                    owner,
                    change_id: row.change_id().map(|change_id| change_id.to_string()),
                },
            );
        }
    }
    Ok(owners)
}

fn raw_page_sizes<'a>(
    row_count: usize,
    max_decoded_row_bytes: usize,
    mut row_at: impl FnMut(usize) -> Option<crate::hot_state::MaterializedHotStateRowRef<'a>>,
) -> Result<(usize, usize), LixError> {
    let mut raw_bytes = 0usize;
    let mut decoded_bytes = 0usize;
    for slot in 0..row_count {
        let Some(row) = row_at(slot) else {
            continue;
        };
        let deleted = row.deleted();
        let raw = row.raw_snapshot();
        if !deleted && raw.is_none() {
            return Err(executable_dependency_payload_bound());
        }
        let Some(raw) = raw else {
            continue;
        };
        raw_bytes = raw_bytes
            .checked_add(raw.len())
            .ok_or_else(executable_dependency_payload_bound)?;
        if !deleted {
            let declared_decoded_bytes =
                crate::row_payload::durable_payload_declared_decoded_len_bounded(
                    raw.as_ref(),
                    max_decoded_row_bytes,
                )?;
            decoded_bytes = decoded_bytes
                .checked_add(declared_decoded_bytes)
                .ok_or_else(executable_dependency_payload_bound)?;
        }
    }
    Ok((raw_bytes, decoded_bytes))
}

pub(crate) async fn prepare_executable_blobs(
    blobs: &dyn BlobDataReader,
    hashes: impl IntoIterator<Item = BlobId>,
) -> Result<(), LixError> {
    if !blobs.requires_referenced_content_preparation() {
        return Ok(());
    }
    let mut selected = BTreeSet::new();
    let mut seen = 0usize;
    for hash in hashes {
        seen = seen
            .checked_add(1)
            .ok_or_else(crate::binary_cas::work_bound_error)?;
        if seen > crate::binary_cas::MAX_REFERENCED_BLOB_HASHES {
            return Err(crate::binary_cas::work_bound_error());
        }
        selected.insert(hash);
    }
    let hashes = selected.into_iter().collect::<Vec<_>>();
    blobs.require_referenced_content(&hashes).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binary_cas::{BlobBytesBatch, BlobDataReader};
    use crate::common::LixTimestamp;
    use crate::hot_state::{
        HotStateExactBatchRequest, MaterializedHotStateBatch, MaterializedHotStateBatchBuilder,
        MaterializedHotStateExactBatch, MaterializedHotStateRow,
    };
    use crate::plugin::runtime::{PluginFileOwner, WasmTypedRow};
    use crate::tracked_state::TrackedStateKey;
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct RecordingHotStateReader {
        requests: Mutex<Vec<HotStateExactBatchRequest>>,
        owner_payload_override: Mutex<Option<Bytes>>,
        owner_payload_pages: Mutex<VecDeque<Vec<Bytes>>>,
    }

    #[async_trait]
    impl HotStateReader for RecordingHotStateReader {
        async fn scan_batch(
            &self,
            _request: &crate::hot_state::HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(MaterializedHotStateBatch::default())
        }

        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<MaterializedHotStateExactBatch, LixError> {
            self.requests.lock().unwrap().push(request.clone());
            let owner_page_override = request
                .rows
                .first()
                .is_some_and(|identity| {
                    identity.row_pk.as_single_string().ok() == Some(PLUGIN_OWNER_KEY)
                })
                .then(|| self.owner_payload_pages.lock().unwrap().pop_front())
                .flatten();
            let owner_payload_override = self.owner_payload_override.lock().unwrap().clone();
            let mut builder = MaterializedHotStateBatchBuilder::with_capacity(request.rows.len());
            let mut slots = Vec::with_capacity(request.rows.len());
            for (slot, identity) in request.rows.iter().enumerate() {
                if identity.row_pk.as_single_string().ok() == Some(PLUGIN_OWNER_KEY) {
                    let (row, raw) = owner_row(identity);
                    let raw = owner_page_override
                        .as_ref()
                        .and_then(|page| page.get(slot))
                        .cloned()
                        .or_else(|| owner_payload_override.clone())
                        .unwrap_or(raw);
                    let ordinal = builder.len();
                    builder.push_owned(row);
                    builder.set_raw_snapshot(ordinal, Some(raw));
                    slots.push(Some(u32::try_from(ordinal).unwrap()));
                } else {
                    slots.push(None);
                }
            }
            Ok(MaterializedHotStateExactBatch::new(builder.finish(), slots).unwrap())
        }
    }

    fn owner_row(identity: &HotStateExactRowRequest) -> (MaterializedHotStateRow, Bytes) {
        let file_id = identity
            .file_id
            .as_deref()
            .expect("owner lookup is file-scoped");
        let owner =
            PluginFileOwner::new(file_id, "uninstalled_plugin", vec!["plugin_schema".into()])
                .unwrap();
        let typed = WasmTypedRow::from_builtin_json(
            "lix_key_value",
            &identity.row_pk,
            &owner.to_snapshot().unwrap(),
        )
        .unwrap();
        let raw = Bytes::from_owner(typed.durable_payload().unwrap());
        let timestamp = LixTimestamp::expect_parse("timestamp", "2026-01-01T00:00:00.000Z");
        (
            MaterializedHotStateRow {
                row_pk: identity.row_pk.clone(),
                schema_key: identity.schema_key.clone(),
                file_id: identity.file_id.clone(),
                snapshot_content: None,
                metadata: None,
                deleted: false,
                created_at: timestamp,
                updated_at: timestamp,
                global: false,
                change_id: None,
                commit_id: None,
                author_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
                untracked: false,
                branch_id: Arc::from(identity.branch_id.as_str()),
            },
            raw,
        )
    }

    #[derive(Default)]
    struct RecordingBlobReader {
        calls: Mutex<Vec<Vec<BlobId>>>,
        content_loads: Mutex<Vec<Vec<BlobId>>>,
    }

    #[async_trait]
    impl BlobDataReader for RecordingBlobReader {
        fn requires_referenced_content_preparation(&self) -> bool {
            true
        }

        async fn require_referenced_content(&self, hashes: &[BlobId]) -> Result<(), LixError> {
            self.calls.lock().unwrap().push(hashes.to_vec());
            Ok(())
        }

        async fn load_bytes_many(&self, hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError> {
            self.content_loads.lock().unwrap().push(hashes.to_vec());
            Ok(BlobBytesBatch::new(vec![None; hashes.len()]))
        }
    }

    #[tokio::test]
    async fn returned_owner_dependencies_use_aligned_bounded_exact_pages() {
        let reader = RecordingHotStateReader::default();
        let blobs = RecordingBlobReader::default();
        let rows = (0..40)
            .map(|index| {
                (
                    if index < 20 { "branch-a" } else { "branch-b" }.to_owned(),
                    TrackedStateKey {
                        schema_key: "plugin_schema".to_owned(),
                        file_id: Some(format!("file-{index:02}")),
                        row_pk: RowPk::single(format!("row-{index:02}")),
                    },
                )
            })
            .collect::<Vec<_>>();

        prepare_returned_row_executables(&reader, &blobs, &rows)
            .await
            .unwrap();

        let requests = reader.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].rows.len(), 2);
        assert_eq!(requests[0].projection.columns, ["raw_snapshot"]);
        assert!(
            requests[0]
                .rows
                .iter()
                .all(|row| row.schema_key == "lix_key_value"
                    && row.row_pk.as_single_string().ok() == Some(PLUGIN_REGISTRY_KEY)
                    && row.file_id.is_none())
        );
        let owner_requests = &requests[1..];
        assert_eq!(
            owner_requests
                .iter()
                .map(|request| request.rows.len())
                .collect::<Vec<_>>(),
            [32, 8]
        );
        assert!(
            owner_requests
                .iter()
                .all(|request| request.projection.columns == ["raw_snapshot"])
        );
        let actual = owner_requests
            .iter()
            .flat_map(|request| {
                assert!(!request.include_tombstones);
                assert_eq!(request.untracked, Some(false));
                request.rows.iter().map(|row| {
                    assert_eq!(row.schema_key, "lix_key_value");
                    assert_eq!(row.row_pk.as_single_string().ok(), Some(PLUGIN_OWNER_KEY));
                    (row.branch_id.clone(), row.file_id.clone().unwrap())
                })
            })
            .collect::<Vec<_>>();
        let expected = rows
            .iter()
            .map(|(branch, key)| (branch.clone(), key.file_id.clone().unwrap()))
            .collect::<BTreeSet<_>>();
        assert_eq!(actual.into_iter().collect::<BTreeSet<_>>(), expected);
        drop(requests);
        assert_eq!(*blobs.calls.lock().unwrap(), vec![Vec::<BlobId>::new()]);
        assert!(blobs.content_loads.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn registry_exact_pages_bound_the_raw_decode_admission() {
        let reader = RecordingHotStateReader::default();
        let blobs = RecordingBlobReader::default();
        let rows = (0..5)
            .map(|index| {
                (
                    format!("branch-{index}"),
                    TrackedStateKey {
                        schema_key: "plugin_schema".to_owned(),
                        file_id: Some(format!("file-{index}")),
                        row_pk: RowPk::single(format!("row-{index}")),
                    },
                )
            })
            .collect::<Vec<_>>();

        prepare_returned_row_executables(&reader, &blobs, &rows)
            .await
            .unwrap();

        let requests = reader.requests.lock().unwrap();
        let registry_pages = requests
            .iter()
            .filter(|request| {
                request.rows.first().is_some_and(|row| {
                    row.row_pk.as_single_string().ok() == Some(PLUGIN_REGISTRY_KEY)
                })
            })
            .map(|request| request.rows.len())
            .collect::<Vec<_>>();
        assert_eq!(registry_pages, [4, 1]);
        assert!(
            requests
                .iter()
                .all(|request| request.projection.columns == ["raw_snapshot"])
        );
    }

    #[tokio::test]
    async fn owner_page_rejects_oversized_compressed_snapshot_before_decode() {
        let reader = RecordingHotStateReader::default();
        let mut oversized = vec![crate::row_payload::COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION];
        oversized.extend_from_slice(
            &((crate::plugin::runtime::MAX_PLUGIN_OWNER_SNAPSHOT_BYTES + 1) as u32).to_le_bytes(),
        );
        oversized.push(0);
        *reader.owner_payload_override.lock().unwrap() = Some(Bytes::from(oversized));

        let error =
            load_plugin_owner_pages(&reader, &[("branch-a".to_owned(), "file-a".to_owned())])
                .await
                .expect_err("compressed owner row over the decoded byte bound must fail closed");

        assert!(error.message.contains("decoded byte bound"), "{error:?}");
        let requests = reader.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].projection.columns, ["raw_snapshot"]);
    }

    #[tokio::test]
    async fn owner_pages_reserve_aggregate_decoded_bytes_before_inflating_next_page() {
        let reader = RecordingHotStateReader::default();
        let schema_keys = (0..64)
            .map(|index| format!("{}-{index:02}", "s".repeat(509)))
            .collect::<Vec<_>>();
        let owner = PluginFileOwner::new("file-000", "uninstalled_plugin", schema_keys).unwrap();
        let typed = WasmTypedRow::from_builtin_json(
            "lix_key_value",
            &RowPk::single(PLUGIN_OWNER_KEY),
            &owner.to_snapshot().unwrap(),
        )
        .unwrap();
        let valid_payload = Bytes::from_owner(typed.durable_payload().unwrap());
        assert_eq!(
            valid_payload.first(),
            Some(&crate::row_payload::COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION),
            "large repetitive owner snapshots should use the compressed durable frame",
        );
        let decoded_len = crate::row_payload::durable_payload_declared_decoded_len_bounded(
            valid_payload.as_ref(),
            crate::plugin::runtime::MAX_PLUGIN_OWNER_SNAPSHOT_BYTES,
        )
        .unwrap();
        assert!(decoded_len * 32 < MAX_PLUGIN_OWNER_BATCH_DECODED_BYTES);
        assert!(decoded_len * 64 > MAX_PLUGIN_OWNER_BATCH_DECODED_BYTES);

        let mut malformed_second_page =
            vec![crate::row_payload::COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION];
        malformed_second_page.extend_from_slice(
            &(crate::plugin::runtime::MAX_PLUGIN_OWNER_SNAPSHOT_BYTES as u32).to_le_bytes(),
        );
        malformed_second_page.push(0);
        *reader.owner_payload_pages.lock().unwrap() = VecDeque::from([
            vec![valid_payload; OWNER_EXACT_BATCH_ROWS],
            vec![Bytes::from(malformed_second_page); OWNER_EXACT_BATCH_ROWS],
        ]);

        let targets = (0..OWNER_EXACT_BATCH_ROWS * 2)
            .map(|index| ("branch-a".to_owned(), format!("file-{index:03}")))
            .collect::<Vec<_>>();
        let error = load_plugin_owner_pages(&reader, &targets)
            .await
            .expect_err("aggregate decoded-byte budget should reject the second page");

        assert!(
            error.message.contains("payload exceeds its byte bound"),
            "the aggregate reservation must reject before decoding the malformed second page: {error:?}"
        );
        assert_eq!(reader.requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn executable_roots_are_prepared_in_one_shared_content_request() {
        let first = BlobId::from_content(b"first wasm");
        let second = BlobId::from_content(b"second wasm");
        let blobs = RecordingBlobReader::default();

        prepare_executable_blobs(&blobs, [second, first, second])
            .await
            .unwrap();

        assert_eq!(
            *blobs.calls.lock().unwrap(),
            vec![vec![first.min(second), first.max(second)]]
        );
    }
}
