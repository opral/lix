use std::collections::BTreeMap;
use std::ops::Bound;

use crate::LixError;
use crate::changelog::ChangeRecord;
use crate::hot_state::ROW_SPACE;
use crate::json_store::{JsonLoadRequestRef, JsonReadScopeRef, JsonStoreContext, LegacyJsonValue};
use crate::migration::publish::PublicationPlan;
use crate::migration::row_rewrite::{
    HistoricalSchemaCatalog, MaterializedV68Change, RewrittenChange,
};
use crate::migration::v68::{
    V68ChangeRecord, V68HotStateSlot, V68HotStateValue, V68WorkingDiffBaseline, V68WorkingDiffSlot,
    V68WorkingDiffVersion, decode_hot_state_value,
};
use crate::storage_adapter::{
    StorageAdapterRead, StorageBeginScanOptions, StorageCoreProjection, StorageKeyRange,
    StorageProjectedValue,
};

const VERSION: u8 = 11;

pub(super) async fn plan_hot_rows(
    read: &(impl StorageAdapterRead + ?Sized),
    rewritten: &[RewrittenChange],
    catalog: &HistoricalSchemaCatalog,
    max_rows: usize,
    publication: &mut PublicationPlan,
) -> Result<u64, LixError> {
    let changes = rewritten
        .iter()
        .map(|change| (change.record.change_id, &change.record))
        .collect::<BTreeMap<_, _>>();
    let mut cursor = read
        .begin_scan(
            ROW_SPACE,
            StorageKeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    let mut rows = Vec::new();
    while let Some(entries) = cursor.next_chunk().await? {
        for entry in entries {
            if rows.len() >= max_rows {
                return Err(LixError::new(
                    "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
                    "HOT rows exceed the configured migration row bound",
                ));
            }
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(error("HOT row scan omitted a value"));
            };
            let old = decode_hot_state_value(&value)?;
            let owned;
            let change = if let Some(change) = changes.get(&old.change_id).copied() {
                change
            } else if old.untracked {
                let key = crate::hot_state::decode_hot_row_key_for_migration(entry.key.0.as_ref())?;
                owned = catalog.rewrite(&MaterializedV68Change {
                    snapshot_json: materialize_hot_slot(read, &old.snapshot).await?,
                    metadata_json: materialize_hot_slot(read, &old.metadata).await?,
                    record: V68ChangeRecord {
                        format_version: 1,
                        change_id: old.change_id,
                        account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
                        schema_key: key.schema_key,
                        row_pk: key.row_pk,
                        file_id: key.file_id,
                        snapshot: hot_slot_to_json_slot(&old.snapshot),
                        metadata: hot_slot_to_json_slot(&old.metadata),
                        created_at: old.created_at,
                        origin_key: None,
                    },
                })?;
                &owned.record
            } else {
                return Err(error(format!(
                    "HOT row references missing change '{}'",
                    old.change_id
                )));
            };
            rows.push((entry.key.0.to_vec(), encode_v11(&old, change)?));
        }
    }
    let count = rows.len() as u64;
    publication.put_mutable(ROW_SPACE, rows)?;
    Ok(count)
}

fn hot_slot_to_json_slot(slot: &V68HotStateSlot) -> LegacyJsonValue {
    match slot {
        V68HotStateSlot::None => LegacyJsonValue::None,
        V68HotStateSlot::Ref(reference) => LegacyJsonValue::Ref(*reference),
        V68HotStateSlot::Inline(json) | V68HotStateSlot::InlineFingerprinted { json, .. } => {
            LegacyJsonValue::Inline(json.clone().into())
        }
    }
}

async fn materialize_hot_slot(
    read: &(impl StorageAdapterRead + ?Sized),
    slot: &V68HotStateSlot,
) -> Result<Option<String>, LixError> {
    match slot {
        V68HotStateSlot::None => Ok(None),
        V68HotStateSlot::Inline(json) | V68HotStateSlot::InlineFingerprinted { json, .. } => {
            Ok(Some(json.clone()))
        }
        V68HotStateSlot::Ref(reference) => {
            let bytes = JsonStoreContext::new()
                .load_bytes_many(
                    read,
                    JsonLoadRequestRef {
                        refs: std::slice::from_ref(reference),
                        scope: JsonReadScopeRef::OutOfBand,
                    },
                )
                .await?
                .into_values()
                .into_iter()
                .next()
                .flatten()
                .ok_or_else(|| error("HOT row references missing JSON"))?;
            String::from_utf8(bytes.to_vec())
                .map(Some)
                .map_err(|error_value| error(format!("HOT row JSON is not UTF-8: {error_value}")))
        }
    }
}

fn encode_v11(old: &V68HotStateValue, change: &ChangeRecord) -> Result<Vec<u8>, LixError> {
    let metadata = if old.deleted {
        None
    } else {
        change
            .metadata
            .as_ref()
            .map(lix_schema::Jsonb::binary)
            .transpose()
            .map_err(|error_value| {
                error(format!("cannot encode HOT JSONB metadata: {error_value}"))
            })?
    };
    let typed = if old.deleted {
        None
    } else {
        Some(
            crate::plugin::runtime::compress_durable_payload(
                change
                    .snapshot
                    .clone()
                    .ok_or_else(|| error("live HOT row has no typed payload"))?,
            )
            .map_err(|error_value| {
                error(format!(
                    "cannot compress HOT typed payload: {error_value:?}"
                ))
            })?,
        )
    };
    let metadata_len = metadata.as_deref().map_or(0, <[u8]>::len);
    let typed_len = typed.as_deref().map_or(0, <[u8]>::len);
    let mut flags = u8::from(old.deleted);
    if old.untracked {
        flags |= 0b0010_0000;
    }
    flags |= baseline_tag(old.working_diff_baseline) << 6;

    let mut bytes = Vec::new();
    bytes.push(VERSION);
    bytes.push(flags);
    bytes.extend_from_slice(old.change_id.as_uuid().as_bytes());
    bytes.extend_from_slice(old.commit_id.unwrap_or_default().as_uuid().as_bytes());
    bytes.extend_from_slice(&old.created_at.packed().to_be_bytes());
    bytes.extend_from_slice(&old.updated_at.packed().to_be_bytes());
    bytes.extend_from_slice(&u32_len(metadata_len, "metadata")?.to_be_bytes());
    bytes.push(u8::from(old.columnar_base_coordinate.is_some()));
    bytes.extend_from_slice(&u32_len(typed_len, "typed payload")?.to_be_bytes());
    if let Some(metadata) = metadata.as_deref() {
        bytes.extend_from_slice(metadata);
    }
    append_baseline(&mut bytes, old.working_diff_baseline);
    if let Some(coordinate) = old.columnar_base_coordinate {
        bytes.extend_from_slice(coordinate.base_commit_id.as_uuid().as_bytes());
        bytes.extend_from_slice(&coordinate.group_index.to_be_bytes());
        bytes.extend_from_slice(&coordinate.row_index.to_be_bytes());
    }
    if let Some(typed) = typed.as_deref() {
        bytes.extend_from_slice(typed);
    }
    Ok(bytes)
}

fn baseline_tag(baseline: V68WorkingDiffBaseline) -> u8 {
    match baseline {
        V68WorkingDiffBaseline::Disabled => 0,
        V68WorkingDiffBaseline::Clean => 1,
        V68WorkingDiffBaseline::BeforeAbsent { .. } => 2,
        V68WorkingDiffBaseline::BeforePresent { .. } => 3,
    }
}

fn append_baseline(bytes: &mut Vec<u8>, baseline: V68WorkingDiffBaseline) {
    match baseline {
        V68WorkingDiffBaseline::Disabled | V68WorkingDiffBaseline::Clean => {}
        V68WorkingDiffBaseline::BeforeAbsent {
            checkpoint_commit_id,
        } => bytes.extend_from_slice(checkpoint_commit_id.as_uuid().as_bytes()),
        V68WorkingDiffBaseline::BeforePresent {
            checkpoint_commit_id,
            mut version,
        } => {
            bytes.extend_from_slice(checkpoint_commit_id.as_uuid().as_bytes());
            // v68 stored the before-image snapshot fingerprint over JSON
            // bytes. v69 compares the native typed payload bytes instead, so
            // carrying the old hash forward would report a false modification
            // after a row is changed and then restored. Preserve the change
            // identity and force the existing lazy hydration path to resolve
            // its fingerprint from the rewritten ChangeRecord.
            version.snapshot = V68WorkingDiffSlot::Unresolved;
            append_version(bytes, version);
        }
    }
}

fn append_version(bytes: &mut Vec<u8>, version: V68WorkingDiffVersion) {
    bytes.extend_from_slice(version.change_id.as_uuid().as_bytes());
    bytes.extend_from_slice(version.commit_id.as_uuid().as_bytes());
    bytes.push(u8::from(version.deleted));
    bytes.extend_from_slice(&version.created_at.packed().to_be_bytes());
    bytes.extend_from_slice(&version.updated_at.packed().to_be_bytes());
    append_working_slot(bytes, version.snapshot);
    append_working_slot(bytes, version.metadata);
}

fn append_working_slot(bytes: &mut Vec<u8>, slot: V68WorkingDiffSlot) {
    match slot {
        V68WorkingDiffSlot::None => {
            bytes.push(0);
            bytes.extend_from_slice(&[0; 32]);
        }
        V68WorkingDiffSlot::Ref(reference) => {
            bytes.push(1);
            bytes.extend_from_slice(reference.as_hash_bytes());
        }
        V68WorkingDiffSlot::Inline(reference) => {
            bytes.push(2);
            bytes.extend_from_slice(reference.as_hash_bytes());
        }
        V68WorkingDiffSlot::Unresolved => {
            bytes.push(3);
            bytes.extend_from_slice(&[0; 32]);
        }
    }
}

fn u32_len(value: usize, label: &str) -> Result<u32, LixError> {
    u32::try_from(value).map_err(|_| error(format!("HOT {label} exceeds u32")))
}

fn error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_ERROR_MIGRATION_FAILED", message.into())
}
