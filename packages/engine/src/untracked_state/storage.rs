use bytes::Bytes;

use crate::entity_identity::EntityIdentity;
use crate::storage::{
    PointReadPlan, ScanPlan, StorageCoreProjection, StorageGetOptions, StorageKey, StoragePrefix,
    StorageProjectedValue, StorageRead, StorageScanOptions, StorageSpace, StorageSpaceId,
    StorageValue, StorageWriteSet,
};
use crate::untracked_state::{
    MaterializedUntrackedStateRow, UntrackedMaterializationProjection, UntrackedStateIdentity,
    UntrackedStateIdentityRef, UntrackedStateRow, UntrackedStateRowRef, UntrackedStateRowRequest,
    UntrackedStateScanRequest,
};
use crate::{LixError, NullableKeyFilter};

pub(super) const UNTRACKED_STATE_ROW_NAMESPACE: &str = "untracked_state.row.v1";
pub(crate) const UNTRACKED_STATE_ROW_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0001_0002), UNTRACKED_STATE_ROW_NAMESPACE);
// Durable key bytes:
//   b"LXUK" | version:u8 |
//   version_id_len:u32be | version_id:utf8 |
//   schema_key_len:u32be | schema_key:utf8 |
//   entity_part_count:u32be | {entity_part_len:u32be | entity_part:utf8}* |
//   file_id_tag:u8 | [file_id_len:u32be | file_id:utf8]
const UNTRACKED_STATE_ROW_KEY_IDENTIFIER: &[u8; 4] = b"LXUK";
const UNTRACKED_STATE_ROW_KEY_VERSION_V1: u8 = 1;

pub(crate) async fn scan_rows(
    store: &impl StorageRead,
    request: &UntrackedStateScanRequest,
) -> Result<Vec<MaterializedUntrackedStateRow>, LixError> {
    let projection = UntrackedMaterializationProjection::from_columns(&request.projection.columns);
    let plans = scan_plans_for_request(request)?;
    let mut materialized = Vec::new();

    for plan in plans {
        scan_matching_rows(store, request, &projection, &plan, &mut materialized)?;
        if request
            .limit
            .is_some_and(|limit| materialized.len() >= limit)
        {
            break;
        }
    }

    Ok(materialized)
}

pub(crate) async fn load_row(
    store: &impl StorageRead,
    request: &UntrackedStateRowRequest,
) -> Result<Option<MaterializedUntrackedStateRow>, LixError> {
    let Some(identity) = identity_from_request(request) else {
        return Ok(None);
    };
    let result = PointReadPlan::new(
        UNTRACKED_STATE_ROW_SPACE,
        &[StorageKey(Bytes::from(encode_untracked_state_row_key(
            &identity,
        )?))],
    )
    .materialize(store, StorageGetOptions::default())?;
    let bytes = result
        .value
        .into_iter()
        .next()
        .flatten()
        .and_then(full_value);
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let row = crate::untracked_state::codec::decode_payload_with_identity(identity, &bytes)?;
    crate::untracked_state::materialize_row(row, &UntrackedMaterializationProjection::full())
        .map(Some)
}

pub(super) async fn existing_identities<'a>(
    store: &(impl StorageRead + ?Sized),
    identities: impl IntoIterator<Item = UntrackedStateIdentityRef<'a>>,
) -> Result<Vec<UntrackedStateIdentity>, LixError> {
    let mut candidates = identities
        .into_iter()
        .map(|identity| {
            let owned = UntrackedStateIdentity {
                version_id: identity.version_id.to_string(),
                schema_key: identity.schema_key.to_string(),
                entity_id: identity.entity_id.clone(),
                file_id: identity.file_id.map(str::to_string),
            };
            let key = encode_untracked_state_row_key_ref(owned.as_ref())?;
            Ok((key, owned))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    candidates.sort_by(|(left, _), (right, _)| left.cmp(right));
    candidates.dedup_by(|(left, _), (right, _)| left == right);
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let keys = candidates
        .iter()
        .map(|(key, _)| StorageKey(Bytes::from(key.clone())))
        .collect::<Vec<_>>();

    let result = PointReadPlan::from_unique_keys(UNTRACKED_STATE_ROW_SPACE, keys).materialize(
        store,
        StorageGetOptions {
            projection: StorageCoreProjection::KeyOnly,
            ..StorageGetOptions::default()
        },
    )?;
    let exists = result
        .value
        .into_iter()
        .map(|value| value.is_some())
        .collect::<Vec<_>>();
    if exists.len() != candidates.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "untracked identity existence probe returned {} results for {} requested keys",
                exists.len(),
                candidates.len()
            ),
        ));
    }

    Ok(candidates
        .into_iter()
        .zip(exists)
        .filter_map(|((_, identity), exists)| exists.then_some(identity))
        .collect())
}

pub(crate) fn stage_rows<'a, I>(writes: &mut StorageWriteSet, rows: I) -> Result<(), LixError>
where
    I: IntoIterator<Item = UntrackedStateRowRef<'a>>,
{
    for row in rows {
        if row.snapshot_content.is_none() {
            writes.delete(
                UNTRACKED_STATE_ROW_SPACE,
                StorageKey(Bytes::from(encode_untracked_state_row_key_ref(row.into())?)),
            );
        } else {
            writes.put(
                UNTRACKED_STATE_ROW_SPACE,
                StorageKey(Bytes::from(encode_untracked_state_row_key_ref(row.into())?)),
                StorageValue {
                    bytes: Bytes::from(crate::untracked_state::codec::encode_payload_ref(row)?),
                },
            );
        }
    }
    Ok(())
}

pub(crate) fn stage_delete_rows<'a, I>(
    writes: &mut StorageWriteSet,
    identities: I,
) -> Result<(), LixError>
where
    I: IntoIterator<Item = UntrackedStateIdentityRef<'a>>,
{
    for identity in identities {
        writes.delete(
            UNTRACKED_STATE_ROW_SPACE,
            StorageKey(Bytes::from(encode_untracked_state_row_key_ref(identity)?)),
        );
    }
    Ok(())
}

fn scan_matching_rows(
    store: &impl StorageRead,
    request: &UntrackedStateScanRequest,
    projection: &UntrackedMaterializationProjection,
    plan: &ScanPlan,
    materialized: &mut Vec<MaterializedUntrackedStateRow>,
) -> Result<(), LixError> {
    let mut resume_after = None;
    loop {
        let remaining_limit = request
            .limit
            .map(|limit| limit.saturating_sub(materialized.len()));
        if matches!(remaining_limit, Some(0)) {
            break;
        }
        let page = plan.collect(
            store,
            StorageScanOptions {
                resume_after: resume_after.as_ref(),
                limit_rows: remaining_limit
                    .unwrap_or_else(|| StorageScanOptions::default().limit_rows),
                ..StorageScanOptions::default()
            },
        )?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());

        for entry in page.value.entries {
            let Some(bytes) = full_value(entry.value) else {
                continue;
            };
            let identity = decode_untracked_state_row_key_ref(entry.key.0.as_ref())?;
            let row = crate::untracked_state::codec::decode_payload_with_identity(
                identity,
                bytes.as_ref(),
            )?;
            if !row_matches_scan(&row, request) {
                continue;
            }
            materialized.push(crate::untracked_state::materialize_row(row, projection)?);
            if request
                .limit
                .is_some_and(|limit| materialized.len() >= limit)
            {
                break;
            }
        }

        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(())
}

fn scan_plans_for_request(request: &UntrackedStateScanRequest) -> Result<Vec<ScanPlan>, LixError> {
    let mut prefixes = scan_prefixes_for_filter(&request.filter)?;
    prefixes.sort();
    prefixes.dedup();
    Ok(prefixes
        .into_iter()
        .map(|prefix| {
            ScanPlan::prefix(
                UNTRACKED_STATE_ROW_SPACE,
                StoragePrefix {
                    bytes: Bytes::from(prefix),
                },
            )
        })
        .collect())
}

fn scan_prefixes_for_filter(
    filter: &crate::untracked_state::UntrackedStateFilter,
) -> Result<Vec<Vec<u8>>, LixError> {
    if filter.version_ids.is_empty() {
        return Ok(vec![Vec::new()]);
    }

    let mut prefixes = Vec::new();
    for version_id in &filter.version_ids {
        let mut version_prefix = key_header();
        push_component(&mut version_prefix, version_id)?;
        if filter.schema_keys.is_empty() {
            prefixes.push(version_prefix);
            continue;
        }

        for schema_key in &filter.schema_keys {
            let mut schema_prefix = version_prefix.clone();
            push_component(&mut schema_prefix, schema_key)?;
            if filter.entity_ids.is_empty() {
                prefixes.push(schema_prefix);
                continue;
            }

            for entity_id in &filter.entity_ids {
                let mut entity_prefix = schema_prefix.clone();
                push_entity_component(&mut entity_prefix, entity_id)?;
                append_file_prefixes(&mut prefixes, entity_prefix, &filter.file_ids)?;
            }
        }
    }
    Ok(prefixes)
}

fn push_entity_component(out: &mut Vec<u8>, entity_id: &EntityIdentity) -> Result<(), LixError> {
    push_entity_tuple(out, entity_id)
}

fn append_file_prefixes(
    prefixes: &mut Vec<Vec<u8>>,
    entity_prefix: Vec<u8>,
    file_filters: &[NullableKeyFilter<String>],
) -> Result<(), LixError> {
    if file_filters.is_empty()
        || file_filters
            .iter()
            .any(|filter| matches!(filter, NullableKeyFilter::Any))
    {
        prefixes.push(entity_prefix);
        return Ok(());
    }

    for filter in file_filters {
        let mut prefix = entity_prefix.clone();
        match filter {
            NullableKeyFilter::Null => prefix.push(0),
            NullableKeyFilter::Value(file_id) => {
                prefix.push(1);
                push_component(&mut prefix, file_id)?;
            }
            NullableKeyFilter::Any => unreachable!("Any handled before exact file prefixes"),
        }
        prefixes.push(prefix);
    }
    Ok(())
}

fn full_value(value: StorageProjectedValue) -> Option<Bytes> {
    match value {
        StorageProjectedValue::FullValue(bytes) => Some(bytes),
        StorageProjectedValue::KeyOnly => None,
    }
}

fn row_matches_scan(row: &UntrackedStateRow, request: &UntrackedStateScanRequest) -> bool {
    (request.filter.schema_keys.is_empty() || request.filter.schema_keys.contains(&row.schema_key))
        && (request.filter.entity_ids.is_empty()
            || request.filter.entity_ids.contains(&row.entity_id))
        && (request.filter.version_ids.is_empty()
            || request.filter.version_ids.contains(&row.version_id))
        && nullable_matches_filters(&row.file_id, &request.filter.file_ids)
}

fn nullable_matches_filters(value: &Option<String>, filters: &[NullableKeyFilter<String>]) -> bool {
    filters.is_empty()
        || filters.iter().any(|filter| match filter {
            NullableKeyFilter::Any => true,
            NullableKeyFilter::Null => value.is_none(),
            NullableKeyFilter::Value(expected) => value.as_ref() == Some(expected),
        })
}

fn identity_from_request(request: &UntrackedStateRowRequest) -> Option<UntrackedStateIdentity> {
    let file_id = match &request.file_id {
        NullableKeyFilter::Null => None,
        NullableKeyFilter::Value(value) => Some(value.clone()),
        NullableKeyFilter::Any => return None,
    };
    Some(UntrackedStateIdentity {
        version_id: request.version_id.clone(),
        schema_key: request.schema_key.clone(),
        entity_id: request.entity_id.clone(),
        file_id,
    })
}

fn encode_untracked_state_row_key(identity: &UntrackedStateIdentity) -> Result<Vec<u8>, LixError> {
    encode_untracked_state_row_key_ref(identity.as_ref())
}

pub(crate) fn encode_untracked_state_row_key_ref(
    identity: UntrackedStateIdentityRef<'_>,
) -> Result<Vec<u8>, LixError> {
    let mut out = key_header();
    push_component(&mut out, identity.version_id)?;
    push_component(&mut out, identity.schema_key)?;
    push_entity_tuple(&mut out, identity.entity_id)?;
    match identity.file_id {
        Some(file_id) => {
            out.push(1);
            push_component(&mut out, file_id)?;
        }
        None => out.push(0),
    }
    Ok(out)
}

fn decode_untracked_state_row_key_ref(bytes: &[u8]) -> Result<UntrackedStateIdentity, LixError> {
    if !bytes.starts_with(UNTRACKED_STATE_ROW_KEY_IDENTIFIER) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "failed to decode untracked-state key: invalid key identifier",
        ));
    }
    let mut cursor = UNTRACKED_STATE_ROW_KEY_IDENTIFIER.len();
    let version = bytes.get(cursor).copied().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "failed to decode untracked-state key: missing version",
        )
    })?;
    cursor += 1;
    if version != UNTRACKED_STATE_ROW_KEY_VERSION_V1 {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to decode untracked-state key: unsupported version {version}"),
        ));
    }
    let version_id = read_key_component(bytes, &mut cursor, "version_id")?;
    let schema_key = read_key_component(bytes, &mut cursor, "schema_key")?;
    let entity_id = read_entity_tuple(bytes, &mut cursor)?;
    let file_tag = bytes.get(cursor).copied().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "failed to decode untracked-state key: missing file_id tag",
        )
    })?;
    cursor += 1;
    let file_id = match file_tag {
        0 => None,
        1 => Some(read_key_component(bytes, &mut cursor, "file_id")?),
        _ => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to decode untracked-state key: invalid file_id tag",
            ));
        }
    };
    if cursor != bytes.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "failed to decode untracked-state key: trailing bytes",
        ));
    }
    Ok(UntrackedStateIdentity {
        version_id,
        schema_key,
        entity_id,
        file_id,
    })
}

fn read_key_component(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<String, LixError> {
    let len_end = cursor.checked_add(4).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to decode untracked-state key: `{field}` cursor overflow"),
        )
    })?;
    let len_bytes = bytes.get(*cursor..len_end).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to decode untracked-state key: truncated `{field}` length"),
        )
    })?;
    *cursor = len_end;
    let len = u32::from_be_bytes(len_bytes.try_into().expect("slice length checked")) as usize;
    let value_end = cursor.checked_add(len).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to decode untracked-state key: `{field}` length overflow"),
        )
    })?;
    let value = bytes.get(*cursor..value_end).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to decode untracked-state key: truncated `{field}`"),
        )
    })?;
    *cursor = value_end;
    std::str::from_utf8(value)
        .map(str::to_string)
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "failed to decode untracked-state key: invalid utf-8 for `{field}`: {error}"
                ),
            )
        })
}

fn read_entity_tuple(bytes: &[u8], cursor: &mut usize) -> Result<EntityIdentity, LixError> {
    let part_count = read_key_u32(bytes, cursor, "entity_part_count")? as usize;
    if part_count == 0 {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "failed to decode untracked-state key: entity identity has no parts",
        ));
    }

    let mut parts = Vec::with_capacity(part_count);
    for index in 0..part_count {
        let part = read_key_component(bytes, cursor, "entity_part")?;
        if part.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "failed to decode untracked-state key: entity identity part {index} is empty"
                ),
            ));
        }
        parts.push(part);
    }
    Ok(EntityIdentity { parts })
}

fn read_key_u32(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<u32, LixError> {
    let len_end = cursor.checked_add(4).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to decode untracked-state key: `{field}` cursor overflow"),
        )
    })?;
    let len_bytes = bytes.get(*cursor..len_end).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to decode untracked-state key: truncated `{field}`"),
        )
    })?;
    *cursor = len_end;
    Ok(u32::from_be_bytes(
        len_bytes.try_into().expect("slice length checked"),
    ))
}

fn key_header() -> Vec<u8> {
    let mut out = Vec::with_capacity(UNTRACKED_STATE_ROW_KEY_IDENTIFIER.len() + 1);
    out.extend_from_slice(UNTRACKED_STATE_ROW_KEY_IDENTIFIER);
    out.push(UNTRACKED_STATE_ROW_KEY_VERSION_V1);
    out
}

fn push_component(out: &mut Vec<u8>, value: &str) -> Result<(), LixError> {
    let bytes = value.as_bytes();
    push_bytes_component(out, bytes)
}

fn push_entity_tuple(out: &mut Vec<u8>, entity_id: &EntityIdentity) -> Result<(), LixError> {
    let part_count = u32::try_from(entity_id.parts.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "failed to encode untracked-state key: entity identity part count exceeds u32",
        )
    })?;
    if part_count == 0 {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "failed to encode untracked-state key: entity identity has no parts",
        ));
    }
    out.extend_from_slice(&part_count.to_be_bytes());
    for part in &entity_id.parts {
        if part.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to encode untracked-state key: entity identity part is empty",
            ));
        }
        push_bytes_component(out, part.as_bytes())?;
    }
    Ok(())
}

fn push_bytes_component(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), LixError> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "failed to encode untracked-state key: component length exceeds u32",
        )
    })?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageContext;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};
    use crate::untracked_state::UntrackedStateContext;

    #[test]
    fn key_v1_roundtrips_null_file_id() {
        let identity = UntrackedStateIdentity {
            version_id: "version-1".to_string(),
            schema_key: "schema-1".to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            file_id: None,
        };
        let key = encode_untracked_state_row_key_ref(identity.as_ref()).expect("key should encode");

        assert_eq!(&key[..4], b"LXUK");
        assert_eq!(key[4], 1);
        assert_eq!(
            decode_untracked_state_row_key_ref(&key).expect("key should decode"),
            identity
        );
    }

    #[test]
    fn key_v1_roundtrips_empty_file_id() {
        let identity = UntrackedStateIdentity {
            version_id: "version-1".to_string(),
            schema_key: "schema-1".to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            file_id: Some(String::new()),
        };
        let key = encode_untracked_state_row_key_ref(identity.as_ref()).expect("key should encode");

        assert_eq!(
            decode_untracked_state_row_key_ref(&key).expect("key should decode"),
            identity
        );
    }

    #[test]
    fn key_v1_roundtrips_tuple_and_unicode_identity() {
        let identity = UntrackedStateIdentity {
            version_id: "version-東京".to_string(),
            schema_key: "schema-1".to_string(),
            entity_id: crate::entity_identity::EntityIdentity::tuple(vec![
                "entity-1".to_string(),
                "ключ".to_string(),
            ])
            .expect("entity identity should build"),
            file_id: Some("file-δ".to_string()),
        };
        let key = encode_untracked_state_row_key_ref(identity.as_ref()).expect("key should encode");

        assert_eq!(
            decode_untracked_state_row_key_ref(&key).expect("key should decode"),
            identity
        );
    }

    #[test]
    fn key_v1_encodes_entity_as_binary_tuple_not_json_text() {
        let identity = UntrackedStateIdentity {
            version_id: "version-1".to_string(),
            schema_key: "schema-1".to_string(),
            entity_id: crate::entity_identity::EntityIdentity::tuple(vec![
                "entity/1".to_string(),
                "quote\"part".to_string(),
            ])
            .expect("entity identity should build"),
            file_id: None,
        };
        let key = encode_untracked_state_row_key_ref(identity.as_ref()).expect("key should encode");

        assert!(!key
            .windows(2)
            .any(|window| window == br#"[""# || window == br#""]"#));
        assert_eq!(
            decode_untracked_state_row_key_ref(&key).expect("key should decode"),
            identity
        );
    }

    #[test]
    fn key_decode_rejects_invalid_identifier() {
        let error = decode_untracked_state_row_key_ref(b"LXUQ\x01")
            .expect_err("invalid key identifier should fail");
        assert!(error.to_string().contains("invalid key identifier"));
    }

    #[test]
    fn key_decode_rejects_unknown_version() {
        let identity = UntrackedStateIdentity {
            version_id: "version-1".to_string(),
            schema_key: "schema-1".to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            file_id: None,
        };
        let mut key =
            encode_untracked_state_row_key_ref(identity.as_ref()).expect("key should encode");
        key[4] = 2;
        let error =
            decode_untracked_state_row_key_ref(&key).expect_err("unknown key version should fail");
        assert!(error.to_string().contains("unsupported version 2"));
    }

    #[test]
    fn key_decode_rejects_trailing_bytes() {
        let identity = UntrackedStateIdentity {
            version_id: "version-1".to_string(),
            schema_key: "schema-1".to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            file_id: None,
        };
        let mut key =
            encode_untracked_state_row_key_ref(identity.as_ref()).expect("key should encode");
        key.push(0);
        let error =
            decode_untracked_state_row_key_ref(&key).expect_err("trailing key bytes should fail");
        assert!(error.to_string().contains("trailing bytes"));
    }

    #[test]
    fn key_decode_rejects_truncated_component() {
        let identity = UntrackedStateIdentity {
            version_id: "version-1".to_string(),
            schema_key: "schema-1".to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            file_id: None,
        };
        let mut key =
            encode_untracked_state_row_key_ref(identity.as_ref()).expect("key should encode");
        key.truncate(key.len() - 2);
        let error =
            decode_untracked_state_row_key_ref(&key).expect_err("truncated key should fail");
        assert!(error.to_string().contains("truncated"));
    }

    async fn write_materialized_rows_to_store(
        context: &UntrackedStateContext,
        storage: &StorageContext,
        rows: &[MaterializedUntrackedStateRow],
    ) {
        let mut writes = storage.new_write_set();
        let canonical_rows = rows
            .iter()
            .map(|row| crate::test_support::untracked_state_row_from_materialized(&mut writes, row))
            .collect::<Result<Vec<_>, _>>()
            .expect("rows should canonicalize");
        context
            .writer(&mut writes)
            .stage_rows(canonical_rows.iter().map(|row| row.as_ref()))
            .expect("rows should write");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("rows should commit");
    }

    #[tokio::test]
    async fn write_and_load_roundtrips() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let context = UntrackedStateContext::new();
        let row = untracked_row("global", "lix_key_value", "ui-tab");

        write_materialized_rows_to_store(&context, &storage, std::slice::from_ref(&row)).await;

        let loaded = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open");
            let mut reader = context.reader(read);
            reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "lix_key_value".to_string(),
                    version_id: "global".to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single("ui-tab"),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("load should succeed");
        assert_eq!(loaded, Some(row));
    }

    #[tokio::test]
    async fn scan_filters_by_schema_and_version() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let context = UntrackedStateContext::new();
        write_materialized_rows_to_store(
            &context,
            &storage,
            &[
                untracked_row("global", "lix_key_value", "global-ui"),
                untracked_row("version-a", "lix_key_value", "version-ui"),
                untracked_row("version-a", "other_schema", "other"),
            ],
        )
        .await;

        let rows = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open");
            let mut reader = context.reader(read);
            reader
                .scan_rows(&UntrackedStateScanRequest {
                    filter: crate::untracked_state::UntrackedStateFilter {
                        schema_keys: vec!["lix_key_value".to_string()],
                        version_ids: vec!["version-a".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await
        }
        .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].entity_id,
            crate::entity_identity::EntityIdentity::single("version-ui")
        );
    }

    #[tokio::test]
    async fn delete_removes_row() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let context = UntrackedStateContext::new();
        let row = untracked_row("global", "lix_key_value", "ui-tab");
        let identity = UntrackedStateIdentity {
            version_id: row.version_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        };
        write_materialized_rows_to_store(&context, &storage, std::slice::from_ref(&row)).await;

        let mut writes = storage.new_write_set();
        let mut writer = context.writer(&mut writes);
        writer
            .stage_delete_rows(std::iter::once(identity.as_ref()))
            .expect("delete should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("writes should commit");

        let loaded = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open");
            let mut reader = context.reader(read);
            reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "lix_key_value".to_string(),
                    version_id: "global".to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single("ui-tab"),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("load should succeed");
        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn v1_layout_ignores_previous_untracked_row_space() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let context = UntrackedStateContext::new();
        let legacy_space = StorageSpace::new(StorageSpaceId(0x0001_0001), "untracked_state.row");
        let mut writes = storage.new_write_set();
        writes.put(
            legacy_space,
            StorageKey(Bytes::from_static(b"legacy-row-key")),
            StorageValue {
                bytes: Bytes::from_static(b"legacy-row-value"),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("legacy row should commit");

        let loaded = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open");
            let mut reader = context.reader(read);
            reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "lix_key_value".to_string(),
                    version_id: "global".to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single("legacy-row-key"),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("load should succeed");
        assert_eq!(loaded, None);

        let rows = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open");
            let mut reader = context.reader(read);
            reader
                .scan_rows(&UntrackedStateScanRequest {
                    filter: crate::untracked_state::UntrackedStateFilter {
                        version_ids: vec!["global".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await
        }
        .expect("scan should succeed");
        assert!(rows.is_empty());
    }

    fn untracked_row(
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
    ) -> MaterializedUntrackedStateRow {
        MaterializedUntrackedStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single(entity_id),
            schema_key: schema_key.to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"key\":\"{}\",\"value\":\"value\"}}", entity_id)),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: version_id == "global",
            version_id: version_id.to_string(),
        }
    }
}
