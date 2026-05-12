use crate::storage::KvScanRange;
use crate::storage::{KvGetGroup, KvGetRequest, KvScanRequest, StorageReader, StorageWriteSet};
use crate::untracked_state::{
    MaterializedUntrackedStateRow, UntrackedMaterializationProjection, UntrackedStateIdentity,
    UntrackedStateIdentityRef, UntrackedStateRow, UntrackedStateRowRef, UntrackedStateRowRequest,
    UntrackedStateScanRequest,
};
use crate::{LixError, NullableKeyFilter};

// Compact physical namespace for untracked rows. This string is stored in every
// backend key, so keep it short; the typed constant preserves the semantic name.
pub(super) const UNTRACKED_STATE_ROW_NAMESPACE: &str = "u";
const LOAD_ROWS_BATCH_SIZE: usize = 512;

pub(crate) async fn scan_rows(
    store: &mut impl StorageReader,
    request: &UntrackedStateScanRequest,
) -> Result<Vec<MaterializedUntrackedStateRow>, LixError> {
    if request.limit == Some(0) {
        return Ok(Vec::new());
    }
    if projection_is_identity_only(&request.projection.columns) {
        return scan_identity_rows(store, request).await;
    }

    if should_load_filtered_rows_by_key(request) {
        return scan_filtered_rows_by_key(store, request).await;
    }

    scan_unfiltered_rows(store, request).await
}

async fn scan_unfiltered_rows(
    store: &mut impl StorageReader,
    request: &UntrackedStateScanRequest,
) -> Result<Vec<MaterializedUntrackedStateRow>, LixError> {
    let limit = request.limit.unwrap_or(usize::MAX);
    let backend_limit = if has_identity_filters(request) {
        usize::MAX
    } else {
        limit
    };
    let page = store
        .scan_entries(KvScanRequest {
            namespace: UNTRACKED_STATE_ROW_NAMESPACE.to_string(),
            range: KvScanRange::prefix(Vec::new()),
            after: None,
            limit: backend_limit,
        })
        .await?;
    let projection = UntrackedMaterializationProjection::from_columns(&request.projection.columns);
    let mut materialized = Vec::with_capacity(page.len().min(limit));
    for (key, value) in page.keys.iter().zip(page.values.iter()) {
        let identity = decode_untracked_state_row_key(key)?;
        let row = crate::untracked_state::codec::decode_row_value(value, identity)?;
        if row_matches_scan(&row, request) {
            materialized.push(crate::untracked_state::materialize_row(row, &projection)?);
            if materialized.len() == limit {
                break;
            }
        }
    }
    Ok(materialized)
}

async fn scan_filtered_rows_by_key(
    store: &mut impl StorageReader,
    request: &UntrackedStateScanRequest,
) -> Result<Vec<MaterializedUntrackedStateRow>, LixError> {
    let candidates = scan_matching_identities(store, request).await?;
    let projection = UntrackedMaterializationProjection::from_columns(&request.projection.columns);
    let mut rows = Vec::with_capacity(candidates.len());
    for chunk in candidates.chunks(LOAD_ROWS_BATCH_SIZE) {
        let result = store
            .get_values(KvGetRequest {
                groups: vec![KvGetGroup {
                    namespace: UNTRACKED_STATE_ROW_NAMESPACE.to_string(),
                    keys: chunk.iter().map(|(_, key)| key.clone()).collect(),
                }],
            })
            .await?;
        let group = result.groups.into_iter().next().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "filtered untracked row load returned no result group",
            )
        })?;
        if group.namespace() != UNTRACKED_STATE_ROW_NAMESPACE {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "filtered untracked row load returned namespace `{}` instead of `{}`",
                    group.namespace(),
                    UNTRACKED_STATE_ROW_NAMESPACE
                ),
            ));
        }
        if group.len() != chunk.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "filtered untracked row load returned {} results for {} requested keys",
                    group.len(),
                    chunk.len()
                ),
            ));
        }
        for ((identity, _), bytes) in chunk.iter().zip(group.values_iter()) {
            let Some(bytes) = bytes else {
                continue;
            };
            let row = crate::untracked_state::codec::decode_row_value(bytes, identity.clone())?;
            rows.push(crate::untracked_state::materialize_row(row, &projection)?);
        }
    }
    Ok(rows)
}

async fn scan_identity_rows(
    store: &mut impl StorageReader,
    request: &UntrackedStateScanRequest,
) -> Result<Vec<MaterializedUntrackedStateRow>, LixError> {
    let limit = if has_identity_filters(request) {
        usize::MAX
    } else {
        request.limit.unwrap_or(usize::MAX)
    };
    let page = store
        .scan_keys(KvScanRequest {
            namespace: UNTRACKED_STATE_ROW_NAMESPACE.to_string(),
            range: KvScanRange::prefix(Vec::new()),
            after: None,
            limit,
        })
        .await?;
    let output_limit = request.limit.unwrap_or(usize::MAX);
    let mut rows = Vec::with_capacity(page.keys.len().min(output_limit));
    for key in page.keys.iter() {
        let identity = decode_untracked_state_row_key(key)?;
        if identity_matches_scan(&identity, request) {
            rows.push(materialize_identity_row(identity)?);
            if rows.len() == output_limit {
                break;
            }
        }
    }
    Ok(rows)
}

async fn scan_matching_identities(
    store: &mut impl StorageReader,
    request: &UntrackedStateScanRequest,
) -> Result<Vec<(UntrackedStateIdentity, Vec<u8>)>, LixError> {
    let limit = if has_identity_filters(request) {
        usize::MAX
    } else {
        request.limit.unwrap_or(usize::MAX)
    };
    let page = store
        .scan_keys(KvScanRequest {
            namespace: UNTRACKED_STATE_ROW_NAMESPACE.to_string(),
            range: KvScanRange::prefix(Vec::new()),
            after: None,
            limit,
        })
        .await?;
    let output_limit = request.limit.unwrap_or(usize::MAX);
    let mut rows = Vec::with_capacity(page.keys.len().min(output_limit));
    for key in page.keys.iter() {
        let identity = decode_untracked_state_row_key(key)?;
        if identity_matches_scan(&identity, request) {
            rows.push((identity, key.to_vec()));
            if rows.len() == output_limit {
                break;
            }
        }
    }
    Ok(rows)
}

pub(crate) async fn load_rows(
    store: &mut impl StorageReader,
    requests: &[UntrackedStateRowRequest],
) -> Result<Vec<Option<MaterializedUntrackedStateRow>>, LixError> {
    if let [request] = requests {
        return load_single_row(store, request).await.map(|row| vec![row]);
    }

    let mut rows = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
    let mut candidates = Vec::new();
    for (index, request) in requests.iter().enumerate() {
        let Some(identity) = identity_from_request(request) else {
            continue;
        };
        let key = encode_untracked_state_row_key(&identity);
        candidates.push((index, identity, key));
    }
    for chunk in candidates.chunks(LOAD_ROWS_BATCH_SIZE) {
        load_rows_chunk(store, chunk, &mut rows).await?;
    }
    Ok(rows)
}

async fn load_single_row(
    store: &mut impl StorageReader,
    request: &UntrackedStateRowRequest,
) -> Result<Option<MaterializedUntrackedStateRow>, LixError> {
    let Some(identity) = identity_from_request(request) else {
        return Ok(None);
    };
    let bytes = store
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: UNTRACKED_STATE_ROW_NAMESPACE.to_string(),
                keys: vec![encode_untracked_state_row_key(&identity)],
            }],
        })
        .await?
        .groups
        .into_iter()
        .next()
        .and_then(|group| group.single_value_owned());
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let row = crate::untracked_state::codec::decode_row_value(&bytes, identity)?;
    crate::untracked_state::materialize_row(row, &UntrackedMaterializationProjection::full())
        .map(Some)
}

async fn load_rows_chunk(
    store: &mut impl StorageReader,
    candidates: &[(usize, UntrackedStateIdentity, Vec<u8>)],
    rows: &mut [Option<MaterializedUntrackedStateRow>],
) -> Result<(), LixError> {
    if candidates.is_empty() {
        return Ok(());
    }
    let result = store
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: UNTRACKED_STATE_ROW_NAMESPACE.to_string(),
                keys: candidates.iter().map(|(_, _, key)| key.clone()).collect(),
            }],
        })
        .await?;
    let group = result.groups.into_iter().next().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "untracked row batch load returned no result group",
        )
    })?;
    if group.namespace() != UNTRACKED_STATE_ROW_NAMESPACE {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "untracked row batch load returned namespace `{}` instead of `{}`",
                group.namespace(),
                UNTRACKED_STATE_ROW_NAMESPACE
            ),
        ));
    }
    if group.len() != candidates.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "untracked row batch load returned {} results for {} requested keys",
                group.len(),
                candidates.len()
            ),
        ));
    }
    for ((index, identity, _), bytes) in candidates.iter().zip(group.values_iter()) {
        let Some(bytes) = bytes else {
            continue;
        };
        let row = crate::untracked_state::codec::decode_row_value(bytes, identity.clone())?;
        rows[*index] = Some(crate::untracked_state::materialize_row(
            row,
            &UntrackedMaterializationProjection::full(),
        )?);
    }
    Ok(())
}

pub(super) async fn existing_identities<'a>(
    store: &mut (impl StorageReader + ?Sized),
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
            let key = encode_untracked_state_row_key_ref(owned.as_ref());
            (key, owned)
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|(left, _), (right, _)| left.cmp(right));
    candidates.dedup_by(|(left, _), (right, _)| left == right);
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let keys = candidates
        .iter()
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();

    let result = store
        .exists_many(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: UNTRACKED_STATE_ROW_NAMESPACE.to_string(),
                keys,
            }],
        })
        .await?;
    let group = result.groups.into_iter().next().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "untracked identity existence probe returned no result group",
        )
    })?;
    if group.exists.len() != candidates.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "untracked identity existence probe returned {} results for {} requested keys",
                group.exists.len(),
                candidates.len()
            ),
        ));
    }

    Ok(candidates
        .into_iter()
        .zip(group.exists)
        .filter_map(|((_, identity), exists)| exists.then_some(identity))
        .collect())
}

pub(crate) fn stage_rows<'a, I>(writes: &mut StorageWriteSet, rows: I) -> Result<(), LixError>
where
    I: IntoIterator<Item = UntrackedStateRowRef<'a>>,
{
    for row in rows {
        if row.snapshot_content.is_none() {
            let key = encode_untracked_state_row_key_ref(row.into());
            writes.delete(UNTRACKED_STATE_ROW_NAMESPACE, key);
        } else {
            let key = encode_untracked_state_row_key_ref(row.into());
            writes.put(
                UNTRACKED_STATE_ROW_NAMESPACE,
                key,
                crate::untracked_state::codec::encode_row_value_ref(row)?,
            );
        }
    }
    Ok(())
}

pub(crate) fn stage_delete_rows<'a, I>(writes: &mut StorageWriteSet, identities: I)
where
    I: IntoIterator<Item = UntrackedStateIdentityRef<'a>>,
{
    for identity in identities {
        let key = encode_untracked_state_row_key_ref(identity);
        writes.delete(UNTRACKED_STATE_ROW_NAMESPACE, key);
    }
}

#[allow(dead_code)]
pub(crate) fn stage_delete_all_rows(writes: &mut StorageWriteSet) {
    writes.delete_range(
        UNTRACKED_STATE_ROW_NAMESPACE,
        KvScanRange::prefix(Vec::new()),
    );
}

fn projection_is_identity_only(columns: &[String]) -> bool {
    !columns.is_empty()
        && columns.iter().all(|column| {
            matches!(
                column.as_str(),
                "entity_id" | "schema_key" | "file_id" | "version_id"
            )
        })
}

fn materialize_identity_row(
    identity: UntrackedStateIdentity,
) -> Result<MaterializedUntrackedStateRow, LixError> {
    Ok(MaterializedUntrackedStateRow {
        entity_id: identity.entity_id,
        schema_key: identity.schema_key,
        file_id: identity.file_id,
        snapshot_content: None,
        metadata: None,
        deleted: false,
        created_at: String::new(),
        updated_at: String::new(),
        global: false,
        version_id: identity.version_id,
    })
}

fn row_matches_scan(row: &UntrackedStateRow, request: &UntrackedStateScanRequest) -> bool {
    (request.filter.schema_keys.is_empty() || request.filter.schema_keys.contains(&row.schema_key))
        && (request.filter.entity_ids.is_empty()
            || request.filter.entity_ids.contains(&row.entity_id))
        && (request.filter.version_ids.is_empty()
            || request.filter.version_ids.contains(&row.version_id))
        && nullable_matches_filters(&row.file_id, &request.filter.file_ids)
}

fn identity_matches_scan(
    identity: &UntrackedStateIdentity,
    request: &UntrackedStateScanRequest,
) -> bool {
    (request.filter.schema_keys.is_empty()
        || request.filter.schema_keys.contains(&identity.schema_key))
        && (request.filter.entity_ids.is_empty()
            || request.filter.entity_ids.contains(&identity.entity_id))
        && (request.filter.version_ids.is_empty()
            || request.filter.version_ids.contains(&identity.version_id))
        && nullable_matches_filters(&identity.file_id, &request.filter.file_ids)
}

fn has_identity_filters(request: &UntrackedStateScanRequest) -> bool {
    !request.filter.schema_keys.is_empty()
        || !request.filter.entity_ids.is_empty()
        || !request.filter.version_ids.is_empty()
        || !request.filter.file_ids.is_empty()
}

fn should_load_filtered_rows_by_key(request: &UntrackedStateScanRequest) -> bool {
    // Key-first hydration helps selective filters, but broad schema/file scans
    // can be all-match workloads where a single entry scan is materially faster.
    request.limit.is_some()
        || !request.filter.entity_ids.is_empty()
        || !request.filter.version_ids.is_empty()
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

fn encode_untracked_state_row_key(identity: &UntrackedStateIdentity) -> Vec<u8> {
    encode_untracked_state_row_key_ref(identity.as_ref())
}

fn decode_untracked_state_row_key(bytes: &[u8]) -> Result<UntrackedStateIdentity, LixError> {
    let mut cursor = 0;
    let version_id = read_component(bytes, &mut cursor)?.to_string();
    let schema_key = read_component(bytes, &mut cursor)?.to_string();
    let entity_id = read_entity_identity(bytes, &mut cursor)?;
    let file_id = match bytes.get(cursor).copied() {
        Some(0) => {
            cursor += 1;
            None
        }
        Some(1) => {
            cursor += 1;
            Some(read_component(bytes, &mut cursor)?.to_string())
        }
        Some(marker) => {
            return Err(LixError::unknown(format!(
                "failed to decode untracked-state key: invalid file marker {marker}"
            )))
        }
        None => {
            return Err(LixError::unknown(
                "failed to decode untracked-state key: missing file marker",
            ))
        }
    };
    if cursor != bytes.len() {
        return Err(LixError::unknown(
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

pub(super) fn encode_untracked_state_row_key_ref(
    identity: UntrackedStateIdentityRef<'_>,
) -> Vec<u8> {
    // This compact component framing is for exact-key identity lookups and
    // whole-namespace scans. It is not a logical order-preserving tuple codec.
    let mut out = Vec::with_capacity(encoded_untracked_state_row_key_len(identity));
    push_component(&mut out, identity.version_id);
    push_component(&mut out, identity.schema_key);
    push_entity_identity(&mut out, identity.entity_id);
    match identity.file_id {
        Some(file_id) => {
            out.push(1);
            push_component(&mut out, file_id);
        }
        None => out.push(0),
    }
    out
}

fn encoded_untracked_state_row_key_len(identity: UntrackedStateIdentityRef<'_>) -> usize {
    encoded_component_len(identity.version_id)
        + encoded_component_len(identity.schema_key)
        + varint_len(identity.entity_id.parts.len())
        + identity
            .entity_id
            .parts
            .iter()
            .map(|part| encoded_component_len(part))
            .sum::<usize>()
        + 1
        + identity.file_id.map(encoded_component_len).unwrap_or(0)
}

fn push_entity_identity(out: &mut Vec<u8>, entity_id: &crate::entity_identity::EntityIdentity) {
    push_varint_len(out, entity_id.parts.len());
    for part in &entity_id.parts {
        push_component(out, part);
    }
}

fn read_entity_identity(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<crate::entity_identity::EntityIdentity, LixError> {
    let part_count = read_varint_len(bytes, cursor)?;
    if part_count == 0 {
        return Err(LixError::unknown(
            "failed to decode untracked-state key: empty entity identity",
        ));
    }
    if part_count > bytes.len().saturating_sub(*cursor) {
        return Err(LixError::unknown(
            "failed to decode untracked-state key: entity identity part count exceeds remaining bytes",
        ));
    }
    let mut parts = Vec::new();
    for _ in 0..part_count {
        let part = read_component(bytes, cursor)?;
        if part.is_empty() {
            return Err(LixError::unknown(
                "failed to decode untracked-state key: empty entity identity part",
            ));
        }
        parts.push(part.to_string());
    }
    Ok(crate::entity_identity::EntityIdentity { parts })
}

fn push_component(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    push_varint_len(out, bytes.len());
    out.extend_from_slice(bytes);
}

fn encoded_component_len(value: &str) -> usize {
    varint_len(value.len()) + value.len()
}

fn read_component<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<&'a str, LixError> {
    let len = read_varint_len(bytes, cursor)?;
    let component = bytes
        .get(*cursor..cursor.saturating_add(len))
        .ok_or_else(|| LixError::unknown("failed to decode untracked-state key: short value"))?;
    *cursor += len;
    std::str::from_utf8(component).map_err(|error| {
        LixError::unknown(format!(
            "failed to decode untracked-state key component as UTF-8: {error}"
        ))
    })
}

fn push_varint_len(out: &mut Vec<u8>, mut len: usize) {
    while len >= 0x80 {
        out.push((len as u8 & 0x7f) | 0x80);
        len >>= 7;
    }
    out.push(len as u8);
}

fn varint_len(mut len: usize) -> usize {
    let mut encoded_len = 1;
    while len >= 0x80 {
        encoded_len += 1;
        len >>= 7;
    }
    encoded_len
}

fn read_varint_len(bytes: &[u8], cursor: &mut usize) -> Result<usize, LixError> {
    let start = *cursor;
    let mut len = 0u128;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*cursor).ok_or_else(|| {
            LixError::unknown("failed to decode untracked-state key: short length")
        })?;
        *cursor += 1;
        len |= u128::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            if len > usize::MAX as u128 {
                return Err(LixError::unknown(
                    "failed to decode untracked-state key: length overflow",
                ));
            }
            let len = len as usize;
            let mut canonical = Vec::new();
            push_varint_len(&mut canonical, len);
            if bytes.get(start..*cursor) != Some(canonical.as_slice()) {
                return Err(LixError::unknown(
                    "failed to decode untracked-state key: non-canonical length",
                ));
            }
            return Ok(len);
        }
        shift += 7;
        if shift >= 128 {
            return Err(LixError::unknown(
                "failed to decode untracked-state key: length overflow",
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::testing::UnitTestBackend;
    use crate::storage::{StorageContext, StorageWriteTransaction};
    use crate::untracked_state::UntrackedStateContext;

    async fn write_materialized_rows_to_store(
        context: &UntrackedStateContext,
        store: &mut (impl StorageWriteTransaction + ?Sized),
        rows: &[MaterializedUntrackedStateRow],
    ) {
        let mut writes = StorageWriteSet::new();
        let canonical_rows = rows
            .iter()
            .map(|row| crate::test_support::untracked_state_row_from_materialized(&mut writes, row))
            .collect::<Result<Vec<_>, _>>()
            .expect("rows should canonicalize");
        context
            .writer(&mut writes)
            .stage_rows(canonical_rows.iter().map(|row| row.as_ref()))
            .expect("rows should write");
        writes.apply(store).await.expect("rows should apply");
    }

    #[tokio::test]
    async fn write_and_load_roundtrips() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let row = untracked_row("global", "lix_key_value", "ui-tab");

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        write_materialized_rows_to_store(
            &context,
            transaction.as_mut(),
            std::slice::from_ref(&row),
        )
        .await;
        transaction.commit().await.expect("commit should succeed");

        let loaded = {
            let mut reader = context.reader(storage.clone());
            let request = UntrackedStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: "global".to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("ui-tab"),
                file_id: NullableKeyFilter::Null,
            };
            reader
                .load_rows(std::slice::from_ref(&request))
                .await
                .map(|rows| rows.into_iter().next().flatten())
        }
        .expect("load should succeed");
        assert_eq!(loaded, Some(row));
    }

    #[tokio::test]
    async fn delete_all_rows_clears_only_untracked_namespace() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let row = untracked_row("global", "lix_key_value", "ui-tab");

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        write_materialized_rows_to_store(
            &context,
            transaction.as_mut(),
            std::slice::from_ref(&row),
        )
        .await;
        {
            let mut writes = StorageWriteSet::new();
            writes.put("other", vec![0xFF], vec![1]);
            writes
                .apply(transaction.as_mut())
                .await
                .expect("other namespace write should apply");
        }
        transaction.commit().await.expect("commit should succeed");

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("delete transaction should open");
        {
            let mut writes = StorageWriteSet::new();
            stage_delete_all_rows(&mut writes);
            writes
                .apply(transaction.as_mut())
                .await
                .expect("delete-all should apply");
        }
        transaction.commit().await.expect("commit should succeed");

        let untracked_rows = {
            let mut reader = context.reader(storage.clone());
            reader
                .scan_rows(&UntrackedStateScanRequest::default())
                .await
                .expect("untracked scan should succeed")
        };
        assert!(untracked_rows.is_empty());

        let mut reader = storage
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let values = reader
            .get_values(KvGetRequest {
                groups: vec![KvGetGroup {
                    namespace: "other".to_string(),
                    keys: vec![vec![0xFF]],
                }],
            })
            .await
            .expect("other namespace read should succeed");
        assert_eq!(
            values.groups[0].single_value_owned(),
            Some(vec![1]),
            "delete-all must not cross namespaces"
        );
    }

    #[tokio::test]
    async fn scan_filters_by_schema_and_version() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        write_materialized_rows_to_store(
            &context,
            transaction.as_mut(),
            &[
                untracked_row("global", "lix_key_value", "global-ui"),
                untracked_row("version-a", "lix_key_value", "version-ui"),
                untracked_row("version-a", "other_schema", "other"),
            ],
        )
        .await;
        transaction.commit().await.expect("commit should succeed");

        let rows = {
            let mut reader = context.reader(storage.clone());
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
    async fn scan_limit_zero_returns_no_rows() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let row = untracked_row("version-a", "lix_key_value", "version-ui");
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        write_materialized_rows_to_store(&context, transaction.as_mut(), &[row]).await;
        transaction.commit().await.expect("commit should succeed");

        let full_rows = {
            let mut reader = context.reader(storage.clone());
            reader
                .scan_rows(&UntrackedStateScanRequest {
                    limit: Some(0),
                    ..Default::default()
                })
                .await
        }
        .expect("full scan should succeed");
        assert!(full_rows.is_empty());

        let identity_rows = {
            let mut reader = context.reader(storage.clone());
            reader
                .scan_rows(&UntrackedStateScanRequest {
                    projection: crate::untracked_state::UntrackedStateProjection {
                        columns: vec!["entity_id".to_string()],
                    },
                    limit: Some(0),
                    ..Default::default()
                })
                .await
        }
        .expect("identity scan should succeed");
        assert!(identity_rows.is_empty());
    }

    #[tokio::test]
    async fn filtered_full_scan_limit_preserves_key_order() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        write_materialized_rows_to_store(
            &context,
            transaction.as_mut(),
            &[
                untracked_row("version-a", "lix_key_value", "b"),
                untracked_row("version-a", "lix_key_value", "a"),
                untracked_row("version-a", "lix_key_value", "c"),
                untracked_row("version-b", "lix_key_value", "d"),
            ],
        )
        .await;
        transaction.commit().await.expect("commit should succeed");

        let rows = {
            let mut reader = context.reader(storage.clone());
            reader
                .scan_rows(&UntrackedStateScanRequest {
                    filter: crate::untracked_state::UntrackedStateFilter {
                        version_ids: vec!["version-a".to_string()],
                        ..Default::default()
                    },
                    limit: Some(2),
                    ..Default::default()
                })
                .await
        }
        .expect("filtered scan should succeed");

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].entity_id,
            crate::entity_identity::EntityIdentity::single("a")
        );
        assert_eq!(
            rows[1].entity_id,
            crate::entity_identity::EntityIdentity::single("b")
        );
    }

    #[tokio::test]
    async fn batch_load_preserves_request_order_and_misses() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let first = untracked_row("global", "lix_key_value", "first");
        let second = untracked_row("global", "lix_key_value", "second");
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        write_materialized_rows_to_store(
            &context,
            transaction.as_mut(),
            &[first.clone(), second.clone()],
        )
        .await;
        transaction.commit().await.expect("commit should succeed");

        let loaded = {
            let mut reader = context.reader(storage.clone());
            reader
                .load_rows(&[
                    UntrackedStateRowRequest {
                        schema_key: "lix_key_value".to_string(),
                        version_id: "global".to_string(),
                        entity_id: crate::entity_identity::EntityIdentity::single("second"),
                        file_id: NullableKeyFilter::Null,
                    },
                    UntrackedStateRowRequest {
                        schema_key: "lix_key_value".to_string(),
                        version_id: "global".to_string(),
                        entity_id: crate::entity_identity::EntityIdentity::single("missing"),
                        file_id: NullableKeyFilter::Null,
                    },
                    UntrackedStateRowRequest {
                        schema_key: "lix_key_value".to_string(),
                        version_id: "global".to_string(),
                        entity_id: crate::entity_identity::EntityIdentity::single("first"),
                        file_id: NullableKeyFilter::Any,
                    },
                    UntrackedStateRowRequest {
                        schema_key: "lix_key_value".to_string(),
                        version_id: "global".to_string(),
                        entity_id: crate::entity_identity::EntityIdentity::single("first"),
                        file_id: NullableKeyFilter::Null,
                    },
                    UntrackedStateRowRequest {
                        schema_key: "lix_key_value".to_string(),
                        version_id: "global".to_string(),
                        entity_id: crate::entity_identity::EntityIdentity::single("second"),
                        file_id: NullableKeyFilter::Null,
                    },
                ])
                .await
        }
        .expect("batch load should succeed");

        assert_eq!(
            loaded,
            vec![Some(second.clone()), None, None, Some(first), Some(second)]
        );
    }

    #[tokio::test]
    async fn key_only_scan_projects_identity_without_snapshot_payload() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let mut row = untracked_row(crate::GLOBAL_VERSION_ID, "lix_key_value", "ui-tab");
        row.entity_id = crate::entity_identity::EntityIdentity::tuple(vec![
            "ui".to_string(),
            "tab".to_string(),
        ])
        .expect("tuple identity should be valid");
        row.file_id = Some("settings.json".to_string());
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        write_materialized_rows_to_store(
            &context,
            transaction.as_mut(),
            std::slice::from_ref(&row),
        )
        .await;
        transaction.commit().await.expect("commit should succeed");

        let rows = {
            let mut reader = context.reader(storage.clone());
            reader
                .scan_rows(&UntrackedStateScanRequest {
                    projection: crate::untracked_state::UntrackedStateProjection {
                        columns: vec!["entity_id".to_string()],
                    },
                    ..Default::default()
                })
                .await
        }
        .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, row.entity_id);
        assert_eq!(rows[0].schema_key, row.schema_key);
        assert_eq!(rows[0].version_id, row.version_id);
        assert_eq!(rows[0].file_id, row.file_id);
        assert!(!rows[0].global);
        assert_eq!(rows[0].created_at, "");
        assert_eq!(rows[0].updated_at, "");
        assert_eq!(rows[0].snapshot_content, None);

        let full_rows = {
            let mut reader = context.reader(storage.clone());
            reader
                .scan_rows(&UntrackedStateScanRequest::default())
                .await
        }
        .expect("full scan should succeed");

        assert_eq!(full_rows.len(), 1);
        assert_eq!(full_rows[0].snapshot_content, row.snapshot_content);
        assert_eq!(full_rows[0].created_at, row.created_at);
        assert_eq!(full_rows[0].updated_at, row.updated_at);
    }

    #[test]
    fn row_key_roundtrips_identity() {
        let identity = UntrackedStateIdentity {
            version_id: "version-a".to_string(),
            schema_key: "lix_key_value".to_string(),
            entity_id: crate::entity_identity::EntityIdentity::tuple(vec![
                "ui".to_string(),
                "tab".to_string(),
            ])
            .expect("tuple identity should be valid"),
            file_id: Some("settings.json".to_string()),
        };

        let key = encode_untracked_state_row_key(&identity);
        let decoded = decode_untracked_state_row_key(&key).expect("key should decode");
        assert_eq!(decoded, identity);
    }

    #[test]
    fn row_key_capacity_matches_encoded_length() {
        let long = "x".repeat(128);
        let identities = [
            UntrackedStateIdentity {
                version_id: "v".to_string(),
                schema_key: "s".to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("entity"),
                file_id: None,
            },
            UntrackedStateIdentity {
                version_id: long.clone(),
                schema_key: "schema".to_string(),
                entity_id: crate::entity_identity::EntityIdentity::tuple(vec![
                    "left".to_string(),
                    long,
                ])
                .expect("tuple identity should be valid"),
                file_id: Some("settings.json".to_string()),
            },
        ];

        for identity in identities {
            let key = encode_untracked_state_row_key(&identity);
            assert_eq!(key.capacity(), key.len());
        }
    }

    #[test]
    fn row_key_rejects_malformed_varints_and_identity_parts() {
        let mut cursor = 0;
        assert!(read_varint_len(&[0x80, 0x00], &mut cursor).is_err());

        let mut cursor = 0;
        let mut overflowing = vec![0xff; 19];
        overflowing.push(0x01);
        assert!(read_varint_len(&overflowing, &mut cursor).is_err());

        let mut empty_part_key = Vec::new();
        push_component(&mut empty_part_key, "version-a");
        push_component(&mut empty_part_key, "schema-a");
        push_varint_len(&mut empty_part_key, 1);
        push_component(&mut empty_part_key, "");
        empty_part_key.push(0);
        assert!(decode_untracked_state_row_key(&empty_part_key).is_err());

        let mut impossible_part_count_key = Vec::new();
        push_component(&mut impossible_part_count_key, "version-a");
        push_component(&mut impossible_part_count_key, "schema-a");
        push_varint_len(&mut impossible_part_count_key, 1024);
        impossible_part_count_key.push(0);
        assert!(decode_untracked_state_row_key(&impossible_part_count_key).is_err());
    }

    #[tokio::test]
    async fn delete_removes_row() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let row = untracked_row("global", "lix_key_value", "ui-tab");
        let identity = UntrackedStateIdentity {
            version_id: row.version_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        };
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        let canonical_row =
            crate::test_support::untracked_state_row_from_materialized(&mut writes, &row)
                .expect("row should canonicalize");
        let mut writer = context.writer(&mut writes);
        writer
            .stage_rows(std::iter::once(canonical_row.as_ref()))
            .expect("write should succeed");
        writer.stage_delete_rows(std::iter::once(identity.as_ref()));
        writes
            .apply(&mut transaction.as_mut())
            .await
            .expect("writes should apply");
        transaction.commit().await.expect("commit should succeed");

        let loaded = {
            let mut reader = context.reader(storage.clone());
            let request = UntrackedStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: "global".to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("ui-tab"),
                file_id: NullableKeyFilter::Null,
            };
            reader
                .load_rows(std::slice::from_ref(&request))
                .await
                .map(|rows| rows.into_iter().next().flatten())
        }
        .expect("load should succeed");
        assert_eq!(loaded, None);
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
