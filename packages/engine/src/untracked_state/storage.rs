use crate::storage::{
    get_values_single_namespace_chunked, KvEntryPage, KvGetGroup, KvGetRequest, KvScanRange,
    KvScanRequest, KvValueGroup, KvWriteGroup, StorageReader, StorageWriteSet,
    DEFAULT_GET_VALUES_CHUNK_SIZE,
};
use crate::untracked_state::{
    UntrackedStateGetManyRequest, UntrackedStateGetManyResponse, UntrackedStateIdentity,
    UntrackedStateIdentityRef, UntrackedStateProjectedRow, UntrackedStateProjection,
    UntrackedStateRow, UntrackedStateRowRef, UntrackedStateScanRequest, UntrackedStateScanResponse,
};
use crate::{LixError, NullableKeyFilter};

// Compact physical namespaces for untracked rows. Identity fields live in the
// key; hot header fields and larger payload fields are split so projections
// read only the bytes they request.
pub(super) const UNTRACKED_STATE_HEADER_NAMESPACE: &str = "uh2";
const UNTRACKED_STATE_PAYLOAD_NAMESPACE: &str = "up2";
const LEGACY_UNTRACKED_STATE_ROW_NAMESPACE_V1: &str = "u1";
const LEGACY_UNTRACKED_STATE_ROW_NAMESPACE: &str = "u";
const UNTRACKED_STATE_FORMAT_NAMESPACE: &str = "lix.storage_format";
const UNTRACKED_STATE_FORMAT_KEY: &[u8] = b"untracked_state";
const UNTRACKED_STATE_FORMAT_VALUE: &[u8] = b"2";

pub(crate) async fn get_many(
    store: &mut impl StorageReader,
    request: UntrackedStateGetManyRequest,
) -> Result<UntrackedStateGetManyResponse, LixError> {
    ensure_read_format(store).await?;
    let rows = match request.projection {
        UntrackedStateProjection::Identity => {
            load_identity_existence(store, &request.identities).await?
        }
        UntrackedStateProjection::Header => {
            load_projected_headers(store, &request.identities).await?
        }
        UntrackedStateProjection::Payload => {
            load_projected_payloads(store, &request.identities).await?
        }
        UntrackedStateProjection::Full => {
            load_projected_full_rows(store, &request.identities).await?
        }
    };
    Ok(UntrackedStateGetManyResponse { rows })
}

async fn load_identity_existence(
    store: &mut (impl StorageReader + ?Sized),
    identities: &[UntrackedStateIdentity],
) -> Result<Vec<Option<UntrackedStateProjectedRow>>, LixError> {
    let mut rows = Vec::with_capacity(identities.len());
    for chunk in identities.chunks(DEFAULT_GET_VALUES_CHUNK_SIZE) {
        let keys = chunk
            .iter()
            .map(encode_untracked_state_row_key)
            .collect::<Vec<_>>();
        let result = store
            .exists_many(KvGetRequest {
                groups: vec![KvGetGroup {
                    namespace: UNTRACKED_STATE_HEADER_NAMESPACE.to_string(),
                    keys,
                }],
            })
            .await?;
        let group = result.groups.into_iter().next().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "chunked storage exists returned no result group",
            )
        })?;
        if group.namespace != UNTRACKED_STATE_HEADER_NAMESPACE {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "chunked storage exists returned namespace `{}` instead of `{}`",
                    group.namespace, UNTRACKED_STATE_HEADER_NAMESPACE
                ),
            ));
        }
        if group.exists.len() != chunk.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "chunked storage exists returned {} results for {} requested keys",
                    group.exists.len(),
                    chunk.len()
                ),
            ));
        }
        rows.extend(
            chunk
                .iter()
                .zip(group.exists)
                .map(|(identity, exists)| exists.then(|| project_identity(identity.clone()))),
        );
    }
    Ok(rows)
}

async fn load_projected_headers(
    store: &mut (impl StorageReader + ?Sized),
    identities: &[UntrackedStateIdentity],
) -> Result<Vec<Option<UntrackedStateProjectedRow>>, LixError> {
    let keys = identities
        .iter()
        .map(encode_untracked_state_row_key)
        .collect::<Vec<_>>();
    let values =
        get_values_single_namespace_chunked(store, UNTRACKED_STATE_HEADER_NAMESPACE, &keys).await?;
    identities
        .iter()
        .cloned()
        .zip(values)
        .map(|(identity, bytes)| {
            let Some(bytes) = bytes else {
                return Ok(None);
            };
            let row = crate::untracked_state::codec::decode_header_value(&bytes, identity)?;
            Ok(Some(project_header(row)))
        })
        .collect()
}

async fn load_projected_payloads(
    store: &mut (impl StorageReader + ?Sized),
    identities: &[UntrackedStateIdentity],
) -> Result<Vec<Option<UntrackedStateProjectedRow>>, LixError> {
    let keys = identities
        .iter()
        .map(encode_untracked_state_row_key)
        .collect::<Vec<_>>();
    let values =
        get_values_single_namespace_chunked(store, UNTRACKED_STATE_PAYLOAD_NAMESPACE, &keys)
            .await?;
    identities
        .iter()
        .cloned()
        .zip(values)
        .map(|(identity, bytes)| {
            let Some(bytes) = bytes else {
                return Ok(None);
            };
            let payload = crate::untracked_state::codec::decode_payload_value(&bytes)?;
            Ok(Some(project_payload(identity, payload)))
        })
        .collect()
}

async fn load_projected_full_rows(
    store: &mut (impl StorageReader + ?Sized),
    identities: &[UntrackedStateIdentity],
) -> Result<Vec<Option<UntrackedStateProjectedRow>>, LixError> {
    let mut rows = Vec::with_capacity(identities.len());
    for chunk in identities.chunks(DEFAULT_GET_VALUES_CHUNK_SIZE) {
        let keys = chunk
            .iter()
            .map(encode_untracked_state_row_key)
            .collect::<Vec<_>>();
        let result = store
            .get_values(KvGetRequest {
                groups: vec![
                    KvGetGroup {
                        namespace: UNTRACKED_STATE_HEADER_NAMESPACE.to_string(),
                        keys: keys.clone(),
                    },
                    KvGetGroup {
                        namespace: UNTRACKED_STATE_PAYLOAD_NAMESPACE.to_string(),
                        keys,
                    },
                ],
            })
            .await?;
        let mut groups = result.groups.into_iter();
        let headers = groups.next().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "storage get returned no header result group",
            )
        })?;
        let payloads = groups.next().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "storage get returned no payload result group",
            )
        })?;
        validate_value_group(&headers, UNTRACKED_STATE_HEADER_NAMESPACE, chunk.len())?;
        validate_value_group(&payloads, UNTRACKED_STATE_PAYLOAD_NAMESPACE, chunk.len())?;
        for (index, identity) in chunk.iter().cloned().enumerate() {
            let header = headers.value(index).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "storage header result group index missing",
                )
            })?;
            let payload = payloads.value(index).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "storage payload result group index missing",
                )
            })?;
            rows.push(match (header, payload) {
                (None, None) => None,
                (None, Some(_)) => return Err(orphan_payload_error(&identity)),
                (Some(_), None) => return Err(missing_payload_error_for_identity(&identity)),
                (Some(header), Some(payload)) => {
                    let mut row =
                        crate::untracked_state::codec::decode_header_value(header, identity)?;
                    let payload = crate::untracked_state::codec::decode_payload_value(payload)?;
                    row.snapshot_content = Some(payload.snapshot_content);
                    row.metadata = payload.metadata;
                    Some(project_row(row, UntrackedStateProjection::Full)?)
                }
            });
        }
    }
    Ok(rows)
}

pub(crate) async fn scan(
    store: &mut impl StorageReader,
    request: UntrackedStateScanRequest,
) -> Result<UntrackedStateScanResponse, LixError> {
    ensure_read_format(store).await?;
    if request.limit == Some(0) || request.batch_size == Some(0) {
        return Ok(UntrackedStateScanResponse {
            rows: Vec::new(),
            resume_after: None,
        });
    }
    match request.projection {
        UntrackedStateProjection::Identity => scan_identity(store, &request).await,
        UntrackedStateProjection::Header => scan_projected_headers(store, &request).await,
        UntrackedStateProjection::Payload => scan_projected_payloads(store, &request).await,
        UntrackedStateProjection::Full => scan_projected_full_rows(store, &request).await,
    }
}

async fn scan_identity(
    store: &mut (impl StorageReader + ?Sized),
    request: &UntrackedStateScanRequest,
) -> Result<UntrackedStateScanResponse, LixError> {
    let mut rows = Vec::new();
    let batch_size = request.batch_size.unwrap_or(usize::MAX);
    let output_limit = request.limit.unwrap_or(usize::MAX).min(batch_size);
    for range in scan_ranges_for_request(request) {
        let Some(mut after) = scan_after_for_range(&range, request.after.as_deref()) else {
            continue;
        };
        loop {
            let page = store
                .scan_keys(KvScanRequest {
                    namespace: UNTRACKED_STATE_HEADER_NAMESPACE.to_string(),
                    range: range.clone(),
                    after: after.clone(),
                    limit: batch_size,
                })
                .await?;
            for key in page.keys.iter() {
                let identity = decode_untracked_state_row_key(key)?;
                if identity_matches_scan(&identity, request) {
                    rows.push(project_identity(identity));
                    if rows.len() == output_limit {
                        return Ok(UntrackedStateScanResponse {
                            rows,
                            resume_after: Some(key.to_vec()),
                        });
                    }
                }
            }
            let Some(resume_after) = page.resume_after else {
                break;
            };
            after = Some(resume_after);
        }
    }
    Ok(UntrackedStateScanResponse {
        rows,
        resume_after: None,
    })
}

async fn scan_projected_headers(
    store: &mut (impl StorageReader + ?Sized),
    request: &UntrackedStateScanRequest,
) -> Result<UntrackedStateScanResponse, LixError> {
    let mut rows = Vec::new();
    let batch_size = request.batch_size.unwrap_or(usize::MAX);
    let output_limit = request.limit.unwrap_or(usize::MAX).min(batch_size);
    for range in scan_ranges_for_request(request) {
        let Some(mut after) = scan_after_for_range(&range, request.after.as_deref()) else {
            continue;
        };
        loop {
            let page = store
                .scan_entries(KvScanRequest {
                    namespace: UNTRACKED_STATE_HEADER_NAMESPACE.to_string(),
                    range: range.clone(),
                    after: after.clone(),
                    limit: batch_size,
                })
                .await?;
            for (key, value) in page.keys.iter().zip(page.values.iter()) {
                let identity = decode_untracked_state_row_key(key)?;
                let row = crate::untracked_state::codec::decode_header_value(value, identity)?;
                if row_matches_scan(&row, request) {
                    rows.push(project_header(row));
                    if rows.len() == output_limit {
                        return Ok(UntrackedStateScanResponse {
                            rows,
                            resume_after: Some(key.to_vec()),
                        });
                    }
                }
            }
            let Some(resume_after) = page.resume_after else {
                break;
            };
            after = Some(resume_after);
        }
    }
    Ok(UntrackedStateScanResponse {
        rows,
        resume_after: None,
    })
}

async fn scan_projected_payloads(
    store: &mut (impl StorageReader + ?Sized),
    request: &UntrackedStateScanRequest,
) -> Result<UntrackedStateScanResponse, LixError> {
    let mut rows = Vec::new();
    let batch_size = request.batch_size.unwrap_or(usize::MAX);
    let output_limit = request.limit.unwrap_or(usize::MAX).min(batch_size);
    for range in scan_ranges_for_request(request) {
        let Some(mut after) = scan_after_for_range(&range, request.after.as_deref()) else {
            continue;
        };
        loop {
            let page = store
                .scan_entries(KvScanRequest {
                    namespace: UNTRACKED_STATE_PAYLOAD_NAMESPACE.to_string(),
                    range: range.clone(),
                    after: after.clone(),
                    limit: batch_size,
                })
                .await?;
            for (key, value) in page.keys.iter().zip(page.values.iter()) {
                let identity = decode_untracked_state_row_key(key)?;
                if identity_matches_scan(&identity, request) {
                    let payload = crate::untracked_state::codec::decode_payload_value(value)?;
                    rows.push(project_payload(identity, payload));
                    if rows.len() == output_limit {
                        return Ok(UntrackedStateScanResponse {
                            rows,
                            resume_after: Some(key.to_vec()),
                        });
                    }
                }
            }
            let Some(resume_after) = page.resume_after else {
                break;
            };
            after = Some(resume_after);
        }
    }
    Ok(UntrackedStateScanResponse {
        rows,
        resume_after: None,
    })
}

async fn scan_projected_full_rows(
    store: &mut (impl StorageReader + ?Sized),
    request: &UntrackedStateScanRequest,
) -> Result<UntrackedStateScanResponse, LixError> {
    let mut rows = Vec::new();
    let batch_size = request.batch_size.unwrap_or(usize::MAX);
    let output_limit = request.limit.unwrap_or(usize::MAX).min(batch_size);
    for range in scan_ranges_for_request(request) {
        let Some(mut after) = scan_after_for_range(&range, request.after.as_deref()) else {
            continue;
        };
        loop {
            let header_page = store
                .scan_entries(KvScanRequest {
                    namespace: UNTRACKED_STATE_HEADER_NAMESPACE.to_string(),
                    range: range.clone(),
                    after: after.clone(),
                    limit: batch_size,
                })
                .await?;
            let payload_page = store
                .scan_entries(KvScanRequest {
                    namespace: UNTRACKED_STATE_PAYLOAD_NAMESPACE.to_string(),
                    range: range.clone(),
                    after: after.clone(),
                    limit: batch_size,
                })
                .await?;
            validate_join_pages(&header_page, &payload_page)?;
            for ((key, header), payload) in header_page
                .keys
                .iter()
                .zip(header_page.values.iter())
                .zip(payload_page.values.iter())
            {
                let identity = decode_untracked_state_row_key(key)?;
                let mut row = crate::untracked_state::codec::decode_header_value(header, identity)?;
                let payload = crate::untracked_state::codec::decode_payload_value(payload)?;
                row.snapshot_content = Some(payload.snapshot_content);
                row.metadata = payload.metadata;
                if row_matches_scan(&row, request) {
                    rows.push(project_row(row, UntrackedStateProjection::Full)?);
                    if rows.len() == output_limit {
                        return Ok(UntrackedStateScanResponse {
                            rows,
                            resume_after: Some(key.to_vec()),
                        });
                    }
                }
            }
            match (header_page.resume_after, payload_page.resume_after) {
                (None, None) => break,
                (Some(resume_after), Some(payload_resume_after))
                    if payload_resume_after == resume_after =>
                {
                    after = Some(resume_after);
                }
                _ => {
                    return Err(LixError::unknown(
                        "untracked-state header and payload scan cursors diverged",
                    ))
                }
            }
        }
    }
    Ok(UntrackedStateScanResponse {
        rows,
        resume_after: None,
    })
}

fn scan_after_for_range(range: &KvScanRange, after: Option<&[u8]>) -> Option<Option<Vec<u8>>> {
    let Some(after) = after else {
        return Some(None);
    };
    if key_in_range(after, range) {
        return Some(Some(after.to_vec()));
    }
    if range_is_exhausted_by_after(range, after) {
        return None;
    }
    Some(None)
}

fn range_is_exhausted_by_after(range: &KvScanRange, after: &[u8]) -> bool {
    match range {
        KvScanRange::Prefix(prefix) => prefix_upper_bound(prefix)
            .as_deref()
            .is_some_and(|upper| upper <= after),
        KvScanRange::Range { end, .. } => end.as_slice() <= after,
    }
}

fn key_in_range(key: &[u8], range: &KvScanRange) -> bool {
    match range {
        KvScanRange::Prefix(prefix) => key.starts_with(prefix),
        KvScanRange::Range { start, end } => start.as_slice() <= key && key < end.as_slice(),
    }
}

fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for index in (0..upper.len()).rev() {
        if upper[index] != 0xFF {
            upper[index] += 1;
            upper.truncate(index + 1);
            return Some(upper);
        }
    }
    None
}

fn project_identity(identity: UntrackedStateIdentity) -> UntrackedStateProjectedRow {
    UntrackedStateProjectedRow {
        identity,
        created_at: None,
        updated_at: None,
        global: None,
        snapshot_content: None,
        metadata: None,
        deleted: None,
    }
}

fn project_header(row: UntrackedStateRow) -> UntrackedStateProjectedRow {
    UntrackedStateProjectedRow {
        identity: UntrackedStateIdentity {
            version_id: row.version_id,
            schema_key: row.schema_key,
            entity_id: row.entity_id,
            file_id: row.file_id,
        },
        created_at: Some(row.created_at),
        updated_at: Some(row.updated_at),
        global: Some(row.global),
        snapshot_content: None,
        metadata: None,
        deleted: Some(false),
    }
}

fn project_payload(
    identity: UntrackedStateIdentity,
    payload: crate::untracked_state::codec::UntrackedStatePayloadValue,
) -> UntrackedStateProjectedRow {
    UntrackedStateProjectedRow {
        identity,
        created_at: None,
        updated_at: None,
        global: None,
        snapshot_content: Some(payload.snapshot_content),
        metadata: payload.metadata,
        deleted: Some(false),
    }
}

fn project_row(
    row: UntrackedStateRow,
    projection: UntrackedStateProjection,
) -> Result<UntrackedStateProjectedRow, LixError> {
    let deleted = row.snapshot_content.is_none();
    let identity = UntrackedStateIdentity {
        version_id: row.version_id,
        schema_key: row.schema_key,
        entity_id: row.entity_id,
        file_id: row.file_id,
    };
    let include_header = matches!(
        projection,
        UntrackedStateProjection::Header | UntrackedStateProjection::Full
    );
    let include_payload = matches!(
        projection,
        UntrackedStateProjection::Payload | UntrackedStateProjection::Full
    );
    let metadata = if include_payload {
        row.metadata
            .as_deref()
            .map(|json| crate::parse_row_metadata(json, "untracked_state metadata"))
            .transpose()?
    } else {
        None
    };
    Ok(UntrackedStateProjectedRow {
        identity,
        created_at: include_header.then_some(row.created_at),
        updated_at: include_header.then_some(row.updated_at),
        global: include_header.then_some(row.global),
        snapshot_content: include_payload.then_some(row.snapshot_content).flatten(),
        metadata,
        deleted: (include_header || include_payload).then_some(deleted),
    })
}

pub(crate) fn stage_rows<'a, I>(writes: &mut StorageWriteSet, rows: I) -> Result<(), LixError>
where
    I: IntoIterator<Item = UntrackedStateRowRef<'a>>,
{
    stage_format_marker(writes);
    let rows = rows.into_iter();
    let mut header_group = KvWriteGroup::new(UNTRACKED_STATE_HEADER_NAMESPACE);
    let mut payload_group = KvWriteGroup::new(UNTRACKED_STATE_PAYLOAD_NAMESPACE);
    let lower_bound = rows.size_hint().0;
    header_group.reserve(lower_bound);
    payload_group.reserve(lower_bound);
    for row in rows {
        let key = encode_untracked_state_row_key_ref(row.into());
        if row.snapshot_content.is_none() {
            header_group.delete(key.clone());
            payload_group.delete(key);
        } else {
            header_group.put(
                key.clone(),
                crate::untracked_state::codec::encode_header_value_ref(row),
            );
            let payload = crate::untracked_state::codec::encode_payload_value_ref(row)
                .ok_or_else(|| LixError::unknown("live untracked row missing payload"))?;
            payload_group.put(key, payload);
        }
    }
    header_group.sort_point_ops_by_key();
    payload_group.sort_point_ops_by_key();
    writes.push_group(header_group);
    writes.push_group(payload_group);
    Ok(())
}

pub(crate) fn stage_delete_rows<'a, I>(writes: &mut StorageWriteSet, identities: I)
where
    I: IntoIterator<Item = UntrackedStateIdentityRef<'a>>,
{
    stage_format_marker(writes);
    let identities = identities.into_iter();
    let mut header_group = KvWriteGroup::new(UNTRACKED_STATE_HEADER_NAMESPACE);
    let mut payload_group = KvWriteGroup::new(UNTRACKED_STATE_PAYLOAD_NAMESPACE);
    let lower_bound = identities.size_hint().0;
    header_group.reserve(lower_bound);
    payload_group.reserve(lower_bound);
    for identity in identities {
        let key = encode_untracked_state_row_key_ref(identity);
        header_group.delete(key.clone());
        payload_group.delete(key);
    }
    header_group.sort_point_ops_by_key();
    payload_group.sort_point_ops_by_key();
    writes.push_group(header_group);
    writes.push_group(payload_group);
}

#[allow(dead_code)]
pub(crate) fn stage_delete_all_rows(writes: &mut StorageWriteSet) {
    stage_format_marker(writes);
    writes.delete_range(
        UNTRACKED_STATE_HEADER_NAMESPACE,
        KvScanRange::prefix(Vec::new()),
    );
    writes.delete_range(
        UNTRACKED_STATE_PAYLOAD_NAMESPACE,
        KvScanRange::prefix(Vec::new()),
    );
}

async fn ensure_read_format(store: &mut (impl StorageReader + ?Sized)) -> Result<(), LixError> {
    let marker = store
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: UNTRACKED_STATE_FORMAT_NAMESPACE.to_string(),
                keys: vec![UNTRACKED_STATE_FORMAT_KEY.to_vec()],
            }],
        })
        .await?
        .groups
        .into_iter()
        .next()
        .and_then(|group| group.single_value_owned());
    match marker.as_deref() {
        Some(UNTRACKED_STATE_FORMAT_VALUE) => Ok(()),
        Some(value) => Err(LixError::unknown(format!(
            "unsupported untracked-state storage format marker `{}`",
            String::from_utf8_lossy(value)
        ))),
        None => {
            let has_header = namespace_has_any_key(store, UNTRACKED_STATE_HEADER_NAMESPACE).await?;
            let has_payload =
                namespace_has_any_key(store, UNTRACKED_STATE_PAYLOAD_NAMESPACE).await?;
            let has_v1 =
                namespace_has_any_key(store, LEGACY_UNTRACKED_STATE_ROW_NAMESPACE_V1).await?;
            let has_legacy =
                namespace_has_any_key(store, LEGACY_UNTRACKED_STATE_ROW_NAMESPACE).await?;
            if has_header || has_payload || has_v1 || has_legacy {
                return Err(LixError::unknown(
                    "untracked-state rows exist without a storage format marker",
                ));
            }
            Ok(())
        }
    }
}

async fn namespace_has_any_key(
    store: &mut (impl StorageReader + ?Sized),
    namespace: &str,
) -> Result<bool, LixError> {
    let page = store
        .scan_keys(KvScanRequest {
            namespace: namespace.to_string(),
            range: KvScanRange::prefix(Vec::new()),
            after: None,
            limit: 1,
        })
        .await?;
    Ok(!page.keys.is_empty())
}

fn stage_format_marker(writes: &mut StorageWriteSet) {
    writes.put(
        UNTRACKED_STATE_FORMAT_NAMESPACE,
        UNTRACKED_STATE_FORMAT_KEY.to_vec(),
        UNTRACKED_STATE_FORMAT_VALUE.to_vec(),
    );
}

fn scan_ranges_for_request(request: &UntrackedStateScanRequest) -> Vec<KvScanRange> {
    let mut ranges = Vec::new();
    if request.filter.version_ids.is_empty() {
        ranges.push(KvScanRange::prefix(Vec::new()));
        return ranges;
    }

    for version_id in &request.filter.version_ids {
        if request.filter.schema_keys.is_empty() {
            ranges.push(KvScanRange::prefix(row_key_version_prefix(version_id)));
        } else {
            for schema_key in &request.filter.schema_keys {
                ranges.push(KvScanRange::prefix(row_key_version_schema_prefix(
                    version_id, schema_key,
                )));
            }
        }
    }
    ranges.sort_by(|left, right| range_start(left).cmp(range_start(right)));
    ranges.dedup_by(|left, right| range_start(left) == range_start(right));
    ranges
}

fn range_start(range: &KvScanRange) -> &[u8] {
    match range {
        KvScanRange::Prefix(prefix) => prefix,
        KvScanRange::Range { start, .. } => start,
    }
}

fn row_key_version_prefix(version_id: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(encoded_component_len(version_id));
    push_component(&mut out, version_id);
    out
}

fn row_key_version_schema_prefix(version_id: &str, schema_key: &str) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(encoded_component_len(version_id) + encoded_component_len(schema_key));
    push_component(&mut out, version_id);
    push_component(&mut out, schema_key);
    out
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

fn nullable_matches_filters(value: &Option<String>, filters: &[NullableKeyFilter<String>]) -> bool {
    filters.is_empty()
        || filters.iter().any(|filter| match filter {
            NullableKeyFilter::Any => true,
            NullableKeyFilter::Null => value.is_none(),
            NullableKeyFilter::Value(expected) => value.as_ref() == Some(expected),
        })
}

fn validate_value_group(
    group: &KvValueGroup,
    namespace: &'static str,
    expected_len: usize,
) -> Result<(), LixError> {
    if group.namespace() != namespace {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "storage get returned namespace `{}` instead of `{namespace}`",
                group.namespace()
            ),
        ));
    }
    if group.len() != expected_len {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "storage get returned {} results for {expected_len} requested keys",
                group.len()
            ),
        ));
    }
    Ok(())
}

fn validate_join_pages(
    header_page: &KvEntryPage,
    payload_page: &KvEntryPage,
) -> Result<(), LixError> {
    if header_page.len() != payload_page.len() {
        return Err(LixError::unknown(format!(
            "untracked-state header/payload scan length mismatch: {} headers, {} payloads",
            header_page.len(),
            payload_page.len()
        )));
    }
    for (header_key, payload_key) in header_page.keys.iter().zip(payload_page.keys.iter()) {
        if header_key != payload_key {
            return Err(LixError::unknown(
                "untracked-state header and payload keys diverged during scan",
            ));
        }
    }
    Ok(())
}

fn missing_payload_error_for_identity(identity: &UntrackedStateIdentity) -> LixError {
    LixError::unknown(format!(
        "untracked-state payload missing for header identity `{}` `{}`",
        identity.version_id, identity.schema_key
    ))
}

fn orphan_payload_error(identity: &UntrackedStateIdentity) -> LixError {
    LixError::unknown(format!(
        "untracked-state payload exists without header for identity `{}` `{}`",
        identity.version_id, identity.schema_key
    ))
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
    if len < 0x80 {
        out.push(len as u8);
        return;
    }
    while len >= 0x80 {
        out.push((len as u8 & 0x7f) | 0x80);
        len >>= 7;
    }
    out.push(len as u8);
}

fn varint_len(mut len: usize) -> usize {
    if len < 0x80 {
        return 1;
    }
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
            if *cursor - start != varint_len(len) {
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
    use crate::untracked_state::{
        MaterializedUntrackedStateRow, UntrackedStateContext, UntrackedStateRowRequest,
    };

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

    fn materialized_identity(row: &MaterializedUntrackedStateRow) -> UntrackedStateIdentity {
        UntrackedStateIdentity {
            version_id: row.version_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    async fn read_scan(
        context: &UntrackedStateContext,
        storage: StorageContext,
        request: UntrackedStateScanRequest,
    ) -> Result<Vec<MaterializedUntrackedStateRow>, LixError> {
        context
            .reader(storage)
            .scan(UntrackedStateScanRequest {
                projection: crate::untracked_state::UntrackedStateProjection::Full,
                ..request
            })
            .await?
            .rows
            .into_iter()
            .map(|row| row.into_materialized_full())
            .collect()
    }

    async fn read_get(
        context: &UntrackedStateContext,
        storage: StorageContext,
        requests: &[UntrackedStateRowRequest],
        projection: crate::untracked_state::UntrackedStateProjection,
    ) -> Result<Vec<Option<MaterializedUntrackedStateRow>>, LixError> {
        let mut rows = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
        let mut identities = Vec::new();
        let mut indices = Vec::new();
        for (index, request) in requests.iter().enumerate() {
            if let Some(identity) = UntrackedStateIdentity::from_exact_row_request(request) {
                identities.push(identity);
                indices.push(index);
            }
        }
        if identities.is_empty() {
            return Ok(rows);
        }
        let loaded = context
            .reader(storage)
            .get_many(crate::untracked_state::UntrackedStateGetManyRequest {
                identities,
                projection: if projection == crate::untracked_state::UntrackedStateProjection::Full
                {
                    crate::untracked_state::UntrackedStateProjection::Full
                } else {
                    projection
                },
            })
            .await?
            .rows;
        for (index, row) in indices.into_iter().zip(loaded) {
            rows[index] = row.map(|row| row.into_materialized_full()).transpose()?;
        }
        Ok(rows)
    }

    #[tokio::test]
    async fn scan_and_get_many_are_separate_projected_apis() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let first = untracked_row("global", "lix_key_value", "ui-tab-a");
        let second = untracked_row("global", "lix_key_value", "ui-tab-b");

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        write_materialized_rows_to_store(&context, transaction.as_mut(), &[first.clone(), second])
            .await;
        transaction.commit().await.expect("commit should succeed");

        let scan_rows = context
            .reader(storage.clone())
            .scan(UntrackedStateScanRequest {
                filter: crate::untracked_state::UntrackedStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    version_ids: vec!["global".to_string()],
                    ..Default::default()
                },
                projection: crate::untracked_state::UntrackedStateProjection::Identity,
                limit: Some(1),
                ..Default::default()
            })
            .await
            .expect("scan should succeed")
            .rows;
        assert_eq!(scan_rows.len(), 1);
        assert!(scan_rows[0].snapshot_content.is_none());

        let mut get_rows = context
            .reader(storage.clone())
            .get_many(crate::untracked_state::UntrackedStateGetManyRequest {
                identities: vec![materialized_identity(&first)],
                projection: crate::untracked_state::UntrackedStateProjection::Full,
            })
            .await
            .expect("get_many should succeed")
            .rows;
        assert_eq!(
            get_rows
                .pop()
                .flatten()
                .map(|row| row.into_materialized_full())
                .transpose()
                .expect("row should materialize"),
            Some(first)
        );
    }

    #[tokio::test]
    async fn get_many_header_preserves_order_and_missing_rows() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let row = untracked_row("global", "lix_key_value", "ui-tab");
        let missing = UntrackedStateIdentity {
            entity_id: crate::entity_identity::EntityIdentity::single("missing"),
            ..materialized_identity(&row)
        };

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

        let rows = context
            .reader(storage.clone())
            .get_many(crate::untracked_state::UntrackedStateGetManyRequest {
                identities: vec![missing, materialized_identity(&row)],
                projection: crate::untracked_state::UntrackedStateProjection::Header,
            })
            .await
            .expect("get_many should succeed")
            .rows;

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], None);
        let loaded = rows[1].as_ref().expect("second row should load");
        assert_eq!(loaded.created_at.as_deref(), Some(row.created_at.as_str()));
        assert_eq!(loaded.updated_at.as_deref(), Some(row.updated_at.as_str()));
        assert_eq!(loaded.global, Some(row.global));
        assert_eq!(loaded.snapshot_content, None);
    }

    #[tokio::test]
    async fn get_many_identity_preserves_order_and_misses_without_materialized_rows() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let row = untracked_row("global", "lix_key_value", "ui-tab");
        let missing = UntrackedStateIdentity {
            entity_id: crate::entity_identity::EntityIdentity::single("missing"),
            ..materialized_identity(&row)
        };

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

        let rows = context
            .reader(storage.clone())
            .get_many(crate::untracked_state::UntrackedStateGetManyRequest {
                identities: vec![missing, materialized_identity(&row)],
                projection: crate::untracked_state::UntrackedStateProjection::Identity,
            })
            .await
            .expect("get_many should succeed")
            .rows;

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], None);
        let loaded = rows[1].as_ref().expect("second row should exist");
        assert_eq!(loaded.identity, materialized_identity(&row));
        assert_eq!(loaded.created_at, None);
        assert_eq!(loaded.updated_at, None);
        assert_eq!(loaded.global, None);
        assert_eq!(loaded.snapshot_content, None);
        assert_eq!(loaded.metadata, None);
        assert_eq!(loaded.deleted, None);
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

        let request = UntrackedStateRowRequest {
            schema_key: "lix_key_value".to_string(),
            version_id: "global".to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("ui-tab"),
            file_id: NullableKeyFilter::Null,
        };
        let loaded = read_get(
            &context,
            storage.clone(),
            std::slice::from_ref(&request),
            crate::untracked_state::UntrackedStateProjection::Full,
        )
        .await
        .map(|rows| rows.into_iter().next().flatten())
        .expect("load should succeed");
        assert_eq!(loaded, Some(row));
    }

    #[tokio::test]
    async fn writes_install_untracked_format_marker() {
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

        let mut reader = storage
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let marker = reader
            .get_values(KvGetRequest {
                groups: vec![KvGetGroup {
                    namespace: UNTRACKED_STATE_FORMAT_NAMESPACE.to_string(),
                    keys: vec![UNTRACKED_STATE_FORMAT_KEY.to_vec()],
                }],
            })
            .await
            .expect("marker read should succeed");
        assert_eq!(
            marker.groups[0].single_value_owned().as_deref(),
            Some(UNTRACKED_STATE_FORMAT_VALUE)
        );
    }

    #[tokio::test]
    async fn read_rejects_unmarked_current_untracked_rows() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let row = crate::untracked_state::UntrackedStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single("ui-tab"),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some("{\"key\":\"ui-tab\",\"value\":\"value\"}".to_string()),
            metadata: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: true,
            version_id: "global".to_string(),
        };

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        writes.put(
            UNTRACKED_STATE_HEADER_NAMESPACE,
            encode_untracked_state_row_key_ref(row.as_ref().into()),
            crate::untracked_state::codec::encode_header_value_ref(row.as_ref()),
        );
        writes
            .apply(transaction.as_mut())
            .await
            .expect("manual unmarked row should write");
        transaction.commit().await.expect("commit should succeed");

        let error = read_scan(
            &context,
            storage.clone(),
            UntrackedStateScanRequest::default(),
        )
        .await
        .expect_err("unmarked rows should fail the format gate");
        assert!(
            error.message.contains("without a storage format marker"),
            "error should describe missing marker: {error:?}"
        );
    }

    #[tokio::test]
    async fn read_rejects_unmarked_legacy_untracked_rows() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        writes.put(LEGACY_UNTRACKED_STATE_ROW_NAMESPACE, vec![1], vec![1]);
        writes
            .apply(transaction.as_mut())
            .await
            .expect("manual legacy row should write");
        transaction.commit().await.expect("commit should succeed");

        let error = read_scan(
            &context,
            storage.clone(),
            UntrackedStateScanRequest::default(),
        )
        .await
        .expect_err("legacy unmarked rows should fail the format gate");
        assert!(
            error.message.contains("without a storage format marker"),
            "error should describe missing marker: {error:?}"
        );
    }

    #[tokio::test]
    async fn header_projection_tolerates_missing_payload_but_full_projection_rejects_it() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let row = untracked_row("global", "lix_key_value", "header-only");

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        let canonical =
            crate::test_support::untracked_state_row_from_materialized(&mut writes, &row)
                .expect("row should canonicalize");
        stage_format_marker(&mut writes);
        writes.put(
            UNTRACKED_STATE_HEADER_NAMESPACE,
            encode_untracked_state_row_key_ref(canonical.as_ref().into()),
            crate::untracked_state::codec::encode_header_value_ref(canonical.as_ref()),
        );
        writes
            .apply(transaction.as_mut())
            .await
            .expect("manual header row should write");
        transaction.commit().await.expect("commit should succeed");

        let header_rows = context
            .reader(storage.clone())
            .scan(UntrackedStateScanRequest {
                projection: crate::untracked_state::UntrackedStateProjection::Header,
                ..Default::default()
            })
            .await
            .expect("header scan should not need payload");
        assert_eq!(header_rows.rows.len(), 1);
        assert_eq!(
            header_rows.rows[0].created_at.as_deref(),
            Some(row.created_at.as_str())
        );
        assert_eq!(header_rows.rows[0].snapshot_content, None);

        let error = read_scan(
            &context,
            storage.clone(),
            UntrackedStateScanRequest::default(),
        )
        .await
        .expect_err("full scan should reject missing payload");
        assert!(
            error.message.contains("payload"),
            "error should describe missing payload: {error:?}"
        );
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

        let untracked_rows = read_scan(
            &context,
            storage.clone(),
            UntrackedStateScanRequest::default(),
        )
        .await
        .expect("untracked scan should succeed");
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

        let rows = read_scan(
            &context,
            storage.clone(),
            UntrackedStateScanRequest {
                filter: crate::untracked_state::UntrackedStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    version_ids: vec!["version-a".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
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

        let full_rows = read_scan(
            &context,
            storage.clone(),
            UntrackedStateScanRequest {
                limit: Some(0),
                ..Default::default()
            },
        )
        .await
        .expect("full scan should succeed");
        assert!(full_rows.is_empty());

        let identity_rows = read_scan(
            &context,
            storage.clone(),
            UntrackedStateScanRequest {
                projection: crate::untracked_state::UntrackedStateProjection::Identity,
                limit: Some(0),
                ..Default::default()
            },
        )
        .await
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

        let rows = read_scan(
            &context,
            storage.clone(),
            UntrackedStateScanRequest {
                filter: crate::untracked_state::UntrackedStateFilter {
                    version_ids: vec!["version-a".to_string()],
                    ..Default::default()
                },
                limit: Some(2),
                ..Default::default()
            },
        )
        .await
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

        let loaded = read_get(
            &context,
            storage.clone(),
            &[
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
            ],
            crate::untracked_state::UntrackedStateProjection::Full,
        )
        .await
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

        let rows = context
            .reader(storage.clone())
            .scan(UntrackedStateScanRequest {
                projection: crate::untracked_state::UntrackedStateProjection::Identity,
                ..Default::default()
            })
            .await
            .expect("scan should succeed")
            .rows;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].identity.entity_id, row.entity_id);
        assert_eq!(rows[0].identity.schema_key, row.schema_key);
        assert_eq!(rows[0].identity.version_id, row.version_id);
        assert_eq!(rows[0].identity.file_id, row.file_id);
        assert_eq!(rows[0].global, None);
        assert_eq!(rows[0].created_at, None);
        assert_eq!(rows[0].updated_at, None);
        assert_eq!(rows[0].snapshot_content, None);

        let full_rows = read_scan(
            &context,
            storage.clone(),
            UntrackedStateScanRequest::default(),
        )
        .await
        .expect("full scan should succeed");

        assert_eq!(full_rows.len(), 1);
        assert_eq!(full_rows[0].snapshot_content, row.snapshot_content);
        assert_eq!(full_rows[0].created_at, row.created_at);
        assert_eq!(full_rows[0].updated_at, row.updated_at);
    }

    #[tokio::test]
    async fn scan_identity_returns_projected_identities_only() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let first = untracked_row(crate::GLOBAL_VERSION_ID, "lix_key_value", "a");
        let mut second = untracked_row(crate::GLOBAL_VERSION_ID, "lix_key_value", "b");
        second.file_id = Some("settings.json".to_string());
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

        let page = context
            .reader(storage.clone())
            .scan(crate::untracked_state::UntrackedStateScanRequest {
                filter: crate::untracked_state::UntrackedStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    file_ids: vec![NullableKeyFilter::Any],
                    ..Default::default()
                },
                projection: crate::untracked_state::UntrackedStateProjection::Identity,
                limit: None,
                after: None,
                batch_size: None,
            })
            .await
            .expect("identity scan should succeed");

        assert_eq!(page.resume_after, None);
        assert_eq!(
            page.rows
                .iter()
                .map(|row| row.identity.clone())
                .collect::<Vec<_>>(),
            vec![
                materialized_identity(&first),
                materialized_identity(&second)
            ]
        );
        assert!(page.rows.iter().all(|row| {
            row.created_at.is_none()
                && row.updated_at.is_none()
                && row.global.is_none()
                && row.snapshot_content.is_none()
                && row.metadata.is_none()
                && row.deleted.is_none()
        }));
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
    fn row_key_golden_bytes_are_intentional() {
        let null_file = UntrackedStateIdentity {
            version_id: "v".to_string(),
            schema_key: "s".to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("e"),
            file_id: None,
        };
        assert_eq!(
            encode_untracked_state_row_key(&null_file),
            b"\x01v\x01s\x01\x01e\x00",
            "null-file row key format should stay intentional"
        );

        let with_file = UntrackedStateIdentity {
            version_id: "v".to_string(),
            schema_key: "s".to_string(),
            entity_id: crate::entity_identity::EntityIdentity::tuple(vec![
                "left".to_string(),
                "right".to_string(),
            ])
            .expect("tuple identity should be valid"),
            file_id: Some("f".to_string()),
        };
        assert_eq!(
            encode_untracked_state_row_key(&with_file),
            b"\x01v\x01s\x02\x04left\x05right\x01\x01f",
            "file-backed tuple row key format should stay intentional"
        );

        let boundary = "x".repeat(128);
        let boundary_identity = UntrackedStateIdentity {
            version_id: boundary.clone(),
            schema_key: "s".to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("e"),
            file_id: None,
        };
        let encoded = encode_untracked_state_row_key(&boundary_identity);
        assert_eq!(&encoded[..2], &[0x80, 0x01]);
        assert_eq!(&encoded[2..130], boundary.as_bytes());
    }

    #[test]
    fn row_key_filter_prefixes_match_component_boundaries() {
        assert_eq!(row_key_version_prefix("v"), b"\x01v");
        assert_eq!(
            row_key_version_schema_prefix("v", "schema"),
            b"\x01v\x06schema"
        );

        let request = UntrackedStateScanRequest {
            filter: crate::untracked_state::UntrackedStateFilter {
                version_ids: vec!["v2".to_string(), "v1".to_string()],
                schema_keys: vec!["b".to_string(), "a".to_string()],
                ..Default::default()
            },
            ..Default::default()
        };
        let starts = scan_ranges_for_request(&request)
            .into_iter()
            .map(|range| range_start(&range).to_vec())
            .collect::<Vec<_>>();
        assert_eq!(
            starts,
            vec![
                b"\x02v1\x01a".to_vec(),
                b"\x02v1\x01b".to_vec(),
                b"\x02v2\x01a".to_vec(),
                b"\x02v2\x01b".to_vec(),
            ]
        );
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

        let request = UntrackedStateRowRequest {
            schema_key: "lix_key_value".to_string(),
            version_id: "global".to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("ui-tab"),
            file_id: NullableKeyFilter::Null,
        };
        let loaded = read_get(
            &context,
            storage.clone(),
            std::slice::from_ref(&request),
            crate::untracked_state::UntrackedStateProjection::Full,
        )
        .await
        .map(|rows| rows.into_iter().next().flatten())
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
