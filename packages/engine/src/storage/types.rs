use async_trait::async_trait;
use std::collections::BTreeMap;

use crate::backend;
use crate::backend::BytePage;
use crate::backend::BytePageBuilder;
use crate::LixError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KvScanRange {
    Prefix(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
}

impl KvScanRange {
    pub(crate) fn prefix(prefix: impl Into<Vec<u8>>) -> Self {
        Self::Prefix(prefix.into())
    }

    pub(crate) fn range(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self::Range {
            start: start.into(),
            end: end.into(),
        }
    }
}

impl From<KvScanRange> for backend::BackendKvScanRange {
    fn from(range: KvScanRange) -> Self {
        match range {
            KvScanRange::Prefix(prefix) => Self::Prefix(prefix),
            KvScanRange::Range { start, end } => Self::Range { start, end },
        }
    }
}

#[async_trait]
pub(crate) trait StorageReader: Send {
    async fn get_values(&mut self, request: KvGetRequest) -> Result<KvValueBatch, LixError>;

    async fn exists_many(&mut self, request: KvGetRequest) -> Result<KvExistsBatch, LixError>;

    async fn scan_keys(&mut self, request: KvScanRequest) -> Result<KvKeyPage, LixError>;

    async fn scan_values(&mut self, request: KvScanRequest) -> Result<KvValuePage, LixError>;

    async fn scan_entries(&mut self, request: KvScanRequest) -> Result<KvEntryPage, LixError>;

    async fn scan2(&mut self, request: KvScan2Request) -> Result<KvScan2Page, LixError> {
        storage_scan2_fallback(self, request).await
    }

    async fn read_v3(&mut self, request: KvReadV3Request) -> Result<KvReadV3Page, LixError> {
        storage_read_v3_fallback(self, request).await
    }

    async fn read4(&mut self, request: KvTableReadRequest) -> Result<KvRead4Page, LixError> {
        storage_read4_unsupported(self, request).await
    }
}

#[async_trait]
pub(crate) trait StorageWriter: StorageReader {
    async fn write_kv_batch(&mut self, batch: KvWriteBatch) -> Result<KvWriteStats, LixError>;
}

pub(crate) const DEFAULT_GET_VALUES_CHUNK_SIZE: usize = 2048;

pub(crate) async fn get_values_single_namespace_chunked(
    store: &mut (impl StorageReader + ?Sized),
    namespace: &'static str,
    keys: &[Vec<u8>],
) -> Result<Vec<Option<Vec<u8>>>, LixError> {
    let mut values = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(DEFAULT_GET_VALUES_CHUNK_SIZE) {
        let result = store
            .get_values(KvGetRequest {
                groups: vec![KvGetGroup {
                    namespace: namespace.to_string(),
                    keys: chunk.to_vec(),
                }],
            })
            .await?;
        let group = result.groups.into_iter().next().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "chunked storage get returned no result group",
            )
        })?;
        if group.namespace() != namespace {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "chunked storage get returned namespace `{}` instead of `{namespace}`",
                    group.namespace()
                ),
            ));
        }
        if group.len() != chunk.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "chunked storage get returned {} results for {} requested keys",
                    group.len(),
                    chunk.len()
                ),
            ));
        }
        values.extend(group.values_iter().map(|value| value.map(<[u8]>::to_vec)));
    }
    Ok(values)
}

async fn storage_scan2_fallback<T>(
    store: &mut T,
    request: KvScan2Request,
) -> Result<KvScan2Page, LixError>
where
    T: StorageReader + ?Sized,
{
    match request.projection {
        KvScan2Projection::KeysOnly => {
            let page = store
                .scan_keys(KvScanRequest {
                    namespace: request.namespace,
                    range: request.range,
                    after: request.after,
                    limit: request.page_size,
                })
                .await?;
            Ok(KvScan2Page {
                keys: page.keys,
                values: None,
                resume_after: page.resume_after,
            })
        }
        KvScan2Projection::FullValue => {
            let page = store
                .scan_entries(KvScanRequest {
                    namespace: request.namespace,
                    range: request.range,
                    after: request.after,
                    limit: request.page_size,
                })
                .await?;
            Ok(KvScan2Page {
                keys: page.keys,
                values: Some(page.values),
                resume_after: page.resume_after,
            })
        }
        KvScan2Projection::ValuePart(part) => {
            let page = store
                .scan_entries(KvScanRequest {
                    namespace: request.namespace,
                    range: request.range,
                    after: request.after,
                    limit: request.page_size,
                })
                .await?;
            let mut values = BytePageBuilder::with_capacity(page.values.len(), 0);
            for value in page.values.iter() {
                values.push(project_value_part(value, &part)?);
            }
            Ok(KvScan2Page {
                keys: page.keys,
                values: Some(values.finish()),
                resume_after: page.resume_after,
            })
        }
    }
}

async fn storage_read_v3_scan_spans_fallback<T>(
    store: &mut T,
    namespace: String,
    spans: Vec<KvKeySpan>,
    after: Option<Vec<u8>>,
    page_size: usize,
    projection: KvReadV3Projection,
) -> Result<KvReadV3Page, LixError>
where
    T: StorageReader + ?Sized,
{
    match projection {
        KvReadV3Projection::KeysOnly => {
            let mut keys = BytePageBuilder::new();
            let mut resume_after = None;
            let spans = normalize_spans(spans);
            let span_count = spans.len();
            for (span_index, span) in spans.into_iter().enumerate() {
                let Some(after) = scan_after_for_span(&span, after.as_deref()) else {
                    continue;
                };
                let remaining = page_size.saturating_sub(keys.len());
                if remaining == 0 {
                    break;
                }
                let page = store
                    .scan_keys(KvScanRequest {
                        namespace: namespace.clone(),
                        range: span_scan_range(&span),
                        after,
                        limit: remaining,
                    })
                    .await?;
                for key in page.keys.iter() {
                    keys.push(key);
                }
                resume_after = page.resume_after;
                if keys.len() == page_size {
                    if resume_after.is_some() || span_index + 1 < span_count {
                        resume_after = last_key(&keys);
                    }
                    break;
                }
                if resume_after.is_some() {
                    break;
                }
            }
            Ok(KvReadV3Page {
                keys: keys.finish(),
                presence: KvReadV3Presence::All,
                values: Vec::new(),
                request_indexes: None,
                resume_after,
            })
        }
        KvReadV3Projection::ValueParts(parts) => {
            let mut keys = BytePageBuilder::new();
            let mut value_builders = parts
                .iter()
                .map(|_| BytePageBuilder::new())
                .collect::<Vec<_>>();
            let mut resume_after = None;
            let spans = normalize_spans(spans);
            let span_count = spans.len();
            for (span_index, span) in spans.into_iter().enumerate() {
                let Some(after) = scan_after_for_span(&span, after.as_deref()) else {
                    continue;
                };
                let remaining = page_size.saturating_sub(keys.len());
                if remaining == 0 {
                    break;
                }
                let page = store
                    .scan_entries(KvScanRequest {
                        namespace: namespace.clone(),
                        range: span_scan_range(&span),
                        after,
                        limit: remaining,
                    })
                    .await?;
                for (index, key) in page.keys.iter().enumerate() {
                    let value = page.value(index).ok_or_else(|| {
                        LixError::unknown("storage scan plan fallback value missing")
                    })?;
                    keys.push(key);
                    for (part, builder) in parts.iter().zip(value_builders.iter_mut()) {
                        builder.push(project_read_v3_value_part(value, *part)?);
                    }
                }
                resume_after = page.resume_after;
                if keys.len() == page_size {
                    if resume_after.is_some() || span_index + 1 < span_count {
                        resume_after = last_key(&keys);
                    }
                    break;
                }
                if resume_after.is_some() {
                    break;
                }
            }
            Ok(KvReadV3Page {
                keys: keys.finish(),
                presence: KvReadV3Presence::All,
                values: value_builders
                    .into_iter()
                    .map(BytePageBuilder::finish)
                    .collect(),
                request_indexes: None,
                resume_after,
            })
        }
    }
}

async fn storage_read_v3_fallback<T>(
    store: &mut T,
    request: KvReadV3Request,
) -> Result<KvReadV3Page, LixError>
where
    T: StorageReader + ?Sized,
{
    match request.source {
        KvReadV3Source::Keys { keys } => {
            storage_read_v3_keys_fallback(
                store,
                request.namespace,
                keys,
                request.projection,
                request.order,
            )
            .await
        }
        KvReadV3Source::KeysOrSpans { keys, spans } => match request.strategy {
            KvReadV3Strategy::Scan => {
                storage_read_v3_scan_then_reorder_fallback(
                    store,
                    request.namespace,
                    keys,
                    spans,
                    request.projection,
                    request.order,
                )
                .await
            }
            KvReadV3Strategy::Auto | KvReadV3Strategy::Points => {
                storage_read_v3_keys_fallback(
                    store,
                    request.namespace,
                    keys,
                    request.projection,
                    request.order,
                )
                .await
            }
        },
        KvReadV3Source::Spans { spans, after } => {
            let page_size = request.page_size.unwrap_or(usize::MAX);
            storage_read_v3_scan_spans_fallback(
                store,
                request.namespace,
                spans,
                after,
                page_size,
                request.projection,
            )
            .await
        }
    }
}

async fn storage_read4_unsupported<T>(
    store: &mut T,
    request: KvTableReadRequest,
) -> Result<KvRead4Page, LixError>
where
    T: StorageReader + ?Sized,
{
    let _ = (store, request);
    Err(LixError::unknown("storage read4 is not implemented"))
}

async fn storage_read_v3_keys_fallback<T>(
    store: &mut T,
    namespace: String,
    keys: Vec<Vec<u8>>,
    projection: KvReadV3Projection,
    order: KvReadV3Order,
) -> Result<KvReadV3Page, LixError>
where
    T: StorageReader + ?Sized,
{
    match projection {
        KvReadV3Projection::KeysOnly => {
            let result = store
                .exists_many(KvGetRequest {
                    groups: vec![KvGetGroup {
                        namespace,
                        keys: keys.clone(),
                    }],
                })
                .await?;
            let group = result.groups.into_iter().next().ok_or_else(|| {
                LixError::unknown("storage read_v3 fallback exists returned no result group")
            })?;
            let mut key_builder = BytePageBuilder::new();
            let mut present = Vec::new();
            let mut request_indexes = match order {
                KvReadV3Order::RequestOrder => None,
                KvReadV3Order::KeyOrder => Some(Vec::new()),
            };
            for (index, (key, exists)) in keys.into_iter().zip(group.exists).enumerate() {
                match order {
                    KvReadV3Order::RequestOrder => {
                        key_builder.push(key);
                        present.push(exists);
                    }
                    KvReadV3Order::KeyOrder => {
                        if exists {
                            key_builder.push(key);
                            present.push(true);
                            request_indexes
                                .as_mut()
                                .expect("request indexes exist")
                                .push(u32::try_from(index).map_err(|_| {
                                    LixError::unknown("storage read_v3 request index overflow")
                                })?);
                        }
                    }
                }
            }
            Ok(KvReadV3Page {
                keys: key_builder.finish(),
                presence: KvReadV3Presence::bitmap(present),
                values: Vec::new(),
                request_indexes,
                resume_after: None,
            })
        }
        KvReadV3Projection::ValueParts(parts) => {
            let result = store
                .get_values(KvGetRequest {
                    groups: vec![KvGetGroup {
                        namespace,
                        keys: keys.clone(),
                    }],
                })
                .await?;
            let group = result.groups.into_iter().next().ok_or_else(|| {
                LixError::unknown("storage read_v3 fallback get returned no result group")
            })?;
            let mut key_builder = BytePageBuilder::new();
            let mut present = Vec::new();
            let mut value_builders = parts
                .iter()
                .map(|_| BytePageBuilder::new())
                .collect::<Vec<_>>();
            let mut request_indexes = match order {
                KvReadV3Order::RequestOrder => None,
                KvReadV3Order::KeyOrder => Some(Vec::new()),
            };
            for (index, key) in keys.into_iter().enumerate() {
                let value = group.value(index).ok_or_else(|| {
                    LixError::unknown("storage read_v3 fallback result index missing")
                })?;
                match (order, value) {
                    (KvReadV3Order::RequestOrder, Some(value)) => {
                        key_builder.push(key);
                        present.push(true);
                        for (part, builder) in parts.iter().zip(value_builders.iter_mut()) {
                            builder.push(project_read_v3_value_part(value, *part)?);
                        }
                    }
                    (KvReadV3Order::RequestOrder, None) => {
                        key_builder.push(key);
                        present.push(false);
                        for builder in &mut value_builders {
                            builder.push([]);
                        }
                    }
                    (KvReadV3Order::KeyOrder, Some(value)) => {
                        key_builder.push(key);
                        present.push(true);
                        request_indexes
                            .as_mut()
                            .expect("request indexes exist")
                            .push(u32::try_from(index).map_err(|_| {
                                LixError::unknown("storage read_v3 request index overflow")
                            })?);
                        for (part, builder) in parts.iter().zip(value_builders.iter_mut()) {
                            builder.push(project_read_v3_value_part(value, *part)?);
                        }
                    }
                    (KvReadV3Order::KeyOrder, None) => {}
                }
            }
            Ok(KvReadV3Page {
                keys: key_builder.finish(),
                presence: KvReadV3Presence::bitmap(present),
                values: value_builders
                    .into_iter()
                    .map(BytePageBuilder::finish)
                    .collect(),
                request_indexes,
                resume_after: None,
            })
        }
    }
}

async fn storage_read_v3_scan_then_reorder_fallback<T>(
    store: &mut T,
    namespace: String,
    keys: Vec<Vec<u8>>,
    spans: Vec<KvKeySpan>,
    projection: KvReadV3Projection,
    order: KvReadV3Order,
) -> Result<KvReadV3Page, LixError>
where
    T: StorageReader + ?Sized,
{
    if spans.is_empty() {
        return storage_read_v3_keys_fallback(store, namespace, keys, projection, order).await;
    }

    let part_count = match &projection {
        KvReadV3Projection::KeysOnly => 0,
        KvReadV3Projection::ValueParts(parts) => parts.len(),
    };
    let page =
        storage_read_v3_scan_spans_fallback(store, namespace, spans, None, usize::MAX, projection)
            .await?;
    let mut values_by_key = BTreeMap::new();
    for (index, key) in page.keys.iter().enumerate() {
        let mut values = Vec::with_capacity(part_count);
        for values_page in &page.values {
            values.push(
                values_page
                    .get(index)
                    .ok_or_else(|| LixError::unknown("storage read_v3 scan value missing"))?
                    .to_vec(),
            );
        }
        values_by_key.insert(key.to_vec(), values);
    }

    let mut key_builder = BytePageBuilder::new();
    let mut present = Vec::new();
    let mut value_builders = (0..part_count)
        .map(|_| BytePageBuilder::new())
        .collect::<Vec<_>>();
    let mut request_indexes = match order {
        KvReadV3Order::RequestOrder => None,
        KvReadV3Order::KeyOrder => Some(Vec::new()),
    };
    for (index, key) in keys.into_iter().enumerate() {
        let values = values_by_key.get(&key);
        match (order, values) {
            (KvReadV3Order::RequestOrder, Some(values)) => {
                key_builder.push(&key);
                present.push(true);
                for (value, builder) in values.iter().zip(value_builders.iter_mut()) {
                    builder.push(value);
                }
            }
            (KvReadV3Order::RequestOrder, None) => {
                key_builder.push(&key);
                present.push(false);
                for builder in &mut value_builders {
                    builder.push([]);
                }
            }
            (KvReadV3Order::KeyOrder, Some(values)) => {
                key_builder.push(&key);
                present.push(true);
                request_indexes
                    .as_mut()
                    .expect("request indexes exist")
                    .push(u32::try_from(index).map_err(|_| {
                        LixError::unknown("storage read_v3 request index overflow")
                    })?);
                for (value, builder) in values.iter().zip(value_builders.iter_mut()) {
                    builder.push(value);
                }
            }
            (KvReadV3Order::KeyOrder, None) => {}
        }
    }

    Ok(KvReadV3Page {
        keys: key_builder.finish(),
        presence: KvReadV3Presence::bitmap(present),
        values: value_builders
            .into_iter()
            .map(BytePageBuilder::finish)
            .collect(),
        request_indexes,
        resume_after: None,
    })
}

pub(crate) fn project_read_v3_value_part(
    value: &[u8],
    part: KvReadV3ValuePart,
) -> Result<&[u8], LixError> {
    match part {
        KvReadV3ValuePart::Header => {
            project_header_payload_frame_part(value, KvHeaderPayloadFramePart::Header)
        }
        KvReadV3ValuePart::Payload => {
            project_header_payload_frame_part(value, KvHeaderPayloadFramePart::Payload)
        }
        KvReadV3ValuePart::FullValue => Ok(value),
    }
}

fn normalize_spans(mut spans: Vec<KvKeySpan>) -> Vec<KvKeySpan> {
    spans.retain(|span| span.end.is_empty() || span.start < span.end);
    spans.sort_by(|left, right| {
        left.start
            .cmp(&right.start)
            .then_with(|| span_end_for_order(left).cmp(span_end_for_order(right)))
    });
    let mut normalized: Vec<KvKeySpan> = Vec::new();
    for span in spans {
        let Some(last) = normalized.last_mut() else {
            normalized.push(span);
            continue;
        };
        if spans_overlap_or_touch(last, &span) {
            if last.end.is_empty() || span.end.is_empty() {
                last.end.clear();
            } else if span.end > last.end {
                last.end = span.end;
            }
        } else {
            normalized.push(span);
        }
    }
    normalized
}

fn spans_overlap_or_touch(left: &KvKeySpan, right: &KvKeySpan) -> bool {
    left.end.is_empty() || left.end >= right.start
}

fn span_end_for_order(span: &KvKeySpan) -> &[u8] {
    if span.end.is_empty() {
        &[0xFF]
    } else {
        &span.end
    }
}

fn span_scan_range(span: &KvKeySpan) -> KvScanRange {
    if span.start.is_empty() && span.end.is_empty() {
        KvScanRange::Prefix(Vec::new())
    } else {
        KvScanRange::Range {
            start: span.start.clone(),
            end: span.end.clone(),
        }
    }
}

fn scan_after_for_span(span: &KvKeySpan, after: Option<&[u8]>) -> Option<Option<Vec<u8>>> {
    let Some(after) = after else {
        return Some(None);
    };
    if !span.end.is_empty() && span.end.as_slice() <= after {
        return None;
    }
    if span.start.as_slice() <= after {
        return Some(Some(after.to_vec()));
    }
    Some(None)
}

fn last_key(keys: &BytePageBuilder) -> Option<Vec<u8>> {
    keys.len()
        .checked_sub(1)
        .and_then(|index| keys.get(index))
        .map(<[u8]>::to_vec)
}

pub(crate) fn project_value_part<'a>(
    value: &'a [u8],
    part: &KvValuePart,
) -> Result<&'a [u8], LixError> {
    match part {
        KvValuePart::ByteRange { offset, len } => value
            .get(*offset..offset.saturating_add(*len))
            .ok_or_else(|| LixError::unknown("storage value projection range is out of bounds")),
        KvValuePart::ByteSuffix { offset } => value
            .get(*offset..)
            .ok_or_else(|| LixError::unknown("storage value projection suffix is out of bounds")),
        KvValuePart::HeaderPayloadFrame(frame_part) => {
            project_header_payload_frame_part(value, *frame_part)
        }
    }
}

const HEADER_PAYLOAD_FRAME_HEADER_LEN: usize = 25;

fn project_header_payload_frame_part(
    value: &[u8],
    part: KvHeaderPayloadFramePart,
) -> Result<&[u8], LixError> {
    let header = value
        .get(..HEADER_PAYLOAD_FRAME_HEADER_LEN)
        .ok_or_else(|| {
            LixError::unknown("storage framed value projection found a short frame header")
        })?;
    let header_len = read_fixed_width_decimal(&header[5..15])?;
    let payload_len = read_fixed_width_decimal(&header[15..25])?;
    let header_start = HEADER_PAYLOAD_FRAME_HEADER_LEN;
    let header_end = header_start
        .checked_add(header_len)
        .ok_or_else(|| LixError::unknown("storage framed value projection length overflow"))?;
    let payload_end = header_end
        .checked_add(payload_len)
        .ok_or_else(|| LixError::unknown("storage framed value projection length overflow"))?;
    if payload_end != value.len() {
        return Err(LixError::unknown(
            "storage framed value projection length does not match value",
        ));
    }
    match part {
        KvHeaderPayloadFramePart::Header => value
            .get(header_start..header_end)
            .ok_or_else(|| LixError::unknown("storage framed header projection is out of bounds")),
        KvHeaderPayloadFramePart::Payload => value
            .get(header_end..payload_end)
            .ok_or_else(|| LixError::unknown("storage framed payload projection is out of bounds")),
    }
}

fn read_fixed_width_decimal(bytes: &[u8]) -> Result<usize, LixError> {
    if bytes.len() != 10 || bytes.iter().any(|byte| !byte.is_ascii_digit()) {
        return Err(LixError::unknown(
            "storage framed value projection found an invalid length field",
        ));
    }
    let text = std::str::from_utf8(bytes).map_err(|error| {
        LixError::unknown(format!(
            "storage framed value projection found invalid length UTF-8: {error}"
        ))
    })?;
    text.parse::<usize>().map_err(|error| {
        LixError::unknown(format!(
            "storage framed value projection found invalid length: {error}"
        ))
    })
}

#[async_trait]
pub(crate) trait StorageReadTransaction: StorageReader + Send + Sync {
    async fn rollback(self: Box<Self>) -> Result<(), LixError>;
}

#[async_trait]
pub(crate) trait StorageWriteTransaction:
    StorageReadTransaction + StorageWriter + Send + Sync
{
    async fn commit(self: Box<Self>) -> Result<(), LixError>;
}

#[async_trait]
impl<T> StorageReader for &mut T
where
    T: StorageReader + ?Sized,
{
    async fn get_values(&mut self, request: KvGetRequest) -> Result<KvValueBatch, LixError> {
        (**self).get_values(request).await
    }

    async fn exists_many(&mut self, request: KvGetRequest) -> Result<KvExistsBatch, LixError> {
        (**self).exists_many(request).await
    }

    async fn scan_keys(&mut self, request: KvScanRequest) -> Result<KvKeyPage, LixError> {
        (**self).scan_keys(request).await
    }

    async fn scan_values(&mut self, request: KvScanRequest) -> Result<KvValuePage, LixError> {
        (**self).scan_values(request).await
    }

    async fn scan_entries(&mut self, request: KvScanRequest) -> Result<KvEntryPage, LixError> {
        (**self).scan_entries(request).await
    }

    async fn scan2(&mut self, request: KvScan2Request) -> Result<KvScan2Page, LixError> {
        (**self).scan2(request).await
    }

    async fn read_v3(&mut self, request: KvReadV3Request) -> Result<KvReadV3Page, LixError> {
        (**self).read_v3(request).await
    }

    async fn read4(&mut self, request: KvTableReadRequest) -> Result<KvRead4Page, LixError> {
        (**self).read4(request).await
    }
}

#[async_trait]
impl<T> StorageReader for Box<T>
where
    T: StorageReader + ?Sized,
{
    async fn get_values(&mut self, request: KvGetRequest) -> Result<KvValueBatch, LixError> {
        (**self).get_values(request).await
    }

    async fn exists_many(&mut self, request: KvGetRequest) -> Result<KvExistsBatch, LixError> {
        (**self).exists_many(request).await
    }

    async fn scan_keys(&mut self, request: KvScanRequest) -> Result<KvKeyPage, LixError> {
        (**self).scan_keys(request).await
    }

    async fn scan_values(&mut self, request: KvScanRequest) -> Result<KvValuePage, LixError> {
        (**self).scan_values(request).await
    }

    async fn scan_entries(&mut self, request: KvScanRequest) -> Result<KvEntryPage, LixError> {
        (**self).scan_entries(request).await
    }

    async fn scan2(&mut self, request: KvScan2Request) -> Result<KvScan2Page, LixError> {
        (**self).scan2(request).await
    }

    async fn read_v3(&mut self, request: KvReadV3Request) -> Result<KvReadV3Page, LixError> {
        (**self).read_v3(request).await
    }

    async fn read4(&mut self, request: KvTableReadRequest) -> Result<KvRead4Page, LixError> {
        (**self).read4(request).await
    }
}

#[async_trait]
impl<T> StorageWriter for &mut T
where
    T: StorageWriter + ?Sized,
{
    async fn write_kv_batch(&mut self, batch: KvWriteBatch) -> Result<KvWriteStats, LixError> {
        (**self).write_kv_batch(batch).await
    }
}

#[async_trait]
impl<T> StorageWriter for Box<T>
where
    T: StorageWriter + ?Sized,
{
    async fn write_kv_batch(&mut self, batch: KvWriteBatch) -> Result<KvWriteStats, LixError> {
        (**self).write_kv_batch(batch).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvGetRequest {
    pub(crate) groups: Vec<KvGetGroup>,
}

impl From<KvGetRequest> for backend::BackendKvGetRequest {
    fn from(request: KvGetRequest) -> Self {
        Self {
            groups: request.groups.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvGetGroup {
    pub(crate) namespace: String,
    pub(crate) keys: Vec<Vec<u8>>,
}

impl From<KvGetGroup> for backend::BackendKvGetGroup {
    fn from(group: KvGetGroup) -> Self {
        Self {
            namespace: group.namespace,
            keys: group.keys,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvValueBatch {
    pub(crate) groups: Vec<KvValueGroup>,
}

impl From<backend::BackendKvValueBatch> for KvValueBatch {
    fn from(result: backend::BackendKvValueBatch) -> Self {
        Self {
            groups: result.groups.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvValueGroup {
    namespace: String,
    values: BytePage,
    present: Vec<bool>,
}

impl From<backend::BackendKvValueGroup> for KvValueGroup {
    fn from(group: backend::BackendKvValueGroup) -> Self {
        let (namespace, values, present) = group.into_parts();
        Self {
            namespace,
            values,
            present,
        }
    }
}

impl KvValueGroup {
    fn new(namespace: impl Into<String>, values: BytePage, present: Vec<bool>) -> Self {
        assert_eq!(
            values.len(),
            present.len(),
            "storage value batch must have one value slot per presence bit"
        );
        Self {
            namespace: namespace.into(),
            values,
            present,
        }
    }

    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn len(&self) -> usize {
        self.present.len()
    }

    pub(crate) fn value(&self, index: usize) -> Option<Option<&[u8]>> {
        let present = *self.present.get(index)?;
        if present {
            Some(Some(
                self.values
                    .get(index)
                    .expect("storage value batch invariant violated"),
            ))
        } else {
            Some(None)
        }
    }

    pub(crate) fn values_iter(&self) -> impl Iterator<Item = Option<&[u8]>> {
        (0..self.len()).filter_map(|index| self.value(index))
    }

    pub(crate) fn single_value_owned(&self) -> Option<Vec<u8>> {
        if self.len() != 1 {
            return None;
        }
        self.value(0).flatten().map(<[u8]>::to_vec)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvExistsBatch {
    pub(crate) groups: Vec<KvExistsGroup>,
}

impl From<backend::BackendKvExistsBatch> for KvExistsBatch {
    fn from(result: backend::BackendKvExistsBatch) -> Self {
        Self {
            groups: result.groups.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvExistsGroup {
    pub(crate) namespace: String,
    pub(crate) exists: Vec<bool>,
}

impl From<backend::BackendKvExistsGroup> for KvExistsGroup {
    fn from(group: backend::BackendKvExistsGroup) -> Self {
        Self {
            namespace: group.namespace,
            exists: group.exists,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvScanRequest {
    pub(crate) namespace: String,
    pub(crate) range: KvScanRange,
    pub(crate) after: Option<Vec<u8>>,
    pub(crate) limit: usize,
}

impl From<KvScanRequest> for backend::BackendKvScanRequest {
    fn from(request: KvScanRequest) -> Self {
        Self {
            namespace: request.namespace,
            range: request.range.into(),
            after: request.after,
            limit: request.limit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvScan2Request {
    pub(crate) namespace: String,
    pub(crate) range: KvScanRange,
    pub(crate) after: Option<Vec<u8>>,
    pub(crate) page_size: usize,
    pub(crate) projection: KvScan2Projection,
}

impl From<KvScan2Request> for backend::BackendKvScan2Request {
    fn from(request: KvScan2Request) -> Self {
        Self {
            namespace: request.namespace,
            range: request.range.into(),
            after: request.after,
            page_size: request.page_size,
            projection: request.projection.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvKeySpan {
    pub(crate) start: Vec<u8>,
    pub(crate) end: Vec<u8>,
}

impl KvKeySpan {
    pub(crate) fn new(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self {
            start: start.into(),
            end: end.into(),
        }
    }

    pub(crate) fn all() -> Self {
        Self {
            start: Vec::new(),
            end: Vec::new(),
        }
    }
}

impl From<KvKeySpan> for backend::BackendKvKeySpan {
    fn from(span: KvKeySpan) -> Self {
        Self {
            start: span.start,
            end: span.end,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KvScan2Projection {
    KeysOnly,
    FullValue,
    ValuePart(KvValuePart),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KvValuePart {
    ByteRange { offset: usize, len: usize },
    ByteSuffix { offset: usize },
    HeaderPayloadFrame(KvHeaderPayloadFramePart),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KvHeaderPayloadFramePart {
    Header,
    Payload,
}

impl From<KvScan2Projection> for backend::BackendKvScan2Projection {
    fn from(projection: KvScan2Projection) -> Self {
        match projection {
            KvScan2Projection::KeysOnly => Self::KeysOnly,
            KvScan2Projection::FullValue => Self::FullValue,
            KvScan2Projection::ValuePart(part) => Self::ValuePart(part.into()),
        }
    }
}

impl From<KvValuePart> for backend::BackendKvValuePart {
    fn from(part: KvValuePart) -> Self {
        match part {
            KvValuePart::ByteRange { offset, len } => Self::ByteRange { offset, len },
            KvValuePart::ByteSuffix { offset } => Self::ByteSuffix { offset },
            KvValuePart::HeaderPayloadFrame(part) => Self::HeaderPayloadFrame(part.into()),
        }
    }
}

impl From<KvHeaderPayloadFramePart> for backend::BackendKvHeaderPayloadFramePart {
    fn from(part: KvHeaderPayloadFramePart) -> Self {
        match part {
            KvHeaderPayloadFramePart::Header => Self::Header,
            KvHeaderPayloadFramePart::Payload => Self::Payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvKeyPage {
    pub(crate) keys: BytePage,
    pub(crate) resume_after: Option<Vec<u8>>,
}

impl From<backend::BackendKvKeyPage> for KvKeyPage {
    fn from(result: backend::BackendKvKeyPage) -> Self {
        Self {
            keys: result.keys,
            resume_after: result.resume_after,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvValuePage {
    pub(crate) values: BytePage,
    pub(crate) resume_after: Option<Vec<u8>>,
}

impl From<backend::BackendKvValuePage> for KvValuePage {
    fn from(result: backend::BackendKvValuePage) -> Self {
        Self {
            values: result.values,
            resume_after: result.resume_after,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvEntryPage {
    pub(crate) keys: BytePage,
    pub(crate) values: BytePage,
    pub(crate) resume_after: Option<Vec<u8>>,
}

impl From<backend::BackendKvEntryPage> for KvEntryPage {
    fn from(result: backend::BackendKvEntryPage) -> Self {
        Self {
            keys: result.keys,
            values: result.values,
            resume_after: result.resume_after,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvScan2Page {
    pub(crate) keys: BytePage,
    pub(crate) values: Option<BytePage>,
    pub(crate) resume_after: Option<Vec<u8>>,
}

impl From<backend::BackendKvScan2Page> for KvScan2Page {
    fn from(result: backend::BackendKvScan2Page) -> Self {
        Self {
            keys: result.keys,
            values: result.values,
            resume_after: result.resume_after,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvReadV3Request {
    pub(crate) namespace: String,
    pub(crate) source: KvReadV3Source,
    pub(crate) projection: KvReadV3Projection,
    pub(crate) order: KvReadV3Order,
    pub(crate) page_size: Option<usize>,
    pub(crate) strategy: KvReadV3Strategy,
}

impl From<KvReadV3Request> for backend::BackendKvReadV3Request {
    fn from(request: KvReadV3Request) -> Self {
        Self {
            namespace: request.namespace,
            source: request.source.into(),
            projection: request.projection.into(),
            order: request.order.into(),
            page_size: request.page_size,
            strategy: request.strategy.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KvReadV3Source {
    Keys {
        keys: Vec<Vec<u8>>,
    },
    Spans {
        spans: Vec<KvKeySpan>,
        after: Option<Vec<u8>>,
    },
    KeysOrSpans {
        keys: Vec<Vec<u8>>,
        spans: Vec<KvKeySpan>,
    },
}

impl From<KvReadV3Source> for backend::BackendKvReadV3Source {
    fn from(source: KvReadV3Source) -> Self {
        match source {
            KvReadV3Source::Keys { keys } => Self::Keys { keys },
            KvReadV3Source::Spans { spans, after } => Self::Spans {
                spans: spans.into_iter().map(Into::into).collect(),
                after,
            },
            KvReadV3Source::KeysOrSpans { keys, spans } => Self::KeysOrSpans {
                keys,
                spans: spans.into_iter().map(Into::into).collect(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KvReadV3Projection {
    KeysOnly,
    ValueParts(Vec<KvReadV3ValuePart>),
}

impl From<KvReadV3Projection> for backend::BackendKvReadV3Projection {
    fn from(projection: KvReadV3Projection) -> Self {
        match projection {
            KvReadV3Projection::KeysOnly => Self::KeysOnly,
            KvReadV3Projection::ValueParts(parts) => {
                Self::ValueParts(parts.into_iter().map(Into::into).collect())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KvReadV3ValuePart {
    Header,
    Payload,
    FullValue,
}

impl From<KvReadV3ValuePart> for backend::BackendKvReadV3ValuePart {
    fn from(part: KvReadV3ValuePart) -> Self {
        match part {
            KvReadV3ValuePart::Header => Self::Header,
            KvReadV3ValuePart::Payload => Self::Payload,
            KvReadV3ValuePart::FullValue => Self::FullValue,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KvReadV3Order {
    RequestOrder,
    KeyOrder,
}

impl From<KvReadV3Order> for backend::BackendKvReadV3Order {
    fn from(order: KvReadV3Order) -> Self {
        match order {
            KvReadV3Order::RequestOrder => Self::RequestOrder,
            KvReadV3Order::KeyOrder => Self::KeyOrder,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KvReadV3Strategy {
    Auto,
    Points,
    Scan,
}

impl From<KvReadV3Strategy> for backend::BackendKvReadV3Strategy {
    fn from(strategy: KvReadV3Strategy) -> Self {
        match strategy {
            KvReadV3Strategy::Auto => Self::Auto,
            KvReadV3Strategy::Points => Self::Points,
            KvReadV3Strategy::Scan => Self::Scan,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KvReadV3Presence {
    All,
    Bitmap(Vec<bool>),
}

impl KvReadV3Presence {
    pub(crate) fn bitmap(bits: Vec<bool>) -> Self {
        if bits.iter().all(|present| *present) {
            Self::All
        } else {
            Self::Bitmap(bits)
        }
    }

    pub(crate) fn len(&self, row_count: usize) -> usize {
        match self {
            Self::All => row_count,
            Self::Bitmap(bits) => bits.len(),
        }
    }

    pub(crate) fn is_present(&self, row_count: usize, index: usize) -> Option<bool> {
        match self {
            Self::All => (index < row_count).then_some(true),
            Self::Bitmap(bits) => bits.get(index).copied(),
        }
    }

    pub(crate) fn present_count(&self, row_count: usize) -> usize {
        match self {
            Self::All => row_count,
            Self::Bitmap(bits) => bits.iter().filter(|present| **present).count(),
        }
    }

    pub(crate) fn to_vec(&self, row_count: usize) -> Vec<bool> {
        match self {
            Self::All => vec![true; row_count],
            Self::Bitmap(bits) => bits.clone(),
        }
    }
}

impl From<backend::BackendKvReadV3Presence> for KvReadV3Presence {
    fn from(presence: backend::BackendKvReadV3Presence) -> Self {
        match presence {
            backend::BackendKvReadV3Presence::All => Self::All,
            backend::BackendKvReadV3Presence::Bitmap(bits) => Self::Bitmap(bits),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvReadV3Page {
    pub(crate) keys: BytePage,
    pub(crate) presence: KvReadV3Presence,
    pub(crate) values: Vec<BytePage>,
    pub(crate) request_indexes: Option<Vec<u32>>,
    pub(crate) resume_after: Option<Vec<u8>>,
}

impl KvReadV3Page {
    pub(crate) fn presence_len(&self) -> usize {
        self.presence.len(self.keys.len())
    }

    pub(crate) fn is_present(&self, index: usize) -> Option<bool> {
        self.presence.is_present(self.keys.len(), index)
    }

    pub(crate) fn present_count(&self) -> usize {
        self.presence.present_count(self.keys.len())
    }

    pub(crate) fn presence_vec(&self) -> Vec<bool> {
        self.presence.to_vec(self.keys.len())
    }
}

impl From<backend::BackendKvReadV3Page> for KvReadV3Page {
    fn from(result: backend::BackendKvReadV3Page) -> Self {
        Self {
            keys: result.keys,
            presence: result.presence.into(),
            values: result.values,
            request_indexes: result.request_indexes,
            resume_after: result.resume_after,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvTableReadRequest {
    pub(crate) table: KvTableId,
    pub(crate) key_space: KvKeySpace,
    pub(crate) access: Vec<KvAccessSegment>,
    pub(crate) after: Option<Vec<u8>>,
    pub(crate) projection: KvRead4Projection,
    pub(crate) residual_filter: Option<KvResidualFilter>,
    pub(crate) output_order: KvRead4Order,
    pub(crate) limit: Option<usize>,
    pub(crate) session: Option<KvReadSessionId>,
}

impl From<KvTableReadRequest> for backend::BackendKvTableReadRequest {
    fn from(request: KvTableReadRequest) -> Self {
        Self {
            table: request.table.into(),
            key_space: request.key_space.into(),
            access: request.access.into_iter().map(Into::into).collect(),
            after: request.after,
            projection: request.projection.into(),
            residual_filter: request.residual_filter.map(Into::into),
            output_order: request.output_order.into(),
            limit: request.limit,
            session: request.session.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvTableId {
    pub(crate) namespace: String,
}

impl From<KvTableId> for backend::BackendKvTableId {
    fn from(table: KvTableId) -> Self {
        Self {
            namespace: table.namespace,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KvKeySpace {
    OrderedBytes,
}

impl From<KvKeySpace> for backend::BackendKvKeySpace {
    fn from(key_space: KvKeySpace) -> Self {
        match key_space {
            KvKeySpace::OrderedBytes => Self::OrderedBytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KvAccessSegment {
    Points {
        keys: Vec<Vec<u8>>,
        request_indexes: Vec<u32>,
    },
    Run {
        lower: Vec<u8>,
        upper: Vec<u8>,
        keys: Vec<Vec<u8>>,
        request_indexes: Vec<u32>,
    },
    Span {
        lower: Vec<u8>,
        upper: Vec<u8>,
    },
}

impl From<KvAccessSegment> for backend::BackendKvAccessSegment {
    fn from(segment: KvAccessSegment) -> Self {
        match segment {
            KvAccessSegment::Points {
                keys,
                request_indexes,
            } => Self::Points {
                keys,
                request_indexes,
            },
            KvAccessSegment::Run {
                lower,
                upper,
                keys,
                request_indexes,
            } => Self::Run {
                lower,
                upper,
                keys,
                request_indexes,
            },
            KvAccessSegment::Span { lower, upper } => Self::Span { lower, upper },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KvRead4Projection {
    KeysOnly,
    Parts(Vec<KvRead4ValuePart>),
}

impl From<KvRead4Projection> for backend::BackendKvRead4Projection {
    fn from(projection: KvRead4Projection) -> Self {
        match projection {
            KvRead4Projection::KeysOnly => Self::KeysOnly,
            KvRead4Projection::Parts(parts) => {
                Self::Parts(parts.into_iter().map(Into::into).collect())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KvRead4ValuePart {
    Header,
    PayloadRef,
    Payload,
    FullValue,
}

impl From<KvRead4ValuePart> for backend::BackendKvRead4ValuePart {
    fn from(part: KvRead4ValuePart) -> Self {
        match part {
            KvRead4ValuePart::Header => Self::Header,
            KvRead4ValuePart::PayloadRef => Self::PayloadRef,
            KvRead4ValuePart::Payload => Self::Payload,
            KvRead4ValuePart::FullValue => Self::FullValue,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KvResidualFilter {
    UntrackedState,
}

impl From<KvResidualFilter> for backend::BackendKvResidualFilter {
    fn from(filter: KvResidualFilter) -> Self {
        match filter {
            KvResidualFilter::UntrackedState => Self::UntrackedState,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KvRead4Order {
    RequestOrder,
    KeyOrder,
}

impl From<KvRead4Order> for backend::BackendKvRead4Order {
    fn from(order: KvRead4Order) -> Self {
        match order {
            KvRead4Order::RequestOrder => Self::RequestOrder,
            KvRead4Order::KeyOrder => Self::KeyOrder,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct KvReadSessionId(pub(crate) u64);

impl From<KvReadSessionId> for backend::BackendKvReadSessionId {
    fn from(session: KvReadSessionId) -> Self {
        Self(session.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvRead4Page {
    pub(crate) keys: BytePage,
    pub(crate) presence: KvReadV3Presence,
    pub(crate) values: Vec<BytePage>,
    pub(crate) request_indexes: Option<Vec<u32>>,
    pub(crate) resume_after: Option<Vec<u8>>,
}

impl KvRead4Page {
    pub(crate) fn presence_len(&self) -> usize {
        self.presence.len(self.keys.len())
    }

    pub(crate) fn is_present(&self, index: usize) -> Option<bool> {
        self.presence.is_present(self.keys.len(), index)
    }

    pub(crate) fn present_count(&self) -> usize {
        self.presence.present_count(self.keys.len())
    }

    pub(crate) fn presence_vec(&self) -> Vec<bool> {
        self.presence.to_vec(self.keys.len())
    }
}

impl From<backend::BackendKvRead4Page> for KvRead4Page {
    fn from(result: backend::BackendKvRead4Page) -> Self {
        Self {
            keys: result.keys,
            presence: result.presence.into(),
            values: result.values,
            request_indexes: result.request_indexes,
            resume_after: result.resume_after,
        }
    }
}

impl KvEntryPage {
    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub(crate) fn key(&self, index: usize) -> Option<&[u8]> {
        self.keys.get(index)
    }

    pub(crate) fn value(&self, index: usize) -> Option<&[u8]> {
        self.values.get(index)
    }
}

#[derive(Debug, Default)]
pub(crate) struct StorageWriteSet {
    batch: KvWriteBatch,
}

impl StorageWriteSet {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn put(&mut self, namespace: &'static str, key: Vec<u8>, value: Vec<u8>) {
        self.batch.put(namespace, key, value);
    }

    pub(crate) fn delete(&mut self, namespace: &'static str, key: Vec<u8>) {
        self.batch.delete(namespace, key);
    }

    pub(crate) fn delete_range(&mut self, namespace: &'static str, range: KvScanRange) {
        self.batch.delete_range(namespace, range);
    }

    pub(crate) fn push_group(&mut self, group: KvWriteGroup) {
        self.batch.push_group(group);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    pub(crate) async fn apply(
        self,
        writer: &mut (impl StorageWriter + ?Sized),
    ) -> Result<KvWriteStats, LixError> {
        writer.write_kv_batch(self.batch).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct KvWriteBatch {
    pub(crate) groups: Vec<KvWriteGroup>,
}

impl KvWriteBatch {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn put(
        &mut self,
        namespace: &'static str,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) {
        let group = self.group_mut(namespace);
        group.put(key.into(), value.into());
    }

    pub(crate) fn delete(&mut self, namespace: &'static str, key: impl Into<Vec<u8>>) {
        let group = self.group_mut(namespace);
        group.delete(key.into());
    }

    pub(crate) fn delete_range(&mut self, namespace: &'static str, range: KvScanRange) {
        let group = self.group_mut(namespace);
        group.delete_range(range);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.groups.iter().all(KvWriteGroup::is_empty)
    }

    pub(crate) fn push_group(&mut self, group: KvWriteGroup) {
        if group.is_empty() {
            return;
        }
        if let Some(index) = self
            .groups
            .iter()
            .position(|existing| existing.namespace == group.namespace)
        {
            self.groups[index].ops.extend(group.ops);
        } else {
            self.groups.push(group);
        }
    }

    fn group_mut(&mut self, namespace: &'static str) -> &mut KvWriteGroup {
        if let Some(index) = self
            .groups
            .iter()
            .position(|group| group.namespace == namespace)
        {
            return &mut self.groups[index];
        }
        self.groups.push(KvWriteGroup {
            namespace,
            ops: Vec::new(),
        });
        self.groups.last_mut().expect("group just pushed")
    }
}

impl From<KvWriteBatch> for backend::BackendKvWriteBatch {
    fn from(batch: KvWriteBatch) -> Self {
        Self {
            groups: batch.groups.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvWriteGroup {
    namespace: &'static str,
    ops: Vec<KvWriteOp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KvWriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    DeleteRange { range: KvScanRange },
}

impl From<KvWriteGroup> for backend::BackendKvWriteGroup {
    fn from(group: KvWriteGroup) -> Self {
        let mut backend_group = Self::new(group.namespace.to_string());
        for op in group.ops {
            backend_group.push(op.into());
        }
        backend_group
    }
}

impl From<KvWriteOp> for backend::BackendKvWriteOp {
    fn from(op: KvWriteOp) -> Self {
        match op {
            KvWriteOp::Put { key, value } => Self::Put { key, value },
            KvWriteOp::Delete { key } => Self::Delete { key },
            KvWriteOp::DeleteRange { range } => Self::DeleteRange {
                range: range.into(),
            },
        }
    }
}

impl KvWriteGroup {
    pub(crate) fn new(namespace: &'static str) -> Self {
        Self {
            namespace,
            ops: Vec::new(),
        }
    }

    pub(crate) fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.ops.push(KvWriteOp::Put {
            key: key.into(),
            value: value.into(),
        });
    }

    pub(crate) fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.ops.push(KvWriteOp::Delete { key: key.into() });
    }

    pub(crate) fn delete_range(&mut self, range: KvScanRange) {
        self.ops.push(KvWriteOp::DeleteRange { range });
    }

    pub(crate) fn reserve(&mut self, additional: usize) {
        self.ops.reserve(additional);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub(crate) fn sort_point_ops_by_key(&mut self) {
        if self
            .ops
            .iter()
            .any(|op| matches!(op, KvWriteOp::DeleteRange { .. }))
        {
            panic!("range deletes are not point write operations");
        }
        // `sort_by` is stable, so repeated operations for the same key keep
        // their original order and preserve last-writer-wins behavior.
        self.ops
            .sort_by(|left, right| write_point_op_key(left).cmp(write_point_op_key(right)));
    }

    pub(crate) fn ops(&self) -> &[KvWriteOp] {
        &self.ops
    }
}

fn write_point_op_key(op: &KvWriteOp) -> &[u8] {
    match op {
        KvWriteOp::Put { key, .. } | KvWriteOp::Delete { key } => key,
        KvWriteOp::DeleteRange { .. } => {
            unreachable!("range deletes are not point write operations")
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct KvWriteStats {
    pub(crate) puts: usize,
    pub(crate) deletes: usize,
    pub(crate) delete_ranges: usize,
    pub(crate) bytes_written: usize,
}

impl From<backend::BackendKvWriteStats> for KvWriteStats {
    fn from(stats: backend::BackendKvWriteStats) -> Self {
        Self {
            puts: stats.puts,
            deletes: stats.deletes,
            delete_ranges: stats.delete_ranges,
            bytes_written: stats.bytes_written,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_group_merges_with_existing_namespace_preserving_same_namespace_order() {
        let mut writes = StorageWriteSet::new();

        let mut first = KvWriteGroup::new("ns");
        first.delete(b"old".to_vec());
        writes.push_group(first);

        let mut second = KvWriteGroup::new("ns");
        second.put(b"new".to_vec(), b"value".to_vec());
        writes.push_group(second);

        writes.delete_range("ns", KvScanRange::prefix(Vec::new()));

        assert_eq!(writes.batch.groups.len(), 1);
        assert_eq!(writes.batch.groups[0].namespace, "ns");
        assert!(matches!(
            writes.batch.groups[0].ops[0],
            KvWriteOp::Delete { .. }
        ));
        assert!(matches!(
            writes.batch.groups[0].ops[1],
            KvWriteOp::Put { .. }
        ));
        assert!(matches!(
            writes.batch.groups[0].ops[2],
            KvWriteOp::DeleteRange { .. }
        ));
    }

    #[test]
    fn sort_point_ops_by_key_preserves_same_key_order() {
        let mut group = KvWriteGroup::new("ns");
        group.put(b"b".to_vec(), b"first-b".to_vec());
        group.put(b"a".to_vec(), b"first-a".to_vec());
        group.delete(b"a".to_vec());
        group.put(b"b".to_vec(), b"second-b".to_vec());

        group.sort_point_ops_by_key();

        assert!(matches!(group.ops[0], KvWriteOp::Put { ref key, .. } if key == b"a"));
        assert!(matches!(group.ops[1], KvWriteOp::Delete { ref key } if key == b"a"));
        assert!(
            matches!(group.ops[2], KvWriteOp::Put { ref key, ref value } if key == b"b" && value == b"first-b")
        );
        assert!(
            matches!(group.ops[3], KvWriteOp::Put { ref key, ref value } if key == b"b" && value == b"second-b")
        );
    }
}
