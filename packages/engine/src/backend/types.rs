use async_trait::async_trait;
use std::collections::BTreeMap;

use crate::backend::{
    BackendKvEntryPage, BackendKvExistsBatch, BackendKvGetRequest, BackendKvHeaderPayloadFramePart,
    BackendKvKeyPage, BackendKvKeySpan, BackendKvRead3Order, BackendKvRead3Page,
    BackendKvRead3Presence, BackendKvRead3Projection, BackendKvRead3Request, BackendKvRead3Source,
    BackendKvRead3Strategy, BackendKvRead3ValuePart, BackendKvScan2Page, BackendKvScan2Projection,
    BackendKvScan2Request, BackendKvScanPlanPage, BackendKvScanPlanRequest,
    BackendKvScanPlanValuePart, BackendKvScanProjection, BackendKvScanRange, BackendKvScanRequest,
    BackendKvValueBatch, BackendKvValuePage, BackendKvValuePart, BackendKvWriteBatch,
    BackendKvWriteStats, BytePageBuilder,
};
use crate::LixError;

#[async_trait]
pub trait Backend: Send + Sync {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError>;

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError>;

    /// Releases physical resources held by this backend handle.
    ///
    /// This is a resource lifecycle operation, not a durability boundary and
    /// not a destructive operation. Successful write transactions are durable
    /// when their commit returns; callers should not rely on `close` to save
    /// data. Implementations that do not own external resources may keep the
    /// default no-op behavior.
    async fn close(&self) -> Result<(), LixError> {
        Ok(())
    }

    /// Destroys the physical storage target represented by this backend.
    ///
    /// This is a persistence lifecycle operation, not a logical SQL operation.
    ///
    /// Callers should treat the backend as the authority for what constitutes
    /// the full storage target. For example:
    ///
    /// - native SQLite may delete the main database file plus WAL/SHM sidecars
    /// - wasm/opfs SQLite may clear the persisted OPFS target
    /// - Postgres may drop or clear the configured schema/database target
    ///
    /// Callers must not attempt to infer or delete backend-owned physical
    /// artifacts themselves.
    ///
    /// Implementations may choose not to support destroy if the backend
    /// instance does not have enough information or authority to remove its
    /// target.
    async fn destroy(&self) -> Result<(), LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: "destroy is not supported by this backend".to_string(),
            hint: None,
            details: None,
        })
    }
}

#[async_trait]
pub trait BackendReadTransaction: Send + Sync {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError>;

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError>;

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError>;

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError>;

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError>;

    async fn scan2(
        &mut self,
        request: BackendKvScan2Request,
    ) -> Result<BackendKvScan2Page, LixError> {
        backend_scan2_fallback(self, request).await
    }

    async fn scan_plan(
        &mut self,
        request: BackendKvScanPlanRequest,
    ) -> Result<BackendKvScanPlanPage, LixError> {
        backend_scan_plan_fallback(self, request).await
    }

    async fn read3(
        &mut self,
        request: BackendKvRead3Request,
    ) -> Result<BackendKvRead3Page, LixError> {
        backend_read3_fallback(self, request).await
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError>;
}

#[async_trait]
pub trait BackendWriteTransaction: BackendReadTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError>;

    async fn commit(self: Box<Self>) -> Result<(), LixError>;
}

async fn backend_scan2_fallback<T>(
    transaction: &mut T,
    request: BackendKvScan2Request,
) -> Result<BackendKvScan2Page, LixError>
where
    T: BackendReadTransaction + ?Sized,
{
    match request.projection {
        BackendKvScan2Projection::KeysOnly => {
            let page = transaction
                .scan_keys(BackendKvScanRequest {
                    namespace: request.namespace,
                    range: request.range,
                    after: request.after,
                    limit: request.page_size,
                })
                .await?;
            Ok(BackendKvScan2Page {
                keys: page.keys,
                values: None,
                resume_after: page.resume_after,
            })
        }
        BackendKvScan2Projection::FullValue => {
            let page = transaction
                .scan_entries(BackendKvScanRequest {
                    namespace: request.namespace,
                    range: request.range,
                    after: request.after,
                    limit: request.page_size,
                })
                .await?;
            Ok(BackendKvScan2Page {
                keys: page.keys,
                values: Some(page.values),
                resume_after: page.resume_after,
            })
        }
        BackendKvScan2Projection::ValuePart(part) => {
            let page = transaction
                .scan_entries(BackendKvScanRequest {
                    namespace: request.namespace,
                    range: request.range,
                    after: request.after,
                    limit: request.page_size,
                })
                .await?;
            let mut values = BytePageBuilder::with_capacity(page.values.len(), 0);
            for value in page.values.iter() {
                values.push(project_backend_value_part(value, &part)?);
            }
            Ok(BackendKvScan2Page {
                keys: page.keys,
                values: Some(values.finish()),
                resume_after: page.resume_after,
            })
        }
    }
}

async fn backend_scan_plan_fallback<T>(
    transaction: &mut T,
    request: BackendKvScanPlanRequest,
) -> Result<BackendKvScanPlanPage, LixError>
where
    T: BackendReadTransaction + ?Sized,
{
    match request.projection {
        BackendKvScanProjection::KeysOnly => {
            let mut keys = BytePageBuilder::new();
            let mut resume_after = None;
            let spans = normalize_backend_spans(request.spans);
            let span_count = spans.len();
            for (span_index, span) in spans.into_iter().enumerate() {
                let Some(after) = scan_after_for_backend_span(&span, request.after.as_deref())
                else {
                    continue;
                };
                let remaining = request.page_size.saturating_sub(keys.len());
                if remaining == 0 {
                    break;
                }
                let page = transaction
                    .scan_keys(BackendKvScanRequest {
                        namespace: request.namespace.clone(),
                        range: backend_span_scan_range(&span),
                        after,
                        limit: remaining,
                    })
                    .await?;
                for key in page.keys.iter() {
                    keys.push(key);
                }
                resume_after = page.resume_after;
                if keys.len() == request.page_size {
                    if resume_after.is_some() || span_index + 1 < span_count {
                        resume_after = last_backend_key(&keys);
                    }
                    break;
                }
                if resume_after.is_some() {
                    break;
                }
            }
            Ok(BackendKvScanPlanPage {
                keys: keys.finish(),
                values: Vec::new(),
                resume_after,
            })
        }
        BackendKvScanProjection::ValueParts(parts) => {
            let mut keys = BytePageBuilder::new();
            let mut value_builders = parts
                .iter()
                .map(|_| BytePageBuilder::new())
                .collect::<Vec<_>>();
            let mut resume_after = None;
            let spans = normalize_backend_spans(request.spans);
            let span_count = spans.len();
            for (span_index, span) in spans.into_iter().enumerate() {
                let Some(after) = scan_after_for_backend_span(&span, request.after.as_deref())
                else {
                    continue;
                };
                let remaining = request.page_size.saturating_sub(keys.len());
                if remaining == 0 {
                    break;
                }
                let page = transaction
                    .scan_entries(BackendKvScanRequest {
                        namespace: request.namespace.clone(),
                        range: backend_span_scan_range(&span),
                        after,
                        limit: remaining,
                    })
                    .await?;
                for (index, key) in page.keys.iter().enumerate() {
                    let value = page.value(index).ok_or_else(|| {
                        LixError::unknown("backend scan plan fallback value missing")
                    })?;
                    keys.push(key);
                    for (part, builder) in parts.iter().zip(value_builders.iter_mut()) {
                        builder.push(project_backend_scan_plan_value_part(value, *part)?);
                    }
                }
                resume_after = page.resume_after;
                if keys.len() == request.page_size {
                    if resume_after.is_some() || span_index + 1 < span_count {
                        resume_after = last_backend_key(&keys);
                    }
                    break;
                }
                if resume_after.is_some() {
                    break;
                }
            }
            Ok(BackendKvScanPlanPage {
                keys: keys.finish(),
                values: value_builders
                    .into_iter()
                    .map(BytePageBuilder::finish)
                    .collect(),
                resume_after,
            })
        }
    }
}

async fn backend_read3_fallback<T>(
    transaction: &mut T,
    request: BackendKvRead3Request,
) -> Result<BackendKvRead3Page, LixError>
where
    T: BackendReadTransaction + ?Sized,
{
    match request.source {
        BackendKvRead3Source::Keys { keys } => {
            backend_read3_keys_fallback(
                transaction,
                request.namespace,
                keys,
                request.projection,
                request.order,
            )
            .await
        }
        BackendKvRead3Source::KeysOrSpans { keys, spans } => match request.strategy {
            BackendKvRead3Strategy::Scan => {
                backend_read3_scan_then_reorder_fallback(
                    transaction,
                    request.namespace,
                    keys,
                    spans,
                    request.projection,
                    request.order,
                )
                .await
            }
            BackendKvRead3Strategy::Auto | BackendKvRead3Strategy::Points => {
                backend_read3_keys_fallback(
                    transaction,
                    request.namespace,
                    keys,
                    request.projection,
                    request.order,
                )
                .await
            }
        },
        BackendKvRead3Source::Spans { spans, after } => {
            let page_size = request.page_size.unwrap_or(usize::MAX);
            let projection = match request.projection {
                BackendKvRead3Projection::KeysOnly => BackendKvScanProjection::KeysOnly,
                BackendKvRead3Projection::ValueParts(parts) => BackendKvScanProjection::ValueParts(
                    parts
                        .into_iter()
                        .map(BackendKvScanPlanValuePart::from)
                        .collect(),
                ),
            };
            let page = transaction
                .scan_plan(BackendKvScanPlanRequest {
                    namespace: request.namespace,
                    spans,
                    after,
                    page_size,
                    projection,
                })
                .await?;
            Ok(BackendKvRead3Page {
                keys: page.keys,
                presence: BackendKvRead3Presence::All,
                values: page.values,
                request_indexes: None,
                resume_after: page.resume_after,
            })
        }
    }
}

async fn backend_read3_keys_fallback<T>(
    transaction: &mut T,
    namespace: String,
    keys: Vec<Vec<u8>>,
    projection: BackendKvRead3Projection,
    order: BackendKvRead3Order,
) -> Result<BackendKvRead3Page, LixError>
where
    T: BackendReadTransaction + ?Sized,
{
    match projection {
        BackendKvRead3Projection::KeysOnly => {
            let result = transaction
                .exists_many(BackendKvGetRequest {
                    groups: vec![crate::backend::BackendKvGetGroup {
                        namespace,
                        keys: keys.clone(),
                    }],
                })
                .await?;
            let group = result.groups.into_iter().next().ok_or_else(|| {
                LixError::unknown("backend read3 fallback exists returned no result group")
            })?;
            let mut key_builder = BytePageBuilder::new();
            let mut present = Vec::new();
            let mut request_indexes = match order {
                BackendKvRead3Order::RequestOrder => None,
                BackendKvRead3Order::KeyOrder => Some(Vec::new()),
            };
            for (index, (key, exists)) in keys.into_iter().zip(group.exists).enumerate() {
                match order {
                    BackendKvRead3Order::RequestOrder => {
                        key_builder.push(key);
                        present.push(exists);
                    }
                    BackendKvRead3Order::KeyOrder => {
                        if exists {
                            key_builder.push(key);
                            present.push(true);
                            request_indexes
                                .as_mut()
                                .expect("request indexes exist")
                                .push(u32::try_from(index).map_err(|_| {
                                    LixError::unknown("backend read3 request index overflow")
                                })?);
                        }
                    }
                }
            }
            Ok(BackendKvRead3Page {
                keys: key_builder.finish(),
                presence: BackendKvRead3Presence::bitmap(present),
                values: Vec::new(),
                request_indexes,
                resume_after: None,
            })
        }
        BackendKvRead3Projection::ValueParts(parts) => {
            let result = transaction
                .get_values(BackendKvGetRequest {
                    groups: vec![crate::backend::BackendKvGetGroup {
                        namespace,
                        keys: keys.clone(),
                    }],
                })
                .await?;
            let group = result.groups.into_iter().next().ok_or_else(|| {
                LixError::unknown("backend read3 fallback get returned no result group")
            })?;
            let mut key_builder = BytePageBuilder::new();
            let mut present = Vec::new();
            let mut value_builders = parts
                .iter()
                .map(|_| BytePageBuilder::new())
                .collect::<Vec<_>>();
            let mut request_indexes = match order {
                BackendKvRead3Order::RequestOrder => None,
                BackendKvRead3Order::KeyOrder => Some(Vec::new()),
            };
            for (index, key) in keys.into_iter().enumerate() {
                let value = group.value(index).ok_or_else(|| {
                    LixError::unknown("backend read3 fallback result index missing")
                })?;
                match (order, value) {
                    (BackendKvRead3Order::RequestOrder, Some(value)) => {
                        key_builder.push(key);
                        present.push(true);
                        for (part, builder) in parts.iter().zip(value_builders.iter_mut()) {
                            builder.push(project_backend_read3_value_part(value, *part)?);
                        }
                    }
                    (BackendKvRead3Order::RequestOrder, None) => {
                        key_builder.push(key);
                        present.push(false);
                        for builder in &mut value_builders {
                            builder.push([]);
                        }
                    }
                    (BackendKvRead3Order::KeyOrder, Some(value)) => {
                        key_builder.push(key);
                        present.push(true);
                        request_indexes
                            .as_mut()
                            .expect("request indexes exist")
                            .push(u32::try_from(index).map_err(|_| {
                                LixError::unknown("backend read3 request index overflow")
                            })?);
                        for (part, builder) in parts.iter().zip(value_builders.iter_mut()) {
                            builder.push(project_backend_read3_value_part(value, *part)?);
                        }
                    }
                    (BackendKvRead3Order::KeyOrder, None) => {}
                }
            }
            Ok(BackendKvRead3Page {
                keys: key_builder.finish(),
                presence: BackendKvRead3Presence::bitmap(present),
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

async fn backend_read3_scan_then_reorder_fallback<T>(
    transaction: &mut T,
    namespace: String,
    keys: Vec<Vec<u8>>,
    spans: Vec<BackendKvKeySpan>,
    projection: BackendKvRead3Projection,
    order: BackendKvRead3Order,
) -> Result<BackendKvRead3Page, LixError>
where
    T: BackendReadTransaction + ?Sized,
{
    if spans.is_empty() {
        return backend_read3_keys_fallback(transaction, namespace, keys, projection, order).await;
    }

    let part_count = match &projection {
        BackendKvRead3Projection::KeysOnly => 0,
        BackendKvRead3Projection::ValueParts(parts) => parts.len(),
    };
    let scan_projection = match projection {
        BackendKvRead3Projection::KeysOnly => BackendKvScanProjection::KeysOnly,
        BackendKvRead3Projection::ValueParts(parts) => BackendKvScanProjection::ValueParts(
            parts
                .into_iter()
                .map(BackendKvScanPlanValuePart::from)
                .collect(),
        ),
    };
    let page = transaction
        .scan_plan(BackendKvScanPlanRequest {
            namespace,
            spans,
            after: None,
            page_size: usize::MAX,
            projection: scan_projection,
        })
        .await?;
    let mut values_by_key = BTreeMap::new();
    for (index, key) in page.keys.iter().enumerate() {
        let mut values = Vec::with_capacity(part_count);
        for values_page in &page.values {
            values.push(
                values_page
                    .get(index)
                    .ok_or_else(|| LixError::unknown("backend read3 scan value missing"))?
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
        BackendKvRead3Order::RequestOrder => None,
        BackendKvRead3Order::KeyOrder => Some(Vec::new()),
    };
    for (index, key) in keys.into_iter().enumerate() {
        let values = values_by_key.get(&key);
        match (order, values) {
            (BackendKvRead3Order::RequestOrder, Some(values)) => {
                key_builder.push(&key);
                present.push(true);
                for (value, builder) in values.iter().zip(value_builders.iter_mut()) {
                    builder.push(value);
                }
            }
            (BackendKvRead3Order::RequestOrder, None) => {
                key_builder.push(&key);
                present.push(false);
                for builder in &mut value_builders {
                    builder.push([]);
                }
            }
            (BackendKvRead3Order::KeyOrder, Some(values)) => {
                key_builder.push(&key);
                present.push(true);
                request_indexes
                    .as_mut()
                    .expect("request indexes exist")
                    .push(
                        u32::try_from(index).map_err(|_| {
                            LixError::unknown("backend read3 request index overflow")
                        })?,
                    );
                for (value, builder) in values.iter().zip(value_builders.iter_mut()) {
                    builder.push(value);
                }
            }
            (BackendKvRead3Order::KeyOrder, None) => {}
        }
    }
    Ok(BackendKvRead3Page {
        keys: key_builder.finish(),
        presence: BackendKvRead3Presence::bitmap(present),
        values: value_builders
            .into_iter()
            .map(BytePageBuilder::finish)
            .collect(),
        request_indexes,
        resume_after: None,
    })
}

pub fn project_backend_read3_value_part(
    value: &[u8],
    part: BackendKvRead3ValuePart,
) -> Result<&[u8], LixError> {
    project_backend_scan_plan_value_part(value, part.into())
}

pub fn project_backend_scan_plan_value_part(
    value: &[u8],
    part: BackendKvScanPlanValuePart,
) -> Result<&[u8], LixError> {
    match part {
        BackendKvScanPlanValuePart::Header => project_backend_header_payload_frame_part(
            value,
            BackendKvHeaderPayloadFramePart::Header,
        ),
        BackendKvScanPlanValuePart::Payload => project_backend_header_payload_frame_part(
            value,
            BackendKvHeaderPayloadFramePart::Payload,
        ),
        BackendKvScanPlanValuePart::FullValue => Ok(value),
    }
}

impl From<BackendKvRead3ValuePart> for BackendKvScanPlanValuePart {
    fn from(part: BackendKvRead3ValuePart) -> Self {
        match part {
            BackendKvRead3ValuePart::Header => Self::Header,
            BackendKvRead3ValuePart::Payload => Self::Payload,
            BackendKvRead3ValuePart::FullValue => Self::FullValue,
        }
    }
}

fn normalize_backend_spans(mut spans: Vec<BackendKvKeySpan>) -> Vec<BackendKvKeySpan> {
    spans.retain(|span| span.end.is_empty() || span.start < span.end);
    spans.sort_by(|left, right| {
        left.start
            .cmp(&right.start)
            .then_with(|| backend_span_end_for_order(left).cmp(backend_span_end_for_order(right)))
    });
    let mut normalized: Vec<BackendKvKeySpan> = Vec::new();
    for span in spans {
        let Some(last) = normalized.last_mut() else {
            normalized.push(span);
            continue;
        };
        if backend_spans_overlap_or_touch(last, &span) {
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

fn backend_spans_overlap_or_touch(left: &BackendKvKeySpan, right: &BackendKvKeySpan) -> bool {
    left.end.is_empty() || left.end >= right.start
}

fn backend_span_end_for_order(span: &BackendKvKeySpan) -> &[u8] {
    if span.end.is_empty() {
        &[0xFF]
    } else {
        &span.end
    }
}

fn backend_span_scan_range(span: &BackendKvKeySpan) -> BackendKvScanRange {
    if span.start.is_empty() && span.end.is_empty() {
        BackendKvScanRange::Prefix(Vec::new())
    } else {
        BackendKvScanRange::Range {
            start: span.start.clone(),
            end: span.end.clone(),
        }
    }
}

fn scan_after_for_backend_span(
    span: &BackendKvKeySpan,
    after: Option<&[u8]>,
) -> Option<Option<Vec<u8>>> {
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

fn last_backend_key(keys: &BytePageBuilder) -> Option<Vec<u8>> {
    keys.len()
        .checked_sub(1)
        .and_then(|index| keys.get(index))
        .map(<[u8]>::to_vec)
}

pub fn project_backend_value_part<'a>(
    value: &'a [u8],
    part: &BackendKvValuePart,
) -> Result<&'a [u8], LixError> {
    match part {
        BackendKvValuePart::ByteRange { offset, len } => value
            .get(*offset..offset.saturating_add(*len))
            .ok_or_else(|| LixError::unknown("backend value projection range is out of bounds")),
        BackendKvValuePart::ByteSuffix { offset } => value
            .get(*offset..)
            .ok_or_else(|| LixError::unknown("backend value projection suffix is out of bounds")),
        BackendKvValuePart::HeaderPayloadFrame(frame_part) => {
            project_backend_header_payload_frame_part(value, *frame_part)
        }
    }
}

const HEADER_PAYLOAD_FRAME_HEADER_LEN: usize = 25;

fn project_backend_header_payload_frame_part(
    value: &[u8],
    part: BackendKvHeaderPayloadFramePart,
) -> Result<&[u8], LixError> {
    let header = value
        .get(..HEADER_PAYLOAD_FRAME_HEADER_LEN)
        .ok_or_else(|| {
            LixError::unknown("backend framed value projection found a short frame header")
        })?;
    let header_len = read_fixed_width_decimal(&header[5..15])?;
    let payload_len = read_fixed_width_decimal(&header[15..25])?;
    let header_start = HEADER_PAYLOAD_FRAME_HEADER_LEN;
    let header_end = header_start
        .checked_add(header_len)
        .ok_or_else(|| LixError::unknown("backend framed value projection length overflow"))?;
    let payload_end = header_end
        .checked_add(payload_len)
        .ok_or_else(|| LixError::unknown("backend framed value projection length overflow"))?;
    if payload_end != value.len() {
        return Err(LixError::unknown(
            "backend framed value projection length does not match value",
        ));
    }
    match part {
        BackendKvHeaderPayloadFramePart::Header => value
            .get(header_start..header_end)
            .ok_or_else(|| LixError::unknown("backend framed header projection is out of bounds")),
        BackendKvHeaderPayloadFramePart::Payload => value
            .get(header_end..payload_end)
            .ok_or_else(|| LixError::unknown("backend framed payload projection is out of bounds")),
    }
}

fn read_fixed_width_decimal(bytes: &[u8]) -> Result<usize, LixError> {
    if bytes.len() != 10 || bytes.iter().any(|byte| !byte.is_ascii_digit()) {
        return Err(LixError::unknown(
            "backend framed value projection found an invalid length field",
        ));
    }
    let text = std::str::from_utf8(bytes).map_err(|error| {
        LixError::unknown(format!(
            "backend framed value projection found invalid length UTF-8: {error}"
        ))
    })?;
    text.parse::<usize>().map_err(|error| {
        LixError::unknown(format!(
            "backend framed value projection found invalid length: {error}"
        ))
    })
}
