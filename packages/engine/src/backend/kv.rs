#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BytePage {
    bytes: Vec<u8>,
    offsets: Vec<u32>,
}

impl BytePage {
    pub fn new() -> Self {
        Self {
            bytes: Vec::new(),
            offsets: vec![0],
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, index: usize) -> Option<&[u8]> {
        let start = usize::try_from(*self.offsets.get(index)?).ok()?;
        let end = usize::try_from(*self.offsets.get(index + 1)?).ok()?;
        self.bytes.get(start..end)
    }

    pub fn iter(&self) -> BytePageIter<'_> {
        BytePageIter {
            page: self,
            index: 0,
        }
    }
}

pub struct BytePageIter<'a> {
    page: &'a BytePage,
    index: usize,
}

impl<'a> Iterator for BytePageIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.page.get(self.index)?;
        self.index += 1;
        Some(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BytePageBuilder {
    bytes: Vec<u8>,
    offsets: Vec<u32>,
}

impl BytePageBuilder {
    pub fn new() -> Self {
        Self {
            bytes: Vec::new(),
            offsets: vec![0],
        }
    }

    pub fn with_capacity(items: usize, bytes: usize) -> Self {
        let mut offsets = Vec::with_capacity(items.saturating_add(1));
        offsets.push(0);
        Self {
            bytes: Vec::with_capacity(bytes),
            offsets,
        }
    }

    pub fn from_page(page: BytePage) -> Self {
        Self {
            bytes: page.bytes,
            offsets: page.offsets,
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, index: usize) -> Option<&[u8]> {
        let start = usize::try_from(*self.offsets.get(index)?).ok()?;
        let end = usize::try_from(*self.offsets.get(index + 1)?).ok()?;
        self.bytes.get(start..end)
    }

    pub fn push(&mut self, value: impl AsRef<[u8]>) {
        let value = value.as_ref();
        self.bytes.extend_from_slice(value);
        let end = u32::try_from(self.bytes.len()).expect("byte page exceeds u32 offset capacity");
        self.offsets.push(end);
    }

    pub fn finish(self) -> BytePage {
        BytePage {
            bytes: self.bytes,
            offsets: self.offsets,
        }
    }
}

/// Ordered byte range for backend KV scans.
///
/// Ranges are half-open: `start <= key < end`. `Prefix` is explicit because it
/// is a common access pattern and lets each backend choose the safest
/// implementation for its storage engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendKvScanRange {
    Prefix(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
}

impl BackendKvScanRange {
    pub fn prefix(prefix: impl Into<Vec<u8>>) -> Self {
        Self::Prefix(prefix.into())
    }

    pub fn range(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self::Range {
            start: start.into(),
            end: end.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvGetRequest {
    pub groups: Vec<BackendKvGetGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvGetGroup {
    pub namespace: String,
    pub keys: Vec<Vec<u8>>,
}

impl BackendKvGetGroup {
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvValueBatch {
    pub groups: Vec<BackendKvValueGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvValueGroup {
    namespace: String,
    values: BytePage,
    present: Vec<bool>,
}

impl BackendKvValueGroup {
    pub fn new(namespace: impl Into<String>, values: BytePage, present: Vec<bool>) -> Self {
        assert_eq!(
            values.len(),
            present.len(),
            "backend value batch must have one value slot per presence bit"
        );
        Self {
            namespace: namespace.into(),
            values,
            present,
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn len(&self) -> usize {
        self.present.len()
    }

    pub fn is_empty(&self) -> bool {
        self.present.is_empty()
    }

    pub fn value(&self, index: usize) -> Option<Option<&[u8]>> {
        let present = *self.present.get(index)?;
        if present {
            Some(Some(
                self.values
                    .get(index)
                    .expect("backend value batch invariant violated"),
            ))
        } else {
            Some(None)
        }
    }

    pub fn values_iter(&self) -> impl Iterator<Item = Option<&[u8]>> {
        (0..self.len()).filter_map(|index| self.value(index))
    }

    pub fn into_parts(self) -> (String, BytePage, Vec<bool>) {
        (self.namespace, self.values, self.present)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvExistsBatch {
    pub groups: Vec<BackendKvExistsGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvExistsGroup {
    pub namespace: String,
    pub exists: Vec<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvScanRequest {
    pub namespace: String,
    pub range: BackendKvScanRange,
    pub after: Option<Vec<u8>>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvScan2Request {
    pub namespace: String,
    pub range: BackendKvScanRange,
    pub after: Option<Vec<u8>>,
    pub page_size: usize,
    pub projection: BackendKvScan2Projection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendKvScan2Projection {
    KeysOnly,
    FullValue,
    ValuePart(BackendKvValuePart),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendKvValuePart {
    ByteRange { offset: usize, len: usize },
    ByteSuffix { offset: usize },
    HeaderPayloadFrame(BackendKvHeaderPayloadFramePart),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKvHeaderPayloadFramePart {
    Header,
    Payload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvKeyPage {
    pub keys: BytePage,
    pub resume_after: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvValuePage {
    pub values: BytePage,
    pub resume_after: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvEntryPage {
    pub keys: BytePage,
    pub values: BytePage,
    pub resume_after: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvScan2Page {
    pub keys: BytePage,
    pub values: Option<BytePage>,
    pub resume_after: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvKeySpan {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

impl BackendKvKeySpan {
    pub fn new(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self {
            start: start.into(),
            end: end.into(),
        }
    }

    pub fn all() -> Self {
        Self {
            start: Vec::new(),
            end: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvReadV3Request {
    pub namespace: String,
    pub source: BackendKvReadV3Source,
    pub projection: BackendKvReadV3Projection,
    pub order: BackendKvReadV3Order,
    pub page_size: Option<usize>,
    pub strategy: BackendKvReadV3Strategy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendKvReadV3Source {
    Keys {
        keys: Vec<Vec<u8>>,
    },
    Spans {
        spans: Vec<BackendKvKeySpan>,
        after: Option<Vec<u8>>,
    },
    KeysOrSpans {
        keys: Vec<Vec<u8>>,
        spans: Vec<BackendKvKeySpan>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendKvReadV3Projection {
    KeysOnly,
    ValueParts(Vec<BackendKvReadV3ValuePart>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKvReadV3ValuePart {
    Header,
    Payload,
    FullValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKvReadV3Order {
    RequestOrder,
    KeyOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKvReadV3Strategy {
    Auto,
    Points,
    Scan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendKvReadV3Presence {
    All,
    Bitmap(Vec<bool>),
}

impl BackendKvReadV3Presence {
    pub fn bitmap(bits: Vec<bool>) -> Self {
        if bits.iter().all(|present| *present) {
            Self::All
        } else {
            Self::Bitmap(bits)
        }
    }

    pub fn len(&self, row_count: usize) -> usize {
        match self {
            Self::All => row_count,
            Self::Bitmap(bits) => bits.len(),
        }
    }

    pub fn is_present(&self, row_count: usize, index: usize) -> Option<bool> {
        match self {
            Self::All => (index < row_count).then_some(true),
            Self::Bitmap(bits) => bits.get(index).copied(),
        }
    }

    pub fn present_count(&self, row_count: usize) -> usize {
        match self {
            Self::All => row_count,
            Self::Bitmap(bits) => bits.iter().filter(|present| **present).count(),
        }
    }

    pub fn to_vec(&self, row_count: usize) -> Vec<bool> {
        match self {
            Self::All => vec![true; row_count],
            Self::Bitmap(bits) => bits.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvReadV3Page {
    pub keys: BytePage,
    pub presence: BackendKvReadV3Presence,
    pub values: Vec<BytePage>,
    pub request_indexes: Option<Vec<u32>>,
    pub resume_after: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvTableReadRequest {
    pub table: BackendKvTableId,
    pub key_space: BackendKvKeySpace,
    pub access: Vec<BackendKvAccessSegment>,
    pub after: Option<Vec<u8>>,
    pub projection: BackendKvRead4Projection,
    pub residual_filter: Option<BackendKvResidualFilter>,
    pub output_order: BackendKvRead4Order,
    pub limit: Option<usize>,
    pub session: Option<BackendKvReadSessionId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvTableId {
    pub namespace: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendKvKeySpace {
    OrderedBytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendKvAccessSegment {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendKvRead4Projection {
    KeysOnly,
    Parts(Vec<BackendKvRead4ValuePart>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKvRead4ValuePart {
    Header,
    PayloadRef,
    Payload,
    FullValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendKvResidualFilter {
    UntrackedState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKvRead4Order {
    RequestOrder,
    KeyOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendKvReadSessionId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvRead4Page {
    pub keys: BytePage,
    pub presence: BackendKvReadV3Presence,
    pub values: Vec<BytePage>,
    pub request_indexes: Option<Vec<u32>>,
    pub resume_after: Option<Vec<u8>>,
}

impl BackendKvReadV3Page {
    pub fn presence_len(&self) -> usize {
        self.presence.len(self.keys.len())
    }

    pub fn is_present(&self, index: usize) -> Option<bool> {
        self.presence.is_present(self.keys.len(), index)
    }

    pub fn present_count(&self) -> usize {
        self.presence.present_count(self.keys.len())
    }
}

impl BackendKvRead4Page {
    pub fn presence_len(&self) -> usize {
        self.presence.len(self.keys.len())
    }

    pub fn is_present(&self, index: usize) -> Option<bool> {
        self.presence.is_present(self.keys.len(), index)
    }

    pub fn present_count(&self) -> usize {
        self.presence.present_count(self.keys.len())
    }
}

impl BackendKvEntryPage {
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn key(&self, index: usize) -> Option<&[u8]> {
        self.keys.get(index)
    }

    pub fn value(&self, index: usize) -> Option<&[u8]> {
        self.values.get(index)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BackendKvWriteBatch {
    pub groups: Vec<BackendKvWriteGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvWriteGroup {
    namespace: String,
    ops: Vec<BackendKvWriteOp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendKvWriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    DeleteRange { range: BackendKvScanRange },
}

impl BackendKvWriteGroup {
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            ops: Vec::new(),
        }
    }

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.ops.push(BackendKvWriteOp::Put {
            key: key.into(),
            value: value.into(),
        });
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.ops.push(BackendKvWriteOp::Delete { key: key.into() });
    }

    pub fn delete_range(&mut self, range: BackendKvScanRange) {
        self.ops.push(BackendKvWriteOp::DeleteRange { range });
    }

    pub fn push(&mut self, op: BackendKvWriteOp) {
        self.ops.push(op);
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn ops(&self) -> &[BackendKvWriteOp] {
        &self.ops
    }

    pub fn into_ops(self) -> (String, Vec<BackendKvWriteOp>) {
        (self.namespace, self.ops)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BackendKvWriteStats {
    pub puts: usize,
    pub deletes: usize,
    pub delete_ranges: usize,
    pub bytes_written: usize,
}
