use crate::{
    CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId, FileId, LixError,
};
use std::cmp::Ordering;
use std::fmt;

pub(crate) const LIVE_TRACKED_HASH_BYTES: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LiveTrackedRootId([u8; LIVE_TRACKED_HASH_BYTES]);

impl LiveTrackedRootId {
    pub fn new(bytes: [u8; LIVE_TRACKED_HASH_BYTES]) -> Self {
        Self(bytes)
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self, LixError> {
        if bytes.len() != LIVE_TRACKED_HASH_BYTES {
            return Err(LixError::unknown(format!(
                "live tracked root id must be {LIVE_TRACKED_HASH_BYTES} bytes, got {}",
                bytes.len()
            )));
        }
        let mut out = [0_u8; LIVE_TRACKED_HASH_BYTES];
        out.copy_from_slice(bytes);
        Ok(Self(out))
    }

    pub fn as_bytes(&self) -> &[u8; LIVE_TRACKED_HASH_BYTES] {
        &self.0
    }

    pub fn into_bytes(self) -> [u8; LIVE_TRACKED_HASH_BYTES] {
        self.0
    }

    pub fn to_hex(&self) -> String {
        hex_string(&self.0)
    }
}

impl fmt::Display for LiveTrackedRootId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LiveTrackedValueRef {
    hash: [u8; LIVE_TRACKED_HASH_BYTES],
    size_bytes: usize,
}

impl LiveTrackedValueRef {
    pub fn new(hash: [u8; LIVE_TRACKED_HASH_BYTES], size_bytes: usize) -> Self {
        Self { hash, size_bytes }
    }

    pub fn from_slice(bytes: &[u8], size_bytes: usize) -> Result<Self, LixError> {
        Ok(Self {
            hash: LiveTrackedRootId::from_slice(bytes)?.into_bytes(),
            size_bytes,
        })
    }

    pub fn hash(&self) -> &[u8; LIVE_TRACKED_HASH_BYTES] {
        &self.hash
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    pub fn to_hex(&self) -> String {
        hex_string(&self.hash)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LiveTrackedEntityKey {
    pub schema_key: CanonicalSchemaKey,
    pub file_id: FileId,
    pub entity_id: EntityId,
}

impl LiveTrackedEntityKey {
    pub fn new(schema_key: CanonicalSchemaKey, file_id: FileId, entity_id: EntityId) -> Self {
        Self {
            schema_key,
            file_id,
            entity_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LiveTrackedFieldValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Real(f64),
    Text(String),
    Json(String),
    Blob(Vec<u8>),
    LargeText(LiveTrackedValueRef),
    LargeJson(LiveTrackedValueRef),
    LargeBlob(LiveTrackedValueRef),
}

impl LiveTrackedFieldValue {
    pub fn inline_len(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::Integer(_) => std::mem::size_of::<i64>(),
            Self::Real(_) => std::mem::size_of::<f64>(),
            Self::Text(text) | Self::Json(text) => text.len(),
            Self::Blob(blob) => blob.len(),
            Self::LargeText(reference)
            | Self::LargeJson(reference)
            | Self::LargeBlob(reference) => reference.size_bytes(),
        }
    }

    pub fn is_large_ref(&self) -> bool {
        matches!(
            self,
            Self::LargeText(_) | Self::LargeJson(_) | Self::LargeBlob(_)
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveTrackedPayloadColumn {
    pub name: String,
    pub value: LiveTrackedFieldValue,
}

impl LiveTrackedPayloadColumn {
    pub fn new(name: impl Into<String>, value: LiveTrackedFieldValue) -> Result<Self, LixError> {
        let name = name.into();
        if name.is_empty() {
            return Err(LixError::unknown(
                "live tracked payload column name must be non-empty",
            ));
        }
        Ok(Self { name, value })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveTrackedEntityValue {
    pub change_id: String,
    pub tombstone: bool,
    pub schema_version: CanonicalSchemaVersion,
    pub plugin_key: CanonicalPluginKey,
    pub metadata: Option<String>,
    pub columns: Vec<LiveTrackedPayloadColumn>,
}

impl LiveTrackedEntityValue {
    pub fn new(
        change_id: impl Into<String>,
        schema_version: CanonicalSchemaVersion,
        plugin_key: CanonicalPluginKey,
        metadata: Option<String>,
        columns: Vec<LiveTrackedPayloadColumn>,
    ) -> Result<Self, LixError> {
        let change_id = change_id.into();
        if change_id.is_empty() {
            return Err(LixError::unknown(
                "live tracked change_id must be non-empty",
            ));
        }
        Ok(Self {
            change_id,
            tombstone: false,
            schema_version,
            plugin_key,
            metadata,
            columns,
        })
    }

    pub fn tombstone(
        change_id: impl Into<String>,
        schema_version: CanonicalSchemaVersion,
        plugin_key: CanonicalPluginKey,
        metadata: Option<String>,
    ) -> Result<Self, LixError> {
        let change_id = change_id.into();
        if change_id.is_empty() {
            return Err(LixError::unknown(
                "live tracked change_id must be non-empty",
            ));
        }
        Ok(Self {
            change_id,
            tombstone: true,
            schema_version,
            plugin_key,
            metadata,
            columns: Vec::new(),
        })
    }

    pub fn logical_len(&self) -> usize {
        let mut total = self.change_id.len()
            + self.schema_version.as_str().len()
            + self.plugin_key.as_str().len()
            + self.metadata.as_deref().unwrap_or_default().len();
        for column in &self.columns {
            total += column.name.len();
            total += column.value.inline_len();
        }
        total
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LiveTrackedMutation {
    Put {
        key: LiveTrackedEntityKey,
        value: LiveTrackedEntityValue,
    },
    Delete {
        key: LiveTrackedEntityKey,
        value: LiveTrackedEntityValue,
    },
}

impl LiveTrackedMutation {
    pub fn put(key: LiveTrackedEntityKey, value: LiveTrackedEntityValue) -> Self {
        Self::Put { key, value }
    }

    pub fn delete(
        key: LiveTrackedEntityKey,
        value: LiveTrackedEntityValue,
    ) -> Result<Self, LixError> {
        if !value.tombstone {
            return Err(LixError::unknown(
                "live tracked delete mutation requires a tombstone value",
            ));
        }
        Ok(Self::Delete { key, value })
    }

    pub fn key(&self) -> &LiveTrackedEntityKey {
        match self {
            Self::Put { key, .. } | Self::Delete { key, .. } => key,
        }
    }

    pub fn value(&self) -> &LiveTrackedEntityValue {
        match self {
            Self::Put { value, .. } | Self::Delete { value, .. } => value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTrackedReadRequest {
    pub key: LiveTrackedEntityKey,
}

impl LiveTrackedReadRequest {
    pub fn new(key: LiveTrackedEntityKey) -> Self {
        Self { key }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LiveTrackedKeyField {
    SchemaKey,
    FileId,
    EntityId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LiveTrackedKeyComponent {
    SchemaKey(CanonicalSchemaKey),
    FileId(FileId),
    EntityId(EntityId),
}

impl LiveTrackedKeyComponent {
    pub fn field(&self) -> LiveTrackedKeyField {
        match self {
            Self::SchemaKey(_) => LiveTrackedKeyField::SchemaKey,
            Self::FileId(_) => LiveTrackedKeyField::FileId,
            Self::EntityId(_) => LiveTrackedKeyField::EntityId,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::SchemaKey(value) => value.as_str(),
            Self::FileId(value) => value.as_str(),
            Self::EntityId(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTrackedRangeBound {
    pub value: LiveTrackedKeyComponent,
    pub inclusive: bool,
}

impl LiveTrackedRangeBound {
    pub fn inclusive(value: LiveTrackedKeyComponent) -> Self {
        Self {
            value,
            inclusive: true,
        }
    }

    pub fn exclusive(value: LiveTrackedKeyComponent) -> Self {
        Self {
            value,
            inclusive: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTrackedRangeField {
    pub field: LiveTrackedKeyField,
    pub lower: Option<LiveTrackedRangeBound>,
    pub upper: Option<LiveTrackedRangeBound>,
    pub exact: bool,
}

impl LiveTrackedRangeField {
    pub fn exact(value: LiveTrackedKeyComponent) -> Self {
        Self {
            field: value.field(),
            lower: Some(LiveTrackedRangeBound::inclusive(value.clone())),
            upper: Some(LiveTrackedRangeBound::inclusive(value)),
            exact: true,
        }
    }

    pub fn interval(
        field: LiveTrackedKeyField,
        lower: Option<LiveTrackedRangeBound>,
        upper: Option<LiveTrackedRangeBound>,
    ) -> Result<Self, LixError> {
        if let Some(lower) = &lower {
            validate_bound_field(field, &lower.value)?;
        }
        if let Some(upper) = &upper {
            validate_bound_field(field, &upper.value)?;
        }
        Ok(Self {
            field,
            lower,
            upper,
            exact: false,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTrackedRangeRequest {
    pub fields: Vec<LiveTrackedRangeField>,
    pub contiguous: bool,
}

impl LiveTrackedRangeRequest {
    pub fn all() -> Self {
        Self {
            fields: Vec::new(),
            contiguous: true,
        }
    }

    pub fn prefix(
        schema_key: Option<CanonicalSchemaKey>,
        file_id: Option<FileId>,
        entity_id: Option<EntityId>,
    ) -> Self {
        let mut fields = Vec::new();
        if let Some(schema_key) = schema_key {
            fields.push(LiveTrackedRangeField::exact(
                LiveTrackedKeyComponent::SchemaKey(schema_key),
            ));
        }
        if let Some(file_id) = file_id {
            fields.push(LiveTrackedRangeField::exact(
                LiveTrackedKeyComponent::FileId(file_id),
            ));
        }
        if let Some(entity_id) = entity_id {
            fields.push(LiveTrackedRangeField::exact(
                LiveTrackedKeyComponent::EntityId(entity_id),
            ));
        }
        Self {
            fields,
            contiguous: true,
        }
    }

    pub fn validate(&self) -> Result<(), LixError> {
        let mut last_field = None;
        for field in &self.fields {
            if let Some(previous) = last_field {
                if previous >= field.field {
                    return Err(LixError::unknown(
                        "live tracked range fields must be ordered schema_key, file_id, entity_id",
                    ));
                }
            }
            if let Some(lower) = &field.lower {
                validate_bound_field(field.field, &lower.value)?;
            }
            if let Some(upper) = &field.upper {
                validate_bound_field(field.field, &upper.value)?;
            }
            last_field = Some(field.field);
        }
        Ok(())
    }

    pub fn matches(&self, key: &LiveTrackedEntityKey) -> bool {
        self.fields
            .iter()
            .all(|field| range_field_matches(field, key))
    }

    pub fn compare_key_to_lower_bound(&self, key: &LiveTrackedEntityKey) -> Ordering {
        for field in &self.fields {
            let Some(lower) = &field.lower else {
                continue;
            };
            let ordering = compare_key_component_to_bound(key, field.field, &lower.value);
            if ordering != Ordering::Equal {
                return ordering;
            }
            if !lower.inclusive && field.exact {
                return Ordering::Greater;
            }
        }
        Ordering::Equal
    }

    pub fn compare_key_to_upper_bound(&self, key: &LiveTrackedEntityKey) -> Ordering {
        for field in &self.fields {
            let Some(upper) = &field.upper else {
                continue;
            };
            let ordering = compare_key_component_to_bound(key, field.field, &upper.value);
            if ordering != Ordering::Equal {
                return ordering;
            }
            if !upper.inclusive && field.exact {
                return Ordering::Less;
            }
        }
        Ordering::Equal
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveTrackedRow {
    pub key: LiveTrackedEntityKey,
    pub value: LiveTrackedEntityValue,
}

impl LiveTrackedRow {
    pub fn new(key: LiveTrackedEntityKey, value: LiveTrackedEntityValue) -> Self {
        Self { key, value }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct LiveTrackedScan {
    rows: Vec<LiveTrackedRow>,
}

impl LiveTrackedScan {
    pub fn new(rows: Vec<LiveTrackedRow>) -> Self {
        Self { rows }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn rows(&self) -> &[LiveTrackedRow] {
        &self.rows
    }
}

impl IntoIterator for LiveTrackedScan {
    type Item = LiveTrackedRow;
    type IntoIter = std::vec::IntoIter<LiveTrackedRow>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveTrackedStateOptions {
    pub large_value_threshold_bytes: usize,
    pub target_chunk_bytes: usize,
    pub min_chunk_bytes: usize,
    pub max_chunk_bytes: usize,
    pub cache_capacity: usize,
    pub read_many_batch_size: usize,
}

impl Default for LiveTrackedStateOptions {
    fn default() -> Self {
        Self {
            large_value_threshold_bytes: 2 * 1024,
            target_chunk_bytes: 4 * 1024,
            min_chunk_bytes: 512,
            max_chunk_bytes: 16 * 1024,
            cache_capacity: 4_096,
            read_many_batch_size: 256,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveTrackedCodecProfile {
    pub row_count: usize,
    pub encoded_leaf_bytes: usize,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub large_value_count: usize,
    pub large_value_bytes: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveTrackedApplyResult {
    pub root_id: LiveTrackedRootId,
    pub row_count: usize,
    pub tree_height: usize,
    pub chunk_count: usize,
    pub chunk_bytes: usize,
    pub value_ref_count: usize,
    pub value_ref_bytes: usize,
    pub persisted_root: bool,
}

fn validate_bound_field(
    field: LiveTrackedKeyField,
    value: &LiveTrackedKeyComponent,
) -> Result<(), LixError> {
    if field != value.field() {
        return Err(LixError::unknown(format!(
            "live tracked range field {:?} does not match bound {:?}",
            field,
            value.field()
        )));
    }
    Ok(())
}

fn range_field_matches(field: &LiveTrackedRangeField, key: &LiveTrackedEntityKey) -> bool {
    if let Some(lower) = &field.lower {
        let ordering = compare_key_component_to_bound(key, field.field, &lower.value);
        if ordering == Ordering::Less
            || (ordering == Ordering::Equal && !lower.inclusive && !field.exact)
        {
            return false;
        }
    }
    if let Some(upper) = &field.upper {
        let ordering = compare_key_component_to_bound(key, field.field, &upper.value);
        if ordering == Ordering::Greater
            || (ordering == Ordering::Equal && !upper.inclusive && !field.exact)
        {
            return false;
        }
    }
    true
}

fn compare_key_component_to_bound(
    key: &LiveTrackedEntityKey,
    field: LiveTrackedKeyField,
    bound: &LiveTrackedKeyComponent,
) -> Ordering {
    match (field, bound) {
        (LiveTrackedKeyField::SchemaKey, LiveTrackedKeyComponent::SchemaKey(value)) => {
            key.schema_key.cmp(value)
        }
        (LiveTrackedKeyField::FileId, LiveTrackedKeyComponent::FileId(value)) => {
            key.file_id.cmp(value)
        }
        (LiveTrackedKeyField::EntityId, LiveTrackedKeyComponent::EntityId(value)) => {
            key.entity_id.cmp(value)
        }
        _ => Ordering::Equal,
    }
}

fn hex_string(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
