use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use crate::common::{NullableKeyFilter, Value};
use crate::live_state::tracked::TrackedRow;
#[cfg(test)]
use crate::live_state::tracked::TrackedTombstoneMarker;
use crate::live_state::untracked::UntrackedRow;
use crate::version::CommittedVersionFrontier;

use super::constraints::{ScanConstraint, ScanField, ScanOperator};
use super::ReplayCursor;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveStateMode {
    Uninitialized,
    Bootstrapping,
    Ready,
    NeedsRebuild,
    Rebuilding,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveStateProjectionStatus {
    pub mode: LiveStateMode,
    pub applied_cursor: Option<ReplayCursor>,
    pub latest_cursor: Option<ReplayCursor>,
    pub applied_committed_frontier: Option<CommittedVersionFrontier>,
    pub current_committed_frontier: CommittedVersionFrontier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveSnapshotStorage {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum LiveFilterField {
    EntityId,
    FileId,
    PluginKey,
    SchemaVersion,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum LiveFilterOp {
    Eq(Value),
    In(Vec<Value>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveFilter {
    pub field: LiveFilterField,
    pub operator: LiveFilterOp,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveSnapshotRow {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: Option<String>,
    pub version_id: String,
    pub plugin_key: Option<String>,
    pub metadata: Option<String>,
    pub source_change_id: Option<String>,
    pub snapshot: JsonValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LiveWriteOperation {
    Upsert,
    Tombstone,
    Delete,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LiveWriteRow {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: Option<String>,
    pub version_id: String,
    pub global: bool,
    pub untracked: bool,
    pub plugin_key: Option<String>,
    pub metadata: Option<String>,
    pub change_id: String,
    pub writer_key: Option<String>,
    pub snapshot_content: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: String,
    pub operation: LiveWriteOperation,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SchemaRegistration {
    schema_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    registered_snapshot: Option<JsonValue>,
    #[serde(skip, default)]
    source: SchemaRegistrationSource,
}

#[derive(Debug, Clone, Default)]
pub struct SchemaRegistrationSet {
    inner: BTreeMap<String, SchemaRegistration>,
}

impl SchemaRegistrationSet {
    pub fn insert(&mut self, registration: impl Into<SchemaRegistration>) {
        let registration = registration.into();
        self.inner
            .entry(registration.schema_key().to_string())
            .and_modify(|existing| {
                if !existing.has_request_local_layout() && registration.has_request_local_layout() {
                    *existing = registration.clone();
                }
            })
            .or_insert(registration);
    }

    pub fn extend(&mut self, other: SchemaRegistrationSet) {
        for registration in other.inner.into_values() {
            self.insert(registration);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn values(&self) -> impl Iterator<Item = &SchemaRegistration> {
        self.inner.values()
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
enum SchemaRegistrationSource {
    #[default]
    StoredLayout,
    SchemaDefinition(JsonValue),
}

impl From<&str> for SchemaRegistration {
    fn from(schema_key: &str) -> Self {
        Self::new(schema_key)
    }
}

impl From<String> for SchemaRegistration {
    fn from(schema_key: String) -> Self {
        Self::new(schema_key)
    }
}

impl SchemaRegistration {
    pub fn new(schema_key: impl Into<String>) -> Self {
        Self {
            schema_key: schema_key.into(),
            registered_snapshot: None,
            source: SchemaRegistrationSource::StoredLayout,
        }
    }

    pub fn schema_key(&self) -> &str {
        &self.schema_key
    }

    pub fn with_registered_snapshot(
        schema_key: impl Into<String>,
        registered_snapshot: JsonValue,
    ) -> Self {
        Self {
            schema_key: schema_key.into(),
            registered_snapshot: Some(registered_snapshot),
            source: SchemaRegistrationSource::StoredLayout,
        }
    }

    pub fn with_schema_definition(
        schema_key: impl Into<String>,
        schema_definition: JsonValue,
    ) -> Self {
        Self {
            schema_key: schema_key.into(),
            registered_snapshot: None,
            source: SchemaRegistrationSource::SchemaDefinition(schema_definition),
        }
    }

    pub fn registered_snapshot(&self) -> Option<&JsonValue> {
        self.registered_snapshot.as_ref()
    }

    fn has_request_local_layout(&self) -> bool {
        self.schema_definition_override().is_some() || self.registered_snapshot().is_some()
    }

    pub fn schema_definition_override(&self) -> Option<&JsonValue> {
        match &self.source {
            SchemaRegistrationSource::StoredLayout => None,
            SchemaRegistrationSource::SchemaDefinition(schema_definition) => {
                Some(schema_definition)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExactRowRequest {
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    #[serde(default)]
    pub file_id: NullableKeyFilter<String>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct BatchRowRequest {
    pub schema_key: String,
    pub version_id: String,
    pub entity_ids: Vec<String>,
    #[serde(default)]
    pub file_id: NullableKeyFilter<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct ScanRequest {
    pub schema_key: String,
    pub version_id: String,
    #[serde(default)]
    pub constraints: Vec<ScanConstraint>,
    #[serde(default)]
    pub required_columns: Vec<String>,
}

pub fn exact_row_constraints(request: &ExactRowRequest) -> Vec<ScanConstraint> {
    let mut constraints = vec![ScanConstraint {
        field: ScanField::EntityId,
        operator: ScanOperator::Eq(Value::Text(request.entity_id.clone())),
    }];
    push_nullable_key_constraint(&mut constraints, ScanField::FileId, &request.file_id);
    constraints
}

#[cfg(test)]
pub fn batch_row_constraints(request: &BatchRowRequest) -> Vec<ScanConstraint> {
    let mut constraints = vec![ScanConstraint {
        field: ScanField::EntityId,
        operator: ScanOperator::In(
            request
                .entity_ids
                .iter()
                .cloned()
                .map(Value::Text)
                .collect(),
        ),
    }];
    push_nullable_key_constraint(&mut constraints, ScanField::FileId, &request.file_id);
    constraints
}

fn push_nullable_key_constraint(
    constraints: &mut Vec<ScanConstraint>,
    field: ScanField,
    filter: &NullableKeyFilter<String>,
) {
    match filter {
        NullableKeyFilter::Any => {}
        NullableKeyFilter::Null => constraints.push(ScanConstraint {
            field,
            operator: ScanOperator::Eq(Value::Null),
        }),
        NullableKeyFilter::Value(value) => constraints.push(ScanConstraint {
            field,
            operator: ScanOperator::Eq(Value::Text(value.clone())),
        }),
    }
}

pub fn matches_constraints(
    entity_id: &str,
    file_id: Option<&str>,
    plugin_key: Option<&str>,
    schema_version: &str,
    constraints: &[ScanConstraint],
) -> bool {
    constraints.iter().all(|constraint| {
        let candidate = match constraint.field {
            ScanField::EntityId => Some(entity_id),
            ScanField::FileId => file_id,
            ScanField::PluginKey => plugin_key,
            ScanField::SchemaVersion => Some(schema_version),
        };
        matches_constraint(candidate, &constraint.operator)
    })
}

fn matches_constraint(candidate: Option<&str>, operator: &ScanOperator) -> bool {
    match operator {
        ScanOperator::Eq(Value::Null) => candidate.is_none(),
        ScanOperator::Eq(value) => {
            value_as_text(value).is_some_and(|value| candidate == Some(value))
        }
        ScanOperator::In(values) => values.iter().any(|value| match value {
            Value::Null => candidate.is_none(),
            _ => value_as_text(value).is_some_and(|expected| candidate == Some(expected)),
        }),
        ScanOperator::Range { lower, upper } => {
            let Some(candidate) = candidate else {
                return false;
            };
            lower
                .as_ref()
                .is_none_or(|bound| compare_lower(candidate, &bound.value, bound.inclusive))
                && upper
                    .as_ref()
                    .is_none_or(|bound| compare_upper(candidate, &bound.value, bound.inclusive))
        }
    }
}

fn compare_lower(candidate: &str, bound: &Value, inclusive: bool) -> bool {
    value_as_text(bound).is_some_and(|value| {
        if inclusive {
            candidate >= value
        } else {
            candidate > value
        }
    })
}

fn compare_upper(candidate: &str, bound: &Value, inclusive: bool) -> bool {
    value_as_text(bound).is_some_and(|value| {
        if inclusive {
            candidate <= value
        } else {
            candidate < value
        }
    })
}

fn value_as_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(value) => Some(value.as_str()),
        _ => None,
    }
}

/// Logical live-state row key shared across tracked and untracked lanes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowIdentity {
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    pub file_id: Option<String>,
}

impl RowIdentity {
    pub(crate) fn storage_scope_key(&self) -> String {
        crate::common::storage_scope_key_for_file_id(self.file_id.as_deref())
    }

    #[cfg(test)]
    pub fn from_live_write(row: &LiveWriteRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub fn from_tracked_row(row: &TrackedRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub fn from_untracked_row(row: &UntrackedRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    #[cfg(test)]
    pub fn from_tombstone(row: &TrackedTombstoneMarker) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    #[cfg(test)]
    pub fn matches_batch(&self, request: &BatchRowRequest) -> bool {
        self.schema_key == request.schema_key
            && self.version_id == request.version_id
            && request.entity_ids.contains(&self.entity_id)
            && request.file_id.matches(self.file_id.as_ref())
    }

    #[cfg(test)]
    pub fn matches_scan_partition(&self, request: &ScanRequest) -> bool {
        self.schema_key == request.schema_key && self.version_id == request.version_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EffectiveRowRequest {
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    #[serde(default)]
    pub file_id: NullableKeyFilter<String>,
    pub include_global: bool,
    pub include_untracked: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct EffectiveRowsRequest {
    pub schema_key: String,
    pub version_id: String,
    #[serde(default)]
    pub constraints: Vec<ScanConstraint>,
    #[serde(default)]
    pub required_columns: Vec<String>,
    pub include_global: bool,
    pub include_untracked: bool,
    pub include_tombstones: bool,
}

#[cfg(test)]
pub fn values_from_snapshot_content(
    snapshot_content: Option<&str>,
) -> Result<BTreeMap<String, Value>, crate::LixError> {
    let Some(snapshot_content) = snapshot_content else {
        return Ok(BTreeMap::new());
    };

    let parsed = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        crate::LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("failed to decode transaction snapshot content: {error}"),
        )
    })?;

    let JsonValue::Object(object) = parsed else {
        return Ok(BTreeMap::new());
    };

    Ok(object
        .into_iter()
        .map(|(key, value)| (key, value_from_json(value)))
        .collect())
}

#[cfg(test)]
fn value_from_json(value: JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Boolean(value),
        JsonValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                Value::Integer(value)
            } else if let Some(value) = value.as_f64() {
                Value::Real(value)
            } else {
                Value::Null
            }
        }
        JsonValue::String(value) => Value::Text(value),
        JsonValue::Array(value) => Value::Json(JsonValue::Array(value)),
        JsonValue::Object(value) => Value::Json(JsonValue::Object(value)),
    }
}
