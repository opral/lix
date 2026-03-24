use std::collections::BTreeMap;

use crate::constraints::ScanConstraint;
use crate::live_tracked_state::{TrackedReadView, TrackedTombstoneView};
use crate::live_untracked_state::UntrackedReadView;
use crate::Value;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum OverlayLane {
    LocalUntracked,
    LocalTracked,
    GlobalUntracked,
    GlobalTracked,
}

impl OverlayLane {
    pub fn is_global(self) -> bool {
        matches!(self, Self::GlobalTracked | Self::GlobalUntracked)
    }

    pub fn is_untracked(self) -> bool {
        matches!(self, Self::LocalUntracked | Self::GlobalUntracked)
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LaneResult<T> {
    Found(T),
    Missing,
    Tombstone,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EffectiveRowRequest {
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    pub file_id: Option<String>,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct EffectiveRowIdentity {
    pub entity_id: String,
    pub file_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EffectiveRowState {
    Visible,
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EffectiveRow {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: Option<String>,
    pub file_id: String,
    pub version_id: String,
    pub source_version_id: String,
    pub global: bool,
    pub untracked: bool,
    pub plugin_key: Option<String>,
    pub metadata: Option<String>,
    pub writer_key: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub source_change_id: Option<String>,
    pub overlay_lane: OverlayLane,
    pub state: EffectiveRowState,
    pub values: BTreeMap<String, Value>,
}

impl EffectiveRow {
    pub fn identity(&self) -> EffectiveRowIdentity {
        EffectiveRowIdentity {
            entity_id: self.entity_id.clone(),
            file_id: self.file_id.clone(),
        }
    }

    pub fn is_tombstone(&self) -> bool {
        matches!(self.state, EffectiveRowState::Tombstone)
    }

    pub fn property_text(&self, property_name: &str) -> Option<String> {
        self.values.get(property_name).and_then(text_from_value)
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct EffectiveRowSet {
    pub rows: Vec<EffectiveRow>,
}

pub struct ReadContext<'a> {
    pub tracked: &'a dyn TrackedReadView,
    pub untracked: &'a dyn UntrackedReadView,
    pub tracked_tombstones: Option<&'a dyn TrackedTombstoneView>,
}

impl<'a> ReadContext<'a> {
    pub fn new(tracked: &'a dyn TrackedReadView, untracked: &'a dyn UntrackedReadView) -> Self {
        Self {
            tracked,
            untracked,
            tracked_tombstones: None,
        }
    }

    pub fn with_tracked_tombstones(
        mut self,
        tracked_tombstones: &'a dyn TrackedTombstoneView,
    ) -> Self {
        self.tracked_tombstones = Some(tracked_tombstones);
        self
    }
}

fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(value) => Some(value.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(value.to_string()),
        Value::Real(value) => Some(value.to_string()),
        _ => None,
    }
}
