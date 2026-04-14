//! Overlay precedence resolution over tracked and untracked live state.

#[cfg(test)]
use async_trait::async_trait;

use std::collections::BTreeMap;

#[cfg(test)]
mod resolve;

pub use crate::live_state::types::{EffectiveRowRequest, EffectiveRowsRequest};
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
        self.values
            .get(property_name)
            .and_then(value_as_text)
            .map(ToString::to_string)
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct EffectiveRowSet {
    pub rows: Vec<EffectiveRow>,
}

fn value_as_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(value) => Some(value.as_str()),
        _ => None,
    }
}

#[cfg(test)]
#[async_trait(?Send)]
pub trait EffectiveRowsResolver {
    async fn resolve_effective_rows(
        &self,
        request: &EffectiveRowsRequest,
    ) -> Result<EffectiveRowSet, crate::LixError>;
}

#[cfg(test)]
mod tests;
