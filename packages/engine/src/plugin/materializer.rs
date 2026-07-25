use crate::entity_pk::EntityPk;
use crate::live_state::LiveStateProjection;

/// A validated plugin-owned semantic mutation ready for durable staging.
///
/// The v2 component boundary uses packet records; this compact host type is
/// deliberately internal to transaction reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginDetectedChange {
    pub(crate) entity_pk: EntityPk,
    pub(crate) schema_key: String,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
}

pub(crate) fn plugin_state_live_state_projection() -> LiveStateProjection {
    LiveStateProjection {
        columns: vec!["snapshot_content".to_string(), "metadata".to_string()],
    }
}
