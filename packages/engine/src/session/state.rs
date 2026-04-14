use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SessionStateSnapshot {
    pub active_version_id: String,
    #[serde(default)]
    pub active_account_ids: Vec<String>,
    #[serde(default)]
    pub generation: u64,
    #[serde(default)]
    pub public_surface_registry_generation: u64,
}
