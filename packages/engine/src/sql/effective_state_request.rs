use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectiveStateVersionScope {
    ActiveVersion,
    ExplicitVersion,
    History,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveStateRequest {
    pub schema_set: BTreeSet<String>,
    pub version_scope: EffectiveStateVersionScope,
    pub include_global_overlay: bool,
    pub include_untracked_overlay: bool,
    pub include_tombstones: bool,
    pub predicate_classes: Vec<String>,
    pub required_columns: Vec<String>,
}
