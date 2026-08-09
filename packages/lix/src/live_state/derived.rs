//! Schema classification for live relations whose physical readers are
//! intentionally outside the current-state ForkTree reader.
//!
//! The former provider scanner lived here and could acquire its own storage
//! readers.  Derived/history rows now fail closed at the current-state
//! boundary; only the schema predicates remain until their semantic owners are
//! lowered in a later compiler wave.

use crate::branch::BRANCH_REF_SCHEMA_KEY;

const DERIVED_SCHEMA_KEYS: &[&str] = &["lix_commit", "lix_commit_edge", BRANCH_REF_SCHEMA_KEY];

pub(crate) fn is_derived_schema(schema_key: &str) -> bool {
    DERIVED_SCHEMA_KEYS.contains(&schema_key)
}
