//! Schema classification for live relations whose physical readers are
//! intentionally outside the current-state ForkTree reader.
//!
//! The former provider scanner lived here and could acquire its own storage
//! readers.  Derived/history rows now fail closed at the current-state
//! boundary; only the schema predicates remain until their semantic owners are
//! lowered in a later compiler wave.

use crate::branch::BRANCH_REF_SCHEMA_KEY;
use crate::live_state::LiveStateScanRequest;

const DERIVED_SCHEMA_KEYS: &[&str] = &["lix_commit", "lix_commit_edge", BRANCH_REF_SCHEMA_KEY];

pub(super) fn request_may_include_derived(request: &LiveStateScanRequest) -> bool {
    request.filter.schema_keys.is_empty()
        || request
            .filter
            .schema_keys
            .iter()
            .any(|schema_key| is_derived_schema(schema_key))
}

pub(super) fn is_derived_schema(schema_key: &str) -> bool {
    DERIVED_SCHEMA_KEYS
        .iter()
        .any(|candidate| *candidate == schema_key)
}
