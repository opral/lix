mod effects;
mod preprocess;

pub(crate) use effects::{
    from_legacy_detected_file_domain_changes, to_legacy_detected_file_domain_changes_by_statement,
};
pub(crate) use preprocess::{
    from_legacy_preprocess_output, preprocess_plan_fingerprint,
};
