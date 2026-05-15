use std::collections::BTreeSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum VersionScope {
    Active { version_id: String },
    Explicit { version_ids: BTreeSet<String> },
    ExplicitRequired { version_ids: BTreeSet<String> },
    Global,
    Empty,
}
