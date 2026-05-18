use std::collections::BTreeSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum VersionScope {
    Active {
        version_id: String,
    },
    Explicit {
        version_ids: BTreeSet<String>,
    },
    ExplicitDynamic {
        version_ids: BTreeSet<String>,
        param_indexes: BTreeSet<usize>,
    },
    ExplicitRequired {
        version_ids: BTreeSet<String>,
    },
    ExplicitRequiredDynamic {
        version_ids: BTreeSet<String>,
        param_indexes: BTreeSet<usize>,
    },
    Global,
    Empty,
}
